"""Safely reset ONLY this project's data before a fresh demo run.

What it does
  * Connects to the project database (logistics_streaming_project by default)
    and empties exactly three tables: delivery_events, rejected_events,
    pipeline_runs. Table structure is kept.
  * With --kafka it also deletes and re-creates ONLY this project's two Kafka
    topics and deletes ONLY this project's consumer group, so no old events
    can be replayed into the new run.

What it never does
  * Drop databases, touch other databases, remove Docker containers or volumes.

Usage (venv active, producer and processor STOPPED):
    python reset_database.py               # asks you to type the database name
    python reset_database.py --yes         # no prompt
    python reset_database.py --yes --kafka # recommended before a fresh demo
"""
from __future__ import annotations

import argparse
import time

from common import (PROJECT_TABLES, apply_schema, ensure_topics, load_settings,
                    mysql_connect, setup_logging, wait_for_kafka)


def reset_mysql(cfg, log):
    conn = mysql_connect(cfg, log)
    try:
        apply_schema(conn, log)  # make sure the tables exist first
        with conn.cursor() as cur:
            cur.execute("SELECT DATABASE()")
            current = cur.fetchone()[0]
            if current != cfg.mysql_database:
                raise SystemExit(f"Safety stop: connected to '{current}', expected '{cfg.mysql_database}'.")
            before = {}
            for t in PROJECT_TABLES:
                cur.execute(f"SELECT COUNT(*) FROM `{t}`")
                before[t] = cur.fetchone()[0]
            # Child tables first; TRUNCATE is fast and resets AUTO_INCREMENT.
            for t in PROJECT_TABLES:
                cur.execute(f"TRUNCATE TABLE `{current}`.`{t}`")
            for t in PROJECT_TABLES:
                cur.execute(f"SELECT COUNT(*) FROM `{t}`")
                log.info("  %-16s %6d rows -> %d", t, before[t], cur.fetchone()[0])
        conn.commit()
        log.info("MySQL reset done for database '%s' (only the 3 project tables).", current)
    finally:
        conn.close()


def reset_kafka(cfg, log):
    admin = wait_for_kafka(cfg, log)
    # 1) consumer group (fails if the processor is still running -> that's a safety check)
    try:
        groups = [g.group_id for g in admin.list_consumer_groups(request_timeout=10).result().valid]
    except Exception:
        groups = []
    if cfg.consumer_group in groups:
        try:
            admin.delete_consumer_groups([cfg.consumer_group], request_timeout=15)[cfg.consumer_group].result()
            log.info("Deleted consumer group '%s'.", cfg.consumer_group)
        except Exception as exc:
            raise SystemExit(f"Could not delete consumer group '{cfg.consumer_group}': {exc}\n"
                             "Stop processor.py first (Ctrl+C in its terminal), wait 10 s, then retry.")
    # 2) project topics only
    project_topics = [cfg.topic_events, cfg.topic_control]
    existing = set(admin.list_topics(timeout=10).topics)
    to_delete = [t for t in project_topics if t in existing]
    if to_delete:
        for t, fut in admin.delete_topics(to_delete, operation_timeout=30).items():
            fut.result()
            log.info("Deleted Kafka topic %s", t)
        # topic deletion is asynchronous; wait until they are really gone
        for _ in range(30):
            if not set(to_delete) & set(admin.list_topics(timeout=10).topics):
                break
            time.sleep(1)
    ensure_topics(cfg, log)
    log.info("Kafka topics re-created empty: %s", ", ".join(project_topics))


def main():
    cfg = load_settings()
    p = argparse.ArgumentParser(description="Reset only this project's tables (and optionally its Kafka topics).")
    p.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    p.add_argument("--kafka", action="store_true", help="also empty this project's Kafka topics and consumer group")
    args = p.parse_args()
    log = setup_logging("reset_database", cfg.log_level)

    print(f"\nThis will DELETE all rows in {', '.join(PROJECT_TABLES)}")
    print(f"in MySQL database '{cfg.mysql_database}' at {cfg.mysql_host}:{cfg.mysql_port}.")
    if args.kafka:
        print(f"It will also empty Kafka topics {cfg.topic_events}, {cfg.topic_control} "
              f"and delete consumer group {cfg.consumer_group}.")
    print("Nothing else (other databases, containers, volumes) is touched.\n")
    if not args.yes:
        typed = input(f"Type the database name ({cfg.mysql_database}) to confirm: ").strip()
        if typed != cfg.mysql_database:
            print("Cancelled. Nothing was changed.")
            return

    if args.kafka:
        reset_kafka(cfg, log)
    reset_mysql(cfg, log)
    log.info("Reset complete. Start processor.py, then producer.py, for a fresh run.")


if __name__ == "__main__":
    main()
