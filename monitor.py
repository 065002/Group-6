"""Watch the live pipeline: Kafka lag + MySQL row counts, refreshed every 2 s.

Usage (venv active, third terminal):  python monitor.py      (Ctrl+C to stop)
"""
from __future__ import annotations

import time

from common import load_settings, mysql_connect, setup_logging


def kafka_end_offsets(cfg):
    """Return (events_in_topic, committed_by_processor) or (None, None)."""
    try:
        from confluent_kafka import Consumer, TopicPartition
        c = Consumer({"bootstrap.servers": cfg.kafka_bootstrap, "group.id": cfg.consumer_group,
                      "enable.auto.commit": False})
        md = c.list_topics(cfg.topic_events, timeout=5)
        parts = [TopicPartition(cfg.topic_events, p) for p in md.topics[cfg.topic_events].partitions]
        high = sum(c.get_watermark_offsets(tp, timeout=5)[1] for tp in parts)
        low = sum(c.get_watermark_offsets(tp, timeout=5)[0] for tp in parts)
        committed = sum(tp.offset for tp in c.committed(parts, timeout=5) if tp.offset >= 0)
        c.close()
        return high - low, max(committed - low, 0)
    except Exception:
        return None, None


def main():
    cfg = load_settings()
    log = setup_logging("monitor", "WARNING")
    conn = mysql_connect(cfg, log)
    print("time      | kafka events | processor committed | lag  | run / status / inserted / rejected / dup")
    try:
        while True:
            conn.ping(reconnect=True)
            with conn.cursor() as cur:
                cur.execute("SELECT run_id, status, inserted_count, rejected_count, duplicate_count, "
                            "COALESCE(source_rows, 0) FROM pipeline_runs WHERE run_id <> 'UNKNOWN' ORDER BY started_at DESC LIMIT 1")
                r = cur.fetchone()
            conn.commit()
            total, committed = kafka_end_offsets(cfg)
            lag = (total - committed) if total is not None else None
            run = (f"{r[0]} / {r[1]} / {r[2]}/{r[5]} / {r[3]} / {r[4]}" if r else "no run yet")
            print(f"{time.strftime('%H:%M:%S')}  | {str(total):>12} | {str(committed):>19} | "
                  f"{str(lag):>4} | {run}")
            time.sleep(2)
    except KeyboardInterrupt:
        pass
    finally:
        conn.close()


if __name__ == "__main__":
    main()
