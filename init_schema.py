"""Create the project tables in MySQL (idempotent, never deletes data).

Usage (inside the activated virtual environment):
    python init_schema.py
"""
from common import load_settings, setup_logging, mysql_connect, apply_schema, PROJECT_TABLES


def main() -> None:
    cfg = load_settings()
    log = setup_logging("init_schema", cfg.log_level)
    conn = mysql_connect(cfg, log)
    try:
        apply_schema(conn, log)
        with conn.cursor() as cur:
            cur.execute("SELECT DATABASE(), VERSION()")
            db, version = cur.fetchone()
            log.info("Connected to database '%s' on MySQL %s", db, version)
            for table in PROJECT_TABLES:
                cur.execute(f"SELECT COUNT(*) FROM `{table}`")
                log.info("  table %-16s rows=%s", table, cur.fetchone()[0])
        log.info("Schema ready.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
