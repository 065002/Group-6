"""Shared configuration, logging, MySQL and Kafka helpers.

Every script imports from here so ports, names and credentials come from ONE
place: the local .env file (see .env.example).
"""
from __future__ import annotations

import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

PROJECT_DIR = Path(__file__).resolve().parent
ENV_FILE = PROJECT_DIR / ".env"
LOG_DIR = PROJECT_DIR / "logs"

# The 33 columns found when the source CSV was inspected, in file order.
EXPECTED_COLUMNS = [
    "order_id", "order_date", "order_time", "customer_id", "priority",
    "order_status", "warehouse_id", "hub", "city", "zone", "region",
    "latitude", "longitude", "vehicle_id", "vehicle_type", "driver_id",
    "vehicle_capacity_kg", "vehicle_status", "driver_experience_years",
    "delivery_distance_km", "estimated_delivery_time", "actual_delivery_time",
    "delay_minutes", "sla_breach", "at_risk", "eta_variance_minutes",
    "delay_reason", "delivery_attempts", "speed_kmph", "fuel_level_pct",
    "route_condition", "traffic_level", "package_weight_kg",
]

PROJECT_TABLES = ["delivery_events", "rejected_events", "pipeline_runs"]


def _bool(value: str | None, default: bool = False) -> bool:
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class Settings:
    mysql_host: str
    mysql_port: int
    mysql_user: str
    mysql_password: str
    mysql_database: str
    kafka_bootstrap: str
    topic_events: str
    topic_control: str
    topic_partitions: int
    consumer_group: str
    source_csv: Path
    stream_interval_ms: int
    stream_order: str
    stream_loop: bool
    run_id: str
    processor_batch_size: int
    processor_exit_on_complete: bool
    grafana_url: str
    log_level: str


def load_settings() -> Settings:
    if not ENV_FILE.exists():
        print(
            f"ERROR: {ENV_FILE} not found.\n"
            "Create it first:  copy .env.example .env   (CMD)\n"
            "             or:  Copy-Item .env.example .env   (PowerShell)\n"
            "then replace the CHANGE_ME passwords.",
            file=sys.stderr,
        )
        sys.exit(2)
    load_dotenv(ENV_FILE, override=True)

    password = os.getenv("MYSQL_PASSWORD", "")
    if not password or password.startswith("CHANGE_ME"):
        print("ERROR: set MYSQL_PASSWORD in .env (it still says CHANGE_ME).", file=sys.stderr)
        sys.exit(2)

    csv_path = Path(os.getenv("SOURCE_CSV", "data/logistics_streaming_master.csv"))
    if not csv_path.is_absolute():
        csv_path = PROJECT_DIR / csv_path

    order = os.getenv("STREAM_ORDER", "chronological").strip().lower()
    if order not in {"chronological", "file"}:
        print("ERROR: STREAM_ORDER must be 'chronological' or 'file'.", file=sys.stderr)
        sys.exit(2)

    return Settings(
        mysql_host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        mysql_port=int(os.getenv("MYSQL_HOST_PORT", "3307")),
        mysql_user=os.getenv("MYSQL_USER", "logistics_user"),
        mysql_password=password,
        mysql_database=os.getenv("MYSQL_DATABASE", "logistics_streaming_project"),
        kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        topic_events=os.getenv("KAFKA_TOPIC_EVENTS", "logistics.delivery.events"),
        topic_control=os.getenv("KAFKA_TOPIC_CONTROL", "logistics.pipeline.control"),
        topic_partitions=int(os.getenv("KAFKA_TOPIC_PARTITIONS", "1")),
        consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", "logistics-stream-processor"),
        source_csv=csv_path,
        stream_interval_ms=int(os.getenv("STREAM_INTERVAL_MS", "50")),
        stream_order=order,
        stream_loop=_bool(os.getenv("STREAM_LOOP"), False),
        run_id=(os.getenv("RUN_ID") or "").strip(),
        processor_batch_size=int(os.getenv("PROCESSOR_BATCH_SIZE", "200")),
        processor_exit_on_complete=_bool(os.getenv("PROCESSOR_EXIT_ON_COMPLETE"), True),
        grafana_url=os.getenv("GRAFANA_URL", "http://localhost:3000").rstrip("/"),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
    )


def setup_logging(name: str, level: str = "INFO") -> logging.Logger:
    LOG_DIR.mkdir(exist_ok=True)
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(level)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    logger.addHandler(console)
    file_handler = logging.FileHandler(LOG_DIR / f"{name}.log", encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
    return logger


def utc_now() -> datetime:
    """Naive UTC datetime (MySQL DATETIME has no timezone; Grafana reads it as UTC)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ----------------------------------------------------------------- MySQL ----
def mysql_connect(cfg: Settings, logger: logging.Logger | None = None,
                  retries: int = 30, delay: float = 3.0, database: bool = True):
    """Connect with retries (MySQL can take ~30 s on first container start)."""
    import pymysql

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return pymysql.connect(
                host=cfg.mysql_host,
                port=cfg.mysql_port,
                user=cfg.mysql_user,
                password=cfg.mysql_password,
                database=cfg.mysql_database if database else None,
                charset="utf8mb4",
                autocommit=False,
                connect_timeout=10,
                init_command="SET time_zone = '+00:00'",
            )
        except pymysql.err.OperationalError as exc:
            last_err = exc
            code = exc.args[0] if exc.args else None
            if code in (1044, 1045, 1049):  # access denied / unknown database: retrying won't help
                break
            if logger:
                logger.warning("MySQL not ready (%s/%s): %s", attempt, retries, exc)
            time.sleep(delay)
    msg = (f"Cannot connect to MySQL at {cfg.mysql_host}:{cfg.mysql_port} "
           f"as '{cfg.mysql_user}' (db '{cfg.mysql_database}'): {last_err}")
    if logger:
        logger.error(msg)
    raise SystemExit(msg)


def apply_schema(conn, logger: logging.Logger | None = None) -> None:
    sql_path = PROJECT_DIR / "mysql" / "init" / "init.sql"
    text = sql_path.read_text(encoding="utf-8")
    statements = []
    for chunk in text.split(";"):
        lines = [ln for ln in chunk.splitlines() if not ln.strip().startswith("--")]
        stmt = "\n".join(lines).strip()
        if stmt:
            statements.append(stmt)
    with conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)
    conn.commit()
    if logger:
        logger.info("Schema applied (%d statements, idempotent).", len(statements))


# ----------------------------------------------------------------- Kafka ----
def kafka_admin(cfg: Settings):
    from confluent_kafka.admin import AdminClient
    return AdminClient({"bootstrap.servers": cfg.kafka_bootstrap, "socket.timeout.ms": 10000})


def wait_for_kafka(cfg: Settings, logger: logging.Logger, retries: int = 30, delay: float = 3.0):
    admin = kafka_admin(cfg)
    for attempt in range(1, retries + 1):
        try:
            md = admin.list_topics(timeout=5)
            logger.info("Kafka reachable at %s (%d broker).", cfg.kafka_bootstrap, len(md.brokers))
            return admin
        except Exception as exc:  # KafkaException
            logger.warning("Kafka not ready (%s/%s): %s", attempt, retries, exc)
            time.sleep(delay)
    raise SystemExit(f"Kafka not reachable at {cfg.kafka_bootstrap}. Is the logistics-kafka container healthy?")


def ensure_topics(cfg: Settings, logger: logging.Logger) -> None:
    from confluent_kafka.admin import NewTopic

    admin = wait_for_kafka(cfg, logger)
    existing = set(admin.list_topics(timeout=10).topics)
    wanted = [
        NewTopic(cfg.topic_events, num_partitions=cfg.topic_partitions, replication_factor=1),
        NewTopic(cfg.topic_control, num_partitions=1, replication_factor=1),
    ]
    to_create = [t for t in wanted if t.topic not in existing]
    if not to_create:
        return
    for topic, fut in admin.create_topics(to_create).items():
        try:
            fut.result()
            logger.info("Created Kafka topic %s", topic)
        except Exception as exc:
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                continue
            raise
