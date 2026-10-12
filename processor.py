"""Kafka -> MySQL stream processor.

Guarantees
  * Reads events from Kafka with auto-commit DISABLED.
  * Validates and converts every field (types, ranges, Yes/No flags).
  * Writes with parameterized SQL inside ONE MySQL transaction per batch;
    the run counters (inserted / duplicate / rejected) are updated in the
    same transaction, so they can never disagree with the table contents.
  * Commits Kafka offsets ONLY after the MySQL COMMIT succeeded.
  * If MySQL fails, the transaction is rolled back and the consumer seeks back
    to the first un-committed offset, so the same events are retried.
  * Re-delivered events hit the UNIQUE key (run_id, pass_no, order_id) and are
    counted as duplicates instead of creating a second row.
  * Invalid events are stored in rejected_events with the reason, never hidden.
  * When the producer's run_end has arrived and inserted + rejected equals the
    number of published events, the run is marked COMPLETED and (by default)
    the processor exits.

Usage (venv active):
    python processor.py
    python processor.py --no-exit      # keep running after the batch completes
"""
from __future__ import annotations

import argparse
import json
import signal
import time
from collections import defaultdict
from datetime import date, datetime, time as dtime
from decimal import Decimal, InvalidOperation

from common import (EXPECTED_COLUMNS, ensure_topics, load_settings, mysql_connect,
                    apply_schema, setup_logging, utc_now)

_stop = False


def _handle_stop(signum, frame):
    global _stop
    _stop = True


# ------------------------------------------------------------ validation ----
class Invalid(ValueError):
    pass


def _req(r, col):
    v = r.get(col)
    if v is None or str(v).strip() == "":
        raise Invalid(f"{col} is empty")
    return str(v).strip()


def _dec(r, col, lo=None, hi=None):
    s = _req(r, col)
    try:
        v = Decimal(s)
    except InvalidOperation:
        raise Invalid(f"{col}='{s}' is not a number")
    if not v.is_finite():
        raise Invalid(f"{col}='{s}' is not finite")
    if lo is not None and v < lo:
        raise Invalid(f"{col}={s} below {lo}")
    if hi is not None and v > hi:
        raise Invalid(f"{col}={s} above {hi}")
    return v


def _int(r, col, lo=None, hi=None):
    v = _dec(r, col, lo, hi)
    if v != v.to_integral_value():
        raise Invalid(f"{col}={v} is not a whole number")
    return int(v)


def _dt(r, col, required=True):
    s = (r.get(col) or "").strip()
    if not s:
        if required:
            raise Invalid(f"{col} is empty")
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    raise Invalid(f"{col}='{s}' is not YYYY-MM-DD HH:MM:SS")


def _yes_no(r, col):
    """Source uses Yes/No. Accept common boolean spellings, store 1/0."""
    s = _req(r, col).lower()
    if s in {"yes", "y", "true", "1"}:
        return 1
    if s in {"no", "n", "false", "0"}:
        return 0
    raise Invalid(f"{col}='{r.get(col)}' is not Yes/No")


def validate(record: dict) -> dict:
    if not isinstance(record, dict):
        raise Invalid("record is not an object")
    missing = [c for c in EXPECTED_COLUMNS if c not in record]
    if missing:
        raise Invalid(f"missing columns: {missing}")

    order_date_s = _req(record, "order_date")
    order_time_s = _req(record, "order_time")
    try:
        order_date = date.fromisoformat(order_date_s)
    except ValueError:
        raise Invalid(f"order_date='{order_date_s}' is not YYYY-MM-DD")
    try:
        order_time = dtime.fromisoformat(order_time_s)
    except ValueError:
        raise Invalid(f"order_time='{order_time_s}' is not HH:MM:SS")

    capacity = _int(record, "vehicle_capacity_kg", 1)
    weight = _dec(record, "package_weight_kg", Decimal("0"))
    row = {
        "order_id": _req(record, "order_id"),
        "order_date": order_date,
        "order_time": order_time,
        "customer_id": _req(record, "customer_id"),
        "priority": _req(record, "priority"),
        "order_status": _req(record, "order_status"),
        "warehouse_id": _req(record, "warehouse_id"),
        "hub": _req(record, "hub"),
        "city": _req(record, "city"),
        "zone": _req(record, "zone"),
        "region": _req(record, "region"),
        "latitude": _dec(record, "latitude", -90, 90),
        "longitude": _dec(record, "longitude", -180, 180),
        "vehicle_id": _req(record, "vehicle_id"),
        "vehicle_type": _req(record, "vehicle_type"),
        "driver_id": _req(record, "driver_id"),
        "vehicle_capacity_kg": capacity,
        "vehicle_status": _req(record, "vehicle_status"),
        "driver_experience_years": _dec(record, "driver_experience_years", 0, 80),
        "delivery_distance_km": _dec(record, "delivery_distance_km", 0),
        "estimated_delivery_time": _dt(record, "estimated_delivery_time"),
        "actual_delivery_time": _dt(record, "actual_delivery_time", required=False),
        "delay_minutes": _int(record, "delay_minutes", 0),
        "sla_breach": _yes_no(record, "sla_breach"),
        "at_risk": _yes_no(record, "at_risk"),
        "eta_variance_minutes": _int(record, "eta_variance_minutes"),
        "delay_reason": _req(record, "delay_reason"),
        "delivery_attempts": _int(record, "delivery_attempts", 0),
        "speed_kmph": _dec(record, "speed_kmph", 0),
        "fuel_level_pct": _dec(record, "fuel_level_pct", 0, 100),
        "route_condition": _req(record, "route_condition"),
        "traffic_level": _req(record, "traffic_level"),
        "package_weight_kg": weight,
    }
    if len(row["order_id"]) > 20:
        raise Invalid("order_id longer than 20 characters")
    row["order_ts"] = datetime.combine(order_date, order_time)
    row["load_utilization_pct"] = (weight / Decimal(capacity) * 100).quantize(Decimal("0.01"))
    return row


INSERT_SQL = """
INSERT INTO delivery_events (
  event_id, run_id, pass_no, source_row_number,
  order_id, order_date, order_time, customer_id, priority, order_status,
  warehouse_id, hub, city, zone, region, latitude, longitude,
  vehicle_id, vehicle_type, driver_id, vehicle_capacity_kg, vehicle_status,
  driver_experience_years, delivery_distance_km, estimated_delivery_time,
  actual_delivery_time, delay_minutes, sla_breach, at_risk, eta_variance_minutes,
  delay_reason, delivery_attempts, speed_kmph, fuel_level_pct, route_condition,
  traffic_level, package_weight_kg, order_ts, load_utilization_pct,
  produced_at, ingested_at, kafka_partition, kafka_offset
) VALUES (
  %(event_id)s, %(run_id)s, %(pass_no)s, %(source_row_number)s,
  %(order_id)s, %(order_date)s, %(order_time)s, %(customer_id)s, %(priority)s, %(order_status)s,
  %(warehouse_id)s, %(hub)s, %(city)s, %(zone)s, %(region)s, %(latitude)s, %(longitude)s,
  %(vehicle_id)s, %(vehicle_type)s, %(driver_id)s, %(vehicle_capacity_kg)s, %(vehicle_status)s,
  %(driver_experience_years)s, %(delivery_distance_km)s, %(estimated_delivery_time)s,
  %(actual_delivery_time)s, %(delay_minutes)s, %(sla_breach)s, %(at_risk)s, %(eta_variance_minutes)s,
  %(delay_reason)s, %(delivery_attempts)s, %(speed_kmph)s, %(fuel_level_pct)s, %(route_condition)s,
  %(traffic_level)s, %(package_weight_kg)s, %(order_ts)s, %(load_utilization_pct)s,
  %(produced_at)s, %(ingested_at)s, %(kafka_partition)s, %(kafka_offset)s
)
ON DUPLICATE KEY UPDATE event_id = event_id
"""
# ^ duplicate => affected rows 0 (PyMySQL does not set CLIENT_FOUND_ROWS), new row => 1

REJECT_SQL = """
INSERT INTO rejected_events (run_id, pass_no, order_id, source_row_number,
  kafka_partition, kafka_offset, reason, raw_payload, rejected_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE id = id
"""

ENSURE_RUN_SQL = """
INSERT INTO pipeline_runs (run_id, status, started_at, updated_at)
VALUES (%s, 'RUNNING', %s, %s)
ON DUPLICATE KEY UPDATE run_id = run_id
"""


def _parse_iso(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", ""))
    except ValueError:
        return None


# -------------------------------------------------------------- processor ----
class Processor:
    def __init__(self, cfg, log, exit_on_complete: bool):
        self.cfg = cfg
        self.log = log
        self.exit_on_complete = exit_on_complete
        self.conn = None
        self.consumer = None
        self.session = defaultdict(int)  # counters for this processor session
        self.completed_runs = []

    # -- infrastructure -------------------------------------------------------
    def connect_db(self):
        if self.conn is not None:
            try:
                self.conn.ping(reconnect=True)
                return
            except Exception:
                self.conn = None
        self.conn = mysql_connect(self.cfg, self.log)

    def start(self):
        from confluent_kafka import Consumer

        self.connect_db()
        apply_schema(self.conn, self.log)
        ensure_topics(self.cfg, self.log)
        self.consumer = Consumer({
            "bootstrap.servers": self.cfg.kafka_bootstrap,
            "group.id": self.cfg.consumer_group,
            "enable.auto.commit": False,          # we commit manually after MySQL commit
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
            "session.timeout.ms": 45000,
            "max.poll.interval.ms": 600000,
            "client.id": "logistics-stream-processor",
        })
        self.consumer.subscribe([self.cfg.topic_events, self.cfg.topic_control])
        self.log.info("Subscribed to %s + %s as group '%s'. Waiting for events... (Ctrl+C to stop)",
                      self.cfg.topic_events, self.cfg.topic_control, self.cfg.consumer_group)

    # -- per-message handlers (run inside an open transaction) ----------------
    def _ensure_run(self, cur, run_id, now):
        cur.execute(ENSURE_RUN_SQL, (run_id, now, now))

    def _control(self, cur, msg, payload, now):
        run_id = str(payload.get("run_id") or "")
        if not run_id:
            self.log.warning("Control message without run_id at offset %s ignored", msg.offset())
            return
        self._ensure_run(cur, run_id, now)
        if payload.get("type") == "run_start":
            cur.execute(
                """UPDATE pipeline_runs SET source_file=%s, source_rows=%s, source_sha256=%s,
                   stream_order=%s, stream_interval_ms=%s, loop_mode=%s,
                   started_at=COALESCE(%s, started_at), updated_at=%s WHERE run_id=%s""",
                (payload.get("source_file"), payload.get("source_rows"), payload.get("source_sha256"),
                 payload.get("stream_order"), payload.get("stream_interval_ms"),
                 1 if payload.get("loop_mode") else 0, _parse_iso(payload.get("sent_at")), now, run_id))
            self.log.info("run_start  %s  (%s source rows, order=%s, interval=%sms)", run_id,
                          payload.get("source_rows"), payload.get("stream_order"),
                          payload.get("stream_interval_ms"))
        elif payload.get("type") == "run_end":
            cur.execute(
                """UPDATE pipeline_runs SET published_count=%s, producer_complete=%s,
                   producer_finished_at=%s, updated_at=%s,
                   status=IF(status='RUNNING','PUBLISHED',status) WHERE run_id=%s""",
                (int(payload.get("published_count") or 0), 1 if payload.get("complete") else 0,
                 _parse_iso(payload.get("sent_at")) or now, now, run_id))
            self.log.info("run_end    %s  producer published %s events (complete=%s)", run_id,
                          payload.get("published_count"), payload.get("complete"))

    def _reject(self, cur, msg, run_id, pass_no, order_id, row_no, reason, raw, now, counts):
        cur.execute(REJECT_SQL, (run_id or "UNKNOWN", pass_no or 1, (order_id or None) and str(order_id)[:64],
                                 row_no, msg.partition(), msg.offset(), reason[:500], raw, now))
        if cur.rowcount == 1:
            counts[run_id or "UNKNOWN"]["rejected"] += 1
            self.log.warning("REJECTED offset %s order=%s: %s", msg.offset(), order_id, reason)
        else:
            counts[run_id or "UNKNOWN"]["duplicate"] += 1

    def _data(self, cur, msg, raw, now, counts):
        try:
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                raise ValueError("payload is not a JSON object")
        except ValueError as exc:
            self._reject(cur, msg, "UNKNOWN", 1, None, None, f"invalid JSON: {exc}", raw, now, counts)
            self._ensure_run(cur, "UNKNOWN", now)
            return
        run_id = str(payload.get("run_id") or "UNKNOWN")[:64]
        pass_no = payload.get("pass_no") if isinstance(payload.get("pass_no"), int) else 1
        row_no = payload.get("source_row_number") if isinstance(payload.get("source_row_number"), int) else None
        record = payload.get("record") or {}
        order_id = record.get("order_id") if isinstance(record, dict) else None
        self._ensure_run(cur, run_id, now)
        try:
            if row_no is None:
                raise Invalid("source_row_number missing")
            row = validate(record)
        except Invalid as exc:
            self._reject(cur, msg, run_id, pass_no, order_id, row_no, str(exc), raw, now, counts)
            return
        row.update({
            "event_id": f"{run_id}|{pass_no}|{row['order_id']}",
            "run_id": run_id, "pass_no": pass_no, "source_row_number": row_no,
            "produced_at": _parse_iso(payload.get("produced_at")),
            "ingested_at": now,
            "kafka_partition": msg.partition(), "kafka_offset": msg.offset(),
        })
        cur.execute(INSERT_SQL, row)
        if cur.rowcount == 1:
            counts[run_id]["inserted"] += 1
        else:
            counts[run_id]["duplicate"] += 1
            self.log.debug("Duplicate ignored: %s (offset %s)", row["event_id"], msg.offset())

    # -- batch ----------------------------------------------------------------
    def process_batch(self, msgs) -> None:
        from confluent_kafka import TopicPartition

        good = [m for m in msgs if m.error() is None]
        for m in msgs:
            if m.error() is not None and m.error().code() != -191:  # -191 = partition EOF
                self.log.warning("Kafka message error: %s", m.error())
        if not good:
            return

        attempt = 0
        while True:
            attempt += 1
            counts = defaultdict(lambda: defaultdict(int))
            try:
                self.connect_db()
                now = utc_now()
                with self.conn.cursor() as cur:
                    for m in good:
                        raw = (m.value() or b"").decode("utf-8", errors="replace")
                        if m.topic() == self.cfg.topic_control:
                            try:
                                self._control(cur, m, json.loads(raw), now)
                            except (ValueError, AttributeError) as exc:
                                self.log.warning("Bad control message ignored: %s", exc)
                        else:
                            self._data(cur, m, raw, now, counts)
                    for run_id, c in counts.items():
                        cur.execute(
                            """UPDATE pipeline_runs SET inserted_count=inserted_count+%s,
                               duplicate_count=duplicate_count+%s, rejected_count=rejected_count+%s,
                               updated_at=%s WHERE run_id=%s""",
                            (c["inserted"], c["duplicate"], c["rejected"], now, run_id))
                self.conn.commit()  # <-- data is durable here
                break
            except Exception as exc:  # DB problem: roll back and retry the SAME batch
                self.log.error("MySQL write failed (attempt %d): %s", attempt, exc)
                try:
                    self.conn.rollback()
                except Exception:
                    self.conn = None
                if _stop:
                    self._rewind(good)
                    raise SystemExit("Stopped before batch could be written; offsets NOT committed.")
                time.sleep(min(30, 2 ** min(attempt, 5)))

        # Only now commit Kafka offsets (next offset to read = last + 1).
        last = {}
        for m in good:
            key = (m.topic(), m.partition())
            last[key] = max(last.get(key, -1), m.offset())
        offsets = [TopicPartition(t, p, o + 1) for (t, p), o in last.items()]
        for i in range(5):
            try:
                self.consumer.commit(offsets=offsets, asynchronous=False)
                break
            except Exception as exc:
                self.log.warning("Offset commit failed (%d/5): %s. Rows are safe; a redelivery "
                                 "would be counted as duplicates.", i + 1, exc)
                time.sleep(2)

        for run_id, c in counts.items():
            if c["duplicate"]:
                self.log.warning("Run %s: %d re-delivered event(s) ignored as duplicates (no extra rows).",
                                 run_id, c["duplicate"])
            for k, v in c.items():
                self.session[k] += v
        self.session["messages"] += len(good)

    def _rewind(self, good):
        from confluent_kafka import TopicPartition
        first = {}
        for m in good:
            key = (m.topic(), m.partition())
            first[key] = min(first.get(key, m.offset()), m.offset())
        for (t, p), o in first.items():
            try:
                self.consumer.seek(TopicPartition(t, p, o))
            except Exception:
                pass

    # -- completion -------------------------------------------------------------
    def check_completion(self) -> bool:
        """Mark runs whose every published event is accounted for. Returns True if any completed now."""
        now = utc_now()
        done_now = False
        with self.conn.cursor() as cur:
            cur.execute("""SELECT run_id, published_count, producer_complete, inserted_count,
                                  rejected_count, duplicate_count, source_rows
                           FROM pipeline_runs WHERE status='PUBLISHED'""")
            for run_id, published, producer_ok, ins, rej, dup, src in cur.fetchall():
                if published is not None and ins + rej >= published:
                    status = "COMPLETED" if producer_ok else "INCOMPLETE"
                    cur.execute("UPDATE pipeline_runs SET status=%s, completed_at=%s, updated_at=%s "
                                "WHERE run_id=%s", (status, now, now, run_id))
                    self.log.info("=" * 70)
                    self.log.info("RUN %s %s", run_id, status)
                    self.log.info("  source rows      : %s", src)
                    self.log.info("  published events : %s", published)
                    self.log.info("  inserted rows    : %s", ins)
                    self.log.info("  rejected rows    : %s", rej)
                    self.log.info("  duplicates seen  : %s (ignored, no extra rows)", dup)
                    self.log.info("  accounted        : %s / %s", ins + rej, published)
                    self.log.info("=" * 70)
                    self.completed_runs.append(run_id)
                    done_now = True
        self.conn.commit()
        return done_now

    def run(self):
        self.start()
        last_report = time.monotonic()
        try:
            while not _stop:
                msgs = self.consumer.consume(num_messages=self.cfg.processor_batch_size, timeout=1.0)
                if msgs:
                    self.process_batch(msgs)
                if time.monotonic() - last_report > 10:
                    s = self.session
                    self.log.info("progress: messages=%d inserted=%d duplicates=%d rejected=%d",
                                  s["messages"], s["inserted"], s["duplicate"], s["rejected"])
                    last_report = time.monotonic()
                self.connect_db()
                if self.check_completion() and self.exit_on_complete:
                    self.log.info("Batch complete. Exiting (use --no-exit to keep running).")
                    break
        finally:
            self.log.info("Closing consumer (committed offsets are kept for the next start).")
            try:
                self.consumer.close()
            except Exception:
                pass
            if self.conn:
                self.conn.close()


def main() -> None:
    cfg = load_settings()
    p = argparse.ArgumentParser(description="Consume logistics events from Kafka into MySQL.")
    p.add_argument("--no-exit", action="store_true", help="keep running after a run completes")
    args = p.parse_args()
    log = setup_logging("processor", cfg.log_level)
    signal.signal(signal.SIGINT, _handle_stop)
    Processor(cfg, log, exit_on_complete=cfg.processor_exit_on_complete and not args.no_exit).run()


if __name__ == "__main__":
    main()
