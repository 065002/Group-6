"""CSV -> Kafka streaming producer.

Default behaviour (one-pass mode):
  * reads data/logistics_streaming_master.csv (read-only, never modified)
  * publishes EXACTLY one Kafka event per CSV data row
  * waits STREAM_INTERVAL_MS between events to simulate live arrivals
  * sends a run_start and run_end control message (separate control topic)
  * stops after the last row

Usage examples (venv active):
    python producer.py                         # settings from .env
    python producer.py --interval-ms 20        # faster demo (~4 min)
    python producer.py --interval-ms 0         # as fast as possible
    python producer.py --run-id DEMO-01
    python producer.py --order file            # original CSV row order
    python producer.py --loop                  # OPTIONAL replay mode (off by default)
    python producer.py --dry-run               # validate CSV only, publish nothing
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import signal
import sys
import time
from datetime import datetime

from common import (EXPECTED_COLUMNS, ensure_topics, load_settings, setup_logging, utc_now)

SCHEMA_VERSION = 1
_stop = False


def _handle_stop(signum, frame):  # Ctrl+C
    global _stop
    _stop = True


def read_source(path, log):
    """Return (rows, sha256). rows = list of (source_row_number, dict-of-strings)."""
    if not path.exists():
        raise SystemExit(f"Source CSV not found: {path}\nCopy logistics_streaming_master.csv into the data folder.")
    raw = path.read_bytes()
    sha = hashlib.sha256(raw).hexdigest()
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        header = reader.fieldnames or []
        if header != EXPECTED_COLUMNS:
            missing = [c for c in EXPECTED_COLUMNS if c not in header]
            extra = [c for c in header if c not in EXPECTED_COLUMNS]
            raise SystemExit(f"CSV header mismatch. missing={missing} extra={extra}")
        rows = [(i, dict(r)) for i, r in enumerate(reader, start=1)]
    log.info("Read %d data rows from %s (sha256 %s...)", len(rows), path.name, sha[:12])
    return rows, sha


def order_rows(rows, mode):
    if mode == "file":
        return rows

    def key(item):
        _, r = item
        try:
            ts = datetime.strptime(f"{r['order_date']} {r['order_time']}", "%Y-%m-%d %H:%M:%S")
        except ValueError:
            ts = datetime.max  # unparseable rows still get published; processor rejects them visibly
        return (ts, r["order_id"])

    return sorted(rows, key=key)


def main() -> None:
    cfg = load_settings()
    p = argparse.ArgumentParser(description="Stream the logistics CSV into Kafka.")
    p.add_argument("--run-id", default=cfg.run_id or None)
    p.add_argument("--interval-ms", type=int, default=cfg.stream_interval_ms)
    p.add_argument("--order", choices=["chronological", "file"], default=cfg.stream_order)
    p.add_argument("--loop", action="store_true", default=cfg.stream_loop,
                   help="replay the file repeatedly (pass_no 2, 3, ...). OFF by default.")
    p.add_argument("--max-passes", type=int, default=0, help="with --loop: stop after N passes (0 = until Ctrl+C)")
    p.add_argument("--limit", type=int, default=0, help="publish only the first N rows (testing). 0 = all")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    log = setup_logging("producer", cfg.log_level)
    run_id = args.run_id or utc_now().strftime("RUN-%Y%m%d-%H%M%S")
    if len(run_id) > 64 or "|" in run_id:
        raise SystemExit("run id must be <= 64 chars and must not contain '|'")

    rows, sha = read_source(cfg.source_csv, log)
    rows = order_rows(rows, args.order)
    if args.limit:
        rows = rows[: args.limit]
        log.warning("--limit %d: only a subset will be published (verification will flag this).", args.limit)
    expected = len(rows)

    est = expected * args.interval_ms / 1000
    log.info("RUN_ID=%s  rows=%d  order=%s  interval=%dms  (~%.0f min)  loop=%s",
             run_id, expected, args.order, args.interval_ms, est / 60, args.loop)
    if args.dry_run:
        log.info("Dry run: CSV is readable and has the expected 33 columns. Nothing published.")
        return

    from confluent_kafka import Producer

    ensure_topics(cfg, log)
    producer = Producer({
        "bootstrap.servers": cfg.kafka_bootstrap,
        "enable.idempotence": True,     # no broker-side duplicates on retry
        "acks": "all",
        "linger.ms": 5,
        "message.timeout.ms": 120000,
        "client.id": "logistics-csv-producer",
    })

    delivered = {"ok": 0, "failed": 0}

    def on_delivery(err, msg):
        if err is not None:
            delivered["failed"] += 1
            log.error("Delivery failed for key=%s: %s", msg.key(), err)
        else:
            delivered["ok"] += 1

    def send_control(payload):
        producer.produce(cfg.topic_control, key=run_id.encode(), value=json.dumps(payload).encode())
        producer.flush(30)

    signal.signal(signal.SIGINT, _handle_stop)
    send_control({
        "type": "run_start", "schema_version": SCHEMA_VERSION, "run_id": run_id,
        "source_file": cfg.source_csv.name, "source_rows": expected, "source_sha256": sha,
        "stream_order": args.order, "stream_interval_ms": args.interval_ms,
        "loop_mode": bool(args.loop), "sent_at": utc_now().isoformat(timespec="milliseconds"),
    })

    interval = max(args.interval_ms, 0) / 1000.0
    pass_no = 0
    sent_total = 0
    started = time.monotonic()
    while not _stop:
        pass_no += 1
        sent_this_pass = 0
        for source_row_number, record in rows:
            if _stop:
                break
            event = {
                "type": "delivery_event", "schema_version": SCHEMA_VERSION,
                "run_id": run_id, "pass_no": pass_no,
                "source_row_number": source_row_number,
                "produced_at": utc_now().isoformat(timespec="milliseconds"),
                "record": record,  # raw CSV strings, exactly as in the file
            }
            while True:
                try:
                    producer.produce(cfg.topic_events, key=record["order_id"].encode(),
                                     value=json.dumps(event).encode(), on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.5)  # local queue full, wait and retry the SAME record
            producer.poll(0)
            sent_this_pass += 1
            sent_total += 1
            if sent_total % 500 == 0:
                rate = sent_total / max(time.monotonic() - started, 1e-6)
                log.info("pass %d: %d/%d sent (%.1f ev/s, confirmed %d)",
                         pass_no, sent_this_pass, expected, rate, delivered["ok"])
            if interval:
                time.sleep(interval)
        log.info("Pass %d finished: %d events sent.", pass_no, sent_this_pass)
        if not args.loop or (args.max_passes and pass_no >= args.max_passes):
            break

    remaining = producer.flush(60)
    complete = (not _stop) and remaining == 0 and delivered["failed"] == 0 \
        and delivered["ok"] == expected * pass_no
    send_control({
        "type": "run_end", "schema_version": SCHEMA_VERSION, "run_id": run_id,
        "published_count": delivered["ok"], "failed_count": delivered["failed"],
        "passes": pass_no, "source_rows": expected, "complete": complete,
        "interrupted": _stop, "sent_at": utc_now().isoformat(timespec="milliseconds"),
    })
    log.info("DONE run=%s published=%d failed=%d passes=%d complete=%s",
             run_id, delivered["ok"], delivered["failed"], pass_no, complete)
    if not complete:
        log.warning("Run is NOT complete (interrupted or delivery failures). verify_data.py will report it.")
        sys.exit(1)


if __name__ == "__main__":
    main()
