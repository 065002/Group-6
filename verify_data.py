"""Verify that a streaming run loaded the source CSV completely and correctly.

Checks (for the latest run, or --run-id):
  1. Source CSV: row count, unique order_ids, sha256 (file is only read).
  2. pipeline_runs: status, published / inserted / duplicate / rejected counters.
  3. Accounting: inserted + rejected == published == source rows.
  4. Table counts match the counters (no hidden rows).
  5. No duplicate (run, pass, order_id) rows in delivery_events.
  6. Every source order_id is present (or explicitly rejected); no extra ids.
  7. Field-by-field comparison of EVERY stored row against the CSV.
  8. Only one run in the database (so dashboards cannot mix runs).

Exit code 0 = PASS, 1 = FAIL.     Usage:  python verify_data.py  [--run-id X]
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import sys
from datetime import date, datetime, time as dtime
from decimal import Decimal

from common import EXPECTED_COLUMNS, load_settings, mysql_connect, setup_logging

FLAGS = {"sla_breach", "at_risk"}


def norm_source(col, value):
    """Normalise a CSV string the same way the processor converts it."""
    v = value.strip()
    if v == "":
        return None
    if col in FLAGS:
        return 1 if v.lower() in {"yes", "y", "true", "1"} else 0
    if col in {"latitude", "longitude", "driver_experience_years", "delivery_distance_km",
               "speed_kmph", "fuel_level_pct", "package_weight_kg"}:
        return Decimal(v).normalize()
    if col in {"vehicle_capacity_kg", "delay_minutes", "eta_variance_minutes", "delivery_attempts"}:
        return int(Decimal(v))
    if col in {"estimated_delivery_time", "actual_delivery_time"}:
        return datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
    if col == "order_date":
        return date.fromisoformat(v)
    if col == "order_time":
        return dtime.fromisoformat(v)
    return v


def norm_db(col, value):
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value.normalize()
    if col == "order_time":  # PyMySQL returns TIME as timedelta
        secs = int(value.total_seconds())
        return dtime(secs // 3600, (secs % 3600) // 60, secs % 60)
    return value


def main():
    cfg = load_settings()
    p = argparse.ArgumentParser()
    p.add_argument("--run-id")
    args = p.parse_args()
    log = setup_logging("verify_data", cfg.log_level)
    results = []

    def check(name, ok, detail=""):
        results.append(ok)
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  -> {detail}" if detail else ""))

    # ---------------- source ----------------
    path = cfg.source_csv
    if not path.exists():
        sys.exit(f"Source CSV not found: {path}")
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        header = reader.fieldnames
        source = list(reader)
    src_ids = [r["order_id"] for r in source]
    print("\nSOURCE CSV")
    print(f"  file      : {path}")
    print(f"  rows      : {len(source)}   columns: {len(header)}   sha256: {sha[:16]}...")
    check("CSV header has the 33 expected columns", header == EXPECTED_COLUMNS)
    check("order_id unique in CSV", len(set(src_ids)) == len(src_ids),
          f"{len(src_ids) - len(set(src_ids))} duplicates")

    conn = mysql_connect(cfg, log, retries=3)
    cur = conn.cursor()
    cur.execute("SELECT run_id FROM pipeline_runs WHERE run_id <> 'UNKNOWN' ORDER BY started_at DESC")
    runs = [r[0] for r in cur.fetchall()]
    if not runs:
        print("\nNo runs in pipeline_runs yet. Start processor.py and producer.py first.")
        sys.exit(1)
    run_id = args.run_id or runs[0]
    cur.execute("""SELECT status, source_rows, source_sha256, published_count, producer_complete,
                          inserted_count, duplicate_count, rejected_count, loop_mode,
                          started_at, completed_at
                   FROM pipeline_runs WHERE run_id=%s""", (run_id,))
    row = cur.fetchone()
    if not row:
        sys.exit(f"Run {run_id} not found. Known runs: {runs}")
    (status, src_rows, src_sha, published, prod_ok, ins, dup, rej, loop_mode, started, completed) = row

    print(f"\nRUN {run_id}")
    print(f"  status={status}  started={started} UTC  completed={completed} UTC")
    print(f"  source_rows={src_rows}  published={published}  inserted={ins}  "
          f"rejected={rej}  duplicates_ignored={dup}")

    cur.execute("SELECT COUNT(*), COUNT(DISTINCT order_id), COUNT(DISTINCT pass_no) "
                "FROM delivery_events WHERE run_id=%s", (run_id,))
    db_rows, db_distinct, passes = cur.fetchone()
    cur.execute("SELECT COUNT(*) FROM rejected_events WHERE run_id=%s", (run_id,))
    db_rej = cur.fetchone()[0]
    cur.execute("""SELECT COUNT(*) FROM (SELECT 1 FROM delivery_events WHERE run_id=%s
                   GROUP BY pass_no, order_id HAVING COUNT(*)>1) d""", (run_id,))
    dup_groups = cur.fetchone()[0]
    print(f"  delivery_events rows={db_rows} (distinct order_id={db_distinct}, passes={passes})  "
          f"rejected_events rows={db_rej}")

    print("\nCHECKS")
    check("run status is COMPLETED", status == "COMPLETED", status)
    check("producer published every source row", bool(prod_ok))
    check("run used the same CSV (sha256 matches)", src_sha == sha,
          "different file!" if src_sha != sha else "")
    check("source_rows == rows in CSV", src_rows == len(source), f"{src_rows} vs {len(source)}")
    check("inserted + rejected == published", published is not None and ins + rej == published,
          f"{ins} + {rej} vs {published}")
    if not loop_mode:
        check("published == CSV rows (one event per source row)", published == len(source),
              f"{published} vs {len(source)}")
    check("delivery_events count == inserted counter", db_rows == ins, f"{db_rows} vs {ins}")
    check("rejected_events count == rejected counter", db_rej == rej, f"{db_rej} vs {rej}")
    check("no duplicate rows in delivery_events", dup_groups == 0, f"{dup_groups} duplicate keys")

    cur.execute("SELECT order_id FROM rejected_events WHERE run_id=%s", (run_id,))
    rejected_ids = {r[0] for r in cur.fetchall()}
    cur.execute(f"SELECT {', '.join(EXPECTED_COLUMNS)} FROM delivery_events "
                f"WHERE run_id=%s AND pass_no=1", (run_id,))
    stored = {r[0]: r for r in cur.fetchall()}
    missing = [i for i in src_ids if i not in stored and i not in rejected_ids]
    extra = [i for i in stored if i not in set(src_ids)]
    check("every source order_id stored or explicitly rejected", not missing,
          f"{len(missing)} missing e.g. {missing[:5]}")
    check("no order_ids that are not in the CSV", not extra, f"{len(extra)} extra e.g. {extra[:5]}")

    mismatches = []
    for src in source:
        db = stored.get(src["order_id"])
        if db is None:
            continue
        for i, col in enumerate(EXPECTED_COLUMNS):
            if norm_source(col, src[col]) != norm_db(col, db[i]):
                mismatches.append((src["order_id"], col, src[col], db[i]))
    check(f"field-by-field match for {len(stored)} stored rows x 33 columns", not mismatches,
          f"{len(mismatches)} differences e.g. {mismatches[:3]}")

    check("only this run is in the database (no mixing with old runs)", len(runs) == 1,
          f"{len(runs)} runs present: {runs[:5]}. Dashboards default to the latest run; "
          "use reset_database.py before a fresh demo." if len(runs) > 1 else "")

    cur.execute("SELECT COUNT(*) FROM rejected_events WHERE run_id='UNKNOWN'")
    unknown = cur.fetchone()[0]
    check("no unreadable (non-JSON / no run_id) messages", unknown == 0,
          f"{unknown} rows in rejected_events with run_id UNKNOWN")

    if rej:
        cur.execute("SELECT order_id, reason FROM rejected_events WHERE run_id=%s LIMIT 10", (run_id,))
        print("\nRejected examples:")
        for oid, reason in cur.fetchall():
            print(f"  {oid}: {reason}")
    conn.close()

    passed = all(results)
    print(f"\nRESULT: {'PASS' if passed else 'FAIL'}  ({sum(results)}/{len(results)} checks passed)\n")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
