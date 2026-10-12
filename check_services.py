"""Health check for Kafka, MySQL and Grafana as seen FROM WINDOWS.

Usage (venv active):  python check_services.py
"""
from __future__ import annotations

import base64
import json
import os
import socket
import sys
import urllib.request

from common import PROJECT_TABLES, load_settings, mysql_connect, setup_logging

EXPECTED_DASHBOARDS = {
    "logistics-ops-control-tower": "1. Delivery Operations Control Tower",
    "logistics-sla-delays": "2. SLA Compliance and Delivery Delays",
    "logistics-fleet-efficiency": "3. Fleet and Resource Efficiency",
    "logistics-risk-route": "4. Last-Mile Risk and Route Intelligence",
}


def port_open(host, port):
    try:
        with socket.create_connection((host, port), timeout=3):
            return True
    except OSError:
        return False


def main():
    cfg = load_settings()
    log = setup_logging("check_services", "WARNING")
    ok_all = True

    def report(name, ok, detail=""):
        nonlocal ok_all
        ok_all &= ok
        print(f"  [{'OK  ' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))

    print("\nPORTS (Windows host)")
    kh, kp = cfg.kafka_bootstrap.split(",")[0].rsplit(":", 1)
    report(f"Kafka   {kh}:{kp}", port_open(kh, int(kp)))
    report(f"MySQL   {cfg.mysql_host}:{cfg.mysql_port}", port_open(cfg.mysql_host, cfg.mysql_port))
    report("Grafana localhost:3000", port_open("127.0.0.1", 3000))

    print("\nKAFKA")
    try:
        from confluent_kafka.admin import AdminClient
        md = AdminClient({"bootstrap.servers": cfg.kafka_bootstrap}).list_topics(timeout=8)
        report("broker metadata", True, f"{len(md.brokers)} broker(s): "
               + ", ".join(f"{b.host}:{b.port}" for b in md.brokers.values()))
        for t in (cfg.topic_events, cfg.topic_control):
            report(f"topic {t}", t in md.topics, "" if t in md.topics else "(created on first producer/processor start)")
    except Exception as exc:
        report("broker metadata", False, str(exc))

    print("\nMYSQL")
    try:
        conn = mysql_connect(cfg, log, retries=2, delay=1)
        with conn.cursor() as cur:
            cur.execute("SELECT VERSION(), DATABASE()")
            v, db = cur.fetchone()
            report("login + database", True, f"MySQL {v}, db={db}, user={cfg.mysql_user}")
            for t in PROJECT_TABLES:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM `{t}`")
                    report(f"table {t}", True, f"{cur.fetchone()[0]} rows")
                except Exception:
                    report(f"table {t}", False, "missing -> run: python init_schema.py")
            cur.execute("SELECT run_id, status, inserted_count, rejected_count FROM pipeline_runs "
                        "ORDER BY started_at DESC LIMIT 3")
            for r in cur.fetchall():
                print(f"         run {r[0]}: {r[1]} inserted={r[2]} rejected={r[3]}")
        conn.close()
    except SystemExit as exc:
        report("login + database", False, str(exc))

    print("\nGRAFANA  (http://localhost:3000)")
    try:
        with urllib.request.urlopen("http://localhost:3000/api/health", timeout=5) as r:
            health = json.loads(r.read())
        report("/api/health", health.get("database") == "ok", f"Grafana {health.get('version')}")
        user = os.getenv("GRAFANA_ADMIN_USER", "admin")
        pwd = os.getenv("GRAFANA_ADMIN_PASSWORD", "")
        auth = base64.b64encode(f"{user}:{pwd}".encode()).decode()
        req = urllib.request.Request("http://localhost:3000/api/search?type=dash-db",
                                     headers={"Authorization": f"Basic {auth}"})
        with urllib.request.urlopen(req, timeout=5) as r:
            found = {d["uid"]: d for d in json.loads(r.read())}
        for uid, title in EXPECTED_DASHBOARDS.items():
            d = found.get(uid)
            report(f"dashboard {title}", d is not None,
                   f"http://localhost:3000{d['url']}" if d else "not provisioned")
        req = urllib.request.Request("http://localhost:3000/api/datasources/uid/logistics-mysql/health",
                                     headers={"Authorization": f"Basic {auth}"})
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                ds = json.loads(r.read())
            report("MySQL data source", ds.get("status") == "OK", ds.get("message", ""))
        except urllib.error.HTTPError as exc:
            report("MySQL data source", False, f"HTTP {exc.code}: {exc.read()[:200]!r}")
    except urllib.error.HTTPError as exc:
        report("Grafana API", False, f"HTTP {exc.code} (check GRAFANA_ADMIN_PASSWORD in .env)")
    except Exception as exc:
        report("Grafana", False, str(exc))

    print(f"\nOVERALL: {'ALL OK' if ok_all else 'SOME CHECKS FAILED'}\n")
    sys.exit(0 if ok_all else 1)


if __name__ == "__main__":
    main()
