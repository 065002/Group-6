# End-to-End Streaming Analytics: Logistics & Last-Mile Delivery

**Python CSV producer → Apache Kafka (KRaft, no ZooKeeper) → Python stream processor → MySQL → Grafana**

Four live, auto-refreshing (5 s) Grafana dashboards at **http://localhost:3000**, fed record by record from
`logistics_streaming_master.csv` (12,000 orders × 33 columns).

| # | Dashboard | Business question |
|---|---|---|
| 1 | Delivery Operations Control Tower | How can operations managers monitor delivery workload, order status and current delivery performance? |
| 2 | SLA Compliance and Delivery Delays | Where and why are delivery commitments being missed? |
| 3 | Fleet and Resource Efficiency | How efficiently are vehicles and delivery resources being utilized? |
| 4 | Last-Mile Risk and Route Intelligence | Which orders, routes and locations are most at risk of late delivery? |

* `docs/DATA_PROFILE.md` – what the CSV contains, data-quality handling, fields that do **not** exist.
* `docs/DASHBOARD_GUIDE.md` – every panel: purpose, exact SQL, visualization, unit, thresholds, interpretation.
* `grafana_queries.sql` – all panel and variable SQL in one file.

---

## Contents

1. [Project layout](#1-project-layout)
2. [Architecture and design](#2-architecture-and-design)
3. [Versions](#3-versions)
4. [Windows 11 step-by-step guide (steps 1–22)](#4-windows-11-step-by-step-guide)
5. [Opening the dashboards](#5-opening-the-dashboards)
6. [Sharing with your professor](#6-sharing-with-your-professor)
7. [Fresh run (repeat the demo)](#7-fresh-run-repeat-the-demo)
8. [Troubleshooting](#8-troubleshooting)
9. [Acceptance checklist](#9-acceptance-checklist)

---

## 1. Project layout

```
logistics-streaming-project\
├── docker-compose.yml          Kafka + MySQL + Grafana (one file)
├── .env.example                copy to .env and set passwords
├── .gitignore
├── requirements.txt
├── common.py                   shared settings / MySQL / Kafka helpers
├── init_schema.py              creates tables (idempotent)
├── producer.py                 CSV  -> Kafka
├── processor.py                Kafka -> MySQL
├── reset_database.py           empties ONLY this project's tables (+ topics with --kafka)
├── verify_data.py              proves all 12,000 rows arrived correctly
├── check_services.py           health check of Kafka, MySQL, Grafana from Windows
├── monitor.py                  live counters while streaming
├── grafana_queries.sql         every dashboard query
├── data\logistics_streaming_master.csv   source dataset (read-only)
├── mysql\init\init.sql         schema
├── grafana\
│   ├── provisioning\datasources\mysql.yml     MySQL data source (auto)
│   ├── provisioning\dashboards\dashboards.yml dashboard loader (auto)
│   └── dashboards\01..04_*.json               the four dashboards (also importable by hand)
├── tools\build_dashboards.py   regenerates dashboards + SQL + guide from one definition
├── docs\DATA_PROFILE.md, docs\DASHBOARD_GUIDE.md
└── logs\                       producer / processor logs
```

The ZIP includes an unchanged copy of your CSV in `data\` (step 4 shows how to check or replace it).

---

## 2. Architecture and design

```
 Windows host (venv)                         Docker Desktop (one compose project: logistics-streaming)
 ┌──────────────┐  127.0.0.1:9092   ┌───────────────────────┐
 │ producer.py  │ ───────────────►  │ logistics-kafka        │  topics: logistics.delivery.events
 └──────────────┘                   │ apache/kafka (KRaft)   │          logistics.pipeline.control
 ┌──────────────┐ ◄───────────────  └───────────────────────┘
 │ processor.py │  127.0.0.1:3307   ┌───────────────────────┐  kafka:19092 (internal listener)
 └──────────────┘ ───────────────►  │ logistics-mysql :3306  │
                                    └──────────▲────────────┘
 Browser  http://localhost:3000     ┌──────────┴────────────┐
 ─────────────────────────────────► │ logistics-grafana:3000 │  data source = mysql:3306
                                    └───────────────────────┘
```

**Kafka listeners.** `EXTERNAL://127.0.0.1:9092` is advertised to the Windows Python scripts; `INTERNAL://kafka:19092` is for containers; `CONTROLLER://kafka:9093` is the KRaft quorum. No ZooKeeper container exists.

**Ports.** Grafana `3000:3000` (fixed). MySQL `127.0.0.1:3307 → 3306`. Kafka `127.0.0.1:9092 → 9092`. MySQL and Kafka are bound to loopback only, so they are never reachable from other computers.

**Producer (`producer.py`).**
1. Reads the CSV (read-only), checks the 33-column header, computes its SHA-256.
2. Sends a `run_start` control message (run id, row count, checksum).
3. Publishes **exactly one event per CSV row** (key = `order_id`, value = JSON with the raw CSV strings, `run_id`, `pass_no`, `source_row_number`, `produced_at`), waiting `STREAM_INTERVAL_MS` between events (default 50 ms → about 10 minutes for 12,000 rows).
4. Default order is chronological (`order_date`+`order_time`) so trend charts fill day by day; `--order file` keeps CSV order.
5. Idempotent Kafka producer (`enable.idempotence`, `acks=all`), waits for every delivery confirmation, then sends `run_end` with the confirmed count and stops.
6. `--loop` replays the file as pass 2, 3, … It is **off by default** and must be requested explicitly.

**Processor (`processor.py`).**
* Manual offset management (`enable.auto.commit=false`).
* Validates every field (types, ranges, Yes/No), derives `order_ts` and `load_utilization_pct`.
* Batch of up to 200 events → one MySQL transaction with parameterized `INSERT`; counters in `pipeline_runs` updated in the same transaction.
* **Kafka offsets are committed only after the MySQL commit succeeds.** On a MySQL error the batch is rolled back and retried with back-off.
* Deterministic key `event_id = run_id|pass_no|order_id` + `UNIQUE(run_id, pass_no, order_id)`: a redelivered event is counted as a *duplicate* and never creates a second row.
* Invalid events go to `rejected_events` with the reason and raw payload, never silently dropped.
* When `run_end` has arrived and `inserted + rejected = published`, the run is marked `COMPLETED`, a summary is printed, and the processor exits.

**MySQL schema (`logistics_streaming_project`).** `delivery_events` (all 33 source columns, same names, plus derived fields and stream metadata), `rejected_events`, `pipeline_runs`. Details in `mysql/init/init.sql`.

**Two kinds of timestamps.**
| Timestamp | Meaning | Used for |
|---|---|---|
| `order_date`, `order_time`, `order_ts`, `estimated_delivery_time`, `actual_delivery_time` | **Event time** from the CSV (September 2026, wall-clock as written in the file) | All delivery-performance analysis: trends by order date, delays, SLA, ETA variance |
| `produced_at`, `ingested_at` | **Processing time** in UTC: when the producer sent / the processor stored the record | Panels titled **Live:**. These follow the Grafana time picker (default *Last 15 minutes*) and show records arriving |

Dashboards use `timezone: browser`, so `ingested_at` (UTC) appears in your local time (IST). Event times are formatted as text, so they appear exactly as in the CSV.

---

## 3. Versions

| Component | Version | Why |
|---|---|---|
| Kafka | `apache/kafka:3.9.1` | Official Apache image, KRaft mode, last 3.x line |
| MySQL | `mysql:8.4` | Current LTS series |
| Grafana | `grafana/grafana:11.6.0` | Stable OSS release with MySQL data source, geomap, provisioning |
| Python | 3.12 or 3.13 (64-bit) | confluent-kafka, PyMySQL and cryptography ship Windows wheels |
| Python packages | see `requirements.txt` | confluent-kafka ≥ 2.6.1, PyMySQL ≥ 1.1.1, cryptography, python-dotenv |

Prerequisites: Windows 11, **Docker Desktop** (WSL 2 backend, running), **Python 3.12/3.13** from python.org (tick *Add python.exe to PATH*), **VS Code**. First start downloads about 1.5 GB of images.

---

## 4. Windows 11 step-by-step guide

Commands are labelled **CMD** (Command Prompt) or **PS** (PowerShell). In VS Code, open a terminal with **Ctrl+`**; the dropdown next to **+** lets you choose *Command Prompt* or *PowerShell*. Paths below assume the ZIP is in your **Downloads** folder and you extract to `C:\Users\<you>\Projects`.

### Step 1 – Download the ZIP
Save `logistics-streaming-project.zip` to `Downloads`.
*Expected:* `C:\Users\<you>\Downloads\logistics-streaming-project.zip` exists.

### Step 2 – Extract
**File Explorer:** right-click the ZIP → *Extract All…* → destination `C:\Users\<you>\Projects` → *Extract*.

**PS:**
```powershell
New-Item -ItemType Directory -Force "$env:USERPROFILE\Projects" | Out-Null
Expand-Archive -Path "$env:USERPROFILE\Downloads\logistics-streaming-project.zip" -DestinationPath "$env:USERPROFILE\Projects" -Force
```
**CMD:**
```bat
mkdir "%USERPROFILE%\Projects"
tar -xf "%USERPROFILE%\Downloads\logistics-streaming-project.zip" -C "%USERPROFILE%\Projects"
```
*Expected:* folder `C:\Users\<you>\Projects\logistics-streaming-project` containing `docker-compose.yml`.
*If it fails:* check the ZIP name; make sure you did not end up with a doubled folder (`...\logistics-streaming-project\logistics-streaming-project`). If so, use the inner one.

### Step 3 – Open in VS Code
**CMD or PS:**
```bat
cd %USERPROFILE%\Projects\logistics-streaming-project
code .
```
(PS: `cd $env:USERPROFILE\Projects\logistics-streaming-project`.) Or *File → Open Folder…*.
*Expected:* VS Code shows the project files. All later commands run **in this folder**.
*If `code` is not recognised:* use *File → Open Folder*.

### Step 4 – Copy the CSV into `data\`
The ZIP already contains a byte-identical copy of your file at `data\logistics_streaming_master.csv` (MD5 `14b7f8497ac7182183a68da4a459b373`). Check it is there with `dir data` (CMD) or `Get-ChildItem data` (PS). Only if it is missing, or you want to use your own copy, overwrite it:

**PS:**
```powershell
Copy-Item "$env:USERPROFILE\Downloads\logistics_streaming_master.csv" .\data\
Get-Item .\data\logistics_streaming_master.csv | Select-Object Name, Length
```
**CMD:**
```bat
copy "%USERPROFILE%\Downloads\logistics_streaming_master.csv" data\
dir data
```
*Expected:* `data\logistics_streaming_master.csv`, about 2.8 MB (2,887,707 bytes).
*If it fails:* adjust the source path to where your CSV actually is. The file name must be exactly `logistics_streaming_master.csv` (or change `SOURCE_CSV` in `.env`).

### Step 5 – Create the virtual environment
**CMD or PS:**
```bat
py -3.12 -m venv .venv
```
*Expected:* a `.venv` folder appears.
*If it fails:* `py -0` lists installed Pythons. Use `py -3.13 -m venv .venv` or `python -m venv .venv`. If none, install Python 3.12 from python.org.

### Step 6 – Activate it
**CMD:**
```bat
.venv\Scripts\activate.bat
```
**PS:**
```powershell
.\.venv\Scripts\Activate.ps1
```
*Expected:* the prompt starts with `(.venv)`.
*If PS says "running scripts is disabled":* run `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` (this terminal only) and activate again. Every new terminal must be activated again.

### Step 7 – Install dependencies
**CMD or PS (venv active):**
```bat
python -m pip install --upgrade pip
pip install -r requirements.txt
python -c "import confluent_kafka, pymysql; print('kafka client', confluent_kafka.version()[0], '| pymysql', pymysql.__version__)"
```
*Expected:* `Successfully installed ...` and a version line.
*If confluent-kafka tries to compile:* your Python is too new or 32-bit. Use 64-bit Python 3.12.

### Step 8 – Create `.env`
**CMD:** `copy .env.example .env`  **PS:** `Copy-Item .env.example .env`
*Expected:* `.env` exists (it is git-ignored and never in the ZIP).

### Step 9 – Set the values
**CMD or PS:** `code .env`
Replace the three `CHANGE_ME_...` values with your own passwords (letters and digits only):
```
MYSQL_ROOT_PASSWORD=...
MYSQL_PASSWORD=...
GRAFANA_ADMIN_PASSWORD=...
```
Leave the rest. `STREAM_INTERVAL_MS=50` gives about a 10-minute demo (12,000 × 50 ms); use 100 for ~20 min or 20 for ~4 min.
*Important:* MySQL reads these passwords **only the first time** its volume is created. If you change them later, see Troubleshooting §8.4.

### Step 10 – Check ports 3000, 3307, 9092 are free
**PS:**
```powershell
Get-NetTCPConnection -LocalPort 3000,3307,9092 -State Listen -ErrorAction SilentlyContinue | Select-Object LocalAddress,LocalPort,OwningProcess
docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Status}}"
```
**CMD:**
```bat
netstat -ano | findstr /R ":3000 :3307 :9092" | findstr LISTENING
docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Status}}"
```
*Expected:* no output from the first command (ports free).
*If something listens:*
* If `docker ps` shows a container publishing that port (e.g. an **old Grafana** with `0.0.0.0:3000->3000/tcp`), stop **only that container**: `docker stop <container-name>`. Do **not** delete volumes.
* If it is a Windows process, find it: CMD `tasklist /FI "PID eq <pid>"` or PS `Get-Process -Id <pid>`. Close that program (for example a locally installed Grafana service: *Services → Grafana → Stop*).
* For 3307 / 9092 only, you may instead change `MYSQL_HOST_PORT` / `KAFKA_HOST_PORT` (and `KAFKA_BOOTSTRAP_SERVERS`) in `.env`. **Port 3000 must stay 3000.**
* See §8.8 for old project containers.

### Step 11 – Start Kafka, MySQL, Grafana
Make sure **Docker Desktop is running** (whale icon says *Engine running*).
**CMD or PS:**
```bat
docker compose up -d
```
*Expected:* images download the first time, then `Container logistics-kafka Started`, `logistics-mysql Started`, `logistics-grafana Started`.
*If it fails:* `error during connect` → start Docker Desktop. `port is already allocated` → step 10. `Set MYSQL_PASSWORD in .env` → step 9.

### Step 12 – Verify the services are healthy
**CMD or PS:**
```bat
docker compose ps
docker port logistics-grafana
```
*Expected (after 30–60 s):* all three `Up ... (healthy)`; `docker port` prints `3000/tcp -> 0.0.0.0:3000`.
Then from the venv:
```bat
python check_services.py
```
*Expected:* `[OK  ]` for the three ports, Kafka broker `127.0.0.1:9092`, MySQL login, Grafana `/api/health`, four dashboards and the data source. (Topic / table lines may fail before step 13–14 — that is normal.)
*If a service stays `starting`/`unhealthy`:* `docker compose logs --tail 80 kafka` (or `mysql`, `grafana`). See §8.

### Step 13 – Initialise the schema
**CMD or PS (venv):**
```bat
python init_schema.py
```
*Expected:* `Schema applied (3 statements, idempotent)` and three tables with `rows=0`.
*If `Access denied`:* §8.4. *If `Can't connect`:* MySQL not healthy yet, wait and retry.

### Step 14 – Start the processor (terminal 1)
**CMD or PS (venv):**
```bat
python processor.py
```
*Expected:* `Subscribed to logistics.delivery.events + logistics.pipeline.control ... Waiting for events...`. Leave it running.
*If `Kafka not reachable`:* §8.3.

### Step 15 – Start the producer (terminal 2)
Open a **second** terminal (**+** in VS Code), `cd` into the project, **activate the venv again** (step 6), then:
```bat
python producer.py
```
*Expected:* `Read 12000 data rows`, `RUN_ID=RUN-YYYYMMDD-HHMMSS rows=12000 ... (~10 min)`, then progress every 500 events. Processor terminal shows `run_start` and `progress:` lines.
*Options:* `python producer.py --interval-ms 100` (slower demo), `--run-id DEMO-01`, `--dry-run` (check CSV only).

### Step 16 – Watch records arrive in Kafka and MySQL
Third terminal (venv):
```bat
python monitor.py
```
*Expected:* every 2 s, Kafka event count and MySQL inserted count rise together; lag stays small. Ctrl+C to stop monitoring.

Optional direct checks (**CMD or PS**):
```bat
docker exec logistics-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic logistics.delivery.events
docker exec logistics-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic logistics.delivery.events --from-beginning --max-messages 2
docker exec logistics-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --describe --group logistics-stream-processor
docker exec -it logistics-mysql mysql -u logistics_user -p logistics_streaming_project -e "SELECT run_id,status,inserted_count,rejected_count FROM pipeline_runs; SELECT COUNT(*) FROM delivery_events;"
```
(the last one asks for your `MYSQL_PASSWORD`).

### Step 17 – Wait for the batch to finish
*Expected:* producer prints `DONE run=... published=12000 failed=0 passes=1 complete=True`. Processor prints a block:
```
RUN RUN-... COMPLETED
  source rows      : 12000
  published events : 12000
  inserted rows    : 12000
  rejected rows    : 0
  duplicates seen  : 0 (ignored, no extra rows)
  accounted        : 12000 / 12000
```
and exits. *If the processor keeps waiting:* the producer did not finish (check its terminal) or was interrupted (then run a fresh run, §7).

### Step 18 – Verify the data
**CMD or PS (venv):**
```bat
python verify_data.py
```
*Expected:* 16 `[PASS]` lines, including *field-by-field match for 12000 stored rows x 33 columns*, and `RESULT: PASS`.
*If any check fails:* the line explains it (e.g. several runs present → do a fresh run §7).

### Step 19 – Open Grafana
Browser: **http://localhost:3000** → log in with `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` from `.env`.
**PS** quick test: `Invoke-RestMethod http://localhost:3000/api/health`  **CMD:** `curl http://localhost:3000/api/health`
*Expected:* JSON with `"database": "ok"` and version `11.6.0`; the home page is the Control Tower.

### Step 20 – Verify (or import) the four dashboards
They are provisioned automatically: *Dashboards → Logistics Streaming Analytics* contains all four. Data source: *Connections → Data sources → Logistics MySQL → Test* → *Database Connection OK*.
**Manual import (only if missing):** *Dashboards → New → Import → Upload dashboard JSON file* → choose `grafana\dashboards\01_...json` (repeat for 02–04) → *Import*. The JSONs reference the data source uid `logistics-mysql`, which provisioning creates.

### Step 21 – Watch live updates and test filters
For the live effect, open the dashboards **while the producer is running** (start a fresh run, §7, if the first one already finished). Every 5 s: totals rise, the **Live:** panels get new bars, the *Latest orders streamed* table scrolls, daily bars fill from 1 Sep to 30 Sep.
Test filters: choose *Region = North*, then the *City* list narrows to Delhi/Gurugram/Lucknow/Noida; pick *Priority = Express*; every panel recalculates. Click *All* to reset. If a new run started and dropdowns look stale, press **F5** once.

### Step 22 – Repeat the demo
See §7: stop scripts → `python reset_database.py --yes --kafka` → processor → producer.

---

## 5. Opening the dashboards

**Home page:** http://localhost:3000 (opens the Control Tower). Direct links:

| Dashboard | URL |
|---|---|
| 1 Delivery Operations Control Tower | http://localhost:3000/d/logistics-ops-control-tower |
| 2 SLA Compliance and Delivery Delays | http://localhost:3000/d/logistics-sla-delays |
| 3 Fleet and Resource Efficiency | http://localhost:3000/d/logistics-fleet-efficiency |
| 4 Last-Mile Risk and Route Intelligence | http://localhost:3000/d/logistics-risk-route |

Each dashboard also has links to the other three in the top-right corner (filters are carried along).

---

## 6. Sharing with your professor

**`http://localhost:3000` works only on the computer that runs the project.** "localhost" always means "this machine", so if your professor clicks that link on their own computer it will try to open *their* computer and fail. That is expected, not a bug.

Options to let them see it, from simplest to most involved:

1. **Present from your laptop (recommended).** Open http://localhost:3000 on your laptop and show it in class, or share your screen on Teams/Zoom/Meet. Fully live, nothing exposed to the network.
2. **Same Wi-Fi / campus LAN.** Grafana is published on all interfaces of your PC, so on the same network it is reachable at `http://<your-PC-IPv4>:3000`.
   * Find your IP: **CMD** `ipconfig` → *IPv4 Address* of the Wi-Fi adapter (e.g. `192.168.1.25`).
   * Allow Docker Desktop through Windows Defender Firewall on **Private** networks if prompted.
   * Create a **Viewer** account for the professor (*Administration → Users → New user*, role *Viewer*) rather than sharing the admin password.
   * Campus networks often block device-to-device traffic, so this may not work there.
   * MySQL (3307) and Kafka (9092) stay bound to `127.0.0.1` and are **not** reachable from the network.
3. **Static snapshot (not live).** In a dashboard: *Share → Snapshot* (local) or *Export → Save to file* (JSON), or take screenshots/PDF. Good as a backup for submission.
4. **Public tunnel (optional, needs your explicit decision).** Tools such as Cloudflare Tunnel or ngrok can give a temporary public HTTPS URL that forwards only to port 3000. This puts your Grafana on the internet. Use a strong admin password plus a Viewer account, share the link only with the professor, and stop the tunnel after the session. It is **not** configured in this project; only do it if you choose to.

---

## 7. Fresh run (repeat the demo)

Terminals: **CMD or PS**, venv active, in the project folder.

1. **Stop the scripts safely.** Press **Ctrl+C** once in the producer terminal (it flushes and sends `run_end`) and once in the processor terminal (it finishes the current batch, commits, and closes). If they already finished, nothing to do.
2. **Reset only this project's data:**
   ```bat
   python reset_database.py --yes --kafka
   ```
   Empties `delivery_events`, `rejected_events`, `pipeline_runs` in `logistics_streaming_project`, deletes and re-creates only the two project topics, and deletes only the `logistics-stream-processor` consumer group. Containers and volumes are untouched. (Without `--yes` it asks you to type the database name.)
   *If it says the consumer group could not be deleted:* the processor is still running, stop it and wait 10 s.
3. **Start the processor:** `python processor.py`
4. **Start the producer** (second terminal): `python producer.py` (optionally `--run-id DEMO-PROF`).
5. **Verify after it completes:** `python verify_data.py` → `RESULT: PASS`, including *only this run is in the database*.

**Confirming the dashboards show the current run:** the *Run* dropdown shows `Latest run (auto)`; the list contains the run id printed by the producer, and after a reset it is the only one. *Processed events (run)* and *Stream progress* on dashboard 1 restart from 0 for the new run.

Never use `docker compose down -v` for a routine reset (it deletes the MySQL and Grafana volumes). Use `docker compose stop` / `docker compose start` to pause and resume the stack; data is kept and **not** cleared on restart.

---

## 8. Troubleshooting

### 8.1 Useful status commands (CMD or PS)
```bat
docker compose ps
docker compose logs --tail 100 kafka
docker compose logs --tail 100 mysql
docker compose logs --tail 100 grafana
python check_services.py
```

### 8.2 Stopping safely
Ctrl+C in producer/processor terminals (see §7). Stop the stack without losing data: `docker compose stop`. Start again: `docker compose start`.

### 8.3 Kafka listener / connection problems
* `Kafka not reachable at 127.0.0.1:9092` → `docker compose ps`: is `logistics-kafka` healthy? If not, `docker compose logs kafka`.
* Logs show `Connection to node 1 (localhost/127.0.0.1:9092) could not be established` from **inside** a container → expected only if something other than our scripts uses the external listener; containers must use `kafka:19092`.
* Port changed? `KAFKA_HOST_PORT` and `KAFKA_BOOTSTRAP_SERVERS` in `.env` must match, then `docker compose up -d` to recreate Kafka.
* Kafka keeps restarting after a crash: `docker compose restart kafka`. Kafka data is in the container's temp dir by design; if you **recreate** the Kafka container, topics are recreated automatically on the next producer/processor start. MySQL data is unaffected.

### 8.4 MySQL authentication or connection failures
* `Access denied for user 'logistics_user'` usually means `.env` passwords were changed **after** the MySQL volume was first created (MySQL only reads them on first initialisation). Fix without losing anything else: set `MYSQL_PASSWORD` in `.env` back to the original value, **or** change the user's password:
  ```bat
  docker exec -it logistics-mysql mysql -u root -p -e "ALTER USER 'logistics_user'@'%' IDENTIFIED BY 'NEW_PASSWORD';"
  ```
  (enter the root password you used the first time), then put `NEW_PASSWORD` in `.env` and run `docker compose up -d grafana` so Grafana picks it up.
* `Can't connect to MySQL server on '127.0.0.1:3307'` → container not healthy yet, or another program uses 3307 (step 10).
* `Unknown database` → the volume was created by another project with a different `MYSQL_DATABASE`. Check with `docker volume ls`; this project's volume is `logistics_streaming_mysql_data`.
* `cryptography package is required` → `pip install -r requirements.txt` inside the venv.

### 8.5 Grafana data-source errors
* *Connections → Data sources → Logistics MySQL → Test.* It must use host `mysql:3306` (Docker network name and internal port, **not** `localhost:3307`).
* `Access denied` → same fix as 8.4, then `docker compose up -d grafana` (provisioning re-reads the password at container start).
* Panels show `db query error` mentioning a column → you edited a query; restore with `docker compose restart grafana` (files win on restart).

### 8.6 Empty dashboard vs broken query
* **Empty but healthy:** panels show `No data` (grey), not a red triangle. Causes: no run yet, a filter excludes everything (set dropdowns to *All*), or a **Live:** panel's time range does not include when the data was ingested (use the time picker → *Last 1 hour*/*Last 24 hours*).
* **Broken:** a red ⚠ in the panel corner. Hover it to read the MySQL error, or *Edit panel → Query inspector → Refresh* to see the exact SQL sent.
* Database check: `python check_services.py` shows table row counts and recent runs.

### 8.7 Old dashboard, blank page, connection error, wrong Grafana
* **Connection refused at localhost:3000** → `docker compose ps` (is `logistics-grafana` up?), `docker port logistics-grafana` (must show `3000/tcp -> 0.0.0.0:3000`).
* **Wrong/old Grafana appears** (other dashboards, other login) → another Grafana owns port 3000. **PS** `docker ps --filter "publish=3000"` shows which container. Stop it: `docker stop <name>`, then `docker compose up -d grafana`. If no container is listed, a Windows Grafana service may be running: CMD `netstat -ano | findstr :3000` → `tasklist /FI "PID eq <pid>"`.
* **Blank page / old content** → hard refresh **Ctrl+F5** or open a private window; clear site data for `localhost:3000`.
* **Dashboards missing** → `docker compose logs grafana | findstr /I provisioning` (CMD) or `... | Select-String provisioning` (PS); restart: `docker compose restart grafana`; or import manually (step 20).

### 8.8 Finding old project containers (without deleting anything)
```bat
docker compose ls
docker ps -a --format "table {{.Names}}\t{{.Image}}\t{{.Ports}}\t{{.Status}}"
```
This project's containers are exactly `logistics-kafka`, `logistics-mysql`, `logistics-grafana` (compose project `logistics-streaming`). If an *older* version of your project is running, stop it from its own folder with `docker compose stop`, or stop its containers by name with `docker stop <name>`. Removing a container is optional (`docker rm <name>`); never remove volumes to fix a port conflict.

### 8.9 Docker services restarted unexpectedly
All three use `restart: unless-stopped` and recover by themselves. Then:
1. `docker compose ps` until all are healthy.
2. If the processor stopped with an error, start it again: `python processor.py`. It resumes from the last **committed** offset; anything already written is recognised as a duplicate, so no row is doubled.
3. If the producer was interrupted, its run is incomplete (`verify_data.py` will say so). Do a fresh run (§7).

### 8.10 Proving the dashboards show the current run
* `python verify_data.py` → *only this run is in the database* = PASS.
* Grafana *Run* dropdown = `Latest run (auto)`, list contains only your new run id.
* SQL (MySQL): `SELECT run_id, status, inserted_count FROM pipeline_runs;`

---

## 9. Acceptance checklist

**Executed by Claude in a Linux sandbox before packaging** (Docker images and Kafka/Grafana downloads were blocked there, so a real MySQL 8.0.46 server was used and Kafka was replaced by an in-memory stand-in of the confluent-kafka API):

- [x] CSV inspected in full before implementation (`docs/DATA_PROFILE.md`); file only read, never modified.
- [x] `producer.py` + `processor.py` end-to-end on real MySQL: 12,000 published, 12,000 inserted, 0 rejected, run `COMPLETED`.
- [x] `verify_data.py`: 16/16 PASS, including field-by-field comparison of all 12,000 rows × 33 columns against the CSV.
- [x] Redelivery test: 300 events re-consumed → 300 counted as duplicates, still exactly 12,000 rows. A malformed event and a non-JSON message → stored in `rejected_events` and counted.
- [x] Simulated offset-commit failure handled without data loss.
- [x] `reset_database.py` empties only the three project tables; verification then reports a single run.
- [x] All 68 panel queries + 6 variable queries executed on MySQL with *All* filters, with specific multi-value filters, and on an empty database: 0 errors. (This caught and fixed one type bug in the date filter.)
- [x] `docker compose config` validates; Grafana published as `3000:3000`, MySQL `127.0.0.1:3307→3306`, Kafka `127.0.0.1:9092`.
- [x] Dashboard JSON is valid, panel ids unique, layout within 24 columns, refresh `5s` on all four.

**Not executed by Claude — please confirm on your Windows PC** (steps 11–21):

- [ ] Images pull and all three containers report `healthy`.
- [ ] Kafka KRaft broker accepts the Windows clients on `127.0.0.1:9092` (the `apache/kafka` listener setup was not run).
- [ ] Grafana 11.6 loads the provisioned data source and four dashboards; panels render (geomap needs internet for map tiles).
- [ ] Live updates are visible every 5 s during streaming.
- [ ] `verify_data.py` shows `RESULT: PASS` after your run.

**Dataset limitations reflected in the dashboards:** no fuel consumption, cost, driver ratings or route IDs; `zone` inconsistent with `city`; vehicle fields are per-order snapshots. Replacements are documented on dashboards 3 and 4 and in `docs/DATA_PROFILE.md`.
