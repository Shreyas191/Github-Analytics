# Implementation Guide

Step-by-step setup for running the GitHub Developer Ecosystem Analytics platform end-to-end.

> Prerequisites: Docker (with Compose v2), `curl`, ~10 GB free disk, 4 GB free RAM. WSL2 users: enable Docker Desktop's WSL integration for your distro.

## 1. Prepare environment

```bash
git clone https://github.com/Shreyas191/Github-Analytics.git
cd Github-Analytics
cp .env.example .env
```

Edit `.env` and fill in at least one PAT. The producer skips empty PATs and runs with however many are set (1 PAT = 5k req/hr, 4 PATs = 20k req/hr).

```ini
PAT_HARIHARAN=ghp_xxxxxxxxxxxxxxxxxxxx
PAT_SHREYAS=
PAT_TANUSHA=
PAT_VIKRAM=
```

## 2. Bring up the stack

```bash
docker compose up -d
```

First run downloads ~6 GB of images (Hadoop ×4, Spark, Kafka, Zookeeper, Airflow, Postgres, InfluxDB, Grafana). Plan ~10–15 min. Subsequent runs start in under a minute.

Verify all 15 services are healthy:

```bash
docker compose ps
```

Web UIs (login table is in the README):

| URL | Service |
|---|---|
| http://localhost:9870 | HDFS NameNode |
| http://localhost:8088 | YARN ResourceManager |
| http://localhost:8080 | Spark Master |
| http://localhost:8090 | Airflow |
| http://localhost:3000 | Grafana |
| http://localhost:8086 | InfluxDB |

## 3. Apply the analytics schema

```bash
docker exec -i postgres-analytics psql -U analytics -d github_analytics \
  < analytics/db_schema.sql
```

Creates `geo_contributions`, `ecosystem_health`, `language_velocity`, `language_rankings`, `pmi_pairs`, `language_nodes`.

## 4. Streaming pipeline (live GitHub events → Grafana)

### 4a. Create Kafka topics (once)

```bash
for t in push-events watch-events fork-events pr-events issue-events create-events pending-repos; do
  docker exec kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic "$t" \
    --partitions 4 \
    --replication-factor 1 \
    --config retention.ms=172800000
done
```

### 4b. Run the producer + metrics emitter (containerised, shared stats file)

Both processes need to see the same `stream03_stats.json` file, so we mount a shared host directory into each.

```bash
mkdir -p /tmp/gh-share

docker run -d --name gh-producer \
  --network github-analytics_bigdata-net \
  --restart unless-stopped \
  -v "$PWD/streaming":/app -v /tmp/gh-share:/share -w /app \
  --env-file .env \
  -e KAFKA_BOOTSTRAP=kafka:9092 \
  -e PRODUCER_STATS_PATH=/share/stream03_stats.json \
  -e PYTHONUNBUFFERED=1 \
  python:3.11-slim \
  bash -c "pip install --quiet kafka-python==2.0.2 requests==2.31.0 && python -u stream03_producer.py"

docker run -d --name gh-metrics \
  --network github-analytics_bigdata-net \
  --restart unless-stopped \
  -v "$PWD/streaming":/app -v /tmp/gh-share:/share -w /app \
  -e KAFKA_BOOTSTRAP=kafka:9092 \
  -e INFLUXDB_URL=http://influxdb:8086 \
  -e INFLUXDB_TOKEN=github-analytics-token \
  -e INFLUXDB_ORG=github-analytics \
  -e INFLUXDB_BUCKET=github-stream \
  -e PRODUCER_STATS_PATH=/share/stream03_stats.json \
  -e PYTHONUNBUFFERED=1 \
  python:3.11-slim \
  bash -c "pip install --quiet kafka-python==2.0.2 influxdb-client==1.38.0 && python -u stream07_metrics.py"
```

Tail logs:

```bash
docker logs -f gh-producer
docker logs -f gh-metrics
```

> **Auto-restart note:** `--restart unless-stopped` keeps these containers up across Docker daemon restarts and transient failures (we hit a `NoBrokersAvailable` once during testing — auto-restart recovers cleanly).

### 4c. Submit Spark Structured Streaming consumer

```bash
docker exec spark-master pip3 install --quiet influxdb-client
docker cp streaming spark-master:/opt/spark-apps/streaming

docker exec -d \
  -e KAFKA_BOOTSTRAP=kafka:9092 \
  -e INFLUXDB_URL=http://influxdb:8086 \
  spark-master bash -c "cd /opt/spark-apps/streaming && \
    /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      --conf spark.jars.ivy=/tmp/.ivy \
      --total-executor-cores 1 \
      stream05_spark_streaming.py > /tmp/stream05.log 2>&1"
```

`--total-executor-cores 1` reserves 1 core so the batch parser can co-exist on the single-worker dev setup.

> **Supervision gap:** the streaming job is launched via `docker exec -d` and is not supervised. If it crashes, `docker exec spark-master tail /tmp/stream05.log` for the cause and re-run the same `spark-submit` command. For production use, switch to `--deploy-mode cluster --supervise`.

## 5. Batch pipeline (GHArchive → Parquet → analytics)

### 5a. Download a sample slice

GHArchive is ~50–70 MB per hour. Two consecutive hours are enough for a demo.

```bash
mkdir -p /tmp/gharchive && cd /tmp/gharchive
curl -s -o 2024-06-15-12.json.gz https://data.gharchive.org/2024-06-15-12.json.gz
curl -s -o 2024-06-15-13.json.gz https://data.gharchive.org/2024-06-15-13.json.gz
```

For the proposal-scope full run (2023–2025, ~120 GB), use `gsutil` against `gs://data.gharchive.org/` and submit to a Dataproc cluster.

### 5b. Push to HDFS

```bash
docker exec namenode hdfs dfs -mkdir -p /github/events/raw
docker cp /tmp/gharchive/2024-06-15-12.json.gz namenode:/tmp/
docker cp /tmp/gharchive/2024-06-15-13.json.gz namenode:/tmp/
docker exec namenode hdfs dfs -put -f /tmp/2024-06-15-12.json.gz \
  /tmp/2024-06-15-13.json.gz /github/events/raw/
```

### 5c. Parse to partitioned Parquet (INFRA-04)

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy \
  --py-files /opt/spark-apps/batch/schema.py \
  /opt/spark-apps/batch/infra04_batch_parser.py
```

### 5d. Validation report (INFRA-05)

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy \
  /opt/spark-apps/batch/infra05_validation_report.py
```

### 5e. Language enrichment (VIZ-02)

`viz02_enrichment.py` produces F5 (`f5_owner_star_med`) and F6 (`f6_readme_bytes`) for ML-03 — the proposal's overnight enrichment job for the ML training set.

For **repository-language** enrichment specifically (needed by VIZ-05/07/08), GitHub's secondary rate limit caps sustained calls at ~80 req/min on top of the per-PAT 5000 req/hr quota. Plan ~5 hours per 30k repos.

> **Demo caveat:** the live demo uses a hybrid enrichment for the top 600 most-active repos: real REST API responses for as many as time allowed (typically 19–50 of 600 before secondary-limit kick-in), with the remainder filled deterministically from a realistic distribution (Python 18%, JavaScript 17%, TypeScript 12%, Go 8%, Java 8%, Rust 6%, …) keyed off SHA-256 of `repo_name`. Match counts in the dashboard reflect this — the analytics pipeline runs end-to-end, but only the real-API subset is ground-truth. For a clean run on the full proposal scope, allocate ~24h of dedicated PAT budget against `viz02_enrichment.py` modified to pull `language` from `/repos/{owner}/{name}` instead of (or alongside) F5/F6.

### 5f. Run the analytics jobs

```bash
docker exec spark-master pip3 install --quiet requests psycopg2-binary
docker exec spark-worker pip3 install --quiet requests influxdb-client psycopg2-binary

# VIZ-04 — geo heatmap (REST resolution; use --sample for fast demo)
ENV_FILE=.env
docker exec -d \
  -e PAT_HARIHARAN=$(grep ^PAT_HARIHARAN= $ENV_FILE | cut -d= -f2) \
  -e PAT_TANUSHA=$(grep ^PAT_TANUSHA= $ENV_FILE | cut -d= -f2) \
  -e PAT_VIKRAM=$(grep ^PAT_VIKRAM= $ENV_FILE | cut -d= -f2) \
  spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.postgresql:postgresql:42.6.0 \
    --conf spark.jars.ivy=/tmp/.ivy \
    /opt/spark-apps/analytics/viz04_geo_heatmap.py --sample

# VIZ-05 / VIZ-07 / VIZ-08 — PMI, health score, language velocity
for s in viz05_pmi_cooccurrence viz07_health_score viz08_language_velocity; do
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.postgresql:postgresql:42.6.0 \
    --conf spark.jars.ivy=/tmp/.ivy \
    /opt/spark-apps/analytics/${s}.py
done
```

### 5g. Open the dashboards

- Live: http://localhost:3000/d/github-live-stream
- Batch: http://localhost:3000/d/github-batch-analytics

If panels say "No data", set the time range to **Last 1 hour** (top-right time picker). The streaming consumer's first window emission is 5 minutes after start.

## 6. Airflow DAGs

Both DAGs are auto-loaded from `batch/dags/`:

- `infra06_daily_ingestion` (`@daily`) — incremental hourly archive download → parse → append
- `infra07_weekly_retraining` (`@weekly`) — refresh features → split → train → verify → notify

Trigger manually from http://localhost:8090 (admin/admin) or wait for the schedule.

## 7. Stop everything

```bash
docker rm -f gh-producer gh-metrics
docker compose down            # keeps HDFS + Postgres + InfluxDB volumes
docker compose down -v         # also wipes data volumes
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `dependency failed to start: container zookeeper is unhealthy` on first `up` | Run `docker compose up -d` again — health-checks were too slow during cold start; underlying services come up fine on retry. |
| Spark job hangs at `Initial job has not accepted any resources` | Worker is fully consumed by another app. Stop the existing app (`docker exec spark-master pkill -f spark-submit`) or pass `--total-executor-cores 1` to share. |
| `kafka.vendor.six.moves` ModuleNotFoundError | `kafka-python==2.0.2` is broken on Python 3.12. Use `python:3.11-slim` for the producer/metrics containers. |
| Grafana panels say "No data" | Default time range is 5 minutes. Switch to **Last 1 hour**. |
| `language_rankings` table empty after viz08 | YoY computation needs multi-year data. Single-day samples produce no rankings. |
| `events_received: 0` in stream07 metrics table | Producer and metrics container aren't sharing the stats file. Both must mount the same host directory and set `PRODUCER_STATS_PATH` to a path inside it. |
