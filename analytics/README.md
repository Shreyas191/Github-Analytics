# Analytics and Visualization -- Vikram Markali (vrm9190)

VIZ-01 through VIZ-09.

## Ticket Status

| Ticket | Title | Week | Status |
|--------|-------|------|--------|
| VIZ-01 | Grafana data store decision | 1 | Done |
| VIZ-02 | REST API enrichment lookup table (F5 + F6) | 1-2 | Done |
| VIZ-03 | Grafana dashboards -- leaderboard + trending repos | 3 | Pending STREAM-05 |
| VIZ-04 | Geographic contribution heatmap | 3 | Pending |
| VIZ-05 | PMI co-occurrence computation | 3 | Pending |
| VIZ-06 | PMI force-directed graph | 4 | Pending VIZ-05 |
| VIZ-07 | Ecosystem Health Score | 3-4 | Pending |
| VIZ-08 | Language velocity rankings report | 4 | Pending |
| VIZ-09 | Final report + all deliverables | 5 | Pending |

---

## VIZ-01: Data Store Decision

**Confirmed: Option A.**

- **InfluxDB 2.7** -- time-series store for Shreyas's STREAM-05 streaming sink.
  Grafana reads from here for live panels (star velocity, language momentum).
- **postgres-analytics** -- batch outputs (PMI pairs, Health Score, language velocity,
  geo contributions). Separate from airflow-db so Grafana queries never compete
  with Airflow metadata reads.
- **Grafana 10.2** -- dashboard layer. Both datasources provisioned at startup
  via `grafana/provisioning/datasources/datasources.yml` -- no manual setup needed.

### Start services

```bash
# Start all data store services (includes InfluxDB, postgres-analytics, Grafana):
docker compose up -d influxdb postgres-analytics grafana

# Check they are healthy:
docker compose ps

# Verify InfluxDB is up:
curl http://localhost:8086/ping

# Open Grafana (both datasources should be green):
# http://localhost:3000  login: admin / admin
```

### Service URLs

| Service | URL | Login |
|---------|-----|-------|
| InfluxDB | http://localhost:8086 | admin / adminpass |
| Grafana | http://localhost:3000 | admin / admin |
| postgres-analytics | localhost:5433 | analytics / analytics / github_analytics |

### InfluxDB connection info (for Shreyas -- STREAM-05)

```
URL   : http://influxdb:8086        (from inside Docker network)
Org   : github-analytics
Bucket: github-stream
Token : github-analytics-token
```

---

## VIZ-02: REST API Enrichment Lookup Table

Computes F5 (owner_star_med) and F6 (readme_bytes) for every repo in the
ML-01 training set. Output Parquet at `hdfs://namenode:9000/github/ml/enrichment/`
is read by Tanusha's ML-03 feature engineering job.

### Prerequisites

1. Hariharan's INFRA-04 must have run (events Parquet in HDFS).
2. Tanusha's ML-01 must have run (labels Parquet in HDFS).
3. HDFS and Spark containers must be running.
4. GitHub PATs set in your shell environment.

### Set PATs

```bash
# Never commit tokens. Set them in your shell before running.
export PAT_SHREYAS=ghp_...
export PAT_HARIHARAN=ghp_...
export PAT_TANUSHA=ghp_...
export PAT_VIKRAM=ghp_...
```

### Run commands

```bash
# 1. Start required containers (if not already running):
docker compose up -d namenode datanode1 datanode2 spark-master spark-worker

# 2. Pass PATs into the container and run a dry run first (first 100 repos):
docker exec \
  -e PAT_SHREYAS="$PAT_SHREYAS" \
  -e PAT_HARIHARAN="$PAT_HARIHARAN" \
  -e PAT_TANUSHA="$PAT_TANUSHA" \
  -e PAT_VIKRAM="$PAT_VIKRAM" \
  spark-master \
  spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-apps/analytics/viz02_enrichment.py --dry-run

# 3. If dry run looks good, run the full overnight job (detached):
docker exec -d \
  -e PAT_SHREYAS="$PAT_SHREYAS" \
  -e PAT_HARIHARAN="$PAT_HARIHARAN" \
  -e PAT_TANUSHA="$PAT_TANUSHA" \
  -e PAT_VIKRAM="$PAT_VIKRAM" \
  spark-master \
  spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-apps/analytics/viz02_enrichment.py

# 4. Monitor progress (script logs every 500 repos):
docker logs -f spark-master

# 5. After it finishes, verify the output:
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/analytics/viz02_enrichment.py --verify-only

# 6. If the script was interrupted, restart it -- it resumes from checkpoint:
#    (checkpoint saved at /tmp/viz02_checkpoint.json inside spark-master)
#    Just re-run the same full run command above.

# 7. To restart from scratch (clear checkpoint):
docker exec spark-master \
  spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-apps/analytics/viz02_enrichment.py --clear-checkpoint
```

### Expected output

```
============================================================
VIZ-02 ENRICHMENT SUMMARY
============================================================
  Total repos enriched    : 94,231
  F5 missing (-1)         : 1,203  (1.3%)
  F6 missing (-1)         : 4,811  (5.1%)
  F5 mean stars           : 142.3
  F5 median stars         : 18.0
  F6 mean README bytes    : 3,421
  F6 median README bytes  : 1,847
============================================================
```

Missing values (-1) are filled with 0 by ML-03's fillna step, matching the
original fallback behavior. Any rate higher than 10% warrants investigation.

### Notify Tanusha

Once the full run is complete and `--verify-only` shows clean output, tell
Tanusha to re-run ML-03 (and ML-04, ML-05) with the real enrichment data.
The HDFS path ML-03 already points to is `hdfs://namenode:9000/github/ml/enrichment/`.

---

## Directory Structure

```
analytics/
  viz02_enrichment.py           -- VIZ-02: F5 + F6 REST API enrichment
  grafana/
    provisioning/
      datasources/
        datasources.yml         -- InfluxDB + PostgreSQL auto-provisioned
      dashboards/
        dashboards.yml          -- dashboard file loader config
  README.md                     -- this file
```
