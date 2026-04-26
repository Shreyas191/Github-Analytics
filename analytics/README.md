# Analytics and Visualization -- Vikram Markali (vrm9190)

VIZ-01 through VIZ-09.

## Ticket Status

| Ticket | Title | Week | Status |
|--------|-------|------|--------|
| VIZ-01 | Grafana data store decision | 1 | Done |
| VIZ-02 | REST API enrichment lookup table (F5 + F6) | 1-2 | Done |
| VIZ-03 | Grafana dashboards -- leaderboard + trending repos | 3 | Done |
| VIZ-04 | Geographic contribution heatmap | 3 | Done |
| VIZ-05 | PMI co-occurrence computation | 3 | Done |
| VIZ-06 | PMI force-directed graph | 4 | Done |
| VIZ-07 | Ecosystem Health Score | 3-4 | Done |
| VIZ-08 | Language velocity rankings report | 4 | Done |
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

## VIZ-03: Grafana Dashboards

Two dashboards provisioned automatically at Grafana startup from
`grafana/provisioning/dashboards/`. No manual import needed.

| Dashboard | File | Datasource | Refresh |
|-----------|------|------------|---------|
| GitHub Live -- Streaming Analytics | `github_live.json` | InfluxDB (influxdb-stream) | 30s |
| GitHub Batch -- Analytics Dashboard | `github_batch.json` | PostgreSQL (postgres-batch) | 5m |

### github_live.json (VIZ-03 live view)

Reads from InfluxDB measurements written by Shreyas's STREAM-05 and STREAM-07 jobs.

Panels:
- **Pipeline Health** -- events/min stat, stars last 30 min, new repos last 30 min, uptime
- **Star Velocity** -- top repos time series (5-min windows) + trending repos table
- **Language Momentum** -- new repos bar chart over time + language leaderboard bar gauge
- **Kafka Consumer Lag** -- per-topic lag table + lag history time series
- **Throughput Details** -- events received/min + events published over time

### github_batch.json (VIZ-04 through VIZ-08 results)

Reads from PostgreSQL tables written by the PySpark batch jobs.

Panels:
- **Overview** -- languages scored, co-occurrence pairs, countries, total contributors
- **Geographic Distribution** -- Grafana Geomap + top 20 countries table
- **Ecosystem Health Score** -- bar gauge (top 20) + component breakdown table
- **Language Velocity Rankings** -- rising top 10 table + declining top 10 table
- **Language Adoption Trends** -- star acquisition time series + contributor growth time series
- **PMI Co-occurrence** -- top pairs table + node size bar gauge + link to VIZ-06 D3 graph

### Open dashboards

```bash
# Grafana must be running:
docker compose up -d grafana

# Open in browser:
# http://localhost:3000  login: admin / admin
# Navigate to Dashboards to see both dashboards listed.
```

---

## Schema Setup (run once before any VIZ-04 through VIZ-08 job)

```bash
# Apply all postgres-analytics table definitions:
docker exec -i postgres-analytics psql -U analytics -d github_analytics \
  < /opt/spark-apps/analytics/db_schema.sql
```

Tables created: `geo_contributions`, `pmi_pairs`, `language_nodes`,
`ecosystem_health`, `language_velocity`, `language_rankings`.

---

## VIZ-04: Geographic Contribution Heatmap

Fetches GitHub user profiles via REST API to resolve actor locations to
ISO 3166-1 alpha-2 country codes, then aggregates event counts by
(year, country). Covers top 50,000 most active actors.

Writes to: `postgres-analytics.geo_contributions`

```bash
# Sample run -- top 200 actors (smoke test):
docker exec \
  -e PAT_SHREYAS="$PAT_SHREYAS" \
  -e PAT_HARIHARAN="$PAT_HARIHARAN" \
  -e PAT_TANUSHA="$PAT_TANUSHA" \
  -e PAT_VIKRAM="$PAT_VIKRAM" \
  spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages org.postgresql:postgresql:42.6.0 \
    /opt/spark-apps/analytics/viz04_geo_heatmap.py --sample

# Full run (pass PATs same way, remove --sample):
docker exec \
  -e PAT_SHREYAS="$PAT_SHREYAS" \
  -e PAT_HARIHARAN="$PAT_HARIHARAN" \
  -e PAT_TANUSHA="$PAT_TANUSHA" \
  -e PAT_VIKRAM="$PAT_VIKRAM" \
  spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages org.postgresql:postgresql:42.6.0 \
    /opt/spark-apps/analytics/viz04_geo_heatmap.py
```

Checkpoint at `/tmp/viz04_actor_locations.json` -- restarts resume automatically.
Use `--clear-checkpoint` to force a fresh run.

---

## VIZ-05: PMI Language Co-occurrence Computation

Computes Pointwise Mutual Information between all language pairs based on
developer co-contribution patterns. High PMI means developers who use
language A are significantly more likely to also use language B.

Formula: `PMI(A,B) = log2( P(A,B) / (P(A) * P(B)) )`

Writes to: `postgres-analytics.pmi_pairs`, `postgres-analytics.language_nodes`
Also exports: `/tmp/viz05_pmi_graph.json` (consumed by VIZ-06 D3 graph)

```bash
# Sample run -- 10% of events:
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.6.0 \
  /opt/spark-apps/analytics/viz05_pmi_cooccurrence.py --sample

# Full run:
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.6.0 \
  /opt/spark-apps/analytics/viz05_pmi_cooccurrence.py
```

Config: top 50 languages by actor count, minimum 500 co-occurrences per pair.

---

## VIZ-06: PMI Force-Directed Graph

Interactive D3.js v7 force-directed network graph of the language ecosystem.
Reads `/tmp/viz05_pmi_graph.json` -- run VIZ-05 first.

Features:
- Node size scales with actor count (radius 5 to 30)
- Edge width scales with PMI score
- Ecosystem color coding: systems (red), web (blue), data/ML (green),
  JVM (purple), mobile (orange)
- Zoom, pan, drag-and-drop nodes
- PMI cutoff slider, co-occurrence slider, language search box
- Tooltip on hover (top 5 pairs + PMI score + co-occurrence count)

```bash
# Serve locally (from repo root):
python -m http.server 8888

# Open in browser:
# http://localhost:8888/analytics/viz06_pmi_graph.html
```

---

## VIZ-07: Ecosystem Health Score

Computes a composite Ecosystem Health Score (0 to 100) per programming language
from 2023 to 2025 batch data. Min-max normalized so scores are relative across languages.

| Component | Weight | Definition |
|-----------|--------|------------|
| velocity_score | 30% | Stars per repo per month (adoption momentum) |
| growth_score | 25% | Month-over-month unique contributor growth rate |
| maintenance_score | 25% | Push + PR + Issue events per active repo |
| diversity_score | 20% | Unique actors / total events (contributor breadth) |

Writes to: `postgres-analytics.ecosystem_health`

```bash
# Sample run -- 10% of events:
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.6.0 \
  /opt/spark-apps/analytics/viz07_health_score.py --sample

# Full run:
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.6.0 \
  /opt/spark-apps/analytics/viz07_health_score.py
```

Minimum thresholds: 50 repos and 100 unique actors per language (filters out
niche languages with too little data for reliable scoring).

---

## VIZ-08: Language Velocity Rankings Report

Computes monthly snapshots per language (star count, fork count, push count,
new repos, contributor count) from 2023 to 2025, then ranks the top 10
fastest-rising and top 10 fastest-declining languages by year-over-year
star growth rate.

Writes to:
- `postgres-analytics.language_velocity` (monthly snapshots for time series)
- `postgres-analytics.language_rankings` (top 10 rising + top 10 declining)

```bash
# Sample run -- 10% of events:
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.6.0 \
  /opt/spark-apps/analytics/viz08_language_velocity.py --sample

# Full run:
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.6.0 \
  /opt/spark-apps/analytics/viz08_language_velocity.py
```

---

## Recommended Run Order

Run these in sequence after the schema setup:

```
1. db_schema.sql          -- create all postgres tables (one time)
2. viz04_geo_heatmap.py   -- needs PATs, long-running (checkpoint resumes)
3. viz05_pmi_cooccurrence.py -- outputs pmi_graph.json for VIZ-06
4. viz07_health_score.py
5. viz08_language_velocity.py
6. Open viz06_pmi_graph.html after viz05 finishes
7. Grafana dashboards load automatically once tables have data
```

---

## Directory Structure

```
analytics/
  db_schema.sql                   -- one-time schema setup for postgres-analytics
  viz02_enrichment.py             -- VIZ-02: F5 + F6 REST API enrichment
  viz04_geo_heatmap.py            -- VIZ-04: country-level contributor heatmap
  viz05_pmi_cooccurrence.py       -- VIZ-05: PMI language co-occurrence
  viz06_pmi_graph.html            -- VIZ-06: D3.js force-directed PMI graph
  viz07_health_score.py           -- VIZ-07: ecosystem health score (0-100)
  viz08_language_velocity.py      -- VIZ-08: language velocity rankings
  grafana/
    provisioning/
      datasources/
        datasources.yml           -- InfluxDB + PostgreSQL auto-provisioned
      dashboards/
        dashboards.yml            -- dashboard file loader config
        github_live.json          -- VIZ-03: live streaming dashboard
        github_batch.json         -- VIZ-03: batch analytics dashboard
  README.md                       -- this file
```
