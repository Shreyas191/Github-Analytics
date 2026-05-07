# Demo Script

**Audience:** Reviewers / professor for the Big Data Systems class project.
**Length:** 8–10 min (5 min architecture + live, 3 min batch, 2 min Q&A buffer).
**Format:** Live screen-share, browser tabs pre-opened in this order.

---

## Pre-flight (do this before clicking "share screen")

1. **All 15 services healthy:**
   ```bash
   docker compose ps
   ```
   Expect every row to show `Up ... (healthy)` for the services that have a health check.

2. **Sidecars running:**
   ```bash
   docker ps --filter name=gh- --format "table {{.Names}}\t{{.Status}}"
   ```
   Expect `gh-producer` and `gh-metrics` Up. If missing, follow `IMPLEMENTATION.md` §4b.

3. **Streaming consumer running:**
   ```bash
   curl -s http://localhost:8080/json/ | python3 -c "import json,sys; print([a['name'] for a in json.load(sys.stdin).get('activeapps',[])])"
   ```
   Expect `['STREAM-05-structured-streaming']`. If empty, re-submit per `IMPLEMENTATION.md` §4c.

4. **Open these tabs in this exact order** (so muscle memory matches the script):
   1. https://github.com/Shreyas191/Github-Analytics — repo for the README diagram
   2. http://localhost:9870 — HDFS NameNode
   3. http://localhost:8080 — Spark Master
   4. http://localhost:3000/d/github-live-stream?orgId=1&from=now-1h&to=now&refresh=10s — Live Grafana
   5. http://localhost:3000/d/github-batch-analytics?orgId=1 — Batch Grafana
   6. `analytics/viz06_pmi_graph.html` opened locally (file:// or via `python3 -m http.server` from `analytics/`)
   7. http://localhost:8090 — Airflow (admin/admin)

5. **Time picker in both Grafana dashboards:** "Last 1 hour", auto-refresh "10s".

6. **Verify data is fresh** (run within the 5 minutes before demo start):
   ```bash
   docker logs gh-producer --tail 1   # should show recent [METRICS]
   docker logs gh-metrics --tail 5    # should show recent [METRICS WRITTEN]
   ```

---

## Slide 0 — Title (10 s)

> "GitHub Developer Ecosystem Analytics. Lambda architecture, real GitHub events, end-to-end pipeline on a Docker stack. Four-person team, five weeks, 26 tickets, all in repo."

---

## Slide 1 — Architecture (60 s) — **tab 1 (GitHub README)**

Scroll to the architecture diagram. Talk through each path:

- **Batch:** GitHub Archive on GCS → bulk download → HDFS raw → PySpark parser → partitioned Parquet → ML labels → Random Forest → model artifact on HDFS.
- **Streaming:** GitHub Events API → 4-PAT rotator → Kafka topics → PySpark Structured Streaming → InfluxDB → Grafana live panels. Plus a 48 h scoring-queue path that loads the RF model.
- **Why lambda:** batch gives accuracy and historical depth (2023–2025, ~120 GB target); streaming gives latency (live trending). They share a unified events schema (INFRA-03).

> "Four owners — Hariharan (infra), Shreyas (streaming), Tanusha (ML), Vikram (analytics + report). Branch-per-owner workflow, merged via PRs."

---

## Slide 2 — Stack is up (30 s) — **terminal**

Run live:
```bash
docker compose ps
```

> "15 containers. HDFS namenode plus 2 datanodes. YARN resource + node manager. Spark master plus worker. Kafka plus Zookeeper. Airflow webserver, scheduler, plus its own Postgres. InfluxDB for time-series. PostgreSQL-analytics for batch outputs. Grafana on top."

---

## Slide 3 — HDFS state (45 s) — **tab 2 (HDFS UI)**

Click **Utilities → Browse the file system** → navigate to `/github/events/parquet/`.

Talk through the partition layout:
- `year=2024/month=06/event_type=PushEvent/`
- `event_type=CreateEvent/`, `WatchEvent/`, …
- snappy-compressed, ~14 MB total for ~395k events parsed from 2 hours of GHArchive.

> "INFRA-04 PySpark job parses raw JSON.GZ from the Archive into this partitioned Parquet layout. Partitioning by year/month/event_type means downstream analytics scan only what they need — language velocity reads only PushEvents and WatchEvents, never touches Issues or Forks."

---

## Slide 4 — Spark Master (30 s) — **tab 3 (Spark UI)**

Show:
- One **running application**: `STREAM-05-structured-streaming`
- Multiple **completed apps** (the batch parser, viz04, viz05, viz07, viz08, validation report, etc.)
- One worker, 2 cores, 1 in use by the streaming job.

> "STREAM-05 is the live consumer. It reads from all 6 Kafka event topics, watermarks at 10 minutes, computes 5-minute tumbling windows for star velocity and language momentum, and writes to InfluxDB via foreachBatch."

---

## Slide 5 — **Live dashboard** (90 s) — **tab 4 (Grafana live)**

This is the showpiece. Talk through panels in this order:

1. **Pipeline Health row** (top-left):
   - **Events / Min** — live throughput from `pipeline_throughput` measurement.
   - **Stars** / **New Repos** — counts from current time window.
   - **Pipeline Uptime** — minutes since metrics emitter started.

2. **Star Velocity row**:
   - **Star Velocity — Top 10 Repos Over Time** — line chart, each line is a repo, populated as 5-minute Spark windows close. Real GitHub repos: `apache/spark`, `facebook/react`, etc.
   - **Trending Repos — Top by Stars** — table sorted descending.

3. **Language Momentum row**:
   - **Language Momentum — New Repos Over Time** — stacked bar by language.
   - **Language Leaderboard** — bargauge showing leader: Python, JavaScript, TypeScript usually.

4. **Kafka Consumer Lag row**:
   - **Consumer Lag per Kafka Topic** — current backlog per topic. push-events typically dominates.
   - **Consumer Lag History by Topic** — line chart over time. Useful to point out: "the Spark consumer is processing slower than the producer publishes for push-events; lag grows then resets when a window closes and Spark catches up."

5. **Throughput Details** — `pipeline_throughput` line chart with two series: `Received / min` (producer side) and `Published` (Kafka-acked side).

> "Every number you see is read off the running pipeline. The producer container is calling GitHub `/events` every 15 seconds with 3 rotating PATs. Spark is reading from Kafka, computing windowed aggregates, writing line-protocol to InfluxDB. Grafana is querying InfluxDB. End to end, no hand-tuned fixtures."

**Honest caveat to include verbally:**

> "WatchEvents — actual GitHub stars — are sparse in the public events firehose, around three per hour for our 4-hour run. To make the visual panels meaningful in a 10-minute demo, I seeded Kafka with 80 synthetic WatchEvents and 60 CreateEvents-with-language tags from a realistic distribution. The pipeline mechanics, lag, throughput, and timing are unmodified. This is documented in `IMPLEMENTATION.md` §5e."

---

## Slide 6 — **Batch dashboard** (90 s) — **tab 5 (Grafana batch)**

Talk through:

- **Geographic Contributions** — Geomap panel from VIZ-04. We resolved 200 sample actor locations via real REST API calls. 31 had geocodable locations across 18 countries — France, UK, Germany, US, China, India, etc.
- **Ecosystem Health Score** — VIZ-07. Composite 0–100 score per language: 30% velocity, 25% growth, 25% maintenance, 20% diversity. TypeScript leads (48), Python second (44), JavaScript third (20). Min-max normalized so it's relative across the languages we have data for.
- **Language Activity Counts** — VIZ-05/VIZ-08 outputs.

> "The architecture works on real public data — 394,304 actual GitHub events from 2024-06-15 hours 12 and 13 of the GHArchive. Language enrichment is hybrid: top 600 most-active repos got real REST API calls for as many as PAT budget allowed; the remainder filled deterministically from a hash. This is the trade-off we made for a live demo — the proposal-scope full enrichment is a 24+-hour Dataproc job, which is documented but out of scope for a laptop demo."

---

## Slide 7 — PMI Language Co-occurrence Graph (45 s) — **tab 6 (viz06 D3.js page)**

Open `analytics/viz06_pmi_graph.html` in a browser tab.

> "VIZ-05 computes pointwise mutual information across language pairs from PushEvents — which language pairs co-occur in the same actor's events more than chance? VIZ-06 renders that as a force-directed network in D3. Hover for PMI scores, drag to rearrange. Clusters tend to form around web stack, ML/data stack, systems stack."

---

## Slide 8 — Airflow + retraining (30 s) — **tab 7 (Airflow UI)**

Show two DAGs:
- `infra06_daily_ingestion` (`@daily`) — incremental hourly archive download, parse, append.
- `infra07_weekly_retraining` (`@weekly`) — refresh features → split → train → verify → notify Spark.

> "Both DAGs run on Airflow's LocalExecutor. INFRA-06 keeps the Parquet table fresh; INFRA-07 retrains the Random Forest weekly and republishes to HDFS at `/models/viral_rf/latest/`, where the streaming scoring job picks it up."

---

## Slide 9 — Wrap (45 s)

Three points:
1. **All 26 tickets shipped.** 25 code deliverables in repo + VIZ-09 final analytical report.
2. **Honest scope:** 2-hour GHArchive sample + hybrid language enrichment in the demo. Full proposal scope (2023–2025, 120 GB, overnight enrichment) runs unchanged on Dataproc.
3. **What we'd do next:** language enrichment moved into INFRA-04 itself (extract language from PushEvent and PullRequestEvent payloads — no extra REST calls); switch Spark Streaming to cluster mode + supervised; add CI on the existing 261 pytest cases.

---

## If something breaks live

| Symptom | Recovery |
|---|---|
| Grafana panel shows "No data" | Hit ↻ refresh icon; verify time picker says "Last 1 hour" |
| Stars / Language panels empty | Run `docker exec spark-master tail -10 /tmp/stream05.log` — if the consumer crashed, re-submit per IMPLEMENTATION.md §4c |
| Producer log empty | `docker logs gh-producer --tail 5` — if the container exited, `docker start gh-producer` (it has `--restart unless-stopped`) |
| Consumer lag panel says huge number | This is honest — the demo machine has 1 worker × 2 cores, real producer outpaces a 5-minute-window consumer. Mention it as observed back-pressure. |
| Spark UI shows 0 active apps | Re-submit STREAM-05; takes 2 min to come up |
| HDFS UI 503 | `docker compose restart namenode datanode1 datanode2` — wait 30 s |

---

## Talking-point cheat sheet

If asked **"Why Kafka and not direct API → Spark?"**
> "Decoupling. The producer rate-limits against GitHub's quotas independently from how fast Spark consumes. We can pause Spark to deploy a new model without losing events; Kafka retention is 48 hours."

If asked **"Why InfluxDB plus PostgreSQL instead of one store?"**
> "Different access patterns. InfluxDB is optimised for time-series writes at 5-minute granularity; Grafana's InfluxDB datasource gives us windowed aggregations cheaply. Postgres holds the batch analytics outputs — geo aggregates, PMI pairs, health scores — which are tabular and join-heavy. Putting them together would compromise both."

If asked **"How does this scale?"**
> "Single-laptop dev: 1 Spark worker, 2 cores. Production target was Dataproc — same Spark code submits unchanged with `--master yarn`. Kafka and HDFS replication factors are 1 for dev; production would be 3."

If asked **"Why is language enrichment done at REST-call time?"**
> "GitHub's events firehose doesn't include the repo's primary language — only payload-specific fields. The proposal-scope solution is `viz02_enrichment.py`, an overnight job that calls `/repos/{owner}/{name}` for every distinct repo. For a laptop demo we hybridised — real for the top 600 most active, hash-deterministic for the rest. Documented in IMPLEMENTATION.md §5e."

If asked **"What was hardest?"**
> Pick one of:
> - PAT rotation + secondary rate limits (GitHub will 403 you above ~80 sustained req/min even within quota).
> - Streaming/batch schema parity — STREAM-04 is a dedicated assertion test.
> - Single-worker resource contention between batch parser and streaming consumer; resolved with `--total-executor-cores 1` for streaming.

---

## Stop the demo cleanly

```bash
docker rm -f gh-producer gh-metrics
docker compose down            # keeps data volumes
# OR
docker compose down -v         # also wipes HDFS / Postgres / InfluxDB
```
