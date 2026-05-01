# GitHub Developer Ecosystem Analytics

Big Data Systems Project · NYU · 5 Weeks · 4 Members

## Team

| Member | Role | Domain |
|--------|------|--------|
| Hariharan L (hl5865) | Data Infrastructure Lead | `batch/` |
| Shreyas Kaldate (sk12898) | Streaming Pipeline Lead | `streaming/` |
| Tanusha Karnam (tk3514) | ML & Feature Engineering Lead | `ml/` |
| Vikram Markali (vrm9190) | Analytics & Visualization Lead | `analytics/` |

## Architecture

Lambda architecture with batch + streaming paths feeding a Grafana dashboard.

```
GitHub Archive (GCS, 2023–2025, ~120GB)
  └── infra02_download.py (gsutil)
        └── HDFS /github/events/raw/
              └── infra04_batch_parser.py (PySpark)
                    └── HDFS /github/events/parquet/   ← partitioned Parquet
                          ├── ML-01 label construction
                          ├── ML-03 feature engineering
                          ├── ML-04 train/test split
                          └── ML-05 Random Forest → /models/viral_rf/latest/

GitHub Events API (live)
  └── stream03_producer.py (every 15s, 4 PATs rotating)
        └── Kafka topics (push, watch, fork, pr, issues, create, pending-repos)
              └── PySpark Structured Streaming (STREAM-05)
                    └── InfluxDB → Grafana dashboards
                          └── STREAM-06 scoring queue (loads ML-05 model)
```

## Stack

`PySpark 3.5` · `Kafka 7.4` · `HDFS 3.2` · `Airflow 2.8` · `Grafana 10.2` · `InfluxDB 2.7` · `PostgreSQL 15` · `Docker`

## Quick Start

```bash
git clone https://github.com/Shreyas191/Github-Analytics.git
cd Github-Analytics

cp .env.example .env
# Fill in your GitHub PAT (read:public_repo scope)

docker-compose up -d
```

Wait ~60 seconds for all services to become healthy.

## Services

| Service | URL | Login |
|---------|-----|-------|
| HDFS NameNode | http://localhost:9870 | — |
| YARN ResourceManager | http://localhost:8088 | — |
| Spark Master | http://localhost:8080 | — |
| Airflow | http://localhost:8090 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |
| InfluxDB | http://localhost:8086 | admin / adminpass |

## Repository Structure

```
batch/
  schema.py                       # INFRA-03: unified events schema (all field definitions)
  infra02_download.py             # INFRA-02: GitHub Archive bulk download (GCS → HDFS)
  infra04_batch_parser.py         # INFRA-04: PySpark JSON.GZ → Parquet parser
  infra05_validation_report.py    # INFRA-05: row counts, null rates, partition sizes
  dags/
    infra06_daily_ingestion.py    # INFRA-06: Airflow DAG — daily archive ingestion
    infra07_weekly_retraining.py  # INFRA-07: Airflow DAG — weekly model retraining

ml/
  ml01_label_construction.py      # ML-01: label ≥1000 stars within 90 days of T=0
  ml02_class_imbalance.py         # ML-02: weightCol strategy — validates ~200 positive weight
  ml03_feature_engineering.py     # ML-03: 8 features computed within T+48h window
  ml04_train_test_split.py        # ML-04: 2023-24 train / 2025 holdout split
  ml05_random_forest.py           # ML-05: MLlib RF 200 trees, weightCol, PR-AUC evaluation
  ml06_model_serialization.py     # ML-06: PipelineModel.save to HDFS + Shreyas handoff
  ml07_case_studies.py            # ML-07: top 5 early-detection case studies for report

streaming/
  stream01_pat_rotator.py         # STREAM-01: 4-PAT round-robin rotator (20k req/hr)
  stream02_create_topics.py       # STREAM-02: Kafka topic setup (7 topics, 4 partitions each)
  stream03_producer.py            # STREAM-03: GitHub Events API producer with PAT rotation
  stream04_schema_test.py         # STREAM-04: batch vs stream schema parity assertion
  stream05_spark_streaming.py     # STREAM-05: PySpark Structured Streaming, 5-min windows
  stream06_scoring_job.py         # STREAM-06: 48h pending-repos queue + viral scoring job
  stream07_metrics.py             # STREAM-07: throughput + consumer lag logging to Grafana

analytics/
  viz02_enrichment.py             # VIZ-02: REST API enrichment — F5 (owner stars) + F6 (README bytes)
  viz04_geo_heatmap.py            # VIZ-04: geographic contribution heatmap (Grafana Geomap)
  viz05_pmi_cooccurrence.py       # VIZ-05: language co-occurrence PMI computation
  viz06_pmi_graph.html            # VIZ-06: D3.js force-directed PMI language graph
  viz07_health_score.py           # VIZ-07: Ecosystem Health Score per language → PostgreSQL
  viz08_language_velocity.py      # VIZ-08: top 10 rising/declining languages 2023-2025
  db_schema.sql                   # PostgreSQL schema for batch analytics outputs
  grafana/provisioning/
    datasources/                  # auto-provisioned InfluxDB + PostgreSQL datasources
    dashboards/
      github_batch.json           # Grafana batch dashboard (language velocity, health score)
      github_live.json            # Grafana live dashboard (trending repos, star velocity)
```

## Detailed Setup

Each subdirectory ships its own README with deeper component-level instructions:
[`batch/`](batch/README.md), [`streaming/`](streaming/README.md), [`ml/`](ml/README.md), [`analytics/`](analytics/README.md).

## Branch Convention

```
main              ← stable only, merge via PR
hariharan/        ← Hariharan's working branches
shreyas/          ← Shreyas's working branches
tanusha/          ← Tanusha's working branches
vikram/           ← Vikram's working branches
```

## Important

Never commit `.env` files or PAT tokens to this repo.
