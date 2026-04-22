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
  schema.py                  # INFRA-03: unified events schema (all field definitions)
  infra02_download.py        # INFRA-02: GitHub Archive bulk download (GCS → HDFS)
  infra04_batch_parser.py    # INFRA-04: PySpark JSON.GZ → Parquet parser
  infra05_validation_report.py  # INFRA-05: row counts, null rates, partition sizes
  dags/
    infra06_daily_ingestion.py   # INFRA-06: Airflow DAG — daily archive ingestion
    infra07_weekly_retraining.py # INFRA-07: Airflow DAG — weekly model retraining

ml/
  ml01_label_construction.py    # label: ≥1000 stars within 90 days
  ml03_feature_engineering.py   # 8 features within T+48h window
  ml04_train_test_split.py      # 2023-24 train / 2025 holdout
  ml05_random_forest.py         # MLlib RF: 200 trees, weightCol for imbalance
  ml06_model_serialization.py   # PipelineModel.save to HDFS

streaming/
  stream02_create_topics.py     # Kafka topic setup (7 topics, 4 partitions each)
  stream03_producer.py          # GitHub Events API producer with PAT rotation
  stream04_schema_test.py       # batch vs stream schema parity assertion

analytics/
  viz02_enrichment.py           # REST API enrichment: F5 (owner stars) + F6 (README bytes)
  grafana/provisioning/         # auto-provisioned InfluxDB + Postgres datasources
```

## Detailed Setup

See [IMPLEMENTATION.md](IMPLEMENTATION.md) for step-by-step instructions including HDFS directory creation, sample data download, and running each pipeline component.

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
