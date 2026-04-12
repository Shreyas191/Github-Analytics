# GitHub Developer Ecosystem Analytics

Big Data Systems Project · NYU · 5 Weeks · 4 Members

## Team
| Member | Role | Domain |
|---|---|---|
| Hariharan L (hl5865) | Data Infrastructure Lead | `batch/` |
| Shreyas Kaldate (sk12898) | Streaming Pipeline Lead | `streaming/` |
| Tanusha Karnam (tk3514) | ML & Feature Engineering Lead | `ml/` |
| Vikram Markali (vrm9190) | Analytics & Visualization Lead | `analytics/` |

## Architecture
Lambda architecture with batch + streaming paths feeding a Grafana dashboard.
- **Batch**: GitHub Archive (2023–2025, ~120GB) → HDFS → PySpark → Parquet
- **Streaming**: GitHub Events API → Kafka → PySpark Structured Streaming → InfluxDB → Grafana
- **ML**: Random Forest (MLlib) — viral repo prediction at T+48h

## Stack
`PySpark` · `Kafka` · `HDFS` · `Airflow` · `Grafana` · `InfluxDB` · `Docker`

## Setup
See each domain folder for setup instructions.

## Branch Convention
```
main              ← stable only, merge via PR
shreyas/          ← Shreyas's working branches
hariharan/        ← Hariharan's working branches
tanusha/          ← Tanusha's working branches
vikram/           ← Vikram's working branches
```

## Important
Never commit `.env` files or PAT tokens to this repo.
