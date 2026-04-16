"""
INFRA-07: Airflow DAG — Weekly Model Retraining
GitHub Developer Ecosystem Analytics
Author: Hariharan L (hl5865)

Schedule: @weekly (runs every Sunday at midnight UTC)

What this DAG does:
  1. Runs ML-03 feature engineering (refreshes features with latest data)
  2. Runs ML-04 train/test split
  3. Runs ML-05 Random Forest training
  4. Saves the new model to HDFS /models/viral_rf/latest/
  5. Notifies Shreyas's STREAM-06 scoring job to reload the model

Why weekly retraining?
  New repos are created every day. A model trained only once on 2023-2024
  data will drift as GitHub's ecosystem evolves. Weekly retraining keeps
  the viral repo predictor fresh with the latest patterns.

DAG structure:
  refresh_features → retrain_model → verify_model → notify_stream

Dependencies:
  - Requires INFRA-04 to have run (events Parquet must exist)
  - Requires Vikram's VIZ-02 enrichment table to exist
  - Notifies Shreyas (STREAM-06) when new model is live
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "hariharan",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

SPARK_MASTER   = "spark://spark-master:7077"
ML_SCRIPTS_DIR = "/opt/spark-apps/ml"
HDFS_MODEL_PATH = "hdfs://namenode:9000/github/ml/model/rf_trained"
HDFS_MODEL_LATEST = "hdfs://namenode:9000/github/ml/model/latest"


def verify_model(**context):
    """
    Check that the newly trained model was actually saved to HDFS.
    If the model path doesn't exist, fail loudly before STREAM-06
    tries to load a missing model.
    """
    import subprocess

    result = subprocess.run(
        ["hdfs", "dfs", "-test", "-e", HDFS_MODEL_PATH],
        capture_output=True
    )

    if result.returncode != 0:
        raise FileNotFoundError(
            f"Model not found at {HDFS_MODEL_PATH}. "
            "ML-05 may have failed. Check spark logs."
        )

    print(f"Model verified at: {HDFS_MODEL_PATH}")

    # Create/update a 'latest' symlink in HDFS
    # STREAM-06 always loads from /latest/ so it picks up new models automatically
    subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", HDFS_MODEL_LATEST])
    subprocess.run([
        "hdfs", "dfs", "-cp", HDFS_MODEL_PATH, HDFS_MODEL_LATEST
    ])

    print(f"Latest model pointer updated: {HDFS_MODEL_LATEST}")


def notify_stream(**context):
    """
    Write a small marker file to HDFS that STREAM-06 polls.
    When STREAM-06 sees this file, it reloads the model on the next scoring cycle.

    Why a marker file instead of a direct API call?
      - STREAM-06 is a long-running Spark Structured Streaming job
      - You can't easily interrupt it mid-stream
      - A marker file is a simple, reliable handoff mechanism
      - STREAM-06 checks for the marker every 30 minutes
    """
    import subprocess
    from datetime import datetime, timezone

    marker_content = datetime.now(timezone.utc).isoformat()
    marker_path = "hdfs://namenode:9000/github/ml/model/.retrained"

    # Write timestamp to marker file
    result = subprocess.run(
        f'echo "{marker_content}" | hdfs dfs -put -f - {marker_path}',
        shell=True,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Failed to write marker file: {result.stderr}")

    print(f"Retraining marker written to: {marker_path}")
    print(f"STREAM-06 will reload model on next scoring cycle (within 30 min)")
    print(f"Timestamp: {marker_content}")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="infra07_weekly_retraining",
    description="Weekly ML model retraining: features → train → save → notify",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    tags=["infra", "ml", "hariharan"],
) as dag:

    # ------------------------------------------------------------------
    # Task 1: Refresh feature engineering (ML-03)
    # Recomputes all 8 features with latest events data
    # ------------------------------------------------------------------
    refresh_features = BashOperator(
        task_id="refresh_features",
        bash_command=f"""
            spark-submit \\
                --master {SPARK_MASTER} \\
                {ML_SCRIPTS_DIR}/ml03_feature_engineering.py
        """,
    )

    # ------------------------------------------------------------------
    # Task 2: Rebuild train/test split (ML-04)
    # As new 2025 data comes in, the holdout set grows
    # ------------------------------------------------------------------
    rebuild_split = BashOperator(
        task_id="rebuild_train_test_split",
        bash_command=f"""
            spark-submit \\
                --master {SPARK_MASTER} \\
                {ML_SCRIPTS_DIR}/ml04_train_test_split.py
        """,
    )

    # ------------------------------------------------------------------
    # Task 3: Train new Random Forest (ML-05)
    # Saves model to HDFS /github/ml/model/rf_trained/
    # ------------------------------------------------------------------
    train_model = BashOperator(
        task_id="train_model",
        bash_command=f"""
            spark-submit \\
                --master {SPARK_MASTER} \\
                {ML_SCRIPTS_DIR}/ml05_random_forest.py
        """,
    )

    # ------------------------------------------------------------------
    # Task 4: Verify the model was saved correctly
    # ------------------------------------------------------------------
    verify = PythonOperator(
        task_id="verify_model",
        python_callable=verify_model,
        provide_context=True,
    )

    # ------------------------------------------------------------------
    # Task 5: Notify STREAM-06 to reload the model
    # ------------------------------------------------------------------
    notify = PythonOperator(
        task_id="notify_stream",
        python_callable=notify_stream,
        provide_context=True,
    )

    # ------------------------------------------------------------------
    # Dependency chain
    # ------------------------------------------------------------------
    refresh_features >> rebuild_split >> train_model >> verify >> notify
