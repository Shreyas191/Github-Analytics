"""
INFRA-06: Airflow DAG — Daily GitHub Archive Ingestion
GitHub Developer Ecosystem Analytics
Author: Hariharan L (hl5865)

Schedule: @daily (runs at midnight UTC every day)

What this DAG does:
  1. Detects which hourly Archive .json.gz files are new (not yet in HDFS)
  2. Downloads them from GCS to a staging area
  3. Runs INFRA-04 parser on the new files only (append mode)
  4. Cleans up the staging area

Why daily and not hourly?
  GitHub Archive publishes files hourly but there's no urgency for batch
  analysis — daily ingestion keeps things simple and avoids hammering GCS.
  The streaming path handles real-time freshness.

DAG structure:
  check_new_files → download_new_files → parse_new_files → cleanup_staging

Airflow concepts used:
  - PythonOperator: runs a Python function as a task
  - BashOperator: runs a shell command as a task
  - task dependencies: >> operator chains tasks in order
  - XCom: how tasks pass data to each other (file lists, counts)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# DAG default arguments
# These apply to every task in the DAG unless overridden per task.
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "hariharan",
    "depends_on_past": False,       # don't wait for yesterday's run to succeed
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,                   # retry failed tasks twice before giving up
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
HDFS_RAW_PATH     = "hdfs://namenode:9000/github/events/raw"
HDFS_PARQUET_PATH = "hdfs://namenode:9000/github/events/parquet"
GCS_BASE_URL      = "gs://data.gharchive.org"
STAGING_DIR       = "/tmp/gharchive_staging"
SPARK_MASTER      = "spark://spark-master:7077"
PARSER_SCRIPT     = "/opt/spark-apps/batch/infra04_batch_parser.py"


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def check_new_files(**context):
    """
    Figure out which hourly GCS files haven't been ingested yet.

    How it works:
      - The DAG runs daily for yesterday's date (Airflow's execution_date)
      - Yesterday had 24 hourly files: 2025-01-15-0.json.gz through 2025-01-15-23.json.gz
      - We check HDFS to see which ones are already there
      - Push the missing file list to XCom so the next task can use it

    XCom (Cross-Communication):
      Airflow's way of passing small data between tasks.
      task_instance.xcom_push() stores a value.
      task_instance.xcom_pull() retrieves it in the next task.
      Only use XCom for small data (file lists, counts) — not dataframes.
    """
    import subprocess

    # Airflow passes execution_date as the scheduled run date
    # We process the previous day's files
    execution_date = context["execution_date"]
    target_date = execution_date - timedelta(days=1)
    date_str = target_date.strftime("%Y-%m-%d")

    print(f"Checking for new files for date: {date_str}")

    # Build list of all 24 hourly files for this date
    all_files = [
        f"{GCS_BASE_URL}/{date_str}-{hour}.json.gz"
        for hour in range(24)
    ]

    # Check which ones are already in HDFS
    # hdfs dfs -ls returns non-zero exit code if path doesn't exist
    new_files = []
    for gcs_file in all_files:
        filename = gcs_file.split("/")[-1]
        hdfs_path = f"{HDFS_RAW_PATH}/{filename}"

        result = subprocess.run(
            ["hdfs", "dfs", "-test", "-e", hdfs_path],
            capture_output=True
        )

        if result.returncode != 0:
            # File not in HDFS yet — needs ingestion
            new_files.append(gcs_file)

    print(f"Found {len(new_files)} new files to ingest out of 24")

    # Push to XCom for the next task
    context["task_instance"].xcom_push(key="new_files", value=new_files)
    context["task_instance"].xcom_push(key="date_str", value=date_str)

    return len(new_files)


def download_new_files(**context):
    """
    Download new GCS files to local staging directory.
    Uses gsutil -m for parallel downloads.
    """
    import subprocess
    import os

    new_files = context["task_instance"].xcom_pull(
        task_ids="check_new_files", key="new_files"
    )
    date_str = context["task_instance"].xcom_pull(
        task_ids="check_new_files", key="date_str"
    )

    if not new_files:
        print("No new files to download. Skipping.")
        return 0

    # Create staging directory for this date
    staging_path = f"{STAGING_DIR}/{date_str}"
    os.makedirs(staging_path, exist_ok=True)

    print(f"Downloading {len(new_files)} files to {staging_path}")

    # gsutil -m downloads files in parallel
    result = subprocess.run(
        ["gsutil", "-m", "cp"] + new_files + [staging_path + "/"],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"gsutil download failed:\n{result.stderr}")

    print(f"Download complete. Files in {staging_path}")

    # Push staging path for next task
    context["task_instance"].xcom_push(key="staging_path", value=staging_path)
    return len(new_files)


def upload_to_hdfs(**context):
    """
    Upload downloaded files from staging to HDFS raw folder.
    """
    import subprocess

    staging_path = context["task_instance"].xcom_pull(
        task_ids="download_new_files", key="staging_path"
    )

    if not staging_path:
        print("No staging path found. Skipping upload.")
        return

    print(f"Uploading from {staging_path} to {HDFS_RAW_PATH}/")

    result = subprocess.run(
        ["hdfs", "dfs", "-put", f"{staging_path}/*.json.gz", HDFS_RAW_PATH + "/"],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"HDFS upload failed:\n{result.stderr}")

    print("Upload to HDFS complete.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="infra06_daily_ingestion",
    description="Daily GitHub Archive ingestion: GCS → HDFS → Parquet",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,       # don't backfill missed runs — we already have the data
    tags=["infra", "batch", "hariharan"],
) as dag:

    # ------------------------------------------------------------------
    # Task 1: Check which files are new
    # ------------------------------------------------------------------
    check_new = PythonOperator(
        task_id="check_new_files",
        python_callable=check_new_files,
    )

    # ------------------------------------------------------------------
    # Task 2: Download new files from GCS
    # ------------------------------------------------------------------
    download = PythonOperator(
        task_id="download_new_files",
        python_callable=download_new_files,
    )

    # ------------------------------------------------------------------
    # Task 3: Upload to HDFS
    # ------------------------------------------------------------------
    upload = PythonOperator(
        task_id="upload_to_hdfs",
        python_callable=upload_to_hdfs,
    )

    # ------------------------------------------------------------------
    # Task 4: Run INFRA-04 parser on new files (append mode)
    # BashOperator runs a shell command directly
    # ------------------------------------------------------------------
    parse = BashOperator(
        task_id="parse_new_files",
        bash_command=f"""
            spark-submit \\
                --master {SPARK_MASTER} \\
                {PARSER_SCRIPT} \\
                --input {HDFS_RAW_PATH}/*.json.gz \\
                --output {HDFS_PARQUET_PATH}
        """,
    )

    # ------------------------------------------------------------------
    # Task 5: Clean up staging directory
    # ------------------------------------------------------------------
    cleanup = BashOperator(
        task_id="cleanup_staging",
        bash_command=f"rm -rf {STAGING_DIR}/{{{{ ds }}}}",
        # {{ ds }} is Airflow's template for execution date (YYYY-MM-DD)
        trigger_rule="all_done",   # run cleanup even if parse failed
    )

    # ------------------------------------------------------------------
    # DAG dependency chain
    # >> means "run after"
    # ------------------------------------------------------------------
    check_new >> download >> upload >> parse >> cleanup
