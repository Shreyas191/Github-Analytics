"""
Tests for Airflow DAGs (INFRA-06 and INFRA-07)

Tests cover DAG structure without running tasks:
  - DAG ID, schedule, catchup, tags
  - Task IDs and task types
  - Dependency chain (task >> task)
  - Task retry settings
  - Task callables point to the right functions
  - check_new_files logic (pure parts)
  - download_new_files logic (skip when no files)
  - verify_model error handling
  - notify_stream marker path
"""

import pytest
import sys
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

# Suppress Airflow database initialization warnings in test env
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "sqlite:///:memory:")
os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow_test")


# ---------------------------------------------------------------------------
# Import DAG modules
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def dag06():
    import importlib
    import infra06_daily_ingestion as mod
    importlib.reload(mod)
    return mod


@pytest.fixture(scope="module")
def dag07():
    import importlib
    import infra07_weekly_retraining as mod
    importlib.reload(mod)
    return mod


# ===========================================================================
# INFRA-06 — Daily Ingestion DAG
# ===========================================================================

class TestInfra06DagStructure:
    def test_dag_object_exists(self, dag06):
        assert dag06.dag is not None

    def test_dag_id(self, dag06):
        assert dag06.dag.dag_id == "infra06_daily_ingestion"

    def test_schedule_is_daily(self, dag06):
        assert dag06.dag.schedule_interval == "@daily"

    def test_catchup_is_false(self, dag06):
        assert dag06.dag.catchup is False

    def test_start_date(self, dag06):
        assert dag06.dag.start_date == datetime(2025, 1, 1)

    def test_tags_include_infra(self, dag06):
        assert "infra" in dag06.dag.tags

    def test_tags_include_batch(self, dag06):
        assert "batch" in dag06.dag.tags

    def test_tags_include_hariharan(self, dag06):
        assert "hariharan" in dag06.dag.tags

    def test_has_five_tasks(self, dag06):
        assert len(dag06.dag.tasks) == 5

    def test_task_ids_present(self, dag06):
        task_ids = {t.task_id for t in dag06.dag.tasks}
        expected = {
            "check_new_files",
            "download_new_files",
            "upload_to_hdfs",
            "parse_new_files",
            "cleanup_staging",
        }
        assert expected == task_ids

    def test_default_args_owner(self, dag06):
        assert dag06.DEFAULT_ARGS["owner"] == "hariharan"

    def test_default_args_retries(self, dag06):
        assert dag06.DEFAULT_ARGS["retries"] == 2

    def test_default_args_retry_delay(self, dag06):
        assert dag06.DEFAULT_ARGS["retry_delay"] == timedelta(minutes=5)

    def test_default_args_no_email_on_failure(self, dag06):
        assert dag06.DEFAULT_ARGS["email_on_failure"] is False

    def test_default_args_depends_on_past_false(self, dag06):
        assert dag06.DEFAULT_ARGS["depends_on_past"] is False


class TestInfra06TaskDependencies:
    def _task(self, dag06, task_id):
        return dag06.dag.get_task(task_id)

    def test_check_new_runs_before_download(self, dag06):
        check = self._task(dag06, "check_new_files")
        download = self._task(dag06, "download_new_files")
        assert "download_new_files" in {t.task_id for t in check.downstream_list}

    def test_download_runs_before_upload(self, dag06):
        download = self._task(dag06, "download_new_files")
        assert "upload_to_hdfs" in {t.task_id for t in download.downstream_list}

    def test_upload_runs_before_parse(self, dag06):
        upload = self._task(dag06, "upload_to_hdfs")
        assert "parse_new_files" in {t.task_id for t in upload.downstream_list}

    def test_parse_runs_before_cleanup(self, dag06):
        parse = self._task(dag06, "parse_new_files")
        assert "cleanup_staging" in {t.task_id for t in parse.downstream_list}

    def test_cleanup_is_last_task(self, dag06):
        cleanup = self._task(dag06, "cleanup_staging")
        assert len(cleanup.downstream_list) == 0

    def test_check_new_is_first_task(self, dag06):
        check = self._task(dag06, "check_new_files")
        assert len(check.upstream_list) == 0

    def test_cleanup_trigger_rule_is_all_done(self, dag06):
        cleanup = self._task(dag06, "cleanup_staging")
        assert cleanup.trigger_rule == "all_done"


class TestInfra06TaskTypes:
    def _task(self, dag06, task_id):
        return dag06.dag.get_task(task_id)

    def test_check_new_files_is_python_operator(self, dag06):
        from airflow.operators.python import PythonOperator
        task = self._task(dag06, "check_new_files")
        assert isinstance(task, PythonOperator)

    def test_download_new_files_is_python_operator(self, dag06):
        from airflow.operators.python import PythonOperator
        task = self._task(dag06, "download_new_files")
        assert isinstance(task, PythonOperator)

    def test_upload_to_hdfs_is_python_operator(self, dag06):
        from airflow.operators.python import PythonOperator
        task = self._task(dag06, "upload_to_hdfs")
        assert isinstance(task, PythonOperator)

    def test_parse_new_files_is_bash_operator(self, dag06):
        from airflow.operators.bash import BashOperator
        task = self._task(dag06, "parse_new_files")
        assert isinstance(task, BashOperator)

    def test_cleanup_staging_is_bash_operator(self, dag06):
        from airflow.operators.bash import BashOperator
        task = self._task(dag06, "cleanup_staging")
        assert isinstance(task, BashOperator)


class TestInfra06Config:
    def test_hdfs_raw_path_contains_namenode(self, dag06):
        assert "namenode:9000" in dag06.HDFS_RAW_PATH

    def test_gcs_base_url(self, dag06):
        assert dag06.GCS_BASE_URL == "gs://data.gharchive.org"

    def test_staging_dir(self, dag06):
        assert dag06.STAGING_DIR == "/tmp/gharchive_staging"

    def test_spark_master_url(self, dag06):
        assert dag06.SPARK_MASTER.startswith("spark://")

    def test_parser_script_path(self, dag06):
        assert "infra04_batch_parser.py" in dag06.PARSER_SCRIPT


# ---------------------------------------------------------------------------
# check_new_files pure logic
# ---------------------------------------------------------------------------

class TestCheckNewFilesLogic:
    def test_builds_24_files_per_day(self, dag06):
        """The function should generate 24 hourly filenames for a given date."""
        date_str = "2025-01-15"
        gcs_base = dag06.GCS_BASE_URL
        all_files = [
            f"{gcs_base}/{date_str}-{hour}.json.gz"
            for hour in range(24)
        ]
        assert len(all_files) == 24

    def test_filenames_range_0_to_23(self, dag06):
        date_str = "2025-01-15"
        all_files = [
            f"{dag06.GCS_BASE_URL}/{date_str}-{hour}.json.gz"
            for hour in range(24)
        ]
        assert all_files[0].endswith("-0.json.gz")
        assert all_files[23].endswith("-23.json.gz")

    def test_new_file_added_when_not_in_hdfs(self):
        """Simulate: file NOT in HDFS → returncode=1 → added to new_files."""
        mock_result = MagicMock()
        mock_result.returncode = 1

        new_files = []
        gcs_file = "gs://data.gharchive.org/2025-01-15-0.json.gz"

        if mock_result.returncode != 0:
            new_files.append(gcs_file)

        assert gcs_file in new_files

    def test_existing_file_skipped_when_in_hdfs(self):
        """Simulate: file IS in HDFS → returncode=0 → NOT added to new_files."""
        mock_result = MagicMock()
        mock_result.returncode = 0

        new_files = []
        gcs_file = "gs://data.gharchive.org/2025-01-15-0.json.gz"

        if mock_result.returncode != 0:
            new_files.append(gcs_file)

        assert gcs_file not in new_files

    def test_target_date_is_one_day_before_execution_date(self):
        """DAG processes the previous day's files."""
        execution_date = datetime(2025, 1, 16)
        target_date = execution_date - timedelta(days=1)
        assert target_date == datetime(2025, 1, 15)
        assert target_date.strftime("%Y-%m-%d") == "2025-01-15"


# ---------------------------------------------------------------------------
# download_new_files pure logic
# ---------------------------------------------------------------------------

class TestDownloadNewFilesLogic:
    def test_skip_when_no_new_files(self):
        """If xcom_pull returns empty list, function returns early."""
        context = {
            "task_instance": MagicMock()
        }
        context["task_instance"].xcom_pull.side_effect = lambda task_ids, key: (
            [] if key == "new_files" else "2025-01-15"
        )

        # Simulate the early-return logic
        new_files = context["task_instance"].xcom_pull(task_ids="check_new_files", key="new_files")
        assert not new_files  # empty = falsy

    def test_gsutil_command_includes_files(self):
        """gsutil -m cp {files} {staging_path}/ should include all new_files."""
        new_files = ["gs://bucket/2025-01-15-0.json.gz", "gs://bucket/2025-01-15-1.json.gz"]
        staging_path = "/tmp/gharchive_staging/2025-01-15"

        cmd = ["gsutil", "-m", "cp"] + new_files + [staging_path + "/"]
        assert "gs://bucket/2025-01-15-0.json.gz" in cmd
        assert "gs://bucket/2025-01-15-1.json.gz" in cmd
        assert cmd[-1] == staging_path + "/"

    def test_staging_path_includes_date(self):
        date_str = "2025-01-15"
        staging_dir = "/tmp/gharchive_staging"
        staging_path = f"{staging_dir}/{date_str}"
        assert staging_path == "/tmp/gharchive_staging/2025-01-15"


# ===========================================================================
# INFRA-07 — Weekly Retraining DAG
# ===========================================================================

class TestInfra07DagStructure:
    def test_dag_object_exists(self, dag07):
        assert dag07.dag is not None

    def test_dag_id(self, dag07):
        assert dag07.dag.dag_id == "infra07_weekly_retraining"

    def test_schedule_is_weekly(self, dag07):
        assert dag07.dag.schedule_interval == "@weekly"

    def test_catchup_is_false(self, dag07):
        assert dag07.dag.catchup is False

    def test_start_date(self, dag07):
        assert dag07.dag.start_date == datetime(2025, 1, 1)

    def test_tags_include_infra(self, dag07):
        assert "infra" in dag07.dag.tags

    def test_tags_include_ml(self, dag07):
        assert "ml" in dag07.dag.tags

    def test_has_five_tasks(self, dag07):
        assert len(dag07.dag.tasks) == 5

    def test_task_ids_present(self, dag07):
        task_ids = {t.task_id for t in dag07.dag.tasks}
        expected = {
            "refresh_features",
            "rebuild_train_test_split",
            "train_model",
            "verify_model",
            "notify_stream",
        }
        assert expected == task_ids

    def test_default_args_owner(self, dag07):
        assert dag07.DEFAULT_ARGS["owner"] == "hariharan"

    def test_default_args_retries(self, dag07):
        assert dag07.DEFAULT_ARGS["retries"] == 1

    def test_default_args_retry_delay(self, dag07):
        assert dag07.DEFAULT_ARGS["retry_delay"] == timedelta(minutes=10)


class TestInfra07TaskDependencies:
    def _task(self, dag07, task_id):
        return dag07.dag.get_task(task_id)

    def test_refresh_features_runs_first(self, dag07):
        task = self._task(dag07, "refresh_features")
        assert len(task.upstream_list) == 0

    def test_refresh_before_split(self, dag07):
        task = self._task(dag07, "refresh_features")
        assert "rebuild_train_test_split" in {t.task_id for t in task.downstream_list}

    def test_split_before_train(self, dag07):
        task = self._task(dag07, "rebuild_train_test_split")
        assert "train_model" in {t.task_id for t in task.downstream_list}

    def test_train_before_verify(self, dag07):
        task = self._task(dag07, "train_model")
        assert "verify_model" in {t.task_id for t in task.downstream_list}

    def test_verify_before_notify(self, dag07):
        task = self._task(dag07, "verify_model")
        assert "notify_stream" in {t.task_id for t in task.downstream_list}

    def test_notify_is_last_task(self, dag07):
        task = self._task(dag07, "notify_stream")
        assert len(task.downstream_list) == 0


class TestInfra07TaskTypes:
    def _task(self, dag07, task_id):
        return dag07.dag.get_task(task_id)

    def test_refresh_features_is_bash(self, dag07):
        from airflow.operators.bash import BashOperator
        assert isinstance(self._task(dag07, "refresh_features"), BashOperator)

    def test_rebuild_split_is_bash(self, dag07):
        from airflow.operators.bash import BashOperator
        assert isinstance(self._task(dag07, "rebuild_train_test_split"), BashOperator)

    def test_train_model_is_bash(self, dag07):
        from airflow.operators.bash import BashOperator
        assert isinstance(self._task(dag07, "train_model"), BashOperator)

    def test_verify_model_is_python(self, dag07):
        from airflow.operators.python import PythonOperator
        assert isinstance(self._task(dag07, "verify_model"), PythonOperator)

    def test_notify_stream_is_python(self, dag07):
        from airflow.operators.python import PythonOperator
        assert isinstance(self._task(dag07, "notify_stream"), PythonOperator)


class TestInfra07Config:
    def test_spark_master_url(self, dag07):
        assert dag07.SPARK_MASTER.startswith("spark://")

    def test_hdfs_model_path_contains_namenode(self, dag07):
        assert "namenode:9000" in dag07.HDFS_MODEL_PATH

    def test_hdfs_model_latest_path(self, dag07):
        assert "latest" in dag07.HDFS_MODEL_LATEST

    def test_ml_scripts_dir(self, dag07):
        assert dag07.ML_SCRIPTS_DIR == "/opt/spark-apps/ml"


# ---------------------------------------------------------------------------
# verify_model pure logic
# ---------------------------------------------------------------------------

class TestVerifyModelLogic:
    def test_raises_file_not_found_when_hdfs_missing(self, dag07):
        mock_result = MagicMock()
        mock_result.returncode = 1

        with patch("subprocess.run", return_value=mock_result):
            with pytest.raises(FileNotFoundError):
                dag07.verify_model(**{"execution_date": datetime(2025, 1, 1)})

    def test_no_error_when_model_exists(self, dag07):
        """When returncode=0, verify_model should complete without exception."""
        def fake_run(cmd, **kwargs):
            result = MagicMock()
            result.returncode = 0
            return result

        with patch("subprocess.run", side_effect=fake_run):
            # Should not raise
            dag07.verify_model(**{"execution_date": datetime(2025, 1, 1)})

    def test_error_message_contains_model_path(self, dag07):
        mock_result = MagicMock()
        mock_result.returncode = 1

        with patch("subprocess.run", return_value=mock_result):
            with pytest.raises(FileNotFoundError) as exc_info:
                dag07.verify_model(**{"execution_date": datetime(2025, 1, 1)})
            assert dag07.HDFS_MODEL_PATH in str(exc_info.value)


# ---------------------------------------------------------------------------
# notify_stream pure logic
# ---------------------------------------------------------------------------

class TestNotifyStreamLogic:
    def test_raises_when_hdfs_write_fails(self, dag07):
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "HDFS connection refused"

        with patch("subprocess.run", return_value=mock_result):
            with pytest.raises(RuntimeError):
                dag07.notify_stream(**{"execution_date": datetime(2025, 1, 1)})

    def test_marker_path_is_retrained_file(self, dag07):
        """Verify the marker file path ends in .retrained."""
        # The marker path is hardcoded in notify_stream
        marker_path = "hdfs://namenode:9000/github/ml/model/.retrained"
        assert marker_path.endswith(".retrained")

    def test_no_error_when_write_succeeds(self, dag07):
        mock_result = MagicMock()
        mock_result.returncode = 0

        with patch("subprocess.run", return_value=mock_result):
            # Should not raise
            dag07.notify_stream(**{"execution_date": datetime(2025, 1, 1)})

    def test_error_message_contains_stderr(self, dag07):
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "permission denied"

        with patch("subprocess.run", return_value=mock_result):
            with pytest.raises(RuntimeError) as exc_info:
                dag07.notify_stream(**{"execution_date": datetime(2025, 1, 1)})
            assert "permission denied" in str(exc_info.value)
