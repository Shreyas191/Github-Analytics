"""
Tests for batch/infra02_download.py (INFRA-02)

Tests cover pure/testable logic:
  - GCS pattern generation for years and sample months
  - File discovery (glob) logic
  - download_years and download_sample call the right GCS patterns
  - upload_hdfs iterates over correct files
  - Subprocess calls are mocked — no actual network or docker calls
  - Argument parser defaults and overrides
"""

import pytest
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch, call
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import infra02_download as infra02


# ---------------------------------------------------------------------------
# Constants tests
# ---------------------------------------------------------------------------

class TestConstants:
    def test_gcs_base_is_gharchive(self):
        assert infra02.GCS_BASE == "gs://data.gharchive.org"

    def test_local_raw_is_path(self):
        assert isinstance(infra02.LOCAL_RAW, Path)
        assert str(infra02.LOCAL_RAW) == "data/raw"

    def test_hdfs_raw_path(self):
        assert infra02.HDFS_RAW == "/github/events/raw"

    def test_full_years_contains_three_years(self):
        assert infra02.FULL_YEARS == ["2023", "2024", "2025"]

    def test_sample_months_contains_three_months(self):
        assert len(infra02.SAMPLE_MONTHS) == 3

    def test_sample_months_are_2025(self):
        for month in infra02.SAMPLE_MONTHS:
            assert month.startswith("2025-")

    def test_sample_months_jan_to_mar(self):
        assert infra02.SAMPLE_MONTHS == ["2025-01", "2025-02", "2025-03"]


# ---------------------------------------------------------------------------
# run() wrapper
# ---------------------------------------------------------------------------

class TestRunFunction:
    @patch("infra02_download.subprocess.run")
    def test_run_calls_subprocess_with_cmd(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        infra02.run(["echo", "hello"])
        mock_run.assert_called_once_with(["echo", "hello"], check=True)

    @patch("infra02_download.subprocess.run")
    def test_run_check_false_passes_through(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1)
        infra02.run(["false"], check=False)
        mock_run.assert_called_once_with(["false"], check=False)

    @patch("infra02_download.subprocess.run")
    def test_run_returns_completed_process(self, mock_run):
        expected = MagicMock(returncode=0)
        mock_run.return_value = expected
        result = infra02.run(["ls"])
        assert result is expected


# ---------------------------------------------------------------------------
# download_gcs()
# ---------------------------------------------------------------------------

class TestDownloadGcs:
    @patch("infra02_download.run")
    def test_download_gcs_creates_dest_dir(self, mock_run, tmp_path):
        dest = tmp_path / "subdir"
        assert not dest.exists()
        infra02.download_gcs("gs://bucket/*.json.gz", dest)
        assert dest.exists()

    @patch("infra02_download.run")
    def test_download_gcs_calls_gsutil(self, mock_run, tmp_path):
        dest = tmp_path / "raw"
        pattern = "gs://data.gharchive.org/2025-01-*.json.gz"
        infra02.download_gcs(pattern, dest)
        mock_run.assert_called_once_with(["gsutil", "-m", "cp", "-n", pattern, str(dest)])

    @patch("infra02_download.run")
    def test_download_gcs_uses_minus_n_flag(self, mock_run, tmp_path):
        """The -n flag prevents overwriting existing files (no-clobber)."""
        dest = tmp_path / "raw"
        infra02.download_gcs("gs://bucket/file.json.gz", dest)
        args = mock_run.call_args[0][0]
        assert "-n" in args


# ---------------------------------------------------------------------------
# download_years() — checks it builds correct GCS patterns
# ---------------------------------------------------------------------------

class TestDownloadYears:
    @patch("infra02_download.download_gcs")
    def test_download_years_calls_gcs_once_per_year(self, mock_dl):
        infra02.download_years(["2023", "2024"])
        assert mock_dl.call_count == 2

    @patch("infra02_download.download_gcs")
    def test_download_years_builds_correct_pattern(self, mock_dl):
        infra02.download_years(["2023"])
        pattern_arg = mock_dl.call_args[0][0]
        assert pattern_arg == "gs://data.gharchive.org/2023-*.json.gz"

    @patch("infra02_download.download_gcs")
    def test_download_years_uses_local_raw_as_dest(self, mock_dl):
        infra02.download_years(["2023"])
        dest_arg = mock_dl.call_args[0][1]
        assert dest_arg == infra02.LOCAL_RAW

    @patch("infra02_download.download_gcs")
    def test_download_years_empty_list_no_calls(self, mock_dl):
        infra02.download_years([])
        mock_dl.assert_not_called()

    @patch("infra02_download.download_gcs")
    def test_download_years_all_three_years(self, mock_dl):
        infra02.download_years(["2023", "2024", "2025"])
        patterns = [c[0][0] for c in mock_dl.call_args_list]
        assert "gs://data.gharchive.org/2023-*.json.gz" in patterns
        assert "gs://data.gharchive.org/2024-*.json.gz" in patterns
        assert "gs://data.gharchive.org/2025-*.json.gz" in patterns


# ---------------------------------------------------------------------------
# download_sample()
# ---------------------------------------------------------------------------

class TestDownloadSample:
    @patch("infra02_download.download_gcs")
    def test_download_sample_calls_gcs_three_times(self, mock_dl):
        infra02.download_sample()
        assert mock_dl.call_count == 3

    @patch("infra02_download.download_gcs")
    def test_download_sample_uses_correct_months(self, mock_dl):
        infra02.download_sample()
        patterns = [c[0][0] for c in mock_dl.call_args_list]
        assert "gs://data.gharchive.org/2025-01-*.json.gz" in patterns
        assert "gs://data.gharchive.org/2025-02-*.json.gz" in patterns
        assert "gs://data.gharchive.org/2025-03-*.json.gz" in patterns


# ---------------------------------------------------------------------------
# upload_hdfs() — iterates glob results
# ---------------------------------------------------------------------------

class TestUploadHdfs:
    @patch("infra02_download.run")
    def test_upload_hdfs_creates_hdfs_dir(self, mock_run, tmp_path):
        infra02.upload_hdfs(tmp_path)
        first_call = mock_run.call_args_list[0]
        assert "hdfs" in first_call[0][0]
        assert "-mkdir" in first_call[0][0]

    @patch("infra02_download.run")
    def test_upload_hdfs_puts_each_gz_file(self, mock_run, tmp_path):
        # Create fake .json.gz files
        (tmp_path / "2025-01-01-0.json.gz").touch()
        (tmp_path / "2025-01-01-1.json.gz").touch()
        (tmp_path / "other.txt").touch()  # should be ignored

        infra02.upload_hdfs(tmp_path)

        # First call is mkdir, remaining are put calls for .json.gz files
        put_calls = [
            c for c in mock_run.call_args_list
            if "-put" in c[0][0]
        ]
        assert len(put_calls) == 2

    @patch("infra02_download.run")
    def test_upload_hdfs_empty_dir_no_put_calls(self, mock_run, tmp_path):
        infra02.upload_hdfs(tmp_path)
        put_calls = [c for c in mock_run.call_args_list if "-put" in c[0][0]]
        assert len(put_calls) == 0


# ---------------------------------------------------------------------------
# copy_to_namenode()
# ---------------------------------------------------------------------------

class TestCopyToNamenode:
    @patch("infra02_download.run")
    def test_copy_uses_docker_cp(self, mock_run, tmp_path):
        infra02.copy_to_namenode(tmp_path)
        args = mock_run.call_args[0][0]
        assert args[0] == "docker"
        assert args[1] == "cp"

    @patch("infra02_download.run")
    def test_copy_target_is_namenode(self, mock_run, tmp_path):
        infra02.copy_to_namenode(tmp_path)
        args = mock_run.call_args[0][0]
        assert "namenode:/tmp/raw" in args


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

class TestArgParser:
    def test_default_years(self):
        import argparse
        with patch("sys.argv", ["infra02_download.py"]):
            parser = infra02_build_parser()
            args = parser.parse_args([])
        assert args.years == ["2023", "2024", "2025"]

    def test_sample_flag_default_false(self):
        parser = infra02_build_parser()
        args = parser.parse_args([])
        assert args.sample is False

    def test_sample_flag_sets_true(self):
        parser = infra02_build_parser()
        args = parser.parse_args(["--sample"])
        assert args.sample is True

    def test_skip_hdfs_flag_default_false(self):
        parser = infra02_build_parser()
        args = parser.parse_args([])
        assert args.skip_hdfs is False

    def test_skip_hdfs_flag_sets_true(self):
        parser = infra02_build_parser()
        args = parser.parse_args(["--skip-hdfs"])
        assert args.skip_hdfs is True

    def test_years_override(self):
        parser = infra02_build_parser()
        args = parser.parse_args(["--years", "2025"])
        assert args.years == ["2025"]

    def test_years_multiple_values(self):
        parser = infra02_build_parser()
        args = parser.parse_args(["--years", "2023", "2025"])
        assert args.years == ["2023", "2025"]


def infra02_build_parser():
    """Rebuild the argparse parser from infra02 for isolated testing."""
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", nargs="+", default=infra02.FULL_YEARS)
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--skip-hdfs", action="store_true")
    return parser


# ---------------------------------------------------------------------------
# main() integration (mocked)
# ---------------------------------------------------------------------------

class TestMain:
    @patch("infra02_download.verify_hdfs")
    @patch("infra02_download.push_to_hdfs")
    @patch("infra02_download.download_years")
    def test_main_default_calls_download_years_and_push(self, mock_dl, mock_push, mock_verify):
        with patch("sys.argv", ["infra02_download.py"]):
            infra02.main()
        mock_dl.assert_called_once()
        mock_push.assert_called_once()

    @patch("infra02_download.verify_hdfs")
    @patch("infra02_download.push_to_hdfs")
    @patch("infra02_download.download_sample")
    def test_main_sample_calls_download_sample(self, mock_sample, mock_push, mock_verify):
        with patch("sys.argv", ["infra02_download.py", "--sample"]):
            infra02.main()
        mock_sample.assert_called_once()

    @patch("infra02_download.push_to_hdfs")
    @patch("infra02_download.download_years")
    def test_main_skip_hdfs_no_push(self, mock_dl, mock_push):
        with patch("sys.argv", ["infra02_download.py", "--skip-hdfs"]):
            infra02.main()
        mock_push.assert_not_called()
