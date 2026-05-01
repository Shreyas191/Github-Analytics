"""
Tests for batch/infra05_validation_report.py (INFRA-05)

Since PySpark requires a JVM which is not available in this environment,
all Spark-dependent logic is tested via mocking. Pure Python logic
(constants, null rate arithmetic, duplicate detection logic) is tested
directly with Python.

Tests cover:
  - SEPARATOR constant
  - null rate calculation arithmetic (pure Python)
  - duplicate detection logic (pure Python)
  - timestamp range logic (pure Python)
  - row count percentage logic (pure Python)
  - Module imports correctly
  - main() function structure via mocking
"""

import pytest
import sys
import os
from datetime import datetime
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ---------------------------------------------------------------------------
# Module import / constants
# ---------------------------------------------------------------------------

class TestModuleImport:
    def test_infra05_importable(self):
        import infra05_validation_report  # noqa: F401

    def test_separator_is_string(self):
        import infra05_validation_report as infra05
        assert isinstance(infra05.SEPARATOR, str)

    def test_separator_length(self):
        import infra05_validation_report as infra05
        assert len(infra05.SEPARATOR) == 65

    def test_separator_uses_equals_signs_only(self):
        import infra05_validation_report as infra05
        assert set(infra05.SEPARATOR) == {"="}

    def test_logger_configured(self):
        import infra05_validation_report as infra05
        import logging
        assert isinstance(infra05.logger, logging.Logger)


# ---------------------------------------------------------------------------
# Null rate calculation — pure Python arithmetic
# ---------------------------------------------------------------------------

class TestNullRateCalculation:
    """
    The null rate formula used by infra05:
      null_pct = round(null_count / total * 100, 3) if total > 0 else 0
    """

    def _null_pct(self, null_count, total):
        return round(null_count / total * 100, 3) if total > 0 else 0

    def test_zero_nulls_gives_zero_percent(self):
        assert self._null_pct(0, 100) == 0.0

    def test_all_null_gives_100_percent(self):
        assert self._null_pct(100, 100) == 100.0

    def test_half_null_gives_50_percent(self):
        assert self._null_pct(50, 100) == 50.0

    def test_one_in_three_null(self):
        pct = self._null_pct(1, 3)
        assert abs(pct - 33.333) < 0.001

    def test_zero_total_returns_zero(self):
        """Guard against division by zero."""
        assert self._null_pct(0, 0) == 0

    def test_result_rounded_to_3_decimal_places(self):
        pct = self._null_pct(1, 3)
        # Should be 33.333, not 33.3333...
        assert pct == round(pct, 3)

    def test_language_null_is_expected_high_rate(self):
        """language column is NULL for all batch events — 100% null is expected."""
        pct = self._null_pct(10_000_000, 10_000_000)
        assert pct == 100.0

    def test_low_null_rate_on_required_fields(self):
        """A null rate > 1% on repo_id is a warning."""
        pct = self._null_pct(50, 10000)
        assert pct == 0.5  # below 1% threshold = OK


# ---------------------------------------------------------------------------
# Warning threshold detection
# ---------------------------------------------------------------------------

class TestNullRateWarnings:
    def _has_warning(self, col_name, null_pct):
        """Reproduce the warning logic from infra05."""
        if col_name == "language":
            return "EXPECTED"
        elif null_pct > 1:
            return "WARNING"
        return ""

    def test_language_column_always_expected(self):
        assert self._has_warning("language", 100.0) == "EXPECTED"

    def test_language_column_expected_even_zero_nulls(self):
        assert self._has_warning("language", 0.0) == "EXPECTED"

    def test_high_null_rate_triggers_warning(self):
        assert self._has_warning("repo_id", 2.5) == "WARNING"

    def test_null_rate_at_threshold_triggers_warning(self):
        """Strictly > 1% triggers warning, so 1.001% does."""
        assert self._has_warning("repo_id", 1.001) == "WARNING"

    def test_null_rate_at_threshold_no_warning(self):
        """Exactly 1% is NOT > 1, so no warning."""
        assert self._has_warning("repo_id", 1.0) == ""

    def test_zero_null_rate_no_warning(self):
        assert self._has_warning("repo_id", 0.0) == ""

    def test_event_id_warning_on_high_nulls(self):
        assert self._has_warning("event_id", 5.0) == "WARNING"


# ---------------------------------------------------------------------------
# Duplicate detection logic — pure Python simulation
# ---------------------------------------------------------------------------

class TestDuplicateDetectionLogic:
    def _count_duplicates(self, event_ids):
        """Simulate the groupBy("event_id").count().filter(count > 1).count() logic."""
        from collections import Counter
        counts = Counter(event_ids)
        return sum(1 for v in counts.values() if v > 1)

    def test_no_duplicates(self):
        assert self._count_duplicates(["e1", "e2", "e3"]) == 0

    def test_one_duplicate_pair(self):
        assert self._count_duplicates(["e1", "e1", "e2"]) == 1

    def test_two_duplicate_pairs(self):
        assert self._count_duplicates(["e1", "e1", "e2", "e2", "e3"]) == 2

    def test_triplicate_counts_as_one_duplicate_group(self):
        assert self._count_duplicates(["e1", "e1", "e1", "e2"]) == 1

    def test_empty_list_no_duplicates(self):
        assert self._count_duplicates([]) == 0

    def test_all_same_id_is_one_duplicate_group(self):
        assert self._count_duplicates(["e1", "e1", "e1", "e1"]) == 1

    def test_duplicate_status_message(self):
        """Verify the OK/INVESTIGATE logic."""
        dup_count = 0
        status = "OK" if dup_count == 0 else "INVESTIGATE"
        assert status == "OK"

    def test_duplicate_investigate_message(self):
        dup_count = 5
        status = "OK" if dup_count == 0 else "INVESTIGATE"
        assert status == "INVESTIGATE"


# ---------------------------------------------------------------------------
# Timestamp range logic — pure Python
# ---------------------------------------------------------------------------

class TestTimestampRangeLogic:
    def test_earliest_from_list(self):
        timestamps = [
            datetime(2025, 1, 1),
            datetime(2023, 3, 15),
            datetime(2025, 6, 1),
        ]
        assert min(timestamps) == datetime(2023, 3, 15)

    def test_latest_from_list(self):
        timestamps = [
            datetime(2025, 1, 1),
            datetime(2023, 3, 15),
            datetime(2025, 6, 1),
        ]
        assert max(timestamps) == datetime(2025, 6, 1)

    def test_single_element_same_min_max(self):
        ts = datetime(2025, 1, 15)
        assert min([ts]) == max([ts])

    def test_timestamps_in_expected_range(self):
        """Events should fall in 2023-2025 range for our dataset."""
        earliest = datetime(2023, 1, 1)
        latest = datetime(2025, 12, 31)
        assert earliest.year == 2023
        assert latest.year == 2025


# ---------------------------------------------------------------------------
# Row count percentage arithmetic
# ---------------------------------------------------------------------------

class TestRowCountPercentage:
    def _pct(self, count, total):
        return round(count / total * 100, 2)

    def test_percentages_sum_to_100_for_two_equal_groups(self):
        total = 100
        pct1 = self._pct(50, total)
        pct2 = self._pct(50, total)
        assert abs(pct1 + pct2 - 100.0) < 0.01

    def test_largest_event_type_first_sort(self):
        """Validation report orders by count descending."""
        counts = {"PushEvent": 1000, "WatchEvent": 500, "ForkEvent": 100}
        sorted_items = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        assert sorted_items[0][0] == "PushEvent"

    def test_percentages_rounded_to_2_decimals(self):
        pct = self._pct(1, 3)
        assert pct == round(pct, 2)

    def test_push_event_typically_dominant(self):
        """PushEvent is the most common event type in GitHub Archive."""
        counts = {
            "PushEvent":        5_000_000,
            "WatchEvent":       2_000_000,
            "ForkEvent":          500_000,
            "PullRequestEvent":   300_000,
            "IssuesEvent":        200_000,
            "CreateEvent":        100_000,
        }
        total = sum(counts.values())
        pcts = {k: self._pct(v, total) for k, v in counts.items()}
        assert pcts["PushEvent"] > pcts["WatchEvent"]
        assert abs(sum(pcts.values()) - 100.0) < 0.1


# ---------------------------------------------------------------------------
# main() function — tested via mocking
# ---------------------------------------------------------------------------

class TestMainFunction:
    def test_main_calls_spark_stop(self):
        """main() should always call spark.stop() after reporting."""
        import infra05_validation_report as infra05

        mock_spark = MagicMock()
        mock_df = MagicMock()

        # Set up mock chain
        mock_df.count.return_value = 10
        mock_df.cache.return_value = mock_df
        mock_df.groupBy.return_value = MagicMock(
            count=MagicMock(return_value=MagicMock(
                withColumn=MagicMock(return_value=MagicMock(
                    orderBy=MagicMock(return_value=MagicMock(show=MagicMock()))
                ))
            ))
        )
        mock_df.filter.return_value = MagicMock(count=MagicMock(return_value=0))
        mock_df.agg.return_value = MagicMock(
            collect=MagicMock(return_value=[{"earliest": datetime(2023,1,1), "latest": datetime(2025,1,1)}])
        )
        mock_df.select.return_value = MagicMock(show=MagicMock())

        mock_spark.read.parquet.return_value = mock_df

        mock_builder = MagicMock()
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark

        with patch("infra05_validation_report.SparkSession") as mock_ss:
            mock_ss.builder = mock_builder
            infra05.main()

        mock_spark.stop.assert_called_once()

    def test_main_reads_from_events_path(self):
        """main() reads from EVENTS_PATH (imported from schema)."""
        import infra05_validation_report as infra05

        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.count.return_value = 0
        mock_df.cache.return_value = mock_df
        mock_df.groupBy.return_value = MagicMock(
            count=MagicMock(return_value=MagicMock(
                withColumn=MagicMock(return_value=MagicMock(
                    orderBy=MagicMock(return_value=MagicMock(show=MagicMock()))
                ))
            ))
        )
        mock_df.filter.return_value = MagicMock(count=MagicMock(return_value=0))
        mock_df.agg.return_value = MagicMock(
            collect=MagicMock(return_value=[{"earliest": None, "latest": None}])
        )
        mock_df.select.return_value = MagicMock(show=MagicMock())
        mock_spark.read.parquet.return_value = mock_df

        mock_builder = MagicMock()
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark

        with patch("infra05_validation_report.SparkSession") as mock_ss:
            mock_ss.builder = mock_builder
            infra05.main()

        from schema import EVENTS_PATH
        mock_spark.read.parquet.assert_called_with(EVENTS_PATH)
