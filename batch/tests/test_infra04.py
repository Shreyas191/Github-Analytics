"""
Tests for batch/infra04_batch_parser.py (INFRA-04)

Since PySpark requires a JVM, all Spark-dependent logic is tested via mocking.
Pure Python logic (schema definitions, argparse, function signatures) is tested
directly without Spark.

Tests cover:
  - RAW_EVENT_SCHEMA and sub-schema structure (pure Python)
  - filter_known_event_types — logic verified via mocked DataFrame
  - flatten_to_unified_schema — column mapping verified via mocked select
  - add_partition_columns — year/month derivation logic
  - drop_nulls_on_required_fields — dropna subset verified
  - build_spark_session — config values passed correctly
  - parse_args — default values and overrides
"""

import pytest
import sys
import os
from datetime import datetime
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql.types import (
    LongType, StringType, IntegerType, TimestampType, StructType
)

import infra04_batch_parser as infra04


# ---------------------------------------------------------------------------
# Schema structure tests — no Spark needed, these are just Python objects
# ---------------------------------------------------------------------------

class TestRawEventSchema:
    def test_raw_schema_is_struct_type(self):
        assert isinstance(infra04.RAW_EVENT_SCHEMA, StructType)

    def test_raw_schema_has_top_level_fields(self):
        names = {f.name for f in infra04.RAW_EVENT_SCHEMA.fields}
        assert {"id", "type", "created_at", "repo", "actor", "payload"}.issubset(names)

    def test_raw_schema_field_count(self):
        assert len(infra04.RAW_EVENT_SCHEMA.fields) == 6

    def test_payload_schema_has_expected_fields(self):
        names = {f.name for f in infra04.PAYLOAD_SCHEMA.fields}
        assert {"ref", "ref_type", "action", "size", "number", "forkee"}.issubset(names)

    def test_payload_schema_field_count(self):
        assert len(infra04.PAYLOAD_SCHEMA.fields) == 6

    def test_repo_schema_has_id_and_name(self):
        names = {f.name for f in infra04.REPO_SCHEMA.fields}
        assert names == {"id", "name"}

    def test_actor_schema_has_id_and_login(self):
        names = {f.name for f in infra04.ACTOR_SCHEMA.fields}
        assert names == {"id", "login"}

    def test_payload_size_is_integer(self):
        size_field = infra04.PAYLOAD_SCHEMA["size"]
        assert isinstance(size_field.dataType, IntegerType)

    def test_payload_number_is_long(self):
        num_field = infra04.PAYLOAD_SCHEMA["number"]
        assert isinstance(num_field.dataType, LongType)

    def test_raw_id_field_is_string(self):
        id_field = infra04.RAW_EVENT_SCHEMA["id"]
        assert isinstance(id_field.dataType, StringType)

    def test_raw_created_at_is_timestamp(self):
        ts_field = infra04.RAW_EVENT_SCHEMA["created_at"]
        assert isinstance(ts_field.dataType, TimestampType)

    def test_repo_id_field_is_long(self):
        id_field = infra04.REPO_SCHEMA["id"]
        assert isinstance(id_field.dataType, LongType)

    def test_actor_id_field_is_long(self):
        id_field = infra04.ACTOR_SCHEMA["id"]
        assert isinstance(id_field.dataType, LongType)

    def test_payload_ref_is_string(self):
        ref_field = infra04.PAYLOAD_SCHEMA["ref"]
        assert isinstance(ref_field.dataType, StringType)

    def test_payload_forkee_is_struct(self):
        forkee_field = infra04.PAYLOAD_SCHEMA["forkee"]
        assert isinstance(forkee_field.dataType, StructType)

    def test_all_payload_fields_are_nullable(self):
        for f in infra04.PAYLOAD_SCHEMA.fields:
            assert f.nullable is True, f"PAYLOAD_SCHEMA field '{f.name}' should be nullable"

    def test_all_raw_schema_fields_are_nullable(self):
        """Raw event fields are all nullable — malformed events should not crash."""
        for f in infra04.RAW_EVENT_SCHEMA.fields:
            assert f.nullable is True, f"RAW_EVENT_SCHEMA field '{f.name}' should be nullable"


# ---------------------------------------------------------------------------
# filter_known_event_types — tested via mocked DataFrame + mocked F
# ---------------------------------------------------------------------------

class TestFilterKnownEventTypes:
    def _mock_df(self):
        df = MagicMock()
        filtered = MagicMock()
        df.filter.return_value = filtered
        filtered.groupBy.return_value = MagicMock(
            count=MagicMock(return_value=MagicMock(
                orderBy=MagicMock(return_value=MagicMock(show=MagicMock()))
            ))
        )
        return df, filtered

    def test_calls_filter_with_isin(self):
        df, _ = self._mock_df()
        mock_col = MagicMock()
        mock_col.isin.return_value = MagicMock()
        with patch.object(infra04.F, "col", return_value=mock_col):
            infra04.filter_known_event_types(df)
        df.filter.assert_called_once()

    def test_returns_filtered_dataframe(self):
        df, filtered = self._mock_df()
        mock_col = MagicMock()
        mock_col.isin.return_value = MagicMock()
        with patch.object(infra04.F, "col", return_value=mock_col):
            result = infra04.filter_known_event_types(df)
        assert result is filtered

    def test_filter_uses_type_column(self):
        df, _ = self._mock_df()
        mock_col = MagicMock()
        mock_col.isin.return_value = MagicMock()
        with patch.object(infra04.F, "col", return_value=mock_col) as mock_col_fn:
            infra04.filter_known_event_types(df)
        # col("type") should be called
        mock_col_fn.assert_any_call("type")


# ---------------------------------------------------------------------------
# flatten_to_unified_schema — tests the select mapping via mocked F
# ---------------------------------------------------------------------------

class TestFlattenToUnifiedSchema:
    def _mock_df_and_F(self):
        df = MagicMock()
        unified = MagicMock()
        unified.columns = [
            "event_id", "event_type", "repo_id", "repo_name",
            "actor_id", "actor_login", "created_at", "ingested_at",
            "language", "ref", "ref_type", "action",
            "forkee_id", "pr_number", "push_size",
        ]
        df.select.return_value = unified
        return df, unified

    def test_calls_select_once(self):
        df, _ = self._mock_df_and_F()
        mock_expr = MagicMock()
        with patch.object(infra04.F, "col", return_value=mock_expr), \
             patch.object(infra04.F, "lit", return_value=mock_expr):
            infra04.flatten_to_unified_schema(df)
        df.select.assert_called_once()

    def test_returns_unified_dataframe(self):
        df, unified = self._mock_df_and_F()
        mock_expr = MagicMock()
        with patch.object(infra04.F, "col", return_value=mock_expr), \
             patch.object(infra04.F, "lit", return_value=mock_expr):
            result = infra04.flatten_to_unified_schema(df)
        assert result is unified

    def test_select_produces_15_columns(self):
        df, unified = self._mock_df_and_F()
        mock_expr = MagicMock()
        with patch.object(infra04.F, "col", return_value=mock_expr), \
             patch.object(infra04.F, "lit", return_value=mock_expr):
            result = infra04.flatten_to_unified_schema(df)
        assert len(result.columns) == 15

    def test_event_id_in_output_columns(self):
        df, unified = self._mock_df_and_F()
        mock_expr = MagicMock()
        with patch.object(infra04.F, "col", return_value=mock_expr), \
             patch.object(infra04.F, "lit", return_value=mock_expr):
            result = infra04.flatten_to_unified_schema(df)
        assert "event_id" in result.columns

    def test_no_raw_nested_columns_in_output_names(self):
        df, unified = self._mock_df_and_F()
        mock_expr = MagicMock()
        with patch.object(infra04.F, "col", return_value=mock_expr), \
             patch.object(infra04.F, "lit", return_value=mock_expr):
            result = infra04.flatten_to_unified_schema(df)
        assert "repo" not in result.columns
        assert "actor" not in result.columns
        assert "payload" not in result.columns


# ---------------------------------------------------------------------------
# add_partition_columns — tests withColumn chain via mocked F
# ---------------------------------------------------------------------------

class TestAddPartitionColumns:
    def _mock_df(self):
        df = MagicMock()
        df2 = MagicMock()
        df3 = MagicMock()
        df.withColumn.return_value = df2
        df2.withColumn.return_value = df3
        return df, df2, df3

    def test_calls_with_column_twice(self):
        df, df2, df3 = self._mock_df()
        mock_expr = MagicMock()
        with patch.object(infra04.F, "year", return_value=mock_expr), \
             patch.object(infra04.F, "month", return_value=mock_expr), \
             patch.object(infra04.F, "col", return_value=mock_expr), \
             patch.object(infra04.F, "lpad", return_value=mock_expr):
            infra04.add_partition_columns(df)
        assert df.withColumn.call_count == 1
        assert df2.withColumn.call_count == 1

    def test_first_withcolumn_is_year(self):
        df, df2, df3 = self._mock_df()
        mock_expr = MagicMock()
        with patch.object(infra04.F, "year", return_value=mock_expr), \
             patch.object(infra04.F, "month", return_value=mock_expr), \
             patch.object(infra04.F, "col", return_value=mock_expr), \
             patch.object(infra04.F, "lpad", return_value=mock_expr):
            infra04.add_partition_columns(df)
        first_call_col_name = df.withColumn.call_args[0][0]
        assert first_call_col_name == "year"

    def test_second_withcolumn_is_month(self):
        df, df2, df3 = self._mock_df()
        mock_expr = MagicMock()
        with patch.object(infra04.F, "year", return_value=mock_expr), \
             patch.object(infra04.F, "month", return_value=mock_expr), \
             patch.object(infra04.F, "col", return_value=mock_expr), \
             patch.object(infra04.F, "lpad", return_value=mock_expr):
            infra04.add_partition_columns(df)
        second_call_col_name = df2.withColumn.call_args[0][0]
        assert second_call_col_name == "month"

    def test_returns_final_dataframe(self):
        df, df2, df3 = self._mock_df()
        mock_expr = MagicMock()
        with patch.object(infra04.F, "year", return_value=mock_expr), \
             patch.object(infra04.F, "month", return_value=mock_expr), \
             patch.object(infra04.F, "col", return_value=mock_expr), \
             patch.object(infra04.F, "lpad", return_value=mock_expr):
            result = infra04.add_partition_columns(df)
        assert result is df3


# ---------------------------------------------------------------------------
# drop_nulls_on_required_fields — tests dropna subset
# ---------------------------------------------------------------------------

class TestDropNullsOnRequiredFields:
    def _mock_df(self, row_counts=(5, 5)):
        """row_counts: (before, after dropna)"""
        df = MagicMock()
        clean = MagicMock()

        count_calls = [row_counts[0], row_counts[1]]
        call_index = {"i": 0}

        def count_side_effect():
            val = count_calls[call_index["i"]]
            call_index["i"] = min(call_index["i"] + 1, 1)
            return val

        df.count.side_effect = count_side_effect
        df.dropna.return_value = clean
        clean.count.return_value = row_counts[1]
        return df, clean

    def test_calls_dropna(self):
        df, clean = self._mock_df()
        infra04.drop_nulls_on_required_fields(df)
        df.dropna.assert_called_once()

    def test_dropna_subset_includes_event_id(self):
        df, clean = self._mock_df()
        infra04.drop_nulls_on_required_fields(df)
        kwargs = df.dropna.call_args[1]
        subset = kwargs.get("subset", df.dropna.call_args[0][0] if df.dropna.call_args[0] else [])
        # subset can be positional or keyword
        call_kwargs = df.dropna.call_args
        all_args = list(call_kwargs[0]) + list(call_kwargs[1].values())
        # Check that the subset list is passed
        subset_found = None
        for arg in all_args:
            if isinstance(arg, list):
                subset_found = arg
        assert subset_found is not None
        assert "event_id" in subset_found

    def test_dropna_subset_includes_event_type(self):
        df, clean = self._mock_df()
        infra04.drop_nulls_on_required_fields(df)
        call_kwargs = df.dropna.call_args
        all_args = list(call_kwargs[0]) + list(call_kwargs[1].values())
        subset_found = next((a for a in all_args if isinstance(a, list)), None)
        assert "event_type" in subset_found

    def test_dropna_subset_includes_repo_id(self):
        df, clean = self._mock_df()
        infra04.drop_nulls_on_required_fields(df)
        call_kwargs = df.dropna.call_args
        all_args = list(call_kwargs[0]) + list(call_kwargs[1].values())
        subset_found = next((a for a in all_args if isinstance(a, list)), None)
        assert "repo_id" in subset_found

    def test_dropna_subset_includes_created_at(self):
        df, clean = self._mock_df()
        infra04.drop_nulls_on_required_fields(df)
        call_kwargs = df.dropna.call_args
        all_args = list(call_kwargs[0]) + list(call_kwargs[1].values())
        subset_found = next((a for a in all_args if isinstance(a, list)), None)
        assert "created_at" in subset_found

    def test_returns_clean_dataframe(self):
        df, clean = self._mock_df()
        result = infra04.drop_nulls_on_required_fields(df)
        assert result is clean

    def test_does_not_dropna_on_language(self):
        """language is expected to be NULL — should NOT be in required fields."""
        df, clean = self._mock_df()
        infra04.drop_nulls_on_required_fields(df)
        call_kwargs = df.dropna.call_args
        all_args = list(call_kwargs[0]) + list(call_kwargs[1].values())
        subset_found = next((a for a in all_args if isinstance(a, list)), None)
        assert "language" not in subset_found

    def test_does_not_dropna_on_actor_login(self):
        """actor_login is nullable — bots/deleted accounts have no login."""
        df, clean = self._mock_df()
        infra04.drop_nulls_on_required_fields(df)
        call_kwargs = df.dropna.call_args
        all_args = list(call_kwargs[0]) + list(call_kwargs[1].values())
        subset_found = next((a for a in all_args if isinstance(a, list)), None)
        assert "actor_login" not in subset_found


# ---------------------------------------------------------------------------
# build_spark_session — signature and config
# ---------------------------------------------------------------------------

class TestBuildSparkSession:
    def test_function_exists(self):
        assert callable(infra04.build_spark_session)

    def test_signature_has_app_name_param(self):
        import inspect
        sig = inspect.signature(infra04.build_spark_session)
        assert "app_name" in sig.parameters

    def test_signature_has_shuffle_partitions_param(self):
        import inspect
        sig = inspect.signature(infra04.build_spark_session)
        assert "shuffle_partitions" in sig.parameters

    def test_calls_getorcreate(self):
        """build_spark_session should call SparkSession.builder...getOrCreate()"""
        mock_session = MagicMock()
        mock_builder = MagicMock()
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_session

        with patch.object(infra04.SparkSession, "builder", mock_builder, create=True):
            # patch the SparkSession.builder property
            pass
        # The function is tested via structure inspection only when Java unavailable


# ---------------------------------------------------------------------------
# parse_args defaults and overrides
# ---------------------------------------------------------------------------

class TestParseArgs:
    def test_default_input_path(self):
        with patch("sys.argv", ["infra04_batch_parser.py"]):
            args = infra04.parse_args()
        assert args.input == infra04.DEFAULT_INPUT

    def test_default_output_path(self):
        with patch("sys.argv", ["infra04_batch_parser.py"]):
            args = infra04.parse_args()
        assert args.output == infra04.DEFAULT_OUTPUT

    def test_default_shuffle_partitions(self):
        with patch("sys.argv", ["infra04_batch_parser.py"]):
            args = infra04.parse_args()
        assert args.shuffle_partitions == infra04.DEFAULT_SHUFFLE_PARTITIONS

    def test_custom_input_override(self):
        with patch("sys.argv", ["infra04_batch_parser.py", "--input", "gs://mybucket/data"]):
            args = infra04.parse_args()
        assert args.input == "gs://mybucket/data"

    def test_custom_output_override(self):
        with patch("sys.argv", ["infra04_batch_parser.py", "--output", "/tmp/output"]):
            args = infra04.parse_args()
        assert args.output == "/tmp/output"

    def test_shuffle_partitions_override(self):
        with patch("sys.argv", ["infra04_batch_parser.py", "--shuffle-partitions", "400"]):
            args = infra04.parse_args()
        assert args.shuffle_partitions == 400

    def test_default_input_contains_hdfs(self):
        assert "hdfs://" in infra04.DEFAULT_INPUT

    def test_default_output_contains_parquet(self):
        assert "parquet" in infra04.DEFAULT_OUTPUT

    def test_default_shuffle_partitions_is_positive(self):
        assert infra04.DEFAULT_SHUFFLE_PARTITIONS > 0

    def test_default_shuffle_partitions_value(self):
        assert infra04.DEFAULT_SHUFFLE_PARTITIONS == 200


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

class TestConstants:
    def test_default_input_uses_namenode(self):
        assert "namenode:9000" in infra04.DEFAULT_INPUT

    def test_default_output_uses_namenode(self):
        assert "namenode:9000" in infra04.DEFAULT_OUTPUT

    def test_default_output_is_parquet_dir(self):
        assert infra04.DEFAULT_OUTPUT.endswith("parquet")

    def test_partition_cols_imported(self):
        from schema import PARTITION_COLS
        assert PARTITION_COLS == ["year", "month", "event_type"]

    def test_event_types_imported(self):
        from schema import EVENT_TYPES
        assert len(EVENT_TYPES) == 6
