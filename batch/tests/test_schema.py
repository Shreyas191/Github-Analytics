"""
Tests for batch/schema.py (INFRA-03)

Tests cover:
  - EVENTS_SCHEMA structure (field names, types, nullability)
  - EVENT_TYPES set contents
  - EVENT_TYPE_FIELDS applicability map correctness
  - HDFS path constants
  - PARTITION_COLS list
"""

import pytest
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructType,
    TimestampType,
)

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from schema import (
    EVENTS_SCHEMA,
    EVENT_TYPES,
    EVENT_TYPE_FIELDS,
    HDFS_BASE,
    EVENTS_PATH,
    LABELS_PATH,
    FEATURES_PATH,
    ENRICHMENT_PATH,
    TRAIN_PATH,
    HOLDOUT_PATH,
    MODEL_PATH,
    CHECKPOINTS_PATH,
    PARTITION_COLS,
)


# ---------------------------------------------------------------------------
# EVENTS_SCHEMA structural tests
# ---------------------------------------------------------------------------

class TestEventsSchema:
    def test_schema_is_struct_type(self):
        assert isinstance(EVENTS_SCHEMA, StructType)

    def test_schema_has_expected_field_count(self):
        # Core (7: event_id, event_type, repo_id, repo_name, actor_id, actor_login, created_at)
        # + ingested_at (1) + language (1) + payload (6: ref, ref_type, action, forkee_id, pr_number, push_size)
        # = 15 fields total
        assert len(EVENTS_SCHEMA.fields) == 15

    def test_required_field_names_present(self):
        names = {f.name for f in EVENTS_SCHEMA.fields}
        required = {
            "event_id", "event_type", "repo_id", "repo_name",
            "actor_id", "actor_login", "created_at", "ingested_at",
            "language", "ref", "ref_type", "action", "forkee_id",
            "pr_number", "push_size",
        }
        assert required == names

    # --- field types ---
    def _field(self, name):
        return EVENTS_SCHEMA[name]

    def test_event_id_is_string(self):
        assert isinstance(self._field("event_id").dataType, StringType)

    def test_event_type_is_string(self):
        assert isinstance(self._field("event_type").dataType, StringType)

    def test_repo_id_is_long(self):
        assert isinstance(self._field("repo_id").dataType, LongType)

    def test_actor_id_is_long(self):
        assert isinstance(self._field("actor_id").dataType, LongType)

    def test_forkee_id_is_long(self):
        assert isinstance(self._field("forkee_id").dataType, LongType)

    def test_pr_number_is_long(self):
        assert isinstance(self._field("pr_number").dataType, LongType)

    def test_push_size_is_integer(self):
        assert isinstance(self._field("push_size").dataType, IntegerType)

    def test_created_at_is_timestamp(self):
        assert isinstance(self._field("created_at").dataType, TimestampType)

    def test_ingested_at_is_timestamp(self):
        assert isinstance(self._field("ingested_at").dataType, TimestampType)

    def test_repo_name_is_string(self):
        assert isinstance(self._field("repo_name").dataType, StringType)

    def test_language_is_string(self):
        assert isinstance(self._field("language").dataType, StringType)

    # --- nullability ---
    def test_event_id_not_nullable(self):
        assert self._field("event_id").nullable is False

    def test_event_type_not_nullable(self):
        assert self._field("event_type").nullable is False

    def test_repo_id_not_nullable(self):
        assert self._field("repo_id").nullable is False

    def test_repo_name_not_nullable(self):
        assert self._field("repo_name").nullable is False

    def test_actor_id_not_nullable(self):
        assert self._field("actor_id").nullable is False

    def test_created_at_not_nullable(self):
        assert self._field("created_at").nullable is False

    def test_actor_login_is_nullable(self):
        # Bot/deleted accounts may have no login
        assert self._field("actor_login").nullable is True

    def test_ingested_at_is_nullable(self):
        # NULL for historical batch data
        assert self._field("ingested_at").nullable is True

    def test_language_is_nullable(self):
        assert self._field("language").nullable is True

    def test_payload_fields_are_nullable(self):
        nullable_payload = ["ref", "ref_type", "action", "forkee_id", "pr_number", "push_size"]
        for name in nullable_payload:
            assert self._field(name).nullable is True, f"{name} should be nullable"


# ---------------------------------------------------------------------------
# EVENT_TYPES tests
# ---------------------------------------------------------------------------

class TestEventTypes:
    def test_event_types_is_set(self):
        assert isinstance(EVENT_TYPES, set)

    def test_has_exactly_six_types(self):
        assert len(EVENT_TYPES) == 6

    def test_contains_push_event(self):
        assert "PushEvent" in EVENT_TYPES

    def test_contains_watch_event(self):
        assert "WatchEvent" in EVENT_TYPES

    def test_contains_fork_event(self):
        assert "ForkEvent" in EVENT_TYPES

    def test_contains_pull_request_event(self):
        assert "PullRequestEvent" in EVENT_TYPES

    def test_contains_issues_event(self):
        assert "IssuesEvent" in EVENT_TYPES

    def test_contains_create_event(self):
        assert "CreateEvent" in EVENT_TYPES

    def test_no_unknown_types(self):
        # Ensure no typos slipped in
        known = {"PushEvent", "WatchEvent", "ForkEvent", "PullRequestEvent", "IssuesEvent", "CreateEvent"}
        assert EVENT_TYPES == known

    def test_event_types_are_strings(self):
        for t in EVENT_TYPES:
            assert isinstance(t, str)

    def test_event_types_end_with_event(self):
        for t in EVENT_TYPES:
            assert t.endswith("Event"), f"Expected '{t}' to end with 'Event'"


# ---------------------------------------------------------------------------
# EVENT_TYPE_FIELDS applicability map
# ---------------------------------------------------------------------------

class TestEventTypeFields:
    def test_all_event_types_have_entries(self):
        for event_type in EVENT_TYPES:
            assert event_type in EVENT_TYPE_FIELDS, f"{event_type} missing from EVENT_TYPE_FIELDS"

    def test_each_entry_has_six_payload_keys(self):
        expected_keys = {"ref", "ref_type", "action", "forkee_id", "pr_number", "push_size"}
        for event_type, fields in EVENT_TYPE_FIELDS.items():
            assert set(fields.keys()) == expected_keys, f"{event_type} has wrong keys"

    def test_all_values_are_bool(self):
        for event_type, fields in EVENT_TYPE_FIELDS.items():
            for field, val in fields.items():
                assert isinstance(val, bool), f"{event_type}.{field} should be bool, got {type(val)}"

    # PushEvent: ref=True, push_size=True, rest False
    def test_push_event_has_ref_and_push_size(self):
        push = EVENT_TYPE_FIELDS["PushEvent"]
        assert push["ref"] is True
        assert push["push_size"] is True
        assert push["ref_type"] is False
        assert push["action"] is False
        assert push["forkee_id"] is False
        assert push["pr_number"] is False

    # WatchEvent: all False
    def test_watch_event_has_no_payload_fields(self):
        watch = EVENT_TYPE_FIELDS["WatchEvent"]
        assert all(v is False for v in watch.values())

    # ForkEvent: forkee_id=True, rest False
    def test_fork_event_has_only_forkee_id(self):
        fork = EVENT_TYPE_FIELDS["ForkEvent"]
        assert fork["forkee_id"] is True
        assert fork["ref"] is False
        assert fork["ref_type"] is False
        assert fork["action"] is False
        assert fork["pr_number"] is False
        assert fork["push_size"] is False

    # PullRequestEvent: action=True, pr_number=True, rest False
    def test_pull_request_event_has_action_and_pr_number(self):
        pr = EVENT_TYPE_FIELDS["PullRequestEvent"]
        assert pr["action"] is True
        assert pr["pr_number"] is True
        assert pr["ref"] is False
        assert pr["ref_type"] is False
        assert pr["forkee_id"] is False
        assert pr["push_size"] is False

    # IssuesEvent: action=True, rest False
    def test_issues_event_has_only_action(self):
        issue = EVENT_TYPE_FIELDS["IssuesEvent"]
        assert issue["action"] is True
        assert issue["ref"] is False
        assert issue["ref_type"] is False
        assert issue["forkee_id"] is False
        assert issue["pr_number"] is False
        assert issue["push_size"] is False

    # CreateEvent: ref=True, ref_type=True, rest False
    def test_create_event_has_ref_and_ref_type(self):
        create = EVENT_TYPE_FIELDS["CreateEvent"]
        assert create["ref"] is True
        assert create["ref_type"] is True
        assert create["action"] is False
        assert create["forkee_id"] is False
        assert create["pr_number"] is False
        assert create["push_size"] is False


# ---------------------------------------------------------------------------
# HDFS path constants
# ---------------------------------------------------------------------------

class TestHdfsConstants:
    def test_hdfs_base_uses_namenode(self):
        assert "namenode:9000" in HDFS_BASE

    def test_hdfs_base_has_github_prefix(self):
        assert HDFS_BASE.endswith("/github")

    def test_events_path_under_hdfs_base(self):
        assert EVENTS_PATH.startswith(HDFS_BASE)

    def test_events_path_ends_with_parquet(self):
        assert EVENTS_PATH.endswith("parquet")

    def test_all_ml_paths_under_hdfs_base(self):
        for path in [LABELS_PATH, FEATURES_PATH, ENRICHMENT_PATH, TRAIN_PATH, HOLDOUT_PATH, MODEL_PATH]:
            assert path.startswith(HDFS_BASE), f"{path} should be under HDFS_BASE"

    def test_checkpoints_path_under_hdfs_base(self):
        assert CHECKPOINTS_PATH.startswith(HDFS_BASE)

    def test_all_paths_are_strings(self):
        paths = [HDFS_BASE, EVENTS_PATH, LABELS_PATH, FEATURES_PATH,
                 ENRICHMENT_PATH, TRAIN_PATH, HOLDOUT_PATH, MODEL_PATH, CHECKPOINTS_PATH]
        for p in paths:
            assert isinstance(p, str)

    def test_paths_have_no_trailing_slash(self):
        paths = [HDFS_BASE, EVENTS_PATH, LABELS_PATH, FEATURES_PATH,
                 ENRICHMENT_PATH, TRAIN_PATH, HOLDOUT_PATH, MODEL_PATH, CHECKPOINTS_PATH]
        for p in paths:
            assert not p.endswith("/"), f"Path '{p}' has trailing slash"


# ---------------------------------------------------------------------------
# PARTITION_COLS
# ---------------------------------------------------------------------------

class TestPartitionCols:
    def test_partition_cols_is_list(self):
        assert isinstance(PARTITION_COLS, list)

    def test_partition_cols_has_three_items(self):
        assert len(PARTITION_COLS) == 3

    def test_year_in_partition_cols(self):
        assert "year" in PARTITION_COLS

    def test_month_in_partition_cols(self):
        assert "month" in PARTITION_COLS

    def test_event_type_in_partition_cols(self):
        assert "event_type" in PARTITION_COLS

    def test_partition_order_is_year_month_event_type(self):
        assert PARTITION_COLS == ["year", "month", "event_type"]
