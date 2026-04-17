"""
INFRA-03: Unified Events Schema
GitHub Developer Ecosystem Analytics
Author: Hariharan L (hl5865)

This file is the single source of truth for the events table schema.
Everyone imports from here — do NOT redefine fields elsewhere.

    from batch.schema import EVENTS_SCHEMA, EVENT_TYPES

Why this matters:
  The batch parser (INFRA-04) writes Parquet using this schema.
  Shreyas's streaming parser (STREAM-04) asserts its output matches this schema.
  Tanusha's feature engineering (ML-03) reads Parquet written with this schema.
  Any mismatch = silent wrong results or runtime crashes.

Sign-off required before anyone writes feature or streaming code:
  Tanusha Karnam (tk3514)   — ML / feature engineering
  Shreyas Kaldate (sk12898) — Streaming pipeline
"""

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    IntegerType,
)

# =============================================================================
# UNIFIED EVENTS SCHEMA
#
# One row = one GitHub event (any type).
# Fields that don't apply to a given event type are NULL.
#
# Field naming rules:
#   - snake_case throughout
#   - no abbreviations except well-known ones (pr, ref)
#   - nullable=True unless the field is ALWAYS present in the GitHub API
# =============================================================================

EVENTS_SCHEMA = StructType([

    # -------------------------------------------------------------------------
    # Core fields — present on every event type, never null
    # -------------------------------------------------------------------------

    StructField("event_id",    StringType(),    nullable=False),
    # GitHub's unique event ID (string, e.g. "33291552947")
    # Use this for deduplication in the streaming path.

    StructField("event_type",  StringType(),    nullable=False),
    # One of the EVENT_TYPES list below.
    # e.g. "PushEvent", "WatchEvent", "ForkEvent"

    StructField("repo_id",     LongType(),      nullable=False),
    # GitHub's numeric repo ID — stable even if the repo is renamed.
    # Use this (not repo_name) as the join key across tables.

    StructField("repo_name",   StringType(),    nullable=False),
    # Full repo name e.g. "torvalds/linux"
    # Can change if a repo is renamed or transferred — prefer repo_id for joins.

    StructField("actor_id",    LongType(),      nullable=False),
    # GitHub's numeric user ID of whoever triggered the event.

    StructField("actor_login", StringType(),    nullable=True),
    # GitHub username of the actor e.g. "torvalds"
    # Nullable — some bot/deleted accounts have no login in the archive.

    StructField("created_at",  TimestampType(), nullable=False),
    # Timestamp when the event occurred on GitHub (UTC).
    # This is the primary time dimension for all queries.

    StructField("ingested_at", TimestampType(), nullable=True),
    # Timestamp when we ingested the event into our pipeline.
    # NULL for historical batch data (archive doesn't include ingestion time).
    # Always set for streaming path events.

    # -------------------------------------------------------------------------
    # Language — enriched field
    # NULL in raw events. Filled by INFRA-04 batch parser via GitHub REST API
    # or by joining repo metadata. Critical for language velocity analysis.
    # -------------------------------------------------------------------------

    StructField("language",    StringType(),    nullable=True),
    # Primary programming language of the repo e.g. "Python", "TypeScript"
    # NULL if GitHub doesn't have language info for the repo (common for new repos).

    # -------------------------------------------------------------------------
    # Event-type-specific payload fields
    # Each field is NULL for event types where it doesn't apply.
    # See EVENT_TYPE_FIELDS below for which fields apply to which events.
    # -------------------------------------------------------------------------

    StructField("ref",         StringType(),    nullable=True),
    # CreateEvent: name of branch or tag created e.g. "main", "v1.0.0"
    # PushEvent:   full ref name e.g. "refs/heads/main"
    # NULL for all other event types.

    StructField("ref_type",    StringType(),    nullable=True),
    # CreateEvent only: "repository", "branch", or "tag"
    # NULL for all other event types.

    StructField("action",      StringType(),    nullable=True),
    # IssuesEvent:      "opened", "closed", "reopened", "labeled", etc.
    # PullRequestEvent: "opened", "closed", "merged", "synchronize", etc.
    # NULL for PushEvent, WatchEvent, ForkEvent, CreateEvent.

    StructField("forkee_id",   LongType(),      nullable=True),
    # ForkEvent only: repo_id of the newly created fork.
    # NULL for all other event types.

    StructField("pr_number",   LongType(),      nullable=True),
    # PullRequestEvent only: PR number within the repo e.g. 42
    # NULL for all other event types.

    StructField("push_size",   IntegerType(),   nullable=True),
    # PushEvent only: number of commits in the push.
    # NULL for all other event types.

])


# =============================================================================
# EVENT TYPES WE PROCESS
#
# These are the 6 event types routed through the pipeline.
# Any other event type in the archive is silently dropped.
# =============================================================================

EVENT_TYPES = {
    "PushEvent",         # someone pushed commits to a branch
    "WatchEvent",        # someone starred a repo (misleadingly named "Watch")
    "ForkEvent",         # someone forked a repo
    "PullRequestEvent",  # PR opened / closed / merged
    "IssuesEvent",       # issue opened / closed / commented
    "CreateEvent",       # repo / branch / tag created
}


# =============================================================================
# FIELD APPLICABILITY MAP
#
# Documents which payload fields are populated for each event type.
# NULL means the field is always null for that event type.
# Used by INFRA-04 parser and STREAM-04 schema assertion test.
#
#                        ref   ref_type  action  forkee_id  pr_number  push_size
# PushEvent              YES   NULL      NULL    NULL       NULL       YES
# WatchEvent             NULL  NULL      NULL    NULL       NULL       NULL
# ForkEvent              NULL  NULL      NULL    YES        NULL       NULL
# PullRequestEvent       NULL  NULL      YES     NULL       YES        NULL
# IssuesEvent            NULL  NULL      YES     NULL       NULL       NULL
# CreateEvent            YES   YES       NULL    NULL       NULL       NULL
# =============================================================================

EVENT_TYPE_FIELDS = {
    "PushEvent":         {"ref": True,  "ref_type": False, "action": False, "forkee_id": False, "pr_number": False, "push_size": True},
    "WatchEvent":        {"ref": False, "ref_type": False, "action": False, "forkee_id": False, "pr_number": False, "push_size": False},
    "ForkEvent":         {"ref": False, "ref_type": False, "action": False, "forkee_id": True,  "pr_number": False, "push_size": False},
    "PullRequestEvent":  {"ref": False, "ref_type": False, "action": True,  "forkee_id": False, "pr_number": True,  "push_size": False},
    "IssuesEvent":       {"ref": False, "ref_type": False, "action": True,  "forkee_id": False, "pr_number": False, "push_size": False},
    "CreateEvent":       {"ref": True,  "ref_type": True,  "action": False, "forkee_id": False, "pr_number": False, "push_size": False},
}


# =============================================================================
# HDFS OUTPUT PATHS
#
# Centralised here so all scripts use the same paths.
# Import these instead of hardcoding strings in individual scripts.
# =============================================================================

HDFS_BASE          = "hdfs://namenode:9000/github"

# INFRA-04 writes here — all other scripts read from here
EVENTS_PATH        = f"{HDFS_BASE}/events/parquet"

# ML pipeline paths (Tanusha)
LABELS_PATH        = f"{HDFS_BASE}/ml/labels"
FEATURES_PATH      = f"{HDFS_BASE}/ml/features"
ENRICHMENT_PATH    = f"{HDFS_BASE}/ml/enrichment"
TRAIN_PATH         = f"{HDFS_BASE}/ml/train"
HOLDOUT_PATH       = f"{HDFS_BASE}/ml/holdout"
MODEL_PATH         = f"{HDFS_BASE}/ml/model/rf_trained"

# Streaming checkpoints (Shreyas)
CHECKPOINTS_PATH   = f"{HDFS_BASE}/checkpoints/streaming"


# =============================================================================
# PARTITION COLUMNS
#
# The events Parquet table is partitioned by these columns.
# Partitioning means files are organized into folders by value:
#   /github/events/parquet/year=2023/month=01/event_type=PushEvent/...
#
# This makes queries that filter on year, month, or event_type
# much faster — Spark skips reading irrelevant partitions entirely.
# =============================================================================

PARTITION_COLS = ["year", "month", "event_type"]
# Note: year and month are derived columns added by INFRA-04,
# not present in the raw GitHub archive JSON.
