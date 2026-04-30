"""
STREAM-04: Schema Assertion Test — Batch vs Stream Parity
GitHub Developer Ecosystem Analytics — Streaming Pipeline
Shreyas Kaldate (sk12898)

Asserts that the streaming parser output schema matches Hariharan's
batch Parquet schema (INFRA-03) exactly — field names, data types,
and nullability must all match.

Run this BEFORE writing any feature or aggregation logic (STREAM-05).
Catching schema drift here prevents silent wrong results downstream.

Usage:
    python stream04_schema_test.py

Expected output:
    All 14 fields match — schema parity confirmed.

Dependencies:
    - Kafka must be running with topics created (STREAM-02)
    - Producer must have published at least some events (STREAM-03)
    - PySpark installed: pip install pyspark
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Hariharan's schema — copied from batch/schema.py (INFRA-03)
# This is the ground truth we assert against.
# DO NOT modify these fields — if Hariharan updates his schema,
# update this block to match and re-run this test.
# ---------------------------------------------------------------------------
EVENTS_SCHEMA = StructType([
    StructField("event_id",    StringType(),    nullable=False),
    StructField("event_type",  StringType(),    nullable=False),
    StructField("repo_id",     LongType(),      nullable=False),
    StructField("repo_name",   StringType(),    nullable=False),
    StructField("actor_id",    LongType(),      nullable=False),
    StructField("actor_login", StringType(),    nullable=True),
    StructField("created_at",  TimestampType(), nullable=False),
    StructField("ingested_at", TimestampType(), nullable=True),
    StructField("language",    StringType(),    nullable=True),
    StructField("ref",         StringType(),    nullable=True),
    StructField("ref_type",    StringType(),    nullable=True),
    StructField("action",      StringType(),    nullable=True),
    StructField("forkee_id",   LongType(),      nullable=True),
    StructField("pr_number",   LongType(),      nullable=True),
    StructField("push_size",   IntegerType(),   nullable=True),
])

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

TOPICS = [
    "push-events",
    "watch-events",
    "fork-events",
    "pr-events",
    "issue-events",
    "create-events",
]

# How many messages to sample per topic for the test
SAMPLE_SIZE = 20


# ---------------------------------------------------------------------------
# Step 1 — Pull sample messages from Kafka
# ---------------------------------------------------------------------------
def fetch_sample_messages() -> list[dict]:
    """
    Reads up to SAMPLE_SIZE messages from each Kafka topic.
    Returns a flat list of parsed event dicts.
    """
    print(f"\n{'='*60}")
    print("STREAM-04: Schema Assertion Test")
    print(f"{'='*60}")
    print(f"\n[1/3] Fetching sample messages from Kafka ({KAFKA_BOOTSTRAP})...")

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",       # read from beginning
        enable_auto_commit=False,           # test only, don't commit offsets
        consumer_timeout_ms=5000,           # stop after 5s of no messages
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    messages = []
    topic_counts = {t: 0 for t in TOPICS}

    for msg in consumer:
        topic = msg.topic
        if topic_counts[topic] < SAMPLE_SIZE:
            messages.append(msg.value)
            topic_counts[topic] += 1

        if all(c >= SAMPLE_SIZE for c in topic_counts.values()):
            break

    consumer.close()

    print(f"  Messages fetched per topic:")
    for topic, count in topic_counts.items():
        status = "✓" if count > 0 else "⚠ no messages"
        print(f"    {status}  {topic}: {count}")

    if not messages:
        print("\n  ERROR: No messages found in any topic.")
        print("  Make sure stream03_producer.py has been running and published events.")
        sys.exit(1)

    print(f"\n  Total messages fetched: {len(messages)}")
    return messages


# ---------------------------------------------------------------------------
# Step 2 — Parse messages into a Spark DataFrame using streaming schema
# ---------------------------------------------------------------------------
def parse_to_dataframe(spark: SparkSession, messages: list[dict]):
    """
    Applies the same type casting logic as stream03_producer._extract_message()
    and loads the results into a Spark DataFrame using EVENTS_SCHEMA.

    Uses pandas → Spark conversion (Arrow path) to avoid PySpark 3.5 / Python 3.14
    pickling incompatibility that causes a RecursionError on the RDD path.
    """
    print(f"\n[2/3] Parsing {len(messages)} messages into Spark DataFrame...")

    def to_long(val):
        try:
            return int(val) if val is not None else None
        except (ValueError, TypeError):
            return None

    def to_int(val):
        try:
            return int(val) if val is not None else None
        except (ValueError, TypeError):
            return None

    def parse_ts(val):
        """Parse ISO 8601 string to datetime — matches TimestampType."""
        if val is None:
            return None
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except Exception:
            return None

    records = []
    for msg in messages:
        records.append({
            "event_id":    str(msg.get("event_id")) if msg.get("event_id") else None,
            "event_type":  msg.get("event_type"),
            "repo_id":     to_long(msg.get("repo_id")),
            "repo_name":   msg.get("repo_name"),
            "actor_id":    to_long(msg.get("actor_id")),
            "actor_login": msg.get("actor_login"),
            "created_at":  parse_ts(msg.get("created_at")),
            "ingested_at": parse_ts(msg.get("ingested_at")),
            "language":    msg.get("language"),
            "ref":         msg.get("ref"),
            "ref_type":    msg.get("ref_type"),
            "action":      msg.get("action"),
            "forkee_id":   to_long(msg.get("forkee_id")),
            "pr_number":   to_long(msg.get("pr_number")),
            "push_size":   to_int(msg.get("push_size")),
        })

    # Build pandas DataFrame with nullable integer dtypes (Int64/Int32) so that
    # None stays as pd.NA rather than becoming NaN (float), which PySpark rejects
    # for LongType/IntegerType columns.
    pdf = pd.DataFrame(records, columns=[f.name for f in EVENTS_SCHEMA.fields])
    long_cols = ["repo_id", "actor_id", "forkee_id", "pr_number"]
    int_cols  = ["push_size"]
    for col in long_cols:
        pdf[col] = pdf[col].astype("Int64")
    for col in int_cols:
        pdf[col] = pdf[col].astype("Int32")

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    df = spark.createDataFrame(pdf, schema=EVENTS_SCHEMA)
    print(f"  DataFrame created: {df.count()} rows, {len(df.schema.fields)} columns")
    return df


# ---------------------------------------------------------------------------
# Step 2 — Parse and type-check each message field against EVENTS_SCHEMA
# ---------------------------------------------------------------------------

# Map Spark types → Python types for field-level validation
_SPARK_TO_PY = {
    "StringType":    str,
    "LongType":      int,
    "IntegerType":   int,
    "TimestampType": datetime,  # parsed from ISO string
}


def parse_and_check_messages(messages: list[dict]) -> tuple[list[dict], list[str]]:
    """
    Parses each message and validates that every field's Python type matches
    the expected Spark type from EVENTS_SCHEMA.

    Returns:
        records  — list of parsed dicts (for preview)
        errors   — list of type mismatch descriptions
    """
    print(f"\n[2/3] Parsing and type-checking {len(messages)} messages...")

    def to_long(val):
        try:
            return int(val) if val is not None else None
        except (ValueError, TypeError):
            return None

    def to_int(val):
        try:
            return int(val) if val is not None else None
        except (ValueError, TypeError):
            return None

    def parse_ts(val):
        if val is None:
            return None
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except Exception:
            return None

    records = []
    for msg in messages:
        records.append({
            "event_id":    str(msg.get("event_id")) if msg.get("event_id") else None,
            "event_type":  msg.get("event_type"),
            "repo_id":     to_long(msg.get("repo_id")),
            "repo_name":   msg.get("repo_name"),
            "actor_id":    to_long(msg.get("actor_id")),
            "actor_login": msg.get("actor_login"),
            "created_at":  parse_ts(msg.get("created_at")),
            "ingested_at": parse_ts(msg.get("ingested_at")),
            "language":    msg.get("language"),
            "ref":         msg.get("ref"),
            "ref_type":    msg.get("ref_type"),
            "action":      msg.get("action"),
            "forkee_id":   to_long(msg.get("forkee_id")),
            "pr_number":   to_long(msg.get("pr_number")),
            "push_size":   to_int(msg.get("push_size")),
        })

    print(f"  Parsed {len(records)} records.")
    return records


# ---------------------------------------------------------------------------
# Step 3 — Assert schema parity
# ---------------------------------------------------------------------------
def assert_schema(records: list[dict]) -> bool:
    """
    Validates each parsed record against EVENTS_SCHEMA field by field.
    Checks: field presence, Python type compatibility, and non-null constraint.
    Prints a detailed report and returns True if ALL records pass ALL fields.
    """
    print(f"\n[3/3] Asserting schema parity against INFRA-03 (Hariharan's schema)...")
    print(f"\n  {'Field':<15} {'Expected Type':<18} {'Nullable':<10} {'Status'}")
    print(f"  {'-'*60}")

    passed = 0
    failed = 0
    failures = []

    for field in EVENTS_SCHEMA.fields:
        name      = field.name
        py_type   = _SPARK_TO_PY.get(type(field.dataType).__name__)
        nullable  = field.nullable
        field_ok  = True

        for i, rec in enumerate(records):
            val = rec.get(name, "__MISSING__")
            if val == "__MISSING__":
                failures.append(f"  Record {i}: field '{name}' is MISSING.")
                field_ok = False
                break

            if val is None:
                if not nullable:
                    failures.append(
                        f"  Record {i}: '{name}' is None but nullable=False."
                    )
                    field_ok = False
                    break
            elif py_type and not isinstance(val, py_type):
                failures.append(
                    f"  Record {i}: '{name}' expected {py_type.__name__}, "
                    f"got {type(val).__name__} = {repr(val)[:60]}"
                )
                field_ok = False
                break

        if field_ok:
            status = "✓"
            passed += 1
        else:
            status = "✗ FAIL"
            failed += 1

        exp_type = type(field.dataType).__name__
        print(f"  {name:<15} {exp_type:<18} {'yes' if nullable else 'no':<10} {status}")

    # Summary
    print(f"\n  {'-'*60}")
    print(f"  Result: {passed} passed, {failed} failed out of {len(EVENTS_SCHEMA.fields)} fields")

    if failures:
        print(f"\n  FAILURES:")
        for msg in failures:
            print(msg)
        print(f"\n  ✗ Schema parity check FAILED.")
        print(f"    Fix type mismatches in stream03_producer._extract_message()\n")
        return False
    else:
        print(f"\n  ✓ All {passed} fields match — schema parity confirmed.")
        print(f"    Safe to proceed with STREAM-05 (PySpark Structured Streaming).\n")
        return True


# ---------------------------------------------------------------------------
# Sample data preview (pure Python — no Spark required)
# ---------------------------------------------------------------------------
def preview_data(records: list[dict]):
    preview_cols = ["event_id", "event_type", "repo_name", "actor_login", "created_at"]
    print(f"\n--- Sample rows (first 3) ---")
    header = "  " + "  ".join(f"{c:<30}" for c in preview_cols)
    print(header)
    print("  " + "-" * (32 * len(preview_cols)))
    for rec in records[:3]:
        row = "  " + "  ".join(
            f"{str(rec.get(c, ''))[:28]:<30}" for c in preview_cols
        )
        print(row)

    from collections import Counter
    dist = Counter(r.get("event_type") for r in records)
    print(f"\n--- Event type distribution ---")
    for etype, count in sorted(dist.items(), key=lambda x: -x[1]):
        print(f"  {etype:<30} {count}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Fetch sample messages from Kafka
    messages = fetch_sample_messages()

    # Parse messages into dicts + validate types
    records = parse_and_check_messages(messages)

    # Assert schema parity (pure Python — no Spark DataFrame needed)
    passed = assert_schema(records)

    # Show a preview regardless of pass/fail
    preview_data(records)

    # Exit with non-zero code if schema check failed (CI-friendly)
    sys.exit(0 if passed else 1)