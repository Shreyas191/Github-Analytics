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

import json
import os
import sys
from datetime import datetime, timezone

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
    This simulates exactly what the streaming path produces.
    """
    print(f"\n[2/3] Parsing {len(messages)} messages into Spark DataFrame...")

    def to_long(val):
        return int(val) if val is not None else None

    def to_int(val):
        return int(val) if val is not None else None

    def parse_ts(val):
        """Parse ISO 8601 string to datetime — matches TimestampType."""
        if val is None:
            return None
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except Exception:
            return None

    rows = []
    for msg in messages:
        rows.append((
            str(msg.get("event_id"))        if msg.get("event_id")   else None,
            msg.get("event_type"),
            to_long(msg.get("repo_id")),
            msg.get("repo_name"),
            to_long(msg.get("actor_id")),
            msg.get("actor_login"),
            parse_ts(msg.get("created_at")),
            parse_ts(msg.get("ingested_at")),
            msg.get("language"),
            msg.get("ref"),
            msg.get("ref_type"),
            msg.get("action"),
            to_long(msg.get("forkee_id")),
            to_long(msg.get("pr_number")),
            to_int(msg.get("push_size")),
        ))

    df = spark.createDataFrame(rows, schema=EVENTS_SCHEMA)
    print(f"  DataFrame created: {df.count()} rows, {len(df.schema.fields)} columns")
    return df


# ---------------------------------------------------------------------------
# Step 3 — Assert schema matches EVENTS_SCHEMA exactly
# ---------------------------------------------------------------------------
def assert_schema(df) -> bool:
    """
    Compares the DataFrame schema against EVENTS_SCHEMA field by field.
    Checks: field name, data type, nullability.
    Prints a detailed report and returns True if all fields pass.
    """
    print(f"\n[3/3] Asserting schema parity against INFRA-03 (Hariharan's schema)...")
    print(f"\n  {'Field':<15} {'Expected Type':<18} {'Actual Type':<18} {'Nullable':<10} {'Status'}")
    print(f"  {'-'*75}")

    expected_fields = {f.name: f for f in EVENTS_SCHEMA.fields}
    actual_fields   = {f.name: f for f in df.schema.fields}

    passed = 0
    failed = 0
    failures = []

    for name, expected in expected_fields.items():
        actual = actual_fields.get(name)

        if actual is None:
            status = "✗ MISSING"
            failures.append(f"  Field '{name}' is missing from streaming output.")
            failed += 1
        elif type(actual.dataType) != type(expected.dataType):
            status = "✗ TYPE MISMATCH"
            failures.append(
                f"  Field '{name}': expected {expected.dataType}, "
                f"got {actual.dataType}"
            )
            failed += 1
        elif actual.nullable != expected.nullable:
            status = "✗ NULLABLE MISMATCH"
            failures.append(
                f"  Field '{name}': expected nullable={expected.nullable}, "
                f"got nullable={actual.nullable}"
            )
            failed += 1
        else:
            status = "✓"
            passed += 1

        exp_type = type(expected.dataType).__name__
        act_type = type(actual.dataType).__name__ if actual else "MISSING"
        nullable = f"expected={expected.nullable}"

        print(f"  {name:<15} {exp_type:<18} {act_type:<18} {nullable:<10} {status}")

    # Check for extra fields in streaming output not in batch schema
    extra = set(actual_fields.keys()) - set(expected_fields.keys())
    if extra:
        print(f"\n  ⚠  Extra fields in streaming output (not in batch schema): {extra}")
        failures.append(f"  Extra fields found: {extra}")

    # Summary
    print(f"\n  {'-'*75}")
    print(f"  Result: {passed} passed, {failed} failed out of {len(expected_fields)} fields")

    if failures:
        print(f"\n  FAILURES:")
        for f in failures:
            print(f)
        print(f"\n  ✗ Schema parity check FAILED.")
        print(f"    Fix the type mismatches in stream03_producer._extract_message()")
        print(f"    then re-run this test.\n")
        return False
    else:
        print(f"\n  ✓ All {passed} fields match — schema parity confirmed.")
        print(f"    Safe to proceed with STREAM-05 (PySpark Structured Streaming).\n")
        return True


# ---------------------------------------------------------------------------
# Sample data preview
# ---------------------------------------------------------------------------
def preview_data(df):
    print(f"\n--- Sample rows (first 3) ---")
    df.select(
        "event_id", "event_type", "repo_name", "actor_login", "created_at"
    ).show(3, truncate=40)

    print("--- Event type distribution ---")
    df.groupBy("event_type").count().orderBy("count", ascending=False).show()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Fetch sample messages from Kafka
    messages = fetch_sample_messages()

    # Start a local Spark session for schema validation
    spark = (
        SparkSession.builder
        .appName("STREAM-04-schema-test")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")  # suppress verbose Spark logs

    # Parse messages into DataFrame
    df = parse_to_dataframe(spark, messages)

    # Assert schema
    passed = assert_schema(df)

    # Show a preview regardless of pass/fail — useful for debugging
    preview_data(df)

    spark.stop()

    # Exit with non-zero code if schema check failed
    # This makes it easy to use in CI or a pre-commit hook
    sys.exit(0 if passed else 1)