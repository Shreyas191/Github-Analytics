"""
INFRA-04: PySpark Batch Parser — GitHub Archive JSON.GZ → Parquet
GitHub Developer Ecosystem Analytics
Author: Hariharan L (hl5865)

What this does:
  1. Reads GitHub Archive JSON.GZ files from HDFS (hourly partitions)
  2. Parses 20+ event types into a unified flat schema
  3. Adds year/month derived columns for partitioning
  4. Writes clean Parquet partitioned by year/month/event_type

Why Parquet?
  - Columnar format: reading only "event_type" column skips all other columns
  - Compressed by default (~3-5x smaller than JSON)
  - Native PySpark type support (timestamps, longs, etc.)
  - Partition pruning: filter on year/month/event_type skips irrelevant files

Input:  hdfs://namenode:9000/github/events/raw/*.json.gz
Output: hdfs://namenode:9000/github/events/parquet/
        partitioned by year/month/event_type

Usage:
  # Local sample run
  spark-submit --master spark://spark-master:7077 \\
    /opt/spark-apps/batch/infra04_batch_parser.py

  # NYU Dataproc (full 2023-2025 run)
  gcloud dataproc jobs submit pyspark infra04_batch_parser.py \\
    --cluster=github-analytics \\
    --region=us-central1 \\
    -- --input gs://data.gharchive.org/202*.json.gz \\
       --output gs://your-bucket/github/events/parquet
"""

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    IntegerType,
)

# Import our unified schema — single source of truth
# When running via spark-submit, add batch/ to PYTHONPATH or submit with --py-files
try:
    from schema import EVENTS_SCHEMA, EVENT_TYPES, EVENT_TYPE_FIELDS, PARTITION_COLS
except ImportError:
    from batch.schema import EVENTS_SCHEMA, EVENT_TYPES, EVENT_TYPE_FIELDS, PARTITION_COLS

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("infra04.batch_parser")

# ---------------------------------------------------------------------------
# Defaults — override via CLI args or environment
# ---------------------------------------------------------------------------
DEFAULT_INPUT  = "hdfs://namenode:9000/github/events/raw/*.json.gz"
DEFAULT_OUTPUT = "hdfs://namenode:9000/github/events/parquet"
DEFAULT_SHUFFLE_PARTITIONS = 200


# ===========================================================================
# RAW JSON SCHEMA
#
# GitHub Archive JSON structure — what the raw files actually look like.
# We only extract the fields we need; everything else is dropped.
#
# Why define this manually instead of inferSchema=True?
#   - inferSchema reads the entire dataset twice (once to infer, once to load)
#   - On 120GB that's a ~30 minute penalty
#   - Our schema is stable — GitHub Archive format hasn't changed since 2015
# ===========================================================================

# Payload sub-schema — the "payload" field differs per event type
# We extract a superset of all payload fields; nulls fill in automatically
PAYLOAD_SCHEMA = StructType([
    StructField("ref",      StringType(),  nullable=True),   # CreateEvent, PushEvent
    StructField("ref_type", StringType(),  nullable=True),   # CreateEvent
    StructField("action",   StringType(),  nullable=True),   # IssuesEvent, PREvent
    StructField("size",     IntegerType(), nullable=True),   # PushEvent: commit count
    StructField("number",   LongType(),    nullable=True),   # PullRequestEvent: PR number
    StructField("forkee", StructType([                       # ForkEvent
        StructField("id", LongType(), nullable=True),
    ]), nullable=True),
])

# Repo sub-schema
REPO_SCHEMA = StructType([
    StructField("id",   LongType(),   nullable=True),
    StructField("name", StringType(), nullable=True),
])

# Actor sub-schema
ACTOR_SCHEMA = StructType([
    StructField("id",    LongType(),   nullable=True),
    StructField("login", StringType(), nullable=True),
])

# Top-level raw event schema
RAW_EVENT_SCHEMA = StructType([
    StructField("id",         StringType(),    nullable=True),
    StructField("type",       StringType(),    nullable=True),
    StructField("created_at", TimestampType(), nullable=True),
    StructField("repo",       REPO_SCHEMA,     nullable=True),
    StructField("actor",      ACTOR_SCHEMA,    nullable=True),
    StructField("payload",    PAYLOAD_SCHEMA,  nullable=True),
])


# ===========================================================================
# PARSER
# ===========================================================================

def build_spark_session(app_name: str, shuffle_partitions: int) -> SparkSession:
    """
    Create SparkSession with settings tuned for JSON.GZ parsing.

    spark.sql.shuffle.partitions:
      Controls how many partitions are created after a shuffle (join, groupBy).
      Default is 200 which is fine for our data size.
      On Dataproc with more cores you'd increase this.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        # Tell Spark to merge schemas across Parquet files automatically
        # Needed because GitHub Archive files can have slightly different fields
        .config("spark.sql.parquet.mergeSchema", "false")
        # Push down filters to Parquet row groups for faster reads on re-runs
        .config("spark.sql.parquet.filterPushdown", "true")
        .getOrCreate()
    )


def read_raw_events(spark: SparkSession, input_path: str):
    """
    Read GitHub Archive JSON.GZ files into a DataFrame.

    GitHub Archive format:
      Each .json.gz file = one hour of events
      Each line = one JSON object (one event)
      Files are NOT a JSON array — they're newline-delimited JSON (NDJSON)

    Spark reads NDJSON natively with spark.read.json().
    We pass our schema explicitly to avoid the inferSchema penalty.
    """
    logger.info(f"Reading raw events from: {input_path}")

    df = (
        spark.read
        .schema(RAW_EVENT_SCHEMA)
        .option("mode", "PERMISSIVE")       # don't crash on malformed lines
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(input_path)
    )

    total = df.count()
    logger.info(f"Raw events loaded: {total:,}")
    return df


def filter_known_event_types(df):
    """
    Keep only the 6 event types we process. Drop the rest.

    Why: GitHub Archive has 20+ event types (GollumEvent, MemberEvent, etc.)
    We only care about the 6 types that feed our analytics and ML pipeline.
    Dropping unknowns early reduces data volume for all downstream steps.
    """
    filtered = df.filter(F.col("type").isin(EVENT_TYPES))

    logger.info("Event type distribution after filtering:")
    filtered.groupBy("type").count().orderBy(F.col("count").desc()).show(10)

    return filtered


def flatten_to_unified_schema(df):
    """
    Flatten the nested raw JSON into our unified flat schema.

    The raw event has nested structs:
      event.repo.id, event.actor.login, event.payload.ref, etc.

    We extract everything to top-level columns matching EVENTS_SCHEMA.
    Null-filling is automatic — if a field doesn't exist in the payload
    for a given event type, it's already NULL from the schema definition.
    """
    logger.info("Flattening nested JSON to unified schema...")

    unified = df.select(

        # Core fields — always present
        F.col("id").cast(StringType()).alias("event_id"),
        F.col("type").alias("event_type"),
        F.col("repo.id").cast(LongType()).alias("repo_id"),
        F.col("repo.name").alias("repo_name"),
        F.col("actor.id").cast(LongType()).alias("actor_id"),
        F.col("actor.login").alias("actor_login"),
        F.col("created_at"),

        # ingested_at is NULL for historical batch data
        # (the archive doesn't record when we downloaded it)
        F.lit(None).cast(TimestampType()).alias("ingested_at"),

        # language is NULL at this stage
        # INFRA-04 enriches it via a separate REST API join (or left as NULL)
        # Tanusha's ML-03 uses language from the batch-computed language rank table
        F.lit(None).cast(StringType()).alias("language"),

        # Payload fields — NULL for event types where they don't apply
        F.col("payload.ref").alias("ref"),
        F.col("payload.ref_type").alias("ref_type"),
        F.col("payload.action").alias("action"),
        F.col("payload.forkee.id").cast(LongType()).alias("forkee_id"),
        F.col("payload.number").cast(LongType()).alias("pr_number"),
        F.col("payload.size").cast(IntegerType()).alias("push_size"),

    )

    return unified


def add_partition_columns(df):
    """
    Add year and month columns derived from created_at.

    These become the partition directories in HDFS:
      /github/events/parquet/year=2023/month=01/event_type=PushEvent/

    Why partition by year/month/event_type?
      - Tanusha's ML jobs filter on event_type heavily (WatchEvent, PushEvent, etc.)
      - Language velocity queries filter on year/month ranges
      - With partitioning, Spark reads only the relevant folders — skips the rest
      - Without partitioning, every query scans all 120GB
    """
    return df \
        .withColumn("year",  F.year(F.col("created_at")).cast(StringType())) \
        .withColumn("month", F.lpad(F.month(F.col("created_at")).cast(StringType()), 2, "0"))
    # lpad pads month to 2 digits: "1" → "01" so folder sort order is correct


def drop_nulls_on_required_fields(df):
    """
    Drop rows where critical fields are NULL.

    A row with NULL repo_id or NULL created_at is unusable for any downstream
    job. Better to drop it here cleanly than have silent wrong joins later.

    We keep rows with NULL language, actor_login — those are expected NULLs.
    """
    before = df.count()

    df_clean = df.dropna(subset=["event_id", "event_type", "repo_id", "created_at"])

    dropped = before - df_clean.count()
    if dropped > 0:
        logger.warning(f"Dropped {dropped:,} rows with NULL required fields")
    else:
        logger.info("No rows dropped — all required fields present")

    return df_clean


def write_parquet(df, output_path: str):
    """
    Write the unified events table to HDFS as partitioned Parquet.

    mode="overwrite":
      Replaces existing data at output_path. Safe for full re-runs.
      On incremental runs (new months only) you'd use mode="append"
      and filter input to only new files — that's what INFRA-06 Airflow DAG does.

    partitionBy:
      Creates the folder hierarchy year= / month= / event_type=
      Spark automatically creates one or more .parquet files per partition.
    """
    logger.info(f"Writing Parquet to: {output_path}")

    (
        df.write
        .mode("overwrite")
        .partitionBy("year", "month", "event_type")
        .parquet(output_path)
    )

    logger.info("Parquet write complete.")


def print_summary(spark: SparkSession, output_path: str):
    """
    Read back the written Parquet and print a summary for the validation report.
    This is a quick sanity check — the full validation is in INFRA-05.
    """
    logger.info("Reading back Parquet for quick summary...")

    df = spark.read.parquet(output_path)
    total = df.count()

    print("\n" + "=" * 60)
    print("INFRA-04 OUTPUT SUMMARY")
    print("=" * 60)
    print(f"  Total rows written : {total:,}")
    print(f"  Output path        : {output_path}")

    print("\n  Row counts by event_type:")
    df.groupBy("event_type").count() \
      .orderBy(F.col("count").desc()) \
      .show(10)

    print("\n  Row counts by year:")
    df.groupBy("year").count() \
      .orderBy("year") \
      .show()

    print("\n  NULL rates on key columns:")
    null_counts = df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in ["repo_id", "actor_id", "created_at", "language", "event_type"]
    ])
    null_counts.show()

    print("=" * 60)


# ===========================================================================
# ENTRY POINT
# ===========================================================================

def parse_args():
    parser = argparse.ArgumentParser(description="INFRA-04: GitHub Archive batch parser")
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT,
        help="Input path — HDFS glob or GCS path (for Dataproc runs)"
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="Output path for Parquet"
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=DEFAULT_SHUFFLE_PARTITIONS,
        help="spark.sql.shuffle.partitions (increase for Dataproc)"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    spark = build_spark_session(
        app_name="INFRA-04: GitHub Archive Batch Parser",
        shuffle_partitions=args.shuffle_partitions,
    )
    spark.sparkContext.setLogLevel("WARN")

    logger.info("=" * 60)
    logger.info("INFRA-04: GitHub Archive Batch Parser")
    logger.info(f"  Input  : {args.input}")
    logger.info(f"  Output : {args.output}")
    logger.info("=" * 60)

    # Pipeline
    df = read_raw_events(spark, args.input)
    df = filter_known_event_types(df)
    df = flatten_to_unified_schema(df)
    df = add_partition_columns(df)
    df = drop_nulls_on_required_fields(df)
    write_parquet(df, args.output)
    print_summary(spark, args.output)

    spark.stop()
    logger.info("INFRA-04 complete.")


if __name__ == "__main__":
    main()
