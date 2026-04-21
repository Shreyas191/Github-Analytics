"""
STREAM-05: PySpark Structured Streaming — Windowed Aggregations
GitHub Developer Ecosystem Analytics — Streaming Pipeline
Shreyas Kaldate (sk12898)

Reads from all 6 Kafka topics, computes 5-minute tumbling window aggregations,
and sinks results to InfluxDB (Vikram's VIZ-01 data store).

Two metrics computed per window:
  1. Star velocity   — WatchEvents (stars) per repo per 5-min window
  2. Language momentum — new repo creation rate per language per 5-min window

InfluxDB connection (from Vikram's datasources.yml VIZ-01):
  URL   : http://influxdb:8086
  Org   : github-analytics
  Bucket: github-stream
  Token : github-analytics-token

Checkpoint:
  HDFS /github/checkpoints/streaming  (from Hariharan's schema.py)

Usage:
  # Inside Docker (spark-master container):
  spark-submit --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    streaming/stream05_spark_streaming.py

  # Local dev (no Docker):
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    streaming/stream05_spark_streaming.py
"""

import os
import json
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("stream05.spark_streaming")

# ---------------------------------------------------------------------------
# Config — reads from environment, falls back to Docker network defaults
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP",    "localhost:9092")
INFLUXDB_URL        = os.getenv("INFLUXDB_URL",        "http://influxdb:8086")
INFLUXDB_TOKEN      = os.getenv("INFLUXDB_TOKEN",      "github-analytics-token")
INFLUXDB_ORG        = os.getenv("INFLUXDB_ORG",        "github-analytics")
INFLUXDB_BUCKET     = os.getenv("INFLUXDB_BUCKET",     "github-stream")

# HDFS checkpoint path from Hariharan's schema.py
CHECKPOINT_PATH     = os.getenv("CHECKPOINT_PATH",     "hdfs://namenode:9000/github/checkpoints/streaming")

# For local dev without HDFS, override via env:
#   export CHECKPOINT_PATH=/tmp/stream05_checkpoint
LOCAL_CHECKPOINT    = os.getenv("LOCAL_CHECKPOINT",    "/tmp/stream05_checkpoint")

# Use HDFS if running inside Docker, local otherwise
USE_HDFS            = os.getenv("USE_HDFS", "false").lower() == "true"
EFFECTIVE_CHECKPOINT = CHECKPOINT_PATH if USE_HDFS else LOCAL_CHECKPOINT

KAFKA_TOPICS = ",".join([
    "push-events",
    "watch-events",
    "fork-events",
    "pr-events",
    "issue-events",
    "create-events",
])

# Window config — matches proposal spec
WINDOW_DURATION  = "5 minutes"
WATERMARK_DELAY  = "10 minutes"

# ---------------------------------------------------------------------------
# Message schema — matches INFRA-03 (Hariharan's unified events schema)
# This is what stream03_producer._extract_message() produces as JSON
# ---------------------------------------------------------------------------
MESSAGE_SCHEMA = StructType([
    StructField("event_id",    StringType(),    nullable=False),
    StructField("event_type",  StringType(),    nullable=False),
    StructField("repo_id",     LongType(),      nullable=False),
    StructField("repo_name",   StringType(),    nullable=False),
    StructField("actor_id",    LongType(),      nullable=False),
    StructField("actor_login", StringType(),    nullable=True),
    StructField("created_at",  StringType(),    nullable=False),  # ISO string from Kafka
    StructField("ingested_at", StringType(),    nullable=True),
    StructField("language",    StringType(),    nullable=True),
    StructField("ref",         StringType(),    nullable=True),
    StructField("ref_type",    StringType(),    nullable=True),
    StructField("action",      StringType(),    nullable=True),
    StructField("forkee_id",   LongType(),      nullable=True),
    StructField("pr_number",   LongType(),      nullable=True),
    StructField("push_size",   IntegerType(),   nullable=True),
])


# ---------------------------------------------------------------------------
# InfluxDB writer — writes a batch of rows as line protocol
# Called by foreachBatch sink
# ---------------------------------------------------------------------------
def write_to_influxdb(df, epoch_id: int, measurement: str, tag_col: str, field_col: str, value_col: str):
    """
    Writes a micro-batch DataFrame to InfluxDB using line protocol.
    Line protocol format:
        <measurement>,<tag>=<value> <field>=<value> <timestamp>

    Args:
        df         : micro-batch DataFrame
        epoch_id   : Spark streaming epoch (used for logging)
        measurement: InfluxDB measurement name (e.g. "star_velocity")
        tag_col    : column to use as InfluxDB tag (e.g. "repo_name")
        field_col  : column name for the field key (e.g. "star_count")
        value_col  : column with the numeric value
    """
    from influxdb_client import InfluxDBClient, WriteOptions
    from influxdb_client.client.write_api import SYNCHRONOUS

    rows = df.collect()
    if not rows:
        logger.info(f"[epoch {epoch_id}] No rows to write for {measurement}")
        return

    client = InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG,
    )
    write_api = client.write_api(write_options=SYNCHRONOUS)

    lines = []
    for row in rows:
        tag_val   = str(row[tag_col]).replace(" ", "\\ ").replace(",", "\\,")
        field_val = row[value_col]
        # Use window end time as the InfluxDB timestamp (nanoseconds)
        window_end = row["window_end"]
        ts_ns = int(window_end.timestamp() * 1e9)
        line = f"{measurement},{tag_col}={tag_val} {field_col}={field_val}i {ts_ns}"
        lines.append(line)

    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=lines)
    client.close()

    logger.info(f"[epoch {epoch_id}] Wrote {len(lines)} points to InfluxDB "
                f"measurement='{measurement}'")


# ---------------------------------------------------------------------------
# Main streaming job
# ---------------------------------------------------------------------------
def main():
    logger.info("Starting STREAM-05: PySpark Structured Streaming")
    logger.info(f"  Kafka        : {KAFKA_BOOTSTRAP}")
    logger.info(f"  InfluxDB     : {INFLUXDB_URL} bucket={INFLUXDB_BUCKET}")
    logger.info(f"  Checkpoint   : {EFFECTIVE_CHECKPOINT}")
    logger.info(f"  Window       : {WINDOW_DURATION} / watermark {WATERMARK_DELAY}")

    spark = (
        SparkSession.builder
        .appName("STREAM-05-structured-streaming")
        .config("spark.sql.session.timeZone", "UTC")
        # Reduce shuffle partitions for streaming — default 200 is too many
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------
    # 1. Read from Kafka — all 6 topics
    # ------------------------------------------------------------------
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPICS)
        .option("startingOffsets", "earliest")      # read backfill on startup
        .option("failOnDataLoss", "false")          # tolerate topic resets
        .load()
    )

    # ------------------------------------------------------------------
    # 2. Parse JSON value into structured columns
    # ------------------------------------------------------------------
    parsed = (
        raw_stream
        .select(
            F.from_json(
                F.col("value").cast("string"),
                MESSAGE_SCHEMA
            ).alias("data"),
            F.col("topic"),
        )
        .select("data.*", "topic")
        # Parse created_at string → timestamp for windowing
        .withColumn(
            "event_time",
            F.to_timestamp(F.col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
        )
        # Apply watermark — allows 10 minutes of late-arriving events
        .withWatermark("event_time", WATERMARK_DELAY)
    )

    # ------------------------------------------------------------------
    # 3. Metric 1 — Star velocity
    # WatchEvent = someone starred a repo
    # Compute: stars received per repo per 5-min window
    # Sink: InfluxDB measurement "star_velocity"
    # Used by: Vikram's VIZ-03 trending repos panel
    # ------------------------------------------------------------------
    star_velocity = (
        parsed
        .filter(F.col("event_type") == "WatchEvent")
        .groupBy(
            F.window("event_time", WINDOW_DURATION).alias("window"),
            F.col("repo_id"),
            F.col("repo_name"),
        )
        .agg(
            F.count("*").alias("star_count")
        )
        .select(
            F.col("repo_id"),
            F.col("repo_name"),
            F.col("star_count"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
        )
    )

    star_velocity_query = (
        star_velocity.writeStream
        .outputMode("update")
        .option("checkpointLocation", f"{EFFECTIVE_CHECKPOINT}/star_velocity")
        .foreachBatch(
            lambda df, epoch_id: write_to_influxdb(
                df, epoch_id,
                measurement="star_velocity",
                tag_col="repo_name",
                field_col="star_count",
                value_col="star_count",
            )
        )
        .trigger(processingTime=WINDOW_DURATION)
        .start()
    )

    logger.info("star_velocity stream started.")

    # ------------------------------------------------------------------
    # 4. Metric 2 — Language momentum
    # CreateEvent with ref_type="repository" = new repo created
    # Compute: new repos created per language per 5-min window
    # Sink: InfluxDB measurement "language_momentum"
    # Used by: Vikram's VIZ-03 language leaderboard panel
    # ------------------------------------------------------------------
    language_momentum = (
        parsed
        .filter(
            (F.col("event_type") == "CreateEvent") &
            (F.col("ref_type") == "repository")
        )
        .groupBy(
            F.window("event_time", WINDOW_DURATION).alias("window"),
            # language is null for new repos (enriched later by batch path)
            # use "unknown" as fallback so we don't lose the count
            F.coalesce(F.col("language"), F.lit("unknown")).alias("language"),
        )
        .agg(
            F.count("*").alias("new_repo_count")
        )
        .select(
            F.col("language"),
            F.col("new_repo_count"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
        )
    )

    language_momentum_query = (
        language_momentum.writeStream
        .outputMode("update")
        .option("checkpointLocation", f"{EFFECTIVE_CHECKPOINT}/language_momentum")
        .foreachBatch(
            lambda df, epoch_id: write_to_influxdb(
                df, epoch_id,
                measurement="language_momentum",
                tag_col="language",
                field_col="new_repo_count",
                value_col="new_repo_count",
            )
        )
        .trigger(processingTime=WINDOW_DURATION)
        .start()
    )

    logger.info("language_momentum stream started.")

    # ------------------------------------------------------------------
    # 5. Console sink for local dev — prints aggregations to terminal
    # Disabled when USE_HDFS=true (running in Docker)
    # ------------------------------------------------------------------
    if not USE_HDFS:
        logger.info("Local dev mode: also printing to console.")

        # Debug sink — shows ALL parsed events (not just watch/create)
        # Proves the Kafka→Spark pipeline is working end-to-end
        raw_console = (
            parsed
            .select("event_type", "repo_name", "actor_login", "event_time", "topic")
            .writeStream
            .outputMode("append")
            .format("console")
            .option("truncate", "false")
            .option("numRows", "5")
            .option("checkpointLocation", f"{LOCAL_CHECKPOINT}/raw_console")
            .trigger(processingTime=WINDOW_DURATION)
            .start()
        )

        star_console = (
            star_velocity.writeStream
            .outputMode("update")
            .format("console")
            .option("truncate", "false")
            .option("numRows", "10")
            .option("checkpointLocation", f"{LOCAL_CHECKPOINT}/star_console")
            .trigger(processingTime=WINDOW_DURATION)
            .start()
        )

        lang_console = (
            language_momentum.writeStream
            .outputMode("update")
            .format("console")
            .option("truncate", "false")
            .option("numRows", "10")
            .option("checkpointLocation", f"{LOCAL_CHECKPOINT}/lang_console")
            .trigger(processingTime=WINDOW_DURATION)
            .start()
        )

    # ------------------------------------------------------------------
    # 6. Wait for termination
    # ------------------------------------------------------------------
    logger.info("Both streaming queries running. Ctrl+C to stop.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
