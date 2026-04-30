"""
STREAM-07: Throughput + Consumer Lag Logging
GitHub Developer Ecosystem Analytics — Streaming Pipeline
Shreyas Kaldate (sk12898)

Logs the following metrics to Grafana (via InfluxDB) every 60 seconds:
  - events_received_per_min   : events picked up from GitHub API
  - events_processed_per_min  : events successfully published to Kafka
  - consumer_lag_per_topic    : how far behind the Spark consumer is per topic

Why this matters:
  The project proposal specifically calls out demoing during US/EU peak hours
  (500-800 events/min). This script proves the pipeline is handling real volume,
  not a toy stream. Reviewers will look at these metrics during the demo.

Run this alongside stream03_producer.py and stream05_spark_streaming.py.

Usage:
    python stream07_metrics.py

Dependencies:
    pip install kafka-python-ng influxdb-client
"""

from __future__ import annotations

import os
import time
import logging
from datetime import datetime, timezone

from kafka import KafkaAdminClient, KafkaConsumer
from kafka.structs import TopicPartition
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("stream07.metrics")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "localhost:29092")
INFLUXDB_URL     = os.getenv("INFLUXDB_URL",     "http://localhost:8086")
INFLUXDB_TOKEN   = os.getenv("INFLUXDB_TOKEN",   "github-analytics-token")
INFLUXDB_ORG     = os.getenv("INFLUXDB_ORG",     "github-analytics")
INFLUXDB_BUCKET  = os.getenv("INFLUXDB_BUCKET",  "github-stream")

# Poll interval in seconds
POLL_INTERVAL = 60

# Kafka consumer group used by STREAM-05 Spark job
# This is what Spark registers as when it reads from Kafka
SPARK_CONSUMER_GROUP = "stream05-spark-streaming"

TOPICS = [
    "push-events",
    "watch-events",
    "fork-events",
    "pr-events",
    "issue-events",
    "create-events",
    "pending-repos",
]


# ---------------------------------------------------------------------------
# Kafka lag calculator
# ---------------------------------------------------------------------------
def get_consumer_lag(bootstrap: str, group_id: str, topics: list[str]) -> dict[str, int]:
    """
    Calculates consumer lag per topic for a given consumer group.

    Lag = latest offset (end of topic) - committed offset (where consumer is)

    Returns dict: { topic_name: lag_count }
    """
    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap,
        client_id="stream07-lag-checker",
        request_timeout_ms=5000,
    )

    # Get committed offsets for the Spark consumer group
    try:
        committed = admin.list_consumer_group_offsets(group_id)
    except Exception as e:
        logger.warning(f"Could not fetch committed offsets for group '{group_id}': {e}")
        logger.warning("Spark streaming job may not have started yet — lag will show as 0.")
        admin.close()
        return {t: 0 for t in topics}

    admin.close()

    # Get end offsets (latest available) for all partitions
    consumer = KafkaConsumer(bootstrap_servers=bootstrap)
    topic_partitions = []
    for topic in topics:
        partitions = consumer.partitions_for_topic(topic)
        if partitions:
            for p in partitions:
                topic_partitions.append(TopicPartition(topic, p))

    end_offsets = consumer.end_offsets(topic_partitions)
    consumer.close()

    # Calculate lag per topic
    lag_per_topic = {t: 0 for t in topics}
    for tp, end_offset in end_offsets.items():
        committed_offset = committed.get(tp)
        if committed_offset is not None:
            lag = max(0, end_offset - committed_offset.offset)
        else:
            # No committed offset = consumer hasn't read this partition yet
            lag = end_offset
        lag_per_topic[tp] = lag_per_topic.get(tp.topic, 0) + lag

    return lag_per_topic


# ---------------------------------------------------------------------------
# InfluxDB writer
# ---------------------------------------------------------------------------
def write_metrics(
    influx_client: InfluxDBClient,
    events_received: int,
    events_published: int,
    lag_per_topic: dict[str, int],
    uptime_min: float,
):
    """Write all metrics as InfluxDB points."""
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)
    now = datetime.now(timezone.utc)

    # Throughput metrics
    rate = events_received / max(uptime_min, 0.01)

    throughput_point = (
        Point("pipeline_throughput")
        .field("events_received_per_min", round(rate, 2))
        .field("events_published",        int(events_published))
        .field("events_received",         int(events_received))
        .field("uptime_min",              round(uptime_min, 1))
        .time(now)
    )
    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=throughput_point)

    # Consumer lag per topic — one point per topic
    for topic, lag in lag_per_topic.items():
        lag_point = (
            Point("consumer_lag")
            .tag("topic", topic)
            .field("lag", int(lag))
            .time(now)
        )
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=lag_point)

    logger.info(
        f"[METRICS WRITTEN] "
        f"rate={rate:.1f} events/min | "
        f"published={events_published} | "
        f"lag={lag_per_topic}"
    )


# ---------------------------------------------------------------------------
# Metrics collector — reads from producer stats file
# ---------------------------------------------------------------------------
def read_producer_stats() -> tuple[int, int]:
    """
    Reads cumulative stats written by stream03_producer.py.
    Falls back to 0 if stats file doesn't exist yet.

    Returns: (events_received, events_published)
    """
    stats_file = "/tmp/stream03_stats.json"
    try:
        import json
        with open(stats_file) as f:
            stats = json.load(f)
        return stats.get("events_received", 0), stats.get("events_published", 0)
    except FileNotFoundError:
        return 0, 0
    except Exception as e:
        logger.warning(f"Could not read producer stats: {e}")
        return 0, 0


# ---------------------------------------------------------------------------
# Console reporter — prints a clean metrics table every poll cycle
# ---------------------------------------------------------------------------
def print_metrics_table(
    events_received: int,
    events_published: int,
    lag_per_topic: dict[str, int],
    uptime_min: float,
):
    rate = events_received / max(uptime_min, 0.01)
    total_lag = sum(lag_per_topic.values())

    print(f"\n{'='*60}")
    print(f"  STREAM-07 METRICS  —  {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*60}")
    print(f"  Uptime              : {uptime_min:.1f} min")
    print(f"  Events received     : {events_received:,}")
    print(f"  Events published    : {events_published:,}")
    print(f"  Rate                : {rate:.1f} events/min")
    print(f"  Total consumer lag  : {total_lag:,} messages")
    print(f"\n  Consumer lag per topic:")
    for topic, lag in lag_per_topic.items():
        bar = "█" * min(lag // 10, 20)
        print(f"    {topic:<20} {lag:>6,}  {bar}")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    logger.info("Starting STREAM-07: Throughput + Consumer Lag Logging")
    logger.info(f"  Kafka    : {KAFKA_BOOTSTRAP}")
    logger.info(f"  InfluxDB : {INFLUXDB_URL}")
    logger.info(f"  Interval : {POLL_INTERVAL}s")

    influx_client = InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG,
    )

    start_time = time.time()

    try:
        while True:
            loop_start = time.time()
            uptime_min = (loop_start - start_time) / 60

            # Read producer stats
            events_received, events_published = read_producer_stats()

            # Get consumer lag from Kafka
            lag_per_topic = get_consumer_lag(
                KAFKA_BOOTSTRAP,
                SPARK_CONSUMER_GROUP,
                TOPICS,
            )

            # Print to console
            print_metrics_table(
                events_received,
                events_published,
                lag_per_topic,
                uptime_min,
            )

            # Write to InfluxDB
            try:
                write_metrics(
                    influx_client,
                    events_received,
                    events_published,
                    lag_per_topic,
                    uptime_min,
                )
            except Exception as e:
                logger.warning(f"Failed to write to InfluxDB: {e}")
                logger.warning("Metrics still printed to console.")

            # Sleep for remainder of interval
            elapsed = time.time() - loop_start
            time.sleep(max(0, POLL_INTERVAL - elapsed))

    except KeyboardInterrupt:
        logger.info("Shutdown signal received. Stopping metrics logger.")
    finally:
        influx_client.close()


if __name__ == "__main__":
    main()
