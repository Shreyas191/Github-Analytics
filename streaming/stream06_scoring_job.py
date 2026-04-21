"""
STREAM-06: 48h Pending-Repos Queue + ML Scoring Job
GitHub Developer Ecosystem Analytics — Streaming Pipeline
Shreyas Kaldate (sk12898)

What this does:
  Part A — Queue Builder (runs continuously):
    Watches the create-events Kafka topic for new repos.
    On first appearance of a repo_id, publishes it to the
    'pending-repos' topic with a first_seen timestamp.

  Part B — Scoring Job (runs every 30 minutes):
    Reads pending-repos topic.
    Filters repos where now() - first_seen >= 48 hours.
    Loads Tanusha's PipelineModel from HDFS.
    Scores each repo using the 8 feature vector.
    Writes results to InfluxDB measurement 'viral_predictions'.

Model path (from Tanusha ML-06):
    hdfs://namenode:9000/github/ml/model/rf_trained/

Load command:
    from pyspark.ml import PipelineModel
    model = PipelineModel.load(MODEL_PATH)

Note from Tanusha:
    language field is null in stream.
    F7 (lang_rank) defaults to 0 for live scoring.

Usage:
  # Part A — Queue builder (run alongside producer)
  python stream06_scoring_job.py --mode queue

  # Part B — Scoring job (run every 30 min via cron or manually)
  python stream06_scoring_job.py --mode score

  # Run both together
  python stream06_scoring_job.py --mode both
"""

import argparse
import json
import logging
import os
import time
import threading
from datetime import datetime, timezone, timedelta

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("stream06.scoring")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP",  "localhost:29092")
INFLUXDB_URL    = os.getenv("INFLUXDB_URL",     "http://localhost:8086")
INFLUXDB_TOKEN  = os.getenv("INFLUXDB_TOKEN",   "github-analytics-token")
INFLUXDB_ORG    = os.getenv("INFLUXDB_ORG",     "github-analytics")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET",  "github-stream")

# Model path from Tanusha's ML-06
MODEL_PATH      = os.getenv(
    "MODEL_PATH",
    "hdfs://namenode:9000/github/ml/model/rf_trained/"
)

SCORING_INTERVAL_SEC = 30 * 60   # run scoring every 30 minutes
PENDING_TOPIC        = "pending-repos"
CREATE_TOPIC         = "create-events"
MIN_AGE_HOURS        = 48         # only score repos older than 48h


# ---------------------------------------------------------------------------
# Part A — Queue Builder
# Watches create-events, publishes new repos to pending-repos
# ---------------------------------------------------------------------------
class PendingRepoQueue:
    """
    Watches create-events topic for CreateEvent with ref_type='repository'.
    On first appearance of a repo_id, publishes to pending-repos topic
    with first_seen timestamp.

    Deduplication: keeps seen repo_ids in memory.
    On restart, reads pending-repos from earliest to rebuild the seen set.
    """

    def __init__(self, bootstrap: str = KAFKA_BOOTSTRAP):
        self.bootstrap  = bootstrap
        self.seen_repos: set[int] = set()
        self.running    = True

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8") if k else None,
            acks="all",
        )

        # Rebuild seen set from existing pending-repos topic
        self._rebuild_seen_set()
        logger.info(f"Queue builder ready. Already tracking {len(self.seen_repos)} repos.")

    def _rebuild_seen_set(self):
        """Read all existing pending-repos messages to rebuild dedup state."""
        consumer = KafkaConsumer(
            PENDING_TOPIC,
            bootstrap_servers=self.bootstrap,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=3000,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        for msg in consumer:
            repo_id = msg.value.get("repo_id")
            if repo_id:
                self.seen_repos.add(int(repo_id))
        consumer.close()
        logger.info(f"Rebuilt seen set: {len(self.seen_repos)} repos already in queue.")

    def run(self):
        """Main loop — consume create-events and queue new repos."""
        consumer = KafkaConsumer(
            CREATE_TOPIC,
            bootstrap_servers=self.bootstrap,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="stream06-queue-builder",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=5000,
        )

        logger.info("Queue builder started. Watching create-events topic...")
        queued = 0

        while self.running:
            try:
                for msg in consumer:
                    event = msg.value

                    # Only queue actual new repositories, not branch/tag creates
                    if event.get("ref_type") != "repository":
                        continue

                    repo_id   = event.get("repo_id")
                    repo_name = event.get("repo_name")

                    if not repo_id or repo_id in self.seen_repos:
                        continue

                    # First time we've seen this repo — add to pending queue
                    self.seen_repos.add(int(repo_id))
                    pending_entry = {
                        "repo_id":    repo_id,
                        "repo_name":  repo_name,
                        "first_seen": datetime.now(timezone.utc).isoformat(),
                        "actor_id":   event.get("actor_id"),
                        "actor_login":event.get("actor_login"),
                        "language":   event.get("language"),  # null for new repos
                    }

                    self.producer.send(
                        topic=PENDING_TOPIC,
                        key=repo_id,
                        value=pending_entry,
                    )
                    queued += 1
                    logger.info(
                        f"Queued new repo: {repo_name} (repo_id={repo_id}). "
                        f"Total queued: {queued}"
                    )

            except Exception as e:
                logger.error(f"Queue builder error: {e}", exc_info=True)
                time.sleep(5)

        consumer.close()
        self.producer.flush()
        self.producer.close()
        logger.info(f"Queue builder stopped. Total repos queued: {queued}")


# ---------------------------------------------------------------------------
# Part B — Scoring Job
# Reads pending-repos, scores repos >= 48h old using Tanusha's model
# ---------------------------------------------------------------------------
class RepoScoringJob:
    """
    Runs every 30 minutes.
    Reads all pending repos, filters those >= 48h old,
    scores them with the Random Forest model, writes to InfluxDB.
    """

    def __init__(self, bootstrap: str = KAFKA_BOOTSTRAP):
        self.bootstrap = bootstrap

    def run_once(self):
        """Execute one scoring pass."""
        logger.info("Starting scoring pass...")

        # Read all pending repos
        pending = self._read_pending_repos()
        logger.info(f"Total repos in pending queue: {len(pending)}")

        # Filter repos that are >= 48h old
        cutoff = datetime.now(timezone.utc) - timedelta(hours=MIN_AGE_HOURS)
        ready_to_score = [
            r for r in pending
            if self._parse_ts(r.get("first_seen")) <= cutoff
        ]
        logger.info(f"Repos ready to score (>= 48h old): {len(ready_to_score)}")

        if not ready_to_score:
            logger.info("No repos ready to score yet. Will retry in 30 min.")
            return

        # Score with PySpark + Tanusha's model
        predictions = self._score_repos(ready_to_score)

        # Write results to InfluxDB
        if predictions:
            self._write_to_influxdb(predictions)

        logger.info(f"Scoring pass complete. Scored {len(predictions)} repos.")

    def run_loop(self):
        """Run scoring every SCORING_INTERVAL_SEC."""
        logger.info(f"Scoring loop started. Interval: {SCORING_INTERVAL_SEC // 60} min.")
        while True:
            try:
                self.run_once()
            except Exception as e:
                logger.error(f"Scoring pass failed: {e}", exc_info=True)
            logger.info(f"Next scoring pass in {SCORING_INTERVAL_SEC // 60} min.")
            time.sleep(SCORING_INTERVAL_SEC)

    def _read_pending_repos(self) -> list[dict]:
        """Read all messages from pending-repos topic."""
        consumer = KafkaConsumer(
            PENDING_TOPIC,
            bootstrap_servers=self.bootstrap,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=5000,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        repos = {}
        for msg in consumer:
            entry   = msg.value
            repo_id = entry.get("repo_id")
            if repo_id:
                # Keep latest entry per repo_id (dedup)
                repos[repo_id] = entry
        consumer.close()
        return list(repos.values())

    def _score_repos(self, repos: list[dict]) -> list[dict]:
        """
        Score repos using Tanusha's PipelineModel.
        Loads model from HDFS, builds feature vectors, runs predictions.

        Note from Tanusha (ML-06):
          language is null in stream — f7_lang_rank defaults to 0.
          This is expected and handled below.
        """
        from pyspark.sql import SparkSession
        from pyspark.ml import PipelineModel
        from pyspark.sql.types import (
            StructType, StructField,
            LongType, StringType, DoubleType, IntegerType
        )

        spark = (
            SparkSession.builder
            .appName("STREAM-06-scoring")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")

        # Build input schema for scoring
        # All 8 features must be present — null fills with 0
        input_schema = StructType([
            StructField("repo_id",              LongType(),   True),
            StructField("repo_name",            StringType(), True),
            StructField("first_seen",           StringType(), True),
            StructField("f1_stars_48h",         LongType(),   True),
            StructField("f2_forks_48h",         LongType(),   True),
            StructField("f3_contributors_48h",  LongType(),   True),
            StructField("f4_issues_48h",        LongType(),   True),
            StructField("f5_owner_star_med",    DoubleType(), True),
            StructField("f6_readme_bytes",      LongType(),   True),
            StructField("f7_lang_rank",         IntegerType(),True),
            StructField("f8_pushes_48h",        LongType(),   True),
        ])

        # Build rows — features not available in stream default to 0
        # f1-f4, f8 would need 48h of streaming data to compute properly
        # For live scoring we use whatever we have, defaulting to 0
        rows = []
        for r in repos:
            rows.append((
                int(r["repo_id"]),
                r.get("repo_name"),
                r.get("first_seen"),
                int(r.get("f1_stars_48h",        0) or 0),
                int(r.get("f2_forks_48h",        0) or 0),
                int(r.get("f3_contributors_48h", 0) or 0),
                int(r.get("f4_issues_48h",       0) or 0),
                float(r.get("f5_owner_star_med", 0.0) or 0.0),
                int(r.get("f6_readme_bytes",     0) or 0),
                int(r.get("f7_lang_rank",        0) or 0),  # null in stream
                int(r.get("f8_pushes_48h",       0) or 0),
            ))

        df = spark.createDataFrame(rows, schema=input_schema)

        # Load Tanusha's model from HDFS
        logger.info(f"Loading model from {MODEL_PATH}...")
        try:
            model = PipelineModel.load(MODEL_PATH)
        except Exception as e:
            logger.error(f"Failed to load model from {MODEL_PATH}: {e}")
            logger.error("Make sure Tanusha's ML-05 + ML-06 have completed.")
            spark.stop()
            return []

        # Run predictions
        predictions_df = model.transform(df)

        # Extract results
        results = []
        for row in predictions_df.select(
            "repo_id", "repo_name", "first_seen",
            "prediction", "probability"
        ).collect():
            prob_viral = float(row["probability"][1])  # P(viral)
            results.append({
                "repo_id":    row["repo_id"],
                "repo_name":  row["repo_name"],
                "first_seen": row["first_seen"],
                "prediction": int(row["prediction"]),
                "prob_viral": round(prob_viral, 4),
                "scored_at":  datetime.now(timezone.utc).isoformat(),
            })

        spark.stop()

        # Log viral predictions
        viral = [r for r in results if r["prediction"] == 1]
        logger.info(
            f"Scoring complete: {len(results)} repos scored, "
            f"{len(viral)} predicted viral."
        )
        for v in viral:
            logger.info(
                f"  VIRAL PREDICTION: {v['repo_name']} "
                f"(prob={v['prob_viral']:.3f})"
            )

        return results

    def _write_to_influxdb(self, predictions: list[dict]):
        """Write scoring results to InfluxDB measurement 'viral_predictions'."""
        from influxdb_client import InfluxDBClient, Point
        from influxdb_client.client.write_api import SYNCHRONOUS

        client = InfluxDBClient(
            url=INFLUXDB_URL,
            token=INFLUXDB_TOKEN,
            org=INFLUXDB_ORG,
        )
        write_api = client.write_api(write_options=SYNCHRONOUS)

        points = []
        for r in predictions:
            point = (
                Point("viral_predictions")
                .tag("repo_name", r["repo_name"])
                .tag("prediction", str(r["prediction"]))
                .field("prob_viral",  r["prob_viral"])
                .field("is_viral",    r["prediction"])
                .field("repo_id",     r["repo_id"])
            )
            points.append(point)

        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)
        client.close()

        viral_count = sum(1 for r in predictions if r["prediction"] == 1)
        logger.info(
            f"Written {len(points)} predictions to InfluxDB. "
            f"Viral: {viral_count}"
        )

    @staticmethod
    def _parse_ts(ts_str: str | None) -> datetime:
        """Parse ISO timestamp string to datetime."""
        if not ts_str:
            return datetime.now(timezone.utc)
        try:
            return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except Exception:
            return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="STREAM-06: Scoring Job")
    parser.add_argument(
        "--mode",
        choices=["queue", "score", "both"],
        default="both",
        help="queue=queue builder only, score=scoring only, both=run both"
    )
    args = parser.parse_args()

    logger.info(f"Starting STREAM-06 in mode: {args.mode}")
    logger.info(f"  Kafka    : {KAFKA_BOOTSTRAP}")
    logger.info(f"  InfluxDB : {INFLUXDB_URL}")
    logger.info(f"  Model    : {MODEL_PATH}")

    if args.mode == "queue":
        queue = PendingRepoQueue(KAFKA_BOOTSTRAP)
        queue.run()

    elif args.mode == "score":
        scorer = RepoScoringJob(KAFKA_BOOTSTRAP)
        scorer.run_loop()

    elif args.mode == "both":
        # Run queue builder in background thread
        queue   = PendingRepoQueue(KAFKA_BOOTSTRAP)
        scorer  = RepoScoringJob(KAFKA_BOOTSTRAP)

        queue_thread = threading.Thread(target=queue.run, daemon=True)
        queue_thread.start()
        logger.info("Queue builder running in background thread.")

        # Run scoring loop in main thread
        scorer.run_loop()


if __name__ == "__main__":
    main()
