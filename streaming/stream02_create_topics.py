"""
STREAM-02: Kafka Topic Setup
GitHub Developer Ecosystem Analytics — Streaming Pipeline
Shreyas Kaldate (sk12898)

Creates all 7 required Kafka topics with correct config.
Run this ONCE after Hariharan's Docker Compose (INFRA-01) is up.

Topics:
    push-events       — PushEvent
    watch-events      — WatchEvent (stars)
    fork-events       — ForkEvent
    pr-events         — PullRequestEvent
    issue-events      — IssuesEvent
    create-events     — CreateEvent (new repos)
    pending-repos     — repos queued for 48h ML scoring (STREAM-06)

Config per topic:
    partitions        = 4
    replication       = 1  (single-node local setup)
    retention.ms      = 172800000  (48 hours — minimum for scoring queue)
"""

import logging
import sys
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config — update KAFKA_BOOTSTRAP if your broker runs on a different port
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = "localhost:9092"   # matches Hariharan's Docker Compose default

TOPICS = [
    "push-events",
    "watch-events",
    "fork-events",
    "pr-events",
    "issue-events",
    "create-events",
    "pending-repos",   # Used by STREAM-06 for 48h ML scoring queue
]

PARTITIONS = 4
REPLICATION = 1
RETENTION_MS = 172_800_000   # 48 hours in milliseconds


def create_topics(bootstrap: str = KAFKA_BOOTSTRAP):
    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap,
        client_id="stream02-topic-setup",
        request_timeout_ms=10_000,
    )

    topic_configs = {"retention.ms": str(RETENTION_MS)}

    new_topics = [
        NewTopic(
            name=name,
            num_partitions=PARTITIONS,
            replication_factor=REPLICATION,
            topic_configs=topic_configs,
        )
        for name in TOPICS
    ]

    logger.info(f"Connecting to Kafka at {bootstrap} ...")
    logger.info(f"Creating {len(new_topics)} topics with "
                f"{PARTITIONS} partitions, retention={RETENTION_MS}ms (48h)")

    created, skipped = [], []
    for topic in new_topics:
        try:
            admin.create_topics([topic], validate_only=False)
            logger.info(f"  ✓ Created   : {topic.name}")
            created.append(topic.name)
        except TopicAlreadyExistsError:
            logger.info(f"  ~ Exists    : {topic.name} (skipped)")
            skipped.append(topic.name)
        except Exception as e:
            logger.error(f"  ✗ Failed    : {topic.name} — {e}")
            raise

    admin.close()

    print(f"\n{'='*50}")
    print(f"Done. Created: {len(created)}  Skipped: {len(skipped)}")
    print(f"Topics ready: {TOPICS}")
    print(f"{'='*50}\n")

    return created, skipped


def verify_topics(bootstrap: str = KAFKA_BOOTSTRAP):
    """List all topics on the broker to confirm setup."""
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(bootstrap_servers=bootstrap)
    existing = consumer.topics()
    consumer.close()

    our_topics = [t for t in TOPICS if t in existing]
    missing = [t for t in TOPICS if t not in existing]

    print("\n--- Topic verification ---")
    for t in our_topics:
        print(f"  ✓ {t}")
    for t in missing:
        print(f"  ✗ MISSING: {t}")

    if missing:
        logger.error(f"Missing topics: {missing}. Re-run this script.")
        sys.exit(1)
    else:
        logger.info("All 7 topics verified successfully.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="STREAM-02: Create Kafka topics")
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP,
                        help="Kafka bootstrap server (default: localhost:9092)")
    parser.add_argument("--verify-only", action="store_true",
                        help="Only verify topics exist, don't create")
    args = parser.parse_args()

    if args.verify_only:
        verify_topics(args.bootstrap)
    else:
        create_topics(args.bootstrap)
        verify_topics(args.bootstrap)
