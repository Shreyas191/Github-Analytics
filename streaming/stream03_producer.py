"""
STREAM-03: Kafka Producer — GitHub Events API Polling
GitHub Developer Ecosystem Analytics — Streaming Pipeline
Shreyas Kaldate (sk12898)

Polls the GitHub Events API every 15 seconds using ETag conditional requests.
Routes each event to the correct Kafka topic by event_type.
Rotates PATs round-robin to stay within rate limits.

Key design decisions:
  - ETag headers: GitHub returns a 304 Not Modified when no new events exist.
    304s don't count against the rate limit — so polling every 15s is safe.
  - 15s poll interval: GitHub publishes new event batches roughly every 60s,
    but 15s gives us a tighter lag window with minimal wasted calls.
  - We deduplicate by event_id to avoid reprocessing the same event across
    overlapping API pages (GitHub /events returns the last ~300 events).
"""

import json
import logging
import os
import signal
import time
from collections import deque
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

from stream01_pat_rotator import PATRotator

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("stream03.producer")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
POLL_INTERVAL_SEC = 15          # seconds between API polls
GITHUB_EVENTS_URL = "https://api.github.com/events"

# Map GitHub event_type → Kafka topic name (matches STREAM-02)
TOPIC_MAP = {
    "PushEvent":        "push-events",
    "WatchEvent":       "watch-events",     # WatchEvent = starring a repo
    "ForkEvent":        "fork-events",
    "PullRequestEvent": "pr-events",
    "IssuesEvent":      "issue-events",
    "CreateEvent":      "create-events",
}

# Deduplication window — keep the last N event IDs in memory
# GitHub /events returns ~300 events; 2000 gives comfortable overlap buffer
DEDUP_WINDOW = 2000

# ---------------------------------------------------------------------------
# PAT configuration — fill in real tokens or load from env / secrets file
# ---------------------------------------------------------------------------
def load_pats() -> list[tuple[str, str]]:
    """
    Load PATs from environment variables.
    Set these before running:
        export PAT_SHREYAS=ghp_...
        export PAT_HARIHARAN=ghp_...
        export PAT_TANUSHA=ghp_...
        export PAT_VIKRAM=ghp_...
    """
    pats = []
    for alias in ["SHREYAS", "HARIHARAN", "TANUSHA", "VIKRAM"]:
        token = os.getenv(f"PAT_{alias}")
        if token:
            pats.append((token, alias.lower()))
        else:
            logger.warning(f"PAT_{alias} not set — skipping.")

    if not pats:
        raise RuntimeError(
            "No PATs found. Set PAT_SHREYAS, PAT_HARIHARAN, PAT_TANUSHA, PAT_VIKRAM "
            "as environment variables."
        )
    logger.info(f"Loaded {len(pats)} PAT(s): {[p[1] for p in pats]}")
    return pats


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------
class GitHubEventsProducer:

    def __init__(self, pats: list[tuple[str, str]], bootstrap: str = KAFKA_BOOTSTRAP):
        self.rotator = PATRotator(pats)
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",              # wait for all in-sync replicas
            retries=3,
            linger_ms=100,          # batch messages for up to 100ms
            compression_type="gzip",
        )
        self.etag: str | None = None        # ETag from last API response
        self.seen_ids: deque = deque(maxlen=DEDUP_WINDOW)
        self.running = True

        # Metrics
        self.stats = {
            "events_received": 0,
            "events_published": 0,
            "api_calls": 0,
            "api_304s": 0,
            "api_errors": 0,
            "started_at": time.time(),
        }

        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)
        logger.info(f"Producer initialized. Kafka: {bootstrap}")

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    def run(self):
        logger.info(f"Starting poll loop — interval: {POLL_INTERVAL_SEC}s")
        while self.running:
            loop_start = time.time()
            try:
                events = self._fetch_events()
                if events:
                    self._publish_events(events)
                self._log_metrics()
            except Exception as e:
                logger.error(f"Unexpected error in poll loop: {e}", exc_info=True)
                self.stats["api_errors"] += 1

            # Sleep for the remainder of the poll interval
            elapsed = time.time() - loop_start
            sleep_for = max(0, POLL_INTERVAL_SEC - elapsed)
            time.sleep(sleep_for)

        self._flush_and_close()

    # ------------------------------------------------------------------
    # GitHub API fetch with ETag conditional request
    # ------------------------------------------------------------------
    def _fetch_events(self) -> list[dict]:
        token = self.rotator.get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if self.etag:
            headers["If-None-Match"] = self.etag  # Conditional request

        self.stats["api_calls"] += 1
        try:
            resp = requests.get(GITHUB_EVENTS_URL, headers=headers, timeout=10)
        except requests.RequestException as e:
            logger.warning(f"Network error fetching events: {e}")
            self.stats["api_errors"] += 1
            return []

        # Update rate limit tracking regardless of status code
        self.rotator.update_from_headers(token, resp.headers)

        if resp.status_code == 304:
            # No new events since last poll — ETag matched
            self.stats["api_304s"] += 1
            logger.debug("304 Not Modified — no new events.")
            return []

        if resp.status_code == 403 or resp.status_code == 429:
            reset_at = float(resp.headers.get("X-RateLimit-Reset", time.time() + 3600))
            self.rotator.mark_rate_limited(token, reset_at)
            logger.warning(f"Rate limited ({resp.status_code}). Switching PAT.")
            return []

        if resp.status_code != 200:
            logger.warning(f"Unexpected status {resp.status_code}: {resp.text[:200]}")
            self.stats["api_errors"] += 1
            return []

        # Store ETag for next conditional request
        self.etag = resp.headers.get("ETag")

        events = resp.json()
        self.stats["events_received"] += len(events)
        return events

    # ------------------------------------------------------------------
    # Route and publish events to Kafka topics
    # ------------------------------------------------------------------
    def _publish_events(self, events: list[dict]):
        published = 0
        skipped_dup = 0
        skipped_unknown = 0

        for event in events:
            event_id   = event.get("id", "")
            event_type = event.get("type", "")

            # Deduplicate — GitHub /events can return the same events across polls
            if event_id in self.seen_ids:
                skipped_dup += 1
                continue
            self.seen_ids.append(event_id)

            # Route to correct topic
            topic = TOPIC_MAP.get(event_type)
            if not topic:
                skipped_unknown += 1
                continue

            # Build the message — include only what we need downstream
            message = self._extract_message(event)
            repo_name = event.get("repo", {}).get("name", "unknown")

            try:
                self.producer.send(
                    topic=topic,
                    key=repo_name,      # partition by repo so same repo → same partition
                    value=message,
                )
                published += 1
            except KafkaError as e:
                logger.error(f"Failed to publish event {event_id}: {e}")

        self.stats["events_published"] += published

        if published > 0:
            logger.info(
                f"Published {published} events | "
                f"dupes skipped: {skipped_dup} | "
                f"unknown types: {skipped_unknown}"
            )

    def _extract_message(self, event: dict) -> dict:
        """
        Flatten the raw GitHub event into the unified schema.
        Field names match Hariharan's batch Parquet schema (INFRA-03).
        STREAM-04 will assert this schema matches exactly.
        """
        repo    = event.get("repo", {})
        actor   = event.get("actor", {})
        payload = event.get("payload", {})

        def to_long(val) -> int | None:
            """Cast to Python int (maps to LongType in Spark). Returns None if missing."""
            return int(val) if val is not None else None

        def to_int(val) -> int | None:
            """Cast to Python int (maps to IntegerType in Spark). Returns None if missing."""
            return int(val) if val is not None else None

        return {
            # Core fields — always present
            # event_id is StringType in schema — keep as string
            "event_id":    str(event.get("id")) if event.get("id") is not None else None,
            "event_type":  event.get("type"),
            # repo_id, actor_id are LongType — cast to int
            "repo_id":     to_long(repo.get("id")),
            "repo_name":   repo.get("name"),
            "actor_id":    to_long(actor.get("id")),
            "actor_login": actor.get("login"),
            # created_at is TimestampType — keep ISO 8601 string, Spark parses it
            "created_at":  event.get("created_at"),
            "ingested_at": datetime.now(timezone.utc).isoformat(),

            # Event-type-specific payload fields
            # Null-filled for event types where they don't apply
            "ref":         payload.get("ref"),              # CreateEvent / PushEvent
            "ref_type":    payload.get("ref_type"),         # CreateEvent: "repository"/"branch"/"tag"
            "action":      payload.get("action"),           # IssuesEvent, PREvent
            # forkee_id, pr_number are LongType — cast to int
            "forkee_id":   to_long(payload.get("forkee", {}).get("id") if payload.get("forkee") else None),
            "pr_number":   to_long(payload.get("number")),
            # push_size is IntegerType — cast to int
            "push_size":   to_int(payload.get("size")),
            "language":    None,                            # Enriched by batch path (INFRA-04)
        }

    # ------------------------------------------------------------------
    # Metrics logging — called every poll cycle
    # ------------------------------------------------------------------
    def _log_metrics(self):
        uptime_min = (time.time() - self.stats["started_at"]) / 60
        if uptime_min < 0.01:
            return
        rate = self.stats["events_received"] / uptime_min
        logger.info(
            f"[METRICS] uptime={uptime_min:.1f}min | "
            f"api_calls={self.stats['api_calls']} | "
            f"304s={self.stats['api_304s']} | "
            f"events_received={self.stats['events_received']} | "
            f"events_published={self.stats['events_published']} | "
            f"rate={rate:.1f} events/min | "
            f"PAT status={self.rotator.status()}"
        )

    # ------------------------------------------------------------------
    # Graceful shutdown
    # ------------------------------------------------------------------
    def _shutdown(self, signum, frame):
        logger.info("Shutdown signal received. Flushing producer...")
        self.running = False

    def _flush_and_close(self):
        self.producer.flush(timeout=10)
        self.producer.close()
        logger.info("Producer shut down cleanly.")
        self._log_metrics()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    pats = load_pats()
    producer = GitHubEventsProducer(pats=pats)
    producer.run()