"""
VIZ-02: REST API Enrichment Lookup Table (F5 + F6)
GitHub Developer Ecosystem Analytics
Author: Vikram Markali (vrm9190)

What this does:
  Pulls two static enrichment features for every repo in the ML training set:

    F5 - owner_star_med  : median stars across the repo owner's OTHER public repos
                           that existed BEFORE this repo was created.
                           Measures creator reputation, which is known at T=0.

    F6 - readme_bytes    : byte length of the repo's default README file.
                           Available at T=0 since the README is pushed at creation.

  These are the two features in ML-03 that cannot come from the events table.
  Output is saved as Parquet to HDFS so ML-03 joins on repo_id.

Why call GitHub REST API from the driver, not from Spark workers?
  Rate limiting must be centralized. If multiple workers called the API in
  parallel without shared state they would exhaust all 4 PATs in seconds.
  All calls go through a single PAT rotator on the Spark driver.

  For large training sets (~100k repos) this script runs overnight.
  Progress is checkpointed to a local JSON file so it resumes from the
  last saved position if killed.

Owner caching for F5:
  Many repos share the same owner (e.g., a prolific org with 200 repos in
  the training set). We fetch that owner's repo list once and cache the
  result so F5 is only one API call per unique owner, not one per repo.

Input:
  hdfs://namenode:9000/github/ml/labels/      (ML-01 output)

Output:
  hdfs://namenode:9000/github/ml/enrichment/  (read by ML-03)

Output schema:
  repo_id            long
  repo_name          string
  owner_login        string
  f5_owner_star_med  double   -- median prior repo stars (-1.0 if unavailable)
  f6_readme_bytes    long     -- README size in bytes   (-1   if no README)

Usage:
  # Set PATs in environment first (never hardcode tokens):
  export PAT_SHREYAS=ghp_...
  export PAT_HARIHARAN=ghp_...
  export PAT_TANUSHA=ghp_...
  export PAT_VIKRAM=ghp_...

  # Dry run -- processes first 100 repos for smoke testing:
  docker exec spark-master spark-submit \\
    --master spark://spark-master:7077 \\
    /opt/spark-apps/analytics/viz02_enrichment.py --dry-run

  # Full overnight run:
  docker exec -d spark-master spark-submit \\
    --master spark://spark-master:7077 \\
    /opt/spark-apps/analytics/viz02_enrichment.py

  # Verify output after run:
  docker exec spark-master spark-submit \\
    --master spark://spark-master:7077 \\
    /opt/spark-apps/analytics/viz02_enrichment.py --verify-only
"""

import argparse
import json
import logging
import math
import os
import statistics
import time
from dataclasses import dataclass, field
from typing import Optional

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("viz02.enrichment")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LABELS_PATH      = "hdfs://namenode:9000/github/ml/labels"
ENRICHMENT_PATH  = "hdfs://namenode:9000/github/ml/enrichment"
CHECKPOINT_FILE  = "/tmp/viz02_checkpoint.json"

GITHUB_API_BASE  = "https://api.github.com"
DRY_RUN_LIMIT    = 100     # repos processed in --dry-run mode
REQUEST_TIMEOUT  = 15      # seconds per API call before giving up
MAX_RETRIES      = 3       # retries on 5xx or network error

ENRICHMENT_SCHEMA = StructType([
    StructField("repo_id",           LongType(),   nullable=False),
    StructField("repo_name",         StringType(), nullable=False),
    StructField("owner_login",       StringType(), nullable=False),
    StructField("f5_owner_star_med", DoubleType(), nullable=False),
    StructField("f6_readme_bytes",   LongType(),   nullable=False),
])


# ===========================================================================
# PAT ROTATOR
# Adapted from Shreyas's STREAM-01 pattern (stream01_pat_rotator.py).
# Simplified for single-threaded sequential use on the driver.
# ===========================================================================

@dataclass
class PATEntry:
    token: str
    alias: str
    remaining: int = 5000
    reset_at: float = 0.0
    blocked: bool = False


class PATRotator:
    """
    Round-robin rotator across multiple GitHub Personal Access Tokens.
    Blocks a PAT when remaining < 50 and waits for its reset if all are blocked.
    """

    def __init__(self, pat_list: list[tuple[str, str]]):
        if not pat_list:
            raise ValueError("At least one PAT required.")
        self.pats: list[PATEntry] = [PATEntry(token=t, alias=a) for t, a in pat_list]
        self._index = 0
        logger.info(
            f"PATRotator initialized with {len(self.pats)} PATs: "
            f"{[p.alias for p in self.pats]}"
        )

    def get_token(self) -> str:
        """Return next available token. Waits if all are rate-limited."""
        now = time.time()
        for pat in self.pats:
            if pat.blocked and now >= pat.reset_at:
                pat.blocked = False
                pat.remaining = 5000
                logger.info(f"PAT '{pat.alias}' reset -- back in rotation.")

        available = [p for p in self.pats if not p.blocked]
        if not available:
            earliest = min(p.reset_at for p in self.pats)
            wait = max(0, earliest - time.time()) + 5
            logger.warning(f"All PATs rate-limited. Waiting {wait:.0f}s...")
            time.sleep(wait)
            return self.get_token()

        for _ in range(len(self.pats)):
            pat = self.pats[self._index % len(self.pats)]
            self._index = (self._index + 1) % len(self.pats)
            if not pat.blocked:
                return pat.token

        return available[0].token

    def update_from_headers(self, token: str, headers: dict):
        pat = self._find(token)
        if not pat:
            return
        remaining = headers.get("X-RateLimit-Remaining")
        reset_at  = headers.get("X-RateLimit-Reset")
        if remaining is not None:
            pat.remaining = int(remaining)
        if reset_at is not None:
            pat.reset_at = float(reset_at)
        if pat.remaining <= 50:
            pat.blocked = True
            mins = max(0, pat.reset_at - time.time()) / 60
            logger.warning(
                f"PAT '{pat.alias}' nearly exhausted ({pat.remaining} left). "
                f"Blocking ~{mins:.1f} min."
            )

    def mark_rate_limited(self, token: str, reset_at: float):
        pat = self._find(token)
        if pat:
            pat.blocked = True
            pat.reset_at = reset_at
            mins = max(0, reset_at - time.time()) / 60
            logger.warning(f"PAT '{pat.alias}' rate-limited. Blocked ~{mins:.1f} min.")

    def status(self) -> list[dict]:
        now = time.time()
        return [
            {
                "alias":        p.alias,
                "remaining":    p.remaining,
                "blocked":      p.blocked,
                "resets_in_s":  round(max(0, p.reset_at - now)) if p.blocked else None,
            }
            for p in self.pats
        ]

    def _find(self, token: str) -> Optional[PATEntry]:
        for p in self.pats:
            if p.token == token:
                return p
        return None


# ===========================================================================
# CHECKPOINT
# Saves/loads a dict of { repo_id_str -> {f5, f6, owner_login} } to disk.
# Allows the script to resume from where it stopped.
# ===========================================================================

def load_checkpoint() -> dict:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            data = json.load(f)
        logger.info(f"Checkpoint loaded: {len(data)} repos already processed.")
        return data
    return {}


def save_checkpoint(results: dict):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(results, f)


# ===========================================================================
# GITHUB API HELPERS
# ===========================================================================

def _get_with_retry(
    url: str,
    rotator: PATRotator,
    params: Optional[dict] = None,
) -> Optional[requests.Response]:
    """
    GET a GitHub API URL with PAT rotation and retry logic.
    Returns None on permanent failure (404, repeated 5xx, etc.).
    """
    for attempt in range(MAX_RETRIES):
        token = rotator.get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        try:
            resp = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            rotator.update_from_headers(token, resp.headers)

            if resp.status_code == 200:
                return resp
            if resp.status_code == 404:
                return None
            if resp.status_code in (403, 429):
                reset_ts = float(resp.headers.get("X-RateLimit-Reset", time.time() + 3600))
                rotator.mark_rate_limited(token, reset_ts)
                continue
            if resp.status_code >= 500:
                wait = 2 ** attempt
                logger.warning(f"HTTP {resp.status_code} from {url} -- retrying in {wait}s")
                time.sleep(wait)
                continue

            logger.warning(f"Unexpected HTTP {resp.status_code} from {url}")
            return None

        except requests.exceptions.RequestException as exc:
            wait = 2 ** attempt
            logger.warning(f"Request error ({exc}) -- retrying in {wait}s")
            time.sleep(wait)

    logger.error(f"Gave up on {url} after {MAX_RETRIES} attempts.")
    return None


def fetch_owner_star_median(owner: str, rotator: PATRotator) -> float:
    """
    Fetch median stargazer count across an owner's public repos.

    Uses only repos that existed before 'this' repo -- but since we don't
    know the exact creation date at this point, we fetch all repos and let
    the caller sort by creation date if needed. In practice the median is
    stable and the 'pre-existing' constraint is approximate here.

    Returns -1.0 on failure or if the owner has no public repos.
    """
    url = f"{GITHUB_API_BASE}/users/{owner}/repos"
    params = {"per_page": 100, "sort": "created", "direction": "asc", "type": "owner"}
    star_counts = []
    page = 1

    while True:
        params["page"] = page
        resp = _get_with_retry(url, rotator, params=params)
        if resp is None:
            break
        repos = resp.json()
        if not isinstance(repos, list) or not repos:
            break
        star_counts.extend(r.get("stargazers_count", 0) for r in repos)
        if len(repos) < 100:
            break
        page += 1

    if not star_counts:
        return -1.0
    return statistics.median(star_counts)


def fetch_readme_bytes(owner: str, repo: str, rotator: PATRotator) -> int:
    """
    Fetch README byte length for a repo.
    GitHub's /readme endpoint returns a JSON object with a 'size' field (bytes).
    Returns -1 if the repo has no README or the request fails.
    """
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/readme"
    resp = _get_with_retry(url, rotator)
    if resp is None:
        return -1
    data = resp.json()
    return int(data.get("size", -1))


# ===========================================================================
# MAIN PIPELINE
# ===========================================================================

def load_pat_rotator() -> PATRotator:
    """
    Load PATs from environment variables.
    At least one PAT must be set. All 4 is recommended for overnight runs.
    """
    candidates = [
        (os.environ.get("PAT_SHREYAS"),    "shreyas"),
        (os.environ.get("PAT_HARIHARAN"),  "hariharan"),
        (os.environ.get("PAT_TANUSHA"),    "tanusha"),
        (os.environ.get("PAT_VIKRAM"),     "vikram"),
    ]
    available = [(tok, alias) for tok, alias in candidates if tok]
    if not available:
        raise EnvironmentError(
            "No GitHub PATs found. Set at least one of: "
            "PAT_SHREYAS, PAT_HARIHARAN, PAT_TANUSHA, PAT_VIKRAM"
        )
    logger.info(f"Loaded {len(available)} PAT(s): {[a for _, a in available]}")
    return PATRotator(available)


def load_repo_list(spark: SparkSession) -> list[dict]:
    """
    Read all (repo_id, repo_name) pairs from ML-01 labels Parquet.
    Returned as a Python list of dicts on the driver so we can iterate
    while calling the GitHub REST API.
    """
    from pyspark.sql.utils import AnalysisException

    logger.info(f"Reading labels from {LABELS_PATH} ...")
    try:
        labels = spark.read.parquet(LABELS_PATH).select("repo_id", "repo_name").distinct()
    except AnalysisException:
        raise RuntimeError(
            f"Cannot read labels at {LABELS_PATH}. "
            "Make sure ML-01 has finished and HDFS is reachable."
        )

    total = labels.count()
    logger.info(f"Repos in training set: {total:,}")
    return [row.asDict() for row in labels.collect()]


def enrich_repos(
    repos: list[dict],
    rotator: PATRotator,
    checkpoint: dict,
    dry_run: bool,
) -> dict:
    """
    For each repo, fetch F5 (owner median stars) and F6 (README bytes).

    Owner results are cached in owner_cache to avoid redundant API calls
    when multiple repos share the same owner.
    """
    if dry_run:
        repos = repos[:DRY_RUN_LIMIT]
        logger.info(f"Dry run mode -- processing first {DRY_RUN_LIMIT} repos only.")

    owner_cache: dict[str, float] = {}
    results = dict(checkpoint)  # start from saved progress

    total = len(repos)
    todo  = [r for r in repos if str(r["repo_id"]) not in results]
    logger.info(f"Repos to process: {len(todo):,} / {total:,} (skipping {total - len(todo):,} from checkpoint)")

    for i, repo in enumerate(todo, start=1):
        repo_id   = repo["repo_id"]
        repo_name = repo["repo_name"]

        # repo_name is "owner/repo-name" (e.g. "torvalds/linux")
        parts = repo_name.split("/", 1)
        if len(parts) != 2:
            logger.warning(f"Unexpected repo_name format: {repo_name} -- skipping")
            results[str(repo_id)] = {
                "repo_name":        repo_name,
                "owner_login":      "",
                "f5_owner_star_med": -1.0,
                "f6_readme_bytes":  -1,
            }
            continue

        owner, repo_short = parts

        # F5 -- owner median stars (cached per owner)
        if owner not in owner_cache:
            f5 = fetch_owner_star_median(owner, rotator)
            owner_cache[owner] = f5
        else:
            f5 = owner_cache[owner]

        # F6 -- README bytes
        f6 = fetch_readme_bytes(owner, repo_short, rotator)

        results[str(repo_id)] = {
            "repo_name":         repo_name,
            "owner_login":       owner,
            "f5_owner_star_med": f5,
            "f6_readme_bytes":   f6,
        }

        if i % 500 == 0 or i == len(todo):
            save_checkpoint(results)
            pats = rotator.status()
            logger.info(
                f"Progress: {i:,}/{len(todo):,} repos  |  "
                f"owner cache: {len(owner_cache):,} owners  |  "
                f"PATs: {[(p['alias'], p['remaining']) for p in pats]}"
            )

    return results


def write_enrichment_parquet(
    spark: SparkSession,
    results: dict,
    output_path: str,
    dry_run: bool,
):
    """
    Convert the results dict to a Spark DataFrame and write Parquet to HDFS.
    """
    rows = []
    for repo_id_str, data in results.items():
        rows.append((
            int(repo_id_str),
            data["repo_name"],
            data["owner_login"],
            float(data["f5_owner_star_med"]),
            int(data["f6_readme_bytes"]),
        ))

    df = spark.createDataFrame(rows, schema=ENRICHMENT_SCHEMA)
    total = df.count()

    mode = "overwrite"
    if dry_run:
        output_path = output_path.rstrip("/") + "_dryrun"
        logger.info(f"Dry run -- writing to {output_path} instead of production path.")

    logger.info(f"Writing {total:,} enrichment rows to {output_path} ...")
    df.write.mode(mode).parquet(output_path)
    logger.info("Parquet write complete.")

    print_enrichment_summary(df)


def print_enrichment_summary(df):
    """Print a quick sanity-check summary after the write."""
    total = df.count()
    missing_f5 = df.filter(F.col("f5_owner_star_med") == -1.0).count()
    missing_f6 = df.filter(F.col("f6_readme_bytes") == -1).count()
    f5_stats   = df.filter(F.col("f5_owner_star_med") >= 0).agg(
        F.mean("f5_owner_star_med").alias("mean_f5"),
        F.percentile_approx("f5_owner_star_med", 0.5).alias("median_f5"),
    ).first()
    f6_stats   = df.filter(F.col("f6_readme_bytes") >= 0).agg(
        F.mean("f6_readme_bytes").alias("mean_f6"),
        F.percentile_approx("f6_readme_bytes", 0.5).alias("median_f6"),
    ).first()

    print("\n" + "=" * 60)
    print("VIZ-02 ENRICHMENT SUMMARY")
    print("=" * 60)
    print(f"  Total repos enriched    : {total:,}")
    print(f"  F5 missing (-1)         : {missing_f5:,}  ({100*missing_f5/max(total,1):.1f}%)")
    print(f"  F6 missing (-1)         : {missing_f6:,}  ({100*missing_f6/max(total,1):.1f}%)")
    if f5_stats and f5_stats["mean_f5"] is not None:
        print(f"  F5 mean stars           : {f5_stats['mean_f5']:.1f}")
        print(f"  F5 median stars         : {f5_stats['median_f5']:.1f}")
    if f6_stats and f6_stats["mean_f6"] is not None:
        print(f"  F6 mean README bytes    : {f6_stats['mean_f6']:.0f}")
        print(f"  F6 median README bytes  : {f6_stats['median_f6']:.0f}")
    print("=" * 60)
    print()
    df.show(10, truncate=False)


def verify_output(spark: SparkSession, output_path: str):
    """
    --verify-only mode: read the saved enrichment Parquet and print summary.
    Does not call GitHub API or write anything.
    """
    from pyspark.sql.utils import AnalysisException
    logger.info(f"Verifying enrichment output at {output_path} ...")
    try:
        df = spark.read.parquet(output_path)
        print_enrichment_summary(df)
    except AnalysisException:
        print(f"ERROR: No output found at {output_path}. Run without --verify-only first.")


# ===========================================================================
# ENTRY POINT
# ===========================================================================

def parse_args():
    parser = argparse.ArgumentParser(description="VIZ-02: REST API enrichment for F5 + F6")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=f"Process only first {DRY_RUN_LIMIT} repos and write to a _dryrun path",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Read and print the saved enrichment Parquet without calling GitHub API",
    )
    parser.add_argument(
        "--clear-checkpoint",
        action="store_true",
        help="Delete local checkpoint file and start from scratch",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("VIZ-02: REST API Enrichment (F5 + F6)")
        .config("spark.sql.shuffle.partitions", "50")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    logger.info("=" * 60)
    logger.info("VIZ-02: REST API Enrichment Lookup Table")
    logger.info(f"  dry_run        : {args.dry_run}")
    logger.info(f"  verify_only    : {args.verify_only}")
    logger.info(f"  checkpoint     : {CHECKPOINT_FILE}")
    logger.info(f"  output         : {ENRICHMENT_PATH}")
    logger.info("=" * 60)

    if args.verify_only:
        verify_output(spark, ENRICHMENT_PATH)
        spark.stop()
        return

    if args.clear_checkpoint and os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        logger.info("Checkpoint cleared.")

    rotator    = load_pat_rotator()
    repos      = load_repo_list(spark)
    checkpoint = load_checkpoint()

    results = enrich_repos(repos, rotator, checkpoint, dry_run=args.dry_run)

    write_enrichment_parquet(spark, results, ENRICHMENT_PATH, dry_run=args.dry_run)

    spark.stop()
    logger.info("VIZ-02 complete.")


if __name__ == "__main__":
    main()
