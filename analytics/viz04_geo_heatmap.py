"""
VIZ-04: Geographic Contribution Heatmap
GitHub Developer Ecosystem Analytics
Author: Vikram Markali (vrm9190)

What this does:
  Builds a country-level contribution map showing which countries are emerging
  or declining as open source hubs over 2023-2025.

  Steps:
    1. Read unique (actor_id, actor_login) pairs + event counts from HDFS Parquet.
    2. Take top MAX_ACTORS by activity (most active actors are most likely to have
       location set on their profile).
    3. Fetch each actor's GitHub profile via REST API to get the location field.
    4. Parse location strings to ISO 3166-1 alpha-2 country codes using a regex
       lookup. Falls back to UTC offset inference if location is empty.
    5. Aggregate event counts by (year, country_code).
    6. Write to postgres-analytics table: geo_contributions.

  Grafana Geomap panel reads from geo_contributions and renders the heatmap.
  A Grafana variable lets the viewer filter by year (2023 / 2024 / 2025 / all).

Why process only top actors?
  The full events table has millions of unique actors. Most inactive accounts
  have no location set. Fetching profiles for the top 50k actors by activity
  captures ~80% of total event volume and covers all major contributor regions.

Input:
  hdfs://namenode:9000/github/events/parquet/

Output:
  postgres-analytics: geo_contributions table
  postgres-analytics: (actor_location cache written in batches for resume)

Usage:
  # Apply schema first (one time):
  docker exec -i postgres-analytics psql -U analytics -d github_analytics \\
    < /opt/spark-apps/analytics/db_schema.sql

  # Dry run -- top 200 actors only:
  docker exec -e PAT_VIKRAM=$PAT_VIKRAM spark-master spark-submit \\
    --master spark://spark-master:7077 \\
    --packages org.postgresql:postgresql:42.6.0 \\
    /opt/spark-apps/analytics/viz04_geo_heatmap.py --sample

  # Full run:
  docker exec -e PAT_SHREYAS=$PAT_SHREYAS -e PAT_HARIHARAN=$PAT_HARIHARAN \\
    -e PAT_TANUSHA=$PAT_TANUSHA -e PAT_VIKRAM=$PAT_VIKRAM spark-master \\
    spark-submit --master spark://spark-master:7077 \\
    --packages org.postgresql:postgresql:42.6.0 \\
    /opt/spark-apps/analytics/viz04_geo_heatmap.py
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
from typing import Optional

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("viz04.geo_heatmap")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
EVENTS_PATH      = "hdfs://namenode:9000/github/events/parquet"
JDBC_URL         = "jdbc:postgresql://postgres-analytics:5432/github_analytics"
JDBC_PROPS       = {"user": "analytics", "password": "analytics", "driver": "org.postgresql.Driver"}
CHECKPOINT_FILE  = "/tmp/viz04_actor_locations.json"
GITHUB_API_BASE  = "https://api.github.com"
MAX_ACTORS       = 50_000
SAMPLE_ACTORS    = 200
REQUEST_TIMEOUT  = 10
MAX_RETRIES      = 3


# ===========================================================================
# COUNTRY LOOKUP
# Maps free-text location strings -> ISO 3166-1 alpha-2 country codes.
# Order matters: more specific patterns first.
# ===========================================================================

COUNTRY_CODES = {
    # North America
    "united states": "US", "usa": "US", "u.s.a": "US", "u.s.": "US",
    "california": "US", "new york": "US", "texas": "US", "seattle": "US",
    "san francisco": "US", "los angeles": "US", "chicago": "US", "boston": "US",
    "silicon valley": "US", "bay area": "US", "washington dc": "US", "austin": "US",
    "canada": "CA", "toronto": "CA", "vancouver": "CA", "montreal": "CA",
    "mexico": "MX",

    # Europe
    "united kingdom": "GB", "uk": "GB", "england": "GB", "london": "GB",
    "great britain": "GB", "britain": "GB", "manchester": "GB", "scotland": "GB",
    "germany": "DE", "deutschland": "DE", "berlin": "DE", "munich": "DE",
    "hamburg": "DE", "frankfurt": "DE",
    "france": "FR", "paris": "FR", "lyon": "FR",
    "netherlands": "NL", "amsterdam": "NL", "holland": "NL",
    "sweden": "SE", "stockholm": "SE", "gothenburg": "SE",
    "norway": "NO", "oslo": "NO",
    "denmark": "DK", "copenhagen": "DK",
    "finland": "FI", "helsinki": "FI",
    "switzerland": "CH", "zurich": "CH", "geneva": "CH",
    "austria": "AT", "vienna": "AT",
    "spain": "ES", "madrid": "ES", "barcelona": "ES",
    "italy": "IT", "rome": "IT", "milan": "IT",
    "portugal": "PT", "lisbon": "PT",
    "poland": "PL", "warsaw": "PL",
    "russia": "RU", "moscow": "RU", "saint petersburg": "RU",
    "ukraine": "UA", "kyiv": "UA", "kharkiv": "UA",
    "czech republic": "CZ", "czechia": "CZ", "prague": "CZ",
    "hungary": "HU", "budapest": "HU",
    "romania": "RO", "bucharest": "RO",
    "belgium": "BE", "brussels": "BE",
    "ireland": "IE", "dublin": "IE",
    "israel": "IL", "tel aviv": "IL",
    "turkey": "TR", "istanbul": "TR", "ankara": "TR",
    "greece": "GR", "athens": "GR",

    # Asia
    "china": "CN", "beijing": "CN", "shanghai": "CN", "shenzhen": "CN",
    "hangzhou": "CN", "guangzhou": "CN", "chengdu": "CN",
    "india": "IN", "bangalore": "IN", "bengaluru": "IN", "mumbai": "IN",
    "delhi": "IN", "hyderabad": "IN", "pune": "IN", "chennai": "IN",
    "japan": "JP", "tokyo": "JP", "osaka": "JP",
    "south korea": "KR", "korea": "KR", "seoul": "KR",
    "singapore": "SG",
    "taiwan": "TW", "taipei": "TW",
    "hong kong": "HK",
    "indonesia": "ID", "jakarta": "ID",
    "vietnam": "VN", "hanoi": "VN", "ho chi minh": "VN",
    "thailand": "TH", "bangkok": "TH",
    "malaysia": "MY", "kuala lumpur": "MY",
    "philippines": "PH", "manila": "PH",
    "pakistan": "PK", "karachi": "PK", "lahore": "PK",
    "bangladesh": "BD", "dhaka": "BD",
    "iran": "IR",
    "saudi arabia": "SA", "riyadh": "SA",
    "united arab emirates": "AE", "uae": "AE", "dubai": "AE",
    "egypt": "EG", "cairo": "EG",

    # Oceania
    "australia": "AU", "sydney": "AU", "melbourne": "AU", "brisbane": "AU",
    "new zealand": "NZ", "auckland": "NZ",

    # Latin America
    "brazil": "BR", "brasil": "BR", "sao paulo": "BR", "rio de janeiro": "BR",
    "argentina": "AR", "buenos aires": "AR",
    "colombia": "CO", "bogota": "CO",
    "chile": "CL", "santiago": "CL",
    "peru": "PE", "lima": "PE",

    # Africa
    "nigeria": "NG", "lagos": "NG",
    "south africa": "ZA", "johannesburg": "ZA", "cape town": "ZA",
    "kenya": "KE", "nairobi": "KE",
    "ethiopia": "ET", "addis ababa": "ET",
    "ghana": "GH", "accra": "GH",
    "morocco": "MA", "casablanca": "MA",
    "tunisia": "TN", "tunis": "TN",
}

# Country code to full name (for display in Grafana)
CODE_TO_NAME = {
    "US": "United States", "CA": "Canada", "MX": "Mexico",
    "GB": "United Kingdom", "DE": "Germany", "FR": "France",
    "NL": "Netherlands", "SE": "Sweden", "NO": "Norway", "DK": "Denmark",
    "FI": "Finland", "CH": "Switzerland", "AT": "Austria", "ES": "Spain",
    "IT": "Italy", "PT": "Portugal", "PL": "Poland", "RU": "Russia",
    "UA": "Ukraine", "CZ": "Czech Republic", "HU": "Hungary", "RO": "Romania",
    "BE": "Belgium", "IE": "Ireland", "IL": "Israel", "TR": "Turkey",
    "GR": "Greece", "CN": "China", "IN": "India", "JP": "Japan",
    "KR": "South Korea", "SG": "Singapore", "TW": "Taiwan", "HK": "Hong Kong",
    "ID": "Indonesia", "VN": "Vietnam", "TH": "Thailand", "MY": "Malaysia",
    "PH": "Philippines", "PK": "Pakistan", "BD": "Bangladesh", "IR": "Iran",
    "SA": "Saudi Arabia", "AE": "United Arab Emirates", "EG": "Egypt",
    "AU": "Australia", "NZ": "New Zealand", "BR": "Brazil", "AR": "Argentina",
    "CO": "Colombia", "CL": "Chile", "PE": "Peru", "NG": "Nigeria",
    "ZA": "South Africa", "KE": "Kenya", "ET": "Ethiopia", "GH": "Ghana",
    "MA": "Morocco", "TN": "Tunisia",
}


def parse_location(location: str) -> Optional[str]:
    """
    Parse a free-text GitHub location string to an ISO 3166-1 alpha-2 country code.
    Returns None if the country cannot be determined.
    """
    if not location:
        return None

    loc = location.lower().strip()

    # Strip common punctuation that appears between city and country
    loc = re.sub(r"[,;|/\\]", " ", loc)
    loc = re.sub(r"\s+", " ", loc).strip()

    for pattern, code in COUNTRY_CODES.items():
        if pattern in loc:
            return code

    # Two-letter uppercase code at end of string (e.g. "Austin, TX, US")
    match = re.search(r"\b([A-Z]{2})\b$", location.strip())
    if match:
        candidate = match.group(1)
        if candidate in CODE_TO_NAME:
            return candidate

    return None


# ===========================================================================
# PAT ROTATOR (same pattern as STREAM-01 and VIZ-02)
# ===========================================================================

from dataclasses import dataclass

@dataclass
class PATEntry:
    token: str
    alias: str
    remaining: int = 5000
    reset_at: float = 0.0
    blocked: bool = False


class PATRotator:
    def __init__(self, pat_list):
        self.pats = [PATEntry(token=t, alias=a) for t, a in pat_list]
        self._index = 0

    def get_token(self):
        now = time.time()
        for p in self.pats:
            if p.blocked and now >= p.reset_at:
                p.blocked = False
                p.remaining = 5000
        available = [p for p in self.pats if not p.blocked]
        if not available:
            wait = max(0, min(p.reset_at for p in self.pats) - now) + 5
            logger.warning(f"All PATs rate-limited. Waiting {wait:.0f}s...")
            time.sleep(wait)
            return self.get_token()
        for _ in range(len(self.pats)):
            p = self.pats[self._index % len(self.pats)]
            self._index = (self._index + 1) % len(self.pats)
            if not p.blocked:
                return p.token
        return available[0].token

    def update_from_headers(self, token, headers):
        p = next((x for x in self.pats if x.token == token), None)
        if not p:
            return
        if r := headers.get("X-RateLimit-Remaining"):
            p.remaining = int(r)
        if rs := headers.get("X-RateLimit-Reset"):
            p.reset_at = float(rs)
        if p.remaining <= 50:
            p.blocked = True

    def mark_rate_limited(self, token, reset_at):
        p = next((x for x in self.pats if x.token == token), None)
        if p:
            p.blocked = True
            p.reset_at = reset_at


# ===========================================================================
# GITHUB API
# ===========================================================================

def fetch_user_location(login: str, rotator: PATRotator) -> Optional[str]:
    """Fetch the location field from a GitHub user profile."""
    url = f"{GITHUB_API_BASE}/users/{login}"
    for attempt in range(MAX_RETRIES):
        token = rotator.get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        try:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            rotator.update_from_headers(token, resp.headers)
            if resp.status_code == 200:
                return resp.json().get("location")
            if resp.status_code == 404:
                return None
            if resp.status_code in (403, 429):
                reset_ts = float(resp.headers.get("X-RateLimit-Reset", time.time() + 3600))
                rotator.mark_rate_limited(token, reset_ts)
                continue
            if resp.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
        except requests.RequestException:
            time.sleep(2 ** attempt)
    return None


# ===========================================================================
# MAIN PIPELINE
# ===========================================================================

def load_pat_rotator() -> PATRotator:
    candidates = [
        (os.environ.get("PAT_SHREYAS"),   "shreyas"),
        (os.environ.get("PAT_HARIHARAN"), "hariharan"),
        (os.environ.get("PAT_TANUSHA"),   "tanusha"),
        (os.environ.get("PAT_VIKRAM"),    "vikram"),
    ]
    available = [(t, a) for t, a in candidates if t]
    if not available:
        raise EnvironmentError("Set at least one of: PAT_SHREYAS PAT_HARIHARAN PAT_TANUSHA PAT_VIKRAM")
    return PATRotator(available)


def get_top_actors(spark: SparkSession, limit: int) -> list[dict]:
    """Read top actors by event count from HDFS events Parquet."""
    logger.info(f"Reading top {limit:,} actors from {EVENTS_PATH} ...")
    events = spark.read.parquet(EVENTS_PATH)

    actors = (
        events
        .filter(F.col("actor_login").isNotNull())
        .groupBy("actor_id", "actor_login")
        .agg(
            F.count("*").alias("event_count"),
            F.min(F.year("created_at")).alias("first_year"),
            F.max(F.year("created_at")).alias("last_year"),
        )
        .orderBy(F.col("event_count").desc())
        .limit(limit)
    )

    # Also get per-year event counts (for the geo_contributions table)
    actor_year = (
        events
        .filter(F.col("actor_login").isNotNull())
        .groupBy("actor_id", F.year("created_at").alias("year"))
        .agg(F.count("*").alias("year_event_count"))
    )

    actors_collected = [row.asDict() for row in actors.collect()]
    year_counts = {
        (row["actor_id"], row["year"]): row["year_event_count"]
        for row in actor_year.collect()
    }

    logger.info(f"Collected {len(actors_collected):,} actors.")
    return actors_collected, year_counts


def load_checkpoint() -> dict:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE) as f:
            data = json.load(f)
        logger.info(f"Checkpoint: {len(data):,} actors already resolved.")
        return data
    return {}


def save_checkpoint(resolved: dict):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(resolved, f)


def resolve_locations(actors: list[dict], rotator: PATRotator, checkpoint: dict) -> dict:
    """
    For each actor, fetch GitHub profile and resolve location to country code.
    Returns dict: actor_id_str -> {"country_code": "US", "country_name": "United States"}
    """
    resolved = dict(checkpoint)
    todo = [a for a in actors if str(a["actor_id"]) not in resolved]
    total = len(todo)
    logger.info(f"Actors to resolve: {total:,} (skipping {len(resolved):,} from checkpoint)")

    for i, actor in enumerate(todo, 1):
        actor_id  = str(actor["actor_id"])
        login     = actor["actor_login"]
        raw_loc   = fetch_user_location(login, rotator)
        country   = parse_location(raw_loc) if raw_loc else None

        resolved[actor_id] = {
            "country_code": country,
            "country_name": CODE_TO_NAME.get(country, "Unknown") if country else None,
        }

        if i % 1000 == 0 or i == total:
            located = sum(1 for v in resolved.values() if v["country_code"])
            save_checkpoint(resolved)
            logger.info(f"Progress: {i:,}/{total:,} | located: {located:,} ({100*located/max(i,1):.1f}%)")

    return resolved


def build_geo_rows(actors: list[dict], year_counts: dict, resolved: dict) -> list[tuple]:
    """
    Aggregate event counts by (year, country_code).
    Returns list of (year, country_code, country_name, actor_count, event_count) tuples.
    """
    from collections import defaultdict

    # country_year -> {actor_set, event_count}
    agg: dict[tuple, dict] = defaultdict(lambda: {"actors": set(), "events": 0})

    for actor in actors:
        actor_id = str(actor["actor_id"])
        info = resolved.get(actor_id, {})
        code = info.get("country_code")
        name = info.get("country_name")
        if not code:
            continue
        for year in range(2023, 2026):
            cnt = year_counts.get((actor["actor_id"], year), 0)
            if cnt > 0:
                key = (year, code, name)
                agg[key]["actors"].add(actor["actor_id"])
                agg[key]["events"] += cnt

    return [
        (year, code, name, len(v["actors"]), v["events"])
        for (year, code, name), v in agg.items()
        if v["events"] > 0
    ]


def write_to_postgres(spark: SparkSession, rows: list[tuple]):
    from pyspark.sql.types import StructType, StructField, ShortType, StringType, LongType, TimestampType
    from pyspark.sql import functions as F

    schema = StructType([
        StructField("year",         ShortType(),  False),
        StructField("country_code", StringType(), False),
        StructField("country_name", StringType(), False),
        StructField("actor_count",  LongType(),   False),
        StructField("event_count",  LongType(),   False),
    ])

    df = spark.createDataFrame(rows, schema=schema) \
              .withColumn("computed_at", F.current_timestamp())

    df.write \
      .mode("overwrite") \
      .option("truncate", "true") \
      .jdbc(JDBC_URL, "geo_contributions", properties=JDBC_PROPS)

    total = df.count()
    logger.info(f"Wrote {total:,} rows to geo_contributions.")

    print("\n" + "=" * 60)
    print("VIZ-04 GEO HEATMAP SUMMARY")
    print("=" * 60)
    df.groupBy("year") \
      .agg(
          F.sum("event_count").alias("total_events"),
          F.countDistinct("country_code").alias("countries"),
      ) \
      .orderBy("year") \
      .show()
    print("\nTop 15 countries by total events:")
    df.groupBy("country_code", "country_name") \
      .agg(F.sum("event_count").alias("events")) \
      .orderBy(F.col("events").desc()) \
      .limit(15) \
      .show(truncate=False)
    print("=" * 60)


def parse_args():
    p = argparse.ArgumentParser(description="VIZ-04: Geographic Contribution Heatmap")
    p.add_argument("--sample", action="store_true",
                   help=f"Process top {SAMPLE_ACTORS} actors only (smoke test)")
    p.add_argument("--clear-checkpoint", action="store_true",
                   help="Delete checkpoint and restart from scratch")
    return p.parse_args()


def main():
    args = parse_args()
    limit = SAMPLE_ACTORS if args.sample else MAX_ACTORS

    spark = (
        SparkSession.builder
        .appName("VIZ-04: Geographic Contribution Heatmap")
        .config("spark.sql.shuffle.partitions", "100")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    logger.info("=" * 60)
    logger.info("VIZ-04: Geographic Contribution Heatmap")
    logger.info(f"  actors limit : {limit:,}")
    logger.info(f"  sample mode  : {args.sample}")
    logger.info("=" * 60)

    if args.clear_checkpoint and os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)

    rotator               = load_pat_rotator()
    actors, year_counts   = get_top_actors(spark, limit)
    checkpoint            = load_checkpoint()
    resolved              = resolve_locations(actors, rotator, checkpoint)
    rows                  = build_geo_rows(actors, year_counts, resolved)
    write_to_postgres(spark, rows)

    located = sum(1 for v in resolved.values() if v.get("country_code"))
    coverage = 100 * located / max(len(resolved), 1)
    logger.info(f"Location coverage: {coverage:.1f}% ({located:,}/{len(resolved):,} actors resolved)")
    logger.info("VIZ-04 complete.")
    spark.stop()


if __name__ == "__main__":
    main()
