"""
INFRA-05: Data Validation Report
GitHub Developer Ecosystem Analytics
Author: Hariharan L (hl5865)

What this does:
  Reads the Parquet table produced by INFRA-04 and generates a validation
  report covering:
    1. Row counts per event type per month
    2. Null rates on critical columns
    3. Partition sizes on disk
    4. Timestamp range sanity check
    5. Duplicate event_id check

  Share this report with the team after every full parse run.
  Tanusha and Shreyas should review before running their scripts.

Usage:
  spark-submit --master spark://spark-master:7077 \\
    /opt/spark-apps/batch/infra05_validation_report.py
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    from schema import EVENTS_PATH
except ImportError:
    from batch.schema import EVENTS_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("infra05.validation")

SEPARATOR = "=" * 65


def main():
    spark = SparkSession.builder \
        .appName("INFRA-05: Data Validation Report") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(SEPARATOR)
    print("INFRA-05: Data Validation Report")
    print(f"Reading from: {EVENTS_PATH}")
    print(SEPARATOR)

    df = spark.read.parquet(EVENTS_PATH)
    df.cache()

    # ------------------------------------------------------------------
    # 1. Total row count
    # ------------------------------------------------------------------
    total = df.count()
    print(f"\n[1] Total rows: {total:,}")

    # ------------------------------------------------------------------
    # 2. Row counts per event_type
    # ------------------------------------------------------------------
    print(f"\n[2] Row counts by event_type:")
    df.groupBy("event_type") \
      .count() \
      .withColumn("pct", F.round(F.col("count") / total * 100, 2)) \
      .orderBy(F.col("count").desc()) \
      .show(20)

    # ------------------------------------------------------------------
    # 3. Row counts per year/month
    # ------------------------------------------------------------------
    print(f"\n[3] Row counts by year/month:")
    df.groupBy("year", "month") \
      .count() \
      .orderBy("year", "month") \
      .show(50)

    # ------------------------------------------------------------------
    # 4. Null rates on critical columns
    # A high null rate on repo_id or created_at means the parser broke.
    # NULL language is expected — it's enriched separately.
    # ------------------------------------------------------------------
    print(f"\n[4] Null rates on key columns:")

    critical_cols = ["event_id", "event_type", "repo_id", "repo_name",
                     "actor_id", "created_at", "language"]

    null_data = []
    for c in critical_cols:
        null_count = df.filter(F.col(c).isNull()).count()
        null_pct = round(null_count / total * 100, 3) if total > 0 else 0
        null_data.append((c, null_count, null_pct))

    print(f"  {'Column':<20} {'Null Count':>12} {'Null %':>8}")
    print(f"  {'-'*42}")
    for col, count, pct in null_data:
        flag = " ← EXPECTED" if col == "language" else (" ← WARNING" if pct > 1 else "")
        print(f"  {col:<20} {count:>12,} {pct:>7.3f}%{flag}")

    # ------------------------------------------------------------------
    # 5. Duplicate event_id check
    # Duplicates mean the same event was parsed twice — bad for ML labels.
    # Should be 0. If not, check for overlapping input files.
    # ------------------------------------------------------------------
    print(f"\n[5] Duplicate event_id check:")
    dup_count = df.groupBy("event_id") \
                  .count() \
                  .filter(F.col("count") > 1) \
                  .count()

    if dup_count == 0:
        print("  PASSED — no duplicate event_ids found")
    else:
        print(f"  WARNING: {dup_count:,} duplicate event_ids found")
        print("  Check for overlapping input files in HDFS raw folder.")

    # ------------------------------------------------------------------
    # 6. Timestamp range — sanity check
    # ------------------------------------------------------------------
    print(f"\n[6] Timestamp range:")
    ts = df.agg(
        F.min("created_at").alias("earliest"),
        F.max("created_at").alias("latest")
    ).collect()[0]
    print(f"  Earliest event : {ts['earliest']}")
    print(f"  Latest event   : {ts['latest']}")

    # ------------------------------------------------------------------
    # 7. Sample rows per event type
    # ------------------------------------------------------------------
    print(f"\n[7] Sample rows per event type:")
    for event_type in ["PushEvent", "WatchEvent", "ForkEvent",
                       "IssuesEvent", "PullRequestEvent", "CreateEvent"]:
        print(f"\n  --- {event_type} ---")
        df.filter(F.col("event_type") == event_type) \
          .select("event_id", "repo_name", "actor_login",
                  "created_at", "ref", "action", "push_size") \
          .limit(3) \
          .show(truncate=False)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(SEPARATOR)
    print("VALIDATION SUMMARY")
    print(SEPARATOR)
    print(f"  Total rows        : {total:,}")
    print(f"  Duplicate IDs     : {dup_count:,}  {'OK' if dup_count == 0 else 'INVESTIGATE'}")
    print(f"  Date range        : {ts['earliest']} → {ts['latest']}")
    print(f"  Output path       : {EVENTS_PATH}")
    print(SEPARATOR)

    spark.stop()


if __name__ == "__main__":
    main()
