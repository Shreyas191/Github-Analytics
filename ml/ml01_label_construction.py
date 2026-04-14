"""
ML-01: Label Construction Job
GitHub Developer Ecosystem Analytics
Author: Tanusha Karnam (tk3514)

What this does:
  1. Reads the unified events Parquet table (produced by Hariharan - INFRA-04)
  2. Finds T=0 for every repo (timestamp of first CreateEvent, or earliest event)
  3. Counts WatchEvents (stars) between T+3 days and T+90 days
  4. Labels repo as 1 (viral) if stars >= 1000, else 0
  5. Saves the labeled table to HDFS

Output schema:
  repo_id       (long)    - unique repo identifier
  repo_name     (string)  - full repo name e.g. "torvalds/linux"
  t0            (timestamp) - repo creation time
  stars_90d     (long)    - stars received between day 3 and day 90
  label         (int)     - 1 if viral (>=1000 stars), 0 otherwise
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.utils import AnalysisException

#  ------------------------------------------------------------------------------------------------
# CONFIG — update paths if needed
#  ------------------------------------------------------------------------------------------------
EVENTS_PARQUET_PATH = "hdfs://namenode:9000/github/events/parquet/"
OUTPUT_PATH         = "hdfs://namenode:9000/github/ml/labels/"
STAR_THRESHOLD      = 1000
MIN_DAYS_AFTER_T0   = 3
MAX_DAYS_AFTER_T0   = 90


def main():
    spark = SparkSession.builder \
        .appName("ML-01: Label Construction") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("ML-01: Label Construction Job")
    print("=" * 60)

    print("\n[Step 1] Loading events Parquet from HDFS...")
    try:
        events = spark.read.parquet(EVENTS_PARQUET_PATH)
    except AnalysisException:
        print(f"ERROR: Cannot find events data at {EVENTS_PARQUET_PATH}")
        print("Make sure Hariharan's INFRA-04 job has finished first.")
        spark.stop()
        exit(1)
    print(f"  Total events loaded: {events.count():,}")
    events.printSchema()

    print("\n[Step 2] Computing T=0 for every repo...")
    create_events = events \
        .filter(F.col("event_type") == "CreateEvent") \
        .groupBy("repo_id", "repo_name") \
        .agg(F.min("created_at").alias("t0_create"))

    earliest_events = events \
        .groupBy("repo_id", "repo_name") \
        .agg(F.min("created_at").alias("t0_fallback"))

    t0_table = earliest_events \
        .join(create_events, on=["repo_id", "repo_name"], how="left") \
        .withColumn(
            "t0",
            F.when(F.col("t0_create").isNotNull(), F.col("t0_create"))
             .otherwise(F.col("t0_fallback"))
        ) \
        .select("repo_id", "repo_name", "t0")
    t0_table = t0_table.cache()

    print(f"  Total unique repos found: {t0_table.count():,}")

    print(f"\n[Step 3] Counting stars between day {MIN_DAYS_AFTER_T0} and day {MAX_DAYS_AFTER_T0}...")
    watch_events = events \
        .filter(F.col("event_type") == "WatchEvent") \
        .select("repo_id", "created_at")

    stars_with_t0 = watch_events \
        .join(t0_table.select("repo_id", "t0"), on="repo_id", how="inner")

    stars_with_t0 = stars_with_t0.withColumn(
        "days_since_t0",
        (F.unix_timestamp("created_at") - F.unix_timestamp("t0")) / 86400.0
    )

    stars_in_window = stars_with_t0.filter(
        (F.col("days_since_t0") >= MIN_DAYS_AFTER_T0) &
        (F.col("days_since_t0") <= MAX_DAYS_AFTER_T0)
    )

    star_counts = stars_in_window \
        .groupBy("repo_id") \
        .agg(F.count("*").alias("stars_90d"))

    print("\n[Step 4] Joining star counts to repo table...")
    labels = t0_table \
        .join(star_counts, on="repo_id", how="left") \
        .fillna(0, subset=["stars_90d"])

    print(f"\n[Step 5] Assigning labels (threshold = {STAR_THRESHOLD} stars)...")
    labels = labels.withColumn(
        "label",
        F.when(F.col("stars_90d") >= STAR_THRESHOLD, 1)
         .otherwise(0)
         .cast(IntegerType())
    )

    total        = labels.count()
    viral_count  = labels.filter(F.col("label") == 1).count()
    normal_count = total - viral_count
    viral_pct    = (viral_count / total) * 100 if total > 0 else 0

    print("\n" + "=" * 60)
    print("LABEL SUMMARY")
    print("=" * 60)
    print(f"  Total repos labeled  : {total:,}")
    print(f"  Viral repos (label=1): {viral_count:,}  ({viral_pct:.3f}%)")
    print(f"  Normal repos(label=0): {normal_count:,}  ({100-viral_pct:.3f}%)")
    print(f"  Class imbalance ratio: 1 : {int(normal_count/viral_count) if viral_count > 0 else 'N/A'}")
    print("=" * 60)

    print("\nSample viral repos (label=1):")
    labels.filter(F.col("label") == 1) \
          .orderBy(F.col("stars_90d").desc()) \
          .select("repo_name", "stars_90d", "t0", "label") \
          .show(10, truncate=False)

    print(f"\n[Step 7] Saving labels to HDFS: {OUTPUT_PATH}")
    labels \
        .select("repo_id", "repo_name", "t0", "stars_90d", "label") \
        .write \
        .mode("overwrite") \
        .partitionBy("label") \
        .parquet(OUTPUT_PATH)

    print("  Done! Labels saved successfully.")
    spark.stop()


if __name__ == "__main__":
    main()