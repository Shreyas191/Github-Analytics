"""
ML-03: Feature Engineering Pipeline
GitHub Developer Ecosystem Analytics
Author: Tanusha Karnam (tk3514)

What this does:
  Computes 8 features per repo, all strictly within T+48h window.
  Joins with Vikram's enrichment table (F5, F6) and language rank table.
  Output: one row per repo with 8 features + repo_id + t0, ready for ML-04.

Input paths:
  - Events Parquet       : Hariharan INFRA-04 output
  - Labels Parquet       : ML-01 output (for t0 timestamps)
  - Enrichment Parquet   : Vikram VIZ-02 output (F5 + F6)

Output schema:
  repo_id             (long)
  t0                  (timestamp)
  f1_stars_48h        (long)
  f2_forks_48h        (long)
  f3_contributors_48h (long)
  f4_issues_48h       (long)
  f5_owner_star_med   (double)
  f6_readme_bytes     (long)
  f7_lang_rank        (int)
  f8_pushes_48h       (long)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

# ------------------------------------------------------------------------------------------------
# CONFIG — update paths once team confirms
# ------------------------------------------------------------------------------------------------
import sys
sys.path.append("/opt/spark/jobs")
from batch.schema import EVENTS_PATH, LABELS_PATH, ENRICHMENT_PATH, FEATURES_PATH

OUTPUT_PATH = FEATURES_PATH
WINDOW_HOURS    = 48


def main():
    spark = SparkSession.builder \
        .appName("ML-03: Feature Engineering") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("ML-03: Feature Engineering Pipeline")
    print("=" * 60)

    # ------------------------------------------------------------------------------------------------
    # STEP 1: Load events + labels
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 1] Loading events and labels...")

    try:
        events = spark.read.parquet(EVENTS_PATH)
    except AnalysisException:
        print(f"ERROR: Cannot find events data at {EVENTS_PATH}")
        print("Make sure Hariharan's INFRA-04 job has finished first.")
        spark.stop()
        exit(1)

    try:
        labels = spark.read.parquet(LABELS_PATH) \
                      .select("repo_id", "repo_name", "t0")
    except AnalysisException:
        print(f"ERROR: Cannot find labels at {LABELS_PATH}")
        print("Make sure ML-01 has finished first.")
        spark.stop()
        exit(1)

    # Join events with t0 so we know each repo's creation time
    events = events.join(labels, on="repo_id", how="inner")

    # Compute hours since T=0 for every event
    events = events.withColumn(
        "hours_since_t0",
        (F.unix_timestamp("created_at") - F.unix_timestamp("t0")) / 3600.0
    )

    # STRICT 48h filter — most important line in the script
    events_48h = events.filter(
        (F.col("hours_since_t0") >= 0) &
        (F.col("hours_since_t0") <= WINDOW_HOURS)
    )

    events_48h = events_48h.cache()
    print(f"  Events within 48h window: {events_48h.count():,}")

    #  ------------------------------------------------------------------------------------------------
    # STEP 2: F1 — Stars in first 48h
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 2] Computing F1 — stars in 48h...")

    f1 = events_48h \
        .filter(F.col("event_type") == "WatchEvent") \
        .groupBy("repo_id") \
        .agg(F.count("*").alias("f1_stars_48h"))

    #  ------------------------------------------------------------------------------------------------
    # STEP 3: F2 — Forks in first 48h
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 3] Computing F2 — forks in 48h...")

    f2 = events_48h \
        .filter(F.col("event_type") == "ForkEvent") \
        .groupBy("repo_id") \
        .agg(F.count("*").alias("f2_forks_48h"))

    #  ------------------------------------------------------------------------------------------------
    # STEP 4: F3 — Unique contributors in first 48h
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 4] Computing F3 — unique contributors in 48h...")

    f3 = events_48h \
        .filter(F.col("event_type").isin("PushEvent", "PullRequestEvent")) \
        .groupBy("repo_id") \
        .agg(F.countDistinct("actor_id").alias("f3_contributors_48h"))

    #  ------------------------------------------------------------------------------------------------
    # STEP 5: F4 — Issues opened in first 48h
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 5] Computing F4 — issues in 48h...")

    f4 = events_48h \
        .filter(F.col("event_type") == "IssuesEvent") \
        .groupBy("repo_id") \
        .agg(F.count("*").alias("f4_issues_48h"))

    #  ------------------------------------------------------------------------------------------------
    # STEP 6: F8 — Push count in first 48h
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 6] Computing F8 — push count in 48h...")

    f8 = events_48h \
        .filter(F.col("event_type") == "PushEvent") \
        .groupBy("repo_id") \
        .agg(F.count("*").alias("f8_pushes_48h"))

    #  ------------------------------------------------------------------------------------------------
    # STEP 7: F7 — Language popularity rank
    # Uses full dataset — no leakage, this is a
    # property of the language not the repo
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 7] Computing F7 — language popularity rank...")

    lang_counts = events \
        .filter(F.col("language").isNotNull()) \
        .select("repo_id", "language") \
        .distinct() \
        .groupBy("language") \
        .agg(F.count("*").alias("lang_repo_count"))

    lang_rank_window = Window.orderBy(F.col("lang_repo_count").desc())
    lang_ranks = lang_counts.withColumn(
        "f7_lang_rank",
        F.rank().over(lang_rank_window).cast("int")
    ).select("language", "f7_lang_rank")

    repo_language = events \
        .filter(F.col("event_type") == "CreateEvent") \
        .select("repo_id", "language") \
        .dropDuplicates(["repo_id"])

    f7 = repo_language.join(lang_ranks, on="language", how="left") \
                      .select("repo_id", "f7_lang_rank")

    #  ------------------------------------------------------------------------------------------------
    # STEP 8: F5 + F6 — Vikram's enrichment table
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 8] Loading F5 + F6 from Vikram's enrichment table...")

    try:
        enrichment = spark.read.parquet(ENRICHMENT_PATH) \
                          .select("repo_id", "f5_owner_star_med", "f6_readme_bytes")
    except AnalysisException:
        print(f"WARNING: Enrichment table not found at {ENRICHMENT_PATH}")
        print("Running without F5 + F6 — will fill with 0 for now.")
        enrichment = labels.select("repo_id") \
                           .withColumn("f5_owner_star_med", F.lit(0.0)) \
                           .withColumn("f6_readme_bytes", F.lit(0))

    #  ------------------------------------------------------------------------------------------------
    # STEP 9: Assemble all 8 features
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 9] Assembling final feature table...")

    features = labels \
        .join(f1, on="repo_id", how="left") \
        .join(f2, on="repo_id", how="left") \
        .join(f3, on="repo_id", how="left") \
        .join(f4, on="repo_id", how="left") \
        .join(f8, on="repo_id", how="left") \
        .join(f7, on="repo_id", how="left") \
        .join(enrichment, on="repo_id", how="left") \
        .fillna(0, subset=[
            "f1_stars_48h", "f2_forks_48h", "f3_contributors_48h",
            "f4_issues_48h", "f8_pushes_48h", "f7_lang_rank",
            "f5_owner_star_med", "f6_readme_bytes"
        ])

    #  ------------------------------------------------------------------------------------------------
    # STEP 10: Sanity checks
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 10] Sanity checks...")

    total = features.count()
    nulls = features.filter(
        F.col("f1_stars_48h").isNull() |
        F.col("f5_owner_star_med").isNull()
    ).count()

    print(f"  Total repos in feature table : {total:,}")
    print(f"  Rows with any null features  : {nulls:,}")
    print(f"  (Should be 0 after fillna)")

    print("\nSample rows:")
    features.show(5, truncate=False)

    #  ------------------------------------------------------------------------------------------------
    # STEP 11: Save to HDFS
    #  ------------------------------------------------------------------------------------------------
    print(f"\n[Step 11] Saving features to HDFS: {OUTPUT_PATH}")

    features \
        .select(
            "repo_id", "repo_name", "t0",
            "f1_stars_48h", "f2_forks_48h", "f3_contributors_48h",
            "f4_issues_48h", "f5_owner_star_med", "f6_readme_bytes",
            "f7_lang_rank", "f8_pushes_48h"
        ) \
        .write \
        .mode("overwrite") \
        .parquet(OUTPUT_PATH)

    print("  Done! Features saved successfully.")
    print(f"\n  Output path : {OUTPUT_PATH}")
    print(f"  Output cols : repo_id, repo_name, t0, f1-f8")

    spark.stop()


if __name__ == "__main__":
    main()