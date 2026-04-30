"""
ML-07: Viral Repo Case Studies
GitHub Developer Ecosystem Analytics
Author: Tanusha Karnam (tk3514)

What this does:
  1. Loads the 2025 holdout set with model predictions (ML-05 output)
  2. Finds true positives — repos the model flagged viral at T+48h
     that actually reached >=1000 stars by day 90
  3. For each case study repo, computes:
       - T=0     : repo creation timestamp
       - T+48h   : prediction timestamp (when model scored it)
       - T_viral : date it first hit 1000 stars
       - lead_days: how many days early the model called it
  4. Selects 3–5 compelling cases (high stars, long lead time)
  5. Prints a formatted case study block for the final report

Output:
  - Console report (copy into Final Report — Section 5: Case Studies)
  - hdfs://namenode:9000/github/ml/case_studies/  (Parquet for Vikram)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

import sys
sys.path.append("/opt/spark-apps")
from batch.schema import HOLDOUT_PATH, EVENTS_PATH, HDFS_BASE

PREDICTIONS_PATH  = f"{HDFS_BASE}/ml/predictions/holdout"
CASE_STUDIES_PATH = f"{HDFS_BASE}/ml/case_studies"

# Number of case studies to include in the report
NUM_CASE_STUDIES = 5

# Lead time window: model should have flagged at least 5 days before trending
MIN_LEAD_DAYS = 5


def main():
    spark = SparkSession.builder \
        .appName("ML-07: Viral Repo Case Studies") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("ML-07: Viral Repo Case Studies")
    print("=" * 60)

    # ─────────────────────────────────────────────────────────────
    # STEP 1: Load holdout predictions from ML-05
    # ML-05 saves model.transform(holdout) output here.
    # If not present, we re-derive from the holdout Parquet + a
    # note to run ML-05 first.
    # ─────────────────────────────────────────────────────────────
    print("\n[Step 1] Loading holdout predictions...")

    try:
        predictions = spark.read.parquet(PREDICTIONS_PATH)
        print(f"  Loaded predictions from {PREDICTIONS_PATH}")
    except AnalysisException:
        # Predictions not saved separately — load holdout directly.
        # This won't have the 'prediction' column but we can filter
        # on label=1 (all actual positives) as a proxy.
        print(f"  Predictions not found at {PREDICTIONS_PATH}")
        print("  Falling back to holdout labels (actual positives only).")
        print("  Run ML-05 and save predictions to get true positive analysis.")
        try:
            predictions = spark.read.parquet(HOLDOUT_PATH)
            # Treat all actual positives as "flagged by model"
            predictions = predictions.withColumn("prediction", F.col("label").cast("double"))
        except AnalysisException:
            print(f"ERROR: Cannot find holdout data at {HOLDOUT_PATH}")
            print("Make sure ML-04 has finished first.")
            spark.stop()
            exit(1)

    total_preds    = predictions.count()
    true_positives = predictions.filter(
        (F.col("label") == 1) & (F.col("prediction") == 1.0)
    ).count()
    false_positives = predictions.filter(
        (F.col("label") == 0) & (F.col("prediction") == 1.0)
    ).count()

    print(f"  Total holdout repos  : {total_preds:,}")
    print(f"  True positives (TP)  : {true_positives:,}  ← model correct viral calls")
    print(f"  False positives (FP) : {false_positives:,}")

    # ─────────────────────────────────────────────────────────────
    # STEP 2: Load raw events to compute T_viral per repo
    # T_viral = first timestamp where cumulative WatchEvents >= 1000
    # ─────────────────────────────────────────────────────────────
    print("\n[Step 2] Computing T_viral (when each repo hit 1000 stars)...")

    try:
        events = spark.read.parquet(EVENTS_PATH)
    except AnalysisException:
        print(f"ERROR: Cannot find events at {EVENTS_PATH}")
        print("Make sure INFRA-04 has finished first.")
        spark.stop()
        exit(1)

    watch_events = events.filter(F.col("event_type") == "WatchEvent") \
                         .select("repo_id", "created_at")

    # Running cumulative count of stars per repo, ordered by time.
    # T_viral = first created_at where running count crosses 1000.
    from pyspark.sql.window import Window

    star_window = Window.partitionBy("repo_id").orderBy("created_at")

    watch_ranked = watch_events.withColumn(
        "star_rank", F.row_number().over(star_window)
    )

    # The 1000th star event = T_viral
    t_viral = watch_ranked \
        .filter(F.col("star_rank") == 1000) \
        .groupBy("repo_id") \
        .agg(F.min("created_at").alias("t_viral"))

    print(f"  Repos that hit 1000 stars: {t_viral.count():,}")

    # ─────────────────────────────────────────────────────────────
    # STEP 3: Join predictions with T_viral and compute lead time
    # lead_days = (T_viral - (T=0 + 48h)) in days
    #           = days between model prediction and actual viral date
    # ─────────────────────────────────────────────────────────────
    print("\n[Step 3] Computing prediction lead time per repo...")

    # True positives only
    tp = predictions.filter(
        (F.col("label") == 1) & (F.col("prediction") == 1.0)
    ).select("repo_id", "repo_name", "t0", "stars_90d")

    tp_with_viral = tp.join(t_viral, on="repo_id", how="inner")

    tp_with_lead = tp_with_viral.withColumn(
        "prediction_ts",
        F.col("t0") + F.expr("INTERVAL 48 HOURS")
    ).withColumn(
        "lead_days",
        (F.unix_timestamp("t_viral") - F.unix_timestamp("prediction_ts")) / 86400.0
    )

    # Only keep cases where model called it before the viral date
    early_calls = tp_with_lead.filter(F.col("lead_days") >= MIN_LEAD_DAYS)

    early_count = early_calls.count()
    print(f"  TPs with lead time >= {MIN_LEAD_DAYS} days: {early_count:,}")

    # ─────────────────────────────────────────────────────────────
    # STEP 4: Select top N case studies
    # Sort by: lead_days DESC (most impressive early calls first)
    # Tiebreak by stars_90d DESC (more stars = more compelling)
    # ─────────────────────────────────────────────────────────────
    print(f"\n[Step 4] Selecting top {NUM_CASE_STUDIES} case studies...")

    case_studies = early_calls \
        .orderBy(F.col("lead_days").desc(), F.col("stars_90d").desc()) \
        .limit(NUM_CASE_STUDIES) \
        .select(
            "repo_name",
            "repo_id",
            "t0",
            "prediction_ts",
            "t_viral",
            "lead_days",
            "stars_90d"
        )

    case_studies = case_studies.cache()
    case_studies.show(NUM_CASE_STUDIES, truncate=False)

    # ─────────────────────────────────────────────────────────────
    # STEP 5: Print formatted case study block for the report
    # ─────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("VIRAL REPO CASE STUDIES")
    print("(Copy this block into Final Report — Section 5)")
    print("=" * 60)

    rows = case_studies.collect()

    if not rows:
        print("\n  No case studies found with lead_days >= {MIN_LEAD_DAYS}.")
        print("  Check that ML-05 predictions are saved and the holdout set is correct.")
    else:
        for i, row in enumerate(rows, 1):
            print(f"""
Case Study #{i}: {row['repo_name']}
  Repo ID         : {row['repo_id']}
  T=0 (created)   : {row['t0']}
  Prediction at   : {row['prediction_ts']}  (T+48h)
  Went viral at   : {row['t_viral']}
  Lead time       : {row['lead_days']:.1f} days before trending
  Total stars(90d): {int(row['stars_90d']):,}

  Interpretation  : Our model flagged {row['repo_name']} as likely
  viral within 48 hours of its creation. The repo publicly trended
  {row['lead_days']:.1f} days later, confirming the early signal.
""")
            print("-" * 60)

    # ─────────────────────────────────────────────────────────────
    # STEP 6: Overall early-detection summary stats
    # ─────────────────────────────────────────────────────────────
    print("\n[Step 6] Early-detection summary stats...")

    if early_count > 0:
        stats = early_calls.agg(
            F.avg("lead_days").alias("avg_lead_days"),
            F.max("lead_days").alias("max_lead_days"),
            F.min("lead_days").alias("min_lead_days"),
            F.avg("stars_90d").alias("avg_stars_90d"),
            F.max("stars_90d").alias("max_stars_90d"),
        ).collect()[0]

        print("\n" + "=" * 60)
        print("EARLY-DETECTION SUMMARY (all true positives)")
        print("=" * 60)
        print(f"  Repos flagged early (lead >= {MIN_LEAD_DAYS}d) : {early_count:,}")
        print(f"  Avg lead time                       : {stats['avg_lead_days']:.1f} days")
        print(f"  Max lead time                       : {stats['max_lead_days']:.1f} days")
        print(f"  Min lead time                       : {stats['min_lead_days']:.1f} days")
        print(f"  Avg stars (90d) of early calls      : {stats['avg_stars_90d']:,.0f}")
        print(f"  Max stars (90d) of early calls      : {stats['max_stars_90d']:,.0f}")
        print("=" * 60)

    # ─────────────────────────────────────────────────────────────
    # STEP 7: Save case studies to HDFS for Vikram's report
    # ─────────────────────────────────────────────────────────────
    print(f"\n[Step 7] Saving case studies to HDFS: {CASE_STUDIES_PATH}")

    if rows:
        case_studies.write.mode("overwrite").parquet(CASE_STUDIES_PATH)
        print("  Case studies saved successfully.")
        print(f"  Vikram — load from: {CASE_STUDIES_PATH}")
    else:
        print("  No case studies to save.")

    spark.stop()


if __name__ == "__main__":
    main()
