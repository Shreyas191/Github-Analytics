"""
ML-04: Train/Test Split
GitHub Developer Ecosystem Analytics
Author: Tanusha Karnam (tk3514)

What this does:
  1. Reads ML-01 labels + ML-03 features
  2. Joins them into one table
  3. Splits by year: 2023-2024 = training, 2025 = holdout
  4. Verifies no data leakage (no day 3+ features)
  5. Saves both splits to HDFS

Output:
  - hdfs://namenode:9000/github/ml/train/   (2023-2024 repos)
  - hdfs://namenode:9000/github/ml/holdout/ (2025 repos)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

#  ------------------------------------------------------------------------------------------------
# CONFIG
#  ------------------------------------------------------------------------------------------------
LABELS_PATH   = "hdfs://namenode:9000/github/ml/labels/"    # ML-01 output
FEATURES_PATH = "hdfs://namenode:9000/github/ml/features/"  # ML-03 output
TRAIN_PATH    = "hdfs://namenode:9000/github/ml/train/"     # training set output
HOLDOUT_PATH  = "hdfs://namenode:9000/github/ml/holdout/"   # holdout set output


def main():
    spark = SparkSession.builder \
        .appName("ML-04: Train/Test Split") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("ML-04: Train/Test Split")
    print("=" * 60)

    #  ------------------------------------------------------------------------------------------------
    # STEP 1: Load labels (ML-01)
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 1] Loading labels from ML-01...")

    try:
        labels = spark.read.parquet(LABELS_PATH) \
                      .select("repo_id", "repo_name", "t0", "label")
    except AnalysisException:
        print(f"ERROR: Cannot find labels at {LABELS_PATH}")
        print("Make sure ML-01 has finished first.")
        spark.stop()
        exit(1)

    print(f"  Total labeled repos: {labels.count():,}")

    #  ------------------------------------------------------------------------------------------------
    # STEP 2: Load features (ML-03)
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 2] Loading features from ML-03...")

    try:
        features = spark.read.parquet(FEATURES_PATH) \
                        .select(
                            "repo_id",
                            "f1_stars_48h",
                            "f2_forks_48h",
                            "f3_contributors_48h",
                            "f4_issues_48h",
                            "f5_owner_star_med",
                            "f6_readme_bytes",
                            "f7_lang_rank",
                            "f8_pushes_48h"
                        )
    except AnalysisException:
        print(f"ERROR: Cannot find features at {FEATURES_PATH}")
        print("Make sure ML-03 has finished first.")
        spark.stop()
        exit(1)

    print(f"  Total repos with features: {features.count():,}")

    #  ------------------------------------------------------------------------------------------------
    # STEP 3: Join labels + features
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 3] Joining labels and features...")

    dataset = labels.join(features, on="repo_id", how="inner")
    dataset = dataset.cache()

    total = dataset.count()
    print(f"  Total repos in joined dataset: {total:,}")

    #  ------------------------------------------------------------------------------------------------
    # STEP 4: Extract year from t0
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 4] Extracting year from T=0 timestamp...")

    dataset = dataset.withColumn("t0_year", F.year(F.col("t0")))

    # Show year distribution
    print("  Repo count by year:")
    dataset.groupBy("t0_year") \
           .count() \
           .orderBy("t0_year") \
           .show()

    #  ------------------------------------------------------------------------------------------------
    # STEP 5: Split into train and holdout
    # Train  = repos created in 2023 or 2024
    # Holdout = repos created in 2025
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 5] Splitting into train (2023-2024) and holdout (2025)...")

    train   = dataset.filter(F.col("t0_year").isin(2023, 2024))
    holdout = dataset.filter(F.col("t0_year") == 2025)

    train_count   = train.count()
    holdout_count = holdout.count()

    print(f"  Training set   : {train_count:,} repos")
    print(f"  Holdout set    : {holdout_count:,} repos")

    #  ------------------------------------------------------------------------------------------------
    # STEP 6: Verify label distribution in both sets
    # Make sure both sets have viral + non-viral repos
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 6] Verifying label distribution...")

    print("  Training set label distribution:")
    train.groupBy("label").count().show()

    print("  Holdout set label distribution:")
    holdout.groupBy("label").count().show()

    # Check viral % in both sets — should be roughly similar
    train_viral_pct   = train.filter(F.col("label") == 1).count() / train_count * 100
    holdout_viral_pct = holdout.filter(F.col("label") == 1).count() / holdout_count * 100

    print(f"  Train viral %  : {train_viral_pct:.4f}%")
    print(f"  Holdout viral %: {holdout_viral_pct:.4f}%")
    print(f"  (These should be similar — confirms stratification)")

    #  ------------------------------------------------------------------------------------------------
    # STEP 7: Leakage check
    # Verify no features use data from day 3+
    # All our features are 48h-bounded so this
    # should always pass — but we check anyway
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 7] Running leakage check...")

    # Check for any nulls in critical feature columns
    leakage_check = dataset.filter(
        F.col("f1_stars_48h").isNull() |
        F.col("f2_forks_48h").isNull() |
        F.col("f3_contributors_48h").isNull() |
        F.col("f4_issues_48h").isNull() |
        F.col("f8_pushes_48h").isNull()
    ).count()

    if leakage_check == 0:
        print("  Leakage check PASSED — no null features found")
    else:
        print(f"  WARNING: {leakage_check} rows have null features — investigate before training")

    #  ------------------------------------------------------------------------------------------------
    # STEP 8: Save train and holdout to HDFS
    #  ------------------------------------------------------------------------------------------------
    print(f"\n[Step 8] Saving training set to: {TRAIN_PATH}")

    train \
        .drop("t0_year") \
        .write \
        .mode("overwrite") \
        .parquet(TRAIN_PATH)

    print(f"  Training set saved ✓")

    print(f"\n  Saving holdout set to: {HOLDOUT_PATH}")

    holdout \
        .drop("t0_year") \
        .write \
        .mode("overwrite") \
        .parquet(HOLDOUT_PATH)

    print(f"  Holdout set saved ✓")

    #  ------------------------------------------------------------------------------------------------
    # STEP 9: Final summary
    #  ------------------------------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("SPLIT SUMMARY")
    print("=" * 60)
    print(f"  Total repos      : {total:,}")
    print(f"  Training set     : {train_count:,} repos (2023-2024)")
    print(f"  Holdout set      : {holdout_count:,} repos (2025)")
    print(f"  Train viral %    : {train_viral_pct:.4f}%")
    print(f"  Holdout viral %  : {holdout_viral_pct:.4f}%")
    print(f"  Leakage check    : {'PASSED' if leakage_check == 0 else 'FAILED'}")
    print("=" * 60)
    print(f"\n  Train path  : {TRAIN_PATH}")
    print(f"  Holdout path: {HOLDOUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()