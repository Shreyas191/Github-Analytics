"""
ML-06: Model Serialization Verification
GitHub Developer Ecosystem Analytics
Author: Tanusha Karnam (tk3514)

What this does:
  Model is already saved to HDFS at the end of ML-05.
  This script verifies the model exists on HDFS and
  prints the exact load path for Shreyas to use in STREAM-06.

  IMPORTANT: Run this immediately after ML-05 finishes.
  Notify Shreyas with the model path — he cannot build
  STREAM-06 until this path is confirmed live on HDFS.

  Shreyas loads the model in STREAM-06 with:
    from pyspark.ml import PipelineModel
    model = PipelineModel.load("hdfs://namenode:9000/github/ml/model/rf_trained/")
"""

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
MODEL_PATH = "hdfs://namenode:9000/github/ml/model/rf_trained/"


def main():
    spark = SparkSession.builder \
        .appName("ML-06: Model Serialization Verify") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("ML-06: Model Serialization Verification")
    print("=" * 60)

    # ─────────────────────────────────────────────
    # STEP 1: Verify model loads correctly from HDFS
    # ─────────────────────────────────────────────
    print(f"\n[Step 1] Loading model from HDFS: {MODEL_PATH}")

    try:
        model = PipelineModel.load(MODEL_PATH)
        print("  Model loaded successfully!")
        print(f"  Number of stages : {len(model.stages)}")
        print(f"  Stage 1          : {type(model.stages[0]).__name__} (VectorAssembler)")
        print(f"  Stage 2          : {type(model.stages[1]).__name__} (RandomForestModel)")
        print(f"  Total trees      : {model.stages[1].getNumTrees}")
    except Exception as e:
        print(f"  ERROR: Could not load model — {e}")
        print("  Make sure ML-05 has finished and saved the model first.")
        spark.stop()
        exit(1)

    # ─────────────────────────────────────────────
    # STEP 2: Print feature importance from saved model
    # ─────────────────────────────────────────────
    print("\n[Step 2] Feature importance from saved model...")

    feature_cols = [
        "f1_stars_48h",
        "f2_forks_48h",
        "f3_contributors_48h",
        "f4_issues_48h",
        "f5_owner_star_med",
        "f6_readme_bytes",
        "f7_lang_rank",
        "f8_pushes_48h"
    ]

    importances = model.stages[1].featureImportances
    feature_importance = sorted(
        zip(feature_cols, importances),
        key=lambda x: x[1],
        reverse=True
    )

    print(f"\n  {'Feature':<25} {'Importance':>10}")
    print(f"  {'-' * 35}")
    for feat, imp in feature_importance:
        print(f"  {feat:<25} {imp:>10.4f}")

    # ─────────────────────────────────────────────
    # STEP 3: Print handoff info for Shreyas
    # ─────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("MODEL HANDOFF — NOTIFY SHREYAS NOW")
    print("=" * 60)
    print(f"  Model path : {MODEL_PATH}")
    print(f"\n  Shreyas — use this in STREAM-06:")
    print(f"    from pyspark.ml import PipelineModel")
    print(f"    model = PipelineModel.load('{MODEL_PATH}')")
    print("\n  Note: language field is null in stream.")
    print("  F7 (lang_rank) will default to 0 for live scoring.")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()