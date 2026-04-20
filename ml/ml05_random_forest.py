"""
ML-05: Random Forest Training + Evaluation
GitHub Developer Ecosystem Analytics
Author: Tanusha Karnam (tk3514)

What this does:
  1. Loads training set (ML-04 output)
  2. Loads holdout set (ML-04 output)
  3. Adds class weights to handle imbalance (~200:1)
  4. Trains MLlib Random Forest (200 trees, depth 10)
  5. Evaluates on 2025 holdout — PR-AUC, Precision, Recall, F1
  6. Prints feature importance rankings
  7. Saves trained model to HDFS for ML-06

Output:
  - hdfs://namenode:9000/github/ml/model/rf_trained/
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.sql.utils import AnalysisException

#  ------------------------------------------------------------------------------------------------
# CONFIG
#  ------------------------------------------------------------------------------------------------
import sys
sys.path.append("/opt/spark-apps")
from batch.schema import TRAIN_PATH, HOLDOUT_PATH, MODEL_PATH

FEATURE_COLS = [
    "f1_stars_48h",
    "f2_forks_48h",
    "f3_contributors_48h",
    "f4_issues_48h",
    "f5_owner_star_med",
    "f6_readme_bytes",
    "f7_lang_rank",
    "f8_pushes_48h"
]

# Using weightCol instead of SMOTE
# MLlib has no native SMOTE implementation
# Positive class weight ~200 reflects ~10,000:1 imbalance
POSITIVE_CLASS_WEIGHT = 200.0


def main():
    spark = SparkSession.builder \
        .appName("ML-05: Random Forest Training") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("ML-05: Random Forest Training + Evaluation")
    print("=" * 60)

    #  ------------------------------------------------------------------------------------------------
    # STEP 1: Load train + holdout sets
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 1] Loading train and holdout sets...")

    try:
        train = spark.read.parquet(TRAIN_PATH)
    except AnalysisException:
        print(f"ERROR: Cannot find training data at {TRAIN_PATH}")
        print("Make sure ML-04 has finished first.")
        spark.stop()
        exit(1)

    try:
        holdout = spark.read.parquet(HOLDOUT_PATH)
    except AnalysisException:
        print(f"ERROR: Cannot find holdout data at {HOLDOUT_PATH}")
        print("Make sure ML-04 has finished first.")
        spark.stop()
        exit(1)

    train_count   = train.count()
    holdout_count = holdout.count()

    print(f"  Training set : {train_count:,} repos")
    print(f"  Holdout set  : {holdout_count:,} repos")

    #  ------------------------------------------------------------------------------------------------
    # STEP 2: Add class weights
    # Positive class (viral) gets weight ~200
    # Negative class (not viral) gets weight 1.0
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 2] Adding class weights...")

    train = train.withColumn(
        "classWeight",
        F.when(F.col("label") == 1, POSITIVE_CLASS_WEIGHT)
         .otherwise(1.0)
    )

    holdout = holdout.withColumn(
        "classWeight",
        F.when(F.col("label") == 1, POSITIVE_CLASS_WEIGHT)
         .otherwise(1.0)
    )

    viral_train = train.filter(F.col("label") == 1).count()
    print(f"  Viral repos in training : {viral_train:,}")
    print(f"  Positive class weight   : {POSITIVE_CLASS_WEIGHT}")

    #  ------------------------------------------------------------------------------------------------
    # STEP 3: Build ML pipeline
    # VectorAssembler → RandomForestClassifier
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 3] Building ML pipeline...")

    assembler = VectorAssembler(
        inputCols=FEATURE_COLS,
        outputCol="features",
        handleInvalid="skip"
    )

    # Using weightCol instead of SMOTE
    # MLlib has no native SMOTE implementation
    rf = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        weightCol="classWeight",
        numTrees=200,
        maxDepth=10,
        seed=42
    )

    pipeline = Pipeline(stages=[assembler, rf])

    #  ------------------------------------------------------------------------------------------------
    # STEP 4: Train on 2023-2024 data
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 4] Training Random Forest on 2023-2024 data...")
    print("  This will take a while on the full dataset...")

    model = pipeline.fit(train)

    print("  Training complete!")

    #  ------------------------------------------------------------------------------------------------
    # STEP 5: Run predictions on 2025 holdout
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 5] Running predictions on 2025 holdout...")

    predictions = model.transform(holdout)
    predictions.cache()

    #  ------------------------------------------------------------------------------------------------
    # STEP 6: Evaluate — PR-AUC (primary metric)
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 6] Evaluating model...")

    # PR-AUC — primary metric under class imbalance
    pr_evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderPR"
    )
    pr_auc = pr_evaluator.evaluate(predictions)

    # ROC-AUC — secondary metric
    roc_evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )
    roc_auc = roc_evaluator.evaluate(predictions)

    # Precision
    precision_evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="weightedPrecision"
    )
    precision = precision_evaluator.evaluate(predictions)

    # Recall
    recall_evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="weightedRecall"
    )
    recall = recall_evaluator.evaluate(predictions)

    # F1
    f1_evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="f1"
    )
    f1 = f1_evaluator.evaluate(predictions)

    #  ------------------------------------------------------------------------------------------------
    # STEP 7: Feature importance rankings
    #  ------------------------------------------------------------------------------------------------
    print("\n[Step 7] Feature importance rankings...")

    rf_model     = model.stages[-1]
    importances  = rf_model.featureImportances
    feature_importance = sorted(
        zip(FEATURE_COLS, importances),
        key=lambda x: x[1],
        reverse=True
    )

    print("\n  Feature Importance (descending):")
    print(f"  {'Feature':<25} {'Importance':>10}")
    print(f"  {'-'*35}")
    for feat, imp in feature_importance:
        print(f"  {feat:<25} {imp:>10.4f}")

    #  ------------------------------------------------------------------------------------------------
    # STEP 8: Print full evaluation summary
    #  ------------------------------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("MODEL EVALUATION SUMMARY (2025 Holdout)")
    print("=" * 60)
    print(f"  PR-AUC    : {pr_auc:.4f}   ← primary metric")
    print(f"  ROC-AUC   : {roc_auc:.4f}")
    print(f"  Precision : {precision:.4f}")
    print(f"  Recall    : {recall:.4f}")
    print(f"  F1 Score  : {f1:.4f}")
    print("=" * 60)

    # Show sample predictions
    print("\nSample predictions (viral repos only):")
    predictions \
        .filter(F.col("label") == 1) \
        .select("repo_name", "label", "prediction", "probability") \
        .show(10, truncate=False)

    #  ------------------------------------------------------------------------------------------------
    # STEP 9: Save model to HDFS
    # Shreyas loads this in STREAM-06
    #  ------------------------------------------------------------------------------------------------
    print(f"\n[Step 9] Saving model to HDFS: {MODEL_PATH}")

    model.write().overwrite().save(MODEL_PATH)

    print("  Model saved successfully!")
    print(f"  Model path: {MODEL_PATH}")
    print("\n  Notify Shreyas — STREAM-06 can now load the model")
    print(f"  Load path for Shreyas: {MODEL_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()