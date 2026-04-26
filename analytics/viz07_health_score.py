"""
VIZ-07: Ecosystem Health Score -- Formula + Computation
GitHub Developer Ecosystem Analytics
Author: Vikram Markali (vrm9190)

What this does:
  Computes a composite Ecosystem Health Score (0-100) per programming language
  from 2023-2025 batch data. The score synthesizes four dimensions:

    Component            Weight   Definition
    -------------------------------------------------------
    velocity_score         30%   Stars per repo per month (adoption momentum)
    growth_score           25%   Month-over-month unique contributor growth rate
    maintenance_score      25%   Push + PR + Issue events per repo (active dev)
    diversity_score        20%   Unique actors / total events (contributor breadth)

  Each component is min-max normalized across all languages to a 0-100 scale,
  so scores are relative (Python at 90 means it outperforms most others, not
  that it achieved 90% of some absolute target).

  Formula locked in Week 1 as specified in the project tracker.

Input:
  hdfs://namenode:9000/github/events/parquet/

Output:
  postgres-analytics: ecosystem_health table

Usage:
  # Apply schema first (one time):
  docker exec -i postgres-analytics psql -U analytics -d github_analytics \\
    < /opt/spark-apps/analytics/db_schema.sql

  # Sample run:
  docker exec spark-master spark-submit \\
    --master spark://spark-master:7077 \\
    --packages org.postgresql:postgresql:42.6.0 \\
    /opt/spark-apps/analytics/viz07_health_score.py --sample

  # Full run:
  docker exec spark-master spark-submit \\
    --master spark://spark-master:7077 \\
    --packages org.postgresql:postgresql:42.6.0 \\
    /opt/spark-apps/analytics/viz07_health_score.py
"""

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("viz07.health_score")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
EVENTS_PATH  = "hdfs://namenode:9000/github/events/parquet"
JDBC_URL     = "jdbc:postgresql://postgres-analytics:5432/github_analytics"
JDBC_PROPS   = {"user": "analytics", "password": "analytics", "driver": "org.postgresql.Driver"}

# Health score weights -- locked in Week 1 per project tracker
W_VELOCITY    = 0.30
W_GROWTH      = 0.25
W_MAINTENANCE = 0.25
W_DIVERSITY   = 0.20

# Only score languages with enough data to be meaningful
MIN_REPOS    = 50
MIN_ACTORS   = 100


def normalize(df, col_name: str, out_col: str):
    """
    Min-max normalize a column to [0, 100] across the full DataFrame.
    Ties at min get 0; ties at max get 100.
    """
    stats = df.agg(
        F.min(col_name).alias("mn"),
        F.max(col_name).alias("mx"),
    ).first()
    mn, mx = stats["mn"], stats["mx"]
    rng = mx - mn if mx != mn else 1.0

    return df.withColumn(
        out_col,
        F.round(100.0 * (F.col(col_name) - F.lit(mn)) / F.lit(rng), 2)
    )


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("VIZ-07: Ecosystem Health Score")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    logger.info("=" * 60)
    logger.info("VIZ-07: Ecosystem Health Score")
    logger.info(f"  weights: velocity={W_VELOCITY} growth={W_GROWTH} "
                f"maintenance={W_MAINTENANCE} diversity={W_DIVERSITY}")
    logger.info(f"  sample: {args.sample}")
    logger.info("=" * 60)

    events = spark.read.parquet(EVENTS_PATH)

    if args.sample:
        events = events.sample(fraction=0.10, seed=42)
        logger.info("Sample mode: using 10% of events.")

    # Keep only events with a known language
    events = events.filter(
        F.col("language").isNotNull() &
        (F.col("language") != "unknown")
    )

    # Add year_month for monthly granularity
    events = events.withColumn(
        "year_month",
        F.concat(F.year("created_at"), F.lit("-"),
                 F.lpad(F.month("created_at").cast("string"), 2, "0"))
    )

    events.cache()
    logger.info(f"Events loaded and cached.")

    # -----------------------------------------------------------------------
    # Component 1: velocity_score -- stars per repo per month
    # WatchEvent = star. Stars per unique repo per month, averaged over all months.
    # High velocity means repos get starred at a consistent rate.
    # -----------------------------------------------------------------------
    logger.info("Computing velocity_score (stars per repo per month)...")

    repos_per_lang = (
        events
        .select("language", "repo_id")
        .distinct()
        .groupBy("language")
        .agg(F.countDistinct("repo_id").alias("total_repos"))
    )

    stars_per_lang_month = (
        events
        .filter(F.col("event_type") == "WatchEvent")
        .groupBy("language", "year_month")
        .agg(F.count("*").alias("monthly_stars"))
    )

    velocity_raw = (
        stars_per_lang_month
        .groupBy("language")
        .agg(F.avg("monthly_stars").alias("avg_monthly_stars"))
        .join(repos_per_lang, on="language")
        .withColumn("velocity_raw", F.col("avg_monthly_stars") / F.col("total_repos"))
    )

    # -----------------------------------------------------------------------
    # Component 2: growth_score -- contributor growth month over month
    # Counts unique contributors per language per month, computes average
    # month-over-month growth rate using a lag window.
    # -----------------------------------------------------------------------
    logger.info("Computing growth_score (contributor MoM growth)...")

    contributors_monthly = (
        events
        .groupBy("language", "year_month")
        .agg(F.countDistinct("actor_id").alias("monthly_actors"))
        .orderBy("language", "year_month")
    )

    w = Window.partitionBy("language").orderBy("year_month")
    growth_raw = (
        contributors_monthly
        .withColumn("prev_actors", F.lag("monthly_actors", 1).over(w))
        .filter(F.col("prev_actors").isNotNull() & (F.col("prev_actors") > 0))
        .withColumn(
            "mom_growth",
            (F.col("monthly_actors") - F.col("prev_actors")) / F.col("prev_actors")
        )
        .groupBy("language")
        .agg(F.avg("mom_growth").alias("avg_mom_growth"))
        .withColumn("growth_raw", F.greatest(F.col("avg_mom_growth"), F.lit(0.0)))
    )

    # -----------------------------------------------------------------------
    # Component 3: maintenance_score -- active dev events per repo
    # Push + PR + Issues events per unique repo, averaged per month.
    # Measures whether repos stay actively maintained after creation.
    # -----------------------------------------------------------------------
    logger.info("Computing maintenance_score (active dev events per repo)...")

    maintenance_events = (
        events
        .filter(F.col("event_type").isin("PushEvent", "PullRequestEvent", "IssuesEvent"))
    )

    maintenance_monthly = (
        maintenance_events
        .groupBy("language", "year_month")
        .agg(
            F.count("*").alias("maint_events"),
            F.countDistinct("repo_id").alias("active_repos"),
        )
        .withColumn("maint_per_repo", F.col("maint_events") / F.col("active_repos"))
    )

    maintenance_raw = (
        maintenance_monthly
        .groupBy("language")
        .agg(F.avg("maint_per_repo").alias("maintenance_raw"))
    )

    # -----------------------------------------------------------------------
    # Component 4: diversity_score -- unique actors / total events
    # A language dominated by a few power users has low diversity.
    # Many distinct contributors = healthy open community.
    # -----------------------------------------------------------------------
    logger.info("Computing diversity_score (actor diversity ratio)...")

    diversity_raw = (
        events
        .groupBy("language")
        .agg(
            F.countDistinct("actor_id").alias("total_actors"),
            F.count("*").alias("total_events"),
        )
        .withColumn("diversity_raw", F.col("total_actors") / F.col("total_events"))
    )

    # -----------------------------------------------------------------------
    # Assemble all components
    # -----------------------------------------------------------------------
    logger.info("Assembling health score components...")

    health = (
        repos_per_lang
        .join(velocity_raw.select("language", "velocity_raw"),     on="language", how="left")
        .join(growth_raw.select("language", "growth_raw"),         on="language", how="left")
        .join(maintenance_raw.select("language", "maintenance_raw"), on="language", how="left")
        .join(diversity_raw.select("language", "total_actors", "total_events", "diversity_raw"),
              on="language", how="left")
        .filter(
            (F.col("total_repos") >= MIN_REPOS) &
            (F.col("total_actors") >= MIN_ACTORS)
        )
        .fillna(0.0, subset=["velocity_raw", "growth_raw", "maintenance_raw", "diversity_raw"])
    )

    # Normalize each component 0-100
    health = normalize(health, "velocity_raw",    "velocity_score")
    health = normalize(health, "growth_raw",      "growth_score")
    health = normalize(health, "maintenance_raw", "maintenance_score")
    health = normalize(health, "diversity_raw",   "diversity_score")

    # Weighted composite
    health = health.withColumn(
        "health_score",
        F.round(
            F.lit(W_VELOCITY)    * F.col("velocity_score") +
            F.lit(W_GROWTH)      * F.col("growth_score")   +
            F.lit(W_MAINTENANCE) * F.col("maintenance_score") +
            F.lit(W_DIVERSITY)   * F.col("diversity_score"),
            2
        )
    )

    final = health.select(
        "language",
        "velocity_score",
        "growth_score",
        "maintenance_score",
        "diversity_score",
        "health_score",
        F.col("total_repos").alias("total_repos"),
        F.col("total_actors").alias("total_actors"),
        F.current_timestamp().alias("computed_at"),
    ).orderBy(F.col("health_score").desc())

    final.cache()
    total_langs = final.count()
    logger.info(f"Health scores computed for {total_langs:,} languages.")

    # -----------------------------------------------------------------------
    # Write to postgres
    # -----------------------------------------------------------------------
    final.write \
         .mode("overwrite") \
         .option("truncate", "true") \
         .jdbc(JDBC_URL, "ecosystem_health", properties=JDBC_PROPS)
    logger.info("Wrote ecosystem_health table.")

    print("\n" + "=" * 60)
    print("VIZ-07 ECOSYSTEM HEALTH SCORE SUMMARY")
    print("=" * 60)
    print("\nTop 20 healthiest language ecosystems:")
    final.select(
        "language", "health_score",
        "velocity_score", "growth_score", "maintenance_score", "diversity_score"
    ).show(20, truncate=False)
    print("=" * 60)

    spark.stop()
    logger.info("VIZ-07 complete.")


def parse_args():
    p = argparse.ArgumentParser(description="VIZ-07: Ecosystem Health Score")
    p.add_argument("--sample", action="store_true",
                   help="Use 10% sample of events for fast testing")
    return p.parse_args()


if __name__ == "__main__":
    main()
