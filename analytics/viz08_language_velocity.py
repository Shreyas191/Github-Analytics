"""
VIZ-08: Language Velocity Rankings Report
GitHub Developer Ecosystem Analytics
Author: Vikram Markali (vrm9190)

What this does:
  Computes monthly language velocity metrics across 2023-2025 and identifies
  the top 10 fastest-rising and top 10 fastest-declining programming languages.

  Velocity is measured by year-over-year growth rate in star acquisition:
    YoY growth = (stars_2024 - stars_2023) / stars_2023  (for 2023->2024)

  Final rankings use the average growth rate across available year transitions.

  Monthly snapshots are also stored so Grafana can render a time series of
  language adoption trends from 2023 to 2025.

  The final report annotates inflection points -- months where a language
  showed unusually high growth -- which the team will correlate with real-world
  events in the VIZ-09 final report (e.g., a major framework release).

Input:
  hdfs://namenode:9000/github/events/parquet/

Output:
  postgres-analytics: language_velocity table (monthly snapshots)
  postgres-analytics: language_rankings table (top 10 rising/declining)

Usage:
  # Sample run:
  docker exec spark-master spark-submit \\
    --master spark://spark-master:7077 \\
    --packages org.postgresql:postgresql:42.6.0 \\
    /opt/spark-apps/analytics/viz08_language_velocity.py --sample

  # Full run:
  docker exec spark-master spark-submit \\
    --master spark://spark-master:7077 \\
    --packages org.postgresql:postgresql:42.6.0 \\
    /opt/spark-apps/analytics/viz08_language_velocity.py
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
logger = logging.getLogger("viz08.language_velocity")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
EVENTS_PATH  = "hdfs://namenode:9000/github/events/parquet"
JDBC_URL     = "jdbc:postgresql://postgres-analytics:5432/github_analytics"
JDBC_PROPS   = {"user": "analytics", "password": "analytics", "driver": "org.postgresql.Driver"}

MIN_ANNUAL_STARS = 100   # ignore languages with fewer than 100 stars/year (too niche)
TOP_N            = 10    # how many rising / declining to report


def compute_monthly_snapshots(spark: SparkSession, sample: bool):
    """
    Compute monthly metrics per language:
      - new_repo_count  : CreateEvent with ref_type='repository'
      - star_count      : WatchEvent
      - fork_count      : ForkEvent
      - push_count      : PushEvent
      - actor_count     : unique contributors
    """
    events = spark.read.parquet(EVENTS_PATH)

    if sample:
        events = events.sample(fraction=0.10, seed=42)
        logger.info("Sample mode: 10% of events.")

    events = events.filter(
        F.col("language").isNotNull() &
        (F.col("language") != "unknown")
    ).withColumn(
        "year_month",
        F.concat(
            F.year("created_at").cast("string"),
            F.lit("-"),
            F.lpad(F.month("created_at").cast("string"), 2, "0"),
        )
    )

    events.cache()

    # New repos per language per month
    new_repos = (
        events
        .filter(
            (F.col("event_type") == "CreateEvent") &
            (F.col("ref_type") == "repository")
        )
        .groupBy("language", "year_month")
        .agg(F.count("*").alias("new_repo_count"))
    )

    # Stars per language per month (WatchEvent = star)
    stars = (
        events
        .filter(F.col("event_type") == "WatchEvent")
        .groupBy("language", "year_month")
        .agg(F.count("*").alias("star_count"))
    )

    # Forks per language per month
    forks = (
        events
        .filter(F.col("event_type") == "ForkEvent")
        .groupBy("language", "year_month")
        .agg(F.count("*").alias("fork_count"))
    )

    # Pushes per language per month
    pushes = (
        events
        .filter(F.col("event_type") == "PushEvent")
        .groupBy("language", "year_month")
        .agg(F.count("*").alias("push_count"))
    )

    # Unique actors per language per month
    actors = (
        events
        .groupBy("language", "year_month")
        .agg(F.countDistinct("actor_id").alias("actor_count"))
    )

    # Get all (language, year_month) combinations that exist in any event type
    all_months = events.select("language", "year_month").distinct()

    monthly = (
        all_months
        .join(new_repos, on=["language", "year_month"], how="left")
        .join(stars,     on=["language", "year_month"], how="left")
        .join(forks,     on=["language", "year_month"], how="left")
        .join(pushes,    on=["language", "year_month"], how="left")
        .join(actors,    on=["language", "year_month"], how="left")
        .fillna(0, subset=["new_repo_count", "star_count", "fork_count",
                           "push_count", "actor_count"])
        .withColumn("computed_at", F.current_timestamp())
        .orderBy("language", "year_month")
    )

    count = monthly.count()
    logger.info(f"Monthly snapshots: {count:,} rows.")
    return monthly


def compute_rankings(monthly):
    """
    Compute year-over-year star growth rate per language.
    Returns top 10 rising and top 10 declining.
    """
    # Annual star totals
    annual = (
        monthly
        .withColumn("year", F.col("year_month").substr(1, 4).cast("int"))
        .groupBy("language", "year")
        .agg(F.sum("star_count").alias("annual_stars"))
        .filter(F.col("annual_stars") >= MIN_ANNUAL_STARS)
    )

    # Year-over-year: compare each year to the previous
    w = Window.partitionBy("language").orderBy("year")
    yoy = (
        annual
        .withColumn("prev_stars", F.lag("annual_stars", 1).over(w))
        .filter(F.col("prev_stars").isNotNull() & (F.col("prev_stars") > 0))
        .withColumn(
            "yoy_growth_rate",
            (F.col("annual_stars") - F.col("prev_stars")) / F.col("prev_stars")
        )
    )

    # Average YoY growth rate across all year transitions per language
    avg_yoy = (
        yoy
        .groupBy("language")
        .agg(F.avg("yoy_growth_rate").alias("yoy_growth_rate"))
    )

    # Total stars + repos across all years
    totals = (
        monthly
        .groupBy("language")
        .agg(
            F.sum("star_count").alias("total_stars"),
            F.sum("new_repo_count").alias("total_repos"),
        )
    )

    rankings_base = avg_yoy.join(totals, on="language")

    # Rising: top 10 by YoY growth rate (positive)
    w_rank = Window.orderBy(F.col("yoy_growth_rate").desc())
    rising = (
        rankings_base
        .filter(F.col("yoy_growth_rate") > 0)
        .withColumn("velocity_rank", F.rank().over(w_rank))
        .filter(F.col("velocity_rank") <= TOP_N)
        .withColumn("direction", F.lit("rising"))
    )

    # Declining: top 10 by YoY growth rate (most negative)
    w_rank_dec = Window.orderBy(F.col("yoy_growth_rate").asc())
    declining = (
        rankings_base
        .filter(F.col("yoy_growth_rate") < 0)
        .withColumn("velocity_rank", F.rank().over(w_rank_dec))
        .filter(F.col("velocity_rank") <= TOP_N)
        .withColumn("direction", F.lit("declining"))
    )

    rankings = (
        rising.union(declining)
        .withColumn("computed_at", F.current_timestamp())
        .select(
            "language", "total_repos", "total_stars",
            "yoy_growth_rate", "velocity_rank", "direction", "computed_at"
        )
        .orderBy("direction", "velocity_rank")
    )

    return rankings


def write_postgres(monthly, rankings):
    monthly.write \
           .mode("overwrite") \
           .option("truncate", "true") \
           .jdbc(JDBC_URL, "language_velocity", properties=JDBC_PROPS)
    logger.info(f"Wrote language_velocity table.")

    rankings.write \
            .mode("overwrite") \
            .option("truncate", "true") \
            .jdbc(JDBC_URL, "language_rankings", properties=JDBC_PROPS)
    logger.info("Wrote language_rankings table.")


def print_summary(rankings):
    print("\n" + "=" * 60)
    print("VIZ-08 LANGUAGE VELOCITY RANKINGS")
    print("=" * 60)
    print("\nTop 10 RISING languages (by YoY star growth rate):")
    rankings.filter(F.col("direction") == "rising") \
            .orderBy("velocity_rank") \
            .select("velocity_rank", "language", "yoy_growth_rate", "total_stars") \
            .show(10, truncate=False)
    print("\nTop 10 DECLINING languages (by YoY star growth rate):")
    rankings.filter(F.col("direction") == "declining") \
            .orderBy("velocity_rank") \
            .select("velocity_rank", "language", "yoy_growth_rate", "total_stars") \
            .show(10, truncate=False)
    print("=" * 60)


def parse_args():
    p = argparse.ArgumentParser(description="VIZ-08: Language Velocity Rankings")
    p.add_argument("--sample", action="store_true",
                   help="Use 10% sample of events for fast testing")
    return p.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("VIZ-08: Language Velocity Rankings")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    logger.info("=" * 60)
    logger.info("VIZ-08: Language Velocity Rankings")
    logger.info(f"  sample : {args.sample}")
    logger.info(f"  top N  : {TOP_N}")
    logger.info("=" * 60)

    monthly  = compute_monthly_snapshots(spark, args.sample)
    monthly.cache()

    rankings = compute_rankings(monthly)
    rankings.cache()

    write_postgres(monthly, rankings)
    print_summary(rankings)

    spark.stop()
    logger.info("VIZ-08 complete.")


if __name__ == "__main__":
    main()
