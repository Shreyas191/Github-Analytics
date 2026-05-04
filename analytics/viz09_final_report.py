"""
VIZ-09: Final Analytical Report
GitHub Developer Ecosystem Analytics
Author: Vikram Markali (vrm9190)

What this does:
  Consolidates findings from VIZ-04 through VIZ-08 and ML-07 into a
  complete formatted final report. Reads from postgres-analytics tables
  and HDFS (ML-07 case studies). Prints a section-by-section report
  suitable for direct inclusion in the project submission.

  Sections:
    1. Platform Overview       -- total scale of the dataset
    2. Geographic Shift        -- VIZ-04 top countries + YoY movement
    3. Language Ecosystem      -- VIZ-05 top PMI pairs, most connected nodes
    4. Ecosystem Health        -- VIZ-07 top and bottom ranked languages
    5. Language Velocity       -- VIZ-08 rising + declining + inflection points
    6. Viral Repo Case Studies -- ML-07 early-detection results
    7. Key Takeaways           -- summary findings for the business case

Prerequisites (run in order before this script):
  1. INFRA-04 -- events Parquet in HDFS
  2. VIZ-04   -- geo_contributions table populated
  3. VIZ-05   -- pmi_pairs + language_nodes tables populated
  4. VIZ-07   -- ecosystem_health table populated
  5. VIZ-08   -- language_velocity + language_rankings tables populated
  6. ML-07    -- case studies Parquet at hdfs://.../github/ml/case_studies/

Usage:
  docker exec spark-master spark-submit \\
    --master spark://spark-master:7077 \\
    --packages org.postgresql:postgresql:42.6.0 \\
    --conf spark.jars.ivy=/tmp/.ivy \\
    /opt/spark-apps/analytics/viz09_final_report.py

  # Compact output (fewer rows per section):
  docker exec spark-master spark-submit \\
    --master spark://spark-master:7077 \\
    --packages org.postgresql:postgresql:42.6.0 \\
    --conf spark.jars.ivy=/tmp/.ivy \\
    /opt/spark-apps/analytics/viz09_final_report.py --brief
"""

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("viz09.final_report")

# ---------------------------------------------------------------------------
# Config -- same JDBC settings as VIZ-04 through VIZ-08
# ---------------------------------------------------------------------------
JDBC_URL   = "jdbc:postgresql://postgres-analytics:5432/github_analytics"
JDBC_PROPS = {"user": "analytics", "password": "analytics", "driver": "org.postgresql.Driver"}

CASE_STUDIES_PATH = "hdfs://namenode:9000/github/ml/case_studies"

# ---------------------------------------------------------------------------
# Known real-world events for inflection point annotation (VIZ-08 section)
# Keyed by YYYY-MM matching year_month column in language_velocity table.
# ---------------------------------------------------------------------------
KNOWN_EVENTS = {
    "2023-01": "ChatGPT mainstream adoption surge",
    "2023-03": "GPT-4 released by OpenAI",
    "2023-06": "GitHub Copilot GA for all developers",
    "2023-09": "Meta Llama 2 open-source release",
    "2023-11": "OpenAI DevDay -- GPT-4 Turbo + Assistants API",
    "2024-01": "Google Gemini Pro API public launch",
    "2024-03": "Claude 3 Opus released (Anthropic)",
    "2024-05": "OpenAI GPT-4o release",
    "2024-09": "Rust 2024 edition stabilized",
    "2024-10": "Meta Llama 3.2 multimodal open release",
    "2024-11": "OpenAI o1 model series expansion",
    "2025-01": "DeepSeek R1 open-source release -- global AI surge",
    "2025-02": "Google Gemini 2.0 Flash launch",
    "2025-03": "Claude 3.7 Sonnet (Anthropic) release",
}


def divider(char="=", width=70):
    print(char * width)


def section(title):
    print()
    divider()
    print(f"  {title}")
    divider()


def read_table(spark, table):
    return spark.read.jdbc(JDBC_URL, table, properties=JDBC_PROPS)


# ---------------------------------------------------------------------------
# Section 1: Platform Overview
# ---------------------------------------------------------------------------
def section_overview(spark, brief):
    section("SECTION 1: PLATFORM OVERVIEW")

    try:
        geo   = read_table(spark, "geo_contributions")
        pmi   = read_table(spark, "pmi_pairs")
        hlth  = read_table(spark, "ecosystem_health")
        vel   = read_table(spark, "language_velocity")
        ranks = read_table(spark, "language_rankings")
    except Exception as e:
        print(f"  ERROR reading overview tables: {e}")
        print("  Run VIZ-04 through VIZ-08 first.")
        return

    total_contributors = geo.agg(F.sum("actor_count")).collect()[0][0] or 0
    total_events       = geo.agg(F.sum("event_count")).collect()[0][0] or 0
    total_countries    = geo.select("country_code").distinct().count()
    total_pmi_pairs    = pmi.count()
    total_languages    = hlth.count()
    total_months       = vel.select("year_month").distinct().count()

    print(f"""
  Dataset span       : 2023 - 2025
  Countries tracked  : {total_countries:,}
  Total contributors : {total_contributors:,}
  Total events       : {total_events:,}
  Languages scored   : {total_languages}
  PMI pairs computed : {total_pmi_pairs:,}
  Monthly snapshots  : {total_months:,}
""")


# ---------------------------------------------------------------------------
# Section 2: Geographic Contribution Shift (VIZ-04)
# ---------------------------------------------------------------------------
def section_geo(spark, brief):
    section("SECTION 2: GEOGRAPHIC CONTRIBUTION SHIFT  (VIZ-04)")

    try:
        geo = read_table(spark, "geo_contributions")
    except Exception as e:
        print(f"  ERROR: {e}  -- run VIZ-04 first.")
        return

    top_n = 5 if brief else 10

    print(f"\n  Top {top_n} countries by total contributors (all years):")
    print(f"  {'Country':<25} {'Contributors':>14} {'Events':>14}")
    print(f"  {'-'*25} {'-'*14} {'-'*14}")

    totals = (
        geo
        .groupBy("country_name")
        .agg(
            F.sum("actor_count").alias("actors"),
            F.sum("event_count").alias("events"),
        )
        .orderBy(F.col("actors").desc())
        .limit(top_n)
        .collect()
    )
    for row in totals:
        print(f"  {row['country_name']:<25} {row['actors']:>14,} {row['events']:>14,}")

    # Year-over-year shift: compare latest year vs earliest year
    years = [r[0] for r in geo.select("year").distinct().orderBy("year").collect()]
    if len(years) >= 2:
        y_early = years[0]
        y_late  = years[-1]

        early = (
            geo.filter(F.col("year") == y_early)
            .groupBy("country_name")
            .agg(F.sum("actor_count").alias("actors_early"))
        )
        late = (
            geo.filter(F.col("year") == y_late)
            .groupBy("country_name")
            .agg(F.sum("actor_count").alias("actors_late"))
        )

        shift = (
            early.join(late, on="country_name", how="inner")
            .withColumn(
                "pct_change",
                F.round(
                    100.0 * (F.col("actors_late") - F.col("actors_early"))
                    / F.col("actors_early"),
                    1,
                ),
            )
            .filter(F.col("actors_early") >= 100)
            .orderBy(F.col("pct_change").desc())
        )

        print(f"\n  Fastest-growing contributor regions ({y_early} to {y_late}):")
        print(f"  {'Country':<25} {str(y_early):>8} {str(y_late):>8} {'Change':>8}")
        print(f"  {'-'*25} {'-'*8} {'-'*8} {'-'*8}")
        for row in shift.limit(top_n).collect():
            print(
                f"  {row['country_name']:<25} "
                f"{row['actors_early']:>8,} "
                f"{row['actors_late']:>8,} "
                f"{row['pct_change']:>+7.1f}%"
            )

        print(f"\n  Fastest-declining contributor regions ({y_early} to {y_late}):")
        print(f"  {'Country':<25} {str(y_early):>8} {str(y_late):>8} {'Change':>8}")
        print(f"  {'-'*25} {'-'*8} {'-'*8} {'-'*8}")
        for row in shift.orderBy(F.col("pct_change").asc()).limit(5).collect():
            print(
                f"  {row['country_name']:<25} "
                f"{row['actors_early']:>8,} "
                f"{row['actors_late']:>8,} "
                f"{row['pct_change']:>+7.1f}%"
            )


# ---------------------------------------------------------------------------
# Section 3: Language Co-occurrence Ecosystem (VIZ-05 / VIZ-06)
# ---------------------------------------------------------------------------
def section_pmi(spark, brief):
    section("SECTION 3: LANGUAGE CO-OCCURRENCE ECOSYSTEM  (VIZ-05 / VIZ-06)")

    try:
        pmi   = read_table(spark, "pmi_pairs")
        nodes = read_table(spark, "language_nodes")
    except Exception as e:
        print(f"  ERROR: {e}  -- run VIZ-05 first.")
        return

    top_n = 5 if brief else 10

    print(f"\n  Top {top_n} language pairs by PMI score:")
    print(f"  {'Language A':<20} {'Language B':<20} {'PMI Score':>10} {'Co-occurrences':>16}")
    print(f"  {'-'*20} {'-'*20} {'-'*10} {'-'*16}")

    for row in pmi.orderBy(F.col("pmi_score").desc()).limit(top_n).collect():
        print(
            f"  {row['lang_a']:<20} {row['lang_b']:<20} "
            f"{row['pmi_score']:>10.4f} {row['cooccurrence']:>16,}"
        )

    print(f"\n  Most connected languages (highest edge count in PMI graph):")
    print(f"  {'Language':<22} {'Edges':>8} {'Actor Count':>14}")
    print(f"  {'-'*22} {'-'*8} {'-'*14}")

    edges_a = pmi.groupBy("lang_a").agg(F.count("*").alias("cnt")).withColumnRenamed("lang_a", "language")
    edges_b = pmi.groupBy("lang_b").agg(F.count("*").alias("cnt")).withColumnRenamed("lang_b", "language")
    edge_counts = (
        edges_a.union(edges_b)
        .groupBy("language")
        .agg(F.sum("cnt").alias("edge_count"))
        .join(nodes.select("language", "actor_count"), on="language", how="left")
        .orderBy(F.col("edge_count").desc())
        .limit(top_n)
        .collect()
    )
    for row in edge_counts:
        actors = row["actor_count"] or 0
        print(f"  {row['language']:<22} {row['edge_count']:>8,} {actors:>14,}")

    print("""
  Interpretation:
    High PMI between two languages means developers who use one are far
    more likely than average to also use the other. Pairs like
    JavaScript + TypeScript or Python + Jupyter reflect tight ecosystem
    coupling, not just popularity. Low-PMI high-co-occurrence pairs
    (e.g. Python + JavaScript) indicate broad overlap driven purely by
    language dominance rather than ecosystem affinity.
    See the interactive graph: analytics/viz06_pmi_graph.html
""")


# ---------------------------------------------------------------------------
# Section 4: Ecosystem Health Rankings (VIZ-07)
# ---------------------------------------------------------------------------
def section_health(spark, brief):
    section("SECTION 4: ECOSYSTEM HEALTH SCORE  (VIZ-07)")

    try:
        hlth = read_table(spark, "ecosystem_health")
    except Exception as e:
        print(f"  ERROR: {e}  -- run VIZ-07 first.")
        return

    top_n = 5 if brief else 10

    print("""
  Health score (0-100) weights:
    velocity_score     30%  -- stars per repo per month
    growth_score       25%  -- month-over-month unique contributor growth
    maintenance_score  25%  -- push + PR + issue events per active repo
    diversity_score    20%  -- unique actors / total events
""")

    print(f"  Top {top_n} healthiest language ecosystems:")
    print(f"  {'#':<4} {'Language':<20} {'Health':>7} {'Velocity':>9} {'Growth':>8} {'Maint':>7} {'Diversity':>9}")
    print(f"  {'-'*4} {'-'*20} {'-'*7} {'-'*9} {'-'*8} {'-'*7} {'-'*9}")

    top = hlth.orderBy(F.col("health_score").desc()).limit(top_n).collect()
    for i, row in enumerate(top, 1):
        print(
            f"  {i:<4} {row['language']:<20} "
            f"{row['health_score']:>7.1f} "
            f"{row['velocity_score']:>9.1f} "
            f"{row['growth_score']:>8.1f} "
            f"{row['maintenance_score']:>7.1f} "
            f"{row['diversity_score']:>9.1f}"
        )

    print(f"\n  Bottom 5 languages by health score (among scored languages):")
    print(f"  {'#':<4} {'Language':<20} {'Health':>7} {'Repos':>8} {'Contributors':>14}")
    print(f"  {'-'*4} {'-'*20} {'-'*7} {'-'*8} {'-'*14}")

    bottom = hlth.orderBy(F.col("health_score").asc()).limit(5).collect()
    for i, row in enumerate(bottom, 1):
        print(
            f"  {i:<4} {row['language']:<20} "
            f"{row['health_score']:>7.1f} "
            f"{row['total_repos']:>8,} "
            f"{row['total_actors']:>14,}"
        )


# ---------------------------------------------------------------------------
# Section 5: Language Velocity Rankings + Inflection Points (VIZ-08)
# ---------------------------------------------------------------------------
def section_velocity(spark, brief):
    section("SECTION 5: LANGUAGE VELOCITY RANKINGS  (VIZ-08)")

    try:
        ranks = read_table(spark, "language_rankings")
        vel   = read_table(spark, "language_velocity")
    except Exception as e:
        print(f"  ERROR: {e}  -- run VIZ-08 first.")
        return

    print("\n  Top 10 RISING languages (by YoY star growth rate, 2023-2025):")
    print(f"  {'Rank':<6} {'Language':<22} {'YoY Growth':>12} {'Total Stars':>13}")
    print(f"  {'-'*6} {'-'*22} {'-'*12} {'-'*13}")

    rising = (
        ranks.filter(F.col("direction") == "rising")
        .orderBy(F.col("velocity_rank").asc())
        .limit(10)
        .collect()
    )
    for row in rising:
        pct = row["yoy_growth_rate"] * 100
        print(
            f"  {row['velocity_rank']:<6} {row['language']:<22} "
            f"{pct:>+11.1f}% {row['total_stars']:>13,}"
        )

    print("\n  Top 10 DECLINING languages (by YoY star growth rate, 2023-2025):")
    print(f"  {'Rank':<6} {'Language':<22} {'YoY Growth':>12} {'Total Stars':>13}")
    print(f"  {'-'*6} {'-'*22} {'-'*12} {'-'*13}")

    declining = (
        ranks.filter(F.col("direction") == "declining")
        .orderBy(F.col("velocity_rank").asc())
        .limit(10)
        .collect()
    )
    for row in declining:
        pct = row["yoy_growth_rate"] * 100
        print(
            f"  {row['velocity_rank']:<6} {row['language']:<22} "
            f"{pct:>+11.1f}% {row['total_stars']:>13,}"
        )

    # Inflection point annotation
    print("\n  Inflection points -- months of peak star activity per rising language:")
    print(f"  {'Language':<22} {'Peak Month':<12} {'Stars':>8}  Real-world event")
    print(f"  {'-'*22} {'-'*12} {'-'*8}  {'-'*30}")

    rising_langs = [r["language"] for r in rising[:5]]

    peak_rows = (
        vel
        .filter(F.col("language").isin(rising_langs))
        .groupBy("language")
        .agg(
            F.max_by("year_month", "star_count").alias("peak_month"),
            F.max("star_count").alias("peak_stars"),
        )
        .collect()
    )

    for row in peak_rows:
        month  = row["peak_month"] or ""
        stars  = row["peak_stars"] or 0
        event  = KNOWN_EVENTS.get(month, "no correlated event recorded")
        print(f"  {row['language']:<22} {month:<12} {stars:>8,}  {event}")


# ---------------------------------------------------------------------------
# Section 6: Viral Repo Case Studies (ML-07)
# ---------------------------------------------------------------------------
def section_case_studies(spark, brief):
    section("SECTION 6: VIRAL REPO CASE STUDIES  (ML-07)")

    print("""
  The Random Forest model (ML-05, 200 trees, depth 10) was scored
  against the 2025 holdout set at T+48h -- 48 hours after each repo
  was created. Repos predicted viral that actually reached 1000 stars
  within 90 days are true positives (TP).

  ML-07 selects the top case studies ranked by how many days early the
  model called it before the repo publicly trended.
""")

    try:
        case_studies = spark.read.parquet(CASE_STUDIES_PATH)
    except AnalysisException:
        print("  ML-07 case studies not found at:")
        print(f"    {CASE_STUDIES_PATH}")
        print("  Run ML-07 first: docker exec spark-master spark-submit ...")
        print("  Skipping this section.")
        return

    rows = case_studies.orderBy(F.col("lead_days").desc()).collect()

    if not rows:
        print("  No case studies available -- ML-07 output is empty.")
        print("  This requires 2025 holdout data with at least 90 days of events.")
        return

    for i, row in enumerate(rows, 1):
        repo      = row["repo_name"]
        t0        = str(row["t0"])[:10]
        pred_ts   = str(row["prediction_ts"])[:10]
        t_viral   = str(row["t_viral"])[:10]
        lead_days = float(row["lead_days"])
        stars     = int(row.get("stars_90d", 0) or 0)

        print(f"  Case Study #{i}: {repo}")
        print(f"    Repo created        : {t0}")
        print(f"    Model flagged viral : {pred_ts}  (T+48h)")
        print(f"    Actually trended    : {t_viral}")
        print(f"    Lead time           : {lead_days:.1f} days before public trending")
        if stars:
            print(f"    Stars (90 days)     : {stars:,}")
        print(f"    Insight             : Our model identified {repo} as likely viral")
        print(f"                          {lead_days:.1f} days before it publicly trended,")
        print( "                          demonstrating actionable early-detection capability.")
        print()
        divider("-", 70)


# ---------------------------------------------------------------------------
# Section 7: Key Takeaways
# ---------------------------------------------------------------------------
def section_takeaways(spark, brief):
    section("SECTION 7: KEY TAKEAWAYS AND BUSINESS CASE")

    print("""
  1. GEOGRAPHIC SHIFT
     Open source contribution is decentralizing. Beyond the US and
     Western Europe, significant growth is coming from Southeast Asia,
     India, and Latin America. Platforms that serve these communities
     with localized tooling and low-latency infrastructure will capture
     the next wave of developers.

  2. LANGUAGE ECOSYSTEM COUPLING
     PMI analysis reveals that language choices cluster into ecosystems:
     systems languages (C/C++/Rust/Go), web stacks (JS/TS/HTML/CSS),
     and AI/data stacks (Python/Jupyter). These clusters are stable --
     developers specialize within ecosystems rather than jumping across.
     Tooling, hiring, and curriculum design should target whole clusters,
     not individual languages.

  3. ECOSYSTEM HEALTH AS SIGNAL
     High health scores (velocity + growth + maintenance + diversity) are
     predictive of sustained adoption. Languages that score high on
     diversity in particular show more resilient growth -- they are not
     dependent on a small set of power users or a single company's
     investment.

  4. VELOCITY TRENDS
     AI/ML tooling and systems languages are the fastest-rising segment.
     The sharpest inflection points correlate with major AI model releases
     (GPT-4, DeepSeek R1, Llama 2), suggesting the developer ecosystem
     responds faster to model releases than to language version releases.
     Older scripting languages show steady decline as developers migrate
     to statically typed and AI-native alternatives.

  5. EARLY VIRAL DETECTION
     The Random Forest model (PR-AUC evaluated on 2025 holdout) flags
     viral repositories at T+48h -- before they publicly trend. This
     has direct commercial value: content platforms, developer tool
     vendors, and VC firms can act on early signals rather than chasing
     lagging indicators like trending pages or media coverage.

  6. PIPELINE RELIABILITY
     The Lambda architecture (batch + streaming) ran end-to-end with a
     4-person student team in 5 weeks. The streaming path (Kafka +
     PySpark Structured Streaming + InfluxDB) sustains live Grafana
     panels with 30-second refresh. The batch path (GHArchive +
     HDFS + PySpark + PostgreSQL) handles 120 GB of historical data
     with reproducible Airflow DAGs for daily and weekly refresh.
""")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description="VIZ-09: Final Analytical Report")
    p.add_argument("--brief", action="store_true",
                   help="Shorter output -- 5 rows per table instead of 10")
    return p.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("VIZ-09: Final Analytical Report")
        .config("spark.sql.shuffle.partitions", "50")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    logger.info("=" * 60)
    logger.info("VIZ-09: Final Analytical Report")
    logger.info(f"  brief mode : {args.brief}")
    logger.info("=" * 60)

    print()
    divider("=")
    print("  GITHUB DEVELOPER ECOSYSTEM ANALYTICS")
    print("  Final Report -- VIZ-09")
    print("  NYU Tandon Big Data Systems -- Spring 2026")
    print("  Authors: Hariharan L, Shreyas Kaldate, Tanusha Karnam, Vikram Markali")
    divider("=")

    section_overview(spark, args.brief)
    section_geo(spark, args.brief)
    section_pmi(spark, args.brief)
    section_health(spark, args.brief)
    section_velocity(spark, args.brief)
    section_case_studies(spark, args.brief)
    section_takeaways(spark, args.brief)

    print()
    divider("=")
    print("  END OF REPORT")
    divider("=")
    print()

    spark.stop()
    logger.info("VIZ-09 complete.")


if __name__ == "__main__":
    main()
