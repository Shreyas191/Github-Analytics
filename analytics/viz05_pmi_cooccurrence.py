"""
VIZ-05: PMI Language Co-occurrence Computation
GitHub Developer Ecosystem Analytics
Author: Vikram Markali (vrm9190)

What this does:
  Computes Pointwise Mutual Information (PMI) between programming language pairs
  based on which developers work across multiple languages.

  Co-occurrence definition:
    If actor X contributes to repos written in both Python and JavaScript,
    that is one co-occurrence of the pair (JavaScript, Python).
    PMI measures whether this pairing happens more than random chance would predict.

  PMI formula:
    PMI(A, B) = log2( P(A, B) / (P(A) * P(B)) )
    where:
      P(A)    = fraction of actors who worked in language A
      P(B)    = fraction of actors who worked in language B
      P(A, B) = fraction of actors who worked in BOTH A and B

  A high positive PMI means developers who use language A are far more likely
  than average to also use language B -- they form a natural ecosystem pair.

  Minimum threshold: 500 co-occurrences (filters noisy rare pairs).
  Output: ~50-150 edges covering the core developer ecosystem graph.

  Also exports a JSON file consumed by VIZ-06 D3.js force-directed graph.

Input:
  hdfs://namenode:9000/github/events/parquet/

Output:
  postgres-analytics: pmi_pairs table
  postgres-analytics: language_nodes table
  /tmp/viz05_pmi_graph.json  (consumed by viz06_pmi_graph.html)

Usage:
  # Apply schema first (one time):
  docker exec -i postgres-analytics psql -U analytics -d github_analytics \\
    < /opt/spark-apps/analytics/db_schema.sql

  # Sample run (fast, uses 10% of data):
  docker exec spark-master spark-submit \\
    --master spark://spark-master:7077 \\
    --packages org.postgresql:postgresql:42.6.0 \\
    /opt/spark-apps/analytics/viz05_pmi_cooccurrence.py --sample

  # Full run:
  docker exec spark-master spark-submit \\
    --master spark://spark-master:7077 \\
    --packages org.postgresql:postgresql:42.6.0 \\
    /opt/spark-apps/analytics/viz05_pmi_cooccurrence.py
"""

import argparse
import json
import logging
import math

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("viz05.pmi_cooccurrence")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
EVENTS_PATH       = "hdfs://namenode:9000/github/events/parquet"
JDBC_URL          = "jdbc:postgresql://postgres-analytics:5432/github_analytics"
JDBC_PROPS        = {"user": "analytics", "password": "analytics", "driver": "org.postgresql.Driver"}
JSON_OUTPUT_PATH  = "/tmp/viz05_pmi_graph.json"
MIN_COOCCURRENCE  = 500     # filter noisy pairs below this threshold
MIN_LANG_ACTORS   = 100     # ignore niche languages with fewer than 100 contributors
TOP_LANGUAGES     = 50      # only consider top N languages by actor count


def build_spark(sample: bool) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("VIZ-05: PMI Language Co-occurrence")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_actor_language_pairs(spark: SparkSession, sample: bool):
    """
    Build (actor_id, language) pairs from the events table.

    Language comes from the CreateEvent 'language' field -- this is the primary
    language GitHub assigns to a repo at creation time. It's the most reliable
    source since it comes from GitHub's linguist analysis.

    A single actor can appear multiple times with the same language (if they
    contributed to many repos of the same type). We distinct on (actor_id, language)
    so each pair is counted once per actor.
    """
    logger.info("Loading actor-language pairs from events Parquet...")

    events = spark.read.parquet(EVENTS_PATH)

    if sample:
        events = events.sample(fraction=0.10, seed=42)
        logger.info("Sample mode: using 10% of events.")

    # Use all event types to capture language signal -- not just CreateEvent.
    # A WatchEvent, ForkEvent, or PushEvent on a Python repo also signals
    # the actor's engagement with Python.
    actor_lang = (
        events
        .filter(F.col("language").isNotNull())
        .filter(F.col("language") != "unknown")
        .select("actor_id", "language")
        .distinct()
    )

    total_pairs = actor_lang.count()
    logger.info(f"Distinct (actor, language) pairs: {total_pairs:,}")
    return actor_lang


def filter_top_languages(actor_lang, top_n: int):
    """
    Keep only the top N languages by actor count.
    Filters out niche/one-off languages that would produce noisy PMI scores.
    """
    lang_actor_counts = (
        actor_lang
        .groupBy("language")
        .agg(F.countDistinct("actor_id").alias("actor_count"))
        .filter(F.col("actor_count") >= MIN_LANG_ACTORS)
        .orderBy(F.col("actor_count").desc())
        .limit(top_n)
    )

    top_langs = {row["language"] for row in lang_actor_counts.collect()}
    logger.info(f"Top {len(top_langs)} languages retained after filtering.")

    filtered = actor_lang.filter(F.col("language").isin(top_langs))
    return filtered, lang_actor_counts


def compute_pmi(actor_lang, lang_actor_counts, total_actors: int):
    """
    Compute PMI for all language pairs.

    Steps:
      1. Self-join actor_lang on actor_id to get all (lang_a, lang_b, actor_id) triples.
      2. Keep only pairs where lang_a < lang_b (avoid duplicates and self-pairs).
      3. Count distinct actors per pair = co-occurrence count.
      4. Filter by MIN_COOCCURRENCE.
      5. Join individual language actor counts.
      6. Apply PMI formula.
    """
    logger.info("Computing language pair co-occurrences...")

    # Self-join to get pairs
    pairs = (
        actor_lang.alias("a")
        .join(actor_lang.alias("b"), on="actor_id")
        .filter(F.col("a.language") < F.col("b.language"))
        .select(
            F.col("a.language").alias("lang_a"),
            F.col("b.language").alias("lang_b"),
            F.col("actor_id"),
        )
    )

    cooccurrence = (
        pairs
        .groupBy("lang_a", "lang_b")
        .agg(F.countDistinct("actor_id").alias("cooccurrence"))
        .filter(F.col("cooccurrence") >= MIN_COOCCURRENCE)
    )

    n = total_actors  # total unique actors across all languages

    # Join individual counts for PMI denominator
    count_a = lang_actor_counts.withColumnRenamed("language", "lang_a") \
                                .withColumnRenamed("actor_count", "count_a")
    count_b = lang_actor_counts.withColumnRenamed("language", "lang_b") \
                                .withColumnRenamed("actor_count", "count_b")

    pmi_df = (
        cooccurrence
        .join(count_a, on="lang_a")
        .join(count_b, on="lang_b")
        .withColumn(
            "pmi_score",
            F.log2(
                (F.col("cooccurrence") / F.lit(n)) /
                ((F.col("count_a") / F.lit(n)) * (F.col("count_b") / F.lit(n)))
            ).cast(DoubleType())
        )
        .select("lang_a", "lang_b", "cooccurrence", "pmi_score")
        .orderBy(F.col("pmi_score").desc())
    )

    pair_count = pmi_df.count()
    logger.info(f"Language pairs after threshold: {pair_count:,}")
    return pmi_df


def write_postgres(pmi_df, lang_actor_counts, spark: SparkSession):
    """Write PMI pairs and language nodes to postgres-analytics."""
    from pyspark.sql import functions as F

    # pmi_pairs table
    pmi_df.withColumn("computed_at", F.current_timestamp()) \
          .write \
          .mode("overwrite") \
          .option("truncate", "true") \
          .jdbc(JDBC_URL, "pmi_pairs", properties=JDBC_PROPS)
    logger.info(f"Wrote {pmi_df.count():,} rows to pmi_pairs.")

    # language_nodes table -- include repo count for node sizing in D3.js
    lang_actor_counts.withColumnRenamed("actor_count", "actor_count") \
                     .withColumn("repo_count", F.col("actor_count"))  \
                     .withColumn("computed_at", F.current_timestamp()) \
                     .write \
                     .mode("overwrite") \
                     .option("truncate", "true") \
                     .jdbc(JDBC_URL, "language_nodes", properties=JDBC_PROPS)
    logger.info("Wrote language_nodes.")


def export_json(pmi_df, lang_actor_counts):
    """
    Export PMI graph as JSON for VIZ-06 D3.js visualization.
    Format:
      { "nodes": [{id, actor_count}, ...],
        "links": [{source, target, pmi_score, cooccurrence}, ...] }
    """
    nodes_rows = lang_actor_counts.collect()
    links_rows = pmi_df.collect()

    # Normalize actor_count for D3 node radius (range 5-30)
    max_actors = max(r["actor_count"] for r in nodes_rows) if nodes_rows else 1
    nodes = [
        {
            "id":          r["language"],
            "actor_count": r["actor_count"],
            "radius":      5 + int(25 * r["actor_count"] / max_actors),
        }
        for r in nodes_rows
    ]

    # Normalize PMI for edge thickness (range 0.5-4)
    pmi_vals = [r["pmi_score"] for r in links_rows if r["pmi_score"] is not None]
    max_pmi = max(pmi_vals) if pmi_vals else 1.0
    min_pmi = min(pmi_vals) if pmi_vals else 0.0
    pmi_range = max_pmi - min_pmi or 1.0

    links = [
        {
            "source":       r["lang_a"],
            "target":       r["lang_b"],
            "pmi_score":    round(r["pmi_score"], 4) if r["pmi_score"] else 0,
            "cooccurrence": r["cooccurrence"],
            "thickness":    0.5 + 3.5 * (r["pmi_score"] - min_pmi) / pmi_range,
        }
        for r in links_rows
        if r["pmi_score"] is not None
    ]

    graph = {"nodes": nodes, "links": links}
    with open(JSON_OUTPUT_PATH, "w") as f:
        json.dump(graph, f, indent=2)
    logger.info(f"PMI graph JSON written to {JSON_OUTPUT_PATH} "
                f"({len(nodes)} nodes, {len(links)} edges)")


def print_summary(pmi_df, lang_actor_counts):
    print("\n" + "=" * 60)
    print("VIZ-05 PMI CO-OCCURRENCE SUMMARY")
    print("=" * 60)
    print(f"\nTop 20 language pairs by PMI score:")
    pmi_df.show(20, truncate=False)
    print(f"\nTop 20 languages by actor count:")
    lang_actor_counts.orderBy(F.col("actor_count").desc()).show(20, truncate=False)
    print("=" * 60)


def parse_args():
    p = argparse.ArgumentParser(description="VIZ-05: PMI Language Co-occurrence")
    p.add_argument("--sample", action="store_true",
                   help="Use 10% sample of events for fast testing")
    return p.parse_args()


def main():
    args = parse_args()

    spark = build_spark(args.sample)

    logger.info("=" * 60)
    logger.info("VIZ-05: PMI Language Co-occurrence")
    logger.info(f"  sample mode         : {args.sample}")
    logger.info(f"  min co-occurrence   : {MIN_COOCCURRENCE:,}")
    logger.info(f"  top languages       : {TOP_LANGUAGES}")
    logger.info("=" * 60)

    actor_lang  = load_actor_language_pairs(spark, args.sample)
    actor_lang, lang_actor_counts = filter_top_languages(actor_lang, TOP_LANGUAGES)

    total_actors = actor_lang.select("actor_id").distinct().count()
    logger.info(f"Total unique actors across top languages: {total_actors:,}")

    actor_lang.cache()

    pmi_df = compute_pmi(actor_lang, lang_actor_counts, total_actors)
    pmi_df.cache()

    write_postgres(pmi_df, lang_actor_counts, spark)
    export_json(pmi_df, lang_actor_counts)
    print_summary(pmi_df, lang_actor_counts)

    spark.stop()
    logger.info("VIZ-05 complete.")


if __name__ == "__main__":
    main()
