-- PostgreSQL Analytics Schema
-- GitHub Developer Ecosystem Analytics
-- Author: Vikram Markali (vrm9190)
--
-- All tables written by VIZ-04 through VIZ-08 PySpark jobs.
-- Run once against postgres-analytics before running any VIZ script.
--
-- Usage:
--   docker exec -i postgres-analytics psql -U analytics -d github_analytics \
--     < analytics/db_schema.sql

-- ===========================================================================
-- VIZ-04: Geographic Contribution Heatmap
-- One row per (year, country). Used by Grafana Geomap panel.
-- ===========================================================================
DROP TABLE IF EXISTS geo_contributions;
CREATE TABLE geo_contributions (
    year          SMALLINT     NOT NULL,
    country_code  CHAR(2)      NOT NULL,
    country_name  VARCHAR(100) NOT NULL,
    actor_count   BIGINT       NOT NULL DEFAULT 0,
    event_count   BIGINT       NOT NULL DEFAULT 0,
    computed_at   TIMESTAMP    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (year, country_code)
);

CREATE INDEX idx_geo_country ON geo_contributions (country_code);
CREATE INDEX idx_geo_year    ON geo_contributions (year);

-- ===========================================================================
-- VIZ-05: PMI Language Co-occurrence Pairs
-- One row per ordered language pair (lang_a < lang_b alphabetically).
-- Used by Grafana table panel and VIZ-06 D3.js graph.
-- ===========================================================================
DROP TABLE IF EXISTS pmi_pairs;
CREATE TABLE pmi_pairs (
    lang_a        VARCHAR(100)     NOT NULL,
    lang_b        VARCHAR(100)     NOT NULL,
    cooccurrence  BIGINT           NOT NULL,
    pmi_score     DOUBLE PRECISION NOT NULL,
    computed_at   TIMESTAMP        NOT NULL DEFAULT NOW(),
    PRIMARY KEY (lang_a, lang_b)
);

CREATE INDEX idx_pmi_score ON pmi_pairs (pmi_score DESC);
CREATE INDEX idx_pmi_lang_a ON pmi_pairs (lang_a);
CREATE INDEX idx_pmi_lang_b ON pmi_pairs (lang_b);

-- ===========================================================================
-- VIZ-05: Language Node Metadata (for D3.js graph sizing)
-- One row per language with total actor count and repo count.
-- ===========================================================================
DROP TABLE IF EXISTS language_nodes;
CREATE TABLE language_nodes (
    language      VARCHAR(100)     NOT NULL PRIMARY KEY,
    actor_count   BIGINT           NOT NULL,
    repo_count    BIGINT           NOT NULL,
    computed_at   TIMESTAMP        NOT NULL DEFAULT NOW()
);

-- ===========================================================================
-- VIZ-07: Ecosystem Health Score
-- One row per language. Composite score 0-100.
-- ===========================================================================
DROP TABLE IF EXISTS ecosystem_health;
CREATE TABLE ecosystem_health (
    language          VARCHAR(100)     NOT NULL PRIMARY KEY,
    velocity_score    DOUBLE PRECISION NOT NULL,
    growth_score      DOUBLE PRECISION NOT NULL,
    maintenance_score DOUBLE PRECISION NOT NULL,
    diversity_score   DOUBLE PRECISION NOT NULL,
    health_score      DOUBLE PRECISION NOT NULL,
    total_repos       BIGINT           NOT NULL,
    total_actors      BIGINT           NOT NULL,
    computed_at       TIMESTAMP        NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_health_score ON ecosystem_health (health_score DESC);

-- ===========================================================================
-- VIZ-08: Language Velocity -- Monthly Snapshot
-- One row per (language, year_month). Used for time series and rankings.
-- ===========================================================================
DROP TABLE IF EXISTS language_velocity;
CREATE TABLE language_velocity (
    language       VARCHAR(100) NOT NULL,
    year_month     CHAR(7)      NOT NULL,   -- e.g. '2024-03'
    new_repo_count BIGINT       NOT NULL DEFAULT 0,
    star_count     BIGINT       NOT NULL DEFAULT 0,
    fork_count     BIGINT       NOT NULL DEFAULT 0,
    push_count     BIGINT       NOT NULL DEFAULT 0,
    actor_count    BIGINT       NOT NULL DEFAULT 0,
    computed_at    TIMESTAMP    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (language, year_month)
);

CREATE INDEX idx_vel_language   ON language_velocity (language);
CREATE INDEX idx_vel_year_month ON language_velocity (year_month);

-- ===========================================================================
-- VIZ-08: Language Velocity Rankings
-- Top 10 rising and declining languages overall (2023-2025 aggregate).
-- ===========================================================================
DROP TABLE IF EXISTS language_rankings;
CREATE TABLE language_rankings (
    language          VARCHAR(100)     NOT NULL PRIMARY KEY,
    total_repos       BIGINT           NOT NULL,
    total_stars       BIGINT           NOT NULL,
    yoy_growth_rate   DOUBLE PRECISION NOT NULL,   -- year-over-year % change
    velocity_rank     INTEGER          NOT NULL,
    direction         VARCHAR(10)      NOT NULL,   -- 'rising' or 'declining'
    computed_at       TIMESTAMP        NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_rank_direction ON language_rankings (direction, velocity_rank);

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO analytics;
