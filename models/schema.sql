-- ============================================================
-- Music Royalty Analytics — Star Schema DDL
-- Engine: DuckDB (Snowflake-compatible SQL dialect)
-- ============================================================
-- This DDL uses Snowflake-compatible types & syntax so it can
-- be ported to Snowflake with minimal changes (swap DuckDB
-- file paths for Snowflake stages).
-- ============================================================

-- ── Dimensions ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_artists (
    artist_id       INTEGER PRIMARY KEY,
    artist_name     VARCHAR NOT NULL,
    role            VARCHAR,          -- Songwriter, Composer, Lyricist, …
    society         VARCHAR,          -- Collecting society (GEMA, PRS, …)
    country_of_origin VARCHAR(2),
    created_at      DATE
);

CREATE TABLE IF NOT EXISTS dim_works (
    work_id             INTEGER PRIMARY KEY,
    iswc                VARCHAR,      -- International Standard Work Code
    title               VARCHAR NOT NULL,
    genre               VARCHAR,
    primary_artist_id   INTEGER REFERENCES dim_artists(artist_id),
    release_date        DATE,
    duration_seconds    INTEGER
);

CREATE TABLE IF NOT EXISTS dim_territories (
    territory_id    INTEGER PRIMARY KEY,
    iso_code        VARCHAR(2) NOT NULL,
    name            VARCHAR NOT NULL,
    region          VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_platforms (
    platform_id     INTEGER PRIMARY KEY,
    platform_name   VARCHAR NOT NULL,
    platform_type   VARCHAR           -- Streaming, Video, Broadcast, …
);

CREATE TABLE IF NOT EXISTS dim_dates (
    date_key        DATE PRIMARY KEY,
    year            INTEGER,
    quarter         INTEGER,
    month           INTEGER,
    month_name      VARCHAR,
    day_of_week     VARCHAR,
    is_weekend      BOOLEAN
);

-- ── Facts ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_streams (
    stream_id       VARCHAR PRIMARY KEY,
    work_id         INTEGER REFERENCES dim_works(work_id),
    territory_id    INTEGER REFERENCES dim_territories(territory_id),
    platform_id     INTEGER REFERENCES dim_platforms(platform_id),
    date_key        DATE    REFERENCES dim_dates(date_key),
    stream_count    BIGINT
);

CREATE TABLE IF NOT EXISTS fact_royalties (
    royalty_id          VARCHAR PRIMARY KEY,
    work_id             INTEGER REFERENCES dim_works(work_id),
    artist_id           INTEGER REFERENCES dim_artists(artist_id),
    territory_id        INTEGER REFERENCES dim_territories(territory_id),
    platform_id         INTEGER REFERENCES dim_platforms(platform_id),
    date_key            DATE,
    gross_royalty_eur   DECIMAL(14,2),
    commission_eur      DECIMAL(14,2),
    net_royalty_eur     DECIMAL(14,2),
    currency            VARCHAR(3) DEFAULT 'EUR',
    status              VARCHAR       -- Distributed, Pending, Disputed
);
