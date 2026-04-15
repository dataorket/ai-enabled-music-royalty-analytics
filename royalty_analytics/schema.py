from __future__ import annotations

import duckdb


def connect_database(path: str = ":memory:") -> duckdb.DuckDBPyConnection:
    return duckdb.connect(path)


def initialize_star_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_artist (
            artist_id INTEGER PRIMARY KEY,
            artist_name VARCHAR NOT NULL
        );

        CREATE TABLE IF NOT EXISTS dim_track (
            track_id INTEGER PRIMARY KEY,
            track_title VARCHAR NOT NULL,
            artist_id INTEGER NOT NULL,
            isrc VARCHAR,
            FOREIGN KEY (artist_id) REFERENCES dim_artist(artist_id)
        );

        CREATE TABLE IF NOT EXISTS dim_platform (
            platform_id INTEGER PRIMARY KEY,
            platform_name VARCHAR NOT NULL
        );

        CREATE TABLE IF NOT EXISTS dim_date (
            date_id INTEGER PRIMARY KEY,
            full_date DATE NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            day INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS fact_royalty (
            royalty_id BIGINT PRIMARY KEY,
            track_id INTEGER NOT NULL,
            platform_id INTEGER NOT NULL,
            date_id INTEGER NOT NULL,
            streams BIGINT NOT NULL,
            royalty_amount DECIMAL(12,2) NOT NULL,
            currency VARCHAR NOT NULL DEFAULT 'USD',
            FOREIGN KEY (track_id) REFERENCES dim_track(track_id),
            FOREIGN KEY (platform_id) REFERENCES dim_platform(platform_id),
            FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
        );
        """
    )
