from __future__ import annotations

import duckdb


def seed_sample_data(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("DELETE FROM fact_royalty;")
    conn.execute("DELETE FROM dim_track;")
    conn.execute("DELETE FROM dim_artist;")
    conn.execute("DELETE FROM dim_platform;")
    conn.execute("DELETE FROM dim_date;")

    conn.executemany(
        "INSERT INTO dim_artist (artist_id, artist_name) VALUES (?, ?)",
        [
            (1, "The Echoes"),
            (2, "Lunar Keys"),
        ],
    )

    conn.executemany(
        "INSERT INTO dim_track (track_id, track_title, artist_id, isrc) VALUES (?, ?, ?, ?)",
        [
            (101, "Midnight Signal", 1, "US-R1L-24-00001"),
            (102, "Velvet Circuit", 1, "US-R1L-24-00002"),
            (201, "Solar Waltz", 2, "US-R1L-24-00003"),
        ],
    )

    conn.executemany(
        "INSERT INTO dim_platform (platform_id, platform_name) VALUES (?, ?)",
        [
            (1, "Spotify"),
            (2, "Apple Music"),
        ],
    )

    conn.executemany(
        "INSERT INTO dim_date (date_id, full_date, year, month, day) VALUES (?, ?, ?, ?, ?)",
        [
            (20240101, "2024-01-01", 2024, 1, 1),
            (20240201, "2024-02-01", 2024, 2, 1),
            (20240301, "2024-03-01", 2024, 3, 1),
        ],
    )

    conn.executemany(
        """
        INSERT INTO fact_royalty
        (royalty_id, track_id, platform_id, date_id, streams, royalty_amount, currency)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (1, 101, 1, 20240101, 120000, 480.00, "USD"),
            (2, 102, 1, 20240101, 90000, 360.00, "USD"),
            (3, 201, 2, 20240201, 70000, 350.00, "USD"),
            (4, 101, 2, 20240301, 80000, 400.00, "USD"),
            (5, 201, 1, 20240301, 50000, 200.00, "USD"),
        ],
    )
