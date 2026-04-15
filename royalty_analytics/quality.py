from __future__ import annotations

import duckdb


def run_data_quality_checks(conn: duckdb.DuckDBPyConnection) -> list[str]:
    failures: list[str] = []

    if conn.execute("SELECT COUNT(*) FROM fact_royalty").fetchone()[0] == 0:
        failures.append("fact_royalty has no records")

    if conn.execute("SELECT COUNT(*) FROM fact_royalty WHERE royalty_amount < 0").fetchone()[0] > 0:
        failures.append("negative royalty_amount detected")

    orphaned = conn.execute(
        """
        SELECT COUNT(*)
        FROM fact_royalty f
        LEFT JOIN dim_track t ON t.track_id = f.track_id
        LEFT JOIN dim_platform p ON p.platform_id = f.platform_id
        LEFT JOIN dim_date d ON d.date_id = f.date_id
        WHERE t.track_id IS NULL OR p.platform_id IS NULL OR d.date_id IS NULL
        """
    ).fetchone()[0]
    if orphaned > 0:
        failures.append("orphaned foreign keys detected in fact_royalty")

    return failures
