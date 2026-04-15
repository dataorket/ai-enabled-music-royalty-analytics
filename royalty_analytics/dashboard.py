from __future__ import annotations

import duckdb


def build_dashboard_payload(conn: duckdb.DuckDBPyConnection) -> dict:
    kpi = conn.execute(
        """
        SELECT ROUND(SUM(royalty_amount), 2) AS total_royalty_usd,
               SUM(streams) AS total_streams,
               COUNT(DISTINCT track_id) AS active_tracks
        FROM fact_royalty
        """
    ).fetchone()

    monthly = conn.execute(
        """
        SELECT DATE_TRUNC('month', d.full_date) AS month,
               ROUND(SUM(f.royalty_amount), 2) AS total_royalty_usd
        FROM fact_royalty f
        JOIN dim_date d ON d.date_id = f.date_id
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchall()

    platform = conn.execute(
        """
        SELECT p.platform_name, ROUND(SUM(f.royalty_amount), 2) AS total_royalty_usd
        FROM fact_royalty f
        JOIN dim_platform p ON p.platform_id = f.platform_id
        GROUP BY 1
        ORDER BY 2 DESC
        """
    ).fetchall()

    monthly_by_platform = conn.execute(
        """
        SELECT DATE_TRUNC('month', d.full_date) AS month,
               p.platform_name,
               ROUND(SUM(f.royalty_amount), 2) AS total_royalty_usd
        FROM fact_royalty f
        JOIN dim_date d ON d.date_id = f.date_id
        JOIN dim_platform p ON p.platform_id = f.platform_id
        GROUP BY 1, 2
        ORDER BY 1, 2
        """
    ).fetchall()

    return {
        "kpis": {
            "total_royalty_usd": float(kpi[0] or 0),
            "total_streams": int(kpi[1] or 0),
            "active_tracks": int(kpi[2] or 0),
        },
        "monthly_royalty_trend": [{"month": str(row[0]), "total_royalty_usd": float(row[1])} for row in monthly],
        "platform_split": [{"platform_name": row[0], "total_royalty_usd": float(row[1])} for row in platform],
        "monthly_by_platform": [
            {"month": str(row[0]), "platform_name": row[1], "total_royalty_usd": float(row[2])} for row in monthly_by_platform
        ],
    }
