from __future__ import annotations

import re
from typing import Any

import duckdb


class NaturalLanguageSQLEngine:
    _BLOCKED_SQL = re.compile(r"\b(insert|update|delete|drop|alter|create|attach|detach|copy|pragma|call)\b", re.I)

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self.conn = conn

    def generate_sql(self, question: str) -> str:
        text = question.strip()
        normalized = text.lower()

        if normalized.startswith("sql:"):
            return text[4:].strip()

        if "total" in normalized and "artist" in normalized:
            return """
                SELECT a.artist_name, ROUND(SUM(f.royalty_amount), 2) AS total_royalty_usd
                FROM fact_royalty f
                JOIN dim_track t ON t.track_id = f.track_id
                JOIN dim_artist a ON a.artist_id = t.artist_id
                GROUP BY a.artist_name
                ORDER BY total_royalty_usd DESC
            """

        if "top" in normalized and "track" in normalized:
            limit_match = re.search(r"top\s+(\d+)", normalized)
            limit = int(limit_match.group(1)) if limit_match else 5
            limit = max(1, min(limit, 100))
            return f"""
                SELECT t.track_title, a.artist_name, ROUND(SUM(f.royalty_amount), 2) AS total_royalty_usd
                FROM fact_royalty f
                JOIN dim_track t ON t.track_id = f.track_id
                JOIN dim_artist a ON a.artist_id = t.artist_id
                GROUP BY t.track_title, a.artist_name
                ORDER BY total_royalty_usd DESC
                LIMIT {limit}
            """

        if "monthly" in normalized or "trend" in normalized:
            return """
                SELECT DATE_TRUNC('month', d.full_date) AS month,
                       ROUND(SUM(f.royalty_amount), 2) AS total_royalty_usd
                FROM fact_royalty f
                JOIN dim_date d ON d.date_id = f.date_id
                GROUP BY 1
                ORDER BY 1
            """

        if "platform" in normalized:
            return """
                SELECT p.platform_name,
                       ROUND(SUM(f.royalty_amount), 2) AS total_royalty_usd,
                       SUM(f.streams) AS total_streams
                FROM fact_royalty f
                JOIN dim_platform p ON p.platform_id = f.platform_id
                GROUP BY p.platform_name
                ORDER BY total_royalty_usd DESC
            """

        raise ValueError(
            "Unsupported question. Try asking about artist totals, top tracks, monthly trends, or platform performance."
        )

    def validate_sql(self, sql: str) -> None:
        compact = sql.strip().rstrip(";")
        if not compact.lower().startswith(("select", "with")):
            raise ValueError("Only SELECT/CTE queries are allowed.")
        if self._BLOCKED_SQL.search(compact):
            raise ValueError("Unsafe SQL detected.")
        if ";" in compact:
            raise ValueError("Multiple statements are not allowed.")

    def ask(self, question: str) -> list[dict[str, Any]]:
        sql = self.generate_sql(question)
        self.validate_sql(sql)
        cursor = self.conn.execute(sql)
        columns = [c[0] for c in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
