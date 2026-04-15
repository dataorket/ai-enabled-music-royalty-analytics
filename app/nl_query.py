"""
Natural-Language → SQL Query Engine

Converts plain-English questions about music royalties into SQL,
executes against DuckDB, and returns results.

Two modes:
  1. **LLM mode** (OpenAI / compatible API) — best quality
  2. **Template mode** — keyword-based fallback, no API key needed

This is the "AI-enabled analytics" feature — allowing users
to query data using natural language.
"""

import re
import duckdb
import pandas as pd
from decimal import Decimal
from pathlib import Path
from typing import Optional

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "warehouse.duckdb"

# ── Schema context fed to the LLM ────────────────────────────

SCHEMA_PROMPT = """
You are an expert SQL analyst for a music royalty data warehouse (DuckDB / Snowflake-compatible).

STAR SCHEMA:

dim_artists(artist_id PK, artist_name, role, society, country_of_origin, created_at)
dim_works(work_id PK, iswc, title, genre, primary_artist_id FK→dim_artists, release_date, duration_seconds)
dim_territories(territory_id PK, iso_code, name, region)
dim_platforms(platform_id PK, platform_name, platform_type)
dim_dates(date_key PK [DATE], year, quarter, month, month_name, day_of_week, is_weekend)

fact_streams(stream_id PK, work_id FK, territory_id FK, platform_id FK, date_key FK, stream_count)
fact_royalties(royalty_id PK, work_id FK, artist_id FK, territory_id FK, platform_id FK, date_key [DATE monthly],
               gross_royalty_eur, commission_eur, net_royalty_eur, currency, status)

PRE-BUILT VIEWS:
  v_royalties_by_artist(artist_id, artist_name, society, total_gross_eur, total_net_eur, txn_count)
  v_royalties_by_territory(territory_id, iso_code, territory_name, region, total_gross_eur, total_net_eur, txn_count)
  v_royalties_by_platform(platform_id, platform_name, platform_type, total_gross_eur, total_net_eur, txn_count)
  v_monthly_trend(year, month, total_gross_eur, total_net_eur, txn_count)
  v_streams_by_genre(genre, total_streams, record_count)
  v_top_works(work_id, title, genre, artist_name, total_net_eur, total_streams)

RULES:
- Write a single SELECT statement. No DDL/DML.
- Use DuckDB SQL dialect (Snowflake-compatible).
- Return at most 100 rows unless the user asks for more.
- If the question is ambiguous, make reasonable assumptions.
- Return ONLY the SQL, no explanation.
"""


# ── LLM-based NL→SQL ─────────────────────────────────────────

def _query_openai(question: str, api_key: str) -> str:
    """Call OpenAI (or compatible) to generate SQL from a question."""
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0,
            messages=[
                {"role": "system", "content": SCHEMA_PROMPT},
                {"role": "user", "content": question},
            ],
            max_tokens=512,
        )
        sql = resp.choices[0].message.content.strip()
        # Strip markdown fences if present
        sql = re.sub(r"^```(?:sql)?\s*", "", sql)
        sql = re.sub(r"\s*```$", "", sql)
        return sql
    except Exception as e:
        print(f"  ⚠ OpenAI call failed: {e}")
        return ""


# ── Template-based fallback ───────────────────────────────────

TEMPLATES = [
    {
        "patterns": [r"top\s+(\d+)\s+artist", r"highest.earning\s+artist"],
        "sql": "SELECT artist_name, total_net_eur FROM v_royalties_by_artist ORDER BY total_net_eur DESC LIMIT {limit}",
        "defaults": {"limit": "10"},
    },
    {
        "patterns": [r"royalt.*by\s+territor", r"royalt.*by\s+country", r"royalt.*per\s+country"],
        "sql": "SELECT territory_name, region, total_net_eur FROM v_royalties_by_territory ORDER BY total_net_eur DESC LIMIT 20",
        "defaults": {},
    },
    {
        "patterns": [r"royalt.*by\s+platform", r"platform\s+revenue", r"which\s+platform"],
        "sql": "SELECT platform_name, platform_type, total_net_eur FROM v_royalties_by_platform ORDER BY total_net_eur DESC",
        "defaults": {},
    },
    {
        "patterns": [r"monthly\s+trend", r"royalt.*over\s+time", r"trend"],
        "sql": "SELECT year, month, total_gross_eur, total_net_eur FROM v_monthly_trend ORDER BY year, month",
        "defaults": {},
    },
    {
        "patterns": [r"stream.*by\s+genre", r"genre\s+stream"],
        "sql": "SELECT genre, total_streams FROM v_streams_by_genre ORDER BY total_streams DESC",
        "defaults": {},
    },
    {
        "patterns": [r"top\s+(\d+)\s+(?:song|work|track)", r"most\s+(?:streamed|popular)\s+(?:song|work|track)"],
        "sql": "SELECT title, artist_name, genre, total_streams, total_net_eur FROM v_top_works ORDER BY total_streams DESC LIMIT {limit}",
        "defaults": {"limit": "10"},
    },
    {
        "patterns": [r"total\s+royalt", r"how\s+much.*royalt"],
        "sql": "SELECT SUM(net_royalty_eur) AS total_net_royalties_eur, SUM(gross_royalty_eur) AS total_gross_royalties_eur, COUNT(*) AS total_transactions FROM fact_royalties",
        "defaults": {},
    },
]

# ── Territory lookup ──────────────────────────────────────────
TERRITORY_MAP = {
    "germany": "DE", "german": "DE", "de": "DE",
    "uk": "GB", "united kingdom": "GB", "britain": "GB", "british": "GB", "gb": "GB",
    "france": "FR", "french": "FR", "fr": "FR",
    "sweden": "SE", "swedish": "SE", "se": "SE",
    "netherlands": "NL", "dutch": "NL", "nl": "NL",
    "spain": "ES", "spanish": "ES", "es": "ES",
    "italy": "IT", "italian": "IT", "it": "IT",
    "usa": "US", "united states": "US", "us": "US", "america": "US", "american": "US",
    "canada": "CA", "canadian": "CA", "ca": "CA",
    "mexico": "MX", "mexican": "MX", "mx": "MX",
    "brazil": "BR", "brazilian": "BR", "br": "BR",
    "argentina": "AR", "argentine": "AR", "ar": "AR",
    "japan": "JP", "japanese": "JP", "jp": "JP",
    "south korea": "KR", "korean": "KR", "kr": "KR",
    "australia": "AU", "australian": "AU", "au": "AU",
    "india": "IN", "indian": "IN",
    "nigeria": "NG", "nigerian": "NG", "ng": "NG",
    "south africa": "ZA", "za": "ZA",
    "egypt": "EG", "egyptian": "EG", "eg": "EG",
    "uae": "AE", "ae": "AE",
}

# ── Smart filter extraction ───────────────────────────────────

def _extract_filters(question: str) -> dict:
    """Parse the question for status, year, quarter, territory, limit, platform."""
    q = question.lower().strip()
    filters = {}

    # Status
    if re.search(r"\bdisputed\b", q):
        filters["status"] = "Disputed"
    elif re.search(r"\bpending\b", q):
        filters["status"] = "Pending"
    elif re.search(r"\bdistributed\b", q):
        filters["status"] = "Distributed"

    # Year  (4-digit number that looks like a year)
    year_m = re.search(r"\b(20[2-3]\d)\b", q)
    if year_m:
        filters["year"] = int(year_m.group(1))

    # Quarter
    quarter_m = re.search(r"\bq([1-4])\b", q)
    if quarter_m:
        filters["quarter"] = int(quarter_m.group(1))

    # Territory
    for name, code in TERRITORY_MAP.items():
        if re.search(r"\b" + re.escape(name) + r"\b", q):
            filters["territory"] = code
            break

    # Platform
    platform_names = ["spotify", "apple music", "youtube music", "amazon music",
                      "deezer", "tidal", "soundcloud", "pandora", "youtube",
                      "tiktok", "instagram", "fm radio", "tv sync"]
    for pname in platform_names:
        if pname in q:
            filters["platform"] = pname
            break

    # Limit (top N)
    limit_m = re.search(r"\btop\s+(\d+)\b", q)
    if limit_m:
        filters["limit"] = int(limit_m.group(1))

    # Genre
    genres = ["pop", "rock", "hip-hop", "hip hop", "electronic", "classical",
              "jazz", "latin", "r&b", "country", "afrobeats"]
    for g in genres:
        if g in q:
            filters["genre"] = g.title().replace("Hip Hop", "Hip-Hop").replace("R&b", "R&B")
            break

    # Artist name — detect "named X", "artist X", "by artist X", "called X"
    artist_m = re.search(r"(?:named|artist|called|by\s+artist|for\s+artist|singer)\s+[\"']?([A-Za-z\s\-]+?)[\"']?\s*$", question.strip())
    if not artist_m:
        # Also try "named X" mid-sentence
        artist_m = re.search(r"(?:named|artist|called)\s+[\"']?([A-Za-z\s\-]{2,30}?)[\"']?(?:\s+(?:in|by|for|from|during|on|at|$))", question.strip(), re.IGNORECASE)
    if artist_m:
        filters["artist_name"] = artist_m.group(1).strip()

    # Sort column — detect "by <column_name>"
    sort_m = re.search(r"\bby\s+(net_royalty_eur|gross_royalty_eur|stream_count|total_net_eur|total_gross_eur|total_streams)\b", q)
    if sort_m:
        filters["sort_col"] = sort_m.group(1)

    return filters


def _build_dynamic_sql(question: str, filters: dict) -> str | None:
    """Build a SQL query dynamically from extracted filters.
    
    Returns None if no meaningful filters are found (fall through to templates).
    """
    q = question.lower().strip()

    # ── Helper: inject artist_name filter ──
    def _add_artist_filter(joins: list, wheres: list, artist_alias: str = "a"):
        if "artist_name" in filters:
            name = filters["artist_name"].replace("'", "''")
            wheres.append(f"LOWER({artist_alias}.artist_name) LIKE '%{name.lower()}%'")

    # ── Helper: determine ORDER BY column ──
    def _order_col() -> str:
        col = filters.get("sort_col", "total_net_eur")
        # Map raw column names to aggregate aliases
        alias_map = {
            "net_royalty_eur": "total_net_eur",
            "gross_royalty_eur": "total_gross_eur",
            "total_net_eur": "total_net_eur",
            "total_gross_eur": "total_gross_eur",
        }
        return alias_map.get(col, "total_net_eur")

    # If the question is about status (disputed/pending/distributed)
    if "status" in filters:
        wheres = [f"r.status = '{filters['status']}'"]
        joins = ["JOIN dim_artists a ON r.artist_id = a.artist_id"]

        if "year" in filters or "quarter" in filters:
            joins.append("JOIN dim_dates d ON r.date_key = d.date_key")
            if "year" in filters:
                wheres.append(f"d.year = {filters['year']}")
            if "quarter" in filters:
                wheres.append(f"d.quarter = {filters['quarter']}")

        if "territory" in filters:
            joins.append("JOIN dim_territories t ON r.territory_id = t.territory_id")
            wheres.append(f"t.iso_code = '{filters['territory']}'")

        if "platform" in filters:
            joins.append("JOIN dim_platforms p ON r.platform_id = p.platform_id")
            wheres.append(f"LOWER(p.platform_name) = '{filters['platform']}'")

        _add_artist_filter(joins, wheres)
        join_clause = " ".join(joins)
        where_clause = " AND ".join(wheres)
        limit = filters.get("limit", 50)
        order = _order_col()

        # Decide grouping — if asking about artists or filtering by name, group by artist
        if re.search(r"artist|who|songwriter|composer", q) or "artist_name" in filters:
            return (
                f"SELECT a.artist_name, COUNT(*) AS txn_count, "
                f"SUM(r.net_royalty_eur) AS total_net_eur, SUM(r.gross_royalty_eur) AS total_gross_eur "
                f"FROM fact_royalties r "
                f"{join_clause} "
                f"WHERE {where_clause} "
                f"GROUP BY a.artist_name ORDER BY {order} DESC LIMIT {limit}"
            )
        else:
            return (
                f"SELECT r.status, COUNT(*) AS txn_count, "
                f"SUM(r.net_royalty_eur) AS total_net_eur, SUM(r.gross_royalty_eur) AS total_gross_eur "
                f"FROM fact_royalties r "
                f"{join_clause} "
                f"WHERE {where_clause} "
                f"GROUP BY r.status ORDER BY {order} DESC"
            )

    # Royalties filtered by territory / year / artist_name / genre — general royalty queries
    has_royalty_signal = (
        "territory" in filters
        or "artist_name" in filters
        or "genre" in filters
        or ("year" in filters and re.search(r"royalt|earn|revenue|paid|top|best|highest", q))
        or ("quarter" in filters and re.search(r"royalt|earn|revenue|paid|top|best|highest", q))
    )
    if has_royalty_signal:
        wheres = []
        joins = ["JOIN dim_artists a ON r.artist_id = a.artist_id"]

        if "territory" in filters:
            joins.append("JOIN dim_territories t ON r.territory_id = t.territory_id")
            wheres.append(f"t.iso_code = '{filters['territory']}'")

        if "year" in filters or "quarter" in filters:
            joins.append("JOIN dim_dates d ON r.date_key = d.date_key")
            if "year" in filters:
                wheres.append(f"d.year = {filters['year']}")
            if "quarter" in filters:
                wheres.append(f"d.quarter = {filters['quarter']}")

        if "status" in filters:
            wheres.append(f"r.status = '{filters['status']}'")

        if "genre" in filters:
            joins.append("JOIN dim_works w ON r.work_id = w.work_id")
            wheres.append(f"w.genre = '{filters['genre']}'")

        _add_artist_filter(joins, wheres)

        join_clause = " ".join(joins)
        where_clause = (" WHERE " + " AND ".join(wheres)) if wheres else ""
        limit = filters.get("limit", 20)
        order = _order_col()

        return (
            f"SELECT a.artist_name, COUNT(*) AS txn_count, "
            f"SUM(r.net_royalty_eur) AS total_net_eur, SUM(r.gross_royalty_eur) AS total_gross_eur "
            f"FROM fact_royalties r "
            f"{join_clause} "
            f"{where_clause} "
            f"GROUP BY a.artist_name ORDER BY {order} DESC LIMIT {limit}"
        )

    # Streams filtered by genre/territory/year
    if re.search(r"stream", q) and any(k in filters for k in ["genre", "territory", "year"]):
        wheres = []
        joins = []

        if "genre" in filters:
            joins.append("JOIN dim_works w ON s.work_id = w.work_id")
            wheres.append(f"w.genre = '{filters['genre']}'")

        if "territory" in filters:
            joins.append("JOIN dim_territories t ON s.territory_id = t.territory_id")
            wheres.append(f"t.iso_code = '{filters['territory']}'")

        if "year" in filters or "quarter" in filters:
            joins.append("JOIN dim_dates d ON s.date_key = d.date_key")
            if "year" in filters:
                wheres.append(f"d.year = {filters['year']}")
            if "quarter" in filters:
                wheres.append(f"d.quarter = {filters['quarter']}")

        join_clause = " ".join(joins)
        where_clause = (" WHERE " + " AND ".join(wheres)) if wheres else ""

        return (
            f"SELECT SUM(s.stream_count) AS total_streams, COUNT(*) AS records "
            f"FROM fact_streams s "
            f"{join_clause} "
            f"{where_clause}"
        )

    return None


def _query_template(question: str) -> str:
    """Match question to SQL — first try dynamic filter extraction, then static templates."""
    q = question.lower().strip()

    # 1) Extract filters and try dynamic SQL
    filters = _extract_filters(question)
    if filters:
        dynamic_sql = _build_dynamic_sql(question, filters)
        if dynamic_sql:
            return dynamic_sql

    # 2) Fall back to static templates
    for t in TEMPLATES:
        for pat in t["patterns"]:
            m = re.search(pat, q, re.IGNORECASE)
            if m:
                params = dict(t["defaults"])
                groups = m.groups()
                if "limit" in t["sql"] and groups:
                    params["limit"] = groups[0]
                try:
                    return t["sql"].format(**params)
                except KeyError:
                    return t["sql"]

    # 3) Last resort
    return "SELECT 'Sorry, I could not understand that question. Try: top 10 artists, royalties by territory, monthly trend, etc.' AS message"


# ── Public API ────────────────────────────────────────────────

def nl_to_sql(question: str, api_key: str = "") -> str:
    """Convert a natural-language question to SQL.
    
    Uses OpenAI if api_key is provided, otherwise falls back to templates.
    """
    if api_key:
        sql = _query_openai(question, api_key)
        if sql:
            return sql
    return _query_template(question)


def ask(question: str, api_key: str = "", db_path: str | Path = DB_PATH) -> dict:
    """
    End-to-end: question → SQL → execute → return results.
    
    Returns:
        {
            "question": str,
            "sql": str,
            "columns": list[str],
            "rows": list[list],
            "row_count": int,
            "mode": "llm" | "template",
            "error": str | None,
        }
    """
    sql = nl_to_sql(question, api_key)
    mode = "llm" if api_key else "template"

    try:
        con = duckdb.connect(str(db_path), read_only=True)
        result = con.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        con.close()

        def _sanitize(val):
            if isinstance(val, Decimal):
                return float(val)
            return val

        return {
            "question": question,
            "sql": sql,
            "columns": columns,
            "rows": [[_sanitize(v) for v in r] for r in rows],
            "row_count": len(rows),
            "mode": mode,
            "error": None,
        }
    except Exception as e:
        return {
            "question": question,
            "sql": sql,
            "columns": [],
            "rows": [],
            "row_count": 0,
            "mode": mode,
            "error": str(e),
        }


# ── CLI demo ──────────────────────────────────────────────────

if __name__ == "__main__":
    import sys, json, os
    api_key = os.getenv("OPENAI_API_KEY", "")

    questions = [
        "Top 10 artists by royalties",
        "Royalties by territory",
        "Monthly trend",
        "Streams by genre",
        "How much total royalties?",
        "Top 5 songs",
    ]

    if len(sys.argv) > 1:
        questions = [" ".join(sys.argv[1:])]

    for q in questions:
        print(f"\n💬 {q}")
        result = ask(q, api_key=api_key)
        print(f"🔍 SQL: {result['sql']}")
        if result["error"]:
            print(f"❌ Error: {result['error']}")
        else:
            print(f"📊 {result['row_count']} rows returned")
            # Print first 5 rows
            if result["columns"]:
                print(f"   Columns: {result['columns']}")
            for row in result["rows"][:5]:
                print(f"   {row}")
