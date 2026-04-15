"""
ETL Pipeline — Load CSVs into DuckDB star-schema warehouse.

Steps:
  1. Create / reset DuckDB database
  2. Execute DDL (models/schema.sql)
  3. COPY CSV files into dimension & fact tables
  4. Build aggregation views for the dashboard

Snowflake note:
  In a Snowflake deployment you would replace the CSV COPY with
  COPY INTO … FROM @stage pattern. The SQL is kept compatible.
"""

import os
import sys
import duckdb
from pathlib import Path

# ── paths ────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
MODELS_DIR = BASE_DIR / "models"
DB_PATH = DATA_DIR / "warehouse.duckdb"

DDL_FILE = MODELS_DIR / "schema.sql"


def get_connection(db_path: str | Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection (creates the file if needed)."""
    return duckdb.connect(str(db_path))


def run_ddl(con: duckdb.DuckDBPyConnection):
    """Execute the star-schema DDL."""
    sql = DDL_FILE.read_text()
    con.execute(sql)
    print("  ✓ DDL executed — tables created")


def load_csv(con: duckdb.DuckDBPyConnection, table: str, filename: str):
    """Bulk-load a CSV into a table using DuckDB's fast CSV reader."""
    csv_path = DATA_DIR / filename
    if not csv_path.exists():
        print(f"  ✗ {filename} not found — skipping")
        return
    con.execute(f"""
        INSERT INTO {table}
        SELECT * FROM read_csv_auto('{csv_path}', header=true)
    """)
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"  ✓ {table}: {count:,} rows loaded")


def create_views(con: duckdb.DuckDBPyConnection):
    """Create analytics views used by the dashboard and NL query engine."""

    con.execute("""
        CREATE OR REPLACE VIEW v_royalties_by_artist AS
        SELECT
            a.artist_id,
            a.artist_name,
            a.society,
            SUM(r.gross_royalty_eur)  AS total_gross_eur,
            SUM(r.net_royalty_eur)    AS total_net_eur,
            COUNT(*)                  AS txn_count
        FROM fact_royalties r
        JOIN dim_artists a ON r.artist_id = a.artist_id
        GROUP BY a.artist_id, a.artist_name, a.society
    """)

    con.execute("""
        CREATE OR REPLACE VIEW v_royalties_by_territory AS
        SELECT
            t.territory_id,
            t.iso_code,
            t.name       AS territory_name,
            t.region,
            SUM(r.gross_royalty_eur)  AS total_gross_eur,
            SUM(r.net_royalty_eur)    AS total_net_eur,
            COUNT(*)                  AS txn_count
        FROM fact_royalties r
        JOIN dim_territories t ON r.territory_id = t.territory_id
        GROUP BY t.territory_id, t.iso_code, t.name, t.region
    """)

    con.execute("""
        CREATE OR REPLACE VIEW v_royalties_by_platform AS
        SELECT
            p.platform_id,
            p.platform_name,
            p.platform_type,
            SUM(r.gross_royalty_eur)  AS total_gross_eur,
            SUM(r.net_royalty_eur)    AS total_net_eur,
            COUNT(*)                  AS txn_count
        FROM fact_royalties r
        JOIN dim_platforms p ON r.platform_id = p.platform_id
        GROUP BY p.platform_id, p.platform_name, p.platform_type
    """)

    con.execute("""
        CREATE OR REPLACE VIEW v_monthly_trend AS
        SELECT
            EXTRACT(YEAR FROM r.date_key)  AS year,
            EXTRACT(MONTH FROM r.date_key) AS month,
            SUM(r.gross_royalty_eur)        AS total_gross_eur,
            SUM(r.net_royalty_eur)          AS total_net_eur,
            COUNT(*)                        AS txn_count
        FROM fact_royalties r
        GROUP BY year, month
        ORDER BY year, month
    """)

    con.execute("""
        CREATE OR REPLACE VIEW v_streams_by_genre AS
        SELECT
            w.genre,
            SUM(s.stream_count)  AS total_streams,
            COUNT(*)             AS record_count
        FROM fact_streams s
        JOIN dim_works w ON s.work_id = w.work_id
        GROUP BY w.genre
    """)

    con.execute("""
        CREATE OR REPLACE VIEW v_top_works AS
        SELECT
            w.work_id,
            w.title,
            w.genre,
            a.artist_name,
            SUM(r.net_royalty_eur)   AS total_net_eur,
            SUM(s.stream_count)      AS total_streams
        FROM dim_works w
        JOIN dim_artists a ON w.primary_artist_id = a.artist_id
        LEFT JOIN fact_royalties r ON w.work_id = r.work_id
        LEFT JOIN fact_streams s   ON w.work_id = s.work_id
        GROUP BY w.work_id, w.title, w.genre, a.artist_name
    """)

    print("  ✓ Analytics views created")


def run_etl():
    """Full ETL pipeline."""
    print("═══ Music Royalty ETL Pipeline ═══\n")

    # Remove old DB to get a clean load
    if DB_PATH.exists():
        os.remove(DB_PATH)
        print("  ✓ Old warehouse removed")

    con = get_connection()

    print("\n── 1. Schema creation ──")
    run_ddl(con)

    print("\n── 2. Loading dimensions ──")
    load_csv(con, "dim_artists",     "dim_artists.csv")
    load_csv(con, "dim_works",       "dim_works.csv")
    load_csv(con, "dim_territories", "dim_territories.csv")
    load_csv(con, "dim_platforms",   "dim_platforms.csv")
    load_csv(con, "dim_dates",       "dim_dates.csv")

    print("\n── 3. Loading facts ──")
    load_csv(con, "fact_streams",    "fact_streams.csv")
    load_csv(con, "fact_royalties",  "fact_royalties.csv")

    print("\n── 4. Creating analytics views ──")
    create_views(con)

    con.close()
    print(f"\n✅ Warehouse ready → {DB_PATH}\n")


if __name__ == "__main__":
    run_etl()
