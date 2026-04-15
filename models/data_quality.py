"""
Data Quality Framework — validates the DuckDB warehouse after ETL.

Checks:
  1. Row-count minimums per table
  2. Null checks on critical columns
  3. Referential integrity (FK → PK)
  4. Metric thresholds (e.g., no negative royalties)

Returns a report dict and prints a summary. Used by the dashboard
health endpoint and CI pipeline.
"""

import duckdb
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "warehouse.duckdb"


# ── Check definitions ────────────────────────────────────────

ROW_COUNT_CHECKS = {
    "dim_artists":     50,
    "dim_works":       100,
    "dim_territories": 10,
    "dim_platforms":   5,
    "dim_dates":       365,
    "fact_streams":    1_000,
    "fact_royalties":  1_000,
}

NOT_NULL_CHECKS = [
    ("dim_artists",     "artist_name"),
    ("dim_works",       "title"),
    ("dim_territories", "iso_code"),
    ("dim_platforms",   "platform_name"),
    ("fact_streams",    "work_id"),
    ("fact_royalties",  "work_id"),
    ("fact_royalties",  "net_royalty_eur"),
]

FK_CHECKS = [
    # (child_table, fk_column, parent_table, pk_column)
    ("dim_works",       "primary_artist_id", "dim_artists",     "artist_id"),
    ("fact_streams",    "work_id",           "dim_works",       "work_id"),
    ("fact_streams",    "territory_id",      "dim_territories", "territory_id"),
    ("fact_streams",    "platform_id",       "dim_platforms",   "platform_id"),
    ("fact_royalties",  "work_id",           "dim_works",       "work_id"),
    ("fact_royalties",  "artist_id",         "dim_artists",     "artist_id"),
    ("fact_royalties",  "territory_id",      "dim_territories", "territory_id"),
    ("fact_royalties",  "platform_id",       "dim_platforms",   "platform_id"),
]

METRIC_CHECKS = [
    # (description, sql returning 0 on pass)
    (
        "No negative net royalties",
        "SELECT COUNT(*) FROM fact_royalties WHERE net_royalty_eur < 0",
    ),
    (
        "Gross ≥ Net for every royalty row",
        "SELECT COUNT(*) FROM fact_royalties WHERE gross_royalty_eur < net_royalty_eur",
    ),
    (
        "No zero stream counts",
        "SELECT COUNT(*) FROM fact_streams WHERE stream_count <= 0",
    ),
]


# ── Runner ───────────────────────────────────────────────────

def run_dq_checks(db_path: str | Path = DB_PATH) -> dict[str, Any]:
    """Run all DQ checks and return a structured report."""
    con = duckdb.connect(str(db_path), read_only=True)
    results: list[dict] = []
    passed = 0
    failed = 0

    # 1. Row counts
    for table, minimum in ROW_COUNT_CHECKS.items():
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        ok = count >= minimum
        results.append({
            "category": "row_count",
            "check": f"{table} ≥ {minimum:,} rows",
            "actual": count,
            "passed": ok,
        })
        passed += ok
        failed += not ok

    # 2. Not-null
    for table, col in NOT_NULL_CHECKS:
        nulls = con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
        ).fetchone()[0]
        ok = nulls == 0
        results.append({
            "category": "not_null",
            "check": f"{table}.{col} has no NULLs",
            "actual": nulls,
            "passed": ok,
        })
        passed += ok
        failed += not ok

    # 3. Referential integrity
    for child, fk, parent, pk in FK_CHECKS:
        orphans = con.execute(f"""
            SELECT COUNT(*)
            FROM {child} c
            LEFT JOIN {parent} p ON c.{fk} = p.{pk}
            WHERE p.{pk} IS NULL
        """).fetchone()[0]
        ok = orphans == 0
        results.append({
            "category": "referential_integrity",
            "check": f"{child}.{fk} → {parent}.{pk}",
            "actual": orphans,
            "passed": ok,
        })
        passed += ok
        failed += not ok

    # 4. Metric thresholds
    for desc, sql in METRIC_CHECKS:
        violations = con.execute(sql).fetchone()[0]
        ok = violations == 0
        results.append({
            "category": "metric",
            "check": desc,
            "actual": violations,
            "passed": ok,
        })
        passed += ok
        failed += not ok

    con.close()

    report = {
        "total": passed + failed,
        "passed": passed,
        "failed": failed,
        "checks": results,
    }
    return report


def print_report(report: dict):
    """Pretty-print a DQ report to stdout."""
    print("═══ Data Quality Report ═══\n")
    for r in report["checks"]:
        icon = "✅" if r["passed"] else "❌"
        print(f"  {icon}  [{r['category']}] {r['check']}  (actual: {r['actual']})")

    print(f"\n  Total: {report['total']}  |  ✅ Passed: {report['passed']}  |  ❌ Failed: {report['failed']}")
    if report["failed"] == 0:
        print("\n🎉 All data quality checks passed!\n")
    else:
        print(f"\n⚠️  {report['failed']} check(s) failed — review above.\n")


if __name__ == "__main__":
    report = run_dq_checks()
    print_report(report)
