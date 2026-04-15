from __future__ import annotations

import argparse
import json

from .dashboard import build_dashboard_payload
from .etl import seed_sample_data
from .nl_sql import NaturalLanguageSQLEngine
from .quality import run_data_quality_checks
from .schema import connect_database, initialize_star_schema


def main() -> None:
    parser = argparse.ArgumentParser(description="AI-enabled music royalty analytics CLI")
    parser.add_argument("--db", default=":memory:", help="DuckDB path")
    parser.add_argument("--question", help="Natural language analytics question")
    parser.add_argument("--dashboard", action="store_true", help="Show dashboard payload JSON")
    parser.add_argument("--quality", action="store_true", help="Run data quality checks")
    args = parser.parse_args()

    conn = connect_database(args.db)
    initialize_star_schema(conn)
    seed_sample_data(conn)

    if args.question:
        result = NaturalLanguageSQLEngine(conn).ask(args.question)
        print(json.dumps(result, indent=2, default=str))

    if args.dashboard:
        print(json.dumps(build_dashboard_payload(conn), indent=2, default=str))

    if args.quality:
        failures = run_data_quality_checks(conn)
        print(json.dumps({"status": "ok" if not failures else "failed", "failures": failures}, indent=2))


if __name__ == "__main__":
    main()
