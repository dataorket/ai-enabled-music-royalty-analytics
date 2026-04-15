import unittest

from royalty_analytics.dashboard import build_dashboard_payload
from royalty_analytics.etl import seed_sample_data
from royalty_analytics.nl_sql import NaturalLanguageSQLEngine
from royalty_analytics.quality import run_data_quality_checks
from royalty_analytics.schema import connect_database, initialize_star_schema


class TestRoyaltyAnalyticsCore(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = connect_database(":memory:")
        initialize_star_schema(self.conn)
        seed_sample_data(self.conn)

    def tearDown(self) -> None:
        self.conn.close()

    def test_star_schema_tables_exist(self) -> None:
        names = {
            row[0]
            for row in self.conn.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                """
            ).fetchall()
        }
        self.assertTrue({"dim_artist", "dim_track", "dim_platform", "dim_date", "fact_royalty"}.issubset(names))

    def test_nl_sql_returns_artist_totals(self) -> None:
        rows = NaturalLanguageSQLEngine(self.conn).ask("Show total royalties by artist")
        self.assertGreaterEqual(len(rows), 1)
        self.assertIn("artist_name", rows[0])
        self.assertIn("total_royalty_usd", rows[0])

    def test_nl_sql_blocks_unsafe_queries(self) -> None:
        with self.assertRaises(ValueError):
            NaturalLanguageSQLEngine(self.conn).ask("sql: drop table fact_royalty")

    def test_data_quality_checks_pass_with_seed_data(self) -> None:
        self.assertEqual(run_data_quality_checks(self.conn), [])

    def test_dashboard_payload_contains_expected_sections(self) -> None:
        payload = build_dashboard_payload(self.conn)
        self.assertIn("kpis", payload)
        self.assertIn("monthly_royalty_trend", payload)
        self.assertIn("platform_split", payload)
        self.assertIn("monthly_by_platform", payload)


if __name__ == "__main__":
    unittest.main()
