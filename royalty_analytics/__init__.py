"""AI-enabled music royalty analytics platform core package."""

from .etl import seed_sample_data
from .nl_sql import NaturalLanguageSQLEngine
from .quality import run_data_quality_checks
from .schema import connect_database, initialize_star_schema

__all__ = [
    "NaturalLanguageSQLEngine",
    "connect_database",
    "initialize_star_schema",
    "run_data_quality_checks",
    "seed_sample_data",
]
