"""
Central configuration — reads from .env or falls back to defaults.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent

# DuckDB warehouse
DUCKDB_PATH = os.getenv("DUCKDB_PATH", str(BASE_DIR / "data" / "warehouse.duckdb"))

# OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# Flask
FLASK_SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-secret-key")
FLASK_DEBUG = os.getenv("FLASK_DEBUG", "1") == "1"
