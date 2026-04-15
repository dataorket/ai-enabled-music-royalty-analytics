# ai-enabled-music-royalty-analytics

Minimal AI-enabled music royalty analytics platform components implemented in Python:

- Natural-language SQL query engine (`royalty_analytics/nl_sql.py`)
- DuckDB star schema (Snowflake-compatible SQL style) (`royalty_analytics/schema.py`)
- ETL sample data pipeline (`royalty_analytics/etl.py`)
- Data quality monitoring checks (`royalty_analytics/quality.py`)
- Interactive dashboard server (`royalty_analytics/dashboard_app.py`)

## Setup

```bash
python -m pip install -r requirements.txt
```

## Run focused tests

```bash
python -m unittest -v tests/test_core.py
```

## Ask natural-language analytics questions

```bash
python -m royalty_analytics.cli --question "show total royalties by artist"
python -m royalty_analytics.cli --question "top 2 tracks"
python -m royalty_analytics.cli --question "monthly royalty trend"
```

## Dashboard payload and quality checks

```bash
python -m royalty_analytics.cli --dashboard --quality
```

## Start interactive dashboard

```bash
python -m royalty_analytics.dashboard_app
```

Then open `http://127.0.0.1:8502`.
