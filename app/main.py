"""
Flask Dashboard — Music Royalty Analytics

Features:
  • KPI cards (total royalties, artists, works, territories)
  • Interactive Plotly charts (territory, platform, trend, genre)
  • Natural-language query box (AI-enabled analytics)
  • Data quality health endpoint
"""

import json
import duckdb
from decimal import Decimal
from pathlib import Path
from flask import Flask, render_template, request, jsonify


class DecimalEncoder(json.JSONEncoder):
    """Handle Decimal types from DuckDB."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def safe_json(obj):
    """json.dumps with Decimal support."""
    return json.dumps(obj, cls=DecimalEncoder)

# Resolve paths
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "warehouse.duckdb"

import sys
sys.path.insert(0, str(BASE_DIR))
from config import OPENAI_API_KEY, FLASK_SECRET_KEY, FLASK_DEBUG
from app.nl_query import ask as nl_ask
from models.data_quality import run_dq_checks

app = Flask(
    __name__,
    template_folder=str(BASE_DIR / "app" / "templates"),
    static_folder=str(BASE_DIR / "static"),
)
app.secret_key = FLASK_SECRET_KEY


def get_db():
    return duckdb.connect(str(DB_PATH), read_only=True)


# ── Helpers ──────────────────────────────────────────────────

PENDING_SQL = "SELECT COALESCE(SUM(net_royalty_eur),0) FROM fact_royalties WHERE status='Pending'"

def _sanitize(val):
    """Convert Decimal/other non-JSON-serializable types to float."""
    if isinstance(val, Decimal):
        return float(val)
    return val


def query_df(sql: str) -> list[dict]:
    """Execute SQL and return a list of dicts (JSON-safe)."""
    con = get_db()
    result = con.execute(sql)
    cols = [d[0] for d in result.description]
    rows = result.fetchall()
    con.close()
    return [dict(zip(cols, [_sanitize(v) for v in row])) for row in rows]


def query_scalar(sql: str):
    con = get_db()
    val = con.execute(sql).fetchone()[0]
    con.close()
    return _sanitize(val)
    return val


# ── Routes ───────────────────────────────────────────────────

@app.route("/")
def index():
    # KPIs
    kpis = {
        "total_net_royalties": f"€{query_scalar('SELECT COALESCE(SUM(net_royalty_eur),0) FROM fact_royalties'):,.0f}",
        "total_gross_royalties": f"€{query_scalar('SELECT COALESCE(SUM(gross_royalty_eur),0) FROM fact_royalties'):,.0f}",
        "total_artists": f"{query_scalar('SELECT COUNT(*) FROM dim_artists'):,}",
        "total_works": f"{query_scalar('SELECT COUNT(*) FROM dim_works'):,}",
        "total_territories": f"{query_scalar('SELECT COUNT(*) FROM dim_territories'):,}",
        "total_streams": f"{query_scalar('SELECT COALESCE(SUM(stream_count),0) FROM fact_streams'):,}",
        "pending_royalties": f"€{query_scalar(PENDING_SQL):,.0f}",
    }

    # Chart data
    by_territory = query_df("SELECT territory_name, total_net_eur FROM v_royalties_by_territory ORDER BY total_net_eur DESC LIMIT 15")
    by_platform = query_df("SELECT platform_name, total_net_eur FROM v_royalties_by_platform ORDER BY total_net_eur DESC")
    monthly = query_df("SELECT year, month, total_gross_eur, total_net_eur FROM v_monthly_trend ORDER BY year, month")
    by_genre = query_df("SELECT genre, total_streams FROM v_streams_by_genre ORDER BY total_streams DESC")
    top_artists = query_df("SELECT artist_name, total_net_eur FROM v_royalties_by_artist ORDER BY total_net_eur DESC LIMIT 10")

    return render_template(
        "dashboard.html",
        kpis=kpis,
        by_territory=safe_json(by_territory),
        by_platform=safe_json(by_platform),
        monthly=safe_json(monthly),
        by_genre=safe_json(by_genre),
        top_artists=safe_json(top_artists),
    )


@app.route("/api/query", methods=["POST"])
def api_query():
    """Natural-language query endpoint."""
    data = request.get_json(force=True)
    question = data.get("question", "")
    if not question:
        return jsonify({"error": "No question provided"}), 400

    result = nl_ask(question, api_key=OPENAI_API_KEY, db_path=DB_PATH)
    return jsonify(result)


@app.route("/api/health")
def api_health():
    """Data quality health check."""
    try:
        report = run_dq_checks(DB_PATH)
        return jsonify(report)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Main ─────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=FLASK_DEBUG)
