from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer

from .dashboard import build_dashboard_payload
from .etl import seed_sample_data
from .schema import connect_database, initialize_star_schema


def _render_html(payload: dict) -> str:
    return f"""
<!doctype html>
<html>
<head>
  <meta charset='utf-8' />
  <title>Music Royalty Dashboard</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    .kpis {{ display:flex; gap:16px; margin-bottom:16px; }}
    .card {{ border:1px solid #ddd; border-radius:8px; padding:12px 16px; min-width:180px; }}
    .bar {{ background:#4f46e5; color:white; padding:6px; margin:6px 0; border-radius:4px; }}
  </style>
</head>
<body>
  <h1>AI-enabled Music Royalty Analytics</h1>
  <div class='kpis'>
    <div class='card'><strong>Total Royalty (USD)</strong><div id='kpi-royalty'></div></div>
    <div class='card'><strong>Total Streams</strong><div id='kpi-streams'></div></div>
    <div class='card'><strong>Active Tracks</strong><div id='kpi-tracks'></div></div>
  </div>

  <label for='platform'>Monthly royalty trend by platform:</label>
  <select id='platform'></select>
  <div id='bars'></div>

  <script>
    const data = {json.dumps(payload)};
    document.getElementById('kpi-royalty').textContent = data.kpis.total_royalty_usd.toFixed(2);
    document.getElementById('kpi-streams').textContent = data.kpis.total_streams;
    document.getElementById('kpi-tracks').textContent = data.kpis.active_tracks;

    const platformSelect = document.getElementById('platform');
    const platforms = ['All', ...new Set(data.monthly_by_platform.map(r => r.platform_name))];
    platforms.forEach(p => {{
      const opt = document.createElement('option');
      opt.value = p;
      opt.textContent = p;
      platformSelect.appendChild(opt);
    }});

    function drawBars(selectedPlatform) {{
      const bars = document.getElementById('bars');
      bars.innerHTML = '';
      let rows;
      if (selectedPlatform === 'All') {{
        rows = data.monthly_royalty_trend;
      }} else {{
        rows = data.monthly_by_platform
          .filter(r => r.platform_name === selectedPlatform)
          .map(r => ({{ month: r.month, total_royalty_usd: r.total_royalty_usd }}));
      }}
      const max = Math.max(1, ...rows.map(r => r.total_royalty_usd));
      rows.forEach(r => {{
        const bar = document.createElement('div');
        const pct = Math.round((r.total_royalty_usd / max) * 100);
        bar.className = 'bar';
        bar.style.width = pct + '%';
        bar.textContent = `${{r.month}} — $${{r.total_royalty_usd.toFixed(2)}}`;
        bars.appendChild(bar);
      }});
    }}

    platformSelect.addEventListener('change', () => drawBars(platformSelect.value));
    drawBars('All');
  </script>
</body>
</html>
"""


class DashboardHandler(BaseHTTPRequestHandler):
    payload: dict = {}

    def do_GET(self) -> None:
        if self.path not in ("/", "/index.html"):
            self.send_response(404)
            self.end_headers()
            return
        html = _render_html(self.payload)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(html.encode("utf-8"))


def run_dashboard_server(host: str = "127.0.0.1", port: int = 8502) -> None:
    conn = connect_database(":memory:")
    initialize_star_schema(conn)
    seed_sample_data(conn)
    DashboardHandler.payload = build_dashboard_payload(conn)
    server = HTTPServer((host, port), DashboardHandler)
    print(f"Dashboard running at http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    run_dashboard_server()
