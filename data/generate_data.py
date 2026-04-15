"""
Generate synthetic music royalty data for a star-schema warehouse.

Dimensions: artists, works (songs), territories, platforms, time (dates)
Facts:      streams, royalties

Covers the music copyright domain:
  - Musical works, rightsholders, territories, digital platforms, royalties.
"""

import csv
import os
import random
import uuid
from datetime import date, timedelta

random.seed(42)

OUT_DIR = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# Dimension: Artists / Rightsholders
# ---------------------------------------------------------------------------
FIRST_NAMES = [
    "Anna", "Ben", "Clara", "David", "Elena", "Felix", "Greta", "Hugo",
    "Isla", "Jonas", "Kira", "Liam", "Mila", "Noah", "Olivia", "Paul",
    "Quinn", "Rosa", "Sam", "Tina", "Uwe", "Vera", "Will", "Xena",
    "Yuki", "Zara", "Amir", "Bianca", "Carlos", "Diana",
]
LAST_NAMES = [
    "Müller", "Schmidt", "Rossi", "García", "Dubois", "Johansson",
    "Andersen", "Silva", "Nakamura", "Kim", "Chen", "Ali", "Okonkwo",
    "Petrov", "Jensen", "Berg", "Torres", "Hansen", "Becker", "Fischer",
]
ROLES = ["Songwriter", "Composer", "Lyricist", "Arranger", "Publisher"]

def gen_artists(n=200):
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "artist_id": i,
            "artist_name": f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
            "role": random.choice(ROLES),
            "society": random.choice(["GEMA", "PRS", "SACEM", "STIM", "BUMA", "ASCAP", "SGAE", "SIAE"]),
            "country_of_origin": random.choice(["DE", "GB", "FR", "SE", "NL", "US", "ES", "IT", "JP", "KR"]),
            "created_at": "2020-01-01",
        })
    return rows

# ---------------------------------------------------------------------------
# Dimension: Musical Works
# ---------------------------------------------------------------------------
GENRES = ["Pop", "Rock", "Hip-Hop", "Electronic", "Classical", "Jazz", "Latin", "R&B", "Country", "Afrobeats"]
ADJECTIVES = ["Midnight", "Golden", "Electric", "Silent", "Broken", "Eternal", "Velvet", "Crystal", "Wild", "Neon"]
NOUNS = ["Heart", "Dream", "Sky", "River", "Fire", "Echo", "Shadow", "Light", "Rain", "Storm"]

def gen_works(n=500, artists=None):
    rows = []
    for i in range(1, n + 1):
        title = f"{random.choice(ADJECTIVES)} {random.choice(NOUNS)}"
        primary_artist = random.choice(artists)
        rows.append({
            "work_id": i,
            "iswc": f"T-{random.randint(100000000, 999999999)}-{random.randint(0,9)}",
            "title": title,
            "genre": random.choice(GENRES),
            "primary_artist_id": primary_artist["artist_id"],
            "release_date": str(date(2020, 1, 1) + timedelta(days=random.randint(0, 1800))),
            "duration_seconds": random.randint(120, 420),
        })
    return rows

# ---------------------------------------------------------------------------
# Dimension: Territories
# ---------------------------------------------------------------------------
TERRITORIES = [
    ("DE", "Germany", "Europe"), ("GB", "United Kingdom", "Europe"),
    ("FR", "France", "Europe"), ("SE", "Sweden", "Europe"),
    ("NL", "Netherlands", "Europe"), ("ES", "Spain", "Europe"),
    ("IT", "Italy", "Europe"), ("US", "United States", "North America"),
    ("CA", "Canada", "North America"), ("MX", "Mexico", "North America"),
    ("BR", "Brazil", "South America"), ("AR", "Argentina", "South America"),
    ("JP", "Japan", "Asia"), ("KR", "South Korea", "Asia"),
    ("AU", "Australia", "Oceania"), ("IN", "India", "Asia"),
    ("NG", "Nigeria", "Africa"), ("ZA", "South Africa", "Africa"),
    ("EG", "Egypt", "Africa"), ("AE", "UAE", "Asia"),
]

def gen_territories():
    return [
        {"territory_id": i + 1, "iso_code": t[0], "name": t[1], "region": t[2]}
        for i, t in enumerate(TERRITORIES)
    ]

# ---------------------------------------------------------------------------
# Dimension: Platforms (DSPs)
# ---------------------------------------------------------------------------
PLATFORMS = [
    ("Spotify", "Streaming"), ("Apple Music", "Streaming"),
    ("YouTube Music", "Streaming"), ("Amazon Music", "Streaming"),
    ("Deezer", "Streaming"), ("Tidal", "Streaming"),
    ("SoundCloud", "Streaming"), ("Pandora", "Streaming"),
    ("YouTube", "Video"), ("TikTok", "Short-form Video"),
    ("Instagram Reels", "Short-form Video"), ("FM Radio", "Broadcast"),
    ("TV Sync", "Broadcast"),
]

def gen_platforms():
    return [
        {"platform_id": i + 1, "platform_name": p[0], "platform_type": p[1]}
        for i, p in enumerate(PLATFORMS)
    ]

# ---------------------------------------------------------------------------
# Dimension: Date (calendar)
# ---------------------------------------------------------------------------
def gen_dates(start="2023-01-01", end="2025-12-31"):
    rows = []
    d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    while d <= end_d:
        rows.append({
            "date_key": d.isoformat(),
            "year": d.year,
            "quarter": (d.month - 1) // 3 + 1,
            "month": d.month,
            "month_name": d.strftime("%B"),
            "day_of_week": d.strftime("%A"),
            "is_weekend": 1 if d.weekday() >= 5 else 0,
        })
        d += timedelta(days=1)
    return rows

# ---------------------------------------------------------------------------
# Fact: Streams  (daily granularity, sampled)
# ---------------------------------------------------------------------------
def gen_streams(works, territories, platforms, dates, n=50_000):
    rows = []
    work_ids = [w["work_id"] for w in works]
    terr_ids = [t["territory_id"] for t in territories]
    plat_ids = [p["platform_id"] for p in platforms]
    date_keys = [d["date_key"] for d in dates]

    for _ in range(n):
        rows.append({
            "stream_id": str(uuid.uuid4())[:12],
            "work_id": random.choice(work_ids),
            "territory_id": random.choice(terr_ids),
            "platform_id": random.choice(plat_ids),
            "date_key": random.choice(date_keys),
            "stream_count": random.randint(100, 500_000),
        })
    return rows

# ---------------------------------------------------------------------------
# Fact: Royalties  (monthly granularity, sampled)
# ---------------------------------------------------------------------------
RATE_PER_STREAM = {
    "Streaming": 0.004,
    "Video": 0.001,
    "Short-form Video": 0.0005,
    "Broadcast": 0.01,
}

def gen_royalties(streams, platforms, works, n=30_000):
    plat_map = {p["platform_id"]: p["platform_type"] for p in platforms}
    work_artist = {w["work_id"]: w["primary_artist_id"] for w in works}
    rows = []
    sample = random.sample(streams, min(n, len(streams)))
    for s in sample:
        ptype = plat_map[s["platform_id"]]
        rate = RATE_PER_STREAM.get(ptype, 0.003)
        gross = round(s["stream_count"] * rate, 2)
        commission = round(gross * random.uniform(0.10, 0.20), 2)
        rows.append({
            "royalty_id": str(uuid.uuid4())[:12],
            "work_id": s["work_id"],
            "artist_id": work_artist[s["work_id"]],
            "territory_id": s["territory_id"],
            "platform_id": s["platform_id"],
            "date_key": s["date_key"][:7] + "-01",  # monthly
            "gross_royalty_eur": gross,
            "commission_eur": commission,
            "net_royalty_eur": round(gross - commission, 2),
            "currency": "EUR",
            "status": random.choice(["Distributed", "Distributed", "Pending", "Disputed"]),
        })
    return rows

# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------
def write_csv(filename, rows):
    if not rows:
        return
    path = os.path.join(OUT_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  ✓ {filename}: {len(rows):,} rows")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("Generating synthetic music royalty data …")

    artists = gen_artists(200)
    works = gen_works(500, artists)
    territories = gen_territories()
    platforms = gen_platforms()
    dates = gen_dates()
    streams = gen_streams(works, territories, platforms, dates, n=50_000)
    royalties = gen_royalties(streams, platforms, works, n=30_000)

    write_csv("dim_artists.csv", artists)
    write_csv("dim_works.csv", works)
    write_csv("dim_territories.csv", territories)
    write_csv("dim_platforms.csv", platforms)
    write_csv("dim_dates.csv", dates)
    write_csv("fact_streams.csv", streams)
    write_csv("fact_royalties.csv", royalties)

    print("Done ✓")

if __name__ == "__main__":
    main()
