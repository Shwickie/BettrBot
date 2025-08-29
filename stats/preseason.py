#!/usr/bin/env python3
# Hardcode 2025 preseason summary into team_season_summary so the dashboard can render now.
import math
import pandas as pd
from sqlalchemy import create_engine, text

DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
# wait up to 30s if DB is busy
engine = create_engine(DB_PATH, connect_args={"timeout": 30})

# Wins, Losses, Ties from your list (use your table's 3-letter team codes; Rams = 'LA' in your DB)
records = {
    "IND": (1, 2, 0), "BAL": (3, 0, 0), "CIN": (1, 2, 0), "PHI": (2, 1, 0),
    "LV":  (0, 2, 1), "SEA": (1, 1, 1), "DET": (1, 3, 0), "ATL": (0, 3, 0),
    "CLE": (3, 0, 0), "CAR": (0, 3, 0), "WAS": (0, 3, 0), "NE":  (2, 1, 0),
    "NYG": (3, 0, 0), "BUF": (1, 2, 0), "HOU": (2, 1, 0), "MIN": (1, 2, 0),
    "PIT": (2, 1, 0), "JAX": (0, 2, 1), "DAL": (1, 2, 0), "LA":  (2, 1, 0),  # Rams
    "TEN": (2, 1, 0), "TB":  (2, 1, 0), "KC":  (0, 3, 0), "ARI": (2, 1, 0),
    "NYJ": (1, 2, 0), "GB":  (2, 1, 0), "DEN": (3, 0, 0), "SF":  (2, 1, 0),
    "MIA": (2, 0, 1), "CHI": (2, 0, 1), "NO":  (0, 2, 1), "LAC": (2, 2, 0),
}

rows = []
for team, (w, l, t) in records.items():
    gp = w + l + t
    # Count ties as half for display (feels right for a "record"); if you prefer 0, change to: w / gp
    win_pct = (w + 0.5 * t) / gp if gp else 0.0
    # simple power proxy so rankings aren't all zero
    power = round((win_pct - 0.5) * 20, 1)  # roughly -10..+10

    rows.append({
        "season": 2025,
        "team": team,
        "games_played": gp,
        "avg_points_for": 0.0,
        "avg_points_against": 0.0,
        "wins": w,
        "losses": l,
        "win_pct": float(win_pct),
        "point_diff": 0.0,
        "star_players": 0,
        "superstars": 0,
        "power_score": float(power),
        "preseason_scheduled": gp,
        "preseason_completed": gp,
    })

df = pd.DataFrame(rows, columns=[
    "season","team","games_played","avg_points_for","avg_points_against",
    "wins","losses","win_pct","point_diff","star_players","superstars",
    "power_score","preseason_scheduled","preseason_completed"
])

with engine.begin() as conn:
    # Make sure the table exists with at least these columns
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS team_season_summary (
            season INTEGER, team TEXT,
            games_played INTEGER, avg_points_for REAL, avg_points_against REAL,
            wins INTEGER, losses INTEGER, win_pct REAL, point_diff REAL,
            star_players INTEGER, superstars INTEGER, power_score REAL,
            preseason_scheduled INTEGER, preseason_completed INTEGER
        )
    """))
    # Nuke existing 2025 rows and insert our hardcoded ones
    conn.execute(text("DELETE FROM team_season_summary WHERE season = 2025"))
    df.to_sql("team_season_summary", conn, if_exists="append", index=False)
    # Optional: view how many rows we wrote
    cnt = conn.execute(text("SELECT COUNT(*) FROM team_season_summary WHERE season=2025")).scalar_one()
    print(f"Inserted {cnt} preseason summary rows for 2025.")
