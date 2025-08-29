# update_scores.py

import pandas as pd
import nfl_data_py as nfl
from sqlalchemy import create_engine, MetaData, Table, select, update

# --------------------------
# CONFIG
# --------------------------
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)
metadata = MetaData()
metadata.reflect(bind=engine)
games_table = metadata.tables.get("games")

if games_table is None:
    raise Exception("❌ 'games' table not found")

# Team abbreviation to full name
TEAM_NAME_MAP = {
    "ARI": "Arizona Cardinals",
    "ATL": "Atlanta Falcons",
    "BAL": "Baltimore Ravens",
    "BUF": "Buffalo Bills",
    "CAR": "Carolina Panthers",
    "CHI": "Chicago Bears",
    "CIN": "Cincinnati Bengals",
    "CLE": "Cleveland Browns",
    "DAL": "Dallas Cowboys",
    "DEN": "Denver Broncos",
    "DET": "Detroit Lions",
    "GB": "Green Bay Packers",
    "HOU": "Houston Texans",
    "IND": "Indianapolis Colts",
    "JAX": "Jacksonville Jaguars",
    "KC": "Kansas City Chiefs",
    "LAC": "Los Angeles Chargers",
    "LAR": "Los Angeles Rams",
    "LV": "Las Vegas Raiders",
    "MIA": "Miami Dolphins",
    "MIN": "Minnesota Vikings",
    "NE": "New England Patriots",
    "NO": "New Orleans Saints",
    "NYG": "New York Giants",
    "NYJ": "New York Jets",
    "PHI": "Philadelphia Eagles",
    "PIT": "Pittsburgh Steelers",
    "SEA": "Seattle Seahawks",
    "SF": "San Francisco 49ers",
    "TB": "Tampa Bay Buccaneers",
    "TEN": "Tennessee Titans",
    "WAS": "Washington Commanders"
}

# --------------------------
# Load NFL historical schedule
# --------------------------
print("📦 Loading NFL historical schedule...")
df = nfl.import_schedules([2021, 2022, 2023, 2024])
df = df[['game_id', 'home_team', 'away_team', 'home_score', 'away_score', 'gameday']]
df.columns = df.columns.str.lower()
df['gameday'] = pd.to_datetime(df['gameday'])  # 👈 convert to datetime


# Map abbreviations to full names
df['home_team'] = df['home_team'].map(TEAM_NAME_MAP)
df['away_team'] = df['away_team'].map(TEAM_NAME_MAP)

# --------------------------
# Update DB
# --------------------------
with engine.connect() as conn:
    existing = pd.read_sql(select(games_table), conn)

# Merge using full team names
merged = pd.merge(df, existing, how="inner", on=["home_team", "away_team"])
updates = merged[(merged['home_score_y'].isna()) & (merged['home_score_x'].notna())]

print(f"🔁 Updating {len(updates)} old games with score data...")
with engine.begin() as conn:
    for _, row in updates.iterrows():
        stmt = (
            update(games_table)
            .where(games_table.c.id == row['id'])
            .values(
                home_score=row['home_score_x'],
                away_score=row['away_score_x'],
                game_date=row['gameday']
            )
        )
        conn.execute(stmt)

print("✅ Finished updating historical scores.")
