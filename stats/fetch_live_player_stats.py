# fetch_live_player_stats.py

import nfl_data_py as nfl
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

year = 2025
table_name = f"player_stats_{year}"
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

print(f"📡 Fetching weekly player stats for {year}...")

try:
    weekly = nfl.import_weekly_data([year])
except Exception as e:
    print("⚠️ No data available yet for the current season.")
    exit()

if weekly.empty:
    print("⚠️ No data returned. Season likely hasn't started.")
    exit()

# Clean and store
weekly.columns = [c.strip().lower().replace(" ", "_") for c in weekly.columns]
weekly['week'] = weekly['week'].astype(int)
weekly['season'] = weekly['season'].astype(int)

with engine.begin() as conn:
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    weekly.to_sql(table_name, conn, index=False)

print(f"✅ Imported {len(weekly)} weekly records into {table_name}")
