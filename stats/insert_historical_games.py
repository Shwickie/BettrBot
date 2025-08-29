# insert_historical_games.py
import pandas as pd
import nfl_data_py as nfl
from sqlalchemy import create_engine, MetaData, Table, select

DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)
metadata = MetaData()
metadata.reflect(bind=engine)
games_table = metadata.tables.get("games")

if games_table is None:
    raise Exception("❌ 'games' table not found")

print("📦 Importing full NFL schedule 2021–2024...")
df = nfl.import_schedules([2021, 2022, 2023, 2024])

# Keep only needed columns
df = df[['game_id', 'home_team', 'away_team', 'home_score', 'away_score', 'gameday']]
df.columns = df.columns.str.lower()

# Insert only missing games (primary key = game_id)
with engine.begin() as conn:
    existing_ids = {row[0] for row in conn.execute(select(games_table.c.id))}
    insert_count = 0
    for _, row in df.iterrows():
        gid = row['game_id']
        if gid not in existing_ids:
            conn.execute(
                games_table.insert().values(
                    id=gid,  # primary key
                    game_id=gid,
                    home_team=row['home_team'],
                    away_team=row['away_team'],
                    home_score=row['home_score'],
                    away_score=row['away_score'],
                    game_date=pd.to_datetime(row['gameday']) if pd.notna(row['gameday']) else None
                )
            )
            insert_count += 1

print(f"✅ Inserted {insert_count} new historical games.")
