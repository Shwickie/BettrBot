from sqlalchemy import create_engine, MetaData, Table, select
import pandas as pd

DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)
metadata = MetaData()
metadata.reflect(bind=engine)

games_table = metadata.tables.get("games")

if games_table is None:
    raise Exception("❌ 'games' table not found")

# Query scores
with engine.connect() as conn:
    result = conn.execute(select(games_table)).fetchall()
    rows = [dict(row._mapping) for row in result if row.home_score is not None and row.away_score is not None]

print(f"📊 Found {len(rows)} games with scores:")
for row in rows[:10]:  # print only the first 10 for now
    print(f"{row['game_date']}: {row['home_team']} {row['home_score']} - {row['away_team']} {row['away_score']}")
