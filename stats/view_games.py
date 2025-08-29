# view_games.py
from sqlalchemy import create_engine, MetaData, Table, select

engine = create_engine("sqlite:///E:/Bettr Bot/betting-bot/data/betting.db")
metadata = MetaData()
metadata.reflect(bind=engine)
games = metadata.tables.get("games")

with engine.connect() as conn:
    result = conn.execute(select(games).limit(20)).fetchall()
    for row in result:
        print(dict(row._mapping))
