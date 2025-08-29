# setup_db.py
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, UniqueConstraint, text
from sqlalchemy.orm import declarative_base
from datetime import datetime
import os

# ✅ Make sure the DB folder exists
db_dir = "E:/Bettr Bot/betting-bot/data"
os.makedirs(db_dir, exist_ok=True)

# ✅ Absolute path to DB
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"

Base = declarative_base()

class Game(Base):
    __tablename__ = 'games'
    id = Column(String, primary_key=True)  # Will match game_id for consistency
    game_date = Column(DateTime)
    home_team = Column(String)
    away_team = Column(String)
    home_score = Column(Integer)
    away_score = Column(Integer)
    game_id = Column(String)
    start_time_utc = Column(String)    
    start_time_local = Column(String)   

class Odds(Base):
    __tablename__ = 'odds'
    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String)
    sportsbook = Column(String)
    team = Column(String)
    market = Column(String)
    odds = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow)
    __table_args__ = (UniqueConstraint('game_id', 'sportsbook', 'team', 'market', name='uix_odds_unique'),)

# === DB Setup ===
engine = create_engine(DB_PATH)
Base.metadata.create_all(engine)

with engine.begin() as conn:
    # Backup games table (if not already backed up)
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS games_backup AS 
        SELECT * FROM games
    """))

    # Remove junk rows from odds insert (no valid schedule game_id)
    conn.execute(text("DELETE FROM games WHERE game_id IS NULL"))

    # Ensure new columns exist
    cols = [r[1] for r in conn.exec_driver_sql("PRAGMA table_info(games)").fetchall()]
    if "start_time_utc" not in cols:
        conn.exec_driver_sql("ALTER TABLE games ADD COLUMN start_time_utc TEXT")
    if "start_time_local" not in cols:
        conn.exec_driver_sql("ALTER TABLE games ADD COLUMN start_time_local TEXT")

print("✅ Backup created, junk rows removed, columns verified.")
print("DB ready at", DB_PATH)
