# migrate_odds_to_history.py
import sqlite3
DB_PATH = r"E:/Bettr Bot/betting-bot/data/betting.db"
con = sqlite3.connect(DB_PATH)
cur = con.cursor()
cur.executescript("""
PRAGMA journal_mode=WAL;
BEGIN;

-- Make sure odds table exists
CREATE TABLE IF NOT EXISTS odds (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  game_id    TEXT,
  sportsbook TEXT,
  team       TEXT,
  market     TEXT,
  odds       REAL,
  timestamp  DATETIME
);

-- Ensure we can store multiple snapshots per book/team/market over time
-- (no unique constraint on (game_id,sportsbook,team,market))
-- Optional: avoid exact duplicates for the same instant:
CREATE UNIQUE INDEX IF NOT EXISTS uix_odds_snapshot
  ON odds (game_id, sportsbook, team, market, timestamp);

CREATE INDEX IF NOT EXISTS idx_odds_game_team_book_ts
  ON odds (game_id, team, sportsbook, timestamp);

COMMIT;
""")
con.commit()
con.close()
print("✅ Migration complete.")
