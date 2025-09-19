# Add to find_odds.py
import sqlite3
import pandas as pd

DB_PATH = r"E:\Bettr Bot\betting-bot\data\betting.db"
conn = sqlite3.connect(DB_PATH)

# Check team names in both tables
print("=== TEAM NAMES COMPARISON ===")

games_teams = pd.read_sql_query("""
    SELECT DISTINCT home_team as team FROM games 
    UNION 
    SELECT DISTINCT away_team as team FROM games 
    ORDER BY team
""", conn)

odds_teams = pd.read_sql_query("""
    SELECT DISTINCT team FROM odds 
    WHERE timestamp >= datetime('now', '-7 days')
    ORDER BY team
""", conn)

print("Games table teams:", games_teams['team'].tolist()[:10])
print("Odds table teams:", odds_teams['team'].tolist()[:10])

# Check for exact matches
game_set = set(games_teams['team'])
odds_set = set(odds_teams['team'])

print(f"Teams that match exactly: {len(game_set.intersection(odds_set))}")
print(f"Games-only teams: {list(game_set - odds_set)[:5]}")
print(f"Odds-only teams: {list(odds_set - game_set)[:5]}")

conn.close()