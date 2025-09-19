# debug_odds_data.py
import sqlite3
import pandas as pd

DB_PATH = r"E:\Bettr Bot\betting-bot\data\betting.db"

conn = sqlite3.connect(DB_PATH)

# Check what's in odds for these specific games
print("=== CHECKING ODDS FOR SPECIFIC GAMES ===")

games_to_check = [
    "20250921_ATL_CAR",
    "20250921_HOU_JAX", 
    "20250921_IND_TEN"
]

for game_id in games_to_check:
    print(f"\nGame ID: {game_id}")
    
    odds = pd.read_sql_query("""
        SELECT team, sportsbook, odds, timestamp
        FROM odds 
        WHERE game_id = ?
        ORDER BY timestamp DESC
        LIMIT 10
    """, conn, params=[game_id])
    
    if odds.empty:
        print("  No odds found")
    else:
        print(f"  Found {len(odds)} odds:")
        for _, row in odds.iterrows():
            print(f"    {row['team']}: {row['odds']} @ {row['sportsbook']}")

conn.close()