#!/usr/bin/env python3
"""
Database Checker - See what's actually in your database for those dates
"""

import sqlite3
import pandas as pd

DB_PATH = "E:/Bettr Bot/betting-bot/data/betting.db"

def check_september_games():
    print("CHECKING SEPTEMBER GAMES IN DATABASE")
    print("=" * 50)
    
    conn = sqlite3.connect(DB_PATH)
    
    # Check games from September 4-8 specifically
    games = pd.read_sql("""
        SELECT game_date, away_team, home_team, away_score, home_score
        FROM games 
        WHERE date(game_date) BETWEEN '2025-09-04' AND '2025-09-08'
        ORDER BY game_date
    """, conn)
    
    print(f"Games from Sept 4-8: {len(games)}")
    
    for _, game in games.iterrows():
        has_score = "YES" if pd.notna(game['home_score']) else "NO"
        if pd.notna(game['home_score']):
            score = f"{int(game['away_score'])}-{int(game['home_score'])}"
        else:
            score = "No score"
        
        print(f"  {game['game_date'][:10]}: {game['away_team']} @ {game['home_team']} - {score} (Has score: {has_score})")
    
    # Check what team name formats we have
    print(f"\nTeam name samples from these games:")
    if not games.empty:
        all_teams = set(games['away_team'].tolist() + games['home_team'].tolist())
        print(f"Teams found: {sorted(list(all_teams))[:10]}")
    
    conn.close()

if __name__ == "__main__":
    check_september_games()