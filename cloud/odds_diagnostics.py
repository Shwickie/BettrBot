#!/usr/bin/env python3
"""
Diagnostic script to check odds data in your PostgreSQL database
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

def check_odds_data():
    # Get database URL - handle both cloud and local
    DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
    
    if not DATABASE_URL:
        # Fallback to local SQLite for testing
        print("No DATABASE_URL found, using local SQLite...")
        local_db = r"E:\Bettr Bot\betting-bot\data\betting.db"
        if os.path.exists(local_db):
            engine = create_engine(f"sqlite:///{local_db}")
            USE_CLOUD = False
        else:
            print(f"Local database not found at: {local_db}")
            return
    else:
        if DATABASE_URL.startswith("postgres://"):
            DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://", 1)
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        USE_CLOUD = True
    
    print("=== ODDS DATA DIAGNOSTIC ===")
    
    with engine.connect() as conn:
        # 1. Check total odds count
        total_odds = conn.execute(text("SELECT COUNT(*) FROM odds")).scalar()
        print(f"Total odds records: {total_odds}")
        
        # 2. Check recent odds  
        if USE_CLOUD:
            recent_count = conn.execute(text("""
                SELECT COUNT(*) FROM odds 
                WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
            """)).scalar()
        else:
            recent_count = conn.execute(text("""
                SELECT COUNT(*) FROM odds 
                WHERE timestamp >= datetime('now', '-7 days')
            """)).scalar()
        print(f"Recent odds (last 7 days): {recent_count}")
        
        # 3. Check distinct sportsbooks
        books = conn.execute(text("SELECT DISTINCT sportsbook FROM odds")).fetchall()
        print(f"Sportsbooks: {[b[0] for b in books]}")
        
        # 4. Check distinct markets
        markets = conn.execute(text("SELECT DISTINCT market FROM odds")).fetchall()
        print(f"Markets: {[m[0] for m in markets]}")
        
        # 5. Check sample odds data
        sample = pd.read_sql(text("""
            SELECT game_id, team, sportsbook, odds, market, timestamp
            FROM odds 
            ORDER BY timestamp DESC 
            LIMIT 10
        """), conn)
        print(f"\nSample odds records:")
        print(sample)
        
        # 6. Check upcoming games
        if USE_CLOUD:
            upcoming = pd.read_sql(text("""
                SELECT game_id, home_team, away_team, game_date
                FROM games 
                WHERE game_date >= CURRENT_DATE
                ORDER BY game_date 
                LIMIT 10
            """), conn)
        else:
            upcoming = pd.read_sql(text("""
                SELECT game_id, home_team, away_team, game_date
                FROM games 
                WHERE date(game_date) >= date('now')
                ORDER BY game_date 
                LIMIT 10
            """), conn)
        print(f"\nUpcoming games:")
        print(upcoming)
        
        # 7. Check games WITH odds
        if USE_CLOUD:
            games_with_odds = conn.execute(text("""
                SELECT COUNT(DISTINCT g.game_id) 
                FROM games g
                INNER JOIN odds o ON g.game_id = o.game_id
                WHERE g.game_date >= CURRENT_DATE
            """)).scalar()
        else:
            games_with_odds = conn.execute(text("""
                SELECT COUNT(DISTINCT g.game_id) 
                FROM games g
                INNER JOIN odds o ON g.game_id = o.game_id
                WHERE date(g.game_date) >= date('now')
            """)).scalar()
        print(f"\nUpcoming games with odds: {games_with_odds}")
        
        # 8. Check for game_id mismatches
        orphan_odds = pd.read_sql(text("""
            SELECT DISTINCT o.game_id, COUNT(*) as odds_count
            FROM odds o
            LEFT JOIN games g ON o.game_id = g.game_id
            WHERE g.game_id IS NULL
            GROUP BY o.game_id
            LIMIT 10
        """), conn)
        print(f"\nOrphaned odds (no matching game):")
        print(orphan_odds)
        
        # 9. Check team name format in odds vs games
        if USE_CLOUD:
            odds_teams = pd.read_sql(text("""
                SELECT DISTINCT team 
                FROM odds 
                WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
                ORDER BY team
            """), conn)
            games_teams = pd.read_sql(text("""
                SELECT DISTINCT home_team as team FROM games WHERE game_date >= CURRENT_DATE
                UNION
                SELECT DISTINCT away_team as team FROM games WHERE game_date >= CURRENT_DATE
                ORDER BY team
            """), conn)
        else:
            odds_teams = pd.read_sql(text("""
                SELECT DISTINCT team 
                FROM odds 
                WHERE timestamp >= datetime('now', '-7 days')
                ORDER BY team
            """), conn)
            games_teams = pd.read_sql(text("""
                SELECT DISTINCT home_team as team FROM games WHERE date(game_date) >= date('now')
                UNION
                SELECT DISTINCT away_team as team FROM games WHERE date(game_date) >= date('now')
                ORDER BY team
            """), conn)
        
        print(f"\nTeam names in odds table:")
        print(odds_teams['team'].tolist() if not odds_teams.empty else 'No odds teams found')
        
        print(f"\nTeam names in games table:")
        print(games_teams['team'].tolist() if not games_teams.empty else 'No games teams found')

if __name__ == "__main__":
    check_odds_data()