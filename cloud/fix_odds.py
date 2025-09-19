#!/usr/bin/env python3
"""
Script to check and fix odds data in local SQLite database
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import random

DB_PATH = r"E:\Bettr Bot\betting-bot\data\betting.db"

def check_and_fix_local_odds():
    print("=== LOCAL ODDS DIAGNOSTIC & FIX ===")
    
    conn = sqlite3.connect(DB_PATH)
    
    try:
        # 1. Check total odds
        total_odds = conn.execute("SELECT COUNT(*) FROM odds").fetchone()[0]
        print(f"Total odds records: {total_odds}")
        
        # 2. Check recent odds
        recent_odds = conn.execute("""
            SELECT COUNT(*) FROM odds 
            WHERE timestamp >= datetime('now', '-7 days')
        """).fetchone()[0]
        print(f"Recent odds (last 7 days): {recent_odds}")
        
        # 3. Check upcoming games
        upcoming_games = pd.read_sql_query("""
            SELECT game_id, home_team, away_team, game_date
            FROM games 
            WHERE date(game_date) >= date('now')
            ORDER BY game_date 
            LIMIT 10
        """, conn)
        print(f"\nUpcoming games:")
        print(upcoming_games)
        
        # 4. Check which games have odds
        games_with_odds = conn.execute("""
            SELECT COUNT(DISTINCT g.game_id) 
            FROM games g
            INNER JOIN odds o ON g.game_id = o.game_id
            WHERE date(g.game_date) >= date('now')
        """).fetchone()[0]
        print(f"\nUpcoming games with odds: {games_with_odds}")
        
        # 5. Check team name mismatches
        print("\n=== TEAM NAME COMPARISON ===")
        
        # Get team names from games
        games_teams = pd.read_sql_query("""
            SELECT DISTINCT home_team as team FROM games WHERE date(game_date) >= date('now')
            UNION
            SELECT DISTINCT away_team as team FROM games WHERE date(game_date) >= date('now')
            ORDER BY team
        """, conn)
        print("Teams in games table:")
        print(games_teams['team'].tolist())
        
        # Get team names from odds
        odds_teams = pd.read_sql_query("""
            SELECT DISTINCT team FROM odds 
            ORDER BY team
        """, conn)
        print("\nTeams in odds table:")
        print(odds_teams['team'].tolist() if not odds_teams.empty else "NO ODDS TEAMS")
        
        # 6. If no odds exist, add them
        if total_odds == 0 or games_with_odds == 0:
            print("\n=== ADDING TEST ODDS ===")
            
            # Get games without odds
            games_needing_odds = pd.read_sql_query("""
                SELECT DISTINCT g.game_id, g.home_team, g.away_team, g.game_date
                FROM games g
                LEFT JOIN odds o ON g.game_id = o.game_id
                WHERE date(g.game_date) >= date('now')
                AND date(g.game_date) <= date('now', '+21 days')
                AND o.game_id IS NULL
                ORDER BY g.game_date
                LIMIT 20
            """, conn)
            
            print(f"Adding odds for {len(games_needing_odds)} games...")
            
            sportsbooks = ['DraftKings', 'FanDuel', 'BetMGM']
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            added_count = 0
            
            for _, game in games_needing_odds.iterrows():
                # Create realistic odds
                home_odds = random.randint(-180, 150)
                away_odds = random.randint(-180, 150)
                
                # Make sure they're not both positive
                if home_odds > 0 and away_odds > 0:
                    home_odds = -home_odds
                
                for sportsbook in sportsbooks:
                    # Add small variance per sportsbook
                    home_final = home_odds + random.randint(-10, 10)
                    away_final = away_odds + random.randint(-10, 10)
                    
                    # Insert home team odds
                    conn.execute("""
                        INSERT INTO odds (game_id, team, sportsbook, odds, market, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (game['game_id'], game['home_team'], sportsbook, home_final, 'h2h', timestamp))
                    
                    # Insert away team odds
                    conn.execute("""
                        INSERT INTO odds (game_id, team, sportsbook, odds, market, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (game['game_id'], game['away_team'], sportsbook, away_final, 'h2h', timestamp))
                    
                    added_count += 2
                
                print(f"  {game['away_team']} @ {game['home_team']}: {away_final}/{home_final}")
            
            conn.commit()
            print(f"\nAdded {added_count} odds records!")
            
            # Verify
            new_total = conn.execute("SELECT COUNT(*) FROM odds").fetchone()[0]
            print(f"New total odds: {new_total}")
            
        # 7. Show sample of current odds
        print("\n=== SAMPLE ODDS ===")
        sample_odds = pd.read_sql_query("""
            SELECT game_id, team, sportsbook, odds, market, timestamp
            FROM odds 
            ORDER BY timestamp DESC 
            LIMIT 10
        """, conn)
        print(sample_odds)
        
    finally:
        conn.close()

if __name__ == "__main__":
    check_and_fix_local_odds()