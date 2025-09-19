# check_cloud_data.py - Verify your cloud database has games
"""
Quick script to check if your cloud database has the necessary data
"""

import os
import pandas as pd
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine, text

def main():
    DATABASE_URL = os.environ.get("DATABASE_URL")
    if not DATABASE_URL:
        print("ERROR: No DATABASE_URL environment variable")
        return
    
    # Fix postgres:// URLs
    if DATABASE_URL.startswith('postgres://'):
        DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
    
    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        
        with engine.connect() as conn:
            # Check games table
            games_count = conn.execute(text("SELECT COUNT(*) FROM games")).scalar()
            print(f"Games in database: {games_count}")
            
            if games_count > 0:
                # Check date range
                date_range = conn.execute(text("""
                    SELECT MIN(game_date) as earliest, MAX(game_date) as latest 
                    FROM games
                """)).fetchone()
                print(f"Game date range: {date_range[0]} to {date_range[1]}")
                
                # Check recent/future games
                today = date.today()
                future_games = conn.execute(text("""
                    SELECT COUNT(*) FROM games WHERE game_date >= :today
                """), {"today": today}).scalar()
                print(f"Future games (from {today}): {future_games}")
                
                # Sample games
                sample = conn.execute(text("""
                    SELECT away_team, home_team, game_date 
                    FROM games 
                    WHERE game_date >= :today
                    ORDER BY game_date 
                    LIMIT 5
                """), {"today": today}).fetchall()
                
                print("\nSample upcoming games:")
                for game in sample:
                    print(f"  {game[0]} @ {game[1]} on {game[2]}")
            
            # Check team_season_summary
            teams_count = conn.execute(text("SELECT COUNT(*) FROM team_season_summary")).scalar()
            print(f"\nTeam season records: {teams_count}")
            
            if teams_count > 0:
                sample_teams = conn.execute(text("""
                    SELECT team, season, power_score, games_played
                    FROM team_season_summary 
                    ORDER BY power_score DESC 
                    LIMIT 5
                """)).fetchall()
                
                print("Top teams by power:")
                for team in sample_teams:
                    print(f"  {team[0]} ({team[1]}): {team[2]:.1f} power, {team[3]} GP")
            
            # Check odds table
            try:
                odds_count = conn.execute(text("SELECT COUNT(*) FROM odds")).scalar()
                print(f"\nOdds records: {odds_count}")
            except:
                print("\nOdds table: Not found or empty")
                
        print(f"\nDatabase status: {'✅ Ready' if games_count > 0 and teams_count > 0 else '❌ Needs population'}")
        
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    main()