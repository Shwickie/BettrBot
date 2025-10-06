# fix_game_dates.py - Fix incorrect 2026 game dates
"""
Fix the game dates that got set to 2026 instead of 2025
This script will correct all games currently dated in 2026 back to 2025
"""

import os
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# Database setup
DATABASE_URL = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"
if DATABASE_URL and DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

USE_CLOUD_DB = bool(DATABASE_URL)

def get_engine():
    if USE_CLOUD_DB:
        return create_engine(DATABASE_URL, pool_pre_ping=True)
    else:
        local_db = r"E:/Bettr Bot/betting-bot/data/betting.db"
        return create_engine(f"sqlite:///{local_db}")

def fix_game_dates():
    """Fix games incorrectly dated in 2026"""
    print("FIXING GAME DATES")
    print("=" * 40)
    
    engine = get_engine()
    
    try:
        with engine.connect() as conn:
            # First, check what we have
            if USE_CLOUD_DB:
                check_query = """
                SELECT 
                    EXTRACT(YEAR FROM game_date) as year,
                    COUNT(*) as count,
                    MIN(game_date) as earliest,
                    MAX(game_date) as latest
                FROM games 
                GROUP BY EXTRACT(YEAR FROM game_date)
                ORDER BY year
                """
            else:
                check_query = """
                SELECT 
                    strftime('%Y', game_date) as year,
                    COUNT(*) as count,
                    MIN(game_date) as earliest,
                    MAX(game_date) as latest
                FROM games 
                GROUP BY strftime('%Y', game_date)
                ORDER BY year
                """
            
            result = conn.execute(text(check_query))
            years_data = result.fetchall()
            
            print("Current game date distribution:")
            for row in years_data:
                print(f"  {row[0]}: {row[1]} games ({row[2]} to {row[3]})")
            
            # Check for 2026 games specifically
            if USE_CLOUD_DB:
                games_2026 = conn.execute(text("""
                    SELECT COUNT(*) FROM games 
                    WHERE EXTRACT(YEAR FROM game_date) = 2026
                """)).scalar()
            else:
                games_2026 = conn.execute(text("""
                    SELECT COUNT(*) FROM games 
                    WHERE strftime('%Y', game_date) = '2026'
                """)).scalar()
            
            if games_2026 == 0:
                print("No games found in 2026 - dates look correct!")
                return True
            
            print(f"\nFound {games_2026} games incorrectly dated in 2026")
            
            # Fix: Convert 2026-01-04 to 2025 dates
            # The most likely scenario is these should be current NFL season games
            
            if USE_CLOUD_DB:
                # PostgreSQL version
                update_query = """
                UPDATE games 
                SET game_date = game_date - INTERVAL '1 year'
                WHERE EXTRACT(YEAR FROM game_date) = 2026
                """
            else:
                # SQLite version
                update_query = """
                UPDATE games 
                SET game_date = date(game_date, '-1 year')
                WHERE strftime('%Y', game_date) = '2026'
                """
            
            print("Fixing dates (subtracting 1 year from 2026 games)...")
            result = conn.execute(text(update_query))
            updated_count = result.rowcount
            
            conn.commit()
            
            print(f"Successfully updated {updated_count} games")
            
            # Verify the fix
            if USE_CLOUD_DB:
                check_2026 = conn.execute(text("""
                    SELECT COUNT(*) FROM games 
                    WHERE EXTRACT(YEAR FROM game_date) = 2026
                """)).scalar()
                
                current_games = conn.execute(text("""
                    SELECT COUNT(*) FROM games 
                    WHERE game_date >= CURRENT_DATE 
                    AND game_date <= CURRENT_DATE + INTERVAL '30 days'
                """)).scalar()
            else:
                check_2026 = conn.execute(text("""
                    SELECT COUNT(*) FROM games 
                    WHERE strftime('%Y', game_date) = '2026'
                """)).scalar()
                
                current_games = conn.execute(text("""
                    SELECT COUNT(*) FROM games 
                    WHERE date(game_date) >= date('now') 
                    AND date(game_date) <= date('now', '+30 days')
                """)).scalar()
            
            print(f"\nVerification:")
            print(f"  Games still in 2026: {check_2026}")
            print(f"  Upcoming games (next 30 days): {current_games}")
            
            if check_2026 == 0 and current_games > 0:
                print("\n✅ SUCCESS: Game dates have been fixed!")
                print("Your dashboard should now show upcoming games correctly.")
            else:
                print("\n⚠️  Issue may persist - check the data manually")
            
            return True
            
    except Exception as e:
        print(f"Error fixing dates: {e}")
        return False

def show_sample_games():
    """Show a sample of games after fixing"""
    engine = get_engine()
    
    try:
        with engine.connect() as conn:
            if USE_CLOUD_DB:
                sample_query = """
                SELECT game_date, away_team, home_team, home_score, away_score
                FROM games 
                WHERE game_date >= CURRENT_DATE - INTERVAL '7 days'
                AND game_date <= CURRENT_DATE + INTERVAL '14 days'
                ORDER BY game_date 
                LIMIT 10
                """
            else:
                sample_query = """
                SELECT game_date, away_team, home_team, home_score, away_score
                FROM games 
                WHERE date(game_date) >= date('now', '-7 days')
                AND date(game_date) <= date('now', '+14 days')
                ORDER BY game_date 
                LIMIT 10
                """
            
            result = conn.execute(text(sample_query))
            games = result.fetchall()
            
            print("\nSample games (past week + next 2 weeks):")
            for game in games:
                status = "FINAL" if game[3] is not None else "UPCOMING"
                score = f"{game[3]}-{game[4]}" if game[3] is not None else "TBD"
                print(f"  {game[0]}: {game[1]} @ {game[2]} ({score}) [{status}]")
                
    except Exception as e:
        print(f"Error showing sample games: {e}")

if __name__ == "__main__":
    if fix_game_dates():
        show_sample_games()
        print("\n🎉 Game dates fixed! Try refreshing your dashboard.")
    else:
        print("\n❌ Failed to fix game dates. Check the error above.")