# migrate_games_cloud_fixed.py - Migrate ALL games (completed AND upcoming)
"""
FIXED VERSION: Migrates both completed and upcoming games to cloud database.
The original version only migrated completed games, which broke the dashboard's
ability to show upcoming games.
"""

import os
import pandas as pd
import sqlite3
from sqlalchemy import create_engine, text

# Database connections
SQLITE_PATH = r"E:\Bettr Bot\betting-bot\data\betting.db"
DATABASE_URL = "postgresql://postgres:YviqtXqcsCIgRzSCofNjbfwgjkYNLydX@maglev.proxy.rlwy.net:54187/railway"
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

def main():
    """Migrate ALL games data (completed AND upcoming)"""
    print("COMPLETE GAMES MIGRATION (FIXED)")
    print("=" * 35)
    
    try:
        # Connect to databases
        sqlite_conn = sqlite3.connect(SQLITE_PATH)
        cloud_engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,
            pool_recycle=280,
            pool_timeout=30,
            connect_args={
                "sslmode": "require",
                "connect_timeout": 30,
                "application_name": "complete_games"
            }
        )
        
        # CRITICAL FIX: Select ALL games, not just completed ones
        # SCHEMA FIX: Remove 'season' column since cloud DB doesn't have it
        print("Reading ALL games from SQLite...")
        games_df = pd.read_sql_query("""
            SELECT game_id, home_team, away_team, game_date, home_score, away_score, start_time_local
            FROM games
            ORDER BY game_date
        """, sqlite_conn)
        
        print(f"Found {len(games_df)} total games")
        
        # Show breakdown
        completed = games_df[games_df['home_score'].notna()]
        upcoming = games_df[games_df['home_score'].isna()]
        print(f"  - Completed games: {len(completed)}")
        print(f"  - Upcoming games: {len(upcoming)}")
        
        if games_df.empty:
            print("No games found!")
            return False
        
        # Check current cloud games
        with cloud_engine.connect() as conn:
            current_count = conn.execute(text("SELECT COUNT(*) FROM games")).scalar()
            print(f"Cloud database currently has {current_count} games")
        
        # Clean and prepare data
        games_df['game_date'] = pd.to_datetime(games_df['game_date']).dt.date
        
        # Handle time column if it exists
        if 'start_time_local' in games_df.columns:
            games_df['start_time_local'] = games_df['start_time_local'].apply(lambda x: 
                pd.to_datetime(str(x)).time() if pd.notna(x) and str(x) != '' else None
            )
        
        # Clear existing games and insert new ones in small chunks
        print("Replacing ALL cloud games data...")
        with cloud_engine.begin() as conn:
            # Clear existing
            deleted = conn.execute(text("DELETE FROM games")).rowcount
            print(f"Deleted {deleted} existing games")
            
            # Insert in small chunks
            chunk_size = 100
            total_inserted = 0
            
            for i in range(0, len(games_df), chunk_size):
                chunk = games_df.iloc[i:i+chunk_size]
                chunk.to_sql(
                    'games',
                    conn,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                total_inserted += len(chunk)
                print(f"  Inserted {total_inserted}/{len(games_df)} games ({(total_inserted/len(games_df)*100):.1f}%)")
        
        # Verify the migration
        with cloud_engine.connect() as conn:
            final_count = conn.execute(text("SELECT COUNT(*) FROM games")).scalar()
            completed_games = conn.execute(text("""
                SELECT COUNT(*) FROM games 
                WHERE home_score IS NOT NULL AND away_score IS NOT NULL
            """)).scalar()
            upcoming_games = conn.execute(text("""
                SELECT COUNT(*) FROM games 
                WHERE home_score IS NULL OR away_score IS NULL
            """)).scalar()
            
            print(f"\nMigration verification:")
            print(f"  Total games in cloud: {final_count}")
            print(f"  Completed games: {completed_games}")
            print(f"  Upcoming games: {upcoming_games}")
            
            # Show sample of upcoming games (what dashboard needs)
            upcoming_sample = conn.execute(text("""
                SELECT home_team, away_team, game_date, start_time_local
                FROM games 
                WHERE home_score IS NULL
                AND game_date >= CURRENT_DATE
                ORDER BY game_date
                LIMIT 5
            """)).fetchall()
            
            print(f"\nSample upcoming games (for dashboard):")
            for row in upcoming_sample:
                print(f"  {row[1]} @ {row[0]} on {row[2]} at {row[3] or 'TBD'}")
        
        sqlite_conn.close()
        
        print("\n" + "=" * 60)
        print("SUCCESS: ALL games data migrated!")
        print("Your dashboard should now show:")
        print("  ✅ Rankings with real W-L records")
        print("  ✅ Upcoming games for betting")
        print("  ✅ Complete game schedule")
        
        return True
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    print(f"\nProcess {'completed successfully' if success else 'failed'}")