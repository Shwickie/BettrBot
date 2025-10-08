# add_2025_games.py - FIXED VERSION
"""
FIXED: Only adds completed 2025 games (not future unplayed games)
Properly handles NFL data column names (gameday -> game_date)
"""

import os
import pandas as pd
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine, text
import nfl_data_py as nfl

def setup_cloud_database():
    """Setup cloud database connection"""
    DATABASE_URL = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"
    if DATABASE_URL.startswith('postgres://'):
        DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
    
    try:
        engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,
            pool_recycle=280,
            pool_timeout=30,
            connect_args={
                "sslmode": "require",
                "connect_timeout": 30,
                "application_name": "bettrbot_2025_games"
            }
        )
        
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        print("SUCCESS: Connected to cloud database")
        return engine
        
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}")
        return None

def fetch_2025_games():
    """Fetch 2025 NFL schedule - ONLY COMPLETED GAMES"""
    try:
        print("Fetching 2025 NFL schedule...")
        
        games_2025 = nfl.import_schedules([2025])
        
        if games_2025.empty:
            print("No 2025 games found from nfl_data_py")
            return pd.DataFrame()
        
        # CRITICAL FIX: Handle NFL data column names
        if 'gameday' in games_2025.columns:
            games_2025['game_date'] = pd.to_datetime(games_2025['gameday']).dt.date
        
        if 'gametime' in games_2025.columns:
            games_2025['start_time_local'] = games_2025['gametime']
        
        # CRITICAL FIX: Only keep COMPLETED games (those with scores)
        if 'home_score' in games_2025.columns and 'away_score' in games_2025.columns:
            completed_games = games_2025[
                games_2025['home_score'].notna() & 
                games_2025['away_score'].notna()
            ].copy()
        else:
            # If no scores in data, don't add any games
            print("Warning: No score data in NFL schedule")
            return pd.DataFrame()
        
        print(f"Found {len(completed_games)} COMPLETED games for 2025")
        
        # Ensure we have required columns
        if 'season' not in completed_games.columns:
            completed_games['season'] = 2025
        
        if 'week' not in completed_games.columns:
            # Calculate week from game_date
            completed_games['week'] = 1  # Default
        
        # Create game_id if missing
        if 'game_id' not in completed_games.columns:
            completed_games['game_id'] = completed_games.apply(
                lambda row: f"{row['season']}_{row['week']:02d}_{row['away_team']}_{row['home_team']}", 
                axis=1
            )
        
        return completed_games
        
    except Exception as e:
        print(f"Error fetching 2025 games: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def insert_2025_games(engine, games_df):
    """Insert 2025 games into database"""
    try:
        if games_df.empty:
            print("No games to insert")
            return True
        
        print(f"Inserting {len(games_df)} COMPLETED games...")
        
        games_clean = games_df.copy()
        
        # Ensure proper data types
        games_clean['season'] = games_clean['season'].astype(int)
        games_clean['week'] = games_clean['week'].astype(int)
        
        # game_date should already be date type from fetch function
        if not isinstance(games_clean['game_date'].iloc[0], date):
            games_clean['game_date'] = pd.to_datetime(games_clean['game_date']).dt.date
        
        # Handle time column
        if 'start_time_local' in games_clean.columns:
            # Only convert if it's not already time type
            if not isinstance(games_clean['start_time_local'].iloc[0], (type(None), pd._libs.tslibs.nattype.NaTType)):
                try:
                    games_clean['start_time_local'] = pd.to_datetime(
                        games_clean['start_time_local'], format='%H:%M:%S', errors='coerce'
                    ).dt.time
                except:
                    pass  # Keep as is if conversion fails
        
        print(f"Date range: {games_clean['game_date'].min()} to {games_clean['game_date'].max()}")
        
        # Check if 2025 games already exist
        with engine.connect() as conn:
            existing_2025 = conn.execute(text("""
                SELECT COUNT(*) FROM games WHERE season = 2025
            """)).scalar()
            
            if existing_2025 > 0:
                print(f"Found {existing_2025} existing 2025 games, clearing them first...")
                conn.execute(text("DELETE FROM games WHERE season = 2025"))
                conn.commit()
        
        # Select only columns that exist in database (no game_type!)
        db_columns = ['game_id', 'season', 'week', 'game_date', 'start_time_local', 
                      'home_team', 'away_team', 'home_score', 'away_score']
        
        insert_cols = [col for col in db_columns if col in games_clean.columns]
        games_insert = games_clean[insert_cols].copy()
        
        # Insert new games
        with engine.begin() as conn:
            games_insert.to_sql(
                'games',
                conn,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=100
            )
            
            print(f"SUCCESS: Inserted {len(games_insert)} games for 2025")
            
            count_2025 = conn.execute(text("SELECT COUNT(*) FROM games WHERE season = 2025")).scalar()
            total_count = conn.execute(text("SELECT COUNT(*) FROM games")).scalar()
            
            print(f"Database now contains:")
            print(f"  - 2025 games: {count_2025}")
            print(f"  - Total games: {total_count}")
        
        return True
        
    except Exception as e:
        print(f"ERROR: Failed to insert games: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main execution"""
    print("ADDING 2025 NFL GAMES (COMPLETED ONLY)")
    print("=" * 50)
    
    engine = setup_cloud_database()
    if not engine:
        return False
    
    try:
        with engine.connect() as conn:
            total_games = conn.execute(text("SELECT COUNT(*) FROM games")).scalar()
            games_2025 = conn.execute(text("SELECT COUNT(*) FROM games WHERE season = 2025")).scalar()
            
            print(f"Current database status:")
            print(f"  - Total games: {total_games}")
            print(f"  - 2025 games: {games_2025}")
        
        # Fetch COMPLETED 2025 games only
        games_df = fetch_2025_games()
        
        if games_df.empty:
            print("No completed games to add")
            return True
        
        # Insert games
        if not insert_2025_games(engine, games_df):
            return False
        
        print("\n" + "=" * 50)
        print("SUCCESS: 2025 completed games added!")
        print("=" * 50)
        
        return True
        
    except Exception as e:
        print(f"ERROR: Process failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    print(f"\nProcess {'completed successfully' if success else 'failed'}")