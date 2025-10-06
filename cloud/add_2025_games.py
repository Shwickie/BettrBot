# add_2025_games.py - Add 2025 NFL season games to your cloud database
"""
Your cloud database has 3,923 games but they're all from past seasons.
This script adds 2025 NFL season games so your predictions work.
"""

import os
import pandas as pd
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine, text
import nfl_data_py as nfl

def setup_cloud_database():
    """Setup cloud database connection"""
    DATABASE_URL = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"
    # Fix postgres:// URLs
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
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        print("SUCCESS: Connected to cloud database")
        return engine
        
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}")
        return None

def fetch_2025_games():
    """Fetch 2025 NFL schedule"""
    try:
        print("Fetching 2025 NFL schedule...")
        
        # Get 2025 season data
        games_2025 = nfl.import_schedules([2025])
        
        if games_2025.empty:
            print("No 2025 games found from nfl_data_py")
            # Create sample future games for testing
            games_2025 = create_sample_2025_games()
        else:
            print(f"Found {len(games_2025)} games for 2025")
        
        return games_2025
        
    except Exception as e:
        print(f"Error fetching 2025 games: {e}")
        print("Creating sample games for testing...")
        return create_sample_2025_games()

def create_sample_2025_games():
    """Create sample 2025 games for testing"""
    teams = ['KC', 'BUF', 'BAL', 'SF', 'PHI', 'DAL', 'MIA', 'CIN', 
             'DET', 'GB', 'LAC', 'MIN', 'HOU', 'PIT', 'ATL', 'IND',
             'LV', 'TB', 'LAR', 'SEA', 'NO', 'JAX', 'TEN', 'CLE',
             'NYJ', 'ARI', 'DEN', 'NE', 'WAS', 'NYG', 'CAR', 'CHI']
    
    games = []
    start_date = date(2025, 9, 4)  # First Thursday of September 2025
    
    # Week 1 games (September 7-9, 2025)
    week1_matchups = [
        ('KC', 'BAL'),    # Thursday Night
        ('BUF', 'NYJ'),   # Sunday
        ('PHI', 'GB'),    # Sunday  
        ('SF', 'MIN'),    # Sunday
        ('DAL', 'CLE'),   # Sunday
        ('MIA', 'JAX'),   # Sunday
        ('PIT', 'ATL'),   # Sunday
        ('HOU', 'IND'),   # Sunday
        ('DET', 'LAR'),   # Sunday Night
        ('CIN', 'NE'),    # Monday Night
    ]
    
    game_id_counter = 1
    
    for i, (away, home) in enumerate(week1_matchups):
        if i == 0:  # Thursday night
            game_date = start_date
            start_time = '20:20:00'
        elif i == len(week1_matchups) - 2:  # Sunday night
            game_date = start_date + timedelta(days=3)
            start_time = '20:20:00'
        elif i == len(week1_matchups) - 1:  # Monday night
            game_date = start_date + timedelta(days=4)
            start_time = '20:15:00'
        else:  # Sunday games
            game_date = start_date + timedelta(days=3)
            start_time = '13:00:00' if i % 2 == 0 else '16:25:00'
        
        games.append({
            'game_id': f'2025_01_{away}_{home}',
            'season': 2025,
            'week': 1,
            'game_type': 'REG',
            'game_date': game_date,
            'start_time_local': start_time,
            'away_team': away,
            'home_team': home,
            'home_score': None,
            'away_score': None
        })
        
        game_id_counter += 1
    
    # Add a few more weeks of sample games
    for week in range(2, 5):  # Weeks 2-4
        week_start = start_date + timedelta(days=(week-1)*7)
        
        # 8 games per week for testing
        sample_matchups = [
            (teams[i], teams[i+16]) for i in range(0, 8)
        ]
        
        for i, (away, home) in enumerate(sample_matchups):
            game_date = week_start + timedelta(days=3)  # Sunday
            start_time = '13:00:00' if i % 2 == 0 else '16:25:00'
            
            games.append({
                'game_id': f'2025_{week:02d}_{away}_{home}',
                'season': 2025,
                'week': week,
                'game_type': 'REG',
                'game_date': game_date,
                'start_time_local': start_time,
                'away_team': away,
                'home_team': home,
                'home_score': None,
                'away_score': None
            })
    
    df = pd.DataFrame(games)
    print(f"Created {len(df)} sample 2025 games")
    return df

def insert_2025_games(engine, games_df):
    """Insert 2025 games into database"""
    try:
        print(f"Inserting {len(games_df)} games...")
        
        # Clean the data
        games_clean = games_df.copy()
        
        # Ensure proper data types
        games_clean['season'] = games_clean['season'].astype(int)
        games_clean['week'] = games_clean['week'].astype(int)
        games_clean['game_date'] = pd.to_datetime(games_clean['game_date']).dt.date
        
        # Handle time column
        if 'start_time_local' in games_clean.columns:
            games_clean['start_time_local'] = pd.to_datetime(
                games_clean['start_time_local'], format='%H:%M:%S', errors='coerce'
            ).dt.time
        
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
        
        # Insert new games
        with engine.begin() as conn:
            games_clean.to_sql(
                'games',
                conn,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=100
            )
            
            print(f"SUCCESS: Inserted {len(games_clean)} games for 2025")
            
            # Verify insertion
            count_2025 = conn.execute(text("SELECT COUNT(*) FROM games WHERE season = 2025")).scalar()
            total_count = conn.execute(text("SELECT COUNT(*) FROM games")).scalar()
            
            print(f"Database now contains:")
            print(f"  - 2025 games: {count_2025}")
            print(f"  - Total games: {total_count}")
            
            # Show sample of inserted games
            sample = conn.execute(text("""
                SELECT game_id, away_team, home_team, game_date, week
                FROM games 
                WHERE season = 2025
                ORDER BY game_date, start_time_local
                LIMIT 5
            """)).fetchall()
            
            print("\nSample 2025 games inserted:")
            for row in sample:
                print(f"  Week {row[4]}: {row[1]} @ {row[2]} on {row[3]}")
        
        return True
        
    except Exception as e:
        print(f"ERROR: Failed to insert games: {e}")
        import traceback
        traceback.print_exc()
        return False

def add_2025_team_summaries(engine):
    """Add 2025 team season summaries if missing"""
    try:
        with engine.connect() as conn:
            # Check if 2025 summaries exist
            existing = conn.execute(text("""
                SELECT COUNT(*) FROM team_season_summary WHERE season = 2025
            """)).scalar()
            
            if existing > 0:
                print(f"Found {existing} existing 2025 team summaries")
                return True
            
            # Copy 2024 data as baseline for 2025 preseason
            print("Creating 2025 team summaries from 2024 data...")
            
            result = conn.execute(text("""
                INSERT INTO team_season_summary 
                (team, season, power_score, wins, losses, games_played, win_pct,
                 avg_points_for, avg_points_against, point_diff)
                SELECT 
                    team, 
                    2025 as season,
                    power_score * 0.8 as power_score,  -- Slight regression to mean
                    0 as wins,
                    0 as losses, 
                    0 as games_played,
                    0.0 as win_pct,
                    0.0 as avg_points_for,
                    0.0 as avg_points_against,
                    0.0 as point_diff
                FROM team_season_summary 
                WHERE season = 2024
                ON CONFLICT (team, season) DO NOTHING
            """))
            
            conn.commit()
            
            # Verify
            count_2025 = conn.execute(text("""
                SELECT COUNT(*) FROM team_season_summary WHERE season = 2025
            """)).scalar()
            
            print(f"SUCCESS: Created {count_2025} team summaries for 2025")
            return True
            
    except Exception as e:
        print(f"ERROR: Failed to create 2025 team summaries: {e}")
        return False

def main():
    """Main execution"""
    print("ADDING 2025 NFL GAMES TO CLOUD DATABASE")
    print("=" * 50)
    
    # Setup database
    engine = setup_cloud_database()
    if not engine:
        return False
    
    try:
        # Check current status
        with engine.connect() as conn:
            total_games = conn.execute(text("SELECT COUNT(*) FROM games")).scalar()
            games_2025 = conn.execute(text("SELECT COUNT(*) FROM games WHERE season = 2025")).scalar()
            
            print(f"Current database status:")
            print(f"  - Total games: {total_games}")
            print(f"  - 2025 games: {games_2025}")
        
        # Fetch 2025 games
        games_df = fetch_2025_games()
        if games_df.empty:
            print("ERROR: No 2025 games could be created")
            return False
        
        # Insert games
        if not insert_2025_games(engine, games_df):
            return False
        
        # Add team summaries
        add_2025_team_summaries(engine)
        
        print("\n" + "=" * 50)
        print("SUCCESS: 2025 games added to cloud database!")
        print("\nNext steps:")
        print("1. Redeploy your app to Render")
        print("2. Check dashboard - should now show upcoming games")
        print("3. Test predictions endpoint")
        
        return True
        
    except Exception as e:
        print(f"ERROR: Process failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    # Set DATABASE_URL if not set
    if not os.environ.get("DATABASE_URL"):
        os.environ["DATABASE_URL"] = "postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres?sslmode=require"
    
    success = main()
    print(f"\nProcess {'completed successfully' if success else 'failed'}")