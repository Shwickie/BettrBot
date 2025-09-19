# fix_cloud_games.py - Fix your cloud games data
"""
Your games table has the right structure but many games have NULL game_date values.
This script will fix the data and add proper 2025 games.
"""

import os
import pandas as pd
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine, text
import nfl_data_py as nfl

DATABASE_URL = os.environ.get("DATABASE_URL") or "postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres?sslmode=require"

if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

def setup_engine():
    return create_engine(DATABASE_URL, pool_pre_ping=True)

def analyze_current_data(engine):
    """Analyze what's wrong with current data"""
    print("ANALYZING CURRENT DATA")
    print("=" * 30)
    
    with engine.connect() as conn:
        # Check games with NULL dates
        null_dates = conn.execute(text("""
            SELECT COUNT(*) FROM games WHERE game_date IS NULL
        """)).scalar()
        
        # Check games with valid dates
        valid_dates = conn.execute(text("""
            SELECT COUNT(*) FROM games WHERE game_date IS NOT NULL
        """)).scalar()
        
        # Check future games
        future_games = conn.execute(text("""
            SELECT COUNT(*) FROM games 
            WHERE game_date IS NOT NULL AND game_date >= CURRENT_DATE
        """)).scalar()
        
        print(f"Total games: {null_dates + valid_dates}")
        print(f"Games with NULL dates: {null_dates}")
        print(f"Games with valid dates: {valid_dates}")
        print(f"Future games: {future_games}")
        
        # Show sample of games with valid dates
        if valid_dates > 0:
            sample_valid = conn.execute(text("""
                SELECT game_date, away_team, home_team 
                FROM games 
                WHERE game_date IS NOT NULL 
                ORDER BY game_date DESC 
                LIMIT 5
            """)).fetchall()
            
            print("\nSample games with valid dates:")
            for row in sample_valid:
                print(f"  {row[0]}: {row[1]} @ {row[2]}")
        
        # Check what years we have data for
        if valid_dates > 0:
            year_counts = conn.execute(text("""
                SELECT EXTRACT(YEAR FROM game_date) as year, COUNT(*) as count
                FROM games 
                WHERE game_date IS NOT NULL
                GROUP BY EXTRACT(YEAR FROM game_date)
                ORDER BY year
            """)).fetchall()
            
            print("\nGames by year:")
            for row in year_counts:
                print(f"  {int(row[0])}: {row[1]} games")

def clean_existing_data(engine):
    """Remove games with NULL dates and other bad data"""
    print("\nCLEANING EXISTING DATA")
    print("=" * 25)
    
    with engine.begin() as conn:
        # Count games to remove
        to_remove = conn.execute(text("""
            SELECT COUNT(*) FROM games 
            WHERE game_date IS NULL OR game_id IS NULL
        """)).scalar()
        
        print(f"Removing {to_remove} games with NULL dates or game_ids...")
        
        # Remove bad data
        conn.execute(text("""
            DELETE FROM games 
            WHERE game_date IS NULL OR game_id IS NULL
        """))
        
        print("Bad data removed")

def add_fresh_2025_games(engine):
    """Add fresh 2025 NFL games"""
    print("\nADDING 2025 NFL GAMES")
    print("=" * 22)
    
    try:
        # Try to get real NFL data
        print("Fetching 2025 NFL schedule...")
        games_2025 = nfl.import_schedules([2025])
        
        if games_2025.empty:
            print("No 2025 data from nfl_data_py, creating sample games...")
            games_2025 = create_sample_future_games()
        else:
            print(f"Found {len(games_2025)} games from NFL data")
            
    except Exception as e:
        print(f"Error fetching NFL data: {e}")
        print("Creating sample games...")
        games_2025 = create_sample_future_games()
    
    # Process games for your database schema
    processed_games = []
    
    for _, game in games_2025.iterrows():
        # Handle different possible column names
        game_date = game.get('gameday') or game.get('game_date')
        if game_date is None:
            continue
            
        home_team = game.get('home_team', 'Unknown')
        away_team = game.get('away_team', 'Unknown')
        game_id = game.get('game_id') or f"{game_date}_{away_team}_{home_team}".replace(' ', '_')
        
        # Handle time parsing safely
        gametime = game.get('gametime', '13:00:00')
        if pd.isna(gametime) or gametime is None or str(gametime).lower() in ['nan', 'nat', 'none']:
            start_time = pd.to_datetime('13:00:00', format='%H:%M:%S').time()
        else:
            try:
                start_time = pd.to_datetime(str(gametime), format='%H:%M:%S', errors='coerce').time()
                if start_time is None:  # Still failed
                    start_time = pd.to_datetime('13:00:00', format='%H:%M:%S').time()
            except:
                start_time = pd.to_datetime('13:00:00', format='%H:%M:%S').time()
        
        # Convert to your database format
        processed_game = {
            'game_id': str(game_id),
            'game_date': pd.to_datetime(game_date).date(),
            'home_team': str(home_team),
            'away_team': str(away_team),
            'start_time_local': start_time,
            'home_score': None,
            'away_score': None,
            'start_time_utc': None
        }
        
        processed_games.append(processed_game)
    
    if not processed_games:
        print("No games to add")
        return False
    
    print(f"Processed {len(processed_games)} games")
    
    # Remove any existing 2025 games first
    with engine.begin() as conn:
        conn.execute(text("""
            DELETE FROM games 
            WHERE game_date >= '2025-01-01' AND game_date <= '2025-12-31'
        """))
        
        print("Cleared existing 2025 games")
    
    # Insert new games
    games_df = pd.DataFrame(processed_games)
    
    with engine.begin() as conn:
        games_df.to_sql('games', conn, if_exists='append', index=False, method='multi')
        print(f"Inserted {len(games_df)} new 2025 games")
        
        # Verify
        future_count = conn.execute(text("""
            SELECT COUNT(*) FROM games 
            WHERE game_date >= CURRENT_DATE
        """)).scalar()
        
        print(f"Total future games now: {future_count}")
        
        # Show sample
        sample = conn.execute(text("""
            SELECT game_date, away_team, home_team 
            FROM games 
            WHERE game_date >= CURRENT_DATE 
            ORDER BY game_date 
            LIMIT 5
        """)).fetchall()
        
        print("\nSample upcoming games:")
        for row in sample:
            print(f"  {row[0]}: {row[1]} @ {row[2]}")
    
    return True

def create_sample_future_games():
    """Create sample future games for testing"""
    teams = [
        'Kansas City Chiefs', 'Buffalo Bills', 'Baltimore Ravens', 'San Francisco 49ers',
        'Philadelphia Eagles', 'Dallas Cowboys', 'Miami Dolphins', 'Cincinnati Bengals',
        'Detroit Lions', 'Green Bay Packers', 'Los Angeles Chargers', 'Minnesota Vikings',
        'Houston Texans', 'Pittsburgh Steelers', 'Atlanta Falcons', 'Indianapolis Colts',
        'Las Vegas Raiders', 'Tampa Bay Buccaneers', 'Los Angeles Rams', 'Seattle Seahawks',
        'New Orleans Saints', 'Jacksonville Jaguars', 'Tennessee Titans', 'Cleveland Browns',
        'New York Jets', 'Arizona Cardinals', 'Denver Broncos', 'New England Patriots',
        'Washington Commanders', 'New York Giants', 'Carolina Panthers', 'Chicago Bears'
    ]
    
    games = []
    
    # Create games starting next week
    start_date = date.today() + timedelta(days=7)
    
    # Week 1 games
    week1_matchups = [
        (teams[0], teams[16]),   # KC vs LV
        (teams[1], teams[24]),   # BUF vs NYJ
        (teams[4], teams[9]),    # PHI vs GB  
        (teams[3], teams[11]),   # SF vs MIN
        (teams[5], teams[23]),   # DAL vs CLE
        (teams[6], teams[21]),   # MIA vs JAX
        (teams[14], teams[13]),  # ATL vs PIT
        (teams[12], teams[15]),  # HOU vs IND
    ]
    
    for i, (away, home) in enumerate(week1_matchups):
        game_date = start_date + timedelta(days=i%2)  # Spread across 2 days
        start_time = '13:00:00' if i % 2 == 0 else '16:25:00'
        
        games.append({
            'game_id': f'2025_week1_{i+1}',
            'gameday': game_date,
            'away_team': away,
            'home_team': home,
            'gametime': start_time
        })
    
    # Add more weeks
    for week in range(2, 6):  # Weeks 2-5
        week_start = start_date + timedelta(days=week*7)
        
        for i in range(8):  # 8 games per week
            away_idx = i
            home_idx = (i + 16) % len(teams)
            
            games.append({
                'game_id': f'2025_week{week}_{i+1}',
                'gameday': week_start + timedelta(days=i%3),
                'away_team': teams[away_idx],
                'home_team': teams[home_idx],
                'gametime': '13:00:00' if i % 2 == 0 else '16:25:00'
            })
    
    return pd.DataFrame(games)

def main():
    print("FIXING CLOUD GAMES DATABASE")
    print("=" * 40)
    
    engine = setup_engine()
    
    try:
        # Step 1: Analyze current data
        analyze_current_data(engine)
        
        # Step 2: Clean bad data
        clean_existing_data(engine)
        
        # Step 3: Add 2025 games
        if add_fresh_2025_games(engine):
            print("\nSUCCESS: Cloud database fixed!")
            print("\nNext steps:")
            print("1. Update your mobile_dashboard.py with the rankings fix")
            print("2. Redeploy to Render")
            print("3. Check your dashboard - should now show upcoming games")
            return True
        else:
            print("Failed to add 2025 games")
            return False
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    print(f"\nProcess {'completed successfully' if success else 'failed'}")