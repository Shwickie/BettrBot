# add_future_games.py - Add upcoming NFL games to database
"""
Add the rest of the 2025 NFL season games that are missing from your database
This will fetch the current NFL schedule and add upcoming games
"""

import os
import pandas as pd
import nfl_data_py as nfl
from sqlalchemy import create_engine, text
from datetime import datetime, date

# Database setup
DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

USE_CLOUD_DB = bool(DATABASE_URL)
SEASON = 2025

def get_engine():
    if USE_CLOUD_DB:
        return create_engine(DATABASE_URL, pool_pre_ping=True)
    else:
        local_db = r"E:/Bettr Bot/betting-bot/data/betting.db"
        return create_engine(f"sqlite:///{local_db}")

# Team name mapping to your database format
TEAM_MAPPING = {
    'ARI': 'Arizona Cardinals', 'ATL': 'Atlanta Falcons', 'BAL': 'Baltimore Ravens',
    'BUF': 'Buffalo Bills', 'CAR': 'Carolina Panthers', 'CHI': 'Chicago Bears',
    'CIN': 'Cincinnati Bengals', 'CLE': 'Cleveland Browns', 'DAL': 'Dallas Cowboys',
    'DEN': 'Denver Broncos', 'DET': 'Detroit Lions', 'GB': 'Green Bay Packers',
    'HOU': 'Houston Texans', 'IND': 'Indianapolis Colts', 'JAX': 'Jacksonville Jaguars',
    'KC': 'Kansas City Chiefs', 'LV': 'Las Vegas Raiders', 'LAC': 'Los Angeles Chargers',
    'LAR': 'Los Angeles Rams', 'MIA': 'Miami Dolphins', 'MIN': 'Minnesota Vikings',
    'NE': 'New England Patriots', 'NO': 'New Orleans Saints', 'NYG': 'New York Giants',
    'NYJ': 'New York Jets', 'PHI': 'Philadelphia Eagles', 'PIT': 'Pittsburgh Steelers',
    'SF': 'San Francisco 49ers', 'SEA': 'Seattle Seahawks', 'TB': 'Tampa Bay Buccaneers',
    'TEN': 'Tennessee Titans', 'WAS': 'Washington Commanders'
}

def normalize_team_name(team_abbr):
    """Convert NFL abbreviation to full team name"""
    return TEAM_MAPPING.get(team_abbr, team_abbr)

def add_future_games():
    """Add missing future games to the database"""
    print("ADDING FUTURE NFL GAMES")
    print("=" * 40)
    
    try:
        print(f"Fetching NFL schedule for {SEASON}...")
        nfl_schedule = nfl.import_schedules([SEASON])
        
        if nfl_schedule is None or nfl_schedule.empty:
            print("No NFL schedule data available")
            return False
            
        print(f"Retrieved {len(nfl_schedule)} total games from NFL")
        
        # Get current date
        today = date.today()
        print(f"Today's date: {today}")
        
        # Filter for future games
        nfl_schedule['gameday'] = pd.to_datetime(nfl_schedule['gameday']).dt.date
        future_games = nfl_schedule[nfl_schedule['gameday'] >= today].copy()
        
        print(f"Found {len(future_games)} future games in NFL schedule")
        
        if future_games.empty:
            print("No future games found in NFL schedule")
            return False
        
        # Show date range
        earliest = future_games['gameday'].min()
        latest = future_games['gameday'].max()
        print(f"Future games date range: {earliest} to {latest}")
        
        # Process the games for your database format
        processed_games = []
        
        for _, game in future_games.iterrows():
            # Convert team abbreviations to full names
            home_team = normalize_team_name(game['home_team'])
            away_team = normalize_team_name(game['away_team'])
            
            # Create game_id (you can adjust this format as needed)
            game_id = f"{game['game_id']}" if 'game_id' in future_games.columns else f"{game['gameday']}_{away_team}_{home_team}".replace(' ', '_')
            
            processed_game = {
                'game_id': game_id,
                'home_team': home_team,
                'away_team': away_team,
                'game_date': game['gameday'],
                'start_time_local': game.get('gametime', '13:00'),  # Default to 1 PM if no time
                'season': SEASON,
                'week': game.get('week', 1),
                'home_score': None,  # Future games don't have scores yet
                'away_score': None
            }
            
            processed_games.append(processed_game)
        
        print(f"Processed {len(processed_games)} games for insertion")
        
        # Show sample of what we're about to add
        print("\nSample future games to add:")
        for game in processed_games[:5]:
            print(f"  {game['game_date']}: {game['away_team']} @ {game['home_team']}")
        
        # Check what's already in database
        engine = get_engine()
        
        with engine.connect() as conn:
            if USE_CLOUD_DB:
                existing_future = conn.execute(text("""
                    SELECT COUNT(*) FROM games 
                    WHERE game_date >= CURRENT_DATE
                """)).scalar()
            else:
                existing_future = conn.execute(text("""
                    SELECT COUNT(*) FROM games 
                    WHERE date(game_date) >= date('now')
                """)).scalar()
            
            print(f"\nCurrently {existing_future} future games in database")
            
            # Insert new games
            games_df = pd.DataFrame(processed_games)
            
            # Remove any duplicates that might already exist
            if USE_CLOUD_DB:
                existing_games = pd.read_sql_query(text("""
                    SELECT game_id, home_team, away_team, game_date 
                    FROM games 
                    WHERE game_date >= CURRENT_DATE
                """), conn)
            else:
                existing_games = pd.read_sql_query(text("""
                    SELECT game_id, home_team, away_team, game_date 
                    FROM games 
                    WHERE date(game_date) >= date('now')
                """), conn)
            
            if not existing_games.empty:
                # Remove games that already exist
                existing_games['game_date'] = pd.to_datetime(existing_games['game_date']).dt.date
                
                # Create a merge key to identify duplicates
                games_df['merge_key'] = games_df['game_date'].astype(str) + '_' + games_df['home_team'] + '_' + games_df['away_team']
                existing_games['merge_key'] = existing_games['game_date'].astype(str) + '_' + existing_games['home_team'] + '_' + existing_games['away_team']
                
                # Filter out existing games
                new_games = games_df[~games_df['merge_key'].isin(existing_games['merge_key'])].copy()
                new_games = new_games.drop('merge_key', axis=1)
            else:
                new_games = games_df.copy()
            
            print(f"After removing duplicates: {len(new_games)} new games to add")
            
            if new_games.empty:
                print("No new games to add - all future games already in database")
                return True
            
            # Insert new games
            new_games.to_sql('games', conn, if_exists='append', index=False, method='multi')
            conn.commit()
            
            print(f"Successfully added {len(new_games)} future games!")
            
            # Verify
            if USE_CLOUD_DB:
                total_future = conn.execute(text("""
                    SELECT COUNT(*) FROM games 
                    WHERE game_date >= CURRENT_DATE
                """)).scalar()
            else:
                total_future = conn.execute(text("""
                    SELECT COUNT(*) FROM games 
                    WHERE date(game_date) >= date('now')
                """)).scalar()
            
            print(f"Total future games now in database: {total_future}")
            
            # Show upcoming games
            if USE_CLOUD_DB:
                upcoming = pd.read_sql_query(text("""
                    SELECT game_date, away_team, home_team 
                    FROM games 
                    WHERE game_date >= CURRENT_DATE 
                    AND game_date <= CURRENT_DATE + INTERVAL '14 days'
                    ORDER BY game_date 
                    LIMIT 10
                """), conn)
            else:
                upcoming = pd.read_sql_query(text("""
                    SELECT game_date, away_team, home_team 
                    FROM games 
                    WHERE date(game_date) >= date('now') 
                    AND date(game_date) <= date('now', '+14 days')
                    ORDER BY game_date 
                    LIMIT 10
                """), conn)
            
            print("\nUpcoming games (next 2 weeks):")
            for _, game in upcoming.iterrows():
                print(f"  {game['game_date']}: {game['away_team']} @ {game['home_team']}")
            
            return True
            
    except Exception as e:
        print(f"Error adding future games: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    if add_future_games():
        print("\n✅ SUCCESS: Future games added!")
        print("Your dashboard should now show upcoming games and predictions.")
        print("\nNext steps:")
        print("1. Refresh your dashboard")
        print("2. Check the predictions section")
        print("3. Run your pipeline to get odds for the new games")
    else:
        print("\n❌ Failed to add future games. Check the error above.")