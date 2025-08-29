#!/usr/bin/env python3
"""
Final NFL Schedule Updater - Handles the id column requirement
Gets current NFL schedule and properly adds it to your database
"""

import requests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import json

# Config
API_KEY = '2ea42e6f961b41a105cd8dac8a3490a8'
SPORT = 'americanfootball_nfl'
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"

def get_next_id():
    """Get the next available ID for the games table"""
    engine = create_engine(DB_PATH)
    
    try:
        with engine.connect() as conn:
            result = pd.read_sql(text("SELECT MAX(id) as max_id FROM games"), conn)
            max_id = result.iloc[0]['max_id']
            return (max_id + 1) if max_id is not None else 1
    except:
        return 1

def get_current_nfl_schedule():
    """Get current NFL schedule from The Odds API"""
    print("\n🏈 FETCHING CURRENT NFL SCHEDULE")
    print("=" * 40)
    
    url = f'https://api.the-odds-api.com/v4/sports/{SPORT}/odds'
    params = {
        'apiKey': API_KEY,
        'regions': 'us',
        'markets': 'h2h',
        'oddsFormat': 'decimal'
    }
    
    try:
        response = requests.get(url, params=params)
        
        if response.status_code != 200:
            print(f"❌ Failed to fetch schedule: {response.status_code}")
            print(f"Error: {response.text}")
            return []
        
        games = response.json()
        print(f"✅ Found {len(games)} games from API")
        
        schedule_data = []
        
        for game in games:
            game_id = game.get('id')
            home_team = game.get('home_team')
            away_team = game.get('away_team')
            commence_time = game.get('commence_time')
            
            if not all([game_id, home_team, away_team, commence_time]):
                continue
            
            # Parse the datetime
            try:
                game_datetime = datetime.fromisoformat(commence_time.replace('Z', '+00:00'))
                game_date = game_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
                start_time_utc = commence_time
                start_time_local = game_datetime.strftime('%Y-%m-%d %H:%M:%S')
            except:
                continue
            
            schedule_data.append({
                'game_id': game_id,
                'home_team': home_team,
                'away_team': away_team,
                'game_date': game_date,
                'start_time_utc': start_time_utc,
                'start_time_local': start_time_local,
                'home_score': None,  # Will be filled after game
                'away_score': None   # Will be filled after game
            })
        
        return schedule_data
        
    except Exception as e:
        print(f"❌ Error fetching schedule: {e}")
        return []

def update_games_database(schedule_data):
    """Update the games table with new schedule data"""
    print(f"\n💾 UPDATING GAMES DATABASE")
    print("=" * 35)
    
    if not schedule_data:
        print("❌ No schedule data to update")
        return
    
    engine = create_engine(DB_PATH)
    
    try:
        with engine.begin() as conn:
            # Get existing game IDs to avoid duplicates
            existing_games = pd.read_sql(text("""
                SELECT DISTINCT game_id FROM games
            """), conn)
            existing_ids = set(existing_games['game_id'].tolist())
            
            print(f"📊 Existing games in database: {len(existing_ids)}")
            
            # Get next available ID
            next_id = get_next_id()
            
            new_games = 0
            updated_games = 0
            current_id = next_id
            
            for game in schedule_data:
                if game['game_id'] in existing_ids:
                    # Update existing game
                    conn.execute(text("""
                        UPDATE games 
                        SET home_team = :home_team,
                            away_team = :away_team,
                            game_date = :game_date,
                            start_time_utc = :start_time_utc,
                            start_time_local = :start_time_local
                        WHERE game_id = :game_id
                    """), game)
                    updated_games += 1
                else:
                    # Insert new game with proper ID
                    game_with_id = game.copy()
                    game_with_id['id'] = current_id
                    
                    conn.execute(text("""
                        INSERT INTO games (id, game_id, home_team, away_team, game_date, 
                                         start_time_utc, start_time_local, home_score, away_score)
                        VALUES (:id, :game_id, :home_team, :away_team, :game_date,
                               :start_time_utc, :start_time_local, :home_score, :away_score)
                    """), game_with_id)
                    new_games += 1
                    current_id += 1
            
            print(f"✅ Added {new_games} new games")
            print(f"🔄 Updated {updated_games} existing games")
            
            # Show some sample new games
            if new_games > 0 or updated_games > 0:
                sample_games = pd.read_sql(text("""
                    SELECT home_team, away_team, game_date, game_id
                    FROM games 
                    WHERE game_date >= datetime('now')
                    ORDER BY game_date
                    LIMIT 10
                """), conn)
                
                print(f"\n📅 UPCOMING GAMES SAMPLE:")
                for _, game in sample_games.iterrows():
                    game_date = game['game_date'][:10]  # Just the date part
                    game_id_short = game['game_id'][:8]
                    print(f"  {game_date}: {game['away_team']} @ {game['home_team']} (ID: {game_id_short}...)")
            
    except Exception as e:
        print(f"❌ Error updating database: {e}")
        import traceback
        traceback.print_exc()

def verify_schedule_update():
    """Verify that the schedule update worked"""
    print(f"\n✅ VERIFYING SCHEDULE UPDATE")
    print("=" * 35)
    
    engine = create_engine(DB_PATH)
    
    try:
        with engine.connect() as conn:
            # Count upcoming games
            upcoming_count = pd.read_sql(text("""
                SELECT COUNT(*) as count
                FROM games 
                WHERE game_date >= datetime('now')
            """), conn).iloc[0]['count']
            
            print(f"🔮 Upcoming games: {upcoming_count}")
            
            # Show date range of upcoming games
            if upcoming_count > 0:
                date_range = pd.read_sql(text("""
                    SELECT 
                        MIN(game_date) as first_game,
                        MAX(game_date) as last_game,
                        COUNT(DISTINCT DATE(game_date)) as unique_dates
                    FROM games 
                    WHERE game_date >= datetime('now')
                """), conn)
                
                if not date_range.empty:
                    row = date_range.iloc[0]
                    print(f"📅 Date range: {row['first_game'][:10]} to {row['last_game'][:10]}")
                    print(f"📆 Unique game dates: {row['unique_dates']}")
                
                # Test a specific API game ID
                print(f"\n🔍 TESTING SPECIFIC API GAME ID:")
                test_id = 'f1bc532dff946d15cb85654b5c4b246e'  # From the API output
                test_game = pd.read_sql(text("""
                    SELECT game_id, home_team, away_team, game_date
                    FROM games 
                    WHERE game_id = :game_id
                """), conn, params={'game_id': test_id})
                
                if not test_game.empty:
                    game = test_game.iloc[0]
                    print(f"  ✅ Found: {game['away_team']} @ {game['home_team']} ({game['game_date'][:10]})")
                else:
                    print(f"  ❌ Test game ID {test_id} not found")
            
            # Check if we can now get odds for these games
            odds_compatible = pd.read_sql(text("""
                SELECT COUNT(DISTINCT g.game_id) as games_with_potential_odds
                FROM games g
                WHERE g.game_date >= datetime('now')
                AND g.game_date <= datetime('now', '+14 days')
            """), conn).iloc[0]['games_with_potential_odds']
            
            print(f"🎯 Games ready for odds fetching: {odds_compatible}")
            
            return upcoming_count > 0
            
    except Exception as e:
        print(f"❌ Error verifying update: {e}")
        return False

def test_odds_fetcher_compatibility():
    """Test if odds fetcher will now work"""
    print(f"\n🧪 TESTING ODDS FETCHER COMPATIBILITY")
    print("=" * 45)
    
    # Get a sample API game
    url = f'https://api.the-odds-api.com/v4/sports/{SPORT}/odds'
    params = {
        'apiKey': API_KEY,
        'regions': 'us',
        'markets': 'h2h',
        'oddsFormat': 'decimal'
    }
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            api_games = response.json()
            if api_games:
                sample_game = api_games[0]
                api_game_id = sample_game['id']
                
                # Check if this game is now in our database
                engine = create_engine(DB_PATH)
                with engine.connect() as conn:
                    db_game = pd.read_sql(text("""
                        SELECT game_id, home_team, away_team 
                        FROM games 
                        WHERE game_id = :game_id
                    """), conn, params={'game_id': api_game_id})
                    
                    if not db_game.empty:
                        print(f"✅ SUCCESS! API game found in database:")
                        print(f"  Game ID: {api_game_id}")
                        print(f"  Teams: {db_game.iloc[0]['away_team']} @ {db_game.iloc[0]['home_team']}")
                        print(f"💡 Odds fetcher should now work!")
                        return True
                    else:
                        print(f"❌ API game still not in database")
                        return False
    except Exception as e:
        print(f"❌ Error testing compatibility: {e}")
        return False

def main():
    """Main execution function"""
    print("🏈 FINAL NFL SCHEDULE UPDATER")
    print("=" * 35)
    print("Fixing the database structure issues...")
    
    # Get current schedule from API
    schedule_data = get_current_nfl_schedule()
    
    if not schedule_data:
        print("❌ Failed to get schedule data")
        return
    
    # Show what we found
    print(f"\n📊 SCHEDULE SUMMARY:")
    dates = sorted(set(game['game_date'][:10] for game in schedule_data))
    print(f"  📅 Date range: {dates[0]} to {dates[-1]}")
    print(f"  🏈 Total games: {len(schedule_data)}")
    
    # Update database
    update_games_database(schedule_data)
    
    # Verify it worked
    success = verify_schedule_update()
    
    # Test odds fetcher compatibility
    if success:
        compatible = test_odds_fetcher_compatibility()
        
        if compatible:
            print(f"\n🎉 COMPLETE SUCCESS!")
            print(f"✅ Games added to database with proper IDs")
            print(f"✅ API games now match database games")
            print(f"🚀 Next steps:")
            print(f"  1. Run: python odds_fetcher.py  (should now get odds)")
            print(f"  2. Run: python core_betting_engine.py  (should find opportunities)")
        else:
            print(f"\n⚠️ PARTIAL SUCCESS")
            print(f"✅ Games were added but compatibility test failed")
            print(f"💡 Try running odds_fetcher.py anyway")
    else:
        print(f"\n❌ Update failed")
        print(f"💡 Check the error messages above")

if __name__ == "__main__":
    main()