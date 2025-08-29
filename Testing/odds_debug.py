#!/usr/bin/env python3
"""
Odds Debug Tool - Debug why odds fetcher isn't getting new odds
"""

import requests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# Config
API_KEY = '2ea42e6f961b41a105cd8dac8a3490a8'
SPORT = 'americanfootball_nfl'
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"

def check_api_vs_database():
    """Compare what the API returns vs what's in our database"""
    print("🔍 COMPARING API vs DATABASE")
    print("=" * 40)
    
    # Get games from API
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
            print(f"❌ API Error: {response.status_code}")
            return
        
        api_games = response.json()
        print(f"🌐 API returned {len(api_games)} games")
        
        # Get a few sample game IDs from API
        api_game_ids = [game['id'] for game in api_games[:5]]
        print(f"📋 Sample API game IDs:")
        for i, game in enumerate(api_games[:5]):
            print(f"  {i+1}. {game['id']}: {game['away_team']} @ {game['home_team']} ({game['commence_time'][:10]})")
        
        # Check our database
        engine = create_engine(DB_PATH)
        with engine.connect() as conn:
            # Get total games in DB
            total_games = pd.read_sql(text("SELECT COUNT(*) as count FROM games"), conn).iloc[0]['count']
            print(f"\n💾 Database has {total_games} games")
            
            # Check for API game IDs in our DB
            print(f"\n🔍 CHECKING IF API GAMES ARE IN DATABASE:")
            for game_id in api_game_ids:
                db_check = pd.read_sql(text("""
                    SELECT game_id, home_team, away_team, game_date 
                    FROM games 
                    WHERE game_id = :game_id
                """), conn, params={'game_id': game_id})
                
                if not db_check.empty:
                    row = db_check.iloc[0]
                    print(f"  ✅ {game_id}: {row['away_team']} @ {row['home_team']} ({row['game_date']})")
                else:
                    print(f"  ❌ {game_id}: NOT FOUND in database")
            
            # Check upcoming games in DB
            upcoming_games = pd.read_sql(text("""
                SELECT COUNT(*) as count 
                FROM games 
                WHERE game_date >= date('now')
            """), conn).iloc[0]['count']
            
            print(f"\n📅 Upcoming games in database: {upcoming_games}")
            
            if upcoming_games == 0:
                print("❌ This is why odds fetcher finds no games to update!")
    
    except Exception as e:
        print(f"❌ Error comparing API vs DB: {e}")

def test_odds_insertion():
    """Test if we can manually insert an odds record"""
    print(f"\n🧪 TESTING ODDS INSERTION")
    print("=" * 30)
    
    engine = create_engine(DB_PATH)
    
    try:
        with engine.begin() as conn:
            # Get the first upcoming game
            upcoming_game = pd.read_sql(text("""
                SELECT game_id, home_team, away_team, game_date
                FROM games 
                WHERE game_date >= date('now')
                ORDER BY game_date
                LIMIT 1
            """), conn)
            
            if upcoming_game.empty:
                print("❌ No upcoming games to test with")
                return
            
            game = upcoming_game.iloc[0]
            print(f"🎯 Testing with: {game['away_team']} @ {game['home_team']} ({game['game_date']})")
            
            # Try to insert a test odds record
            test_odds = {
                'game_id': game['game_id'],
                'sportsbook': 'TEST_DEBUG',
                'team': game['home_team'],
                'market': 'h2h',
                'odds': 2.00,
                'timestamp': datetime.utcnow()
            }
            
            conn.execute(text("""
                INSERT INTO odds (game_id, sportsbook, team, market, odds, timestamp)
                VALUES (:game_id, :sportsbook, :team, :market, :odds, :timestamp)
            """), test_odds)
            
            print("✅ Test odds insertion successful!")
            
            # Clean up test record
            conn.execute(text("""
                DELETE FROM odds 
                WHERE sportsbook = 'TEST_DEBUG'
            """))
            
            print("🧹 Cleaned up test record")
    
    except Exception as e:
        print(f"❌ Error testing odds insertion: {e}")
        import traceback
        traceback.print_exc()

def analyze_odds_fetcher_logic():
    """Analyze why the odds fetcher isn't working"""
    print(f"\n🔍 ANALYZING ODDS FETCHER LOGIC")
    print("=" * 40)
    
    engine = create_engine(DB_PATH)
    
    try:
        with engine.connect() as conn:
            # Check the games that odds fetcher sees
            existing_game_ids = pd.read_sql(text("SELECT DISTINCT game_id FROM games"), conn)
            print(f"📊 Total unique game IDs in database: {len(existing_game_ids)}")
            
            # Check recent game IDs
            recent_games = pd.read_sql(text("""
                SELECT game_id, home_team, away_team, game_date
                FROM games 
                ORDER BY game_date DESC
                LIMIT 10
            """), conn)
            
            print(f"\n📅 MOST RECENT 10 GAMES IN DATABASE:")
            for _, game in recent_games.iterrows():
                print(f"  {game['game_date']}: {game['game_id'][:8]}... {game['away_team']} @ {game['home_team']}")
            
            # Get a sample API game and check if it would match
            url = f'https://api.the-odds-api.com/v4/sports/{SPORT}/odds'
            params = {'apiKey': API_KEY, 'regions': 'us', 'markets': 'h2h', 'oddsFormat': 'decimal'}
            
            response = requests.get(url, params=params)
            if response.status_code == 200:
                api_games = response.json()
                if api_games:
                    sample_api_game = api_games[0]
                    api_game_id = sample_api_game['id']
                    
                    print(f"\n🔍 SAMPLE API GAME DEBUG:")
                    print(f"  API Game ID: {api_game_id}")
                    print(f"  API Teams: {sample_api_game['away_team']} @ {sample_api_game['home_team']}")
                    print(f"  API Date: {sample_api_game['commence_time']}")
                    
                    # Check if this ID exists in our DB
                    db_match = pd.read_sql(text("""
                        SELECT * FROM games WHERE game_id = :game_id
                    """), conn, params={'game_id': api_game_id})
                    
                    if db_match.empty:
                        print(f"  ❌ This API game ID is NOT in our database")
                        print(f"  💡 This is why odds fetcher skips it!")
                    else:
                        print(f"  ✅ This API game ID IS in our database")
                        print(f"  🤔 Odds fetcher should process this game...")
    
    except Exception as e:
        print(f"❌ Error analyzing fetcher logic: {e}")

def main():
    """Main debug function"""
    print("🐛 ODDS FETCHER DEBUG TOOL")
    print("=" * 35)
    print("Debugging why odds fetcher isn't working...")
    
    # Compare API vs Database
    check_api_vs_database()
    
    # Test basic odds insertion
    test_odds_insertion()
    
    # Analyze the fetcher logic
    analyze_odds_fetcher_logic()
    
    print(f"\n💡 DEBUGGING SUMMARY:")
    print(f"  1. If API games aren't in DB: Run the fixed schedule updater")
    print(f"  2. If games are in DB but no odds: Check odds fetcher game ID matching")
    print(f"  3. If test insertion fails: Check database permissions/schema")

if __name__ == "__main__":
    main()