#!/usr/bin/env python3
"""
FINAL WORKING migrate_odds.py - Based on debug findings
The schema is correct, we just need proper connection handling
"""

import requests
import os
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# API Configuration
API_KEY = '2ea42e6f961b41a105cd8dac8a3490a8'
SPORT = 'americanfootball_nfl'
REGIONS = 'us'
ODDS_FORMAT = 'american'

# Database Configuration  
DATABASE_URL = "postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres"

# Team name mapping
TEAM_MAPPING = {
    'Arizona Cardinals': 'ARI', 'Atlanta Falcons': 'ATL', 'Baltimore Ravens': 'BAL',
    'Buffalo Bills': 'BUF', 'Carolina Panthers': 'CAR', 'Chicago Bears': 'CHI',
    'Cincinnati Bengals': 'CIN', 'Cleveland Browns': 'CLE', 'Dallas Cowboys': 'DAL',
    'Denver Broncos': 'DEN', 'Detroit Lions': 'DET', 'Green Bay Packers': 'GB',
    'Houston Texans': 'HOU', 'Indianapolis Colts': 'IND', 'Jacksonville Jaguars': 'JAX',
    'Kansas City Chiefs': 'KC', 'Las Vegas Raiders': 'LV', 'Los Angeles Chargers': 'LAC',
    'Los Angeles Rams': 'LAR', 'Miami Dolphins': 'MIA', 'Minnesota Vikings': 'MIN',
    'New England Patriots': 'NE', 'New Orleans Saints': 'NO', 'New York Giants': 'NYG',
    'New York Jets': 'NYJ', 'Philadelphia Eagles': 'PHI', 'Pittsburgh Steelers': 'PIT',
    'San Francisco 49ers': 'SF', 'Seattle Seahawks': 'SEA', 'Tampa Bay Buccaneers': 'TB',
    'Tennessee Titans': 'TEN', 'Washington Commanders': 'WAS'
}

class CloudOddsFetcher:
    """FINAL working odds fetcher - based on debug findings"""
    
    def __init__(self):
        self.engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,
            pool_recycle=280,
            pool_timeout=30,
            connect_args={
                "sslmode": "require",
                "connect_timeout": 30
            }
        )
        print(f"Connected to cloud database")
    
    def normalize_team_name(self, api_team_name):
        """Convert API team name to cloud database abbreviation"""
        return TEAM_MAPPING.get(api_team_name, api_team_name)
    
    def clear_old_odds(self):
        """Clear odds older than 24 hours"""
        print("Clearing old odds...")
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    DELETE FROM odds 
                    WHERE timestamp < NOW() - INTERVAL '24 hours'
                """))
                conn.commit()
                print(f"  Cleared {result.rowcount} old odds records")
        except Exception as e:
            print(f"  Warning: Could not clear old odds: {e}")
    
    def fetch_fresh_odds(self):
        """Fetch fresh odds with proper connection handling"""
        print("=== FETCHING FRESH ODDS FOR CLOUD ===")
        
        url = f'https://api.the-odds-api.com/v4/sports/{SPORT}/odds'
        params = {
            'apiKey': API_KEY,
            'regions': REGIONS,
            'markets': 'h2h',
            'oddsFormat': ODDS_FORMAT
        }
        
        try:
            response = requests.get(url, params=params)
            
            if response.status_code != 200:
                print(f"API Error: {response.status_code} - {response.text}")
                return 0
            
            games_data = response.json()
            print(f"Retrieved {len(games_data)} games from The Odds API")
            
            # Clear old odds first
            self.clear_old_odds()
            
            # Get upcoming games from cloud database
            with self.engine.connect() as conn:
                upcoming_games = pd.read_sql(text("""
                    SELECT game_id, home_team, away_team, game_date
                    FROM games 
                    WHERE game_date >= CURRENT_DATE
                    AND game_date <= CURRENT_DATE + INTERVAL '21 days'
                    ORDER BY game_date
                """), conn)
                
                print(f"Found {len(upcoming_games)} upcoming games in cloud database")
            
            # Process odds in batches with fresh connections
            odds_processed = 0
            odds_failed = 0
            batch_size = 10
            
            for api_game in games_data:
                api_home = api_game.get('home_team')
                api_away = api_game.get('away_team') 
                api_date = api_game.get('commence_time', '')[:10]
                
                # Convert to database team names
                db_home = self.normalize_team_name(api_home)
                db_away = self.normalize_team_name(api_away)
                
                print(f"\nProcessing: {api_away} @ {api_home}")
                print(f"  Database: {db_away} @ {db_home}")
                
                # Find matching game in database
                matching_game = upcoming_games[
                    (upcoming_games['home_team'] == db_home) & 
                    (upcoming_games['away_team'] == db_away) &
                    (upcoming_games['game_date'].astype(str).str[:10] == api_date)
                ]
                
                if matching_game.empty:
                    print(f"  No matching game found in database")
                    continue
                
                game_id = matching_game.iloc[0]['game_id']
                print(f"  Matched to game_id: {game_id}")
                
                # Collect all odds for this game
                game_odds = []
                
                for bookmaker in api_game.get('bookmakers', []):
                    sportsbook = bookmaker.get('title', 'Unknown')
                    
                    for market in bookmaker.get('markets', []):
                        if market.get('key') != 'h2h':
                            continue
                        
                        for outcome in market.get('outcomes', []):
                            api_team = outcome.get('name')
                            odds_value = outcome.get('price')
                            
                            if not api_team or odds_value is None:
                                continue
                            
                            db_team = self.normalize_team_name(api_team)
                            
                            game_odds.append({
                                'game_id': game_id,
                                'team': db_team,
                                'sportsbook': sportsbook,
                                'odds': odds_value,
                                'market': 'h2h',
                                'timestamp': datetime.utcnow()
                            })
                
                # Insert all odds for this game in one transaction
                if game_odds:
                    try:
                        with self.engine.connect() as conn:
                            with conn.begin():
                                for odds_data in game_odds:
                                    conn.execute(text("""
                                        INSERT INTO odds (game_id, team, sportsbook, odds, market, timestamp)
                                        VALUES (:game_id, :team, :sportsbook, :odds, :market, :timestamp)
                                    """), odds_data)
                                    
                                    odds_processed += 1
                                    print(f"    ✓ {odds_data['sportsbook']}: {odds_data['team']} {odds_data['odds']}")
                                    
                    except Exception as e:
                        print(f"  ✗ Game failed: {e}")
                        odds_failed += len(game_odds)
                        
                        # Try individual inserts as fallback
                        for odds_data in game_odds:
                            try:
                                with self.engine.connect() as conn:
                                    with conn.begin():
                                        conn.execute(text("""
                                            INSERT INTO odds (game_id, team, sportsbook, odds, market, timestamp)
                                            VALUES (:game_id, :team, :sportsbook, :odds, :market, :timestamp)
                                        """), odds_data)
                                        odds_processed += 1
                                        odds_failed -= 1
                                        print(f"    ✓ {odds_data['sportsbook']}: {odds_data['team']} {odds_data['odds']} (retry)")
                            except Exception as e2:
                                print(f"    ✗ Failed: {odds_data['team']} {odds_data['sportsbook']}: {e2}")
            
            print(f"\n=== RESULTS ===")
            print(f"Odds successfully processed: {odds_processed}")
            print(f"Odds failed: {odds_failed}")
            print(f"Success rate: {odds_processed/(odds_processed+odds_failed)*100:.1f}%" if (odds_processed + odds_failed) > 0 else "0%")
            
            return odds_processed
            
        except Exception as e:
            print(f"Error fetching odds: {e}")
            import traceback
            traceback.print_exc()
            return 0
    
    def verify_cloud_odds(self):
        """Verify odds are properly stored"""
        print(f"\n=== VERIFYING CLOUD ODDS ===")
        
        try:
            with self.engine.connect() as conn:
                # Check recent odds count
                recent_count = conn.execute(text("""
                    SELECT COUNT(*) FROM odds 
                    WHERE timestamp >= NOW() - INTERVAL '24 hours'
                """)).scalar()
                
                # Check sportsbooks
                sportsbooks = conn.execute(text("""
                    SELECT COUNT(DISTINCT sportsbook) FROM odds 
                    WHERE timestamp >= NOW() - INTERVAL '24 hours'
                """)).scalar()
                
                # Check games with odds
                games_with_odds = conn.execute(text("""
                    SELECT COUNT(DISTINCT g.game_id) 
                    FROM games g
                    INNER JOIN odds o ON g.game_id = o.game_id
                    WHERE g.game_date >= CURRENT_DATE
                    AND o.timestamp >= NOW() - INTERVAL '24 hours'
                """)).scalar()
                
                print(f"Recent odds (24h): {recent_count}")
                print(f"Active sportsbooks: {sportsbooks}")
                print(f"Upcoming games with odds: {games_with_odds}")
                
                # Show sample matched odds
                sample = pd.read_sql(text("""
                    SELECT 
                        g.away_team || ' @ ' || g.home_team as matchup,
                        o.team, o.sportsbook, o.odds
                    FROM games g
                    JOIN odds o ON g.game_id = o.game_id
                    WHERE g.game_date >= CURRENT_DATE
                    AND o.timestamp >= NOW() - INTERVAL '24 hours'
                    ORDER BY g.game_date, o.team
                    LIMIT 10
                """), conn)
                
                print(f"\nSample fresh odds:")
                if sample.empty:
                    print("  No fresh odds found")
                else:
                    for _, row in sample.iterrows():
                        print(f"  {row['matchup']}: {row['team']} {row['odds']} @ {row['sportsbook']}")
                
                return recent_count > 0
                
        except Exception as e:
            print(f"Error verifying odds: {e}")
            return False

def main():
    """Main execution"""
    print("CLOUD ODDS FETCHER - FINAL WORKING VERSION")
    print("=" * 50)
    
    try:
        fetcher = CloudOddsFetcher()
        
        # Fetch fresh odds
        total_processed = fetcher.fetch_fresh_odds()
        
        # Verify the results
        success = fetcher.verify_cloud_odds()
        
        if success and total_processed > 0:
            print(f"\nSUCCESS! Processed {total_processed} odds")
            print("Your cloud dashboard should now show:")
            print("- Fresh live odds")
            print("- Active sportsbooks") 
            print("- Real betting opportunities")
            print("- Working Place Bet functionality")
        else:
            print(f"\nIssue: {total_processed} odds processed")
            
    except Exception as e:
        print(f"ERROR: Migration failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()