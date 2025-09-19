#!/usr/bin/env python3
"""
FIXED Odds Fetcher - Normalizes team names to match your database
Fixes the team name mismatch issues
"""

import requests
import time
import pandas as pd
from datetime import datetime
from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker

# ---------------------------
# CONFIG
# ---------------------------
API_KEY = '2ea42e6f961b41a105cd8dac8a3490a8'
SPORT = 'americanfootball_nfl'
REGIONS = 'us'
ODDS_FORMAT = 'american'  # Changed to american odds format
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"

# CRITICAL: Team name mapping to match your database
TEAM_NAME_MAPPING = {
    # API team names -> Your database team names
    'Arizona Cardinals': 'ARI',
    'Atlanta Falcons': 'ATL', 
    'Baltimore Ravens': 'BAL',
    'Buffalo Bills': 'BUF',
    'Carolina Panthers': 'CAR',
    'Chicago Bears': 'CHI',
    'Cincinnati Bengals': 'CIN',
    'Cleveland Browns': 'CLE',
    'Dallas Cowboys': 'DAL',
    'Denver Broncos': 'DEN',
    'Detroit Lions': 'DET',
    'Green Bay Packers': 'GB',
    'Houston Texans': 'HOU',
    'Indianapolis Colts': 'IND',
    'Jacksonville Jaguars': 'JAX',
    'Kansas City Chiefs': 'KC',
    'Las Vegas Raiders': 'LV',
    'Los Angeles Chargers': 'LAC',
    'Los Angeles Rams': 'LAR',
    'Miami Dolphins': 'MIA',
    'Minnesota Vikings': 'MIN',
    'New England Patriots': 'NE',
    'New Orleans Saints': 'NO',
    'New York Giants': 'NYG',
    'New York Jets': 'NYJ',
    'Philadelphia Eagles': 'PHI',
    'Pittsburgh Steelers': 'PIT',
    'San Francisco 49ers': 'SF',
    'Seattle Seahawks': 'SEA',
    'Tampa Bay Buccaneers': 'TB',
    'Tennessee Titans': 'TEN',
    'Washington Commanders': 'WAS'
}

class FixedOddsFetcher:
    """Fixed odds fetcher with proper team name mapping"""
    
    def __init__(self):
        self.engine = create_engine(DB_PATH, connect_args={"timeout": 30})
        self.Session = sessionmaker(bind=self.engine)
        
    def normalize_team_name(self, api_team_name):
        """Convert API team name to database team name"""
        return TEAM_NAME_MAPPING.get(api_team_name, api_team_name)
    
    def fetch_h2h_odds(self):
        """Fetch head-to-head moneyline odds"""
        print("🎯 FETCHING H2H ODDS WITH TEAM NAME FIXING")
        print("=" * 50)
        
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
                print(f"❌ API Error: {response.status_code}")
                return 0
            
            games = response.json()
            print(f"📥 Retrieved {len(games)} games from API")
            
            session = self.Session()
            
            # Get existing game IDs from your database
            result = session.execute(text("SELECT DISTINCT game_id FROM games WHERE date(game_date) >= date('now')"))
            existing_game_ids = {row[0] for row in result.fetchall()}
            print(f"🎮 Found {len(existing_game_ids)} upcoming games in database")
            
            total_inserted = 0
            total_updated = 0
            
            for game in games:
                api_game_id = game.get('id')
                api_home_team = game.get('home_team')
                api_away_team = game.get('away_team')
                
                # Normalize team names
                db_home_team = self.normalize_team_name(api_home_team)
                db_away_team = self.normalize_team_name(api_away_team)
                
                print(f"\n🏈 Processing: {api_away_team} @ {api_home_team}")
                print(f"   Normalized: {db_away_team} @ {db_home_team}")
                
                # Try to match with existing games by team names and date
                game_date = game.get('commence_time', '')[:10]  # Get date part
                
                # Find matching game in database
                match_query = text("""
                    SELECT game_id FROM games 
                    WHERE ((home_team = :home AND away_team = :away) OR 
                           (home_team = :home_full AND away_team = :away_full))
                    AND date(game_date) = date(:game_date)
                    LIMIT 1
                """)
                
                db_game = session.execute(match_query, {
                    'home': db_home_team,
                    'away': db_away_team, 
                    'home_full': api_home_team,
                    'away_full': api_away_team,
                    'game_date': game_date
                }).fetchone()
                
                if not db_game:
                    print(f"   ⚠️ No matching game found in database")
                    continue
                    
                db_game_id = db_game[0]
                print(f"   ✅ Matched to game_id: {db_game_id}")
                
                # Process each bookmaker
                for bookmaker in game.get('bookmakers', []):
                    sportsbook = bookmaker.get('title', 'Unknown')
                    
                    for market_data in bookmaker.get('markets', []):
                        if market_data.get('key') != 'h2h':
                            continue
                            
                        for outcome in market_data.get('outcomes', []):
                            api_team = outcome.get('name')
                            odds_value = outcome.get('price')
                            
                            if not api_team or odds_value is None:
                                continue
                            
                            # Normalize team name for database
                            db_team = self.normalize_team_name(api_team)
                            
                            # Check if record exists
                            check_query = text("""
                                SELECT id FROM odds 
                                WHERE game_id = :game_id 
                                AND team = :team 
                                AND market = 'h2h' 
                                AND sportsbook = :sportsbook
                            """)
                            
                            existing = session.execute(check_query, {
                                'game_id': db_game_id,
                                'team': db_team,
                                'sportsbook': sportsbook
                            }).fetchone()
                            
                            if existing:
                                # Update existing record
                                update_query = text("""
                                    UPDATE odds 
                                    SET odds = :odds, timestamp = :timestamp 
                                    WHERE game_id = :game_id 
                                    AND team = :team 
                                    AND market = 'h2h' 
                                    AND sportsbook = :sportsbook
                                """)
                                
                                session.execute(update_query, {
                                    'odds': odds_value,
                                    'timestamp': datetime.utcnow(),
                                    'game_id': db_game_id,
                                    'team': db_team,
                                    'sportsbook': sportsbook
                                })
                                total_updated += 1
                            else:
                                # Insert new record
                                insert_query = text("""
                                    INSERT INTO odds (game_id, sportsbook, team, market, odds, timestamp)
                                    VALUES (:game_id, :sportsbook, :team, :market, :odds, :timestamp)
                                """)
                                
                                session.execute(insert_query, {
                                    'game_id': db_game_id,
                                    'sportsbook': sportsbook,
                                    'team': db_team,
                                    'market': 'h2h',
                                    'odds': odds_value,
                                    'timestamp': datetime.utcnow()
                                })
                                total_inserted += 1
                            
                            print(f"     {sportsbook}: {db_team} {odds_value}")
            
            session.commit()
            session.close()
            
            print(f"\n🏆 RESULTS:")
            print(f"  📥 New odds: {total_inserted}")
            print(f"  🔄 Updated: {total_updated}")
            print(f"  📊 Total: {total_inserted + total_updated}")
            
            return total_inserted + total_updated
            
        except Exception as e:
            print(f"❌ Error fetching odds: {e}")
            return 0
    
    def verify_odds_data(self):
        """Verify odds are properly stored"""
        print(f"\n🔍 VERIFYING ODDS DATA")
        print("=" * 30)
        
        try:
            with self.engine.connect() as conn:
                # Check team names in odds
                teams_in_odds = pd.read_sql(text("""
                    SELECT DISTINCT team, COUNT(*) as count
                    FROM odds 
                    WHERE timestamp >= datetime('now', '-1 hour')
                    GROUP BY team
                    ORDER BY team
                """), conn)
                
                print("Teams in recent odds:")
                for _, row in teams_in_odds.iterrows():
                    print(f"  {row['team']}: {row['count']} lines")
                
                # Check sample odds with games
                sample = pd.read_sql(text("""
                    SELECT g.away_team, g.home_team, g.game_date,
                           o.team, o.sportsbook, o.odds
                    FROM games g
                    JOIN odds o ON g.game_id = o.game_id
                    WHERE date(g.game_date) >= date('now')
                    AND o.market = 'h2h'
                    LIMIT 10
                """), conn)
                
                if not sample.empty:
                    print(f"\nSample matched odds:")
                    for _, row in sample.iterrows():
                        print(f"  {row['away_team']} @ {row['home_team']}: {row['team']} {row['odds']} @ {row['sportsbook']}")
                else:
                    print("\n❌ No matched odds found!")
                    
        except Exception as e:
            print(f"❌ Error verifying: {e}")

def main():
    """Main execution"""
    print("🎰 FIXED ODDS FETCHER")
    print("Fixes team name mismatches for your AI chat")
    print("=" * 45)
    
    fetcher = FixedOddsFetcher()
    
    # Fetch h2h odds with proper team names
    total_odds = fetcher.fetch_h2h_odds()
    
    # Verify the data
    fetcher.verify_odds_data()
    
    if total_odds > 0:
        print(f"\n✅ SUCCESS!")
        print(f"Your AI chat should now work with {total_odds} odds")
        print(f"Team names are now properly matched between odds and games")
    else:
        print(f"\n❌ No odds were fetched - check API key and network")

if __name__ == "__main__":
    main()