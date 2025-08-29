#!/usr/bin/env python3
"""
Fixed Comprehensive Odds Fetcher - Get ALL available odds data
Fixed SQLAlchemy boolean check and other issues
"""

import requests
import time
import pandas as pd
from datetime import datetime
from sqlalchemy import text, create_engine, MetaData
from sqlalchemy.orm import sessionmaker

# ---------------------------
# CONFIG
# ---------------------------
API_KEY = '2ea42e6f961b41a105cd8dac8a3490a8'
SPORT = 'americanfootball_nfl'
REGIONS = 'us'
ODDS_FORMAT = 'decimal'
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"

# Available markets (we'll test these dynamically)
BASIC_MARKETS = ['h2h', 'spreads', 'totals']
PLAYER_PROP_MARKETS = [
    'player_pass_tds', 'player_pass_yds', 'player_rush_yds', 'player_rec_yds',
    'player_receiving_yards', 'player_rushing_yards', 'player_passing_yards',
    'player_pass_completions', 'player_receptions', 'player_anytime_td',
    'player_first_td', 'player_last_td', 'player_pass_attempts',
    'player_pass_interceptions', 'player_rush_attempts', 'player_longest_rush',
    'player_longest_reception', 'player_kicking_points'
]

# Additional markets to try
ADDITIONAL_MARKETS = [
    'alternate_spreads', 'alternate_totals', 'team_totals',
    'first_half_h2h', 'first_half_spreads', 'first_half_totals',
    'second_half_h2h', 'second_half_spreads', 'second_half_totals'
]

class FixedOddsFetcher:
    """Fetch all available odds data with fixes"""
    
    def __init__(self):
        self.engine = create_engine(DB_PATH, connect_args={"timeout": 30})
        self.Session = sessionmaker(bind=self.engine)
        self.working_markets = []
        self.failed_markets = []
        
    def test_market_availability(self):
        """Test which markets are currently available"""
        print("🔍 TESTING MARKET AVAILABILITY")
        print("=" * 40)
        
        all_markets = BASIC_MARKETS + PLAYER_PROP_MARKETS + ADDITIONAL_MARKETS
        
        for market in all_markets:
            url = f'https://api.the-odds-api.com/v4/sports/{SPORT}/odds'
            params = {
                'apiKey': API_KEY,
                'regions': REGIONS,
                'markets': market,
                'oddsFormat': ODDS_FORMAT
            }
            
            try:
                response = requests.get(url, params=params)
                
                if response.status_code == 200:
                    games = response.json()
                    if games:  # Only count if there are actual games
                        self.working_markets.append(market)
                        print(f"✅ {market}: {len(games)} games")
                    else:
                        print(f"⚪ {market}: Available but no current games")
                else:
                    self.failed_markets.append(market)
                    error_msg = response.json().get('message', 'Unknown error')
                    print(f"❌ {market}: {error_msg}")
                
                # Small delay to avoid rate limiting
                time.sleep(0.1)
                
            except Exception as e:
                self.failed_markets.append(market)
                print(f"❌ {market}: Exception - {e}")
        
        print(f"\n📊 SUMMARY:")
        print(f"  ✅ Working markets: {len(self.working_markets)}")
        print(f"  ❌ Failed markets: {len(self.failed_markets)}")
        
        return self.working_markets
    
    def fetch_all_odds(self, working_markets):
        """Fetch odds for all working markets"""
        print(f"\n📥 FETCHING ODDS FOR {len(working_markets)} MARKETS")
        print("=" * 50)
        
        session = self.Session()
        
        # Fixed table check
        try:
            # Check if tables exist
            result = session.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('games', 'odds')"))
            existing_tables = [row[0] for row in result.fetchall()]
            
            if 'games' not in existing_tables or 'odds' not in existing_tables:
                print("❌ Missing required database tables")
                session.close()
                return 0
            
            print("✅ Database tables found")
            
        except Exception as e:
            print(f"❌ Error checking tables: {e}")
            session.close()
            return 0
        
        # Get existing game IDs
        try:
            result = session.execute(text("SELECT DISTINCT game_id FROM games"))
            existing_game_ids = {row[0] for row in result.fetchall()}
            print(f"📋 Found {len(existing_game_ids)} games in schedule")
        except Exception as e:
            print(f"❌ Error getting game IDs: {e}")
            session.close()
            return 0
        
        total_inserted = 0
        total_updated = 0
        
        for market in working_markets:
            print(f"\n🎯 Processing {market}...")
            
            url = f'https://api.the-odds-api.com/v4/sports/{SPORT}/odds'
            params = {
                'apiKey': API_KEY,
                'regions': REGIONS,
                'markets': market,
                'oddsFormat': ODDS_FORMAT
            }
            
            try:
                response = requests.get(url, params=params)
                
                if response.status_code != 200:
                    print(f"  ❌ Failed to fetch {market}")
                    continue
                
                games = response.json()
                market_inserted = 0
                market_updated = 0
                
                for game in games:
                    game_id = game.get('id')
                    home_team = game.get('home_team')
                    away_team = game.get('away_team')
                    
                    # Skip if game not in our schedule
                    if game_id not in existing_game_ids:
                        continue
                    
                    # Process each bookmaker
                    for bookmaker in game.get('bookmakers', []):
                        sportsbook = bookmaker.get('title', 'Unknown')
                        
                        for market_data in bookmaker.get('markets', []):
                            market_key = market_data.get('key')
                            
                            for outcome in market_data.get('outcomes', []):
                                team_or_player = outcome.get('name')
                                price = outcome.get('price')
                                point = outcome.get('point')  # For spreads/totals
                                
                                if not team_or_player or price is None:
                                    continue
                                
                                # Check if record exists
                                check_query = text("""
                                    SELECT id FROM odds 
                                    WHERE game_id = :game_id 
                                    AND team = :team 
                                    AND market = :market 
                                    AND sportsbook = :sportsbook
                                """)
                                
                                existing = session.execute(check_query, {
                                    'game_id': game_id,
                                    'team': team_or_player,
                                    'market': market_key,
                                    'sportsbook': sportsbook
                                }).fetchone()
                                
                                if existing:
                                    # Update existing record
                                    update_query = text("""
                                        UPDATE odds 
                                        SET odds = :odds, timestamp = :timestamp 
                                        WHERE game_id = :game_id 
                                        AND team = :team 
                                        AND market = :market 
                                        AND sportsbook = :sportsbook
                                    """)
                                    
                                    session.execute(update_query, {
                                        'odds': price,
                                        'timestamp': datetime.utcnow(),
                                        'game_id': game_id,
                                        'team': team_or_player,
                                        'market': market_key,
                                        'sportsbook': sportsbook
                                    })
                                    market_updated += 1
                                else:
                                    # Insert new record
                                    insert_query = text("""
                                        INSERT INTO odds (game_id, sportsbook, team, market, odds, timestamp)
                                        VALUES (:game_id, :sportsbook, :team, :market, :odds, :timestamp)
                                    """)
                                    
                                    session.execute(insert_query, {
                                        'game_id': game_id,
                                        'sportsbook': sportsbook,
                                        'team': team_or_player,
                                        'market': market_key,
                                        'odds': price,
                                        'timestamp': datetime.utcnow()
                                    })
                                    market_inserted += 1
                
                session.commit()
                print(f"  ✅ {market}: +{market_inserted} new, ~{market_updated} updated")
                total_inserted += market_inserted
                total_updated += market_updated
                
                # Small delay between markets
                time.sleep(0.5)
                
            except Exception as e:
                print(f"  ❌ Error processing {market}: {e}")
                session.rollback()
                continue
        
        # Clean up duplicates
        print(f"\n🧹 CLEANING UP DUPLICATES...")
        try:
            session.execute(text("""
                DELETE FROM odds
                WHERE id NOT IN (
                    SELECT MAX(id)
                    FROM odds
                    GROUP BY game_id, sportsbook, team, market
                )
            """))
            session.commit()
            print("✅ Duplicates removed")
        except Exception as e:
            print(f"❌ Error cleaning duplicates: {e}")
        
        session.close()
        
        print(f"\n🏆 FINAL RESULTS:")
        print(f"  📥 Total inserted: {total_inserted}")
        print(f"  🔄 Total updated: {total_updated}")
        print(f"  📊 Markets processed: {len(working_markets)}")
        
        return total_inserted + total_updated
    
    def get_odds_summary(self):
        """Get summary of current odds in database"""
        print(f"\n📊 CURRENT ODDS SUMMARY")
        print("=" * 30)
        
        try:
            with self.engine.connect() as conn:
                # Count by market
                market_counts = pd.read_sql(text("""
                    SELECT market, COUNT(*) as count, COUNT(DISTINCT sportsbook) as books
                    FROM odds 
                    GROUP BY market 
                    ORDER BY count DESC
                """), conn)
                
                if not market_counts.empty:
                    print("📈 ODDS BY MARKET:")
                    for _, row in market_counts.iterrows():
                        print(f"  {row['market']}: {row['count']} odds from {row['books']} books")
                
                # Count by sportsbook
                book_counts = pd.read_sql(text("""
                    SELECT sportsbook, COUNT(*) as count, COUNT(DISTINCT market) as markets
                    FROM odds 
                    GROUP BY sportsbook 
                    ORDER BY count DESC
                """), conn)
                
                if not book_counts.empty:
                    print(f"\n📱 ODDS BY SPORTSBOOK:")
                    for _, row in book_counts.iterrows():
                        print(f"  {row['sportsbook']}: {row['count']} odds, {row['markets']} markets")
                
                # Recent activity
                recent = pd.read_sql(text("""
                    SELECT COUNT(*) as recent_count
                    FROM odds 
                    WHERE timestamp >= datetime('now', '-1 hour')
                """), conn).iloc[0]['recent_count']
                
                print(f"\n⏰ Recent activity: {recent} odds updated in last hour")
                
        except Exception as e:
            print(f"❌ Error getting summary: {e}")


def main():
    """Main execution"""
    print("🎰 FIXED COMPREHENSIVE ODDS FETCHER")
    print("=" * 45)
    print("Getting ALL available odds data...")
    
    fetcher = FixedOddsFetcher()
    
    # Test what markets are available
    working_markets = fetcher.test_market_availability()
    
    if not working_markets:
        print("❌ No working markets found!")
        return
    
    # Fetch odds for all working markets
    total_odds = fetcher.fetch_all_odds(working_markets)
    
    # Show summary
    fetcher.get_odds_summary()
    
    print(f"\n✅ COMPLETE!")
    print(f"💰 Ready for betting analysis with {len(working_markets)} markets")
    print(f"📊 Total odds collected: {total_odds}")

if __name__ == "__main__":
    main()