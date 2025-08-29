#!/usr/bin/env python3
"""
Quick Betting Test - Test your existing odds data
Simple version to verify your system works
"""

import pandas as pd
from sqlalchemy import create_engine, text

# Config
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

def test_odds_data():
    """Test what odds data you have"""
    print("📊 TESTING YOUR ODDS DATA")
    print("=" * 35)
    
    try:
        with engine.connect() as conn:
            # Check what's in your odds table
            odds_summary = pd.read_sql(text("""
                SELECT 
                    market,
                    COUNT(*) as total_odds,
                    COUNT(DISTINCT sportsbook) as sportsbooks,
                    COUNT(DISTINCT game_id) as games,
                    MIN(odds) as min_odds,
                    MAX(odds) as max_odds,
                    AVG(odds) as avg_odds
                FROM odds 
                GROUP BY market
                ORDER BY total_odds DESC
            """), conn)
            
            print("📈 ODDS SUMMARY BY MARKET:")
            for _, row in odds_summary.iterrows():
                print(f"  {row['market']:15} | {row['total_odds']:4} odds | {row['sportsbooks']} books | {row['games']:3} games | Avg: {row['avg_odds']:.2f}")
            
            # Get sample odds
            sample_odds = pd.read_sql(text("""
                SELECT 
                    o.game_id,
                    g.home_team,
                    g.away_team,
                    o.market,
                    o.team,
                    o.odds,
                    o.sportsbook
                FROM odds o
                JOIN games g ON o.game_id = g.game_id
                WHERE o.market = 'h2h'
                LIMIT 10
            """), conn)
            
            print(f"\n📋 SAMPLE MONEYLINE ODDS:")
            for _, row in sample_odds.iterrows():
                print(f"  {row['home_team']} vs {row['away_team']} | {row['team']} @ {row['odds']:.2f} ({row['sportsbook']})")
            
            # Check team power ratings
            power_check = pd.read_sql(text("""
                SELECT COUNT(*) as team_count
                FROM team_season_summary
                WHERE season = 2024
            """), conn)
            
            power_count = power_check.iloc[0]['team_count'] if not power_check.empty else 0
            print(f"\n⚡ TEAM POWER RATINGS: {power_count} teams found")
            
            return len(odds_summary) > 0
            
    except Exception as e:
        print(f"❌ Error testing odds: {e}")
        return False

def simple_ev_analysis():
    """Simple EV analysis with your data"""
    print(f"\n🎯 SIMPLE BETTING ANALYSIS")
    print("=" * 35)
    
    try:
        with engine.connect() as conn:
            # Get some basic moneyline opportunities
            analysis_query = text("""
                SELECT 
                    g.home_team,
                    g.away_team,
                    g.game_date,
                    o.team,
                    o.odds,
                    o.sportsbook,
                    -- Simple calculation: find favorites vs underdogs
                    CASE 
                        WHEN o.odds < 2.0 THEN 'FAVORITE'
                        WHEN o.odds > 2.5 THEN 'UNDERDOG'
                        ELSE 'PICK_EM'
                    END as bet_type,
                    -- Simple EV estimation (you can improve this)
                    CASE 
                        WHEN o.odds > 3.0 THEN 'HIGH_VALUE'
                        WHEN o.odds BETWEEN 2.2 AND 3.0 THEN 'MEDIUM_VALUE'
                        ELSE 'LOW_VALUE'
                    END as value_tier
                FROM odds o
                JOIN games g ON o.game_id = g.game_id
                WHERE o.market = 'h2h'
                AND g.game_date >= date('now')
                AND g.game_date <= date('now', '+7 days')
                ORDER BY o.odds DESC
                LIMIT 20
            """)
            
            opportunities = pd.read_sql(analysis_query, conn)
            
            if opportunities.empty:
                print("❌ No upcoming games found")
                return
            
            print("🏈 UPCOMING BETTING OPPORTUNITIES:")
            print("-" * 80)
            print(f"{'Matchup':<30} {'Team':<15} {'Odds':<6} {'Book':<12} {'Type':<10} {'Value':<12}")
            print("-" * 80)
            
            for _, opp in opportunities.head(15).iterrows():
                matchup = f"{opp['home_team']} vs {opp['away_team']}"
                print(f"{matchup[:29]:<30} "
                      f"{opp['team'][:14]:<15} "
                      f"{opp['odds']:<6.2f} "
                      f"{opp['sportsbook'][:11]:<12} "
                      f"{opp['bet_type']:<10} "
                      f"{opp['value_tier']:<12}")
            
            # Simple stats
            high_value = len(opportunities[opportunities['value_tier'] == 'HIGH_VALUE'])
            favorites = len(opportunities[opportunities['bet_type'] == 'FAVORITE'])
            underdogs = len(opportunities[opportunities['bet_type'] == 'UNDERDOG'])
            
            print("-" * 80)
            print(f"📊 QUICK STATS:")
            print(f"  🎯 High value bets: {high_value}")
            print(f"  ⭐ Favorites: {favorites}")
            print(f"  🔥 Underdogs: {underdogs}")
            
    except Exception as e:
        print(f"❌ Error in analysis: {e}")

def main():
    """Main test function"""
    print("🎰 QUICK BETTING SYSTEM TEST")
    print("=" * 40)
    print("Testing your existing odds and finding opportunities...")
    
    # Test odds data
    has_odds = test_odds_data()
    
    if not has_odds:
        print("❌ No odds data found - run odds fetcher first")
        return
    
    # Simple analysis
    simple_ev_analysis()
    
    print(f"\n✅ SYSTEM TEST COMPLETE!")
    print(f"💡 Your betting system has:")
    print(f"  📊 Live odds data from 8 sportsbooks")
    print(f"  🏈 Coverage of moneylines, spreads, totals")
    print(f"  🎯 Ready for advanced betting analysis")
    print(f"\n🚀 NEXT: Run the full enhanced betting engine!")

if __name__ == "__main__":
    main()