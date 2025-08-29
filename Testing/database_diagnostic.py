#!/usr/bin/env python3
"""
Database Diagnostic Tool - Check what's in your betting database
"""

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# Config
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

def check_database_status():
    """Check overall database status"""
    print("🔍 DATABASE DIAGNOSTIC TOOL")
    print("=" * 40)
    
    try:
        with engine.connect() as conn:
            # Check tables
            tables = pd.read_sql(text("""
                SELECT name FROM sqlite_master 
                WHERE type='table' 
                ORDER BY name
            """), conn)
            
            print(f"📋 TABLES IN DATABASE:")
            for _, table in tables.iterrows():
                print(f"  ✅ {table['name']}")
            
            return tables['name'].tolist()
            
    except Exception as e:
        print(f"❌ Error checking database: {e}")
        return []

def analyze_games_table():
    """Analyze the games table"""
    print(f"\n🏈 GAMES TABLE ANALYSIS")
    print("=" * 30)
    
    try:
        with engine.connect() as conn:
            # Count games
            game_count = pd.read_sql(text("SELECT COUNT(*) as count FROM games"), conn).iloc[0]['count']
            print(f"📊 Total games: {game_count}")
            
            # Date analysis
            date_analysis = pd.read_sql(text("""
                SELECT 
                    MIN(game_date) as earliest_date,
                    MAX(game_date) as latest_date,
                    COUNT(DISTINCT game_date) as unique_dates
                FROM games
            """), conn)
            
            if not date_analysis.empty:
                row = date_analysis.iloc[0]
                print(f"📅 Date range: {row['earliest_date']} to {row['latest_date']}")
                print(f"📆 Unique dates: {row['unique_dates']}")
            
            # Current date context
            today = datetime.now().strftime('%Y-%m-%d')
            print(f"🕐 Today's date: {today}")
            
            # Games by date relative to today
            date_relative = pd.read_sql(text("""
                SELECT 
                    CASE 
                        WHEN game_date < date('now') THEN 'PAST'
                        WHEN game_date = date('now') THEN 'TODAY' 
                        WHEN game_date > date('now') THEN 'FUTURE'
                    END as time_period,
                    COUNT(*) as game_count
                FROM games
                GROUP BY time_period
                ORDER BY time_period
            """), conn)
            
            print(f"\n📊 GAMES BY TIME PERIOD:")
            for _, row in date_relative.iterrows():
                print(f"  {row['time_period']}: {row['game_count']} games")
            
            # Upcoming games in next 14 days
            upcoming = pd.read_sql(text("""
                SELECT 
                    game_date,
                    COUNT(*) as games,
                    GROUP_CONCAT(home_team || ' vs ' || away_team, '; ') as matchups
                FROM games 
                WHERE game_date >= date('now')
                AND game_date <= date('now', '+14 days')
                GROUP BY game_date
                ORDER BY game_date
                LIMIT 10
            """), conn)
            
            print(f"\n🔮 UPCOMING GAMES (Next 14 days):")
            if upcoming.empty:
                print("  ❌ No upcoming games found!")
            else:
                for _, row in upcoming.iterrows():
                    print(f"  {row['game_date']}: {row['games']} games")
                    # Show first few matchups
                    matchups = row['matchups'][:100] + "..." if len(row['matchups']) > 100 else row['matchups']
                    print(f"    {matchups}")
            
    except Exception as e:
        print(f"❌ Error analyzing games: {e}")

def analyze_odds_table():
    """Analyze the odds table"""
    print(f"\n💰 ODDS TABLE ANALYSIS")
    print("=" * 30)
    
    try:
        with engine.connect() as conn:
            # Count odds
            odds_count = pd.read_sql(text("SELECT COUNT(*) as count FROM odds"), conn).iloc[0]['count']
            print(f"📊 Total odds records: {odds_count}")
            
            # Odds by market
            market_analysis = pd.read_sql(text("""
                SELECT 
                    market,
                    COUNT(*) as odds_count,
                    COUNT(DISTINCT sportsbook) as sportsbooks,
                    COUNT(DISTINCT game_id) as games,
                    MIN(odds) as min_odds,
                    MAX(odds) as max_odds,
                    AVG(odds) as avg_odds
                FROM odds
                GROUP BY market
                ORDER BY odds_count DESC
            """), conn)
            
            print(f"\n📈 ODDS BY MARKET:")
            for _, row in market_analysis.iterrows():
                print(f"  {row['market']:15} | {row['odds_count']:4} odds | {row['sportsbooks']} books | {row['games']:3} games")
            
            # Recent odds activity
            recent_odds = pd.read_sql(text("""
                SELECT 
                    DATE(timestamp) as date,
                    COUNT(*) as odds_updated
                FROM odds
                WHERE timestamp >= datetime('now', '-7 days')
                GROUP BY DATE(timestamp)
                ORDER BY date DESC
            """), conn)
            
            print(f"\n⏰ RECENT ODDS ACTIVITY (Last 7 days):")
            if recent_odds.empty:
                print("  ❌ No recent odds activity!")
            else:
                for _, row in recent_odds.iterrows():
                    print(f"  {row['date']}: {row['odds_updated']} odds updated")
            
            # Odds with game info
            odds_with_games = pd.read_sql(text("""
                SELECT 
                    COUNT(DISTINCT o.game_id) as games_with_odds,
                    COUNT(DISTINCT g.game_id) as total_games,
                    ROUND(COUNT(DISTINCT o.game_id) * 100.0 / COUNT(DISTINCT g.game_id), 1) as coverage_pct
                FROM games g
                LEFT JOIN odds o ON g.game_id = o.game_id
            """), conn)
            
            if not odds_with_games.empty:
                row = odds_with_games.iloc[0]
                print(f"\n📊 ODDS COVERAGE:")
                print(f"  Games with odds: {row['games_with_odds']}")
                print(f"  Total games: {row['total_games']}")
                print(f"  Coverage: {row['coverage_pct']}%")
            
    except Exception as e:
        print(f"❌ Error analyzing odds: {e}")

def check_team_power_ratings():
    """Check team power ratings"""
    print(f"\n⚡ TEAM POWER RATINGS")
    print("=" * 25)
    
    try:
        with engine.connect() as conn:
            # Check if table exists
            table_check = pd.read_sql(text("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='team_season_summary'
            """), conn)
            
            if table_check.empty:
                print("❌ team_season_summary table not found!")
                return
            
            # Count teams
            team_count = pd.read_sql(text("""
                SELECT COUNT(*) as count 
                FROM team_season_summary 
                WHERE season = 2024
            """), conn).iloc[0]['count']
            
            print(f"📊 Teams with 2024 data: {team_count}")
            
            # Top 5 teams
            top_teams = pd.read_sql(text("""
                SELECT 
                    team,
                    power_score,
                    wins,
                    win_pct,
                    avg_points_for,
                    avg_points_against
                FROM team_season_summary
                WHERE season = 2024
                ORDER BY power_score DESC
                LIMIT 5
            """), conn)
            
            print(f"\n🏆 TOP 5 TEAMS BY POWER SCORE:")
            for _, row in top_teams.iterrows():
                print(f"  {row['team']:15} | Power: {row['power_score']:.2f} | Record: {row['wins']} wins | PPG: {row['avg_points_for']:.1f}")
            
    except Exception as e:
        print(f"❌ Error checking team power: {e}")

def diagnose_betting_issues():
    """Diagnose why betting analysis might not be working"""
    print(f"\n🔧 BETTING ANALYSIS DIAGNOSTICS")
    print("=" * 35)
    
    try:
        with engine.connect() as conn:
            # Check for games with odds in the future
            future_games_with_odds = pd.read_sql(text("""
                SELECT 
                    g.game_date,
                    g.home_team,
                    g.away_team,
                    COUNT(DISTINCT o.market) as markets,
                    COUNT(DISTINCT o.sportsbook) as sportsbooks,
                    COUNT(*) as total_odds
                FROM games g
                JOIN odds o ON g.game_id = o.game_id
                WHERE g.game_date >= date('now')
                GROUP BY g.game_id, g.game_date, g.home_team, g.away_team
                ORDER BY g.game_date
                LIMIT 10
            """), conn)
            
            print(f"🔮 UPCOMING GAMES WITH ODDS:")
            if future_games_with_odds.empty:
                print("  ❌ NO UPCOMING GAMES HAVE ODDS!")
                print("  This is why your betting analysis shows 'No upcoming games found'")
                
                # Check if we have future games at all
                future_games = pd.read_sql(text("""
                    SELECT COUNT(*) as count
                    FROM games 
                    WHERE game_date >= date('now')
                """), conn).iloc[0]['count']
                
                print(f"  📊 Future games in schedule: {future_games}")
                
                if future_games > 0:
                    print("  💡 SOLUTION: Run odds_fetcher.py to get current odds")
                else:
                    print("  💡 SOLUTION: Update your games schedule with future games")
            else:
                print("  ✅ Found games with odds:")
                for _, row in future_games_with_odds.iterrows():
                    print(f"    {row['game_date']}: {row['home_team']} vs {row['away_team']} ({row['markets']} markets, {row['sportsbooks']} books)")
            
            # Check odds freshness
            fresh_odds = pd.read_sql(text("""
                SELECT 
                    MAX(timestamp) as latest_odds,
                    COUNT(*) as recent_count
                FROM odds 
                WHERE timestamp >= datetime('now', '-24 hours')
            """), conn)
            
            if not fresh_odds.empty:
                row = fresh_odds.iloc[0]
                print(f"\n⏰ ODDS FRESHNESS:")
                print(f"  Latest odds: {row['latest_odds']}")
                print(f"  Odds from last 24h: {row['recent_count']}")
                
                if row['recent_count'] == 0:
                    print("  ⚠️  No recent odds found - run odds fetcher to update")
            
    except Exception as e:
        print(f"❌ Error in diagnostics: {e}")

def main():
    """Main diagnostic function"""
    print("🩺 BETTING DATABASE HEALTH CHECK")
    print("=" * 45)
    print("Diagnosing your betting system...\n")
    
    # Check database structure
    tables = check_database_status()
    
    # Analyze each component
    if 'games' in tables:
        analyze_games_table()
    else:
        print("❌ No 'games' table found!")
    
    if 'odds' in tables:
        analyze_odds_table()
    else:
        print("❌ No 'odds' table found!")
    
    if 'team_season_summary' in tables:
        check_team_power_ratings()
    
    # Final diagnosis
    diagnose_betting_issues()
    
    print(f"\n✅ DIAGNOSTIC COMPLETE!")
    print(f"🔧 RECOMMENDATIONS:")
    print(f"  1. If no upcoming games with odds: Run odds_fetcher.py")
    print(f"  2. If odds are stale: Run odds_fetcher.py to refresh")
    print(f"  3. If missing team power: Check team_season_summary table")
    print(f"  4. Then run core_betting_engine.py for analysis")

if __name__ == "__main__":
    main()