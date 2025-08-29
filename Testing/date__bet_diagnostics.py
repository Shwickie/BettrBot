#!/usr/bin/env python3
"""
Date Diagnostic Tool - Debug date filtering issues
"""

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# Config
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

def check_current_date_context():
    """Check what SQLite thinks the current date is"""
    print("🕐 CHECKING DATE CONTEXT")
    print("=" * 30)
    
    try:
        with engine.connect() as conn:
            # Check SQLite's current datetime
            sqlite_now = pd.read_sql(text("SELECT datetime('now') as sqlite_now"), conn).iloc[0]['sqlite_now']
            sqlite_date = pd.read_sql(text("SELECT date('now') as sqlite_date"), conn).iloc[0]['sqlite_date']
            
            print(f"🔍 SQLite datetime('now'): {sqlite_now}")
            print(f"📅 SQLite date('now'): {sqlite_date}")
            
            # Python current time
            python_now = datetime.now()
            print(f"🐍 Python datetime.now(): {python_now}")
            
            return sqlite_now, sqlite_date
            
    except Exception as e:
        print(f"❌ Error checking dates: {e}")
        return None, None

def analyze_game_dates():
    """Analyze the distribution of game dates"""
    print(f"\n📅 ANALYZING GAME DATES")
    print("=" * 30)
    
    try:
        with engine.connect() as conn:
            # Get game date distribution
            game_dates = pd.read_sql(text("""
                SELECT 
                    DATE(game_date) as date,
                    COUNT(*) as games,
                    MIN(game_date) as earliest_time,
                    MAX(game_date) as latest_time
                FROM games 
                GROUP BY DATE(game_date)
                ORDER BY date DESC
                LIMIT 20
            """), conn)
            
            print(f"📊 RECENT GAME DATES:")
            for _, row in game_dates.iterrows():
                print(f"  {row['date']}: {row['games']} games ({row['earliest_time']} to {row['latest_time']})")
            
            # Check upcoming games
            upcoming = pd.read_sql(text("""
                SELECT 
                    COUNT(*) as total_upcoming,
                    MIN(game_date) as next_game,
                    MAX(game_date) as last_game
                FROM games 
                WHERE game_date >= datetime('now')
            """), conn)
            
            if not upcoming.empty:
                row = upcoming.iloc[0]
                print(f"\n🔮 UPCOMING GAMES:")
                print(f"  Total: {row['total_upcoming']}")
                print(f"  Next: {row['next_game']}")
                print(f"  Last: {row['last_game']}")
            
            # Check games in next 7 days with different filters
            next_week_filters = [
                ("datetime('now')", "datetime('now', '+7 days')"),
                ("date('now')", "date('now', '+7 days')"),
                ("datetime('now')", "datetime('now', '+14 days')"),
                ("'2025-08-20'", "'2025-09-10'")  # Manual date range
            ]
            
            print(f"\n🔍 TESTING DIFFERENT DATE FILTERS:")
            for start_filter, end_filter in next_week_filters:
                query = f"""
                    SELECT COUNT(*) as count
                    FROM games 
                    WHERE game_date >= {start_filter}
                    AND game_date <= {end_filter}
                """
                result = pd.read_sql(text(query), conn).iloc[0]['count']
                print(f"  {start_filter} to {end_filter}: {result} games")
            
    except Exception as e:
        print(f"❌ Error analyzing game dates: {e}")

def analyze_odds_dates():
    """Analyze the distribution of odds timestamps"""
    print(f"\n💰 ANALYZING ODDS TIMESTAMPS")
    print("=" * 35)
    
    try:
        with engine.connect() as conn:
            # Get odds timestamp distribution
            odds_dates = pd.read_sql(text("""
                SELECT 
                    DATE(timestamp) as date,
                    COUNT(*) as odds_records,
                    MIN(timestamp) as earliest,
                    MAX(timestamp) as latest
                FROM odds 
                GROUP BY DATE(timestamp)
                ORDER BY date DESC
                LIMIT 10
            """), conn)
            
            print(f"📊 RECENT ODDS TIMESTAMPS:")
            for _, row in odds_dates.iterrows():
                print(f"  {row['date']}: {row['odds_records']} odds ({row['earliest']} to {row['latest']})")
            
            # Check recent odds with different filters
            recent_filters = [
                "datetime('now', '-24 hours')",
                "datetime('now', '-48 hours')",
                "datetime('now', '-7 days')",
                "'2025-08-20 00:00:00'"  # Manual timestamp
            ]
            
            print(f"\n🔍 TESTING ODDS TIMESTAMP FILTERS:")
            for time_filter in recent_filters:
                query = f"""
                    SELECT COUNT(*) as count
                    FROM odds 
                    WHERE timestamp >= {time_filter}
                """
                result = pd.read_sql(text(query), conn).iloc[0]['count']
                print(f"  timestamp >= {time_filter}: {result} odds")
            
    except Exception as e:
        print(f"❌ Error analyzing odds dates: {e}")

def test_join_with_dates():
    """Test the join between games and odds with date filters"""
    print(f"\n🔗 TESTING GAMES-ODDS JOIN WITH DATES")
    print("=" * 45)
    
    try:
        with engine.connect() as conn:
            # Test different combinations
            test_queries = [
                # Original query from betting engine
                """
                SELECT COUNT(*) as count
                FROM odds o
                JOIN games g ON o.game_id = g.game_id
                WHERE g.game_date >= datetime('now')
                AND g.game_date <= datetime('now', '+7 days')
                AND o.timestamp >= datetime('now', '-48 hours')
                """,
                # Relaxed date filters
                """
                SELECT COUNT(*) as count
                FROM odds o
                JOIN games g ON o.game_id = g.game_id
                WHERE g.game_date >= '2025-09-01'
                AND g.game_date <= '2025-12-31'
                AND o.timestamp >= '2025-08-19'
                """,
                # Just upcoming games with any odds
                """
                SELECT COUNT(*) as count
                FROM odds o
                JOIN games g ON o.game_id = g.game_id
                WHERE g.game_date >= datetime('now')
                """,
                # Just recent odds with any games
                """
                SELECT COUNT(*) as count
                FROM odds o
                JOIN games g ON o.game_id = g.game_id
                WHERE o.timestamp >= datetime('now', '-48 hours')
                """
            ]
            
            query_names = [
                "Original betting engine query",
                "Manual date range query", 
                "Any upcoming games with odds",
                "Any recent odds with games"
            ]
            
            for i, query in enumerate(test_queries):
                result = pd.read_sql(text(query), conn).iloc[0]['count']
                print(f"  {query_names[i]}: {result} records")
            
            # Get a sample of actual data
            sample_query = """
                SELECT 
                    g.game_date,
                    g.home_team,
                    g.away_team,
                    o.market,
                    o.timestamp,
                    COUNT(*) as odds_count
                FROM odds o
                JOIN games g ON o.game_id = g.game_id
                WHERE g.game_date >= '2025-09-01'
                GROUP BY g.game_id, o.market
                ORDER BY g.game_date
                LIMIT 10
            """
            
            sample = pd.read_sql(text(sample_query), conn)
            
            if not sample.empty:
                print(f"\n📋 SAMPLE JOINED DATA:")
                for _, row in sample.iterrows():
                    print(f"  {row['game_date'][:10]}: {row['away_team']} @ {row['home_team']} | {row['market']} | {row['odds_count']} odds | {row['timestamp'][:10]}")
            else:
                print(f"\n❌ No sample data found")
            
    except Exception as e:
        print(f"❌ Error testing joins: {e}")

def suggest_fixes():
    """Suggest fixes based on findings"""
    print(f"\n💡 SUGGESTED FIXES")
    print("=" * 25)
    
    print(f"🔧 Try these modifications to the betting engine:")
    print(f"")
    print(f"1. RELAX DATE FILTERS:")
    print(f"   Change: g.game_date >= datetime('now')")
    print(f"   To:     g.game_date >= '2025-09-01'")
    print(f"")
    print(f"2. RELAX TIMESTAMP FILTERS:")
    print(f"   Change: o.timestamp >= datetime('now', '-48 hours')")
    print(f"   To:     o.timestamp >= '2025-08-19'")
    print(f"")
    print(f"3. REMOVE TIME RESTRICTIONS TEMPORARILY:")
    print(f"   Just use: WHERE g.game_date >= '2025-09-01'")
    print(f"")
    print(f"4. CHECK FOR TIMEZONE ISSUES:")
    print(f"   Your game dates might be in different timezone")

def main():
    """Main diagnostic function"""
    print("🕐 DATE DIAGNOSTIC TOOL")
    print("=" * 30)
    print("Diagnosing date filtering issues in betting engine...")
    
    # Check current date context
    check_current_date_context()
    
    # Analyze game dates
    analyze_game_dates()
    
    # Analyze odds dates  
    analyze_odds_dates()
    
    # Test joins
    test_join_with_dates()
    
    # Suggest fixes
    suggest_fixes()
    
    print(f"\n✅ DIAGNOSIS COMPLETE!")
    print(f"💡 The issue is likely with date/timestamp filtering")
    print(f"🔧 Try the suggested fixes to get your betting engine working")

if __name__ == "__main__":
    main()