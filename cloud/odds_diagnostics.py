#!/usr/bin/env python3
"""
Quick diagnostic to check what's wrong with odds in your cloud database
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

def diagnose_cloud_odds():
    """Check what's wrong with cloud odds data"""
    
    # Connect to cloud PostgreSQL  
    DATABASE_URL = "postgresql://postgres:YviqtXqcsCIgRzSCofNjbfwgjkYNLydX@maglev.proxy.rlwy.net:54187/railway"
    if DATABASE_URL.startswith('postgres://'):
        DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
    
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    
    print("=== CLOUD ODDS DIAGNOSTIC ===")
    
    try:
        with engine.connect() as conn:
            # 1. Check if odds table exists
            table_check = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'odds'
                )
            """)).scalar()
            
            if not table_check:
                print("❌ PROBLEM: odds table doesn't exist in cloud database!")
                return False
            
            print("✅ odds table exists")
            
            # 2. Check total odds count
            total_odds = conn.execute(text("SELECT COUNT(*) FROM odds")).scalar()
            print(f"Total odds records in cloud: {total_odds}")
            
            if total_odds == 0:
                print("❌ PROBLEM: No odds data in cloud database!")
                print("This explains why dashboard shows '0 Live Odds'")
                
                # Check if games exist
                total_games = conn.execute(text("SELECT COUNT(*) FROM games")).scalar()
                print(f"Total games in cloud: {total_games}")
                
                if total_games > 0:
                    print("✅ Games data exists, so odds migration failed")
                    return False
                else:
                    print("❌ No games either - need to migrate everything")
                    return False
            
            # 3. Check recent odds (for dashboard stats)
            recent_odds = conn.execute(text("""
                SELECT COUNT(*) FROM odds 
                WHERE timestamp >= NOW() - INTERVAL '24 hours'
            """)).scalar()
            print(f"Recent odds (24h): {recent_odds}")
            
            # 4. Check distinct sportsbooks
            sportsbooks = conn.execute(text("""
                SELECT COUNT(DISTINCT sportsbook) FROM odds 
                WHERE timestamp >= NOW() - INTERVAL '24 hours'
            """)).scalar()
            print(f"Active sportsbooks (24h): {sportsbooks}")
            
            # 5. Check games with odds
            games_with_odds = conn.execute(text("""
                SELECT COUNT(DISTINCT g.game_id) 
                FROM games g
                INNER JOIN odds o ON g.game_id = o.game_id
                WHERE g.game_date >= CURRENT_DATE
            """)).scalar()
            print(f"Upcoming games with odds: {games_with_odds}")
            
            # 6. Show sample odds if any exist
            if total_odds > 0:
                sample = pd.read_sql(text("""
                    SELECT game_id, team, sportsbook, odds, market, timestamp
                    FROM odds 
                    ORDER BY timestamp DESC 
                    LIMIT 5
                """), conn)
                print(f"\nSample odds in cloud:")
                print(sample)
            
            # 7. Check what the dashboard query would return
            dashboard_stats = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_games,
                    (SELECT COUNT(*) FROM odds WHERE timestamp >= NOW() - INTERVAL '24 hours') as live_odds,
                    (SELECT COUNT(DISTINCT sportsbook) FROM odds WHERE timestamp >= NOW() - INTERVAL '24 hours') as sportsbooks
                FROM games
            """)).fetchone()
            
            print(f"\nDashboard would show:")
            print(f"  Games: {dashboard_stats[0]}")
            print(f"  Live Odds: {dashboard_stats[1]}")
            print(f"  Sportsbooks: {dashboard_stats[2]}")
            
            if dashboard_stats[1] == 0:
                print(f"\n❌ CONFIRMED: Dashboard shows 0 live odds because:")
                print(f"   - No odds with timestamp in last 24 hours")
                print(f"   - Total odds: {total_odds}")
                if total_odds > 0:
                    print(f"   - Odds might be too old")
                else:
                    print(f"   - No odds data at all")
                return False
            else:
                print(f"\n✅ Should show live odds on dashboard")
                return True
                
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

if __name__ == "__main__":
    success = diagnose_cloud_odds()
    
    if not success:
        print(f"\n" + "="*50)
        print("🔧 SOLUTION NEEDED:")
        print("1. Your local database HAS odds (19,860 records)")
        print("2. Your cloud database has NO odds (or very old odds)")
        print("3. You need to migrate odds from local to cloud")
        print(f"\nRun this command:")
        print("python migrate_odds.py")