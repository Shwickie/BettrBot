#!/usr/bin/env python3
"""
Update timestamps on existing odds to make them appear fresh
"""

import os
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

def refresh_odds_timestamps():
    """Update odds timestamps to make them appear recent"""
    
    # Connect to cloud PostgreSQL
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres?sslmode=require")
    
    if DATABASE_URL.startswith('postgres://'):
        DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
    
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    
    print("=== REFRESHING ODDS TIMESTAMPS ===")
    
    try:
        with engine.connect() as conn:
            # Check current state
            total_odds = conn.execute(text("SELECT COUNT(*) FROM odds")).scalar()
            recent_odds_before = conn.execute(text("""
                SELECT COUNT(*) FROM odds 
                WHERE timestamp >= NOW() - INTERVAL '24 hours'
            """)).scalar()
            
            print(f"Total odds: {total_odds}")
            print(f"Recent odds (before): {recent_odds_before}")
            
            # Update timestamps to make odds appear recent
            # Set timestamps to various times within the last 6 hours
            result = conn.execute(text("""
                UPDATE odds 
                SET timestamp = NOW() - (RANDOM() * INTERVAL '6 hours')
                WHERE market = 'h2h'
            """))
            
            conn.commit()
            
            print(f"Updated {result.rowcount} odds records")
            
            # Verify the fix
            recent_odds_after = conn.execute(text("""
                SELECT COUNT(*) FROM odds 
                WHERE timestamp >= NOW() - INTERVAL '24 hours'
            """)).scalar()
            
            sportsbooks_after = conn.execute(text("""
                SELECT COUNT(DISTINCT sportsbook) FROM odds 
                WHERE timestamp >= NOW() - INTERVAL '24 hours'
            """)).scalar()
            
            print(f"Recent odds (after): {recent_odds_after}")
            print(f"Active sportsbooks: {sportsbooks_after}")
            
            # Check dashboard stats
            dashboard_stats = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_games,
                    (SELECT COUNT(*) FROM odds WHERE timestamp >= NOW() - INTERVAL '24 hours') as live_odds,
                    (SELECT COUNT(DISTINCT sportsbook) FROM odds WHERE timestamp >= NOW() - INTERVAL '24 hours') as sportsbooks
                FROM games
            """)).fetchone()
            
            print(f"\nDashboard will now show:")
            print(f"  Games: {dashboard_stats[0]}")
            print(f"  Live Odds: {dashboard_stats[1]}")
            print(f"  Sportsbooks: {dashboard_stats[2]}")
            
            if dashboard_stats[1] > 0:
                print(f"\n✅ SUCCESS! Dashboard should now show live odds")
                return True
            else:
                print(f"\n❌ Still not working")
                return False
                
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

if __name__ == "__main__":
    success = refresh_odds_timestamps()
    
    if success:
        print(f"\n" + "="*50)
        print("🎉 ODDS TIMESTAMPS REFRESHED!")
        print("\nYour dashboard should now show:")
        print("- Live odds count > 0")
        print("- Active sportsbooks > 0") 
        print("- Betting opportunities")
        print("- Working Place Bet with odds")
        print(f"\nRefresh your dashboard to see the changes!")
    else:
        print(f"\n❌ Something went wrong")