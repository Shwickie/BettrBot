#!/usr/bin/env python3
"""
Quick verification that the odds fix worked
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text

# Database connection
POSTGRES_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres?sslmode=require"
)

def verify_odds_fix():
    """Verify that games now have matching odds"""
    
    print("=== VERIFYING ODDS FIX ===")
    
    if POSTGRES_URL.startswith('postgres://'):
        postgres_url = POSTGRES_URL.replace('postgres://', 'postgresql://', 1)
    else:
        postgres_url = POSTGRES_URL
        
    pg_engine = create_engine(postgres_url, pool_pre_ping=True)
    
    try:
        with pg_engine.connect() as conn:
            # 1. Check total odds
            total_odds = conn.execute(text("SELECT COUNT(*) FROM odds")).scalar()
            print(f"Total odds records: {total_odds}")
            
            # 2. Check games with odds (fixed query)
            games_with_odds = conn.execute(text("""
                SELECT COUNT(DISTINCT g.game_id) 
                FROM games g
                INNER JOIN odds o ON g.game_id = o.game_id
                WHERE g.game_date >= CURRENT_DATE
            """)).scalar()
            
            # 3. Check total upcoming games (fixed query)
            total_upcoming = conn.execute(text("""
                SELECT COUNT(*) FROM games 
                WHERE game_date >= CURRENT_DATE
            """)).scalar()
            
            print(f"Upcoming games with odds: {games_with_odds}")
            print(f"Total upcoming games: {total_upcoming}")
            print(f"Coverage: {games_with_odds}/{total_upcoming} games have odds")
            
            # 4. Show sample matched data
            print("\n=== SAMPLE MATCHED ODDS ===")
            sample_matched = pd.read_sql_query(text("""
                SELECT 
                    g.game_id,
                    g.away_team || ' @ ' || g.home_team as matchup,
                    g.game_date,
                    o.team,
                    o.sportsbook,
                    o.odds
                FROM games g
                INNER JOIN odds o ON g.game_id = o.game_id
                WHERE g.game_date >= CURRENT_DATE
                ORDER BY g.game_date, o.team
                LIMIT 15
            """), conn)
            
            for _, row in sample_matched.iterrows():
                print(f"  {row['matchup']} | {row['team']} @ {row['sportsbook']}: {row['odds']}")
            
            # 5. Check if betting analysis will work
            print(f"\n=== BETTING ANALYSIS CHECK ===")
            analysis_ready = games_with_odds > 0
            print(f"Ready for betting analysis: {'✅ YES' if analysis_ready else '❌ NO'}")
            
            if analysis_ready:
                print("\nYour dashboard should now show:")
                print("- Games with live odds in Place Bet")
                print("- Betting opportunities in Analysis")
                print("- Working predictions with odds")
            
            return analysis_ready
            
    except Exception as e:
        print(f"Verification error: {e}")
        return False

if __name__ == "__main__":
    success = verify_odds_fix()
    
    if success:
        print("\n" + "=" * 50)
        print("🎉 ODDS FIX VERIFICATION SUCCESSFUL!")
        print("\nNext steps:")
        print("1. Open your dashboard")
        print("2. Check Place Bet - should show live odds")
        print("3. Check Betting Analysis - should find opportunities")
        print("4. Test a prediction to see odds integration")
    else:
        print("\n❌ Verification failed")