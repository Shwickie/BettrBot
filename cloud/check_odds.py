#!/usr/bin/env python3
"""
Check what odds data exists and identify the problem
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta

def check_odds_status():
    """Check both local and what was migrated to cloud"""
    
    # Local SQLite check
    SQLITE_PATH = r"E:\Bettr Bot\betting-bot\data\betting.db"
    print("=== LOCAL ODDS STATUS ===")
    
    try:
        conn = sqlite3.connect(SQLITE_PATH)
        
        # Total odds
        total_odds = conn.execute("SELECT COUNT(*) FROM odds").fetchone()[0]
        print(f"Total odds in local DB: {total_odds}")
        
        # Recent odds
        recent = conn.execute("""
            SELECT COUNT(*) FROM odds 
            WHERE timestamp >= datetime('now', '-7 days')
        """).fetchone()[0]
        print(f"Recent odds (7 days): {recent}")
        
        # Check sportsbooks
        books = pd.read_sql_query("""
            SELECT sportsbook, COUNT(*) as count 
            FROM odds 
            GROUP BY sportsbook 
            ORDER BY count DESC
        """, conn)
        print(f"\nSportsbooks in local DB:")
        print(books)
        
        # Sample odds with edge calculation
        print(f"\n=== SAMPLE ODDS ANALYSIS ===")
        sample = pd.read_sql_query("""
            SELECT game_id, team, sportsbook, odds, timestamp
            FROM odds 
            WHERE market = 'h2h'
            ORDER BY timestamp DESC 
            LIMIT 10
        """, conn)
        
        if not sample.empty:
            print("Recent odds:")
            for _, row in sample.iterrows():
                odds_val = row['odds']
                if odds_val > 0:
                    implied_prob = 100 / (odds_val + 100)
                else:
                    implied_prob = abs(odds_val) / (abs(odds_val) + 100)
                
                print(f"  {row['team']}: {odds_val} ({implied_prob:.1%} implied) @ {row['sportsbook']}")
        else:
            print("No odds found!")
        
        # Check for test data patterns
        test_books = conn.execute("""
            SELECT COUNT(*) FROM odds 
            WHERE sportsbook IN ('TestBook', 'DraftKings') 
            AND odds IN (136, 210, 400, 750, 164, 122, 260, 205, 215, 156, 285)
        """).fetchone()[0]
        
        if test_books > 0:
            print(f"\n⚠️  FOUND {test_books} TEST ODDS RECORDS")
            print("These are fake odds with unrealistic edges!")
        
        conn.close()
        
    except Exception as e:
        print(f"Error checking local DB: {e}")
    
    print(f"\n=== DIAGNOSIS ===")
    if total_odds == 0:
        print("❌ No odds data found - need to collect real odds")
    elif test_books > 0:
        print("❌ Using test/fake odds data - need real sportsbook odds")
        print("💡 The huge edges you see (10-25%) are because test odds don't reflect real markets")
    else:
        print("✅ Odds data looks legitimate")
        print("💡 If edges still look too high, your ML model may be miscalibrated")

if __name__ == "__main__":
    check_odds_status()