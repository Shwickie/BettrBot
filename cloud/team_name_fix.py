#!/usr/bin/env python3
"""
SAFE team name fix that handles duplicate conflicts
This will delete conflicting full-name odds instead of trying to merge them
"""

import sqlite3
import pandas as pd
import os

# Your database path
DB_PATH = r"E:\Bettr Bot\betting-bot\data\betting.db"

# Team name mappings
FULL_TO_ABBR = {
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

def safe_fix_team_names():
    """Safely fix team names by removing full-name duplicates"""
    
    if not os.path.exists(DB_PATH):
        print(f"Database not found: {DB_PATH}")
        return False
    
    conn = sqlite3.connect(DB_PATH)
    
    try:
        print("🔧 SAFE TEAM NAME FIX - REMOVING DUPLICATES")
        print("=" * 60)
        
        # Step 1: Remove full team name odds (since we have abbrevs already)
        total_deleted = 0
        for full_name in FULL_TO_ABBR.keys():
            result = conn.execute("""
                DELETE FROM odds 
                WHERE team = ?
            """, (full_name,))
            
            if result.rowcount > 0:
                print(f"  🗑️  Deleted {result.rowcount} odds for '{full_name}'")
                total_deleted += result.rowcount
        
        # Step 2: Handle any remaining team name issues
        # Fix "LA" to "LAR" if it exists
        la_fix = conn.execute("""
            UPDATE odds SET team = 'LAR' WHERE team = 'LA'
        """)
        if la_fix.rowcount > 0:
            print(f"  ✅ Fixed {la_fix.rowcount} 'LA' → 'LAR'")
        
        conn.commit()
        
        # Step 3: Verify the fix
        final_teams = pd.read_sql_query("""
            SELECT team, COUNT(*) as count 
            FROM odds 
            WHERE market = 'h2h'
            GROUP BY team 
            ORDER BY team
        """, conn)
        
        print(f"\nFINAL STATE - Team names ({len(final_teams)}):")
        for _, row in final_teams.iterrows():
            print(f"  {row['team']}: {row['count']} odds")
        
        # Check if all teams are now abbreviations (3 chars or less)
        full_names_remaining = [team for team in final_teams['team'] if len(team) > 3]
        if full_names_remaining:
            print(f"\n⚠️  Still have full names: {full_names_remaining}")
            return False
        
        print(f"\n✅ SUCCESS!")
        print(f"   - Deleted {total_deleted} duplicate full-name odds")
        print(f"   - All teams now use abbreviations")
        print(f"   - Total odds remaining: {final_teams['count'].sum()}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    success = safe_fix_team_names()
    if success:
        print("\n🎉 Database fixed! Your dashboard should work now.")
        print("Restart your dashboard and test the AI chat.")
    else:
        print("\n❌ Fix failed. Check the errors above.")