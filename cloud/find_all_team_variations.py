#!/usr/bin/env python3
"""
Find ALL team name variations in the database
"""

from sqlalchemy import create_engine, text
import pandas as pd

DATABASE_URL = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"

def find_all_names():
    print("ALL TEAM NAME VARIATIONS IN DATABASE")
    print("=" * 60)
    
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    
    with engine.connect() as conn:
        # Get all unique team names from Sept 2025 onwards
        all_teams = pd.read_sql(text("""
            SELECT DISTINCT team, COUNT(*) as game_count
            FROM (
                SELECT home_team as team FROM games 
                WHERE game_date >= '2025-09-01' AND home_score IS NOT NULL
                UNION ALL
                SELECT away_team as team FROM games 
                WHERE game_date >= '2025-09-01' AND home_score IS NOT NULL
            ) t
            GROUP BY team
            ORDER BY team
        """), conn)
        
        print(f"\n{len(all_teams)} unique team names found:\n")
        for _, row in all_teams.iterrows():
            print(f"   '{row['team']}' - {row['game_count']} games")
        
        # Check specifically for Colts/Rams variations
        print("\n\nSearching for COLTS variations:")
        colts_matches = all_teams[all_teams['team'].str.contains('colt', case=False, na=False)]
        if colts_matches.empty:
            print("   ❌ NO matches for 'colt'")
            # Try other variations
            for variant in ['IND', 'Indianapolis', 'Indy']:
                matches = all_teams[all_teams['team'].str.contains(variant, case=False, na=False)]
                if not matches.empty:
                    print(f"   ✓ Found using '{variant}':")
                    for _, row in matches.iterrows():
                        print(f"      '{row['team']}'")
        else:
            for _, row in colts_matches.iterrows():
                print(f"   '{row['team']}'")
        
        print("\n\nSearching for RAMS variations:")
        rams_matches = all_teams[all_teams['team'].str.contains('ram', case=False, na=False)]
        if rams_matches.empty:
            print("   ❌ NO matches for 'ram'")
            for variant in ['LAR', 'LA ', 'L.A.']:
                matches = all_teams[all_teams['team'] == variant]
                if not matches.empty:
                    print(f"   ✓ Found using '{variant}':")
                    for _, row in matches.iterrows():
                        print(f"      '{row['team']}'")
        else:
            for _, row in rams_matches.iterrows():
                print(f"   '{row['team']}'")

if __name__ == "__main__":
    find_all_names()