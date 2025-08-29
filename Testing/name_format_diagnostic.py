#!/usr/bin/env python3
"""
Name Format Diagnostic - Find out why players aren't matching
"""

import pandas as pd
from sqlalchemy import create_engine, text
import difflib

# Config
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

def compare_name_formats():
    """Compare name formats between injury data and player database"""
    print("🔍 COMPARING NAME FORMATS")
    print("=" * 30)
    
    try:
        with engine.connect() as conn:
            # Get unmapped injury names
            injury_names = pd.read_sql(text("""
                SELECT DISTINCT player_name 
                FROM nfl_injuries_tracking 
                WHERE is_active = 1 
                AND player_id IS NULL
                AND player_name IN ('Deforest Buckner', 'Rashawn Slater', 'Charles Cross', 'Derek Barnett')
            """), conn)
            
            # Get player database names
            db_names = pd.read_sql(text("""
                SELECT DISTINCT player_display_name 
                FROM current_nfl_players 
                WHERE player_display_name IS NOT NULL
                ORDER BY player_display_name
            """), conn)
            
            print(f"🔍 INJURY DATA NAMES:")
            for name in injury_names['player_name']:
                print(f"  '{name}'")
            
            print(f"\n🔍 DATABASE NAMES (sample):")
            for name in db_names['player_display_name'].head(10):
                print(f"  '{name}'")
            
            # Check for similar names
            print(f"\n🔍 LOOKING FOR SIMILAR NAMES:")
            for injury_name in injury_names['player_name']:
                print(f"\nSearching for '{injury_name}':")
                best_matches = []
                
                for db_name in db_names['player_display_name']:
                    similarity = difflib.SequenceMatcher(None, injury_name.lower(), db_name.lower()).ratio()
                    if similarity > 0.6:
                        best_matches.append((db_name, similarity))
                
                best_matches.sort(key=lambda x: x[1], reverse=True)
                
                if best_matches:
                    for db_name, sim in best_matches[:3]:
                        print(f"  {sim:.2f} - '{db_name}'")
                else:
                    print(f"  ❌ No similar names found")
            
            return injury_names, db_names
            
    except Exception as e:
        print(f"❌ Comparison failed: {e}")
        return pd.DataFrame(), pd.DataFrame()

def check_exact_matches():
    """Check if any exact matches exist that we're missing"""
    print("\n🎯 CHECKING FOR EXACT MATCHES")
    print("=" * 35)
    
    try:
        with engine.connect() as conn:
            # Check if Deforest Buckner exists in any form
            test_names = ['Deforest Buckner', 'DeForest Buckner', 'D. Buckner', 'Buckner']
            
            for test_name in test_names:
                result = pd.read_sql(text("""
                    SELECT player_display_name, recent_team, player_id
                    FROM current_nfl_players 
                    WHERE LOWER(player_display_name) LIKE :name
                """), conn, params={'name': f'%{test_name.lower()}%'})
                
                if not result.empty:
                    print(f"✅ Found '{test_name}':")
                    for _, row in result.iterrows():
                        print(f"  DB: '{row['player_display_name']}' ({row['recent_team']}) - {row['player_id']}")
                else:
                    print(f"❌ Not found: '{test_name}'")
            
    except Exception as e:
        print(f"❌ Exact match check failed: {e}")

def check_database_tables():
    """Check which player tables we actually have and their contents"""
    print("\n📋 CHECKING ALL PLAYER TABLES")
    print("=" * 35)
    
    try:
        with engine.connect() as conn:
            # Check available tables
            tables = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%player%'")).fetchall()
            table_names = [table[0] for table in tables]
            
            print(f"📊 Available player tables: {table_names}")
            
            for table in table_names:
                try:
                    count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                    print(f"\n🔍 {table}: {count} records")
                    
                    # Check if it has our test players
                    if 'buckner' in table.lower() or 'player' in table.lower():
                        sample = pd.read_sql(text(f"""
                            SELECT * FROM {table} 
                            WHERE LOWER(player_display_name) LIKE '%buckner%' 
                            OR LOWER(player_name) LIKE '%buckner%'
                            OR LOWER(full_name) LIKE '%buckner%'
                            LIMIT 3
                        """), conn)
                        
                        if not sample.empty:
                            print(f"  ✅ Found Buckner in {table}:")
                            for _, row in sample.iterrows():
                                print(f"    {dict(row)}")
                        else:
                            # Check what names look like in this table
                            sample = pd.read_sql(text(f"SELECT * FROM {table} LIMIT 2"), conn)
                            if not sample.empty:
                                print(f"  Sample data: {list(sample.columns)}")
                
                except Exception as e:
                    print(f"  ❌ Error checking {table}: {e}")
                    
    except Exception as e:
        print(f"❌ Table check failed: {e}")

def suggest_fix():
    """Suggest the specific fix needed"""
    print("\n💡 SUGGESTED FIX")
    print("=" * 20)
    
    print("Based on the analysis above, the issue is likely:")
    print()
    print("1. **Name Format Mismatch**: Injury data has 'Deforest Buckner', database has 'D.Buckner'")
    print("2. **Missing Players**: Some players not in your 2024 player database")
    print("3. **Case Sensitivity**: Matching algorithm too strict")
    print()
    print("🔧 **Next Steps:**")
    print("1. Run your existing `import_player_stats.py` to ensure fresh 2024 data")
    print("2. Then run the validation again")
    print("3. Or use a more aggressive fuzzy matching approach")

def main():
    """Main diagnostic"""
    print("🔍 NAME FORMAT DIAGNOSTIC")
    print("=" * 50)
    print("Finding out why Deforest Buckner and others aren't matching")
    
    # Step 1: Compare name formats
    compare_name_formats()
    
    # Step 2: Check for exact matches
    check_exact_matches()
    
    # Step 3: Check all player tables
    check_database_tables()
    
    # Step 4: Suggest fix
    suggest_fix()

if __name__ == "__main__":
    main()