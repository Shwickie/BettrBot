#!/usr/bin/env python3
"""
Test Missing Players - Quick diagnostic to see what's happening
"""

import pandas as pd
from sqlalchemy import create_engine, text

# Config
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

def diagnose_missing_players():
    """Diagnose exactly what's happening with missing players"""
    print("🔍 DIAGNOSING MISSING PLAYER PROBLEM")
    print("=" * 45)
    
    with engine.connect() as conn:
        # Check current injury mappings
        total_injuries = conn.execute(text("SELECT COUNT(*) FROM nfl_injuries_tracking WHERE is_active = 1")).scalar()
        mapped_injuries = conn.execute(text("SELECT COUNT(*) FROM nfl_injuries_tracking WHERE is_active = 1 AND player_id IS NOT NULL")).scalar()
        
        print(f"📊 CURRENT STATUS:")
        print(f"  Total active injuries: {total_injuries}")
        print(f"  Mapped injuries: {mapped_injuries} ({mapped_injuries/total_injuries*100:.1f}%)")
        print(f"  Unmapped injuries: {total_injuries - mapped_injuries}")
        
        # Show some unmapped injuries
        unmapped = pd.read_sql(text("""
            SELECT player_name, team, designation, injury_detail
            FROM nfl_injuries_tracking 
            WHERE is_active = 1 AND player_id IS NULL
            ORDER BY player_name
            LIMIT 20
        """), conn)
        
        print(f"\n❌ SAMPLE UNMAPPED INJURIES:")
        for _, row in unmapped.iterrows():
            print(f"  '{row['player_name']}' ({row['team']}) - {row['designation']}")
        
        # Check what player tables we have
        tables = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%player%'")).fetchall()
        
        print(f"\n📋 AVAILABLE PLAYER TABLES:")
        for table in tables:
            table_name = table[0]
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            print(f"  {table_name}: {count} records")
        
        # Check specific missing players in existing tables
        test_players = ['DeForest Buckner', 'Charles Cross', 'Derek Barnett', 'Rashawn Slater']
        
        print(f"\n🧪 TESTING KEY MISSING PLAYERS:")
        for player_name in test_players:
            print(f"\n  🔍 Searching for '{player_name}':")
            
            # Check each player table
            for table in tables:
                table_name = table[0]
                try:
                    # Try different column names
                    possible_columns = ['player_name', 'player_display_name', 'full_name', 'display_name']
                    
                    for col in possible_columns:
                        try:
                            result = pd.read_sql(text(f"""
                                SELECT {col}, team, position 
                                FROM {table_name} 
                                WHERE LOWER({col}) LIKE :name 
                                LIMIT 1
                            """), conn, params={'name': f'%{player_name.lower()}%'})
                            
                            if not result.empty:
                                row = result.iloc[0]
                                print(f"    ✅ Found in {table_name}: {row[col]} ({row.get('team', 'N/A')})")
                                break
                        except:
                            continue
                    else:
                        print(f"    ❌ Not found in {table_name}")
                except Exception as e:
                    print(f"    ⚠️ Error checking {table_name}: {e}")

def show_defensive_coverage():
    """Show how much defensive/OL coverage we have"""
    print(f"\n🛡️ DEFENSIVE/OFFENSIVE LINE COVERAGE:")
    print("=" * 45)
    
    with engine.connect() as conn:
        # Check main player table for defensive positions
        try:
            pos_query = text("""
                SELECT position_x as position, COUNT(*) as count
                FROM player_game_stats
                WHERE position_x IS NOT NULL
                GROUP BY position_x
                ORDER BY count DESC
            """)
            
            positions = pd.read_sql(pos_query, conn)
            
            print(f"📊 POSITIONS IN MAIN TABLE:")
            defensive_pos = ['DE', 'DT', 'LB', 'CB', 'S', 'FS', 'SS', 'OLB', 'ILB', 'NT']
            ol_pos = ['OT', 'OG', 'C', 'G', 'T']
            
            def_count = 0
            ol_count = 0
            
            for _, row in positions.iterrows():
                pos = row['position']
                count = row['count']
                
                if pos in defensive_pos:
                    def_count += count
                    icon = "🛡️"
                elif pos in ol_pos:
                    ol_count += count
                    icon = "🗿"
                else:
                    icon = "⚪"
                
                print(f"  {icon} {pos}: {count}")
            
            print(f"\n📈 COVERAGE SUMMARY:")
            print(f"  🛡️ Defensive players: {def_count}")
            print(f"  🗿 Offensive line: {ol_count}")
            
            if def_count < 100:
                print(f"  ⚠️ LOW DEFENSIVE COVERAGE - This is likely the problem!")
            if ol_count < 50:
                print(f"  ⚠️ LOW OL COVERAGE - Missing offensive linemen!")
                
        except Exception as e:
            print(f"❌ Error checking positions: {e}")

def main():
    """Main diagnostic"""
    diagnose_missing_players()
    show_defensive_coverage()
    
    print(f"\n💡 RECOMMENDATION:")
    print(f"Run the complete_roster_fix.py script to solve this problem!")
    print(f"It will get ALL NFL players including defensive and offensive line players.")

if __name__ == "__main__":
    main()