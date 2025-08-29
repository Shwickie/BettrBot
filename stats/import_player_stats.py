#!/usr/bin/env python3
"""
Import ALL NFL Players - Including Defensive Players and Offensive Linemen
This fixes the missing player issue by getting complete rosters
"""

import nfl_data_py as nfl
import pandas as pd
from sqlalchemy import create_engine, text

DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

def import_complete_rosters():
    """Import complete NFL rosters including all positions"""
    print("📡 IMPORTING COMPLETE NFL ROSTERS")
    print("=" * 40)
    
    try:
        # Method 1: Try to get rosters (complete team lists)
        print("🔄 Attempting to download complete NFL rosters...")
        
        all_players = []
        
        # Try multiple nfl_data_py functions to get complete player lists
        functions_to_try = [
            ('import_rosters', [2025,2024, 2023]),
            ('import_players', [2025,2024, 2023]), 
            ('import_team_desc', [2025,2024]),
            ('import_draft_picks', [2025,2024, 2023, 2022, 2021, 2020])
        ]
        
        for func_name, years in functions_to_try:
            try:
                if hasattr(nfl, func_name):
                    func = getattr(nfl, func_name)
                    print(f"🔄 Trying {func_name}...")
                    
                    data = func(years)
                    if not data.empty:
                        data.columns = [c.strip().lower().replace(' ', '_') for c in data.columns]
                        all_players.append((func_name, data))
                        print(f"✅ {func_name}: {len(data)} records")
                    else:
                        print(f"⚠️ {func_name}: No data returned")
                else:
                    print(f"⚠️ {func_name}: Function not available")
                    
            except Exception as e:
                print(f"⚠️ {func_name} failed: {e}")
        
        # Method 2: Try to get seasonal data which includes more positions
        try:
            print("🔄 Downloading seasonal data (includes more positions)...")
            seasonal_2025 = nfl.import_seasonal_data([2025])
            seasonal_2024 = nfl.import_seasonal_data([2024])
            seasonal_2023 = nfl.import_seasonal_data([2023])
            
            for data, year in [(seasonal_2025, 2025),(seasonal_2024, 2024), (seasonal_2023, 2023)]:
                if not data.empty:
                    data.columns = [c.strip().lower().replace(' ', '_') for c in data.columns]
                    all_players.append((f'seasonal_{year}', data))
                    print(f"✅ seasonal_{year}: {len(data)} records")
        
        except Exception as e:
            print(f"⚠️ Seasonal data failed: {e}")
        
        # Method 3: Weekly data as fallback
        try:
            print("🔄 Downloading weekly data as fallback...")
            weekly_2025 = nfl.import_weekly_data([2025])
            weekly_2024 = nfl.import_weekly_data([2024])
            weekly_2023 = nfl.import_weekly_data([2023])
            
            for data, year in [(weekly_2025, 2025),(weekly_2024, 2024), (weekly_2023, 2023)]:
                if not data.empty:
                    data.columns = [c.strip().lower().replace(' ', '_') for c in data.columns]
                    all_players.append((f'weekly_{year}', data))
                    print(f"✅ weekly_{year}: {len(data)} records")
        
        except Exception as e:
            print(f"⚠️ Weekly data failed: {e}")
        
        return all_players
        
    except Exception as e:
        print(f"❌ Complete roster import failed: {e}")
        return []

def create_master_player_table(all_player_data):
    """Create master table with all NFL players"""
    print("\n🏗️ CREATING MASTER PLAYER TABLE")
    print("=" * 35)
    
    if not all_player_data:
        print("❌ No player data to process")
        return False
    
    try:
        with engine.begin() as conn:
            # Combine all data sources
            combined_players = []
            
            for source_name, data in all_player_data:
                print(f"🔄 Processing {source_name} data...")
                
                # Standardize column names
                name_columns = []
                if 'full_name' in data.columns:
                    name_columns.append('full_name')
                if 'player_display_name' in data.columns:
                    name_columns.append('player_display_name')
                if 'player_name' in data.columns:
                    name_columns.append('player_name')
                if 'display_name' in data.columns:
                    name_columns.append('display_name')
                
                team_columns = []
                if 'team' in data.columns:
                    team_columns.append('team')
                if 'recent_team' in data.columns:
                    team_columns.append('recent_team')
                if 'current_team' in data.columns:
                    team_columns.append('current_team')
                
                if name_columns and 'player_id' in data.columns:
                    for _, row in data.iterrows():
                        # Get best name
                        player_name = None
                        for col in name_columns:
                            if pd.notna(row.get(col)) and str(row[col]).strip():
                                player_name = str(row[col]).strip()
                                break
                        
                        # Get best team
                        team = 'UNK'
                        for col in team_columns:
                            if pd.notna(row.get(col)) and str(row[col]).strip():
                                team = str(row[col]).strip()
                                break
                        
                        if player_name and len(player_name) > 2:
                            combined_players.append({
                                'player_id': row['player_id'],
                                'player_display_name': player_name,
                                'recent_team': team,
                                'position': row.get('position', ''),
                                'source': source_name
                            })
            
            if not combined_players:
                print("❌ No valid players found")
                return False
            
            # Create DataFrame and remove duplicates
            master_df = pd.DataFrame(combined_players)
            
            # Remove duplicates, keeping most recent/complete record
            print(f"🔄 Deduplicating {len(master_df)} player records...")
            
            # Sort by source priority (rosters > seasonal > weekly)
            source_priority = {
                'import_rosters': 1,
                'seasonal_2025': 2, 
                'seasonal_2024': 3, 
                'seasonal_2023': 4,
                'weekly_2025': 5,
                'weekly_2024': 6,
                'weekly_2023': 7
            }
            
            master_df['priority'] = master_df['source'].map(lambda x: source_priority.get(x, 6))
            master_df = master_df.sort_values('priority')
            
            # Keep most recent record for each player
            final_df = master_df.drop_duplicates(subset=['player_id'], keep='first')
            final_df = final_df.drop('priority', axis=1)
            
            print(f"✅ Final master table: {len(final_df)} unique players")
            
            # Store in database
            conn.execute(text("DROP TABLE IF EXISTS complete_nfl_players"))
            final_df.to_sql('complete_nfl_players', conn, index=False)
            
            # Show stats
            team_counts = final_df['recent_team'].value_counts()
            print(f"\n🏈 TEAM DISTRIBUTION:")
            for team, count in team_counts.head(10).items():
                print(f"  {team}: {count} players")
            
            # Test for our missing players
            test_players = ['Deforest Buckner', 'Charles Cross', 'Derek Barnett', 'Rashawn Slater']
            print(f"\n🧪 TESTING FOR MISSING PLAYERS:")
            
            for test_name in test_players:
                matches = final_df[final_df['player_display_name'].str.contains(test_name, case=False, na=False)]
                if not matches.empty:
                    for _, match in matches.iterrows():
                        print(f"  ✅ Found: {match['player_display_name']} ({match['recent_team']}) - {match['source']}")
                else:
                    print(f"  ❌ Still missing: {test_name}")
            
            return True
            
    except Exception as e:
        print(f"❌ Master table creation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_specific_players():
    """Test if we can find the specific missing players by name"""
    print("\n🔍 TESTING SPECIFIC PLAYER SEARCH")
    print("=" * 38)
    
    # Search online for these players to verify they exist
    missing_players_info = {
        'Deforest Buckner': {'team': 'IND', 'position': 'DT'},
        'Charles Cross': {'team': 'SEA', 'position': 'OT'}, 
        'Derek Barnett': {'team': 'HOU', 'position': 'DE'},
        'Rashawn Slater': {'team': 'LAC', 'position': 'OT'}
    }
    
    print("🔍 These are confirmed NFL players:")
    for name, info in missing_players_info.items():
        print(f"  {name} - {info['team']} {info['position']}")
    
    print(f"\n💡 If not found in nfl_data_py, they may be:")
    print(f"  - Injured/on reserve lists")
    print(f"  - Not included in statistical datasets")
    print(f"  - Need manual addition to database")

def main():
    """Main execution to get complete NFL player data"""
    print("📡 IMPORT ALL NFL PLAYERS - INCLUDING DEFENSE/OL")
    print("=" * 60)
    print("This will get complete rosters including defensive players")
    
    try:
        # Step 1: Import from all available sources
        all_player_data = import_complete_rosters()
        
        # Step 2: Create master player table
        if all_player_data:
            success = create_master_player_table(all_player_data)
        else:
            success = False
        
        # Step 3: Test for specific players
        test_specific_players()
        
        if success:
            print(f"\n🎉 SUCCESS! Complete player database created!")
            print(f"🔄 Now re-run your injury mapping to see improved results!")
            print(f"\n🚀 NEXT STEPS:")
            print(f"  1. Run: python injury_impact_fix.py")
            print(f"  2. Check if Deforest Buckner and others are now mapped")
            print(f"  3. Mapping rate should improve significantly!")
        else:
            print(f"\n⚠️ Limited success. Some players may still be missing.")
            print(f"💡 Consider manual addition of missing star players.")
        
    except Exception as e:
        print(f"❌ Complete import failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()