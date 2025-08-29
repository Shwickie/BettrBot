#!/usr/bin/env python3
"""
Verify Player Roster Data - Check team assignments and find missing players
"""

import pandas as pd
from sqlalchemy import create_engine, text

# Config
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

def check_enhanced_nfl_players():
    """Check the enhanced_nfl_players table"""
    print("🔍 CHECKING ENHANCED_NFL_PLAYERS TABLE")
    print("=" * 45)
    
    try:
        with engine.connect() as conn:
            # Get basic stats
            stats = pd.read_sql(text("""
                SELECT 
                    COUNT(*) as total_players,
                    COUNT(DISTINCT team) as unique_teams,
                    COUNT(CASE WHEN is_active = 1 THEN 1 END) as active_players,
                    MIN(created_date) as oldest_entry,
                    MAX(created_date) as newest_entry
                FROM enhanced_nfl_players
            """), conn).iloc[0]
            
            print(f"📊 ENHANCED PLAYERS STATS:")
            print(f"  Total players: {stats['total_players']}")
            print(f"  Active players: {stats['active_players']}")
            print(f"  Unique teams: {stats['unique_teams']}")
            print(f"  Date range: {stats['oldest_entry']} to {stats['newest_entry']}")
            
            # Check for Morice Norris specifically
            norris_check = pd.read_sql(text("""
                SELECT player_name, team, position, is_active, player_id, source
                FROM enhanced_nfl_players 
                WHERE LOWER(player_name) LIKE '%norris%'
                ORDER BY player_name
            """), conn)
            
            print(f"\n🔍 NORRIS PLAYERS IN ENHANCED TABLE:")
            if not norris_check.empty:
                for _, row in norris_check.iterrows():
                    status = "✅ Active" if row['is_active'] else "❌ Inactive"
                    print(f"  {row['player_name']} ({row['team']}) - {row['position']} - {status}")
                    print(f"    ID: {row['player_id']} | Source: {row['source']}")
            else:
                print("  ❌ No Norris players found in enhanced table")
            
            # Check team distribution
            team_dist = pd.read_sql(text("""
                SELECT team, COUNT(*) as player_count
                FROM enhanced_nfl_players 
                WHERE is_active = 1
                GROUP BY team
                ORDER BY player_count DESC
                LIMIT 15
            """), conn)
            
            print(f"\n🏈 TOP TEAMS BY PLAYER COUNT:")
            for _, row in team_dist.iterrows():
                print(f"  {row['team']}: {row['player_count']} players")
            
            return norris_check
            
    except Exception as e:
        print(f"❌ Error checking enhanced players: {e}")
        return pd.DataFrame()

def check_all_player_tables_for_norris():
    """Check all player tables for Norris"""
    print("\n🔍 CHECKING ALL PLAYER TABLES FOR NORRIS")
    print("=" * 45)
    
    player_tables = [
        'enhanced_nfl_players',
        'complete_nfl_players', 
        'comprehensive_nfl_players',
        'current_nfl_players',
        'current_nfl_rosters'
    ]
    
    norris_findings = {}
    
    try:
        with engine.connect() as conn:
            for table in player_tables:
                try:
                    # Check if table exists and get column names
                    columns_check = pd.read_sql(text(f"PRAGMA table_info({table})"), conn)
                    if columns_check.empty:
                        print(f"⚪ {table}: Table not found")
                        continue
                    
                    # Determine name and team columns
                    name_col = None
                    team_col = None
                    
                    for col in ['player_name', 'player_display_name', 'full_name']:
                        if col in columns_check['name'].values:
                            name_col = col
                            break
                    
                    for col in ['team', 'recent_team']:
                        if col in columns_check['name'].values:
                            team_col = col
                            break
                    
                    if not name_col or not team_col:
                        print(f"⚪ {table}: Missing required columns")
                        continue
                    
                    # Search for Norris
                    norris_query = text(f"""
                        SELECT {name_col} as player_name, {team_col} as team
                        FROM {table}
                        WHERE LOWER({name_col}) LIKE '%norris%'
                    """)
                    
                    norris_results = pd.read_sql(norris_query, conn)
                    
                    if not norris_results.empty:
                        norris_findings[table] = norris_results
                        print(f"✅ {table}: Found {len(norris_results)} Norris player(s)")
                        for _, row in norris_results.iterrows():
                            print(f"     {row['player_name']} - {row['team']}")
                    else:
                        print(f"❌ {table}: No Norris players found")
                        
                except Exception as e:
                    print(f"⚠️ {table}: Error - {e}")
                    continue
            
            return norris_findings
            
    except Exception as e:
        print(f"❌ Error checking tables: {e}")
        return {}

def check_specific_players_teams():
    """Check team assignments for players we know from the injury validation"""
    print("\n🎯 CHECKING SPECIFIC PLAYER TEAM ASSIGNMENTS")
    print("=" * 50)
    
    # Players we know from the validation results
    test_players = [
        ('Eric Rogers', 'Should be LAC (Chargers)'),
        ('Morice Norris', 'Should be DET (Lions)'),
        ('Fabian Moreau', 'Listed as MIN vs NO'),
        ('Byron Young', 'Listed as LV vs CAR'), 
        ('Darius Slay', 'Listed as PHI vs NYG'),
        ('Dax Hill', 'Listed as CIN vs MIA'),
        ('Christian Watson', 'Should be GB'),
        ('Chris Godwin', 'Should be TB'),
        ('Tyreek Hill', 'Should be MIA'),
        ('Stefon Diggs', 'Should be HOU')
    ]
    
    try:
        with engine.connect() as conn:
            for player_name, expected in test_players:
                print(f"\n🔍 {player_name} ({expected}):")
                
                # Check enhanced table
                enhanced_result = pd.read_sql(text("""
                    SELECT player_name, team, position, is_active, player_id
                    FROM enhanced_nfl_players 
                    WHERE LOWER(player_name) LIKE :pattern
                    ORDER BY is_active DESC
                    LIMIT 3
                """), conn, params={'pattern': f'%{player_name.lower()}%'})
                
                if not enhanced_result.empty:
                    for _, row in enhanced_result.iterrows():
                        status = "✅" if row['is_active'] else "❌"
                        print(f"  Enhanced: {status} {row['player_name']} - {row['team']} ({row['position']})")
                else:
                    print(f"  Enhanced: ❌ Not found")
                
                # Check current players
                current_result = pd.read_sql(text("""
                    SELECT player_display_name, recent_team, position
                    FROM current_nfl_players 
                    WHERE LOWER(player_display_name) LIKE :pattern
                    LIMIT 2
                """), conn, params={'pattern': f'%{player_name.lower()}%'})
                
                if not current_result.empty:
                    for _, row in current_result.iterrows():
                        print(f"  Current:  ✅ {row['player_display_name']} - {row['recent_team']} ({row['position']})")
                else:
                    print(f"  Current:  ❌ Not found")
            
    except Exception as e:
        print(f"❌ Error checking specific players: {e}")

def check_team_consistency():
    """Check for team consistency across tables"""
    print("\n⚖️ CHECKING TEAM CONSISTENCY")
    print("=" * 30)
    
    try:
        with engine.connect() as conn:
            # Get team codes from enhanced table
            enhanced_teams = pd.read_sql(text("""
                SELECT DISTINCT team, COUNT(*) as player_count
                FROM enhanced_nfl_players 
                WHERE is_active = 1
                GROUP BY team
                ORDER BY team
            """), conn)
            
            print(f"🏈 TEAMS IN ENHANCED TABLE ({len(enhanced_teams)} teams):")
            for _, row in enhanced_teams.iterrows():
                print(f"  {row['team']}: {row['player_count']} players")
            
            # Check for non-standard team codes
            standard_teams = {
                'ARI', 'ATL', 'BAL', 'BUF', 'CAR', 'CHI', 'CIN', 'CLE', 'DAL', 'DEN',
                'DET', 'GB', 'HOU', 'IND', 'JAX', 'KC', 'LV', 'LAC', 'LAR', 'MIA',
                'MIN', 'NE', 'NO', 'NYG', 'NYJ', 'PHI', 'PIT', 'SF', 'SEA', 'TB', 'TEN', 'WSH'
            }
            
            non_standard = enhanced_teams[~enhanced_teams['team'].isin(standard_teams)]
            
            if not non_standard.empty:
                print(f"\n⚠️ NON-STANDARD TEAM CODES:")
                for _, row in non_standard.iterrows():
                    print(f"  {row['team']}: {row['player_count']} players")
            else:
                print(f"\n✅ All team codes are standard NFL codes")
            
            return enhanced_teams
            
    except Exception as e:
        print(f"❌ Error checking team consistency: {e}")
        return pd.DataFrame()

def search_detroit_lions_players():
    """Search for Detroit Lions players to verify roster"""
    print("\n🦁 DETROIT LIONS ROSTER CHECK")
    print("=" * 35)
    
    try:
        with engine.connect() as conn:
            # Get Lions players from enhanced table
            lions_players = pd.read_sql(text("""
                SELECT player_name, position, is_active, player_id, source
                FROM enhanced_nfl_players 
                WHERE team = 'DET'
                AND is_active = 1
                ORDER BY 
                    CASE position 
                        WHEN 'QB' THEN 1 
                        WHEN 'RB' THEN 2 
                        WHEN 'WR' THEN 3 
                        WHEN 'TE' THEN 4 
                        ELSE 5 
                    END,
                    player_name
                LIMIT 25
            """), conn)
            
            print(f"🏈 DETROIT LIONS PLAYERS ({len(lions_players)} found):")
            current_pos = None
            for _, row in lions_players.iterrows():
                if row['position'] != current_pos:
                    current_pos = row['position']
                    print(f"\n  {current_pos}:")
                print(f"    {row['player_name']} | {row['player_id']} | {row['source']}")
            
            # Look for any Norris in DET specifically
            norris_det = pd.read_sql(text("""
                SELECT player_name, position, is_active, player_id, source
                FROM enhanced_nfl_players 
                WHERE team = 'DET'
                AND LOWER(player_name) LIKE '%norris%'
            """), conn)
            
            if not norris_det.empty:
                print(f"\n✅ NORRIS IN DET ROSTER:")
                for _, row in norris_det.iterrows():
                    print(f"  {row['player_name']} - {row['position']} | {row['player_id']}")
            else:
                print(f"\n❌ NO NORRIS FOUND IN DET ROSTER")
            
            return lions_players
            
    except Exception as e:
        print(f"❌ Error checking Lions roster: {e}")
        return pd.DataFrame()

def main():
    """Main verification function"""
    print("🔍 PLAYER ROSTER VERIFICATION")
    print("=" * 45)
    print("Checking player tables and team assignments")
    
    try:
        # Step 1: Check enhanced players table
        enhanced_norris = check_enhanced_nfl_players()
        
        # Step 2: Check all tables for Norris
        all_norris = check_all_player_tables_for_norris()
        
        # Step 3: Check specific problem players
        check_specific_players_teams()
        
        # Step 4: Check team consistency
        team_consistency = check_team_consistency()
        
        # Step 5: Check Detroit Lions roster specifically
        lions_roster = search_detroit_lions_players()
        
        print(f"\n🏆 VERIFICATION SUMMARY:")
        print(f"  📋 Enhanced table checked: {'✅' if not enhanced_norris.empty else '❌'}")
        print(f"  🔍 Norris found in {len(all_norris)} tables")
        print(f"  🏈 Team codes verified: {'✅' if not team_consistency.empty else '❌'}")
        print(f"  🦁 Lions roster size: {len(lions_roster) if not lions_roster.empty else 0}")
        
        if all_norris:
            print(f"\n📍 NORRIS FINDINGS:")
            for table, data in all_norris.items():
                print(f"  {table}: {len(data)} Norris player(s)")
        
        print(f"\n💡 RECOMMENDATIONS:")
        if not any('norris' in str(enhanced_norris).lower() for enhanced_norris in [enhanced_norris]):
            print(f"  ⚠️ Morice Norris NOT in enhanced_nfl_players - may need to add")
        
        print(f"  ✅ Verify injury team mappings use enhanced_nfl_players as source")
        print(f"  📊 Consider updating player roster data if outdated")
        
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()