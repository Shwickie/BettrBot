#!/usr/bin/env python3
"""
Team Mapping Fix - Fix incorrect team assignments in injury data
Uses the same file names as your existing scripts
"""

import pandas as pd
from sqlalchemy import create_engine, text
import re
from datetime import datetime
import difflib

# Config
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

def create_team_mapping_dictionary():
    """Create comprehensive team mapping from all variations"""
    print("🏈 CREATING TEAM MAPPING DICTIONARY")
    print("=" * 40)
    
    # Standard NFL team codes and their variations
    team_mappings = {
        # Standard mappings
        'ARI': ['ARI', 'ARIZONA', 'CARDINALS', 'AZ'],
        'ATL': ['ATL', 'ATLANTA', 'FALCONS'],
        'BAL': ['BAL', 'BALTIMORE', 'RAVENS'],
        'BUF': ['BUF', 'BUFFALO', 'BILLS'],
        'CAR': ['CAR', 'CAROLINA', 'PANTHERS'],
        'CHI': ['CHI', 'CHICAGO', 'BEARS'],
        'CIN': ['CIN', 'CINCINNATI', 'BENGALS'],
        'CLE': ['CLE', 'CLEVELAND', 'BROWNS'],
        'DAL': ['DAL', 'DALLAS', 'COWBOYS'],
        'DEN': ['DEN', 'DENVER', 'BRONCOS'],
        'DET': ['DET', 'DETROIT', 'LIONS'],
        'GB': ['GB', 'GREEN BAY', 'PACKERS', 'GNB'],
        'HOU': ['HOU', 'HOUSTON', 'TEXANS'],
        'IND': ['IND', 'INDIANAPOLIS', 'COLTS'],
        'JAX': ['JAX', 'JACKSONVILLE', 'JAGUARS', 'JAC'],
        'KC': ['KC', 'KANSAS CITY', 'CHIEFS', 'KAN'],
        'LV': ['LV', 'LAS VEGAS', 'RAIDERS', 'LVR', 'OAK', 'OAKLAND'],
        'LAC': ['LAC', 'LOS ANGELES CHARGERS', 'CHARGERS', 'SD', 'SAN DIEGO'],
        'LAR': ['LAR', 'LOS ANGELES RAMS', 'RAMS', 'LA', 'STL', 'ST LOUIS'],
        'MIA': ['MIA', 'MIAMI', 'DOLPHINS'],
        'MIN': ['MIN', 'MINNESOTA', 'VIKINGS'],
        'NE': ['NE', 'NEW ENGLAND', 'PATRIOTS', 'NEP'],
        'NO': ['NO', 'NEW ORLEANS', 'SAINTS', 'NOS'],
        'NYG': ['NYG', 'NEW YORK GIANTS', 'GIANTS', 'NY'],
        'NYJ': ['NYJ', 'NEW YORK JETS', 'JETS'],
        'PHI': ['PHI', 'PHILADELPHIA', 'EAGLES'],
        'PIT': ['PIT', 'PITTSBURGH', 'STEELERS'],
        'SF': ['SF', 'SAN FRANCISCO', '49ERS', 'SFO'],
        'SEA': ['SEA', 'SEATTLE', 'SEAHAWKS'],
        'TB': ['TB', 'TAMPA BAY', 'BUCCANEERS', 'TAM'],
        'TEN': ['TEN', 'TENNESSEE', 'TITANS'],
        'WSH': ['WSH', 'WASHINGTON', 'COMMANDERS', 'WAS', 'REDSKINS']
    }
    
    # Create reverse lookup
    team_lookup = {}
    for standard_code, variations in team_mappings.items():
        for variation in variations:
            team_lookup[variation.upper()] = standard_code
    
    print(f"📋 Created mappings for {len(team_lookup)} team variations")
    return team_lookup

def get_current_player_teams():
    """Get current team assignments from our comprehensive roster"""
    print("\n🔍 GETTING CURRENT PLAYER TEAM ASSIGNMENTS")
    print("=" * 50)
    
    try:
        with engine.connect() as conn:
            # Get player teams from enhanced table (highest priority)
            player_teams = pd.read_sql(text("""
                SELECT DISTINCT 
                    LOWER(TRIM(player_name)) as clean_name,
                    team as current_team,
                    'enhanced_nfl_players' as source
                FROM enhanced_nfl_players 
                WHERE is_active = 1 
                AND team IS NOT NULL 
                AND team != ''
                
                UNION ALL
                
                SELECT DISTINCT 
                    LOWER(TRIM(player_display_name)) as clean_name,
                    recent_team as current_team,
                    'current_nfl_players' as source
                FROM current_nfl_players 
                WHERE recent_team IS NOT NULL 
                AND recent_team != ''
                AND player_display_name IS NOT NULL
                
                UNION ALL
                
                SELECT DISTINCT 
                    LOWER(TRIM(full_name)) as clean_name,
                    team as current_team,
                    'player_team_map' as source
                FROM player_team_map 
                WHERE season = 2024
                AND team IS NOT NULL 
                AND team != ''
            """), conn)
            
            if not player_teams.empty:
                # Remove duplicates, keeping enhanced table priority
                player_teams = player_teams.drop_duplicates(subset=['clean_name'], keep='first')
                print(f"✅ Found teams for {len(player_teams)} players")
                return player_teams
            else:
                print("❌ No player team data found")
                return pd.DataFrame()
                
    except Exception as e:
        print(f"❌ Error getting player teams: {e}")
        return pd.DataFrame()

def normalize_team_code(team_input, team_lookup):
    """Normalize team code using mapping dictionary"""
    if not team_input or pd.isna(team_input):
        return 'UNKNOWN'
    
    team_clean = str(team_input).upper().strip()
    
    # Direct lookup
    if team_clean in team_lookup:
        return team_lookup[team_clean]
    
    # Partial matching for complex names
    for variation, standard in team_lookup.items():
        if variation in team_clean or team_clean in variation:
            return standard
    
    return team_clean if team_clean else 'UNKNOWN'

def fix_injury_team_assignments():
    """Fix team assignments in injury data"""
    print("\n🔧 FIXING INJURY TEAM ASSIGNMENTS")
    print("=" * 40)
    
    try:
        team_lookup = create_team_mapping_dictionary()
        player_teams = get_current_player_teams()
        
        if player_teams.empty:
            print("❌ Cannot proceed without player team data")
            return 0
        
        # Create player name to team mapping
        name_to_team = dict(zip(player_teams['clean_name'], player_teams['current_team']))
        
        with engine.begin() as conn:
            # Get all injuries that need team fixes
            injuries = pd.read_sql(text("""
                SELECT id, player_name, team, designation, player_id, confidence_score
                FROM nfl_injuries_tracking 
                WHERE is_active = 1
                ORDER BY 
                    CASE WHEN player_id IS NULL THEN 2 ELSE 1 END,
                    player_name
            """), conn)
            
            if injuries.empty:
                print("❌ No injury data found")
                return 0
            
            print(f"🔄 Processing {len(injuries)} injury records...")
            
            fixed_teams = 0
            fixed_mappings = 0
            
            for i, injury in injuries.iterrows():
                player_name = injury['player_name']
                current_team = injury['team']
                player_id = injury['player_id']
                
                if not player_name:
                    continue
                
                # Clean player name for lookup
                clean_name = clean_player_name(player_name).lower().strip()
                
                # Strategy 1: Fix team assignment using player database
                correct_team = None
                if clean_name in name_to_team:
                    correct_team = name_to_team[clean_name]
                elif player_name.lower().strip() in name_to_team:
                    correct_team = name_to_team[player_name.lower().strip()]
                
                # Strategy 2: Normalize existing team code
                normalized_team = normalize_team_code(current_team, team_lookup)
                
                # Determine best team assignment
                final_team = correct_team if correct_team else normalized_team
                
                # Update if we have a better team assignment
                if final_team and final_team != current_team and final_team != 'UNKNOWN':
                    conn.execute(text("""
                        UPDATE nfl_injuries_tracking 
                        SET team = :new_team,
                            last_updated = :timestamp,
                            notes = COALESCE(notes, '') || ' Team corrected'
                        WHERE id = :injury_id
                    """), {
                        'new_team': final_team,
                        'timestamp': datetime.now(),
                        'injury_id': injury['id']
                    })
                    
                    fixed_teams += 1
                    print(f"🔧 {player_name}: {current_team} → {final_team}")
                
                # Strategy 3: Try to map unmapped players with corrected teams
                if not player_id and clean_name in name_to_team:
                    # Try to find player in database with correct team
                    player_lookup = pd.read_sql(text("""
                        SELECT player_id, team, player_name 
                        FROM enhanced_nfl_players 
                        WHERE LOWER(TRIM(player_name)) = :clean_name
                        AND team = :team
                        AND is_active = 1
                        LIMIT 1
                    """), conn, params={
                        'clean_name': clean_name,
                        'team': final_team
                    })
                    
                    if not player_lookup.empty:
                        found_player = player_lookup.iloc[0]
                        conn.execute(text("""
                            UPDATE nfl_injuries_tracking 
                            SET player_id = :player_id,
                                team = :correct_team,
                                confidence_score = 0.95,
                                last_updated = :timestamp,
                                notes = 'Team-corrected mapping'
                            WHERE id = :injury_id
                        """), {
                            'player_id': found_player['player_id'],
                            'correct_team': found_player['team'],
                            'timestamp': datetime.now(),
                            'injury_id': injury['id']
                        })
                        
                        fixed_mappings += 1
                        print(f"✅ {player_name}: Mapped with team correction → {found_player['team']}")
                
                # Progress indicator
                if (i + 1) % 50 == 0:
                    print(f"   ... processed {i+1}/{len(injuries)}, fixed {fixed_teams} teams, {fixed_mappings} mappings")
            
            print(f"\n📊 TEAM FIXING RESULTS:")
            print(f"  🔧 Teams corrected: {fixed_teams}")
            print(f"  ✅ New mappings: {fixed_mappings}")
            print(f"  📈 Total fixes: {fixed_teams + fixed_mappings}")
            
            return fixed_teams + fixed_mappings
            
    except Exception as e:
        print(f"❌ Team fixing failed: {e}")
        import traceback
        traceback.print_exc()
        return 0

def clean_player_name(name):
    """Enhanced player name cleaning"""
    if not name or pd.isna(name):
        return ""
    
    name = str(name).strip()
    
    # Remove suffixes
    name = re.sub(r'\s+(Jr\.?|Sr\.?|III?|IV|V)$', '', name, flags=re.IGNORECASE)
    
    # Fix common abbreviations
    name = re.sub(r'\bTj\b', 'T.J.', name, flags=re.IGNORECASE)
    name = re.sub(r'\bCj\b', 'C.J.', name, flags=re.IGNORECASE)
    name = re.sub(r'\bAj\b', 'A.J.', name, flags=re.IGNORECASE)
    name = re.sub(r'\bJj\b', 'J.J.', name, flags=re.IGNORECASE)
    name = re.sub(r'\bBj\b', 'B.J.', name, flags=re.IGNORECASE)
    
    # Handle apostrophes
    name = name.replace("'", "'")
    
    # Clean whitespace
    name = ' '.join(name.split())
    
    return name.title()

def validate_team_fixes():
    """Validate the team fixing results"""
    print("\n📊 VALIDATING TEAM FIXES")
    print("=" * 30)
    
    try:
        with engine.connect() as conn:
            # Get final statistics
            stats = pd.read_sql(text("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(player_id) as mapped,
                    COUNT(CASE WHEN team != 'UNKNOWN' THEN 1 END) as known_teams,
                    COUNT(DISTINCT team) as unique_teams,
                    COUNT(CASE WHEN confidence_score >= 0.9 THEN 1 END) as high_conf
                FROM nfl_injuries_tracking 
                WHERE is_active = 1
            """), conn).iloc[0]
            
            total = stats['total']
            mapped = stats['mapped']
            known_teams = stats['known_teams']
            unique_teams = stats['unique_teams']
            high_conf = stats['high_conf']
            
            mapping_rate = (mapped / total * 100) if total > 0 else 0
            team_rate = (known_teams / total * 100) if total > 0 else 0
            
            print(f"📈 FINAL RESULTS:")
            print(f"  Total injuries: {total}")
            print(f"  Successfully mapped: {mapped} ({mapping_rate:.1f}%)")
            print(f"  Known teams: {known_teams} ({team_rate:.1f}%)")
            print(f"  Unique teams: {unique_teams}")
            print(f"  High confidence: {high_conf}")
            
            # Show team distribution
            team_dist = pd.read_sql(text("""
                SELECT team, COUNT(*) as count
                FROM nfl_injuries_tracking 
                WHERE is_active = 1
                GROUP BY team
                ORDER BY count DESC
                LIMIT 10
            """), conn)
            
            print(f"\n🏈 TOP TEAMS BY INJURIES:")
            for _, row in team_dist.iterrows():
                print(f"  {row['team']}: {row['count']} injuries")
            
            return mapping_rate
            
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return 0

def main():
    """Main execution for team mapping fix"""
    print("🏈 TEAM MAPPING FIX - CORRECT TEAM ASSIGNMENTS")
    print("=" * 60)
    print("Fixing incorrect team assignments in injury data")
    
    try:
        # Step 1: Fix team assignments
        fixes_made = fix_injury_team_assignments()
        
        # Step 2: Validate results
        final_rate = validate_team_fixes()
        
        print(f"\n🏆 TEAM MAPPING RESULTS:")
        print(f"  🔧 Total fixes made: {fixes_made}")
        print(f"  📈 Final mapping rate: {final_rate:.1f}%")
        
        if fixes_made > 10:
            print(f"\n🎉 EXCELLENT! Team mapping working great!")
            print(f"🤖 Your injury data now has better team assignments!")
        elif fixes_made > 0:
            print(f"\n✅ Some improvements made with team mapping.")
        else:
            print(f"\n💡 Teams may already be correctly assigned.")
        
        print(f"\n🚀 NEXT STEPS:")
        print(f"  1. Run: python injury_data_validation.py")
        print(f"  2. Check if mapping rate improved")
        print(f"  3. Teams should now be more accurate!")
        
    except Exception as e:
        print(f"❌ Team mapping fix failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()