#!/usr/bin/env python3
"""
Cloud Injury Processing - Runs your injury mapping model on cloud database
This is the cloud version of your injury_impact_model.py
"""

import pandas as pd
from sqlalchemy import create_engine, text
import re
from datetime import datetime
import difflib

# Cloud database
DATABASE_URL = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"

def get_engine():
    return create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=280,
        connect_args={
            "connect_timeout": 30,
            "application_name": "bettr-injury-processing"
        }
    )

def clean_player_name(name):
    """Your exact name cleaning logic"""
    if not name or str(name) == 'nan':
        return ""
    
    name = str(name).strip()
    name = re.sub(r'\s+(Jr\.?|Sr\.?|III?|IV|V)$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\bTj\b', 'T.J.', name, flags=re.IGNORECASE)
    name = re.sub(r'\bCj\b', 'C.J.', name, flags=re.IGNORECASE)
    name = re.sub(r'\bAj\b', 'A.J.', name, flags=re.IGNORECASE)
    name = re.sub(r'\bJj\b', 'J.J.', name, flags=re.IGNORECASE)
    name = name.replace("'", "'")
    name = ' '.join(name.split())
    return name.title()

def create_name_variations(name):
    """Your exact variation logic"""
    if not name:
        return []
    
    variations = set([name.lower()])
    
    if "'" in name:
        variations.add(name.replace("'", "").lower())
        variations.add(name.replace("'", "'").lower())
    
    variations.add(name.replace(".", "").lower())
    variations.add(name.replace(" ", "").lower())
    variations.add(name.replace("-", " ").lower())
    variations.add(name.replace("-", "").lower())
    
    parts = name.split()
    if len(parts) >= 2:
        first, last = parts[0], parts[-1]
        variations.add(f"{first[0]}. {last}".lower())
        variations.add(f"{first[0]} {last}".lower())
        variations.add(f"{first}{last}".lower())
    
    return list(variations)

def build_player_lookup():
    """Build player lookup from cloud database tables"""
    print("🔍 Building player lookup from cloud database...")
    
    engine = get_engine()
    all_players = {}
    
    with engine.connect() as conn:
        # Try multiple cloud tables
        cloud_sources = [
            ("player_game_stats", "player_display_name", "recent_team"),
            ("current_nfl_players", "player_display_name", "recent_team"),
        ]
        
        for table, name_col, team_col in cloud_sources:
            try:
                query = text(f"""
                    SELECT DISTINCT 
                        player_id,
                        {name_col} as player_name,
                        {team_col} as team
                    FROM {table}
                    WHERE player_id IS NOT NULL 
                    AND {name_col} IS NOT NULL
                    AND {team_col} IS NOT NULL
                    LIMIT 2000
                """)
                
                players = pd.read_sql(query, conn)
                
                if not players.empty:
                    new_count = 0
                    for _, player in players.iterrows():
                        clean_name = clean_player_name(player['player_name'])
                        if clean_name and clean_name.lower() not in all_players:
                            all_players[clean_name.lower()] = {
                                'player_id': player['player_id'],
                                'team': player['team'],
                                'display_name': clean_name,
                                'source': table
                            }
                            new_count += 1
                    
                    print(f"  ✅ Added {new_count} players from {table}")
                    
            except Exception as e:
                print(f"  ⚠️ {table} not available: {e}")
                continue
    
    print(f"📊 Total lookup: {len(all_players)} players")
    return all_players

def map_injuries_to_players(player_lookup):
    """Your exact injury mapping logic adapted for cloud"""
    print("\n🎯 Mapping injuries to players...")
    
    engine = get_engine()
    
    with engine.begin() as conn:
        # Get unmapped injuries
        unmapped = pd.read_sql(text("""
            SELECT id, player_name, team, designation
            FROM nfl_injuries_tracking 
            WHERE is_active = true 
            AND (player_id IS NULL OR player_id = '')
        """), conn)
        
        if unmapped.empty:
            print("✅ All injuries already mapped!")
            return 0
        
        print(f"Processing {len(unmapped)} unmapped injuries...")
        
        mapped_count = 0
        
        for _, injury in unmapped.iterrows():
            player_name = injury['player_name']
            clean_name = clean_player_name(player_name)
            
            # Strategy 1: Exact match
            if clean_name.lower() in player_lookup:
                player_info = player_lookup[clean_name.lower()]
                
                conn.execute(text("""
                    UPDATE nfl_injuries_tracking 
                    SET player_id = :player_id,
                        team = :team,
                        confidence_score = 0.99,
                        last_updated = :updated
                    WHERE id = :id
                """), {
                    'player_id': player_info['player_id'],
                    'team': player_info['team'],
                    'updated': datetime.now(),
                    'id': injury['id']
                })
                
                mapped_count += 1
                print(f"  ✅ {player_name} -> {player_info['team']}")
                continue
            
            # Strategy 2: Name variations
            variations = create_name_variations(clean_name)
            found = False
            
            for variation in variations:
                if variation in player_lookup:
                    player_info = player_lookup[variation]
                    
                    conn.execute(text("""
                        UPDATE nfl_injuries_tracking 
                        SET player_id = :player_id,
                            team = :team,
                            confidence_score = 0.95,
                            last_updated = :updated
                        WHERE id = :id
                    """), {
                        'player_id': player_info['player_id'],
                        'team': player_info['team'],
                        'updated': datetime.now(),
                        'id': injury['id']
                    })
                    
                    mapped_count += 1
                    found = True
                    print(f"  🔄 {player_name} -> {player_info['team']} (via variation)")
                    break
        
        print(f"\n📊 Mapped {mapped_count} injuries")
        return mapped_count

def calculate_injury_impact():
    """Calculate team injury impact scores"""
    print("\n📊 Calculating injury impact scores...")
    
    POS_W = {
        'QB': 3.0, 'WR': 1.5, 'RB': 1.5, 'TE': 1.4,
        'CB': 1.3, 'S': 1.2, 'LB': 1.1, 'EDGE': 1.2
    }
    
    DESIG_W = {
        'IR': 1.0, 'OUT': 0.9, 'DOUBTFUL': 0.6, 'QUESTIONABLE': 0.3
    }
    
    engine = get_engine()
    
    with engine.begin() as conn:
        injuries = pd.read_sql(text("""
            SELECT team, position, designation
            FROM nfl_injuries_tracking
            WHERE is_active = true
            AND team IS NOT NULL
            AND team != 'UNK'
        """), conn)
        
        if injuries.empty:
            print("No injuries to calculate")
            return
        
        impact_by_team = {}
        
        for _, inj in injuries.iterrows():
            team = inj['team']
            pos_weight = POS_W.get(inj['position'], 1.0)
            desig_weight = DESIG_W.get(inj['designation'], 0.3)
            impact = pos_weight * desig_weight
            
            if team not in impact_by_team:
                impact_by_team[team] = 0
            impact_by_team[team] += impact
        
        # Update validation table - FIXED: Use string formatting instead of cast
        conn.execute(text("DELETE FROM ai_injury_validation_detail"))
        
        for team, impact in impact_by_team.items():
            impact_str = str(round(impact, 2))  # Convert to string in Python
            conn.execute(text("""
                INSERT INTO ai_injury_validation_detail (team_ai, inj_name, position, designation)
                VALUES (:team, 'TEAM_TOTAL', 'ALL', :impact)
            """), {'team': team, 'impact': impact_str})  # Pass as string
        
        print(f"✅ Calculated impact for {len(impact_by_team)} teams")
        
        sorted_teams = sorted(impact_by_team.items(), key=lambda x: x[1], reverse=True)[:5]
        print("\nTop 5 teams by injury impact:")
        for team, impact in sorted_teams:
            print(f"  {team}: {impact:.2f}")


def fix_unk_teams():
    """Fix injuries with UNK teams by looking up player teams"""
    print("\n🔧 Fixing UNK team assignments...")
    
    engine = get_engine()
    
    with engine.begin() as conn:
        # Get injuries with UNK team but valid player_id
        unk_injuries = pd.read_sql(text("""
            SELECT id, player_name, player_id
            FROM nfl_injuries_tracking
            WHERE is_active = true
            AND (team = 'UNK' OR team = 'UNKNOWN' OR team IS NULL)
            AND player_id IS NOT NULL
        """), conn)
        
        if unk_injuries.empty:
            print("✅ No UNK teams to fix")
            return 0
        
        print(f"Found {len(unk_injuries)} injuries with UNK teams")
        
        fixed = 0
        
        for _, injury in unk_injuries.iterrows():
            # Look up team from player_game_stats
            result = conn.execute(text("""
                SELECT recent_team
                FROM player_game_stats
                WHERE player_id = :pid
                AND recent_team IS NOT NULL
                AND recent_team != 'UNK'
                LIMIT 1
            """), {'pid': injury['player_id']})
            
            team_row = result.fetchone()
            
            if team_row and team_row[0]:
                team = team_row[0]
                
                conn.execute(text("""
                    UPDATE nfl_injuries_tracking
                    SET team = :team, last_updated = :ts
                    WHERE id = :id
                """), {
                    'team': team,
                    'ts': datetime.now(),
                    'id': injury['id']
                })
                
                fixed += 1
                print(f"  ✅ {injury['player_name']} -> {team}")
        
        print(f"\n📊 Fixed {fixed} UNK team assignments")
        return fixed
def main():
    """Main execution"""
    print("CLOUD INJURY PROCESSING")
    print("=" * 50)
    
    # Step 1: Build player lookup
    player_lookup = build_player_lookup()
    
    if not player_lookup:
        print("❌ No player data available for mapping")
        return False
    
    # Step 2: Map injuries to players
    mapped = map_injuries_to_players(player_lookup)
    
    # Step 3: Fix UNK teams (NEW!)
    fixed_unk = fix_unk_teams()
    
    # Step 4: Calculate injury impact
    calculate_injury_impact()
    
    print(f"\n✅ Injury processing complete!")
    print(f"   Mapped: {mapped}, Fixed UNK teams: {fixed_unk}")
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)