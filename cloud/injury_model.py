#!/usr/bin/env python3
"""
Cloud Injury Model - Runs your 91.8% accurate injury mapping on PostgreSQL
This is your injury_impact_model.py adapted for cloud database
"""

import pandas as pd
from sqlalchemy import create_engine, text
import re
from datetime import datetime
import difflib

# Cloud PostgreSQL
DATABASE_URL = "postgresql://postgres:YviqtXqcsCIgRzSCofNjbfwgjkYNLydX@maglev.proxy.rlwy.net:54187/railway"

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=280,
    connect_args={
        "connect_timeout": 30,
        "application_name": "bettr-injury-model"
    }
)

# Copy ALL your functions from injury_impact_model.py
def clean_player_name(name):
    """Your exact logic"""
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
    """Your exact logic - copy from injury_impact_model.py"""
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

def build_cloud_player_lookup():
    """Build from cloud database tables"""
    print("🔍 Building player lookup from cloud...")
    
    all_players = {}
    
    with engine.connect() as conn:
        # Get from cloud tables
        cloud_sources = [
            ("player_game_stats", "player_display_name", "recent_team"),
            ("current_nfl_players", "player_display_name", "recent_team"),
        ]
        
        for table, name_col, team_col in cloud_sources:
            try:
                query = text(f"""
                    SELECT DISTINCT 
                        player_id, {name_col} as player_name, {team_col} as team
                    FROM {table}
                    WHERE player_id IS NOT NULL 
                    LIMIT 2000
                """)
                
                players = pd.read_sql(query, conn)
                
                for _, player in players.iterrows():
                    clean_name = clean_player_name(player['player_name'])
                    if clean_name and clean_name.lower() not in all_players:
                        all_players[clean_name.lower()] = {
                            'player_id': player['player_id'],
                            'team': player['team'],
                            'display_name': clean_name
                        }
                
                print(f"  ✅ Added players from {table}")
            except Exception as e:
                print(f"  ⚠️ {table}: {e}")
    
    print(f"📊 Total: {len(all_players)} players")
    return all_players

def map_injuries_cloud(player_lookup):
    """Map injuries using your exact logic"""
    print("\n🎯 Mapping injuries...")
    
    with engine.begin() as conn:
        unmapped = pd.read_sql(text("""
            SELECT id, player_name, team, designation
            FROM nfl_injuries_tracking 
            WHERE is_active = true 
            AND (player_id IS NULL OR player_id = '')
        """), conn)
        
        if unmapped.empty:
            print("✅ All injuries mapped!")
            return 0
        
        mapped = 0
        
        for _, injury in unmapped.iterrows():
            clean_name = clean_player_name(injury['player_name'])
            
            # Exact match
            if clean_name.lower() in player_lookup:
                info = player_lookup[clean_name.lower()]
                conn.execute(text("""
                    UPDATE nfl_injuries_tracking 
                    SET player_id = :pid, team = :team, 
                        confidence_score = 0.99, last_updated = :ts
                    WHERE id = :id
                """), {
                    'pid': info['player_id'],
                    'team': info['team'],
                    'ts': datetime.now(),
                    'id': injury['id']
                })
                mapped += 1
                print(f"  ✅ {injury['player_name']} -> {info['team']}")
                continue
            
            # Variation match
            for var in create_name_variations(clean_name):
                if var in player_lookup:
                    info = player_lookup[var]
                    conn.execute(text("""
                        UPDATE nfl_injuries_tracking 
                        SET player_id = :pid, team = :team,
                            confidence_score = 0.95, last_updated = :ts
                        WHERE id = :id
                    """), {
                        'pid': info['player_id'],
                        'team': info['team'],
                        'ts': datetime.now(),
                        'id': injury['id']
                    })
                    mapped += 1
                    print(f"  🔄 {injury['player_name']} -> {info['team']}")
                    break
        
        print(f"\n📊 Mapped {mapped} injuries")
        return mapped

def main():
    """Run cloud injury model"""
    print("CLOUD INJURY MODEL - USING POSTGRESQL")
    print("=" * 50)
    
    lookup = build_cloud_player_lookup()
    mapped = map_injuries_cloud(lookup)
    
    print(f"\n✅ Complete! Mapped {mapped} injuries in cloud")
    return True

if __name__ == "__main__":
    main()