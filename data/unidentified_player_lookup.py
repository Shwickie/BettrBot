#!/usr/bin/env python3
"""
Unified Player Lookup - Uses ALL existing player tables for mapping
This fixes the injury mapping by combining all your player sources
"""

import pandas as pd
from sqlalchemy import create_engine, text
import re
from datetime import datetime
import difflib

# Config
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

def create_unified_player_lookup():
    """Create unified lookup from ALL existing player tables"""
    print("🔄 CREATING UNIFIED PLAYER LOOKUP FROM ALL TABLES")
    print("=" * 55)
    
    all_players = []
    
    with engine.connect() as conn:
        # Table 1: enhanced_nfl_players (your key missing players!)
        try:
            enhanced = pd.read_sql(text("""
                SELECT player_name, team, position, player_id, 'enhanced_nfl_players' as source
                FROM enhanced_nfl_players 
                WHERE is_active = 1
            """), conn)
            
            for _, row in enhanced.iterrows():
                all_players.append({
                    'player_name': clean_player_name(row['player_name']),
                    'team': row['team'],
                    'position': row['position'],
                    'player_id': row['player_id'],
                    'source': 'enhanced_key_players',
                    'priority': 1  # Highest priority
                })
            
            print(f"✅ enhanced_nfl_players: {len(enhanced)} players (HIGH PRIORITY)")
            
        except Exception as e:
            print(f"⚠️ enhanced_nfl_players: {e}")
        
        # Table 2: current_nfl_players
        try:
            current = pd.read_sql(text("""
                SELECT player_display_name as player_name, recent_team as team, position, player_id, 'current_nfl_players' as source
                FROM current_nfl_players
                WHERE player_id IS NOT NULL AND player_display_name IS NOT NULL
            """), conn)
            
            for _, row in current.iterrows():
                all_players.append({
                    'player_name': clean_player_name(row['player_name']),
                    'team': row['team'],
                    'position': row['position'] or 'UNK',
                    'player_id': row['player_id'],
                    'source': 'current_nfl_players',
                    'priority': 2
                })
            
            print(f"✅ current_nfl_players: {len(current)} players")
            
        except Exception as e:
            print(f"⚠️ current_nfl_players: {e}")
        
        # Table 3: complete_nfl_players
        try:
            complete = pd.read_sql(text("""
                SELECT player_display_name as player_name, recent_team as team, position, player_id, 'complete_nfl_players' as source
                FROM complete_nfl_players
                WHERE player_id IS NOT NULL AND player_display_name IS NOT NULL
            """), conn)
            
            for _, row in complete.iterrows():
                all_players.append({
                    'player_name': clean_player_name(row['player_name']),
                    'team': row['team'],
                    'position': row['position'] or 'UNK',
                    'player_id': row['player_id'],
                    'source': 'complete_nfl_players',
                    'priority': 2
                })
            
            print(f"✅ complete_nfl_players: {len(complete)} players")
            
        except Exception as e:
            print(f"⚠️ complete_nfl_players: {e}")
        
        # Table 4: player_team_map
        try:
            team_map = pd.read_sql(text("""
                SELECT full_name as player_name, team, position, player_id, 'player_team_map' as source
                FROM player_team_map
                WHERE player_id IS NOT NULL AND full_name IS NOT NULL
            """), conn)
            
            for _, row in team_map.iterrows():
                all_players.append({
                    'player_name': clean_player_name(row['player_name']),
                    'team': row['team'],
                    'position': row['position'] or 'UNK',
                    'player_id': row['player_id'],
                    'source': 'player_team_map',
                    'priority': 3
                })
            
            print(f"✅ player_team_map: {len(team_map)} players")
            
        except Exception as e:
            print(f"⚠️ player_team_map: {e}")
        
        # Table 5: player_season_summary
        try:
            season_summary = pd.read_sql(text("""
                SELECT full_name as player_name, team, position, player_id, 'player_season_summary' as source
                FROM player_season_summary
                WHERE player_id IS NOT NULL AND full_name IS NOT NULL
            """), conn)
            
            for _, row in season_summary.iterrows():
                all_players.append({
                    'player_name': clean_player_name(row['player_name']),
                    'team': row['team'],
                    'position': row['position'] or 'UNK',
                    'player_id': row['player_id'],
                    'source': 'player_season_summary',
                    'priority': 3
                })
            
            print(f"✅ player_season_summary: {len(season_summary)} players")
            
        except Exception as e:
            print(f"⚠️ player_season_summary: {e}")
        
        # Table 6: Recent player stats tables
        for year in [2024, 2023]:
            try:
                stats = pd.read_sql(text(f"""
                    SELECT player_display_name as player_name, recent_team as team, position, player_id, 'player_stats_{year}' as source
                    FROM player_stats_{year}
                    WHERE player_id IS NOT NULL AND player_display_name IS NOT NULL
                    GROUP BY player_id
                """), conn)
                
                for _, row in stats.iterrows():
                    all_players.append({
                        'player_name': clean_player_name(row['player_name']),
                        'team': row['team'],
                        'position': row['position'] or 'UNK',
                        'player_id': row['player_id'],
                        'source': f'player_stats_{year}',
                        'priority': 4
                    })
                
                print(f"✅ player_stats_{year}: {len(stats)} players")
                
            except Exception as e:
                print(f"⚠️ player_stats_{year}: {e}")
    
    print(f"\n📊 TOTAL PLAYERS COLLECTED: {len(all_players)}")
    return all_players

def deduplicate_players(all_players):
    """Remove duplicates, keeping highest priority version"""
    print(f"\n🔧 DEDUPLICATING PLAYERS (KEEPING HIGHEST PRIORITY)")
    print("=" * 55)
    
    # Sort by priority (1 = highest)
    all_players.sort(key=lambda x: x['priority'])
    
    unique_players = {}
    
    for player in all_players:
        # Create key for deduplication
        name_key = player['player_name'].lower().strip()
        team_key = player['team']
        
        # Primary key: name + team
        primary_key = f"{name_key}|{team_key}"
        
        # Secondary key: just name (for players who might have wrong team)
        name_only_key = name_key
        
        # If we haven't seen this exact player before, add them
        if primary_key not in unique_players:
            unique_players[primary_key] = player
        # If we have a better priority source, use name-only key
        elif name_only_key not in unique_players or player['priority'] < unique_players[name_only_key]['priority']:
            unique_players[name_only_key] = player
    
    # Convert back to list
    final_players = list(unique_players.values())
    
    print(f"📉 Deduplicated: {len(all_players)} → {len(final_players)} unique players")
    
    # Show source distribution
    source_counts = {}
    for player in final_players:
        source = player['source']
        source_counts[source] = source_counts.get(source, 0) + 1
    
    print(f"\n📋 SOURCES IN FINAL LIST:")
    for source, count in sorted(source_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {source}: {count} players")
    
    return final_players

def create_lookup_dictionary(unique_players):
    """Create comprehensive lookup dictionary with name variations"""
    print(f"\n🔍 CREATING LOOKUP DICTIONARY WITH NAME VARIATIONS")
    print("=" * 55)
    
    lookup = {}
    
    for player in unique_players:
        player_name = player['player_name']
        
        # Create all possible lookup keys
        lookup_keys = create_name_variations(player_name)
        
        for key in lookup_keys:
            clean_key = key.lower().strip()
            if clean_key and len(clean_key) > 1:
                # If key doesn't exist or current player has higher priority
                if clean_key not in lookup or player['priority'] < lookup[clean_key]['priority']:
                    lookup[clean_key] = player
    
    print(f"🔗 Created {len(lookup)} lookup entries from {len(unique_players)} players")
    return lookup

def create_name_variations(name):
    """Create comprehensive name variations"""
    variations = set([name])
    
    # Basic variations
    variations.add(name.replace('.', ''))
    variations.add(name.replace(' ', ''))
    variations.add(name.replace('.', '').replace(' ', ''))
    variations.add(name.replace("'", ""))
    variations.add(name.replace("'", "'"))
    
    # Handle name parts
    parts = name.split()
    if len(parts) >= 2:
        first, last = parts[0], parts[-1]
        
        # First initial variations
        if len(first) > 0:
            variations.add(f"{first[0]}. {last}")
            variations.add(f"{first[0]} {last}")
            variations.add(f"{first[0]}{last}")
            
        # Middle initial handling
        if len(parts) > 2:
            for i, middle in enumerate(parts[1:-1], 1):
                if len(middle) > 0:
                    variations.add(f"{first} {middle[0]}. {last}")
                    variations.add(f"{first[0]}. {middle[0]}. {last}")
    
    # Common nickname patterns
    nickname_map = {
        'deforest': 'buckner',  # Special case for DeForest Buckner
        'christopher': 'chris', 'chris': 'christopher',
        'michael': 'mike', 'mike': 'michael',
        'matthew': 'matt', 'matt': 'matthew',
        'anthony': 'tony', 'tony': 'anthony',
        'thomas': 'tom', 'tom': 'thomas',
        'william': 'will', 'will': 'william',
        'james': 'jim', 'jim': 'james',
        'robert': 'rob', 'rob': 'robert',
        'benjamin': 'ben', 'ben': 'benjamin',
        'alexander': 'alex', 'alex': 'alexander'
    }
    
    # Apply nickname variations
    for original, nickname in nickname_map.items():
        name_lower = name.lower()
        if original in name_lower:
            new_name = name_lower.replace(original, nickname)
            variations.add(new_name.title())
        if nickname in name_lower:
            new_name = name_lower.replace(nickname, original)
            variations.add(new_name.title())
    
    return list(variations)

def update_injury_mappings_unified(lookup):
    """Update injury mappings using unified lookup"""
    print(f"\n🔄 UPDATING INJURY MAPPINGS WITH UNIFIED LOOKUP")
    print("=" * 52)
    
    with engine.connect() as conn:
        # Get unmapped injuries
        unmapped_injuries = pd.read_sql(text("""
            SELECT id, player_name, team, designation, injury_detail
            FROM nfl_injuries_tracking 
            WHERE is_active = 1 
            AND player_id IS NULL
            ORDER BY 
                CASE WHEN team = 'UNKNOWN' THEN 2 ELSE 1 END,
                player_name
        """), conn)
        
        if unmapped_injuries.empty:
            print("✅ No unmapped injuries found!")
            return 0
        
        print(f"🔄 Processing {len(unmapped_injuries)} unmapped injuries...")
        
        fixed_count = 0
        high_conf_count = 0
        
        with engine.begin() as trans_conn:
            for i, injury in unmapped_injuries.iterrows():
                player_name = injury['player_name']
                injury_team = injury['team']
                
                # Try exact match first
                clean_name = clean_player_name(player_name)
                match_result = find_best_match(clean_name, injury_team, lookup)
                
                if match_result:
                    player_info = match_result['player']
                    confidence = match_result['confidence']
                    
                    trans_conn.execute(text("""
                        UPDATE nfl_injuries_tracking 
                        SET player_id = :player_id,
                            team = :correct_team,
                            confidence_score = :confidence,
                            last_updated = :timestamp,
                            notes = :notes
                        WHERE id = :injury_id
                    """), {
                        'player_id': player_info['player_id'],
                        'correct_team': player_info['team'],
                        'confidence': confidence,
                        'timestamp': datetime.now(),
                        'notes': f"Unified lookup - {player_info['source']}",
                        'injury_id': injury['id']
                    })
                    
                    fixed_count += 1
                    if confidence >= 0.95:
                        high_conf_count += 1
                    
                    # Show result
                    conf_icon = "🎯" if confidence >= 0.95 else "✅" if confidence >= 0.85 else "⚠️"
                    source_icon = "⭐" if player_info['source'] == 'enhanced_key_players' else ""
                    print(f"{conf_icon}{source_icon} '{player_name}' → {player_info['team']} | {player_info['player_id']} | {confidence:.2f}")
                
                # Progress indicator
                if (i + 1) % 25 == 0:
                    print(f"   ... processed {i+1}/{len(unmapped_injuries)}, fixed {fixed_count}")
        
        print(f"\n📊 UNIFIED MAPPING RESULTS:")
        print(f"  ✅ Successfully mapped: {fixed_count}")
        print(f"  🎯 High confidence (≥0.95): {high_conf_count}")
        print(f"  📈 Success rate: {fixed_count/len(unmapped_injuries)*100:.1f}%")
        
        return fixed_count

def find_best_match(player_name, injury_team, lookup):
    """Find best match using unified lookup"""
    if not player_name:
        return None
    
    # Strategy 1: Exact name match
    clean_name = player_name.lower().strip()
    if clean_name in lookup:
        player = lookup[clean_name]
        confidence = 1.0 if injury_team == player['team'] else 0.95
        return {'player': player, 'confidence': confidence}
    
    # Strategy 2: Try name variations
    variations = create_name_variations(player_name)
    for variation in variations:
        var_key = variation.lower().strip()
        if var_key in lookup:
            player = lookup[var_key]
            confidence = 0.95 if injury_team == player['team'] else 0.90
            return {'player': player, 'confidence': confidence}
    
    # Strategy 3: Fuzzy matching
    best_match = None
    best_score = 0
    
    # Limit fuzzy search to avoid performance issues
    search_keys = list(lookup.keys())[:2000]  # Sample for performance
    
    for lookup_key in search_keys:
        if len(lookup_key) < 3:  # Skip very short keys
            continue
            
        # Calculate similarity
        similarity = difflib.SequenceMatcher(None, clean_name, lookup_key).ratio()
        
        # Boost score for team match
        if injury_team != 'UNKNOWN' and lookup[lookup_key]['team'] == injury_team:
            similarity += 0.1
        
        # Boost score for enhanced players
        if lookup[lookup_key]['source'] == 'enhanced_key_players':
            similarity += 0.05
        
        if similarity > best_score and similarity > 0.80:
            best_score = similarity
            best_match = lookup[lookup_key]
    
    if best_match:
        return {'player': best_match, 'confidence': min(best_score, 0.95)}
    
    return None

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
    
    # Fix specific known cases
    name = re.sub(r'\bDeforest\b', 'DeForest', name, flags=re.IGNORECASE)
    
    # Handle apostrophes
    name = name.replace("'", "'")
    
    # Clean whitespace
    name = ' '.join(name.split())
    
    return name.title().strip()

def validate_unified_results():
    """Validate the unified mapping results"""
    print(f"\n📊 VALIDATING UNIFIED RESULTS")
    print("=" * 35)
    
    with engine.connect() as conn:
        # Get final statistics
        total = conn.execute(text("SELECT COUNT(*) FROM nfl_injuries_tracking WHERE is_active = 1")).scalar()
        mapped = conn.execute(text("SELECT COUNT(*) FROM nfl_injuries_tracking WHERE is_active = 1 AND player_id IS NOT NULL")).scalar()
        high_conf = conn.execute(text("SELECT COUNT(*) FROM nfl_injuries_tracking WHERE is_active = 1 AND confidence_score >= 0.95")).scalar()
        enhanced_mapped = conn.execute(text("SELECT COUNT(*) FROM nfl_injuries_tracking WHERE is_active = 1 AND notes LIKE '%enhanced%'")).scalar()
        
        mapping_rate = (mapped / total * 100) if total > 0 else 0
        
        print(f"📈 FINAL UNIFIED RESULTS:")
        print(f"  Total injuries: {total}")
        print(f"  Successfully mapped: {mapped} ({mapping_rate:.1f}%)")
        print(f"  High confidence: {high_conf}")
        print(f"  ⭐ Enhanced key players: {enhanced_mapped}")
        
        # Test the key missing players
        test_players = ['DeForest Buckner', 'Charles Cross', 'Derek Barnett', 'Rashawn Slater', 'T.J. Watt']
        print(f"\n🧪 TESTING KEY MISSING PLAYERS:")
        
        found_count = 0
        for player_name in test_players:
            result = pd.read_sql(text("""
                SELECT player_name, team, player_id, confidence_score, notes
                FROM nfl_injuries_tracking 
                WHERE is_active = 1 
                AND player_id IS NOT NULL
                AND LOWER(player_name) LIKE :name
                LIMIT 1
            """), conn, params={'name': f'%{player_name.lower()}%'})
            
            if not result.empty:
                row = result.iloc[0]
                found_count += 1
                enhanced_icon = "⭐" if 'enhanced' in str(row['notes']) else ""
                print(f"  ✅{enhanced_icon} {row['player_name']}: {row['team']} | {row['player_id']} | {row['confidence_score']:.2f}")
            else:
                print(f"  ❌ {player_name}: Still not found")
        
        print(f"\n⭐ Key players found: {found_count}/{len(test_players)}")
        return mapping_rate, found_count

def main():
    """Main execution for unified player lookup"""
    print("🔄 UNIFIED PLAYER LOOKUP - USES ALL YOUR TABLES")
    print("=" * 60)
    print("Combines ALL existing player tables for comprehensive mapping")
    
    try:
        # Step 1: Collect players from all tables
        all_players = create_unified_player_lookup()
        
        if not all_players:
            print("❌ No players collected")
            return
        
        # Step 2: Deduplicate players
        unique_players = deduplicate_players(all_players)
        
        # Step 3: Create lookup dictionary
        lookup = create_lookup_dictionary(unique_players)
        
        # Step 4: Update injury mappings
        fixed_count = update_injury_mappings_unified(lookup)
        
        # Step 5: Validate results
        final_rate, key_found = validate_unified_results()
        
        print(f"\n🏆 UNIFIED LOOKUP RESULTS:")
        print(f"  📊 Total unique players: {len(unique_players)}")
        print(f"  🔗 Lookup entries created: {len(lookup)}")
        print(f"  🔄 Additional mappings: {fixed_count}")
        print(f"  📈 Final mapping rate: {final_rate:.1f}%")
        print(f"  ⭐ Key missing players found: {key_found}")
        
        if final_rate > 70:
            print(f"\n🎉 EXCELLENT! Unified lookup working great!")
            print(f"🤖 Your injury system is now much more comprehensive!")
        elif final_rate > 60:
            print(f"\n🚀 GOOD! Significant improvement achieved!")
        else:
            print(f"\n✅ Some improvement made.")
        
        print(f"\n🚀 NEXT STEPS:")
        print(f"  1. Check specific players: DeForest Buckner, Charles Cross")
        print(f"  2. Your enhanced_nfl_players table is being used!")
        print(f"  3. Run injury_data_validation.py to test results")
        
    except Exception as e:
        print(f"❌ Unified lookup failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()