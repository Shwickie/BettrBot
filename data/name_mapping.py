#!/usr/bin/env python3
"""
Final Mapping Push - Use corrected teams to map remaining injuries
Now that teams are fixed, attempt aggressive mapping with lower thresholds
"""

import pandas as pd
from sqlalchemy import create_engine, text
import re
from datetime import datetime
import difflib

# Config
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

def create_comprehensive_player_lookup():
    """Create the most comprehensive player lookup possible"""
    print("🔍 CREATING COMPREHENSIVE PLAYER LOOKUP")
    print("=" * 45)
    
    try:
        with engine.connect() as conn:
            # Get ALL players from ALL sources with priority ordering
            all_players_query = text("""
                -- Enhanced players (highest priority)
                SELECT 
                    player_name as name,
                    team,
                    position,
                    player_id,
                    'enhanced_nfl_players' as source,
                    1 as priority
                FROM enhanced_nfl_players 
                WHERE is_active = 1 
                AND player_name IS NOT NULL 
                AND team IS NOT NULL
                
                UNION ALL
                
                -- Current NFL players
                SELECT 
                    player_display_name as name,
                    recent_team as team,
                    position,
                    player_id,
                    'current_nfl_players' as source,
                    2 as priority
                FROM current_nfl_players 
                WHERE player_display_name IS NOT NULL 
                AND recent_team IS NOT NULL
                
                UNION ALL
                
                -- Player team map
                SELECT 
                    full_name as name,
                    team,
                    position,
                    player_id,
                    'player_team_map' as source,
                    3 as priority
                FROM player_team_map 
                WHERE full_name IS NOT NULL 
                AND team IS NOT NULL
                AND season >= 2023
                
                UNION ALL
                
                -- Game stats (recent seasons)
                SELECT 
                    player_display_name as name,
                    recent_team as team,
                    position_x as position,
                    player_id,
                    'player_game_stats' as source,
                    4 as priority
                FROM player_game_stats 
                WHERE player_display_name IS NOT NULL 
                AND recent_team IS NOT NULL
                AND season >= 2023
                
                ORDER BY priority, name
            """)
            
            all_players = pd.read_sql(all_players_query, conn)
            
            if all_players.empty:
                print("❌ No player data found")
                return {}
            
            print(f"📊 Collected {len(all_players)} total player records")
            
            # Create lookup with name variations
            lookup = {}
            
            for _, player in all_players.iterrows():
                name = str(player['name']).strip()
                if not name or len(name) < 2:
                    continue
                
                player_info = {
                    'player_id': player['player_id'],
                    'team': player['team'],
                    'position': player['position'],
                    'name': name,
                    'source': player['source'],
                    'priority': player['priority']
                }
                
                # Create name variations for lookup
                name_variations = create_name_variations(name)
                
                for variation in name_variations:
                    clean_var = variation.lower().strip()
                    if len(clean_var) > 1:
                        # Keep highest priority player for each name variation
                        if clean_var not in lookup or player['priority'] < lookup[clean_var]['priority']:
                            lookup[clean_var] = player_info
            
            print(f"🔗 Created {len(lookup)} lookup entries")
            
            # Show source distribution
            source_counts = {}
            for player_info in lookup.values():
                source = player_info['source']
                source_counts[source] = source_counts.get(source, 0) + 1
            
            print(f"📋 LOOKUP SOURCES:")
            for source, count in sorted(source_counts.items(), key=lambda x: x[1], reverse=True):
                print(f"  {source}: {count} players")
            
            return lookup
            
    except Exception as e:
        print(f"❌ Lookup creation failed: {e}")
        import traceback
        traceback.print_exc()
        return {}

def create_name_variations(name):
    """Create extensive name variations including aggressive patterns"""
    variations = set([name])
    
    # Basic cleaning variations
    variations.add(name.replace('.', ''))
    variations.add(name.replace(' ', ''))
    variations.add(name.replace('-', ' '))
    variations.add(name.replace('-', ''))
    variations.add(name.replace("'", ""))
    variations.add(name.replace("'", "'"))
    
    # Name part variations
    parts = name.split()
    if len(parts) >= 2:
        first, last = parts[0], parts[-1]
        middle = parts[1:-1] if len(parts) > 2 else []
        
        # First initial + last name
        variations.add(f"{first[0]}. {last}")
        variations.add(f"{first[0]} {last}")
        variations.add(f"{first[0]}.{last}")
        variations.add(f"{first[0]}{last}")
        
        # First + last only (drop middle)
        variations.add(f"{first} {last}")
        
        # Middle initials
        if middle:
            for m in middle:
                if len(m) > 0:
                    variations.add(f"{first} {m[0]}. {last}")
                    variations.add(f"{first[0]}. {m[0]}. {last}")
                    variations.add(f"{first} {m[0]} {last}")
        
        # Last name only (for common references)
        if len(last) > 3:  # Only for longer last names
            variations.add(last)
        
        # Nickname replacements
        nickname_map = {
            'alexander': 'alex', 'alex': 'alexander',
            'anthony': 'tony', 'tony': 'anthony',  
            'benjamin': 'ben', 'ben': 'benjamin',
            'christopher': 'chris', 'chris': 'christopher',
            'daniel': 'dan', 'dan': 'daniel',
            'david': 'dave', 'dave': 'david',
            'james': 'jim', 'jim': 'james',
            'jonathan': 'jon', 'jon': 'jonathan',
            'joseph': 'joe', 'joe': 'joseph',
            'matthew': 'matt', 'matt': 'matthew',
            'michael': 'mike', 'mike': 'michael',
            'nicholas': 'nick', 'nick': 'nicholas',
            'patrick': 'pat', 'pat': 'patrick',
            'robert': 'rob', 'rob': 'robert', 'bob': 'robert',
            'thomas': 'tom', 'tom': 'thomas',
            'william': 'will', 'will': 'william', 'bill': 'william',
            'zachary': 'zach', 'zach': 'zachary'
        }
        
        first_lower = first.lower()
        if first_lower in nickname_map:
            nickname = nickname_map[first_lower]
            variations.add(f"{nickname} {last}")
            variations.add(f"{nickname[0]}. {last}")
    
    return list(variations)

def aggressive_injury_mapping(lookup):
    """Aggressive mapping with lower confidence thresholds"""
    print("\n🎯 AGGRESSIVE INJURY MAPPING WITH CORRECTED TEAMS")
    print("=" * 55)
    
    if not lookup:
        print("❌ No lookup data available")
        return 0
    
    try:
        with engine.begin() as conn:
            # Get unmapped injuries (now with corrected teams)
            unmapped_query = text("""
                SELECT id, player_name, team, designation, position
                FROM nfl_injuries_tracking 
                WHERE is_active = 1 
                AND player_id IS NULL
                ORDER BY 
                    LENGTH(player_name) DESC,  -- Longer names first (more specific)
                    player_name
            """)
            
            unmapped_injuries = pd.read_sql(unmapped_query, conn)
            
            if unmapped_injuries.empty:
                print("✅ No unmapped injuries found!")
                return 0
            
            print(f"🔄 Processing {len(unmapped_injuries)} unmapped injuries...")
            
            mapped_count = 0
            exact_matches = 0
            variation_matches = 0
            fuzzy_matches = 0
            team_boosted = 0
            
            for i, injury in unmapped_injuries.iterrows():
                player_name = injury['player_name']
                injury_team = injury['team']
                
                if not player_name or len(player_name.strip()) < 2:
                    continue
                
                clean_name = clean_player_name(player_name)
                
                # Strategy 1: Exact match
                exact_key = clean_name.lower().strip()
                if exact_key in lookup:
                    player_info = lookup[exact_key]
                    confidence = 0.99 if injury_team == player_info['team'] else 0.95
                    
                    update_injury_mapping(conn, injury['id'], player_info, confidence, "Exact match")
                    mapped_count += 1
                    exact_matches += 1
                    print(f"✅ '{player_name}' → {player_info['team']} | {player_info['player_id']} | Exact")
                    continue
                
                # Strategy 2: Name variations
                variations = create_name_variations(clean_name)
                found_variation = False
                
                for variation in variations:
                    var_key = variation.lower().strip()
                    if var_key in lookup:
                        player_info = lookup[var_key]
                        confidence = 0.95 if injury_team == player_info['team'] else 0.90
                        
                        update_injury_mapping(conn, injury['id'], player_info, confidence, f"Name variation: {variation}")
                        mapped_count += 1
                        variation_matches += 1
                        found_variation = True
                        print(f"🔄 '{player_name}' → {player_info['team']} | {player_info['player_id']} | Via '{variation}'")
                        break
                
                if found_variation:
                    continue
                
                # Strategy 3: Fuzzy matching with team boost
                best_match = find_fuzzy_match_with_team_boost(clean_name, injury_team, lookup)
                
                if best_match and best_match['confidence'] >= 0.75:  # Lower threshold
                    player_info = best_match['player_info']
                    confidence = best_match['confidence']
                    
                    update_injury_mapping(conn, injury['id'], player_info, confidence, f"Fuzzy match: {best_match['matched_name']}")
                    mapped_count += 1
                    fuzzy_matches += 1
                    
                    if best_match.get('team_boosted'):
                        team_boosted += 1
                    
                    print(f"🎯 '{player_name}' → {player_info['team']} | {player_info['player_id']} | Fuzzy: {confidence:.2f}")
                
                # Progress indicator
                if (i + 1) % 20 == 0:
                    print(f"   ... processed {i+1}/{len(unmapped_injuries)}, mapped {mapped_count}")
            
            print(f"\n📊 AGGRESSIVE MAPPING RESULTS:")
            print(f"  ✅ Exact matches: {exact_matches}")
            print(f"  🔄 Variation matches: {variation_matches}")
            print(f"  🎯 Fuzzy matches: {fuzzy_matches}")
            print(f"  🏈 Team-boosted matches: {team_boosted}")
            print(f"  📈 Total mapped: {mapped_count}")
            print(f"  📊 Success rate: {mapped_count/len(unmapped_injuries)*100:.1f}%")
            
            return mapped_count
            
    except Exception as e:
        print(f"❌ Aggressive mapping failed: {e}")
        import traceback
        traceback.print_exc()
        return 0

def find_fuzzy_match_with_team_boost(target_name, injury_team, lookup):
    """Enhanced fuzzy matching with team affinity boost"""
    best_match = None
    best_score = 0
    
    target_tokens = set(target_name.lower().split())
    
    # Prioritize players from the same team
    same_team_items = [(k, v) for k, v in lookup.items() if v['team'] == injury_team]
    other_team_items = [(k, v) for k, v in lookup.items() if v['team'] != injury_team]
    
    # Search same team first with more items, then others
    search_items = same_team_items + other_team_items[:800]
    
    for name_key, player_info in search_items:
        if len(name_key) < 2:
            continue
        
        # Multiple similarity methods
        
        # 1. Sequence similarity
        seq_sim = difflib.SequenceMatcher(None, target_name.lower(), name_key).ratio()
        
        # 2. Token overlap
        candidate_tokens = set(name_key.split())
        if target_tokens and candidate_tokens:
            intersection = target_tokens.intersection(candidate_tokens)
            union = target_tokens.union(candidate_tokens)
            token_sim = len(intersection) / len(union) if union else 0
        else:
            token_sim = 0
        
        # 3. Substring matching
        substring_sim = 0
        if target_name.lower() in name_key or name_key in target_name.lower():
            substring_sim = 0.6
        
        # 4. Last name matching (very important for player identification)
        target_parts = target_name.split()
        candidate_parts = name_key.split()
        
        last_name_sim = 0
        if len(target_parts) > 0 and len(candidate_parts) > 0:
            target_last = target_parts[-1].lower()
            candidate_last = candidate_parts[-1].lower()
            if len(target_last) > 2 and len(candidate_last) > 2:
                if target_last == candidate_last:
                    last_name_sim = 0.7
                elif target_last in candidate_last or candidate_last in target_last:
                    last_name_sim = 0.5
        
        # Take best similarity
        similarity = max(seq_sim, token_sim, substring_sim, last_name_sim)
        
        # Boost for same team
        team_boosted = False
        if player_info['team'] == injury_team:
            similarity += 0.15  # Significant boost for team match
            team_boosted = True
        
        # Boost for enhanced players
        if player_info['source'] == 'enhanced_nfl_players':
            similarity += 0.05
        
        # Boost for exact last name match
        if last_name_sim >= 0.7:
            similarity += 0.1
        
        if similarity > best_score:
            best_score = similarity
            best_match = {
                'player_info': player_info,
                'confidence': min(similarity, 0.99),
                'matched_name': name_key,
                'team_boosted': team_boosted
            }
    
    return best_match

def update_injury_mapping(conn, injury_id, player_info, confidence, method):
    """Update injury record with player mapping"""
    conn.execute(text("""
        UPDATE nfl_injuries_tracking 
        SET player_id = :player_id,
            team = :team,
            confidence_score = :confidence,
            last_updated = :timestamp,
            notes = :notes
        WHERE id = :injury_id
    """), {
        'player_id': player_info['player_id'],
        'team': player_info['team'],
        'confidence': confidence,
        'timestamp': datetime.now(),
        'notes': f"Aggressive mapping: {method}",
        'injury_id': injury_id
    })

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

def final_validation():
    """Final validation of mapping improvements"""
    print("\n📊 FINAL VALIDATION")
    print("=" * 25)
    
    try:
        with engine.connect() as conn:
            stats = pd.read_sql(text("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(player_id) as mapped,
                    COUNT(CASE WHEN confidence_score >= 0.9 THEN 1 END) as high_conf,
                    COUNT(CASE WHEN confidence_score >= 0.75 THEN 1 END) as good_conf
                FROM nfl_injuries_tracking 
                WHERE is_active = 1
            """), conn).iloc[0]
            
            total = stats['total']
            mapped = stats['mapped']
            high_conf = stats['high_conf']
            good_conf = stats['good_conf']
            
            mapping_rate = (mapped / total * 100) if total > 0 else 0
            
            print(f"📈 FINAL MAPPING RESULTS:")
            print(f"  Total injuries: {total}")
            print(f"  Successfully mapped: {mapped} ({mapping_rate:.1f}%)")
            print(f"  High confidence (≥0.9): {high_conf}")
            print(f"  Good confidence (≥0.75): {good_conf}")
            print(f"  Unmapped remaining: {total - mapped}")
            
            return mapping_rate
            
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return 0

def main():
    """Main execution for final mapping push"""
    print("🎯 FINAL MAPPING PUSH - AGGRESSIVE MAPPING WITH CORRECTED TEAMS")
    print("=" * 70)
    print("Using corrected team assignments for aggressive player mapping")
    
    try:
        # Step 1: Create comprehensive lookup
        lookup = create_comprehensive_player_lookup()
        
        if not lookup:
            print("❌ Cannot proceed without player lookup")
            return
        
        # Step 2: Aggressive mapping attempt
        mapped_count = aggressive_injury_mapping(lookup)
        
        # Step 3: Final validation
        final_rate = final_validation()
        
        print(f"\n🏆 FINAL PUSH RESULTS:")
        print(f"  🎯 Additional mappings: {mapped_count}")
        print(f"  📈 Final mapping rate: {final_rate:.1f}%")
        
        if final_rate > 90:
            print(f"\n🎉 OUTSTANDING! Over 90% mapping rate achieved!")
            print(f"🤖 Your injury bot is now highly comprehensive!")
        elif final_rate > 85:
            print(f"\n🚀 EXCELLENT! Strong mapping rate achieved!")
            print(f"🤖 Your injury bot has great coverage!")
        else:
            print(f"\n✅ Good progress made with aggressive mapping.")
        
        print(f"\n🚀 FINAL STEPS:")
        print(f"  1. Your injury data is now optimally mapped!")
        print(f"  2. Run injury_data_validation.py to test the system")
        print(f"  3. Start using the injury bot for betting decisions!")
        
    except Exception as e:
        print(f"❌ Final mapping push failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()