#!/usr/bin/env python3
"""
UPDATED injury_impact_fix.py - Enhanced for your existing database
Replaces your current injury_impact_fix.py
"""

import pandas as pd
from sqlalchemy import create_engine, text
import re
from datetime import datetime
import difflib

# Config
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

def diagnose_current_state():
    """Check what's currently in the database"""
    print("🔍 DIAGNOSING CURRENT STATE")
    print("=" * 35)
    
    try:
        with engine.connect() as conn:
            # Current injury stats
            stats = pd.read_sql(text("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(player_id) as mapped,
                    COUNT(DISTINCT team) as teams,
                    COUNT(CASE WHEN confidence_score >= 0.9 THEN 1 END) as high_conf,
                    COUNT(CASE WHEN team = 'UNKNOWN' THEN 1 END) as unknown_teams
                FROM nfl_injuries_tracking 
                WHERE is_active = 1
            """), conn).iloc[0]
            
            print(f"📊 Current Status:")
            print(f"  Total injuries: {stats['total']}")
            print(f"  Mapped: {stats['mapped']} ({stats['mapped']/stats['total']*100:.1f}%)")
            print(f"  High confidence: {stats['high_conf']}")
            print(f"  Teams: {stats['teams']}")
            print(f"  Unknown teams: {stats['unknown_teams']}")
            
            return stats
            
    except Exception as e:
        print(f"❌ Diagnosis failed: {e}")
        return None

def build_comprehensive_player_lookup():
    """Build lookup from all your existing player tables"""
    print("\n🏈 BUILDING COMPREHENSIVE PLAYER LOOKUP")
    print("=" * 45)
    
    try:
        with engine.connect() as conn:
            all_players = {}
            
            # Strategy 1: Use enhanced_nfl_players (your best table)
            try:
                enhanced_players = pd.read_sql(text("""
                    SELECT player_id, player_name, team, position, 'enhanced_nfl_players' as source
                    FROM enhanced_nfl_players 
                    WHERE is_active = 1
                """), conn)
                
                if not enhanced_players.empty:
                    for _, player in enhanced_players.iterrows():
                        clean_name = clean_player_name(player['player_name'])
                        if clean_name:
                            all_players[clean_name.lower()] = {
                                'player_id': player['player_id'],
                                'team': player['team'],
                                'position': player['position'],
                                'display_name': clean_name,
                                'source': 'enhanced_nfl_players',
                                'priority': 1
                            }
                    
                    print(f"✅ Enhanced players: {len(enhanced_players)} (PRIORITY)")
            except Exception as e:
                print(f"⚠️ enhanced_nfl_players: {e}")
            
            # Strategy 2: Add from accurate_nfl_roster_2024 if it exists
            try:
                accurate_players = pd.read_sql(text("""
                    SELECT player_id, player_name, team, position, source
                    FROM accurate_nfl_roster_2024 
                    WHERE is_active = 1
                """), conn)
                
                if not accurate_players.empty:
                    new_count = 0
                    for _, player in accurate_players.iterrows():
                        clean_name = clean_player_name(player['player_name'])
                        if clean_name and clean_name.lower() not in all_players:
                            all_players[clean_name.lower()] = {
                                'player_id': player['player_id'],
                                'team': player['team'],
                                'position': player['position'],
                                'display_name': clean_name,
                                'source': 'accurate_nfl_roster_2024',
                                'priority': 2
                            }
                            new_count += 1
                    
                    print(f"✅ Accurate roster: {new_count} new players")
            except Exception as e:
                print(f"⚠️ accurate_nfl_roster_2024: {e}")
            
            # Strategy 3: Add from your main game stats table
            try:
                game_stats_query = text("""
                    SELECT DISTINCT 
                        player_id,
                        player_display_name as player_name,
                        recent_team as team,
                        position_x as position
                    FROM player_game_stats 
                    WHERE season >= 2023
                    AND player_id IS NOT NULL 
                    AND player_display_name IS NOT NULL
                    AND recent_team IS NOT NULL
                    ORDER BY season DESC
                    LIMIT 2000
                """)
                
                game_players = pd.read_sql(game_stats_query, conn)
                
                if not game_players.empty:
                    new_count = 0
                    for _, player in game_players.iterrows():
                        clean_name = clean_player_name(player['player_name'])
                        if clean_name and clean_name.lower() not in all_players:
                            all_players[clean_name.lower()] = {
                                'player_id': player['player_id'],
                                'team': player['team'],
                                'position': player['position'] or 'UNK',
                                'display_name': clean_name,
                                'source': 'player_game_stats',
                                'priority': 3
                            }
                            new_count += 1
                    
                    print(f"✅ Game stats: {new_count} new players")
            except Exception as e:
                print(f"⚠️ player_game_stats: {e}")
            
            # Strategy 4: Add from other existing tables
            other_sources = [
                ('current_nfl_players', 'player_display_name', 'recent_team'),
                ('player_team_map', 'full_name', 'team'),
                ('player_season_summary', 'full_name', 'team')
            ]
            
            for table, name_col, team_col in other_sources:
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
                        LIMIT 1000
                    """)
                    
                    table_players = pd.read_sql(query, conn)
                    
                    if not table_players.empty:
                        new_count = 0
                        for _, player in table_players.iterrows():
                            clean_name = clean_player_name(player['player_name'])
                            if clean_name and clean_name.lower() not in all_players:
                                all_players[clean_name.lower()] = {
                                    'player_id': player['player_id'],
                                    'team': player['team'],
                                    'position': 'UNK',
                                    'display_name': clean_name,
                                    'source': table,
                                    'priority': 4
                                }
                                new_count += 1
                        
                        print(f"✅ {table}: {new_count} new players")
                        
                except Exception as e:
                    print(f"⚠️ {table}: {e}")
                    continue
            
            print(f"\n📊 TOTAL LOOKUP ENTRIES: {len(all_players)}")
            
            # Show team distribution
            team_counts = {}
            for player in all_players.values():
                team = player['team']
                team_counts[team] = team_counts.get(team, 0) + 1
            
            print(f"🏈 TOP TEAMS IN LOOKUP:")
            sorted_teams = sorted(team_counts.items(), key=lambda x: x[1], reverse=True)
            for team, count in sorted_teams[:10]:
                print(f"  {team}: {count} players")
            
            return all_players
            
    except Exception as e:
        print(f"❌ Lookup building failed: {e}")
        import traceback
        traceback.print_exc()
        return {}

def clean_player_name(name):
    """Enhanced name cleaning"""
    if not name or str(name) == 'nan':
        return ""
    
    name = str(name).strip()
  # Remove suffixes
    name = re.sub(r'\s+(Jr\.?|Sr\.?|III?|IV|V),?', '', name, flags=re.IGNORECASE)

    
    # Fix common abbreviations
    name = re.sub(r'\bTj\b', 'T.J.', name, flags=re.IGNORECASE)
    name = re.sub(r'\bCj\b', 'C.J.', name, flags=re.IGNORECASE)
    name = re.sub(r'\bAj\b', 'A.J.', name, flags=re.IGNORECASE)
    name = re.sub(r'\bJj\b', 'J.J.', name, flags=re.IGNORECASE)
    name = re.sub(r'\bBj\b', 'B.J.', name, flags=re.IGNORECASE)
    name = re.sub(r'\bDj\b', 'D.J.', name, flags=re.IGNORECASE)
    
    # Handle apostrophes
    name = name.replace("'", "'")
    
    # Clean whitespace
    name = ' '.join(name.split())
    
    return name.title()

def create_name_variations(name):
    """Create extensive name variations for better matching"""
    if not name:
        return []
    
    variations = set([name.lower()])
    
    # Handle dots and spaces
    variations.add(name.replace('.', '').lower())
    variations.add(name.replace(' ', '').lower())
    variations.add(name.replace('.', '').replace(' ', '').lower())
    
    # Handle apostrophes
    variations.add(name.replace("'", "").lower())
    variations.add(name.replace("'", "'").lower())
    variations.add(name.replace("'", "").lower())
    
    # Handle hyphens
    variations.add(name.replace('-', ' ').lower())
    variations.add(name.replace('-', '').lower())
    
    # Handle name parts
    parts = name.split()
    if len(parts) >= 2:
        first, last = parts[0], parts[-1]
        middle = parts[1:-1] if len(parts) > 2 else []
        
        # Common variations
        variations.add(f"{first[0]}. {last}".lower())
        variations.add(f"{first[0]} {last}".lower())
        variations.add(f"{first}{last}".lower())
        
        # With middle initials
        if middle:
            for m in middle:
                variations.add(f"{first} {m[0]}. {last}".lower())
                variations.add(f"{first[0]}. {m[0]}. {last}".lower())
        
        # Nickname mappings
        nickname_map = {
            'christopher': 'chris', 'chris': 'christopher',
            'michael': 'mike', 'mike': 'michael',
            'matthew': 'matt', 'matt': 'matthew',
            'anthony': 'tony', 'tony': 'anthony',
            'robert': 'rob', 'rob': 'robert',
            'william': 'will', 'will': 'william',
            'james': 'jim', 'jim': 'james',
            'thomas': 'tom', 'tom': 'thomas',
            'daniel': 'dan', 'dan': 'daniel',
            'benjamin': 'ben', 'ben': 'benjamin',
            'alexander': 'alex', 'alex': 'alexander',
            'andrew': 'andy', 'andy': 'andrew'
        }
        
        first_lower = first.lower()
        if first_lower in nickname_map:
            alt_first = nickname_map[first_lower]
            variations.add(f"{alt_first} {last}".lower())
            variations.add(f"{alt_first[0]}. {last}".lower())
    
    return list(variations)

def enhanced_injury_mapping(player_lookup):
    """Enhanced mapping with current roster focus"""
    print("\n🎯 ENHANCED INJURY MAPPING")
    print("=" * 30)
    
    if not player_lookup:
        print("❌ No player lookup available")
        return 0
    
    try:
        with engine.begin() as conn:
            # Get unmapped injuries
            unmapped_query = text("""
                SELECT id, player_name, team, designation
                FROM nfl_injuries_tracking 
                WHERE is_active = 1 
                AND player_id IS NULL
                ORDER BY 
                    CASE WHEN team = 'UNKNOWN' THEN 2 ELSE 1 END,
                    LENGTH(player_name) DESC,
                    player_name
            """)
            
            unmapped_injuries = pd.read_sql(unmapped_query, conn)
            
            if unmapped_injuries.empty:
                print("✅ No unmapped injuries found!")
                return 0
            
            print(f"🔄 Processing {len(unmapped_injuries)} unmapped injuries...")
            
            fixed_count = 0
            exact_matches = 0
            variation_matches = 0
            fuzzy_matches = 0
            
            for i, injury in unmapped_injuries.iterrows():
                player_name = injury['player_name']
                current_team = injury['team']
                
                clean_injury_name = clean_player_name(player_name)
                
                # Strategy 1: Exact match
                if clean_injury_name.lower() in player_lookup:
                    player_info = player_lookup[clean_injury_name.lower()]
                    
                    conn.execute(text("""
                        UPDATE nfl_injuries_tracking 
                        SET player_id = :player_id,
                            team = :correct_team,
                            confidence_score = 0.99,
                            last_updated = :timestamp,
                            notes = 'Enhanced mapping - exact match'
                        WHERE id = :injury_id
                    """), {
                        'player_id': player_info['player_id'],
                        'correct_team': player_info['team'],
                        'timestamp': datetime.now(),
                        'injury_id': injury['id']
                    })
                    
                    fixed_count += 1
                    exact_matches += 1
                    
                    source_icon = "⭐" if player_info['source'] == 'enhanced_nfl_players' else "✅"
                    print(f"{source_icon} {player_name} -> {player_info['team']} | {player_info['player_id']}")
                    
                    continue
                
                # Strategy 2: Name variations
                variations = create_name_variations(clean_injury_name)
                found_variation = False
                
                for variation in variations:
                    if variation in player_lookup:
                        player_info = player_lookup[variation]
                        
                        conn.execute(text("""
                            UPDATE nfl_injuries_tracking 
                            SET player_id = :player_id,
                                team = :correct_team,
                                confidence_score = 0.95,
                                last_updated = :timestamp,
                                notes = 'Enhanced mapping - name variation'
                            WHERE id = :injury_id
                        """), {
                            'player_id': player_info['player_id'],
                            'correct_team': player_info['team'],
                            'timestamp': datetime.now(),
                            'injury_id': injury['id']
                        })
                        
                        fixed_count += 1
                        variation_matches += 1
                        found_variation = True
                        
                        print(f"🔄 {player_name} -> {player_info['team']} | via '{variation}'")
                        break
                
                if found_variation:
                    continue
                
                # Strategy 3: Fuzzy matching
                best_match = find_best_fuzzy_match(clean_injury_name.lower(), player_lookup, threshold=0.85)
                
                if best_match and best_match['confidence'] > 0.85:
                    player_info = best_match['player_info']
                    
                    conn.execute(text("""
                        UPDATE nfl_injuries_tracking 
                        SET player_id = :player_id,
                            team = :correct_team,
                            confidence_score = :confidence,
                            last_updated = :timestamp,
                            notes = 'Enhanced mapping - fuzzy match'
                        WHERE id = :injury_id
                    """), {
                        'player_id': player_info['player_id'],
                        'correct_team': player_info['team'],
                        'confidence': best_match['confidence'],
                        'timestamp': datetime.now(),
                        'injury_id': injury['id']
                    })
                    
                    fixed_count += 1
                    fuzzy_matches += 1
                    
                    print(f"🎯 {player_name} -> {player_info['team']} | fuzzy: {best_match['confidence']:.2f}")
                
                # Progress indicator
                if (i + 1) % 25 == 0:
                    print(f"   ... processed {i+1}/{len(unmapped_injuries)}, fixed {fixed_count}")
            
            print(f"\n📊 ENHANCED MAPPING RESULTS:")
            print(f"  ✅ Exact matches: {exact_matches}")
            print(f"  🔄 Variation matches: {variation_matches}")
            print(f"  🎯 Fuzzy matches: {fuzzy_matches}")
            print(f"  📈 Total mapped: {fixed_count}")
            print(f"  📊 Success rate: {fixed_count/len(unmapped_injuries)*100:.1f}%")
            
            return fixed_count
            
    except Exception as e:
        print(f"❌ Enhanced mapping failed: {e}")
        import traceback
        traceback.print_exc()
        return 0

def find_best_fuzzy_match(target_name, player_lookup, threshold=0.85):
    """Enhanced fuzzy matching"""
    best_match = None
    best_score = 0
    
    # Prioritize enhanced players
    enhanced_items = [(k, v) for k, v in player_lookup.items() if v.get('source') == 'enhanced_nfl_players']
    other_items = [(k, v) for k, v in player_lookup.items() if v.get('source') != 'enhanced_nfl_players']
    
    # Search enhanced first, then others (limit for performance)
    search_items = enhanced_items + other_items[:1000]
    
    target_tokens = set(target_name.split())
    
    for name_key, player_info in search_items:
        # Multiple similarity algorithms
        
        # 1. Sequence similarity
        seq_similarity = difflib.SequenceMatcher(None, target_name, name_key).ratio()
        
        # 2. Token-based similarity
        candidate_tokens = set(name_key.split())
        if target_tokens and candidate_tokens:
            intersection = target_tokens.intersection(candidate_tokens)
            union = target_tokens.union(candidate_tokens)
            token_similarity = len(intersection) / len(union) if union else 0
        else:
            token_similarity = 0
        
        # 3. Last name matching
        target_last = target_name.split()[-1] if target_name.split() else ""
        candidate_last = name_key.split()[-1] if name_key.split() else ""
        last_name_similarity = 0
        if len(target_last) > 2 and len(candidate_last) > 2:
            if target_last.lower() == candidate_last.lower():
                last_name_similarity = 0.8
        
        # Take the best similarity score
        similarity = max(seq_similarity, token_similarity, last_name_similarity)
        
        # Bonus for enhanced players
        if player_info.get('source') == 'enhanced_nfl_players':
            similarity += 0.05
        
        if similarity > best_score and similarity >= threshold:
            best_score = similarity
            best_match = {
                'player_info': player_info,
                'confidence': min(similarity, 0.99),
                'matched_name': name_key
            }
    
    return best_match

def final_validation():
    """Final validation of the fixes"""
    print("\n📊 FINAL VALIDATION")
    print("=" * 25)
    
    try:
        with engine.connect() as conn:
            # Get final stats
            stats = pd.read_sql(text("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(player_id) as mapped,
                    COUNT(CASE WHEN confidence_score >= 0.9 THEN 1 END) as high_conf,
                    COUNT(CASE WHEN team != 'UNKNOWN' THEN 1 END) as team_known,
                    COUNT(DISTINCT team) as teams
                FROM nfl_injuries_tracking 
                WHERE is_active = 1
            """), conn).iloc[0]
            
            total = stats['total']
            mapped = stats['mapped']
            high_conf = stats['high_conf']
            team_known = stats['team_known']
            teams = stats['teams']
            
            mapping_rate = (mapped / total * 100) if total > 0 else 0
            team_rate = (team_known / total * 100) if total > 0 else 0
            
            print(f"📈 FINAL RESULTS:")
            print(f"  Total injuries: {total}")
            print(f"  Successfully mapped: {mapped} ({mapping_rate:.1f}%)")
            print(f"  High confidence: {high_conf}")
            print(f"  Teams identified: {team_known} ({team_rate:.1f}%)")
            print(f"  Unique teams: {teams}")
            
            # Check specific test players from your enhanced table
            test_players = ['DeForest Buckner', 'Charles Cross', 'Derek Barnett', 'Rashawn Slater']
            print(f"\n🧪 TEST ENHANCED PLAYERS:")
            
            for player_name in test_players:
                result = pd.read_sql(text("""
                    SELECT player_name, team, player_id, confidence_score
                    FROM nfl_injuries_tracking 
                    WHERE is_active = 1 
                    AND LOWER(player_name) LIKE :name
                    LIMIT 1
                """), conn, params={'name': f'%{player_name.lower()}%'})
                
                if not result.empty:
                    row = result.iloc[0]
                    print(f"  ✅ {row['player_name']}: {row['team']} | {row['player_id']} | {row['confidence_score']:.2f}")
                else:
                    print(f"  ❌ {player_name}: Not found in injuries")
            
            return mapping_rate
            
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return 0

def main():
    """Main execution with enhanced approach"""
    print("🏥 UPDATED INJURY MAPPING FIX - ENHANCED APPROACH")
    print("=" * 65)
    print("Uses your enhanced_nfl_players and existing data for better mapping")
    
    try:
        # Step 1: Diagnose current state
        current_stats = diagnose_current_state()
        
        # Step 2: Build comprehensive lookup from your tables
        player_lookup = build_comprehensive_player_lookup()
        
        if not player_lookup:
            print("❌ Cannot proceed without player lookup")
            return
        
        # Step 3: Enhanced injury mapping
        fixed_count = enhanced_injury_mapping(player_lookup)
        
        # Step 4: Final validation
        final_rate = final_validation()
        
        print(f"\n🏆 ENHANCED RESULTS:")
        print(f"  🔄 Players mapped: {fixed_count}")
        print(f"  📈 Final mapping rate: {final_rate:.1f}%")
        
        if final_rate > 75:
            print(f"\n🎉 EXCELLENT! Enhanced mapping working great!")
            print(f"🤖 Your AI bot has comprehensive player data!")
        elif final_rate > 65:
            print(f"\n🚀 GOOD! Significant improvement achieved!")
        else:
            print(f"\n✅ Improvement made with enhanced approach.")
        
        print(f"\n🚀 NEXT STEPS:")
        print(f"  1. Test: python injury_data_validation.py")
        print(f"  2. Your enhanced_nfl_players table is being prioritized!")
        print(f"  3. System uses all your existing player data!")
        
    except Exception as e:
        print(f"❌ Enhanced fix failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()