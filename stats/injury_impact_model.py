#!/usr/bin/env python3
"""
UPDATED injury_impact_fix.py - Current Roster Focus
Enhances your existing file to pull current NFL rosters and map properly
This replaces your current injury_impact_fix.py
"""

import pandas as pd
from sqlalchemy import create_engine, text
import re
from datetime import datetime
import difflib

# Config
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

def fetch_current_nfl_rosters():
    """Fetch current NFL rosters using nfl_data_py"""
    print("📡 FETCHING CURRENT NFL ROSTERS")
    print("=" * 35)
    
    try:
        import nfl_data_py as nfl
        
        # Get current season weekly data (contains current rosters)
        print("🔄 Downloading current NFL player data...")
        
        # Try multiple approaches to get current players
        try:
            # Method 1: Get recent weekly data which contains all active players
            recent_data = nfl.import_weekly_data([2024])
            if recent_data.empty:
                recent_data = nfl.import_weekly_data([2023])
            
            if not recent_data.empty:
                # Get unique players from recent weeks
                recent_data.columns = [c.strip().lower().replace(' ', '_') for c in recent_data.columns]
                
                # Get most recent week for each player
                rosters_2024 = recent_data.sort_values('week', ascending=False).drop_duplicates(
                    subset=['player_id'], keep='first'
                )[['player_id', 'player_display_name', 'recent_team']].rename(columns={
                    'player_display_name': 'full_name',
                    'recent_team': 'team'
                })
                
                print(f"✅ Downloaded {len(rosters_2024)} current NFL players from weekly data")
            else:
                rosters_2024 = pd.DataFrame()
                
        except Exception as e:
            print(f"⚠️ Weekly data approach failed: {e}")
            rosters_2024 = pd.DataFrame()
        
        # Method 2: Try seasonal data
        if rosters_2024.empty:
            try:
                seasonal_data = nfl.import_seasonal_data([2024])
                if seasonal_data.empty:
                    seasonal_data = nfl.import_seasonal_data([2023])
                
                if not seasonal_data.empty:
                    seasonal_data.columns = [c.strip().lower().replace(' ', '_') for c in seasonal_data.columns]
                    rosters_2024 = seasonal_data[['player_id', 'player_display_name', 'recent_team']].rename(columns={
                        'player_display_name': 'full_name',
                        'recent_team': 'team'
                    })
                    print(f"✅ Downloaded {len(rosters_2024)} current NFL players from seasonal data")
            except Exception as e:
                print(f"⚠️ Seasonal data approach failed: {e}")
        
        if rosters_2024.empty:
            print("❌ No current roster data available from nfl_data_py")
            return pd.DataFrame()
        
        # Store in database for future use
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS current_nfl_rosters"))
            rosters_2024.to_sql('current_nfl_rosters', conn, index=False)
        
        print(f"💾 Stored current rosters in database")
        return rosters_2024
        
    except ImportError:
        print("⚠️ nfl_data_py not available, using existing data")
        return pd.DataFrame()
    except Exception as e:
        print(f"⚠️ Roster fetch failed: {e}")
        return pd.DataFrame()

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
    """Build lookup from current rosters + existing data"""
    print("\n🏈 BUILDING COMPREHENSIVE PLAYER LOOKUP")
    print("=" * 45)
    
    # First try to get current rosters
    current_rosters = fetch_current_nfl_rosters()
    
    try:
        with engine.connect() as conn:
            all_players = {}
            
            # Strategy 1: Use current rosters if available
            if not current_rosters.empty:
                print(f"🔄 Processing {len(current_rosters)} current roster players...")
                
                for _, player in current_rosters.iterrows():
                    # Get name fields that exist
                    name_fields = ['full_name', 'display_name', 'first_name', 'last_name']
                    player_name = None
                    
                    for field in name_fields:
                        if field in current_rosters.columns and pd.notna(player.get(field)):
                            if field in ['first_name', 'last_name'] and player_name is None:
                                first = player.get('first_name', '')
                                last = player.get('last_name', '')
                                if first and last:
                                    player_name = f"{first} {last}"
                            else:
                                player_name = str(player[field])
                            break
                    
                    if player_name:
                        clean_name = clean_player_name(player_name)
                        team = player.get('team', 'UNK')
                        gsis_id = player.get('gsis_id', f"ROSTER_{hash(player_name) % 100000}")
                        
                        if clean_name:
                            all_players[clean_name.lower()] = {
                                'player_id': gsis_id,
                                'team': team,
                                'display_name': clean_name,
                                'source': 'current_roster'
                            }
                
                print(f"✅ Added {len(all_players)} players from current rosters")
            
            # Strategy 2: Add from existing database tables
            existing_sources = [
                ('player_stats_2024', 'player_display_name', 'recent_team'),
                ('player_game_stats', 'player_display_name', 'recent_team'),
                ('player_stats_2023', 'player_display_name', 'recent_team'),
                ('player_team_map', 'full_name', 'team'),
                ('player_season_summary', 'full_name', 'team')
            ]
            
            for table, name_col, team_col in existing_sources:
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
                        ORDER BY {name_col}
                        LIMIT 2000
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
                                    'display_name': clean_name,
                                    'source': table
                                }
                                new_count += 1
                        
                        print(f"✅ Added {new_count} new players from {table}")
                        
                except Exception as e:
                    print(f"⚠️ {table} not available: {e}")
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
    name = re.sub(r'\s+(Jr\.?|Sr\.?|III?|IV|V)$', '', name, flags=re.IGNORECASE)
    
    # Fix common abbreviations
    name = re.sub(r'\bTj\b', 'T.J.', name, flags=re.IGNORECASE)
    name = re.sub(r'\bCj\b', 'C.J.', name, flags=re.IGNORECASE)
    name = re.sub(r'\bAj\b', 'A.J.', name, flags=re.IGNORECASE)
    name = re.sub(r'\bJj\b', 'J.J.', name, flags=re.IGNORECASE)
    
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
    
    # Handle apostrophes
    if "'" in name:
        variations.add(name.replace("'", "").lower())
        variations.add(name.replace("'", "'").lower())  # Different apostrophe type
    
    # Handle dots and spaces
    variations.add(name.replace(".", "").lower())
    variations.add(name.replace(" ", "").lower())
    
    # Handle hyphens and special characters
    variations.add(name.replace("-", " ").lower())
    variations.add(name.replace("-", "").lower())
    
    # Handle nicknames and abbreviations
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
        
        # Extended nickname mappings
        nickname_map = {
            'marquise': 'hollywood', 'hollywood': 'marquise',
            'christopher': 'chris', 'chris': 'christopher',
            'michael': 'mike', 'mike': 'michael',
            'matthew': 'matt', 'matt': 'matthew',
            'anthony': 'tony', 'tony': 'anthony',
            'jonathan': 'jon', 'jon': 'jonathan',
            'benjamin': 'ben', 'ben': 'benjamin',
            'alexander': 'alex', 'alex': 'alexander',
            'andrew': 'andy', 'andy': 'andrew',
            'robert': 'rob', 'rob': 'robert', 'bobby': 'robert',
            'william': 'will', 'will': 'william', 'bill': 'william',
            'james': 'jim', 'jim': 'james', 'jimmy': 'james',
            'charles': 'chuck', 'chuck': 'charles', 'charlie': 'charles',
            'richard': 'rick', 'rick': 'richard', 'dick': 'richard',
            'thomas': 'tom', 'tom': 'thomas', 'tommy': 'thomas',
            'daniel': 'dan', 'dan': 'daniel', 'danny': 'daniel',
            'joseph': 'joe', 'joe': 'joseph', 'joey': 'joseph',
            'david': 'dave', 'dave': 'david', 'davey': 'david',
            'patrick': 'pat', 'pat': 'patrick', 'patty': 'patrick',
            'zachary': 'zach', 'zach': 'zachary', 'zack': 'zachary',
            'nicholas': 'nick', 'nick': 'nicholas', 'nicky': 'nicholas',
            'timothy': 'tim', 'tim': 'timothy', 'timmy': 'timothy'
        }
        
        first_lower = first.lower()
        if first_lower in nickname_map:
            alt_first = nickname_map[first_lower]
            variations.add(f"{alt_first} {last}".lower())
            variations.add(f"{alt_first[0]}. {last}".lower())
    
    return list(variations)

def enhanced_injury_mapping(player_lookup):
    """Enhanced mapping with current roster focus"""
    print("\n🎯 ENHANCED INJURY MAPPING WITH CURRENT ROSTERS")
    print("=" * 50)
    
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
                            notes = 'Current roster - exact match'
                        WHERE id = :injury_id
                    """), {
                        'player_id': player_info['player_id'],
                        'correct_team': player_info['team'],
                        'timestamp': datetime.now(),
                        'injury_id': injury['id']
                    })
                    
                    fixed_count += 1
                    exact_matches += 1
                    
                    source_icon = "🔄" if player_info['source'] == 'current_roster' else "✅"
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
                                notes = 'Current roster - name variation'
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
                
                # Strategy 3: Fuzzy matching with lower threshold
                best_match = find_best_fuzzy_match(clean_injury_name.lower(), player_lookup, threshold=0.80)
                
                if best_match and best_match['confidence'] > 0.80:
                    player_info = best_match['player_info']
                    
                    conn.execute(text("""
                        UPDATE nfl_injuries_tracking 
                        SET player_id = :player_id,
                            team = :correct_team,
                            confidence_score = :confidence,
                            last_updated = :timestamp,
                            notes = 'Current roster - fuzzy match'
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

def find_best_fuzzy_match(target_name, player_lookup, threshold=0.80):
    """Enhanced fuzzy matching with multiple algorithms"""
    best_match = None
    best_score = 0
    
    # Prioritize current roster players
    roster_items = [(k, v) for k, v in player_lookup.items() if v.get('source') == 'current_roster']
    other_items = [(k, v) for k, v in player_lookup.items() if v.get('source') != 'current_roster']
    
    # Search current roster first, then others (limit for performance)
    search_items = roster_items + other_items[:800]  # Increased search size
    
    target_tokens = set(target_name.split())
    
    for name_key, player_info in search_items:
        # Multiple similarity algorithms
        
        # 1. Sequence similarity
        seq_similarity = difflib.SequenceMatcher(None, target_name, name_key).ratio()
        
        # 2. Token-based similarity (good for name order differences)
        candidate_tokens = set(name_key.split())
        if target_tokens and candidate_tokens:
            intersection = target_tokens.intersection(candidate_tokens)
            union = target_tokens.union(candidate_tokens)
            token_similarity = len(intersection) / len(union) if union else 0
        else:
            token_similarity = 0
        
        # 3. Substring matching (good for partial names)
        substring_similarity = 0
        if len(target_name) > 3 and len(name_key) > 3:
            if target_name in name_key or name_key in target_name:
                substring_similarity = 0.7
        
        # 4. Last name matching (many players known by last name)
        target_last = target_name.split()[-1] if target_name.split() else ""
        candidate_last = name_key.split()[-1] if name_key.split() else ""
        last_name_similarity = 0
        if len(target_last) > 2 and len(candidate_last) > 2:
            if target_last.lower() == candidate_last.lower():
                last_name_similarity = 0.6
        
        # Take the best similarity score
        similarity = max(seq_similarity, token_similarity, substring_similarity, last_name_similarity)
        
        # Bonus for current roster players
        if player_info.get('source') == 'current_roster':
            similarity += 0.02
        
        # Bonus for exact last name match
        if last_name_similarity > 0:
            similarity += 0.05
        
        if similarity > best_score and similarity >= threshold:
            best_score = similarity
            best_match = {
                'player_info': player_info,
                'confidence': min(similarity, 0.99),  # Cap at 0.99 for fuzzy matches
                'matched_name': name_key
            }
    
    return best_match

def remove_duplicates():
    """Remove duplicate injury entries"""
    print("\n🧹 REMOVING DUPLICATES")
    print("=" * 25)
    
    try:
        with engine.begin() as conn:
            duplicate_removal = conn.execute(text("""
                DELETE FROM nfl_injuries_tracking 
                WHERE id NOT IN (
                    SELECT MIN(id)
                    FROM nfl_injuries_tracking 
                    WHERE is_active = 1
                    GROUP BY player_name, designation
                )
                AND is_active = 1
            """))
            
            print(f"🗑️ Removed {duplicate_removal.rowcount} duplicates")
            return duplicate_removal.rowcount
            
    except Exception as e:
        print(f"❌ Duplicate removal failed: {e}")
        return 0

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
            
            # Check specific test players
            test_players = ['Tyreek Hill', 'Jaylen Waddle', 'Stefon Diggs', 'Hollywood Brown', 'Tristan Wirfs']
            print(f"\n🧪 TEST PLAYERS CHECK:")
            
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
                    print(f"  ❌ {player_name}: Not found")
            
            return mapping_rate
            
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return 0

def main():
    """Main execution with current roster focus"""
    print("🏥 UPDATED INJURY MAPPING FIX - CURRENT ROSTER FOCUS")
    print("=" * 65)
    print("Enhanced to use current NFL rosters for better mapping")
    
    try:
        # Step 1: Diagnose current state
        current_stats = diagnose_current_state()
        
        # Step 2: Build comprehensive lookup with current rosters
        player_lookup = build_comprehensive_player_lookup()
        
        if not player_lookup:
            print("❌ Cannot proceed without player lookup")
            return
        
        # Step 3: Enhanced injury mapping
        fixed_count = enhanced_injury_mapping(player_lookup)
        
        # Step 4: Remove duplicates
        removed_dupes = remove_duplicates()
        
        # Step 5: Final validation
        final_rate = final_validation()
        
        print(f"\n🏁 COMPLETE RESULTS:")
        print(f"  🔄 Players mapped: {fixed_count}")
        print(f"  🗑️ Duplicates removed: {removed_dupes}")
        print(f"  📈 Final mapping rate: {final_rate:.1f}%")
        
        if final_rate > 45:
            print(f"\n🎉 EXCELLENT! Current roster mapping working great!")
            print(f"🤖 Your AI bot has comprehensive current player data!")
        elif final_rate > 35:
            print(f"\n🚀 GOOD! Significant improvement with current rosters!")
            print(f"🤖 Your AI bot has quality injury data!")
        else:
            print(f"\n✅ Improvement made with current roster approach.")
        
        print(f"\n🚀 NEXT STEPS:")
        print(f"  1. Test: python injury_data_validation.py")
        print(f"  2. Check team reports for current players")
        print(f"  3. Your system now uses current NFL rosters!")
        
    except Exception as e:
        print(f"❌ Enhanced fix failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()