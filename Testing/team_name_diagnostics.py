#!/usr/bin/env python3
"""
Team Name Diagnostic Tool - Find and fix team name mismatches
"""

import pandas as pd
from sqlalchemy import create_engine, text

# Config
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

def get_team_names_from_odds():
    """Get unique team names from odds data"""
    print("📊 TEAM NAMES IN ODDS DATA")
    print("=" * 35)
    
    try:
        with engine.connect() as conn:
            # Get team names from games (home/away)
            games_teams = pd.read_sql(text("""
                SELECT DISTINCT home_team as team FROM games 
                WHERE game_date >= '2025-09-01'
                UNION
                SELECT DISTINCT away_team as team FROM games 
                WHERE game_date >= '2025-09-01'
                ORDER BY team
            """), conn)
            
            print(f"🏈 Teams in GAMES table ({len(games_teams)} teams):")
            for _, row in games_teams.iterrows():
                print(f"  '{row['team']}'")
            
            return games_teams['team'].tolist()
            
    except Exception as e:
        print(f"❌ Error getting team names from odds: {e}")
        return []

def get_team_names_from_power_ratings():
    """Get unique team names from power ratings"""
    print(f"\n⚡ TEAM NAMES IN POWER RATINGS")
    print("=" * 40)
    
    try:
        with engine.connect() as conn:
            power_teams = pd.read_sql(text("""
                SELECT team, power_score 
                FROM team_season_summary 
                WHERE season = 2024
                ORDER BY team
            """), conn)
            
            print(f"💪 Teams in POWER RATINGS table ({len(power_teams)} teams):")
            for _, row in power_teams.iterrows():
                print(f"  '{row['team']}' (Power: {row['power_score']:.1f})")
            
            return power_teams['team'].tolist()
            
    except Exception as e:
        print(f"❌ Error getting team names from power ratings: {e}")
        return []

def find_team_name_mismatches(odds_teams, power_teams):
    """Find mismatches between team names"""
    print(f"\n🔍 TEAM NAME MISMATCH ANALYSIS")
    print("=" * 40)
    
    odds_set = set(odds_teams)
    power_set = set(power_teams)
    
    # Teams in odds but not in power ratings
    missing_in_power = odds_set - power_set
    # Teams in power ratings but not in odds
    missing_in_odds = power_set - odds_set
    # Teams that match perfectly
    matching = odds_set & power_set
    
    print(f"✅ MATCHING TEAMS ({len(matching)}):")
    for team in sorted(matching):
        print(f"  '{team}'")
    
    print(f"\n❌ IN ODDS BUT NOT IN POWER RATINGS ({len(missing_in_power)}):")
    for team in sorted(missing_in_power):
        print(f"  '{team}'")
    
    print(f"\n❓ IN POWER RATINGS BUT NOT IN ODDS ({len(missing_in_odds)}):")
    for team in sorted(missing_in_odds):
        print(f"  '{team}'")
    
    return missing_in_power, missing_in_odds, matching

def suggest_team_name_mapping(missing_in_power, power_teams):
    """Suggest mappings for mismatched team names"""
    print(f"\n💡 SUGGESTED TEAM NAME MAPPINGS")
    print("=" * 40)
    
    # Common team name variations
    name_mappings = {}
    
    for odds_team in missing_in_power:
        best_match = None
        best_score = 0
        
        # Try to find similar team names
        for power_team in power_teams:
            # Simple similarity check
            odds_words = odds_team.lower().split()
            power_words = power_team.lower().split()
            
            # Check for common words
            common_words = set(odds_words) & set(power_words)
            if common_words:
                score = len(common_words)
                if score > best_score:
                    best_score = score
                    best_match = power_team
            
            # Check for city/team name matches
            city_matches = [
                ('Los Angeles', 'LA'), ('New York', 'NY'), ('Green Bay', 'GB'),
                ('New England', 'NE'), ('Kansas City', 'KC'), ('San Francisco', 'SF'),
                ('Tampa Bay', 'TB'), ('Las Vegas', 'LV')
            ]
            
            for full_name, abbrev in city_matches:
                if full_name.lower() in odds_team.lower() and abbrev.lower() in power_team.lower():
                    best_match = power_team
                    best_score = 10
                elif abbrev.lower() in odds_team.lower() and full_name.lower() in power_team.lower():
                    best_match = power_team
                    best_score = 10
        
        if best_match and best_score > 0:
            name_mappings[odds_team] = best_match
            print(f"  '{odds_team}' → '{best_match}' (confidence: {best_score})")
        else:
            print(f"  '{odds_team}' → ❓ NO MATCH FOUND")
    
    return name_mappings

def create_team_mapping_fix(name_mappings):
    """Create SQL to fix team name mismatches"""
    print(f"\n🔧 TEAM NAME FIXING OPTIONS")
    print("=" * 35)
    
    if not name_mappings:
        print("✅ No team name fixes needed!")
        return
    
    print("📝 OPTION 1: Update team_season_summary table")
    print("Copy and run these SQL commands:")
    print()
    
    for odds_name, power_name in name_mappings.items():
        print(f"UPDATE team_season_summary SET team = '{odds_name}' WHERE team = '{power_name}' AND season = 2024;")
    
    print()
    print("📝 OPTION 2: Create a team mapping function in the betting engine")
    print("Add this mapping dictionary to your betting engine:")
    print()
    print("TEAM_NAME_MAPPING = {")
    for odds_name, power_name in name_mappings.items():
        print(f"    '{odds_name}': '{power_name}',")
    print("}")

def test_sample_game():
    """Test a sample game to see the exact team name issue"""
    print(f"\n🧪 TESTING SAMPLE GAME")
    print("=" * 25)
    
    try:
        with engine.connect() as conn:
            # Get a sample game
            sample = pd.read_sql(text("""
                SELECT 
                    g.home_team,
                    g.away_team,
                    g.game_date,
                    COUNT(o.id) as odds_count
                FROM games g
                LEFT JOIN odds o ON g.game_id = o.game_id
                WHERE g.game_date >= '2025-09-01'
                GROUP BY g.game_id
                ORDER BY g.game_date
                LIMIT 1
            """), conn)
            
            if not sample.empty:
                game = sample.iloc[0]
                home_team = game['home_team']
                away_team = game['away_team']
                
                print(f"🏈 Sample Game: {away_team} @ {home_team}")
                print(f"📅 Date: {game['game_date']}")
                print(f"💰 Odds count: {game['odds_count']}")
                
                # Check if these teams exist in power ratings
                power_check = pd.read_sql(text("""
                    SELECT team, power_score 
                    FROM team_season_summary 
                    WHERE season = 2024 
                    AND (team = :home OR team = :away)
                """), conn, params={'home': home_team, 'away': away_team})
                
                print(f"\n🔍 Power ratings lookup:")
                if power_check.empty:
                    print(f"  ❌ Neither '{home_team}' nor '{away_team}' found in power ratings")
                else:
                    for _, row in power_check.iterrows():
                        print(f"  ✅ '{row['team']}' found (Power: {row['power_score']:.1f})")
        
    except Exception as e:
        print(f"❌ Error testing sample game: {e}")

def main():
    """Main diagnostic function"""
    print("🏈 TEAM NAME DIAGNOSTIC TOOL")
    print("=" * 35)
    print("Finding team name mismatches between odds and power ratings...")
    
    # Get team names from both sources
    odds_teams = get_team_names_from_odds()
    power_teams = get_team_names_from_power_ratings()
    
    if not odds_teams or not power_teams:
        print("❌ Could not retrieve team names")
        return
    
    # Find mismatches
    missing_in_power, missing_in_odds, matching = find_team_name_mismatches(odds_teams, power_teams)
    
    # Suggest mappings
    name_mappings = suggest_team_name_mapping(missing_in_power, power_teams)
    
    # Create fixes
    create_team_mapping_fix(name_mappings)
    
    # Test sample game
    test_sample_game()
    
    print(f"\n✅ DIAGNOSIS COMPLETE!")
    print(f"📊 Summary:")
    print(f"  🎯 {len(matching)} teams match perfectly")
    print(f"  🔧 {len(missing_in_power)} teams need mapping fixes")
    print(f"  💡 Use the suggested SQL updates or mapping dictionary")

if __name__ == "__main__":
    main()