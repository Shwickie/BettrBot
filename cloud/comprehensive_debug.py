# comprehensive_debug_script.py - Diagnose odds and AI chat issues

import sqlite3
import pandas as pd
import os
from datetime import datetime, timedelta

# Use your database path
DB_PATH = r"E:\Bettr Bot\betting-bot\data\betting.db"

def diagnose_comprehensive_issues():
    """Complete diagnostic of odds display and AI chat issues"""
    
    print("=" * 60)
    print("🔍 COMPREHENSIVE BETTR BOT DIAGNOSTICS")
    print("=" * 60)
    
    conn = sqlite3.connect(DB_PATH)
    
    # 1. Check odds table post-fix
    print("\n1. ODDS TABLE STATUS (POST-FIX):")
    
    odds_check = pd.read_sql_query("""
        SELECT COUNT(*) as total_odds,
               COUNT(DISTINCT team) as unique_teams,
               MIN(timestamp) as earliest_odds,
               MAX(timestamp) as latest_odds
        FROM odds
    """, conn)
    
    print(f"   Total odds records: {odds_check.iloc[0]['total_odds']}")
    print(f"   Unique teams: {odds_check.iloc[0]['unique_teams']}")
    print(f"   Date range: {odds_check.iloc[0]['earliest_odds']} to {odds_check.iloc[0]['latest_odds']}")
    
    # Check team names in odds (should all be abbreviations now)
    team_names = pd.read_sql_query("""
        SELECT team, COUNT(*) as count 
        FROM odds 
        GROUP BY team 
        ORDER BY team
    """, conn)
    
    print(f"\n   Teams in odds table:")
    for _, row in team_names.iterrows():
        print(f"     {row['team']}: {row['count']} records")
    
    # 2. Check games table
    print("\n\n2. GAMES TABLE STATUS:")
    
    today = datetime.now().date()
    future_date = today + timedelta(days=7)
    
    games_check = pd.read_sql_query("""
        SELECT COUNT(*) as total_games,
               MIN(game_date) as earliest_game,
               MAX(game_date) as latest_game
        FROM games
    """, conn)
    
    print(f"   Total games: {games_check.iloc[0]['total_games']}")
    print(f"   Date range: {games_check.iloc[0]['earliest_game']} to {games_check.iloc[0]['latest_game']}")
    
    # Check upcoming games
    upcoming_games = pd.read_sql_query("""
        SELECT game_id, away_team, home_team, game_date
        FROM games 
        WHERE date(game_date) >= date('now')
        ORDER BY game_date
        LIMIT 5
    """, conn)
    
    print(f"\n   Upcoming games ({len(upcoming_games)}):")
    for _, game in upcoming_games.iterrows():
        print(f"     {game['away_team']} @ {game['home_team']} on {game['game_date']}")
    
    # 3. Check game-odds matching
    print("\n\n3. GAME-ODDS MATCHING:")
    
    if not upcoming_games.empty:
        sample_game = upcoming_games.iloc[0]
        print(f"\n   Testing game: {sample_game['away_team']} @ {sample_game['home_team']}")
        print(f"   Game ID: {sample_game['game_id']}")
        
        # Check if odds exist for this game
        game_odds = pd.read_sql_query("""
            SELECT team, sportsbook, odds, market, timestamp
            FROM odds
            WHERE game_id = ?
            ORDER BY timestamp DESC
            LIMIT 10
        """, conn, params=[sample_game['game_id']])
        
        print(f"   Odds for this game: {len(game_odds)} records")
        
        if not game_odds.empty:
            print("   Sample odds:")
            for _, odds in game_odds.iterrows():
                print(f"     {odds['team']}: {odds['odds']} @ {odds['sportsbook']}")
        else:
            print("   ❌ NO ODDS FOUND for this game")
            
            # Check if odds exist for these teams at all
            team_odds_check = pd.read_sql_query("""
                SELECT team, COUNT(*) as count
                FROM odds
                WHERE team IN (?, ?)
                GROUP BY team
            """, conn, params=[sample_game['away_team'], sample_game['home_team']])
            
            print(f"   Team odds availability:")
            for _, team_odds in team_odds_check.iterrows():
                print(f"     {team_odds['team']}: {team_odds['count']} total odds")
    
    # 4. Check AI model dependencies
    print("\n\n4. AI MODEL DEPENDENCIES:")
    
    # Check if model file exists
    model_paths = [
        r"E:\Bettr Bot\betting-bot\models\betting_model.pkl",
        r"E:\Bettr Bot\betting-bot\betting_model_fixed.pkl",
        "betting_model_fixed.pkl"
    ]
    
    model_found = False
    for path in model_paths:
        if os.path.exists(path):
            print(f"   ✅ Model found: {path}")
            print(f"   File size: {os.path.getsize(path)} bytes")
            model_found = True
            break
    
    if not model_found:
        print(f"   ❌ Model NOT found in any of: {model_paths}")
    
    # Check team_season_summary table
    try:
        tss_check = pd.read_sql_query("""
            SELECT COUNT(*) as count,
                   MIN(season) as min_season,
                   MAX(season) as max_season
            FROM team_season_summary
        """, conn)
        
        print(f"   Team season summary: {tss_check.iloc[0]['count']} records")
        print(f"   Seasons: {tss_check.iloc[0]['min_season']} to {tss_check.iloc[0]['max_season']}")
        
        # Check current season data
        current_season_data = pd.read_sql_query("""
            SELECT team, power_score, games_played
            FROM team_season_summary 
            WHERE season = 2025
            LIMIT 5
        """, conn)
        
        print(f"   2025 season teams: {len(current_season_data)}")
        for _, team in current_season_data.iterrows():
            print(f"     {team['team']}: {team['games_played']} games, {team['power_score']:.1f} power")
        
    except Exception as e:
        print(f"   ❌ Error checking team_season_summary: {e}")
    
    # 5. Check the specific function causing issues
    print("\n\n5. FUNCTION ERROR ANALYSIS:")
    print("   Looking for 'normalize_team_for_odds_lookup' and 'full_team_name' errors...")
    
    # These errors suggest missing functions in mobile_dashboard.py
    print("   ❌ Error: 'normalize_team_for_odds_lookup' is not defined")
    print("   ❌ Error: 'full_team_name' is not defined")
    print("   🔧 SOLUTION: Add missing helper functions to mobile_dashboard.py")
    
    conn.close()
    
    print("\n\n" + "=" * 60)
    print("🎯 RECOMMENDED FIXES:")
    print("=" * 60)
    print("1. Add missing functions to mobile_dashboard.py:")
    print("   - normalize_team_for_odds_lookup()")
    print("   - Ensure all team name normalization is consistent")
    print("\n2. Check game_id matching between games and odds tables")
    print("\n3. Verify model file exists and is accessible")
    print("\n4. Test API endpoints individually")
    print("\n5. Check console for JavaScript errors in browser")

if __name__ == "__main__":
    diagnose_comprehensive_issues()