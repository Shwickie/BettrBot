# debug_ai_issues.py - Quick diagnostic script to find the root problems

import sqlite3
import pandas as pd
import os

# Use your local database path
DB_PATH = r"E:\Bettr Bot\betting-bot\data\betting.db"

def diagnose_ai_issues():
    """Diagnose why AI chat and odds aren't working"""
    
    conn = sqlite3.connect(DB_PATH)
    
    print("=== BETTR BOT AI DIAGNOSTICS ===\n")
    
    # 1. Check if we have games
    print("1. CHECKING GAMES TABLE:")
    games = pd.read_sql_query("""
        SELECT COUNT(*) as total,
               MIN(game_date) as earliest_date,
               MAX(game_date) as latest_date
        FROM games
    """, conn)
    print(f"   Total games: {games.iloc[0]['total']}")
    print(f"   Date range: {games.iloc[0]['earliest_date']} to {games.iloc[0]['latest_date']}")
    
    # Check upcoming games
    upcoming = pd.read_sql_query("""
        SELECT game_id, home_team, away_team, game_date 
        FROM games 
        WHERE date(game_date) >= date('now')
        ORDER BY game_date 
        LIMIT 5
    """, conn)
    print(f"   Upcoming games: {len(upcoming)}")
    if not upcoming.empty:
        for _, game in upcoming.iterrows():
            print(f"     {game['away_team']} @ {game['home_team']} on {game['game_date']}")
    
    print("\n2. CHECKING ODDS TABLE:")
    
    # Check if odds table exists and has data
    try:
        odds_count = pd.read_sql_query("SELECT COUNT(*) as count FROM odds", conn)
        print(f"   Total odds records: {odds_count.iloc[0]['count']}")
        
        # Check recent odds
        recent_odds = pd.read_sql_query("""
            SELECT COUNT(*) as count,
                   MIN(timestamp) as earliest,
                   MAX(timestamp) as latest
            FROM odds 
            WHERE timestamp >= datetime('now', '-7 days')
        """, conn)
        print(f"   Recent odds (7 days): {recent_odds.iloc[0]['count']}")
        print(f"   Time range: {recent_odds.iloc[0]['earliest']} to {recent_odds.iloc[0]['latest']}")
        
        # Check sportsbooks
        books = pd.read_sql_query("""
            SELECT sportsbook, COUNT(*) as count 
            FROM odds 
            WHERE timestamp >= datetime('now', '-7 days')
            GROUP BY sportsbook
            ORDER BY count DESC
        """, conn)
        print(f"   Active sportsbooks: {len(books)}")
        for _, book in books.iterrows():
            print(f"     {book['sportsbook']}: {book['count']} lines")
        
        # Check team names in odds vs games
        print("\n3. CHECKING TEAM NAME MISMATCHES:")
        
        odds_teams = pd.read_sql_query("""
            SELECT DISTINCT team FROM odds 
            WHERE timestamp >= datetime('now', '-7 days')
            ORDER BY team
        """, conn)
        
        game_teams = pd.read_sql_query("""
            SELECT DISTINCT home_team as team FROM games
            WHERE date(game_date) >= date('now')
            UNION
            SELECT DISTINCT away_team as team FROM games  
            WHERE date(game_date) >= date('now')
            ORDER BY team
        """, conn)
        
        odds_team_set = set(odds_teams['team'].tolist())
        game_team_set = set(game_teams['team'].tolist())
        
        print(f"   Teams in odds: {len(odds_team_set)}")
        print(f"   Teams in games: {len(game_team_set)}")
        
        # Find mismatches
        in_games_not_odds = game_team_set - odds_team_set
        in_odds_not_games = odds_team_set - game_team_set
        
        if in_games_not_odds:
            print(f"   ❌ In games but NOT in odds: {list(in_games_not_odds)[:5]}")
        
        if in_odds_not_games:
            print(f"   ❌ In odds but NOT in games: {list(in_odds_not_games)[:5]}")
        
        matches = odds_team_set.intersection(game_team_set)
        print(f"   ✅ Matching teams: {len(matches)}")
        
    except Exception as e:
        print(f"   ❌ Error checking odds: {e}")
    
    print("\n4. CHECKING AI CHAT DEPENDENCIES:")
    
    # Check if model files exist
    model_candidates = [
        "betting_model_fixed.pkl",
        os.path.join(os.getcwd(), "betting_model_fixed.pkl"),
        os.path.join("models", "betting_model_fixed.pkl"),
    ]
    
    model_found = False
    for path in model_candidates:
        if os.path.exists(path):
            print(f"   ✅ Model found: {path}")
            model_found = True
            break
    
    if not model_found:
        print(f"   ❌ Model NOT found in: {model_candidates}")
    
    # Check team_season_summary table
    try:
        tss = pd.read_sql_query("""
            SELECT COUNT(*) as count, 
                   MIN(season) as min_season,
                   MAX(season) as max_season
            FROM team_season_summary
        """, conn)
        print(f"   Team season summary: {tss.iloc[0]['count']} records")
        print(f"   Seasons: {tss.iloc[0]['min_season']} to {tss.iloc[0]['max_season']}")
        
        # Check current season data
        current_data = pd.read_sql_query("""
            SELECT team, power_score, win_pct, games_played
            FROM team_season_summary 
            WHERE season = 2024
            LIMIT 5
        """, conn)
        print(f"   2024 season teams: {len(current_data)}")
        if not current_data.empty:
            for _, team in current_data.iterrows():
                print(f"     {team['team']}: {team['games_played']} games, {team['power_score']:.1f} power")
        
    except Exception as e:
        print(f"   ❌ Error checking team_season_summary: {e}")
    
    conn.close()
    
    print("\n=== RECOMMENDATIONS ===")
    print("1. If no odds showing: Run odds fetcher or check team name mappings")
    print("2. If AI not working: Check if model file exists and team names match")
    print("3. If games missing: Run schedule update")
    print("4. Check cloud_run_all.py to ensure all data pipelines ran successfully")

if __name__ == "__main__":
    diagnose_ai_issues()