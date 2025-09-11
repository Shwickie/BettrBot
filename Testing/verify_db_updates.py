#!/usr/bin/env python3
"""
Verify Database Updates - Check if scores and rankings actually updated
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta

DB_PATH = r"E:\Bettr Bot\betting-bot\data\betting.db"

def check_recent_scores():
    """Check if recent games now have scores"""
    conn = sqlite3.connect(DB_PATH)
    try:
        cutoff = (datetime.now() - timedelta(days=14)).date()
        
        games = pd.read_sql_query("""
            SELECT game_date, home_team, away_team, home_score, away_score
            FROM games 
            WHERE date(game_date) >= date(?)
            AND home_score IS NOT NULL 
            AND away_score IS NOT NULL
            ORDER BY game_date DESC
            LIMIT 10
        """, conn, params=[cutoff.isoformat()])
        
        print("RECENT GAMES WITH SCORES:")
        print("=" * 50)
        if games.empty:
            print("NO RECENT GAMES HAVE SCORES - This is the problem!")
            return False
        else:
            print(f"Found {len(games)} recent games with scores:")
            for _, game in games.iterrows():
                print(f"  {game['game_date']}: {game['away_team']} {game['away_score']} @ {game['home_team']} {game['home_score']}")
            return True
            
    finally:
        conn.close()

def check_team_rankings():
    """Check current team rankings"""
    conn = sqlite3.connect(DB_PATH)
    try:
        current_season = datetime.now().year
        if datetime.now().month < 8:
            current_season -= 1
            
        rankings = pd.read_sql_query("""
            SELECT team, wins, losses, games_played, win_pct, power_score
            FROM team_season_summary 
            WHERE season = ?
            ORDER BY power_score DESC
            LIMIT 10
        """, conn, params=[current_season])
        
        print(f"\nCURRENT TEAM RANKINGS ({current_season} season):")
        print("=" * 50)
        if rankings.empty:
            print("NO TEAM RANKINGS FOUND - team_season_summary is empty!")
            return False
        else:
            print("Top 10 teams:")
            for i, team in rankings.iterrows():
                record = f"{team['wins']}-{team['losses']}" if team['games_played'] > 0 else "0-0"
                print(f"  {i+1:2}. {team['team']:3} {record:5} ({team['games_played']} games) Power: {team['power_score']:5.1f}")
            return True
            
    finally:
        conn.close()

def check_power_cache_in_dashboard():
    """Check if the dashboard power cache might be stale"""
    print(f"\nDASHBOARD CACHE CHECK:")
    print("=" * 50)
    print("The dashboard has a power cache with 60-second TTL.")
    print("If you just updated the database, the cache might be stale.")
    print("\nTo force refresh:")
    print("1. Restart your dashboard server")
    print("2. Or wait 60 seconds and refresh the page")
    print("3. Or clear browser cache")

def check_predictions_date_range():
    """Check what date range the predictions API is looking at"""
    today = datetime.now().date()
    horizon = today + timedelta(days=21)
    
    print(f"\nPREDICTIONS DATE RANGE:")
    print("=" * 50)
    print(f"The /api/predictions endpoint looks for games between:")
    print(f"  Start: {today}")
    print(f"  End:   {horizon}")
    
    conn = sqlite3.connect(DB_PATH)
    try:
        games_in_range = pd.read_sql_query("""
            SELECT COUNT(*) as game_count
            FROM games 
            WHERE date(game_date) BETWEEN date(?) AND date(?)
        """, conn, params=[today.isoformat(), horizon.isoformat()])
        
        count = games_in_range.iloc[0]['game_count'] if not games_in_range.empty else 0
        print(f"  Games in this range: {count}")
        
        if count == 0:
            print("  NO GAMES IN PREDICTION RANGE - This explains empty predictions!")
            
            # Check what games exist
            all_games = pd.read_sql_query("""
                SELECT MIN(game_date) as earliest, MAX(game_date) as latest, COUNT(*) as total
                FROM games
            """, conn)
            
            if not all_games.empty and all_games.iloc[0]['total'] > 0:
                earliest = all_games.iloc[0]['earliest']
                latest = all_games.iloc[0]['latest']
                total = all_games.iloc[0]['total']
                print(f"  Database has {total} games from {earliest} to {latest}")
                
    finally:
        conn.close()

def main():
    print("VERIFYING DATABASE UPDATES")
    print("=" * 50)
    
    # Check if scores updated
    scores_ok = check_recent_scores()
    
    # Check if rankings updated  
    rankings_ok = check_team_rankings()
    
    # Check cache issues
    check_power_cache_in_dashboard()
    
    # Check prediction date range
    check_predictions_date_range()
    
    print(f"\nDIAGNOSIS:")
    print("=" * 50)
    
    if not scores_ok:
        print("PROBLEM: Recent games still don't have scores")
        print("SOLUTION: The score update didn't work. Try:")
        print("  python update_current_scores.py")
        
    elif not rankings_ok:
        print("PROBLEM: Team rankings weren't calculated")
        print("SOLUTION: Try running:")
        print('  python "E:\\Bettr Bot\\betting-bot\\stats\\team_season_summary.py"')
        
    else:
        print("DATABASE LOOKS GOOD - The issue might be:")
        print("1. Dashboard cache (restart server)")
        print("2. Browser cache (hard refresh)")  
        print("3. Date range issue (no upcoming games)")
        print("4. API endpoint bug")
        
        print(f"\nTo test the API directly, try:")
        print(f"  http://localhost:5000/api/rankings")
        print(f"  http://localhost:5000/api/predictions")

if __name__ == "__main__":
    main()