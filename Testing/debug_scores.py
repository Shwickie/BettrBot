#!/usr/bin/env python3
"""
Debug Score Updater - Shows exactly why matching fails
"""

import pandas as pd
import nfl_data_py as nfl
import sqlite3
from datetime import datetime, timedelta

DB_PATH = "E:/Bettr Bot/betting-bot/data/betting.db"

TEAM_MAPPINGS = {
    "ARI": "Arizona Cardinals", "ATL": "Atlanta Falcons", "BAL": "Baltimore Ravens", 
    "BUF": "Buffalo Bills", "CAR": "Carolina Panthers", "CHI": "Chicago Bears", 
    "CIN": "Cincinnati Bengals", "CLE": "Cleveland Browns", "DAL": "Dallas Cowboys", 
    "DEN": "Denver Broncos", "DET": "Detroit Lions", "GB": "Green Bay Packers",
    "HOU": "Houston Texans", "IND": "Indianapolis Colts", "JAX": "Jacksonville Jaguars", 
    "KC": "Kansas City Chiefs", "LAC": "Los Angeles Chargers", "LAR": "Los Angeles Rams", 
    "LV": "Las Vegas Raiders", "MIA": "Miami Dolphins", "MIN": "Minnesota Vikings", 
    "NE": "New England Patriots", "NO": "New Orleans Saints", "NYG": "New York Giants",
    "NYJ": "New York Jets", "PHI": "Philadelphia Eagles", "PIT": "Pittsburgh Steelers", 
    "SEA": "Seattle Seahawks", "SF": "San Francisco 49ers", "TB": "Tampa Bay Buccaneers", 
    "TEN": "Tennessee Titans", "WAS": "Washington Commanders"
}

def debug_team_matching():
    print("DEBUG: DETAILED TEAM MATCHING ANALYSIS")
    print("=" * 60)
    
    # Get NFL completed games
    nfl_df = nfl.import_schedules([2025])
    nfl_df['gameday'] = pd.to_datetime(nfl_df['gameday'])
    completed_games = nfl_df[
        (nfl_df['home_score'].notna()) & 
        (nfl_df['away_score'].notna())
    ].copy()
    
    print(f"NFL completed games: {len(completed_games)}")
    
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    
    # Get all database team names
    team_query = """
        SELECT DISTINCT home_team AS team FROM games 
        UNION 
        SELECT DISTINCT away_team AS team FROM games
    """
    db_teams = set(pd.read_sql(team_query, conn)['team'].tolist())
    
    print(f"Database teams: {len(db_teams)}")
    print(f"All DB teams: {sorted(list(db_teams))}")
    
    # Get games needing scores
    games_needing_scores = pd.read_sql("""
        SELECT game_id, game_date, away_team, home_team
        FROM games 
        WHERE home_score IS NULL 
        AND strftime('%Y', game_date) = '2025'
        ORDER BY game_date
        LIMIT 10
    """, conn)
    
    print(f"\nFirst 10 DB games needing scores:")
    for _, game in games_needing_scores.iterrows():
        print(f"  {game['game_date']}: {game['away_team']} @ {game['home_team']}")
    
    print(f"\nTesting each NFL completed game:")
    
    for i, (_, nfl_game) in enumerate(completed_games.iterrows()):
        if i >= 5:  # Only test first 5 games
            break
            
        nfl_away = nfl_game['away_team']
        nfl_home = nfl_game['home_team']
        nfl_date = nfl_game['gameday']
        
        print(f"\n--- NFL Game {i+1}: {nfl_away} @ {nfl_home} ({nfl_date.strftime('%Y-%m-%d')}) ---")
        
        # Check if teams exist in database
        away_in_db = nfl_away in db_teams
        home_in_db = nfl_home in db_teams
        
        print(f"Direct match - Away '{nfl_away}' in DB: {away_in_db}")
        print(f"Direct match - Home '{nfl_home}' in DB: {home_in_db}")
        
        # Try full name mapping
        away_full = TEAM_MAPPINGS.get(nfl_away, "")
        home_full = TEAM_MAPPINGS.get(nfl_home, "")
        
        away_full_in_db = away_full in db_teams if away_full else False
        home_full_in_db = home_full in db_teams if home_full else False
        
        print(f"Full name - Away '{away_full}' in DB: {away_full_in_db}")
        print(f"Full name - Home '{home_full}' in DB: {home_full_in_db}")
        
        # Find what we should use for matching
        db_away = None
        db_home = None
        
        if away_in_db:
            db_away = nfl_away
        elif away_full_in_db:
            db_away = away_full
            
        if home_in_db:
            db_home = nfl_home
        elif home_full_in_db:
            db_home = home_full
        
        print(f"Match result: {nfl_away} -> '{db_away}', {nfl_home} -> '{db_home}'")
        
        if db_away and db_home:
            # Look for this specific game in database
            start_date = (nfl_date - timedelta(days=3)).strftime('%Y-%m-%d')
            end_date = (nfl_date + timedelta(days=3)).strftime('%Y-%m-%d')
            
            match_query = """
                SELECT game_id, game_date, away_team, home_team
                FROM games 
                WHERE away_team = ? AND home_team = ?
                AND date(game_date) BETWEEN ? AND ?
                AND home_score IS NULL
            """
            
            match_result = pd.read_sql(match_query, conn, params=[db_away, db_home, start_date, end_date])
            
            print(f"Database search: {len(match_result)} matches found")
            if not match_result.empty:
                for _, match in match_result.iterrows():
                    print(f"  Found: {match['away_team']} @ {match['home_team']} ({match['game_date']})")
            
            # Try reverse (swapped home/away)
            if match_result.empty:
                reverse_match = pd.read_sql(match_query, conn, params=[db_home, db_away, start_date, end_date])
                print(f"Reverse search: {len(reverse_match)} matches found")
                if not reverse_match.empty:
                    for _, match in reverse_match.iterrows():
                        print(f"  Found (swapped): {match['away_team']} @ {match['home_team']} ({match['game_date']})")
        else:
            print("CANNOT MATCH - Missing team mapping")
    
    conn.close()

def show_specific_examples():
    print("\n" + "=" * 60)
    print("SPECIFIC EXAMPLE ANALYSIS")
    print("=" * 60)
    
    # Let's manually check one specific game
    conn = sqlite3.connect(DB_PATH)
    
    # Check for a specific completed NFL game in the database
    print("Looking for SF @ SEA game around 2025-09-07...")
    
    sf_variants = ["SF", "San Francisco 49ers", "SAN FRANCISCO", "49ers"]
    sea_variants = ["SEA", "Seattle Seahawks", "SEATTLE", "Seahawks"]
    
    for sf in sf_variants:
        for sea in sea_variants:
            check_query = """
                SELECT game_id, game_date, away_team, home_team, away_score, home_score
                FROM games 
                WHERE away_team = ? AND home_team = ?
                AND date(game_date) BETWEEN '2025-09-04' AND '2025-09-10'
            """
            
            result = pd.read_sql(check_query, conn, params=[sf, sea])
            if not result.empty:
                game = result.iloc[0]
                score_status = "HAS SCORE" if pd.notna(game['home_score']) else "NO SCORE"
                print(f"  FOUND: {sf} @ {sea} -> {game['away_team']} @ {game['home_team']} ({score_status})")
    
    # Also check reverse
    print("\nChecking reverse (SEA @ SF)...")
    for sf in sf_variants:
        for sea in sea_variants:
            result = pd.read_sql(check_query, conn, params=[sea, sf])
            if not result.empty:
                game = result.iloc[0]
                score_status = "HAS SCORE" if pd.notna(game['home_score']) else "NO SCORE"
                print(f"  FOUND: {sea} @ {sf} -> {game['away_team']} @ {game['home_team']} ({score_status})")
    
    conn.close()

if __name__ == "__main__":
    debug_team_matching()
    show_specific_examples()