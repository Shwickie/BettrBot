#!/usr/bin/env python3
"""
Check what games actually exist in the database and what dates they're for
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta

DB_PATH = r"E:\Bettr Bot\betting-bot\data\betting.db"

def check_all_games():
    """Check what games exist in the database"""
    conn = sqlite3.connect(DB_PATH)
    try:
        # Get overview of all games
        overview = pd.read_sql_query("""
            SELECT 
                MIN(game_date) as earliest_game,
                MAX(game_date) as latest_game,
                COUNT(*) as total_games,
                COUNT(CASE WHEN home_score IS NOT NULL THEN 1 END) as games_with_scores,
                COUNT(CASE WHEN home_score IS NULL THEN 1 END) as games_without_scores
            FROM games
        """, conn)
        
        print("DATABASE OVERVIEW:")
        print("=" * 50)
        if not overview.empty:
            row = overview.iloc[0]
            print(f"Total games: {row['total_games']}")
            print(f"Date range: {row['earliest_game']} to {row['latest_game']}")
            print(f"Games with scores: {row['games_with_scores']}")
            print(f"Games without scores: {row['games_without_scores']}")
        
        # Check games by date - especially around today
        today = datetime.now().date()
        recent_window = today - timedelta(days=7)
        future_window = today + timedelta(days=7)
        
        print(f"\nGAMES AROUND TODAY ({today}):")
        print("=" * 50)
        
        recent_games = pd.read_sql_query("""
            SELECT game_date, home_team, away_team, home_score, away_score,
                   CASE WHEN home_score IS NULL THEN 'No Score' ELSE 'Has Score' END as status
            FROM games 
            WHERE date(game_date) BETWEEN date(?) AND date(?)
            ORDER BY game_date DESC
        """, conn, params=[recent_window.isoformat(), future_window.isoformat()])
        
        if recent_games.empty:
            print("No games found around today's date")
        else:
            print(f"Found {len(recent_games)} games in the past/next 7 days:")
            for _, game in recent_games.iterrows():
                score_info = f"{game['away_score']}-{game['home_score']}" if game['status'] == 'Has Score' else "No Score"
                print(f"  {game['game_date']}: {game['away_team']} @ {game['home_team']} ({score_info})")
        
        # Check what the most recent completed games are
        print(f"\nMOST RECENT COMPLETED GAMES:")
        print("=" * 50)
        
        completed_games = pd.read_sql_query("""
            SELECT game_date, home_team, away_team, home_score, away_score
            FROM games 
            WHERE home_score IS NOT NULL AND away_score IS NOT NULL
            ORDER BY game_date DESC
            LIMIT 10
        """, conn)
        
        if completed_games.empty:
            print("No completed games found in database at all")
        else:
            print(f"Most recent {len(completed_games)} completed games:")
            for _, game in completed_games.iterrows():
                print(f"  {game['game_date']}: {game['away_team']} {game['away_score']} @ {game['home_team']} {game['home_score']}")
        
        # Check what the most recent scheduled games are (without scores)
        print(f"\nMOST RECENT SCHEDULED GAMES (No Scores):")
        print("=" * 50)
        
        scheduled_games = pd.read_sql_query("""
            SELECT game_date, home_team, away_team
            FROM games 
            WHERE home_score IS NULL
            ORDER BY game_date DESC
            LIMIT 10
        """, conn)
        
        if not scheduled_games.empty:
            print(f"Most recent {len(scheduled_games)} games without scores:")
            for _, game in scheduled_games.iterrows():
                print(f"  {game['game_date']}: {game['away_team']} @ {game['home_team']}")
                
    finally:
        conn.close()

def check_what_season_we_are():
    """Figure out what NFL season this actually is"""
    today = datetime.now()
    print(f"\nSEASON DETECTION:")
    print("=" * 50)
    print(f"Today's date: {today.strftime('%Y-%m-%d')}")
    print(f"Month: {today.month}")
    
    # NFL season logic
    if today.month >= 8:  # August or later = current year's season
        current_season = today.year
        print(f"Current NFL season: {current_season} (season runs Aug {current_season} - Feb {current_season + 1})")
    else:  # Before August = previous year's season
        current_season = today.year - 1
        print(f"Current NFL season: {current_season} (season runs Aug {current_season} - Feb {current_season + 1})")
    
    return current_season

def main():
    print("CHECKING WHAT GAMES ACTUALLY EXIST")
    print("=" * 50)
    
    check_all_games()
    current_season = check_what_season_we_are()
    
    print(f"\nDIAGNOSIS:")
    print("=" * 50)
    print("Based on the output above, the issue is likely:")
    print("1. Games that 'just played' are actually in the future in your database")
    print("2. The score fetching API doesn't have the latest results yet") 
    print("3. Your database has games from a different season than expected")
    print("4. The team name mapping is wrong between API and database")
    
    print(f"\nIf the games that 'just played' show as future dates above,")
    print(f"that means your database has incorrect game dates.")

if __name__ == "__main__":
    main()