#!/usr/bin/env python3
"""
Debug Team Records - Find why rankings show wrong game counts
This will help identify team name mismatches causing record calculation issues
"""

import pandas as pd
import sqlite3
import os
from sqlalchemy import create_engine, text
from datetime import datetime, date

# Database setup (same as your mobile_dashboard.py)
DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

USE_CLOUD_DB = bool(DATABASE_URL)
SEASON = 2025

# Your team mappings (from mobile_dashboard.py)
ABBR_TO_FULL = {
    'ARI': 'Arizona Cardinals','ATL': 'Atlanta Falcons','BAL': 'Baltimore Ravens','BUF': 'Buffalo Bills',
    'CAR': 'Carolina Panthers','CHI': 'Chicago Bears','CIN': 'Cincinnati Bengals','CLE': 'Cleveland Browns',
    'DAL': 'Dallas Cowboys','DEN': 'Denver Broncos','DET': 'Detroit Lions','GB': 'Green Bay Packers',
    'HOU': 'Houston Texans','IND': 'Indianapolis Colts','JAX': 'Jacksonville Jaguars','KC': 'Kansas City Chiefs',
    'LV': 'Las Vegas Raiders','LAC': 'Los Angeles Chargers','LA': 'Los Angeles Rams','LAR': 'Los Angeles Rams','MIA': 'Miami Dolphins',
    'MIN': 'Minnesota Vikings','NE': 'New England Patriots','NO': 'New Orleans Saints','NYG': 'New York Giants',
    'NYJ': 'New York Jets','PHI': 'Philadelphia Eagles','PIT': 'Pittsburgh Steelers','SF': 'San Francisco 49ers',
    'SEA': 'Seattle Seahawks','TB': 'Tampa Bay Buccaneers','TEN': 'Tennessee Titans','WAS': 'Washington Commanders'
}
FULL_NAMES = set(ABBR_TO_FULL.values())
FULL_TO_ABBR = {v: k for k, v in ABBR_TO_FULL.items()}

def to_full(x):
    if not x:
        return "Unknown"
    s = str(x).strip()
    if s in FULL_NAMES:
        return s
    return ABBR_TO_FULL.get(s.upper(), s)

def to_abbr(x):
    x = (x or '').strip()
    if not x:
        return ''
    if x in FULL_TO_ABBR:
        return FULL_TO_ABBR[x]
    return x.upper()

def get_engine():
    if USE_CLOUD_DB:
        return create_engine(DATABASE_URL, pool_pre_ping=True)
    else:
        local_db = r"E:/Bettr Bot/betting-bot/data/betting.db"
        return create_engine(f"sqlite:///{local_db}")

def debug_team_records():
    """Debug why team records are showing wrong game counts"""
    print("DEBUGGING TEAM RECORDS")
    print("=" * 50)
    
    engine = get_engine()
    
    # Get all games with scores for current season
    if USE_CLOUD_DB:
        with engine.connect() as conn:
            games = pd.read_sql_query(text("""
                WITH g AS (
                    SELECT
                        game_id, home_team, away_team,
                        home_score, away_score, week, id, game_date,
                        CASE
                          WHEN EXTRACT(MONTH FROM game_date) >= 8
                            THEN EXTRACT(YEAR FROM game_date)::int
                          ELSE (EXTRACT(YEAR FROM game_date)::int) - 1
                        END AS season_year
                    FROM games
                )
                SELECT game_id, home_team, away_team, home_score, away_score, week, id, game_date
                FROM g
                WHERE season_year = :season
                  AND home_score IS NOT NULL AND away_score IS NOT NULL
                ORDER BY game_date
            """), conn, params={"season": SEASON})
    else:
        # For SQLite, use raw connection to avoid SQLAlchemy parameter issues
        import sqlite3
        local_db = r"E:/Bettr Bot/betting-bot/data/betting.db"
        conn = sqlite3.connect(local_db)
        games = pd.read_sql_query("""
            WITH g AS (
                SELECT
                    game_id, home_team, away_team,
                    home_score, away_score, week, id, game_date,
                    CASE
                      WHEN CAST(strftime('%m', game_date) AS INT) >= 8
                        THEN CAST(strftime('%Y', game_date) AS INT)
                      ELSE CAST(strftime('%Y', game_date) AS INT) - 1
                    END AS season_year
                FROM games
            )
            SELECT game_id, home_team, away_team, home_score, away_score, week, id, game_date
            FROM g
            WHERE season_year = ?
              AND home_score IS NOT NULL AND away_score IS NOT NULL
            ORDER BY game_date
        """, conn, params=[SEASON])
        conn.close()
    
    print(f"Found {len(games)} completed games for {SEASON} season")
    
    if games.empty:
        print("No completed games found!")
        return
    
    # Show sample of raw data
    print("\nSample raw games from database:")
    for _, g in games.head(10).iterrows():
        print(f"  {g['game_date']}: {g['away_team']} @ {g['home_team']} ({g['away_score']}-{g['home_score']})")
    
    # Check team name variations
    print(f"\nUnique team names in database:")
    all_teams = set(games['home_team'].unique()) | set(games['away_team'].unique())
    for team in sorted(all_teams):
        full_name = to_full(team)
        abbr = to_abbr(full_name)
        print(f"  '{team}' -> Full: '{full_name}' -> Abbr: '{abbr}'")
    
    # Now replicate the exact logic from compute_live_records
    print(f"\n" + "="*50)
    print("REPLICATING compute_live_records LOGIC")
    print("="*50)
    
    # Apply the same transformations as your function
    games["home_team"] = games["home_team"].map(to_full)
    games["away_team"] = games["away_team"].map(to_full)
    
    print(f"\nAfter to_full() mapping:")
    print(f"Unique home teams: {sorted(games['home_team'].unique())}")
    print(f"Unique away teams: {sorted(games['away_team'].unique())}")
    
    # Check for duplicate game_ids (this could cause issues)
    games["game_id"] = games["game_id"].fillna("").astype(str).str.strip()
    games["gid_fallback"] = (
        pd.to_datetime(games["game_date"]).dt.strftime("%Y%m%d") + "_" +
        games["away_team"].map(to_abbr) + "_" +
        games["home_team"].map(to_abbr)
    )
    games["gid"] = games["game_id"].where(games["game_id"] != "", games["gid_fallback"])
    
    print(f"\nGame ID analysis:")
    print(f"  Original game_ids: {games['game_id'].nunique()} unique")
    print(f"  After deduplication: {games['gid'].nunique()} unique")
    
    # Check for duplicates
    duplicates = games[games.duplicated('gid', keep=False)]
    if not duplicates.empty:
        print(f"\nWARNING: Found {len(duplicates)} duplicate games:")
        for _, dup in duplicates.iterrows():
            print(f"  {dup['gid']}: {dup['away_team']} @ {dup['home_team']} on {dup['game_date']}")
    
    # Apply deduplication (same as your function)
    keep_col = "updated_at" if "updated_at" in games.columns else ("id" if "id" in games.columns else None)
    if keep_col:
        games = games.sort_values(keep_col).drop_duplicates("gid", keep="last")
    else:
        games = games.drop_duplicates("gid", keep="last")
    
    print(f"After deduplication: {len(games)} games")
    
    # Calculate win/loss for each team
    games["home_win"] = (games["home_score"] > games["away_score"]).astype(int)
    games["away_win"] = (games["away_score"] > games["home_score"]).astype(int)
    games["tie"] = (games["home_score"] == games["away_score"]).astype(int)
    
    # Focus on Pittsburgh Steelers (your example)
    steelers_games = games[(games['home_team'] == 'Pittsburgh Steelers') | 
                          (games['away_team'] == 'Pittsburgh Steelers')]
    
    print(f"\n" + "="*50)
    print("PITTSBURGH STEELERS ANALYSIS")
    print("="*50)
    print(f"Found {len(steelers_games)} Steelers games:")
    
    steelers_wins = 0
    steelers_losses = 0
    steelers_ties = 0
    
    for _, game in steelers_games.iterrows():
        if game['home_team'] == 'Pittsburgh Steelers':
            # Steelers at home
            if game['home_score'] > game['away_score']:
                result = "WIN"
                steelers_wins += 1
            elif game['home_score'] < game['away_score']:
                result = "LOSS"
                steelers_losses += 1
            else:
                result = "TIE"
                steelers_ties += 1
            print(f"  {game['game_date']}: vs {game['away_team']} {game['home_score']}-{game['away_score']} ({result})")
        else:
            # Steelers away
            if game['away_score'] > game['home_score']:
                result = "WIN"
                steelers_wins += 1
            elif game['away_score'] < game['home_score']:
                result = "LOSS"
                steelers_losses += 1
            else:
                result = "TIE"
                steelers_ties += 1
            print(f"  {game['game_date']}: @ {game['home_team']} {game['away_score']}-{game['home_score']} ({result})")
    
    print(f"\nSteelers Manual Count: {steelers_wins}-{steelers_losses}-{steelers_ties}")
    
    # Now check what your groupby logic produces
    home_stats = games[games['home_team'] == 'Pittsburgh Steelers'].agg(
        wins=("home_win", "sum"),
        losses=("away_win", "sum"), 
        ties=("tie", "sum"),
        games=("home_win", "size")
    )
    
    away_stats = games[games['away_team'] == 'Pittsburgh Steelers'].agg(
        wins=("away_win", "sum"),
        losses=("home_win", "sum"),
        ties=("tie", "sum"), 
        games=("away_win", "size")
    )
    
    print(f"\nGroupby logic results:")
    print(f"  Home games: {home_stats['wins']}-{home_stats['losses']}-{home_stats['ties']} ({home_stats['games']} games)")
    print(f"  Away games: {away_stats['wins']}-{away_stats['losses']}-{away_stats['ties']} ({away_stats['games']} games)")
    print(f"  Combined: {home_stats['wins'] + away_stats['wins']}-{home_stats['losses'] + away_stats['losses']}-{home_stats['ties'] + away_stats['ties']} ({home_stats['games'] + away_stats['games']} games)")
    
    # Check if there are team name mismatches
    print(f"\n" + "="*50)
    print("TEAM NAME CONSISTENCY CHECK")
    print("="*50)
    
    # Check how many different ways "Pittsburgh Steelers" appears
    steelers_variations = []
    for team_col in ['home_team', 'away_team']:
        variations = games[team_col].unique()
        steelers_vars = [v for v in variations if 'steelers' in v.lower() or 'pittsburgh' in v.lower() or 'pit' in v.lower()]
        steelers_variations.extend(steelers_vars)
    
    steelers_variations = list(set(steelers_variations))
    print(f"Steelers name variations found: {steelers_variations}")
    
    if len(steelers_variations) > 1:
        print("WARNING: Multiple team name variations found!")
        print("This could be causing the record calculation issues.")
        
        for variation in steelers_variations:
            count = len(games[(games['home_team'] == variation) | (games['away_team'] == variation)])
            print(f"  '{variation}': {count} games")

def main():
    debug_team_records()

if __name__ == "__main__":
    main()