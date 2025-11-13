#!/usr/bin/env python3
"""
Check what 2025 games are ACTUALLY in the database
"""

from sqlalchemy import create_engine, text
import pandas as pd

DATABASE_URL = "postgresql://postgres:YviqtXqcsCIgRzSCofNjbfwgjkYNLydX@maglev.proxy.rlwy.net:54187/railway"
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

print("🔍 CHECKING 2025 GAMES IN DATABASE")
print("=" * 70)

with engine.connect() as conn:
    
    # Check ALL 2025 games with scores
    all_2025 = pd.read_sql(text("""
        SELECT game_id, game_date, home_team, away_team, home_score, away_score
        FROM games
        WHERE game_date >= '2025-09-01'
        AND home_score IS NOT NULL
        ORDER BY game_date
    """), conn)
    
    print(f"\nTotal 2025 games with scores: {len(all_2025)}")
    print("\nALL 2025 completed games:")
    for _, g in all_2025.iterrows():
        print(f"  {g['game_date']}  {g['away_team']:6} @ {g['home_team']:6}  {g['away_score']:.0f}-{g['home_score']:.0f}")
    
    # Check Rams specifically
    print("\n" + "=" * 70)
    print("RAMS GAMES:")
    
    rams_games = pd.read_sql(text("""
        SELECT DISTINCT game_id, game_date, home_team, away_team, home_score, away_score
        FROM games
        WHERE (home_team = 'LAR' OR away_team = 'LAR')
        AND game_date >= '2025-09-01'
        AND home_score IS NOT NULL
        ORDER BY game_date
    """), conn)
    
    print(f"\nTotal Rams games: {len(rams_games)}")
    for _, g in rams_games.iterrows():
        if g['home_team'] == 'LAR':
            result = 'W' if g['home_score'] > g['away_score'] else ('L' if g['home_score'] < g['away_score'] else 'T')
            print(f"  {g['game_date']}  vs {g['away_team']:6}  {g['home_score']:.0f}-{g['away_score']:.0f}  {result}")
        else:
            result = 'W' if g['away_score'] > g['home_score'] else ('L' if g['away_score'] < g['home_score'] else 'T')
            print(f"  {g['game_date']}  @ {g['home_team']:6}  {g['away_score']:.0f}-{g['home_score']:.0f}  {result}")
    
    # Check for duplicates
    print("\n" + "=" * 70)
    print("CHECKING FOR DUPLICATE GAMES:")
    
    duplicates = pd.read_sql(text("""
        SELECT game_date, home_team, away_team, COUNT(*) as count
        FROM games
        WHERE game_date >= '2025-09-01'
        AND home_score IS NOT NULL
        GROUP BY game_date, home_team, away_team
        HAVING COUNT(*) > 1
    """), conn)
    
    if duplicates.empty:
        print("  ✅ No duplicates found")
    else:
        print(f"  ⚠️ Found {len(duplicates)} duplicate games:")
        for _, dup in duplicates.iterrows():
            print(f"    {dup['game_date']}  {dup['away_team']} @ {dup['home_team']}  (x{dup['count']})")

print("\n" + "=" * 70)