#!/usr/bin/env python3
"""
Debug why Eagles/other teams show double the games
"""

from sqlalchemy import create_engine, text
import pandas as pd

DATABASE_URL = "postgresql://postgres:YviqtXqcsCIgRzSCofNjbfwgjkYNLydX@maglev.proxy.rlwy.net:54187/railway"

def debug_duplicates():
    print("DEBUGGING DUPLICATE GAME COUNT")
    print("=" * 60)
    
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    
    with engine.connect() as conn:
        # Check actual Eagles games in database
        print("\n1. ACTUAL EAGLES GAMES IN DATABASE:")
        actual_games = pd.read_sql(text("""
            SELECT 
                game_id,
                game_date,
                home_team,
                away_team,
                home_score,
                away_score,
                CASE 
                    WHEN home_team ILIKE '%eagle%' OR home_team = 'PHI' THEN 'HOME'
                    WHEN away_team ILIKE '%eagle%' OR away_team = 'PHI' THEN 'AWAY'
                END as eagles_location
            FROM games
            WHERE (home_team ILIKE '%eagle%' OR home_team = 'PHI' 
                   OR away_team ILIKE '%eagle%' OR away_team = 'PHI')
            AND home_score IS NOT NULL
            AND EXTRACT(YEAR FROM game_date) = 2025
            ORDER BY game_date
        """), conn)
        
        print(f"   Total Eagles games: {len(actual_games)}")
        print("\n   Game details:")
        for _, game in actual_games.iterrows():
            print(f"   {game['game_date'].strftime('%Y-%m-%d')}: {game['away_team']} @ {game['home_team']} ({game['away_score']}-{game['home_score']})")
        
        # Check what the normalized CTE produces
        print("\n\n2. WHAT THE NORMALIZED CTE PRODUCES:")
        normalized = pd.read_sql(text("""
            WITH normalized_games AS (
                SELECT DISTINCT
                    game_id,
                    CASE 
                        WHEN home_team ILIKE '%eagle%' OR home_team = 'PHI' 
                        THEN 'Philadelphia Eagles'
                        ELSE home_team
                    END as home_team,
                    CASE 
                        WHEN away_team ILIKE '%eagle%' OR away_team = 'PHI' 
                        THEN 'Philadelphia Eagles'
                        ELSE away_team
                    END as away_team,
                    home_score,
                    away_score
                FROM games
                WHERE home_score IS NOT NULL 
                AND EXTRACT(YEAR FROM game_date) = 2025
            )
            SELECT * FROM normalized_games
            WHERE home_team = 'Philadelphia Eagles' OR away_team = 'Philadelphia Eagles'
            ORDER BY game_id
        """), conn)
        
        print(f"   CTE produces: {len(normalized)} rows")
        if len(normalized) > 4:
            print("   ⚠️ WARNING: CTE is creating duplicates!")
        
        # Check the UNION ALL output
        print("\n\n3. WHAT THE UNION ALL PRODUCES:")
        union_output = pd.read_sql(text("""
            WITH normalized_games AS (
                SELECT DISTINCT
                    game_id,
                    CASE 
                        WHEN home_team ILIKE '%eagle%' OR home_team = 'PHI' 
                        THEN 'Philadelphia Eagles'
                        ELSE home_team
                    END as home_team,
                    CASE 
                        WHEN away_team ILIKE '%eagle%' OR away_team = 'PHI' 
                        THEN 'Philadelphia Eagles'
                        ELSE away_team
                    END as away_team,
                    home_score,
                    away_score
                FROM games
                WHERE home_score IS NOT NULL 
                AND EXTRACT(YEAR FROM game_date) = 2025
            )
            SELECT * FROM (
                SELECT DISTINCT ON (game_id, home_team)
                    game_id,
                    home_team as team,
                    'HOME' as location
                FROM normalized_games
                WHERE home_team = 'Philadelphia Eagles'
                
                UNION ALL
                
                SELECT DISTINCT ON (game_id, away_team)
                    game_id,
                    away_team as team,
                    'AWAY' as location
                FROM normalized_games
                WHERE away_team = 'Philadelphia Eagles'
            ) x
            ORDER BY game_id
        """), conn)
        
        print(f"   UNION ALL produces: {len(union_output)} rows for Eagles")
        print("   Breakdown:")
        for _, row in union_output.iterrows():
            print(f"   {row['game_id']}: {row['location']}")
        
        # Check for actual duplicates
        duplicate_game_ids = union_output.groupby('game_id').size()
        duplicates = duplicate_game_ids[duplicate_game_ids > 1]
        
        if len(duplicates) > 0:
            print(f"\n   ❌ PROBLEM: {len(duplicates)} game_ids appear multiple times!")
            for gid, count in duplicates.items():
                print(f"      {gid}: appears {count} times")
        
        print("\n" + "=" * 60)
        print("DIAGNOSIS:")
        if len(actual_games) == 4 and len(union_output) == 8:
            print("❌ Each game is being counted TWICE in the UNION ALL")
            print("   Root cause: DISTINCT in CTE isn't preventing duplicates")
            print("   Solution: Need to deduplicate BEFORE the UNION")

if __name__ == "__main__":
    debug_duplicates()