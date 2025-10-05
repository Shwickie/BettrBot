#!/usr/bin/env python3
"""
Check the actual Rams games in raw database
"""

from sqlalchemy import create_engine, text
import pandas as pd

DATABASE_URL = "postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres"

def check_rams():
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    
    with engine.connect() as conn:
        # Get ALL Rams games Sept 2025+
        print("ALL RAMS GAMES (Sept 2025+):")
        rams_games = pd.read_sql(text("""
            SELECT game_id, game_date, home_team, away_team, home_score, away_score
            FROM games
            WHERE game_date >= '2025-09-01'
            AND home_score IS NOT NULL
            AND (home_team = 'Los Angeles Rams' OR away_team = 'Los Angeles Rams')
            ORDER BY game_date
        """), conn)
        
        print(f"Found {len(rams_games)} Rams games:\n")
        for _, game in rams_games.iterrows():
            print(f"{game['game_date']}: {game['away_team']} @ {game['home_team']}")
            print(f"  Score: {game['away_score']}-{game['home_score']}")
        
        # Check what the deduped CTE sees
        print("\n\nWHAT DEDUPED_GAMES CTE PRODUCES:")
        deduped = pd.read_sql(text("""
            WITH deduped_games AS (
                SELECT DISTINCT ON (game_id)
                    game_id,
                    home_team,
                    away_team,
                    home_score,
                    away_score
                FROM games 
                WHERE home_score IS NOT NULL 
                AND game_date >= '2025-09-01'
                AND game_date <= CURRENT_DATE
                ORDER BY game_id
            )
            SELECT * FROM deduped_games
            WHERE home_team = 'Los Angeles Rams' OR away_team = 'Los Angeles Rams'
        """), conn)
        
        print(f"Deduped CTE finds {len(deduped)} Rams games")
        
        # Check what normalized sees
        print("\n\nWHAT NORMALIZED_GAMES CTE PRODUCES:")
        normalized = pd.read_sql(text("""
            WITH deduped_games AS (
                SELECT DISTINCT ON (game_id)
                    game_id,
                    home_team,
                    away_team,
                    home_score,
                    away_score
                FROM games 
                WHERE home_score IS NOT NULL 
                AND game_date >= '2025-09-01'
                AND game_date <= CURRENT_DATE
                ORDER BY game_id
            ),
            normalized_games AS (
                SELECT 
                    game_id,
                    CASE 
                        WHEN home_team = 'IND' THEN 'Indianapolis Colts'
                        WHEN home_team = 'PHI' OR home_team ILIKE '%eagle%' THEN 'Philadelphia Eagles'
                        WHEN home_team = 'LAR' OR home_team = 'LA' OR home_team ILIKE '%ram%' THEN 'Los Angeles Rams'
                        ELSE home_team
                    END as home_team,
                    CASE 
                        WHEN away_team = 'IND' THEN 'Indianapolis Colts'
                        WHEN away_team = 'PHI' OR away_team ILIKE '%eagle%' THEN 'Philadelphia Eagles'
                        WHEN away_team = 'LAR' OR away_team = 'LA' OR away_team ILIKE '%ram%' THEN 'Los Angeles Rams'
                        ELSE away_team
                    END as away_team,
                    home_score,
                    away_score
                FROM deduped_games
            )
            SELECT * FROM normalized_games
            WHERE home_team = 'Los Angeles Rams' OR away_team = 'Los Angeles Rams'
        """), conn)
        
        print(f"Normalized CTE finds {len(normalized)} Rams games:")
        for _, game in normalized.iterrows():
            print(f"  {game['away_team']} @ {game['home_team']}")

if __name__ == "__main__":
    check_rams()