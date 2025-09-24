#!/usr/bin/env python3
"""
FIXED Team Season Summary Update for Cloud Pipeline
Handles both PostgreSQL and SQLite, prevents duplicates, uses same file names as your scheduler
"""

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime

# Database setup - cloud or local
DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

USE_CLOUD_DB = bool(DATABASE_URL)

def get_engine():
    """Get database engine for cloud or local"""
    if USE_CLOUD_DB:
        return create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300)
    else:
        # Local SQLite fallback
        local_db = r"E:/Bettr Bot/betting-bot/data/betting.db"
        return create_engine(f"sqlite:///{local_db}")

# Team mappings - handle both abbreviations and full names
TEAM_TO_ABBR = {
    "Arizona Cardinals": "ARI", "Atlanta Falcons": "ATL", "Baltimore Ravens": "BAL",
    "Buffalo Bills": "BUF", "Carolina Panthers": "CAR", "Chicago Bears": "CHI",
    "Cincinnati Bengals": "CIN", "Cleveland Browns": "CLE", "Dallas Cowboys": "DAL",
    "Denver Broncos": "DEN", "Detroit Lions": "DET", "Green Bay Packers": "GB",
    "Houston Texans": "HOU", "Indianapolis Colts": "IND", "Jacksonville Jaguars": "JAX",
    "Kansas City Chiefs": "KC", "Las Vegas Raiders": "LV", "Los Angeles Chargers": "LAC",
    "Los Angeles Rams": "LAR", "Miami Dolphins": "MIA", "Minnesota Vikings": "MIN",
    "New England Patriots": "NE", "New Orleans Saints": "NO", "New York Giants": "NYG",
    "New York Jets": "NYJ", "Philadelphia Eagles": "PHI", "Pittsburgh Steelers": "PIT",
    "San Francisco 49ers": "SF", "Seattle Seahawks": "SEA", "Tampa Bay Buccaneers": "TB",
    "Tennessee Titans": "TEN", "Washington Commanders": "WAS"
}

ALIAS_TO_ABBR = {
    "LA": "LAR", "STL": "LAR", "SD": "LAC", "OAK": "LV", 
    "JAC": "JAX", "WSH": "WAS"
}

def normalize_team_name(team_name):
    """Convert any team name format to standard abbreviation"""
    if not team_name:
        return team_name
    
    team_str = str(team_name).strip()
    
    # If it's already a known abbreviation
    if team_str.upper() in TEAM_TO_ABBR.values():
        return team_str.upper()
    
    # If it's a full name
    if team_str in TEAM_TO_ABBR:
        return TEAM_TO_ABBR[team_str]
    
    # Handle aliases
    team_upper = team_str.upper()
    if team_upper in ALIAS_TO_ABBR:
        return ALIAS_TO_ABBR[team_upper]
    
    # Fallback - return as is but uppercased
    return team_upper

def get_current_season():
    """Get current NFL season year"""
    now = datetime.now()
    # NFL season starts in August, so if we're in Jan-July, it's the previous year's season
    return now.year if now.month >= 8 else now.year - 1

def compute_team_season_summary():
    """
    FIXED: Compute team season summary with proper duplicate handling
    This replaces the problematic function in your cloud_run_all.py
    """
    print("TASK: Running team_season_summary...")
    
    try:
        engine = get_engine()
        
        with engine.connect() as conn:
            # Get current season
            current_season = get_current_season()
            print(f"   Updating team stats for season {current_season}")
            
            # CRITICAL: Clean up duplicates first before adding constraint
            if USE_CLOUD_DB:
                # PostgreSQL version
                # Step 1: Find and remove duplicates
                duplicates_check = conn.execute(text("""
                    SELECT team, season, COUNT(*) as count
                    FROM team_season_summary 
                    GROUP BY team, season 
                    HAVING COUNT(*) > 1
                """)).fetchall()
                
                if duplicates_check:
                    print(f"   Found {len(duplicates_check)} duplicate team/season combinations, cleaning...")
                    for team, season, count in duplicates_check:
                        # Keep only the most recent entry (highest ID or best data)
                        conn.execute(text("""
                            DELETE FROM team_season_summary 
                            WHERE team = :team AND season = :season
                            AND id NOT IN (
                                SELECT id FROM team_season_summary 
                                WHERE team = :team AND season = :season
                                ORDER BY games_played DESC, power_score DESC
                                LIMIT 1
                            )
                        """), {"team": team, "season": season})
                    
                    conn.commit()
                    print("   Duplicates cleaned")
                
                # Step 2: Ensure unique constraint exists (PostgreSQL)
                try:
                    conn.execute(text("""
                        ALTER TABLE team_season_summary 
                        ADD CONSTRAINT team_season_unique UNIQUE (team, season)
                    """))
                    conn.commit()
                except Exception as e:
                    if "already exists" in str(e).lower():
                        print("   Unique constraint already exists")
                    else:
                        print(f"   Warning: Could not add constraint: {e}")
                
                # Step 3: Calculate team stats with UPSERT (PostgreSQL)
                result = conn.execute(text("""
                    WITH team_stats AS (
                        SELECT 
                            team,
                            :season as season,
                            COUNT(*) as games_played,
                            SUM(wins) as wins,
                            SUM(losses) as losses,
                            AVG(points_for) as avg_points_for,
                            AVG(points_against) as avg_points_against,
                            AVG(point_diff) as point_diff,
                            CASE WHEN COUNT(*) > 0 THEN SUM(wins)::float / COUNT(*) ELSE 0.0 END as win_pct
                        FROM (
                            SELECT DISTINCT
                                game_id,
                                home_team as team,
                                CASE WHEN home_score > away_score THEN 1 ELSE 0 END as wins,
                                CASE WHEN home_score < away_score THEN 1 ELSE 0 END as losses,
                                home_score as points_for,
                                away_score as points_against,
                                home_score - away_score as point_diff
                            FROM games 
                            WHERE home_score IS NOT NULL AND away_score IS NOT NULL
                            AND EXTRACT(YEAR FROM game_date) = :season
                            AND game_date <= CURRENT_DATE
                            
                            UNION ALL
                            
                            SELECT DISTINCT
                                game_id,
                                away_team as team,
                                CASE WHEN away_score > home_score THEN 1 ELSE 0 END as wins,
                                CASE WHEN away_score < home_score THEN 1 ELSE 0 END as losses,
                                away_score as points_for,
                                home_score as points_against,
                                away_score - home_score as point_diff
                            FROM games 
                            WHERE home_score IS NOT NULL AND away_score IS NOT NULL
                            AND EXTRACT(YEAR FROM game_date) = :season
                            AND game_date <= CURRENT_DATE
                        ) team_games
                        GROUP BY team
                    )
                    INSERT INTO team_season_summary (
                        team, season, games_played, wins, losses, 
                        win_pct, avg_points_for, avg_points_against, point_diff, power_score
                    )
                    SELECT 
                        team, season, games_played, wins, losses,
                        win_pct, avg_points_for, avg_points_against, point_diff,
                        point_diff as power_score
                    FROM team_stats
                    ON CONFLICT (team, season) DO UPDATE SET
                        games_played = EXCLUDED.games_played,
                        wins = EXCLUDED.wins,
                        losses = EXCLUDED.losses,
                        win_pct = EXCLUDED.win_pct,
                        avg_points_for = EXCLUDED.avg_points_for,
                        avg_points_against = EXCLUDED.avg_points_against,
                        point_diff = EXCLUDED.point_diff,
                        power_score = EXCLUDED.power_score
                """), {"season": current_season})
                
            else:
                # SQLite version - simpler handling
                # Step 1: Remove duplicates
                conn.execute(text("""
                    DELETE FROM team_season_summary 
                    WHERE rowid NOT IN (
                        SELECT MIN(rowid) 
                        FROM team_season_summary 
                        GROUP BY team, season
                    )
                """))
                
                # Step 2: Calculate and replace team stats
                conn.execute(text("""
                    REPLACE INTO team_season_summary (
                        team, season, games_played, wins, losses, 
                        win_pct, avg_points_for, avg_points_against, point_diff, power_score
                    )
                    SELECT 
                        team,
                        :season as season,
                        COUNT(*) as games_played,
                        SUM(wins) as wins,
                        SUM(losses) as losses,
                        CASE WHEN COUNT(*) > 0 THEN CAST(SUM(wins) AS REAL) / COUNT(*) ELSE 0.0 END as win_pct,
                        AVG(points_for) as avg_points_for,
                        AVG(points_against) as avg_points_against,
                        AVG(point_diff) as point_diff,
                        AVG(point_diff) as power_score
                    FROM (
                        SELECT 
                            game_id,
                            home_team as team,
                            CASE WHEN home_score > away_score THEN 1 ELSE 0 END as wins,
                            CASE WHEN home_score < away_score THEN 1 ELSE 0 END as losses,
                            home_score as points_for,
                            away_score as points_against,
                            home_score - away_score as point_diff
                        FROM games 
                        WHERE home_score IS NOT NULL AND away_score IS NOT NULL
                        AND strftime('%Y', game_date) = :season_str
                        AND date(game_date) <= date('now')
                        
                        UNION ALL
                        
                        SELECT 
                            game_id,
                            away_team as team,
                            CASE WHEN away_score > home_score THEN 1 ELSE 0 END as wins,
                            CASE WHEN away_score < home_score THEN 1 ELSE 0 END as losses,
                            away_score as points_for,
                            home_score as points_against,
                            away_score - home_score as point_diff
                        FROM games 
                        WHERE home_score IS NOT NULL AND away_score IS NOT NULL
                        AND strftime('%Y', game_date) = :season_str
                        AND date(game_date) <= date('now')
                    ) team_games
                    GROUP BY team
                """), {"season": current_season, "season_str": str(current_season)})
            
            conn.commit()
            
            # Verify results
            if USE_CLOUD_DB:
                count_check = conn.execute(text("""
                    SELECT COUNT(*) FROM team_season_summary WHERE season = :season
                """), {"season": current_season}).fetchone()[0]
            else:
                count_check = conn.execute(text("""
                    SELECT COUNT(*) FROM team_season_summary WHERE season = :season
                """), {"season": current_season}).fetchone()[0]
            
            print(f"   SUCCESS: team_season_summary updated - {count_check} teams for season {current_season}")
            
            # Show sample results
            if USE_CLOUD_DB:
                sample = pd.read_sql(text("""
                    SELECT team, wins, losses, games_played, power_score
                    FROM team_season_summary 
                    WHERE season = :season 
                    ORDER BY power_score DESC 
                    LIMIT 5
                """), conn, params={"season": current_season})
            else:
                sample = pd.read_sql(text("""
                    SELECT team, wins, losses, games_played, power_score
                    FROM team_season_summary 
                    WHERE season = :season 
                    ORDER BY power_score DESC 
                    LIMIT 5
                """), conn, params={"season": current_season})
            
            if not sample.empty:
                print("   Top 5 teams:")
                for _, row in sample.iterrows():
                    print(f"     {row['team']}: {row['wins']}-{row['losses']} ({row['games_played']} games), Power: {row['power_score']:.1f}")
            
            return True
            
    except Exception as e:
        print(f"   ERROR: team_season_summary failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function for standalone execution"""
    print("TEAM SEASON SUMMARY UPDATE - FIXED VERSION")
    print("=" * 50)
    
    success = compute_team_season_summary()
    
    if success:
        print("\nSUCCESS: Team season summary updated!")
        print("Your dashboard rankings should now show correct win-loss records")
    else:
        print("\nFAILED: Could not update team season summary")
    
    return success

if __name__ == "__main__":
    main()