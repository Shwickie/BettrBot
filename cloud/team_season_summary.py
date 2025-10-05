#!/usr/bin/env python3
"""
FIXED Team Season Summary Update for Cloud Pipeline
Handles both PostgreSQL and SQLite, prevents duplicates, includes ties support
FIXES: Handles mixed team name formats (abbreviations AND full names)
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
    FIXED: Compute team season summary with proper duplicate handling, ties, and name normalization
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
                # Step 1: DELETE OLD DATA for this season first
                deleted = conn.execute(text("""
                    DELETE FROM team_season_summary WHERE season = :season
                """), {"season": current_season}).rowcount
                print(f"   Cleared {deleted} old entries")
                conn.commit()
                
                # Step 2: Find and remove duplicates from other seasons
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
                    conn.rollback()  # CRITICAL FIX: Rollback failed transaction
                
                # Step 3: Add ties column if missing
                try:
                    conn.execute(text("""
                        ALTER TABLE team_season_summary 
                        ADD COLUMN ties INTEGER DEFAULT 0
                    """))
                    conn.commit()
                    print("   Added ties column")
                except Exception as e:
                    if "already exists" in str(e).lower() or "duplicate column" in str(e).lower():
                        pass  # Column exists
                    else:
                        print(f"   Ties column warning: {e}")
                    conn.rollback()  # CRITICAL FIX: Rollback failed transaction
                
                # Step 4: Calculate team stats with NAME NORMALIZATION + TIES (PostgreSQL)
                result = conn.execute(text("""
                    WITH deduped_games AS (
                        -- First: Deduplicate and filter to 2025 REGULAR SEASON only
                        SELECT DISTINCT ON (game_id)
                            game_id,
                            home_team,
                            away_team,
                            home_score,
                            away_score,
                            game_date
                        FROM games 
                        WHERE home_score IS NOT NULL 
                        AND away_score IS NOT NULL
                        -- CRITICAL FIX: Only include games from Sept 2025 onwards
                        AND game_date >= '2025-09-01'
                        AND game_date <= CURRENT_DATE
                        ORDER BY game_id
                    ),
                    normalized_games AS (
                        -- Then: Normalize ALL team names to full names (handles BOTH abbreviations AND full names)
                        SELECT 
                            game_id,
                            CASE 
                                WHEN home_team = 'ARI' THEN 'Arizona Cardinals'
                                WHEN home_team = 'ATL' THEN 'Atlanta Falcons'
                                WHEN home_team = 'BAL' THEN 'Baltimore Ravens'
                                WHEN home_team = 'BUF' THEN 'Buffalo Bills'
                                WHEN home_team = 'CAR' THEN 'Carolina Panthers'
                                WHEN home_team = 'CHI' THEN 'Chicago Bears'
                                WHEN home_team = 'CIN' THEN 'Cincinnati Bengals'
                                WHEN home_team = 'CLE' THEN 'Cleveland Browns'
                                WHEN home_team = 'DAL' THEN 'Dallas Cowboys'
                                WHEN home_team = 'DEN' THEN 'Denver Broncos'
                                WHEN home_team = 'DET' THEN 'Detroit Lions'
                                WHEN home_team = 'GB' THEN 'Green Bay Packers'
                                WHEN home_team = 'HOU' THEN 'Houston Texans'
                                WHEN home_team = 'IND' OR home_team ILIKE '%colts%' OR home_team = 'Indianapolis Colts' THEN 'Indianapolis Colts'
                                WHEN home_team = 'JAX' THEN 'Jacksonville Jaguars'
                                WHEN home_team = 'KC' THEN 'Kansas City Chiefs'
                                WHEN home_team = 'LAC' THEN 'Los Angeles Chargers'
                                WHEN home_team = 'LAR' OR home_team = 'LA' OR home_team ILIKE '%rams%' OR home_team = 'Los Angeles Rams' THEN 'Los Angeles Rams'
                                WHEN home_team = 'LV' THEN 'Las Vegas Raiders'
                                WHEN home_team = 'MIA' THEN 'Miami Dolphins'
                                WHEN home_team = 'MIN' THEN 'Minnesota Vikings'
                                WHEN home_team = 'NE' THEN 'New England Patriots'
                                WHEN home_team = 'NO' THEN 'New Orleans Saints'
                                WHEN home_team = 'NYG' THEN 'New York Giants'
                                WHEN home_team = 'NYJ' THEN 'New York Jets'
                                WHEN home_team = 'PHI' OR home_team ILIKE '%eagles%' OR home_team = 'Philadelphia Eagles' THEN 'Philadelphia Eagles'
                                WHEN home_team = 'PIT' THEN 'Pittsburgh Steelers'
                                WHEN home_team = 'SEA' THEN 'Seattle Seahawks'
                                WHEN home_team = 'SF' THEN 'San Francisco 49ers'
                                WHEN home_team = 'TB' THEN 'Tampa Bay Buccaneers'
                                WHEN home_team = 'TEN' THEN 'Tennessee Titans'
                                WHEN home_team = 'WAS' THEN 'Washington Commanders'
                                ELSE home_team
                            END as home_team,
                            CASE 
                                WHEN away_team = 'ARI' THEN 'Arizona Cardinals'
                                WHEN away_team = 'ATL' THEN 'Atlanta Falcons'
                                WHEN away_team = 'BAL' THEN 'Baltimore Ravens'
                                WHEN away_team = 'BUF' THEN 'Buffalo Bills'
                                WHEN away_team = 'CAR' THEN 'Carolina Panthers'
                                WHEN away_team = 'CHI' THEN 'Chicago Bears'
                                WHEN away_team = 'CIN' THEN 'Cincinnati Bengals'
                                WHEN away_team = 'CLE' THEN 'Cleveland Browns'
                                WHEN away_team = 'DAL' THEN 'Dallas Cowboys'
                                WHEN away_team = 'DEN' THEN 'Denver Broncos'
                                WHEN away_team = 'DET' THEN 'Detroit Lions'
                                WHEN away_team = 'GB' THEN 'Green Bay Packers'
                                WHEN away_team = 'HOU' THEN 'Houston Texans'
                                WHEN away_team = 'IND' OR away_team ILIKE '%colts%' OR away_team = 'Indianapolis Colts' THEN 'Indianapolis Colts'
                                WHEN away_team = 'JAX' THEN 'Jacksonville Jaguars'
                                WHEN away_team = 'KC' THEN 'Kansas City Chiefs'
                                WHEN away_team = 'LAC' THEN 'Los Angeles Chargers'
                                WHEN away_team = 'LAR' OR away_team = 'LA' OR away_team ILIKE '%rams%' OR away_team = 'Los Angeles Rams' THEN 'Los Angeles Rams'
                                WHEN away_team = 'LV' THEN 'Las Vegas Raiders'
                                WHEN away_team = 'MIA' THEN 'Miami Dolphins'
                                WHEN away_team = 'MIN' THEN 'Minnesota Vikings'
                                WHEN away_team = 'NE' THEN 'New England Patriots'
                                WHEN away_team = 'NO' THEN 'New Orleans Saints'
                                WHEN away_team = 'NYG' THEN 'New York Giants'
                                WHEN away_team = 'NYJ' THEN 'New York Jets'
                                WHEN away_team = 'PHI' OR away_team ILIKE '%eagles%' OR away_team = 'Philadelphia Eagles' THEN 'Philadelphia Eagles'
                                WHEN away_team = 'PIT' THEN 'Pittsburgh Steelers'
                                WHEN away_team = 'SEA' THEN 'Seattle Seahawks'
                                WHEN away_team = 'SF' THEN 'San Francisco 49ers'
                                WHEN away_team = 'TB' THEN 'Tampa Bay Buccaneers'
                                WHEN away_team = 'TEN' THEN 'Tennessee Titans'
                                WHEN away_team = 'WAS' THEN 'Washington Commanders'
                                ELSE away_team
                            END as away_team,
                            home_score,
                            away_score
                        FROM deduped_games
                    ),
                    team_stats AS (
                        SELECT 
                            team,
                            :season as season,
                            COUNT(*) as games_played,
                            SUM(wins) as wins,
                            SUM(losses) as losses,
                            SUM(ties) as ties,
                            AVG(points_for) as avg_points_for,
                            AVG(points_against) as avg_points_against,
                            AVG(point_diff) as point_diff,
                            -- NFL tie handling: ties count as 0.5 wins
                            CASE 
                                WHEN COUNT(*) > 0 
                                THEN (SUM(wins)::float + (SUM(ties)::float * 0.5)) / COUNT(*) 
                                ELSE 0.0 
                            END as win_pct
                        FROM (
                            -- Home games
                            SELECT DISTINCT ON (game_id, home_team)
                                game_id,
                                home_team as team,
                                CASE WHEN home_score > away_score THEN 1 ELSE 0 END as wins,
                                CASE WHEN home_score < away_score THEN 1 ELSE 0 END as losses,
                                CASE WHEN home_score = away_score THEN 1 ELSE 0 END as ties,
                                home_score as points_for,
                                away_score as points_against,
                                home_score - away_score as point_diff
                            FROM normalized_games
                            
                            UNION ALL
                            
                            -- Away games
                            SELECT DISTINCT ON (game_id, away_team)
                                game_id,
                                away_team as team,
                                CASE WHEN away_score > home_score THEN 1 ELSE 0 END as wins,
                                CASE WHEN away_score < home_score THEN 1 ELSE 0 END as losses,
                                CASE WHEN away_score = home_score THEN 1 ELSE 0 END as ties,
                                away_score as points_for,
                                home_score as points_against,
                                away_score - home_score as point_diff
                            FROM normalized_games
                        ) team_games
                        GROUP BY team
                    )
                    INSERT INTO team_season_summary (
                        team, season, games_played, wins, losses, ties,
                        win_pct, avg_points_for, avg_points_against, point_diff, power_score
                    )
                    SELECT 
                        team, season, games_played, wins, losses, ties,
                        win_pct, avg_points_for, avg_points_against, point_diff,
                        point_diff as power_score
                    FROM team_stats
                    ON CONFLICT (team, season) DO UPDATE SET
                        games_played = EXCLUDED.games_played,
                        wins = EXCLUDED.wins,
                        losses = EXCLUDED.losses,
                        ties = EXCLUDED.ties,
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
                
                # Step 2: Calculate and replace team stats with TIES
                conn.execute(text("""
                    REPLACE INTO team_season_summary (
                        team, season, games_played, wins, losses, ties,
                        win_pct, avg_points_for, avg_points_against, point_diff, power_score
                    )
                    SELECT 
                        team,
                        :season as season,
                        COUNT(*) as games_played,
                        SUM(wins) as wins,
                        SUM(losses) as losses,
                        SUM(ties) as ties,
                        CASE 
                            WHEN COUNT(*) > 0 
                            THEN CAST((SUM(wins) + (SUM(ties) * 0.5)) AS REAL) / COUNT(*) 
                            ELSE 0.0 
                        END as win_pct,
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
                            CASE WHEN home_score = away_score THEN 1 ELSE 0 END as ties,
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
                            CASE WHEN away_score = home_score THEN 1 ELSE 0 END as ties,
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
            
            # Show sample results with TIES
            if USE_CLOUD_DB:
                sample = pd.read_sql(text("""
                    SELECT team, wins, losses, ties, games_played, power_score
                    FROM team_season_summary 
                    WHERE season = :season 
                    ORDER BY wins DESC, ties DESC, power_score DESC
                    LIMIT 5
                """), conn, params={"season": current_season})
            else:
                sample = pd.read_sql(text("""
                    SELECT team, wins, losses, ties, games_played, power_score
                    FROM team_season_summary 
                    WHERE season = :season 
                    ORDER BY wins DESC, ties DESC, power_score DESC
                    LIMIT 5
                """), conn, params={"season": current_season})
            
            if not sample.empty:
                print("   Top 5 teams:")
                for _, row in sample.iterrows():
                    ties_str = f"-{int(row['ties'])}" if row['ties'] > 0 else ""
                    print(f"     {row['team']}: {int(row['wins'])}-{int(row['losses'])}{ties_str} ({int(row['games_played'])} games), Power: {row['power_score']:.1f}")
            
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
        print("Your dashboard rankings should now show correct win-loss records with ties")
    else:
        print("\nFAILED: Could not update team season summary")
    
    return success

if __name__ == "__main__":
    main()