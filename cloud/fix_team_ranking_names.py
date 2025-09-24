#!/usr/bin/env python3
"""
Fix team name matching issues for power rankings
SPECIFIC FIX: Eagles and Rams not showing correct records due to team name mismatches
"""

from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime

DATABASE_URL = "postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres"

def get_team_name_mapping():
    """Comprehensive team name mapping to handle all variations"""
    return {
        # Los Angeles Rams variations
        'LA': 'Los Angeles Rams',
        'LAR': 'Los Angeles Rams', 
        'Los Angeles Rams': 'Los Angeles Rams',
        'LA Rams': 'Los Angeles Rams',
        'Rams': 'Los Angeles Rams',
        'STL': 'Los Angeles Rams',  # Legacy St. Louis
        
        # Philadelphia Eagles variations
        'PHI': 'Philadelphia Eagles',
        'Philadelphia Eagles': 'Philadelphia Eagles',
        'Eagles': 'Philadelphia Eagles',
        
        # All other teams (standard mapping)
        'ARI': 'Arizona Cardinals',
        'ATL': 'Atlanta Falcons',
        'BAL': 'Baltimore Ravens',
        'BUF': 'Buffalo Bills',
        'CAR': 'Carolina Panthers',
        'CHI': 'Chicago Bears',
        'CIN': 'Cincinnati Bengals',
        'CLE': 'Cleveland Browns',
        'DAL': 'Dallas Cowboys',
        'DEN': 'Denver Broncos',
        'DET': 'Detroit Lions',
        'GB': 'Green Bay Packers',
        'HOU': 'Houston Texans',
        'IND': 'Indianapolis Colts',
        'JAX': 'Jacksonville Jaguars',
        'KC': 'Kansas City Chiefs',
        'LV': 'Las Vegas Raiders',
        'LAC': 'Los Angeles Chargers',
        'MIA': 'Miami Dolphins',
        'NE': 'New England Patriots',
        'NO': 'New Orleans Saints',
        'NYG': 'New York Giants',
        'NYJ': 'New York Jets',
        'PIT': 'Pittsburgh Steelers',
        'SF': 'San Francisco 49ers',
        'SEA': 'Seattle Seahawks',
        'TB': 'Tampa Bay Buccaneers',
        'TEN': 'Tennessee Titans',
        'WAS': 'Washington Commanders',
        
        # Full names for completeness
        'Arizona Cardinals': 'Arizona Cardinals',
        'Atlanta Falcons': 'Atlanta Falcons',
        'Baltimore Ravens': 'Baltimore Ravens',
        'Buffalo Bills': 'Buffalo Bills',
        'Carolina Panthers': 'Carolina Panthers',
        'Chicago Bears': 'Chicago Bears',
        'Cincinnati Bengals': 'Cincinnati Bengals',
        'Cleveland Browns': 'Cleveland Browns',
        'Dallas Cowboys': 'Dallas Cowboys',
        'Denver Broncos': 'Denver Broncos',
        'Detroit Lions': 'Detroit Lions',
        'Green Bay Packers': 'Green Bay Packers',
        'Houston Texans': 'Houston Texans',
        'Indianapolis Colts': 'Indianapolis Colts',
        'Jacksonville Jaguars': 'Jacksonville Jaguars',
        'Kansas City Chiefs': 'Kansas City Chiefs',
        'Las Vegas Raiders': 'Las Vegas Raiders',
        'Los Angeles Chargers': 'Los Angeles Chargers',
        'Miami Dolphins': 'Miami Dolphins',
        'New England Patriots': 'New England Patriots',
        'New Orleans Saints': 'New Orleans Saints',
        'New York Giants': 'New York Giants',
        'New York Jets': 'New York Jets',
        'Pittsburgh Steelers': 'Pittsburgh Steelers',
        'San Francisco 49ers': 'San Francisco 49ers',
        'Seattle Seahawks': 'Seattle Seahawks',
        'Tampa Bay Buccaneers': 'Tampa Bay Buccaneers',
        'Tennessee Titans': 'Tennessee Titans',
        'Washington Commanders': 'Washington Commanders'
    }

def normalize_team_name(team_name):
    """Normalize any team name to standard format"""
    if not team_name:
        return team_name
    
    mapping = get_team_name_mapping()
    normalized = mapping.get(team_name, team_name)
    return normalized

def diagnose_team_name_issues():
    """Diagnose team name matching issues specifically for Eagles and Rams"""
    print("DIAGNOSING TEAM NAME MATCHING ISSUES")
    print("=" * 40)
    
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    
    try:
        with engine.connect() as conn:
            # Check what team names exist in games table
            print("1. Checking team names in games table...")
            team_names = pd.read_sql(text("""
                SELECT DISTINCT home_team FROM games
                UNION
                SELECT DISTINCT away_team FROM games
                ORDER BY 1
            """), conn)
            
            print("Team names in games table:")
            for name in team_names['home_team']:
                print(f"  '{name}'")
            
            # Check Eagles and Rams specifically
            print("\n2. Looking for Eagles games...")
            eagles_games = pd.read_sql(text("""
                SELECT game_id, home_team, away_team, home_score, away_score, game_date
                FROM games 
                WHERE (home_team LIKE '%Eagle%' OR away_team LIKE '%Eagle%'
                       OR home_team = 'PHI' OR away_team = 'PHI')
                AND home_score IS NOT NULL AND away_score IS NOT NULL
                ORDER BY game_date
            """), conn)
            
            print(f"Found {len(eagles_games)} Eagles games with scores:")
            for _, game in eagles_games.iterrows():
                home_score = int(game['home_score']) if pd.notna(game['home_score']) else 'N/A'
                away_score = int(game['away_score']) if pd.notna(game['away_score']) else 'N/A'
                print(f"  {game['game_date']}: {game['away_team']} {away_score} @ {game['home_team']} {home_score}")
            
            print("\n3. Looking for Rams games...")
            rams_games = pd.read_sql(text("""
                SELECT game_id, home_team, away_team, home_score, away_score, game_date
                FROM games 
                WHERE (home_team LIKE '%Rams%' OR away_team LIKE '%Rams%'
                       OR home_team LIKE '%LA%' OR away_team LIKE '%LA%'
                       OR home_team = 'LAR' OR away_team = 'LAR')
                AND home_score IS NOT NULL AND away_score IS NOT NULL
                ORDER BY game_date
            """), conn)
            
            print(f"Found {len(rams_games)} Rams games with scores:")
            for _, game in rams_games.iterrows():
                home_score = int(game['home_score']) if pd.notna(game['home_score']) else 'N/A'
                away_score = int(game['away_score']) if pd.notna(game['away_score']) else 'N/A'
                print(f"  {game['game_date']}: {game['away_team']} {away_score} @ {game['home_team']} {home_score}")
            
            # Check current team_season_summary
            print("\n4. Current team_season_summary for Eagles and Rams...")
            eagles_rams_stats = pd.read_sql(text("""
                SELECT team, wins, losses, games_played, power_score
                FROM team_season_summary 
                WHERE season = 2025
                AND (team LIKE '%Eagle%' OR team LIKE '%Rams%' 
                     OR team = 'PHI' OR team = 'LAR')
            """), conn)
            
            print("Current stats:")
            for _, team in eagles_rams_stats.iterrows():
                print(f"  {team['team']}: {int(team['wins'])}-{int(team['losses'])} ({int(team['games_played'])} games)")
            
            return len(eagles_games) > 0 or len(rams_games) > 0
            
    except Exception as e:
        print(f"Error: {e}")
        return False

def fix_team_name_consistency():
    """Fix team name consistency in the database"""
    print("\nFIXING TEAM NAME CONSISTENCY")
    print("=" * 30)
    
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    
    try:
        with engine.connect() as conn:
            # First, standardize team names in games table if needed
            print("Checking if team names need standardization...")
            
            # Check for common variations that need fixing
            team_fixes = [
                ("LA", "Los Angeles Rams"),
                ("LAR", "Los Angeles Rams"),
                ("PHI", "Philadelphia Eagles")
            ]
            
            fixes_made = 0
            for old_name, new_name in team_fixes:
                # Update home_team
                result1 = conn.execute(text("""
                    UPDATE games 
                    SET home_team = :new_name 
                    WHERE home_team = :old_name
                """), {"old_name": old_name, "new_name": new_name})
                
                # Update away_team  
                result2 = conn.execute(text("""
                    UPDATE games 
                    SET away_team = :new_name 
                    WHERE away_team = :old_name
                """), {"old_name": old_name, "new_name": new_name})
                
                total_updates = result1.rowcount + result2.rowcount
                if total_updates > 0:
                    print(f"  Updated {total_updates} games: '{old_name}' -> '{new_name}'")
                    fixes_made += total_updates
            
            conn.commit()
            
            if fixes_made > 0:
                print(f"Made {fixes_made} team name fixes")
            else:
                print("No team name fixes needed")
            
            return True
            
    except Exception as e:
        print(f"Error fixing team names: {e}")
        return False

def recalculate_team_rankings():
    """Recalculate team season summary with fixed team names"""
    print("\nRECALCULATING TEAM RANKINGS")
    print("=" * 30)
    
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    
    try:
        current_season = 2025
        
        with engine.connect() as conn:
            print("Clearing existing 2025 team summaries...")
            
            # Delete current 2025 summaries
            deleted = conn.execute(text("""
                DELETE FROM team_season_summary WHERE season = :season
            """), {"season": current_season}).rowcount
            
            print(f"Deleted {deleted} old summaries")
            
            # First, check what 2025 games actually exist
            games_2025 = pd.read_sql(text("""
                SELECT game_date, home_team, away_team, home_score, away_score
                FROM games 
                WHERE EXTRACT(YEAR FROM game_date) = :season
                AND home_score IS NOT NULL AND away_score IS NOT NULL
                ORDER BY game_date
            """), conn, params={"season": current_season})
            
            print(f"Found {len(games_2025)} completed games in 2025:")
            if not games_2025.empty:
                date_range = f"{games_2025['game_date'].min()} to {games_2025['game_date'].max()}"
                print(f"Date range: {date_range}")
                
                # Show sample 2025 games
                print("Sample 2025 games:")
                for _, game in games_2025.head(5).iterrows():
                    print(f"  {game['game_date']}: {game['away_team']} {int(game['away_score'])} @ {game['home_team']} {int(game['home_score'])}")
            else:
                print("ERROR: No completed 2025 games found!")
                return False
            
            # Recalculate with proper team name handling AND 2025-only filter
            print(f"Recalculating for {current_season} season only...")
            
            result = conn.execute(text("""
                INSERT INTO team_season_summary (
                    team, season, games_played, wins, losses, 
                    win_pct, avg_points_for, avg_points_against, point_diff, power_score
                )
                SELECT 
                    team, 
                    :season as season,
                    COUNT(*) as games_played,
                    SUM(wins) as wins,
                    SUM(losses) as losses,
                    CASE WHEN COUNT(*) > 0 THEN SUM(wins)::float / COUNT(*) ELSE 0.0 END as win_pct,
                    AVG(points_for) as avg_points_for,
                    AVG(points_against) as avg_points_against,
                    AVG(point_diff) as point_diff,
                    AVG(point_diff) as power_score
                FROM (
                    -- Home games (2025 ONLY)
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
                    AND game_date >= '2025-09-01'  -- Only actual 2025 season
                    
                    UNION ALL
                    
                    -- Away games (2025 ONLY)
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
                    AND game_date >= '2025-09-01'  -- Only actual 2025 season
                ) team_games
                GROUP BY team
            """), {"season": current_season})
            
            conn.commit()
            
            # Verify the fix - check Eagles and Rams specifically
            eagles_rams_check = pd.read_sql(text("""
                SELECT team, wins, losses, games_played, win_pct, power_score
                FROM team_season_summary 
                WHERE season = 2025
                AND (team LIKE '%Eagles%' OR team LIKE '%Rams%')
                ORDER BY power_score DESC
            """), conn)
            
            print(f"\nEagles and Rams after fix:")
            if not eagles_rams_check.empty:
                for _, team in eagles_rams_check.iterrows():
                    print(f"  {team['team']}: {int(team['wins'])}-{int(team['losses'])} ({int(team['games_played'])} games, {team['win_pct']:.3f})")
            else:
                print("  WARNING: Still no Eagles or Rams found!")
            
            # Show all teams to verify
            all_teams = pd.read_sql(text("""
                SELECT team, wins, losses, games_played, win_pct, power_score
                FROM team_season_summary 
                WHERE season = 2025
                ORDER BY power_score DESC
                LIMIT 10
            """), conn)
            
            print(f"\nTop 10 teams after fix:")
            for i, team in all_teams.iterrows():
                print(f"  {i+1:2d}. {team['team']:25s} {int(team['wins'])}-{int(team['losses'])} ({team['win_pct']:.3f})")
            
            return True
            
    except Exception as e:
        print(f"Error recalculating rankings: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main execution - Fix team name matching for Eagles and Rams"""
    print("TEAM NAME MATCHING FIX FOR EAGLES & RAMS")
    print("=" * 50)
    
    # Step 1: Diagnose the current issue
    has_games = diagnose_team_name_issues()
    
    if not has_games:
        print("\nNo Eagles or Rams games found - this suggests a data issue")
        return False
    
    # Step 2: Fix team name consistency
    print(f"\n{'='*50}")
    if fix_team_name_consistency():
        print("Team name fixes applied successfully")
    else:
        print("ERROR: Team name fixes failed")
        return False
    
    # Step 3: Recalculate rankings
    print(f"\n{'='*50}")
    if recalculate_team_rankings():
        print("SUCCESS: Team rankings recalculated!")
        print("\nEagles and Rams should now show correct records")
        print("All teams should have proper game counts")
        return True
    else:
        print("ERROR: Ranking recalculation failed")
        return False

if __name__ == "__main__":
    main()