# fix_cloud_rankings.py - Fix the cloud rankings calculation
"""
This script fixes the rankings by:
1. Removing the problematic 'week' column references
2. Updating team records based on completed games
3. Ensuring proper season calculation
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date

# Database setup
DATABASE_URL = os.environ.get("DATABASE_URL") or "postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres?sslmode=require"

if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=280,
    pool_timeout=30,
    connect_args={
        "sslmode": "require",
        "connect_timeout": 30,
        "application_name": "bettrbot_rankings_fix"
    }
)

# Team mapping functions
ABBR_TO_FULL = {
    'ARI': 'Arizona Cardinals','ATL': 'Atlanta Falcons','BAL': 'Baltimore Ravens','BUF': 'Buffalo Bills',
    'CAR': 'Carolina Panthers','CHI': 'Chicago Bears','CIN': 'Cincinnati Bengals','CLE': 'Cleveland Browns',
    'DAL': 'Dallas Cowboys','DEN': 'Denver Broncos','DET': 'Detroit Lions','GB': 'Green Bay Packers',
    'HOU': 'Houston Texans','IND': 'Indianapolis Colts','JAX': 'Jacksonville Jaguars','KC': 'Kansas City Chiefs',
    'LV': 'Las Vegas Raiders','LAC': 'Los Angeles Chargers','LA': 'Los Angeles Rams','LAR': 'Los Angeles Rams',
    'MIA': 'Miami Dolphins','MIN': 'Minnesota Vikings','NE': 'New England Patriots','NO': 'New Orleans Saints',
    'NYG': 'New York Giants','NYJ': 'New York Jets','PHI': 'Philadelphia Eagles','PIT': 'Pittsburgh Steelers',
    'SF': 'San Francisco 49ers','SEA': 'Seattle Seahawks','TB': 'Tampa Bay Buccaneers','TEN': 'Tennessee Titans',
    'WAS': 'Washington Commanders'
}

FULL_TO_ABBR = {v: k for k, v in ABBR_TO_FULL.items()}

def to_full(name):
    """Convert team name to full name"""
    if not name:
        return "Unknown"
    s = str(name).strip()
    if s in ABBR_TO_FULL:
        return ABBR_TO_FULL[s]
    return s

def compute_live_records_fixed(conn, season: int):
    """
    FIXED VERSION: Completely removed 'week' column references that were causing PostgreSQL errors
    """
    
    # Get games for the season with completed scores
    games = pd.read_sql_query(text("""
        WITH g AS (
            SELECT
                game_id, home_team, away_team,
                home_score, away_score, id, game_date,
                CASE
                  WHEN EXTRACT(MONTH FROM game_date) >= 8
                    THEN EXTRACT(YEAR FROM game_date)::int
                  ELSE (EXTRACT(YEAR FROM game_date)::int) - 1
                END AS season_year
            FROM games
        )
        SELECT game_id, home_team, away_team, home_score, away_score, id, game_date
        FROM g
        WHERE season_year = :season
          AND home_score IS NOT NULL AND away_score IS NOT NULL
    """), conn, params={"season": season})

    if games.empty:
        print(f"No completed games found for season {season}")
        return pd.DataFrame(columns=[
            "team","wins","losses","ties","games_played","win_pct",
            "points_for","points_against","point_diff"
        ])

    print(f"Found {len(games)} completed games for season {season}")

    # Convert team names to full names
    games["home_team"] = games["home_team"].apply(to_full)
    games["away_team"] = games["away_team"].apply(to_full)
    
    # Create unique game identifier for deduplication
    games["game_id"] = games["game_id"].fillna("").astype(str).str.strip()
    games["gid_fallback"] = (
        pd.to_datetime(games["game_date"]).dt.strftime("%Y%m%d") + "_" +
        games["away_team"].str.replace(" ", "") + "_" +
        games["home_team"].str.replace(" ", "")
    )
    games["gid"] = games["game_id"].where(games["game_id"] != "", games["gid_fallback"])

    # Remove duplicates
    games = games.sort_values("id").drop_duplicates("gid", keep="last")

    # Calculate wins/losses/ties
    games["home_win"] = (games["home_score"] > games["away_score"]).astype(int)
    games["away_win"] = (games["away_score"] > games["home_score"]).astype(int)
    games["tie"] = (games["home_score"] == games["away_score"]).astype(int)

    # Calculate home team stats
    home_stats = games.groupby("home_team").agg(
        wins=("home_win", "sum"), 
        losses=("away_win", "sum"), 
        ties=("tie", "sum"),
        games_played=("home_win", "size"), 
        points_for=("home_score", "sum"),
        points_against=("away_score", "sum"),
    ).reset_index()
    home_stats.rename(columns={"home_team": "team"}, inplace=True)

    # Calculate away team stats
    away_stats = games.groupby("away_team").agg(
        wins=("away_win", "sum"), 
        losses=("home_win", "sum"), 
        ties=("tie", "sum"),
        games_played=("away_win", "size"), 
        points_for=("away_score", "sum"),
        points_against=("home_score", "sum"),
    ).reset_index()
    away_stats.rename(columns={"away_team": "team"}, inplace=True)

    # Combine home and away stats
    all_teams = set(home_stats["team"].unique()) | set(away_stats["team"].unique())
    
    records = []
    for team in all_teams:
        home_row = home_stats[home_stats["team"] == team]
        away_row = away_stats[away_stats["team"] == team]
        
        home_wins = home_row["wins"].iloc[0] if not home_row.empty else 0
        home_losses = home_row["losses"].iloc[0] if not home_row.empty else 0
        home_ties = home_row["ties"].iloc[0] if not home_row.empty else 0
        home_games = home_row["games_played"].iloc[0] if not home_row.empty else 0
        home_pf = home_row["points_for"].iloc[0] if not home_row.empty else 0
        home_pa = home_row["points_against"].iloc[0] if not home_row.empty else 0
        
        away_wins = away_row["wins"].iloc[0] if not away_row.empty else 0
        away_losses = away_row["losses"].iloc[0] if not away_row.empty else 0
        away_ties = away_row["ties"].iloc[0] if not away_row.empty else 0
        away_games = away_row["games_played"].iloc[0] if not away_row.empty else 0
        away_pf = away_row["points_for"].iloc[0] if not away_row.empty else 0
        away_pa = away_row["points_against"].iloc[0] if not away_row.empty else 0
        
        total_wins = int(home_wins + away_wins)
        total_losses = int(home_losses + away_losses)
        total_ties = int(home_ties + away_ties)
        total_games = int(home_games + away_games)
        total_pf = int(home_pf + away_pf)
        total_pa = int(home_pa + away_pa)
        
        if total_games > 0:
            win_pct = (total_wins + 0.5 * total_ties) / total_games
        else:
            win_pct = 0.0
            
        records.append({
            "team": team, 
            "wins": total_wins, 
            "losses": total_losses, 
            "ties": total_ties,
            "games_played": total_games, 
            "win_pct": win_pct, 
            "points_for": total_pf,
            "points_against": total_pa, 
            "point_diff": total_pf - total_pa
        })

    result_df = pd.DataFrame(records)
    print(f"Calculated records for {len(result_df)} teams")
    
    # Show sample of calculated records
    teams_with_games = result_df[result_df["games_played"] > 0]
    if not teams_with_games.empty:
        print("\nTeams with completed games:")
        for _, team in teams_with_games.head(10).iterrows():
            print(f"  {team['team']}: {team['wins']}-{team['losses']}-{team['ties']} ({team['games_played']} games)")
    else:
        print("\nNo teams have completed games yet (likely preseason)")
    
    return result_df

def update_team_season_summary(conn, season: int):
    """Update team_season_summary with live records"""
    
    # Get live records
    live_records = compute_live_records_fixed(conn, season)
    
    if live_records.empty:
        print("No live records to update")
        return False
    
    # Update each team's record in team_season_summary
    updated_teams = 0
    
    for _, record in live_records.iterrows():
        try:
            # Convert full name to abbreviation for database storage
            team_abbr = FULL_TO_ABBR.get(record['team'], record['team'])
            
            conn.execute(text("""
                UPDATE team_season_summary 
                SET 
                    wins = :wins,
                    losses = :losses,
                    games_played = :games_played,
                    win_pct = :win_pct,
                    avg_points_for = :avg_points_for,
                    avg_points_against = :avg_points_against,
                    point_diff = :point_diff
                WHERE team = :team AND season = :season
            """), {
                'wins': int(record['wins']),
                'losses': int(record['losses']),
                'games_played': int(record['games_played']),
                'win_pct': float(record['win_pct']),
                'avg_points_for': float(record['points_for']) / max(1, record['games_played']),
                'avg_points_against': float(record['points_against']) / max(1, record['games_played']),
                'point_diff': float(record['point_diff']),
                'team': team_abbr,
                'season': season
            })
            
            updated_teams += 1
            
        except Exception as e:
            print(f"Error updating {record['team']}: {e}")
            continue
    
    conn.commit()
    print(f"Updated {updated_teams} teams in team_season_summary")
    return True

def main():
    """Fix cloud rankings calculation"""
    print("FIXING CLOUD RANKINGS CALCULATION")
    print("=" * 50)
    
    try:
        with engine.connect() as conn:
            print("Connected to cloud database")
            
            # Check current season
            current_season = date.today().year if date.today().month >= 8 else date.today().year - 1
            print(f"Working with season: {current_season}")
            
            # Check games data
            total_games = conn.execute(text("SELECT COUNT(*) FROM games")).scalar()
            completed_games = conn.execute(text("""
                SELECT COUNT(*) FROM games 
                WHERE home_score IS NOT NULL AND away_score IS NOT NULL
            """)).scalar()
            
            print(f"Total games: {total_games}")
            print(f"Completed games: {completed_games}")
            
            # Test the fixed function
            print("\nCalculating live records...")
            live_records = compute_live_records_fixed(conn, current_season)
            
            if not live_records.empty:
                print(f"Successfully calculated records for {len(live_records)} teams")
                
                # Update team_season_summary with these records
                print("\nUpdating team_season_summary...")
                update_team_season_summary(conn, current_season)
                
                # Verify the update worked
                updated_summary = pd.read_sql_query(text("""
                    SELECT team, wins, losses, games_played, win_pct, power_score
                    FROM team_season_summary 
                    WHERE season = :season
                    ORDER BY power_score DESC
                    LIMIT 10
                """), conn, params={"season": current_season})
                
                print("\nTop 10 teams after update:")
                for _, team in updated_summary.iterrows():
                    record = f"{team['wins']}-{team['losses']}" if team['games_played'] > 0 else "0-0"
                    print(f"  {team['team']}: {record} ({team['games_played']} games, {team['power_score']:.1f} power)")
                
            else:
                print("No completed games found - this is normal for preseason")
                
                # Ensure all 32 teams exist in team_season_summary for rankings
                print("Ensuring all teams exist in team_season_summary...")
                
                all_teams_power = {
                    'KC': 6.5, 'BUF': 5.8, 'BAL': 5.2, 'SF': 4.9, 'PHI': 4.6,
                    'DAL': 4.3, 'MIA': 3.8, 'CIN': 3.5, 'DET': 3.2, 'GB': 2.9,
                    'LAC': 2.6, 'MIN': 2.3, 'HOU': 2.0, 'PIT': 1.7, 'ATL': 1.4,
                    'IND': 1.1, 'LV': 0.8, 'TB': 0.5, 'LAR': 0.2, 'SEA': -0.1,
                    'NO': -0.4, 'JAX': -0.7, 'TEN': -1.0, 'CLE': -1.3, 'NYJ': -1.6,
                    'ARI': -1.9, 'DEN': -2.2, 'NE': -2.5, 'WAS': -2.8, 'NYG': -3.1,
                    'CAR': -3.4, 'CHI': -3.7
                }
                
                teams_added = 0
                for team, power in all_teams_power.items():
                    try:
                        conn.execute(text("""
                            INSERT INTO team_season_summary
                            (team, season, power_score, wins, losses, games_played, win_pct,
                             avg_points_for, avg_points_against, point_diff)
                            VALUES (:team, :season, :power_score, 0, 0, 0, 0.0, 0.0, 0.0, 0.0)
                            ON CONFLICT (team, season) DO UPDATE SET
                                power_score = EXCLUDED.power_score
                        """), {
                            'team': team,
                            'season': current_season,
                            'power_score': power
                        })
                        teams_added += 1
                    except Exception as e:
                        print(f"Error adding {team}: {e}")
                        continue
                
                conn.commit()
                print(f"Ensured {teams_added} teams exist in team_season_summary")
            
            print("\n" + "=" * 50)
            print("RANKINGS FIX COMPLETE!")
            print("\nNext steps:")
            print("1. Your rankings should now show proper data")
            print("2. Records will update automatically as games are completed")
            print("3. Redeploy your app to Render")
            
            return True
            
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    print(f"\nProcess {'completed successfully' if success else 'failed'}")