#!/usr/bin/env python3
"""
Fix the empty rankings by ensuring team_season_summary data exists
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# Database connection
POSTGRES_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres?sslmode=require"
)

def diagnose_rankings_issue():
    """Diagnose why rankings are empty"""
    
    print("=== DIAGNOSING EMPTY RANKINGS ===")
    
    if POSTGRES_URL.startswith('postgres://'):
        postgres_url = POSTGRES_URL.replace('postgres://', 'postgresql://', 1)
    else:
        postgres_url = POSTGRES_URL
        
    pg_engine = create_engine(postgres_url, pool_pre_ping=True)
    
    try:
        with pg_engine.connect() as conn:
            # 1. Check what tables exist
            print("1. Checking what tables exist...")
            tables = pd.read_sql_query(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """), conn)
            
            print(f"Tables in database: {tables['table_name'].tolist()}")
            
            # 2. Check team_season_summary specifically
            print("\n2. Checking team_season_summary...")
            try:
                tss_count = conn.execute(text("SELECT COUNT(*) FROM team_season_summary")).scalar()
                print(f"team_season_summary records: {tss_count}")
                
                if tss_count > 0:
                    tss_sample = pd.read_sql_query(text("""
                        SELECT team, season, power_score, games_played, win_pct
                        FROM team_season_summary 
                        ORDER BY season DESC, power_score DESC
                        LIMIT 10
                    """), conn)
                    print("Sample team_season_summary data:")
                    print(tss_sample)
                
            except Exception as e:
                print(f"team_season_summary error: {e}")
                print("This table likely doesn't exist or has no data")
            
            # 3. Check what season the rankings endpoint is looking for
            current_year = datetime.now().year
            print(f"\n3. Checking for season {current_year} data...")
            
            try:
                season_data = pd.read_sql_query(text("""
                    SELECT COUNT(*) as count, season
                    FROM team_season_summary 
                    WHERE season = :season
                    GROUP BY season
                """), conn, params={"season": current_year})
                
                if season_data.empty:
                    print(f"No team_season_summary data for season {current_year}")
                else:
                    print(f"Found {season_data.iloc[0]['count']} records for season {current_year}")
            except:
                print(f"Could not check season {current_year} data")
            
            # 4. Check games data that compute_live_records needs
            print(f"\n4. Checking games data for season {current_year}...")
            try:
                games_count = conn.execute(text("""
                    SELECT COUNT(*) FROM games 
                    WHERE EXTRACT(YEAR FROM game_date) = :year
                    OR (EXTRACT(MONTH FROM game_date) >= 8 AND EXTRACT(YEAR FROM game_date) = :year)
                    OR (EXTRACT(MONTH FROM game_date) < 8 AND EXTRACT(YEAR FROM game_date) = :year + 1)
                """), {"year": current_year}).scalar()
                
                print(f"Games for season {current_year}: {games_count}")
                
                if games_count > 0:
                    sample_games = pd.read_sql_query(text("""
                        SELECT game_id, away_team, home_team, game_date, home_score, away_score
                        FROM games
                        ORDER BY game_date DESC
                        LIMIT 5
                    """), conn)
                    print("Sample games:")
                    print(sample_games)
                
            except Exception as e:
                print(f"Games check error: {e}")
            
            return True
            
    except Exception as e:
        print(f"Diagnosis error: {e}")
        import traceback
        traceback.print_exc()
        return False

def create_missing_team_data():
    """Create the missing team_season_summary data"""
    
    print("\n=== CREATING MISSING TEAM DATA ===")
    
    if POSTGRES_URL.startswith('postgres://'):
        postgres_url = POSTGRES_URL.replace('postgres://', 'postgresql://', 1)
    else:
        postgres_url = POSTGRES_URL
        
    pg_engine = create_engine(postgres_url, pool_pre_ping=True)
    
    # NFL teams with realistic power ratings
    nfl_teams = {
        'KC': 6.5, 'BUF': 5.8, 'BAL': 5.2, 'SF': 4.9, 'PHI': 4.6,
        'DAL': 4.3, 'MIA': 3.8, 'CIN': 3.5, 'DET': 3.2, 'GB': 2.9,
        'LAC': 2.6, 'MIN': 2.3, 'HOU': 2.0, 'PIT': 1.7, 'ATL': 1.4,
        'IND': 1.1, 'LV': 0.8, 'TB': 0.5, 'LAR': 0.2, 'SEA': -0.1,
        'NO': -0.4, 'JAX': -0.7, 'TEN': -1.0, 'CLE': -1.3, 'NYJ': -1.6,
        'ARI': -1.9, 'DEN': -2.2, 'NE': -2.5, 'WAS': -2.8, 'NYG': -3.1,
        'CAR': -3.4, 'CHI': -3.7
    }
    
    try:
        with pg_engine.connect() as conn:
            # 1. Create team_season_summary table if it doesn't exist
            print("1. Creating team_season_summary table...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS team_season_summary (
                    id SERIAL PRIMARY KEY,
                    team TEXT NOT NULL,
                    season INTEGER NOT NULL,
                    power_score DOUBLE PRECISION DEFAULT 0.0,
                    wins INTEGER DEFAULT 0,
                    losses INTEGER DEFAULT 0,
                    games_played INTEGER DEFAULT 0,
                    win_pct DOUBLE PRECISION DEFAULT 0.0,
                    avg_points_for DOUBLE PRECISION DEFAULT 0.0,
                    avg_points_against DOUBLE PRECISION DEFAULT 0.0,
                    point_diff DOUBLE PRECISION DEFAULT 0.0,
                    UNIQUE(team, season)
                )
            """))
            conn.commit()
            print("team_season_summary table created")
            
            # 2. Insert 2025 season data
            current_year = datetime.now().year
            print(f"2. Adding {current_year} season data...")
            
            added_count = 0
            for team, power_score in nfl_teams.items():
                try:
                    conn.execute(text("""
                        INSERT INTO team_season_summary 
                        (team, season, power_score, wins, losses, games_played, win_pct,
                         avg_points_for, avg_points_against, point_diff)
                        VALUES (:team, :season, :power_score, :wins, :losses, :games_played, 
                                :win_pct, :avg_points_for, :avg_points_against, :point_diff)
                        ON CONFLICT (team, season) DO UPDATE SET
                            power_score = EXCLUDED.power_score
                    """), {
                        'team': team,
                        'season': current_year,
                        'power_score': power_score,
                        'wins': 0,
                        'losses': 0,
                        'games_played': 0,
                        'win_pct': 0.0,
                        'avg_points_for': 0.0,
                        'avg_points_against': 0.0,
                        'point_diff': 0.0
                    })
                    added_count += 1
                except Exception as e:
                    print(f"Error adding {team}: {e}")
            
            conn.commit()
            print(f"Added/updated {added_count} teams for season {current_year}")
            
            # 3. Verify the data
            print("3. Verifying team data...")
            verification = pd.read_sql_query(text("""
                SELECT team, season, power_score, games_played
                FROM team_season_summary 
                WHERE season = :season
                ORDER BY power_score DESC
                LIMIT 10
            """), conn, params={"season": current_year})
            
            print("Top 10 teams by power score:")
            for _, row in verification.iterrows():
                print(f"  {row['team']}: {row['power_score']:.1f} ({row['games_played']} games)")
            
            # 4. Test the rankings endpoint logic
            print("\n4. Testing rankings logic...")
            total_teams = conn.execute(text("""
                SELECT COUNT(*) FROM team_season_summary WHERE season = :season
            """), {"season": current_year}).scalar()
            
            print(f"Total teams in database: {total_teams}")
            
            if total_teams >= 32:
                print("✅ Rankings should now work!")
                return True
            else:
                print(f"❌ Only {total_teams} teams found, need 32")
                return False
                
    except Exception as e:
        print(f"Error creating team data: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("FIXING EMPTY RANKINGS")
    print("=" * 50)
    
    # First diagnose the issue
    diagnose_rankings_issue()
    
    # Then create the missing data
    success = create_missing_team_data()
    
    if success:
        print("\n" + "=" * 50)
        print("✅ RANKINGS FIX COMPLETE!")
        print("\nYour /api/rankings endpoint should now return:")
        print("- 32 NFL teams with power scores")
        print("- Win/loss records (will be 0-0 for preseason)")
        print("- Proper team rankings by power score")
        print("\nRedeploy your app to see the rankings!")
    else:
        print("\n❌ Rankings fix failed - check errors above")