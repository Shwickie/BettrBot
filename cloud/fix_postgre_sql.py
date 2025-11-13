#!/usr/bin/env python3
"""
Fix PostgreSQL constraints for team_season_summary table
This fixes the empty rankings issue by ensuring proper database constraints
"""

import os
import sys
from sqlalchemy import create_engine, text
import pandas as pd

# Add project root to path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

def fix_postgresql_constraints():
    """Fix the missing unique constraint causing the rankings issue"""
    
    # Get database URL
    DATABASE_URL = "postgresql://postgres:YviqtXqcsCIgRzSCofNjbfwgjkYNLydX@maglev.proxy.rlwy.net:54187/railway"
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://", 1)
    
    if not DATABASE_URL.startswith(("postgresql://", "postgresql+psycopg2://")):
        print("❌ This fix is only for PostgreSQL databases")
        return False
    
    print("FIXING POSTGRESQL CONSTRAINTS FOR RANKINGS")
    print("=" * 50)
    
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    
    try:
        with engine.connect() as conn:
            # Check current table structure
            print("1. Checking current table structure...")
            
            # Check if table exists
            table_exists = conn.execute(text("""
                SELECT to_regclass('public.team_season_summary') IS NOT NULL
            """)).scalar()
            
            if not table_exists:
                print("❌ team_season_summary table doesn't exist!")
                return False
            
            # Check current constraints
            constraints = conn.execute(text("""
                SELECT conname, contype 
                FROM pg_constraint 
                WHERE conrelid = 'team_season_summary'::regclass
            """)).fetchall()
            
            print(f"Current constraints: {[c[0] for c in constraints]}")
            
            # Check if we have the unique constraint we need
            has_unique_constraint = any(
                'team' in c[0] and 'season' in c[0] and c[1] == 'u' 
                for c in constraints
            )
            
            if has_unique_constraint:
                print("✅ Unique constraint already exists")
            else:
                print("2. Adding unique constraint for (team, season)...")
                
                # Add the unique constraint
                conn.execute(text("""
                    ALTER TABLE team_season_summary 
                    ADD CONSTRAINT unique_team_season 
                    UNIQUE (team, season)
                """))
                conn.commit()
                print("✅ Added unique constraint")
            
            # Check current data
            print("3. Checking current data...")
            current_data = pd.read_sql(text("""
                SELECT team, season, power_score, games_played, win_pct
                FROM team_season_summary 
                WHERE season = 2025
                ORDER BY power_score DESC
            """), conn)
            
            print(f"Found {len(current_data)} teams for season 2025")
            
            if len(current_data) < 32:
                print("4. Adding missing teams...")
                
                # Define all 32 NFL teams with preseason power scores
                all_teams = {
                    'KC': 6.5, 'BUF': 5.8, 'BAL': 5.2, 'SF': 4.9, 'PHI': 4.6,
                    'DAL': 4.3, 'MIA': 3.8, 'CIN': 3.5, 'DET': 3.2, 'GB': 2.9,
                    'LAC': 2.6, 'MIN': 2.3, 'HOU': 2.0, 'PIT': 1.7, 'ATL': 1.4,
                    'IND': 1.1, 'LV': 0.8, 'TB': 0.5, 'LAR': 0.2, 'SEA': -0.1,
                    'NO': -0.4, 'JAX': -0.7, 'TEN': -1.0, 'CLE': -1.3, 'NYJ': -1.6,
                    'ARI': -1.9, 'DEN': -2.2, 'NE': -2.5, 'WAS': -2.8, 'NYG': -3.1,
                    'CAR': -3.4, 'CHI': -3.7
                }
                
                existing_teams = set(current_data['team'].tolist())
                
                for team, power in all_teams.items():
                    if team not in existing_teams:
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
                            'season': 2025,
                            'power_score': power,
                            'wins': 0,
                            'losses': 0,
                            'games_played': 0,
                            'win_pct': 0.0,
                            'avg_points_for': 0.0,
                            'avg_points_against': 0.0,
                            'point_diff': 0.0
                        })
                
                conn.commit()
                print(f"✅ Added missing teams")
            
            # Verify final state
            print("5. Verifying final state...")
            final_data = pd.read_sql(text("""
                SELECT team, season, power_score, games_played, win_pct
                FROM team_season_summary 
                WHERE season = 2025
                ORDER BY power_score DESC
                LIMIT 10
            """), conn)
            
            print(f"Final verification - Top 10 teams:")
            for _, row in final_data.iterrows():
                print(f"  {row['team']}: {row['power_score']} power ({row['games_played']} games)")
            
            # Test the rankings query specifically
            print("6. Testing rankings query...")
            test_query = """
                SELECT team, power_score as power, games_played, win_pct, 0 as wins, 0 as losses, 0 as ties,
                       0 as point_diff, 0.0 as injury_impact
                FROM team_season_summary 
                WHERE season = 2025
                ORDER BY power_score DESC, win_pct DESC, point_diff DESC
            """
            
            rankings_test = pd.read_sql(text(test_query), conn)
            print(f"✅ Rankings query successful - {len(rankings_test)} teams returned")
            
            return True
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = fix_postgresql_constraints()
    if success:
        print("\n" + "=" * 50)
        print("✅ POSTGRESQL CONSTRAINTS FIXED!")
        print("\nYour rankings should now work:")
        print("- Unique constraint added to team_season_summary")
        print("- All 32 NFL teams populated with preseason data")
        print("- Rankings API should return proper data")
        print("\nRedeploy your app to see the changes!")
    else:
        print("\n❌ Fix failed - check the errors above")