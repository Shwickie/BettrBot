# restore_real_rankings.py - Restore real power rankings from SQLite
"""
Your cloud rankings got overwritten with fake data by fix_cloud_rankings.py
This restores the real rankings from your SQLite database.
"""

import os
import pandas as pd
import sqlite3
from sqlalchemy import create_engine, text

# Database connections
SQLITE_PATH = r"E:\Bettr Bot\betting-bot\data\betting.db"
DATABASE_URL = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

def main():
    """Restore real power rankings from SQLite to cloud"""
    print("RESTORING REAL POWER RANKINGS")
    print("=" * 40)
    
    try:
        # Connect to SQLite (your real data)
        sqlite_conn = sqlite3.connect(SQLITE_PATH)
        
        # Connect to cloud database
        cloud_engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,
            pool_recycle=280,
            pool_timeout=30,
            connect_args={
                "sslmode": "require",
                "connect_timeout": 30,
                "application_name": "restore_rankings"
            }
        )
        
        # Read REAL rankings from SQLite
        print("Reading real rankings from SQLite...")
        real_rankings = pd.read_sql_query("""
            SELECT team, season, power_score, wins, losses, games_played, win_pct,
                   avg_points_for, avg_points_against, point_diff
            FROM team_season_summary
            WHERE season = 2025
        """, sqlite_conn)
        
        print(f"Found {len(real_rankings)} real team records for 2025")
        
        if real_rankings.empty:
            print("No 2025 data in SQLite, checking 2024...")
            real_rankings = pd.read_sql_query("""
                SELECT team, 2025 as season, power_score, 0 as wins, 0 as losses, 
                       0 as games_played, 0.0 as win_pct, 0.0 as avg_points_for, 
                       0.0 as avg_points_against, 0.0 as point_diff
                FROM team_season_summary
                WHERE season = 2024
            """, sqlite_conn)
            print(f"Using {len(real_rankings)} teams from 2024 as baseline")
        
        if real_rankings.empty:
            print("ERROR: No team data found in SQLite")
            return False
        
        # Show sample of real data
        print("\nSample of REAL power rankings:")
        top_5 = real_rankings.nlargest(5, 'power_score')
        for _, row in top_5.iterrows():
            print(f"  {row['team']}: {row['power_score']:.1f} power")
        
        # Clear fake data and restore real data
        print("\nReplacing fake rankings with real data...")
        with cloud_engine.begin() as conn:
            # Delete 2025 fake data
            deleted = conn.execute(text("""
                DELETE FROM team_season_summary WHERE season = 2025
            """)).rowcount
            print(f"Deleted {deleted} fake records")
            
            # Insert real data
            real_rankings.to_sql(
                'team_season_summary',
                conn,
                if_exists='append',
                index=False,
                method='multi'
            )
            print(f"Restored {len(real_rankings)} real team records")
        
        # Verify restoration
        with cloud_engine.connect() as conn:
            verification = pd.read_sql_query(text("""
                SELECT team, power_score, wins, losses, games_played
                FROM team_season_summary 
                WHERE season = 2025
                ORDER BY power_score DESC
                LIMIT 10
            """), conn)
            
            print(f"\nTop 10 teams after restoration:")
            for i, row in verification.iterrows():
                print(f"  {i+1}. {row['team']}: {row['power_score']:.1f} power, {row['wins']}-{row['losses']} record")
        
        sqlite_conn.close()
        
        print("\n" + "=" * 50)
        print("SUCCESS: Real power rankings restored!")
        print("\nYour dashboard should now show correct rankings")
        print("The fake Indianapolis Colts #1 ranking is gone")
        
        return True
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    print(f"\nProcess {'completed successfully' if success else 'failed'}")