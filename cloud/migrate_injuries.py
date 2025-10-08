#!/usr/bin/env python3
"""
Migrate working injury data from SQLite to PostgreSQL cloud
FIXES: Proper timestamp handling for PostgreSQL
"""

import pandas as pd
from sqlalchemy import create_engine, text
import sqlite3
from datetime import datetime

# Local SQLite
LOCAL_DB = r"E:/Bettr Bot/betting-bot/data/betting.db"

# Cloud PostgreSQL
CLOUD_DB = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"

def migrate_injuries():
    """Copy injury data from local to cloud with proper types"""
    print("MIGRATING INJURY DATA TO CLOUD")
    print("=" * 50)
    
    # Get local injuries
    local_conn = sqlite3.connect(LOCAL_DB)
    injuries = pd.read_sql_query("""
        SELECT player_name, player_id, team, position, designation,
               is_active, confidence_score, last_updated, notes
        FROM nfl_injuries_tracking
        WHERE is_active = 1
    """, local_conn)
    local_conn.close()
    
    print(f"Found {len(injuries)} active injuries in local DB")
    
    # FIX: Convert last_updated to proper datetime
    injuries['last_updated'] = pd.to_datetime(injuries['last_updated'], errors='coerce')
    # After line 20, add:
    injuries['is_active'] = injuries['is_active'].astype(bool)  # FIX: Convert to boolean
    injuries['last_updated'] = pd.to_datetime(injuries['last_updated'], errors='coerce')
    
    # Upload to cloud
    cloud_engine = create_engine(
        CLOUD_DB, 
        pool_pre_ping=True,
        pool_recycle=280
    )
    
    with cloud_engine.begin() as conn:
        # Drop and recreate table with correct types
        conn.execute(text("DROP TABLE IF EXISTS nfl_injuries_tracking"))
        
        conn.execute(text("""
            CREATE TABLE nfl_injuries_tracking (
                id SERIAL PRIMARY KEY,
                player_name TEXT,
                player_id TEXT,
                team TEXT,
                position TEXT,
                designation TEXT,
                is_active BOOLEAN DEFAULT true,
                confidence_score REAL,
                last_updated TIMESTAMP,  -- FIXED: Proper timestamp type
                notes TEXT
            )
        """))
        
        print("Created fresh injury table with correct types")
        
        # Insert data
        injuries.to_sql('nfl_injuries_tracking', conn, if_exists='append', index=False)
        print(f"✅ Uploaded {len(injuries)} injuries to cloud")
        
        # Create validation detail table (for your dashboard)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ai_injury_validation_detail (
                id SERIAL PRIMARY KEY,
                team_ai TEXT,
                inj_name TEXT,
                position TEXT,
                designation TEXT,
                inj_missing_team INTEGER DEFAULT 0,
                roster_missing_team INTEGER DEFAULT 0,
                team_mismatch INTEGER DEFAULT 0
            )
        """))
        
        # Populate validation table from injuries
        conn.execute(text("""
            INSERT INTO ai_injury_validation_detail 
                (team_ai, inj_name, position, designation)
            SELECT team, player_name, position, designation
            FROM nfl_injuries_tracking
            WHERE is_active = true
            ON CONFLICT DO NOTHING
        """))
        
        # Verify
        count = conn.execute(text("SELECT COUNT(*) FROM nfl_injuries_tracking")).scalar()
        print(f"Verified: {count} injuries now in cloud database")
        
        # Show sample
        sample = conn.execute(text("""
            SELECT team, COUNT(*) as injury_count
            FROM nfl_injuries_tracking
            WHERE is_active = true
            GROUP BY team
            ORDER BY injury_count DESC
            LIMIT 5
        """)).fetchall()
        
        print("\nTop 5 teams by injury count:")
        for team, count in sample:
            print(f"  {team}: {count} injuries")

if __name__ == "__main__":
    migrate_injuries()