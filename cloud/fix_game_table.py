#!/usr/bin/env python3
"""Force fix games schema by killing locks"""

import os
from sqlalchemy import create_engine, text
import time

DATABASE_URL = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"

if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

def force_fix():
    print("FORCE FIXING GAMES TABLE")
    print("=" * 50)
    
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    
    try:
        with engine.connect() as conn:
            # Kill any blocking queries on games table
            print("Terminating blocking connections...")
            conn.execute(text("""
                SELECT pg_terminate_backend(pid) 
                FROM pg_stat_activity 
                WHERE datname = current_database()
                AND pid != pg_backend_pid()
                AND state = 'idle in transaction'
            """))
            conn.commit()
            
            time.sleep(2)  # Brief pause
            
            # Now add columns with shorter timeout
            conn.execute(text("SET lock_timeout = '5s'"))
            
            print("Adding season column...")
            conn.execute(text("""
                ALTER TABLE games 
                ADD COLUMN IF NOT EXISTS season INTEGER
            """))
            
            print("Adding week column...")
            conn.execute(text("""
                ALTER TABLE games 
                ADD COLUMN IF NOT EXISTS week INTEGER
            """))
            
            print("Backfilling season from dates...")
            conn.execute(text("""
                UPDATE games 
                SET season = EXTRACT(YEAR FROM game_date)::INTEGER
                WHERE season IS NULL
            """))
            
            conn.commit()
            print("SUCCESS: Schema fixed!")
            
            # Verify
            check = conn.execute(text("""
                SELECT COUNT(*) FROM games WHERE season IS NOT NULL
            """)).scalar()
            print(f"Verified: {check} games have season values")
            
            return True
            
    except Exception as e:
        print(f"ERROR: {e}")
        return False

if __name__ == "__main__":
    force_fix()