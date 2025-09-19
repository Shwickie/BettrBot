#!/usr/bin/env python3
"""
Fixed odds migration that handles game_id mismatches and orphaned records
"""

import os
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

def fix_odds_migration():
    """Fix the odds migration by cleaning data first"""
    
    # Connect to local SQLite
    SQLITE_PATH = r"E:\Bettr Bot\betting-bot\data\betting.db"
    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    
    # Connect to cloud PostgreSQL
    POSTGRES_URL = os.getenv(
        "POSTGRES_URL",
        "postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres?sslmode=require",
    )
    pg_engine = create_engine(POSTGRES_URL, pool_pre_ping=True)
    
    print("=== FIXING ODDS MIGRATION ===")
    
    # Step 1: Clean up local odds data first
    print("\n1. Cleaning local odds data...")
    
    # Get valid game_ids from games table
    valid_games = pd.read_sql_query("""
        SELECT DISTINCT game_id 
        FROM games 
        WHERE game_id IS NOT NULL 
        AND game_id != ''
        AND date(game_date) >= date('now', '-30 days')
    """, sqlite_conn)
    
    valid_game_ids = set(valid_games['game_id'].tolist())
    print(f"Found {len(valid_game_ids)} valid game IDs")
    
    # Get all odds and filter to valid games only
    all_odds = pd.read_sql_query("""
        SELECT * FROM odds 
        WHERE game_id IS NOT NULL 
        AND game_id != ''
        AND market = 'h2h'
        ORDER BY timestamp DESC
    """, sqlite_conn)
    
    print(f"Total odds before cleaning: {len(all_odds)}")
    
    # Filter to only odds with valid game_ids
    clean_odds = all_odds[all_odds['game_id'].isin(valid_game_ids)].copy()
    print(f"Clean odds after filtering: {len(clean_odds)}")
    
    # Remove duplicates (keep latest by timestamp)
    clean_odds = clean_odds.sort_values('timestamp').drop_duplicates(
        ['game_id', 'team', 'sportsbook', 'market'], keep='last'
    )
    print(f"Odds after deduplication: {len(clean_odds)}")
    
    # Step 2: Normalize odds data for PostgreSQL
    print("\n2. Normalizing odds data...")
    
    # Ensure proper data types
    clean_odds['odds'] = pd.to_numeric(clean_odds['odds'], errors='coerce')
    clean_odds['timestamp'] = pd.to_datetime(clean_odds['timestamp'], errors='coerce')
    
    # Remove rows with invalid odds or timestamps
    before_validation = len(clean_odds)
    clean_odds = clean_odds.dropna(subset=['odds', 'timestamp'])
    clean_odds = clean_odds[clean_odds['odds'].between(-2000, 2000)]  # Reasonable odds range
    print(f"Odds after validation: {len(clean_odds)} (removed {before_validation - len(clean_odds)} invalid records)")
    
    # Step 3: Upload to cloud
    print("\n3. Uploading to cloud PostgreSQL...")
    
    with pg_engine.connect() as conn:
        # Clear existing odds
        conn.execute(text("TRUNCATE TABLE odds RESTART IDENTITY"))
        print("Cleared existing odds from cloud database")
        
        # Upload in batches to avoid parameter limits
        batch_size = 200  # Small batches for odds
        total_batches = (len(clean_odds) + batch_size - 1) // batch_size
        
        for i, start in enumerate(range(0, len(clean_odds), batch_size)):
            batch = clean_odds.iloc[start:start + batch_size]
            
            batch.to_sql(
                'odds',
                conn,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            print(f"Uploaded batch {i+1}/{total_batches} ({len(batch)} records)")
        
        conn.commit()
        
        # Verify upload
        total_uploaded = conn.execute(text("SELECT COUNT(*) FROM odds")).scalar()
        print(f"\nVerification: {total_uploaded} odds records in cloud database")
        
        # Check sample
        sample = pd.read_sql(text("""
            SELECT game_id, team, sportsbook, odds, market
            FROM odds 
            ORDER BY timestamp DESC 
            LIMIT 10
        """), conn)
        print("\nSample uploaded odds:")
        print(sample)
        
        # Check games with odds
        games_with_odds = conn.execute(text("""
            SELECT COUNT(DISTINCT g.game_id) 
            FROM games g
            INNER JOIN odds o ON g.game_id = o.game_id
            WHERE g.game_date >= CURRENT_DATE
        """)).scalar()
        print(f"\nUpcoming games with odds: {games_with_odds}")
    
    sqlite_conn.close()
    print("\n✅ ODDS MIGRATION FIXED!")
    print("Your cloud database should now have proper odds data.")

if __name__ == "__main__":
    fix_odds_migration()