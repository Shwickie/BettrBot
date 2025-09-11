# migrate_to_cloud.py - FIXED VERSION
"""
Migrate SQLite database to cloud PostgreSQL (Supabase)
Run this once to move all your data online
"""

import sqlite3
import psycopg2
import os
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime

# Your current SQLite database
SQLITE_PATH = r"E:\Bettr Bot\betting-bot\data\betting.db"

# Supabase connection - YOUR ACTUAL CREDENTIALS
POSTGRES_URL = "postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres"

def migrate_table_simple(table_name, batch_size=1000):
    """Simple table migration using pandas"""
    print(f"📊 Migrating table: {table_name}")
    
    try:
        # Read from SQLite using pandas
        sqlite_conn = sqlite3.connect(SQLITE_PATH)
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", sqlite_conn)
        sqlite_conn.close()
        
        if df.empty:
            print(f"   ⚠️ Table {table_name} is empty, skipping")
            return True
        
        print(f"   📈 {len(df):,} rows to migrate")
        
        # Create PostgreSQL engine
        postgres_engine = create_engine(POSTGRES_URL, pool_pre_ping=True)
        
        # Write to PostgreSQL in batches
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            batch.to_sql(table_name, postgres_engine, if_exists='append', index=False, method='multi')
            print(f"   📤 {min(i+batch_size, len(df)):,}/{len(df):,} rows migrated ({min(i+batch_size, len(df))/len(df)*100:.1f}%)")
        
        postgres_engine.dispose()
        print(f"   ✅ {table_name} migration complete")
        return True
        
    except Exception as e:
        print(f"   ❌ Error migrating {table_name}: {e}")
        return False

def migrate_essential_missing():
    """Migrate the critical missing tables"""
    print("🔧 FIXING MISSING ESSENTIAL DATA")
    print("=" * 40)
    
    # Tables we need for the dashboard to work
    critical_tables = {
        'games': 'Game schedules and scores',
        'team_season_summary': 'Team power rankings and stats'
    }
    
    success_count = 0
    
    for table_name, description in critical_tables.items():
        print(f"\n📊 Migrating {table_name} ({description})")
        
        try:
            # Read from SQLite
            sqlite_conn = sqlite3.connect(SQLITE_PATH)
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", sqlite_conn)
            sqlite_conn.close()
            
            if df.empty:
                print(f"   ⚠️ {table_name} is empty in local database")
                continue
            
            print(f"   📈 Found {len(df):,} rows")
            
            # Write to PostgreSQL
            postgres_engine = create_engine(POSTGRES_URL, pool_pre_ping=True)
            
            # Clear existing data first
            with postgres_engine.begin() as conn:
                conn.execute(text(f"DELETE FROM {table_name}"))
                print(f"   🗑️ Cleared existing {table_name} data")
            
            # Insert new data in batches
            batch_size = 1000
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                batch.to_sql(table_name, postgres_engine, if_exists='append', index=False, method='multi')
                print(f"   📤 {min(i+batch_size, len(df)):,}/{len(df):,} rows migrated")
            
            postgres_engine.dispose()
            print(f"   ✅ {table_name} migration complete")
            success_count += 1
            
        except Exception as e:
            print(f"   ❌ Error migrating {table_name}: {e}")
    
    print(f"\n🎉 ESSENTIAL DATA FIX COMPLETE")
    print(f"✅ Successful: {success_count}/{len(critical_tables)}")
    
    # Verify the fix
    print(f"\n🔍 Verifying fix...")
    try:
        engine = create_engine(POSTGRES_URL, pool_pre_ping=True)
        with engine.connect() as conn:
            for table in ['games', 'team_season_summary', 'odds']:
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
                    print(f"   📊 {table}: {result[0]:,} rows")
                except Exception as e:
                    print(f"   ❌ {table}: Error - {e}")
        engine.dispose()
    except Exception as e:
        print(f"   ❌ Verification failed: {e}")

def create_essential_tables():
    """Create essential tables in PostgreSQL"""
    print("🏗️ Creating essential table schemas...")
    
    # Essential table schemas (simplified)
    schemas = {
        'games': '''
            CREATE TABLE IF NOT EXISTS games (
                id SERIAL PRIMARY KEY,
                game_id TEXT,
                home_team TEXT,
                away_team TEXT,
                game_date DATE,
                start_time_local TIME,
                home_score INTEGER,
                away_score INTEGER,
                season INTEGER
            )
        ''',
        'odds': '''
            CREATE TABLE IF NOT EXISTS odds (
                id SERIAL PRIMARY KEY,
                game_id TEXT,
                team TEXT,
                sportsbook TEXT,
                odds REAL,
                market TEXT,
                timestamp TIMESTAMP
            )
        ''',
        'team_season_summary': '''
            CREATE TABLE IF NOT EXISTS team_season_summary (
                id SERIAL PRIMARY KEY,
                team TEXT,
                season INTEGER,
                power_score REAL,
                wins INTEGER,
                losses INTEGER,
                games_played INTEGER,
                win_pct REAL,
                avg_points_for REAL,
                avg_points_against REAL,
                point_diff REAL
            )
        ''',
        'system_status': '''
            CREATE TABLE IF NOT EXISTS system_status (
                id SERIAL PRIMARY KEY,
                task TEXT,
                started_at TEXT,
                finished_at TEXT,
                status TEXT,
                message TEXT,
                run_type TEXT DEFAULT 'cloud',
                timeout_seconds INTEGER
            )
        '''
    }
    
    try:
        engine = create_engine(POSTGRES_URL, pool_pre_ping=True)
        with engine.begin() as conn:
            for table_name, schema in schemas.items():
                conn.execute(text(schema))
                print(f"   ✅ Created table: {table_name}")
        engine.dispose()
        return True
    except Exception as e:
        print(f"   ❌ Schema creation failed: {e}")
        return False

def main():
    print("🚀 MIGRATING BETTR BOT TO CLOUD DATABASE")
    print("=" * 50)
    
    # Test connections
    try:
        # Test SQLite
        sqlite_conn = sqlite3.connect(SQLITE_PATH)
        sqlite_conn.execute("SELECT 1")
        sqlite_conn.close()
        
        # Test PostgreSQL
        postgres_engine = create_engine(POSTGRES_URL, pool_pre_ping=True)
        with postgres_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        postgres_engine.dispose()
        
        print("✅ Database connections established")
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return
    
    # Create essential schemas first
    if not create_essential_tables():
        print("❌ Failed to create schemas")
        return
    
    # Get tables from SQLite
    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    cursor = sqlite_conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    sqlite_conn.close()
    migrate_essential_missing()
    
    print(f"📋 Found {len(tables)} tables to migrate")
    
    # Migrate essential tables first
    essential_tables = ['games', 'odds', 'team_season_summary', 'system_status', 'player_stats_2024', 'current_nfl_players']
    
    successful = 0
    failed = 0
    
    print("\n📦 Migrating essential tables...")
    for table in essential_tables:
        if table in tables:
            success = migrate_table_simple(table)
            if success:
                successful += 1
            else:
                failed += 1
    
    print(f"\n🎉 ESSENTIAL MIGRATION COMPLETE")
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    
    # Test the migration
    print("\n🔍 Verifying migration...")
    try:
        engine = create_engine(POSTGRES_URL, pool_pre_ping=True)
        with engine.connect() as conn:
            for table in ['games', 'odds', 'team_season_summary']:
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
                    print(f"   📊 {table}: {result[0]:,} rows")
                except Exception as e:
                    print(f"   ❌ {table}: Error - {e}")
        engine.dispose()
    except Exception as e:
        print(f"   ❌ Verification failed: {e}")
    
    print("\n🌐 Next Steps:")
    print("1. Test your app.py locally")
    print("2. Push to GitHub") 
    print("3. Deploy to Render")
    print("4. Your dashboard will work with cloud data!")

if __name__ == "__main__":
    main()