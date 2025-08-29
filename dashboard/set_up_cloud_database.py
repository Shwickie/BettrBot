#!/usr/bin/env python3
"""
Simple Cloud Database Setup for Bettr Bot
Export your data and get setup instructions
"""

import pandas as pd
import sqlite3
import os

# Your local database
LOCAL_DB = "E:/Bettr Bot/betting-bot/data/betting.db"

def export_data():
    """Export your database to CSV files"""
    print("📤 Exporting your database to CSV files...")
    
    if not os.path.exists(LOCAL_DB):
        print(f"❌ Database not found: {LOCAL_DB}")
        return False
    
    # Create export folder
    export_folder = "cloud_export"
    if not os.path.exists(export_folder):
        os.makedirs(export_folder)
    
    try:
        conn = sqlite3.connect(LOCAL_DB)
        
        # Export games
        games_df = pd.read_sql_query("SELECT * FROM games", conn)
        games_df.to_csv(f"{export_folder}/games.csv", index=False)
        print(f"✅ Exported games: {len(games_df)} records")
        
        # Export odds
        odds_df = pd.read_sql_query("SELECT * FROM odds", conn)
        odds_df.to_csv(f"{export_folder}/odds.csv", index=False)
        print(f"✅ Exported odds: {len(odds_df)} records")
        
        # Export team summaries
        teams_df = pd.read_sql_query("SELECT * FROM team_season_summary", conn)
        teams_df.to_csv(f"{export_folder}/team_season_summary.csv", index=False)
        print(f"✅ Exported teams: {len(teams_df)} records")
        
        conn.close()
        
        print(f"\n📁 All files exported to: {export_folder}/")
        return True
        
    except Exception as e:
        print(f"❌ Export failed: {e}")
        return False

def create_supabase_guide():
    """Create simple Supabase setup guide"""
    guide = """
SUPABASE SETUP - STEP BY STEP
================================

1. Create Account
   - Go to https://supabase.com
   - Sign up with GitHub or email
   - Create new project (choose free tier)
   - Wait for project to be ready

2. Create Tables
   - Go to "SQL Editor" in your Supabase dashboard
   - Copy and paste this SQL:

CREATE TABLE games (
    game_id TEXT PRIMARY KEY,
    home_team TEXT,
    away_team TEXT,
    game_date TEXT,
    season INTEGER,
    week INTEGER,
    home_score INTEGER,
    away_score INTEGER
);

CREATE TABLE odds (
    id SERIAL PRIMARY KEY,
    game_id TEXT,
    sportsbook TEXT,
    home_odds REAL,
    away_odds REAL,
    home_spread REAL,
    away_spread REAL,
    over_under REAL,
    timestamp TEXT
);

CREATE TABLE team_season_summary (
    id SERIAL PRIMARY KEY,
    team TEXT,
    season INTEGER,
    wins INTEGER,
    losses INTEGER,
    power_score REAL,
    point_diff REAL
);

3. Import Data
   - Go to "Table editor"
   - Click on "games" table
   - Click "Insert" > "Import data from CSV"
   - Upload games.csv from cloud_export folder
   - Repeat for odds and team_season_summary tables

4. Get Connection String
   - Go to Settings > Database
   - Copy the connection string
   - Looks like: postgresql://postgres:password@db.xxx.supabase.co:5432/postgres

5. Update Your Code
   - Replace this line in your dashboard files:
   
   OLD: DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
   NEW: DB_PATH = "your_supabase_connection_string_here"

6. Test
   - Run your dashboard
   - Should work from anywhere now!

Benefits:
- Works when your PC is off
- Access from anywhere in the world
- Fast and reliable
- Free tier includes 500MB database
"""
    
    with open("SUPABASE_SETUP.txt", "w", encoding='utf-8') as f:
        f.write(guide)
    
    print("✅ Created SUPABASE_SETUP.txt")

def main():
    print("BETTR BOT CLOUD SETUP")
    print("=" * 30)
    
    # Export data
    if export_data():
        # Create guide
        create_supabase_guide()
        
        print("\nSETUP COMPLETE!")
        print("\nWhat you got:")
        print("  - cloud_export/ - Your data as CSV files")
        print("  - SUPABASE_SETUP.txt - Step-by-step instructions")
        
        print("\nNext steps:")
        print("  1. Open SUPABASE_SETUP.txt")
        print("  2. Follow the steps to create Supabase account")
        print("  3. Upload your CSV files")
        print("  4. Update your dashboard code")
        print("  5. Access from anywhere!")
        
        print("\nWhy Supabase?")
        print("  - Free tier (500MB database)")
        print("  - PostgreSQL (powerful)")
        print("  - Real-time updates")
        print("  - Easy to use dashboard")
        print("  - Works from anywhere")
        
        print(f"\nYour database stats:")
        print(f"  - Total size: Large (1411 games, 3043 odds, 160 teams)")
        print(f"  - Should fit easily in 500MB free tier")
    
    else:
        print("Export failed - check your database path")

if __name__ == "__main__":
    main()