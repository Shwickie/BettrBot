#!/usr/bin/env python3
"""
Fixed Cloud Database Connection Test
Helps you set up and test cloud database properly
"""

import pandas as pd
from sqlalchemy import create_engine, text
import sys
import os

def test_local_database():
    """First test your local database to make sure it works"""
    LOCAL_DB = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
    
    print("Testing local database connection...")
    
    if not os.path.exists("E:/Bettr Bot/betting-bot/data/betting.db"):
        print("❌ Local database file not found at E:/Bettr Bot/betting-bot/data/betting.db")
        return False
    
    try:
        engine = create_engine(LOCAL_DB)
        with engine.connect() as conn:
            # Test basic connection
            result = pd.read_sql(text("SELECT COUNT(*) as count FROM games"), conn)
            games_count = result.iloc[0]['count']
            
            result = pd.read_sql(text("SELECT COUNT(*) as count FROM odds"), conn)
            odds_count = result.iloc[0]['count']
            
            result = pd.read_sql(text("SELECT COUNT(*) as count FROM team_season_summary"), conn)
            teams_count = result.iloc[0]['count']
            
            print(f"✅ Local database connected successfully")
            print(f"   📊 Games: {games_count}")
            print(f"   💰 Odds: {odds_count}")
            print(f"   🏈 Teams: {teams_count}")
            
            return True
            
    except Exception as e:
        print(f"❌ Local database connection failed: {e}")
        return False

def setup_supabase_instructions():
    """Provide step-by-step Supabase setup"""
    print("\n🌟 SUPABASE CLOUD SETUP - STEP BY STEP")
    print("=" * 50)
    
    print("1. CREATE SUPABASE ACCOUNT")
    print("   - Go to https://supabase.com")
    print("   - Click 'Start your project'")
    print("   - Sign up with GitHub (recommended) or email")
    print("   - Create new organization (free)")
    
    print("\n2. CREATE PROJECT")
    print("   - Click 'New Project'")
    print("   - Choose organization")
    print("   - Name: 'bettr-bot-db'")
    print("   - Database Password: Choose a strong password (SAVE THIS!)")
    print("   - Region: Choose closest to you")
    print("   - Click 'Create new project'")
    print("   - Wait 2-3 minutes for setup")
    
    print("\n3. CREATE TABLES")
    print("   - Go to 'SQL Editor' tab in your project")
    print("   - Click 'New Query'")
    print("   - Copy and paste this SQL:")
    
    sql_code = """
-- Create games table
CREATE TABLE games (
    game_id TEXT PRIMARY KEY,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    game_date TEXT NOT NULL,
    season INTEGER,
    week INTEGER,
    home_score INTEGER,
    away_score INTEGER
);

-- Create odds table
CREATE TABLE odds (
    id BIGSERIAL PRIMARY KEY,
    game_id TEXT NOT NULL,
    sportsbook TEXT NOT NULL,
    market TEXT NOT NULL DEFAULT 'h2h',
    team TEXT,
    odds REAL,
    home_odds REAL,
    away_odds REAL,
    home_spread REAL,
    away_spread REAL,
    over_under REAL,
    timestamp TEXT NOT NULL
);

-- Create team season summary table
CREATE TABLE team_season_summary (
    id BIGSERIAL PRIMARY KEY,
    team TEXT NOT NULL,
    season INTEGER NOT NULL,
    wins INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    power_score REAL DEFAULT 0,
    point_diff REAL DEFAULT 0
);

-- Create indexes for performance
CREATE INDEX idx_games_date ON games(game_date);
CREATE INDEX idx_odds_game_id ON odds(game_id);
CREATE INDEX idx_odds_timestamp ON odds(timestamp);
CREATE INDEX idx_team_summary_season ON team_season_summary(season);
"""
    
    print(sql_code)
    print("   - Click 'Run' to execute")
    print("   - You should see 'Success' messages")
    
    print("\n4. GET CONNECTION STRING")
    print("   - Go to 'Settings' > 'Database'")
    print("   - Scroll down to 'Connection string'")
    print("   - Copy the 'URI' connection string")
    print("   - It looks like: postgresql://postgres:[PASSWORD]@db.[PROJECT-ID].supabase.co:5432/postgres")
    
    print("\n5. TEST CONNECTION")
    print("   - Replace 'your_connection_string_here' in this file with your actual string")
    print("   - Run this test again")
    
    print("\n6. UPLOAD DATA")
    print("   - First export your data: python set_up_cloud_database.py")
    print("   - Go to 'Table editor' in Supabase")
    print("   - For each table (games, odds, team_season_summary):")
    print("     * Click on the table name")
    print("     * Click 'Insert' > 'Import data from CSV'")
    print("     * Upload the corresponding CSV file")
    print("     * Map columns correctly")
    print("     * Click 'Save'")

def test_cloud_connection():
    """Test connection to cloud database with proper setup"""
    print("\n🔧 CLOUD DATABASE CONNECTION TEST")
    print("=" * 40)
    
    # Read connection string from environment or file
    cloud_url = None
    
    # Try to read from environment variable
    cloud_url = os.environ.get('SUPABASE_CONNECTION_STRING')
    
    # Try to read from config file
    if not cloud_url and os.path.exists('cloud_config.txt'):
        try:
            with open('cloud_config.txt', 'r') as f:
                cloud_url = f.read().strip()
        except:
            pass
    
    if not cloud_url:
        print("❌ No cloud database connection string found")
        print("\n📝 To fix this, do ONE of these:")
        print("1. Set environment variable:")
        print("   set SUPABASE_CONNECTION_STRING=your_connection_string")
        print("\n2. Create file 'cloud_config.txt' with your connection string")
        print("\n3. Edit this test file and put your connection string in the code")
        
        # Ask user to input connection string
        print("\n🔗 Or paste your connection string now:")
        cloud_url = input("Connection string: ").strip()
        
        if not cloud_url:
            print("❌ No connection string provided")
            return False
        
        # Save to file for future use
        try:
            with open('cloud_config.txt', 'w') as f:
                f.write(cloud_url)
            print("✅ Connection string saved to cloud_config.txt")
        except:
            print("⚠️ Could not save connection string to file")
    
    # Test the connection
    try:
        print(f"🔗 Testing connection to cloud database...")
        engine = create_engine(cloud_url)
        
        with engine.connect() as conn:
            # Test basic connection
            result = conn.execute(text("SELECT 1 as test"))
            print("✅ Basic connection successful")
            
            # Check if tables exist
            try:
                tables_result = pd.read_sql(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name IN ('games', 'odds', 'team_season_summary')
                """), conn)
                
                existing_tables = list(tables_result['table_name'])
                required_tables = ['games', 'odds', 'team_season_summary']
                
                print(f"📋 Found tables: {existing_tables}")
                
                missing_tables = [t for t in required_tables if t not in existing_tables]
                if missing_tables:
                    print(f"❌ Missing tables: {missing_tables}")
                    print("   Please create these tables using the SQL above")
                    return False
                
                # Test each table
                for table in required_tables:
                    try:
                        count_result = pd.read_sql(text(f"SELECT COUNT(*) as count FROM {table}"), conn)
                        count = count_result.iloc[0]['count']
                        print(f"   📊 {table}: {count} records")
                    except Exception as e:
                        print(f"   ❌ {table}: Error reading ({e})")
                
                print("\n🎉 CLOUD DATABASE READY!")
                print("✅ Connection successful")
                print("✅ All tables exist")
                print("✅ Ready to use with dashboard")
                
                # Update dashboard files
                print("\n📝 To use with dashboard, update this line in your dashboard files:")
                print(f'   OLD: DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"')
                print(f'   NEW: DB_PATH = "{cloud_url[:50]}..."')
                
                return True
                
            except Exception as e:
                print(f"❌ Error checking tables: {e}")
                return False
                
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\n🔧 Troubleshooting:")
        print("1. Check your connection string is correct")
        print("2. Make sure you replaced [PASSWORD] with your actual password")
        print("3. Verify your Supabase project is running")
        print("4. Check your internet connection")
        print("5. Try creating the tables again")
        return False

def export_local_data():
    """Export local database to CSV files for cloud upload"""
    LOCAL_DB = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
    
    if not os.path.exists("E:/Bettr Bot/betting-bot/data/betting.db"):
        print("❌ Local database not found - cannot export")
        return False
    
    print("\n📤 EXPORTING DATA FOR CLOUD UPLOAD")
    print("=" * 40)
    
    try:
        engine = create_engine(LOCAL_DB)
        
        # Create export directory
        export_dir = "cloud_export"
        if not os.path.exists(export_dir):
            os.makedirs(export_dir)
        
        with engine.connect() as conn:
            # Export games
            games_df = pd.read_sql(text("SELECT * FROM games"), conn)
            games_df.to_csv(f"{export_dir}/games.csv", index=False)
            print(f"✅ Exported {len(games_df)} games to {export_dir}/games.csv")
            
            # Export odds
            odds_df = pd.read_sql(text("SELECT * FROM odds"), conn)
            odds_df.to_csv(f"{export_dir}/odds.csv", index=False)
            print(f"✅ Exported {len(odds_df)} odds records to {export_dir}/odds.csv")
            
            # Export team summaries
            teams_df = pd.read_sql(text("SELECT * FROM team_season_summary"), conn)
            teams_df.to_csv(f"{export_dir}/team_season_summary.csv", index=False)
            print(f"✅ Exported {len(teams_df)} team records to {export_dir}/team_season_summary.csv")
            
            print(f"\n📁 All files exported to: {export_dir}/")
            print("📤 Upload these CSV files to your Supabase tables")
            
            return True
            
    except Exception as e:
        print(f"❌ Export failed: {e}")
        return False

def main():
    """Main test and setup function"""
    print("☁️ BETTR BOT CLOUD DATABASE SETUP & TEST")
    print("=" * 50)
    
    print("This will help you:")
    print("1. Test your local database")
    print("2. Set up Supabase cloud database")
    print("3. Export and upload your data")
    print("4. Test cloud connection")
    print("5. Update your dashboard")
    
    print("\n" + "=" * 50)
    
    # Step 1: Test local database
    if not test_local_database():
        print("\n❌ Fix your local database first before setting up cloud")
        return
    
    # Step 2: Check if user wants to set up cloud
    print("\n" + "=" * 50)
    choice = input("Do you want to set up cloud database? (y/n): ").lower()
    
    if choice == 'y':
        # Show setup instructions
        setup_supabase_instructions()
        
        # Export data
        export_choice = input("\nExport your data to CSV files for upload? (y/n): ").lower()
        if export_choice == 'y':
            export_local_data()
        
        # Test connection
        input("\nPress Enter after you've set up Supabase and want to test connection...")
        test_cloud_connection()
        
    else:
        print("📱 Your local database is working fine for local dashboard access")
        print("💡 Run the cloud setup anytime with: python test_cloud.py")

if __name__ == "__main__":
    main()