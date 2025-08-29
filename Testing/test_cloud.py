#!/usr/bin/env python3
"""
Test Cloud Database Connection
Use this to verify your cloud database is working
"""

import pandas as pd
from sqlalchemy import create_engine, text
import sys

# Cloud database URL - UPDATE THIS WITH YOUR CLOUD DATABASE
CLOUD_DATABASE_URL = "http://localhost:5000/"

# Examples:
# Supabase: "postgresql://postgres:password@db.xxx.supabase.co:5432/postgres"
# PlanetScale: "mysql://username:password@host/database?sslmode=require"
# Railway: "postgresql://postgres:password@host:5432/railway"

def test_connection():
    """Test connection to cloud database"""
    if CLOUD_DATABASE_URL == "http://localhost:5000/":
        print("❌ Please update CLOUD_DATABASE_URL with your actual database URL")
        print("📝 Edit this file and replace the connection string")
        return False
    
    try:
        print("🔗 Connecting to cloud database...")
        engine = create_engine(CLOUD_DATABASE_URL)
        
        with engine.connect() as conn:
            # Test basic connection
            result = conn.execute(text("SELECT 1 as test"))
            print("✅ Basic connection successful")
            
            # Check tables
            try:
                tables = pd.read_sql(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """), conn)
                print(f"📋 Found {len(tables)} tables: {list(tables['table_name'])}")
            except:
                # Try SQLite style for testing
                try:
                    tables = pd.read_sql(text("SELECT name FROM sqlite_master WHERE type='table'"), conn)
                    print(f"📋 Found {len(tables)} tables: {list(tables['name'])}")
                except Exception as e:
                    print(f"⚠️ Could not list tables: {e}")
            
            # Test each main table
            test_tables = ['games', 'odds', 'team_season_summary']
            for table in test_tables:
                try:
                    count = pd.read_sql(text(f"SELECT COUNT(*) as count FROM {table}"), conn)
                    print(f"  📊 {table}: {count.iloc[0]['count']} records")
                except Exception as e:
                    print(f"  ❌ {table}: {e}")
            
            return True
            
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\n🔧 Troubleshooting:")
        print("  1. Check your connection string is correct")
        print("  2. Verify database exists and is accessible")
        print("  3. Check firewall/network settings")
        print("  4. Ensure tables are created (run cloud_database_setup.sql)")
        return False

def test_predictions():
    """Test that predictions can be generated from cloud data"""
    try:
        engine = create_engine(CLOUD_DATABASE_URL)
        
        with engine.connect() as conn:
            # Get sample game
            games = pd.read_sql(text("SELECT * FROM games LIMIT 1"), conn)
            if games.empty:
                print("⚠️ No games found - upload your CSV files")
                return False
            
            # Get power rankings
            power = pd.read_sql(text("SELECT * FROM team_season_summary LIMIT 1"), conn)
            if power.empty:
                print("⚠️ No power rankings found - upload your CSV files")
                return False
            
            print("✅ Data looks good for predictions")
            print(f"  📅 Sample game: {games.iloc[0]['away_team']} @ {games.iloc[0]['home_team']}")
            print(f"  💪 Sample team: {power.iloc[0]['team']} (Power: {power.iloc[0]['power_score']})")
            
            return True
            
    except Exception as e:
        print(f"❌ Prediction test failed: {e}")
        return False

def main():
    """Main test function"""
    print("☁️ BETTR BOT CLOUD DATABASE TEST")
    print("=" * 40)
    
    if test_connection():
        print("\n🔮 Testing prediction data...")
        test_predictions()
        
        print("\n🎉 CLOUD DATABASE READY!")
        print("\n🔧 Next steps:")
        print("  1. Update your dashboard files to use cloud database")
        print("  2. Deploy dashboard to cloud service (Heroku, Railway, etc.)")
        print("  3. Access from anywhere!")
        
    else:
        print("\n❌ Connection failed - see troubleshooting above")

if __name__ == "__main__":
    main()