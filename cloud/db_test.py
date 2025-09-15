# simple_db_test.py - Quick database connection test
"""
Run this to verify your cloud database is accessible and working
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text

# Your database URL
DATABASE_URL = "postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres"

def test_basic_connection():
    """Test if we can connect at all"""
    print("1. Testing basic connection...")
    
    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).fetchone()
            print("   ✅ Basic connection successful")
            return engine
            
    except Exception as e:
        print(f"   ❌ Connection failed: {e}")
        return None

def test_table_access(engine):
    """Test if we can access your tables"""
    print("2. Testing table access...")
    
    tables_to_check = ['games', 'team_season_summary', 'odds', 'current_nfl_players']
    
    for table in tables_to_check:
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
                count = result[0] if result else 0
                print(f"   ✅ {table}: {count:,} rows")
                
        except Exception as e:
            print(f"   ❌ {table}: {e}")

def test_parameterized_queries(engine):
    """Test the specific query patterns causing issues"""
    print("3. Testing parameterized queries...")
    
    try:
        with engine.connect() as conn:
            # Test 1: Simple parameter
            result = conn.execute(
                text("SELECT COUNT(*) FROM team_season_summary WHERE season = :season"),
                {"season": 2024}
            ).fetchone()
            
            print(f"   ✅ Named parameters work: {result[0]} records for 2024")
            
            # Test 2: DataFrame query (the one failing)
            df = pd.read_sql_query(
                text("SELECT team, power_score FROM team_season_summary WHERE season = :season LIMIT 5"),
                conn,
                params={"season": 2024}
            )
            
            print(f"   ✅ Pandas queries work: {len(df)} teams loaded")
            print(f"   Sample teams: {list(df['team'].head(3))}")
            
    except Exception as e:
        print(f"   ❌ Parameterized query failed: {e}")
        print("   This is likely your core issue!")

def test_rankings_query(engine):
    """Test the specific rankings query that's failing"""
    print("4. Testing rankings query (the failing one)...")
    
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT team, power_score, wins, losses, games_played, win_pct
                FROM team_season_summary 
                WHERE season = :season
                ORDER BY power_score DESC
                LIMIT 10
            """)
            
            df = pd.read_sql_query(query, conn, params={"season": 2024})
            
            if df.empty:
                print("   ⚠️ No data for 2024, trying 2023...")
                df = pd.read_sql_query(query, conn, params={"season": 2023})
            
            if not df.empty:
                print(f"   ✅ Rankings query works: {len(df)} teams")
                print("   Top 3 teams:")
                for _, row in df.head(3).iterrows():
                    print(f"     {row['team']}: {row['power_score']:.1f}")
            else:
                print("   ⚠️ No rankings data found")
                
    except Exception as e:
        print(f"   ❌ Rankings query failed: {e}")

def main():
    print("🔍 CLOUD DATABASE CONNECTION TEST")
    print("=" * 40)
    
    # Test 1: Basic connection
    engine = test_basic_connection()
    if not engine:
        print("\n❌ Cannot proceed - fix connection first")
        return
    
    # Test 2: Table access
    test_table_access(engine)
    
    # Test 3: Parameterized queries
    test_parameterized_queries(engine)
    
    # Test 4: Specific failing query
    test_rankings_query(engine)
    
    print("\n📊 SUMMARY:")
    print("If all tests passed ✅, your database is working!")
    print("If any failed ❌, that's what's breaking your website.")
    print("\nNext steps:")
    print("1. If tests pass: The issue is in your Flask app code")
    print("2. If tests fail: Fix the database connection/queries first")
    
    engine.dispose()

if __name__ == "__main__":
    main()