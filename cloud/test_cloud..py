# test_cloud.py - Quick test of cloud database connection
"""
Test if your cloud database connection works
"""

import os
import sys
from sqlalchemy import create_engine, text

# Set the database URL
DATABASE_URL = "postgresql://postgres:YviqtXqcsCIgRzSCofNjbfwgjkYNLydX@maglev.proxy.rlwy.net:54187/railway"

def test_connection():
    print("Testing cloud database connection...")
    
    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        
        with engine.connect() as conn:
            # Test basic connection
            result = conn.execute(text("SELECT 1")).fetchone()
            print("✅ Basic connection works")
            
            # Check what tables exist
            tables_result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """))
            tables = [row[0] for row in tables_result.fetchall()]
            print(f"✅ Found {len(tables)} tables: {tables}")
            
            # Check key tables
            for table in ['odds', 'player_stats_2024', 'current_nfl_players']:
                if table in tables:
                    count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
                    print(f"✅ {table}: {count_result[0]:,} rows")
                else:
                    print(f"❌ Missing table: {table}")
        
        engine.dispose()
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def test_flask_app():
    print("\nTesting Flask app imports...")
    
    # Add paths
    project_root = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(project_root)
    sys.path.insert(0, parent_dir)
    
    try:
        # Test if we can import Flask
        from flask import Flask
        print("✅ Flask import works")
        
        # Test basic app creation
        test_app = Flask(__name__)
        @test_app.route('/test')
        def test():
            return {'status': 'working'}
        
        print("✅ Basic Flask app creation works")
        return True
        
    except Exception as e:
        print(f"❌ Flask test failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 CLOUD SETUP TEST")
    print("=" * 30)
    
    db_ok = test_connection()
    flask_ok = test_flask_app()
    
    print("\n📊 TEST RESULTS:")
    print(f"Database: {'✅ PASS' if db_ok else '❌ FAIL'}")
    print(f"Flask: {'✅ PASS' if flask_ok else '❌ FAIL'}")
    
    if db_ok and flask_ok:
        print("\n🎉 Cloud setup looks good! Ready to deploy.")
    else:
        print("\n⚠️ Some issues found. Fix before deploying.")