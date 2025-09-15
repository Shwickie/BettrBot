# diagnose_real_issue.py - Find the actual problem
"""
Your database works fine. Let's find what's really breaking.
"""

import os
import sys

def check_actual_mobile_dashboard():
    """Look at the actual mobile_dashboard.py to see what's wrong"""
    
    file_path = "mobile_dashboard.py"
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    print("ANALYZING MOBILE_DASHBOARD.PY")
    print("=" * 40)
    
    # Check for the error patterns from your logs
    issues = []
    
    # Look for the specific error pattern
    if 'params=[' in content:
        print("Found SQLite-style parameter lists")
        # Count occurrences
        import re
        matches = re.findall(r'params=\[[^\]]+\]', content)
        print(f"  Found {len(matches)} SQLite-style parameter patterns")
        for i, match in enumerate(matches[:5]):  # Show first 5
            print(f"    {i+1}: {match}")
        issues.append("SQLite parameter syntax")
    
    # Check if text() is imported
    if 'from sqlalchemy import' in content:
        if ', text' not in content:
            print("SQLAlchemy text() not imported")
            issues.append("Missing text() import")
        else:
            print("SQLAlchemy text() is imported")
    
    # Look for the specific failing queries from logs
    failing_patterns = [
        "Dashboard stats error",
        "Top team calculation error", 
        "Error in betting analysis",
        "Rankings query error"
    ]
    
    for pattern in failing_patterns:
        if pattern.lower() in content.lower():
            print(f"Found reference to: {pattern}")
    
    return issues

def create_minimal_test():
    """Create a minimal test to reproduce the exact error"""
    
    test_content = '''# minimal_test.py - Reproduce the exact error
import os
os.environ['DATABASE_URL'] = 'postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres'

import pandas as pd
from sqlalchemy import create_engine, text

# Test the exact pattern that's failing
def test_rankings_query():
    print("Testing the rankings query that's failing...")
    
    try:
        DATABASE_URL = os.environ['DATABASE_URL']
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        
        with engine.connect() as conn:
            # This is likely the failing pattern
            season = 2024
            
            # OLD WAY (probably what's in your code)
            try:
                df_old = pd.read_sql_query(
                    "SELECT team, power_score FROM team_season_summary WHERE season = ?",
                    conn, 
                    params=[season]
                )
                print("OLD WAY works (unexpected!)")
            except Exception as e:
                print(f"OLD WAY fails: {e}")
            
            # NEW WAY (what should work)
            try:
                df_new = pd.read_sql_query(
                    text("SELECT team, power_score FROM team_season_summary WHERE season = :season"),
                    conn, 
                    params={"season": season}
                )
                print(f"NEW WAY works: {len(df_new)} teams")
            except Exception as e:
                print(f"NEW WAY fails: {e}")
                
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    test_rankings_query()
'''
    
    with open('minimal_test.py', 'w', encoding='utf-8') as f:
        f.write(test_content)
    
    print("Created minimal_test.py")

def find_exact_error_location():
    """Try to start the Flask app and see where it breaks"""
    
    startup_test = '''# startup_test.py - See where Flask app breaks
import os
os.environ['DATABASE_URL'] = 'postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres'

print("Testing Flask app startup...")

try:
    print("1. Importing mobile_dashboard...")
    from mobile_dashboard import app
    print("   Import successful")
    
    print("2. Testing app creation...")
    with app.test_client() as client:
        print("   Test client created")
        
        print("3. Testing root endpoint...")
        response = client.get('/')
        print(f"   Root status: {response.status_code}")
        
        print("4. Testing rankings endpoint...")
        response = client.get('/api/rankings')
        print(f"   Rankings status: {response.status_code}")
        if response.status_code != 200:
            print(f"   Error: {response.get_data(as_text=True)}")
            
except ImportError as e:
    print(f"Import failed: {e}")
except Exception as e:
    print(f"Runtime error: {e}")
    import traceback
    traceback.print_exc()
'''
    
    with open('startup_test.py', 'w', encoding='utf-8') as f:
        f.write(startup_test)
    
    print("Created startup_test.py")

def main():
    print("DIAGNOSING THE REAL ISSUE")
    print("=" * 30)
    
    # Check what's actually in the mobile dashboard
    issues = check_actual_mobile_dashboard()
    
    print(f"\nFound {len(issues)} potential issues:")
    for issue in issues:
        print(f"  - {issue}")
    
    # Create diagnostic tests
    create_minimal_test()
    find_exact_error_location()
    
    print("\nNEXT STEPS:")
    print("1. Run: python minimal_test.py")
    print("2. Run: python startup_test.py") 
    print("3. Based on results, we'll know exactly what to fix")
    
    print("\nLikely the issue is:")
    print("- Your mobile_dashboard.py still has SQLite-style queries")
    print("- But they weren't fixed by the previous script")
    print("- Let's see exactly which ones are failing")

if __name__ == "__main__":
    main()