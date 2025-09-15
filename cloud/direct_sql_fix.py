# direct_sql_fix.py - Fix the exact SQL error
"""
This finds and fixes the EXACT SQL query causing the error
"""

import os
import re

def find_and_fix_exact_error():
    """Find and fix the exact query causing the error"""
    
    file_path = "mobile_dashboard.py"
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    print("Looking for the exact problematic queries...")
    
    # Look for any query_df calls with SQLite syntax
    patterns_found = []
    
    # Pattern 1: Look for query_df calls with params=[]
    pattern1 = r'query_df\([^,]+,\s*[^,]+,\s*params=\[[^\]]+\]'
    matches1 = re.findall(pattern1, content, re.MULTILINE | re.DOTALL)
    if matches1:
        patterns_found.extend(matches1)
        print(f"Found {len(matches1)} query_df calls with params=[]")
    
    # Pattern 2: Look for pd.read_sql_query with params=[]
    pattern2 = r'pd\.read_sql_query\([^,]+,\s*[^,]+,\s*params=\[[^\]]+\]'
    matches2 = re.findall(pattern2, content, re.MULTILINE | re.DOTALL)
    if matches2:
        patterns_found.extend(matches2)
        print(f"Found {len(matches2)} pd.read_sql_query calls with params=[]")
    
    if patterns_found:
        print("\nProblematic patterns found:")
        for i, pattern in enumerate(patterns_found[:5]):
            print(f"{i+1}: {pattern[:100]}...")
    
    # Fix specific known problematic patterns
    fixes_applied = []
    
    # Fix 1: Season parameter in team_season_summary queries
    old1 = 'params=[season]'
    new1 = 'params={"season": season}'
    if old1 in content:
        content = content.replace(old1, new1)
        fixes_applied.append("Season parameters")
    
    # Fix 2: Season-1 parameters
    old2 = 'params=[season-1]'
    new2 = 'params={"season": season-1}'
    if old2 in content:
        content = content.replace(old2, new2)
        fixes_applied.append("Season-1 parameters")
    
    old2b = 'params=[season - 1]'
    new2b = 'params={"season": season - 1}'
    if old2b in content:
        content = content.replace(old2b, new2b)
        fixes_applied.append("Season - 1 parameters")
    
    # Fix 3: Date range parameters
    old3 = 'params=[today, end]'
    new3 = 'params={"start_date": today, "end_date": end}'
    if old3 in content:
        content = content.replace(old3, new3)
        fixes_applied.append("Date range parameters")
    
    old3b = 'params=[start, end]'
    new3b = 'params={"start_date": start, "end_date": end}'
    if old3b in content:
        content = content.replace(old3b, new3b)
        fixes_applied.append("Start/end parameters")
    
    old3c = 'params=[today, horizon]'
    new3c = 'params={"start_date": today, "end_date": horizon}'
    if old3c in content:
        content = content.replace(old3c, new3c)
        fixes_applied.append("Today/horizon parameters")
    
    # Fix 4: Single team parameters
    old4 = 'params=[team]'
    new4 = 'params={"team": team}'
    if old4 in content:
        content = content.replace(old4, new4)
        fixes_applied.append("Team parameters")
    
    # Fix 5: Game ID parameters
    old5 = 'params=[game_id]'
    new5 = 'params={"game_id": game_id}'
    if old5 in content:
        content = content.replace(old5, new5)
        fixes_applied.append("Game ID parameters")
    
    # Fix 6: Multiple game IDs
    old6 = 'params=game_ids'
    new6 = 'params={"gid0": game_ids[0]} if game_ids else {}'
    if old6 in content and 'game_ids' in content:
        # This is trickier - need to rebuild the query
        print("Found complex game_ids parameter - manual fix needed")
    
    # Also fix any remaining WHERE ? patterns
    # This regex finds WHERE column = ? followed by params=[value]
    where_pattern = r'WHERE\s+(\w+)\s*=\s*\?\s*"""\s*,\s*conn\s*,\s*params=\[([^\]]+)\]'
    def fix_where(match):
        column = match.group(1)
        value = match.group(2)
        return f'WHERE {column} = :{column}""", conn, params={{"{column}": {value}}}'
    
    new_content = re.sub(where_pattern, fix_where, content)
    if new_content != content:
        content = new_content
        fixes_applied.append("WHERE clause parameters")
    
    # Write the fixed content
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"\nApplied fixes: {fixes_applied}")
    return len(fixes_applied) > 0

def add_text_import():
    """Ensure text import is present"""
    
    file_path = "mobile_dashboard.py"
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    if 'from sqlalchemy import create_engine, text' not in content:
        if 'from sqlalchemy import create_engine' in content:
            content = content.replace(
                'from sqlalchemy import create_engine',
                'from sqlalchemy import create_engine, text'
            )
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            print("Added text import")
            return True
    
    return False

def test_specific_endpoint():
    """Test the specific endpoint that's failing"""
    
    print("\nTesting the rankings endpoint specifically...")
    
    try:
        os.environ['DATABASE_URL'] = 'postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres'
        
        from mobile_dashboard import app
        
        with app.test_client() as client:
            response = client.get('/api/rankings')
            
            if response.status_code == 200:
                data = response.get_json()
                print(f"SUCCESS: Rankings returned {len(data)} teams")
                return True
            else:
                error_text = response.get_data(as_text=True)
                print(f"FAILED: HTTP {response.status_code}")
                print(f"Error: {error_text[:200]}...")
                return False
                
    except Exception as e:
        print(f"EXCEPTION: {e}")
        return False

def main():
    print("DIRECT SQL ERROR FIX")
    print("=" * 30)
    
    # Step 1: Find and fix exact queries
    if find_and_fix_exact_error():
        print("Applied SQL fixes")
    else:
        print("No SQL fixes found")
    
    # Step 2: Add text import
    if add_text_import():
        print("Added text import")
    
    # Step 3: Test the specific failing endpoint
    if test_specific_endpoint():
        print("\nSUCCESS: The fix worked!")
        print("\nNow deploy with:")
        print("  git add .")
        print("  git commit -m 'Fix SQL parameters for PostgreSQL'")
        print("  git push origin main")
    else:
        print("\nThe error persists. Let me check what's in your mobile_dashboard.py...")
        
        # Show what's actually in the file around rankings
        try:
            with open("mobile_dashboard.py", 'r') as f:
                content = f.read()
            
            # Find rankings function
            start = content.find("def api_rankings")
            if start != -1:
                end = content.find("\ndef ", start + 1)
                if end == -1:
                    end = start + 1000
                
                rankings_code = content[start:end]
                print("\nRankings function code:")
                print(rankings_code[:500] + "..." if len(rankings_code) > 500 else rankings_code)
        except:
            print("Could not read mobile_dashboard.py")

if __name__ == "__main__":
    main()