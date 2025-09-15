# surgical_fix.py - Fix the exact SQLite patterns that are failing
"""
Now that we know the exact issue, fix only the problematic queries
"""

import re
import os

def find_and_fix_sqlite_queries():
    """Find and fix all SQLite-style queries in mobile_dashboard.py"""
    
    file_path = "mobile_dashboard.py"
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Track what we fix
    fixes_made = []
    
    # Pattern 1: Simple WHERE clause with single parameter
    # FROM: WHERE column = ?", conn, params=[value]
    # TO: WHERE column = :column", conn, params={"column": value}
    
    pattern1 = r'WHERE\s+(\w+)\s*=\s*\?\s*""",?\s*conn,?\s*params=\[([^\]]+)\]'
    def replace1(match):
        column = match.group(1)
        value = match.group(2).strip()
        return f'WHERE {column} = :{column}""", conn, params={{"{column}": {value}}}'
    
    new_content = re.sub(pattern1, replace1, content)
    if new_content != content:
        fixes_made.append("Fixed single parameter WHERE clauses")
        content = new_content
    
    # Pattern 2: BETWEEN clause with two parameters
    # FROM: BETWEEN date(?) AND date(?)", conn, params=[start, end]
    # TO: BETWEEN date(:start) AND date(:end)", conn, params={"start": start, "end": end}
    
    pattern2 = r'BETWEEN\s+date\(\?\)\s+AND\s+date\(\?\)\s*""",?\s*conn,?\s*params=\[([^,]+),\s*([^\]]+)\]'
    def replace2(match):
        param1 = match.group(1).strip()
        param2 = match.group(2).strip()
        return f'BETWEEN date(:start_date) AND date(:end_date)""", conn, params={{"start_date": {param1}, "end_date": {param2}}}'
    
    new_content = re.sub(pattern2, replace2, content)
    if new_content != content:
        fixes_made.append("Fixed BETWEEN date clauses")
        content = new_content
    
    # Pattern 3: Any remaining ? with params=[...]
    # This is a catch-all for any patterns we missed
    
    pattern3 = r'(\s+"""),\s*conn,?\s*params=\[([^\]]+)\]'
    def replace3(match):
        ending = match.group(1)
        params = match.group(2).strip()
        
        # Count the ? marks in the preceding query
        # This is a simplified fix - might need manual adjustment
        if ',' in params:
            # Multiple parameters - this needs manual handling
            return match.group(0)  # Don't change complex cases
        else:
            # Single parameter case
            return f'{ending}, conn, params={{"param": {params}}}'
    
    # Apply this cautiously
    question_marks = content.count('WHERE') + content.count('?')
    if question_marks > 0:
        print(f"Found {question_marks} potential query patterns to check")
    
    # Manual fixes for specific known patterns from your logs
    
    # Fix the team_season_summary queries specifically
    old_pattern = 'WHERE season = ?", conn, params=[season]'
    new_pattern = 'WHERE season = :season", conn, params={"season": season}'
    if old_pattern in content:
        content = content.replace(old_pattern, new_pattern)
        fixes_made.append("Fixed season parameter queries")
    
    old_pattern2 = 'WHERE season = ?", conn, params=[season - 1]'
    new_pattern2 = 'WHERE season = :season", conn, params={"season": season - 1}'
    if old_pattern2 in content:
        content = content.replace(old_pattern2, new_pattern2)
        fixes_made.append("Fixed season-1 parameter queries")
    
    old_pattern3 = 'WHERE season = ?", conn, params=[season-1]'
    new_pattern3 = 'WHERE season = :season", conn, params={"season": season-1}'
    if old_pattern3 in content:
        content = content.replace(old_pattern3, new_pattern3)
        fixes_made.append("Fixed season-1 parameter queries (variant)")
    
    # Fix date range queries
    old_date = 'params=[today, end]'
    new_date = 'params={"start_date": today, "end_date": end}'
    if old_date in content:
        content = content.replace(old_date, new_date)
        fixes_made.append("Fixed date range parameters")
    
    old_date2 = 'params=[today, horizon]'
    new_date2 = 'params={"start_date": today, "end_date": horizon}'
    if old_date2 in content:
        content = content.replace(old_date2, new_date2)
        fixes_made.append("Fixed horizon date parameters")
    
    old_date3 = 'params=[start, end]'
    new_date3 = 'params={"start_date": start, "end_date": end}'
    if old_date3 in content:
        content = content.replace(old_date3, new_date3)
        fixes_made.append("Fixed start/end date parameters")
    
    # Now ensure all queries that use parameters also use text()
    # Look for pd.read_sql_query with raw strings that have :parameter syntax
    text_pattern = r'pd\.read_sql_query\(\s*"""([^"]*:[^"]*?)"""\s*,'
    def add_text_wrapper(match):
        query = match.group(1)
        if 'text(' not in match.group(0):
            return f'pd.read_sql_query(text("""{query}"""),'
        return match.group(0)
    
    new_content = re.sub(text_pattern, add_text_wrapper, content)
    if new_content != content:
        fixes_made.append("Added text() wrappers to parameterized queries")
        content = new_content
    
    # Save the fixed file
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return fixes_made

def test_fixed_queries():
    """Test that our fixes actually work"""
    
    test_script = '''# test_fixes.py
import os
os.environ['DATABASE_URL'] = 'postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres'

try:
    from mobile_dashboard import app
    
    with app.test_client() as client:
        print("Testing fixed endpoints...")
        
        endpoints = [
            ('/api/rankings', 'Rankings'),
            ('/api/predictions', 'Predictions'),
            ('/api/games', 'Games'),
            ('/api/betting-analysis', 'Betting Analysis')
        ]
        
        for endpoint, name in endpoints:
            try:
                response = client.get(endpoint)
                if response.status_code == 200:
                    data = response.get_json()
                    print(f"✓ {name}: {len(data) if isinstance(data, list) else 'OK'}")
                else:
                    print(f"✗ {name}: HTTP {response.status_code}")
            except Exception as e:
                print(f"✗ {name}: {str(e)[:50]}...")
    
    print("\\nIf all show ✓, your fixes worked and you can deploy!")
    
except Exception as e:
    print(f"Import/startup error: {e}")
'''
    
    with open('test_fixes.py', 'w') as f:
        f.write(test_script)
    
    print("Created test_fixes.py")

def main():
    print("APPLYING SURGICAL FIXES TO MOBILE_DASHBOARD.PY")
    print("=" * 50)
    
    # Apply the fixes
    fixes = find_and_fix_sqlite_queries()
    
    if fixes:
        print("Applied fixes:")
        for fix in fixes:
            print(f"  ✓ {fix}")
        
        # Create test script
        test_fixed_queries()
        
        print("\nNEXT STEPS:")
        print("1. Run: python test_fixes.py")
        print("2. If all endpoints show ✓, deploy:")
        print("   git add .")
        print("   git commit -m 'Fix SQLite to PostgreSQL parameter syntax'")
        print("   git push")
        
    else:
        print("No SQLite patterns found to fix.")
        print("Your mobile_dashboard.py might already be correct.")
        print("The issue might be elsewhere - check your deployment logs.")

if __name__ == "__main__":
    main()