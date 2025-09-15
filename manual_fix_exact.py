# manual_fix_exact.py - Fix the exact SQLite patterns
import re

def fix_mobile_dashboard():
    with open("mobile_dashboard.py", "r") as f:
        content = f.read()
    
    # Replace ALL variations of SQLite parameter syntax
    replacements = [
        # Basic WHERE clause
        (r'WHERE season = \?", conn, params=\[season\]', 'WHERE season = :season", conn, params={"season": season}'),
        (r'WHERE season = \?", conn, params=\[season - 1\]', 'WHERE season = :season", conn, params={"season": season - 1}'),
        (r'WHERE season = \?", conn, params=\[season-1\]', 'WHERE season = :season", conn, params={"season": season-1}'),
        
        # Date range patterns
        (r'BETWEEN date\(\?\) AND date\(\?\)", conn, params=\[today, end\]', 'BETWEEN date(:start) AND date(:end)", conn, params={"start": today, "end": end}'),
        (r'BETWEEN date\(\?\) AND date\(\?\)", conn, params=\[today, horizon\]', 'BETWEEN date(:start) AND date(:end)", conn, params={"start": today, "end": horizon}'),
        (r'BETWEEN date\(\?\) AND date\(\?\)", conn, params=\[start, end\]', 'BETWEEN date(:start) AND date(:end)", conn, params={"start": start, "end": end}'),
        
        # Any remaining params=[] patterns
        (r'params=\[([^,\]]+)\]', r'params={"param": \1}'),
        (r'params=\[([^,]+),\s*([^\]]+)\]', r'params={"param1": \1, "param2": \2}'),
    ]
    
    for old, new in replacements:
        if re.search(old, content):
            content = re.sub(old, new, content)
            print(f"Fixed: {old[:50]}...")
    
    # Ensure text() wrapper for parameterized queries
    content = re.sub(
        r'pd\.read_sql_query\(\s*"""([^"]*:[^"]*?)"""',
        r'pd.read_sql_query(text("""\1""")',
        content
    )
    
    with open("mobile_dashboard.py", "w") as f:
        f.write(content)
    
    print("Manual fixes applied!")

if __name__ == "__main__":
    fix_mobile_dashboard()
