# inspect_deployed_code.py - See exactly what's in your current code
"""
Since the fixes say "already correct" but logs show errors, 
let's see exactly what's deployed
"""

import os
import re

def search_for_sqlite_patterns():
    """Search all Python files for SQLite-style patterns"""
    
    files_to_check = [
        "mobile_dashboard.py",
        "app.py", 
        "ai_chat_stub.py",
        "prediction.py"
    ]
    
    sqlite_patterns = [
        r'params=\[[^\]]+\]',  # params=[value] 
        r'WHERE\s+\w+\s*=\s*\?',  # WHERE col = ?
        r'BETWEEN.*\?.*AND.*\?',  # BETWEEN ? AND ?
    ]
    
    found_issues = []
    
    for file_path in files_to_check:
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            continue
            
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                content = f.read()
                
                for i, pattern in enumerate(sqlite_patterns):
                    matches = re.findall(pattern, content, re.IGNORECASE)
                    if matches:
                        found_issues.append({
                            'file': file_path,
                            'pattern': pattern,
                            'matches': matches
                        })
                        print(f"Found SQLite pattern in {file_path}:")
                        for match in matches[:3]:  # Show first 3
                            print(f"  {match}")
                        if len(matches) > 3:
                            print(f"  ... and {len(matches)-3} more")
                        print()
                        
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
    
    return found_issues

def check_imports():
    """Check if proper imports exist"""
    
    files_to_check = ["mobile_dashboard.py", "app.py"]
    
    for file_path in files_to_check:
        if not os.path.exists(file_path):
            continue
            
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        print(f"\nChecking imports in {file_path}:")
        
        if 'from sqlalchemy import' in content:
            if 'text' in content:
                print("  ✓ SQLAlchemy text imported")
            else:
                print("  ✗ SQLAlchemy text NOT imported")
        else:
            print("  ✗ No SQLAlchemy imports found")

def find_specific_failing_functions():
    """Look for the specific functions mentioned in error logs"""
    
    error_functions = [
        "api_rankings",
        "displayRankings", 
        "loadRankings",
        "api_betting_analysis",
        "get_betting_recommendations"
    ]
    
    file_path = "mobile_dashboard.py"
    if not os.path.exists(file_path):
        print("mobile_dashboard.py not found")
        return
        
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    print("\nChecking specific functions from error logs:")
    
    for func_name in error_functions:
        if f"def {func_name}" in content:
            print(f"Found function: {func_name}")
            
            # Extract the function to see its queries
            func_start = content.find(f"def {func_name}")
            if func_start != -1:
                # Find the next function or end of file
                next_func = content.find("\ndef ", func_start + 1)
                if next_func == -1:
                    func_content = content[func_start:]
                else:
                    func_content = content[func_start:next_func]
                
                # Look for SQL queries in this function
                if "pd.read_sql_query" in func_content:
                    sql_queries = re.findall(r'pd\.read_sql_query\([^)]+\)', func_content)
                    print(f"  Found {len(sql_queries)} SQL queries")
                    
                    for query in sql_queries:
                        if "params=" in query:
                            if "params=[" in query:
                                print(f"    ✗ SQLite style: {query[:100]}...")
                            elif "params={" in query:
                                print(f"    ✓ PostgreSQL style: {query[:100]}...")
        else:
            print(f"Function {func_name} not found")

def check_file_timestamps():
    """Check when files were last modified"""
    
    files_to_check = [
        "mobile_dashboard.py",
        "app.py"
    ]
    
    print("\nFile modification times:")
    for file_path in files_to_check:
        if os.path.exists(file_path):
            mtime = os.path.getmtime(file_path)
            import datetime
            mod_time = datetime.datetime.fromtimestamp(mtime)
            print(f"  {file_path}: {mod_time}")

def main():
    print("INSPECTING DEPLOYED CODE FOR ACTUAL ISSUES")
    print("=" * 50)
    
    # Check for SQLite patterns
    issues = search_for_sqlite_patterns()
    
    # Check imports
    check_imports()
    
    # Check specific failing functions
    find_specific_failing_functions()
    
    # Check file timestamps
    check_file_timestamps()
    
    print("\n" + "=" * 50)
    if issues:
        print(f"FOUND {len(issues)} ISSUES TO FIX")
        print("\nThe problem is definitely SQLite syntax in your code.")
        print("Manual fix needed - the patterns weren't caught by regex.")
    else:
        print("NO SQLITE PATTERNS FOUND")
        print("\nPossible causes:")
        print("1. Error is coming from a different file")
        print("2. Cached code on the server")
        print("3. Different environment")
        print("4. The error logs are old")

if __name__ == "__main__":
    main()