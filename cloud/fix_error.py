# find_all_queries.py - Find ALL database queries in the file
"""
Since the simple api_rankings shows hardcoded data but errors still occur,
let's find ALL database queries in the entire file.
"""

import re
import os

def find_all_database_queries():
    """Find every single database query in mobile_dashboard.py"""
    
    file_path = "mobile_dashboard.py"
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
        lines = content.split('\n')
    
    print("FINDING ALL DATABASE QUERIES")
    print("=" * 40)
    
    # Find all pd.read_sql_query calls
    sql_queries = []
    
    for i, line in enumerate(lines):
        if 'pd.read_sql_query' in line:
            # Get context around this line
            start = max(0, i - 3)
            end = min(len(lines), i + 10)
            context = lines[start:end]
            
            sql_queries.append({
                'line_num': i + 1,
                'context': context,
                'has_params': any('params=' in l for l in context),
                'has_question_mark': any('?' in l for l in context),
                'has_list_params': any('params=[' in l for l in context)
            })
    
    print(f"Found {len(sql_queries)} pd.read_sql_query calls:")
    
    for j, query in enumerate(sql_queries):
        print(f"\n--- Query {j+1} at line {query['line_num']} ---")
        for k, line in enumerate(query['context']):
            line_num = query['line_num'] - 3 + k
            marker = ">>> " if 'pd.read_sql_query' in line else "    "
            if query['has_list_params'] and 'params=[' in line:
                marker = "!!! "  # Mark problematic lines
            print(f"{marker}{line_num:3d}: {line}")
        
        if query['has_list_params']:
            print("    ^^^ THIS QUERY HAS SQLite-STYLE PARAMS[] ^^^")
    
    return sql_queries

def find_all_functions_with_db_queries():
    """Find which functions contain database queries"""
    
    file_path = "mobile_dashboard.py"
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Find all function definitions
    functions = re.findall(r'def (\w+)\([^)]*\):', content)
    
    print(f"\nFUNCTIONS WITH DATABASE QUERIES:")
    print("=" * 35)
    
    for func_name in functions:
        # Find the function body
        func_start = content.find(f'def {func_name}(')
        if func_start == -1:
            continue
        
        # Find the next function or end of file
        next_func_start = content.find('\ndef ', func_start + 1)
        if next_func_start == -1:
            func_body = content[func_start:]
        else:
            func_body = content[func_start:next_func_start]
        
        # Check if this function has database queries
        if 'pd.read_sql_query' in func_body or 'conn.execute' in func_body:
            print(f"\n{func_name}():")
            
            # Check for problematic patterns
            if 'params=[' in func_body:
                print("  ❌ HAS SQLite params=[] syntax")
            elif 'params={' in func_body:
                print("  ✅ Uses PostgreSQL params={} syntax")
            else:
                print("  ⚠️  Has queries but no clear params pattern")
            
            # Show query snippets
            query_lines = [line.strip() for line in func_body.split('\n') 
                          if 'pd.read_sql_query' in line or 'params=' in line]
            for line in query_lines[:3]:  # Show first 3 query-related lines
                print(f"     {line}")

def check_specific_error_sources():
    """Check the specific patterns that cause the error message"""
    
    file_path = "mobile_dashboard.py"
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    print(f"\nSPECIFIC ERROR PATTERN ANALYSIS:")
    print("=" * 35)
    
    # The error "List argument must consist only of tuples or dictionaries"
    # comes from SQLAlchemy when you pass a list to params= with named parameters
    
    # Look for specific problematic patterns
    patterns_to_check = [
        (r'params=\[[^\]]+\]', 'SQLite-style params=[] with list'),
        (r'WHERE\s+\w+\s*=\s*\?', 'WHERE column = ? syntax'),
        (r'text\([^)]*\?\s*[^)]*\).*params=\[', 'text() with ? but params=[]'),
    ]
    
    for pattern, description in patterns_to_check:
        matches = re.findall(pattern, content, re.MULTILINE | re.DOTALL)
        if matches:
            print(f"\n❌ Found {len(matches)} instances of: {description}")
            for i, match in enumerate(matches[:3]):
                print(f"   {i+1}: {match}")
        else:
            print(f"✅ No instances of: {description}")

def main():
    print("COMPREHENSIVE DATABASE QUERY ANALYSIS")
    print("=" * 45)
    
    # Find all queries
    queries = find_all_database_queries()
    
    # Find functions with queries
    find_all_functions_with_db_queries()
    
    # Check specific error patterns
    check_specific_error_sources()
    
    print(f"\nSUMMARY:")
    print(f"Total pd.read_sql_query calls found: {len(queries)}")
    
    problematic_queries = [q for q in queries if q['has_list_params']]
    print(f"Queries with SQLite-style params=[]: {len(problematic_queries)}")
    
    if problematic_queries:
        print(f"\n❌ FOUND THE PROBLEM!")
        print(f"You have {len(problematic_queries)} queries still using SQLite syntax.")
        print(f"These need to be manually fixed.")
    else:
        print(f"\n⚠️  No obvious SQLite patterns found.")
        print(f"The error might be coming from:")
        print(f"  - Imported modules")
        print(f"  - Dynamic query construction")
        print(f"  - Exception handlers that swallow the real error")

if __name__ == "__main__":
    main()