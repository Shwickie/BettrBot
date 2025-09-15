# fix_mobile_dashboard.py - Apply the exact fixes needed
"""
Since your database connection works, we need to fix the Flask app queries
"""

import os
import shutil

def fix_mobile_dashboard_queries():
    """Fix the specific query issues in mobile_dashboard.py"""
    
    file_path = "mobile_dashboard.py"
    
    # Read the current file
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Apply specific fixes for the failing queries
    fixes_applied = []
    
    # Fix 1: Rankings endpoint - replace the problematic query
    old_rankings = '''try:
        # Get base power scores from team_season_summary
        pr = pd.read_sql_query("""
            SELECT
                team,
                power_score,
                wins,
                losses,
                games_played,
                win_pct,
                point_diff,
                preseason_scheduled,
                preseason_completed
            FROM team_season_summary
            WHERE season = ?
        """, conn, params=[season])'''
    
    new_rankings = '''try:
        # Get base power scores from team_season_summary
        pr = pd.read_sql_query(text("""
            SELECT
                team,
                power_score,
                wins,
                losses,
                games_played,
                win_pct,
                point_diff,
                preseason_scheduled,
                preseason_completed
            FROM team_season_summary
            WHERE season = :season
        """), conn, params={"season": season})'''
    
    if old_rankings in content:
        content = content.replace(old_rankings, new_rankings)
        fixes_applied.append("Fixed rankings query")
    
    # Fix 2: Similar pattern for fallback query
    old_fallback = '''pr = pd.read_sql_query("""
                SELECT team, power_score, wins, losses, games_played, win_pct, point_diff
                FROM team_season_summary 
                WHERE season = ?
            """, conn, params=[season-1])'''
    
    new_fallback = '''pr = pd.read_sql_query(text("""
                SELECT team, power_score, wins, losses, games_played, win_pct, point_diff
                FROM team_season_summary 
                WHERE season = :season
            """), conn, params={"season": season-1})'''
    
    if old_fallback in content:
        content = content.replace(old_fallback, new_fallback)
        fixes_applied.append("Fixed fallback rankings query")
    
    # Fix 3: Games query
    old_games = '''games = pd.read_sql_query(
            """
            SELECT game_id, away_team AS away, home_team AS home, game_date, start_time_local AS game_time
            FROM games
            WHERE date(game_date) BETWEEN date(?) AND date(?)
            ORDER BY date(game_date), time(start_time_local)
            """,
            conn, params=[today, end]
        )'''
    
    new_games = '''games = pd.read_sql_query(
            text("""
            SELECT game_id, away_team AS away, home_team AS home, game_date, start_time_local AS game_time
            FROM games
            WHERE date(game_date) BETWEEN date(:start_date) AND date(:end_date)
            ORDER BY date(game_date), time(start_time_local)
            """),
            conn, params={"start_date": today, "end_date": end}
        )'''
    
    if old_games in content:
        content = content.replace(old_games, new_games)
        fixes_applied.append("Fixed games query")
    
    # Fix 4: Any remaining ? parameter patterns
    import re
    
    # Find patterns like: WHERE season = ?, params=[value]
    pattern = r'WHERE\s+(\w+)\s*=\s*\?\s*""",?\s*conn,?\s*params=\[([^\]]+)\]'
    
    def replace_params(match):
        column = match.group(1)
        param_value = match.group(2)
        return f'WHERE {column} = :{column}""", conn, params={{"{column}": {param_value}}}'
    
    new_content = re.sub(pattern, replace_params, content)
    if new_content != content:
        content = new_content
        fixes_applied.append("Fixed remaining parameter patterns")
    
    # Fix 5: Ensure text import exists
    if 'from sqlalchemy import create_engine, text' not in content:
        if 'from sqlalchemy import create_engine' in content:
            content = content.replace(
                'from sqlalchemy import create_engine',
                'from sqlalchemy import create_engine, text'
            )
            fixes_applied.append("Added text import")
    
    # Write the fixed content back
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return fixes_applied

def move_endpoint_fixes():
    """Move the endpoint fixes to the cloud folder"""
    source = "../endpoint_fixes.py"
    target = "endpoint_fixes.py"
    
    if os.path.exists(source):
        shutil.move(source, target)
        print(f"Moved endpoint_fixes.py to cloud folder")
        return True
    return False

def create_test_locally():
    """Create a simple test to verify the fixes work"""
    
    test_content = '''# test_fixed_app.py - Test your fixed mobile dashboard
import os
os.environ['DATABASE_URL'] = 'postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres'

try:
    from mobile_dashboard import app
    
    with app.test_client() as client:
        print("Testing fixed endpoints...")
        
        # Test rankings
        response = client.get('/api/rankings')
        if response.status_code == 200:
            data = response.get_json()
            print(f"✅ Rankings: {len(data)} teams loaded")
        else:
            print(f"❌ Rankings failed: {response.status_code}")
        
        # Test predictions  
        response = client.get('/api/predictions')
        if response.status_code == 200:
            data = response.get_json()
            print(f"✅ Predictions: {len(data)} games loaded")
        else:
            print(f"❌ Predictions failed: {response.status_code}")
        
        # Test games
        response = client.get('/api/games')
        if response.status_code == 200:
            data = response.get_json()
            print(f"✅ Games: {len(data)} games loaded")
        else:
            print(f"❌ Games failed: {response.status_code}")
    
    print("\\n🎉 If all tests passed, your fixes worked!")
    print("Deploy with: git add . && git commit -m 'Fix SQL queries' && git push")
    
except Exception as e:
    print(f"❌ Test failed: {e}")
    import traceback
    traceback.print_exc()
'''
    
    with open('test_fixed_app.py', 'w') as f:
        f.write(test_content)
    
    print("Created test_fixed_app.py")

def main():
    print("APPLYING MOBILE DASHBOARD FIXES")
    print("=" * 40)
    
    # Apply the fixes
    fixes = fix_mobile_dashboard_queries()
    
    if fixes:
        print("Applied fixes:")
        for fix in fixes:
            print(f"  ✅ {fix}")
    else:
        print("⚠️ No fixes applied - file may already be correct")
    
    # Move any misplaced files
    move_endpoint_fixes()
    
    # Create test
    create_test_locally()
    
    print("\n🎯 NEXT STEPS:")
    print("1. Run: python test_fixed_app.py")
    print("2. If tests pass locally, deploy:")
    print("   git add .")
    print("   git commit -m 'Fix SQL parameter binding for PostgreSQL'")
    print("   git push")
    
    print("\n💡 The core issue was SQLite vs PostgreSQL parameter syntax:")
    print("   SQLite: WHERE col = ?, params=[value]")
    print("   PostgreSQL: WHERE col = :param, params={'param': value}")

if __name__ == "__main__":
    main()