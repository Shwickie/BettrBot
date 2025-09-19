# verification_after_fix.py - Run this after applying the fixes

import sqlite3
import pandas as pd
import requests
import json
from datetime import datetime, timedelta

DB_PATH = r"E:\Bettr Bot\betting-bot\data\betting.db"
DASHBOARD_URL = "http://localhost:5000"  # Change if your port is different

def verify_fixes():
    """Verify that the fixes are working properly"""
    
    print("🔍 VERIFYING BETTR BOT FIXES")
    print("=" * 50)
    
    # 1. Test database directly
    print("\n1. DATABASE VERIFICATION:")
    conn = sqlite3.connect(DB_PATH)
    
    # Check team name consistency
    team_check = pd.read_sql_query("""
        SELECT 
            (SELECT COUNT(DISTINCT team) FROM odds) as odds_teams,
            (SELECT COUNT(DISTINCT home_team) FROM games) + 
            (SELECT COUNT(DISTINCT away_team) FROM games) as game_teams
    """, conn)
    
    print(f"   Unique teams in odds: {team_check.iloc[0]['odds_teams']}")
    print(f"   Unique teams in games: {team_check.iloc[0]['game_teams']}")
    
    # Check recent odds
    recent_odds = pd.read_sql_query("""
        SELECT team, COUNT(*) as count
        FROM odds 
        WHERE timestamp >= datetime('now', '-24 hours')
        GROUP BY team
        ORDER BY count DESC
        LIMIT 5
    """, conn)
    
    print(f"   Recent odds (24h): {len(recent_odds)} teams")
    for _, row in recent_odds.iterrows():
        print(f"     {row['team']}: {row['count']} lines")
    
    # Check upcoming games with odds
    games_with_odds = pd.read_sql_query("""
        SELECT g.game_id, g.away_team, g.home_team, COUNT(o.team) as odds_count
        FROM games g
        LEFT JOIN odds o ON g.game_id = o.game_id
        WHERE date(g.game_date) >= date('now')
        GROUP BY g.game_id, g.away_team, g.home_team
        ORDER BY g.game_date
        LIMIT 5
    """, conn)
    
    print(f"\n   Upcoming games with odds:")
    for _, game in games_with_odds.iterrows():
        print(f"     {game['away_team']} @ {game['home_team']}: {game['odds_count']} odds")
    
    conn.close()
    
    # 2. Test API endpoints
    print("\n\n2. API ENDPOINT TESTING:")
    
    def test_endpoint(endpoint, description):
        try:
            response = requests.get(f"{DASHBOARD_URL}{endpoint}", timeout=10)
            if response.status_code == 200:
                data = response.json()
                print(f"   ✅ {description}: {response.status_code}")
                return data
            else:
                print(f"   ❌ {description}: {response.status_code}")
                return None
        except Exception as e:
            print(f"   ❌ {description}: {e}")
            return None
    
    # Test basic endpoints
    health = test_endpoint("/api/health", "Health check")
    rankings = test_endpoint("/api/rankings", "Rankings")
    predictions = test_endpoint("/api/predictions", "Predictions")
    games = test_endpoint("/api/games", "Games")
    
    # 3. Test specific functions
    print("\n\n3. FUNCTION TESTING:")
    
    # Test the debug function we added
    debug_test = test_endpoint("/api/debug/test-fixed-functions", "Fixed functions test")
    if debug_test:
        print(f"   Normalize function tests: {debug_test.get('results', {}).get('normalize_tests', 'Failed')}")
        print(f"   ML System available: {debug_test.get('results', {}).get('ml_system_available', 'Unknown')}")
    
    # 4. Test AI recommendations
    print("\n\n4. AI RECOMMENDATIONS TEST:")
    
    ai_recs = test_endpoint("/api/ai-betting-recommendations", "AI Recommendations")
    if ai_recs:
        result = ai_recs.get('result', {})
        recommendations = result.get('recommendations', [])
        print(f"   ✅ AI Recommendations working: {len(recommendations)} picks found")
        
        if recommendations:
            sample = recommendations[0]
            print(f"   Sample pick: {sample.get('team', 'Unknown')} at {sample.get('odds', 'N/A')} odds")
            print(f"   Edge: {sample.get('edge_percentage', 0)}%")
    else:
        print("   ❌ AI Recommendations failed")
    
    # 5. Test betting analysis
    print("\n\n5. BETTING ANALYSIS TEST:")
    
    betting_analysis = test_endpoint("/api/betting-analysis?week=current&edge=all", "Betting Analysis")
    if betting_analysis:
        opportunities = betting_analysis.get('opportunities', [])
        print(f"   ✅ Betting Analysis working: {len(opportunities)} opportunities found")
        
        if opportunities:
            sample = opportunities[0]
            print(f"   Sample opportunity: {sample.get('team', 'Unknown')} with {sample.get('edge_pct', 0)}% edge")
    else:
        print("   ❌ Betting Analysis failed")
    
    # 6. Summary
    print("\n\n" + "=" * 50)
    print("📋 VERIFICATION SUMMARY:")
    print("=" * 50)
    
    working_count = 0
    total_tests = 5
    
    if health: working_count += 1
    if rankings: working_count += 1  
    if predictions: working_count += 1
    if ai_recs and ai_recs.get('ok'): working_count += 1
    if betting_analysis: working_count += 1
    
    print(f"Working endpoints: {working_count}/{total_tests}")
    
    if working_count >= 4:
        print("🎉 SUCCESS! Your Bettr Bot should be working now.")
        print("\nNext steps:")
        print("1. Open http://localhost:5000 in your browser")
        print("2. Try the AI chat feature")
        print("3. Check the betting opportunities section")
    else:
        print("⚠️  Some issues remain. Check the failed endpoints above.")
        print("\nTroubleshooting:")
        print("1. Ensure your dashboard is running")
        print("2. Check for any remaining console errors")
        print("3. Verify all functions were added correctly")

if __name__ == "__main__":
    verify_fixes()