#!/usr/bin/env python3
"""
Debug script to check what's happening with predictions
Run this to diagnose the issues with your dashboard
"""

import os
import sys
from sqlalchemy import create_engine, text
import pandas as pd

# Your database URL
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql+psycopg2://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway")

ENGINE = create_engine(DATABASE_URL, pool_pre_ping=True)

def check_model_status():
    """Check if ML model is actually loaded"""
    print("=" * 60)
    print("CHECKING ML MODEL STATUS")
    print("=" * 60)
    
    try:
        # Check if model file exists
        model_paths = [
            os.path.join(os.getcwd(), "betting_model_fixed.pkl"),
            os.path.join(os.path.dirname(__file__), "betting_model_fixed.pkl"),
            os.path.join(os.path.dirname(__file__), "..", "models", "betting_model_fixed.pkl"),
        ]
        
        model_found = None
        for path in model_paths:
            if os.path.exists(path):
                model_found = path
                print(f"✅ Model file found: {path}")
                break
        
        if not model_found:
            print("❌ NO MODEL FILE FOUND")
            print("   Your predictions are using FALLBACK (power-based) method")
            print("   This explains the inflated confidence scores!")
            return False
        
        # Try to load it
        import pickle
        with open(model_found, 'rb') as f:
            model_pack = pickle.load(f)
        
        print(f"✅ Model loaded successfully")
        print(f"   Features: {len(model_pack.get('feature_cols', []))}")
        print(f"   Has 'model' key: {'model' in model_pack}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error checking model: {e}")
        return False

def check_predictions_logic():
    """Check what prediction method is being used"""
    print("\n" + "=" * 60)
    print("CHECKING PREDICTION LOGIC")
    print("=" * 60)
    
    try:
        from model.prediction import FixedNFLSystem
        
        system = FixedNFLSystem()
        
        # Try a sample prediction
        result = system.predict_game(
            "Kansas City Chiefs",
            "Buffalo Bills",
            "2025-10-19"
        )
        
        print(f"✅ ML System working")
        print(f"   Sample prediction: {result.get('home_win_probability', 'N/A')}")
        print(f"   Confidence: {result.get('confidence', 'N/A')}")
        print(f"   Method: {'ML Model' if result.get('model_prediction') else 'Fallback'}")
        
        return True
        
    except Exception as e:
        print(f"❌ ML System error: {e}")
        print("   Predictions falling back to simple power-based method")
        return False

def check_odds_data():
    """Check if odds data exists and matches games"""
    print("\n" + "=" * 60)
    print("CHECKING ODDS DATA")
    print("=" * 60)
    
    try:
        with ENGINE.connect() as conn:
            # Check total odds
            total_odds = conn.execute(text("SELECT COUNT(*) FROM odds")).scalar()
            print(f"Total odds records: {total_odds}")
            
            # Check recent odds
            recent_odds = conn.execute(text("""
                SELECT COUNT(*) FROM odds 
                WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
            """)).scalar()
            print(f"Recent odds (last 7 days): {recent_odds}")
            
            # Check if odds match upcoming games
            games_with_odds = pd.read_sql(text("""
                SELECT 
                    g.game_id,
                    g.home_team,
                    g.away_team,
                    g.game_date,
                    COUNT(DISTINCT o.game_id) as has_odds
                FROM games g
                LEFT JOIN odds o ON g.game_id = o.game_id
                WHERE g.game_date >= CURRENT_DATE
                AND g.game_date <= CURRENT_DATE + INTERVAL '7 days'
                GROUP BY g.game_id, g.home_team, g.away_team, g.game_date
                ORDER BY g.game_date
                LIMIT 10
            """), conn)
            
            games_without_odds = len(games_with_odds[games_with_odds['has_odds'] == 0])
            
            print(f"\nUpcoming games (next 7 days): {len(games_with_odds)}")
            print(f"Games WITHOUT odds: {games_without_odds}")
            
            if games_without_odds > 0:
                print("\n❌ ISSUE: Many games missing odds data")
                print("   This is why you see no betting opportunities!")
                print("\nGames missing odds:")
                for _, game in games_with_odds[games_with_odds['has_odds'] == 0].iterrows():
                    print(f"   - {game['away_team']} @ {game['home_team']} ({game['game_date']})")
            else:
                print("✅ All upcoming games have odds")
            
            # Sample odds data to check team names
            print("\nSample odds data (checking team name format):")
            sample_odds = pd.read_sql(text("""
                SELECT team, sportsbook, odds 
                FROM odds 
                WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
                LIMIT 5
            """), conn)
            print(sample_odds.to_string())
            
            return games_without_odds == 0
            
    except Exception as e:
        print(f"❌ Error checking odds: {e}")
        return False

def check_team_name_consistency():
    """Check if team names match between games and odds tables"""
    print("\n" + "=" * 60)
    print("CHECKING TEAM NAME CONSISTENCY")
    print("=" * 60)
    
    try:
        with ENGINE.connect() as conn:
            # Get unique team names from games
            game_teams = pd.read_sql(text("""
                SELECT DISTINCT home_team as team FROM games
                UNION
                SELECT DISTINCT away_team as team FROM games
                ORDER BY team
            """), conn)
            
            # Get unique team names from odds
            odds_teams = pd.read_sql(text("""
                SELECT DISTINCT team FROM odds
                ORDER BY team
            """), conn)
            
            game_team_set = set(game_teams['team'])
            odds_team_set = set(odds_teams['team'])
            
            print(f"Teams in games table: {len(game_team_set)}")
            print(f"Teams in odds table: {len(odds_team_set)}")
            
            # Check for mismatches
            in_games_not_odds = game_team_set - odds_team_set
            in_odds_not_games = odds_team_set - game_team_set
            
            if in_games_not_odds:
                print(f"\n❌ Teams in games but NOT in odds ({len(in_games_not_odds)}):")
                for team in sorted(in_games_not_odds):
                    print(f"   - {team}")
            
            if in_odds_not_games:
                print(f"\n⚠️  Teams in odds but NOT in games ({len(in_odds_not_games)}):")
                for team in sorted(in_odds_not_games):
                    print(f"   - {team}")
            
            if not in_games_not_odds and not in_odds_not_games:
                print("✅ All team names match perfectly!")
                return True
            else:
                print("\n❌ TEAM NAME MISMATCH - This breaks odds lookups!")
                return False
                
    except Exception as e:
        print(f"❌ Error checking team names: {e}")
        return False

def main():
    print("\n🔍 BETTR BOT DIAGNOSTIC TOOL\n")
    
    issues = []
    
    # Run checks
    if not check_model_status():
        issues.append("ML model not loaded - using fallback predictions")
    
    if not check_predictions_logic():
        issues.append("Prediction system not working correctly")
    
    if not check_odds_data():
        issues.append("Odds data missing or incomplete")
    
    if not check_team_name_consistency():
        issues.append("Team name mismatches between tables")
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    if issues:
        print("❌ ISSUES FOUND:")
        for i, issue in enumerate(issues, 1):
            print(f"   {i}. {issue}")
        
        print("\n💡 RECOMMENDED FIXES:")
        if "ML model" in str(issues):
            print("   1. Run train.py to generate betting_model_fixed.pkl")
            print("   2. Make sure model file is in project root or models/ folder")
        
        if "Odds data" in str(issues):
            print("   3. Run: curl -X POST http://localhost:5000/api/admin/add-test-odds")
            print("      (Must be logged in as admin)")
        
        if "Team name" in str(issues):
            print("   4. Check odds fetcher - ensure team names match games table")
            print("      Example: Use 'KC' not 'Kansas City Chiefs'")
    else:
        print("✅ All systems operational!")
    
    print("\n")

if __name__ == "__main__":
    main()