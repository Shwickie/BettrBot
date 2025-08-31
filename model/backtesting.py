import sys
import os
sys.path.append('E:/Bettr Bot/betting-bot/')  # Add your project root to path

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def comprehensive_backtest(db_path, start_date, end_date, min_edge=0.03):
    """
    Rigorous backtesting framework to validate model performance
    """
    conn = sqlite3.connect(db_path)
    
    # Get historical games with results
    games = pd.read_sql_query("""
        SELECT 
            g.game_id, g.home_team, g.away_team, g.game_date,
            g.home_score, g.away_score,
            CASE WHEN g.home_score > g.away_score THEN 1 ELSE 0 END as home_win
        FROM games g
        WHERE g.game_date BETWEEN ? AND ?
        AND g.home_score IS NOT NULL
        ORDER BY g.game_date
    """, conn, params=[start_date, end_date])
    
    print(f"Backtesting {len(games)} games from {start_date} to {end_date}")
    
    if games.empty:
        print("No games found in date range!")
        return
    
    results = {
        'total_opportunities': 0,
        'bets_placed': 0,
        'wins': 0,
        'losses': 0,
        'total_profit': 0.0,
        'bet_details': []
    }
    
    # Try to import your BettingAI
    try:
        # First try the direct import
        sys.path.append('E:/Bettr Bot/betting-bot/dashboard/')
        from ai_chat_stub import BettingAI
        ai = BettingAI(db_path)
        print("✓ Using ML model from ai_chat_stub")
        use_ai_model = True
    except Exception as e:
        print(f"⚠️  Could not load AI model: {e}")
        print("Using simple power rating fallback")
        use_ai_model = False
    
    # Fallback power rating function
    def simple_power_prediction(home_team, away_team):
        """Simple power rating prediction as fallback"""
        try:
            # Get current season power scores
            power_df = pd.read_sql_query("""
                SELECT team, power_score 
                FROM team_season_summary 
                WHERE season = 2024
            """, conn)
            
            power_map = dict(zip(power_df['team'], power_df['power_score']))
            
            home_power = power_map.get(home_team, 0) + 2.5  # HFA
            away_power = power_map.get(away_team, 0)
            
            # Logistic function
            home_prob = 1.0 / (1.0 + math.exp(-(home_power - away_power) / 8.0))
            return max(0.05, min(0.95, home_prob))  # Clamp extreme values
        except:
            return 0.5  # Neutral if error
    
    for _, game in games.iterrows():
        # Get historical odds for this game
        odds_data = pd.read_sql_query("""
            SELECT team, AVG(CAST(odds AS REAL)) as avg_odds
            FROM odds 
            WHERE game_id = ? AND market = 'h2h'
            GROUP BY team
        """, conn, params=[game['game_id']])
        
        if len(odds_data) < 2:  # Need odds for both teams
            continue
            
        # Get model prediction
        if use_ai_model:
            game_dict = {
                'game_id': game['game_id'],
                'home': game['home_team'],
                'away': game['away_team'], 
                'date': game['game_date']
            }
            
            try:
                home_prob = ai._predict_home_prob_model(conn, game_dict)
                away_prob = 1.0 - home_prob
            except Exception as e:
                # Fallback to simple model
                home_prob = simple_power_prediction(game['home_team'], game['away_team'])
                away_prob = 1.0 - home_prob
        else:
            home_prob = simple_power_prediction(game['home_team'], game['away_team'])
            away_prob = 1.0 - home_prob
            
        results['total_opportunities'] += 1
        
        # Check each team for value
        for team_name, prob in [('home', home_prob), ('away', away_prob)]:
            actual_team = game['home_team'] if team_name == 'home' else game['away_team']
            
            team_odds = odds_data[odds_data['team'] == actual_team]
            if team_odds.empty:
                continue
                
            avg_odds = team_odds['avg_odds'].iloc[0]
            
            # Convert to probability and calculate edge
            if avg_odds > 0:
                implied_prob = 100.0 / (avg_odds + 100.0)
            else:
                implied_prob = abs(avg_odds) / (abs(avg_odds) + 100.0)
                
            edge = prob - implied_prob
            
            # Apply conservative edge threshold
            if edge >= min_edge:
                results['bets_placed'] += 1
                
                # Determine if bet won
                actual_winner = 'home' if game['home_win'] == 1 else 'away'
                bet_won = (team_name == actual_winner)
                
                # Calculate profit/loss
                if bet_won:
                    if avg_odds > 0:
                        profit = avg_odds / 100.0  # $1 bet returns odds/100
                    else:
                        profit = 100.0 / abs(avg_odds)
                    results['wins'] += 1
                    results['total_profit'] += profit
                else:
                    results['losses'] += 1  
                    results['total_profit'] -= 1.0  # Lost $1 bet
                    
                results['bet_details'].append({
                    'game': f"{game['away_team']} @ {game['home_team']}",
                    'date': game['game_date'],
                    'bet_team': actual_team,
                    'odds': avg_odds,
                    'edge': edge,
                    'won': bet_won,
                    'profit': profit if bet_won else -1.0
                })
    
    # Calculate performance metrics
    if results['bets_placed'] > 0:
        win_rate = results['wins'] / results['bets_placed']
        roi = (results['total_profit'] / results['bets_placed']) * 100
        
        print(f"\n=== BACKTEST RESULTS ===")
        print(f"Total Opportunities Scanned: {results['total_opportunities']}")
        print(f"Bets Placed (≥{min_edge*100:.1f}% edge): {results['bets_placed']}")
        print(f"Win Rate: {win_rate:.1%}")
        print(f"Total Profit: ${results['total_profit']:.2f}")
        print(f"ROI: {roi:.1f}%")
        print(f"Profit per Bet: ${results['total_profit']/results['bets_placed']:.3f}")
        
        # Show sample bets
        print(f"\nSample Bets:")
        for bet in results['bet_details'][:10]:
            status = "✓" if bet['won'] else "✗"
            print(f"{status} {bet['game']} - {bet['bet_team']} ({bet['odds']:+.0f}) - Edge: {bet['edge']:.1%} - P/L: ${bet['profit']:.2f}")
            
        # Red flags
        if win_rate > 0.65:
            print("\n⚠️  WARNING: Win rate suspiciously high - possible overfitting")
        if roi > 15:
            print(f"⚠️  WARNING: {roi:.1f}% ROI unrealistic for sports betting")
        if results['bets_placed'] > results['total_opportunities'] * 0.3:
            print("⚠️  WARNING: Too many 'value' bets found - model likely miscalibrated")
            
    else:
        print("No qualifying bets found in backtest period")
        
    conn.close()
    return results

def diagnose_system_inconsistencies():
    """Check why dashboard and AI chat show different results"""
    print("=== SYSTEM DIAGNOSIS ===")
    
    # Check if trained model exists
    model_path = r"E:/Bettr Bot/betting-bot/models/betting_model.pkl"
    if os.path.exists(model_path):
        print("✓ Trained model file exists")
        try:
            import pickle
            with open(model_path, 'rb') as f:
                model_data = pickle.load(f)
            print(f"✓ Model loaded successfully: {model_data.get('timestamp', 'Unknown date')}")
        except Exception as e:
            print(f"✗ Error loading model: {e}")
    else:
        print("✗ No trained model found - dashboard using simple power ratings")
    
    # Check AI chat import
    try:
        sys.path.append('E:/Bettr Bot/betting-bot/dashboard/')
        from ai_chat_stub import BettingAI
        print("✓ AI chat module imports successfully")
    except Exception as e:
        print(f"✗ AI chat import error: {e}")
    
    print("\nRecommendation:")
    print("1. Train your model using train_betting_model.py")
    print("2. Update dashboard to use the same prediction system as AI chat")
    print("3. Run this backtest to validate performance before any real betting")

# Run comprehensive backtest
if __name__ == "__main__":
    # First diagnose the system
    diagnose_system_inconsistencies()
    
    print("\n" + "="*50)
    
    # Test on recent data
    end_date = "2024-08-30"    # Adjust based on your data
    start_date = "2024-01-01"  # Shorter test period
    
    results = comprehensive_backtest(
        r"E:/Bettr Bot/betting-bot/data/betting.db",
        start_date, 
        end_date,
        min_edge=0.05  # 5% minimum edge
    )