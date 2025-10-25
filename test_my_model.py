#!/usr/bin/env python3
"""
Test ML Model Accuracy on YOUR Actual Data
This script evaluates the betting model's performance on past games to determine profitability.
"""

import os
import sys
os.environ['PYTHONIOENCODING'] = 'utf-8'

import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from model.prediction import FixedNFLSystem

# Database connection
DATABASE_URL = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

print("=" * 80)
print("NFL ML MODEL ACCURACY TEST")
print("=" * 80)
print()

# Initialize the ML system
print("Initializing ML Prediction System...")
try:
    ml_system = FixedNFLSystem()
    print(f"Model loaded successfully with {len(ml_system.model_data['feature_cols'])} features")
except Exception as e:
    print(f"ERROR loading model: {e}")
    sys.exit(1)
print()

# Get completed games (ALL seasons with scores)
print("Fetching completed games from database...")
query = text("""
    SELECT
        game_id,
        game_date,
        home_team,
        away_team,
        home_score,
        away_score,
        week,
        season
    FROM games
    WHERE home_score IS NOT NULL
    AND away_score IS NOT NULL
    ORDER BY game_date DESC
    LIMIT 150
""")

with engine.connect() as conn:
    result = conn.execute(query)
    games_df = pd.DataFrame(result.fetchall(), columns=result.keys())

print(f"Found {len(games_df)} completed games to test")
print(f"Date range: {games_df['game_date'].min()} to {games_df['game_date'].max()}")
print()

# Test the model on each game
results = []
print("Running predictions on historical games...")
print()

for idx, game in games_df.iterrows():
    home_team = game['home_team']
    away_team = game['away_team']
    game_date = game['game_date']

    # Get actual winner
    actual_winner = home_team if game['home_score'] > game['away_score'] else away_team
    actual_home_won = game['home_score'] > game['away_score']

    try:
        # Make prediction using the ML model
        prediction = ml_system.predict_game(home_team, away_team, game_date)

        if prediction:
            predicted_winner = prediction['predicted_winner']
            confidence = prediction['confidence']
            home_win_prob = prediction['home_win_probability']

            # Check if prediction was correct
            correct = (predicted_winner == actual_winner)

            results.append({
                'game_id': game['game_id'],
                'week': game['week'],
                'season': game['season'],
                'matchup': f"{away_team} @ {home_team}",
                'predicted_winner': predicted_winner,
                'actual_winner': actual_winner,
                'confidence': confidence,
                'home_win_prob': home_win_prob,
                'correct': correct,
                'home_score': game['home_score'],
                'away_score': game['away_score']
            })

        if (idx + 1) % 20 == 0:
            print(f"  Processed {idx+1}/{len(games_df)} games...")

    except Exception as e:
        print(f"  ERROR predicting {away_team} @ {home_team}: {e}")

# Convert to DataFrame for analysis
results_df = pd.DataFrame(results)

if len(results_df) == 0:
    print("ERROR: No predictions could be made. Model may be having issues.")
    sys.exit(1)

print(f"\nSuccessfully predicted {len(results_df)} games")
print()
print("=" * 80)
print("OVERALL PERFORMANCE")
print("=" * 80)

# Overall accuracy
overall_accuracy = (results_df['correct'].sum() / len(results_df)) * 100
print(f"Overall Accuracy: {overall_accuracy:.1f}% ({results_df['correct'].sum()}/{len(results_df)} correct)")
print()

# Accuracy by confidence level
print("PERFORMANCE BY CONFIDENCE LEVEL")
print("-" * 80)

confidence_bins = [
    (0.70, 1.00, "Very High (70%+)", "Strong Bet"),
    (0.65, 0.70, "High (65-70%)", "Strong Bet"),
    (0.58, 0.65, "Medium (58-65%)", "Consider"),
    (0.52, 0.58, "Low (52-58%)", "Weak Edge"),
    (0.00, 0.52, "Very Low (<52%)", "Avoid")
]

for min_conf, max_conf, label, recommendation in confidence_bins:
    bin_games = results_df[(results_df['confidence'] >= min_conf) & (results_df['confidence'] < max_conf)]

    if len(bin_games) > 0:
        bin_accuracy = (bin_games['correct'].sum() / len(bin_games)) * 100

        # Calculate expected profit (assuming $100 bets at -110 odds)
        wins = bin_games['correct'].sum()
        losses = len(bin_games) - wins
        profit = (wins * 90.91) - (losses * 100)  # Win $90.91 per $100 bet at -110
        roi = (profit / (len(bin_games) * 100)) * 100

        status = "PROFITABLE" if roi > 0 else "LOSING"

        print(f"{label:25} ({recommendation:12}): {bin_accuracy:5.1f}% ({bin_games['correct'].sum()}/{len(bin_games):2}) | ROI: {roi:+6.1f}% [{status}]")

print()

# Win rate needed to break even at -110 odds (52.38%)
breakeven_rate = 52.38
print(f"BETTING ANALYSIS")
print("-" * 80)
print(f"Break-even win rate at -110 odds: {breakeven_rate:.2f}%")
print(f"Model win rate: {overall_accuracy:.2f}%")

if overall_accuracy > breakeven_rate:
    edge = overall_accuracy - breakeven_rate
    print(f"[+] Model has a {edge:.2f}% edge over break-even")

    # Calculate expected ROI
    total_bets = len(results_df)
    total_wins = results_df['correct'].sum()
    total_losses = total_bets - total_wins

    # Profit calculation (betting $100 per game at -110 odds)
    total_profit = (total_wins * 90.91) - (total_losses * 100)
    total_roi = (total_profit / (total_bets * 100)) * 100

    print(f"Expected ROI: {total_roi:+.2f}%")
    print(f"On $100 bets: ${total_profit:+,.2f} profit on {total_bets} games")
else:
    edge = breakeven_rate - overall_accuracy
    print(f"[-] Model is {edge:.2f}% BELOW break-even - NOT PROFITABLE")

print()

# Show recent performance
print("RECENT PERFORMANCE (Last 20 Games)")
print("-" * 80)
recent_games = results_df.head(20)
for idx, game in recent_games.iterrows():
    status = "[CORRECT]" if game['correct'] else "[WRONG]  "
    season_str = f"S{int(game['season'])}" if pd.notna(game['season']) else "S???"
    week_str = f"W{int(game['week']):2d}" if pd.notna(game['week']) else "W??"
    print(f"{status} {season_str} {week_str}: {game['matchup']:30} | Pred: {game['predicted_winner']:3} ({game['confidence']*100:.1f}%) | Actual: {game['actual_winner']:3} ({game['home_score']}-{game['away_score']})")

print()
print("=" * 80)
print("RECOMMENDATIONS")
print("=" * 80)

# Recommendations based on confidence levels
high_conf_games = results_df[results_df['confidence'] >= 0.65]
if len(high_conf_games) > 0:
    high_conf_accuracy = (high_conf_games['correct'].sum() / len(high_conf_games)) * 100

    if high_conf_accuracy > 60:
        print(f"[RECOMMENDED] Bet on games with 65%+ confidence")
        print(f"   - Win rate at this level: {high_conf_accuracy:.1f}%")
        print(f"   - Sample size: {len(high_conf_games)} games")
    else:
        print(f"[CAUTION] Even high confidence picks only hit {high_conf_accuracy:.1f}%")

med_conf_games = results_df[(results_df['confidence'] >= 0.58) & (results_df['confidence'] < 0.65)]
if len(med_conf_games) > 0:
    med_conf_accuracy = (med_conf_games['correct'].sum() / len(med_conf_games)) * 100

    if med_conf_accuracy > breakeven_rate:
        print(f"[CONSIDER] Medium confidence (58-65%) bets")
        print(f"   - Win rate: {med_conf_accuracy:.1f}% (above break-even)")
        print(f"   - Sample size: {len(med_conf_games)} games")
    else:
        print(f"[AVOID] Medium confidence bets only hit {med_conf_accuracy:.1f}%")

low_conf_games = results_df[results_df['confidence'] < 0.58]
if len(low_conf_games) > 0:
    low_conf_accuracy = (low_conf_games['correct'].sum() / len(low_conf_games)) * 100
    print(f"[AVOID] Low confidence (<58%) bets - only {low_conf_accuracy:.1f}% accurate")

print()
print("=" * 80)
print("FINAL VERDICT")
print("=" * 80)

if overall_accuracy > 55:
    print("[GOOD] Model shows promise - consider using for betting")
    print("[TIP] Stick to high confidence picks (65%+) for best results")
elif overall_accuracy > breakeven_rate:
    print("[MARGINAL] Model is slightly profitable but risky")
    print("[TIP] Only bet high confidence picks and use small stakes")
else:
    print("[BAD] Model is NOT profitable - DO NOT USE FOR BETTING")
    print("[TIP] Model needs more training data or feature engineering")

print()
print(f"Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)
