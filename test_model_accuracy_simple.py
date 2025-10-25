#!/usr/bin/env python3
"""
Test ML Model Accuracy via API - Simplified version that uses deployed service
This fetches historical game data and checks how accurate the model would have been
"""

import json
import urllib.request
import urllib.error
from datetime import datetime

print("=" * 80)
print("🎯 NFL ML MODEL ACCURACY TEST (VIA API)")
print("=" * 80)
print()

# Query the Railway database for completed games
print("📅 Querying completed 2024 NFL games...")
print()

query_sql = """
SELECT
    game_id,
    game_date,
    home_team,
    away_team,
    home_score,
    away_score,
    week
FROM games
WHERE season = 2024
AND home_score IS NOT NULL
AND away_score IS NOT NULL
ORDER BY game_date DESC
LIMIT 80
"""

# Database connection string (same as in your env)
import os
os.environ['DATABASE_URL'] = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"

try:
    from sqlalchemy import create_engine, text
    import pandas as pd

    DATABASE_URL = os.environ['DATABASE_URL']
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    with engine.connect() as conn:
        result = conn.execute(text(query_sql))
        games = [dict(row._mapping) for row in result]

    print(f"✅ Found {len(games)} completed games to analyze")
    print()

except Exception as e:
    print(f"❌ Error connecting to database: {e}")
    print("Please ensure DATABASE_URL is set correctly")
    exit(1)

# Now test each game using the model
from model.prediction import FixedNFLSystem

print("🔮 Loading ML model...")
ml_system = FixedNFLSystem()
print(f"✅ Model loaded with {len(ml_system.model_data['feature_cols'])} features")
print()

results = []
print("📊 Testing predictions...")
for i, game in enumerate(games[:80]):  # Test last 80 games
    home_team = game['home_team']
    away_team = game['away_team']
    game_date = game['game_date']

    # Actual result
    actual_winner = home_team if game['home_score'] > game['away_score'] else away_team

    try:
        # Get prediction
        prediction = ml_system.predict_game(home_team, away_team, game_date)

        if prediction:
            predicted_winner = prediction['predicted_winner']
            confidence = prediction['confidence']
            correct = (predicted_winner == actual_winner)

            results.append({
                'week': game['week'],
                'matchup': f"{away_team} @ {home_team}",
                'predicted': predicted_winner,
                'actual': actual_winner,
                'confidence': confidence,
                'correct': correct,
                'score': f"{game['away_score']}-{game['home_score']}"
            })

            if (i + 1) % 10 == 0:
                print(f"  Processed {i+1}/{len(games)} games...")

    except Exception as e:
        print(f"  ⚠️ Error on {away_team} @ {home_team}: {e}")

print()
print("=" * 80)
print("📊 RESULTS")
print("=" * 80)

if len(results) == 0:
    print("❌ No predictions could be made")
    exit(1)

# Overall accuracy
correct_predictions = sum(1 for r in results if r['correct'])
total_predictions = len(results)
overall_accuracy = (correct_predictions / total_predictions) * 100

print(f"\n✅ Overall Accuracy: {overall_accuracy:.1f}% ({correct_predictions}/{total_predictions})")
print()

# Break down by confidence level
print("📈 Performance by Confidence Level:")
print("-" * 80)

confidence_levels = [
    (0.70, 1.00, "Very High (70%+)"),
    (0.65, 0.70, "High (65-70%)"),
    (0.58, 0.65, "Medium (58-65%)"),
    (0.52, 0.58, "Low (52-58%)"),
    (0.00, 0.52, "Very Low (<52%)")
]

for min_conf, max_conf, label in confidence_levels:
    level_results = [r for r in results if min_conf <= r['confidence'] < max_conf]

    if level_results:
        level_correct = sum(1 for r in level_results if r['correct'])
        level_total = len(level_results)
        level_accuracy = (level_correct / level_total) * 100

        # Calculate ROI (assuming -110 odds, need 52.38% to break even)
        wins = level_correct
        losses = level_total - wins
        profit_per_bet = (wins * 90.91) - (losses * 100)
        roi = (profit_per_bet / (level_total * 100)) * 100

        status = "✅ Profitable" if roi > 0 else "❌ Losing"

        print(f"{label:20}: {level_accuracy:5.1f}% ({level_correct}/{level_total:2}) | ROI: {roi:+6.1f}% {status}")

print()
print("💰 BETTING ANALYSIS")
print("-" * 80)
breakeven = 52.38
print(f"Break-even needed (at -110 odds): {breakeven:.2f}%")
print(f"Model accuracy: {overall_accuracy:.2f}%")

if overall_accuracy > breakeven:
    edge = overall_accuracy - breakeven
    print(f"✅ Model has {edge:.2f}% edge!")

    # Calculate total ROI
    total_wins = correct_predictions
    total_losses = total_predictions - total_wins
    total_profit = (total_wins * 90.91) - (total_losses * 100)
    total_roi = (total_profit / (total_predictions * 100)) * 100

    print(f"Expected ROI: {total_roi:+.2f}%")
    print(f"If you bet $100 per game: ${total_profit:+,.2f} on {total_predictions} bets")
else:
    print(f"❌ Model is {breakeven - overall_accuracy:.2f}% BELOW break-even - NOT PROFITABLE")

print()
print("🎯 RECOMMENDATION")
print("=" * 80)

# Check high-confidence games
high_conf = [r for r in results if r['confidence'] >= 0.65]
if high_conf:
    high_conf_accuracy = (sum(1 for r in high_conf if r['correct']) / len(high_conf)) * 100

    if high_conf_accuracy >= 60:
        print(f"✅ RECOMMENDED: Bet on 65%+ confidence picks")
        print(f"   Win rate: {high_conf_accuracy:.1f}% on {len(high_conf)} games")
        print(f"   This is {high_conf_accuracy - breakeven:.1f}% above break-even")
    elif high_conf_accuracy >= breakeven:
        print(f"⚠️  MARGINAL: 65%+ confidence picks at {high_conf_accuracy:.1f}%")
        print(f"   Profitable but risky. Use small bet sizes.")
    else:
        print(f"❌ AVOID: Even high confidence picks only {high_conf_accuracy:.1f}% accurate")

if overall_accuracy < breakeven:
    print()
    print("🚫 DO NOT BET WITH THIS MODEL - IT WILL LOSE MONEY")

print()
print("📋 Last 10 Games:")
print("-" * 80)
for r in results[:10]:
    status = "✅" if r['correct'] else "❌"
    print(f"{status} Wk{r['week']:2} {r['matchup']:35} | Predicted: {r['predicted']:3} ({r['confidence']*100:.0f}%) | Actual: {r['actual']:3} ({r['score']})")

print()
print(f"Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)
