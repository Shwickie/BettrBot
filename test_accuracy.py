#!/usr/bin/env python3
"""Test ML Model Accuracy - No Emojis Version"""

import os
os.environ['DATABASE_URL'] = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"

from sqlalchemy import create_engine, text
from model.prediction import FixedNFLSystem
from datetime import datetime

print("="*80)
print("NFL ML MODEL ACCURACY TEST")
print("="*80)
print()

# Connect to database
DATABASE_URL = os.environ['DATABASE_URL']
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Get completed games
query_sql = """
SELECT game_id, game_date, home_team, away_team, home_score, away_score, week
FROM games
WHERE season = 2024 AND home_score IS NOT NULL AND away_score IS NOT NULL
ORDER BY game_date DESC
LIMIT 80
"""

print("Fetching completed 2024 NFL games...")
with engine.connect() as conn:
    result = conn.execute(text(query_sql))
    games = [dict(row._mapping) for row in result]

print(f"Found {len(games)} completed games")
print()

# Load model
print("Loading ML model...")
ml_system = FixedNFLSystem()
print(f"Model loaded with {len(ml_system.model_data['feature_cols'])} features")
print()

# Test predictions
results = []
print("Testing predictions on historical games...")
for i, game in enumerate(games):
    home_team = game['home_team']
    away_team = game['away_team']
    game_date = game['game_date']

    actual_winner = home_team if game['home_score'] > game['away_score'] else away_team

    try:
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
        print(f"  Error on {away_team} @ {home_team}: {e}")

print()
print("="*80)
print("RESULTS")
print("="*80)

if len(results) == 0:
    print("ERROR: No predictions could be made")
    exit(1)

# Overall accuracy
correct = sum(1 for r in results if r['correct'])
total = len(results)
accuracy = (correct / total) * 100

print(f"\nOverall Accuracy: {accuracy:.1f}% ({correct}/{total})")
print()

# By confidence level
print("Performance by Confidence Level:")
print("-"*80)

levels = [
    (0.70, 1.00, "Very High (70%+)"),
    (0.65, 0.70, "High (65-70%)"),
    (0.58, 0.65, "Medium (58-65%)"),
    (0.52, 0.58, "Low (52-58%)"),
    (0.00, 0.52, "Very Low (<52%)")
]

for min_c, max_c, label in levels:
    level_games = [r for r in results if min_c <= r['confidence'] < max_c]

    if level_games:
        level_correct = sum(1 for r in level_games if r['correct'])
        level_total = len(level_games)
        level_acc = (level_correct / level_total) * 100

        # ROI calculation (at -110 odds)
        wins = level_correct
        losses = level_total - wins
        profit = (wins * 90.91) - (losses * 100)
        roi = (profit / (level_total * 100)) * 100

        status = "PROFITABLE" if roi > 0 else "LOSING"

        print(f"{label:20}: {level_acc:5.1f}% ({level_correct}/{level_total:2}) | ROI: {roi:+6.1f}% [{status}]")

print()
print("BETTING ANALYSIS")
print("-"*80)
breakeven = 52.38
print(f"Break-even needed at -110 odds: {breakeven:.2f}%")
print(f"Model accuracy: {accuracy:.2f}%")

if accuracy > breakeven:
    edge = accuracy - breakeven
    print(f"[POSITIVE] Model has {edge:.2f}% edge over break-even")

    # Total ROI
    total_wins = correct
    total_losses = total - correct
    total_profit = (total_wins * 90.91) - (total_losses * 100)
    total_roi = (total_profit / (total * 100)) * 100

    print(f"Expected ROI: {total_roi:+.2f}%")
    print(f"$100 bets on {total} games = ${total_profit:+,.2f} profit")
else:
    print(f"[NEGATIVE] Model is {breakeven - accuracy:.2f}% BELOW break-even")
    print("[WARNING] This model will LOSE MONEY - do not bet!")

print()
print("RECOMMENDATION")
print("="*80)

# High confidence analysis
high_conf = [r for r in results if r['confidence'] >= 0.65]
if high_conf:
    high_acc = (sum(1 for r in high_conf if r['correct']) / len(high_conf)) * 100

    if high_acc >= 60:
        print(f"[RECOMMENDED] Bet on games with 65%+ confidence")
        print(f"  Win rate: {high_acc:.1f}% ({len(high_conf)} games tested)")
        print(f"  Edge: {high_acc - breakeven:.1f}% above break-even")
    elif high_acc >= breakeven:
        print(f"[MARGINAL] 65%+ confidence at {high_acc:.1f}%")
        print(f"  Profitable but risky - use small stakes")
    else:
        print(f"[AVOID] Even high confidence only {high_acc:.1f}%")

if accuracy < breakeven:
    print()
    print("[DO NOT BET] Model is not profitable")

print()
print("Last 10 Games:")
print("-"*80)
for r in results[:10]:
    status = "CORRECT" if r['correct'] else "WRONG  "
    print(f"[{status}] Wk{r['week']:2} {r['matchup']:35} Pred:{r['predicted']:3} ({r['confidence']*100:.0f}%) Act:{r['actual']:3} ({r['score']})")

print()
print(f"Report: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)
