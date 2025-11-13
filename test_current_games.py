#!/usr/bin/env python3
"""
Test the CURRENT model on TOMORROW'S games with REAL odds
"""

import os
os.environ['PYTHONIOENCODING'] = 'utf-8'

from model.prediction import FixedNFLSystem
from sqlalchemy import create_engine, text
import pandas as pd

DATABASE_URL = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"
engine = create_engine(DATABASE_URL)

print("=" * 80)
print("TESTING CURRENT MODEL ON TOMORROW'S GAMES")
print("=" * 80)

# Load model
print("\nLoading model...")
ml_system = FixedNFLSystem()
print(f"Model: {len(ml_system.model_data['feature_cols'])} features")
print(f"Type: {type(ml_system.model).__name__}")
print()

# Tomorrow's games with real odds
games = [
    {'home': 'PHI', 'away': 'NYG', 'date': '2025-10-26', 'home_odds': -395, 'away_odds': +310},
    {'home': 'ATL', 'away': 'MIA', 'date': '2025-10-26', 'home_odds': -375, 'away_odds': +295},
    {'home': 'BAL', 'away': 'CHI', 'date': '2025-10-26', 'home_odds': -125, 'away_odds': +105},
    {'home': 'CIN', 'away': 'NYJ', 'date': '2025-10-26', 'home_odds': -285, 'away_odds': +230},
    {'home': 'HOU', 'away': 'SF', 'date': '2025-10-26', 'home_odds': -130, 'away_odds': +110},
    {'home': 'CAR', 'away': 'BUF', 'date': '2025-10-26', 'home_odds': +310, 'away_odds': -395},
]

print("=" * 80)
print("PREDICTIONS VS REAL MARKET ODDS")
print("=" * 80)
print()

results = []

for game in games:
    home = game['home']
    away = game['away']

    # Get prediction
    pred = ml_system.predict_game(home, away, game['date'])

    home_prob = pred['home_win_probability']
    away_prob = pred['away_win_probability']
    confidence = pred['confidence']
    pick = pred['predicted_winner']

    # Calculate implied probabilities from real odds
    if game['home_odds'] > 0:
        home_implied = 100 / (game['home_odds'] + 100)
    else:
        home_implied = abs(game['home_odds']) / (abs(game['home_odds']) + 100)

    if game['away_odds'] > 0:
        away_implied = 100 / (game['away_odds'] + 100)
    else:
        away_implied = abs(game['away_odds']) / (abs(game['away_odds']) + 100)

    # Determine which bet to make (if any)
    home_edge = home_prob - home_implied
    away_edge = away_prob - away_implied

    best_bet = None
    best_edge = 0
    best_odds = 0

    if home_edge >= 0.025 and home_prob >= 0.58:  # 2.5% edge, 58% confidence
        best_bet = home
        best_edge = home_edge
        best_odds = game['home_odds']

    if away_edge >= 0.025 and away_prob >= 0.58:
        if away_edge > home_edge:
            best_bet = away
            best_edge = away_edge
            best_odds = game['away_odds']

    results.append({
        'game': f"{away} @ {home}",
        'model_pick': pick,
        'model_conf': confidence,
        'home_prob': home_prob,
        'away_prob': away_prob,
        'home_odds': game['home_odds'],
        'away_odds': game['away_odds'],
        'home_implied': home_implied,
        'away_implied': away_implied,
        'home_edge': home_edge,
        'away_edge': away_edge,
        'bet_recommendation': best_bet,
        'bet_edge': best_edge,
        'bet_odds': best_odds
    })

# Print results
for r in results:
    print(f"Game: {r['game']}")
    print(f"  Model Pick: {r['model_pick']} ({r['model_conf']*100:.1f}% confidence)")
    print(f"  Probabilities: {r['home_prob']*100:.1f}% home, {r['away_prob']*100:.1f}% away")
    print(f"  Market Odds: {r['home_odds']:+d} home, {r['away_odds']:+d} away")
    print(f"  Implied Prob: {r['home_implied']*100:.1f}% home, {r['away_implied']*100:.1f}% away")
    print(f"  Edge: {r['home_edge']*100:+.1f}% home, {r['away_edge']*100:+.1f}% away")

    if r['bet_recommendation']:
        print(f"  >>> BET: {r['bet_recommendation']} ({r['bet_odds']:+d}) - Edge: {r['bet_edge']*100:.1f}%")
    else:
        reasons = []
        if r['model_conf'] < 0.58:
            reasons.append("confidence too low")
        if abs(r['home_edge']) < 0.025 and abs(r['away_edge']) < 0.025:
            reasons.append("edge too small")
        if r['home_edge'] < 0 and r['away_edge'] < 0:
            reasons.append("negative edge on both sides")
        print(f"  >>> SKIP: {', '.join(reasons)}")

    print()

print("=" * 80)
print("BETTING SUMMARY")
print("=" * 80)

bets = [r for r in results if r['bet_recommendation']]

if not bets:
    print("\nNO BETS RECOMMENDED")
    print("Reasons:")
    print("  - All games have negative edge OR")
    print("  - Model confidence below 58% OR")
    print("  - Edge below 2.5% minimum")
    print("\nRECOMMENDATION: Skip all bets this week")
else:
    print(f"\n{len(bets)} RECOMMENDED BET(S):")
    print()
    for r in bets:
        print(f"{r['game']}")
        print(f"  Bet: {r['bet_recommendation']} ({r['bet_odds']:+d})")
        print(f"  Model Confidence: {r['model_conf']*100:.1f}%")
        print(f"  Edge: {r['bet_edge']*100:+.1f}%")
        print(f"  Suggested Stake: $10-15")
        print()

print("=" * 80)
