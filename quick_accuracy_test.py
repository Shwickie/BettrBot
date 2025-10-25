"""Quick accuracy test using live bettr-bot service"""
import urllib.request
import json

print("="*80)
print("TESTING ML MODEL ACCURACY")
print("="*80)
print()

# Fetch predictions from live service
print("Fetching predictions from https://bettr-bot.onrender.com...")
try:
    with urllib.request.urlopen('https://bettr-bot.onrender.com/api/predictions') as response:
        predictions = json.loads(response.read())

    print(f"Got {len(predictions)} predictions")

    # Check if using ML
    ml_count = sum(1 for p in predictions if p.get('using_ml_model'))
    print(f"Using ML Model: {ml_count}/{len(predictions)}")
    print()

    if ml_count == 0:
        print("ERROR: Not using ML model!")
        exit(1)

    # Now fetch completed games from database to test accuracy
    print("Connecting to database to test historical accuracy...")

    import os
    os.environ['DATABASE_URL'] = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"

    from sqlalchemy import create_engine, text
    from model.prediction import FixedNFLSystem

    DATABASE_URL = os.environ['DATABASE_URL']
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    # Get last 50 completed games
    query = text("""
        SELECT game_id, game_date, home_team, away_team, home_score, away_score, week
        FROM games
        WHERE season = 2024 AND home_score IS NOT NULL AND away_score IS NOT NULL
        ORDER BY game_date DESC
        LIMIT 50
    """)

    with engine.connect() as conn:
        result = conn.execute(query)
        games = [dict(row._mapping) for row in result]

    print(f"Testing on {len(games)} completed games...")
    print()

    # Load model and test
    ml_system = FixedNFLSystem()

    results = []
    for game in games:
        home = game['home_team']
        away = game['away_team']
        actual_winner = home if game['home_score'] > game['away_score'] else away

        try:
            pred = ml_system.predict_game(home, away, game['game_date'])
            if pred:
                predicted = pred['predicted_winner']
                confidence = pred['confidence']
                correct = (predicted == actual_winner)

                results.append({
                    'week': game['week'],
                    'matchup': f"{away} @ {home}",
                    'predicted': predicted,
                    'actual': actual_winner,
                    'confidence': confidence,
                    'correct': correct
                })
        except Exception as e:
            pass

    print("="*80)
    print("RESULTS")
    print("="*80)

    if not results:
        print("ERROR: Could not test any games")
        exit(1)

    # Overall
    correct = sum(1 for r in results if r['correct'])
    total = len(results)
    accuracy = (correct / total) * 100

    print(f"\nOverall: {accuracy:.1f}% accurate ({correct}/{total})")
    print()

    # By confidence
    print("By Confidence Level:")
    print("-"*80)

    for min_c, max_c, label in [(0.65, 1.0, "High (65%+)"), (0.58, 0.65, "Med (58-65%)"), (0.0, 0.58, "Low (<58%)")]:
        level = [r for r in results if min_c <= r['confidence'] < max_c]
        if level:
            lc = sum(1 for r in level if r['correct'])
            lt = len(level)
            la = (lc / lt) * 100

            # ROI at -110 odds
            wins, losses = lc, lt - lc
            profit = (wins * 90.91) - (losses * 100)
            roi = (profit / (lt * 100)) * 100

            status = "PROFIT" if roi > 0 else "LOSS"
            print(f"{label:15}: {la:5.1f}% ({lc:2}/{lt:2}) ROI: {roi:+6.1f}% [{status}]")

    print()
    print("RECOMMENDATION:")
    print("-"*80)

    # Break even is 52.38% at -110 odds
    if accuracy > 52.38:
        edge = accuracy - 52.38
        print(f"[PROFITABLE] {edge:.1f}% edge over break-even")

        # Check high confidence
        high = [r for r in results if r['confidence'] >= 0.65]
        if high:
            hc = sum(1 for r in high if r['correct'])
            ha = (hc / len(high)) * 100
            if ha >= 60:
                print(f"[BET] Use high confidence picks (65%+): {ha:.1f}% win rate")
            else:
                print(f"[CAUTION] Even high confidence only {ha:.1f}%")
    else:
        print(f"[DO NOT BET] Model is {52.38 - accuracy:.1f}% below break-even")

    print()
    print("Sample of last 5 games:")
    print("-"*80)
    for r in results[:5]:
        s = "WIN " if r['correct'] else "LOSS"
        print(f"[{s}] {r['matchup']:30} Pred:{r['predicted']:3} Act:{r['actual']:3} ({r['confidence']*100:.0f}%)")

    print()
    print("="*80)

except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
