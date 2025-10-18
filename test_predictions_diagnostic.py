#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Diagnostic script to test prediction system and identify issues
This will help debug why game predictions aren't using the ML model
"""

import os
import sys
import traceback
from pathlib import Path

# Fix Windows console encoding
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Ensure we can import from the project
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

print("=" * 80)
print("BETTING BOT PREDICTION SYSTEM DIAGNOSTIC")
print("=" * 80)

# Test 1: Check model file exists
print("\n[TEST 1] Checking for model files...")
print("-" * 80)

model_paths = [
    PROJECT_ROOT / "models" / "betting_model_fixed.pkl",
    PROJECT_ROOT / "models" / "betting_model.pkl",
    PROJECT_ROOT / "dashboard" / "betting_model_fixed.pkl",
    PROJECT_ROOT / "cloud" / "models" / "betting_model_fixed.pkl",
]

found_models = []
for path in model_paths:
    exists = path.exists()
    size = path.stat().st_size if exists else 0
    status = "✅ FOUND" if exists else "❌ MISSING"
    print(f"{status} {path}")
    if exists:
        print(f"         Size: {size:,} bytes ({size/1024:.1f} KB)")
        found_models.append(path)

if not found_models:
    print("\n❌ CRITICAL: No model files found!")
    sys.exit(1)
else:
    print(f"\n✅ Found {len(found_models)} model file(s)")

# Test 2: Try importing the prediction system
print("\n[TEST 2] Importing prediction system...")
print("-" * 80)

try:
    from model.prediction import FixedNFLSystem
    print("✅ Successfully imported FixedNFLSystem")
except Exception as e:
    print(f"❌ Failed to import FixedNFLSystem: {e}")
    traceback.print_exc()
    sys.exit(1)

# Test 3: Initialize the prediction system
print("\n[TEST 3] Initializing prediction system...")
print("-" * 80)

try:
    predictor = FixedNFLSystem()
    print("✅ Successfully initialized FixedNFLSystem")
except Exception as e:
    print(f"❌ Failed to initialize: {e}")
    traceback.print_exc()
    sys.exit(1)

# Test 4: Check model was loaded
print("\n[TEST 4] Checking model data...")
print("-" * 80)

if predictor.model_data is None:
    print("❌ CRITICAL: model_data is None")
    sys.exit(1)
else:
    print("✅ model_data exists")
    if isinstance(predictor.model_data, dict):
        print(f"   Keys: {list(predictor.model_data.keys())}")

        if 'model' in predictor.model_data:
            model = predictor.model_data['model']
            if model is None:
                print("❌ CRITICAL: model_data['model'] is None")
                sys.exit(1)
            else:
                print(f"✅ Model object exists: {type(model).__name__}")
                print(f"   Has predict_proba: {hasattr(model, 'predict_proba')}")
        else:
            print("❌ CRITICAL: 'model' key not in model_data")
            sys.exit(1)

        if 'feature_cols' in predictor.model_data:
            features = predictor.model_data['feature_cols']
            print(f"✅ Feature columns: {len(features)} features")
        else:
            print("⚠️  Warning: 'feature_cols' not in model_data")

# Test 5: Check database connection
print("\n[TEST 5] Checking database connection...")
print("-" * 80)

try:
    from model.prediction import get_engine
    engine = get_engine()
    if engine is None:
        print("⚠️  Warning: Database engine is None (will use defaults)")
    else:
        print("✅ Database engine initialized")
        # Try a simple query
        from sqlalchemy import text
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM games"))
            count = result.scalar()
            print(f"   Games in database: {count}")
except Exception as e:
    print(f"⚠️  Database issue: {e}")
    print("   (Will use default team power rankings)")

# Test 6: Check team power data
print("\n[TEST 6] Checking team power data...")
print("-" * 80)

if predictor.team_power_data is None:
    print("❌ team_power_data is None")
else:
    print(f"✅ Team power data loaded: {len(predictor.team_power_data)} teams")
    if not predictor.team_power_data.empty:
        top_team = predictor.team_power_data.iloc[0]
        print(f"   Top team: {top_team['team']} (power: {top_team['power_score']:.1f})")

# Test 7: Test a prediction
print("\n[TEST 7] Testing prediction with real teams...")
print("-" * 80)

test_matchups = [
    ("Kansas City Chiefs", "Buffalo Bills", "2024-10-20"),
    ("Philadelphia Eagles", "Dallas Cowboys", "2024-10-22"),
]

for home, away, date in test_matchups:
    try:
        print(f"\nTesting: {away} @ {home} ({date})")
        pred = predictor.predict_game(home, away, date)

        print(f"✅ Prediction successful!")
        print(f"   Home win prob: {pred['home_win_probability']:.1%}")
        print(f"   Away win prob: {pred['away_win_probability']:.1%}")
        print(f"   Predicted winner: {pred['predicted_winner']}")
        print(f"   Confidence: {pred['confidence']:.1%}")
        print(f"   Power difference: {pred['power_difference']:.2f}")

    except Exception as e:
        print(f"❌ Prediction failed: {e}")
        traceback.print_exc()

# Test 8: Test mobile dashboard integration
print("\n[TEST 8] Testing mobile dashboard integration...")
print("-" * 80)

try:
    # Add cloud directory to path
    cloud_path = PROJECT_ROOT / "cloud"
    if str(cloud_path) not in sys.path:
        sys.path.insert(0, str(cloud_path))

    from cloud.mobile_dashboard import get_ml_prediction_system

    ml_system = get_ml_prediction_system()

    if ml_system is None:
        print("❌ CRITICAL: get_ml_prediction_system() returned None")
        print("\nThis is the main issue! The dashboard can't access the ML system.")
    else:
        print("✅ Dashboard can access ML system")

        # Test if it can make predictions
        try:
            test_pred = ml_system.predict_game(
                "Kansas City Chiefs",
                "Buffalo Bills",
                "2024-10-20"
            )
            print("✅ Dashboard ML system can make predictions")
            print(f"   Test prediction: {test_pred['predicted_winner']} ({test_pred['confidence']:.1%})")
        except Exception as e:
            print(f"❌ Dashboard ML system can't make predictions: {e}")

except Exception as e:
    print(f"❌ Failed to import dashboard: {e}")
    traceback.print_exc()

# Test 9: Check if app.py imports correctly
print("\n[TEST 9] Testing Flask app import...")
print("-" * 80)

try:
    # Don't actually run the app, just import it
    os.environ['TESTING'] = 'true'  # Prevent auto-run

    from cloud.mobile_dashboard import app
    print("✅ Flask app imported successfully")

    # Check if routes are registered
    print(f"   Registered routes: {len(app.url_map._rules)} routes")

    # Find prediction routes
    pred_routes = [r for r in app.url_map._rules if 'prediction' in r.rule]
    print(f"   Prediction routes: {[r.rule for r in pred_routes]}")

except Exception as e:
    print(f"❌ Failed to import Flask app: {e}")
    traceback.print_exc()

# Summary
print("\n" + "=" * 80)
print("DIAGNOSTIC SUMMARY")
print("=" * 80)

issues = []
warnings = []

if not found_models:
    issues.append("No model files found")

if predictor.model_data is None:
    issues.append("Model data not loaded")
elif predictor.model_data.get('model') is None:
    issues.append("Model object is None in model_data")

if predictor.team_power_data is None or predictor.team_power_data.empty:
    warnings.append("Team power data not loaded (using defaults)")

try:
    ml_system = get_ml_prediction_system()
    if ml_system is None:
        issues.append("Dashboard cannot access ML prediction system")
except:
    issues.append("Dashboard integration failed")

if issues:
    print("\n❌ CRITICAL ISSUES FOUND:")
    for i, issue in enumerate(issues, 1):
        print(f"   {i}. {issue}")
else:
    print("\n✅ No critical issues found!")

if warnings:
    print("\n⚠️  WARNINGS:")
    for i, warning in enumerate(warnings, 1):
        print(f"   {i}. {warning}")

print("\n" + "=" * 80)

if issues:
    sys.exit(1)
else:
    print("✅ ALL TESTS PASSED - Prediction system is working!")
    sys.exit(0)
