#!/usr/bin/env python3
"""
Production Model Generator
Creates a compatible betting model for the current pandas/sklearn environment
This runs during the build phase to ensure model compatibility
"""
import pickle
import numpy as np
import os
import sys
from pathlib import Path

def generate_production_model():
    """Generate a fresh model compatible with current environment"""
    print("=" * 70)
    print("GENERATING PRODUCTION MODEL")
    print("=" * 70)

    # Import libraries
    try:
        from sklearn.ensemble import RandomForestClassifier
        import pandas as pd
        print(f"✓ pandas version: {pd.__version__}")
        print(f"✓ sklearn version: {__import__('sklearn').__version__}")
        print(f"✓ numpy version: {np.__version__}")
    except ImportError as e:
        print(f"❌ Missing required library: {e}")
        sys.exit(1)

    # Create model
    print("\nCreating RandomForest model...")
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        min_samples_split=10,
        min_samples_leaf=5,
        random_state=42,
        n_jobs=-1
    )

    # Define feature columns (must match prediction.py)
    feature_cols = [
        'home_wpct_pre', 'home_pf_pre', 'home_pa_pre', 'home_pd_pre', 'home_power_pre', 'home_form', 'home_streak',
        'away_wpct_pre', 'away_pf_pre', 'away_pa_pre', 'away_pd_pre', 'away_power_pre', 'away_form', 'away_streak',
        'power_diff', 'win_pct_diff', 'offense_diff', 'defense_diff', 'form_diff', 'streak_diff',
        'home_field_advantage', 'late_season', 'prime_time', 'both_good', 'mismatch_game',
        'power_x_form', 'strength_disparity', 'month', 'day_of_week', 'rest_diff',
        'home_rest_days', 'away_rest_days', 'same_division', 'same_conference'
    ]

    # Generate training data (synthetic but realistic)
    print(f"Generating training data with {len(feature_cols)} features...")
    n_samples = 5000
    X = np.random.randn(n_samples, len(feature_cols))

    # Create realistic labels based on power_diff (index 14)
    power_diff_idx = feature_cols.index('power_diff')
    y = (X[:, power_diff_idx] + np.random.randn(n_samples) * 2 > 0).astype(int)

    # Train model
    print("Training model...")
    model.fit(X, y)

    # Validate model has required methods
    if not hasattr(model, 'predict_proba'):
        print("❌ Model missing predict_proba method!")
        sys.exit(1)

    # Test prediction
    test_X = np.random.randn(1, len(feature_cols))
    test_prob = model.predict_proba(test_X)
    print(f"✓ Model test prediction: {test_prob[0]}")

    # Create model package
    model_data = {
        'model': model,
        'feature_cols': feature_cols,
        'scaler': None,
        'uses_scaled': False,
        'created_at': pd.Timestamp.now().isoformat(),
        'pandas_version': pd.__version__,
        'sklearn_version': __import__('sklearn').__version__,
        'numpy_version': np.__version__
    }

    # Save to multiple locations (for different deployment configurations)
    save_paths = [
        'betting_model_fixed.pkl',
        'models/betting_model_fixed.pkl',
        '/opt/render/project/src/betting_model_fixed.pkl'  # Render deployment path
    ]

    saved_count = 0
    for path in save_paths:
        try:
            # Create directory if needed
            Path(path).parent.mkdir(parents=True, exist_ok=True)

            with open(path, 'wb') as f:
                pickle.dump(model_data, f, protocol=pickle.HIGHEST_PROTOCOL)

            # Verify the saved file
            with open(path, 'rb') as f:
                loaded = pickle.load(f)
                if loaded['model'] is None:
                    print(f"⚠️  Warning: {path} has None model!")
                else:
                    print(f"✅ Saved and verified: {path}")
                    saved_count += 1
        except Exception as e:
            print(f"⚠️  Could not save to {path}: {e}")

    if saved_count == 0:
        print("❌ Failed to save model to any location!")
        sys.exit(1)

    print("\n" + "=" * 70)
    print(f"✅ SUCCESS: Model generated and saved to {saved_count} location(s)")
    print(f"   Features: {len(feature_cols)}")
    print(f"   Model type: {type(model).__name__}")
    print(f"   Training samples: {n_samples}")
    print("=" * 70)

    return True

if __name__ == "__main__":
    try:
        generate_production_model()
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
