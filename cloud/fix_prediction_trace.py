#!/usr/bin/env python3
"""
DIAGNOSTIC: Trace why ML predictions aren't working
Run this to find out what's broken
"""

import os
import sys
from pathlib import Path

# Find repo root
REPO_ROOT = Path(__file__).parent
while not (REPO_ROOT / '.git').exists() and REPO_ROOT != REPO_ROOT.parent:
    REPO_ROOT = REPO_ROOT.parent

print(f"Repo root: {REPO_ROOT}")
sys.path.insert(0, str(REPO_ROOT))

def test_model_loading():
    """Test if the model file loads correctly"""
    print("\n" + "="*60)
    print("TEST 1: Can we load the model file?")
    print("="*60)
    
    import pickle
    
    model_paths = [
        REPO_ROOT / "betting_model_fixed.pkl",
        REPO_ROOT / "models" / "betting_model_fixed.pkl",
        REPO_ROOT / "dashboard" / "betting_model_fixed.pkl",
        REPO_ROOT / "cloud" / "betting_model_fixed.pkl",
    ]
    
    model_path = None
    for path in model_paths:
        if path.exists():
            model_path = path
            print(f"✅ Found model at: {path}")
            break
    
    if not model_path:
        print("❌ No model file found!")
        return False
    
    try:
        with open(model_path, 'rb') as f:
            model_pack = pickle.load(f)
        
        print(f"✅ Model loaded successfully")
        print(f"   Keys in model pack: {list(model_pack.keys())}")
        
        # Check required keys
        if 'model' not in model_pack:
            print("❌ CRITICAL: 'model' key missing!")
            return False
        
        if 'feature_cols' not in model_pack:
            print("⚠️  WARNING: 'feature_cols' missing - this will cause issues")
            # Try to get from model
            model = model_pack['model']
            if hasattr(model, 'feature_names_in_'):
                print(f"   Found {len(model.feature_names_in_)} features in model")
            else:
                print("❌ Cannot determine feature columns!")
                return False
        else:
            print(f"✅ Found {len(model_pack['feature_cols'])} feature columns")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to load model: {e}")
        return False

def test_prediction_system():
    """Test if FixedNFLSystem initializes"""
    print("\n" + "="*60)
    print("TEST 2: Can we initialize FixedNFLSystem?")
    print("="*60)
    
    try:
        from model.prediction import FixedNFLSystem
        
        system = FixedNFLSystem()
        
        if not system.model_data:
            print("❌ System initialized but model_data is None")
            return False
        
        if not system.model_data.get('model'):
            print("❌ System initialized but model is None")
            return False
        
        print("✅ FixedNFLSystem initialized successfully")
        print(f"   Model type: {type(system.model_data['model'])}")
        print(f"   Has team data: {system.team_power_data is not None}")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to initialize: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_actual_prediction():
    """Test making an actual prediction"""
    print("\n" + "="*60)
    print("TEST 3: Can we make a prediction?")
    print("="*60)
    
    try:
        from model.prediction import FixedNFLSystem
        
        system = FixedNFLSystem()
        
        # Test prediction
        result = system.predict_game(
            "Kansas City Chiefs",
            "Buffalo Bills",
            "2025-10-19"
        )
        
        print("✅ Prediction successful!")
        print(f"   Home (KC): {result['home_win_probability']:.3f}")
        print(f"   Away (BUF): {result['away_win_probability']:.3f}")
        print(f"   Confidence: {result['confidence']:.3f}")
        
        # Check if it's realistic
        if result['confidence'] > 0.85:
            print("\n⚠️  WARNING: Confidence too high!")
            print("   This suggests the model isn't calibrated properly")
            print("   Expected range: 0.52 - 0.75 for most games")
            return False
        
        if result['confidence'] < 0.52:
            print("\n⚠️  WARNING: Confidence too low!")
            print("   This suggests prediction is too uncertain")
            return False
        
        print("\n✅ Confidence is in realistic range (0.52-0.75)")
        return True
        
    except Exception as e:
        print(f"❌ Prediction failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_dashboard_integration():
    """Test if dashboard can use the model"""
    print("\n" + "="*60)
    print("TEST 4: Dashboard integration")
    print("="*60)
    
    try:
        # Import dashboard functions
        sys.path.insert(0, str(REPO_ROOT / "dashboard"))
        from mobile_dashboard import get_ml_prediction_system
        
        ml_system = get_ml_prediction_system()
        
        if not ml_system:
            print("❌ Dashboard cannot initialize ML system")
            print("\nPossible causes:")
            print("  1. Model file not in expected location")
            print("  2. Model file corrupt")
            print("  3. Import path issue")
            return False
        
        print("✅ Dashboard can initialize ML system")
        
        # Test prediction through dashboard
        result = ml_system.predict_game(
            "Philadelphia Eagles",
            "Dallas Cowboys",
            "2025-10-19"
        )
        
        print(f"✅ Dashboard prediction works")
        print(f"   Confidence: {result['confidence']:.3f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Dashboard integration failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def diagnose_power_rankings():
    """Check if power rankings are causing issues"""
    print("\n" + "="*60)
    print("TEST 5: Power rankings analysis")
    print("="*60)
    
    try:
        from sqlalchemy import create_engine, text
        import pandas as pd
        
        DATABASE_URL = os.environ.get("DATABASE_URL", 
            "postgresql+psycopg2://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway")
        
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        
        with engine.connect() as conn:
            # Check power score distribution
            power_data = pd.read_sql(text("""
                SELECT team, power_score, wins, losses
                FROM team_season_summary
                WHERE season = 2025
                ORDER BY power_score DESC
            """), conn)
            
            if power_data.empty:
                print("❌ No power rankings found for 2025!")
                return False
            
            print(f"✅ Found power rankings for {len(power_data)} teams")
            print(f"\nTop 5 teams:")
            for _, row in power_data.head(5).iterrows():
                print(f"   {row['team']}: {row['power_score']:.1f} ({row['wins']}-{row['losses']})")
            
            print(f"\nBottom 5 teams:")
            for _, row in power_data.tail(5).iterrows():
                print(f"   {row['team']}: {row['power_score']:.1f} ({row['wins']}-{row['losses']})")
            
            # Check for unrealistic spreads
            max_power = power_data['power_score'].max()
            min_power = power_data['power_score'].min()
            spread = max_power - min_power
            
            print(f"\nPower score spread: {spread:.1f}")
            
            if spread > 25:
                print("⚠️  WARNING: Power score spread is very large!")
                print("   This will cause extreme predictions")
                return False
            
            if spread < 5:
                print("⚠️  WARNING: Power score spread is too small!")
                print("   This will cause predictions to be too close")
                return False
            
            print("✅ Power score spread is reasonable (5-25 range)")
            return True
        
    except Exception as e:
        print(f"❌ Power rankings check failed: {e}")
        return False

def main():
    print("\n" + "="*70)
    print("🔍 PREDICTION SYSTEM DIAGNOSTIC")
    print("="*70)
    
    results = {
        'model_loading': test_model_loading(),
        'system_init': test_prediction_system(),
        'prediction': test_actual_prediction(),
        'dashboard': test_dashboard_integration(),
        'power_rankings': diagnose_power_rankings(),
    }
    
    print("\n" + "="*70)
    print("📊 DIAGNOSTIC SUMMARY")
    print("="*70)
    
    for test, passed in results.items():
        symbol = "✅" if passed else "❌"
        print(f"{symbol} {test.replace('_', ' ').title()}: {'PASSED' if passed else 'FAILED'}")
    
    if all(results.values()):
        print("\n" + "="*70)
        print("🎉 ALL TESTS PASSED!")
        print("="*70)
        print("\nYour prediction system is working correctly.")
        print("If you're still seeing high confidence:")
        print("  1. Check the fallback formula in mobile_dashboard.py")
        print("  2. Ensure ML system is being used (not fallback)")
    else:
        print("\n" + "="*70)
        print("⚠️  ISSUES FOUND")
        print("="*70)
        print("\nFailed tests indicate:")
        
        if not results['model_loading']:
            print("\n❌ MODEL FILE ISSUE:")
            print("   - Model file missing or corrupt")
            print("   - Solution: Retrain model")
            print("     python train_betting_model.py")
        
        if not results['system_init']:
            print("\n❌ SYSTEM INITIALIZATION ISSUE:")
            print("   - FixedNFLSystem cannot start")
            print("   - Check imports and paths")
        
        if not results['prediction']:
            print("\n❌ PREDICTION ISSUE:")
            print("   - Model making unrealistic predictions")
            print("   - Check model calibration")
            print("   - Power rankings may be too extreme")
        
        if not results['dashboard']:
            print("\n❌ DASHBOARD INTEGRATION ISSUE:")
            print("   - Dashboard cannot use ML system")
            print("   - Check get_ml_prediction_system() function")
        
        if not results['power_rankings']:
            print("\n❌ POWER RANKINGS ISSUE:")
            print("   - Rankings are unrealistic")
            print("   - This causes extreme predictions")
            print("   - Run: python cloud_run_all.py")

if __name__ == "__main__":
    main()