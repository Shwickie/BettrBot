#!/usr/bin/env python3
"""
Model verification script - run this to check which models are being loaded
"""
import os
import pickle
import sys

# Add project root to path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

def check_model_file(path):
    """Check what's in a model file"""
    if not os.path.exists(path):
        return f"❌ File does not exist: {path}"
    
    try:
        with open(path, 'rb') as f:
            model_pack = pickle.load(f)
        
        feature_count = len(model_pack.get('feature_cols', []))
        metrics = model_pack.get('model_metrics', {})
        auc = metrics.get('auc', metrics.get('RandomForest', {}).get('auc', 'Unknown'))
        
        return f"✓ {path}\n  Features: {feature_count}\n  AUC: {auc}\n  Keys: {list(model_pack.keys())}"
    except Exception as e:
        return f"❌ Error loading {path}: {e}"

def main():
    print("=== MODEL VERIFICATION ===\n")
    
    # Check both model files
    old_model = r"E:/Bettr Bot/betting-bot/models/betting_model.pkl"
    new_model = r"E:/Bettr Bot/betting-bot/models/betting_model_fixed.pkl"
    
    print("1. OLD MODEL:")
    print(check_model_file(old_model))
    print()
    
    print("2. NEW MODEL:")
    print(check_model_file(new_model))
    print()
    
    # Check environment variable
    env_path = os.getenv("BETTR_MODEL_PKL")
    print(f"3. ENVIRONMENT VARIABLE:")
    print(f"  BETTR_MODEL_PKL = {env_path}")
    if env_path:
        print(f"  Points to: {check_model_file(env_path)}")
    print()
    
    # Test actual imports
    print("4. TESTING IMPORTS:")
    try:
        from dashboard.ai_chat_stub import load_model_pack
        model = load_model_pack()
        if model:
            features = len(model.get('feature_cols', []))
            print(f"  AI Chat Stub loaded model with {features} features")
        else:
            print("  AI Chat Stub: No model loaded")
    except Exception as e:
        print(f"  AI Chat Stub import error: {e}")
    
    try:
        from mobile_dashboard import load_model_pack as dash_load
        model = dash_load()
        if model:
            features = len(model.get('feature_cols', []))
            print(f"  Dashboard loaded model with {features} features")
        else:
            print("  Dashboard: No model loaded")
    except Exception as e:
        print(f"  Dashboard import error: {e}")

if __name__ == "__main__":
    main()