# check_model.py
import pickle
import os

# Check what's in your model file
model_paths = [
    "betting_model_fixed.pkl",
    "../models/betting_model_fixed.pkl",
    "../dashboard/betting_model_fixed.pkl"
]

for path in model_paths:
    if os.path.exists(path):
        print(f"Found model at: {path}")
        try:
            with open(path, 'rb') as f:
                model_pack = pickle.load(f)
            
            print(f"Keys in model pack: {list(model_pack.keys())}")
            
            if 'feature_cols' not in model_pack:
                print("ERROR: Missing feature_cols!")
            else:
                print(f"feature_cols length: {len(model_pack['feature_cols'])}")
                
            if 'scaler' not in model_pack:
                print("ERROR: Missing scaler!")
            else:
                print(f"scaler type: {type(model_pack['scaler'])}")
                
            if 'model' not in model_pack:
                print("ERROR: Missing model!")
            else:
                print(f"model type: {type(model_pack['model'])}")
                
        except Exception as e:
            print(f"Error loading {path}: {e}")
        break
else:
    print("No model file found!")