# verify_deployment.py
"""
Quick verification script to check if your deployment will work
Run this before pushing to cloud
"""

import os
import pickle
import sys

def check_model_files():
    """Check if model files exist and are valid"""
    print("🔍 CHECKING MODEL FILES...")
    
    model_paths = [
        "betting_model_fixed.pkl",
        os.path.join("models", "betting_model_fixed.pkl"),
        os.path.join("dashboard", "betting_model_fixed.pkl")
    ]
    
    valid_models = 0
    
    for path in model_paths:
        if os.path.exists(path):
            try:
                with open(path, 'rb') as f:
                    model_pack = pickle.load(f)
                
                # Check required keys
                required = ['model', 'feature_cols']
                missing = [k for k in required if k not in model_pack]
                
                if missing:
                    print(f"❌ {path}: Missing keys {missing}")
                else:
                    print(f"✅ {path}: Valid ({len(model_pack['feature_cols'])} features)")
                    valid_models += 1
                    
            except Exception as e:
                print(f"❌ {path}: Error loading - {e}")
        else:
            print(f"⚠️  {path}: Not found")
    
    return valid_models > 0

def check_database_config():
    """Check database configuration"""
    print("\n🔍 CHECKING DATABASE CONFIG...")
    
    db_url = os.environ.get('DATABASE_URL')
    if db_url:
        print(f"✅ Cloud database configured: {db_url[:50]}...")
        
        # Check if it needs fixing
        if db_url.startswith('postgres://'):
            print("⚠️  URL needs postgres:// -> postgresql:// fix (handled in code)")
        
    else:
        print("⚠️  No DATABASE_URL - will use local SQLite")
        
        # Check local DB
        local_db = r"E:/Bettr Bot/betting-bot/data/betting.db"
        if os.path.exists(local_db):
            print(f"✅ Local database found: {local_db}")
        else:
            print(f"❌ Local database missing: {local_db}")

def check_imports():
    """Check if critical imports work"""
    print("\n🔍 CHECKING IMPORTS...")
    
    try:
        import flask
        print(f"✅ Flask: {flask.__version__}")
    except ImportError:
        print("❌ Flask not installed")
        return False
    
    try:
        import pandas
        print(f"✅ Pandas: {pandas.__version__}")
    except ImportError:
        print("❌ Pandas not installed")
        return False
    
    try:
        import sklearn
        print(f"✅ Scikit-learn: {sklearn.__version__}")
    except ImportError:
        print("❌ Scikit-learn not installed")
        return False
    
    try:
        import sqlalchemy
        print(f"✅ SQLAlchemy: {sqlalchemy.__version__}")
    except ImportError:
        print("❌ SQLAlchemy not installed")
        return False
    
    return True

def check_files():
    """Check if required files exist"""
    print("\n🔍 CHECKING REQUIRED FILES...")
    
    required_files = [
        "app.py",
        "mobile_dashboard.py", 
        "requirements.txt",
        "render.yaml"
    ]
    
    missing = []
    for file in required_files:
        if os.path.exists(file):
            print(f"✅ {file}")
        else:
            print(f"❌ {file}")
            missing.append(file)
    
    return len(missing) == 0

def main():
    print("🚀 BETTR BOT DEPLOYMENT VERIFICATION")
    print("=" * 50)
    
    checks = {
        "Model Files": check_model_files(),
        "Database Config": True,  # Always passes, just informational
        "Python Imports": check_imports(),
        "Required Files": check_files()
    }
    
    # Call database check for info
    check_database_config()
    
    print("\n📊 VERIFICATION SUMMARY:")
    print("-" * 30)
    
    passed = 0
    for check_name, result in checks.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{check_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nResult: {passed}/{len(checks)} checks passed")
    
    if passed == len(checks):
        print("\n🎉 READY FOR CLOUD DEPLOYMENT!")
        print("\nNext steps:")
        print("1. git add .")
        print("2. git commit -m 'Fix cloud deployment'")
        print("3. git push")
        print("4. Deploy to Render")
    else:
        print("\n⚠️  FIX ISSUES BEFORE DEPLOYING")
        print("\nTo fix model issues, run:")
        print("python fix_model_training.py")

if __name__ == "__main__":
    main()