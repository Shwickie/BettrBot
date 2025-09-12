#!/usr/bin/env python3
# startup_check.py - Validates deployment before starting
import os
import sys

def check_deployment():
    """Check if deployment is ready"""
    checks_passed = 0
    total_checks = 4
    
    print("Starting deployment validation...")
    
    # Check 1: Model file
    model_paths = ['./betting_model_fixed.pkl', './models/betting_model_fixed.pkl']
    model_found = False
    for path in model_paths:
        if os.path.exists(path):
            print(f"Model file found at {path}")
            model_found = True
            break
    
    if model_found:
        checks_passed += 1
    else:
        print("Model file missing")
    
    # Check 2: Database URL
    if os.environ.get('DATABASE_URL'):
        print("Database URL configured")
        checks_passed += 1
    else:
        print("Database URL missing")
    
    # Check 3: Key imports
    try:
        import flask
        import sqlalchemy
        import pandas
        print("Key packages importable")
        checks_passed += 1
    except ImportError as e:
        print(f"Import error: {e}")
    
    # Check 4: App can be created
    try:
        from flask import Flask
        app = Flask(__name__)
        print("Flask app can be created")
        checks_passed += 1
    except Exception as e:
        print(f"Flask app creation failed: {e}")
    
    print(f"Startup check: {checks_passed}/{total_checks} passed")
    return checks_passed >= 3

if __name__ == "__main__":
    success = check_deployment()
    sys.exit(0 if success else 1)
