#!/usr/bin/env python3
"""
Simple script to add 'engine = ENGINE' to prediction.py
"""

import os
from pathlib import Path

def find_prediction_file():
    """Find prediction.py in the repo"""
    possible_locations = [
        Path("E:/Bettr Bot/betting-bot/model/prediction.py"),
        Path("E:/Bettr Bot/betting-bot/cloud/model/prediction.py"),
        Path(__file__).parent / "model" / "prediction.py",
        Path(__file__).parent.parent / "model" / "prediction.py",
    ]
    
    for location in possible_locations:
        if location.exists():
            return location
    
    return None

def add_engine_variable():
    """Add 'engine = ENGINE' to prediction.py"""
    
    prediction_file = find_prediction_file()
    
    if not prediction_file:
        print("❌ Could not find prediction.py")
        print("\nManual fix:")
        print("1. Open E:/Bettr Bot/betting-bot/model/prediction.py")
        print("2. Find the line that says:")
        print("   ENGINE = create_engine(...)")
        print("3. After the closing parenthesis of ENGINE creation, add:")
        print("   engine = ENGINE")
        return False
    
    print(f"Found prediction.py at: {prediction_file}")
    
    # Read the file
    with open(prediction_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Check if fix already applied
    for line in lines:
        if 'engine = ENGINE' in line:
            print("✅ Fix already applied - engine variable exists")
            return True
    
    # Find a good place to add it
    # Look for the ENGINE creation and the print statement after it
    insert_index = None
    
    for i, line in enumerate(lines):
        # Look for the print statement after ENGINE creation
        if 'print("✅ Using Railway PostgreSQL")' in line or 'Using Railway PostgreSQL' in line:
            insert_index = i + 1
            break
    
    if insert_index is None:
        # Fallback: look for ENGINE = create_engine
        for i, line in enumerate(lines):
            if 'ENGINE = create_engine' in line:
                # Find the end of this statement (closing paren)
                j = i
                while j < len(lines) and ')' not in lines[j]:
                    j += 1
                insert_index = j + 1
                break
    
    if insert_index is None:
        print("❌ Could not find where to insert the fix")
        print("\nAdd this manually after ENGINE creation:")
        print("engine = ENGINE")
        return False
    
    # Insert the fix
    lines.insert(insert_index, '\n')
    lines.insert(insert_index + 1, '# Global engine reference for compatibility\n')
    lines.insert(insert_index + 2, 'engine = ENGINE\n')
    lines.insert(insert_index + 3, '\n')
    
    # Write back
    with open(prediction_file, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    
    print(f"✅ Added 'engine = ENGINE' to {prediction_file}")
    return True

def test_fix():
    """Test that the fix works"""
    print("\nTesting the fix...")
    
    try:
        import sys
        sys.path.insert(0, "E:/Bettr Bot/betting-bot")
        
        # Clear any cached module
        if 'model.prediction' in sys.modules:
            del sys.modules['model.prediction']
        
        from model.prediction import FixedNFLSystem
        
        system = FixedNFLSystem()
        
        result = system.predict_game(
            "Kansas City Chiefs",
            "Buffalo Bills",
            "2025-10-19"
        )
        
        print(f"✅ Prediction successful!")
        print(f"   Home (KC): {result['home_win_probability']:.1%}")
        print(f"   Away (BUF): {result['away_win_probability']:.1%}")
        print(f"   Confidence: {result['confidence']:.1%}")
        
        if result['confidence'] < 0.85:
            print("✅ Confidence looks realistic - ML model is working!")
            return True
        else:
            print("⚠️  Very high confidence - may still be using fallback")
            return False
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def main():
    print("="*60)
    print("ADDING ENGINE VARIABLE TO PREDICTION.PY")
    print("="*60 + "\n")
    
    success = add_engine_variable()
    
    if success:
        test_success = test_fix()
        
        if test_success:
            print("\n" + "="*60)
            print("🎉 ALL FIXES COMPLETE!")
            print("="*60)
            print("\nYour dashboard should now work!")
            print("\nRestart it with:")
            print("  python dashboard/mobile_dashboard.py")
        else:
            print("\n" + "="*60)
            print("⚠️  Engine added but test failed")
            print("="*60)
            print("\nTry restarting Python and testing again")
    else:
        print("\n" + "="*60)
        print("❌ Could not auto-fix")
        print("="*60)
        print("\nManual steps:")
        print("1. Open: E:/Bettr Bot/betting-bot/model/prediction.py")
        print("2. Find line ~70 that says: ENGINE = create_engine(...)")
        print("3. After the closing ) add these lines:")
        print("")
        print("   # Global engine reference")
        print("   engine = ENGINE")
        print("")
        print("4. Save and restart dashboard")

if __name__ == "__main__":
    main()