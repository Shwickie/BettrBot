#!/usr/bin/env python3
"""
Script to regenerate the model pickle with current pandas version
Run this on Render to fix the pandas compatibility issue
"""
import sys
import os

# Add parent directory to path to import the training module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'model'))

from train_betting_model import main

if __name__ == "__main__":
    print("="*60)
    print("REGENERATING MODEL WITH CURRENT PANDAS VERSION")
    print("="*60)
    main()
