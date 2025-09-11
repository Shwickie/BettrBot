# cleanup_and_regenerate_odds.py
"""
Clean up incorrect odds data and regenerate with proper game_ids
"""

import sqlite3
import pandas as pd

DB_PATH = r"E:/Bettr Bot/betting-bot/data/betting.db"

def cleanup_bad_odds():
    """Remove the incorrectly generated odds"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("Cleaning up incorrect odds data...")
    
    # Count current odds
    before_count = cursor.execute("SELECT COUNT(*) FROM odds").fetchone()[0]
    print(f"Current odds records: {before_count}")
    
    # Delete all odds (since they all have wrong game_ids)
    cursor.execute("DELETE FROM odds")
    conn.commit()
    
    after_count = cursor.execute("SELECT COUNT(*) FROM odds").fetchone()[0]
    print(f"Deleted {before_count - after_count} incorrect odds records")
    
    conn.close()

def main():
    print("ODDS CLEANUP AND REGENERATION")
    print("=" * 40)
    
    response = input("Delete all existing odds and regenerate? (y/n): ").strip().lower()
    if response != 'y':
        print("Cancelled.")
        return
    
    # Clean up bad odds
    cleanup_bad_odds()
    
    print("\nNow run the fixed historical_odds_fetcher.py to regenerate with correct game_ids:")
    print("python historical_odds_fetcher.py")

if __name__ == "__main__":
    main()