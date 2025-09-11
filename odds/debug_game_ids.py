# debug_game_ids.py
"""
Debug script to check game_id formats and fix the mismatch
"""

import sqlite3
import pandas as pd

DB_PATH = r"E:/Bettr Bot/betting-bot/data/betting.db"

def debug_game_ids():
    conn = sqlite3.connect(DB_PATH)
    
    print("DEBUGGING GAME ID MISMATCH")
    print("=" * 40)
    
    # Check games table game_id format
    print("Sample game_ids from games table:")
    games_sample = pd.read_sql_query("""
        SELECT game_id, home_team, away_team, game_date 
        FROM games 
        WHERE home_score IS NOT NULL 
        ORDER BY game_date DESC 
        LIMIT 5
    """, conn)
    for _, row in games_sample.iterrows():
        print(f"  {row['game_id']} | {row['game_date']} | {row['away_team']} @ {row['home_team']}")
    
    # Check odds table game_id format  
    print("\nSample game_ids from odds table:")
    odds_sample = pd.read_sql_query("""
        SELECT DISTINCT game_id, COUNT(*) as odds_count
        FROM odds 
        GROUP BY game_id 
        ORDER BY game_id 
        LIMIT 5
    """, conn)
    for _, row in odds_sample.iterrows():
        print(f"  {row['game_id']} | {row['odds_count']} odds records")
    
    # Check for any matches
    print("\nChecking for matches:")
    match_check = pd.read_sql_query("""
        SELECT 
            COUNT(DISTINCT g.game_id) as total_games,
            COUNT(DISTINCT o.game_id) as total_odds_games,
            COUNT(DISTINCT CASE WHEN o.game_id IS NOT NULL THEN g.game_id END) as matched_games
        FROM games g
        LEFT JOIN odds o ON g.game_id = o.game_id
        WHERE g.home_score IS NOT NULL
    """, conn)
    
    print(f"Total games: {match_check.iloc[0]['total_games']}")
    print(f"Games in odds table: {match_check.iloc[0]['total_odds_games']}")  
    print(f"Matched games: {match_check.iloc[0]['matched_games']}")
    
    conn.close()

def fix_game_id_mismatch():
    """Fix the game_id mismatch by updating odds table"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("\nFIXING GAME ID MISMATCH")
    print("=" * 30)
    
    # Get all games
    games_df = pd.read_sql_query("""
        SELECT game_id, home_team, away_team, game_date
        FROM games 
        WHERE home_score IS NOT NULL
        ORDER BY game_date
    """, conn)
    
    print(f"Processing {len(games_df)} games...")
    
    updated_count = 0
    
    for _, game in games_df.iterrows():
        # Generate the expected odds game_id format
        game_date = pd.to_datetime(game['game_date']).strftime('%Y-%m-%d')
        expected_odds_id = f"{game_date}_{game['home_team']}_{game['away_team']}"
        
        # Check if odds exist with this format
        check_query = "SELECT COUNT(*) FROM odds WHERE game_id = ?"
        count = cursor.execute(check_query, (expected_odds_id,)).fetchone()[0]
        
        if count > 0:
            # Update odds to use the correct game_id
            update_query = "UPDATE odds SET game_id = ? WHERE game_id = ?"
            cursor.execute(update_query, (game['game_id'], expected_odds_id))
            updated_rows = cursor.rowcount
            if updated_rows > 0:
                updated_count += 1
                if updated_count <= 5:  # Show first 5 updates
                    print(f"  Updated: {expected_odds_id} → {game['game_id']}")
    
    conn.commit()
    print(f"Updated game_ids for {updated_count} games")
    
    # Verify the fix
    print("\nVerifying fix...")
    verification = pd.read_sql_query("""
        SELECT 
            COUNT(DISTINCT g.game_id) as total_games,
            COUNT(DISTINCT CASE WHEN o.game_id IS NOT NULL THEN g.game_id END) as matched_games,
            ROUND(100.0 * COUNT(DISTINCT CASE WHEN o.game_id IS NOT NULL THEN g.game_id END) / COUNT(DISTINCT g.game_id), 1) as coverage_pct
        FROM games g
        LEFT JOIN odds o ON g.game_id = o.game_id
        WHERE g.home_score IS NOT NULL
        AND g.game_date > date('now', '-3 years')
    """, conn)
    
    result = verification.iloc[0]
    print(f"Coverage after fix: {result['coverage_pct']}% ({result['matched_games']}/{result['total_games']} games)")
    
    conn.close()
    
    if result['coverage_pct'] > 80:
        print("\n✅ SUCCESS! Game IDs are now properly matched.")
        print("You can now run: python train_betting_model_fixed.py")
    else:
        print(f"\n⚠️  Still low coverage. Manual inspection needed.")

if __name__ == "__main__":
    debug_game_ids()
    
    response = input("\nFix the game_id mismatch? (y/n): ").strip().lower()
    if response == 'y':
        fix_game_id_mismatch()
    else:
        print("Skipping fix. You'll need to resolve the game_id mismatch manually.")