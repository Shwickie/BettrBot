import sqlite3
import pandas as pd

DB_PATH = r"E:\Bettr Bot\betting-bot\data\betting.db"

def fix_remaining_odds_links():
    """Link odds to games with hash-based game_ids"""
    conn = sqlite3.connect(DB_PATH)
    
    try:
        # Get unlinked odds
        unlinked = pd.read_sql_query("""
            SELECT id, team, timestamp 
            FROM odds 
            WHERE game_id IS NULL 
            ORDER BY timestamp DESC
        """, conn)
        
        print(f"Found {len(unlinked)} unlinked odds")
        
        # Get games with hash game_ids
        hash_games = pd.read_sql_query("""
            SELECT game_id, home_team, away_team, game_date 
            FROM games 
            WHERE length(game_id) > 20  -- Hash format
            AND date(game_date) >= date('now', '-1 day')
        """, conn)
        
        print(f"Found {len(hash_games)} hash-format games")
        
        linked_count = 0
        
        for _, odds_row in unlinked.iterrows():
            team = odds_row['team']
            odds_date = pd.to_datetime(odds_row['timestamp']).date()
            
            # Find matching games by team and date
            for _, game in hash_games.iterrows():
                game_date = pd.to_datetime(game['game_date']).date()
                date_diff = abs((game_date - odds_date).days)
                
                # Check if team matches and date is close
                if (team == game['home_team'] or team == game['away_team']) and date_diff <= 1:
                    conn.execute("""
                        UPDATE odds 
                        SET game_id = ? 
                        WHERE id = ?
                    """, (game['game_id'], odds_row['id']))
                    
                    linked_count += 1
                    
                    if linked_count <= 5:
                        print(f"  Linked: {team} -> {game['game_id'][:12]}...")
                    
                    break  # Found a match, move to next odds record
        
        conn.commit()
        print(f"\nLinked {linked_count} additional odds records")
        
        # Final verification
        final_check = pd.read_sql_query("""
            SELECT 
                COUNT(*) as total,
                COUNT(game_id) as linked,
                COUNT(*) - COUNT(game_id) as unlinked
            FROM odds
        """, conn)
        
        total = final_check.iloc[0]['total']
        linked = final_check.iloc[0]['linked']
        unlinked = final_check.iloc[0]['unlinked']
        
        print(f"\nFinal Status:")
        print(f"  Total odds: {total}")
        print(f"  Linked: {linked}")
        print(f"  Unlinked: {unlinked}")
        print(f"  Success rate: {(linked/total*100):.1f}%")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    fix_remaining_odds_links()
