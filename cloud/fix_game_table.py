# fix_games_table.py - Clean up corrupted games table

import sqlite3
import pandas as pd

DB_PATH = r"E:\Bettr Bot\betting-bot\data\betting.db"

# Team abbreviation mappings
ABBR_TO_FULL = {
    'ARI': 'Arizona Cardinals', 'ATL': 'Atlanta Falcons', 'BAL': 'Baltimore Ravens', 
    'BUF': 'Buffalo Bills', 'CAR': 'Carolina Panthers', 'CHI': 'Chicago Bears',
    'CIN': 'Cincinnati Bengals', 'CLE': 'Cleveland Browns', 'DAL': 'Dallas Cowboys',
    'DEN': 'Denver Broncos', 'DET': 'Detroit Lions', 'GB': 'Green Bay Packers',
    'HOU': 'Houston Texans', 'IND': 'Indianapolis Colts', 'JAX': 'Jacksonville Jaguars',
    'KC': 'Kansas City Chiefs', 'LV': 'Las Vegas Raiders', 'LAC': 'Los Angeles Chargers',
    'LAR': 'Los Angeles Rams', 'MIA': 'Miami Dolphins', 'MIN': 'Minnesota Vikings',
    'NE': 'New England Patriots', 'NO': 'New Orleans Saints', 'NYG': 'New York Giants',
    'NYJ': 'New York Jets', 'PHI': 'Philadelphia Eagles', 'PIT': 'Pittsburgh Steelers',
    'SF': 'San Francisco 49ers', 'SEA': 'Seattle Seahawks', 'TB': 'Tampa Bay Buccaneers',
    'TEN': 'Tennessee Titans', 'WAS': 'Washington Commanders'
}

def normalize_team_name(team):
    """Convert any team name to standard abbreviation"""
    if not team:
        return team
    
    team = str(team).strip()
    
    # If it's already an abbreviation, return it
    if team in ABBR_TO_FULL:
        return team
    
    # If it's a full name, convert to abbreviation
    for abbr, full in ABBR_TO_FULL.items():
        if team == full:
            return abbr
    
    # Handle some edge cases
    team_upper = team.upper()
    if team_upper in ['LA', 'STL']:
        return 'LAR'
    elif team_upper == 'LAS VEGAS RAIDERS':
        return 'LV'
    elif team_upper == 'WASHINGTON COMMANDERS':
        return 'WAS'
    
    return team  # Return as-is if no match

def fix_games_table():
    """Clean up the games table to use consistent team names"""
    conn = sqlite3.connect(DB_PATH)
    
    try:
        # First, let's see what we're working with
        games = pd.read_sql_query("""
            SELECT DISTINCT home_team, away_team 
            FROM games 
            WHERE date(game_date) >= date('now')
        """, conn)
        
        print(f"Found {len(games)} unique game combinations")
        
        # Create a backup first
        print("Creating backup of games table...")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS games_backup AS 
            SELECT * FROM games
        """)
        
        # Update all team names to use abbreviations
        print("Standardizing team names to abbreviations...")
        
        # Get all games that need updating
        all_games = pd.read_sql_query("SELECT * FROM games", conn)
        updated_count = 0
        
        for index, row in all_games.iterrows():
            old_home = row['home_team']
            old_away = row['away_team']
            
            new_home = normalize_team_name(old_home)
            new_away = normalize_team_name(old_away)
            
            if old_home != new_home or old_away != new_away:
                conn.execute("""
                    UPDATE games 
                    SET home_team = ?, away_team = ?
                    WHERE id = ?
                """, (new_home, new_away, row['id']))
                updated_count += 1
                
                print(f"  Updated: {old_away} @ {old_home} -> {new_away} @ {new_home}")
        
        # Remove duplicates (games with same date, home_team, away_team)
        print("Removing duplicate games...")
        
        duplicates_removed = conn.execute("""
            DELETE FROM games 
            WHERE id NOT IN (
                SELECT MIN(id) 
                FROM games 
                GROUP BY game_date, home_team, away_team
            )
        """).rowcount
        
        print(f"Removed {duplicates_removed} duplicate games")
        
        # Generate proper game_ids if missing
        print("Generating proper game_ids...")
        
        games_without_ids = pd.read_sql_query("""
            SELECT id, home_team, away_team, game_date 
            FROM games 
            WHERE game_id IS NULL OR game_id = ''
        """, conn)
        
        for _, game in games_without_ids.iterrows():
            # Create game_id from date and teams
            date_str = str(game['game_date'])[:10].replace('-', '')
            game_id = f"{date_str}_{game['away_team']}_{game['home_team']}"
            
            conn.execute("""
                UPDATE games 
                SET game_id = ? 
                WHERE id = ?
            """, (game_id, game['id']))
        
        print(f"Generated game_ids for {len(games_without_ids)} games")
        
        conn.commit()
        
        # Show final summary
        final_games = pd.read_sql_query("""
            SELECT DISTINCT home_team, away_team 
            FROM games 
            WHERE date(game_date) >= date('now')
        """, conn)
        
        print(f"\nSUCCESS! Games table cleaned:")
        print(f"  - Updated {updated_count} team name records")
        print(f"  - Removed {duplicates_removed} duplicates")
        print(f"  - Generated game_ids for {len(games_without_ids)} games")
        print(f"  - Final unique games: {len(final_games)}")
        
        print("\nSample of cleaned games:")
        sample = pd.read_sql_query("""
            SELECT game_id, away_team, home_team, game_date 
            FROM games 
            WHERE date(game_date) >= date('now')
            ORDER BY game_date 
            LIMIT 5
        """, conn)
        
        for _, game in sample.iterrows():
            print(f"  {game['game_id']}: {game['away_team']} @ {game['home_team']} ({game['game_date']})")
            
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    fix_games_table()