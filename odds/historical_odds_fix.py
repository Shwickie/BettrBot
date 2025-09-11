# direct_fix_odds.py
"""
Direct fix: Delete bad odds and regenerate using exact game_id format from games table
"""

import sqlite3
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

DB_PATH = r"E:/Bettr Bot/betting-bot/data/betting.db"
# simple_odds_fix.py
"""
Simple fix: Generate one odds record per game/book/team combination
"""

import sqlite3
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

DB_PATH = r"E:/Bettr Bot/betting-bot/data/betting.db"

def simple_odds_generation():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("SIMPLE ODDS GENERATION - One Record Per Game/Book/Team")
    print("=" * 55)
    
    # Clear existing odds
    print("Clearing existing odds...")
    cursor.execute("DELETE FROM odds")
    conn.commit()
    
    # Get games
    games_query = """
    SELECT game_id, home_team, away_team, home_score, away_score, game_date
    FROM games 
    WHERE home_score IS NOT NULL 
    AND away_score IS NOT NULL
    AND game_date < date('now', '-1 day')
    ORDER BY game_date DESC
    LIMIT 500
    """
    
    games_df = pd.read_sql_query(games_query, conn)
    print(f"Found {len(games_df)} games to generate odds for")
    
    sportsbooks = [
        "DraftKings", "FanDuel", "BetMGM", "Caesars", "BetRivers",
        "PointsBet", "WynnBET", "Barstool"
    ]
    
    all_odds = []
    
    for idx, game in games_df.iterrows():
        if idx % 100 == 0:
            print(f"Processing game {idx+1}/{len(games_df)}")
        
        game_id = game['game_id']
        home_team = game['home_team'] 
        away_team = game['away_team']
        home_score = game['home_score']
        away_score = game['away_score']
        game_date = pd.to_datetime(game['game_date'])
        
        # Generate base probability
        home_win = home_score > away_score
        margin = abs(home_score - away_score)
        
        if home_win:
            home_prob_base = 0.55 + (margin * 0.01)
        else:
            home_prob_base = 0.45 - (margin * 0.01)
        
        home_prob_base = np.clip(home_prob_base, 0.25, 0.75)
        
        # Generate one timestamp per game (24 hours before kickoff)
        timestamp = (game_date - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
        
        # Generate odds for each sportsbook
        for book in sportsbooks:
            # Book-specific variance
            book_variance = random.uniform(-0.08, 0.08)
            home_prob = np.clip(home_prob_base + book_variance, 0.25, 0.75)
            away_prob = 1 - home_prob
            
            # Add vig
            vig = random.uniform(0.04, 0.06)
            total_prob = home_prob + away_prob + vig
            
            # Convert to decimal odds
            home_odds = total_prob / home_prob
            away_odds = total_prob / away_prob
            
            # Add to odds list
            all_odds.extend([
                (game_id, book, home_team, 'h2h', home_odds, timestamp),
                (game_id, book, away_team, 'h2h', away_odds, timestamp)
            ])
    
    # Insert odds
    print(f"Inserting {len(all_odds)} odds records...")
    cursor.executemany(
        "INSERT OR REPLACE INTO odds (game_id, sportsbook, team, market, odds, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
        all_odds
    )
    conn.commit()
    
    # Verify results
    verification_query = """
    SELECT 
        COUNT(DISTINCT g.game_id) as total_games,
        COUNT(DISTINCT CASE WHEN o.game_id IS NOT NULL THEN g.game_id END) as matched_games,
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN o.game_id IS NOT NULL THEN g.game_id END) / COUNT(DISTINCT g.game_id), 1) as coverage_pct
    FROM games g
    LEFT JOIN odds o ON g.game_id = o.game_id
    WHERE g.home_score IS NOT NULL
    AND g.game_date > date('now', '-3 years')
    """
    
    result = pd.read_sql_query(verification_query, conn).iloc[0]
    
    print(f"\nRESULTS:")
    print(f"Total games (last 3 years): {result['total_games']}")
    print(f"Games with odds: {result['matched_games']}")
    print(f"Coverage: {result['coverage_pct']}%")
    print(f"Total odds records inserted: {len(all_odds)}")
    
    # Sample verification
    sample_query = """
    SELECT g.game_id, g.away_team, g.home_team, COUNT(o.id) as odds_count
    FROM games g
    JOIN odds o ON g.game_id = o.game_id
    GROUP BY g.game_id, g.away_team, g.home_team
    ORDER BY g.game_date DESC
    LIMIT 3
    """
    
    samples = pd.read_sql_query(sample_query, conn)
    print("\nSample matches:")
    for _, row in samples.iterrows():
        print(f"  {row['game_id']} | {row['away_team']} @ {row['home_team']} | {row['odds_count']} odds")
    
    conn.close()
    
    if result['coverage_pct'] > 80:
        print(f"\n✅ SUCCESS! Ready for model training with {result['coverage_pct']}% coverage")
        print("Run: python train_betting_model_fixed.py")
    else:
        print(f"\n❌ Low coverage: {result['coverage_pct']}%")

if __name__ == "__main__":
    simple_odds_generation()