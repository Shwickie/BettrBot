#!/usr/bin/env python3
from sqlalchemy import create_engine, text
import pandas as pd

DATABASE_URL = "postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

with engine.connect() as conn:
    # Check the specific games
    print("RAMS-COLTS GAME:")
    rams_colts = pd.read_sql(text("""
        SELECT game_id, game_date, home_team, away_team, home_score, away_score
        FROM games
        WHERE (home_team = 'Los Angeles Rams' AND away_team = 'IND')
           OR (away_team = 'Los Angeles Rams' AND home_team = 'IND')
    """), conn)
    
    if not rams_colts.empty:
        for _, g in rams_colts.iterrows():
            print(f"  Date: {g['game_date']}")
            print(f"  {g['away_team']} @ {g['home_team']}")
            print(f"  Score: {g['away_score']} - {g['home_score']}")
            print(f"  Has scores: {g['home_score'] is not None and g['away_score'] is not None}")
    
    print("\nRAMS-EAGLES GAME:")
    rams_eagles = pd.read_sql(text("""
        SELECT game_id, game_date, home_team, away_team, home_score, away_score
        FROM games
        WHERE (home_team = 'Los Angeles Rams' AND away_team IN ('PHI', 'Philadelphia Eagles'))
           OR (away_team = 'Los Angeles Rams' AND home_team IN ('PHI', 'Philadelphia Eagles'))
    """), conn)
    
    if not rams_eagles.empty:
        for _, g in rams_eagles.iterrows():
            print(f"  Date: {g['game_date']}")
            print(f"  {g['away_team']} @ {g['home_team']}")
            print(f"  Score: {g['away_score']} - {g['home_score']}")
            print(f"  Has scores: {g['home_score'] is not None and g['away_score'] is not None}")