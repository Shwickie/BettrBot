#!/usr/bin/env python3
"""
DEBUG version of migrate_odds.py - Shows actual error messages
"""

import requests
import os
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import traceback

# API Configuration
API_KEY = '2ea42e6f961b41a105cd8dac8a3490a8'
SPORT = 'americanfootball_nfl'
REGIONS = 'us'
ODDS_FORMAT = 'american'

# Database Configuration
DATABASE_URL = "postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres"

# Team name mapping
TEAM_MAPPING = {
    'Arizona Cardinals': 'ARI', 'Atlanta Falcons': 'ATL', 'Baltimore Ravens': 'BAL',
    'Buffalo Bills': 'BUF', 'Carolina Panthers': 'CAR', 'Chicago Bears': 'CHI',
    'Cincinnati Bengals': 'CIN', 'Cleveland Browns': 'CLE', 'Dallas Cowboys': 'DAL',
    'Denver Broncos': 'DEN', 'Detroit Lions': 'DET', 'Green Bay Packers': 'GB',
    'Houston Texans': 'HOU', 'Indianapolis Colts': 'IND', 'Jacksonville Jaguars': 'JAX',
    'Kansas City Chiefs': 'KC', 'Las Vegas Raiders': 'LV', 'Los Angeles Chargers': 'LAC',
    'Los Angeles Rams': 'LAR', 'Miami Dolphins': 'MIA', 'Minnesota Vikings': 'MIN',
    'New England Patriots': 'NE', 'New Orleans Saints': 'NO', 'New York Giants': 'NYG',
    'New York Jets': 'NYJ', 'Philadelphia Eagles': 'PHI', 'Pittsburgh Steelers': 'PIT',
    'San Francisco 49ers': 'SF', 'Seattle Seahawks': 'SEA', 'Tampa Bay Buccaneers': 'TB',
    'Tennessee Titans': 'TEN', 'Washington Commanders': 'WAS'
}

def debug_database_schema():
    """Debug the actual database schema"""
    print("=== DEBUGGING DATABASE SCHEMA ===")
    
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    
    try:
        with engine.connect() as conn:
            # Check odds table structure
            print("Odds table structure:")
            odds_schema = conn.execute(text("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_name = 'odds'
                ORDER BY ordinal_position
            """)).fetchall()
            
            for col in odds_schema:
                print(f"  {col[0]}: {col[1]} (nullable: {col[2]}, default: {col[3]})")
            
            # Check constraints
            print("\nOdds table constraints:")
            constraints = conn.execute(text("""
                SELECT constraint_name, constraint_type 
                FROM information_schema.table_constraints 
                WHERE table_name = 'odds'
            """)).fetchall()
            
            for constraint in constraints:
                print(f"  {constraint[0]}: {constraint[1]}")
            
            # Check for unique constraints details
            print("\nUnique constraint details:")
            unique_constraints = conn.execute(text("""
                SELECT tc.constraint_name, kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                  ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_name = 'odds' 
                AND tc.constraint_type = 'UNIQUE'
            """)).fetchall()
            
            for uc in unique_constraints:
                print(f"  {uc[0]} on column: {uc[1]}")
            
            # Check existing data
            print(f"\nExisting odds count: {conn.execute(text('SELECT COUNT(*) FROM odds')).scalar()}")
            
            # Show sample existing data
            sample_odds = conn.execute(text("""
                SELECT * FROM odds LIMIT 3
            """)).fetchall()
            
            print("Sample existing odds:")
            for odds in sample_odds:
                print(f"  {odds}")
                
    except Exception as e:
        print(f"Schema debug error: {e}")
        traceback.print_exc()

def test_single_insert():
    """Test inserting a single odds record with full error details"""
    print("\n=== TESTING SINGLE INSERT ===")
    
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    
    test_data = {
        'game_id': '4dd631102a977fd398f7ea594ed208f2',  # From your log
        'team': 'MIN',
        'sportsbook': 'TestBook',
        'odds': -150,
        'market': 'h2h',
        'timestamp': datetime.utcnow()
    }
    
    print(f"Testing insert with data: {test_data}")
    
    try:
        with engine.connect() as conn:
            with conn.begin():
                # Try simple insert first
                print("Trying simple INSERT...")
                result = conn.execute(text("""
                    INSERT INTO odds (game_id, team, sportsbook, odds, market, timestamp)
                    VALUES (:game_id, :team, :sportsbook, :odds, :market, :timestamp)
                """), test_data)
                print(f"✓ Simple INSERT succeeded: {result.rowcount} rows")
                
    except Exception as e:
        print(f"✗ Simple INSERT failed: {e}")
        print(f"Error type: {type(e)}")
        traceback.print_exc()
        
        # Try UPSERT
        try:
            with engine.connect() as conn:
                with conn.begin():
                    print("Trying UPSERT...")
                    result = conn.execute(text("""
                        INSERT INTO odds (game_id, team, sportsbook, odds, market, timestamp)
                        VALUES (:game_id, :team, :sportsbook, :odds, :market, :timestamp)
                        ON CONFLICT (game_id, team, sportsbook, market) 
                        DO UPDATE SET 
                            odds = EXCLUDED.odds,
                            timestamp = EXCLUDED.timestamp
                    """), test_data)
                    print(f"✓ UPSERT succeeded: {result.rowcount} rows")
        except Exception as e2:
            print(f"✗ UPSERT failed: {e2}")
            print(f"Error type: {type(e2)}")
            
            # Try without ON CONFLICT
            try:
                with engine.connect() as conn:
                    with conn.begin():
                        print("Trying INSERT without ON CONFLICT...")
                        result = conn.execute(text("""
                            INSERT INTO odds (game_id, team, sportsbook, odds, market, timestamp)
                            VALUES (:game_id, :team, :sportsbook, :odds, :market, :timestamp)
                        """), {
                            'game_id': test_data['game_id'] + '_test',  # Make it unique
                            'team': test_data['team'],
                            'sportsbook': 'TestBook2',
                            'odds': test_data['odds'],
                            'market': test_data['market'],
                            'timestamp': test_data['timestamp']
                        })
                        print(f"✓ Modified INSERT succeeded: {result.rowcount} rows")
            except Exception as e3:
                print(f"✗ All INSERT methods failed: {e3}")
                traceback.print_exc()

def check_game_ids():
    """Check if game_ids from API match what's in database"""
    print("\n=== CHECKING GAME ID MATCHES ===")
    
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    
    try:
        with engine.connect() as conn:
            # Show some actual game_ids from database
            game_ids = conn.execute(text("""
                SELECT game_id, home_team, away_team, game_date 
                FROM games 
                WHERE game_date >= CURRENT_DATE
                ORDER BY game_date
                LIMIT 5
            """)).fetchall()
            
            print("Sample game_ids in database:")
            for game in game_ids:
                print(f"  {game[0]} | {game[2]} @ {game[1]} | {game[3]}")
                
    except Exception as e:
        print(f"Game ID check error: {e}")

def main():
    """Debug main execution"""
    print("ODDS MIGRATION DEBUG")
    print("=" * 50)
    
    # 1. Check database schema
    debug_database_schema()
    
    # 2. Test single insert
    test_single_insert()
    
    # 3. Check game ID format
    check_game_ids()
    
    print("\n" + "=" * 50)
    print("DEBUG COMPLETE")
    print("This should reveal the exact cause of insert failures")

if __name__ == "__main__":
    main()