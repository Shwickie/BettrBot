#!/usr/bin/env python3
"""
Quick Database Structure Check
Run this first to understand your current database setup
"""

import pandas as pd
from sqlalchemy import create_engine, text, inspect

# Your database path
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"

def quick_inspection():
    """Quick database inspection to understand structure"""
    print("🔍 QUICK DATABASE INSPECTION")
    print("=" * 40)
    
    try:
        engine = create_engine(DB_PATH)
        inspector = inspect(engine)
        
        # Get all tables
        tables = inspector.get_table_names()
        print(f"📋 Available Tables: {tables}")
        
        with engine.connect() as conn:
            # Check key tables
            for table in ['player_game_stats', 'nfl_injuries_tracking']:
                if table in tables:
                    print(f"\n🔍 {table.upper()}:")
                    
                    # Get count
                    count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                    print(f"  Records: {count:,}")
                    
                    # Get columns
                    columns = inspector.get_columns(table)
                    col_names = [col['name'] for col in columns]
                    print(f"  Columns: {col_names}")
                    
                    # Sample data
                    if count > 0:
                        sample = pd.read_sql(text(f"SELECT * FROM {table} LIMIT 2"), conn)
                        print(f"  Sample:")
                        for i, (_, row) in enumerate(sample.iterrows()):
                            print(f"    Row {i+1}: {dict(list(row.items())[:5])}...")  # First 5 columns
                    
                    # Special checks
                    if table == 'nfl_injuries_tracking':
                        # Check dates
                        date_check = conn.execute(text("""
                            SELECT MIN(date) as min_date, MAX(date) as max_date, 
                                   COUNT(DISTINCT date) as unique_dates
                            FROM nfl_injuries_tracking
                        """)).fetchone()
                        
                        if date_check:
                            print(f"  Date range: {date_check[0]} to {date_check[1]} ({date_check[2]} unique dates)")
                        
                        # Check teams
                        team_check = pd.read_sql(text("""
                            SELECT team, COUNT(*) as count 
                            FROM nfl_injuries_tracking 
                            GROUP BY team 
                            ORDER BY count DESC 
                            LIMIT 5
                        """), conn)
                        
                        print(f"  Top teams: {list(team_check.itertuples(index=False, name=None))}")
                    
                    elif table == 'player_game_stats':
                        # Check seasons
                        season_check = pd.read_sql(text("""
                            SELECT season, COUNT(DISTINCT player_id) as players
                            FROM player_game_stats 
                            GROUP BY season 
                            ORDER BY season DESC
                        """), conn)
                        
                        print(f"  Seasons: {list(season_check.itertuples(index=False, name=None))}")
                
                else:
                    print(f"\n❌ {table} table not found")
            
            print(f"\n✅ Quick inspection complete")
            return True
            
    except Exception as e:
        print(f"❌ Inspection failed: {e}")
        return False

if __name__ == "__main__":
    quick_inspection()