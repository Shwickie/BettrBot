# check_games_schema.py - Check the actual structure of your games table
"""
Check what columns exist in your cloud games table
"""

import os
from sqlalchemy import create_engine, text, inspect

DATABASE_URL = os.environ.get("DATABASE_URL") or "postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres?sslmode=require"

if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

def main():
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    
    try:
        with engine.connect() as conn:
            # Get table schema
            inspector = inspect(engine)
            columns = inspector.get_columns('games')
            
            print("GAMES TABLE SCHEMA:")
            print("=" * 30)
            for col in columns:
                print(f"  {col['name']}: {col['type']}")
            
            # Check sample data
            sample = conn.execute(text("""
                SELECT * FROM games 
                ORDER BY game_date DESC 
                LIMIT 3
            """)).fetchall()
            
            print(f"\nSAMPLE DATA (Total rows: {conn.execute(text('SELECT COUNT(*) FROM games')).scalar()}):")
            print("=" * 50)
            
            if sample:
                # Print column headers
                print("  " + " | ".join([col['name'] for col in columns]))
                print("  " + "-" * 80)
                
                # Print sample rows
                for row in sample:
                    values = []
                    for val in row:
                        if val is None:
                            values.append("NULL")
                        else:
                            values.append(str(val)[:20])  # Truncate long values
                    print("  " + " | ".join(values))
            
            # Check date range
            date_range = conn.execute(text("""
                SELECT MIN(game_date) as earliest, MAX(game_date) as latest
                FROM games
            """)).fetchone()
            
            print(f"\nDATE RANGE: {date_range[0]} to {date_range[1]}")
            
            # Check if there are any future games
            future_count = conn.execute(text("""
                SELECT COUNT(*) FROM games 
                WHERE game_date >= CURRENT_DATE
            """)).scalar()
            
            print(f"FUTURE GAMES: {future_count}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()