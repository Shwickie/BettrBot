#!/usr/bin/env python3
"""
Add ties column to team_season_summary table
"""

from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql://postgres:YviqtXqcsCIgRzSCofNjbfwgjkYNLydX@maglev.proxy.rlwy.net:54187/railway"

def add_ties_column():
    print("Adding ties column to team_season_summary...")
    
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    
    with engine.connect() as conn:
        # Check if ties column exists
        result = conn.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'team_season_summary' 
            AND column_name = 'ties'
        """)).fetchone()
        
        if not result:
            print("  Adding ties column...")
            conn.execute(text("""
                ALTER TABLE team_season_summary 
                ADD COLUMN ties INTEGER DEFAULT 0
            """))
            conn.commit()
            print("  ✓ Ties column added")
        else:
            print("  ✓ Ties column already exists")
        
        # Verify
        columns = conn.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'team_season_summary'
            ORDER BY ordinal_position
        """)).fetchall()
        
        print(f"\n  Current columns: {', '.join([c[0] for c in columns])}")

if __name__ == "__main__":
    add_ties_column()