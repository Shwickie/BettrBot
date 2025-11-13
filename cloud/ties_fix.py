from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://postgres:YviqtXqcsCIgRzSCofNjbfwgjkYNLydX@maglev.proxy.rlwy.net:54187/railway"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

with engine.connect() as conn:
    # Add ties column with default value 0
    conn.execute(text("""
        ALTER TABLE team_season_summary 
        ADD COLUMN IF NOT EXISTS ties INTEGER DEFAULT 0
    """))
    conn.commit()
    print("✅ Added 'ties' column to team_season_summary")

print("\nDone!")