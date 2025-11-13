# cloud/check_team_names.py
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://postgres:YviqtXqcsCIgRzSCofNjbfwgjkYNLydX@maglev.proxy.rlwy.net:54187/railway"
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

with engine.connect() as conn:
    # Check what names are used for Rams in games table
    rams_games = conn.execute(text("""
        SELECT DISTINCT home_team, away_team
        FROM games 
        WHERE home_team LIKE '%Ram%' OR home_team LIKE '%LA%'
           OR away_team LIKE '%Ram%' OR away_team LIKE '%LA%'
    """)).fetchall()
    
    print("Rams team names in games:")
    for row in rams_games:
        print(f"  {row}")
    
    # Check team_season_summary
    teams = conn.execute(text("""
        SELECT DISTINCT team FROM team_season_summary WHERE season = 2025
    """)).fetchall()
    
    print("\nAll teams in team_season_summary:")
    for t in teams:
        print(f"  {t[0]}")
    
    # Show actual Rams games
    rams_actual = conn.execute(text("""
        SELECT game_date, home_team, away_team, home_score, away_score
        FROM games 
        WHERE (home_team LIKE '%Ram%' OR away_team LIKE '%Ram%'
           OR home_team LIKE '%LA%' OR away_team LIKE '%LA%'
           OR home_team = 'LAR' OR away_team = 'LAR')
        AND home_score IS NOT NULL
        ORDER BY game_date
    """)).fetchall()
    
    print(f"\nRams completed games: {len(rams_actual)}")
    for row in rams_actual:
        print(f"  {row[0]}: {row[1]} vs {row[2]} = {row[3]}-{row[4]}")