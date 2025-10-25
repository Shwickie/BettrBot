#!/usr/bin/env python3
"""Quick script to diagnose odds issue"""

from sqlalchemy import create_engine, text
import pandas as pd

DATABASE_URL = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"
engine = create_engine(DATABASE_URL)

print("=" * 80)
print("ODDS DATABASE DIAGNOSTIC")
print("=" * 80)

with engine.connect() as conn:
    # Check teams in odds table
    result = conn.execute(text('SELECT DISTINCT team FROM odds ORDER BY team'))
    teams = [row[0] for row in result]

    print(f"\nTeams in odds table ({len(teams)} teams):")
    for team in teams:
        print(f"  - {team}")

    # Check for Philadelphia/Eagles
    print("\n" + "=" * 80)
    print("Checking for Philadelphia Eagles:")
    result = conn.execute(text("""
        SELECT team, COUNT(*) as count
        FROM odds
        WHERE team LIKE '%Phil%' OR team LIKE '%PHI%' OR team LIKE '%Eagles%'
        GROUP BY team
    """))
    philly_odds = result.fetchall()
    if philly_odds:
        for row in philly_odds:
            print(f"  Found: {row[0]} ({row[1]} odds records)")
    else:
        print("  NO PHILADELPHIA EAGLES ODDS FOUND!")

    # Check what games exist
    print("\n" + "=" * 80)
    print("Games in database:")
    result = conn.execute(text("""
        SELECT game_id, home_team, away_team, game_date
        FROM games
        WHERE game_date >= '2025-10-26'
        ORDER BY game_date
        LIMIT 5
    """))
    games_df = pd.DataFrame(result.fetchall(), columns=result.keys())
    print(games_df.to_string())

    # Check which games have odds
    print("\n" + "=" * 80)
    print("Games WITH odds:")
    result = conn.execute(text("""
        SELECT DISTINCT g.home_team, g.away_team, g.game_date, COUNT(o.odds) as odds_count
        FROM games g
        JOIN odds o ON g.game_id = o.game_id
        WHERE g.game_date >= '2025-10-26'
        GROUP BY g.game_id, g.home_team, g.away_team, g.game_date
        ORDER BY g.game_date
    """))
    games_with_odds = pd.DataFrame(result.fetchall(), columns=result.keys())
    if not games_with_odds.empty:
        print(games_with_odds.to_string())
    else:
        print("  NO GAMES WITH ODDS FOR TOMORROW!")

    # Check sample odds data
    print("\n" + "=" * 80)
    print("Sample odds from database (most recent):")
    result = conn.execute(text("""
        SELECT team, odds, sportsbook, timestamp
        FROM odds
        ORDER BY timestamp DESC
        LIMIT 10
    """))
    sample_df = pd.DataFrame(result.fetchall(), columns=result.keys())
    print(sample_df.to_string())

    # Check last update time
    print("\n" + "=" * 80)
    result = conn.execute(text("SELECT MAX(timestamp) as last_update FROM odds"))
    last_update = result.fetchone()[0]
    print(f"Last odds update: {last_update}")
    print(f"Hours ago: {(pd.Timestamp.now() - pd.Timestamp(last_update)).total_seconds() / 3600:.1f}")

print("\n" + "=" * 80)
print("DIAGNOSTIC COMPLETE")
print("=" * 80)
