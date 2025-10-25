#!/usr/bin/env python3
"""
Fix team names in games table to match odds table abbreviations
This solves the Philadelphia Eagles / PHI mismatch issue
"""

from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"
engine = create_engine(DATABASE_URL)

# Mapping of full names to abbreviations
TEAM_STANDARDIZATION = {
    'Philadelphia Eagles': 'PHI',
    'Pittsburgh Steelers': 'PIT',
    'Green Bay Packers': 'GB',
    'Kansas City Chiefs': 'KC',
    'Buffalo Bills': 'BUF',
    'San Francisco 49ers': 'SF',
    'Dallas Cowboys': 'DAL',
    'New York Giants': 'NYG',
    'New York Jets': 'NYJ',
    'New England Patriots': 'NE',
    'Miami Dolphins': 'MIA',
    'Baltimore Ravens': 'BAL',
    'Cincinnati Bengals': 'CIN',
    'Cleveland Browns': 'CLE',
    'Pittsburgh Steelers': 'PIT',
    'Houston Texans': 'HOU',
    'Indianapolis Colts': 'IND',
    'Jacksonville Jaguars': 'JAX',
    'Tennessee Titans': 'TEN',
    'Denver Broncos': 'DEN',
    'Las Vegas Raiders': 'LV',
    'Los Angeles Chargers': 'LAC',
    'Minnesota Vikings': 'MIN',
    'Chicago Bears': 'CHI',
    'Detroit Lions': 'DET',
    'Atlanta Falcons': 'ATL',
    'Carolina Panthers': 'CAR',
    'New Orleans Saints': 'NO',
    'Tampa Bay Buccaneers': 'TB',
    'Arizona Cardinals': 'ARI',
    'Los Angeles Rams': 'LAR',
    'Seattle Seahawks': 'SEA',
    'Washington Commanders': 'WAS'
}

print("=" * 80)
print("STANDARDIZING TEAM NAMES IN GAMES TABLE")
print("=" * 80)

with engine.connect() as conn:
    # First, check what needs fixing
    print("\nChecking for full team names in games table...")
    result = conn.execute(text("""
        SELECT DISTINCT home_team FROM games
        WHERE LENGTH(home_team) > 3
        UNION
        SELECT DISTINCT away_team FROM games
        WHERE LENGTH(away_team) > 3
        ORDER BY 1
    """))

    full_names = [row[0] for row in result]

    if not full_names:
        print("[OK] No full team names found - all teams already standardized!")
    else:
        print(f"\nFound {len(full_names)} full team names to fix:")
        for name in full_names:
            abbr = TEAM_STANDARDIZATION.get(name, '???')
            print(f"  - {name} -> {abbr}")

        print("\nApplying fixes...")
        total_updates = 0

        for full_name, abbr in TEAM_STANDARDIZATION.items():
            # Update home_team
            result = conn.execute(text("""
                UPDATE games
                SET home_team = :abbr
                WHERE home_team = :full_name
            """), {'abbr': abbr, 'full_name': full_name})
            home_updates = result.rowcount

            # Update away_team
            result = conn.execute(text("""
                UPDATE games
                SET away_team = :abbr
                WHERE away_team = :full_name
            """), {'abbr': abbr, 'full_name': full_name})
            away_updates = result.rowcount

            if home_updates > 0 or away_updates > 0:
                print(f"  FIXED: {full_name} -> {abbr} ({home_updates + away_updates} games updated)")
                total_updates += home_updates + away_updates

        conn.commit()

        print(f"\n[OK] Total games updated: {total_updates}")

    # Verify fix
    print("\n" + "=" * 80)
    print("VERIFICATION")
    print("=" * 80)

    # Check Eagles game specifically
    result = conn.execute(text("""
        SELECT game_id, home_team, away_team, game_date
        FROM games
        WHERE (home_team LIKE '%Eagles%' OR away_team LIKE '%Eagles%'
               OR home_team = 'PHI' OR away_team = 'PHI')
        AND game_date >= '2025-10-26'
        LIMIT 5
    """))

    print("\nEagles games:")
    for row in result:
        print(f"  {row.away_team} @ {row.home_team} ({row.game_date})")

    # Check if odds will now match
    result = conn.execute(text("""
        SELECT COUNT(*) as count
        FROM games g
        JOIN odds o ON g.game_id = o.game_id
        WHERE (g.home_team = 'PHI' OR g.away_team = 'PHI')
        AND g.game_date >= '2025-10-26'
    """))

    eagles_odds_count = result.scalar()
    print(f"\nEagles games with matching odds: {eagles_odds_count}")

    if eagles_odds_count > 0:
        print("[SUCCESS] Eagles game now has odds!")
    else:
        print("[WARNING] Eagles game still doesn't have odds (may need to fetch odds)")

print("\n" + "=" * 80)
print("FIX COMPLETE")
print("=" * 80)
