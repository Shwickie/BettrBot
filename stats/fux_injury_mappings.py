#!/usr/bin/env python3
"""
Aggressive cleanup of stale injuries and fix team assignments.
This will deactivate injuries for players not on current rosters
and fix team assignments for players who have changed teams.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, bindparam


DB = r"sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB, connect_args={'timeout': 60})

def kcanon(name: str) -> str:
    """Canonical key for player name matching"""
    if not name: return ""
    import re
    s = name.strip().lower()
    s = re.sub(r"[.\-'\s]+", "", s)
    return s

def identify_and_fix_wrong_teams():
    """Identify injuries with wrong teams and fix them or deactivate"""
    
    # Known wrong assignments that need fixing
    known_fixes = {
        # Player: (wrong_team, correct_team)
        'Nick Chubb': ('HOU', 'CLE'),
        'Darren Waller': ('MIA', None),  # Retired
        'Najee Harris': ('LAC', 'PIT'),
        'A.J. Brown': ('PHI', 'PHI'),  # Correct but verify
        'Romeo Doubs': ('GB', 'GB'),
        # Add more as needed
    }
    
    with engine.begin() as conn:
        # Get all active injuries
        injuries = pd.read_sql(text("""
            SELECT id, player_name, team, designation, last_updated
            FROM nfl_injuries_tracking
            WHERE is_active = 1
        """), conn)
        
        # Get current roster with correct teams
        roster_best = pd.read_sql(text("""
        WITH canon AS (
        SELECT
            LOWER(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(full_name),'.',''),'''',''),'-',''),' ','')) AS pkey,
            team_abbr,
            team_source
        FROM player_team_current
        WHERE team_abbr NOT IN ('UNK','UNKNOWN','')
        ),
        best AS (
        SELECT *,
                ROW_NUMBER() OVER (
                PARTITION BY pkey
                ORDER BY CASE team_source
                            WHEN 'import_players'     THEN 1
                            WHEN 'espn'               THEN 2
                            WHEN 'player_game_stats'  THEN 3
                            ELSE 99
                            END
                ) AS rn
        FROM canon
        )
        SELECT pkey, team_abbr
        FROM best
        WHERE rn = 1
        """), conn)

        roster_map = dict(zip(roster_best['pkey'], roster_best['team_abbr']))

        
        # Also check 2025 game stats for most recent teams
        recent_teams = pd.read_sql(text("""
            SELECT DISTINCT player_name, team,
                   ROW_NUMBER() OVER (PARTITION BY player_name ORDER BY season DESC, week DESC) as rn
            FROM player_game_stats
            WHERE season = '2025'
        """), conn)
        recent_teams = recent_teams[recent_teams['rn'] == 1]
    
    recent_teams['pkey'] = recent_teams['player_name'].apply(kcanon)
    recent_map = dict(zip(recent_teams['pkey'], recent_teams['team']))
    
    fixes = []
    deactivations = []
    
    for _, inj in injuries.iterrows():
        player_name = inj['player_name']
        current_team = inj['team']
        pkey = kcanon(player_name)
        
        # Check known fixes first
        if player_name in known_fixes:
            correct_team = known_fixes[player_name][1]
            if correct_team is None:
                # Player retired or not active
                deactivations.append(inj['id'])
                print(f"Deactivating {player_name} - retired/inactive")
            elif current_team != correct_team:
                fixes.append((inj['id'], correct_team, player_name))
                print(f"Fixing {player_name}: {current_team} → {correct_team}")
            continue
        
        # Check against current roster
        if pkey in roster_map:
            correct_team = roster_map[pkey]
            if current_team != correct_team:
                fixes.append((inj['id'], correct_team, player_name))
                print(f"Fixing {player_name}: {current_team} → {correct_team}")
        elif pkey in recent_map:
            # Player played in 2025 but not on current roster
            correct_team = recent_map[pkey]
            if current_team != correct_team:
                fixes.append((inj['id'], correct_team, player_name))
                print(f"Fixing {player_name}: {current_team} → {correct_team} (from 2025 games)")
        else:
            # Player not found anywhere - likely cut or practice squad
            deactivations.append(inj['id'])
    
    # Apply fixes
    with engine.begin() as conn:
        for injury_id, correct_team, player_name in fixes:
            conn.execute(text("""
                UPDATE nfl_injuries_tracking
                SET team = :team,
                    last_updated = :ts,
                    confidence_score = 0.95
                WHERE id = :id
            """), {
                'team': correct_team,
                'ts': datetime.now(),
                'id': injury_id
            })
        
        if deactivations:
            stmt = (
                text("""
                    UPDATE nfl_injuries_tracking
                    SET is_active = 0
                    WHERE id IN :ids
                """)
                .bindparams(bindparam("ids", expanding=True))
            )
            conn.execute(stmt, {"ids": list(map(int, deactivations))})

    
    print(f"\n✅ Fixed {len(fixes)} team assignments")
    print(f"✅ Deactivated {len(deactivations)} stale injuries")
    return len(fixes), len(deactivations)

def deactivate_old_injuries(days_threshold=14):
    cutoff_date = datetime.now() - timedelta(days=days_threshold)
    with engine.begin() as conn:
        old_injuries = pd.read_sql(text("""
            SELECT id, player_name, team, designation, last_updated
            FROM nfl_injuries_tracking
            WHERE is_active = 1
              AND designation NOT IN ('IR', 'Injured Reserve', 'PUP')
              AND (last_updated < :cutoff OR last_updated IS NULL)
        """), conn, params={'cutoff': cutoff_date})

        if old_injuries.empty:
            return 0

        print(f"\nDeactivating {len(old_injuries)} injuries older than {days_threshold} days:")
        for _, row in old_injuries.head(10).iterrows():
            print(f"  - {row['player_name']} ({row['team']}) - {row['designation']}")

        ids = old_injuries['id'].astype(int).tolist()
        stmt = (
            text("""
                UPDATE nfl_injuries_tracking
                SET is_active = 0
                WHERE id IN :ids
            """).bindparams(bindparam("ids", expanding=True))
        )
        conn.execute(stmt, {"ids": ids})
        return len(ids)

def clean_unknown_teams():
    """Clean up UNK/UNKNOWN team entries"""
    
    with engine.begin() as conn:
        # Get injuries with unknown teams
        unknowns = pd.read_sql(text("""
            SELECT id, player_name, team
            FROM nfl_injuries_tracking
            WHERE is_active = 1
              AND team IN ('UNK', 'UNKNOWN', '')
        """), conn)
        
        if unknowns.empty:
            print("No unknown teams to clean")
            return 0
        
        # Try to find these players on rosters
        roster = pd.read_sql(text("""
            SELECT full_name, team_abbr
            FROM player_team_current
            WHERE team_abbr NOT IN ('UNK', 'UNKNOWN', '')
        """), conn)
        
        roster['pkey'] = roster['full_name'].apply(kcanon)
        roster_map = dict(zip(roster['pkey'], roster['team_abbr']))
        
        fixed = 0
        deactivated = 0
        
        for _, unk in unknowns.iterrows():
            pkey = kcanon(unk['player_name'])
            if pkey in roster_map:
                # Found on roster - update team
                conn.execute(text("""
                    UPDATE nfl_injuries_tracking
                    SET team = :team, last_updated = :ts
                    WHERE id = :id
                """), {
                    'team': roster_map[pkey],
                    'ts': datetime.now(),
                    'id': unk['id']
                })
                fixed += 1
            else:
                # Not on any roster - deactivate
                conn.execute(text("""
                    UPDATE nfl_injuries_tracking
                    SET is_active = 0
                    WHERE id = :id
                """), {'id': unk['id']})
                deactivated += 1
        
        print(f"✅ Fixed {fixed} unknown teams")
        print(f"✅ Deactivated {deactivated} injuries with unknown teams")
        return fixed + deactivated

def validate_final_state():
    """Show final state after cleanup"""
    
    with engine.begin() as conn:
        # Active injuries summary
        summary = pd.read_sql(text("""
            SELECT team, COUNT(*) as injury_count,
                   SUM(CASE WHEN designation IN ('IR', 'Injured Reserve', 'PUP', 'Out') THEN 1 ELSE 0 END) as out_count,
                   SUM(CASE WHEN designation = 'Questionable' THEN 1 ELSE 0 END) as questionable
            FROM nfl_injuries_tracking
            WHERE is_active = 1
              AND team NOT IN ('UNK', 'UNKNOWN', '')
            GROUP BY team
            ORDER BY injury_count DESC
        """), conn)
        
        # Check for remaining issues
        issues = pd.read_sql(text("""
            SELECT player_name, team, designation
            FROM nfl_injuries_tracking
            WHERE is_active = 1
              AND (team IN ('UNK', 'UNKNOWN', '') OR team IS NULL)
        """), conn)
        
        # Wrong team checks (sample)
        wrong_teams = pd.read_sql(text("""
        WITH canon AS (
        SELECT
            LOWER(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(full_name),'.',''),'''',''),'-',''),' ','')) AS pkey,
            team_abbr,
            team_source
        FROM player_team_current
        WHERE team_abbr NOT IN ('UNK','UNKNOWN','')
        ),
        best AS (
        SELECT *,
                ROW_NUMBER() OVER (
                PARTITION BY pkey
                ORDER BY CASE team_source
                            WHEN 'import_players'     THEN 1
                            WHEN 'espn'               THEN 2
                            WHEN 'player_game_stats'  THEN 3
                            ELSE 99
                            END
                ) AS rn
        FROM canon
        )
        SELECT i.player_name,
            i.team  AS injury_team,
            b.team_abbr AS roster_team
        FROM nfl_injuries_tracking i
        LEFT JOIN best b
        ON LOWER(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(i.player_name),'.',''),'''',''),'-',''),' ','')) = b.pkey
        AND b.rn = 1
        WHERE i.is_active = 1
        AND b.team_abbr IS NOT NULL
        AND i.team != b.team_abbr
        LIMIT 20
        """), conn)

    
    print("\n📊 Final State:")
    print(f"Teams with injuries: {len(summary)}")
    print("\nTop 10 teams by injury count:")
    print(summary.head(10).to_string(index=False))
    
    if not issues.empty:
        print(f"\n⚠️ Still have {len(issues)} injuries with unknown teams")
        print(issues.head(10).to_string(index=False))
    
    if not wrong_teams.empty:
        print(f"\n⚠️ Potential wrong team assignments (sample):")
        print(wrong_teams.to_string(index=False))

def main():
    print("🧹 Aggressive Injury Cleanup")
    print("=" * 50)
    
    # 1. Fix known wrong teams and deactivate players not on rosters
    fixes, deactivations = identify_and_fix_wrong_teams()
    
    # 2. Deactivate old injuries (not updated in 14 days)
    old_deactivated = deactivate_old_injuries(days_threshold=14)
    
    # 3. Clean up unknown teams
    unknown_cleaned = clean_unknown_teams()
    
    # 4. Show final state
    validate_final_state()
    
    print("\n✅ Cleanup Complete!")
    print(f"  - Team fixes: {fixes}")
    print(f"  - Deactivated stale: {deactivations}")
    print(f"  - Deactivated old: {old_deactivated}")
    print(f"  - Unknown teams cleaned: {unknown_cleaned}")
    
    print("\n💡 Next steps:")
    print("  1. Re-run your roster import to ensure roster is current")
    print("  2. Check the injury validation table for remaining issues")
    print("  3. Consider setting up regular cleanup (weekly)")

if __name__ == "__main__":
    main()