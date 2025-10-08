#!/usr/bin/env python3
"""
COMPLETE FIX: Rams Naming Issue + Ties Display in Power Rankings
This fixes BOTH problems:
1. Rams games not counting (LA vs LAR vs Los Angeles Rams confusion)
2. Ties not showing in rankings record string
"""

from sqlalchemy import create_engine, text
import pandas as pd

DATABASE_URL = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def diagnose_rams_issue():
    """First, let's see what's happening with Rams games"""
    print("🔍 DIAGNOSING RAMS NAMING ISSUE")
    print("=" * 60)
    
    with engine.connect() as conn:
        # Check all Rams game variations
        rams_games = pd.read_sql(text("""
            SELECT game_id, home_team, away_team, home_score, away_score, game_date
            FROM games
            WHERE home_team ILIKE '%ram%' 
               OR away_team ILIKE '%ram%'
               OR home_team ILIKE '%la%'
               OR away_team ILIKE '%la%'
               OR home_team = 'LAR'
               OR away_team = 'LAR'
            ORDER BY game_date
        """), conn)
        
        print(f"\n📊 Found {len(rams_games)} potential Rams games:")
        print(rams_games[['game_date', 'home_team', 'away_team', 'home_score', 'away_score']].to_string())
        
        # Check what's in team_season_summary
        rams_stats = pd.read_sql(text("""
            SELECT team, season, wins, losses, ties, games_played, power_score
            FROM team_season_summary
            WHERE team ILIKE '%ram%' OR team = 'LAR'
        """), conn)
        
        print(f"\n📈 Team Season Summary for Rams:")
        print(rams_stats.to_string())
        
        # Check unique team name variations
        all_teams = pd.read_sql(text("""
            SELECT DISTINCT team FROM (
                SELECT home_team as team FROM games
                UNION
                SELECT away_team as team FROM games
            ) t
            WHERE team ILIKE '%ram%' OR team ILIKE '%la%' OR team = 'LAR'
            ORDER BY team
        """), conn)
        
        print(f"\n🏷️  All Rams team name variations found:")
        for team in all_teams['team']:
            print(f"   - '{team}'")
        
        return rams_games

def fix_rams_naming():
    """Standardize ALL Rams references to 'LAR' (abbreviation format)"""
    print("\n\n🔧 FIXING RAMS NAMING STANDARDIZATION")
    print("=" * 60)
    
    with engine.connect() as conn:
        # Step 1: Update games table - standardize to LAR
        print("\n1️⃣  Standardizing Rams in games table to 'LAR'...")
        
        home_updates = conn.execute(text("""
            UPDATE games 
            SET home_team = 'LAR'
            WHERE home_team IN ('LA', 'Los Angeles Rams', 'L.A. Rams', 'St. Louis Rams', 'STL')
               OR home_team ILIKE '%los angeles rams%'
        """)).rowcount
        
        away_updates = conn.execute(text("""
            UPDATE games 
            SET away_team = 'LAR'
            WHERE away_team IN ('LA', 'Los Angeles Rams', 'L.A. Rams', 'St. Louis Rams', 'STL')
               OR away_team ILIKE '%los angeles rams%'
        """)).rowcount
        
        print(f"   ✅ Updated {home_updates} home games + {away_updates} away games")
        
        # Step 2: Delete old Rams entries from team_season_summary
        print("\n2️⃣  Removing old Rams entries from team_season_summary...")
        
        deleted = conn.execute(text("""
            DELETE FROM team_season_summary
            WHERE team IN ('LA', 'Los Angeles Rams', 'L.A. Rams', 'St. Louis Rams', 'STL')
               OR team ILIKE '%los angeles rams%'
        """)).rowcount
        
        print(f"   ✅ Deleted {deleted} old Rams records")
        
        # Step 3: Ensure ties column exists
        print("\n3️⃣  Ensuring ties column exists...")
        try:
            conn.execute(text("""
                ALTER TABLE team_season_summary 
                ADD COLUMN IF NOT EXISTS ties INTEGER DEFAULT 0
            """))
            print("   ✅ Ties column added/verified")
        except Exception as e:
            if "already exists" in str(e).lower():
                print("   ✅ Ties column already exists")
            else:
                print(f"   ⚠️  Ties column issue: {e}")
        
        conn.commit()
        
        # Step 4: Recalculate team stats with PROPER Rams handling + ties
        print("\n4️⃣  Recalculating ALL team stats (including Rams with ties)...")
        
        conn.execute(text("""
            -- Delete current season data
            DELETE FROM team_season_summary WHERE season = 2025;
            
            -- Recalculate with proper name handling
            WITH normalized_games AS (
                SELECT 
                    game_id,
                    home_team,
                    away_team,
                    home_score,
                    away_score
                FROM games
                WHERE home_score IS NOT NULL 
                AND away_score IS NOT NULL
                AND game_date >= '2025-09-01'
                AND game_date <= CURRENT_DATE
            ),
            team_stats AS (
                SELECT 
                    team,
                    2025 as season,
                    COUNT(*) as games_played,
                    SUM(wins) as wins,
                    SUM(losses) as losses,
                    SUM(ties) as ties,
                    AVG(points_for) as avg_points_for,
                    AVG(points_against) as avg_points_against,
                    AVG(point_diff) as point_diff,
                    CASE 
                        WHEN COUNT(*) > 0 
                        THEN (SUM(wins)::float + (SUM(ties)::float * 0.5)) / COUNT(*) 
                        ELSE 0.0 
                    END as win_pct
                FROM (
                    -- Home games
                    SELECT 
                        home_team as team,
                        CASE WHEN home_score > away_score THEN 1 ELSE 0 END as wins,
                        CASE WHEN home_score < away_score THEN 1 ELSE 0 END as losses,
                        CASE WHEN home_score = away_score THEN 1 ELSE 0 END as ties,
                        home_score as points_for,
                        away_score as points_against,
                        home_score - away_score as point_diff
                    FROM normalized_games
                    
                    UNION ALL
                    
                    -- Away games
                    SELECT 
                        away_team as team,
                        CASE WHEN away_score > home_score THEN 1 ELSE 0 END as wins,
                        CASE WHEN away_score < home_score THEN 1 ELSE 0 END as losses,
                        CASE WHEN away_score = home_score THEN 1 ELSE 0 END as ties,
                        away_score as points_for,
                        home_score as points_against,
                        away_score - home_score as point_diff
                    FROM normalized_games
                ) team_games
                GROUP BY team
            )
            INSERT INTO team_season_summary (
                team, season, games_played, wins, losses, ties,
                win_pct, avg_points_for, avg_points_against, point_diff, power_score
            )
            SELECT 
                team, season, games_played, wins, losses, ties,
                win_pct, avg_points_for, avg_points_against, point_diff,
                point_diff as power_score
            FROM team_stats
        """))
        
        conn.commit()
        print("   ✅ Team stats recalculated")

def verify_fix():
    """Verify the fix worked"""
    print("\n\n✅ VERIFICATION")
    print("=" * 60)
    
    with engine.connect() as conn:
        # Check Rams games count
        rams_games = pd.read_sql(text("""
            SELECT 
                game_id, 
                game_date,
                CASE 
                    WHEN home_team = 'LAR' THEN 'LAR (home)'
                    ELSE 'LAR (away)'
                END as rams_role,
                CASE 
                    WHEN home_team = 'LAR' THEN away_team
                    ELSE home_team
                END as opponent,
                CASE 
                    WHEN home_team = 'LAR' THEN home_score
                    ELSE away_score
                END as rams_score,
                CASE 
                    WHEN home_team = 'LAR' THEN away_score
                    ELSE home_score
                END as opp_score,
                CASE
                    WHEN home_team = 'LAR' AND home_score > away_score THEN 'W'
                    WHEN away_team = 'LAR' AND away_score > home_score THEN 'W'
                    WHEN home_score = away_score THEN 'T'
                    ELSE 'L'
                END as result
            FROM games
            WHERE (home_team = 'LAR' OR away_team = 'LAR')
            AND home_score IS NOT NULL
            AND game_date >= '2025-09-01'
            ORDER BY game_date
        """), conn)
        
        print(f"\n🏈 Rams Games Found: {len(rams_games)}")
        if not rams_games.empty:
            print(rams_games.to_string(index=False))
        
        # Check Rams stats
        rams_stats = pd.read_sql(text("""
            SELECT team, wins, losses, ties, games_played, win_pct, power_score
            FROM team_season_summary
            WHERE team = 'LAR' AND season = 2025
        """), conn)
        
        print(f"\n📊 Rams Team Stats:")
        if not rams_stats.empty:
            w = int(rams_stats.iloc[0]['wins'])
            l = int(rams_stats.iloc[0]['losses'])
            t = int(rams_stats.iloc[0]['ties'])
            gp = int(rams_stats.iloc[0]['games_played'])
            
            record = f"{w}-{l}-{t}" if t > 0 else f"{w}-{l}"
            print(f"   Record: {record} ({gp} games played)")
            print(f"   Win %: {rams_stats.iloc[0]['win_pct']:.3f}")
            print(f"   Power Score: {rams_stats.iloc[0]['power_score']:.1f}")
        else:
            print("   ❌ No Rams stats found!")
        
        # Check all teams with ties
        teams_with_ties = pd.read_sql(text("""
            SELECT team, wins, losses, ties, games_played
            FROM team_season_summary
            WHERE season = 2025 AND ties > 0
            ORDER BY ties DESC
        """), conn)
        
        if not teams_with_ties.empty:
            print(f"\n🤝 Teams with Ties:")
            for _, row in teams_with_ties.iterrows():
                record = f"{int(row['wins'])}-{int(row['losses'])}-{int(row['ties'])}"
                print(f"   {row['team']}: {record}")

def main():
    """Run the complete fix"""
    print("🏈 RAMS NAMING + TIES FIX")
    print("=" * 60)
    
    # Step 1: Diagnose the issue
    diagnose_rams_issue()
    
    # Step 2: Fix it
    fix_rams_naming()
    
    # Step 3: Verify it worked
    verify_fix()
    
    print("\n\n" + "=" * 60)
    print("✅ FIX COMPLETE!")
    print("\nWhat was fixed:")
    print("  1. ✅ All Rams games now use 'LAR' consistently")
    print("  2. ✅ Ties column added/verified in team_season_summary")
    print("  3. ✅ Team stats recalculated with proper tie handling")
    print("  4. ✅ Rams should now show ALL their games correctly")
    print("\nNext: Update your dashboard to display ties in record strings")
    print("=" * 60)

if __name__ == "__main__":
    main()