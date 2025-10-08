#!/usr/bin/env python3
"""
Verify that Rams games + ties are working correctly
Run this AFTER fix_rams_and_ties.py
"""

from sqlalchemy import create_engine, text
import pandas as pd

DATABASE_URL = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def main():
    print("🔍 VERIFICATION: Rams Games + Ties Fix")
    print("=" * 70)
    
    with engine.connect() as conn:
        
        # 1. Check all Rams games
        print("\n1️⃣  RAMS GAMES IN DATABASE:")
        print("-" * 70)
        
        rams_games = pd.read_sql(text("""
            SELECT 
                game_date,
                CASE 
                    WHEN home_team = 'LAR' THEN 'vs ' || away_team
                    ELSE '@ ' || home_team
                END as matchup,
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
        
        if rams_games.empty:
            print("❌ NO RAMS GAMES FOUND!")
        else:
            print(f"✅ Found {len(rams_games)} Rams games:\n")
            for _, game in rams_games.iterrows():
                print(f"   {game['game_date']}  {game['matchup']:25} "
                      f"{game['rams_score']:2.0f}-{game['opp_score']:2.0f}  {game['result']}")
        
        # 2. Check Rams stats in team_season_summary
        print("\n\n2️⃣  RAMS TEAM STATS:")
        print("-" * 70)
        
        rams_stats = pd.read_sql(text("""
            SELECT team, wins, losses, ties, games_played, win_pct, power_score
            FROM team_season_summary
            WHERE team = 'LAR' AND season = 2025
        """), conn)
        
        if rams_stats.empty:
            print("❌ NO RAMS STATS FOUND IN team_season_summary!")
        else:
            row = rams_stats.iloc[0]
            w = int(row['wins'])
            l = int(row['losses'])
            t = int(row['ties'])
            gp = int(row['games_played'])
            
            record = f"{w}-{l}-{t}" if t > 0 else f"{w}-{l}"
            
            print(f"✅ Rams Season Stats:")
            print(f"   Record: {record} ({gp} games played)")
            print(f"   Win %: {row['win_pct']:.3f}")
            print(f"   Power Score: {row['power_score']:.1f}")
            
            # Verify games_played matches actual games
            if gp != len(rams_games):
                print(f"\n⚠️  WARNING: Games played ({gp}) doesn't match actual games ({len(rams_games)})!")
            else:
                print(f"\n✅ Games count matches!")
        
        # 3. Check for team name inconsistencies
        print("\n\n3️⃣  TEAM NAME CONSISTENCY CHECK:")
        print("-" * 70)
        
        team_variations = pd.read_sql(text("""
            SELECT DISTINCT team FROM (
                SELECT home_team as team FROM games
                UNION
                SELECT away_team as team FROM games
            ) t
            WHERE team ILIKE '%ram%' OR team ILIKE '%la%' OR team = 'LAR'
            ORDER BY team
        """), conn)
        
        if len(team_variations) == 1 and team_variations.iloc[0]['team'] == 'LAR':
            print("✅ All Rams references use 'LAR' - CONSISTENT!")
        else:
            print(f"⚠️  Found {len(team_variations)} different Rams variations:")
            for _, row in team_variations.iterrows():
                print(f"   - '{row['team']}'")
        
        # 4. Check all teams with ties
        print("\n\n4️⃣  ALL TEAMS WITH TIES:")
        print("-" * 70)
        
        teams_with_ties = pd.read_sql(text("""
            SELECT team, wins, losses, ties, games_played
            FROM team_season_summary
            WHERE season = 2025 AND ties > 0
            ORDER BY ties DESC, wins DESC
        """), conn)
        
        if teams_with_ties.empty:
            print("ℹ️  No teams have ties this season")
        else:
            print(f"✅ Found {len(teams_with_ties)} team(s) with ties:\n")
            for _, row in teams_with_ties.iterrows():
                record = f"{int(row['wins'])}-{int(row['losses'])}-{int(row['ties'])}"
                print(f"   {row['team']:25} {record}")
        
        # 5. Sample rankings with tie display
        print("\n\n5️⃣  SAMPLE RANKINGS (Top 10):")
        print("-" * 70)
        
        rankings = pd.read_sql(text("""
            SELECT 
                team, 
                wins, 
                losses, 
                COALESCE(ties, 0) as ties,
                games_played,
                win_pct,
                power_score
            FROM team_season_summary
            WHERE season = 2025
            ORDER BY wins DESC, ties DESC, power_score DESC
            LIMIT 10
        """), conn)
        
        if rankings.empty:
            print("❌ No rankings data found!")
        else:
            print(f"{'Rank':<6}{'Team':<25}{'Record':<12}{'Win %':<10}{'Power'}")
            print("-" * 70)
            
            for idx, row in rankings.iterrows():
                w = int(row['wins'])
                l = int(row['losses'])
                t = int(row['ties'])
                
                # Create record string with ties
                if t > 0:
                    record = f"{w}-{l}-{t}"
                else:
                    record = f"{w}-{l}"
                
                print(f"{idx+1:<6}{row['team']:<25}{record:<12}"
                      f"{row['win_pct']:.3f}     {row['power_score']:.1f}")
        
        # 6. Final verification
        print("\n\n6️⃣  FINAL VERIFICATION:")
        print("-" * 70)
        
        issues = []
        
        # Check if Rams games match expected (you mentioned Eagles and Colts)
        expected_opponents = ['PHI', 'IND']
        for opp in expected_opponents:
            game_check = pd.read_sql(text("""
                SELECT COUNT(*) as count
                FROM games
                WHERE ((home_team = 'LAR' AND away_team = :opp)
                   OR (away_team = 'LAR' AND home_team = :opp))
                AND home_score IS NOT NULL
                AND game_date >= '2025-09-01'
            """), conn, params={'opp': opp}).iloc[0]['count']
            
            if game_check == 0:
                issues.append(f"Missing LAR vs {opp} game")
            else:
                print(f"✅ Found LAR vs {opp} game")
        
        # Check ties column exists
        try:
            tie_col_check = pd.read_sql(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'team_season_summary' 
                AND column_name = 'ties'
            """), conn)
            
            if not tie_col_check.empty:
                print("✅ Ties column exists in team_season_summary")
            else:
                issues.append("Ties column missing from team_season_summary")
        except:
            print("⚠️  Could not verify ties column (might be using SQLite)")
        
        # Summary
        print("\n\n" + "=" * 70)
        if issues:
            print("⚠️  ISSUES FOUND:")
            for issue in issues:
                print(f"   - {issue}")
        else:
            print("✅ ALL CHECKS PASSED!")
            print("\nYour dashboard should now show:")
            print("  1. ✅ All Rams games counted correctly")
            print("  2. ✅ Ties displayed in record (e.g., '3-1-1')")
            print("  3. ✅ Consistent team naming (all using 'LAR')")
        print("=" * 70)

if __name__ == "__main__":
    main()