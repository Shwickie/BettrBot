#!/usr/bin/env python3
"""
Simple Team Name Fix - Just standardize team names without changing games
Run this ONCE to fix the Rams/Eagles naming issue
"""

from sqlalchemy import create_engine, text
import pandas as pd

DATABASE_URL = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def fix_team_names():
    """Standardize ALL team names to abbreviations"""
    print("🔧 FIXING TEAM NAMES (ONE-TIME FIX)")
    print("=" * 60)
    
    with engine.connect() as conn:
        
        # Just fix the specific issues: Philadelphia Eagles -> PHI
        print("\n1️⃣ Fixing 'Philadelphia Eagles' -> 'PHI'...")
        
        home_updates = conn.execute(text("""
            UPDATE games 
            SET home_team = 'PHI'
            WHERE home_team = 'Philadelphia Eagles'
        """)).rowcount
        
        away_updates = conn.execute(text("""
            UPDATE games 
            SET away_team = 'PHI'
            WHERE away_team = 'Philadelphia Eagles'
        """)).rowcount
        
        print(f"   ✅ Updated {home_updates} home games + {away_updates} away games")
        
        # Fix any other full team names to abbreviations
        team_fixes = {
            'Los Angeles Rams': 'LAR',
            'LA': 'LAR',
            'Indianapolis Colts': 'IND',
            'Kansas City Chiefs': 'KC',
            'Buffalo Bills': 'BUF',
            'Baltimore Ravens': 'BAL',
            'San Francisco 49ers': 'SF',
            'Dallas Cowboys': 'DAL',
            'Miami Dolphins': 'MIA',
            'Cincinnati Bengals': 'CIN',
            'Detroit Lions': 'DET',
            'Green Bay Packers': 'GB',
            'Los Angeles Chargers': 'LAC',
            'Minnesota Vikings': 'MIN',
            'Houston Texans': 'HOU',
            'Pittsburgh Steelers': 'PIT',
            'Atlanta Falcons': 'ATL',
            'Las Vegas Raiders': 'LV',
            'Tampa Bay Buccaneers': 'TB',
            'Seattle Seahawks': 'SEA',
            'New Orleans Saints': 'NO',
            'Jacksonville Jaguars': 'JAX',
            'Tennessee Titans': 'TEN',
            'Cleveland Browns': 'CLE',
            'New York Jets': 'NYJ',
            'Arizona Cardinals': 'ARI',
            'Denver Broncos': 'DEN',
            'New England Patriots': 'NE',
            'Washington Commanders': 'WAS',
            'New York Giants': 'NYG',
            'Carolina Panthers': 'CAR',
            'Chicago Bears': 'CHI'
        }
        
        total_fixed = 0
        for full_name, abbr in team_fixes.items():
            home_count = conn.execute(text(f"""
                UPDATE games 
                SET home_team = '{abbr}'
                WHERE home_team = '{full_name}'
            """)).rowcount
            
            away_count = conn.execute(text(f"""
                UPDATE games 
                SET away_team = '{abbr}'
                WHERE away_team = '{full_name}'
            """)).rowcount
            
            if home_count > 0 or away_count > 0:
                print(f"   '{full_name}' -> '{abbr}': {home_count + away_count} games")
                total_fixed += home_count + away_count
        
        conn.commit()
        print(f"\n✅ Total: {total_fixed} team names standardized")
        
        # Recalculate team stats for 2025
        print("\n2️⃣ Recalculating 2025 team stats...")
        
        conn.execute(text("DELETE FROM team_season_summary WHERE season = 2025"))
        
        conn.execute(text("""
            WITH team_stats AS (
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
                    SELECT 
                        home_team as team,
                        CASE WHEN home_score > away_score THEN 1 ELSE 0 END as wins,
                        CASE WHEN home_score < away_score THEN 1 ELSE 0 END as losses,
                        CASE WHEN home_score = away_score THEN 1 ELSE 0 END as ties,
                        home_score as points_for,
                        away_score as points_against,
                        home_score - away_score as point_diff
                    FROM games 
                    WHERE home_score IS NOT NULL
                    AND game_date >= '2025-09-01'
                    AND game_date <= CURRENT_DATE
                    
                    UNION ALL
                    
                    SELECT 
                        away_team as team,
                        CASE WHEN away_score > home_score THEN 1 ELSE 0 END as wins,
                        CASE WHEN away_score < home_score THEN 1 ELSE 0 END as losses,
                        CASE WHEN away_score = home_score THEN 1 ELSE 0 END as ties,
                        away_score as points_for,
                        home_score as points_against,
                        away_score - home_score as point_diff
                    FROM games 
                    WHERE away_score IS NOT NULL
                    AND game_date >= '2025-09-01'
                    AND game_date <= CURRENT_DATE
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
        
        # Show results
        rams = pd.read_sql(text("""
            SELECT team, wins, losses, ties, games_played
            FROM team_season_summary
            WHERE team = 'LAR' AND season = 2025
        """), conn)
        
        if not rams.empty:
            row = rams.iloc[0]
            w, l, t = int(row['wins']), int(row['losses']), int(row['ties'])
            record = f"{w}-{l}-{t}" if t > 0 else f"{w}-{l}"
            print(f"\n✅ Rams 2025: {record} ({int(row['games_played'])} games)")
        
        # Show all teams
        all_teams = pd.read_sql(text("""
            SELECT team, wins, losses, ties, games_played
            FROM team_season_summary
            WHERE season = 2025
            ORDER BY wins DESC, ties DESC
            LIMIT 10
        """), conn)
        
        print(f"\n📊 Top 10 Teams (2025):")
        for _, row in all_teams.iterrows():
            w, l, t = int(row['wins']), int(row['losses']), int(row['ties'])
            record = f"{w}-{l}-{t}" if t > 0 else f"{w}-{l}"
            print(f"   {row['team']:6} {record:8} ({int(row['games_played'])} games)")

def main():
    print("🏈 TEAM NAME STANDARDIZATION FIX")
    print("=" * 60)
    
    fix_team_names()
    
    print("\n" + "=" * 60)
    print("✅ DONE! Team names standardized")
    print("\nYour dashboard should now show:")
    print("  - Correct Rams record with all games counted")
    print("  - Ties displayed properly")
    print("  - All teams using abbreviations (LAR, PHI, etc.)")
    print("=" * 60)

if __name__ == "__main__":
    main()