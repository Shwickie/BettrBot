#!/usr/bin/env python3
"""
Delete ALL duplicate games from database
Keeps only ONE copy of each unique game
"""

from sqlalchemy import create_engine, text
import pandas as pd

DATABASE_URL = "postgresql://postgres:YviqtXqcsCIgRzSCofNjbfwgjkYNLydX@maglev.proxy.rlwy.net:54187/railway"
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def delete_duplicates():
    """Delete duplicate games, keeping only the first occurrence"""
    print("🗑️  DELETING DUPLICATE GAMES")
    print("=" * 70)
    
    with engine.connect() as conn:
        
        # Check for duplicates first
        print("\n1️⃣ Finding duplicates...")
        
        duplicates = pd.read_sql(text("""
            SELECT game_date, home_team, away_team, home_score, away_score,
                   COUNT(*) as count
            FROM games
            WHERE game_date >= '2025-09-01'
            AND home_score IS NOT NULL
            GROUP BY game_date, home_team, away_team, home_score, away_score
            HAVING COUNT(*) > 1
        """), conn)
        
        if duplicates.empty:
            print("   ✅ No duplicates found!")
            return
        
        print(f"   Found {len(duplicates)} sets of duplicate games")
        total_to_delete = duplicates['count'].sum() - len(duplicates)
        print(f"   Will delete {total_to_delete} duplicate records")
        
        # FIXED: Delete duplicates using ctid (PostgreSQL row identifier)
        print("\n2️⃣ Deleting duplicates (keeping first occurrence)...")
        
        deleted = conn.execute(text("""
            DELETE FROM games
            WHERE ctid NOT IN (
                SELECT MIN(ctid)
                FROM games
                WHERE game_date >= '2025-09-01'
                AND home_score IS NOT NULL
                GROUP BY game_date, home_team, away_team, home_score, away_score
            )
            AND game_date >= '2025-09-01'
            AND home_score IS NOT NULL
        """)).rowcount
        
        conn.commit()
        
        print(f"   ✅ Deleted {deleted} duplicate games")
        
        # Verify no duplicates remain
        remaining = pd.read_sql(text("""
            SELECT game_date, home_team, away_team, COUNT(*) as count
            FROM games
            WHERE game_date >= '2025-09-01'
            AND home_score IS NOT NULL
            GROUP BY game_date, home_team, away_team
            HAVING COUNT(*) > 1
        """), conn)
        
        if remaining.empty:
            print("   ✅ All duplicates removed!")
        else:
            print(f"   ⚠️  Still {len(remaining)} duplicates remaining")

def recalculate_stats():
    """Recalculate team stats after removing duplicates"""
    print("\n📊 RECALCULATING TEAM STATS")
    print("=" * 70)
    
    with engine.connect() as conn:
        
        # Delete old stats
        conn.execute(text("DELETE FROM team_season_summary WHERE season = 2025"))
        
        # Recalculate
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
        all_teams = pd.read_sql(text("""
            SELECT team, wins, losses, ties, games_played
            FROM team_season_summary
            WHERE season = 2025
            ORDER BY wins DESC, ties DESC
        """), conn)
        
        print(f"✅ Stats recalculated for {len(all_teams)} teams\n")
        
        print("All teams:")
        for _, row in all_teams.iterrows():
            w, l, t = int(row['wins']), int(row['losses']), int(row['ties'])
            record = f"{w}-{l}-{t}" if t > 0 else f"{w}-{l}"
            print(f"   {row['team']:6} {record:8} ({int(row['games_played'])} games)")
        
        # Check total games
        total_games = pd.read_sql(text("""
            SELECT COUNT(*) as total
            FROM games
            WHERE game_date >= '2025-09-01'
            AND home_score IS NOT NULL
        """), conn).iloc[0]['total']
        
        print(f"\n📈 Total 2025 games in database: {total_games}")

def main():
    print("🏈 DELETE ALL DUPLICATE GAMES")
    print("=" * 70)
    
    delete_duplicates()
    recalculate_stats()
    
    print("\n" + "=" * 70)
    print("✅ COMPLETE!")
    print("\nYour database should now have:")
    print("  - NO duplicate games")
    print("  - Correct game counts (max 5 games per team)")
    print("  - Accurate win-loss records with ties")
    print("=" * 70)

if __name__ == "__main__":
    main()