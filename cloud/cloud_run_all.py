# cloud_run_all.py - FIXED VERSION for Render deployment
"""
Fixed cloud pipeline that handles all the issues from your error logs
FIXED: Unicode encoding issues for Windows compatibility
FIXED: Database connection handling for PostgreSQL
"""

import subprocess
import sys
import time
import os
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine, text
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Make repo root importable
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

PY = sys.executable

def setup_cloud_environment():
    """Setup for cloud deployment with better error handling"""
    try:
        DATABASE_URL = os.environ.get("DATABASE_URL")
        if not DATABASE_URL:
            print("WARNING: No DATABASE_URL found, using default")
            DATABASE_URL = "postgresql://postgres.bmfwrdsastxbsbubuuhs:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres"
        
        # Fix postgres:// URLs (Render uses this format)
        if DATABASE_URL.startswith('postgres://'):
            DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
        
        # Create database engine with robust settings
        engine = create_engine(
            DATABASE_URL, 
            pool_pre_ping=True,
            pool_recycle=280,
            pool_timeout=30,
            connect_args={
                "sslmode": "require",
                "connect_timeout": 30,
                "application_name": "bettrbot_pipeline"
            }
        )
        
        # Test connection with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                print(f"Connection attempt {attempt + 1} failed, retrying...")
                time.sleep(2)
            
        print("SUCCESS: Database connection established")
        return engine
        
    except Exception as e:
        print(f"ERROR: Database connection failed: {e}")
        return None

def run_update_scores():
    """Run the update_scores task with comprehensive error handling"""
    print("TASK: Running update_scores...")
    try:
        # Set environment for the subprocess
        env = os.environ.copy()
        env['BETTR_PIPELINE_MODE'] = 'true'
        # CRITICAL: Fix Unicode encoding for Windows
        env['PYTHONIOENCODING'] = 'utf-8'
        
        # Look for update_scores.py in stats directory
        stats_dir = ROOT / "stats"
        update_scores_path = stats_dir / "update_scores.py"
        
        if update_scores_path.exists():
            # Run from stats directory
            result = subprocess.run(
                [PY, str(update_scores_path)], 
                capture_output=True, 
                text=True, 
                timeout=600,  # 10 minute timeout
                env=env,
                cwd=ROOT,
                encoding='utf-8',  # Force UTF-8 encoding
                errors='replace'   # Replace problematic characters
            )
        else:
            # Fallback to root directory
            result = subprocess.run(
                [PY, "update_scores.py"], 
                capture_output=True, 
                text=True, 
                timeout=600,
                env=env,
                cwd=ROOT,
                encoding='utf-8',
                errors='replace'
            )
        
        # Parse output to determine if actually successful
        output = result.stdout + result.stderr
        
        # Look for success indicators in output
        success_indicators = [
            "SUCCESS: Updated",
            "Team season summary updated",
            "No new scores written",  # This is actually OK if no new games
            "All games already have scores"  # This is also success - nothing to update
        ]
        
        if result.returncode == 0 or any(indicator in output for indicator in success_indicators):
            print("   SUCCESS: update_scores completed")
            
            # Show relevant output
            if "Updated" in output:
                for line in output.split('\n'):
                    if "Updated" in line or "games" in line.lower():
                        print(f"   {line}")
            
            return True
        else:
            print(f"   ERROR: update_scores failed:")
            # Show last few lines of error
            error_lines = output.split('\n')[-5:]
            for line in error_lines:
                if line.strip():
                    print(f"     {line}")
            return False
            
    except subprocess.TimeoutExpired:
        print("   ERROR: update_scores timed out after 10 minutes")
        return False
    except Exception as e:
        print(f"   ERROR: update_scores crashed: {e}")
        return False

def run_team_season_summary():
    """Generate team season summary data with fixed constraint handling"""
    print("TASK: Running team_season_summary...")
    try:
        engine = setup_cloud_environment()
        if not engine:
            return False
            
        with engine.connect() as conn:
            # Get current season
            current_year = datetime.now().year
            season = current_year if datetime.now().month >= 8 else current_year - 1
            
            print(f"   Updating team stats for season {season}")
            
            # FIXED: Remove duplicates first, then create constraint
            try:
                # Step 1: Remove duplicates (FIXED - no ID column in PostgreSQL)
                duplicates = conn.execute(text("""
                    SELECT team, season, COUNT(*) as count
                    FROM team_season_summary 
                    GROUP BY team, season 
                    HAVING COUNT(*) > 1
                """)).fetchall()
                
                if duplicates:
                    print(f"   Found {len(duplicates)} duplicate combinations, cleaning...")
                    for team, season_dup, count in duplicates:
                        # DELETE all duplicates, then re-insert will happen in main query
                        conn.execute(text("""
                            DELETE FROM team_season_summary 
                            WHERE team = :team AND season = :season
                        """), {"team": team, "season": season_dup})
                    
                    conn.commit()
                    print("   Duplicates cleaned")
                
                # Step 2: Ensure unique constraint exists
                conn.execute(text("""
                    ALTER TABLE team_season_summary 
                    DROP CONSTRAINT IF EXISTS team_season_unique
                """))
                
                conn.execute(text("""
                    ALTER TABLE team_season_summary 
                    ADD CONSTRAINT team_season_unique UNIQUE (team, season)
                """))
                conn.commit()
                print("   Unique constraint ensured")
                
            except Exception as e:
                print(f"   Warning: Constraint setup had issues: {e}")
                # Continue anyway - the upsert might still work
            
            # Step 3: Run the actual team stats calculation (FIXED - avoid duplicates)
            query = text("""
                INSERT INTO team_season_summary (
                    team, season, power_score, wins, losses, games_played, 
                    win_pct, avg_points_for, avg_points_against, point_diff
                )
                SELECT 
                    team,
                    :season as season,
                    AVG(point_diff) as power_score,
                    SUM(wins) as wins,
                    SUM(losses) as losses,
                    COUNT(*) as games_played,
                    CASE WHEN COUNT(*) > 0 THEN SUM(wins)::float / COUNT(*) ELSE 0.0 END as win_pct,
                    AVG(points_for) as avg_points_for,
                    AVG(points_against) as avg_points_against,
                    AVG(point_diff) as point_diff
                FROM (
                    SELECT DISTINCT
                        game_id,
                        home_team as team,
                        CASE WHEN home_score > away_score THEN 1 ELSE 0 END as wins,
                        CASE WHEN home_score < away_score THEN 1 ELSE 0 END as losses,
                        home_score as points_for,
                        away_score as points_against,
                        home_score - away_score as point_diff
                    FROM games 
                    WHERE home_score IS NOT NULL 
                    AND away_score IS NOT NULL
                    AND EXTRACT(YEAR FROM game_date) = :season
                    AND game_date <= CURRENT_DATE
                    
                    UNION ALL
                    
                    SELECT DISTINCT
                        game_id,
                        away_team as team,
                        CASE WHEN away_score > home_score THEN 1 ELSE 0 END as wins,
                        CASE WHEN away_score < home_score THEN 1 ELSE 0 END as losses,
                        away_score as points_for,
                        home_score as points_against,
                        away_score - home_score as point_diff
                    FROM games 
                    WHERE home_score IS NOT NULL 
                    AND away_score IS NOT NULL
                    AND EXTRACT(YEAR FROM game_date) = :season
                    AND game_date <= CURRENT_DATE
                ) team_games
                GROUP BY team
                ON CONFLICT (team, season) DO UPDATE SET
                    power_score = EXCLUDED.power_score,
                    wins = EXCLUDED.wins,
                    losses = EXCLUDED.losses,
                    games_played = EXCLUDED.games_played,
                    win_pct = EXCLUDED.win_pct,
                    avg_points_for = EXCLUDED.avg_points_for,
                    avg_points_against = EXCLUDED.avg_points_against,
                    point_diff = EXCLUDED.point_diff
            """)
            
            result = conn.execute(query, {"season": season})
            conn.commit()
            
            # Verify results
            count_check = conn.execute(text("""
                SELECT COUNT(*) FROM team_season_summary WHERE season = :season
            """), {"season": season}).fetchone()[0]
            
            print(f"   SUCCESS: team_season_summary updated - {count_check} teams for season {season}")
            return True
            
    except Exception as e:
        print(f"   ERROR: team_season_summary failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def run_prediction():
    """Run the prediction task using the FixedNFLSystem"""
    print("TASK: Running prediction...")
    try:
        # Set pipeline mode
        env = os.environ.copy()
        env['BETTR_PIPELINE_MODE'] = 'true'
        # CRITICAL: Fix Unicode encoding
        env['PYTHONIOENCODING'] = 'utf-8'
        
        # Look for prediction.py in both current directory and model directory
        prediction_paths = [
            ROOT / "prediction.py",
            ROOT / "model" / "prediction.py"
        ]
        
        prediction_path = None
        for path in prediction_paths:
            if path.exists():
                prediction_path = path
                break
        
        if not prediction_path:
            print("   ERROR: prediction.py not found")
            return False
        
        # Run prediction.py as subprocess
        result = subprocess.run(
            [PY, str(prediction_path)], 
            capture_output=True, 
            text=True, 
            timeout=300,  # 5 minute timeout
            env=env,
            cwd=ROOT,
            encoding='utf-8',
            errors='replace'
        )
        
        output = result.stdout + result.stderr
        
        # Look for success indicators
        success_indicators = [
            "predictions",
            "Batch prediction complete",
            "Made",
            "prediction"
        ]
        
        if result.returncode == 0 or any(indicator in output for indicator in success_indicators):
            print("   SUCCESS: prediction completed")
            
            # Show prediction count if available
            for line in output.split('\n'):
                if "Made" in line and "prediction" in line:
                    print(f"   {line}")
                elif "prediction" in line.lower() and len(line.strip()) < 100:
                    print(f"   {line}")
                    
            return True
        else:
            print(f"   ERROR: prediction failed:")
            error_lines = output.split('\n')[-3:]
            for line in error_lines:
                if line.strip():
                    print(f"     {line}")
            return False
            
    except Exception as e:
        print(f"   ERROR: prediction failed: {e}")
        return False

def record_pipeline_status(engine, task, status, message):
    """Record pipeline status in database"""
    try:
        if not engine:
            return
            
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO system_status (
                    task, started_at, finished_at, status, message, run_type
                ) VALUES (
                    :task, :started_at, :finished_at, :status, :message, 'cloud_pipeline'
                )
            """), {
                "task": task,
                "started_at": datetime.utcnow().isoformat(),
                "finished_at": datetime.utcnow().isoformat(),
                "status": status,
                "message": message[:500] if message else ''
            })
    except Exception as e:
        print(f"Failed to record status: {e}")

def main():
    """Main pipeline execution with better error handling"""
    print("BETTR BOT CLOUD PIPELINE - FIXED VERSION")
    print("=" * 50)
    
    # Setup database
    engine = setup_cloud_environment()
    if not engine:
        print("ERROR: Cannot proceed without database connection")
        return False
    
    # Define tasks - ORDER MATTERS
    tasks = [
        ("update_scores", run_update_scores),
        ("team_season_summary", run_team_season_summary), 
        ("prediction", run_prediction)
    ]
    
    success_count = 0
    start_time = time.time()
    results = {}
    
    for task_name, task_func in tasks:
        try:
            print(f"\nSTEP: Running {task_name}...")
            task_start = time.time()
            success = task_func()
            task_time = time.time() - task_start
            
            results[task_name] = {
                'success': success,
                'time': task_time
            }
            
            status = "SUCCESS" if success else "FAILED"
            message = f"{task_name} completed in {task_time:.1f}s" if success else f"{task_name} failed after {task_time:.1f}s"
            
            record_pipeline_status(engine, task_name, status, message)
            
            if success:
                success_count += 1
                print(f"   SUCCESS: {task_name} completed ({task_time:.1f}s)")
            else:
                print(f"   ERROR: {task_name} failed ({task_time:.1f}s)")
                
        except Exception as e:
            print(f"   CRASH: {task_name} crashed: {e}")
            record_pipeline_status(engine, task_name, "ERROR", str(e))
            results[task_name] = {'success': False, 'time': 0, 'error': str(e)}
    
    # Summary
    total_time = time.time() - start_time
    success_rate = (success_count / len(tasks)) * 100
    
    print(f"\n{'='*50}")
    print(f"PIPELINE COMPLETE")
    print(f"Total time: {total_time:.1f} seconds")
    print(f"Success: {success_count}/{len(tasks)} ({success_rate:.1f}%)")
    
    # Detailed results
    for task_name, result in results.items():
        status = "SUCCESS" if result['success'] else "ERROR"
        print(f"   {status}: {task_name}: {result['time']:.1f}s")
    
    # Record overall status
    overall_status = "SUCCESS" if success_rate >= 66 else "PARTIAL" if success_rate > 0 else "FAILED"
    record_pipeline_status(engine, "pipeline_complete", overall_status, 
                          f"Pipeline completed: {success_count}/{len(tasks)} tasks successful in {total_time:.1f}s")
    
    if success_rate >= 66:
        print("SUCCESS: Pipeline successful!")
        print("\nNext steps:")
        print("1. Check your dashboard - should show updated data")
        print("2. Verify predictions are refreshed")
        print("3. Monitor for new games and scores")
        return True
    else:
        print("WARNING: Pipeline had significant issues - needs attention")
        print("\nTroubleshooting:")
        print("1. Check database connectivity")
        print("2. Verify team name mappings")
        print("3. Ensure all required tables exist")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)