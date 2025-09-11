# cloud_run_all.py - Updated cloud-optimized pipeline
"""
Cloud-optimized data pipeline for Bettr Bot
Designed for hosting services like Render, Railway, or Heroku
"""

import subprocess
import sys
import time
import os
from datetime import datetime
from sqlalchemy import create_engine, text
from config import get_config

from pathlib import Path
import sys, subprocess, os, time
from datetime import datetime

# Make repo root importable (…/BettrBot/)
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
    
PY = sys.executable

TASKS = [
    ("setup_db",            [PY, "-m", "data.setup_db"]),
    ("check_scores",        [PY, "-m", "stats.check_scores"]),
    ("update_scores",       [PY, "-m", "stats.update_scores"]),
    ("team_season_summary", [PY, "-m", "stats.team_season_summary"]),
    ("train_betting_model", [PY, "-m", "model.train_betting_model"]),
    ("prediction",          [PY, "-m", "model.prediction"]),
    ("get_odds",            [PY, "-m", "odds.get_odds_fixed"]),
]


def setup_cloud_environment():
    """Setup for cloud deployment"""
    config_class = get_config()
    
    # Create database engine
    try:
        engine = create_engine(config_class.get_database_url(), pool_pre_ping=True)
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            
        print("✅ Database connection established")
        return engine
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def ensure_status_table(engine):
    """Ensure system_status table exists"""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS system_status (
                id SERIAL PRIMARY KEY,
                task TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT,
                message TEXT,
                run_type TEXT DEFAULT 'cloud',
                timeout_seconds INTEGER
            )
        """))

def record_status(engine, task, started_at, finished_at, status, message, run_type='cloud'):
    """Record task status"""
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO system_status (task, started_at, finished_at, status, message, run_type)
            VALUES (:task, :started_at, :finished_at, :status, :message, :run_type)
        """), dict(
            task=task,
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            message=message[:500] if message else '',
            run_type=run_type
        ))

def run_cloud_task(engine, name, module_name, timeout=300):
    """Run a Python module as a task with cloud-friendly approach"""
    started_at = datetime.utcnow().isoformat()
    
    print(f"🚀 Running {name}...")
    
    try:
        # Import and run the module directly instead of subprocess
        # This is more reliable in cloud environments
        
        if module_name == 'check_scores':
            from stats.check_scores import main as task_main
        elif module_name == 'update_scores':
            from stats.update_scores import main as task_main
        elif module_name == 'team_season_summary':
            from stats.team_season_summary import main as task_main
        elif module_name == 'train_betting_model':
            from model.train_betting_model import main as task_main
        elif module_name == 'prediction':
            from model.prediction import main as task_main
        elif module_name == 'get_odds_fixed':
            from odds.get_odds_fixed import main as task_main
        else:
            raise ImportError(f"Unknown module: {module_name}")
        
        # Run the task
        result = task_main()
        
        finished_at = datetime.utcnow().isoformat()
        
        print(f"✅ {name} completed successfully")
        record_status(engine, name, started_at, finished_at, "OK", "Completed successfully")
        
        return True
        
    except ImportError as e:
        finished_at = datetime.utcnow().isoformat()
        print(f"⚠️ {name} - Module not found: {e}")
        record_status(engine, name, started_at, finished_at, "SKIP", f"Module not available: {e}")
        return True  # Don't fail the pipeline for missing modules
        
    except Exception as e:
        finished_at = datetime.utcnow().isoformat()
        print(f"❌ {name} failed: {e}")
        record_status(engine, name, started_at, finished_at, "FAIL", str(e))
        return False

def cloud_pipeline():
    """Run the essential pipeline for cloud deployment"""
    print("🌐 BETTR BOT CLOUD PIPELINE")
    print("=" * 40)
    
    # Setup
    engine = setup_cloud_environment()
    if not engine:
        print("💥 Cannot proceed without database connection")
        return False
    
    ensure_status_table(engine)
    
    # Core tasks for cloud deployment
    CLOUD_TASKS = [
        ("Database Check", "setup_db"),
        ("Score Check", "check_scores"),
        ("Score Update", "update_scores"),
        ("Team Summary", "team_season_summary"),
        ("Model Training", "train_betting_model"),
        ("Generate Predictions", "prediction"),
        ("Fetch Odds", "get_odds_fixed"),
    ]
    
    print(f"📋 Running {len(CLOUD_TASKS)} essential tasks")
    
    start_time = time.time()
    success_count = 0
    
    for name, module in CLOUD_TASKS:
        try:
            success = run_cloud_task(engine, name, module)
            if success:
                success_count += 1
            
            # Brief pause between tasks
            time.sleep(1)
            
        except KeyboardInterrupt:
            print("\n⏹️ Pipeline interrupted by user")
            break
        except Exception as e:
            print(f"💥 Unexpected error in {name}: {e}")
            continue
    
    # Summary
    total_time = time.time() - start_time
    success_rate = (success_count / len(CLOUD_TASKS)) * 100
    
    print(f"\n{'='*40}")
    print(f"🏁 PIPELINE COMPLETE")
    print(f"⏱️  Total time: {total_time:.1f} seconds")
    print(f"✅ Success: {success_count}/{len(CLOUD_TASKS)} ({success_rate:.1f}%)")
    
    if success_count >= len(CLOUD_TASKS) * 0.7:  # 70% success threshold
        print(f"🎉 Pipeline successful - predictions ready!")
        return True
    else:
        print(f"⚠️ Pipeline had issues - check logs")
        return False

def health_check():
    """Quick health check for monitoring"""
    engine = setup_cloud_environment()
    if not engine:
        return False
    
    try:
        with engine.connect() as conn:
            # Check if we have recent data
            result = conn.execute(text("""
                SELECT COUNT(*) FROM games 
                WHERE game_date >= date('now', '-7 days')
            """)).fetchone()
            
            recent_games = result[0] if result else 0
            
            if recent_games > 0:
                print(f"✅ Health check passed - {recent_games} recent games")
                return True
            else:
                print("⚠️ No recent games found")
                return False
                
    except Exception as e:
        print(f"❌ Health check failed: {e}")
        return False

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "health":
        # Health check mode
        success = health_check()
        sys.exit(0 if success else 1)
    else:
        # Full pipeline
        success = cloud_pipeline()
        sys.exit(0 if success else 1)