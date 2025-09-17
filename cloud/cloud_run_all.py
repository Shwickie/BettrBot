# cloud_run_all.py - Updated cloud-optimized pipeline for Bettr Bot
"""
Cloud-optimized data pipeline for Bettr Bot
Handles daily updates: scores, team stats, predictions, odds
Model training runs weekly to prevent overfitting
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

# Add parent directory for model imports
PARENT = ROOT.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

PY = sys.executable

# Updated task definitions with proper scheduling
DAILY_TASKS = [
    ("check_scores",        "check_scores"),
    ("update_scores",       "update_scores"), 
    ("team_season_summary", "team_season_summary"),
    ("prediction",          "prediction"),
    ("get_odds",            "get_odds_fixed"),
]

WEEKLY_TASKS = [
    ("train_betting_model", "train_betting_model"),  # Only Mondays
]

def setup_cloud_environment():
    """Setup for cloud deployment with better error handling"""
    try:
        # Direct environment reading instead of config import
        DATABASE_URL = os.environ.get("DATABASE_URL")
        if not DATABASE_URL:
            print("WARNING: No DATABASE_URL found, using default")
            DATABASE_URL = "postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres"
        
        # Fix postgres:// URLs
        if DATABASE_URL.startswith('postgres://'):
            DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
        
        # Create database engine
        engine = create_engine(
            DATABASE_URL, 
            pool_pre_ping=True,
            pool_recycle=300,
            connect_args={"sslmode": "require"} if "localhost" not in DATABASE_URL else {}
        )
        
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
    try:
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
                    timeout_seconds INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
        return True
    except Exception as e:
        print(f"Failed to ensure status table: {e}")
        return False

def record_status(engine, task, started_at, finished_at, status, message, run_type='cloud'):
    """Record task status with better error handling"""
    try:
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
    except Exception as e:
        print(f"Failed to record status for {task}: {e}")

def should_run_weekly_tasks():
    """Check if today is Monday (model training day)"""
    return date.today().weekday() == 0  # Monday = 0

def run_cloud_task(engine, name, module_name, timeout=300):
    """Run a Python module as a task with improved import handling"""
    started_at = datetime.utcnow().isoformat()
    
    print(f"🚀 Running {name}...")
    
    try:
        # Import and run the module directly - FIXED import logic
        task_main = None
        
        if module_name == 'check_scores':
            try:
                from stats.check_scores import main as task_main
            except ImportError:
                try:
                    from check_scores import main as task_main
                except ImportError:
                    print(f"   ⚠️ check_scores module not found")
                    
        elif module_name == 'update_scores':
            try:
                from stats.update_scores import main as task_main
            except ImportError:
                try:
                    from update_scores import main as task_main
                except ImportError:
                    print(f"   ⚠️ update_scores module not found")
                    
        elif module_name == 'team_season_summary':
            try:
                from stats.team_season_summary import main as task_main
            except ImportError:
                try:
                    from team_season_summary import main as task_main
                except ImportError:
                    print(f"   ⚠️ team_season_summary module not found")
                    
        elif module_name == 'train_betting_model':
            try:
                from model.train_betting_model import main as task_main
            except ImportError:
                try:
                    from train_betting_model import main as task_main
                except ImportError:
                    print(f"   ⚠️ train_betting_model module not found")
                    
        elif module_name == 'prediction':
            try:
                from model.prediction import main as task_main
            except ImportError:
                try:
                    from prediction import main as task_main
                except ImportError:
                    # For prediction, we can set pipeline mode and run directly
                    try:
                        os.environ['BETTR_PIPELINE_MODE'] = 'true'
                        from prediction import FixedNFLSystem
                        system = FixedNFLSystem()
                        system.show_all_predictions_batch()
                        print(f"   ✅ {name} completed using FixedNFLSystem")
                        finished_at = datetime.utcnow().isoformat()
                        record_status(engine, name, started_at, finished_at, "OK", "Completed using FixedNFLSystem")
                        return True
                    except Exception as e:
                        print(f"   ❌ prediction system failed: {e}")
                        
        elif module_name == 'get_odds_fixed':
            try:
                from odds.get_odds_fixed import main as task_main
            except ImportError:
                try:
                    from get_odds_fixed import main as task_main
                except ImportError:
                    print(f"   ⚠️ get_odds_fixed module not found")
        
        # If we found a main function, run it
        if task_main:
            result = task_main()
            finished_at = datetime.utcnow().isoformat()
            print(f"   ✅ {name} completed successfully")
            record_status(engine, name, started_at, finished_at, "OK", "Completed successfully")
            return True
        else:
            finished_at = datetime.utcnow().isoformat()
            print(f"   ⚠️ {name} - Module not available")
            record_status(engine, name, started_at, finished_at, "SKIP", f"Module {module_name} not available")
            return True  # Don't fail the pipeline for missing optional modules
            
    except ImportError as e:
        finished_at = datetime.utcnow().isoformat()
        print(f"   ⚠️ {name} - Import failed: {e}")
        record_status(engine, name, started_at, finished_at, "SKIP", f"Import failed: {e}")
        return True  # Don't fail pipeline for import issues
        
    except Exception as e:
        finished_at = datetime.utcnow().isoformat()
        print(f"   ❌ {name} failed: {e}")
        record_status(engine, name, started_at, finished_at, "FAIL", str(e))
        return False

def cloud_pipeline():
    """Run the essential daily pipeline with weekly model training"""
    print("🌐 BETTR BOT CLOUD PIPELINE")
    print("=" * 40)
    
    # Setup
    engine = setup_cloud_environment()
    if not engine:
        print("💥 Cannot proceed without database connection")
        return False
    
    ensure_status_table(engine)
    
    # Determine tasks to run
    today_is_training_day = should_run_weekly_tasks()
    tasks_to_run = DAILY_TASKS.copy()
    
    if today_is_training_day:
        print("📅 Monday detected - including weekly model training")
        tasks_to_run.extend(WEEKLY_TASKS)
    else:
        print("📅 Regular day - daily tasks only")
    
    print(f"📋 Running {len(tasks_to_run)} tasks")
    
    start_time = time.time()
    success_count = 0
    
    for name, module in tasks_to_run:
        try:
            # Special handling for model training
            if module == "train_betting_model" and not today_is_training_day:
                print(f"⏭️ Skipping {name} (not training day)")
                continue
                
            success = run_cloud_task(engine, name, module)
            if success:
                success_count += 1
            
            # Brief pause between tasks
            time.sleep(2)
            
        except KeyboardInterrupt:
            print("\n⏹️ Pipeline interrupted by user")
            break
        except Exception as e:
            print(f"💥 Unexpected error in {name}: {e}")
            continue
    
    # Summary
    total_time = time.time() - start_time
    success_rate = (success_count / len(tasks_to_run)) * 100 if tasks_to_run else 0
    
    print(f"\n{'='*40}")
    print(f"🏁 PIPELINE COMPLETE")
    print(f"⏱️ Total time: {total_time:.1f} seconds")
    print(f"✅ Success: {success_count}/{len(tasks_to_run)} ({success_rate:.1f}%)")
    
    if today_is_training_day:
        print(f"🤖 Model training {'included' if 'train_betting_model' in [t[1] for t in tasks_to_run] else 'skipped'}")
    
    # Record pipeline completion
    try:
        record_status(engine, "pipeline_complete", 
                     datetime.utcnow().isoformat(),
                     datetime.utcnow().isoformat(),
                     "OK" if success_rate >= 70 else "PARTIAL",
                     f"Pipeline completed: {success_count}/{len(tasks_to_run)} tasks successful")
    except:
        pass
    
    if success_rate >= 70:
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
                WHERE game_date >= date('now') - interval '7 days'
            """)).fetchone()
            
            recent_games = result[0] if result else 0
            
            # Check if model file exists
            model_paths = [
                "./betting_model_fixed.pkl",
                "./models/betting_model_fixed.pkl",
                os.path.join(os.path.dirname(__file__), "betting_model_fixed.pkl")
            ]
            
            model_exists = any(os.path.exists(path) for path in model_paths)
            
            if recent_games > 0 and model_exists:
                print(f"✅ Health check passed - {recent_games} recent games, model available")
                return True
            else:
                print(f"⚠️ Health issues - games: {recent_games}, model: {model_exists}")
                return False
                
    except Exception as e:
        print(f"❌ Health check failed: {e}")
        return False

def show_last_run_status():
    """Show status of last pipeline run"""
    engine = setup_cloud_environment()
    if not engine:
        return
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT task, finished_at, status, message 
                FROM system_status 
                WHERE started_at >= date('now') - interval '2 days'
                ORDER BY started_at DESC 
                LIMIT 10
            """)).fetchall()
            
            if result:
                print("\n📊 Recent Task Status:")
                for row in result:
                    task, finished, status, msg = row
                    print(f"  {task}: {status} ({finished}) - {msg[:50]}")
            else:
                print("No recent task history found")
                
    except Exception as e:
        print(f"Failed to get status: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "health":
            # Health check mode
            success = health_check()
            sys.exit(0 if success else 1)
            
        elif command == "status":
            # Show recent status
            show_last_run_status()
            sys.exit(0)
            
        elif command == "force-training":
            # Force model training regardless of day
            print("🔄 Forcing model training...")
            engine = setup_cloud_environment()
            if engine:
                ensure_status_table(engine)
                success = run_cloud_task(engine, "Force Model Training", "train_betting_model")
                sys.exit(0 if success else 1)
            else:
                sys.exit(1)
    else:
        # Full pipeline mode
        success = cloud_pipeline()
        sys.exit(0 if success else 1)