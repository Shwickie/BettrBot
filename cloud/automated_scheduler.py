# automated_scheduler.py - COMPLETE Automated Scheduler
"""
COMPLETE VERSION: Points to the correct cloud_run_all.py pipeline
- Calls cloud_run_all.py (not cloud_run_all_complete.py)
- Includes proper error handling and monitoring
- Safe scheduling intervals
"""

import os
import sys
import time
import logging
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

# Set up logging with better formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Safe APScheduler import
try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.interval import IntervalTrigger
    from apscheduler.triggers.cron import CronTrigger
    APSCHEDULER_AVAILABLE = True
except ImportError:
    APSCHEDULER_AVAILABLE = False
    logger.error("APScheduler not available - install with: pip install APScheduler==3.10.4")

class BettrBotScheduler:
    """Complete automated scheduler for Bettr Bot pipeline"""
    
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.scheduler = None
        self.last_pipeline_run = None
        self.run_count = 0
        self.consecutive_failures = 0
        self.last_failure_time = None
        
        # FIXED: Point to the correct pipeline script
        self.pipeline_script = self.project_root / "cloud_run_all.py"
        
        # Safety settings
        self.MAX_CONSECUTIVE_FAILURES = 3
        self.COOLDOWN_AFTER_FAILURE = 3600  # 1 hour cooldown after failures
        
    def setup_scheduler(self):
        """Initialize scheduler with proper timezone handling"""
        if not APSCHEDULER_AVAILABLE:
            return False
            
        self.scheduler = BackgroundScheduler(timezone='UTC')
        logger.info("Bettr Bot scheduler configured")
        return True
    
    def check_database_health(self):
        """Check database health before running pipeline"""
        try:
            from sqlalchemy import create_engine, text
            
            DATABASE_URL = "postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres"
            engine = create_engine(DATABASE_URL, pool_pre_ping=True)
            
            with engine.connect() as conn:
                # Check basic connectivity
                conn.execute(text("SELECT 1"))
                
                # Check critical tables exist
                critical_tables = ['games', 'team_season_summary', 'odds']
                for table in critical_tables:
                    result = conn.execute(text(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = '{table}'
                        );
                    """)).scalar()
                    if not result:
                        logger.error(f"Critical table {table} missing")
                        return False
                
                logger.info("Database health check passed")
                return True
                
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False
    
    def run_complete_pipeline(self):
        """Execute the complete pipeline with proper error handling"""
        self.run_count += 1
        start_time = time.time()
        
        logger.info(f"\n{'='*60}")
        logger.info(f"PIPELINE RUN #{self.run_count}")
        logger.info(f"Started: {datetime.now()} UTC")
        logger.info(f"{'='*60}")
        
        # Check if we're in cooldown period after failures
        if (self.last_failure_time and 
            time.time() - self.last_failure_time < self.COOLDOWN_AFTER_FAILURE):
            remaining_cooldown = self.COOLDOWN_AFTER_FAILURE - (time.time() - self.last_failure_time)
            logger.warning(f"In cooldown period, {remaining_cooldown/60:.1f} minutes remaining")
            return False
        
        # Pre-flight checks
        if not self.check_database_health():
            logger.error("Database health check failed - aborting pipeline")
            self.consecutive_failures += 1
            self.last_failure_time = time.time()
            return False
        
        try:
            # FIXED: Use the correct pipeline script
            if not self.pipeline_script.exists():
                logger.error(f"Pipeline script {self.pipeline_script} not found")
                return False
            
            result = self._run_python_script(str(self.pipeline_script))
            
            duration = time.time() - start_time
            
            if result.returncode == 0:
                self.last_pipeline_run = datetime.now()
                self.consecutive_failures = 0
                self.last_failure_time = None
                logger.info(f"SUCCESS - Duration: {duration:.1f}s")
                logger.info("All bot systems updated successfully")
                
                # Log successful run details
                output_lines = (result.stdout + result.stderr).split('\n')
                for line in output_lines:
                    if any(word in line.lower() for word in ['success', 'updated', 'processed', 'predictions']):
                        if len(line.strip()) > 0 and len(line) < 200:
                            logger.info(f"  {line.strip()}")
                
                return True
                    
            else:
                self.consecutive_failures += 1
                self.last_failure_time = time.time()
                logger.error(f"PIPELINE FAILED - Duration: {duration:.1f}s")
                logger.error(f"Consecutive failures: {self.consecutive_failures}")
                
                # Log error details
                if result.stderr:
                    logger.error(f"Error output: {result.stderr[:500]}")
                
                # Send alert if too many failures
                if self.consecutive_failures >= self.MAX_CONSECUTIVE_FAILURES:
                    self._send_failure_alert()
                
                return False
                
        except Exception as e:
            duration = time.time() - start_time
            self.consecutive_failures += 1
            self.last_failure_time = time.time()
            logger.error(f"PIPELINE CRASHED - Duration: {duration:.1f}s")
            logger.error(f"Error: {e}")
            
            if self.consecutive_failures >= self.MAX_CONSECUTIVE_FAILURES:
                self._send_failure_alert()
            
            return False
        
        finally:
            logger.info(f"{'='*60}\n")
    
    def _send_failure_alert(self):
        """Send alert for consecutive failures"""
        logger.error(f"ALERT: {self.consecutive_failures} consecutive pipeline failures!")
        logger.error("System entering safe mode - manual intervention required")
        
        # Write failure file that monitoring can pick up
        try:
            failure_file = self.project_root / "PIPELINE_FAILURE.flag"
            with open(failure_file, 'w') as f:
                f.write(f"Pipeline failed {self.consecutive_failures} times\n")
                f.write(f"Last attempt: {datetime.now()}\n")
                f.write(f"Safe mode activated\n")
        except Exception:
            pass
    
    def _run_python_script(self, script_path, timeout=1800):
        """Run a Python script with proper environment"""
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"Script not found: {script_path}")
        
        env = os.environ.copy()
        env['BETTR_PIPELINE_MODE'] = 'true'
        env['PYTHONIOENCODING'] = 'utf-8'
        
        logger.info(f"Executing: {script_path}")
        
        return subprocess.run(
            [sys.executable, script_path],
            cwd=str(self.project_root),
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
            encoding='utf-8',
            errors='replace'
        )
    
    def quick_health_check(self):
        """Quick health check of the system"""
        try:
            logger.info("Running quick health check...")
            
            # Check database health
            if not self.check_database_health():
                return False
            
            # Check if pipeline script exists
            if not self.pipeline_script.exists():
                logger.error(f"Pipeline script missing: {self.pipeline_script}")
                return False
            
            # Check if model file exists
            model_paths = [
                self.project_root / "betting_model_fixed.pkl",
                self.project_root / "models" / "betting_model_fixed.pkl",
                Path(os.getcwd()) / "betting_model_fixed.pkl"
            ]
            
            model_found = False
            for path in model_paths:
                if path.exists():
                    logger.info(f"Model found at {path}")
                    model_found = True
                    break
            
            if not model_found:
                logger.warning("Model file not found - predictions may not work")
            
            return True
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    def add_scheduled_jobs(self):
        """Add scheduled jobs with proper intervals"""
        if not self.scheduler:
            return False
        
        # Main pipeline runs every 4 hours (aligned with your cron job)
        self.scheduler.add_job(
            func=self.run_complete_pipeline,
            trigger=IntervalTrigger(hours=4),
            id='main_pipeline',
            name='Complete Pipeline Every 4 Hours',
            replace_existing=True,
            max_instances=1  # Prevent overlapping runs
        )
        
        # Health check every 2 hours
        self.scheduler.add_job(
            func=self.quick_health_check,
            trigger=IntervalTrigger(hours=2),
            id='health_check',
            name='Health Check Every 2 Hours', 
            replace_existing=True
        )
        
        # Daily maintenance - 3 AM UTC
        self.scheduler.add_job(
            func=self.daily_maintenance,
            trigger=CronTrigger(hour=3, minute=0),
            id='daily_maintenance',
            name='Daily Maintenance',
            replace_existing=True
        )
        
        logger.info("Scheduled jobs configured:")
        logger.info("  - Main pipeline: Every 4 hours")
        logger.info("  - Health checks: Every 2 hours")
        logger.info("  - Daily maintenance: 3 AM UTC")
        
        return True
    
    def daily_maintenance(self):
        """Daily maintenance tasks"""
        logger.info("Running daily maintenance...")
        
        try:
            # Check if we should skip maintenance due to recent failures
            if self.consecutive_failures >= 2:
                logger.warning("Skipping maintenance due to recent pipeline failures")
                return
            
            # Run pipeline
            success = self.run_complete_pipeline()
            
            # Clean up old logs only if pipeline succeeded
            if success:
                self._cleanup_old_files()
                
                # Reset failure counter on successful maintenance
                self.consecutive_failures = 0
                
                # Remove failure flag if it exists
                failure_file = self.project_root / "PIPELINE_FAILURE.flag"
                if failure_file.exists():
                    failure_file.unlink()
                    logger.info("Cleared failure flag")
            
            logger.info("Daily maintenance completed")
            
        except Exception as e:
            logger.error(f"Daily maintenance failed: {e}")
    
    def _cleanup_old_files(self):
        """Clean up old log files"""
        try:
            # Clean logs older than 30 days
            log_files = list(self.project_root.glob("*.log"))
            cutoff = time.time() - (30 * 24 * 3600)
            
            cleaned = 0
            for log_file in log_files:
                try:
                    if log_file.stat().st_mtime < cutoff:
                        log_file.unlink()
                        cleaned += 1
                except Exception:
                    pass
            
            if cleaned > 0:
                logger.info(f"Cleaned up {cleaned} old log files")
                        
        except Exception as e:
            logger.warning(f"File cleanup failed: {e}")
    
    def start(self):
        """Start the scheduler"""
        if not self.setup_scheduler():
            return False
        
        if not self.add_scheduled_jobs():
            return False
        
        try:
            # Run initial health check
            if not self.quick_health_check():
                logger.warning("Initial health check failed - starting anyway with caution")
            
            self.scheduler.start()
            logger.info("Bettr Bot scheduler started successfully")
            
            # Run initial pipeline if no recent failures
            if self.last_pipeline_run is None and self.consecutive_failures == 0:
                logger.info("Running initial pipeline...")
                self.run_complete_pipeline()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")
            return False
    
    def stop(self):
        """Stop the scheduler gracefully"""
        if self.scheduler and self.scheduler.running:
            try:
                self.scheduler.shutdown(wait=True, timeout=30)
                logger.info("Scheduler stopped gracefully")
            except Exception as e:
                logger.error(f"Error stopping scheduler: {e}")
    
    def get_status(self):
        """Get scheduler status"""
        return {
            'running': self.scheduler.running if self.scheduler else False,
            'run_count': self.run_count,
            'last_pipeline_run': self.last_pipeline_run.isoformat() if self.last_pipeline_run else None,
            'consecutive_failures': self.consecutive_failures,
            'in_cooldown': bool(self.last_failure_time and 
                               time.time() - self.last_failure_time < self.COOLDOWN_AFTER_FAILURE),
            'apscheduler_available': APSCHEDULER_AVAILABLE,
            'pipeline_script': str(self.pipeline_script),
            'pipeline_exists': self.pipeline_script.exists()
        }
    
    def force_run(self):
        """Force run the pipeline"""
        if self.consecutive_failures >= self.MAX_CONSECUTIVE_FAILURES:
            logger.warning("Cannot force run - system in safe mode")
            return False
            
        logger.info("Force running pipeline...")
        return self.run_complete_pipeline()

# Global instance for app integration
_global_scheduler = None

def get_scheduler():
    """Get the global scheduler instance"""
    global _global_scheduler
    if _global_scheduler is None:
        _global_scheduler = BettrBotScheduler()
    return _global_scheduler

def start_background_scheduler():
    """Start the background scheduler"""
    scheduler = get_scheduler()
    return scheduler.start()

def stop_background_scheduler():
    """Stop the background scheduler"""
    scheduler = get_scheduler()
    scheduler.stop()

def get_scheduler_status():
    """Get scheduler status"""
    scheduler = get_scheduler()
    return scheduler.get_status()

def force_pipeline_run():
    """Force run the pipeline"""
    scheduler = get_scheduler()
    return scheduler.force_run()

# Manual execution and CLI
if __name__ == "__main__":
    print("BETTR BOT AUTOMATED SCHEDULER (COMPLETE)")
    print("=" * 50)
    
    if not APSCHEDULER_AVAILABLE:
        print("APScheduler not installed")
        print("Install with: pip install APScheduler==3.10.4")
        sys.exit(1)
    
    scheduler = BettrBotScheduler()
    
    # Handle command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "--once":
            print("Running pipeline once...")
            success = scheduler.run_complete_pipeline()
            sys.exit(0 if success else 1)
        elif sys.argv[1] == "--health":
            print("Running health check...")
            healthy = scheduler.quick_health_check()
            sys.exit(0 if healthy else 1)
        elif sys.argv[1] == "--status":
            status = scheduler.get_status()
            print(f"Status: {status}")
            sys.exit(0)
    
    # Start scheduler
    if not scheduler.start():
        print("Failed to start scheduler")
        sys.exit(1)
    
    print("Scheduler started successfully!")
    print(f"Pipeline script: {scheduler.pipeline_script}")
    print("Pipeline runs every 4 hours")
    print("Health checks every 2 hours")
    print("Daily maintenance at 3 AM UTC")
    print("Press Ctrl+C to stop...")
    
    try:
        while True:
            time.sleep(60)
            status = scheduler.get_status()
            if status.get('running'):
                health = "healthy" if status['consecutive_failures'] == 0 else f"degraded ({status['consecutive_failures']} failures)"
                safe_mode = " [SAFE MODE]" if status.get('consecutive_failures', 0) >= 3 else ""
                print(f"Running... (completed {status['run_count']} runs, {health}){safe_mode}")
            else:
                print("Scheduler not running")
                break
    except KeyboardInterrupt:
        print("\nStopping scheduler...")
        scheduler.stop()
        print("Scheduler stopped.")