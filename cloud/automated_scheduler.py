# automated_scheduler.py - Enhanced scheduling system for cloud deployment
"""
Automated scheduler that runs your pipeline every 4 hours with proper error handling
Can be run as a standalone service or integrated into your Flask app
FIXED: Unicode encoding issues for Windows compatibility
"""

import os
import sys
import time
import logging
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

# Safe APScheduler import with fallback
try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.interval import IntervalTrigger
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.executors.pool import ThreadPoolExecutor
    from apscheduler.jobstores.memory import MemoryJobStore
    APSCHEDULER_AVAILABLE = True
except ImportError as e:
    print(f"APScheduler not available: {e}")
    print("Install with: pip install APScheduler==3.10.4")
    APSCHEDULER_AVAILABLE = False
    BackgroundScheduler = None

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BettrBotScheduler:
    """Automated scheduler for Bettr Bot pipeline updates"""
    
    def __init__(self):
        self.scheduler = None
        self.is_cloud = bool(os.environ.get('DATABASE_URL'))
        self.project_root = Path(__file__).parent
        self.pipeline_script = self.project_root / "cloud_run_all.py"
        self.last_run = None
        self.run_count = 0
        
    def setup_scheduler(self):
        """Initialize the APScheduler with proper configuration"""
        if not APSCHEDULER_AVAILABLE:
            logger.error("APScheduler not available - cannot set up automated scheduling")
            return False
            
        # Configure job stores and executors
        jobstores = {
            'default': MemoryJobStore()
        }
        
        executors = {
            'default': ThreadPoolExecutor(max_workers=2)
        }
        
        job_defaults = {
            'coalesce': True,
            'max_instances': 1,
            'misfire_grace_time': 300  # 5 minutes
        }
        
        self.scheduler = BackgroundScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone='UTC'
        )
        
        logger.info("Scheduler configured successfully")
        return True
    
    def run_pipeline(self):
        """Execute the cloud pipeline with proper error handling and Unicode safety"""
        try:
            self.run_count += 1
            logger.info(f"Starting pipeline run #{self.run_count}")
            
            # Set environment variables for pipeline mode
            env = os.environ.copy()
            env['BETTR_PIPELINE_MODE'] = 'true'
            # CRITICAL: Fix Unicode encoding for Windows
            env['PYTHONIOENCODING'] = 'utf-8'
            
            # Run the pipeline script
            start_time = datetime.now()
            result = subprocess.run(
                [sys.executable, str(self.pipeline_script)],
                capture_output=True,
                text=True,
                timeout=1800,  # 30 minute timeout
                env=env,
                cwd=str(self.project_root),
                encoding='utf-8',  # Force UTF-8 encoding
                errors='replace'   # Replace problematic characters
            )
            
            duration = datetime.now() - start_time
            self.last_run = start_time
            
            # Log results
            if result.returncode == 0:
                logger.info(f"Pipeline run #{self.run_count} completed successfully in {duration}")
                logger.info(f"Output preview: {result.stdout[-200:] if result.stdout else 'No output'}")
            else:
                logger.error(f"Pipeline run #{self.run_count} failed with code {result.returncode}")
                logger.error(f"Error output: {result.stderr[-500:] if result.stderr else 'No error output'}")
            
            # Record run in a simple log file
            self._record_run(start_time, duration, result.returncode == 0)
            
        except subprocess.TimeoutExpired:
            logger.error(f"Pipeline run #{self.run_count} timed out after 30 minutes")
        except Exception as e:
            logger.error(f"Pipeline run #{self.run_count} failed with exception: {e}")
    
    def _record_run(self, start_time, duration, success):
        """Record pipeline run in a log file"""
        try:
            log_file = self.project_root / "pipeline_runs.log"
            with open(log_file, "a", encoding='utf-8') as f:
                f.write(f"{start_time.isoformat()},{duration.total_seconds()},{success},{self.run_count}\n")
        except Exception as e:
            logger.warning(f"Could not record run to log file: {e}")
    
    def add_jobs(self):
        """Add scheduled jobs to the scheduler"""
        if not self.scheduler:
            logger.error("Scheduler not initialized")
            return False
        
        # Main pipeline job - every 4 hours
        self.scheduler.add_job(
            func=self.run_pipeline,
            trigger=IntervalTrigger(hours=4),
            id='pipeline_4_hour',
            name='Pipeline Every 4 Hours',
            replace_existing=True
        )
        
        # Daily summary job at 6 AM UTC
        self.scheduler.add_job(
            func=self.daily_summary,
            trigger=CronTrigger(hour=6, minute=0),
            id='daily_summary',
            name='Daily Summary',
            replace_existing=True
        )
        
        # Weekly deep clean on Sundays at 2 AM UTC
        self.scheduler.add_job(
            func=self.weekly_maintenance,
            trigger=CronTrigger(day_of_week=6, hour=2, minute=0),
            id='weekly_maintenance',
            name='Weekly Maintenance',
            replace_existing=True
        )
        
        logger.info("Scheduled jobs added:")
        logger.info("  - Pipeline updates: Every 4 hours")
        logger.info("  - Daily summary: 6:00 AM UTC")
        logger.info("  - Weekly maintenance: Sunday 2:00 AM UTC")
        
        return True
    
    def daily_summary(self):
        """Generate daily summary of pipeline runs"""
        try:
            logger.info("Generating daily summary...")
            
            # Read recent runs from log
            log_file = self.project_root / "pipeline_runs.log"
            if log_file.exists():
                with open(log_file, "r", encoding='utf-8') as f:
                    lines = f.readlines()
                
                # Get runs from last 24 hours
                cutoff = datetime.now() - timedelta(days=1)
                recent_runs = []
                
                for line in lines[-50:]:  # Check last 50 runs
                    try:
                        parts = line.strip().split(',')
                        run_time = datetime.fromisoformat(parts[0])
                        if run_time > cutoff:
                            recent_runs.append({
                                'time': run_time,
                                'duration': float(parts[1]),
                                'success': parts[2] == 'True',
                                'run_number': int(parts[3])
                            })
                    except Exception:
                        continue
                
                # Generate summary
                total_runs = len(recent_runs)
                successful_runs = sum(1 for r in recent_runs if r['success'])
                avg_duration = sum(r['duration'] for r in recent_runs) / total_runs if total_runs > 0 else 0
                
                logger.info(f"Daily Summary: {successful_runs}/{total_runs} successful runs, avg duration: {avg_duration:.1f}s")
            
        except Exception as e:
            logger.error(f"Daily summary failed: {e}")
    
    def weekly_maintenance(self):
        """Perform weekly maintenance tasks"""
        try:
            logger.info("Running weekly maintenance...")
            
            # Clean up old log entries (keep last 1000)
            log_file = self.project_root / "pipeline_runs.log"
            if log_file.exists():
                with open(log_file, "r", encoding='utf-8') as f:
                    lines = f.readlines()
                
                if len(lines) > 1000:
                    with open(log_file, "w", encoding='utf-8') as f:
                        f.writelines(lines[-1000:])
                    logger.info(f"Trimmed log file to last 1000 entries")
            
            # Force a pipeline run for maintenance
            logger.info("Running maintenance pipeline update...")
            self.run_pipeline()
            
        except Exception as e:
            logger.error(f"Weekly maintenance failed: {e}")
    
    def start(self):
        """Start the scheduler"""
        if not APSCHEDULER_AVAILABLE:
            logger.error("Cannot start scheduler - APScheduler not available")
            return False
            
        if not self.setup_scheduler():
            return False
            
        if not self.add_jobs():
            return False
        
        try:
            self.scheduler.start()
            logger.info("Scheduler started successfully")
            
            # Run pipeline once immediately if this is the first start
            if self.run_count == 0:
                logger.info("Running initial pipeline update...")
                self.run_pipeline()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")
            return False
    
    def stop(self):
        """Stop the scheduler"""
        if self.scheduler and self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            logger.info("Scheduler stopped")
    
    def get_status(self):
        """Get current scheduler status"""
        status = {
            'running': self.scheduler.running if self.scheduler else False,
            'last_run': self.last_run.isoformat() if self.last_run else None,
            'run_count': self.run_count,
            'next_run': None,
            'apscheduler_available': APSCHEDULER_AVAILABLE
        }
        
        if self.scheduler and self.scheduler.running:
            jobs = self.scheduler.get_jobs()
            if jobs:
                next_job = min(jobs, key=lambda j: j.next_run_time)
                status['next_run'] = next_job.next_run_time.isoformat()
        
        return status

# Global scheduler instance
_scheduler = None

def get_scheduler():
    """Get the global scheduler instance"""
    global _scheduler
    if _scheduler is None:
        _scheduler = BettrBotScheduler()
    return _scheduler

def start_background_scheduler():
    """Start the background scheduler (for Flask integration)"""
    scheduler = get_scheduler()
    return scheduler.start()

def stop_background_scheduler():
    """Stop the background scheduler"""
    scheduler = get_scheduler()
    scheduler.stop()

def get_scheduler_status():
    """Get scheduler status for API endpoints"""
    scheduler = get_scheduler()
    return scheduler.get_status()

# Standalone execution
if __name__ == "__main__":
    print("BETTR BOT AUTOMATED SCHEDULER")
    print("=" * 40)
    
    if not APSCHEDULER_AVAILABLE:
        print("ERROR: APScheduler not installed")
        print("Install with: pip install APScheduler==3.10.4")
        sys.exit(1)
    
    scheduler = BettrBotScheduler()
    
    if not scheduler.start():
        print("Failed to start scheduler")
        sys.exit(1)
    
    print("Scheduler started successfully!")
    print("Pipeline will run every 4 hours")
    print("Press Ctrl+C to stop...")
    
    try:
        # Keep running
        while True:
            time.sleep(60)
            status = scheduler.get_status()
            if status['next_run']:
                next_run = datetime.fromisoformat(status['next_run'])
                time_until = next_run - datetime.now()
                print(f"Next run in: {time_until}")
    except KeyboardInterrupt:
        print("\nStopping scheduler...")
        scheduler.stop()
        print("Scheduler stopped.")