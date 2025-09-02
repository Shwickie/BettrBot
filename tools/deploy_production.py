# tools/cloud_production_system.py
"""
Complete Cloud Production System for Bettr Bot
Handles deployment, scheduling, monitoring, and maintenance
"""

import os
import sys
import subprocess
import schedule
import time
import logging
import json
import sqlite3
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import threading
import queue
import signal

# Add project root to path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, PROJECT_ROOT)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/production.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ProductionManager:
    """Complete production management system"""
    
    def __init__(self):
        self.db_path = os.environ.get("BETTR_DB_PATH", 
                                     os.path.join(PROJECT_ROOT, "data", "betting.db"))
        self.config = self.load_config()
        self.is_running = False
        self.task_queue = queue.Queue()
        self.status_data = {}
        
        # Ensure directories exist
        os.makedirs("logs", exist_ok=True)
        os.makedirs("data", exist_ok=True)
        
    def load_config(self) -> Dict:
        """Load production configuration"""
        default_config = {
            "scheduling": {
                "full_refresh_time": "06:00",
                "odds_refresh_hours": list(range(8, 24)),  # 8 AM to 11 PM
                "model_retrain_days": 7,
                "backup_time": "02:00"
            },
            "deployment": {
                "port": int(os.environ.get("PORT", 5000)),
                "host": "0.0.0.0",
                "workers": 1,
                "timeout": 300
            },
            "monitoring": {
                "health_check_interval": 300,  # 5 minutes
                "alert_webhook": os.environ.get("ALERT_WEBHOOK"),
                "status_retention_days": 30
            },
            "database": {
                "backup_enabled": True,
                "backup_retention_days": 14,
                "cleanup_interval_hours": 24
            }
        }
        
        config_file = os.path.join(PROJECT_ROOT, "production_config.json")
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    custom_config = json.load(f)
                # Deep merge configs
                self._merge_configs(default_config, custom_config)
            except Exception as e:
                logger.warning(f"Failed to load custom config: {e}")
        
        return default_config
    
    def _merge_configs(self, base: Dict, override: Dict):
        """Deep merge configuration dictionaries"""
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._merge_configs(base[key], value)
            else:
                base[key] = value
    
    def create_deployment_files(self):
        """Create deployment files for various platforms"""
        logger.info("Creating deployment files...")
        
        # Procfile for Heroku/Railway
        procfile = f"""web: python -m dashboard.mobile_dashboard
worker: python tools/cloud_production_system.py worker
release: python tools/enhanced_daily_refresh.py full
"""
        
        # requirements.txt
        requirements = """flask>=2.0.0
pandas>=1.3.0
numpy>=1.21.0
scikit-learn>=1.0.0
xgboost>=1.5.0
requests>=2.25.0
schedule>=1.1.0
sqlalchemy>=1.4.0
werkzeug>=2.0.0
openai>=1.0.0
beautifulsoup4>=4.9.0
selenium>=4.0.0
psutil>=5.8.0
"""
        
        # Dockerfile
        dockerfile = """FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \\
    gcc g++ curl wget \\
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV FLASK_ENV=production

# Create logs directory
RUN mkdir -p /app/logs /app/data

# Expose port
EXPOSE $PORT

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \\
  CMD curl -f http://localhost:$PORT/api/health || exit 1

# Start command
CMD ["python", "tools/cloud_production_system.py", "start"]
"""
        
        # docker-compose.yml for local development
        docker_compose = """version: '3.8'
services:
  web:
    build: .
    ports:
      - "${PORT:-5000}:${PORT:-5000}"
    environment:
      - DATABASE_URL=${DATABASE_URL}
      - OPENAI_API_KEY=${OPENAI_API_KEY}
      - PORT=${PORT:-5000}
      - FLASK_ENV=production
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    restart: unless-stopped
    depends_on:
      - worker
    
  worker:
    build: .
    command: python tools/cloud_production_system.py worker
    environment:
      - DATABASE_URL=${DATABASE_URL}
      - OPENAI_API_KEY=${OPENAI_API_KEY}
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    restart: unless-stopped
"""
        
        # Render.yaml for Render deployment
        render_yaml = """services:
  - type: web
    name: bettr-bot-web
    env: python
    plan: starter
    buildCommand: pip install -r requirements.txt
    startCommand: python tools/cloud_production_system.py start
    healthCheckPath: /api/health
    envVars:
      - key: FLASK_ENV
        value: production
      - key: PORT
        generateValue: true
        
  - type: worker
    name: bettr-bot-worker
    env: python
    plan: starter
    buildCommand: pip install -r requirements.txt
    startCommand: python tools/cloud_production_system.py worker
"""
        
        # Railway config
        railway_json = """{
  "build": {
    "builder": "NIXPACKS"
  },
  "deploy": {
    "startCommand": "python tools/cloud_production_system.py start",
    "healthcheckPath": "/api/health",
    "healthcheckTimeout": 300
  }
}"""
        
        # GitHub Actions workflow
        github_workflow = """name: Deploy to Production

on:
  push:
    branches: [ main ]

jobs:
  deploy:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v3
      with:
        python-version: '3.9'
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
    
    - name: Run tests
      run: |
        python tools/enhanced_daily_refresh.py status
        python -c "import dashboard.mobile_dashboard; print('Dashboard imports OK')"
    
    - name: Deploy to Railway
      uses: railway-app/cli@v1
      with:
        command: railway up
      env:
        RAILWAY_TOKEN: ${{ secrets.RAILWAY_TOKEN }}
"""
        
        # Write all deployment files
        files_to_create = {
            "Procfile": procfile,
            "requirements.txt": requirements,
            "Dockerfile": dockerfile,
            "docker-compose.yml": docker_compose,
            "render.yaml": render_yaml,
            "railway.json": railway_json,
            ".github/workflows/deploy.yml": github_workflow
        }
        
        for filename, content in files_to_create.items():
            filepath = os.path.join(PROJECT_ROOT, filename)
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            with open(filepath, 'w') as f:
                f.write(content.strip())
            
            logger.info(f"✓ Created {filename}")
        
        # Create .env.example
        env_example = """# Environment Variables for Bettr Bot

# Database
DATABASE_URL=sqlite:///./data/betting.db
BETTR_DB_PATH=./data/betting.db

# OpenAI API (for AI chat features)
OPENAI_API_KEY=your_openai_api_key_here

# Flask Configuration
FLASK_ENV=production
PORT=5000

# Optional: Webhook for alerts
ALERT_WEBHOOK=https://hooks.slack.com/services/...

# Cloud Database Examples:
# DATABASE_URL=postgresql://user:pass@host:port/dbname
# DATABASE_URL=mysql://user:pass@host:port/dbname
"""
        
        with open(os.path.join(PROJECT_ROOT, ".env.example"), 'w') as f:
            f.write(env_example.strip())
        
        logger.info("✓ Created .env.example")
        logger.info("✓ All deployment files created successfully!")
        
        self.print_deployment_instructions()
    
    def print_deployment_instructions(self):
        """Print deployment instructions for different platforms"""
        print("\n" + "="*60)
        print("DEPLOYMENT INSTRUCTIONS")
        print("="*60)
        
        print("\n🚀 RAILWAY DEPLOYMENT:")
        print("1. Install Railway CLI: npm install -g @railway/cli")
        print("2. Login: railway login")
        print("3. Deploy: railway up")
        print("4. Add environment variables in Railway dashboard")
        
        print("\n🟣 HEROKU DEPLOYMENT:")
        print("1. Install Heroku CLI")
        print("2. heroku create your-app-name")
        print("3. heroku addons:create heroku-postgresql:hobby-dev")
        print("4. git push heroku main")
        
        print("\n🔷 RENDER DEPLOYMENT:")
        print("1. Connect GitHub repo to Render")
        print("2. Create PostgreSQL database in Render")
        print("3. Deploy web service and worker from render.yaml")
        
        print("\n🐳 DOCKER DEPLOYMENT:")
        print("1. docker-compose up -d")
        print("2. Access at http://localhost:5000")
        
        print("\n📋 NEXT STEPS:")
        print("1. Copy .env.example to .env and fill in your values")
        print("2. Set up a PostgreSQL database (recommended for production)")
        print("3. Configure environment variables in your chosen platform")
        print("4. Deploy and monitor logs for any issues")
        print("="*60)
    
    def setup_scheduling(self):
        """Setup automated task scheduling"""
        logger.info("Setting up task scheduling...")
        
        try:
            from tools.enhanced_daily_refresh import EnhancedDailyRefresh
            refresh_manager = EnhancedDailyRefresh(self.db_path)
            
            # Schedule full refresh
            schedule.every().day.at(self.config['scheduling']['full_refresh_time']).do(
                self._run_scheduled_task, "full_refresh", refresh_manager.full_data_refresh
            )
            
            # Schedule odds refresh
            for hour in self.config['scheduling']['odds_refresh_hours']:
                schedule.every().day.at(f"{hour:02d}:00").do(
                    self._run_scheduled_task, "odds_refresh", refresh_manager.hourly_odds_refresh
                )
            
            # Schedule model retraining weekly
            schedule.every(self.config['scheduling']['model_retrain_days']).days.do(
                self._run_scheduled_task, "model_retrain", refresh_manager._check_model_retraining
            )
            
            # Schedule database backup
            if self.config['database']['backup_enabled']:
                schedule.every().day.at(self.config['scheduling']['backup_time']).do(
                    self._run_scheduled_task, "database_backup", self._backup_database
                )
            
            # Schedule health monitoring
            schedule.every(self.config['monitoring']['health_check_interval']).seconds.do(
                self._monitor_system_health
            )
            
            logger.info("✓ Task scheduling configured")
            
        except Exception as e:
            logger.error(f"Scheduling setup failed: {e}")
            raise
    
    def _run_scheduled_task(self, task_name: str, task_func):
        """Run a scheduled task with monitoring and error handling"""
        start_time = datetime.now()
        logger.info(f"Starting scheduled task: {task_name}")
        
        try:
            task_func()
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"✓ Task {task_name} completed in {duration:.1f}s")
            
            self.status_data[task_name] = {
                'last_run': start_time.isoformat(),
                'status': 'success',
                'duration': duration
            }
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"✗ Task {task_name} failed after {duration:.1f}s: {e}")
            
            self.status_data[task_name] = {
                'last_run': start_time.isoformat(),
                'status': 'failed',
                'duration': duration,
                'error': str(e)
            }
            
            # Send alert for failed tasks
            self._send_alert(f"Task {task_name} failed", str(e))
    
    def _monitor_system_health(self):
        """Monitor system health and performance"""
        try:
            # Check database connectivity
            db_healthy = self._check_database_health()
            
            # Check recent data updates
            recent_data = self._check_recent_data()
            
            # Check disk space
            disk_usage = self._check_disk_space()
            
            # Update status
            health_status = {
                'timestamp': datetime.now().isoformat(),
                'database': 'healthy' if db_healthy else 'unhealthy',
                'recent_data': recent_data,
                'disk_usage': disk_usage,
                'status': 'healthy' if db_healthy and disk_usage < 90 else 'warning'
            }
            
            self.status_data['system_health'] = health_status
            
            # Alert on issues
            if not db_healthy:
                self._send_alert("Database Health Alert", "Database connectivity issues detected")
            
            if disk_usage > 90:
                self._send_alert("Disk Space Alert", f"Disk usage at {disk_usage}%")
                
        except Exception as e:
            logger.error(f"Health monitoring failed: {e}")
    
    def _check_database_health(self) -> bool:
        """Check if database is accessible and responsive"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            conn.execute("SELECT 1").fetchone()
            conn.close()
            return True
        except Exception as e:
            logger.warning(f"Database health check failed: {e}")
            return False
    
    def _check_recent_data(self) -> Dict:
        """Check if recent data updates are happening"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Check recent odds
            recent_odds = conn.execute("""
                SELECT COUNT(*) FROM odds 
                WHERE timestamp >= datetime('now', '-6 hours')
            """).fetchone()[0]
            
            # Check recent injuries
            recent_injuries = conn.execute("""
                SELECT COUNT(*) FROM nfl_injuries_tracking 
                WHERE last_updated >= datetime('now', '-24 hours')
            """).fetchone()[0]
            
            # Check recent games
            recent_games = conn.execute("""
                SELECT COUNT(*) FROM games 
                WHERE game_date >= date('now', '-7 days')
                  AND home_score IS NOT NULL
            """).fetchone()[0]
            
            conn.close()
            
            return {
                'recent_odds': recent_odds,
                'recent_injuries': recent_injuries,
                'recent_games': recent_games,
                'status': 'good' if recent_odds > 10 else 'stale'
            }
            
        except Exception as e:
            logger.warning(f"Recent data check failed: {e}")
            return {'status': 'unknown', 'error': str(e)}
    
    def _check_disk_space(self) -> float:
        """Check disk space usage percentage"""
        try:
            import shutil
            total, used, free = shutil.disk_usage(PROJECT_ROOT)
            usage_percent = (used / total) * 100
            return usage_percent
        except Exception:
            return 50.0  # Default assumption
    
    def _backup_database(self):
        """Create database backup"""
        logger.info("Creating database backup...")
        
        try:
            backup_dir = os.path.join(PROJECT_ROOT, "backups")
            os.makedirs(backup_dir, exist_ok=True)
            
            # Create backup filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = os.path.join(backup_dir, f"betting_backup_{timestamp}.db")
            
            # Create backup using SQLite backup API
            source = sqlite3.connect(self.db_path)
            backup = sqlite3.connect(backup_file)
            source.backup(backup)
            backup.close()
            source.close()
            
            # Compress backup
            import gzip
            with open(backup_file, 'rb') as f_in:
                with gzip.open(f"{backup_file}.gz", 'wb') as f_out:
                    f_out.writelines(f_in)
            
            # Remove uncompressed backup
            os.remove(backup_file)
            
            # Cleanup old backups
            self._cleanup_old_backups(backup_dir)
            
            logger.info(f"✓ Database backup created: {backup_file}.gz")
            
        except Exception as e:
            logger.error(f"Database backup failed: {e}")
            raise
    
    def _cleanup_old_backups(self, backup_dir: str):
        """Clean up old backup files"""
        try:
            retention_days = self.config['database']['backup_retention_days']
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            
            removed_count = 0
            for filename in os.listdir(backup_dir):
                if filename.startswith("betting_backup_") and filename.endswith(".gz"):
                    file_path = os.path.join(backup_dir, filename)
                    file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                    
                    if file_time < cutoff_date:
                        os.remove(file_path)
                        removed_count += 1
            
            if removed_count > 0:
                logger.info(f"✓ Cleaned up {removed_count} old backups")
                
        except Exception as e:
            logger.warning(f"Backup cleanup failed: {e}")
    
    def _send_alert(self, title: str, message: str):
        """Send alert via webhook or logging"""
        alert_data = {
            'timestamp': datetime.now().isoformat(),
            'title': title,
            'message': message,
            'hostname': os.environ.get('DYNO', 'local'),
            'service': 'bettr-bot'
        }
        
        # Log alert
        logger.warning(f"ALERT: {title} - {message}")
        
        # Send webhook if configured
        webhook_url = self.config['monitoring']['alert_webhook']
        if webhook_url:
            try:
                requests.post(webhook_url, json=alert_data, timeout=10)
            except Exception as e:
                logger.warning(f"Failed to send webhook alert: {e}")
    
    def start_web_server(self):
        """Start the Flask web server"""
        logger.info("Starting Bettr Bot web server...")
        
        try:
            # Set environment variables
            os.environ["FLASK_ENV"] = "production"
            os.environ["BETTR_DB_PATH"] = self.db_path
            
            # Import and configure Flask app
            sys.path.insert(0, PROJECT_ROOT)
            from dashboard.mobile_dashboard import app
            
            app.config.update({
                'DEBUG': False,
                'TESTING': False,
                'SECRET_KEY': os.environ.get('SECRET_KEY', 'bettr-bot-production-key')
            })
            
            # Start server
            port = self.config['deployment']['port']
            host = self.config['deployment']['host']
            
            logger.info(f"Server starting on {host}:{port}")
            app.run(host=host, port=port, debug=False)
            
        except Exception as e:
            logger.error(f"Web server startup failed: {e}")
            raise
    
    def start_worker(self):
        """Start the background worker process"""
        logger.info("Starting Bettr Bot worker...")
        
        self.is_running = True
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._shutdown_handler)
        signal.signal(signal.SIGINT, self._shutdown_handler)
        
        try:
            # Setup scheduling
            self.setup_scheduling()
            
            # Worker main loop
            logger.info("Worker started successfully. Running scheduled tasks...")
            
            while self.is_running:
                try:
                    schedule.run_pending()
                    time.sleep(30)  # Check every 30 seconds
                    
                except Exception as e:
                    logger.error(f"Worker loop error: {e}")
                    time.sleep(60)  # Wait longer on error
                    
        except KeyboardInterrupt:
            logger.info("Worker interrupted by user")
        except Exception as e:
            logger.error(f"Worker failed: {e}")
            raise
        finally:
            self.is_running = False
            logger.info("Worker shut down")
    
    def _shutdown_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received shutdown signal {signum}")
        self.is_running = False
    
    def get_status_report(self) -> Dict:
        """Get comprehensive status report"""
        try:
            # Database stats
            conn = sqlite3.connect(self.db_path)
            
            db_stats = {}
            try:
                db_stats = {
                    'total_games': conn.execute("SELECT COUNT(*) FROM games").fetchone()[0],
                    'recent_odds': conn.execute("""
                        SELECT COUNT(*) FROM odds 
                        WHERE timestamp >= datetime('now', '-24 hours')
                    """).fetchone()[0],
                    'active_injuries': conn.execute("""
                        SELECT COUNT(*) FROM nfl_injuries_tracking 
                        WHERE is_active = 1
                    """).fetchone()[0],
                    'teams_tracked': conn.execute("""
                        SELECT COUNT(DISTINCT team) FROM team_season_summary 
                        WHERE season = 2025
                    """).fetchone()[0]
                }
            finally:
                conn.close()
            
            # File system stats
            model_exists = os.path.exists(os.path.join(PROJECT_ROOT, "models", "betting_model.pkl"))
            
            status_report = {
                'timestamp': datetime.now().isoformat(),
                'service': 'bettr-bot',
                'version': '2.0',
                'environment': os.environ.get('FLASK_ENV', 'development'),
                'database': {
                    'path': self.db_path,
                    'accessible': self._check_database_health(),
                    'stats': db_stats
                },
                'model': {
                    'exists': model_exists,
                    'path': os.path.join(PROJECT_ROOT, "models", "betting_model.pkl")
                },
                'tasks': self.status_data,
                'config': {
                    'scheduling': self.config['scheduling'],
                    'monitoring_enabled': bool(self.config['monitoring']['alert_webhook'])
                }
            }
            
            return status_report
            
        except Exception as e:
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'status': 'error'
            }
    
    def run_initial_setup(self):
        """Run initial setup for production deployment"""
        logger.info("Running initial production setup...")
        
        try:
            # Ensure database exists and is set up
            if not os.path.exists(self.db_path):
                logger.info("Database not found, running initial setup...")
                self._setup_initial_database()
            
            # Run a basic data refresh to populate essentials
            logger.info("Running initial data refresh...")
            try:
                from tools.enhanced_daily_refresh import EnhancedDailyRefresh
                refresh_manager = EnhancedDailyRefresh(self.db_path)
                
                # Run essential tasks only
                essential_tasks = [
                    refresh_manager._update_team_stats,
                    refresh_manager.hourly_odds_refresh
                ]
                
                for task in essential_tasks:
                    try:
                        task()
                    except Exception as e:
                        logger.warning(f"Initial setup task failed: {e}")
                        # Continue with other tasks
                        
            except Exception as e:
                logger.warning(f"Initial data refresh failed: {e}")
            
            logger.info("✓ Initial production setup completed")
            
        except Exception as e:
            logger.error(f"Initial setup failed: {e}")
            # Don't raise - allow service to start even if setup is incomplete
    
    def _setup_initial_database(self):
        """Set up initial database structure"""
        try:
            from data.setup_db import main as setup_database
            setup_database()
            logger.info("✓ Database structure created")
        except Exception as e:
            logger.error(f"Database setup failed: {e}")
            raise

def main():
    """Main CLI interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Bettr Bot Production System")
    parser.add_argument("command", choices=[
        "start", "worker", "deploy-files", "setup", "status", "backup"
    ], help="Command to run")
    
    args = parser.parse_args()
    
    # Initialize manager
    manager = ProductionManager()
    
    try:
        if args.command == "deploy-files":
            manager.create_deployment_files()
            
        elif args.command == "setup":
            manager.run_initial_setup()
            
        elif args.command == "start":
            # Run initial setup if needed
            manager.run_initial_setup()
            # Start web server
            manager.start_web_server()
            
        elif args.command == "worker":
            # Start background worker
            manager.start_worker()
            
        elif args.command == "status":
            # Print status report
            status = manager.get_status_report()
            print(json.dumps(status, indent=2))
            
        elif args.command == "backup":
            # Create manual backup
            manager._backup_database()
            
    except Exception as e:
        logger.error(f"Command failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Ensure logs directory exists
    os.makedirs("logs", exist_ok=True)
    main()