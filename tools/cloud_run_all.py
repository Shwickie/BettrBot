# tools/cloud_run_all.py
"""
Cloud-based run_all system for Bettr Bot
Supports PostgreSQL, MySQL, and cloud SQLite databases
Designed for Heroku, Railway, Render, or other cloud platforms
"""

import subprocess
import sys
import time
import argparse
import os
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging

# Database imports - support multiple cloud providers
try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.exc import SQLAlchemyError
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False
    print("Warning: SQLAlchemy not available")

try:
    import psycopg2
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

try:
    import pymysql
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CloudDatabaseManager:
    """Manages cloud database connections and operations"""
    
    def __init__(self):
        self.engine = None
        self.db_type = None
        self.connection_string = self._get_database_url()
        
    def _get_database_url(self) -> str:
        """Get database URL from environment variables"""
        # Try common cloud database environment variables
        db_urls = [
            os.getenv("DATABASE_URL"),           # Heroku standard
            os.getenv("POSTGRES_URL"),           # Railway
            os.getenv("MYSQL_URL"),              # PlanetScale
            os.getenv("BETTR_DB_URL"),           # Custom
            os.getenv("SUPABASE_DB_URL"),        # Supabase
            os.getenv("NEON_DATABASE_URL"),      # Neon
        ]
        
        for url in db_urls:
            if url:
                return url
        
        # Fallback to local SQLite for development
        return "sqlite:///./data/betting.db"
    
    def connect(self):
        """Establish database connection"""
        if not SQLALCHEMY_AVAILABLE:
            raise Exception("SQLAlchemy is required for database operations")
        
        try:
            self.engine = create_engine(
                self.connection_string,
                pool_pre_ping=True,
                pool_recycle=300,
                connect_args={"connect_timeout": 30}
            )
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            # Determine database type
            if "postgresql" in self.connection_string or "postgres" in self.connection_string:
                self.db_type = "postgresql"
            elif "mysql" in self.connection_string:
                self.db_type = "mysql"
            else:
                self.db_type = "sqlite"
                
            logger.info(f"Connected to {self.db_type} database")
            return True
            
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def ensure_status_table(self):
        """Create system_status table if it doesn't exist"""
        try:
            with self.engine.begin() as conn:
                if self.db_type == "postgresql":
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS system_status (
                            id SERIAL PRIMARY KEY,
                            task VARCHAR(255) NOT NULL,
                            started_at TIMESTAMP NOT NULL,
                            finished_at TIMESTAMP,
                            status VARCHAR(50),
                            message TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """))
                elif self.db_type == "mysql":
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS system_status (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            task VARCHAR(255) NOT NULL,
                            started_at DATETIME NOT NULL,
                            finished_at DATETIME,
                            status VARCHAR(50),
                            message TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """))
                else:  # SQLite
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS system_status (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            task TEXT NOT NULL,
                            started_at TEXT NOT NULL,
                            finished_at TEXT,
                            status TEXT,
                            message TEXT,
                            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                        )
                    """))
            logger.info("System status table ready")
        except Exception as e:
            logger.error(f"Failed to create status table: {e}")
            raise
    
    def record_status(self, task: str, started_at: str, finished_at: str, status: str, message: str):
        """Record task execution status"""
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO system_status (task, started_at, finished_at, status, message)
                    VALUES (:task, :started_at, :finished_at, :status, :message)
                """), {
                    "task": task,
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "status": status,
                    "message": message[:2000]  # Truncate long messages
                })
        except Exception as e:
            logger.error(f"Failed to record status: {e}")

class CloudTaskRunner:
    """Runs data pipeline tasks in cloud environment"""
    
    def __init__(self, db_manager: CloudDatabaseManager):
        self.db_manager = db_manager
        self.project_root = self._find_project_root()
        self.tasks = self._define_tasks()
        
    def _find_project_root(self) -> str:
        """Find project root directory"""
        current = os.path.dirname(os.path.abspath(__file__))
        
        # Look for key files that indicate project root
        indicators = ["requirements.txt", "setup.py", "dashboard", "model", "data"]
        
        for _ in range(5):  # Don't search too far up
            if any(os.path.exists(os.path.join(current, indicator)) for indicator in indicators):
                return current
            current = os.path.dirname(current)
        
        # Fallback to current directory
        return os.getcwd()
    
    def _define_tasks(self) -> List[Tuple[str, List[str]]]:
        """Define all data pipeline tasks"""
        python_exe = sys.executable
        root = self.project_root
        
        return [
            # Core database setup
            ("setup_database", [python_exe, "-c", """
import os, sys
sys.path.append('""" + root + """')
from data.setup_db import main
main()
"""]),
            
            # Historical data import
            ("import_historical_games", [python_exe, "-c", """
import os, sys
sys.path.append('""" + root + """')
from stats.insert_historical_games import main
main()
"""]),
            
            # Game scheduling and times
            ("fill_game_times", [python_exe, "-c", """
import os, sys
sys.path.append('""" + root + """')
from stats.fill_game_times import main
main()
"""]),
            
            # Team statistics
            ("import_team_stats", [python_exe, "-c", """
import os, sys
sys.path.append('""" + root + """')
from stats.import_team_stats import main
main()
"""]),
            
            # Player statistics
            ("import_player_stats", [python_exe, "-c", """
import os, sys
sys.path.append('""" + root + """')
from stats.import_player_stats import main
main()
"""]),
            
            # Live player data
            ("fetch_live_players", [python_exe, "-c", """
import os, sys
sys.path.append('""" + root + """')
from stats.fetch_live_player_stats import main
main()
"""]),
            
            # Player team mappings
            ("map_player_teams", [python_exe, "-c", """
import os, sys
sys.path.append('""" + root + """')
from stats.map_player_teams import main
main()
"""]),
            
            # Team season summaries
            ("team_season_summary", [python_exe, "-c", """
import os, sys
sys.path.append('""" + root + """')
from stats.team_season_summary import main
main()
"""]),
            
            # Power rankings and matchups
            ("matchup_power_summary", [python_exe, "-c", """
import os, sys
sys.path.append('""" + root + """')
from stats.matchup_power_summary import main
main()
"""]),
            
            # Player vs defense analysis
            ("player_vs_defense", [python_exe, "-c", """
import os, sys
sys.path.append('""" + root + """')
from stats.player_vs_defense_summary import main
main()
"""]),
            
            # Position vs defense analysis
            ("pos_vs_defense", [python_exe, "-c", """
import os, sys
sys.path.append('""" + root + """')
from stats.pos_vs_def_summary import main
main()
"""]),
            
            # Player form trends
            ("player_form_trends", [python_exe, "-c", """
import os, sys
sys.path.append('""" + root + """')
from stats.player_form_trends import main
main()
"""]),
            
            # Injury impact modeling
            ("injury_impact_model", [python_exe, "-c", """
import os, sys
sys.path.append('""" + root + """')
from stats.injury_impact_model import main
main()
"""]),
            
            # Import current rosters
            ("import_2025_roster", [python_exe, "-c", """
import os, sys
sys.path.append('""" + root + """')
from stats.import_2025_roster import main
main()
"""]),
            
            # Injury data collection
            ("injury_database", [python_exe, "-c", """
import os, sys
sys.path.append('""" + root + """')
from data.injury_database import main
main()
"""]),
            
            # Odds collection
            ("fetch_odds", [python_exe, "-c", """
import os, sys
sys.path.append('""" + root + """')
from odds.get_odds_fixed import main
main()
"""]),
            
            # Score updates
            ("update_scores", [python_exe, "-c", """
import os, sys
sys.path.append('""" + root + """')
from stats.update_scores import main
main()
"""]),
            
            # Validation checks
            ("check_scores", [python_exe, "-c", """
import os, sys
sys.path.append('""" + root + """')
from stats.check_scores import main
main()
"""]),
            
            # Model training (optional)
            ("train_model", [python_exe, "-c", """
import os, sys
sys.path.append('""" + root + """')
from model.train_betting_model import BettingModelTrainer
trainer = BettingModelTrainer()
df = trainer.build_training_dataset()
if len(df) > 100:
    trainer.train_models(df)
    trainer.save_model()
    print('Model training completed')
else:
    print('Insufficient data for training')
"""]),
        ]
    
    def run_task(self, name: str, cmd: List[str], dry_run: bool = False) -> bool:
        """Execute a single task with error handling and logging"""
        started_at = datetime.utcnow().isoformat()
        logger.info(f"Starting task: {name}")
        
        if dry_run:
            logger.info(f"[DRY RUN] Would execute: {' '.join(cmd[:3])}...")
            self.db_manager.record_status(name, started_at, datetime.utcnow().isoformat(), "SKIPPED", "dry_run")
            return True
        
        try:
            # Set up environment
            env = os.environ.copy()
            env.update({
                "PYTHONIOENCODING": "utf-8",
                "PYTHONPATH": self.project_root,
                "BETTR_DB_URL": self.db_manager.connection_string,
            })
            
            # Execute task with timeout
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                env=env,
                timeout=1800,  # 30 minute timeout
                cwd=self.project_root
            )
            
            finished_at = datetime.utcnow().isoformat()
            
            # Process output
            stdout = proc.stdout or ""
            stderr = proc.stderr or ""
            combined_output = f"STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
            
            if proc.returncode == 0:
                logger.info(f"Task {name} completed successfully")
                if stdout:
                    logger.debug(f"Output: {stdout[-500:]}")  # Log last 500 chars
                
                self.db_manager.record_status(name, started_at, finished_at, "SUCCESS", combined_output)
                return True
            else:
                logger.error(f"Task {name} failed with return code {proc.returncode}")
                if stderr:
                    logger.error(f"Error: {stderr[-1000:]}")  # Log last 1000 chars
                
                self.db_manager.record_status(name, started_at, finished_at, "FAILED", combined_output)
                return False
                
        except subprocess.TimeoutExpired:
            finished_at = datetime.utcnow().isoformat()
            logger.error(f"Task {name} timed out after 30 minutes")
            self.db_manager.record_status(name, started_at, finished_at, "TIMEOUT", "Task exceeded 30 minute timeout")
            return False
            
        except Exception as e:
            finished_at = datetime.utcnow().isoformat()
            logger.error(f"Task {name} failed with exception: {e}")
            self.db_manager.record_status(name, started_at, finished_at, "EXCEPTION", str(e))
            return False
    
    def run_pipeline(self, 
                    only_task: Optional[str] = None,
                    from_task: Optional[str] = None,
                    exclude_tasks: Optional[List[str]] = None,
                    dry_run: bool = False) -> Dict[str, any]:
        """Run the complete data pipeline or specified tasks"""
        
        # Filter tasks based on parameters
        tasks_to_run = self.tasks.copy()
        
        if only_task:
            tasks_to_run = [t for t in self.tasks if t[0] == only_task]
            if not tasks_to_run:
                raise ValueError(f"Task '{only_task}' not found")
        
        if from_task:
            start_found = False
            filtered_tasks = []
            for task in self.tasks:
                if task[0] == from_task:
                    start_found = True
                if start_found:
                    filtered_tasks.append(task)
            
            if not start_found:
                raise ValueError(f"Start task '{from_task}' not found")
            tasks_to_run = filtered_tasks
        
        if exclude_tasks:
            tasks_to_run = [t for t in tasks_to_run if t[0] not in exclude_tasks]
        
        # Execute tasks
        results = {
            "started_at": datetime.utcnow().isoformat(),
            "total_tasks": len(tasks_to_run),
            "successful": 0,
            "failed": 0,
            "task_results": []
        }
        
        logger.info(f"Starting pipeline with {len(tasks_to_run)} tasks")
        
        for task_name, task_cmd in tasks_to_run:
            logger.info(f"Executing task {results['successful'] + results['failed'] + 1}/{len(tasks_to_run)}: {task_name}")
            
            success = self.run_task(task_name, task_cmd, dry_run)
            
            if success:
                results["successful"] += 1
            else:
                results["failed"] += 1
            
            results["task_results"].append({
                "name": task_name,
                "success": success,
                "timestamp": datetime.utcnow().isoformat()
            })
            
            # Small delay between tasks to prevent resource conflicts
            time.sleep(2)
        
        results["finished_at"] = datetime.utcnow().isoformat()
        results["success_rate"] = results["successful"] / results["total_tasks"] if results["total_tasks"] > 0 else 0
        
        logger.info(f"Pipeline completed: {results['successful']}/{results['total_tasks']} tasks successful")
        
        return results

def create_cloud_deployment_files():
    """Create deployment configuration files for various cloud platforms"""
    
    # Heroku Procfile
    procfile_content = """web: python -m dashboard.mobile_dashboard
worker: python tools/cloud_run_all.py --schedule
release: python tools/cloud_run_all.py --only setup_database
"""
    
    # Railway deployment
    railway_config = {
        "build": {
            "builder": "NIXPACKS"
        },
        "deploy": {
            "startCommand": "python -m dashboard.mobile_dashboard",
            "healthcheckPath": "/api/health"
        }
    }
    
    # Render deployment
    render_config = {
        "services": [
            {
                "type": "web",
                "name": "bettr-bot-web",
                "env": "python",
                "buildCommand": "pip install -r requirements.txt",
                "startCommand": "python -m dashboard.mobile_dashboard",
                "healthCheckPath": "/api/health"
            },
            {
                "type": "worker",
                "name": "bettr-bot-worker", 
                "env": "python",
                "buildCommand": "pip install -r requirements.txt",
                "startCommand": "python tools/cloud_run_all.py --schedule"
            }
        ]
    }
    
    # Docker configuration
    dockerfile_content = """FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \\
    gcc \\
    g++ \\
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Expose port
EXPOSE 5000

# Default command (can be overridden)
CMD ["python", "-m", "dashboard.mobile_dashboard"]
"""
    
    # Docker Compose for local development with cloud database
    docker_compose_content = """version: '3.8'
services:
  web:
    build: .
    ports:
      - "5000:5000"
    environment:
      - DATABASE_URL=${DATABASE_URL}
      - OPENAI_API_KEY=${OPENAI_API_KEY}
      - FLASK_ENV=production
    volumes:
      - ./logs:/app/logs
    restart: unless-stopped
    
  worker:
    build: .
    command: python tools/cloud_run_all.py --schedule
    environment:
      - DATABASE_URL=${DATABASE_URL}
      - OPENAI_API_KEY=${OPENAI_API_KEY}
    volumes:
      - ./logs:/app/logs
    restart: unless-stopped
    depends_on:
      - web
"""
    
    # Write files
    files = {
        "Procfile": procfile_content,
        "railway.json": json.dumps(railway_config, indent=2),
        "render.yaml": json.dumps(render_config, indent=2),
        "Dockerfile": dockerfile_content,
        "docker-compose.yml": docker_compose_content
    }
    
    for filename, content in files.items():
        with open(filename, 'w') as f:
            f.write(content)
        logger.info(f"Created {filename}")

def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(description="Cloud-based Bettr Bot Data Pipeline")
    parser.add_argument("--only", help="Run only the specified task")
    parser.add_argument("--from", dest="from_task", help="Start pipeline from this task")
    parser.add_argument("--exclude", nargs="+", help="Exclude these tasks from execution")
    parser.add_argument("--dry-run", action="store_true", help="Show what would run without executing")
    parser.add_argument("--create-deployment", action="store_true", help="Create cloud deployment files")
    parser.add_argument("--schedule", action="store_true", help="Run scheduled pipeline (for worker processes)")
    
    args = parser.parse_args()
    
    if args.create_deployment:
        create_cloud_deployment_files()
        return
    
    # Initialize database manager
    db_manager = CloudDatabaseManager()
    
    if not db_manager.connect():
        logger.error("Failed to connect to database")
        sys.exit(1)
    
    try:
        db_manager.ensure_status_table()
    except Exception as e:
        logger.error(f"Failed to setup database: {e}")
        sys.exit(1)
    
    # Initialize task runner
    task_runner = CloudTaskRunner(db_manager)
    
    try:
        if args.schedule:
            # This would integrate with a scheduler like APScheduler for production
            logger.info("Running scheduled pipeline...")
            
        # Run pipeline
        results = task_runner.run_pipeline(
            only_task=args.only,
            from_task=args.from_task,
            exclude_tasks=args.exclude,
            dry_run=args.dry_run
        )
        
        # Print summary
        logger.info("=" * 50)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Total tasks: {results['total_tasks']}")
        logger.info(f"Successful: {results['successful']}")
        logger.info(f"Failed: {results['failed']}")
        logger.info(f"Success rate: {results['success_rate']:.1%}")
        
        if results["failed"] > 0:
            logger.warning("Some tasks failed. Check system_status table for details.")
            sys.exit(1)
        else:
            logger.info("All tasks completed successfully!")
            
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()