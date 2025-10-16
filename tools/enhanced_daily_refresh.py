# tools/enhanced_daily_refresh.py
"""
Enhanced daily data refresh system for Bettr Bot
Handles player trades, roster updates, model retraining, and data pipeline
"""

import os
import sys
import sqlite3
import logging
import schedule
import time
import subprocess
import json
import pickle
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import pandas as pd

# Add project root to path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, PROJECT_ROOT)

# Configure logging with better formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/daily_refresh.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EnhancedDailyRefresh:
    def __init__(self, db_path: str = None):
        self.db_path = db_path or os.environ.get("BETTR_DB_PATH", 
                                                 r"E:\Bettr Bot\betting-bot\data\betting.db")
        self.models_path = os.path.join(PROJECT_ROOT, "models")
        self.status_file = os.path.join(PROJECT_ROOT, 'data', 'refresh_status.json')
        
        # Ensure directories exist
        os.makedirs("logs", exist_ok=True)
        os.makedirs(os.path.dirname(self.status_file), exist_ok=True)
        os.makedirs(self.models_path, exist_ok=True)
    
    def full_data_refresh(self):
        """Complete daily data refresh pipeline with enhanced logging"""
        start_time = datetime.now()
        logger.info("=" * 60)
        logger.info("STARTING FULL DAILY DATA REFRESH")
        logger.info("=" * 60)
        
        try:
            refresh_tasks = [
                ("Player Rosters & Trades", self._update_player_rosters),
                ("Injury Reports", self._update_injury_reports),
                ("Team Stats & Power Rankings", self._update_team_stats),
                ("Game Scores & Results", self._update_game_scores),
                ("Historical Player Stats", self._update_player_stats),
                ("Season Summaries", self._update_season_summaries),
                ("Data Validation", self._validate_data_integrity),
                ("Old Data Cleanup", self._cleanup_old_data),
                ("Model Assessment", self._check_model_retraining),
                ("Cache Refresh", self._refresh_caches)
            ]
            
            completed_tasks = 0
            failed_tasks = []
            
            for task_name, task_func in refresh_tasks:
                logger.info(f"\n[{completed_tasks + 1}/{len(refresh_tasks)}] Starting: {task_name}")
                task_start = datetime.now()
                
                try:
                    task_func()
                    duration = (datetime.now() - task_start).total_seconds()
                    logger.info(f"✓ {task_name} completed in {duration:.1f}s")
                    completed_tasks += 1
                    
                except Exception as e:
                    duration = (datetime.now() - task_start).total_seconds()
                    logger.error(f"✗ {task_name} failed after {duration:.1f}s: {e}")
                    failed_tasks.append((task_name, str(e)))
                    # Continue with other tasks even if one fails
            
            total_duration = (datetime.now() - start_time).total_seconds()
            
            logger.info("=" * 60)
            logger.info("FULL REFRESH SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Total Duration: {total_duration:.1f}s")
            logger.info(f"Completed Tasks: {completed_tasks}/{len(refresh_tasks)}")
            logger.info(f"Failed Tasks: {len(failed_tasks)}")
            
            if failed_tasks:
                logger.warning("Failed tasks:")
                for task, error in failed_tasks:
                    logger.warning(f"  - {task}: {error}")
            
            status = "partial_success" if failed_tasks else "success"
            message = f"Refresh completed. {completed_tasks}/{len(refresh_tasks)} tasks successful"
            
            self._update_status(status, message, {
                'completed_tasks': completed_tasks,
                'failed_tasks': len(failed_tasks),
                'duration': total_duration,
                'failures': failed_tasks
            })
            
            if len(failed_tasks) > len(refresh_tasks) // 2:
                raise Exception(f"Too many tasks failed: {len(failed_tasks)}/{len(refresh_tasks)}")
                
            logger.info("Full data refresh completed successfully")
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Full data refresh failed after {duration:.1f}s: {e}")
            self._update_status("error", str(e), {'duration': duration})
            raise
    
    def _update_player_rosters(self):
        """Update player rosters and handle team changes with trade detection"""
        logger.info("Updating player rosters and detecting trades...")
        
        try:
            # Run roster update scripts
            scripts_to_run = [
                ("Import 2025 Roster", "stats.import_2025_roster"),
                ("Map Player Teams", "stats.map_player_teams"), 
                ("Fix Team Mappings", "data.team_mapping_fix")
            ]
            
            for script_name, module_name in scripts_to_run:
                try:
                    logger.info(f"Running {script_name}...")
                    self._run_python_script(module_name)
                    logger.info(f"✓ {script_name} completed")
                except Exception as e:
                    logger.warning(f"✗ {script_name} failed: {e}")
                    # Continue with other scripts
            
            # Detect and process player trades
            trades_detected = self._detect_player_trades()
            if trades_detected:
                logger.info(f"Detected {trades_detected} potential trades")
            
            # Update injury tracking with new team assignments
            self._update_injury_team_mappings()
            
        except Exception as e:
            logger.error(f"Player roster update failed: {e}")
            raise
    
    def _detect_player_trades(self) -> int:
        """Advanced trade detection with validation"""
        conn = sqlite3.connect(self.db_path)
        trades_count = 0
        
        try:
            # Find players with recent team changes
            trade_detection_query = """
            WITH recent_stats AS (
                SELECT 
                    player_name, 
                    team, 
                    COUNT(*) as game_count,
                    MAX(game_date) as last_game,
                    MIN(game_date) as first_game
                FROM player_game_stats 
                WHERE game_date >= date('now', '-45 days')
                  AND team IS NOT NULL
                GROUP BY player_name, team
            ),
            roster_current AS (
                SELECT 
                    full_name, 
                    team_abbr as current_team,
                    position
                FROM player_team_current
                WHERE team_abbr IS NOT NULL
            ),
            potential_trades AS (
                SELECT 
                    r.full_name,
                    r.current_team,
                    r.position,
                    s.team as stats_team,
                    s.last_game,
                    s.game_count,
                    s.first_game
                FROM roster_current r
                JOIN recent_stats s ON LOWER(TRIM(r.full_name)) = LOWER(TRIM(s.player_name))
                WHERE r.current_team != s.team 
                  AND s.game_count >= 2  -- Multiple games with old team
                  AND s.last_game >= date('now', '-30 days')  -- Recent activity
            )
            SELECT * FROM potential_trades
            ORDER BY last_game DESC
            """
            
            potential_trades = conn.execute(trade_detection_query).fetchall()
            
            if potential_trades:
                logger.info(f"Found {len(potential_trades)} potential player trades:")
                
                for trade in potential_trades:
                    player, new_team, pos, old_team, last_game, games, first_game = trade
                    logger.info(f"  📈 {player} ({pos}): {old_team} → {new_team}")
                    logger.info(f"     Last game with {old_team}: {last_game} ({games} games)")
                    
                    # Update injury tracking
                    update_count = conn.execute("""
                        UPDATE nfl_injuries_tracking 
                        SET team = ?, 
                            last_updated = datetime('now'),
                            notes = COALESCE(notes, '') || ' [Trade detected: ' || ? || ' to ' || ? || ']'
                        WHERE LOWER(TRIM(player_name)) = LOWER(TRIM(?))
                          AND team = ?
                    """, (new_team, old_team, new_team, player, old_team))
                    
                    if update_count.rowcount > 0:
                        trades_count += 1
                        logger.info(f"     ✓ Updated injury tracking for {player}")
                
                if trades_count > 0:
                    conn.commit()
                    logger.info(f"✓ Applied {trades_count} trade updates to injury tracking")
            
            return trades_count
            
        except Exception as e:
            logger.error(f"Trade detection failed: {e}")
            return 0
        finally:
            conn.close()
    
    def _update_injury_team_mappings(self):
        """Update injury tracking with correct team mappings"""
        conn = sqlite3.connect(self.db_path)
        try:
            # Update injury records with correct current teams
            update_query = """
            UPDATE nfl_injuries_tracking 
            SET team = (
                SELECT team_abbr 
                FROM player_team_current ptc 
                WHERE LOWER(TRIM(ptc.full_name)) = LOWER(TRIM(nfl_injuries_tracking.player_name))
                LIMIT 1
            ),
            last_updated = datetime('now')
            WHERE EXISTS (
                SELECT 1 FROM player_team_current ptc 
                WHERE LOWER(TRIM(ptc.full_name)) = LOWER(TRIM(nfl_injuries_tracking.player_name))
                  AND ptc.team_abbr != nfl_injuries_tracking.team
            )
            """
            
            updates = conn.execute(update_query)
            if updates.rowcount > 0:
                conn.commit()
                logger.info(f"✓ Updated {updates.rowcount} injury team mappings")
            
        except Exception as e:
            logger.warning(f"Injury team mapping update failed: {e}")
        finally:
            conn.close()
    
    def _update_injury_reports(self):
        """Refresh injury reports with enhanced error handling"""
        logger.info("Updating injury reports...")
        
        try:
            self._run_python_script("data.injury_database")
            
            # Validate injury data after update
            conn = sqlite3.connect(self.db_path)
            try:
                # Check for recent injury updates
                recent_injuries = conn.execute("""
                    SELECT COUNT(*) as count, MAX(last_updated) as last_update
                    FROM nfl_injuries_tracking 
                    WHERE last_updated >= date('now', '-2 days')
                """).fetchone()
                
                if recent_injuries and recent_injuries[0] > 0:
                    logger.info(f"✓ {recent_injuries[0]} injuries updated recently (last: {recent_injuries[1]})")
                else:
                    logger.warning("No recent injury updates found")
                    
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"Injury update failed: {e}")
            raise
    
    def _update_team_stats(self):
        """Update team statistics and power rankings"""
        logger.info("Updating team stats and power rankings...")
        
        try:
            scripts = [
                "stats.import_team_stats",
                "stats.team_season_summary", 
                "stats.matchup_power_summary"
            ]
            
            for script in scripts:
                self._run_python_script(script)
                
        except Exception as e:
            logger.error(f"Team stats update failed: {e}")
            raise
    
    def _update_game_scores(self):
        """Update game scores and results"""
        logger.info("Updating game scores and results...")
        
        try:
            scripts = [
                "stats.update_scores",
                "stats.check_scores"
            ]
            
            for script in scripts:
                self._run_python_script(script)
                
        except Exception as e:
            logger.error(f"Game scores update failed: {e}")
            raise
    
    def _update_player_stats(self):
        """Update player statistics"""
        logger.info("Updating player statistics...")
        
        try:
            scripts = [
                "stats.import_player_stats",
                "stats.fetch_live_player_stats"
            ]
            
            for script in scripts:
                try:
                    self._run_python_script(script)
                except Exception as e:
                    logger.warning(f"Player stats script failed: {e}")
                    # Continue with other scripts
                    
        except Exception as e:
            logger.error(f"Player stats update failed: {e}")
            # Don't raise - player stats are less critical
    
    def _update_season_summaries(self):
        """Update season summaries and derived statistics"""
        logger.info("Updating season summaries...")
        
        try:
            scripts = [
                "stats.player_vs_defense_summary",
                "stats.pos_vs_def_summary",
                "stats.player_form_trends"
            ]
            
            for script in scripts:
                try:
                    self._run_python_script(script)
                except Exception as e:
                    logger.warning(f"Summary script failed: {e}")
                    
        except Exception as e:
            logger.error(f"Season summaries update failed: {e}")
    
    def _validate_data_integrity(self):
        """Validate data integrity after updates"""
        logger.info("Validating data integrity...")
        
        conn = sqlite3.connect(self.db_path)
        try:
            validation_checks = [
                ("Games with missing scores", "SELECT COUNT(*) FROM games WHERE home_score IS NULL AND game_date < date('now', '-1 day')"),
                ("Players without teams", "SELECT COUNT(*) FROM player_team_current WHERE team_abbr IS NULL"),
                ("Active injuries without teams", "SELECT COUNT(*) FROM nfl_injuries_tracking WHERE is_active = 1 AND (team IS NULL OR team = '')"),
                ("Duplicate players", "SELECT COUNT(*) - COUNT(DISTINCT full_name) FROM player_team_current"),
                ("Recent odds count", "SELECT COUNT(*) FROM odds WHERE timestamp >= datetime('now', '-24 hours')")
            ]
            
            warnings = []
            for check_name, query in validation_checks:
                try:
                    result = conn.execute(query).fetchone()[0]
                    if check_name == "Recent odds count":
                        if result < 100:  # Expect at least 100 odds updates per day
                            warnings.append(f"{check_name}: Only {result} (expected >100)")
                        else:
                            logger.info(f"✓ {check_name}: {result}")
                    elif result > 0:
                        if result > 50:  # More than 50 issues is concerning
                            warnings.append(f"{check_name}: {result}")
                        else:
                            logger.info(f"⚠ {check_name}: {result} (minor)")
                    else:
                        logger.info(f"✓ {check_name}: {result}")
                        
                except Exception as e:
                    warnings.append(f"{check_name}: Query failed - {e}")
            
            if warnings:
                logger.warning("Data integrity warnings:")
                for warning in warnings:
                    logger.warning(f"  - {warning}")
            else:
                logger.info("✓ Data integrity validation passed")
                
        finally:
            conn.close()
    
    def _cleanup_old_data(self):
        """Clean up old data with better logging"""
        logger.info("Cleaning up old data...")
        
        conn = sqlite3.connect(self.db_path)
        try:
            cleanup_operations = [
                ("Old odds (>30 days)", "DELETE FROM odds WHERE timestamp < datetime('now', '-30 days')"),
                ("Inactive injuries (>14 days)", "DELETE FROM nfl_injuries_tracking WHERE is_active = 0 AND last_updated < datetime('now', '-14 days')"),
                ("Old system status (>7 days)", "DELETE FROM system_status WHERE created_at < datetime('now', '-7 days') AND status != 'FAILED'"),
                ("Orphaned player stats", "DELETE FROM player_game_stats WHERE game_id NOT IN (SELECT game_id FROM games)")
            ]
            
            total_deleted = 0
            for operation_name, query in cleanup_operations:
                try:
                    cursor = conn.execute(query)
                    deleted = cursor.rowcount
                    total_deleted += deleted
                    if deleted > 0:
                        logger.info(f"✓ {operation_name}: Deleted {deleted} records")
                    else:
                        logger.info(f"✓ {operation_name}: No records to delete")
                except Exception as e:
                    logger.warning(f"✗ {operation_name}: Failed - {e}")
            
            # Vacuum database to reclaim space
            if total_deleted > 100:
                logger.info("Running VACUUM to reclaim space...")
                conn.execute("VACUUM")
                logger.info("✓ Database vacuumed")
            
            conn.commit()
            logger.info(f"✓ Cleanup completed. Total records deleted: {total_deleted}")
            
        except Exception as e:
            logger.error(f"Database cleanup failed: {e}")
            raise
        finally:
            conn.close()
    
    def _check_model_retraining(self):
        """Enhanced model retraining logic"""
        logger.info("Checking model retraining needs...")
        
        model_file = os.path.join(self.models_path, "betting_model.pkl")
        should_retrain = False
        retrain_reasons = []
        
        try:
            # Check if model exists
            if not os.path.exists(model_file):
                should_retrain = True
                retrain_reasons.append("No model file found")
            else:
                # Check model age
                model_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(model_file))
                if model_age.days >= 7:
                    should_retrain = True
                    retrain_reasons.append(f"Model is {model_age.days} days old")
                
                # Check if we have enough new data
                conn = sqlite3.connect(self.db_path)
                try:
                    new_games = conn.execute("""
                        SELECT COUNT(*) FROM games 
                        WHERE home_score IS NOT NULL 
                        AND game_date >= date('now', '-7 days')
                    """).fetchone()[0]
                    
                    if new_games >= 10:  # At least 10 new games
                        should_retrain = True
                        retrain_reasons.append(f"{new_games} new games completed")
                        
                finally:
                    conn.close()
            
            if should_retrain:
                logger.info(f"Model retraining triggered: {', '.join(retrain_reasons)}")
                self._retrain_model()
            else:
                logger.info("Model is up to date, no retraining needed")
                
        except Exception as e:
            logger.error(f"Model check failed: {e}")
            # Don't raise - model issues shouldn't stop data refresh
    
    def _retrain_model(self):
        """Retrain the betting model with progress logging"""
        logger.info("Starting model retraining...")
        
        try:
            # Import and run model training
            from model.train_betting_model import BettingModelTrainer
            
            trainer = BettingModelTrainer()
            
            logger.info("Building training dataset...")
            df = trainer.build_training_dataset()
            
            if len(df) < 50:
                logger.warning(f"Insufficient data for training: {len(df)} games (need >50)")
                return
            
            logger.info(f"Training with {len(df)} games...")
            results = trainer.train_models(df)
            
            logger.info("Saving model...")
            trainer.save_model()
            
            # Log best model performance
            best_model = max(results.keys(), key=lambda x: results[x]['auc'])
            best_auc = results[best_model]['auc']
            logger.info(f"✓ Model retrained successfully: {best_model} (AUC: {best_auc:.3f})")
            
            # Run quick validation
            logger.info("Running post-training validation...")
            trainer.backtest(df.tail(50), trainer.best_model)
            
        except Exception as e:
            logger.error(f"Model retraining failed: {e}")
            raise
    
    def _refresh_caches(self):
        """Refresh cached calculations and summaries"""
        logger.info("Refreshing caches and pre-calculations...")
        
        conn = sqlite3.connect(self.db_path)
        try:
            # Clear old cache tables
            cache_tables = ["cached_rankings", "cached_predictions", "cached_value_bets"]
            for table in cache_tables:
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {table}")
                except:
                    pass
            
            # Rebuild power rankings cache
            conn.execute("""
                CREATE TABLE cached_rankings AS
                SELECT 
                    ROW_NUMBER() OVER (ORDER BY power_score DESC) as rank,
                    team, 
                    power_score, 
                    wins, 
                    losses, 
                    win_pct,
                    games_played
                FROM team_season_summary 
                WHERE season = ? 
                ORDER BY power_score DESC
            """, (datetime.now().year,))
            
            # Rebuild predictions cache
            conn.execute("""
                CREATE TABLE cached_predictions AS
                SELECT 
                    g.game_id, 
                    g.home_team, 
                    g.away_team, 
                    g.game_date,
                    g.start_time_local,
                    ht.power_score as home_power, 
                    at.power_score as away_power,
                    (ht.power_score + 2.5 - at.power_score) as power_diff
                FROM games g
                LEFT JOIN team_season_summary ht ON g.home_team = ht.team AND ht.season = ?
                LEFT JOIN team_season_summary at ON g.away_team = at.team AND at.season = ?
                WHERE g.game_date BETWEEN date('now') AND date('now', '+21 days')
                  AND g.home_score IS NULL
                ORDER BY g.game_date, g.start_time_local
            """, (datetime.now().year, datetime.now().year))
            
            conn.commit()
            
            # Count cached records
            rankings_count = conn.execute("SELECT COUNT(*) FROM cached_rankings").fetchone()[0]
            predictions_count = conn.execute("SELECT COUNT(*) FROM cached_predictions").fetchone()[0]
            
            logger.info(f"✓ Cached {rankings_count} rankings and {predictions_count} predictions")
            
        except Exception as e:
            logger.error(f"Cache refresh failed: {e}")
            # Don't raise - cache failures aren't critical
        finally:
            conn.close()
    
    def _run_python_script(self, module_path: str):
        """Run a Python script/module with proper error handling"""
        try:
            # Convert module path to actual import and execution
            parts = module_path.split('.')
            if len(parts) == 2:
                module_name, function_name = parts
                module_name = module_name.replace('_', '.')
                
                # Try to import and run the main function
                if module_name == "stats.import.2025.roster":
                    from stats.import_2025_roster import main
                    main()
                elif module_name == "stats.map.player.teams":
                    from stats.map_player_teams import main
                    main()
                elif module_name == "data.team.mapping.fix":
                    from data.team_mapping_fix import fix_team_mappings
                    fix_team_mappings()
                elif module_name == "data.injury.database":
                    from data.injury_database import main
                    main()
                elif module_name == "stats.import.team.stats":
                    from stats.import_team_stats import main
                    main()
                elif module_name == "stats.team.season.summary":
                    from stats.team_season_summary import main
                    main()
                elif module_name == "stats.matchup.power.summary":
                    from stats.matchup_power_summary import main
                    main()
                elif module_name == "stats.update.scores":
                    from stats.update_scores import main
                    main()
                elif module_name == "stats.check.scores":
                    from stats.check_scores import main
                    main()
                elif module_name == "stats.import.player.stats":
                    from stats.import_player_stats import main
                    main()
                elif module_name == "stats.fetch.live.player.stats":
                    from stats.fetch_live_player_stats import main
                    main()
                else:
                    # Fallback to subprocess for complex modules
                    self._run_subprocess(module_path)
            else:
                self._run_subprocess(module_path)
                
        except ImportError:
            # If direct import fails, use subprocess
            self._run_subprocess(module_path)
        except Exception as e:
            logger.error(f"Script execution failed for {module_path}: {e}")
            raise
    
    def _run_subprocess(self, module_path: str):
        """Run script via subprocess"""
        script_path = os.path.join(PROJECT_ROOT, module_path.replace('.', '/') + '.py')
        if os.path.exists(script_path):
            result = subprocess.run([sys.executable, script_path], 
                                  capture_output=True, text=True, cwd=PROJECT_ROOT)
            if result.returncode != 0:
                raise Exception(f"Script failed: {result.stderr}")
        else:
            raise Exception(f"Script not found: {script_path}")
    
    def hourly_odds_refresh(self):
        """Lighter refresh for odds data with better logging"""
        start_time = datetime.now()
        logger.info("Starting hourly odds refresh...")
        
        try:
            # Get odds count before update
            conn = sqlite3.connect(self.db_path)
            before_count = conn.execute("SELECT COUNT(*) FROM odds").fetchone()[0]
            conn.close()
            
            # Update odds
            self._run_python_script("odds.get_odds_fixed")
            
            # Get odds count after update
            conn = sqlite3.connect(self.db_path)
            after_count = conn.execute("SELECT COUNT(*) FROM odds").fetchone()[0]
            recent_count = conn.execute("""
                SELECT COUNT(*) FROM odds 
                WHERE timestamp >= datetime('now', '-1 hour')
            """).fetchone()[0]
            conn.close()
            
            new_odds = after_count - before_count
            duration = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"✓ Odds refresh completed in {duration:.1f}s")
            logger.info(f"  New odds: {new_odds}, Recent odds: {recent_count}")
            
            self._update_status("success", f"Hourly odds refresh completed. {new_odds} new odds added.", {
                'new_odds': new_odds,
                'recent_odds': recent_count,
                'duration': duration
            })
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Hourly odds refresh failed after {duration:.1f}s: {e}")
            self._update_status("error", f"Hourly refresh error: {str(e)}", {
                'duration': duration
            })
    
    def _update_status(self, status: str, message: str, details: Dict = None):
        """Update refresh status with structured logging"""
        status_data = {
            "last_run": datetime.now().isoformat(),
            "status": status,
            "message": message,
            "details": details or {}
        }
        
        try:
            os.makedirs(os.path.dirname(self.status_file), exist_ok=True)
            with open(self.status_file, 'w') as f:
                json.dump(status_data, f, indent=2)
                
            # Also log to database if available
            try:
                conn = sqlite3.connect(self.db_path)
                conn.execute("""
                    INSERT OR IGNORE INTO system_status 
                    (task, started_at, finished_at, status, message)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    'daily_refresh',
                    datetime.now().isoformat(),
                    datetime.now().isoformat(),
                    status,
                    message[:500]  # Truncate long messages
                ))
                conn.commit()
                conn.close()
            except:
                pass  # Don't fail status update if DB write fails
                
        except Exception as e:
            logger.warning(f"Failed to update status file: {e}")

def setup_scheduler():
    """Setup the scheduling for automated runs"""
    refresh_manager = EnhancedDailyRefresh()
    
    # Daily full refresh at 6 AM
    schedule.every().day.at("06:00").do(refresh_manager.full_data_refresh)
    
    # Hourly odds refresh during active hours (8 AM to 11 PM)
    for hour in range(8, 24):  # 8 AM to 11 PM
        schedule.every().day.at(f"{hour:02d}:00").do(refresh_manager.hourly_odds_refresh)
    
    logger.info("Scheduler setup complete:")
    logger.info("- Daily full refresh: 6:00 AM")
    logger.info("- Hourly odds refresh: 8:00 AM - 11:00 PM")
    
    return refresh_manager

def run_scheduler():
    """Run the scheduler daemon"""
    logger.info("Starting Enhanced Bettr Bot Data Refresh Scheduler...")
    logger.info("=" * 60)
    
    refresh_manager = setup_scheduler()
    
    # Run initial quick check (not full refresh)
    try:
        logger.info("Running initial odds refresh...")
        refresh_manager.hourly_odds_refresh()
    except Exception as e:
        logger.error(f"Initial refresh failed: {e}")
    
    logger.info("Scheduler running. Press Ctrl+C to stop.")
    
    # Keep scheduler running
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
    except Exception as e:
        logger.error(f"Scheduler error: {e}")
        time.sleep(300)  # Wait 5 minutes before restart
        run_scheduler()  # Restart scheduler

if __name__ == "__main__":
    # Ensure logs directory exists
    os.makedirs("logs", exist_ok=True)
    
    # Run based on command line argument
    if len(sys.argv) > 1:
        refresh_manager = EnhancedDailyRefresh()
        
        if sys.argv[1] == "full":
            # Run full refresh once
            refresh_manager.full_data_refresh()
        elif sys.argv[1] == "odds":
            # Just update odds
            refresh_manager.hourly_odds_refresh()
        elif sys.argv[1] == "model":
            # Just retrain model
            refresh_manager._check_model_retraining()
        elif sys.argv[1] == "status":
            # Show current status
            try:
                with open(refresh_manager.status_file, 'r') as f:
                    status = json.load(f)
                print(json.dumps(status, indent=2))
            except:
                print("No status file found")
    else:
        # Run scheduler daemon
        run_scheduler()