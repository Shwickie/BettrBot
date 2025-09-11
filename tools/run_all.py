# run_all.py - Updated to use comprehensive score updates
"""
BETTR BOT WEEKLY DATA PIPELINE - UPDATED FOR DAILY REFRESHES

This script runs the complete data pipeline for updating NFL betting data.
Now properly handles both historical training data and current season updates.
"""

import subprocess, sys, time, argparse, os
from datetime import datetime
from sqlalchemy import create_engine, text

DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
ROOT = r"E:\Bettr Bot\betting-bot"

# Core tasks that must run for the system to work (UPDATED)
CRITICAL_TASKS = [
    ("setup_db",                [sys.executable, os.path.join(ROOT, "data", "setup_db.py")]),
    ("check_scores",            [sys.executable, os.path.join(ROOT, "stats", "check_scores.py")]),
    ("update_scores",     [sys.executable, os.path.join(ROOT, "stats", "update_scores.py")]),
    ("team_season_summary",     [sys.executable, os.path.join(ROOT, "stats", "team_season_summary.py")]),
    ("train_betting_model",     [sys.executable, os.path.join(ROOT, "model", "train_betting_model.py")]),
    ("prediction",              [sys.executable, os.path.join(ROOT, "model", "prediction.py")]),
    ("get_odds",                [sys.executable, os.path.join(ROOT, "odds", "get_odds_fixed.py")]),
]

# Fast tasks that enhance the system but aren't critical
FAST_TASKS = [
    ("import_team_stats",       [sys.executable, os.path.join(ROOT, "stats", "import_team_stats.py")]),
    ("injury_impact_model",     [sys.executable, os.path.join(ROOT, "stats", "injury_impact_model.py")]),
    ("matchup_power_summary",   [sys.executable, os.path.join(ROOT, "stats", "matchup_power_summary.py")]),
]

# Slow tasks that can timeout or take very long - with more aggressive timeouts
SLOW_TASKS = [
    ("insert_historical",       [sys.executable, os.path.join(ROOT, "stats", "insert_historical_games.py")]),
    ("fill_game_times",         [sys.executable, os.path.join(ROOT, "stats", "fill_game_times.py")]),
    ("import_player_stats",     [sys.executable, os.path.join(ROOT, "stats", "import_player_stats.py")]),
    ("fetch_live_players",      [sys.executable, os.path.join(ROOT, "stats", "fetch_live_player_stats.py")]),
    ("import_2025_roster",      [sys.executable, os.path.join(ROOT, "stats", "import_2025_roster.py")]),
    ("map_player_teams",        [sys.executable, os.path.join(ROOT, "stats", "map_player_teams.py")]),
    ("player_vs_def_summary",   [sys.executable, os.path.join(ROOT, "stats", "player_vs_defense_summary.py")]),
    ("pos_vs_def_summary",      [sys.executable, os.path.join(ROOT, "stats", "pos_vs_def_summary.py")]),
    ("player_form_trends",      [sys.executable, os.path.join(ROOT, "stats", "player_form_trends.py")]),
    ("ai_validate_and_pack",    [sys.executable, os.path.join(ROOT, "model", "ai_validate_and_pack.py")]),
]

# All tasks in priority order
ALL_TASKS = CRITICAL_TASKS + FAST_TASKS + SLOW_TASKS

def ensure_status_table(engine):
    """Create or update the system_status table with proper schema"""
    with engine.begin() as conn:
        # First, create the table if it doesn't exist
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS system_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT,
                message TEXT,
                run_type TEXT DEFAULT 'weekly'
            )
        """))
        
        # Check if timeout_seconds column exists, if not add it
        try:
            conn.execute(text("SELECT timeout_seconds FROM system_status LIMIT 1"))
        except:
            # Column doesn't exist, add it
            try:
                conn.execute(text("ALTER TABLE system_status ADD COLUMN timeout_seconds INTEGER"))
                print("✅ Added timeout_seconds column to system_status table")
            except Exception as e:
                print(f"⚠️ Could not add timeout_seconds column: {e}")

def record_status(engine, task, started_at, finished_at, status, message, run_type='weekly', timeout_seconds=None):
    """Record task status in database with proper column handling"""
    with engine.begin() as conn:
        # Check if timeout_seconds column exists
        try:
            conn.execute(text("SELECT timeout_seconds FROM system_status LIMIT 1"))
            has_timeout_column = True
        except:
            has_timeout_column = False
        
        if has_timeout_column:
            # Use the full query with timeout_seconds
            conn.execute(text("""
                INSERT INTO system_status (task, started_at, finished_at, status, message, run_type, timeout_seconds)
                VALUES (:task, :started_at, :finished_at, :status, :message, :run_type, :timeout)
            """), dict(
                task=task, 
                started_at=started_at, 
                finished_at=finished_at, 
                status=status, 
                message=message[:600], 
                run_type=run_type,
                timeout=timeout_seconds
            ))
        else:
            # Use the query without timeout_seconds
            conn.execute(text("""
                INSERT INTO system_status (task, started_at, finished_at, status, message, run_type)
                VALUES (:task, :started_at, :finished_at, :status, :message, :run_type)
            """), dict(
                task=task, 
                started_at=started_at, 
                finished_at=finished_at, 
                status=status, 
                message=message[:600], 
                run_type=run_type
            ))

def run_task(engine, name, cmd, dry_run=False, run_type='weekly', timeout_override=None):
    started_at = datetime.utcnow().isoformat()
    
    # More aggressive timeouts to prevent hanging
    if timeout_override:
        timeout = timeout_override
    elif name in ['train_betting_model']:
        timeout = 900   # 15 minutes for model training (reduced from 30)
    elif name in ['prediction']:
        timeout = 30 
    elif name in ['update_scores_fixed']:  # NEW: reasonable timeout for comprehensive score update
        timeout = 180   # 3 minutes for score fetching
    elif name in ['import_2025_roster']:
        timeout = 120   # 2 minutes for roster import (reduced from 5)
    elif name in ['import_player_stats']:
        timeout = 180   # 3 minutes for player stats (reduced from 5)
    elif name in ['fill_game_times', 'insert_historical', 'fetch_live_players']:
        timeout = 90    # 1.5 minutes for network tasks (reduced)
    else:
        timeout = 60    # 1 minute default (reduced from 2)
    
    is_critical = name in [t[0] for t in CRITICAL_TASKS]
    icon = "🔥" if is_critical else "▶️"
    
    print(f"\n{icon} {name} — START (timeout: {timeout}s)")
    
    if dry_run:
        print(f"   (dry run) would run: {' '.join(cmd)}")
        record_status(engine, name, started_at, datetime.utcnow().isoformat(), "SKIPPED", "dry_run", run_type)
        return True

    try:
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["BETTR_PIPELINE_MODE"] = "true" 

        proc = subprocess.run(cmd, capture_output=True, text=False, env=env, timeout=timeout)
        finished_at = datetime.utcnow().isoformat()
        
        out = (proc.stdout or b"").decode("utf-8", errors="replace")
        err = (proc.stderr or b"").decode("utf-8", errors="replace")

        if proc.returncode == 0:
            success_icon = "🎉" if is_critical else "✅"
            print(f"{success_icon} {name} — OK")
            
            # Show condensed output for important tasks
            if out and len(out.strip()) > 0:
                lines = out.strip().split('\n')
                if len(lines) > 5:
                    print(f"   Output: {lines[0]}")
                    print(f"   ... ({len(lines)-2} more lines) ...")
                    print(f"   {lines[-1]}")
                else:
                    print(out.strip()[-500:])
                    
            record_status(engine, name, started_at, finished_at, "OK", out, run_type, timeout)
            return True
        else:
            fail_icon = "💥" if is_critical else "❌"
            print(f"{fail_icon} {name} — FAILED")
            if err:
                print(f"   Error: {err.strip()[-300:]}")
            record_status(engine, name, started_at, finished_at, "FAIL", out + "\n" + err, run_type, timeout)
            return False
            
    except subprocess.TimeoutExpired:
        finished_at = datetime.utcnow().isoformat()
        print(f"⏰ {name} — TIMEOUT after {timeout}s")
        
        # For non-critical tasks, timeout is OK
        if not is_critical:
            print(f"   (non-critical task - continuing)")
            record_status(engine, name, started_at, finished_at, "TIMEOUT_OK", f"Timeout after {timeout}s (non-critical)", run_type, timeout)
            return True
        else:
            print(f"   (critical task failed - consider investigating)")
            record_status(engine, name, started_at, finished_at, "TIMEOUT_FAIL", f"Critical task timeout after {timeout}s", run_type, timeout)
            return False
            
    except Exception as e:
        finished_at = datetime.utcnow().isoformat()
        print(f"💥 {name} — EXCEPTION: {e}")
        record_status(engine, name, started_at, finished_at, "EXCEPTION", str(e), run_type)
        return False

def check_current_week_status(engine):
    """Check current season status"""
    try:
        with engine.connect() as conn:
            recent_games = conn.execute(text("""
                SELECT COUNT(*) as completed_games,
                       MAX(game_date) as latest_game_date
                FROM games 
                WHERE CAST(strftime('%Y', game_date) AS INTEGER) = 2024  
                AND home_score IS NOT NULL 
                AND away_score IS NOT NULL
            """)).fetchone()
            
            completed = recent_games[0] if recent_games else 0
            latest_date = recent_games[1] if recent_games and recent_games[1] else None
            
            print(f"📊 Season Status: {completed} completed games (2024)")
            if latest_date:
                print(f"📅 Latest completed game: {latest_date}")
            
            return {
                'completed_games': completed,
                'latest_date': latest_date,
                'needs_update': True  # Always check for updates
            }
                
    except Exception as e:
        print(f"⚠️ Could not check status: {e}")
        return {'completed_games': 0, 'needs_update': True}

def main():
    parser = argparse.ArgumentParser(description="Bettr Bot Weekly Data Pipeline - DAILY REFRESH OPTIMIZED")
    parser.add_argument("--only", help="Run a single task by name")
    parser.add_argument("--from", dest="from_task", help="Start from this task name")
    parser.add_argument("--critical-only", action="store_true", help="Only critical tasks (3-5 min)")
    parser.add_argument("--fast", action="store_true", help="Critical + fast tasks (5-10 min)")
    parser.add_argument("--skip-slow", action="store_true", help="Skip potentially slow tasks")
    parser.add_argument("--skip-problematic", action="store_true", help="Skip tasks known to hang")
    parser.add_argument("--timeout", type=int, help="Override timeout for all tasks (seconds)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would run")
    parser.add_argument("--daily", action="store_true", help="Optimized daily refresh (critical + scores)")
    
    args = parser.parse_args()

    print("🏈 BETTR BOT - DAILY PREDICTION REFRESH")
    print("=" * 65)
    
    engine = create_engine(DB_PATH, connect_args={"timeout": 30})
    ensure_status_table(engine)
    
    current_status = check_current_week_status(engine)
    run_type = "daily_refresh" if args.daily else "weekly_update"
    
    # Tasks that are known to cause problems
    PROBLEMATIC_TASKS = ['import_2025_roster', 'fill_game_times']
    
    # Determine task list
    if args.only:
        # Single task
        to_run = [t for t in ALL_TASKS if t[0] == args.only]
        if not to_run:
            print(f"❌ Task not found: {args.only}")
            sys.exit(1)
    elif args.from_task:
        # From specific task onwards
        seen = False
        to_run = []
        for t in ALL_TASKS:
            if t[0] == args.from_task:
                seen = True
            if seen:
                to_run.append(t)
    elif args.daily:
        # NEW: Daily refresh mode - critical tasks only, optimized for speed
        to_run = CRITICAL_TASKS
        print("🔄 DAILY REFRESH MODE - Fast prediction updates")
    elif args.critical_only:
        # Just the essentials - perfect for cloud automation
        to_run = CRITICAL_TASKS
        print("🔥 CRITICAL TASKS ONLY - Fast cloud update")
    elif args.fast:
        # Critical + fast tasks
        to_run = CRITICAL_TASKS + FAST_TASKS
        print("⚡ FAST MODE - Essential updates")
    elif args.skip_slow:
        # Everything except slow tasks
        to_run = CRITICAL_TASKS + FAST_TASKS
        print("🚀 SKIPPING SLOW TASKS - Optimized for cloud")
    elif args.skip_problematic:
        # Everything except problematic tasks
        to_run = [t for t in ALL_TASKS if t[0] not in PROBLEMATIC_TASKS]
        print("🛠️ SKIPPING PROBLEMATIC TASKS - Stable run")
    else:
        # Full pipeline
        to_run = ALL_TASKS
        print("📋 FULL PIPELINE - All tasks")

    # Filter out problematic tasks if skip_problematic is set
    if args.skip_problematic:
        to_run = [t for t in to_run if t[0] not in PROBLEMATIC_TASKS]

    # Show task plan
    if not args.dry_run:
        print(f"\n📝 RUNNING {len(to_run)} TASKS:")
        for i, (name, _) in enumerate(to_run):
            task_type = "🔥" if name in [t[0] for t in CRITICAL_TASKS] else "⚡" if name in [t[0] for t in FAST_TASKS] else "🌍"
            print(f"  {i+1:2}. {name} {task_type}")
        print()

    # Execute pipeline
    start_time = time.time()
    failures = 0
    success_count = 0
    
    for i, (name, cmd) in enumerate(to_run):
        print(f"\n{'='*50}")
        print(f"TASK {i+1}/{len(to_run)}: {name.upper()}")
        print(f"{'='*50}")
        
        ok = run_task(engine, name, cmd, dry_run=args.dry_run, run_type=run_type, timeout_override=args.timeout)
        
        if ok:
            success_count += 1
        else:
            failures += 1
            
            # For critical tasks, offer to continue
            is_critical = name in [t[0] for t in CRITICAL_TASKS]
            if is_critical and not args.dry_run:
                print(f"\n⚠️ CRITICAL TASK FAILED: {name}")
                print("Options:")
                print("  y - Continue with remaining tasks")
                print("  n - Stop pipeline")
                print("  s - Skip remaining slow tasks")
                try:
                    choice = input("Choose (y/n/s): ").strip().lower()
                    if choice == 'n':
                        print("🛑 Stopping pipeline")
                        break
                    elif choice == 's':
                        print("⚡ Switching to fast mode")
                        # Remove remaining slow tasks
                        remaining_tasks = to_run[i+1:]
                        to_run = to_run[:i+1] + [t for t in remaining_tasks if t[0] in [x[0] for x in CRITICAL_TASKS + FAST_TASKS]]
                except (EOFError, KeyboardInterrupt):
                    print("\n🛑 Pipeline interrupted")
                    break
        
        time.sleep(0.1)  # Brief pause

    # Final summary
    total_time = time.time() - start_time
    
    print(f"\n{'='*50}")
    print("🏁 PIPELINE COMPLETE")
    print(f"{'='*50}")
    print(f"⏱️ Total time: {total_time/60:.1f} minutes")
    print(f"✅ Successful: {success_count}")
    print(f"❌ Failed: {failures}")
    if success_count + failures > 0:
        print(f"📈 Success rate: {success_count/(success_count+failures)*100:.1f}%")
    
    if failures == 0:
        print(f"\n🎉 ALL TASKS COMPLETED SUCCESSFULLY!")
        print(f"🌐 Dashboard ready with fresh predictions")
        print(f"🤖 AI systems updated with latest data")
        print(f"📊 Rankings refreshed with current scores")
    elif success_count >= len(CRITICAL_TASKS):
        print(f"\n✅ CORE SYSTEMS OPERATIONAL")
        print(f"🔧 Some optional tasks failed - predictions still functional")
        print(f"💡 Try --daily for fastest updates")
    else:
        print(f"\n⚠️ CRITICAL FAILURES")

if __name__ == "__main__":
    try:
        # On Windows, this enables ANSI emojis/colors in some terminals
        import os, sys
        if sys.platform.startswith("win"):
            os.system("")
        main()
    except KeyboardInterrupt:
        print("\n🛑 Pipeline aborted by user")
