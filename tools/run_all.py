# run_all.py
import subprocess, sys, time, argparse, os
from datetime import datetime
from sqlalchemy import create_engine, text

DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
ROOT = r"E:\Bettr Bot\betting-bot"
TASKS = [
    ("setup_db",             [sys.executable, os.path.join(ROOT, "data", "setup_db.py")]),
    ("insert_historical",    [sys.executable, os.path.join(ROOT, "stats", "insert_historical_games.py")]),
    ("fill_game_times",      [sys.executable, os.path.join(ROOT, "stats", "fill_game_times.py")]),  # <-- new
    ("import_team_stats",    [sys.executable, os.path.join(ROOT, "stats", "import_team_stats.py")]),
    ("import_player_stats",  [sys.executable, os.path.join(ROOT, "stats", "import_player_stats.py")]),
    ("fetch_live_players",   [sys.executable, os.path.join(ROOT, "stats", "fetch_live_player_stats.py")]),
    ("map_player_teams",     [sys.executable, os.path.join(ROOT, "stats", "map_player_teams.py")]),
    ("team_season_summary",  [sys.executable, os.path.join(ROOT, "stats", "team_season_summary.py")]),
    ("matchup_power_summary",[sys.executable, os.path.join(ROOT, "stats", "matchup_power_summary.py")]),
    ("player_vs_def_summary",[sys.executable, os.path.join(ROOT, "stats", "player_vs_defense_summary.py")]),
    ("pos_vs_def_summary",   [sys.executable, os.path.join(ROOT, "stats", "pos_vs_def_summary.py")]),
    ("player_form_trends",   [sys.executable, os.path.join(ROOT, "stats", "player_form_trends.py")]),
    ("injury_impact_model",  [sys.executable, os.path.join(ROOT, "stats", "injury_impact_model.py")]),
    ("get_odds",             [sys.executable, os.path.join(ROOT, "odds", "get_odds_fixed.py")]),
    ("update_scores",        [sys.executable, os.path.join(ROOT, "stats", "update_scores.py")]),
    ("check_scores",         [sys.executable, os.path.join(ROOT, "stats", "check_scores.py")]),
]

def ensure_status_table(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS system_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT,
                message TEXT
            )
        """))

def record_status(engine, task, started_at, finished_at, status, message):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO system_status (task, started_at, finished_at, status, message)
            VALUES (:task, :started_at, :finished_at, :status, :message)
        """), dict(task=task, started_at=started_at, finished_at=finished_at, status=status, message=message[:600]))

def run_task(engine, name, cmd, dry_run=False):
    started_at = datetime.utcnow().isoformat()
    print(f"\n▶️  {name} — START ({started_at})")
    if dry_run:
        print("   (dry run) would run:", " ".join(cmd))
        record_status(engine, name, started_at, datetime.utcnow().isoformat(), "SKIPPED", "dry_run")
        return True

    try:
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"

        # capture bytes, decode ourselves
        proc = subprocess.run(cmd, capture_output=True, text=False, env=env)

        finished_at = datetime.utcnow().isoformat()
        out = (proc.stdout or b"").decode("utf-8", errors="replace")
        err = (proc.stderr or b"").decode("utf-8", errors="replace")

        if proc.returncode == 0:
            print(f"✅ {name} — OK")
            if out:
                print(out.strip()[-1000:])
            record_status(engine, name, started_at, finished_at, "OK", out)
            return True
        else:
            print(f"❌ {name} — FAILED")
            if err:
                print(err.strip()[-2000:])
            record_status(engine, name, started_at, finished_at, "FAIL", out + "\n" + err)
            return False
    except Exception as e:
        finished_at = datetime.utcnow().isoformat()
        print(f"💥 {name} — EXCEPTION: {e}")
        record_status(engine, name, started_at, finished_at, "EXCEPTION", str(e))
        return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", help="Run a single task by name (matches TASKS key).")
    parser.add_argument("--from", dest="from_task", help="Start from this task name.")
    parser.add_argument("--dry-run", action="store_true", help="List what would run without executing.")
    args = parser.parse_args()

    engine = create_engine(DB_PATH, connect_args={"timeout": 30})
    ensure_status_table(engine)

    to_run = TASKS
    if args.only:
        to_run = [t for t in TASKS if t[0] == args.only]
        if not to_run:
            print("Task not found:", args.only)
            sys.exit(1)
    elif args.from_task:
        seen = False
        filtered = []
        for t in TASKS:
            if t[0] == args.from_task:
                seen = True
            if seen:
                filtered.append(t)
        if not filtered:
            print("Start task not found:", args.from_task)
            sys.exit(1)
        to_run = filtered

    failures = 0
    for name, cmd in to_run:
        ok = run_task(engine, name, cmd, dry_run=args.dry_run)
        if not ok:
            failures += 1
            # Optional: break early
            # break
        time.sleep(0.2)  # tiny pause so logs/locks don’t collide

    print("\n==== SUMMARY ====")
    print(f"Ran {len(to_run)} tasks with {failures} failure(s).")
    print("Check system_status table for details.")

if __name__ == "__main__":
    main()
