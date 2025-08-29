#!/usr/bin/env python3
import sys, os

def check_dependencies():
    required = ['flask', 'pandas', 'sqlalchemy', 'werkzeug']
    missing = []
    for p in required:
        try:
            __import__(p)
        except ImportError:
            missing.append(p)
    if missing:
        print("Missing required packages:")
        for pkg in missing:
            print(f"  - {pkg}")
        print("\nInstall with:")
        print(f"pip install {' '.join(missing)}")
        return False
    return True

def main():
    print("🎰 BETTR BOT DASHBOARD STARTUP")
    print("=" * 50)

    if not check_dependencies():
        sys.exit(1)

    # ✅ Force a single DB path for both SQLAlchemy and sqlite3 in the app
    os.environ.setdefault('BETTR_DB_PATH', r"E:/Bettr Bot/betting-bot/data/betting.db")
    print(f"Connected to local database\nDB_PATH={os.environ['BETTR_DB_PATH']}")

    ROOT = os.path.dirname(os.path.abspath(__file__))
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)

    tpl_candidates = [
        os.path.join(ROOT, 'dashboard', 'templates.py'),
        os.path.join(ROOT, 'templates.py'),
    ]
    found_tpl = next((p for p in tpl_candidates if os.path.exists(p)), None)
    if found_tpl:
        print(f"✅ Found templates.py at: {found_tpl}")
    else:
        print("⚠️  templates.py not found (expected in /dashboard/).")

    try:
        # Import from this folder
        from mobile_dashboard import main as dashboard_main
        print("✅ Imported mobile_dashboard.main (local folder)")
    except ImportError:
        try:
            from dashboard.mobile_dashboard import main as dashboard_main
            print("✅ Imported dashboard.mobile_dashboard.main")
        except ImportError as e:
            print(f"❌ Import error: {e}")
            print("Make sure 'dashboard/mobile_dashboard.py' exists and add an empty 'dashboard/__init__.py' if needed.")
            sys.exit(1)

    try:
        dashboard_main()
    except Exception as e:
        print(f"❌ Error starting dashboard: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
