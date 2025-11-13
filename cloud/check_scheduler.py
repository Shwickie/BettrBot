#!/usr/bin/env python3
# check_scheduler.py - Comprehensive scheduler diagnostic and fix tool

import sys
import os
from pathlib import Path
from datetime import datetime

def check_apscheduler():
    """Check if APScheduler is installed"""
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        print("✅ APScheduler is installed")
        return True
    except ImportError:
        print("❌ APScheduler is NOT installed")
        print("   Fix: pip install APScheduler==3.10.4")
        return False

def check_files():
    """Check if required files exist"""
    root = Path.cwd()
    
    required_files = {
        'cloud_run_all.py': 'Main pipeline script',
        'automated_scheduler.py': 'Scheduler script',
        'betting_model_fixed.pkl': 'ML model file'
    }
    
    print("\n📁 Checking required files:")
    all_exist = True
    
    for filename, description in required_files.items():
        filepath = root / filename
        exists = filepath.exists()
        status = "✅" if exists else "❌"
        print(f"   {status} {filename} - {description}")
        if not exists:
            all_exist = False
    
    return all_exist

def check_database():
    """Check database connection"""
    try:
        from sqlalchemy import create_engine, text
        
        DATABASE_URL = "postgresql://postgres:YviqtXqcsCIgRzSCofNjbfwgjkYNLydX@maglev.proxy.rlwy.net:54187/railway"
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            
            # Check critical tables
            tables = ['games', 'team_season_summary', 'odds', 'predictions']
            missing = []
            
            for table in tables:
                result = conn.execute(text(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = '{table}'
                    );
                """)).scalar()
                
                if not result:
                    missing.append(table)
            
            if missing:
                print(f"\n⚠️  Database connected but missing tables: {', '.join(missing)}")
                return False
            else:
                print("\n✅ Database connection successful - all tables present")
                return True
                
    except Exception as e:
        print(f"\n❌ Database connection failed: {e}")
        return False

def check_scheduler_status():
    """Check if scheduler is running"""
    try:
        sys.path.insert(0, str(Path.cwd()))
        from automated_scheduler import get_scheduler_status
        
        status = get_scheduler_status()
        
        print("\n🤖 Scheduler Status:")
        print(f"   Running: {status.get('running', False)}")
        print(f"   Pipeline script: {status.get('pipeline_script')}")
        print(f"   Pipeline exists: {status.get('pipeline_exists', False)}")
        print(f"   Total runs: {status.get('run_count', 0)}")
        print(f"   Last run: {status.get('last_pipeline_run', 'Never')}")
        print(f"   Consecutive failures: {status.get('consecutive_failures', 0)}")
        print(f"   In cooldown: {status.get('in_cooldown', False)}")
        
        return status.get('running', False)
        
    except ImportError as e:
        print(f"\n❌ Cannot import scheduler module: {e}")
        return False
    except Exception as e:
        print(f"\n❌ Error checking scheduler: {e}")
        return False

def check_recent_runs():
    """Check recent pipeline runs"""
    try:
        log_file = Path("pipeline_runs.log")
        
        if not log_file.exists():
            print("\n📊 No pipeline run history found")
            return False
        
        with open(log_file, 'r') as f:
            lines = f.readlines()
        
        if not lines:
            print("\n📊 Pipeline log is empty")
            return False
        
        print(f"\n📊 Recent Pipeline Runs (last 5):")
        for line in lines[-5:]:
            try:
                parts = line.strip().split(',')
                timestamp = parts[0]
                duration = float(parts[1])
                success = parts[2] == 'True'
                run_num = parts[3]
                
                status = "✅" if success else "❌"
                dt = datetime.fromisoformat(timestamp)
                print(f"   {status} Run #{run_num}: {dt.strftime('%Y-%m-%d %H:%M:%S')} ({duration:.1f}s)")
            except:
                continue
        
        return True
        
    except Exception as e:
        print(f"\n⚠️  Could not read pipeline log: {e}")
        return False

def test_pipeline_manual():
    """Test if pipeline can be run manually"""
    print("\n🧪 Testing manual pipeline run...")
    print("   This will take 30-60 seconds...")
    
    try:
        import subprocess
        
        result = subprocess.run(
            [sys.executable, "cloud_run_all.py"],
            capture_output=True,
            text=True,
            timeout=120,
            encoding='utf-8',
            errors='replace'
        )
        
        if result.returncode == 0:
            print("   ✅ Manual pipeline run successful!")
            return True
        else:
            print("   ❌ Manual pipeline run failed")
            print(f"   Error: {result.stderr[:200]}")
            return False
            
    except subprocess.TimeoutExpired:
        print("   ⚠️  Pipeline timed out (might still be working)")
        return False
    except Exception as e:
        print(f"   ❌ Could not run pipeline: {e}")
        return False

def provide_recommendations(checks):
    """Provide recommendations based on check results"""
    print("\n" + "="*50)
    print("📋 RECOMMENDATIONS:")
    print("="*50)
    
    if not checks['apscheduler']:
        print("\n1. Install APScheduler:")
        print("   pip install APScheduler==3.10.4")
    
    if not checks['files']:
        print("\n2. Missing files - ensure you're in the correct directory:")
        print("   cd E:\\Bettr Bot\\betting-bot\\cloud")
    
    if not checks['database']:
        print("\n3. Fix database connection:")
        print("   - Check your DATABASE_URL environment variable")
        print("   - Verify database credentials")
        print("   - Run: python fix_postgre_sql.py")
    
    if not checks['scheduler_running']:
        print("\n4. Start the scheduler:")
        print("   python automated_scheduler.py")
    
    if checks['scheduler_running'] and checks['consecutive_failures'] > 0:
        print("\n5. Scheduler has failures - check logs:")
        print("   - Review scheduler.log")
        print("   - Force a manual run: python automated_scheduler.py --once")
        print("   - Check database health: python automated_scheduler.py --health")
    
    # Overall status
    print("\n" + "="*50)
    if all([checks['apscheduler'], checks['files'], checks['database']]):
        if checks['scheduler_running']:
            print("✅ SYSTEM STATUS: All systems operational!")
        else:
            print("⚠️  SYSTEM STATUS: Ready but scheduler not running")
            print("   Run: python automated_scheduler.py")
    else:
        print("❌ SYSTEM STATUS: Issues need to be resolved")

def main():
    """Run comprehensive scheduler diagnostic"""
    print("BETTR BOT SCHEDULER DIAGNOSTIC")
    print("="*50)
    print(f"Time: {datetime.now()}")
    print(f"Working directory: {Path.cwd()}")
    
    # Run all checks
    checks = {
        'apscheduler': check_apscheduler(),
        'files': check_files(),
        'database': check_database(),
        'scheduler_running': False,
        'consecutive_failures': 0
    }
    
    # Check scheduler status
    scheduler_status = check_scheduler_status()
    checks['scheduler_running'] = scheduler_status
    
    # Check recent runs
    check_recent_runs()
    
    # Provide recommendations
    provide_recommendations(checks)
    
    print("\n" + "="*50)
    print(f"Diagnostic complete: {datetime.now()}")
    print("="*50)

if __name__ == "__main__":
    main()