#!/usr/bin/env python3
# fix_scheduler.py - Automatically fix common scheduler issues

import sys
import os
import subprocess
from pathlib import Path
from datetime import datetime

def install_apscheduler():
    """Install APScheduler if missing"""
    print("📦 Installing APScheduler...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "APScheduler==3.10.4"])
        print("   ✅ APScheduler installed")
        return True
    except Exception as e:
        print(f"   ❌ Failed to install: {e}")
        return False

def clear_failure_flag():
    """Clear failure flag if it exists"""
    failure_file = Path("PIPELINE_FAILURE.flag")
    if failure_file.exists():
        print("🧹 Clearing failure flag...")
        failure_file.unlink()
        print("   ✅ Failure flag cleared")
        return True
    return False

def reset_cooldown():
    """Reset scheduler cooldown by restarting it"""
    print("🔄 Resetting scheduler cooldown...")
    try:
        sys.path.insert(0, str(Path.cwd()))
        from automated_scheduler import get_scheduler
        
        scheduler = get_scheduler()
        scheduler.consecutive_failures = 0
        scheduler.last_failure_time = None
        print("   ✅ Cooldown reset")
        return True
    except Exception as e:
        print(f"   ⚠️  Could not reset cooldown: {e}")
        return False

def test_database_connection():
    """Test and fix database connection"""
    print("🔌 Testing database connection...")
    try:
        from sqlalchemy import create_engine, text
        
        DATABASE_URL = "postgresql://postgres:YviqtXqcsCIgRzSCofNjbfwgjkYNLydX@maglev.proxy.rlwy.net:54187/railway"
        engine = create_engine(DATABASE_URL, pool_pre_ping=True, connect_args={"connect_timeout": 10})
        
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        print("   ✅ Database connection OK")
        return True
    except Exception as e:
        print(f"   ❌ Database connection failed: {e}")
        print("   💡 Try running: python fix_postgre_sql.py")
        return False

def fix_encoding_issues():
    """Fix encoding issues in cloud_run_all.py"""
    print("📝 Checking for encoding issues...")
    
    pipeline_file = Path("cloud_run_all.py")
    if not pipeline_file.exists():
        print("   ⚠️  cloud_run_all.py not found")
        return False
    
    try:
        # Read with proper encoding
        with open(pipeline_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check for problematic characters
        if '\U0001f310' in content or '🌐' in content:
            print("   🔧 Found emoji characters - fixing...")
            # Replace emojis with text
            content = content.replace('🌐', 'CLOUD')
            content = content.replace('🎯', 'TARGET')
            content = content.replace('✅', 'SUCCESS')
            content = content.replace('❌', 'ERROR')
            content = content.replace('⚠️', 'WARNING')
            
            # Write back
            with open(pipeline_file, 'w', encoding='utf-8') as f:
                f.write(content)
            
            print("   ✅ Encoding issues fixed")
            return True
        else:
            print("   ✅ No encoding issues found")
            return True
            
    except Exception as e:
        print(f"   ⚠️  Could not check encoding: {e}")
        return False

def test_manual_run():
    """Test a manual pipeline run"""
    print("🧪 Testing manual pipeline run...")
    print("   (This may take 30-60 seconds)")
    
    try:
        result = subprocess.run(
            [sys.executable, "cloud_run_all.py", "--test"],
            capture_output=True,
            text=True,
            timeout=120,
            encoding='utf-8',
            errors='replace',
            env={**os.environ, 'PYTHONIOENCODING': 'utf-8'}
        )
        
        # Check for success indicators
        output = result.stdout + result.stderr
        
        if result.returncode == 0 or 'SUCCESS' in output:
            print("   ✅ Manual run successful")
            return True
        else:
            print("   ❌ Manual run failed")
            # Show last few lines of error
            error_lines = output.split('\n')[-5:]
            for line in error_lines:
                if line.strip():
                    print(f"      {line}")
            return False
            
    except subprocess.TimeoutExpired:
        print("   ⚠️  Pipeline timed out")
        return False
    except Exception as e:
        print(f"   ❌ Could not test: {e}")
        return False

def start_scheduler():
    """Start the scheduler"""
    print("🚀 Starting scheduler...")
    
    try:
        sys.path.insert(0, str(Path.cwd()))
        from automated_scheduler import start_background_scheduler
        
        success = start_background_scheduler()
        
        if success:
            print("   ✅ Scheduler started successfully")
            print("   📅 Pipeline will run every 4 hours")
            print("   💾 Check status: python automated_scheduler.py --status")
            return True
        else:
            print("   ❌ Failed to start scheduler")
            return False
            
    except Exception as e:
        print(f"   ❌ Could not start scheduler: {e}")
        return False

def main():
    """Run all fixes"""
    print("BETTR BOT SCHEDULER FIX UTILITY")
    print("="*50)
    print(f"Time: {datetime.now()}")
    print(f"Directory: {Path.cwd()}\n")
    
    fixes_applied = []
    fixes_needed = []
    
    # 1. Check/install APScheduler
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        print("✅ APScheduler already installed")
    except ImportError:
        if install_apscheduler():
            fixes_applied.append("Installed APScheduler")
        else:
            fixes_needed.append("Install APScheduler manually")
    
    # 2. Clear failure flags
    if clear_failure_flag():
        fixes_applied.append("Cleared failure flag")
    
    # 3. Fix encoding issues
    if fix_encoding_issues():
        fixes_applied.append("Fixed encoding issues")
    
    # 4. Test database
    if not test_database_connection():
        fixes_needed.append("Fix database connection")
    
    # 5. Reset cooldown
    if reset_cooldown():
        fixes_applied.append("Reset scheduler cooldown")
    
    # Summary
    print("\n" + "="*50)
    print("SUMMARY:")
    print("="*50)
    
    if fixes_applied:
        print("\n✅ Fixes Applied:")
        for fix in fixes_applied:
            print(f"   - {fix}")
    
    if fixes_needed:
        print("\n⚠️  Manual Action Needed:")
        for need in fixes_needed:
            print(f"   - {need}")
    
    if not fixes_needed:
        print("\n🎉 All issues resolved!")
        print("\nNext steps:")
        print("1. Start the scheduler:")
        print("   python automated_scheduler.py")
        print("\n2. Monitor it:")
        print("   python check_scheduler.py")
        print("\n3. Check logs:")
        print("   tail -f scheduler.log")
    else:
        print("\n⚠️  Please resolve the issues above before starting the scheduler")
    
    print("\n" + "="*50)

if __name__ == "__main__":
    main()