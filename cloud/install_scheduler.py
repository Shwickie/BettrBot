# install_scheduler.py - Quick fix for APScheduler installation
"""
Run this script to install APScheduler and test the automated scheduling
"""

import subprocess
import sys
import os

def install_apscheduler():
    """Install APScheduler package"""
    print("Installing APScheduler...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "APScheduler==3.10.4"])
        print("✅ APScheduler installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install APScheduler: {e}")
        return False

def test_apscheduler():
    """Test if APScheduler can be imported"""
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        print("✅ APScheduler import test passed")
        return True
    except ImportError as e:
        print(f"❌ APScheduler import failed: {e}")
        return False

def test_scheduler_integration():
    """Test if automated scheduler can be imported"""
    try:
        # Add current directory to path
        sys.path.insert(0, os.getcwd())
        
        from automated_scheduler import BettrBotScheduler
        scheduler = BettrBotScheduler()
        print("✅ Automated scheduler integration test passed")
        return True
    except ImportError as e:
        print(f"❌ Automated scheduler import failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Scheduler test failed: {e}")
        return False

def main():
    print("BETTR BOT SCHEDULER INSTALLATION")
    print("=" * 40)
    
    # Test if already installed
    if test_apscheduler():
        print("APScheduler already installed")
    else:
        # Install APScheduler
        if not install_apscheduler():
            print("Installation failed - please install manually:")
            print("pip install APScheduler==3.10.4")
            return False
    
    # Test integration
    if test_scheduler_integration():
        print("\n🎉 SUCCESS: Automated scheduling is ready!")
        print("\nNext steps:")
        print("1. Your app will now start the scheduler automatically")
        print("2. Pipeline will run every 4 hours")
        print("3. Check the admin panel for automation status")
        print("4. Monitor with: python monitor_pipeline.py")
        return True
    else:
        print("\n⚠️ APScheduler installed but integration failed")
        print("Check that automated_scheduler.py exists in your project")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)