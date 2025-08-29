#!/usr/bin/env python3
"""
Install Missing Requirements for Injury Scraper
"""

import subprocess
import sys

def install_package(package):
    """Install a package using pip"""
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        print(f"✅ Successfully installed {package}")
        return True
    except subprocess.CalledProcessError:
        print(f"❌ Failed to install {package}")
        return False

def main():
    """Install required packages"""
    print("📦 INSTALLING REQUIRED PACKAGES")
    print("=" * 40)
    
    required_packages = [
        "beautifulsoup4",  # for bs4
        "requests",        # for web scraping
        "lxml",           # for better HTML parsing
    ]
    
    success_count = 0
    
    for package in required_packages:
        print(f"📥 Installing {package}...")
        if install_package(package):
            success_count += 1
    
    print(f"\n📊 INSTALLATION RESULTS:")
    print(f"  ✅ Successfully installed: {success_count}/{len(required_packages)}")
    
    if success_count == len(required_packages):
        print(f"\n🎉 All packages installed successfully!")
        print(f"💡 You can now run the injury scraper:")
        print(f"   python current_injury_scraper_fixed.py")
    else:
        print(f"\n⚠️ Some packages failed to install")
        print(f"💡 Try installing manually:")
        for package in required_packages:
            print(f"   pip install {package}")

if __name__ == "__main__":
    main()