# app.py - FIXED
import os
import sys

# Environment setup
os.environ.setdefault('FLASK_ENV', 'production')
DATABASE_URL = os.environ.get('DATABASE_URL')

if DATABASE_URL and DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
    os.environ['DATABASE_URL'] = DATABASE_URL

# Add current directory to path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# Also add cloud directory if it exists
cloud_path = os.path.join(project_root, 'cloud')
if os.path.exists(cloud_path):
    sys.path.insert(0, cloud_path)
    print(f"✅ Added cloud directory to path: {cloud_path}")

try:
    # Try importing from cloud.mobile_dashboard first
    print("🔍 Attempting to import from cloud.mobile_dashboard...")
    from cloud.mobile_dashboard import app
    print("✅ Cloud dashboard imported successfully")
except ImportError as e:
    print(f"⚠️ Cloud dashboard import failed: {e}")
    print(f"   Current directory: {os.getcwd()}")
    print(f"   Project root: {project_root}")
    print(f"   sys.path: {sys.path[:3]}")
    print(f"   Files in project root: {os.listdir(project_root)[:10]}")

    # Check if cloud directory exists
    if os.path.exists(cloud_path):
        print(f"   ✓ cloud/ directory exists")
        cloud_files = os.listdir(cloud_path)
        print(f"   Files in cloud/: {cloud_files[:10]}")
        if 'mobile_dashboard.py' in cloud_files:
            print(f"   ✓ cloud/mobile_dashboard.py exists")
        else:
            print(f"   ✗ cloud/mobile_dashboard.py NOT FOUND")
    else:
        print(f"   ✗ cloud/ directory NOT FOUND")

    try:
        # Fallback to root mobile_dashboard
        print("🔍 Attempting fallback to root mobile_dashboard...")
        from mobile_dashboard import app
        print("✅ Using fallback root dashboard")
    except ImportError as e2:
        print(f"❌ All dashboard imports failed: {e2}")
        from flask import Flask, jsonify
        app = Flask(__name__)

        @app.route('/')
        def fallback():
            return jsonify({'error': 'Dashboard unavailable', 'message': str(e2)})

@app.route('/health')
def health():
    return {'status': 'healthy', 'database': 'cloud' if DATABASE_URL else 'local'}

application = app

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
