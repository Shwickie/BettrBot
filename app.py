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
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    # Import the full-featured cloud dashboard
    from cloud.mobile_dashboard import app
    print("✅ Cloud dashboard imported successfully")
except ImportError as e:
    print(f"⚠️ Cloud dashboard import failed: {e}")
    try:
        # Fallback to root mobile_dashboard
        from mobile_dashboard import app
        print("⚠️ Using fallback root dashboard")
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
