# app.py - Fixed import paths for cloud deployment
"""
Main Flask application for cloud deployment - uses your existing dashboard code
"""

import os
import sys

# Set environment variables BEFORE importing anything
os.environ['DATABASE_URL'] = os.environ.get('DATABASE_URL', 
    'postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres')
os.environ['FLASK_ENV'] = 'production'

# Add paths for proper imports
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
PARENT_DIR = os.path.dirname(PROJECT_ROOT)
sys.path.insert(0, PARENT_DIR)
sys.path.insert(0, PROJECT_ROOT)

# Add dashboard directory specifically
DASHBOARD_DIR = os.path.join(PARENT_DIR, 'dashboard')
sys.path.insert(0, DASHBOARD_DIR)

try:
    # Import the app from mobile_dashboard
    from dashboard.mobile_dashboard import app
    print("Successfully imported dashboard app")
except ImportError as e:
    print(f"Import error: {e}")
    print("Available paths:")
    for path in sys.path:
        print(f"  {path}")
    
    # Fallback: try direct import
    sys.path.append(os.path.join(PARENT_DIR, 'dashboard'))
    from mobile_dashboard import app

# Add cloud-specific health check
@app.route('/health')
def health_check():
    """Health check for cloud platforms"""
    try:
        # Test database connection
        from sqlalchemy import create_engine, text
        engine = create_engine(os.environ['DATABASE_URL'], pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        return {
            'status': 'healthy',
            'environment': 'cloud',
            'database': 'connected'
        }
    except Exception as e:
        return {
            'status': 'unhealthy', 
            'database': 'error',
            'error': str(e)
        }, 500

# For cloud platforms that expect 'application' variable
application = app

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    host = os.environ.get('HOST', '0.0.0.0')
    
    print(f"Starting Bettr Bot on {host}:{port}")
    print(f"Database: {os.environ['DATABASE_URL'][:50]}...")
    app.run(host=host, port=port, debug=False)