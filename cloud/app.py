# app.py - FIXED for cloud deployment
"""
Main Flask application for cloud deployment - FIXED version
"""

import os
import sys
import logging

# Set up logging first
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment setup for cloud
os.environ.setdefault('FLASK_ENV', 'production')
# SIMPLIFIED DATABASE SETUP
# FIXED: Use Session Pooler (IPv4 compatible, port 5432)
DATABASE_URL = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"
# Remove prefix if present
if DATABASE_URL.startswith("DATABASE_URL="):
    DATABASE_URL = DATABASE_URL[13:].strip()

# Ensure proper protocol
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Add psycopg2 driver if not present
if DATABASE_URL.startswith("postgresql://") and "+psycopg2" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://", 1)

# Ensure SSL mode
if "sslmode=" not in DATABASE_URL and DATABASE_URL.startswith("postgresql"):
    separator = "&" if "?" in DATABASE_URL else "?"
    DATABASE_URL += f"{separator}sslmode=require"

USE_CLOUD_DB = DATABASE_URL.startswith("postgresql+psycopg2://")

# Add paths for proper imports
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)

try:
    # Import dashboard app
    from mobile_dashboard import app
    print("✅ Successfully imported dashboard app")
    
    # Verify model is available
    try:
        from mobile_dashboard import load_model_pack
        model_pack = load_model_pack()
        if model_pack and 'feature_cols' in model_pack:
            print(f"✅ Model validated: {len(model_pack['feature_cols'])} features")
        else:
            print("⚠️ Model validation failed - will use fallback predictions")
    except Exception as e:
        print(f"⚠️ Model check failed: {e}")
    
except ImportError as e:
    print(f"❌ Critical import error: {e}")
    sys.exit(1)

# Health check for cloud platforms
@app.route('/health')
def health_check():
    """Enhanced health check for cloud deployment"""
    try:
        # Test database connection
        status = {'status': 'healthy', 'checks': {}}
        
        # Database check
        try:
            if DATABASE_URL:
                from sqlalchemy import create_engine, text
                engine = create_engine(
                    DATABASE_URL if "sslmode=" in DATABASE_URL else f"{DATABASE_URL}&sslmode=require" if "?" in DATABASE_URL else f"{DATABASE_URL}?sslmode=require",
                    pool_pre_ping=True
                )
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                status['checks']['database'] = 'connected'
            else:
                import sqlite3
                conn = sqlite3.connect("data/betting.db")
                conn.execute("SELECT 1")
                conn.close()
                status['checks']['database'] = 'connected'
        except Exception as e:
            status['checks']['database'] = f'error: {str(e)[:100]}'
            status['status'] = 'degraded'
        
        # Model check
        try:
            from mobile_dashboard import load_model_pack
            model = load_model_pack()
            if model and 'feature_cols' in model:
                status['checks']['model'] = f'loaded ({len(model["feature_cols"])} features)'
            else:
                status['checks']['model'] = 'missing or invalid'
                status['status'] = 'degraded'
        except Exception as e:
            status['checks']['model'] = f'error: {str(e)[:100]}'
            status['status'] = 'degraded'
        
        # Environment check
        status['checks']['environment'] = 'cloud' if DATABASE_URL else 'local'
        status['checks']['python_version'] = sys.version.split()[0]
        
        return status, 200 if status['status'] == 'healthy' else 503
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            'status': 'unhealthy',
            'error': str(e)
        }, 500

# Liveness probe (simpler) - check if route already exists
if '/healthz' not in [rule.rule for rule in app.url_map.iter_rules()]:
    @app.route('/healthz')
    def healthz():
        return {'status': 'alive'}, 200

# Version info
@app.route('/version')
def version():
    return {
        'version': '1.0.0',
        'environment': 'cloud' if DATABASE_URL else 'local',
        'python': sys.version.split()[0]
    }

# Error handlers
@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {error}")
    return {
        'error': 'Internal server error',
        'message': 'The application encountered an unexpected error'
    }, 500

@app.errorhandler(404)
def not_found(error):
    return {
        'error': 'Not found',
        'message': 'The requested resource was not found'
    }, 404

# For cloud platforms that expect 'application' variable
application = app

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    host = os.environ.get('HOST', '0.0.0.0')
    
    print(f"🚀 Starting Bettr Bot on {host}:{port}")
    print(f"Database: {'Cloud PostgreSQL' if DATABASE_URL else 'Local SQLite'}")
    
    # Run with gunicorn-compatible settings
    app.run(
        host=host, 
        port=port, 
        debug=False,
        threaded=True
    )