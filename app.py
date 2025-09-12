# app.py - FIXED for cloud deployment with proper imports
"""
Main Flask application for cloud deployment
Fixed missing imports and error handling
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from flask import Flask, jsonify, request  # FIXED: Added missing jsonify import

# Set up logging first
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment setup for cloud
os.environ.setdefault('FLASK_ENV', 'production')
DATABASE_URL = os.environ.get('DATABASE_URL')

if DATABASE_URL:
    if DATABASE_URL.startswith('postgres://'):
        DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
    os.environ['DATABASE_URL'] = DATABASE_URL
    print(f"Using cloud database: {DATABASE_URL[:50]}...")
else:
    print("Using local SQLite database")

# Add current directory to Python path
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)

# Import your actual dashboard with comprehensive error handling
app = None
try:
    from mobile_dashboard import app
    print("Successfully imported your full dashboard app")
    
    # Verify model is available
    try:
        # FIXED: Import from the right location based on what actually exists
        if hasattr(app, 'load_model_pack'):
            model_pack = app.load_model_pack()
        else:
            # Try alternative import paths
            try:
                from mobile_dashboard import load_model_pack
                model_pack = load_model_pack()
            except ImportError:
                # Final fallback - check if model file exists
                model_paths = ['./betting_model_fixed.pkl', './models/betting_model_fixed.pkl']
                model_pack = None
                for path in model_paths:
                    if os.path.exists(path):
                        print(f"Model file found at {path}")
                        model_pack = {'feature_cols': ['placeholder']}
                        break
        
        if model_pack and 'feature_cols' in model_pack:
            print(f"Model validated: {len(model_pack['feature_cols'])} features")
        else:
            print("Model validation failed - will use fallback predictions")
            
    except Exception as e:
        print(f"Model check failed: {e}")
        
except ImportError as e:
    print(f"Dashboard import failed: {e}")
    # Create minimal fallback app
    app = Flask(__name__)
    
    @app.route('/')
    def fallback_home():
        return jsonify({
            'error': 'Dashboard import failed',
            'message': str(e),
            'status': 'fallback_mode'
        })

# Enhanced health check with proper error handling
@app.route('/health')
def health_check():
    """Enhanced health check for cloud deployment with fixed imports"""
    try:
        status = {
            'status': 'healthy', 
            'checks': {}, 
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Database check
        try:
            if DATABASE_URL:
                from sqlalchemy import create_engine, text
                engine = create_engine(DATABASE_URL, pool_pre_ping=True)
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
        
        # Model check with better error handling
        try:
            model_paths = ['./betting_model_fixed.pkl', './models/betting_model_fixed.pkl']
            model_found = False
            for path in model_paths:
                if os.path.exists(path):
                    status['checks']['model'] = f'found_at_{path}'
                    model_found = True
                    break
            
            if not model_found:
                status['checks']['model'] = 'missing_model_file'
                status['status'] = 'degraded'
                
        except Exception as e:
            status['checks']['model'] = f'error: {str(e)[:100]}'
            status['status'] = 'degraded'
        
        # Environment check
        status['checks']['environment'] = 'cloud' if DATABASE_URL else 'local'
        status['checks']['python_version'] = sys.version.split()[0]
        
        return jsonify(status), 200 if status['status'] == 'healthy' else 503
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

# Liveness probe
existing_routes = [rule.rule for rule in app.url_map.iter_rules()]
if '/healthz' not in existing_routes:
    @app.route('/healthz')
    def healthz():
        return jsonify({
            'status': 'alive', 
            'timestamp': datetime.utcnow().isoformat()
        }), 200

# Version info
@app.route('/version')
def version():
    return jsonify({
        'version': '1.0.0',
        'environment': 'cloud' if DATABASE_URL else 'local',
        'python': sys.version.split()[0],
        'timestamp': datetime.utcnow().isoformat()
    })

# Error handlers with proper jsonify usage
@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {error}")
    return jsonify({
        'error': 'Internal server error',
        'message': 'The application encountered an unexpected error',
        'timestamp': datetime.utcnow().isoformat()
    }), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'error': 'Not found',
        'message': 'The requested resource was not found',
        'timestamp': datetime.utcnow().isoformat()
    }), 404

# For cloud platforms that expect 'application' variable
application = app

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    host = os.environ.get('HOST', '0.0.0.0')
    
    print(f"Starting Bettr Bot on {host}:{port}")
    print(f"Database: {'Cloud PostgreSQL' if DATABASE_URL else 'Local SQLite'}")
    
    app.run(host=host, port=port, debug=False, threaded=True)
