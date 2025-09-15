
# Add this to your mobile_dashboard.py to debug what's actually running on server

@app.route('/debug/environment')
def debug_environment():
    """Debug endpoint to see what's running on the server"""
    import sys
    import os
    from datetime import datetime
    
    return {
        "python_version": sys.version,
        "current_time": datetime.now().isoformat(),
        "database_url_configured": bool(os.environ.get('DATABASE_URL')),
        "file_modification": {
            "mobile_dashboard.py": os.path.getmtime(__file__) if os.path.exists(__file__) else "not found"
        },
        "imports_working": {
            "sqlalchemy_text": "text" in str(type(text)) if 'text' in locals() else False,
            "pandas": "pandas" in sys.modules,
            "flask": "flask" in sys.modules
        }
    }
