# mobile_dashboard.py - Cloud-compatible version
import os
import sys
import json
import sqlite3
from datetime import datetime, timedelta
from flask import Flask, render_template_string, jsonify, request, session, redirect, url_for

# Add current directory to path
sys.path.insert(0, os.path.dirname(__file__))

# Database configuration
DATABASE_URL = os.environ.get('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

USE_CLOUD_DB = bool(DATABASE_URL)

if USE_CLOUD_DB:
    from sqlalchemy import create_engine, text, text
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
else:
    engine = None

# Create Flask app
# Add missing load_model_pack function for cloud compatibility
def load_model_pack():
    """Load model pack from available locations"""
    import pickle
    import os
    
    model_paths = [
        './betting_model_fixed.pkl',
        './models/betting_model_fixed.pkl',
        os.path.join(os.path.dirname(__file__), 'betting_model_fixed.pkl')
    ]
    
    for path in model_paths:
        if os.path.exists(path):
            try:
                with open(path, 'rb') as f:
                    model_pack = pickle.load(f)
                print(f"SUCCESS: Loaded model pack from {path}")
                print(f"  Features: {len(model_pack.get('feature_cols', []))}")
                return model_pack
            except Exception as e:
                print(f"Failed to load model from {path}: {e}")
                continue
    
    print("WARNING: No model pack could be loaded")
    return None


app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'bettr-bot-secret-2025')

# Simple user storage (in production, use proper database)
USERS = {
    'admin': {
        'password_hash': 'pbkdf2:sha256:600000$...',  # You'd hash 'admin123'
        'name': 'Admin User',
        'bankroll': 500.0,
        'is_admin': True,
        'bet_history': [],
        'money_transactions': []
    }
}

# Simple templates
LOGIN_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Bettr Bot - Login</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 400px; margin: 100px auto; padding: 20px; }
        .form-group { margin-bottom: 15px; }
        input { width: 100%; padding: 10px; margin-top: 5px; }
        button { width: 100%; padding: 12px; background: #007bff; color: white; border: none; cursor: pointer; }
        .error { color: red; margin-top: 10px; }
    </style>
</head>
<body>
    <h2>Bettr Bot Login</h2>
    <form method="post">
        <div class="form-group">
            <label>Username:</label>
            <input name="username" type="text" required>
        </div>
        <div class="form-group">
            <label>Password:</label>
            <input name="password" type="password" required>
        </div>
        <button type="submit">Login</button>
        {% if error %}<div class="error">{{ error }}</div>{% endif %}
    </form>
</body>
</html>
"""

DASHBOARD_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Bettr Bot - Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 0; padding: 20px; }
        .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 30px; }
        .card { background: #f8f9fa; padding: 20px; margin: 10px 0; border-radius: 8px; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }
        .nav { margin-bottom: 20px; }
        .nav a { margin-right: 15px; color: #007bff; text-decoration: none; }
    </style>
</head>
<body>
    <div class="header">
        <h1>Bettr Bot Dashboard</h1>
        <div>
            <span>{{ username }} | ${{ "%.2f"|format(user.bankroll) }}</span>
            <a href="/logout" style="margin-left: 15px;">Logout</a>
        </div>
    </div>
    
    <div class="nav">
        <a href="/">Dashboard</a>
        <a href="/api/predictions">Predictions</a>
        <a href="/api/rankings">Rankings</a>
    </div>
    
    <div class="stats">
        <div class="card">
            <h3>Account</h3>
            <p>Bankroll: ${{ "%.2f"|format(user.bankroll) }}</p>
            <p>Bets: {{ user.bet_history|length }}</p>
        </div>
        <div class="card">
            <h3>System Status</h3>
            <p>Database: {{ 'Cloud' if use_cloud else 'Local' }}</p>
            <p>Status: Operational</p>
        </div>
    </div>
</body>
</html>
"""

# Routes
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '').lower()
        password = request.form.get('password', '')
        
        # Simple auth check (in production, use proper password hashing)
        if username == 'admin' and password == 'admin123':
            session['username'] = username
            return redirect(url_for('dashboard'))
        else:
            return render_template_string(LOGIN_TEMPLATE, error="Invalid credentials")
    
    return render_template_string(LOGIN_TEMPLATE)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/')
def dashboard():
    if 'username' not in session:
        return redirect(url_for('login'))
    
    username = session['username']
    user = USERS.get(username, USERS['admin'])
    
    return render_template_string(DASHBOARD_TEMPLATE, 
                                  username=username, 
                                  user=user, 
                                  use_cloud=USE_CLOUD_DB)

@app.route('/api/rankings')
def api_rankings():
    """Simple rankings endpoint"""
    sample_rankings = [
        {'rank': 1, 'team': 'Kansas City Chiefs', 'record': '11-6', 'power_score': 6.6},
        {'rank': 2, 'team': 'Buffalo Bills', 'record': '11-6', 'power_score': 5.8},
        {'rank': 3, 'team': 'Baltimore Ravens', 'record': '10-7', 'power_score': 5.2},
        {'rank': 4, 'team': 'Detroit Lions', 'record': '12-5', 'power_score': 4.8},
        {'rank': 5, 'team': 'Philadelphia Eagles', 'record': '11-6', 'power_score': 4.1},
    ]
    return jsonify(sample_rankings)

@app.route('/api/predictions')
def api_predictions():
    """Simple predictions endpoint"""
    sample_predictions = [
        {
            'game_id': '2025-01-13_KC_HOU',
            'matchup': 'Houston Texans @ Kansas City Chiefs',
            'prediction': 'Kansas City Chiefs',
            'confidence': 0.68,
            'game_date': '2025-01-13',
            'game_time': '16:30'
        }
    ]
    return jsonify(sample_predictions)

@app.route('/api/health')
def api_health():
    """Health check endpoint"""
    status = {'status': 'healthy', 'database': 'cloud' if USE_CLOUD_DB else 'local'}
    
    if USE_CLOUD_DB:
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            status['database_connection'] = 'ok'
        except Exception as e:
            status['database_connection'] = f'error: {str(e)[:50]}'
            status['status'] = 'degraded'
    
    return jsonify(status)

# Error handlers
@app.errorhandler(500)
def handle_500_error(e):
    return jsonify({'error': 'Internal server error'}), 500

@app.errorhandler(404)
def handle_404_error(e):
    return jsonify({'error': 'Not found'}), 404

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
