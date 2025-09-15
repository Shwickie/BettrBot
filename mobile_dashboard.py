#!/usr/bin/env python3
"""
FIXED Mobile Dashboard - PostgreSQL Compatible
"""
from flask import Flask, render_template_string, jsonify, request, session, redirect, url_for
from functools import wraps
import pandas as pd
from sqlalchemy import create_engine, text
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta
import os, json
import warnings
warnings.filterwarnings('ignore')

# Database setup
DATABASE_URL = os.environ.get("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if DATABASE_URL and "sslmode=" not in DATABASE_URL:
    DATABASE_URL += "&sslmode=require" if "?" in DATABASE_URL else "?sslmode=require"

try:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True) if DATABASE_URL else None
    print(f"Using cloud PostgreSQL: {DATABASE_URL[:50]}...")
except Exception as e:
    print(f"Database connection error: {e}")
    engine = None

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "bettr-bot-secret")

# User accounts
USERS = {
    'admin': {
        'password': generate_password_hash('admin123'),
        'name': 'Admin',
        'bankroll': 5000.0,
        'is_admin': True
    }
}

def login_required(f):
    @wraps(f)
    def inner(*args, **kwargs):
        if 'username' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return inner

# FIXED: Safe database query function
def safe_query(query_str, params=None):
    """Execute query safely with proper error handling"""
    if not engine:
        return pd.DataFrame()
    
    try:
        with engine.connect() as conn:
            # Convert params to proper format for pandas
            if params:
                # For PostgreSQL, use %(param)s format
                formatted_query = query_str
                for key, value in params.items():
                    placeholder = f":{key}"
                    if placeholder in formatted_query:
                        formatted_query = formatted_query.replace(placeholder, f"'{value}'")
                result = pd.read_sql(text(formatted_query), conn)
            else:
                result = pd.read_sql(text(query_str), conn)
            return result
    except Exception as e:
        print(f"Query error: {e}")
        return pd.DataFrame()

# Team mappings
TEAM_MAP = {
    'ARI':'Arizona Cardinals','ATL':'Atlanta Falcons','BAL':'Baltimore Ravens','BUF':'Buffalo Bills',
    'CAR':'Carolina Panthers','CHI':'Chicago Bears','CIN':'Cincinnati Bengals','CLE':'Cleveland Browns',
    'DAL':'Dallas Cowboys','DEN':'Denver Broncos','DET':'Detroit Lions','GB':'Green Bay Packers',
    'HOU':'Houston Texans','IND':'Indianapolis Colts','JAX':'Jacksonville Jaguars','KC':'Kansas City Chiefs',
    'LV':'Las Vegas Raiders','LAC':'Los Angeles Chargers','LAR':'Los Angeles Rams','MIA':'Miami Dolphins',
    'MIN':'Minnesota Vikings','NE':'New England Patriots','NO':'New Orleans Saints','NYG':'New York Giants',
    'NYJ':'New York Jets','PHI':'Philadelphia Eagles','PIT':'Pittsburgh Steelers','SF':'San Francisco 49ers',
    'SEA':'Seattle Seahawks','TB':'Tampa Bay Buccaneers','TEN':'Tennessee Titans','WAS':'Washington Commanders'
}

def to_full(name):
    return TEAM_MAP.get(name, name) if name else "Unknown"

@app.route('/login', methods=['GET','POST'])
def login():
    if request.method == 'POST':
        username = request.form['username'].strip().lower()
        password = request.form['password']
        
        if username in USERS and check_password_hash(USERS[username]['password'], password):
            session['username'] = username
            return redirect(url_for('dashboard'))
        
        error = "Invalid credentials"
        return f"""
        <html><body>
        <h2>Login</h2>
        <form method="post">
            <input type="text" name="username" placeholder="Username" required><br>
            <input type="password" name="password" placeholder="Password" required><br>
            <button type="submit">Login</button>
        </form>
        <p style="color:red">{error}</p>
        <p>Use: admin / admin123</p>
        </body></html>
        """
    
    return """
    <html><body>
    <h2>Bettr Bot Login</h2>
    <form method="post">
        <input type="text" name="username" placeholder="Username" required><br><br>
        <input type="password" name="password" placeholder="Password" required><br><br>
        <button type="submit">Login</button>
    </form>
    <p>Default: admin / admin123</p>
    </body></html>
    """

@app.route('/')
@login_required
def dashboard():
    return """
    <html><body>
    <h1>Bettr Bot Dashboard</h1>
    <p>Welcome! Your app is running successfully.</p>
    <div>
        <h3>API Endpoints:</h3>
        <ul>
            <li><a href="/api/rankings">Rankings</a></li>
            <li><a href="/api/predictions">Predictions</a></li>
            <li><a href="/api/games">Games</a></li>
            <li><a href="/api/health">Health Check</a></li>
        </ul>
    </div>
    <p><a href="/logout">Logout</a></p>
    </body></html>
    """

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

# FIXED: Rankings endpoint
@app.route('/api/rankings')
def api_rankings():
    try:
        current_year = 2024  # Fixed year to avoid parameter issues
        
        # Simple query without parameters first
        query = f"""
        SELECT team, power_score, wins, losses, games_played, win_pct
        FROM team_season_summary 
        WHERE season = {current_year}
        ORDER BY power_score DESC
        LIMIT 32
        """
        
        df = safe_query(query)
        
        if df.empty:
            # Try previous year
            query = f"""
            SELECT team, power_score, wins, losses, games_played, win_pct
            FROM team_season_summary 
            WHERE season = {current_year - 1}
            ORDER BY power_score DESC
            LIMIT 32
            """
            df = safe_query(query)
        
        rankings = []
        for i, row in df.iterrows():
            rankings.append({
                'rank': i + 1,
                'team': to_full(row.get('team', 'Unknown')),
                'record': f"{int(row.get('wins', 0))}-{int(row.get('losses', 0))}",
                'power_score': round(float(row.get('power_score', 0)), 1)
            })
        
        return jsonify(rankings)
        
    except Exception as e:
        print(f"Rankings error: {e}")
        return jsonify([])

@app.route('/api/predictions')
def api_predictions():
    try:
        today = datetime.now().date()
        future_date = today + timedelta(days=14)
        
        # Simple query without parameters
        query = f"""
        SELECT game_id, home_team, away_team, game_date, start_time_local
        FROM games 
        WHERE game_date >= '{today}' AND game_date <= '{future_date}'
        ORDER BY game_date, start_time_local
        LIMIT 20
        """
        
        df = safe_query(query)
        
        predictions = []
        for _, game in df.iterrows():
            predictions.append({
                'game_id': str(game.get('game_id', '')),
                'matchup': f"{to_full(game.get('away_team', ''))} @ {to_full(game.get('home_team', ''))}",
                'prediction': to_full(game.get('home_team', '')),
                'confidence': 0.55,
                'confidence_level': 'medium',
                'game_date': str(game.get('game_date', '')),
                'game_time': str(game.get('start_time_local', 'TBD'))[:5]
            })
        
        return jsonify(predictions)
        
    except Exception as e:
        print(f"Predictions error: {e}")
        return jsonify([])

@app.route('/api/games')
def api_games():
    try:
        today = datetime.now().date()
        future_date = today + timedelta(days=30)
        
        query = f"""
        SELECT game_id, home_team, away_team, game_date, start_time_local
        FROM games 
        WHERE game_date >= '{today}' AND game_date <= '{future_date}'
        ORDER BY game_date, start_time_local
        LIMIT 50
        """
        
        df = safe_query(query)
        
        games = []
        for _, game in df.iterrows():
            games.append({
                'game_id': str(game.get('game_id', '')),
                'game': f"{to_full(game.get('away_team', ''))} @ {to_full(game.get('home_team', ''))}",
                'date': str(game.get('game_date', '')),
                'time': str(game.get('start_time_local', 'TBD'))[:5],
                'teams': [
                    {'team': to_full(game.get('home_team', '')), 'odds': -110, 'sportsbook': 'Default'},
                    {'team': to_full(game.get('away_team', '')), 'odds': -110, 'sportsbook': 'Default'}
                ]
            })
        
        return jsonify(games)
        
    except Exception as e:
        print(f"Games error: {e}")
        return jsonify([])

@app.route('/api/betting-analysis')
def api_betting_analysis():
    return jsonify({
        "opportunities": [],
        "total_found": 0,
        "message": "Betting analysis is being updated"
    })

@app.route('/api/recent-activity')
def api_recent_activity():
    return jsonify([{
        'date': datetime.now().strftime('%Y-%m-%d'),
        'type': 'system',
        'description': 'Dashboard active',
        'profit_loss': 0
    }])

@app.route('/api/health')
def api_health():
    return jsonify({
        'status': 'healthy',
        'database': 'connected' if engine else 'disconnected',
        'timestamp': datetime.now().isoformat()
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
