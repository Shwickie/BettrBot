#!/usr/bin/env python3
"""
Bettr Bot Dashboard (updated)
- Uses team_season_summary for power + wins/losses (preseason-supported).
- /api/games now returns per-sportsbook odds so the UI can auto-fill.
- Rankings include a record string (wins-losses).
- Last Update timestamp formatted.
- Admin endpoints for users list and balance adjust.
"""
from __future__ import annotations

from flask import Flask, render_template_string, jsonify, request, session, redirect, url_for, g
from functools import wraps
import pandas as pd
from sqlalchemy import create_engine, text
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta, date
import sqlite3, os, json, threading, time
import numpy as np
import math
from templates import LOGIN_TEMPLATE, HTML_TEMPLATE, AI_CHAT_TEMPLATE
import sys
import os
from flask import Blueprint
import os, sys
from sqlalchemy import create_engine, text
import sqlite3
import time
from functools import wraps
import datetime as dt


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
# robust import so Windows path works when running from /dashboard
# FIXED DATABASE SETUP - Robust PostgreSQL connection handling
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

# Handle Render's postgres:// URLs
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://", 1)

USE_CLOUD_DB = DATABASE_URL.startswith(("postgresql://", "postgresql+psycopg2://"))

if USE_CLOUD_DB:
    print(f"Using cloud database: {DATABASE_URL[:50]}...")
    
    # CRITICAL: More robust connection settings for Render PostgreSQL
    ENGINE = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=280,
        pool_timeout=30,
        pool_size=5,  # Smaller pool
        max_overflow=10,
        connect_args={
            "sslmode": "require",
            "connect_timeout": 30,
            "application_name": "bettrbot_dashboard",
        },
        pool_reset_on_return=None,
        echo=False,
        # Add these parameters to handle SSL issues better
        isolation_level="AUTOCOMMIT"
    )
    
    # Add connection event to handle SSL disconnections
    from sqlalchemy import event
    
    @event.listens_for(ENGINE, "connect")
    def set_postgresql_settings(dbapi_connection, connection_record):
        try:
            with dbapi_connection.cursor() as cursor:
                cursor.execute("SET statement_timeout = '30s'")
                cursor.execute("SET lock_timeout = '10s'")
        except Exception:
            pass  # Ignore if these settings fail
    
    print("Using cloud PostgreSQL with robust connection handling")
else:
    # Local SQLite setup
    DEFAULT_DB = r"E:/Bettr Bot/betting-bot/data/betting.db" if os.name == "nt" else "/tmp/betting.db"
    DB_PATH = os.getenv("BETTR_DB_PATH", DEFAULT_DB)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    ENGINE = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
    print(f"Using local SQLite: {DB_PATH}")

# Set global engine reference
engine = ENGINE
DB_PATH = DATABASE_URL if USE_CLOUD_DB else DEFAULT_DB


try:
    from model.prediction import FixedNFLSystem
    print("Successfully imported FixedNFLSystem from model.prediction")
except ImportError as e:
    print(f"Warning: Could not import FixedNFLSystem: {e}")
    FixedNFLSystem = None

try:
    from model.ai_tools import list_value_bets
except Exception:
    import os, sys
    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
    from model.ai_tools import list_value_bets


try:
    from automated_scheduler import start_background_scheduler, stop_background_scheduler, get_scheduler_status
    SCHEDULER_AVAILABLE = True
except ImportError:
    print("Automated scheduler not available")
    SCHEDULER_AVAILABLE = False
    start_background_scheduler = lambda: False
    stop_background_scheduler = lambda: None
    get_scheduler_status = lambda: {"error": "Scheduler not available"}

try:
    from dashboard.ai_chat_stub import comprehensive_ai_bp
except Exception:
    import os, sys
    sys.path.append(os.path.dirname(__file__))
    from ai_chat_stub import comprehensive_ai_bp


def safe_query(query_str, params=None):
    """Execute query safely with proper PostgreSQL parameter handling"""
    if not ENGINE:
        return pd.DataFrame()
    
    try:
        if USE_CLOUD_DB:
            with ENGINE.connect() as conn:
                if params:
                    result = pd.read_sql(text(query_str), conn, params=params)
                else:
                    result = pd.read_sql(text(query_str), conn)
                return result
        else:
            conn = get_db()
            if params:
                # Convert dict params to list for SQLite
                param_list = list(params.values()) if isinstance(params, dict) else params
                result = pd.read_sql_query(query_str, conn, params=param_list)
            else:
                result = pd.read_sql_query(query_str, conn)
            return result
    except Exception as e:
        print(f"Query error: {e}")
        return pd.DataFrame()
    

def compute_live_records(conn, season: int) -> pd.DataFrame:
    """
    FIXED VERSION: Compute W-L-T for the requested season using game_date.
    Handles both SQLAlchemy connections and raw sqlite3 connections properly.
    """
    # Check what type of connection we have
    is_sqlite_raw = hasattr(conn, 'execute') and not hasattr(conn, 'engine')
    is_sqlalchemy = hasattr(conn, 'engine')
    
    if is_sqlalchemy:
        # SQLAlchemy connection - check if it's PostgreSQL
        USE_CLOUD_DB = 'postgresql' in str(conn.engine.url)
    else:
        # Raw SQLite connection
        USE_CLOUD_DB = False

    # Get games with proper database handling
    if USE_CLOUD_DB:
        # PostgreSQL with SQLAlchemy
        games = pd.read_sql_query(text("""
            WITH g AS (
                SELECT
                    game_id, home_team, away_team,
                    home_score, away_score, week, id, game_date,
                    CASE
                      WHEN EXTRACT(MONTH FROM game_date) >= 8
                        THEN EXTRACT(YEAR FROM game_date)::int
                      ELSE (EXTRACT(YEAR FROM game_date)::int) - 1
                    END AS season_year
                FROM games
            )
            SELECT game_id, home_team, away_team, home_score, away_score, week, id, game_date
            FROM g
            WHERE season_year = :season
              AND home_score IS NOT NULL AND away_score IS NOT NULL
        """), conn, params={"season": season})
    else:
        # SQLite - works with both raw connections and SQLAlchemy
        if is_sqlite_raw:
            # Raw sqlite3.Connection
            games = pd.read_sql_query("""
                WITH g AS (
                    SELECT
                        game_id, home_team, away_team,
                        home_score, away_score, week, id, game_date,
                        CASE
                          WHEN CAST(strftime('%m', game_date) AS INT) >= 8
                            THEN CAST(strftime('%Y', game_date) AS INT)
                          ELSE CAST(strftime('%Y', game_date) AS INT) - 1
                        END AS season_year
                    FROM games
                )
                SELECT game_id, home_team, away_team, home_score, away_score, week, id, game_date
                FROM g
                WHERE season_year = ?
                  AND home_score IS NOT NULL AND away_score IS NOT NULL
            """, conn, params=[season])
        else:
            # SQLAlchemy connection to SQLite
            games = pd.read_sql_query(text("""
                WITH g AS (
                    SELECT
                        game_id, home_team, away_team,
                        home_score, away_score, week, id, game_date,
                        CASE
                          WHEN CAST(strftime('%m', game_date) AS INT) >= 8
                            THEN CAST(strftime('%Y', game_date) AS INT)
                          ELSE CAST(strftime('%Y', game_date) AS INT) - 1
                        END AS season_year
                    FROM games
                )
                SELECT game_id, home_team, away_team, home_score, away_score, week, id, game_date
                FROM g
                WHERE season_year = :season
                  AND home_score IS NOT NULL AND away_score IS NOT NULL
            """), conn, params={"season": season})

    if games.empty:
        return pd.DataFrame(columns=[
            "team","wins","losses","ties","games_played","win_pct",
            "points_for","points_against","point_diff"
        ])

    # Normalize team names to full names BEFORE any processing
    games["home_team"] = games["home_team"].apply(to_full)
    games["away_team"] = games["away_team"].apply(to_full)

    # Handle duplicate games with fallback game_id
    games["game_id"] = games["game_id"].fillna("").astype(str).str.strip()
    games["gid_fallback"] = (
        pd.to_datetime(games["game_date"]).dt.strftime("%Y%m%d") + "_" +
        games["away_team"].str.replace(" ", "") + "_" +
        games["home_team"].str.replace(" ", "")
    )
    games["gid"] = games["game_id"].where(games["game_id"] != "", games["gid_fallback"])

    # Remove duplicates (keeping most recent)
    keep_col = "updated_at" if "updated_at" in games.columns else ("id" if "id" in games.columns else None)
    if keep_col:
        games = games.sort_values(keep_col).drop_duplicates("gid", keep="last")
    else:
        games = games.drop_duplicates("gid", keep="last")

    # Calculate win/loss indicators
    games["home_win"] = (games["home_score"] > games["away_score"]).astype(int)
    games["away_win"] = (games["away_score"] > games["home_score"]).astype(int)
    games["tie"] = (games["home_score"] == games["away_score"]).astype(int)

    # Aggregate home stats
    home_stats = games.groupby("home_team").agg(
        wins=("home_win", "sum"),
        losses=("away_win", "sum"), 
        ties=("tie", "sum"),
        games_played=("home_win", "size"),
        points_for=("home_score", "sum"),
        points_against=("away_score", "sum"),
    ).reset_index()
    home_stats.rename(columns={"home_team": "team"}, inplace=True)

    # Aggregate away stats  
    away_stats = games.groupby("away_team").agg(
        wins=("away_win", "sum"),
        losses=("home_win", "sum"),
        ties=("tie", "sum"), 
        games_played=("away_win", "size"),
        points_for=("away_score", "sum"),
        points_against=("home_score", "sum"),
    ).reset_index()
    away_stats.rename(columns={"away_team": "team"}, inplace=True)

    # Combine home and away stats
    all_teams = set(home_stats["team"].unique()) | set(away_stats["team"].unique())
    
    records = []
    for team in all_teams:
        home_row = home_stats[home_stats["team"] == team]
        away_row = away_stats[away_stats["team"] == team]
        
        # Get stats or default to 0
        home_wins = home_row["wins"].iloc[0] if not home_row.empty else 0
        home_losses = home_row["losses"].iloc[0] if not home_row.empty else 0
        home_ties = home_row["ties"].iloc[0] if not home_row.empty else 0
        home_games = home_row["games_played"].iloc[0] if not home_row.empty else 0
        home_pf = home_row["points_for"].iloc[0] if not home_row.empty else 0
        home_pa = home_row["points_against"].iloc[0] if not home_row.empty else 0
        
        away_wins = away_row["wins"].iloc[0] if not away_row.empty else 0
        away_losses = away_row["losses"].iloc[0] if not away_row.empty else 0
        away_ties = away_row["ties"].iloc[0] if not away_row.empty else 0
        away_games = away_row["games_played"].iloc[0] if not away_row.empty else 0
        away_pf = away_row["points_for"].iloc[0] if not away_row.empty else 0
        away_pa = away_row["points_against"].iloc[0] if not away_row.empty else 0
        
        # Combine totals
        total_wins = int(home_wins + away_wins)
        total_losses = int(home_losses + away_losses)
        total_ties = int(home_ties + away_ties)
        total_games = int(home_games + away_games)
        total_pf = int(home_pf + away_pf)
        total_pa = int(home_pa + away_pa)
        
        # Calculate win percentage
        if total_games > 0:
            win_pct = (total_wins + 0.5 * total_ties) / total_games
        else:
            win_pct = 0.0
            
        records.append({
            "team": team,
            "wins": total_wins,
            "losses": total_losses, 
            "ties": total_ties,
            "games_played": total_games,
            "win_pct": win_pct,
            "points_for": total_pf,
            "points_against": total_pa,
            "point_diff": total_pf - total_pa
        })

    result = pd.DataFrame(records)
    return result


def test_fixed_function():
    """Test the fixed function with your database"""
    import os
    import sqlite3
    from sqlalchemy import create_engine
    
    # Database setup (same as your mobile_dashboard.py)
    DATABASE_URL = os.environ.get("DATABASE_URL")
    if DATABASE_URL and DATABASE_URL.startswith('postgres://'):
        DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
    
    USE_CLOUD_DB = bool(DATABASE_URL)
    SEASON = 2025
    
    if USE_CLOUD_DB:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        with engine.connect() as conn:
            records = compute_live_records(conn, SEASON)
    else:
        local_db = r"E:/Bettr Bot/betting-bot/data/betting.db"
        conn = sqlite3.connect(local_db)
        records = compute_live_records(conn, SEASON)
        conn.close()
    
    print(f"\nFINAL RESULTS:")
    print(f"Total teams: {len(records)}")
    
    # Show teams with games played
    teams_with_games = records[records["games_played"] > 0].sort_values("games_played", ascending=False)
    print(f"\nTeams with completed games:")
    for _, team in teams_with_games.iterrows():
        print(f"  {team['team']}: {team['wins']}-{team['losses']}-{team['ties']} ({team['games_played']} games)")
    
    return records

def get_engine():
    """Return the global SQLAlchemy Engine for BOTH SQLite and Postgres."""
    return ENGINE

def df_query(sqlite_sql: str, pg_sql: str, params: dict):
    """Run the right SQL flavor for SQLite vs Postgres and return a DataFrame."""
    if USE_CLOUD_DB:
        return pd.read_sql_query(text(pg_sql), get_engine(), params=params)
    else:
        # use positional ? for sqlite
        # infer order from common param names
        order = []
        if "season" in params: order.append(params["season"])
        if "start" in params: order.append(params["start"])
        if "end" in params: order.append(params["end"])
        return pd.read_sql_query(sqlite_sql, get_db(), params=order)
# Add these functions to mobile_dashboard.py if they're missing
def normalize_team_for_odds_lookup(team_name):
    """
    Return the format that matches your odds table (abbreviations like ATL, NYG)
    """
    if not team_name:
        return ''
    
    team_str = str(team_name).strip()
    
    # If already an abbreviation, return it
    if team_str in ABBR_TO_FULL:
        return team_str
    
    # If it's a full name, convert to abbreviation  
    if team_str in FULL_TO_ABBR:
        return FULL_TO_ABBR[team_str]
    
    # Handle canonical mappings
    team_upper = team_str.upper()
    team_upper = CANON.get(team_upper, team_upper)
    
    return team_upper

def get_full_team_name(team_abbr):
    """
    Convert team abbreviation to full name
    This was referenced as 'full_team_name' in the error logs
    """
    if not team_abbr:
        return "Unknown"
    
    team_str = str(team_abbr).strip().upper()
    
    # Apply canonical mapping first
    team_str = CANON.get(team_str, team_str)
    
    # Return full name if we have it
    return ABBR_TO_FULL.get(team_str, team_abbr)

# Also make sure these are available as aliases
full_team_name = get_full_team_name

def fix_user_data_structure():
    """Fix any inconsistencies in user data structure"""
    global USERS
    fixed_count = 0
    
    for username, user_data in USERS.items():
        original_data = dict(user_data)  # backup
        
        # Fix field name mismatches
        field_mappings = {
            'deposits': 'total_deposits',
            'withdrawals': 'total_withdrawals', 
            'bet_profit_loss': 'betting_profit_loss',
            'transactions': 'money_transactions'
        }
        
        for old_field, new_field in field_mappings.items():
            if old_field in user_data and new_field not in user_data:
                user_data[new_field] = user_data.pop(old_field)
                print(f"  Fixed {username}: {old_field} -> {new_field}")
                fixed_count += 1
        
        # Ensure all required fields exist with defaults
        defaults = {
            'name': username.title(),
            'bankroll': 0.0,
            'total_deposits': 0.0,
            'total_withdrawals': 0.0,
            'betting_profit_loss': 0.0,
            'bet_history': [],
            'money_transactions': [],
            'is_admin': False
        }
        
        for field, default_value in defaults.items():
            if field not in user_data:
                user_data[field] = default_value
                print(f"  Added missing field {username}.{field} = {default_value}")
                fixed_count += 1
    
    if fixed_count > 0:
        save_user_accounts(USERS)
        print(f"ðŸ”§ Fixed {fixed_count} user data issues")
    
    return fixed_count

def normalize_team_for_api_games(team_name):
    """
    Ensure team names in /api/games match the format used in odds table
    """
    if not team_name:
        return ''
    
    team_str = str(team_name).strip()
    
    # If it's already an abbreviation that exists in our mapping, return it
    if team_str in ABBR_TO_FULL:
        return team_str
    
    # If it's a full name, convert to abbreviation for odds lookup
    if team_str in FULL_TO_ABBR:
        return FULL_TO_ABBR[team_str]
    
    # Apply canonical mappings for edge cases
    team_upper = team_str.upper()
    team_upper = CANON.get(team_upper, team_upper)
    
    return team_upper


def _normalize_model_pack(sysobj):
    """Ensure sysobj.model exists and sysobj.model_data['model'] is set."""
    md = sysobj.model_data if isinstance(getattr(sysobj, "model_data", None), dict) else {}
    model = None

    # 1) Try common keys inside the dict
    if isinstance(md, dict):
        for key in (
            "model", "best_model", "calibrated_model", "cal_model",
            "final_model", "rf_model", "clf", "estimator", "pipeline", "sk_model"
        ):
            m = md.get(key)
            if m is not None and (hasattr(m, "predict_proba") or hasattr(m, "predict")):
                model = m
                # Standardize the key expected by the rest of the app:
                md["model"] = m
                break

    # 2) Fall back to attribute on the system itself
    if model is None and hasattr(sysobj, "model") and (
        hasattr(sysobj.model, "predict_proba") or hasattr(sysobj.model, "predict")
    ):
        model = sysobj.model
        if isinstance(md, dict):
            md.setdefault("model", model)

    # 3) If the entire model_data is actually the estimator (rare)
    if model is None and md and (hasattr(md, "predict_proba") or hasattr(md, "predict")):
        model = md
        sysobj.model_data = {"model": model}

    # 4) Last resort: nothing found â€“ keep it None but avoid KeyErrors later
    if model is None:
        if not isinstance(sysobj.model_data, dict):
            sysobj.model_data = {}
        sysobj.model_data.setdefault("model", None)

    # Standardize feature_cols too
    if isinstance(sysobj.model_data, dict):
        if not sysobj.model_data.get("feature_cols"):
            # best-effort from sklearn
            fe = getattr(model, "feature_names_in_", None)
            if fe is not None:
                sysobj.model_data["feature_cols"] = list(fe)
            else:
                sysobj.model_data.setdefault("feature_cols", [])

    # also mirror onto sysobj.model for direct use
    sysobj.model = model


_model_pack = None
_ml_prediction_system = None

def get_ml_prediction_system():
    global _ml_prediction_system
    if _ml_prediction_system is None and FixedNFLSystem is not None:
        try:
            _ml_prediction_system = FixedNFLSystem()
            # Harden the pack so downstream code can always use ['model'] safely
            _normalize_model_pack(_ml_prediction_system)

            # keep your existing prints
            print("ML Prediction System initialized successfully")
            md = _ml_prediction_system.model_data or {}
            auc = md.get("model_metrics", {}).get("RandomForest", {}).get("auc", "Unknown")
            print(f"  Model AUC: {auc}")
        except Exception as e:
            print(f"Failed to initialize ML prediction system: {e}")
            _ml_prediction_system = None
    return _ml_prediction_system


# -----------------
# Auth decorators
# -----------------

def login_required(f):
    @wraps(f)
    def inner(*args, **kwargs):
        if 'username' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return inner

def admin_required(f):
    @wraps(f)
    def inner(*args, **kwargs):
        if 'username' not in session:
            return redirect(url_for('login'))
        if not USERS.get(session['username'], {}).get('is_admin', False):
            return jsonify({'error': 'Admin access required'}), 403
        return f(*args, **kwargs)
    return inner

# ====== Trained Model loader (cached) ======
import pickle

# near the imports
import os, pickle, logging
logger = logging.getLogger(__name__)

def get_dashboard_model_path():
    candidates = [
        os.environ.get("BETTR_MODEL_PKL"),
        os.path.join(os.getcwd(), "betting_model_fixed.pkl"),  # Cloud deployment
        os.path.join(os.path.dirname(__file__), "betting_model_fixed.pkl"),
        os.path.join(os.path.dirname(__file__), "..", "models", "betting_model_fixed.pkl"),
    ]
    
    for path in candidates:
        if path and os.path.exists(path):
            return path
    
    return None

def safe_sql_query(query, conn, params=None):
    """Helper to safely execute SQL queries on both SQLite and PostgreSQL"""
    try:
        if USE_CLOUD_DB:
            # PostgreSQL - use named parameters with text()
            return pd.read_sql_query(text(query), conn, params=params or {})
        else:
            # SQLite - use positional parameters without text()
            return pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        print(f"SQL Query Error: {e}")
        return pd.DataFrame()

def load_model_pack():
    global _model_pack
    if _model_pack is not None:
        return _model_pack

    path = get_dashboard_model_path()
    if not path:
        logger.error("CRITICAL: Model pack not found!")
        _model_pack = None
        return None

    try:
        with open(path, "rb") as f:
            _model_pack = pickle.load(f)
        
        # VALIDATE the model pack
        required_keys = ['model', 'feature_cols']
        missing_keys = [key for key in required_keys if key not in _model_pack]
        
        if missing_keys:
            logger.error(f"CRITICAL: Model pack missing keys: {missing_keys}")
            _model_pack = None
            return None
        
        # Add scaler if missing
        if 'scaler' not in _model_pack:
            _model_pack['scaler'] = None
            
        logger.info(f"SUCCESS: Loaded model pack from {path}")
        logger.info(f"  Features: {len(_model_pack.get('feature_cols', []))}")
        return _model_pack
        
    except Exception as e:
        logger.error(f"CRITICAL: Failed to load model pack from {path}: {e}")
        import traceback
        traceback.print_exc()
        _model_pack = None
        return None

def build_features_for_games(conn, games_df: pd.DataFrame) -> pd.DataFrame:
    """Build the same feature columns the trainer used, for each game (home-team perspective)."""
    if games_df is None or games_df.empty:
        return pd.DataFrame()

    ts = pd.read_sql_query("""
        SELECT season, team,
               power_score AS power,
               win_pct,
               avg_points_for  AS off,
               avg_points_against AS def
        FROM team_season_summary
    """, conn)

    tmp = games_df.rename(columns={'home': 'home_team', 'away': 'away_team'}).copy()
    tmp["season"] = pd.to_datetime(tmp["game_date"]).dt.year

    home = ts.rename(columns={
        "team": "home_team", "power": "home_power",
        "off": "home_offense", "def": "home_defense",
        "win_pct": "home_win_pct"
    })
    tmp = tmp.merge(home, on=["season", "home_team"], how="left")

    away = ts.rename(columns={
        "team": "away_team", "power": "away_power",
        "off": "away_offense", "def": "away_defense",
        "win_pct": "away_win_pct"
    })
    tmp = tmp.merge(away, on=["season", "away_team"], how="left")

    # engineered features used by trainer
    tmp["power_diff"]   = tmp["home_power"]   - tmp["away_power"]
    tmp["win_pct_diff"] = tmp["home_win_pct"] - tmp["away_win_pct"]
    tmp["offense_diff"] = tmp["home_offense"] - tmp["away_offense"]
    tmp["defense_diff"] = tmp["home_defense"] - tmp["away_defense"]
    tmp["form_diff"]    = 0.0
    tmp["home_field_advantage"] = 3.0

    dt = pd.to_datetime(tmp["game_date"])
    tmp["month"] = dt.dt.month
    tmp["day_of_week"] = dt.dt.weekday

    # placeholders for schema stability
    tmp["home_injury_impact"] = 0.0
    tmp["away_injury_impact"] = 0.0
    tmp["home_qb_injury"] = 0.0
    tmp["away_qb_injury"] = 0.0
    tmp["home_recent_form"] = 0.0
    tmp["away_recent_form"] = 0.0
    tmp["h2h_games"] = 0.0
    tmp["home_h2h_win_rate"] = 0.5
    return tmp



# =========================
# DB PATH (single source)
# =========================

app = Flask(__name__)
# register AI blueprint at /api/ai-*
app.register_blueprint(comprehensive_ai_bp, url_prefix='')
app.secret_key = 'bettr-bot-enhanced-2025'

# --- ADD: one-time indexes + WAL ---
def ensure_indexes():
    if USE_CLOUD_DB:
        return 
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("CREATE INDEX IF NOT EXISTS idx_games_date_time ON games(game_date, start_time_local)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_tss_season_team ON team_season_summary(season, team)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_odds_market_ts ON odds(market, timestamp)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_odds_game_team_book_ts ON odds(game_id, team, sportsbook, timestamp)")
        con.commit()
    finally:
        con.close()

_initialized = False

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Prefer a persistent path on Render; allow override by env
USERS_PATH_DEFAULT = os.environ.get("BETTR_USERS_PATH")
if not USERS_PATH_DEFAULT:
    USERS_PATH_DEFAULT = "/cloud/user_accounts.json" if os.path.isdir("/cloud") else os.path.join(BASE_DIR, "user_accounts.json")
# FIX - Replace with:
def get_user_data_path():
    candidates = [
        os.path.join(os.getcwd(), "user_accounts.json"),
        os.path.join(os.path.dirname(__file__), "user_accounts.json"),
        os.path.join(os.path.dirname(__file__), "..", "user_accounts.json"),
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    return os.path.join(os.getcwd(), "user_accounts.json")

USER_DATA_FILE = get_user_data_path()
os.makedirs(os.path.dirname(USER_DATA_FILE), exist_ok=True)

# Create an empty file once so load() doesn't blow up
if not os.path.exists(USER_DATA_FILE):
    with open(USER_DATA_FILE, "w") as f:
        f.write("{}")

app.secret_key = os.environ.get("FLASK_SECRET", "bettr-bot-enhanced-2025")
print(f"Looking for user file at: {USER_DATA_FILE}")
print(f"File exists: {os.path.exists(USER_DATA_FILE)}")
print(f"Current working directory: {os.getcwd()}")
print(f"Database connection: {'PostgreSQL' if USE_CLOUD_DB else 'SQLite'}")
print(f"Users loaded: {len(USERS) if 'USERS' in globals() else 'Not loaded yet'}")


@app.route('/debug/routes')
def list_routes():
    import urllib
    output = []
    for rule in app.url_map.iter_rules():
        methods = ','.join(rule.methods)
        line = urllib.parse.unquote("{:50s} {:20s} {}".format(rule.endpoint, methods, rule))
        output.append(line)
    
    return '<pre>' + '\n'.join(sorted(output)) + '</pre>'


@app.route('/ai')
@login_required
def ai_page():
    return render_template_string(AI_CHAT_TEMPLATE)



# --------------
# Team mappings
# --------------
ABBR_TO_FULL = {
    'ARI': 'Arizona Cardinals','ATL': 'Atlanta Falcons','BAL': 'Baltimore Ravens','BUF': 'Buffalo Bills',
    'CAR': 'Carolina Panthers','CHI': 'Chicago Bears','CIN': 'Cincinnati Bengals','CLE': 'Cleveland Browns',
    'DAL': 'Dallas Cowboys','DEN': 'Denver Broncos','DET': 'Detroit Lions','GB': 'Green Bay Packers',
    'HOU': 'Houston Texans','IND': 'Indianapolis Colts','JAX': 'Jacksonville Jaguars','KC': 'Kansas City Chiefs',
    'LV': 'Las Vegas Raiders','LAC': 'Los Angeles Chargers','LA': 'Los Angeles Rams','LAR': 'Los Angeles Rams','MIA': 'Miami Dolphins',
    'MIN': 'Minnesota Vikings','NE': 'New England Patriots','NO': 'New Orleans Saints','NYG': 'New York Giants',
    'NYJ': 'New York Jets','PHI': 'Philadelphia Eagles','PIT': 'Pittsburgh Steelers','SF': 'San Francisco 49ers',
    'SEA': 'Seattle Seahawks','TB': 'Tampa Bay Buccaneers','TEN': 'Tennessee Titans','WAS': 'Washington Commanders'
}
FULL_NAMES = set(ABBR_TO_FULL.values())
TEAM_TO_FULL = {**{n: n for n in FULL_NAMES}, **ABBR_TO_FULL}
# Canonical team maps
FULL_TO_ABBR = {v: k for k, v in ABBR_TO_FULL.items()}
CANON = {
    'LA':'LAR', 'STL':'LAR',  # Rams
    'SD':'LAC',               # Chargers (old)
    'OAK':'LV',               # Raiders (old)
    'JAC':'JAX',              # Jaguars alt
    'WSH':'WAS'               # Commanders old
}

def to_abbr(x: str | None) -> str:
    x = (x or '').strip()
    if not x:
        return ''
    if x in FULL_TO_ABBR:          # full name -> abbr
        return FULL_TO_ABBR[x]
    xu = x.upper()
    return CANON.get(xu, xu)       # already abbr -> canon

# --- Odds helpers (keep everything in American odds) ---
def normalize_american_odds(raw):
    """
    Accepts American like '+110'/'-120' or decimal like 1.91/2.35
    and returns an int American price (e.g., 110, -120).
    """
    try:
        s = str(raw).strip()
        if s.startswith('+'):
            s = s[1:]
        v = float(s)
    except Exception:
        return None

    # Looks like decimal odds (typical range 1.01 - ~10.0)
    if 1.01 <= v <= 10.0:
        if v >= 2.0:
            return int(round((v - 1) * 100))
        else:
            return int(round(-100 / (v - 1)))

    # Already American
    return int(round(v))

# --- ADD: cached power map (reused by endpoints) ---
POWER_CACHE = {"ts": 0.0, "map": {}}
POWER_TTL = 60  # seconds

def get_power_map_cached(conn):
    now = time.time()
    if POWER_CACHE["map"] and (now - POWER_CACHE["ts"] < POWER_TTL):
        return POWER_CACHE["map"]

    df = get_unified_power_scores(conn)  # you already have this
    pmap = {to_full(t): float(ap) for t, ap in zip(df['team'], df['adj_power'])}
    # allow both full + abbr lookups
    for abbr, full in ABBR_TO_FULL.items():
        if full in pmap:
            pmap[abbr] = pmap[full]

    POWER_CACHE["map"] = pmap
    POWER_CACHE["ts"] = now
    return pmap


def get_unified_power_scores(conn):
    """
    Returns a DataFrame with columns:
      team (full name), power_score, games_played, win_pct,
      injury_impact, qb_risk, adj_power
    """
    season, _ = current_phase_and_season()

    # 1) Base power from team_season_summary (can be preseason seeded)
    try:
        if USE_CLOUD_DB:
            base = safe_query(
                "SELECT team, power_score, games_played, win_pct FROM team_season_summary WHERE season = :season",
                {"season": season}
            )
        else:
            base = pd.read_sql_query(
                "SELECT team, power_score, games_played, win_pct FROM team_season_summary WHERE season = ?",
                get_db(), params=[season]
            )
    except Exception:
        base = pd.DataFrame(columns=["team","power_score","games_played","win_pct"])

    # Normalize team names to FULL
    base["team"] = base["team"].map(to_full)

    # 2) Live records from games (overrides games_played & win_pct)
    live = compute_live_records(conn, season)
    df = base.merge(live[["team","games_played","win_pct"]], on="team", how="left", suffixes=("", "_live"))
    df["games_played"] = df["games_played_live"].fillna(df["games_played"])
    df["win_pct"]      = df["win_pct_live"].fillna(df["win_pct"])
    df.drop(columns=[c for c in ["games_played_live","win_pct_live"] if c in df.columns], inplace=True)

    # 3) Injury view
    try:
        inj = load_injury_impact_from_detail(conn)[["team","injury_impact","qb_risk"]]
        inj["team"] = inj["team"].map(to_full)
    except Exception:
        inj = pd.DataFrame(columns=["team","injury_impact","qb_risk"])

    df = df.merge(inj, on="team", how="left")
    df["injury_impact"] = df["injury_impact"].fillna(0.0)
    df["qb_risk"] = df["qb_risk"].fillna(0.0)

    # 4) Small form component from win_pct (won’t blow up preseason)
    df["form_component"] = np.where(
        df["games_played"].fillna(0) > 0,
        (df["win_pct"].fillna(0.5) - 0.5) * 20,
        0.0
    )

    # 5) Final adjusted power (keep your scale/weights)
    df["adj_power"] = (
        df["power_score"].fillna(0.0) * 1.0 +
        df["form_component"] * 0.20 -
        df["injury_impact"] * 0.05
    )

    return df[["team","power_score","games_played","win_pct","injury_impact","qb_risk","adj_power"]]

# --------------
# Helpers
# --------------

def current_season_year(today: date | None = None) -> int:
    d = today or date.today()
    return d.year if d.month >= 8 else d.year - 1

def current_phase_and_season(today: date | None = None):
    d = today or date.today()
    PRESEASON_START = date(d.year, 8, 1)
    PRESEASON_END   = date(d.year, 9, 7)
    season = current_season_year(d)
    phase = 'preseason' if PRESEASON_START <= d <= PRESEASON_END else 'regular'
    return season, phase

def compute_team_records(conn, season_year: int) -> pd.DataFrame:
    """
    Build W-L-T and GP from scored games in the 'games' table for the given NFL season.
    Works in SQLite.
    """
    sql = text("""
    WITH g AS (
      SELECT
        CASE WHEN CAST(strftime('%m', game_date) AS INTEGER) >= 8
             THEN CAST(strftime('%Y', game_date) AS INTEGER)
             ELSE CAST(strftime('%Y', game_date) AS INTEGER) - 1
        END AS season_year,
        home_team, away_team, home_score, away_score
      FROM games
      WHERE home_score IS NOT NULL AND away_score IS NOT NULL
    ),
    home AS (
      SELECT season_year, home_team AS team,
             SUM(CASE WHEN home_score > away_score THEN 1 ELSE 0 END) AS wins,
             SUM(CASE WHEN home_score < away_score THEN 1 ELSE 0 END) AS losses,
             SUM(CASE WHEN home_score = away_score THEN 1 ELSE 0 END) AS ties,
             COUNT(*) AS gp
      FROM g
      GROUP BY season_year, home_team
    ),
    away AS (
      SELECT season_year, away_team AS team,
             SUM(CASE WHEN away_score > home_score THEN 1 ELSE 0 END) AS wins,
             SUM(CASE WHEN away_score < home_score THEN 1 ELSE 0 END) AS losses,
             SUM(CASE WHEN away_score = home_score THEN 1 ELSE 0 END) AS ties,
             COUNT(*) AS gp
      FROM g
      GROUP BY season_year, away_team
    ),
    tot AS (
      SELECT season_year, team,
             SUM(wins)   AS wins,
             SUM(losses) AS losses,
             SUM(ties)   AS ties,
             SUM(gp)     AS games_played
      FROM (SELECT * FROM home UNION ALL SELECT * FROM away)
      GROUP BY season_year, team
    )
    SELECT team, wins, losses, ties, games_played
    FROM tot
    WHERE season_year = :season
    """)
    df = pd.read_sql_query(sql, conn, params={'season': season_year})
    if df.empty:
        # still return a frame with expected columns
        df = pd.DataFrame(columns=['team', 'wins', 'losses', 'ties', 'games_played'])
    for col in ('wins','losses','ties','games_played'):
        if col not in df.columns:
            df[col] = 0
    df['wins'] = df['wins'].fillna(0).astype(int)
    df['losses'] = df['losses'].fillna(0).astype(int)
    df['ties'] = df['ties'].fillna(0).astype(int)
    df['games_played'] = df['games_played'].fillna(0).astype(int)
    df['win_pct'] = df.apply(lambda r: (r['wins'] / r['games_played']) if r['games_played'] else 0.0, axis=1)
    return df

# ---------- Injury impact from ai_injury_validation_detail (with superstar weighting) ----------
# Designation weights (severity)
DESIG_W = {
    'INJURED RESERVE': 1.00, 'IR': 1.00,
    'OUT': 0.90, 'PUP': 0.80,
    'DOUBTFUL': 0.60,
    'QUESTIONABLE': 0.30
}

# Position multipliers (impact on team strength)
POS_W = {
    'QB': 3.0,
    'WR': 1.5, 'RB': 1.5, 'TE': 1.4,
    'CB': 1.3, 'S': 1.2, 'LB': 1.1, 'EDGE': 1.2, 'DE': 1.2, 'DT': 1.1,
    'T': 1.0, 'G': 0.9, 'C': 0.9, 'OL': 0.9,
    'FB': 0.6, 'K': 0.4, 'P': 0.4, 'LS': 0.3
}

# Default superstar multipliers (fallback if no DB table exists)
# You can add/remove freely; values are multipliers applied on top of POS_W and DESIG_W.
DEFAULT_SUPERSTARS = {
    # WR / skill
    "Tyreek Hill": 1.6, "Justin Jefferson": 1.6, "A.J. Brown": 1.5, "Stefon Diggs": 1.5,
    "Jaylen Waddle": 1.4, "CeeDee Lamb": 1.5, "Ja'Marr Chase": 1.6,
    # RB / TE examples
    "Nick Chubb": 1.5, "Rachaad White": 1.2, "Hunter Henry": 1.2,
    # QBs (still dominated by QB position weight; this is a small extra nudge)
    "Jordan Love": 1.15, "Tyrod Taylor": 1.10,
    # Defense examples
    "Micah Parsons": 1.6, "Sauce Gardner": 1.5, "Trevon Diggs": 1.4
}

def _normalize_text(x: str | None) -> str:
    return (x or '').strip()

def _normalize_pos(p: str | None) -> str:
    p = (p or '').strip().upper()
    if p in ('OT','OG','OC'):  # sometimes OL specifics
        return p[-1]            # map OT->T, OG->G, OC->C
    if p in ('LT','RT'): return 'T'
    if p in ('LG','RG'): return 'G'
    if p == 'OL': return 'OL'
    return p

def _normalize_desig(d: str | None) -> str:
    d = (d or '').strip().upper()
    # unify a few common variants
    if d in ('IR', 'INJURED RESERVE'): return 'IR'
    return d

def load_superstars(conn) -> dict[str, float]:
    """
    Optional: if you create a table ai_star_players(name TEXT PRIMARY KEY, weight REAL),
    we'll load it, else fall back to DEFAULT_SUPERSTARS.
    """
    import pandas as pd
    try:
        df = pd.read_sql_query("SELECT name, weight FROM ai_star_players", conn)
        if not df.empty:
            d = {}
            for _, r in df.iterrows():
                nm = _normalize_text(r['name'])
                wt = float(r.get('weight', 1.3) or 1.3)
                if nm:
                    d[nm] = wt
            return d
    except Exception:
        pass
    return DEFAULT_SUPERSTARS

def load_injury_impact_from_detail(conn):
    """
    Reads ai_injury_validation_detail and returns per-team injury metrics:
    columns: team, injury_impact, total_injuries, qb_risk, skill_position_risk
    """
    import pandas as pd

    try:
        if USE_CLOUD_DB:
            with ENGINE.connect() as db_conn:
                table_check = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name='ai_injury_validation_detail'")
                ).fetchone()  # if using SQLAlchemy Connection
                if not table_check:
                    print("ai_injury_validation_detail table not found")
                    return pd.DataFrame(columns=['team','injury_impact','total_injuries','qb_risk','skill_position_risk'])

                df = pd.read_sql(text("""
                    SELECT
                        COALESCE(team_ai, team_inj)          AS team,
                        COALESCE(position, '')               AS position,
                        COALESCE(designation, '')            AS designation,
                        COALESCE(inj_name, roster_name, '')  AS player,
                        COALESCE(inj_missing_team, 0)        AS inj_missing_team,
                        COALESCE(roster_missing_team, 0)     AS roster_missing_team,
                        COALESCE(team_mismatch, 0)           AS team_mismatch
                    FROM ai_injury_validation_detail
                """), db_conn)
        else:
            # Check if table exists in SQLite (works for sqlite3 or SQLAlchemy)
            try:
                if isinstance(conn, sqlite3.Connection):
                    table_check = conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name='ai_injury_validation_detail'"
                    ).fetchone()
                else:
                    # SQLAlchemy Connection against SQLite
                    table_check = conn.execute(
                        text("SELECT name FROM sqlite_master WHERE type='table' AND name='ai_injury_validation_detail'")
                    ).fetchone()
            except Exception:
                table_check = None

            if not table_check:
                print("ai_injury_validation_detail table not found")
                return pd.DataFrame(columns=['team','injury_impact','total_injuries','qb_risk','skill_position_risk'])

            df = pd.read_sql_query("""
                SELECT
                    COALESCE(team_ai, team_inj)          AS team,
                    COALESCE(position, '')               AS position,
                    COALESCE(designation, '')            AS designation,
                    COALESCE(inj_name, roster_name, '')  AS player,
                    COALESCE(inj_missing_team, 0)        AS inj_missing_team,
                    COALESCE(roster_missing_team, 0)     AS roster_missing_team,
                    COALESCE(team_mismatch, 0)           AS team_mismatch
                FROM ai_injury_validation_detail
            """, conn)
    except Exception as e:
        print(f"Error loading injury data: {e}")
        return pd.DataFrame(columns=['team','injury_impact','total_injuries','qb_risk','skill_position_risk'])
    if df.empty:
        return pd.DataFrame(columns=['team','injury_impact','total_injuries','qb_risk','skill_position_risk'])

    # Keep only validated rows actually on the team
    df = df[(df['inj_missing_team'] == 0) &
            (df['roster_missing_team'] == 0) &
            (df['team_mismatch'] == 0)].copy()

    # ---- normalize fields ----
    df['team'] = df['team'].map(lambda t: to_abbr((t or '').strip()))
    df['position'] = df['position'].map(_normalize_pos)
    df['designation'] = df['designation'].map(_normalize_desig)
    df['player'] = df['player'].map(_normalize_text)

    # Superstar table (optional) or defaults
    STAR = load_superstars(conn)

    # Row impact
    def row_impact(r):
        des_w = DESIG_W.get(r['designation'], 0.30)
        pos_w = POS_W.get(r['position'], 1.0)
        star_w = STAR.get(r['player'], 1.0)
        return des_w * pos_w * star_w

    df['impact'] = df.apply(row_impact, axis=1)

    # ---- remove FutureWarning: use Index.intersection, not '&' ----
    qb_idx = df.index[df['position'] == 'QB']
    skill_idx = df.index[df['position'].isin(['WR','RB','TE'])]

    agg = df.groupby('team').agg(
        injury_impact=('impact', 'sum'),
        total_injuries=('player', 'count'),
        qb_risk=('position', lambda s: float(df.loc[s.index.intersection(qb_idx), 'impact'].sum())),
        skill_position_risk=('position', lambda s: float(df.loc[s.index.intersection(skill_idx), 'impact'].sum())),
    ).reset_index()

    for c in ('injury_impact','total_injuries','qb_risk','skill_position_risk'):
        agg[c] = agg[c].fillna(0)

    return agg[['team','injury_impact','total_injuries','qb_risk','skill_position_risk']]

# Per-request sqlite3 connection (same DB as SQLAlchemy)
def get_db():
    if USE_CLOUD_DB:
        # Return the global engine for cloud DB
        return ENGINE
    else:
        if not hasattr(g, "_db"):
            g._db = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
            g._db.row_factory = sqlite3.Row
        return g._db

def db_retry(max_retries=3):
    """Decorator to retry database operations on connection failures"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if "SSL connection has been closed" in str(e) or "connection" in str(e).lower():
                        if attempt == max_retries - 1:
                            print(f"DB operation failed after {max_retries} attempts: {e}")
                            return jsonify({"error": "Database connection failed", "details": str(e)}), 500
                        print(f"DB retry attempt {attempt + 1} for {func.__name__}")
                        time.sleep(0.5)
                        continue
                    else:
                        # Non-connection error, don't retry
                        raise
            return func(*args, **kwargs)
        return wrapper
    return decorator

@app.teardown_appcontext
def _close_db(_exc):
    db = getattr(g, '_db', None)
    if db is not None:
        try:
            db.close()
        except Exception:
            pass

# -----------------
# User management
# -----------------

def save_user_accounts(users: dict):
    try:
        with open(USER_DATA_FILE, "w") as f:
            json.dump(users, f, indent=2)
    except Exception as e:
        print(f"Error saving user accounts: {e}")

def _hash_if_plain(pw: str) -> str:
    # Accept WerkZeug/Bcrypt/Scrypt hashes; otherwise hash as plain text
    if not isinstance(pw, str):
        pw = str(pw)
    known_prefixes = ("pbkdf2:", "scrypt:", "bcrypt$")
    return pw if any(pw.startswith(p) for p in known_prefixes) else generate_password_hash(pw)

def load_user_accounts() -> dict:
    """
    Loads users from USER_DATA_FILE with improved error handling and structure fixing.
    """
    defaults = {
        "admin": {
            "password": generate_password_hash("admin123"),
            "name": "Admin",
            "bankroll": 5000.0,
            "total_deposits": 5000.0,
            "total_withdrawals": 0.0,
            "betting_profit_loss": 0.0,
            "bet_history": [],
            "money_transactions": [],
            "is_admin": True,
        }
    }

    existing_raw = None
    try:
        with open(USER_DATA_FILE, "r") as f:
            txt = f.read().strip()
            existing_raw = json.loads(txt) if txt else {}
            print(f"ðŸ“ Loaded user data from {USER_DATA_FILE}")
    except Exception as e:
        print(f"âŒ Error reading {USER_DATA_FILE}: {e}")
        existing_raw = {}

    out: dict[str, dict] = {}

    # Support both dict and list styles
    if isinstance(existing_raw, list):
        for rec in existing_raw:
            uname = str(rec.get("username", "")).strip().lower()
            if not uname:
                continue
            pw = rec.get("password_hash") or rec.get("password") or ""
            user = {
                "password": _hash_if_plain(pw),
                "name": rec.get("name", uname),
                "bankroll": float(rec.get("bankroll", 0.0)),
                "total_deposits": float(rec.get("total_deposits", rec.get("deposits", 0.0))),
                "total_withdrawals": float(rec.get("total_withdrawals", rec.get("withdrawals", 0.0))),
                "betting_profit_loss": float(rec.get("betting_profit_loss", rec.get("bet_profit_loss", 0.0))),
                "bet_history": rec.get("bet_history", []) or [],
                "money_transactions": rec.get("money_transactions", rec.get("transactions", [])) or [],
                "is_admin": bool(rec.get("is_admin", False)),
            }
            out[uname] = user
    elif isinstance(existing_raw, dict):
        for k, v in existing_raw.items():
            uname = str(k).strip().lower()
            pw = (v or {}).get("password_hash") or (v or {}).get("password") or ""
            user = dict(v or {})
            user["password"] = _hash_if_plain(pw)
            user.setdefault("name", uname)
            user.setdefault("bankroll", 0.0)
            
            # Handle field name variations
            user.setdefault("total_deposits", user.get("deposits", 0.0))
            user.setdefault("total_withdrawals", user.get("withdrawals", 0.0))
            user.setdefault("betting_profit_loss", user.get("bet_profit_loss", 0.0))
            user.setdefault("money_transactions", user.get("transactions", []))
            
            user.setdefault("bet_history", [])
            user.setdefault("is_admin", False)
            
            # Clean up old field names if they exist
            for old_field in ["deposits", "withdrawals", "bet_profit_loss", "transactions"]:
                user.pop(old_field, None)
            
            out[uname] = user
    else:
        out = {}

    # Ensure an admin exists
    if "admin" not in out:
        out["admin"] = defaults["admin"]
        print("ðŸ“ Created default admin user")

    # Save the corrected structure
    save_user_accounts(out)
    
    print(f"ðŸ‘¥ Loaded {len(out)} users: {sorted(out.keys())}")
    for username, user in out.items():
        print(f"  {username}: ${user.get('bankroll', 0):.2f}, {len(user.get('bet_history', []))} bets")
    
    return out

USERS = load_user_accounts()

# -----------------
# Templates
# -----------------
from templates import LOGIN_TEMPLATE, HTML_TEMPLATE
app.permanent_session_lifetime = timedelta(days=7)  # Sessions last 7 days

@app.route('/login', methods=['GET','POST'])
def login():
    if request.method == 'POST':
        username = request.form['username'].strip().lower()
        password = request.form['password']
        
        print(f"ðŸ” Login attempt for user: {username}")
        print(f"ðŸ” Available users: {list(USERS.keys())}")
        
        # Validate user exists and password is correct
        if username in USERS:
            user = USERS[username]
            stored_password = user.get('password', '')
            
            print(f"ðŸ” Stored password hash starts with: {stored_password[:20]}...")
            
            if check_password_hash(stored_password, password):
                # CRITICAL: Clear any existing session first
                session.clear()
                
                # Set session data
                session['username'] = username
                session['user_bankroll'] = float(user.get('bankroll', 0))
                session['is_admin'] = bool(user.get('is_admin', False))
                session.permanent = True  # Make session persistent
                
                print(f"âœ… User {username} logged in successfully")
                print(f"   - Name: {user.get('name', 'Unknown')}")
                print(f"   - Bankroll: ${user.get('bankroll', 0):.2f}")
                print(f"   - Is Admin: {user.get('is_admin', False)}")
                print(f"   - Bet History Count: {len(user.get('bet_history', []))}")
                
                return redirect(url_for('dashboard'))
            else:
                print(f"âŒ Invalid password for user {username}")
        else:
            print(f"âŒ User {username} not found. Available users: {list(USERS.keys())}")
        
        return render_template_string(LOGIN_TEMPLATE, error="Invalid username or password")
    
    return render_template_string(LOGIN_TEMPLATE)

# 7. FIXED logout route to clear session properly
@app.route('/logout')
def logout():
    username = session.get('username', 'unknown')
    print(f"User {username} logging out")
    session.clear()
    return redirect(url_for('login'))

# 8. ADD route to check which user is currently logged in
@app.route('/api/current-user')
@login_required  
def current_user():
    username = session.get('username')
    if username and username in USERS:
        user = USERS[username]
        return jsonify({
            'username': username,
            'name': user.get('name', username),
            'bankroll': user.get('bankroll', 0),
            'is_admin': user.get('is_admin', False)
        })
    else:
        return jsonify({'error': 'No valid user session'}), 401

print("ðŸ”§ Session fixes loaded - Apply these changes to mobile_dashboard.py")

# -----------------
# Dashboard page
# -----------------
@app.route('/', methods=['GET', 'HEAD'])
@db_retry()
@login_required
def dashboard():
    if request.method == 'HEAD':
        return '', 204
    
    # CRITICAL: Validate session exists and user is valid
    username = session.get('username')
    if not username:
        print("âŒ No username in session, redirecting to login")
        return redirect(url_for('login'))
    
    if username not in USERS:
        print(f"âŒ Username {username} not found in USERS, clearing session")
        session.clear()
        return redirect(url_for('login'))
    
    user = USERS[username]
    print(f"âœ… Dashboard loaded for user: {username} - Bankroll: ${user['bankroll']:.2f}")
    
    # Sync session bankroll with user data
    session['user_bankroll'] = user['bankroll']
    
    # ... rest of dashboard code (keep existing stats logic) ...
    conn = get_db()
    
    # top row stats (keep existing logic)
    try:
        if USE_CLOUD_DB:
            with ENGINE.connect() as conn:
                total_games = conn.execute(text("SELECT COUNT(*) FROM games")).scalar()
                total_odds = conn.execute(text("SELECT COUNT(*) FROM odds WHERE timestamp >= NOW() - INTERVAL '24 hours'")).scalar()
                sportsbooks = conn.execute(text("SELECT COUNT(DISTINCT sportsbook) FROM odds WHERE timestamp >= NOW() - INTERVAL '24 hours'")).scalar()
                last_update_result = conn.execute(text("SELECT MAX(timestamp) AS ts FROM odds")).fetchone()
                last_update = last_update_result[0] if last_update_result else None
        else:
            conn = get_db()
            total_games = conn.execute("SELECT COUNT(*) FROM games").fetchone()[0]
            total_odds = conn.execute("SELECT COUNT(*) FROM odds WHERE timestamp >= datetime('now','-24 hours')").fetchone()[0]
            sportsbooks = conn.execute("SELECT COUNT(DISTINCT sportsbook) FROM odds WHERE timestamp >= datetime('now','-24 hours')").fetchone()[0]
            last_update_row = conn.execute("SELECT MAX(timestamp) AS ts FROM odds").fetchone()
            last_update = last_update_row['ts'] if last_update_row else None
        
        last_str = pd.to_datetime(last_update).strftime('%Y-%m-%d %H:%M') if last_update else 'Never'
    except Exception as e:
        print(f"Dashboard stats error: {e}")
        total_games, total_odds, sportsbooks, last_str = 0, 0, 0, 'Error'

    # Top team calculation with proper connection
    try:
        season, _ = current_phase_and_season()
        with ENGINE.connect() as conn:
            rankings_df = safe_query(
                "SELECT team, power_score, games_played, win_pct FROM team_season_summary WHERE season = :season",
                {"season": season}
            )
            if rankings_df.empty:
                rankings_df = safe_query(
                    "SELECT team, power_score, games_played, win_pct FROM team_season_summary WHERE season = :season",
                    {"season": season - 1}
                )

            injuries_df = load_injury_impact_from_detail(conn)[['team','injury_impact']]
            merged_df = rankings_df.merge(injuries_df, on='team', how='left')
            merged_df['injury_impact'] = merged_df['injury_impact'].fillna(0.0)
            merged_df['form_component'] = np.where(merged_df['games_played'] > 0, (merged_df['win_pct'] - 0.5) * 20, 0)
            merged_df['adjusted_power'] = (merged_df['power_score'] * 0.6 +
                                           merged_df['form_component'] * 0.2 -
                                           merged_df['injury_impact'] * 0.10)

            top_team = to_full(merged_df.loc[merged_df['adjusted_power'].idxmax()]['team']) if not merged_df.empty else "N/A"
    except Exception as e:
        print(f"Top team calculation error: {e}")
        top_team = 'Error'

    stats = {
        'total_games': int(total_games),
        'live_odds': int(total_odds),
        'sportsbooks': int(sportsbooks),
        'opportunities': 0,
        'top_team': top_team,
        'last_update': last_str,
    }

    return render_template_string(HTML_TEMPLATE, username=username, user=user, stats=stats, db_type='cloud' if USE_CLOUD_DB else 'local', users=USERS)


# ==================
# API: /api/rankings
# ==================
# REPLACE the api_rankings function in mobile_dashboard.py with this fixed version

@app.get("/api/rankings")
@db_retry()
def api_rankings():
    """Fixed rankings API that works with both SQLite and PostgreSQL"""
    season = request.args.get("season", type=int) or date.today().year
    try:
        # Use the global ENGINE instead of get_db() to avoid OptionEngine issues
        with ENGINE.connect() as conn:
            
            if USE_CLOUD_DB:
                # PostgreSQL queries
                power = pd.read_sql_query(
                    text("SELECT season, team, power_score AS power FROM team_season_summary WHERE season = :season"),
                    conn, params={"season": season}
                )
                
                # Get live records using compute_live_records function
                rec = compute_live_records(conn, season)
                
                # Get injury data
                try:
                    injuries_df = load_injury_impact_from_detail(conn)[['team','injury_impact']]
                    injuries_df["team"] = injuries_df["team"].map(to_full)
                except Exception as e:
                    print(f"Error loading injury data: {e}")
                    injuries_df = pd.DataFrame(columns=["team","injury_impact"])
            else:
                # SQLite queries - use raw sqlite3 connection
                sqlite_conn = sqlite3.connect(DB_PATH)
                try:
                    power = pd.read_sql_query(
                        "SELECT season, team, power_score AS power FROM team_season_summary WHERE season = ?",
                        sqlite_conn, params=[season]
                    )
                    
                    # Get live records
                    rec = compute_live_records(sqlite_conn, season)
                    
                    # Get injury data
                    try:
                        injuries_df = load_injury_impact_from_detail(sqlite_conn)[['team','injury_impact']]
                        injuries_df["team"] = injuries_df["team"].map(to_full)
                    except Exception as e:
                        print(f"Error loading injury data: {e}")
                        injuries_df = pd.DataFrame(columns=["team","injury_impact"])
                finally:
                    sqlite_conn.close()
        
        if rec.empty:
            return jsonify({"ok": True, "rankings": []})

        # Normalize team names
        power["team"] = power["team"].map(to_full) if not power.empty else []
        rec["team"] = rec["team"].map(to_full)

        # Merge all data
        df = pd.merge(rec, power[["team","power"]], on="team", how="left") if not power.empty else rec.copy()
        df["power"] = df["power"].fillna(0.0) if "power" in df.columns else 0.0
        
        # Merge injury data
        df = df.merge(injuries_df, on="team", how="left")
        df["injury_impact"] = df["injury_impact"].fillna(0.0)

        # Create record string
        def _rec_str(r):
            w = int(r.get("wins", 0) or 0)
            l = int(r.get("losses", 0) or 0)
            t = int(r.get("ties", 0) or 0)
            return f"{w}-{l}" + (f"-{t}" if t > 0 else "")

        df["record_str"] = df.apply(_rec_str, axis=1)

        # Sort by power DESC, then win pct
        df = df.sort_values(["power","win_pct","point_diff"], ascending=[False, False, False])

        out = df[[
            "team","record_str","power","wins","losses","ties",
            "games_played","win_pct","point_diff","injury_impact"
        ]].rename(columns={
            "record_str":"record"
        })

        rankings_data = out.to_dict(orient="records")
        return jsonify({"ok": True, "rankings": rankings_data})
        
    except Exception as e:
        print(f"Rankings error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "rankings": [], "error": str(e)})

# Also add this database connection helper near the top of mobile_dashboard.py
def get_proper_connection():
    """Get the right type of connection for database operations"""
    if USE_CLOUD_DB:
        return ENGINE.connect()
    else:
        return sqlite3.connect(DB_PATH)

@app.route('/api/debug/cloud-connection')
def debug_cloud_connection():
    try:
        with ENGINE.connect() as conn:
            games_count = conn.execute(text("SELECT COUNT(*) FROM games")).scalar()
            today = datetime.utcnow().date()
            recent_games = conn.execute(text("""
                SELECT COUNT(*) FROM games 
                WHERE game_date >= :today
            """), {"today": today}).scalar()
            
            return jsonify({
                'connection': 'success',
                'total_games': games_count,
                'future_games': recent_games,
                'database_type': 'cloud'
            })
    except Exception as e:
        return jsonify({
            'connection': 'failed',
            'error': str(e)
        })

@app.route('/api/debug/games-check')
@login_required
def debug_games_check():
    """Debug what games are in the database"""
    if not USERS.get(session['username'], {}).get('is_admin', False):
        return jsonify({'error': 'Admin only'}), 403
    
    try:
        conn = get_db()
        today = datetime.utcnow().date()
        
        # Check all games
        if USE_CLOUD_DB:
            all_games = safe_query("SELECT game_id, away_team, home_team, game_date FROM games ORDER BY game_date LIMIT 10")
        else:
            all_games = pd.read_sql_query("SELECT game_id, away_team, home_team, game_date FROM games ORDER BY game_date LIMIT 10", conn)
        
        # Check future games specifically
        if USE_CLOUD_DB:
            future_games = safe_query("""
                SELECT game_id, away_team, home_team, game_date 
                FROM games 
                WHERE game_date >= :today 
                ORDER BY game_date LIMIT 5
            """, {"today": today})
        else:
            future_games = pd.read_sql_query("""
                SELECT game_id, away_team, home_team, game_date 
                FROM games 
                WHERE date(game_date) >= date(?) 
                ORDER BY game_date LIMIT 5
            """, conn, params=[today])
        
        return jsonify({
            'today': str(today),
            'total_games_sample': all_games.to_dict('records') if not all_games.empty else [],
            'future_games': future_games.to_dict('records') if not future_games.empty else [],
            'total_future_count': len(future_games),
            'database_type': 'cloud' if USE_CLOUD_DB else 'local'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)})
    
@app.route('/api/debug/ai-status')
@login_required  
def debug_ai_status():
    """Check AI system status"""
    try:
        ml_system = get_ml_prediction_system()
        return jsonify({
            'ml_system_available': ml_system is not None,
            'ml_system_type': type(ml_system).__name__ if ml_system else None,
            'has_model_data': hasattr(ml_system, 'model_data') if ml_system else False,
            'model_data_keys': list(ml_system.model_data.keys()) if ml_system and hasattr(ml_system, 'model_data') and ml_system.model_data else [],
            'error': None
        })
    except Exception as e:
        return jsonify({
            'ml_system_available': False,
            'error': str(e)
        })
# ======================
# API: /api/predictions
# ======================
@app.route('/api/predictions')
def api_predictions():
    """Predictions using ML when possible, otherwise power-based fallback (no crashes)."""
    conn = get_db()
    today = datetime.utcnow().date()
    # FIXED: Back to 21 days like old system for predictions
    horizon = today + timedelta(days=21)

    # Load games with proper database handling
    try:
        if USE_CLOUD_DB:
            games = safe_query("""
                SELECT game_id, away_team AS away, home_team AS home, game_date, start_time_local AS game_time
                FROM games
                WHERE game_date BETWEEN :start_date AND :end_date
                ORDER BY game_date, start_time_local
            """, {"start_date": today, "end_date": horizon})
        else:
            games = pd.read_sql_query("""
                SELECT game_id, away_team AS away, home_team AS home, game_date, start_time_local AS game_time
                FROM games
                WHERE date(game_date) BETWEEN date(?) AND date(?)
                ORDER BY date(game_date), time(start_time_local)
            """, conn, params=[today, horizon])

        print(f"DEBUG: Found {len(games)} games between {today} and {horizon}")
        if not games.empty:
            print(f"Sample games: {games[['away', 'home', 'game_date']].head()}")

    except Exception as e:
        print(f"Error loading games for predictions: {e}")
        return jsonify([])

    # --- build power fallback once ---
    try:
        pmap = get_power_map_cached(conn)
    except Exception:
        pmap = {}

    HFA = 2.5
    def win_prob_fallback(away_abbr, home_abbr):
        aw = pmap.get(to_full(away_abbr), pmap.get(away_abbr, 0.0))
        hm = pmap.get(to_full(home_abbr), pmap.get(home_abbr, 0.0)) + HFA
        ph = 1.0 / (1.0 + math.exp(-(hm - aw) / 8.0))
        return 1.0 - ph, ph  # away, home

    ml_system = get_ml_prediction_system()
    if not ml_system:
        print("ML system not available, using fallback predictions")
    
    rows = []

    for _, g in games.iterrows():
        try:
            # 1) Try ML path
            if ml_system:
                try:
                    prediction_result = ml_system.predict_game(
                        home_team=to_full(g['home']),
                        away_team=to_full(g['away']),
                        game_date=str(g['game_date'])
                    )
                    home_win_prob = float(prediction_result.get('home_win_probability', 0.5))
                    away_win_prob = 1.0 - home_win_prob
                    pick_abbr = g['home'] if home_win_prob >= away_win_prob else g['away']
                    confidence = max(home_win_prob, away_win_prob)

                    rows.append({
                        'game_id': g['game_id'],
                        'matchup': f"{to_full(g['away'])} @ {to_full(g['home'])}",
                        'prediction': to_full(pick_abbr),
                        'confidence': confidence,
                        'confidence_level': 'high' if confidence >= 0.70 else ('medium' if confidence >= 0.60 else 'low'),
                        'betting_grade': 'strong' if confidence >= 0.70 else ('consider' if confidence >= 0.60 else 'weak'),
                        'home_win_probability': home_win_prob,
                        'away_win_probability': away_win_prob,
                        'home_win_prob': home_win_prob,
                        'away_win_prob': away_win_prob,
                        'game_date': str(g['game_date']),
                        'game_time': str(g['game_time'])[:5] if g['game_time'] else 'TBD',
                        'model_prediction': True,
                        'power_difference': 0,
                        'key_factors': prediction_result.get('key_factors', {}),
                        'home_team': to_full(g['home']),
                        'away_team': to_full(g['away']),
                        'feature_count': len((ml_system.model_data or {}).get('feature_cols', []))
                    })
                    continue  # success → next game
                except Exception as e:
                    print(f"FixedNFLSystem failed: {e}")

            # 2) Fallback path
            try:
                pa, ph = win_prob_fallback(g['away'], g['home'])
                pick_abbr = g['home'] if ph >= pa else g['away']
                confidence = max(pa, ph)

                if confidence >= 0.70:
                    confidence_level = 'medium'; betting_grade = 'consider'
                elif confidence >= 0.60:
                    confidence_level = 'low'; betting_grade = 'weak'
                else:
                    confidence_level = 'very-low'; betting_grade = 'avoid'

                rows.append({
                    'game_id': g['game_id'],
                    'matchup': f"{to_full(g['away'])} @ {to_full(g['home'])}",
                    'prediction': to_full(pick_abbr),
                    'confidence': float(confidence),
                    'confidence_level': confidence_level,
                    'betting_grade': betting_grade,
                    'home_win_probability': float(ph),
                    'away_win_probability': float(pa),
                    'home_win_prob': float(ph),
                    'away_win_prob': float(pa),
                    'game_date': str(g['game_date']),
                    'game_time': str(g['game_time'])[:5] if g['game_time'] else 'TBD',
                    'model_prediction': False,
                    'power_difference': 0,
                    'key_factors': {},
                    'home_team': to_full(g['home']),
                    'away_team': to_full(g['away'])
                })
            except Exception as e:
                print(f"Error predicting game {g['away']} @ {g['home']}: {e}")
                continue

        except Exception as e:
            print(f"Error processing game {g['away']} @ {g['home']}: {e}")
            continue

    # Sort by date/time
    rows.sort(key=lambda r: (r['game_date'], str(r.get('game_time', '99:99'))[:5]))
    return jsonify(rows)



def to_full(name: str | None) -> str:
    if not name:
        return "Unknown"
    s = str(name).strip()
    if not s:
        return "Unknown"
    # already a full name?
    if s in FULL_NAMES:
        return s
    su = s.upper()
    su = CANON.get(su, su)         # LA->LAR, WSH->WAS, etc.
    return ABBR_TO_FULL.get(su, s) # fallback to original if unknown



@app.route('/api/predictions/debug')
@login_required  # Only for testing
def debug_predictions():
    """Debug endpoint to verify ML model integration"""
    ml_system = get_ml_prediction_system()
    
    if not ml_system:
        return jsonify({"error": "ML system not available"})
    
    # Test with a simple game prediction
    try:
        sample_prediction = ml_system.predict_game("Philadelphia Eagles", "Dallas Cowboys")
        return jsonify({
            "ml_system_available": True,
            "model_auc": ml_system.model_data.get('model_metrics', {}).get('RandomForest', {}).get('auc', 'Unknown'),
            "feature_count": len(ml_system.model_data.get('feature_cols', [])),
            "sample_prediction": sample_prediction,
            "team_power_data_count": len(ml_system.team_power_data) if ml_system.team_power_data is not None else 0
        })
    except Exception as e:
        return jsonify({
            "error": str(e),
            "ml_system_available": True,
            "initialization_error": True
        })


# =============================
# API: /api/betting-analysis
# =============================
@app.route('/api/betting-analysis')
@db_retry()
def api_betting_analysis():
    """FIXED: Now properly uses game_id linking"""
    conn = get_db()
    week = request.args.get('week', 'current')
    edge_filter = request.args.get('edge', 'all')

    # Get user bankroll
    username = session.get('username', '')
    user_bankroll = float(USERS.get(username, {}).get('bankroll', 100.0))

    # Get time window
    today = datetime.utcnow().date()
    if week == 'current':
        start, end = today, today + timedelta(days=7)
    elif week.isdigit():
        n = int(week)
        start = today + timedelta(days=(n - 1) * 7)
        end = start + timedelta(days=7)
    elif week == 'playoffs':
        start, end = today, today + timedelta(days=120)
    else:
        start, end = today - timedelta(days=60), today + timedelta(days=60)

    # Edge threshold
    min_edge_pct = {'all': 0.0, 'positive': 1.0, '2': 2.0, '5': 5.0, '7': 7.0}.get(edge_filter, 0.0)

    opportunities = []
    
    try:
        # Get games in the time window - REQUIRE valid game_ids
        if USE_CLOUD_DB:
            games = safe_query("""
                SELECT game_id, away_team, home_team, game_date, start_time_local
                FROM games
                WHERE game_date BETWEEN :start_date AND :end_date
                AND game_id IS NOT NULL
                ORDER BY game_date, start_time_local
            """, {"start_date": start, "end_date": end})
        else:
            games = pd.read_sql_query("""
                SELECT game_id, away_team, home_team, game_date, start_time_local
                FROM games
                WHERE date(game_date) BETWEEN date(?) AND date(?)
                AND game_id IS NOT NULL
                ORDER BY date(game_date), time(start_time_local)
            """, conn, params=[start, end])

        if games.empty:
            return jsonify({
                "opportunities": [],
                "total_found": 0,
                "user_bankroll": user_bankroll,
                "message": f"No games with valid game_ids found between {start} and {end}"
            })

        print(f"FIXED: Found {len(games)} games with valid game_ids")

        # Get ML system predictions for these games
        ml_system = get_ml_prediction_system()
        
        for _, game in games.iterrows():
            try:
                if ml_system:
                    # Get ML prediction
                    prediction_result = ml_system.predict_game(
                        home_team=to_full(game['home_team']),
                        away_team=to_full(game['away_team']),
                        game_date=str(game['game_date'])
                    )
                    
                    home_prob = prediction_result['home_win_probability']
                    away_prob = prediction_result['away_win_probability']
                    confidence = prediction_result['confidence']
                    
                    print(f"FIXED: {game['away_team']} @ {game['home_team']} (ID: {game['game_id']})")
                    print(f"  Model: Home {home_prob:.1%}, Away {away_prob:.1%}")
                    
                    # Get odds using game_id - THIS IS THE KEY FIX
                    if USE_CLOUD_DB:
                        odds_df = safe_query("""
                            SELECT team, sportsbook, odds
                            FROM odds 
                            WHERE game_id = :game_id AND market = 'h2h'
                            AND odds IS NOT NULL
                            AND odds BETWEEN -1000 AND 1000
                            ORDER BY timestamp DESC
                        """, {"game_id": game['game_id']})
                    else:
                        odds_df = pd.read_sql_query("""
                            SELECT team, sportsbook, odds
                            FROM odds 
                            WHERE game_id = ? AND market = 'h2h'
                            AND odds IS NOT NULL
                            AND odds BETWEEN -1000 AND 1000
                            ORDER BY timestamp DESC
                        """, conn, params=[game['game_id']])
                    
                    if odds_df.empty:
                        print(f"  No odds found for game_id: {game['game_id']}")
                        continue
                    
                    print(f"  Found {len(odds_df)} odds records for this game")
                    
                    # Check each team for value
                    for team_abbr in [game['home_team'], game['away_team']]:
                        model_prob = home_prob if team_abbr == game['home_team'] else away_prob
                        
                        # Find odds for this team
                        team_odds = odds_df[odds_df['team'] == team_abbr]
                        
                        if team_odds.empty:
                            print(f"    No odds found for {team_abbr}")
                            continue
                        
                        print(f"    Found {len(team_odds)} odds records for {team_abbr}")
                        
                        # Get best odds (highest)
                        best_row = team_odds.loc[team_odds['odds'].idxmax()]
                        odds_val = float(best_row['odds'])
                        
                        # Handle decimal vs American odds
                        if 1.01 <= odds_val <= 10.0:  # Decimal odds
                            implied_prob = 1.0 / odds_val
                            american_odds = int((odds_val - 1) * 100) if odds_val >= 2.0 else int(-100 / (odds_val - 1))
                        else:  # American odds
                            american_odds = int(odds_val)
                            if odds_val > 0:
                                implied_prob = 100 / (odds_val + 100)
                            else:
                                implied_prob = abs(odds_val) / (abs(odds_val) + 100)
                        
                        # Validate implied prob is reasonable
                        if implied_prob > 0.95 or implied_prob < 0.05:
                            print(f"    Skipping unrealistic implied prob: {implied_prob:.1%}")
                            continue
                        
                        edge = model_prob - implied_prob
                        edge_pct = edge * 100
                        
                        print(f"    {team_abbr}: Model {model_prob:.1%}, Implied {implied_prob:.1%}, Edge {edge_pct:.1f}%")
                        
                        # Check if meets threshold
                        if edge_pct >= min_edge_pct:
                            print(f"    *** OPPORTUNITY FOUND: {edge_pct:.1f}% edge ***")
                            
                            # Calculate bet size using Kelly criterion
                            if american_odds > 0:
                                decimal_odds = 1 + (american_odds / 100)
                            else:
                                decimal_odds = 1 + (100 / abs(american_odds))
                            
                            # Kelly fraction
                            kelly = (model_prob * decimal_odds - 1) / (decimal_odds - 1)
                            
                            # Conservative sizing (25% Kelly with caps)
                            stake = max(1.0, min(
                                user_bankroll * 0.05,  # 5% max
                                kelly * user_bankroll * 0.25  # 25% Kelly
                            ))
                            
                            opportunities.append({
                                "game": f"{to_full(game['away_team'])} @ {to_full(game['home_team'])}",
                                "date": str(game['game_date']),
                                "time": str(game['start_time_local'])[:5] if game['start_time_local'] else 'TBD',
                                "team": to_full(team_abbr),
                                "odds": american_odds,
                                "sportsbook": best_row['sportsbook'],
                                "model_prob": round(model_prob, 3),
                                "implied_prob": round(implied_prob, 3),
                                "edge": round(edge, 3),
                                "edge_pct": round(edge_pct, 1),
                                "recommended_amount": round(stake, 2),
                                "confidence": round(confidence * 100, 1),
                                "game_id": str(game['game_id']),
                                "user_bankroll": user_bankroll
                            })
                
            except Exception as e:
                print(f"Error analyzing game {game.get('game_id', 'unknown')}: {e}")
                continue
                
    except Exception as e:
        print(f"Error in betting analysis: {e}")
        return jsonify({"opportunities": [], "total_found": 0, "error": str(e)})

    # Sort by edge percentage
    opportunities.sort(key=lambda x: x['edge_pct'], reverse=True)
    total_recommended = sum(o['recommended_amount'] for o in opportunities)

    print(f"FIXED: Found {len(opportunities)} opportunities, total recommended: ${total_recommended:.2f}")

    return jsonify({
        "opportunities": opportunities,
        "total_found": len(opportunities),
        "week": week,
        "edge_filter": edge_filter,
        "user_bankroll": round(user_bankroll, 2),
        "max_bet_cap": round(user_bankroll * 0.05, 2),
        "slate_budget": round(user_bankroll * 0.10, 2),
        "total_recommended": round(total_recommended, 2),
    })





@app.route('/api/ai-betting-recommendations-debug', methods=['GET'])
def get_betting_recommendations_debug():
    """DEBUG: Lower thresholds to find opportunities"""
    try:
        username = session.get('username')
        if not username:
            return jsonify({'ok': False, 'error': 'No active session'}), 200
        if username not in USERS:
            session.clear()
            return jsonify({'ok': False, 'error': 'Invalid session'}), 200

        user_data = USERS[username]
        user_bankroll = float(user_data.get('bankroll', 500))

        today = datetime.utcnow().date()
        end = today + timedelta(days=7)

        recommendations = []
        total_staked = 0.0
        ml_system = get_ml_prediction_system()
        if not ml_system:
            return jsonify({'ok': True, 'success': True, 'result': {
                'recommendations': [],
                'bankroll': user_bankroll,
                'total_recommended': 0.0,
                'user_info': f"Recommendations for {user_data.get('name', username)}",
                'note': 'Model not available'
            }})

        # Use a Connection for all queries
        with get_engine().connect() as cx:
            if USE_CLOUD_DB:
                games = safe_query("""
                    SELECT game_id, away_team, home_team, game_date, start_time_local
                    FROM games
                    WHERE game_date BETWEEN :start_date AND :end_date
                    ORDER BY game_date, start_time_local
                """, {"start_date": today, "end_date": end})

                all_odds = safe_query("""
                    SELECT team, sportsbook, odds
                    FROM odds 
                    WHERE market = 'h2h'
                    AND odds IS NOT NULL
                    AND odds BETWEEN -800 AND 800
                    ORDER BY timestamp DESC
                """)
            else:
                games = pd.read_sql_query("""
                    SELECT game_id, away_team, home_team, game_date, start_time_local
                    FROM games
                    WHERE date(game_date) BETWEEN date(?) AND date(?)
                    ORDER BY date(game_date), time(start_time_local)
                """, cx, params=[today, end])

                all_odds = pd.read_sql_query("""
                    SELECT team, sportsbook, odds
                    FROM odds 
                    WHERE market = 'h2h'
                    AND odds IS NOT NULL
                    AND odds BETWEEN -800 AND 800
                    ORDER BY timestamp DESC
                """, cx)

        for _, game in games.iterrows():
            try:
                prediction_result = ml_system.predict_game(
                    home_team=to_full(game['home_team']),
                    away_team=to_full(game['away_team']),
                    game_date=str(game['game_date'])
                )
                home_prob = float(prediction_result['home_win_probability'])
                away_prob = float(prediction_result['away_win_probability'])
                confidence = float(prediction_result.get('confidence', max(home_prob, away_prob)))

                # modest threshold to actually show picks
                if confidence < 0.55:
                    continue

                for team_abbr in [game['home_team'], game['away_team']]:
                    model_prob = home_prob if team_abbr == game['home_team'] else away_prob
                    if model_prob < 0.52:
                        continue

                    # FIXED: Use the normalize function instead of the missing one
                    normalized_team = normalize_team_for_odds_lookup(team_abbr)
                    team_odds = all_odds[all_odds['team'] == normalized_team]
                    
                    if team_odds.empty:
                        # Try with original team name
                        team_odds = all_odds[all_odds['team'] == team_abbr]
                    
                    if team_odds.empty:
                        continue

                    best_row = team_odds.loc[team_odds['odds'].idxmax()]
                    odds_val = float(best_row['odds'])

                    # american vs decimal
                    if 1.01 <= odds_val <= 10.0:
                        implied_prob = 1.0 / odds_val
                        american_odds = int((odds_val - 1) * 100) if odds_val >= 2.0 else int(-100 / (odds_val - 1))
                        decimal_odds = odds_val
                    else:
                        american_odds = int(odds_val)
                        if odds_val > 0:
                            implied_prob = 100 / (odds_val + 100)
                            decimal_odds = 1 + (odds_val / 100)
                        else:
                            implied_prob = abs(odds_val) / (abs(odds_val) + 100)
                            decimal_odds = 1 + (100 / abs(odds_val))

                    edge = model_prob - implied_prob
                    edge_pct = edge * 100.0
                    if edge_pct < 2.0:
                        continue

                    kelly = (model_prob * decimal_odds - 1) / (decimal_odds - 1)
                    stake = max(5.0, min(user_bankroll * 0.05, kelly * user_bankroll * 0.25))

                    recommendations.append({
                        'type': 'model_bet',
                        'game': f"{to_full(game['away_team'])} @ {to_full(game['home_team'])}",
                        'date': str(game['game_date']),
                        'time': str(game.get('start_time_local', 'TBD'))[:5],
                        'team': to_full(team_abbr),
                        'odds': american_odds,
                        'decimal_odds': round(decimal_odds, 2),
                        'sportsbook': best_row['sportsbook'],
                        'model_probability': round(model_prob, 3),
                        'implied_probability': round(implied_prob, 3),
                        'edge_percentage': round(edge_pct, 1),
                        'recommended_stake': round(stake, 2),
                        'potential_profit': round(stake * (decimal_odds - 1), 2),
                        'confidence_level': "High" if edge_pct > 6 else ("Medium" if edge_pct > 3 else "Low"),
                        'reason': f"{edge_pct:.1f}% model edge with {confidence*100:.0f}% prediction confidence"
                    })
                    total_staked += stake

            except Exception as e:
                print(f"Error processing game in AI recs: {e}")
                continue

        recommendations.sort(key=lambda x: x['edge_percentage'], reverse=True)
        return jsonify({
            'ok': True,
            'success': True,
            'result': {
                'recommendations': recommendations,
                'bankroll': user_bankroll,
                'total_recommended': round(total_staked, 2),
                'user_info': f"Recommendations for {user_data.get('name', username)}"
            }
        })
    except Exception as e:
        print(f"Error in get_betting_recommendations: {e}")
        return jsonify({'ok': False, 'success': False, 'error': str(e)}), 200



@app.route('/api/debug-odds-data')
@login_required
def debug_odds_data():
    """Debug what's in your odds table"""
    conn = get_db()
    
    try:
        # Check what's in your odds table
        sample_odds = pd.read_sql_query("""
            SELECT game_id, team, sportsbook, odds, market, timestamp
            FROM odds 
            WHERE market = 'h2h'
            ORDER BY timestamp DESC 
            LIMIT 20
        """, conn)
        
        # Check what games exist
        sample_games = pd.read_sql_query("""
            SELECT game_id, home_team, away_team, game_date
            FROM games 
            WHERE date(game_date) >= date('now')
            ORDER BY game_date
            LIMIT 10
        """, conn)
        
        # Check team name mismatches
        game_teams = set()
        if not sample_games.empty:
            for _, g in sample_games.iterrows():
                game_teams.add(g['home_team'])
                game_teams.add(g['away_team'])
        
        odds_teams = set()
        if not sample_odds.empty:
            odds_teams = set(sample_odds['team'].unique())
        
        return jsonify({
            'sample_odds': sample_odds.to_dict('records') if not sample_odds.empty else [],
            'sample_games': sample_games.to_dict('records') if not sample_games.empty else [],
            'total_odds_count': len(sample_odds),
            'total_games_count': len(sample_games),
            'game_teams': list(game_teams),
            'odds_teams': list(odds_teams),
            'team_name_matches': len(game_teams.intersection(odds_teams)),
            'team_name_mismatches': {
                'in_games_not_odds': list(game_teams - odds_teams),
                'in_odds_not_games': list(odds_teams - game_teams)
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)})


# ALSO ADD: Route to insert fake test odds so you can see opportunities
@app.route('/api/add-test-odds')
@login_required  
def add_test_odds():
    """Add realistic test odds for current games"""
    if not USERS.get(session['username'], {}).get('is_admin', False):
        return jsonify({'error': 'Admin only'}), 403
        
    conn = get_db()
    
    try:
        # Get upcoming games
        games = pd.read_sql_query("""
            SELECT game_id, home_team, away_team 
            FROM games 
            WHERE date(game_date) >= date('now')
            LIMIT 5
        """, conn)
        
        if games.empty:
            return jsonify({'error': 'No upcoming games found'})
        
        # Add realistic test odds
        import random
        from datetime import datetime
        
        added_odds = []
        
        for _, game in games.iterrows():
            # Create realistic odds around -110 to +110
            home_odds = random.randint(-150, 120)
            away_odds = random.randint(-150, 120)
            
            # Make sure they're not both positive (unrealistic)
            if home_odds > 0 and away_odds > 0:
                home_odds = -home_odds
            
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Add odds for both teams
            for team, odds in [(game['home_team'], home_odds), (game['away_team'], away_odds)]:
                conn.execute("""
                    INSERT INTO odds (game_id, team, sportsbook, odds, market, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (game['game_id'], team, 'TestBook', odds, 'h2h', timestamp))
                
                added_odds.append({
                    'game_id': game['game_id'],
                    'team': team,
                    'odds': odds
                })
        
        conn.commit()
        
        return jsonify({
            'success': True,
            'message': f'Added {len(added_odds)} test odds',
            'odds_added': added_odds
        })
        
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/betting-analysis-fixed')
def api_betting_analysis_fixed():
    """FIXED version with proper odds validation"""
    conn = get_db()
    
    username = session.get('username', '')
    user_bankroll = float(USERS.get(username, {}).get('bankroll', 100.0))
    
    today = datetime.utcnow().date()
    end = today + timedelta(days=7)
    
    opportunities = []
    
    try:
        # Get games
        odds_df = pd.read_sql_query(text("""
            SELECT team, sportsbook, odds
            FROM odds
            WHERE game_id = :gid AND market = 'h2h'
            AND odds IS NOT NULL
            AND odds BETWEEN -1000 AND 1000
            ORDER BY timestamp DESC
        """), get_engine(), params={"gid": game['game_id']})


        print(f"DEBUG FIXED: Found {len(games)} games")
        
        ml_system = get_ml_prediction_system()
        
        for _, game in games.iterrows():
            try:
                if ml_system:
                    # Get prediction
                    prediction_result = ml_system.predict_game(
                        home_team=game['home_team'],
                        away_team=game['away_team'],
                        game_date=str(game['game_date'])
                    )
                    
                    home_prob = prediction_result['home_win_probability']
                    away_prob = prediction_result['away_win_probability']
                    confidence = prediction_result['confidence']
                    
                    print(f"DEBUG FIXED: {game['away_team']} @ {game['home_team']}")
                    print(f"  Model: Home {home_prob:.1%}, Away {away_prob:.1%}")
                    
                    # Get odds with BETTER query
                    odds_df = pd.read_sql_query(text("""
                        SELECT team, sportsbook, odds
                        FROM odds 
                        WHERE game_id = ? AND market = 'h2h'
                        AND odds IS NOT NULL
                        AND odds BETWEEN -1000 AND 1000  -- Filter out bad odds
                        ORDER BY timestamp DESC
                    """, conn, params=[game['game_id']]))
                    
                    print(f"  Found {len(odds_df)} odds records")
                    
                    if odds_df.empty:
                        print(f"  No valid odds found")
                        continue
                    
                    # Show what odds we found
                    for _, row in odds_df.iterrows():
                        print(f"    {row['team']}: {row['odds']} @ {row['sportsbook']}")
                    
                    # Check each team for value
                    for team_name in [game['home_team'], game['away_team']]:
                        model_prob = home_prob if team_name == game['home_team'] else away_prob
                        
                        # Find odds - try multiple matching strategies
                        team_odds = odds_df[odds_df['team'] == team_name]
                        
                        if team_odds.empty:
                            # Try partial match
                            team_odds = odds_df[odds_df['team'].str.contains(team_name.split()[-1], case=False, na=False)]
                        
                        if team_odds.empty:
                            # Try abbreviation match
                            team_abbr = to_abbr(team_name)
                            team_odds = odds_df[odds_df['team'] == team_abbr]
                        
                        if team_odds.empty:
                            print(f"    No odds match for {team_name}")
                            continue
                        
                        # Get best odds
                        best_row = team_odds.loc[team_odds['odds'].idxmax()]
                        odds_val = float(best_row['odds'])
                        
                        # VALIDATE odds are reasonable
                        if abs(odds_val) > 800:  # Skip if odds too extreme
                            print(f"    Skipping extreme odds: {odds_val}")
                            continue
                        
                        # Calculate implied probability CORRECTLY
                        if odds_val > 0:
                            implied_prob = 100 / (odds_val + 100)
                        elif odds_val < 0:
                            implied_prob = abs(odds_val) / (abs(odds_val) + 100)
                        else:
                            continue  # Skip zero odds
                        
                        # Validate implied prob is reasonable
                        if implied_prob > 0.95 or implied_prob < 0.05:
                            print(f"    Skipping unrealistic implied prob: {implied_prob:.1%}")
                            continue
                        
                        edge = model_prob - implied_prob
                        edge_pct = edge * 100
                        
                        print(f"    {team_name}: Model {model_prob:.1%}, Implied {implied_prob:.1%}, Edge {edge_pct:.1f}%")
                        
                        # Lower threshold for testing
                        if edge_pct >= 1.0:  # 1% edge minimum
                            # Calculate stake
                            if odds_val > 0:
                                decimal_odds = 1 + (odds_val / 100)
                            else:
                                decimal_odds = 1 + (100 / abs(odds_val))
                            
                            kelly = (model_prob * decimal_odds - 1) / (decimal_odds - 1)
                            stake = max(1.0, min(
                                user_bankroll * 0.05,
                                kelly * user_bankroll * 0.25
                            ))
                            
                            opportunities.append({
                                "game": f"{to_full(game['away_team'])} @ {to_full(game['home_team'])}",
                                "date": str(game['game_date']),
                                "time": str(game.get('start_time_local', 'TBD'))[:5],
                                "team": to_full(team_name),
                                "odds": int(odds_val),
                                "sportsbook": best_row['sportsbook'],
                                "model_prob": round(model_prob, 3),
                                "implied_prob": round(implied_prob, 3),
                                "edge": round(edge, 3),
                                "edge_pct": round(edge_pct, 1),
                                "recommended_amount": round(stake, 2),
                                "confidence": round(confidence * 100, 1),
                                "game_id": str(game['game_id']),
                                "user_bankroll": user_bankroll
                            })
                            
                            print(f"    *** OPPORTUNITY: {edge_pct:.1f}% edge ***")
                
            except Exception as e:
                print(f"Error processing game {game['game_id']}: {e}")
                continue
                
    except Exception as e:
        print(f"Error in fixed analysis: {e}")
        return jsonify({"error": str(e)})

    opportunities.sort(key=lambda x: x['edge_pct'], reverse=True)
    total_recommended = sum(o['recommended_amount'] for o in opportunities)

    print(f"DEBUG FIXED: Found {len(opportunities)} opportunities, total: ${total_recommended:.2f}")

    return jsonify({
        "opportunities": opportunities,
        "total_found": len(opportunities),
        "user_bankroll": round(user_bankroll, 2),
        "total_recommended": round(total_recommended, 2),
        "debug": "fixed_version_with_validation"
    })



@app.route('/api/ai-betting-recommendations', methods=['GET'])
def get_betting_recommendations():
    """
    Stable version:
      - Always uses a SQLAlchemy Connection (no OptionEngine issues).
      - Works with PG (text + named params) and SQLite (Pandas + Connection is fine).
      - On any internal error, returns 200 with ok=False (no UI 500).
    """
    try:
        username = session.get('username')
        print(f"Betting recommendations request - Session username: {username}")
        if not username:
            return jsonify({'ok': False, 'error': 'No active session'}), 200
        if username not in USERS:
            session.clear()
            return jsonify({'ok': False, 'error': 'Invalid session'}), 200

        user_data = USERS[username]
        user_bankroll = float(user_data.get('bankroll', 500))

        today = datetime.utcnow().date()
        end   = today + timedelta(days=7)

        recommendations = []
        total_staked = 0.0
        ml_system = get_ml_prediction_system()
        if not ml_system:
            return jsonify({'ok': True, 'success': True, 'result': {
                'recommendations': [],
                'bankroll': user_bankroll,
                'total_recommended': 0.0,
                'user_info': f"Recommendations for {user_data.get('name', username)}",
                'note': 'Model not available'
            }})

        # Use a Connection for all queries
        with get_engine().connect() as cx:
            games = pd.read_sql_query(
                text("""
                    SELECT game_id, away_team, home_team, game_date, start_time_local
                    FROM games
                    WHERE game_date BETWEEN :start_date AND :end_date
                    ORDER BY game_date, start_time_local
                """),
                cx,
                params={"start_date": today, "end_date": end},
            )

            all_odds = pd.read_sql_query(
                text("""
                    SELECT team, sportsbook, odds
                    FROM odds 
                    WHERE market = 'h2h'
                    AND odds IS NOT NULL
                    AND odds BETWEEN -800 AND 800
                    ORDER BY timestamp DESC
                """),
                cx,
            )

        for _, game in games.iterrows():
            try:
                prediction_result = ml_system.predict_game(
                    home_team=game['home_team'],
                    away_team=game['away_team'],
                    game_date=str(game['game_date'])
                )
                home_prob = float(prediction_result['home_win_probability'])
                away_prob = float(prediction_result['away_win_probability'])
                confidence = float(prediction_result.get('confidence', max(home_prob, away_prob)))

                # modest threshold to actually show picks
                if confidence < 0.55:
                    continue

                for team_abbr in [game['home_team'], game['away_team']]:
                    model_prob = home_prob if team_abbr == game['home_team'] else away_prob
                    if model_prob < 0.52:
                        continue

                    full_team_name = normalize_team_for_odds_lookup(team_abbr)
                    team_odds = all_odds[all_odds['team'] == full_team_name]
                    if team_odds.empty:
                        continue

                    best_row = team_odds.loc[team_odds['odds'].idxmax()]
                    odds_val = float(best_row['odds'])

                    # american vs decimal
                    if 1.01 <= odds_val <= 10.0:
                        implied_prob = 1.0 / odds_val
                        american_odds = int((odds_val - 1) * 100) if odds_val >= 2.0 else int(-100 / (odds_val - 1))
                        decimal_odds = odds_val
                    else:
                        american_odds = int(odds_val)
                        if odds_val > 0:
                            implied_prob = 100 / (odds_val + 100)
                            decimal_odds = 1 + (odds_val / 100)
                        else:
                            implied_prob = abs(odds_val) / (abs(odds_val) + 100)
                            decimal_odds = 1 + (100 / abs(odds_val))

                    edge = model_prob - implied_prob
                    edge_pct = edge * 100.0
                    if edge_pct < 2.0:
                        continue

                    kelly = (model_prob * decimal_odds - 1) / (decimal_odds - 1)
                    stake = max(5.0, min(user_bankroll * 0.05, kelly * user_bankroll * 0.25))

                    recommendations.append({
                        'type': 'model_bet',
                        'game': f"{to_full(game['away_team'])} @ {to_full(game['home_team'])}",
                        'date': str(game['game_date']),
                        'time': str(game.get('start_time_local', 'TBD'))[:5],
                        'team': to_full(team_abbr),
                        'odds': american_odds,
                        'decimal_odds': round(decimal_odds, 2),
                        'sportsbook': best_row['sportsbook'],
                        'model_probability': round(model_prob, 3),
                        'implied_probability': round(implied_prob, 3),
                        'edge_percentage': round(edge_pct, 1),
                        'recommended_stake': round(stake, 2),
                        'potential_profit': round(stake * (decimal_odds - 1), 2),
                        'confidence_level': "High" if edge_pct > 6 else ("Medium" if edge_pct > 3 else "Low"),
                        'reason': f"{edge_pct:.1f}% model edge with {confidence*100:.0f}% prediction confidence"
                    })
                    total_staked += stake

            except Exception as e:
                print(f"Error processing game in AI recs: {e}")
                continue

        recommendations.sort(key=lambda x: x['edge_percentage'], reverse=True)
        return jsonify({
            'ok': True,
            'success': True,
            'result': {
                'recommendations': recommendations,
                'bankroll': user_bankroll,
                'total_recommended': round(total_staked, 2),
                'user_info': f"Recommendations for {user_data.get('name', username)}"
            }
        })
    except Exception as e:
        # Return 200 so the UI doesn't show a "Network error" bubble; surface the error in JSON
        print(f"Error in get_betting_recommendations: {e}")
        return jsonify({'ok': False, 'success': False, 'error': str(e)}), 200

# 6. ADD debug route to check session state
@app.route('/api/debug/session-full')
@login_required
def debug_session_full():
    username = session.get('username')
    user_data = USERS.get(username, {}) if username else {}
    
    return jsonify({
        'session_data': {
            'username': session.get('username'),
            'user_bankroll': session.get('user_bankroll'),
            'is_admin': session.get('is_admin'),
            'permanent': session.permanent,
            'all_session_keys': list(session.keys())
        },
        'user_data_from_file': {
            'exists': username in USERS if username else False,
            'name': user_data.get('name', 'Not found'),
            'bankroll': user_data.get('bankroll', 0),
            'bet_history_count': len(user_data.get('bet_history', [])),
            'is_admin': user_data.get('is_admin', False),
            'total_deposits': user_data.get('total_deposits', 0),
            'betting_profit_loss': user_data.get('betting_profit_loss', 0)
        },
        'file_info': {
            'users_file_path': USER_DATA_FILE,
            'total_users_loaded': len(USERS),
            'all_usernames': list(USERS.keys())
        },
        'environment': {
            'is_cloud_db': USE_CLOUD_DB,
            'flask_secret_key_set': bool(app.secret_key)
        }
    })

@app.route('/api/delete-bet', methods=['POST'])
@login_required
def api_delete_bet():
    try:
        username = session['username']
        data = request.json or {}
        idx = int(data.get('bet_index', -1))
        user = USERS[username]
        hist = user.get('bet_history', [])
        if idx < 0 or idx >= len(hist):
            return jsonify({'error': 'Bet not found'}), 400

        bet = hist[idx]
        if bet.get('result', 'Pending') != 'Pending':
            return jsonify({'error': 'Cannot delete a settled bet'}), 400

        # refund the original stake
        amount = float(bet.get('amount', 0.0))
        user['bankroll'] += amount

        # remove bet
        hist.pop(idx)
        save_user_accounts(USERS)
        return jsonify({'success': True, 'new_balance': user['bankroll']})
    except Exception as e:
        print("/api/delete-bet error:", e)
        return jsonify({'error': str(e)}), 500


# Simple games + odds preview (now includes per-book breakdown)
@app.route('/api/games')
def api_games():
    """FIXED VERSION - Searches odds by team names, not game_id"""
    try:
        conn = get_db()
        today = datetime.utcnow().date()
        end = today + timedelta(days=60)
        
        # Get games
        games = pd.read_sql_query("""
            SELECT game_id, away_team AS away, home_team AS home, game_date, start_time_local AS game_time
            FROM games
            WHERE date(game_date) BETWEEN date(?) AND date(?)
            AND game_id IS NOT NULL AND game_id != ''
            ORDER BY date(game_date), time(start_time_local)
        """, conn, params=[today, end])

        print(f"DEBUG: Found {len(games)} games with valid game_ids")

        if games.empty:
            return jsonify([])

        # Get ALL recent odds (not filtered by game_id since they don't match)
        odds = pd.read_sql_query("""
            SELECT team, sportsbook, odds, timestamp
            FROM odds 
            WHERE market = 'h2h'
            AND odds IS NOT NULL
            AND timestamp >= datetime('now', '-7 days')
            ORDER BY timestamp DESC
        """, conn)

        print(f"DEBUG: Found {len(odds)} recent odds records")

        # Process each game
        result = []
        for _, game in games.iterrows():
            teams = []
            
            for team_name in [game['away'], game['home']]:
                # Search odds by team name directly
                team_odds = odds[odds['team'] == team_name]
                
                print(f"DEBUG: Team {team_name} has {len(team_odds)} odds records")
                
                if team_odds.empty:
                    teams.append({
                        "team": to_full(team_name),
                        "odds": -110,
                        "sportsbook": "No Line",
                        "by_book": []
                    })
                else:
                    # Process valid odds
                    valid_odds = []
                    for _, row in team_odds.iterrows():
                        try:
                            odds_val = float(row['odds'])
                            
                            # Convert to American odds if needed
                            if 1.01 <= odds_val <= 10.0:  # Decimal
                                if odds_val >= 2.0:
                                    american_odds = int((odds_val - 1) * 100)
                                else:
                                    american_odds = int(-100 / (odds_val - 1))
                            else:  # Already American
                                american_odds = int(odds_val)
                            
                            # Validate range
                            if -1000 <= american_odds <= 1000:
                                valid_odds.append({
                                    'sportsbook': row['sportsbook'],
                                    'odds': american_odds
                                })
                        except (ValueError, TypeError):
                            continue
                    
                    if valid_odds:
                        # Find best odds (highest for positive, closest to 0 for negative)
                        best_odds_entry = max(valid_odds, key=lambda x: x['odds'])
                        
                        teams.append({
                            "team": to_full(team_name),
                            "odds": best_odds_entry['odds'],
                            "sportsbook": best_odds_entry['sportsbook'],
                            "by_book": valid_odds
                        })
                        
                        print(f"DEBUG: {team_name} -> {best_odds_entry['odds']} @ {best_odds_entry['sportsbook']}")
                    else:
                        teams.append({
                            "team": to_full(team_name),
                            "odds": -110,
                            "sportsbook": "No Odds",
                            "by_book": []
                        })

            result.append({
                "game_id": str(game['game_id']),
                "game": f"{to_full(game['away'])} @ {to_full(game['home'])}",
                "date": str(game['game_date']),
                "time": str(game['game_time'])[:5] if game['game_time'] else "TBD",
                "teams": teams
            })

        print(f"DEBUG: Returning {len(result)} games")
        return jsonify(result)

    except Exception as e:
        print(f"api_games error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify([])

    except Exception as e:
        print(f"api_games error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify([])


# Activity + betting endpoints
@app.route('/api/recent-activity')
def api_recent_activity():
    try:
        if 'username' not in session:
            return jsonify([])
        username = session['username']
        user = USERS[username]
        acts = []
        for t in user.get('money_transactions', [])[-5:]:
            acts.append({
                'date': t.get('date','Unknown'),
                'type': t.get('type','transaction'),
                'description': f"{t.get('type','Transaction').title()}: ${t.get('amount',0):.2f}",
                'profit_loss': 0
            })
        for b in user.get('bet_history', [])[-5:]:
            acts.append({
                'date': b.get('date','Unknown'),
                'type': 'bet',
                'description': f"{b.get('bet_type','Unknown bet')} - ${b.get('amount',0):.2f}",
                'profit_loss': b.get('profit_loss', 0)
            })
        acts.sort(key=lambda x: x['date'], reverse=True)
        return jsonify(acts[:10])
    except Exception as e:
        print("/api/recent-activity error:", e)
        return jsonify([])

@app.route('/api/bet-history')
def api_bet_history():
    try:
        if 'username' not in session:
            return jsonify([])
        return jsonify(USERS.get(session['username'], {}).get('bet_history', []))
    except Exception as e:
        print("/api/bet-history error:", e)
        return jsonify([])

@app.route('/api/place-bet', methods=['POST'])
@login_required
def api_place_bet():
    try:
        username = session['username']
        data = request.json
        amount = float(data.get('amount', 0))
        if amount <= 0: return jsonify({'error':'Invalid bet amount'}), 400
        user = USERS[username]
        if user['bankroll'] < amount: return jsonify({'error':'Insufficient bankroll'}), 400
        user['bankroll'] -= amount
        bet = {
            'date': datetime.now().strftime('%Y-%m-%d %H:%M'),
            'game': data.get('game',''),
            'bet_type': data.get('bet_type',''),
            'amount': amount,
            'odds': data.get('odds',''),
            'sportsbook': data.get('sportsbook',''),
            'game_id': data.get('game_id'),   # <-- add this
            'result': 'Pending',
            'profit_loss': 0.0
        }
        user['bet_history'].append(bet)
        save_user_accounts(USERS)
        return jsonify({'success': True, 'new_balance': user['bankroll']})
    except Exception as e:
        print("/api/place-bet error:", e)
        return jsonify({'error': str(e)}), 500

@app.route('/api/ai/value-bets')
@login_required
def api_ai_value_bets():
    if list_value_bets is None:
        return jsonify([])  # ai_tools.py not importable
    edge = float(request.args.get('min_edge', 0.07))  # default 7%
    out = list_value_bets(edge_min=edge)
    return jsonify(out)



@app.route('/api/money-transaction', methods=['POST'])
@login_required
def api_money_transaction():
    try:
        username = session['username']
        data = request.json
        t = data.get('type')
        amount = float(data.get('amount', 0))
        if amount <= 0: return jsonify({'error':'Invalid amount'}), 400
        user = USERS[username]
        if t == 'deposit':
            user['bankroll'] += amount
            user['total_deposits'] += amount
        elif t == 'withdraw':
            if user['bankroll'] < amount: return jsonify({'error':'Insufficient balance'}), 400
            user['bankroll'] -= amount
            user['total_withdrawals'] += amount
        else:
            return jsonify({'error':'Invalid transaction type'}), 400
        tx = {
            'date': datetime.now().strftime('%Y-%m-%d %H:%M'),
            'type': t,
            'amount': amount,
            'description': data.get('description',''),
            'balance_after': user['bankroll']
        }
        user['money_transactions'].append(tx)
        save_user_accounts(USERS)
        return jsonify({'success': True, 'new_balance': user['bankroll']})
    except Exception as e:
        print("/api/money-transaction error:", e)
        return jsonify({'error': str(e)}), 500


@app.route('/api/debug/games-count-by-date')
@login_required
def debug_games_by_date():
    if not USERS.get(session['username'], {}).get('is_admin', False):
        return jsonify({'error': 'Admin only'}), 403
    
    conn = get_db()
    today = datetime.utcnow().date()
    
    try:
        if USE_CLOUD_DB:
            games_count = safe_query("""
                SELECT DATE(game_date) as date, COUNT(*) as count
                FROM games 
                WHERE game_date >= :today
                GROUP BY DATE(game_date)
                ORDER BY game_date LIMIT 10
            """, {"today": today})
        else:
            games_count = pd.read_sql_query("""
                SELECT DATE(game_date) as date, COUNT(*) as count
                FROM games 
                WHERE date(game_date) >= date(?)
                GROUP BY date(game_date)
                ORDER BY date(game_date) LIMIT 10
            """, conn, params=[today])
        
        return jsonify({
            'today': str(today),
            'games_by_date': games_count.to_dict('records') if not games_count.empty else [],
            'database_type': 'cloud' if USE_CLOUD_DB else 'local'
        })
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/debug/raw-games-data')
@login_required
def debug_raw_games():
    if not USERS.get(session['username'], {}).get('is_admin', False):
        return jsonify({'error': 'Admin only'}), 403
    
    conn = get_db()
    try:
        if USE_CLOUD_DB:
            raw_games = safe_query("""
                SELECT game_id, away_team, home_team, game_date, start_time_local, 
                       created_at, updated_at
                FROM games 
                ORDER BY game_date 
                LIMIT 20
            """)
        else:
            raw_games = pd.read_sql_query("""
                SELECT game_id, away_team, home_team, game_date, start_time_local
                FROM games 
                ORDER BY game_date 
                LIMIT 20
            """, conn)
        
        return jsonify({
            'total_games': len(raw_games),
            'sample_games': raw_games.to_dict('records'),
            'date_range': {
                'earliest': str(raw_games['game_date'].min()) if not raw_games.empty else None,
                'latest': str(raw_games['game_date'].max()) if not raw_games.empty else None
            }
        })
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/settle-bet', methods=['POST'])
@login_required
def api_settle_bet():
    try:
        username = session['username']
        data = request.json
        idx = int(data.get('bet_index', -1))
        result = str(data.get('result','')).lower()
        user = USERS[username]
        hist = user.get('bet_history', [])
        if idx < 0 or idx >= len(hist):
            return jsonify({'error':'Bet not found'}), 400
        bet = hist[idx]
        if bet['result'] != 'Pending':
            return jsonify({'error':'Bet already settled'}), 400
        bet['result'] = result.title()
        amount = float(bet['amount'])
        odds = str(bet['odds'])
        if result == 'win':
            if odds.startswith('+'):
                payout = amount * (int(odds[1:]) / 100)
            elif odds.startswith('-'):
                payout = amount * (100 / abs(int(odds)))
            else:
                payout = amount
            bet['profit_loss'] = payout
            user['bankroll'] += amount + payout
            user['betting_profit_loss'] += payout
        elif result == 'loss':
            bet['profit_loss'] = -amount
            user['betting_profit_loss'] -= amount
        elif result == 'push':
            bet['profit_loss'] = 0
            user['bankroll'] += amount
        save_user_accounts(USERS)
        return jsonify({'success': True, 'new_balance': user['bankroll']})
    except Exception as e:
        print("/api/settle-bet error:", e)
        return jsonify({'error': str(e)}), 500

# -------- Admin helpers --------
@app.route('/api/admin/users')
@admin_required
def api_admin_users():
    users = []
    for uname, u in USERS.items():
        users.append({
            'username': uname,
            'name': u.get('name', uname),
            'bankroll': u.get('bankroll', 0.0),
            'betting_profit_loss': u.get('betting_profit_loss', 0.0),
            'bet_count': len(u.get('bet_history', []))
        })
    return jsonify(users)

@app.route('/api/admin/adjust-balance', methods=['POST'])
@admin_required
def api_admin_adjust_balance():
    data = request.json or {}
    username = (data.get('username') or '').lower()
    adjustment = float(data.get('adjustment', 0))
    reason = data.get('reason', 'Admin adjustment')
    if username not in USERS:
        return jsonify({'error': 'User not found'}), 404
    user = USERS[username]
    old_balance = user.get('bankroll', 0.0)
    user['bankroll'] = old_balance + adjustment
    tx = {
        'date': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'type': 'admin_adjust',
        'amount': adjustment,
        'description': reason,
        'balance_after': user['bankroll']
    }
    user.setdefault('money_transactions', []).append(tx)
    save_user_accounts(USERS)
    return jsonify({'success': True, 'old_balance': old_balance, 'new_balance': user['bankroll']})

@app.route('/api/admin/clear-activity', methods=['POST'])
@admin_required
def api_admin_clear_activity():
    try:
        for u in USERS.values():
            u['bet_history'] = []
            u['money_transactions'] = []
            u['betting_profit_loss'] = 0.0
        save_user_accounts(USERS)
        return jsonify({'success': True, 'message': 'All activity cleared'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# REPLACE all your @app.before_request functions with this single one in mobile_dashboard.py

# REPLACE all your @app.before_request functions with this single one in mobile_dashboard.py

# REPLACE BOTH of your @app.before_request functions with this SINGLE one:

@app.before_request  
def unified_before_request():
    global _initialized, _model_pack, _ml_prediction_system
    
    # Skip validation for specific routes
    if request.endpoint in ['login', 'logout', 'static']:
        return
    
    # Skip validation for health checks
    if request.path in ['/health', '/healthz', '/version', '/api/health']:
        return
    
    # One-time initialization
    if not _initialized:
        print("🔧 Running one-time initialization...")
        
        # Force model cache clear
        _model_pack = None
        _ml_prediction_system = None
        print("Forced model cache clear on startup")
        
        # Initialize database indexes
        ensure_indexes()
        
        # Initialize ML system
        get_ml_prediction_system()
        
        # Fix user data structure issues
        try:
            fix_user_data_structure()
            print("✅ User data structure fixes completed")
        except Exception as e:
            print(f"❌ Error fixing user data structure: {e}")
        
        _initialized = True
        print("✅ One-time initialization completed")
    
    # Session validation for API routes
    if request.path.startswith('/api/') and request.endpoint not in ['login', 'current_user']:
        username = session.get('username')
        
        # Check if session has username but user doesn't exist
        if username and username not in USERS:
            print(f"❌ Session cleanup: User {username} no longer exists")
            session.clear()
            return jsonify({'error': 'Session expired - user not found'}), 401
        
# ALSO ADD: Debug route to manually trigger user data fix
@app.route('/api/debug/fix-user-data')
@login_required
def debug_fix_user_data():
    global USERS  # Move this to the TOP
    
    if not USERS.get(session['username'], {}).get('is_admin', False):
        return jsonify({'error': 'Admin only'}), 403
    
    try:
        fixed_count = fix_user_data_structure()
        
        # Reload USERS global
        USERS = load_user_accounts()
        
        return jsonify({
            'success': True,
            'fixes_applied': fixed_count,
            'users_loaded': len(USERS),
            'user_data': {username: USERS.get(username, {}) for username in USERS.keys()}
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    
# ADD: Debug route to check the actual user data on disk
@app.route('/api/debug/raw-user-data')
@login_required  
def debug_raw_user_data():
    if not USERS.get(session['username'], {}).get('is_admin', False):
        return jsonify({'error': 'Admin only'}), 403
    
    try:
        # Read raw file
        with open(USER_DATA_FILE, 'r') as f:
            raw_data = json.loads(f.read())
        
        return jsonify({
            'file_path': USER_DATA_FILE,
            'raw_file_data': raw_data,
            'loaded_users_count': len(USERS),
            'loaded_users': list(USERS.keys()),
            'session_username': session.get('username'),
            'current_user_in_memory': USERS.get(session.get('username'), 'NOT_FOUND')
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# Add this route to mobile_dashboard.py in the admin section:
@app.route('/api/admin/reset-money', methods=['POST'])
@login_required
def api_admin_reset_money():
    try:
        username = session['username']
        me = USERS.get(username)
        if not me or not me.get('is_admin'):
            return jsonify({'error': 'Admin only'}), 403

        data = request.json or {}
        new_bankroll = float(data.get('new_bankroll', 100.0))

        users_updated = 0
        for u in USERS.values():
            u['money_transactions'] = []
            u['total_deposits'] = 0.0
            u['total_withdrawals'] = 0.0
            u['bankroll'] = new_bankroll
            # leave bet history and P&L alone unless you want a hard reset:
            # u['bet_history'] = []
            # u['betting_profit_loss'] = 0.0
            users_updated += 1

        save_user_accounts(USERS)
        return jsonify({'success': True,
                        'message': f'All users set to ${new_bankroll:.2f} and money history cleared',
                        'users_updated': users_updated})
    except Exception as e:
        print("reset-money error:", e)
        return jsonify({'error': str(e)}), 500


# Also add this route for individual user money reset:
@app.route('/api/admin/reset-user-money', methods=['POST'])
@login_required
def api_admin_reset_user_money():
    try:
        username = session['username']
        me = USERS.get(username)
        if not me or not me.get('is_admin'):
            return jsonify({'error': 'Admin only'}), 403

        data = request.json or {}
        target = str(data.get('username') or '').strip()
        bankroll = float(data.get('bankroll', 100.0))

        if not target or target not in USERS:
            return jsonify({'error': 'User not found'}), 404

        u = USERS[target]
        u['money_transactions'] = []
        u['total_deposits'] = 0.0
        u['total_withdrawals'] = 0.0
        u['bankroll'] = bankroll
        # leave bet history and P&L as-is unless you want to zero them
        # u['bet_history'] = []
        # u['betting_profit_loss'] = 0.0

        save_user_accounts(USERS)
        return jsonify({'success': True,
                        'message': f'{target} reset to ${bankroll:.2f} and money history cleared',
                        'new_balance': bankroll})
    except Exception as e:
        print("reset-user-money error:", e)
        return jsonify({'error': str(e)}), 500

# Add this to mobile_dashboard.py
@app.route('/api/admin/wipe-deposits-withdrawals', methods=['POST'])
@login_required
def api_admin_wipe_deposits_withdrawals():
    try:
        username = session['username']
        me = USERS.get(username)
        if not me or not me.get('is_admin'):
            return jsonify({'error': 'Admin only'}), 403

        DEFAULT_START = 100.0  # matches your UI message
        users_reset = 0

        for u in USERS.values():
            # clear money history & totals
            u['money_transactions'] = []
            u['total_deposits'] = 0.0
            u['total_withdrawals'] = 0.0

            # give everyone a clean starting bankroll
            u['bankroll'] = float(DEFAULT_START)

            # keep bet history and betting P&L intact (as your UI text says)
            # u['bet_history'] stays as-is
            # u['betting_profit_loss'] stays as-is

            users_reset += 1

        save_user_accounts(USERS)
        return jsonify({'success': True,
                        'message': 'Cleared all deposits/withdrawals and reset bankrolls to $100',
                        'users_reset': users_reset})
    except Exception as e:
        print("wipe-deposits-withdrawals error:", e)
        return jsonify({'error': str(e)}), 500
    
@app.route("/healthz")
def healthz():
    return {"ok": True}, 200
@app.route('/api/health')
def api_health():
    try:
        if USE_CLOUD_DB:
            with ENGINE.connect() as conn:
                has_tss = conn.execute(text("SELECT to_regclass('public.team_season_summary')")).scalar()
                has_games = conn.execute(text("SELECT to_regclass('public.games')")).scalar()
                has_odds  = conn.execute(text("SELECT to_regclass('public.odds')")).scalar()
                return jsonify({
                    'backend': 'postgres',
                    'db_url_or_path': DATABASE_URL[:50] + "...",
                    'team_season_summary': bool(has_tss),
                    'games': bool(has_games),
                    'odds': bool(has_odds),
                })
        else:
            # SQLite
            con = sqlite3.connect(DB_PATH)
            con.row_factory = sqlite3.Row
            try:
                cur = con.cursor()
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='team_season_summary'")
                has_tss = cur.fetchone() is not None
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='games'")
                has_games = cur.fetchone() is not None
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='odds'")
                has_odds = cur.fetchone() is not None
            finally:
                con.close()
            return jsonify({
                'backend': 'sqlite',
                'db_url_or_path': DB_PATH,
                'team_season_summary': has_tss,
                'games': has_games,
                'odds': has_odds,
            })
    except Exception as e:
        return jsonify({'error': str(e), 'db_url_or_path': DB_PATH if not USE_CLOUD_DB else DATABASE_URL[:50] + "...", 'backend': 'postgres' if USE_CLOUD_DB else 'sqlite'}), 500
# Entrypoint

def main():
    print(f"Bettr Bot Dashboard at http://localhost:5000\nDB: {DB_PATH}")
    def _open():
        time.sleep(1.2)
        import webbrowser
        webbrowser.open('http://localhost:5000')
    threading.Thread(target=_open, daemon=True).start()
    app.run(debug=True, host='0.0.0.0', port=5000)

if __name__ == '__main__':
    main()
    test_fixed_function()