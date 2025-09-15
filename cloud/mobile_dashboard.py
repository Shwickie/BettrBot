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

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
# robust import so Windows path works when running from /dashboard
# SIMPLIFIED DATABASE SETUP - SINGLE SOURCE OF TRUTH
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

# Handle Render's postgres:// URLs
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://", 1)

USE_CLOUD_DB = DATABASE_URL.startswith(("postgresql://", "postgresql+psycopg2://"))

if USE_CLOUD_DB:
    print(f"Using cloud database: {DATABASE_URL[:50]}...")
    ENGINE = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=300,
        connect_args={"sslmode": "require"} if "sslmode=" not in DATABASE_URL else {}
    )
    print("Using cloud PostgreSQL")
else:
    # Local SQLite setup
    DEFAULT_DB = r"E:/Bettr Bot/betting-bot/data/betting.db" if os.name == "nt" else "/tmp/betting.db"
    DB_PATH = os.getenv("BETTR_DB_PATH", DEFAULT_DB)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    ENGINE = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
    print(f"Using local SQLite: {DB_PATH}")

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
    from dashboard.ai_chat_stub import comprehensive_ai_bp
except Exception:
    import os, sys
    sys.path.append(os.path.dirname(__file__))
    from ai_chat_stub import comprehensive_ai_bp

def safe_query(query_str, params=None):
    """Execute query safely with proper PostgreSQL parameter handling"""
    if not engine:
        return pd.DataFrame()
    
    try:
        with engine.connect() as conn:
            if params:
                # Use SQLAlchemy text() with proper parameter binding
                result = pd.read_sql(text(query_str), conn, params=params)
            else:
                result = pd.read_sql(text(query_str), conn)
            return result
    except Exception as e:
        print(f"Query error: {e}")
        return pd.DataFrame()



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

    # 4) Last resort: nothing found – keep it None but avoid KeyErrors later
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
            return pd.read_sql_query(text(query), conn, params=params or {})
        else:
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
DEFAULT_DB = r"E:/Bettr Bot/betting-bot/data/betting.db"
DB_PATH = os.environ.get("BETTR_DB_PATH", DEFAULT_DB)

# SQLAlchemy engine for summary stats
_engine = create_engine(f"sqlite:///{DB_PATH}")

# Flask app
app = Flask(__name__)
# register AI blueprint at /api/ai-*
app.register_blueprint(comprehensive_ai_bp, url_prefix='')
app.secret_key = 'bettr-bot-enhanced-2025'
DATABASE_URL = os.environ.get("DATABASE_URL", "")
USE_CLOUD_DB = DATABASE_URL.startswith(("postgres://", "postgresql://"))

# create engine
if USE_CLOUD_DB:
    engine = create_engine(
    DATABASE_URL if "sslmode=" in DATABASE_URL else f"{DATABASE_URL}&sslmode=require" if "?" in DATABASE_URL else f"{DATABASE_URL}?sslmode=require",
    pool_pre_ping=True
)
else:
    DEFAULT_DB = r"E:/Bettr Bot/betting-bot/data/betting.db"
    DB_PATH = os.environ.get("BETTR_DB_PATH", DEFAULT_DB)
    engine = create_engine(f"sqlite:///{DB_PATH}")

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
USER_DATA_FILE = os.environ.get("BETTR_USERS_PATH", os.path.join(BASE_DIR, "user_accounts.json"))


app.secret_key = os.environ.get("FLASK_SECRET", "bettr-bot-enhanced-2025")


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

@app.before_request
def _init_once():
    global _initialized
    if not _initialized:
        ensure_indexes()
        get_ml_prediction_system()
        _initialized = True

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

    # 1) Base power + record (seed current season; fallback to last season if empty)
    try:
        base = pd.read_sql_query(
            "SELECT team, power_score, games_played, win_pct FROM team_season_summary WHERE season = :season",
            conn, params={"season": season}
        )
        base['team'] = base['team'].map(to_abbr)
        if base.empty:
            base = pd.read_sql_query(
                "SELECT team, power_score, games_played, win_pct FROM team_season_summary WHERE season = :season",
                conn, params=[season - 1]
            )
    except Exception:
        base = pd.DataFrame(columns=['team','power_score','games_played','win_pct'])

    # 2) Injury view (keep it light so we donâ€™t drive everything negative)
    # In get_unified_power_scores()
    try:
        inj = load_injury_impact_from_detail(conn)
        # keep columns: team, injury_impact, qb_risk
    except Exception:
        inj = pd.DataFrame(columns=['team','injury_impact','qb_risk'])


    df = base.merge(inj, on='team', how='left')
    df['injury_impact'] = df['injury_impact'].fillna(0.0)
    df['qb_risk'] = df['qb_risk'].fillna(0)

    # 3) Small â€œformâ€ component so 0â€“0 teams donâ€™t all look identical
    df['form_component'] = df.apply(
        lambda r: (r['win_pct'] - 0.5) * 20 if pd.notnull(r['win_pct']) and pd.notnull(r['games_played']) and r['games_played'] > 0 else 0.0,
        axis=1
    )

    # 4) Final adjusted power (keep roughly your historical 0â€“12 feel)
    # In get_unified_power_scores()
    df['adj_power'] = (
        # Let the base power score have its full impact
        df['power_score'].fillna(0.0) * 1.0 +

        # Keep a small component for recent form
        df['form_component'] * 0.20 -

        # Soften the injury penalty
        df['injury_impact'] * 0.05
    )

    return df[['team','power_score','games_played','win_pct','injury_impact','qb_risk','adj_power']]

def to_full(name: str | None) -> str:
    if not name:
        return "Unknown"
    return TEAM_TO_FULL.get(name, name)

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
    except Exception:
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
        return ENGINE.connect()
    else:
        if not hasattr(g, "_db"):
            g._db = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
            g._db.row_factory = sqlite3.Row
        return g._db

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

def save_user_accounts(users):
    try:
        with open(USER_DATA_FILE, 'w') as f:
            json.dump(users, f, indent=2)
    except Exception as e:
        print(f"Error saving user accounts: {e}")

def load_user_accounts():
    defaults = {
        'admin': {
            'password': generate_password_hash('admin123'),
            'name': 'Admin',
            'bankroll': 5000.0,
            'total_deposits': 5000.0,
            'total_withdrawals': 0.0,
            'betting_profit_loss': 0.0,
            'bet_history': [],
            'money_transactions': [],
            'is_admin': True
        }
    }
    existing = {}
    if os.path.exists(USER_DATA_FILE):
        try:
            with open(USER_DATA_FILE, 'r') as f:
                existing = json.load(f)
        except Exception as e:
            print(f"Error reading {USER_DATA_FILE}: {e}")
    out = {k.lower(): v for k,v in existing.items()}
    for k,v in defaults.items():
        out.setdefault(k, v)
    for u in out.values():
        u.setdefault('bet_history', [])
        u.setdefault('money_transactions', [])
        u.setdefault('betting_profit_loss', 0.0)
        u.setdefault('total_deposits', 0.0)
        u.setdefault('total_withdrawals', 0.0)
        u.setdefault('bankroll', u.get('bankroll', 0.0))
        u.setdefault('is_admin', False)
    save_user_accounts(out)
    return out

USERS = load_user_accounts()

# -----------------
# Templates
# -----------------
from templates import LOGIN_TEMPLATE, HTML_TEMPLATE

@app.route('/login', methods=['GET','POST'])
def login():
    if request.method == 'POST':
        u = request.form['username'].strip().lower()
        p = request.form['password']
        if u in USERS and check_password_hash(USERS[u]['password'], p):
            session['username'] = u
            return redirect(url_for('dashboard'))
        return render_template_string(LOGIN_TEMPLATE, error="Invalid username or password")
    return render_template_string(LOGIN_TEMPLATE)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

# -----------------
# Dashboard page
# -----------------
@app.route('/', methods=['GET', 'HEAD'])
@login_required
def dashboard():
    if request.method == 'HEAD':
        return '', 204
    username = session['username']
    user = USERS[username]
    conn = get_db()  # use sqlite3 connection everywhere here

    
    # top row stats
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

    # find true top team (same logic as rankings)
    try:
        season, _ = current_phase_and_season()
        rankings_df = pd.read_sql_query(
            text("SELECT team, power_score, games_played, win_pct FROM team_season_summary WHERE season=:season"),
            conn, params={"season": season}
        )
        if rankings_df.empty:
            rankings_df = pd.read_sql_query(
                text("SELECT team, power_score, games_played, win_pct FROM team_season_summary WHERE season=:season"),
                conn, params={"season": season - 1}
            )

        injuries_df = load_injury_impact_from_detail(conn)[['team','injury_impact']]


        merged_df = rankings_df.merge(injuries_df, on='team', how='left')
        merged_df['injury_impact'] = merged_df['injury_impact'].fillna(0.0)
        merged_df['form_component'] = np.where(merged_df['games_played'] > 0, (merged_df['win_pct'] - 0.5) * 20, 0)
        # lighter injury weight so numbers donâ€™t go negative
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

    return render_template_string(HTML_TEMPLATE, username=username, user=user, stats=stats, db_type='local', users=USERS)

# ==================
# API: /api/rankings
# ==================
@app.route('/api/rankings')
def api_rankings():
    """FIXED rankings with proper duplicate handling"""
    season, _phase = current_phase_and_season()
    
    try:
        # FIXED: Use DISTINCT and proper query structure
        if USE_CLOUD_DB:
            conn = ENGINE.connect()
            base_query = """
                SELECT DISTINCT
                    team,
                    power_score,
                    wins,
                    losses,
                    games_played,
                    win_pct,
                    point_diff
                FROM team_season_summary 
                WHERE season = :season
            """
            pr = pd.read_sql_query(text(base_query), conn, params={"season": season})
            conn.close()
        else:
            conn = get_db()
            base_query = """
                SELECT DISTINCT
                    team,
                    power_score,
                    wins,
                    losses,
                    games_played,
                    win_pct,
                    point_diff
                FROM team_season_summary 
                WHERE season = ?
            """
            pr = pd.read_sql_query(base_query, conn, params=[season])
        
        if pr.empty:
            if USE_CLOUD_DB:
                conn = ENGINE.connect()
                pr = pd.read_sql_query(text(base_query), conn, params={"season": season - 1})
                conn.close()
            else:
                pr = pd.read_sql_query(base_query, conn, params=[season - 1])
        
        # CRITICAL: Remove any remaining duplicates at DataFrame level
        pr = pr.drop_duplicates(subset=['team']).reset_index(drop=True)
        pr['team'] = pr['team'].map(to_abbr)
        
    except Exception as e:
        print(f"Rankings query error: {e}")
        return jsonify([])

    # Calculate adjusted power (simplified to avoid complications)
    pr['form_component'] = np.where(
        pr['games_played'] > 0,
        (pr['win_pct'] - 0.5) * 10,  # Reduced multiplier
        0
    )
    
    pr['adjusted_power'] = pr['power_score'] + pr['form_component']
    
    # Build record string
    pr['record'] = pr.apply(lambda r: f"{int(r.get('wins', 0))}-{int(r.get('losses', 0))}", axis=1)
    
    # Sort by adjusted power
    pr['team_full'] = pr['team'].map(to_full)
    pr = pr.sort_values('adjusted_power', ascending=False).reset_index(drop=True)
    pr['rank'] = pr.index + 1

    # FINAL CHECK: Ensure no duplicates in output
    seen_teams = set()
    final_rankings = []
    
    for r in pr.itertuples():
        if r.team_full not in seen_teams:
            seen_teams.add(r.team_full)
            final_rankings.append({
                'rank': int(r.rank),
                'team': r.team_full,
                'record': r.record,
                'power_score': float(round(r.adjusted_power, 2)),
                'base_power': float(round(r.power_score, 2)),
                'injury_impact': None,
                'injuries': None
            })

    return jsonify(final_rankings)






# ======================
# API: /api/predictions
# ======================
# ======================
# API: /api/predictions
# ======================
@app.route('/api/predictions')
def api_predictions():
    """Predictions using ML when possible, otherwise power-based fallback (no crashes)."""
    conn = get_db()
    today = datetime.utcnow().date()
    horizon = today + timedelta(days=21)

    # Load games
    try:
        games = pd.read_sql_query(text("""
        SELECT game_id, away_team AS away, home_team AS home, game_date, start_time_local AS game_time
        FROM games
        WHERE date(game_date) BETWEEN date(:start_date) AND date(:end_date)
        ORDER BY date(game_date), time(start_time_local)
    """), conn, params={"start_date": today, "end_date": horizon})

    except Exception:
        return jsonify([])

    # --- build power fallback once ---
    # (matches your existing logic)
    try:
        pmap = get_power_map_cached(conn)  # your helper
    except Exception:
        pmap = {}

    HFA = 2.5
    def to_full(name: str | None) -> str:
        return TEAM_TO_FULL.get(name, name or "Unknown")

    def win_prob_fallback(away_abbr, home_abbr):
        aw = pmap.get(to_full(away_abbr), pmap.get(away_abbr, 0.0))
        hm = pmap.get(to_full(home_abbr), pmap.get(home_abbr, 0.0)) + HFA
        ph = 1.0 / (1.0 + math.exp(-(hm - aw) / 8.0))
        return 1.0 - ph, ph  # away, home

    ml_system = get_ml_prediction_system()
    rows = []

    for _, g in games.iterrows():
        used_ml = False
        prediction_result = None

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
                    'game_time': g['game_time'] if g['game_time'] else 'TBD',
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

        # 2) Fallback path (NO ML FIELDS TOUCHED)
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
                'game_time': g['game_time'] if g['game_time'] else 'TBD',
                'model_prediction': False,
                'power_difference': 0,
                'key_factors': {},
                'home_team': to_full(g['home']),
                'away_team': to_full(g['away'])
            })
        except Exception as e:
            print(f"Error predicting game {g['away']} @ {g['home']}: {e}")
            continue

    # Sort by date/time (keep your existing sort)
    rows.sort(key=lambda r: (r['game_date'], (r.get('game_time') or '99:99')[:5]))
    return jsonify(rows)



def to_full(name: str | None) -> str:
    """Convert team abbreviation or partial name to full team name"""
    if not name:
        return "Unknown"
    
    # Use your existing TEAM_TO_FULL mapping or create one
    return TEAM_TO_FULL.get(name, name)


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
def api_betting_analysis():
    """FIXED: Now properly matches team names between games and odds"""
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
        # Get games in the time window
        games = pd.read_sql_query(text("""
            SELECT game_id, away_team, home_team, game_date, start_time_local
            FROM games 
            WHERE date(game_date) BETWEEN :start_date AND :end_date
            ORDER BY game_date, start_time_local
        """), conn, params={"start_date": start, "end_date": end})

        if games.empty:
            return jsonify({
                "opportunities": [],
                "total_found": 0,
                "user_bankroll": user_bankroll,
                "message": f"No games found between {start} and {end}"
            })

        print(f"FIXED: Found {len(games)} games in window")

        # Get ML system predictions for these games
        ml_system = get_ml_prediction_system()
        
        for _, game in games.iterrows():
            try:
                if ml_system:
                    # Get ML prediction
                    prediction_result = ml_system.predict_game(
                        home_team=game['home_team'],
                        away_team=game['away_team'],
                        game_date=str(game['game_date'])
                    )
                    
                    home_prob = prediction_result['home_win_probability']
                    away_prob = prediction_result['away_win_probability']
                    confidence = prediction_result['confidence']
                    
                    print(f"FIXED: {game['away_team']} @ {game['home_team']}")
                    print(f"  Model: Home {home_prob:.1%}, Away {away_prob:.1%}")
                    
                    # FIXED: Get odds using proper team name matching
                    # First, get ALL odds (we'll match by team name, not game_id since game_id is null)
                    odds_df = pd.read_sql_query(text("""
                        SELECT team, sportsbook, odds
                        FROM odds 
                        WHERE market = 'h2h'
                        AND odds IS NOT NULL
                        AND odds BETWEEN -800 AND 800
                        ORDER BY timestamp DESC
                    """, conn))
                    
                    if odds_df.empty:
                        print(f"  No odds found in database")
                        continue
                    
                    # Check each team for value using FIXED team name matching
                    for team_abbr in [game['home_team'], game['away_team']]:
                        # Convert abbreviation to full name for odds lookup
                        full_team_name = normalize_team_for_odds_lookup(team_abbr)
                        model_prob = home_prob if team_abbr == game['home_team'] else away_prob
                        
                        print(f"  Looking for odds for: {team_abbr} -> {full_team_name}")
                        
                        # Find odds for this team using full name
                        team_odds = odds_df[odds_df['team'] == full_team_name]
                        
                        if team_odds.empty:
                            print(f"    No odds found for {full_team_name}")
                            continue
                        
                        print(f"    Found {len(team_odds)} odds records")
                        
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
                                "game_id": str(game.get('game_id', 'unknown')),
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
            return jsonify({'ok': False, 'error': 'User not logged in'}), 401
            
        user_data = USERS.get(username, {})
        user_bankroll = float(user_data.get('bankroll', 500))
        
        conn = get_db()
        today = datetime.utcnow().date()
        end = today + timedelta(days=7)
        
        # Get upcoming games
        games = pd.read_sql_query(text("""
            SELECT game_id, away_team, home_team, game_date, start_time_local
            FROM games 
            WHERE date(game_date) BETWEEN :start_date AND :end_date
            ORDER BY game_date, start_time_local
        """), conn, params={"start_date": today, "end_date": end})

        debug_info = []
        recommendations = []
        ml_system = get_ml_prediction_system()
        
        for _, game in games.iterrows():
            game_debug = {
                'game': f"{game['away_team']} @ {game['home_team']}",
                'has_ml_system': ml_system is not None,
                'has_odds': False,
                'predictions': None,
                'opportunities': []
            }
            
            if ml_system:
                try:
                    # Get ML prediction
                    prediction_result = ml_system.predict_game(
                        home_team=game['home_team'],
                        away_team=game['away_team'],
                        game_date=str(game['game_date'])
                    )
                    
                    game_debug['predictions'] = {
                        'home_prob': prediction_result['home_win_probability'],
                        'away_prob': prediction_result['away_win_probability'],
                        'confidence': prediction_result['confidence']
                    }
                    
                    # LOWERED THRESHOLDS FOR TESTING
                    if prediction_result['confidence'] < 0.51:  # Was 0.58, now 0.51
                        game_debug['skip_reason'] = f"Low confidence: {prediction_result['confidence']:.1%}"
                        debug_info.append(game_debug)
                        continue
                    
                    # Get odds
                    odds_df = pd.read_sql_query(text("""
                        SELECT team, sportsbook, odds
                        FROM odds 
                        WHERE market = 'h2h'
                        AND odds IS NOT NULL
                        AND odds BETWEEN -800 AND 800
                        ORDER BY timestamp DESC
                    """), conn)
                    
                    game_debug['has_odds'] = not odds_df.empty
                    game_debug['odds_count'] = len(odds_df)
                    
                    if odds_df.empty:
                        game_debug['skip_reason'] = "No odds found"
                        debug_info.append(game_debug)
                        continue
                    
                    # Check both teams for value with LOWER threshold
                    for team_name in [game['home_team'], game['away_team']]:
                        model_prob = prediction_result['home_win_probability'] if team_name == game['home_team'] else prediction_result['away_win_probability']
                        
                        # LOWERED threshold from 0.52 to 0.51
                        if model_prob < 0.51:
                            continue
                        
                        # Find odds
                        team_odds = odds_df[odds_df['team'] == team_name]
                        if team_odds.empty:
                            team_odds = odds_df[odds_df['team'].str.contains(team_name, case=False, na=False)]
                        if team_odds.empty:
                            continue
                        
                        # Get best odds
                        best_row = team_odds.loc[team_odds['odds'].idxmax()]
                        odds_val = float(best_row['odds'])
                        
                        # Calculate edge
                        if odds_val > 0:
                            implied_prob = 100 / (odds_val + 100)
                            decimal_odds = 1 + (odds_val / 100)
                        else:
                            implied_prob = abs(odds_val) / (abs(odds_val) + 100)
                            decimal_odds = 1 + (100 / abs(odds_val))
                        
                        edge = model_prob - implied_prob
                        edge_pct = edge * 100
                        
                        opportunity = {
                            'team': team_name,
                            'model_prob': f"{model_prob:.1%}",
                            'implied_prob': f"{implied_prob:.1%}", 
                            'edge_pct': f"{edge_pct:.1f}%",
                            'odds': odds_val
                        }
                        game_debug['opportunities'].append(opportunity)
                        
                        # LOWERED edge threshold from 3% to 1%
                        if edge_pct >= 1.0:  # Was 3.0%, now 1.0%
                            kelly = (model_prob * decimal_odds - 1) / (decimal_odds - 1)
                            stake = max(5.0, min(
                                user_bankroll * 0.05,
                                kelly * user_bankroll * 0.25
                            ))
                            
                            recommendations.append({
                                'game': f"{game['away_team']} @ {game['home_team']}",
                                'team': team_name,
                                'odds': int(odds_val),
                                'edge_percentage': round(edge_pct, 1),
                                'recommended_stake': round(stake, 2),
                                'confidence_level': "Debug",
                                'reason': f"{edge_pct:.1f}% edge (debug mode)"
                            })
                
                except Exception as e:
                    game_debug['error'] = str(e)
            
            debug_info.append(game_debug)
        
        return jsonify({
            'ok': True,
            'success': True,
            'debug_info': debug_info,
            'result': {
                'recommendations': recommendations,
                'bankroll': user_bankroll,
                'total_recommended': sum(r['recommended_stake'] for r in recommendations),
                'games_analyzed': len(games),
                'games_with_odds': sum(1 for g in debug_info if g['has_odds']),
                'games_with_predictions': sum(1 for g in debug_info if g['predictions']),
                'note': 'DEBUG MODE: Lowered thresholds to 51% confidence and 1% edge'
            }
        })
        
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


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
        games = pd.read_sql_query(text("""
            SELECT game_id, away_team, home_team, game_date, start_time_local
            FROM games 
            WHERE date(game_date) BETWEEN :start_date AND :end_date
            ORDER BY game_date, start_time_local
        """), conn, params={"start_date": today, "end_date": end})

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

# Add this to mobile_dashboard.py to fix the team name matching

def normalize_team_for_odds_lookup(team_abbr):
    """Convert team abbreviation to full name for odds lookup"""
    ABBR_TO_ODDS_NAME = {
        'ARI': 'Arizona Cardinals',
        'ATL': 'Atlanta Falcons', 
        'BAL': 'Baltimore Ravens',
        'BUF': 'Buffalo Bills',
        'CAR': 'Carolina Panthers',
        'CHI': 'Chicago Bears',
        'CIN': 'Cincinnati Bengals',
        'CLE': 'Cleveland Browns',
        'DAL': 'Dallas Cowboys',
        'DEN': 'Denver Broncos',
        'DET': 'Detroit Lions',
        'GB': 'Green Bay Packers',
        'HOU': 'Houston Texans',
        'IND': 'Indianapolis Colts',
        'JAX': 'Jacksonville Jaguars',
        'KC': 'Kansas City Chiefs',
        'LV': 'Las Vegas Raiders',
        'LAC': 'Los Angeles Chargers',
        'LAR': 'Los Angeles Rams',
        'MIA': 'Miami Dolphins',
        'MIN': 'Minnesota Vikings',
        'NE': 'New England Patriots',
        'NO': 'New Orleans Saints',
        'NYG': 'New York Giants',
        'NYJ': 'New York Jets',
        'PHI': 'Philadelphia Eagles',
        'PIT': 'Pittsburgh Steelers',
        'SF': 'San Francisco 49ers',
        'SEA': 'Seattle Seahawks',
        'TB': 'Tampa Bay Buccaneers',
        'TEN': 'Tennessee Titans',
        'WAS': 'Washington Commanders'
    }
    return ABBR_TO_ODDS_NAME.get(team_abbr, team_abbr)

@app.route('/api/ai-betting-recommendations', methods=['GET'])
def get_betting_recommendations():
    """FIXED: Now uses proper team name matching"""
    try:
        username = session.get('username')
        if not username:
            return jsonify({'ok': False, 'error': 'User not logged in'}), 401
            
        user_data = USERS.get(username, {})
        user_bankroll = float(user_data.get('bankroll', 500))
        
        conn = get_db()
        today = datetime.utcnow().date()
        end = today + timedelta(days=7)
        
        # Get upcoming games
        games = pd.read_sql_query(text("""
            SELECT game_id, away_team, home_team, game_date, start_time_local
            FROM games 
            WHERE date(game_date) BETWEEN :start_date AND :end_date
            ORDER BY game_date, start_time_local
        """), conn, params={"start_date": today, "end_date": end})

        recommendations = []
        total_staked = 0.0
        ml_system = get_ml_prediction_system()
        
        # Get all odds once
        all_odds = pd.read_sql_query(text("""
            SELECT team, sportsbook, odds
            FROM odds 
            WHERE market = 'h2h'
            AND odds IS NOT NULL
            AND odds BETWEEN -800 AND 800
            ORDER BY timestamp DESC
        """), conn)
        
        for _, game in games.iterrows():
            if not ml_system:
                continue
                
            try:
                # Get ML prediction
                prediction_result = ml_system.predict_game(
                    home_team=game['home_team'],
                    away_team=game['away_team'],
                    game_date=str(game['game_date'])
                )
                
                home_prob = prediction_result['home_win_probability']
                away_prob = prediction_result['away_win_probability']
                confidence = prediction_result['confidence']
                
                # Only recommend high-confidence picks
                if confidence < 0.55:  # Lowered from 0.58
                    continue
                
                # Check both teams for value using FIXED team matching
                for team_abbr in [game['home_team'], game['away_team']]:
                    model_prob = home_prob if team_abbr == game['home_team'] else away_prob
                    
                    # Only recommend if model is confident in this team
                    if model_prob < 0.52:
                        continue
                    
                    # FIXED: Find odds using proper team name conversion
                    full_team_name = normalize_team_for_odds_lookup(team_abbr)
                    team_odds = all_odds[all_odds['team'] == full_team_name]
                    
                    if team_odds.empty:
                        continue
                    
                    # Get best odds
                    best_row = team_odds.loc[team_odds['odds'].idxmax()]
                    odds_val = float(best_row['odds'])
                    
                    # Handle decimal vs American odds (same as above)
                    if 1.01 <= odds_val <= 10.0:  # Decimal odds
                        implied_prob = 1.0 / odds_val
                        american_odds = int((odds_val - 1) * 100) if odds_val >= 2.0 else int(-100 / (odds_val - 1))
                        decimal_odds = odds_val
                    else:  # American odds
                        american_odds = int(odds_val)
                        if odds_val > 0:
                            implied_prob = 100 / (odds_val + 100)
                            decimal_odds = 1 + (odds_val / 100)
                        else:
                            implied_prob = abs(odds_val) / (abs(odds_val) + 100)
                            decimal_odds = 1 + (100 / abs(odds_val))
                    
                    edge = model_prob - implied_prob
                    edge_pct = edge * 100
                    
                    # Only recommend 2%+ edges (lowered from 3%)
                    if edge_pct < 2.0:
                        continue
                    
                    # Calculate stake
                    kelly = (model_prob * decimal_odds - 1) / (decimal_odds - 1)
                    stake = max(5.0, min(
                        user_bankroll * 0.05,  # 5% max
                        kelly * user_bankroll * 0.25  # 25% Kelly
                    ))
                    
                    # Confidence level
                    if edge_pct > 6:
                        confidence_level = "High"
                    elif edge_pct > 3:
                        confidence_level = "Medium"
                    else:
                        confidence_level = "Low"
                    
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
                        'confidence_level': confidence_level,
                        'reason': f"{edge_pct:.1f}% model edge with {confidence*100:.0f}% prediction confidence"
                    })
                    
                    total_staked += stake
                    
            except Exception as e:
                print(f"Error processing game: {e}")
                continue
        
        # Sort by edge
        recommendations.sort(key=lambda x: x['edge_percentage'], reverse=True)
        
        return jsonify({
            'ok': True,
            'success': True,
            'result': {
                'recommendations': recommendations,
                'bankroll': user_bankroll,
                'total_recommended': round(total_staked, 2),
                'remaining_budget': round(user_bankroll * 0.10 - total_staked, 2),
                'risk_level': 'Conservative' if total_staked < user_bankroll * 0.05 else 'Moderate',
                'games_scanned': len(games),
                'minimum_edge': '2.0%',
                'minimum_confidence': '55%'
            }
        })
        
    except Exception as e:
        print(f"Error in get_betting_recommendations: {e}")
        return jsonify({
            'ok': False,
            'success': False,
            'error': str(e)
        }), 500

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
    conn = get_db()
    today = datetime.utcnow().date()
    end = today + timedelta(days=60)
    try:
        games = pd.read_sql_query(
            text("""
            SELECT game_id, away_team AS away, home_team AS home, game_date, start_time_local AS game_time
            FROM games
            WHERE date(game_date) BETWEEN :start_date AND :end_date
            ORDER BY date(game_date), start_time_local
            """),
            conn, params={"start_date": today, "end_date": end}
        )

        game_ids = games['game_id'].tolist()
        if game_ids:
            ph = ",".join(["?"] * len(game_ids))
            odds = pd.read_sql_query(f"""
                SELECT o.game_id, o.team, o.sportsbook, o.odds, o.timestamp
                FROM odds o
                JOIN (
                    SELECT game_id, team, sportsbook, MAX(timestamp) AS ts
                    FROM odds
                    WHERE market='h2h' AND game_id IN ({ph})
                    GROUP BY game_id, team, sportsbook
                ) x ON x.game_id=o.game_id AND x.team=o.team AND x.sportsbook=o.sportsbook AND x.ts=o.timestamp
            """, conn, params=game_ids)
            
            # FIXED: Better odds processing
            odds_processed = []
            for _, row in odds.iterrows():
                normalized = normalize_american_odds(row['odds'])
                if normalized is not None:  # Only include valid odds
                    odds_processed.append({
                        'game_id': row['game_id'],
                        'team': row['team'],
                        'sportsbook': row['sportsbook'],
                        'odds': row['odds'],
                        'ao': normalized,
                        'timestamp': row['timestamp']
                    })
            
            odds = pd.DataFrame(odds_processed)
        else:
            odds = pd.DataFrame(columns=['game_id','team','sportsbook','odds','timestamp','ao'])
            
        out = []
        for _, g in games.iterrows():
            o = odds[odds['game_id']==g['game_id']] if not odds.empty else pd.DataFrame()
            teams = []
            
            for tm in (g['home'], g['away']):
                ot = o[o['team']==tm] if not o.empty else pd.DataFrame()
                
                if ot.empty:
                    # FIXED: Use realistic default odds instead of 100
                    teams.append({
                        "team": to_full(tm), 
                        "odds": -110,  # Standard line instead of 100
                        "sportsbook": "No Line", 
                        "by_book": []
                    })
                else:
                    # Find best odds (highest for positive, closest to 0 for negative)
                    best_idx = None
                    best_value = None
                    
                    for idx, row in ot.iterrows():
                        odds_val = int(row['ao'])
                        if best_value is None:
                            best_value = odds_val
                            best_idx = idx
                        elif odds_val > 0 and best_value > 0:
                            # Both positive, take higher
                            if odds_val > best_value:
                                best_value = odds_val
                                best_idx = idx
                        elif odds_val < 0 and best_value < 0:
                            # Both negative, take closer to zero (less negative)
                            if odds_val > best_value:
                                best_value = odds_val
                                best_idx = idx
                        elif odds_val > 0 and best_value < 0:
                            # Positive beats negative
                            best_value = odds_val
                            best_idx = idx
                    
                    best_odds = int(ot.loc[best_idx,'ao']) if best_idx is not None else -110
                    best_book = str(ot.loc[best_idx,'sportsbook']) if best_idx is not None else "Unknown"
                    
                    by_book = [
                        {"sportsbook": str(r['sportsbook']), "odds": int(r['ao'])}
                        for _, r in ot.iterrows()
                    ]

                    teams.append({
                        "team": to_full(tm),
                        "odds": best_odds,
                        "sportsbook": best_book,
                        "by_book": by_book
                    })

            out.append({
                "game_id": str(g['game_id']),
                "game": f"{to_full(g['away'])} @ {to_full(g['home'])}",
                "date": str(g['game_date']),
                "time": (str(g['game_time'])[:5] if g['game_time'] else "TBD"),
                "teams": teams
            })
        return jsonify(out)
    except Exception as e:
        print('games error', e)
        return jsonify([]), 200

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

# In mobile_dashboard.py, add this to app startup
@app.before_request
def force_reload_models():
    global _model_pack, _ml_prediction_system, _initialized
    if not _initialized:
        _model_pack = None
        _ml_prediction_system = None
        print("Forced model cache clear on startup")


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
# Health
@app.route('/api/health')
def api_health():
    try:
        if DB_KIND == "pg":
            with ENGINE.connect() as conn:
                has_tss = conn.execute(text("SELECT to_regclass('public.team_season_summary')")).scalar()
                has_games = conn.execute(text("SELECT to_regclass('public.games')")).scalar()
                has_odds  = conn.execute(text("SELECT to_regclass('public.odds')")).scalar()
                return jsonify({
                    'backend': 'postgres',
                    'db_url_or_path': DB_INFO,
                    'team_season_summary': bool(has_tss),
                    'games': bool(has_games),
                    'odds': bool(has_odds),
                })
        else:
            # SQLite
            con = sqlite3.connect(DB_INFO)
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
                'db_url_or_path': DB_INFO,
                'team_season_summary': has_tss,
                'games': has_games,
                'odds': has_odds,
            })
    except Exception as e:
        return jsonify({'error': str(e), 'db_url_or_path': DB_INFO, 'backend': DB_KIND}), 500


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