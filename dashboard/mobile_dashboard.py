
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


# =========================
# DB PATH (single source)
# =========================
DEFAULT_DB = r"E:/Bettr Bot/betting-bot/data/betting.db"
DB_PATH = os.environ.get("BETTR_DB_PATH", DEFAULT_DB)

# SQLAlchemy engine for summary stats
_engine = create_engine(f"sqlite:///{DB_PATH}")

# Flask app
app = Flask(__name__)
app.secret_key = 'bettr-bot-enhanced-2025'
# --- ADD: one-time indexes + WAL ---
def ensure_indexes():
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
USER_DATA_FILE = os.environ.get(
    "BETTR_USERS_PATH",
    os.path.join(BASE_DIR, "..", "user_accounts.json")  # lives in project root
)
app.secret_key = os.environ.get("FLASK_SECRET", "bettr-bot-enhanced-2025")

@app.before_request
def _init_once():
    global _initialized
    if not _initialized:
        ensure_indexes()
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
            "SELECT team, power_score, games_played, win_pct FROM team_season_summary WHERE season = ?",
            conn, params=[season]
        )
        base['team'] = base['team'].map(to_abbr)
        if base.empty:
            base = pd.read_sql_query(
                "SELECT team, power_score, games_played, win_pct FROM team_season_summary WHERE season = ?",
                conn, params=[season - 1]
            )
    except Exception:
        base = pd.DataFrame(columns=['team','power_score','games_played','win_pct'])

    # 2) Injury view (keep it light so we don’t drive everything negative)
    # In get_unified_power_scores()
    try:
        inj = load_injury_impact_from_detail(conn)
        # keep columns: team, injury_impact, qb_risk
    except Exception:
        inj = pd.DataFrame(columns=['team','injury_impact','qb_risk'])


    df = base.merge(inj, on='team', how='left')
    df['injury_impact'] = df['injury_impact'].fillna(0.0)
    df['qb_risk'] = df['qb_risk'].fillna(0)

    # 3) Small “form” component so 0–0 teams don’t all look identical
    df['form_component'] = df.apply(
        lambda r: (r['win_pct'] - 0.5) * 20 if pd.notnull(r['win_pct']) and pd.notnull(r['games_played']) and r['games_played'] > 0 else 0.0,
        axis=1
    )

    # 4) Final adjusted power (keep roughly your historical 0–12 feel)
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
    if not hasattr(g, '_db'):
        g._db = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
        g._db.row_factory = sqlite3.Row
    return g._db

@app.teardown_appcontext
def _close_db(_exc):
    db = getattr(g, '_db', None)
    if db is not None:
        db.close()

# -----------------
# User management
# -----------------
USER_DATA_FILE = 'user_accounts.json'

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
@app.route('/')
@login_required
def dashboard():
    username = session['username']
    user = USERS[username]
    conn = get_db()  # use sqlite3 connection everywhere here

    # top row stats
    try:
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
            "SELECT team, power_score, games_played, win_pct FROM team_season_summary WHERE season=?",
            conn, params=[season]
        )
        if rankings_df.empty:
            rankings_df = pd.read_sql_query(
                "SELECT team, power_score, games_played, win_pct FROM team_season_summary WHERE season=?",
                conn, params=[season - 1]
            )

        injuries_df = load_injury_impact_from_detail(conn)[['team','injury_impact']]


        merged_df = rankings_df.merge(injuries_df, on='team', how='left')
        merged_df['injury_impact'] = merged_df['injury_impact'].fillna(0.0)
        merged_df['form_component'] = np.where(merged_df['games_played'] > 0, (merged_df['win_pct'] - 0.5) * 20, 0)
        # lighter injury weight so numbers don’t go negative
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
    """Enhanced rankings using team_season_summary with injuries and recent form."""
    conn = get_db()
    season, _phase = current_phase_and_season()
    
    try:
        # Get base power scores from team_season_summary
        pr = pd.read_sql_query("""
            SELECT
                team,
                power_score,
                wins,
                losses,
                games_played,
                win_pct,
                point_diff,
                preseason_scheduled,
                preseason_completed
            FROM team_season_summary
            WHERE season = ?
        """, conn, params=[season])
        pr['team'] = pr['team'].map(to_abbr)
        if pr.empty:
            # Fall back to previous season
            pr = pd.read_sql_query("""
                SELECT team, power_score, wins, losses, games_played, win_pct, point_diff
                FROM team_season_summary 
                WHERE season = ?
            """, conn, params=[season-1])
    except Exception as e:
        print(f"Rankings query error: {e}")
        return jsonify([])

    # Get injury impacts from the view
    try:
        injv = load_injury_impact_from_detail(conn)
        # We already provide injury_impact, total_injuries, qb_risk, skill_position_risk
        # If you want a slightly stronger penalty, you can optionally do:
        injv = injv.assign(injury_impact = injv['injury_impact'] + 0.7*injv['qb_risk'] + 0.4*injv['skill_position_risk'])
    except Exception:
        injv = pd.DataFrame(columns=['team','injury_impact','total_injuries','qb_risk'])


    # Get recent form (last 3 games trend)
    try:
        recent = pd.read_sql_query("""
            SELECT 
                CASE 
                    WHEN home_team IN (SELECT DISTINCT team FROM team_season_summary) 
                    THEN home_team 
                    ELSE away_team 
                END as team,
                AVG(CASE 
                    WHEN home_score > away_score AND home_team = team THEN 1.0
                    WHEN away_score > home_score AND away_team = team THEN 1.0
                    ELSE 0.0
                END) as recent_win_rate
            FROM (
                SELECT * FROM games 
                WHERE home_score IS NOT NULL 
                ORDER BY game_date DESC 
                LIMIT 100
            )
            GROUP BY team
        """, conn)
    except Exception:
        recent = pd.DataFrame(columns=['team','recent_win_rate'])

    # Merge all data
    df = pr.copy()
    df = df.merge(injv, on='team', how='left')
    df = df.merge(recent, on='team', how='left')
    
    # Fill missing values
    df['injury_impact'] = df['injury_impact'].fillna(0.0)
    df['recent_win_rate'] = df['recent_win_rate'].fillna(df['win_pct'])
    df['total_injuries'] = df['total_injuries'].fillna(0)
    df['qb_risk'] = df['qb_risk'].fillna(0)
    
    # Calculate adjusted power score
    # Base power (60%) + Recent form (20%) - Injuries (20%)
    # NEW LOGIC:
    # Only apply the 'recent form' component if regular season games have been played
    df['form_component'] = np.where(
        df['games_played'] > 0,
        (df['recent_win_rate'] - 0.5) * 20,
        0  # Otherwise, the component is neutral (zero)
    )

    df['adjusted_power'] = (
        df['power_score'] * 0.6 +
        df['form_component'] * 0.2 -  # Use the new form component
        df['injury_impact'] * 0.2
    )
        
    # Special adjustment for QB injuries
    df.loc[df['qb_risk'] > 0, 'adjusted_power'] -= 2.0
    
    # Build record string
    df['record'] = df.apply(lambda r: {
        'regular': f"{int(r.get('wins', 0))}-{int(r.get('losses', 0))}",
        'preseason': f"({int(r.get('preseason_completed', 0))} PS)" if r.get('preseason_completed', 0) > 0 else ""
    }, axis=1)
    
    # Sort by adjusted power
    df['team_full'] = df['team'].map(to_full)
    df = df.sort_values('adjusted_power', ascending=False).reset_index(drop=True)
    df['rank'] = df.index + 1

    return jsonify([
        {
            'rank': int(r.rank),
            'team': r.team_full,
            'record': f"{r.record['regular']} {r.record['preseason']}".strip(),
            'power_score': float(round(r.adjusted_power, 2)),
            'base_power': float(round(r.power_score, 2)),
            'injury_impact': float(round(r.injury_impact, 2)) if r.injury_impact > 0 else None,
            'injuries': int(r.total_injuries) if r.total_injuries > 0 else None
        } for r in df.itertuples()
    ])

# ======================
# API: /api/predictions
# ======================
@app.route('/api/predictions')
def api_predictions():
    conn = get_db()
    season, _phase = current_phase_and_season()
    today = datetime.utcnow().date()
    horizon = today + timedelta(days=21)

    try:
        games = pd.read_sql_query("""
            SELECT
                game_id, away_team AS away, home_team AS home,
                STRFTIME('%Y-%m-%d', game_date) AS game_date,
                STRFTIME('%H:%M', start_time_local) AS game_time
            FROM games WHERE date(game_date) BETWEEN date(?) AND date(?)
            ORDER BY date(game_date), time(start_time_local)
        """, conn, params=[today, horizon])
    except Exception:
        return jsonify([])

    # === START: UNIFIED POWER SCORE LOGIC ===
    # This block is now consistent with the /api/rankings endpoint

    try:
        # Get base power and win percentages
        pr = pd.read_sql_query("""
            SELECT team, power_score, games_played, win_pct
            FROM team_season_summary WHERE season = ?
        """, conn, params=[season])
        pr['team'] = pr['team'].map(to_abbr)

        if pr.empty:
            pr = pd.read_sql_query("""
                SELECT team, power_score, games_played, win_pct
                FROM team_season_summary WHERE season = ?
            """, conn, params=[season - 1])
    except Exception:
        pr = pd.DataFrame(columns=['team', 'power_score', 'games_played', 'win_pct'])

    try:
        injv = load_injury_impact_from_detail(conn)[['team','injury_impact']]
    except Exception:
        injv = pd.DataFrame(columns=['team','injury_impact'])


    # Merge data and calculate final adjusted power score
    # In api_predictions() function
    power = pr.merge(injv, on='team', how='left')
    power['injury_impact'] = power['injury_impact'].fillna(0.0) # Only fill the injury column

    # This is the same corrected logic from the rankings endpoint
    power['form_component'] = np.where(
        power['games_played'] > 0,
        (power['win_pct'] - 0.5) * 20, # In preseason, win_pct is 0, so this won't apply
        0
    )
    power['adj_power'] = (
        power['power_score'] * 0.6 +
        power['form_component'] * 0.2 -
        power['injury_impact'] * 0.10  # lighter injury weight
    )
    conn = get_db()
    pmap = get_power_map_cached(conn)

    for abbr, full in ABBR_TO_FULL.items():
        if full in pmap:
            pmap[abbr] = pmap[full]

    HFA = 2.5  # Home field advantage

    def win_prob(away, home):
        aw = pmap.get(to_full(away), pmap.get(away, 0.0))
        hm = pmap.get(to_full(home), pmap.get(home, 0.0)) + HFA
        ph = 1.0 / (1.0 + math.exp(-(hm - aw) / 8.0))
        return 1.0 - ph, ph

    # === END: UNIFIED POWER SCORE LOGIC ===

    rows = []
    for _, g in games.iterrows():
        pa, ph = win_prob(g['away'], g['home'])
        pick_abbr = g['home'] if ph >= pa else g['away']
        confidence = max(pa, ph)

        rows.append({
            'matchup': f"{to_full(g['away'])} @ {to_full(g['home'])}",
            'prediction': to_full(pick_abbr),
            'confidence': round(float(confidence), 3),
            'game_date': str(g['game_date']),
            'game_time': g['game_time'] if g['game_time'] else 'TBD',
            'game_id': g['game_id']
        })

    return jsonify(rows)
# =============================
# API: /api/betting-analysis
# =============================
@app.route('/api/betting-analysis')
def api_betting_analysis():
    conn = get_db()
    week = request.args.get('week', 'current')
    edge_filter = request.args.get('edge', 'all')

    # time window
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
        start, end = today - timedelta(days=7), today + timedelta(days=60)

    include_negative = (edge_filter == 'all')
    min_edge = {'all': 0.0, 'positive': 0.0001, '2': 0.02, '5': 0.05}.get(edge_filter, 0.0)

    # user bankroll (per-user basis)
    try:
        username = session.get('username', '')
        user_bankroll = float(USERS.get(username, {}).get('bankroll', 100.0))
    except Exception:
        user_bankroll = 100.0

    # staking + portfolio controls
    RISK_FRACTION = 0.25    # quarter-Kelly
    MAX_BET_PCT  = 0.05     # 5% cap per bet
    MIN_BET      = 1.00     # $1 floor
    SLATE_BUDGET_PCT = 0.10 # total new exposure this slate/day
    PER_GAME_CAP_PCT = 0.06 # total exposure per game

    # games in window
    try:
        games = pd.read_sql_query(
            """
            SELECT game_id, away_team AS away, home_team AS home, game_date, start_time_local AS game_time
            FROM games
            WHERE date(game_date) BETWEEN date(?) AND date(?)
            ORDER BY date(game_date), time(start_time_local)
            """,
            conn, params=[start, end]
        )
    except Exception:
        return jsonify({"opportunities": [], "total_found": 0, "week": week, "edge_filter": edge_filter,
                        "user_bankroll": round(user_bankroll, 2), "max_bet_cap": round(user_bankroll*MAX_BET_PCT, 2),
                        "slate_budget": round(user_bankroll*SLATE_BUDGET_PCT, 2), "total_recommended": 0.0})

    if games.empty:
        return jsonify({"opportunities": [], "total_found": 0, "week": week, "edge_filter": edge_filter,
                        "user_bankroll": round(user_bankroll, 2), "max_bet_cap": round(user_bankroll*MAX_BET_PCT, 2),
                        "slate_budget": round(user_bankroll*SLATE_BUDGET_PCT, 2), "total_recommended": 0.0})

    game_ids = games['game_id'].tolist()
    ph = ",".join(["?"] * len(game_ids))

    # latest moneyline per (game, team, book)
    try:
        odds = pd.read_sql_query(f"""
            SELECT o.game_id, o.team, o.sportsbook, o.odds, o.timestamp
            FROM odds o
            JOIN (
                SELECT game_id, team, sportsbook, MAX(timestamp) AS ts
                FROM odds
                WHERE market='h2h' AND game_id IN ({ph})
                GROUP BY game_id, team, sportsbook
            ) x ON x.game_id = o.game_id
               AND x.team = o.team
               AND x.sportsbook = o.sportsbook
               AND x.ts = o.timestamp
        """, conn, params=game_ids)
        odds['ao'] = odds['odds'].apply(normalize_american_odds)
    except Exception:
        odds = pd.DataFrame(columns=['game_id','team','sportsbook','odds','timestamp','ao'])

    # model prob via cached power map (uses injuries + form inside)
    pmap = get_power_map_cached(conn)
    for abbr, full in ABBR_TO_FULL.items():
        if full in pmap:
            pmap[abbr] = pmap[full]

    HFA = 2.5
    def model_prob(away, home, team):
        hm = pmap.get(to_full(home), pmap.get(home, 0.0)) + HFA
        aw = pmap.get(to_full(away), pmap.get(away, 0.0))
        ph_ = 1.0 / (1.0 + math.exp(-(hm - aw) / 8.0))
        return ph_ if to_full(team) == to_full(home) else (1.0 - ph_)

    def american_to_prob(od):
        od = float(od)
        return 100.0/(od+100.0) if od > 0 else abs(od)/(abs(od) + 100.0)

    def best_line(df_team: pd.DataFrame):
        if df_team.empty:
            return None, None
        t = df_team.dropna(subset=['ao']).copy()
        if t.empty:
            return None, None
        t['ao'] = t['ao'].astype(int)
        idx = t['ao'].idxmax()
        return int(t.loc[idx, 'ao']), str(t.loc[idx, 'sportsbook'])


    opps = []
    for _, g in games.itertuples(index=False).items() if False else []:  # placeholder to appease linters
        pass  # (no-op)

    # real loop
    for _, g in games.iterrows():
        gid, away, home = g['game_id'], g['away'], g['home']
        o_game = odds[odds['game_id'] == gid].copy()
        o_game['ao'] = o_game['odds'].apply(normalize_american_odds)

        for team in (home, away):
            prob = model_prob(away, home, team)
            team_odds = o_game[o_game['team'] == team]
            ml, book = best_line(team_odds)
            if ml is None:
                ml, book = (100 if prob > 0.5 else -110), 'No Line'

            dec = 1 + (ml / 100.0) if ml > 0 else 1 + (100.0 / abs(ml))
            implied = american_to_prob(ml)
            edge = prob - implied

            # filter by requested edge view
            if not (include_negative or edge >= min_edge):
                continue

            # Kelly stake (pre-allocator)
            kelly = ((prob * (dec - 1)) - (1 - prob)) / (dec - 1) if dec > 1 else 0.0
            stake_raw = max(0.0, kelly) * user_bankroll * RISK_FRACTION
            stake_cap = user_bankroll * MAX_BET_PCT
            stake = max(MIN_BET, min(stake_cap, stake_raw))

            opps.append({
                "game": f"{to_full(away)} @ {to_full(home)}",
                "date": str(g['game_date']),
                "time": (str(g['game_time'])[:5] if g['game_time'] else "TBD"),
                "team": to_full(team),
                "bet_type": f"{to_full(team)} ML",
                "odds": f"+{int(ml)}" if ml > 0 else str(int(ml)),
                "decimal_odds": round(dec, 2),
                "sportsbook": book,
                "model_prob": round(prob, 3),
                "implied_prob": round(implied, 3),
                "edge": round(edge, 3),
                "edge_pct": round(edge * 100, 1),
                "recommended_amount": float(stake),
                "confidence": round(prob * 100, 1),
                "game_id": str(gid),
                "user_bankroll": round(user_bankroll, 2)
            })

    # ---------- Portfolio allocator (slate + per-game caps) ----------
    username = session.get('username', '')
    user_hist = USERS.get(username, {}).get('bet_history', [])

    open_risk_total = sum(float(b.get('amount', 0.0))
                          for b in user_hist if (b.get('result', 'Pending') == 'Pending'))

    from collections import defaultdict
    open_risk_by_game = defaultdict(float)
    for b in user_hist:
        if b.get('result', 'Pending') == 'Pending':
            gid = str(b.get('game_id'))
            if gid is not None:
                open_risk_by_game[gid] += float(b.get('amount', 0.0))

    slate_budget_total = max(0.0, user_bankroll * SLATE_BUDGET_PCT - open_risk_total)

    gross_recommended = sum(o['recommended_amount'] for o in opps)
    scale_slate = (slate_budget_total / gross_recommended) if gross_recommended > 0 and slate_budget_total < gross_recommended else 1.0
    if scale_slate < 1.0:
        for o in opps:
            o['recommended_amount'] *= scale_slate

    # per-game caps
    for gid in set(o['game_id'] for o in opps if o.get('game_id') is not None):
        group = [o for o in opps if o['game_id'] == gid]
        group_cap = max(0.0, user_bankroll * PER_GAME_CAP_PCT - open_risk_by_game.get(gid, 0.0))
        group_sum = sum(o['recommended_amount'] for o in group)
        if group_sum > group_cap and group_sum > 0:
            s = group_cap / group_sum
            for o in group:
                o['recommended_amount'] *= s

    # floor/round & drop dust
    final_total = 0.0
    kept = []
    for o in opps:
        amt = round(max(0.0, o['recommended_amount']), 2)
        if amt >= MIN_BET and final_total + amt <= slate_budget_total + 1e-9:
            o['recommended_amount'] = amt
            kept.append(o)
            final_total += amt
    opps = kept

    # order by edge desc
    opps.sort(key=lambda x: x['edge_pct'], reverse=True)

    summary = {
        "user_bankroll": round(user_bankroll, 2),
        "max_bet_cap": round(user_bankroll * MAX_BET_PCT, 2),
        "slate_budget": round(slate_budget_total, 2),
        "total_recommended": round(final_total, 2),
    }

    return jsonify({
        "opportunities": opps,
        "total_found": len(opps),
        "week": week,
        "edge_filter": edge_filter,
        "user_bankroll": round(user_bankroll, 2),  # <- keep comma here
        **summary
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
    conn = get_db()
    today = datetime.utcnow().date()
    end = today + timedelta(days=60)
    try:
        games = pd.read_sql_query(
            """
            SELECT game_id, away_team AS away, home_team AS home, game_date, start_time_local AS game_time
            FROM games
            WHERE date(game_date) BETWEEN date(?) AND date(?)
            ORDER BY date(game_date), time(start_time_local)
            """,
            conn, params=[today, end]
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
            odds['ao'] = odds['odds'].apply(normalize_american_odds)
        else:
            odds = pd.DataFrame(columns=['game_id','team','sportsbook','odds','timestamp','ao'])
        out = []
        for _, g in games.iterrows():
            o = odds[odds['game_id']==g['game_id']]
            teams = []
            for tm in (g['home'], g['away']):
                ot = o[o['team']==tm]
                if ot.empty:
                    teams.append({"team": to_full(tm), "odds": 100, "sportsbook":"No Line", "by_book": []})
                else:
                    # best line
                    # normalize every quote to American and pick best
                    ot = ot.copy()
                    ot['ao'] = ot['odds'].apply(normalize_american_odds)
                    idx = ot['ao'].astype(int).idxmax()
                    best_odds = int(ot.loc[idx,'ao'])
                    best_book = str(ot.loc[idx,'sportsbook'])
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

# Health
@app.route('/api/health')
def api_health():
    try:
        with _engine.connect() as conn:
            has_tss = pd.read_sql(text("SELECT name FROM sqlite_master WHERE type='table' AND name='team_season_summary'"), conn)
            has_games = pd.read_sql(text("SELECT name FROM sqlite_master WHERE type='table' AND name='games'"), conn)
            has_odds  = pd.read_sql(text("SELECT name FROM sqlite_master WHERE type='table' AND name='odds'"), conn)
        return jsonify({
            'db_path': DB_PATH,
            'team_season_summary': not has_tss.empty,
            'games': not has_games.empty,
            'odds': not has_odds.empty
        })
    except Exception as e:
        return jsonify({'error': str(e), 'db_path': DB_PATH}), 500

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
