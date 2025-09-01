# ai_chat_stub.py — fixed version

import os, sqlite3, math, datetime as dt, re, pickle
from typing import Dict, Any, Optional
import pandas as pd
from flask import Blueprint, request, jsonify
from sqlalchemy import create_engine, text

# --- add near the other imports
import os, sys, re, math, datetime as dt
from flask import Blueprint, request, jsonify

# Ensure we can import "model.*" when running from /dashboard
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

try:
    from model.ai_tools import list_value_bets
except Exception as e:
    print("ai_tools import error:", e)
    list_value_bets = None



def _american_from_decimal(d: float) -> Optional[int]:
    try:
        d = float(d)
    except Exception:
        return None
    if d <= 1.01 or d > 50:  # junk or out-of-range
        return None
    if d >= 2.0:
        return int(round((d - 1.0) * 100.0))
    # 1.01 – 1.99  => negative American
    return int(round(-100.0 / (d - 1.0)))



def _normalize_american(odds_val) -> Optional[int]:
    """Accepts +120 / -110 / '1.91' / 1.91 / 'EVEN' etc. Returns clean American int or None."""
    if odds_val is None:
        return None
    s = str(odds_val).strip().upper()
    if s in ("EVEN", "EV", "+100", "100"):
        return 100
    # explicit +/-
    if s.startswith('+') or s.startswith('-'):
        try:
            return int(float(s))
        except Exception:
            return None
    # numeric path
    try:
        x = float(s)
    except Exception:
        return None
    # very small / 0 / 1 are junk from some books → ignore
    if x in (0.0, 1.0) or abs(x) < 1.01:
        return None
    # If it looks like decimal odds (1.2–20ish) convert to American
    if 1.01 <= x <= 50.0:
        return _american_from_decimal(x)
    # else assume already American
    if abs(x) < 100:  # 70, 80 happen from dirty feeds → not valid
        return None
    return int(round(x))

def _safe_prob(x) -> float:
    try:
        x = float(x)
    except Exception:
        return 0.5
    if not math.isfinite(x):
        return 0.5
    return max(0.0, min(1.0, x))

def _implied_prob(any_odds):
    am = _normalize_american(any_odds)
    if am is None:
        return float("nan")
    return 100.0 / (am + 100.0) if am > 0 else -am / (-am + 100.0)

# ---- Paths (same env var your dashboard uses) ----
DB_PATH = os.environ.get("BETTR_DB_PATH", r"E:/Bettr Bot/betting-bot/data/betting.db")
MODEL_PKL = os.environ.get("BETTR_MODEL_PKL", r"E:/Bettr Bot/betting-bot/models/betting_model.pkl")

def _load_model_pack(path: str) -> Optional[dict]:
    try:
        with open(path, "rb") as f:
            return pickle.load(f)  # expects {'model','scaler','feature_cols'}
    except Exception:
        return None

# ----- features used by the trainer (home-perspective) -----
def _build_features_for_games(conn: sqlite3.Connection, games_df: pd.DataFrame) -> pd.DataFrame:
    if games_df is None or games_df.empty:
        return pd.DataFrame()

    try:
        ts = pd.read_sql_query("""
            SELECT season, team,
                   power_score AS power,
                   win_pct,
                   avg_points_for  AS off,
                   avg_points_against AS def
            FROM team_season_summary
        """, conn)
    except Exception:
        return pd.DataFrame()

    tmp = games_df.rename(columns={'home':'home_team','away':'away_team'}).copy()
    tmp["season"] = pd.to_datetime(tmp["game_date"]).dt.year

    home = ts.rename(columns={
        "team":"home_team","power":"home_power",
        "off":"home_offense","def":"home_defense",
        "win_pct":"home_win_pct"
    })
    tmp = tmp.merge(home, on=["season","home_team"], how="left")

    away = ts.rename(columns={
        "team":"away_team","power":"away_power",
        "off":"away_offense","def":"away_defense",
        "win_pct":"away_win_pct"
    })
    tmp = tmp.merge(away, on=["season","away_team"], how="left")

    # engineered columns (match trainer)
    tmp["power_diff"]   = tmp["home_power"].fillna(0)   - tmp["away_power"].fillna(0)
    tmp["win_pct_diff"] = tmp["home_win_pct"].fillna(0.5) - tmp["away_win_pct"].fillna(0.5)
    tmp["offense_diff"] = tmp["home_offense"].fillna(0) - tmp["away_offense"].fillna(0)
    tmp["defense_diff"] = tmp["home_defense"].fillna(0) - tmp["away_defense"].fillna(0)
    tmp["form_diff"]    = 0.0
    tmp["home_field_advantage"] = 3.0

    dtt = pd.to_datetime(tmp["game_date"])
    tmp["month"] = dtt.dt.month
    tmp["day_of_week"] = dtt.dt.weekday

    # placeholders (keep schema stable)
    tmp["home_injury_impact"] = 0.0
    tmp["away_injury_impact"] = 0.0
    tmp["home_qb_injury"] = 0.0
    tmp["away_qb_injury"] = 0.0
    tmp["home_recent_form"] = 0.0
    tmp["away_recent_form"] = 0.0
    tmp["h2h_games"] = 0.0
    tmp["home_h2h_win_rate"] = 0.5
    return tmp

class BettingAI:
    def __init__(self, db_path: str = None, model_path: Optional[str] = None):
        self.db_path = db_path or os.environ.get("BETTR_DB_PATH", r"E:/Bettr Bot/betting-bot/data/betting.db")
        default_pkl = os.path.join(os.path.dirname(__file__), "betting_model.pkl")
        model_pkl = model_path or os.environ.get("BETTR_MODEL_PKL", default_pkl)
        self.model_pack = _load_model_pack(model_pkl)
        self.context = {
            'user_bankroll': 500.0,
            'risk_tolerance': 'medium',
            'betting_style': 'balanced',
            'last_game_id': None,
            'last_team': None,
        }

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        c.row_factory = sqlite3.Row
        return c

    def get_game(self, game_id: str, conn: Optional[sqlite3.Connection] = None) -> Optional[dict]:
        close = False
        if conn is None:
            conn, close = self._conn(), True
        try:
            r = conn.execute("""
                SELECT game_id,
                       away_team AS away,
                       home_team AS home,
                       DATE(game_date) AS game_date,
                       TIME(start_time_local) AS game_time
                FROM games
                WHERE game_id=?
            """, (game_id,)).fetchone()
            if not r:
                return None
            return {
                'game_id': r['game_id'],
                'away': r['away'],
                'home': r['home'],
                'date': r['game_date'],
                'time': r['game_time'],
            }
        except Exception as e:
            print(f"Error getting game {game_id}: {e}")
            return None
        finally:
            if close:
                conn.close()

    def _predict_home_prob_model(self, conn: sqlite3.Connection, g: dict) -> float:
        """Predict home team win probability using model or fallback"""
        if not self.model_pack:
            return self._prob_power_injury(conn, g['away'], g['home'])
        
        try:
            # Build features for this single game
            df = pd.DataFrame([{
                'game_id': g.get('game_id', 'temp'),
                'home': g['home'], 
                'away': g['away'], 
                'game_date': g['date']
            }])
            
            feats = _build_features_for_games(conn, df)
            if feats.empty:
                return self._prob_power_injury(conn, g['away'], g['home'])
            
            # Fill any remaining NaN values
            feats = feats.fillna(0.0)
            
            # Use model to predict
            X = feats
            cols = self.model_pack.get('feature_cols')
            if cols:
                X = X.reindex(columns=cols, fill_value=0.0)
            
            scaler = self.model_pack.get('scaler')
            if scaler is not None:
                X = scaler.transform(X)
            
            model = self.model_pack['model']
            ph = float(model.predict_proba(X)[:, 1][0])  # P(home wins)
            return _safe_prob(ph)
            
        except Exception as e:
            print(f"Model prediction error: {e}")
            return self._prob_power_injury(conn, g['away'], g['home'])

    def _calibrated_prob(self, raw_prob: float) -> float:
        """
        Calibrate model predictions to be less extreme
        Push probabilities closer to 50% to account for market efficiency
        """
        # Apply temperature scaling - makes predictions less confident
        temp_scaled = raw_prob
        
        # Regression toward the mean (50%)
        calibrated = 0.5 + (temp_scaled - 0.5) * 0.75  # Reduce confidence by 25%
        
        return max(0.1, min(0.9, calibrated))

    def _prob_power_injury(self, conn: sqlite3.Connection, away: str, home: str) -> float:
        """Fallback probability calculation using power scores"""
        try:
            season = dt.date.today().year if dt.date.today().month >= 8 else dt.date.today().year - 1
            
            # Get power scores
            df = pd.read_sql_query("""
                SELECT team, power_score
                FROM team_season_summary
                WHERE season=?
            """, conn, params=[season])
            
            power_map = df.set_index('team')['power_score'].to_dict()
            
            # Get injury impact (simplified)
            try:
                inj_df = pd.read_sql_query("""
                    SELECT team, SUM(
                        CASE 
                            WHEN designation IN ('IR', 'OUT') THEN 2.0
                            WHEN designation = 'DOUBTFUL' THEN 1.0 
                            WHEN designation = 'QUESTIONABLE' THEN 0.5
                            ELSE 0.0
                        END *
                        CASE 
                            WHEN position = 'QB' THEN 3.0
                            WHEN position IN ('WR', 'RB') THEN 1.5
                            ELSE 1.0
                        END
                    ) as injury_impact
                    FROM ai_injury_validation_detail 
                    WHERE COALESCE(inj_missing_team, 0) = 0 
                      AND COALESCE(team_mismatch, 0) = 0
                    GROUP BY team
                """, conn)
                injury_map = inj_df.set_index('team')['injury_impact'].to_dict()
            except:
                injury_map = {}
            
            # Calculate adjusted power
            home_power = power_map.get(home, 0.0) + 2.5 - injury_map.get(home, 0.0) * 0.5  # HFA + injury adjustment
            away_power = power_map.get(away, 0.0) - injury_map.get(away, 0.0) * 0.5
            
            # Convert to probability
            raw_prob = 1.0 / (1.0 + math.exp(-(home_power - away_power) / 8.0))
            return _safe_prob(raw_prob)
            
        except Exception as e:
            print(f"Power calculation error: {e}")
            return 0.5  # neutral if all else fails

    def _best_lines_for_game(self, conn: sqlite3.Connection, game_id: str) -> dict:
        """Get best available lines for each team in a game"""
        try:
            odds = pd.read_sql_query("""
                SELECT o.game_id, o.team, o.sportsbook, o.odds, o.timestamp
                FROM odds o
                JOIN (
                    SELECT game_id, team, sportsbook, MAX(timestamp) AS ts
                    FROM odds
                    WHERE market='h2h' AND game_id=?
                    GROUP BY game_id, team, sportsbook
                ) x ON x.game_id=o.game_id AND x.team=o.team AND x.sportsbook=o.sportsbook AND x.ts=o.timestamp
            """, conn, params=[game_id])

            if odds.empty:
                return {}

            # Normalize odds to American
            odds["american"] = odds["odds"].apply(_normalize_american)
            odds = odds[pd.notnull(odds["american"])]

            # Find best line for each team (highest American odds = best for bettor)
            best_lines = {}
            for team, grp in odds.groupby("team"):
                grp = grp.copy()
                # For American odds: higher positive is better, less negative is better
                grp["score"] = grp["american"].apply(lambda x: x if x > 0 else -abs(x))
                if not grp.empty:
                    best_row = grp.loc[grp["score"].idxmax()]
                    
                    best_lines[team] = {
                        "odds": int(best_row["american"]), 
                        "sportsbook": str(best_row["sportsbook"])
                    }
            
            return best_lines
            
        except Exception as e:
            print(f"Best lines error: {e}")
            return {}

    def _get_injury_summary(self, conn, home_team, away_team):
        """Get injury summary for both teams"""
        try:
            # Query injury data if available
            query = """
                SELECT team_ai as team, position, designation, 
                       COUNT(*) as injury_count
                FROM ai_injury_validation_detail 
                WHERE (team_ai = ? OR team_ai = ?) 
                  AND COALESCE(inj_missing_team, 0) = 0
                  AND COALESCE(team_mismatch, 0) = 0
                GROUP BY team_ai, position, designation
            """
            injuries = conn.execute(query, (home_team, away_team)).fetchall()
            
            home_qb = sum(1 for inj in injuries if inj['team'] == home_team and inj['position'] == 'QB')
            away_qb = sum(1 for inj in injuries if inj['team'] == away_team and inj['position'] == 'QB')
            
            return {
                "home": {"qb": home_qb},
                "away": {"qb": away_qb}
            }
        except Exception as e:
            print(f"Injury summary error: {e}")
            return {"home": {"qb": 0}, "away": {"qb": 0}}

    def _enrich_for_ui(self, bets: list[dict]) -> list[dict]:
        out = []
        for b in bets:
            game = b.get('game') or f"{b.get('away_team','?')} @ {b.get('home_team','?')}"
            date = str(b.get('date') or '')
            time = str(b.get('time') or b.get('t') or '')
            team = b.get('team') or ''
            ml = b.get('odds')
            try:
                ml_int = int(ml) if ml is not None else None
            except Exception:
                ml_int = None
            odds_str = (f"+{ml_int}" if (ml_int is not None and ml_int > 0) else (str(ml_int) if ml_int is not None else ''))

            # add multiple compatible fields so whatever the UI looks for will exist
            b.setdefault('game', game)
            b.setdefault('title', game)                                   # many UIs use title
            b.setdefault('subtitle', f"{date} {time}".strip())            # optional subtitle
            b.setdefault('summary', f"{game} • {date} {time}".strip())    # some UIs use summary
            b.setdefault('bet', f"{team} ML")
            b.setdefault('line', f"{odds_str}{(' @ ' + (b.get('sportsbook') or '—')) if odds_str else ''}")
            b.setdefault('t', time)                                       # some UIs use 't' for time
            b.setdefault('odds_str', odds_str)
            out.append(b)
        return out


    def _analyze_game_payload(self, game_id: str) -> dict:
        """Fixed version with better error handling"""
        conn = self._conn()
        try:
            g = self.get_game(game_id, conn)
            if not g:
                return {"message": "Game not found", "error": True}

            # Get model prediction
            ph = self._predict_home_prob_model(conn, g)
            pa = 1.0 - ph

            # Get best lines
            lines = self._best_lines_for_game(conn, game_id)
            
            home_odds = (lines.get(g["home"], {}) or {}).get("odds")
            away_odds = (lines.get(g["away"], {}) or {}).get("odds")

            # Calculate EV for both sides
            def calculate_ev(prob, ml_odds):
                if ml_odds is None:
                    return -1e9
                try:
                    ml_odds = float(ml_odds)
                    if ml_odds >= 0:
                        return prob * (ml_odds / 100.0) - (1.0 - prob)
                    else:
                        return prob * (100.0 / abs(ml_odds)) - (1.0 - prob)
                except:
                    return -1e9

            ev_home = calculate_ev(ph, home_odds)
            ev_away = calculate_ev(pa, away_odds)

            # Determine pick
            if ev_home >= ev_away and home_odds is not None:
                pick_team = g["home"]
                pick_odds = home_odds
                pick_prob = ph
                pick_book = (lines.get(g["home"], {}) or {}).get("sportsbook", "best")
            elif away_odds is not None:
                pick_team = g["away"] 
                pick_odds = away_odds
                pick_prob = pa
                pick_book = (lines.get(g["away"], {}) or {}).get("sportsbook", "best")
            else:
                return {
                    "game_id": g["game_id"],
                    "game": f"{g['away']} @ {g['home']} ({g['date']})",
                    "probabilities": {"home": ph, "away": pa},
                    "best_bet": None,
                    "odds": {"home": home_odds, "away": away_odds},
                    "meta": {"using_model": bool(self.model_pack)},
                    "message": "No odds available"
                }

            # Calculate edge
            ih = _implied_prob(home_odds) if home_odds is not None else None
            ia = _implied_prob(away_odds) if away_odds is not None else None
            if ih is not None and ia is not None:
                tot = max(ih + ia, 1e-9)
                ih_n, ia_n = ih / tot, ia / tot
            else:
                ih_n = ih if ih is not None else 0.5
                ia_n = ia if ia is not None else 0.5

            pick_implied = ih_n if pick_team == g["home"] else ia_n
            edge_pct = (pick_prob - pick_implied) * 100.0

            
            # Add injury information
            injury_info = self._get_injury_summary(conn, g["home"], g["away"])

            return {
                "game_id": g["game_id"],
                "game": f"{g['away']} @ {g['home']} ({g['date']})",
                "probabilities": {"home": round(ph, 3), "away": round(pa, 3)},
                "teams": {"home": g["home"], "away": g["away"]},
                "odds": {
                    "home": {"odds": home_odds, "sportsbook": (lines.get(g["home"], {}) or {}).get("sportsbook")},
                    "away": {"odds": away_odds, "sportsbook": (lines.get(g["away"], {}) or {}).get("sportsbook")},
                },
                "best_bet": {
                    "team": pick_team,
                    "odds": int(pick_odds) if pick_odds is not None else None,
                    "sportsbook": pick_book,
                    "edge": round(edge_pct, 1) if math.isfinite(edge_pct) else None,
                    "confidence": round(pick_prob * 100.0, 1),
                },
                "injuries": injury_info,
                "summary": f"Model favors {pick_team} with {round(pick_prob*100,1)}% confidence",
                "meta": {"using_model": bool(self.model_pack)},
            }
        except Exception as e:
            print(f"Error in _analyze_game_payload: {e}")
            return {"message": f"Analysis error: {str(e)}", "error": True}
        finally:
            conn.close()

    def process_natural_language(self, message: str, game_id: Optional[str] = None, team: Optional[str] = None):
        """Fixed NL processing with better error handling"""
        m = (message or '').strip().lower()

        try:
            # inside your NL intent handler
            if "value" in m and ("bet" in m or "edge" in m):
                bets = self._scan_value_bets(days=21, min_edge=0.05)
                bets = self._enrich_for_ui(bets)
                return (bets, 'value_bets')


            if 'analy' in m or 'analyze this game' in m or ('analyze' in m and game_id):
                if not game_id:
                    return ({'message': 'Pick a game first (left panel), then hit "Analyze this game".'}, 'info')
                payload = self._analyze_game_payload(game_id)
                return (payload, 'analysis')

            if any(k in m for k in ['explain', 'why', 'reason']):
                gid = game_id or self.context.get('last_game_id')
                if not gid:
                    return ({'message': 'Pick a game first.'}, 'info')

                analysis = self._analyze_game_payload(gid)
                if analysis.get('error'):
                    return (analysis, 'info')

                bb = analysis.get('best_bet')
                if not bb:
                    return ({'message': 'No pick available for this game.'}, 'info')

                home = analysis['teams']['home']
                away = analysis['teams']['away']
                prob = analysis['probabilities']['home'] if bb['team'] == home else analysis['probabilities']['away']

                return ({
                    'team': bb['team'],
                    'pick': bb['team'],
                    'odds': bb['odds'],
                    'factors': ['Power rating edge vs implied odds', 'Injury impact considered'],
                    'confidence': bb['confidence'],
                    'model_probability': prob
                }, 'explain_pick')



            if m.startswith('bankroll'):
                amt = re.search(r'(\d+(\.\d+)?)', m)
                if amt:
                    self.context['user_bankroll'] = float(amt.group(1))
                    return ({'message': f"Bankroll updated to ${self.context['user_bankroll']:.2f}"}, 'settings')

            return ({'message': "Try: 'Analyze this game', 'Find value bets with 5% edge', or 'Explain the pick'."}, 'info')
            
        except Exception as e:
            print(f"Error in process_natural_language: {e}")
            return ({'message': f'Analysis error: {str(e)}'}, 'error')

    def _is_genuine_edge(self, prob: float, implied_prob: float, min_edge: float = 0.08) -> bool:
        """
        More conservative edge detection
        """
        edge = prob - implied_prob
        
        # Require higher edge for extreme probabilities
        if prob > 0.7 or prob < 0.3:
            min_edge *= 1.5  # 50% higher threshold for extreme picks
        
        # Additional filters
        if abs(prob - 0.5) < 0.05:  # Essentially a coin flip
            return False
            
        return edge >= min_edge



    def _scan_value_bets(self, days: int = 21, min_edge: float = 0.05):
        """Scan for value betting opportunities (aligned with dashboard)."""
        today = dt.date.today()
        conn = self._conn()
        opps = []
        try:
            games = pd.read_sql_query("""
                SELECT game_id, away_team AS away, home_team AS home,
                    DATE(game_date) AS game_date,
                    TIME(start_time_local) AS game_time
                FROM games
                WHERE DATE(game_date) BETWEEN DATE(?) AND DATE(?)
                ORDER BY game_date, start_time_local
            """, conn, params=[today, today + dt.timedelta(days=days)])
            if games.empty:
                return []

            for _, game in games.iterrows():
                g = {'game_id': game['game_id'], 'home': game['home'], 'away': game['away'], 'date': game['game_date']}
                ph = self._predict_home_prob_model(conn, g)  # P(home)
                pa = 1.0 - ph

                # calibration shrink toward 50%
                ph = 0.5 + (ph - 0.5) * 0.7
                pa = 1.0 - ph

                # latest lines per (team,book)
                odds = pd.read_sql_query("""
                    SELECT o.team, o.sportsbook, o.odds
                    FROM odds o
                    JOIN (
                        SELECT game_id, team, sportsbook, MAX(timestamp) AS ts
                        FROM odds
                        WHERE market='h2h' AND game_id=?
                        GROUP BY game_id, team, sportsbook
                    ) x ON x.game_id=o.game_id AND x.team=o.team AND x.sportsbook=o.sportsbook AND x.ts=o.timestamp
                """, conn, params=[game['game_id']])

                if odds.empty:
                    continue

                # best line for each team + unique book counts
                lines = {}
                for team, grp in odds.groupby("team"):
                    grp = grp.copy()
                    grp["american"] = grp["odds"].apply(_normalize_american)
                    grp = grp[pd.notnull(grp["american"])]
                    if grp.empty: 
                        continue
                    # higher positive is better; less negative is better → max by "score"
                    grp["score"] = grp["american"].apply(lambda x: x if x > 0 else -abs(x))
                    best = grp.loc[grp["score"].idxmax()]
                    lines[team] = {
                        "odds": int(best["american"]),
                        "sportsbook": str(best["sportsbook"]),
                        "books": grp["sportsbook"].nunique()
                    }

                if game["home"] not in lines or game["away"] not in lines:
                    continue

                home_ml = lines[game["home"]]["odds"]
                away_ml = lines[game["away"]]["odds"]

                # normalize implied (remove juice)
                ih = _implied_prob(home_ml)
                ia = _implied_prob(away_ml)
                tot = max(ih + ia, 1e-9)
                ih_n, ia_n = ih / tot, ia / tot

                for team, prob in [(game['home'], ph), (game['away'], pa)]:
                    # clamp model probs to avoid goofy 99% vs +400 artifacts
                    prob = max(0.20, min(0.80, prob))

                    # require ≥2 independent books
                    if lines[team]["books"] < 2:
                        continue

                    ml_odds = lines[team]["odds"]
                    sportsbook = lines[team]["sportsbook"]
                    implied = ih_n if team == game["home"] else ia_n

                    # guard-rails (match dashboard spirit)
                    if ml_odds >= 250 and prob < 0.38:
                        continue
                    if abs(prob - 0.50) < 0.03 and abs(ml_odds) < 140:
                        continue

                    # optional: market-efficiency discount
                    edge = (prob - implied) * 0.93

                    if edge >= float(min_edge):
                        dec = 1 + (ml_odds / 100.0) if ml_odds > 0 else 1 + (100.0 / abs(ml_odds))
                        kelly = ((prob * (dec - 1)) - (1 - prob)) / (dec - 1) if dec > 1 else 0.0
                        stake = max(1.0, min(50.0, max(0.0, kelly) * 500.0 * 0.25))

                        opps.append({
                            "game_id": game["game_id"],
                            "game": f"{game['away']} @ {game['home']}",
                            "date": game["game_date"],
                            "time": (game["game_time"] or "")[:5],
                            "team": team,
                            "odds": int(ml_odds),
                            "sportsbook": sportsbook,
                            "model_prob": round(prob, 3),
                            "implied_prob": round(implied, 3),
                            "edge": round(edge, 3),
                            "edge_pct": round(edge * 100.0, 1),
                            "recommended_amount": round(stake, 2),
                        })

            opps.sort(key=lambda x: x["edge"], reverse=True)
            return opps
        except Exception as e:
            print(f"Value bets scan error: {e}")
            return []
        finally:
            conn.close()

# Flask routes with better error handling
ai_bp = Blueprint("ai", __name__)
betting_ai = BettingAI()

@ai_bp.post("/ai-chat")
def ai_chat():
    try:
        data = request.get_json() or {}
        message = (data.get('message') or '').strip()
        game_id = data.get('game_id')
        team = data.get('team')

        print(f"[AI] /api/ai-chat message='{message[:120]}' game_id={game_id}")

        if not message:
            return jsonify({'ok': False, 'error': 'No message provided'})

        payload, intent = betting_ai.process_natural_language(message, game_id, team)
        return jsonify({'ok': True, 'result': payload, 'intent': intent})

    except Exception as e:
        print(f"[AI] AI chat error: {e}")
        return jsonify({'ok': False, 'error': f'Server error: {str(e)}'}), 500


@ai_bp.get("/ai-insights/<game_id>")
def ai_insights(game_id):
    try:
        if not game_id:
            return jsonify({'ok': False, 'error': 'Game ID required'})
            
        analysis = betting_ai._analyze_game_payload(game_id)
        if analysis.get('error'):
            return jsonify({'ok': False, 'error': analysis.get('message', 'Analysis failed')})
            
        return jsonify({'ok': True, 'analysis': analysis})
        
    except Exception as e:
        print(f"AI insights error: {e}")
        return jsonify({'ok': False, 'error': f'Server error: {str(e)}'}), 500