# dashboard/ai_chat_stub.py
"""
Comprehensive AI Chat System for Bettr Bot
Full-featured betting analysis with advanced AI integration
Designed for local/Windows and cloud deployments (SQLite or SQLAlchemy).
"""

import os
import sys
import sqlite3
import math
import datetime as dt
import re
from math import sqrt, erf
import pickle
import json
import time
import logging
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass
from enum import Enum

import pandas as pd
import numpy as np
from flask import Blueprint, request, jsonify, session

# ---------------------------
# Optional deps (safe import)
# ---------------------------
try:
    import requests  # noqa
except Exception:
    pass

try:
    from sqlalchemy import create_engine, text
    SQLALCHEMY_AVAILABLE = True
except Exception:
    SQLALCHEMY_AVAILABLE = False
    text = None  # type: ignore

try:
    from openai import OpenAI
    import openai as _openai  # noqa
    OPENAI_AVAILABLE = True
except Exception:
    OPENAI_AVAILABLE = False
    OpenAI = None  # type: ignore

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------
# Local project imports
# ---------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

try:
    from model.ai_tools import list_value_bets  # noqa
except Exception as e:
    logger.warning(f"ai_tools import error (ignored): {e}")
    list_value_bets = None  # noqa: F401

# ---------------------------
# Data classes & enums
# ---------------------------
class MessageIntent(Enum):
    GAME_ANALYSIS = "game_analysis"
    VALUE_BETS = "value_bets"
    INJURY_REPORT = "injury_report"
    TEAM_ANALYSIS = "team_analysis"
    PLAYER_ANALYSIS = "player_analysis"
    BETTING_STRATEGY = "betting_strategy"
    MARKET_ANALYSIS = "market_analysis"
    BANKROLL_MANAGEMENT = "bankroll_management"
    GENERAL_CHAT = "general_chat"
    SYSTEM_STATUS = "system_status"


@dataclass
class GameAnalysis:
    game_id: str
    home_team: str
    away_team: str
    game_date: str
    home_probability: float
    away_probability: float
    best_bet: Optional[Dict]
    injury_impact: Dict
    weather_impact: Optional[Dict]
    key_factors: List[str]
    confidence_score: float
    recommendation: str


@dataclass
class ValueBet:
    game_id: str
    team: str
    odds: int
    sportsbook: str
    model_probability: float
    implied_probability: float
    edge_percentage: float
    recommended_stake: float
    confidence_level: str
    risk_assessment: str

# ---------------------------
# DB utils
# ---------------------------
class DatabaseManager:
    """Handles database connections for cloud and local environments."""

    def __init__(self):
        self.connection_string = self._get_database_url()
        self.engine = None

    def _get_database_url(self) -> str:
        # Prefer URL envs
        for url in (os.getenv("DATABASE_URL"),
                    os.getenv("POSTGRES_URL"),
                    os.getenv("MYSQL_URL")):
            if url:
                return url

        # Shared sqlite path (Windows-friendly)
        default_db = r"E:/Bettr Bot/betting-bot/data/betting.db"
        path = os.getenv("BETTR_DB_PATH", default_db).replace("\\", "/")
        if "://" in path:
            return path
        return f"sqlite:///{path}"

    def get_connection(self):
        """Return a connection that pandas can read from."""
        try:
            if SQLALCHEMY_AVAILABLE:
                if self.engine is None:
                    self.engine = create_engine(
                        self.connection_string,
                        pool_pre_ping=True,
                        pool_recycle=300,
                    )
                return self.engine.connect()
            # Fallback: raw sqlite3
            db_path = self.connection_string.replace("sqlite:///", "")
            return sqlite3.connect(db_path)
        except Exception as e:
            logger.error(f"Database connection failed; falling back to same sqlite path. Error: {e}")
            db_path = self.connection_string.replace("sqlite:///", "")
            return sqlite3.connect(db_path)


def query_df(conn, sql: str, params: Optional[Dict] = None) -> pd.DataFrame:
    """
    Pandas/SQLAlchemy/sqlite3 compatibility helper.
    Named binds (:name) work if `conn` is SQLAlchemy; otherwise sqlite3 requires `?`.
    For sqlite3 connections, we do a simple replacement for named params.
    """
    try:
        # SQLAlchemy Connection?
        if hasattr(conn, "exec_driver_sql") or "sqlalchemy" in str(type(conn)).lower():
            if text is None:
                # Shouldn't happen when using SQLAlchemy, but guard anyway
                return pd.read_sql_query(sql, conn, params=params)
            return pd.read_sql_query(text(sql), conn, params=params)
        else:
            # Raw sqlite3: convert :name to ? and order positional args
            if params:
                # Simple & safe replacement (only for :name patterns)
                names = re.findall(r":([A-Za-z_][A-Za-z0-9_]*)", sql)
                qmarks = []
                for nm in names:
                    if nm not in params:
                        raise KeyError(f"Missing SQL param: {nm}")
                    qmarks.append(params[nm])
                sql_q = re.sub(r":[A-Za-z_][A-Za-z0-9_]*", "?", sql)
                return pd.read_sql_query(sql_q, conn, params=qmarks)
            return pd.read_sql_query(sql, conn)
    except Exception:
        logger.exception("query_df failed")
        raise

# ---------------------------
# Core analyzer
# ---------------------------
class AdvancedBettingAnalyzer:
    """Advanced betting analysis engine with ML models and statistical analysis."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.model_pack = self._load_model()
        self.cache: Dict[str, Tuple[Any, float]] = {}
        self.cache_ttl = 300  # seconds
        # cache feature vectors for narrative "drivers"
        self.feature_cache: Dict[str, Dict[str, float]] = {}

    # ---------- Cache helpers ----------
    def _get_cached(self, key: str):
        item = self.cache.get(key)
        if not item:
            return None
        data, ts = item
        if time.time() - ts < self.cache_ttl:
            return data
        return None

    def method_breakdown(self, game_id: str) -> List[Tuple[str, float, Optional[float]]]:
        """Public accessor to recompute method details for a given game id."""
        conn = self.db_manager.get_connection()
        try:
            gdf = query_df(conn, "SELECT game_id, home_team, away_team, game_date FROM games WHERE game_id = :gid", {"gid": game_id})
            if gdf.empty:
                return []
            game = gdf.iloc[0]
            d = self._calculate_win_probabilities(conn, game)
            return d.get("method_details", [])
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _cache_result(self, key: str, data):
        self.cache[key] = (data, time.time())

    # ---------- Model ----------
    def _load_model(self) -> Optional[Dict]:
        paths = [
            os.getenv("BETTR_MODEL_PKL"),
            r"E:/Bettr Bot/betting-bot/models/betting_model.pkl",
            "/app/models/betting_model.pkl",
            "./models/betting_model.pkl",
        ]
        for p in paths:
            if not p:
                continue
            if os.path.exists(p):
                try:
                    with open(p, "rb") as f:
                        obj = pickle.load(f)

                    # Support both dict packs and bare estimators
                    if isinstance(obj, dict) and "model" in obj:
                        model_data = obj
                    else:
                        # bare estimator – synthesize a pack
                        feature_cols = []
                        if hasattr(obj, "feature_names_in_"):
                            feature_cols = list(obj.feature_names_in_)  # scikit saves this on some pipelines/estimators
                        model_data = {
                            "model": obj,
                            "scaler": None,          # unknown
                            "feature_cols": feature_cols
                        }

                    logger.info(f"Model loaded from {p}")
                    return model_data
                except Exception as e:
                    logger.warning(f"Failed to load model from {p}: {e}")
        logger.warning("No model found, using statistical fallback.")
        return None

    # ---------- Public API ----------
    def analyze_game_comprehensive(self, game_id: str) -> GameAnalysis:
        cache_key = f"game_analysis_{game_id}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        conn = self.db_manager.get_connection()
        try:
            game_sql = """
                SELECT game_id, home_team, away_team, game_date, start_time_local
                FROM games
                WHERE game_id = :game_id
            """
            gdf = query_df(conn, game_sql, {"game_id": game_id})
            if gdf.empty:
                raise ValueError(f"Game {game_id} not found.")

            game = gdf.iloc[0]

            probs = self._calculate_win_probabilities(conn, game)
            injuries = self._analyze_injury_impact(conn, game["home_team"], game["away_team"])
            weather = self._get_weather_impact(game)
            key_factors = self._identify_key_factors(conn, game)
            best_bet = self._find_best_bet(conn, game_id, probs)
            confidence = self._calculate_confidence_score(probs, injuries, key_factors)
            recommendation = self._generate_recommendation(probs, best_bet, confidence)

            result = GameAnalysis(
                game_id=game_id,
                home_team=game["home_team"],
                away_team=game["away_team"],
                game_date=game["game_date"],
                home_probability=probs["home"],
                away_probability=probs["away"],
                best_bet=best_bet,
                injury_impact=injuries,
                weather_impact=weather,
                key_factors=key_factors,
                confidence_score=confidence,
                recommendation=recommendation,
            )
            self._cache_result(cache_key, result)
            return result
        except Exception:
            logger.exception("analyze_game_comprehensive failed")
            raise
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def find_value_bets_advanced(self, min_edge: float = 0.05, max_odds: int = 400) -> List[ValueBet]:
        cache_key = f"value_bets_{min_edge}_{max_odds}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        conn = self.db_manager.get_connection()
        value_bets: List[ValueBet] = []
        try:
            games_sql = """
                SELECT DISTINCT g.game_id, g.home_team, g.away_team, g.game_date
                FROM games g
                JOIN odds o ON g.game_id = o.game_id
                WHERE g.game_date BETWEEN date('now') AND date('now', '+14 days')
                  AND o.market = 'h2h'
                ORDER BY g.game_date
                LIMIT 50
            """
            games = query_df(conn, games_sql)

            odds_sql = """
                SELECT o.team, o.sportsbook, o.odds
                FROM odds o
                JOIN (
                    SELECT team, sportsbook, MAX(timestamp) AS max_ts
                    FROM odds 
                    WHERE game_id = :gid AND market = 'h2h'
                    GROUP BY team, sportsbook
                ) latest
                  ON o.team = latest.team 
                 AND o.sportsbook = latest.sportsbook 
                 AND o.timestamp = latest.max_ts
                WHERE o.game_id = :gid
            """

            for _, game in games.iterrows():
                try:
                    probs = self._calculate_win_probabilities(conn, game)
                    odds_df = query_df(conn, odds_sql, {"gid": game["game_id"]})

                    for team in [game["home_team"], game["away_team"]]:
                        tdf = odds_df.loc[odds_df["team"] == team]
                        if tdf.empty:
                            continue
                        row = tdf.loc[tdf["odds"].idxmax()]
                        odds_val = float(row["odds"])
                        if abs(odds_val) > max_odds:
                            continue

                        model_p = probs["home"] if team == game["home_team"] else probs["away"]
                        implied = 100 / (odds_val + 100) if odds_val > 0 else abs(odds_val) / (abs(odds_val) + 100)
                        edge = model_p - implied
                        if edge >= min_edge:
                            dec = (1 + (odds_val / 100)) if odds_val > 0 else (1 + (100 / abs(odds_val)))
                            kelly = (model_p * dec - 1) / (dec - 1)
                            stake = max(1, min(50, kelly * 100 * 0.25))
                            value_bets.append(ValueBet(
                                game_id=game["game_id"],
                                team=team,
                                odds=int(odds_val),
                                sportsbook=row["sportsbook"],
                                model_probability=model_p,
                                implied_probability=implied,
                                edge_percentage=edge * 100,
                                recommended_stake=float(stake),
                                confidence_level=("High" if edge > 0.08 else "Medium" if edge > 0.05 else "Low"),
                                risk_assessment=("Low" if abs(odds_val) < 150 else "Medium" if abs(odds_val) < 250 else "High"),
                            ))
                except Exception as e:
                    logger.warning(f"Error analyzing game {game['game_id']}: {e}")
                    continue

            value_bets.sort(key=lambda x: x.edge_percentage, reverse=True)
            top = value_bets[:20]
            self._cache_result(cache_key, top)
            return top
        except Exception:
            logger.exception("find_value_bets_advanced failed")
            raise
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # ---------- Extra market util ----------
    def latest_h2h_odds(self, conn, game_id: str) -> Dict[str, Dict[str, Any]]:
        """
        Returns {team: {'odds': int, 'sportsbook': str, 'implied': float}} for latest H2H per sportsbook,
        and picks the best (highest) moneyline for each team.
        """
        try:
            q = """
                SELECT o.team, o.sportsbook, o.odds
                FROM odds o
                JOIN (
                    SELECT team, sportsbook, MAX(timestamp) AS max_ts
                    FROM odds 
                    WHERE game_id = :gid AND market = 'h2h'
                    GROUP BY team, sportsbook
                ) latest
                  ON o.team = latest.team 
                 AND o.sportsbook = latest.sportsbook 
                 AND o.timestamp = latest.max_ts
                WHERE o.game_id = :gid
            """
            df = query_df(conn, q, {"gid": game_id})
            out: Dict[str, Dict[str, Any]] = {}
            if df.empty:
                return out
            for team in df["team"].unique():
                tdf = df.loc[df["team"] == team]
                row = tdf.loc[tdf["odds"].idxmax()]  # best price
                odds_val = int(row["odds"])
                implied = 100 / (odds_val + 100) if odds_val > 0 else abs(odds_val) / (abs(odds_val) + 100)
                out[str(team)] = {"odds": odds_val, "sportsbook": row["sportsbook"], "implied": float(implied)}
            return out
        except Exception:
            logger.warning("latest_h2h_odds failed", exc_info=True)
            return {}

    # ---------- Internals ----------
    def _calculate_win_probabilities(self, conn, game) -> Dict[str, float]:
        methods = []

        # 1) ML model (soft-fail to keep app running)
        used_ml = False
        if self.model_pack:
            try:
                ml_prob = self._predict_with_model(conn, game)
                methods.append(("ml_model", ml_prob, 1 - ml_prob))
                used_ml = True
            except Exception as e:
                logger.warning(f"ML model prediction failed: {e}")

        # 2) Power ratings
        methods.append(("power_ratings", self._predict_with_power_ratings(conn, game), None))

        # 3) Recent form
        methods.append(("recent_form", self._predict_with_recent_form(conn, game), None))

        # 4) Head-to-head
        methods.append(("h2h_history", self._predict_with_h2h_history(conn, game), None))

        # Weighted ensemble — if ML available, let it dominate
        weights = {"ml_model": 1.0 if used_ml else 0.0, "power_ratings": 0.15, "recent_form": 0.10, "h2h_history": 0.05}

        home_prob_weighted = 0.0
        total_weight = 0.0
        for m, home_prob, _ in methods:
            if home_prob is None:
                continue
            w = weights.get(m, 0.0)
            home_prob_weighted += home_prob * w
            total_weight += w
        final_home = (home_prob_weighted / total_weight) if total_weight > 0 else 0.5

        # Light calibration toward 50/50
        calibrated = 0.85 * final_home + 0.15 * 0.5

        return {
            "home": max(0.10, min(0.90, calibrated)),
            "away": max(0.10, min(0.90, 1 - calibrated)),
            "method_details": methods,
            "source": "ml_model" if used_ml else "heuristics",
        }

    def _predict_with_model(self, conn, game) -> float:
        # Build features
        feat = self._build_game_features(conn, game)

        model = self.model_pack.get("model")
        scaler = self.model_pack.get("scaler")
        feature_cols: List[str] = self.model_pack.get("feature_cols", [])

        # If the pack didn’t carry names, use whatever we built
        if not feature_cols:
            feature_cols = list(feat.keys())

        # Guarantee every expected feature exists
        for col in feature_cols:
            if col not in feat:
                feat[col] = 0.0

        X = pd.DataFrame([feat])[feature_cols].fillna(0.0)
        if scaler is not None and hasattr(scaler, "transform"):
            X = scaler.transform(X)

        # Some scikit pipelines expose predict_proba on .named_steps[-1] only.
        estimator = model
        if not hasattr(estimator, "predict_proba") and hasattr(estimator, "steps"):
            try:
                estimator = estimator.steps[-1][1]
            except Exception:
                pass

        if not hasattr(estimator, "predict_proba"):
            raise RuntimeError("Loaded model has no predict_proba(...)")

        prob_home = float(estimator.predict_proba(X)[0, 1])

        # store features for narrative drivers
        try:
            self.feature_cache[str(game["game_id"])] = feat
        except Exception:
            pass

        logger.info(
            "ML prob (home) %s vs %s = %.3f (features nonzero=%d/%d)",
            game["home_team"], game["away_team"],
            prob_home,
            sum(1 for k, v in feat.items() if v not in (0, 0.0, 0.5)),
            len(feat),
        )
        return prob_home

    def _predict_with_power_ratings(self, conn, game) -> float:
        try:
            season = int(pd.to_datetime(game["game_date"]).year)
            q = """
                SELECT team, power_score
                FROM team_season_summary
                WHERE season = :season AND team IN (:t1, :t2)
            """
            df = query_df(conn, q, {"season": season, "t1": game["home_team"], "t2": game["away_team"]})
            if len(df) < 2:
                return 0.5
            home_power = df.loc[df["team"] == game["home_team"], "power_score"].iloc[0]
            away_power = df.loc[df["team"] == game["away_team"], "power_score"].iloc[0]
            adjusted_home = float(home_power) + 2.5  # simple HFA
            diff = adjusted_home - float(away_power)
            return 1.0 / (1.0 + math.exp(-diff / 8.0))
        except Exception:
            return 0.5

    def _recent_form_pct(self, conn, team: str, cutoff_date: str) -> float:
        try:
            q = """
                SELECT 
                    CASE WHEN home_team = :team THEN 
                        CASE WHEN home_score > away_score THEN 1 ELSE 0 END
                    ELSE 
                        CASE WHEN away_score > home_score THEN 1 ELSE 0 END 
                    END as team_won
                FROM games 
                WHERE (home_team = :team OR away_team = :team)
                  AND home_score IS NOT NULL 
                  AND game_date < :cutoff
                ORDER BY game_date DESC 
                LIMIT 5
            """
            df = query_df(conn, q, {"team": team, "cutoff": cutoff_date})
            if df.empty:
                return 0.5
            return float(df["team_won"].mean())
        except Exception:
            return 0.5

    def _predict_with_recent_form(self, conn, game) -> float:
        try:
            home_form = self._recent_form_pct(conn, game["home_team"], game["game_date"])
            away_form = self._recent_form_pct(conn, game["away_team"], game["game_date"])
            diff = home_form - away_form
            home_prob = 0.5 + diff * 0.3
            return max(0.2, min(0.8, home_prob))
        except Exception:
            return 0.5

    def _predict_with_h2h_history(self, conn, game) -> float:
        try:
            q = """
                SELECT 
                    CASE WHEN home_team = :home AND home_score > away_score THEN 1
                         WHEN away_team = :home AND away_score > home_score THEN 1
                         ELSE 0 END as home_team_won
                FROM games 
                WHERE ((home_team = :home AND away_team = :away) OR 
                       (home_team = :away AND away_team = :home))
                  AND home_score IS NOT NULL
                  AND game_date > date('now', '-3 years')
                ORDER BY game_date DESC
                LIMIT 10
            """
            df = query_df(conn, q, {"home": game["home_team"], "away": game["away_team"]})
            if df.empty:
                return 0.5
            return max(0.2, min(0.8, float(df["home_team_won"].mean())))
        except Exception:
            return 0.5

    def _qb_injury_flag(self, conn, team: str) -> float:
        """
        Return 1.0 if recent QB injury with negative designation exists, else 0.0.
        Column names vary across your DB — we probe safely.
        """
        try:
            tables = query_df(conn, "SELECT name FROM sqlite_master WHERE type='table' AND name='ai_injury_validation_detail'")
            if tables.empty:
                return 0.0

            cols = query_df(conn, "PRAGMA table_info(ai_injury_validation_detail)")
            names = set(cols["name"].tolist())

            team_col = "team_ai" if "team_ai" in names else "team_inj" if "team_inj" in names else "team" if "team" in names else None
            pos_col = "position" if "position" in names else "pos" if "pos" in names else None
            des_col = "designation" if "designation" in names else "status" if "status" in names else None
            date_col = None
            for c in ("date", "injury_date", "report_date", "updated_at", "ts"):
                if c in names:
                    date_col = c
                    break

            if not (team_col and pos_col and des_col):
                return 0.0

            where = [f"COALESCE({team_col},'') = :team", f"COALESCE({pos_col},'') = 'QB'"]
            if "inj_missing_team" in names:
                where.append("COALESCE(inj_missing_team,0)=0")
            if "roster_missing_team" in names:
                where.append("COALESCE(roster_missing_team,0)=0")
            if "team_mismatch" in names:
                where.append("COALESCE(team_mismatch,0)=0")
            if date_col:
                where.append(f"{date_col} >= date('now','-21 days')")

            sql = f"""
                SELECT {des_col} AS desig
                FROM ai_injury_validation_detail
                WHERE {" AND ".join(where)}
                LIMIT 5
            """
            df = query_df(conn, sql, {"team": team})
            if df.empty:
                return 0.0
            bad = {"OUT", "IR", "DOUBTFUL", "QUESTIONABLE"}
            has_bad = any(str(x).upper() in bad for x in df["desig"].fillna(""))
            return 1.0 if has_bad else 0.0
        except Exception:
            return 0.0

    def _build_game_features(self, conn, game) -> Dict[str, float]:
        """Build feature vector for ML prediction. Includes all fields your model has logged in errors."""
        features: Dict[str, float] = {}
        season = int(pd.to_datetime(game["game_date"]).year)

        # defaults in case DB is sparse
        defaults = {
            'home_power': 0.0, 'away_power': 0.0, 'power_diff': 0.0,
            'home_win_pct': 0.5, 'away_win_pct': 0.5, 'win_pct_diff': 0.0,
            'home_offense': 20.0, 'away_offense': 20.0, 'offense_diff': 0.0,
            'home_defense': 20.0, 'away_defense': 20.0, 'defense_diff': 0.0,
            'home_recent_form': 0.5, 'away_recent_form': 0.5, 'form_diff': 0.0,
            'h2h_games': 0.0, 'home_h2h_win_rate': 0.5,
            'home_qb_injury': 0.0, 'away_qb_injury': 0.0,
            'month': 9.0, 'day_of_week': 0.0, 'home_field_advantage': 3.0,
            'home_injury_impact': 0.0, 'away_injury_impact': 0.0,
        }
        features.update(defaults)

        try:
            # Team season summary (power, win pct, offense/defense)
            q = """
                SELECT team, power_score, win_pct, avg_points_for, avg_points_against
                FROM team_season_summary 
                WHERE season = :season AND team IN (:t1, :t2)
            """
            ts = query_df(conn, q, {"season": season, "t1": game["home_team"], "t2": game["away_team"]})
            if len(ts) >= 2:
                hs = ts[ts["team"] == game["home_team"]].iloc[0]
                as_ = ts[ts["team"] == game["away_team"]].iloc[0]
                features['home_power'] = float(hs.get("power_score", defaults['home_power']) or 0.0)
                features['away_power'] = float(as_.get("power_score", defaults['away_power']) or 0.0)
                features['power_diff'] = features['home_power'] - features['away_power']

                features['home_win_pct'] = float(hs.get("win_pct", defaults['home_win_pct']) or 0.5)
                features['away_win_pct'] = float(as_.get("win_pct", defaults['away_win_pct']) or 0.5)
                features['win_pct_diff'] = features['home_win_pct'] - features['away_win_pct']

                features['home_offense'] = float(hs.get("avg_points_for", defaults['home_offense']) or 0.0)
                features['away_offense'] = float(as_.get("avg_points_for", defaults['away_offense']) or 0.0)
                features['offense_diff'] = features['home_offense'] - features['away_offense']

                features['home_defense'] = float(hs.get("avg_points_against", defaults['home_defense']) or 0.0)
                features['away_defense'] = float(as_.get("avg_points_against", defaults['away_defense']) or 0.0)
                features['defense_diff'] = features['home_defense'] - features['away_defense']
        except Exception:
            logger.warning("Team season summary features fell back to defaults.")

        # Temporal features
        try:
            game_date = pd.to_datetime(game["game_date"])
            features['month'] = float(int(game_date.month))
            features['day_of_week'] = float(int(game_date.weekday()))
        except Exception:
            pass

        # Recent form
        try:
            home_form = self._recent_form_pct(conn, game["home_team"], game["game_date"])
            away_form = self._recent_form_pct(conn, game["away_team"], game["game_date"])
            features['home_recent_form'] = float(home_form)
            features['away_recent_form'] = float(away_form)
            features['form_diff'] = float(home_form - away_form)
        except Exception:
            pass

        # Head-to-head extended features
        try:
            q = """
                SELECT 
                    CASE WHEN home_team = :home AND home_score > away_score THEN 1
                         WHEN away_team = :home AND away_score > home_score THEN 1
                         ELSE 0 END as home_team_won
                FROM games 
                WHERE ((home_team = :home AND away_team = :away) OR 
                       (home_team = :away AND away_team = :home))
                  AND home_score IS NOT NULL
                  AND game_date > date('now', '-3 years')
            """
            h2h = query_df(conn, q, {"home": game["home_team"], "away": game["away_team"]})
            features['h2h_games'] = float(len(h2h))
            features['home_h2h_win_rate'] = float(h2h["home_team_won"].mean()) if len(h2h) else 0.5
        except Exception:
            pass

        # Basic team injury impact (totals)
        try:
            ii = self._get_basic_injury_impact(conn, game["home_team"], game["away_team"])
            features['home_injury_impact'] = float(ii.get("home", 0.0) or 0.0)
            features['away_injury_impact'] = float(ii.get("away", 0.0) or 0.0)
        except Exception:
            pass

        # Specific QB injury flags
        try:
            features['home_qb_injury'] = self._qb_injury_flag(conn, game["home_team"])
            features['away_qb_injury'] = self._qb_injury_flag(conn, game["away_team"])
        except Exception:
            pass

        # Fill any NaNs
        for k, v in list(features.items()):
            if pd.isna(v):
                features[k] = float(defaults.get(k, 0.0))

        return features

    def _analyze_injury_impact(self, conn, home_team: str, away_team: str) -> Dict[str, Dict[str, float]]:
        """Summarized injury impact by team/position; tolerant to schema differences."""
        try:
            tables = query_df(conn, "SELECT name FROM sqlite_master WHERE type='table' AND name='ai_injury_validation_detail'")
            if tables.empty:
                return {'home': {'total': 0.0}, 'away': {'total': 0.0}}

            cols = query_df(conn, "PRAGMA table_info(ai_injury_validation_detail)")
            names = set(cols["name"].tolist())

            team_col = "team_ai" if "team_ai" in names else "team_inj" if "team_inj" in names else "team" if "team" in names else None
            pos_col = "position" if "position" in names else "pos" if "pos" in names else None
            des_col = "designation" if "designation" in names else "status" if "status" in names else None

            if not (team_col and pos_col and des_col):
                return {'home': {'total': 0.0}, 'away': {'total': 0.0}}

            where = [f"{team_col} IN (:t1, :t2)"]
            if "inj_missing_team" in names:
                where.append("COALESCE(inj_missing_team,0)=0")
            if "roster_missing_team" in names:
                where.append("COALESCE(roster_missing_team,0)=0")
            if "team_mismatch" in names:
                where.append("COALESCE(team_mismatch,0)=0")

            sql = f"""
                SELECT {team_col} AS team, {pos_col} AS position, {des_col} AS designation, COUNT(*) AS cnt
                FROM ai_injury_validation_detail
                WHERE {" AND ".join(where)}
                GROUP BY {team_col}, {pos_col}, {des_col}
            """
            injuries = query_df(conn, sql, {"t1": home_team, "t2": away_team})

            impact = {'home': {}, 'away': {}}
            sev_w = {'OUT': 3, 'IR': 3, 'DOUBTFUL': 2, 'QUESTIONABLE': 1}
            pos_w = {'QB': 3, 'RB': 2, 'WR': 2, 'TE': 1.5}

            for _, r in injuries.iterrows():
                team_key = 'home' if str(r["team"]) == home_team else 'away'
                severity = sev_w.get(str(r["designation"]).upper(), 0)
                mult = pos_w.get(str(r["position"]).upper(), 1)
                score = float(severity * mult * (r["cnt"] or 0))
                impact[team_key][str(r["position"]).upper()] = impact[team_key].get(str(r["position"]).upper(), 0.0) + score

            impact['home']['total'] = sum(v for k, v in impact['home'].items() if k != 'total')
            impact['away']['total'] = sum(v for k, v in impact['away'].items() if k != 'total')
            return impact
        except Exception:
            logger.exception("injury impact failed")
            return {'home': {'total': 0.0}, 'away': {'total': 0.0}}

    def _get_basic_injury_impact(self, conn, home_team: str, away_team: str) -> Dict[str, float]:
        """Basic scalar per team used as features; very tolerant to schema."""
        try:
            tables = query_df(conn, "SELECT name FROM sqlite_master WHERE type='table' AND name='ai_injury_validation_detail'")
            if tables.empty:
                return {'home': 0.0, 'away': 0.0}

            cols = query_df(conn, "PRAGMA table_info(ai_injury_validation_detail)")
            names = set(cols["name"].tolist())

            team_col = "team_ai" if "team_ai" in names else "team_inj" if "team_inj" in names else "team" if "team" in names else None
            des_col = "designation" if "designation" in names else "status" if "status" in names else None
            pos_col = "position" if "position" in names else "pos" if "pos" in names else None

            if not (team_col and des_col):
                return {'home': 0.0, 'away': 0.0}

            where = [f"{team_col} IN (:t1, :t2)"]
            if "inj_missing_team" in names:
                where.append("COALESCE(inj_missing_team,0)=0")
            if "roster_missing_team" in names:
                where.append("COALESCE(roster_missing_team,0)=0")
            if "team_mismatch" in names:
                where.append("COALESCE(team_mismatch,0)=0")

            sql = f"""
                SELECT {team_col} AS team, {des_col} AS designation, {('' if not pos_col else pos_col)} AS position
                FROM ai_injury_validation_detail
                WHERE {" AND ".join(where)}
                LIMIT 500
            """
            df = query_df(conn, sql, {"t1": home_team, "t2": away_team})
            if df.empty:
                return {'home': 0.0, 'away': 0.0}

            sev_w = {'OUT': 3, 'IR': 3, 'DOUBTFUL': 2, 'QUESTIONABLE': 1}
            pos_w = {'QB': 3, 'RB': 2, 'WR': 2, 'TE': 1.5}

            scores = {'home': 0.0, 'away': 0.0}
            for _, r in df.iterrows():
                key = 'home' if str(r["team"]) == home_team else 'away'
                sev = sev_w.get(str(r["designation"]).upper(), 0)
                mult = pos_w.get(str(r.get("position", "")).upper(), 1)
                scores[key] += float(sev * mult)
            return scores
        except Exception:
            return {'home': 0.0, 'away': 0.0}

    def _get_weather_impact(self, game) -> Optional[Dict]:
        # Placeholder
        return None

    def _identify_key_factors(self, conn, game) -> List[str]:
        out: List[str] = []
        try:
            season = dt.datetime.now().year
            q = """
                SELECT team, power_score 
                FROM team_season_summary 
                WHERE season = :season AND team IN (:t1, :t2)
            """
            df = query_df(conn, q, {"season": season, "t1": game["home_team"], "t2": game["away_team"]})
            if len(df) == 2:
                hp = float(df.loc[df["team"] == game["home_team"], "power_score"].iloc[0])
                ap = float(df.loc[df["team"] == game["away_team"], "power_score"].iloc[0])
                diff = abs(hp - ap)
                if diff > 5.0:
                    stronger = game["home_team"] if hp > ap else game["away_team"]
                    out.append(f"Large talent gap favoring {stronger} ({diff:.1f}-pt diff).")
                elif diff < 1.0:
                    out.append("Very evenly matched by power ratings.")

            ii = self._get_basic_injury_impact(conn, game["home_team"], game["away_team"])
            if ii.get("home", 0.0) > 3.0:
                out.append(f"Notable injury concerns for {game['home_team']}.")
            if ii.get("away", 0.0) > 3.0:
                out.append(f"Notable injury concerns for {game['away_team']}.")

            if not out:
                out.append("Standard matchup; no major external flags.")
            return out
        except Exception:
            logger.warning("key factor derivation fell back")
            return ["Analysis data unavailable."]

    def _find_best_bet(self, conn, game_id: str, probabilities: Dict) -> Optional[Dict]:
        try:
            odds_sql = """
                SELECT o.team, o.sportsbook, o.odds, o.timestamp
                FROM odds o
                JOIN (
                    SELECT game_id, team, sportsbook, MAX(timestamp) AS max_ts
                    FROM odds 
                    WHERE game_id = :gid AND market = 'h2h'
                    GROUP BY game_id, team, sportsbook
                ) latest
                  ON o.game_id = latest.game_id 
                 AND o.team = latest.team 
                 AND o.sportsbook = latest.sportsbook 
                 AND o.timestamp = latest.max_ts
                WHERE o.game_id = :gid
            """
            odds = query_df(conn, odds_sql, {"gid": game_id})
            if odds.empty:
                return None

            ginfo = query_df(conn, "SELECT home_team, away_team FROM games WHERE game_id = :gid", {"gid": game_id}).iloc[0]
            best = []

            for team in [ginfo["home_team"], ginfo["away_team"]]:
                tdf = odds.loc[odds["team"] == team]
                if tdf.empty:
                    continue
                row = tdf.loc[tdf["odds"].idxmax()]
                ov = float(row["odds"])
                implied = 100 / (ov + 100) if ov > 0 else abs(ov) / (abs(ov) + 100)
                model_p = probabilities["home"] if team == ginfo["home_team"] else probabilities["away"]
                edge = model_p - implied
                if edge > 0.03:
                    best.append({
                        "team": team,
                        "odds": int(ov),
                        "sportsbook": row["sportsbook"],
                        "model_prob": model_p,
                        "implied_prob": implied,
                        "edge": edge,
                        "edge_pct": edge * 100,
                    })
            return max(best, key=lambda x: x["edge"]) if best else None
        except Exception:
            logger.warning("best bet calc failed", exc_info=True)
            return None

    def _calculate_confidence_score(self, probabilities: Dict, injury_impact: Dict, key_factors: List[str]) -> float:
        base = 0.7
        maxp = max(probabilities["home"], probabilities["away"])
        if maxp > 0.65:
            base += 0.1
        elif maxp < 0.55:
            base -= 0.1

        tot_inj = float(injury_impact.get("home", {}).get("total", 0.0)) + float(injury_impact.get("away", {}).get("total", 0.0))
        if tot_inj > 5.0:
            base -= 0.1

        if len(key_factors) < 2:
            base -= 0.05

        return max(0.3, min(0.95, base))

    def _generate_recommendation(self, probabilities: Dict, best_bet: Optional[Dict], confidence_score: float) -> str:
        home_prob = probabilities["home"]
        if best_bet and best_bet["edge"] > 0.05:
            return f"RECOMMENDED BET: {best_bet['team']} at {best_bet['odds']} ({best_bet['edge_pct']:.1f}% edge)"
        if confidence_score > 0.8 and abs(home_prob - 0.5) > 0.1:
            favored = "Home" if home_prob > 0.5 else "Away"
            return f"Strong lean toward {favored} but no clear market edge."
        return "No strong recommendation — market appears efficient."

# ---------------------------
# Orchestrator
# ---------------------------
# Fixed sections of ai_chat_stub.py

class ComprehensiveAI:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.analyzer = AdvancedBettingAnalyzer(self.db_manager)
        self.openai_client = self._init_openai()
        self.context_history: List[str] = []
        self.current_game_context = None 

    def _init_openai(self) -> Optional[OpenAI]:
        if not OPENAI_AVAILABLE:
            logger.warning("OpenAI not available - AI responses will be limited")
            return None
        key = os.getenv("OPENAI_API_KEY")
        if key:
            return OpenAI(api_key=key)
        logger.warning("No OpenAI API key found - AI responses will be limited")
        return None

    def generate_betting_recommendations(self, user_context: Optional[Dict] = None) -> Dict[str, Any]:
        """Generate personalized betting recommendations based on user's bankroll."""
        try:
            bankroll = float(user_context.get('bankroll', 500) if user_context else 500)
            
            # Get value bets
            value_bets = self.analyzer.find_value_bets_advanced(min_edge=0.03, max_odds=300)
            
            # Get game predictions
            conn = self.db_manager.get_connection()
            try:
                games_sql = """
                    SELECT DISTINCT g.game_id, g.home_team, g.away_team, g.game_date
                    FROM games g
                    WHERE g.game_date BETWEEN date('now') AND date('now', '+7 days')
                    ORDER BY g.game_date
                    LIMIT 20
                """
                games = query_df(conn, games_sql)
                
                recommendations = []
                total_recommended = 0
                max_single_bet = bankroll * 0.05  # 5% max per bet
                daily_budget = bankroll * 0.10    # 10% daily budget
                
                # Process value bets first
                for bet in value_bets:
                    if total_recommended >= daily_budget:
                        break
                        
                    # Calculate Kelly stake
                    if bet.edge_percentage > 3:  # Only recommend 3%+ edges
                        kelly_fraction = min(0.25, bet.edge_percentage / 100 * 0.5)  # Conservative Kelly
                        recommended_stake = min(max_single_bet, bankroll * kelly_fraction)
                        
                        if recommended_stake >= 5:  # Minimum $5 bet
                            recommendations.append({
                                'type': 'value_bet',
                                'game': f"Game ID: {bet.game_id}",
                                'team': bet.team,
                                'odds': bet.odds,
                                'sportsbook': bet.sportsbook,
                                'edge_percentage': bet.edge_percentage,
                                'recommended_stake': round(recommended_stake, 2),
                                'potential_profit': round(recommended_stake * (abs(bet.odds)/100 if bet.odds > 0 else 100/abs(bet.odds)), 2),
                                'confidence': bet.confidence_level,
                                'reason': f"{bet.edge_percentage:.1f}% edge over implied probability"
                            })
                            total_recommended += recommended_stake
                
                # Add high-confidence predictions without clear value
                if len(recommendations) < 3:
                    for _, game in games.head(10).iterrows():
                        if total_recommended >= daily_budget:
                            break
                            
                        analysis = self.analyzer.analyze_game_comprehensive(game['game_id'])
                        if analysis.confidence_score > 0.75:
                            max_prob = max(analysis.home_probability, analysis.away_probability)
                            if max_prob > 0.65:  # High confidence pick
                                favored_team = analysis.home_team if analysis.home_probability > analysis.away_probability else analysis.away_team
                                
                                # Conservative stake for non-value bets
                                stake = min(max_single_bet * 0.5, bankroll * 0.02)
                                if stake >= 5:
                                    recommendations.append({
                                        'type': 'confidence_bet',
                                        'game': f"{analysis.away_team} @ {analysis.home_team}",
                                        'team': favored_team,
                                        'odds': 'Check sportsbook',
                                        'sportsbook': 'Various',
                                        'model_probability': round(max_prob * 100, 1),
                                        'recommended_stake': round(stake, 2),
                                        'confidence': 'High',
                                        'reason': f"Model confidence: {analysis.confidence_score:.2f}, Probability: {max_prob:.1%}"
                                    })
                                    total_recommended += stake
                
                return {
                    'ok': True,
                    'success': True,
                    'result': {
                        'recommendations': recommendations[:5],  # Top 5 recommendations
                        'bankroll': bankroll,
                        'total_recommended': round(total_recommended, 2),
                        'remaining_budget': round(daily_budget - total_recommended, 2),
                        'risk_level': 'Conservative' if total_recommended < bankroll * 0.05 else 'Moderate'
                    }
                }
                
            finally:
                conn.close()
                
        except Exception as e:
            logger.exception("generate_betting_recommendations failed")
            return {
                'ok': False,
                'success': False,
                'error': str(e),
                'message': 'Failed to generate betting recommendations'
            }

    def _classify_intent(self, message: str) -> MessageIntent:
        """Improved intent classification to ensure proper routing."""
        m = message.lower()
        
        # More specific patterns for game analysis
        if any(w in m for w in ["analyze", "analysis", "predict", "pick", "who wins", "explain", "breakdown", "odds", "probability"]):
            return MessageIntent.GAME_ANALYSIS
        
        # Value bet patterns
        if any(w in m for w in ["value", "edge", "opportunity", "bet", "find"]) and any(w in m for w in ["bet", "edge", "%", "percent"]):
            return MessageIntent.VALUE_BETS
        
        # Injury patterns
        if any(w in m for w in ["injury", "injured", "hurt", "out", "questionable", "doubtful"]):
            return MessageIntent.INJURY_REPORT
        
        # Default to game analysis if we have a selected game and it's analysis-related
        return MessageIntent.GAME_ANALYSIS

    def _build_context(self, game_id: Optional[str] = None, user_context: Optional[Dict] = None) -> str:
        """Build context string for AI requests."""
        parts = []
        
        if game_id:
            try:
                analysis = self.analyzer.analyze_game_comprehensive(game_id)
                parts.append(f"Selected Game: {analysis.away_team} @ {analysis.home_team}")
                parts.append(f"Model Prediction: {analysis.home_team if analysis.home_probability > 0.5 else analysis.away_team} ({max(analysis.home_probability, analysis.away_probability):.1%})")
                if analysis.best_bet:
                    parts.append(f"Best Bet: {analysis.best_bet.get('team')} at {analysis.best_bet.get('odds')} ({analysis.best_bet.get('edge_pct', 0):.1f}% edge)")
            except Exception as e:
                logger.warning(f"Failed to build game context: {e}")
        
        if user_context:
            bankroll = user_context.get('bankroll', 500)
            parts.append(f"User Bankroll: ${bankroll:.2f}")
        
        return "\n".join(parts) if parts else "No specific context available"

    def _handle_value_bets(self, message: str, context: str) -> Dict[str, Any]:
        """Handle value bet requests."""
        try:
            # Extract edge threshold from message
            edge_match = re.search(r'(\d+(?:\.\d+)?)%?\s*(?:edge|or higher|or better)', message.lower())
            min_edge = float(edge_match.group(1)) / 100 if edge_match else 0.05
            
            # Get value bets from analyzer
            value_bets = self.analyzer.find_value_bets_advanced(min_edge=min_edge)
            
            # Convert ValueBet objects to dicts
            bets_data = []
            for bet in value_bets:
                bets_data.append({
                    "game_id": bet.game_id,
                    "team": bet.team,
                    "odds": bet.odds,
                    "sportsbook": bet.sportsbook,
                    "edge_pct": round(bet.edge_percentage, 1),
                    "model_prob": round(bet.model_probability, 3),
                    "implied_prob": round(bet.implied_probability, 3),
                    "recommended_amount": round(bet.recommended_stake, 2),
                    "confidence_level": bet.confidence_level,
                    "risk_assessment": bet.risk_assessment
                })

            return {
                "ok": True,
                "intent": "value_bets",
                "success": True,
                "result": bets_data,
                "total_found": len(bets_data),
                "min_edge_used": min_edge * 100
            }

        except Exception as e:
            logger.exception("_handle_value_bets failed")
            return {
                "ok": False,
                "intent": "value_bets",
                "success": False,
                "error": str(e),
                "message": "Failed to find value bets."
            }

    def _handle_general_chat(self, message: str, context: str) -> Dict[str, Any]:
        """Handle general chat requests."""
        if not self.openai_client:
            return self._handle_fallback(message, MessageIntent.GENERAL_CHAT, context)
            
        try:
            system_prompt = f"""You are a professional NFL betting analyst with years of experience.

CURRENT CONTEXT:
{context}

You provide:
- Specific betting insights based on data
- Honest assessments of betting opportunities  
- Proper bankroll management advice
- Market efficiency perspectives

Be conversational but analytical. Avoid guarantees or reckless advice.
USER MESSAGE: {message}"""

            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": message}
                ],
                max_tokens=600,
                temperature=0.7
            )
            
            ai_message = response.choices[0].message.content.strip()
            
            return {
                "ok": True,
                "intent": "general_chat",
                "success": True,
                "result": {"message": ai_message}
            }
            
        except Exception as e:
            logger.exception("_handle_general_chat failed")
            return self._handle_fallback(message, MessageIntent.GENERAL_CHAT, context)

    def _handle_fallback(self, message: str, intent: MessageIntent, context: str) -> Dict[str, Any]:
        """Handle fallback responses when other handlers fail."""
        fallback_messages = {
            MessageIntent.GAME_ANALYSIS: "Please select a specific game from the sidebar to analyze.",
            MessageIntent.VALUE_BETS: "Unable to find value bets at this time. Please try again or lower your edge threshold.",
            MessageIntent.INJURY_REPORT: "Injury data is currently unavailable or being updated.",
            MessageIntent.GENERAL_CHAT: "I can help you analyze games, find value bets, or explain betting strategies. What would you like to know?"
        }
        
        return {
            "ok": False,
            "intent": intent.value,
            "success": False,
            "message": fallback_messages.get(intent, "I can help with NFL betting analysis. Please try rephrasing your question.")
        }

    def _request_game_selection(self) -> Dict[str, Any]:
        """Request user to select a game."""
        return {
            "ok": False,
            "intent": "game_selection_required",
            "success": False,
            "message": "Please select a specific game from the sidebar to analyze."
        }

    # ADD THIS MISSING METHOD FOR THE BLUEPRINT:
    def _to_frontend(self, internal_response: Dict[str, Any]) -> Dict[str, Any]:
        """Convert internal response format to frontend format."""
        return {
            "ok": internal_response.get("ok", internal_response.get("success", True)),
            "intent": internal_response.get("intent", "general"),
            "result": internal_response.get("result"),
            "success": internal_response.get("success", True),
            "error": internal_response.get("error"),
            "message": internal_response.get("message")
        }

    # ALSO ADD THIS METHOD TO YOUR COMPREHENSIVE AI CLASS:
    def _format_value_bets_text(self, bets_data: List[Dict], min_edge: float, near_data: List) -> str:
        """Format value bets into readable text."""
        if not bets_data:
            return f"No value bets found with {min_edge:.1f}%+ edge."
        
        lines = [f"Found {len(bets_data)} value opportunities with {min_edge:.1f}%+ edge:"]
        for bet in bets_data[:5]:  # Top 5
            odds_str = f"+{bet['odds']}" if bet['odds'] > 0 else str(bet['odds'])
            lines.append(f"• {bet['team']} ML {odds_str} @ {bet.get('sportsbook', 'Unknown')} ({bet['edge_pct']:.1f}% edge)")
        
        if len(bets_data) > 5:
            lines.append(f"... and {len(bets_data) - 5} more")
            
        return "\n".join(lines)

    def _handle_game_analysis(self, game_id: str, message: str, context: str) -> Dict[str, Any]:
        """Enhanced game analysis handler that always calls LLM for detailed explanations."""
        try:
            # Get the core analysis from your model
            analysis = self.analyzer.analyze_game_comprehensive(game_id)
            
            # Always generate AI commentary for game analysis requests
            if self.openai_client:
                ai_commentary = self._generate_detailed_analysis_commentary(analysis, message, context)
            else:
                # Fallback with more detailed explanation when OpenAI not available
                ai_commentary = self._generate_detailed_fallback_commentary(analysis, message)

            return {
                "ok": True,
                "intent": "analysis",
                "success": True,
                "result": {
                    "game": f"{analysis.away_team} @ {analysis.home_team}",
                    "date": analysis.game_date,
                    "probabilities": {"home": round(analysis.home_probability, 3), "away": round(analysis.away_probability, 3)},
                    "best_bet": analysis.best_bet,
                    "key_factors": analysis.key_factors,
                    "injury_impact": analysis.injury_impact,
                    "confidence_score": round(analysis.confidence_score, 2),
                    "recommendation": analysis.recommendation,
                    "summary": ai_commentary,  # This is the detailed LLM response
                    "injuries": {
                        "home": {"qb": analysis.injury_impact.get("home", {}).get("QB", 0)},
                        "away": {"qb": analysis.injury_impact.get("away", {}).get("QB", 0)}
                    }
                }
            }
        except Exception as e:
            logger.exception("_handle_game_analysis failed")
            return {
                "ok": False,
                "intent": "analysis",
                "success": False,
                "error": str(e) or "Game analysis error",
                "message": "Failed to analyze game."
            }

    def _generate_detailed_analysis_commentary(self, analysis: GameAnalysis, user_message: str, context: str) -> str:
        """Generate detailed AI commentary using OpenAI for game analysis."""
        try:
            # Build comprehensive prompt with all your model's data
            prompt = f"""You are a professional NFL betting analyst. Provide an in-depth analysis based on this data from our ML model:

GAME: {analysis.away_team} @ {analysis.home_team} ({analysis.game_date})

MODEL PREDICTIONS:
- Home win probability: {analysis.home_probability:.1%}
- Away win probability: {analysis.away_probability:.1%}
- Model confidence: {analysis.confidence_score:.2f}

BETTING RECOMMENDATION:
{analysis.recommendation}

BEST BET DETECTED:
{f"Team: {analysis.best_bet.get('team', 'None')}, Odds: {analysis.best_bet.get('odds', 'N/A')}, Edge: {analysis.best_bet.get('edge_pct', 0):.1f}%, Sportsbook: {analysis.best_bet.get('sportsbook', 'N/A')}" if analysis.best_bet else "No clear value bet identified"}

KEY FACTORS:
{chr(10).join(f"• {factor}" for factor in analysis.key_factors)}

INJURY ANALYSIS:
- Home team injury impact: {analysis.injury_impact.get('home', {}).get('total', 0):.1f}
- Away team injury impact: {analysis.injury_impact.get('away', {}).get('total', 0):.1f}

USER QUESTION: "{user_message}"

CONTEXT: {context}

Provide a detailed explanation that:
1. Explains WHY the model favors one team
2. Discusses the specific factors driving the prediction
3. Addresses any injury concerns and their impact
4. Explains the betting value (or lack thereof)
5. Gives actionable insights for betting decisions

Be specific about the data points and explain the reasoning behind the model's confidence level."""

            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=600,
                temperature=0.7,
            )
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            logger.exception("OpenAI commentary generation failed")
            return self._generate_detailed_fallback_commentary(analysis, user_message)

    def _generate_detailed_fallback_commentary(self, analysis: GameAnalysis, user_message: str) -> str:
        """Generate detailed fallback commentary when OpenAI is unavailable."""
        
        favored_team = analysis.home_team if analysis.home_probability > 0.5 else analysis.away_team
        favored_prob = max(analysis.home_probability, analysis.away_probability)
        
        commentary = []
        
        # Model prediction explanation
        commentary.append(f"**Model Analysis:** Our ML model predicts {favored_team} with {favored_prob:.1%} probability to win.")
        
        # Confidence explanation
        if analysis.confidence_score > 0.8:
            commentary.append(f"**High Confidence:** The model has strong conviction ({analysis.confidence_score:.2f}/1.0) in this prediction.")
        elif analysis.confidence_score < 0.6:
            commentary.append(f"**Lower Confidence:** This is a closer matchup with moderate model confidence ({analysis.confidence_score:.2f}/1.0).")
        else:
            commentary.append(f"**Moderate Confidence:** Standard confidence level ({analysis.confidence_score:.2f}/1.0) for this prediction.")
        
        # Key factors
        if analysis.key_factors:
            commentary.append(f"**Key Factors:** {' '.join(analysis.key_factors)}")
        
        # Betting value
        if analysis.best_bet and analysis.best_bet.get('edge', 0) > 0.03:
            bet = analysis.best_bet
            commentary.append(f"**Betting Value:** {bet['team']} at {bet['odds']} shows {bet.get('edge_pct', bet['edge']*100):.1f}% edge over the implied probability.")
        else:
            commentary.append("**Betting Value:** No significant edge detected in current market prices.")
        
        # Injuries
        home_inj = analysis.injury_impact.get('home', {}).get('total', 0)
        away_inj = analysis.injury_impact.get('away', {}).get('total', 0)
        if home_inj > 2 or away_inj > 2:
            commentary.append(f"**Injury Impact:** Notable injury concerns factored into the analysis.")
        
        return "\n\n".join(commentary)

    def _handle_injury_report(self, message: str, context: str) -> Dict[str, Any]:
        """Enhanced injury report that focuses on selected game teams when applicable."""
        try:
            conn = self.db_manager.get_connection()
            
            # Check if we have a selected game to filter injuries
            game_teams = None
            if hasattr(self, 'current_game_context') and self.current_game_context:
                game_teams = [self.current_game_context.get('home_team'), self.current_game_context.get('away_team')]

            # Get injury data (same DB query as before)
            tables = query_df(conn, "SELECT name FROM sqlite_master WHERE type='table' AND name='ai_injury_validation_detail'")
            if tables.empty:
                return {
                    "ok": True,
                    "intent": "injury_report",
                    "success": True,
                    "result": {
                        "injuries": [],
                        "team_impacts": {},
                        "total_injuries": 0
                    }
                }

            cols = query_df(conn, "PRAGMA table_info(ai_injury_validation_detail)")
            avail = set(cols["name"].tolist())

            def pick(cands, literal_default):
                for c in cands:
                    if c in avail:
                        return c
                return literal_default

            name_col = pick(["player_name", "inj_name", "roster_name", "player"], "'N/A'")
            team_col = pick(["team_ai", "team_inj", "team", "team_name"], "'N/A'")
            pos_col = pick(["position", "pos"], "''")
            des_col = pick(["designation", "status"], "''")
            det_col = pick(["injury_detail", "detail", "notes"], "''")
            imp_col = pick(["impact_score"], "0")
            
            filters = []
            for c in ("inj_missing_team", "roster_missing_team", "team_mismatch"):
                if c in avail:
                    filters.append(f"COALESCE({c},0)=0")
            
            # Add team filter if we're in game context
            if game_teams and any(game_teams):
                team_filter = " OR ".join([f"{team_col} = '{team}'" for team in game_teams if team])
                filters.append(f"({team_filter})")
            
            where_sql = ("WHERE " + " AND ".join(filters)) if filters else ""

            sql = f"""
                SELECT
                    {name_col} AS player_name,
                    {team_col} AS team,
                    COALESCE({pos_col}, '') AS position,
                    COALESCE({des_col}, '') AS designation,
                    COALESCE({det_col}, '') AS injury_detail,
                    COALESCE({imp_col}, 0) AS impact_score
                FROM ai_injury_validation_detail
                {where_sql}
                ORDER BY COALESCE({imp_col},0) DESC,
                         CASE COALESCE({pos_col}, '')
                            WHEN 'QB' THEN 1 WHEN 'RB' THEN 2
                            WHEN 'WR' THEN 3 ELSE 4 END
                LIMIT 25
            """

            injuries = query_df(conn, sql)

            data = []
            for _, r in injuries.iterrows():
                # Only include meaningful injuries
                if str(r.get("designation", "")).upper() in ["OUT", "DOUBTFUL", "QUESTIONABLE", "IR"]:
                    data.append({
                        "player": r.get("player_name", "N/A"),
                        "team": r.get("team", "N/A"), 
                        "position": r.get("position", ""),
                        "designation": r.get("designation", ""),
                        "detail": r.get("injury_detail", ""),
                        "impact_score": float(r.get("impact_score", 0) or 0),
                    })

            team_impacts = {}
            if "team" in injuries.columns and "impact_score" in injuries.columns:
                team_impacts = injuries.groupby("team")["impact_score"].sum().to_dict()

            try:
                conn.close()
            except Exception:
                pass

            return {
                "ok": True,
                "intent": "injury_report", 
                "success": True,
                "result": {
                    "injuries": data,
                    "team_impacts": team_impacts,
                    "total_injuries": len(data),
                    "filtered_to_game": bool(game_teams)
                }
            }
            
        except Exception as e:
            logger.exception("_handle_injury_report failed")
            return {
                "ok": False,
                "intent": "injury_report",
                "success": False,
                "error": str(e) or "Injury report error", 
                "message": "Failed to get injury report."
            }

    def process_message(self, message: str, game_id: Optional[str] = None, user_context: Optional[Dict] = None) -> Dict[str, Any]:
        """Enhanced message processing that maintains game context."""
        intent = self._classify_intent(message)
        context = self._build_context(game_id, user_context)
        
        # Store current game context for injury filtering
        if game_id:
            try:
                conn = self.db_manager.get_connection()
                game_info = query_df(conn, "SELECT home_team, away_team FROM games WHERE game_id = :gid", {"gid": game_id})
                if not game_info.empty:
                    self.current_game_context = {
                        'home_team': game_info.iloc[0]['home_team'],
                        'away_team': game_info.iloc[0]['away_team']
                    }
                conn.close()
            except Exception:
                pass

        try:
            if intent == MessageIntent.GAME_ANALYSIS:
                if game_id:
                    return self._handle_game_analysis(game_id, message, context)
                return self._request_game_selection()
                
            elif intent == MessageIntent.VALUE_BETS:
                return self._handle_value_bets(message, context)
                
            elif intent == MessageIntent.INJURY_REPORT:
                return self._handle_injury_report(message, context)
                
            elif intent == MessageIntent.GENERAL_CHAT:
                return self._handle_general_chat(message, context)
                
            return self._handle_fallback(message, intent, context)
            
        except Exception as e:
            logger.exception("process_message failed")
            return {
                "ok": False,
                "intent": intent.value,
                "success": False,
                "error": str(e) or "Unhandled error",
                "message": "Operation failed. See server logs for details."
            }
# ---------------------------
# Flask Blueprint
# ---------------------------
# Use the same blueprint name your UI imports
comprehensive_ai_bp = Blueprint("ai", __name__)
ai_system = ComprehensiveAI()

@comprehensive_ai_bp.route("/api/ai-chat", methods=["POST"])
def ai_chat_compat():
    return comprehensive_ai_chat()

@comprehensive_ai_bp.route("/api/ai-betting-recommendations", methods=["GET"])
def get_betting_recommendations():
    try:
        user_context = {
            'bankroll': session.get('user_bankroll', 500),
            'username': session.get('username', 'User')
        }
        
        recommendations = ai_system.generate_betting_recommendations(user_context)
        return jsonify(recommendations)
        
    except Exception as e:
        logger.exception("get_betting_recommendations failed")
        return jsonify({'ok': False, 'error': str(e)}), 500

@comprehensive_ai_bp.route("/api/ai-chat-comprehensive", methods=["POST"])
def comprehensive_ai_chat():
    """Main comprehensive AI chat endpoint (normalized to {ok,intent,result})."""
    try:
        data = request.get_json() or {}
        message = data.get('message', '').strip()
        game_id = data.get('game_id')

        # Get user context from session
        user_context = {
            'bankroll': session.get('user_bankroll', 500),
            'username': session.get('username', 'User')
        }

        if not message:
            return jsonify({
                'ok': False,
                'error': 'No message provided'
            }), 400

        # Internal processing
        internal = ai_system.process_message(message, game_id, user_context)

        # Normalize to frontend shape
        payload = ai_system._to_frontend(internal)
        return jsonify(payload)

    except Exception as e:
        logger.error(f"Comprehensive AI chat error: {e}")
        return jsonify({
            'ok': False,
            'error': 'Internal server error'
        }), 500


@comprehensive_ai_bp.route("/api/ai-game-analysis/<game_id>", methods=["GET"])
def get_game_analysis(game_id: str):
    try:
        analysis = ai_system.analyzer.analyze_game_comprehensive(game_id)
        internal = {
            "success": True,
            "intent": "game_analysis",
            "analysis": {
                "game": f"{analysis.away_team} @ {analysis.home_team}",
                "date": analysis.game_date,
                "probabilities": {
                    "home": round(analysis.home_probability, 3),
                    "away": round(analysis.away_probability, 3)
                },
                "best_bet": analysis.best_bet,
                "key_factors": analysis.key_factors,
                "injury_impact": analysis.injury_impact,
                "confidence_score": round(analysis.confidence_score, 2),
                "recommendation": analysis.recommendation
            },
            "message": f"{analysis.away_team} @ {analysis.home_team} — Home {analysis.home_probability:.1%} / Away {analysis.away_probability:.1%}"
        }
        return jsonify(ai_system._to_frontend(internal))
    except Exception as e:
        logger.error(f"Game analysis API error: {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500


@comprehensive_ai_bp.route("/api/ai-value-bets", methods=["GET"])
def get_value_bets():
    try:
        min_edge = float(request.args.get('min_edge', 0.05))
        max_odds = int(request.args.get('max_odds', 400))

        value_bets = ai_system.analyzer.find_value_bets_advanced(min_edge=min_edge, max_odds=max_odds)

        bets_data = [{
            "game_id": bet.game_id,
            "team": bet.team,
            "odds": bet.odds,
            "sportsbook": bet.sportsbook,
            "edge_percentage": round(bet.edge_percentage, 1),
            "recommended_stake": round(bet.recommended_stake, 2),
            "confidence_level": bet.confidence_level,
            "risk_assessment": bet.risk_assessment
        } for bet in value_bets]

        # Provide the same rich text as chat endpoint
        near_data = []
        internal = {
            "success": True,
            "intent": "value_bets",
            "value_bets": bets_data,
            "total_found": len(bets_data),
            "min_edge_used": min_edge * 100.0,
            "message": ComprehensiveAI()._format_value_bets_text(bets_data, min_edge * 100.0, near_data)
        }
        return jsonify(ai_system._to_frontend(internal))
    except Exception as e:
        logger.error(f"Value bets API error: {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500

@comprehensive_ai_bp.route("/api/ai-status", methods=["GET"])
def get_ai_status():
    return jsonify({
        "openai_available": ai_system.openai_client is not None,
        "model_loaded": ai_system.analyzer.model_pack is not None,
        "database_connected": True,  # lightweight check
        "cache_size": len(ai_system.analyzer.cache),
        "status": "operational",
    })

__all__ = ["comprehensive_ai_bp", "ai_system", "ComprehensiveAI"]
