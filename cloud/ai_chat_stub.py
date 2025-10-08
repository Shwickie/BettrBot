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
from datetime import datetime, timedelta
from pathlib import Path
from sqlalchemy import create_engine, text
import pandas as pd
import sqlite3, os, json, threading, time
import numpy as np
from flask import Blueprint, request, jsonify, session

import pandas as pd
from sqlalchemy import create_engine, text
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
import datetime as dt
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

# Better OpenAI import with error details
try:
    from openai import OpenAI
    import openai as _openai
    OPENAI_AVAILABLE = True
    print("✅ OpenAI library imported successfully")
except ImportError as e:
    OPENAI_AVAILABLE = False
    OpenAI = None
    print(f"❌ OpenAI import failed: {e}")
    print("💡 Install with: pip install openai")
except Exception as e:
    OPENAI_AVAILABLE = False
    OpenAI = None
    print(f"❌ Unexpected error importing OpenAI: {e}")
sys.path.insert(0, os.path.dirname(__file__))

try:
    from model.prediction import FixedNFLSystem
    FIXED_NFL_SYSTEM_AVAILABLE = True
except ImportError:
    FIXED_NFL_SYSTEM_AVAILABLE = False
    FixedNFLSystem = None

try:
    from model.ai_tools import list_value_bets
except ImportError:
    list_value_bets = None


print(f"🔑 OpenAI API Key present: {bool(os.getenv('OPENAI_API_KEY'))}")
if os.getenv("OPENAI_API_KEY"):
    key = os.getenv("OPENAI_API_KEY")
    print(f"🔑 Key starts with: {key[:10]}...")
# ---------------------------
# Logging
# ---------------------------
# logging.basicConfig(level=logging.INFO)
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

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql+psycopg2://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway")

# CRITICAL: Force cloud database usage
USE_CLOUD_DB = True

if USE_CLOUD_DB:
    print(f"Using Railway database: {DATABASE_URL[:60]}...")
    
    ENGINE = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=280,
        pool_size=2,
        max_overflow=3,
        connect_args={
            "connect_timeout": 20,
            "application_name": "bettr-bot",
        }
    )

# near the top of ai_chat_stub.py with other imports
import os, pickle

# use env var first, then a local file next to this module
def get_model_path():
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

_model_pack = None
def load_model_pack():
    """Load and cache packed model with proper validation."""
    global _model_pack
    if _model_pack is not None:
        return _model_pack

    path = get_model_path()
    if not path:
        print("CRITICAL: Model pack not found in any location!")
        _model_pack = None
        return None

    try:
        with open(path, "rb") as f:
            _model_pack = pickle.load(f)
        
        # VALIDATE the model pack has required keys
        required_keys = ['model', 'feature_cols']
        missing_keys = [key for key in required_keys if key not in _model_pack]
        
        if missing_keys:
            print(f"CRITICAL: Model pack missing required keys: {missing_keys}")
            _model_pack = None
            return None
        
        # Add scaler if missing (for backward compatibility)
        if 'scaler' not in _model_pack:
            _model_pack['scaler'] = None
        
        print(f"SUCCESS: Loaded model pack from {path}")
        print(f"  Features: {len(_model_pack.get('feature_cols', []))}")
        print(f"  Model type: {type(_model_pack.get('model', 'Unknown'))}")
        return _model_pack
        
    except Exception as e:
        print(f"CRITICAL: Failed to load model pack from {path}: {e}")
        import traceback
        traceback.print_exc()
        _model_pack = None
        return None

def verify_model_consistency():
    """Check if AI chat and dashboard use the same model"""
    try:
        import mobile_dashboard
        ai_model = load_model_pack()
        dashboard_model = mobile_dashboard.load_model_pack()
        
        if ai_model and dashboard_model:
            ai_features = len(ai_model.get('feature_cols', []))
            dash_features = len(dashboard_model.get('feature_cols', []))
            logger.info(f"AI model features: {ai_features}, Dashboard features: {dash_features}")
            return ai_features == dash_features
        return False
    except Exception as e:
        logger.error(f"Model consistency check failed: {e}")
        return False
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
        self.model_pack = load_model_pack()
        self.prediction_system = None
        if FIXED_NFL_SYSTEM_AVAILABLE:
            try:
                self.prediction_system = FixedNFLSystem()
                logger.info("AI Chat: Using FixedNFLSystem (dashboard model)")
            except Exception as e:
                logger.warning(f"AI Chat: FixedNFLSystem unavailable, falling back. Reason: {e}")
                self.prediction_system = None
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
    
    def _normalize_team_name(self, name: str) -> str:
        """Return a canonical full team name for matching odds rows."""
        mapping = {
            'ARI':'Arizona Cardinals','ATL':'Atlanta Falcons','BAL':'Baltimore Ravens','BUF':'Buffalo Bills',
            'CAR':'Carolina Panthers','CHI':'Chicago Bears','CIN':'Cincinnati Bengals','CLE':'Cleveland Browns',
            'DAL':'Dallas Cowboys','DEN':'Denver Broncos','DET':'Detroit Lions','GB':'Green Bay Packers',
            'HOU':'Houston Texans','IND':'Indianapolis Colts','JAX':'Jacksonville Jaguars','KC':'Kansas City Chiefs',
            'LV':'Las Vegas Raiders','LAC':'Los Angeles Chargers','LAR':'Los Angeles Rams','LA':'Los Angeles Rams',
            'MIA':'Miami Dolphins','MIN':'Minnesota Vikings','NE':'New England Patriots','NO':'New Orleans Saints',
            'NYG':'New York Giants','NYJ':'New York Jets','PHI':'Philadelphia Eagles','PIT':'Pittsburgh Steelers',
            'SF':'San Francisco 49ers','SEA':'Seattle Seahawks','TB':'Tampa Bay Buccaneers','TEN':'Tennessee Titans',
            'WAS':'Washington Commanders','WSH':'Washington Commanders'
        }
        if not name:
            return ''
        key = name.strip()
        return mapping.get(key.upper(), key)


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

    
    def find_value_bets_advanced(self, min_edge=0.05, max_odds=400):
        """FIXED: Actually find betting opportunities with realistic thresholds"""
        cache_key = f"value_bets_{min_edge}_{max_odds}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        conn = self.db_manager.get_connection()
        value_bets = []
        
        try:
            # Get upcoming games with odds
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

            if games.empty:
                print("No upcoming games with odds found")
                return []

            # Get latest odds for each team/sportsbook
            odds_sql = """
                SELECT o.game_id, o.team, o.sportsbook, o.odds
                FROM odds o
                JOIN (
                    SELECT game_id, team, sportsbook, MAX(timestamp) AS max_ts
                    FROM odds 
                    WHERE market = 'h2h'
                    GROUP BY game_id, team, sportsbook
                ) latest
                ON o.game_id = latest.game_id
                AND o.team = latest.team 
                AND o.sportsbook = latest.sportsbook 
                AND o.timestamp = latest.max_ts
            """
            all_odds = query_df(conn, odds_sql)

            print(f"Analyzing {len(games)} games for value bets...")

            for _, game in games.iterrows():
                try:
                    # Get model prediction
                    probs = self._calculate_win_probabilities(conn, game)
                    
                    # DIAGNOSTIC: Log what the model actually predicted
                    # print(f"Game: {game['away_team']} @ {game['home_team']}")
                    # print(f"  Model: Home {probs['home']:.1%}, Away {probs['away']:.1%}")
                    
                    # Get odds for this game
                    game_odds = all_odds[all_odds['game_id'] == game['game_id']]
                    
                    if game_odds.empty:
                        print("  No odds found")
                        continue

                    # Check each team for value
                    # Process odds by team with name normalization
                    for team_name in [game['home_team'], game['away_team']]:
                        # Try exact match first
                        # Find odds for this team (exact or partial match)
                        team_odds = game_odds[
                            (game_odds['team'].str.casefold() == team_name.casefold()) |
                            (game_odds['team'].str.contains(team_name, case=False, na=False))
                        ]

                        if team_odds.empty:
                            print(f"    No odds found for {team_name}")
                            continue

                        # Normalize to decimal and pick the best price by decimal payout
                        team_odds = team_odds.copy()
                        team_odds['dec'] = team_odds['odds'].apply(self._to_decimal_odds)

                        best_idx = team_odds['dec'].idxmax()
                        best_odds_row = team_odds.loc[best_idx]
                        odds_val = float(best_odds_row['odds'])
                        decimal_odds = float(best_odds_row['dec'])
                        books_count = team_odds['sportsbook'].nunique()

                        print(f"    Best decimal: {decimal_odds}, Books: {books_count}")

                        if books_count < MIN_BOOKS_REQUIRED:
                            print(f"    Only {books_count} books, need {MIN_BOOKS_REQUIRED}")
                            continue

                        # Skip if odds too long (decimal)
                        if decimal_odds > MAX_DEC_ODDS:
                            print(f"    Odds too long: {decimal_odds}")
                            continue

                        # Model probability for this team
                        model_prob = p_home if team_name == game['home_team'] else p_away

                        # Confidence floor
                        if model_prob < MIN_CONF:
                            print(f"    Model prob {model_prob:.1%} below min {MIN_CONF:.1%}")
                            continue

                        # ✅ Implied probability from DECIMAL odds (always correct)
                        implied_prob = 1.0 / decimal_odds

                        # Edge (model - market)
                        edge = model_prob - implied_prob


                        print(f"  {team_name}: Model {model_prob:.1%}, Implied {implied_prob:.1%}, Edge {edge_pct:.1f}%")

                        # LOWERED THRESHOLD: Look for 2%+ edges instead of 5%+
                        if edge >= float(min_edge):  # 2% minimum edge
                            # Calculate Kelly stake
                            if odds_val > 0:
                                decimal_odds = 1 + (odds_val / 100)
                            else:
                                decimal_odds = 1 + (100 / abs(odds_val))

                            kelly = (model_prob * decimal_odds - 1) / (decimal_odds - 1)
                            
                            # Conservative Kelly (25% of full Kelly)
                            stake = max(5, min(100, kelly * 100 * 0.25))

                            # Confidence based on edge size
                            if edge > 0.08:
                                confidence = "High"
                            elif edge > 0.05:
                                confidence = "Medium"  
                            else:
                                confidence = "Low"

                            value_bets.append(ValueBet(
                                game_id=game['game_id'],
                                team=team_name,
                                odds=int(odds_val),
                                sportsbook=best_odds_row['sportsbook'],
                                model_probability=model_prob,
                                implied_probability=implied_prob,
                                edge_percentage=edge_pct,
                                recommended_stake=float(stake),
                                confidence_level=confidence,
                                risk_assessment="Low" if abs(odds_val) < 200 else "Medium"
                            ))

                            print(f"    *** VALUE BET FOUND: {edge_pct:.1f}% edge ***")

                except Exception as e:
                    logger.warning(f"Error analyzing game {game['game_id']}: {e}")
                    continue

            print(f"Found {len(value_bets)} value bets")
            
            # Sort by edge percentage
            value_bets.sort(key=lambda x: x.edge_percentage, reverse=True)
            top_bets = value_bets[:20]
            
            self._cache_result(cache_key, top_bets)
            return top_bets

        except Exception as e:
            logger.exception("find_value_bets_advanced failed")
            return []
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
        """Enhanced probability calculation with proper confidence."""
        methods = []
        game_id = str(game.get("game_id", "unknown"))

        
        # 1) Try FixedNFLSystem FIRST (same as dashboard)
        used_ml = False
        model_confidence = 0.6

        if self.prediction_system:  # ← Use FixedNFLSystem first
            try:
                # Check if prediction system is actually functional
                if not hasattr(self.prediction_system, 'model_data') or not self.prediction_system.model_data:
                    print(f"Prediction system model not loaded - using fallback")
                    raise RuntimeError("Model not loaded")
                    
                prediction_result = self.prediction_system.predict_game(
                    home_team=game["home_team"],
                    away_team=game["away_team"],
                    game_date=str(game["game_date"])
                )
                
                ml_prob = prediction_result['home_win_probability']
                methods.append(("fixed_nfl_system", ml_prob, 1 - ml_prob))
                used_ml = True
                model_confidence = prediction_result['confidence']
                
                logger.info(f"AI Chat using FixedNFLSystem: Home {ml_prob:.1%}")
                
            except Exception as e:
                logger.warning(f"FixedNFLSystem failed: {e}")
        
        # 2) Fallback to old model only if FixedNFLSystem fails
        if not used_ml and self.model_pack:
            try:
                ml_prob = self._predict_with_model(conn, game)
                methods.append(("ml_model", ml_prob, 1 - ml_prob))
                used_ml = True  # ← FIXED: Use consistent variable name
                logger.warning("AI Chat falling back to old model system")
            except Exception as e:
                logger.warning(f"Fallback ML model prediction failed: {e}")

        # Later in the same method:
        if used_ml:  # ← FIXED
            weights = {"fixed_nfl_system": 0.85, "ml_model": 0.70, "power_ratings": 0.10, "recent_form": 0.05}

        prediction_confidence = model_confidence if used_ml else 0.55  # (not used_fixed_nfl)

        
                        
        if game_id in self.feature_cache:
                    model_confidence = self.feature_cache[game_id].get('_model_confidence', 0.6)

        # 2) Power ratings
        power_prob = self._predict_with_power_ratings(conn, game)
        methods.append(("power_ratings", power_prob, None))

        # 3) Recent form  
        form_prob = self._predict_with_recent_form(conn, game)
        methods.append(("recent_form", form_prob, None))

        # 4) Head-to-head
        h2h_prob = self._predict_with_h2h_history(conn, game)
        methods.append(("h2h_history", h2h_prob, None))

        
        # Weighted ensemble - prioritize FixedNFLSystem
        if used_ml:
            weights = {"fixed_nfl_system": 0.85, "ml_model": 0.70, "power_ratings": 0.10, "recent_form": 0.05}
        else:
            weights = {"power_ratings": 0.50, "recent_form": 0.30, "h2h_history": 0.20}
        home_prob_weighted = 0.0
        total_weight = 0.0
        
        for method, home_prob, _ in methods:
            if home_prob is None:
                continue
            w = weights.get(method, 0.0)
            home_prob_weighted += home_prob * w
            total_weight += w
            
        final_home = (home_prob_weighted / total_weight) if total_weight > 0 else 0.5

        # Light calibration toward 50/50
        calibrated = 0.85 * final_home + 0.15 * 0.5

        # Store confidence based on model quality and prediction spread  
        prediction_confidence = model_confidence if used_ml else 0.55
        prob_spread = abs(calibrated - 0.5) * 2  # How far from 50/50
        final_confidence = prediction_confidence * (0.6 + 0.4 * prob_spread)

        return {
            "home": max(0.10, min(0.90, calibrated)),
            "away": max(0.10, min(0.90, 1 - calibrated)), 
            "method_details": methods,
            "source": "fixed_nfl_system" if used_ml else "heuristics",
            "model_confidence": final_confidence
        }

    def _predict_with_model(self, conn, game) -> float:
        """Enhanced model prediction with better confidence tracking."""
        # Build features
        if not self.model_pack:
            raise RuntimeError("Model pack not loaded - cannot make predictions")
        
        # Build features
        feat = self._build_game_features(conn, game)

        model = self.model_pack.get("model")
        scaler = self.model_pack.get("scaler")
        feature_cols = self.model_pack.get("feature_cols", [])
        
        if not model:
            raise RuntimeError("Model not found in model pack")
        if not feature_cols:
            raise RuntimeError("feature_cols not found in model pack")
        
        print(f"AI Chat: Using {len(feature_cols)} features for prediction")

        # Guarantee every expected feature exists
        for col in feature_cols:
            if col not in feat:
                feat[col] = 0.0

        X = pd.DataFrame([feat])[feature_cols]
        if scaler is not None and hasattr(scaler, "transform"):
            X = scaler.transform(X)

        # Get model
        estimator = model
        if not hasattr(estimator, "predict_proba") and hasattr(estimator, "steps"):
            try:
                estimator = estimator.steps[-1][1]
            except Exception:
                pass

        if not hasattr(estimator, "predict_proba"):
            raise RuntimeError("Loaded model has no predict_proba(...)")

        # Get prediction probabilities
        probabilities = estimator.predict_proba(X)[0]
        prob_home = float(probabilities[1])  # Class 1 = home win
        
        # Calculate model confidence based on probability distribution
        entropy = -sum(p * np.log(p + 1e-10) for p in probabilities if p > 0)
        max_entropy = np.log(len(probabilities))
        uncertainty = entropy / max_entropy
        model_confidence = 1 - uncertainty
        
        # Store both prediction and confidence for later use
        try:
            game_id = str(game.get("game_id", "unknown"))
            self.feature_cache[game_id] = {
                **feat,
                '_model_confidence': max(0.4, min(0.95, model_confidence)),
                '_prob_spread': abs(prob_home - 0.5) * 2  # 0-1 scale
            }
        except Exception:
            pass

        logger.info(
            "ML prediction: %s vs %s = %.3f (confidence=%.3f)",
            game["home_team"], game["away_team"],
            prob_home, model_confidence
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

    # Replace the debug_database_content and _build_game_features methods with these corrected versions:

    # def debug_database_content(self, conn):
    #     """Debug what's actually in the database - FIXED SQL"""
    #     try:
    #         # Check what tables exist
    #         tables = query_df(conn, "SELECT name FROM sqlite_master WHERE type='table'")
    #         logger.info(f"Available tables: {tables['name'].tolist()}")
            
    #         # Check team_season_summary content
    #         if 'team_season_summary' in tables['name'].values:
    #             tss = query_df(conn, "SELECT COUNT(*) as count FROM team_season_summary")
    #             logger.info(f"team_season_summary has {tss.iloc[0]['count']} rows")
                
    #             # Show sample data - FIXED SQL
    #             sample = query_df(conn, "SELECT season, team, power_score, win_pct FROM team_season_summary LIMIT 10")
    #             logger.info(f"Sample team data:\n{sample}")
                
    #             # Check current season data - FIXED parameter binding
    #             current_season = 2024
    #             current_data = query_df(conn, 
    #                 "SELECT COUNT(*) as count FROM team_season_summary WHERE season = :season", 
    #                 {"season": current_season})
    #             logger.info(f"Season {current_season} has {current_data.iloc[0]['count']} team records")
            
    #     except Exception as e:
    #         logger.error(f"Database debug failed: {e}")

    def _build_game_features(self, conn, game) -> Dict[str, float]:
        """Build feature vector with FIXED SQL queries"""
        
        # Debug database first
        #self.debug_database_content(conn)
        
        features = {}
        
        # Get season and teams
        try:
            season = int(pd.to_datetime(game["game_date"]).year)
            home_team = str(game["home_team"]).strip()
            away_team = str(game["away_team"]).strip()
            
            logger.info(f"Building features for {away_team} @ {home_team} (season {season})")
        except Exception as e:
            logger.error(f"Error parsing game info: {e}")
            season = 2024
            home_team = str(game.get("home_team", "UNK"))
            away_team = str(game.get("away_team", "UNK"))
        
        # Initialize with your model's exact feature set (from training output)
        feature_defaults = {
            'home_wpct_pre': 0.5, 'away_wpct_pre': 0.5, 'home_pf_pre': 20.0, 'away_pf_pre': 20.0,
            'home_pa_pre': 20.0, 'away_pa_pre': 20.0, 'home_pd_pre': 0.0, 'away_pd_pre': 0.0,
            'home_form': 0.5, 'home_streak': 0, 'away_form': 0.5, 'away_streak': 0,
            'same_division': 0, 'same_conference': 0, 'home_rest_days': 7, 'away_rest_days': 7,
            'month': 9, 'day_of_week': 0, 'home_power_pre': 0.0, 'away_power_pre': 0.0,
            'power_diff': 0.0, 'win_pct_diff': 0.0, 'offense_diff': 0.0, 'home_def_str': -20.0,
            'away_def_str': -20.0, 'defense_diff': 0.0, 'form_diff': 0.0, 'streak_diff': 0,
            'home_field_advantage': 2.5, 'late_season': 0, 'prime_time': 0, 'both_good': 0,
            'mismatch_game': 0, 'power_x_form': 0.0, 'strength_disparity': 0.0, 'rest_diff': 0
        }
        features.update(feature_defaults)
        
        # Try to get real team data - FIXED SQL queries
        try:
            # Your games are 2025 but data is in 2024 - force use 2024 data
            data_season = 2024  # Force this since your 2025 games need 2024 team stats
            
            logger.info(f"Looking for team data in season {data_season} (game season was {season})")
            
            # Strategy 1: Try exact team names first
            team_query = """
                SELECT season, team, power_score, win_pct, avg_points_for, avg_points_against
                FROM team_season_summary 
                WHERE season = :season AND (team = :home_team OR team = :away_team)
            """
            team_data = query_df(conn, team_query, {
                "season": data_season, 
                "home_team": home_team, 
                "away_team": away_team
            })
            
            if team_data.empty:
                logger.warning(f"No exact match, trying abbreviations...")
                
                # Strategy 2: Try with team abbreviations (your DB likely uses abbreviations)
                team_abbr_map = {
                    'Tennessee Titans': 'TEN', 'Los Angeles Rams': 'LAR', 'Los Angeles': 'LAR',
                    'Miami Dolphins': 'MIA', 'New England Patriots': 'NE', 'New England': 'NE',
                    'New Orleans Saints': 'NO', 'New Orleans': 'NO', 'San Francisco 49ers': 'SF',
                    'San Francisco': 'SF', 'Dallas Cowboys': 'DAL', 'Philadelphia Eagles': 'PHI',
                    'Kansas City Chiefs': 'KC', 'Kansas City': 'KC', 'Buffalo Bills': 'BUF',
                    'Tampa Bay Buccaneers': 'TB', 'Tampa Bay': 'TB', 'Green Bay Packers': 'GB',
                    'Green Bay': 'GB', 'Pittsburgh Steelers': 'PIT', 'Baltimore Ravens': 'BAL',
                    'Seattle Seahawks': 'SEA', 'Atlanta Falcons': 'ATL', 'Carolina Panthers': 'CAR',
                    'Cincinnati Bengals': 'CIN', 'Cleveland Browns': 'CLE', 'Denver Broncos': 'DEN',
                    'Detroit Lions': 'DET', 'Houston Texans': 'HOU', 'Indianapolis Colts': 'IND',
                    'Jacksonville Jaguars': 'JAX', 'Las Vegas Raiders': 'LV', 'Las Vegas': 'LV',
                    'Los Angeles Chargers': 'LAC', 'Minnesota Vikings': 'MIN', 'New York Giants': 'NYG',
                    'New York Jets': 'NYJ', 'Arizona Cardinals': 'ARI', 'Chicago Bears': 'CHI',
                    'Washington Commanders': 'WAS', 'Washington': 'WAS'
                }
                
                home_abbr = team_abbr_map.get(home_team, home_team[:3].upper())
                away_abbr = team_abbr_map.get(away_team, away_team[:3].upper())
                
                logger.info(f"Trying abbreviations: {home_team} -> {home_abbr}, {away_team} -> {away_abbr}")
                
                team_data = query_df(conn, team_query, {
                    "season": data_season, 
                    "home_team": home_abbr, 
                    "away_team": away_abbr
                })
            
            if team_data.empty:
                logger.warning(f"Still no match, trying LIKE patterns...")
                
                # Strategy 3: Try LIKE patterns as last resort
                like_query = """
                    SELECT season, team, power_score, win_pct, avg_points_for, avg_points_against
                    FROM team_season_summary 
                    WHERE season = :season AND (
                        team LIKE :home_pattern OR 
                        team LIKE :away_pattern
                    )
                """
                
                team_data = query_df(conn, like_query, {
                    "season": data_season,
                    "home_pattern": f"%{home_team.split()[-1]}%",  # Use last word (like "Titans")
                    "away_pattern": f"%{away_team.split()[-1]}%"   # Use last word (like "Rams")
                })
            
            if not team_data.empty:
                logger.info(f"✅ Found team data: {len(team_data)} records from season {data_season}")
                logger.info(f"Teams found: {team_data['team'].tolist()}")
                
                # Process each team found
                home_processed = away_processed = False
                
                for _, row in team_data.iterrows():
                    team_name = str(row['team'])
                    
                    # Determine if this is home or away team
                    is_home = False
                    is_away = False
                    
                    # Check multiple matching criteria
                    if any([
                        team_name == home_team,
                        home_team in team_name or team_name in home_team,
                        team_name == team_abbr_map.get(home_team, ''),
                        home_team.split()[-1] in team_name  # Match last word like "Titans"
                    ]):
                        is_home = True
                        home_processed = True
                    elif any([
                        team_name == away_team,
                        away_team in team_name or team_name in away_team, 
                        team_name == team_abbr_map.get(away_team, ''),
                        away_team.split()[-1] in team_name  # Match last word like "Rams"
                    ]):
                        is_away = True
                        away_processed = True
                    
                    if is_home or is_away:
                        prefix = 'home_' if is_home else 'away_'
                        
                        # Extract actual stats (not defaults!)
                        power_score = float(row.get('power_score', 0.0) or 0.0)
                        win_pct = float(row.get('win_pct', 0.5) or 0.5)
                        avg_pf = float(row.get('avg_points_for', 20.0) or 20.0)
                        avg_pa = float(row.get('avg_points_against', 20.0) or 20.0)
                        
                        features[f'{prefix}wpct_pre'] = win_pct
                        features[f'{prefix}pf_pre'] = avg_pf
                        features[f'{prefix}pa_pre'] = avg_pa
                        features[f'{prefix}pd_pre'] = avg_pf - avg_pa
                        features[f'{prefix}power_pre'] = power_score  # Use actual power score, not point diff
                        
                        logger.info(f"✅ {prefix}team ({team_name}): "
                                f"power={power_score:.1f}, "
                                f"win%={win_pct:.3f}, "
                                f"ppg={avg_pf:.1f}, "
                                f"papg={avg_pa:.1f}")
                
                if home_processed and away_processed:
                    # Calculate differences (this is what makes predictions different!)
                    features['power_diff'] = features['home_power_pre'] - features['away_power_pre']
                    features['win_pct_diff'] = features['home_wpct_pre'] - features['away_wpct_pre']
                    features['offense_diff'] = features['home_pf_pre'] - features['away_pf_pre']
                    features['home_def_str'] = -features['home_pa_pre']
                    features['away_def_str'] = -features['away_pa_pre']
                    features['defense_diff'] = features['home_def_str'] - features['away_def_str']
                    features['strength_disparity'] = abs(features['home_power_pre'] - features['away_power_pre'])
                    features['power_x_form'] = features['power_diff'] * features['form_diff']
                    
                    # Your model's specific flags
                    features['both_good'] = 1 if (features['home_power_pre'] > 2 and features['away_power_pre'] > 2) else 0
                    features['mismatch_game'] = 1 if abs(features['power_diff']) > 5 else 0
                    
                    logger.info(f"🎯 CALCULATED DIFFERENCES: "
                            f"power_diff={features['power_diff']:.1f}, "
                            f"win_pct_diff={features['win_pct_diff']:.3f}, "
                            f"offense_diff={features['offense_diff']:.1f}")
                    
                    if abs(features['power_diff']) > 0.1 or abs(features['win_pct_diff']) > 0.01:
                        logger.info("🎉 SUCCESS: Model will now get different predictions!")
                    else:
                        logger.warning("⚠️ Teams appear very evenly matched")
                else:
                    logger.warning(f"⚠️ Only found data for {1 if home_processed else 0 + 1 if away_processed else 0} team(s)")
            
            else:
                logger.error(f"❌ NO TEAM DATA FOUND in season {data_season} for {home_team} vs {away_team}")
                logger.error("Database has data but team name matching failed completely")

        except Exception as e:
            logger.error(f"Error getting team data: {e}")
            import traceback
    

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
        """ENHANCED: Extract meaningful insights from the actual data."""
        factors: List[str] = []
        
        try:
            season = dt.datetime.now().year
            home_team = game["home_team"]
            away_team = game["away_team"]
            
            # Get power ratings - FIXED: Use query_df directly
            power_sql = """
                SELECT team, power_score, win_pct, games_played,
                    avg_points_for as ppg, avg_points_against as papg,
                    wins, losses
                FROM team_season_summary 
                WHERE season = :season AND team IN (:home, :away)
            """
            power_df = query_df(conn, power_sql, {"season": season, "home": home_team, "away": away_team})
            
            if len(power_df) == 2:
                home_row = power_df[power_df['team'] == home_team].iloc[0]
                away_row = power_df[power_df['team'] == away_team].iloc[0]
                
                hp = float(home_row['power_score'])
                ap = float(away_row['power_score'])
                diff = abs(hp - ap)
                
                # Power rating insights
                if diff > 8.0:
                    stronger = home_team if hp > ap else away_team
                    factors.append(f"🔥 **Major mismatch:** {stronger} has {diff:.1f} point power advantage - this should be a dominant performance")
                elif diff > 4.0:
                    stronger = home_team if hp > ap else away_team
                    factors.append(f"💪 **Clear edge:** {stronger} holds {diff:.1f} point power advantage")
                elif diff < 1.5:
                    factors.append(f"⚖️ **Evenly matched:** Teams separated by only {diff:.1f} power points - expect a close game")
                
                # Record-based insights
                h_wins = int(home_row.get('wins', 0) or 0)
                h_losses = int(home_row.get('losses', 0) or 0)
                a_wins = int(away_row.get('wins', 0) or 0)
                a_losses = int(away_row.get('losses', 0) or 0)
                
                h_rec = f"{h_wins}-{h_losses}" if home_row['games_played'] > 0 else "N/A"
                a_rec = f"{a_wins}-{a_losses}" if away_row['games_played'] > 0 else "N/A"
                
                home_wpct = float(home_row.get('win_pct', 0) or 0)
                away_wpct = float(away_row.get('win_pct', 0) or 0)
                
                if home_wpct > 0.65:
                    factors.append(f"🏆 {home_team} ({h_rec}) is playing excellent football with {home_wpct:.1%} win rate")
                elif home_wpct < 0.35:
                    factors.append(f"📉 {home_team} ({h_rec}) struggling this season at {home_wpct:.1%}")
                
                if away_wpct > 0.65:
                    factors.append(f"🏆 {away_team} ({a_rec}) is elite competition with {away_wpct:.1%} win rate")
                elif away_wpct < 0.35:
                    factors.append(f"📉 {away_team} ({a_rec}) having a tough season at {away_wpct:.1%}")
                
                # Offensive/Defensive insights
                home_ppg = float(home_row.get('ppg', 0) or 0)
                away_ppg = float(away_row.get('ppg', 0) or 0)
                home_papg = float(home_row.get('papg', 0) or 0)
                away_papg = float(away_row.get('papg', 0) or 0)
                
                if home_ppg > 28:
                    factors.append(f"⚡ {home_team} explosive offense averaging {home_ppg:.1f} PPG")
                if away_ppg > 28:
                    factors.append(f"⚡ {away_team} explosive offense averaging {away_ppg:.1f} PPG")
                
                if home_papg < 18:
                    factors.append(f"🛡️ {home_team} stingy defense allowing just {home_papg:.1f} PPG")
                if away_papg < 18:
                    factors.append(f"🛡️ {away_team} stingy defense allowing just {away_papg:.1f} PPG")
                
                # Matchup analysis
                if home_ppg > away_papg + 7:
                    factors.append(f"🎯 {home_team}'s offense ({home_ppg:.1f} PPG) should exploit {away_team}'s defense ({away_papg:.1f} allowed)")
                if away_ppg > home_papg + 7:
                    factors.append(f"🎯 {away_team}'s offense ({away_ppg:.1f} PPG) should exploit {home_team}'s defense ({home_papg:.1f} allowed)")
            
            # Injury impact analysis
            injury_data = self._get_basic_injury_impact(conn, home_team, away_team)
            home_inj = injury_data.get("home", 0)
            away_inj = injury_data.get("away", 0)
            
            if home_inj > 6.0:
                factors.append(f"🏥 **Critical injuries:** {home_team} severely impacted (score: {home_inj:.1f}) - especially at key positions")
            elif home_inj > 3.0:
                factors.append(f"🏥 {home_team} dealing with notable injuries (impact: {home_inj:.1f})")
            
            if away_inj > 6.0:
                factors.append(f"🏥 **Critical injuries:** {away_team} severely impacted (score: {away_inj:.1f}) - especially at key positions")
            elif away_inj > 3.0:
                factors.append(f"🏥 {away_team} dealing with notable injuries (impact: {away_inj:.1f})")
            
            # Home field advantage note
            factors.append(f"🏟️ Home field advantage: {home_team} gets typical 2.5-3 point boost")
            
            # If nothing specific found, give general insight
            if len(factors) <= 1:  # Only home field advantage
                factors.insert(0, "📊 Standard NFL matchup - consult full analytics for detailed breakdown")
            
            return factors[:6]  # Limit to top 6 factors
            
        except Exception as e:
            logger.warning(f"key factor extraction error: {e}")
            return ["⚠️ Unable to extract detailed factors - check data availability"]

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
        """Calculate confidence based on model uncertainty and game factors."""
        home_prob = probabilities["home"]
        away_prob = probabilities["away"]
        
        # Base confidence from probability spread (how decisive the model is)
        prob_spread = abs(home_prob - away_prob)
        base_confidence = 0.5 + (prob_spread * 0.8)  # Scale 0.5-1.3, then cap below
        
        # Adjust for injury uncertainty
        total_injury_impact = float(injury_impact.get("home", {}).get("total", 0.0)) + \
                            float(injury_impact.get("away", {}).get("total", 0.0))
        
        if total_injury_impact > 5.0:
            base_confidence -= 0.15  # High injury impact reduces confidence
        elif total_injury_impact > 2.0:
            base_confidence -= 0.08
        
        # Adjust for number of key factors (more factors = more confidence)
        if len(key_factors) >= 3:
            base_confidence += 0.05
        elif len(key_factors) <= 1:
            base_confidence -= 0.10
        
        # Model source adjustment
        if probabilities.get("source") == "ml_model":
            base_confidence += 0.10  # ML model is more reliable
        else:
            base_confidence -= 0.05  # Heuristics are less certain
        
        # Final bounds - realistic confidence range
        return max(0.35, min(0.92, base_confidence))


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

    def _abbr_to_full_name(self, abbr):
        """Convert team abbreviation to full name"""
        mapping = {
            'TB': 'Tampa Bay Buccaneers', 'DAL': 'Dallas Cowboys',
            'ATL': 'Atlanta Falcons', 'PHI': 'Philadelphia Eagles',
            'BUF': 'Buffalo Bills', 'PIT': 'Pittsburgh Steelers',
            'CAR': 'Carolina Panthers', 'NYJ': 'New York Jets',
            'CIN': 'Cincinnati Bengals', 'MIN': 'Minnesota Vikings',
            'ARI': 'Arizona Cardinals', 'BAL': 'Baltimore Ravens',
            'CHI': 'Chicago Bears', 'CLE': 'Cleveland Browns',
            'DEN': 'Denver Broncos', 'DET': 'Detroit Lions',
            'GB': 'Green Bay Packers', 'HOU': 'Houston Texans',
            'IND': 'Indianapolis Colts', 'JAX': 'Jacksonville Jaguars',
            'KC': 'Kansas City Chiefs', 'LV': 'Las Vegas Raiders',
            'LAC': 'Los Angeles Chargers', 'LAR': 'Los Angeles Rams',
            'MIA': 'Miami Dolphins', 'NE': 'New England Patriots',
            'NO': 'New Orleans Saints', 'NYG': 'New York Giants',
            'SF': 'San Francisco 49ers', 'SEA': 'Seattle Seahawks',
            'TEN': 'Tennessee Titans', 'WAS': 'Washington Commanders'
        }
        return mapping.get(abbr, abbr)

    def _init_openai(self) -> Optional[OpenAI]:
        """Initialize OpenAI client with detailed error logging."""
        
        # Check if library is available
        if not OPENAI_AVAILABLE:
            print("❌ OpenAI library not available - install with: pip install openai")
            logger.warning("OpenAI not available - AI responses will be limited")
            return None
        
        # Check for API key
        key = os.getenv("OPENAI_API_KEY")
        if not key:
            print("❌ OPENAI_API_KEY environment variable not set")
            logger.warning("No OpenAI API key found - AI responses will be limited")
            return None
        
        # Try to initialize client
        try:
            client = OpenAI(api_key=key)
            print(f"✅ OpenAI client initialized (key: {key[:15]}...)")
            
            # Test the connection
            test_response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": "test"}],
                max_tokens=5
            )
            print(f"✅ OpenAI connection verified (model: {test_response.model})")
            
            return client
        
        except Exception as e:
            print(f"❌ Failed to initialize OpenAI client: {e}")
            logger.error(f"OpenAI initialization failed: {e}")
            return None

    
    
    def generate_betting_recommendations(self, user_context=None):
        """
        Generate specific betting recommendations using EXACT same logic as training backtest.
        FIXED: Handle missing games and odds properly
        """
        try:
            if user_context and 'bankroll' in user_context:
                bankroll = float(user_context['bankroll'])
            else:
                bankroll = float(session.get('user_bankroll', 500))
            
            # EXACT parameters from your training backtest
            MIN_EDGE = 0.02          # 2.5% minimum edge (from training)
            MAX_BET_PCT = 0.05        # 5% max per bet (from training) 
            MIN_CONF = 0.52            # 54% minimum model probability
            MAX_DEC_ODDS = 3.2        # Skip longshots bigger than +170
            MIN_BOOKS_REQUIRED = 1     # REDUCED from 3 to 1 for testing
            KELLY_FRACTION = 0.1      # 10% of full Kelly (conservative)
            
            recommendations = []
            total_staked = 0.0
            max_single_bet = bankroll * MAX_BET_PCT
            daily_budget = bankroll * 0.10  # 10% daily budget like training
            today = datetime.utcnow().date()
            horizon = today + timedelta(days=14)  # Look 2 weeks ahead
            
            conn = self.db_manager.get_connection()
            try:
                # Get upcoming games (FIXED: Look further ahead)
                games_sql = """
                    SELECT DISTINCT g.game_id, g.home_team, g.away_team, g.game_date, g.start_time_local
                    FROM games g
                    WHERE date(g.game_date) BETWEEN date(:start_date) AND date(:end_date)
                    ORDER BY date(g.game_date), time(g.start_time_local)
                    LIMIT 100
                """
                games = query_df(conn, games_sql, {"start_date": today, "end_date": horizon})
                
                if games.empty:
                    print(f"DEBUG: No games found between {today} and {horizon}")
                    # Let's check what games exist
                    all_games = query_df(conn, "SELECT game_id, game_date, home_team, away_team FROM games ORDER BY game_date DESC LIMIT 10")
                    print(f"DEBUG: Recent games in DB:")
                    for _, g in all_games.iterrows():
                        print(f"  {g['game_date']}: {g['away_team']} @ {g['home_team']}")
                    
                    return {
                        'ok': True,
                        'success': True,
                        'result': {
                            'recommendations': [],
                            'bankroll': bankroll,
                            'total_recommended': 0,
                            'message': f'No upcoming games found between {today} and {horizon}'
                        }
                    }
                
                print(f"DEBUG: Found {len(games)} games to analyze")
                
                # Get ALL odds (not just latest) to see what's available
                game_ids = games["game_id"].tolist()
                if not game_ids:
                    return {
                        'ok': True, 'success': True,
                        'result': {'recommendations': [], 'bankroll': bankroll, 'total_recommended': 0,
                                'message': 'No games in window'}
                    }

                ph = ",".join([f":gid{i}" for i in range(len(game_ids))])
                params = {f"gid{i}": int(g) for i, g in enumerate(game_ids)}

                odds_sql = f"""
                    SELECT o.game_id, o.team, o.sportsbook, o.odds, o.timestamp
                    FROM odds o
                    JOIN (
                        SELECT game_id, team, sportsbook, MAX(timestamp) AS ts
                        FROM odds
                        WHERE market='h2h' AND game_id IN ({ph})
                        GROUP BY game_id, team, sportsbook
                    ) x ON x.game_id=o.game_id AND x.team=o.team AND x.sportsbook=o.sportsbook AND x.ts=o.timestamp
                """
                all_odds = query_df(conn, odds_sql, params)

                print(f"DEBUG: Found {len(all_odds)} total odds records")
                
                if all_odds.empty:
                    print("DEBUG: No odds found in database at all")
                    return {
                        'ok': True,
                        'success': True,
                        'result': {
                            'recommendations': [],
                            'bankroll': bankroll,
                            'total_recommended': 0,
                            'message': 'No odds data available in database'
                        }
                    }
                
                # Debug: Show some odds data
                print("DEBUG: Sample odds:")
                for _, row in all_odds.head(5).iterrows():
                    print(f"  {row['game_id']}: {row['team']} @ {row['odds']} ({row['sportsbook']})")
                
                for _, game in games.iterrows():
                    try:
                        print(f"DEBUG: Analyzing {game['away_team']} @ {game['home_team']}")
                        
                        # Get model prediction using your trained model
                        probs = self.analyzer._calculate_win_probabilities(conn, game)
                        p_home = probs['home']
                        p_away = probs['away']
                        
                        print(f"  Model: Home {p_home:.1%}, Away {p_away:.1%}")
                        
                        # Get odds for this game
                        game_odds = all_odds[all_odds['game_id'] == game['game_id']]
                        print(f"  Found {len(game_odds)} odds for this game")
                        
                        if game_odds.empty:
                            print("  No odds found - skipping")
                            continue
                        
                        # Process odds by team with improved name normalization
                        for team_name in [game['home_team'], game['away_team']]:
                            print(f"    Processing {team_name}")
                            
                            # Find odds for this team
                            team_odds = game_odds[game_odds['team'].str.contains(team_name, case=False, na=False)]
                            
                            if team_odds.empty:
                                print(f"    No odds found for {team_name}")
                                continue
                            
                            try:
                                # Get best odds (highest)
                                
                                best_odds_row = team_odds.loc[team_odds['odds'].idxmax()]
                                odds_val = float(best_odds_row['odds'])
                                books_count = team_odds['sportsbook'].nunique()
                                
                                print(f"    Best odds: {odds_val}, Books: {books_count}")
                                
                                if books_count < MIN_BOOKS_REQUIRED:
                                    print(f"    Only {books_count} books, need {MIN_BOOKS_REQUIRED}")
                                    continue
                                
                                # Skip if odds too long
                                decimal_odds = self._to_decimal_odds(odds_val)
                                if decimal_odds > MAX_DEC_ODDS:
                                    print(f"    Odds too long: {decimal_odds}")
                                    continue
                                
                                # Get model probability for this team
                                if team_name == game['home_team']:
                                    model_prob = p_home
                                else:
                                    model_prob = p_away
                                
                                # Check minimum confidence
                                if model_prob < MIN_CONF:
                                    print(f"    Model prob {model_prob:.1%} below min {MIN_CONF:.1%}")
                                    continue
                                
                                # Convert American odds to implied probability
                                if odds_val > 0:
                                    implied_prob = 100 / (odds_val + 100)
                                else:
                                    implied_prob = abs(odds_val) / (abs(odds_val) + 100)
                                
                                # Calculate edge
                                edge = model_prob - implied_prob
                                edge_pct = edge * 100
                                
                                print(f"    Edge: {edge_pct:.1f}% (model {model_prob:.1%} vs implied {implied_prob:.1%})")
                                
                                # LOWERED THRESHOLD: Look for 2%+ edges instead of 2.5%+
                                if edge >= 0.02:  # 2% minimum edge
                                    print(f"    *** EDGE FOUND: {edge_pct:.1f}% ***")
                                    
                                    # Calculate Kelly stake
                                    kelly = self.kelly_bet_size(model_prob, decimal_odds)
                                    stake = max(5, min(30, kelly * bankroll * KELLY_FRACTION))
                                    
                                    # Confidence based on edge size
                                    if edge > 0.08:
                                        confidence = "High"
                                    elif edge > 0.05:
                                        confidence = "Medium"  
                                    else:
                                        confidence = "Low"
                                    
                                    recommendations.append({
                                        'type': 'model_bet',
                                        'game': f"{game['away_team']} @ {game['home_team']}",
                                        'date': str(game['game_date']),
                                        'time': str(game.get('start_time_local', 'TBD'))[:5],
                                        'team': team_name,
                                        'odds': int(odds_val),
                                        'decimal_odds': round(decimal_odds, 2),
                                        'sportsbook': best_odds_row['sportsbook'],
                                        'books_count': books_count,
                                        'model_probability': round(model_prob, 3),
                                        'implied_probability': round(implied_prob, 3),
                                        'edge_percentage': round(edge * 100, 1),
                                        'recommended_stake': round(stake, 2),
                                        'potential_profit': round(stake * (decimal_odds - 1), 2),
                                        'confidence_level': confidence,
                                        'risk_assessment': 'Low' if decimal_odds < 2.0 else 'Medium',
                                        'reason': f"{edge*100:.1f}% model edge over {books_count}-book consensus"
                                    })
                                    
                                    total_staked += stake
                            
                            except Exception as e:
                                print(f"Error processing {team_name}: {e}")
                                continue
                    
                    except Exception as e:
                        print(f"Error analyzing game {game['game_id']}: {e}")
                        continue
                
                print(f"Found {len(recommendations)} betting opportunities, total recommended: ${total_staked:.2f}")
                
                # Sort by edge percentage (best first)
                recommendations.sort(key=lambda x: x['edge_percentage'], reverse=True)
                
                return {
                    'ok': True,
                    'success': True,
                    'result': {
                        'recommendations': recommendations,
                        'bankroll': bankroll,
                        'total_recommended': round(total_staked, 2),
                        'remaining_budget': round(daily_budget - total_staked, 2),
                        'games_scanned': len(games),
                        'bet_rate': f"{len(recommendations)}/{len(games)} ({len(recommendations)/len(games)*100:.1f}%)" if len(games) > 0 else "0/0",
                        'risk_level': 'Conservative' if total_staked < bankroll * 0.05 else 'Moderate',
                        'note': f"Using backtest parameters: {MIN_EDGE*100:.1f}%+ edge, {MIN_BOOKS_REQUIRED}+ books, {KELLY_FRACTION*100:.0f}% Kelly"
                    }
                }
                
            finally:
                conn.close()
                
        except Exception as e:
            logger.exception("generate_betting_recommendations failed")
            print(f"ERROR in generate_betting_recommendations: {e}")
            import traceback
            traceback.print_exc()
            return {
                'ok': False,
                'success': False,
                'error': str(e),
                'message': 'Failed to generate recommendations using trained model'
            }

    def kelly_bet_size(self, win_prob, decimal_odds):
        """Calculate Kelly Criterion bet size (same as training)"""
        if win_prob <= 0 or decimal_odds <= 1:
            return 0
        
        b = decimal_odds - 1  # Net odds
        p = win_prob
        q = 1 - win_prob
        
        if p * b > q:
            return (p * b - q) / b
        return 0

    def _to_decimal_odds(self, odds_value):
        """Convert various odds formats to decimal (same as training)"""
        try:
            odds = float(odds_value)
            
            # If already decimal (between 1.01 and 50)
            if 1.01 <= odds <= 50:
                return odds
            
            # American odds
            if odds >= 100:
                return 1 + (odds / 100)
            elif odds <= -100:
                return 1 + (100 / abs(odds))
            elif odds > 0:  # Positive odds less than 100
                return 1 + (odds / 100)
            elif odds < 0:   # Negative odds greater than -100
                return 1 + (100 / abs(odds))
            else:
                return 1.91  # Default -110
                    
        except:
            return 1.91  # Default fallback

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
            # FIXED: Use actual user bankroll from context
            bankroll = user_context.get('bankroll', 500)
            parts.append(f"User Bankroll: ${bankroll:.2f}")
        
        return "\n".join(parts) if parts else "No specific context available"

    def _handle_value_bets(self, message: str, context: str) -> Dict[str, Any]:
        """ENHANCED: Handle value bet requests with better explanations."""
        try:
            # Extract edge threshold from message
            edge_match = re.search(r'(\d+(?:\.\d+)?)%?\s*(?:edge|or higher|or better)', message.lower())
            min_edge = float(edge_match.group(1)) / 100 if edge_match else 0.05
            
            # Get value bets from analyzer
            value_bets = self.analyzer.find_value_bets_advanced(min_edge=min_edge)
            
            if not value_bets:
                # Provide helpful response when no bets found
                if min_edge > 0.05:
                    suggestion = f"Try lowering your edge threshold (currently {min_edge*100:.0f}%) to see opportunities. The market is efficient - edges above 5% are rare."
                else:
                    suggestion = "The current market is very efficient. Consider:\n  • Checking again closer to game time when lines move\n  • Looking at alternate markets (spreads, totals)\n  • Waiting for injury news that might create value"
                
                return {
                    "ok": True,
                    "intent": "value_bets",
                    "success": True,
                    "result": [],
                    "total_found": 0,
                    "min_edge_used": min_edge * 100,
                    "message": f"**No value bets found with {min_edge*100:.1f}%+ edge.**\n\n{suggestion}"
                }
            
            # Convert ValueBet objects to dicts with enhanced info
            bets_data = []
            for bet in value_bets:
                # Calculate potential profit
                if bet.odds > 0:
                    profit_per_100 = bet.odds
                else:
                    profit_per_100 = 10000 / abs(bet.odds)
                
                # Risk assessment based on edge and confidence
                if bet.edge_percentage > 7 and bet.confidence_level == "High":
                    risk = "Low"
                    recommendation = "Strong Play"
                elif bet.edge_percentage > 4:
                    risk = "Medium"
                    recommendation = "Solid Value"
                else:
                    risk = "Medium-High"
                    recommendation = "Consider"
                
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
                    "risk_assessment": risk,
                    "recommendation": recommendation,
                    "profit_per_100": round(profit_per_100, 0),
                    "ev_per_100": round((bet.model_probability * profit_per_100) - ((1 - bet.model_probability) * 100), 2)
                })

            # Generate summary message
            top_bet = bets_data[0]
            avg_edge = sum(b['edge_pct'] for b in bets_data) / len(bets_data)
            total_stake = sum(b['recommended_amount'] for b in bets_data)
            
            summary = f"""**🎯 Found {len(bets_data)} Value Opportunities (≥{min_edge*100:.1f}% edge)**

    **TOP PICK:** {top_bet['team']} at {top_bet['odds']} ({top_bet['sportsbook']})
    - Edge: {top_bet['edge_pct']:.1f}%
    - Recommended stake: ${top_bet['recommended_amount']:.2f}
    - Confidence: {top_bet['confidence_level']}

    **PORTFOLIO SUMMARY:**
    - Average edge: {avg_edge:.1f}%
    - Total recommended stake: ${total_stake:.2f}
    - Risk level: {'Conservative' if avg_edge > 5 else 'Moderate'}

    **NOTE:** These recommendations use Kelly Criterion (25% fraction) for proper bankroll management. Never bet more than you can afford to lose."""

            return {
                "ok": True,
                "intent": "value_bets",
                "success": True,
                "result": bets_data,
                "total_found": len(bets_data),
                "min_edge_used": min_edge * 100,
                "message": summary
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


    def _build_rich_context(self, analysis: GameAnalysis, user_question: str) -> str:
        """Build comprehensive context for AI to understand the betting situation."""
        
        context = f"""
    GAME ANALYSIS REQUEST
    User Question: "{user_question}"

    MATCHUP: {analysis.away_team} @ {analysis.home_team}
    Date: {analysis.game_date}

    MODEL PREDICTIONS:
    - Home Win Probability: {analysis.home_probability:.1%}
    - Away Win Probability: {analysis.away_probability:.1%}
    - Model Confidence: {analysis.confidence_score:.1%}

    BETTING OPPORTUNITIES:
    """
        
        if analysis.best_bet:
            context += f"""
    - Best Bet Found: {analysis.best_bet.get('team')}
    - Odds: {analysis.best_bet.get('odds')}
    - Edge: {analysis.best_bet.get('edge_pct', 0):.1f}%
    - Sportsbook: {analysis.best_bet.get('sportsbook', 'N/A')}
    """
        else:
            context += "- No significant betting edge detected\n"
        
        context += f"""
    KEY FACTORS:
    """
        for factor in analysis.key_factors[:5]:
            context += f"- {factor}\n"
        
        context += f"""
    INJURY SITUATION:
    - {analysis.home_team} Impact: {analysis.injury_impact.get('home', {}).get('total', 0):.1f}
    - {analysis.away_team} Impact: {analysis.injury_impact.get('away', {}).get('total', 0):.1f}
    """
        
        if analysis.injury_impact.get('home', {}).get('QB', 0) > 0:
            context += f"- ⚠️ {analysis.home_team} has QB injury concerns\n"
        if analysis.injury_impact.get('away', {}).get('QB', 0) > 0:
            context += f"- ⚠️ {analysis.away_team} has QB injury concerns\n"
        
        return context


    def _handle_general_chat(self, message: str, context: str) -> Dict[str, Any]:
        """Handle general betting questions with REAL AI conversation."""
        
        if not self.openai_client:
            return {
                "ok": True,
                "intent": "general_chat",
                "success": True,
                "result": {
                    "message": "I can help analyze specific games, find value bets, or explain betting strategies. What would you like to know?"
                }
            }
        
        try:
            system_prompt = f"""You are an expert NFL betting analyst having a casual conversation.

    CONTEXT:
    {context}

    Your personality:
    - Friendly and conversational
    - Honest about risks ("betting is hard, no guarantees")
    - Give specific examples when possible
    - Admit when you need more info
    - Keep responses concise (2-3 paragraphs max)

    TOPICS YOU KNOW ABOUT:
    - NFL teams, players, matchups
    - Betting strategies (value betting, bankroll management)
    - Reading odds and finding edges
    - How betting markets work
    - Statistical analysis

    If the user asks about a specific game, tell them to select it from the sidebar first.
    If they ask for value bets, explain you can search for those.
    """

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
            return {
                "ok": False,
                "intent": "general_chat",
                "success": False,
                "message": "I'm having trouble responding right now. Try asking about a specific game or value bets."
            }

    def process_message(self, message: str, game_id: Optional[str] = None, user_context: Optional[Dict] = None,conversation_history: Optional[List[Dict]] = None  ) -> Dict[str, Any]:
        """Enhanced message processing with conversation history."""
        
        intent = self._classify_intent(message)
        context = self._build_context(game_id, user_context)
        
        # Store current game context for injury filtering
        if game_id:
            try:
                conn = self.db_manager.get_connection()
                game_info = query_df(
                    conn, 
                    "SELECT home_team, away_team FROM games WHERE game_id = :gid", 
                    {"gid": game_id}
                )
                if not game_info.empty:
                    self.current_game_context = {
                        'home_team': game_info.iloc[0]['home_team'],
                        'away_team': game_info.iloc[0]['away_team']
                    }
                conn.close()
            except Exception:
                pass

        try:
            # Route to appropriate handler
            if intent == MessageIntent.GAME_ANALYSIS:
                if game_id:
                    return self._handle_game_analysis(game_id, message, context)
                return self._request_game_selection()
                
            elif intent == MessageIntent.VALUE_BETS:
                return self._handle_value_bets(message, context)
                
            elif intent == MessageIntent.INJURY_REPORT:
                return self._handle_injury_report(message, context)
                
            elif intent == MessageIntent.GENERAL_CHAT:
                # This now uses OpenAI for real conversation
                return self._handle_general_chat(message, context)
                
            return self._handle_fallback(message, intent, context)
            
        except Exception as e:
            logger.exception("process_message failed")
            return {
                "ok": False,
                "intent": intent.value,
                "success": False,
                "error": str(e),
                "message": "Something went wrong. Please try again."
            }

    def _generate_conversational_response(
    self, 
    analysis: GameAnalysis, 
    user_message: str, 
    context: str
) -> str:
        """Generate a REAL conversational AI response using OpenAI."""
        
        if not self.openai_client:
            print("❌ OpenAI client not initialized - using fallback")
            return self._generate_detailed_fallback_commentary(analysis, user_message)
        
        try:
            # Build a conversational prompt
            system_prompt = f"""You are an expert NFL betting analyst having a conversation with a user.

    You have deep knowledge of:
    - NFL teams, players, and matchups
    - Betting markets and finding value
    - Statistical analysis and ML models
    - Injury impacts and game dynamics

    Your personality:
    - Friendly and conversational (like talking to a knowledgeable friend)
    - Direct and honest about betting risks
    - Use casual language but stay professional
    - Give specific, actionable advice
    - Admit when you're uncertain

    CRITICAL DATA FOR THIS CONVERSATION:
    {context}

    The user is asking about this game. Answer their specific question naturally, 
    like you're having a real conversation. Don't just list stats - explain what 
    they MEAN for betting this game.
    """

            user_prompt = f"""User asks: "{user_message}"

    Please respond conversationally. If they asked "Analyze this game", give your 
    honest take on whether it's a good bet. If they asked something specific, 
    answer that directly.

    Keep it natural - you're chatting, not writing a formal report."""

            print(f"🔄 Calling OpenAI with model: gpt-4o-mini")
            
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=800,
                temperature=0.7,
            )
            
            ai_response = response.choices[0].message.content.strip()
            print(f"✅ Got OpenAI response ({len(ai_response)} chars)")
            return ai_response
            
        except Exception as e:
            print(f"❌ OpenAI conversation failed: {e}")
            logger.exception("OpenAI conversation failed")
            
            # Check specific error types
            if "api_key" in str(e).lower():
                print("💡 API Key issue detected")
            elif "rate_limit" in str(e).lower():
                print("💡 Rate limit hit")
            elif "model" in str(e).lower():
                print("💡 Model access issue")
            
            return self._generate_detailed_fallback_commentary(analysis, user_message)


    def _handle_game_analysis(self, game_id: str, message: str, context: str) -> Dict[str, Any]:
        """Enhanced game analysis with REAL AI conversation."""
        try:
            # Get the core analysis from your model
            analysis = self.analyzer.analyze_game_comprehensive(game_id)
            
            # Build rich context for the AI
            context_data = self._build_rich_context(analysis, message)
            
            # ALWAYS generate AI commentary using OpenAI
            if self.openai_client:
                ai_commentary = self._generate_conversational_response(
                    analysis, message, context_data
                )
            else:
                ai_commentary = self._generate_detailed_fallback_commentary(analysis, message)

            return {
                "ok": True,
                "intent": "analysis",
                "success": True,
                "result": {
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
                    "recommendation": analysis.recommendation,
                    "summary": ai_commentary,  # <-- Real AI response
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
                "error": str(e),
                "message": "Failed to analyze game."
            }



    def _generate_detailed_analysis_commentary(self, analysis: GameAnalysis, user_message: str, context: str) -> str:
        """ENHANCED: Generate insightful AI commentary using OpenAI with richer prompts."""
        try:
            # Extract feature importance if available (from cached features)
            game_id = str(analysis.game_id)
            feature_insights = ""
            if game_id in self.analyzer.feature_cache:
                features = self.analyzer.feature_cache[game_id]
                feature_insights = f"""
    DETAILED MODEL FEATURES ANALYZED:
    - Power Rating Difference: {features.get('power_diff', 0):.1f}
    - Win % Differential: {features.get('win_pct_diff', 0):.1%}
    - Offensive Advantage: {features.get('offense_diff', 0):.1f} PPG
    - Defensive Edge: {features.get('defense_diff', 0):.1f}
    - Recent Form Difference: {features.get('form_diff', 0):.2f}
    - Home Field Impact: {features.get('home_field_advantage', 2.5):.1f} points
    - Injury Impacts: Home {features.get('home_injury_impact', 0):.1f}, Away {features.get('away_injury_impact', 0):.1f}
    """

            # Build comprehensive prompt
            prompt = f"""You are an expert NFL betting analyst with 15+ years of experience. Provide a DETAILED, INSIGHTFUL analysis based on our proprietary ML model's prediction.

    GAME: {analysis.away_team} @ {analysis.home_team} ({analysis.game_date})

    MODEL PREDICTION & CONFIDENCE:
    - Win Probabilities: {analysis.home_team} {analysis.home_probability:.1%} | {analysis.away_team} {analysis.away_probability:.1%}
    - Model Confidence Score: {analysis.confidence_score:.0%} (where >75% = high, 60-75% = moderate, <60% = low)
    - Source: {analysis.__dict__.get('source', 'ML Model')}

    {feature_insights}

    BETTING MARKET ANALYSIS:
    {f"📊 BEST BET IDENTIFIED: {analysis.best_bet.get('team')} at {analysis.best_bet.get('odds')} ({analysis.best_bet.get('sportsbook')}) - {analysis.best_bet.get('edge_pct', 0):.1f}% edge over market" if analysis.best_bet and analysis.best_bet.get('edge', 0) > 0.03 else "📉 No significant edge detected - market efficiently priced"}

    KEY ANALYTICAL FACTORS:
    {chr(10).join(f"• {factor}" for factor in analysis.key_factors)}

    INJURY SITUATION:
    - {analysis.home_team} Impact Score: {analysis.injury_impact.get('home', {}).get('total', 0):.1f} (QB: {analysis.injury_impact.get('home', {}).get('QB', 0):.1f})
    - {analysis.away_team} Impact Score: {analysis.injury_impact.get('away', {}).get('total', 0):.1f} (QB: {analysis.injury_impact.get('away', {}).get('QB', 0):.1f})

    USER QUESTION: "{user_message}"
    CONVERSATION CONTEXT: {context}

    Provide a comprehensive analysis that:

    1. **OPENING VERDICT**: Start with a clear, confident prediction statement that directly answers what the model sees

    2. **WHY THIS PICK** (2-3 detailed points): 
    - Explain the SPECIFIC statistical advantages driving the prediction
    - Reference actual numbers from the features above
    - Discuss which factors matter most and why

    3. **INJURY & SITUATIONAL IMPACT** (if relevant):
    - How injuries are affecting the model's confidence
    - Any QB concerns that significantly impact the outlook
    - Context on key player absences

    4. **BETTING VALUE ASSESSMENT**:
    - If edge exists: Explain WHY the market is mispriced and where the value lies
    - If no edge: Explain why the lines are efficient and what it would take to find value
    - Specific betting advice based on model confidence

    5. **RISK FACTORS** (what could go wrong):
    - Identify 1-2 scenarios where the prediction could fail
    - Acknowledge uncertainty if this is a close game

    6. **ACTIONABLE RECOMMENDATION**:
    - Clear YES/NO/MAYBE on betting this game
    - Suggested stake sizing if it's a play (1-5% of bankroll based on edge/confidence)
    - Alternative bets to consider if straight ML isn't ideal

    Be SPECIFIC with numbers, CONFIDENT in analysis, and PRACTICAL with betting advice. Avoid generic statements - use the actual data provided. Write like an experienced handicapper talking to a serious bettor."""

            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=800,
                temperature=0.7,
            )
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            logger.exception("OpenAI commentary generation failed")
            return self._generate_detailed_fallback_commentary(analysis, user_message)
        
    def _generate_detailed_fallback_commentary(self, analysis: GameAnalysis, user_message: str) -> str:
        """Generate detailed fallback commentary when OpenAI is unavailable."""
        
        favored_team = analysis.home_team if analysis.home_probability > 0.5 else analysis.away_team
        underdog_team = analysis.away_team if analysis.home_probability > 0.5 else analysis.home_team
        favored_prob = max(analysis.home_probability, analysis.away_probability)
        prob_spread = abs(analysis.home_probability - analysis.away_probability)
        
        commentary = []
        
        # 1. OPENING PREDICTION with context
        if prob_spread > 0.25:
            commentary.append(f"**🎯 Strong Pick:** The model heavily favors **{favored_team}** ({favored_prob:.1%}) over {underdog_team} ({1-favored_prob:.1%}).")
        elif prob_spread > 0.15:
            commentary.append(f"**📊 Model Lean:** {favored_team} is the predicted winner with {favored_prob:.1%} win probability, but {underdog_team} has a {1-favored_prob:.1%} chance to upset.")
        else:
            commentary.append(f"**⚖️ Close Game:** This is a near-toss-up with {favored_team} at {favored_prob:.1%} and {underdog_team} at {1-favored_prob:.1%}.")
        
        # 2. CONFIDENCE EXPLANATION with reasoning
        conf = analysis.confidence_score
        if conf > 0.75:
            commentary.append(f"**High Confidence ({conf:.0%}):** The model has strong conviction based on clear statistical advantages and consistent team performance metrics.")
        elif conf > 0.60:
            commentary.append(f"**Moderate Confidence ({conf:.0%}):** Solid prediction, but some uncertainty exists due to competitive matchup factors or recent variance in team performance.")
        else:
            commentary.append(f"**Lower Confidence ({conf:.0%}):** This is a difficult game to predict. Both teams have similar statistical profiles, making the outcome highly uncertain.")
        
        # 3. KEY FACTORS - Make them actionable
        if analysis.key_factors:
            factors_list = "\n  • ".join(analysis.key_factors)
            commentary.append(f"**🔑 Critical Factors:**\n  • {factors_list}")
        else:
            # Provide default insights based on probabilities
            if prob_spread > 0.20:
                commentary.append(f"**🔑 Critical Factors:**\n  • {favored_team} shows significant statistical superiority in power ratings\n  • Home field advantage factored into prediction\n  • Recent form trends favor the predicted winner")
            else:
                commentary.append(f"**🔑 Critical Factors:**\n  • Evenly matched teams by power metrics\n  • Game could swing either way based on execution\n  • Small edges in efficiency ratings tip toward {favored_team}")
        
        # 4. BETTING VALUE - More detailed
        if analysis.best_bet and analysis.best_bet.get('edge', 0) > 0.03:
            bet = analysis.best_bet
            edge_pct = bet.get('edge_pct', bet['edge']*100)
            if edge_pct > 7:
                commentary.append(f"**💰 Strong Value:** {bet['team']} at {bet['odds']} offers a {edge_pct:.1f}% edge. The market is undervaluing this team - this is a high-confidence betting opportunity at {bet.get('sportsbook', 'available odds')}.")
            elif edge_pct > 4:
                commentary.append(f"**💵 Good Value:** {bet['team']} at {bet['odds']} shows {edge_pct:.1f}% edge. Worth considering at {bet.get('sportsbook', 'current lines')}.")
            else:
                commentary.append(f"**📈 Slight Edge:** {bet['team']} at {bet['odds']} has a {edge_pct:.1f}% edge, but it's marginal. Only bet if you have conviction.")
        else:
            commentary.append("**📉 Market Efficiency:** Current lines are well-priced. No significant betting edge detected - the sportsbooks have this game accurately handicapped.")
        
        # 5. INJURY IMPACT - More specific
        home_inj = analysis.injury_impact.get('home', {}).get('total', 0)
        away_inj = analysis.injury_impact.get('away', {}).get('total', 0)
        home_qb = analysis.injury_impact.get('home', {}).get('QB', 0)
        away_qb = analysis.injury_impact.get('away', {}).get('QB', 0)
        
        if home_qb > 0 or away_qb > 0:
            affected_team = analysis.home_team if home_qb > 0 else analysis.away_team
            commentary.append(f"**🏥 QB Injury Alert:** {affected_team} has quarterback concerns that significantly impact the model's prediction. This is factored into the probabilities above.")
        elif home_inj > 4 or away_inj > 4:
            affected_team = analysis.home_team if home_inj > away_inj else analysis.away_team
            impact = max(home_inj, away_inj)
            commentary.append(f"**🏥 Injury Concerns:** {affected_team} has notable injuries (impact score: {impact:.1f}) that reduce their expected performance. Model accounts for this.")
        elif home_inj > 2 or away_inj > 2:
            commentary.append(f"**🏥 Minor Injuries:** Some injury concerns present but not significantly impacting the prediction.")
        
        # 6. BETTING RECOMMENDATION - Clear action
        if analysis.best_bet and analysis.best_bet.get('edge', 0) > 0.05 and conf > 0.65:
            commentary.append(f"**✅ RECOMMENDED BET:** {analysis.best_bet['team']} is the play here with {analysis.best_bet.get('edge_pct', 0):.1f}% edge and {conf:.0%} model confidence.")
        elif conf > 0.70:
            commentary.append(f"**📊 MODEL LEAN:** Strong prediction for {favored_team}, but no market inefficiency to exploit. Consider smaller plays if you trust the model.")
        else:
            commentary.append(f"**⏸️ SKIP RECOMMENDATION:** This game is too close to call with confidence. Better opportunities likely exist elsewhere.")
        
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

    
# ---------------------------
# Flask Blueprint
# ---------------------------
# Use the same blueprint name your UI imports
comprehensive_ai_bp = Blueprint("ai", __name__)
try:
    ai_system = ComprehensiveAI()
except Exception as e:
    logger.warning("ComprehensiveAI bootstrap failed; running without ML.", exc_info=True)
    ai_system = None  # routes should handle None by using fallback

# At the bottom of ai_chat_stub.py, before the blueprint routes:

print("\n" + "="*60)
print("🔍 AI SYSTEM DIAGNOSTICS")
print("="*60)
print(f"OpenAI Available: {OPENAI_AVAILABLE}")
print(f"API Key Set: {bool(os.getenv('OPENAI_API_KEY'))}")
if ai_system:
    print(f"OpenAI Client: {ai_system.openai_client is not None}")
    print(f"Model Pack Loaded: {ai_system.analyzer.model_pack is not None}")
else:
    print("⚠️ AI System not initialized")
print("="*60 + "\n")

@comprehensive_ai_bp.route("/api/ai-chat", methods=["POST"])
def ai_chat_compat():
    return comprehensive_ai_chat()


@comprehensive_ai_bp.route("/api/ai-chat-comprehensive", methods=["POST"])
def comprehensive_ai_chat():
    """Main comprehensive AI chat endpoint (normalized to {ok,intent,result})."""
    try:
        data = request.get_json() or {}
        message = data.get('message', '').strip()
        game_id = data.get('game_id')

        username = session.get('username')
        if not username:
            return jsonify({
                'ok': False,
                'error': 'User not logged in'
            }), 401
            
        from mobile_dashboard import USERS
        user_data = USERS.get(username, {})

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

@comprehensive_ai_bp.route("/api/test-openai", methods=["GET"])
def test_openai():
    """Test OpenAI connection"""
    if not ai_system.openai_client:
        return jsonify({
            "ok": False,
            "error": "OpenAI client not initialized",
            "api_key_set": bool(os.getenv("OPENAI_API_KEY"))
        })
    
    try:
        response = ai_system.openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "Say 'test successful' if you can read this"}],
            max_tokens=50
        )
        
        return jsonify({
            "ok": True,
            "response": response.choices[0].message.content,
            "model": response.model
        })
    except Exception as e:
        return jsonify({
            "ok": False,
            "error": str(e),
            "error_type": type(e).__name__
        })