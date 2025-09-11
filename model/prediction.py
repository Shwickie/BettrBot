#!/usr/bin/env python3
"""
Fixed NFL Prediction System with Pipeline Compatibility
Keeps ALL your existing functionality and adds pipeline mode
"""

import pandas as pd
import numpy as np
import pickle
import os
import sys
from sqlalchemy import create_engine, text
from datetime import datetime
import warnings 
from pathlib import Path
import os, pickle, logging
warnings.filterwarnings('ignore')

# Config - using your exact paths and structure
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
MODEL_PATH = os.environ.get(
    "BETTR_MODEL_PKL",
    str(Path(__file__).resolve().parent.parent / "models" / "betting_model_fixed.pkl")
)
engine = create_engine(DB_PATH)

class FixedNFLSystem:
    """Your complete prediction system with pipeline compatibility added"""
    
    def __init__(self):
        self.model_data = None
        self.team_power_data = None
        self.team_mapping = self._create_team_mapping()
        self.load_model()
        self.load_team_data()
    
    def _create_team_mapping(self):
        """Your existing team mapping logic - complete"""
        return {
            # Full names to abbreviations
            'Arizona Cardinals': 'ARI', 'Atlanta Falcons': 'ATL', 'Baltimore Ravens': 'BAL',
            'Buffalo Bills': 'BUF', 'Carolina Panthers': 'CAR', 'Chicago Bears': 'CHI',
            'Cincinnati Bengals': 'CIN', 'Cleveland Browns': 'CLE', 'Dallas Cowboys': 'DAL',
            'Denver Broncos': 'DEN', 'Detroit Lions': 'DET', 'Green Bay Packers': 'GB',
            'Houston Texans': 'HOU', 'Indianapolis Colts': 'IND', 'Jacksonville Jaguars': 'JAX',
            'Kansas City Chiefs': 'KC', 'Las Vegas Raiders': 'LV', 'Los Angeles Chargers': 'LAC',
            'Los Angeles Rams': 'LAR', 'Miami Dolphins': 'MIA', 'Minnesota Vikings': 'MIN',
            'New England Patriots': 'NE', 'New Orleans Saints': 'NO', 'New York Giants': 'NYG',
            'New York Jets': 'NYJ', 'Philadelphia Eagles': 'PHI', 'Pittsburgh Steelers': 'PIT',
            'San Francisco 49ers': 'SF', 'Seattle Seahawks': 'SEA', 'Tampa Bay Buccaneers': 'TB',
            'Tennessee Titans': 'TEN', 'Washington Commanders': 'WAS',
            
            # Alternative names
            'Washington': 'WAS', 'Washington Redskins': 'WAS',
            'Los Angeles': 'LAR', 'LA Rams': 'LAR', 'LA Chargers': 'LAC',
            'Las Vegas': 'LV', 'Oakland Raiders': 'LV', 'San Francisco': 'SF',
            'New York': 'NYG', 'Tampa Bay': 'TB', 'New England': 'NE',
            'Green Bay': 'GB', 'Kansas City': 'KC',
            
            # Abbreviations to themselves
            'ARI': 'ARI', 'ATL': 'ATL', 'BAL': 'BAL', 'BUF': 'BUF',
            'CAR': 'CAR', 'CHI': 'CHI', 'CIN': 'CIN', 'CLE': 'CLE',
            'DAL': 'DAL', 'DEN': 'DEN', 'DET': 'DET', 'GB': 'GB',
            'HOU': 'HOU', 'IND': 'IND', 'JAX': 'JAX', 'KC': 'KC',
            'LV': 'LV', 'LAC': 'LAC', 'LAR': 'LAR', 'MIA': 'MIA',
            'MIN': 'MIN', 'NE': 'NE', 'NO': 'NO', 'NYG': 'NYG',
            'NYJ': 'NYJ', 'PHI': 'PHI', 'PIT': 'PIT', 'SF': 'SF',
            'SEA': 'SEA', 'TB': 'TB', 'TEN': 'TEN', 'WAS': 'WAS'
        }
    
    def normalize_team_name(self, team_name):
        """Your existing team name normalization logic"""
        if not team_name:
            return None
        
        if team_name in self.team_mapping:
            return self.team_mapping[team_name]
        
        for full_name, abbrev in self.team_mapping.items():
            if team_name.lower() == full_name.lower():
                return abbrev
        
        team_lower = team_name.lower()
        for full_name, abbrev in self.team_mapping.items():
            if team_lower in full_name.lower() or full_name.lower() in team_lower:
                return abbrev
        
        print(f"Warning: Could not map team '{team_name}' - using default")
        return team_name[:3].upper()
    
    def load_model(self):
        path = Path(MODEL_PATH)
        if not path.exists():
            logging.warning(f"Model not found at {path} — continuing without ML model.")
            self.model = None
            return
        try:
            with path.open("rb") as f:
                self.model = pickle.load(f)
        except Exception as e:
            logging.exception(f"Failed to load model: {e}")
            self.model = None
        
    def load_team_data(self):
        """Your existing team data loading logic"""
        try:
            with engine.connect() as conn:
                query = text("""
                    SELECT team, power_score, wins, losses, win_pct,
                           avg_points_for, avg_points_against, point_diff, season
                    FROM team_season_summary
                    WHERE season = (SELECT MAX(season) FROM team_season_summary)
                    ORDER BY power_score DESC
                """)
                
                self.team_power_data = pd.read_sql(query, conn)
                
                if self.team_power_data.empty:
                    print("Warning: No team power data found - using defaults")
                    self._create_default_team_data()
                else:
                    print(f"Loaded power ratings for {len(self.team_power_data)} teams")
                    print(f"  Best: {self.team_power_data.iloc[0]['team']} ({self.team_power_data.iloc[0]['power_score']:.1f})")
                    print(f"  Worst: {self.team_power_data.iloc[-1]['team']} ({self.team_power_data.iloc[-1]['power_score']:.1f})")
                    
                    print("\nTeam mapping verification:")
                    print("  Philadelphia Eagles ->", self.normalize_team_name("Philadelphia Eagles"))
                    print("  Kansas City Chiefs ->", self.normalize_team_name("Kansas City Chiefs"))
                    print("  BAL ->", self.normalize_team_name("BAL"))
        
        except Exception as e:
            print(f"Error loading team data: {e}")
            self._create_default_team_data()
    
    def _create_default_team_data(self):
        """Your existing default team data creation logic"""
        default_rankings = {
            'KC': 12.5, 'BUF': 10.2, 'BAL': 8.8, 'DET': 7.3, 'PHI': 6.1,
            'SF': 5.2, 'DAL': 4.8, 'MIA': 3.9, 'HOU': 3.1, 'CIN': 2.4,
            'GB': 1.8, 'LAC': 1.2, 'PIT': 0.7, 'SEA': 0.1, 'ATL': -0.5,
            'TB': -1.1, 'LAR': -1.8, 'MIN': -2.3, 'IND': -2.9, 'NYJ': -3.4,
            'CLE': -4.0, 'LV': -4.6, 'TEN': -5.2, 'NO': -5.8, 'JAX': -6.3,
            'DEN': -6.9, 'WAS': -7.4, 'ARI': -8.0, 'CHI': -8.6, 'NYG': -9.1,
            'NE': -9.7, 'CAR': -10.2
        }
        
        data = []
        for team, power in default_rankings.items():
            win_pct = max(0.15, min(0.85, 0.5 + (power * 0.03)))
            wins = int(win_pct * 17)
            losses = 17 - wins
            pf = max(15, min(35, 23.5 + power * 0.4))
            pa = max(15, min(35, 23.5 - power * 0.4))
            
            data.append({
                'team': team, 'power_score': power, 'wins': wins, 'losses': losses,
                'win_pct': win_pct, 'avg_points_for': pf, 'avg_points_against': pa,
                'point_diff': pf - pa, 'season': 2024
            })
        
        self.team_power_data = pd.DataFrame(data)
        print("Created realistic default team power rankings")
    
    def get_team_features(self, team_name, as_of=None, window=8):
        """Your existing team features logic"""
        team = self.normalize_team_name(team_name)
        if as_of is None:
            as_of = pd.Timestamp.utcnow().normalize()
        else:
            as_of = pd.to_datetime(as_of)

        sql = text("""
            WITH plays AS (
            SELECT g.game_date AS d,
                    CASE WHEN g.home_team = :t THEN g.home_score ELSE g.away_score END AS pf,
                    CASE WHEN g.home_team = :t THEN g.away_score ELSE g.home_score END AS pa
            FROM games g
            WHERE (g.home_team = :t OR g.away_team = :t)
                AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL
                AND g.game_date < :as_of
            ORDER BY g.game_date DESC
            LIMIT :lim
            )
            SELECT
            AVG(CASE WHEN pf > pa THEN 1.0 ELSE 0.0 END)     AS wpct_pre,
            AVG(pf)                                          AS pf_pre,
            AVG(pa)                                          AS pa_pre,
            AVG(pf - pa)                                     AS pd_pre
            FROM plays;
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={"t": team, "as_of": as_of.date(), "lim": int(window)})

        if df.empty or df.isna().all().all():
            row = self.team_power_data[self.team_power_data['team'] == team]
            if row.empty:
                return {'wpct_pre':0.5,'pf_pre':22.0,'pa_pre':22.0,'pd_pre':0.0,'power_pre':0.0,'form':0.5,'streak':0}
            r = row.iloc[0]
            return {
                'wpct_pre': float(r.get('win_pct', 0.5)),
                'pf_pre': float(r.get('avg_points_for', 22.0)),
                'pa_pre': float(r.get('avg_points_against', 22.0)),
                'pd_pre': float(r.get('point_diff', 0.0)),
                'power_pre': float(r.get('power_score', 0.0)),
                'form': float(r.get('win_pct', 0.5)),
                'streak': 0
            }

        wpct = float(df['wpct_pre'].iloc[0] if pd.notna(df['wpct_pre'].iloc[0]) else 0.5)
        pf   = float(df['pf_pre'].iloc[0]   if pd.notna(df['pf_pre'].iloc[0])   else 22.0)
        pa   = float(df['pa_pre'].iloc[0]   if pd.notna(df['pa_pre'].iloc[0])   else 22.0)
        pdif = float(df['pd_pre'].iloc[0]   if pd.notna(df['pd_pre'].iloc[0])   else 0.0)

        return {'wpct_pre':wpct,'pf_pre':pf,'pa_pre':pa,'pd_pre':pdif,'power_pre':pdif,'form':wpct,'streak':0}
    
    def predict_game(self, home_team, away_team, game_date=None):
        """Your existing prediction logic"""
        as_of = pd.to_datetime(game_date) if game_date else pd.Timestamp.utcnow()
        home_features = self.get_team_features(home_team)
        away_features = self.get_team_features(away_team)
        
        feature_dict = {}
        
        for side, features in [('home', home_features), ('away', away_features)]:
            for key, value in features.items():
                feature_dict[f'{side}_{key}'] = value
        
        feature_dict['power_diff'] = home_features['power_pre'] - away_features['power_pre']
        feature_dict['win_pct_diff'] = home_features['wpct_pre'] - away_features['wpct_pre']
        feature_dict['offense_diff'] = home_features['pf_pre'] - away_features['pf_pre']
        feature_dict['defense_diff'] = -home_features['pa_pre'] + away_features['pa_pre']
        feature_dict['form_diff'] = home_features['form'] - away_features['form']
        feature_dict['streak_diff'] = home_features['streak'] - away_features['streak']
        
        if game_date:
            dt = pd.to_datetime(game_date)
            month = dt.month
            day_of_week = dt.weekday()
        else:
            month = 9
            day_of_week = 0
        
        feature_dict.update({
            'home_field_advantage': 2.5,
            'late_season': 1 if month >= 11 else 0,
            'prime_time': 1 if day_of_week in [0, 1] else 0,
            'both_good': 1 if (home_features['power_pre'] > 2 and away_features['power_pre'] > 2) else 0,
            'mismatch_game': 1 if abs(feature_dict['power_diff']) > 5 else 0,
            'power_x_form': feature_dict['power_diff'] * feature_dict['form_diff'],
            'strength_disparity': abs(feature_dict['power_diff']),
            'month': month, 'day_of_week': day_of_week, 'rest_diff': 0,
            'home_rest_days': 7, 'away_rest_days': 7, 'same_division': 0, 'same_conference': 0
        })
        
        feature_cols = self.model_data['feature_cols']
        X_values = []
        
        for col in feature_cols:
            X_values.append(feature_dict.get(col, 0.0))
        
        X_df = pd.DataFrame([X_values], columns=feature_cols)
        
        if self.model_data.get('uses_scaled', False):
            X_df = pd.DataFrame(
                self.model_data['scaler'].transform(X_df),
                columns=feature_cols
            )
        
        model = self.model_data['model']
        home_win_prob = float(model.predict_proba(X_df)[0, 1])
        away_win_prob = 1.0 - home_win_prob
        
        return {
            'home_team': home_team,
            'away_team': away_team,
            'home_team_abbrev': self.normalize_team_name(home_team),
            'away_team_abbrev': self.normalize_team_name(away_team),
            'home_win_probability': home_win_prob,
            'away_win_probability': away_win_prob,
            'game_date': as_of.strftime('%Y-%m-%d'),
            'predicted_winner': home_team if home_win_prob > 0.5 else away_team,
            'confidence': max(home_win_prob, away_win_prob),
            'power_difference': feature_dict['power_diff'],
            'key_factors': {
                'power_diff': feature_dict['power_diff'],
                'win_pct_diff': feature_dict['win_pct_diff'],
                'offense_diff': feature_dict['offense_diff'],
                'form_diff': feature_dict['form_diff']
            }
        }

    # === ALL YOUR ODDS HELPERS & KELLY METHODS ===========================

    def _to_decimal_odds(self, odds_value):
        """Your existing decimal odds conversion"""
        try:
            x = float(odds_value)
            if 1.01 <= x <= 50:   # already decimal
                return x
            if x >= 100:          # American +
                return 1 + (x / 100.0)
            if x <= -100:         # American -
                return 1 + (100.0 / abs(x))
            return 1.91           # fallback ≈ -110
        except:
            return 1.91

    def _kelly_bet_size(self, p, dec_odds):
        """Your existing Kelly bet sizing"""
        if p <= 0 or dec_odds <= 1:
            return 0.0
        b = dec_odds - 1.0
        q = 1.0 - p
        edge = p*b - q
        return max(0.0, edge / b)

    def _aggregate_latest_odds(self, game_date, home_team, away_team, now=None):
        """Your existing odds aggregation logic"""
        if now is None:
            now = pd.Timestamp.utcnow()
        gd = pd.to_datetime(game_date)
        home_norm = self.normalize_team_name(home_team)
        away_norm = self.normalize_team_name(away_team)

        with engine.connect() as conn:
            odds = pd.read_sql(text("""
                SELECT game_id as odds_game_id, sportsbook, team, market, odds, timestamp
                FROM odds
                WHERE lower(market) IN ('h2h','moneyline','ml')
            """), conn)

        if odds.empty:
            return None

        odds['team_norm'] = odds['team'].apply(self.normalize_team_name)
        odds['timestamp'] = pd.to_datetime(odds['timestamp'], errors='coerce')

        date_str = gd.strftime('%Y-%m-%d')
        cand = odds[
            (odds['odds_game_id'].isin([
                f"{date_str}_{home_team}_{away_team}",
                f"{gd.date()}_{home_team}_{away_team}"
            ])) |
            (odds['timestamp'].dt.date == gd.date())
        ].copy()

        if cand.empty:
            return None

        cand = cand[cand['timestamp'].notna() & (cand['timestamp'] <= now)]
        if cand.empty:
            return None

        out = {}
        for side_norm in (home_norm, away_norm):
            ss = cand[cand['team_norm'] == side_norm]
            if ss.empty:
                return None
            last_per_book = ss.sort_values('timestamp').groupby('sportsbook').tail(1).copy()
            last_per_book['dec'] = last_per_book['odds'].apply(self._to_decimal_odds)
            last_per_book['imp'] = 1.0 / last_per_book['dec']
            out[side_norm] = {
                'books': last_per_book['sportsbook'].nunique(),
                'dec':   float(1.0 / last_per_book['imp'].median()),
                'imp':   float(last_per_book['imp'].median())
            }

        tot_imp = out[home_norm]['imp'] + out[away_norm]['imp']
        out[home_norm]['mkt_prob'] = out[home_norm]['imp'] / tot_imp
        out[away_norm]['mkt_prob'] = out[away_norm]['imp'] / tot_imp
        return out

    def picks_for_upcoming_games(self, bankroll=1000.0):
        """Your existing live picks logic"""
        MAX_DEC_ODDS_TO_BET = 2.70
        MIN_ABS_PROB_TO_BET = 0.54
        MIN_BOOKS_REQUIRED  = 3
        MIN_EDGE            = 0.025
        KELLY_FRACTION      = 0.10
        MAX_BET_PCT         = 0.015

        def edge_bump(dec):
            bumps = [(2.2, 0.01), (2.4, 0.02), (2.6, 0.03)]
            b = 0.0
            for thr, inc in bumps:
                if dec >= thr:
                    b = inc
            return b

        games = self.get_upcoming_games()
        if games.empty:
            print("No upcoming games found.")
            return []

        picks = []
        for _, g in games.iterrows():
            home = g['home_team']; away = g['away_team']; gd = g['game_date']
            pred = self.predict_game(home, away, gd)
            p_home = float(pred['home_win_probability'])
            p_away = 1.0 - p_home

            agg = self._aggregate_latest_odds(gd, home, away)
            if not agg:
                continue

            h = agg[self.normalize_team_name(home)]
            a = agg[self.normalize_team_name(away)]

            if min(h['books'], a['books']) < MIN_BOOKS_REQUIRED:
                continue
            if max(h['dec'], a['dec']) > MAX_DEC_ODDS_TO_BET:
                continue

            e_home = p_home - h['mkt_prob']
            e_away = p_away - a['mkt_prob']

            candidates = []
            if p_home >= MIN_ABS_PROB_TO_BET and e_home >= (MIN_EDGE + edge_bump(h['dec'])):
                candidates.append(('HOME', p_home, e_home, h['dec']))
            if p_away >= MIN_ABS_PROB_TO_BET and e_away >= (MIN_EDGE + edge_bump(a['dec'])):
                candidates.append(('AWAY', p_away, e_away, a['dec']))

            if not candidates:
                continue

            side, win_prob, edge, dec = max(candidates, key=lambda t: t[2])

            kelly = self._kelly_bet_size(win_prob, dec)
            bet_size = min(MAX_BET_PCT * bankroll, kelly * bankroll * KELLY_FRACTION)
            if bet_size <= 0:
                continue

            picks.append({
                'game_date': str(gd),
                'start_time_utc': str(g.get('start_time_utc', '')),
                'matchup': f"{away} @ {home}",
                'pick': f"{home if side=='HOME' else away} ML",
                'side': side,
                'model_prob': round(win_prob, 4),
                'market_prob': round(h['mkt_prob'] if side=='HOME' else a['mkt_prob'], 4),
                'edge': round(edge, 4),
                'price': round(dec, 3),
                'books': int(h['books'] if side=='HOME' else a['books']),
                'stake': round(bet_size, 2),
            })

        picks.sort(key=lambda x: (x['game_date'], x.get('start_time_utc','')))
        return picks

    def get_upcoming_games(self):
        """Your existing upcoming games logic"""
        try:
            with engine.connect() as conn:
                query = text("""
                    SELECT game_id, home_team, away_team, game_date, start_time_utc
                    FROM games
                    WHERE game_date >= date('now')
                    AND home_score IS NULL
                    ORDER BY game_date
                    LIMIT 20
                """)
                return pd.read_sql(query, conn)
        except Exception as e:
            print(f"Error fetching games: {e}")
            return pd.DataFrame()

    def display_predictions(self, predictions):
        """Your existing display predictions logic"""
        print("\nNFL GAME PREDICTIONS (Fixed Team Mapping)")
        print("=" * 90)
        print(f"{'Matchup':<25} {'Prediction':<20} {'Conf':<8} {'Power':<8} {'WinPct':<8} {'OffDiff':<8}")
        print("-" * 90)
        
        for pred in predictions:
            home_abbrev = pred['home_team_abbrev']
            away_abbrev = pred['away_team_abbrev']
            matchup = f"{away_abbrev} @ {home_abbrev}"
            
            winner_abbrev = pred['home_team_abbrev'] if pred['predicted_winner'] == pred['home_team'] else pred['away_team_abbrev']
            prob = pred['confidence']
            prediction_str = f"{winner_abbrev} ({prob:.1%})"
            
            power_diff = pred['power_difference']
            factors = pred['key_factors']
            
            print(f"{matchup:<25} {prediction_str:<20} {prob:<7.1%} {power_diff:<7.1f} "
                  f"{factors['win_pct_diff']:<7.1%} {factors['offense_diff']:<7.1f}")
    
    def conservative_betting_analysis(self, predictions, bankroll=100):
        """Your existing betting analysis logic"""
        print(f"\nCONSERVATIVE BETTING ANALYSIS (${bankroll} bankroll)")
        print("=" * 70)
        print("Criteria: >60% confidence AND (power diff >4 OR multiple factors)")
        print("-" * 70)
        
        betting_opportunities = []
        
        for pred in predictions:
            confidence = pred['confidence']
            factors = pred['key_factors']
            power_diff = abs(factors['power_diff'])
            
            strong_power_edge = power_diff > 4.0
            multiple_factors = (abs(factors['win_pct_diff']) > 0.15 and 
                              abs(factors['offense_diff']) > 2.0)
            
            if confidence > 0.60 and (strong_power_edge or multiple_factors):
                bet_size = min(bankroll * 0.015, 10.0)
                
                betting_opportunities.append({
                    'matchup': f"{pred['away_team_abbrev']} @ {pred['home_team_abbrev']}",
                    'pick': pred['home_team_abbrev'] if pred['predicted_winner'] == pred['home_team'] else pred['away_team_abbrev'],
                    'confidence': confidence,
                    'power_diff': factors['power_diff'],
                    'suggested_bet': bet_size,
                    'reason': self._get_betting_reason(pred)
                })
        
        if betting_opportunities:
            print(f"{'Matchup':<15} {'Pick':<8} {'Conf':<8} {'Power':<8} {'Bet':<8} {'Reason'}")
            print("-" * 70)
            
            for opp in betting_opportunities:
                print(f"{opp['matchup']:<15} {opp['pick']:<8} {opp['confidence']:<7.1%} "
                      f"{opp['power_diff']:<7.1f} ${opp['suggested_bet']:<7.0f} {opp['reason']}")
            
            total_risk = sum(opp['suggested_bet'] for opp in betting_opportunities)
            print(f"\nTotal weekly risk: ${total_risk:.0f} ({total_risk/bankroll:.1%} of bankroll)")
            print("REMEMBER: Sports betting is high risk. Never bet money you can't afford to lose.")
        else:
            print("No games meet conservative betting criteria this week.")
            print("This is normal and protects your bankroll.")
        
        return betting_opportunities
    
    def _get_betting_reason(self, pred):
        """Your existing betting reason logic"""
        factors = pred['key_factors']
        
        if abs(factors['power_diff']) > 6:
            return "Major power gap"
        elif abs(factors['power_diff']) > 4:
            return "Power advantage"
        elif abs(factors['offense_diff']) > 4:
            return "Offense mismatch"
        elif abs(factors['win_pct_diff']) > 0.25:
            return "Record gap"
        else:
            return "Multiple edges"

    def show_all_predictions_batch(self):
        """NEW: Batch mode for pipeline - shows predictions and exits"""
        upcoming = self.get_upcoming_games()
        
        if upcoming.empty:
            print("No upcoming games found")
            return True
        
        print(f"\nPREDICTIONS FOR {len(upcoming)} UPCOMING GAMES:")
        print("=" * 80)
        
        predictions_made = 0
        
        for _, game in upcoming.iterrows():
            try:
                pred = self.predict_game(game['home_team'], game['away_team'], game['game_date'])
                
                away_abbrev = pred['away_team_abbrev']
                home_abbrev = pred['home_team_abbrev']
                winner_abbrev = pred['home_team_abbrev'] if pred['predicted_winner'] == pred['home_team'] else pred['away_team_abbrev']
                confidence = pred['confidence']
                
                print(f"{game['game_date'][:10]} | {away_abbrev:3} @ {home_abbrev:3} | {winner_abbrev:3} wins | {confidence:.1%} confidence")
                predictions_made += 1
                
            except Exception as e:
                print(f"{game['game_date'][:10]} | {game['away_team']:3} @ {game['home_team']:3} | Error: {str(e)[:30]}")
        
        print(f"\nMade {predictions_made} predictions")
        return True


def is_running_in_pipeline():
    """NEW: Check if we're running in a non-interactive environment (pipeline)"""
    try:
        return not sys.stdin.isatty()
    except:
        return True


def main():
    print("FIXED NFL PREDICTION SYSTEM - Team Mapping Resolved")
    print("=" * 60)
    
    try:
        system = FixedNFLSystem()
        
        # Check for pipeline mode using environment variable
        is_pipeline = os.environ.get('BETTR_PIPELINE_MODE') == 'true'
        
        if is_pipeline:
            # Batch mode - show predictions and exit
            print("Running in batch mode (pipeline)")
            success = system.show_all_predictions_batch()
            if success:
                print("Batch prediction complete")
                sys.exit(0)
            else:
                print("Batch prediction failed")
                sys.exit(1)
        
        # Interactive mode - your complete existing menu system
        while True:
            print("\nOPTIONS:")
            print("1. Show all predictions")
            print("2. Conservative betting analysis")
            print("3. Predict specific matchup")
            print("4. Test team mapping")
            print("5. Exit")
            print("6. Live picks from current odds snapshot")
            
            try:
                choice = input("\nChoice (1-6): ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nGoodbye!")
                break
                
            if choice == '1' or choice == '2':
                print("\nGetting upcoming games...")
                upcoming = system.get_upcoming_games()
                
                if upcoming.empty:
                    print("No upcoming games found")
                    continue
                
                predictions = []
                for _, game in upcoming.iterrows():
                    try:
                        pred = system.predict_game(
                            game['home_team'],
                            game['away_team'],
                            game['game_date']
                        )
                        predictions.append(pred)
                    except Exception as e:
                        print(f"Error predicting {game['away_team']} @ {game['home_team']}: {e}")
                
                if choice == '1':
                    system.display_predictions(predictions)
                else:
                    system.display_predictions(predictions)
                    bankroll = float(input(f"\nEnter your bankroll (default $500): ") or "500")
                    system.conservative_betting_analysis(predictions, bankroll)
            
            elif choice == '3':
                try:
                    print("\nEnter teams (full names or abbreviations):")
                    home_team = input("Home team: ").strip()
                    away_team = input("Away team: ").strip()
                    
                    pred = system.predict_game(home_team, away_team)
                    
                    print(f"\nPREDICTION: {pred['away_team_abbrev']} @ {pred['home_team_abbrev']}")
                    print("-" * 50)
                    print(f"Home Win: {pred['home_win_probability']:.1%}")
                    print(f"Away Win: {pred['away_win_probability']:.1%}")
                    print(f"Predicted Winner: {pred['predicted_winner']} ({pred['confidence']:.1%})")
                    print(f"Power Difference: {pred['power_difference']:.2f}")
                    
                    print(f"\nKey Factors:")
                    for factor, value in pred['key_factors'].items():
                        print(f"  {factor}: {value:.2f}")
                        
                except (EOFError, KeyboardInterrupt):
                    print("\nReturning to menu...")
            
            elif choice == '4':
                try:
                    print("\nTesting team name mapping:")
                    test_names = [
                        "Philadelphia Eagles", "Kansas City Chiefs", "PHI", "KC",
                        "New York Giants", "Los Angeles Rams", "Green Bay Packers"
                    ]
                    for name in test_names:
                        mapped = system.normalize_team_name(name)
                        print(f"  '{name}' -> '{mapped}'")
                except (EOFError, KeyboardInterrupt):
                    print("\nReturning to menu...")
            
            elif choice == '5':
                print("Goodbye!")
                break
            
            elif choice == '6':
                try:
                    print("Live picks from current odds snapshot")
                    bankroll = float(input("Bankroll for sizing (default 1000): ") or "1000")
                    picks = system.picks_for_upcoming_games(bankroll=bankroll)
                    if not picks:
                        print("No qualifying picks right now (filters too tight or no odds).")
                    else:
                        print("\nLIVE PICKS (sorted by date/time)")
                        print("=================================")
                        for p in picks:
                            print(f"{p['game_date']}  {p['matchup']}")
                            print(f"  Pick: {p['pick']}  @{p['price']}  books:{p['books']}")
                            print(f"  Model: {p['model_prob']:.1%}  Market: {p['market_prob']:.1%}  Edge: {p['edge']:.1%}")
                            print(f"  Suggested stake: ${p['stake']:.2f}")
                except (EOFError, KeyboardInterrupt):
                    print("\nReturning to menu...")
            
            else:
                print("Invalid choice")
    
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()