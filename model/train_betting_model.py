# train_betting_model.py
"""
Train a real ML model on your 4 years of betting data
Run this separately to create your model, then use it in the dashboard
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pickle
import os

# Install these if needed: pip install scikit-learn xgboost
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, roc_auc_score, classification_report

DB_PATH = r"E:/Bettr Bot/betting-bot/data/betting.db"
MODEL_PATH = r"E:/Bettr Bot/betting-bot/models/"

class BettingModelTrainer:
    """Train ML models on your historical betting data"""
    
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)
        self.models = {}
        self.scaler = StandardScaler()
        self.feature_importance = {}

    
        
    def build_training_dataset(self):
        """Extract features from your 4 years of data (robust to missing player_game_stats columns)."""
        print("Building training dataset from your database...")
        conn = self.conn

        def _table_has_cols(table, cols):
            try:
                cur = conn.execute(f"PRAGMA table_info({table})")
                names = {r[1].lower() for r in cur.fetchall()}
                return all(c.lower() in names for c in cols)
            except Exception:
                return False

        use_qb_stats = _table_has_cols("player_game_stats", ["position", "fantasy_points", "team", "game_id"])

        player_stats_cte = """
        , player_stats AS (
            SELECT 
                g.game_id,
                MAX(CASE WHEN ps.position = 'QB' AND ps.team = g.home_team THEN ps.fantasy_points END) AS home_qb_fantasy,
                MAX(CASE WHEN ps.position = 'QB' AND ps.team = g.away_team THEN ps.fantasy_points END) AS away_qb_fantasy
            FROM games g
            LEFT JOIN player_game_stats ps ON g.game_id = ps.game_id
            GROUP BY g.game_id
        )
        """ if use_qb_stats else """
        , player_stats AS (
            SELECT g.game_id, 0.0 AS home_qb_fantasy, 0.0 AS away_qb_fantasy
            FROM games g GROUP BY g.game_id
        )
        """

        query = f"""
        WITH game_features AS (
            SELECT 
                g.game_id,
                g.home_team,
                g.away_team,
                g.home_score,
                g.away_score,
                g.game_date,
                CASE WHEN g.home_score > g.away_score THEN 1 ELSE 0 END AS home_win,

                -- Power scores
                ht.power_score AS home_power,
                at.power_score AS away_power,
                ht.win_pct AS home_win_pct,
                at.win_pct AS away_win_pct,
                ht.avg_points_for AS home_offense,
                ht.avg_points_against AS home_defense,
                at.avg_points_for AS away_offense,
                at.avg_points_against AS away_defense,
                ht.games_played AS home_games,
                at.games_played AS away_games,

                -- Get season/calendar
                CAST(strftime('%Y', g.game_date) AS INTEGER) AS season,
                CAST(strftime('%m', g.game_date) AS INTEGER) AS month,
                CAST(strftime('%w', g.game_date) AS INTEGER) AS day_of_week

            FROM games g
            LEFT JOIN team_season_summary ht 
            ON g.home_team = ht.team 
            AND ht.season = CAST(strftime('%Y', g.game_date) AS INTEGER)
            LEFT JOIN team_season_summary at 
            ON g.away_team = at.team 
            AND at.season = CAST(strftime('%Y', g.game_date) AS INTEGER)
            WHERE g.home_score IS NOT NULL
            AND g.game_date > date('now', '-4 years')
        ),

        recent_form AS (
            SELECT
                g1.game_id,
                (SELECT AVG(
                    CASE
                        WHEN (g2.home_team = g1.home_team AND g2.home_score > g2.away_score) OR
                            (g2.away_team = g1.home_team AND g2.away_score > g2.home_score)
                        THEN 1 ELSE 0 END)
                FROM games g2
                WHERE g2.game_date < g1.game_date
                AND g2.game_date > date(g1.game_date, '-30 days')
                AND (g2.home_team = g1.home_team OR g2.away_team = g1.home_team)
                AND g2.home_score IS NOT NULL
                LIMIT 5) AS home_recent_form,

                (SELECT AVG(
                    CASE
                        WHEN (g2.home_team = g1.away_team AND g2.home_score > g2.away_score) OR
                            (g2.away_team = g1.away_team AND g2.away_score > g2.home_score)
                        THEN 1 ELSE 0 END)
                FROM games g2
                WHERE g2.game_date < g1.game_date
                AND g2.game_date > date(g1.game_date, '-30 days')
                AND (g2.home_team = g1.away_team OR g2.away_team = g1.away_team)
                AND g2.home_score IS NOT NULL
                LIMIT 5) AS away_recent_form
            FROM games g1
        ),

        head_to_head AS (
            SELECT
                g1.game_id,
                COUNT(g2.game_id) AS h2h_games,
                AVG(CASE WHEN g2.home_team = g1.home_team
                        THEN CASE WHEN g2.home_score > g2.away_score THEN 1 ELSE 0 END
                        ELSE CASE WHEN g2.away_score > g2.home_score THEN 1 ELSE 0 END
                    END) AS home_h2h_win_rate
            FROM games g1
            LEFT JOIN games g2 ON
                ((g2.home_team = g1.home_team AND g2.away_team = g1.away_team) OR
                (g2.home_team = g1.away_team AND g2.away_team = g1.home_team))
                AND g2.game_date < g1.game_date
                AND g2.game_date > date(g1.game_date, '-2 years')
                AND g2.home_score IS NOT NULL
            GROUP BY g1.game_id
        )
        {player_stats_cte}

        SELECT 
            gf.*,
            rf.home_recent_form,
            rf.away_recent_form,
            h2h.h2h_games,
            h2h.home_h2h_win_rate,
            ps.home_qb_fantasy,
            ps.away_qb_fantasy
        FROM game_features gf
        LEFT JOIN recent_form rf ON gf.game_id = rf.game_id
        LEFT JOIN head_to_head h2h ON gf.game_id = h2h.game_id
        LEFT JOIN player_stats ps ON gf.game_id = ps.game_id
        WHERE gf.home_power IS NOT NULL
        AND gf.away_power IS NOT NULL
        """

        df = pd.read_sql_query(query, conn)

        # engineered diffs + simple placeholders for injuries/hfa/form
        df['power_diff']   = df['home_power'] - df['away_power']
        df['win_pct_diff'] = df['home_win_pct'] - df['away_win_pct']
        df['offense_diff'] = df['home_offense'] - df['away_offense']
        df['defense_diff'] = df['home_defense'] - df['away_defense']
        df['form_diff']    = (df['home_recent_form'].fillna(0) - df['away_recent_form'].fillna(0))
        df['home_field_advantage'] = 3.0
        df['home_injury_impact'] = 0.0
        df['away_injury_impact'] = 0.0
        df['home_qb_injury'] = 0.0
        df['away_qb_injury'] = 0.0

        return df

    
    def train_models(self, df):
        """Train multiple models and compare performance"""
        
        # Define features
        feature_cols = [
            'home_power', 'away_power',
            'home_win_pct', 'away_win_pct',
            'home_offense', 'away_offense',
            'home_defense', 'away_defense',
            'home_recent_form', 'away_recent_form',
            'h2h_games', 'home_h2h_win_rate',
            'power_diff', 'win_pct_diff',
            'offense_diff', 'defense_diff',
            'form_diff', 'home_field_advantage',
            'home_injury_impact', 'away_injury_impact',
            'home_qb_injury', 'away_qb_injury',
            'month', 'day_of_week'
        ]
        
        # Handle missing values
        df[feature_cols] = df[feature_cols].fillna(0)
        
        X = df[feature_cols]
        y = df['home_win']
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Train multiple models
        models = {
            'XGBoost': XGBClassifier(
                n_estimators=200,
                learning_rate=0.1,
                max_depth=6,
                random_state=42,
                use_label_encoder=False,
                eval_metric='logloss'
            ),
            'GradientBoosting': GradientBoostingClassifier(
                n_estimators=150,
                learning_rate=0.1,
                max_depth=5,
                random_state=42
            ),
            'RandomForest': RandomForestClassifier(
                n_estimators=200,
                max_depth=10,
                random_state=42
            )
        }
        
        results = {}
        
        for name, model in models.items():
            print(f"\nTraining {name}...")
            
            # Train
            model.fit(X_train_scaled, y_train)
            
            # Predict
            y_pred = model.predict(X_test_scaled)
            y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]
            
            # Evaluate
            accuracy = accuracy_score(y_test, y_pred)
            auc = roc_auc_score(y_test, y_pred_proba)
            
            # Cross-validation
            cv_scores = cross_val_score(model, X_train_scaled, y_train, cv=5, scoring='roc_auc')
            
            results[name] = {
                'model': model,
                'accuracy': accuracy,
                'auc': auc,
                'cv_mean': cv_scores.mean(),
                'cv_std': cv_scores.std()
            }
            
            print(f"{name} - Accuracy: {accuracy:.3f}, AUC: {auc:.3f}, CV: {cv_scores.mean():.3f} (+/- {cv_scores.std():.3f})")
            
            # Feature importance
            if hasattr(model, 'feature_importances_'):
                importance = pd.DataFrame({
                    'feature': feature_cols,
                    'importance': model.feature_importances_
                }).sort_values('importance', ascending=False)
                
                print(f"\nTop 10 features for {name}:")
                print(importance.head(10))
                
                self.feature_importance[name] = importance
        
        # Select best model
        best_model_name = max(results, key=lambda x: results[x]['auc'])
        print(f"\nBest model: {best_model_name} with AUC: {results[best_model_name]['auc']:.3f}")
        
        self.best_model = results[best_model_name]['model']
        self.feature_cols = feature_cols
        
        return results
    
    def save_model(self):
        """Save the trained model"""
        os.makedirs(MODEL_PATH, exist_ok=True)
        
        model_data = {
            'model': self.best_model,
            'scaler': self.scaler,
            'feature_cols': self.feature_cols,
            'feature_importance': self.feature_importance,
            'timestamp': datetime.now().isoformat()
        }
        
        with open(os.path.join(MODEL_PATH, 'betting_model.pkl'), 'wb') as f:
            pickle.dump(model_data, f)
        
        print(f"Model saved to {MODEL_PATH}")
    
    def backtest(self, df, model):
        """Backtest model performance on historical odds"""
        print("\nRunning backtest...")
        
        # Get historical odds for games
        odds_query = """
        SELECT 
            o.game_id,
            o.team,
            AVG(o.odds) as avg_odds
        FROM odds o
        WHERE o.market = 'h2h'
        GROUP BY o.game_id, o.team
        """
        
        odds = pd.read_sql_query(odds_query, self.conn)
        
        # Calculate ROI
        total_bets = 0
        winning_bets = 0
        total_profit = 0
        
        for _, game in df.iterrows():
            # Get model prediction
            features = game[self.feature_cols].values.reshape(1, -1)
            features_scaled = self.scaler.transform(features)
            prob = model.predict_proba(features_scaled)[0, 1]
            
            # Get odds for this game
            game_odds = odds[odds['game_id'] == game['game_id']]
            
            if not game_odds.empty:
                home_odds = game_odds[game_odds['team'] == game['home_team']]['avg_odds'].values
                away_odds = game_odds[game_odds['team'] == game['away_team']]['avg_odds'].values
                
                if len(home_odds) > 0 and len(away_odds) > 0:
                    home_odds = home_odds[0]
                    away_odds = away_odds[0]
                    
                    # Convert to implied probability
                    home_implied = 1 / home_odds if home_odds > 1 else 0.5
                    away_implied = 1 / away_odds if away_odds > 1 else 0.5
                    
                    # Check for value bet
                    if prob > home_implied * 1.05:  # 5% edge minimum
                        total_bets += 1
                        if game['home_win'] == 1:
                            winning_bets += 1
                            total_profit += (home_odds - 1)
                        else:
                            total_profit -= 1
                    elif (1 - prob) > away_implied * 1.05:
                        total_bets += 1
                        if game['home_win'] == 0:
                            winning_bets += 1
                            total_profit += (away_odds - 1)
                        else:
                            total_profit -= 1
        
        if total_bets > 0:
            roi = (total_profit / total_bets) * 100
            win_rate = (winning_bets / total_bets) * 100
            print(f"Backtest Results:")
            print(f"Total Bets: {total_bets}")
            print(f"Win Rate: {win_rate:.1f}%")
            print(f"ROI: {roi:.1f}%")
            print(f"Profit: ${total_profit:.2f} per unit")
        else:
            print("No qualifying bets found in backtest")

# Run training
if __name__ == "__main__":
    trainer = BettingModelTrainer()
    
    # Build dataset
    df = trainer.build_training_dataset()
    
    # Train models
    results = trainer.train_models(df)
    
    # Save best model
    trainer.save_model()
    
    # Backtest
    trainer.backtest(df.tail(100), trainer.best_model)  # Test on last 100 games
    
    print("\nTraining complete! Model saved to:", MODEL_PATH)