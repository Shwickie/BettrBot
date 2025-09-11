# simple_betting_model.py - A model that actually works
"""
Simplified betting model that focuses on what works:
- Power ratings differential  
- Recent form
- Home field advantage
- Simple injury flags
- Clean, interpretable predictions
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pickle
import os
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score, log_loss
from sklearn.model_selection import train_test_split

DB_PATH = r"E:/Bettr Bot/betting-bot/data/betting.db"
MODEL_PATH = r"E:/Bettr Bot/betting-bot/models/"

class SimpleBettingModel:
    """Simple, working betting model focused on core factors"""
    
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)
        self.model = None
        self.feature_names = []
        
    def build_simple_dataset(self):
        """Build dataset using only reliable, core features"""
        print("Building simple but effective dataset...")
        
        # Core query focusing on team strength metrics
        query = """
        WITH game_data AS (
            SELECT 
                g.game_id,
                g.home_team,
                g.away_team,
                g.home_score,
                g.away_score,
                g.game_date,
                CASE WHEN g.home_score > g.away_score THEN 1 ELSE 0 END AS home_win,
                strftime('%Y', g.game_date) AS season,
                strftime('%m', g.game_date) AS month,
                strftime('%w', g.game_date) AS day_of_week,
                
                -- Home team strength
                COALESCE(ht.power_score, 0) as home_power,
                COALESCE(ht.win_pct, 0.5) as home_win_pct,
                COALESCE(ht.avg_points_for, 20) as home_offense,
                COALESCE(ht.avg_points_against, 20) as home_defense,
                COALESCE(ht.wins, 0) as home_wins,
                COALESCE(ht.losses, 0) as home_losses,
                
                -- Away team strength
                COALESCE(at.power_score, 0) as away_power,
                COALESCE(at.win_pct, 0.5) as away_win_pct,
                COALESCE(at.avg_points_for, 20) as away_offense,
                COALESCE(at.avg_points_against, 20) as away_defense,
                COALESCE(at.wins, 0) as away_wins,
                COALESCE(at.losses, 0) as away_losses
                
            FROM games g
            LEFT JOIN team_season_summary ht ON g.home_team = ht.team 
                AND CAST(strftime('%Y', g.game_date) AS INTEGER) = ht.season
            LEFT JOIN team_season_summary at ON g.away_team = at.team 
                AND CAST(strftime('%Y', g.game_date) AS INTEGER) = at.season
            WHERE g.home_score IS NOT NULL 
            AND g.game_date > date('now', '-4 years')
        )
        SELECT * FROM game_data
        WHERE home_power != 0 AND away_power != 0  -- Only games with team data
        """
        
        df = pd.read_sql_query(query, self.conn)
        
        if len(df) < 100:
            raise Exception(f"Dataset too small: {len(df)} games. Check team_season_summary table.")
        
        print(f"Loaded {len(df)} games with complete data")
        
        # Core engineered features (the ones that actually matter)
        df['power_diff'] = df['home_power'] - df['away_power']
        df['win_pct_diff'] = df['home_win_pct'] - df['away_win_pct']
        df['offense_diff'] = df['home_offense'] - df['away_offense']  
        df['defense_diff'] = df['away_defense'] - df['home_defense']  # Away defense - Home defense (good for home)
        df['record_diff'] = (df['home_wins'] - df['home_losses']) - (df['away_wins'] - df['away_losses'])
        
        # Home field advantage (constant)
        df['home_field_advantage'] = 2.5
        
        # Temporal features
        df['month'] = pd.to_numeric(df['month'])
        df['day_of_week'] = pd.to_numeric(df['day_of_week'])
        df['late_season'] = (df['month'] >= 11).astype(int)  # November+ games
        
        # Simple interaction terms
        df['power_x_form'] = df['power_diff'] * df['win_pct_diff']
        df['total_strength'] = df['home_power'] + df['away_power']  # Game quality
        
        # Division rivalry (simplified)
        df['likely_rivalry'] = 0  # Can enhance this later
        
        return df
    
    def train_simple_model(self, df):
        """Train a simple but effective model"""
        
        # Features to use (only the reliable ones)
        feature_cols = [
            'power_diff', 'win_pct_diff', 'offense_diff', 'defense_diff', 
            'record_diff', 'home_field_advantage', 'power_x_form', 
            'total_strength', 'month', 'late_season'
        ]
        
        self.feature_names = feature_cols
        
        # Prepare data
        X = df[feature_cols].fillna(0)
        y = df['home_win']
        
        print(f"Training on {len(feature_cols)} core features")
        print("Features:", feature_cols)
        
        # Time-based split to avoid lookahead bias
        df_sorted = df.sort_values('game_date')
        split_point = int(len(df_sorted) * 0.8)
        
        X_train = X.iloc[:split_point]
        X_test = X.iloc[split_point:]
        y_train = y.iloc[:split_point]
        y_test = y.iloc[split_point:]
        
        print(f"Train: {len(X_train)}, Test: {len(X_test)}")
        
        # Train multiple models
        models = {
            'RandomForest': RandomForestClassifier(
                n_estimators=100, 
                max_depth=6,  # Prevent overfitting
                min_samples_split=50,
                min_samples_leaf=20,
                random_state=42
            ),
            'LogisticRegression': LogisticRegression(
                C=1.0, 
                random_state=42, 
                max_iter=1000
            )
        }
        
        best_model = None
        best_score = 0
        results = {}
        
        for name, model in models.items():
            print(f"\nTraining {name}...")
            
            model.fit(X_train, y_train)
            y_pred_proba = model.predict_proba(X_test)[:, 1]
            y_pred = model.predict(X_test)
            
            accuracy = accuracy_score(y_test, y_pred)
            auc = roc_auc_score(y_test, y_pred_proba)
            logloss = log_loss(y_test, y_pred_proba)
            
            # Key diagnostic: prediction spread
            pred_std = np.std(y_pred_proba)
            pred_range = np.max(y_pred_proba) - np.min(y_pred_proba)
            
            print(f"  Accuracy: {accuracy:.3f}")
            print(f"  AUC: {auc:.3f}")
            print(f"  LogLoss: {logloss:.3f}")
            print(f"  Prediction Std: {pred_std:.3f}")
            print(f"  Prediction Range: {pred_range:.3f}")
            
            # Check if model is actually learning
            if pred_std < 0.05:
                print(f"  WARNING: {name} has low prediction variance - may not be learning")
            
            results[name] = {
                'model': model,
                'auc': auc,
                'logloss': logloss,
                'pred_std': pred_std
            }
            
            if auc > best_score and pred_std > 0.05:  # Must have reasonable prediction spread
                best_score = auc
                best_model = model
        
        if best_model is None:
            print("WARNING: No model achieved good prediction spread!")
            best_model = results['RandomForest']['model']  # Fallback
        
        self.model = best_model
        
        # Feature importance
        if hasattr(best_model, 'feature_importances_'):
            importance_df = pd.DataFrame({
                'feature': feature_cols,
                'importance': best_model.feature_importances_
            }).sort_values('importance', ascending=False)
            
            print(f"\nFeature Importance:")
            for _, row in importance_df.iterrows():
                print(f"  {row['feature']}: {row['importance']:.3f}")
                
            # Sanity check
            if importance_df.iloc[0]['importance'] < 0.1:
                print("WARNING: Top feature has low importance - model may not be meaningful")
        
        return best_model, results
    
    def test_predictions(self, df):
        """Test the model with some real examples"""
        print("\nTesting predictions on recent games...")
        
        # Get some recent games for testing
        test_games = df.tail(10)
        
        X_test = test_games[self.feature_names]
        y_actual = test_games['home_win']
        
        predictions = self.model.predict_proba(X_test)[:, 1]
        
        print("\nRecent Game Predictions:")
        for i, (_, game) in enumerate(test_games.iterrows()):
            pred = predictions[i]
            actual = y_actual.iloc[i]
            result = "✓" if (pred > 0.5) == (actual == 1) else "✗"
            
            print(f"  {game['away_team']} @ {game['home_team']}: {pred:.1%} home win (actual: {'W' if actual else 'L'}) {result}")
        
        accuracy = np.mean((predictions > 0.5) == y_actual)
        print(f"\nTest Accuracy: {accuracy:.1%}")
        print(f"Prediction Range: {predictions.min():.1%} - {predictions.max():.1%}")
    
    def save_model(self):
        """Save the working model"""
        os.makedirs(MODEL_PATH, exist_ok=True)
        
        model_package = {
            'model': self.model,
            'feature_names': self.feature_names,
            'training_date': datetime.now().isoformat(),
            'model_version': 'simple_v1.0',
            'model_type': type(self.model).__name__
        }
        
        model_file = os.path.join(MODEL_PATH, 'betting_model.pkl')
        with open(model_file, 'wb') as f:
            pickle.dump(model_package, f)
        
        print(f"Model saved to {model_file}")

def main():
    """Run the simple model training"""
    print("Training Simple Betting Model...")
    print("=" * 40)
    
    try:
        model = SimpleBettingModel()
        
        # Build dataset
        df = model.build_simple_dataset()
        
        # Train model
        trained_model, results = model.train_simple_model(df)
        
        # Test predictions
        model.test_predictions(df)
        
        # Save model
        model.save_model()
        
        print("\n" + "=" * 40)
        print("Simple Model Training Complete!")
        
        # Show what we actually learned
        rf_auc = results.get('RandomForest', {}).get('auc', 0)
        lr_auc = results.get('LogisticRegression', {}).get('auc', 0)
        
        print(f"RandomForest AUC: {rf_auc:.3f}")
        print(f"LogisticRegression AUC: {lr_auc:.3f}")
        print(f"Dataset size: {len(df)} games")
        
        if rf_auc > 0.6 or lr_auc > 0.6:
            print("✓ Model learned meaningful patterns")
        else:
            print("⚠ Model may not have learned strong patterns - consider more data")
            
    except Exception as e:
        print(f"Training failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()