# Issues Found & Recommended Fixes

## CURRENT STATUS

### Model Verification ✅
- **Model Type:** RandomForestClassifier
- **Training Date:** September 11, 2025
- **Features:** 19 features
- **Version:** fixed_cloud_deployment
- **Status:** ✅ LOADED AND WORKING

### Current Betting Parameters
**File:** `model/train_betting_model.py` and `model/prediction.py`

```python
MIN_EDGE            = 0.025  # 2.5% minimum edge
MIN_CONF            = 0.55   # 55% minimum confidence
MIN_ABS_PROB_TO_BET = 0.54   # 54% minimum probability
```

---

## CRITICAL ISSUES TO FIX

### Issue #1: MIN_CONF Too Low (55%) 🔴

**Problem:**
- Your testing shows that picks with <58% confidence lose money (ROI: -9.3%)
- Current setting (55%) allows unprofitable bets

**Current Performance by Confidence:**
- 70%+: 100% win rate, +90.9% ROI ✅
- 65-70%: 85.7% win rate, +63.6% ROI ✅
- 58-65%: 71.7% win rate, +37.0% ROI ✅
- 52-58%: 51.2% win rate, -2.2% ROI ❌
- <52%: 44.4% win rate, -15.2% ROI ❌

**Solution:**
Change `MIN_CONF` from 0.55 to **0.58**

**Files to update:**
1. `model/train_betting_model.py` line 36
2. `model/prediction.py` (if betting logic is there)
3. `cloud/train_betting_model.py` line 31
4. `dashboard/ai_chat_stub.py` line 451 (change from 0.52 to 0.58)
5. `cloud/ai_chat_stub.py` line 1569 (change from 0.52 to 0.58)

---

### Issue #2: Database Connection Timeouts 🔴

**Problem:**
```
connection not available and request was dropped from queue after 10000ms
```

**Impact:** 3 out of 150 games failed to get predictions

**Solution:**

**Option A - Increase Connection Pool (Recommended)**
```python
# In model/prediction.py or wherever engine is created
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,          # Increase from default (5)
    max_overflow=20,       # Allow temporary connections
    pool_recycle=3600,     # Recycle connections every hour
    connect_args={'connect_timeout': 30}  # Increase timeout
)
```

**Option B - Add Retry Logic**
```python
from tenacity import retry, wait_exponential, stop_after_attempt

@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(3))
def get_team_data():
    # Your database query here
    pass
```

**Files to update:**
- `model/prediction.py` (engine initialization around line 50-60)

---

### Issue #3: Team Name Inconsistency ⚠️

**Problem:**
Database contains "Philadelphia Eagles" instead of "PHI"

**Impact:**
- Model may fail to match team names
- Inconsistent data lookups

**Solution:**
Run SQL update to standardize all team names:

```sql
UPDATE games SET home_team = 'PHI' WHERE home_team = 'Philadelphia Eagles';
UPDATE games SET away_team = 'PHI' WHERE away_team = 'Philadelphia Eagles';
UPDATE games SET home_team = 'KC' WHERE home_team = 'Kansas City Chiefs';
UPDATE games SET away_team = 'KC' WHERE away_team = 'Kansas City Chiefs';
-- Repeat for all 32 teams
```

**Or use Python script:**
```python
# Create standardization mapping
TEAM_MAPPING = {
    'Philadelphia Eagles': 'PHI',
    'Kansas City Chiefs': 'KC',
    'Buffalo Bills': 'BUF',
    # ... etc for all 32 teams
}

# Update database
for full_name, abbrev in TEAM_MAPPING.items():
    conn.execute(text(f"UPDATE games SET home_team = '{abbrev}' WHERE home_team = '{full_name}'"))
    conn.execute(text(f"UPDATE games SET away_team = '{abbrev}' WHERE away_team = '{full_name}'"))
```

---

### Issue #4: Scikit-learn Version Mismatch ⚠️

**Problem:**
```
InconsistentVersionWarning: Trying to unpickle estimator from version 1.7.1 when using version 1.7.2
```

**Impact:**
- May cause unpredictable behavior
- Model could give inconsistent results

**Solution:**
Either:
1. **Downgrade scikit-learn** to match training version
   ```bash
   pip install scikit-learn==1.7.1
   ```

2. **Retrain model** with current version (Recommended)
   ```bash
   python model/train_betting_model.py
   ```

---

## RECOMMENDED IMPROVEMENTS

### Improvement #1: Add Bet Logging System 💡

**Why:** Track actual performance vs predictions

**Implementation:**

**Step 1: Create Database Table**
```sql
CREATE TABLE bets_placed (
    bet_id SERIAL PRIMARY KEY,
    game_id INTEGER,
    placed_at TIMESTAMP DEFAULT NOW(),
    home_team VARCHAR(10),
    away_team VARCHAR(10),
    predicted_winner VARCHAR(10),
    confidence FLOAT,
    home_win_prob FLOAT,
    bet_amount FLOAT,
    odds FLOAT,

    -- Results (filled in after game)
    actual_winner VARCHAR(10),
    result VARCHAR(10),  -- 'win', 'loss', 'pending'
    profit_loss FLOAT,

    -- Metadata
    model_version VARCHAR(50),
    min_edge_used FLOAT,
    min_conf_used FLOAT
);
```

**Step 2: Add Logging to Prediction Code**
```python
def log_bet(game_id, prediction, bet_amount, odds):
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO bets_placed
            (game_id, home_team, away_team, predicted_winner,
             confidence, home_win_prob, bet_amount, odds, model_version)
            VALUES
            (:game_id, :home_team, :away_team, :predicted_winner,
             :confidence, :home_win_prob, :bet_amount, :odds, :model_version)
        """), {
            'game_id': game_id,
            'home_team': prediction['home_team'],
            'away_team': prediction['away_team'],
            'predicted_winner': prediction['predicted_winner'],
            'confidence': prediction['confidence'],
            'home_win_prob': prediction['home_win_probability'],
            'bet_amount': bet_amount,
            'odds': odds,
            'model_version': 'fixed_cloud_deployment'
        })
        conn.commit()
```

---

### Improvement #2: Implement Confidence-Based Bet Sizing 💡

**Why:** Bet more on high-confidence picks, less on medium

**Implementation:**

```python
def calculate_bet_size(bankroll, confidence, kelly_fraction=0.25):
    """
    Scale bet size based on confidence level

    Args:
        bankroll: Total bankroll
        confidence: Model confidence (0.0-1.0)
        kelly_fraction: Fraction of Kelly to use (0.25 = conservative)

    Returns:
        bet_size: Amount to bet
    """

    # Base percentages by confidence tier
    if confidence >= 0.70:
        base_pct = 0.020  # 2.0% of bankroll
    elif confidence >= 0.65:
        base_pct = 0.015  # 1.5% of bankroll
    elif confidence >= 0.58:
        base_pct = 0.010  # 1.0% of bankroll
    else:
        return 0  # Don't bet

    bet_size = bankroll * base_pct

    # Apply max bet cap
    MAX_BET = 50  # Never bet more than $50
    return min(bet_size, MAX_BET)

# Example usage:
bankroll = 1000
confidence = 0.68

bet = calculate_bet_size(bankroll, confidence)
print(f"Bet ${bet:.2f} on this {confidence*100:.1f}% confidence pick")
# Output: Bet $15.00 on this 68.0% confidence pick
```

---

### Improvement #3: Add Real Odds Integration 💡

**Why:** Currently testing assumes all games are -110 odds, but real odds vary

**Implementation:**

```python
def fetch_current_odds(game_id):
    """Fetch latest odds from database"""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT team, AVG(odds) as avg_odds
            FROM odds
            WHERE game_id = :game_id
            AND market = 'h2h'
            AND timestamp > NOW() - INTERVAL '1 hour'
            GROUP BY team
        """), {'game_id': game_id})

        odds_dict = {}
        for row in result:
            odds_dict[row.team] = row.avg_odds

        return odds_dict

def calculate_true_edge(model_prob, market_odds):
    """
    Calculate true edge based on real odds

    Args:
        model_prob: Your model's win probability
        market_odds: American odds from sportsbook

    Returns:
        edge: Your edge over the market
    """
    # Convert American odds to implied probability
    if market_odds > 0:
        implied_prob = 100 / (market_odds + 100)
    else:
        implied_prob = abs(market_odds) / (abs(market_odds) + 100)

    # Your edge
    edge = model_prob - implied_prob

    return edge

# Example usage:
model_prob = 0.68  # 68% chance home team wins
market_odds = -150  # Home team is -150 favorite

edge = calculate_true_edge(model_prob, market_odds)
print(f"Edge: {edge*100:.2f}%")

if edge >= 0.03:  # 3% minimum edge
    print("BET!")
else:
    print("SKIP - not enough edge")
```

---

### Improvement #4: Add Performance Dashboard 💡

**Why:** Visualize model performance over time

**Implementation:**

Create `performance_dashboard.py`:

```python
import pandas as pd
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt

DATABASE_URL = "your_database_url"
engine = create_engine(DATABASE_URL)

# Fetch bet history
query = text("""
    SELECT
        placed_at::date as bet_date,
        COUNT(*) as num_bets,
        SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) as wins,
        SUM(profit_loss) as total_profit,
        AVG(confidence) as avg_confidence
    FROM bets_placed
    WHERE result IS NOT NULL
    GROUP BY bet_date
    ORDER BY bet_date
""")

df = pd.read_sql(query, engine)

# Calculate metrics
df['win_rate'] = df['wins'] / df['num_bets']
df['cumulative_profit'] = df['total_profit'].cumsum()

# Plot
fig, axes = plt.subplots(2, 2, figsize=(15, 10))

# Win rate over time
axes[0, 0].plot(df['bet_date'], df['win_rate'])
axes[0, 0].axhline(y=0.5238, color='r', linestyle='--', label='Break-even')
axes[0, 0].set_title('Win Rate Over Time')
axes[0, 0].set_ylabel('Win Rate')
axes[0, 0].legend()

# Cumulative profit
axes[0, 1].plot(df['bet_date'], df['cumulative_profit'])
axes[0, 1].axhline(y=0, color='r', linestyle='--')
axes[0, 1].set_title('Cumulative Profit')
axes[0, 1].set_ylabel('Profit ($)')

# Bets per day
axes[1, 0].bar(df['bet_date'], df['num_bets'])
axes[1, 0].set_title('Bets Placed Per Day')
axes[1, 0].set_ylabel('Number of Bets')

# Confidence distribution
axes[1, 1].hist(df['avg_confidence'], bins=20)
axes[1, 1].set_title('Confidence Distribution')
axes[1, 1].set_xlabel('Confidence')

plt.tight_layout()
plt.savefig('performance_dashboard.png')
print("Dashboard saved to performance_dashboard.png")
```

---

## IMMEDIATE ACTION PLAN

### Step 1: Fix Critical Issues (DO NOW)

1. **Update MIN_CONF to 0.58**
   - Edit `model/train_betting_model.py` line 36
   - Edit `dashboard/ai_chat_stub.py` line 451
   - Edit `cloud/ai_chat_stub.py` line 1569

2. **Fix Database Connection Pooling**
   - Edit `model/prediction.py`
   - Add pool configuration

3. **Standardize Team Names**
   - Run SQL update script or Python standardization

### Step 2: Test Changes

```bash
# After making changes, re-run the test
python test_my_model.py
```

Expected improvement:
- Win rate should stay ~59% but on fewer games
- ROI should increase (filtering out losing bets)
- Fewer total bets, but all should be profitable

### Step 3: Deploy to Production

Once testing confirms improvements:
1. Commit changes to git
2. Deploy updated model
3. Start betting with high confidence picks only
4. Monitor results

---

## MONITORING CHECKLIST

After deploying fixes, monitor these metrics weekly:

- [ ] Win rate stays above 55%
- [ ] High confidence picks (65%+) maintain 80%+ win rate
- [ ] No database connection errors
- [ ] All team names mapping correctly
- [ ] ROI remains positive
- [ ] Bankroll growing steadily

---

## SUMMARY

**Critical Fixes Needed:**
1. ✅ Increase MIN_CONF from 0.55 → 0.58 (filters out losing bets)
2. ✅ Fix database connection pooling (prevents errors)
3. ✅ Standardize team names (consistency)

**Nice-to-Have Improvements:**
4. Add bet logging system (track performance)
5. Implement confidence-based bet sizing (optimize returns)
6. Integrate real odds (calculate true edge)
7. Create performance dashboard (visualization)

**Expected Impact:**
- Fewer bets placed (good - quality over quantity)
- Higher win rate on remaining bets
- Better ROI (cutting losing bets)
- More stable, predictable returns

Your model is **already profitable** - these fixes will make it **even better**.
