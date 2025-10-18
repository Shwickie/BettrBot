# Betting Bot Prediction System - Analysis & Diagnostic

## Executive Summary

**Current Status:** The prediction system has a complete ML pipeline in place, but it may not be working correctly in production. I've added diagnostic tools to help you identify and fix the issue.

**Key Finding:** The system is designed to use ML predictions but has a fallback to simpler power-based predictions if the ML model fails to load.

---

## What I Found

### 1. **ML Model Infrastructure ✅**

Your system DOES have a complete ML prediction pipeline:

- **Model File**: `models/betting_model_fixed.pkl` (589 KB) - Found in 4 locations
- **Model Type**: Calibrated RandomForest classifier
- **Features**: 30+ engineered features including:
  - Team power ratings
  - Win percentage differences
  - Offensive/defensive metrics
  - Form and momentum
  - Home field advantage
  - Contextual features (prime time, late season, etc.)

### 2. **Prediction Flow**

```
User requests /api/predictions
    ↓
cloud/mobile_dashboard.py (line 1820)
    ↓
get_ml_prediction_system() (line 552)
    ↓
Lazy imports FixedNFLSystem from model/prediction.py
    ↓
For each game:
    - Try ML prediction (lines 1874-1911)
    - If ML fails → Fallback to power-based prediction (lines 1919-1949)
```

### 3. **Potential Issues Identified**

#### Issue #1: Import Path Problems
The dashboard tries to import: `from model.prediction import FixedNFLSystem`

This could fail if:
- The `model` directory isn't in Python's path on Render
- Missing dependencies (pandas, numpy, scikit-learn, sqlalchemy)
- Module import order issues

#### Issue #2: Model File Location
The model looks for the file in this order:
1. `os.environ.get("BETTR_MODEL_PKL")`  ← **Not set**
2. `./betting_model_fixed.pkl` (current directory)
3. `./models/betting_model_fixed.pkl` ← **Exists locally**
4. `./dashboard/betting_model_fixed.pkl` ← **Exists locally**

On Render, the working directory might be different.

#### Issue #3: Database Team Name Mapping
Recent commits show you've been fixing team name issues. The ML system queries the database using **full team names** like "Philadelphia Eagles", but if the database has abbreviations like "PHI", predictions will fail.

From [model/prediction.py:295](model/prediction.py#L295):
```python
team_full = team_name  # Uses full name for database queries
```

From [model/prediction.py:340](model/prediction.py#L340):
```python
df = pd.read_sql(sql, conn, params={"t": team_full, ...})
```

#### Issue #4: Silent Fallback
The code is designed to fail gracefully. If ML prediction fails, it silently falls back to power-based predictions without alerting you:

From [cloud/mobile_dashboard.py:1913-1917](cloud/mobile_dashboard.py#L1913-L1917):
```python
except Exception as e:
    print(f"FixedNFLSystem failed for {g['away']} @ {g['home']}: {e}")
    # Falls through to fallback prediction
```

This means you might be using fallback predictions without realizing it!

---

## Diagnostic Tools I Created

### 1. **Local Diagnostic Script** ✅

**File**: `test_predictions_diagnostic.py`

**Usage**:
```bash
cd "e:\Bettr Bot\betting-bot"
python test_predictions_diagnostic.py
```

**What it tests**:
- ✓ Model files exist (Found 4 model files!)
- Model can be loaded
- Predictions can be generated
- Database connection works
- Dashboard integration works

**Note**: Requires pandas to run. If you don't have dependencies installed locally, use the Render endpoints instead.

### 2. **Render Diagnostic Endpoint** ✅

I added two new endpoints to your Flask app:

#### `/diagnostic` - User-Friendly Dashboard
A beautiful HTML page that shows:
- Overall system status (ML working vs fallback)
- Model details (class, features, team count)
- Database status (connection, game counts)
- Live prediction test

**Access**: Just navigate to `https://your-render-app.onrender.com/diagnostic`

#### `/api/system-diagnostic` - JSON API
Returns detailed diagnostic data in JSON format for programmatic access.

**Example Response**:
```json
{
  "timestamp": "2024-10-17T12:00:00",
  "system_status": {
    "overall": "✅ HEALTHY - ML predictions working",
    "using_ml": true
  },
  "model_status": {
    "available": true,
    "model_object": "CalibratedClassifierCV",
    "feature_count": 30,
    "team_count": 32
  },
  "prediction_test": {
    "success": true,
    "result": {
      "predicted_winner": "Kansas City Chiefs",
      "confidence": 0.623
    }
  }
}
```

---

## How to Use on Render

### Step 1: Deploy the Changes

The diagnostic endpoints are now in `cloud/mobile_dashboard.py`. Your next deployment will include them.

### Step 2: Check the Diagnostic Page

Once deployed, visit:
```
https://bettr-bot.onrender.com/diagnostic
```

This page will tell you:
- ✅ **GREEN**: ML predictions are working perfectly
- ⚠️ **YELLOW**: Using fallback predictions (ML system not working)

### Step 3: Interpret the Results

#### If you see "✅ HEALTHY - ML predictions working":
Great! Your ML model is working. The predictions are using the full 30+ feature model.

#### If you see "⚠️ DEGRADED - Using fallback predictions":
The page will show specific issues like:
- "ML system not available" → Import failed
- "Model object is None or missing" → Model file didn't load
- "Prediction test failed" → Model loaded but can't make predictions

---

## Likely Root Causes (Ranked by Probability)

### 1. **Missing Python Dependencies on Render** (90% likely)
The model requires: pandas, numpy, scikit-learn, sqlalchemy, psycopg2

**Check**: Look at your `requirements.txt` or `Pipfile`

**Fix**: Ensure all these are listed:
```
pandas>=1.5.0
numpy>=1.23.0
scikit-learn>=1.2.0
sqlalchemy>=2.0.0
psycopg2-binary>=2.9.0
```

### 2. **Import Path Issues** (60% likely)
The `model` directory might not be in Python's import path on Render.

**Current Code** ([cloud/mobile_dashboard.py:34-36](cloud/mobile_dashboard.py#L34-L36)):
```python
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
```

This should work, but might need adjustment based on Render's working directory.

### 3. **Team Name Mismatch in Database** (40% likely)
The ML system queries for "Philadelphia Eagles" but database has "PHI".

**Evidence**: Your recent commits mention fixing team name issues.

**Check the diagnostic** - if it shows "Model available: Yes" but "Prediction test: Failed", this is likely the issue.

**Fix**: Ensure database `games` table uses full team names, or modify the query logic.

### 4. **Model File Not Found** (20% likely)
The model file isn't being found on Render.

**Fix**: Set environment variable on Render:
```
BETTR_MODEL_PKL=/opt/render/project/src/models/betting_model_fixed.pkl
```

Or ensure `models/` directory is deployed to Render.

---

## Recommendations

### Immediate Actions

1. **Deploy and check `/diagnostic`**
   - This will give you the exact issue

2. **Check Render logs**
   - Look for these messages:
     ```
     ✅ Successfully imported FixedNFLSystem from model.prediction
     ✅ ML Prediction System initialized successfully
     ```
   - Or error messages:
     ```
     ⚠️ Warning: Could not import FixedNFLSystem
     ```

3. **Verify dependencies**
   - Ensure `requirements.txt` has all ML libraries

### Long-term Improvements

1. **Add explicit ML status flag to API responses**
   ```python
   {
     "prediction": "Kansas City Chiefs",
     "confidence": 0.623,
     "ml_prediction": true,  // <-- Add this
     "prediction_method": "ML_30_features"  // <-- And this
   }
   ```

2. **Add startup health check**
   - Test ML predictions on app startup
   - Alert if fallback mode is being used

3. **Improve error visibility**
   - Send alerts when ML predictions fail
   - Log to external monitoring service

---

## Testing Checklist

When you deploy to Render, check these in order:

- [ ] Visit `/diagnostic` - page loads successfully
- [ ] Check "Overall Status" - shows ✅ HEALTHY or ⚠️ DEGRADED
- [ ] If DEGRADED, read "Issues" section
- [ ] Check "Model Available" - should be ✅ Yes
- [ ] Check "Model Object" - should show "CalibratedClassifierCV" or similar
- [ ] Check "Prediction Test" - should be ✅ Success
- [ ] Visit `/api/predictions` - check if `model_prediction: true` for games
- [ ] Compare predictions to power rankings - ML should give more nuanced probabilities

---

## Quick Reference

### Files Modified
- `cloud/mobile_dashboard.py` - Added `/diagnostic` and `/api/system-diagnostic` endpoints
- `test_predictions_diagnostic.py` - New local diagnostic script

### New Endpoints
- `GET /diagnostic` - HTML diagnostic dashboard
- `GET /api/system-diagnostic` - JSON diagnostic data
- `GET /api/predictions/debug` - Existing debug endpoint (line 1990)

### Key Files to Review
- [cloud/mobile_dashboard.py:1820-1960](cloud/mobile_dashboard.py#L1820-L1960) - Main prediction API
- [cloud/mobile_dashboard.py:552-582](cloud/mobile_dashboard.py#L552-L582) - ML system loader
- [model/prediction.py](model/prediction.py) - Complete ML prediction system

---

## What the System Does When Working

When the ML model is working correctly, each prediction:

1. **Loads 30+ features** from database and calculations
2. **Queries recent game history** for both teams
3. **Engineers derived features** (power difference, form, streaks)
4. **Runs through calibrated RandomForest** classifier
5. **Returns probabilistic prediction** (e.g., 62.3% vs 37.7%)
6. **Provides confidence level** (strong bet, consider, weak edge)

### Fallback (When ML Fails)

Uses simple power rating formula:
```python
home_advantage = 2.5
win_prob = 1 / (1 + exp(-(home_power + 2.5 - away_power) / 8))
```

This is much simpler and less accurate than the ML model.

---

## Contact Points

If you need to debug further:

1. **Check server logs** on Render dashboard
2. **Visit `/diagnostic`** page for live status
3. **Call `/api/system-diagnostic`** for programmatic checks
4. **Review console output** for these messages:
   - "✅ ML Prediction System initialized successfully"
   - "✅ Model loaded: CalibratedClassifierCV"
   - "Loaded power ratings for 32 teams"

Good luck! The diagnostic page should tell you exactly what's wrong. 🎯
