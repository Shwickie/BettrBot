# Testing Guide - Prediction System Diagnostic

## Quick Start

### On Render (Production)

1. **Deploy your latest code** to Render
2. **Visit the diagnostic page**:
   ```
   https://your-app.onrender.com/diagnostic
   ```
3. **Check the status**:
   - ✅ GREEN = ML predictions working
   - ⚠️ YELLOW = Using fallback (something's wrong)

That's it! The page will tell you exactly what's working and what's not.

---

## Detailed Testing

### 1. Visual Diagnostic Dashboard

**URL**: `/diagnostic`

**What you'll see**:
```
┌─────────────────────────────────────────┐
│ 🔍 Bettr Bot System Diagnostic          │
├─────────────────────────────────────────┤
│ ✅ HEALTHY - ML predictions working     │
│ Using ML Predictions: ✅ Yes            │
│ Last checked: 10/17/2024, 2:30:15 PM   │
├─────────────────────────────────────────┤
│ 🤖 ML Model Status                      │
│ Model Available: ✅ Yes                 │
│ Model Object: CalibratedClassifierCV    │
│ Feature Count: 30 features              │
│ Teams Loaded: 32 teams                  │
│ Top Team: Kansas City Chiefs (12.5)    │
├─────────────────────────────────────────┤
│ 💾 Database Status                      │
│ Connected: ✅ Yes                       │
│ Total Games: 5,234                      │
│ Upcoming Games: 14                      │
├─────────────────────────────────────────┤
│ 🎯 Prediction Test                      │
│ ✅ Test Prediction Successful           │
│ Predicted Winner: Kansas City Chiefs    │
│ Confidence: 62.3%                       │
│ KC Win Prob: 62.3%                      │
│ BUF Win Prob: 37.7%                     │
└─────────────────────────────────────────┘
```

### 2. JSON API Endpoint

**URL**: `/api/system-diagnostic`

**Returns**:
```json
{
  "timestamp": "2024-10-17T14:30:15.123456",
  "system_status": {
    "overall": "✅ HEALTHY - ML predictions working",
    "using_ml": true,
    "issues": []
  },
  "model_status": {
    "available": true,
    "class": "FixedNFLSystem",
    "model_object": "CalibratedClassifierCV",
    "has_predict_proba": true,
    "feature_count": 30,
    "features": ["home_wpct_pre", "away_wpct_pre", ...],
    "team_count": 32,
    "top_team": {
      "name": "KC",
      "power": 12.5
    }
  },
  "database_status": {
    "connected": true,
    "total_games": 5234,
    "upcoming_games": 14
  },
  "prediction_test": {
    "success": true,
    "result": {
      "predicted_winner": "Kansas City Chiefs",
      "confidence": 0.623,
      "home_win_prob": 0.623,
      "away_win_prob": 0.377,
      "power_difference": 3.7
    }
  }
}
```

### 3. Check Actual Predictions

**URL**: `/api/predictions`

Look for the `model_prediction` field:
```json
{
  "matchup": "Buffalo Bills @ Kansas City Chiefs",
  "prediction": "Kansas City Chiefs",
  "confidence": 0.623,
  "model_prediction": true,  // <-- Should be true!
  "feature_count": 30        // <-- Should show 30 features
}
```

If `model_prediction: false`, you're using the fallback.

---

## Common Issues and Solutions

### Issue: "ML system not available"

**Cause**: Import failed - can't import `FixedNFLSystem`

**Solutions**:
1. Check Render logs for import errors
2. Verify `requirements.txt` has:
   ```
   pandas>=1.5.0
   numpy>=1.23.0
   scikit-learn>=1.2.0
   sqlalchemy>=2.0.0
   psycopg2-binary>=2.9.0
   ```
3. Ensure `model/` directory is deployed

**How to verify**:
- Check build logs on Render
- Look for "✅ Successfully imported FixedNFLSystem" in logs

### Issue: "Model object is None or missing"

**Cause**: Model file didn't load

**Solutions**:
1. Verify `models/betting_model_fixed.pkl` exists in deployment
2. Set environment variable on Render:
   ```
   BETTR_MODEL_PKL=/opt/render/project/src/models/betting_model_fixed.pkl
   ```
3. Check file permissions

**How to verify**:
- Look for "✅ Model loaded: CalibratedClassifierCV" in logs
- Check `/diagnostic` page - Model Object field

### Issue: "Prediction test failed"

**Cause**: Model loaded but can't make predictions (likely database issue)

**Solutions**:
1. **Team name mismatch**: Ensure database uses full names
   - Database should have "Kansas City Chiefs", not "KC"
   - Or modify queries to use abbreviations
2. **Missing data**: No historical games in database
3. **Database connection**: Check DATABASE_URL env variable

**How to verify**:
- Check error message on `/diagnostic` page
- Look for detailed traceback in "Prediction Test" section

---

## Comparing ML vs Fallback

### ML Predictions (Good ✅)
```json
{
  "prediction": "Kansas City Chiefs",
  "confidence": 0.623,
  "model_prediction": true,
  "home_win_prob": 0.623,
  "away_win_prob": 0.377,
  "feature_count": 30,
  "key_factors": {
    "power_diff": 3.7,
    "win_pct_diff": 0.15,
    "offense_diff": 2.3,
    "form_diff": 0.12
  }
}
```

**Characteristics**:
- `model_prediction: true`
- Has `feature_count: 30`
- Provides `key_factors` breakdown
- Probabilities are nuanced (not round numbers)

### Fallback Predictions (Not Ideal ⚠️)
```json
{
  "prediction": "Kansas City Chiefs",
  "confidence": 0.65,
  "model_prediction": false,
  "home_win_prob": 0.65,
  "away_win_prob": 0.35,
  "power_difference": 0,
  "key_factors": {}
}
```

**Characteristics**:
- `model_prediction: false`
- No `feature_count`
- Empty `key_factors`
- `power_difference: 0` (placeholder)
- Less accurate probabilities

---

## Testing Checklist

Before going live with predictions:

- [ ] Visit `/diagnostic` - page loads
- [ ] Overall status is ✅ HEALTHY
- [ ] Model Available shows ✅ Yes
- [ ] Model Object is NOT "None"
- [ ] Feature Count shows ~30 features
- [ ] Teams Loaded shows 32 teams
- [ ] Prediction Test shows ✅ Success
- [ ] Visit `/api/predictions`
- [ ] Check games have `model_prediction: true`
- [ ] Verify predictions make sense (not all 50/50)

---

## Monitoring in Production

### Startup Checks

When your app starts on Render, look for these log messages:

```
✅ Successfully imported FixedNFLSystem from model.prediction
🔄 Initializing ML Prediction System...
Initializing database engine: postgresql+psycopg2://...
✅ Database engine initialized
Loading model from: /opt/render/project/src/models/betting_model_fixed.pkl
  Model data keys: ['model', 'scaler', 'feature_cols', 'model_metrics', 'uses_scaled']
  Features: 30
  Model type: CalibratedClassifierCV
  Has predict_proba: True
✅ SUCCESS: Loaded model pack
Loaded power ratings for 32 teams
  Best: KC (12.5)
  Worst: CAR (-10.2)
✅ ML Prediction System initialized successfully
  Model AUC: 0.58
  ✅ Model loaded: CalibratedClassifierCV
```

### During Operation

Check logs for prediction attempts:

**Success**:
```
🔍 Attempting ML prediction: Buffalo Bills @ Kansas City Chiefs
✅ ML prediction succeeded for Buffalo Bills @ Kansas City Chiefs
```

**Failure** (falls back):
```
🔍 Attempting ML prediction: Buffalo Bills @ Kansas City Chiefs
FixedNFLSystem failed for BUF @ KC: [error message]
```

---

## Automated Testing Script

If you want to test locally (requires dependencies):

```bash
cd "e:\Bettr Bot\betting-bot"
python test_predictions_diagnostic.py
```

**Output**:
```
================================================================================
BETTING BOT PREDICTION SYSTEM DIAGNOSTIC
================================================================================

[TEST 1] Checking for model files...
✅ FOUND models/betting_model_fixed.pkl (589.1 KB)

[TEST 2] Importing prediction system...
✅ Successfully imported FixedNFLSystem

[TEST 3] Initializing prediction system...
✅ Successfully initialized FixedNFLSystem

[TEST 4] Checking model data...
✅ model_data exists
✅ Model object exists: CalibratedClassifierCV
✅ Feature columns: 30 features

[TEST 5] Checking database connection...
✅ Database engine initialized
   Games in database: 5234

[TEST 6] Checking team power data...
✅ Team power data loaded: 32 teams
   Top team: KC (12.5)

[TEST 7] Testing prediction...
✅ Prediction successful!
   Predicted winner: Kansas City Chiefs
   Confidence: 62.3%

[TEST 8] Testing dashboard integration...
✅ Dashboard can access ML system
✅ Dashboard ML system can make predictions

[TEST 9] Testing Flask app import...
✅ Flask app imported successfully

================================================================================
✅ ALL TESTS PASSED - Prediction system is working!
================================================================================
```

---

## Quick Fixes

### If you see degraded status:

1. **First**: Check which specific check failed on `/diagnostic`
2. **Second**: Look at server logs on Render
3. **Third**: Check environment variables
4. **Fourth**: Verify database has full team names

### Emergency fallback:

If ML won't work and you need predictions now:
- The fallback predictions are still better than nothing
- They use power ratings (which are accurate)
- Just less sophisticated than ML

### Getting help:

If stuck:
1. Take screenshot of `/diagnostic` page
2. Copy error from "Prediction Test" section
3. Check Render build logs for errors
4. Verify all dependencies are installed

---

## Success Criteria

You'll know it's working when:

1. `/diagnostic` shows "✅ HEALTHY - ML predictions working"
2. All checks on diagnostic page are green
3. `/api/predictions` returns `model_prediction: true`
4. Predictions vary in confidence (not all 50/50)
5. Key factors are populated for each game

---

## Next Steps

Once confirmed working:

1. **Monitor accuracy**: Track prediction vs actual results
2. **Compare to Vegas**: Are ML predictions better than market?
3. **Tune model**: Retrain with more recent data
4. **Add features**: Weather, injuries, etc.
5. **Optimize**: Cache predictions, reduce latency

Good luck! 🎯
