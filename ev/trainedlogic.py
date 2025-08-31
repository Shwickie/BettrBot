import pandas as pd, numpy as np
from sqlalchemy import create_engine, text

DB = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB)

# 1) Pull historical power diffs + outcomes (2020-2024)
with engine.connect() as con:
    hist = pd.read_sql(text("""
      SELECT season, game_id, power_diff,
             CASE WHEN home_score > away_score THEN 1 ELSE 0 END AS home_won
      FROM matchup_power_summary
      WHERE home_score IS NOT NULL AND away_score IS NOT NULL
        AND season BETWEEN 2020 AND 2024
        AND power_diff IS NOT NULL
    """), con)

if hist.empty:
    raise SystemExit("No historical games to train on (check matchup_power_summary).")

X = np.c_[np.ones(len(hist)), hist["power_diff"].values]  # intercept + power_diff
y = hist["home_won"].values.astype(float)

# 2) Logistic fit (Newton-Raphson)
w = np.zeros(2)
for _ in range(30):
    z = X @ w
    p = 1/(1+np.exp(-z))
    W = np.diag(p*(1-p) + 1e-9)
    H = X.T @ W @ X
    g = X.T @ (p - y)
    try:
        step = np.linalg.solve(H, g)
    except np.linalg.LinAlgError:
        break
    w -= step
b0, b1 = float(w[0]), float(w[1])

# 3) Save params
with engine.begin() as con:
    con.exec_driver_sql("DROP TABLE IF EXISTS ai_model_params")
    pd.DataFrame([{"model":"logit_power","b0":b0,"b1":b1}]).to_sql("ai_model_params", con, index=False)

# 4) Predict upcoming (scores null)
with engine.connect() as con:
    up = pd.read_sql(text("""
      SELECT game_id, game_date, home_team, away_team, power_diff
      FROM matchup_power_summary
      WHERE home_score IS NULL AND away_score IS NULL
    """), con)

def sigmoid(z): return 1/(1+np.exp(-z))
up["power_diff"] = up["power_diff"].fillna(0.0)
up["home_win_prob"] = sigmoid(b0 + b1*up["power_diff"])
up["away_win_prob"] = 1.0 - up["home_win_prob"]
up["pred_team_abbr"] = np.where(up["home_win_prob"]>=0.5, up["home_team"], up["away_team"])
ABBR_TO_FULL = {
  'ARI':'Arizona Cardinals','ATL':'Atlanta Falcons','BAL':'Baltimore Ravens','BUF':'Buffalo Bills',
  'CAR':'Carolina Panthers','CHI':'Chicago Bears','CIN':'Cincinnati Bengals','CLE':'Cleveland Browns',
  'DAL':'Dallas Cowboys','DEN':'Denver Broncos','DET':'Detroit Lions','GB':'Green Bay Packers',
  'HOU':'Houston Texans','IND':'Indianapolis Colts','JAX':'Jacksonville Jaguars','KC':'Kansas City Chiefs',
  'LV':'Las Vegas Raiders','LAC':'Los Angeles Chargers','LA':'Los Angeles Rams','LAR':'Los Angeles Rams',
  'MIA':'Miami Dolphins','MIN':'Minnesota Vikings','NE':'New England Patriots','NO':'New Orleans Saints',
  'NYG':'New York Giants','NYJ':'New York Jets','PHI':'Philadelphia Eagles','PIT':'Pittsburgh Steelers',
  'SF':'San Francisco 49ers','SEA':'Seattle Seahawks','TB':'Tampa Bay Buccaneers','TEN':'Tennessee Titans',
  'WAS':'Washington Commanders'
}
up["pred_team_full"] = up["pred_team_abbr"].map(ABBR_TO_FULL).fillna(up["pred_team_abbr"])
up["matchup"] = up.apply(lambda r: f"{ABBR_TO_FULL.get(r['away_team'], r['away_team'])} @ {ABBR_TO_FULL.get(r['home_team'], r['home_team'])}", axis=1)
up["confidence"] = (up["home_win_prob"] - 0.5).abs()*2  # 0-1

out = up[["game_id","game_date","matchup","pred_team_full","pred_team_abbr","home_win_prob","away_win_prob","confidence"]].copy()
out = out.rename(columns={"pred_team_full":"pred_team"})

with engine.begin() as con:
    con.exec_driver_sql("DROP TABLE IF EXISTS ai_game_predictions")
    out.to_sql("ai_game_predictions", con, index=False)

print("✅ Trained logit_power and wrote ai_model_params + ai_game_predictions.")