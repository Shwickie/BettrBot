import sqlite3, pandas as pd, numpy as np, math
from datetime import date, timedelta
DB = r"E:\Bettr Bot\betting-bot\data\betting.db"
con = sqlite3.connect(DB)

def q(sql, params=None):
    return pd.read_sql_query(sql, con, params=params or [])

def banner(t): print("\n"+"="*8+" "+t+" "+"="*8)

today = pd.Timestamp.utcnow().date()
horizon = today + pd.Timedelta(days=30)

# 1) GAMES UPCOMING
banner("GAMES UPCOMING (30d)")
games = q("""
  SELECT game_id, away_team AS away, home_team AS home, game_date
  FROM games
  WHERE date(game_date) BETWEEN date(?) AND date(?)
  ORDER BY game_date
""", [str(today), str(horizon)])
print(games.head(12))
print(f"Upcoming games (30d): {len(games)}")

# 2) ODDS COVERAGE (h2h latest per game/team/book)
banner("ODDS COVERAGE (h2h latest)")
if len(games):
    ph = ",".join(["?"]*len(games["game_id"]))
    odds = q(f"""
      SELECT o.game_id, o.team, o.sportsbook, o.odds, o.timestamp
      FROM odds o
      JOIN (
        SELECT game_id, team, sportsbook, MAX(timestamp) AS ts
        FROM odds
        WHERE market='h2h' AND game_id IN ({ph})
        GROUP BY game_id, team, sportsbook
      ) x ON x.game_id=o.game_id AND x.team=o.team AND x.sportsbook=o.sportsbook AND x.ts=o.timestamp
    """, list(games["game_id"]))
else:
    odds = pd.DataFrame(columns=["game_id","team","sportsbook","odds","timestamp"])
print(odds.head(12))

# Coverage by game: need at least one price per side
cov = (odds.groupby(["game_id","team"]).size().reset_index(name="n")
            .groupby("game_id")["team"].nunique().reset_index(name="sides"))
need = games[["game_id"]].merge(cov, on="game_id", how="left").fillna({"sides":0})
missing = need[need["sides"]<2]
print(f"Games with both sides priced: {(need['sides']>=2).sum()} / {len(need)}")
if not missing.empty:
    print("Games missing lines:", len(missing))
    print(missing.head(10))

# 3) AI PREDICTIONS sanity
banner("AI PREDICTIONS")
try:
    preds = q("SELECT * FROM ai_game_predictions ORDER BY game_date")
    print(preds[["matchup","pred_team","home_win_prob","away_win_prob","confidence"]].head(12))
    if not preds.empty:
        badp = preds[(preds["home_win_prob"]<0)|(preds["home_win_prob"]>1)|(preds["away_win_prob"]<0)|(preds["away_win_prob"]>1)]
        print("Prob sanity OK" if badp.empty else f"WARNING: {len(badp)} rows with probs outside [0,1]")
        # Coverage vs games
        miss = set(games["game_id"]) - set(preds["game_id"])
        print("Coverage OK" if not miss else f"Missing predictions for {len(miss)} upcoming games")
except Exception as e:
    print("No ai_game_predictions (ok if not trained yet):", e)

# 4) EV OPPORTUNITIES sanity
banner("EV OPPORTUNITIES")
try:
    ev = q("SELECT * FROM ai_betting_opportunities ORDER BY edge_pct DESC")
    print(ev.head(12))
    if not ev.empty:
        neg = ev[ev["edge_pct"]<-50]
        if not neg.empty:
            print(f"WARNING: {len(neg)} rows with very negative edge (<-50%)")
        dec = ev["odds"].astype(float)
        if (dec<1.01).any():
            print("WARNING: some decimal odds < 1.01 (bad data)")
except Exception as e:
    print("No ai_betting_opportunities:", e)

# 5) QUICK 2024 VALIDATION for your logistic (uses ai_model_params if present)
banner("2024 VALIDATION (home/away from power_diff)")
try:
    p = q("SELECT b0,b1 FROM ai_model_params WHERE model='logit_power'")
    if not p.empty:
        b0,b1 = float(p.iloc[0]["b0"]), float(p.iloc[0]["b1"])
        hist = q("""
          SELECT season, power_diff,
                 CASE WHEN home_score>away_score THEN 1 ELSE 0 END AS home_won
          FROM matchup_power_summary
          WHERE season=2024 AND home_score IS NOT NULL AND away_score IS NOT NULL
        """)
        if not hist.empty:
            z = b0 + b1*hist["power_diff"].fillna(0).values
            ph = 1/(1+np.exp(-z))
            pick = (ph>=0.5).astype(int)
            y = hist["home_won"].values
            acc = (pick==y).mean()
            brier = np.mean((ph - y)**2)
            print(f"2024 n={len(hist)}  acc={acc:.3f}  brier={brier:.3f}  (baseline brier?0.25)")
        else:
            print("No finished 2024 games for validation.")
    else:
        print("No ai_model_params found.")
except Exception as e:
    print("Validation error:", e)

con.close()
print("\n? Validation complete.")
