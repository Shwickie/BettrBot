import pandas as pd, json, numpy as np
from sqlalchemy import create_engine, text

DB="sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine=create_engine(DB)

# bankroll
try:
    ua=json.load(open(r"E:\Bettr Bot\betting-bot\user_accounts.json","r"))
    users=list(ua.keys())
    BK=float(ua.get("admin", ua[users[0]]).get("bankroll",1000.0)) if users else 1000.0
except Exception:
    BK=1000.0

ABBR_TO_FULL={
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
def to_full(x): 
    s=str(x).strip()
    return ABBR_TO_FULL.get(s, s)

with engine.connect() as con:
    preds=pd.read_sql(text("SELECT * FROM ai_game_predictions"), con)
    odds=pd.read_sql(text("SELECT game_id, team, sportsbook, market, odds, timestamp FROM odds WHERE market='h2h'"), con)

if preds.empty or odds.empty:
    raise SystemExit("Need ai_game_predictions and h2h odds.")

# explode predictions to teams (full names)
rows=[]
for _,r in preds.iterrows():
    gid=r["game_id"]
    # matchup was 'Away @ Home' with full names
    if " @ " in str(r["matchup"]):
        away, home = r["matchup"].split(" @ ",1)
    else:
        away, home = ("Away", "Home")
    rows.append(dict(game_id=gid, team=home, model_prob=float(r["home_win_prob"])))
    rows.append(dict(game_id=gid, team=away, model_prob=float(r["away_win_prob"])))
model=pd.DataFrame(rows)

# merge by full team names
odds["team_full"]=odds["team"].apply(str)
model["team_full"]=model["team"].apply(str)
m=odds.merge(model[["game_id","team_full","model_prob"]], on=["game_id","team_full"], how="inner")

# decimal odds math
m["odds"] = m["odds"].astype(float)
m["implied_prob"] = 1.0 / m["odds"]
m["edge_pct"] = (m["model_prob"] - m["implied_prob"]) * 100.0

B = (m["odds"] - 1.0).clip(lower=0)
m["kelly_fraction"] = ((m["model_prob"]*(B+1) - 1.0) / B).replace([np.inf, -np.inf], 0.0).fillna(0.0)
m["kelly_fraction"] = m["kelly_fraction"].clip(lower=0, upper=0.10) * 0.25  # quarter Kelly, cap 10%
m["recommended_amount"] = (BK * m["kelly_fraction"]).clip(lower=0)

out = m[["game_id","team_full","sportsbook","odds","implied_prob","model_prob","edge_pct","recommended_amount"]].copy()
out = out.rename(columns={"team_full":"team"}).sort_values("edge_pct", ascending=False)

from sqlalchemy import types as T
with engine.begin() as con:
    con.exec_driver_sql("DROP TABLE IF EXISTS ai_betting_opportunities")
    out.to_sql("ai_betting_opportunities", con, index=False,
               dtype={"game_id":T.String(),"team":T.String(),"sportsbook":T.String(),"odds":T.Float()})
print("✅ ai_betting_opportunities ready. Bankroll used: $%.2f" % BK)