# ai_validate_and_pack.py
import sqlite3, pandas as pd, numpy as np, json, os
from datetime import datetime, timedelta

DB = r"E:\Bettr Bot\betting-bot\data\betting.db"
OUT_DIR = r"E:\Bettr Bot\betting-bot\dashboard\cloud_export"
os.makedirs(OUT_DIR, exist_ok=True)

REQUIRED = [
    "games","odds","team_season_summary",
    "ai_game_predictions","ai_betting_opportunities","ai_injury_validation_detail"
]

def to_full(abbr):
    m = {
        'ARI':'Arizona Cardinals','ATL':'Atlanta Falcons','BAL':'Baltimore Ravens','BUF':'Buffalo Bills',
        'CAR':'Carolina Panthers','CHI':'Chicago Bears','CIN':'Cincinnati Bengals','CLE':'Cleveland Browns',
        'DAL':'Dallas Cowboys','DEN':'Denver Broncos','DET':'Detroit Lions','GB':'Green Bay Packers',
        'HOU':'Houston Texans','IND':'Indianapolis Colts','JAX':'Jacksonville Jaguars','KC':'Kansas City Chiefs',
        'LV':'Las Vegas Raiders','LAC':'Los Angeles Chargers','LAR':'Los Angeles Rams','MIA':'Miami Dolphins',
        'MIN':'Minnesota Vikings','NE':'New England Patriots','NO':'New Orleans Saints','NYG':'New York Giants',
        'NYJ':'New York Jets','PHI':'Philadelphia Eagles','PIT':'Pittsburgh Steelers','SF':'San Francisco 49ers',
        'SEA':'Seattle Seahawks','TB':'Tampa Bay Buccaneers','TEN':'Tennessee Titans','WAS':'Washington Commanders'
    }
    return m.get(abbr, abbr)

def am_from_decimal(d):
    d = float(d)
    return int(round((d-1)*100)) if d>=2 else -int(round(100/(d-1)))

def american_to_dec(od):
    od=float(od)
    return 1+(od/100.0) if od>0 else 1+(100.0/abs(od))

def run():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row

    # 0) schema
    got = set(pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", con)["name"].tolist())
    missing = [t for t in REQUIRED if t not in got]
    print("Tables missing:", missing if missing else "None")

    # 1) recency
    ts = pd.read_sql("SELECT MAX(timestamp) ts FROM odds", con)["ts"].iloc[0]
    last_ts = pd.to_datetime(ts) if ts else None
    print("Latest odds timestamp:", last_ts)

    # 2) upcoming games (21 days)
    today = datetime.utcnow().date()
    horizon = today + timedelta(days=21)
    games = pd.read_sql("""
      SELECT game_id, away_team, home_team, game_date, start_time_local
      FROM games WHERE date(game_date) BETWEEN date(?) AND date(?)
      ORDER BY game_date, start_time_local
    """, con, params=[today, horizon])
    print("Upcoming games (21d):", len(games))

    # 3) h2h coverage (both sides priced)
    if not games.empty:
        ph = ",".join(["?"]*len(games))
        odds = pd.read_sql(f"""
          SELECT o.game_id, o.team, o.sportsbook, o.odds, o.timestamp
          FROM odds o JOIN (
            SELECT game_id, team, sportsbook, MAX(timestamp) ts
            FROM odds WHERE market='h2h' AND game_id IN ({ph})
            GROUP BY game_id, team, sportsbook
          ) x ON x.game_id=o.game_id AND x.team=o.team AND x.sportsbook=o.sportsbook AND x.ts=o.timestamp
        """, con, params=list(games["game_id"]))
    else:
        odds = pd.DataFrame(columns=["game_id","team","sportsbook","odds","timestamp"])
    sides = (odds.groupby(["game_id","team"]).size().reset_index(name="n")
                  .groupby("game_id")["team"].nunique().reset_index(name="sides"))
    cov = games[["game_id"]].merge(sides, on="game_id", how="left").fillna({"sides":0})
    lack = cov[cov["sides"]<2]
    print("Games with both sides priced:", (cov["sides"]>=2).sum(), "/", len(cov))
    if not lack.empty:
        print("Missing lines (sample):", lack.head(5).to_dict("records"))

    # 4) sanity on AI tables
    preds = pd.read_sql("SELECT * FROM ai_game_predictions", con) if "ai_game_predictions" in got else pd.DataFrame()
    ev    = pd.read_sql("SELECT * FROM ai_betting_opportunities", con) if "ai_betting_opportunities" in got else pd.DataFrame()
    # ranges
    if not preds.empty:
        badp = preds[(preds.home_win_prob<0)|(preds.home_win_prob>1)|(preds.away_win_prob<0)|(preds.away_win_prob>1)]
        print("Pred prob out-of-range rows:", len(badp))
    if not ev.empty:
        print("EV edge range (%):", ev["edge_pct"].min(), "→", ev["edge_pct"].max())

    # 5) pack team cards
    season = int(pd.Timestamp.utcnow().year if pd.Timestamp.utcnow().month>=8 else pd.Timestamp.utcnow().year-1)
    tss = pd.read_sql("""
        SELECT team, power_score, games_played, win_pct, wins, losses
        FROM team_season_summary WHERE season=?
    """, con, params=[season])
    inj = pd.read_sql("""
        SELECT COALESCE(team_ai,team_inj) team, position, designation,
               COALESCE(inj_name, roster_name,'') player
        FROM ai_injury_validation_detail
        WHERE COALESCE(inj_missing_team,0)=0
          AND COALESCE(roster_missing_team,0)=0
          AND COALESCE(team_mismatch,0)=0
    """, con) if "ai_injury_validation_detail" in got else pd.DataFrame(columns=["team","position","designation","player"])
    if not inj.empty:
        # very light weights just for summary
        DES = {'IR':1.0,'INJURED RESERVE':1.0,'OUT':0.9,'DOUBTFUL':0.6,'QUESTIONABLE':0.3,'PUP':0.8}
        POS  = {'QB':3.0,'WR':1.5,'RB':1.5,'TE':1.4,'CB':1.3,'S':1.2,'LB':1.1,'EDGE':1.2,'DE':1.2,'DT':1.1,'T':1.0,'G':0.9,'C':0.9,'OL':0.9,'K':0.4,'P':0.4,'LS':0.3}
        inj["designation"]=inj["designation"].str.upper()
        inj["impact"]=inj.apply(lambda r: DES.get(r["designation"],0.3)*POS.get(str(r["position"]).upper(),1.0), axis=1)
        inj_team = inj.groupby("team").agg(injury_impact=("impact","sum"), total_injuries=("player","count")).reset_index()
    else:
        inj_team = pd.DataFrame(columns=["team","injury_impact","total_injuries"])

    team_cards = (tss.merge(inj_team, on="team", how="left").fillna({"injury_impact":0,"total_injuries":0}))
    team_cards["team_full"]=team_cards["team"].map(to_full)
    team_cards = team_cards.sort_values("power_score", ascending=False)

    # 6) pack per-game cards for next 21 days
    if preds.empty:
        game_cards = pd.DataFrame(columns=[])
    else:
        gsub = games[["game_id","away_team","home_team","game_date","start_time_local"]]
        p = preds.merge(gsub, on="game_id", how="inner")
        # best ML per team
        def best_quotes(df):
            if df.empty: return {}
            df = df.copy()
            # normalize everything to decimal for EV math
            df["dec"] = df["odds"].astype(float).map(american_to_dec)
            # keep best price (highest dec) per team
            idx = df.groupby(["game_id","team"])["dec"].idxmax()
            return df.loc[idx, ["game_id","team","sportsbook","odds","dec"]]
        best = best_quotes(odds)
        if isinstance(best, pd.DataFrame) and not best.empty:
            best = best
        else:
            best = pd.DataFrame(columns=["game_id","team","sportsbook","odds","dec"])

        # join EV (recommended_amount) if you want it on card
        ev_keep = (ev.sort_values(["game_id","team","edge_pct"], ascending=[True,True,False])
                     .groupby(["game_id","team"], as_index=False).head(1)) if not ev.empty else pd.DataFrame(columns=["game_id","team","edge_pct","recommended_amount"])

        # --- build rows (robust to 'game_date' vs 'd' and 'start_time_local' vs 't') ---
        rows = []
        for _, r in p.iterrows():
            gid   = r["game_id"]
            away  = r["away_team"]; home = r["home_team"]

            # date/time with safe fallbacks
            dt_raw  = r.get("game_date", r.get("d"))
            tm_raw  = r.get("start_time_local", r.get("t"))
            date_str = (str(dt_raw)[:10] if pd.notnull(dt_raw) else "")
            time_str = (str(tm_raw)[:5]  if pd.notnull(tm_raw) and str(tm_raw) != "None" else "TBD")

            # prediction fields (works whether your table has pred_team or not)
            ph = float(r.get("home_win_prob", 0.0))
            pa = float(r.get("away_win_prob", 0.0))
            pick_team = (r.get("pred_team") or (home if ph >= pa else away))
            conf = max(ph, pa)

            # best odds for each side
            b_home = best[(best["game_id"]==gid) & (best["team"]==home)]
            b_away = best[(best["game_id"]==gid) & (best["team"]==away)]
            def pack_best(df_):
                if df_.empty: return None
                rr = df_.iloc[0]
                return dict(sportsbook=str(rr["sportsbook"]), american=int(rr["odds"]), decimal=float(rr["dec"]))

            # EV (if present)
            ev_home = ev_keep[(ev_keep["game_id"]==gid) & (ev_keep["team"]==home)]
            ev_away = ev_keep[(ev_keep["game_id"]==gid) & (ev_keep["team"]==away)]

            rows.append(dict(
                game_id=str(gid),
                date=date_str,
                time=time_str,
                matchup=f"{to_full(away)} @ {to_full(home)}",
                teams=dict(home=to_full(home), away=to_full(away)),
                probs=dict(home=round(ph,3), away=round(pa,3)),
                pick=dict(team=to_full(pick_team), confidence=round(conf,3)),
                best_odds=dict(
                    home=pack_best(b_home),
                    away=pack_best(b_away)
                ),
                ev=dict(
                    home=float(ev_home["recommended_amount"].iloc[0]) if not ev_home.empty else 0.0,
                    away=float(ev_away["recommended_amount"].iloc[0]) if not ev_away.empty else 0.0
                )
            ))

        game_cards = pd.DataFrame(rows)

    # write artifacts
    with open(os.path.join(OUT_DIR,"ai_team_cards.jsonl"), "w", encoding="utf-8") as f:
        for _, r in team_cards.iterrows():
            f.write(json.dumps({
                "team": to_full(r["team"]),
                "abbr": r["team"],
                "power_score": float(r["power_score"]),
                "games_played": int(r.get("games_played",0) or 0),
                "win_pct": float(r.get("win_pct",0) or 0),
                "record": f"{int(r.get('wins',0) or 0)}-{int(r.get('losses',0) or 0)}",
                "injury_impact": float(r["injury_impact"]),
                "total_injuries": int(r["total_injuries"]),
            })+"\n")

    with open(os.path.join(OUT_DIR,"ai_knowledge_pack.jsonl"), "w", encoding="utf-8") as f:
        for row in (game_cards.to_dict("records") if not game_cards.empty else []):
            f.write(json.dumps(row)+"\n")

    # human-readable report
    with open(os.path.join(OUT_DIR,"ai_schema_report.md"),"w",encoding="utf-8") as f:
        f.write(f"# Bettr AI Data Report\n\n")
        f.write(f"- DB: `{DB}`\n")
        f.write(f"- Missing tables: {missing if missing else 'None'}\n")
        f.write(f"- Latest odds: {last_ts}\n")
        f.write(f"- Upcoming games (21d): {len(games)}\n")
        f.write(f"- Games with both sides priced: {(cov['sides']>=2).sum()} / {len(cov)}\n")
        if not preds.empty:
            f.write(f"- ai_game_predictions rows: {len(preds)}\n")
        if not ev.empty:
            f.write(f"- ai_betting_opportunities rows: {len(ev)} (edge % range {ev['edge_pct'].min():.1f} → {ev['edge_pct'].max():.1f})\n")

    con.close()
    print("✅ Wrote:",
          os.path.join(OUT_DIR,"ai_team_cards.jsonl"),
          os.path.join(OUT_DIR,"ai_knowledge_pack.jsonl"),
          os.path.join(OUT_DIR,"ai_schema_report.md"))

if __name__ == "__main__":
    run()
