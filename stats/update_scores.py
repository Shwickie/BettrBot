#!/usr/bin/env python3
"""
Weekly Score Updater - canonical + auto-backfill (rowid-safe)
- Canonicalize NFL & DB team names to 3-letter abbr
- Expand 'LA' to LAR/LAC during matching
- Auto-insert (backfill) any missing 2025 games into `games`
- Assigns id = MAX(rowid)+1 to avoid NOT NULL/UNIQUE id issues
- Update by PRIMARY KEY `id`
"""

import pandas as pd
import nfl_data_py as nfl
import sqlite3
from datetime import datetime
import os, subprocess

DB_PATH = r"E:/Bettr Bot/betting-bot/data/betting.db"
SEASON = 2025

TEAM_NAMES = {
    "ARI":"Arizona Cardinals","ATL":"Atlanta Falcons","BAL":"Baltimore Ravens","BUF":"Buffalo Bills",
    "CAR":"Carolina Panthers","CHI":"Chicago Bears","CIN":"Cincinnati Bengals","CLE":"Cleveland Browns",
    "DAL":"Dallas Cowboys","DEN":"Denver Broncos","DET":"Detroit Lions","GB":"Green Bay Packers",
    "HOU":"Houston Texans","IND":"Indianapolis Colts","JAX":"Jacksonville Jaguars","KC":"Kansas City Chiefs",
    "LAC":"Los Angeles Chargers","LAR":"Los Angeles Rams","LV":"Las Vegas Raiders","MIA":"Miami Dolphins",
    "MIN":"Minnesota Vikings","NE":"New England Patriots","NO":"New Orleans Saints","NYG":"New York Giants",
    "NYJ":"New York Jets","PHI":"Philadelphia Eagles","PIT":"Pittsburgh Steelers","SEA":"Seattle Seahawks",
    "SF":"San Francisco 49ers","TB":"Tampa Bay Buccaneers","TEN":"Tennessee Titans","WAS":"Washington Commanders",
}

CANON = {
    # NFC West
    "San Francisco 49ers":"SF","49ers":"SF","SF":"SF","SFO":"SF",
    "Seattle Seahawks":"SEA","Seahawks":"SEA","SEA":"SEA",
    "Los Angeles Rams":"LAR","LA Rams":"LAR","Rams":"LAR","LAR":"LAR","STL":"LAR","St. Louis Rams":"LAR",
    "Arizona Cardinals":"ARI","Cardinals":"ARI","ARI":"ARI",
    # NFC North
    "Detroit Lions":"DET","Lions":"DET","DET":"DET",
    "Green Bay Packers":"GB","Packers":"GB","GB":"GB","GBP":"GB",
    "Chicago Bears":"CHI","Bears":"CHI","CHI":"CHI",
    "Minnesota Vikings":"MIN","Vikings":"MIN","MIN":"MIN",
    # NFC East
    "Philadelphia Eagles":"PHI","Eagles":"PHI","PHI":"PHI",
    "Dallas Cowboys":"DAL","Cowboys":"DAL","DAL":"DAL",
    "New York Giants":"NYG","Giants":"NYG","NYG":"NYG",
    "Washington Commanders":"WAS","Commanders":"WAS","WAS":"WAS","WSH":"WAS",
    # NFC South
    "Tampa Bay Buccaneers":"TB","Buccaneers":"TB","TB":"TB","TAM":"TB",
    "New Orleans Saints":"NO","Saints":"NO","NO":"NO","NOR":"NO",
    "Atlanta Falcons":"ATL","Falcons":"ATL","ATL":"ATL",
    "Carolina Panthers":"CAR","Panthers":"CAR","CAR":"CAR",
    # AFC West
    "Kansas City Chiefs":"KC","Chiefs":"KC","KC":"KC","KCC":"KC",
    "Los Angeles Chargers":"LAC","LA Chargers":"LAC","Chargers":"LAC","LAC":"LAC","SD":"LAC","San Diego Chargers":"LAC",
    "Denver Broncos":"DEN","Broncos":"DEN","DEN":"DEN",
    "Las Vegas Raiders":"LV","Raiders":"LV","LV":"LV","OAK":"LV","Oakland Raiders":"LV",
    # AFC North
    "Baltimore Ravens":"BAL","Ravens":"BAL","BAL":"BAL",
    "Cincinnati Bengals":"CIN","Bengals":"CIN","CIN":"CIN",
    "Cleveland Browns":"CLE","Browns":"CLE","CLE":"CLE",
    "Pittsburgh Steelers":"PIT","Steelers":"PIT","PIT":"PIT",
    # AFC South
    "Houston Texans":"HOU","Texans":"HOU","HOU":"HOU",
    "Indianapolis Colts":"IND","Colts":"IND","IND":"IND",
    "Jacksonville Jaguars":"JAX","Jaguars":"JAX","JAX":"JAX","JAC":"JAX",
    "Tennessee Titans":"TEN","Titans":"TEN","TEN":"TEN",
    # AFC East
    "Buffalo Bills":"BUF","Bills":"BUF","BUF":"BUF",
    "Miami Dolphins":"MIA","Dolphins":"MIA","MIA":"MIA",
    "New England Patriots":"NE","Patriots":"NE","NE":"NE","NWE":"NE",
    "New York Jets":"NYJ","Jets":"NYJ","NYJ":"NYJ",
}

def canon(team: str) -> str:
    if team is None:
        return ""
    t = str(team).strip()
    if t.upper() == "LA":  # keep ambiguous; expand later
        return "LA"
    return CANON.get(t, CANON.get(t.title(), t.upper()))

def _expand_la(df: pd.DataFrame, side: str) -> pd.DataFrame:
    col = f"{side}_abbr"
    if col not in df.columns:
        return df
    m = df[col].eq("LA")
    if not m.any():
        return df
    a = df.loc[m].copy()
    a_lar = a.copy(); a_lar[col] = "LAR"
    a_lac = a.copy(); a_lac[col] = "LAC"
    return pd.concat([df.loc[~m], a_lar, a_lac], ignore_index=True)

def _resolve_columns(df: pd.DataFrame):
    dc = next((c for c in ("gameday","game_date","start_time") if c in df.columns), None)
    hc = next((c for c in ("home_team","home","team_home") if c in df.columns), None)
    ac = next((c for c in ("away_team","away","team_away") if c in df.columns), None)
    hsc = next((c for c in ("home_score","score_home") if c in df.columns), None)
    asc = next((c for c in ("away_score","score_away") if c in df.columns), None)
    wk  = next((c for c in ("week","game_week","wk") if c in df.columns), None)
    return dc, hc, ac, hsc, asc, wk

def _get_columns(conn):
    return {row[1] for row in conn.execute("PRAGMA table_info(games)").fetchall()}

def _next_rowid_id(conn) -> int:
    """Use table rowid to derive a safe, unique numeric id."""
    return conn.execute("SELECT IFNULL(MAX(rowid), 0) + 1 FROM games").fetchone()[0]

def _row_exists(conn, date_str, home_abbr, away_abbr):
    """Row exists for date+teams (accept abbr or full names)."""
    home_full = TEAM_NAMES.get(home_abbr, home_abbr)
    away_full = TEAM_NAMES.get(away_abbr, away_abbr)
    q = """
    SELECT id FROM games
     WHERE date(game_date)=?
       AND (home_team IN (?,?) )
       AND (away_team IN (?,?) )
     LIMIT 1
    """
    r = conn.execute(q, (date_str, home_abbr, home_full, away_abbr, away_full)).fetchone()
    return r[0] if r else None

def _insert_game(conn, date_str, home_abbr, away_abbr, week=None):
    """Insert a missing game; assigns id from MAX(rowid)+1 to avoid collisions."""
    cols = _get_columns(conn)
    new_id = _next_rowid_id(conn)

    # Prefer abbreviations in DB going forward
    home_val = home_abbr
    away_val = away_abbr

    fields = ["id", "game_date", "home_team", "away_team"]
    params = [new_id, date_str, home_val, away_val]

    # Optional/likely columns
    if "season" in cols:
        fields += ["season"]; params += [SEASON]
    if "week" in cols and week is not None:
        fields += ["week"]; params += [int(week)]
    if "league" in cols:
        fields += ["league"]; params += ["NFL"]
    if "sport" in cols:
        fields += ["sport"]; params += ["football"]
    if "home_score" in cols:
        fields += ["home_score"]; params += [None]
    if "away_score" in cols:
        fields += ["away_score"]; params += [None]
    if "status" in cols:
        fields += ["status"]; params += ["scheduled"]

    sql = f"INSERT INTO games ({', '.join(fields)}) VALUES ({', '.join(['?']*len(fields))})"
    conn.execute(sql, params)

def _backfill_missing_games(conn, nfl_df):
    """Ensure all 2025 NFL games exist in `games` for matching."""
    date_col, home_col, away_col, _, _, week_col = _resolve_columns(nfl_df)
    sched = nfl_df[[date_col, home_col, away_col] + ([week_col] if week_col else [])].copy()
    sched[date_col] = pd.to_datetime(sched[date_col], errors="coerce")
    sched["game_date"] = sched[date_col].dt.date
    sched = sched[sched["game_date"].notna()]

    sched["home_abbr"] = sched[home_col].map(canon)
    sched["away_abbr"] = sched[away_col].map(canon)

    inserted = 0
    for _, r in sched.iterrows():
        ds = r["game_date"].strftime("%Y-%m-%d")
        ha, aa = r["home_abbr"], r["away_abbr"]
        if not ha or not aa:
            continue

        # Expand LA ambiguity
        # If either side is 'LA', skip backfill for this row; we'll resolve at match time
        if ha == "LA" or aa == "LA":
            continue

        # Single definite insert
        if not _row_exists(conn, ds, ha, aa):
            _insert_game(conn, ds, ha, aa, r.get(week_col))
            inserted += 1



    if inserted:
        print(f"🔧 Backfilled {inserted} missing game rows into `games`.")

def update_all_completed_games() -> int:
    print("NFL WEEKLY SCORE UPDATER")
    print("=" * 60)
    print(f"Running at: {datetime.now():%Y-%m-%d %H:%M}\n")
    print(f"Fetching NFL data for {SEASON} season...")

    nfl_df = nfl.import_schedules([SEASON])
    if nfl_df is None or nfl_df.empty:
        print("❌ No NFL data available")
        return 0

    date_col, home_col, away_col, hs_col, as_col, _ = _resolve_columns(nfl_df)
    if None in (date_col, home_col, away_col, hs_col, as_col):
        print("❌ Unexpected NFL columns:", list(nfl_df.columns))
        return 0

    nfl_df[date_col] = pd.to_datetime(nfl_df[date_col], errors="coerce")

    # Completed games from feed
    nfl_completed = nfl_df.loc[
        nfl_df[hs_col].notna() & nfl_df[as_col].notna(),
        [date_col, home_col, away_col, hs_col, as_col]
    ].copy()
    if nfl_completed.empty:
        print("ℹ️ No completed games found in NFL data")
        return 0

    nfl_completed["game_date"] = pd.to_datetime(nfl_completed[date_col]).dt.date
    nfl_completed["home_abbr_raw"] = nfl_completed[home_col].astype(str)
    nfl_completed["away_abbr_raw"] = nfl_completed[away_col].astype(str)
    nfl_completed["home_abbr"] = nfl_completed["home_abbr_raw"].map(canon)
    nfl_completed["away_abbr"] = nfl_completed["away_abbr_raw"].map(canon)

    print("\nRecent completed games from NFL:")
    for _, g in nfl_completed.sort_values("game_date").tail(5).iterrows():
        print(f"  {g['game_date']:%m-%d}: {g['away_abbr_raw']} {int(g[as_col])} @ {g['home_abbr_raw']} {int(g[hs_col])}")

    # Expand LA on feed side
    nfl_completed = _expand_la(nfl_completed, "home")
    nfl_completed = _expand_la(nfl_completed, "away")

    # Ensure schedule rows exist in DB (backfill all season)
    with sqlite3.connect(DB_PATH) as conn:
        _backfill_missing_games(conn, nfl_df)
        conn.commit()

    # Load DB rows needing scores (2025)
    with sqlite3.connect(DB_PATH) as conn:
        db_games = pd.read_sql_query(
            """
            SELECT id, date(game_date) AS game_date, home_team, away_team,
                   home_score, away_score
              FROM games
             WHERE (home_score IS NULL OR away_score IS NULL)
               AND strftime('%Y', game_date) = ?
            """, conn, params=[str(SEASON)]
        )
    db_games["game_date"] = pd.to_datetime(db_games["game_date"], errors="coerce").dt.date
    print(f"\nDatabase games needing scores: {len(db_games)}")
    if db_games.empty:
        print("All games already have scores.")
        return 0

    # Canonicalize DB
    db_games["home_abbr"] = db_games["home_team"].map(canon)
    db_games["away_abbr"] = db_games["away_team"].map(canon)
    db_games = _expand_la(db_games, "home")
    db_games = _expand_la(db_games, "away")

    # Join by date + canonical teams
    nfl_slim = nfl_completed[["game_date","home_abbr","away_abbr",hs_col,as_col]].rename(
        columns={hs_col:"home_score_nfl", as_col:"away_score_nfl"}
    )
    merged_direct = db_games.merge(nfl_slim, on=["game_date","home_abbr","away_abbr"], how="inner")
    nfl_swapped = nfl_slim.rename(columns={
        "home_abbr":"away_abbr","away_abbr":"home_abbr",
        "home_score_nfl":"away_score_nfl","away_score_nfl":"home_score_nfl"
    })
    merged_swapped = db_games.merge(nfl_swapped, on=["game_date","home_abbr","away_abbr"], how="inner")
    merged = pd.concat([merged_direct, merged_swapped], ignore_index=True)

    print(f"Matches found after canonical join: {len(merged)}")
    if merged.empty:
        print("\nExample NFL (canon) tail:")
        print(nfl_completed.tail(8)[["game_date","away_abbr_raw","home_abbr_raw","away_abbr","home_abbr"]])
        print("\nExample DB (canon) head:")
        print(db_games.head(12)[["game_date","away_team","home_team","away_abbr","home_abbr"]])
        return 0

    updates_made = 0
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        for _, r in merged.iterrows():
            hs = int(r["home_score_nfl"]); as_ = int(r["away_score_nfl"]); pk = int(r["id"])
            cur.execute("UPDATE games SET home_score=?, away_score=? WHERE id=?", (hs, as_, pk))
            if cur.rowcount == 1:
                updates_made += 1
        conn.commit()

    print(f"Successfully updated: {updates_made}")
    return updates_made

def run_post_update_tasks() -> bool:
    print("\nRunning team_season_summary.py to update team stats...")
    try:
        candidates = [
            "team_season_summary.py",
            "stats/team_season_summary.py",
            os.path.join(os.path.dirname(__file__), "team_season_summary.py"),
        ]
        script = next((p for p in candidates if os.path.exists(p)), None)
        if not script:
            print("⚠️ Could not find team_season_summary.py")
            return False
        res = subprocess.run(["python", script], capture_output=True, text=True)
        if res.returncode == 0:
            print("✅ Team season summary updated successfully")
            return True
        print(f"⚠️ Team summary warning: {res.stderr.strip()}")
        return False
    except Exception as e:
        print(f"⚠️ Error running post-update tasks: {e}")
        return False

def main():
    print("🏈 NFL WEEKLY SCORE UPDATER")
    print("Automatically updates all completed game scores")
    print("=" * 60)
    updates = update_all_completed_games()
    print("\n" + "=" * 60)
    print("FINAL RESULTS")
    print("=" * 60)
    if updates > 0:
        print(f"🎉 SUCCESS: Updated {updates} games with scores!")
        if run_post_update_tasks():
            print("\n🔄 Restart your mobile dashboard to see changes")
    else:
        print("ℹ️ No new scores written. See debug above.")
    print("\n📅 Next: run after each week’s games.")

if __name__ == "__main__":
    main()
