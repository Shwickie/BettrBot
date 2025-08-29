# E:\Bettr Bot\betting-bot\stats\fill_game_times.py
import requests, pandas as pd
from datetime import datetime, timezone
from sqlalchemy import create_engine, text

DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

# Map full names to your 3-letter codes if needed
TEAM_MAP = {
    "Dallas Cowboys": "DAL", "Philadelphia Eagles": "PHI", # ... fill rest if your DB stores full names
}

def to_utc_iso(dt_str):
    # ESPN gives ISO with Z; normalize to strict UTC ISO
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return None

def load_games(conn):
    df = pd.read_sql(text("""
        SELECT id, game_id, home_team, away_team, game_date, start_time_utc
        FROM games
        WHERE start_time_utc IS NULL
    """), conn)
    # if your teams are like "Dallas Cowboys", coerce to 3-letter codes:
    df["home_team"] = df["home_team"].map(TEAM_MAP).fillna(df["home_team"])
    df["away_team"] = df["away_team"].map(TEAM_MAP).fillna(df["away_team"])
    df["game_date"] = pd.to_datetime(df["game_date"]).dt.date
    return df

def fetch_espn_scoreboard(date_str):
    # ex: date_str = "20250907"
    url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates={date_str}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

def extract_events(json_obj):
    rows = []
    for ev in json_obj.get("events", []):
        comp = ev.get("competitions", [{}])[0]
        date_iso = comp.get("date")  # kickoff datetime
        teams = comp.get("competitors", [])
        if len(teams) != 2:
            continue
        home = next((t for t in teams if t.get("homeAway") == "home"), None)
        away = next((t for t in teams if t.get("homeAway") == "away"), None)
        if not home or not away: 
            continue
        home_abbr = home.get("team", {}).get("abbreviation")
        away_abbr = away.get("team", {}).get("abbreviation")
        rows.append({
            "home_team": home_abbr,
            "away_team": away_abbr,
            "kickoff_utc": to_utc_iso(date_iso),
            "kickoff_local": date_iso,  # ESPN local-ish ISO; you can convert if you want exact local tz
            "date_only": date_iso[:10] if date_iso else None,
        })
    return pd.DataFrame(rows)

def main():
    with engine.begin() as conn:
        games = load_games(conn)
    if games.empty:
        print("No games need kickoff times.")
        return

    # Pull unique dates we need
    need_dates = sorted({g.strftime("%Y%m%d") for g in games["game_date"]})
    all_events = []
    for d in need_dates:
        try:
            js = fetch_espn_scoreboard(d)
            df = extract_events(js)
            if not df.empty:
                all_events.append(df)
        except Exception as e:
            print("Failed ESPN fetch for", d, e)

    if not all_events:
        print("No events fetched.")
        return

    events = pd.concat(all_events, ignore_index=True)

    # Join by home/away and date
    games["date_only"] = games["game_date"].astype(str)
    merged = games.merge(
        events,
        left_on=["home_team", "away_team", "date_only"],
        right_on=["home_team", "away_team", "date_only"],
        how="left"
    )

    updates = merged.dropna(subset=["kickoff_utc"])
    if updates.empty:
        print("No kickoff matches found.")
        return

    # Write back
    with engine.begin() as conn:
        for _, r in updates.iterrows():
            conn.execute(text("""
                UPDATE games
                SET start_time_utc = :utc, start_time_local = :local
                WHERE id = :id
            """), dict(utc=r["kickoff_utc"], local=r["kickoff_local"], id=r["id"]))
    print(f"Updated {len(updates)} games with kickoff times.")

if __name__ == "__main__":
    main()
