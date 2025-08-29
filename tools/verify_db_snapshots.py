# tools/verify_db_snapshots.py
import pandas as pd
from sqlalchemy import create_engine, text

DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

TABLES = [
    "games",
    "team_season_summary",
    "matchup_power_summary",
    "player_game_stats",
    "player_vs_defense_summary",
    "pos_vs_def_summary",
    "odds",                  # odds table to inspect
    "odds_raw",              # if you store raw odds
    "system_status",
    "nfl_injuries",
]

def peek_table(conn, table, n=3):
    print(f"\n=== {table} ===")
    try:
        count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        print(f"rows: {count}")

        head = pd.read_sql(
            text(f"SELECT * FROM {table} ORDER BY rowid ASC LIMIT :n"),
            conn, params={"n": n}
        )
        tail = pd.read_sql(
            text(f"SELECT * FROM {table} ORDER BY rowid DESC LIMIT :n"),
            conn, params={"n": n}
        )

        if not head.empty:
            print("\n-- first rows --")
            print(head.to_string(index=False, max_cols=12, max_colwidth=28))
        if not tail.empty:
            print("\n-- last rows --")
            print(tail.iloc[::-1].to_string(index=False, max_cols=12, max_colwidth=28))

        # Extra odds checks
        if table == "odds":
            # Markets present
            markets = pd.read_sql(text("SELECT DISTINCT market FROM odds"), conn)
            print(f"\n-- distinct markets ({len(markets)} total) --")
            print(", ".join(sorted(markets['market'].dropna().unique())))

            # Flag if props expected but missing
            props_expected = {"player_pass_tds", "player_rush_yds", "player_rec_yds"}
            missing_props = props_expected - set(markets['market'])
            if missing_props:
                print(f"⚠️ Missing props markets: {', '.join(missing_props)}")
            else:
                print("✅ All expected props markets found")

            # Odds rows with game_id not in games
            junk = pd.read_sql(
                text("""
                    SELECT o.*
                    FROM odds o
                    LEFT JOIN games g ON o.game_id = g.game_id
                    WHERE g.game_id IS NULL
                    LIMIT 5
                """), conn
            )
            if not junk.empty:
                print(f"⚠️ Found odds for {len(junk)} junk games (showing 5):")
                print(junk.to_string(index=False, max_cols=10, max_colwidth=25))
            else:
                print("✅ No junk odds found")

    except Exception as e:
        print(f"(could not read {table}): {e}")

with engine.connect() as conn:
    for t in TABLES:
        peek_table(conn, t)

print("\n✅ verify_db_snapshots complete.")
