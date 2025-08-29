import requests
import pandas as pd
from sqlalchemy import create_engine, text
import time

API_KEY = "Bearer gR07b41N/GjTJZlj0zvn9vafwIgxuaAdVn9rMw2uGUBWwQMKjvgOm2gyHTHovAsr"
HEADERS = {"Authorization": API_KEY}
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

def fetch_weekly_stats(year, week):
    url = f"https://api.collegefootballdata.com/stats/player/game?year={year}&week={week}"
    try:
        res = requests.get(url, headers=HEADERS)
        if "application/json" not in res.headers.get("Content-Type", ""):
            raise Exception("Non-JSON response (possibly rate-limited or blocked)")

        if res.status_code == 429:
            print("⚠️ Rate limit hit. Sleeping 60s...")
            time.sleep(60)
            return fetch_weekly_stats(year, week)

        return res.json()
    except Exception as e:
        print(f"❌ Error {year} Week {week}: {e}")
        return []

all_rows = []

for year in [2021, 2022, 2023]:
    print(f"\n📦 Fetching NCAAF weekly player stats for {year}...")
    for week in range(1, 16):
        print(f"📅 Week {week}...")
        rows = fetch_weekly_stats(year, week)
        if not rows:
            continue

        df = pd.DataFrame(rows)
        df["year"] = year
        df["week"] = week
        all_rows.append(df)

        time.sleep(1.5)  # avoid rate limit

if not all_rows:
    print("❌ No data pulled.")
    exit()

df_all = pd.concat(all_rows, ignore_index=True)

# Pivot: stat_type + stat = new column
df_all["stat_col"] = df_all["stat_type"] + "_" + df_all["stat"]
df_all["value"] = pd.to_numeric(df_all["value"], errors="coerce")

pivot = df_all.pivot_table(
    index=["athlete_id", "athlete", "team", "opponent", "position", "year", "week"],
    columns="stat_col",
    values="value",
    aggfunc="sum"
).reset_index()

pivot.columns = [str(col).lower().replace(" ", "_") for col in pivot.columns]

# Save to SQL
table_name = f"ncaaf_player_stats_all"
with engine.begin() as conn:
    conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
    pivot.to_sql(table_name, conn, index=False)

print(f"✅ Imported {len(pivot)} rows to {table_name}")
