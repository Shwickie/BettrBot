import pandas as pd
from sqlalchemy import create_engine, text

# DB config
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

years = [2021, 2022, 2023, 2024, 2025]
all_data = []

with engine.connect() as conn:
    for year in years:
        table = f"player_stats_{year}"
        try:
            df = pd.read_sql(text(f"SELECT * FROM {table}"), conn)
            df["season"] = year
            all_data.append(df)
        except Exception as e:
            print(f"⚠️ Could not load {table}: {e}")

df = pd.concat(all_data, ignore_index=True)
df = df.sort_values(by=["player_id", "season", "week"])

# Replace 'team' with 'recent_team' if needed
df = df[["player_id", "player_name", "position", "recent_team", "season", "week", "fantasy_points"]].copy()
df = df.rename(columns={"recent_team": "team"})

df["avg_last_3"] = df.groupby("player_id")["fantasy_points"].transform(lambda x: x.rolling(window=3, min_periods=1).mean())
df["avg_last_5"] = df.groupby("player_id")["fantasy_points"].transform(lambda x: x.rolling(window=5, min_periods=1).mean())

with engine.begin() as conn:
    df.to_sql("player_form_trends", conn, index=False, if_exists="replace")

print("✅ Created player_form_trends with rolling averages (3-game & 5-game)")
