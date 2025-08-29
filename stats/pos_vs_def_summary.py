import pandas as pd
from sqlalchemy import create_engine, text

# Config
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)
table_name = "player_stats_2023"  # or loop over years if needed

# Load data
with engine.connect() as conn:
    df = pd.read_sql(text(f"SELECT * FROM {table_name}"), conn)

# Clean + set opponent/team
df = df.dropna(subset=["opponent_team", "position"])  # required fields

# Normalize column naming
df["team"] = df["recent_team"]
df["opponent"] = df["opponent_team"]
df["position"] = df["position"].str.upper()

# Choose stat columns to average
stat_cols = [
    "fantasy_points", "passing_yards", "rushing_yards", "receiving_yards",
    "passing_tds", "rushing_tds", "receiving_tds",
    "completions", "attempts", "interceptions", "targets", "receptions"
]
for col in stat_cols:
    if col not in df.columns:
        df[col] = 0  # pad missing stats

# Group by position vs defense
summary = (
    df.groupby(["season", "opponent", "position"])
    [stat_cols]
    .mean()
    .reset_index()
    .rename(columns={"opponent": "defense_team"})
)

# Save to DB
with engine.begin() as conn:
    summary.to_sql("pos_vs_def_summary", conn, index=False, if_exists="replace")

print("✅ Saved pos_vs_def_summary (position-wide averages vs each defense)")
