import pandas as pd
from sqlalchemy import create_engine, text

# DB Setup
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

# Load player game stats and games
with engine.connect() as conn:
    player_df = pd.read_sql(text("SELECT * FROM player_game_stats"), conn)
    games_df = pd.read_sql(text("SELECT * FROM games"), conn)

# Ensure datetime and extract season
games_df["game_date"] = pd.to_datetime(games_df["game_date"])
games_df["season"] = games_df["game_date"].dt.year
player_df["game_date"] = pd.to_datetime(player_df["game_date"])
player_df["season"] = player_df["game_date"].dt.year

# Merge player stats with game info (to get opponent team)
player_df = pd.merge(player_df, games_df[["game_id", "home_team", "away_team"]], on="game_id", how="left")

# Assign opponent based on player team
player_df["opponent"] = player_df.apply(
    lambda r: r["away_team"] if r["team"] == r["home_team"] else r["home_team"], axis=1
)

# Normalize position column
position_col = "position_x" if "position_x" in player_df.columns else "position_y"
player_df["position"] = player_df[position_col]

# OPTIONAL: Filter out non-productive games
# player_df = player_df[player_df["fantasy_points"] > 0]

# Defensive summary: how many fantasy points that opponent allows to each position
defense_summary = (
    player_df.groupby(["season", "opponent", "position"])
    .agg(defensive_score=("fantasy_points", "mean"))
    .reset_index()
)

# Merge defense performance back to player rows
player_df = pd.merge(
    player_df,
    defense_summary,
    on=["season", "opponent", "position"],
    how="left"
)

# Compute matchup advantage
player_df["offensive_score"] = player_df["fantasy_points"]
player_df["advantage_score"] = player_df["offensive_score"] - player_df["defensive_score"]

# Player-vs-defense summary over all games
matchup_summary = (
    player_df.groupby(["player_id", "full_name", "position", "opponent", "season"])
    .agg(
        avg_offensive_score=("offensive_score", "mean"),
        avg_defensive_score=("defensive_score", "mean"),
        avg_advantage_score=("advantage_score", "mean"),
        games_played=("game_id", "nunique")
    )
    .reset_index()
)

# Save results
with engine.begin() as conn:
    matchup_summary.to_sql("player_vs_defense_summary", conn, index=False, if_exists="replace")

print("✅ Created player_vs_defense_summary with offensive, defensive, and advantage metrics")
