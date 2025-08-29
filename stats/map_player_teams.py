import pandas as pd
from sqlalchemy import create_engine, MetaData, text

# DB setup
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)
metadata = MetaData()
metadata.reflect(bind=engine)

# Load games and player_team_map
with engine.connect() as conn:
    games_df = pd.read_sql(text("SELECT * FROM games"), conn)
    player_team_map = pd.read_sql(text("SELECT * FROM player_team_map"), conn)

# Process games
games_df = games_df.dropna(subset=["game_date", "home_team", "away_team"])
games_df["game_date"] = pd.to_datetime(games_df["game_date"])
games_df["season"] = games_df["game_date"].dt.year

# Build game-team-week mapping
game_team_rows = []
for _, row in games_df.iterrows():
    for team in [row["home_team"], row["away_team"]]:
        game_team_rows.append({
            "game_id": row["id"],
            "game_date": row["game_date"],
            "season": row["season"],
            "team": team,
        })

game_team_df = pd.DataFrame(game_team_rows)
game_team_df = game_team_df.sort_values(["season", "team", "game_date"])
game_team_df["week"] = game_team_df.groupby(["season", "team"]).cumcount() + 1

# Load player stats from all seasons
all_stats = []
with engine.connect() as conn:
    for table_name in metadata.tables:
        if table_name.startswith("player_stats_"):
            season = int(table_name.split("_")[-1])
            stats_df = pd.read_sql(text(f"SELECT * FROM {table_name}"), conn)
            stats_df["season"] = season
            stats_df["player_id"] = stats_df["player_id"].astype(str).str.strip()
            all_stats.append(stats_df)

# Combine all player stats
if not all_stats:
    raise Exception("❌ No player stats found to merge.")

player_stats = pd.concat(all_stats, ignore_index=True)

# Merge with player-to-team map
stats_with_teams = pd.merge(player_stats, player_team_map, on=["player_id", "season"], how="left")

# Merge with game data on season, team, and week
# This assumes player stats also have a `week` column
if "week" not in stats_with_teams.columns:
    raise Exception("❌ Your player stats tables are missing the 'week' column. It is required for accurate matching.")

player_game_stats = pd.merge(
    stats_with_teams,
    game_team_df,
    on=["season", "team", "week"],
    how="inner"
)

# Drop unmatched records
player_game_stats = player_game_stats.dropna(subset=["game_id", "game_date"])

# Save final table
with engine.begin() as conn:
    player_game_stats.to_sql("player_game_stats", conn, index=False, if_exists="replace")

print("✅ player_game_stats table created with", len(player_game_stats), "rows.")
