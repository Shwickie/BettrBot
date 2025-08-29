import pandas as pd
from sqlalchemy import create_engine, text

# DB Setup
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

# Load full player game stats and games
with engine.connect() as conn:
    df = pd.read_sql(text("SELECT * FROM player_game_stats"), conn)
    games_df = pd.read_sql(text("SELECT * FROM games"), conn)

# Fix: Convert game_date to datetime and extract season
games_df["game_date"] = pd.to_datetime(games_df["game_date"])
games_df["season"] = games_df["game_date"].dt.year

# Determine game winners
games_df["winner"] = games_df.apply(
    lambda r: r["home_team"] if r["home_score"] > r["away_score"] else r["away_team"], axis=1
)

# Compute team win % by season
team_wins = games_df.groupby(["season", "winner"]).size().reset_index(name="wins")
team_games = pd.concat([
    games_df[["season", "home_team"]].rename(columns={"home_team": "team"}),
    games_df[["season", "away_team"]].rename(columns={"away_team": "team"})
])
team_games = team_games.groupby(["season", "team"]).size().reset_index(name="games_played")
team_win_pct = pd.merge(
    team_games,
    team_wins.rename(columns={"winner": "team"}),
    on=["season", "team"],
    how="left"
)
team_win_pct["wins"] = team_win_pct["wins"].fillna(0)
team_win_pct["win_pct"] = team_win_pct["wins"] / team_win_pct["games_played"]

# Choose correct position column
position_col = "position_x" if "position_x" in df.columns else "position_y"

# Group player stats
summary = (
    df.groupby(["player_id", "full_name", position_col, "team", "season"])
    .agg(
        games_played=("game_id", "nunique"),
        total_fantasy_points=("fantasy_points", "sum"),
        avg_fantasy_points=("fantasy_points", "mean"),
        total_touchdowns=("passing_tds", "sum"),
    )
    .reset_index()
    .rename(columns={position_col: "position"})
)

# Filter out low-playtime players
summary = summary[summary["games_played"] >= 4]

# Merge win %
summary = pd.merge(summary, team_win_pct[["season", "team", "win_pct"]], on=["season", "team"], how="left")
summary["win_pct"] = summary["win_pct"].fillna(0.5)
summary["tds_per_game"] = summary["total_touchdowns"] / summary["games_played"]

# Calculate custom star index
summary["star_index"] = (
    summary["avg_fantasy_points"] * 0.5 +
    summary["tds_per_game"] * 10 +
    summary["win_pct"] * 20
)

# Assign Madden-style grades using quantiles
quantiles = summary["star_index"].quantile([0.95, 0.85, 0.65, 0.4])

def grade(score):
    if score >= quantiles[0.95]:
        return "Superstar"
    elif score >= quantiles[0.85]:
        return "Star"
    elif score >= quantiles[0.65]:
        return "Starter"
    elif score >= quantiles[0.4]:
        return "Backup"
    else:
        return "Bench"

summary["madden_grade"] = summary["star_index"].apply(grade)

# Preview top and bottom players
print("\n🌟 Top 10 Players by Star Index:")
print(summary.sort_values("star_index", ascending=False).head(10)[
    ["full_name", "position", "team", "season", "star_index", "madden_grade", "avg_fantasy_points", "tds_per_game", "win_pct"]
])

print("\n🪑 Bottom 10 Players by Star Index:")
print(summary.sort_values("star_index", ascending=True).head(10)[
    ["full_name", "position", "team", "season", "star_index", "madden_grade", "avg_fantasy_points", "tds_per_game", "win_pct"]
])

# Save view to DB
with engine.begin() as conn:
    summary.to_sql("player_season_summary", conn, index=False, if_exists="replace")

print("✅ Created upgraded player_season_summary with Madden-style ratings and star_index.")
