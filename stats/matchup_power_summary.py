import pandas as pd
from sqlalchemy import create_engine, text

# DB Setup
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

# Load games and team summary
with engine.connect() as conn:
    games_df = pd.read_sql(text("SELECT * FROM games"), conn)
    team_summary = pd.read_sql(text("SELECT * FROM team_season_summary"), conn)

# Standardize team names (same as used in team_summary)
team_name_map = {
    "Arizona Cardinals": "ARI", "Atlanta Falcons": "ATL", "Baltimore Ravens": "BAL",
    "Buffalo Bills": "BUF", "Carolina Panthers": "CAR", "Chicago Bears": "CHI",
    "Cincinnati Bengals": "CIN", "Cleveland Browns": "CLE", "Dallas Cowboys": "DAL",
    "Denver Broncos": "DEN", "Detroit Lions": "DET", "Green Bay Packers": "GB",
    "Houston Texans": "HOU", "Indianapolis Colts": "IND", "Jacksonville Jaguars": "JAX",
    "Kansas City Chiefs": "KC", "Las Vegas Raiders": "LV", "Los Angeles Chargers": "LAC",
    "Los Angeles Rams": "LA", "Miami Dolphins": "MIA", "Minnesota Vikings": "MIN",
    "New England Patriots": "NE", "New Orleans Saints": "NO", "New York Giants": "NYG",
    "New York Jets": "NYJ", "Philadelphia Eagles": "PHI", "Pittsburgh Steelers": "PIT",
    "San Francisco 49ers": "SF", "Seattle Seahawks": "SEA", "Tampa Bay Buccaneers": "TB",
    "Tennessee Titans": "TEN", "Washington Commanders": "WAS"
}

games_df["home_team"] = games_df["home_team"].replace(team_name_map)
games_df["away_team"] = games_df["away_team"].replace(team_name_map)
games_df["game_date"] = pd.to_datetime(games_df["game_date"])
games_df["season"] = games_df["game_date"].dt.year

# Merge home/away team power scores
games_df = pd.merge(
    games_df,
    team_summary[["season", "team", "power_score"]].rename(columns={"team": "home_team", "power_score": "home_power"}),
    on=["season", "home_team"],
    how="left"
)

games_df = pd.merge(
    games_df,
    team_summary[["season", "team", "power_score"]].rename(columns={"team": "away_team", "power_score": "away_power"}),
    on=["season", "away_team"],
    how="left"
)

# Calculate matchup power delta and favorite
games_df["power_diff"] = games_df["home_power"] - games_df["away_power"]
games_df["favored_team"] = games_df.apply(lambda r: r["home_team"] if r["power_diff"] >= 0 else r["away_team"], axis=1)

# Determine actual winner
games_df["actual_winner"] = games_df.apply(lambda r: r["home_team"] if r["home_score"] > r["away_score"] else r["away_team"], axis=1)

# Flag whether it was an upset
games_df["was_upset"] = games_df["favored_team"] != games_df["actual_winner"]

# Compute power margin (confidence level of favorite)
games_df["power_margin"] = abs(games_df["home_power"] - games_df["away_power"])

# Save to DB
with engine.begin() as conn:
    games_df.to_sql("matchup_power_summary", conn, index=False, if_exists="replace")

print("✅ Created matchup_power_summary with power scores, deltas, winner, upset flag, and margin")
