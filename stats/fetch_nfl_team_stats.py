import nfl_data_py as nfl
import pandas as pd

# Get past 3 seasons of game results
games = nfl.import_schedules([2021, 2022, 2023, 2024])

# Preview the structure
print(games.columns)
print(games[['game_id', 'week', 'home_team', 'away_team', 'home_score', 'away_score']].head())
