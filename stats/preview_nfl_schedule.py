# preview_nfl_schedule.py
import nfl_data_py as nfl
import pandas as pd

df = nfl.import_schedules([2021, 2022, 2023])
print(df[['game_id', 'gameday', 'home_team', 'away_team', 'home_score', 'away_score']].head(20))
