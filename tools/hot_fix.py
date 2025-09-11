
SELECT id, game_date, home_team, away_team, home_score, away_score
FROM games
WHERE date(game_date)='2025-09-07'
  AND (home_team IN ('LAC','LAR') OR away_team IN ('LAC','LAR'));

-- Reassign the HOU@LAC row(s) to LAR (correct: Texans played the Rams)
UPDATE games
SET home_team = CASE WHEN home_team='LAC' AND away_team='HOU' THEN 'LAR' ELSE home_team END,
    away_team = CASE WHEN away_team='LAC' AND home_team='HOU' THEN 'LAR' ELSE away_team END
WHERE date(game_date)='2025-09-07'
  AND ((home_team='LAC' AND away_team='HOU')
    OR (away_team='LAC' AND home_team='HOU'));
