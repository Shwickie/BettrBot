import sqlite3, pandas as pd
con=sqlite3.connect(r"E:\Bettr Bot\betting-bot\data\betting.db")
def peek(sql): 
    try: print(pd.read_sql_query(sql, con).head(12))
    except Exception as e: print("ERR:", e)
print("\n[GAMES UPCOMING]")
peek("SELECT game_id, away_team, home_team, game_date FROM games WHERE game_date >= '2025-09-01' AND home_score IS NULL ORDER BY game_date LIMIT 12")

print("\n[POWER JOIN CHECK]")
peek("SELECT season, home_team, away_team, ROUND(home_power,1) h, ROUND(away_power,1) a, ROUND(power_diff,1) diff FROM matchup_power_summary WHERE season=2025 AND home_score IS NULL ORDER BY game_date LIMIT 12")

print("\n[ODDS SAMPLE h2h]")
peek("SELECT game_id, team, sportsbook, market, odds, timestamp FROM odds WHERE market='h2h' ORDER BY timestamp DESC LIMIT 12")

print("\n[PREDICTIONS]")
peek("SELECT game_id, matchup, pred_team, ROUND(home_win_prob,3) hwp, ROUND(confidence,3) conf FROM ai_game_predictions ORDER BY game_date LIMIT 12")

print("\n[EV OPPORTUNITIES]")
peek("SELECT game_id, team, sportsbook, odds, ROUND(implied_prob,3) ip, ROUND(model_prob,3) mp, ROUND(edge_pct,2) edge, ROUND(recommended_amount,2) bet FROM ai_betting_opportunities ORDER BY edge_pct DESC LIMIT 12")
con.close()
