import sqlite3, pandas as pd
con=sqlite3.connect(r"E:\Bettr Bot\betting-bot\data\betting.db")
def show(t, q): 
    print(f"\n[{t}]"); 
    try: print(pd.read_sql_query(q, con).head(12))
    except Exception as e: print("ERR:", e)

show("PREDICTIONS", "SELECT matchup, pred_team, ROUND(home_win_prob,3) hwp, ROUND(confidence,3) conf FROM ai_game_predictions ORDER BY game_date LIMIT 12")
show("EV OPPORTUNITIES", "SELECT team, sportsbook, odds, ROUND(implied_prob,3) ip, ROUND(model_prob,3) mp, ROUND(edge_pct,2) edge, ROUND(recommended_amount,2) bet FROM ai_betting_opportunities ORDER BY edge_pct DESC LIMIT 12")
con.close()
