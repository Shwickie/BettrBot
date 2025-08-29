# ev_calculator.py

from sqlalchemy import create_engine, MetaData, Table, select
import pandas as pd

# ---------------------------
# CONFIG
# ---------------------------
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)
metadata = MetaData()
metadata.reflect(bind=engine)

# Ensure 'odds' table exists
if 'odds' not in metadata.tables:
    raise Exception("❌ 'odds' table not found in DB")
odds_table = metadata.tables['odds']

# ---------------------------
# LOAD LATEST ODDS
# ---------------------------
with engine.connect() as conn:
    query = select(odds_table)
    rows = conn.execute(query).fetchall()
    df = pd.DataFrame([dict(row._mapping) for row in rows])

# ---------------------------
# STEP 1: Calculate Implied Probabilities
# ---------------------------
df['implied_prob'] = 1 / df['odds']

# ---------------------------
# STEP 2: Normalize Probabilities Within Each Market
# ---------------------------
group_cols = ['game_id', 'market', 'sportsbook']
df['sum_prob'] = df.groupby(group_cols)['implied_prob'].transform('sum')
df['norm_prob'] = df['implied_prob'] / df['sum_prob']

# ---------------------------
# STEP 3: Assume True Win Prob (for now use norm_prob)
# ---------------------------
df['true_prob'] = df['norm_prob']  # Replace with model-based later

# ---------------------------
# STEP 4: Calculate Expected Value (EV)
# EV = (true_prob * (odds - 1)) - (1 - true_prob)
# ---------------------------
df['ev'] = (df['true_prob'] * (df['odds'] - 1)) - (1 - df['true_prob'])

# ---------------------------
# STEP 5: Flag Value Bets (EV > 0)
# ---------------------------
value_bets = df[df['ev'] > 0].sort_values(by='ev', ascending=False)

print("\n💰 Value Bets Found:")
if value_bets.empty:
    print("No positive EV bets found.")
else:
    print(value_bets[['game_id', 'team', 'market', 'sportsbook', 'odds', 'ev']])