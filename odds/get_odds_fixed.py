import requests
import time
from datetime import datetime
from sqlalchemy import text, create_engine, Table, MetaData, select, update, and_
from sqlalchemy.orm import sessionmaker

# ---------------------------
# CONFIG
# ---------------------------
API_KEY = '2ea42e6f961b41a105cd8dac8a3490a8'
SPORT = 'americanfootball_nfl'
REGIONS = 'us'

# Add player props here if you want them
MARKETS = 'h2h,spreads,totals,player_pass_tds,player_rush_yds,player_rec_yds'
ODDS_FORMAT = 'decimal'
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"

# ---------------------------
# API CALL
# ---------------------------
url = f'https://api.the-odds-api.com/v4/sports/{SPORT}/odds'
params = {
    'apiKey': API_KEY,
    'regions': REGIONS,
    'markets': MARKETS,
    'oddsFormat': ODDS_FORMAT
}
response = requests.get(url, params=params)

# ---------------------------
# DATABASE SETUP
# ---------------------------
engine = create_engine(DB_PATH, connect_args={"timeout": 30})
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData()
metadata.reflect(bind=engine)

games_table = metadata.tables.get('games')
odds_table = metadata.tables.get('odds')

if games_table is None or odds_table is None:
    raise Exception("❌ Missing 'games' or 'odds' table in database")

# ---------------------------
# Commit Retry Logic
# ---------------------------
def safe_commit(batch_statements):
    for attempt in range(5):
        try:
            for stmt in batch_statements:
                session.execute(stmt)
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            print(f"⚠️ Commit attempt {attempt+1} failed: {e}")
            time.sleep(0.5 + attempt * 0.5)
    print("❌ Final commit failed after retries.")
    return False

# ---------------------------
# PROCESS RESULTS
# ---------------------------
insert_count = 0
update_count = 0
batch = []

if response.status_code == 200:
    games = response.json()
    print(f"✅ Fetched {len(games)} games")

    # Pull existing valid game_ids from DB
    existing_game_ids = {row[0] for row in session.execute(select(games_table.c.game_id))}

    for game in games:
        game_id = game.get('id')
        home = game.get('home_team')
        away = game.get('away_team')

        # Only insert odds if the game exists in our schedule
        if game_id not in existing_game_ids:
            print(f"⏭ Skipping {home} vs {away} — not in schedule")
            continue

        for book in game.get('bookmakers', []):
            sportsbook = book.get('title', 'Unknown')
            for market in book.get('markets', []):
                market_key = market.get('key')
                for outcome in market.get('outcomes', []):
                    team = outcome.get('name')
                    price = outcome.get('price')
                    if not team or price is None:
                        continue

                    stmt = None
                    existing = session.execute(
                        select(odds_table).where(and_(
                            odds_table.c.game_id == game_id,
                            odds_table.c.team == team,
                            odds_table.c.market == market_key,
                            odds_table.c.sportsbook == sportsbook
                        ))
                    ).fetchone()

                    if existing:
                        stmt = update(odds_table).where(and_(
                            odds_table.c.game_id == game_id,
                            odds_table.c.team == team,
                            odds_table.c.market == market_key,
                            odds_table.c.sportsbook == sportsbook
                        )).values(
                            odds=price,
                            timestamp=datetime.utcnow()
                        )
                        update_count += 1
                        print(f"🔁 Updated: {team} | {market_key} | {sportsbook} => {price}")
                    else:
                        stmt = odds_table.insert().values(
                            game_id=game_id,
                            sportsbook=sportsbook,
                            team=team,
                            market=market_key,
                            odds=price,
                            timestamp=datetime.utcnow()
                        )
                        insert_count += 1
                        print(f"📥 Inserted: {team} | {market_key} | {sportsbook} => {price}")

                    batch.append(stmt)

                    if len(batch) >= 50:
                        safe_commit(batch)
                        batch.clear()

    if batch:
        safe_commit(batch)

    # 🧹 Remove duplicates: keep only the latest odds per group
    session.execute(text("""
        DELETE FROM odds
        WHERE id NOT IN (
            SELECT MAX(id)
            FROM odds
            GROUP BY game_id, sportsbook, team, market
        )
    """))
    session.commit()
    print("🧼 Cleaned up old duplicate odds")
    session.close()
    print(f"✅ Done. Inserted: {insert_count} | Updated: {update_count}")
else:
    print("❌ Failed to fetch odds")
    print("Status:", response.status_code)
    print("Message:", response.text)
