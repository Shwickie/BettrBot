from sqlalchemy import create_engine, MetaData

DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)
metadata = MetaData()
metadata.reflect(bind=engine)

print("📋 Player Stats Tables and Columns:")
for table_name in metadata.tables:
    if table_name.startswith("player_stats_"):
        columns = metadata.tables[table_name].columns.keys()
        print(f" - {table_name}: {list(columns)}")
