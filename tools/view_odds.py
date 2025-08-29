from sqlalchemy import create_engine, MetaData, Table, select

# Connect to the correct DB path
engine = create_engine("sqlite:///E:/Bettr Bot/betting-bot/data/betting.db")

metadata = MetaData()
metadata.reflect(bind=engine)

# Print available tables
print("📋 Tables in DB:", metadata.tables.keys())

# Use the actual existing table
table_name = 'odds'
if table_name not in metadata.tables:
    print(f"❌ Table '{table_name}' not found.")
    exit()

odds_table = metadata.tables[table_name]

# Query and print last 10 rows
with engine.connect() as conn:
    stmt = select(odds_table).limit(10)
    result = conn.execute(stmt)
    rows = result.fetchall()

    print(f"\n🧾 Showing last {len(rows)} rows from '{table_name}':")
    for row in rows:
        print(dict(row._mapping))

