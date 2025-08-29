from sqlalchemy import create_engine, MetaData, inspect, text


DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)
inspector = inspect(engine)

print("📋 Tables in the database:")
for table_name in inspector.get_table_names():
    print(f" - {table_name}")
    columns = inspector.get_columns(table_name)
    print(f"   🔍 {len(columns)} columns, example columns: {[col['name'] for col in columns[:5]]}")

    with engine.connect() as conn:
        try:
            result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT 1")).fetchall()
            if result:
                print(f"   📌 Example row: {dict(result[0]._mapping)}")
            else:
                print("   ⚠️ Table is empty.")
        except Exception as e:
            print(f"   ❌ Error querying table: {e}")
