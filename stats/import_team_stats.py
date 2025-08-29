import os
import pandas as pd
from sqlalchemy import create_engine

# Config
DATA_DIR = "E:/Bettr Bot/betting-bot/data"
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"

# Connect to database
engine = create_engine(DB_PATH)

# Loop through all CSVs that start with team_stats_
for filename in os.listdir(DATA_DIR):
    if filename.startswith("team_stats_") and filename.endswith(".csv"):
        year = filename.replace("team_stats_", "").replace(".csv", "")
        table_name = f"team_stats_{year}"
        file_path = os.path.join(DATA_DIR, filename)

        print(f"📄 Loading: {filename} → Table: {table_name}")
        
        # Load CSV
        df = pd.read_csv(file_path)

        # Clean column names
        df.columns = [col.strip().replace(" ", "_").lower() for col in df.columns]

        # Drop table if exists, then recreate
        with engine.begin() as conn:
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            df.to_sql(table_name, conn, index=False)

        print(f"✅ Imported {len(df)} rows into {table_name}")

print("🎯 Done importing all team stats.")
