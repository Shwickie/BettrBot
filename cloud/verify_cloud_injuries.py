# verify_cloud_injuries.py
from sqlalchemy import create_engine, text
import pandas as pd

DATABASE_URL = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"

engine = create_engine(DATABASE_URL)

with engine.connect() as conn:
    # Check what's in the injury table
    injuries = pd.read_sql(text("""
        SELECT team, position, designation, player_name, is_active
        FROM nfl_injuries_tracking
        WHERE is_active = true
        ORDER BY team
        LIMIT 20
    """), conn)
    
    print("ACTUAL INJURIES IN CLOUD:")
    print(injuries.to_string())
    
    # Check validation table (aggregates)
    validation = pd.read_sql(text("""
        SELECT * FROM ai_injury_validation_detail LIMIT 10
    """), conn)
    
    print("\n\nVALIDATION TABLE (AGGREGATES):")
    print(validation.to_string())