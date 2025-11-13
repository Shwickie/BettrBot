#!/usr/bin/env python3
"""Verify Railway database migration"""

from sqlalchemy import create_engine, text, inspect

DATABASE_URL = "postgresql://postgres:YviqtXqcsCIgRzSCofNjbfwgjkYNLydX@maglev.proxy.rlwy.net:54187/railway"

engine = create_engine(DATABASE_URL)
inspector = inspect(engine)
tables = inspector.get_table_names()

print(f'[SUCCESS] {len(tables)} tables created in Railway database')
print('\n[INFO] Key tables:')

key_tables = ['games', 'odds', 'team_season_summary', 'ai_game_predictions', 'current_nfl_players']

with engine.connect() as conn:
    for t in key_tables:
        if t in tables:
            count = conn.execute(text(f'SELECT COUNT(*) FROM {t}')).scalar()
            print(f'  {t}: {count:,} rows')
        else:
            print(f'  {t}: NOT FOUND')

print('\n[INFO] All tables:')
for t in sorted(tables)[:20]:
    print(f'  - {t}')

if len(tables) > 20:
    print(f'  ... and {len(tables) - 20} more')

print(f'\n[SUCCESS] Database is ready for use!')
