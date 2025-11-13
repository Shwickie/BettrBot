from sqlalchemy import create_engine, text, inspect

engine = create_engine('postgresql://postgres:YviqtXqcsCIgRzSCofNjbfwgjkYNLydX@maglev.proxy.rlwy.net:54187/railway')
inspector = inspect(engine)
tables = inspector.get_table_names()
injury_tables = [t for t in tables if 'injury' in t.lower() or 'inj' in t.lower()]

print('Injury-related tables:')
for t in injury_tables:
    print(f'  - {t}')
    conn = engine.connect()
    count = conn.execute(text(f'SELECT COUNT(*) FROM {t}')).scalar()
    print(f'    Rows: {count}')
    conn.close()
