from sqlalchemy import create_engine, text

# Your Railway URL (replace ******** with actual password)
DATABASE_URL = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"

def test_connection():
    """Test Railway database connection"""
    try:
        engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,
            connect_args={
                "connect_timeout": 10,
                "application_name": "bettr-bot-test"
            }
        )
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.scalar()
            print(f"✅ Connected to Railway PostgreSQL!")
            print(f"   Version: {version[:50]}...")
            
            # Check if tables exist
            tables = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)).fetchall()
            
            print(f"\n📊 Found {len(tables)} tables:")
            for table in tables:
                print(f"   - {table[0]}")
            
            return True
            
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    test_connection()