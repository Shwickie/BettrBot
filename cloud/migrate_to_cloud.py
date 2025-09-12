# migrate_to_cloud.py
"""
Migrate SQLite database to cloud PostgreSQL (Supabase) safely.

Fixes:
- Adaptive chunksize to avoid "too many parameters" errors on big/wide inserts.
- Normalizes games date/time columns before upload.
- If games is empty but games_backup has data, auto-fill games from backup.
- REPLACE on schema mismatch, APPEND when compatible.
- For `odds`: force a truncate+append each run to avoid duplicate PKs, and use
  smaller batches + timestamp/odds normalization to prevent driver errors.
"""

import os
import sqlite3
from typing import Optional, Sequence

import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine

# === CONFIG ===
SQLITE_PATH = r"E:\Bettr Bot\betting-bot\data\betting.db"
POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    # keep the default but ALWAYS set POSTGRES_URL on Render
    "postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres?sslmode=require",
)

ESSENTIAL_TABLES: Sequence[str] = (
    "games",
    "odds",
    "team_season_summary",
    "system_status",
    "player_stats_2024",
    "current_nfl_players",
)

SKIP_TABLES = {
    "sqlite_sequence", "sqlite_stat1", "sqlite_stat2", "sqlite_stat3",
    "sqlite_stat4", "alembic_version"
}

# Base target; will be lowered automatically if needed per table
DEFAULT_BATCH_ROWS = 1000
# Keep total parameters in a single INSERT well below ~65k
MAX_PARAMS_PER_INSERT = 50000

# Tables we always clear and re-load to keep runs idempotent (avoid duplicate PKs)
FORCE_TRUNCATE_APPEND = {"odds"}
# Per-table batch-size overrides
SPECIAL_BATCH_ROWS = {"odds": 200}


def connect_sqlite(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=OFF;")
    return conn


def connect_postgres(url: str) -> Engine:
    return create_engine(url, pool_pre_ping=True)


def pg_table_columns(engine: Engine, table_name: str) -> Optional[set]:
    insp = inspect(engine)
    if not insp.has_table(table_name):
        return None
    return {col["name"] for col in insp.get_columns(table_name)}


def safe_rows_per_insert(num_cols: int, table: Optional[str] = None) -> int:
    # respect per-table override first
    if table and table in SPECIAL_BATCH_ROWS:
        return SPECIAL_BATCH_ROWS[table]
    if num_cols <= 0:
        return DEFAULT_BATCH_ROWS
    cap = MAX_PARAMS_PER_INSERT // num_cols
    return max(100, min(DEFAULT_BATCH_ROWS, cap))


def _to_date(val):
    try:
        ts = pd.to_datetime(val, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.date()
    except Exception:
        return None


def _to_hms(val):
    # Accept strings like "13:00", "13:00:00", "2024-09-01 13:00", etc.
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if s == "" or s.lower() in {"none", "nan", "nat"}:
        return None

    # First try strict time-only formats
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return pd.to_datetime(s, format=fmt).time()
        except Exception:
            pass

    # Fallback: general parser (handles full datetimes)
    try:
        ts = pd.to_datetime(s, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.time()
    except Exception:
        return None


def normalize_games_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "game_date" in df.columns:
        df["game_date"] = df["game_date"].apply(_to_date)
    if "start_time_local" in df.columns:
        df["start_time_local"] = df["start_time_local"].apply(_to_hms)
    return df


def normalize_odds_df(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure odds types are stable for Postgres schema."""
    df = df.copy()
    # expected: id, game_id, team, sportsbook, odds, market, timestamp
    if "timestamp" in df.columns:
        ts = pd.to_datetime(df["timestamp"], errors="coerce", utc=False)
        try:
            # make tz-naive for TIMESTAMP WITHOUT TIME ZONE
            df["timestamp"] = ts.dt.tz_localize(None)
        except Exception:
            df["timestamp"] = ts
    if "odds" in df.columns:
        df["odds"] = pd.to_numeric(df["odds"], errors="coerce")
    for c in ("team", "game_id", "sportsbook", "market"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df


def write_df(df: pd.DataFrame, engine: Engine, table: str, *, replace: bool) -> None:
    if df.empty:
        print(f"   ⚠️ {table}: source is empty, skipping")
        return

    # adapt chunk size to stay under param limits / table overrides
    chunk_rows = safe_rows_per_insert(len(df.columns), table)
    first = True if replace else False
    total = len(df)

    for start in range(0, total, chunk_rows):
        chunk = df.iloc[start:start + chunk_rows]
        mode = "replace" if first else "append"
        chunk.to_sql(
            table,
            engine,
            if_exists=mode,
            index=False,
            method="multi",
            chunksize=chunk_rows,
        )
        first = False
        done = min(start + chunk_rows, total)
        pct = done / total * 100
        print(f"   📤 {done:,}/{total:,} rows migrated ({pct:.1f}%) [{table}] {mode.upper() if mode=='replace' else 'append'}")


def migrate_table(pg: Engine, sqlite_conn: sqlite3.Connection, table: str) -> bool:
    print(f"\n📊 Migrating table: {table}")

    try:
        df = pd.read_sql_query(f'SELECT * FROM "{table}"', sqlite_conn)
    except Exception as e:
        print(f"   ❌ Failed to read from SQLite: {e}")
        return False

    # Per-table normalization
    if table == "games":
        df = normalize_games_df(df)
    elif table == "odds":
        df = normalize_odds_df(df)

    if df.empty:
        print("   ⚠️ No rows, skipping")
        return True

    # Idempotent path: certain tables are always cleared then appended
    if table in FORCE_TRUNCATE_APPEND:
        try:
            with pg.begin() as c:
                c.execute(text(f'TRUNCATE TABLE "{table}" RESTART IDENTITY'))
            print("   🧹 Truncated destination table (RESTART IDENTITY)")
            write_df(df, pg, table, replace=False)
            print("   ✅ Truncate+Append complete")
            return True
        except Exception as e:
            print(f"   ⚠️ Truncate path failed ({e}); falling back to REPLACE")
            try:
                write_df(df, pg, table, replace=True)
                print("   ✅ Replaced & migrated (fallback)")
                return True
            except Exception as e2:
                print(f"   ❌ Replace fallback failed: {e2}")
                return False

    try:
        dest_cols = pg_table_columns(pg, table)
        if dest_cols is None:
            print("   🆕 Destination table not found; creating it from DataFrame schema")
            write_df(df, pg, table, replace=True)
            print("   ✅ Created & migrated")
            return True

        df_cols = list(df.columns)
        missing = [c for c in df_cols if c not in dest_cols]
        if not missing:
            ordered_cols = [c for c in dest_cols if c in df.columns]
            df2 = df[ordered_cols]
            write_df(df2, pg, table, replace=False)
            print("   ✅ Appended (schemas compatible)")
            return True
        else:
            print(f"   ⚠️ Schema mismatch. Missing in destination: {missing[:8]}{' ...' if len(missing)>8 else ''}")
            print("   🔁 Replacing destination table to match source schema (one-time destructive change)")
            write_df(df, pg, table, replace=True)
            print("   ✅ Replaced & migrated")
            return True

    except Exception as e:
        msg = str(e)
        print(f"   ❌ Append/Replace failed: {msg}")
        # Common recoverable schema/param errors
        hints = (
            "UndefinedColumn", "does not exist", "INSERT has more expressions than target columns",
            "bind parameters", "too many arguments", "too many parameters", "duplicate key",
            "UniqueViolation"
        )
        if any(h in msg for h in hints):
            try:
                print("   🔁 Retrying with REPLACE & safe chunks due to error")
                write_df(df, pg, table, replace=True)
                print("   ✅ Replaced & migrated (retry)")
                return True
            except Exception as e2:
                print(f"   ❌ Replace retry failed: {e2}")
        return False


def ensure_core_schemas(pg: Engine) -> bool:
    print("\n🏗️ Ensuring essential table schemas exist (non-destructive)")
    ddls = {
        "games": """
            CREATE TABLE IF NOT EXISTS games (
                id SERIAL PRIMARY KEY,
                game_id TEXT,
                home_team TEXT,
                away_team TEXT,
                game_date DATE,
                start_time_local TIME,
                home_score INTEGER,
                away_score INTEGER,
                season INTEGER
            )
        """,
        "odds": """
            CREATE TABLE IF NOT EXISTS odds (
                id SERIAL PRIMARY KEY,
                game_id TEXT,
                team TEXT,
                sportsbook TEXT,
                odds DOUBLE PRECISION,
                market TEXT,
                timestamp TIMESTAMP
            )
        """,
        "team_season_summary": """
            CREATE TABLE IF NOT EXISTS team_season_summary (
                id SERIAL PRIMARY KEY,
                team TEXT,
                season INTEGER,
                power_score DOUBLE PRECISION,
                wins INTEGER,
                losses INTEGER,
                games_played INTEGER,
                win_pct DOUBLE PRECISION,
                avg_points_for DOUBLE PRECISION,
                avg_points_against DOUBLE PRECISION,
                point_diff DOUBLE PRECISION
            )
        """,
        "system_status": """
            CREATE TABLE IF NOT EXISTS system_status (
                id SERIAL PRIMARY KEY,
                task TEXT,
                started_at TEXT,
                finished_at TEXT,
                status TEXT,
                message TEXT,
                run_type TEXT DEFAULT 'cloud',
                timeout_seconds INTEGER
            )
        """,
    }
    try:
        with pg.begin() as c:
            for t, ddl in ddls.items():
                c.execute(text(ddl))
                print(f"   ✅ ensured: {t}")
        return True
    except Exception as e:
        print(f"   ❌ Schema ensure failed: {e}")
        return False


def list_sqlite_tables(conn: sqlite3.Connection) -> list[str]:
    q = ("SELECT name FROM sqlite_master WHERE type='table' "
         "AND name NOT LIKE 'sqlite_%' ORDER BY 1;")
    rows = conn.execute(q).fetchall()
    return [r[0] for r in rows if r[0] not in SKIP_TABLES]


def table_count(pg: Engine, name: str) -> int:
    try:
        with pg.connect() as c:
            res = c.execute(text(f'SELECT COUNT(*) FROM "{name}"'))
            return int(res.scalar() or 0)
    except Exception:
        return -1


def verify_counts(pg: Engine, tables: Sequence[str]) -> None:
    print("\n🔍 Verifying migration counts (Postgres)")
    try:
        with pg.connect() as conn:
            for t in tables:
                try:
                    n = conn.execute(text(f'SELECT COUNT(*) FROM "{t}"')).scalar() or 0
                    print(f"   📊 {t}: {int(n):,} rows")
                except Exception as e:
                    print(f"   ❌ {t}: {e}")
    except Exception as e:
        print(f"   ❌ Verification failed: {e}")


def maybe_fill_games_from_backup(pg: Engine, sqlite_conn: sqlite3.Connection) -> None:
    # If games has 0 rows but games_backup exists with data, copy it into games
    g = table_count(pg, "games")
    if g == 0:
        try:
            df_backup = pd.read_sql_query('SELECT * FROM "games_backup"', sqlite_conn)
        except Exception:
            df_backup = pd.DataFrame()
        if not df_backup.empty:
            print("\n🩹 games is empty; filling from games_backup...")
            df_backup = normalize_games_df(df_backup)
            write_df(df_backup, pg, "games", replace=True)
            print("   ✅ games populated from games_backup")
        else:
            print("\nℹ️ games is empty and games_backup not found or empty; skipping fill.")


def main():
    print("🚀 MIGRATING BETTR BOT TO CLOUD DATABASE")
    print("=" * 56)
    # Connections
    try:
        sqlite_conn = connect_sqlite(SQLITE_PATH)
        sqlite_conn.execute("SELECT 1").fetchone()
        pg = connect_postgres(POSTGRES_URL)
        with pg.connect() as c:
            c.execute(text("SELECT 1")).fetchone()
        print("✅ Database connections established")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return

    if not ensure_core_schemas(pg):
        print("❌ Failed to ensure schemas; aborting.")
        return

    tables = list_sqlite_tables(sqlite_conn)
    print(f"📋 Found {len(tables)} tables in SQLite")

    essentials = [t for t in ESSENTIAL_TABLES if t in tables]
    others = [t for t in tables if t not in essentials]

    ok = fail = 0
    print("\n📦 Migrating ESSENTIAL tables first...")
    for t in essentials:
        if migrate_table(pg, sqlite_conn, t):
            ok += 1
        else:
            fail += 1

    print("\n📦 Migrating remaining tables...")
    for t in others:
        if migrate_table(pg, sqlite_conn, t):
            ok += 1
        else:
            fail += 1

    # If games is empty but games_backup has data, fill it
    maybe_fill_games_from_backup(pg, sqlite_conn)

    print("\n🎉 MIGRATION COMPLETE")
    print(f"✅ Successful: {ok}")
    print(f"❌ Failed: {fail}")

    # Verify
    verify_sample = list(dict.fromkeys([*essentials, *others[:5]]))
    verify_counts(pg, verify_sample)

    sqlite_conn.close()
    pg.dispose()
    print("\n🌐 Next Steps:")
    print("1) Point your app at POSTGRES_URL (Render env).")
    print("2) Run app locally against cloud DB to sanity-check pages.")
    print("3) Commit & deploy to Render.")


if __name__ == "__main__":
    main()
