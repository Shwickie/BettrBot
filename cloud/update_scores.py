#!/usr/bin/env python3
"""
FIXED NFL Score Updater - Combines working local logic with cloud compatibility
Uses the proven team matching approach that was working locally
"""

import pandas as pd
import nfl_data_py as nfl
import os
from datetime import datetime
from sqlalchemy import create_engine, text
import logging
import sqlite3

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database setup - cloud or local
DATABASE_URL = "postgresql://postgres:YviqtXqcsCIgRzSCofNjbfwgjkYNLydX@maglev.proxy.rlwy.net:54187/railway"
if DATABASE_URL and DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

USE_CLOUD_DB = bool(DATABASE_URL)
SEASON = 2025

# Use your proven CANON mapping from the working local version
CANON = {
    # NFC West
    "San Francisco 49ers":"SF","49ers":"SF","SF":"SF","SFO":"SF",
    "Seattle Seahawks":"SEA","Seahawks":"SEA","SEA":"SEA",
    "Los Angeles Rams":"LAR","LA Rams":"LAR","Rams":"LAR","LAR":"LAR","STL":"LAR","St. Louis Rams":"LAR",
    "Arizona Cardinals":"ARI","Cardinals":"ARI","ARI":"ARI",
    # NFC North
    "Detroit Lions":"DET","Lions":"DET","DET":"DET",
    "Green Bay Packers":"GB","Packers":"GB","GB":"GB","GBP":"GB",
    "Chicago Bears":"CHI","Bears":"CHI","CHI":"CHI",
    "Minnesota Vikings":"MIN","Vikings":"MIN","MIN":"MIN",
    # NFC East
    "Philadelphia Eagles":"PHI","Eagles":"PHI","PHI":"PHI",
    "Dallas Cowboys":"DAL","Cowboys":"DAL","DAL":"DAL",
    "New York Giants":"NYG","Giants":"NYG","NYG":"NYG",
    "Washington Commanders":"WAS","Commanders":"WAS","WAS":"WAS","WSH":"WAS",
    # NFC South
    "Tampa Bay Buccaneers":"TB","Buccaneers":"TB","TB":"TB","TAM":"TB",
    "New Orleans Saints":"NO","Saints":"NO","NO":"NO","NOR":"NO",
    "Atlanta Falcons":"ATL","Falcons":"ATL","ATL":"ATL",
    "Carolina Panthers":"CAR","Panthers":"CAR","CAR":"CAR",
    # AFC West
    "Kansas City Chiefs":"KC","Chiefs":"KC","KC":"KC","KCC":"KC",
    "Los Angeles Chargers":"LAC","LA Chargers":"LAC","Chargers":"LAC","LAC":"LAC","SD":"LAC","San Diego Chargers":"LAC",
    "Denver Broncos":"DEN","Broncos":"DEN","DEN":"DEN",
    "Las Vegas Raiders":"LV","Raiders":"LV","LV":"LV","OAK":"LV","Oakland Raiders":"LV",
    # AFC North
    "Baltimore Ravens":"BAL","Ravens":"BAL","BAL":"BAL",
    "Cincinnati Bengals":"CIN","Bengals":"CIN","CIN":"CIN",
    "Cleveland Browns":"CLE","Browns":"CLE","CLE":"CLE",
    "Pittsburgh Steelers":"PIT","Steelers":"PIT","PIT":"PIT",
    # AFC South
    "Houston Texans":"HOU","Texans":"HOU","HOU":"HOU",
    "Indianapolis Colts":"IND","Colts":"IND","IND":"IND",
    "Jacksonville Jaguars":"JAX","Jaguars":"JAX","JAX":"JAX","JAC":"JAX",
    "Tennessee Titans":"TEN","Titans":"TEN","TEN":"TEN",
    # AFC East
    "Buffalo Bills":"BUF","Bills":"BUF","BUF":"BUF",
    "Miami Dolphins":"MIA","Dolphins":"MIA","MIA":"MIA",
    "New England Patriots":"NE","Patriots":"NE","NE":"NE","NWE":"NE",
    "New York Jets":"NYJ","Jets":"NYJ","NYJ":"NYJ",
}

def canon(team: str) -> str:
    """Convert team name to standard abbreviation using proven local logic"""
    if team is None:
        return ""
    t = str(team).strip()
    if t.upper() == "LA":  # keep ambiguous; expand later
        return "LA"
    return CANON.get(t, CANON.get(t.title(), t.upper()))

def _expand_la(df: pd.DataFrame, side: str) -> pd.DataFrame:
    """Expand LA ambiguity to both LAR and LAC - from working local version"""
    col = f"{side}_abbr"
    if col not in df.columns:
        return df
    m = df[col].eq("LA")
    if not m.any():
        return df
    a = df.loc[m].copy()
    a_lar = a.copy(); a_lar[col] = "LAR"
    a_lac = a.copy(); a_lac[col] = "LAC"
    return pd.concat([df.loc[~m], a_lar, a_lac], ignore_index=True)

def _resolve_columns(df: pd.DataFrame):
    """Find the right column names in NFL data"""
    dc = next((c for c in ("gameday","game_date","start_time") if c in df.columns), None)
    hc = next((c for c in ("home_team","home","team_home") if c in df.columns), None)
    ac = next((c for c in ("away_team","away","team_away") if c in df.columns), None)
    hsc = next((c for c in ("home_score","score_home") if c in df.columns), None)
    asc = next((c for c in ("away_score","score_away") if c in df.columns), None)
    wk  = next((c for c in ("week","game_week","wk") if c in df.columns), None)
    return dc, hc, ac, hsc, asc, wk

def get_engine():
    """Get database engine for cloud or local"""
    if USE_CLOUD_DB:
        return create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300)
    else:
        # Fallback to local SQLite
        local_db = r"E:/Bettr Bot/betting-bot/data/betting.db"
        return create_engine(f"sqlite:///{local_db}")

def update_all_completed_games() -> int:
    """Main function - uses your working local logic with cloud compatibility"""
    logger.info("NFL WEEKLY SCORE UPDATER - FIXED VERSION")
    logger.info("=" * 60)
    logger.info(f"Running at: {datetime.now():%Y-%m-%d %H:%M}")
    logger.info(f"Database: {'Cloud PostgreSQL' if USE_CLOUD_DB else 'Local SQLite'}")
    logger.info(f"Fetching NFL data for {SEASON} season...")

    # Get NFL data
    try:
        nfl_df = nfl.import_schedules([SEASON])
        if nfl_df is None or nfl_df.empty:
            logger.error("No NFL data available")
            return 0
        logger.info(f"Retrieved {len(nfl_df)} games from NFL API")
    except Exception as e:
        logger.error(f"Failed to fetch NFL data: {e}")
        return 0

    # Find column names
    date_col, home_col, away_col, hs_col, as_col, _ = _resolve_columns(nfl_df)
    if None in (date_col, home_col, away_col, hs_col, as_col):
        logger.error(f"Missing required columns. Available: {list(nfl_df.columns)}")
        return 0

    # Clean up date column
    nfl_df[date_col] = pd.to_datetime(nfl_df[date_col], errors="coerce")

    # Get completed games only
    nfl_completed = nfl_df.loc[
        nfl_df[hs_col].notna() & nfl_df[as_col].notna(),
        [date_col, home_col, away_col, hs_col, as_col]
    ].copy()
    
    if nfl_completed.empty:
        logger.info("No completed games found in NFL data")
        return 0

    logger.info(f"Found {len(nfl_completed)} completed games")

    # Apply your proven canonicalization logic
    nfl_completed["game_date"] = pd.to_datetime(nfl_completed[date_col]).dt.date
    nfl_completed["home_abbr_raw"] = nfl_completed[home_col].astype(str)
    nfl_completed["away_abbr_raw"] = nfl_completed[away_col].astype(str)
    nfl_completed["home_abbr"] = nfl_completed["home_abbr_raw"].map(canon)
    nfl_completed["away_abbr"] = nfl_completed["away_abbr_raw"].map(canon)

    # Show what we found
    logger.info("Recent completed games from NFL:")
    for _, g in nfl_completed.sort_values("game_date").tail(5).iterrows():
        logger.info(f"  {g['game_date']:%m-%d}: {g['away_abbr_raw']} {int(g[as_col])} @ {g['home_abbr_raw']} {int(g[hs_col])}")

    # Apply LA expansion (your working logic)
    nfl_completed = _expand_la(nfl_completed, "home")
    nfl_completed = _expand_la(nfl_completed, "away")

    # Get database games needing scores - FIXED to focus on games that should be completed
    engine = get_engine()
    today = datetime.now().date()
    
    if USE_CLOUD_DB:
        with engine.connect() as conn:
            db_games = pd.read_sql_query(text("""
                SELECT id, DATE(game_date) AS game_date, home_team, away_team,
                       home_score, away_score
                FROM games
                WHERE (home_score IS NULL OR away_score IS NULL)
                AND EXTRACT(YEAR FROM game_date) = :season
                AND DATE(game_date) <= :today
                ORDER BY game_date DESC
            """), conn, params={"season": SEASON, "today": today})
    else:
        with engine.connect() as conn:
            db_games = pd.read_sql_query(text("""
                SELECT id, date(game_date) AS game_date, home_team, away_team,
                       home_score, away_score
                FROM games
                WHERE (home_score IS NULL OR away_score IS NULL)
                AND strftime('%Y', game_date) = :season
                AND date(game_date) <= :today
                ORDER BY game_date DESC
            """), conn, params={"season": str(SEASON), "today": today})

    logger.info(f"Database games needing scores: {len(db_games)}")
    
    if db_games.empty:
        logger.info("All games already have scores.")
        return 0

    # Apply canonicalization to DB data
    db_games["game_date"] = pd.to_datetime(db_games["game_date"], errors="coerce").dt.date
    db_games["home_abbr"] = db_games["home_team"].map(canon)
    db_games["away_abbr"] = db_games["away_team"].map(canon)
    db_games = _expand_la(db_games, "home")
    db_games = _expand_la(db_games, "away")

    # Join using your proven logic
    nfl_slim = nfl_completed[["game_date","home_abbr","away_abbr",hs_col,as_col]].rename(
        columns={hs_col:"home_score_nfl", as_col:"away_score_nfl"}
    )
    
    # Direct match
    merged_direct = db_games.merge(nfl_slim, on=["game_date","home_abbr","away_abbr"], how="inner")
    
    # Swapped match (in case home/away reversed)
    nfl_swapped = nfl_slim.rename(columns={
        "home_abbr":"away_abbr","away_abbr":"home_abbr",
        "home_score_nfl":"away_score_nfl","away_score_nfl":"home_score_nfl"
    })
    merged_swapped = db_games.merge(nfl_swapped, on=["game_date","home_abbr","away_abbr"], how="inner")
    
    merged = pd.concat([merged_direct, merged_swapped], ignore_index=True)

    logger.info(f"Matches found after canonical join: {len(merged)}")
    
    if merged.empty:
        logger.info("No matches found - debugging info:")
        logger.info("Sample NFL teams (canonicalized):")
        sample_nfl = nfl_completed.tail(5)[["game_date","away_abbr_raw","home_abbr_raw","away_abbr","home_abbr"]]
        for _, row in sample_nfl.iterrows():
            logger.info(f"  {row['game_date']}: {row['away_abbr_raw']}->{row['away_abbr']} @ {row['home_abbr_raw']}->{row['home_abbr']}")
        
        logger.info("Sample DB teams (canonicalized):")
        sample_db = db_games.head(5)[["game_date","away_team","home_team","away_abbr","home_abbr"]]
        for _, row in sample_db.iterrows():
            logger.info(f"  {row['game_date']}: {row['away_team']}->{row['away_abbr']} @ {row['home_team']}->{row['home_abbr']}")
        
        return 0

    # Update scores in database
    updates_made = 0
    
    if USE_CLOUD_DB:
        with engine.connect() as conn:
            for _, r in merged.iterrows():
                try:
                    result = conn.execute(text("""
                        UPDATE games 
                        SET home_score = :home_score, away_score = :away_score 
                        WHERE id = :id
                    """), {
                        "home_score": int(r["home_score_nfl"]),
                        "away_score": int(r["away_score_nfl"]),
                        "id": int(r["id"])
                    })
                    
                    if result.rowcount == 1:
                        updates_made += 1
                        
                except Exception as e:
                    logger.error(f"Error updating game {r['id']}: {e}")
            
            conn.commit()
    else:
        # SQLite version using raw connection for compatibility
        with sqlite3.connect(r"E:/Bettr Bot/betting-bot/data/betting.db") as conn:
            cur = conn.cursor()
            for _, r in merged.iterrows():
                try:
                    hs = int(r["home_score_nfl"])
                    as_ = int(r["away_score_nfl"])
                    pk = int(r["id"])
                    cur.execute("UPDATE games SET home_score=?, away_score=? WHERE id=?", (hs, as_, pk))
                    if cur.rowcount == 1:
                        updates_made += 1
                except Exception as e:
                    logger.error(f"Error updating game {r['id']}: {e}")
            conn.commit()

    logger.info(f"Successfully updated: {updates_made}")
    
    if updates_made > 0:
        logger.info(f"SUCCESS: Updated {updates_made} games with scores!")
    else:
        logger.info("No new scores written.")
    
    return updates_made

def main():
    """Main function"""
    try:
        return update_all_completed_games()
    except Exception as e:
        logger.error(f"Update failed: {e}")
        import traceback
        traceback.print_exc()
        return 0

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)