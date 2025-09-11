#!/usr/bin/env python3
"""
FIXED Team Season Summary - Works with mixed team name formats
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, Float, String
from sqlalchemy.pool import NullPool

DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(
    DB_PATH,
    connect_args={"check_same_thread": False, "timeout": 30},
    pool_pre_ping=True,
    poolclass=NullPool,   # <- no pooling for this script
)

TEAM_TO_ABBR = {
    "Arizona Cardinals":"ARI","Atlanta Falcons":"ATL","Baltimore Ravens":"BAL","Buffalo Bills":"BUF",
    "Carolina Panthers":"CAR","Chicago Bears":"CHI","Cincinnati Bengals":"CIN","Cleveland Browns":"CLE",
    "Dallas Cowboys":"DAL","Denver Broncos":"DEN","Detroit Lions":"DET","Green Bay Packers":"GB",
    "Houston Texans":"HOU","Indianapolis Colts":"IND","Jacksonville Jaguars":"JAX","Kansas City Chiefs":"KC",
    "Las Vegas Raiders":"LV","Los Angeles Chargers":"LAC","Los Angeles Rams":"LAR","Miami Dolphins":"MIA",
    "Minnesota Vikings":"MIN","New England Patriots":"NE","New Orleans Saints":"NO","New York Giants":"NYG",
    "New York Jets":"NYJ","Philadelphia Eagles":"PHI","Pittsburgh Steelers":"PIT","San Francisco 49ers":"SF",
    "Seattle Seahawks":"SEA","Tampa Bay Buccaneers":"TB","Tennessee Titans":"TEN","Washington Commanders":"WAS",
}

# Hardcoded preseason records for 2025 (temporary until scores are in DB)
PRESEASON_2025 = {
    "IND": (1, 2, 0), "BAL": (3, 0, 0), "CIN": (1, 2, 0), "PHI": (2, 1, 0),
    "LV":  (0, 2, 1), "SEA": (1, 1, 1), "DET": (1, 3, 0), "ATL": (0, 3, 0),
    "CLE": (3, 0, 0), "CAR": (0, 3, 0), "WAS": (0, 3, 0), "NE":  (2, 1, 0),
    "NYG": (3, 0, 0), "BUF": (1, 2, 0), "HOU": (2, 1, 0), "MIN": (1, 2, 0),
    "PIT": (2, 1, 0), "JAX": (0, 2, 1), "DAL": (1, 2, 0), "LAR":  (2, 1, 0),
    "TEN": (2, 1, 0), "TB":  (2, 1, 0), "KC":  (0, 3, 0), "ARI": (2, 1, 0),
    "NYJ": (1, 2, 0), "GB":  (2, 1, 0), "DEN": (3, 0, 0), "SF":  (2, 1, 0),
    "MIA": (2, 0, 1), "CHI": (2, 0, 1), "NO":  (0, 2, 1), "LAC": (2, 2, 0),
}

def _infer_season(dt: pd.Timestamp) -> int | None:
    if pd.isna(dt): return None
    return dt.year - 1 if dt.month in (1, 2) else dt.year

def _is_preseason(dt: pd.Timestamp) -> bool:
    if pd.isna(dt):
        return False
    # NFL preseason is in August; Week 1 in early Sept is regular season
    return (dt.month == 8)


def get_team_player_stats():
    """Get star players and superstars from roster data"""
    try:
        with engine.connect() as conn:
            # Try to get player stats from roster or player_stats table
            player_stats = pd.read_sql(text("""
                SELECT team, 
                       COUNT(CASE WHEN overall_rating >= 85 AND overall_rating < 90 THEN 1 END) as star_players,
                       COUNT(CASE WHEN overall_rating >= 90 THEN 1 END) as superstars
                FROM roster 
                WHERE overall_rating IS NOT NULL
                GROUP BY team
            """), conn)
            
            if not player_stats.empty:
                return player_stats
                
        # Fallback: try player_stats table
        with engine.connect() as conn:
            player_stats = pd.read_sql(text("""
                SELECT team,
                       COUNT(CASE WHEN rating >= 85 AND rating < 90 THEN 1 END) as star_players,
                       COUNT(CASE WHEN rating >= 90 THEN 1 END) as superstars
                FROM player_stats
                WHERE rating IS NOT NULL
                GROUP BY team
            """), conn)
            
            if not player_stats.empty:
                return player_stats
                
    except Exception as e:
        print(f"Could not load player stats: {e}")
    
    # Final fallback: estimated star players based on team quality (both formats)
    estimated_stars = {
        "KC": (8, 3), "Kansas City Chiefs": (8, 3),
        "BAL": (7, 2), "Baltimore Ravens": (7, 2), 
        "BUF": (6, 2), "Buffalo Bills": (6, 2),
        "SF": (7, 2), "San Francisco 49ers": (7, 2),
        "PHI": (6, 2), "Philadelphia Eagles": (6, 2),
        "CIN": (6, 2), "Cincinnati Bengals": (6, 2),
        "DAL": (6, 1), "Dallas Cowboys": (6, 1),
        "MIA": (5, 1), "Miami Dolphins": (5, 1),
        "MIN": (5, 1), "Minnesota Vikings": (5, 1),
        "DET": (5, 1), "Detroit Lions": (5, 1),
        "LAC": (5, 1), "Los Angeles Chargers": (5, 1),
        "GB": (5, 1), "Green Bay Packers": (5, 1),
        "TEN": (4, 1), "Tennessee Titans": (4, 1),
        "PIT": (4, 1), "Pittsburgh Steelers": (4, 1),
        "SEA": (4, 1), "Seattle Seahawks": (4, 1),
        "LAR": (4, 1), "Los Angeles Rams": (4, 1),
        "JAX": (4, 0), "Jacksonville Jaguars": (4, 0),
        "LV": (3, 0), "Las Vegas Raiders": (3, 0),
        "CLE": (3, 0), "Cleveland Browns": (3, 0),
        "ATL": (3, 0), "Atlanta Falcons": (3, 0),
        "TB": (3, 0), "Tampa Bay Buccaneers": (3, 0),
        "IND": (3, 0), "Indianapolis Colts": (3, 0),
        "DEN": (3, 0), "Denver Broncos": (3, 0),
        "HOU": (3, 0), "Houston Texans": (3, 0),
        "NYJ": (2, 0), "New York Jets": (2, 0),
        "NE": (2, 0), "New England Patriots": (2, 0),
        "NYG": (2, 0), "New York Giants": (2, 0),
        "WAS": (2, 0), "Washington Commanders": (2, 0),
        "CHI": (2, 0), "Chicago Bears": (2, 0),
        "ARI": (2, 0), "Arizona Cardinals": (2, 0),
        "CAR": (1, 0), "Carolina Panthers": (1, 0),
        "NO": (1, 0), "New Orleans Saints": (1, 0)
    }
    
    fallback_df = pd.DataFrame([
        {"team": team, "star_players": stars, "superstars": supers}
        for team, (stars, supers) in estimated_stars.items()
    ])
    
    return fallback_df

def calculate_initial_power_from_preseason(team: str, season: int) -> float:
    """Calculate initial power score based on preseason performance"""
    if season == 2025:
        # Handle both full names and abbreviations
        team_abbr = team
        if team in TEAM_TO_ABBR.values():  # Full name
            # Find the abbreviation
            for full_name, abbr in TEAM_TO_ABBR.items():
                if full_name == team:
                    team_abbr = abbr
                    break
        
        if team_abbr in PRESEASON_2025:
            w, l, t = PRESEASON_2025[team_abbr]
            games = w + l + t
            if games == 0:
                return 0.0
            
            win_pct = (w + 0.5 * t) / games
            power = (win_pct - 0.5) * 10  # Base power
            
            # Bonuses/penalties
            if l == 0 and games >= 3:  # Undefeated
                power += 2.0
            elif w == 0 and games >= 3:  # Winless
                power -= 2.0
            
            return round(power, 1)
    return 0.0

def calculate_power_from_actual_preseason(season_df, all_completed_games, target_season):
    """Calculates initial power scores from actual preseason game results using a robust merge."""

    pre_games = all_completed_games[
        (all_completed_games['season'] == target_season) & 
        (all_completed_games['is_preseason'] == True)
    ].copy()

    if pre_games.empty:
        print(f"No completed preseason games found in database for {target_season}. Power scores will be based on talent only.")
        return pd.Series([0.0] * len(season_df), index=season_df.index)

    pre_games['home_win'] = (pre_games['home_score'] > pre_games['away_score']).astype(int)
    pre_games['away_win'] = (pre_games['away_score'] > pre_games['home_score']).astype(int)
    pre_games['tie'] = (pre_games['home_score'] == pre_games['away_score']).astype(int)

    home = pre_games.groupby('home_team').agg(
        wins=('home_win', 'sum'), losses=('away_win', 'sum'), ties=('tie', 'sum')
    ).rename_axis('team')

    away = pre_games.groupby('away_team').agg(
        wins=('away_win', 'sum'), losses=('home_win', 'sum'), ties=('tie', 'sum')
    ).rename_axis('team')

    pre_results = pd.concat([home, away]).groupby('team').sum()

    pre_results['games'] = pre_results['wins'] + pre_results['losses'] + pre_results['ties']
    pre_results['win_pct'] = (pre_results['wins'] + 0.5 * pre_results['ties']) / pre_results['games']
    pre_results['preseason_power'] = (pre_results['win_pct'] - 0.5) * 10

    # Use a left merge to safely map power scores back to the original team list
    power_df = pre_results[['preseason_power']].reset_index()
    merged = pd.merge(season_df[['team']], power_df, on='team', how='left')

    # Fill NaNs for any teams that didn't play preseason and return the final Series
    return merged['preseason_power'].fillna(0.0)

def get_season_carryover_power(season: int):
    """Fetches the final power scores from the previous season."""
    print(f"Fetching carryover power from season {season - 1}...")
    try:
        with engine.connect() as conn:
            last_season_power = pd.read_sql(
                text("SELECT team, power_score FROM team_season_summary WHERE season = :s"),
                conn,
                params={'s': season - 1}
            )
            if not last_season_power.empty:
                # Return as a dictionary for easy lookup: {'KC': 10.5, 'BUF': 9.8}
                return pd.Series(last_season_power.power_score.values, index=last_season_power.team).to_dict()
    except Exception as e:
        print(f"Could not fetch carryover power: {e}")
    return {}




def calculate_team_season_summary():
    print("CALCULATING TEAM SEASON SUMMARY - FIXED VERSION")
    print("=" * 50)
    
    # Load all scheduled/completed games
    with engine.connect() as conn:
        games_all = pd.read_sql(
            text("""
                SELECT game_id, home_team, away_team, home_score, away_score, game_date
                FROM games
            """), conn
        )

    if games_all.empty:
        print("No games found. Writing empty team_season_summary.")
        empty = pd.DataFrame(columns=[
            "season","team","games_played","avg_points_for","avg_points_against",
            "wins","losses","win_pct","point_diff","star_players","superstars",
            "power_score","preseason_scheduled","preseason_completed"
        ])
        with engine.begin() as conn:
            empty.to_sql("team_season_summary", conn, if_exists="replace", index=False,
                         dtype={"season": Integer(), "team": String(4)})
        return

    print(f"Loaded {len(games_all)} total games from database")

    # Get player stats for all teams
    player_stats = get_team_player_stats()
    
    # FIXED: Don't normalize team names - keep them as they are in the database
    # This was the main bug causing problems
    games_all["game_date"] = pd.to_datetime(games_all["game_date"], errors="coerce")
    games_all["season"] = games_all["game_date"].apply(_infer_season).astype("Int64")
    games_all["is_preseason"] = games_all["game_date"].apply(_is_preseason).astype(bool)

    # Coerce scores to numeric
    for c in ("home_score","away_score"):
        games_all[c] = pd.to_numeric(games_all[c], errors="coerce")

    # Completed games only
    complete_mask = games_all["home_score"].notna() & games_all["away_score"].notna()
    comp = games_all.loc[complete_mask].copy()
    
    print(f"Found {len(comp)} completed games")

    # Count preseason games
    pre_sched = (
        pd.concat([
            games_all.loc[games_all["is_preseason"], ["season","home_team"]].rename(columns={"home_team":"team"}),
            games_all.loc[games_all["is_preseason"], ["season","away_team"]].rename(columns={"away_team":"team"})
        ]).dropna().assign(preseason_scheduled=1)
        .groupby(["season","team"])["preseason_scheduled"].sum().reset_index()
    )
    
    # For 2025, use hardcoded preseason data if no completed games yet
    if 2025 in games_all["season"].values:
        pre_2025 = []
        for team, (w, l, t) in PRESEASON_2025.items():
            pre_2025.append({
                "season": 2025,
                "team": team,
                "preseason_scheduled": w + l + t,
                "preseason_completed": w + l + t
            })
        pre_2025_df = pd.DataFrame(pre_2025)
        
        # Merge with existing or replace
        pre_sched = pd.concat([
            pre_sched[pre_sched["season"] != 2025],
            pre_2025_df[["season", "team", "preseason_scheduled"]]
        ]).reset_index(drop=True)

    pre_comp = (
        pd.concat([
            comp.loc[comp["is_preseason"], ["season","home_team"]].rename(columns={"home_team":"team"}),
            comp.loc[comp["is_preseason"], ["season","away_team"]].rename(columns={"away_team":"team"})
        ]).dropna().assign(preseason_completed=1)
        .groupby(["season","team"])["preseason_completed"].sum().reset_index()
    )
    
    # Again for 2025 completed
    if 2025 in games_all["season"].values and not comp[comp["season"] == 2025].empty:
        # If we have actual completed preseason games, use those
        pass
    elif 2025 in games_all["season"].values:
        # Use hardcoded data
        pre_2025_comp = []
        for team, (w, l, t) in PRESEASON_2025.items():
            pre_2025_comp.append({
                "season": 2025,
                "team": team,
                "preseason_completed": w + l + t
            })
        pre_comp = pd.concat([
            pre_comp[pre_comp["season"] != 2025],
            pd.DataFrame(pre_2025_comp)
        ]).reset_index(drop=True)

    # Process regular season games
    if not comp.empty:
        # Filter to regular season only for stats calculation
        comp_regular = comp[~comp["is_preseason"]].copy()
        
        print(f"Found {len(comp_regular)} completed regular season games")
        
        if not comp_regular.empty:
            comp_regular["winner"] = np.where(
                comp_regular["home_score"] > comp_regular["away_score"], 
                comp_regular["home_team"], 
                comp_regular["away_team"]
            )
            comp_regular["weight"] = 1.0  # Regular season weight
            
            home_part = comp_regular.rename(columns={
                "home_team":"team","home_score":"points_for","away_score":"points_against"
            })[["season","team","points_for","points_against","winner","weight"]]
            
            away_part = comp_regular.rename(columns={
                "away_team":"team","away_score":"points_for","home_score":"points_against"
            })[["season","team","points_for","points_against","winner","weight"]]
            
            team_games = pd.concat([home_part, away_part], ignore_index=True)
        else:
            team_games = pd.DataFrame(columns=["season","team","points_for","points_against","winner","weight"])
    else:
        team_games = pd.DataFrame(columns=["season","team","points_for","points_against","winner","weight"])

    # Regular season aggregations
    if not team_games.empty:
        agg = team_games.groupby(["season","team"]).agg(
            games_played=("points_for","count"),
            total_points_for=("points_for","sum"),
            total_points_against=("points_against","sum"),
            avg_points_for=("points_for","mean"),
            avg_points_against=("points_against","mean"),
        ).reset_index()
        
        team_games["win_flag"] = (team_games["team"] == team_games["winner"]).astype(int)
        win_counts = team_games.groupby(["season","team"]).agg(
            wins=("win_flag","sum"),
        ).reset_index()
        
        summary = pd.merge(agg, win_counts, on=["season","team"], how="outer")
    else:
        summary = pd.DataFrame(columns=["season","team","games_played","avg_points_for","avg_points_against","wins"])

    # Seed rows for all teams with scheduled games
    sched_pairs = (
        pd.concat([
            games_all[["season","home_team"]].rename(columns={"home_team":"team"}),
            games_all[["season","away_team"]].rename(columns={"away_team":"team"}),
        ], ignore_index=True)
        .dropna(subset=["season","team"])
        .drop_duplicates()
    )
    summary = sched_pairs.merge(summary, on=["season","team"], how="left")

    # Fill missing values
    for c in ["games_played","total_points_for","total_points_against","avg_points_for","avg_points_against","wins"]:
        if c not in summary.columns:
            summary[c] = 0
        summary[c] = summary[c].fillna(0)

    summary["wins"] = summary["wins"].astype(int)
    summary["losses"] = (summary["games_played"] - summary["wins"]).astype(int)
    summary["win_pct"] = np.where(
        summary["games_played"] > 0,
        summary["wins"] / summary["games_played"], 
        0.0
    ).astype(float)
    summary["point_diff"] = (summary["avg_points_for"] - summary["avg_points_against"]).astype(float)

    # Merge preseason columns
    summary = summary.merge(pre_sched, on=["season","team"], how="left") \
                     .merge(pre_comp, on=["season","team"], how="left")
    summary["preseason_scheduled"] = summary["preseason_scheduled"].fillna(0).astype(int)
    summary["preseason_completed"] = summary["preseason_completed"].fillna(0).astype(int)

    # Merge player stats - handle case where merge fails
    if not player_stats.empty and "star_players" in player_stats.columns:
        summary = summary.merge(player_stats, on="team", how="left")
    else:
        # Add empty columns if merge failed
        summary["star_players"] = 0
        summary["superstars"] = 0

    # Ensure columns exist and are properly typed
    if "star_players" not in summary.columns:
        summary["star_players"] = 0
    if "superstars" not in summary.columns:
        summary["superstars"] = 0
        
    summary["star_players"] = summary["star_players"].fillna(0).astype(int)
    summary["superstars"] = summary["superstars"].fillna(0).astype(int)

    # Calculate power scores
    summary["power_score"] = 0.0
    
    # Process by season to handle different scenarios
    out_frames = []
    for season, sdf in summary.groupby("season", dropna=True):
        sdf = sdf.copy()
        active = sdf[sdf["games_played"] > 0].copy()
        
        if active.empty:
            # No regular season games yet
            print(f"Calculating initial power for season {season}...")
            
            # Get the final power scores from last season
            carryover_power = get_season_carryover_power(season)
            
            for idx, row in sdf.iterrows():
                team = row["team"]
                
                # 1. Start with 60% of last year's final power score
                last_season_component = carryover_power.get(team, 0.0) * 0.60
                
                # 2. Add 40% of the power from this year's preseason games
                preseason_component = calculate_initial_power_from_preseason(team, season) * 0.40
                
                # 3. Add a bonus for elite player talent
                talent_bonus = row["star_players"] * 0.2 + row["superstars"] * 1.0
                
                # Combine the components for the final score
                final_power = last_season_component + preseason_component + talent_bonus
                sdf.at[idx, "power_score"] = round(final_power, 1)

            out_frames.append(sdf)
            continue
            
        # Normal power calculation for seasons with games
        league_pf = active["avg_points_for"].mean()
        league_pa = active["avg_points_against"].mean()
        active["norm_point_diff"] = (active["avg_points_for"] - league_pf) - (active["avg_points_against"] - league_pa)
        active["win_component"] = (active["win_pct"] - 0.5) * 20
        max_abs = float(np.max(np.abs(active["norm_point_diff"]))) if not active.empty else 0.0
        active["diff_component"] = (active["norm_point_diff"] / max_abs) * 8 if max_abs > 0 else 0.0
        active["talent_component"] = active["star_players"] * 0.3 + active["superstars"] * 0.8
        active["raw_power"] = (
            active["win_component"] * 0.5 +
            active["diff_component"] * 0.4 +
            active["talent_component"] * 0.1
        )
        mu, sigma = active["raw_power"].mean(), active["raw_power"].std()
        active["power_score"] = ((active["raw_power"] - mu) / sigma) * 5 if sigma and sigma > 0 else active["talent_component"]
        active["power_score"] = active["power_score"].clip(-15, 15).round(1)
        
        # Merge back
        result = sdf.drop(columns=["power_score"]).merge(
            active[["team","power_score"]], on="team", how="left"
        )
        
        # Fill missing power scores for teams without games
        for idx, row in result.iterrows():
            if pd.isna(row["power_score"]):
                preseason_power = calculate_initial_power_from_preseason(row["team"], row["season"])
                talent_bonus = row["star_players"] * 0.3 + row["superstars"] * 0.8
                result.at[idx, "power_score"] = round(preseason_power + talent_bonus, 1)
        
        out_frames.append(result)

    if out_frames:
        final = pd.concat(out_frames, ignore_index=True)
    else:
        final = summary

    final = final[[
        "season","team","games_played","avg_points_for","avg_points_against",
        "wins","losses","win_pct","point_diff","star_players","superstars",
        "power_score","preseason_scheduled","preseason_completed"
    ]].sort_values(["season","team"]).reset_index(drop=True)

    # === Standardize team labels to abbreviations and drop duplicates ===
    ALIAS_TO_ABBR = {"LA":"LAR","STL":"LAR","SD":"LAC","OAK":"LV","JAC":"JAX","WSH":"WAS"}

    def _to_abbr(x: str) -> str:
        x = (x or "").strip()
        if x in TEAM_TO_ABBR:           # full -> abbr
            return TEAM_TO_ABBR[x]
        xu = x.upper()
        return ALIAS_TO_ABBR.get(xu, xu)  # abbr/alias -> canon abbr

    final["team"] = final["team"].map(_to_abbr)

    final = (
        final.sort_values(["season","games_played","preseason_completed","power_score"],
                        ascending=[True, False, False, False])
            .drop_duplicates(subset=["season","team"], keep="first")
            .reset_index(drop=True)
    )

    # === Re-add preseason counts for 2025 when DB has none (to keep "(PS)" badges) ===
    def _pre_ct(abbr: str) -> int:
        w, l, t = PRESEASON_2025.get(abbr, (0, 0, 0))
        return int(w + l + t)

    mask_2025 = final["season"].eq(2025)
    missing_pre = mask_2025 & (final["preseason_completed"].fillna(0) == 0)

    final.loc[missing_pre, "preseason_completed"] = final.loc[missing_pre, "team"].map(_pre_ct).fillna(0).astype(int)
    final.loc[missing_pre, "preseason_scheduled"] = final.loc[missing_pre, "team"].map(_pre_ct).fillna(0).astype(int)

    with engine.begin() as conn:
        # Play nicely with concurrent readers
        conn.exec_driver_sql("PRAGMA journal_mode=WAL;")
        conn.exec_driver_sql("PRAGMA synchronous=NORMAL;")
        conn.exec_driver_sql("PRAGMA busy_timeout=8000;")

        # Ensure the live table exists (NO DROP)
        conn.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS team_season_summary (
                season INTEGER,
                team TEXT,
                games_played INTEGER,
                avg_points_for REAL,
                avg_points_against REAL,
                wins INTEGER,
                losses INTEGER,
                win_pct REAL,
                point_diff REAL,
                star_players INTEGER,
                superstars INTEGER,
                power_score REAL,
                preseason_scheduled INTEGER,
                preseason_completed INTEGER
            );
        """)

        # Make (season, team) unique so REPLACE works deterministically
        conn.exec_driver_sql(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_tss_unique ON team_season_summary(season, team);"
        )

        # 1) Write to a staging table (safe to replace; readers don't use it)
        final.to_sql(
            "team_season_summary__staging",
            conn,
            if_exists="replace",   # only touches staging
            index=False,
            method="multi",
            chunksize=1000,
        )

        # 2) REPLACE rows from staging into live table (SQLite-compatible UPSERT)
        conn.exec_driver_sql("""
            REPLACE INTO team_season_summary (
                season, team, games_played, avg_points_for, avg_points_against,
                wins, losses, win_pct, point_diff,
                star_players, superstars, power_score,
                preseason_scheduled, preseason_completed
            )
            SELECT
                season, team, games_played, avg_points_for, avg_points_against,
                wins, losses, win_pct, point_diff,
                star_players, superstars, power_score,
                preseason_scheduled, preseason_completed
            FROM team_season_summary__staging;
        """)

        # 3) Best-effort cleanup of staging (optional)
        try:
            conn.exec_driver_sql("DROP TABLE team_season_summary__staging;")
        except Exception as e:
            print("⚠️ Staging drop skipped:", e)



    seasons_present = sorted([int(s) for s in final["season"].dropna().unique()])
    print(f"Team season summary written: {len(final)} rows, seasons: {seasons_present}")
    
    if 2025 in seasons_present:
        t25 = final[final["season"] == 2025]
        print("\n2025 Season Summary:")
        print(f"  Regular season games: {t25['games_played'].sum()}")
        print(f"  Preseason games: {t25['preseason_completed'].sum()}")
        print(f"  Total star players: {t25['star_players'].sum()}")
        print(f"  Total superstars: {t25['superstars'].sum()}")
        
        # Show teams with actual games played
        teams_with_games = t25[t25["games_played"] > 0]
        if not teams_with_games.empty:
            print(f"\nTeams with games played: {len(teams_with_games)}")
            print("Team records:")
            for _, team in teams_with_games.iterrows():
                print(f"  {team['team']}: {team['wins']}-{team['losses']} ({team['games_played']} games)")
        
        # Show power rankings
        top5 = t25.nlargest(5, "power_score")[["team", "power_score", "wins", "losses", "games_played"]]
        print(f"\nTop 5 Teams by Power:")
        for _, r in top5.iterrows():
            record = f"{r['wins']}-{r['losses']}" if r['games_played'] > 0 else "0-0"
            print(f"  {r['team']}: {r['power_score']:.1f} ({record})")

if __name__ == "__main__":
    calculate_team_season_summary()