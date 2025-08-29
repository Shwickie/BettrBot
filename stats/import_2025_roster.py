#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
2025 Roster & Injury Remap — full teams, fuzzy mapping, UNK-proof view.

Build "current roster" from (priority):
  A) nfl_data_py.import_players() with a real current team (e.g., latest_team/recent_team/current_team)
  B) Local DB fallbacks:
       - player_game_stats (most recent team per player)
       - player_team_map
       - player_stats_2024 / player_stats_2023
       - player_season_summary
  C) Active injuries (last resort so nobody stays UNK)

Then:
  - fuzzy map every ACTIVE injury to that roster team (and set player_id when possible)
  - recreate ai_team_injury_impact as a VIEW that uses roster team when injury team is UNK

Safe to re-run anytime.
"""

import re, difflib
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy import event
import random, time
from sqlalchemy.exc import OperationalError

def _sqlite_on_connect(dbapi_con, con_record):
    cur = dbapi_con.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA busy_timeout=60000;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.close()

DB = r"sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB, connect_args={'timeout': 60})
event.listen(engine, "connect", _sqlite_on_connect)
with engine.begin() as conn:
    conn.execute(text("PRAGMA journal_mode=WAL"))
    conn.execute(text("PRAGMA busy_timeout=60000"))
    conn.execute(text("PRAGMA synchronous=NORMAL"))


TEAM_MAP = {
    'ARI':'Arizona Cardinals','ATL':'Atlanta Falcons','BAL':'Baltimore Ravens','BUF':'Buffalo Bills',
    'CAR':'Carolina Panthers','CHI':'Chicago Bears','CIN':'Cincinnati Bengals','CLE':'Cleveland Browns',
    'DAL':'Dallas Cowboys','DEN':'Denver Broncos','DET':'Detroit Lions','GB':'Green Bay Packers',
    'HOU':'Houston Texans','IND':'Indianapolis Colts','JAX':'Jacksonville Jaguars','KC':'Kansas City Chiefs',
    'LV':'Las Vegas Raiders','LAC':'Los Angeles Chargers','LAR':'Los Angeles Rams',
    'MIA':'Miami Dolphins','MIN':'Minnesota Vikings','NE':'New England Patriots','NO':'New Orleans Saints',
    'NYG':'New York Giants','NYJ':'New York Jets','PHI':'Philadelphia Eagles','PIT':'Pittsburgh Steelers',
    'SF':'San Francisco 49ers','SEA':'Seattle Seahawks','TB':'Tampa Bay Buccaneers','TEN':'Tennessee Titans',
    'WAS':'Washington Commanders'  # <-- use WAS as the official key
}
FULL_TO_ABBR = {v:k for k,v in TEAM_MAP.items()}

def norm_team(x) -> str:
    if pd.isna(x) or not str(x).strip(): return 'UNK'
    tU = str(x).strip().upper()
    if tU == 'WSH': return 'WAS'  # <-- force alias
    if tU in TEAM_MAP: return tU
    if tU in (name.upper() for name in TEAM_MAP.values()):
        return FULL_TO_ABBR.get(str(x).strip().title(), 'UNK')
    alias = {
        'WASHINGTON':'WAS','WASHINGTON FOOTBALL TEAM':'WAS','WASHINGTON COMMANDERS':'WAS',
        'LA RAMS':'LAR','LOS ANGELES RAMS':'LAR',
        'LA CHARGERS':'LAC','LOS ANGELES CHARGERS':'LAC',
        'JAC':'JAX','JACKSONVILLE':'JAX','JACKSONVILLE JAGUARS':'JAX',
        'TAMPA BAY':'TB','TAMPA BAY BUCCANEERS':'TB',
        'NEW ENGLAND':'NE','NEW ORLEANS':'NO','SAN FRANCISCO':'SF','GREEN BAY':'GB',
        'SD':'LAC','STL':'LAR','OAK':'LV'
    }
    return alias.get(tU, 'UNK')

GOOD_TEAM_COLS = {'latest_team','recent_team','current_team','team','team_abbr','team_code','team_name'}
BAD_TEAM_COLS  = {'draft_team','college_team'}

POS_SET = {
    'QB','RB','WR','TE',
    'LT','RT','LG','RG','C','OL','T','G',
    'DE','DT','DL','EDGE','LB','OLB','ILB','MLB',
    'CB','S','FS','SS','DB',
    'K','P','LS','KR','PR','FB'
}

SUFFIX_RE = re.compile(r"\s+(Jr\.?|Sr\.?|III?|IV|V)$", re.IGNORECASE)
DIGITS_TAIL_RE = re.compile(r"\s*(?:#)?\d{1,3}$")



def write_roster_tables(roster: pd.DataFrame):
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS nfl_roster_2025"))
        roster.assign(updated_at=stamp).to_sql('nfl_roster_2025', conn, index=False)

        conn.execute(text("DROP TABLE IF EXISTS player_team_current"))
        # Preserve provenance so mapping can trust ESPN/PGS/import_players rows
        roster[['player_key','full_name','team_abbr','team_full','position','source']] \
            .rename(columns={'source':'team_source'}) \
            .assign(updated_at=stamp) \
            .to_sql('player_team_current', conn, index=False)
        
def build_roster_validation():
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS ai_roster_validation_detail"))
        conn.execute(text("""
          CREATE TABLE ai_roster_validation_detail AS
          WITH ptc AS (
            SELECT
              LOWER(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(full_name),'.',''),'''',''),'-',''),' ','')) AS pkey,
              full_name, team_abbr AS roster_team, team_source
            FROM player_team_current
          ),
          pgs AS (
            SELECT
              LOWER(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(player_name),'.',''),'''',''),'-',''),' ','')) AS pkey,
              UPPER(TRIM(team)) AS pgs_team,
              season, week,
              ROW_NUMBER() OVER (PARTITION BY
                LOWER(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(player_name),'.',''),'''',''),'-',''),' ','')) 
                ORDER BY CAST(season AS INT) DESC, CAST(week AS INT) DESC
              ) AS rn
            FROM player_game_stats
            WHERE CAST(season AS INT)=2025
          ),
          latest_pgs AS (SELECT pkey, pgs_team FROM pgs WHERE rn=1)
          SELECT
            p.full_name,
            p.roster_team,
            p.team_source,
            lp.pgs_team,
            CASE WHEN lp.pgs_team IS NOT NULL AND lp.pgs_team <> p.roster_team THEN 1 ELSE 0 END AS team_mismatch_2025
          FROM ptc p
          LEFT JOIN latest_pgs lp USING (pkey)
        """))

        conn.execute(text("DROP TABLE IF EXISTS ai_roster_validation_team_summary"))
        conn.execute(text("""
          CREATE TABLE ai_roster_validation_team_summary AS
          SELECT roster_team AS team,
                 SUM(team_mismatch_2025) AS mismatches_2025,
                 SUM(CASE WHEN team_source <> 'espn' THEN 1 ELSE 0 END) AS non_espn_rows,
                 COUNT(*) AS roster_rows
          FROM ai_roster_validation_detail
          GROUP BY roster_team
          ORDER BY mismatches_2025 DESC, non_espn_rows DESC
        """))


NON_PLAYER_PAT = r"\b(?:coach|assistant|trainer|co-?ordinator|coordinator|gm|owner|staff)\b"

def is_probably_not_player(name: str) -> bool:
    return bool(re.search(NON_PLAYER_PAT, name or "", re.I))

# and change drop_nonplayers() to:
def drop_nonplayers(df, col='full_name'):
    m = df[col].astype(str).str.contains(NON_PLAYER_PAT, case=False, regex=True, na=False)
    return df[~m]



def kcanon(name: str) -> str:
    if not name: return ""
    s = name.strip().lower()
    s = re.sub(r"[.\-'\s]+", "", s)          # remove punctuation/space only
    s = s.replace("’","'")                   # normalize curly apostrophes
    return s


# was: def clean_name(name: str) -> str:
def clean_name(name: str, strip_suffix: bool = False) -> str:
    if not isinstance(name, str): name = '' if pd.isna(name) else str(name)
    s = name.strip()
    if strip_suffix:
        s = SUFFIX_RE.sub('', s)  # only when you explicitly want to drop suffix
    s = s.replace("’","'").replace("`","'")
    s = re.sub(r"\bTj\b","T.J.", s, flags=re.IGNORECASE)
    s = re.sub(r"\bCj\b","C.J.", s, flags=re.IGNORECASE)
    s = re.sub(r"\bAj\b","A.J.", s, flags=re.IGNORECASE)
    s = re.sub(r"\bJj\b","J.J.", s, flags=re.IGNORECASE)
    s = DIGITS_TAIL_RE.sub('', s)
    s = ' '.join(s.split())
    return s.title()


def kcanon_series(s: pd.Series) -> pd.Series:
    s = s.fillna('').astype(str).str.strip().str.lower()
    s = s.str.replace(r"[.\-'\s]+", "", regex=True)
    return s

def kcanon_loose(name: str) -> str:
    s = kcanon(name)
    return re.sub(r"(jr|sr|iii|ii|iv|v)$", "", s)

def kcanon_series_loose(s: pd.Series) -> pd.Series:
    s = kcanon_series(s)
    return s.str.replace(r"(jr|sr|iii|ii|iv|v)$", "", regex=True)

def build_injury_validation_tables():
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS ai_injury_validation_detail"))
        conn.execute(text("""
            CREATE TABLE ai_injury_validation_detail AS
            WITH
            inj AS (
              SELECT
                id,
                LOWER(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(player_name),'.',''),'''',''),'-',''),' ','')) AS pkey,
                UPPER(TRIM(team)) AS team_inj,
                TRIM(player_name) AS inj_name,
                TRIM(designation) AS designation
              FROM nfl_injuries_tracking
              WHERE is_active = 1
            ),
            ros AS (
              SELECT
                LOWER(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(full_name),'.',''),'''',''),'-',''),' ','')) AS pkey,
                team_abbr AS team_ai,
                full_name  AS roster_name,
                position   AS position
              FROM player_team_current
            )
            SELECT
              ros.team_ai,
              inj.team_inj,
              CASE WHEN inj.team_inj IS NULL OR inj.team_inj='' THEN 1 ELSE 0 END AS inj_missing_team,
              CASE WHEN ros.team_ai IS NULL OR ros.team_ai='' THEN 1 ELSE 0 END AS roster_missing_team,
              (inj.team_inj IS NOT NULL AND ros.team_ai IS NOT NULL AND inj.team_inj <> ros.team_ai) AS team_mismatch,
              inj.designation,
              ros.position,
              inj.inj_name,
              ros.roster_name,
              inj.id AS injury_row_id
            FROM inj
            LEFT JOIN ros USING (pkey)
        """))

        conn.execute(text("DROP TABLE IF EXISTS ai_injury_validation_team_summary"))
        conn.execute(text("""
            CREATE TABLE ai_injury_validation_team_summary AS
            SELECT
              COALESCE(team_ai, team_inj, 'UNK') AS team,
              SUM(CASE WHEN designation IN ('IR','Injured Reserve','PUP','Out','DNP') THEN 1 ELSE 0 END) AS out_like,
              SUM(CASE WHEN designation = 'Doubtful' THEN 1 ELSE 0 END) AS doubtful,
              SUM(CASE WHEN designation = 'Questionable' THEN 1 ELSE 0 END) AS questionable,
              SUM(team_mismatch) AS team_mismatch_cnt,
              SUM(inj_missing_team) AS inj_missing_team_cnt,
              SUM(roster_missing_team) AS roster_missing_team_cnt,
              COUNT(*) AS total_rows
            FROM ai_injury_validation_detail
            GROUP BY COALESCE(team_ai, team_inj, 'UNK')
            ORDER BY team
        """))
    print("🧪 Built ai_injury_validation_detail and ai_injury_validation_team_summary.")


def present(df: pd.DataFrame, *candidates):
    """Return only the columns that exist in df."""
    return [c for c in candidates if c and c in df.columns]


def resolve_team_conflicts(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d = d[d['team_abbr'] != 'UNK']

    # ESPN pick (if available)
    espn = (d[d['source'].str.lower().eq('espn')]
              .groupby('player_key', as_index=False)['team_abbr']
              .agg(lambda s: s.mode().iat[0] if not s.mode().empty else s.iloc[0])
              .rename(columns={'team_abbr':'espn_team'}))

    # Weighted fallback
    wt = {'espn':4,'player_game_stats':3,'import_players':3,'player_team_map':2,
          'player_stats_2024':1,'player_season_summary':1,'player_stats_2023':1,'injuries_last_resort':1}
    d['__w'] = d['source'].map(wt).fillna(1).astype(int)
    pick = (d.groupby(['player_key','team_abbr'], as_index=False)['__w'].sum()
              .sort_values(['player_key','__w'], ascending=[True,False])
              .drop_duplicates('player_key', keep='first')
              .rename(columns={'team_abbr':'vote_team'}))

    out = df.merge(espn, on='player_key', how='left').merge(pick[['player_key','vote_team']], on='player_key', how='left')
    out['team_abbr'] = out['espn_team'].fillna(out['vote_team']).fillna(out['team_abbr'])
    out['team_full'] = out['team_abbr'].map(lambda a: TEAM_MAP.get(a, a))
    return out.drop(columns=['espn_team','vote_team','__w'], errors='ignore')



def choose(df: pd.DataFrame, preferred: list[str], contains: list[str] = None, exclude: set[str] = None):
    cols = list(df.columns)
    excl = exclude or set()
    for c in preferred:
        if c in df.columns and c not in excl:
            return c
    if contains:
        cands = [c for c in cols if any(tok in c for tok in contains) and c not in excl]
        if cands:
            cands.sort(key=len)
            return cands[0]
    return None

def name_variants(name: str):
    if not name: return []
    base = name.lower()
    v = {base, base.replace(".",""), base.replace("'",""), base.replace("-"," "), base.replace("-",""), base.replace(" ","")}
    parts = base.split()
    if len(parts) >= 2:
        f, l = parts[0], parts[-1]
        if f:
            v.add(f"{f[0]}. {l}")
            v.add(f"{f[0]} {l}")
            v.add(f"{f}{l}")
    return list(v)

def best_fuzzy(target: str, keys: set[str], cutoff=0.88):
    if not target or not keys: return None, 0.0
    t = target.lower()
    if t in keys: return t, 0.99
    for v in name_variants(target):
        if v in keys: return v, 0.95
    cand = difflib.get_close_matches(t, list(keys), n=1, cutoff=cutoff)
    if cand:
        from difflib import SequenceMatcher
        return cand[0], SequenceMatcher(None, t, cand[0]).ratio()
    return None, 0.0

def table_exists(conn, name: str) -> bool:
    q = pd.read_sql(text("SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name=:n"), conn, params={'n':name})
    return not q.empty

def take_latest_team(df: pd.DataFrame, date_cols=('game_date','date'), season_cols=('season',), week_cols=('week',)) -> pd.DataFrame:
    if df.empty: return df
    df = df.copy()
    score = pd.Series(0, index=df.index, dtype='int64')
    # season dominates
    for c in season_cols:
        if c in df.columns:
            s = pd.to_numeric(df[c], errors='coerce').fillna(0).astype('int64')
            score = score + (s * 10_000_000)
    # week next
    for c in week_cols:
        if c in df.columns:
            w = pd.to_numeric(df[c], errors='coerce').fillna(0).astype('int64')
            score = score + (w * 100_000)
    # date for tie-breaker
    for c in date_cols:
        if c in df.columns:
            dt = pd.to_datetime(df[c], errors='coerce')
            score = score + dt.view('int64').fillna(0)
    df['__score'] = score
    df = df.sort_values(['player_key','__score']).drop_duplicates('player_key', keep='last').drop(columns=['__score'])
    return df



# ---------- Source A: nfl_data_py.import_players() with a real current team ----------
def fetch_players_current() -> pd.DataFrame:
    try:
        import nfl_data_py as nfl
    except Exception as e:
        print(f"❌ nfl_data_py import failed: {e}")
        return pd.DataFrame()

    df = nfl.import_players()
    if df is None or df.empty:
        print("⚠️ import_players() returned empty.")
        return pd.DataFrame()

    df = df.copy()
    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]

    name_col = choose(df, ['full_name','player_display_name','display_name','player_name','name'], ['name'])
    team_col = choose(df, list(GOOD_TEAM_COLS), ['team'], exclude=BAD_TEAM_COLS)
    pos_col  = choose(df, ['position','pos','gsis_position','pfr_position','position_group'], ['pos','position'])
    status_col = choose(df, ['status','current_status','player_status','roster_status'], ['status'])
    pid_col  = choose(df, ['player_id','gsis_id','pfr_id','esb_id','nfl_id'], ['id'])

    print("import_players() column picks:")
    print(f"  name_col   = {name_col}")
    print(f"  team_col   = {team_col}  (ignored if None)")
    print(f"  pos_col    = {pos_col}")
    print(f"  status_col = {status_col}")
    print(f"  pid_col    = {pid_col}")

    if not name_col:
        return pd.DataFrame()

    cols = [c for c in [name_col, team_col, pos_col, status_col, pid_col] if c]
    out = df[cols].copy()

    rename = {name_col:'full_name'}
    if team_col: rename[team_col] = 'team_raw'
    if pos_col:  rename[pos_col] = 'position'
    if status_col: rename[status_col] = 'status'
    if pid_col:  rename[pid_col] = 'player_id'
    out.rename(columns=rename, inplace=True)

    out['player_key'] = kcanon_series(out['full_name'])
    out['team_abbr'] = out['team_raw'].map(norm_team) if 'team_raw' in out.columns else 'UNK'
    out['team_full'] = out['team_abbr'].map(lambda a: TEAM_MAP.get(a, a))
    out['position'] = out.get('position','')
    out['position'] = out['position'].astype(str).str.upper().str.strip()

   # normalize + keep only true roster players
    if 'status' in out.columns:
        out['status'] = out['status'].astype(str).str.title()
    else:
        out['status'] = ''  # make sure column exists

    # <-- IMPORTANT: Active only (no Practice Squad / IR / PUP / NFI)
    out = out[(out['status'] == 'Active') | out['status'].isna()]

    # ...
    out['source'] = 'import_players'
    return out[['player_key','full_name','player_id','team_abbr','team_full','position','status','source']]


def enforce_53_cap(combined: pd.DataFrame, per_team: int = 53) -> pd.DataFrame:
    combined = combined.copy()
    if 'status' not in combined.columns:
        combined['status'] = ''

    # current injuries -> always keep
    with engine.begin() as conn:
        inj = pd.read_sql(text("""
            SELECT DISTINCT player_name
            FROM nfl_injuries_tracking WHERE is_active=1
        """), conn)
    inj_keys = set(kcanon_series(inj['player_name']).tolist()) if not inj.empty else set()
    pkeys = kcanon_series(combined['full_name'])


    # players who actually appeared in 2025
    seen_2025 = set()
    with engine.begin() as conn:
        if table_exists(conn, 'player_game_stats'):
            pgs = pd.read_sql(text("SELECT player_name FROM player_game_stats WHERE CAST(season AS INTEGER)=2025"), conn)
            if not pgs.empty:
                seen_2025 = set(kcanon_series(pgs['player_name']).tolist())


    # keep rule: must be 2025-active OR currently injured
    # keep rule: ESPN 53 *or* Active status *or* 2025 snaps *or* currently injured
    is_espn   = combined['source'].astype(str).str.lower().eq('espn')
    is_active = combined['status'].astype(str).str.upper().eq('ACTIVE')

    keep = is_espn | is_active | pkeys.isin(seen_2025) | pkeys.isin(inj_keys)

    combined = combined[keep].copy()
    pkeys = kcanon_series(combined['full_name'])  # re-align keys to filtered frame


    # drop IR/PUP unless they’re in current injuries (prevents old IRs bloating the roster)
    status_col = combined['status'].astype(str).str.upper()
    on_reserve = status_col.isin({'INJURED RESERVE','IR','PUP','RESERVE'})
    combined = combined.loc[(~on_reserve) | (pkeys.isin(inj_keys))]

    # ranking: injured first, 2025 activity, source priority, then QB
    src_pri = {
        'player_game_stats': 4,
        'import_players':   3,
        'player_team_map':  2,
        'player_stats_2024':1, 'player_season_summary':1, 'player_stats_2023':1,
        'espn':             5,   # when ESPN succeeded, its rows tend to be best
        'injuries_last_resort': 6  # only to preserve injured
    }
    combined['src_pri']   = combined['source'].map(src_pri).fillna(0).astype(int)
    combined['is_injured']= pkeys.isin(inj_keys).astype(int)
    combined['seen_2025'] = pkeys.isin(seen_2025).astype(int)
    combined['is_qb']     = (combined['position']=='QB').astype(int)

    combined['rank'] = (
        combined['is_injured']*1_000_000 +
        combined['seen_2025']*10_000 +
        combined['src_pri']*100 +
        combined['is_qb']
    )

    combined = combined.sort_values(['team_abbr','rank','full_name'], ascending=[True,False,True])
    return combined.groupby('team_abbr', as_index=False).head(per_team).reset_index(drop=True)

def _injury_select_sql(conn) -> str:
    cols = set(pd.read_sql(text("PRAGMA table_info(nfl_injuries_tracking)"), conn)['name'].tolist())
    ts = next((c for c in ['last_seen','last_updated','report_date','injury_date','scraped_at','date'] if c in cols), None)
    ts_select = f", {ts} AS last_ts" if ts else ""
    # add name guard in SQL
    return f"""
        SELECT player_name, designation, team{ts_select}
        FROM nfl_injuries_tracking
        WHERE is_active=1
          AND player_name NOT REGEXP '(?i)coach|assistant|trainer|co-?ordinator|coordinator|gm|owner|staff'
          AND UPPER(team)=:t
        ORDER BY designation DESC, player_name
    """



def print_team_verification(team: str):
    team = (team or '').strip().upper()
    if not team:
        print("No team provided.")
        return
    with engine.begin() as conn:
        sql = _injury_select_sql(conn)
        inj = pd.read_sql(text(sql), conn, params={'t': team})

        ros = pd.read_sql(
            text("""
                SELECT full_name, position
                FROM player_team_current
                WHERE team_abbr=:t
                ORDER BY full_name
            """),
            conn, params={'t': team}
        )

    inj['pkey'] = kcanon_series(inj['player_name'])
    ros['pkey'] = kcanon_series(ros['full_name'])
    merged = inj.merge(ros[['pkey','full_name','position']], on='pkey', how='left')

    print(f"\n=== {team} verification ===")
    print(f"Roster size: {len(ros)}   Active injuries: {len(inj)}")

    if not merged.empty:
        print("\nActive injuries → roster matches (first 50):")
        # collapse "Jaylen/Jayden Reed"-style near-dupes by pkey
        merged = merged.sort_values('designation')  # so Out/IR wins
        merged = merged.drop_duplicates('pkey', keep='first')

        print(merged[['player_name','designation','full_name','position']].head(50).to_string(index=False))

    miss = merged[merged['full_name'].isna()]
    if not miss.empty:
        print("\nInjuries NOT found on roster (first 50):")
        print(miss[['player_name','designation']].head(50).to_string(index=False))

    extras = ros[~ros['pkey'].isin(merged['pkey'])]
    if not extras.empty:
        print("\nRoster players without an active injury (first 50):")
        print(extras[['full_name','position']].head(50).to_string(index=False))


def prompt_and_verify_team():
    try:
        ans = input("\nVerify a team now? (y/n): ").strip().lower()
    except Exception:
        ans = 'n'
    while ans.startswith('y'):
        try:
            team = input("Enter team abbr (e.g., ARI, LAR, CLE): ").strip().upper()
        except Exception:
            team = ''
        print_team_verification(team)
        ans = input("\nVerify a team now? (y/n): ").strip().lower()


# ---------- Source B/C: Local DB + Injuries ----------
def pull_local_roster_fallbacks() -> pd.DataFrame:
    """Harvest player->team from your DB tables, pick most recent per player."""
    out = []
    with engine.begin() as conn:
        # ---------- 1) player_game_stats ----------
        if table_exists(conn, 'player_game_stats'):
            pgs = pd.read_sql(text("SELECT * FROM player_game_stats"), conn)
            if not pgs.empty:
                pgs.columns = [c.lower() for c in pgs.columns]
                # keep only recent seasons to infer CURRENT team
                if 'season' in pgs.columns:
                    pgs = pgs[pgs['season'].astype(str).astype(int) >= 2024]

                name_col = next((c for c in ['player_name','player','player_display_name','full_name','name'] if c in pgs.columns), None)
                team_col = next((c for c in ['recent_team','team','team_abbr','team_name'] if c in pgs.columns), None)
                pos_col  = next((c for c in ['position','pos','p'] if c in pgs.columns), None)
                cols = present(pgs, name_col, team_col, pos_col,
                               'game_date','date','gamedate','game_datetime',
                               'season','week')
                d = pgs[cols].copy() if (name_col and team_col) and cols else pd.DataFrame()
                if not d.empty:
                    d.rename(columns={name_col:'full_name', team_col:'team_raw', **({pos_col:'position'} if pos_col else {})}, inplace=True)
                    d['full_name'] = d['full_name'].map(clean_name)
                    d['player_key'] = kcanon_series(d['full_name'])
                    d['team_abbr'] = d['team_raw'].map(norm_team)
                    d['team_full'] = d['team_abbr'].map(lambda a: TEAM_MAP.get(a, a))
                    d['position']  = d.get('position','')
                    d['position']  = d['position'].astype(str).str.upper().str.strip()
                    d = d[(d['team_abbr']!='UNK') & d['team_abbr'].notna()]
                    d = take_latest_team(d, date_cols=('game_date','date','gamedate','game_datetime'))
                    d['source'] = 'player_game_stats'
                    d['status'] = ''  # unknown/neutral
                    out.append(d[['player_key','full_name','team_abbr','team_full','position','status','source']])


        # ---------- 2) player_team_map ----------
        if table_exists(conn, 'player_team_map'):
            ptm = pd.read_sql(text("SELECT * FROM player_team_map"), conn)
            if not ptm.empty:
                ptm.columns = [c.lower() for c in ptm.columns]
                if 'season' in ptm.columns:
                    ptm = ptm[ptm['season'].astype(str).astype(int) >= 2024]

                name_col = next((c for c in ['full_name','player_name','player','name'] if c in ptm.columns), None)
                team_col = next((c for c in ['recent_team','team','team_abbr','team_name'] if c in ptm.columns), None)  # <-- use ptm.columns

                cols = present(ptm, name_col, team_col, 'season','last_seen','updated_at')
                d = ptm[cols].copy() if (name_col and team_col and cols) else pd.DataFrame()
                if not d.empty:
                    rename = {name_col: 'full_name'}
                    if team_col in d.columns:
                        rename[team_col] = 'team_raw'
                    d.rename(columns=rename, inplace=True)

                    if 'team_raw' not in d.columns:
                        # no usable team column after all — skip
                        pass
                    else:
                        d['full_name'] = d['full_name'].map(clean_name)
                        d['player_key'] = kcanon_series(d['full_name'])
                        d['team_abbr'] = d['team_raw'].map(norm_team)
                        d['team_full'] = d['team_abbr'].map(lambda a: TEAM_MAP.get(a, a))
                        d['position']  = ''
                        d = d[(d['team_abbr']!='UNK') & d['team_abbr'].notna()]
                        d = take_latest_team(d)
                        d['source'] = 'player_team_map'
                        d['status'] = ''
                        out.append(d[['player_key','full_name','team_abbr','team_full','position','status','source']])

        # ---------- 3) player_stats_2024 / 2023 / player_season_summary ----------
        for tbl in ['player_stats_2024','player_stats_2023','player_season_summary']:
            if table_exists(conn, tbl):
                ps = pd.read_sql(text(f"SELECT * FROM {tbl}"), conn)
                if not ps.empty:
                    ps.columns = [c.lower() for c in ps.columns]
                    if 'season' in ps.columns:
                        ps = ps[ps['season'].astype(str).astype(int) >= 2024]
                    name_col = next((c for c in ['player','player_name','full_name','name'] if c in ps.columns), None)
                    team_col = next((c for c in ['recent_team','team','team_abbr','team_name'] if c in ps.columns), None)
                    pos_col  = next((c for c in ['position','pos'] if c in ps.columns), None)
                    cols = present(ps, name_col, team_col, pos_col, 'season','week')
                    d = ps[cols].copy() if (name_col and team_col) and cols else pd.DataFrame()
                    if not d.empty:
                        d.rename(columns={name_col:'full_name', team_col:'team_raw', **({pos_col:'position'} if pos_col else {})}, inplace=True)
                        d['full_name'] = d['full_name'].map(clean_name)
                        d['player_key'] = kcanon_series(d['full_name'])
                        d['team_abbr'] = d['team_raw'].map(norm_team)
                        d['team_full'] = d['team_abbr'].map(lambda a: TEAM_MAP.get(a, a))
                        d['position']  = d.get('position','')
                        d['position']  = d['position'].astype(str).str.upper().str.strip()
                        d = d[(d['team_abbr']!='UNK') & d['team_abbr'].notna()]
                        d = take_latest_team(d)
                        d['source'] = tbl
                        d['status'] = ''
                        out.append(d[['player_key','full_name','team_abbr','team_full','position','status','source']])

        # ---------- 4) Active injuries (last resort) ----------
        if table_exists(conn, 'nfl_injuries_tracking'):
            inj = pd.read_sql(text("SELECT player_name, team FROM nfl_injuries_tracking WHERE is_active=1"), conn)
            if not inj.empty:
                inj.columns = [c.lower() for c in inj.columns]
                cols = present(inj, 'player_name','team')
                d = inj[cols].copy() if {'player_name','team'}.issubset(set(cols)) else pd.DataFrame()
                if not d.empty:
                    d.rename(columns={'player_name':'full_name'}, inplace=True)
                    d['full_name'] = d['full_name'].map(clean_name)
                    d['player_key'] = kcanon_series(d['full_name'])
                    d['team_abbr'] = d['team'].map(norm_team)
                    d['team_full'] = d['team_abbr'].map(lambda a: TEAM_MAP.get(a, a))
                    d['position'] = ''
                    d = d[(d['team_abbr']!='UNK') & d['team_abbr'].notna()]
                    d['source'] = 'injuries_last_resort'
                    d['status'] = ''
                    out.append(d[['player_key','full_name','team_abbr','team_full','position','status','source']])

    if not out:
        return pd.DataFrame()

    merged = pd.concat(out, ignore_index=True)
    merged = merged.dropna(subset=['full_name'])
    merged = merged[merged['full_name'].str.len() > 0]

    # compress across sources by priority (A wins later when we union)
    priority = {'import_players':1,'player_game_stats':2,'player_team_map':3,
                'player_stats_2024':4,'player_stats_2023':5,'player_season_summary':6,
                'injuries_last_resort':9}
    merged['__pri'] = merged['source'].map(lambda s: priority.get(s, 8))
    merged = merged.sort_values(['player_key','__pri']).drop_duplicates('player_key', keep='first').drop(columns=['__pri'])
    return merged[['player_key','full_name','team_abbr','team_full','position','source']]

def _active_2025_keys() -> set:
    """Players who have appeared on a 2025 game stat row (any snaps)"""
    with engine.begin() as conn:
        if not table_exists(conn, "player_game_stats"):
            return set()
        pgs = pd.read_sql(text("""
            SELECT player_name, season, week
            FROM player_game_stats
            WHERE CAST(season AS INTEGER) = 2025
        """), conn)
    if pgs.empty:
        return set()
    pgs.columns = [c.lower() for c in pgs.columns]
    keys = pgs['player_name'].fillna('').astype(str).str.strip().str.lower()
    keys = keys.str.replace(r"[.\-'\s]+", "", regex=True)
    return set(keys.tolist())


def filter_and_cap_to_53(combined: pd.DataFrame, limit_per_team: int = 53) -> pd.DataFrame:
    """
    Keep only: (a) players seen in 2025 games OR (b) currently injured.
    Then rank within each team and cap to 53 (injured are guaranteed to stay).
    """
    combined = combined.copy()

    # who is injured now?
    with engine.begin() as conn:
        inj = pd.read_sql(text("""
            SELECT DISTINCT LOWER(REPLACE(REPLACE(REPLACE(TRIM(player_name),'.',''),'''',''),'-','')) AS pkey
            FROM nfl_injuries_tracking
            WHERE is_active = 1
        """), conn)
    inj_keys = set(inj['pkey'].tolist()) if not inj.empty else set()

    # who actually appeared in 2025?
    seen_2025 = _active_2025_keys()

    # keep only 2025-seen OR currently injured
    pkeys = combined['player_key'].str.replace(r"[.\-'\s]+", "", regex=True)
    keep_mask = pkeys.isin(seen_2025) | pkeys.isin(inj_keys)
    combined = combined[keep_mask].copy()

    # source priority (prefer true activity > metadata)
    src_pri = {
        'player_game_stats': 5,
        'import_players': 4,
        'player_team_map': 3,
        'player_stats_2024': 2,
        'player_season_summary': 1,
        'player_stats_2023': 1,
        'injuries_last_resort': 6  # guarantees we don't cut injured players
    }
    combined['src_pri'] = combined['source'].map(src_pri).fillna(1).astype(int)

    # recency score from 2025 season (optional but helpful)
    activity_map = {}
    with engine.begin() as conn:
        if table_exists(conn, 'player_game_stats'):
            pgs = pd.read_sql(text("""
                SELECT player_name, season, week
                FROM player_game_stats
                WHERE CAST(season AS INTEGER) = 2025
            """), conn)
            if not pgs.empty:
                pgs.columns = [c.lower() for c in pgs.columns]
                pgs['player_key'] = pgs['player_name'].fillna('').astype(str).str.strip().str.lower()
                pgs['score'] = pgs['season'].astype('Int64').fillna(0)*100 + pgs['week'].astype('Int64').fillna(0)
                pgs = pgs.sort_values('score').drop_duplicates('player_key', keep='last')
                activity_map = dict(zip(pgs['player_key'], pgs['score']))
    combined['activity'] = combined['player_key'].map(activity_map).fillna(0).astype(int)

    # flags
    combined['is_injured'] = pkeys.isin(inj_keys).astype(int)
    combined['is_qb'] = (combined['position'] == 'QB').astype(int)

    # rank within team: injured first, 2025 activity next, then source, then QB
    combined['rank'] = (
        combined['is_injured']*1_000_000 +
        combined['activity']*10_000 +
        combined['src_pri']*100 +
        combined['is_qb']
    )

    combined = combined.sort_values(['team_abbr','rank','full_name'], ascending=[True, False, True])
    return combined.groupby('team_abbr', as_index=False).head(limit_per_team).reset_index(drop=True)


def map_injuries_to_current():
    print("🔗 Mapping active injuries → player_team_current (fuzzy, guarded)…")

    # Pull injuries and roster (with a safe team_source alias)
    with engine.begin() as conn:
        inj = pd.read_sql(text("""
            SELECT id, player_name, team, designation, player_id, is_active
            FROM nfl_injuries_tracking
            WHERE is_active = 1
        """), conn)

        cols = set(pd.read_sql(text("PRAGMA table_info(player_team_current)"), conn)['name'].tolist())
        sel_src = "team_source" if "team_source" in cols else "'unknown' AS team_source"
        ros = pd.read_sql(text(f"""
            SELECT player_key, full_name, team_abbr, position, {sel_src}
            FROM player_team_current
        """), conn)

    if inj.empty:
        print("⚠️ No active injuries found.")
        return 0, 0, 0

    # Build canonical roster map
    ros['pkey'] = ros['full_name'].map(kcanon)
    ros_map = dict(zip(ros['pkey'], zip(ros['team_abbr'], ros['team_source'])))
    ros_keys = set(ros['pkey'])

    fixed_team = fixed_pid = 0
    log_rows = []

    for _, r in inj.iterrows():
        raw_name = r['player_name'] or ''
        if is_probably_not_player(raw_name):
            continue  # ignore coaches/staff/etc.

        pk = kcanon(raw_name)
        if pk not in ros_keys and str(r['team']).strip().upper() in {'', 'UNK', 'UNKNOWN'}:
            target = kcanon(raw_name)
            # small candidate pool: same-position players improves precision
            same_pos = ros[ros['position'].astype(str).str.upper().eq(str((r.get('position') or '')).upper())] \
                    if 'position' in inj.columns else ros
            cand_keys = set(same_pos['pkey'])
            fuzzy_key, score = best_fuzzy(raw_name, cand_keys, cutoff=0.93)
            if fuzzy_key:
                want_team, team_src = ros_map[fuzzy_key]
                if str(team_src).lower() in {'espn','player_game_stats'} and want_team not in {'','UNK','UNKNOWN'}:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            UPDATE nfl_injuries_tracking
                            SET team=:tm, confidence_score=:conf, last_updated=:ts
                            WHERE id=:iid
                        """), dict(tm=want_team, conf=score, ts=datetime.now(), iid=int(r['id'])))
                    fixed_team += 1
        if pk not in ros_keys:
            continue  # no exact canonical match → don't override team

        want_team, team_src = ros_map[pk]
        curr_team = (r['team'] or 'UNKNOWN').strip().upper()

        reliable_src = str(team_src).lower() in {'espn','player_game_stats','import_players'}
        needs_change = (curr_team in {'', 'UNK', 'UNKNOWN'}) or (want_team != curr_team)

        if reliable_src and needs_change and want_team not in {'', 'UNK', 'UNKNOWN'}:
            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE nfl_injuries_tracking
                       SET team = :tm,
                           confidence_score = :conf,
                           last_updated = :ts
                     WHERE id = :iid
                """), dict(tm=want_team, conf=0.99, ts=datetime.now(), iid=int(r['id'])))
            fixed_team += 1
            log_rows.append((raw_name, curr_team or 'UNKNOWN', want_team, r['designation']))

    if log_rows:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS injury_team_mismatches(
                    player_name TEXT, old_team TEXT, new_team TEXT, designation TEXT, logged_at TEXT
                )
            """))
            df_log = pd.DataFrame(log_rows, columns=['player_name','old_team','new_team','designation'])
            df_log['logged_at'] = datetime.now().strftime("%Y-%m-%d %H:%M")
            df_log.to_sql('injury_team_mismatches', conn, if_exists='append', index=False)

    print(f"✅ Injury mapping done. team corrected: {fixed_team}")
    return fixed_team, fixed_pid, len(log_rows)

def rebuild_team_injury_impact():
    print("🏗️ Rebuilding ai_team_injury_impact (VIEW)…")
    with engine.begin() as conn:
        for ddl in ["DROP VIEW IF EXISTS ai_team_injury_impact", "DROP TABLE IF EXISTS ai_team_injury_impact"]:
            try: conn.execute(text(ddl))
            except: pass

        # canonical key builder (SQLite: nested REPLACE)
        # pkey = lower(trim(name)) with . ' - spaces and jr/sr/ii/iii/iv/v removed
        def pkey(expr):
            return ("LOWER(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(" + expr +
                    "),'.',''),'''',''),'-',''),' ',''))")



        conn.execute(text(f"""
            CREATE VIEW ai_team_injury_impact AS
            WITH joined AS (
                SELECT
                  {pkey("i.player_name")} AS pkey,
                  ptc.team_abbr AS team,
                  COALESCE(ptc.position,'') AS position,
                  i.designation
                FROM nfl_injuries_tracking i
                JOIN (
                    SELECT 
                      {pkey("full_name")} AS pkey,
                      team_abbr,
                      position
                    FROM player_team_current
                ) AS ptc
                ON {pkey("i.player_name")} = ptc.pkey
                WHERE i.is_active = 1
            ),
            per_player AS (
                SELECT
                  team, position, pkey,
                  MAX(CASE
                        WHEN designation IN ('IR','Injured Reserve','PUP','Out','DNP') THEN 3
                        WHEN designation = 'Doubtful' THEN 2
                        WHEN designation = 'Questionable' THEN 1
                        ELSE 0
                      END) AS severity
                FROM joined
                GROUP BY team, position, pkey
            )
            SELECT
              team,
              SUM(CASE WHEN severity >= 3 THEN 1 ELSE 0 END) AS players_out,
              SUM(CASE WHEN severity = 2 THEN 1 ELSE 0 END)   AS players_doubtful,
              SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END)   AS players_questionable,
              SUM(CASE WHEN position='QB' AND severity >= 1 THEN 1 ELSE 0 END) AS qb_risk,
              SUM(CASE WHEN position IN ('WR','RB','TE','LT','RT','C','LG','RG') AND severity >= 1 THEN 1 ELSE 0 END) AS skill_position_risk,
              COUNT(*) AS total_injuries
            FROM per_player
            GROUP BY team
            ORDER BY team;
        """))
    print("💾 ai_team_injury_impact VIEW ready.")

# --- ESPN roster helpers ---
ESPN_TEAM_IDS = {
    'ARI':22,'ATL':1,'BAL':33,'BUF':2,'CAR':29,'CHI':3,'CIN':4,'CLE':5,'DAL':6,'DEN':7,'DET':8,
    'GB':9,'HOU':34,'IND':11,'JAX':30,'KC':12,'LV':13,'LAC':24,'LAR':14,'MIA':15,'MIN':16,'NE':17,
    'NO':18,'NYG':19,'NYJ':20,'PHI':21,'PIT':23,'SF':25,'SEA':26,'TB':27,'TEN':10,'WAS':28
}
ESPN_TEAM_SLUGS = {
    'ARI':'ari/arizona-cardinals','ATL': 'atl/atlanta-falcons','BAL':'bal/baltimore-ravens','BUF':'buf/buffalo-bills',
    'CAR':'car/carolina-panthers','CHI':'chi/chicago-bears','CIN':'cin/cincinnati-bengals','CLE':'cle/cleveland-browns',
    'DAL':'dal/dallas-cowboys','DEN':'den/denver-broncos','DET':'det/detroit-lions','GB':'gb/green-bay-packers',
    'HOU':'hou/houston-texans','IND':'ind/indianapolis-colts','JAX':'jax/jacksonville-jaguars','KC':'kc/kansas-city-chiefs',
    'LV':'lv/las-vegas-raiders','LAC':'lac/los-angeles-chargers','LAR':'lar/los-angeles-rams','MIA':'mia/miami-dolphins',
    'MIN':'min/minnesota-vikings','NE':'ne/new-england-patriots','NO':'no/new-orleans-saints','NYG':'nyg/new-york-giants',
    'NYJ':'nyj/new-york-jets','PHI':'phi/philadelphia-eagles','PIT':'pit/pittsburgh-steelers','SF':'sf/san-francisco-49ers',
    'SEA':'sea/seattle-seahawks','TB':'tb/tampa-bay-buccaneers','TEN':'ten/tennessee-titans','WAS':'wsh/washington-commanders'
}



def fetch_espn_rosters(active_only: bool = True, limit_per_team: int = 53) -> pd.DataFrame:
    import requests, time
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.espn.com/",
    }

    rows = []
    for abbr, tid in ESPN_TEAM_IDS.items():
        succeeded = None

        # A) site.api team endpoint (entries under team->roster)
        try:
            url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{tid}?enable=roster,depthchart"
            r = requests.get(url, headers=HEADERS, timeout=12)
            if r.ok:
                data = r.json()
                entries = (data.get('team', {}) or {}).get('roster', {}).get('entries') or []
                if entries:
                    roster = []
                    for e in entries:
                        player = e.get('player') or e.get('athlete') or e
                        full = (player or {}).get('fullName') or (player or {}).get('displayName') or (player or {}).get('name')
                        if not full:
                            continue
                        pos = (player or {}).get('position') or {}
                        pos_abbr = (pos or {}).get('abbreviation') or (pos or {}).get('abbrev') or (pos or {}).get('name') or ''
                        if not active_only or (player or {}).get('active') is True:
                            roster.append((full, pos_abbr))
                    if roster:
                        succeeded = "site.api/team?enable=roster"
        except Exception:
            pass

        # B) core.api bulk athletes (usually most reliable)
        if succeeded is None:
            try:
                core = f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/teams/{tid}/athletes?limit=2000"
                r = requests.get(core, headers=HEADERS, timeout=12)
                if r.ok:
                    data = r.json()
                    items = data.get('items') or []
                    roster = []
                    for it in items[:300]:  # plenty
                        # follow each athlete item to get details
                        try:
                            dr = requests.get(it.get('href'), headers=HEADERS, timeout=10)
                            if not dr.ok:
                                continue
                            ad = dr.json()
                            full = ad.get('fullName') or ad.get('displayName') or ad.get('name')
                            pos = (ad.get('position') or {}).get('abbreviation') or ''
                            active = ad.get('active')
                            status_name = ((ad.get('status') or {}).get('type') or {}).get('name')
                            if not active_only or active is True or (status_name and str(status_name).lower() == 'active'):
                                roster.append((full, pos))
                        except Exception:
                            continue
                    if roster:
                        succeeded = "core.api/athletes"
            except Exception:
                pass

        # C) HTML tables as last resort
        if succeeded is None:
            try:
                slug = ESPN_TEAM_SLUGS.get(abbr)
                if slug:
                    html_url = f"https://www.espn.com/nfl/team/roster/_/name/{slug}"
                    hr = requests.get(html_url, headers=HEADERS, timeout=12)
                    if hr.ok:
                        import pandas as pd
                        tables = pd.read_html(hr.text, flavor="lxml")
                        roster = []
                        for t in tables:
                            cols = [str(c).strip().lower() for c in t.columns]
                            # 🔧 Only accept a true roster-shaped table
                            # must have name + pos/position and at least one roster-ish column
                            if (
                                'name' in cols and
                                ('pos' in cols or 'position' in cols) and
                                any(x in cols for x in ('ht','height','wt','weight','age','dob','#','no','number'))
                            ):
                                name_col = t.columns[cols.index('name')]
                                pos_col = t.columns[cols.index('pos')] if 'pos' in cols else (
                                    t.columns[cols.index('position')] if 'position' in cols else None
                                )
                                for _, row in t.iterrows():
                                    full = clean_name(str(row[name_col]))
                                    if not full: 
                                        continue
                                    pos = str(row[pos_col]).upper().strip() if pos_col else ''
                                    roster.append((full, pos))
                        if roster:
                            succeeded = "html"
            except Exception:
                pass


        # normalize + cap to 53
        if succeeded:
            if limit_per_team and len(roster) > limit_per_team:
                roster = roster[:limit_per_team]
            for full, pos in roster:
                rows.append({
                    'team_abbr': abbr,
                    'full_name': clean_name(full),
                    'position': str(pos).upper().strip(),
                    'source': 'espn'
                })
        else:
            print(f"⚠️ ESPN roster fetch failed for {abbr}")

        time.sleep(0.2)  # be kind

    df = pd.DataFrame(rows)
    if df.empty:
        print("⚠️ ESPN roster fetch returned empty.")
        return df
    df['player_key'] = kcanon_series(df['full_name'])
    df['team_full'] = df['team_abbr'].map(lambda a: TEAM_MAP.get(a, a))
    df['position'] = df['position'].where(df['position'].isin(POS_SET), '')
    df['status'] = 'Active'
    return df[['player_key','full_name','team_abbr','team_full','position','status','source']]




def prune_resolved_injuries(staleness_days: int = 10):
    """
    Turn OFF is_active for stale/healed injuries so counts shrink as players recover.
    Rules:
      - IR/PUP stay active until a newer, lower-severity update shows up.
      - Out/Doubtful/Questionable become inactive if not updated in `staleness_days`.
      - Only the most recent row per player stays active; older rows for that player are turned off.
    """
    import numpy as np
    from pandas.tseries.offsets import Day
    # tz-safe "today in UTC"
    now = pd.Timestamp.now(tz="UTC").normalize()


    with engine.begin() as conn:
        inj = pd.read_sql(text("SELECT * FROM nfl_injuries_tracking"), conn)
        if inj.empty:
            print("⚠️ No injuries in table to prune.")
            return 0,0

        cols = {c.lower() for c in inj.columns}
        inj.columns = [c.lower() for c in inj.columns]

        # best available timestamp per row
        date_candidates = [c for c in ['last_seen','last_updated','report_date','injury_date','scraped_at','date'] if c in cols]
        for c in date_candidates:
            inj[c] = pd.to_datetime(inj[c], errors='coerce', utc=True)
        if date_candidates:
            inj['last_dt'] = inj[date_candidates].max(axis=1)
        else:
            inj['last_dt'] = pd.NaT

        # normalize names and severities
        inj['player_name'] = inj['player_name'].fillna('').astype(str)
        key = inj['player_name'].str.strip().str.lower()
        key = key.str.replace(r"[.\-'\s]+", "", regex=True)   # pkey
        inj['pkey'] = key

        desig = inj['designation'].fillna('').astype(str).str.title()
        sev_map = {
            'Ir': 3, 'Injured Reserve': 3, 'Pup': 3, 'Dnp': 3, 'Out': 3,
            'Doubtful': 2, 'Questionable': 1
        }
        inj['severity'] = desig.map(sev_map).fillna(0).astype(int)

        # most recent row per player
        inj = inj.sort_values(['pkey','last_dt']).reset_index(drop=True)
        latest_idx = inj.groupby('pkey')['last_dt'].transform('max') == inj['last_dt']

        # determine which "latest" rows should be active
        latest = inj[latest_idx].copy()
        latest['last_dt'] = pd.to_datetime(latest['last_dt'], utc=True)

        latest['age_days'] = ((now - latest['last_dt']).dt.total_seconds() / 86400.0)
        latest['age_days'] = latest['age_days'].fillna(1e9).astype(int)

        cond_ir = latest['severity'] >= 3  # IR/Out/PUP
        cond_recent = latest['age_days'] <= staleness_days
        latest['active_new'] = np.where(cond_ir | (latest['severity'] >= 1) & cond_recent, 1, 0)

        # ids to activate: the latest row per player if active_new==1
        ids_activate = set(latest.loc[latest['active_new'] == 1, 'id'].astype(int))
        # everything else -> deactivate
        all_ids = set(inj['id'].astype(int))
        ids_deactivate = all_ids - ids_activate
        
        with engine.connect() as conn:
            inj = pd.read_sql(text("SELECT * FROM nfl_injuries_tracking"), conn)
        # write back
        _update_ids_chunked_autocommit(ids_deactivate, flag=0, chunk=200)
        _update_ids_chunked_autocommit(ids_activate,   flag=1, chunk=200)

    print(f"🧹 Pruned injuries. Activated {len(ids_activate)}, deactivated {len(ids_deactivate)}.")
    return len(ids_activate), len(ids_deactivate)

def _exec_with_retry(conn, sql, params, retries=6):
    for i in range(retries):
        try:
            conn.execute(sql, params)
            return
        except OperationalError as e:
            if "database is locked" in str(e).lower():
                time.sleep(0.25 * (2 ** i) + random.random() * 0.1)
                continue
            raise
    conn.execute(sql, params)

def _update_ids_chunked_autocommit(ids, flag, chunk=200):
    ids = list(ids or [])
    if not ids:
        return
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        for i in range(0, len(ids), chunk):
            sub = ids[i:i+chunk]
            placeholders = ",".join(f":x{j}" for j in range(len(sub)))
            params = {"flag": int(flag), **{f"x{j}": v for j, v in enumerate(sub)}}
            _exec_with_retry(conn,
                text(f"UPDATE nfl_injuries_tracking SET is_active=:flag WHERE id IN ({placeholders})"),
                params)
def build_roster_validation():
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS ai_roster_validation_detail"))
        conn.execute(text("""
          CREATE TABLE ai_roster_validation_detail AS
          WITH ptc AS (
            SELECT LOWER(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(full_name),'.',''),'''',''),'-',''),' ','')) AS pkey,
                   full_name, team_abbr AS roster_team, team_source
            FROM player_team_current
          ),
          pgs AS (
            SELECT LOWER(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(player_name),'.',''),'''',''),'-',''),' ','')) AS pkey,
                   UPPER(TRIM(team)) AS pgs_team,
                   season, week,
                   ROW_NUMBER() OVER (PARTITION BY LOWER(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(player_name),'.',''),'''',''),'-',''),' ','')) 
                                      ORDER BY CAST(season AS INT) DESC, CAST(week AS INT) DESC) AS rn
            FROM player_game_stats
            WHERE CAST(season AS INT)=2025
          ),
          latest_pgs AS (SELECT pkey, pgs_team FROM pgs WHERE rn=1)
          SELECT p.full_name, p.roster_team, p.team_source, lp.pgs_team,
                 CASE WHEN lp.pgs_team IS NOT NULL AND lp.pgs_team <> p.roster_team THEN 1 ELSE 0 END AS team_mismatch_2025
          FROM ptc p
          LEFT JOIN latest_pgs lp USING (pkey)
        """))

        conn.execute(text("DROP TABLE IF EXISTS ai_roster_validation_team_summary"))
        conn.execute(text("""
          CREATE TABLE ai_roster_validation_team_summary AS
          SELECT roster_team AS team,
                 SUM(team_mismatch_2025) AS mismatches_2025,
                 SUM(CASE WHEN team_source <> 'espn' THEN 1 ELSE 0 END) AS non_espn_rows,
                 COUNT(*) AS roster_rows
          FROM ai_roster_validation_detail
          GROUP BY roster_team
          ORDER BY mismatches_2025 DESC, non_espn_rows DESC
        """))

def force_espn_team(df: pd.DataFrame,
                    espn_53: pd.DataFrame,
                    espn_by_keypos: dict,
                    espn_key_counts: pd.Series) -> pd.DataFrame:
    df = df.copy()
    df['player_key'] = kcanon_series(df['full_name'])
    df['position']   = df['position'].astype(str).str.upper()
    kp = df['player_key'] + '|' + df['position']

    # A) exact (key+pos)
    df['team_abbr'] = df['team_abbr'].where(~kp.isin(espn_by_keypos),
                                            kp.map(espn_by_keypos))
    # B) safe fallback: name-only if unique on ESPN
    safe = df['player_key'].map(lambda k: espn_key_counts.get(k, 0) == 1)
    name_to_team = dict(zip(espn_53['player_key'], espn_53['team_abbr']))
    df.loc[safe, 'team_abbr'] = df.loc[safe, 'player_key'].map(name_to_team).fillna(df.loc[safe, 'team_abbr'])

    df['team_full'] = df['team_abbr'].map(lambda a: TEAM_MAP.get(a, a))
    return df

def main():
    print("🏈 2025 Roster/ Injury Refresh")
    print("============================================")
    # Turn off stale/healed injuries so the view reflects current reality
    # Turn off stale/healed injuries so the view reflects current reality
    prune_resolved_injuries(staleness_days=7)

    # 0) ESPN 53s first
    espn_53 = fetch_espn_rosters(active_only=True, limit_per_team=53)
    with engine.begin() as conn:
        espn_53.assign(team_source='espn').to_sql('espn_roster_2025', conn, if_exists='replace', index=False)
    if espn_53 is not None and not espn_53.empty:
        espn_53['player_key'] = kcanon_series(espn_53['full_name'])
        espn_53['position']   = espn_53['position'].astype(str).str.upper()

        keypos = espn_53['player_key'] + '|' + espn_53['position']
        espn_by_keypos = dict(zip(keypos, espn_53['team_abbr']))
        espn_key_counts = espn_53['player_key'].value_counts()

    # A/B) existing sources
    players_current = fetch_players_current()
    merged = pull_local_roster_fallbacks()

    # --- collect sources in priority order: ESPN > import_players > local fallbacks
    frames = []
    for df in (espn_53, players_current, merged):
        if df is not None and not df.empty:
            df = force_espn_team(df, espn_53, espn_by_keypos, espn_key_counts)
            frames.append(df)

    # and only then drop_nonplayers + concat
    frames = [drop_nonplayers(f) for f in frames]
    combined = pd.concat(frames, ignore_index=True)



    combined = resolve_team_conflicts(combined)
    combined['player_key'] = kcanon_series(combined['full_name'])


    combined['position'] = combined['position'].astype(str).str.upper().str.strip()
    combined.loc[~combined['position'].isin(POS_SET), 'position'] = ''
    combined['__pri'] = combined['source'].map({'espn': -10, 'import_players': -5}).fillna(0).astype(int)
    combined = combined.sort_values(['player_key','__pri']).drop_duplicates('player_key', keep='first').drop(columns=['__pri'])
    combined = enforce_53_cap(combined, per_team=53)
    print(f"✅ AFTER CAP: {len(combined)} players (≈ 53 × teams + injured without snaps)")
    print(f"✅ Built roster with {len(combined)} players.")
    write_roster_tables(combined)
    build_roster_validation()

    # sanity: top counts
    with engine.begin() as conn:
        cnt = pd.read_sql(text("""
            SELECT team_abbr, COUNT(*) AS n
            FROM player_team_current
            GROUP BY team_abbr
            ORDER BY n DESC
            LIMIT 12
        """), conn)
    print("\nTop teams by player count:")
    for _, r in cnt.iterrows():
        print(f"  {r['team_abbr']}: {int(r['n'])}")

    map_injuries_to_current()
    rebuild_team_injury_impact()
    build_injury_validation_tables()

    # Optional one-shot check while you're here
    prompt_and_verify_team()

    print("\nAll set. Re-run your validator & refresh the dashboard.")

if __name__ == "__main__":
    main()
