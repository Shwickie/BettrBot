# ai_tools.py
import os, sqlite3, math, datetime as dt
from collections import defaultdict
# --- ADD: Player stats + vs-defense helpers ---

import sqlite3, math, re

DB_PATH = os.environ.get("BETTR_DB_PATH", os.path.join(os.path.dirname(__file__), "database.db"))


def _find_table(conn, candidates):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    names = {r[0].lower() for r in cur.fetchall()}
    for c in candidates:
        if c.lower() in names:
            return c
    return None

def player_stats(name: str, season: int | None = None):
    """
    Returns per-season totals and lifetime career lines for a player.
    Expects a table like player_game_stats or player_season_stats.
    # TODO: adjust table/column names to your schema if needed.
    """
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    try:
        t_season = _find_table(db, ["player_season_stats", "season_player_stats", "players_season"])
        t_game   = _find_table(db, ["player_game_stats", "game_player_stats", "players_games"])
        if not (t_season or t_game):
            return {"error":"No player stats table found"}

        qname = f"%{name.strip()}%"
        rows = []
        if t_season:
            sql = f"""
                SELECT player_name, season, team,
                       COALESCE(passing_yards,0) AS pass_yds,
                       COALESCE(rushing_yards,0) AS rush_yds,
                       COALESCE(receiving_yards,0) AS rec_yds,
                       COALESCE(passing_tds,0) AS pass_td,
                       COALESCE(rushing_tds,0) AS rush_td,
                       COALESCE(receiving_tds,0) AS rec_td,
                       COALESCE(receptions,0) AS rec
                FROM {t_season}
                WHERE player_name LIKE ?
                { 'AND season = ?' if season else '' }
                ORDER BY season DESC
            """
            params = [qname] + ([season] if season else [])
            rows = db.execute(sql, params).fetchall()
        elif t_game:
            # roll up games → season if only game table exists
            sql = f"""
                SELECT player_name, season, team,
                       SUM(COALESCE(passing_yards,0)) AS pass_yds,
                       SUM(COALESCE(rushing_yards,0)) AS rush_yds,
                       SUM(COALESCE(receiving_yards,0)) AS rec_yds,
                       SUM(COALESCE(passing_tds,0)) AS pass_td,
                       SUM(COALESCE(rushing_tds,0)) AS rush_td,
                       SUM(COALESCE(receiving_tds,0)) AS rec_td,
                       SUM(COALESCE(receptions,0)) AS rec
                FROM {t_game}
                WHERE player_name LIKE ?
                { 'AND season = ?' if season else '' }
                GROUP BY player_name, season, team
                ORDER BY season DESC
            """
            params = [qname] + ([season] if season else [])
            rows = db.execute(sql, params).fetchall()

        out = [dict(r) for r in rows]
        # simple HTML rendering for the chat
        html = "<strong>Player totals</strong><br>" + "<br>".join(
           f"{r['season']} {r['team']}: {int(r['pass_yds'])} pass • {int(r['rush_yds'])} rush • {int(r['rec_yds'])} rec"
           for r in out
        ) if out else "No results."
        return {"ok": True, "seasons": out, "html": html}
    finally:
        db.close()

def player_vs_defense(player: str, position: str | None = None,
                      since_season: int | None = None, defense_team: str | None = None):
    """
    Aggregates player's production vs defenses (optionally a specific defense), across seasons.
    Expects a game-level player table with opponent column.
    # TODO: adjust table/column names to your schema.
    """
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    try:
        t_game = _find_table(db, ["player_game_stats","game_player_stats","players_games"])
        if not t_game:
            return {"error":"No per-game player table found"}

        qname = f"%{player.strip()}%"
        sql = f"""
            SELECT player_name, season, opponent AS defense,
                   SUM(COALESCE(passing_yards,0)) as pass_yds,
                   SUM(COALESCE(rushing_yards,0)) as rush_yds,
                   SUM(COALESCE(receiving_yards,0)) as rec_yds,
                   SUM(COALESCE(receptions,0)) as rec
            FROM {t_game}
            WHERE player_name LIKE ?
            { 'AND season >= ?' if since_season else '' }
            { 'AND opponent = ?' if defense_team else '' }
            GROUP BY player_name, season, opponent
            ORDER BY season DESC
        """
        params = [qname] + ([since_season] if since_season else []) + ([defense_team] if defense_team else [])
        rows = [dict(r) for r in db.execute(sql, params).fetchall()]
        if not rows:
            return {"ok": True, "html": "No games found."}

        # quick readable output
        lines = []
        for r in rows:
            line = f"{r['season']} vs {r['defense']}: {int(r['pass_yds'])} pass / {int(r['rush_yds'])} rush / {int(r['rec_yds'])} rec"
            if r.get('rec') is not None:
                line += f" ({int(r['rec'])} recs)"
            lines.append(line)
        html = "<strong>Player vs Defense</strong><br>" + "<br>".join(lines)
        return {"ok": True, "splits": rows, "html": html}
    finally:
        db.close()

DB_PATH = os.environ.get("BETTR_DB_PATH", r"E:/Bettr Bot/betting-bot/data/betting.db")
HFA = 2.5  # home field

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c

def _normalize_us(odds):
    try:
        s = str(odds).strip()
        if s.startswith('+'): s = s[1:]
        v = float(s)
    except Exception:
        return None
    # decimal?
    if 1.01 <= v <= 10.0:
        return round((v - 1) * 100) if v >= 2 else round(-100/(v-1))
    return int(round(v))

def _implied_prob(od):
    if od is None:
        return None
    od = int(od)
    return 100.0 / (od + 100.0) if od > 0 else abs(od) / (abs(od) + 100.0)


def _season_now():
    today = dt.date.today()
    return today.year if today.month >= 8 else today.year - 1

def _injury_impact_map(conn):
    # Sum of active injury impact per team (uses your injuries table)
    # nfl_injuries_tracking(team, designation, impact_score, is_active, ...) exists in your DB.
    q = """SELECT team, COALESCE(SUM(impact_score),0) AS inj
           FROM nfl_injuries_tracking
           WHERE is_active=1
           GROUP BY team"""
    d = {r["team"]: float(r["inj"]) for r in conn.execute(q).fetchall()}
    return d

def _power_map(conn):
    # Use team_season_summary(power_score, win_pct, games_played, ...) for all seasons; bias to latest
    # If current season missing, fallback to last season.
    season = _season_now()
    def _read(season_):
        return conn.execute("""SELECT team, power_score, games_played, win_pct
                               FROM team_season_summary WHERE season=?""", (season_,)).fetchall()
    rows = _read(season) or _read(season-1)
    inj = _injury_impact_map(conn)
    out = {}
    for r in rows:
        # same spirit as your API (base + light form - light injury)
        form = ((r["win_pct"] or 0) - 0.5)*20 if (r["games_played"] or 0) > 0 else 0.0
        adj = (float(r["power_score"] or 0)*0.6 + form*0.2 - float(inj.get(r["team"],0))*0.10)
        out[r["team"]] = adj
    return out

def _win_probs(pmap, away, home):
    hm = pmap.get(home, 0.0) + HFA
    aw = pmap.get(away, 0.0)
    ph = 1.0/(1.0 + math.exp(-(hm - aw)/8.0))
    return (1.0 - ph, ph)

def _latest_moneylines(conn, game_id):
    q = """SELECT team, sportsbook, odds
           FROM odds
           WHERE game_id=? AND market='h2h'
           AND (game_id, sportsbook, team, timestamp) IN (
              SELECT game_id, sportsbook, team, MAX(timestamp)
              FROM odds WHERE game_id=? AND market='h2h' GROUP BY game_id, sportsbook, team
           )"""
    rows = conn.execute(q, (game_id, game_id)).fetchall()
    by_team = defaultdict(list)
    for r in rows:
        us = _normalize_us(r["odds"])
        if us is not None:
            by_team[r["team"]].append({"sportsbook": r["sportsbook"], "odds": int(us)})
    return by_team

def _best(by_book):
    if not by_book: return None
    # choose highest US (best for bettor)
    best = max(by_book, key=lambda x: x["odds"])
    return best["odds"], best["sportsbook"]

def _normalize_american(raw):
    """
    Accepts American like '+110'/'-120' or decimal like 1.91/2.35
    and returns an int American price (e.g., 110, -120). Returns None if unusable.
    """
    try:
        s = str(raw).strip()
        if s.startswith('+'):
            s = s[1:]
        v = float(s)
    except Exception:
        return None

    # Decimal odds heuristic
    if 1.01 <= v <= 10.0:
        if v >= 2.0:
            return int(round((v - 1) * 100))
        else:
            return int(round(-100 / (v - 1)))

    # Already American
    return int(round(v))


def _book_count(by_book):
    if not by_book:
        return 0
    return len({str(r.get("sportsbook","")) for r in by_book if r.get("sportsbook") is not None})

def list_value_bets(edge_min=0.05):
    """More conservative value betting with stricter filters"""
    conn = _conn()
    try:
        import datetime as dt, math
        today = dt.date.today()
        end = today + dt.timedelta(days=7)  # Shorter window - only next 7 days

        games = conn.execute("""
            SELECT game_id, home_team, away_team,
                   DATE(game_date) AS d, TIME(start_time_local) AS t
            FROM games
            WHERE DATE(game_date) BETWEEN DATE(?) AND DATE(?)
            AND game_date >= DATE('now')  -- Only future games
            ORDER BY DATE(game_date), TIME(start_time_local)
        """, (today, end)).fetchall()

        pmap = _power_map(conn)
        out = []

        for g in games:
            by_team = _latest_moneylines(conn, g["game_id"])
            pa, ph = _win_probs(pmap, g["away_team"], g["home_team"])
            
            # VERY AGGRESSIVE calibration - NFL markets are 90%+ efficient
            ph_cal = 0.5 + (ph - 0.5) * 0.35  # Only keep 35% of raw edge
            pa_cal = 1.0 - ph_cal

            def _best(by_book):
                if not by_book:
                    return None, None
                rows = []
                for q in by_book:
                    ao = _normalize_american(q.get("odds"))
                    if ao is None:
                        continue
                    rows.append({"odds": int(ao), "sportsbook": str(q.get("sportsbook",""))})
                if not rows:
                    return None, None
                best = max(rows, key=lambda r: r["odds"])  # Best price for bettor
                return best["odds"], best["sportsbook"]

            home_bb = by_team.get(g["home_team"], [])
            away_bb = by_team.get(g["away_team"], [])

            home_ml, home_book = _best(home_bb)
            away_ml, away_book = _best(away_bb)
            home_n = _book_count(home_bb)
            away_n = _book_count(away_bb)

            if home_ml is None or away_ml is None:
                continue

            # Remove juice properly
            ih = _implied_prob(home_ml)
            ia = _implied_prob(away_ml)
            tot = max(ih + ia, 1e-9)
            ih_n, ia_n = ih/tot, ia/tot

            for team, prob, ml, book, nbooks, implied in [
                (g["home_team"], ph_cal, home_ml, home_book, home_n, ih_n),
                (g["away_team"], pa_cal, away_ml, away_book, away_n, ia_n)
            ]:
                # MUCH STRICTER FILTERS:
                
                # 1. Probability range - avoid extreme picks
                prob = max(0.28, min(0.72, prob))
                
                # 2. Need at least 3 sportsbooks (not 2)
                if nbooks < 3:
                    continue
                
                # 3. Skip lines too close to pick'em
                if abs(ml) < 110:
                    continue
                
                # 4. Big underdogs need very strong model conviction
                if ml >= 200 and prob < 0.45:
                    continue
                
                # 5. Skip near coin-flip probabilities
                if abs(prob - 0.50) < 0.05:
                    continue

                # 6. Market efficiency discount (assume market is 88% efficient)
                raw_edge = prob - implied
                edge = raw_edge * 0.88
                
                # 7. Higher minimum edge threshold
                min_threshold = max(0.06, float(edge_min))  # At least 6%
                if edge < min_threshold:
                    continue

                # 8. Conservative Kelly sizing
                dec = 1 + (ml/100.0) if ml > 0 else 1 + (100.0/abs(ml))
                kelly = ((prob*(dec-1)) - (1-prob)) / (dec-1) if dec > 1 else 0.0
                
                # Fractional Kelly with lower cap
                stake = max(2.0, min(20.0, max(0.0, kelly) * 500.0 * 0.12))
                
                # 9. Only include if stake is meaningful
                if stake < 3.0:
                    continue

                out.append({
                    "away_team": g["away_team"], "home_team": g["home_team"],
                    "date": g["d"], "t": (g["t"] or "")[:5],
                    "team": team, "sportsbook": book, "odds": int(ml),
                    "model_prob": round(prob, 3), "implied_prob": round(implied, 3),
                    "edge": round(edge, 3), "edge_pct": round(edge*100, 1),
                    "recommended_amount": round(stake, 2), "game_id": g["game_id"],
                    "raw_edge": round(raw_edge, 3)  # For debugging
                })

        out.sort(key=lambda x: x["edge"], reverse=True)
        
        # Limit to top 2-3 opportunities max (realistic for NFL)
        return out[:3]
        
    finally:
        conn.close()
def game_card(game_id: str):
    conn = _conn()
    try:
        g = conn.execute("""SELECT game_id, home_team, away_team,
                                   DATE(game_date) AS d, TIME(start_time_local) AS t
                            FROM games WHERE game_id=?""", (game_id,)).fetchone()
        if not g: return {"error":"game not found"}
        pmap = _power_map(conn)
        pa, ph = _win_probs(pmap, g["away_team"], g["home_team"])
        by_team = _latest_moneylines(conn, game_id)
        b_home = _best(by_team.get(g["home_team"], []))
        b_away = _best(by_team.get(g["away_team"], []))
        pick_team = g["home_team"] if ph >= pa else g["away_team"]
        pick_conf = ph if ph >= pa else pa
        return {
            "matchup": f"{g['away_team']} @ {g['home_team']}",
            "date": g["d"], "time": (g["t"] or "")[:5],
            "teams": {"home": g["home_team"], "away": g["away_team"]},
            "probs": {"home": round(ph,3), "away": round(pa,3)},
            "best_moneyline": [
                {"team": g["home_team"], "odds": b_home[0] if b_home else None, "sportsbook": b_home[1] if b_home else None},
                {"team": g["away_team"], "odds": b_away[0] if b_away else None, "sportsbook": b_away[1] if b_away else None},
            ],
            "pick": {"team": pick_team, "confidence": round(pick_conf,3)},
            "ev": {
                "home": round((ph - (_implied_prob(b_home[0]) if b_home else 0))*100, 2),
                "away": round((pa - (_implied_prob(b_away[0]) if b_away else 0))*100, 2),
            }
        }
    finally:
        conn.close()

def explain_pick(game_id: str, team: str):
    card = game_card(game_id)
    if card.get("error"): return card
    t = team
    home = card["teams"]["home"]; away = card["teams"]["away"]
    ph = card["probs"]["home"]; pa = card["probs"]["away"]
    p_team = ph if t == home else pa
    # quick narrative factors
    factors = []
    if card["pick"]["team"] == t:
        factors.append("Model edge favors this side versus implied odds.")
        if (ph if t==home else pa) > 0.6: factors.append("Solid (>60%) win probability.")
    else:
        factors.append("Note: other side is the model pick based on power diff.")
    return {
        "team": t,
        "model_probability": round(p_team,3),
        "power_scores": {},  # can be filled with per-team adj power if you want
        "factors": factors
    }
