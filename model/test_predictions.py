# model/test_predictions.py
"""
Quick, leakage-safe prediction check that uses the SAME feature pipeline
as the trainer: pre-game rolling stats + rest + matchup + engineered diffs.
"""

import sqlite3
import pandas as pd
import numpy as np
import pickle
import os
from datetime import datetime

DB_PATH    = r"E:/Bettr Bot/betting-bot/data/betting.db"
MODEL_PATH = r"E:/Bettr Bot/betting-bot/models/betting_model_fixed.pkl"

# ---------- tiny feature builder (mirrors trainer) ----------
class FeatureBuilder:
    def __init__(self, conn):
        self.conn = conn
        self.team_name_map = {}
        self._init_team_map()

    def _init_team_map(self):
        ABBR_TO_FULL = {
            'ARI':'Arizona Cardinals','ATL':'Atlanta Falcons','BAL':'Baltimore Ravens','BUF':'Buffalo Bills',
            'CAR':'Carolina Panthers','CHI':'Chicago Bears','CIN':'Cincinnati Bengals','CLE':'Cleveland Browns',
            'DAL':'Dallas Cowboys','DEN':'Denver Broncos','DET':'Detroit Lions','GB':'Green Bay Packers',
            'HOU':'Houston Texans','IND':'Indianapolis Colts','JAX':'Jacksonville Jaguars','KC':'Kansas City Chiefs',
            'LV':'Las Vegas Raiders','LAC':'Los Angeles Chargers','LAR':'Los Angeles Rams','MIA':'Miami Dolphins',
            'MIN':'Minnesota Vikings','NE':'New England Patriots','NO':'New Orleans Saints','NYG':'New York Giants',
            'NYJ':'New York Jets','PHI':'Philadelphia Eagles','PIT':'Pittsburgh Steelers','SF':'San Francisco 49ers',
            'SEA':'Seattle Seahawks','TB':'Tampa Bay Buccaneers','TEN':'Tennessee Titans','WAS':'Washington Commanders'
        }
        m={}
        for abbr, full in ABBR_TO_FULL.items():
            m[abbr]=abbr; m[full]=abbr; m[full.upper()]=abbr; m[full.lower()]=abbr
        m.update({'LA':'LAR','Los Angeles':'LAR','Las Vegas':'LV','Oakland':'LV','Washington':'WAS',
                  'Washington Redskins':'WAS','San Francisco':'SF','Green Bay':'GB','Kansas City':'KC',
                  'New England':'NE','New York Giants':'NYG','New York Jets':'NYJ','Tampa Bay':'TB','New Orleans':'NO'})
        self.team_name_map = m

    def _normalize_team(self, name):
        if name is None: return None
        s = str(name).strip()
        if s in self.team_name_map: return self.team_name_map[s]
        if s.upper() in self.team_name_map: return self.team_name_map[s.upper()]
        if len(s) <= 3: return s.upper()
        for k, ab in self.team_name_map.items():
            if s.lower() in k.lower() or k.lower() in s.lower():
                return ab
        return s[:3].upper()

    def load_games(self, years_back=3):
        q = """
        SELECT 
            g.game_id, g.home_team, g.away_team,
            g.home_score, g.away_score, g.game_date,
            CASE WHEN g.home_score > g.away_score THEN 1 ELSE 0 END AS home_win,
            CAST(strftime('%Y', g.game_date) AS INTEGER) AS season,
            CAST(strftime('%m', g.game_date) AS INTEGER) AS month,
            CAST(strftime('%w', g.game_date) AS INTEGER) AS day_of_week
        FROM games g
        WHERE g.home_score IS NOT NULL AND g.away_score IS NOT NULL
          AND g.game_date > date('now', ?)
        ORDER BY g.game_date
        """
        return pd.read_sql_query(q, self.conn, params=[f'-{years_back} years'])

    def pregame_rollups(self, df):
        df = df.sort_values('game_date').copy()
        for side in ('home','away'):
            wpct_pre=[]; pf_pre=[]; pa_pre=[]; pd_pre=[]
            totals={}
            for _, r in df.iterrows():
                t = self._normalize_team(r[f'{side}_team'])
                if t not in totals:
                    totals[t]={'gp':0,'w':0,'pf_sum':0.0,'pa_sum':0.0}
                cur=totals[t]; gp=cur['gp']
                pfm = cur['pf_sum']/gp if gp>0 else 20.0
                pam = cur['pa_sum']/gp if gp>0 else 20.0
                wpct_pre.append((cur['w']/gp) if gp>0 else 0.5)
                pf_pre.append(pfm); pa_pre.append(pam); pd_pre.append(pfm-pam)
                # update with current game
                pf = float(r['home_score'] if side=='home' else r['away_score'])
                pa = float(r['away_score'] if side=='home' else r['home_score'])
                win = 1 if pf>pa else 0
                cur['gp']+=1; cur['w']+=win; cur['pf_sum']+=pf; cur['pa_sum']+=pa
            df[f'{side}_wpct_pre']=wpct_pre
            df[f'{side}_pf_pre']=pf_pre
            df[f'{side}_pa_pre']=pa_pre
            df[f'{side}_pd_pre']=pd_pre
        return df

    def momentum(self, df):
        df['home_form']=0.5; df['home_streak']=0
        df['away_form']=0.5; df['away_streak']=0
        # lightweight: last 5 wins pulled from games table
        teams = pd.concat([df['home_team'], df['away_team']]).astype(str).unique()
        recs={}
        for t in teams:
            q = """
            SELECT game_date,
                   CASE WHEN home_team = ? THEN 
                        CASE WHEN home_score > away_score THEN 1 ELSE 0 END
                   ELSE CASE WHEN away_score > home_score THEN 1 ELSE 0 END
                   END AS won
            FROM games
            WHERE (home_team=? OR away_team=?)
              AND home_score IS NOT NULL AND away_score IS NOT NULL
            ORDER BY game_date DESC
            """
            try:
                g = pd.read_sql_query(q, self.conn, params=[t,t,t])
                g['game_date']=pd.to_datetime(g['game_date'])
                recs[t]=g
            except:
                recs[t]=pd.DataFrame(columns=['game_date','won'])
        for i,r in df.iterrows():
            if i % 500==0: pass
            gd = pd.to_datetime(r['game_date'])
            for side in ('home','away'):
                t=str(r[f'{side}_team'])
                rr=recs.get(t)
                if rr is None or rr.empty:
                    form,streak=0.5,0
                else:
                    recent = rr[rr['game_date']<gd].head(5)
                    if len(recent)<2: form,streak=0.5,0
                    else:
                        form=float(recent['won'].mean())
                        streak=0
                        last=int(recent['won'].iloc[0]) if len(recent)>0 else 0
                        for w in recent['won']:
                            if int(w)==last: streak+=1
                            else: break
                        if last==0: streak=-streak
                df.at[i,f'{side}_form']=form
                df.at[i,f'{side}_streak']=streak
        return df

    def matchup_flags(self, df):
        divisions={
            'AFC_EAST':['BUF','MIA','NE','NYJ'],'AFC_NORTH':['BAL','CIN','CLE','PIT'],
            'AFC_SOUTH':['HOU','IND','JAX','TEN'],'AFC_WEST':['DEN','KC','LV','LAC'],
            'NFC_EAST':['DAL','NYG','PHI','WAS'],'NFC_NORTH':['CHI','DET','GB','MIN'],
            'NFC_SOUTH':['ATL','CAR','NO','TB'],'NFC_WEST':['ARI','LAR','SF','SEA']
        }
        t2d={}
        for d,teams in divisions.items():
            for t in teams:
                t2d[t]=d; t2d[self._normalize_team(t)]=d
        df['same_division']=df.apply(
            lambda r: 1 if t2d.get(self._normalize_team(r['home_team']))==
                           t2d.get(self._normalize_team(r['away_team'])) else 0, axis=1)
        df['same_conference']=df.apply(
            lambda r: 1 if (t2d.get(self._normalize_team(r['home_team']), '')[:3] ==
                            t2d.get(self._normalize_team(r['away_team']), '')[:3]) else 0, axis=1)
        return df

    def rest(self, df):
        df=df.sort_values('game_date').copy()
        last={}; hr=[]; ar=[]
        for _,r in df.iterrows():
            gd=pd.to_datetime(r['game_date'])
            ht=self._normalize_team(r['home_team']); at=self._normalize_team(r['away_team'])
            hr.append( (gd-last[ht]).days if ht in last else 7 )
            ar.append( (gd-last[at]).days if at in last else 7 )
            last[ht]=gd; last[at]=gd
        df['home_rest_days']=hr; df['away_rest_days']=ar
        df['rest_diff']=df['home_rest_days']-df['away_rest_days']
        return df

    def engineer(self, df):
        df=df.fillna(0)
        df['home_power_pre']=df['home_pd_pre']
        df['away_power_pre']=df['away_pd_pre']
        df['power_diff']=df['home_power_pre']-df['away_power_pre']
        df['win_pct_diff']=df['home_wpct_pre']-df['away_wpct_pre']
        df['offense_diff']=df['home_pf_pre']-df['away_pf_pre']
        df['home_def_str']=-df['home_pa_pre']; df['away_def_str']=-df['away_pa_pre']
        df['defense_diff']=df['home_def_str']-df['away_def_str']
        if 'home_form' in df.columns:
            df['form_diff']=df['home_form']-df['away_form']
            df['streak_diff']=df['home_streak']-df['away_streak']
        df['home_field_advantage']=2.5
        df['late_season']=(df['month']>=11).astype(int)
        df['prime_time']=((df['day_of_week']==0)|(df['day_of_week']==1)).astype(int)
        df['both_good']=((df['home_power_pre']>2)&(df['away_power_pre']>2)).astype(int)
        df['mismatch_game']=(df['power_diff'].abs()>5).astype(int)
        df['power_x_form']=df['power_diff']*df.get('form_diff',0)
        df['strength_disparity']=(df['home_power_pre']-df['away_power_pre']).abs()
        return df

    def build_dataset(self, years_back=3):
        df = self.load_games(years_back)
        df = self.pregame_rollups(df)
        df = self.momentum(df)
        df = self.matchup_flags(df)
        df = self.rest(df)
        df = self.engineer(df)
        return df

# ---------- main evaluation ----------
def main():
    if not os.path.exists(MODEL_PATH):
        print(f"Model not found at {MODEL_PATH}. Train first.")
        return

    with open(MODEL_PATH, 'rb') as f:
        bundle = pickle.load(f)
    model = bundle['model']
    scaler = bundle['scaler']
    feats  = bundle['feature_cols']
    uses_scaled = bundle.get('uses_scaled', False)

    print("Loading fixed model...")
    print(f"Model version: {bundle.get('model_version')}")
    print(f"Features: {len(feats)}")

    conn = sqlite3.connect(DB_PATH)
    fb = FeatureBuilder(conn)
    df = fb.build_dataset(years_back=3)  # same horizon as training
    conn.close()

    # Evaluate on last N completed games
    N = 20
    eval_df = df.sort_values('game_date').tail(N).copy()

    X = eval_df[feats].copy()
    if uses_scaled:
        X = scaler.transform(X)

    proba = model.predict_proba(X)[:,1]
    eval_df['p_home']=proba
    eval_df['pred_home']=(eval_df['p_home']>0.5).astype(int)

    # Print a few
    print("\nTesting model predictions vs actual results:")
    print("="*60)
    for _,r in eval_df.head(10).iterrows():
        away=r['away_team']; home=r['home_team']
        print(f"{away} @ {home}")
        print(f"  Score: {away} {int(r['away_score'])} - {int(r['home_score'])} {home}")
        print(f"  Model: {r['p_home']:.1%} home | {1-r['p_home']:.1%} away")
        pred = "HOME" if r['p_home']>0.5 else "AWAY"
        actual = "HOME" if r['home_win']==1 else "AWAY"
        mark = "✓" if pred==actual else "✗"
        print(f"  Predicted: {pred} | Actual: {actual} {mark}")
        print(f"  Power diff (as-of): {r['power_diff']:.2f}")
        print()

    # Summary
    rng = proba.max()-proba.min()
    uniq = len(np.unique(np.round(proba,3)))
    acc = ( (proba>0.5).astype(int)==eval_df['home_win'].values ).mean()
    print("PREDICTION QUALITY CHECK:")
    print(f"Range: {proba.min():.1%} to {proba.max():.1%} (Δ={rng:.1%})")
    print(f"Unique probs (rounded .003): {uniq}")
    print(f"Directional accuracy on last {N}: {acc:.1%}")

if __name__=="__main__":
    main()
