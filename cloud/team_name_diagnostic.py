#!/usr/bin/env python3
"""
Team name diagnostic script - Find mismatches between database and API
"""

from sqlalchemy import create_engine, text
import requests

def main():
    print("TEAM NAME DIAGNOSTIC")
    print("=" * 30)
    
    try:
        # Connect to database
        engine = create_engine('postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres')
        
        print("Checking database teams...")
        with engine.connect() as conn:
            db_teams = conn.execute(text("""
                SELECT DISTINCT home_team FROM games 
                WHERE game_date >= CURRENT_DATE 
                ORDER BY home_team
            """)).fetchall()
            
            db_team_list = [t[0] for t in db_teams]
            print(f"Database teams ({len(db_team_list)}):")
            for team in db_team_list:
                print(f"  {team}")
        
        print("\nChecking API teams...")
        response = requests.get(
            'https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds',
            params={
                'apiKey': '2ea42e6f961b41a105cd8dac8a3490a8',
                'regions': 'us',
                'markets': 'h2h'
            }
        )
        
        if response.status_code != 200:
            print(f"API Error: {response.status_code}")
            return
            
        api_teams = set()
        games_data = response.json()
        
        for game in games_data:
            api_teams.add(game['home_team'])
            api_teams.add(game['away_team'])
        
        api_team_list = sorted(api_teams)
        print(f"API teams ({len(api_team_list)}):")
        for team in api_team_list:
            print(f"  {team}")
        
        # Find mismatches
        print("\n" + "=" * 50)
        print("MISMATCH ANALYSIS")
        
        # Teams in database but not in API
        db_set = set(db_team_list)
        api_set = set(api_team_list)
        
        db_only = db_set - api_set
        api_only = api_set - db_set
        
        if db_only:
            print(f"\nTeams in DATABASE but NOT in API ({len(db_only)}):")
            for team in sorted(db_only):
                print(f"  {team}")
        
        if api_only:
            print(f"\nTeams in API but NOT in DATABASE ({len(api_only)}):")
            for team in sorted(api_only):
                print(f"  {team}")
        
        # Check current mapping
        team_mapping = {
            'Arizona Cardinals': 'ARI', 'Atlanta Falcons': 'ATL', 'Baltimore Ravens': 'BAL',
            'Buffalo Bills': 'BUF', 'Carolina Panthers': 'CAR', 'Chicago Bears': 'CHI',
            'Cincinnati Bengals': 'CIN', 'Cleveland Browns': 'CLE', 'Dallas Cowboys': 'DAL',
            'Denver Broncos': 'DEN', 'Detroit Lions': 'DET', 'Green Bay Packers': 'GB',
            'Houston Texans': 'HOU', 'Indianapolis Colts': 'IND', 'Jacksonville Jaguars': 'JAX',
            'Kansas City Chiefs': 'KC', 'Las Vegas Raiders': 'LV', 'Los Angeles Chargers': 'LAC',
            'Los Angeles Rams': 'LAR', 'Miami Dolphins': 'MIA', 'Minnesota Vikings': 'MIN',
            'New England Patriots': 'NE', 'New Orleans Saints': 'NO', 'New York Giants': 'NYG',
            'New York Jets': 'NYJ', 'Philadelphia Eagles': 'PHI', 'Pittsburgh Steelers': 'PIT',
            'San Francisco 49ers': 'SF', 'Seattle Seahawks': 'SEA', 'Tampa Bay Buccaneers': 'TB',
            'Tennessee Titans': 'TEN', 'Washington Commanders': 'WAS'
        }
        
        print(f"\nCURRENT MAPPING CHECK:")
        print("API Team -> Database Team (Expected)")
        
        missing_mappings = []
        for api_team in api_team_list:
            expected_db_team = team_mapping.get(api_team, "MISSING MAPPING")
            if expected_db_team == "MISSING MAPPING":
                missing_mappings.append(api_team)
            print(f"  {api_team} -> {expected_db_team}")
        
        if missing_mappings:
            print(f"\nMISSING MAPPINGS ({len(missing_mappings)}):")
            for team in missing_mappings:
                print(f"  '{team}': 'XXX',  # ADD THIS TO TEAM_MAPPING")
        
        print("\n" + "=" * 50)
        print("RECOMMENDATION:")
        if missing_mappings:
            print("Add the missing mappings above to your migrate_odds.py TEAM_MAPPING dictionary")
        elif db_only or api_only:
            print("Team name format mismatch - check if database uses abbreviations vs full names")
        else:
            print("All teams should be mapping correctly. Check game date matching instead.")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()