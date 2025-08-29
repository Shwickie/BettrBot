#!/usr/bin/env python3
"""
Check Available Markets - See what player props The Odds API supports
"""

import requests

# Your API config
API_KEY = '2ea42e6f961b41a105cd8dac8a3490a8'
SPORT = 'americanfootball_nfl'

def check_available_markets():
    """Check what markets are available for NFL"""
    print("🔍 CHECKING AVAILABLE MARKETS")
    print("=" * 40)
    
    # Get list of available markets
    url = f'https://api.the-odds-api.com/v4/sports/{SPORT}'
    params = {'apiKey': API_KEY}
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        print(f"✅ Sport: {data.get('title', 'Unknown')}")
        print(f"📅 Active: {data.get('active', False)}")
        print(f"🏈 Description: {data.get('description', 'No description')}")
    
    # Try to get sample odds to see available markets
    print(f"\n🎯 TESTING BASIC MARKETS")
    print("=" * 30)
    
    basic_markets = ['h2h', 'spreads', 'totals']
    
    for market in basic_markets:
        url = f'https://api.the-odds-api.com/v4/sports/{SPORT}/odds'
        params = {
            'apiKey': API_KEY,
            'regions': 'us',
            'markets': market,
            'oddsFormat': 'decimal'
        }
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            games = response.json()
            print(f"✅ {market}: {len(games)} games found")
        else:
            print(f"❌ {market}: Error {response.status_code}")
    
    # Test player prop markets one by one
    print(f"\n🏈 TESTING PLAYER PROP MARKETS")
    print("=" * 35)
    
    player_markets = [
        'player_pass_tds',
        'player_pass_yds', 
        'player_rush_yds',
        'player_rec_yds',
        'player_receiving_yards',
        'player_rushing_yards',
        'player_passing_yards',
        'player_pass_completions',
        'player_receptions',
        'player_anytime_td',
        'player_first_td'
    ]
    
    working_markets = []
    
    for market in player_markets:
        url = f'https://api.the-odds-api.com/v4/sports/{SPORT}/odds'
        params = {
            'apiKey': API_KEY,
            'regions': 'us',
            'markets': market,
            'oddsFormat': 'decimal'
        }
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            games = response.json()
            print(f"✅ {market}: {len(games)} games found")
            working_markets.append(market)
        else:
            print(f"❌ {market}: Error {response.status_code} - {response.json().get('message', 'Unknown error')}")
    
    print(f"\n💰 WORKING PLAYER PROP MARKETS:")
    print("=" * 35)
    for market in working_markets:
        print(f"  ✅ {market}")
    
    return working_markets

if __name__ == "__main__":
    working_markets = check_available_markets()
    
    print(f"\n🔧 FIXED MARKETS STRING:")
    if working_markets:
        markets_string = 'h2h,spreads,totals,' + ','.join(working_markets)
        print(f"MARKETS = '{markets_string}'")
    else:
        print("MARKETS = 'h2h,spreads,totals'")