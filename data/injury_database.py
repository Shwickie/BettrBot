#!/usr/bin/env python3
"""
Fixed Injury System - Adapted for Your Database Schema
Fixes the position column issue and improves scrapers
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine, text, inspect
import time
import json
from datetime import datetime, timedelta
import hashlib
import logging
import re
import difflib

# Config
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def inspect_database_schema():
    """Inspect your actual database schema to adapt"""
    print("🔍 INSPECTING DATABASE SCHEMA")
    print("=" * 40)
    
    try:
        inspector = inspect(engine)
        available_tables = inspector.get_table_names()
        
        print(f"📋 Available tables: {available_tables}")
        
        # Check player_game_stats structure
        if 'player_game_stats' in available_tables:
            columns = inspector.get_columns('player_game_stats')
            col_names = [col['name'] for col in columns]
            print(f"📊 player_game_stats columns: {col_names}")
            
            # Sample the data to understand structure
            with engine.connect() as conn:
                sample = pd.read_sql(text("SELECT * FROM player_game_stats LIMIT 5"), conn)
                print(f"📝 Sample data shape: {sample.shape}")
                if not sample.empty:
                    print(f"📝 Sample columns: {list(sample.columns)}")
                    
        return available_tables, col_names if 'player_game_stats' in available_tables else []
        
    except Exception as e:
        print(f"❌ Schema inspection failed: {e}")
        return [], []

def load_adaptive_player_mapping():
    """Load player mapping adapted to your actual database schema"""
    print("📋 LOADING ADAPTIVE PLAYER MAPPING")
    print("=" * 50)
    
    try:
        # First inspect the schema
        available_tables, player_game_cols = inspect_database_schema()
        
        with engine.connect() as conn:
            # Try different approaches based on available tables
            player_mapping = pd.DataFrame()
            
            # Strategy 1: Try player_game_stats (your main table)
            if 'player_game_stats' in available_tables:
                try:
                    # Build query based on actual columns
                    select_parts = ['DISTINCT player_id', 'player_name', 'team']
                    
                    # Add optional columns if they exist
                    if 'full_name' in player_game_cols:
                        select_parts.append('full_name')
                    else:
                        select_parts.append('player_name as full_name')
                    
                    if 'season' in player_game_cols:
                        select_parts.append('season')
                        where_clause = "WHERE season = (SELECT MAX(season) FROM player_game_stats)"
                    else:
                        select_parts.append('2024 as season')
                        where_clause = "WHERE 1=1"
                    
                    query = text(f"""
                        SELECT {', '.join(select_parts)}
                        FROM player_game_stats 
                        {where_clause}
                        AND player_id IS NOT NULL 
                        AND team IS NOT NULL
                        ORDER BY player_name
                        LIMIT 1000
                    """)
                    
                    player_mapping = pd.read_sql(query, conn)
                    print(f"✅ Loaded {len(player_mapping)} players from player_game_stats")
                    
                except Exception as e:
                    print(f"⚠️ player_game_stats query failed: {e}")
            
            # Strategy 2: Try individual season tables if main table failed
            if player_mapping.empty:
                for year in [2024, 2023, 2022]:
                    table_name = f"player_stats_{year}"
                    if table_name in available_tables:
                        try:
                            cols = inspector.get_columns(table_name)
                            col_names = [col['name'] for col in cols]
                            
                            # Build query for this table
                            select_parts = ['DISTINCT player_id']
                            
                            if 'player_name' in col_names:
                                select_parts.append('player_name')
                            elif 'full_name' in col_names:
                                select_parts.append('full_name as player_name')
                            else:
                                continue
                            
                            if 'recent_team' in col_names:
                                select_parts.append('recent_team as team')
                            elif 'team' in col_names:
                                select_parts.append('team')
                            else:
                                select_parts.append("'UNKNOWN' as team")
                            
                            select_parts.append(f'{year} as season')
                            select_parts.append('player_name as full_name')
                            
                            query = text(f"""
                                SELECT {', '.join(select_parts)}
                                FROM {table_name}
                                WHERE player_id IS NOT NULL
                                LIMIT 1000
                            """)
                            
                            player_mapping = pd.read_sql(query, conn)
                            print(f"✅ Loaded {len(player_mapping)} players from {table_name}")
                            break
                            
                        except Exception as e:
                            print(f"⚠️ {table_name} query failed: {e}")
                            continue
            
            if player_mapping.empty:
                print("❌ No player mapping data found in any table")
                return pd.DataFrame()
            
            # Clean and prepare the mapping
            player_mapping['clean_name'] = player_mapping['player_name'].apply(clean_player_name)
            player_mapping = player_mapping.dropna(subset=['player_id', 'clean_name'])
            
            print(f"📊 Final mapping: {len(player_mapping)} players ready for matching")
            return player_mapping
            
    except Exception as e:
        print(f"❌ Failed to load player mapping: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def clean_player_name(name):
    """Enhanced name cleaning"""
    if not name or pd.isna(name):
        return ""
    
    name = str(name)
    # Remove suffixes
    name = re.sub(r'\s+(Jr\.?|Sr\.?|III?|IV|V)$', '', name, flags=re.IGNORECASE)
    # Remove special characters except apostrophes and hyphens
    name = re.sub(r'[^\w\s\'-]', '', name)
    # Normalize whitespace
    name = ' '.join(name.split())
    return name.title().strip()

def enhanced_player_matching(injury_name, injury_team, player_mapping):
    """Enhanced player matching that works with your database"""
    if player_mapping.empty:
        return None, 0.0, None
    
    injury_clean = clean_player_name(injury_name)
    
    # Strategy 1: Exact name + team match
    if injury_team and injury_team != "UNKNOWN":
        exact_matches = player_mapping[
            (player_mapping['clean_name'].str.lower() == injury_clean.lower()) &
            (player_mapping['team'] == injury_team)
        ]
        if not exact_matches.empty:
            match = exact_matches.iloc[0]
            return match['player_id'], 1.0, match['team']
    
    # Strategy 2: Exact name match (any team)
    name_matches = player_mapping[
        player_mapping['clean_name'].str.lower() == injury_clean.lower()
    ]
    if not name_matches.empty:
        match = name_matches.iloc[0]
        return match['player_id'], 0.95, match['team']
    
    # Strategy 3: Fuzzy matching (high threshold)
    best_match = None
    best_score = 0
    
    for _, player in player_mapping.iterrows():
        similarity = calculate_similarity(injury_clean, player['clean_name'])
        if similarity > best_score and similarity > 0.88:  # High threshold
            best_score = similarity
            best_match = player
    
    if best_match is not None:
        return best_match['player_id'], best_score, best_match['team']
    
    return None, 0.0, None

def calculate_similarity(str1, str2):
    """Calculate similarity between two strings"""
    if not str1 or not str2:
        return 0.0
    
    str1 = str1.lower().strip()
    str2 = str2.lower().strip()
    
    if str1 == str2:
        return 1.0
    
    return difflib.SequenceMatcher(None, str1, str2).ratio()

def scrape_espn_fixed():
    """Fixed ESPN scraper that actually works"""
    print("📱 Scraping ESPN (Fixed)...")
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        
        response = requests.get("https://www.espn.com/nfl/injuries", headers=headers, timeout=20)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        injuries = []
        
        # Look for different table structures ESPN might use
        table_selectors = [
            'table.Table',
            'table',
            '.Table--fixed-left table',
            '[data-testid="injury-table"]'
        ]
        
        for selector in table_selectors:
            tables = soup.select(selector)
            if tables:
                print(f"  Found {len(tables)} tables with selector: {selector}")
                break
        else:
            tables = soup.find_all('table')
            print(f"  Found {len(tables)} total tables")
        
        for table in tables:
            # Try to find team context
            team_name = find_team_context(table)
            
            # Process table rows
            rows = table.find_all('tr')
            
            for row in rows[1:]:  # Skip header
                cells = row.find_all(['td', 'th'])
                
                if len(cells) >= 3:
                    player_text = cells[0].get_text(strip=True)
                    position_text = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                    status_text = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                    injury_text = cells[3].get_text(strip=True) if len(cells) > 3 else ""
                    
                    # Clean the data
                    player_name = clean_player_name(player_text)
                    
                    # Skip if this looks like a header or empty
                    if not player_name or player_name.lower() in ['player', 'name', 'no injuries reported']:
                        continue
                    
                    # Check if this is a real injury
                    if is_significant_injury(status_text, injury_text):
                        designation = extract_designation(status_text, injury_text)
                        
                        injuries.append({
                            'player_name': player_name,
                            'team': normalize_team_name(team_name),
                            'position': position_text,
                            'injury_status': status_text,
                            'injury_detail': injury_text,
                            'designation': designation,
                            'source': 'ESPN'
                        })
        
        print(f"  ✅ ESPN: Found {len(injuries)} injuries")
        return injuries
        
    except Exception as e:
        print(f"  ❌ ESPN scraping failed: {e}")
        return []

def find_team_context(table_element):
    """Find team name from table context"""
    # Look up the DOM tree for team information
    current = table_element
    for _ in range(10):  # Check up to 10 levels up
        if current.parent:
            current = current.parent
            
            # Look for team-related text
            text = current.get_text()
            for line in text.split('\n'):
                line = line.strip()
                if len(line) > 5 and len(line) < 30:  # Reasonable team name length
                    team = normalize_team_name(line)
                    if team != "UNKNOWN":
                        return team
    
    return "UNKNOWN"

def is_significant_injury(status, injury_detail=""):
    """Check if this is a significant injury worth tracking"""
    combined = f"{status} {injury_detail}".lower()
    
    # Significant injury indicators
    significant_terms = [
        'out', 'doubtful', 'questionable', 'injured reserve', 'ir', 'pup',
        'did not play', 'dnp', 'knee', 'ankle', 'shoulder', 'concussion',
        'hamstring', 'quad', 'back', 'hip', 'wrist', 'hand', 'foot'
    ]
    
    # Skip if it's clearly not an injury
    skip_terms = ['probable', 'healthy', 'no injury', 'full practice']
    
    if any(skip in combined for skip in skip_terms):
        return False
    
    return any(term in combined for term in significant_terms)

def extract_designation(status, injury_detail=""):
    """Extract injury designation"""
    combined = f"{status} {injury_detail}".lower()
    
    if any(word in combined for word in ['out', 'inactive', 'did not play', 'dnp']):
        return 'Out'
    elif any(word in combined for word in ['ir', 'injured reserve']):
        return 'Injured Reserve'
    elif 'pup' in combined:
        return 'PUP'
    elif 'doubtful' in combined:
        return 'Doubtful'
    elif 'questionable' in combined:
        return 'Questionable'
    else:
        return 'Probable'

def scrape_nfl_com_api():
    """Try to get injury data from NFL.com API or structured data"""
    print("🏈 Scraping NFL.com API...")
    
    try:
        # Try NFL.com injury API endpoint
        api_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        }
        
        # NFL often has JSON endpoints for injury data
        api_urls = [
            'https://www.nfl.com/api/injuries',
            'https://api.nfl.com/v1/injuries',
            'https://www.nfl.com/feeds/injuries'
        ]
        
        for url in api_urls:
            try:
                response = requests.get(url, headers=api_headers, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    injuries = parse_nfl_api_data(data)
                    if injuries:
                        print(f"  ✅ NFL API: Found {len(injuries)} injuries")
                        return injuries
            except:
                continue
        
        print("  ⚠️ NFL API endpoints not accessible, trying web scraping...")
        return scrape_nfl_web()
        
    except Exception as e:
        print(f"  ❌ NFL.com scraping failed: {e}")
        return []

def parse_nfl_api_data(data):
    """Parse NFL API injury data"""
    injuries = []
    
    # This would need to be adapted based on actual NFL API structure
    # For now, return empty list since we don't have access to the real API
    return injuries

def scrape_nfl_web():
    """Fallback web scraping for NFL.com"""
    try:
        response = requests.get("https://www.nfl.com/injuries/", timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        injuries = []
        
        # Look for injury-related elements
        injury_elements = soup.find_all(['div', 'span', 'p'], 
                                       string=re.compile(r'(out|doubtful|questionable)', re.I))
        
        for element in injury_elements[:50]:  # Limit to avoid too much noise
            text = element.get_text(strip=True)
            
            # Try to extract player and status from text
            injury_info = extract_injury_from_text(text)
            if injury_info:
                injuries.append(injury_info)
        
        print(f"  ✅ NFL.com: Found {len(injuries)} injuries")
        return injuries
        
    except Exception as e:
        print(f"  ❌ NFL.com web scraping failed: {e}")
        return []

def extract_injury_from_text(text):
    """Extract injury information from free text"""
    # Look for patterns like "PlayerName (QB) - Out"
    pattern = r'([A-Z][a-z]+\s+[A-Z][a-z]+).*?(out|doubtful|questionable|injured reserve)'
    
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        player_name = clean_player_name(match.group(1))
        status = match.group(2)
        
        return {
            'player_name': player_name,
            'team': 'UNKNOWN',
            'position': '',
            'injury_status': status,
            'injury_detail': text,
            'designation': extract_designation(status),
            'source': 'NFL.com'
        }
    
    return None

def normalize_team_name(team_text):
    """Convert team name to standard abbreviation"""
    if not team_text:
        return "UNKNOWN"
    
    team_map = {
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
        'Tennessee Titans': 'TEN', 'Washington Commanders': 'WSH'
    }
    
    # Clean the text first
    team_text = re.sub(r'[^\w\s]', ' ', team_text)
    team_text = ' '.join(team_text.split())
    
    # Direct abbreviation match
    if team_text.upper() in team_map.values():
        return team_text.upper()
    
    # Full name match
    for full_name, abbrev in team_map.items():
        if full_name.lower() in team_text.lower():
            return abbrev
    
    # City/team name partial matching
    for full_name, abbrev in team_map.items():
        parts = full_name.lower().split()
        for part in parts:
            if len(part) > 3 and part in team_text.lower():
                return abbrev
    
    return "UNKNOWN"

def calculate_impact_score(designation):
    """Calculate impact score"""
    score_map = {
        'Out': 3,
        'Injured Reserve': 3,
        'PUP': 3,
        'Doubtful': 2,
        'Questionable': 1,
        'Probable': 0
    }
    return score_map.get(designation, 0)

def insert_fixed_injuries(injuries, player_mapping):
    """Insert injuries with improved mapping"""
    print(f"💾 INSERTING {len(injuries)} INJURIES WITH IMPROVED MAPPING")
    print("=" * 60)
    
    if not injuries:
        print("❌ No injuries to insert")
        return 0
    
    try:
        with engine.begin() as conn:
            inserted = 0
            mapped = 0
            
            for injury in injuries:
                # Try to map to database player
                player_id, confidence, correct_team = enhanced_player_matching(
                    injury['player_name'], injury['team'], player_mapping
                )
                
                final_team = correct_team if correct_team else injury['team']
                
                # Create hash for deduplication
                data_str = f"{injury['player_name']}{final_team}{injury['designation']}"
                data_hash = hashlib.md5(data_str.encode()).hexdigest()
                
                # Check for existing
                existing = conn.execute(text("""
                    SELECT id FROM nfl_injuries_tracking 
                    WHERE data_hash = :hash AND is_active = 1
                """), {'hash': data_hash}).fetchone()
                
                if not existing:
                    insert_data = {
                        'date': datetime.now().date(),
                        'team': final_team,
                        'player_name': injury['player_name'],
                        'player_id': player_id,
                        'position': injury.get('position', ''),
                        'injury_status': injury['injury_status'],
                        'injury_detail': injury['injury_detail'],
                        'designation': injury['designation'],
                        'impact_score': calculate_impact_score(injury['designation']),
                        'source': injury['source'],
                        'data_hash': data_hash,
                        'first_seen': datetime.now(),
                        'last_updated': datetime.now(),
                        'is_active': True,
                        'confidence_score': confidence,
                        'notes': f"Fixed scraper - {injury['source']}"
                    }
                    
                    conn.execute(text("""
                        INSERT INTO nfl_injuries_tracking (
                            date, team, player_name, player_id, position,
                            injury_status, injury_detail, designation, impact_score,
                            source, data_hash, first_seen, last_updated,
                            is_active, confidence_score, notes
                        ) VALUES (
                            :date, :team, :player_name, :player_id, :position,
                            :injury_status, :injury_detail, :designation, :impact_score,
                            :source, :data_hash, :first_seen, :last_updated,
                            :is_active, :confidence_score, :notes
                        )
                    """), insert_data)
                    
                    inserted += 1
                    if player_id:
                        mapped += 1
                        
                    status_icon = "🎯" if confidence > 0.9 else "✅" if confidence > 0.8 else "⚠️"
                    print(f"{status_icon} {injury['player_name']} ({final_team}) - {injury['designation']} | Conf: {confidence:.2f}")
            
            print(f"\n📊 INSERTION SUMMARY:")
            print(f"  ✅ Total inserted: {inserted}")
            print(f"  🎯 Successfully mapped: {mapped} ({mapped/inserted*100 if inserted > 0 else 0:.1f}%)")
            
            return inserted
            
    except Exception as e:
        print(f"❌ Insertion failed: {e}")
        import traceback
        traceback.print_exc()
        return 0

def main():
    """Fixed main execution"""
    print("🏈 FIXED INJURY DATA SYSTEM")
    print("=" * 60)
    print("Adapted for your database schema with working scrapers")
    
    try:
        # Step 1: Load player mapping (adapted to your schema)
        print("\n" + "="*60)
        print("STEP 1: LOADING ADAPTIVE PLAYER MAPPING")
        print("="*60)
        player_mapping = load_adaptive_player_mapping()
        
        # Step 2: Scrape with fixed scrapers
        print("\n" + "="*60)
        print("STEP 2: SCRAPING WITH FIXED METHODS")
        print("="*60)
        
        all_injuries = []
        
        # ESPN with fixed scraper
        espn_injuries = scrape_espn_fixed()
        all_injuries.extend(espn_injuries)
        
        # NFL.com with API fallback
        nfl_injuries = scrape_nfl_com_api()
        all_injuries.extend(nfl_injuries)
        
        print(f"📊 Total scraped: {len(all_injuries)} injuries")
        
        # Step 3: Insert with improved mapping
        print("\n" + "="*60)
        print("STEP 3: INSERTING WITH IMPROVED MAPPING")
        print("="*60)
        
        if all_injuries:
            inserted = insert_fixed_injuries(all_injuries, player_mapping)
        else:
            print("⚠️ No new injuries found to insert")
            inserted = 0
        
        # Step 4: Final validation
        print("\n" + "="*60)
        print("STEP 4: VALIDATION")
        print("="*60)
        
        with engine.connect() as conn:
            total = conn.execute(text("SELECT COUNT(*) FROM nfl_injuries_tracking WHERE is_active = 1")).scalar()
            mapped = conn.execute(text("SELECT COUNT(*) FROM nfl_injuries_tracking WHERE is_active = 1 AND player_id IS NOT NULL")).scalar()
            high_impact = conn.execute(text("SELECT COUNT(*) FROM nfl_injuries_tracking WHERE is_active = 1 AND impact_score >= 2")).scalar()
            recent = conn.execute(text("SELECT COUNT(*) FROM nfl_injuries_tracking WHERE date >= date('now', '-1 day')")).scalar()
            
            print(f"📊 SYSTEM STATUS:")
            print(f"  Total active injuries: {total}")
            print(f"  Mapped to players: {mapped} ({mapped/total*100 if total > 0 else 0:.1f}%)")
            print(f"  High impact injuries: {high_impact}")
            print(f"  Fresh data (24h): {recent}")
            print(f"  New injuries added: {inserted}")
        
        if mapped/total > 0.5 if total > 0 else False:
            print(f"\n🎉 SUCCESS! Injury mapping significantly improved!")
        else:
            print(f"\n⚠️ Mapping still needs work. Consider running again or checking player data quality.")
        
        print(f"\n🤖 AI Bot Status: {'✅ Ready' if high_impact > 0 else '⚠️ Limited data'}")
        
    except Exception as e:
        print(f"❌ System failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()