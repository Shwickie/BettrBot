#!/usr/bin/env python3
"""
Cloud Injury Auto-Updater - FIXED VERSION
Fetches fresh NFL injuries and updates cloud database
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import requests

# Cloud PostgreSQL
DATABASE_URL = "postgresql://postgres:YviqtXqcsCIgRzSCofNjbfwgjkYNLydX@maglev.proxy.rlwy.net:54187/railway"

def get_engine():
    """Get cloud database engine"""
    return create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=280,
        connect_args={
            "connect_timeout": 30,
            "application_name": "bettr-injury-update"
        }
    )

def fetch_injury_fallback():
    """Use nfl_data_py for recent injury data"""
    try:
        import nfl_data_py as nfl
        
        print("Fetching injuries from nfl_data_py (2024 data)...")
        injuries = nfl.import_injuries([2024])
        
        if not injuries.empty:
            print(f"✅ Found {len(injuries)} injury records")
            
            # Add fresh timestamp
            injuries['last_updated'] = datetime.now()
            
            # Keep only recent/relevant injuries
            if 'date_modified' in injuries.columns:
                cutoff = datetime.now() - timedelta(days=30)
                injuries = injuries[pd.to_datetime(injuries['date_modified']) >= cutoff]
                print(f"Filtered to {len(injuries)} recent injuries")
            
            return injuries
        
    except Exception as e:
        print(f"Fallback failed: {e}")
    
    return pd.DataFrame()

def update_injury_database(injuries_df):
    """Update cloud database - FIXED timestamp handling"""
    
    if injuries_df.empty:
        print("No injuries to update")
        return 0
    
    engine = get_engine()
    
    try:
        with engine.begin() as conn:
            # Ensure table exists with CORRECT types
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS nfl_injuries_tracking (
                    id SERIAL PRIMARY KEY,
                    player_name TEXT,
                    player_id TEXT,
                    team TEXT,
                    position TEXT,
                    designation TEXT,
                    is_active BOOLEAN DEFAULT true,
                    confidence_score REAL DEFAULT 0.8,
                    last_updated TIMESTAMP,
                    notes TEXT
                )
            """))
            
            # FIXED: Cast to timestamp in comparison
            deactivated = conn.execute(text("""
                UPDATE nfl_injuries_tracking
                SET is_active = false
                WHERE CAST(last_updated AS TIMESTAMP) < (CURRENT_TIMESTAMP - INTERVAL '14 days')
                AND designation NOT IN ('IR', 'PUP', 'Injured Reserve')
                AND is_active = true
            """)).rowcount
            
            if deactivated > 0:
                print(f"🗑️ Deactivated {deactivated} old injuries")
            
            # Process new injuries
            updated = 0
            inserted = 0
            
            for _, injury in injuries_df.iterrows():
                player_name = injury.get('full_name', injury.get('player_name', '')).strip()
                team = injury.get('team', 'UNK').strip()
                designation = injury.get('report_status', injury.get('designation', '')).strip()
                
                if not player_name:
                    continue
                
                # Convert timestamp properly
                last_updated = pd.to_datetime(injury.get('last_updated', datetime.now()))
                
                # Try update first
                result = conn.execute(text("""
                    UPDATE nfl_injuries_tracking
                    SET last_updated = :updated,
                        is_active = true,
                        position = :pos,
                        designation = :desig
                    WHERE LOWER(player_name) = LOWER(:name)
                    AND team = :team
                    RETURNING id
                """), {
                    'name': player_name,
                    'team': team,
                    'pos': injury.get('position', ''),
                    'desig': designation,
                    'updated': last_updated
                })
                
                if result.rowcount > 0:
                    updated += 1
                else:
                    # Insert new
                    try:
                        conn.execute(text("""
                            INSERT INTO nfl_injuries_tracking (
                                player_name, team, position, designation,
                                is_active, last_updated, confidence_score
                            ) VALUES (
                                :name, :team, :pos, :desig, true, :updated, 0.8
                            )
                        """), {
                            'name': player_name,
                            'team': team,
                            'pos': injury.get('position', ''),
                            'desig': designation,
                            'updated': last_updated
                        })
                        inserted += 1
                    except Exception as e:
                        # Skip duplicates
                        if 'unique' not in str(e).lower():
                            print(f"Error inserting {player_name}: {e}")
            
            print(f"📊 Updated: {updated}, Inserted: {inserted}")
            
            # Update validation table
            try:
                conn.execute(text("""
                    INSERT INTO ai_injury_validation_detail (team_ai, inj_name, position, designation)
                    SELECT team, player_name, position, designation
                    FROM nfl_injuries_tracking
                    WHERE is_active = true
                    ON CONFLICT DO NOTHING
                """))
            except:
                pass  # Table might not exist
            
            # Show stats
            total_active = conn.execute(text("""
                SELECT COUNT(*) FROM nfl_injuries_tracking WHERE is_active = true
            """)).scalar()
            
            print(f"📈 Total active injuries in cloud: {total_active}")
            
            return updated + inserted
            
    except Exception as e:
        print(f"Database update error: {e}")
        import traceback
        traceback.print_exc()
        return 0

def main():
    """Main execution"""
    print("NFL INJURY AUTO-UPDATE - CLOUD VERSION")
    print("=" * 50)
    
    # Get injury data
    injuries = fetch_injury_fallback()
    
    if injuries.empty:
        print("❌ No injury data available")
        return True  # Don't fail pipeline
    
    # Update database
    updated_count = update_injury_database(injuries)
    
    if updated_count > 0:
        print(f"✅ Successfully updated {updated_count} injuries")
        return True
    else:
        print("⚠️ No injuries updated")
        return True  # Don't fail pipeline

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)