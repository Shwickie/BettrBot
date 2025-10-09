#!/usr/bin/env python3
"""
Fix Cloud Injury Data - Normalize positions and designations
RUN THIS ONCE to fix your cloud database
"""

from sqlalchemy import create_engine, text
import pandas as pd

DATABASE_URL = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"

def infer_position_from_name(player_name):
    """Infer position from player name patterns (last resort)"""
    if not player_name:
        return None
    
    name_lower = player_name.lower()
    
    # Common position indicators in player names/context
    position_hints = {
        'CB': ['corner', 'cornerback'],
        'LB': ['linebacker', 'backer'],
        'S': ['safety'],
        'DT': ['tackle', 'nose'],
        'DE': ['defensive end', 'edge'],
        'OL': ['guard', 'center', 'tackle'],
        'G': ['guard'],
        'C': ['center'],
        'T': ['tackle'],
    }
    
    for pos, keywords in position_hints.items():
        for keyword in keywords:
            if keyword in name_lower:
                return pos
    
    return None

def normalize_designations():
    """Normalize injury designations to standard format"""
    print("🔧 NORMALIZING INJURY DESIGNATIONS")
    print("=" * 50)
    
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    
    with engine.begin() as conn:
        # Map full names to abbreviations
        updates = [
            ("Injured Reserve", "IR"),
            ("injured reserve", "IR"),
            ("Out", "OUT"),
            ("out", "OUT"),
            ("Doubtful", "DOUBTFUL"),
            ("doubtful", "DOUBTFUL"),
            ("Questionable", "QUESTIONABLE"),
            ("questionable", "QUESTIONABLE"),
            ("PUP", "IR"),  # Treat PUP as IR
            ("NFI", "IR"),  # Non-Football Injury -> IR
        ]
        
        for old_val, new_val in updates:
            result = conn.execute(text("""
                UPDATE nfl_injuries_tracking
                SET designation = :new_val
                WHERE designation = :old_val
                AND is_active = true
            """), {"old_val": old_val, "new_val": new_val})
            
            if result.rowcount > 0:
                print(f"  ✅ Updated {result.rowcount} '{old_val}' -> '{new_val}'")
        
        # Verify
        counts = pd.read_sql(text("""
            SELECT designation, COUNT(*) as count
            FROM nfl_injuries_tracking
            WHERE is_active = true
            GROUP BY designation
            ORDER BY count DESC
        """), conn)
        
        print(f"\n📊 Current designation distribution:")
        print(counts.to_string(index=False))

def fix_missing_positions():
    """Fill missing positions by looking up player data"""
    print("\n🔧 FIXING MISSING POSITIONS")
    print("=" * 50)
    
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    
    with engine.begin() as conn:
        # Get injuries with missing positions
        missing = pd.read_sql(text("""
            SELECT id, player_name, player_id, team
            FROM nfl_injuries_tracking
            WHERE is_active = true
            AND (position IS NULL OR position = '')
        """), conn)
        
        print(f"Found {len(missing)} injuries with missing positions")
        
        if missing.empty:
            print("✅ All injuries have positions!")
            return
        
        fixed = 0
        
        for _, injury in missing.iterrows():
            player_id = injury['player_id']
            player_name = injury['player_name']
            
            # Strategy 1: Look up by player_id (try both position columns)
            if player_id:
                result = conn.execute(text("""
                    SELECT COALESCE(position_x, position_y) as pos
                    FROM player_game_stats
                    WHERE player_id = :pid
                    AND (position_x IS NOT NULL OR position_y IS NOT NULL)
                    LIMIT 1
                """), {"pid": player_id})
                
                row = result.fetchone()
                if row and row[0]:
                    conn.execute(text("""
                        UPDATE nfl_injuries_tracking
                        SET position = :pos
                        WHERE id = :id
                    """), {"pos": row[0], "id": injury['id']})
                    fixed += 1
                    print(f"  ✅ {player_name}: {row[0]} (via player_id)")
                    continue
            
            # Strategy 2: Look up by name (try both position columns)
            if player_name:
                result = conn.execute(text("""
                    SELECT COALESCE(position_x, position_y) as pos
                    FROM player_game_stats
                    WHERE LOWER(player_display_name) = LOWER(:name)
                    AND (position_x IS NOT NULL OR position_y IS NOT NULL)
                    LIMIT 1
                """), {"name": player_name})
                
                row = result.fetchone()
                if row and row[0]:
                    conn.execute(text("""
                        UPDATE nfl_injuries_tracking
                        SET position = :pos
                        WHERE id = :id
                    """), {"pos": row[0], "id": injury['id']})
                    fixed += 1
                    print(f"  ✅ {player_name}: {row[0]} (via name)")
                    continue
            
            # Strategy 3: Try current_nfl_players table
            if player_name:
                result = conn.execute(text("""
                    SELECT position
                    FROM current_nfl_players
                    WHERE LOWER(player_display_name) = LOWER(:name)
                    AND position IS NOT NULL
                    LIMIT 1
                """), {"name": player_name})
                
                row = result.fetchone()
                if row and row[0]:
                    conn.execute(text("""
                        UPDATE nfl_injuries_tracking
                        SET position = :pos
                        WHERE id = :id
                    """), {"pos": row[0], "id": injury['id']})
                    fixed += 1
                    print(f"  ✅ {player_name}: {row[0]} (via current_nfl_players)")
                    continue
            
            # Strategy 4: Infer position from common NFL names/roles
            inferred_pos = infer_position_from_name(player_name)
            if inferred_pos:
                conn.execute(text("""
                    UPDATE nfl_injuries_tracking
                    SET position = :pos
                    WHERE id = :id
                """), {"pos": inferred_pos, "id": injury['id']})
                fixed += 1
                print(f"  🔍 {player_name}: {inferred_pos} (inferred)")
                continue
            
            # Strategy 3: Default to 'UNKNOWN' for now
            conn.execute(text("""
                UPDATE nfl_injuries_tracking
                SET position = 'UNK'
                WHERE id = :id
            """), {"id": injury['id']})
        
        print(f"\n📊 Fixed {fixed} positions, {len(missing) - fixed} remain unknown")

def recalculate_impact_scores():
    """Recalculate team impact scores with fixed data"""
    print("\n🔧 RECALCULATING IMPACT SCORES")
    print("=" * 50)
    
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    
    POS_WEIGHTS = {
        'QB': 3.0, 'RB': 2.0, 'WR': 2.0, 'TE': 1.5,
        'CB': 1.3, 'S': 1.2, 'LB': 1.1, 'EDGE': 1.2,
        'DT': 1.0, 'DE': 1.1, 'OL': 1.0, 'G': 1.0, 'C': 1.0, 'T': 1.0
    }
    
    DESIG_WEIGHTS = {
        'IR': 3, 'OUT': 3, 'DOUBTFUL': 2, 'QUESTIONABLE': 1
    }
    
    with engine.begin() as conn:
        # Get all active injuries
        injuries = pd.read_sql(text("""
            SELECT team, position, designation
            FROM nfl_injuries_tracking
            WHERE is_active = true
            AND team IS NOT NULL
            AND team != 'UNK'
            AND designation IN ('IR', 'OUT', 'DOUBTFUL', 'QUESTIONABLE')
        """), conn)
        
        if injuries.empty:
            print("No valid injuries to calculate")
            return
        
        # Calculate impact by team
        impact_by_team = {}
        
        for _, inj in injuries.iterrows():
            team = inj['team']
            pos = inj.get('position', 'UNK')
            desig = inj['designation']
            
            pos_weight = POS_WEIGHTS.get(pos, 1.0)
            desig_weight = DESIG_WEIGHTS.get(desig, 1)
            impact = pos_weight * desig_weight
            
            if team not in impact_by_team:
                impact_by_team[team] = 0
            impact_by_team[team] += impact
        
        # Update validation table
        conn.execute(text("DELETE FROM ai_injury_validation_detail"))
        
        for team, impact in impact_by_team.items():
            conn.execute(text("""
                INSERT INTO ai_injury_validation_detail 
                (team_ai, inj_name, position, designation)
                VALUES (:team, 'TEAM_TOTAL', 'ALL', :impact)
            """), {'team': team, 'impact': str(round(impact, 2))})
        
        print(f"✅ Calculated impact for {len(impact_by_team)} teams")
        
        # Show top teams
        sorted_teams = sorted(impact_by_team.items(), key=lambda x: x[1], reverse=True)[:5]
        print("\nTop 5 teams by injury impact:")
        for team, impact in sorted_teams:
            print(f"  {team}: {impact:.2f}")

def verify_fixes():
    """Verify all fixes worked"""
    print("\n📊 VERIFICATION")
    print("=" * 50)
    
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    
    with engine.connect() as conn:
        # Check positions
        pos_check = pd.read_sql(text("""
            SELECT 
                COUNT(*) as total,
                COUNT(position) as with_position,
                COUNT(CASE WHEN position IS NULL OR position = '' THEN 1 END) as missing_position
            FROM nfl_injuries_tracking
            WHERE is_active = true
        """), conn).iloc[0]
        
        print(f"📍 Positions:")
        print(f"  Total injuries: {pos_check['total']}")
        print(f"  With position: {pos_check['with_position']} ({pos_check['with_position']/pos_check['total']*100:.1f}%)")
        print(f"  Missing position: {pos_check['missing_position']}")
        
        # Check designations
        desig_check = pd.read_sql(text("""
            SELECT designation, COUNT(*) as count
            FROM nfl_injuries_tracking
            WHERE is_active = true
            GROUP BY designation
            ORDER BY count DESC
        """), conn)
        
        print(f"\n🏷️ Designations:")
        print(desig_check.to_string(index=False))
        
        # Sample injuries with full data
        sample = pd.read_sql(text("""
            SELECT player_name, team, position, designation
            FROM nfl_injuries_tracking
            WHERE is_active = true
            AND position IS NOT NULL
            AND designation IN ('IR', 'OUT', 'DOUBTFUL', 'QUESTIONABLE')
            ORDER BY 
                CASE designation 
                    WHEN 'OUT' THEN 1 
                    WHEN 'IR' THEN 2 
                    WHEN 'DOUBTFUL' THEN 3 
                    ELSE 4 
                END,
                CASE position
                    WHEN 'QB' THEN 1
                    WHEN 'RB' THEN 2
                    WHEN 'WR' THEN 3
                    ELSE 4
                END
            LIMIT 10
        """), conn)
        
        print(f"\n📋 Sample Fixed Injuries:")
        print(sample.to_string(index=False))

def main():
    """Run all fixes"""
    print("🏥 FIXING CLOUD INJURY DATA")
    print("=" * 60)
    
    try:
        # Step 1: Normalize designations
        normalize_designations()
        
        # Step 2: Fix positions
        fix_missing_positions()
        
        # Step 3: Recalculate impacts
        recalculate_impact_scores()
        
        # Step 4: Verify
        verify_fixes()
        
        print("\n✅ ALL FIXES COMPLETE!")
        print("Your AI chat injury system should now work correctly.")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()