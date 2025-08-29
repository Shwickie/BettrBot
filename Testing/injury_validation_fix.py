#!/usr/bin/env python3
"""
Clean Injury Data Validation - WITHOUT hardcoded players
Test the actual injury data quality and validate real injuries
"""

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# Config
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

def remove_hardcoded_additions():
    """Remove any manually added players that might not be real injuries"""
    print("🧹 REMOVING HARDCODED ADDITIONS")
    print("=" * 35)
    
    try:
        with engine.begin() as conn:
            # Remove entries that were manually added
            removal_result = conn.execute(text("""
                DELETE FROM nfl_injuries_tracking 
                WHERE source LIKE '%MANUAL_ADDITION%'
                OR notes LIKE '%hardcoded%'
                OR notes LIKE '%manual%'
            """))
            
            removed_count = removal_result.rowcount
            print(f"🗑️ Removed {removed_count} manually added entries")
            
            return removed_count
            
    except Exception as e:
        print(f"❌ Removal failed: {e}")
        return 0

def validate_injury_sources():
    """Validate the sources of injury data to ensure legitimacy"""
    print("\n🔍 VALIDATING INJURY DATA SOURCES")
    print("=" * 40)
    
    try:
        with engine.connect() as conn:
            # Get breakdown by source
            source_stats = pd.read_sql(text("""
                SELECT 
                    source,
                    COUNT(*) as count,
                    COUNT(CASE WHEN is_active = 1 THEN 1 END) as active,
                    MIN(date) as first_seen,
                    MAX(date) as last_seen
                FROM nfl_injuries_tracking 
                GROUP BY source
                ORDER BY count DESC
            """), conn)
            
            print("📊 INJURY DATA SOURCES:")
            print(f"{'Source':<25} {'Total':<8} {'Active':<8} {'First Seen':<12} {'Last Seen'}")
            print("-" * 70)
            
            for _, row in source_stats.iterrows():
                print(f"{row['source']:<25} {row['count']:<8} {row['active']:<8} {row['first_seen']:<12} {row['last_seen']}")
            
            # Check for suspicious patterns
            suspicious_sources = source_stats[
                (source_stats['source'].str.contains('MANUAL', case=False, na=False)) |
                (source_stats['source'].str.contains('TEST', case=False, na=False)) |
                (source_stats['source'].str.contains('HARDCODE', case=False, na=False))
            ]
            
            if not suspicious_sources.empty:
                print(f"\n⚠️ SUSPICIOUS SOURCES FOUND:")
                for _, row in suspicious_sources.iterrows():
                    print(f"  {row['source']}: {row['active']} active injuries")
            else:
                print(f"\n✅ No suspicious manual additions found")
            
            return source_stats
            
    except Exception as e:
        print(f"❌ Source validation failed: {e}")
        return pd.DataFrame()

def check_recent_injury_dates():
    """Check if injury dates make sense for current date"""
    print("\n📅 VALIDATING INJURY DATES")
    print("=" * 30)
    
    try:
        with engine.connect() as conn:
            current_date = datetime.now().date()
            
            # Check date distribution
            date_stats = pd.read_sql(text("""
                SELECT 
                    date,
                    COUNT(*) as injuries,
                    COUNT(CASE WHEN is_active = 1 THEN 1 END) as active
                FROM nfl_injuries_tracking 
                WHERE date >= date('now', '-30 days')
                GROUP BY date
                ORDER BY date DESC
                LIMIT 10
            """), conn)
            
            print("📊 RECENT INJURY DATES:")
            print(f"{'Date':<12} {'Total':<8} {'Active'}")
            print("-" * 30)
            
            for _, row in date_stats.iterrows():
                print(f"{row['date']:<12} {row['injuries']:<8} {row['active']}")
            
            # Check for future dates (suspicious)
            future_injuries = pd.read_sql(text("""
                SELECT COUNT(*) as count
                FROM nfl_injuries_tracking 
                WHERE date > date('now')
                AND is_active = 1
            """), conn).iloc[0]['count']
            
            if future_injuries > 0:
                print(f"\n⚠️ WARNING: {future_injuries} injuries dated in the future!")
            else:
                print(f"\n✅ All injury dates are reasonable")
            
            return date_stats
            
    except Exception as e:
        print(f"❌ Date validation failed: {e}")
        return pd.DataFrame()

def validate_player_team_consistency():
    """Check if players are assigned to correct teams"""
    print("\n🏈 VALIDATING PLAYER-TEAM CONSISTENCY")
    print("=" * 45)
    
    try:
        with engine.connect() as conn:
            # Check for team mismatches
            team_mismatches = pd.read_sql(text("""
                SELECT 
                    i.player_name,
                    i.team as injury_team,
                    p.team as roster_team,
                    i.player_id,
                    i.confidence_score
                FROM nfl_injuries_tracking i
                JOIN enhanced_nfl_players p ON i.player_id = p.player_id
                WHERE i.is_active = 1 
                AND i.team != p.team
                AND i.player_id IS NOT NULL
                ORDER BY i.confidence_score DESC
                LIMIT 20
            """), conn)
            
            if not team_mismatches.empty:
                print(f"⚠️ TEAM MISMATCHES FOUND ({len(team_mismatches)}):")
                print(f"{'Player':<20} {'Injury Team':<6} {'Roster Team':<6} {'Confidence'}")
                print("-" * 50)
                
                for _, row in team_mismatches.iterrows():
                    print(f"{row['player_name']:<20} {row['injury_team']:<10} {row['roster_team']:<10} {row['confidence_score']:.2f}")
            else:
                print("✅ No significant team mismatches found")
            
            return team_mismatches
            
    except Exception as e:
        print(f"❌ Team validation failed: {e}")
        return pd.DataFrame()

def check_duplicate_injuries():
    """Check for duplicate injury entries"""
    print("\n🔍 CHECKING FOR DUPLICATE INJURIES")
    print("=" * 40)
    
    try:
        with engine.connect() as conn:
            # Find potential duplicates
            duplicates = pd.read_sql(text("""
                SELECT 
                    player_name,
                    team,
                    designation,
                    COUNT(*) as duplicate_count
                FROM nfl_injuries_tracking 
                WHERE is_active = 1
                GROUP BY player_name, team, designation
                HAVING COUNT(*) > 1
                ORDER BY duplicate_count DESC
                LIMIT 20
            """), conn)
            
            if not duplicates.empty:
                print(f"⚠️ DUPLICATE INJURIES FOUND ({len(duplicates)}):")
                print(f"{'Player':<20} {'Team':<6} {'Status':<15} {'Count'}")
                print("-" * 50)
                
                for _, row in duplicates.iterrows():
                    print(f"{row['player_name']:<20} {row['team']:<6} {row['designation']:<15} {row['duplicate_count']}")
                    
                # Show total duplicate entries
                total_duplicates = duplicates['duplicate_count'].sum() - len(duplicates)
                print(f"\n📊 Total duplicate entries: {total_duplicates}")
            else:
                print("✅ No duplicate injuries found")
            
            return duplicates
            
    except Exception as e:
        print(f"❌ Duplicate check failed: {e}")
        return pd.DataFrame()

def verify_star_players():
    """Verify injuries for well-known star players"""
    print("\n⭐ VERIFYING STAR PLAYER INJURIES")
    print("=" * 40)
    
    # Well-known players who should be verifiable if injured
    star_players = [
        'Tyreek Hill', 'Jaylen Waddle', 'Chris Godwin', 'Nick Chubb',
        'DeForest Buckner', 'Charles Cross', 'Tank Dell', 'Hollywood Brown',
        'Stefon Diggs', 'Matthew Stafford', 'Deshaun Watson', 'Jordan Love'
    ]
    
    try:
        with engine.connect() as conn:
            verified_injuries = []
            questionable_injuries = []
            
            for player in star_players:
                result = pd.read_sql(text("""
                    SELECT 
                        i.player_name,
                        i.team,
                        i.designation,
                        i.injury_detail,
                        i.confidence_score,
                        i.source,
                        i.date
                    FROM nfl_injuries_tracking i
                    WHERE LOWER(i.player_name) LIKE :player_pattern
                    AND i.is_active = 1
                    ORDER BY i.confidence_score DESC
                    LIMIT 1
                """), conn, params={'player_pattern': f'%{player.lower()}%'})
                
                if not result.empty:
                    injury = result.iloc[0]
                    
                    if injury['confidence_score'] >= 0.95:
                        verified_injuries.append(injury)
                        print(f"✅ {injury['player_name']} ({injury['team']}): {injury['designation']} - High confidence")
                    else:
                        questionable_injuries.append(injury)
                        print(f"⚠️ {injury['player_name']} ({injury['team']}): {injury['designation']} - Low confidence ({injury['confidence_score']:.2f})")
                else:
                    print(f"⚪ {player}: No injury found (healthy or not in system)")
            
            print(f"\n📊 STAR PLAYER SUMMARY:")
            print(f"  ✅ High confidence injuries: {len(verified_injuries)}")
            print(f"  ⚠️ Questionable injuries: {len(questionable_injuries)}")
            print(f"  ⚪ No injuries found: {len(star_players) - len(verified_injuries) - len(questionable_injuries)}")
            
            return verified_injuries, questionable_injuries
            
    except Exception as e:
        print(f"❌ Star player verification failed: {e}")
        return [], []

def check_morice_norris_specifically():
    """Specifically check for Morice/Maurice Norris situation"""
    print("\n🔍 CHECKING MORICE/MAURICE NORRIS SITUATION")
    print("=" * 50)
    
    try:
        with engine.connect() as conn:
            # Look for all Norris variations
            norris_results = pd.read_sql(text("""
                SELECT 
                    i.player_name,
                    i.team,
                    i.designation,
                    i.player_id,
                    i.confidence_score,
                    i.source,
                    i.date,
                    i.injury_detail
                FROM nfl_injuries_tracking i
                WHERE LOWER(i.player_name) LIKE '%norris%'
                AND i.is_active = 1
                ORDER BY i.player_name, i.team
            """), conn)
            
            if not norris_results.empty:
                print(f"Found {len(norris_results)} Norris entries:")
                print(f"{'Name':<20} {'Team':<6} {'Status':<15} {'Player ID':<15} {'Conf':<6} {'Source'}")
                print("-" * 80)
                
                for _, row in norris_results.iterrows():
                    player_id_short = (row['player_id'][:12] + "...") if row['player_id'] and len(row['player_id']) > 15 else (row['player_id'] or "None")
                    conf_str = f"{row['confidence_score']:.2f}" if row['confidence_score'] else "N/A"
                    print(f"{row['player_name']:<20} {row['team']:<6} {row['designation']:<15} {player_id_short:<15} {conf_str:<6} {row['source']}")
                
                # Check if these players exist in roster
                roster_check = pd.read_sql(text("""
                    SELECT player_name, team, position, player_id
                    FROM enhanced_nfl_players 
                    WHERE LOWER(player_name) LIKE '%norris%'
                    AND is_active = 1
                """), conn)
                
                print(f"\n🏈 ROSTER CHECK:")
                if not roster_check.empty:
                    for _, row in roster_check.iterrows():
                        print(f"  ✅ {row['player_name']} ({row['team']}) - {row['position']} - ID: {row['player_id']}")
                else:
                    print("  ❌ No Norris players found in roster")
                    
            else:
                print("❌ No Norris entries found in injury data")
            
            return norris_results
            
    except Exception as e:
        print(f"❌ Norris check failed: {e}")
        return pd.DataFrame()

def final_data_quality_assessment():
    """Final assessment of data quality"""
    print("\n📊 FINAL DATA QUALITY ASSESSMENT")
    print("=" * 40)
    
    try:
        with engine.connect() as conn:
            # Get comprehensive stats
            quality_stats = pd.read_sql(text("""
                SELECT 
                    COUNT(*) as total_active,
                    COUNT(player_id) as mapped,
                    COUNT(CASE WHEN confidence_score >= 0.95 THEN 1 END) as very_high_conf,
                    COUNT(CASE WHEN confidence_score >= 0.85 THEN 1 END) as high_conf,
                    COUNT(CASE WHEN confidence_score >= 0.75 THEN 1 END) as medium_conf,
                    COUNT(CASE WHEN confidence_score < 0.75 AND player_id IS NOT NULL THEN 1 END) as low_conf,
                    COUNT(CASE WHEN player_id IS NULL THEN 1 END) as unmapped,
                    COUNT(DISTINCT team) as teams_affected,
                    COUNT(CASE WHEN date >= date('now', '-7 days') THEN 1 END) as recent_injuries
                FROM nfl_injuries_tracking 
                WHERE is_active = 1
            """), conn).iloc[0]
            
            total = quality_stats['total_active']
            mapped = quality_stats['mapped']
            mapping_rate = (mapped / total * 100) if total > 0 else 0
            
            print(f"📈 DATA QUALITY METRICS:")
            print(f"  Total Active Injuries: {total}")
            print(f"  Mapping Rate: {mapping_rate:.1f}% ({mapped}/{total})")
            print(f"  Very High Confidence (≥95%): {quality_stats['very_high_conf']}")
            print(f"  High Confidence (≥85%): {quality_stats['high_conf']}")
            print(f"  Medium Confidence (≥75%): {quality_stats['medium_conf']}")
            print(f"  Low Confidence (<75%): {quality_stats['low_conf']}")
            print(f"  Unmapped: {quality_stats['unmapped']}")
            print(f"  Teams Affected: {quality_stats['teams_affected']}")
            print(f"  Recent Injuries (7 days): {quality_stats['recent_injuries']}")
            
            # Data quality grade
            if mapping_rate >= 90 and quality_stats['very_high_conf'] / total >= 0.7:
                grade = "A+ EXCELLENT"
                color = "🟢"
            elif mapping_rate >= 85 and quality_stats['high_conf'] / total >= 0.6:
                grade = "A VERY GOOD"
                color = "🟢"
            elif mapping_rate >= 80:
                grade = "B GOOD"
                color = "🟡"
            elif mapping_rate >= 70:
                grade = "C ACCEPTABLE"
                color = "🟡"
            else:
                grade = "D NEEDS WORK"
                color = "🔴"
            
            print(f"\n{color} DATA QUALITY GRADE: {grade}")
            
            return quality_stats, grade
            
    except Exception as e:
        print(f"❌ Quality assessment failed: {e}")
        return None, "UNKNOWN"

def main():
    """Main validation without hardcoded additions"""
    print("🔍 CLEAN INJURY DATA VALIDATION - NO HARDCODED PLAYERS")
    print("=" * 65)
    print("Testing your actual injury data quality and legitimacy")
    
    try:
        # Step 1: Remove any hardcoded additions
        removed = remove_hardcoded_additions()
        
        # Step 2: Validate data sources
        sources = validate_injury_sources()
        
        # Step 3: Check injury dates
        dates = check_recent_injury_dates()
        
        # Step 4: Validate team consistency
        team_issues = validate_player_team_consistency()
        
        # Step 5: Check for duplicates
        duplicates = check_duplicate_injuries()
        
        # Step 6: Verify star players
        verified, questionable = verify_star_players()
        
        # Step 7: Check Morice Norris specifically
        norris_data = check_morice_norris_specifically()
        
        # Step 8: Final quality assessment
        quality_stats, grade = final_data_quality_assessment()
        
        print(f"\n🏆 CLEAN VALIDATION RESULTS:")
        print(f"  🗑️ Hardcoded entries removed: {removed}")
        print(f"  📊 Data sources validated: {len(sources) if not sources.empty else 0}")
        print(f"  ⚠️ Team mismatches: {len(team_issues) if not team_issues.empty else 0}")
        print(f"  🔄 Duplicate issues: {len(duplicates) if not duplicates.empty else 0}")
        print(f"  ⭐ Star players verified: {len(verified)}")
        print(f"  ❓ Questionable star injuries: {len(questionable)}")
        print(f"  🔍 Norris entries found: {len(norris_data) if not norris_data.empty else 0}")
        print(f"  📈 Overall grade: {grade}")
        
        print(f"\n🎯 RECOMMENDATIONS:")
        if len(team_issues) > 5:
            print(f"  ⚠️ Fix team assignment issues")
        if len(duplicates) > 0:
            print(f"  🔄 Remove duplicate entries")
        if len(questionable) > len(verified):
            print(f"  📋 Verify questionable star player injuries manually")
        if grade.startswith("A"):
            print(f"  ✅ Your injury data is high quality!")
            print(f"  🤖 Ready for production betting bot use!")
        else:
            print(f"  📈 Consider additional data validation")
        
        print(f"\n🚀 NEXT STEPS:")
        print(f"  1. Run: python injury_data_validation.py")
        print(f"  2. Manually verify questionable high-profile injuries")
        print(f"  3. Consider removing low-confidence entries")
        print(f"  4. Test with actual betting scenarios")
        
    except Exception as e:
        print(f"❌ Clean validation failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()