#!/usr/bin/env python3
"""
Injury Data Validation and Bot Integration Helper
Helps validate injury data mapping and provides utilities for the betting bot
"""

import pandas as pd
from sqlalchemy import create_engine, text
import json
from datetime import datetime, timedelta

# Config
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

def validate_injury_mappings():
    """Validate and review injury-to-player mappings"""
    print("🔍 VALIDATING INJURY MAPPINGS")
    print("=" * 50)
    
    try:
        with engine.connect() as conn:
            # Get mapping quality stats
            mapping_query = text("""
                SELECT 
                    player_name,
                    team,
                    position,
                    designation,
                    injury_detail,
                    player_id,
                    confidence_score,
                    CASE 
                        WHEN confidence_score >= 0.9 THEN 'HIGH'
                        WHEN confidence_score >= 0.75 THEN 'MEDIUM'
                        WHEN confidence_score >= 0.5 THEN 'LOW'
                        ELSE 'VERY_LOW'
                    END as confidence_level,
                    source,
                    last_updated
                FROM nfl_injuries_tracking 
                WHERE is_active = 1 
                AND date >= date('now', '-7 days')
                ORDER BY confidence_score DESC NULLS LAST, impact_score DESC
            """)
            
            mappings_df = pd.read_sql(mapping_query, conn)
            
            if mappings_df.empty:
                print("❌ No active injury records found")
                return False
                
            # Summary statistics
            total_records = len(mappings_df)
            mapped_records = len(mappings_df[mappings_df['player_id'].notna()])
            unmapped_records = total_records - mapped_records
            
            print(f"📊 MAPPING SUMMARY:")
            print(f"  Total Records: {total_records}")
            print(f"  Mapped: {mapped_records} ({mapped_records/total_records*100:.1f}%)")
            print(f"  Unmapped: {unmapped_records} ({unmapped_records/total_records*100:.1f}%)")
            
            # Confidence breakdown
            if mapped_records > 0:
                conf_stats = mappings_df[mappings_df['player_id'].notna()]['confidence_score'].describe()
                print(f"\n📈 CONFIDENCE STATISTICS (Mapped Records):")
                print(f"  Average: {conf_stats['mean']:.3f}")
                print(f"  Min: {conf_stats['min']:.3f}")
                print(f"  Max: {conf_stats['max']:.3f}")
                print(f"  25th percentile: {conf_stats['25%']:.3f}")
                print(f"  75th percentile: {conf_stats['75%']:.3f}")
            
            # Show high confidence mappings
            high_conf = mappings_df[mappings_df['confidence_score'] >= 0.9]
            print(f"\n✅ HIGH CONFIDENCE MAPPINGS ({len(high_conf)} records):")
            for _, row in high_conf.head(10).iterrows():
                print(f"  {row['player_name']:20} ({row['team']}) | {row['designation']:12} | ID: {row['player_id']} | Conf: {row['confidence_score']:.3f}")
            
            # Show uncertain mappings that need review
            uncertain = mappings_df[(mappings_df['confidence_score'] >= 0.5) & (mappings_df['confidence_score'] < 0.75)]
            if not uncertain.empty:
                print(f"\n⚠️ UNCERTAIN MAPPINGS NEEDING REVIEW ({len(uncertain)} records):")
                for _, row in uncertain.head(10).iterrows():
                    print(f"  {row['player_name']:20} ({row['team']}) | {row['designation']:12} | ID: {row['player_id']} | Conf: {row['confidence_score']:.3f}")
            
            # Show unmapped records
            unmapped = mappings_df[mappings_df['player_id'].isna()]
            if not unmapped.empty:
                print(f"\n❌ UNMAPPED PLAYERS ({len(unmapped)} records):")
                for _, row in unmapped.head(15).iterrows():
                    print(f"  {row['player_name']:20} ({row['team']}) | {row['designation']:12} | {row['position']}")
            
            return True
            
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False

def get_injury_data_for_bot(team=None, impact_threshold=1):
    """Get formatted injury data for the betting bot"""
    print(f"🤖 RETRIEVING BOT INJURY DATA")
    if team:
        print(f"   Filtering for team: {team}")
    print(f"   Impact threshold: {impact_threshold}")
    print("=" * 40)
    
    try:
        with engine.connect() as conn:
            # Base query for bot data
            query = """
                SELECT * FROM injury_bot_data
                WHERE impact_score >= :threshold
            """
            
            params = {'threshold': impact_threshold}
            
            if team:
                query += " AND team = :team"
                params['team'] = team
                
            query += " ORDER BY impact_score DESC, confidence_score DESC"
            
            bot_data = pd.read_sql(text(query), conn, params=params)
            
            if bot_data.empty:
                print("❌ No injury data found matching criteria")
                return None
            
            print(f"📋 Found {len(bot_data)} injury records:")
            print(f"{'Player':<20} {'Team':<4} {'Status':<15} {'Risk':<8} {'Fantasy Avg':<11} {'Grade':<10}")
            print("-" * 80)
            
            for _, row in bot_data.iterrows():
                fantasy_avg = f"{row['avg_fantasy_points']:.1f}" if pd.notna(row['avg_fantasy_points']) else "N/A"
                grade = row['madden_grade'] or "N/A"
                print(f"{row['player_name']:<20} {row['team']:<4} {row['availability_status']:<15} {row['risk_level']:<8} {fantasy_avg:<11} {grade:<10}")
            
            return bot_data.to_dict('records')
            
    except Exception as e:
        print(f"❌ Bot data retrieval failed: {e}")
        return None

def check_player_injury_status(player_name_or_id):
    """Check injury status for a specific player"""
    print(f"🔍 CHECKING INJURY STATUS: {player_name_or_id}")
    print("=" * 50)
    
    try:
        with engine.connect() as conn:
            # Try by player_id first, then by name
            query = text("""
                SELECT 
                    player_name,
                    player_id,
                    team,
                    position,
                    designation,
                    injury_detail,
                    availability_status,
                    risk_level,
                    impact_score,
                    confidence_score,
                    injury_date,
                    last_updated
                FROM injury_bot_data
                WHERE player_id = :search_term OR LOWER(player_name) LIKE LOWER(:search_pattern)
                ORDER BY last_updated DESC
            """)
            
            results = pd.read_sql(query, conn, params={
                'search_term': player_name_or_id,
                'search_pattern': f'%{player_name_or_id}%'
            })
            
            if results.empty:
                print(f"❌ No injury data found for '{player_name_or_id}'")
                return None
            
            print(f"✅ Found {len(results)} injury record(s):")
            
            for _, row in results.iterrows():
                print(f"\n📋 INJURY DETAILS:")
                print(f"  Player: {row['player_name']} (ID: {row['player_id']})")
                print(f"  Team: {row['team']} | Position: {row['position']}")
                print(f"  Status: {row['designation']} ({row['availability_status']})")
                print(f"  Injury: {row['injury_detail']}")
                print(f"  Risk Level: {row['risk_level']} (Impact: {row['impact_score']})")
                print(f"  Mapping Confidence: {row['confidence_score']:.2f}" if row['confidence_score'] else "  Mapping Confidence: N/A")
                print(f"  Last Updated: {row['last_updated']}")
                
                # Provide bot-friendly interpretation
                if row['availability_status'] == 'UNAVAILABLE':
                    print(f"  🚨 BOT RECOMMENDATION: AVOID - Player is OUT")
                elif row['availability_status'] == 'VERY_UNLIKELY':
                    print(f"  ⚠️ BOT RECOMMENDATION: HIGH RISK - Player is DOUBTFUL")
                elif row['availability_status'] == 'GAME_TIME_DECISION':
                    print(f"  ❓ BOT RECOMMENDATION: MODERATE RISK - Player is QUESTIONABLE")
                else:
                    print(f"  ✅ BOT RECOMMENDATION: MINIMAL RISK")
            
            return results.to_dict('records')[0]
            
    except Exception as e:
        print(f"❌ Player status check failed: {e}")
        return None

def generate_team_injury_report(team_code):
    """Generate comprehensive injury report for a team"""
    print(f"🏈 TEAM INJURY REPORT: {team_code}")
    print("=" * 50)
    
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT 
                    player_name,
                    position,
                    designation,
                    injury_detail,
                    availability_status,
                    risk_level,
                    impact_score,
                    avg_fantasy_points,
                    madden_grade,
                    confidence_score,
                    last_updated
                FROM injury_bot_data
                WHERE team = :team
                ORDER BY impact_score DESC, 
                         CASE WHEN position IN ('QB', 'RB', 'WR', 'TE') THEN 1 ELSE 2 END,
                         avg_fantasy_points DESC NULLS LAST
            """)
            
            team_injuries = pd.read_sql(query, conn, params={'team': team_code})
            
            if team_injuries.empty:
                print(f"✅ No active injuries found for {team_code}")
                return None
                
            print(f"📊 Found {len(team_injuries)} injured players")
            
            # Group by risk level
            risk_levels = team_injuries.groupby('risk_level').size()
            print(f"\n📈 RISK BREAKDOWN:")
            for risk, count in risk_levels.items():
                print(f"  {risk}: {count} players")
            
            # Show key position players
            key_positions = team_injuries[team_injuries['position'].isin(['QB', 'RB', 'WR', 'TE'])]
            if not key_positions.empty:
                print(f"\n🎯 KEY OFFENSIVE PLAYERS INJURED:")
                for _, row in key_positions.iterrows():
                    fantasy_str = f"({row['avg_fantasy_points']:.1f} fpts avg)" if pd.notna(row['avg_fantasy_points']) else ""
                    grade_str = f"[{row['madden_grade']}]" if row['madden_grade'] else ""
                    print(f"  {row['position']:2} {row['player_name']:18} | {row['designation']:12} | {row['risk_level']} {fantasy_str} {grade_str}")
                    
            # Show all injuries
            print(f"\n📋 COMPLETE INJURY LIST:")
            print(f"{'Pos':<3} {'Player':<18} {'Status':<12} {'Risk':<8} {'Injury':<20} {'Conf':<6}")
            print("-" * 75)
            
            for _, row in team_injuries.iterrows():
                conf_str = f"{row['confidence_score']:.2f}" if row['confidence_score'] else "N/A"
                injury_short = (row['injury_detail'][:17] + "...") if len(str(row['injury_detail'])) > 20 else str(row['injury_detail'])
                print(f"{row['position']:<3} {row['player_name']:<18} {row['designation']:<12} {row['risk_level']:<8} {injury_short:<20} {conf_str:<6}")
            
            # BOT SUMMARY
            high_impact = len(team_injuries[team_injuries['impact_score'] >= 3])
            moderate_impact = len(team_injuries[team_injuries['impact_score'] == 2])
            
            print(f"\n🤖 BOT IMPACT SUMMARY:")
            print(f"  High Impact Injuries: {high_impact} (players OUT/IR)")
            print(f"  Moderate Impact: {moderate_impact} (doubtful players)")
            
            if high_impact >= 3:
                print(f"  🚨 RECOMMENDATION: {team_code} significantly impacted by injuries")
            elif high_impact >= 2:
                print(f"  ⚠️ RECOMMENDATION: {team_code} moderately impacted by injuries")
            else:
                print(f"  ✅ RECOMMENDATION: {team_code} minimally impacted by injuries")
                
            return team_injuries.to_dict('records')
            
    except Exception as e:
        print(f"❌ Team report failed: {e}")
        return None

def export_bot_injury_data(filename="bot_injury_export.json"):
    """Export injury data in bot-friendly format"""
    print(f"💾 EXPORTING INJURY DATA FOR BOT")
    print("=" * 40)
    
    try:
        with engine.connect() as conn:
            # Get all active injury data
            query = text("""
                SELECT 
                    player_name,
                    player_id,
                    team,
                    position,
                    designation,
                    injury_detail,
                    availability_status,
                    risk_level,
                    impact_score,
                    confidence_score,
                    avg_fantasy_points,
                    madden_grade,
                    injury_date,
                    last_updated
                FROM injury_bot_data
                ORDER BY impact_score DESC, team, player_name
            """)
            
            injury_data = pd.read_sql(query, conn)
            
            if injury_data.empty:
                print("❌ No injury data to export")
                return None
                
            # Convert to bot-friendly format
            bot_export = {
                'export_timestamp': datetime.now().isoformat(),
                'total_injuries': len(injury_data),
                'data_freshness': 'Last 14 days',
                'injuries_by_team': {},
                'high_impact_players': [],
                'all_injuries': injury_data.to_dict('records')
            }
            
            # Group by team
            for team in injury_data['team'].unique():
                team_data = injury_data[injury_data['team'] == team]
                bot_export['injuries_by_team'][team] = {
                    'total_injuries': len(team_data),
                    'high_impact': len(team_data[team_data['impact_score'] >= 3]),
                    'moderate_impact': len(team_data[team_data['impact_score'] == 2]),
                    'players': team_data.to_dict('records')
                }
            
            # High impact players across all teams
            high_impact = injury_data[injury_data['impact_score'] >= 2]
            bot_export['high_impact_players'] = high_impact.to_dict('records')
            
            # Save to JSON
            with open(filename, 'w') as f:
                json.dump(bot_export, f, indent=2, default=str)
                
            print(f"✅ Exported {len(injury_data)} injury records to {filename}")
            print(f"📊 Export includes:")
            print(f"  - {len(bot_export['injuries_by_team'])} teams with injuries")
            print(f"  - {len(bot_export['high_impact_players'])} high-impact injuries")
            print(f"  - Complete mapping and confidence scores")
            
            return filename
            
    except Exception as e:
        print(f"❌ Export failed: {e}")
        return None

def main():
    """Main validation and testing function"""
    print("🏈 INJURY DATA VALIDATION & BOT INTEGRATION")
    print("=" * 60)
    
    # Test basic connectivity
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM nfl_injuries_tracking")).scalar()
            print(f"✅ Database connected. Total injury records: {result}")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return
    
    while True:
        print(f"\n📋 AVAILABLE OPERATIONS:")
        print("1. Validate injury mappings")
        print("2. Get bot injury data (all teams)")
        print("3. Check specific player injury status")
        print("4. Generate team injury report")
        print("5. Export bot-friendly injury data")
        print("6. Exit")
        
        choice = input("\nSelect operation (1-6): ").strip()
        
        if choice == '1':
            validate_injury_mappings()
            
        elif choice == '2':
            impact = input("Enter impact threshold (0-3, default=1): ").strip()
            impact = int(impact) if impact.isdigit() else 1
            get_injury_data_for_bot(impact_threshold=impact)
            
        elif choice == '3':
            player = input("Enter player name or ID: ").strip()
            if player:
                check_player_injury_status(player)
            
        elif choice == '4':
            team = input("Enter team code (e.g., KC, BUF): ").strip().upper()
            if team:
                generate_team_injury_report(team)
                
        elif choice == '5':
            filename = input("Enter filename (default: bot_injury_export.json): ").strip()
            if not filename:
                filename = "bot_injury_export.json"
            export_bot_injury_data(filename)
            
        elif choice == '6':
            print("👋 Goodbye!")
            break
            
        else:
            print("❌ Invalid choice. Please select 1-6.")

if __name__ == "__main__":
    main()