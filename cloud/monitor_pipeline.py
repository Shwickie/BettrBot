# monitor_pipeline.py - Simple monitoring for your automated pipeline
"""
Simple monitoring script to check if your pipeline is running properly
Can be run manually or integrated into your dashboard
"""

import os
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path
from sqlalchemy import create_engine, text

def check_database_health():
    """Check if database is accessible and has recent data"""
    try:
        DATABASE_URL = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"
        if DATABASE_URL and DATABASE_URL.startswith('postgres://'):
            DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
        
        if DATABASE_URL:
            engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        else:
            # Local fallback
            db_path = r"E:/Bettr Bot/betting-bot/data/betting.db"
            engine = create_engine(f"sqlite:///{db_path}")
        
        with engine.connect() as conn:
            # Test basic connectivity
            conn.execute(text("SELECT 1"))
            
            # Check if we have recent data
            if DATABASE_URL:  # PostgreSQL
                recent_games = conn.execute(text("""
                    SELECT COUNT(*) FROM games 
                    WHERE game_date >= CURRENT_DATE - INTERVAL '7 days'
                """)).scalar()
                
                recent_odds = conn.execute(text("""
                    SELECT COUNT(*) FROM odds 
                    WHERE timestamp >= NOW() - INTERVAL '24 hours'
                """)).scalar()
            else:  # SQLite
                recent_games = conn.execute(text("""
                    SELECT COUNT(*) FROM games 
                    WHERE date(game_date) >= date('now', '-7 days')
                """)).scalar()
                
                recent_odds = conn.execute(text("""
                    SELECT COUNT(*) FROM odds 
                    WHERE timestamp >= datetime('now', '-24 hours')
                """)).scalar()
        
        return {
            'status': 'healthy',
            'recent_games': recent_games,
            'recent_odds': recent_odds,
            'database_type': 'PostgreSQL' if DATABASE_URL else 'SQLite'
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e),
            'database_type': 'PostgreSQL' if DATABASE_URL else 'SQLite'
        }

def check_pipeline_runs():
    """Check recent pipeline run history"""
    try:
        log_file = Path("pipeline_runs.log")
        if not log_file.exists():
            return {
                'status': 'no_logs',
                'message': 'No pipeline run logs found'
            }
        
        # Read recent runs
        with open(log_file, 'r') as f:
            lines = f.readlines()
        
        if not lines:
            return {
                'status': 'empty_logs',
                'message': 'Pipeline log file is empty'
            }
        
        # Parse last few runs
        recent_runs = []
        for line in lines[-10:]:  # Last 10 runs
            try:
                parts = line.strip().split(',')
                run_time = datetime.fromisoformat(parts[0])
                duration = float(parts[1])
                success = parts[2] == 'True'
                run_number = int(parts[3])
                
                recent_runs.append({
                    'time': run_time,
                    'duration': duration,
                    'success': success,
                    'run_number': run_number
                })
            except Exception:
                continue
        
        if not recent_runs:
            return {
                'status': 'parse_error',
                'message': 'Could not parse pipeline logs'
            }
        
        # Analyze runs
        last_run = recent_runs[-1]
        last_24h = [r for r in recent_runs if r['time'] > datetime.now() - timedelta(days=1)]
        successful_24h = sum(1 for r in last_24h if r['success'])
        
        # Check if last run was recent (within 5 hours)
        time_since_last = datetime.now() - last_run['time']
        is_recent = time_since_last < timedelta(hours=5)
        
        return {
            'status': 'success',
            'last_run': {
                'time': last_run['time'].isoformat(),
                'success': last_run['success'],
                'duration': last_run['duration'],
                'time_ago_hours': time_since_last.total_seconds() / 3600
            },
            'last_24h': {
                'total_runs': len(last_24h),
                'successful_runs': successful_24h,
                'success_rate': successful_24h / len(last_24h) if last_24h else 0
            },
            'is_running_regularly': is_recent
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e)
        }

def check_scheduler_status():
    """Check if automated scheduler is running"""
    try:
        from automated_scheduler import get_scheduler_status
        return get_scheduler_status()
    except ImportError:
        return {
            'status': 'not_available',
            'message': 'Automated scheduler module not found'
        }
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e)
        }

def generate_health_report():
    """Generate comprehensive health report"""
    print("BETTR BOT PIPELINE HEALTH CHECK")
    print("=" * 50)
    print(f"Report generated: {datetime.now()}")
    print()
    
    # Check database
    print("1. DATABASE HEALTH:")
    db_health = check_database_health()
    if db_health['status'] == 'healthy':
        print(f"   ✅ Database ({db_health['database_type']}) is accessible")
        print(f"   📊 Recent games: {db_health['recent_games']}")
        print(f"   📈 Recent odds: {db_health['recent_odds']}")
    else:
        print(f"   ❌ Database error: {db_health.get('error', 'Unknown error')}")
    print()
    
    # Check pipeline runs
    print("2. PIPELINE EXECUTION:")
    pipeline_health = check_pipeline_runs()
    if pipeline_health['status'] == 'success':
        last_run = pipeline_health['last_run']
        print(f"   ✅ Last run: {last_run['time'][:19]} ({'✅ Success' if last_run['success'] else '❌ Failed'})")
        print(f"   ⏱️  Duration: {last_run['duration']:.1f} seconds")
        print(f"   🕐 Time ago: {last_run['time_ago_hours']:.1f} hours")
        
        if pipeline_health['is_running_regularly']:
            print("   ✅ Pipeline is running on schedule")
        else:
            print("   ⚠️  Pipeline may be behind schedule")
            
        stats = pipeline_health['last_24h']
        print(f"   📊 Last 24h: {stats['successful_runs']}/{stats['total_runs']} successful ({stats['success_rate']:.1%})")
    else:
        print(f"   ❌ Pipeline status: {pipeline_health.get('message', pipeline_health.get('error', 'Unknown'))}")
    print()
    
    # Check scheduler
    print("3. AUTOMATED SCHEDULER:")
    scheduler_health = check_scheduler_status()
    if scheduler_health.get('running'):
        print("   ✅ Scheduler is running")
        if scheduler_health.get('next_run'):
            next_run = datetime.fromisoformat(scheduler_health['next_run'])
            time_to_next = next_run - datetime.now()
            print(f"   ⏰ Next run: {next_run.strftime('%Y-%m-%d %H:%M')} (in {time_to_next})")
        print(f"   📈 Total runs: {scheduler_health.get('run_count', 0)}")
    else:
        if scheduler_health.get('apscheduler_available'):
            print("   ⚠️  Scheduler not running")
        else:
            print("   ❌ APScheduler not available")
    print()
    
    # Overall assessment
    print("4. OVERALL ASSESSMENT:")
    issues = []
    
    if db_health['status'] != 'healthy':
        issues.append("Database connectivity")
    
    if pipeline_health['status'] != 'success':
        issues.append("Pipeline execution")
    elif not pipeline_health.get('is_running_regularly'):
        issues.append("Pipeline scheduling")
    
    if not scheduler_health.get('running') and scheduler_health.get('apscheduler_available'):
        issues.append("Automated scheduler")
    
    if not issues:
        print("   🎉 All systems operational!")
        print("   💡 Your betting bot is running smoothly")
    else:
        print(f"   ⚠️  Issues detected: {', '.join(issues)}")
        print("   🔧 Check the details above for troubleshooting")
    
    return {
        'database': db_health,
        'pipeline': pipeline_health,
        'scheduler': scheduler_health,
        'overall_status': 'healthy' if not issues else 'issues',
        'issues': issues
    }

def save_health_report(report_data):
    """Save health report to file for tracking"""
    try:
        report_file = Path("health_reports.json")
        
        # Load existing reports
        if report_file.exists():
            with open(report_file, 'r') as f:
                reports = json.load(f)
        else:
            reports = []
        
        # Add new report
        report_data['timestamp'] = datetime.now().isoformat()
        reports.append(report_data)
        
        # Keep only last 100 reports
        reports = reports[-100:]
        
        # Save back
        with open(report_file, 'w') as f:
            json.dump(reports, f, indent=2)
            
        print(f"\n📝 Health report saved to {report_file}")
        
    except Exception as e:
        print(f"\n⚠️  Could not save health report: {e}")

def get_recommendations(report_data):
    """Generate recommendations based on health report"""
    recommendations = []
    
    # Database recommendations
    if report_data['database']['status'] != 'healthy':
        recommendations.append("🔧 Check database connection and credentials")
    elif report_data['database'].get('recent_odds', 0) == 0:
        recommendations.append("📊 No recent odds data - check odds fetching")
    
    # Pipeline recommendations
    pipeline = report_data['pipeline']
    if pipeline['status'] != 'success':
        recommendations.append("🔄 Check pipeline logs for errors")
    elif not pipeline.get('is_running_regularly'):
        recommendations.append("⏰ Pipeline may need manual restart")
    elif pipeline.get('last_24h', {}).get('success_rate', 1) < 0.8:
        recommendations.append("📈 Recent pipeline failures - investigate logs")
    
    # Scheduler recommendations
    scheduler = report_data['scheduler']
    if not scheduler.get('running') and scheduler.get('apscheduler_available'):
        recommendations.append("🤖 Restart automated scheduler")
    elif not scheduler.get('apscheduler_available'):
        recommendations.append("📦 Install APScheduler: pip install APScheduler==3.10.4")
    
    return recommendations

def main():
    """Main monitoring function"""
    try:
        # Generate health report
        report_data = generate_health_report()
        
        # Save report
        save_health_report(report_data)
        
        # Get recommendations
        recommendations = get_recommendations(report_data)
        
        if recommendations:
            print("\n5. RECOMMENDATIONS:")
            for rec in recommendations:
                print(f"   {rec}")
        
        print(f"\n{'='*50}")
        
        # Return status code for automation
        return 0 if report_data['overall_status'] == 'healthy' else 1
        
    except Exception as e:
        print(f"❌ Health check failed: {e}")
        return 2

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)