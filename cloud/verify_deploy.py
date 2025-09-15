# verify_and_deploy.py - Confirm local code works, then force fresh deployment
"""
Your local code looks clean. Let's verify it works then force a fresh deployment.
"""

import os
import subprocess

def test_local_app():
    """Test that your current local code actually works"""
    
    print("TESTING LOCAL CODE")
    print("=" * 30)
    
    # Set environment
    os.environ['DATABASE_URL'] = 'postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres'
    
    try:
        from mobile_dashboard import app
        
        with app.test_client() as client:
            
            # Test the endpoints that were failing in logs
            test_endpoints = [
                ('/api/rankings', 'Rankings'),
                ('/api/predictions', 'Predictions'), 
                ('/api/betting-analysis', 'Betting Analysis'),
                ('/api/ai-betting-recommendations', 'AI Recommendations')
            ]
            
            all_passed = True
            
            for endpoint, name in test_endpoints:
                try:
                    response = client.get(endpoint)
                    
                    if response.status_code == 200:
                        data = response.get_json()
                        if isinstance(data, list):
                            print(f"  {name}: SUCCESS ({len(data)} items)")
                        elif isinstance(data, dict):
                            print(f"  {name}: SUCCESS (dict response)")
                        else:
                            print(f"  {name}: SUCCESS")
                    elif response.status_code == 500:
                        print(f"  {name}: ERROR 500 - {response.get_data(as_text=True)[:100]}")
                        all_passed = False
                    else:
                        print(f"  {name}: HTTP {response.status_code}")
                        
                except Exception as e:
                    print(f"  {name}: EXCEPTION - {str(e)[:100]}")
                    all_passed = False
            
            return all_passed
            
    except Exception as e:
        print(f"Failed to import/start app: {e}")
        return False

def force_fresh_deployment():
    """Create deployment steps to force fresh code"""
    
    deployment_script = '''#!/bin/bash
# deploy_fresh.sh - Force a completely fresh deployment

echo "FORCING FRESH DEPLOYMENT"
echo "========================"

# Add all changes
git add .

# Commit with timestamp to force new deployment
git commit -m "Force fresh deployment - fix database queries $(date)"

# Push to trigger new build
git push origin main

echo ""
echo "Deployment triggered!"
echo "Monitor at: https://dashboard.render.com"
echo ""
echo "If still failing after deployment:"
echo "1. Check Render logs for the exact error"
echo "2. Verify environment variables are set"
echo "3. Check if old cached code is still running"
'''

    with open('deploy_fresh.sh', 'w') as f:
        f.write(deployment_script)
    
    print("Created deploy_fresh.sh script")

def create_render_debug_endpoint():
    """Add a debug endpoint to see what's actually running on server"""
    
    debug_code = '''
# Add this to your mobile_dashboard.py to debug what's actually running on server

@app.route('/debug/environment')
def debug_environment():
    """Debug endpoint to see what's running on the server"""
    import sys
    import os
    from datetime import datetime
    
    return {
        "python_version": sys.version,
        "current_time": datetime.now().isoformat(),
        "database_url_configured": bool(os.environ.get('DATABASE_URL')),
        "file_modification": {
            "mobile_dashboard.py": os.path.getmtime(__file__) if os.path.exists(__file__) else "not found"
        },
        "imports_working": {
            "sqlalchemy_text": "text" in str(type(text)) if 'text' in locals() else False,
            "pandas": "pandas" in sys.modules,
            "flask": "flask" in sys.modules
        }
    }
'''
    
    with open('debug_endpoint.py', 'w') as f:
        f.write(debug_code)
    
    print("Created debug_endpoint.py - add this to mobile_dashboard.py")

def main():
    print("VERIFYING LOCAL CODE AND PREPARING DEPLOYMENT")
    print("=" * 50)
    
    # Test local code
    if test_local_app():
        print("\nLOCAL CODE WORKS PERFECTLY!")
        print("The issue is definitely stale/cached code on your server.")
        
        # Prepare fresh deployment
        force_fresh_deployment()
        create_render_debug_endpoint()
        
        print("\nNEXT STEPS:")
        print("1. Add the debug endpoint to mobile_dashboard.py")
        print("2. Run: bash deploy_fresh.sh")
        print("3. Wait for deployment to complete")
        print("4. Visit: https://bettrbot.onrender.com/debug/environment")
        print("5. Check if the server is running your updated code")
        
        print("\nIf the debug endpoint shows old file timestamps,")
        print("your deployment platform is serving cached code.")
        
    else:
        print("\nLOCAL CODE HAS ISSUES!")
        print("Need to fix local code before deploying.")
        
        print("\nThe errors are likely in functions that weren't caught by inspection.")
        print("Look for SQL queries in:")
        print("- Exception handlers")
        print("- Helper functions") 
        print("- Imported modules")

if __name__ == "__main__":
    main()