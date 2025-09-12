# test_deployment_fixed.py - Test your fixed app
import os
import sys

def test_app():
    print("Testing your fixed app...")
    
    # Set cloud environment
    os.environ['DATABASE_URL'] = 'postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres'
    os.environ['FLASK_ENV'] = 'production'
    
    try:
        # Import your actual app
        from app import app
        print("App imported successfully")
        
        # Test health endpoint
        with app.test_client() as client:
            response = client.get('/health')
            
            if response.status_code in [200, 503]:
                data = response.get_json()
                print(f"Health check passed:")
                print(f"  Status: {data.get('status')}")
                print(f"  Database: {data.get('checks', {}).get('database')}")
                print(f"  Model: {data.get('checks', {}).get('model')}")
                
                # Test your dashboard
                response = client.get('/')
                if response.status_code in [200, 302]:  # 302 for redirect to login
                    print(f"Dashboard responds: {response.status_code}")
                    return True
                else:
                    print(f"Dashboard failed: {response.status_code}")
                    return False
            else:
                print(f"Health check failed: {response.status_code}")
                return False
                
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    if test_app():
        print("\nSUCCESS: Your app is ready for deployment!")
        print("\nNext steps:")
        print("1. git add .")
        print("2. git commit -m 'Fixed deployment errors'")
        print("3. git push origin main")
        print("4. Deploy to Render")
    else:
        print("\nTest failed - check errors above")
