#!/bin/bash
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
