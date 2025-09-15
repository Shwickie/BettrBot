#!/bin/bash
# Auto-generated deployment script

echo "DEPLOYING FIXED BETTR BOT"
echo "=========================="

# Add all changes
git add .

# Commit with timestamp
git commit -m "COMPLETE FIX: All SQL queries fixed for PostgreSQL $(date)"

# Push to trigger deployment
git push origin main

echo ""
echo "Deployment triggered!"
echo "Monitor at your Render dashboard"
echo ""
echo "Your app will be available at:"
echo "https://bettrbot.onrender.com"
