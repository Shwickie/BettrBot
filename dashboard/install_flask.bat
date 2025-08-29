@echo off
echo 🔧 Installing Flask for Bettr Bot Dashboard
echo ==========================================

cd "E:\Bettr Bot\betting-bot"

echo 📦 Activating virtual environment...
call venv\Scripts\activate.bat

echo 📥 Installing Flask...
pip install Flask

echo 📥 Installing additional requirements...
pip install pandas sqlalchemy

echo ✅ Installation complete!
echo.
echo 🚀 To run your dashboard:
echo    cd "E:\Bettr Bot\betting-bot\dashboard"
echo    python flask_dashboard.py
echo.
pause