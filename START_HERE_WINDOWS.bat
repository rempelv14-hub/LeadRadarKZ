@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo LeadKZ Free v7 - установка и запуск
python -m pip install -r requirements.txt
if errorlevel 1 (
  py -3 -m pip install -r requirements.txt
)
python start_here.py
pause
