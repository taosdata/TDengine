@echo off
chcp 65001 >nul

REM TDGPT Taosanode Service Stop Script for Windows

echo Stopping Taosanode service...

REM Kill uwsgi processes
taskkill /f /im uwsgi.exe 2>nul

REM Kill Python processes related to taosanalytics
wmic process where "commandline like '%%taosanalytics%%'" delete 2>nul

REM Alternative: taskkill for python
taskkill /f /im python.exe /fi "windowtitle eq *taosanode*" 2>nul
taskkill /f /im pythonw.exe 2>nul

echo Taosanode service stopped.
