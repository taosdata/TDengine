@echo off
chcp 65001 >nul

REM Uninstall TDGPT Taosanode Windows Service

echo Uninstalling Taosanode service...

REM Stop service if running
sc stop taosanode >nul 2>nul
timeout /t 3 /nobreak >nul

REM Delete service
sc delete taosanode >nul 2>nul

if %errorlevel% == 0 (
    echo Service uninstalled successfully.
) else (
    echo Service not found or already uninstalled.
)

REM Kill any remaining processes
taskkill /f /im uwsgi.exe 2>nul
taskkill /f /im python.exe /fi "windowtitle eq *taosanode*" 2>nul
