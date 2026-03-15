@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

REM Install TDGPT Taosanode as Windows Service

set INSTALL_DIR=C:\TDengine\taosanode

echo Installing Taosanode service...

REM Remove existing service if exists
sc query taosanode >nul 2>nul
if %errorlevel% == 0 (
    echo Stopping existing service...
    sc stop taosanode >nul 2>nul
    timeout /t 2 /nobreak >nul
    sc delete taosanode >nul 2>nul
    timeout /t 2 /nobreak >nul
)

REM Create new service
REM Using nssm (Non-Sucking Service Manager) approach is recommended for Python scripts
REM For now, we use a simple batch wrapper
sc create taosanode start= delayed-auto binPath= "cmd.exe /c %INSTALL_DIR%\bin\start-taosanode.bat" displayname= "TDGPT Taosanode Service"

if %errorlevel% == 0 (
    sc description taosanode "TDGPT Analytics Node Service for TDengine"
    echo Service installed successfully.
    echo.
    echo To start the service:
    echo   sc start taosanode
    echo   or
    echo   net start taosanode
    echo.
    echo To stop the service:
    echo   sc stop taosanode
    echo   or
    echo   net stop taosanode
) else (
    echo Failed to install service. Please run as Administrator.
    exit /b 1
)

endlocal
