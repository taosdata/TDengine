@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

REM TDGPT Taosanode Service Start Script for Windows

set INSTALL_DIR=C:\TDengine\taosanode
set CFG_FILE=%INSTALL_DIR%\cfg\taosanode.ini
set LOG_DIR=%INSTALL_DIR%\log
set VENV_DIR=%INSTALL_DIR%\venv

REM Create log directory if not exists
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM Check for Python
set PYTHON_EXE=
for %%P in (python.exe python3.exe) do (
    where %%P >nul 2>nul
    if !errorlevel! == 0 (
        set PYTHON_EXE=%%P
        goto :found_python
    )
)

REM Check common Python installation paths
if exist "%LOCALAPPDATA%\Programs\Python\Python39\python.exe" (
    set PYTHON_EXE=%LOCALAPPDATA%\Programs\Python\Python39\python.exe
    goto :found_python
)
if exist "%LOCALAPPDATA%\Programs\Python\Python310\python.exe" (
    set PYTHON_EXE=%LOCALAPPDATA%\Programs\Python\Python310\python.exe
    goto :found_python
)
if exist "%LOCALAPPDATA%\Programs\Python\Python311\python.exe" (
    set PYTHON_EXE=%LOCALAPPDATA%\Programs\Python\Python311\python.exe
    goto :found_python
)
if exist "%PROGRAMFILES%\Python39\python.exe" (
    set PYTHON_EXE=%PROGRAMFILES%\Python39\python.exe
    goto :found_python
)
if exist "%PROGRAMFILES%\Python310\python.exe" (
    set PYTHON_EXE=%PROGRAMFILES%\Python310\python.exe
    goto :found_python
)
if exist "%PROGRAMFILES%\Python311\python.exe" (
    set PYTHON_EXE=%PROGRAMFILES%\Python311\python.exe
    goto :found_python
)

echo Python not found. Please install Python 3.9+ and add it to PATH.
exit /b 1

:found_python
echo Using Python: %PYTHON_EXE%

REM Create virtual environment if not exists
if not exist "%VENV_DIR%\Scripts\activate.bat" (
    echo Creating virtual environment...
    "%PYTHON_EXE%" -m venv "%VENV_DIR%"
    if errorlevel 1 (
        echo Failed to create virtual environment.
        exit /b 1
    )
)

REM Activate virtual environment
call "%VENV_DIR%\Scripts\activate.bat"

REM Install/upgrade pip
python -m pip install --upgrade pip --quiet

REM Install requirements if exists
if exist "%INSTALL_DIR%\requirements.txt" (
    echo Installing dependencies...
    pip install -r "%INSTALL_DIR%\requirements.txt" --quiet
    if errorlevel 1 (
        echo Warning: Some dependencies failed to install.
    )
)

REM Install essential requirements (faster install)
if exist "%INSTALL_DIR%\requirements_ess.txt" (
    echo Installing essential dependencies...
    pip install -r "%INSTALL_DIR%\requirements_ess.txt" --quiet
)

REM Set environment variables
set PYTHONPATH=%INSTALL_DIR%\lib
set TAOSANODE_CFG=%CFG_FILE%

REM Start the service
echo Starting Taosanode service...
cd /d "%INSTALL_DIR%\lib"

REM Try to use waitress (Windows-friendly WSGI server) if available
pip show waitress >nul 2>nul
if !errorlevel! == 0 (
    echo Using waitress WSGI server...
    start /B python -c "from waitress import serve; from taosanalytics.app import app; serve(app, host='0.0.0.0', port=6035, threads=4)" > "%LOG_DIR%\taosanode.log" 2>&1
) else (
    REM Fallback to Flask development server (not recommended for production)
    echo Using Flask development server...
    start /B python -c "from taosanalytics.app import app; app.run(host='0.0.0.0', port=6035, threaded=True)" > "%LOG_DIR%\taosanode.log" 2>&1
)

echo Taosanode service started on port 6035.
echo Log file: %LOG_DIR%\taosanode.log

endlocal
