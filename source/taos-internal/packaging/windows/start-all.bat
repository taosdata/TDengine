@echo off
setlocal EnableExtensions DisableDelayedExpansion

REM start as administrator by default
set "params=%*"
cd /d "%~dp0" && ( if exist "%temp%\getadmin.vbs" del "%temp%\getadmin.vbs" ) && fsutil dirty query %systemdrive% 1>nul 2>nul || ( echo Set UAC = CreateObject^("Shell.Application"^) : UAC.ShellExecute "cmd.exe", "/c cd ""%~sdp0"" ^&^& %~s0 %params% ^& pause", "", "runas", 1 >> "%temp%\getadmin.vbs" && "%temp%\getadmin.vbs" && exit /B )

set "cfg_dir=C:\TDengine\cfg"
set "SVC_MAX_RETRY=15"
set "NODE_MAX_RETRY=10"
set "taosx_server_port=6055"

set "taos_exe=%~dp0taos.exe"
if not exist "%taos_exe%" set "taos_exe=taos"

if "%~1"=="" (
    set "ACTION=start"
) else (
    set "ACTION=%~1"
)

call :detect_environment

if /i "%ACTION%"=="start" goto start_all
if /i "%ACTION%"=="status" goto status_all
if /i "%ACTION%"=="stop" goto stop_all
if /i "%ACTION%"=="help" goto usage
if /i "%ACTION%"=="-h" goto usage
if /i "%ACTION%"=="--help" goto usage

echo Error: Unsupported action "%ACTION%".
goto usage

:usage
echo Usage: start-all.bat [start^|status^|stop]
echo.
echo   start  Start TDengine TSDB related services, verify status, and create snode/xnode when needed.
echo   status Show service status and TDengine TSDB connectivity.
echo   stop   Stop TDengine TSDB related services.
exit /b 1

:detect_environment
REM Detect product name and version from taosd
set "product_name="
set "version=unknown"
set "ver_tmp=%TEMP%\taosd_ver_%RANDOM%.tmp"
"%~dp0taosd.exe" -V > "%ver_tmp%" 2>nul
for /f "usebackq delims=" %%A in ("%ver_tmp%") do (
    if not defined product_name set "product_name=%%A"
)
if not defined product_name set "product_name=TDengine TSDB"
for /f "tokens=3" %%A in ('findstr /i "version:" "%ver_tmp%" 2^>nul') do set "version=%%A"
set "version=%version:.enterprise=%"
set "version=%version:.community=%"
if exist "%ver_tmp%" del /f /q "%ver_tmp%" >nul 2>&1

REM Detect edition by taosx.exe existence
set "SERVICES=taosd taosadapter taos-explorer taoskeeper"
set "XNODE_ENABLED=0"
if exist "%~dp0taosx.exe" (
    set "SERVICES=taosd taosadapter taosx taos-explorer taoskeeper"
    set "XNODE_ENABLED=1"
)
exit /b 0

:start_all
echo %product_name% Service Starter - Version %version%
echo Services to start: %SERVICES%
echo.

set "start_failed=0"
for %%S in (%SERVICES%) do (
    call :start_service "%%S"
    if errorlevel 1 set "start_failed=1"
)

echo.
echo Waiting for TDengine TSDB to be ready...
timeout /t 5 /nobreak >nul

call :check_connectivity
if errorlevel 1 (
    echo Error: TDengine TSDB server is not available, please check the server status.
    exit /b 1
)
echo [OK] TDengine TSDB server is available

call :create_snode_if_needed
if errorlevel 1 echo Warning: create snode failed, but continue.

if "%XNODE_ENABLED%"=="1" (
    call :create_xnode_if_needed
    if errorlevel 1 echo Warning: create xnode failed, but continue.
)

echo.
echo All operations completed
if "%start_failed%"=="1" exit /b 1
exit /b 0

:status_all
echo %product_name% Service Status - Version %version%
for %%S in (%SERVICES%) do call :show_service_status "%%S"
echo.
call :get_service_state "taosd" taosd_state
if not "%taosd_state%"=="4" (
    echo [FAIL] TDengine TSDB server is not running.
    exit /b 1
)

call :check_connectivity
if errorlevel 1 (
    echo [FAIL] TDengine TSDB server is not available.
    exit /b 1
)

echo [OK] TDengine TSDB server is available
exit /b 0

:stop_all
echo Stopping TDengine TSDB services...
for %%S in (taoskeeper taos-explorer taosx taosadapter taosd) do call :stop_service "%%S"
echo All stop commands issued.
exit /b 0

:start_service
set "service=%~1"
call :get_service_state "%service%" service_state
if not defined service_state (
    echo %service% does not exist.
    exit /b 0
)

<nul set /p =Starting %service%...
sc start "%service%" >nul 2>&1
set /a retry=0

:start_service_retry
call :get_service_state "%service%" service_state
if "%service_state%"=="4" (
    echo [OK]
    exit /b 0
)
if %retry% geq %SVC_MAX_RETRY% (
    echo [FAIL]
    exit /b 1
)
timeout /t 1 /nobreak >nul
set /a retry+=1
goto start_service_retry

:stop_service
set "service=%~1"
call :get_service_state "%service%" service_state
if not defined service_state (
    echo %service% does not exist.
    exit /b 0
)
if "%service_state%"=="4" (
    sc stop "%service%" >nul 2>&1
    echo Stop command sent to %service%.
) else (
    echo %service% is not running.
)
exit /b 0

:show_service_status
set "service=%~1"
call :get_service_state "%service%" service_state
if not defined service_state (
    echo %service% : not installed
    exit /b 0
)
call :map_service_state "%service_state%" service_text
echo %service% : %service_text%
exit /b 0

:get_service_state
set "%~2="
for /f "tokens=3" %%A in ('sc query "%~1" 2^>nul ^| findstr /i "STATE"') do set "%~2=%%A"
exit /b 0

:map_service_state
set "%~2=unknown"
if "%~1"=="1" set "%~2=stopped"
if "%~1"=="2" set "%~2=start pending"
if "%~1"=="3" set "%~2=stop pending"
if "%~1"=="4" set "%~2=running"
if "%~1"=="5" set "%~2=continue pending"
if "%~1"=="6" set "%~2=pause pending"
if "%~1"=="7" set "%~2=paused"
exit /b 0

:check_connectivity
"%taos_exe%" -c "%cfg_dir%" -s "select server_status();" >nul 2>&1
exit /b %errorlevel%

:create_snode_if_needed
if not exist "%cfg_dir%" mkdir "%cfg_dir%" >nul 2>&1
set "snode_flag=%cfg_dir%\snode_flag"
if exist "%snode_flag%" (
    findstr /x /c:"snode 1" "%snode_flag%" >nul 2>&1
    if not errorlevel 1 exit /b 0
)

set "snode_tmp=%TEMP%\snodes_%RANDOM%_%RANDOM%.txt"
"%taos_exe%" -c "%cfg_dir%" -s "show snodes;" > "%snode_tmp%" 2>nul
if errorlevel 1 (
    if exist "%snode_tmp%" del /f /q "%snode_tmp%" >nul 2>&1
    echo Error: Failed to query snodes.
    exit /b 1
)

findstr /c:"0 row" "%snode_tmp%" >nul 2>&1
if not errorlevel 1 (
    echo Creating snode...
    "%taos_exe%" -c "%cfg_dir%" -s "create snode on dnode 1;" >nul 2>&1
    if errorlevel 1 (
        if exist "%snode_tmp%" del /f /q "%snode_tmp%" >nul 2>&1
        echo Error: Failed to create snode on dnode 1.
        exit /b 2
    )
    echo [OK] Snode created successfully
)

> "%snode_flag%" echo snode 1
if exist "%snode_tmp%" del /f /q "%snode_tmp%" >nul 2>&1
exit /b 0

:create_xnode_if_needed
if not exist "%cfg_dir%" mkdir "%cfg_dir%" >nul 2>&1
set "xnode_flag=%cfg_dir%\xnode_flag"
if exist "%xnode_flag%" (
    findstr /x /c:"xnode 1" "%xnode_flag%" >nul 2>&1
    if not errorlevel 1 exit /b 0
)

set "server_fqdn=localhost"
if exist "%cfg_dir%\taos.cfg" (
    for /f "tokens=1,2" %%A in ('findstr /r /b /c:"fqdn" "%cfg_dir%\taos.cfg"') do set "server_fqdn=%%B"
)

if defined XNODE_USER (
    set "xnode_user=%XNODE_USER%"
) else (
    set "xnode_user=root"
)

if defined XNODE_PASS (
    set "xnode_pass=%XNODE_PASS%"
) else (
    set "xnode_pass=taosdata"
)

echo(%xnode_user%| findstr /r "^[A-Za-z0-9_][A-Za-z0-9_]*$" >nul
if errorlevel 1 (
    echo Error: Invalid xnode_user "%xnode_user%"
    exit /b 1
)

echo(%taosx_server_port%| findstr /r "^[0-9][0-9]*$" >nul
if errorlevel 1 (
    echo Error: Invalid taosx server port "%taosx_server_port%".
    exit /b 2
)

set "xnode_tmp=%TEMP%\xnodes_%RANDOM%_%RANDOM%.txt"
"%taos_exe%" -c "%cfg_dir%" -s "show xnodes;" > "%xnode_tmp%" 2>nul
if errorlevel 1 (
    if exist "%xnode_tmp%" del /f /q "%xnode_tmp%" >nul 2>&1
    echo Error: Failed to query xnodes.
    exit /b 3
)

findstr /c:"0 row" "%xnode_tmp%" >nul 2>&1
if errorlevel 1 goto xnode_already_exists

echo Creating xnode...
call :escape_single_quotes server_fqdn safe_server_fqdn
call :escape_single_quotes xnode_pass safe_xnode_pass
set "create_sql=CREATE XNODE '%safe_server_fqdn%:%taosx_server_port%' USER %xnode_user% PASS '%safe_xnode_pass%';"
set "redacted_sql=CREATE XNODE '%safe_server_fqdn%:%taosx_server_port%' USER %xnode_user% PASS '******';"
"%taos_exe%" -c "%cfg_dir%" -s "%create_sql%" >nul 2>&1
if errorlevel 1 (
    if exist "%xnode_tmp%" del /f /q "%xnode_tmp%" >nul 2>&1
    echo Error: Failed to create xnode: %redacted_sql%
    exit /b 4
)

set "xnode_status_tmp=%TEMP%\xnode_status_%RANDOM%.txt"
set /a xnode_retry=0
:xnode_status_check
"%taos_exe%" -c "%cfg_dir%" -s "SHOW XNODES;" > "%xnode_status_tmp%" 2>nul
if errorlevel 1 (
    if exist "%xnode_status_tmp%" del /f /q "%xnode_status_tmp%" >nul 2>&1
    echo Error: Failed to query xnode status.
    exit /b 5
)
findstr /i "online" "%xnode_status_tmp%" >nul 2>&1
if not errorlevel 1 (
    echo Xnode status:
    type "%xnode_status_tmp%"
    if exist "%xnode_status_tmp%" del /f /q "%xnode_status_tmp%" >nul 2>&1
    echo [OK] Xnode created successfully
    goto xnode_already_exists
)
if %xnode_retry% geq %NODE_MAX_RETRY% (
    echo Xnode status:
    type "%xnode_status_tmp%"
    if exist "%xnode_status_tmp%" del /f /q "%xnode_status_tmp%" >nul 2>&1
    echo [FAIL] Xnode created but not online
    exit /b 6
)
timeout /t 2 /nobreak >nul
set /a xnode_retry+=1
goto xnode_status_check

:xnode_already_exists
> "%xnode_flag%" echo xnode 1
if exist "%xnode_tmp%" del /f /q "%xnode_tmp%" >nul 2>&1
exit /b 0

:escape_single_quotes
call set "value=%%%~1%%"
set "value=%value:'=''%"
set "%~2=%value%"
exit /b 0
