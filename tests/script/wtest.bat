@echo off

echo TDengine in windows
rem echo Start TDengine Testing Case ...

set "SCRIPT_DIR=%~dp0"
rem echo SCRIPT_DIR: %SCRIPT_DIR%

echo %cd% | grep community > nul && set "BUILD_DIR=%SCRIPT_DIR%..\..\..\debug\build\bin\" || set "BUILD_DIR=%SCRIPT_DIR%..\..\debug\build\bin\"
set "TSIM=%BUILD_DIR%tsim"
rem echo BUILD_DIR:  %BUILD_DIR%
rem echo TSIM:       %TSIM%

echo %cd% | grep community > nul && set "SIM_DIR=%SCRIPT_DIR%..\..\..\sim\" || set "SIM_DIR=%SCRIPT_DIR%..\..\sim\"
rem echo SIM_DIR:    %SIM_DIR%

set "TSIM_DIR=%SIM_DIR%tsim\"
rem echo TSIM_DIR:   %TSIM_DIR%

set "DATA_DIR=%TSIM_DIR%data\"
rem echo DATA_DIR:   %DATA_DIR%

set "CFG_DIR=%TSIM_DIR%cfg\"
rem echo CFG_DIR:    %CFG_DIR%

set "LOG_DIR=%TSIM_DIR%log\"
rem echo LOG_DIR:    %LOG_DIR%

set "TAOS_CFG=%CFG_DIR%taos.cfg"
rem echo TAOS_CFG:   %TAOS_CFG%

if not exist %SIM_DIR%  mkdir %SIM_DIR%
if not exist %TSIM_DIR% mkdir %TSIM_DIR%
if exist %CFG_DIR% rmdir /s/q %CFG_DIR%
if exist %LOG_DIR% rmdir /s/q %LOG_DIR%
if exist %DATA_DIR% rmdir /s/q %DATA_DIR%
if not exist %CFG_DIR% mkdir %CFG_DIR%
if not exist %LOG_DIR% mkdir %LOG_DIR%
if not exist %DATA_DIR% mkdir %DATA_DIR%

set "fqdn=localhost"
for /f "skip=1" %%A in (
  'wmic computersystem get caption'
) do if not defined fqdn set "fqdn=%%A"

echo firstEp        %fqdn%:7100   > %TAOS_CFG%
echo secondEp       %fqdn%:7200   >> %TAOS_CFG%
echo fqdn           %fqdn%        >> %TAOS_CFG%
echo serverPort     7100          >> %TAOS_CFG%
echo dataDir        %DATA_DIR%    >> %TAOS_CFG%
echo logDir         %LOG_DIR%     >> %TAOS_CFG%
echo scriptDir      %SCRIPT_DIR%  >> %TAOS_CFG%
echo numOfLogLines  100000000     >> %TAOS_CFG%
echo rpcDebugFlag   143           >> %TAOS_CFG%
echo tmrDebugFlag   131           >> %TAOS_CFG%
echo cDebugFlag     143           >> %TAOS_CFG%
echo qDebugFlag     143           >> %TAOS_CFG%
echo uDebugFlag     143           >> %TAOS_CFG%
echo debugFlag      143           >> %TAOS_CFG%
echo wal            0             >> %TAOS_CFG%
echo asyncLog       0             >> %TAOS_CFG%
echo locale         en_US.UTF-8   >> %TAOS_CFG%
echo enableCoreFile 1             >> %TAOS_CFG%
echo charset        UTF-8         >> %TAOS_CFG%

set "FILE_NAME=testSuite.sim"
if "%1" == "-f" set "FILE_NAME=%2"
set FILE_NAME=%FILE_NAME:/=\%

start cmd /k "timeout /t 800 /NOBREAK && taskkill /f /im tsim.exe & exit /b"

rem echo FILE_NAME:  %FILE_NAME%
echo ExcuteCmd:  %tsim% -c %CFG_DIR% -f %FILE_NAME%
set result=false
%TSIM% -c %CFG_DIR% -f %FILE_NAME% && set result=true

tasklist | grep timeout && taskkill /f /im timeout.exe
if "%result%" == "true" ( exit /b ) else ( exit /b 8 )