@echo off

rem echo Executing deploy.sh

if %1 == -n set NODE_NAME=%2
if %1 == -i set NODE=%2
if %3 == -n set NODE_NAME=%4
if %3 == -i set NODE=%4

rem echo NODE_NAME:  %NODE_NAME%
rem echo NODE:       %NODE%

set SCRIPT_DIR=%~dp0..\
rem echo SCRIPT_DIR: %SCRIPT_DIR%

set BUILD_DIR=%SCRIPT_DIR%..\..\..\debug\build\bin\
set TSIM=%BUILD_DIR%tsim
rem echo BUILD_DIR:  %BUILD_DIR%
rem echo TSIM:       %TSIM%

set SIM_DIR=%SCRIPT_DIR%..\..\..\sim\
rem echo SIM_DIR:    %SIM_DIR%

set NODE_DIR=%SIM_DIR%%NODE_NAME%\
rem echo NODE_DIR:   %NODE_DIR%

set CFG_DIR=%NODE_DIR%cfg\
rem echo CFG_DIR:    %CFG_DIR%

set LOG_DIR=%NODE_DIR%log\
rem echo LOG_DIR:    %LOG_DIR%

set DATA_DIR=%NODE_DIR%data\
rem echo DATA_DIR:   %DATA_DIR%

set TAOS_CFG=%CFG_DIR%taos.cfg
rem echo TAOS_CFG:   %TAOS_CFG%

if not exist %SIM_DIR%  mkdir %SIM_DIR%
if not exist %NODE_DIR% mkdir %NODE_DIR%
if exist %CFG_DIR%  rmdir /s/q %CFG_DIR%
if exist %LOG_DIR%  rmdir /s/q %LOG_DIR%
if exist %DATA_DIR% rmdir /s/q %DATA_DIR%
if not exist %CFG_DIR%  mkdir %CFG_DIR%
if not exist %LOG_DIR%  mkdir %LOG_DIR%
if not exist %DATA_DIR% mkdir %DATA_DIR%

if %NODE% == 1 set NODE=7100
if %NODE% == 2 set NODE=7200
if %NODE% == 3 set NODE=7300
if %NODE% == 4 set NODE=7400
if %NODE% == 5 set NODE=7500
if %NODE% == 6 set NODE=7600
if %NODE% == 7 set NODE=7700
if %NODE% == 8 set NODE=7800

rem set "fqdn="
for /f "skip=1" %%A in (
  'wmic computersystem get caption'
) do if not defined fqdn set "fqdn=%%A"

echo firstEp                %fqdn%:7100        >  %TAOS_CFG%
echo fqdn                   %fqdn%             >> %TAOS_CFG%
echo serverPort             %NODE%             >> %TAOS_CFG%
echo dataDir                %DATA_DIR%         >> %TAOS_CFG%
echo logDir                 %LOG_DIR%          >> %TAOS_CFG%
echo debugFlag              0                  >> %TAOS_CFG%
echo mDebugFlag             143                >> %TAOS_CFG%
echo sdbDebugFlag           143                >> %TAOS_CFG%
echo dDebugFlag             143                >> %TAOS_CFG%
echo vDebugFlag             143                >> %TAOS_CFG%
echo tsdbDebugFlag          143                >> %TAOS_CFG%
echo cDebugFlag             143                >> %TAOS_CFG%
echo jnidebugFlag           143                >> %TAOS_CFG%
echo odbcdebugFlag          143                >> %TAOS_CFG%
echo httpDebugFlag          143                >> %TAOS_CFG%
echo monDebugFlag           143                >> %TAOS_CFG%
echo mqttDebugFlag          143                >> %TAOS_CFG%
echo qdebugFlag             143                >> %TAOS_CFG%
echo rpcDebugFlag           143                >> %TAOS_CFG%
echo tmrDebugFlag           131                >> %TAOS_CFG%
echo udebugFlag             143                >> %TAOS_CFG%
echo sdebugFlag             143                >> %TAOS_CFG%
echo wdebugFlag             143                >> %TAOS_CFG%
echo cqdebugFlag            143                >> %TAOS_CFG%
echo monitor                0                  >> %TAOS_CFG%
echo monitorInterval        1                  >> %TAOS_CFG%
echo http                   0                  >> %TAOS_CFG%
echo slaveQuery             0                  >> %TAOS_CFG%
echo numOfThreadsPerCore    2.0                >> %TAOS_CFG%
echo defaultPass            taosdata           >> %TAOS_CFG%
echo numOfLogLines          20000000           >> %TAOS_CFG%
echo mnodeEqualVnodeNum     0                  >> %TAOS_CFG%
echo balanceInterval        1                  >> %TAOS_CFG%
echo clog                   2                  >> %TAOS_CFG%
echo days                   10                 >> %TAOS_CFG%
echo statusInterval         1                  >> %TAOS_CFG%
echo maxVgroupsPerDb        4                  >> %TAOS_CFG%
echo minTablesPerVnode      4                  >> %TAOS_CFG%
echo maxTablesPerVnode      1000               >> %TAOS_CFG%
echo tableIncStepPerVnode   10000              >> %TAOS_CFG%
echo asyncLog               0                  >> %TAOS_CFG%
echo numOfMnodes            1                  >> %TAOS_CFG%
echo locale                 en_US.UTF-8        >> %TAOS_CFG%
echo fsync                  0                  >> %TAOS_CFG%
echo telemetryReporting     0                  >> %TAOS_CFG%
