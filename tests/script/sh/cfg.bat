@echo off

if %1 == -n set NODE_NAME=%2
if %1 == -c set CONFIG_NAME=%2
if %1 == -v set CONFIG_VALUE=%2
if %3 == -n set NODE_NAME=%4
if %3 == -c set CONFIG_NAME=%4
if %3 == -v set CONFIG_VALUE=%4
if %5 == -n set NODE_NAME=%6
if %5 == -c set CONFIG_NAME=%6
if %5 == -v set CONFIG_VALUE=%6

rem echo NODE_NAME:  %NODE_NAME%
rem echo NODE:       %NODE%

set SCRIPT_DIR=%~dp0..\
rem echo SCRIPT_DIR: %SCRIPT_DIR%

echo %cd% | grep community > nul && set "BUILD_DIR=%SCRIPT_DIR%..\..\..\debug\build\bin\" || set "BUILD_DIR=%SCRIPT_DIR%..\..\debug\build\bin\"
set TSIM=%BUILD_DIR%tsim
rem echo BUILD_DIR:  %BUILD_DIR%
rem echo TSIM:       %TSIM%

echo %cd% | grep community > nul && set "SIM_DIR=%SCRIPT_DIR%..\..\..\sim\" || set "SIM_DIR=%SCRIPT_DIR%..\..\sim\"
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

echo %CONFIG_NAME%           %CONFIG_VALUE%  >> %TAOS_CFG%
