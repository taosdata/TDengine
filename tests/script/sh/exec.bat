@echo off

rem echo Executing exec.sh

if %1 == -n set NODE_NAME=%2
if %1 == -s set EXEC_OPTON=%2
if %3 == -n set NODE_NAME=%4
if %3 == -s set EXEC_OPTON=%4

rem echo NODE_NAME:  %NODE_NAME%
rem echo NODE:       %EXEC_OPTON%

set SCRIPT_DIR=%~dp0..\
rem echo SCRIPT_DIR: %SCRIPT_DIR%

set BUILD_DIR=%SCRIPT_DIR%..\..\..\debug\build\bin\
set TAOSD=%BUILD_DIR%taosd
rem echo BUILD_DIR:  %BUILD_DIR%
rem echo TAOSD:      %TAOSD%

set SIM_DIR=%SCRIPT_DIR%..\..\..\sim\
rem echo SIM_DIR:    %SIM_DIR%

set NODE_DIR=%SIM_DIR%%NODE_NAME%\
rem echo NODE_DIR:   %NODE_DIR%

set CFG_DIR=%NODE_DIR%cfg\
rem echo CFG_DIR:    %CFG_DIR%

set TAOS_CFG=%CFG_DIR%taos.cfg
rem echo TAOS_CFG:   %TAOS_CFG%

if %EXEC_OPTON% == start (
  echo start %TAOSD% -c %CFG_DIR%
  start %TAOSD% -c %CFG_DIR%
)

if %EXEC_OPTON% == stop (
  rem echo wmic process where "name='taosd.exe' and CommandLine like '%%%NODE_NAME%%%'" list INSTANCE
  rem wmic process where "name='taosd.exe' and CommandLine like '%%%NODE_NAME%%%'" call terminate > NUL 2>&1
  
  for /f "tokens=1 skip=1" %%A in (
    'wmic process where "name='taosd.exe' and CommandLine like '%%%NODE_NAME%%%'" get processId '
  ) do (
    rem echo taskkill /IM %%A 
    taskkill /IM %%A > NUL 2>&1
  ) 
)
