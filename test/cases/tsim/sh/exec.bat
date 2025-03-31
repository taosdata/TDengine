@echo off

rem echo Executing exec.sh

if %1 == -n set NODE_NAME=%2
if %1 == -s set EXEC_OPTON=%2
if %3 == -n set NODE_NAME=%4
if %3 == -s set EXEC_OPTON=%4
if "%5" == "-x" set EXEC_SIGNAL=%6

rem echo NODE_NAME:  %NODE_NAME%
rem echo NODE:       %EXEC_OPTON%

set SCRIPT_DIR=%~dp0..\
rem echo SCRIPT_DIR: %SCRIPT_DIR%

echo %cd% | grep community > nul && set "BUILD_DIR=%SCRIPT_DIR%..\..\..\debug\build\bin\" || set "BUILD_DIR=%SCRIPT_DIR%..\..\debug\build\bin\"
set TAOSD=%BUILD_DIR%taosd
rem echo BUILD_DIR:  %BUILD_DIR%
rem echo TAOSD:      %TAOSD%

echo %cd% | grep community > nul && set "SIM_DIR=%SCRIPT_DIR%..\..\..\sim\" || set "SIM_DIR=%SCRIPT_DIR%..\..\sim\"
rem echo SIM_DIR:    %SIM_DIR%

set NODE_DIR=%SIM_DIR%%NODE_NAME%\
rem echo NODE_DIR:   %NODE_DIR%

set CFG_DIR=%NODE_DIR%cfg\
rem echo CFG_DIR:    %CFG_DIR%

set TAOS_CFG=%CFG_DIR%taos.cfg
rem echo TAOS_CFG:   %TAOS_CFG%

set LOG_DIR=%NODE_DIR%log\
rem echo LOG_DIR:    %LOG_DIR%

set TAOS_LOG=%LOG_DIR%taosdlog.0
rem echo TAOS_LOG:   %TAOS_LOG%

if %EXEC_OPTON% == start (
  rm -rf %TAOS_LOG%
  echo start %TAOSD% -c %CFG_DIR%
  mintty -h never %TAOSD% -c %CFG_DIR%
  set /a check_num=0
:check_online
  sleep 1
  set /a check_num=check_num+1
  if "%check_num%" == "11" (
    echo check online out time.
    goto :finish
  )
  echo check taosd online
  tail -n +0 %TAOS_LOG% | grep -E "TDengine initialized successfully|from offline to online"  || goto :check_online
  echo finish
  goto :finish
)

if %EXEC_OPTON% == stop (
  rem echo wmic process where "name='taosd.exe' and CommandLine like '%%%NODE_NAME%%%'" list INSTANCE
  rem wmic process where "name='taosd.exe' and CommandLine like '%%%NODE_NAME%%%'" call terminate > NUL 2>&1
  
  for /f "tokens=2" %%A in ('wmic process where "name='taosd.exe' and CommandLine like '%%%NODE_NAME%%%'" get processId ^| xargs echo') do (
    for /f "tokens=1" %%B in ('ps ^| grep %%A') do (
      if "%EXEC_SIGNAL%" == "SIGKILL" (
        kill -9 %%B
      ) else (
        kill -INT %%B
        call :check_offline
      )
    )
    goto :finish
  ) 
)
:finish
goto :eof

:check_offline
sleep 1
for /f "tokens=2" %%C in ('wmic process where "name='taosd.exe' and CommandLine like '%%%NODE_NAME%%%'" get processId ^| xargs echo') do (
  for /f "tokens=1" %%D in ('ps ^| grep %%C') do (
    echo kill -INT %%D
    echo check taosd offline %NODE_NAME% %%C %%D
    goto :check_offline
  )
)
goto :eof