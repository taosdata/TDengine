
@echo off

set EXEC_OPTON=start
set DB_NAME=db 
set CDB_NAME=db
set /a POLL_DELAY=5
set /a VALGRIND=0
set SIGNAL=SIGINT
set /a SHOW_MSG=0
set /a SHOW_ROW=0
set /a EXP_USE_SNAPSHOT=0

:param
if "%1"=="" (
    goto :end
)
if %1 == -d ( set "DB_NAME=%2" && shift && shift && goto :param )
if %1 == -g ( set "SHOW_MSG=%2" && shift && shift && goto :param )
if %1 == -r ( set "SHOW_ROW=%2" && shift && shift && goto :param )
if %1 == -s ( set "EXEC_OPTON=%2" && shift && shift && goto :param )
if %1 == -v ( set "VALGRIND=1" && shift && goto :param )
if %1 == -y ( set "POLL_DELAY=%2" && shift && shift && goto :param )
if %1 == -x ( set "SIGNAL=%2" && shift && shift && goto :param )
if %1 == -w ( set "CDB_NAME=%2" && shift && shift && goto :param )
if %1 == -e ( set "EXP_USE_SNAPSHOT=%2" && shift && shift && goto :param )
echo unkown argument %1
goto :eof
:end

echo EXEC_OPTON %EXEC_OPTON%
echo DB_NAME %DB_NAME%
echo CDB_NAME %CDB_NAME%
echo POLL_DELAY %POLL_DELAY%
echo VALGRIND %VALGRIND%
echo SIGNAL %SIGNAL%
echo SHOW_MSG %SHOW_MSG%
echo SHOW_ROW %SHOW_ROW%
echo EXP_USE_SNAPSHOT %EXP_USE_SNAPSHOT%

echo %cd% | grep community > nul && cd  ..\..\.. || cd  ..\..
set BUILD_DIR=%cd%\debug\build\bin
set SIM_DIR=%cd%\sim
set PRG_DIR=%SIM_DIR%\tsim
set CFG_DIR=%PRG_DIR%\cfg
set LOG_DIR=%PRG_DIR%\log
set PROGRAM=%BUILD_DIR%\tmq_sim.exe

echo ------------------------------------------------------------------------
echo BUILD_DIR  : %BUILD_DIR%
echo SIM_DIR    : %SIM_DIR%
echo CFG_DIR    : %CFG_DIR%
echo PRG_DIR    : %PRG_DIR%
echo CFG_DIR    : %CFG_DIR%
echo LOG_DIR    : %LOG_DIR%
echo PROGRAM    : %PROGRAM%
echo POLL_DELAY : %POLL_DELAY% 
echo DB_NAME    : %DB_NAME%
echo ------------------------------------------------------------------------

if "%EXEC_OPTON%" == "start" ( 
  echo  mintty -h never %PROGRAM% -c %CFG_DIR% -y %POLL_DELAY% -d %DB_NAME% -g %SHOW_MSG% -r %SHOW_ROW% -w %CDB_NAME% -e %EXP_USE_SNAPSHOT%
  mintty -h never %PROGRAM% -c %CFG_DIR% -y %POLL_DELAY% -d %DB_NAME% -g %SHOW_MSG% -r %SHOW_ROW% -w %CDB_NAME% -e %EXP_USE_SNAPSHOT%
) else (
  if "%SIGNAL%" == "SIGKILL" ( ps | grep tmq_sim | awk '{print $2}' | xargs kill -9 ) else ( ps | grep tmq_sim | awk '{print $2}' | xargs kill -SIGINT )
)
goto :eof
