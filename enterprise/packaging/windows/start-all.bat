@echo off
REM start as administrator by default
set "params=%*"
cd /d "%~dp0" && ( if exist "%temp%\getadmin.vbs" del "%temp%\getadmin.vbs" ) && fsutil dirty query %systemdrive% 1>nul 2>nul || (  echo Set UAC = CreateObject^("Shell.Application"^) : UAC.ShellExecute "cmd.exe", "/c cd ""%~sdp0"" && %~s0 %params%", "", "runas", 1 >> "%temp%\getadmin.vbs" && "%temp%\getadmin.vbs" && exit /B )

echo Starting TDengine TSDB services...
for %%S in (taosd taosadapter taosx taos-explorer) do (
    sc query %%S | findstr /i "STATE" >nul
    if errorlevel 1 (
        echo %%S does not exist.
    ) else (
        sc start %%S >nul 2>&1
        echo Start command sent to %%S.
    )
)
timeout /t 5 >nul
sc query taoskeeper | findstr /i "STATE" >nul
if errorlevel 1 (
    echo taoskeeper does not exist.
) else (
    sc start taoskeeper >nul 2>&1
    echo Start command sent to taoskeeper.
)

echo All start commands issued.