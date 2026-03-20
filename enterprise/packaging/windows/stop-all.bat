@echo off
REM start as administrator by default
set "params=%*"
cd /d "%~dp0" && ( if exist "%temp%\getadmin.vbs" del "%temp%\getadmin.vbs" ) && fsutil dirty query %systemdrive% 1>nul 2>nul || (  echo Set UAC = CreateObject^("Shell.Application"^) : UAC.ShellExecute "cmd.exe", "/c cd ""%~sdp0"" && %~s0 %params%", "", "runas", 1 >> "%temp%\getadmin.vbs" && "%temp%\getadmin.vbs" && exit /B )

echo Stopping TDengine TSDB services...
REM Only stop services with "STATE : 4 RUNNING"
for %%S in (taoskeeper taos-explorer taosadapter taosx taosd) do (
    sc query %%S | findstr /i "STATE" >nul
    if errorlevel 1 (
        echo %%S does not exist.
    ) else (
        for /f "tokens=3" %%T in ('sc query %%S ^| findstr STATE') do (
            if "%%T"=="4" (
                sc stop %%S >nul 2>&1
                echo Stop command sent to %%S.
            ) else (
                echo %%S is not running.
            )
        )
    )
)
echo All stop commands issued.