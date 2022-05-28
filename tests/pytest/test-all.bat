@echo off
SETLOCAL EnableDelayedExpansion
for /F "tokens=1,2 delims=#" %%a in ('"prompt #$H#$E# & echo on & for %%b in (1) do     rem"') do (  set "DEL=%%a")
set /a a=0
@REM echo Windows Taosd Test
@REM for /F "usebackq tokens=*" %%i in (fulltest.bat) do (
@REM     echo Processing %%i
@REM     set /a a+=1
@REM     call %%i ARG1 -w -m localhost > result_!a!.txt 2>error_!a!.txt
@REM     if errorlevel 1 ( call :colorEcho 0c "failed" &echo. && exit 8 ) else ( call :colorEcho 0a "Success" &echo. )
@REM )
echo Linux Taosd Test
for /F "usebackq tokens=*" %%i in (fulltest.bat) do (
    echo Processing %%i
    set /a a+=1
    call %%i ARG1 -w 1 -m %1 > result_!a!.txt 2>error_!a!.txt
    if errorlevel 1 ( call :colorEcho 0c "failed" &echo. && goto :end ) else ( call :colorEcho 0a "Success" &echo. ) 
)
goto :end

:colorEcho
echo off
<nul set /p ".=%DEL%" > "%~2"
findstr /v /a:%1 /R "^$" "%~2" nul
del "%~2" > nul 2>&1i

:end