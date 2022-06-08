@echo off
SETLOCAL EnableDelayedExpansion
for /F "tokens=1,2 delims=#" %%a in ('"prompt #$H#$E# & echo on & for %%b in (1) do     rem"') do (  set "DEL=%%a")
set /a a=0
@REM echo Windows Taosd Test
@REM for /F "usebackq tokens=*" %%i in (fulltest.bat) do (
@REM     echo Processing %%i
@REM     set /a a+=1
@REM     call %%i ARG1 > result_!a!.txt 2>error_!a!.txt
@REM     if errorlevel 1 ( call :colorEcho 0c "failed" &echo. && exit 8 ) else ( call :colorEcho 0a "Success" &echo. )
@REM )
echo Linux Taosd Test
for /F "usebackq tokens=*" %%i in (fulltest.bat) do (
    for /f "tokens=1* delims= " %%a in ("%%i") do if not "%%a" == "@REM" (
        echo Processing %%i
        call :GetTimeSeconds %time%
        set time1=!_timeTemp!
        echo Start at %time%
        set /a a+=1
        call %%i ARG1 -m %1 > result_!a!.txt 2>error_!a!.txt
        if errorlevel 1 ( call :colorEcho 0c "failed" &echo. && echo result: && cat result_!a!.txt && echo error: && cat error_!a!.txt && exit 8 ) else ( call :colorEcho 0a "Success" &echo. ) 
    )
)
exit

:colorEcho
call :GetTimeSeconds %time%
set time2=%_timeTemp%
set /a interTime=%time2% - %time1%
echo End at %time% , cast %interTime%s
echo off
<nul set /p ".=%DEL%" > "%~2"
findstr /v /a:%1 /R "^$" "%~2" nul
del "%~2" > nul 2>&1i
goto :eof

:GetTimeSeconds
set tt=%1
set tt=%tt:.= %
set tt=%tt::= %
set index=1
for %%a in (%tt%) do (
    if !index! EQU 1 (
        set hh=%%a
    )^
    else if  !index! EQU 2 (
        set mm=%%a
 
    )^
    else if  !index! EQU 3 (
        set ss=%%a
    )
   set /a index=index+1
)
set /a _timeTemp=(%hh%*60+%mm%)*60+%ss%
goto :eof