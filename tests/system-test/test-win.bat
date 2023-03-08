@echo off
SETLOCAL EnableDelayedExpansion
for /F "tokens=1,2 delims=#" %%a in ('"prompt #$H#$E# & echo on & for %%b in (1) do     rem"') do (  set "DEL=%%a")
set /a a=0
if "%1" == "full" (
    echo Windows Taosd Full Test
    set /a exitNum=0
    del /Q /F failed.txt
    set caseFile="win-test-file"
    if not "%2" == "" (
        set caseFile="%2"
    )
    for /F "usebackq tokens=*" %%i in (!caseFile!) do (
        call :CheckSkipCase %%i
        if !skipCase! == false (
            set line=%%i
            if "!line:~,7!" == "python3" (
                set /a a+=1
                echo !a! Processing %%i
                call :GetTimeSeconds !time!
                set time1=!_timeTemp!
                echo Start at !time!
                call %%i ARG1 > result_!a!.txt 2>error_!a!.txt || set errorlevel=8
                if errorlevel 1 ( call :colorEcho 0c "failed" &echo. && set /a exitNum=8 && echo %%i >>failed.txt ) else ( call :colorEcho 0a "Success" &echo. )
            )
        )
    )
    exit /b !exitNum!
)
echo Windows Taosd Test
for /F "usebackq tokens=*" %%i in (win-test-file) do (
    for /f "tokens=1* delims= " %%a in ("%%i") do if not "%%a" == "@REM" (
        set /a a+=1
        set timeNow=!time!
        echo !a! Processing %%i
        call :GetTimeSeconds !timeNow!
        set time1=!_timeTemp!
        echo Start at !timeNow!
        call %%i ARG1 > result_!a!.txt 2>error_!a!.txt || set errorlevel=8
        if errorlevel 1 ( call :colorEcho 0c "failed" &echo. && echo result: && cat result_!a!.txt && echo error: && cat error_!a!.txt && exit /b 8 ) else ( call :colorEcho 0a "Success" &echo. ) 
    )
)
@REM echo Linux Taosd Test
@REM for /F "usebackq tokens=*" %%i in (simpletest.bat) do (
@REM     for /f "tokens=1* delims= " %%a in ("%%i") do if not "%%a" == "@REM" (
@REM         set /a a+=1
@REM         echo !a! Processing %%i
@REM         call :GetTimeSeconds !time!
@REM         set time1=!_timeTemp!
@REM         echo Start at !time!
@REM         call %%i ARG1 -m %1 > result_!a!.txt 2>error_!a!.txt
@REM         if errorlevel 1 ( call :colorEcho 0c "failed" &echo. && echo result: && cat result_!a!.txt && echo error: && cat error_!a!.txt && exit 8 ) else ( call :colorEcho 0a "Success" &echo. ) 
@REM     )
@REM )
exit /b

:colorEcho
set timeNow=%time%
call :GetTimeSeconds %timeNow%
set time2=%_timeTemp%
set /a interTime=%time2% - %time1%
echo End at %timeNow% , cast %interTime%s
echo off
<nul set /p ".=%DEL%" > "%~2"
findstr /v /a:%1 /R "^$" "%~2" nul
del "%~2" > nul 2>&1i
goto :eof

:GetTimeSeconds
set tt=%1
set tt=%tt:.= %
set tt=%tt::= %
set tt=%tt: 0= %
set /a index=1
for %%a in (%tt%) do (
    if !index! EQU 1 (
        set /a hh=%%a
    )^
    else if  !index! EQU 2 (
        set /a mm=%%a
 
    )^
    else if  !index! EQU 3 (
        set /a ss=%%a
    )
   set /a index=index+1
)
set /a _timeTemp=(%hh%*60+%mm%)*60+%ss%
goto :eof

:CheckSkipCase
set skipCase=false
if "%*" == "python3 ./test.py -f 1-insert/insertWithMoreVgroup.py" ( set skipCase=false )
if "%*" == "python3 ./test.py -f 2-query/queryQnode.py" ( set skipCase=false )
echo %* | grep "\-R" && set skipCase=true
:goto eof