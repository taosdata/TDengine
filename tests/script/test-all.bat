@echo off
SETLOCAL EnableDelayedExpansion
for /F "tokens=1,2 delims=#" %%a in ('"prompt #$H#$E# & echo on & for %%b in (1) do     rem"') do (  set "DEL=%%a")
set /a a=0
echo Windows Taosd Full Test
set /a exitNum=0
rm -rf failed.txt
set caseFile="jenkins\\basic.txt"
if not "%2" == "" (
    set caseFile="%2"
)
for /F "usebackq tokens=*" %%i in (!caseFile!) do (
    set line=%%i
    call :CheckSkipCase %%i
    if !skipCase! == false (
        if "!line:~,9!" == "./test.sh" (
            set /a a+=1
            echo !a! Processing %%i
            call :GetTimeSeconds !time!
            set time1=!_timeTemp!
            echo Start at !time!
            call !line:./test.sh=wtest.bat! > result_!a!.txt 2>error_!a!.txt || set /a errorlevel=8
            if errorlevel 1 ( call :colorEcho 0c "failed" &echo. && set /a exitNum=8 && echo %%i >>failed.txt ) else ( call :colorEcho 0a "Success" &echo. )
        )
    )
)
exit /b !exitNum!

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
@REM if "%*" == "./test.sh -f tsim/query/scalarFunction.sim" ( set skipCase=true )
echo %* | grep valgrind && set skipCase=true
:goto eof