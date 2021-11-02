@echo off
SETLOCAL EnableDelayedExpansion
for /F "tokens=1,2 delims=#" %%a in ('"prompt #$H#$E# & echo on & for %%b in (1) do     rem"') do (  set "DEL=%%a")
for /F "usebackq tokens=*" %%i in (fulltest.bat) do (
    echo Processing %%i
    call %%i ARG1 -w 1 -m %1 > result.txt 2>error.txt
    if errorlevel 1 ( call :colorEcho 0c "failed" &echo. && exit 8 ) else ( call :colorEcho 0a "Success" &echo. )   
)
exit

:colorEcho
echo off
<nul set /p ".=%DEL%" > "%~2"
findstr /v /a:%1 /R "^$" "%~2" nul
del "%~2" > nul 2>&1i