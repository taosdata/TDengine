@echo off

echo.
cd C:\ProDB
if not "%1" == "" (
    %1 --help 
    @cmd /k
)