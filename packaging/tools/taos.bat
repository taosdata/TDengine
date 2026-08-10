@echo off
set "APP_DIR=%~dp0.."
cd /d "%APP_DIR%"
if not "%1" == "" (
    %1 --help 
    @cmd /k
)
