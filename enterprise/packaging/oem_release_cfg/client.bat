@echo off
cd C:\TDengine
if not "%1" == "" (
    %1 --help 
    @cmd /k
)