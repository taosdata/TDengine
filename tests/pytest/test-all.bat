@echo off
echo locale en_US.UTF-8>"C:\\TDengine\\cfg\\taos.cfg"
echo charset UTF-8>>"C:\\TDengine\\cfg\\taos.cfg"
for /F "usebackq tokens=*" %%i in ( f.bat) do (
    echo Processing %%i
    call %%i ARG1 > result.txt 2>error.txt
    if errorlevel 1 (echo failed) else (echo sucess)   
)
@echo on