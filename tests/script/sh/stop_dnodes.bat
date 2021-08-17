@echo off

rem echo taskkill /F /IM taosd.exe

wmic process where "name='taosd.exe'" call terminate > NUL 2>&1
taskkill /F /IM taosd.exe > NUL 2>&1