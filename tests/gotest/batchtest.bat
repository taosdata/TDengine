@echo off
echo ==== start Go connector test cases test ====
cd /d %~dp0

set severIp=%1
set serverPort=%2
if "%severIp%"=="" (set severIp=127.0.0.1)
if "%serverPort%"=="" (set serverPort=6030)

go env -w GO111MODULE=on
go env -w GOPROXY=https://goproxy.io,direct

cd case001
case001.bat %severIp% %serverPort%  

rem cd case002
rem case002.bat

:: cd case002
:: case002.bat
