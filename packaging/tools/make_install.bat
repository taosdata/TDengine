@echo off
goto %1
:needAdmin
mshta vbscript:createobject("shell.application").shellexecute("%~s0",":hasAdmin","","runas",1)(window.close)&& echo To start/stop TDengine with administrator privileges: sc start/stop taosd &goto :eof
:hasAdmin
cp -f C:\\TDengine\\driver\\taos.dll C:\\Windows\\System32
sc query "taosd" >nul || sc create "taosd" binPath= "C:\\TDengine\\taosd.exe --win_service" start= DEMAND
