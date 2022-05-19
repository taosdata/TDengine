@echo off
goto %1
:needAdmin
mshta vbscript:createobject("shell.application").shellexecute("%~s0",":hasAdmin","","runas",1)(window.close)&goto :eof
:hasAdmin
cp -f C:\\TDengine\\driver\\taos.dll C:\\Windows\\System32