@echo off
REM install driver
odbcconf /A {INSTALLDRIVER "TAOS|Driver=C:\TDengine\driver\todbc.dll|ConnectFunctions=YYN|DriverODBCVer=03.00|FileUsage=0|SQLLevel=0"}
REM config user dsn
odbcconf /A {CONFIGDSN "TAOS" "DSN=TAOS_DSN"}

