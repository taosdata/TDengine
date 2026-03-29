@echo off

echo.
echo To use TDengine TSDB services, please run start-all.bat under C:\TDengine directory.
echo Supported actions: start-all.bat [start^|status^|stop]
echo Or you can use following instructions to edit configuration files and run commands manually in terminal as Administrator:
echo.
echo To configure TDengine TSDB:   edit C:\TDengine\cfg\taos.cfg
echo To configure taosadapter:     edit C:\TDengine\cfg\taosadapter.toml
echo To configure taos-explorer:   edit C:\TDengine\cfg\taos-explorer.cfg
echo To start taosd:               sc.exe start taosd
echo To start taosadapter:         sc.exe start taosadapter
echo To start taoskeeper:          sc.exe start taoskeeper
echo To start taosx:               sc.exe start taosx
echo To start taos-explorer:       sc.exe start taos-explorer

echo.
cd C:\TDengine
@cmd /k