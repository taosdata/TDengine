@echo off
goto %1
:needAdmin

if exist C:\\TDengine\\data\\dnode\\dnodeCfg.json (
  echo The default data directory C:/TDengine/data contains old data of tdengine 2.x, please clear it before installing!
)
set source_dir=%2
set source_dir=%source_dir:/=\\%
set binary_dir=%3
set binary_dir=%binary_dir:/=\\%
set osType=%4
set verNumber=%5
set tagert_dir=C:\\TDengine

if not exist %tagert_dir% (
    mkdir %tagert_dir%
)
if not exist %tagert_dir%\\cfg (
    mkdir %tagert_dir%\\cfg
)
if not exist %tagert_dir%\\include (
    mkdir %tagert_dir%\\include
)
if not exist %tagert_dir%\\driver (
    mkdir %tagert_dir%\\driver
)
if not exist C:\\TDengine\\cfg\\taos.cfg (
    copy %source_dir%\\packaging\\cfg\\taos.cfg %tagert_dir%\\cfg\\taos.cfg > nul
)

if exist %binary_dir%\\test\\cfg\\taosadapter.toml (
    if not exist %tagert_dir%\\cfg\\taosadapter.toml (
        copy %binary_dir%\\test\\cfg\\taosadapter.toml %tagert_dir%\\cfg\\taosadapter.toml > nul
    )
)

copy %source_dir%\\include\\client\\taos.h %tagert_dir%\\include > nul
copy %source_dir%\\include\\util\\taoserror.h %tagert_dir%\\include > nul
copy %source_dir%\\include\\libs\\function\\taosudf.h %tagert_dir%\\include > nul
copy %binary_dir%\\build\\lib\\taos.lib %tagert_dir%\\driver > nul
copy %binary_dir%\\build\\lib\\taos_static.lib %tagert_dir%\\driver > nul
copy %binary_dir%\\build\\lib\\taos.dll %tagert_dir%\\driver > nul
copy %binary_dir%\\build\\bin\\taos.exe %tagert_dir% > nul
copy %binary_dir%\\build\\bin\\taosd.exe %tagert_dir% > nul
copy %binary_dir%\\build\\bin\\udfd.exe %tagert_dir% > nul
if exist %binary_dir%\\build\\bin\\taosBenchmark.exe (
    copy %binary_dir%\\build\\bin\\taosBenchmark.exe %tagert_dir% > nul
)
if exist %binary_dir%\\build\\bin\\taosadapter.exe (
    copy %binary_dir%\\build\\bin\\taosadapter.exe %tagert_dir% > nul
)

mshta vbscript:createobject("shell.application").shellexecute("%~s0",":hasAdmin","","runas",1)(window.close)&& echo To start/stop TDengine with administrator privileges: sc start/stop taosd &goto :eof
:hasAdmin
copy /y C:\\TDengine\\driver\\taos.dll C:\\Windows\\System32 > nul
sc query "taosd" >nul || sc create "taosd" binPath= "C:\\TDengine\\taosd.exe --win_service" start= DEMAND
