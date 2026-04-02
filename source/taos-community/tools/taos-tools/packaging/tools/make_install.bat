@echo off
goto %1
:needAdmin

set source_dir=%2
set source_dir=%source_dir:/=\\%
set binary_dir=%3
set binary_dir=%binary_dir:/=\\%
set osType=%4

set tagert_dir=C:\\TDengine

if not exist %tagert_dir% (
    mkdir %tagert_dir%
)

if exist %binary_dir%\\build\\bin\\taosdump.exe (
    copy %binary_dir%\\build\\bin\\taosdump.exe %tagert_dir% > nul
)

if exist %binary_dir%\\build\\bin\\taosBenchmark.exe (
    copy %binary_dir%\\build\\bin\\taosBenchmark.exe %tagert_dir% > nul
)
