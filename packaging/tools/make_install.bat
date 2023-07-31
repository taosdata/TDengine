@echo off

for /F %%a in ('echo prompt $E ^| cmd') do set "ESC=%%a"

if "%1" NEQ "" goto %1

:needAdmin

if exist C:\\TDengine\\data\\dnode\\dnodeCfg.json (
  echo The default data directory C:/TDengine/data contains old data of tdengine 2.x, please clear it before installing!
)

rem // stop and delete service
mshta vbscript:createobject("shell.application").shellexecute("%~s0",":stop_delete","","runas",1)(window.close)
echo This might take a few moment to accomplish deleting service taosd/taosadapter ...
call :check_svc taosd
call :check_svc taosadapter

set source_dir=%2
set source_dir=%source_dir:/=\\%
set binary_dir=%3
set binary_dir=%binary_dir:/=\\%
set osType=%4
set verNumber=%5
set Enterprise=%6
set target_dir=C:\\TDengine

if not exist %target_dir% (
    mkdir %target_dir%
)
if not exist %target_dir%\\cfg (
    mkdir %target_dir%\\cfg
)
if not exist %target_dir%\\include (
    mkdir %target_dir%\\include
)
if not exist %target_dir%\\driver (
    mkdir %target_dir%\\driver
)
if not exist C:\\TDengine\\cfg\\taos.cfg (
    copy %source_dir%\\packaging\\cfg\\taos.cfg %target_dir%\\cfg\\taos.cfg > nul
)

if exist %binary_dir%\\test\\cfg\\taosadapter.toml (
    if not exist %target_dir%\\cfg\\taosadapter.toml (
        copy %binary_dir%\\test\\cfg\\taosadapter.toml %target_dir%\\cfg\\taosadapter.toml > nul
    )
)
copy %source_dir%\\include\\client\\taos.h %target_dir%\\include > nul
copy %source_dir%\\include\\util\\taoserror.h %target_dir%\\include > nul
copy %source_dir%\\include\\libs\\function\\taosudf.h %target_dir%\\include > nul
copy %binary_dir%\\build\\lib\\taos.lib %target_dir%\\driver > nul
copy %binary_dir%\\build\\lib\\taos_static.lib %target_dir%\\driver > nul
copy %binary_dir%\\build\\lib\\taos.dll %target_dir%\\driver > nul
copy %binary_dir%\\build\\bin\\taos.exe %target_dir% > nul
if exist %binary_dir%\\build\\bin\\taosBenchmark.exe (
    copy %binary_dir%\\build\\bin\\taosBenchmark.exe %target_dir% > nul
)
if exist %binary_dir%\\build\\lib\\taosws.lib (
    copy %binary_dir%\\build\\lib\\taosws.lib %target_dir%\\driver  > nul
)
if exist %binary_dir%\\build\\lib\\taosws.dll (
    copy %binary_dir%\\build\\lib\\taosws.dll %target_dir%\\driver  > nul
    copy %source_dir%\\tools\\taosws-rs\\target\\release\\taosws.h %target_dir%\\include > nul
)
if exist %binary_dir%\\build\\bin\\taosdump.exe (
    copy %binary_dir%\\build\\bin\\taosdump.exe %target_dir% > nul
)
if %Enterprise% == TRUE (
    if exist %binary_dir%\\build\\bin\\taosx.exe (
        copy %binary_dir%\\build\\bin\\taosx.exe %target_dir% > nul
    )
    if exist %binary_dir%\\build\\bin\\taos-explorer.exe (
        copy %binary_dir%\\build\\bin\\taos-explorer.exe %target_dir% > nul
    )
    if exist %binary_dir%\\build\\bin\\tmq_sim.exe (
        copy %binary_dir%\\build\\bin\\tmq_sim.exe %target_dir% > nul
    )
    if exist %binary_dir%\\build\\bin\\tsim.exe (
        copy %binary_dir%\\build\\bin\\tsim.exe %target_dir% > nul
    )
    if exist %binary_dir%\\build\\bin\\tmq_taosx_ci.exe (
        copy %binary_dir%\\build\\bin\\tmq_taosx_ci.exe %target_dir% > nul
    )
    if exist %binary_dir%\\build\\bin\\tmq_demo.exe (
        copy %binary_dir%\\build\\bin\\tmq_demo.exe %target_dir% > nul
    )
    if exist %binary_dir%\\build\\bin\\dumper.exe (
        copy %binary_dir%\\build\\bin\\dumper.exe %target_dir% > nul
    )
    if exist %binary_dir%\\build\\bin\\runUdf.exe (
        copy %binary_dir%\\build\\bin\\runUdf.exe %target_dir% > nul
    )
    if exist %binary_dir%\\build\\bin\\create_table.exe (
        copy %binary_dir%\\build\\bin\\create_table.exe %target_dir% > nul
    )
    if exist %binary_dir%\\build\\bin\\*explorer.exe (
        copy %binary_dir%\\build\\bin\\*explorer.exe %target_dir% > nul
    )
)
    
copy %binary_dir%\\build\\bin\\taosd.exe %target_dir% > nul
copy %binary_dir%\\build\\bin\\udfd.exe %target_dir% > nul
if exist %binary_dir%\\build\\bin\\taosadapter.exe (
    copy %binary_dir%\\build\\bin\\taosadapter.exe %target_dir% > nul
)

mshta vbscript:createobject("shell.application").shellexecute("%~s0",":hasAdmin","","runas",1)(window.close)

echo.
echo Please manually remove C:\TDengine from your system PATH environment after you remove TDengine software
echo.
echo To start/stop TDengine with administrator privileges:  %ESC%[92msc start/stop taosd %ESC%[0m

if exist %binary_dir%\\build\\bin\\taosadapter.exe (
    echo To start/stop taosAdapter with administrator privileges: %ESC%[92msc start/stop taosadapter %ESC%[0m
)

goto :eof

:hasAdmin

call :stop_delete
call :check_svc taosd
call :check_svc taosadapter

if exist c:\\windows\\sysnative (
    echo x86
    copy /y C:\\TDengine\\driver\\taos.dll %windir%\\sysnative > nul
    if exist C:\\TDengine\\driver\\taosws.dll (
        copy /y C:\\TDengine\\driver\\taosws.dll %windir%\\sysnative > nul
    )
) else (
    echo x64
    copy /y C:\\TDengine\\driver\\taos.dll C:\\Windows\\System32 > nul
    if exist C:\\TDengine\\driver\\taosws.dll (
        copy /y C:\\TDengine\\driver\\taosws.dll C:\\Windows\\System32 > nul
    )
)

rem // create services
sc create "taosd" binPath= "C:\\TDengine\\taosd.exe --win_service" start= DEMAND
sc create "taosadapter" binPath= "C:\\TDengine\\taosadapter.exe" start= DEMAND

set "env=HKLM\System\CurrentControlSet\Control\Session Manager\Environment"
for /f "tokens=2*" %%I in ('reg query "%env%" /v Path ^| findstr /i "\<Path\>"') do (

    call :append_if_not_exists %%J

    rem // apply change to the current process
    for %%a in ("%%J;C:\TDengine") do path %%~a
)

rem // use setx to set a temporary throwaway value to trigger a WM_SETTINGCHANGE
rem // applies change to new console windows without requiring a reboot
(setx /m foo bar & reg delete "%env%" /f /v foo) >NUL 2>NUL

goto :end

:append_if_not_exists
set "_origin_paths=%*"
set "_paths=%*"
set "_found=0"
:loop
for /f "tokens=1* delims=;" %%x in ("%_paths%") do (
    if "%%x" EQU "C:\TDengine" (
      set "_found=1"
    ) else (
      set "_paths=%%y"
      goto :loop
    )
)
if "%_found%" == "0" (
  rem // make addition persistent through reboots
  reg add "%env%" /f /v Path /t REG_EXPAND_SZ /d "%_origin_paths%;C:\TDengine"
)
exit /B 0

:stop_delete
sc stop taosd
sc delete taosd
sc stop taosadapter
sc delete taosadapter
exit /B 0

:check_svc
sc query %1 >nul 2>nul && goto :check_svc %1
exit /B 0

:end
