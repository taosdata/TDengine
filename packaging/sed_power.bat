set sed="C:\Program Files\Git\usr\bin\sed.exe"
set community_dir=%1

::cmake\install.inc
%sed% -i "s/C:\/TDengine/C:\/Power/g" %community_dir%\cmake\install.inc
%sed% -i "s/taos\.cfg/power\.cfg/g" %community_dir%\cmake\install.inc
%sed% -i "s/taos\.exe/power\.exe/g" %community_dir%\cmake\install.inc
%sed% -i "/src\/connector/d" %community_dir%\cmake\install.inc
%sed% -i "/tests\/examples/d" %community_dir%\cmake\install.inc
::src\kit\shell\CMakeLists.txt
%sed% -i "s/OUTPUT_NAME taos/OUTPUT_NAME power/g" %community_dir%\src\kit\shell\CMakeLists.txt
::src\kit\shell\inc\shell.h
%sed% -i "s/taos_history/power_history/g" %community_dir%\src\kit\shell\inc\shell.h
::src\inc\taosdef.h
%sed% -i "s/\"taosdata\"/\"powerdb\"/g" %community_dir%\src\inc\taosdef.h
::src\util\src\tconfig.c
%sed% -i "s/taos\.cfg/power\.cfg/g"  %community_dir%\src\util\src\tconfig.c
%sed% -i "s/etc\/taos/etc\/power/g"   %community_dir%\src\util\src\tconfig.c
::src\util\src\tlog.c
%sed% -i "s/log\/taos/log\/power/g"   %community_dir%\src\util\src\tlog.c
::src\dnode\src\dnodeSystem.c
%sed% -i "s/TDengine/Power/g"  %community_dir%\src\dnode\src\dnodeSystem.c
::src\dnode\src\dnodeMain.c
%sed% -i "s/TDengine/Power/g"   %community_dir%\src\dnode\src\dnodeMain.c
%sed% -i "s/taosdlog/powerdlog/g"  %community_dir%\src\dnode\src\dnodeMain.c
::src\client\src\tscSystem.c
%sed% -i "s/taoslog/powerlog/g"  %community_dir%\src\client\src\tscSystem.c
::src\util\src\tnote.c
%sed% -i "s/taosinfo/powerinfo/g"  %community_dir%\src\util\src\tnote.c
::src\dnode\CMakeLists.txt
%sed% -i "s/taos\.cfg/power\.cfg/g"  %community_dir%\src\dnode\CMakeLists.txt
::src\os\src\linux\linuxEnv.c
%sed% -i "s/etc\/taos/etc\/power/g" %community_dir%\src\os\src\linux\linuxEnv.c
%sed% -i "s/lib\/taos/lib\/power/g" %community_dir%\src\os\src\linux\linuxEnv.c
%sed% -i "s/log\/taos/log\/power/g" %community_dir%\src\os\src\linux\linuxEnv.c
::src\os\src\windows\wEnv.c
%sed% -i "s/TDengine/Power/g" %community_dir%\src\os\src\windows\wEnv.c
::src\kit\shell\src\shellEngine.c
%sed% -i "s/TDengine shell/Power shell/g" %community_dir%\src\kit\shell\src\shellEngine.c
%sed% -i "s/2020 by TAOS Data, Inc/2020 by PowerDB, Inc/g" %community_dir%\src\kit\shell\src\shellEngine.c
%sed% -i "s/taos connect failed/power connect failed/g" %community_dir%\src\kit\shell\src\shellEngine.c
%sed% -i "s/\"taos^> \"/\"power^> \"/g" %community_dir%\src\kit\shell\src\shellEngine.c
%sed% -i "s/\"   -^> \"/\"    -^> \"/g" %community_dir%\src\kit\shell\src\shellEngine.c
%sed% -i "s/prompt_size = 6/prompt_size = 7/g" %community_dir%\src\kit\shell\src\shellEngine.c
::src\rpc\src\rpcMain.c
%sed% -i "s/taos connections/power connections/g" %community_dir%\src\rpc\src\rpcMain.c
::src\plugins\monitor\src\monMain.c
%sed% -i "s/taosd is quiting/powerd is quiting/g" %community_dir%\src\plugins\monitor\src\monMain.c
