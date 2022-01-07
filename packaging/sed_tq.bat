set sed="C:\Program Files\Git\usr\bin\sed.exe"
set community_dir=%1

::cmake\install.inc
%sed% -i "s/C:\/TDengine/C:\/TQ/g" %community_dir%\cmake\install.inc
%sed% -i "s/taos\.cfg/tq\.cfg/g" %community_dir%\cmake\install.inc
%sed% -i "s/taos\.exe/tq\.exe/g" %community_dir%\cmake\install.inc
%sed% -i "/src\/connector/d" %community_dir%\cmake\install.inc
%sed% -i "/tests\/examples/d" %community_dir%\cmake\install.inc
::src\kit\shell\CMakeLists.txt
%sed% -i "s/OUTPUT_NAME taos/OUTPUT_NAME tq/g" %community_dir%\src\kit\shell\CMakeLists.txt
::src\kit\shell\inc\shell.h
%sed% -i "s/taos_history/tq_history/g" %community_dir%\src\kit\shell\inc\shell.h
::src\inc\taosdef.h
%sed% -i "s/\"taosdata\"/\"tqueue\"/g" %community_dir%\src\inc\taosdef.h
::src\util\src\tconfig.c
%sed% -i "s/taos\.cfg/tq\.cfg/g"  %community_dir%\src\util\src\tconfig.c
%sed% -i "s/etc\/taos/etc\/tq/g"   %community_dir%\src\util\src\tconfig.c
::src\util\src\tlog.c
%sed% -i "s/log\/taos/log\/tq/g"   %community_dir%\src\util\src\tlog.c
::src\dnode\src\dnodeSystem.c
%sed% -i "s/TDengine/TQ/g"  %community_dir%\src\dnode\src\dnodeSystem.c
::src\dnode\src\dnodeMain.c
%sed% -i "s/TDengine/TQ/g"   %community_dir%\src\dnode\src\dnodeMain.c
%sed% -i "s/taosdlog/tqdlog/g"  %community_dir%\src\dnode\src\dnodeMain.c
::src\client\src\tscSystem.c
%sed% -i "s/taoslog/tqlog/g"  %community_dir%\src\client\src\tscSystem.c
::src\util\src\tnote.c
%sed% -i "s/taosinfo/tqinfo/g"  %community_dir%\src\util\src\tnote.c
::src\dnode\CMakeLists.txt
%sed% -i "s/taos\.cfg/tq\.cfg/g"  %community_dir%\src\dnode\CMakeLists.txt
::src\os\src\linux\linuxEnv.c
%sed% -i "s/etc\/taos/etc\/tq/g" %community_dir%\src\os\src\linux\linuxEnv.c
%sed% -i "s/lib\/taos/lib\/tq/g" %community_dir%\src\os\src\linux\linuxEnv.c
%sed% -i "s/log\/taos/log\/tq/g" %community_dir%\src\os\src\linux\linuxEnv.c
::src\os\src\windows\wEnv.c
%sed% -i "s/TDengine/TQ/g" %community_dir%\src\os\src\windows\wEnv.c
::src\kit\shell\src\shellEngine.c
%sed% -i "s/TDengine shell/TQ shell/g" %community_dir%\src\kit\shell\src\shellEngine.c
%sed% -i "s/\"taos^> \"/\"tq^> \"/g" %community_dir%\src\kit\shell\src\shellEngine.c
%sed% -i "s/\"   -^> \"/\" -^> \"/g" %community_dir%\src\kit\shell\src\shellEngine.c
%sed% -i "s/prompt_size = 6/prompt_size = 4/g" %community_dir%\src\kit\shell\src\shellEngine.c
::src\rpc\src\rpcMain.c
%sed% -i "s/taos connections/tq connections/g" %community_dir%\src\rpc\src\rpcMain.c
::src\plugins\monitor\src\monMain.c
%sed% -i "s/taosd is quiting/tqd is quiting/g" %community_dir%\src\plugins\monitor\src\monMain.c
