set sed="C:\Program Files\Git\usr\bin\sed.exe"
set community_dir=%1

::cmake\install.inc
%sed% -i "s/C:\/TDengine/C:\/jh_iot/g" %community_dir%\cmake\install.inc
%sed% -i "s/taos\.cfg/jh_taos\.cfg/g" %community_dir%\cmake\install.inc
%sed% -i "s/taos\.exe/jh_taos\.exe/g" %community_dir%\cmake\install.inc
%sed% -i "s/taosdemo\.exe/jhdemo\.exe/g" %community_dir%\cmake\install.inc
%sed% -i "/src\/connector/d" %community_dir%\cmake\install.inc
%sed% -i "/tests\/examples/d" %community_dir%\cmake\install.inc
::src\kit\shell\CMakeLists.txt
%sed% -i "s/OUTPUT_NAME taos/OUTPUT_NAME jh_taos/g" %community_dir%\src\kit\shell\CMakeLists.txt
::src\kit\shell\inc\shell.h
%sed% -i "s/taos_history/jh_taos_history/g" %community_dir%\src\kit\shell\inc\shell.h
::src\inc\taosdef.h
%sed% -i "s/\"taosdata\"/\"jhdata\"/g" %community_dir%\src\inc\taosdef.h
::src\util\src\tconfig.c
%sed% -i "s/taos\.cfg/jh_taos\.cfg/g"  %community_dir%\src\util\src\tconfig.c
%sed% -i "s/etc\/taos/etc\/jh_taos/g"   %community_dir%\src\util\src\tconfig.c
::src\kit\taosdemo\CMakeLists.txt
%sed% -i "s/ADD_EXECUTABLE(taosdemo/ADD_EXECUTABLE(jhdemo/g" %community_dir%\src\kit\taosdemo\CMakeLists.txt
%sed% -i "s/TARGET_LINK_LIBRARIES(taosdemo/TARGET_LINK_LIBRARIES(jhdemo/g" %community_dir%\src\kit\taosdemo\CMakeLists.txt
::src\kit\taosdemo\taosdemo.c
%sed% -i "s/taosdemo --help/jhdemo --help/g" %community_dir%\src\kit\taosdemo\taosdemo.c
%sed% -i "s/taosdemo --usage/jhdemo --usage/g" %community_dir%\src\kit\taosdemo\taosdemo.c
%sed% -i "s/Usage: taosdemo/Usage: jhdemo/g" %community_dir%\src\kit\taosdemo\taosdemo.c
%sed% -i "s/taosdemo is simulating/jhdemo is simulating/g" %community_dir%\src\kit\taosdemo\taosdemo.c
%sed% -i "s/taosdemo version/jhdemo version/g" %community_dir%\src\kit\taosdemo\taosdemo.c
%sed% -i "s/\"taosdata\"/\"jhdata\"/g" %community_dir%\src\kit\taosdemo\taosdemo.c
%sed% -i "s/support@taosdata\.com/jhkj@njsteel\.com\.cn/g" %community_dir%\src\kit\taosdemo\taosdemo.c
%sed% -i "s/taosc, rest, and stmt/jh_taos, rest, and stmt/g" %community_dir%\src\kit\taosdemo\taosdemo.c
%sed% -i "s/taosdemo uses/jhdemo uses/g" %community_dir%\src\kit\taosdemo\taosdemo.c
%sed% -i "s/use 'taosc'/use 'jh_taos'/g" %community_dir%\src\kit\taosdemo\taosdemo.c
::src\util\src\tlog.c
%sed% -i "s/log\/taos/log\/jh_taos/g"   %community_dir%\src\util\src\tlog.c
::src\dnode\src\dnodeSystem.c
%sed% -i "s/TDengine/jh_iot/g"  %community_dir%\src\dnode\src\dnodeSystem.c
::src\dnode\src\dnodeMain.c
%sed% -i "s/TDengine/jh_iot/g"   %community_dir%\src\dnode\src\dnodeMain.c
%sed% -i "s/taosdlog/jh_taosdlog/g"  %community_dir%\src\dnode\src\dnodeMain.c
::src\client\src\tscSystem.c
%sed% -i "s/taoslog/jh_taoslog/g"  %community_dir%\src\client\src\tscSystem.c
::src\util\src\tnote.c
%sed% -i "s/taosinfo/jh_taosinfo/g"  %community_dir%\src\util\src\tnote.c
::src\dnode\CMakeLists.txt
%sed% -i "s/taos\.cfg/jh_taos\.cfg/g"  %community_dir%\src\dnode\CMakeLists.txt
::src\kit\taosdump\taosdump.c
%sed% -i "s/support@taosdata\.com/jhkj@njsteel\.com\.cn/g" %community_dir%\src\kit\taosdump\taosdump.c
%sed% -i "s/Default is taosdata/Default is jhdata/g" %community_dir%\src\kit\taosdump\taosdump.c
%sed% -i "s/\"taosdata\"/\"jhdata\"/g" %community_dir%\src\kit\taosdump\taosdump.c
%sed% -i "s/TDengine/jh_iot/g" %community_dir%\src\kit\taosdump\taosdump.c
%sed% -i "s/taos\/taos\.cfg/jh_taos\/jh_taos\.cfg/g" %community_dir%\src\kit\taosdump\taosdump.c
::src\os\src\linux\linuxEnv.c
%sed% -i "s/etc\/taos/etc\/jh_taos/g" %community_dir%\src\os\src\linux\linuxEnv.c
%sed% -i "s/lib\/taos/lib\/jh_taos/g" %community_dir%\src\os\src\linux\linuxEnv.c
%sed% -i "s/log\/taos/log\/jh_taos/g" %community_dir%\src\os\src\linux\linuxEnv.c
::src\kit\shell\src\shellDarwin.c
%sed% -i "s/TDengine shell/jh_iot shell/g" %community_dir%\src\kit\shell\src\shellDarwin.c
%sed% -i "s/2020 by TAOS Data/2021 by Jinheng Technology/g" %community_dir%\src\kit\shell\src\shellDarwin.c
::src\kit\shell\src\shellLinux.c
%sed% -i "s/support@taosdata\.com/jhkj@njsteel\.com\.cn/g" %community_dir%\src\kit\shell\src\shellLinux.c
%sed% -i "s/TDengine shell/jh_iot shell/g" %community_dir%\src\kit\shell\src\shellLinux.c
%sed% -i "s/2020 by TAOS Data/2021 by Jinheng Technology/g" %community_dir%\src\kit\shell\src\shellLinux.c
::src\os\src\windows\wEnv.c
%sed% -i "s/TDengine/jh_iot/g" %community_dir%\src\os\src\windows\wEnv.c
::src\kit\shell\src\shellEngine.c
%sed% -i "s/TDengine shell/jh_iot shell/g" %community_dir%\src\kit\shell\src\shellEngine.c
%sed% -i "s/2020 by TAOS Data, Inc/2021 by Jinheng Technology, Inc/g" %community_dir%\src\kit\shell\src\shellEngine.c
%sed% -i "s/taos connect failed/jh_taos connect failed/g" %community_dir%\src\kit\shell\src\shellEngine.c
%sed% -i "s/\"taos^> \"/\"jh_taos^> \"/g" %community_dir%\src\kit\shell\src\shellEngine.c
%sed% -i "s/\"   -^> \"/\"      -^> \"/g" %community_dir%\src\kit\shell\src\shellEngine.c
%sed% -i "s/prompt_size = 6/prompt_size = 9/g" %community_dir%\src\kit\shell\src\shellEngine.c
::src\rpc\src\rpcMain.c
%sed% -i "s/taos connections/jh_taos connections/g" %community_dir%\src\rpc\src\rpcMain.c
::src\plugins\monitor\src\monMain.c
%sed% -i "s/taosd is quiting/jh_taosd is quiting/g" %community_dir%\src\plugins\monitor\src\monMain.c
