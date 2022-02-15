set sed="C:\Program Files\Git\usr\bin\sed.exe"
set community_dir=%1

::cmake\install.inc
%sed% -i "s/C:\/TDengine/C:\/KingHistorian/g" %community_dir%\cmake\install.inc
%sed% -i "s/taos\.cfg/kinghistorian\.cfg/g" %community_dir%\cmake\install.inc
%sed% -i "s/taos\.exe/khclient\.exe/g" %community_dir%\cmake\install.inc
%sed% -i "s/taosdemo\.exe/khdemo\.exe/g" %community_dir%\cmake\install.inc
%sed% -i "/src\/connector/d" %community_dir%\cmake\install.inc
%sed% -i "/tests\/examples/d" %community_dir%\cmake\install.inc
::src\kit\shell\CMakeLists.txt
%sed% -i "s/OUTPUT_NAME taos/OUTPUT_NAME khclient/g" %community_dir%\src\kit\shell\CMakeLists.txt
::src\kit\shell\inc\shell.h
%sed% -i "s/taos_history/kh_history/g" %community_dir%\src\kit\shell\inc\shell.h
::src\inc\taosdef.h
%sed% -i "s/\"taosdata\"/\"khroot\"/g" %community_dir%\src\inc\taosdef.h
::src\util\src\tconfig.c
%sed% -i "s/taos\.cfg/kinghistorian\.cfg/g"  %community_dir%\src\util\src\tconfig.c
%sed% -i "s/etc\/taos/etc\/kinghistorian/g"   %community_dir%\src\util\src\tconfig.c
::src\kit\taosdemo\CMakeLists.txt
%sed% -i "s/ADD_EXECUTABLE(taosdemo/ADD_EXECUTABLE(khdemo/g" %community_dir%\src\kit\taosdemo\CMakeLists.txt
%sed% -i "s/TARGET_LINK_LIBRARIES(taosdemo/TARGET_LINK_LIBRARIES(khdemo/g" %community_dir%\src\kit\taosdemo\CMakeLists.txt
::src\kit\taosdemo\taosdemo.c
%sed% -i "s/taosdemo --help/khdemo --help/g" %community_dir%\src\kit\taosdemo\taosdemo.c
%sed% -i "s/taosdemo --usage/khdemo --usage/g" %community_dir%\src\kit\taosdemo\taosdemo.c
%sed% -i "s/Usage: taosdemo/Usage: khdemo/g" %community_dir%\src\kit\taosdemo\taosdemo.c
%sed% -i "s/taosdemo is simulating/khdemo is simulating/g" %community_dir%\src\kit\taosdemo\taosdemo.c
%sed% -i "s/taosdemo version/khdemo version/g" %community_dir%\src\kit\taosdemo\taosdemo.c
%sed% -i "s/\"taosdata\"/\"khroot\"/g" %community_dir%\src\kit\taosdemo\taosdemo.c
%sed% -i "s/support@taosdata\.com/support@wellintech\.com/g" %community_dir%\src\kit\taosdemo\taosdemo.c
%sed% -i "s/taosc, rest, and stmt/khclient, rest, and stmt/g" %community_dir%\src\kit\taosdemo\taosdemo.c
%sed% -i "s/taosdemo uses/khdemo uses/g" %community_dir%\src\kit\taosdemo\taosdemo.c
%sed% -i "s/use 'taosc'/use 'khclient'/g" %community_dir%\src\kit\taosdemo\taosdemo.c
::src\util\src\tlog.c
%sed% -i "s/log\/taos/log\/kinghistorian/g"   %community_dir%\src\util\src\tlog.c
::src\dnode\src\dnodeSystem.c
%sed% -i "s/TDengine/KingHistorian/g"  %community_dir%\src\dnode\src\dnodeSystem.c
::src\dnode\src\dnodeMain.c
%sed% -i "s/TDengine/KingHistorian/g"   %community_dir%\src\dnode\src\dnodeMain.c
%sed% -i "s/taosdlog/khserverlog/g"  %community_dir%\src\dnode\src\dnodeMain.c
::src\client\src\tscSystem.c
%sed% -i "s/taoslog/khclientlog/g"  %community_dir%\src\client\src\tscSystem.c
::src\util\src\tnote.c
%sed% -i "s/taosinfo/khinfo/g"  %community_dir%\src\util\src\tnote.c
::src\dnode\CMakeLists.txt
%sed% -i "s/taos\.cfg/kinghistorian\.cfg/g"  %community_dir%\src\dnode\CMakeLists.txt
::src\kit\taosdump\taosdump.c
%sed% -i "s/support@taosdata\.com/support@wellintech\.com/g" %community_dir%\src\kit\taosdump\taosdump.c
%sed% -i "s/Default is taosdata/Default is khroot/g" %community_dir%\src\kit\taosdump\taosdump.c
%sed% -i "s/\"taosdata\"/\"khroot\"/g" %community_dir%\src\kit\taosdump\taosdump.c
%sed% -i "s/TDengine/KingHistorian/g" %community_dir%\src\kit\taosdump\taosdump.c
%sed% -i "s/taos\/taos\.cfg/kinghistorian\/kinghistorian\.cfg/g" %community_dir%\src\kit\taosdump\taosdump.c
::src\os\src\linux\linuxEnv.c
%sed% -i "s/etc\/taos/etc\/kinghistorian/g" %community_dir%\src\os\src\linux\linuxEnv.c
%sed% -i "s/lib\/taos/lib\/kinghistorian/g" %community_dir%\src\os\src\linux\linuxEnv.c
%sed% -i "s/log\/taos/log\/kinghistorian/g" %community_dir%\src\os\src\linux\linuxEnv.c
::src\kit\shell\src\shellDarwin.c
%sed% -i "s/TDengine shell/KingHistorian shell/g" %community_dir%\src\kit\shell\src\shellDarwin.c
%sed% -i "s/2020 by TAOS Data/2021 by Wellintech/g" %community_dir%\src\kit\shell\src\shellDarwin.c
::src\kit\shell\src\shellLinux.c
%sed% -i "s/support@taosdata\.com/support@wellintech\.com/g" %community_dir%\src\kit\shell\src\shellLinux.c
%sed% -i "s/TDengine shell/KingHistorian shell/g" %community_dir%\src\kit\shell\src\shellLinux.c
%sed% -i "s/2020 by TAOS Data/2021 by Wellintech/g" %community_dir%\src\kit\shell\src\shellLinux.c
::src\os\src\windows\wEnv.c
%sed% -i "s/TDengine/KingHistorian/g" %community_dir%\src\os\src\windows\wEnv.c
::src\kit\shell\src\shellEngine.c
%sed% -i "s/TDengine shell/KingHistorian shell/g" %community_dir%\src\kit\shell\src\shellEngine.c
%sed% -i "s/2020 by TAOS Data, Inc/2021 by Wellintech, Inc/g" %community_dir%\src\kit\shell\src\shellEngine.c
%sed% -i "s/taos connect failed/kh connect failed/g" %community_dir%\src\kit\shell\src\shellEngine.c
%sed% -i "s/\"taos^> \"/\"khclient^> \"/g" %community_dir%\src\kit\shell\src\shellEngine.c
%sed% -i "s/\"   -^> \"/\"       -^> \"/g" %community_dir%\src\kit\shell\src\shellEngine.c
%sed% -i "s/prompt_size = 6/prompt_size = 10/g" %community_dir%\src\kit\shell\src\shellEngine.c
::src\rpc\src\rpcMain.c
%sed% -i "s/taos connections/kh connections/g" %community_dir%\src\rpc\src\rpcMain.c
::src\plugins\monitor\src\monMain.c
%sed% -i "s/taosd is quiting/khserver is quiting/g" %community_dir%\src\plugins\monitor\src\monMain.c