#!/bin/bash

function replace_community_kh(){
  # cmake/install.inc
  sed -i "s/C:\/TDengine/C:\/KingHistorian/g" ${top_dir}/cmake/install.inc
  sed -i "s/taos\.cfg/kinghistorian\.cfg/g" ${top_dir}/cmake/install.inc
  sed -i "s/taos\.exe/khclient\.exe/g" ${top_dir}/cmake/install.inc
  sed -i "s/taosdemo\.exe/khdemo\.exe/g" ${top_dir}/cmake/install.inc
  # src/kit/shell/inc/shell.h
  sed -i "s/taos_history/kh_history/g" ${top_dir}/src/kit/shell/inc/shell.h
  # src/inc/taosdef.h
  sed -i "s/\"taosdata\"/\"khroot\"/g" ${top_dir}/src/inc/taosdef.h
  # src/util/src/tconfig.c
  sed -i "s/taos\.cfg/kinghistorian\.cfg/g"  ${top_dir}/src/util/src/tconfig.c
  sed -i "s/etc\/taos/etc\/kinghistorian/g"   ${top_dir}/src/util/src/tconfig.c
  # src/kit/taosdemo/taosdemo.c
  sed -i "s/taosdemo --help/khdemo --help/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/taosdemo --usage/khdemo --usage/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/Usage: taosdemo/Usage: khdemo/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/taosdemo is simulating/khdemo is simulating/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/taosdemo version/khdemo version/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/\"taosdata\"/\"khroot\"/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/support@taosdata\.com/support@wellintech\.com/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/taosc, rest, and stmt/khclient, rest, and stmt/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/taosdemo uses/khdemo uses/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/use 'taosc'/use 'khclient'/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  # src/util/src/tlog.c
  sed -i "s/log\/taos/log\/kinghistorian/g"   ${top_dir}/src/util/src/tlog.c
  # src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/KingHistorian/g"  ${top_dir}/src/dnode/src/dnodeSystem.c 
  sed -i "s/TDengine/KingHistorian/g"   ${top_dir}/src/dnode/src/dnodeMain.c
  sed -i "s/taosdlog/khserverlog/g"  ${top_dir}/src/dnode/src/dnodeMain.c
  # src/client/src/tscSystem.c
  sed -i "s/taoslog/khclientlog/g"  ${top_dir}/src/client/src/tscSystem.c
  # src/util/src/tnote.c
  sed -i "s/taosinfo/khinfo/g"  ${top_dir}/src/util/src/tnote.c
  # src/dnode/CMakeLists.txt
  sed -i "s/taos\.cfg/kinghistorian\.cfg/g"  ${top_dir}/src/dnode/CMakeLists.txt
  # src/kit/taosdump/taosdump.c
  sed -i "s/support@taosdata\.com/support@wellintech\.com/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/Default is taosdata/Default is khroot/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/\"taosdata\"/\"khroot\"/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/TDengine/KingHistorian/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/taos\/taos\.cfg/kinghistorian\/kinghistorian\.cfg/g" ${top_dir}/src/kit/taosdump/taosdump.c
  # src/os/src/linux/linuxEnv.c
  sed -i "s/etc\/taos/etc\/kinghistorian/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  sed -i "s/lib\/taos/lib\/kinghistorian/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  sed -i "s/log\/taos/log\/kinghistorian/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  # src/kit/shell/src/shellDarwin.c
  sed -i "s/TDengine shell/KingHistorian shell/g" ${top_dir}/src/kit/shell/src/shellDarwin.c
  sed -i "s/2020 by TAOS Data/2021 by Wellintech/g" ${top_dir}/src/kit/shell/src/shellDarwin.c
  # src/kit/shell/src/shellLinux.c
  sed -i "s/support@taosdata\.com/support@wellintech\.com/g" ${top_dir}/src/kit/shell/src/shellLinux.c
  sed -i "s/TDengine shell/KingHistorian shell/g" ${top_dir}/src/kit/shell/src/shellLinux.c
  sed -i "s/2020 by TAOS Data/2021 by Wellintech/g" ${top_dir}/src/kit/shell/src/shellLinux.c
  # src/os/src/windows/wEnv.c
  sed -i "s/TDengine/KingHistorian/g" ${top_dir}/src/os/src/windows/wEnv.c
  # src/kit/shell/src/shellEngine.c
  sed -i "s/TDengine shell/KingHistorian shell/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/2020 by TAOS Data, Inc/2021 by Wellintech, Inc/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/taos connect failed/kh connect failed/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/\"taos> \"/\"khclient> \"/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/\"   -> \"/\"       -> \"/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/prompt_size = 6/prompt_size = 10/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  # src/rpc/src/rpcMain.c
  sed -i "s/taos connections/kh connections/g" ${top_dir}/src/rpc/src/rpcMain.c
  # src/plugins/monitor/src/monMain.c
  sed -i "s/taosd is quiting/khserver is quiting/g" ${top_dir}/src/plugins/monitor/src/monMain.c
}

function replace_enterprise_kh(){
  # enterprise/src/kit/perfMonitor/perfMonitor.c
  sed -i "s/\"taosdata\"/\"khroot\"/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
  sed -i "s/TDengine/KingHistorian/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
  # enterprise/src/plugins/admin/src/httpAdminHandle.c
  sed -i "s/taos\.cfg/kinghistorian\.cfg/g"  ${top_dir}/../enterprise/src/plugins/admin/src/httpAdminHandle.c
  # enterprise/src/plugins/grant/src/grantMain.c
  sed -i "s/taos\.cfg/kinghistorian\.cfg/g"  ${top_dir}/../enterprise/src/plugins/grant/src/grantMain.c
  # enterprise/src/plugins/module/src/moduleMain.c
  sed -i "s/taos\.cfg/kinghistorian\.cfg/g"  ${top_dir}/../enterprise/src/plugins/module/src/moduleMain.c
}
