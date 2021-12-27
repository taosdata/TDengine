#!/bin/bash

function replace_community_jh(){
  # cmake/install.inc
  sed -i "s/C:\/TDengine/C:\/jh_iot/g" ${top_dir}/cmake/install.inc
  sed -i "s/taos\.cfg/jh_taos\.cfg/g" ${top_dir}/cmake/install.inc
  sed -i "s/taos\.exe/jh_taos\.exe/g" ${top_dir}/cmake/install.inc
  sed -i "s/taosdemo\.exe/jhdemo\.exe/g" ${top_dir}/cmake/install.inc
  # src/kit/shell/inc/shell.h
  sed -i "s/taos_history/jh_taos_history/g" ${top_dir}/src/kit/shell/inc/shell.h
  # src/inc/taosdef.h
  sed -i "s/\"taosdata\"/\"jhdata\"/g" ${top_dir}/src/inc/taosdef.h
  # src/util/src/tconfig.c
  sed -i "s/taos\.cfg/jh_taos\.cfg/g"  ${top_dir}/src/util/src/tconfig.c
  sed -i "s/etc\/taos/etc\/jh_taos/g"   ${top_dir}/src/util/src/tconfig.c
  # src/kit/taosdemo/taosdemo.c
  sed -i "s/taosdemo --help/jhdemo --help/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/taosdemo --usage/jhdemo --usage/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/Usage: taosdemo/Usage: jhdemo/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/taosdemo is simulating/jhdemo is simulating/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/taosdemo version/jhdemo version/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/\"taosdata\"/\"jhdata\"/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/support@taosdata\.com/jhkj@njsteel\.com\.cn/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/taosc, rest, and stmt/jh_taos, rest, and stmt/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/taosdemo uses/jhdemo uses/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/use 'taosc'/use 'jh_taos'/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  # src/util/src/tlog.c
  sed -i "s/log\/taos/log\/jh_taos/g"   ${top_dir}/src/util/src/tlog.c
  # src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/jh_iot/g"  ${top_dir}/src/dnode/src/dnodeSystem.c 
  # src/dnode/src/dnodeMain.c
  sed -i "s/TDengine/jh_iot/g"   ${top_dir}/src/dnode/src/dnodeMain.c
  sed -i "s/taosdlog/jh_taosdlog/g"  ${top_dir}/src/dnode/src/dnodeMain.c
  # src/client/src/tscSystem.c
  sed -i "s/taoslog/jh_taoslog/g"  ${top_dir}/src/client/src/tscSystem.c
  # src/util/src/tnote.c
  sed -i "s/taosinfo/jh_taosinfo/g"  ${top_dir}/src/util/src/tnote.c
  # src/dnode/CMakeLists.txt
  sed -i "s/taos\.cfg/jh_taos\.cfg/g"  ${top_dir}/src/dnode/CMakeLists.txt
  # src/kit/taosdump/taosdump.c
  sed -i "s/support@taosdata\.com/jhkj@njsteel\.com\.cn/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/Default is taosdata/Default is jhdata/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/\"taosdata\"/\"jhdata\"/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/TDengine/jh_iot/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/taos\/taos\.cfg/jh_taos\/jh_taos\.cfg/g" ${top_dir}/src/kit/taosdump/taosdump.c
  # src/os/src/linux/linuxEnv.c
  sed -i "s/etc\/taos/etc\/jh_taos/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  sed -i "s/lib\/taos/lib\/jh_taos/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  sed -i "s/log\/taos/log\/jh_taos/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  # src/kit/shell/src/shellDarwin.c
  sed -i "s/TDengine shell/jh_iot shell/g" ${top_dir}/src/kit/shell/src/shellDarwin.c
  sed -i "s/2020 by TAOS Data/2021 by Jinheng Technology/g" ${top_dir}/src/kit/shell/src/shellDarwin.c
  # src/kit/shell/src/shellLinux.c
  sed -i "s/support@taosdata\.com/jhkj@njsteel\.com\.cn/g" ${top_dir}/src/kit/shell/src/shellLinux.c
  sed -i "s/TDengine shell/jh_iot shell/g" ${top_dir}/src/kit/shell/src/shellLinux.c
  sed -i "s/2020 by TAOS Data/2021 by Jinheng Technology/g" ${top_dir}/src/kit/shell/src/shellLinux.c
  # src/os/src/windows/wEnv.c
  sed -i "s/C:\/TDengine/C:\/jh_iot/g" ${top_dir}/src/os/src/windows/wEnv.c
  # src/kit/shell/src/shellEngine.c
  sed -i "s/TDengine shell/jh_iot shell/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/2020 by TAOS Data, Inc/2021 by Jinheng Technology, Inc/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/taos connect failed/jh_taos connect failed/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/\"taos> \"/\"jh_taos> \"/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/\"   -> \"/\"      -> \"/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/prompt_size = 6/prompt_size = 9/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  # src/rpc/src/rpcMain.c
  sed -i "s/taos connections/jh_taos connections/g" ${top_dir}/src/rpc/src/rpcMain.c
  # src/plugins/monitor/src/monMain.c
  sed -i "s/taosd is quiting/jh_taosd is quiting/g" ${top_dir}/src/plugins/monitor/src/monMain.c
}

function replace_enterprise_jh(){
  # enterprise/src/kit/perfMonitor/perfMonitor.c
  sed -i "s/\"taosdata\"/\"jhdata\"/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
  sed -i "s/TDengine/jh_iot/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
  # enterprise/src/plugins/admin/src/httpAdminHandle.c
  sed -i "s/taos\.cfg/jh_taos\.cfg/g"  ${top_dir}/../enterprise/src/plugins/admin/src/httpAdminHandle.c
  # enterprise/src/plugins/grant/src/grantMain.c
  sed -i "s/taos\.cfg/jh_taos\.cfg/g"  ${top_dir}/../enterprise/src/plugins/grant/src/grantMain.c
  # enterprise/src/plugins/module/src/moduleMain.c
  sed -i "s/taos\.cfg/jh_taos\.cfg/g"  ${top_dir}/../enterprise/src/plugins/module/src/moduleMain.c
}
