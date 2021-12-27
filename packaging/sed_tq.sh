#!/bin/bash

function replace_community_tq(){
  # cmake/install.inc
  sed -i "s/C:\/TDengine/C:\/TQ/g" ${top_dir}/cmake/install.inc
  sed -i "s/taos\.cfg/tq\.cfg/g" ${top_dir}/cmake/install.inc
  sed -i "s/taos\.exe/tq\.exe/g" ${top_dir}/cmake/install.inc
  sed -i "s/taosdemo\.exe/tqdemo\.exe/g" ${top_dir}/cmake/install.inc
  # src/kit/shell/inc/shell.h
  sed -i "s/taos_history/tq_history/g" ${top_dir}/src/kit/shell/inc/shell.h
  # src/inc/taosdef.h
  sed -i "s/\"taosdata\"/\"tqueue\"/g" ${top_dir}/src/inc/taosdef.h
  # src/util/src/tconfig.c
  sed -i "s/taos\.cfg/tq\.cfg/g"   ${top_dir}/src/util/src/tconfig.c
  sed -i "s/etc\/taos/etc\/tq/g"   ${top_dir}/src/util/src/tconfig.c
  










  # src/util/src/tlog.c
  sed -i "s/log\/taos/log\/tq/g"   ${top_dir}/src/util/src/tlog.c
  # src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/TQ/g" ${top_dir}/src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/TQ/g"   ${top_dir}/src/dnode/src/dnodeMain.c
  sed -i "s/taosdlog/tqdlog/g"   ${top_dir}/src/dnode/src/dnodeMain.c
  # src/client/src/tscSystem.c
  sed -i "s/taoslog/tqlog/g"   ${top_dir}/src/client/src/tscSystem.c
  # src/util/src/tnote.c
  sed -i "s/taosinfo/tqinfo/g"   ${top_dir}/src/util/src/tnote.c
  # src/dnode/CMakeLists.txt
  sed -i "s/taos\.cfg/tq\.cfg/g"   ${top_dir}/src/dnode/CMakeLists.txt
  # src/kit/taosdump/taosdump.c
  
  sed -i "s/Default is taosdata/Default is tqueue/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/\"taosdata\"/\"tqueue\"/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/TDengine/TQ/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/taos\/taos\.cfg/tq\/tq\.cfg/g" ${top_dir}/src/kit/taosdump/taosdump.c
  # src/os/src/linux/linuxEnv.c
  sed -i "s/etc\/taos/etc\/tq/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  sed -i "s/lib\/taos/lib\/tq/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  sed -i "s/log\/taos/log\/tq/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  # src/kit/shell/src/shellDarwin.c
  sed -i "s/TDengine shell/TQ shell/g" ${top_dir}/src/kit/shell/src/shellDarwin.c
  
  # src/kit/shell/src/shellLinux.c
  
  sed -i "s/TDengine shell/TQ shell/g" ${top_dir}/src/kit/shell/src/shellLinux.c
  
  # src/os/src/windows/wEnv.c
  sed -i "s/C:\/TDengine/C:\/TQ/g" ${top_dir}/src/os/src/windows/wEnv.c
  # src/kit/shell/src/shellEngine.c
  sed -i "s/TDengine shell/TQ shell/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  
  
  sed -i "s/\"taos> \"/\"tq> \"/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/\"   -> \"/\" -> \"/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/prompt_size = 6/prompt_size = 4/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  # src/rpc/src/rpcMain.c
  sed -i "s/taos connections/tq connections/g" ${top_dir}/src/rpc/src/rpcMain.c
  # src/plugins/monitor/src/monMain.c
  sed -i "s/taosd is quiting/tqd is quiting/g" ${top_dir}/src/plugins/monitor/src/monMain.c
}

function replace_enterprise_tq(){
  # enterprise/src/kit/perfMonitor/perfMonitor.c
  sed -i "s/\"taosdata\"/\"tqueue\"/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
  sed -i "s/TDengine/TQ/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
  # enterprise/src/plugins/admin/src/httpAdminHandle.c
  sed -i "s/taos\.cfg/tq\.cfg/g"  ${top_dir}/../enterprise/src/plugins/admin/src/httpAdminHandle.c
  # enterprise/src/plugins/grant/src/grantMain.c
  sed -i "s/taos\.cfg/tq\.cfg/g"  ${top_dir}/../enterprise/src/plugins/grant/src/grantMain.c
  # enterprise/src/plugins/module/src/moduleMain.c
  sed -i "s/taos\.cfg/tq\.cfg/g"  ${top_dir}/../enterprise/src/plugins/module/src/moduleMain.c
}
