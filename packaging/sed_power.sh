#!/bin/bash

function replace_community_power(){
  # cmake/install.inc
  sed -i "s/C:\/TDengine/C:\/Power/g" ${top_dir}/cmake/install.inc
  sed -i "s/taos\.cfg/power\.cfg/g" ${top_dir}/cmake/install.inc
  sed -i "s/taos\.exe/power\.exe/g" ${top_dir}/cmake/install.inc
  sed -i "s/taosdemo\.exe/powerdemo\.exe/g" ${top_dir}/cmake/install.inc
  # src/kit/shell/inc/shell.h
  sed -i "s/taos_history/power_history/g" ${top_dir}/src/kit/shell/inc/shell.h
  # src/inc/taosdef.h
  sed -i "s/\"taosdata\"/\"powerdb\"/g" ${top_dir}/src/inc/taosdef.h
  # src/util/src/tconfig.c
  sed -i "s/taos\.cfg/power\.cfg/g"   ${top_dir}/src/util/src/tconfig.c
  sed -i "s/etc\/taos/etc\/power/g"   ${top_dir}/src/util/src/tconfig.c
  










  # src/util/src/tlog.c
  sed -i "s/log\/taos/log\/power/g"   ${top_dir}/src/util/src/tlog.c
  # src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/Power/g" ${top_dir}/src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/Power/g"   ${top_dir}/src/dnode/src/dnodeMain.c
  sed -i "s/taosdlog/powerdlog/g"   ${top_dir}/src/dnode/src/dnodeMain.c
  # src/client/src/tscSystem.c
  sed -i "s/taoslog/powerlog/g"   ${top_dir}/src/client/src/tscSystem.c
  # src/util/src/tnote.c
  sed -i "s/taosinfo/powerinfo/g"   ${top_dir}/src/util/src/tnote.c
  # src/dnode/CMakeLists.txt
  sed -i "s/taos\.cfg/power\.cfg/g"   ${top_dir}/src/dnode/CMakeLists.txt
  




  
  # src/os/src/linux/linuxEnv.c
  sed -i "s/etc\/taos/etc\/power/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  sed -i "s/lib\/taos/lib\/power/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  sed -i "s/log\/taos/log\/power/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  


  # src/kit/shell/src/shellLinux.c
  sed -i "s/TDengine shell/Power shell/g" ${top_dir}/src/kit/shell/src/shellLinux.c
  

  # src/os/src/windows/wEnv.c
  sed -i "s/C:\/TDengine/C:\/Power/g" ${top_dir}/src/os/src/windows/wEnv.c
  # src/kit/shell/src/shellEngine.c
  sed -i "s/TDengine shell/PowerDB shell/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/2020 by TAOS Data, Inc/2020 by PowerDB, Inc/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  
  sed -i "s/\"taos> \"/\"power> \"/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/\"   -> \"/\"    -> \"/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/prompt_size = 6/prompt_size = 7/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  # src/rpc/src/rpcMain.c
  sed -i "s/taos connections/power connections/g" ${top_dir}/src/rpc/src/rpcMain.c
  # src/plugins/monitor/src/monMain.c
  sed -i "s/taosd is quiting/powerd is quiting/g" ${top_dir}/src/plugins/monitor/src/monMain.c
}

function replace_enterprise_power(){
  # enterprise/src/kit/perfMonitor/perfMonitor.c
  sed -i "s/\"taosdata\"/\"powerdb\"/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
  sed -i "s/TDengine/PowerDB/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
  # enterprise/src/plugins/admin/src/httpAdminHandle.c
  sed -i "s/taos\.cfg/power\.cfg/g"  ${top_dir}/../enterprise/src/plugins/admin/src/httpAdminHandle.c
  # enterprise/src/plugins/grant/src/grantMain.c
  sed -i "s/taos\.cfg/power\.cfg/g"  ${top_dir}/../enterprise/src/plugins/grant/src/grantMain.c
  # enterprise/src/plugins/module/src/moduleMain.c
  sed -i "s/taos\.cfg/power\.cfg/g"  ${top_dir}/../enterprise/src/plugins/module/src/moduleMain.c
}
