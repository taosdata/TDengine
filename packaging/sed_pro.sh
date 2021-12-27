#!/bin/bash

function replace_community_pro(){
  # cmake/install.inc
  sed -i "s/C:\/TDengine/C:\/ProDB/g" ${top_dir}/cmake/install.inc
  sed -i "s/taos\.cfg/prodb\.cfg/g" ${top_dir}/cmake/install.inc
  sed -i "s/taos\.exe/prodbc\.exe/g" ${top_dir}/cmake/install.inc
  sed -i "s/taosdemo\.exe/prodemo\.exe/g" ${top_dir}/cmake/install.inc
  # src/kit/shell/inc/shell.h
  sed -i "s/taos_history/prodb_history/g" ${top_dir}/src/kit/shell/inc/shell.h
  # src/inc/taosdef.h
  sed -i "s/\"taosdata\"/\"prodb\"/g" ${top_dir}/src/inc/taosdef.h
  # src/util/src/tconfig.c
  sed -i "s/taos\.cfg/prodb\.cfg/g"   ${top_dir}/src/util/src/tconfig.c
  sed -i "s/etc\/taos/etc\/ProDB/g"   ${top_dir}/src/util/src/tconfig.c
  # src/kit/taosdemo/taosdemo.c
  sed -i "s/taosdemo --help/prodemo --help/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/taosdemo --usage/prodemo --usage/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/Usage: taosdemo/Usage: prodemo/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/taosdemo is simulating/prodemo is simulating/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/taosdemo version/prodemo version/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/\"taosdata\"/\"prodb\"/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/support@taosdata\.com/support@hanatech\.com\.cn/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/taosc, rest, and stmt/prodbc, rest, and stmt/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/taosdemo uses/prodemo uses/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  sed -i "s/use 'taosc'/use 'prodbc'/g" ${top_dir}/src/kit/taosdemo/taosdemo.c
  # src/util/src/tlog.c
  sed -i "s/log\/taos/log\/ProDB/g"   ${top_dir}/src/util/src/tlog.c
  # src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/ProDB/g" ${top_dir}/src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/ProDB/g"   ${top_dir}/src/dnode/src/dnodeMain.c
  sed -i "s/taosdlog/prodlog/g"   ${top_dir}/src/dnode/src/dnodeMain.c
  # src/client/src/tscSystem.c
  sed -i "s/taoslog/prolog/g"   ${top_dir}/src/client/src/tscSystem.c
  # src/util/src/tnote.c
  sed -i "s/taosinfo/proinfo/g"   ${top_dir}/src/util/src/tnote.c
  # src/dnode/CMakeLists.txt
  sed -i "s/taos\.cfg/prodb\.cfg/g"   ${top_dir}/src/dnode/CMakeLists.txt
  # src/kit/taosdump/taosdump.c
  sed -i "s/support@taosdata\.com/support@hanatech\.com\.cn/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/Default is taosdata/Default is prodb/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/\"taosdata\"/\"prodb\"/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/TDengine/ProDB/g" ${top_dir}/src/kit/taosdump/taosdump.c
  sed -i "s/taos\/taos\.cfg/ProDB\/prodb\.cfg/g" ${top_dir}/src/kit/taosdump/taosdump.c
  # src/os/src/linux/linuxEnv.c
  sed -i "s/etc\/taos/etc\/ProDB/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  sed -i "s/lib\/taos/lib\/ProDB/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  sed -i "s/log\/taos/log\/ProDB/g" ${top_dir}/src/os/src/linux/linuxEnv.c
  # src/kit/shell/src/shellDarwin.c
  sed -i "s/TDengine shell/ProDB shell/g" ${top_dir}/src/kit/shell/src/shellDarwin.c
  sed -i "s/2020 by TAOS Data/2021 by HanaTech/g" ${top_dir}/src/kit/shell/src/shellDarwin.c
  # src/kit/shell/src/shellLinux.c
  sed -i "s/support@taosdata\.com/support@hanatech\.com\.cn/g" ${top_dir}/src/kit/shell/src/shellLinux.c
  sed -i "s/TDengine shell/ProDB shell/g" ${top_dir}/src/kit/shell/src/shellLinux.c
  sed -i "s/2020 by TAOS Data/2021 by HanaTech/g" ${top_dir}/src/kit/shell/src/shellLinux.c
  # src/os/src/windows/wEnv.c
  sed -i "s/C:\/TDengine/C:\/ProDB/g" ${top_dir}/src/os/src/windows/wEnv.c
  # src/kit/shell/src/shellEngine.c
  sed -i "s/TDengine shell/ProDB shell/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/2020 by TAOS Data, Inc/2021 by Hanatech, Inc/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/taos connect failed/prodbc connect failed/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/\"taos> \"/\"ProDB> \"/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/\"   -> \"/\"    -> \"/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/prompt_size = 6/prompt_size = 7/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  # src/rpc/src/rpcMain.c
  sed -i "s/taos connections/prodbc connections/g" ${top_dir}/src/rpc/src/rpcMain.c
  # src/plugins/monitor/src/monMain.c
  sed -i "s/taosd is quiting/prodbs is quiting/g" ${top_dir}/src/plugins/monitor/src/monMain.c
}

function replace_enterprise_pro(){
  # enterprise/src/kit/perfMonitor/perfMonitor.c
  sed -i "s/\"taosdata\"/\"prodb\"/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
  sed -i "s/TDengine/ProDB/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
  # enterprise/src/plugins/admin/src/httpAdminHandle.c
  sed -i "s/taos\.cfg/prodb\.cfg/g"  ${top_dir}/../enterprise/src/plugins/admin/src/httpAdminHandle.c
  # enterprise/src/plugins/grant/src/grantMain.c
  sed -i "s/taos\.cfg/prodb\.cfg/g"  ${top_dir}/../enterprise/src/plugins/grant/src/grantMain.c
  # enterprise/src/plugins/module/src/moduleMain.c
  sed -i "s/taos\.cfg/prodb\.cfg/g"  ${top_dir}/../enterprise/src/plugins/module/src/moduleMain.c
}
