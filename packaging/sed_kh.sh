#!/bin/bash

function replace_community_kh() {
  # src/kit/taos-tools/packaging/tools/install-taostools.sh
  sed -i "s/demoName=\"taosdemo\"/demoName=\"khdemo\"/g" ${top_dir}/src/kit/taos-tools/packaging/tools/install-taostools.sh
  sed -i "s/benchmarkName=\"taosBenchmark\"/benchmarkName=\"khBenchmark\"/g" ${top_dir}/src/kit/taos-tools/packaging/tools/install-taostools.sh
  sed -i "s/dumpName=\"taosdump\"/dumpName=\"khdump\"/g" ${top_dir}/src/kit/taos-tools/packaging/tools/install-taostools.sh
  sed -i "s/emailName=\"taosdata\.com\"/emailName=\"wellintech\.com\"/g" ${top_dir}/src/kit/taos-tools/packaging/tools/install-taostools.sh
  sed -i "s/taosName=\"taos\"/taosName=\"kinghistorian\"/g" ${top_dir}/src/kit/taos-tools/packaging/tools/install-taostools.sh
  sed -i "s/toolsName=\"taostools\"/toolsName=\"khtools\"/g" ${top_dir}/src/kit/taos-tools/packaging/tools/install-taostools.sh
  cp -f ${top_dir}/src/kit/taos-tools/packaging/tools/install-taostools.sh ${top_dir}/src/kit/taos-tools/packaging/tools/install-khtools.sh

  # src/kit/taos-tools/packaging/tools/uninstall-taostools.sh
  sed -i "s/demoName=\"taosdemo\"/demoName=\"khdemo\"/g" ${top_dir}/src/kit/taos-tools/packaging/tools/uninstall-taostools.sh
  sed -i "s/benchmarkName=\"taosBenchmark\"/benchmarkName=\"khBenchmark\"/g" ${top_dir}/src/kit/taos-tools/packaging/tools/uninstall-taostools.sh
  sed -i "s/dumpName=\"taosdump\"/dumpName=\"khdump\"/g" ${top_dir}/src/kit/taos-tools/packaging/tools/uninstall-taostools.sh
  sed -i "s/taosName=\"taos\"/taosName=\"kinghistorian\"/g" ${top_dir}/src/kit/taos-tools/packaging/tools/uninstall-taostools.sh
  sed -i "s/toolsName=\"taostools\"/toolsName=\"khtools\"/g" ${top_dir}/src/kit/taos-tools/packaging/tools/uninstall-taostools.sh
  cp -f ${top_dir}/src/kit/taos-tools/packaging/tools/uninstall-taostools.sh ${top_dir}/src/kit/taos-tools/packaging/tools/uninstall-khtools.sh

  # src/kit/taos-tools/src/CMakeLists.txt
  sed -i "s/taosBenchmark /khBenchmark /g" ${top_dir}/src/kit/taos-tools/src/CMakeLists.txt
  sed -i "s/taosdump /khdump /g" ${top_dir}/src/kit/taos-tools/src/CMakeLists.txt
  # src/kit/taos-tools/CMakeLists.txt
  sed -i "s/taosdump/khdump/g" ${top_dir}/src/kit/taos-tools/CMakeLists.txt
  sed -i "s/taosBenchmark/khBenchmark/g" ${top_dir}/src/kit/taos-tools/CMakeLists.txt
  # src/kit/taos-tools/src/benchCommandOpt.c
  sed -i "s/support@taosdata\.com/support@wellintech\.com/g" ${top_dir}/src/kit/taos-tools/src/benchCommandOpt.c
  sed -i "s/taosc/khclient/g" ${top_dir}/src/kit/taos-tools/src/benchCommandOpt.c
  sed -i "s/default is taosdata/default is khroot/g" ${top_dir}/src/kit/taos-tools/src/benchCommandOpt.c
  sed -i "s/TDengine/KingHistorian/g" ${top_dir}/src/kit/taos-tools/src/benchCommandOpt.c
  # src/kit/taos-tools/src/taosdump.c
  sed -i "s/support@taosdata\.com/support@wellintech\.com/g" ${top_dir}/src/kit/taos-tools/src/taosdump.c
  sed -i "s/\/etc\/taos/\/etc\/kinghistorian/g" ${top_dir}/src/kit/taos-tools/src/taosdump.c
  sed -i "s/taosdata/khroot/g" ${top_dir}/src/kit/taos-tools/src/taosdump.c
  sed -i "s/taosdump version/khdump version/g" ${top_dir}/src/kit/taos-tools/src/taosdump.c
  sed -i "s/taosdump --help/khdump --help/g" ${top_dir}/src/kit/taos-tools/src/taosdump.c
  sed -i "s/taosdump --usage/khdump --usage/g" ${top_dir}/src/kit/taos-tools/src/taosdump.c
  sed -i "s/\"taosdump\"/\"khdump\"/g" ${top_dir}/src/kit/taos-tools/src/taosdump.c
  sed -i "s/TDengine/KingHistorian/g" ${top_dir}/src/kit/taos-tools/src/taosdump.c
  sed -i "s/taosdump requires/khdump requires/g" ${top_dir}/src/kit/taos-tools/src/taosdump.c

  # cmake/install.inc
  sed -i "s/C:\/TDengine/C:\/KingHistorian/g" ${top_dir}/cmake/install.inc
  sed -i "s/taos\.cfg/kinghistorian\.cfg/g" ${top_dir}/cmake/install.inc
  sed -i "s/taos\.exe/khclient\.exe/g" ${top_dir}/cmake/install.inc
  # src/kit/shell/CMakeLists.txt
  sed -i "s/OUTPUT_NAME taos/OUTPUT_NAME khclient/g" ${top_dir}/src/kit/shell/CMakeLists.txt
  # src/kit/shell/inc/shell.h
  sed -i "s/taos_history/kh_history/g" ${top_dir}/src/kit/shell/inc/shell.h
  # src/inc/taosdef.h
  sed -i "s/\"taosdata\"/\"khroot\"/g" ${top_dir}/src/inc/taosdef.h
  # src/util/src/tconfig.c
  sed -i "s/taos\.cfg/kinghistorian\.cfg/g" ${top_dir}/src/util/src/tconfig.c
  sed -i "s/etc\/taos/etc\/kinghistorian/g" ${top_dir}/src/util/src/tconfig.c
  sed -i "s/taos config/kinghistorian config/g" ${top_dir}/src/util/src/tconfig.c
  # src/util/src/tlog.c
  sed -i "s/log\/taos/log\/kinghistorian/g" ${top_dir}/src/util/src/tlog.c
  # src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/KingHistorian/g" ${top_dir}/src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/KingHistorian/g" ${top_dir}/src/dnode/src/dnodeMain.c
  sed -i "s/taosdlog/khserverlog/g" ${top_dir}/src/dnode/src/dnodeMain.c
  # src/client/src/tscSystem.c
  sed -i "s/taoslog/khclientlog/g" ${top_dir}/src/client/src/tscSystem.c
  # src/util/src/tnote.c
  sed -i "s/taosinfo/khinfo/g" ${top_dir}/src/util/src/tnote.c
  # src/dnode/CMakeLists.txt
  sed -i "s/taos\.cfg/kinghistorian\.cfg/g" ${top_dir}/src/dnode/CMakeLists.txt
  echo "SET_TARGET_PROPERTIES(taosd PROPERTIES OUTPUT_NAME khserver)" >>${top_dir}/src/dnode/CMakeLists.txt
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
  sed -i "s/C:\/TDengine/C:\/KingHistorian/g" ${top_dir}/src/os/src/windows/wEnv.c
  # src/kit/shell/src/shellEngine.c
  sed -i "s/TDengine shell/KingHistorian shell/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/2020 by TAOS Data, Inc/2021 by Wellintech, Inc/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/taos connect failed/khclient connect failed/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/\"taos> \"/\"khclient> \"/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/\"   -> \"/\"       -> \"/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/prompt_size = 6/prompt_size = 10/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  # src/rpc/src/rpcMain.c
  sed -i "s/taos connections/kh connections/g" ${top_dir}/src/rpc/src/rpcMain.c
  # src/plugins/monitor/src/monMain.c
  sed -i "s/taosd is quiting/khserver is quiting/g" ${top_dir}/src/plugins/monitor/src/monMain.c

  # packaging/tools/makepkg.sh
  sed -i "s/productName=\"TDengine\"/productName=\"KingHistorian\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"khserver\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/clientName=\"taos\"/clientName=\"khclient\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/configFile=\"taos\.cfg\"/configFile=\"kinghistorian\.cfg\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/tarName=\"taos\.tar\.gz\"/tarName=\"kinghistorian\.tar\.gz\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/dumpName=\"taosdump\"/dumpName=\"khdump\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/benchmarkName=\"taosBenchmark\"/benchmarkName=\"khBenchmark\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/toolsName=\"taostools\"/toolsName=\"khtools\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/adapterName=\"taosadapter\"/adapterName=\"khadapter\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/defaultPasswd=\"taosdata\"/defaultPasswd=\"khroot\"/g" ${top_dir}/packaging/tools/makepkg.sh

  # packaging/tools/remove.sh
  sed -i "s/installDir=\"\/usr\/local\/taos\"/installDir=\"\/usr\/local\/kinghistorian\"/g" ${top_dir}/packaging/tools/remove.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"khserver\"/g" ${top_dir}/packaging/tools/remove.sh
  sed -i "s/clientName=\"taos\"/clientName=\"khclient\"/g" ${top_dir}/packaging/tools/remove.sh
  sed -i "s/uninstallScript=\"rmtaos\"/uninstallScript=\"rmkh\"/g" ${top_dir}/packaging/tools/remove.sh
  sed -i "s/productName=\"TDengine\"/productName=\"KingHistorian\"/g" ${top_dir}/packaging/tools/remove.sh
  sed -i "s/adapterName=\"taosadapter\"/adapterName=\"khadapter\"/g" ${top_dir}/packaging/tools/remove.sh

  # packaging/tools/startPre.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"khserver\"/g" ${top_dir}/packaging/tools/startPre.sh
  sed -i "s/logDir=\"\/var\/log\/taos\"/logDir=\"\/var\/log\/kinghistorian\"/g" ${top_dir}/packaging/tools/startPre.sh
  # packaging/tools/run_taosd_and_taosadapter.sh
  sed -i "s/taosd/khserver/g" ${top_dir}/packaging/tools/run_taosd_and_taosadapter.sh
  sed -i "s/taosadapter/khadapter/g" ${top_dir}/packaging/tools/run_taosd_and_taosadapter.sh

  # packaging/tools/install.sh
  sed -i "s/clientName=\"taos\"/clientName=\"khclient\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"khserver\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/configFile=\"taos\.cfg\"/configFile=\"kinghistorian\.cfg\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/productName=\"TDengine\"/productName=\"KingHistorian\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/emailName=\"taosdata\.com\"/emailName=\"\wellintech\.com\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/uninstallScript=\"rmtaos\"/uninstallScript=\"rmkh\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/historyFile=\"taos_history\"/historyFile=\"kh_history\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/tarName=\"taos\.tar\.gz\"/tarName=\"kinghistorian\.tar\.gz\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/dataDir=\"\/var\/lib\/taos\"/dataDir=\"\/var\/lib\/kinghistorian\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/logDir=\"\/var\/log\/taos\"/logDir=\"\/var\/log\/kinghistorian\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/configDir=\"\/etc\/taos\"/configDir=\"\/etc\/kinghistorian\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/installDir=\"\/usr\/local\/taos\"/installDir=\"\/usr\/local\/kinghistorian\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/adapterName=\"taosadapter\"/adapterName=\"khadapter\"/g" ${top_dir}/packaging/tools/install.sh

  # packaging/tools/makeclient.sh
  sed -i "s/productName=\"TDengine\"/productName=\"KingHistorian\"/g" ${top_dir}/packaging/tools/makeclient.sh
  sed -i "s/clientName=\"taos\"/clientName=\"khclient\"/g" ${top_dir}/packaging/tools/makeclient.sh
  sed -i "s/configFile=\"taos\.cfg\"/configFile=\"kinghistorian\.cfg\"/g" ${top_dir}/packaging/tools/makeclient.sh
  sed -i "s/tarName=\"taos\.tar\.gz\"/tarName=\"kinghistorian\.tar\.gz\"/g" ${top_dir}/packaging/tools/makeclient.sh
  # packaging/tools/remove_client.sh
  sed -i "s/installDir=\"\/usr\/local\/taos\"/installDir=\"\/usr\/local\/kinghistorian\"/g" ${top_dir}/packaging/tools/remove_client.sh
  sed -i "s/clientName=\"taos\"/clientName=\"khclient\"/g" ${top_dir}/packaging/tools/remove_client.sh
  sed -i "s/uninstallScript=\"rmtaos\"/uninstallScript=\"rmkh\"/g" ${top_dir}/packaging/tools/remove_client.sh
  # packaging/tools/install_client.sh
  sed -i "s/dataDir=\"\/var\/lib\/taos\"/dataDir=\"\/var\/lib\/kinghistorian\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/logDir=\"\/var\/log\/taos\"/logDir=\"\/var\/log\/kinghistorian\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/productName=\"TDengine\"/productName=\"KingHistorian\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/installDir=\"\/usr\/local\/taos\"/installDir=\"\/usr\/local\/kinghistorian\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/configDir=\"\/etc\/taos\"/configDir=\"\/etc\/kinghistorian\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"khserver\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/clientName=\"taos\"/clientName=\"khclient\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/uninstallScript=\"rmtaos\"/uninstallScript=\"rmkh\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/configFile=\"taos\.cfg\"/configFile=\"kinghistorian\.cfg\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/tarName=\"taos\.tar\.gz\"/tarName=\"kinghistorian\.tar\.gz\"/g" ${top_dir}/packaging/tools/install_client.sh

  # packaging/tools/makearbi.sh
  sed -i "s/productName=\"TDengine\"/productName=\"KingHistorian\"/g" ${top_dir}/packaging/tools/makearbi.sh
  # packaging/tools/remove_arbi.sh
  sed -i "s/TDengine/KingHistorian/g" ${top_dir}/packaging/tools/remove_arbi.sh
  # packaging/tools/install_arbi.sh
  sed -i "s/TDengine/KingHistorian/g" ${top_dir}/packaging/tools/install_arbi.sh
  sed -i "s/taosdata\.com/wellintech\.com/g" ${top_dir}/packaging/tools/install_arbi.sh

  # packaging/tools/make_install.sh
  sed -i "s/clientName=\"taos\"/clientName=\"khclient\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"khserver\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/logDir=\"\/var\/log\/taos\"/logDir=\"\/var\/log\/kinghistorian\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/dataDir=\"\/var\/lib\/taos\"/dataDir=\"\/var\/lib\/kinghistorian\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/configDir=\"\/etc\/taos\"/configDir=\"\/etc\/kinghistorian\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/configFile=\"taos\.cfg\"/configFile=\"kinghistorian\.cfg\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/installDir=\"\/usr\/local\/taos\"/installDir=\"\/usr\/local\/kinghistorian\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/productName=\"TDengine\"/productName=\"KingHistorian\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/emailName=\"taosdata\.com\"/emailName=\"wellintech\.com\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/uninstallScript=\"rmtaos\"/uninstallScript=\"rmkh\"/g" ${top_dir}/packaging/tools/make_install.sh

  # packaging/rpm/taosd
  sed -i "s/TDengine/KingHistorian/g" ${top_dir}/packaging/rpm/taosd
  sed -i "s/usr\/local\/taos/usr\/local\/kinghistorian/g" ${top_dir}/packaging/rpm/taosd
  sed -i "s/taosd/khserver/g" ${top_dir}/packaging/rpm/taosd
  # packaging/deb/taosd
  sed -i "s/TDengine/KingHistorian/g" ${top_dir}/packaging/deb/taosd
  sed -i "s/usr\/local\/taos/usr\/local\/kinghistorian/g" ${top_dir}/packaging/deb/taosd
  sed -i "s/taosd/khserver/g" ${top_dir}/packaging/deb/taosd
}

function replace_enterprise_kh() {
  # enterprise/src/kit/perfMonitor/perfMonitor.c
  sed -i "s/\"taosdata\"/\"khroot\"/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
  sed -i "s/TDengine/KingHistorian/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
  # enterprise/src/plugins/admin/src/httpAdminHandle.c
  sed -i "s/taos\.cfg/kinghistorian\.cfg/g" ${top_dir}/../enterprise/src/plugins/admin/src/httpAdminHandle.c
  # enterprise/src/plugins/grant/src/grantMain.c
  sed -i "s/taos\.cfg/kinghistorian\.cfg/g" ${top_dir}/../enterprise/src/plugins/grant/src/grantMain.c
  # enterprise/src/plugins/module/src/moduleMain.c
  sed -i "s/taos\.cfg/kinghistorian\.cfg/g" ${top_dir}/../enterprise/src/plugins/module/src/moduleMain.c

  # enterprise/src/plugins/web
  sed -i -e "s/www\.taosdata\.com/www\.kingview\.com/g" $(grep -r "www.taosdata.com" ${top_dir}/../enterprise/src/plugins/web | sed -r "s/(.*\.html):\s*(.*)/\1/g")
  sed -i -e "s/2017, TAOS Data/2021, Wellintech/g" $(grep -r "TAOS Data" ${top_dir}/../enterprise/src/plugins/web | sed -r "s/(.*\.html):\s*(.*)/\1/g")
  sed -i -e "s/taosd/khserver/g" $(grep -r "taosd" ${top_dir}/../enterprise/src/plugins/web | grep -E "*\.js\s*.*" | sed -r -e "s/(.*\.js):\s*(.*)/\1/g" | sort | uniq)
  # enterprise/src/plugins/web/admin/monitor.html
  sed -i -e "s/<th style=\"font-weight: normal\">taosd<\/th>/<th style=\"font-weight: normal\">khserver<\/th>/g" ${top_dir}/../enterprise/src/plugins/web/admin/monitor.html
  sed -i -e "s/data:\['taosd', 'system'\],/data:\['khserver', 'system'\],/g" ${top_dir}/../enterprise/src/plugins/web/admin/monitor.html
  sed -i -e "s/name: 'taosd',/name: 'khserver',/g" ${top_dir}/../enterprise/src/plugins/web/admin/monitor.html
  # enterprise/src/plugins/web/admin/*.html
  sed -i "s/TDengine/KingHistorian/g" ${top_dir}/../enterprise/src/plugins/web/admin/*.html
  # enterprise/src/plugins/web/admin/js/*.js
  sed -i "s/TDengine/KingHistorian/g" ${top_dir}/../enterprise/src/plugins/web/admin/js/*.js

}
