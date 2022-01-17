#!/bin/bash

function replace_community_tq() {
  # cmake/install.inc
  sed -i "s/C:\/TDengine/C:\/TQ/g" ${top_dir}/cmake/install.inc
  sed -i "s/taos\.cfg/tq\.cfg/g" ${top_dir}/cmake/install.inc
  sed -i "s/taos\.exe/tq\.exe/g" ${top_dir}/cmake/install.inc
  # src/kit/shell/CMakeLists.txt
  sed -i "s/OUTPUT_NAME taos/OUTPUT_NAME tq/g" ${top_dir}/src/kit/shell/CMakeLists.txt
  # src/kit/shell/inc/shell.h
  sed -i "s/taos_history/tq_history/g" ${top_dir}/src/kit/shell/inc/shell.h
  # src/inc/taosdef.h
  sed -i "s/\"taosdata\"/\"tqueue\"/g" ${top_dir}/src/inc/taosdef.h
  # src/util/src/tconfig.c
  sed -i "s/taos\.cfg/tq\.cfg/g" ${top_dir}/src/util/src/tconfig.c
  sed -i "s/etc\/taos/etc\/tq/g" ${top_dir}/src/util/src/tconfig.c
  sed -i "s/taos config/tq config/g" ${top_dir}/src/util/src/tconfig.c
  # src/util/src/tlog.c
  sed -i "s/log\/taos/log\/tq/g" ${top_dir}/src/util/src/tlog.c
  # src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/TQ/g" ${top_dir}/src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/TQ/g" ${top_dir}/src/dnode/src/dnodeMain.c
  sed -i "s/taosdlog/tqdlog/g" ${top_dir}/src/dnode/src/dnodeMain.c
  # src/client/src/tscSystem.c
  sed -i "s/taoslog/tqlog/g" ${top_dir}/src/client/src/tscSystem.c
  # src/util/src/tnote.c
  sed -i "s/taosinfo/tqinfo/g" ${top_dir}/src/util/src/tnote.c
  # src/dnode/CMakeLists.txt
  sed -i "s/taos\.cfg/tq\.cfg/g" ${top_dir}/src/dnode/CMakeLists.txt
  echo "SET_TARGET_PROPERTIES(taosd PROPERTIES OUTPUT_NAME tqd)" >>${top_dir}/src/dnode/CMakeLists.txt
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
  sed -i "s/taos connect failed/tq connect failed/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/\"taos> \"/\"tq> \"/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/\"   -> \"/\" -> \"/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  sed -i "s/prompt_size = 6/prompt_size = 4/g" ${top_dir}/src/kit/shell/src/shellEngine.c
  # src/rpc/src/rpcMain.c
  sed -i "s/taos connections/tq connections/g" ${top_dir}/src/rpc/src/rpcMain.c
  # src/plugins/monitor/src/monMain.c
  sed -i "s/taosd is quiting/tqd is quiting/g" ${top_dir}/src/plugins/monitor/src/monMain.c

  # packaging/tools/makepkg.sh
  sed -i "s/productName=\"TDengine\"/productName=\"TQ\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"tqd\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/clientName=\"taos\"/clientName=\"tq\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/configFile=\"taos\.cfg\"/configFile=\"tq\.cfg\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/tarName=\"taos\.tar\.gz\"/tarName=\"tq\.tar\.gz\"/g" ${top_dir}/packaging/tools/makepkg.sh
  # packaging/tools/remove.sh
  sed -i "s/installDir=\"\/usr\/local\/taos\"/installDir=\"\/usr\/local\/tq\"/g" ${top_dir}/packaging/tools/remove.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"tqd\"/g" ${top_dir}/packaging/tools/remove.sh
  sed -i "s/clientName=\"taos\"/clientName=\"tq\"/g" ${top_dir}/packaging/tools/remove.sh
  sed -i "s/uninstallScript=\"rmtaos\"/uninstallScript=\"rmtq\"/g" ${top_dir}/packaging/tools/remove.sh
  sed -i "s/productName=\"TDengine\"/productName=\"TQ\"/g" ${top_dir}/packaging/tools/remove.sh
  # packaging/tools/startPre.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"tqd\"/g" ${top_dir}/packaging/tools/startPre.sh
  sed -i "s/logDir=\"\/var\/log\/taos\"/logDir=\"\/var\/log\/tq\"/g" ${top_dir}/packaging/tools/startPre.sh
  # packaging/tools/run_taosd_and_taosadapter.sh
  sed -i "s/taosd/tqd/g" ${top_dir}/packaging/tools/run_taosd_and_taosadapter.sh
  # packaging/tools/install.sh
  sed -i "s/clientName=\"taos\"/clientName=\"tq\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"tqd\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/configFile=\"taos\.cfg\"/configFile=\"tq\.cfg\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/productName=\"TDengine\"/productName=\"TQ\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/uninstallScript=\"rmtaos\"/uninstallScript=\"rmtq\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/historyFile=\"taos_history\"/historyFile=\"tq_history\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/tarName=\"taos\.tar\.gz\"/tarName=\"tq\.tar\.gz\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/dataDir=\"\/var\/lib\/taos\"/dataDir=\"\/var\/lib\/tq\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/logDir=\"\/var\/log\/taos\"/logDir=\"\/var\/log\/tq\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/configDir=\"\/etc\/taos\"/configDir=\"\/etc\/tq\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/installDir=\"\/usr\/local\/taos\"/installDir=\"\/usr\/local\/tq\"/g" ${top_dir}/packaging/tools/install.sh

  # packaging/tools/makeclient.sh
  sed -i "s/productName=\"TDengine\"/productName=\"TQ\"/g" ${top_dir}/packaging/tools/makeclient.sh
  sed -i "s/clientName=\"taos\"/clientName=\"tq\"/g" ${top_dir}/packaging/tools/makeclient.sh
  sed -i "s/configFile=\"taos\.cfg\"/configFile=\"tq\.cfg\"/g" ${top_dir}/packaging/tools/makeclient.sh
  sed -i "s/tarName=\"taos\.tar\.gz\"/tarName=\"tq\.tar\.gz\"/g" ${top_dir}/packaging/tools/makeclient.sh
  # packaging/tools/remove_client.sh
  sed -i "s/installDir=\"\/usr\/local\/taos\"/installDir=\"\/usr\/local\/tq\"/g" ${top_dir}/packaging/tools/remove_client.sh
  sed -i "s/clientName=\"taos\"/clientName=\"tq\"/g" ${top_dir}/packaging/tools/remove_client.sh
  sed -i "s/uninstallScript=\"rmtaos\"/uninstallScript=\"rmtq\"/g" ${top_dir}/packaging/tools/remove_client.sh
  # packaging/tools/install_client.sh
  sed -i "s/dataDir=\"\/var\/lib\/taos\"/dataDir=\"\/var\/lib\/tq\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/logDir=\"\/var\/log\/taos\"/logDir=\"\/var\/log\/tq\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/productName=\"TDengine\"/productName=\"TQ\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/installDir=\"\/usr\/local\/taos\"/installDir=\"\/usr\/local\/tq\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/configDir=\"\/etc\/taos\"/configDir=\"\/etc\/tq\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"tqd\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/clientName=\"taos\"/clientName=\"tq\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/uninstallScript=\"rmtaos\"/uninstallScript=\"rmtq\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/configFile=\"taos\.cfg\"/configFile=\"tq\.cfg\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/tarName=\"taos\.tar\.gz\"/tarName=\"tq\.tar\.gz\"/g" ${top_dir}/packaging/tools/install_client.sh

  # packaging/tools/makearbi.sh
  sed -i "s/productName=\"TDengine\"/productName=\"TQ\"/g" ${top_dir}/packaging/tools/makearbi.sh
  # packaging/tools/remove_arbi.sh
  sed -i "s/TDengine/TQ/g" ${top_dir}/packaging/tools/remove_arbi.sh
  # packaging/tools/install_arbi.sh
  sed -i "s/TDengine/TQ/g" ${top_dir}/packaging/tools/install_arbi.sh

  # packaging/tools/make_install.sh
  sed -i "s/clientName=\"taos\"/clientName=\"tq\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"tqd\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/logDir=\"\/var\/log\/taos\"/logDir=\"\/var\/log\/tq\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/dataDir=\"\/var\/lib\/taos\"/dataDir=\"\/var\/lib\/tq\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/configDir=\"\/etc\/taos\"/configDir=\"\/etc\/tq\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/configFile=\"taos\.cfg\"/configFile=\"tq\.cfg\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/installDir=\"\/usr\/local\/taos\"/installDir=\"\/usr\/local\/tq\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/productName=\"TDengine\"/productName=\"TQ\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/uninstallScript=\"rmtaos\"/uninstallScript=\"rmtq\"/g" ${top_dir}/packaging/tools/make_install.sh

  # packaging/rpm/taosd
  sed -i "s/TDengine/TQ/g" ${top_dir}/packaging/rpm/taosd
  sed -i "s/usr\/local\/taos/usr\/local\/tq/g" ${top_dir}/packaging/rpm/taosd
  sed -i "s/taosd/tqd/g" ${top_dir}/packaging/rpm/taosd
  # packaging/deb/taosd
  sed -i "s/TDengine/TQ/g" ${top_dir}/packaging/deb/taosd
  sed -i "s/usr\/local\/taos/usr\/local\/tq/g" ${top_dir}/packaging/deb/taosd
  sed -i "s/taosd/tqd/g" ${top_dir}/packaging/deb/taosd
}

function replace_enterprise_tq() {
  # enterprise/src/kit/perfMonitor/perfMonitor.c
  sed -i "s/\"taosdata\"/\"tqueue\"/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
  sed -i "s/TDengine/TQ/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
  # enterprise/src/plugins/admin/src/httpAdminHandle.c
  sed -i "s/taos\.cfg/tq\.cfg/g" ${top_dir}/../enterprise/src/plugins/admin/src/httpAdminHandle.c
  # enterprise/src/plugins/grant/src/grantMain.c
  sed -i "s/taos\.cfg/tq\.cfg/g" ${top_dir}/../enterprise/src/plugins/grant/src/grantMain.c
  # enterprise/src/plugins/module/src/moduleMain.c
  sed -i "s/taos\.cfg/tq\.cfg/g" ${top_dir}/../enterprise/src/plugins/module/src/moduleMain.c

  # enterprise/src/plugins/web
  sed -i -e "s/taosd/tqd/g" $(grep -r "taosd" ${top_dir}/../enterprise/src/plugins/web | grep -E "*\.js\s*.*" | sed -r -e "s/(.*\.js):\s*(.*)/\1/g" | sort | uniq)
  # enterprise/src/plugins/web/admin/monitor.html
  sed -i -e "s/<th style=\"font-weight: normal\">taosd<\/th>/<th style=\"font-weight: normal\">tqd<\/th>/g" ${top_dir}/../enterprise/src/plugins/web/admin/monitor.html
  sed -i -e "s/data:\['taosd', 'system'\],/data:\['tqd', 'system'\],/g" ${top_dir}/../enterprise/src/plugins/web/admin/monitor.html
  sed -i -e "s/name: 'taosd',/name: 'tqd',/g" ${top_dir}/../enterprise/src/plugins/web/admin/monitor.html
  # enterprise/src/plugins/web/admin/*.html
  sed -i "s/TDengine/TQ/g" ${top_dir}/../enterprise/src/plugins/web/admin/*.html
  # enterprise/src/plugins/web/admin/js/*.js
  sed -i "s/TDengine/TQ/g" ${top_dir}/../enterprise/src/plugins/web/admin/js/*.js
}
