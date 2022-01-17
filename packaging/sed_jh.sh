#!/bin/bash

function replace_community_jh() {
  # cmake/install.inc
  sed -i "s/C:\/TDengine/C:\/jh_iot/g" ${top_dir}/cmake/install.inc
  sed -i "s/taos\.cfg/jh_taos\.cfg/g" ${top_dir}/cmake/install.inc
  sed -i "s/taos\.exe/jh_taos\.exe/g" ${top_dir}/cmake/install.inc
  # src/kit/shell/CMakeLists.txt
  sed -i "s/OUTPUT_NAME taos/OUTPUT_NAME jh_taos/g" ${top_dir}/src/kit/shell/CMakeLists.txt
  # src/kit/shell/inc/shell.h
  sed -i "s/taos_history/jh_taos_history/g" ${top_dir}/src/kit/shell/inc/shell.h
  # src/inc/taosdef.h
  sed -i "s/\"taosdata\"/\"jhdata\"/g" ${top_dir}/src/inc/taosdef.h
  # src/util/src/tconfig.c
  sed -i "s/taos\.cfg/jh_taos\.cfg/g" ${top_dir}/src/util/src/tconfig.c
  sed -i "s/etc\/taos/etc\/jh_taos/g" ${top_dir}/src/util/src/tconfig.c
  sed -i "s/taos config/jh_taos config/g" ${top_dir}/src/util/src/tconfig.c
  # src/util/src/tlog.c
  sed -i "s/log\/taos/log\/jh_taos/g" ${top_dir}/src/util/src/tlog.c
  # src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/jh_taos/g" ${top_dir}/src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/jh_taos/g" ${top_dir}/src/dnode/src/dnodeMain.c
  sed -i "s/taosdlog/jh_taosdlog/g" ${top_dir}/src/dnode/src/dnodeMain.c
  # src/client/src/tscSystem.c
  sed -i "s/taoslog/jh_taoslog/g" ${top_dir}/src/client/src/tscSystem.c
  # src/util/src/tnote.c
  sed -i "s/taosinfo/jh_taosinfo/g" ${top_dir}/src/util/src/tnote.c
  # src/dnode/CMakeLists.txt
  sed -i "s/taos\.cfg/jh_taos\.cfg/g" ${top_dir}/src/dnode/CMakeLists.txt
  echo "SET_TARGET_PROPERTIES(taosd PROPERTIES OUTPUT_NAME jh_taosd)" >>${top_dir}/src/dnode/CMakeLists.txt
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

  # packaging/tools/makepkg.sh
  sed -i "s/productName=\"TDengine\"/productName=\"jh_iot\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"jh_taosd\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/clientName=\"taos\"/clientName=\"jh_taos\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/configFile=\"taos\.cfg\"/configFile=\"jh_taos\.cfg\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/tarName=\"taos\.tar\.gz\"/tarName=\"jh_taos\.tar\.gz\"/g" ${top_dir}/packaging/tools/makepkg.sh
  # packaging/tools/remove.sh
  sed -i "s/installDir=\"\/usr\/local\/taos\"/installDir=\"\/usr\/local\/jh_taos\"/g" ${top_dir}/packaging/tools/remove.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"jh_taosd\"/g" ${top_dir}/packaging/tools/remove.sh
  sed -i "s/clientName=\"taos\"/clientName=\"jh_taos\"/g" ${top_dir}/packaging/tools/remove.sh
  sed -i "s/uninstallScript=\"rmtaos\"/uninstallScript=\"rmjh\"/g" ${top_dir}/packaging/tools/remove.sh
  sed -i "s/productName=\"TDengine\"/productName=\"jh_iot\"/g" ${top_dir}/packaging/tools/remove.sh
  # packaging/tools/startPre.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"jh_taosd\"/g" ${top_dir}/packaging/tools/startPre.sh
  sed -i "s/logDir=\"\/var\/log\/taos\"/logDir=\"\/var\/log\/jh_taos\"/g" ${top_dir}/packaging/tools/startPre.sh
  # packaging/tools/run_taosd_and_taosadapter.sh
  sed -i "s/taosd/jh_taosd/g" ${top_dir}/packaging/tools/run_taosd_and_taosadapter.sh
  # packaging/tools/install.sh
  sed -i "s/clientName=\"taos\"/clientName=\"jh_taos\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"jh_taosd\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/configFile=\"taos\.cfg\"/configFile=\"jh_taos\.cfg\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/productName=\"TDengine\"/productName=\"jh_iot\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/emailName=\"taosdata\.com\"/emailName=\"\jhict\.com\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/uninstallScript=\"rmtaos\"/uninstallScript=\"rmjh\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/historyFile=\"taos_history\"/historyFile=\"jh_taos_history\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/tarName=\"taos\.tar\.gz\"/tarName=\"jh_taos\.tar\.gz\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/dataDir=\"\/var\/lib\/taos\"/dataDir=\"\/var\/lib\/jh_taos\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/logDir=\"\/var\/log\/taos\"/logDir=\"\/var\/log\/jh_taos\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/configDir=\"\/etc\/taos\"/configDir=\"\/etc\/jh_taos\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/installDir=\"\/usr\/local\/taos\"/installDir=\"\/usr\/local\/jh_taos\"/g" ${top_dir}/packaging/tools/install.sh

  # packaging/tools/makeclient.sh
  sed -i "s/productName=\"TDengine\"/productName=\"jh_iot\"/g" ${top_dir}/packaging/tools/makeclient.sh
  sed -i "s/clientName=\"taos\"/clientName=\"jh_taos\"/g" ${top_dir}/packaging/tools/makeclient.sh
  sed -i "s/configFile=\"taos\.cfg\"/configFile=\"jh_taos\.cfg\"/g" ${top_dir}/packaging/tools/makeclient.sh
  sed -i "s/tarName=\"taos\.tar\.gz\"/tarName=\"jh_taos\.tar\.gz\"/g" ${top_dir}/packaging/tools/makeclient.sh
  # packaging/tools/remove_client.sh
  sed -i "s/installDir=\"\/usr\/local\/taos\"/installDir=\"\/usr\/local\/jh_taos\"/g" ${top_dir}/packaging/tools/remove_client.sh
  sed -i "s/clientName=\"taos\"/clientName=\"jh_taos\"/g" ${top_dir}/packaging/tools/remove_client.sh
  sed -i "s/uninstallScript=\"rmtaos\"/uninstallScript=\"rmjh\"/g" ${top_dir}/packaging/tools/remove_client.sh
  # packaging/tools/install_client.sh
  sed -i "s/dataDir=\"\/var\/lib\/taos\"/dataDir=\"\/var\/lib\/jh_iot\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/logDir=\"\/var\/log\/taos\"/logDir=\"\/var\/log\/jh_taos\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/productName=\"TDengine\"/productName=\"jh_iot\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/installDir=\"\/usr\/local\/taos\"/installDir=\"\/usr\/local\/jh_taos\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/configDir=\"\/etc\/taos\"/configDir=\"\/etc\/jh_taos\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"jh_taosd\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/clientName=\"taos\"/clientName=\"jh_taos\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/uninstallScript=\"rmtaos\"/uninstallScript=\"rmjh\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/configFile=\"taos\.cfg\"/configFile=\"jh_taos\.cfg\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/tarName=\"taos\.tar\.gz\"/tarName=\"jh_taos\.tar\.gz\"/g" ${top_dir}/packaging/tools/install_client.sh

  # packaging/tools/makearbi.sh
  sed -i "s/productName=\"TDengine\"/productName=\"jh_iot\"/g" ${top_dir}/packaging/tools/makearbi.sh
  # packaging/tools/remove_arbi.sh
  sed -i "s/TDengine/jh_iot/g" ${top_dir}/packaging/tools/remove_arbi.sh
  # packaging/tools/install_arbi.sh
  sed -i "s/TDengine/jh_iot/g" ${top_dir}/packaging/tools/install_arbi.sh
  sed -i "s/taosdata\.com/jhict\.com/g" ${top_dir}/packaging/tools/install_arbi.sh

  # packaging/tools/make_install.sh
  sed -i "s/clientName=\"taos\"/clientName=\"jh_taos\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"jh_taosd\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/logDir=\"\/var\/log\/taos\"/logDir=\"\/var\/log\/jh_taos\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/dataDir=\"\/var\/lib\/taos\"/dataDir=\"\/var\/lib\/jh_taos\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/configDir=\"\/etc\/taos\"/configDir=\"\/etc\/jh_taos\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/configFile=\"taos\.cfg\"/configFile=\"jh_taos\.cfg\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/installDir=\"\/usr\/local\/taos\"/installDir=\"\/usr\/local\/jh_taos\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/productName=\"TDengine\"/productName=\"jh_iot\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/emailName=\"taosdata\.com\"/emailName=\"jhict\.com\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/uninstallScript=\"rmtaos\"/uninstallScript=\"rmjh\"/g" ${top_dir}/packaging/tools/make_install.sh

  # packaging/rpm/taosd
  sed -i "s/TDengine/jh_iot/g" ${top_dir}/packaging/rpm/taosd
  sed -i "s/usr\/local\/taos/usr\/local\/jh_taos/g" ${top_dir}/packaging/rpm/taosd
  sed -i "s/taosd/jh_taosd/g" ${top_dir}/packaging/rpm/taosd
  # packaging/deb/taosd
  sed -i "s/TDengine/jh_iot/g" ${top_dir}/packaging/deb/taosd
  sed -i "s/usr\/local\/taos/usr\/local\/jh_taos/g" ${top_dir}/packaging/deb/taosd
  sed -i "s/taosd/jh_taosd/g" ${top_dir}/packaging/deb/taosd

}

function replace_enterprise_jh() {
  # enterprise/src/kit/perfMonitor/perfMonitor.c
  sed -i "s/\"taosdata\"/\"jhdata\"/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
  sed -i "s/TDengine/jh_iot/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
  # enterprise/src/plugins/admin/src/httpAdminHandle.c
  sed -i "s/taos\.cfg/jh_taos\.cfg/g" ${top_dir}/../enterprise/src/plugins/admin/src/httpAdminHandle.c
  # enterprise/src/plugins/grant/src/grantMain.c
  sed -i "s/taos\.cfg/jh_taos\.cfg/g" ${top_dir}/../enterprise/src/plugins/grant/src/grantMain.c
  # enterprise/src/plugins/module/src/moduleMain.c
  sed -i "s/taos\.cfg/jh_taos\.cfg/g" ${top_dir}/../enterprise/src/plugins/module/src/moduleMain.c

  # enterprise/src/plugins/web
  sed -i -e "s/www\.taosdata\.com/www\.jhict\.com\.cn/g" $(grep -r "www.taosdata.com" ${top_dir}/../enterprise/src/plugins/web | sed -r "s/(.*\.html):\s*(.*)/\1/g")
  sed -i -e "s/2017, TAOS Data/2021, Jinheng Technology/g" $(grep -r "TAOS Data" ${top_dir}/../enterprise/src/plugins/web | sed -r "s/(.*\.html):\s*(.*)/\1/g")
  sed -i -e "s/taosd/jh_taosd/g" $(grep -r "taosd" ${top_dir}/../enterprise/src/plugins/web | grep -E "*\.js\s*.*" | sed -r -e "s/(.*\.js):\s*(.*)/\1/g" | sort | uniq)
  # enterprise/src/plugins/web/admin/monitor.html
  sed -i -e "s/<th style=\"font-weight: normal\">taosd<\/th>/<th style=\"font-weight: normal\">jh_taosd<\/th>/g" ${top_dir}/../enterprise/src/plugins/web/admin/monitor.html
  sed -i -e "s/data:\['taosd', 'system'\],/data:\['jh_taosd', 'system'\],/g" ${top_dir}/../enterprise/src/plugins/web/admin/monitor.html
  sed -i -e "s/name: 'taosd',/name: 'jh_taosd',/g" ${top_dir}/../enterprise/src/plugins/web/admin/monitor.html
  # enterprise/src/plugins/web/admin/*.html
  sed -i "s/TDengine/jh_iot/g" ${top_dir}/../enterprise/src/plugins/web/admin/*.html
  # enterprise/src/plugins/web/admin/js/*.js
  sed -i "s/TDengine/jh_iot/g" ${top_dir}/../enterprise/src/plugins/web/admin/js/*.js
}
