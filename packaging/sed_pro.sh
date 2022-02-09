#!/bin/bash

function replace_community_pro() {
  # src/kit/taos-tools/packaging/tools/install-taostools.sh
  sed -i "s/demoName=\"taosdemo\"/demoName=\"prodemo\"/g" ${top_dir}/src/kit/taos-tools/packaging/tools/install-taostools.sh
  sed -i "s/benchmarkName=\"taosBenchmark\"/benchmarkName=\"proBenchmark\"/g" ${top_dir}/src/kit/taos-tools/packaging/tools/install-taostools.sh
  sed -i "s/dumpName=\"taosdump\"/dumpName=\"prodump\"/g" ${top_dir}/src/kit/taos-tools/packaging/tools/install-taostools.sh
  sed -i "s/emailName=\"taosdata\.com\"/emailName=\"hanatech\.com\.cn\"/g" ${top_dir}/src/kit/taos-tools/packaging/tools/install-taostools.sh
  sed -i "s/taosName=\"taos\"/taosName=\"prodb\"/g" ${top_dir}/src/kit/taos-tools/packaging/tools/install-taostools.sh
  sed -i "s/toolsName=\"taostools\"/toolsName=\"prodbtools\"/g" ${top_dir}/src/kit/taos-tools/packaging/tools/install-taostools.sh
  cp -f ${top_dir}/src/kit/taos-tools/packaging/tools/install-taostools.sh ${top_dir}/src/kit/taos-tools/packaging/tools/install-prodbtools.sh

  # src/kit/taos-tools/packaging/tools/uninstall-taostools.sh
  sed -i "s/demoName=\"taosdemo\"/demoName=\"prodemo\"/g" ${top_dir}/src/kit/taos-tools/packaging/tools/uninstall-taostools.sh
  sed -i "s/benchmarkName=\"taosBenchmark\"/benchmarkName=\"proBenchmark\"/g" ${top_dir}/src/kit/taos-tools/packaging/tools/uninstall-taostools.sh
  sed -i "s/dumpName=\"taosdump\"/dumpName=\"prodump\"/g" ${top_dir}/src/kit/taos-tools/packaging/tools/uninstall-taostools.sh
  sed -i "s/taosName=\"taos\"/taosName=\"prodb\"/g" ${top_dir}/src/kit/taos-tools/packaging/tools/uninstall-taostools.sh
  sed -i "s/toolsName=\"taostools\"/toolsName=\"prodbtools\"/g" ${top_dir}/src/kit/taos-tools/packaging/tools/uninstall-taostools.sh
  cp -f ${top_dir}/src/kit/taos-tools/packaging/tools/uninstall-taostools.sh ${top_dir}/src/kit/taos-tools/packaging/tools/uninstall-prodbtools.sh

  # src/kit/taos-tools/src/CMakeLists.txt
  sed -i "s/taosBenchmark /proBenchmark /g" ${top_dir}/src/kit/taos-tools/src/CMakeLists.txt
  sed -i "s/taosdump /prodump /g" ${top_dir}/src/kit/taos-tools/src/CMakeLists.txt
  # src/kit/taos-tools/CMakeLists.txt
  sed -i "s/taosdump/prodump/g" ${top_dir}/src/kit/taos-tools/CMakeLists.txt
  sed -i "s/taosBenchmark/proBenchmark/g" ${top_dir}/src/kit/taos-tools/CMakeLists.txt
  # src/kit/taos-tools/src/benchCommandOpt.c
  sed -i "s/support@taosdata\.com/support@hanatech\.com\.cn/g" ${top_dir}/src/kit/taos-tools/src/benchCommandOpt.c
  sed -i "s/taosc/prodbc/g" ${top_dir}/src/kit/taos-tools/src/benchCommandOpt.c
  sed -i "s/default is taosdata/default is prodb/g" ${top_dir}/src/kit/taos-tools/src/benchCommandOpt.c
  sed -i "s/TDengine/ProDB/g" ${top_dir}/src/kit/taos-tools/src/benchCommandOpt.c
  # src/kit/taos-tools/src/taosdump.c
  sed -i "s/support@taosdata\.com/support@hanatech\.com\.cn/g" ${top_dir}/src/kit/taos-tools/src/taosdump.c
  sed -i "s/\/etc\/taos/\/etc\/ProDB/g" ${top_dir}/src/kit/taos-tools/src/taosdump.c
  sed -i "s/taosdata/prodb/g" ${top_dir}/src/kit/taos-tools/src/taosdump.c
  sed -i "s/taosdump version/prodump version/g" ${top_dir}/src/kit/taos-tools/src/taosdump.c
  sed -i "s/taosdump --help/prodump --help/g" ${top_dir}/src/kit/taos-tools/src/taosdump.c
  sed -i "s/taosdump --usage/prodump --usage/g" ${top_dir}/src/kit/taos-tools/src/taosdump.c
  sed -i "s/\"taosdump\"/\"prodump\"/g" ${top_dir}/src/kit/taos-tools/src/taosdump.c
  sed -i "s/TDengine/ProDB/g" ${top_dir}/src/kit/taos-tools/src/taosdump.c
  sed -i "s/taosdump requires/prodump requires/g" ${top_dir}/src/kit/taos-tools/src/taosdump.c

  # cmake/install.inc
  sed -i "s/C:\/TDengine/C:\/ProDB/g" ${top_dir}/cmake/install.inc
  sed -i "s/taos\.cfg/prodb\.cfg/g" ${top_dir}/cmake/install.inc
  sed -i "s/taos\.exe/prodbc\.exe/g" ${top_dir}/cmake/install.inc
  # src/kit/shell/CMakeLists.txt
  sed -i "s/OUTPUT_NAME taos/OUTPUT_NAME prodbc/g" ${top_dir}/src/kit/shell/CMakeLists.txt
  # src/kit/shell/inc/shell.h
  sed -i "s/taos_history/prodb_history/g" ${top_dir}/src/kit/shell/inc/shell.h
  # src/inc/taosdef.h
  sed -i "s/\"taosdata\"/\"prodb\"/g" ${top_dir}/src/inc/taosdef.h
  # src/util/src/tconfig.c
  sed -i "s/taos\.cfg/prodb\.cfg/g" ${top_dir}/src/util/src/tconfig.c
  sed -i "s/etc\/taos/etc\/ProDB/g" ${top_dir}/src/util/src/tconfig.c
  sed -i "s/taos config/prodb config/g" ${top_dir}/src/util/src/tconfig.c
  # src/util/src/tlog.c
  sed -i "s/log\/taos/log\/ProDB/g" ${top_dir}/src/util/src/tlog.c
  # src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/ProDB/g" ${top_dir}/src/dnode/src/dnodeSystem.c
  sed -i "s/TDengine/ProDB/g" ${top_dir}/src/dnode/src/dnodeMain.c
  sed -i "s/taosdlog/prodlog/g" ${top_dir}/src/dnode/src/dnodeMain.c
  # src/client/src/tscSystem.c
  sed -i "s/taoslog/prolog/g" ${top_dir}/src/client/src/tscSystem.c
  # src/util/src/tnote.c
  sed -i "s/taosinfo/proinfo/g" ${top_dir}/src/util/src/tnote.c
  # src/dnode/CMakeLists.txt
  sed -i "s/taos\.cfg/prodb\.cfg/g" ${top_dir}/src/dnode/CMakeLists.txt
  echo "SET_TARGET_PROPERTIES(taosd PROPERTIES OUTPUT_NAME prodbs)" >>${top_dir}/src/dnode/CMakeLists.txt
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

  # packaging/tools/makepkg.sh
  sed -i "s/productName=\"TDengine\"/productName=\"ProDB\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"prodbs\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/clientName=\"taos\"/clientName=\"prodbc\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/configFile=\"taos\.cfg\"/configFile=\"prodb\.cfg\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/tarName=\"taos\.tar\.gz\"/tarName=\"prodb\.tar\.gz\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/dumpName=\"taosdump\"/dumpName=\"prodump\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/benchmarkName=\"taosBenchmark\"/benchmarkName=\"proBenchmark\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/toolsName=\"taostools\"/toolsName=\"prodbtools\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/adapterName=\"taosadapter\"/adapterName=\"proadapter\"/g" ${top_dir}/packaging/tools/makepkg.sh
  sed -i "s/defaultPasswd=\"taosdata\"/defaultPasswd=\"prodb\"/g" ${top_dir}/packaging/tools/makepkg.sh

  # packaging/tools/remove.sh
  sed -i "s/installDir=\"\/usr\/local\/taos\"/installDir=\"\/usr\/local\/ProDB\"/g" ${top_dir}/packaging/tools/remove.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"prodbs\"/g" ${top_dir}/packaging/tools/remove.sh
  sed -i "s/clientName=\"taos\"/clientName=\"prodbc\"/g" ${top_dir}/packaging/tools/remove.sh
  sed -i "s/uninstallScript=\"rmtaos\"/uninstallScript=\"rmpro\"/g" ${top_dir}/packaging/tools/remove.sh
  sed -i "s/productName=\"TDengine\"/productName=\"ProDB\"/g" ${top_dir}/packaging/tools/remove.sh
  sed -i "s/adapterName=\"taosadapter\"/adapterName=\"proadapter\"/g" ${top_dir}/packaging/tools/remove.sh

  # packaging/tools/startPre.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"prodbs\"/g" ${top_dir}/packaging/tools/startPre.sh
  sed -i "s/logDir=\"\/var\/log\/taos\"/logDir=\"\/var\/log\/ProDB\"/g" ${top_dir}/packaging/tools/startPre.sh
  # packaging/tools/run_taosd_and_taosadapter.sh
  sed -i "s/taosd/prodbs/g" ${top_dir}/packaging/tools/run_taosd_and_taosadapter.sh
  sed -i "s/taosadapter/proadapter/g" ${top_dir}/packaging/tools/run_taosd_and_taosadapter.sh
  # packaging/tools/install.sh
  sed -i "s/clientName=\"taos\"/clientName=\"prodbc\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"prodbs\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/configFile=\"taos\.cfg\"/configFile=\"prodb\.cfg\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/productName=\"TDengine\"/productName=\"ProDB\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/emailName=\"taosdata\.com\"/emailName=\"\hanatech\.com\.cn\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/uninstallScript=\"rmtaos\"/uninstallScript=\"rmpro\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/historyFile=\"taos_history\"/historyFile=\"prodb_history\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/tarName=\"taos\.tar\.gz\"/tarName=\"prodb\.tar\.gz\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/dataDir=\"\/var\/lib\/taos\"/dataDir=\"\/var\/lib\/ProDB\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/logDir=\"\/var\/log\/taos\"/logDir=\"\/var\/log\/ProDB\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/configDir=\"\/etc\/taos\"/configDir=\"\/etc\/ProDB\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/installDir=\"\/usr\/local\/taos\"/installDir=\"\/usr\/local\/ProDB\"/g" ${top_dir}/packaging/tools/install.sh
  sed -i "s/adapterName=\"taosadapter\"/adapterName=\"proadapter\"/g" ${top_dir}/packaging/tools/install.sh

  # packaging/tools/makeclient.sh
  sed -i "s/productName=\"TDengine\"/productName=\"ProDB\"/g" ${top_dir}/packaging/tools/makeclient.sh
  sed -i "s/clientName=\"taos\"/clientName=\"prodbc\"/g" ${top_dir}/packaging/tools/makeclient.sh
  sed -i "s/configFile=\"taos\.cfg\"/configFile=\"prodb\.cfg\"/g" ${top_dir}/packaging/tools/makeclient.sh
  sed -i "s/tarName=\"taos\.tar\.gz\"/tarName=\"prodb\.tar\.gz\"/g" ${top_dir}/packaging/tools/makeclient.sh
  # packaging/tools/remove_client.sh
  sed -i "s/installDir=\"\/usr\/local\/taos\"/installDir=\"\/usr\/local\/ProDB\"/g" ${top_dir}/packaging/tools/remove_client.sh
  sed -i "s/clientName=\"taos\"/clientName=\"prodbc\"/g" ${top_dir}/packaging/tools/remove_client.sh
  sed -i "s/uninstallScript=\"rmtaos\"/uninstallScript=\"rmpro\"/g" ${top_dir}/packaging/tools/remove_client.sh
  # packaging/tools/install_client.sh
  sed -i "s/dataDir=\"\/var\/lib\/taos\"/dataDir=\"\/var\/lib\/ProDB\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/logDir=\"\/var\/log\/taos\"/logDir=\"\/var\/log\/ProDB\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/productName=\"TDengine\"/productName=\"ProDB\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/installDir=\"\/usr\/local\/taos\"/installDir=\"\/usr\/local\/ProDB\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/configDir=\"\/etc\/taos\"/configDir=\"\/etc\/ProDB\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"prodbs\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/clientName=\"taos\"/clientName=\"prodbc\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/uninstallScript=\"rmtaos\"/uninstallScript=\"rmpro\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/configFile=\"taos\.cfg\"/configFile=\"prodb\.cfg\"/g" ${top_dir}/packaging/tools/install_client.sh
  sed -i "s/tarName=\"taos\.tar\.gz\"/tarName=\"prodb\.tar\.gz\"/g" ${top_dir}/packaging/tools/install_client.sh

  # packaging/tools/makearbi.sh
  sed -i "s/productName=\"TDengine\"/productName=\"ProDB\"/g" ${top_dir}/packaging/tools/makearbi.sh
  # packaging/tools/remove_arbi.sh
  sed -i "s/TDengine/ProDB/g" ${top_dir}/packaging/tools/remove_arbi.sh
  # packaging/tools/install_arbi.sh
  sed -i "s/TDengine/ProDB/g" ${top_dir}/packaging/tools/install_arbi.sh
  sed -i "s/taosdata\.com/hanatech\.com\.cn/g" ${top_dir}/packaging/tools/install_arbi.sh

  # packaging/tools/make_install.sh
  sed -i "s/clientName=\"taos\"/clientName=\"prodbc\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/serverName=\"taosd\"/serverName=\"prodbs\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/logDir=\"\/var\/log\/taos\"/logDir=\"\/var\/log\/ProDB\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/dataDir=\"\/var\/lib\/taos\"/dataDir=\"\/var\/lib\/ProDB\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/configDir=\"\/etc\/taos\"/configDir=\"\/etc\/ProDB\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/configFile=\"taos\.cfg\"/configFile=\"prodb\.cfg\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/installDir=\"\/usr\/local\/taos\"/installDir=\"\/usr\/local\/ProDB\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/productName=\"TDengine\"/productName=\"ProDB\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/emailName=\"taosdata\.com\"/emailName=\"hanatech\.com\.cn\"/g" ${top_dir}/packaging/tools/make_install.sh
  sed -i "s/uninstallScript=\"rmtaos\"/uninstallScript=\"rmpro\"/g" ${top_dir}/packaging/tools/make_install.sh

  # packaging/rpm/taosd
  sed -i "s/TDengine/ProDB/g" ${top_dir}/packaging/rpm/taosd
  sed -i "s/usr\/local\/taos/usr\/local\/ProDB/g" ${top_dir}/packaging/rpm/taosd
  sed -i "s/taosd/prodbs/g" ${top_dir}/packaging/rpm/taosd
  # packaging/deb/taosd
  sed -i "s/TDengine/ProDB/g" ${top_dir}/packaging/deb/taosd
  sed -i "s/usr\/local\/taos/usr\/local\/ProDB/g" ${top_dir}/packaging/deb/taosd
  sed -i "s/taosd/prodbs/g" ${top_dir}/packaging/deb/taosd

}

function replace_enterprise_pro() {
  # enterprise/src/kit/perfMonitor/perfMonitor.c
  sed -i "s/\"taosdata\"/\"prodb\"/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
  sed -i "s/TDengine/ProDB/g" ${top_dir}/../enterprise/src/kit/perfMonitor/perfMonitor.c
  # enterprise/src/plugins/admin/src/httpAdminHandle.c
  sed -i "s/taos\.cfg/prodb\.cfg/g" ${top_dir}/../enterprise/src/plugins/admin/src/httpAdminHandle.c
  # enterprise/src/plugins/grant/src/grantMain.c
  sed -i "s/taos\.cfg/prodb\.cfg/g" ${top_dir}/../enterprise/src/plugins/grant/src/grantMain.c
  # enterprise/src/plugins/module/src/moduleMain.c
  sed -i "s/taos\.cfg/prodb\.cfg/g" ${top_dir}/../enterprise/src/plugins/module/src/moduleMain.c

  # enterprise/src/plugins/web
  sed -i -e "s/www\.taosdata\.com/www\.hanatech\.com\.cn/g" $(grep -r "www.taosdata.com" ${top_dir}/../enterprise/src/plugins/web | sed -r "s/(.*\.html):\s*(.*)/\1/g")
  sed -i -e "s/2017, TAOS Data/2021, Hanatech/g" $(grep -r "TAOS Data" ${top_dir}/../enterprise/src/plugins/web | sed -r "s/(.*\.html):\s*(.*)/\1/g")
  sed -i -e "s/taosd/prodbs/g" $(grep -r "taosd" ${top_dir}/../enterprise/src/plugins/web | grep -E "*\.js\s*.*" | sed -r -e "s/(.*\.js):\s*(.*)/\1/g" | sort | uniq)
  # enterprise/src/plugins/web/admin/monitor.html
  sed -i -e "s/<th style=\"font-weight: normal\">taosd<\/th>/<th style=\"font-weight: normal\">prodbs<\/th>/g" ${top_dir}/../enterprise/src/plugins/web/admin/monitor.html
  sed -i -e "s/data:\['taosd', 'system'\],/data:\['prodbs', 'system'\],/g" ${top_dir}/../enterprise/src/plugins/web/admin/monitor.html
  sed -i -e "s/name: 'taosd',/name: 'prodbs',/g" ${top_dir}/../enterprise/src/plugins/web/admin/monitor.html
  # enterprise/src/plugins/web/admin/*.html
  sed -i "s/TDengine/ProDB/g" ${top_dir}/../enterprise/src/plugins/web/admin/*.html
  # enterprise/src/plugins/web/admin/js/*.js
  sed -i "s/TDengine/ProDB/g" ${top_dir}/../enterprise/src/plugins/web/admin/js/*.js
}
