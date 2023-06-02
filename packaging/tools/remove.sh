#!/bin/bash
#
# Script to stop and uninstall the service, but retain the config, data and log files.

set -e
#set -x

verMode=edge
osType=`uname`

RED='\033[0;31m'
GREEN='\033[1;32m'
NC='\033[0m'

if [ "$osType" != "Darwin" ]; then
  installDir="/usr/local/taos"
  bin_link_dir="/usr/bin"
  lib_link_dir="/usr/lib"
  lib64_link_dir="/usr/lib64"
  inc_link_dir="/usr/include"
else
  if [ -d "/usr/local/Cellar/" ];then
    installDir="/usr/local/Cellar/tdengine/${verNumber}"
  elif [ -d "/opt/homebrew/Cellar/" ];then
    installDir="/opt/homebrew/Cellar/tdengine/${verNumber}"
  else
    installDir="/usr/local/taos"
  fi
  bin_link_dir="/usr/local/bin"
  lib_link_dir="/usr/local/lib"
  lib64_link_dir="/usr/local/lib"
  inc_link_dir="/usr/local/include"
fi
serverName="taosd"
clientName="taos"
uninstallScript="rmtaos"
productName="TDengine"

serverName2="taosd"
clientName2="taos"
productName2="TDengine"

adapterName2="${clientName2}adapter"
demoName2="${clientName2}demo"
benchmarkName2="${clientName2}Benchmark"
dumpName2="${clientName2}dump"
keeperName2="${clientName2}keeper"
xName2="${clientName2}x"
explorerName2="${clientName2}-explorer"
uninstallScript2="rm${clientName2}"

installDir="/usr/local/${clientName2}"

#install main path
install_main_dir=${installDir}
data_link_dir=${installDir}/data
log_link_dir=${installDir}/log
cfg_link_dir=${installDir}/cfg
local_bin_link_dir="/usr/local/bin"


service_config_dir="/etc/systemd/system"
taos_service_name=${serverName2}
taosadapter_service_name="${clientName2}adapter"
tarbitrator_service_name="tarbitratord"
csudo=""
if command -v sudo >/dev/null; then
  csudo="sudo "
fi

initd_mod=0
service_mod=2
if ps aux | grep -v grep | grep systemd &>/dev/null; then
  service_mod=0
elif $(which service &>/dev/null); then
  service_mod=1
  service_config_dir="/etc/init.d"
  if $(which chkconfig &>/dev/null); then
    initd_mod=1
  elif $(which insserv &>/dev/null); then
    initd_mod=2
  elif $(which update-rc.d &>/dev/null); then
    initd_mod=3
  else
    service_mod=2
  fi
else
  service_mod=2
fi

function kill_taosadapter() {
  pid=$(ps -ef | grep "${adapterName2}" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo}kill -9 $pid || :
  fi
}

function kill_taosd() {
  pid=$(ps -ef | grep ${serverName2} | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo}kill -9 $pid || :
  fi
}

function kill_tarbitrator() {
  pid=$(ps -ef | grep "tarbitrator" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo}kill -9 $pid || :
  fi
}

function clean_bin() {
  # Remove link
  ${csudo}rm -f ${bin_link_dir}/${clientName} || :
  ${csudo}rm -f ${bin_link_dir}/${serverName} || :
  ${csudo}rm -f ${bin_link_dir}/udfd || :
  ${csudo}rm -f ${bin_link_dir}/${adapterName2}     || :
  ${csudo}rm -f ${bin_link_dir}/${benchmarkName2}   || :
  ${csudo}rm -f ${bin_link_dir}/${demoName2}        || :
  ${csudo}rm -f ${bin_link_dir}/${dumpName2}        || :
  ${csudo}rm -f ${bin_link_dir}/${uninstallScript}  || :
  ${csudo}rm -f ${bin_link_dir}/tarbitrator || :
  ${csudo}rm -f ${bin_link_dir}/set_core || :
  ${csudo}rm -f ${bin_link_dir}/TDinsight.sh || :
  ${csudo}rm -f ${bin_link_dir}/${keeperName2}      || :
  ${csudo}rm -f ${bin_link_dir}/${xName2}           || :
  ${csudo}rm -f ${bin_link_dir}/${explorerName2}    || :

  if [ "$verMode" == "cluster" ] && [ "$clientName" != "$clientName2" ]; then
    ${csudo}rm -f ${bin_link_dir}/${clientName2} || :
    ${csudo}rm -f ${bin_link_dir}/${benchmarkName2} || :
    ${csudo}rm -f ${bin_link_dir}/${dumpName2} || :
    ${csudo}rm -f ${bin_link_dir}/${uninstallScript2} || :
  fi
}

function clean_local_bin() {
  ${csudo}rm -f ${local_bin_link_dir}/${benchmarkName2} || :
  ${csudo}rm -f ${local_bin_link_dir}/${demoName2}      || :
}

function clean_lib() {
  # Remove link
  ${csudo}rm -f ${lib_link_dir}/libtaos.* || :
  ${csudo}rm -f ${lib_link_dir}/librocksdb.* || :
  [ -f ${lib_link_dir}/libtaosws.* ] && ${csudo}rm -f ${lib_link_dir}/libtaosws.* || :

  ${csudo}rm -f ${lib64_link_dir}/libtaos.* || :
  ${csudo}rm -f ${lib64_link_dir}/librocksdb.* || :
  [ -f ${lib64_link_dir}/libtaosws.* ] && ${csudo}rm -f ${lib64_link_dir}/libtaosws.* || :
  #${csudo}rm -rf ${v15_java_app_dir}           || :
  
}

function clean_header() {
  # Remove link
  ${csudo}rm -f ${inc_link_dir}/taos.h || :
  ${csudo}rm -f ${inc_link_dir}/taosdef.h || :
  ${csudo}rm -f ${inc_link_dir}/taoserror.h || :
  ${csudo}rm -f ${inc_link_dir}/taosudf.h || :

  [ -f ${inc_link_dir}/taosws.h ] && ${csudo}rm -f ${inc_link_dir}/taosws.h || :
}

function clean_config() {
  # Remove link
  ${csudo}rm -f ${cfg_link_dir}/* || :
}

function clean_log() {
  # Remove link
  ${csudo}rm -rf ${log_link_dir} || :
}

function clean_service_on_systemd() {
  taosd_service_config="${service_config_dir}/${taos_service_name}.service"
  if systemctl is-active --quiet ${taos_service_name}; then
    echo "${productName2} ${serverName2} is running, stopping it..."
    ${csudo}systemctl stop ${taos_service_name} &>/dev/null || echo &>/dev/null
  fi
  ${csudo}systemctl disable ${taos_service_name} &>/dev/null || echo &>/dev/null
  ${csudo}rm -f ${taosd_service_config}

  taosadapter_service_config="${service_config_dir}/${clientName2}adapter.service"
  if systemctl is-active --quiet ${taosadapter_service_name}; then
    echo "${productName2}  ${clientName2}Adapter is running, stopping it..."
    ${csudo}systemctl stop ${taosadapter_service_name} &>/dev/null || echo &>/dev/null
  fi
  ${csudo}systemctl disable ${taosadapter_service_name} &>/dev/null || echo &>/dev/null
  [ -f ${taosadapter_service_config} ] && ${csudo}rm -f ${taosadapter_service_config}

  tarbitratord_service_config="${service_config_dir}/${tarbitrator_service_name}.service"
  if systemctl is-active --quiet ${tarbitrator_service_name}; then
    echo "${productName2} tarbitrator is running, stopping it..."
    ${csudo}systemctl stop ${tarbitrator_service_name} &>/dev/null || echo &>/dev/null
  fi
  ${csudo}systemctl disable ${tarbitrator_service_name} &>/dev/null || echo &>/dev/null

  x_service_config="${service_config_dir}/${xName2}.service"
  if [ -e "$x_service_config" ]; then
    if systemctl is-active --quiet ${xName2}; then
      echo "${productName2} ${xName2} is running, stopping it..."
      ${csudo}systemctl stop ${xName2} &>/dev/null || echo &>/dev/null
    fi
    ${csudo}systemctl disable ${xName2} &>/dev/null || echo &>/dev/null
    ${csudo}rm -f ${x_service_config}
  fi

  explorer_service_config="${service_config_dir}/${explorerName2}.service"
  if [ -e "$explorer_service_config" ]; then
    if systemctl is-active --quiet ${explorerName2}; then
      echo "${productName2} ${explorerName2} is running, stopping it..."
      ${csudo}systemctl stop ${explorerName2} &>/dev/null || echo &>/dev/null
    fi
    ${csudo}systemctl disable ${explorerName2} &>/dev/null || echo &>/dev/null
    ${csudo}rm -f ${explorer_service_config}
    ${csudo}rm -f /etc/${clientName2}/explorer.toml
  fi
}

function clean_service_on_sysvinit() {
  if ps aux | grep -v grep | grep ${serverName} &>/dev/null; then
    echo "${productName2} ${serverName2} is running, stopping it..."
    ${csudo}service ${serverName} stop || :
  fi

  if ps aux | grep -v grep | grep tarbitrator &>/dev/null; then
    echo "${productName2} tarbitrator is running, stopping it..."
    ${csudo}service tarbitratord stop || :
  fi

  if ((${initd_mod} == 1)); then
    if [ -e ${service_config_dir}/${serverName} ]; then
      ${csudo}chkconfig --del ${serverName} || :
    fi
    if [ -e ${service_config_dir}/tarbitratord ]; then
      ${csudo}chkconfig --del tarbitratord || :
    fi
  elif ((${initd_mod} == 2)); then
    if [ -e ${service_config_dir}/${serverName} ]; then
      ${csudo}insserv -r ${serverName} || :
    fi
    if [ -e ${service_config_dir}/tarbitratord ]; then
      ${csudo}insserv -r tarbitratord || :
    fi
  elif ((${initd_mod} == 3)); then
    if [ -e ${service_config_dir}/${serverName} ]; then
      ${csudo}update-rc.d -f ${serverName} remove || :
    fi
    if [ -e ${service_config_dir}/tarbitratord ]; then
      ${csudo}update-rc.d -f tarbitratord remove || :
    fi
  fi

  ${csudo}rm -f ${service_config_dir}/${serverName} || :
  ${csudo}rm -f ${service_config_dir}/tarbitratord || :

  if $(which init &>/dev/null); then
    ${csudo}init q || :
  fi
}

function clean_service_on_launchctl() {
  ${csudouser}launchctl unload -w /Library/LaunchDaemons/com.taosdata.taosd.plist > /dev/null 2>&1 || :
  ${csudo}rm /Library/LaunchDaemons/com.taosdata.taosd.plist > /dev/null 2>&1 || :
  ${csudouser}launchctl unload -w /Library/LaunchDaemons/com.taosdata.${clientName2}adapter.plist > /dev/null 2>&1 || :
  ${csudo}rm /Library/LaunchDaemons/com.taosdata.${clientName2}adapter.plist > /dev/null 2>&1 || :
}

function clean_service() {
  if ((${service_mod} == 0)); then
    clean_service_on_systemd
  elif ((${service_mod} == 1)); then
    clean_service_on_sysvinit
  else
    if [ "$osType" = "Darwin" ]; then
      clean_service_on_launchctl
    fi
    kill_taosadapter
    kill_taosd
    kill_tarbitrator
  fi
}

# Stop service and disable booting start.
clean_service
# Remove binary file and links
clean_bin
# Remove links of local bin
clean_local_bin
# Remove header file.
clean_header
# Remove lib file
clean_lib
# Remove link log directory
clean_log
# Remove link configuration file
clean_config
# Remove data link directory
${csudo}rm -rf ${data_link_dir} || :

${csudo}rm -rf ${install_main_dir}
if [[ -e /etc/os-release ]]; then
  osinfo=$(awk -F= '/^NAME/{print $2}' /etc/os-release)
else
  osinfo=""
fi

if echo $osinfo | grep -qwi "ubuntu"; then
  #  echo "this is ubuntu system"
  ${csudo}dpkg --force-all -P tdengine >/dev/null 2>&1 || :
elif echo $osinfo | grep -qwi "debian"; then
  #  echo "this is debian system"
  ${csudo}dpkg --force-all -P tdengine >/dev/null 2>&1 || :
elif echo $osinfo | grep -qwi "centos"; then
  #  echo "this is centos system"
  ${csudo}rpm -e --noscripts tdengine >/dev/null 2>&1 || :
fi
if [ "$osType" = "Darwin" ]; then
  ${csudo}rm -rf /Applications/TDengine.app
fi

echo -e "${GREEN}${productName2} is removed successfully!${NC}"
echo
