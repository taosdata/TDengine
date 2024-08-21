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

PREFIX="taos"

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

serverName="${PREFIX}d"
clientName="${PREFIX}"
uninstallScript="rm${PREFIX}"
adapterName="${PREFIX}adapter"
demoName="${PREFIX}demo"
benchmarkName="${PREFIX}Benchmark"
dumpName="${PREFIX}dump"
keeperName="${PREFIX}keeper"
xName="${PREFIX}x"
explorerName="${PREFIX}-explorer"
tarbitratorName="tarbitratord"
productName="TDengine"

#install main path
install_main_dir=${installDir}
data_link_dir=${installDir}/data
log_link_dir=${installDir}/log
cfg_link_dir=${installDir}/cfg
local_bin_link_dir="/usr/local/bin"
service_config_dir="/etc/systemd/system"
config_dir="/etc/${PREFIX}"

if [ "${verMode}" == "cluster" ]; then
  services=(${PREFIX}"d" ${PREFIX}"adapter" ${PREFIX}"keeper")
else
  services=(${PREFIX}"d" ${PREFIX}"adapter" ${PREFIX}"keeper" ${PREFIX}"-explorer")
fi
tools=(${PREFIX} ${PREFIX}"Benchmark" ${PREFIX}"dump" ${PREFIX}"demo" udfd set_core.sh TDinsight.sh $uninstallScript start-all.sh stop-all.sh)

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

kill_service_of() {
  _service=$1
  pid=$(ps -ef | grep $_service | grep -v grep | grep -v $uninstallScript | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo}kill -9 $pid || :
  fi
}

clean_service_on_systemd_of() {
  _service=$1
  _service_config="${service_config_dir}/${_service}.service"
  if systemctl is-active --quiet ${_service}; then
    echo "${_service} is running, stopping it..."
    ${csudo}systemctl stop ${_service} &>/dev/null || echo &>/dev/null
  fi
  ${csudo}systemctl disable ${_service} &>/dev/null || echo &>/dev/null
  ${csudo}rm -f ${_service_config}
}

clean_service_on_sysvinit_of() {
  _service=$1
  if pidof ${_service} &>/dev/null; then
    echo "${_service} is running, stopping it..."
    ${csudo}service ${_service} stop || :
  fi
  if ((${initd_mod} == 1)); then
    if [ -e ${service_config_dir}/${_service} ]; then
      ${csudo}chkconfig --del ${_service} || :
    fi
  elif ((${initd_mod} == 2)); then
    if [ -e ${service_config_dir}/${_service} ]; then
      ${csudo}insserv -r ${_service} || :
    fi
  elif ((${initd_mod} == 3)); then
    if [ -e ${service_config_dir}/${_service} ]; then
      ${csudo}update-rc.d -f ${_service} remove || :
    fi
  fi

  ${csudo}rm -f ${service_config_dir}/${_service} || :

  if $(which init &>/dev/null); then
    ${csudo}init q || :
  fi
}

clean_service_of() {
  _service=$1
  if ((${service_mod} == 0)); then
    clean_service_on_systemd_of $_service
  elif ((${service_mod} == 1)); then
    clean_service_on_sysvinit_of $_service
  else
    kill_service_of $_service
  fi
}

remove_service_of() {
  _service=$1
  clean_service_of ${_service}
  if [[ -e "${bin_link_dir}/${_service}" || -e "${installDir}/bin/${_service}" || -e "${local_bin_link_dir}/${_service}" ]]; then
    ${csudo}rm -rf ${bin_link_dir}/${_service}
    ${csudo}rm -rf ${installDir}/bin/${_service} 
    ${csudo}rm -rf ${local_bin_link_dir}/${_service}
    echo "${_service} is removed successfully!"
  fi
}

remove_tools_of() {
  _tool=$1
  kill_service_of ${_tool}
  [ -e "${bin_link_dir}/${_tool}" ] && ${csudo}rm -rf ${bin_link_dir}/${_tool} || :
  [ -e "${installDir}/bin/${_tool}" ] && ${csudo}rm -rf ${installDir}/bin/${_tool} || :
  [ -e "${local_bin_link_dir}/${_tool}" ] && ${csudo}rm -rf ${local_bin_link_dir}/${_tool} || :
}

remove_bin() {
  for _service in "${services[@]}"; do
    remove_service_of ${_service}
  done

  for _tool in "${tools[@]}"; do    
    remove_tools_of ${_tool}
  done
}

function clean_lib() {
  # Remove link
  ${csudo}rm -f ${lib_link_dir}/libtaos.* || :
  [ -f ${lib_link_dir}/libtaosws.* ] && ${csudo}rm -f ${lib_link_dir}/libtaosws.* || :

  ${csudo}rm -f ${lib64_link_dir}/libtaos.* || :
  [ -f ${lib64_link_dir}/libtaosws.* ] && ${csudo}rm -f ${lib64_link_dir}/libtaosws.* || :
  #${csudo}rm -rf ${v15_java_app_dir}           || :
}

function clean_header() {
  # Remove link
  ${csudo}rm -f ${inc_link_dir}/taos.h || :
  ${csudo}rm -f ${inc_link_dir}/taosdef.h || :
  ${csudo}rm -f ${inc_link_dir}/taoserror.h || :
  ${csudo}rm -f ${inc_link_dir}/tdef.h || :
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

function clean_service_on_launchctl() {
  ${csudouser}launchctl unload -w /Library/LaunchDaemons/com.taosdata.taosd.plist > /dev/null 2>&1 || :
  ${csudo}rm /Library/LaunchDaemons/com.taosdata.taosd.plist > /dev/null 2>&1 || :
  ${csudouser}launchctl unload -w /Library/LaunchDaemons/com.taosdata.${clientName2}adapter.plist > /dev/null 2>&1 || :
  ${csudo}rm /Library/LaunchDaemons/com.taosdata.${clientName2}adapter.plist > /dev/null 2>&1 || :
}

function remove_data_and_config() {
  data_dir=`grep dataDir /etc/${PREFIX}/${PREFIX}.cfg | grep -v '#' | tail -n 1 | awk {'print $2'}`
  if [ X"$data_dir" == X"" ]; then
    data_dir="/var/lib/${PREFIX}"
  fi
  log_dir=`grep logDir /etc/${PREFIX}/${PREFIX}.cfg | grep -v '#' | tail -n 1 | awk {'print $2'}`
  if [ X"$log_dir" == X"" ]; then
    log_dir="/var/log/${PREFIX}"
  fi  
  [ -d "${config_dir}" ] && ${csudo}rm -rf ${config_dir}
  [ -d "${data_dir}" ] && ${csudo}rm -rf ${data_dir}
  [ -d "${log_dir}" ] && ${csudo}rm -rf ${log_dir}
}

echo 
echo "Do you want to remove all the data, log and configuration files? [y/n]"
read answer
if [ X$answer == X"y" ] || [ X$answer == X"Y" ]; then
  confirmMsg="I confirm that I would like to delete all data, log and configuration files"
  echo "Please enter '${confirmMsg}' to continue"
  read answer
  if [ X"$answer" == X"${confirmMsg}" ]; then
    remove_data_and_config
    if [ -e ${install_main_dir}/uninstall_${PREFIX}x.sh ]; then
      bash ${install_main_dir}/uninstall_${PREFIX}x.sh --clean-all true
    fi
  else    
    echo "answer doesn't match, skip this step"
    if [ -e ${install_main_dir}/uninstall_${PREFIX}x.sh ]; then
      bash ${install_main_dir}/uninstall_${PREFIX}x.sh --clean-all false
    fi
  fi
fi

remove_bin
clean_header
# Remove lib file
clean_lib
# Remove link log directory
clean_log
# Remove link configuration file
clean_config
# Remove data link directory
${csudo}rm -rf ${data_link_dir} || :
${csudo}rm -rf ${install_main_dir} || :
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
  clean_service_on_launchctl
  ${csudo}rm -rf /Applications/TDengine.app
fi

command -v systemctl >/dev/null 2>&1 && ${csudo}systemctl daemon-reload >/dev/null 2>&1 || true 
echo 
echo "${productName} is removed successfully!"
echo
