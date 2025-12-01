#!/bin/bash
#
# Script to stop and uninstall the service, but retain the config, data and log files.

set -e
#set -x

verMode=edge
osType=$(uname)
entMode=full
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
  if [ -d "/usr/local/Cellar/" ]; then
    installDir="/usr/local/Cellar/tdengine/${verNumber}"
  elif [ -d "/opt/homebrew/Cellar/" ]; then
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
inspect_name="${PREFIX}inspect"
tarbitratorName="tarbitratord"
mqtt_name="${PREFIX}mqtt"
taosgen_name="${PREFIX}gen"
productName="TDengine TSDB"

#install main path
install_main_dir=${installDir}
data_link_dir=${installDir}/data
log_link_dir=${installDir}/log
cfg_link_dir=${installDir}/cfg
local_bin_link_dir="/usr/local/bin"
service_config_dir="/etc/systemd/system"
config_dir="/etc/${PREFIX}"
bin_dir="${installDir}/bin"
cfg_dir="${installDir}/cfg"
connector_dir="${installDir}/connector"
driver_dir="${installDir}/driver"
examples_dir="${installDir}/examples"
include_dir="${installDir}/include"
plugins_dir="${installDir}/plugins"
share_dir="${installDir}/share"



if [ "${verMode}" == "cluster" ]; then
  if [ "${entMode}" == "full" ]; then
    services=("${serverName}" ${adapterName} "${keeperName}")
  else
    services=("${serverName}" ${adapterName} "${keeperName}" "${explorerName}")
  fi
  tools=("${clientName}" "${benchmarkName}" "${dumpName}" "${demoName}" "${inspect_name}" "${PREFIX}udf" "${mqtt_name}" "set_core.sh" "TDinsight.sh" "$uninstallScript" "start-all.sh" "stop-all.sh" "${taosgen_name}")
else
  tools=("${clientName}" "${benchmarkName}" "${dumpName}" "${demoName}" "${PREFIX}udf" "${mqtt_name}" "set_core.sh" "TDinsight.sh" "$uninstallScript" "start-all.sh" "stop-all.sh" "${taosgen_name}")
  services=("${serverName}" ${adapterName} "${keeperName}" "${explorerName}")
fi

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
  pid=$(ps aux | grep -w $_service | grep -v grep | grep -v $uninstallScript | awk '{print $2}')
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
  fi
  kill_service_of $_service
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
  [ -L "${bin_link_dir}/${_tool}" ] && ${csudo}unlink ${bin_link_dir}/${_tool} || :
  [ -e "${installDir}/bin/${_tool}" ] && ${csudo}rm -rf ${installDir}/bin/${_tool} || :
  [ -L "${local_bin_link_dir}/${_tool}" ] && ${csudo}unlink ${local_bin_link_dir}/${_tool} || :
}

remove_bin() {
  for _service in "${services[@]}"; do
    if [ -z "$_service" ]; then
      continue
    fi
    remove_service_of "${_service}"
  done

  for _tool in "${tools[@]}"; do
    if [ -z "$_tool" ]; then
      continue
    fi
    remove_tools_of "${_tool}"
  done
}

function clean_lib() {
  # Remove link
  for dir in "${lib_link_dir}" "${lib64_link_dir}"; do
    if [ -d "$dir" ]; then
      for pattern in "libtaos.*" "libtaosnative.*" "libtaosws.*"; do
        ${csudo}find "$dir" -name "$pattern" -exec ${csudo}rm -f {} \; || :
      done
    fi
  done
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
  ${csudo}launchctl unload -w /Library/LaunchDaemons/com.taosdata.${serverName}.plist || :
  ${csudo}launchctl unload -w /Library/LaunchDaemons/com.taosdata.${adapterName}.plist || :
  ${csudo}launchctl unload -w /Library/LaunchDaemons/com.taosdata.${keeperName}.plist || :
  ${csudo}launchctl unload -w /Library/LaunchDaemons/com.taosdata.${explorerName}.plist || :

  ${csudo}launchctl remove com.tdengine.taosd || :
  ${csudo}launchctl remove com.tdengine.${adapterName} || :
  ${csudo}launchctl remove com.tdengine.${keeperName} || :
  ${csudo}launchctl remove com.tdengine.${explorerName} || :

  ${csudo}rm /Library/LaunchDaemons/com.taosdata.* >/dev/null 2>&1 || :
}

function batch_remove_paths_and_clean_dir() {
  local dir="$1"
  shift
  local paths=("$@")
  for path in "${paths[@]}"; do
    ${csudo}rm -rf "$path" || :
  done
  ${csudo}find "$dir" -type d -empty -delete || :
  if [ -z "$(ls -A "$dir" 2>/dev/null)" ]; then
    ${csudo}rm -rf "$dir" || :
  fi
}

function remove_data_and_config() {
  echo "Starting to remove configuration, data and log files..."

  data_dir=$(grep dataDir /etc/${PREFIX}/${PREFIX}.cfg | grep -v '#' | tail -n 1 | awk {'print $2'})
  if [ -z "$data_dir" ]; then
    data_dir="/var/lib/${PREFIX}"
  fi

  log_dir=$(grep logDir /etc/${PREFIX}/${PREFIX}.cfg | grep -v '#' | tail -n 1 | awk {'print $2'})
  if [ -z "$log_dir" ]; then
    log_dir="/var/log/${PREFIX}"
  fi
  
  
  if [ -d "${config_dir}" ]; then
    ${csudo}rm -rf ${config_dir}
    echo "Configuration directory removed: ${config_dir}"
  fi

  if [ -d "${data_dir}" ]; then
    data_remove_list=(
      "${data_dir}/dnode"
      "${data_dir}/mnode"
      "${data_dir}/vnode"
      "${data_dir}/.udf"
      "${data_dir}/.running"*
      "${data_dir}/.taosudf"*
      "${data_dir}/${PREFIX}x"*
      "${data_dir}/explorer"*
    )
    batch_remove_paths_and_clean_dir "${data_dir}" "${data_remove_list[@]}"
    echo "Data directory removed: ${data_dir}"
  fi
  
  if [ -d "${log_dir}" ]; then
    log_remove_list=(
      "${log_dir}/taos"*
      "${log_dir}/udf"*
      "${log_dir}/jemalloc"
      "${log_dir}/tcmalloc"
      "${log_dir}/set_taos_malloc.log"
      "${log_dir}/.startRecord"
      "${log_dir}/.startSeq"
    )
    batch_remove_paths_and_clean_dir "${log_dir}" "${log_remove_list[@]}"
    echo "Log directory removed: ${log_dir}"
  fi
}

function usage() {
  echo -e "\nUsage: $(basename $0) [-e <yes|no>]"
  echo "-e: silent mode, specify whether to remove all the data, log and configuration files."
  echo "  yes: remove the data, log, and configuration files."
  echo "  no:  don't remove the data, log, and configuration files."
}

function remove_install_dir() {
  for dir in \
    "${bin_dir}" \
    "${cfg_dir}" \
    "${connector_dir}" \
    "${driver_dir}" \
    "${examples_dir}" \
    "${include_dir}" \
    "${plugins_dir}" \
    "${share_dir}"
  do
    if [ -d "$dir" ]; then
      ${csudo}rm -rf "$dir"
    fi
  done
  if [ -f "${install_main_dir}/README.md" ]; then
    ${csudo}rm -f "${install_main_dir}/README.md"
  fi
  if [ -f "${install_main_dir}/uninstall_taosx.sh" ]; then
    ${csudo}rm -f "${install_main_dir}/uninstall_taosx.sh"
  fi
  if [ -f "${install_main_dir}/uninstall.sh" ]; then
    ${csudo}rm -f "${install_main_dir}/uninstall.sh"
  fi
  if [ -d "${install_main_dir}" ] && [ -z "$(ls -A "${install_main_dir}")" ]; then
    ${csudo}rm -rf "${install_main_dir}" || :
  fi
}

# main
interactive_remove="yes"
remove_flag="false"

while getopts "e:h" opt; do
  case $opt in
  e)
    interactive_remove="no"

    if [ "$OPTARG" == "yes" ]; then
      remove_flag="true"
      echo "Remove all the data, log, and configuration files."
    elif [ "$OPTARG" == "no" ]; then
      remove_flag="false"
      echo "Do not remove the data, log, and configuration files."
    else
      echo "Invalid option for -e: $OPTARG"
      usage
      exit 1
    fi
    ;;
  h | *)
    usage
    exit 1
    ;;
  esac
done

if [ "$interactive_remove" == "yes" ]; then
  echo -e "\nDo you want to remove all the data, log and configuration files? [y/n]"
  read answer
  if [ X$answer == X"y" ] || [ X$answer == X"Y" ]; then
    confirmMsg="I confirm that I would like to delete all data, log and configuration files"
    echo "Please enter '${confirmMsg}' to continue"
    read answer
    if [ X"$answer" == X"${confirmMsg}" ]; then
      remove_flag="true"
    else
      echo "answer doesn't match, skip this step"
    fi
  fi
  echo
fi

if [ -e ${install_main_dir}/uninstall_${PREFIX}x.sh ]; then
  if [ X$remove_flag == X"true" ]; then
    bash ${install_main_dir}/uninstall_${PREFIX}x.sh --clean-all true
  else
    bash ${install_main_dir}/uninstall_${PREFIX}x.sh --clean-all false
  fi
fi

if [ "$osType" = "Darwin" ]; then
  clean_service_on_launchctl
  ${csudo}rm -rf /Applications/TDengine.app
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

if [ X$remove_flag == X"true" ]; then
  remove_data_and_config
fi

remove_install_dir

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

command -v systemctl >/dev/null 2>&1 && ${csudo}systemctl daemon-reload >/dev/null 2>&1 || true
echo
echo "${productName} is removed successfully!"
echo
