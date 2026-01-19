#!/bin/bash
#
# Script to stop and uninstall the service, but retain the config, data and log files.

set -e

# various settings
verMode=edge
osType=$(uname)
entMode=full
RED='\033[0;31m'
GREEN='\033[1;32m'
NC='\033[0m'

PREFIX="taos"

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
xnode_name="xnoded"
productName="TDengine TSDB"

function usage() {
  echo -e "\nUsage: $(basename $0) [-e <yes|no>] [-d <install_dir>]"
  echo "-e: silent mode, specify whether to remove all the data, log and configuration files."
  echo "  yes: remove the data, log, and configuration files."
  echo "  no:  don't remove the data, log, and configuration files."
  echo "-d: (optional) custom install directory, e.g. /usr/local/${PREFIX} (only needed if auto-detect fails)"
}

# main
interactive_remove="yes"
remove_flag="false"
customDir=""
while getopts "e:d:h" opt; do
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
    d)
      customDir="$OPTARG"
      ;;
    h | *)
      usage
      exit 1
      ;;
  esac
done

# Default path

# 2. installDir decision
if [ -n "$customDir" ]; then
  installDir="$customDir"
else
  # 1. Guess install path
  script_real_path="$(readlink -f "$0" 2>/dev/null || realpath "$0" 2>/dev/null || echo "$0")"
  script_dir="$(dirname "$script_real_path")"
  if [[ "$script_dir" == */bin ]]; then
    guessed_install_path="${script_dir%/bin}"
  else
    guessed_install_path="$script_dir"
  fi

  # 2. installDir decision
  if [ -f "${guessed_install_path}/.install_path" ]; then
    installDir=$(cat "${guessed_install_path}/.install_path")
  elif [ -f "/usr/local/${PREFIX}/.install_path" ]; then
    installDir=$(cat "/usr/local/${PREFIX}/.install_path")
  elif [ -f "$HOME/${PREFIX}/.install_path" ]; then
    installDir=$(cat "$HOME/${PREFIX}/.install_path")
  elif [ -d "${guessed_install_path}" ]; then
    installDir="${guessed_install_path}"
  elif [ -d "/usr/local/${PREFIX}" ]; then
    installDir="/usr/local/${PREFIX}"
  elif [ -d "$HOME/${PREFIX}" ]; then
    installDir="$HOME/${PREFIX}"
  else
    echo -e "${RED}Install directory not found, please specify the path with -d${NC}"
    usage
    exit 1
  fi
fi


validate_safe_path() {
  local path="$1"
  # Disallow empty path and critical system directories to avoid destructive operations
  case "$path" in
    ""|"/"|"/etc"|"/bin"|"/lib"|"/usr"|"/sbin"|"/boot"|"/root"|"/home"|"/var"|"/tmp"|"/dev"|"/proc"|"/sys"|"/run")
      echo -e "${RED}Refusing to operate on dangerous system path: $path${NC}"
      exit 1
      ;;
    *)
      # Allow application-specific directories such as /data/tdengine, /opt/taos, 
      # /usr/local/taos, $HOME/taos, $HOME/.local/, etc.
      return 0
      ;;
  esac
}
validate_safe_path "$installDir"

# Set installDir based on custom path option or default path
# 3. user_mode and related path variables initialization (all based on installDir)
if [ "$(id -u)" -eq 0 ]; then
  user_mode=0
  bin_link_dir="/usr/bin"
  lib_link_dir="/usr/lib"
  lib64_link_dir="/usr/lib64"
  inc_link_dir="/usr/include"
  config_dir="/etc/${PREFIX}"
  service_config_dir="/etc/systemd/system"
  sysctl_cmd="systemctl"
  # macOS root special handling
  if [ "$osType" == "Darwin" ]; then
    bin_link_dir="/usr/local/bin"
    lib_link_dir="/usr/local/lib"
    lib64_link_dir="/usr/local/lib"
    inc_link_dir="/usr/local/include"
  fi
else
  user_mode=1
  bin_link_dir="$HOME/.local/bin"
  lib_link_dir="$HOME/.local/lib"
  lib64_link_dir="$HOME/.local/lib64"
  inc_link_dir="$HOME/.local/include"
  config_dir="${installDir}/cfg"
  service_config_dir="$HOME/.config/systemd/user"
  sysctl_cmd="systemctl --user"
fi

# 4. All other path variables are based on installDir
install_main_dir="${installDir}"
data_link_dir="${installDir}/data"
log_link_dir="${installDir}/log"
cfg_link_dir="${installDir}/cfg"
local_bin_link_dir="/usr/local/bin"
bin_dir="${installDir}/bin"
connector_dir="${installDir}/connector"
driver_dir="${installDir}/driver"
examples_dir="${installDir}/examples"
include_dir="${installDir}/include"
plugins_dir="${installDir}/plugins"
share_dir="${installDir}/share"

if [ "${verMode}" == "cluster" ]; then
  if [ "${entMode}" == "full" ]; then
    services=("${serverName}" "${adapterName}" "${keeperName}" "${xName}" "${explorerName}")
  else
    services=("${serverName}" "${adapterName}" "${keeperName}" "${explorerName}")
  fi
  tools=("${clientName}" "${benchmarkName}" "${dumpName}" "${demoName}" "${inspect_name}" "${PREFIX}udf" "${mqtt_name}" "${xnode_name}" "set_core.sh" "TDinsight.sh" "$uninstallScript" "start-all.sh" "stop-all.sh" "${taosgen_name}" "startPre.sh" "uninstall_taosx.sh")
else
  tools=("${clientName}" "${benchmarkName}" "${dumpName}" "${demoName}" "${PREFIX}udf" "${mqtt_name}" "${xnode_name}" "set_core.sh" "TDinsight.sh" "$uninstallScript" "start-all.sh" "stop-all.sh" "${taosgen_name}" "startPre.sh")
  services=("${serverName}" "${adapterName}" "${keeperName}" "${explorerName}")
fi

initd_mod=0
service_mod=2
if ps aux | grep -v grep | grep systemd &>/dev/null; then
  service_mod=0
elif $(which service &>/dev/null); then
  service_mod=1
  # Only set sysvinit service directory if not in user mode
  if [ "$user_mode" -eq 0 ]; then
    service_config_dir="/etc/init.d"
  fi
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
  local svc=$1
  # grep -v -x "$$" : exclude the current script's own PID
  # ps -o pid=,comm= -p ... : get pid and command name
  # awk '$2 != "rmtaos" && $2 != "uninstall.sh" {print $1}' : exclude rmtaos and uninstall.sh processes
  pids=$(ps -eo pid=,comm= | awk -v svc="$svc" '$2 == svc {print $1}' || true)
  if [ -n "$pids" ]; then
    echo "$pids" | xargs -r ps -o pid=,comm= -p 2>/dev/null \
      | awk '$2 != "rmtaos" && $2 != "uninstall.sh" {print $1}' \
      | xargs -r kill -9 2>/dev/null || true
  fi
}

clean_service_on_systemd_of() {
  _service=$1
  _service_config="${service_config_dir}/${_service}.service"
  if ${sysctl_cmd} is-active --quiet ${_service}; then
    echo "${_service} is running, stopping it..."
    ${sysctl_cmd} stop ${_service} &>/dev/null || echo &>/dev/null
  fi
  ${sysctl_cmd} disable ${_service} &>/dev/null || echo &>/dev/null
  rm -f ${_service_config}
}

clean_service_on_sysvinit_of() {
  _service=$1
  if pidof ${_service} &>/dev/null; then
    echo "${_service} is running, stopping it..."
    service ${_service} stop || :
  fi
  if ((${initd_mod} == 1)); then
    if [ -e ${service_config_dir}/${_service} ]; then
      chkconfig --del ${_service} || :
    fi
  elif ((${initd_mod} == 2)); then
    if [ -e ${service_config_dir}/${_service} ]; then
      insserv -r ${_service} || :
    fi
  elif ((${initd_mod} == 3)); then
    if [ -e ${service_config_dir}/${_service} ]; then
      update-rc.d -f ${_service} remove || :
    fi
  fi

  rm -f ${service_config_dir}/${_service} || :

  if $(which init &>/dev/null); then
    init q || :
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
    rm -rf "${bin_link_dir:?}/${_service}"
    rm -rf "${installDir:?}/bin/${_service}"
    rm -rf "${local_bin_link_dir:?}/${_service}"
    echo "${_service} is removed successfully!"
  fi
}

remove_tools_of() {
  _tool=$1
  kill_service_of ${_tool}
  [ -L "${bin_link_dir:?}/${_tool}" ] && unlink "${bin_link_dir:?}/${_tool}" || :
  [ -e "${installDir:?}/bin/${_tool}" ] && rm -rf "${installDir:?}/bin/${_tool}" || :
  [ -L "${local_bin_link_dir:?}/${_tool}" ] && unlink "${local_bin_link_dir:?}/${_tool}" || :
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
        find "${dir:?}" -name "$pattern" -exec rm -f {} \; || :
      done
    fi
  done
}

function clean_header() {
  # Remove link
  rm -f "${inc_link_dir:?}/taos.h" || :
  rm -f "${inc_link_dir:?}/taosdef.h" || :
  rm -f "${inc_link_dir:?}/taoserror.h" || :
  rm -f "${inc_link_dir:?}/tdef.h" || :
  rm -f "${inc_link_dir:?}/taosudf.h" || :
  [ -f "${inc_link_dir:?}/taosws.h" ] && rm -f "${inc_link_dir:?}/taosws.h" || :
}

function clean_config() {
  # Remove link
  if [ -L "${cfg_link_dir:?}" ]; then
    rm -f "${cfg_link_dir:?}"
  fi
}

function clean_log() {
  # Remove link
  if [ -L "${log_link_dir:?}" ]; then
    rm -f "${log_link_dir:?}"
  fi
}

function clean_service_on_launchctl() {
  # root: /Library/LaunchDaemons, user: ~/Library/LaunchAgents
  if [ "$user_mode" -eq 0 ]; then
    plist_dir="/Library/LaunchDaemons"
    prefix="com.taosdata"
  else
    plist_dir="$HOME/Library/LaunchAgents"
    prefix="com.taosdata"
  fi

  launchctl unload -w "${plist_dir}/${prefix}.${serverName}.plist" 2>/dev/null || :
  launchctl unload -w "${plist_dir}/${prefix}.${adapterName}.plist" 2>/dev/null || :
  launchctl unload -w "${plist_dir}/${prefix}.${keeperName}.plist" 2>/dev/null || :
  launchctl unload -w "${plist_dir}/${prefix}.${explorerName}.plist" 2>/dev/null || :
  launchctl unload -w "${plist_dir}/${prefix}.${xName}.plist" 2>/dev/null || :

  launchctl remove "com.tdengine.${serverName}" 2>/dev/null || :
  launchctl remove "com.tdengine.${adapterName}" 2>/dev/null || :
  launchctl remove "com.tdengine.${keeperName}" 2>/dev/null || :
  launchctl remove "com.tdengine.${explorerName}" 2>/dev/null || :
  launchctl remove "com.tdengine.${xName}" 2>/dev/null || :

  rm -f "${plist_dir}/${prefix}."*".plist" >/dev/null 2>&1 || :
}

function batch_remove_paths_and_clean_dir() {
  local dir="$1"
  shift
  local paths=("$@")
  for path in "${paths[@]}"; do
    rm -rf "${path:?}" || :
  done
  find "${dir:?}" -type d -empty -delete || :
  if [ -z "$(ls -A "${dir:?}" 2>/dev/null)" ]; then
    rm -rf "${dir:?}" || :
  fi
}

function remove_data_and_config() {
  echo "Starting to remove configuration, data and log files..."
  local config_file="${config_dir:?}/${PREFIX}.cfg"
  # Get data and log directories from config file, with user mode defaults
  if [ -f "$config_file" ]; then
    data_dir=$(grep dataDir "$config_file" | grep -v '#' | tail -n 1 | awk {'print $2'})
    log_dir=$(grep logDir "$config_file" | grep -v '#' | tail -n 1 | awk {'print $2'})
  fi

  # Set defaults based on user mode
  if [ -z "$data_dir" ]; then
    if [ "$user_mode" -eq 0 ]; then
      data_dir="/var/lib/${PREFIX}"
    else
      data_dir="${installDir:?}/data"
    fi
  fi

  if [ -z "$log_dir" ]; then
    if [ "$user_mode" -eq 0 ]; then
      log_dir="/var/log/${PREFIX}"
    else
      log_dir="${installDir:?}/log"
    fi
  fi

  if [ -d "${config_dir:?}" ]; then
    rm -rf "${config_dir:?}"
    echo "Configuration directory removed: ${config_dir:?}"
  fi

  if [ -d "${data_dir:?}" ]; then
    data_remove_list=(
      "${data_dir:?}/dnode"
      "${data_dir:?}/mnode"
      "${data_dir:?}/vnode"
      "${data_dir:?}/snode"
      "${data_dir:?}/xnode"
      "${data_dir:?}/.udf"
      "${data_dir:?}/.running"*
      "${data_dir:?}/.taosudf"*
      "${data_dir:?}/${PREFIX}x"*
      "${data_dir:?}/explorer"*
    )
    batch_remove_paths_and_clean_dir "${data_dir:?}" "${data_remove_list[@]}"
    echo "Data directory removed: ${data_dir:?}"
  fi

  if [ -d "${log_dir:?}" ]; then
    log_remove_list=(
      "${log_dir:?}/taos"*
      "${log_dir:?}/udf"*
      "${log_dir:?}/jemalloc"
      "${log_dir:?}/tcmalloc"
      "${log_dir:?}/set_taos_malloc.log"
      "${log_dir:?}/.startRecord"
      "${log_dir:?}/.startSeq"
    )
    batch_remove_paths_and_clean_dir "${log_dir:?}" "${log_remove_list[@]}"
    echo "Log directory removed: ${log_dir:?}"
  fi
}

function remove_install_dir() {
  for dir in \
    "${bin_dir}" \
    "${connector_dir}" \
    "${driver_dir}" \
    "${examples_dir}" \
    "${include_dir}" \
    "${plugins_dir}" \
    "${share_dir}"
  do
    if [ -d "${dir:?}" ]; then
      rm -rf "${dir:?}"
    fi
  done
  if [ -f "${install_main_dir:?}/README.md" ]; then
    rm -f "${install_main_dir:?}/README.md"
  fi
  if [ -f "${install_main_dir:?}/uninstall_taosx.sh" ]; then
    rm -f "${install_main_dir:?}/uninstall_taosx.sh"
  fi
  if [ -f "${install_main_dir:?}/uninstall.sh" ]; then
    rm -f "${install_main_dir:?}/uninstall.sh"
  fi
  if [ -f "${install_main_dir:?}/.install_path" ]; then
    rm -f "${install_main_dir:?}/.install_path"
  fi
  if [ -d "${install_main_dir:?}" ] && [ -z "$(ls -A "${install_main_dir:?}")" ]; then
    rm -rf "${install_main_dir:?}" || :
  fi
}
# ====== main logic starts here ======
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

# if [ -e "${install_main_dir}/uninstall_${PREFIX}x.sh" ]; then
#   if [ X$remove_flag == X"true" ]; then
#     bash "${install_main_dir}/uninstall_${PREFIX}x.sh" --clean-all true
#   else
#     bash "${install_main_dir}/uninstall_${PREFIX}x.sh" --clean-all false
#   fi
# fi

if [ "$osType" = "Darwin" ]; then
  clean_service_on_launchctl
  rm -rf /Applications/TDengine.app
fi

remove_bin
clean_header
# Remove lib file
clean_lib
# Remove link log directory
clean_log
# Remove link configuration file
clean_config

if [ X$remove_flag == X"true" ]; then
  remove_data_and_config
  if [ -L "${data_link_dir}" ]; then
    rm -f "${data_link_dir}"
  fi
fi

remove_install_dir

if [[ -e /etc/os-release ]]; then
  osinfo=$(awk -F= '/^NAME/{print $2}' /etc/os-release)
else
  osinfo=""
fi

if echo $osinfo | grep -qwi "ubuntu"; then
  dpkg --force-all -P tdengine >/dev/null 2>&1 || :
elif echo $osinfo | grep -qwi "debian"; then
  dpkg --force-all -P tdengine >/dev/null 2>&1 || :
elif echo $osinfo | grep -qwi "centos"; then
  rpm -e --noscripts tdengine >/dev/null 2>&1 || :
fi

command -v systemctl >/dev/null 2>&1 && ${sysctl_cmd} daemon-reload >/dev/null 2>&1 || true
echo
echo "${productName} is removed successfully!"
echo