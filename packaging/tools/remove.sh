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
  bin_link_dir="/usr/bin"
  lib_link_dir="/usr/lib"
  lib64_link_dir="/usr/lib64"
  inc_link_dir="/usr/include"
else
  bin_link_dir="/usr/local/bin"
  lib_link_dir="/usr/local/lib"
  lib64_link_dir="/usr/local/lib"
  inc_link_dir="/usr/local/include"
fi

# Install path discovery: prefer the .install_path sentinel written by install.sh.
_script_real="$(readlink -f "$0" 2>/dev/null || echo "$0")"
_script_dir="$(dirname "$_script_real")"
if [[ "$_script_dir" == */bin ]]; then
  _guessed_dir="${_script_dir%/bin}"
else
  _guessed_dir="$_script_dir"
fi

if [ -f "${_guessed_dir}/.install_path" ]; then
  installDir=$(cat "${_guessed_dir}/.install_path")
elif [ -f "/usr/local/${PREFIX}/.install_path" ]; then
  installDir=$(cat "/usr/local/${PREFIX}/.install_path")
elif [ -f "$HOME/${PREFIX}/.install_path" ]; then
  installDir=$(cat "$HOME/${PREFIX}/.install_path")
elif [ "$osType" != "Darwin" ]; then
  installDir="/usr/local/${PREFIX}"
else
  if [ -d "/usr/local/Cellar/" ]; then
    installDir="/usr/local/Cellar/tdengine/"
  elif [ -d "/opt/homebrew/Cellar/" ]; then
    installDir="/opt/homebrew/Cellar/tdengine/"
  else
    installDir="/usr/local/${PREFIX}"
  fi
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
productName="TDengine TSDB"

#install main path
install_main_dir=${installDir}
data_link_dir=${installDir}/data
log_link_dir=${installDir}/log
cfg_link_dir=${installDir}/cfg
local_bin_link_dir="/usr/local/bin"
service_config_dir="/etc/systemd/system"
config_dir="/etc/${PREFIX}"

validate_safe_path() {
  local path="$1"
  case "$path" in
    ""|"/"|"/etc"|"/bin"|"/lib"|"/usr"|"/sbin"|"/boot"|"/root"|"/home"|"/var"|"/tmp"|"/dev"|"/proc"|"/sys"|"/run")
      echo -e "${RED}Refusing to operate on dangerous system path: $path${NC}"
      exit 1
      ;;
  esac
}
validate_safe_path "$installDir"

# User mode detection: non-root uses per-user directories and systemd user bus.
if [ "$(id -u)" -ne 0 ]; then
  user_mode=1
  bin_link_dir="$HOME/.local/bin"
  lib_link_dir="$HOME/.local/lib"
  lib64_link_dir="$HOME/.local/lib64"
  inc_link_dir="$HOME/.local/include"
  config_dir="${installDir}/cfg"
  service_config_dir="$HOME/.config/systemd/user"
  sysctl_cmd="systemctl --user"
else
  user_mode=0
  sysctl_cmd="systemctl"
fi

if [ "$user_mode" -eq 1 ]; then
  data_dir="${installDir}/data"
  log_dir="${installDir}/log"
else
  data_dir="/var/lib/${PREFIX}"
  log_dir="/var/log/${PREFIX}"
fi

if [ "${verMode}" == "cluster" ]; then
  if [ "${entMode}" == "full" ]; then
    services=(${PREFIX}"d" ${PREFIX}"adapter" ${PREFIX}"keeper" ${PREFIX}"x" ${PREFIX}"-explorer")
  else
    services=(${PREFIX}"d" ${PREFIX}"adapter" ${PREFIX}"keeper" ${PREFIX}"-explorer")
  fi
  tools=(${PREFIX} ${PREFIX}"Benchmark" ${PREFIX}"dump" ${PREFIX}"demo" ${PREFIX}"inspect" ${PREFIX}"udf" set_core.sh TDinsight.sh $uninstallScript start-all.sh stop-all.sh uninstall_taosx.sh)
else
  tools=(${PREFIX} ${PREFIX}"Benchmark" ${PREFIX}"dump" ${PREFIX}"demo" ${PREFIX}"udf" set_core.sh TDinsight.sh $uninstallScript start-all.sh stop-all.sh uninstall_taosx.sh)

  services=(${PREFIX}"d" ${PREFIX}"adapter" ${PREFIX}"keeper" ${PREFIX}"-explorer")
fi

if [ -x "${install_main_dir}/bin/${xName}" ] || [ -f "${service_config_dir}/${xName}.service" ]; then
  case " ${services[*]} " in
  *" ${xName} "*) ;;
  *)
    services=(${services[@]} ${xName})
    ;;
  esac
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
  local current_user
  local pids
  local config_dir_escaped

  if [ "$user_mode" -eq 1 ]; then
    current_user=$(id -un)
    pids=$(ps -eo user=,pid=,comm= | awk -v svc="$_service" -v current_user="$current_user" '$1 == current_user && $3 == svc {print $2}')
  else
    config_dir_escaped=${config_dir%/}
    local allow_default_root_config=0
    if [ "$config_dir_escaped" = "/etc/${PREFIX}" ]; then
      allow_default_root_config=1
    fi
    pids=$(ps -eo pid=,args= | awk -v svc="$_service" -v config_dir="$config_dir_escaped" -v allow_default_root_config="$allow_default_root_config" '
      function service_matches(cmd, svc, parts, cmd_name, field_count) {
        field_count = split(cmd, parts, /[[:space:]]+/)
        cmd_name = parts[1]
        sub("^.*/", "", cmd_name)
        return cmd_name == svc
      }

      function config_match(cmd, config_dir, parts, i, cfg_arg, field_count) {
        field_count = split(cmd, parts, /[[:space:]]+/)
        for (i = 1; i < field_count; i++) {
          if (parts[i] == "-c") {
            cfg_arg = parts[i + 1]
            if (cfg_arg == config_dir || index(cfg_arg, config_dir "/") == 1) {
              return 1
            }
            return 0
          }
        }
        return -1
      }

      {
        pid = $1
        $1 = ""
        sub(/^[[:space:]]+/, "", $0)
        cmd = $0
        if (!service_matches(cmd, svc)) {
          next
        }
        cfg_match = config_match(cmd, config_dir)
        if (cfg_match == 1 || (cfg_match == -1 && allow_default_root_config == 1)) {
          print pid
        }
      }')
  fi

  while IFS= read -r pid; do
    [ -n "$pid" ] && kill -9 "$pid" || :
  done <<< "$pids"
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

function clean_env_file() {
  if [ "$user_mode" -ne 1 ]; then
    return 0
  fi

  local env_file=""
  local login_shell="${SHELL##*/}"
  if [ "$login_shell" = "zsh" ]; then
    env_file="${HOME}/.zshrc"
  elif [ "$login_shell" = "bash" ] || [ -z "$login_shell" ]; then
    env_file="${HOME}/.bashrc"
  else
    env_file="${HOME}/.profile"
  fi

  if [ ! -f "$env_file" ]; then
    return 0
  fi

  local tmp_file="${env_file}.tmp.$$"
  sed -e "/^# ${productName} install path$/d" \
      -e "\|^export PATH=\"${bin_link_dir}:.*\"|d" \
      -e "\|^export LD_LIBRARY_PATH=\"${lib_link_dir}:.*\"|d" \
      "$env_file" > "$tmp_file" && mv "$tmp_file" "$env_file" || rm -f "$tmp_file"
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
    rm -rf "${bin_link_dir}/${_service}" || :
    rm -rf "${installDir}/bin/${_service}" || :
    [ "$user_mode" -eq 0 ] && rm -rf "${local_bin_link_dir}/${_service}" || :
    echo "${_service} is removed successfully!"
  fi
}

remove_tools_of() {
  _tool=$1
  kill_service_of ${_tool}
  [ -L "${bin_link_dir}/${_tool}" ] && rm -rf "${bin_link_dir}/${_tool}" || :
  [ -e "${installDir}/bin/${_tool}" ] && rm -rf "${installDir}/bin/${_tool}" || :
  [ -L "${local_bin_link_dir}/${_tool}" ] && rm -rf "${local_bin_link_dir}/${_tool}" || :
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
  for dir in "${lib_link_dir}" "${lib64_link_dir}"; do
    if [ -d "$dir" ]; then
      for pattern in "libtaos.*" "libtaosnative.*" "libtaosws.*"; do
        find "$dir" -name "$pattern" -exec rm -f {} \; || :
      done
    fi
  done
  #rm -rf ${v15_java_app_dir}           || :
}

function clean_header() {
  # Remove link
  rm -f ${inc_link_dir}/taos.h || :
  rm -f ${inc_link_dir}/taosdef.h || :
  rm -f ${inc_link_dir}/taoserror.h || :
  rm -f ${inc_link_dir}/tdef.h || :
  rm -f ${inc_link_dir}/taosudf.h || :

  [ -f ${inc_link_dir}/taosws.h ] && rm -f ${inc_link_dir}/taosws.h || :
}

function clean_config() {
  # Remove link
  if [ "${cfg_link_dir}" = "${config_dir}" ]; then
    return 0
  fi
  rm -f ${cfg_link_dir}/* || :
}

function clean_log() {
  # Remove link
  if [ "${log_link_dir}" = "${log_dir}" ]; then
    return 0
  fi
  rm -rf "${log_link_dir}" || :
}

function clean_service_on_launchctl() {
  launchctl unload -w /Library/LaunchDaemons/com.taosdata.taosd.plist || :
  launchctl unload -w /Library/LaunchDaemons/com.taosdata.${PREFIX}adapter.plist || :
  launchctl unload -w /Library/LaunchDaemons/com.taosdata.${PREFIX}keeper.plist || :
  launchctl unload -w /Library/LaunchDaemons/com.taosdata.${PREFIX}-explorer.plist || :

  launchctl remove com.tdengine.taosd || :
  launchctl remove com.tdengine.${PREFIX}adapter || :
  launchctl remove com.tdengine.${PREFIX}keeper || :
  launchctl remove com.tdengine.${PREFIX}-explorer || :

  rm /Library/LaunchDaemons/com.taosdata.* >/dev/null 2>&1 || :
}

function batch_remove_paths_and_clean_dir() {
  local dir="$1"
  shift
  local paths=("$@")
  for path in "${paths[@]}"; do
    rm -rf "$path" || :
  done
  find "$dir" -type d -empty -delete || :
  if [ -z "$(ls -A "$dir" 2>/dev/null)" ]; then
    rm -rf "$dir" || :
  fi
}

function remove_data_and_config() {
  local cfg_file="${config_dir}/${PREFIX}.cfg"
  data_dir=$(grep dataDir "${cfg_file}" 2>/dev/null | grep -v '#' | tail -n 1 | awk {'print $2'})
  if [ -z "$data_dir" ]; then
    if [ "$user_mode" -eq 1 ]; then
      data_dir="${installDir}/data"
    else
      data_dir="/var/lib/${PREFIX}"
    fi
  fi

  log_dir=$(grep logDir "${cfg_file}" 2>/dev/null | grep -v '#' | tail -n 1 | awk {'print $2'})
  if [ -z "$log_dir" ]; then
    if [ "$user_mode" -eq 1 ]; then
      log_dir="${installDir}/log"
    else
      log_dir="/var/log/${PREFIX}"
    fi
  fi
  
  
  if [ -d "${config_dir}" ]; then
    rm -rf "${config_dir}"
  fi

  if [ -d "${data_dir}" ]; then
    data_remove_list=(
      "${data_dir}/dnode"
      "${data_dir}/mnode"
      "${data_dir}/vnode"
      "${data_dir}/snode"
      "${data_dir}/xnode"
      "${data_dir}/.udf"
      "${data_dir}/.running"*
      "${data_dir}/.taosudf"*
      "${data_dir}/${PREFIX}x"*
      "${data_dir}/explorer"*
      "${data_dir}/backup"
    )
    batch_remove_paths_and_clean_dir "${data_dir}" "${data_remove_list[@]}"
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
  fi
}

function remove_install_main_dir() {
  rm -f "${install_main_dir}/uninstall_${PREFIX}x.sh" "${install_main_dir}/uninstall.sh" "${install_main_dir}/.install_path" || :

  if [ "$user_mode" -eq 1 ] && [ X$remove_flag != X"true" ]; then
    find "${install_main_dir}" -mindepth 1 -maxdepth 1 ! \( -name cfg -o -name log -o -name data \) -exec rm -rf {} + || :
    return 0
  fi

  rm -rf "${install_main_dir}" || :
}

function usage() {
  echo -e "\nUsage: $(basename $0) [-e <yes|no>]"
  echo "-e: silent mode, specify whether to remove all the data, log and configuration files."
  echo "  yes: remove the data, log, and configuration files."
  echo "  no:  don't remove the data, log, and configuration files."
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
# Remove data link directory
if [ "${data_link_dir}" != "${data_dir}" ]; then
  rm -rf "${data_link_dir}" || :
fi

if [ X$remove_flag == X"true" ]; then
  remove_data_and_config
fi

remove_install_main_dir
if [[ -e /etc/os-release ]]; then
  osinfo=$(awk -F= '/^NAME/{print $2}' /etc/os-release)
else
  osinfo=""
fi

if [ "$user_mode" -eq 0 ]; then
  if echo $osinfo | grep -qwi "ubuntu"; then
    dpkg --force-all -P tdengine >/dev/null 2>&1 || :
  elif echo $osinfo | grep -qwi "debian"; then
    dpkg --force-all -P tdengine >/dev/null 2>&1 || :
  elif echo $osinfo | grep -qwi "centos"; then
    rpm -e --noscripts tdengine >/dev/null 2>&1 || :
  fi
fi

clean_env_file
command -v systemctl >/dev/null 2>&1 && ${sysctl_cmd} daemon-reload >/dev/null 2>&1 || true
echo
echo "${productName} is removed successfully!"
echo
