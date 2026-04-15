#!/bin/bash
#
# Script to stop the client and uninstall database, but retain the config and log files.
set -e
# set -x

RED='\033[0;31m'
GREEN='\033[1;32m'
NC='\033[0m'
verMode=edge
osType=$(uname)

clientName="taos"
uninstallScript="rmtaos"
clientName2="taos"
productName2="TDengine"

benchmarkName2="${clientName2}Benchmark"
demoName2="${clientName2}demo"
dumpName2="${clientName2}dump"
inspect_name="${clientName2}inspect"
taosgen_name="${clientName2}gen"
uninstallScript2="rm${clientName2}"

_script_real="$(readlink -f "$0" 2>/dev/null || echo "$0")"
_script_dir="$(dirname "$_script_real")"
if [[ "$_script_dir" == */bin ]]; then
  _guessed_dir="${_script_dir%/bin}"
else
  _guessed_dir="$_script_dir"
fi

if [ -f "${_guessed_dir}/.install_path" ]; then
  installDir=$(cat "${_guessed_dir}/.install_path")
elif [ -f "/usr/local/${clientName2}/.install_path" ]; then
  installDir=$(cat "/usr/local/${clientName2}/.install_path")
elif [ -f "$HOME/${clientName2}/.install_path" ]; then
  installDir=$(cat "$HOME/${clientName2}/.install_path")
elif [ "$osType" != "Darwin" ]; then
  installDir="/usr/local/${clientName2}"
else
  installDir="/usr/local/${clientName2}"
fi

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

install_main_dir=${installDir}
log_link_dir=${installDir}/log
cfg_link_dir=${installDir}/cfg

if [ "$(id -u)" -ne 0 ]; then
  user_mode=1
  bin_link_dir="$HOME/.local/bin"
  lib_link_dir="$HOME/.local/lib"
  if [ "$osType" != "Darwin" ]; then
    lib64_link_dir="$HOME/.local/lib64"
  else
    lib64_link_dir="$HOME/.local/lib"
  fi
  inc_link_dir="$HOME/.local/include"
  log_dir="${installDir}/log"
  cfg_dir="${installDir}/cfg"
else
  user_mode=0
  if [ "$osType" != "Darwin" ]; then
    bin_link_dir="/usr/bin"
    lib_link_dir="/usr/lib"
    lib64_link_dir="/usr/lib64"
    inc_link_dir="/usr/include"
    log_dir="/var/log/${clientName2}"
    cfg_dir="/etc/${clientName2}"
  else
    bin_link_dir="/usr/local/bin"
    lib_link_dir="/usr/local/lib"
    lib64_link_dir="/usr/local/lib"
    inc_link_dir="/usr/local/include"
    log_dir="${installDir}/log"
    cfg_dir="${installDir}/cfg"
  fi
fi

remove_config_and_log="false"

function kill_client() {
  current_user=$(id -un)
  client_processes=("${clientName2}" "${benchmarkName2}" "${dumpName2}" "${demoName2}" "${inspect_name}" "${taosgen_name}")

  for process_name in "${client_processes[@]}"; do
    if [ "$user_mode" -eq 1 ]; then
      pids=$(ps -eo pid=,user=,comm= | awk -v current_user="$current_user" -v process_name="$process_name" '$2 == current_user && $3 == process_name {print $1}' || true)
    else
      pids=$(ps -eo pid=,user=,comm= | awk -v process_name="$process_name" '$3 == process_name {print $1}' || true)
    fi

    if [ -n "$pids" ]; then
      echo "$pids" | while read p; do kill -9 "$p" 2>/dev/null || :; done
    fi
  done
}

function clean_bin() {
  rm -f "${bin_link_dir}/${clientName2}" || :
  rm -f "${bin_link_dir}/${demoName2}" || :
  rm -f "${bin_link_dir}/${benchmarkName2}" || :
  rm -f "${bin_link_dir}/${dumpName2}" || :
  rm -f "${bin_link_dir}/${uninstallScript2}" || :
  rm -f "${bin_link_dir}/set_core" || :
  rm -f "${bin_link_dir}/${taosgen_name}" || :
  [ -L "${bin_link_dir}/${inspect_name}" ] && rm -f "${bin_link_dir}/${inspect_name}" || :

  if [ "$verMode" == "cluster" ] && [ "$clientName" != "$clientName2" ]; then
    rm -f "${bin_link_dir}/${clientName2}" || :
    rm -f "${bin_link_dir}/${demoName2}" || :
    rm -f "${bin_link_dir}/${benchmarkName2}" || :
    rm -f "${bin_link_dir}/${dumpName2}" || :
    rm -f "${bin_link_dir}/${uninstallScript2}" || :
    rm -f "${bin_link_dir}/${taosgen_name}" || :
    [ -L "${bin_link_dir}/${inspect_name}" ] && rm -f "${bin_link_dir}/${inspect_name}" || :
  fi
}

function clean_lib() {
  for dir in "${lib_link_dir}" "${lib64_link_dir}"; do
    if [ -d "$dir" ]; then
      for pattern in "libtaos.*" "libtaosnative.*" "libtaosws.*"; do
        find "$dir" -name "$pattern" -exec rm -f {} \; || :
      done
    fi
  done
}

function clean_header() {
  rm -f "${inc_link_dir}/taos.h" || :
  rm -f "${inc_link_dir}/taosdef.h" || :
  rm -f "${inc_link_dir}/taoserror.h" || :
  rm -f "${inc_link_dir}/tdef.h" || :
  rm -f "${inc_link_dir}/taosudf.h" || :
  rm -f "${inc_link_dir}/taosws.h" || :
}

function clean_config() {
  if [ "${cfg_link_dir}" = "${cfg_dir}" ]; then
    return 0
  fi
  rm -f "${cfg_link_dir}"/* || :
}

function clean_log() {
  if [ "${log_link_dir}" = "${log_dir}" ]; then
    return 0
  fi
  rm -rf "${log_link_dir}" || :
}

function clean_config_and_log_dir() {
  echo "Do you want to remove all the log and configuration files? [y/n]"
  read answer
  if [ X$answer == X"y" ] || [ X$answer == X"Y" ]; then
    confirmMsg="I confirm that I would like to delete all log and configuration files"
    echo "Please enter '${confirmMsg}' to continue"
    read answer
    if [ X"$answer" == X"${confirmMsg}" ]; then
      rm -rf "${cfg_dir}" || :
      rm -rf "${log_dir}" || :
      remove_config_and_log="true"
    else
      echo "answer doesn't match, skip this step"
    fi
  fi
}

function remove_install_main_dir() {
  if { [ "${cfg_link_dir}" = "${cfg_dir}" ] || [ "${log_link_dir}" = "${log_dir}" ]; } && [ "$remove_config_and_log" != "true" ]; then
    find "${install_main_dir}" -mindepth 1 -maxdepth 1 ! \( -name cfg -o -name log \) -exec rm -rf {} + || :
    return 0
  fi

  rm -rf "${install_main_dir}"
}

kill_client
clean_bin
clean_header
clean_lib
clean_log
clean_config
clean_config_and_log_dir

remove_install_main_dir

echo -e "${GREEN}${productName2} client is removed successfully!${NC}"
echo
