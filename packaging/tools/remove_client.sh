#!/bin/bash
#
# Script to stop the client and uninstall database, but retain the config and log files.
set -e
# set -x

RED='\033[0;31m'
GREEN='\033[1;32m'
NC='\033[0m'
verMode=edge
PREFIX="taos"

clientName="taos"
uninstallScript="rmtaos"

clientName2="taos"
productName2="TDengine"
productName="TDengine TSDB"

benchmarkName2="${clientName2}Benchmark"
demoName2="${clientName2}demo"
dumpName2="${clientName2}dump"
inspect_name="${clientName2}inspect"
taosgen_name="${clientName2}taosgen"
uninstallScript2="rm${clientName2}"

# User mode detection and path setup
if [ "$(id -u)" -ne 0 ]; then
  user_mode=1
  installDir="$HOME/${clientName2}"
  bin_link_dir="$HOME/.local/bin"
  lib_link_dir="$HOME/.local/lib"
  lib64_link_dir="$HOME/.local/lib64"
  inc_link_dir="$HOME/.local/include"
  log_dir="$HOME/${clientName2}/log"
  cfg_dir="$HOME/${clientName2}/cfg"
  csudo=""
else
  user_mode=0
  installDir="/usr/local/${clientName2}"
  bin_link_dir="/usr/bin"
  lib_link_dir="/usr/lib"
  lib64_link_dir="/usr/lib64"
  inc_link_dir="/usr/include"
  log_dir="/var/log/${clientName2}"
  cfg_dir="/etc/${clientName2}"
  csudo=""
  if command -v sudo >/dev/null; then
    csudo="sudo "
  fi
fi

# Install path discovery from .install_path
_script_real="$(readlink -f "$0" 2>/dev/null || echo "$0")"
_script_dir="$(dirname "$_script_real")"
_guessed_dir=""
if [[ "$_script_dir" == */bin ]]; then
  _guessed_dir="${_script_dir%/bin}"
elif [[ "$_script_dir" == */${clientName2} ]]; then
  _guessed_dir="$_script_dir"
fi

if [ -n "${_guessed_dir:-}" ] && [ -f "${_guessed_dir}/.install_path" ]; then
  installDir=$(cat "${_guessed_dir}/.install_path")
elif [ -f "/usr/local/${clientName2}/.install_path" ] && [ "$user_mode" -eq 0 ]; then
  installDir=$(cat "/usr/local/${clientName2}/.install_path")
elif [ -f "$HOME/${clientName2}/.install_path" ] && [ "$user_mode" -eq 1 ]; then
  installDir=$(cat "$HOME/${clientName2}/.install_path")
fi

install_main_dir=${installDir}
log_link_dir=${installDir}/log
cfg_link_dir=${installDir}/cfg

function kill_client() {
    pid=$(ps -C ${clientName2} | grep -w ${clientName2} | grep -v $uninstallScript2 | awk '{print $1}')
    if [ -n "$pid" ]; then
        ${csudo}kill -9 $pid || :
    fi
}

function clean_bin() {
    # Remove links
    ${csudo}rm -f ${bin_link_dir}/${clientName2} || :
    ${csudo}rm -f ${bin_link_dir}/${demoName2} || :
    ${csudo}rm -f ${bin_link_dir}/${benchmarkName2} || :
    ${csudo}rm -f ${bin_link_dir}/${dumpName2} || :
    ${csudo}rm -f ${bin_link_dir}/${uninstallScript2} || :
    ${csudo}rm -f ${bin_link_dir}/set_core || :
    [ -L ${bin_link_dir}/${inspect_name} ] && ${csudo}rm -f ${bin_link_dir}/${inspect_name} || :
    [ -L ${bin_link_dir}/${taosgen_name} ] && ${csudo}unlink ${bin_link_dir}/${taosgen_name} || :

    if [ "$verMode" == "cluster" ] && [ "$clientName" != "$clientName2" ]; then
        ${csudo}rm -f ${bin_link_dir}/${clientName2} || :
        ${csudo}rm -f ${bin_link_dir}/${demoName2} || :
        ${csudo}rm -f ${bin_link_dir}/${benchmarkName2} || :
        ${csudo}rm -f ${bin_link_dir}/${dumpName2} || :
        ${csudo}rm -f ${bin_link_dir}/${uninstallScript2} || :
        [ -L ${bin_link_dir}/${inspect_name} ] && ${csudo}rm -f ${bin_link_dir}/${inspect_name} || :
        [ -L ${bin_link_dir}/${taosgen_name} ] && ${csudo}unlink ${bin_link_dir}/${taosgen_name} || :
    fi
}

function clean_lib() {
    # Remove links in lib and lib64 directories
    for dir in "${lib_link_dir}" "${lib64_link_dir}"; do
        if [ -d "$dir" ]; then
            for pattern in "libtaos.*" "libtaosnative.*" "libtaosws.*"; do
                ${csudo}find "$dir" -name "$pattern" -exec ${csudo}rm -f {} \; || :
            done
        fi
    done
}

function clean_header() {
    # Remove link
    ${csudo}rm -f ${inc_link_dir}/taos.h || :
    ${csudo}rm -f ${inc_link_dir}/taosdef.h || :
    ${csudo}rm -f ${inc_link_dir}/taoserror.h || :
    ${csudo}rm -f ${inc_link_dir}/tdef.h || :
    ${csudo}rm -f ${inc_link_dir}/taosudf.h || :
    ${csudo}rm -f ${inc_link_dir}/taosws.h || :
}

function clean_config() {
    # Remove link
    ${csudo}rm -f ${cfg_link_dir}/* || :
}

function clean_log() {
    # Remove link
    ${csudo}rm -rf ${log_link_dir} || :
}

function clean_config_and_log_dir() {
    # Remove link
    echo "Do you want to remove all the log and configuration files? [y/n]"
    read answer
    if [ X$answer == X"y" ] || [ X$answer == X"Y" ]; then
        confirmMsg="I confirm that I would like to delete all log and configuration files"
        echo "Please enter '${confirmMsg}' to continue"
        read answer
        if [ X"$answer" == X"${confirmMsg}" ]; then
            # Remove dir
            rm -rf ${cfg_dir} || :
            rm -rf ${log_dir} || :
        else
            echo "answer doesn't match, skip this step"
        fi
    fi
}

# Stop client.
kill_client
# Remove binary file and links
clean_bin
# Remove header file.
clean_header
# Remove lib file
clean_lib
# Remove link log directory
clean_log
# Remove link configuration file
clean_config
# Remove dir
clean_config_and_log_dir

${csudo}rm -rf ${install_main_dir}

# Clean env variables from shell rc file for non-root uninstall
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
  local escaped_bin escaped_lib
  escaped_bin=$(printf '%s' "${bin_link_dir}" | sed 's/[.[\\/^$*]/\\&/g')
  escaped_lib=$(printf '%s' "${lib_link_dir}" | sed 's/[.[\\/^$*]/\\&/g')
  sed -e "/^# ${productName} install path$/d" \
      -e "\|^export PATH=\"${escaped_bin}:.*\"|d" \
      -e "\|^export LD_LIBRARY_PATH=\"${escaped_lib}:.*\"|d" \
      "$env_file" > "$tmp_file" && mv "$tmp_file" "$env_file" || rm -f "$tmp_file"
}
clean_env_file

echo -e "${GREEN}${productName2} client is removed successfully!${NC}"
echo
