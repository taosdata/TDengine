#!/bin/bash
# uninstall the deployed app info, not remove the python virtual environment

set -e
#set -x

osType=`uname`

RED='\033[0;31m'
GREEN='\033[1;32m'
NC='\033[0m'

MAIN_NAME="taosanode"
installDir="/usr/local/taos/taosanode"
venv_dir="/usr/local/taos/taosanode/venv"
serverName="${MAIN_NAME}d"
uninstallName="rmtaosanode"
productName="TDengine TDgpt"

if [ "$osType" != "Darwin" ]; then
  bin_link_dir="/usr/bin"
else
  bin_link_dir="/usr/local/bin"
fi

#install main path
bin_dir=${installDir}/bin
lib_dir=${installDir}/lib
local_log_dir=${installDir}/log
local_conf_dir=${installDir}/cfg
local_model_dir=${installDir}/model

global_log_dir="/var/log/taos/${MAIN_NAME}"
global_conf_dir="/etc/taos/"

service_config_dir="/etc/systemd/system"

services=(${serverName} ${uninstallName})

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
  pid=$(ps -ef | grep $_service | grep -v grep | awk '{print $2}')
  if [ -n "$pid" ]; then
    "${csudo}"${installDir}/bin/stop.sh:
  fi
}

kill_model_service() {
  for script in stop-tdtsfm.sh stop-time-moe.sh; do
    script_path="${installDir}/bin/${script}"
    [ -f "${script_path}" ] && sudo bash "${script_path}" || :
  done
}

clean_service_on_systemd_of() {
  _service=$1
  _service_config="${service_config_dir}/${_service}.service"
  if systemctl is-active --quiet ${_service}; then
    echo "${_service} is running, stopping it..."
    "${csudo}"systemctl stop ${_service} &>/dev/null || echo &>/dev/null
  fi

  "${csudo}"systemctl disable ${_service} &>/dev/null || echo &>/dev/null

  if [[ ! -z "${_service_config}"  && -f "${_service_config}" ]]; then
    ${csudo}rm ${_service_config}
  fi
}

clean_service_on_sysvinit_of() {
  _service=$1
  if pidof ${_service} &>/dev/null; then
    echo "${_service} is running, stopping it..."
    "${csudo}"service ${_service} stop || :
  fi
  if ((${initd_mod} == 1)); then
    if [ -e ${service_config_dir}/${_service} ]; then
      # shellcheck disable=SC2086
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

  # shellcheck disable=SC2236
  if [ ! -z "${service_config_dir}" ]; then
    echo -e "rm ${service_config_dir}/${_service}"
  fi

  #${csudo}rm ${service_config_dir}/${_service} || :

  if $(which init &>/dev/null); then
    ${csudo}init q || :
  fi
}

clean_service_of() {
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
  if [[ -e "${bin_link_dir}/${_service}" ]]; then
    ${csudo}rm "${bin_link_dir}"/${_service}
    echo "${_service} is removed successfully!"
  fi
}

remove_model_service() {
  declare -A links=(
    ["start-tdtsfm"]="start-tdtsfm.sh"
    ["stop-tdtsfm"]="stop-tdtsfm.sh"
    ["start-time-moe"]="start-time-moe.sh"
    ["stop-time-moe"]="stop-time-moe.sh"
  )

  # Iterate over the array and create/remove links as needed
  for link in "${!links[@]}"; do
    target="${links[$link]}"
    [ -L "${bin_link_dir}/${link}" ] && ${csudo}rm -rf "${bin_link_dir}/${link}" || :
  done
  
}

remove_service() {
  for _service in "${services[@]}"; do
    remove_service_of "${_service}"
  done
}

function clean_venv() {
  # Remove python virtual environment
  #${csudo}rm ${venv_dir}/* || :
  if [ ! -z "${venv_dir}" ]; then
    echo -e "${csudo}rm -rf ${venv_dir}/*"
  fi  
}

function clean_module() {
  if [ ! -z "${local_model_dir}" ]; then
    ${csudo}unlink ${local_model_dir} || :
  fi
}

function clean_config() {
  # Remove config file
  if [ ! -z "${global_conf_dir}" ]; then
     ${csudo}rm -f ${global_conf_dir}/taosanode.ini || :
  fi

  if [ ! -z "${local_conf_dir}" ]; then
     # shellcheck disable=SC2086
     ${csudo}rm -rf ${local_conf_dir} || :
  fi
}

function clean_log() {
  # Remove log files
  if [ ! -z "${global_log_dir}" ]; then
     ${csudo}rm -rf ${global_log_dir} || :
  fi
  
  if [ ! -z "${local_log_dir}" ]; then
    ${csudo}rm -rf ${local_log_dir} || :
  fi
}

function remove_deploy_binary() {
  if [ ! -z "${bin_dir}" ]; then
    ${csudo}rm -rf ${bin_dir} || :
  fi

  if [ ! -z "${lib_dir}" ]; then
     ${csudo}rm -rf ${lib_dir}
  fi
}

kill_model_service
remove_service
remove_model_service
clean_log  # Remove link log directory
clean_config  # Remove link configuration file
remove_deploy_binary
clean_venv

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
echo "${productName} is uninstalled successfully!"
echo
