#!/bin/bash

# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

set -e
#set -x

osType=`uname`

RED='\033[0;31m'
GREEN='\033[1;32m'
NC='\033[0m'

PRODUCTPREFIX="taosanode"
installDir="/usr/local/taos/taosanode"
dataDir="/var/lib/taos/taosanode"
venv_list=(
  "${dataDir}/venv"
  "${dataDir}/timesfm_venv"
  "${dataDir}/moirai_venv"
  "${dataDir}/chronos_venv"
  "${dataDir}/momentfm_venv"
)
serverName="${PRODUCTPREFIX}d"
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

global_log_dir="/var/log/taos/taosanode"
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

run_python_service_command() {
  local command=$1
  shift || true
  local python_cmd=""
  local service_script="${installDir}/bin/taosanode_service.py"

  if [ -x "${dataDir}/venv/bin/python3" ]; then
    python_cmd="${dataDir}/venv/bin/python3"
  elif [ -x "${installDir}/venvs/venv/bin/python3" ]; then
    python_cmd="${installDir}/venvs/venv/bin/python3"
  fi

  if [ -n "${python_cmd}" ] && [ -f "${service_script}" ]; then
    if [ -n "${csudo}" ]; then
      ${csudo}${python_cmd} "${service_script}" "${command}" "$@" || :
    else
      "${python_cmd}" "${service_script}" "${command}" "$@" || :
    fi
  fi
}

force_kill_leftovers() {
  local patterns=(
    "${installDir}/cfg/taosanode.config.py"
    "${installDir}/lib/taosanalytics/tsfmservice/"
    "${dataDir}/venv/bin/python3 -m gunicorn"
    "${dataDir}/moirai_venv/bin/python3"
    "${dataDir}/momentfm_venv/bin/python3"
    "${dataDir}/chronos_venv/bin/python3"
    "${dataDir}/timesfm_venv/bin/python3"
  )

  for pattern in "${patterns[@]}"; do
    pgrep -f "${pattern}" 2>/dev/null | while read -r pid; do
      [ -n "${pid}" ] || continue
      [ "${pid}" -eq "$$" ] && continue
      [ "${pid}" -eq "${PPID:-0}" ] && continue
      ${csudo}kill -TERM "${pid}" >/dev/null 2>&1 || :
    done
  done

  sleep 2

  for pattern in "${patterns[@]}"; do
    pgrep -f "${pattern}" 2>/dev/null | while read -r pid; do
      [ -n "${pid}" ] || continue
      [ "${pid}" -eq "$$" ] && continue
      [ "${pid}" -eq "${PPID:-0}" ] && continue
      ${csudo}kill -KILL "${pid}" >/dev/null 2>&1 || :
    done
  done
}

kill_model_service() {
  run_python_service_command model-stop all

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
  run_python_service_command stop
  for _service in "${services[@]}"; do
    remove_service_of "${_service}"
  done
  force_kill_leftovers
}

function clean_venv() {
  # Prompt the user to manually delete Python virtual environments if desired
  echo "Please manually delete the following Python virtual environment directories if you no longer need them:"
  for venv_dir in "${venv_list[@]}"; do
    if [ -d "$venv_dir" ]; then
      echo "  $venv_dir "
      # echo "Removing venv: $venv_dir"
      # ${csudo}rm -rf "$venv_dir" || :
    fi
  done
}

function clean_module() {
  if [ ! -z "${local_model_dir}" ]; then
    ${csudo}unlink ${local_model_dir} || :
  fi
}

function clean_config() {
  # Remove global config file (taosanode.ini) but preserve local cfg directory
  if [ ! -z "${global_conf_dir}" ]; then
     ${csudo}rm -f ${global_conf_dir}/taosanode.ini || :
  fi

  # Preserve local cfg directory (user configuration)
  if [ ! -z "${local_conf_dir}" ]; then
     echo "Preserving configuration directory: ${local_conf_dir}"
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
  # install.sh creates this launcher as a regular file, not a symlink.
  ${csudo}rm -f "${bin_link_dir}/taosanodectl"

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
