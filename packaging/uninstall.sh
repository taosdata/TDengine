#!/bin/bash
set -e

PREFIX="taos"
xName="${PREFIX}x"
INSTALL_DIR="/usr/bin"
TAOSX_ROOT_DIR="/usr/local/${PREFIX}"
CONFIG_DIR="/etc/${PREFIX}"
SERVICE_CONFIG_DIR="/etc/systemd/system"
agentname="${PREFIX}x-agent"
explorerName="${PREFIX}-explorer"
EXPLORER_CONFIG_NAME="explorer"
AGENT_CONFIG_NAME="agent"
LOG_DIR="/var/log/${PREFIX}"
DATA_DIR="/var/lib/${PREFIX}"
AGENT_DATA_DIR_NAME="${xName}agent"
csudo=""
COMMAND_ARGS=$@

TAOSX_LOG_NAME="${PREFIX}x_*.log*"
TAOSX_AGENT_LOG_NAME="${PREFIX}x_agent_*.log*"
TAOS_EXPLORER_LOG_NAME="${PREFIX}explorer_*.log*"

target=${agentname}
verNumber=3.0

if command -v ${xName} >/dev/null; then
  target=${xName}
fi

if command -v sudo >/dev/null; then
  csudo="sudo "
fi

initd_mod=0
service_mod=2
if ps aux | grep -v grep | grep systemd &>/dev/null; then
  service_mod=0
elif $(which service &>/dev/null); then
  service_mod=1
  SERVICE_CONFIG_DIR="/etc/init.d"
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

# get the operating system type for using the corresponding init file
# ubuntu/debian(deb), centos/fedora(rpm), others: opensuse, redhat, ..., no verification
#osinfo=$(awk -F= '/^NAME/{print $2}' /etc/os-release)
if [[ -e /etc/os-release ]]; then
  osinfo=$(cat /etc/os-release | grep "NAME" | cut -d '"' -f2) || :
else
  osinfo=""
fi
#echo "osinfo: ${osinfo}"
os_type=0
if echo $osinfo | grep -qwi "ubuntu"; then
  #  echo "This is ubuntu system"
  os_type=1
elif echo $osinfo | grep -qwi "debian"; then
  #  echo "This is debian system"
  os_type=1
elif echo $osinfo | grep -qwi "Kylin"; then
  #  echo "This is Kylin system"
  os_type=1
elif echo $osinfo | grep -qwi "centos"; then
  #  echo "This is centos system"
  os_type=2
elif echo $osinfo | grep -qwi "fedora"; then
  #  echo "This is fedora system"
  os_type=2
elif echo $osinfo | grep -qwi "Linx"; then
  #  echo "This is Linx system"
  os_type=1
  service_mod=0
  initd_mod=0
  SERVICE_CONFIG_DIR="/etc/systemd/system"
elif echo `uname` | grep -qwi "Darwin"; then
  os_type=3
  if [ -d "/usr/local/Cellar/" ]; then
    TAOSX_ROOT_DIR="/usr/local/Cellar/tdengine/${verNumber}"
  elif [ -d "/opt/homebrew/Cellar/" ]; then
    TAOSX_ROOT_DIR="/opt/homebrew/Cellar/tdengine/${verNumber}"
  else
    TAOSX_ROOT_DIR="/usr/local/taos"
  fi
  INSTALL_DIR="/usr/local/bin"
  SERVICE_CONFIG_DIR="/Library/LaunchDaemons"
else
  echo " osinfo: ${osinfo}"
  echo " This is an officially unverified linux system,"
  echo " if there are any problems with the installation and operation, "
  os_type=1
fi

stop_taosx_service(){
    x_service_config="${SERVICE_CONFIG_DIR}/${xName}.service"
    if [ -e "$x_service_config" ]; then
      if systemctl is-active --quiet ${xName}; then
        echo "${xName} is running, stopping it..."
        ${csudo}systemctl stop ${xName} &>/dev/null || echo &>/dev/null
      fi
      ${csudo}systemctl disable ${xName} &>/dev/null || echo &>/dev/null
      ${csudo}rm -f ${x_service_config}
    fi

}

stop_taosx_agent_service(){
    agent_service_config="${SERVICE_CONFIG_DIR}/${agentname}.service"
    if [ -e "$agent_service_config" ]; then
      if systemctl is-active --quiet ${agentname}; then
        echo "${agentname} is running, stopping it..."
        ${csudo}systemctl stop ${agentname} &>/dev/null || echo &>/dev/null
      fi
      ${csudo}systemctl disable ${agentname} &>/dev/null || echo &>/dev/null
      ${csudo}rm -f ${agent_service_config}
    fi
}

stop_explorer_service(){
  if echo `uname` | grep -qwi "Darwin"; then
    service=com.tdengine.taos-explorer
    explore_service_config=${SERVICE_CONFIG_DIR}/${service}.plist
    if [ -e "$explore_service_config" ]; then
      ${csudo}launchctl stop ${service}
      ${csudo}launchctl unload -w ${explore_service_config} || :
      ${csudo}launchctl remove ${service} || :
      ${csudo}rm ${explore_service_config} >/dev/null 2>&1 || :
    fi
  else
    explore_service_config="${SERVICE_CONFIG_DIR}/${explorerName}.service"
    if [ -e "$explore_service_config" ]; then
      if systemctl is-active --quiet ${explorerName}; then
        echo "${explorerName} is running, stopping it..."
        ${csudo}systemctl stop ${explorerName} &>/dev/null || echo &>/dev/null
      fi
      ${csudo}systemctl disable ${explorerName} &>/dev/null || echo &>/dev/null
      ${csudo}rm -f ${explore_service_config}
    fi
  fi
}

# remove old taosx and taosx-agent
remove_taosx() {
    stop_taosx_agent_service
    stop_taosx_service
    stop_explorer_service

    hasAgent=0
    if [ -f ${INSTALL_DIR}/${agentname} ]; then
        hasAgent=1
    fi

    ${csudo}rm -rf ${INSTALL_DIR}/${xName}
    ${csudo}rm -rf ${INSTALL_DIR}/${explorerName}
    ${csudo}rm -rf ${INSTALL_DIR}/${agentname}
    ${csudo}rm -rf ${TAOSX_ROOT_DIR}/bin/${xName}
    ${csudo}rm -rf ${TAOSX_ROOT_DIR}/bin/${explorerName}
    ${csudo}rm -rf ${TAOSX_ROOT_DIR}/bin/${agentname}
    ${csudo}rm -rf ${TAOSX_ROOT_DIR}/plugins
    ${csudo}rm -rf ${TAOSX_ROOT_DIR}/uninstall.sh

    if ! need_remove_data $COMMAND_ARGS; then 
        echo "${xName} is removed successfully!"
        echo "${explorerName} is removed successfully!"
        if [ $hasAgent -eq 1 ]; then
            echo "${agentname} is removed successfully!"
        fi
        return
    fi

    remove_custom_data_dir ${CONFIG_DIR}/${xName}.toml
    remove_custom_log_dir ${CONFIG_DIR}/${xName}.toml
    ${csudo}rm -rf ${DATA_DIR}/${xName}
    ${csudo}rm -rf ${LOG_DIR}/${xName}.log*
    ${csudo}rm -rf ${LOG_DIR}/${TAOSX_LOG_NAME}
    ${csudo}rm -rf ${CONFIG_DIR}/${xName}.toml*
    echo "${xName} is removed successfully!"

    remove_custom_data_dir ${CONFIG_DIR}/${EXPLORER_CONFIG_NAME}.toml
    remove_custom_log_dir ${CONFIG_DIR}/${EXPLORER_CONFIG_NAME}.toml
    ${csudo}rm -rf ${DATA_DIR}/${EXPLORER_CONFIG_NAME}
    ${csudo}rm -rf ${LOG_DIR}/${TAOS_EXPLORER_LOG_NAME}
    ${csudo}rm -rf ${CONFIG_DIR}/${EXPLORER_CONFIG_NAME}.toml*
    echo "${explorerName} is removed successfully!"

    remove_custom_data_dir ${CONFIG_DIR}/${AGENT_CONFIG_NAME}.toml
    remove_custom_log_dir ${CONFIG_DIR}/${AGENT_CONFIG_NAME}.toml
    ${csudo}rm -rf ${DATA_DIR}/${AGENT_DATA_DIR_NAME}
    ${csudo}rm -rf ${LOG_DIR}/${AGENT_CONFIG_NAME}.log*
    ${csudo}rm -rf ${LOG_DIR}/${TAOSX_AGENT_LOG_NAME}
    ${csudo}rm -rf ${CONFIG_DIR}/${AGENT_CONFIG_NAME}.toml*
    if [ $hasAgent -eq 1 ]; then
        echo "${agentname} is removed successfully!"
    fi

    remove_plugin_logs
}


# remove old taosx and taosx-agent
remove_explorer() {
    stop_explorer_service

    ${csudo}rm -rf ${INSTALL_DIR}/${explorerName}
    ${csudo}rm -rf ${TAOSX_ROOT_DIR}/bin/${explorerName}
    ${csudo}rm -rf ${TAOSX_ROOT_DIR}/uninstall_explorer.sh

    if ! need_remove_data $COMMAND_ARGS; then
        echo "${xName} is removed successfully!"
        echo "${explorerName} is removed successfully!"
        if [ $hasAgent -eq 1 ]; then
            echo "${agentname} is removed successfully!"
        fi
        return
    fi

    remove_custom_data_dir ${CONFIG_DIR}/${EXPLORER_CONFIG_NAME}.toml
    remove_custom_log_dir ${CONFIG_DIR}/${EXPLORER_CONFIG_NAME}.toml
    ${csudo}rm -rf ${DATA_DIR}/${EXPLORER_CONFIG_NAME}
    ${csudo}rm -rf ${LOG_DIR}/${TAOS_EXPLORER_LOG_NAME}
    ${csudo}rm -rf ${CONFIG_DIR}/${EXPLORER_CONFIG_NAME}.toml*
    echo "${explorerName} is removed successfully!"
}

remove_custom_data_dir() {
    # find config file
    config_file=$1
    if [ ! -e "${config_file}" ]; then
        return
    fi
    # find data dir from config file
    custom_data_dir=$(grep '^\s*data_dir' ${config_file} | sed 's/.*=.*"\(.*\)"/\1/')
    # data dir is not empty and is an absolute path
    if [[ -n "$custom_data_dir" && "$custom_data_dir" == /* ]]; then
        ${csudo}rm -rf $custom_data_dir
    fi
}

remove_custom_log_dir() {
    # find config file
    config_file=$1
    if [ ! -e "${config_file}" ]; then
        return
    fi
    # find log path from config file
    custom_log_dir=$(sed -n '/\[log\]/,/\[.*\]/p' ${config_file} | grep -v '^#' | grep '^\s*path' | sed 's/.*=.*"\(.*\)"/\1/')
    # log path is not empty and is an absolute path
    if [[ -n "$custom_log_dir" && "$custom_log_dir" == /* ]]; then
        ${csudo}rm -rf $custom_log_dir
    fi
}

remove_plugin_logs() {
    ${csudo}rm -rf ${LOG_DIR}/influxdb-*.log*
    ${csudo}rm -rf ${LOG_DIR}/opc-*.log*
    ${csudo}rm -rf ${LOG_DIR}/opc.log*
    ${csudo}rm -rf ${LOG_DIR}/opentsdb-*.log*
    ${csudo}rm -rf ${LOG_DIR}/pi-*.log*
}

# remove taosx-agent
remove_taos_agent() {
    stop_taosx_agent_service

    hasAgent=0
    if [ -f ${INSTALL_DIR}/${agentname} ]; then
        hasAgent=1
    fi

    ${csudo}rm -rf ${INSTALL_DIR}/${agentname}
    ${csudo}rm -rf ${TAOSX_ROOT_DIR}/bin/${agentname}
    # remove plugins, but keep the udt plugin
    ${csudo}rm -rf ${TAOSX_ROOT_DIR}/plugins/influxdb
    ${csudo}rm -rf ${TAOSX_ROOT_DIR}/plugins/opc
    ${csudo}rm -rf ${TAOSX_ROOT_DIR}/plugins/opentsdb
    ${csudo}rm -rf ${TAOSX_ROOT_DIR}/uninstall.sh

    if ! need_remove_data $COMMAND_ARGS; then 
        if [ $hasAgent -eq 1 ]; then
            echo "${agentname} is removed successfully!"
        fi
        return
    fi

    remove_custom_data_dir ${CONFIG_DIR}/${AGENT_CONFIG_NAME}.toml
    remove_custom_log_dir ${CONFIG_DIR}/${AGENT_CONFIG_NAME}.toml
    ${csudo}rm -rf ${DATA_DIR}/${AGENT_DATA_DIR_NAME}
    ${csudo}rm -rf ${LOG_DIR}/${AGENT_CONFIG_NAME}.log*
    ${csudo}rm -rf ${LOG_DIR}/${TAOSX_AGENT_LOG_NAME}
    ${csudo}rm -rf ${CONFIG_DIR}/${AGENT_CONFIG_NAME}.toml
    if [ $hasAgent -eq 1 ]; then
        echo "${agentname} is removed successfully!"
    fi

    remove_plugin_logs
}

remove_target() {
    if [ "$target" = ${xName} ]; then
      remove_taosx
    elif  [ "$target" = ${explorerName} ]; then
      remove_explorer
    else
      remove_taos_agent
    fi
}

need_remove_data() {
    while [[ "$#" -gt 0 ]]; do
      case $1 in
        --clean-all)
          if [[ "$2" == "true" ]]; then
              return 0
          elif [[ "$2" == "false" ]]; then
              return 1
          else
              echo "Error: --clean-all requires a true or false value."
              exit 1
          fi
          ;;
        *)
          break
          ;;
      esac
    done

    echo 
    echo "Do you want to remove all the data, log and configuration files? [y/n]"
    read answer
    if [ X$answer == X"y" ] || [ X$answer == X"Y" ]; then
      confirmMsg="I confirm that I would like to delete all data, log and configuration files"
      echo "Please enter '${confirmMsg}' to continue"
      read answer
      if [ X"$answer" == X"${confirmMsg}" ]; then
        return 0
      else
        echo "answer doesn't match, skip this step"
        return 1
      fi
    fi

    return 1
}

remove_target
