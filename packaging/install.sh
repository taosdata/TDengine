#!/bin/bash
set -e

PREFIX="taos"
xName="${PREFIX}x"
INSTALL_DIR="/usr/bin"
PLUGINS_ROOT_DIR="/usr/local/${xName}"
CONFIG_DIR="/etc/${PREFIX}"
SERVICE_CONFIG_DIR="/etc/systemd"
agentname="${PREFIX}x-agent"
explorerName="${PREFIX}-explorer"
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
  SERVICE_CONFIG_DIR="/etc/systemd"
else
  echo " osinfo: ${osinfo}"
  echo " This is an officially unverified linux system,"
  echo " if there are any problems with the installation and operation, "
  os_type=1
fi

check_and_create_directory() {
  if [ ! -d "$1" ]; then
    mkdir -p "$1"
  fi
}

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

stop_explore_service(){
    explore_service_config="${SERVICE_CONFIG_DIR}/${explorerName}.service"
    if [ -e "$explore_service_config" ]; then
      if systemctl is-active --quiet ${explorerName}; then
        echo "${explorerName} is running, stopping it..."
        ${csudo}systemctl stop ${explorerName} &>/dev/null || echo &>/dev/null
      fi
      ${csudo}systemctl disable ${explorerName} &>/dev/null || echo &>/dev/null
      ${csudo}rm -f ${explore_service_config}
    fi
}


# remove old taosx and taosx-agent
remove_taosx() {
    if [ -f ./bin/${xName} ]; then
        stop_taosx_service
        ${csudo}rm -rf ${INSTALL_DIR}/bin/${xName}
    fi
    if [ -f ./bin/${explorerName} ]; then
        stop_explore_service
        ${csudo}rm -rf ${INSTALL_DIR}/bin/${explorerName}
    fi
    if [ -f ./bin/${agentname} ]; then
        stop_taosx_agent_service
        ${csudo}rm -rf ${INSTALL_DIR}/bin/${agentname}
    fi
    ${csudo}rm -rf ${INSTALL_DIR}/plugins
    ${csudo}rm -rf ${INSTALL_DIR}/uninstall.sh
}

# install new taosx and taosx-agent
install_taosx() {
    echo "install taosx..."
    ${csudo}cp -r bin/* ${INSTALL_DIR}
    check_and_create_directory "${PLUGINS_ROOT_DIR}/plugins"
    echo "install plugins to ${PLUGINS_ROOT_DIR}/plugins..."
    ${csudo}cp -r plugins/* ${PLUGINS_ROOT_DIR}/plugins
    ${csudo}cp uninstall.sh ${PLUGINS_ROOT_DIR}
    echo "install service file to ${SERVICE_CONFIG_DIR}..."
    ${csudo}cp -r etc/systemd/* ${SERVICE_CONFIG_DIR}

    check_and_create_directory "${CONFIG_DIR}"
    # copy config to /etc/taos
    if [ -f ${CONFIG_DIR}/agent.toml ]; then
        ${csudo}cp ./etc/taos/agent.toml ${CONFIG_DIR}/agent.toml.new
    else
       ${csudo}cp ./etc/taos/agent.toml ${CONFIG_DIR}/
    fi

    if [ -f ${CONFIG_DIR}/explorer.toml ]; then
        ${csudo}cp ./etc/taos/explorer.toml ${CONFIG_DIR}/explorer.toml.new
    else
       ${csudo}cp ./etc/taos/explorer.toml ${CONFIG_DIR}/
    fi
}

# install taosx service
install_taosx_service(){
  if ((${service_mod} == 0)); then
    [ -f ./etc/systemd/${xName}.service ] &&
      ${csudo}cp ./etc/systemd/${xName}.service  ${SERVICE_CONFIG_DIR}/ || :
    [ -f ./etc/systemd/${agentname}.service ] &&
      ${csudo}cp ./etc/systemd/${agentname}.service ${SERVICE_CONFIG_DIR}/ || :
    [ -f ./etc/systemd/${explorerName}.service ] &&
      ${csudo}cp ./etc/systemd/${explorerName}.service ${SERVICE_CONFIG_DIR}/ || :
    ${csudo}systemctl daemon-reload
  fi
}


# main entry point
if [ -x ${INSTALL_DIR}/bin/${xName} ]; then
    echo "${xName} is already installed, do you want to reinstall it? [y/n]"
    read answer
    if [ $answer == "y" ]; then
        remove_taosx
        install_taosx
        install_taosx_service
    else
        echo "${xName} installation is cancelled"
        exit 1
    fi
else
    ${csudo}mkdir -p ${INSTALL_DIR}
    install_taosx
    install_taosx_service
fi
