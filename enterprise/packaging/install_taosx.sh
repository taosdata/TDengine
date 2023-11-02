#!/bin/bash
set -e

PREFIX="taos"
xName="${PREFIX}x"
INSTALL_DIR="/usr/local/${PREFIX}/bin"
TAOSX_ROOT_DIR="/usr/local/${PREFIX}"
CONFIG_DIR="/etc/${PREFIX}"
SERVICE_CONFIG_DIR="/etc/systemd/system"
BIN_LINK_DIR="/usr/bin"
agentname="${PREFIX}x-agent"
explorerName="${PREFIX}-explorer"
csudo=""
explorerEndpoint=""
services=(${xName} ${explorerName} ${agentname})

target="taosx"

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
else
  echo " osinfo: ${osinfo}"
  echo " This is an officially unverified linux system,"
  echo " if there are any problems with the installation and operation, "
  os_type=1
fi

check_and_create_directory() {
  if [ ! -d "$1" ]; then
    ${csudo} mkdir -p "$1"
  fi
}

stop_service(){
  service_config="${SERVICE_CONFIG_DIR}/$1.service"
  if [ -e "$service_config" ]; then
    if systemctl is-active --quiet $1; then
      echo "$1 is running, stopping it..."
      ${csudo}systemctl stop $1 &>/dev/null || echo &>/dev/null
    fi
    ${csudo}systemctl disable $1 &>/dev/null || echo &>/dev/null
    ${csudo}rm -f ${service_config}
  fi
}


# remove old taosx and taosx-agent
remove_taosx() { 
  for service in "${services[@]}"; do
    if [ -f ./bin/${service} ]; then
        stop_service ${service}
        ${csudo}rm -rf ${INSTALL_DIR}/${service}
    fi
  done

  ${csudo}rm -rf ${TAOSX_ROOT_DIR}/plugins
  ${csudo}rm -rf ${TAOSX_ROOT_DIR}/uninstall.sh
}

# remove taosx-agent
remove_taos_agent() {
  if [ -f ./bin/${agentname} ]; then
      stop_taosx_agent_service
      ${csudo}rm -rf ${INSTALL_DIR}/${agentname}
  fi
  ${csudo}rm -rf ${TAOSX_ROOT_DIR}/plugins
  ${csudo}rm -rf ${TAOSX_ROOT_DIR}/uninstall.sh
}

remove_target() {
  if [ "$target" = "taosx" ]; then
    remove_taosx
  else
    remove_taos_agent
  fi
}

print_tips(){
    if [ "$target" = "taosx" ]; then
      echo -e "\033[32mTo configure taosx-agent   \033[0m: edit /etc/taos/agent.toml"
      echo -e "\033[32mTo configure taos-explorer \033[0m: edit /etc/taos/explorer.toml"
      echo -e "\033[32mTo start taosx             \033[0m: sudo systemctl start taosx"
      echo -e "\033[32mTo start taosx-agent       \033[0m: sudo systemctl start taosx-agent"
      echo -e "\033[32mTo start taos-explorer     \033[0m: sudo systemctl start taos-explorer"

      echo -e "\n\033[32mtaosX and taosExplorer are installed successfully!\033[0m"
      echo -e "\033[32mTo access the TDengine management system: http://`hostname`:6060\033[0m"
      echo -e "\033[32mTo read the TDengine user manual: http://`hostname`:6060/docs-en\033[0m"
    else
      echo -e "\033[32mTo configure taosx-agent   \033[0m: edit /etc/taos/agent.toml"
      echo -e "\033[32mTo start taosx-agent       \033[0m: sudo systemctl start taosx-agent"

      echo -e "\n\033[32mtaosx-agent is installed successfully!\033[0m"
    fi
}

getUserInputEndpoint() {
  if [ -n "$explorerEndpoint" ]; then
    return
  fi

  echo "Set publicly accessible IP address or domain name you want expose to."
  echo "If you do not set it and press Enter directly, the default 'localhost' will be used."
  while true; do
    echo -n "Please enter fqdn or ip: "
    read endpoint
    if [ -z "$endpoint" ]; then
      echo "You need to enter explorer‘s fqdn or IP address!"
    else
      explorerEndpoint="$endpoint"
      echo "You have set explorer's fqdn or ip:${explorerEndpoint}"
      return
    fi
  done
}

function replaceExplorerEndpoint() {
  local FileName=$1  
  if [ -f "$FileName" ]; then
      ${csudo}sed -i "s/localhost/${explorerEndpoint}/g" $FileName
  fi
}

install_bin() {
  ${csudo}rm -f ${BIN_LINK_DIR}/$1 || :
  [ -x ${INSTALL_DIR}/$1 ] && ${csudo}ln -sf ${INSTALL_DIR}/$1 ${BIN_LINK_DIR}/$1 || :
}

# install new taosx and taosx-agent
install_taosx() {
    echo "taosx install starting..."
    # echo "install binary files to ${INSTALL_DIR}..."
    ${csudo}cp -fr bin/* ${INSTALL_DIR}
    check_and_create_directory "${TAOSX_ROOT_DIR}/plugins"
    # echo "install plugins to ${TAOSX_ROOT_DIR}/plugins..."
    ${csudo}cp -fr plugins/* ${TAOSX_ROOT_DIR}/plugins
    ${csudo}cp uninstall.sh ${TAOSX_ROOT_DIR}
    # echo "install services to ${SERVICE_CONFIG_DIR}..."
    ${csudo}cp -fr etc/systemd/system/* ${SERVICE_CONFIG_DIR}

    for service in "${services[@]}"; do
      install_bin $service
    done

    ${csudo}systemctl daemon-reload

    x_service_config="${SERVICE_CONFIG_DIR}/${xName}.service"
    if [ -e "$x_service_config" ]; then
      ${csudo}systemctl enable ${xName}
    fi

    explore_service_config="${SERVICE_CONFIG_DIR}/${explorerName}.service"
    if [ -e "$explore_service_config" ]; then
      ${csudo}systemctl enable ${explorerName}
    fi

    ${csudo}systemctl daemon-reload

    check_and_create_directory "${CONFIG_DIR}"
    getUserInputEndpoint
    # copy config to /etc/taos
    if [ -f ${CONFIG_DIR}/agent.toml ]; then
        ${csudo}cp -f ./etc/taos/agent.toml ${CONFIG_DIR}/agent.toml.new
    else
        ${csudo}cp -f ./etc/taos/agent.toml ${CONFIG_DIR}/
    fi
    echo "install toml file to ${CONFIG_DIR}..."
    if [ -f ./etc/taos/explorer.toml ]; then
        if [ -f ${CONFIG_DIR}/explorer.toml ]; then
            ${csudo}cp -f ./etc/taos/explorer.toml ${CONFIG_DIR}/explorer.toml.new
            replaceExplorerEndpoint ${CONFIG_DIR}/explorer.toml.new
        else
            ${csudo}cp -f ./etc/taos/explorer.toml ${CONFIG_DIR}/
            replaceExplorerEndpoint ${CONFIG_DIR}/explorer.toml
        fi
    fi
    # print_tips
}

check_java_env() {
    if ! command -v java &> /dev/null
    then
        echo -e "\033[31mWarning: Java command not found. Version 1.8+ is required.\033[0m"
        return
    fi

  java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
  java_version_ok=false
  if [[ $(echo "$java_version" | cut -d"." -f1) -gt 1 ]]; then
    java_version_ok=true
  elif [[ $(echo "$java_version" | cut -d"." -f1) -eq 1 && $(echo "$java_version" | cut -d"." -f2) -ge 8 ]]; then
    java_version_ok=true
  fi

  if $java_version_ok; then
    echo -e "\033[32mJava ${java_version} has been found.\033[0m"
  else
    echo -e "\033[31mWarning: Java Version 1.8+ is required, but version ${java_version} has been found.\033[0m"
  fi
}

while getopts "e:" arg; do
  case $arg in
    e)
      explorerEndpoint=$(echo $OPTARG)
      echo "explorer fqdn has been set to  $explorerEndpoint"
      ;;
    ?)
      echo "Usage: $0 [-e]"
      ;;
  esac
done

check_install_env(){
    echo "Check Java env for InfluxDB/OpenTSDB Connector"
    check_java_env
}
getUserInputEndpoint
check_install_env

# main entry point
if [ -x ${INSTALL_DIR}/${target} ]; then
    echo "${target} is already installed, do you want to reinstall it? [y/n]"
    read answer
    if [ X$answer == X"y" ]; then
        remove_target
        install_taosx
    else
        echo "${target} installation is cancelled"
    fi
else
    check_and_create_directory "${INSTALL_DIR}"
    install_taosx
fi
