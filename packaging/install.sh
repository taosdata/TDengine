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
csudo=""
explorerEndpoint=""

target=""
verNumber=3.0

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

check_and_create_directory() {
  if [ ! -d "$1" ]; then
    ${csudo} mkdir -p "$1"
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
    if [ -f ./bin/${xName} ]; then
        stop_taosx_service
        ${csudo}rm -rf ${INSTALL_DIR}/${xName}
    fi
    if [ -f ./bin/${explorerName} ]; then
        stop_explorer_service
        ${csudo}rm -rf ${INSTALL_DIR}/${explorerName}
    fi
    if [ -f ./bin/${agentname} ]; then
        stop_taosx_agent_service
        ${csudo}rm -rf ${INSTALL_DIR}/${agentname}
    fi
    ${csudo}rm -rf ${TAOSX_ROOT_DIR}/plugins
    ${csudo}rm -rf ${TAOSX_ROOT_DIR}/uninstall.sh
}

remove_explorer() {
    if [ -f ./bin/${explorerName} ]; then
        stop_explorer_service
        ${csudo}rm -rf ${INSTALL_DIR}/${explorerName}
    fi
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
    if [ "$target" = ${xName} ]; then
      remove_taosx
    elif  [ "$target" = ${explorerName} ]; then
      remove_explorer
    else
      remove_taos_agent
    fi
}

print_tips(){
    if [ "$target" = ${xName} ]; then
      echo -e "\033[32mTo configure ${xName}         \033[0m: edit /etc/${PREFIX}/${xName}.toml"
      # echo -e "\033[32mTo configure taosx-agent    \033[0m: edit /etc/taos/agent.toml"
      echo -e "\033[32mTo configure ${explorerName}  \033[0m: edit /etc/${PREFIX}/explorer.toml"
      echo -e "\033[32mTo start ${xName}             \033[0m: sudo systemctl start ${xName}"
      # echo -e "\033[32mTo start taosx-agent        \033[0m: sudo systemctl start taosx-agent"
      echo -e "\033[32mTo start ${explorerName}      \033[0m: sudo systemctl start ${explorerName}"

      echo -e "\n\033[32mtaosX and taosExplorer are installed successfully!\033[0m"
      echo -e "\033[32mTo access the TDengine TSDB management system: http://`hostname`:6060\033[0m"
      echo -e "\033[32mTo read the TDengine TSDB user manual: http://`hostname`:6060/docs-en\033[0m"
    elif [ "$target" = ${explorerName} ]; then
      echo -e "\033[32mTo configure ${explorerName}  \033[0m: edit /etc/${PREFIX}/explorer.toml"
      echo -e "\033[32mTo start ${explorerName}      \033[0m: sudo systemctl start ${explorerName}"
      echo -e "\n\033[32mtaosExplorer are installed successfully!\033[0m"
      echo -e "\033[32mTo access the TDengine TSDB management system: http://`hostname`:6060\033[0m"
      echo -e "\033[32mTo read the TDengine TSDB user manual: http://`hostname`:6060/docs-en\033[0m"
    else
      # echo -e "\033[32mTo configure taosx         \033[0m: edit /etc/taos/taosx.toml"
      echo -e "\033[32mTo configure ${agentname}   \033[0m: edit /etc/${PREFIX}/agent.toml"
      echo -e "\033[32mTo start ${agentname}       \033[0m: sudo systemctl start ${agentname}"

      echo -e "\n\033[32m${agentname} is installed successfully!\033[0m"
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
      #echo "You need to enter explorer‘s fqdn or IP address!"
      explorerEndpoint="localhost"
      echo "You have set explorer's fqdn or ip:${explorerEndpoint}"
      return
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
    if echo `uname` | grep -qwi "Darwin"; then
      ${csudo}sed -i '' "s/localhost/${explorerEndpoint}/g" $FileName
    else
      ${csudo}sed -i "s/localhost/${explorerEndpoint}/g" $FileName
    fi
  fi
}

install_taosx_only() {
    echo "install starting..."
    echo "install binary files to ${INSTALL_DIR}..."
    ${csudo}cp -fr bin/* ${INSTALL_DIR}
    check_and_create_directory "${TAOSX_ROOT_DIR}/plugins"
    echo "install plugins to ${TAOSX_ROOT_DIR}/plugins..."
    ${csudo}cp -fr plugins/* ${TAOSX_ROOT_DIR}/plugins
    ${csudo}cp uninstall.sh ${TAOSX_ROOT_DIR}
    echo "install services to ${SERVICE_CONFIG_DIR}..."
    ${csudo}cp -fr etc/systemd/system/* ${SERVICE_CONFIG_DIR}

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
    if [ -f ${CONFIG_DIR}/${xName}.toml ]; then
        ${csudo}cp -f ./etc/taos/taosx.toml ${CONFIG_DIR}/${xName}.toml.new
    else
        ${csudo}cp -f ./etc/taos/taosx.toml ${CONFIG_DIR}/${xName}.toml
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
    print_tips
}

install_explorer_only(){
    echo "install starting..."
    echo "install binary files to ${INSTALL_DIR}..."
    ${csudo}cp -fr bin/* ${INSTALL_DIR}
    check_and_create_directory ${TAOSX_ROOT_DIR}
    ${csudo}cp uninstall_explorer.sh ${TAOSX_ROOT_DIR}
    echo "install services to ${SERVICE_CONFIG_DIR}..."

    if echo `uname` | grep -qwi "Darwin"; then
      if [ -f etc/launchd/com.tdengine.taos-explorer.plist ]; then
        ${csudo}launchctl unload -w ${SERVICE_CONFIG_DIR}/com.tdengine.taos-explorer.plist
        ${csudo}cp -fr etc/launchd/* ${SERVICE_CONFIG_DIR}
        ${csudo}launchctl load -w ${SERVICE_CONFIG_DIR}/com.tdengine.taos-explorer.plist || :
      fi
    else
      ${csudo}cp -fr etc/systemd/system/* ${SERVICE_CONFIG_DIR}
      ${csudo}systemctl daemon-reload
      explore_service_config="${SERVICE_CONFIG_DIR}/${explorerName}.service"
      if [ -e "$explore_service_config" ]; then
        ${csudo}systemctl enable ${explorerName}
      fi

      ${csudo}systemctl daemon-reload
    fi

    check_and_create_directory "${CONFIG_DIR}"
    getUserInputEndpoint
    # copy config to /etc/taos
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
    print_tips
}

install_agent_only(){
  echo "install starting..."
  echo "install binary files to ${INSTALL_DIR}..."
  ${csudo}cp -fr bin/* ${INSTALL_DIR}
  check_and_create_directory "${TAOSX_ROOT_DIR}/plugins"
  echo "install plugins to ${TAOSX_ROOT_DIR}/plugins..."
  ${csudo}cp -fr plugins/* ${TAOSX_ROOT_DIR}/plugins
  ${csudo}cp uninstall.sh ${TAOSX_ROOT_DIR}
  echo "install services to ${SERVICE_CONFIG_DIR}..."
  ${csudo}cp -fr etc/systemd/system/* ${SERVICE_CONFIG_DIR}
  
  ${csudo}systemctl daemon-reload

  check_and_create_directory "${CONFIG_DIR}"
  echo "install agent.toml file to ${CONFIG_DIR}..."
  if [ -f ${CONFIG_DIR}/agent.toml ]; then
      ${csudo}cp -f ./etc/taos/agent.toml ${CONFIG_DIR}/agent.toml.new
  else
      [ -e ./etc/taos/agent.toml ] && ${csudo}cp -f ./etc/taos/agent.toml ${CONFIG_DIR}/
  fi
}

# install new taosx and taosx-agent
install_target() {
  if [ "$target" = ${xName} ]; then
      install_taosx_only
  elif  [ "$target" = ${explorerName} ]; then
      install_explorer_only
  else
      install_agent_only
  fi
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
  if  [ "$target" != ${explorerName} ]; then
    echo "Check Java env for InfluxDB/OpenTSDB Connector"
    check_java_env
  fi
}
check_install_env

# main entry point
if [ -x ${INSTALL_DIR}/${target} ]; then
    echo "${target} is already installed, do you want to reinstall it? [y/n]"
    read answer
    if [ X$answer == X"y" ]; then
        remove_target
        install_target
    else
        echo "${target} installation is cancelled"
        exit 1
    fi
else
    check_and_create_directory "${INSTALL_DIR}"
    install_target
fi
