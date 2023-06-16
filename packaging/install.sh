#!/bin/bash
set -e

PREFIX="taos"
INSTALL_DIR="/usr/local/${PREFIX}X"
BIN_LINK_DIR="/usr/bin"
CONFIG_DIR="/etc/${PREFIX}x"
service_config_dir="/etc/systemd/system"
xName="${PREFIX}x"
agentname="${PREFIX}x-agent"
exploreName="${PREFIX}-explore"
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
  service_config_dir="/etc/systemd/system"
else
  echo " osinfo: ${osinfo}"
  echo " This is an officially unverified linux system,"
  echo " if there are any problems with the installation and operation, "
  os_type=1
fi

stop_taosx_service(){
    x_service_config="${service_config_dir}/${xName}.service"
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
    agent_service_config="${service_config_dir}/${agentname}.service"
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
    explore_service_config="${service_config_dir}/${exploreName}.service"
    if [ -e "$explore_service_config" ]; then
      if systemctl is-active --quiet ${exploreName}; then
        echo "${exploreName} is running, stopping it..."
        ${csudo}systemctl stop ${exploreName} &>/dev/null || echo &>/dev/null
      fi
      ${csudo}systemctl disable ${exploreName} &>/dev/null || echo &>/dev/null
      ${csudo}rm -f ${explore_service_config}
    fi
}


# remove old taosx and taosx-agent
remove_taosx() {
    stop_taosx_service
    stop_taosx_agent_service
    stop_explore_service

    ${csudo}rm -rf ${INSTALL_DIR}/bin
    ${csudo}rm -rf ${INSTALL_DIR}/plugins
    ${csudo}rm -rf ${INSTALL_DIR}/scripts
    ${csudo}rm -rf ${INSTALL_DIR}/rmtaosX.sh
    [ -x ${BIN_LINK_DIR}/rm${PREFIX}x ] && ${csudo}rm -f ${BIN_LINK_DIR}/rm${PREFIX}x || :
    [ -x ${INSTALL_DIR}/bin/${PREFIX}x ] && ${csudo}rm -f ${BIN_LINK_DIR}/${PREFIX}x || :
    [ -x ${INSTALL_DIR}/bin/${PREFIX}x-agent ] && ${csudo}rm -rf ${BIN_LINK_DIR}/${PREFIX}x-agent || :
    [ -x ${INSTALL_DIR}/bin/${exploreName} ] && ${csudo}rm -rf ${BIN_LINK_DIR}/${exploreName} || :
}

# install new taosx and taosx-agent
install_taosx() {
    ${csudo}cp -r bin ${INSTALL_DIR}
    ${csudo}cp -r plugins ${INSTALL_DIR}
    ${csudo}cp -r scripts ${INSTALL_DIR}
    if [ -d config ]; then
        ${csudo}cp -r config ${INSTALL_DIR}
    fi
    ${csudo}cp rm${PREFIX}X.sh ${INSTALL_DIR}

    # create symbolic link /usr/bin

    # copy config to /etc/taosx
    if [ ! -d /etc/${PREFIX}x ]; then
        ${csudo}mkdir -p /etc/taosx
    fi
    
    if [ -f /etc/taosx/${PREFIX}x-agent.example.toml ]; then
        ${csudo}cp ./config/agent.example.toml /etc/taosx/agent.example.toml.new
    else
       ${csudo}cp ./config/agent.example.toml /etc/taosx/
    fi

    [ -x ${INSTALL_DIR}/rm${PREFIX}X.sh ] && ${csudo}ln -sf ${INSTALL_DIR}/rm${PREFIX}X.sh  ${BIN_LINK_DIR}/rm${PREFIX}x || :
    [ -x ${INSTALL_DIR}/bin/${PREFIX}x ] && ${csudo}ln -sf ${INSTALL_DIR}/bin/${PREFIX}x  ${BIN_LINK_DIR}/${PREFIX}x || :
    [ -x ${INSTALL_DIR}/bin/${PREFIX}x-agent ] && ${csudo}ln -sf ${INSTALL_DIR}/bin/${PREFIX}x-agent  ${BIN_LINK_DIR}/${PREFIX}x-agent || :
    [ -x ${INSTALL_DIR}/bin/${exploreName} ] && ${csudo}ln -sf ${INSTALL_DIR}/bin/${exploreName}  ${BIN_LINK_DIR}/${exploreName} || :
}

# install taosx service
install_taosx_service(){
  if ((${service_mod} == 0)); then
    [ -f ./scripts/${xName}.service ] &&
      ${csudo}cp ./scripts/${xName}.service  ${service_config_dir}/ || :
    [ -f ./scripts/${agentname}.service ] &&
      ${csudo}cp ./scripts/${agentname}.service ${service_config_dir}/ || :
    [ -f ./scripts/${exploreName}.service ] &&
      ${csudo}cp ./scripts/${exploreName}.service ${service_config_dir}/ || :
    ${csudo}systemctl daemon-reload
  fi
}


# main entry point
if [ -x ${INSTALL_DIR}/bin/${xName} ]; then
    echo "${xName} is already installed, do you want to reinstall it? [y/n]"
    read answer
    if [ $answer == "y" ]; then
        stop_taosx_service
        stop_taosx_agent_service
        stop_explore_service
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
