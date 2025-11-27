#!/bin/bash
#
# This file is used to install analysis platform on linux systems. The operating system
# is required to use systemd to manage services at boot

set -e

serverFqdn=""

# -----------------------Variables definition---------------------
script_dir=$(dirname $(readlink -f "$0"))
echo -e "${script_dir}"

# Dynamic directory
PREFIX="taos"
PRODUCTPREFIX="taosanode"
serverName="${PRODUCTPREFIX}d"
configFile="taosanode.ini"
productName="TDengine Anode"
emailName="taosdata.com"
tarName="package.tar.gz"
logDir="/var/log/${PREFIX}/${PRODUCTPREFIX}"
moduleDir="/var/lib/${PREFIX}/${PRODUCTPREFIX}/model"
resourceDir="/var/lib/${PREFIX}/${PRODUCTPREFIX}/resource"
venvDir="/var/lib/${PREFIX}/${PRODUCTPREFIX}/venv"
global_conf_dir="/etc/${PREFIX}"
installDir="/usr/local/${PREFIX}/${PRODUCTPREFIX}"
tar_td_model_name="tdtsfm.tar.gz"
tar_xhs_model_name="timemoe.tar.gz"

python_minor_ver=0  #check the python version
bin_link_dir="/usr/bin"
# if install python venv
install_venv="${INSTALL_VENV:-True}"

#install main path
install_main_dir=${installDir}

service_config_dir="/etc/systemd/system"

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

csudo=""
if command -v sudo >/dev/null; then
  csudo="sudo "
fi

update_flag=0
prompt_force=0

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
elif echo $osinfo | grep -qwi "Linux"; then
  #  echo "This is Linux system"
  os_type=1
  service_mod=0
  initd_mod=0
  service_config_dir="/etc/systemd/system"
else
  echo " osinfo: ${osinfo}"
  echo " This is an officially unverified linux system,"
  echo " if there are any problems with the installation and operation, "
  echo " please feel free to contact ${emailName} for support."
  os_type=1
fi

# =============================  get input parameters =================================================
# set parameters by default value
verType=server      # [server | client]
initType=systemd    # [systemd | service | ...]

services=(${serverName})

function install_services() {
  for service in "${services[@]}"; do
    install_service ${service}
  done
}

function kill_process() {
  pid=$(ps -ef | grep "$1" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo}kill -9 "$pid" || :
  fi
}

function kill_model_service() {
  for script in stop-tdtsfm.sh stop-time-moe.sh; do
    script_path="${installDir}/bin/${script}"
    [ -f "${script_path}" ] && ${csudo}bash "${script_path}" || :
  done
}

function install_main_path() {
  #create install main dir and all sub dir
  if [ ! -z "${install_main_dir}" ]; then
    ${csudo}rm -rf ${install_main_dir} || :
  fi

  ${csudo}mkdir -p ${install_main_dir}
  ${csudo}mkdir -p ${install_main_dir}/cfg
  ${csudo}mkdir -p ${install_main_dir}/bin
  ${csudo}mkdir -p ${install_main_dir}/lib
  ${csudo}mkdir -p ${global_conf_dir}
}

function install_bin_and_lib() {
  ${csudo}cp -r ${script_dir}/bin/* ${install_main_dir}/bin
  ${csudo}cp -r ${script_dir}/lib/* ${install_main_dir}/lib/

  # Handle rmtaosanode separately
  [ -L "${bin_link_dir}/rmtaosanode" ] && ${csudo}rm -rf "${bin_link_dir}/rmtaosanode" || :
  ${csudo}ln -s "${install_main_dir}/bin/uninstall.sh" "${bin_link_dir}/rmtaosanode"

  # Create an array of link names and target scripts
  declare -A links=(
    ["start-tdtsfm"]="${install_main_dir}/bin/start-tdtsfm.sh"
    ["stop-tdtsfm"]="${install_main_dir}/bin/stop-tdtsfm.sh"
    ["start-time-moe"]="${install_main_dir}/bin/start-time-moe.sh"
    ["stop-time-moe"]="${install_main_dir}/bin/stop-time-moe.sh"
    ["start-model-from-remote"]="${install_main_dir}/bin/start_model_from_remote.sh"
  )

  # Iterate over the array and create/remove links as needed
  for link in "${!links[@]}"; do
    target="${links[$link]}"
    [ -L "${bin_link_dir}/${link}" ] && ${csudo}rm -rf "${bin_link_dir}/${link}" || :
    ${csudo}ln -s "${target}" "${bin_link_dir}/${link}"
  done
}

function install_anode_config() {
  fileName="${script_dir}/cfg/${configFile}"
  echo -e $fileName

  if [ -f ${fileName} ]; then
    ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$serverFqdn/" ${script_dir}/cfg/${configFile}

    if [ -f "${global_conf_dir}/${configFile}" ]; then
      ${csudo}cp ${fileName} ${global_conf_dir}/${configFile}.new
    else
      ${csudo}cp ${fileName} ${global_conf_dir}/${configFile}
    fi
  fi

  ${csudo}ln -sf ${global_conf_dir}/${configFile} ${install_main_dir}/cfg
}

function install_config() {

  [ ! -z $1 ] && return 0 || : # only install client

  install_anode_config

  echo
  echo -e -n "${GREEN}Enter FQDN:port (like h1.${emailName}:6030) of an existing ${productName} cluster node to join${NC}"
  echo
  echo -e -n "${GREEN}OR leave it blank to build one${NC}:"
  read firstEp
  while true; do
    if [ ! -z "$firstEp" ]; then
      if [ -f ${global_conf_dir}/${configFile} ]; then
        ${csudo}sed -i -r "s/#*\s*(firstEp\s*).*/\1$firstEp/" ${global_conf_dir}/${configFile}
      else
        ${csudo}sed -i -r "s/#*\s*(firstEp\s*).*/\1$firstEp/" ${script_dir}/cfg/${configFile}
      fi
      break
    else
      break
    fi
  done

  echo
  echo -e -n "${GREEN}Enter your email address for priority support or enter empty to skip${NC}: "
  read emailAddr
  while true; do
    if [ ! -z "$emailAddr" ]; then
      email_file="${install_main_dir}/email"
      ${csudo}bash -c "echo $emailAddr > ${email_file}"
      break
    else
      break
    fi
  done
}

function install_log() {
  ${csudo}mkdir -p ${logDir} && ${csudo}chmod 777 ${logDir}
  ${csudo}ln -sf ${logDir} ${install_main_dir}/log
}

function install_module() {
  ${csudo}mkdir -p ${moduleDir} && ${csudo}chmod 777 ${moduleDir}
  ${csudo}ln -sf ${moduleDir} ${install_main_dir}/model
  [ -f "${script_dir}/model/${tar_td_model_name}" ] && cp -r ${script_dir}/model/* ${moduleDir}/ || : 
}

function install_resource() {
  ${csudo}mkdir -p ${resourceDir} && ${csudo}chmod 777 ${resourceDir}
  ${csudo}ln -sf ${resourceDir} ${install_main_dir}/resource

  ${csudo}cp ${script_dir}/resource/*.sql ${install_main_dir}/resource/
}

function install_anode_venv() {
  ${csudo}mkdir -p ${venvDir} && ${csudo}chmod 777 ${venvDir}
  ${csudo}ln -sf ${venvDir} ${install_main_dir}/venv

  if [ ${install_venv} == "True" ]; then
    # build venv
    ${csudo}python3.${python_minor_ver} -m venv ${venvDir}

    echo -e "active Python3 virtual env: ${venvDir}"
    source ${venvDir}/bin/activate

    echo -e "install the required packages by pip3, this may take a while depending on the network condition"
    ${csudo}${venvDir}/bin/pip3 install -r ${script_dir}/requirements_ess.txt

    echo -e "Install python library for venv completed!"
  else
    echo -e "Install python library for venv skipped!"
  fi
}

function clean_service_on_sysvinit() {
  if ps aux | grep -v grep | grep $1 &>/dev/null; then
    ${csudo}service $1 stop || :
  fi

  if ((${initd_mod} == 1)); then
    if [ -e ${service_config_dir}/$1 ]; then
      ${csudo}chkconfig --del $1 || :
    fi
  elif ((${initd_mod} == 2)); then
    if [ -e ${service_config_dir}/$1 ]; then
      ${csudo}insserv -r $1 || :
    fi
  elif ((${initd_mod} == 3)); then
    if [ -e ${service_config_dir}/$1 ]; then
      ${csudo}update-rc.d -f $1 remove || :
    fi
  fi

  ${csudo}rm -f ${service_config_dir}/$1 || :

  if $(which init &>/dev/null); then
    ${csudo}init q || :
  fi
}

function install_service_on_sysvinit() {
  if [ "$1" != "${serverName}" ]; then
    return
  fi

  clean_service_on_sysvinit $1
  sleep 1

  if ((${os_type} == 1)); then
    ${csudo}cp ${script_dir}/init.d/${serverName}.deb ${service_config_dir}/${serverName} && ${csudo}chmod a+x ${service_config_dir}/${serverName}
  elif ((${os_type} == 2)); then
    ${csudo}cp ${script_dir}/init.d/${serverName}.rpm ${service_config_dir}/${serverName} && ${csudo}chmod a+x ${service_config_dir}/${serverName}
  fi

  if ((${initd_mod} == 1)); then
    ${csudo}chkconfig --add $1 || :
    ${csudo}chkconfig --level 2345 $1 on || :
  elif ((${initd_mod} == 2)); then
    ${csudo}insserv $1} || :
    ${csudo}insserv -d $1 || :
  elif ((${initd_mod} == 3)); then
    ${csudo}update-rc.d $1 defaults || :
  fi
}

function clean_service_on_systemd() {
  service_config="${service_config_dir}/$1.service"

  if systemctl is-active --quiet $1; then
    echo "$1 is running, stopping it..."
    ${csudo}systemctl stop $1 &>/dev/null || echo &>/dev/null
  fi
  ${csudo}systemctl disable $1 &>/dev/null || echo &>/dev/null
  ${csudo}rm -f ${service_config}
}

function install_service_on_systemd() {
  clean_service_on_systemd $1

  cfg_source_dir=${script_dir}/cfg
  if [[ "$1" == "${xname}" || "$1" == "${explorerName}" ]]; then
      cfg_source_dir=${script_dir}/cfg
  fi

  if [ -f ${cfg_source_dir}/$1.service ]; then
    ${csudo}cp ${cfg_source_dir}/$1.service ${service_config_dir}/ || :
  fi

  ${csudo}systemctl enable $1
  ${csudo}systemctl daemon-reload
}

function is_container() {
  if [[ -f /.dockerenv ]] || grep -q "docker\|kubepods" /proc/1/cgroup || [[ -n "$KUBERNETES_SERVICE_HOST" || "$container" == "docker" ]]; then
    return 0  # container env
  else
    return 1  # not container env
  fi
}

function install_service() {
  if ((${service_mod} == 0)); then
    install_service_on_systemd $1
  elif ((${service_mod} == 1)); then
    install_service_on_sysvinit $1
  else
    kill_process $1
  fi
}

function installProduct() {
  # Start to install
  if [ ! -e ${tarName} ]; then
    echo "File ${tarName} does not exist"
    exit 1
  fi

  tar -zxf ${tarName}
 
  [ -f "${script_dir}/model/${tar_td_model_name}" ]  && tar -zxf ${script_dir}/model/${tar_td_model_name} -C ${script_dir}/model || :
  [ -f "${script_dir}/model/${tar_xhs_model_name}" ] && tar -zxf ${script_dir}/model/${tar_xhs_model_name} -C ${script_dir}/model || :

  echo "Start to install ${productName}..."

  install_main_path
  install_log
  install_anode_config
  install_module
  install_resource
  
  install_bin_and_lib
  kill_model_service

  if ! is_container; then
    install_services
  fi

  echo
  echo -e "\033[44;32;1m${productName} is installed successfully!${NC}"

  echo
  echo -e "\033[44;32;1mStart to create virtual python env in ${venvDir}${NC}"
  install_anode_venv
}

# check for python version, only the 3.10/3.11 is supported
check_python3_env() {
  if ! command -v python3 &> /dev/null
  then
      echo -e "\033[31mWarning: Python3 command not found. Version 3.10/3.11 is required.\033[0m"
      exit 1
  fi

  python3_version=$(python3 --version 2>&1 | awk -F' ' '{print $2}')

  python3_version_ok=false
  python_minor_ver=$(echo "$python3_version" | cut -d"." -f2)
  if [[ $(echo "$python3_version" | cut -d"." -f1) -eq 3 && $(echo "$python3_version" | cut -d"." -f2) -ge 10 ]]; then
    python3_version_ok=true
  fi

  if $python3_version_ok; then
    echo -e "\033[32mPython3 ${python3_version} has been found.\033[0m"
  else
    if command -v python3.10 &> /dev/null
    then
      echo -e "\033[32mPython3.10 has been found.\033[0m"
      python_minor_ver=10
    elif command -v python3.11 &> /dev/null
    then
      python_minor_ver=11
      echo -e "\033[32mPython3.11 has been found.\033[0m"
    else
      echo -e "\033[31mWarning: Python3.10/3.11 is required, only found python${python3_version}.\033[0m"
      exit 1
    fi
  fi

#  echo -e "Python3 minor version is:${python_minor_ver}"

  # check the existence pip3.10/pip3.11
  if ! command -v pip3 &> /dev/null
  then
    echo -e "\033[31mWarning: Pip3 command not found. Version 3.10/3.11 is required.\033[0m"
    exit 1
  fi

  pip3_version=$(pip3 --version 2>&1 | awk -F' ' '{print $6}' | cut -d")" -f1)
  major_ver=$(echo "${pip3_version}" | cut -d"." -f1)
  minor_ver=$(echo "${pip3_version}" | cut -d"." -f2)

  pip3_version_ok=false;
  if [[ ${major_ver} -eq 3 && ${minor_ver} -ge 10 ]]; then
    pip3_version_ok=true
  fi

  if $pip3_version_ok; then
    echo -e "\033[32mpip3 ${pip3_version} has been found.\033[0m"
  else
    if command -v pip3.${python_minor_ver} &> /dev/null
    then
      echo -e "\033[32mpip3.${python_minor_ver} has been found.\033[0m"
    else
      echo -e "\033[31mWarning: pip3.10/3.11 is required, only found pip${pip3_version}.\033[0m"
     exit 1
    fi
  fi

#  if ! command -v python3.${python_minor_ver}-venv &> /dev/null
#  then
#    echo -e "\033[31mWarning: python3.${python_minor_ver}-venv command not found.\033[0m"
#    exit 1
#  fi
}

## ==============================Main program starts from here============================
serverFqdn=$(hostname)

if [ "$verType" == "server" ]; then
    check_python3_env
    installProduct
fi
