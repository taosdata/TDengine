#!/bin/bash
#
# This file is used to install analysis platform on linux systems. The operating system
# is required to use systemd to manage services at boot

set -e

iplist=""
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
venvDir="/var/lib/${PREFIX}/${PRODUCTPREFIX}/venv"
global_conf_dir="/etc/${PREFIX}"
installDir="/usr/local/${PREFIX}/${PRODUCTPREFIX}"

python_minor_ver=0  #check the python version
bin_link_dir="/usr/bin"

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

# install.sh -v [server | client]  -e [yes | no] -i [systemd | service | ...]

# set parameters by default value
interactiveFqdn=yes # [yes | no]
verType=server      # [server | client]
initType=systemd    # [systemd | service | ...]

while getopts "hv:e:" arg; do
  case $arg in
  e)
    #echo "interactiveFqdn=$OPTARG"
    interactiveFqdn=$(echo $OPTARG)
    ;;
  v)
    #echo "verType=$OPTARG"
    verType=$(echo $OPTARG)
    ;;
  h)
    echo "Usage: $(basename $0) -v [server | client]  -e [yes | no]"
    exit 0
    ;;
  ?) #unknow option
    echo "unknown argument"
    exit 1
    ;;
  esac
done

#echo "verType=${verType} interactiveFqdn=${interactiveFqdn}"

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

  if [[ ! -e "${bin_link_dir}/rmtaosanode" ]]; then
    ${csudo}ln -s ${install_main_dir}/bin/uninstall.sh ${bin_link_dir}/rmtaosanode
  fi
}

function add_newHostname_to_hosts() {
  localIp="127.0.0.1"
  OLD_IFS="$IFS"
  IFS=" "
  iphost=$(cat /etc/hosts | grep $1 | awk '{print $1}')
  arr=($iphost)
  IFS="$OLD_IFS"
  for s in "${arr[@]}"; do
    if [[ "$s" == "$localIp" ]]; then
      return
    fi
  done

  if grep -q "127.0.0.1  $1" /etc/hosts; then
    return
  else
    ${csudo}chmod 666 /etc/hosts
    ${csudo}echo "127.0.0.1  $1" >>/etc/hosts
  fi
}

function set_hostname() {
  echo -e -n "${GREEN}Host name or IP (assigned to this machine) which can be accessed by your tools or apps (must not be 'localhost')${NC}"
  read -e -p " : " -i "$(hostname)" newHostname
  while true; do
    if [ -z "$newHostname" ]; then
      newHostname=$(hostname)
      break
    elif [ "$newHostname" != "localhost" ]; then
      break
    else
      echo -e -n "${GREEN}Host name or IP (assigned to this machine) which can be accessed by your tools or apps (must not be 'localhost')${NC}"
      read -e -p " : " -i "$(hostname)" newHostname
    fi
  done

  if [ -f ${global_conf_dir}/${configFile} ]; then
    ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$newHostname/" ${global_conf_dir}/${configFile}
  else
    ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$newHostname/" ${script_dir}/cfg/${configFile}
  fi
  serverFqdn=$newHostname

  if [[ -e /etc/hosts ]] && [[ ! $newHostname =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]]; then
    add_newHostname_to_hosts $newHostname
  fi
}

function is_correct_ipaddr() {
  newIp=$1
  OLD_IFS="$IFS"
  IFS=" "
  arr=($iplist)
  IFS="$OLD_IFS"
  for s in "${arr[@]}"; do
    if [[ "$s" == "$newIp" ]]; then
      return 0
    fi
  done

  return 1
}

function set_ipAsFqdn() {
  iplist=$(ip address | grep inet | grep -v inet6 | grep -v 127.0.0.1 | awk '{print $2}' | awk -F "/" '{print $1}') || :
  if [ -z "$iplist" ]; then
    iplist=$(ifconfig | grep inet | grep -v inet6 | grep -v 127.0.0.1 | awk '{print $2}' | awk -F ":" '{print $2}') || :
  fi

  if [ -z "$iplist" ]; then
    echo
    echo -e -n "${GREEN}Unable to get local ip, use 127.0.0.1${NC}"
    localFqdn="127.0.0.1"
    # Write the local FQDN to configuration file

    if [ -f ${global_conf_dir}/${configFile} ]; then
      ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$localFqdn/" ${global_conf_dir}/${configFile}
    else
      ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$localFqdn/" ${script_dir}/cfg/${configFile}
    fi
    serverFqdn=$localFqdn
    echo
    return
  fi

  echo -e -n "${GREEN}Please choose an IP from local IP list${NC}:"
  echo
  echo -e -n "${GREEN}$iplist${NC}"
  echo
  echo
  echo -e -n "${GREEN}Notes: if IP is used as the node name, data can NOT be migrated to other machine directly${NC}:"
  read localFqdn
  while true; do
    if [ ! -z "$localFqdn" ]; then
      # Check if correct ip address
      is_correct_ipaddr $localFqdn
      retval=$(echo $?)
      if [[ $retval != 0 ]]; then
        read -p "Please choose an IP from local IP list:" localFqdn
      else
        # Write the local FQDN to configuration file
        if [ -f ${global_conf_dir}/${configFile} ]; then
          ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$localFqdn/" ${global_conf_dir}/${configFile}
        else
          ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$localFqdn/" ${script_dir}/cfg/${configFile}
        fi
        serverFqdn=$localFqdn
        break
      fi
    else
      read -p "Please choose an IP from local IP list:" localFqdn
    fi
  done
}

function local_fqdn_check() {
  #serverFqdn=$(hostname)
  echo
  echo -e -n "System hostname is: ${GREEN}$serverFqdn${NC}"
  echo
  set_hostname
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

  if ((${update_flag} == 1)); then
    install_taosd_config
    return 0
  fi

  if [ "$interactiveFqdn" == "no" ]; then
    install_taosd_config
    return 0
  fi

  local_fqdn_check
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
}

function install_anode_venv() {
  ${csudo}mkdir -p ${venvDir} && ${csudo}chmod 777 ${venvDir}
  ${csudo}ln -sf ${venvDir} ${install_main_dir}/venv

  # build venv
  ${csudo}python3.${python_minor_ver} -m venv ${venvDir}

  echo -e "active Python3 virtual env: ${venvDir}"
  source ${venvDir}/bin/activate

  echo -e "install the required packages by pip3, this may take a while depending on the network condition"
  ${csudo}${venvDir}/bin/pip3 install numpy==1.26.4
  ${csudo}${venvDir}/bin/pip3 install pandas==1.5.0

  ${csudo}${venvDir}/bin/pip3 install scikit-learn
  ${csudo}${venvDir}/bin/pip3 install outlier_utils
  ${csudo}${venvDir}/bin/pip3 install statsmodels
  ${csudo}${venvDir}/bin/pip3 install pyculiarity
  ${csudo}${venvDir}/bin/pip3 install pmdarima
  ${csudo}${venvDir}/bin/pip3 install flask
  ${csudo}${venvDir}/bin/pip3 install matplotlib
  ${csudo}${venvDir}/bin/pip3 install uwsgi
  ${csudo}${venvDir}/bin/pip3 install torch --index-url https://download.pytorch.org/whl/cpu
  ${csudo}${venvDir}/bin/pip3 install --upgrade keras

  echo -e "Install python library for venv completed!"
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

function install_service() {
  if ((${service_mod} == 0)); then
    install_service_on_systemd $1
  elif ((${service_mod} == 1)); then
    install_service_on_sysvinit $1
  else
    kill_process $1
  fi
}

vercomp() {
  if [[ $1 == $2 ]]; then
    return 0
  fi
  local IFS=.
  local i ver1=($1) ver2=($2)
  # fill empty fields in ver1 with zeros
  for ((i = ${#ver1[@]}; i < ${#ver2[@]}; i++)); do
    ver1[i]=0
  done

  for ((i = 0; i < ${#ver1[@]}; i++)); do
    if [[ -z ${ver2[i]} ]]; then
      # fill empty fields in ver2 with zeros
      ver2[i]=0
    fi
    if ((10#${ver1[i]} > 10#${ver2[i]})); then
      return 1
    fi
    if ((10#${ver1[i]} < 10#${ver2[i]})); then
      return 2
    fi
  done
  return 0
}

function is_version_compatible() {

  curr_version=$(ls ${script_dir}/driver/libtaos.so* | awk -F 'libtaos.so.' '{print $2}')

  if [ -f ${script_dir}/driver/vercomp.txt ]; then
    min_compatible_version=$(cat ${script_dir}/driver/vercomp.txt)
  else
    min_compatible_version=$(${script_dir}/bin/${serverName} -V | grep version | head -1 | cut -d ' ' -f 5)
  fi

  exist_version=$(${installDir}/bin/${serverName} -V | grep version | head -1 | cut -d ' ' -f 3)
  vercomp $exist_version "3.0.0.0"
  case $? in
  2)
    prompt_force=1
    ;;
  esac

  vercomp $curr_version $min_compatible_version
  echo "" # avoid $? value not update

  case $? in
  0) return 0 ;;
  1) return 0 ;;
  2) return 1 ;;
  esac
}

deb_erase() {
  confirm=""
  while [ "" == "${confirm}" ]; do
    echo -e -n "${RED}Existing TDengine deb is detected, do you want to remove it? [yes|no] ${NC}:"
    read confirm
    if [ "yes" == "$confirm" ]; then
      ${csudo}dpkg --remove tdengine || :
      break
    elif [ "no" == "$confirm" ]; then
      break
    fi
  done
}

rpm_erase() {
  confirm=""
  while [ "" == "${confirm}" ]; do
    echo -e -n "${RED}Existing TDengine rpm is detected, do you want to remove it? [yes|no] ${NC}:"
    read confirm
    if [ "yes" == "$confirm" ]; then
      ${csudo}rpm -e tdengine || :
      break
    elif [ "no" == "$confirm" ]; then
      break
    fi
  done
}

function updateProduct() {
  # Check if version compatible
  if ! is_version_compatible; then
    echo -e "${RED}Version incompatible${NC}"
    return 1
  fi

  # Start to update
  if [ ! -e ${tarName} ]; then
    echo "File ${tarName} does not exist"
    exit 1
  fi

  if echo $osinfo | grep -qwi "centos"; then
    rpm -q tdengine 2>&1 >/dev/null && rpm_erase tdengine || :
  elif echo $osinfo | grep -qwi "ubuntu"; then
    dpkg -l tdengine 2>&1 | grep ii >/dev/null && deb_erase tdengine || :
  fi

  tar -zxf ${tarName}

  echo "Start to update ${productName}..."
  # Stop the service if running
  if ps aux | grep -v grep | grep ${serverName} &>/dev/null; then
    if ((${service_mod} == 0)); then
      ${csudo}systemctl stop ${serverName} || :
    elif ((${service_mod} == 1)); then
      ${csudo}service ${serverName} stop || :
    else
      kill_process ${serverName}
    fi
    sleep 1
  fi

  install_main_path
  install_log
  install_module
  install_config

  if [ -z $1 ]; then
    install_bin
    install_services

    echo
    echo -e "${GREEN_DARK}To configure ${productName} ${NC}\t\t: edit ${global_conf_dir}/${configFile}"
    [ -f ${global_conf_dir}/${adapterName}.toml ] && [ -f ${installDir}/bin/${adapterName} ] &&
      echo -e "${GREEN_DARK}To configure ${adapterName} ${NC}\t: edit ${global_conf_dir}/${adapterName}.toml"
    echo -e "${GREEN_DARK}To configure ${explorerName} ${NC}\t: edit ${global_conf_dir}/explorer.toml"
    if ((${service_mod} == 0)); then
      echo -e "${GREEN_DARK}To start ${productName} server     ${NC}\t: ${csudo}systemctl start ${serverName}${NC}"
    elif ((${service_mod} == 1)); then
      echo -e "${GREEN_DARK}To start ${productName} server     ${NC}\t: ${csudo}service ${serverName} start${NC}"
    else
      echo -e "${GREEN_DARK}To start ${productName} server     ${NC}\t: ./${serverName}${NC}"
    fi

    echo
    echo "${productName} is updated successfully!"
    echo

  else
    install_bin
  fi

  cd $script_dir
  rm -rf $(tar -tf ${tarName} | grep -Ev "^\./$|^\/")
}

function installProduct() {
  # Start to install
  if [ ! -e ${tarName} ]; then
    echo "File ${tarName} does not exist"
    exit 1
  fi

  tar -zxf ${tarName}

  echo "Start to install ${productName}..."

  install_main_path
  install_log
  install_anode_config
  install_module

  install_bin_and_lib
  install_services

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
