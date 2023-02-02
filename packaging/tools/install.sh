#!/bin/bash
#
# This file is used to install database on linux systems. The operating system
# is required to use systemd to manage services at boot

set -e
#set -x

verMode=edge
pagMode=full

iplist=""
serverFqdn=""

# -----------------------Variables definition---------------------
script_dir=$(dirname $(readlink -f "$0"))
# Dynamic directory

clientName="taos"
serverName="taosd"
udfdName="udfd"
configFile="taos.cfg"
productName="TDengine"
emailName="taosdata.com"
uninstallScript="rmtaos"
historyFile="taos_history"
tarName="taos.tar.gz"
dataDir="/var/lib/taos"
logDir="/var/log/taos"
configDir="/etc/taos"
installDir="/usr/local/taos"
adapterName="taosadapter"
benchmarkName="taosBenchmark"
dumpName="taosdump"
demoName="taosdemo"
xname="taosx"

data_dir=${dataDir}
log_dir=${logDir}
cfg_install_dir=${configDir}

bin_link_dir="/usr/bin"
lib_link_dir="/usr/lib"
lib64_link_dir="/usr/lib64"
inc_link_dir="/usr/include"

#install main path
install_main_dir=${installDir}
# old bin dir
bin_dir="${installDir}/bin"

service_config_dir="/etc/systemd/system"
web_port=6041

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
  echo " please feel free to contact ${emailName} for support."
  os_type=1
fi

# =============================  get input parameters =================================================

# install.sh -v [server | client]  -e [yes | no] -i [systemd | service | ...]

# set parameters by default value
interactiveFqdn=yes # [yes | no]
verType=server      # [server | client]
initType=systemd    # [systemd | service | ...]

while getopts "hv:e:i:" arg; do
  case $arg in
  e)
    #echo "interactiveFqdn=$OPTARG"
    interactiveFqdn=$(echo $OPTARG)
    ;;
  v)
    #echo "verType=$OPTARG"
    verType=$(echo $OPTARG)
    ;;
  i)
    #echo "initType=$OPTARG"
    initType=$(echo $OPTARG)
    ;;
  h)
    echo "Usage: $(basename $0) -v [server | client]  -e [yes | no]"
    exit 0
    ;;
  ?) #unknow option
    echo "unkonw argument"
    exit 1
    ;;
  esac
done

#echo "verType=${verType} interactiveFqdn=${interactiveFqdn}"

function kill_process() {
  pid=$(ps -ef | grep "$1" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo}kill -9 $pid || :
  fi
}

function install_main_path() {
  #create install main dir and all sub dir
  ${csudo}rm -rf ${install_main_dir} || :
  ${csudo}mkdir -p ${install_main_dir}
  ${csudo}mkdir -p ${install_main_dir}/cfg
  ${csudo}mkdir -p ${install_main_dir}/bin
  #  ${csudo}mkdir -p ${install_main_dir}/connector
  ${csudo}mkdir -p ${install_main_dir}/driver
  ${csudo}mkdir -p ${install_main_dir}/examples
  ${csudo}mkdir -p ${install_main_dir}/include
  #  ${csudo}mkdir -p ${install_main_dir}/init.d
  if [ "$verMode" == "cluster" ]; then
    ${csudo}mkdir -p ${install_main_dir}/share
  fi

  if [[ -e ${script_dir}/email ]]; then
    ${csudo}cp ${script_dir}/email ${install_main_dir}/ || :
  fi
}

function install_bin() {
  # Remove links
  ${csudo}rm -f ${bin_link_dir}/${clientName} || :
  ${csudo}rm -f ${bin_link_dir}/${serverName} || :
  ${csudo}rm -f ${bin_link_dir}/${udfdName} || :
  ${csudo}rm -f ${bin_link_dir}/${adapterName} || :
  ${csudo}rm -f ${bin_link_dir}/${uninstallScript} || :
  ${csudo}rm -f ${bin_link_dir}/${demoName} || :
  ${csudo}rm -f ${bin_link_dir}/${benchmarkName} || :
  ${csudo}rm -f ${bin_link_dir}/${dumpName} || :
  ${csudo}rm -f ${bin_link_dir}/${xname} || :
  ${csudo}rm -f ${bin_link_dir}/set_core || :
  ${csudo}rm -f ${bin_link_dir}/TDinsight.sh || :

  ${csudo}cp -r ${script_dir}/bin/* ${install_main_dir}/bin && ${csudo}chmod 0555 ${install_main_dir}/bin/*

  #Make link
  [ -x ${install_main_dir}/bin/${clientName} ] && ${csudo}ln -s ${install_main_dir}/bin/${clientName} ${bin_link_dir}/${clientName} || :
  [ -x ${install_main_dir}/bin/${serverName} ] && ${csudo}ln -s ${install_main_dir}/bin/${serverName} ${bin_link_dir}/${serverName} || :
  [ -x ${install_main_dir}/bin/${udfdName} ] && ${csudo}ln -s ${install_main_dir}/bin/${udfdName} ${bin_link_dir}/${udfdName} || :
  [ -x ${install_main_dir}/bin/${adapterName} ] && ${csudo}ln -s ${install_main_dir}/bin/${adapterName} ${bin_link_dir}/${adapterName} || :
  [ -x ${install_main_dir}/bin/${benchmarkName} ] && ${csudo}ln -sf ${install_main_dir}/bin/${benchmarkName} ${bin_link_dir}/${demoName} || :
  [ -x ${install_main_dir}/bin/${benchmarkName} ] && ${csudo}ln -sf ${install_main_dir}/bin/${benchmarkName} ${bin_link_dir}/${benchmarkName} || :
  [ -x ${install_main_dir}/bin/${dumpName} ] && ${csudo}ln -s ${install_main_dir}/bin/${dumpName} ${bin_link_dir}/${dumpName} || :
  [ -x ${install_main_dir}/bin/${xname} ] && ${csudo}ln -s ${install_main_dir}/bin/${xname} ${bin_link_dir}/${xname} || :
  [ -x ${install_main_dir}/bin/TDinsight.sh ] && ${csudo}ln -s ${install_main_dir}/bin/TDinsight.sh ${bin_link_dir}/TDinsight.sh || :
  [ -x ${install_main_dir}/bin/remove.sh ] && ${csudo}ln -s ${install_main_dir}/bin/remove.sh ${bin_link_dir}/${uninstallScript} || :
  [ -x ${install_main_dir}/bin/set_core.sh ] && ${csudo}ln -s ${install_main_dir}/bin/set_core.sh ${bin_link_dir}/set_core || :
}

function install_lib() {
  # Remove links
  ${csudo}rm -f ${lib_link_dir}/libtaos.* || :
  ${csudo}rm -f ${lib64_link_dir}/libtaos.* || :
  #${csudo}rm -rf ${v15_java_app_dir}              || :
  ${csudo}cp -rf ${script_dir}/driver/* ${install_main_dir}/driver && ${csudo}chmod 777 ${install_main_dir}/driver/*

  ${csudo}ln -s ${install_main_dir}/driver/libtaos.* ${lib_link_dir}/libtaos.so.1
  ${csudo}ln -s ${lib_link_dir}/libtaos.so.1 ${lib_link_dir}/libtaos.so

  [ -f ${install_main_dir}/driver/libtaosws.so ] && ${csudo}ln -sf ${install_main_dir}/driver/libtaosws.so ${lib_link_dir}/libtaosws.so || :

  if [[ -d ${lib64_link_dir} && ! -e ${lib64_link_dir}/libtaos.so ]]; then
    ${csudo}ln -s ${install_main_dir}/driver/libtaos.* ${lib64_link_dir}/libtaos.so.1 || :
    ${csudo}ln -s ${lib64_link_dir}/libtaos.so.1 ${lib64_link_dir}/libtaos.so || :

    [ -f ${install_main_dir}/libtaosws.so ] && ${csudo}ln -sf ${install_main_dir}/libtaosws.so ${lib64_link_dir}/libtaosws.so || :
  fi

  ${csudo}ldconfig
}

function install_avro() {
  if [ "$osType" != "Darwin" ]; then
    avro_dir=${script_dir}/avro
    if [ -f "${avro_dir}/lib/libavro.so.23.0.0" ] && [ -d /usr/local/$1 ]; then
      ${csudo}/usr/bin/install -c -d /usr/local/$1
      ${csudo}/usr/bin/install -c -m 755 ${avro_dir}/lib/libavro.so.23.0.0 /usr/local/$1
      ${csudo}ln -sf /usr/local/$1/libavro.so.23.0.0 /usr/local/$1/libavro.so.23
      ${csudo}ln -sf /usr/local/$1/libavro.so.23 /usr/local/$1/libavro.so

      ${csudo}/usr/bin/install -c -d /usr/local/$1
      [ -f ${avro_dir}/lib/libavro.a ] &&
        ${csudo}/usr/bin/install -c -m 755 ${avro_dir}/lib/libavro.a /usr/local/$1

      if [ -d /etc/ld.so.conf.d ]; then
        echo "/usr/local/$1" | ${csudo}tee /etc/ld.so.conf.d/libavro.conf >/dev/null || echo -e "failed to write /etc/ld.so.conf.d/libavro.conf"
        ${csudo}ldconfig
      else
        echo "/etc/ld.so.conf.d not found!"
      fi
    fi
  fi
}

function install_jemalloc() {
  jemalloc_dir=${script_dir}/jemalloc

  if [ -d ${jemalloc_dir} ]; then
    ${csudo}/usr/bin/install -c -d /usr/local/bin

    if [ -f ${jemalloc_dir}/bin/jemalloc-config ]; then
      ${csudo}/usr/bin/install -c -m 755 ${jemalloc_dir}/bin/jemalloc-config /usr/local/bin
    fi
    if [ -f ${jemalloc_dir}/bin/jemalloc.sh ]; then
      ${csudo}/usr/bin/install -c -m 755 ${jemalloc_dir}/bin/jemalloc.sh /usr/local/bin
    fi
    if [ -f ${jemalloc_dir}/bin/jeprof ]; then
      ${csudo}/usr/bin/install -c -m 755 ${jemalloc_dir}/bin/jeprof /usr/local/bin
    fi
    if [ -f ${jemalloc_dir}/include/jemalloc/jemalloc.h ]; then
      ${csudo}/usr/bin/install -c -d /usr/local/include/jemalloc
      ${csudo}/usr/bin/install -c -m 644 ${jemalloc_dir}/include/jemalloc/jemalloc.h /usr/local/include/jemalloc
    fi
    if [ -f ${jemalloc_dir}/lib/libjemalloc.so.2 ]; then
      ${csudo}/usr/bin/install -c -d /usr/local/lib
      ${csudo}/usr/bin/install -c -m 755 ${jemalloc_dir}/lib/libjemalloc.so.2 /usr/local/lib
      ${csudo}ln -sf libjemalloc.so.2 /usr/local/lib/libjemalloc.so
      ${csudo}/usr/bin/install -c -d /usr/local/lib
      if [ -f ${jemalloc_dir}/lib/libjemalloc.a ]; then
        ${csudo}/usr/bin/install -c -m 755 ${jemalloc_dir}/lib/libjemalloc.a /usr/local/lib
      fi
      if [ -f ${jemalloc_dir}/lib/libjemalloc_pic.a ]; then
        ${csudo}/usr/bin/install -c -m 755 ${jemalloc_dir}/lib/libjemalloc_pic.a /usr/local/lib
      fi
      if [ -f ${jemalloc_dir}/lib/libjemalloc_pic.a ]; then
        ${csudo}/usr/bin/install -c -d /usr/local/lib/pkgconfig
        ${csudo}/usr/bin/install -c -m 644 ${jemalloc_dir}/lib/pkgconfig/jemalloc.pc /usr/local/lib/pkgconfig
      fi
    fi
    if [ -f ${jemalloc_dir}/share/doc/jemalloc/jemalloc.html ]; then
      ${csudo}/usr/bin/install -c -d /usr/local/share/doc/jemalloc
      ${csudo}/usr/bin/install -c -m 644 ${jemalloc_dir}/share/doc/jemalloc/jemalloc.html /usr/local/share/doc/jemalloc
    fi
    if [ -f ${jemalloc_dir}/share/man/man3/jemalloc.3 ]; then
      ${csudo}/usr/bin/install -c -d /usr/local/share/man/man3
      ${csudo}/usr/bin/install -c -m 644 ${jemalloc_dir}/share/man/man3/jemalloc.3 /usr/local/share/man/man3
    fi

    if [ -d /etc/ld.so.conf.d ]; then
      echo "/usr/local/lib" | ${csudo}tee /etc/ld.so.conf.d/jemalloc.conf >/dev/null || echo -e "failed to write /etc/ld.so.conf.d/jemalloc.conf"
      ${csudo}ldconfig
    else
      echo "/etc/ld.so.conf.d not found!"
    fi
  fi
}

function install_header() {
  ${csudo}rm -f ${inc_link_dir}/taos.h ${inc_link_dir}/taosdef.h ${inc_link_dir}/taoserror.h ${inc_link_dir}/taosudf.h || :

  [ -f ${inc_link_dir}/taosws.h ] && ${csudo}rm -f ${inc_link_dir}/taosws.h || :

  ${csudo}cp -f ${script_dir}/inc/* ${install_main_dir}/include && ${csudo}chmod 644 ${install_main_dir}/include/*
  ${csudo}ln -s ${install_main_dir}/include/taos.h ${inc_link_dir}/taos.h
  ${csudo}ln -s ${install_main_dir}/include/taosdef.h ${inc_link_dir}/taosdef.h
  ${csudo}ln -s ${install_main_dir}/include/taoserror.h ${inc_link_dir}/taoserror.h
  ${csudo}ln -s ${install_main_dir}/include/taosudf.h ${inc_link_dir}/taosudf.h  

  [ -f ${install_main_dir}/include/taosws.h ] && ${csudo}ln -sf ${install_main_dir}/include/taosws.h ${inc_link_dir}/taosws.h || :
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
  ${csudo}echo "127.0.0.1  $1" >>/etc/hosts || :
}

function set_hostname() {
  echo -e -n "${GREEN}Please enter one hostname(must not be 'localhost')${NC}:"
  read newHostname
  while true; do
    if [[ ! -z "$newHostname" && "$newHostname" != "localhost" ]]; then
      break
    else
      read -p "Please enter one hostname(must not be 'localhost'):" newHostname
    fi
  done

  ${csudo}hostname $newHostname || :
  retval=$(echo $?)
  if [[ $retval != 0 ]]; then
    echo
    echo "set hostname fail!"
    return
  fi

  #ubuntu/centos /etc/hostname
  if [[ -e /etc/hostname ]]; then
    ${csudo}echo $newHostname >/etc/hostname || :
  fi

  #debian: #HOSTNAME=yourname
  if [[ -e /etc/sysconfig/network ]]; then
    ${csudo}sed -i -r "s/#*\s*(HOSTNAME=\s*).*/\1$newHostname/" /etc/sysconfig/network || :
  fi

  ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$newHostname/" ${cfg_install_dir}/${configFile}
  serverFqdn=$newHostname

  if [[ -e /etc/hosts ]]; then
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
    ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$localFqdn/" ${cfg_install_dir}/${configFile}
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
        ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$localFqdn/" ${cfg_install_dir}/${configFile}
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
  if [[ "$serverFqdn" == "" ]] || [[ "$serverFqdn" == "localhost" ]]; then
    echo -e -n "${GREEN}It is strongly recommended to configure a hostname for this machine ${NC}"
    echo

    while true; do
      read -r -p "Set hostname now? [Y/n] " input
      if [ ! -n "$input" ]; then
        set_hostname
        break
      else
        case $input in
        [yY][eE][sS] | [yY])
          set_hostname
          break
          ;;

        [nN][oO] | [nN])
          set_ipAsFqdn
          break
          ;;

        *)
          echo "Invalid input..."
          ;;
        esac
      fi
    done
  fi
}

function install_adapter_config() {
  if [ ! -f "${cfg_install_dir}/${adapterName}.toml" ]; then
    ${csudo}mkdir -p ${cfg_install_dir}
    [ -f ${script_dir}/cfg/${adapterName}.toml ] && ${csudo}cp ${script_dir}/cfg/${adapterName}.toml ${cfg_install_dir}
    [ -f ${cfg_install_dir}/${adapterName}.toml ] && ${csudo}chmod 644 ${cfg_install_dir}/${adapterName}.toml
  else
    [ -f ${script_dir}/cfg/${adapterName}.toml ] &&
      ${csudo}cp -f ${script_dir}/cfg/${adapterName}.toml ${cfg_install_dir}/${adapterName}.toml.new
  fi

  [ -f ${cfg_install_dir}/${adapterName}.toml ] &&
    ${csudo}ln -s ${cfg_install_dir}/${adapterName}.toml ${install_main_dir}/cfg/${adapterName}.toml

  [ ! -z $1 ] && return 0 || : # only install client

}

function install_config() {

  if [ ! -f "${cfg_install_dir}/${configFile}" ]; then
    ${csudo}mkdir -p ${cfg_install_dir}
    [ -f ${script_dir}/cfg/${configFile} ] && ${csudo}cp ${script_dir}/cfg/${configFile} ${cfg_install_dir}
    ${csudo}chmod 644 ${cfg_install_dir}/*
  else
    ${csudo}cp -f ${script_dir}/cfg/${configFile} ${cfg_install_dir}/${configFile}.new
  fi

  ${csudo}ln -s ${cfg_install_dir}/${configFile} ${install_main_dir}/cfg

  [ ! -z $1 ] && return 0 || : # only install client

  if ((${update_flag} == 1)); then
    return 0
  fi

  if [ "$interactiveFqdn" == "no" ]; then
    return 0
  fi

  local_fqdn_check

  echo
  echo -e -n "${GREEN}Enter FQDN:port (like h1.${emailName}:6030) of an existing ${productName} cluster node to join${NC}"
  echo
  echo -e -n "${GREEN}OR leave it blank to build one${NC}:"
  read firstEp
  while true; do
    if [ ! -z "$firstEp" ]; then
      ${csudo}sed -i -r "s/#*\s*(firstEp\s*).*/\1$firstEp/" ${cfg_install_dir}/${configFile}
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
  ${csudo}rm -rf ${log_dir} || :
  ${csudo}mkdir -p ${log_dir} && ${csudo}chmod 777 ${log_dir}

  ${csudo}ln -s ${log_dir} ${install_main_dir}/log
}

function install_data() {
  ${csudo}mkdir -p ${data_dir}

  ${csudo}ln -s ${data_dir} ${install_main_dir}/data
}

function install_connector() {
  [ -d "${script_dir}/connector/" ] && ${csudo}cp -rf ${script_dir}/connector/ ${install_main_dir}/
}

function install_examples() {
  if [ -d ${script_dir}/examples ]; then
    ${csudo}cp -rf ${script_dir}/examples/* ${install_main_dir}/examples
  fi
}

function install_web() {
  if [ -d "${script_dir}/share" ]; then
    ${csudo}cp -rf ${script_dir}/share/* ${install_main_dir}/share
  fi
}


function clean_service_on_sysvinit() {
  if ps aux | grep -v grep | grep ${serverName} &>/dev/null; then
    ${csudo}service ${serverName} stop || :
  fi

  if ps aux | grep -v grep | grep tarbitrator &>/dev/null; then
    ${csudo}service tarbitratord stop || :
  fi

  if ((${initd_mod} == 1)); then
    if [ -e ${service_config_dir}/${serverName} ]; then
      ${csudo}chkconfig --del ${serverName} || :
    fi

    if [ -e ${service_config_dir}/tarbitratord ]; then
      ${csudo}chkconfig --del tarbitratord || :
    fi
  elif ((${initd_mod} == 2)); then
    if [ -e ${service_config_dir}/${serverName} ]; then
      ${csudo}insserv -r ${serverName} || :
    fi
    if [ -e ${service_config_dir}/tarbitratord ]; then
      ${csudo}insserv -r tarbitratord || :
    fi
  elif ((${initd_mod} == 3)); then
    if [ -e ${service_config_dir}/${serverName} ]; then
      ${csudo}update-rc.d -f ${serverName} remove || :
    fi
    if [ -e ${service_config_dir}/tarbitratord ]; then
      ${csudo}update-rc.d -f tarbitratord remove || :
    fi
  fi

  ${csudo}rm -f ${service_config_dir}/${serverName} || :
  ${csudo}rm -f ${service_config_dir}/tarbitratord || :

  if $(which init &>/dev/null); then
    ${csudo}init q || :
  fi
}

function install_service_on_sysvinit() {
  clean_service_on_sysvinit
  sleep 1

  if ((${os_type} == 1)); then
    #    ${csudo}cp -f ${script_dir}/init.d/${serverName}.deb ${install_main_dir}/init.d/${serverName}
    ${csudo}cp ${script_dir}/init.d/${serverName}.deb ${service_config_dir}/${serverName} && ${csudo}chmod a+x ${service_config_dir}/${serverName}
  elif ((${os_type} == 2)); then
    #    ${csudo}cp -f ${script_dir}/init.d/${serverName}.rpm ${install_main_dir}/init.d/${serverName}
    ${csudo}cp ${script_dir}/init.d/${serverName}.rpm ${service_config_dir}/${serverName} && ${csudo}chmod a+x ${service_config_dir}/${serverName}
  fi

  if ((${initd_mod} == 1)); then
    ${csudo}chkconfig --add ${serverName} || :
    ${csudo}chkconfig --level 2345 ${serverName} on || :
  elif ((${initd_mod} == 2)); then
    ${csudo}insserv ${serverName} || :
    ${csudo}insserv -d ${serverName} || :
  elif ((${initd_mod} == 3)); then
    ${csudo}update-rc.d ${serverName} defaults || :
  fi
}

function clean_service_on_systemd() {
  taosd_service_config="${service_config_dir}/${serverName}.service"
  if systemctl is-active --quiet ${serverName}; then
    echo "${productName} is running, stopping it..."
    ${csudo}systemctl stop ${serverName} &>/dev/null || echo &>/dev/null
  fi
  ${csudo}systemctl disable ${serverName} &>/dev/null || echo &>/dev/null
  ${csudo}rm -f ${taosd_service_config}

  tarbitratord_service_config="${service_config_dir}/tarbitratord.service"
  if systemctl is-active --quiet tarbitratord; then
    echo "tarbitrator is running, stopping it..."
    ${csudo}systemctl stop tarbitratord &>/dev/null || echo &>/dev/null
  fi
  ${csudo}systemctl disable tarbitratord &>/dev/null || echo &>/dev/null
  ${csudo}rm -f ${tarbitratord_service_config}
}

function install_service_on_systemd() {
  clean_service_on_systemd

  [ -f ${script_dir}/cfg/${serverName}.service ] &&
    ${csudo}cp ${script_dir}/cfg/${serverName}.service \
      ${service_config_dir}/ || :
  ${csudo}systemctl daemon-reload

  ${csudo}systemctl enable ${serverName}

  ${csudo}systemctl daemon-reload
}

function install_adapter_service() {
  if ((${service_mod} == 0)); then
    [ -f ${script_dir}/cfg/${adapterName}.service ] &&
      ${csudo}cp ${script_dir}/cfg/${adapterName}.service \
        ${service_config_dir}/ || :
    ${csudo}systemctl daemon-reload
  fi
}

function install_service() {
  if ((${service_mod} == 0)); then
    install_service_on_systemd
  elif ((${service_mod} == 1)); then
    install_service_on_sysvinit
  else
    kill_process ${serverName}
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
    min_compatible_version=$(${script_dir}/bin/${serverName} -V | head -1 | cut -d ' ' -f 5)
  fi

  exist_version=$(${installDir}/bin/${serverName} -V | head -1 | cut -d ' ' -f 3)
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
      ${csudo}dpkg --remove tdengine ||:
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
      ${csudo}rpm -e tdengine ||:
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
    rpm -q tdengine 2>&1 > /dev/null && rpm_erase tdengine ||:
  elif echo $osinfo | grep -qwi "ubuntu"; then
    dpkg -l tdengine 2>&1 | grep ii > /dev/null && deb_erase tdengine ||:
  fi

  tar -zxf ${tarName}
  install_jemalloc

  echo -e "${GREEN}Start to update ${productName}...${NC}"
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
  install_header
  install_lib

  if [ "$verMode" == "cluster" ]; then
      install_connector
  fi

  install_examples
  install_web
  if [ -z $1 ]; then
    install_bin
    install_service
    install_adapter_service
    install_config
    install_adapter_config

    openresty_work=false

    echo
    echo -e "${GREEN_DARK}To configure ${productName} ${NC}: edit ${cfg_install_dir}/${configFile}"
    [ -f ${configDir}/taosadapter.toml ] && [ -f ${installDir}/bin/taosadapter ] && \
      echo -e "${GREEN_DARK}To configure Taos Adapter ${NC}: edit ${configDir}/taosadapter.toml"
    if ((${service_mod} == 0)); then
      echo -e "${GREEN_DARK}To start ${productName}     ${NC}: ${csudo}systemctl start ${serverName}${NC}"
      [ -f ${service_config_dir}/taosadapter.service ] && [ -f ${installDir}/bin/taosadapter ] && \
        echo -e "${GREEN_DARK}To start Taos Adatper ${NC}: ${csudo}systemctl start taosadapter ${NC}"
    elif ((${service_mod} == 1)); then
      echo -e "${GREEN_DARK}To start ${productName}     ${NC}: ${csudo}service ${serverName} start${NC}"
      [ -f ${service_config_dir}/taosadapter.service ] && [ -f ${installDir}/bin/taosadapter ] && \
        echo -e "${GREEN_DARK}To start Taos Adapter ${NC}: ${csudo}service taosadapter start${NC}"
    else
      echo -e "${GREEN_DARK}To start ${productName}     ${NC}: ./${serverName}${NC}"
      [ -f ${installDir}/bin/taosadapter ] && \
        echo -e "${GREEN_DARK}To start Taos Adapter ${NC}: taosadapter &${NC}"
    fi

    if [ ${openresty_work} = 'true' ]; then
      echo -e "${GREEN_DARK}To access ${productName}    ${NC}: use ${GREEN_UNDERLINE}${clientName} -h $serverFqdn${NC} in shell OR from ${GREEN_UNDERLINE}http://127.0.0.1:${web_port}${NC}"
    else
      echo -e "${GREEN_DARK}To access ${productName}    ${NC}: use ${GREEN_UNDERLINE}${clientName} -h $serverFqdn${NC} in shell${NC}"
    fi

    if ((${prompt_force} == 1)); then
      echo ""
      echo -e "${RED}Please run '${serverName} --force-keep-file' at first time for the exist ${productName} $exist_version!${NC}"
    fi
    echo
    echo -e "\033[44;32;1m${productName} is updated successfully!${NC}"
  else
    install_bin
    install_config

    echo
    echo -e "\033[44;32;1m${productName} client is updated successfully!${NC}"
  fi

  rm -rf $(tar -tf ${tarName} | grep -v "^\./$")
}

function installProduct() {
  # Start to install
  if [ ! -e ${tarName} ]; then
    echo "File ${tarName} does not exist"
    exit 1
  fi
  tar -zxf ${tarName}

  echo -e "${GREEN}Start to install ${productName}...${NC}"

  install_main_path

  if [ -z $1 ]; then
    install_data
  fi

  install_log
  install_header
  install_lib
  install_jemalloc
  #install_avro lib
  #install_avro lib64

  if [ "$verMode" == "cluster" ]; then
      install_connector
  fi
  install_examples
  install_web

  if [ -z $1 ]; then # install service and client
    # For installing new
    install_bin
    install_service
    install_adapter_service
    install_adapter_config

    openresty_work=false

    install_config

    # Ask if to start the service
    echo
    echo -e "${GREEN_DARK}To configure ${productName} ${NC}: edit ${cfg_install_dir}/${configFile}"
    [ -f ${configDir}/taosadapter.toml ] && [ -f ${installDir}/bin/taosadapter ] && \
      echo -e "${GREEN_DARK}To configure Taos Adapter ${NC}: edit ${configDir}/taosadapter.toml"
    if ((${service_mod} == 0)); then
      echo -e "${GREEN_DARK}To start ${productName}     ${NC}: ${csudo}systemctl start ${serverName}${NC}"
      [ -f ${service_config_dir}/taosadapter.service ] && [ -f ${installDir}/bin/taosadapter ] && \
        echo -e "${GREEN_DARK}To start Taos Adatper ${NC}: ${csudo}systemctl start taosadapter ${NC}"
    elif ((${service_mod} == 1)); then
      echo -e "${GREEN_DARK}To start ${productName}     ${NC}: ${csudo}service ${serverName} start${NC}"
      [ -f ${service_config_dir}/taosadapter.service ] && [ -f ${installDir}/bin/taosadapter ] && \
        echo -e "${GREEN_DARK}To start Taos Adapter ${NC}: ${csudo}service taosadapter start${NC}"
    else
      echo -e "${GREEN_DARK}To start ${productName}     ${NC}: ${serverName}${NC}"
      [ -f ${installDir}/bin/taosadapter ] && \
        echo -e "${GREEN_DARK}To start Taos Adapter ${NC}: taosadapter &${NC}"
    fi

    if [ ! -z "$firstEp" ]; then
      tmpFqdn=${firstEp%%:*}
      substr=":"
      if [[ $firstEp =~ $substr ]]; then
        tmpPort=${firstEp#*:}
      else
        tmpPort=""
      fi
      if [[ "$tmpPort" != "" ]]; then
        echo -e "${GREEN_DARK}To access ${productName}    ${NC}: ${clientName} -h $tmpFqdn -P $tmpPort${GREEN_DARK} to login into cluster, then${NC}"
      else
        echo -e "${GREEN_DARK}To access ${productName}    ${NC}: ${clientName} -h $tmpFqdn${GREEN_DARK} to login into cluster, then${NC}"
      fi
      echo -e "${GREEN_DARK}execute ${NC}: create dnode 'newDnodeFQDN:port'; ${GREEN_DARK}to add this new node${NC}"
      echo
    elif [ ! -z "$serverFqdn" ]; then
      echo -e "${GREEN_DARK}To access ${productName}    ${NC}: ${clientName} -h $serverFqdn${GREEN_DARK} to login into ${productName} server${NC}"
      echo
    fi

    echo -e "\033[44;32;1m${productName} is installed successfully!${NC}"
    echo
  else # Only install client
    install_bin
    install_config
    echo
    echo -e "\033[44;32;1m${productName} client is installed successfully!${NC}"
  fi

  touch ~/.${historyFile}
  rm -rf $(tar -tf ${tarName} | grep -v "^\./$")
}

## ==============================Main program starts from here============================
serverFqdn=$(hostname)
if [ "$verType" == "server" ]; then
  # Check default 2.x data file.
  if [ -x ${data_dir}/dnode/dnodeCfg.json ]; then
    echo -e "\033[44;31;5mThe default data directory ${data_dir} contains old data of tdengine 2.x, please clear it before installing!\033[0m"
  else
    # Install server and client
    if [ -x ${bin_dir}/${serverName} ]; then
      update_flag=1
      updateProduct
    else
      installProduct
    fi
  fi
elif [ "$verType" == "client" ]; then
  interactiveFqdn=no
  # Only install client
  if [ -x ${bin_dir}/${clientName} ]; then
    update_flag=1
    updateProduct client
  else
    installProduct client
  fi
else
  echo "please input correct verType"
fi
