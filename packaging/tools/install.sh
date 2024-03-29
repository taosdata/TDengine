#!/bin/bash
#
# This file is used to install database on linux systems. The operating system
# is required to use systemd to manage services at boot

set -e
# set -x

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
tarName="package.tar.gz"
dataDir="/var/lib/taos"
logDir="/var/log/taos"
configDir="/etc/taos"
installDir="/usr/local/taos"
adapterName="taosadapter"
benchmarkName="taosBenchmark"
dumpName="taosdump"
demoName="taosdemo"
xname="taosx"
keeperName="taoskeeper"

clientName2="taos"
serverName2="${clientName2}d"
configFile2="${clientName2}.cfg"
productName2="TDengine"
emailName2="taosdata.com"
xname2="${clientName2}x"
adapterName2="${clientName2}adapter"
keeperName2="${clientName2}keeper"

explorerName="${clientName2}-explorer"
benchmarkName2="${clientName2}Benchmark"
demoName2="${clientName2}demo"
dumpName2="${clientName2}dump"
uninstallScript2="rm${clientName2}"

historyFile="${clientName2}_history"
logDir="/var/log/${clientName2}"
configDir="/etc/${clientName2}"
installDir="/usr/local/${clientName2}"

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
  ${csudo}rm -f ${bin_link_dir}/${clientName2} || :
  ${csudo}rm -f ${bin_link_dir}/${serverName2} || :
  ${csudo}rm -f ${bin_link_dir}/${udfdName} || :
  ${csudo}rm -f ${bin_link_dir}/${adapterName} || :
  ${csudo}rm -f ${bin_link_dir}/${uninstallScript2} || :
  ${csudo}rm -f ${bin_link_dir}/${demoName2} || :
  ${csudo}rm -f ${bin_link_dir}/${benchmarkName2} || :
  ${csudo}rm -f ${bin_link_dir}/${dumpName2} || :
  ${csudo}rm -f ${bin_link_dir}/${keeperName2} || :
  ${csudo}rm -f ${bin_link_dir}/set_core || :
  ${csudo}rm -f ${bin_link_dir}/TDinsight.sh || :

  ${csudo}cp -r ${script_dir}/bin/* ${install_main_dir}/bin && ${csudo}chmod 0555 ${install_main_dir}/bin/*

  #Make link
  [ -x ${install_main_dir}/bin/${clientName2} ] && ${csudo}ln -sf ${install_main_dir}/bin/${clientName2} ${bin_link_dir}/${clientName2} || :
  [ -x ${install_main_dir}/bin/${serverName2} ] && ${csudo}ln -sf ${install_main_dir}/bin/${serverName2} ${bin_link_dir}/${serverName2} || :
  [ -x ${install_main_dir}/bin/${udfdName} ] && ${csudo}ln -sf ${install_main_dir}/bin/${udfdName} ${bin_link_dir}/${udfdName} || :
  [ -x ${install_main_dir}/bin/${adapterName2} ] && ${csudo}ln -sf ${install_main_dir}/bin/${adapterName2} ${bin_link_dir}/${adapterName2} || :
  [ -x ${install_main_dir}/bin/${benchmarkName2} ] && ${csudo}ln -sf ${install_main_dir}/bin/${benchmarkName2} ${bin_link_dir}/${demoName2} || :
  [ -x ${install_main_dir}/bin/${benchmarkName2} ] && ${csudo}ln -sf ${install_main_dir}/bin/${benchmarkName2} ${bin_link_dir}/${benchmarkName2} || :
  [ -x ${install_main_dir}/bin/${dumpName2} ] && ${csudo}ln -sf ${install_main_dir}/bin/${dumpName2} ${bin_link_dir}/${dumpName2} || :
  [ -x ${install_main_dir}/bin/${keeperName2} ] && ${csudo}ln -sf ${install_main_dir}/bin/${keeperName2} ${bin_link_dir}/${keeperName2} || :
  [ -x ${install_main_dir}/bin/TDinsight.sh ] && ${csudo}ln -sf ${install_main_dir}/bin/TDinsight.sh ${bin_link_dir}/TDinsight.sh || :
  if [ "$clientName2" == "${clientName}" ]; then
    [ -x ${install_main_dir}/bin/remove.sh ] && ${csudo}ln -s ${install_main_dir}/bin/remove.sh ${bin_link_dir}/${uninstallScript} || :
  fi
  [ -x ${install_main_dir}/bin/set_core.sh ] && ${csudo}ln -s ${install_main_dir}/bin/set_core.sh ${bin_link_dir}/set_core || :

  if [ "$verMode" == "cluster" ] && [ "$clientName" != "$clientName2" ]; then
    ${csudo}rm -f ${bin_link_dir}/${xname2} || :
    ${csudo}rm -f ${bin_link_dir}/${explorerName} || :

    #Make link
    [ -x ${install_main_dir}/bin/${xname2} ] && ${csudo}ln -sf ${install_main_dir}/bin/${xname2} ${bin_link_dir}/${xname2} || :
    [ -x ${install_main_dir}/bin/${explorerName} ] && ${csudo}ln -sf ${install_main_dir}/bin/${explorerName} ${bin_link_dir}/${explorerName} || :
    [ -x ${install_main_dir}/bin/remove.sh ] && ${csudo}ln -s ${install_main_dir}/bin/remove.sh ${bin_link_dir}/${uninstallScript2} || :
  fi
}

function install_lib() {
  # Remove links
  ${csudo}rm -f ${lib_link_dir}/libtaos.* || :
  ${csudo}rm -f ${lib64_link_dir}/libtaos.* || :
  #${csudo}rm -rf ${v15_java_app_dir}              || :
  ${csudo}cp -rf ${script_dir}/driver/* ${install_main_dir}/driver && ${csudo}chmod 777 ${install_main_dir}/driver/*

  ${csudo}ln -sf ${install_main_dir}/driver/libtaos.* ${lib_link_dir}/libtaos.so.1
  ${csudo}ln -sf ${lib_link_dir}/libtaos.so.1 ${lib_link_dir}/libtaos.so

  [ -f ${install_main_dir}/driver/libtaosws.so ] && ${csudo}ln -sf ${install_main_dir}/driver/libtaosws.so ${lib_link_dir}/libtaosws.so || :

  if [[ -d ${lib64_link_dir} && ! -e ${lib64_link_dir}/libtaos.so ]]; then
    ${csudo}ln -sf ${install_main_dir}/driver/libtaos.* ${lib64_link_dir}/libtaos.so.1 || :
    ${csudo}ln -sf ${lib64_link_dir}/libtaos.so.1 ${lib64_link_dir}/libtaos.so || :

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
      # if [ -f ${jemalloc_dir}/lib/libjemalloc.a ]; then
      #   ${csudo}/usr/bin/install -c -m 755 ${jemalloc_dir}/lib/libjemalloc.a /usr/local/lib
      # fi
      # if [ -f ${jemalloc_dir}/lib/libjemalloc_pic.a ]; then
      #   ${csudo}/usr/bin/install -c -m 755 ${jemalloc_dir}/lib/libjemalloc_pic.a /usr/local/lib
      # fi
      if [ -f ${jemalloc_dir}/lib/pkgconfig/jemalloc.pc ]; then
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
  ${csudo}rm -f ${inc_link_dir}/taos.h ${inc_link_dir}/taosdef.h ${inc_link_dir}/taoserror.h ${inc_link_dir}/tdef.h ${inc_link_dir}/taosudf.h || :

  [ -f ${inc_link_dir}/taosws.h ] && ${csudo}rm -f ${inc_link_dir}/taosws.h || :

  ${csudo}cp -f ${script_dir}/inc/* ${install_main_dir}/include && ${csudo}chmod 644 ${install_main_dir}/include/*
  ${csudo}ln -sf ${install_main_dir}/include/taos.h ${inc_link_dir}/taos.h
  ${csudo}ln -sf ${install_main_dir}/include/taosdef.h ${inc_link_dir}/taosdef.h
  ${csudo}ln -sf ${install_main_dir}/include/taoserror.h ${inc_link_dir}/taoserror.h
  ${csudo}ln -sf ${install_main_dir}/include/tdef.h ${inc_link_dir}/tdef.h
  ${csudo}ln -sf ${install_main_dir}/include/taosudf.h ${inc_link_dir}/taosudf.h  

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

  # ${csudo}hostname $newHostname || :
  # retval=$(echo $?)
  # if [[ $retval != 0 ]]; then
  #   echo
  #   echo "set hostname fail!"
  #   return
  # fi

  # #ubuntu/centos /etc/hostname
  # if [[ -e /etc/hostname ]]; then
  #   ${csudo}echo $newHostname >/etc/hostname || :
  # fi

  # #debian: #HOSTNAME=yourname
  # if [[ -e /etc/sysconfig/network ]]; then
  #   ${csudo}sed -i -r "s/#*\s*(HOSTNAME=\s*).*/\1$newHostname/" /etc/sysconfig/network || :
  # fi

  if [ -f ${cfg_install_dir}/${configFile2} ]; then
    ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$newHostname/" ${cfg_install_dir}/${configFile2}
  else
    ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$newHostname/" ${script_dir}/cfg/${configFile2}
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
    
    if [ -f ${cfg_install_dir}/${configFile2} ]; then
      ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$localFqdn/" ${cfg_install_dir}/${configFile2}
    else
      ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$localFqdn/" ${script_dir}/cfg/${configFile2}
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
        if [ -f ${cfg_install_dir}/${configFile2} ]; then
          ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$localFqdn/" ${cfg_install_dir}/${configFile2}
        else
          ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$localFqdn/" ${script_dir}/cfg/${configFile2}
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

function install_adapter_config() {
  if [ -f ${script_dir}/cfg/${adapterName}.toml ]; then
    ${csudo}sed -i -r "s/localhost/${serverFqdn}/g" ${script_dir}/cfg/${adapterName}.toml
  fi
  if [ ! -f "${cfg_install_dir}/${adapterName}.toml" ]; then
    ${csudo}mkdir -p ${cfg_install_dir}
    [ -f ${script_dir}/cfg/${adapterName}.toml ] && ${csudo}cp ${script_dir}/cfg/${adapterName}.toml ${cfg_install_dir}
    [ -f ${cfg_install_dir}/${adapterName}.toml ] && ${csudo}chmod 644 ${cfg_install_dir}/${adapterName}.toml
  else
    [ -f ${script_dir}/cfg/${adapterName}.toml ] &&
      ${csudo}cp -f ${script_dir}/cfg/${adapterName}.toml ${cfg_install_dir}/${adapterName}.toml.new
  fi

  [ -f ${cfg_install_dir}/${adapterName}.toml ] &&
    ${csudo}ln -sf ${cfg_install_dir}/${adapterName}.toml ${install_main_dir}/cfg/${adapterName}.toml

  [ ! -z $1 ] && return 0 || : # only install client

}

function install_keeper_config() {
  if [ -f ${script_dir}/cfg/${keeperName2}.toml ]; then
    ${csudo}sed -i -r "s/127.0.0.1/${serverFqdn}/g" ${script_dir}/cfg/${keeperName2}.toml
  fi
  if [ -f "${configDir}/keeper.toml" ]; then
    echo "The file keeper.toml will be renamed to ${keeperName2}.toml"        
    ${csudo}cp ${script_dir}/cfg/${keeperName2}.toml ${configDir}/${keeperName2}.toml.new    
    ${csudo}mv ${configDir}/keeper.toml ${configDir}/${keeperName2}.toml
  elif [ -f "${configDir}/${keeperName2}.toml" ]; then
    # "taoskeeper.toml exists,new config is taoskeeper.toml.new"         
    ${csudo}cp ${script_dir}/cfg/${keeperName2}.toml ${configDir}/${keeperName2}.toml.new
  else    
    ${csudo}cp ${script_dir}/cfg/${keeperName2}.toml ${configDir}/${keeperName2}.toml
  fi
  command -v systemctl >/dev/null 2>&1 && ${csudo}systemctl daemon-reload >/dev/null 2>&1 || true  
}

function install_config() {  

  if [ ! -f "${cfg_install_dir}/${configFile2}" ]; then
    ${csudo}mkdir -p ${cfg_install_dir}
    if [ -f ${script_dir}/cfg/${configFile2} ]; then
      ${csudo} echo "monitor 1" >> ${script_dir}/cfg/${configFile2}
      ${csudo} echo "monitorFQDN ${serverFqdn}" >> ${script_dir}/cfg/${configFile2}
      ${csudo} echo "audit 1" >> ${script_dir}/cfg/${configFile2}
      ${csudo}cp ${script_dir}/cfg/${configFile2} ${cfg_install_dir}
    fi
    ${csudo}chmod 644 ${cfg_install_dir}/*
  else
    ${csudo} echo "monitor 1" >> ${script_dir}/cfg/${configFile2}
    ${csudo} echo "monitorFQDN ${serverFqdn}" >> ${script_dir}/cfg/${configFile2}
    ${csudo} echo "audit 1" >> ${script_dir}/cfg/${configFile2}
    ${csudo}cp -f ${script_dir}/cfg/${configFile2} ${cfg_install_dir}/${configFile2}.new
  fi

  ${csudo}ln -sf ${cfg_install_dir}/${configFile2} ${install_main_dir}/cfg

  [ ! -z $1 ] && return 0 || : # only install client

  

  if ((${update_flag} == 1)); then
    return 0
  fi

  if [ "$interactiveFqdn" == "no" ]; then
    return 0
  fi

  local_fqdn_check

  echo
  echo -e -n "${GREEN}Enter FQDN:port (like h1.${emailName2}:6030) of an existing ${productName2} cluster node to join${NC}"
  echo
  echo -e -n "${GREEN}OR leave it blank to build one${NC}:"
  read firstEp
  while true; do
    if [ ! -z "$firstEp" ]; then
      if [ -f ${cfg_install_dir}/${configFile2} ]; then
        ${csudo}sed -i -r "s/#*\s*(firstEp\s*).*/\1$firstEp/" ${cfg_install_dir}/${configFile2}
      else
        ${csudo}sed -i -r "s/#*\s*(firstEp\s*).*/\1$firstEp/" ${script_dir}/cfg/${configFile2}
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

function install_share_etc() {
  [ ! -d ${script_dir}/share/etc ] && return
  for c in `ls ${script_dir}/share/etc/`; do
    if [ -e /etc/${clientName2}/$c ]; then
      out=/etc/${clientName2}/$c.new.`date +%F`
      ${csudo}cp -f ${script_dir}/share/etc/$c $out ||:
    else
      ${csudo}mkdir -p /etc/${clientName2} >/dev/null 2>/dev/null ||:
      ${csudo}cp -f ${script_dir}/share/etc/$c /etc/${clientName2}/$c ||:
    fi
  done

  [ ! -d ${script_dir}/share/srv ] && return
  ${csudo} cp ${script_dir}/share/srv/* ${service_config_dir} ||:
}

function install_log() {  
  ${csudo}mkdir -p ${log_dir} && ${csudo}chmod 777 ${log_dir}

  ${csudo}ln -sf ${log_dir} ${install_main_dir}/log
}

function install_data() {
  ${csudo}mkdir -p ${data_dir}

  ${csudo}ln -sf ${data_dir} ${install_main_dir}/data
}

function install_connector() {
  if [ -d "${script_dir}/connector/" ]; then
    ${csudo}cp -rf ${script_dir}/connector/ ${install_main_dir}/ || echo "failed to copy connector"    
    ${csudo}cp ${script_dir}/start-all.sh ${install_main_dir}/ || echo "failed to copy start-all.sh"
    ${csudo}cp ${script_dir}/stop-all.sh ${install_main_dir}/ || echo "failed to copy stop-all.sh"
    ${csudo}cp ${script_dir}/README.md ${install_main_dir}/ || echo "failed to copy README.md"
  fi
}

function install_examples() {
  if [ -d ${script_dir}/examples ]; then
    ${csudo}cp -rf ${script_dir}/examples/* ${install_main_dir}/examples || echo "failed to copy examples"
  fi
}

function install_web() {
  if [ -d "${script_dir}/share" ]; then
    ${csudo}cp -rf ${script_dir}/share/* ${install_main_dir}/share > /dev/null 2>&1 ||:
  fi
}

function install_taosx() {
  if [ -f "${script_dir}/taosx/install_taosx.sh" ]; then    
    cd ${script_dir}/taosx
    chmod a+x install_taosx.sh
    bash install_taosx.sh -e $serverFqdn
  fi
}

function clean_service_on_sysvinit() {
  if ps aux | grep -v grep | grep ${serverName2} &>/dev/null; then
    ${csudo}service ${serverName2} stop || :
  fi

  if ps aux | grep -v grep | grep tarbitrator &>/dev/null; then
    ${csudo}service tarbitratord stop || :
  fi

  if ((${initd_mod} == 1)); then
    if [ -e ${service_config_dir}/${serverName2} ]; then
      ${csudo}chkconfig --del ${serverName2} || :
    fi

    if [ -e ${service_config_dir}/tarbitratord ]; then
      ${csudo}chkconfig --del tarbitratord || :
    fi
  elif ((${initd_mod} == 2)); then
    if [ -e ${service_config_dir}/${serverName2} ]; then
      ${csudo}insserv -r ${serverName2} || :
    fi
    if [ -e ${service_config_dir}/tarbitratord ]; then
      ${csudo}insserv -r tarbitratord || :
    fi
  elif ((${initd_mod} == 3)); then
    if [ -e ${service_config_dir}/${serverName2} ]; then
      ${csudo}update-rc.d -f ${serverName2} remove || :
    fi
    if [ -e ${service_config_dir}/tarbitratord ]; then
      ${csudo}update-rc.d -f tarbitratord remove || :
    fi
  fi

  ${csudo}rm -f ${service_config_dir}/${serverName2} || :
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
    ${csudo}chkconfig --add ${serverName2} || :
    ${csudo}chkconfig --level 2345 ${serverName2} on || :
  elif ((${initd_mod} == 2)); then
    ${csudo}insserv ${serverName2} || :
    ${csudo}insserv -d ${serverName2} || :
  elif ((${initd_mod} == 3)); then
    ${csudo}update-rc.d ${serverName2} defaults || :
  fi
}

function clean_service_on_systemd() {
  service_config="${service_config_dir}/${serverName2}.service"
  if systemctl is-active --quiet ${serverName2}; then
    echo "${productName} is running, stopping it..."
    ${csudo}systemctl stop ${serverName2} &>/dev/null || echo &>/dev/null
  fi
  ${csudo}systemctl disable ${serverName2} &>/dev/null || echo &>/dev/null
  ${csudo}rm -f ${service_config}

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

  install_share_etc

  [ -f ${script_dir}/cfg/${serverName2}.service ] &&
    ${csudo}cp ${script_dir}/cfg/${serverName2}.service \
      ${service_config_dir}/ || :

  # if [ "$verMode" == "cluster" ] && [ "$clientName" != "$clientName2" ]; then
  #   [ -f ${script_dir}/cfg/${serverName2}.service ] &&
  #   ${csudo}cp ${script_dir}/cfg/${serverName2}.service \
  #     ${service_config_dir}/${serverName2}.service || :
  # fi

  ${csudo}systemctl daemon-reload

  ${csudo}systemctl enable ${serverName2}
  ${csudo}systemctl daemon-reload
}

function install_adapter_service() {
  if ((${service_mod} == 0)); then
    [ -f ${script_dir}/cfg/${adapterName2}.service ] &&
      ${csudo}cp ${script_dir}/cfg/${adapterName2}.service \
        ${service_config_dir}/ || :
    
    ${csudo}systemctl enable ${adapterName2}
    ${csudo}systemctl daemon-reload
  fi
}

function install_keeper_service() {
  if ((${service_mod} == 0)); then
    [ -f ${script_dir}/cfg/${clientName2}keeper.service ] &&
      ${csudo}cp ${script_dir}/cfg/${clientName2}keeper.service \
        ${service_config_dir}/ || :
    
    ${csudo}systemctl enable ${clientName2}keeper
    ${csudo}systemctl daemon-reload
  fi
}

function install_service() {
  if ((${service_mod} == 0)); then
    install_service_on_systemd
  elif ((${service_mod} == 1)); then
    install_service_on_sysvinit
  else
    kill_process ${serverName2}
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
    min_compatible_version=$(${script_dir}/bin/${serverName2} -V | head -1 | cut -d ' ' -f 5)
  fi

  exist_version=$(${installDir}/bin/${serverName2} -V | head -1 | cut -d ' ' -f 3)
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

  echo "Start to update ${productName2}..."
  # Stop the service if running
  if ps aux | grep -v grep | grep ${serverName2} &>/dev/null; then
    if ((${service_mod} == 0)); then
      ${csudo}systemctl stop ${serverName2} || :
    elif ((${service_mod} == 1)); then
      ${csudo}service ${serverName2} stop || :
    else
      kill_process ${serverName2}
    fi
    sleep 1
  fi

  install_main_path

  install_log
  install_header
  install_lib
  install_config

  if [ "$verMode" == "cluster" ]; then
      install_connector
      install_taosx
  fi

  install_examples
  install_web
  if [ -z $1 ]; then
    install_bin
    install_service

    if [ "${pagMode}" != "lite" ]; then
      install_adapter_service
      install_adapter_config
      if [ "${verMode}" != "cloud" ]; then
        install_keeper_service
        install_keeper_config
      fi
    fi

    openresty_work=false

    echo
    echo -e "${GREEN_DARK}To configure ${productName2} ${NC}\t\t: edit ${cfg_install_dir}/${configFile2}"
    [ -f ${configDir}/${clientName2}adapter.toml ] && [ -f ${installDir}/bin/${clientName2}adapter ] && \
      echo -e "${GREEN_DARK}To configure ${clientName2}Adapter ${NC}\t: edit ${configDir}/${clientName2}adapter.toml"
    if [ "$verMode" == "cluster" ]; then
      echo -e "${GREEN_DARK}To configure ${clientName2}-explorer ${NC}\t: edit ${configDir}/explorer.toml"
    fi
    if ((${service_mod} == 0)); then
      echo -e "${GREEN_DARK}To start ${productName2}     ${NC}\t\t: ${csudo}systemctl start ${serverName2}${NC}"
      [ -f ${service_config_dir}/${clientName2}adapter.service ] && [ -f ${installDir}/bin/${clientName2}adapter ] && \
        echo -e "${GREEN_DARK}To start ${clientName2}Adapter ${NC}\t\t: ${csudo}systemctl start ${clientName2}adapter ${NC}"
    elif ((${service_mod} == 1)); then
      echo -e "${GREEN_DARK}To start ${productName2}     ${NC}\t\t: ${csudo}service ${serverName2} start${NC}"
      [ -f ${service_config_dir}/${clientName2}adapter.service ] && [ -f ${installDir}/bin/${clientName2}adapter ] && \
        echo -e "${GREEN_DARK}To start ${clientName2}Adapter ${NC}\t\t: ${csudo}service ${clientName2}adapter start${NC}"
    else
      echo -e "${GREEN_DARK}To start ${productName2}     ${NC}\t\t: ./${serverName2}${NC}"
      [ -f ${installDir}/bin/${clientName2}adapter ] && \
        echo -e "${GREEN_DARK}To start ${clientName2}Adapter ${NC}\t\t: ${clientName2}adapter ${NC}"
    fi
    
    echo -e "${GREEN_DARK}To enable ${clientName2}keeper ${NC}\t\t: sudo systemctl enable ${clientName2}keeper ${NC}"
    if [ "$verMode" == "cluster" ];then
      echo -e "${GREEN_DARK}To start ${clientName2}x ${NC}\t\t\t: sudo systemctl start ${clientName2}x ${NC}"
      echo -e "${GREEN_DARK}To start ${clientName2}-explorer ${NC}\t\t: sudo systemctl start ${clientName2}-explorer ${NC}"
    fi

    # if [ ${openresty_work} = 'true' ]; then
    #   echo -e "${GREEN_DARK}To access ${productName2}    ${NC}\t\t: use ${GREEN_UNDERLINE}${clientName2} -h $serverFqdn${NC} in shell OR from ${GREEN_UNDERLINE}http://127.0.0.1:${web_port}${NC}"
    # else
    #   echo -e "${GREEN_DARK}To access ${productName2}    ${NC}\t\t: use ${GREEN_UNDERLINE}${clientName2} -h $serverFqdn${NC} in shell${NC}"
    # fi

    # if ((${prompt_force} == 1)); then
    #   echo ""
    #   echo -e "${RED}Please run '${serverName2} --force-keep-file' at first time for the exist ${productName2} $exist_version!${NC}"
    # fi

    echo
    echo "${productName2} is updated successfully!"
    echo
    if [ "$verMode" == "cluster" ];then
      echo -e "\033[44;32;1mTo start all the components       : ./start-all.sh${NC}"
    fi
    echo -e "\033[44;32;1mTo access ${productName2}                : ${clientName2} -h $serverFqdn${NC}"
    if [ "$verMode" == "cluster" ];then
      echo -e "\033[44;32;1mTo access the management system   : http://$serverFqdn:6060${NC}"
      echo -e "\033[44;32;1mTo read the user manual           : http://$serverFqdn:6060/docs${NC}"
    fi
  else
    install_bin    

    echo
    echo -e "\033[44;32;1m${productName2} client is updated successfully!${NC}"
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

  echo "Start to install ${productName2}..."

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
  install_config

  if [ "$verMode" == "cluster" ]; then
      install_connector
      install_taosx    
  fi
  install_examples  
  install_web
  if [ -z $1 ]; then # install service and client
    # For installing new
    install_bin
    install_service

    if [ "${pagMode}" != "lite" ]; then
      install_adapter_service
      install_adapter_config
      if [ "${verMode}" != "cloud" ]; then
        install_keeper_service
        install_keeper_config
      fi
    fi

    openresty_work=false


    # Ask if to start the service
    echo
    echo -e "${GREEN_DARK}To configure ${productName2} ${NC}\t\t: edit ${cfg_install_dir}/${configFile2}"
    [ -f ${configDir}/${clientName2}adapter.toml ] && [ -f ${installDir}/bin/${clientName2}adapter ] && \
      echo -e "${GREEN_DARK}To configure ${clientName2}Adapter ${NC}\t: edit ${configDir}/${clientName2}adapter.toml"
    if [ "$verMode" == "cluster" ]; then
      echo -e "${GREEN_DARK}To configure ${clientName2}-explorer ${NC}\t: edit ${configDir}/explorer.toml"
    fi
    if ((${service_mod} == 0)); then
      echo -e "${GREEN_DARK}To start ${productName2}     ${NC}\t\t: ${csudo}systemctl start ${serverName2}${NC}"
      [ -f ${service_config_dir}/${clientName2}adapter.service ] && [ -f ${installDir}/bin/${clientName2}adapter ] && \
        echo -e "${GREEN_DARK}To start ${clientName2}Adapter ${NC}\t\t: ${csudo}systemctl start ${clientName2}adapter ${NC}"
    elif ((${service_mod} == 1)); then
      echo -e "${GREEN_DARK}To start ${productName2}     ${NC}\t\t: ${csudo}service ${serverName2} start${NC}"
      [ -f ${service_config_dir}/${clientName2}adapter.service ] && [ -f ${installDir}/bin/${clientName2}adapter ] && \
        echo -e "${GREEN_DARK}To start ${clientName2}Adapter ${NC}\t\t: ${csudo}service ${clientName2}adapter start${NC}"
    else
      echo -e "${GREEN_DARK}To start ${productName2}     ${NC}\t\t: ${serverName2}${NC}"
      [ -f ${installDir}/bin/${clientName2}adapter ] && \
        echo -e "${GREEN_DARK}To start ${clientName2}Adapter ${NC}\t\t: ${clientName2}adapter ${NC}"
    fi

    echo -e "${GREEN_DARK}To enable ${clientName2}keeper ${NC}\t\t: sudo systemctl enable ${clientName2}keeper ${NC}"
    
    if [ "$verMode" == "cluster" ];then
      echo -e "${GREEN_DARK}To start ${clientName2}x ${NC}\t\t\t: sudo systemctl start ${clientName2}x ${NC}"
      echo -e "${GREEN_DARK}To start ${clientName2}-explorer ${NC}\t\t: sudo systemctl start ${clientName2}-explorer ${NC}"
    fi

    # if [ ! -z "$firstEp" ]; then
    #   tmpFqdn=${firstEp%%:*}
    #   substr=":"
    #   if [[ $firstEp =~ $substr ]]; then
    #     tmpPort=${firstEp#*:}
    #   else
    #     tmpPort=""
    #   fi
    #   if [[ "$tmpPort" != "" ]]; then
    #     echo -e "${GREEN_DARK}To access ${productName2}    ${NC}\t\t: ${clientName2} -h $tmpFqdn -P $tmpPort${GREEN_DARK} to login into cluster, then${NC}"
    #   else
    #     echo -e "${GREEN_DARK}To access ${productName2}    ${NC}\t\t: ${clientName2} -h $tmpFqdn${GREEN_DARK} to login into cluster, then${NC}"
    #   fi
    #   echo -e "${GREEN_DARK}execute ${NC}: create dnode 'newDnodeFQDN:port'; ${GREEN_DARK}to add this new node${NC}"
    #   echo
    # elif [ ! -z "$serverFqdn" ]; then
    #   echo -e "${GREEN_DARK}To access ${productName2}    ${NC}\t\t: ${clientName2} -h $serverFqdn${GREEN_DARK} to login into ${productName2} server${NC}"
    #   echo
    # fi
    echo
    echo "${productName2} is installed successfully!"
    echo
    if [ "$verMode" == "cluster" ];then
      echo -e "\033[44;32;1mTo start all the components       : sudo ./start-all.sh${NC}"
    fi
    echo -e "\033[44;32;1mTo access ${productName2}                : ${clientName2} -h $serverFqdn${NC}"
    if [ "$verMode" == "cluster" ];then
      echo -e "\033[44;32;1mTo access the management system   : http://$serverFqdn:6060${NC}"
      echo -e "\033[44;32;1mTo read the user manual           : http://$serverFqdn:6060/docs-en${NC}"
    fi
    echo
  else # Only install client
    install_bin
        
    echo
    echo -e "\033[44;32;1m${productName2} client is installed successfully!${NC}"
  fi
  
  cd $script_dir
  touch ~/.${historyFile}
  rm -rf $(tar -tf ${tarName} | grep -Ev "^\./$|^\/")
}

## ==============================Main program starts from here============================
serverFqdn=$(hostname)
if [ "$verType" == "server" ]; then
  # Check default 2.x data file.
  if [ -x ${data_dir}/dnode/dnodeCfg.json ]; then
    echo -e "\033[44;31;5mThe default data directory ${data_dir} contains old data of ${productName2} 2.x, please clear it before installing!\033[0m"
  else
    # Install server and client
    if [ -x ${bin_dir}/${serverName2} ]; then
      update_flag=1
      updateProduct
    else
      installProduct
    fi
  fi
elif [ "$verType" == "client" ]; then
  interactiveFqdn=no
  # Only install client
  if [ -x ${bin_dir}/${clientName2} ]; then
    update_flag=1
    updateProduct client
  else
    installProduct client
  fi
else
  echo "please input correct verType"
fi


