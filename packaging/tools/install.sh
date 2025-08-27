#!/bin/bash
#
# This file is used to install database on linux systems. The operating system
# is required to use systemd to manage services at boot

set -e
# set -x

verMode=edge
pkgMode=full
entMode=full

iplist=""
serverFqdn=""
ostype=`uname`

# -----------------------Variables definition---------------------
script_dir=$(dirname $(readlink -f "$0"))
# Dynamic directory

PREFIX="taos"
clientName="${PREFIX}"
serverName="${PREFIX}d"
udfdName="${PREFIX}udf"
configFile="${PREFIX}.cfg"
productName="TDengine TSDB"
emailName="taosdata.com"
uninstallScript="rm${PREFIX}"
historyFile="${PREFIX}_history"
tarName="package.tar.gz"
dataDir="/var/lib/${PREFIX}"
logDir="/var/log/${PREFIX}"
configDir="/etc/${PREFIX}"
installDir="/usr/local/${PREFIX}"
adapterName="${PREFIX}adapter"
benchmarkName="${PREFIX}Benchmark"
dumpName="${PREFIX}dump"
demoName="${PREFIX}demo"
xname="${PREFIX}x"
explorerName="${PREFIX}-explorer"
keeperName="${PREFIX}keeper"
inspect_name="${PREFIX}inspect"
set_malloc_bin="set_taos_malloc.sh"
mqtt_name="${PREFIX}mqtt"

bin_link_dir="/usr/bin"
lib_link_dir="/usr/lib"
lib64_link_dir="/usr/lib64"
inc_link_dir="/usr/include"

#install main path
install_main_dir=${installDir}
# old bin dir
bin_dir="${installDir}/bin"

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

function log() {
  local level="$1"; shift
  local msg="$*"
  case "$level" in
    info)    echo -e "$msg" ;;
    info_color) echo -e "${GREEN_DARK}$msg${NC}" ;;
    success) echo -e "${GREEN_DARK}$msg${NC}" ;;
    warn|warning) echo -e "${YELLOW}$msg${NC}" ;;
    warn_bold) echo -e "${YELLOW}${BOLD}$msg${NC}" ;;
    error)
      echo -e "${RED}$msg${NC}" >&2
      echo -e "${RED}${product_name} has not been installed successfully${NC}" >&2
      exit 1
      ;;
    error_no_exit) echo -e "${RED}$msg${NC}" >&2 ;;
    debug)
      if [[ "$DEBUG" == 1 ]]; then
        echo -e "${GREEN_DARK}$msg ${NC}"
      fi
      ;;
    *) echo -e "$msg" ;;
  esac
}

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

tools=(${clientName} ${benchmarkName} ${dumpName} ${demoName} ${inspect_name} ${mqtt_name} remove.sh ${udfdName} set_core.sh TDinsight.sh start_pre.sh start-all.sh stop-all.sh)
if [ "${verMode}" == "cluster" ]; then
  if [ "${entMode}" == "lite" ]; then
    services=(${serverName} ${adapterName} ${explorerName} ${keeperName})
  else
    services=(${serverName} ${adapterName} ${xname} ${explorerName} ${keeperName})
  fi
elif [ "${verMode}" == "edge" ]; then
  if [ "${pkgMode}" == "full" ]; then
    services=(${serverName} ${adapterName} ${keeperName} ${explorerName})
    tools=(${clientName} ${benchmarkName} ${dumpName} ${demoName} ${mqtt_name} remove.sh ${udfdName} set_core.sh TDinsight.sh start_pre.sh start-all.sh stop-all.sh)
  else
    services=(${serverName})
    tools=(${clientName} ${benchmarkName} remove.sh start_pre.sh)
  fi
else
  services=(${serverName} ${adapterName} ${xname} ${explorerName} ${keeperName})
fi
driver_path=${install_main_dir}/driver


function install_services() {
  for service in "${services[@]}"; do
    install_service ${service}
  done
}

function kill_process() {
  pid=$(ps -ef | grep "$1" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo}kill -9 $pid || :
  fi
}

function install_main_path() {
  #create install main dir and all sub dir
  ${csudo}rm -rf ${install_main_dir}/cfg || :
  ${csudo}rm -rf ${install_main_dir}/bin || :
  ${csudo}rm -rf ${driver_path}/ || :
  ${csudo}rm -rf ${install_main_dir}/examples || :
  ${csudo}rm -rf ${install_main_dir}/include || :
  ${csudo}rm -rf ${install_main_dir}/share || :
  ${csudo}rm -rf ${install_main_dir}/log || :

  ${csudo}mkdir -p ${install_main_dir}
  ${csudo}mkdir -p ${install_main_dir}/cfg
  ${csudo}mkdir -p ${install_main_dir}/bin
  #  ${csudo}mkdir -p ${install_main_dir}/connector
  ${csudo}mkdir -p ${driver_path}/
  ${csudo}mkdir -p ${install_main_dir}/examples
  ${csudo}mkdir -p ${install_main_dir}/include
  ${csudo}mkdir -p ${configDir}
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
  for tool in "${tools[@]}"; do
    ${csudo}rm -f ${bin_link_dir}/${tool} || :
  done

  for service in "${services[@]}"; do
    ${csudo}rm -f ${bin_link_dir}/${service} || :
  done

  if [ "${verType}" == "client" ]; then
    ${csudo}cp -r ${script_dir}/bin/${clientName} ${install_main_dir}/bin
    ${csudo}cp -r ${script_dir}/bin/${benchmarkName} ${install_main_dir}/bin
    ${csudo}cp -r ${script_dir}/bin/${dumpName} ${install_main_dir}/bin
    ${csudo}cp -r ${script_dir}/bin/${inspect_name} ${install_main_dir}/bin
    ${csudo}cp -r ${script_dir}/bin/remove.sh ${install_main_dir}/bin
  else
    ${csudo}cp -r ${script_dir}/bin/* ${install_main_dir}/bin
    ${csudo}cp ${script_dir}/start-all.sh ${install_main_dir}/bin
    ${csudo}cp ${script_dir}/stop-all.sh ${install_main_dir}/bin
  fi

  if [[ "${verMode}" == "cluster" && "${verType}" != "client" ]]; then
    if [ -d ${script_dir}/${xname}/bin ]; then
      ${csudo}cp -r ${script_dir}/${xname}/bin/* ${install_main_dir}/bin
    fi
    if [ -e ${script_dir}/${xname}/uninstall_${xname}.sh ]; then
      ${csudo}cp -r ${script_dir}/${xname}/uninstall_${xname}.sh ${install_main_dir}/uninstall_${xname}.sh
    fi
  fi

  if [ -f ${script_dir}/bin/quick_deploy.sh ]; then
    ${csudo}cp -r ${script_dir}/bin/quick_deploy.sh ${install_main_dir}/bin
  fi

  # set taos_malloc.sh as bin script
  if [ -f ${script_dir}/bin/${set_malloc_bin} ] && [ "${verType}" != "client" ]; then
    ${csudo}cp -r ${script_dir}/bin/${set_malloc_bin} ${install_main_dir}/bin
  else
    echo -e "${RED}Warning: ${set_malloc_bin} not found in bin directory.${NC}"
  fi


  ${csudo}chmod 0555 ${install_main_dir}/bin/*
  [ -x ${install_main_dir}/bin/remove.sh ] && ${csudo}mv ${install_main_dir}/bin/remove.sh ${install_main_dir}/uninstall.sh || :

  #Make link
  for tool in "${tools[@]}"; do
    if [ "${tool}" == "remove.sh" ]; then
      [ -x ${install_main_dir}/uninstall.sh ] && ${csudo}ln -sf ${install_main_dir}/uninstall.sh ${bin_link_dir}/${uninstallScript} || :
    else
      [ -x ${install_main_dir}/bin/${tool} ] && ${csudo}ln -sf ${install_main_dir}/bin/${tool} ${bin_link_dir}/${tool} || :
    fi
  done

  for service in "${services[@]}"; do
    [ -x ${install_main_dir}/bin/${service} ] && ${csudo}ln -sf ${install_main_dir}/bin/${service} ${bin_link_dir}/${service} || :
  done

  [ -x ${install_main_dir}/uninstall_${xname}.sh ] && ${csudo}ln -sf ${install_main_dir}/uninstall_${xname}.sh ${bin_link_dir}/uninstall_${xname}.sh || :
}

function install_lib() {
  # Remove links
  ${csudo}rm -f ${lib_link_dir}/libtaos.* || :
  ${csudo}rm -f ${lib64_link_dir}/libtaos.* || :
  ${csudo}rm -f ${lib_link_dir}/libtaosnative.* || :
  ${csudo}rm -f ${lib64_link_dir}/libtaosnative.* || :
  ${csudo}rm -f ${lib_link_dir}/libtaosws.* || :
  ${csudo}rm -f ${lib64_link_dir}/libtaosws.* || :
  #${csudo}rm -rf ${v15_java_app_dir}              || :
  ${csudo}cp -rf ${script_dir}/driver/* ${driver_path}/ && ${csudo}chmod 777 ${driver_path}/*

  #link lib/link_dir
  ${csudo}ln -sf ${driver_path}/libtaos.* ${lib_link_dir}/libtaos.so.1
  ${csudo}ln -sf ${lib_link_dir}/libtaos.so.1 ${lib_link_dir}/libtaos.so
  ${csudo}ln -sf ${driver_path}/libtaosnative.* ${lib_link_dir}/libtaosnative.so.1
  ${csudo}ln -sf ${lib_link_dir}/libtaosnative.so.1 ${lib_link_dir}/libtaosnative.so

  ${csudo}ln -sf ${driver_path}/libtaosws.so.* ${lib_link_dir}/libtaosws.so || :

  #link jemalloc.so and tcmalloc.so
  jemalloc_file="${driver_path}/libjemalloc.so.2"
  tcmalloc_file="${driver_path}/libtcmalloc.so.4.5.18"
  [ -f "${jemalloc_file}" ] && ${csudo}ln -sf "${jemalloc_file}" "${driver_path}/libjemalloc.so" || echo "jemalloc file not found: ${jemalloc_file}"
  [ -f "${tcmalloc_file}" ] && ${csudo}ln -sf "${tcmalloc_file}" "${driver_path}/libtcmalloc.so" || echo "tcmalloc file not found: ${tcmalloc_file}"


  #link lib64/link_dir
  if [[ -d ${lib64_link_dir} && ! -e ${lib64_link_dir}/libtaos.so ]]; then
    ${csudo}ln -sf ${driver_path}/libtaos.* ${lib64_link_dir}/libtaos.so.1 || :
    ${csudo}ln -sf ${lib64_link_dir}/libtaos.so.1 ${lib64_link_dir}/libtaos.so || :
    ${csudo}ln -sf ${driver_path}/libtaosnative.* ${lib64_link_dir}/libtaosnative.so.1 || :
    ${csudo}ln -sf ${lib64_link_dir}/libtaosnative.so.1 ${lib64_link_dir}/libtaosnative.so || :

    ${csudo}ln -sf ${driver_path}/libtaosws.so.* ${lib64_link_dir}/libtaosws.so || :
  fi

  ${csudo}ldconfig
}

function install_avro() {
  if [ "$ostype" != "Darwin" ]; then
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

  if [ -f ${configDir}/${configFile} ]; then
    ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$newHostname/" ${configDir}/${configFile}
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

    if [ -f ${configDir}/${configFile} ]; then
      ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$localFqdn/" ${configDir}/${configFile}
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
        if [ -f ${configDir}/${configFile} ]; then
          ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$localFqdn/" ${configDir}/${configFile}
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

function install_taosx_config() {
  local only_client=${1:-}
  [ -n "${only_client}" ] && return 0

  file_name="${script_dir}/${xname}/etc/${PREFIX}/${xname}.toml"
  if [ -f ${file_name} ]; then
    ${csudo}sed -i -r "s/#*\s*(fqdn\s*=\s*).*/\1\"${serverFqdn}\"/" ${file_name}

    if [ -f "${configDir}/${xname}.toml" ]; then
      ${csudo}cp ${file_name} ${configDir}/${xname}.toml.new
    else
      ${csudo}cp ${file_name} ${configDir}/${xname}.toml
    fi
  fi
}

function install_explorer_config() {
  local only_client=${1:-}
  [ -n "${only_client}" ] && return 0

  if [ "$verMode" == "cluster" ] && [ "${entMode}" != "lite" ]; then
    file_name="${script_dir}/${xname}/etc/${PREFIX}/explorer.toml"
  else
    file_name="${script_dir}/cfg/explorer.toml"
  fi

  if [ -f "${file_name}" ]; then
    ${csudo}sed -i "s/localhost/${serverFqdn}/g" "${file_name}"
    if [ -f "${configDir}/explorer.toml" ]; then
      ${csudo}cp "${file_name}" "${configDir}/explorer.toml.new"
    else
      ${csudo}cp "${file_name}" "${configDir}/explorer.toml"
    fi
  fi
}

function install_adapter_config() {
  local only_client=${1:-}
  [ -n "${only_client}" ] && return 0

  file_name="${script_dir}/cfg/${adapterName}.toml"
  if [ -f "${file_name}" ]; then
    ${csudo}sed -i -r "s/localhost/${serverFqdn}/g" "${file_name}"

    if [ -f "${configDir}/${adapterName}.toml" ]; then
      ${csudo}cp "${file_name}" "${configDir}/${adapterName}.toml.new"
    else
      ${csudo}cp "${file_name}" "${configDir}/${adapterName}.toml"
    fi
  fi
}

function install_keeper_config() {
  local only_client=${1:-}
  [ -n "${only_client}" ] && return 0

  file_name="${script_dir}/cfg/${keeperName}.toml"
  if [ -f "${file_name}" ]; then
    ${csudo}sed -i -r "s/127.0.0.1/${serverFqdn}/g" "${file_name}"

    if [ -f "${configDir}/${keeperName}.toml" ]; then
      ${csudo}cp "${file_name}" "${configDir}/${keeperName}.toml.new"
    else
      ${csudo}cp "${file_name}" "${configDir}/${keeperName}.toml"
    fi
  fi
}

function install_taosd_config() {
  file_name="${script_dir}/cfg/${configFile}"
  if [ -f ${file_name} ]; then
    ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$serverFqdn/" ${script_dir}/cfg/${configFile}
    ${csudo}echo "monitor 1" >>${script_dir}/cfg/${configFile}
    ${csudo}echo "monitorFQDN ${serverFqdn}" >>${script_dir}/cfg/${configFile}
    if [ "$verMode" == "cluster" ]; then
      ${csudo}echo "audit 1" >>${script_dir}/cfg/${configFile}
    fi

    if [ -f "${configDir}/${configFile}" ]; then
      ${csudo}cp ${file_name} ${configDir}/${configFile}.new
    else
      ${csudo}cp ${file_name} ${configDir}/${configFile}
    fi
  fi

  ${csudo}ln -sf ${configDir}/${configFile} ${install_main_dir}/cfg
}

function install_taosinspect_config() {
  file_name="${script_dir}/cfg/inspect.cfg"
  if [ -f ${file_name} ]; then
    if [ -f "${configDir}/inspect.cfg" ]; then
      ${csudo}cp ${file_name} ${configDir}/inspect.cfg.new
    else
      ${csudo}cp ${file_name} ${configDir}/inspect.cfg
    fi
  fi

  ${csudo}ln -sf ${configDir}/inspect.cfg ${install_main_dir}/cfg
}

function install_config() {
  local only_client=${1:-}
  if ((${update_flag} == 1)); then
    install_taosd_config
    return 0
  fi

  if [ "$interactiveFqdn" == "no" ]; then
    install_taosd_config
    return 0
  fi

  local_fqdn_check
  install_taosd_config

  echo
  echo -e -n "${GREEN}Enter FQDN:port (like h1.${emailName}:6030) of an existing ${productName} cluster node to join${NC}"
  echo
  echo -e -n "${GREEN}OR leave it blank to build one${NC}:"
  read firstEp
  while true; do
    if [ ! -z "$firstEp" ]; then
      if [ -f ${configDir}/${configFile} ]; then
        ${csudo}sed -i -r "s/#*\s*(firstEp\s*).*/\1$firstEp/" ${configDir}/${configFile}
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
  ${csudo}mkdir -p ${logDir} &&  ${csudo}mkdir -p ${logDir}/tcmalloc &&  ${csudo}mkdir -p ${logDir}/jemalloc && ${csudo}chmod 777 ${logDir}

  ${csudo}ln -sf ${logDir} ${install_main_dir}/log
}

function install_data() {
  ${csudo}mkdir -p ${dataDir}

  ${csudo}ln -sf ${dataDir} ${install_main_dir}/data
}

function install_connector() {
  if [ -d "${script_dir}/connector/" ]; then
    ${csudo}cp -rf ${script_dir}/connector/ ${install_main_dir}/ || echo "failed to copy connector"
    ${csudo}cp ${script_dir}/README.md ${install_main_dir}/ || echo "failed to copy README.md"
  fi
}

function install_examples() {
  if [ -d ${script_dir}/examples ]; then
    ${csudo}cp -rf ${script_dir}/examples ${install_main_dir}/ || echo "failed to copy examples"
  fi
}

function install_plugins() {
  if [ -d ${script_dir}/${xname}/plugins ]; then
    ${csudo}cp -rf ${script_dir}/${xname}/plugins/ ${install_main_dir}/ || echo "failed to copy ${PREFIX}x plugins"
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
    if [ "$verMode" == "cluster" ] && [ "${entMode}" != "lite" ]; then
      cfg_source_dir=${script_dir}/${xname}/etc/systemd/system
    else
      cfg_source_dir=${script_dir}/cfg
    fi
  fi

  if [ -f ${cfg_source_dir}/$1.service ]; then
    ${csudo}cp ${cfg_source_dir}/$1.service ${service_config_dir}/ || :
  fi

  # # set default malloc config for cluster(enterprise) and edge(community)
  # if [ "$verMode" == "cluster" ] && [ "$ostype" == "Linux" ]; then
  #   if [ "$1" = "taosd" ] || [ "$1" = "taosadapter" ]; then
  #     echo "set $1 malloc config"
  #     ${csudo} ${install_main_dir}/bin/${set_malloc_bin} -m 0 -q
  #   fi
  # fi

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

function finished_install_info(){
    local entries=()
    # header
    echo
    log info_color "${productName} has been installed successfully!"
    echo

    # collect pairs "label|value"
    entries+=("To configure ${PREFIX}d:|edit ${configDir}/${configFile}")
    if [ -f "${configDir}/${adapterName}.toml" ] && [ -f "${installDir}/bin/${adapterName}" ]; then
      entries+=("To configure ${clientName}Adapter:|edit ${configDir}/${adapterName}.toml")
    fi
    entries+=("To configure ${clientName}Keeper:|edit ${configDir}/${keeperName}.toml")
    entries+=("To configure ${clientName}X:|edit ${configDir}/${xname}.toml")
    entries+=("To configure ${clientName}Explorer:|edit ${configDir}/explorer.toml")

    # insert a blank line between config and start
    entries+=("|")
    
    if ((${service_mod} == 0)); then
      entries+=("To start ${PREFIX}d:|${csudo}systemctl start ${serverName}")
      if [ -f "${service_config_dir}/${clientName}adapter.service" ] && [ -f "${installDir}/bin/${clientName}adapter" ]; then
        entries+=("To start ${clientName}Adapter:|${csudo}systemctl start ${clientName}adapter")
      fi
    elif ((${service_mod} == 1)); then
      entries+=("To start ${productName} server:|${csudo}service ${serverName} start")
      if [ -f "${service_config_dir}/${clientName}adapter.service" ] && [ -f "${installDir}/bin/${clientName}adapter" ]; then
        entries+=("To start ${clientName}Adapter:|${csudo}service ${clientName}adapter start")
      fi
    else
      entries+=("To start ${productName} server:|${serverName}")
      if [ -f "${installDir}/bin/${clientName}adapter" ]; then
        entries+=("To start ${clientName}Adapter:|${clientName}adapter")
      fi
    fi

    entries+=("To start ${clientName}Keeper:|${csudo}systemctl start ${clientName}keeper")

    if [ "$verMode" == "cluster" ] && [ "${entMode}" != "lite" ]; then
      entries+=("To start ${clientName}X:|${csudo}systemctl start ${clientName}x")
    fi
    entries+=("To start ${clientName}Explorer:|${csudo}systemctl start ${clientName}-explorer")
    entries+=("To start all the components:|${csudo}start-all.sh")
    entries+=("|")
    
    entries+=("To access ${productName} CLI:|${clientName} -h $serverFqdn")
    entries+=("To access ${productName} GUI:|http://$serverFqdn:6060")
    entries+=("|")

    if [ "$verMode" == "cluster" ]; then
      entries+=("To read the user manual:|http://$serverFqdn:6060/docs-en")
      entries+=("To manage, analyze and visualize data:|https://tdengine.com/idmp/")
    fi

    # compute max label length
    local max=0
    local label value len
    for pair in "${entries[@]}"; do
      label="${pair%%|*}"
      len=${#label}
      if (( len > max )); then max=$len; fi
    done

    # set fixed max width: at least 40
    local min_width=40
    if (( max < min_width )); then
      max=$min_width
    else
      max=$((max))
    fi

    # print aligned lines
    for pair in "${entries[@]}"; do
      label="${pair%%|*}"
      value="${pair#*|}"
      log info_color "$(printf "%-${max}s %s" "$label" "$value")"
    done

    echo
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
  install_header
  install_lib
  install_config

  if [ "$verMode" == "cluster" ]; then
    install_connector
    install_plugins
  fi

  install_examples
  if [ -z $1 ]; then
    install_bin
    install_services

    if [ "${pkgMode}" != "lite" ]; then
      install_adapter_config
      install_taosx_config
      install_explorer_config
      if [ "${verMode}" == "cluster" ]; then
        install_taosinspect_config
      fi

      if [ "${verMode}" != "cloud" ]; then
        install_keeper_config
      fi
    fi
    finished_install_info
  else
    install_bin

    echo
    echo -e "\033[44;32;1m${productName} client has been installed successfully!${NC}"
  fi

  cd $script_dir
  rm -rf $(tar -tf "${tarName}" | grep -Ev "^\./$|^\/") || :
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

  if [ -z $1 ]; then
    install_data
  fi

  install_log
  install_header
  install_lib
  #install_avro lib
  #install_avro lib64
  install_config

  if [ "$verMode" == "cluster" ]; then
    install_connector
    install_plugins
  fi
  install_examples

  if [ -z $1 ]; then # install service and client
    # For installing new
    install_bin
    install_services

    if [ "${pkgMode}" != "lite" ]; then
      install_adapter_config
      install_taosx_config
      install_explorer_config

      if [ "${verMode}" == "cluster" ]; then
        install_taosinspect_config
      fi

      if [ "${verMode}" != "cloud" ]; then
        install_keeper_config
      fi
    fi

    finished_install_info

  else # Only install client
    install_bin

    echo
    log info_color "${productName} client has been installed successfully!"
  fi

  cd $script_dir
  touch ~/.${historyFile}
  rm -rf $(tar -tf "${tarName}" | grep -Ev "^\./$|^\/") || :
}

check_java_env() {
  if ! command -v java &>/dev/null; then
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

## ==============================Main program starts from here============================
serverFqdn=$(hostname)
if [ "$verType" == "server" ]; then
  if [ -x ${script_dir}/${xname}/bin/${xname} ]; then
    check_java_env
  fi
  # Check default 2.x data file.
  if [ -x ${dataDir}/dnode/dnodeCfg.json ]; then
    echo -e "\033[44;31;5mThe default data directory ${dataDir} contains old data of ${productName} 2.x, please clear it before installing!\033[0m"
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
