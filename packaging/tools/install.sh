#!/bin/bash
#
# This file is used to install database on linux systems. The operating system
# is required to use systemd to manage services at boot

set -e
# set -x

verMode=cluster
pkgMode=full
entMode=full

iplist=""
serverFqdn=""
ostype=$(uname)

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
taosgen_name="${PREFIX}gen"

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'


# Installation mode variables
update_flag=0
prompt_force=0
taos_dir_set=0
initd_mod=0
service_mod=2
silent_mode=0

# User mode variables (will be initialized in setup_env)
user_mode=0
default_path=""
mode_desc=""
sysctl_cmd=""

# Directory variables (will be initialized in setup_env)
bin_link_dir=""
lib_link_dir=""
lib64_link_dir=""
inc_link_dir=""
service_config_dir=""
installDir=""
dataDir=""
logDir=""
configDir=""

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

# set parameters by default value
interactiveFqdn=yes # [yes | no]
verType=client      # [server | client]
initType=systemd    # [systemd | service | ...]

function show_help() {
  cat << EOF
TDengine TSDB Installer.

Usage: $(basename $0) [OPTIONS]

Options:
  -h                        Show help
  -v [server | client]      Install type (server or client)
  -e [yes | no]             Interactive FQDN setting
  -d [install dir]          Custom installation directory
  -s                        Silent mode installation
  -q [taos_dir]             Silent mode with custom installation directory
EOF
}

while getopts "hv:e:d:sq:" arg; do
  case $arg in
  e)
    interactiveFqdn=$(echo $OPTARG)
    silent_mode=1
    ;;
  v)
    verType=$(echo $OPTARG)
    ;;
  d)
    taosDir="${OPTARG%/}/${PREFIX}"
    taosDir=$(eval echo "${taosDir}")
    # user define install dir
    taos_dir_set=1
    ;;
  s)
    silent_mode=1
    ;;
  q)
    silent_mode=1
    taosDir="$OPTARG"
    taos_dir_set=1
    ;;
  h)
    show_help
    exit 0
    ;;
  ?) #unknow option
    echo "unknown argument"
    show_help
    exit 1
    ;;
  esac
done

# ----------------------- Environment setup -----------------------
function setup_env() {
  # 1. Check service type (skip on macOS)
  if [ "$osType" != "Darwin" ]; then
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
  else
    service_mod=2
  fi

  # 2. User mode detection
  if [[ $EUID -eq 0 ]]; then
    user_mode=0
    default_path="/usr/local/${PREFIX}"
    mode_desc="root (system-wide)"
  else
    user_mode=1
    default_path="$HOME/${PREFIX}"
    user="$(whoami)"
    mode_desc="user ($user)"
  fi

  # 3. Install directory setting
  if [[ $silent_mode -eq 0 && $taos_dir_set -eq 0 ]]; then
    while true; do
      echo -ne "${GREEN_DARK}Do you want to install TDengine to the default path?${NC}" \
               "${default_path}" \
               "${GREEN_DARK}(Y/n)${NC}"

      read -e -r -p " : " use_default_path
      if [[ -z "$use_default_path" || "$use_default_path" =~ ^[Yy]$ ]]; then
        taosDir="$default_path"
        break
      elif [[ "$use_default_path" =~ ^[Nn]$ ]]; then
        echo -en "${GREEN_DARK}Please input new install path ${NC}"
        read  -e -r -p " : " new_path
        if [ -n "$new_path" ]; then
          taosDir="${new_path%/}/${PREFIX}"
        else
          taosDir="$default_path"
        fi
        break
      else
        echo -e "${YELLOW}Please enter y, n, or press Enter (default y).${NC}"
      fi
    done
  else
    log info "Detected install mode: $mode_desc"
    if [[ $taos_dir_set -eq 0 ]]; then
      taosDir="$default_path"
    fi
  fi

  # 4. Set directories based on user mode and OS type
  if [[ $user_mode -eq 0 ]]; then
    # Root user directories
    installDir="${taosDir}"
    if [ "$osType" != "Darwin" ]; then
      dataDir="/var/lib/${PREFIX}"
      logDir="/var/log/${PREFIX}"
      configDir="/etc/${PREFIX}"
      bin_link_dir="/usr/bin"
      lib_link_dir="/usr/lib"
      lib64_link_dir="/usr/lib64"
      inc_link_dir="/usr/include"
      service_config_dir="/etc/systemd/system"
      sysctl_cmd="systemctl"
    else
      dataDir="${installDir}/data"
      logDir=~/${productName}/log
      configDir="/etc/${PREFIX}"
      bin_link_dir="/usr/local/bin"
      lib_link_dir="/usr/local/lib"
      lib64_link_dir=""
      inc_link_dir="/usr/local/include"
      service_config_dir=""
      sysctl_cmd=""
    fi
  else
    # Non-root user directories
    installDir="${taosDir}"
    dataDir="${installDir}/data"
    logDir="${installDir}/log"
    configDir="${installDir}/cfg"
    if [ "$osType" != "Darwin" ]; then
      bin_link_dir="$HOME/.local/bin"
      lib_link_dir="$HOME/.local/lib"
      lib64_link_dir="$HOME/.local/lib64"
      inc_link_dir="$HOME/.local/include"
      service_config_dir="$HOME/.config/systemd/user"
      sysctl_cmd="systemctl --user"
    else
      bin_link_dir="$HOME/.local/bin"
      lib_link_dir="$HOME/.local/lib"
      lib64_link_dir=""
      inc_link_dir="$HOME/.local/include"
      service_config_dir=""
      sysctl_cmd=""
    fi
    
    # Create necessary directories for non-root user
    mkdir -p "$bin_link_dir" "$lib_link_dir" "$inc_link_dir" 2>/dev/null || :
    if [ "$osType" != "Darwin" ]; then
      mkdir -p "$lib64_link_dir" "$service_config_dir" 2>/dev/null || :
    fi
  fi
  bin_dir="${installDir}/bin"
  driver_path=${installDir}/driver
  install_main_dir=${installDir}
  
  #  tools/services/config_files files setting
  if [ "$verType" == "client" ]; then
    # 仅 client 工具，不含服务
    remove_name="remove_client.sh"
    tools=("${clientName}" "${benchmarkName}" "${dumpName}" "${demoName}" "${inspect_name}" "${taosgen_name}" "${remove_name}")
    services=()
    
  else
    # server/默认，按 verMode/pkgMode/entMode 细分
    remove_name="remove.sh"
    tools=("${clientName}" "${benchmarkName}" "${dumpName}" "${demoName}" "${inspect_name}" "${mqtt_name}" "${remove_name}" "${udfdName}" set_core.sh TDinsight.sh startPre.sh start-all.sh stop-all.sh "${taosgen_name}")
    if [ "${verMode}" == "cluster" ]; then
      if [ "${entMode}" == "lite" ]; then
        services=("${serverName}" "${adapterName}" "${explorerName}" "${keeperName}")
      else
        services=("${serverName}" "${adapterName}" "${xname}" "${explorerName}" "${keeperName}")
      fi
    elif [ "${verMode}" == "edge" ]; then
      if [ "${pkgMode}" == "full" ]; then
        services=("${serverName}" "${adapterName}" "${keeperName}" "${explorerName}")
        tools=("${clientName}" "${benchmarkName}" "${dumpName}" "${demoName}" "${mqtt_name}" "${remove_name}" "${udfdName}" set_core.sh TDinsight.sh startPre.sh start-all.sh stop-all.sh "${taosgen_name}")
      else
        services=("${serverName}")
        tools=("${clientName}" "${benchmarkName}" "${remove_name}" startPre.sh)
      fi
    else
      services=("${serverName}" "${adapterName}" "${xname}" "${explorerName}" "${keeperName}")
    fi
  fi
  
  log info "TDengine installation path: ${installDir}"
  log info "Data directory: ${dataDir}"
  log info "Log directory: ${logDir}"
  log info "Config directory: ${configDir}"
}

function get_config_file() {
    case "$1" in
      taosd) echo "taos.cfg" ;;
      taosadapter) echo "taosadapter.toml" ;;
      taosx) echo "taosx.toml" ;;
      taoskeeper) echo "taoskeeper.toml" ;;
      taos-explorer) echo "explorer.toml" ;;
      *) echo "" ;;
    esac
}

function install_services() {
  for service in "${services[@]}"; do
    install_service ${service}
  done
}

function kill_process() {
  pid=$(ps -ef | grep "$1" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    kill -9 $pid || :
  fi
}

function install_main_path() {
  #create install main dir and all sub dir
  if [ $taos_dir_set -eq 0 ] && [ $user_mode -eq 0 ]; then
    rm -rf "${install_main_dir}/cfg" || :
  fi
  rm -rf "${install_main_dir}/bin" || :
  rm -rf "${driver_path}/" || :
  rm -rf "${install_main_dir}/examples" || :
  rm -rf "${install_main_dir}/include" || :
  rm -rf "${install_main_dir}/share" || :
  rm -rf ${install_main_dir}/log || :

  mkdir -p ${install_main_dir}
  mkdir -p ${install_main_dir}/cfg
  mkdir -p "${install_main_dir}/bin"
  #  mkdir -p ${install_main_dir}/connector
  mkdir -p "${driver_path}/"
  mkdir -p "${install_main_dir}/examples"
  mkdir -p "${install_main_dir}/include"
  mkdir -p "${configDir}"
  #  mkdir -p "${install_main_dir}/init.d"
  if [ "$verMode" == "cluster" ]; then
    mkdir -p "${install_main_dir}/share"
  fi

  if [[ -e ${script_dir}/email ]]; then
    cp ${script_dir}/email ${install_main_dir}/ || :
  fi
}

function install_bin() {
  # Remove links
  echo tools: ${tools[@]}
  for tool in "${tools[@]}"; do
    rm -f ${bin_link_dir}/${tool} || :
  done
  echo services: ${services[@]}
  for service in "${services[@]}"; do
    rm -f ${bin_link_dir}/${service} || :
  done

  if [ "${verType}" == "client" ]; then
    cp -r ${script_dir}/bin/${clientName} ${install_main_dir}/bin
    cp -r ${script_dir}/bin/${benchmarkName} ${install_main_dir}/bin
    cp -r ${script_dir}/bin/${dumpName} ${install_main_dir}/bin
    cp -r ${script_dir}/bin/${inspect_name} ${install_main_dir}/bin || :
    cp -r ${script_dir}/bin/${taosgen_name} ${install_main_dir}/bin || :
    cp -r ${script_dir}/bin/remove_client.sh ${install_main_dir}/bin
  else
    cp -r ${script_dir}/bin/* ${install_main_dir}/bin
    if [ "${pkgMode}" != "lite" ]; then
      cp ${script_dir}/start-all.sh ${install_main_dir}/bin
      cp ${script_dir}/stop-all.sh ${install_main_dir}/bin
    fi
  fi

  if [[ "${verMode}" == "cluster" && "${verType}" != "client" ]]; then
    if [ -d ${script_dir}/${xname}/bin ]; then
      cp -r ${script_dir}/${xname}/bin/* ${install_main_dir}/bin
    fi
    if [ -e ${script_dir}/${xname}/uninstall_${xname}.sh ]; then
      cp -r ${script_dir}/${xname}/uninstall_${xname}.sh ${install_main_dir}/uninstall_${xname}.sh
    fi
  fi

  if [ -f ${script_dir}/bin/quick_deploy.sh ]; then
    cp -r ${script_dir}/bin/quick_deploy.sh ${install_main_dir}/bin
  fi

  # set taos_malloc.sh as bin script
  if [ "${pkgMode}" != "lite" ]; then
    if [[ -f ${script_dir}/bin/${set_malloc_bin} && "${verType}" != "client" ]]; then
      cp -r ${script_dir}/bin/${set_malloc_bin} ${install_main_dir}/bin
    else
      echo -e "${RED}Warning: ${set_malloc_bin} not found in bin directory.${NC}"
    fi
  fi


  chmod 0555 ${install_main_dir}/bin/*
  [ -x ${install_main_dir}/bin/${remove_name} ] && mv -f ${install_main_dir}/bin/${remove_name} ${install_main_dir}/uninstall.sh || :

  #Make link
  for tool in "${tools[@]}"; do
    if [ "${tool}" == "${remove_name}" ]; then
      [ -x ${install_main_dir}/uninstall.sh ] && ln -sf ${install_main_dir}/uninstall.sh ${bin_link_dir}/${uninstallScript} || :
    else
      if [ "${tool}" == "startPre.sh" ]; then
        sed -i "s|/var/log/taos|${logDir}|g" ${install_main_dir}/bin/${tool} 
      fi
      [ -x ${install_main_dir}/bin/${tool} ] && ln -sf ${install_main_dir}/bin/${tool} ${bin_link_dir}/${tool} || :
    fi
  done

  for service in "${services[@]}"; do
    [ -x ${install_main_dir}/bin/${service} ] && ln -sf ${install_main_dir}/bin/${service} ${bin_link_dir}/${service} || :
  done

  [ -x ${install_main_dir}/uninstall_${xname}.sh ] && ln -sf ${install_main_dir}/uninstall_${xname}.sh ${bin_link_dir}/uninstall_${xname}.sh || :

  # 非Root模式配置环境变量
  if [[ $user_mode -eq 1 ]]; then
    local env_file=""
    if [[ -n "$ZSH_VERSION" ]]; then
      env_file="${HOME}/.zshrc"
    else
      env_file="${HOME}/.bashrc"
    fi

    # 添加PATH
    if ! grep -q "${install_main_dir}/bin" "$env_file" 2>/dev/null; then
      echo -e "\n# TDengine install path" >> "$env_file"
      echo "export PATH=\"${install_main_dir}/bin:\$PATH\"" >> "$env_file"
      log info "Added TDengine bin to PATH (${env_file})"
    fi

    # 添加 LD_LIBRARY_PATH
    if ! grep -q "${lib_link_dir}" "$env_file" 2>/dev/null; then
      echo "export LD_LIBRARY_PATH=\"${lib_link_dir}:\$LD_LIBRARY_PATH\"" >> "$env_file"
      log info "Added TDengine lib to LD_LIBRARY_PATH (${env_file})"
    fi

  fi
  if [ "$verType" != "client" ]; then
    cp ${script_dir}/README.md ${install_main_dir}/ || echo "failed to copy README.md"
  fi
}

function install_lib() {
  # Remove links
  rm -f ${lib_link_dir}/libtaos.* || :
  rm -f ${lib_link_dir}/libtaosnative.* || :
  rm -f ${lib_link_dir}/libtaosws.* || :
  if [ "$osType" != "Darwin" ]; then
    rm -f ${lib64_link_dir}/libtaos.* || :
    rm -f ${lib64_link_dir}/libtaosnative.* || :
    rm -f ${lib64_link_dir}/libtaosws.* || :
  fi
  #rm -rf ${v15_java_app_dir}              || :
  cp -rf ${script_dir}/driver/* ${driver_path}/ && chmod 777 ${driver_path}/*

  # Link libraries based on OS type
  if [ "$osType" != "Darwin" ]; then
    # Linux specific linking
    ln -sf ${driver_path}/libtaos.* ${lib_link_dir}/libtaos.so.1
    ln -sf ${lib_link_dir}/libtaos.so.1 ${lib_link_dir}/libtaos.so
    ln -sf ${driver_path}/libtaosnative.* ${lib_link_dir}/libtaosnative.so.1
    ln -sf ${lib_link_dir}/libtaosnative.so.1 ${lib_link_dir}/libtaosnative.so

    ln -sf ${driver_path}/libtaosws.so.* ${lib_link_dir}/libtaosws.so || :

    # Link lib64 if it exists
    if [[ -d ${lib64_link_dir} && ! -e ${lib64_link_dir}/libtaos.so ]]; then
      ln -sf ${driver_path}/libtaos.* ${lib64_link_dir}/libtaos.so.1 || :
      ln -sf ${lib64_link_dir}/libtaos.so.1 ${lib64_link_dir}/libtaos.so || :
      ln -sf ${driver_path}/libtaosnative.* ${lib64_link_dir}/libtaosnative.so.1 || :
      ln -sf ${lib64_link_dir}/libtaosnative.so.1 ${lib64_link_dir}/libtaosnative.so || :

      ln -sf ${driver_path}/libtaosws.so.* ${lib64_link_dir}/libtaosws.so || :
    fi

    # Update library cache
    if [[ $user_mode -eq 0 ]]; then
      ldconfig 2>/dev/null || log warn "Failed to update library cache (ldconfig)"
    fi
  else
    # macOS specific linking
    ln -sf ${driver_path}/libtaos.* ${lib_link_dir}/libtaos.1.dylib
    ln -sf ${lib_link_dir}/libtaos.1.dylib ${lib_link_dir}/libtaos.dylib
    ln -sf ${driver_path}/libtaosnative.* ${lib_link_dir}/libtaosnative.1.dylib
    ln -sf ${lib_link_dir}/libtaosnative.1.dylib ${lib_link_dir}/libtaosnative.dylib

    [ -f ${driver_path}/libtaosws.dylib.* ] && ln -sf ${driver_path}/libtaosws.dylib.* ${lib_link_dir}/libtaosws.dylib || :

    # Update dyld shared cache
    if [[ $user_mode -eq 0 ]]; then
      update_dyld_shared_cache 2>/dev/null || log warn "Failed to update dyld shared cache"
    fi
  fi

  # Link jemalloc.so and tcmalloc.so (Linux only)
  if [ "$osType" != "Darwin" ]; then
    jemalloc_file="${driver_path}/libjemalloc.so.2"
    tcmalloc_file="${driver_path}/libtcmalloc.so.4.5.18"
    [ -f "${jemalloc_file}" ] && ln -sf "${jemalloc_file}" "${driver_path}/libjemalloc.so" || echo "jemalloc file not found: ${jemalloc_file}"
    [ -f "${tcmalloc_file}" ] && ln -sf "${tcmalloc_file}" "${driver_path}/libtcmalloc.so" || echo "tcmalloc file not found: ${tcmalloc_file}"
  fi
  
}

function install_avro() {
  if [ "$ostype" != "Darwin" ]; then
    avro_dir=${script_dir}/avro
    if [ -f "${avro_dir}/lib/libavro.so.23.0.0" ] && [ -d /usr/local/$1 ]; then
      /usr/bin/install -c -d /usr/local/$1
      /usr/bin/install -c -m 755 ${avro_dir}/lib/libavro.so.23.0.0 /usr/local/$1
      ln -sf /usr/local/$1/libavro.so.23.0.0 /usr/local/$1/libavro.so.23
      ln -sf /usr/local/$1/libavro.so.23 /usr/local/$1/libavro.so

      /usr/bin/install -c -d /usr/local/$1
      [ -f ${avro_dir}/lib/libavro.a ] &&
        /usr/bin/install -c -m 755 ${avro_dir}/lib/libavro.a /usr/local/$1

      if [ -d /etc/ld.so.conf.d ]; then
        echo "/usr/local/$1" | tee /etc/ld.so.conf.d/libavro.conf >/dev/null || echo -e "failed to write /etc/ld.so.conf.d/libavro.conf"
        ldconfig
      else
        echo "/etc/ld.so.conf.d not found!"
      fi
    fi
  fi
}

function install_jemalloc() {
  jemalloc_dir=${script_dir}/jemalloc

  if [ -d ${jemalloc_dir} ]; then
    /usr/bin/install -c -d /usr/local/bin

    if [ -f ${jemalloc_dir}/bin/jemalloc-config ]; then
      /usr/bin/install -c -m 755 ${jemalloc_dir}/bin/jemalloc-config /usr/local/bin
    fi
    if [ -f ${jemalloc_dir}/bin/jemalloc.sh ]; then
      /usr/bin/install -c -m 755 ${jemalloc_dir}/bin/jemalloc.sh /usr/local/bin
    fi
    if [ -f ${jemalloc_dir}/bin/jeprof ]; then
      /usr/bin/install -c -m 755 ${jemalloc_dir}/bin/jeprof /usr/local/bin
    fi
    if [ -f ${jemalloc_dir}/include/jemalloc/jemalloc.h ]; then
      /usr/bin/install -c -d /usr/local/include/jemalloc
      /usr/bin/install -c -m 644 ${jemalloc_dir}/include/jemalloc/jemalloc.h /usr/local/include/jemalloc
    fi
    if [ -f ${jemalloc_dir}/lib/libjemalloc.so.2 ]; then
      /usr/bin/install -c -d /usr/local/lib
      /usr/bin/install -c -m 755 ${jemalloc_dir}/lib/libjemalloc.so.2 /usr/local/lib
      ln -sf libjemalloc.so.2 /usr/local/lib/libjemalloc.so
      /usr/bin/install -c -d /usr/local/lib
      # if [ -f ${jemalloc_dir}/lib/libjemalloc.a ]; then
      #   /usr/bin/install -c -m 755 ${jemalloc_dir}/lib/libjemalloc.a /usr/local/lib
      # fi
      # if [ -f ${jemalloc_dir}/lib/libjemalloc_pic.a ]; then
      #   /usr/bin/install -c -m 755 ${jemalloc_dir}/lib/libjemalloc_pic.a /usr/local/lib
      # fi
      if [ -f ${jemalloc_dir}/lib/pkgconfig/jemalloc.pc ]; then
        /usr/bin/install -c -d /usr/local/lib/pkgconfig
        /usr/bin/install -c -m 644 ${jemalloc_dir}/lib/pkgconfig/jemalloc.pc /usr/local/lib/pkgconfig
      fi
    fi
    if [ -f ${jemalloc_dir}/share/doc/jemalloc/jemalloc.html ]; then
      /usr/bin/install -c -d /usr/local/share/doc/jemalloc
      /usr/bin/install -c -m 644 ${jemalloc_dir}/share/doc/jemalloc/jemalloc.html /usr/local/share/doc/jemalloc
    fi
    if [ -f ${jemalloc_dir}/share/man/man3/jemalloc.3 ]; then
      /usr/bin/install -c -d /usr/local/share/man/man3
      /usr/bin/install -c -m 644 ${jemalloc_dir}/share/man/man3/jemalloc.3 /usr/local/share/man/man3
    fi

    if [ "$osType" != "Darwin" ]; then
      if [ -d /etc/ld.so.conf.d ]; then
        echo "/usr/local/lib" | tee /etc/ld.so.conf.d/jemalloc.conf >/dev/null || echo -e "failed to write /etc/ld.so.conf.d/jemalloc.conf"
        ldconfig
      else
        echo "/etc/ld.so.conf.d not found!"
      fi
    fi
  fi
}

function install_header() {
  rm -f ${inc_link_dir}/taos.h ${inc_link_dir}/taosdef.h ${inc_link_dir}/taoserror.h ${inc_link_dir}/tdef.h ${inc_link_dir}/taosudf.h || :

  [ -f ${inc_link_dir}/taosws.h ] && rm -f ${inc_link_dir}/taosws.h || :

  cp -f ${script_dir}/inc/* ${install_main_dir}/include && chmod 644 ${install_main_dir}/include/*
  ln -sf ${install_main_dir}/include/taos.h ${inc_link_dir}/taos.h
  ln -sf ${install_main_dir}/include/taosdef.h ${inc_link_dir}/taosdef.h
  ln -sf ${install_main_dir}/include/taoserror.h ${inc_link_dir}/taoserror.h
  ln -sf ${install_main_dir}/include/tdef.h ${inc_link_dir}/tdef.h
  ln -sf ${install_main_dir}/include/taosudf.h ${inc_link_dir}/taosudf.h

  [ -f ${install_main_dir}/include/taosws.h ] && ln -sf ${install_main_dir}/include/taosws.h ${inc_link_dir}/taosws.h || :
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
    chmod 666 /etc/hosts
    echo "127.0.0.1  $1" >>/etc/hosts
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

  # hostname $newHostname || :
  # retval=$(echo $?)
  # if [[ $retval != 0 ]]; then
  #   echo
  #   echo "set hostname fail!"
  #   return
  # fi

  # #ubuntu/centos /etc/hostname
  # if [[ -e /etc/hostname ]]; then
  #   echo $newHostname >/etc/hostname || :
  # fi

  # #debian: #HOSTNAME=yourname
  # if [[ -e /etc/sysconfig/network ]]; then
  #   sed -i -r "s/#*\s*(HOSTNAME=\s*).*/\1$newHostname/" /etc/sysconfig/network || :
  # fi

  if [ -f ${configDir}/${configFile} ]; then
    sed -i -r "s/#*\s*(fqdn\s*).*/\1$newHostname/" ${configDir}/${configFile}
  else
    sed -i -r "s/#*\s*(fqdn\s*).*/\1$newHostname/" ${script_dir}/cfg/${configFile}
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
      sed -i -r "s/#*\s*(fqdn\s*).*/\1$localFqdn/" ${configDir}/${configFile}
    else
      sed -i -r "s/#*\s*(fqdn\s*).*/\1$localFqdn/" ${script_dir}/cfg/${configFile}
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
          sed -i -r "s/#*\s*(fqdn\s*).*/\1$localFqdn/" ${configDir}/${configFile}
        else
          sed -i -r "s/#*\s*(fqdn\s*).*/\1$localFqdn/" ${script_dir}/cfg/${configFile}
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
  if [ $taos_dir_set -eq 1 ]; then
    mkdir -p "${dataDir}/taosx"
  fi
  if [ -f "${file_name}" ]; then
    sed -i -r "s/#*\s*(fqdn\s*=\s*).*/\1\"${serverFqdn}\"/" "${file_name}"

    # replace data_dir
    sed -i -r "0,/data_dir\s*=\s*/s|#*\s*(data_dir\s*=\s*).*|\1\"${dataDir}/taosx\"|" "${file_name}"

    # replace log path
    sed -i -r "0,/path\s*=\s*/s|#*\s*(path\s*=\s*).*|\1\"${logDir}\"|" "${file_name}"


    if [ -f "${configDir}/${xname}.toml" ]; then
      cp ${file_name} ${configDir}/${xname}.toml.new
    else
      cp ${file_name} ${configDir}/${xname}.toml
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
  if [ $taos_dir_set -eq 1 ]; then
    mkdir -p "${dataDir}/explorer"
  fi
  if [ -f "${file_name}" ]; then
    # replace fqdn
    sed -i "s/localhost/${serverFqdn}/g" "${file_name}"

    # replace data_dir
    sed -i -r "0,/data_dir\s*=\s*/s|#*\s*(data_dir\s*=\s*).*|\1\"${dataDir}/explorer\"|" "${file_name}"

    # replace log path
    sed -i -r "0,/path\s*=\s*/s|#*\s*(path\s*=\s*).*|\1\"${logDir}\"|" "${file_name}"

    if [ -f "${configDir}/explorer.toml" ]; then
      cp "${file_name}" "${configDir}/explorer.toml.new"
    else
      cp "${file_name}" "${configDir}/explorer.toml"
    fi
  fi
}

function install_adapter_config() {
  local only_client=${1:-}
  [ -n "${only_client}" ] && return 0

  file_name="${script_dir}/cfg/${adapterName}.toml"
  if [ -f "${file_name}" ]; then
    sed -i -r "s/localhost/${serverFqdn}/g" "${file_name}"

    # replace log path
    sed -i -r "s|#*\s*(path\s*=\s*).*|\1\"${logDir}\"|" "${file_name}"
    
    # replace cfg path
    sed -i -r "s|#*\s*(taosConfigDir\s*=\s*).*|\1\"${configDir}\"|" "${file_name}"

    if [ -f "${configDir}/${adapterName}.toml" ]; then
      cp "${file_name}" "${configDir}/${adapterName}.toml.new"
    else
      cp "${file_name}" "${configDir}/${adapterName}.toml"
    fi
  fi
}

function install_keeper_config() {
  local only_client=${1:-}
  [ -n "${only_client}" ] && return 0

  file_name="${script_dir}/cfg/${keeperName}.toml"
  if [ -f "${file_name}" ]; then
    sed -i -r "s/127.0.0.1/${serverFqdn}/g" "${file_name}"
    sed -i -r "s|#*\s*(path\s*=\s*).*|\1\"${logDir}\"|" "${file_name}"

    if [ -f "${configDir}/${keeperName}.toml" ]; then
      cp "${file_name}" "${configDir}/${keeperName}.toml.new"
    else
      cp "${file_name}" "${configDir}/${keeperName}.toml"
    fi
  fi
}

function install_taosd_config() {
  file_name="${script_dir}/cfg/${configFile}"
  if [ -f ${file_name} ]; then
    sed -i -r "s/#*\s*(fqdn\s*).*/\1$serverFqdn/" ${script_dir}/cfg/${configFile}
    echo "monitor 1" >>${script_dir}/cfg/${configFile}
    echo "monitorFQDN ${serverFqdn}" >>${script_dir}/cfg/${configFile}

    if grep -q "^dataDir " "${script_dir}/cfg/${configFile}"; then
      sed -i "s|^dataDir .*|dataDir ${dataDir}|" "${script_dir}/cfg/${configFile}"
    else
      echo "dataDir ${dataDir}" >>"${script_dir}/cfg/${configFile}"
    fi

    if grep -q "^logDir " "${script_dir}/cfg/${configFile}"; then
      sed -i "s|^logDir .*|logDir ${logDir}|" "${script_dir}/cfg/${configFile}"
    else
      echo "logDir ${logDir}" >>"${script_dir}/cfg/${configFile}"
    fi

    if [ "$verMode" == "cluster" ]; then
      echo "audit 1" >>${script_dir}/cfg/${configFile}
    fi

    if [ -f "${configDir}/${configFile}" ]; then
      cp ${file_name} ${configDir}/${configFile}.new
    else
      cp ${file_name} ${configDir}/${configFile}
    fi
  fi
  if [ $taos_dir_set -eq 0 ] && [ $user_mode -eq 0 ]; then
    rm -rf ${install_main_dir}/cfg || :
    ln -sf "${configDir}" "${install_main_dir}/cfg"
  fi
}

function install_taosinspect_config() {
  file_name="${script_dir}/cfg/inspect.cfg"
  if [ -f ${file_name} ]; then
    if [ -f "${configDir}/inspect.cfg" ]; then
      cp ${file_name} ${configDir}/inspect.cfg.new
    else
      cp ${file_name} ${configDir}/inspect.cfg
    fi
  fi
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
        sed -i -r "s/#*\s*(firstEp\s*).*/\1$firstEp/" ${configDir}/${configFile}
      else
        sed -i -r "s/#*\s*(firstEp\s*).*/\1$firstEp/" ${script_dir}/cfg/${configFile}
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
      bash -c "echo $emailAddr > ${email_file}"
      break
    else
      break
    fi
  done
}

function install_log() {    
    if [ "$osType" != "Darwin" ]; then
        mkdir -p ${logDir} && mkdir -p ${logDir}/tcmalloc && mkdir -p ${logDir}/jemalloc && chmod 777 ${logDir}
    else
        mkdir -p ${logDir} && chmod 777 ${logDir}
    fi
    
    if [ $taos_dir_set -eq 0 ] && [ $user_mode -eq 0 ]; then
      ln -sf ${logDir} ${install_main_dir}/log
    fi
}

function install_data() {
  mkdir -p ${dataDir}
  if [ $taos_dir_set -eq 0 ] && [ $user_mode -eq 0 ]; then
    ln -sf ${dataDir} ${install_main_dir}/data
  fi
}

function install_connector() {
  if [ -d "${script_dir}/connector/" ]; then
    cp -rf ${script_dir}/connector/ ${install_main_dir}/ || echo "failed to copy connector"
  fi
}

function install_examples() {
  if [ -d ${script_dir}/examples ]; then
    cp -rf ${script_dir}/examples ${install_main_dir}/ || echo "failed to copy examples"
  fi
}

function install_plugins() {
  if [ -d ${script_dir}/${xname}/plugins ]; then
    cp -rf ${script_dir}/${xname}/plugins/ ${install_main_dir}/ || echo "failed to copy ${PREFIX}x plugins"
  fi
}

function clean_service_on_sysvinit() {
  if ps aux | grep -v grep | grep $1 &>/dev/null; then
    service $1 stop || :
  fi

  if ((${initd_mod} == 1)); then
    if [ -e ${service_config_dir}/$1 ]; then
      chkconfig --del $1 || :
    fi
  elif ((${initd_mod} == 2)); then
    if [ -e ${service_config_dir}/$1 ]; then
      insserv -r $1 || :
    fi
  elif ((${initd_mod} == 3)); then
    if [ -e ${service_config_dir}/$1 ]; then
      update-rc.d -f $1 remove || :
    fi
  fi

  rm -f ${service_config_dir}/$1 || :

  if $(which init &>/dev/null); then
    init q || :
  fi
}

function install_service_on_sysvinit() {
  if [ "$1" != "${serverName}" ]; then
    return
  fi

  clean_service_on_sysvinit $1
  sleep 1

  if ((${os_type} == 1)); then
    cp ${script_dir}/init.d/${serverName}.deb ${service_config_dir}/${serverName} && chmod a+x ${service_config_dir}/${serverName}
  elif ((${os_type} == 2)); then
    cp ${script_dir}/init.d/${serverName}.rpm ${service_config_dir}/${serverName} && chmod a+x ${service_config_dir}/${serverName}
  fi

  if ((${initd_mod} == 1)); then
    chkconfig --add $1 || :
    chkconfig --level 2345 $1 on || :
  elif ((${initd_mod} == 2)); then
    insserv $1} || :
    insserv -d $1 || :
  elif ((${initd_mod} == 3)); then
    update-rc.d $1 defaults || :
  fi
}

function clean_service_on_systemd() {
  service_config="${service_config_dir}/$1.service"

  if ${sysctl_cmd} is-active --quiet $1; then
    echo "$1 is running, stopping it..."
    ${sysctl_cmd} stop $1 &>/dev/null || echo &>/dev/null
  fi
  ${sysctl_cmd} disable $1 &>/dev/null || echo &>/dev/null
  rm -f ${service_config}
}

function install_service_on_systemd() {
  clean_service_on_systemd $1
  service_config="${service_config_dir}/$1.service"
  cfg_source_dir=${script_dir}/cfg
  if [[ "$1" == "${xname}" || "$1" == "${explorerName}" ]]; then
    if [ "$verMode" == "cluster" ] && [ "${entMode}" != "lite" ]; then
      cfg_source_dir=${script_dir}/${xname}/etc/systemd/system
    else
      cfg_source_dir=${script_dir}/cfg
    fi
  fi

  if [ -f "${cfg_source_dir}/$1.service" ]; then
    tmp_service_file="/tmp/${1}.service.$$"
    service_config="${service_config_dir}/$1.service"
    
    # Ensure service config directory exists
    mkdir -p "${service_config_dir}" || { echo "Failed to create directory ${service_config_dir}"; exit 1; }
    
    cp "${cfg_source_dir}/$1.service" "$tmp_service_file"
    
    if [ ${user_mode} -eq 1 ]; then
      sed -i.bak \
        -e "/^EnvironmentFile=/d" \
        -e '/^ExecStartPre=/d' \
        -e "s|^LimitNOFILE=.*|LimitNOFILE=1048576|g" \
        -e '/^LimitCORE=/d' \
        -e '/^LimitNPROC=/d' \
        -e 's|^WantedBy=.*|WantedBy=default.target|g' \
        -e "/^\[Service\]/a Environment=\"LD_LIBRARY_PATH=${lib_link_dir}\"" \
        "$tmp_service_file"
    fi
    cfg_file=$(get_config_file "$1")
    sed -i \
      -e "s|/usr/local/taos/bin|${bin_dir}|g" \
      -e "s|^ExecStart=.*|ExecStart=${bin_dir}/$1 -c ${configDir}/${cfg_file}|g" \
      "$tmp_service_file"

    cp "$tmp_service_file" "${service_config}" || { echo "Failed to copy $tmp_service_file to ${service_config}"; exit 1; }
    chmod 644 "${service_config}" || { echo "Failed to set permissions for ${service_config}"; exit 1; }
    
    rm -f "$tmp_service_file"
  fi

  # # set default malloc config for cluster(enterprise) and edge(community)
  # if [ "$verMode" == "cluster" ] && [ "$ostype" == "Linux" ]; then
  #   if [ "$1" = "taosd" ] || [ "$1" = "taosadapter" ]; then
  #     echo "set $1 malloc config"
  #      ${install_main_dir}/bin/${set_malloc_bin} -m 0 -q
  #   fi
  # fi

  ${sysctl_cmd} daemon-reload
  ${sysctl_cmd} enable $1
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

function deb_erase() {
  confirm=""
  while [ "" == "${confirm}" ]; do
    echo -e -n "${RED}Existing TDengine deb is detected, do you want to remove it? [yes|no] ${NC}:"
    read confirm
    if [ "yes" == "$confirm" ]; then
      dpkg --remove tdengine || :
      break
    elif [ "no" == "$confirm" ]; then
      break
    fi
  done
}

function rpm_erase() {
  confirm=""
  while [ "" == "${confirm}" ]; do
    echo -e -n "${RED}Existing TDengine rpm is detected, do you want to remove it? [yes|no] ${NC}:"
    read confirm
    if [ "yes" == "$confirm" ]; then
      rpm -e tdengine || :
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
    if [ "${pkgMode}" != "lite" ]; then
      entries+=("To configure ${PREFIX}d:|edit ${configDir}/${configFile}")
      if [[ -f "${configDir}/${adapterName}.toml" && -f "${installDir}/bin/${adapterName}" ]]; then
        entries+=("To configure ${clientName}Adapter:|edit ${configDir}/${adapterName}.toml")
      fi
      
      entries+=("To configure ${clientName}Keeper:|edit ${configDir}/${keeperName}.toml")
      entries+=("To configure ${clientName}X:|edit ${configDir}/${xname}.toml")
      entries+=("To configure ${clientName}Explorer:|edit ${configDir}/explorer.toml")

      # insert a blank line between config and start
      entries+=("|")
      
      if ((service_mod == 0)); then
        entries+=("To start ${PREFIX}d:|${sysctl_cmd} start ${serverName}")
        if [[ -f "${service_config_dir}/${clientName}adapter.service" && -f "${installDir}/bin/${clientName}adapter" ]]; then
          entries+=("To start ${clientName}Adapter:|${sysctl_cmd} start ${clientName}adapter")
        fi
      elif ((service_mod == 1)); then
        entries+=("To start ${productName} server:|service ${serverName} start")
        if [[ -f "${service_config_dir}/${clientName}adapter.service" && -f "${installDir}/bin/${clientName}adapter" ]]; then
          entries+=("To start ${clientName}Adapter:|service ${clientName}adapter start")
        fi
      else
        entries+=("To start ${productName} server:|${serverName}")
        if [ -f "${installDir}/bin/${clientName}adapter" ]; then
          entries+=("To start ${clientName}Adapter:|${clientName}adapter")
        fi
      fi

      entries+=("To start ${clientName}Keeper:|${sysctl_cmd} start ${clientName}keeper")

      if [ "$verMode" == "cluster" ] && [ "${entMode}" != "lite" ]; then
        entries+=("To start ${clientName}X:|${sysctl_cmd} start ${clientName}x")
      fi
      entries+=("To start ${clientName}Explorer:|${sysctl_cmd} start ${clientName}-explorer")
      entries+=("To start all the components:|start-all.sh")
      entries+=("|")
      
      entries+=("To access ${productName} CLI:|${clientName} -h $serverFqdn")
      entries+=("To access ${productName} GUI:|http://$serverFqdn:6060")
      entries+=("|")

      if [ "${verMode}" == "cluster" ]; then
        entries+=("To read the user manual:|http://$serverFqdn:6060/docs-en")
        entries+=("To manage, analyze and visualize data:|https://tdengine.com/idmp/")
      else
        entries+=("To read the user manual:|https://docs.tdengine.com")
      fi
    else
      entries+=("To configure ${PREFIX}d:|edit ${configDir}/${configFile}")
      entries+=("To start ${PREFIX}d:|${sysctl_cmd} start ${serverName}")
      entries+=("To access ${productName} CLI:|${clientName} -h $serverFqdn")
      entries+=("To read the user manual:|https://docs.tdengine.com")
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
      ${sysctl_cmd} stop ${serverName} || :
    elif ((${service_mod} == 1)); then
      service ${serverName} stop || :
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
  if [ -z "$1" ]; then
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
# Call setup_env to initialize all directories and user mode settings
setup_env
# Determine installation type: server or client
if [ "$verType" == "client" ] && [ -e "${bin_dir}/${serverName}" ]; then
  echo -e "\033[44;32;1mThere is already an installed ${productName} server, so client installation is not needed!${NC}"
  exit 0
fi
# 
serverFqdn=$(hostname)
if [ "$verType" == "server" ]; then
  if [ -x "${script_dir}/${xname}/bin/${xname}" ]; then
    check_java_env
  fi
  # Check default 2.x data file.
  if [ -x "${dataDir}/dnode/dnodeCfg.json" ]; then
    echo -e "\033[44;31;5mThe default data directory ${dataDir} contains old data of ${productName} 2.x, please clear it before installing!\033[0m"
  else
    # Install server and client
    if [ -x "${bin_dir}/${serverName}" ]; then
      update_flag=1
      updateProduct
    else
      installProduct
    fi
  fi
  echo "${install_main_dir}" > "${install_main_dir}/.install_path"
elif [ "$verType" == "client" ]; then
  interactiveFqdn=no
  # Only install client
  if [ -x "${bin_dir}/${clientName}" ]; then
    update_flag=1
    updateProduct client
  else
    installProduct client
  fi
  echo "${install_main_dir}" > "${install_main_dir}/.install_path"
else
  echo "please input correct verType"
fi
