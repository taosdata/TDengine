#!/bin/bash
#
# This file is used to install TAOS time-series database on linux systems. The operating system
# is required to use systemd to manage services at boot

set -e
#set -x

# -----------------------Variables definition
source_dir=$1
binary_dir=$2
osType=$3
verNumber=$4

if [ "$osType" != "Darwin" ]; then
  script_dir=$(dirname $(readlink -f "$0"))
else
  script_dir=${source_dir}/packaging/tools
fi

# Dynamic directory
clientName="taos"
serverName="taosd"
logDir="/var/log/taos"
dataDir="/var/lib/taos"
configDir="/etc/taos"
configFile="taos.cfg"
installDir="/usr/local/taos"
productName="TDengine"
emailName="taosdata.com"
uninstallScript="rmtaos"

if [ "$osType" != "Darwin" ]; then
  data_dir=${dataDir}
  log_dir=${logDir}

  cfg_install_dir=${configDir}

  bin_link_dir="/usr/bin"
  lib_link_dir="/usr/lib"
  lib64_link_dir="/usr/lib64"
  inc_link_dir="/usr/include"

  install_main_dir=${installDir}

  bin_dir="${installDir}/bin"
else
  data_dir="/usr/local${dataDir}"
  log_dir="/usr/local${logDir}"

  cfg_install_dir="/usr/local${configDir}"

  bin_link_dir="/usr/local/bin"
  lib_link_dir="/usr/local/lib"
  inc_link_dir="/usr/local/include"

  install_main_dir="/usr/local/Cellar/tdengine/${verNumber}"
  install_main_2_dir="/usr/local/Cellar/tdengine@${verNumber}/${verNumber}"

  bin_dir="/usr/local/Cellar/tdengine/${verNumber}/bin"
  bin_2_dir="/usr/local/Cellar/tdengine@${verNumber}/${verNumber}/bin"
fi

service_config_dir="/etc/systemd/system"

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

csudo=""

service_mod=2
os_type=0

if [ "$osType" != "Darwin" ]; then
  if command -v sudo >/dev/null; then
    csudo="sudo "
  fi
  initd_mod=0
  if pidof systemd &>/dev/null; then
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
  osinfo=$(cat /etc/os-release | grep "NAME" | cut -d '"' -f2)
  #echo "osinfo: ${osinfo}"
  if echo $osinfo | grep -qwi "ubuntu"; then
    echo "this is ubuntu system"
    os_type=1
  elif echo $osinfo | grep -qwi "debian"; then
    echo "this is debian system"
    os_type=1
  elif echo $osinfo | grep -qwi "Kylin"; then
    echo "this is Kylin system"
    os_type=1
  elif echo $osinfo | grep -qwi "centos"; then
    echo "this is centos system"
    os_type=2
  elif echo $osinfo | grep -qwi "fedora"; then
    echo "this is fedora system"
    os_type=2
  else
    echo "${osinfo}: This is an officially unverified linux system, If there are any problems with the installation and operation, "
    echo "please feel free to contact ${emailName} for support."
    os_type=1
  fi
fi

function kill_taosadapter() {
  pid=$(ps -ef | grep "taosadapter" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo}kill -9 $pid || :
  fi
}

function kill_taosd() {
  ps -ef | grep ${serverName}
  pid=$(ps -ef | grep -w ${serverName} | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo}kill -9 $pid || :
  fi
}

function install_main_path() {
  #create install main dir and all sub dir
  if [ "$osType" != "Darwin" ]; then
    ${csudo}rm -rf ${install_main_dir} || :
    ${csudo}mkdir -p ${install_main_dir}
    ${csudo}mkdir -p ${install_main_dir}/cfg
    ${csudo}mkdir -p ${install_main_dir}/bin
    #    ${csudo}mkdir -p ${install_main_dir}/connector
    ${csudo}mkdir -p ${install_main_dir}/driver
    ${csudo}mkdir -p ${install_main_dir}/examples
    ${csudo}mkdir -p ${install_main_dir}/include
    #    ${csudo}mkdir -p ${install_main_dir}/init.d
  else
    ${csudo}rm -rf ${install_main_dir} || ${csudo}rm -rf ${install_main_2_dir} || :
    ${csudo}mkdir -p ${install_main_dir} || ${csudo}mkdir -p ${install_main_2_dir}
    ${csudo}mkdir -p ${install_main_dir}/cfg || ${csudo}mkdir -p ${install_main_2_dir}/cfg
    ${csudo}mkdir -p ${install_main_dir}/bin || ${csudo}mkdir -p ${install_main_2_dir}/bin
    #    ${csudo}mkdir -p ${install_main_dir}/connector || ${csudo}mkdir -p ${install_main_2_dir}/connector
    ${csudo}mkdir -p ${install_main_dir}/driver || ${csudo}mkdir -p ${install_main_2_dir}/driver
    ${csudo}mkdir -p ${install_main_dir}/examples || ${csudo}mkdir -p ${install_main_2_dir}/examples
    ${csudo}mkdir -p ${install_main_dir}/include || ${csudo}mkdir -p ${install_main_2_dir}/include
  fi
}

function install_bin() {
  # Remove links
  ${csudo}rm -f ${bin_link_dir}/${clientName} || :
  ${csudo}rm -f ${bin_link_dir}/${serverName} || :
  ${csudo}rm -f ${bin_link_dir}/taosadapter || :
  ${csudo}rm -f ${bin_link_dir}/taosdemo || :
  ${csudo}rm -f ${bin_link_dir}/taosdump || :

  if [ "$osType" != "Darwin" ]; then
    ${csudo}rm -f ${bin_link_dir}/perfMonitor || :
    ${csudo}rm -f ${bin_link_dir}/set_core || :
    ${csudo}rm -f ${bin_link_dir}/${uninstallScript} || :

    ${csudo}cp -r ${binary_dir}/build/bin/${clientName} ${install_main_dir}/bin || :
    [ -f ${binary_dir}/build/bin/taosBenchmark ] && ${csudo}cp -r ${binary_dir}/build/bin/taosBenchmark ${install_main_dir}/bin || :
    [ -f ${install_main_dir}/bin/taosBenchmark ] && ${csudo}ln -sf ${install_main_dir}/bin/taosBenchmark ${install_main_dir}/bin/taosdemo || :
    [ -f ${binary_dir}/build/bin/taosdump ] && ${csudo}cp -r ${binary_dir}/build/bin/taosdump ${install_main_dir}/bin || :
    [ -f ${binary_dir}/build/bin/taosadapter ] && ${csudo}cp -r ${binary_dir}/build/bin/taosadapter ${install_main_dir}/bin || :
    ${csudo}cp -r ${binary_dir}/build/bin/${serverName} ${install_main_dir}/bin || :
    ${csudo}cp -r ${binary_dir}/build/bin/tarbitrator ${install_main_dir}/bin || :

    ${csudo}cp -r ${script_dir}/taosd-dump-cfg.gdb ${install_main_dir}/bin
    ${csudo}cp -r ${script_dir}/remove.sh ${install_main_dir}/bin
    ${csudo}cp -r ${script_dir}/set_core.sh ${install_main_dir}/bin
    ${csudo}cp -r ${script_dir}/startPre.sh ${install_main_dir}/bin

    ${csudo}chmod 0555 ${install_main_dir}/bin/*
    #Make link
    [ -x ${install_main_dir}/bin/${clientName} ] && ${csudo}ln -s ${install_main_dir}/bin/${clientName} ${bin_link_dir}/${clientName} || :
    [ -x ${install_main_dir}/bin/${serverName} ] && ${csudo}ln -s ${install_main_dir}/bin/${serverName} ${bin_link_dir}/${serverName} || :
    [ -x ${install_main_dir}/bin/taosadapter ] && ${csudo}ln -s ${install_main_dir}/bin/taosadapter ${bin_link_dir}/taosadapter || :
    [ -x ${install_main_dir}/bin/taosdump ] && ${csudo}ln -s ${install_main_dir}/bin/taosdump ${bin_link_dir}/taosdump || :
    [ -x ${install_main_dir}/bin/taosdemo ] && ${csudo}ln -s ${install_main_dir}/bin/taosdemo ${bin_link_dir}/taosdemo || :
    [ -x ${install_main_dir}/bin/perfMonitor ] && ${csudo}ln -s ${install_main_dir}/bin/perfMonitor ${bin_link_dir}/perfMonitor || :
    [ -x ${install_main_dir}/set_core.sh ] && ${csudo}ln -s ${install_main_dir}/bin/set_core.sh ${bin_link_dir}/set_core || :
    [ -x ${install_main_dir}/bin/remove.sh ] && ${csudo}ln -s ${install_main_dir}/bin/remove.sh ${bin_link_dir}/${uninstallScript} || :
  else

    ${csudo}cp -r ${binary_dir}/build/bin/* ${install_main_dir}/bin || ${csudo}cp -r ${binary_dir}/build/bin/* ${install_main_2_dir}/bin || :
    ${csudo}cp -r ${script_dir}/taosd-dump-cfg.gdb ${install_main_dir}/bin || ${csudo}cp -r ${script_dir}/taosd-dump-cfg.gdb ${install_main_2_dir} || :
    ${csudo}cp -r ${script_dir}/remove_client.sh ${install_main_dir}/bin || ${csudo}cp -r ${script_dir}/remove_client.sh ${install_main_2_dir}/bin
    ${csudo}chmod 0555 ${install_main_dir}/bin/* || ${csudo}chmod 0555 ${install_main_2_dir}/bin/*
    #Make link
    [ -x ${install_main_dir}/bin/${clientName} ] || [ -x ${install_main_2_dir}/bin/${clientName} ] && ${csudo}ln -s ${install_main_dir}/bin/${clientName} ${bin_link_dir}/${clientName} || ${csudo}ln -s ${install_main_2_dir}/bin/${clientName} || :
    [ -x ${install_main_dir}/bin/${serverName} ] || [ -x ${install_main_2_dir}/bin/${serverName} ] && ${csudo}ln -s ${install_main_dir}/bin/${serverName} ${bin_link_dir}/${serverName} || ${csudo}ln -s ${install_main_2_dir}/bin/${serverName} || :
    [ -x ${install_main_dir}/bin/taosadapter ] || [ -x ${install_main_2_dir}/bin/taosadapter ] && ${csudo}ln -s ${install_main_dir}/bin/taosadapter ${bin_link_dir}/taosadapter || ${csudo}ln -s ${install_main_2_dir}/bin/taosadapter || :
    [ -x ${install_main_dir}/bin/taosdump ] || [ -x ${install_main_2_dir}/bin/taosdump ] && ${csudo}ln -s ${install_main_dir}/bin/taosdump ${bin_link_dir}/taosdump || ln -s ${install_main_2_dir}/bin/taosdump ${bin_link_dir}/taosdump || :
    [ -x ${install_main_dir}/bin/taosdemo ] || [ -x ${install_main_2_dir}/bin/taosdemo ] && ${csudo}ln -s ${install_main_dir}/bin/taosdemo ${bin_link_dir}/taosdemo || ln -s ${install_main_2_dir}/bin/taosdemo ${bin_link_dir}/taosdemo || :
  fi
}

function install_jemalloc() {
  if [ "$osType" != "Darwin" ]; then
    /usr/bin/install -c -d /usr/local/bin

    if [ -f "${binary_dir}/build/bin/jemalloc-config" ]; then
      ${csudo}/usr/bin/install -c -m 755 ${binary_dir}/build/bin/jemalloc-config /usr/local/bin
    fi
    if [ -f "${binary_dir}/build/bin/jemalloc.sh" ]; then
      ${csudo}/usr/bin/install -c -m 755 ${binary_dir}/build/bin/jemalloc.sh /usr/local/bin
    fi
    if [ -f "${binary_dir}/build/bin/jeprof" ]; then
      ${csudo}/usr/bin/install -c -m 755 ${binary_dir}/build/bin/jeprof /usr/local/bin
    fi
    if [ -f "${binary_dir}/build/include/jemalloc/jemalloc.h" ]; then
      ${csudo}/usr/bin/install -c -d /usr/local/include/jemalloc
      ${csudo}/usr/bin/install -c -m 644 ${binary_dir}/build/include/jemalloc/jemalloc.h \
        /usr/local/include/jemalloc
    fi
    if [ -f "${binary_dir}/build/lib/libjemalloc.so.2" ]; then
      ${csudo}/usr/bin/install -c -d /usr/local/lib
      ${csudo}/usr/bin/install -c -m 755 ${binary_dir}/build/lib/libjemalloc.so.2 /usr/local/lib
      ${csudo}ln -sf libjemalloc.so.2 /usr/local/lib/libjemalloc.so
      ${csudo}/usr/bin/install -c -d /usr/local/lib
      [ -f ${binary_dir}/build/lib/libjemalloc.a ] &&
        ${csudo}/usr/bin/install -c -m 755 ${binary_dir}/build/lib/libjemalloc.a /usr/local/lib
      [ -f ${binary_dir}/build/lib/libjemalloc_pic.a ] &&
        ${csudo}/usr/bin/install -c -m 755 ${binary_dir}/build/lib/libjemalloc_pic.a /usr/local/lib
      if [ -f "${binary_dir}/build/lib/pkgconfig/jemalloc.pc" ]; then
        ${csudo}/usr/bin/install -c -d /usr/local/lib/pkgconfig
        ${csudo}/usr/bin/install -c -m 644 ${binary_dir}/build/lib/pkgconfig/jemalloc.pc \
          /usr/local/lib/pkgconfig
      fi
      if [ -d /etc/ld.so.conf.d ]; then
        echo "/usr/local/lib" | ${csudo}tee /etc/ld.so.conf.d/jemalloc.conf >/dev/null || echo -e "failed to write /etc/ld.so.conf.d/jemalloc.conf"
        ${csudo}ldconfig
      else
        echo "/etc/ld.so.conf.d not found!"
      fi
    fi
    if [ -f "${binary_dir}/build/share/doc/jemalloc/jemalloc.html" ]; then
      ${csudo}/usr/bin/install -c -d /usr/local/share/doc/jemalloc
      ${csudo}/usr/bin/install -c -m 644 ${binary_dir}/build/share/doc/jemalloc/jemalloc.html \
        /usr/local/share/doc/jemalloc
    fi
    if [ -f "${binary_dir}/build/share/man/man3/jemalloc.3" ]; then
      ${csudo}/usr/bin/install -c -d /usr/local/share/man/man3
      ${csudo}/usr/bin/install -c -m 644 ${binary_dir}/build/share/man/man3/jemalloc.3 \
        /usr/local/share/man/man3
    fi
  fi
}

function install_avro() {
  if [ "$osType" != "Darwin" ]; then
    if [ -f "${binary_dir}/build/$1/libavro.so.23.0.0" ] && [ -d /usr/local/$1 ]; then
      ${csudo}/usr/bin/install -c -d /usr/local/$1
      ${csudo}/usr/bin/install -c -m 755 ${binary_dir}/build/$1/libavro.so.23.0.0 /usr/local/$1
      ${csudo}ln -sf libavro.so.23.0.0 /usr/local/$1/libavro.so.23
      ${csudo}ln -sf libavro.so.23 /usr/local/$1/libavro.so
      ${csudo}/usr/bin/install -c -d /usr/local/$1
      [ -f ${binary_dir}/build/$1/libavro.a ] &&
        ${csudo}/usr/bin/install -c -m 755 ${binary_dir}/build/$1/libavro.a /usr/local/$1

      if [ -d /etc/ld.so.conf.d ]; then
        echo "/usr/local/$1" | ${csudo}tee /etc/ld.so.conf.d/libavro.conf >/dev/null || echo -e "failed to write /etc/ld.so.conf.d/libavro.conf"
        ${csudo}ldconfig
      else
        echo "/etc/ld.so.conf.d not found!"
      fi
    fi
  fi
}

function install_lib() {
  # Remove links
  ${csudo}rm -f ${lib_link_dir}/libtaos.* || :
  ${csudo}rm -f ${lib_link_dir}/libtaosws.* || :
  if [ "$osType" != "Darwin" ]; then
    ${csudo}rm -f ${lib64_link_dir}/libtaos.* || :
    ${csudo}rm -f ${lib64_link_dir}/libtaosws.* || :
  fi

  if [ "$osType" != "Darwin" ]; then
    ${csudo}cp ${binary_dir}/build/lib/libtaos.so.${verNumber} \
      ${install_main_dir}/driver &&
      ${csudo}chmod 777 ${install_main_dir}/driver/libtaos.so.${verNumber}

    ${csudo}ln -sf ${install_main_dir}/driver/libtaos.* ${lib_link_dir}/libtaos.so.1
    ${csudo}ln -sf ${lib_link_dir}/libtaos.so.1 ${lib_link_dir}/libtaos.so

    if [ -d "${lib64_link_dir}" ]; then
      ${csudo}ln -sf ${install_main_dir}/driver/libtaos.* ${lib64_link_dir}/libtaos.so.1
      ${csudo}ln -sf ${lib64_link_dir}/libtaos.so.1 ${lib64_link_dir}/libtaos.so
    fi

    if [ -f ${binary_dir}/build/lib/libtaosws.so ]; then
      ${csudo}cp ${binary_dir}/build/lib/libtaosws.so \
            ${install_main_dir}/driver &&
            ${csudo}chmod 777 ${install_main_dir}/driver/libtaosws.so ||:
      ${csudo}ln -sf ${install_main_dir}/driver/libtaosws.so ${lib_link_dir}/libtaosws.so || :
      if [ -d "${lib64_link_dir}" ]; then
        ${csudo}ln -sf ${lib64_link_dir}/libtaosws.so ${lib64_link_dir}/libtaosws.so || :
      fi
    fi
  else
    ${csudo}cp -Rf ${binary_dir}/build/lib/libtaos.${verNumber}.dylib \
      ${install_main_dir}/driver ||
      ${csudo}cp -Rf ${binary_dir}/build/lib/libtaos.${verNumber}.dylib \
        ${install_main_2_dir}/driver &&
      ${csudo}chmod 777 ${install_main_dir}/driver/* ||
      ${csudo}chmod 777 ${install_main_2_dir}/driver/*

    ${csudo}ln -sf ${install_main_dir}/driver/libtaos.* \
      ${install_main_dir}/driver/libtaos.1.dylib ||
      ${csudo}ln -sf ${install_main_2_dir}/driver/libtaos.* \
        ${install_main_2_dir}/driver/libtaos.1.dylib || :

    ${csudo}ln -sf ${install_main_dir}/driver/libtaos.1.dylib \
      ${install_main_dir}/driver/libtaos.dylib ||
      ${csudo}ln -sf ${install_main_2_dir}/driver/libtaos.1.dylib \
        ${install_main_2_dir}/driver/libtaos.dylib || :

    ${csudo}ln -sf ${install_main_dir}/driver/libtaos.${verNumber}.dylib \
      ${lib_link_dir}/libtaos.1.dylib ||
      ${csudo}ln -sf ${install_main_2_dir}/driver/libtaos.${verNumber}.dylib \
        ${lib_link_dir}/libtaos.1.dylib || :

    ${csudo}ln -sf ${lib_link_dir}/libtaos.1.dylib ${lib_link_dir}/libtaos.dylib || :
  fi

  install_jemalloc
  #install_avro lib
  #install_avro lib64

  if [ "$osType" != "Darwin" ]; then
    ${csudo}ldconfig
  fi
}

function install_header() {

  if [ "$osType" != "Darwin" ]; then
    ${csudo}rm -f ${inc_link_dir}/taos.h ${inc_link_dir}/taosdef.h ${inc_link_dir}/taoserror.h || :
    ${csudo}rm -f ${inc_link_dir}/taosws.h || :

    ${csudo}cp -f ${source_dir}/src/inc/taos.h ${source_dir}/src/inc/taosdef.h ${source_dir}/src/inc/taoserror.h \
      ${install_main_dir}/include && ${csudo}chmod 644 ${install_main_dir}/include/*

    if [ -f ${binary_dir}/build/include/taosws.h ]; then
      ${csudo}cp -f ${binary_dir}/build/include/taosws.h ${install_main_dir}/include && ${csudo}chmod 644 ${install_main_dir}/include/taosws.h ||:
      ${csudo}ln -sf ${install_main_dir}/include/taosws.h ${inc_link_dir}/taosws.h ||:
    fi

    ${csudo}ln -s ${install_main_dir}/include/taos.h ${inc_link_dir}/taos.h
    ${csudo}ln -s ${install_main_dir}/include/taosdef.h ${inc_link_dir}/taosdef.h
    ${csudo}ln -s ${install_main_dir}/include/taoserror.h ${inc_link_dir}/taoserror.h

  else
    ${csudo}cp -f ${source_dir}/src/inc/taos.h ${source_dir}/src/inc/taosdef.h ${source_dir}/src/inc/taoserror.h \
      ${install_main_dir}/include ||
      ${csudo}cp -f ${source_dir}/src/inc/taos.h ${source_dir}/src/inc/taosdef.h ${source_dir}/src/inc/taoserror.h \
        ${install_main_2_dir}/include &&
      ${csudo}chmod 644 ${install_main_dir}/include/* ||
      ${csudo}chmod 644 ${install_main_2_dir}/include/*
  fi
}

function install_config() {
  if [ ! -f ${cfg_install_dir}/${configFile} ]; then
    ${csudo}mkdir -p ${cfg_install_dir}
    [ -f ${script_dir}/../cfg/${configFile} ] &&
      ${csudo}cp ${script_dir}/../cfg/${configFile} ${cfg_install_dir}
    ${csudo}chmod 644 ${cfg_install_dir}/${configFile}
    ${csudo}cp -f ${script_dir}/../cfg/${configFile} \
      ${cfg_install_dir}/${configFile}.${verNumber}
    ${csudo}ln -s ${cfg_install_dir}/${configFile} \
      ${install_main_dir}/cfg/${configFile}
  else
    ${csudo}cp -f ${script_dir}/../cfg/${configFile} \
      ${cfg_install_dir}/${configFile}.${verNumber}
  fi
}

function install_taosadapter_config() {
  if [ ! -f "${cfg_install_dir}/taosadapter.toml" ]; then
    ${csudo}mkdir -p ${cfg_install_dir}
    [ -f ${binary_dir}/test/cfg/taosadapter.toml ] &&
      ${csudo}cp ${binary_dir}/test/cfg/taosadapter.toml ${cfg_install_dir}
    [ -f ${cfg_install_dir}/taosadapter.toml ] &&
      ${csudo}chmod 644 ${cfg_install_dir}/taosadapter.toml
    [ -f ${binary_dir}/test/cfg/taosadapter.toml ] &&
      ${csudo}cp -f ${binary_dir}/test/cfg/taosadapter.toml \
        ${cfg_install_dir}/taosadapter.toml.${verNumber}
    [ -f ${cfg_install_dir}/taosadapter.toml ] &&
      ${csudo}ln -s ${cfg_install_dir}/taosadapter.toml \
        ${install_main_dir}/cfg/taosadapter.toml
  else
    if [ -f "${binary_dir}/test/cfg/taosadapter.toml" ]; then
      ${csudo}cp -f ${binary_dir}/test/cfg/taosadapter.toml \
        ${cfg_install_dir}/taosadapter.toml.${verNumber}
    fi
  fi
}

function install_log() {
  ${csudo}rm -rf ${log_dir} || :
  ${csudo}mkdir -p ${log_dir} && ${csudo}chmod 777 ${log_dir}
  if [ "$osType" != "Darwin" ]; then
    ${csudo}ln -s ${log_dir} ${install_main_dir}/log
  else
    ${csudo}ln -s ${log_dir} ${install_main_dir}/log || ${csudo}ln -s ${log_dir} ${install_main_2_dir}/log
  fi
}

function install_data() {
  ${csudo}mkdir -p ${data_dir}
  if [ "$osType" != "Darwin" ]; then
    ${csudo}ln -s ${data_dir} ${install_main_dir}/data
  else
    ${csudo}ln -s ${data_dir} ${install_main_dir}/data || ${csudo}ln -s ${data_dir} ${install_main_2_dir}/data
  fi
}

function install_connector() {
  if find ${source_dir}/src/connector/go -mindepth 1 -maxdepth 1 | read; then
    ${csudo}cp -r ${source_dir}/src/connector/go ${install_main_dir}/connector
  else
    echo "WARNING: go connector not found, please check if want to use it!"
  fi
  if [ "$osType" != "Darwin" ]; then
    ${csudo}cp -rf ${source_dir}/src/connector/python ${install_main_dir}/connector
    ${csudo}cp ${binary_dir}/build/lib/*.jar ${install_main_dir}/connector &>/dev/null && ${csudo}chmod 777 ${install_main_dir}/connector/*.jar || echo &>/dev/null
  else
    ${csudo}cp -rf ${source_dir}/src/connector/python ${install_main_dir}/connector || ${csudo}cp -rf ${source_dir}/src/connector/python ${install_main_2_dir}/connector
    ${csudo}cp ${binary_dir}/build/lib/*.jar ${install_main_dir}/connector &>/dev/null && ${csudo}chmod 777 ${install_main_dir}/connector/*.jar || echo &>/dev/null
    ${csudo}cp ${binary_dir}/build/lib/*.jar ${install_main_2_dir}/connector &>/dev/null && ${csudo}chmod 777 ${install_main_2_dir}/connector/*.jar || echo &>/dev/null
  fi
}

function install_examples() {
  if [ "$osType" != "Darwin" ]; then
    ${csudo}cp -rf ${source_dir}/examples/* ${install_main_dir}/examples
  else
    ${csudo}cp -rf ${source_dir}/examples/* ${install_main_dir}/examples || ${csudo}cp -rf ${source_dir}/examples/* ${install_main_2_dir}/examples
  fi
}

function clean_service_on_sysvinit() {
  if pidof ${serverName} &>/dev/null; then
    ${csudo}service ${serverName} stop || :
  fi

  if ((${initd_mod} == 1)); then
    ${csudo}chkconfig --del ${serverName} || :
  elif ((${initd_mod} == 2)); then
    ${csudo}insserv -r ${serverName} || :
  elif ((${initd_mod} == 3)); then
    ${csudo}update-rc.d -f ${serverName} remove || :
  fi

  ${csudo}rm -f ${service_config_dir}/${serverName} || :

  if $(which init &>/dev/null); then
    ${csudo}init q || :
  fi
}

function install_service_on_sysvinit() {
  clean_service_on_sysvinit

  sleep 1

  if ((${os_type} == 1)); then
    #    ${csudo}cp -f ${script_dir}/../deb/${serverName} ${install_main_dir}/init.d
    ${csudo}cp ${script_dir}/../deb/${serverName} ${service_config_dir} && ${csudo}chmod a+x ${service_config_dir}/${serverName}
  elif ((${os_type} == 2)); then
    #    ${csudo}cp -f ${script_dir}/../rpm/${serverName} ${install_main_dir}/init.d
    ${csudo}cp ${script_dir}/../rpm/${serverName} ${service_config_dir} && ${csudo}chmod a+x ${service_config_dir}/${serverName}
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
}

function install_service_on_systemd() {
  clean_service_on_systemd

  taosd_service_config="${service_config_dir}/${serverName}.service"

  ${csudo}bash -c "echo '[Unit]'                             >> ${taosd_service_config}"
  ${csudo}bash -c "echo 'Description=${productName} server service' >> ${taosd_service_config}"
  ${csudo}bash -c "echo 'After=network-online.target'        >> ${taosd_service_config}"
  ${csudo}bash -c "echo 'Wants=network-online.target'        >> ${taosd_service_config}"
  ${csudo}bash -c "echo                                      >> ${taosd_service_config}"
  ${csudo}bash -c "echo '[Service]'                          >> ${taosd_service_config}"
  ${csudo}bash -c "echo 'Type=simple'                        >> ${taosd_service_config}"
  ${csudo}bash -c "echo 'ExecStart=/usr/bin/${serverName}'           >> ${taosd_service_config}"
  ${csudo}bash -c "echo 'ExecStartPre=${installDir}/bin/startPre.sh'           >> ${taosd_service_config}"
  ${csudo}bash -c "echo 'TimeoutStopSec=1000000s'            >> ${taosd_service_config}"
  ${csudo}bash -c "echo 'LimitNOFILE=infinity'               >> ${taosd_service_config}"
  ${csudo}bash -c "echo 'LimitNPROC=infinity'                >> ${taosd_service_config}"
  ${csudo}bash -c "echo 'LimitCORE=infinity'                 >> ${taosd_service_config}"
  ${csudo}bash -c "echo 'TimeoutStartSec=0'                  >> ${taosd_service_config}"
  ${csudo}bash -c "echo 'StandardOutput=null'                >> ${taosd_service_config}"
  ${csudo}bash -c "echo 'Restart=always'                     >> ${taosd_service_config}"
  ${csudo}bash -c "echo 'StartLimitBurst=3'                  >> ${taosd_service_config}"
  ${csudo}bash -c "echo 'StartLimitInterval=60s'             >> ${taosd_service_config}"
  ${csudo}bash -c "echo                                      >> ${taosd_service_config}"
  ${csudo}bash -c "echo '[Install]'                          >> ${taosd_service_config}"
  ${csudo}bash -c "echo 'WantedBy=multi-user.target'         >> ${taosd_service_config}"
  ${csudo}systemctl enable ${serverName}
}

function install_taosadapter_service() {
  if ((${service_mod} == 0)); then
    [ -f ${binary_dir}/test/cfg/taosadapter.service ] &&
      ${csudo}cp ${binary_dir}/test/cfg/taosadapter.service \
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
    kill_taosd
  fi
}

function update_TDengine() {
  echo -e "${GREEN}Start to update ${productName}...${NC}"
  # Stop the service if running

  if pidof ${serverName} &>/dev/null; then
    if ((${service_mod} == 0)); then
      ${csudo}systemctl stop ${serverName} || :
    elif ((${service_mod} == 1)); then
      ${csudo}service ${serverName} stop || :
    else
      kill_taosadapter
      kill_taosd
    fi
    sleep 1
  fi

  install_main_path

  install_log
  install_header
  install_lib
  #  install_connector
  install_examples
  install_bin

  install_service
  install_taosadapter_service

  install_config
  install_taosadapter_config

  echo
  echo -e "\033[44;32;1m${productName} is updated successfully!${NC}"
  echo

  echo -e "${GREEN_DARK}To configure ${productName} ${NC}: edit ${configDir}/${configFile}"
  echo -e "${GREEN_DARK}To configure Taos Adapter (if has) ${NC}: edit ${configDir}/taosadapter.toml"
  if ((${service_mod} == 0)); then
    echo -e "${GREEN_DARK}To start ${productName}     ${NC}: ${csudo}systemctl start ${serverName}${NC}"
  elif ((${service_mod} == 1)); then
    echo -e "${GREEN_DARK}To start ${productName}     ${NC}: ${csudo}service ${serverName} start${NC}"
  else
    echo -e "${GREEN_DARK}To start Taos Adapter (if has)${NC}: taosadapter &${NC}"
    echo -e "${GREEN_DARK}To start ${productName}     ${NC}: ${serverName}${NC}"
  fi

  echo -e "${GREEN_DARK}To access ${productName}    ${NC}: use ${GREEN_UNDERLINE}${clientName}${NC} in shell${NC}"
  echo
  echo -e "\033[44;32;1m${productName} is updated successfully!${NC}"
}

function install_TDengine() {
  # Start to install
  echo -e "${GREEN}Start to install ${productName}...${NC}"

  install_main_path

  install_data
  install_log
  install_header
  install_lib
  #  install_connector
  install_examples
  install_bin

  install_service
  install_taosadapter_service

  install_config
  install_taosadapter_config

  # Ask if to start the service
  echo
  echo -e "\033[44;32;1m${productName} is installed successfully!${NC}"
  echo
  echo -e "${GREEN_DARK}To configure ${productName} ${NC}: edit ${configDir}/${configFile}"
  echo -e "${GREEN_DARK}To configure taosadapter (if has) ${NC}: edit ${configDir}/taosadapter.toml"
  if ((${service_mod} == 0)); then
    echo -e "${GREEN_DARK}To start ${productName}     ${NC}: ${csudo}systemctl start ${serverName}${NC}"
  elif ((${service_mod} == 1)); then
    echo -e "${GREEN_DARK}To start ${productName}    ${NC}: ${csudo}service ${serverName} start${NC}"
  else
    echo -e "${GREEN_DARK}To start Taos Adapter (if has)${NC}: taosadapter &${NC}"
    echo -e "${GREEN_DARK}To start ${productName}     ${NC}: ./${serverName}${NC}"
  fi

  echo -e "${GREEN_DARK}To access ${productName}    ${NC}: use ${GREEN_UNDERLINE}${clientName}${NC} in shell${NC}"
  echo
  echo -e "\033[44;32;1m${productName} is installed successfully!${NC}"
}

## ==============================Main program starts from here============================
echo source directory: $1
echo binary directory: $2
if [ "$osType" != "Darwin" ]; then
  if [ -x ${bin_dir}/${clientName} ]; then
    update_TDengine
  else
    install_TDengine
  fi
else
  if [ -x ${bin_dir}/${clientName} ] || [ -x ${bin_2_dir}/${clientName} ]; then
    update_TDengine
  else
    install_TDengine
  fi
fi
