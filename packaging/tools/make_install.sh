#!/bin/sh
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

data_dir=${dataDir}
log_dir=${logDir}
cfg_install_dir=${configDir}

if [ "$osType" != "Darwin" ]; then
  bin_link_dir="/usr/bin"
  lib_link_dir="/usr/lib"
  lib64_link_dir="/usr/lib64"
  inc_link_dir="/usr/include"
else
  bin_link_dir="/usr/local/bin"
  lib_link_dir="/usr/local/lib"
  inc_link_dir="/usr/local/include"

  if [ -d "/usr/local/Cellar/" ];then
    installDir="/usr/local/Cellar/tdengine/${verNumber}"
  elif [ -d "/opt/homebrew/Cellar/" ];then
    installDir="/opt/homebrew/Cellar/tdengine/${verNumber}"
  else
    installDir="/usr/local/taos"
  fi
fi
install_main_dir=${installDir}
bin_dir="${installDir}/bin"
cfg_dir="${installDir}/cfg"

service_config_dir="/etc/systemd/system"

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

csudo=""
csudouser=""
if command -v sudo >/dev/null; then
  csudo="sudo "
  csudouser="sudo -u ${USER} "
fi

service_mod=2
os_type=0

if [ "$osType" != "Darwin" ]; then
  initd_mod=0
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
  pid=$(ps -ef | grep -w ${serverName} | grep -v "grep" | awk '{print $2}')
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
  #    ${csudo}mkdir -p ${install_main_dir}/connector
  ${csudo}mkdir -p ${install_main_dir}/driver
  ${csudo}mkdir -p ${install_main_dir}/examples
  ${csudo}mkdir -p ${install_main_dir}/include
  ${csudo}mkdir -p ${install_main_dir}/share
  #    ${csudo}mkdir -p ${install_main_dir}/init.d
}

function install_bin() {
  # Remove links
  ${csudo}rm -f ${bin_link_dir}/${clientName} || :
  ${csudo}rm -f ${bin_link_dir}/${serverName} || :
  ${csudo}rm -f ${bin_link_dir}/taosadapter || :
  ${csudo}rm -f ${bin_link_dir}/udfd || :
  ${csudo}rm -f ${bin_link_dir}/taosdemo || :
  ${csudo}rm -f ${bin_link_dir}/taosdump || :
  ${csudo}rm -f ${bin_link_dir}/${uninstallScript} || :

  if [ "$osType" != "Darwin" ]; then
    ${csudo}rm -f ${bin_link_dir}/perfMonitor || :
    ${csudo}rm -f ${bin_link_dir}/set_core || :

    ${csudo}cp -r ${binary_dir}/build/bin/${clientName} ${install_main_dir}/bin || :
    [ -f ${binary_dir}/build/bin/taosBenchmark ] && ${csudo}cp -r ${binary_dir}/build/bin/taosBenchmark ${install_main_dir}/bin || :
    [ -f ${install_main_dir}/bin/taosBenchmark ] && ${csudo}ln -sf ${install_main_dir}/bin/taosBenchmark ${install_main_dir}/bin/taosdemo > /dev/null 2>&1 || :
    [ -f ${binary_dir}/build/bin/taosdump ] && ${csudo}cp -r ${binary_dir}/build/bin/taosdump ${install_main_dir}/bin || :
    [ -f ${binary_dir}/build/bin/taosadapter ] && ${csudo}cp -r ${binary_dir}/build/bin/taosadapter ${install_main_dir}/bin || :
    [ -f ${binary_dir}/build/bin/udfd ] && ${csudo}cp -r ${binary_dir}/build/bin/udfd ${install_main_dir}/bin || :
    [ -f ${binary_dir}/build/bin/taosx ] && ${csudo}cp -r ${binary_dir}/build/bin/taosx ${install_main_dir}/bin || :
    ${csudo}cp -r ${binary_dir}/build/bin/${serverName} ${install_main_dir}/bin || :

    ${csudo}cp -r ${script_dir}/taosd-dump-cfg.gdb ${install_main_dir}/bin || :
    ${csudo}cp -r ${script_dir}/remove.sh ${install_main_dir}/bin || :
    ${csudo}cp -r ${script_dir}/set_core.sh ${install_main_dir}/bin || :
    ${csudo}cp -r ${script_dir}/startPre.sh ${install_main_dir}/bin || :

    ${csudo}chmod 0555 ${install_main_dir}/bin/*
    #Make link
    [ -x ${install_main_dir}/bin/${clientName} ] && ${csudo}ln -s ${install_main_dir}/bin/${clientName} ${bin_link_dir}/${clientName} > /dev/null 2>&1 || :
    [ -x ${install_main_dir}/bin/${serverName} ] && ${csudo}ln -s ${install_main_dir}/bin/${serverName} ${bin_link_dir}/${serverName} > /dev/null 2>&1 || :
    [ -x ${install_main_dir}/bin/taosadapter ] && ${csudo}ln -s ${install_main_dir}/bin/taosadapter ${bin_link_dir}/taosadapter > /dev/null 2>&1 || :
    [ -x ${install_main_dir}/bin/udfd ] && ${csudo}ln -s ${install_main_dir}/bin/udfd ${bin_link_dir}/udfd > /dev/null 2>&1 || :
    [ -x ${install_main_dir}/bin/taosdump ] && ${csudo}ln -s ${install_main_dir}/bin/taosdump ${bin_link_dir}/taosdump > /dev/null 2>&1 || :
    [ -x ${install_main_dir}/bin/taosdemo ] && ${csudo}ln -s ${install_main_dir}/bin/taosdemo ${bin_link_dir}/taosdemo > /dev/null 2>&1 || :
    [ -x ${install_main_dir}/bin/taosx ] && ${csudo}ln -s ${install_main_dir}/bin/taosx ${bin_link_dir}/taosx > /dev/null 2>&1 || :
    [ -x ${install_main_dir}/bin/perfMonitor ] && ${csudo}ln -s ${install_main_dir}/bin/perfMonitor ${bin_link_dir}/perfMonitor > /dev/null 2>&1 || :
    [ -x ${install_main_dir}/set_core.sh ] && ${csudo}ln -s ${install_main_dir}/bin/set_core.sh ${bin_link_dir}/set_core > /dev/null 2>&1 || :
    [ -x ${install_main_dir}/bin/remove.sh ] && ${csudo}ln -s ${install_main_dir}/bin/remove.sh ${bin_link_dir}/${uninstallScript} > /dev/null 2>&1 || :
  else

    ${csudo}cp -r ${binary_dir}/build/bin/${clientName} ${install_main_dir}/bin || :
    [ -f ${binary_dir}/build/bin/taosBenchmark ] && ${csudo}cp -r ${binary_dir}/build/bin/taosBenchmark ${install_main_dir}/bin || :
    [ -f ${install_main_dir}/bin/taosBenchmark ] && ${csudo}ln -sf ${install_main_dir}/bin/taosBenchmark ${install_main_dir}/bin/taosdemo > /dev/null 2>&1 || :
    [ -f ${binary_dir}/build/bin/taosdump ] && ${csudo}cp -r ${binary_dir}/build/bin/taosdump ${install_main_dir}/bin || :
    [ -f ${binary_dir}/build/bin/taosadapter ] && ${csudo}cp -r ${binary_dir}/build/bin/taosadapter ${install_main_dir}/bin || :
    [ -f ${binary_dir}/build/bin/udfd ] && ${csudo}cp -r ${binary_dir}/build/bin/udfd ${install_main_dir}/bin || :
    [ -f ${binary_dir}/build/bin/taosx ] && ${csudo}cp -r ${binary_dir}/build/bin/taosx ${install_main_dir}/bin || :
    [ -f ${binary_dir}/build/bin/*explorer ] && ${csudo}cp -r ${binary_dir}/build/bin/*explorer ${install_main_dir}/bin || :
    ${csudo}cp -r ${binary_dir}/build/bin/${serverName} ${install_main_dir}/bin || :

    ${csudo}cp -r ${script_dir}/remove.sh ${install_main_dir}/bin || :
    ${csudo}chmod 0555 ${install_main_dir}/bin/*
    #Make link
    [ -x ${install_main_dir}/bin/${clientName} ] && ${csudo}ln -s ${install_main_dir}/bin/${clientName} ${bin_link_dir}/${clientName} > /dev/null 2>&1 || :
    [ -x ${install_main_dir}/bin/${serverName} ] && ${csudo}ln -s ${install_main_dir}/bin/${serverName} ${bin_link_dir}/${serverName} > /dev/null 2>&1 || :
    [ -x ${install_main_dir}/bin/taosadapter ] && ${csudo}ln -s ${install_main_dir}/bin/taosadapter ${bin_link_dir}/taosadapter > /dev/null 2>&1 || :
    [ -x ${install_main_dir}/bin/udfd ] && ${csudo}ln -s ${install_main_dir}/bin/udfd ${bin_link_dir}/udfd > /dev/null 2>&1 || :
    [ -x ${install_main_dir}/bin/taosdump ] && ${csudo}ln -s ${install_main_dir}/bin/taosdump ${bin_link_dir}/taosdump > /dev/null 2>&1 || :
    [ -f ${install_main_dir}/bin/taosBenchmark ] && ${csudo}ln -sf ${install_main_dir}/bin/taosBenchmark ${install_main_dir}/bin/taosdemo > /dev/null 2>&1 || :
    [ -x ${install_main_dir}/bin/taosx ] && ${csudo}ln -s ${install_main_dir}/bin/taosx ${bin_link_dir}/taosx > /dev/null 2>&1 || :
    [ -x ${install_main_dir}/bin/*explorer ] && ${csudo}ln -s ${install_main_dir}/bin/*explorer ${bin_link_dir}/*explorer > /dev/null 2>&1 || :
    [ -x ${install_main_dir}/bin/remove.sh ] && ${csudo}ln -s ${install_main_dir}/bin/remove.sh ${bin_link_dir}/${uninstallScript} > /dev/null 2>&1 || :
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
      ${csudo}ln -sf libjemalloc.so.2 /usr/local/lib/libjemalloc.so > /dev/null 2>&1
      ${csudo}/usr/bin/install -c -d /usr/local/lib
      # [ -f ${binary_dir}/build/lib/libjemalloc.a ] &&
      #   ${csudo}/usr/bin/install -c -m 755 ${binary_dir}/build/lib/libjemalloc.a /usr/local/lib
      # [ -f ${binary_dir}/build/lib/libjemalloc_pic.a ] &&
      #   ${csudo}/usr/bin/install -c -m 755 ${binary_dir}/build/lib/libjemalloc_pic.a /usr/local/lib
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
      ${csudo}ln -sf libavro.so.23.0.0 /usr/local/$1/libavro.so.23 > /dev/null 2>&1
      ${csudo}ln -sf libavro.so.23 /usr/local/$1/libavro.so > /dev/null 2>&1
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
  [ -f ${lib_link_dir}/libtaosws.so ] && ${csudo}rm -f ${lib_link_dir}/libtaosws.so || :
  if [ "$osType" != "Darwin" ]; then
    ${csudo}rm -f ${lib64_link_dir}/libtaos.* || :
    [ -f ${lib64_link_dir}/libtaosws.so ] && ${csudo}rm -f ${lib64_link_dir}/libtaosws.so || :
  fi

  if [ "$osType" != "Darwin" ]; then
    ${csudo}cp ${binary_dir}/build/lib/libtaos.so.${verNumber} \
      ${install_main_dir}/driver &&
      ${csudo}chmod 777 ${install_main_dir}/driver/libtaos.so.${verNumber}

    ${csudo}ln -sf ${install_main_dir}/driver/libtaos.* ${lib_link_dir}/libtaos.so.1 > /dev/null 2>&1
    ${csudo}ln -sf ${lib_link_dir}/libtaos.so.1 ${lib_link_dir}/libtaos.so > /dev/null 2>&1
    if [ -d "${lib64_link_dir}" ]; then
        ${csudo}ln -sf ${install_main_dir}/driver/libtaos.* ${lib64_link_dir}/libtaos.so.1 > /dev/null 2>&1
        ${csudo}ln -sf ${lib64_link_dir}/libtaos.so.1 ${lib64_link_dir}/libtaos.so > /dev/null 2>&1
    fi

    if [ -f ${binary_dir}/build/lib/libtaosws.so ]; then
        ${csudo}cp ${binary_dir}/build/lib/libtaosws.so \
            ${install_main_dir}/driver &&
            ${csudo}chmod 777 ${install_main_dir}/driver/libtaosws.so ||:

        ${csudo}ln -sf ${install_main_dir}/driver/libtaosws.so ${lib_link_dir}/libtaosws.so > /dev/null 2>&1 || :
    fi
  else
    ${csudo}cp -Rf ${binary_dir}/build/lib/libtaos.${verNumber}.dylib \
      ${install_main_dir}/driver && ${csudo}chmod 777 ${install_main_dir}/driver/*

    ${csudo}ln -sf ${install_main_dir}/driver/libtaos.${verNumber}.dylib \
      ${lib_link_dir}/libtaos.1.dylib > /dev/null 2>&1 || :

    ${csudo}ln -sf ${lib_link_dir}/libtaos.1.dylib ${lib_link_dir}/libtaos.dylib > /dev/null 2>&1 || :

    if [ -f ${binary_dir}/build/lib/libtaosws.dylib ]; then
        ${csudo}cp ${binary_dir}/build/lib/libtaosws.dylib \
            ${install_main_dir}/driver &&
            ${csudo}chmod 777 ${install_main_dir}/driver/libtaosws.dylib ||:

        ${csudo}ln -sf ${install_main_dir}/driver/libtaosws.dylib ${lib_link_dir}/libtaosws.dylib > /dev/null 2>&1 || :
    fi
  fi

  install_jemalloc
  #install_avro lib
  #install_avro lib64

  if [ "$osType" != "Darwin" ]; then
    ${csudo}ldconfig /etc/ld.so.conf.d
  fi
}

function install_header() {
  ${csudo}mkdir -p ${inc_link_dir}
  ${csudo}rm -f ${inc_link_dir}/taos.h ${inc_link_dir}/taosdef.h ${inc_link_dir}/taoserror.h ${inc_link_dir}/tdef.h ${inc_link_dir}/taosudf.h || :
  [ -f ${inc_link_dir}/taosws.h ] && ${csudo}rm -f ${inc_link_dir}/taosws.h ||:
  ${csudo}cp -f ${source_dir}/include/client/taos.h ${source_dir}/include/common/taosdef.h ${source_dir}/include/util/taoserror.h ${source_dir}/include/util/tdef.h ${source_dir}/include/libs/function/taosudf.h \
    ${install_main_dir}/include && ${csudo}chmod 644 ${install_main_dir}/include/*

  if [ -f ${binary_dir}/build/include/taosws.h ]; then
    ${csudo}cp -f ${binary_dir}/build/include/taosws.h ${install_main_dir}/include && ${csudo}chmod 644 ${install_main_dir}/include/taosws.h ||:
    ${csudo}ln -sf ${install_main_dir}/include/taosws.h ${inc_link_dir}/taosws.h > /dev/null 2>&1 ||:
  fi

  ${csudo}ln -s ${install_main_dir}/include/taos.h ${inc_link_dir}/taos.h > /dev/null 2>&1
  ${csudo}ln -s ${install_main_dir}/include/taosdef.h ${inc_link_dir}/taosdef.h > /dev/null 2>&1
  ${csudo}ln -s ${install_main_dir}/include/taoserror.h ${inc_link_dir}/taoserror.h > /dev/null 2>&1
  ${csudo}ln -s ${install_main_dir}/include/tdef.h ${inc_link_dir}/tdef.h > /dev/null 2>&1
  ${csudo}ln -s ${install_main_dir}/include/taosudf.h ${inc_link_dir}/taosudf.h > /dev/null 2>&1

  ${csudo}chmod 644 ${install_main_dir}/include/*
}

function install_config() {
  if [ ! -f ${cfg_install_dir}/${configFile} ]; then
    ${csudo}mkdir -p ${cfg_install_dir}
    [ -f ${script_dir}/../cfg/${configFile} ] &&
      ${csudo}cp ${script_dir}/../cfg/${configFile} ${cfg_install_dir} &&
      ${csudo}cp ${script_dir}/../cfg/${configFile} ${cfg_dir}
    ${csudo}chmod 644 ${cfg_install_dir}/${configFile}
    ${csudo}cp -f ${script_dir}/../cfg/${configFile} \
      ${cfg_install_dir}/${configFile}.${verNumber}
    ${csudo}ln -s ${cfg_install_dir}/${configFile} \
      ${install_main_dir}/cfg/${configFile} > /dev/null 2>&1
  else
    ${csudo}cp -f ${script_dir}/../cfg/${configFile} \
      ${cfg_install_dir}/${configFile}.${verNumber}
    ${csudo}cp -f ${script_dir}/../cfg/${configFile} ${cfg_dir}
  fi
}

function install_taosadapter_config() {
  if [ ! -f "${cfg_install_dir}/taosadapter.toml" ]; then
    ${csudo}mkdir -p ${cfg_install_dir} || :
    [ -f ${binary_dir}/test/cfg/taosadapter.toml ] &&
      ${csudo}cp ${binary_dir}/test/cfg/taosadapter.toml ${cfg_install_dir} &&
      ${csudo}cp ${binary_dir}/test/cfg/taosadapter.toml ${cfg_dir} || :
    [ -f ${cfg_install_dir}/taosadapter.toml ] &&
      ${csudo}chmod 644 ${cfg_install_dir}/taosadapter.toml || :
    [ -f ${binary_dir}/test/cfg/taosadapter.toml ] &&
      ${csudo}cp -f ${binary_dir}/test/cfg/taosadapter.toml \
        ${cfg_install_dir}/taosadapter.toml.${verNumber} || :
    [ -f ${cfg_install_dir}/taosadapter.toml ] &&
      ${csudo}ln -s ${cfg_install_dir}/taosadapter.toml \
        ${install_main_dir}/cfg/taosadapter.toml > /dev/null 2>&1 || :
  else
    if [ -f "${binary_dir}/test/cfg/taosadapter.toml" ]; then
      ${csudo}cp -f ${binary_dir}/test/cfg/taosadapter.toml \
        ${cfg_install_dir}/taosadapter.toml.${verNumber} || :
      ${csudo}cp -f ${binary_dir}/test/cfg/taosadapter.toml ${cfg_dir} || :
    fi
  fi
}

function install_log() {
  ${csudo}mkdir -p ${log_dir} && ${csudo}chmod 777 ${log_dir}
  ${csudo}ln -s ${log_dir} ${install_main_dir}/log > /dev/null 2>&1
}

function install_data() {
  ${csudo}mkdir -p ${data_dir} && ${csudo}chmod 777 ${data_dir}
  ${csudo}ln -s ${data_dir} ${install_main_dir}/data > /dev/null 2>&1
}

function install_connector() {
  if find ${source_dir}/src/connector/go -mindepth 1 -maxdepth 1 | read; then
    ${csudo}cp -r ${source_dir}/src/connector/go ${install_main_dir}/connector || :
  else
    echo "WARNING: go connector not found, please check if want to use it!"
  fi
  ${csudo}cp -rf ${source_dir}/src/connector/python ${install_main_dir}/connector || :
  ${csudo}cp ${binary_dir}/build/lib/*.jar ${install_main_dir}/connector &>/dev/null && ${csudo}chmod 777 ${install_main_dir}/connector/*.jar || echo &>/dev/null || :
}

function install_examples() {
  ${csudo}cp -rf ${source_dir}/examples/* ${install_main_dir}/examples || :
}

function clean_service_on_sysvinit() {
  if ps aux | grep -v grep | grep ${serverName} &>/dev/null; then
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

function install_service_on_launchctl() {
  ${csudo}launchctl unload -w /Library/LaunchDaemons/com.taosdata.taosd.plist > /dev/null 2>&1 || :
  ${csudo}cp ${script_dir}/com.taosdata.taosd.plist /Library/LaunchDaemons/com.taosdata.taosd.plist
  ${csudo}launchctl load -w /Library/LaunchDaemons/com.taosdata.taosd.plist > /dev/null 2>&1 || :

  ${csudo}launchctl unload -w /Library/LaunchDaemons/com.taosdata.taosadapter.plist > /dev/null 2>&1 || :
  ${csudo}cp ${script_dir}/com.taosdata.taosadapter.plist /Library/LaunchDaemons/com.taosdata.taosadapter.plist
  ${csudo}launchctl load -w /Library/LaunchDaemons/com.taosdata.taosadapter.plist > /dev/null 2>&1 || :
}

function install_service() {
  if [ "$osType" != "Darwin" ]; then
    if ((${service_mod} == 0)); then
      install_service_on_systemd
    elif ((${service_mod} == 1)); then
      install_service_on_sysvinit
    else
      kill_taosd
    fi
  else
    install_service_on_launchctl
  fi
}
function install_app() {
  if [ "$osType" = "Darwin" ]; then
    ${csudo}rm -rf /Applications/TDengine.app &&
      ${csudo}mkdir -p /Applications/TDengine.app/Contents/MacOS/ &&
      ${csudo}cp ${script_dir}/TDengine /Applications/TDengine.app/Contents/MacOS/ &&
      echo "<plist><dict></dict></plist>" | ${csudo}tee /Applications/TDengine.app/Contents/Info.plist > /dev/null &&
      ${csudo}sips -i ${script_dir}/logo.png > /dev/null && 
      DeRez -only icns ${script_dir}/logo.png | ${csudo}tee /Applications/TDengine.app/mac_logo.rsrc > /dev/null &&
      ${csudo}rez -append /Applications/TDengine.app/mac_logo.rsrc -o $'/Applications/TDengine.app/Icon\r' &&
      ${csudo}SetFile -a C /Applications/TDengine.app/ &&
      ${csudo}rm /Applications/TDengine.app/mac_logo.rsrc
  fi
}

function update_TDengine() {
  echo -e "${GREEN}Start to update ${productName}...${NC}"
  # Stop the service if running

  if ps aux | grep -v grep | grep ${serverName} &>/dev/null; then
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
  install_app

  install_service
  install_taosadapter_service

  install_config
  install_taosadapter_config

  echo
  echo -e "\033[44;32;1m${productName} is updated successfully!${NC}"
  echo

  echo -e "${GREEN_DARK}To configure ${productName} ${NC}: edit ${configDir}/${configFile}"
  [ -f ${configDir}/taosadapter.toml ] && [ -f ${installDir}/bin/taosadapter ] && \
    echo -e "${GREEN_DARK}To configure Adapter ${NC}: edit ${configDir}/taosadapter.toml"
  if ((${service_mod} == 0)); then
    echo -e "${GREEN_DARK}To start ${productName}     ${NC}: ${csudo}systemctl start ${serverName}${NC}"
    [ -f ${service_config_dir}/taosadapter.service ] && [ -f ${installDir}/bin/taosadapter ] && \
      echo -e "${GREEN_DARK}To start Adapter ${NC}: ${csudo}systemctl start taosadapter ${NC}"
  elif ((${service_mod} == 1)); then
    echo -e "${GREEN_DARK}To start ${productName}     ${NC}: ${csudo}service ${serverName} start${NC}"
    [ -f ${service_config_dir}/taosadapter.service ] && [ -f ${installDir}/bin/taosadapter ] && \
      echo -e "${GREEN_DARK}To start Adapter ${NC}: ${csudo}service taosadapter start${NC}"
  else
    if [ "$osType" != "Darwin" ]; then
      echo -e "${GREEN_DARK}To start ${productName}     ${NC}: ${serverName}${NC}"
      [ -f ${installDir}/bin/taosadapter ] && \
        echo -e "${GREEN_DARK}To start Adapter ${NC}: taosadapter &${NC}"
    else
      echo -e "${GREEN_DARK}To start service      ${NC}: launchctl start com.tdengine.taosd${NC}"
      echo -e "${GREEN_DARK}To start Adapter ${NC}: launchctl start com.tdengine.taosadapter${NC}"
    fi
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
  install_app

  install_service
  install_taosadapter_service

  install_config
  install_taosadapter_config

  # Ask if to start the service
  echo
  echo -e "\033[44;32;1m${productName} is installed successfully!${NC}"
  echo
  echo -e "${GREEN_DARK}To configure ${productName} ${NC}: edit ${configDir}/${configFile}"
  [ -f ${configDir}/taosadapter.toml ] && [ -f ${installDir}/bin/taosadapter ] && \
    echo -e "${GREEN_DARK}To configure Adapter ${NC}: edit ${configDir}/taosadapter.toml"
  if ((${service_mod} == 0)); then
    echo -e "${GREEN_DARK}To start ${productName}     ${NC}: ${csudo}systemctl start ${serverName}${NC}"
    [ -f ${service_config_dir}/taosadapter.service ] && [ -f ${installDir}/bin/taosadapter ] && \
      echo -e "${GREEN_DARK}To start Adapter ${NC}: ${csudo}systemctl start taosadapter ${NC}"
  elif ((${service_mod} == 1)); then
    echo -e "${GREEN_DARK}To start ${productName}    ${NC}: ${csudo}service ${serverName} start${NC}"
    [ -f ${service_config_dir}/taosadapter.service ] && [ -f ${installDir}/bin/taosadapter ] && \
      echo -e "${GREEN_DARK}To start Adapter ${NC}: ${csudo}service taosadapter start${NC}"
  else
    if [ "$osType" != "Darwin" ]; then
      echo -e "${GREEN_DARK}To start ${productName}     ${NC}: ${serverName}${NC}"
      [ -f ${installDir}/bin/taosadapter ] && \
        echo -e "${GREEN_DARK}To start Adapter ${NC}: taosadapter &${NC}"
    else
      echo -e "${GREEN_DARK}To start service      ${NC}: launchctl start com.tdengine.taosd${NC}"
      echo -e "${GREEN_DARK}To start Adapter ${NC}: launchctl start com.tdengine.taosadapter${NC}"
    fi
  fi

  echo -e "${GREEN_DARK}To access ${productName}    ${NC}: use ${GREEN_UNDERLINE}${clientName}${NC} in shell${NC}"
  echo
  echo -e "\033[44;32;1m${productName} is installed successfully!${NC}"
}

## ==============================Main program starts from here============================
echo source directory: $1
echo binary directory: $2
if [ -x ${data_dir}/dnode/dnodeCfg.json ]; then
  echo -e "\033[44;31;5mThe default data directory ${data_dir} contains old data of tdengine 2.x, please clear it before installing!\033[0m"
else
  if [ -x ${bin_dir}/${clientName} ]; then
    update_TDengine
  else
    install_TDengine
  fi
fi
