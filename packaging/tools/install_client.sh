#!/bin/bash
#
# This file is used to install TDengine client on linux systems. The operating system
# is required to use systemd to manage services at boot

set -e
#set -x

# -----------------------Variables definition---------------------

dataDir="/var/lib/taos"
logDir="/var/log/taos"
productName="TDengine"
installDir="/usr/local/taos"
configDir="/etc/taos"
serverName="taosd"
clientName="taos"
uninstallScript="rmtaos"
configFile="taos.cfg"
tarName="package.tar.gz"

osType=Linux
pagMode=full
verMode=edge

clientName2="taos"
serverName2="taosd"
productName2="TDengine"
emailName2="taosdata.com"

benchmarkName2="${clientName2}Benchmark"
dumpName2="${clientName2}dump"
demoName2="${clientName2}demo"
uninstallScript2="rm${clientName2}"

if [ "$osType" != "Darwin" ]; then
    script_dir=$(dirname $(readlink -f "$0"))
    # Dynamic directory
    data_dir=${dataDir}
    log_dir=${logDir}
else
    script_dir=`dirname $0`
    cd ${script_dir}
    script_dir="$(pwd)"
    data_dir=${dataDir}
    log_dir=~/${productName}/log
fi

log_link_dir="${installDir}/log"

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
fi

#install main path
install_main_dir="${installDir}"

# old bin dir
bin_dir="${installDir}/bin"

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo "
fi

update_flag=0

function kill_client() {
  pid=$(ps -ef | grep "${clientName}" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo}kill -9 $pid   || :
  fi
}

function install_main_path() {
  #create install main dir and all sub dir
  ${csudo}rm -rf ${install_main_dir} || :
  ${csudo}mkdir -p ${install_main_dir}
  ${csudo}mkdir -p ${install_main_dir}/cfg
  ${csudo}mkdir -p ${install_main_dir}/bin
  ${csudo}mkdir -p ${install_main_dir}/driver
  if [ "$productName2" == "TDengine" ]; then
    ${csudo}mkdir -p ${install_main_dir}/examples
  fi
  ${csudo}mkdir -p ${install_main_dir}/include
  if [ "$verMode" == "cluster" ]; then
    ${csudo}mkdir -p ${install_main_dir}/connector
  fi
}

function install_bin() {
  # Remove links
  ${csudo}rm -f ${bin_link_dir}/${clientName}         || :
  if [ "$osType" != "Darwin" ]; then
      ${csudo}rm -f ${bin_link_dir}/taosdemo || :
  fi
  ${csudo}rm -f ${bin_link_dir}/${uninstallScript}       || :
  ${csudo}rm -f ${bin_link_dir}/set_core     || :

  ${csudo}cp -r ${script_dir}/bin/* ${install_main_dir}/bin && ${csudo}chmod 0555 ${install_main_dir}/bin/*

  #Make link
  [ -x ${install_main_dir}/bin/${clientName2} ] && ${csudo}ln -s ${install_main_dir}/bin/${clientName2} ${bin_link_dir}/${clientName2} || :
  if [ "$osType" != "Darwin" ]; then
      [ -x ${install_main_dir}/bin/${demoName2} ] && ${csudo}ln -s ${install_main_dir}/bin/${demoName2} ${bin_link_dir}/${demoName2}  || :
  fi
  [ -x ${install_main_dir}/bin/remove_client.sh ] && ${csudo}ln -s ${install_main_dir}/bin/remove_client.sh ${bin_link_dir}/${uninstallScript} || :
  [ -x ${install_main_dir}/bin/set_core.sh ] && ${csudo}ln -s ${install_main_dir}/bin/set_core.sh ${bin_link_dir}/set_core || :  
  [ -x ${install_main_dir}/bin/${benchmarkName2} ] && ${csudo}ln -s ${install_main_dir}/bin/${benchmarkName2} ${bin_link_dir}/${benchmarkName2}  || :
  [ -x ${install_main_dir}/bin/${dumpName2} ] && ${csudo}ln -s ${install_main_dir}/bin/${dumpName2} ${bin_link_dir}/${dumpName2}  || :
}

function clean_lib() {
    sudo rm -f /usr/lib/libtaos.* || :
    [ -f /usr/lib/libtaosws.so ] && sudo rm -f /usr/lib/libtaosws.so || :
    [ -f /usr/lib64/libtaosws.so ] && sudo rm -f /usr/lib64/libtaosws.so || :
    sudo rm -rf ${lib_dir} || :
}

function install_lib() {
    # Remove links
    ${csudo}rm -f ${lib_link_dir}/libtaos.*         || :
    ${csudo}rm -f ${lib64_link_dir}/libtaos.*       || :

    [ -f ${lib_link_dir}/libtaosws.so ] && ${csudo}rm -f ${lib_link_dir}/libtaosws.so         || :
    [ -f ${lib64_link_dir}/libtaosws.so ] && ${csudo}rm -f ${lib64_link_dir}/libtaosws.so         || :

    #${csudo}rm -rf ${v15_java_app_dir}              || :

    ${csudo}cp -rf ${script_dir}/driver/* ${install_main_dir}/driver && ${csudo}chmod 777 ${install_main_dir}/driver/*

    if [ "$osType" != "Darwin" ]; then
        ${csudo}ln -s ${install_main_dir}/driver/libtaos.* ${lib_link_dir}/libtaos.so.1
        ${csudo}ln -s ${lib_link_dir}/libtaos.so.1 ${lib_link_dir}/libtaos.so

        [ -f ${install_main_dir}/driver/libtaosws.so ] && ${csudo}ln -sf ${install_main_dir}/driver/libtaosws.so ${lib_link_dir}/libtaosws.so ||:

        if [ -d "${lib64_link_dir}" ]; then
            ${csudo}ln -s ${install_main_dir}/driver/libtaos.* ${lib64_link_dir}/libtaos.so.1       || :
            ${csudo}ln -s ${lib64_link_dir}/libtaos.so.1 ${lib64_link_dir}/libtaos.so               || :

            [ -f ${install_main_dir}/driver/libtaosws.so ] && ${csudo}ln -sf ${install_main_dir}/driver/libtaosws.so ${lib64_link_dir}/libtaosws.so       || :
        fi
    else
        ${csudo}ln -s ${install_main_dir}/driver/libtaos.* ${lib_link_dir}/libtaos.1.dylib
        ${csudo}ln -s ${lib_link_dir}/libtaos.1.dylib ${lib_link_dir}/libtaos.dylib

        [ -f ${install_main_dir}/driver/libtaosws.dylib ] && ${csudo}ln -sf ${install_main_dir}/driver/libtaosws.dylib ${lib_link_dir}/libtaosws.dylib ||:
    fi

    if [ "$osType" != "Darwin" ]; then
        ${csudo}ldconfig
    else
        ${csudo}update_dyld_shared_cache
    fi
}

function install_header() {
    ${csudo}rm -f ${inc_link_dir}/taos.h ${inc_link_dir}/taosdef.h ${inc_link_dir}/tdef.h ${inc_link_dir}/taoserror.h  ${inc_link_dir}/taosudf.h  || :
    ${csudo}cp -f ${script_dir}/inc/* ${install_main_dir}/include && ${csudo}chmod 644 ${install_main_dir}/include/*
    ${csudo}ln -s ${install_main_dir}/include/taos.h ${inc_link_dir}/taos.h
    ${csudo}ln -s ${install_main_dir}/include/taosdef.h ${inc_link_dir}/taosdef.h
    ${csudo}ln -s ${install_main_dir}/include/tdef.h ${inc_link_dir}/tdef.h
    ${csudo}ln -s ${install_main_dir}/include/taoserror.h ${inc_link_dir}/taoserror.h
    ${csudo}ln -s ${install_main_dir}/include/taosudf.h ${inc_link_dir}/taosudf.h    

    [ -f ${install_main_dir}/include/taosws.h ] && ${csudo}ln -sf ${install_main_dir}/include/taosws.h ${inc_link_dir}/taosws.h ||:
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
            #     ${csudo}/usr/bin/install -c -m 755 ${jemalloc_dir}/lib/libjemalloc.a /usr/local/lib
            # fi
            # if [ -f ${jemalloc_dir}/lib/libjemalloc_pic.a ]; then
            #     ${csudo}/usr/bin/install -c -m 755 ${jemalloc_dir}/lib/libjemalloc_pic.a /usr/local/lib
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
            echo "/usr/local/lib" | ${csudo}tee /etc/ld.so.conf.d/jemalloc.conf > /dev/null || echo -e "failed to write /etc/ld.so.conf.d/jemalloc.conf"
            ${csudo}ldconfig
        else
            echo "/etc/ld.so.conf.d not found!"
        fi
    fi
}

function install_config() {
    if [ ! -f ${cfg_install_dir}/${configFile} ]; then
        ${csudo}mkdir -p ${cfg_install_dir}
        [ -f ${script_dir}/cfg/${configFile} ] && ${csudo}cp ${script_dir}/cfg/${configFile} ${cfg_install_dir}
        ${csudo}chmod 644 ${cfg_install_dir}/*
    fi

    ${csudo}cp -f ${script_dir}/cfg/${configFile} ${install_main_dir}/cfg/${configFile}.org
    ${csudo}ln -s ${cfg_install_dir}/${configFile} ${install_main_dir}/cfg
}


function install_log() {
    ${csudo}rm -rf ${log_dir}  || :

    if [ "$osType" != "Darwin" ]; then
        ${csudo}mkdir -p ${log_dir} && ${csudo}chmod 777 ${log_dir}
    else
        mkdir -p ${log_dir} && ${csudo}chmod 777 ${log_dir}
    fi
    ${csudo}ln -s ${log_dir} ${install_main_dir}/log
}

function install_connector() {
    if [ -d ${script_dir}/connector ]; then
        ${csudo}cp -rf ${script_dir}/connector/ ${install_main_dir}/
    fi
}

function install_examples() {
    if [ -d ${script_dir}/examples ]; then
        ${csudo}cp -rf ${script_dir}/examples/* ${install_main_dir}/examples
    fi
}

function update_TDengine() {
    # Start to update
    if [ ! -e ${tarName} ]; then
        echo "File ${tarName} does not exist"
        exit 1
    fi
    tar -zxf ${tarName}
    echo -e "${GREEN}Start to update ${productName2} client...${NC}"
    # Stop the client shell if running
    if ps aux | grep -v grep | grep ${clientName2} &> /dev/null; then
        kill_client
        sleep 1
    fi

    install_main_path

    install_log
    install_header
    install_lib
    install_jemalloc
    if [ "$verMode" == "cluster" ]; then
        install_connector
    fi
    install_examples
    install_bin
    install_config

    echo
    echo -e "\033[44;32;1m${productName2} client is updated successfully!${NC}"

    rm -rf $(tar -tf ${tarName} | grep -Ev "^\./$|^\/")
}

function install_TDengine() {
    # Start to install
    if [ ! -e ${tarName} ]; then
        echo "File ${tarName} does not exist"
        exit 1
    fi
    tar -zxf ${tarName}
    echo -e "${GREEN}Start to install ${productName2} client...${NC}"

    install_main_path
    install_log
    install_header
    install_lib
    install_jemalloc
    if [ "$verMode" == "cluster" ]; then
        install_connector
    fi
    install_examples
    install_bin
    install_config

    echo
    echo -e "\033[44;32;1m${productName2} client is installed successfully!${NC}"

    rm -rf $(tar -tf ${tarName} | grep -Ev "^\./$|^\/")
}


## ==============================Main program starts from here============================
# Install or updata client and client
# if server is already install, don't install client
if [ -e ${bin_dir}/${serverName} ]; then
    echo -e "\033[44;32;1mThere are already installed ${productName2} server, so don't need install client!${NC}"
    exit 0
fi

if [ -x ${bin_dir}/${clientName} ]; then
    update_flag=1
    update_TDengine
else
    install_TDengine
fi
