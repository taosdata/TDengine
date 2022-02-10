#!/bin/bash
#
# This file is used to install TAOS time-series database on linux systems. The operating system
# is required to use systemd to manage services at boot

set -e
# set -x

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

if [ "$osType" != "Darwin" ]; then
    data_dir="/var/lib/taos"
    log_dir="/var/log/taos"

    cfg_install_dir="/etc/taos"

    bin_link_dir="/usr/bin"
    lib_link_dir="/usr/lib"
    lib64_link_dir="/usr/lib64"
    inc_link_dir="/usr/include"

    install_main_dir="/usr/local/taos"

    bin_dir="/usr/local/taos/bin"
else
    data_dir="/usr/local/var/lib/taos"
    log_dir="/usr/local/var/log/taos"

    cfg_install_dir="/usr/local/etc/taos"

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

if [ "$osType" != "Darwin" ]; then
    if command -v sudo > /dev/null; then
    csudo="sudo"
    fi
    initd_mod=0
    service_mod=2
    if pidof systemd &> /dev/null; then
        service_mod=0
    elif $(which service &> /dev/null); then
        service_mod=1
        service_config_dir="/etc/init.d"
        if $(which chkconfig &> /dev/null); then
            initd_mod=1
        elif $(which insserv &> /dev/null); then
            initd_mod=2
        elif $(which update-rc.d &> /dev/null); then
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
    os_type=0
    if echo $osinfo | grep -qwi "ubuntu" ; then
        echo "this is ubuntu system"
        os_type=1
    elif echo $osinfo | grep -qwi "debian" ; then
        echo "this is debian system"
        os_type=1
    elif echo $osinfo | grep -qwi "Kylin" ; then
        echo "this is Kylin system"
        os_type=1
    elif  echo $osinfo | grep -qwi "centos" ; then
        echo "this is centos system"
        os_type=2
    elif echo $osinfo | grep -qwi "fedora" ; then
        echo "this is fedora system"
        os_type=2
    else
        echo "${osinfo}: This is an officially unverified linux system, If there are any problems with the installation and operation, "
        echo "please feel free to contact taosdata.com for support."
        os_type=1
    fi
fi

function kill_taosd() {
    pid=$(ps -ef | grep "taosd" | grep -v "grep" | awk '{print $2}')
    if [ -n "$pid" ]; then
        ${csudo} kill -9 $pid   || :
    fi
}

function install_main_path() {
    #create install main dir and all sub dir
    if [ "$osType" != "Darwin" ]; then
        ${csudo} rm -rf ${install_main_dir}    || :
        ${csudo} mkdir -p ${install_main_dir}
        ${csudo} mkdir -p ${install_main_dir}/cfg
        ${csudo} mkdir -p ${install_main_dir}/bin
        ${csudo} mkdir -p ${install_main_dir}/connector
        ${csudo} mkdir -p ${install_main_dir}/driver
        ${csudo} mkdir -p ${install_main_dir}/examples
        ${csudo} mkdir -p ${install_main_dir}/include
        ${csudo} mkdir -p ${install_main_dir}/init.d
    else
        ${csudo} rm -rf ${install_main_dir}             || ${csudo} rm -rf ${install_main_2_dir}             || :
        ${csudo} mkdir -p ${install_main_dir}           || ${csudo} mkdir -p ${install_main_2_dir}
        ${csudo} mkdir -p ${install_main_dir}/cfg       || ${csudo} mkdir -p ${install_main_2_dir}/cfg
        ${csudo} mkdir -p ${install_main_dir}/bin       || ${csudo} mkdir -p ${install_main_2_dir}/bin
        ${csudo} mkdir -p ${install_main_dir}/connector || ${csudo} mkdir -p ${install_main_2_dir}/connector
        ${csudo} mkdir -p ${install_main_dir}/driver    || ${csudo} mkdir -p ${install_main_2_dir}/driver
        ${csudo} mkdir -p ${install_main_dir}/examples  || ${csudo} mkdir -p ${install_main_2_dir}/examples
        ${csudo} mkdir -p ${install_main_dir}/include   || ${csudo} mkdir -p ${install_main_2_dir}/include
    fi
}

function install_bin() {
    # Remove links
    ${csudo} rm -f ${bin_link_dir}/taos     || :
    ${csudo} rm -f ${bin_link_dir}/taosd    || :
    ${csudo} rm -f ${bin_link_dir}/taosdemo || :
    ${csudo} rm -f ${bin_link_dir}/taosdump || :

    if [ "$osType" != "Darwin" ]; then
        ${csudo} rm -f ${bin_link_dir}/perfMonitor || :
        ${csudo} rm -f ${bin_link_dir}/set_core || :
        ${csudo} rm -f ${bin_link_dir}/rmtaos   || :
        
        ${csudo} cp -r ${binary_dir}/build/bin/* ${install_main_dir}/bin
        ${csudo} cp -r ${script_dir}/taosd-dump-cfg.gdb   ${install_main_dir}/bin
        
        ${csudo} cp -r ${script_dir}/remove.sh     ${install_main_dir}/bin
        ${csudo} cp -r ${script_dir}/set_core.sh   ${install_main_dir}/bin
        ${csudo} cp -r ${script_dir}/startPre.sh   ${install_main_dir}/bin
        
        ${csudo} chmod 0555 ${install_main_dir}/bin/*
        #Make link
        [ -x ${install_main_dir}/bin/taos ]      && ${csudo} ln -s ${install_main_dir}/bin/taos ${bin_link_dir}/taos    || :
        [ -x ${install_main_dir}/bin/taosd ]     && ${csudo} ln -s ${install_main_dir}/bin/taosd ${bin_link_dir}/taosd   || :
        [ -x ${install_main_dir}/bin/taosdump ]  && ${csudo} ln -s ${install_main_dir}/bin/taosdump ${bin_link_dir}/taosdump || :
        [ -x ${install_main_dir}/bin/taosdemo ]  && ${csudo} ln -s ${install_main_dir}/bin/taosdemo ${bin_link_dir}/taosdemo || :
        [ -x ${install_main_dir}/bin/perfMonitor ]  && ${csudo} ln -s ${install_main_dir}/bin/perfMonitor ${bin_link_dir}/perfMonitor || :
        [ -x ${install_main_dir}/set_core.sh ]  && ${csudo} ln -s ${install_main_dir}/bin/set_core.sh ${bin_link_dir}/set_core || :
        [ -x ${install_main_dir}/bin/remove.sh ] && ${csudo} ln -s ${install_main_dir}/bin/remove.sh ${bin_link_dir}/rmtaos  || :
   else
    
        ${csudo} cp -r ${binary_dir}/build/bin/*   ${install_main_dir}/bin || ${csudo} cp -r ${binary_dir}/build/bin/*   ${install_main_2_dir}/bin  || :
        ${csudo} cp -r ${script_dir}/taosd-dump-cfg.gdb   ${install_main_dir}/bin || ${csudo}cp -r ${script_dir}/taosd-dump-cfg.gdb   ${install_main_2_dir}  || :
        ${csudo} cp -r ${script_dir}/remove_client.sh   ${install_main_dir}/bin || ${csudo} cp -r ${script_dir}/remove_client.sh   ${install_main_2_dir}/bin
        ${csudo} chmod 0555 ${install_main_dir}/bin/* || ${csudo} chmod 0555 ${install_main_2_dir}/bin/*
        #Make link
        [ -x ${install_main_dir}/bin/taos ] || [ -x ${install_main_2_dir}/bin/taos ] && ${csudo} ln -s ${install_main_dir}/bin/taos ${bin_link_dir}/taos || ${csudo} ln -s ${install_main_2_dir}/bin/taos  || :
        [ -x ${install_main_dir}/bin/taosd ] || [ -x ${install_main_2_dir}/bin/taosd ] &&  ${csudo} ln -s ${install_main_dir}/bin/taosd ${bin_link_dir}/taosd || ${csudo} ln -s ${install_main_2_dir}/bin/taosd || :
        [ -x ${install_main_dir}/bin/taosdump ] || [ -x ${install_main_2_dir}/bin/taosdump ] && ${csudo} ln -s ${install_main_dir}/bin/taosdump ${bin_link_dir}/taosdump || ln -s ${install_main_2_dir}/bin/taosdump ${bin_link_dir}/taosdump   || :
        [ -x ${install_main_dir}/bin/taosdemo ] || [ -x ${install_main_2_dir}/bin/taosdemo ] && ${csudo} ln -s ${install_main_dir}/bin/taosdemo ${bin_link_dir}/taosdemo || ln -s ${install_main_2_dir}/bin/taosdemo ${bin_link_dir}/taosdemo   || :
    fi
}

function install_jemalloc() {
    if [ "$osType" != "Darwin" ]; then
        /usr/bin/install -c -d /usr/local/bin

        if [ -f ${binary_dir}/build/bin/jemalloc-config ]; then
            /usr/bin/install -c -m 755 ${binary_dir}/build/bin/jemalloc-config /usr/local/bin
        fi
        if [ -f ${binary_dir}/build/bin/jemalloc.sh ]; then
            /usr/bin/install -c -m 755 ${binary_dir}/build/bin/jemalloc.sh /usr/local/bin
        fi
        if [ -f ${binary_dir}/build/bin/jeprof ]; then
            /usr/bin/install -c -m 755 ${binary_dir}/build/bin/jeprof /usr/local/bin
        fi
        if [ -f ${binary_dir}/build/include/jemalloc/jemalloc.h ]; then
            /usr/bin/install -c -d /usr/local/include/jemalloc
            /usr/bin/install -c -m 644 ${binary_dir}/build/include/jemalloc/jemalloc.h /usr/local/include/jemalloc
        fi
        if [ -f ${binary_dir}/build/lib/libjemalloc.so.2 ]; then
            /usr/bin/install -c -d /usr/local/lib
            /usr/bin/install -c -m 755 ${binary_dir}/build/lib/libjemalloc.so.2 /usr/local/lib
            ln -sf libjemalloc.so.2 /usr/local/lib/libjemalloc.so
            /usr/bin/install -c -d /usr/local/lib
            if [ -f ${binary_dir}/build/lib/libjemalloc.a ]; then
                /usr/bin/install -c -m 755 ${binary_dir}/build/lib/libjemalloc.a /usr/local/lib
            fi
            if [ -f ${binary_dir}/build/lib/libjemalloc_pic.a ]; then
                /usr/bin/install -c -m 755 ${binary_dir}/build/lib/libjemalloc_pic.a /usr/local/lib
            fi
            if [ -f ${binary_dir}/build/lib/pkgconfig/jemalloc.pc ]; then
                /usr/bin/install -c -d /usr/local/lib/pkgconfig
                /usr/bin/install -c -m 644 ${binary_dir}/build/lib/pkgconfig/jemalloc.pc /usr/local/lib/pkgconfig
            fi
        fi
        if [ -f ${binary_dir}/build/share/doc/jemalloc/jemalloc.html ]; then
            /usr/bin/install -c -d /usr/local/share/doc/jemalloc
            /usr/bin/install -c -m 644 ${binary_dir}/build/share/doc/jemalloc/jemalloc.html /usr/local/share/doc/jemalloc
        fi
        if [ -f ${binary_dir}/build/share/man/man3/jemalloc.3 ]; then
            /usr/bin/install -c -d /usr/local/share/man/man3
            /usr/bin/install -c -m 644 ${binary_dir}/build/share/man/man3/jemalloc.3 /usr/local/share/man/man3
        fi

        if [ -d /etc/ld.so.conf.d ]; then
            echo "/usr/local/lib" | ${csudo} tee /etc/ld.so.conf.d/jemalloc.conf
            ${csudo} ldconfig
        else
            echo "/etc/ld.so.conf.d not found!"
        fi
    fi
}

function install_lib() {
    # Remove links
    ${csudo} rm -f ${lib_link_dir}/libtaos.*     || :
    if [ "$osType" != "Darwin" ]; then
      ${csudo} rm -f ${lib64_link_dir}/libtaos.*   || :
    fi

    if [ "$osType" != "Darwin" ]; then
        ${csudo} cp ${binary_dir}/build/lib/libtaos.so.${verNumber} ${install_main_dir}/driver && ${csudo} chmod 777 ${install_main_dir}/driver/*
        ${csudo} ln -sf ${install_main_dir}/driver/libtaos.* ${lib_link_dir}/libtaos.so.1
        ${csudo} ln -sf ${lib_link_dir}/libtaos.so.1 ${lib_link_dir}/libtaos.so

        if [ -d "${lib64_link_dir}" ]; then
          ${csudo} ln -sf ${install_main_dir}/driver/libtaos.* ${lib64_link_dir}/libtaos.so.1
          ${csudo} ln -sf ${lib64_link_dir}/libtaos.so.1 ${lib64_link_dir}/libtaos.so
        fi
    else
        ${csudo} cp -Rf ${binary_dir}/build/lib/libtaos.${verNumber}.dylib ${install_main_dir}/driver || ${csudo} cp -Rf ${binary_dir}/build/lib/libtaos.${verNumber}.dylib ${install_main_2_dir}/driver && ${csudo} chmod 777 ${install_main_dir}/driver/* || ${csudo} chmod 777 ${install_main_2_dir}/driver/*
  
        ${csudo} ln -sf ${install_main_dir}/driver/libtaos.* ${install_main_dir}/driver/libtaos.dylib || ${csudo} ln -sf ${install_main_2_dir}/driver/libtaos.* ${install_main_2_dir}/driver/libtaos.dylib   || :
        ${csudo} ln -sf ${install_main_dir}/driver/libtaos.${verNumber}.dylib ${lib_link_dir}/libtaos.1.dylib   || :
        ${csudo} ln -sf ${lib_link_dir}/libtaos.1.dylib ${lib_link_dir}/libtaos.dylib   || :
    fi
    
    install_jemalloc

    if [ "$osType" != "Darwin" ]; then
        ${csudo} ldconfig
    fi
}

function install_header() {

    if [ "$osType" != "Darwin" ]; then
        ${csudo} rm -f ${inc_link_dir}/taos.h ${inc_link_dir}/taoserror.h     || :
        ${csudo} cp -f ${source_dir}/src/inc/taos.h ${source_dir}/src/inc/taoserror.h ${install_main_dir}/include && ${csudo} chmod 644 ${install_main_dir}/include/*
        ${csudo} ln -s ${install_main_dir}/include/taos.h ${inc_link_dir}/taos.h
        ${csudo} ln -s ${install_main_dir}/include/taoserror.h ${inc_link_dir}/taoserror.h
    else
        ${csudo} cp -f ${source_dir}/src/inc/taos.h ${source_dir}/src/inc/taoserror.h ${install_main_dir}/include || ${csudo} cp -f ${source_dir}/src/inc/taos.h ${source_dir}/src/inc/taoserror.h ${install_main_2_dir}/include && ${csudo} chmod 644 ${install_main_dir}/include/* || ${csudo} chmod 644 ${install_main_2_dir}/include/*
    fi
}

function install_config() {
    if [ ! -f ${cfg_install_dir}/taos.cfg ]; then
        ${csudo} mkdir -p ${cfg_install_dir}
        [ -f ${script_dir}/../cfg/taos.cfg ] &&
        ${csudo} cp ${script_dir}/../cfg/taos.cfg ${cfg_install_dir}
        ${csudo} chmod 644 ${cfg_install_dir}/*
        ${csudo} cp -f ${script_dir}/../cfg/taos.cfg ${install_main_dir}/cfg/taos.cfg.org
        ${csudo} ln -s ${cfg_install_dir}/taos.cfg ${install_main_dir}/cfg
    else
        ${csudo} cp -f ${script_dir}/../cfg/taos.cfg ${install_main_dir}/cfg/taos.cfg.org || ${csudo} cp -f ${script_dir}/../cfg/taos.cfg ${install_main_2_dir}/cfg/taos.cfg.org
    fi
}

function install_log() {
    ${csudo} rm -rf ${log_dir}  || :
    ${csudo} mkdir -p ${log_dir} && ${csudo} chmod 777 ${log_dir}
    if [ "$osType" != "Darwin" ]; then
        ${csudo} ln -s ${log_dir} ${install_main_dir}/log
    else
        ${csudo} ln -s ${log_dir} ${install_main_dir}/log || ${csudo} ln -s ${log_dir} ${install_main_2_dir}/log
    fi
}

function install_data() {
    ${csudo} mkdir -p ${data_dir}
    if [ "$osType" != "Darwin" ]; then
        ${csudo} ln -s ${data_dir} ${install_main_dir}/data
    else
        ${csudo} ln -s ${data_dir} ${install_main_dir}/data || ${csudo} ln -s ${data_dir} ${install_main_2_dir}/data
    fi
}

function install_connector() {
    if find ${source_dir}/src/connector/go -mindepth 1 -maxdepth 1 | read; then
        ${csudo} cp -r ${source_dir}/src/connector/go ${install_main_dir}/connector
    else
        echo "WARNING: go connector not found, please check if want to use it!"
    fi
    if [ "$osType" != "Darwin" ]; then
        ${csudo} cp -rf ${source_dir}/src/connector/python ${install_main_dir}/connector
        ${csudo} cp ${binary_dir}/build/lib/*.jar ${install_main_dir}/connector &> /dev/null && ${csudo} chmod 777 ${install_main_dir}/connector/*.jar  || echo &> /dev/null
    else
        ${csudo} cp -rf ${source_dir}/src/connector/python ${install_main_dir}/connector || ${csudo} cp -rf ${source_dir}/src/connector/python ${install_main_2_dir}/connector
        ${csudo} cp ${binary_dir}/build/lib/*.jar ${install_main_dir}/connector &> /dev/null && ${csudo} chmod 777 ${install_main_dir}/connector/*.jar  || echo &> /dev/null
        ${csudo} cp ${binary_dir}/build/lib/*.jar ${install_main_2_dir}/connector &> /dev/null && ${csudo} chmod 777 ${install_main_2_dir}/connector/*.jar  || echo &> /dev/null
    fi
}

function install_examples() {
    if [ "$osType" != "Darwin" ]; then
       ${csudo} cp -rf ${source_dir}/examples/* ${install_main_dir}/examples
    else
       ${csudo} cp -rf ${source_dir}/examples/* ${install_main_dir}/examples || ${csudo} cp -rf ${source_dir}/examples/* ${install_main_2_dir}/examples
    fi
}

function clean_service_on_sysvinit() {
    #restart_config_str="taos:2345:respawn:${service_config_dir}/taosd start"
    #${csudo} sed -i "\|${restart_config_str}|d" /etc/inittab || :

    if pidof taosd &> /dev/null; then
        ${csudo} service taosd stop || :
    fi

    if ((${initd_mod}==1)); then
        ${csudo} chkconfig --del taosd || :
    elif ((${initd_mod}==2)); then
        ${csudo} insserv -r taosd || :
    elif ((${initd_mod}==3)); then
        ${csudo} update-rc.d -f taosd remove || :
    fi

    ${csudo} rm -f ${service_config_dir}/taosd || :

    if $(which init &> /dev/null); then
        ${csudo} init q || :
    fi
}

function install_service_on_sysvinit() {
    clean_service_on_sysvinit

    sleep 1

    # Install taosd service
    if ((${os_type}==1)); then
    ${csudo} cp -f ${script_dir}/../deb/taosd ${install_main_dir}/init.d
    ${csudo} cp    ${script_dir}/../deb/taosd ${service_config_dir} && ${csudo} chmod a+x ${service_config_dir}/taosd
    elif ((${os_type}==2)); then
    ${csudo} cp -f ${script_dir}/../rpm/taosd ${install_main_dir}/init.d
    ${csudo} cp    ${script_dir}/../rpm/taosd ${service_config_dir} && ${csudo} chmod a+x ${service_config_dir}/taosd
    fi

    #restart_config_str="taos:2345:respawn:${service_config_dir}/taosd start"
    #${csudo} grep -q -F "$restart_config_str" /etc/inittab || ${csudo} bash -c "echo '${restart_config_str}' >> /etc/inittab"

    if ((${initd_mod}==1)); then
        ${csudo} chkconfig --add taosd || :
        ${csudo} chkconfig --level 2345 taosd on || :
    elif ((${initd_mod}==2)); then
        ${csudo} insserv taosd || :
        ${csudo} insserv -d taosd || :
    elif ((${initd_mod}==3)); then
        ${csudo} update-rc.d taosd defaults || :
    fi
}

function clean_service_on_systemd() {
    taosd_service_config="${service_config_dir}/taosd.service"

    if systemctl is-active --quiet taosd; then
        echo "TDengine is running, stopping it..."
        ${csudo} systemctl stop taosd &> /dev/null || echo &> /dev/null
    fi
    ${csudo} systemctl disable taosd &> /dev/null || echo &> /dev/null

    ${csudo} rm -f ${taosd_service_config}
}

# taos:2345:respawn:/etc/init.d/taosd start

function install_service_on_systemd() {
    clean_service_on_systemd

    taosd_service_config="${service_config_dir}/taosd.service"

    ${csudo} bash -c "echo '[Unit]'                             >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'Description=TDengine server service' >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'After=network-online.target'        >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'Wants=network-online.target'        >> ${taosd_service_config}"
    ${csudo} bash -c "echo                                      >> ${taosd_service_config}"
    ${csudo} bash -c "echo '[Service]'                          >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'Type=simple'                        >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'ExecStart=/usr/bin/taosd'           >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'ExecStartPre=/usr/local/taos/bin/startPre.sh'           >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'TimeoutStopSec=1000000s'            >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'LimitNOFILE=infinity'               >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'LimitNPROC=infinity'                >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'LimitCORE=infinity'                 >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'TimeoutStartSec=0'                  >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'StandardOutput=null'                >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'Restart=always'                     >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'StartLimitBurst=3'                  >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'StartLimitInterval=60s'             >> ${taosd_service_config}"
    ${csudo} bash -c "echo                                      >> ${taosd_service_config}"
    ${csudo} bash -c "echo '[Install]'                          >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'WantedBy=multi-user.target'         >> ${taosd_service_config}"
    ${csudo} systemctl enable taosd
}

function install_service() {
    if ((${service_mod}==0)); then
        install_service_on_systemd
    elif ((${service_mod}==1)); then
        install_service_on_sysvinit
    else
        # must manual stop taosd
        kill_taosd
    fi
}

function update_TDengine() {
    echo -e "${GREEN}Start to update TDengine...${NC}"
    # Stop the service if running

    if [ "$osType" != "Darwin" ]; then
      if pidof taosd &> /dev/null; then
        if ((${service_mod}==0)); then
            ${csudo} systemctl stop taosd || :
        elif ((${service_mod}==1)); then
            ${csudo} service taosd stop || :
        else
            kill_taosd
        fi
        sleep 1
      fi
    fi

    install_main_path

    install_log
    install_header
    install_lib
    install_connector
    install_examples
    install_bin

    if [ "$osType" != "Darwin" ]; then
        install_service
    fi

    install_config

    if [ "$osType" != "Darwin" ]; then
        echo
        echo -e "\033[44;32;1mTDengine is updated successfully!${NC}"
        echo

        echo -e "${GREEN_DARK}To configure TDengine ${NC}: edit /etc/taos/taos.cfg"
        if ((${service_mod}==0)); then
            echo -e "${GREEN_DARK}To start TDengine     ${NC}: ${csudo} systemctl start taosd${NC}"
        elif ((${service_mod}==1)); then
            echo -e "${GREEN_DARK}To start TDengine     ${NC}: ${csudo} service taosd start${NC}"
        else
            echo -e "${GREEN_DARK}To start TDengine     ${NC}: ./taosd${NC}"
        fi

        echo -e "${GREEN_DARK}To access TDengine    ${NC}: use ${GREEN_UNDERLINE}taos${NC} in shell${NC}"
        echo
        echo -e "\033[44;32;1mTDengine is updated successfully!${NC}"
    else
        echo
        echo -e "\033[44;32;1mTDengine Client is updated successfully!${NC}"
        echo

        echo -e "${GREEN_DARK}To access TDengine Client   ${NC}: use ${GREEN_UNDERLINE}taos${NC} in shell${NC}"
        echo
        echo -e "\033[44;32;1mTDengine Client is updated successfully!${NC}"
    fi
}

function install_TDengine() {
    # Start to install
    if [ "$osType" != "Darwin" ]; then
        echo -e "${GREEN}Start to install TDEngine...${NC}"
    else
        echo -e "${GREEN}Start to install TDEngine Client ...${NC}"
    fi
    
    install_main_path

    install_data
    install_log
    install_header
    install_lib
    install_connector
    install_examples
    install_bin
    
    if [ "$osType" != "Darwin" ]; then
        install_service
    fi
    
    install_config

    if [ "$osType" != "Darwin" ]; then
        # Ask if to start the service
        echo
        echo -e "\033[44;32;1mTDengine is installed successfully!${NC}"
        echo
        echo -e "${GREEN_DARK}To configure TDengine ${NC}: edit /etc/taos/taos.cfg"
        if ((${service_mod}==0)); then
            echo -e "${GREEN_DARK}To start TDengine     ${NC}: ${csudo} systemctl start taosd${NC}"
        elif ((${service_mod}==1)); then
                echo -e "${GREEN_DARK}To start TDengine    ${NC}: ${csudo} service taosd start${NC}"
        else
            echo -e "${GREEN_DARK}To start TDengine     ${NC}: ./taosd${NC}"
        fi

        echo -e "${GREEN_DARK}To access TDengine    ${NC}: use ${GREEN_UNDERLINE}taos${NC} in shell${NC}"
        echo
        echo -e "\033[44;32;1mTDengine is installed successfully!${NC}"
    else
        echo -e "${GREEN_DARK}To access TDengine    ${NC}: use ${GREEN_UNDERLINE}taos${NC} in shell${NC}"
        echo
        echo -e "\033[44;32;1mTDengine Client is installed successfully!${NC}"
    fi
}

## ==============================Main program starts from here============================
echo source directory: $1
echo binary directory: $2
if [ "$osType" != "Darwin" ]; then
    if [ -x ${bin_dir}/taos ]; then
        update_TDengine
    else
        install_TDengine
    fi
else
    if [ -x ${bin_dir}/taos ] || [ -x ${bin_2_dir}/taos ]; then
        update_TDengine
    else
        install_TDengine
    fi
fi
