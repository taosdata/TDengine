#!/bin/bash
#
# This file is used to install TAOS time-series database on linux systems. The operating system 
# is required to use systemd to manage services at boot

set -e
# set -x

# -----------------------Variables definition---------------------
source_dir=$1
binary_dir=$2
script_dir=$(dirname $(readlink -m "$0"))
# Dynamic directory
data_dir="/var/lib/taos"
log_dir="/var/log/taos"

data_link_dir="/usr/local/taos/data"
log_link_dir="/usr/local/taos/log"

cfg_install_dir="/etc/taos"

bin_link_dir="/usr/bin"
lib_link_dir="/usr/lib"
inc_link_dir="/usr/include"

#install main path
install_main_dir="/usr/local/taos"

# old bin
bin_dir="/usr/local/taos/bin"

service_config_dir="/etc/systemd/system"

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo"
fi

function is_using_systemd() {
    if pidof systemd &> /dev/null; then
        return 0
    else
        return 1
    fi
}

if ! is_using_systemd; then
    service_config_dir="/etc/init.d"
fi

function install_main_path() {
    #create install main dir and all sub dir
    ${csudo} rm -rf ${install_main_dir}    || :
    ${csudo} mkdir -p ${install_main_dir}  
    ${csudo} mkdir -p ${install_main_dir}/cfg
    ${csudo} mkdir -p ${install_main_dir}/bin    
    ${csudo} mkdir -p ${install_main_dir}/connector
    ${csudo} mkdir -p ${install_main_dir}/driver
    ${csudo} mkdir -p ${install_main_dir}/examples
    ${csudo} mkdir -p ${install_main_dir}/include
    ${csudo} mkdir -p ${install_main_dir}/init.d     
}

function install_bin() {
    # Remove links
    ${csudo} rm -f ${bin_link_dir}/taos     || :
    ${csudo} rm -f ${bin_link_dir}/taosd    || :
    ${csudo} rm -f ${bin_link_dir}/taosdump || :
    ${csudo} rm -f ${bin_link_dir}/taosdemo || :
    ${csudo} rm -f ${bin_link_dir}/rmtaos   || :

    ${csudo} cp -r ${binary_dir}/build/bin/taos ${install_main_dir}/bin 
    ${csudo} cp -r ${binary_dir}/build/bin/taosd ${install_main_dir}/bin
    ${csudo} cp -r ${binary_dir}/build/bin/taosdemo ${install_main_dir}/bin
    ${csudo} cp -r ${binary_dir}/build/bin/taosdump ${install_main_dir}/bin
    ${csudo} cp -r ${script_dir}/remove.sh ${install_main_dir}/bin       
    ${csudo} chmod 0555 ${install_main_dir}/bin/*

    #Make link
    [ -x ${install_main_dir}/bin/taos ] && ${csudo} ln -s ${install_main_dir}/bin/taos ${bin_link_dir}/taos             || :
    [ -x ${install_main_dir}/bin/taosd ] && ${csudo} ln -s ${install_main_dir}/bin/taosd ${bin_link_dir}/taosd          || :  
    [ -x ${install_main_dir}/bin/taosdemo ] && ${csudo} ln -s ${install_main_dir}/bin/taosdemo ${bin_link_dir}/taosdemo || : 
    [ -x ${install_main_dir}/bin/taosdump ] && ${csudo} ln -s ${install_main_dir}/bin/taosdump ${bin_link_dir}/taosdump || :
    [ -x ${install_main_dir}/bin/remove.sh ] && ${csudo} ln -s ${install_main_dir}/bin/remove.sh ${bin_link_dir}/rmtaos || :
}

function install_lib() {
    # Remove links
    ${csudo} rm -f ${lib_link_dir}/libtaos.so     || :
    
    versioninfo=$(${script_dir}/get_version.sh)
    ${csudo} cp ${binary_dir}/build/lib/libtaos.so.${versioninfo} ${install_main_dir}/driver && ${csudo} chmod 777 ${install_main_dir}/driver/*
    ${csudo} ln -sf ${install_main_dir}/driver/libtaos.so.${versioninfo} ${lib_link_dir}/libtaos.so.1
    ${csudo} ln -sf ${lib_link_dir}/libtaos.so.1 ${lib_link_dir}/libtaos.so
}

function install_header() {

    ${csudo} rm -f ${inc_link_dir}/taos.h     || :    
    ${csudo} cp -f ${source_dir}/src/inc/taos.h ${install_main_dir}/include && ${csudo} chmod 644 ${install_main_dir}/include/*    
    ${csudo} ln -s ${install_main_dir}/include/taos.h ${inc_link_dir}/taos.h
}

function install_config() {
    #${csudo} rm -f ${install_main_dir}/cfg/taos.cfg     || :
    
    if [ ! -f ${cfg_install_dir}/taos.cfg ]; then        
        ${csudo} mkdir -p ${cfg_install_dir}
        [ -f ${script_dir}/../cfg/taos.cfg ] && ${csudo} cp ${script_dir}/../cfg/taos.cfg ${cfg_install_dir}
        ${csudo} chmod 644 ${cfg_install_dir}/*
    fi 
    
    ${csudo} cp -f ${script_dir}/../cfg/taos.cfg ${install_main_dir}/cfg/taos.cfg.org
    ${csudo} ln -s ${cfg_install_dir}/taos.cfg ${install_main_dir}/cfg 
}

function install_log() {  
    ${csudo} rm -rf ${log_dir}  || :
    ${csudo} mkdir -p ${log_dir} && ${csudo} chmod 777 ${log_dir}
    
    ${csudo} ln -s ${log_dir} ${install_main_dir}/log
}

function install_data() {
    ${csudo} mkdir -p ${data_dir}
    ${csudo} ln -s ${data_dir} ${install_main_dir}/data  
}

function install_connector() {
    ${csudo} cp -rf ${source_dir}/src/connector/grafana ${install_main_dir}/connector
    ${csudo} cp -rf ${source_dir}/src/connector/python ${install_main_dir}/connector
    ${csudo} cp -rf ${source_dir}/src/connector/go ${install_main_dir}/connector
        
    ${csudo} cp ${binary_dir}/build/lib/*.jar ${install_main_dir}/connector &> /dev/null && ${csudo} chmod 777 ${install_main_dir}/connector/*.jar || echo &> /dev/null 
}

function install_examples() {
    ${csudo} cp -rf ${source_dir}/tests/examples/* ${install_main_dir}/examples
}

function clean_service_on_sysvinit() {
    restart_config_str="taos:2345:respawn:${service_config_dir}/taosd start"
    if pidof taosd &> /dev/null; then
        ${csudo} service taosd stop || :
    fi
    ${csudo} sed -i "\|${restart_config_str}|d" /etc/inittab || :
    ${csudo} rm -f ${service_config_dir}/taosd || :
    ${csudo} update-rc.d -f taosd remove || :
    ${csudo} init q || :
}

function install_service_on_sysvinit() {
    clean_service_on_sysvinit

    sleep 1

    # Install taosd service
    ${csudo} cp ${script_dir}/../rpm/init.d/taosd ${service_config_dir} && ${csudo} chmod a+x ${service_config_dir}/taosd
    restart_config_str="taos:2345:respawn:${service_config_dir}/taosd start"

    ${csudo} grep -q -F "$restart_config_str" /etc/inittab || ${csudo} bash -c "echo '${restart_config_str}' >> /etc/inittab"
    # TODO: for centos, change here
    ${csudo} update-rc.d taosd defaults
    # chkconfig mysqld on
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
    if is_using_systemd; then
        install_service_on_systemd
    elif $(which update-rc.d &> /dev/null); then
        install_service_on_sysvinit
    fi
}

function update_TDengine() {
    echo -e "${GREEN}Start to update TDEngine...${NC}"
    # Stop the service if running
    if pidof taosd &> /dev/null; then
        if is_using_systemd; then
            ${csudo} systemctl stop taosd || :
        else
            ${csudo} service taosd stop || :
        fi
        sleep 1
    fi

    install_main_path

    install_log
    install_header
    install_lib
    install_bin
    # install_service
    install_config
    install_connector
    install_examples

    echo
    echo -e "\033[44;32;1mTDengine is updated successfully!${NC}"
    echo
    echo -e "${GREEN_DARK}To configure TDengine ${NC}: edit /etc/taos/taos.cfg"
    # if is_using_systemd; then
    #     echo -e "${GREEN_DARK}To start TDengine     ${NC}: ${csudo} systemctl start taosd${NC}"
    # else
    #     echo -e "${GREEN_DARK}To start TDengine     ${NC}: ${csudo} update-rc.d taosd default  ${RED} for the first time${NC}"
    #     echo -e "                      : ${csudo} service taosd start ${RED} after${NC}"
    # fi

    # echo -e "${GREEN_DARK}To access TDengine    ${NC}: use ${GREEN_UNDERLINE}taos${NC} in shell${NC}"
    echo
    echo -e "\033[44;32;1mTDengine is updated successfully!${NC}"
}

function install_TDengine() {
    # Start to install
    echo -e "${GREEN}Start to install TDEngine...${NC}"
	
	install_main_path
    install_data
    install_log 
    install_header
    install_bin
    install_lib
    # install_service
    install_config	    
    install_connector
    install_examples

    # Ask if to start the service
    echo
    echo -e "\033[44;32;1mTDengine is installed successfully!${NC}"
    echo
    echo -e "${GREEN_DARK}To configure TDengine ${NC}: edit /etc/taos/taos.cfg"
    # if is_using_systemd; then
    #    echo -e "${GREEN_DARK}To start TDengine     ${NC}: ${csudo} systemctl start taosd${NC}"
    # else
    #    echo -e "${GREEN_DARK}To start TDengine     ${NC}: ${csudo} update-rc.d taosd default  ${RED} for the first time${NC}"
    #    echo -e "                      : ${csudo} service taosd start ${RED} after${NC}"
    #3 fi

    # echo -e "${GREEN_DARK}To access TDengine    ${NC}: use ${GREEN_UNDERLINE}taos${NC} in shell${NC}"
    echo
    echo -e "\033[44;32;1mTDengine is installed successfully!${NC}"
}

## ==============================Main program starts from here============================
echo source directory: $1
echo binary directory: $2
if [ -x ${bin_dir}/taosd ]; then
    update_TDengine
else
    install_TDengine
fi
