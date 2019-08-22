#!/bin/bash
#
# This file is used to install tdengine rpm package on centos systems. The operating system 
# is required to use systemd to manage services at boot
#set -x
# -----------------------Variables definition---------------------
script_dir=$(dirname $(readlink -m "$0"))
# Dynamic directory
data_dir="/var/lib/taos"
log_dir="/var/log/taos"
data_link_dir="/usr/local/taos/data"
log_link_dir="/usr/local/taos/log"

# static directory
cfg_dir="/usr/local/taos/cfg"
bin_dir="/usr/local/taos/bin"
lib_dir="/usr/local/taos/driver"
init_d_dir="/usr/local/taos/init.d"
inc_dir="/usr/local/taos/include"

cfg_install_dir="/etc/taos"
bin_link_dir="/usr/bin"
lib_link_dir="/usr/lib"
inc_link_dir="/usr/include"

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

service_mod=2
if pidof systemd &> /dev/null; then
    service_mod=0
elif $(which update-rc.d &> /dev/null); then
    service_mod=1
    service_config_dir="/etc/init.d"
else 
    service_mod=2
fi

function kill_taosd() {
  ${csudo} pkill -f taosd || :
}

function is_using_systemd() {
    if pidof systemd &> /dev/null; then
        return 0
    else
        return 1
    fi
}

function install_include() {
    ${csudo} rm -f ${inc_link_dir}/taos.h || :
    ${csudo} ln -s ${inc_dir}/taos.h ${inc_link_dir}/taos.h  
}

function install_lib() {
    ${csudo} rm -f ${lib_link_dir}/libtaos.so || :
    
    ${csudo} ln -s ${lib_dir}/libtaos.* ${lib_link_dir}/libtaos.so.1
    ${csudo} ln -s ${lib_link_dir}/libtaos.so.1 ${lib_link_dir}/libtaos.so
}

function install_bin() {
    # Remove links
    ${csudo} rm -f ${bin_link_dir}/taos     || :
    ${csudo} rm -f ${bin_link_dir}/taosd    || :
    ${csudo} rm -f ${bin_link_dir}/taosdump || :
    ${csudo} rm -f ${bin_link_dir}/taosdemo || :
    ${csudo} rm -f ${bin_link_dir}/rmtaos   || :

    ${csudo} chmod 0555 ${bin_dir}/*

    #Make link
    [ -x ${bin_dir}/taos ] && ${csudo} ln -s ${bin_dir}/taos ${bin_link_dir}/taos             || :
    [ -x ${bin_dir}/taosd ] && ${csudo} ln -s ${bin_dir}/taosd ${bin_link_dir}/taosd          || :
    [ -x ${bin_dir}/taosdump ] && ${csudo} ln -s ${bin_dir}/taosdump ${bin_link_dir}/taosdump || :
    [ -x ${bin_dir}/taosdemo ] && ${csudo} ln -s ${bin_dir}/taosdemo ${bin_link_dir}/taosdemo || :
#   [ -x ${bin_dir}/remove.sh ] && ${csudo} ln -s ${bin_dir}/remove.sh ${bin_link_dir}/rmtaos || :
}

function install_config() {
    if [ ! -f ${cfg_install_dir}/taos.cfg ]; then
        ${csudo} ${csudo} mkdir -p ${cfg_install_dir}
        [ -f ${cfg_dir}/taos.cfg ] && ${csudo} cp ${cfg_dir}/taos.cfg ${cfg_install_dir}
        ${csudo} chmod 644 ${cfg_install_dir}/*
    fi

    ${csudo} mv ${cfg_dir}/taos.cfg ${cfg_dir}/taos.cfg.org
    ${csudo} ln -s ${cfg_install_dir}/taos.cfg ${cfg_dir}
}

function clean_service_on_sysvinit() {
    restart_config_str="taos:2345:respawn:${service_config_dir}/taosd start"
    #if pidof taosd &> /dev/null; then
    #    ${csudo} service taosd stop || :
    #fi
    ${csudo} sed -i "\|${restart_config_str}|d" /etc/inittab || :
    ${csudo} rm -f ${service_config_dir}/taosd || :
    ${csudo} update-rc.d -f taosd remove || :
    ${csudo} init q || :
}

function install_service_on_sysvinit() {
    clean_service_on_sysvinit

    sleep 1

    # Install taosd service  
    ${csudo} cp %{init_d_dir}/taosd ${service_config_dir} && ${csudo} chmod a+x ${service_config_dir}/taosd

    restart_config_str="taos:2345:respawn:${service_config_dir}/taosd start"

    ${csudo} grep -q -F "$restart_config_str" /etc/inittab || ${csudo} bash -c "echo '${restart_config_str}' >> /etc/inittab"
    # TODO: for centos, change here
    ${csudo} update-rc.d taosd defaults
    # chkconfig mysqld on
}

function clean_service_on_systemd() {
    taosd_service_config="${service_config_dir}/taosd.service"

    # taosd service already is stoped before install 
    #if systemctl is-active --quiet taosd; then
    #    echo "TDengine is running, stopping it..."
    #    ${csudo} systemctl stop taosd &> /dev/null || echo &> /dev/null
    #fi
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
    if ((${service_mod}==0)); then
        install_service_on_systemd
    elif ((${service_mod}==1)); then
        install_service_on_sysvinit
    else
        # manual start taosd
        kill_taosd
    fi
}

function install_TDengine() {
    echo -e "${GREEN}Start to install TDEngine...${NC}"

    #install log and data dir , then ln to /usr/local/taos
    ${csudo} mkdir -p ${log_dir} && ${csudo} chmod 777 ${log_dir}
    ${csudo} mkdir -p ${data_dir} 
    
    ${csudo} rm -rf ${log_link_dir}   || :
    ${csudo} rm -rf ${data_link_dir}  || :
    
    ${csudo} ln -s ${log_dir} ${log_link_dir}     || :
    ${csudo} ln -s ${data_dir} ${data_link_dir}   || :
    
    # Install include, lib, binary and service
    install_include
    install_lib
    install_bin
    install_service
    install_config	

    # Ask if to start the service
    echo
    echo -e "\033[44;32;1mTDengine is installed successfully!${NC}"
    echo
    echo -e "${GREEN_DARK}To configure TDengine ${NC}: edit /etc/taos/taos.cfg"
    if ((${service_mod}==0)); then
        echo -e "${GREEN_DARK}To start TDengine     ${NC}: ${csudo} systemctl start taosd${NC}"
    elif ((${service_mod}==1)); then
        echo -e "${GREEN_DARK}To start TDengine     ${NC}: ${csudo} update-rc.d taosd default  ${RED} for the first time${NC}"
        echo -e "                      : ${csudo} service taosd start ${RED} after${NC}"
    else 
        echo -e "${GREEN_DARK}To start TDengine     ${NC}: ./taosd${NC}"
    fi

    echo -e "${GREEN_DARK}To access TDengine    ${NC}: use ${GREEN_UNDERLINE}taos${NC} in shell${NC}"

    echo
    echo -e "\033[44;32;1mTDengine is installed successfully!${NC}"
}


## ==============================Main program starts from here============================
install_TDengine
