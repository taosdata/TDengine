#!/bin/bash
#
# This file is used to install TAOS time-series database on linux systems. The operating system 
# is required to use systemd to manage services at boot

set -e
#set -x

# -----------------------Variables definition---------------------
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

# old bin dir
bin_dir="/usr/local/taos/bin"

service_config_dir="/etc/systemd/system"

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

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
    sudo rm -rf ${install_main_dir}    || :
    sudo mkdir -p ${install_main_dir}  
    sudo mkdir -p ${install_main_dir}/cfg
    sudo mkdir -p ${install_main_dir}/bin    
    sudo mkdir -p ${install_main_dir}/connector
    sudo mkdir -p ${install_main_dir}/driver
    sudo mkdir -p ${install_main_dir}/examples
    sudo mkdir -p ${install_main_dir}/include
    sudo mkdir -p ${install_main_dir}/init.d
}

function install_bin() {
    # Remove links
    sudo rm -f ${bin_link_dir}/taos     || :
    sudo rm -f ${bin_link_dir}/taosd    || :
    sudo rm -f ${bin_link_dir}/taosdump || :
    sudo rm -f ${bin_link_dir}/rmtaos   || :

    sudo cp -r ${script_dir}/bin/* ${install_main_dir}/bin && sudo chmod 0555 ${install_main_dir}/bin/*

    #Make link
    [ -x ${install_main_dir}/bin/taos ] && sudo ln -s ${install_main_dir}/bin/taos ${bin_link_dir}/taos             || :
    [ -x ${install_main_dir}/bin/taosd ] && sudo ln -s ${install_main_dir}/bin/taosd ${bin_link_dir}/taosd          || :   
    [ -x ${install_main_dir}/bin/taosdump ] && sudo ln -s ${install_main_dir}/bin/taosdump ${bin_link_dir}/taosdump || :
    [ -x ${install_main_dir}/bin/remove.sh ] && sudo ln -s ${install_main_dir}/bin/remove.sh ${bin_link_dir}/rmtaos || :
}

function install_lib() {
    # Remove links
    sudo rm -f ${lib_link_dir}/libtaos.*     || :    

    sudo cp -rf ${script_dir}/driver/* ${install_main_dir}/driver && sudo chmod 777 ${install_main_dir}/driver/*  
    
    sudo ln -s ${install_main_dir}/driver/libtaos.* ${lib_link_dir}/libtaos.so.1
    sudo ln -s ${lib_link_dir}/libtaos.so.1 ${lib_link_dir}/libtaos.so
}

function install_header() {
    sudo rm -f ${inc_link_dir}/taos.h     || :    
    sudo cp -f ${script_dir}/inc/* ${install_main_dir}/include && sudo chmod 644 ${install_main_dir}/include/*    
    sudo ln -s ${install_main_dir}/include/taos.h ${inc_link_dir}/taos.h
}

function install_config() {
    #sudo rm -f ${install_main_dir}/cfg/taos.cfg     || :
    
    if [ ! -f ${cfg_install_dir}/taos.cfg ]; then  
        sudo sudo mkdir -p ${cfg_install_dir}
        [ -f ${script_dir}/cfg/taos.cfg ] && sudo cp ${script_dir}/cfg/taos.cfg ${cfg_install_dir}
        sudo chmod 644 ${cfg_install_dir}/*
    fi 
    
    sudo cp -f ${script_dir}/cfg/taos.cfg ${install_main_dir}/cfg/taos.cfg.org
    sudo ln -s ${cfg_install_dir}/taos.cfg ${install_main_dir}/cfg   
}

function install_log() {
    sudo rm -rf ${log_dir}  || :
    sudo mkdir -p ${log_dir} && sudo chmod 777 ${log_dir}
    
    sudo ln -s ${log_dir} ${install_main_dir}/log
}

function install_data() {
    sudo mkdir -p ${data_dir}
    
    sudo ln -s ${data_dir} ${install_main_dir}/data    
}

function install_connector() {
    sudo cp -rf ${script_dir}/connector/* ${install_main_dir}/connector
}

function install_examples() {
    sudo cp -rf ${script_dir}/examples/* ${install_main_dir}/examples
}

function clean_service_on_sysvinit() {
    restart_config_str="taos:2345:respawn:${service_config_dir}/taosd start"
    if pidof taosd &> /dev/null; then
        sudo service taosd stop || :
    fi
    sudo sed -i "\|${restart_config_str}|d" /etc/inittab || :
    sudo rm -f ${service_config_dir}/taosd || :
    sudo update-rc.d -f taosd remove || :
    sudo init q || :
}

function install_service_on_sysvinit() {
    clean_service_on_sysvinit

    sleep 1

    # Install taosd service
    sudo cp -f ${script_dir}/init.d/taosd ${install_main_dir}/init.d
    sudo cp ${script_dir}/init.d/taosd ${service_config_dir} && sudo chmod a+x ${service_config_dir}/taosd
    restart_config_str="taos:2345:respawn:${service_config_dir}/taosd start"

    sudo grep -q -F "$restart_config_str" /etc/inittab || sudo bash -c "echo '${restart_config_str}' >> /etc/inittab"
    # TODO: for centos, change here
    sudo update-rc.d taosd defaults
    # chkconfig mysqld on
}

function clean_service_on_systemd() {
    taosd_service_config="${service_config_dir}/taosd.service"

    if systemctl is-active --quiet taosd; then
        echo "TDengine is running, stopping it..."
        sudo systemctl stop taosd &> /dev/null || echo &> /dev/null
    fi
    sudo systemctl disable taosd &> /dev/null || echo &> /dev/null

    sudo rm -f ${taosd_service_config}
}   

# taos:2345:respawn:/etc/init.d/taosd start

function install_service_on_systemd() {
    clean_service_on_systemd

    taosd_service_config="${service_config_dir}/taosd.service"

    sudo bash -c "echo '[Unit]'                             >> ${taosd_service_config}"
    sudo bash -c "echo 'Description=TDengine server service' >> ${taosd_service_config}"
    sudo bash -c "echo 'After=network-online.target'        >> ${taosd_service_config}"
    sudo bash -c "echo 'Wants=network-online.target'        >> ${taosd_service_config}"
    sudo bash -c "echo                                      >> ${taosd_service_config}"
    sudo bash -c "echo '[Service]'                          >> ${taosd_service_config}"
    sudo bash -c "echo 'Type=simple'                        >> ${taosd_service_config}"
    sudo bash -c "echo 'ExecStart=/usr/bin/taosd'           >> ${taosd_service_config}"
    sudo bash -c "echo 'LimitNOFILE=infinity'               >> ${taosd_service_config}"
    sudo bash -c "echo 'LimitNPROC=infinity'                >> ${taosd_service_config}"
    sudo bash -c "echo 'LimitCORE=infinity'                 >> ${taosd_service_config}"
    sudo bash -c "echo 'TimeoutStartSec=0'                  >> ${taosd_service_config}"
    sudo bash -c "echo 'StandardOutput=null'                >> ${taosd_service_config}"
    sudo bash -c "echo 'Restart=always'                     >> ${taosd_service_config}"
    sudo bash -c "echo 'StartLimitBurst=3'                  >> ${taosd_service_config}"
    sudo bash -c "echo 'StartLimitInterval=60s'             >> ${taosd_service_config}"
    sudo bash -c "echo                                      >> ${taosd_service_config}"
    sudo bash -c "echo '[Install]'                          >> ${taosd_service_config}"
    sudo bash -c "echo 'WantedBy=multi-user.target'         >> ${taosd_service_config}"
    sudo systemctl enable taosd
}

function install_service() {
    if is_using_systemd; then
        install_service_on_systemd
    else
        install_service_on_sysvinit
    fi
}

vercomp () {
    if [[ $1 == $2 ]]; then
        return 0
    fi
    local IFS=.
    local i ver1=($1) ver2=($2)
    # fill empty fields in ver1 with zeros
    for ((i=${#ver1[@]}; i<${#ver2[@]}; i++)); do
        ver1[i]=0
    done

    for ((i=0; i<${#ver1[@]}; i++)); do
        if [[ -z ${ver2[i]} ]]
        then
            # fill empty fields in ver2 with zeros
            ver2[i]=0
        fi
        if ((10#${ver1[i]} > 10#${ver2[i]}))
        then
            return 1
        fi
        if ((10#${ver1[i]} < 10#${ver2[i]}))
        then
            return 2
        fi
    done
    return 0
}

function is_version_compatible() {

    curr_version=$(${bin_dir}/taosd -V | cut -d ' ' -f 1)

    min_compatible_version=$(${script_dir}/bin/taosd -V | cut -d ' ' -f 2)

    vercomp $curr_version $min_compatible_version
    case $? in
        0) return 0;;
        1) return 0;;
        2) return 1;;
    esac
}

function update_TDengine() {
    # Start to update
    if [ ! -e taos.tar.gz ]; then
        echo "File taos.tar.gz does not exist"
        exit 1
    fi
    tar -zxf taos.tar.gz 

    # Check if version compatible
    if ! is_version_compatible; then
        echo -e "${RED}Version incompatible${NC}"
        return 1
    fi

    echo -e "${GREEN}Start to update TDEngine...${NC}"
    # Stop the service if running
    if pidof taosd &> /dev/null; then
        if is_using_systemd; then
            sudo systemctl stop taosd || :
        else
            sudo service taosd stop || :
        fi
        sleep 1
    fi
    
    install_main_path

    install_log
    install_header
    install_lib
    install_connector
    install_examples
    if [ -z $1 ]; then
        install_bin
        install_service
        install_config

        echo
        echo -e "\033[44;32;1mTDengine is updated successfully!${NC}"
        echo
        echo -e "${GREEN_DARK}To configure TDengine ${NC}: edit /etc/taos/taos.cfg"
        if is_using_systemd; then
            echo -e "${GREEN_DARK}To start TDengine     ${NC}: sudo systemctl start taosd${NC}"
        else
            echo -e "${GREEN_DARK}To start TDengine     ${NC}: sudo update-rc.d taosd default  ${RED} for the first time${NC}"
            echo -e "                      : sudo service taosd start ${RED} after${NC}"
        fi

        echo -e "${GREEN_DARK}To access TDengine    ${NC}: use ${GREEN_UNDERLINE}taos${NC} in shell${NC}"
        echo
        echo -e "\033[44;32;1mTDengine is updated successfully!${NC}"
    else
        install_bin $1
        install_config

        echo
        echo -e "\033[44;32;1mTDengine client is updated successfully!${NC}"
    fi

    rm -rf $(tar -tf taos.tar.gz)
}

function install_TDengine() {
    # Start to install
    if [ ! -e taos.tar.gz ]; then
        echo "File taos.tar.gz does not exist"
        exit 1
    fi
    tar -zxf taos.tar.gz 

    echo -e "${GREEN}Start to install TDEngine...${NC}"
    
	  install_main_path
	   
    if [ -z $1 ]; then
        install_data
    fi 
    
    install_log 
    install_header
    install_lib
    install_connector
    install_examples

    if [ -z $1 ]; then # install service and client
        # For installing new
        install_bin
        install_service
        install_config	

        # Ask if to start the service
        echo
        echo -e "\033[44;32;1mTDengine is installed successfully!${NC}"
        echo
        echo -e "${GREEN_DARK}To configure TDengine ${NC}: edit /etc/taos/taos.cfg"
        if is_using_systemd; then
            echo -e "${GREEN_DARK}To start TDengine     ${NC}: sudo systemctl start taosd${NC}"
        else
            echo -e "${GREEN_DARK}To start TDengine     ${NC}: sudo update-rc.d taosd default  ${RED} for the first time${NC}"
            echo -e "                      : sudo service taosd start ${RED} after${NC}"
        fi

        echo -e "${GREEN_DARK}To access TDengine    ${NC}: use ${GREEN_UNDERLINE}taos${NC} in shell${NC}"
        echo
        echo -e "\033[44;32;1mTDengine is installed successfully!${NC}"
    else # Only install client
        install_bin
        install_config

        echo
        echo -e "\033[44;32;1mTDengine client is installed successfully!${NC}"
    fi

    rm -rf $(tar -tf taos.tar.gz)
}


## ==============================Main program starts from here============================
if [ -z $1 ]; then
    # Install server and client
    if [ -x ${bin_dir}/taosd ]; then
        update_TDengine
    else
        install_TDengine
    fi
else
    # Only install client
    if [ -x ${bin_dir}/taos ]; then
        update_TDengine client
    else
        install_TDengine client
    fi
fi
