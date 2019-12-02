#!/bin/bash
#
# This file is used to install database on linux systems. The operating system
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

csudo=""
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
  echo "this is other linux system"
  os_type=0
fi

function kill_taosd() {
  pid=$(ps -ef | grep "taosd" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo} kill -9 $pid   || :
  fi
}

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
    ${csudo} rm -f ${bin_link_dir}/taosdemo || :
    ${csudo} rm -f ${bin_link_dir}/taosdump || :
    ${csudo} rm -f ${bin_link_dir}/rmtaos   || :

    ${csudo} cp -r ${script_dir}/bin/* ${install_main_dir}/bin && ${csudo} chmod 0555 ${install_main_dir}/bin/*

    #Make link
    [ -x ${install_main_dir}/bin/taos ] && ${csudo} ln -s ${install_main_dir}/bin/taos ${bin_link_dir}/taos             || :
    [ -x ${install_main_dir}/bin/taosd ] && ${csudo} ln -s ${install_main_dir}/bin/taosd ${bin_link_dir}/taosd          || :
    [ -x ${install_main_dir}/bin/taosdump ] && ${csudo} ln -s ${install_main_dir}/bin/taosdump ${bin_link_dir}/taosdump || :
    [ -x ${install_main_dir}/bin/taosdemo ] && ${csudo} ln -s ${install_main_dir}/bin/taosdemo ${bin_link_dir}/taosdemo || :
    [ -x ${install_main_dir}/bin/remove.sh ] && ${csudo} ln -s ${install_main_dir}/bin/remove.sh ${bin_link_dir}/rmtaos || :
}

function install_lib() {
    # Remove links
    ${csudo} rm -f ${lib_link_dir}/libtaos.*         || :

    ${csudo} cp -rf ${script_dir}/driver/* ${install_main_dir}/driver && ${csudo} chmod 777 ${install_main_dir}/driver/*  
    
    ${csudo} ln -s ${install_main_dir}/driver/libtaos.* ${lib_link_dir}/libtaos.so.1
    ${csudo} ln -s ${lib_link_dir}/libtaos.so.1 ${lib_link_dir}/libtaos.so
}

function install_header() {
    ${csudo} rm -f ${inc_link_dir}/taos.h ${inc_link_dir}/taoserror.h    || :
    ${csudo} cp -f ${script_dir}/inc/* ${install_main_dir}/include && ${csudo} chmod 644 ${install_main_dir}/include/*    
    ${csudo} ln -s ${install_main_dir}/include/taos.h ${inc_link_dir}/taos.h
    ${csudo} ln -s ${install_main_dir}/include/taoserror.h ${inc_link_dir}/taoserror.h
}

function install_config() {
    #${csudo} rm -f ${install_main_dir}/cfg/taos.cfg     || :
    
    if [ ! -f ${cfg_install_dir}/taos.cfg ]; then
        ${csudo} mkdir -p ${cfg_install_dir}
        [ -f ${script_dir}/cfg/taos.cfg ] && ${csudo} cp ${script_dir}/cfg/taos.cfg ${cfg_install_dir}
        ${csudo} chmod 644 ${cfg_install_dir}/*
    fi 
    
    ${csudo} cp -f ${script_dir}/cfg/taos.cfg ${install_main_dir}/cfg/taos.cfg.org
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
    ${csudo} cp -rf ${script_dir}/connector/* ${install_main_dir}/connector
}

function install_examples() {
    ${csudo} cp -rf ${script_dir}/examples/* ${install_main_dir}/examples
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
        ${csudo} cp -f ${script_dir}/init.d/taosd.deb ${install_main_dir}/init.d/taosd
        ${csudo} cp    ${script_dir}/init.d/taosd.deb ${service_config_dir}/taosd && ${csudo} chmod a+x ${service_config_dir}/taosd
    elif ((${os_type}==2)); then
        ${csudo} cp -f ${script_dir}/init.d/taosd.rpm ${install_main_dir}/init.d/taosd
        ${csudo} cp    ${script_dir}/init.d/taosd.rpm ${service_config_dir}/taosd && ${csudo} chmod a+x ${service_config_dir}/taosd
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

    curr_version=$(${bin_dir}/taosd -V | head -1 | cut -d ' ' -f 2)

    min_compatible_version=$(${script_dir}/bin/taosd -V | head -1 | cut -d ' ' -f 4)

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

    echo -e "${GREEN}Start to update TDengine...${NC}"
    # Stop the service if running
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
        install_bin
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

    echo -e "${GREEN}Start to install TDengine...${NC}"
    
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
        if ((${service_mod}==0)); then
            echo -e "${GREEN_DARK}To start TDengine     ${NC}: ${csudo} systemctl start taosd${NC}"
        elif ((${service_mod}==1)); then
            echo -e "${GREEN_DARK}To start TDengine     ${NC}: ${csudo} service taosd start${NC}"
        else
            echo -e "${GREEN_DARK}To start TDengine     ${NC}: taosd${NC}"
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
