#!/bin/bash
#
# This file is used to install database on linux systems. The operating system
# is required to use systemd to manage services at boot

set -e
#set -x

verMode=edge
pagMode=full

# -----------------------Variables definition---------------------
script_dir=$(dirname $(readlink -f "$0"))
# Dynamic directory
data_dir="/var/lib/DB_CLIENT_NAME"
log_dir="/var/log/DB_CLIENT_NAME"

data_link_dir="/usr/local/DB_CLIENT_NAME/data"
log_link_dir="/usr/local/DB_CLIENT_NAME/log"

cfg_install_dir="/etc/DB_CLIENT_NAME"

bin_link_dir="/usr/bin"
lib_link_dir="/usr/lib"
inc_link_dir="/usr/include"

#install main path
install_main_dir="/usr/local/DB_CLIENT_NAME"

# old bin dir
bin_dir="/usr/local/DB_CLIENT_NAME/bin"

# v1.5 jar dir
v15_java_app_dir="/usr/local/lib/taos"

service_config_dir="/etc/systemd/system"
nginx_port=6060
nginx_dir="/usr/local/nginxd"

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

update_flag=0

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
  echo "This is ubuntu system"
  os_type=1
elif echo $osinfo | grep -qwi "debian" ; then
  echo "This is debian system"
  os_type=1
elif echo $osinfo | grep -qwi "Kylin" ; then
  echo "This is Kylin system"
  os_type=1
elif  echo $osinfo | grep -qwi "centos" ; then
  echo "This is centos system"
  os_type=2
elif echo $osinfo | grep -qwi "fedora" ; then
  echo "This is fedora system"
  os_type=2
else
  echo "${osinfo}: This is an officially unverified linux system, If there are any problems with the installation and operation, "
  echo "please feel free to contact us for support."
  os_type=1
fi

function kill_DB_SERVICE_NAME() {
  pid=$(ps -ef | grep "DB_SERVICE_NAME" | grep -v "grep" | awk '{print $2}')
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
    if [ "$verMode" == "cluster" ]; then
        ${csudo} mkdir -p ${nginx_dir}
    fi
}

function install_bin() {
    # Remove links
    ${csudo} rm -f ${bin_link_dir}/DB_CLIENT_NAME     || :
    ${csudo} rm -f ${bin_link_dir}/DB_SERVICE_NAME    || :
    ${csudo} rm -f ${bin_link_dir}/DB_CLIENT_NAMEdemo || :
    ${csudo} rm -f ${bin_link_dir}/DB_CLIENT_NAMEdump || :
    ${csudo} rm -f ${bin_link_dir}/rmDB_CLIENT_NAME   || :

    ${csudo} cp -r ${script_dir}/bin/* ${install_main_dir}/bin && ${csudo} chmod 0555 ${install_main_dir}/bin/*

    #Make link
    [ -x ${install_main_dir}/bin/DB_CLIENT_NAME ] && ${csudo} ln -s ${install_main_dir}/bin/DB_CLIENT_NAME ${bin_link_dir}/DB_CLIENT_NAME             || :
    [ -x ${install_main_dir}/bin/DB_SERVICE_NAME ] && ${csudo} ln -s ${install_main_dir}/bin/DB_SERVICE_NAME ${bin_link_dir}/DB_SERVICE_NAME          || :
    [ -x ${install_main_dir}/bin/DB_CLIENT_NAMEdump ] && ${csudo} ln -s ${install_main_dir}/bin/DB_CLIENT_NAMEdump ${bin_link_dir}/DB_CLIENT_NAMEdump || :
    [ -x ${install_main_dir}/bin/DB_CLIENT_NAMEdemo ] && ${csudo} ln -s ${install_main_dir}/bin/DB_CLIENT_NAMEdemo ${bin_link_dir}/DB_CLIENT_NAMEdemo || :
    [ -x ${install_main_dir}/bin/remove.sh ] && ${csudo} ln -s ${install_main_dir}/bin/remove.sh ${bin_link_dir}/rmDB_CLIENT_NAME || :

    if [ "$verMode" == "cluster" ]; then
        ${csudo} cp -r ${script_dir}/nginxd/* ${nginx_dir} && ${csudo} chmod 0555 ${nginx_dir}/*
        ${csudo} mkdir -p ${nginx_dir}/logs
        ${csudo} chmod 777 ${nginx_dir}/sbin/nginx
    fi
}

function install_lib() {
    # Remove links
    ${csudo} rm -f ${lib_link_dir}/libtaos.*         || :
    ${csudo} rm -rf ${v15_java_app_dir}              || :

    ${csudo} cp -rf ${script_dir}/driver/* ${install_main_dir}/driver && ${csudo} chmod 777 ${install_main_dir}/driver/*  
    
    ${csudo} ln -s ${install_main_dir}/driver/libtaos.* ${lib_link_dir}/libtaos.so.1
    ${csudo} ln -s ${lib_link_dir}/libtaos.so.1 ${lib_link_dir}/libtaos.so
    
	if [ "$verMode" == "cluster" ]; then
        # Compatible with version 1.5
        ${csudo} mkdir -p ${v15_java_app_dir}
        ${csudo} ln -s ${install_main_dir}/connector/taos-jdbcdriver-1.0.2-dist.jar ${v15_java_app_dir}/JDBCDriver-1.0.2-dist.jar
        ${csudo} chmod 777 ${v15_java_app_dir} || :
    fi
}

function install_header() {
    ${csudo} rm -f ${inc_link_dir}/taos.h ${inc_link_dir}/taoserror.h    || :
    ${csudo} cp -f ${script_dir}/inc/* ${install_main_dir}/include && ${csudo} chmod 644 ${install_main_dir}/include/*    
    ${csudo} ln -s ${install_main_dir}/include/taos.h ${inc_link_dir}/taos.h
    ${csudo} ln -s ${install_main_dir}/include/taoserror.h ${inc_link_dir}/taoserror.h
}

function install_config() {
    #${csudo} rm -f ${install_main_dir}/cfg/DB_CLIENT_NAME.cfg     || :
    
    if [ ! -f ${cfg_install_dir}/DB_CLIENT_NAME.cfg ]; then
        ${csudo} mkdir -p ${cfg_install_dir}
        [ -f ${script_dir}/cfg/DB_CLIENT_NAME.cfg ] && ${csudo} cp ${script_dir}/cfg/DB_CLIENT_NAME.cfg ${cfg_install_dir}
        ${csudo} chmod 644 ${cfg_install_dir}/*
    fi 
    
    ${csudo} cp -f ${script_dir}/cfg/DB_CLIENT_NAME.cfg ${install_main_dir}/cfg/DB_CLIENT_NAME.cfg.org
    ${csudo} ln -s ${cfg_install_dir}/DB_CLIENT_NAME.cfg ${install_main_dir}/cfg

    if [ "$verMode" == "cluster" ]; then
        [ ! -z $1 ] && return 0 || : # only install client
    
        if ((${update_flag}==1)); then
            return 0
        fi

        IP_FORMAT="(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)"
        IP_PATTERN="\b$IP_FORMAT\.$IP_FORMAT\.$IP_FORMAT\.$IP_FORMAT\b"

        echo
        echo -e -n "${GREEN}Enter the IP address of an existing DB_FULL_NAME cluster node to join${NC} OR ${GREEN}leave it blank to build one${NC} :"
        read masterIp
        while true; do
            if [ ! -z "$masterIp" ]; then
                # check the format of the masterIp
                if [[ $masterIp =~ $IP_PATTERN ]]; then
                    # Write the first IP to configuration file
                    sudo sed -i -r "s/#*\s*(masterIp\s*).*/\1$masterIp/" ${cfg_dir}/DB_CLIENT_NAME.cfg

                    # Get the second IP address

                    echo
                    echo -e -n "${GREEN}Enter the IP address of another node in cluster${NC} OR ${GREEN}leave it blank to skip${NC}: "
                    read secondIp
                    while true; do

                        if [ ! -z "$secondIp" ]; then
                            if [[ $secondIp =~ $IP_PATTERN ]]; then
                                # Write the second IP to configuration file
                                sudo sed -i -r "s/#*\s*(secondIp\s*).*/\1$secondIp/" ${cfg_dir}/DB_CLIENT_NAME.cfg
                                break
                            else
                                read -p "Please enter the correct IP address: " secondIp
                            fi
                        else
                            break
                        fi
                    done
    
                    break
                else
                    read -p "Please enter the correct IP address: " masterIp
                fi
            else
                break
            fi
        done
	
	fi
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
    if [ -d ${script_dir}/examples ]; then
        ${csudo} cp -rf ${script_dir}/examples/* ${install_main_dir}/examples
    fi
}

function clean_service_on_sysvinit() {
    #restart_config_str="DB_CLIENT_NAME:2345:respawn:${service_config_dir}/DB_SERVICE_NAME start"
    #${csudo} sed -i "\|${restart_config_str}|d" /etc/inittab || :    
    
    if pidof DB_SERVICE_NAME &> /dev/null; then
        ${csudo} service DB_SERVICE_NAME stop || :
    fi

    if ((${initd_mod}==1)); then
        ${csudo} chkconfig --del DB_SERVICE_NAME || :
    elif ((${initd_mod}==2)); then
        ${csudo} insserv -r DB_SERVICE_NAME || :
    elif ((${initd_mod}==3)); then
        ${csudo} update-rc.d -f DB_SERVICE_NAME remove || :
    fi
    
    ${csudo} rm -f ${service_config_dir}/DB_SERVICE_NAME || :
    
    if $(which init &> /dev/null); then
        ${csudo} init q || :
    fi
}

function install_service_on_sysvinit() {
    clean_service_on_sysvinit

    sleep 1

    # Install DB_SERVICE_NAME service

    if ((${os_type}==1)); then
        ${csudo} cp -f ${script_dir}/init.d/DB_SERVICE_NAME.deb ${install_main_dir}/init.d/DB_SERVICE_NAME
        ${csudo} cp    ${script_dir}/init.d/DB_SERVICE_NAME.deb ${service_config_dir}/DB_SERVICE_NAME && ${csudo} chmod a+x ${service_config_dir}/DB_SERVICE_NAME
    elif ((${os_type}==2)); then
        ${csudo} cp -f ${script_dir}/init.d/DB_SERVICE_NAME.rpm ${install_main_dir}/init.d/DB_SERVICE_NAME
        ${csudo} cp    ${script_dir}/init.d/DB_SERVICE_NAME.rpm ${service_config_dir}/DB_SERVICE_NAME && ${csudo} chmod a+x ${service_config_dir}/DB_SERVICE_NAME
    fi
    
    #restart_config_str="DB_CLIENT_NAME:2345:respawn:${service_config_dir}/DB_SERVICE_NAME start"
    #${csudo} grep -q -F "$restart_config_str" /etc/inittab || ${csudo} bash -c "echo '${restart_config_str}' >> /etc/inittab"
    
    if ((${initd_mod}==1)); then
        ${csudo} chkconfig --add DB_SERVICE_NAME || :
        ${csudo} chkconfig --level 2345 DB_SERVICE_NAME on || :
    elif ((${initd_mod}==2)); then
        ${csudo} insserv DB_SERVICE_NAME || :
        ${csudo} insserv -d DB_SERVICE_NAME || :
    elif ((${initd_mod}==3)); then
        ${csudo} update-rc.d DB_SERVICE_NAME defaults || :
    fi
}

function clean_service_on_systemd() {
    DB_SERVICE_NAME_service_config="${service_config_dir}/DB_SERVICE_NAME.service"

    if systemctl is-active --quiet DB_SERVICE_NAME; then
        echo "DB_FULL_NAME is running, stopping it..."
        ${csudo} systemctl stop DB_SERVICE_NAME &> /dev/null || echo &> /dev/null
    fi
    ${csudo} systemctl disable DB_SERVICE_NAME &> /dev/null || echo &> /dev/null

    ${csudo} rm -f ${DB_SERVICE_NAME_service_config}

    if [ "$verMode" == "cluster" ]; then
        nginx_service_config="${service_config_dir}/nginxd.service"
	
        if systemctl is-active --quiet nginxd; then
            echo "Nginx for DB_FULL_NAME is running, stopping it..."
            ${csudo} systemctl stop nginxd &> /dev/null || echo &> /dev/null
        fi
        ${csudo} systemctl disable nginxd &> /dev/null || echo &> /dev/null
	
        ${csudo} rm -f ${nginx_service_config}
	fi
}

# DB_CLIENT_NAME:2345:respawn:/etc/init.d/DB_SERVICE_NAME start

function install_service_on_systemd() {
    clean_service_on_systemd

    DB_SERVICE_NAME_service_config="${service_config_dir}/DB_SERVICE_NAME.service"

    ${csudo} bash -c "echo '[Unit]'                             >> ${DB_SERVICE_NAME_service_config}"
    ${csudo} bash -c "echo 'Description=DB_FULL_NAME server service' >> ${DB_SERVICE_NAME_service_config}"
    ${csudo} bash -c "echo 'After=network-online.target'        >> ${DB_SERVICE_NAME_service_config}"
    ${csudo} bash -c "echo 'Wants=network-online.target'        >> ${DB_SERVICE_NAME_service_config}"
    ${csudo} bash -c "echo                                      >> ${DB_SERVICE_NAME_service_config}"
    ${csudo} bash -c "echo '[Service]'                          >> ${DB_SERVICE_NAME_service_config}"
    ${csudo} bash -c "echo 'Type=simple'                        >> ${DB_SERVICE_NAME_service_config}"
    ${csudo} bash -c "echo 'ExecStart=/usr/bin/DB_SERVICE_NAME' >> ${DB_SERVICE_NAME_service_config}"
    ${csudo} bash -c "echo 'LimitNOFILE=infinity'               >> ${DB_SERVICE_NAME_service_config}"
    ${csudo} bash -c "echo 'LimitNPROC=infinity'                >> ${DB_SERVICE_NAME_service_config}"
    ${csudo} bash -c "echo 'LimitCORE=infinity'                 >> ${DB_SERVICE_NAME_service_config}"
    ${csudo} bash -c "echo 'TimeoutStartSec=0'                  >> ${DB_SERVICE_NAME_service_config}"
    ${csudo} bash -c "echo 'StandardOutput=null'                >> ${DB_SERVICE_NAME_service_config}"
    ${csudo} bash -c "echo 'Restart=always'                     >> ${DB_SERVICE_NAME_service_config}"
    ${csudo} bash -c "echo 'StartLimitBurst=3'                  >> ${DB_SERVICE_NAME_service_config}"
    ${csudo} bash -c "echo 'StartLimitInterval=60s'             >> ${DB_SERVICE_NAME_service_config}"
    ${csudo} bash -c "echo                                      >> ${DB_SERVICE_NAME_service_config}"
    ${csudo} bash -c "echo '[Install]'                          >> ${DB_SERVICE_NAME_service_config}"
    ${csudo} bash -c "echo 'WantedBy=multi-user.target'         >> ${DB_SERVICE_NAME_service_config}"
    ${csudo} systemctl enable DB_SERVICE_NAME

    if [ "$verMode" == "cluster" ]; then		
        nginx_service_config="${service_config_dir}/nginxd.service"
        ${csudo} bash -c "echo '[Unit]'                             >> ${nginx_service_config}"
        ${csudo} bash -c "echo 'Description=Nginx For DB_FULL_NAME Service' >> ${nginx_service_config}"
        ${csudo} bash -c "echo 'After=network-online.target'        >> ${nginx_service_config}"
        ${csudo} bash -c "echo 'Wants=network-online.target'        >> ${nginx_service_config}"
        ${csudo} bash -c "echo                                      >> ${nginx_service_config}"
        ${csudo} bash -c "echo '[Service]'                          >> ${nginx_service_config}"
        ${csudo} bash -c "echo 'Type=forking'                       >> ${nginx_service_config}"
        ${csudo} bash -c "echo 'PIDFile=/usr/local/nginxd/logs/nginx.pid'        >> ${nginx_service_config}"
        ${csudo} bash -c "echo 'ExecStart=/usr/local/nginxd/sbin/nginx'          >> ${nginx_service_config}"
        ${csudo} bash -c "echo 'ExecStop=/usr/local/nginxd/sbin/nginx -s stop'   >> ${nginx_service_config}"
        ${csudo} bash -c "echo 'LimitNOFILE=infinity'               >> ${nginx_service_config}"
        ${csudo} bash -c "echo 'LimitNPROC=infinity'                >> ${nginx_service_config}"
        ${csudo} bash -c "echo 'LimitCORE=infinity'                 >> ${nginx_service_config}"
        ${csudo} bash -c "echo 'TimeoutStartSec=0'                  >> ${nginx_service_config}"
        ${csudo} bash -c "echo 'StandardOutput=null'                >> ${nginx_service_config}"
        ${csudo} bash -c "echo 'Restart=always'                     >> ${nginx_service_config}"
        ${csudo} bash -c "echo 'StartLimitBurst=3'                  >> ${nginx_service_config}"
        ${csudo} bash -c "echo 'StartLimitInterval=60s'             >> ${nginx_service_config}"
        ${csudo} bash -c "echo                                      >> ${nginx_service_config}"
        ${csudo} bash -c "echo '[Install]'                          >> ${nginx_service_config}"
        ${csudo} bash -c "echo 'WantedBy=multi-user.target'         >> ${nginx_service_config}"
        if ! ${csudo} systemctl enable nginxd &> /dev/null; then
            ${csudo} systemctl daemon-reexec
            ${csudo} systemctl enable nginxd
        fi
        ${csudo} systemctl start nginxd
	fi
}

function install_service() {
    if ((${service_mod}==0)); then
        install_service_on_systemd
    elif ((${service_mod}==1)); then
        install_service_on_sysvinit
    else
        # must manual stop DB_SERVICE_NAME
        kill_DB_SERVICE_NAME
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

    curr_version=$(${bin_dir}/DB_SERVICE_NAME -V | head -1 | cut -d ' ' -f 3)

    min_compatible_version=$(${script_dir}/bin/DB_SERVICE_NAME -V | head -1 | cut -d ' ' -f 5)

    vercomp $curr_version $min_compatible_version
    case $? in
        0) return 0;;
        1) return 0;;
        2) return 1;;
    esac
}

function update_DB_FULL_NAME() {
    # Start to update
    if [ ! -e DB_CLIENT_NAME.tar.gz ]; then
        echo "File DB_CLIENT_NAME.tar.gz does not exist"
        exit 1
    fi
    tar -zxf DB_CLIENT_NAME.tar.gz

    # Check if version compatible
    if ! is_version_compatible; then
        echo -e "${RED}Version incompatible${NC}"
        return 1
    fi

    echo -e "${GREEN}Start to update DB_FULL_NAME...${NC}"
    # Stop the service if running
    if pidof DB_SERVICE_NAME &> /dev/null; then
        if ((${service_mod}==0)); then
            ${csudo} systemctl stop DB_SERVICE_NAME || :
        elif ((${service_mod}==1)); then
            ${csudo} service DB_SERVICE_NAME stop || :
        else
            kill_DB_SERVICE_NAME
        fi
        sleep 1
    fi
    
    install_main_path

    install_log
    install_header
    install_lib
    if [ "$pagMode" != "lite" ]; then
      install_connector
    fi
    install_examples
    if [ -z $1 ]; then
        install_bin
        install_service
        install_config
		
		if [ "$verMode" == "cluster" ]; then    
            # Check if openresty is installed
            openresty_work=false

            # Check if nginx is installed successfully
            if type curl &> /dev/null; then
                if curl -sSf http://127.0.0.1:${nginx_port} &> /dev/null; then
                    echo -e "\033[44;32;1mNginx for DB_FULL_NAME is updated successfully!${NC}"
                    openresty_work=true
                else
                    echo -e "\033[44;31;5mNginx for DB_FULL_NAME does not work! Please try again!\033[0m"
                fi
            fi
		fi 

        echo
        echo -e "\033[44;32;1mDB_FULL_NAME is updated successfully!${NC}"
        echo
        echo -e "${GREEN_DARK}To configure DB_FULL_NAME ${NC}: edit /etc/DB_CLIENT_NAME/DB_CLIENT_NAME.cfg"
        if ((${service_mod}==0)); then
            echo -e "${GREEN_DARK}To start DB_FULL_NAME     ${NC}: ${csudo} systemctl start DB_SERVICE_NAME${NC}"
        elif ((${service_mod}==1)); then
            echo -e "${GREEN_DARK}To start DB_FULL_NAME     ${NC}: ${csudo} service DB_SERVICE_NAME start${NC}"
        else
            echo -e "${GREEN_DARK}To start DB_FULL_NAME     ${NC}: ./DB_SERVICE_NAME${NC}"
        fi

        if [ "$verMode" == "cluster" ]; then  
            if [ ${openresty_work} = 'true' ]; then
                echo -e "${GREEN_DARK}To access DB_FULL_NAME    ${NC}: use ${GREEN_UNDERLINE}DB_CLIENT_NAME${NC} in shell OR from ${GREEN_UNDERLINE}http://127.0.0.1:${nginx_port}${NC}"
            else
                echo -e "${GREEN_DARK}To access DB_FULL_NAME    ${NC}: use ${GREEN_UNDERLINE}DB_CLIENT_NAME${NC} in shell${NC}"
            fi
        else
		    echo -e "${GREEN_DARK}To access DB_FULL_NAME    ${NC}: use ${GREEN_UNDERLINE}DB_CLIENT_NAME${NC} in shell${NC}"
        fi
        echo
        echo -e "\033[44;32;1mDB_FULL_NAME is updated successfully!${NC}"
    else
        install_bin
        install_config

        echo
        echo -e "\033[44;32;1mDB_FULL_NAME client is updated successfully!${NC}"
    fi

    rm -rf $(tar -tf DB_CLIENT_NAME.tar.gz)
}

function install_DB_FULL_NAME() {
    # Start to install
    if [ ! -e DB_CLIENT_NAME.tar.gz ]; then
        echo "File DB_CLIENT_NAME.tar.gz does not exist"
        exit 1
    fi
    tar -zxf DB_CLIENT_NAME.tar.gz

    echo -e "${GREEN}Start to install DB_FULL_NAME...${NC}"
    
	  install_main_path
	   
    if [ -z $1 ]; then
        install_data
    fi 
    
    install_log 
    install_header
    install_lib
    if [ "$pagMode" != "lite" ]; then
      install_connector
    fi
    install_examples

    if [ -z $1 ]; then # install service and client
        # For installing new
        install_bin
        install_service

        if [ "$verMode" == "cluster" ]; then  
            openresty_work=false
            # Check if nginx is installed successfully
            if type curl &> /dev/null; then
                if curl -sSf http://127.0.0.1:${nginx_port} &> /dev/null; then
                    echo -e "\033[44;32;1mNginx for DB_FULL_NAME is installed successfully!${NC}"
                    openresty_work=true
                else
                    echo -e "\033[44;31;5mNginx for DB_FULL_NAME does not work! Please try again!\033[0m"
                fi
            fi
        fi
		
        install_config	

        # Ask if to start the service
        echo
        echo -e "\033[44;32;1mDB_FULL_NAME is installed successfully!${NC}"
        echo
        echo -e "${GREEN_DARK}To configure DB_FULL_NAME ${NC}: edit /etc/DB_CLIENT_NAME/DB_CLIENT_NAME.cfg"
        if ((${service_mod}==0)); then
            echo -e "${GREEN_DARK}To start DB_FULL_NAME     ${NC}: ${csudo} systemctl start DB_SERVICE_NAME${NC}"
        elif ((${service_mod}==1)); then
            echo -e "${GREEN_DARK}To start DB_FULL_NAME     ${NC}: ${csudo} service DB_SERVICE_NAME start${NC}"
        else
            echo -e "${GREEN_DARK}To start DB_FULL_NAME     ${NC}: DB_SERVICE_NAME${NC}"
        fi
		
        if [ "$verMode" == "cluster" ]; then  
           if [ ${openresty_work} = 'true' ]; then
                echo -e "${GREEN_DARK}To access DB_FULL_NAME    ${NC}: use ${GREEN_UNDERLINE}DB_CLIENT_NAME${NC} in shell OR from ${GREEN_UNDERLINE}http://127.0.0.1:${nginx_port}${NC}"
           else
                echo -e "${GREEN_DARK}To access DB_FULL_NAME    ${NC}: use ${GREEN_UNDERLINE}DB_CLIENT_NAME${NC} in shell${NC}"
            fi
		else
            echo -e "${GREEN_DARK}To access DB_FULL_NAME    ${NC}: use ${GREEN_UNDERLINE}DB_CLIENT_NAME${NC} in shell${NC}"
        fi
		
        echo
        echo -e "\033[44;32;1mDB_FULL_NAME is installed successfully!${NC}"
    else # Only install client
        install_bin
        install_config

        echo
        echo -e "\033[44;32;1mDB_FULL_NAME client is installed successfully!${NC}"
    fi

    rm -rf $(tar -tf DB_CLIENT_NAME.tar.gz)
}


## ==============================Main program starts from here============================
if [ -z $1 ]; then
    # Install server and client
    if [ -x ${bin_dir}/DB_SERVICE_NAME ]; then
        update_flag=1
        update_DB_FULL_NAME
    else
        install_DB_FULL_NAME
    fi
else
    # Only install client
    if [ -x ${bin_dir}/DB_CLIENT_NAME ]; then
        update_flag=1
        update_DB_FULL_NAME client
    else
        install_DB_FULL_NAME client
    fi
fi
