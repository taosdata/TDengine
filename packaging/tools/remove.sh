#!/bin/bash
#
# Script to stop the service and uninstall database, but retain the config, data and log files.

set -e
#set -x

verMode=edge

RED='\033[0;31m'
GREEN='\033[1;32m'
NC='\033[0m'

#install main path
install_main_dir="/usr/local/DB_CLIENT_NAME"
data_link_dir="/usr/local/DB_CLIENT_NAME/data"
log_link_dir="/usr/local/DB_CLIENT_NAME/log"
cfg_link_dir="/usr/local/DB_CLIENT_NAME/cfg"
bin_link_dir="/usr/bin"
lib_link_dir="/usr/lib"
inc_link_dir="/usr/include"
install_nginxd_dir="/usr/local/nginxd"

# v1.5 jar dir
v15_java_app_dir="/usr/local/lib/taos"

service_config_dir="/etc/systemd/system"
DB_CLIENT_NAME_service_name="DB_SERVICE_NAME"
nginx_service_name="nginxd"
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

function kill_DB_SERVICE_NAME() {
  pid=$(ps -ef | grep "DB_SERVICE_NAME" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo} kill -9 $pid   || :
  fi
}

function clean_bin() {
    # Remove link
    ${csudo} rm -f ${bin_link_dir}/DB_CLIENT_NAME      || :
    ${csudo} rm -f ${bin_link_dir}/DB_SERVICE_NAME     || :
    ${csudo} rm -f ${bin_link_dir}/DB_CLIENT_NAMEdemo  || :
    ${csudo} rm -f ${bin_link_dir}/DB_CLIENT_NAMEdump  || :
    ${csudo} rm -f ${bin_link_dir}/rmDB_CLIENT_NAME    || :
}

function clean_lib() {
    # Remove link
    ${csudo} rm -f ${lib_link_dir}/libtaos.*      || :
    ${csudo} rm -rf ${v15_java_app_dir}                      || :
}

function clean_header() {
    # Remove link
    ${csudo} rm -f ${inc_link_dir}/taos.h       || :
    ${csudo} rm -f ${inc_link_dir}/taoserror.h       || :
}

function clean_config() {
    # Remove link
    ${csudo} rm -f ${cfg_link_dir}/*            || :    
}

function clean_log() {
    # Remove link
    ${csudo} rm -rf ${log_link_dir}    || :
}

function clean_service_on_systemd() {
    DB_SERVICE_NAME_service_config="${service_config_dir}/${DB_CLIENT_NAME_service_name}.service"

    if systemctl is-active --quiet ${DB_CLIENT_NAME_service_name}; then
        echo "DB_FULL_NAME DB_SERVICE_NAME is running, stopping it..."
        ${csudo} systemctl stop ${DB_CLIENT_NAME_service_name} &> /dev/null || echo &> /dev/null
    fi
    ${csudo} systemctl disable ${DB_CLIENT_NAME_service_name} &> /dev/null || echo &> /dev/null

    ${csudo} rm -f ${DB_SERVICE_NAME_service_config}

    if [ "$verMode" == "cluster" ]; then
		nginx_service_config="${service_config_dir}/${nginx_service_name}.service"
	
   	 	if [ -d ${bin_dir}/web ]; then
   	        if systemctl is-active --quiet ${nginx_service_name}; then
   	            echo "Nginx for DB_FULL_NAME is running, stopping it..."
   	            ${csudo} systemctl stop ${nginx_service_name} &> /dev/null || echo &> /dev/null
   	        fi
   	        ${csudo} systemctl disable ${nginx_service_name} &> /dev/null || echo &> /dev/null
        
   	        ${csudo} rm -f ${nginx_service_config}
   	    fi
    fi 
}

function clean_service_on_sysvinit() {
    #restart_config_str="DB_CLIENT_NAME:2345:respawn:${service_config_dir}/DB_SERVICE_NAME start"
    #${csudo} sed -i "\|${restart_config_str}|d" /etc/inittab || :    
    
    if pidof DB_SERVICE_NAME &> /dev/null; then
        echo "DB_FULL_NAME DB_SERVICE_NAME is running, stopping it..."
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

function clean_service() {
    if ((${service_mod}==0)); then
        clean_service_on_systemd
    elif ((${service_mod}==1)); then
        clean_service_on_sysvinit
    else
        # must manual stop DB_SERVICE_NAME
        kill_DB_SERVICE_NAME
    fi
}

# Stop service and disable booting start.
clean_service
# Remove binary file and links
clean_bin
# Remove header file.
clean_header
# Remove lib file
clean_lib
# Remove link log directory
clean_log
# Remove link configuration file
clean_config
# Remove data link directory
${csudo} rm -rf ${data_link_dir}    || : 

${csudo} rm -rf ${install_main_dir}
${csudo} rm -rf ${install_nginxd_dir}

osinfo=$(awk -F= '/^NAME/{print $2}' /etc/os-release)
if echo $osinfo | grep -qwi "ubuntu" ; then
#  echo "this is ubuntu system"
   ${csudo} rm -f /var/lib/dpkg/info/tdengine* || :
elif  echo $osinfo | grep -qwi "centos" ; then
  echo "this is centos system"
  ${csudo} rpm -e --noscripts tdengine || :
fi

echo -e "${GREEN}DB_FULL_NAME is removed successfully!${NC}"
