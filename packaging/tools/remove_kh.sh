#!/bin/bash
#
# Script to stop the service and uninstall kinghistorian, but retain the config, data and log files.

set -e
#set -x

verMode=edge

RED='\033[0;31m'
GREEN='\033[1;32m'
NC='\033[0m'

#install main path
install_main_dir="/usr/local/kinghistorian"
data_link_dir="/usr/local/kinghistorian/data"
log_link_dir="/usr/local/kinghistorian/log"
cfg_link_dir="/usr/local/kinghistorian/cfg"
bin_link_dir="/usr/bin"
lib_link_dir="/usr/lib"
lib64_link_dir="/usr/lib64"
inc_link_dir="/usr/include"
install_nginxd_dir="/usr/local/nginxd"

service_config_dir="/etc/systemd/system"
service_name="khserver"
tarbitrator_service_name="tarbitratord"
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

function kill_process() {
  pid=$(ps -ef | grep "khserver" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo} kill -9 $pid   || :
  fi
}

function kill_tarbitrator() {
  pid=$(ps -ef | grep "tarbitrator" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo} kill -9 $pid   || :
  fi
}

function clean_bin() {
    # Remove link
    ${csudo} rm -f ${bin_link_dir}/khclient      || :
    ${csudo} rm -f ${bin_link_dir}/khserver      || :
    ${csudo} rm -f ${bin_link_dir}/khdemo        || :
    ${csudo} rm -f ${bin_link_dir}/khdump        || :
    ${csudo} rm -f ${bin_link_dir}/rmkh          || :
    ${csudo} rm -f ${bin_link_dir}/tarbitrator   || :
    ${csudo} rm -f ${bin_link_dir}/set_core      || :
    ${csudo} rm -f ${bin_link_dir}/run_taosd.sh     || :
}

function clean_lib() {
    # Remove link
    ${csudo} rm -f ${lib_link_dir}/libtaos.*      || :
    ${csudo} rm -f ${lib64_link_dir}/libtaos.*    || :
}

function clean_header() {
    # Remove link
    ${csudo} rm -f ${inc_link_dir}/taos.h       || :
    ${csudo} rm -f ${inc_link_dir}/taoserror.h  || :
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
    service_config="${service_config_dir}/${service_name}.service"
    if systemctl is-active --quiet ${service_name}; then
        echo "KingHistorian's khserver is running, stopping it..."
        ${csudo} systemctl stop ${service_name} &> /dev/null || echo &> /dev/null
    fi
    ${csudo} systemctl disable ${service_name} &> /dev/null || echo &> /dev/null
    ${csudo} rm -f ${service_config}
    
    tarbitratord_service_config="${service_config_dir}/${tarbitrator_service_name}.service"
    if systemctl is-active --quiet ${tarbitrator_service_name}; then
        echo "KingHistorian's tarbitrator is running, stopping it..."
        ${csudo} systemctl stop ${tarbitrator_service_name} &> /dev/null || echo &> /dev/null
    fi
    ${csudo} systemctl disable ${tarbitrator_service_name} &> /dev/null || echo &> /dev/null
    ${csudo} rm -f ${tarbitratord_service_config}
    
    if [ "$verMode" == "cluster" ]; then
		  nginx_service_config="${service_config_dir}/${nginx_service_name}.service"	
   	 	if [ -d ${bin_dir}/web ]; then
   	    if systemctl is-active --quiet ${nginx_service_name}; then
   	        echo "Nginx for KingHistorian is running, stopping it..."
   	        ${csudo} systemctl stop ${nginx_service_name} &> /dev/null || echo &> /dev/null
   	    fi
   	    ${csudo} systemctl disable ${nginx_service_name} &> /dev/null || echo &> /dev/null
      
   	    ${csudo} rm -f ${nginx_service_config}
   	  fi
    fi 
}

function clean_service_on_sysvinit() {
    if pidof khserver &> /dev/null; then
        echo "KingHistorian's khserver is running, stopping it..."
        ${csudo} service khserver stop || :
    fi
    
    if pidof tarbitrator &> /dev/null; then
        echo "KingHistorian's tarbitrator is running, stopping it..."
        ${csudo} service tarbitratord stop || :
    fi
    
    if ((${initd_mod}==1)); then    
      if [ -e ${service_config_dir}/khserver ]; then
        ${csudo} chkconfig --del khserver || :
      fi
      if [ -e ${service_config_dir}/tarbitratord ]; then 
        ${csudo} chkconfig --del tarbitratord || :
      fi
    elif ((${initd_mod}==2)); then   
      if [ -e ${service_config_dir}/khserver ]; then
        ${csudo} insserv -r khserver || :
      fi
      if [ -e ${service_config_dir}/tarbitratord ]; then 
        ${csudo} insserv -r tarbitratord || :
      fi
    elif ((${initd_mod}==3)); then  
      if [ -e ${service_config_dir}/khserver ]; then
        ${csudo} update-rc.d -f khserver remove || :
      fi
      if [ -e ${service_config_dir}/tarbitratord ]; then 
        ${csudo} update-rc.d -f tarbitratord remove || :
      fi
    fi
    
    ${csudo} rm -f ${service_config_dir}/khserver || :
    ${csudo} rm -f ${service_config_dir}/tarbitratord || :
   
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
        kill_process
        kill_tarbitrator
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
if [[ -e /etc/os-release ]]; then
  osinfo=$(awk -F= '/^NAME/{print $2}' /etc/os-release)
else
  osinfo=""
fi

echo -e "${GREEN}KingHistorian is removed successfully!${NC}"
echo 
