#!/bin/bash
#
# Script to stop the service and uninstall TDengine, but retain the config, data and log files.

set -e
#set -x

verMode=edge

RED='\033[0;31m'
GREEN='\033[1;32m'
NC='\033[0m'

#install main path
install_main_dir="/usr/local/power"
data_link_dir="/usr/local/power/data"
log_link_dir="/usr/local/power/log"
cfg_link_dir="/usr/local/power/cfg"
bin_link_dir="/usr/bin"
lib_link_dir="/usr/lib"
lib64_link_dir="/usr/lib64"
inc_link_dir="/usr/include"
install_nginxd_dir="/usr/local/nginxd"

# v1.5 jar dir
#v15_java_app_dir="/usr/local/lib/power"

service_config_dir="/etc/systemd/system"
power_service_name="powerd"
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

function kill_powerd() {
  pid=$(ps -ef | grep "powerd" | grep -v "grep" | awk '{print $2}')
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
    ${csudo} rm -f ${bin_link_dir}/power        || :
    ${csudo} rm -f ${bin_link_dir}/powerd       || :
    ${csudo} rm -f ${bin_link_dir}/powerdemo    || :
    ${csudo} rm -f ${bin_link_dir}/powerdemox   || :
    ${csudo} rm -f ${bin_link_dir}/powerdump    || :
    ${csudo} rm -f ${bin_link_dir}/rmpower      || :
    ${csudo} rm -f ${bin_link_dir}/tarbitrator  || :
    ${csudo} rm -f ${bin_link_dir}/set_core     || :
}

function clean_lib() {
    # Remove link
    ${csudo} rm -f ${lib_link_dir}/libtaos.*      || :
    ${csudo} rm -f ${lib64_link_dir}/libtaos.*    || :
    #${csudo} rm -rf ${v15_java_app_dir}           || :
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
    power_service_config="${service_config_dir}/${power_service_name}.service"
    if systemctl is-active --quiet ${power_service_name}; then
        echo "PowerDB powerd is running, stopping it..."
        ${csudo} systemctl stop ${power_service_name} &> /dev/null || echo &> /dev/null
    fi
    ${csudo} systemctl disable ${power_service_name} &> /dev/null || echo &> /dev/null
    ${csudo} rm -f ${power_service_config}
    
    tarbitratord_service_config="${service_config_dir}/${tarbitrator_service_name}.service"
    if systemctl is-active --quiet ${tarbitrator_service_name}; then
        echo "TDengine tarbitrator is running, stopping it..."
        ${csudo} systemctl stop ${tarbitrator_service_name} &> /dev/null || echo &> /dev/null
    fi
    ${csudo} systemctl disable ${tarbitrator_service_name} &> /dev/null || echo &> /dev/null
    ${csudo} rm -f ${tarbitratord_service_config}
    
    if [ "$verMode" == "cluster" ]; then
		  nginx_service_config="${service_config_dir}/${nginx_service_name}.service"	
   	 	if [ -d ${bin_dir}/web ]; then
   	    if systemctl is-active --quiet ${nginx_service_name}; then
   	        echo "Nginx for TDengine is running, stopping it..."
   	        ${csudo} systemctl stop ${nginx_service_name} &> /dev/null || echo &> /dev/null
   	    fi
   	    ${csudo} systemctl disable ${nginx_service_name} &> /dev/null || echo &> /dev/null
      
   	    ${csudo} rm -f ${nginx_service_config}
   	  fi
    fi 
}

function clean_service_on_sysvinit() {
    #restart_config_str="power:2345:respawn:${service_config_dir}/powerd start"
    #${csudo} sed -i "\|${restart_config_str}|d" /etc/inittab || :    
    
    if pidof powerd &> /dev/null; then
        echo "PowerDB powerd is running, stopping it..."
        ${csudo} service powerd stop || :
    fi
    
    if pidof tarbitrator &> /dev/null; then
        echo "PowerDB tarbitrator is running, stopping it..."
        ${csudo} service tarbitratord stop || :
    fi
    
    if ((${initd_mod}==1)); then    
      if [ -e ${service_config_dir}/powerd ]; then 
        ${csudo} chkconfig --del powerd || :
      fi
      if [ -e ${service_config_dir}/tarbitratord ]; then 
        ${csudo} chkconfig --del tarbitratord || :
      fi
    elif ((${initd_mod}==2)); then   
      if [ -e ${service_config_dir}/powerd ]; then 
        ${csudo} insserv -r powerd || :
      fi
      if [ -e ${service_config_dir}/tarbitratord ]; then 
        ${csudo} insserv -r tarbitratord || :
      fi
    elif ((${initd_mod}==3)); then  
      if [ -e ${service_config_dir}/powerd ]; then 
        ${csudo} update-rc.d -f powerd remove || :
      fi
      if [ -e ${service_config_dir}/tarbitratord ]; then 
        ${csudo} update-rc.d -f tarbitratord remove || :
      fi
    fi
    
    ${csudo} rm -f ${service_config_dir}/powerd || :
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
        # must manual stop taosd
        kill_powerd
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

#if echo $osinfo | grep -qwi "ubuntu" ; then
##  echo "this is ubuntu system"
#   ${csudo} rm -f /var/lib/dpkg/info/tdengine* || :
#elif echo $osinfo | grep -qwi "debian" ; then
##  echo "this is debian system"
#   ${csudo} rm -f /var/lib/dpkg/info/tdengine* || :
#elif  echo $osinfo | grep -qwi "centos" ; then
##  echo "this is centos system"
#  ${csudo} rpm -e --noscripts tdengine || :
#fi

echo -e "${GREEN}PowerDB is removed successfully!${NC}"
echo 
