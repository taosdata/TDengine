#!/bin/bash
#
# Script to stop the service and uninstall ProDB's arbitrator

set -e
#set -x

verMode=edge

RED='\033[0;31m'
GREEN='\033[1;32m'
NC='\033[0m'

#install main path
install_main_dir="/usr/local/tarbitrator"
bin_link_dir="/usr/bin"

service_config_dir="/etc/systemd/system"
tarbitrator_service_name="tarbitratord"
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

function kill_tarbitrator() {
  pid=$(ps -ef | grep "tarbitrator" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo} kill -9 $pid   || :
  fi
}

function clean_bin() {
    # Remove link
    ${csudo} rm -f ${bin_link_dir}/tarbitrator      || :
}

function clean_header() {
    # Remove link
    ${csudo} rm -f ${inc_link_dir}/taos.h       || :
    ${csudo} rm -f ${inc_link_dir}/taoserror.h  || :
}

function clean_log() {
    # Remove link
    ${csudo} rm -rf /arbitrator.log    || :
}

function clean_service_on_systemd() {
  tarbitratord_service_config="${service_config_dir}/${tarbitrator_service_name}.service"

  if systemctl is-active --quiet ${tarbitrator_service_name}; then
      echo "ProDB tarbitrator is running, stopping it..."
      ${csudo} systemctl stop ${tarbitrator_service_name} &> /dev/null || echo &> /dev/null
  fi
  ${csudo} systemctl disable ${tarbitrator_service_name} &> /dev/null || echo &> /dev/null

  ${csudo} rm -f ${tarbitratord_service_config}
}

function clean_service_on_sysvinit() {
    if pidof tarbitrator &> /dev/null; then
        echo "ProDB's tarbitrator is running, stopping it..."
        ${csudo} service tarbitratord stop || :
    fi
    
    if ((${initd_mod}==1)); then    
      if [ -e ${service_config_dir}/tarbitratord ]; then 
        ${csudo} chkconfig --del tarbitratord || :
      fi
    elif ((${initd_mod}==2)); then   
      if [ -e ${service_config_dir}/tarbitratord ]; then 
        ${csudo} insserv -r tarbitratord || :
      fi
    elif ((${initd_mod}==3)); then  
      if [ -e ${service_config_dir}/tarbitratord ]; then 
        ${csudo} update-rc.d -f tarbitratord remove || :
      fi
    fi

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
        # must manual stop  
        kill_tarbitrator
    fi
}

# Stop service and disable booting start.
clean_service
# Remove binary file and links
clean_bin
# Remove header file.
##clean_header
# Remove log file
clean_log

${csudo} rm -rf ${install_main_dir}

echo -e "${GREEN}ProDB's arbitrator is removed successfully!${NC}"
echo 
