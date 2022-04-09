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
install_main_dir="/usr/local/taos"
data_link_dir="/usr/local/taos/data"
log_link_dir="/usr/local/taos/log"
cfg_link_dir="/usr/local/taos/cfg"
bin_link_dir="/usr/bin"
lib_link_dir="/usr/lib"
lib64_link_dir="/usr/lib64"
inc_link_dir="/usr/include"

service_config_dir="/etc/systemd/system"
taos_service_name="taosd"

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


function kill_taosd() {
  pid=$(ps -ef | grep "taosd" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo} kill -9 $pid   || :
  fi
}


function clean_bin() {
    # Remove link
    ${csudo} rm -f ${bin_link_dir}/taos        || :
    ${csudo} rm -f ${bin_link_dir}/taosd       || :
    ${csudo} rm -f ${bin_link_dir}/create_table  || :
    ${csudo} rm -f ${bin_link_dir}/tmq_sim || :
    ${csudo} rm -f ${bin_link_dir}/taosdemo    || :
    ${csudo} rm -f ${bin_link_dir}/taosdump    || :
    ${csudo} rm -f ${bin_link_dir}/rmtaos      || :
    #${csudo} rm -f ${bin_link_dir}/set_core    || :
}

function clean_lib() {
    # Remove link
    ${csudo} rm -f ${lib_link_dir}/libtaos.*      || :
    ${csudo} rm -f ${lib64_link_dir}/libtaos.*    || :    
    
    ${csudo} rm -f ${lib_link_dir}/libtdb.*      || :
    ${csudo} rm -f ${lib64_link_dir}/libtdb.*    || :
}

function clean_header() {
    # Remove link
    ${csudo} rm -f ${inc_link_dir}/taos.h       || :
    ${csudo} rm -f ${inc_link_dir}/taosdef.h    || :
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
    taosd_service_config="${service_config_dir}/${taos_service_name}.service"
    if systemctl is-active --quiet ${taos_service_name}; then
        echo "TDengine taosd is running, stopping it..."
        ${csudo} systemctl stop ${taos_service_name} &> /dev/null || echo &> /dev/null
    fi
    ${csudo} systemctl disable ${taos_service_name} &> /dev/null || echo &> /dev/null
    ${csudo} rm -f ${taosd_service_config}
}

function clean_service_on_sysvinit() {
    echo " "
}

function clean_service() {
    if ((${service_mod}==0)); then
        clean_service_on_systemd
    elif ((${service_mod}==1)); then
        clean_service_on_sysvinit
    else
        # must manual stop taosd
        kill_taosd
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

if [[ -e /etc/os-release ]]; then
  osinfo=$(awk -F= '/^NAME/{print $2}' /etc/os-release)
else
  osinfo=""
fi

echo -e "${GREEN}TDengine is removed successfully!${NC}"
echo
