#!/bin/bash
#
# Script to stop the service and uninstall TSDB

RED='\033[0;31m'
GREEN='\033[1;32m'
NC='\033[0m'

bin_link_dir="/usr/bin"
lib_link_dir="/usr/lib"
inc_link_dir="/usr/include"

data_link_dir="/usr/local/taos/data"
log_link_dir="/usr/local/taos/log"
cfg_link_dir="/usr/local/taos/cfg"

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
elif $(which insserv &> /dev/null); then
    service_mod=1
    initd_mod=1
    service_config_dir="/etc/init.d"
elif $(which update-rc.d &> /dev/null); then
    service_mod=1
    initd_mod=2
    service_config_dir="/etc/init.d"
else 
    service_mod=2
fi


function kill_taosd() {
  pid=$(ps -ef | grep "taosd" | grep -v "grep" | awk '{print $2}')
  ${csudo} kill -9 ${pid}   || :
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
    restart_config_str="taos:2345:respawn:${service_config_dir}/taosd start"
    if pidof taosd &> /dev/null; then
        ${csudo} service taosd stop || :
    fi
    ${csudo} sed -i "\|${restart_config_str}|d" /etc/inittab || :
    ${csudo} rm -f ${service_config_dir}/taosd || :

    if ((${initd_mod}==1)); then
        ${csudo} grep -q -F "taos" /etc/inittab && ${csudo} insserv -r taosd || :
    elif ((${initd_mod}==2)); then
        ${csudo} grep -q -F "taos" /etc/inittab && ${csudo} update-rc.d -f taosd remove || :
    fi
#    ${csudo} update-rc.d -f taosd remove || :
    ${csudo} init q || :
}

function clean_service() {
    if ((${service_mod}==0)); then
        clean_service_on_systemd
    elif ((${service_mod}==1)); then
        clean_service_on_sysvinit
    else
        # must manual start taosd
        kill_taosd
    fi
}

# Stop service and disable booting start.
clean_service

# Remove all links
${csudo} rm -f ${bin_link_dir}/taos       || :
${csudo} rm -f ${bin_link_dir}/taosd      || :
${csudo} rm -f ${bin_link_dir}/taosdemo   || :
${csudo} rm -f ${bin_link_dir}/taosdump   || :
${csudo} rm -f ${cfg_link_dir}/*          || :
${csudo} rm -f ${inc_link_dir}/taos.h     || :
${csudo} rm -f ${lib_link_dir}/libtaos.*  || :

${csudo} rm -f ${log_link_dir}            || :
${csudo} rm -f ${data_link_dir}           || :

if ((${service_mod}==2)); then
    kill_taosd
fi

echo -e "${GREEN}TDEngine is removed successfully!${NC}"
