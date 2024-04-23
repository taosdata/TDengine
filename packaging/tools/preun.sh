#!/bin/bash
#
# Script to stop the service and uninstall TSDB

RED='\033[0;31m'
GREEN='\033[1;32m'
NC='\033[0m'

bin_link_dir="/usr/bin"
lib_link_dir="/usr/lib"
lib64_link_dir="/usr/lib64"
inc_link_dir="/usr/include"

data_link_dir="/usr/local/taos/data"
log_link_dir="/usr/local/taos/log"
cfg_link_dir="/usr/local/taos/cfg"

service_config_dir="/etc/systemd/system"
taos_service_name="taosd"
taoskeeper_service_name="taoskeeper"
csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo "
fi

initd_mod=0
service_mod=2
if ps aux | grep -v grep | grep systemd &> /dev/null; then
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

function kill_taosadapter() {
  pid=$(ps -ef | grep "taosadapter" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo}kill -9 $pid   || :
  fi
}

function kill_taosd() {
  pid=$(ps -ef | grep "taosd" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo}kill -9 $pid   || :
  fi
}

function kill_taoskeeper() {
  pid=$(ps -ef | grep "taoskeeper" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo}kill -9 $pid   || :
  fi
}

function clean_service_on_systemd() {
    taosadapter_service_config="${service_config_dir}/taosadapter.service"
    if systemctl is-active --quiet taosadapter; then
        echo "taosadapter is running, stopping it..."
        ${csudo}systemctl stop taosadapter &> /dev/null || echo &> /dev/null
    fi

    taosd_service_config="${service_config_dir}/${taos_service_name}.service"

    if systemctl is-active --quiet ${taos_service_name}; then
        echo "TDengine taosd is running, stopping it..."
        ${csudo}systemctl stop ${taos_service_name} &> /dev/null || echo &> /dev/null
    fi
    ${csudo}systemctl disable ${taos_service_name} &> /dev/null || echo &> /dev/null

    ${csudo}rm -f ${taosd_service_config}

    [ -f ${taosadapter_service_config} ] && ${csudo}rm -f ${taosadapter_service_config}

    taoskeeper_service_config="${service_config_dir}/${taoskeeper_service_name}.service"
    if systemctl is-active --quiet ${taoskeeper_service_name}; then
        echo "TDengine taoskeeper is running, stopping it..."
        ${csudo}systemctl stop ${taoskeeper_service_name} &> /dev/null || echo &> /dev/null
    fi
    [ -f ${taoskeeper_service_config} ] && ${csudo}rm -f ${taoskeeper_service_config}
}

function clean_service_on_sysvinit() {
    #restart_config_str="taos:2345:respawn:${service_config_dir}/taosd start"
    #${csudo}sed -i "\|${restart_config_str}|d" /etc/inittab || :

    if ps aux | grep -v grep | grep taosd &> /dev/null; then
        echo "TDengine taosd is running, stopping it..."
        ${csudo}service taosd stop || :
    fi

    if ((${initd_mod}==1)); then
        ${csudo}chkconfig --del taosd || :
    elif ((${initd_mod}==2)); then
        ${csudo}insserv -r taosd || :
    elif ((${initd_mod}==3)); then
        ${csudo}update-rc.d -f taosd remove || :
    fi

    ${csudo}rm -f ${service_config_dir}/taosd || :

    if $(which init &> /dev/null); then
        ${csudo}init q || :
    fi
}

function clean_service() {
    if ((${service_mod}==0)); then
        clean_service_on_systemd
    elif ((${service_mod}==1)); then
        clean_service_on_sysvinit
    else
        # must manual stop taosd
        kill_taosadapter
        kill_taosd
        kill_taoskeeper
    fi
}

# Stop service and disable booting start.
clean_service

# Remove all links
${csudo}rm -f ${bin_link_dir}/taos       || :
${csudo}rm -f ${bin_link_dir}/taosd      || :
${csudo}rm -f ${bin_link_dir}/taosadapter       || :
${csudo}rm -f ${bin_link_dir}/taosBenchmark || :
${csudo}rm -f ${bin_link_dir}/taosdemo   || :
${csudo}rm -f ${bin_link_dir}/set_core   || :
${csudo}rm -f ${bin_link_dir}/taoskeeper  || :
${csudo}rm -f ${cfg_link_dir}/*.new      || :
${csudo}rm -f ${inc_link_dir}/taos.h     || :
${csudo}rm -f ${inc_link_dir}/taosdef.h  || :
${csudo}rm -f ${inc_link_dir}/taoserror.h || :
${csudo}rm -f ${inc_link_dir}/tdef.h || :
${csudo}rm -f ${inc_link_dir}/taosudf.h || :
${csudo}rm -f ${lib_link_dir}/libtaos.*   || :
${csudo}rm -f ${lib64_link_dir}/libtaos.* || :

${csudo}rm -f ${log_link_dir}            || :
${csudo}rm -f ${data_link_dir}           || :

if ((${service_mod}==2)); then
    kill_taosadapter
    kill_taosd
fi

echo -e "${GREEN}TDengine is removed successfully!${NC}"
