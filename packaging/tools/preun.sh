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

function clean_service_on_systemd() {
    taosd_service_config="${service_config_dir}/${taos_service_name}.service"

    if systemctl is-active --quiet ${taos_service_name}; then
        echo "TDengine taosd is running, stopping it..."
        sudo systemctl stop ${taos_service_name} &> /dev/null || echo &> /dev/null
    fi
    sudo systemctl disable ${taos_service_name} &> /dev/null || echo &> /dev/null

    sudo rm -f ${taosd_service_config}	
}

function clean_service_on_sysvinit() {
    restart_config_str="taos:2345:respawn:${service_config_dir}/taosd start"

    if pidof taosd &> /dev/null; then
        echo "TDengine taosd is running, stopping it..."
        sudo service taosd stop || :
    fi

    sudo sed -i "\|${restart_config_str}|d" /etc/inittab || :
    sudo rm -f ${service_config_dir}/taosd || :
    sudo update-rc.d -f taosd remove || :
    sudo init q || :
}

function clean_service() {
    if is_using_systemd; then
        clean_service_on_systemd
    else
        clean_service_on_sysvinit
    fi
}

# Stop service and disable booting start.
clean_service

# Remove all links
sudo rm -f ${bin_link_dir}/taos       || :
sudo rm -f ${bin_link_dir}/taosd      || :
sudo rm -f ${bin_link_dir}/taosdump   || :
sudo rm -f ${cfg_link_dir}/*          || :
sudo rm -f ${inc_link_dir}/taos.h     || :
sudo rm -f ${lib_link_dir}/libtaos.*  || :

sudo rm -f ${log_link_dir}            || :
sudo rm -f ${data_link_dir}           || :

echo -e "${GREEN}TDEngine is removed successfully!${NC}"
