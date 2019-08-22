#!/bin/bash
#
# Script to stop the service and uninstall tdengine, but retain the config, data and log files.

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
inc_link_dir="/usr/include"

service_config_dir="/etc/systemd/system"
taos_service_name="taosd"

csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo"
fi

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

function clean_bin() {
    # Remove link
    ${csudo} rm -f ${bin_link_dir}/taos      || :
    ${csudo} rm -f ${bin_link_dir}/taosd     || :
    ${csudo} rm -f ${bin_link_dir}/taosdemo  || :
    ${csudo} rm -f ${bin_link_dir}/taosdump  || :
    ${csudo} rm -f ${bin_link_dir}/rmtaos    || :
}

function clean_lib() {
    # Remove link
    ${csudo} rm -f ${lib_link_dir}/libtaos.*      || :
}

function clean_header() {
    # Remove link
    ${csudo} rm -f ${inc_link_dir}/taos.h       || :
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
    restart_config_str="taos:2345:respawn:${service_config_dir}/taosd start"

    if pidof taosd &> /dev/null; then
        echo "TDengine taosd is running, stopping it..."
        ${csudo} service taosd stop || :
    fi

    ${csudo} sed -i "\|${restart_config_str}|d" /etc/inittab || :
    ${csudo} rm -f ${service_config_dir}/taosd || :
    ${csudo} update-rc.d -f taosd remove || :
    ${csudo} init q || :
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

osinfo=$(awk -F= '/^NAME/{print $2}' /etc/os-release)
if echo $osinfo | grep -qwi "ubuntu" ; then
#  echo "this is ubuntu system"
   ${csudo} rm -f /var/lib/dpkg/info/tdengine* || :
elif  echo $osinfo | grep -qwi "centos" ; then
  echo "this is centos system"
  ${csudo} rpm -e --noscripts tdengine || :
fi

echo -e "${GREEN}TDEngine is removed successfully!${NC}"
