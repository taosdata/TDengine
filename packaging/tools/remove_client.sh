#!/bin/bash
#
# Script to stop the client and uninstall database, but retain the config and log files.
set -e
# set -x

RED='\033[0;31m'
GREEN='\033[1;32m'
NC='\033[0m'

installDir="/usr/local/taos"
clientName="taos"
uninstallScript="rmtaos"

#install main path
install_main_dir=${installDir}

log_link_dir=${installDir}/log
cfg_link_dir=${installDir}/cfg
bin_link_dir="/usr/bin"
lib_link_dir="/usr/lib"
lib64_link_dir="/usr/lib64"
inc_link_dir="/usr/include"

csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo "
fi

function kill_client() {
  if [ -n "$(pidof ${clientName})" ]; then
    ${csudo}kill -9 $pid   || :
  fi
}

function clean_bin() {
    # Remove link
    ${csudo}rm -f ${bin_link_dir}/${clientName}      || :
    ${csudo}rm -f ${bin_link_dir}/taosdemo  || :
    ${csudo}rm -f ${bin_link_dir}/taosdump  || :
    ${csudo}rm -f ${bin_link_dir}/${uninstallScript}    || :
    ${csudo}rm -f ${bin_link_dir}/set_core  || :
}

function clean_lib() {
    # Remove link
    ${csudo}rm -f ${lib_link_dir}/libtaos.*      || :
    ${csudo}rm -f ${lib64_link_dir}/libtaos.*    || :
    #${csudo}rm -rf ${v15_java_app_dir}           || :

    [ -f ${lib_link_dir}/libtaosws.so ] && ${csudo}rm -f ${lib_link_dir}/libtaosws.so      || :
    [ -f ${lib64_link_dir}/libtaosws.so ] && ${csudo}rm -f ${lib64_link_dir}/libtaosws.*    || :
}

function clean_header() {
    # Remove link
    ${csudo}rm -f ${inc_link_dir}/taos.h           || :
    ${csudo}rm -f ${inc_link_dir}/taosdef.h        || :
    ${csudo}rm -f ${inc_link_dir}/taoserror.h      || :
    [ -f ${inc_link_dir}/taosws.h ] && ${csudo}rm -f ${inc_link_dir}/taosws.h         || :
}

function clean_config() {
    # Remove link
    ${csudo}rm -f ${cfg_link_dir}/*            || :
}

function clean_log() {
    # Remove link
    ${csudo}rm -rf ${log_link_dir}    || :
}

# Stop client.
kill_client
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

${csudo}rm -rf ${install_main_dir}

echo -e "${GREEN}TDengine client is removed successfully!${NC}"
echo 
