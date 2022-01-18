#!/bin/bash
#
# Script to stop the client and uninstall database, but retain the config and log files.
set -e
# set -x

RED='\033[0;31m'
GREEN='\033[1;32m'
NC='\033[0m'

#install main path
install_main_dir="/usr/local/kinghistorian"

log_link_dir="/usr/local/kinghistorian/log"
cfg_link_dir="/usr/local/kinghistorian/cfg"
bin_link_dir="/usr/bin"
lib_link_dir="/usr/lib"
lib64_link_dir="/usr/lib64"
inc_link_dir="/usr/include"

csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo"
fi

function kill_client() {
  if [ -n "$(pidof khclient)" ]; then
    ${csudo} kill -9 $pid   || :
  fi
}

function clean_bin() {
    # Remove link
    ${csudo} rm -f ${bin_link_dir}/khclient  || :
    ${csudo} rm -f ${bin_link_dir}/khdemo    || :
    ${csudo} rm -f ${bin_link_dir}/khdump    || :
    ${csudo} rm -f ${bin_link_dir}/rmkh      || :
    ${csudo} rm -f ${bin_link_dir}/set_core  || :
}

function clean_lib() {
    # Remove link
    ${csudo} rm -f ${lib_link_dir}/libtaos.*      || :
    ${csudo} rm -f ${lib64_link_dir}/libtaos.*    || :
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

${csudo} rm -rf ${install_main_dir}

echo -e "${GREEN}KingHistorian client is removed successfully!${NC}"
echo
