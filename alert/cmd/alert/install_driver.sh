#!/bin/bash
#
# This file is used to install TDengine client library on linux systems.

set -e
#set -x

# -----------------------Variables definition---------------------
script_dir=$(dirname $(readlink -f "$0"))
# Dynamic directory
lib_link_dir="/usr/lib"

#install main path
install_main_dir="/usr/local/taos"

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo"
fi

function clean_driver() {
    ${csudo} rm -f /usr/lib/libtaos.so || :
}

function install_driver() {
    echo -e "${GREEN}Start to install TDengine client driver ...${NC}"
    
    #create install main dir and all sub dir
    ${csudo} mkdir -p ${install_main_dir}  
    ${csudo} mkdir -p ${install_main_dir}/driver

    ${csudo} rm -f ${lib_link_dir}/libtaos.*         || :
    ${csudo} cp -rf ${script_dir}/driver/* ${install_main_dir}/driver && ${csudo} chmod 777 ${install_main_dir}/driver/*  
    ${csudo} ln -s ${install_main_dir}/driver/libtaos.* ${lib_link_dir}/libtaos.so.1
    ${csudo} ln -s ${lib_link_dir}/libtaos.so.1 ${lib_link_dir}/libtaos.so
 
    echo
    echo -e "\033[44;32;1mTDengine client driver is successfully installed!${NC}"
}

install_driver
