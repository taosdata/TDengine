#!/bin/bash
#
# This file is used to install TDengine client library on linux systems.

set -e
#set -x

# -----------------------Variables definition---------------------
script_dir=$(dirname $(readlink -f "$0"))
# Dynamic directory
lib_link_dir="/usr/lib"
lib64_link_dir="/usr/lib64"

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

function install_driver() {
    echo
    if [[ -d ${lib_link_dir} && ! -e ${lib_link_dir}/libtaos.so ]]; then
      echo -e "${GREEN}Start to install TDengine client driver ...${NC}"
      ${csudo} ln -s ${script_dir}/driver/libtaos.* ${lib_link_dir}/libtaos.so.1       || :
      ${csudo} ln -s ${lib_link_dir}/libtaos.so.1 ${lib_link_dir}/libtaos.so           || :
      
      if [[ -d ${lib64_link_dir} && ! -e ${lib64_link_dir}/libtaos.so ]]; then
        ${csudo} ln -s ${script_dir}/driver/libtaos.* ${lib64_link_dir}/libtaos.so.1     || :
        ${csudo} ln -s ${lib64_link_dir}/libtaos.so.1 ${lib64_link_dir}/libtaos.so       || :
      fi

      echo
      echo -e "${GREEN}TDengine client driver is successfully installed!${NC}"
    else
      echo -e "${GREEN}TDengine client driver already exists, Please confirm whether the alert version matches the client driver version!${NC}"
    fi
}

install_driver
