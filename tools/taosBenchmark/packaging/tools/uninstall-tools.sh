#!/bin/bash

# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

set -e
#set -x

demoName="taosdemo"
benchmarkName="taosBenchmark"
dumpName="taosdump"
taosName="taos"
toolsName="taostools"

# -----------------------Variables definition---------------------
# Dynamic directory
bin_link_dir="/usr/bin"

#install main path
install_main_dir="/usr/local/taos"

# Color setting
GREEN='\033[1;32m'
NC='\033[0m'

csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo "
fi

function kill_process() {
  pid=$(ps -ef | grep "$1" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo}kill -9 $pid   || :
  fi
}

function uninstall_libtaosws() {
    if [ ! -f ${install_main_dir}/bin/taos ]; then
        [ -f /usr/lib/libtaosws.so ] && ${csudo}rm -f /usr/lib/libtaosws.so ||:
        [ -f /usr/lib64/libtaosws.so ] && ${csudo}rm -f /usr/lib64/libtaosws.so ||:
        [ -f ${install_main_dir}/driver/libtaosws.so ] && ${csudo}rm -f ${install_main_dir}/driver/libtaosws.so ||:
    fi
}

function uninstall_bin() {
    # Remove links
    ${csudo}rm -f ${bin_link_dir}/${demoName}         || :
    ${csudo}rm -f ${bin_link_dir}/${benchmarkName}    || :
    ${csudo}rm -f ${bin_link_dir}/${dumpName}         || :
    ${csudo}rm -f ${bin_link_dir}/rm${toolsName}      || :

    ${csudo}rm -f ${install_main_dir}/bin/${demoName}               || :
    ${csudo}rm -f ${install_main_dir}/bin/${benchmarkName}          || :
    ${csudo}rm -f ${install_main_dir}/bin/${dumpName}               || :
    ${csudo}rm -f "${install_main_dir}/bin/uninstall-tools.sh"      || :
}


function uninstall_taostools() {
    # Start to uninstall
    echo -e "${GREEN}Start to uninstall tools ...${NC}"

    kill_process ${demoName}
    kill_process ${benchmarkName}
    kill_process ${dumpName}

    uninstall_bin
    uninstall_libtaosws

    echo
    echo -e "${GREEN}tools is uninstalled successfully!${NC}"
}

## ==============================Main program starts from here============================
uninstall_taostools
