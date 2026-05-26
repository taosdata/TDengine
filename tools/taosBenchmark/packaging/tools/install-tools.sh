#!/bin/bash
#
# This file is used to install taos-tools on your linux systems.
#

set -e
#set -x

demoName="taosdemo"
benchmarkName="taosBenchmark"
dumpName="taosdump"
emailName="taosdata.com"
taosName="taos"
toolsName="taostools"

# -----------------------Variables definition---------------------
script_dir=$(dirname $(readlink -f "$0"))
# Dynamic directory

bin_link_dir="/usr/bin"

#install main path
install_main_dir="/usr/local/taos"

# Color setting
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[1;32m'
NC='\033[0m'

csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo "
fi

# get the operating system type for using the corresponding init file
# ubuntu/debian(deb), centos/fedora(rpm), others: opensuse, redhat, ..., no verification
#osinfo=$(awk -F= '/^NAME/{print $2}' /etc/os-release)
if [[ -e /etc/os-release ]]; then
    osinfo=$(grep "^NAME=" /etc/os-release | cut -d '"' -f2)   ||:
else
    osinfo=""
fi

os_type=0

if echo $osinfo | grep -qwi "ubuntu" ; then
    #  echo "This is ubuntu system"
    os_type=1
elif echo $osinfo | grep -qwi "debian" ; then
    #  echo "This is debian system"
    os_type=1
elif echo $osinfo | grep -qwi "Kylin" ; then
    #  echo "This is Kylin system"
    os_type=1
elif  echo $osinfo | grep -qwi "centos" ; then
    #  echo "This is centos system"
    os_type=2
elif echo $osinfo | grep -qwi "fedora" ; then
    #  echo "This is fedora system"
    os_type=2
elif echo $osinfo | grep -qwi "Linx" ; then
    #  echo "This is Linx system"
    os_type=1
fi

if [ $os_type -ne 1 ] && [ $os_type -ne 2 ]; then
    echo -e "${YELLOW}"
    echo -e " This is an officially unverified linux system,"
    echo -e " if there are any problems with the installation and operation, "
    echo -e " please feel free to contact ${emailName} for support."
    echo -e "${NC}"
    os_type=1
fi

function kill_process() {
    pid=$(ps -ef | grep "$1" | grep -v "grep" | awk '{print $2}')
    if [ -n "$pid" ]; then
        ${csudo}kill -9 $pid   || :
    fi
}

function install_main_path() {
    #create install main dir and all sub dir
    [[ ! -d ${install_main_dir}/bin ]] && ${csudo}mkdir -p ${install_main_dir}/bin || :
}

function install_libtaosws() {
    if [ -f ${script_dir}/driver/libtaosws.so ]; then
        [ -d ${install_main_dir}/driver ] || ${csudo}mkdir ${install_main_dir}/driver ||:
        [ -f ${install_main_dir}/driver/libtaosws.so ] || \
            ${csudo}/usr/bin/install -c -m 755 ${script_dir}/driver/libtaosws.so ${install_main_dir}/driver/libtaosws.so && ${csudo}ln -sf ${install_main_dir}/driver/libtaosws.so /usr/lib/libtaosws.so || echo -e "${RED} failed to install libtaosws.so ${NC}"
        if [ -d /usr/lib64 ]; then
            ${csudo}ln -sf ${install_main_dir}/driver/libtaosws.so /usr/lib64/libtaosws.so || echo -e "${RED}failed to link libtaosws.so to /usr/lib64 ${NC}"
        fi
    fi
}

function install_bin() {
    # Remove links
    ${csudo}rm -f ${bin_link_dir}/${demoName}         || :
    ${csudo}rm -f ${bin_link_dir}/${benchmarkName}    || :
    ${csudo}rm -f ${bin_link_dir}/${dumpName}         || :
    ${csudo}rm -f ${bin_link_dir}/rm${toolsName}      || :

    ${csudo}/usr/bin/install -c -m 755 ${script_dir}/bin/${dumpName} ${install_main_dir}/bin/${dumpName} || echo -e "${RED}${dumpName}not installed${NC}"
    ${csudo}/usr/bin/install -c -m 755 ${script_dir}/bin/${benchmarkName} ${install_main_dir}/bin/${benchmarkName} || echo -e "${RED}${benchmarkName}not installed${NC}"
    ${csudo}/usr/bin/install -c -m 755 ${script_dir}/uninstall-tools.sh ${install_main_dir}/bin/uninstall-tools.sh || echo -e "${RED}uninstall-tools not installed${NC}"
    #Make link
    [[ -x ${install_main_dir}/bin/${benchmarkName} ]] && \
        ${csudo}ln -sf ${install_main_dir}/bin/${benchmarkName} ${install_main_dir}/bin/${demoName}     || :
    [[ -x ${install_main_dir}/bin/${benchmarkName} ]] && \
        ${csudo}ln -s ${install_main_dir}/bin/${benchmarkName} ${bin_link_dir}/${benchmarkName}         || :
    [[ -x ${install_main_dir}/bin/${demoName} ]] && \
        ${csudo}ln -s ${install_main_dir}/bin/${demoName} ${bin_link_dir}/${demoName}                   || :
    [[ -x ${install_main_dir}/bin/${dumpName} ]] && \
        ${csudo}ln -s ${install_main_dir}/bin/${dumpName} ${bin_link_dir}/${dumpName}                   || :
    [[ -x ${install_main_dir}/bin/uninstall-tools.sh ]] && \
        ${csudo}ln -s ${install_main_dir}/bin/uninstall-tools.sh ${bin_link_dir}/rm${toolsName}         || :
}

function install_taostools() {
    # Start to install
    echo -e "${GREEN}Start to install tools ...${NC}"

    install_main_path

    # For installing new
    install_bin
    install_libtaosws

    echo
    echo -e "${GREEN}tools is installed successfully!${NC}"
}

## ==============================Main program starts from here============================
install_taostools
