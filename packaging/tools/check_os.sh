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

CSI=$(echo -e "\033[")
CRED="${CSI}1;31m"
CFAILURE="$CRED"
CEND="${CSI}0m"
if [ -n "$(grep 'Aliyun Linux release' /etc/issue)" -o -e /etc/redhat-release ]; then
    OS=CentOS
    [ -n "$(grep ' 7\.' /etc/redhat-release 2> /dev/null)" ] && CentOS_RHEL_version=7
    [ -n "$(grep ' 6\.' /etc/redhat-release 2> /dev/null)" -o -n "$(grep 'Aliyun Linux release6 15' /etc/issue)" ] && CentOS_RHEL_version=6
    [ -n "$(grep ' 5\.' /etc/redhat-release 2> /dev/null)" -o -n "$(grep 'Aliyun Linux release5' /etc/issue)" ] && CentOS_RHEL_version=5
elif [ -n "$(grep 'Amazon Linux AMI release' /etc/issue)" -o -e /etc/system-release ]; then
    OS=CentOS
    CentOS_RHEL_version=6
elif [ -n "$(grep 'bian' /etc/issue)" -o "$(lsb_release -is 2>/dev/null)" == "Debian" ]; then
    OS=Debian
    [ ! -e "$(which lsb_release)" ] && { apt-get -y update; apt-get -y install lsb-release; clear; }
    Debian_version=$(lsb_release -sr | awk -F. '{print $1}')
elif [ -n "$(grep 'Deepin' /etc/issue)" -o "$(lsb_release -is 2>/dev/null)" == "Deepin" ]; then
    OS=Debian
    [ ! -e "$(which lsb_release)" ] && { apt-get -y update; apt-get -y install lsb-release; clear; }
    Debian_version=$(lsb_release -sr | awk -F. '{print $1}')
elif [ -n "$(grep 'Kali GNU/Linux Rolling' /etc/issue)" -o "$(lsb_release -is 2>/dev/null)" == "Kali" ]; then
    OS=Debian
    [ ! -e "$(which lsb_release)" ] && { apt-get -y update; apt-get -y install lsb-release; clear; }
    if [ -n "$(grep 'VERSION="2016.*"' /etc/os-release)" ]; then
        Debian_version=8
    else
        echo "${CFAILURE}Does not support this OS, Please contact the author! ${CEND}"
        kill -9 $$
    fi
elif [ -n "$(grep 'Ubuntu' /etc/issue)" -o "$(lsb_release -is 2>/dev/null)" == "Ubuntu" -o -n "$(grep 'Linux Mint' /etc/issue)" ]; then
    OS=Ubuntu
    [ ! -e "$(which lsb_release)" ] && { apt-get -y update; apt-get -y install lsb-release; clear; }
    Ubuntu_version=$(lsb_release -sr | awk -F. '{print $1}')
    [ -n "$(grep 'Linux Mint 18' /etc/issue)" ] && Ubuntu_version=16
elif [ -n "$(grep 'elementary' /etc/issue)" -o "$(lsb_release -is 2>/dev/null)" == 'elementary' ]; then
    OS=Ubuntu
    [ ! -e "$(which lsb_release)" ] && { apt-get -y update; apt-get -y install lsb-release; clear; }
    Ubuntu_version=16
else
    echo "${CFAILURE}Does not support this OS, Please contact the author! ${CEND}"
    kill -9 $$
fi

echo "${CFAILURE}${OS}${CEND}"
if [ "$OS" == 'CentOS' ]; then
    echo ${CentOS_RHEL_version}
else
    echo ${Ubuntu_version}
fi

