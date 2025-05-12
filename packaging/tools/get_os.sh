#!/bin/bash
#
# This file is used to install TAOS time-series database on linux systems. The operating system 
# is required to use systemd to manage services at boot

set -e
#set -x

# -----------------------Variables definition---------------------

if [[ "$OSTYPE" == "darwin"* ]]; then
    echo -e "Darwin"
else
    OS=$(cat /etc/*-release | grep "^NAME=" | cut -d= -f2)
    len=$(echo ${#OS})
    len=$((len-2))
    retval=$(echo -ne ${OS:1:${len}} | cut -d" " -f1)
    echo -ne $retval
fi
