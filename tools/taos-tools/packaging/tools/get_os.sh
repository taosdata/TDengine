#!/bin/bash
#
# This file is used to install TAOS time-series database on linux systems. The operating system
# is required to use systemd to manage services at boot

set -e
# set -x

# -----------------------Variables definition---------------------
OS=$(grep "^NAME=" /etc/*-release | cut -d= -f2)
len=$(echo ${#OS})
len=$((len-2))
retval=$(echo -ne ${OS:1:${len}} | cut -d" " -f1)
echo -ne $retval
