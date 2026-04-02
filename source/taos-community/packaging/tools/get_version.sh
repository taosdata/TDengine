#!/bin/bash
#
# This file is used to install TAOS time-series database on linux systems. The operating system 
# is required to use systemd to manage services at boot

set -e
# set -x

# -----------------------Variables definition---------------------
verinfo=$(cat $1 | grep " version" | cut -d '"' -f2)
verinfo=$(echo $verinfo | tr "\n" " ")
len=$(echo ${#verinfo})
len=$((len-1))
retval=$(echo -ne ${verinfo:0:${len}})
echo -ne $retval
