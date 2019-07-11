#!/bin/bash
#
# This file is used to install TAOS time-series database on linux systems. The operating system 
# is required to use systemd to manage services at boot

set -e
# set -x

# -----------------------Variables definition---------------------
script_dir=$(dirname $(readlink -m "$0"))
verinfo=$(cat ${script_dir}/../../src/util/src/version.c | grep " version" | cut -d '"' -f2)
verinfo=$(echo $verinfo | tr "\n" " ")
len=$(echo ${#verinfo})
len=$((len-1))
retval=$(echo -ne ${verinfo:0:${len}})
echo -ne $retval
