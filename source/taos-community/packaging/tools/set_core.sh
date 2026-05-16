#!/bin/bash
#
# This file is used to set config for core when taosd crash

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

# set -e
# set -x
corePath=$1

csudo=""
if command -v sudo > /dev/null; then
  csudo="sudo "
fi

if [[ ! -n ${corePath} ]]; then
  echo -e -n "${GREEN}Please enter a file directory to save the coredump file${NC}:"
	read corePath
  while true; do
    if [[ ! -z "$corePath" ]]; then
      break
    else
      read -p "Please enter a file directory to save the coredump file:" corePath
    fi
  done
fi

ulimit -c unlimited
${csudo}sed -i '/ulimit -c unlimited/d' /etc/profile ||:
${csudo}sed -i '$a\ulimit -c unlimited' /etc/profile ||:
source /etc/profile

${csudo}mkdir -p ${corePath}  ||:
${csudo}sysctl -w kernel.core_pattern=${corePath}/core-%e-%p  ||:
${csudo}echo "${corePath}/core-%e-%p" | ${csudo}tee /proc/sys/kernel/core_pattern  ||:
${csudo}echo "kernel.core_pattern = ${corePath}/core_%e-%p" >> /etc/sysctl.conf ||:
