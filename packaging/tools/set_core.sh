#!/bin/bash

# Copyright (c) 2020 TAOS Data, Inc. <jhtao@taosdata.com>
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
