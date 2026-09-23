#!/bin/bash

# Copyright (c) 2021 TAOS Data, Inc. <jhtao@taosdata.com>
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

serverName="taosd"
logDir="/var/log/taos"

taosd=/etc/systemd/system/${serverName}.service
line=$(grep StartLimitBurst ${taosd})
num=${line##*=}
#echo "burst num: ${num}"

startSeqFile=${logDir}/.startSeq
recordFile=${logDir}/.startRecord

startSeq=0

if [[ ! -e ${startSeqFile} ]]; then
  startSeq=0
else
  startSeq=$(cat ${startSeqFile})
fi

nextSeq=$(expr $startSeq + 1)
echo "${nextSeq}" >${startSeqFile}

curTime=$(date "+%Y-%m-%d %H:%M:%S")
echo "startSeq:${startSeq} startPre.sh exec ${curTime}, burstCnt:${num}" >>${recordFile}

coreFlag=$(ulimit -c)
echo "coreFlag: ${coreFlag}" >>${recordFile}

if [ ${coreFlag} = "0" ]; then
  #echo "core is 0"
  if [ ${num} != "20" ]; then
    sed -i "s/^.*StartLimitBurst.*$/StartLimitBurst=20/" ${taosd}
    systemctl daemon-reload
    echo "modify burst count from ${num} to 20" >>${recordFile}
  fi
fi

if [ ${coreFlag} = "unlimited" ]; then
  #echo "core is unlimited"
  if [ ${num} != "3" ]; then
    sed -i "s/^.*StartLimitBurst.*$/StartLimitBurst=3/" ${taosd}
    systemctl daemon-reload
    echo "modify burst count from ${num} to 3" >>${recordFile}
  fi
fi
