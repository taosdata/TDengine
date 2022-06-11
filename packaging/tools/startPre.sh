#!/bin/bash
#
# if enable core dump, set start count to 3, disable core dump, set start count to 20.
# set -e
# set -x

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
