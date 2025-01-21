#!/bin/bash

filesPath="/data/telemetry/crash-report"
version="3.0.4.1"
taosdataIp="103.229.218.146"
grep "\"version\":\"${version}\"" ${filesPath}/*.txt \
| grep  "taosd(" \
| awk -F "stackInfo" '{print $2}' \
| grep "taosAssertDebug" \
| grep -v ${taosdataIp} \
| awk -F "taosd" '{print $3}' \
| cut -d")" -f 1 \
| cut -d"(" -f 2 \
| sort | uniq -c
