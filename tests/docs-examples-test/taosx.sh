#!/bin/bash

set -e

taosd >>/dev/null 2>&1 &
taosadapter >>/dev/null 2>&1 &
sleep 5

taosBenchmark -y -n 10000 -t 1000

taosx run -f 'tmq:///test' -t 'taos:///test1' -y

taos -s 'select count(*) from test1.meters' |grep 10000000 || (echo database sync failed!; exit 1)

taosx run -f 'tmq:///test' -t 'local:./test-backup' -y
taosx run -f 'local:./test-backup' -t 'taos:///test2' -y

rm -rf ./test-backup
