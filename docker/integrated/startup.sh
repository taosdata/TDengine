#!/bin/sh
set -e
# for TZ awareness
if [ "$TZ" != "" ]; then
    ln -sf /usr/share/zoneinfo/$TZ /etc/localtime
    echo $TZ >/etc/timezone
fi

# to get mnodeEpSet from data dir
DATA_DIR=$(taosd -C|grep -E 'dataDir.*(\S+)' -o |head -n1|sed 's/dataDir *//')
DATA_DIR=${DATA_DIR:-/var/lib/taos}

FQDN=$(taosd -C|grep -E 'fqdn.*(\S+)' -o |head -n1|sed 's/fqdn *//')
# ensure the fqdn is resolved as localhost
grep "$FQDN" /etc/hosts >/dev/null || echo "127.0.0.1 $FQDN" >>/etc/hosts
FIRSET_EP=$(taosd -C|grep -E 'firstEp.*(\S+)' -o |head -n1|sed 's/firstEp *//')
# parse first ep host and port
FIRST_EP_HOST=${FIRSET_EP%:*}
FIRST_EP_PORT=${FIRSET_EP#*:}

# in case of custom server port
SERVER_PORT=$(taosd -C|grep -E 'serverPort.*(\S+)' -o |head -n1|sed 's/serverPort *//')
SERVER_PORT=${SERVER_PORT:-6030}

set +e
ulimit -c unlimited
# set core files pattern, maybe failed
sysctl -w kernel.core_pattern=/corefile/core-$FQDN-%e-%p >/dev/null >&1
set -e

# startup taosadapter
taosd &
# wait for 6030 port ready
for _ in $(seq 1 20); do
    nc -z localhost 6030 && break
    sleep 0.5
done

# startup taosadapter
which taosadapter >/dev/null && taosadapter &
# wait for 6041 port ready
for _ in $(seq 1 20); do
    nc -z localhost 6041 && break
    sleep 0.5
done

# startup taosx
/usr/bin/taosx serve -c $TAOSX_CONFIG &
# wait for 6050 port ready
for _ in $(seq 1 20); do
    nc -z localhost 6050 && break
    sleep 0.5
done

# startup explorer
/usr/bin/taos-explorer -C $EXPLORER_CONFIG_FILE &
# wait for 6060 port ready
for _ in $(seq 1 20); do
    nc -z localhost 6060 && break
    sleep 0.5
done

# never exit
while true; do
  sleep 1000s
done
