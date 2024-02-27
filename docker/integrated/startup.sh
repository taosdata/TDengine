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

if [ "$TAOS_MONITOR" = "1" ]; then
    export TAOS_MONITOR_FQDN=${TAOS_MONITOR_FQDN:-localhost}
fi

# startup taosd
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

# if has mnode ep set or the host is first ep or not for cluster, just start.
if [ -f "$DATA_DIR/dnode/mnodeEpSet.json" ] || [ "$TAOS_FQDN" = "$FIRST_EP_HOST" ]; then
    $@
# others will first wait the first ep ready.
else
    if [ "$TAOS_FIRST_EP" = "" ]; then
        echo "run TDengine with single node."
        $@
    fi
    while true; do
        es=$(taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT --check)
        echo "Try to connect to first ep with return: ${es}"
        if [ "${es%%:*}" -eq 2 ]; then
            echo "execute to create dnode after connected to first ep"
            ENDPOINT=$FQDN:$SERVER_PORT
            taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT -s "create dnode \"$ENDPOINT\";"
            DNODETmp=$(taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT -s "set max_binary_display_width 2000;show dnodes;" | grep -E "$ENDPOINT" | awk '{split($0,a,"|");print a[1]}')
            DNODEID=$(echo "$DNODETmp" | sed -e 's/^[[:space:]]*//')
            if [ "$DNODEID" != "" ]; then
                taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT -s "create mnode on dnode $DNODEID;"
                echo "Created the mnode for dnode $DNODEID"
                break
            fi
        fi
        sleep 1s
    done
    $@
fi

# startup taoskeeper
if [ "$TAOS_MONITOR" = "1" ]; then
    which taoskeeper >/dev/null && taoskeeper &
    for _ in $(seq 1 20); do
        nc -z localhost 6043 && break
        sleep 0.5
    done
fi

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
