#!/bin/sh
set -e
# for TZ awareness
if [ "$TZ" != "" ]; then
    ln -sf /usr/share/zoneinfo/$TZ /etc/localtime
    echo $TZ >/etc/timezone
fi

# option to disable taosadapter, default is no
DISABLE_ADAPTER=${TAOS_DISABLE_ADAPTER:-0}
unset TAOS_DISABLE_ADAPTER

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

if [ "$DISABLE_ADAPTER" = "0" ]; then
    which taosadapter >/dev/null && taosadapter &
    # wait for 6041 port ready
    for _ in $(seq 1 20); do
        nc -z localhost 6041 && break
        sleep 0.5
    done
fi

# if dnode has been created or has mnode ep set or the host is first ep or not for cluster, just start.
if [ -f "$DATA_DIR/dnode/dnode.json" ] ||
    [ -f "$DATA_DIR/dnode/mnodeEpSet.json" ] ||
    [ "$TAOS_FQDN" = "$FIRST_EP_HOST" ]; then
    $@
# others will first wait the first ep ready.
else
    if [ "$TAOS_FIRST_EP" = "" ]; then
        echo "run TDengine with single node."
        $@
        exit $?
    fi
    while true; do
        es=$(taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT --check | grep "^[0-9]*:")
        echo ${es}
        if [ "${es%%:*}" -eq 2 ]; then
            echo "execute create dnode"
            taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT -s "create dnode \"$FQDN:$SERVER_PORT\";"
            break
        fi
        sleep 1s
    done
    $@
fi
