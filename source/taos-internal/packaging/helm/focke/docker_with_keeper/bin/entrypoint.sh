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

CONF_DIR=$(taosd -C|grep -E 'configDir.*(\S+)' -o |head -n1|sed 's/configDir *//')
CONF_DIR=${CONF_DIR:-/etc/taos}


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

ulimit -c unlimited
# set core files pattern, maybe failed
set +e
# sysctl -w kernel.core_pattern=/corefile/core-$FQDN-%e-%p >/dev/null >&1


MONITOR=$(cat "$CONF_DIR/taos.cfg"|grep "monitorFqdn")
set -e

if [ "$MONITOR" = "" ]; then
    echo "monitor 1" >> $CONF_DIR/taos.cfg
    echo "monitorFqdn localhost" >> $CONF_DIR/taos.cfg
fi

# if has mnode ep set or the host is first ep or not for cluster, just start.
if [ -f "$DATA_DIR/dnode/mnodeEpSet.json" ] ||
    [ "$TAOS_FQDN" = "$FIRST_EP_HOST" ]; then
    # $@
    echo ""
# others will first wait the first ep ready.
else
    if [ "$TAOS_FIRST_EP" = "" ]; then
        echo "run TDengine with single node."
        # $@
        # exit $?
    else
        while true; do
            es=$(taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT --check)
            echo ${es}
            if [ "${es%%:*}" -eq 2 ]; then
                echo "execute create dnode"
                taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT -s "create dnode \"$FQDN:$SERVER_PORT\";"
                break
            fi
            sleep 1s
        done
        # $@
    fi
fi

$@
