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
DATA_DIR=${TAOS_DATA_DIR:-/var/lib/taos}

# append env to custom taos.cfg
CFG_DIR=/tmp/taos
CFG_FILE=$CFG_DIR/taos.cfg

mkdir -p $CFG_DIR >/dev/null 2>&1

[ -f /etc/taos/taos.cfg ] && cat /etc/taos/taos.cfg | grep -E -v "^#|^\s*$" >$CFG_FILE
env-to-cfg >>$CFG_FILE

FQDN=$(cat $CFG_FILE | grep -E -v "^#|^$" | grep fqdn | tail -n1 | sed -E 's/.*fqdn\s+//')

# ensure the fqdn is resolved as localhost
grep "$FQDN" /etc/hosts >/dev/null || echo "127.0.0.1 $FQDN" >>/etc/hosts

# parse first ep host and port
FIRST_EP_HOST=${TAOS_FIRST_EP%:*}
FIRST_EP_PORT=${TAOS_FIRST_EP#*:}

# in case of custom server port
SERVER_PORT=$(cat $CFG_FILE | grep -E -v "^#|^$" | grep serverPort | tail -n1 | sed -E 's/.*serverPort\s+//')
SERVER_PORT=${SERVER_PORT:-6030}

# for other binaries like interpreters
if echo $1 | grep -E "taosd$" - >/dev/null; then
    true # will run taosd
else
    cp -f $CFG_FILE /etc/taos/taos.cfg || true
    $@
    exit $?
fi

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

# if has mnode ep set or the host is first ep or not for cluster, just start.
if [ -f "$DATA_DIR/dnode/mnodeEpSet.json" ] ||
    [ "$TAOS_FQDN" = "$FIRST_EP_HOST" ]; then
    $@ -c $CFG_DIR
# others will first wait the first ep ready.
else
    if [ "$TAOS_FIRST_EP" = "" ]; then
        echo "run TDengine with single node."
        $@ -c $CFG_DIR
        exit $?
    fi
    while true; do
        es=0
        taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT -n startup >/dev/null || es=$?
        if [ "$es" -eq 0 ]; then
            taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT -s "create dnode \"$FQDN:$SERVER_PORT\";"
            break
        fi
        sleep 1s
    done
    $@ -c $CFG_DIR
fi
