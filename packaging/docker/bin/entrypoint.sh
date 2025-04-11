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

DISABLE_KEEPER=${TAOS_DISABLE_KEEPER:-0}
unset TAOS_DISABLE_KEEPER
if [ "$ENABLE_MONITOR" = "0" ]; then
    DISABLE_KEEPER=1
fi
if [ "$DISABLE_KEEPER" = "0" ]; then
    export TAOS_MONITOR_FQDN=${TAOS_MONITOR_FQDN:-localhost}
fi
which taoskeeper >/dev/null || export DISABLE_KEEPER=1

DISABLE_EXPLORER=${TAOS_DISABLE_EXPLORER:-0}
unset TAOS_DISABLE_EXPLORER
if [ "$ENABLE_TAOSX" = "0" ]; then
    DISABLE_EXPLORER="0"
fi
DISABLE_TAOSX=${TAOS_DISABLE_TAOSX:-0}
unset TAOS_DISABLE_TAOSX
if [ "$ENABLE_TAOSX" = "0" ]; then
    DISABLE_TAOSX="1"
fi

which taosx >/dev/null || export DISABLE_TAOSX=1

DISABLE_SERVER=${TAOS_DISABLE_SERVER:-0}
unset TAOS_DISABLE_SERVER
if [ "$ENABLE_SERVER" = "0" ]; then
    DISABLE_SERVER="1"
fi

TAOS_DISABLE_MNODE=${TAOS_DISABLE_MNODE:-0}

# Get DATA_DIR from taosd -C
DATA_DIR=$(taosd -C |grep -E 'dataDir.*(\S+)' -o |head -n1|sed 's/dataDir *//')
DATA_DIR=${DATA_DIR:-/var/lib/taos}

# Get FQDN from taosd -C
FQDN=$(taosd -C |grep -E 'fqdn.*(\S+)' -o |head -n1|sed 's/fqdn *//')
# ensure the fqdn is resolved as localhost
grep "$FQDN" /etc/hosts >/dev/null || echo "127.0.0.1 $FQDN" >>/etc/hosts

# Get first ep from taosd -C
FIRST_EP=$(taosd -C |grep -E 'firstEp.*(\S+)' -o |head -n1|sed 's/firstEp *//')
# parse first ep host and port
export FIRST_EP_HOST=${FIRST_EP%:*}
export FIRST_EP_PORT=${FIRST_EP#*:}

if [ "$DISABLE_SERVER" = "1" ]; then
    export TAOS_KEEPER_TDENGINE_HOST=${TAOS_KEEPER_TDENGINE_HOST:-$FIRST_EP_HOST}
fi

# in case of custom server port
SERVER_PORT=$(taosd -C|grep -E 'serverPort.*(\S+)' -o |head -n1|sed 's/serverPort *//')
SERVER_PORT=${SERVER_PORT:-6030}

set +e
ulimit -c unlimited
# set core files pattern, maybe failed
sysctl -w kernel.core_pattern=/corefile/core-$FQDN-%e-%p >/dev/null >&1
set -e

if [ $# -gt 0 ]; then
    exec $@
    exit 0
fi
# startup taosd
if [ "$DISABLE_SERVER" = "0" ]; then
    echo "enable server"

    # startup taosd
    if [ -f "$DATA_DIR/dnode/dnode.json" ] ||
        [ -f "$DATA_DIR/dnode/mnodeEpSet.json" ] ||
        [ "$TAOS_FQDN" = "$FIRST_EP_HOST" ]; then
        echo "start taosd with mnode ep set"
        taosd &
        # wait for serverPort ready
        for _ in $(seq 1 20); do
            nc -z localhost $SERVER_PORT && break
            sleep 0.5
        done
    else
        while true; do
            es=$(taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT --check)
            echo "Try to connect to first ep with return: ${es} (taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT --check)"
            if [ "${es%%:*}" -eq 2 ]; then
                echo "execute to create dnode after connected to first ep"
                taosd &
                # wait for serverPort ready
                for _ in $(seq 1 20); do
                    nc -z localhost $SERVER_PORT && break
                    sleep 0.5
                done
                ENDPOINT=$FQDN:$SERVER_PORT
                taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT -s "create dnode \"$ENDPOINT\";"
                DNODETmp=$(taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT -s "set max_binary_display_width 2000;show dnodes;" | grep -E "$ENDPOINT" | awk '{split($0,a,"|");print a[1]}')
                DNODEID=$(echo "$DNODETmp" | sed -e 's/^[[:space:]]*//')
                if [ "$DNODEID" != "" ] && [ "$TAOS_DISABLE_MNODE" = "0" ]; then
                    set +e
                    echo "Create the mnode for dnode $DNODEID"
                    for _ in $(seq 1 5); do
                        sleep 1s
                        MNODETmp=$(taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT -s "create mnode on dnode $DNODEID;")
                        echo "Create the mnode for dnode $DNODEID with return: ${MNODETmp}"

                        MNODEOK=$(echo "$MNODETmp" | grep "Create OK")
                        if [ "$MNODEOK" != "" ]; then
                            echo "Create the mnode for dnode $DNODEID success"
                            break
                        fi
                    done
                    set -e
                    break
                fi
            fi
            sleep 1s
        done
    fi

    if [ "$DISABLE_ADAPTER" = "0" ]; then
        echo "enable taosadapter"
        # startup taosadapter
        taosadapter &
        # wait for 6041 port ready
        for _ in $(seq 1 20); do
            nc -z localhost 6041 && break
            sleep 0.5
        done
    fi
fi


if [ "$DISABLE_KEEPER" = "0" ]; then
    sleep 3
    which taoskeeper >/dev/null && taoskeeper &
    # wait for 6043 port ready
    for _ in $(seq 1 20); do
        nc -z localhost 6043 && break
        sleep 0.5
    done
fi

if [ "$DISABLE_TAOSX" = "0" ]; then
    echo "enable taosx"
    # startup taosx
    taosx serve &
    # wait for 6050 port ready
    for _ in $(seq 1 20); do
        nc -z localhost 6050 && break
        sleep 0.5
    done
else
    echo "disable taosx"
fi

if [ "$DISABLE_EXPLORER" = "0" ]; then
    echo "enable taos-explorer"
    # startup explorer
    taos-explorer &
    # wait for 6060 port ready
    for _ in $(seq 1 20); do
        nc -z localhost 6060 && break
        sleep 0.5
    done
else
    echo "disable taos-explorer"
fi

# never exit
while true; do
  sleep 1000s
done
