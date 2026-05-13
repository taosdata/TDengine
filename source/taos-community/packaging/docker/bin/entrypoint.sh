#!/usr/bin/env bash
set -e
# for TZ awareness
if [ "$TZ" != "" ]; then
    ln -sf /usr/share/zoneinfo/$TZ /etc/localtime
    echo $TZ >/etc/timezone
fi

TAOS_ROOT_PASSWORD=${TAOS_ROOT_PASSWORD:-taosdata}
export TAOS_KEEPER_TDENGINE_PASSWORD=${TAOS_ROOT_PASSWORD}

INITDB_DIR=/docker-entrypoint-initdb.d/

# option to disable taosadapter, default is no
DISABLE_ADAPTER=${TAOS_DISABLE_ADAPTER:-0}
unset TAOS_DISABLE_ADAPTER

DISABLE_KEEPER=${TAOS_DISABLE_KEEPER:-0}
unset TAOS_DISABLE_KEEPER

DISABLE_EXPLORER=${TAOS_DISABLE_EXPLORER:-0}
unset TAOS_DISABLE_EXPLORER

# Get DATA_DIR from taosd -C
DATA_DIR=$(taosd -C|grep -E 'dataDir\s+(\S+)' -o |head -n1|sed 's/dataDir *//')
DATA_DIR=${DATA_DIR:-/var/lib/taos}

# Get FQDN from taosd -C
FQDN=$(taosd -C|grep -E 'fqdn\s+(\S+)' -o |head -n1|sed 's/fqdn *//')
# ensure the fqdn is resolved as localhost
grep "$FQDN" /etc/hosts >/dev/null || echo "127.0.0.1 $FQDN" >>/etc/hosts

# Get first ep from taosd -C
FIRST_EP=$(taosd -C|grep -E 'firstEp\s+(\S+)' -o |head -n1|sed 's/firstEp *//')
# parse first ep host and port
FIRST_EP_HOST=${FIRST_EP%:*}
FIRST_EP_PORT=${FIRST_EP#*:}

# in case of custom server port
SERVER_PORT=$(taosd -C|grep -E 'serverPort\s+(\S+)' -o |head -n1|sed 's/serverPort *//')
SERVER_PORT=${SERVER_PORT:-6030}

set +e
ulimit -c unlimited
# set core files pattern, maybe failed
sysctl -w kernel.core_pattern=/corefile/core-$FQDN-%e-%p >/dev/null >&1
set -e

if [ $# -gt 0 ]; then
    exec "$@"
    exit 0
fi

NEEDS_INITDB=0

# if dnode has been created or has mnode ep set or the host is first ep or not for cluster, just start.
if [ -f "$DATA_DIR/dnode/dnode.json" ] ||
    [ -f "$DATA_DIR/dnode/mnodeEpSet.json" ] ||
    [ "$FQDN" = "$FIRST_EP_HOST" ]; then
    echo "start taosd with mnode ep set"
    taosd &
    while true; do
        es=$(taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT --check | grep "^[0-9]*:")
        echo ${es}
        if [ "${es%%:*}" -eq 2 ]; then

            # Initialization scripts should only work in first node.
            if [ "$FQDN" = "$FIRST_EP_HOST" ]; then
                if [ ! -f "${DATA_DIR}/.docker-entrypoint-root-password-changed" ]; then
                    if [ "$TAOS_ROOT_PASSWORD" != "taosdata" ]; then
                        # change default root password
                        taos -s "ALTER USER root PASS '$TAOS_ROOT_PASSWORD'"
                        touch "${DATA_DIR}/.docker-entrypoint-root-password-changed"
                    fi
                fi
                # Initialization scripts should only work in first node.
                if [ ! -f "${DATA_DIR}/.docker-entrypoint-inited" ]; then
                    NEEDS_INITDB=1
                fi
            fi

            break
        fi
        sleep 1s
    done
# others will first wait the first ep ready.
else
    if [ "$TAOS_FIRST_EP" = "" ]; then
        echo "run TDengine with single node."
        taosd &
    fi
    while true; do
        es=$(taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT --check | grep "^[0-9]*:")
        echo ${es}
        if [ "${es%%:*}" -eq 2 ]; then
            echo "execute create dnode"
            sh -c "taos -p'$TAOS_ROOT_PASSWORD' -h $FIRST_EP_HOST -P $FIRST_EP_PORT -s 'create dnode \"$FQDN:$SERVER_PORT\";'"
            break
        fi
        sleep 1s
    done
fi

if [ "$DISABLE_ADAPTER" = "0" ]; then
    which taosadapter >/dev/null && taosadapter &
    # wait for 6041 port ready
    for _ in $(seq 1 20); do
        curl -sf http://localhost:6041/metrics && break
        sleep 0.5
    done
fi

if [ "$DISABLE_KEEPER" = "0" ]; then
    sleep 3
    which taoskeeper >/dev/null && taoskeeper &
    # wait for 6043 port ready
    for _ in $(seq 1 20); do
        curl -sf http://localhost:6043/metrics && break
        sleep 0.5
    done
fi

if [ "$DISABLE_EXPLORER" = "0" ]; then
    which taos-explorer >/dev/null && taos-explorer &
    # wait for 6060 port ready
    for _ in $(seq 1 20); do
        curl -sf http://localhost:6060/metrics && break
        sleep 0.5
    done
fi

if [ "$NEEDS_INITDB" = "1" ]; then
    # check if initdb.d exists
    if [ -d "${INITDB_DIR}" ]; then
        # execute initdb scripts in sql
        for FILE in "$INITDB_DIR"*.sql; do
            echo "Initialize db with file $FILE"
            MAX_RETRIES=5
            RETRY_COUNT=0
            SUCCESS=0
            while [ $RETRY_COUNT -lt $MAX_RETRIES ] && [ "$SUCCESS" = "0" ]; do
                set -x
                OUTPUT=$(sh -c "taos -f $FILE -p'$TAOS_ROOT_PASSWORD'")
                set +x
                echo $OUTPUT
                if [[ "$OUTPUT" =~ "DB error" ]]; then
                    echo "Retrying in 2 seconds..."
                    sleep 2
                    RETRY_COUNT=$((RETRY_COUNT + 1))
                else
                    SUCCESS=1
                fi
            done
        done
    fi
    touch "${DATA_DIR}/.docker-entrypoint-inited"
fi

sh -c "taos -p'$TAOS_ROOT_PASSWORD' -h $FIRST_EP_HOST -P $FIRST_EP_PORT -s 'create snode on dnode 1;'"

trap 'echo "Received stop signal, killing children"; pkill -P $$ || true; exit 0' SIGINT SIGTERM
wait