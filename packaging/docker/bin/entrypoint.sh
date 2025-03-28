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

DISABLE_EXPLORER=${TAOS_DISABLE_EXPLORER:-0}
unset TAOS_DISABLE_EXPLORER

# Get DATA_DIR from taosd -C
DATA_DIR=$(taosd -C | awk '/^(default|cfg_file)[[:space:]]+dataDir[[:space:]]+/ {print $NF; exit}' | sed 's|/*$||')
DATA_DIR=${DATA_DIR:-/var/lib/taos}

# Get FQDN from taosd -C
FQDN=$(taosd -C | awk '/^(default|cfg_file)[[:space:]]+fqdn[[:space:]]+/ {print $NF; exit}' | sed 's|/*$||')
# ensure the fqdn is resolved as localhost
grep "$FQDN" /etc/hosts >/dev/null || echo "127.0.0.1 $FQDN" >>/etc/hosts

# Get first ep from taosd -C
FIRSET_EP=$(taosd -C | awk '/^(default|cfg_file)[[:space:]]+firstEp[[:space:]]+/ {print $NF; exit}' | sed 's|/*$||')
# parse first ep host and port
FIRST_EP_HOST=${FIRSET_EP%:*}
FIRST_EP_PORT=${FIRSET_EP#*:}

# in case of custom server port
SERVER_PORT=$(taosd -C | awk '/^(default|cfg_file)[[:space:]]+serverPort[[:space:]]+/ {print $NF; exit}' | sed 's|/*$||')
SERVER_PORT=${SERVER_PORT:-6030}

set +e
ulimit -c unlimited
# set core files pattern, maybe failed
sysctl -w kernel.core_pattern=/corefile/core-$FQDN-%e-%p >/dev/null >&1
set -e


PID_TAOSD=0
# if dnode has been created or has mnode ep set or the host is first ep or not for cluster, just start.
if [ -f "$DATA_DIR/dnode/dnode.json" ] ||
    [ -f "$DATA_DIR/dnode/mnodeEpSet.json" ] ||
    [ "$TAOS_FQDN" = "$FIRST_EP_HOST" ]; then
    $@ &
    PID_TAOSD=$!
# others will first wait the first ep ready.
else
    if [ "$TAOS_FIRST_EP" = "" ]; then
        echo "run TDengine with single node."
        $@ &
        PID_TAOSD=$!
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
    if ps aux | grep -v grep | grep taosd > dev/null; then
        echo "TDengine is running"
      else
        $@ &
        PID_TAOSD=$!
    fi
fi

PID_TAOSADAPTER=0
if [ "$DISABLE_ADAPTER" = "0" ]; then
    # check if the process is running
    if ! pgrep taosadapter >/dev/null; then
        which taosadapter >/dev/null && {
            taosadapter &
            PID_TAOSADAPTER=$!
        }
    else
        PID_TAOSADAPTER=$(pgrep taosadapter)
        echo "taosadapter already running with PID: $PID_TAOSADAPTER"
    fi
    # wait for 6041 port ready
    for _ in $(seq 1 20); do
        nc -z localhost 6041 && break
        sleep 0.5
    done
fi

PID_TAOSKEEPER=0
if [ "$DISABLE_KEEPER" = "0" ]; then
    # check if the process is running
    if ! pgrep taoskeeper >/dev/null; then
        which taoskeeper >/dev/null && {
            taoskeeper &
            PID_TAOSKEEPER=$!
        }   
    else
        PID_TAOSKEEPER=$(pgrep taoskeeper)
        echo "taoskeeper already running with PID: $PID_TAOSKEEPER"
    fi
    # wait for 6043 port ready
    for _ in $(seq 1 20); do
        nc -z localhost 6043 && break
        sleep 0.5
    done
fi


PID_TAOS_EXPLORER=0
# check if the process is running
if ! pgrep taos-explorer >/dev/null; then
    which taos-explorer >/dev/null && {
        taos-explorer &
        PID_TAOS_EXPLORER=$!
    }
else
    PID_TAOS_EXPLORER=$(pgrep taos-explorer)
    echo "taos-explorer already running with PID: $PID_TAOS_EXPLORER"
fi
# wait for 6060 port ready
for _ in $(seq 1 20); do
    nc -z localhost 6060 && break
    sleep 0.5
done

# create an array to store the PIDs of the processes
WAIT_PIDS=()
[ $PID_TAOSD -ne 0 ] && WAIT_PIDS+=($PID_TAOSD)
[ $PID_TAOSADAPTER -ne 0 ] && WAIT_PIDS+=($PID_TAOSADAPTER)
[ $PID_TAOSKEEPER -ne 0 ] && WAIT_PIDS+=($PID_TAOSKEEPER)
[ $PID_TAOS_EXPLORER -ne 0 ] && WAIT_PIDS+=($PID_TAOS_EXPLORER)

# wait for the processes to finish
if [ ${#WAIT_PIDS[@]} -gt 0 ]; then
    wait -n "${WAIT_PIDS[@]}"
fi