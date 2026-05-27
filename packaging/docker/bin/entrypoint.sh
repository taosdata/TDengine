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

# PIDs of every long-running child we start. Used by the watchdog at the bottom
# of this script to take the whole container down if any single one of them
# dies (#34179). Without this, taosd being OOM-killed would leave the container
# alive (taosadapter still up) and Docker's --restart would never fire.
PIDS=""

# Install cleanup traps early so that *any* exit path (signal, set -e, watchdog)
# still tears down the background children we are about to start. Without an
# EXIT trap, a `set -e` failure during initdb / create-dnode / create-snode
# would leave orphans behind for tini to reap, and the container would just
# blink instead of going through the normal "restart" loop.
cleanup_children() {
    # Use the saved PIDS first so we don't accidentally signal anything else
    # tini may have adopted. pkill -P $$ is the safety net for anything
    # backgrounded that we forgot to record.
    for pid in $PIDS; do
        kill -TERM "$pid" 2>/dev/null || true
    done
    pkill -P $$ 2>/dev/null || true
}
trap 'echo "Received stop signal, killing children"; cleanup_children; exit 0' SIGINT SIGTERM
# EXIT trap: capture the script's exit code BEFORE running cleanup (pkill etc.
# would otherwise overwrite $?), then re-exit with that same code so Docker
# sees the real cause instead of the trap helpers' last status.
trap 'rc=$?; if [ "$rc" -ne 0 ]; then echo "entrypoint exiting abnormally (rc=$rc), killing children"; cleanup_children; fi; exit "$rc"' EXIT

# Wait up to ~10s for a service's metrics endpoint to come up. If the service
# process itself dies in the meantime (immediate crash on bad config, port
# in use, etc.) fail fast instead of burning the full timeout in silence.
# Per gemini-code-assist review on PR #35368.
wait_for_metric_or_die() {
    local svc=$1 pid=$2 url=$3
    for _ in $(seq 1 20); do
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "$svc (pid $pid) died during startup; aborting entrypoint" >&2
            exit 1
        fi
        curl -sf "$url" && return 0
        sleep 0.5
    done
}

# if dnode has been created or has mnode ep set or the host is first ep or not for cluster, just start.
if [ -f "$DATA_DIR/dnode/dnode.json" ] ||
    [ -f "$DATA_DIR/dnode/mnodeEpSet.json" ] ||
    [ "$FQDN" = "$FIRST_EP_HOST" ]; then
    echo "start taosd with mnode ep set"
    taosd &
    TAOSD_PID=$!
    PIDS="$PIDS $TAOSD_PID"
    while true; do
        # If taosd died during startup, fail fast instead of looping forever
        # waiting for `taos --check` to succeed against a dead server.
        if ! kill -0 "$TAOSD_PID" 2>/dev/null; then
            echo "taosd (pid $TAOSD_PID) died during startup; aborting entrypoint" >&2
            exit 1
        fi
        # `|| true` keeps a transient connection error from tripping set -e while
        # we're still polling for taosd to come up; the kill -0 above is the
        # authoritative "did the process die" check.
        es=$(taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT --check 2>/dev/null | grep "^[0-9]*:" || true)
        echo ${es}
        if [ -n "$es" ] && [ "${es%%:*}" = "2" ]; then

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
    TAOSD_PID=""
    if [ "$TAOS_FIRST_EP" = "" ]; then
        echo "run TDengine with single node."
        taosd &
        TAOSD_PID=$!
        PIDS="$PIDS $TAOSD_PID"
    fi
    while true; do
        # If we started taosd locally and it died early, fail fast.
        # Cluster non-first nodes (TAOSD_PID empty) wait for the remote first EP.
        if [ -n "$TAOSD_PID" ] && ! kill -0 "$TAOSD_PID" 2>/dev/null; then
            echo "taosd (pid $TAOSD_PID) died during startup; aborting entrypoint" >&2
            exit 1
        fi
        # `|| true` keeps a transient connection error from tripping set -e while
        # we're still polling for taosd to come up; the kill -0 above is the
        # authoritative "did the process die" check.
        es=$(taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT --check 2>/dev/null | grep "^[0-9]*:" || true)
        echo ${es}
        if [ -n "$es" ] && [ "${es%%:*}" = "2" ]; then
            echo "execute create dnode"
            sh -c "taos -p'$TAOS_ROOT_PASSWORD' -h $FIRST_EP_HOST -P $FIRST_EP_PORT -s 'create dnode \"$FQDN:$SERVER_PORT\";'"
            break
        fi
        sleep 1s
    done
fi

if [ "$DISABLE_ADAPTER" = "0" ] && which taosadapter >/dev/null; then
    taosadapter &
    TAOSADAPTER_PID=$!
    PIDS="$PIDS $TAOSADAPTER_PID"
    wait_for_metric_or_die taosadapter "$TAOSADAPTER_PID" http://localhost:6041/metrics
fi

if [ "$DISABLE_KEEPER" = "0" ] && which taoskeeper >/dev/null; then
    sleep 3
    taoskeeper &
    TAOSKEEPER_PID=$!
    PIDS="$PIDS $TAOSKEEPER_PID"
    wait_for_metric_or_die taoskeeper "$TAOSKEEPER_PID" http://localhost:6043/metrics
fi

if [ "$DISABLE_EXPLORER" = "0" ] && which taos-explorer >/dev/null; then
    taos-explorer &
    TAOSEXPLORER_PID=$!
    PIDS="$PIDS $TAOSEXPLORER_PID"
    wait_for_metric_or_die taos-explorer "$TAOSEXPLORER_PID" http://localhost:6060/metrics
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

# Watchdog: if any of taosd / taosadapter / taoskeeper / taos-explorer dies
# (e.g. taosd OOM-killed), tear the whole container down so Docker's --restart
# can bring it back. Before this, only taosd dying would silently leave
# taosadapter accepting requests it could not fulfill (#34179).
#
# Polling instead of `wait -n` keeps this compatible with bash versions that
# do not support `-n`; the 2s tick is negligible CPU.
# Cleanup traps were installed at the top of this script — both the SIGINT/
# SIGTERM and the EXIT trap call cleanup_children, so we don't need to repeat
# the pkill plumbing here.

if [ -z "$PIDS" ]; then
    # cluster non-first node with every sidecar disabled — nothing to watch.
    # Stay alive so the SIGINT/SIGTERM trap can still stop the container.
    echo "No background TDengine processes to watch; idling"
    while true; do sleep 3600; done
fi

echo "All services started, watching child processes: $PIDS"
while true; do
    for pid in $PIDS; do
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "child process $pid exited; shutting container down so Docker can restart it"
            # cleanup_children runs via the EXIT trap; just give children a
            # beat to actually wind down before we let the container exit.
            sleep 1
            exit 1
        fi
    done
    sleep 2
done