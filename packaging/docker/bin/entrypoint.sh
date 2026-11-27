#!/usr/bin/env bash

# Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

set -e

# 'wait -n <pid>...' needs bash >= 4.3. The runtime image is debian bookworm
# (bash 5.2), but fail loudly if the base image is ever changed to one with an
# older shell or busybox -- otherwise process supervision would degrade silently.
if [ "${BASH_VERSINFO[0]}" -lt 4 ] ||
    { [ "${BASH_VERSINFO[0]}" -eq 4 ] && [ "${BASH_VERSINFO[1]}" -lt 3 ]; }; then
    echo "entrypoint.sh requires bash >= 4.3 for 'wait -n', found ${BASH_VERSION}" >&2
    exit 1
fi

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

# Fail fast when a child we started has already exited, instead of waiting out
# the full readiness timeout and then running initdb / create-snode against a
# dead server. Uses the shell builtin 'kill -0'; pgrep comes from procps, which
# is not installed in this image.
child_alive_or_die() {
    local name=$1 pid=$2
    [ -n "$pid" ] || return 0
    [ "$pid" -ne 0 ] || return 0
    # Ask bash which background jobs are still running rather than using
    # 'kill -0': in a busy container the PID of a dead child can be recycled by
    # an unrelated process, which would make 'kill -0' report it as alive.
    case " $(jobs -pr | tr '\n' ' ') " in
        *" $pid "*) return 0 ;;
    esac
    echo "$name (pid $pid) exited during startup; aborting entrypoint" >&2
    exit 1
}

# Check every child started so far, not just the one the current loop is waiting
# for: taosd can die while we are still probing taosadapter's metrics port.
check_children_alive() {
    child_alive_or_die taosd "$PID_TAOSD"
    child_alive_or_die taosadapter "$PID_TAOSADAPTER"
    child_alive_or_die taoskeeper "$PID_TAOSKEEPER"
    child_alive_or_die taos-explorer "$PID_TAOS_EXPLORER"
}
PID_TAOSD=0
PID_TAOSADAPTER=0
PID_TAOSKEEPER=0
PID_TAOS_EXPLORER=0

# if dnode has been created or has mnode ep set or the host is first ep or not for cluster, just start.
if [ -f "$DATA_DIR/dnode/dnode.json" ] ||
    [ -f "$DATA_DIR/dnode/mnodeEpSet.json" ] ||
    [ "$FQDN" = "$FIRST_EP_HOST" ]; then
    echo "start taosd with mnode ep set"
    taosd & PID_TAOSD=$!
    ready_wait=0
    while true; do
        check_children_alive
        ready_wait=$((ready_wait + 1))
        if [ "$ready_wait" -gt 300 ]; then
            echo "taosd was not ready within 300s; aborting entrypoint" >&2
            exit 1
        fi
        # 'taos --check' also fails while taosd is still booting, so its exit
        # status must be ignored here or 'set -e' would abort on the first try.
        es=$(taos -h "$FIRST_EP_HOST" -P "$FIRST_EP_PORT" --check 2>/dev/null | grep "^[0-9]*:" || true)
        echo ${es}
        if [ -n "$es" ] && [ "${es%%:*}" -eq 2 ]; then

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
        taosd & PID_TAOSD=$!
    fi
    ready_wait=0
    while true; do
        check_children_alive
        ready_wait=$((ready_wait + 1))
        if [ "$ready_wait" -gt 300 ]; then
            echo "taosd was not ready within 300s; aborting entrypoint" >&2
            exit 1
        fi
        # 'taos --check' also fails while taosd is still booting, so its exit
        # status must be ignored here or 'set -e' would abort on the first try.
        es=$(taos -h "$FIRST_EP_HOST" -P "$FIRST_EP_PORT" --check 2>/dev/null | grep "^[0-9]*:" || true)
        echo ${es}
        if [ -n "$es" ] && [ "${es%%:*}" -eq 2 ]; then
            echo "execute create dnode"
            sh -c "taos -p'$TAOS_ROOT_PASSWORD' -h $FIRST_EP_HOST -P $FIRST_EP_PORT -s 'create dnode \"$FQDN:$SERVER_PORT\";'"
            break
        fi
        sleep 1s
    done
fi

if [ "$DISABLE_ADAPTER" = "0" ]; then
    if which taosadapter >/dev/null; then
        taosadapter &
        PID_TAOSADAPTER=$!
    fi
    # wait for 6041 port ready
    for _ in $(seq 1 20); do
        check_children_alive
        curl -sf http://localhost:6041/metrics && break
        sleep 0.5
    done
fi

if [ "$DISABLE_KEEPER" = "0" ]; then
    sleep 3
    if which taoskeeper >/dev/null; then
        taoskeeper &
        PID_TAOSKEEPER=$!
    fi
    # wait for 6043 port ready
    for _ in $(seq 1 20); do
        check_children_alive
        curl -sf http://localhost:6043/metrics && break
        sleep 0.5
    done
fi

if [ "$DISABLE_EXPLORER" = "0" ]; then
    if which taos-explorer >/dev/null; then
        taos-explorer &
        PID_TAOS_EXPLORER=$!
    fi
    # wait for 6060 port ready
    for _ in $(seq 1 20); do
        check_children_alive
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

# Kill the children we started, using the PIDs we recorded with '$!'.
# NOTE: 'pkill'/'pgrep' are NOT available in this image (no procps package),
# so the previous "pkill -P $$ || true" was silently a no-op.
cleanup_children() {
    for pid in "$PID_TAOSD" "$PID_TAOSADAPTER" "$PID_TAOSKEEPER" "$PID_TAOS_EXPLORER"; do
        if [ -n "$pid" ] && [ "$pid" -ne 0 ]; then
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done
}

# After signalling, wait for the children to finish shutting down instead of
# exiting straight away: once the entrypoint returns, tini exits and the kernel
# SIGKILLs everything left in the container, so taosd would never get a chance
# to flush. docker's stop timeout (--time) still bounds this wait.
trap 'echo "Received stop signal, shutting down children"; cleanup_children; wait; exit 0' SIGINT SIGTERM
# Monitor child processes: exit as soon as any of them dies so that the docker
# --restart policy can bring the container back up. A bare `wait` blocks until
# *all* children exit, which is why a crashed taosd left the container running
# but unusable (community PR #30014).
WAIT_PIDS=()
[ "$PID_TAOSD" -ne 0 ] && WAIT_PIDS+=("$PID_TAOSD")
[ "$PID_TAOSADAPTER" -ne 0 ] && WAIT_PIDS+=("$PID_TAOSADAPTER")
[ "$PID_TAOSKEEPER" -ne 0 ] && WAIT_PIDS+=("$PID_TAOSKEEPER")
[ "$PID_TAOS_EXPLORER" -ne 0 ] && WAIT_PIDS+=("$PID_TAOS_EXPLORER")

if [ ${#WAIT_PIDS[@]} -gt 0 ]; then
    set +e
    wait -n "${WAIT_PIDS[@]}"
    child_rc=$?
    set -e
    echo "one of the child processes exited (rc=${child_rc}), stopping container"
    exit "$child_rc"
else
    wait
fi
