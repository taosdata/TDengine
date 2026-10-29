#!/usr/bin/env bash

# Copyright (c) TAOS Data, Inc. All rights reserved.
#
# SPDX-License-Identifier: LicenseRef-TAOSData-Commercial
#
# This file is proprietary to TAOS Data, Inc. and may contain confidential
# information. No rights are granted by this file. Use is permitted only
# under a written agreement with TAOS Data, Inc. or an authorized affiliate.
# Unauthorized copying, modification, distribution, or disclosure, in whole
# or in part, by any medium, is strictly prohibited.

set -euo pipefail

# ============================================
# Function Definitions
# ============================================
log_info() { echo -e "$(date '+%Y-%m-%d %H:%M:%S') [INFO] $1"; }

PIDS=""
SHUTDOWN_TIMEOUT=${TAOS_SHUTDOWN_TIMEOUT:-8}

cleanup_children() {
    local alive pid

    for pid in $PIDS; do
        kill -TERM "$pid" 2>/dev/null || true
    done
    pkill -TERM -P $$ 2>/dev/null || true

    for _ in $(seq 1 "$SHUTDOWN_TIMEOUT"); do
        alive=0
        for pid in $PIDS; do
            if kill -0 "$pid" 2>/dev/null; then
                alive=1
                break
            fi
        done
        [ "$alive" -eq 0 ] && return 0
        sleep 1
    done

    log_info "Child processes did not stop within ${SHUTDOWN_TIMEOUT}s; forcing shutdown"
    for pid in $PIDS; do
        kill -KILL "$pid" 2>/dev/null || true
    done
    pkill -KILL -P $$ 2>/dev/null || true
    return 0
}

check_managed_processes() {
    local pid process_state proc_root=${1:-/proc}
    for pid in $PIDS; do
        # kill -0 also succeeds for zombies, so inspect procfs before treating
        # a managed service as healthy. A missing proc entry is checked below.
        process_state=$(awk '$1 == "State:" { print $2; exit }' \
            "$proc_root/$pid/status" 2>/dev/null) || process_state=""
        if [ "$process_state" = "Z" ]; then
            log_info "❌ Managed child process $pid is a zombie"
            return 1
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            log_info "❌ Managed child process $pid is not running"
            return 1
        fi
    done
    return 0
}

cleanup_on_exit() {
    local rc=$?
    set +e
    if [ "$rc" -ne 0 ]; then
        log_info "Entrypoint exiting abnormally (rc=$rc), killing children"
        cleanup_children
    fi
    trap - EXIT
    exit "$rc"
}

trap cleanup_on_exit EXIT
trap 'log_info "Shutting down..."; cleanup_children; exit 0' SIGINT SIGTERM

init_env_vars() {
    if [ -n "${TZ:-}" ]; then
        ln -sf "/usr/share/zoneinfo/$TZ" /etc/localtime
        echo "$TZ" > /etc/timezone
    fi

    local taos_pwd_file taos_pwd
    taos_pwd_file="${TAOS_ROOT_PASSWORD_FILE:-}"
    if [ -n "$taos_pwd_file" ] && [ -f "$taos_pwd_file" ]; then
        taos_pwd="$(cat "$taos_pwd_file")"
        export TAOS_ROOT_PASSWORD="$taos_pwd"
    else
        export TAOS_ROOT_PASSWORD="${TAOS_ROOT_PASSWORD:-taosdata}"
    fi
    export TAOS_KEEPER_TDENGINE_PASSWORD="${TAOS_KEEPER_TDENGINE_PASSWORD:-${TAOS_ROOT_PASSWORD}}"
    export INITDB_DIR=/docker-entrypoint-initdb.d/
    export DISABLE_TAOSD=${TAOS_DISABLE_TAOSD:-0}
    export DISABLE_ADAPTER=${TAOS_DISABLE_ADAPTER:-0}
    export DISABLE_KEEPER=${TAOS_DISABLE_KEEPER:-0}
    export DISABLE_EXPLORER=${TAOS_DISABLE_EXPLORER:-0}
    export DISABLE_TAOSX=${TAOS_DISABLE_TAOSX:-0}
    export TAOS_DISABLE_MNODE=${TAOS_DISABLE_MNODE:-0}
    unset TAOS_DISABLE_ADAPTER TAOS_DISABLE_KEEPER TAOS_DISABLE_EXPLORER TAOS_DISABLE_TAOSX TAOS_DISABLE_SERVER
}

read_taosd_config() {
    local taosd_output
    set +e
    taosd_output="$(taosd -C 2>&1)"
    local taosd_rc=$?
    set -e

    if [ $taosd_rc -ne 0 ]; then
        log_info "❌ taosd -C failed"
        echo "$taosd_output" >&2
        if echo "$taosd_output" | grep -Eqi "Operation not permitted|failed to init log file|failed to create log|init log error"; then
            log_info "💡 Detected permission-related runtime issue."
            log_info "   Please try running container with: --privileged=true"
            log_info "   Example: docker run --privileged=true ..."
        fi
        exit 1
    fi

    TAOSD_CONFIG="$taosd_output"

    # Extract dataDir
    local data_dir
    data_dir=$(echo "$TAOSD_CONFIG" | grep -oE 'dataDir\s+\S+' | head -n1 | awk '{print $2}')
    export DATA_DIR="${data_dir:-/var/lib/taos}"

    # Extract fqdn
    local fqdn
    fqdn=$(echo "$TAOSD_CONFIG" | grep -oE 'fqdn\s+\S+' | head -n1 | awk '{print $2}')
    export FQDN="$fqdn"
    grep -q "$FQDN" /etc/hosts || echo "127.0.0.1 $FQDN" >> /etc/hosts

    # Extract firstEp
    local first_ep
    first_ep=$(echo "$TAOSD_CONFIG" | grep -oE 'firstEp\s+\S+' | head -n1 | awk '{print $2}')
    export FIRST_EP_HOST="${first_ep%:*}"
    export FIRST_EP_PORT="${first_ep#*:}"

    # Extract serverPort
    local server_port
    server_port=$(echo "$TAOSD_CONFIG" | grep -oE 'serverPort\s+\S+' | head -n1 | awk '{print $2}')
    export SERVER_PORT="${server_port:-6030}"
}

detect_node_role() {
    export IS_MASTER=0
    if [ "$FQDN" = "$FIRST_EP_HOST" ]; then
        export IS_MASTER=1
    fi
    export IS_SINGLE_NODE=0
    if [ -z "${TAOS_FIRST_EP:-}" ]; then
        if [ "$FQDN" = "$FIRST_EP_HOST" ]; then
            export IS_SINGLE_NODE=1
        else
            log_info "❌ ERROR: Configuration mismatch detected"
            log_info "   FQDN: $FQDN"
            log_info "   firstEp: $FIRST_EP_HOST"
            log_info "   TAOS_FIRST_EP: not set"
            log_info ""
            log_info "This node cannot determine its role in the cluster."
            log_info "Please set TAOS_FIRST_EP environment variable to specify the cluster first entry point."
            exit 1
        fi
    fi
}

detect_initialization_status() {
    # Check if already initialized via data files (similar to MySQL checking datadir)
    export IS_INITIALIZED=0
    if [ -f "$DATA_DIR/dnode/dnode.json" ] || [ -f "$DATA_DIR/dnode/mnodeEpSet.json" ]; then
        export IS_INITIALIZED=1
    fi
}

core_dump_config() {
    # Enable unlimited core dump size
    ulimit -c unlimited 2>/dev/null || true
}

show_startup_info() {
    log_info "=========================================="
    log_info "TDengine Container Startup"
    log_info "=========================================="
    log_info "  FQDN: $FQDN"
    log_info "  First EP: $FIRST_EP_HOST:$FIRST_EP_PORT"
    log_info "  Data Dir: $DATA_DIR"
    log_info "  Role: $([ $IS_MASTER -eq 1 ] && echo 'Master' || echo 'Secondary')"
    log_info "  Initialized: $([ $IS_INITIALIZED -eq 1 ] && echo 'Yes' || echo 'No')"
    log_info "  Mode: $([ $IS_SINGLE_NODE -eq 1 ] && echo 'Single Node' || echo 'Cluster')"
    log_info "  taosd: $([ "${DISABLE_TAOSD}" = "0" ] && echo 'taosd Enabled' || echo 'taosd Disabled')"
    log_info "  taosadapter: $([ "${DISABLE_ADAPTER}" = "0" ] && echo 'taosadapter Enabled' || echo 'taosadapter Disabled')"
    log_info "  taoskeeper: $([ "${DISABLE_KEEPER}" = "0" ] && echo 'taoskeeper Enabled' || echo 'taoskeeper Disabled')"
    log_info "  taos-explorer: $([ "${DISABLE_EXPLORER}" = "0" ] && echo 'taos-explorer Enabled' || echo 'taos-explorer Disabled')" 
    log_info "  taosx: $([ "${DISABLE_TAOSX}" = "0" ] && command -v taosx >/dev/null 2>&1 && echo 'taosx Enabled' || echo 'taosx Disabled')"    
    log_info "=========================================="
}

support_custom_commands() {
    if [ $# -gt 0 ]; then
        exec "$@"
    fi
}

check_taosd_process() {
    pgrep -x taosd >/dev/null 2>&1
}

wait_service_ready() {
    local host=${1:-$FIRST_EP_HOST}
    local port=${2:-$FIRST_EP_PORT}
    local max_wait=${3:-120}
    local password=${4:-"taosdata"}
    log_info "Waiting for TDengine at $host:$port (max ${max_wait}s)..."
    for i in $(seq 1 "$max_wait"); do
        check_managed_processes || return 1
        set +e
        local status
        status=$(taos -h "$host" -P "$port" -p"$password"  --check 2>&1 | grep -oE '^[0-9]+:' | cut -d: -f1)
        set -e
        log_info "  Service check returned status: $status"
        if [ "$status" = "2" ]; then
            log_info "✓ Service ready"
            return 0
        fi
        [ $((i % 10)) -eq 0 ] && log_info "  Still waiting... (${i}s)"
        sleep 1
    done
    log_info "❌ Timeout waiting for service"
    return 1
}

# Responsibility 1: Change root password during initialization (first-time only)
# Check if already initialized via data files, similar to MySQL/PostgreSQL behavior
change_root_password() {
    if [ "${IS_INITIALIZED}" -eq 1 ]; then
        log_info "ℹ️  Already initialized, skip password change"
        return 0
    fi
    if [ "${TAOS_ROOT_PASSWORD}" = "taosdata" ]; then
        log_info "ℹ️  Using default root password"
        return 0
    fi
    log_info "→ Setting initial root password..."
    set +e
    taos -p"${TAOS_ROOT_PASSWORD}" -s 'SELECT 1' >/dev/null 2>&1
    local check_new=$?
    set -e
    if [ $check_new -eq 0 ]; then
        log_info "✓ Desired password already active"
        return 0
    fi
    set +e
    taos -ptaosdata -s 'SELECT 1' >/dev/null 2>&1
    local check_default=$?
    set -e
    if [ $check_default -eq 0 ]; then
        # Escape single quotes to prevent SQL injection
        local password_escaped="${TAOS_ROOT_PASSWORD//\'/\'\\\'\'}"
        set +e
        taos -ptaosdata -s "ALTER USER root PASS '${password_escaped}'" >/dev/null 2>&1
        local alter_result=$?
        set -e
        if [ $alter_result -eq 0 ]; then
            log_info "✓ Root password initialized"
            return 0
        else
            log_info "❌ Failed to set root password"
            return 1
        fi
    fi
    log_info "⚠️  Cannot verify current password state; leaving unchanged"
    return 0
}

run_init_scripts() {
    local flag_file="${DATA_DIR}/.docker-entrypoint-inited"
    if [ -f "$flag_file" ]; then
        log_info "✓ Init scripts already executed"
        return 0
    fi
    if [ ! -d "$INITDB_DIR" ]; then
        log_info "ℹ️  No init script directory"
        touch "$flag_file"
        return 0
    fi
    log_info "→ Running initialization scripts..."
    local has_files=0
    for file in "$INITDB_DIR"*.sql; do
        [ -f "$file" ] || continue
        has_files=1
        log_info "  Executing: $(basename "$file")"
        local max_retries=5
        local success=0
        for attempt in $(seq 1 $max_retries); do
            set +e
            local output
            output=$(taos -f "$file" -p"${TAOS_ROOT_PASSWORD}" 2>&1  >/dev/null)
            local exit_code=$?
            set -e
            if [ $exit_code -eq 0 ]; then
                log_info "    ✓ Success"
                success=1
                break
            fi
            if echo "$output" | grep -q "DB error"; then
                if [ $attempt -lt $max_retries ]; then
                    log_info "    ⚠️  Retry $attempt/$max_retries"
                    sleep 2
                else
                    log_info "    ❌ Failed after $max_retries attempts"
                    return 1
                fi
            else
                log_info "    ✓ Completed (with warnings)"
                success=1
                break
            fi
        done
        [ $success -eq 0 ] && return 1
    done
    if [ $has_files -eq 1 ]; then
        log_info "✓ Initialization scripts completed"
    else
        log_info "ℹ️  No SQL files found in $INITDB_DIR"
    fi
    touch "$flag_file"
}

create_snode() {
    log_info "→ Creating snode on dnode 1..."
    set +e
    local output
    output=$(taos -p"${TAOS_ROOT_PASSWORD}" -h "$FIRST_EP_HOST" -P "$FIRST_EP_PORT" \
        -s 'CREATE SNODE ON DNODE 1;' 2>&1)
    local exit_code=$?
    set -e
    if [ $exit_code -eq 0 ]; then
        log_info "✓ Snode created"
        return 0
    fi
    if echo "$output" | grep -qi "only one snode\|already exists"; then
        log_info "✓ Snode already exists"
        return 0
    fi
    log_info "⚠️  Snode creation failed (exit code: $exit_code)"
    return 0
}

xnode_exists() {
    local xnode_url="$1"
    local xnodes
    # Compare the complete URL; the CLI's default width truncates long FQDNs.
    if xnodes=$(taos -w 1024 -p"${TAOS_ROOT_PASSWORD}" -h "$FIRST_EP_HOST" -P "$FIRST_EP_PORT" \
        -s "show xnodes where url='$xnode_url';" 2>&1); then
        :
    else
        return 2
    fi
    if printf '%s\n' "$xnodes" | grep -qiF "DB error:"; then
        return 2
    fi
    printf '%s\n' "$xnodes" | awk -F '|' -v target="$xnode_url" '
        NF >= 2 {
            url = $2
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", url)
            if (url == target) found = 1
        }
        END { exit !found }
    '
}

is_retryable_xnode_registration_error() {
    local output="$1"
    printf '%s\n' "$output" | grep -Eqi \
        'DB error:.*\[0x(8000000B|80000019|80000020|80000021|80000023|80000024|80000130|800003D3|800003D5|800003D7|80000903|8000090C|80000911|80000914|80008009|80008016)\]'
}

retry_xnode_registration() {
    local xnode_url="$1"
    local output query_code
    while true; do
        if xnode_exists "$xnode_url"; then
            log_info "✓ Xnode was registered."
            return 0
        else
            query_code=$?
        fi
        if [ "$query_code" -ne 1 ]; then
            sleep 5
            continue
        fi
        output=$(taos -p"${TAOS_ROOT_PASSWORD}" -h "$FIRST_EP_HOST" -P "$FIRST_EP_PORT" \
            -s "create xnode '$xnode_url' user root pass '${TAOS_ROOT_PASSWORD}';" 2>&1) || true
        log_info "Background xnode registration returned: $output"
        if printf '%s\n' "$output" | grep -qiF "DB error:"; then
            if is_retryable_xnode_registration_error "$output"; then
                log_info "⚠️  Xnode registration hit a temporary SQL error; background retry will continue."
            else
                log_info "❌ Xnode registration returned a non-retryable SQL error; background retry stopped."
                return 1
            fi
        fi
        sleep 5
    done
}

create_xnode() {
    local xnode_url="$FQDN:6055"
    local output attempt query_code
    if xnode_exists "$xnode_url"; then
        log_info "✓ Xnode already exists."
        return 0
    else
        query_code=$?
    fi
    log_info "→ Creating xnode for taosx with root taosdata."
    for attempt in $(seq 1 5); do
        output=$(taos -p"${TAOS_ROOT_PASSWORD}" -h "$FIRST_EP_HOST" -P "$FIRST_EP_PORT" \
            -s "create xnode '$xnode_url' user root pass '${TAOS_ROOT_PASSWORD}';" 2>&1) || true
        log_info "Xnode creation attempt $attempt/5 returned: $output"
        if xnode_exists "$xnode_url"; then
            log_info "✓ Xnode was created."
            return 0
        else
            query_code=$?
        fi
        if [ "$attempt" -lt 5 ]; then
            sleep 1
        fi
    done
    if [ "$query_code" -eq 2 ]; then
        log_info "⚠️  TDengine is temporarily unavailable; xnode registration will continue in the background."
        retry_xnode_registration "$xnode_url" &
        return 0
    fi
    log_info "❌ Xnode creation failed after 5 attempts."
    return 1
}

join_cluster() {
    log_info "→ Joining cluster as secondary node..."
    retry_times=24
    for i in $(seq 1 ${retry_times}); do
        set +e
        output=$(taos -p"${TAOS_ROOT_PASSWORD}" -h "$FIRST_EP_HOST" -P "$FIRST_EP_PORT" \
            -s "CREATE DNODE '$FQDN:$SERVER_PORT';" 2>&1)
        local exit_code=$?
        log_info "join cluster output:${output}, exit:$exit_code"
        set -e
        if [ $exit_code -eq 0 ]; then
            log_info "✓ Dnode created,successfully joined cluster"
            return 0
        fi
        if echo "$output" | grep -qi "already exists\|duplicate"; then
            log_info "✓ Already in cluster"
            return 0
        fi
        log_info "  Retry joining cluster... ($i/${retry_times})"
        sleep 5
    done
    log_info "❌ Failed to join cluster (exit code: $exit_code)"
    return 1
}

start_components() {
    log_info "→ Starting components..."
    if [ "$DISABLE_ADAPTER" = "0" ] && command -v taosadapter >/dev/null 2>&1; then
        log_info "  Starting taosadapter..."
        taosadapter &
        TAOSADAPTER_PID=$!
        PIDS="$PIDS $TAOSADAPTER_PID"
        
        for i in $(seq 1 20); do
            check_managed_processes || exit 1
            if curl -sf http://localhost:6041/metrics >/dev/null 2>&1; then
                log_info "  ✓ taosadapter ready"
                break
            fi
            sleep 0.5
        done
    fi
    if [ "$DISABLE_KEEPER" = "0" ] && command -v taoskeeper >/dev/null 2>&1; then
        sleep 3
        log_info "  Starting taoskeeper..."
        taoskeeper &
        TAOSKEEPER_PID=$!
        PIDS="$PIDS $TAOSKEEPER_PID"
        # sleep longer to allow taoskeeper to initialize, then check success
        sleep 2
        for i in $(seq 1 60); do
            check_managed_processes || exit 1
            if curl -sf http://localhost:6043/metrics >/dev/null 2>&1; then
                log_info "  ✓ taoskeeper ready"
                break
            fi
            sleep 0.5
        done
    fi
    if [ "$DISABLE_TAOSX" = "0" ] && command -v taosx >/dev/null 2>&1; then
        log_info "  Starting taosx..."
        taosx serve &
        TAOSX_PID=$!
        PIDS="$PIDS $TAOSX_PID"
        for i in $(seq 1 20); do
            check_managed_processes || exit 1
            nc -z localhost 6055 && break
            sleep 0.5
        done
        # 3.3.6 includes taosX but predates SQL xnode registration.
        # Inspect the installed daemon, not the image tag or package filename.
        local daemon_version
        daemon_version=$(taosd -V)
        if printf '%s\n' "$daemon_version" | grep -Eq 'version:[[:space:]]+3\.3\.6\.'; then
            log_info "Skipping xnode registration for TDengine 3.3.6"
        else
            create_xnode
        fi
    fi
    if [ "$DISABLE_EXPLORER" = "0" ] && command -v taos-explorer >/dev/null 2>&1; then
        log_info "  Starting taos-explorer..."
        taos-explorer &
        TAOSEXPLORER_PID=$!
        PIDS="$PIDS $TAOSEXPLORER_PID"
        for i in $(seq 1 20); do
            check_managed_processes || exit 1
            if curl -sf http://localhost:6060/metrics >/dev/null 2>&1; then
                log_info "  ✓ taos-explorer ready"
                break
            fi
            sleep 0.5
        done
    fi
    log_info "✓ Components started"

}

set_taoskeeper_password() {
    set +e
    taos -ptaosdata -s 'SELECT 1' >/dev/null 2>&1
    pwd_ok=$?
    set -e

    if [ $pwd_ok -eq 0 ]; then
        log_info "→ Setting taoskeeper password for default..."
        export TAOS_KEEPER_TDENGINE_PASSWORD="taosdata"
    else
        log_info "→ Setting taoskeeper password for custom password..."
        export TAOS_KEEPER_TDENGINE_PASSWORD="${TAOS_ROOT_PASSWORD}"
    fi
}

start_taosd() {
    # ============================================
    # Single Node Mode
    # ============================================
    if [ $IS_SINGLE_NODE -eq 1 ]; then
        log_info "Scenario: Single node mode"
        # First-time startup
        if [ $IS_INITIALIZED -eq 0 ]; then
            log_info "→ First-time initialization"
            taosd &
            TAOSD_PID=$!
            PIDS="$PIDS $TAOSD_PID"
            wait_service_ready "$FQDN" "$SERVER_PORT" || exit 1
            change_root_password || exit 1
            create_snode
            run_init_scripts || exit 1  
        # Restart
        else
            log_info "→ Restarting existing instance"
            taosd &
            TAOSD_PID=$!
            PIDS="$PIDS $TAOSD_PID"
            set_taoskeeper_password
            # Already initialized, don't change password
        fi
    # ============================================
    # Cluster Mode
    # ============================================
    else
        log_info "Scenario: Cluster mode"
        # Master node
        if [ $IS_MASTER -eq 1 ]; then
            log_info "→ Master node"
            if [ $IS_INITIALIZED -eq 0 ]; then
                log_info "  → First-time initialization"
                taosd &
                TAOSD_PID=$!
                PIDS="$PIDS $TAOSD_PID"
                wait_service_ready || exit 1
                change_root_password || exit 1
                create_snode
                run_init_scripts || exit 1
            else
                log_info "  → Restarting"
                taosd &
                TAOSD_PID=$!
                PIDS="$PIDS $TAOSD_PID"
                set_taoskeeper_password
            fi
        # Secondary node
        else
            log_info "→ Secondary node"
            if [ $IS_INITIALIZED -eq 0 ]; then
                log_info "  → First-time initialization"
                # Responsibility 2: Join cluster using master's password
                log_info "    (using master's password to join cluster)"
                taosd &
                TAOSD_PID=$!
                PIDS="$PIDS $TAOSD_PID"
                wait_service_ready "$FIRST_EP_HOST" "$FIRST_EP_PORT" 120 "$TAOS_ROOT_PASSWORD" || exit 1
                join_cluster || exit 1
            else
                log_info "  → Restarting"
                taosd &
                TAOSD_PID=$!
                PIDS="$PIDS $TAOSD_PID"
                set_taoskeeper_password
            fi
        fi
    fi
}


main() {
    init_env_vars
    read_taosd_config
    core_dump_config
    detect_node_role
    detect_initialization_status
    show_startup_info
    support_custom_commands "$@"
    if [ "$DISABLE_TAOSD" = "0" ]; then
        start_taosd
    fi
    start_components
    if [ -z "$PIDS" ]; then
        log_info "❌ No TDengine services were started; exiting"
        exit 1
    fi

    # Every process in PIDS is a long-running service owned by this container.
    # If any service exits, fail the whole container to avoid a partially
    # available state and let the configured restart policy recover it.
    log_info "All services started, watching child processes: $PIDS"
    while true; do
        if ! check_managed_processes; then
            log_info "Shutting container down so Docker can restart it"
            sleep 1
            exit 1
        fi
        sleep 2
    done
}

main "$@"
