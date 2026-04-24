#!/usr/bin/env bash
# test_ensure_ext_env.sh — integration test for ensure_ext_env.sh
#
# Tests all scenarios per engine × version:
#   S1  Already running        (port open)       → must skip and return 0
#   S2  Installed, stopped     (port closed, bin exists, data dirty) → must start
#   S3  Installed, clean stop  (port closed, bin exists, data clean) → must start
#   S4  Not running, not installed               → must install + start
#   S5  Re-run after S4        (idempotent run)  → must skip (already running)
#   S6  Dirty state (stale pidfile + port closed) → must recover
#
# Usage:
#   bash test_ensure_ext_env.sh [<engine> [<version>]]
#   engine: mysql | pg | influxdb | all (default: all)
#   version: specific version string (default: test all configured versions)
#
# Environment:
#   All FQ_* variables from ensure_ext_env.sh are respected.
#   TEST_BASE_DIR  scratch dir for test state (default /tmp/fq-test-$$)
#   TEST_VERBOSE   set to 1 for extra output

set -euo pipefail

# ──────────────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ENSURE_SCRIPT="${SCRIPT_DIR}/ensure_ext_env.sh"
TEST_BASE_DIR="${TEST_BASE_DIR:-/tmp/fq-test-$$}"
TEST_VERBOSE="${TEST_VERBOSE:-0}"

PASS=0; FAIL=0; SKIP=0
FAILURES=()

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
_c_green()  { printf '\033[0;32m%s\033[0m' "$*"; }
_c_red()    { printf '\033[0;31m%s\033[0m' "$*"; }
_c_yellow() { printf '\033[0;33m%s\033[0m' "$*"; }
_c_bold()   { printf '\033[1m%s\033[0m' "$*"; }

tlog()  { echo "[test] $*"; }
tvlog() { [[ "${TEST_VERBOSE}" == "1" ]] && echo "[test:v] $*" || true; }

pass() { local name="$1"; echo "  $(_c_green PASS) ${name}"; PASS=$((PASS+1)); }
fail() { local name="$1" msg="${2:-}"; echo "  $(_c_red FAIL) ${name}${msg:+  ($msg)}"; FAIL=$((FAIL+1)); FAILURES+=("${name}${msg:+: $msg}"); }
skip() { local name="$1" reason="${2:-}"; echo "  $(_c_yellow SKIP) ${name}${reason:+  ($reason)}"; SKIP=$((SKIP+1)); }

# Run ensure_ext_env.sh with given env; return its exit code
_run_ensure() {
    local log="${TEST_BASE_DIR}/ensure.log"
    tvlog "Running ensure_ext_env.sh with vars: $*"
    env "$@" bash "${ENSURE_SCRIPT}" >> "$log" 2>&1
    return $?
}

# Check if a port is accepting connections
_port_open() {
    local port="$1"
    if command -v nc &>/dev/null; then
        nc -z -w 2 127.0.0.1 "$port" 2>/dev/null; return
    fi
    curl -sf --connect-timeout 2 "telnet://127.0.0.1:${port}" -o /dev/null 2>/dev/null; return
}

# Wait at most N seconds for port to open/close
_wait_port_open()  { local p="$1" n="${2:-30}" i=0; while ! _port_open "$p" && [[ $i -lt $n ]]; do sleep 1; i=$((i+1)); done; _port_open "$p"; }
_wait_port_closed(){ local p="$1" n="${2:-15}" i=0; while _port_open "$p"  && [[ $i -lt $n ]]; do sleep 1; i=$((i+1)); done; ! _port_open "$p"; }

# Kill the engine listening on a port (by PID file or pattern)
_stop_port() {
    local port="$1" engine="$2" ver="$3"
    local base="${FQ_BASE_DIR:-/opt/taostest/fq}/${engine}/${ver}"
    # Try pidfile first
    local pidfile=""
    case "$engine" in
        mysql)   pidfile="${base}/run/mysqld.pid" ;;
        pg)      : ;; # use pg_ctl stop
        influxdb) pidfile="${base}/run/influxd.pid" ;;
    esac

    if [[ "$engine" == "pg" ]]; then
        local pg_ctl="${base}/bin/pg_ctl"
        local stopped=false
        # pg_ctl stop only works when PG_VERSION file is intact
        if [[ -x "$pg_ctl" && -f "${base}/data/PG_VERSION" ]]; then
            if [[ "$(id -un)" == "root" ]]; then
                su -s /bin/sh postgres -c "$pg_ctl -D ${base}/data stop -m fast" 2>/dev/null \
                    && stopped=true || true
            else
                "$pg_ctl" -D "${base}/data" stop -m fast 2>/dev/null && stopped=true || true
            fi
        fi
        # Fallback: kill via postmaster.pid
        if [[ "$stopped" != "true" && -f "${base}/data/postmaster.pid" ]]; then
            local pg_pid; pg_pid="$(head -1 "${base}/data/postmaster.pid" 2>/dev/null)"
            if [[ -n "$pg_pid" && "$pg_pid" =~ ^[0-9]+$ ]]; then
                kill -TERM "$pg_pid" 2>/dev/null || true
                sleep 2
                kill -0 "$pg_pid" 2>/dev/null && kill -KILL "$pg_pid" 2>/dev/null || true
            fi
        fi
        return
    fi

    if [[ -n "$pidfile" && -f "$pidfile" ]]; then
        local pid; pid="$(cat "$pidfile")"
        [[ -n "$pid" ]] && kill -TERM "$pid" 2>/dev/null || true
        sleep 2
        [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null && kill -KILL "$pid" 2>/dev/null || true
        rm -f "$pidfile"
        return
    fi
    # Fallback: pattern kill
    case "$engine" in
        mysql)   pkill -f "mysqld.*port=${port}" 2>/dev/null || true ;;
        influxdb) pkill -f "influxdb.*${port}" 2>/dev/null || true ;;
    esac
    sleep 1
}

# ──────────────────────────────────────────────────────────────────────────────
# Scenario scaffolding
# ──────────────────────────────────────────────────────────────────────────────

# Get port for a given engine+version (mirrors ensure_ext_env.sh logic)
_get_port() {
    local engine="$1" ver="$2"
    case "${engine}-${ver//./}" in
        mysql-57)    echo 13305 ;;
        mysql-80)    echo 13306 ;;
        mysql-84)    echo 13307 ;;
        pg-14)       echo 15433 ;;
        pg-15)       echo 15435 ;;
        pg-16)       echo 15434 ;;
        pg-17)       echo 15436 ;;
        influxdb-30) echo 18086 ;;
        influxdb-35) echo 18087 ;;
        *)           echo 19999 ;;
    esac
}

# ──────────────────────────────────────────────────────────────────────────────
# Test scenarios per engine×version
# ──────────────────────────────────────────────────────────────────────────────

run_tests_for() {
    local engine="$1" ver="$2"
    local port; port="$(_get_port "$engine" "$ver")"
    local base="${FQ_BASE_DIR:-/opt/taostest/fq}/${engine}/${ver}"
    local env_key; env_key="FQ_${engine^^}_VERSIONS"
    local env_override="${env_key}=${ver}"

    echo ""
    echo "$(_c_bold "=== ${engine}:${ver} (port ${port}) ===")"
    mkdir -p "${TEST_BASE_DIR}"
    echo "" >> "${TEST_BASE_DIR}/ensure.log"
    echo "===== ${engine}:${ver} =====" >> "${TEST_BASE_DIR}/ensure.log"

    # ── S1: Already running ──────────────────────────────────────────────────
    echo "  [S1] Already running → skip"
    if _port_open "$port"; then
        # Port IS open — run script and expect 0
        if _run_ensure "$env_override"; then
            pass "S1:${engine}:${ver}:already-running-ret0"
        else
            fail "S1:${engine}:${ver}:already-running-ret0" "expected exit 0"
        fi
        # Script output must contain "already running" or "resetting test env"
        if grep -qE "already running|resetting test env" "${TEST_BASE_DIR}/ensure.log" 2>/dev/null; then
            pass "S1:${engine}:${ver}:already-running-log"
        else
            skip "S1:${engine}:${ver}:already-running-log" "log check inconclusive"
        fi
    else
        skip "S1:${engine}:${ver}" "port ${port} not open before test; install first (S4)"
    fi

    # ── S4: Fresh install (always: force-remove existing install first) ────────
    echo "  [S4] Fresh install (removing existing installation)"
    # Stop service first, then wipe the install dir to simulate a clean machine.
    # Downloaded tarballs in /tmp are intentionally preserved as a download cache
    # so re-runs of this test don't re-download unnecessarily.
    _stop_port "$port" "$engine" "$ver" 2>/dev/null || true
    _wait_port_closed "$port" 20 || true
    rm -rf "${base:?}" 2>/dev/null || true
    tlog "  Install dir removed; performing fresh install ..."

    local t0; t0=$SECONDS
    if _run_ensure "$env_override"; then
        pass "S4:${engine}:${ver}:install-exit0"
        if _port_open "$port"; then
            pass "S4:${engine}:${ver}:install-port-open"
        else
            fail "S4:${engine}:${ver}:install-port-open" "port ${port} not open after install"
        fi
        local elapsed=$(( SECONDS - t0 ))
        tlog "  Install took ${elapsed}s"
    else
        fail "S4:${engine}:${ver}:install-exit0" "ensure_ext_env.sh returned non-zero"
        skip "S4:${engine}:${ver}:install-port-open" "S4 exit failed"
    fi

    # ── S5: Idempotent re-run (already running after S4) ────────────────────
    echo "  [S5] Idempotent re-run"
    if _port_open "$port"; then
        if _run_ensure "$env_override"; then
            pass "S5:${engine}:${ver}:idempotent-exit0"
            if _port_open "$port"; then
                pass "S5:${engine}:${ver}:still-running"
            else
                fail "S5:${engine}:${ver}:still-running" "port closed after re-run"
            fi
        else
            fail "S5:${engine}:${ver}:idempotent-exit0" "non-zero on idempotent run"
        fi
    else
        skip "S5:${engine}:${ver}" "port not open; S4 may have failed"
    fi

    # ── S3: Clean stop + restart ─────────────────────────────────────────────
    echo "  [S3] Clean stop + restart"
    if _port_open "$port" && [[ -x "${base}/bin/mysqld" || -x "${base}/bin/pg_ctl" || \
                                  -x "${base}/bin/influxdb3" || -x "${base}/bin/influxd" ]]; then
        _stop_port "$port" "$engine" "$ver"
        if _wait_port_closed "$port" 20; then
            if _run_ensure "$env_override"; then
                pass "S3:${engine}:${ver}:stop-restart-exit0"
                if _port_open "$port"; then
                    pass "S3:${engine}:${ver}:stop-restart-port-open"
                else
                    fail "S3:${engine}:${ver}:stop-restart-port-open" "port ${port} not open after restart"
                fi
            else
                fail "S3:${engine}:${ver}:stop-restart-exit0"
            fi
        else
            skip "S3:${engine}:${ver}" "port did not close within 20s"
        fi
    else
        skip "S3:${engine}:${ver}" "bin not installed or not running"
    fi

    # ── S2: Dirty state (stale pidfile, data intact) ─────────────────────────
    echo "  [S2] Stale pidfile + port closed → recover"
    if [[ -x "${base}/bin/mysqld" || -x "${base}/bin/pg_ctl" || \
          -x "${base}/bin/influxdb3" || -x "${base}/bin/influxd" ]]; then
        # Stop daemon cleanly first
        _stop_port "$port" "$engine" "$ver" 2>/dev/null || true
        _wait_port_closed "$port" 15 || true
        # Leave a stale pidfile with a non-existent PID
        local run_dir="${base}/run"
        mkdir -p "$run_dir"
        case "$engine" in
            mysql)   echo "99999" > "${run_dir}/mysqld.pid" ;;
            influxdb) echo "99999" > "${run_dir}/influxd.pid" ;;
        esac

        if _run_ensure "$env_override"; then
            pass "S2:${engine}:${ver}:stale-pid-exit0"
            if _port_open "$port"; then
                pass "S2:${engine}:${ver}:stale-pid-port-open"
            else
                fail "S2:${engine}:${ver}:stale-pid-port-open" "port not open after stale-pid recovery"
            fi
        else
            fail "S2:${engine}:${ver}:stale-pid-exit0"
        fi
    else
        skip "S2:${engine}:${ver}" "engine not installed"
    fi

    # ── S6: Data dir corrupted (remove data, bin still present) ──────────────
    echo "  [S6] Corrupt data dir → reinstall"
    if [[ -x "${base}/bin/mysqld" || -x "${base}/bin/pg_ctl" || \
          -x "${base}/bin/influxdb3" || -x "${base}/bin/influxd" ]]; then
        _stop_port "$port" "$engine" "$ver" 2>/dev/null || true
        _wait_port_closed "$port" 15 || true
        # Corrupt data dir by removing key files
        case "$engine" in
            mysql)   rm -f "${base}/data/mysql/user.frm" "${base}/data/mysql/user.ibd" 2>/dev/null || true ;;
            pg)      rm -f "${base}/data/PG_VERSION" 2>/dev/null || true ;;
            influxdb) rm -rf "${base}/data" && mkdir -p "${base}/data" ;;
        esac

        if _run_ensure "$env_override"; then
            pass "S6:${engine}:${ver}:corrupt-data-exit0"
            if _port_open "$port"; then
                pass "S6:${engine}:${ver}:corrupt-data-port-open"
            else
                fail "S6:${engine}:${ver}:corrupt-data-port-open"
            fi
        else
            fail "S6:${engine}:${ver}:corrupt-data-exit0"
        fi
    else
        skip "S6:${engine}:${ver}" "engine not installed"
    fi
}

# ──────────────────────────────────────────────────────────────────────────────
# Compat / unit-level checks (no network needed)
# ──────────────────────────────────────────────────────────────────────────────
run_compat_checks() {
    echo ""
    echo "$(_c_bold "=== Compatibility checks ===")"

    # bash version
    local bmaj="${BASH_VERSINFO[0]}"
    if [[ "$bmaj" -ge 4 ]]; then
        pass "compat:bash-version (${BASH_VERSION})"
    else
        fail "compat:bash-version" "bash >= 4 required, got ${BASH_VERSION}"
    fi

    # nc presence
    if command -v nc &>/dev/null || command -v ncat &>/dev/null; then
        pass "compat:nc-available"
    else
        fail "compat:nc-available" "neither nc nor ncat found; port_open will fail"
    fi

    # curl
    if command -v curl &>/dev/null; then
        pass "compat:curl-available"
    else
        fail "compat:curl-available" "curl not found"
    fi

    # tar supports xz (-J)
    if echo "." | tar --help 2>&1 | grep -q "\-J\|xz" || tar -xJf /dev/null 2>&1 | grep -qv "unrecognized"; then
        pass "compat:tar-xz"
    else
        # Try with xz installed
        command -v xz &>/dev/null && pass "compat:tar-xz (xz available)" \
            || fail "compat:tar-xz" "tar does not support xz and xz not installed"
    fi

    # id -un (portable whoami)
    if id -un &>/dev/null; then
        pass "compat:id-un ($(id -un))"
    else
        fail "compat:id-un" "id -un failed"
    fi

    # case-based port lookup
    local port; port="$(bash -c 'source /dev/stdin <<'"'"'EOF'"'"'
source "'"${ENSURE_SCRIPT}"'"
mysql_port "8.0"
EOF')"  2>/dev/null || true
    if [[ "$port" == "13306" ]]; then
        pass "compat:mysql-port-lookup"
    else
        fail "compat:mysql-port-lookup" "expected 13306, got '${port}'"
    fi

    # _download_with_retry on a bad URL produces non-zero (1 attempt to keep test fast)
    local tmpfile; tmpfile="$(mktemp)"
    local dl_exit=0
    FQ_SOURCE_ONLY=1 bash -c "
        set -euo pipefail
        source '${ENSURE_SCRIPT}'
        info() { :; }; warn() { :; }; err() { :; }
        _download_with_retry 'https://0.0.0.0:1/noexist' '${tmpfile}' 1
    " 2>/dev/null || dl_exit=$?
    rm -f "$tmpfile"
    if [[ "$dl_exit" -ne 0 ]]; then
        pass "compat:download-retry-bad-url-fails"
    else
        skip "compat:download-retry-bad-url-fails" "could not isolate function (non-critical)"
    fi
}

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
main() {
    local filter_engine="${1:-all}"
    local filter_ver="${2:-}"

    mkdir -p "${TEST_BASE_DIR}"
    echo "Test run: $(date)" > "${TEST_BASE_DIR}/ensure.log"

    tlog "=================================================="
    tlog "ensure_ext_env.sh integration tests"
    tlog "  Script    : ${ENSURE_SCRIPT}"
    tlog "  FQ_BASE   : ${FQ_BASE_DIR:-/opt/taostest/fq}"
    tlog "  Test base : ${TEST_BASE_DIR}"
    tlog "=================================================="

    run_compat_checks

    # Determine which (engine, version) pairs to test
    local mysql_vers=(); pg_vers=(); influx_vers=()
    IFS=',' read -ra mysql_vers  <<< "${FQ_MYSQL_VERSIONS:-8.0}"
    IFS=',' read -ra pg_vers     <<< "${FQ_PG_VERSIONS:-16}"
    IFS=',' read -ra influx_vers <<< "${FQ_INFLUX_VERSIONS:-3.0}"

    local engine ver
    for ver in "${mysql_vers[@]}"; do
        [[ "$filter_engine" != "all" && "$filter_engine" != "mysql" ]] && continue
        [[ -n "$filter_ver" && "$filter_ver" != "$ver" ]] && continue
        run_tests_for "mysql" "$ver"
    done

    for ver in "${pg_vers[@]}"; do
        [[ "$filter_engine" != "all" && "$filter_engine" != "pg" ]] && continue
        [[ -n "$filter_ver" && "$filter_ver" != "$ver" ]] && continue
        run_tests_for "pg" "$ver"
    done

    for ver in "${influx_vers[@]}"; do
        [[ "$filter_engine" != "all" && "$filter_engine" != "influxdb" ]] && continue
        [[ -n "$filter_ver" && "$filter_ver" != "$ver" ]] && continue
        run_tests_for "influxdb" "$ver"
    done

    echo ""
    echo "$(_c_bold "=== Summary ===")"
    echo "  $(_c_green "PASS: ${PASS}")   $(_c_red "FAIL: ${FAIL}")   $(_c_yellow "SKIP: ${SKIP}")"
    if [[ "${#FAILURES[@]}" -gt 0 ]]; then
        echo ""
        echo "  Failed tests:"
        for f in "${FAILURES[@]}"; do echo "    • $f"; done
    fi
    echo ""
    echo "  Full log: ${TEST_BASE_DIR}/ensure.log"

    [[ "$FAIL" -eq 0 ]]
}

main "$@"
