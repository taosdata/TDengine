#!/usr/bin/env bash

# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

# ENVIRONMENT VARIABLES (all optional, defaults match federated_query_common.py)
#   FQ_BASE_DIR            install/data root      default /opt/taostest/fq
#   FQ_MYSQL_VERSIONS      comma list             default "8.0"
#   FQ_PG_VERSIONS         comma list             default "16"
#   FQ_INFLUX_VERSIONS     comma list             default "3.0"
#   FQ_MYSQL_MIRROR        base URL for MySQL tarballs
#   FQ_PG_TARBALL_<VV>     full URL for PG prebuilt tarball (fallback if no pkg)
#   FQ_INFLUX_MIRROR       base URL for InfluxDB releases
#   FQ_MYSQL_TARBALL_<VV>  full URL override per MySQL version (VV = 57/80/84)
#   FQ_INFLUX_TARBALL_<VV> full URL override per InfluxDB version (VV = 30/35)
#   FQ_CERT_DIR            cert source dir        default <script_dir>/certs
#   FQ_MYSQL_USER/PASS     credentials            default root / taosdata
#   FQ_PG_USER/PASS        credentials            default postgres / taosdata
#   FQ_PG_TIMEZONE         PostgreSQL server timezone default Asia/Shanghai
#   FQ_INFLUX_TOKEN/ORG    credentials            default test-token / test-org
#   FQ_POOL_TEST_USER      pool-exhaustion test MySQL user   default fq_pool_test
#   FQ_POOL_TEST_PASS      pool-exhaustion test user password default taosdata
#   FQ_POOL_TEST_MAX_CONN  MAX_USER_CONNECTIONS for pool test user  default 1
#
# SERVICE-SELECTION VARIABLES (for in-test restarts; do NOT export in CI setup)
#   FQ_SERVICES_TO_RESET   space-separated subset of "mysql pg influx"
#                          When set, only the listed services are touched.
#                          Default: all three services.
#   FQ_PG_QUICK_RESTART    Set to "1" to skip data-dir wipe and re-init when
#                          resetting PG.  Only starts an already-initialised
#                          instance.  Used by start_pg_instance() so that PG
#                          data survives stop/start test cycles.
#   FQ_INFLUX_QUICK_RESTART Set to "1" to skip the IOx data-dir wipe and
#                          restart InfluxDB in-place (preserving existing data).
#                          Used by start_influx_instance() so that InfluxDB
#                          data survives stop/start test cycles without the
#                          double-restart caused by ensure_influx's hard reset.
#
# EXIT CODES
#   0 = all requested engines ready
#   1 = one or more engines failed

# ──────────────────────────────────────────────────────────────────────────────
# 0.  Bootstrap checks – must run before set -euo pipefail
# ──────────────────────────────────────────────────────────────────────────────

# Windows (including Git-Bash / MSYS2) detection
case "$(uname -s 2>/dev/null)" in
    CYGWIN*|MINGW*|MSYS*)
        echo "[fq-env] FATAL: Windows is not supported. Use WSL2 or Docker." >&2
        exit 1 ;;
esac

# Require bash ≥ 4.0 (needed for associative arrays, $EPOCHSECONDS etc.)
_bash_major="${BASH_VERSINFO[0]:-0}"
if [[ "$_bash_major" -lt 4 ]]; then
    # On macOS the system bash is 3.2; try Homebrew bash if available
    for _try in /usr/local/bin/bash /opt/homebrew/bin/bash; do
        if [[ -x "$_try" ]]; then
            exec "$_try" "$0" "$@"
        fi
    done
    echo "[fq-env] FATAL: bash >= 4.0 required (current: ${BASH_VERSION})." >&2
    echo "[fq-env]        On macOS: brew install bash" >&2
    exit 1
fi

set -euo pipefail

# ──────────────────────────────────────────────────────────────────────────────
# 1.  Globals
# ──────────────────────────────────────────────────────────────────────────────

# Resolve script directory portably (no readlink -f on macOS without coreutils)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

OS="$(uname -s)"          # Linux | Darwin
ARCH="$(uname -m)"        # x86_64 | aarch64 | arm64

FQ_BASE_DIR="${FQ_BASE_DIR:-/opt/taostest/fq}"
CERT_SRC="${FQ_CERT_DIR:-${SCRIPT_DIR}/certs}"
# Tarball cache directory — mount a host path here to avoid re-downloading.
# CI mounts /data0/compat-packages → /usr/local/src; local Ubuntu can use
# /data0/compat-packages directly for MySQL/Influx tarballs (no re-download).
_compat_packages_staged_in() {
    local d="$1"
    [[ -d "$d" ]] || return 1
    [[ -d "${d}/mysql" || -d "${d}/influx" \
        || -f "${d}/fq-mysql-8.0.tar.xz" || -f "${d}/fq-influxdb-3.0.tar.gz" \
        || -f "${d}/mysql-8.0.45-linux-glibc2.28-x86_64.tar.xz" \
        || -f "${d}/influxdb3-core-3.0.3_linux_amd64.tar.gz" ]] && return 0
    compgen -G "${d}/mysql-*.tar.xz" >/dev/null 2>&1 && return 0
    compgen -G "${d}/influxdb3-core-*.tar.gz" >/dev/null 2>&1 && return 0
    compgen -G "${d}/fq-apt-*.tar.gz" >/dev/null 2>&1
}

_default_tarball_cache_dir() {
    local d
    for d in /usr/local/src /data0/compat-packages; do
        if _compat_packages_staged_in "$d"; then
            printf '%s\n' "$d"
            return
        fi
    done
    printf '%s\n' "/tmp"
}

_fq_staging_dirs() {
    local seen="" d
    for d in "${FQ_STAGING_DIR}" /usr/local/src /data0/compat-packages; do
        [[ -n "$d" && -d "$d" ]] || continue
        [[ " $seen " == *" $d "* ]] && continue
        seen+=" $d"
        printf '%s\n' "$d"
    done
}

FQ_TARBALL_CACHE_DIR="${FQ_TARBALL_CACHE_DIR:-$(_default_tarball_cache_dir)}"
FQ_POSTGIS_VERSION="${FQ_POSTGIS_VERSION:-3.6.3}"
FQ_POSTGIS_URL="${FQ_POSTGIS_URL:-https://codeload.github.com/postgis/postgis/tar.gz/refs/tags/${FQ_POSTGIS_VERSION}}"
FQ_POSTGIS_FALLBACK_URL="${FQ_POSTGIS_FALLBACK_URL:-https://download.osgeo.org/postgis/source/postgis-${FQ_POSTGIS_VERSION}.tar.gz}"
# Extra search paths for pre-staged tarballs (container mount or host compat-packages).
FQ_STAGING_DIR="${FQ_STAGING_DIR:-/usr/local/src}"

# Empty FQ_*_VERSIONS="" disables that engine (e.g. FQ_PG_VERSIONS= for local tarball-only runs).
if [[ -n "${FQ_MYSQL_VERSIONS+x}" && -z "${FQ_MYSQL_VERSIONS}" ]]; then
    MYSQL_VERSIONS=()
else
    IFS=',' read -ra MYSQL_VERSIONS <<< "${FQ_MYSQL_VERSIONS:-8.0}"
fi
if [[ -n "${FQ_PG_VERSIONS+x}" && -z "${FQ_PG_VERSIONS}" ]]; then
    PG_VERSIONS=()
else
    IFS=',' read -ra PG_VERSIONS <<< "${FQ_PG_VERSIONS:-16}"
fi
if [[ -n "${FQ_INFLUX_VERSIONS+x}" && -z "${FQ_INFLUX_VERSIONS}" ]]; then
    INFLUX_VERSIONS=()
else
    IFS=',' read -ra INFLUX_VERSIONS <<< "${FQ_INFLUX_VERSIONS:-3.0}"
fi
if [[ -n "${FQ_SQLSERVER_VERSIONS+x}" && -z "${FQ_SQLSERVER_VERSIONS}" ]]; then
    SQLSERVER_VERSIONS=()
else
    IFS=',' read -ra SQLSERVER_VERSIONS <<< "${FQ_SQLSERVER_VERSIONS:-2022}"
fi

MYSQL_USER="${FQ_MYSQL_USER:-root}"
MYSQL_PASS="${FQ_MYSQL_PASS:-taosdata}"
PG_USER="${FQ_PG_USER:-postgres}"
PG_PASS="${FQ_PG_PASS:-taosdata}"
PG_TIMEZONE="${FQ_PG_TIMEZONE:-Asia/Shanghai}"
export TZ="${TZ:-$PG_TIMEZONE}"
export PGTZ="${PGTZ:-$PG_TIMEZONE}"
SQLSERVER_USER="${FQ_SQLSERVER_USER:-sa}"
SQLSERVER_PASS="${FQ_SQLSERVER_PASS:-TaosFq!2026}"
# Container image template; {ver} is replaced by the configured version
# (e.g. 2022 -> mcr.microsoft.com/mssql/server:2022-latest).
# {ver} cannot be used inside ${VAR:-...}: the closing brace would end the
# parameter expansion.  %VER% is substituted per version below.
# (removed) SQL Server now runs as a native process — see ensure_sqlserver().

INFLUX_TOKEN="${FQ_INFLUX_TOKEN:-test-token}"
INFLUX_ORG="${FQ_INFLUX_ORG:-test-org}"

CURRENT_USER="$(id -un)"   # portable alternative to whoami

OVERALL_OK=0

# ──────────────────────────────────────────────────────────────────────────────
# 2.  Logging
# ──────────────────────────────────────────────────────────────────────────────
log()  { echo "[fq-env] $*"; }
info() { echo "[fq-env] INFO  $*"; }
warn() { echo "[fq-env] WARN  $*" >&2; }
err()  { echo "[fq-env] ERROR $*" >&2; }

# External engines must not inherit TDengine's ASAN / loader overrides.
# Pytest launches this script with LD_PRELOAD + LD_LIBRARY_PATH pointed at the
# TDengine debug tree, which can wedge foreign binaries such as PostgreSQL
# initdb/postgres. Clear those variables in the child shell only.
sanitize_foreign_engine_env() {
    local leaked=0 var
    for var in \
        LD_PRELOAD LD_LIBRARY_PATH \
        ASAN_OPTIONS UBSAN_OPTIONS LSAN_OPTIONS TSAN_OPTIONS MSAN_OPTIONS \
        DYLD_INSERT_LIBRARIES DYLD_LIBRARY_PATH; do
        if [[ -n "${!var-}" ]]; then
            leaked=1
            unset "$var"
        fi
    done
    if [[ "$leaked" -eq 1 ]]; then
        info "Cleared inherited loader/sanitizer env for external engines."
    fi
}

# ──────────────────────────────────────────────────────────────────────────────
# 3.  Pre-flight: required tools
# ──────────────────────────────────────────────────────────────────────────────
_require() {
    local cmd="$1" hint="${2:-}"
    if ! command -v "$cmd" &>/dev/null; then
        err "Required tool not found: $cmd${hint:+  (hint: $hint)}"
        exit 1
    fi
}

_require curl  "install curl via package manager"
_require tar
_require grep
_require sed
_require awk

# curl must support --retry (curl ≥ 7.12, effectively universal)
# Warn if python3 missing (used only for optional InfluxDB v2 fallback)
command -v python3 &>/dev/null || warn "python3 not found; some InfluxDB helpers may be skipped."

# ──────────────────────────────────────────────────────────────────────────────
# 4.  Port helpers (no /dev/tcp; use nc with multiple fallbacks)
# ──────────────────────────────────────────────────────────────────────────────
port_open() {
    local port="$1"
    # Prefer nc (netcat); fall back to curl TCP probe; last resort /dev/tcp
    if command -v nc &>/dev/null; then
        nc -z -w 2 127.0.0.1 "$port" 2>/dev/null
        return
    fi
    if command -v ncat &>/dev/null; then
        ncat -z -w 2 127.0.0.1 "$port" 2>/dev/null
        return
    fi
    # curl can probe TCP without HTTP
    curl -sf --connect-timeout 2 "telnet://127.0.0.1:${port}" -o /dev/null 2>/dev/null
    return
}

# SQL Server runs as a plain background process from packages extracted under
# ${FQ_BASE_DIR}/sqlserver/<ver> — the same provisioning model as the other
# engines (no docker anywhere).
sqlserver_base() {
    echo "${FQ_BASE_DIR}/sqlserver/$1"
}

sqlserver_bin() {
    echo "$(sqlserver_base "$1")/server/opt/mssql/bin/sqlservr"
}

sqlserver_deps_dir() {
    echo "$(sqlserver_base "$1")/server/deps/usr/lib/x86_64-linux-gnu"
}

sqlserver_port() {
    case "$1" in
        2019) echo "${FQ_SQLSERVER_PORT_2019:-1434}" ;;
        2022) echo "${FQ_SQLSERVER_PORT_2022:-1433}" ;;
        *)    echo "${FQ_SQLSERVER_PORT:-1433}" ;;
    esac
}

# Locate sqlcmd (extracted from mssql-tools18 next to the engine binaries).
sqlserver_sqlcmd_path() {
    local ver="$1"
    local base; base="$(sqlserver_base "${ver}")"
    local cand
    for cand in "${base}/tools/opt/mssql-tools18/bin/sqlcmd" \
                "${base}/tools/opt/mssql-tools/bin/sqlcmd" \
                /opt/mssql-tools18/bin/sqlcmd /opt/mssql-tools/bin/sqlcmd; do
        if [[ -x "${cand}" ]]; then
            echo "${cand}"
            return 0
        fi
    done
    return 1
}

wait_port() {
    local port="$1" max="${2:-60}" i=0
    while ! port_open "$port"; do
        sleep 1
        i=$((i + 1))
        if [[ "$i" -ge "$max" ]]; then
            return 1
        fi
    done
}

# ──────────────────────────────────────────────────────────────────────────────
# 5.  Process management (pkill compatible across Linux + macOS + BusyBox)
# ──────────────────────────────────────────────────────────────────────────────
# Return PIDs whose command line matches a pattern.
_list_matching_pids() {
    local pattern="$1"
    # `ps` / `pgrep` can wedge on this host while FQ teardown is scanning the
    # process table. On Linux, scan /proc directly so process enumeration
    # cannot block the whole suite on external command hangs.
    if [[ -d /proc && "$(uname -s)" == "Linux" ]]; then
        local pid_dir pid comm cmdline exe
        local -a argv
        for pid_dir in /proc/[0-9]*; do
            [[ -d "$pid_dir" ]] || continue
            pid="${pid_dir#/proc/}"
            [[ "$pid" == "$$" ]] && continue

            comm=""
            IFS= read -r comm < "${pid_dir}/comm" 2>/dev/null || continue
            # NOTE: every long-running FQ engine must be listed here — a missing
            # name silently turns the corresponding stop/kill into a no-op.
            # sqlservr was missing, so _sqlserver_stop_procs never stopped the
            # engine: the "hard reset" then started a SECOND instance which
            # failed bootstrap against the still-held /var/opt/mssql ("Access is
            # denied") while the probe passed against the old one.
            case "$comm" in
                postgres|initdb|mysqld|influxdb3|influxd|sqlservr) ;;
                *) continue ;;
            esac

            cmdline=""
            argv=()
            if [[ -r "${pid_dir}/cmdline" ]]; then
                mapfile -d '' -t argv < "${pid_dir}/cmdline" 2>/dev/null || true
            fi
            if (( ${#argv[@]} > 0 )); then
                cmdline="${argv[*]}"
            else
                cmdline="$comm"
                exe=""
                [[ -L "${pid_dir}/exe" ]] && exe="$(readlink "${pid_dir}/exe" 2>/dev/null || true)"
                [[ -n "$exe" ]] && cmdline+=" ${exe}"
            fi

            [[ -n "$cmdline" && "$cmdline" =~ $pattern ]] && printf '%s\n' "$pid"
        done
        return 0
    fi

    if command -v pgrep &>/dev/null; then
        pgrep -f "$pattern" 2>/dev/null | awk -v self="$$" '$1 != self {print $1}' || true
        return 0
    fi

    if command -v timeout &>/dev/null; then
        timeout 5 ps ax -o pid= -o command= 2>/dev/null \
            | awk -v pat="$pattern" -v self="$$" '
                $1 != self && $0 ~ pat && $0 !~ /awk/ && $0 !~ /ps ax -o pid= -o command=/ {print $1}
            ' || true
        return 0
    fi

    ps ax -o pid= -o command= 2>/dev/null \
        | awk -v pat="$pattern" -v self="$$" '
            $1 != self && $0 ~ pat && $0 !~ /awk/ && $0 !~ /ps ax -o pid= -o command=/ {print $1}
        ' || true
}

# Kill processes whose command line matches a pattern.
_kill_matching() {
    local pattern="$1"
    local sig="${2:-TERM}"
    local pids
    pids="$(_list_matching_pids "$pattern")"
    if [[ -n "$pids" ]]; then
        # shellcheck disable=SC2086
        kill -"$sig" $pids 2>/dev/null || true
        return 0
    fi
    return 0
}

# Write a PID into a pidfile; used by _start_daemon
_write_pidfile() {
    echo "$!" > "$1"
}

# Rotate a log file if it exceeds LOG_ROTATE_MAX_BYTES (default 50 MiB).
# Keeps at most 1 rotated copy (.1); silently does nothing if file is absent.
: "${LOG_ROTATE_MAX_BYTES:=$((50 * 1024 * 1024))}"
_rotate_log() {
    local f="$1"
    [[ -f "$f" ]] || return 0
    local size
    size=$(stat -c%s "$f" 2>/dev/null || echo 0)
    if (( size > LOG_ROTATE_MAX_BYTES )); then
        mv -f "$f" "${f}.1" 2>/dev/null || true
        : > "$f"
    fi
}

# Run a command without inherited loader and sanitizer overrides from the
# TDengine test process. External DB tools must use system libraries.
_fq_env_clean() {
    local _runner=(env)
    local var
    for var in \
        LD_PRELOAD LD_LIBRARY_PATH \
        ASAN_OPTIONS UBSAN_OPTIONS LSAN_OPTIONS TSAN_OPTIONS MSAN_OPTIONS \
        DYLD_INSERT_LIBRARIES DYLD_LIBRARY_PATH \
        PYTEST_CURRENT_TEST; do
        [[ -n "${!var-}" ]] && _runner+=( -u "$var" )
    done
    "${_runner[@]}" "$@"
}

_check_pymysql_dependency() {
    local python_bin="${FQ_PYTHON:-python3}"

    if ! command -v "$python_bin" >/dev/null 2>&1; then
        err "MySQL setup requires Python, but the interpreter was not found: ${python_bin}"
        err "Set FQ_PYTHON to the Python interpreter used by pytest."
        return 1
    fi

    if ! _fq_env_clean "$python_bin" -c 'import pymysql' >/dev/null 2>&1; then
        err "MySQL setup requires the pymysql module in: ${python_bin}"
        err "Install it with: ${python_bin} -m pip install pymysql"
        err "Or install the dependencies from source/taos-community/test/requirements.txt."
        return 1
    fi
}

_fq_mysql_ld_path() {
    local base="$1"
    local lib_private="${base}/lib/private"
    if [[ -d "$lib_private" ]]; then
        echo "$lib_private"
    fi
}

# MySQL tarball clients need lib/private on LD_LIBRARY_PATH and must not
# inherit TDengine's loader overrides.
_fq_mysql_run() {
    local base="$1"; shift
    local executable="${1:-}"
    local _ldlp; _ldlp="$(_fq_mysql_ld_path "$base")"
    if [[ -n "$_ldlp" && "$executable" == "${base}/"* ]]; then
        _fq_env_clean env LD_LIBRARY_PATH="$_ldlp" "$@"
    else
        _fq_env_clean "$@"
    fi
}

# Execute MySQL SQL through pymysql rather than the bundled mysql CLI.
# The MySQL tarball client can require legacy libncurses/libtinfo libraries
# that are not available on newer distro releases.
_mysql_py_exec() {
    local host="$1" port="$2" user="$3" password="$4" sql="$5"
    MYSQL_HOST="$host" \
    MYSQL_PORT="$port" \
    MYSQL_USER="$user" \
    MYSQL_PASS="$password" \
    MYSQL_SQL="$sql" \
        _fq_env_clean "${FQ_PYTHON:-python3}" - <<'PY'
import os
import pymysql
from pymysql.constants import CLIENT

conn = pymysql.connect(
    host=os.environ["MYSQL_HOST"],
    port=int(os.environ["MYSQL_PORT"]),
    user=os.environ["MYSQL_USER"],
    password=os.environ.get("MYSQL_PASS", ""),
    connect_timeout=5,
    autocommit=True,
    charset="utf8mb4",
    client_flag=CLIENT.MULTI_STATEMENTS,
)
try:
    with conn.cursor() as cur:
        sql = os.environ.get("MYSQL_SQL", "").strip()
        if sql:
            cur.execute(sql)
            while cur.nextset():
                pass
finally:
    conn.close()
PY
}

_mysql_py_exec_stdin() {
    local host="$1" port="$2" user="$3" password="$4"
    MYSQL_HOST="$host" \
    MYSQL_PORT="$port" \
    MYSQL_USER="$user" \
    MYSQL_PASS="$password" \
        _fq_env_clean "${FQ_PYTHON:-python3}" -c '
import os
import sys
import pymysql
from pymysql.constants import CLIENT

conn = pymysql.connect(
    host=os.environ["MYSQL_HOST"],
    port=int(os.environ["MYSQL_PORT"]),
    user=os.environ["MYSQL_USER"],
    password=os.environ.get("MYSQL_PASS", ""),
    database="mysql",
    connect_timeout=5,
    autocommit=True,
    charset="utf8mb4",
    client_flag=CLIENT.MULTI_STATEMENTS,
)
try:
    with conn.cursor() as cur:
        sql = sys.stdin.read().strip()
        if sql:
            cur.execute(sql)
            while cur.nextset():
                pass
finally:
    conn.close()
'
}

_mysql_py_scalar() {
    local host="$1" port="$2" user="$3" password="$4" sql="$5"
    MYSQL_HOST="$host" \
    MYSQL_PORT="$port" \
    MYSQL_USER="$user" \
    MYSQL_PASS="$password" \
    MYSQL_SQL="$sql" \
        _fq_env_clean "${FQ_PYTHON:-python3}" -c '
import os
import pymysql

conn = pymysql.connect(
    host=os.environ["MYSQL_HOST"],
    port=int(os.environ["MYSQL_PORT"]),
    user=os.environ["MYSQL_USER"],
    password=os.environ.get("MYSQL_PASS", ""),
    connect_timeout=5,
    autocommit=True,
)
try:
    with conn.cursor() as cur:
        cur.execute(os.environ["MYSQL_SQL"])
        row = cur.fetchone()
        if row is not None and row[0] is not None:
            print(row[0])
finally:
    conn.close()
'
}

_mysql_py_check_timezone() {
    local host="$1" port="$2" user="$3" password="$4"
    MYSQL_HOST="$host" \
    MYSQL_PORT="$port" \
    MYSQL_USER="$user" \
    MYSQL_PASS="$password" \
        _fq_env_clean "${FQ_PYTHON:-python3}" - <<'PY'
import os
import pymysql

conn = pymysql.connect(
    host=os.environ["MYSQL_HOST"],
    port=int(os.environ["MYSQL_PORT"]),
    user=os.environ["MYSQL_USER"],
    password=os.environ.get("MYSQL_PASS", ""),
    connect_timeout=5,
    autocommit=True,
)
try:
    with conn.cursor() as cur:
        for zone in ("Asia/Shanghai", "UTC"):
            cur.execute(
                "SELECT CONVERT_TZ(%s, %s, '+00:00') IS NOT NULL",
                ("2024-01-01 00:00:00", zone),
            )
            if cur.fetchone()[0] != 1:
                raise RuntimeError(f"MySQL timezone data is unavailable for {zone}")
finally:
    conn.close()
PY
}

# Start a daemon via nohup, record PID in pidfile, return immediately
# Usage: _start_daemon <pidfile> <logfile> <cmd> [args...]
_start_daemon() {
    local pidfile="$1" logfile="$2"
    shift 2
    mkdir -p "$(dirname "$pidfile")" "$(dirname "$logfile")"
    _rotate_log "$logfile"
    local _runner=(env -u LD_PRELOAD -u LD_LIBRARY_PATH)
    local _pid
    # setsid is missing on macOS; nohup + & is sufficient there.
    # Avoid _fq_env_clean here: backgrounding a shell function can leave the
    # ensure_ext_env.sh wrapper alive as the daemon's parent.
    if command -v setsid >/dev/null 2>&1; then
        "${_runner[@]}" setsid nohup "$@" >> "$logfile" 2>&1 &
    else
        "${_runner[@]}" nohup "$@" >> "$logfile" 2>&1 &
    fi
    _pid="$!"
    echo "$_pid" > "$pidfile"
    disown "$_pid" 2>/dev/null || true
}

# Stop a daemon by pidfile; fall back to pattern kill.
# Polls every 0.1 s for graceful exit instead of sleeping a fixed duration.
_stop_daemon() {
    local pidfile="$1" pattern="$2"
    local pid=""
    if [[ -f "$pidfile" ]]; then
        pid=$(cat "$pidfile" 2>/dev/null || true)
    fi

    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null || true
        # Poll for graceful exit (up to 5 s = 50 × 0.1 s)
        local _di=0
        while kill -0 "$pid" 2>/dev/null && [[ $_di -lt 50 ]]; do
            sleep 0.1; _di=$((_di+1))
        done
        kill -0 "$pid" 2>/dev/null && kill -KILL "$pid" 2>/dev/null || true
    fi
    rm -f "$pidfile" 2>/dev/null || true

    # Sweep any stragglers matched by pattern
    if [[ -n "$(_list_matching_pids "$pattern")" ]]; then
        _kill_matching "$pattern" TERM
        local _di=0
        while [[ -n "$(_list_matching_pids "$pattern")" ]] && [[ $_di -lt 50 ]]; do
            sleep 0.1; _di=$((_di+1))
        done
        _kill_matching "$pattern" KILL 2>/dev/null || true
    fi
}

# ── Verified-exit helpers ──────────────────────────────────────────────────────
# Wait until no process matching PATTERN exists.
# SIGKILL takes effect within milliseconds; this normally exits on the first
# check.  Polls every 0.1 s; returns 0 if gone within MAX checks, 1 otherwise.
_wait_procs_gone() {
    local pattern="$1" max="${2:-100}" i=0
    while [[ -n "$(_list_matching_pids "$pattern")" ]]; do
        i=$((i+1))
        if [[ $i -ge $max ]]; then
            warn "Processes still alive after $((max/10))s: $pattern"
            return 1
        fi
        sleep 0.1
    done
    return 0
}

# Assert PATH does not exist; log an error and return 1 if it does.
# Used to confirm rm -rf succeeded before re-initialising data directories.
_verify_absent() {
    local path="$1" label="${2:-$path}"
    if [[ -e "$path" ]]; then
        err "Expected absent but still present: $label"
        return 1
    fi
    return 0
}

# Wait until a TCP port is no longer open (released by the killed process).
# nc -z returns immediately (0 ms) when the port IS open, so without a sleep
# the loop would busy-spin.  We add 0.2 s between probes.
_wait_port_free() {
    local port="$1" max="${2:-30}" i=0
    while port_open "$port"; do
        i=$((i+1))
        if [[ $i -ge $max ]]; then
            warn "Port ${port} still open after ${max}×0.2 s"
            return 1
        fi
        sleep 0.2
    done
    return 0
}

# ──────────────────────────────────────────────────────────────────────────────
# 6.  Download with retry + integrity (portable)
# ──────────────────────────────────────────────────────────────────────────────
_download_with_retry() {
    local url="$1" dest="$2" max_attempts="${3:-5}"
    local attempt=1 wait=5

    # Verify if dest already complete: curl -I for Content-Length vs file size
    # (skip check for simplicity; just re-download if last attempt was partial)

    while [[ "$attempt" -le "$max_attempts" ]]; do
        info "download (attempt ${attempt}/${max_attempts}): $(basename "$dest")"
        info "  URL: $url"

        # -C - resumes; if server doesn't support Range it re-downloads
        # --location follows redirects (GitHub releases redirect to S3)
        if curl -fL \
                --location \
                --retry 3 --retry-delay 5 --retry-connrefused \
                --connect-timeout 30 --max-time 3600 \
                --speed-time 60 --speed-limit 1024 \
                -C - \
                -o "$dest" \
                "$url" 2>&1; then
            # Basic integrity: file must exist and be non-empty
            if [[ -s "$dest" ]]; then
                return 0
            fi
            warn "download produced empty file, retrying ..."
            rm -f "$dest"
        else
            warn "curl failed (attempt ${attempt}), retrying in ${wait}s ..."
            rm -f "$dest"
        fi

        sleep "$wait"
        wait=$(( wait * 2 > 120 ? 120 : wait * 2 ))
        attempt=$((attempt + 1))
    done
    err "download failed after ${max_attempts} attempts: $url"
    return 1
}

# Writable dir for fq-* symlinks/downloads.  CI mounts compat-packages :ro at
# /usr/local/src, so fall back to /tmp when the preferred cache is read-only.
_fq_writable_tarball_dir() {
    local preferred="${1:-${FQ_TARBALL_CACHE_DIR}}"
    if mkdir -p "$preferred" 2>/dev/null && touch "${preferred}/.write-test" 2>/dev/null; then
        rm -f "${preferred}/.write-test"
        printf '%s\n' "$preferred"
        return 0
    fi
    local fallback="/tmp/fq-tarball-cache"
    mkdir -p "$fallback"
    printf '%s\n' "$fallback"
}

# Resolve a tarball without network when possible; prints the resolved path.
# 1. fq-<engine>-<ver>.* in writable cache → reuse
# 2. staged upstream name (compat-packages, possibly read-only) → symlink in cache
# 3. otherwise download into writable cache
_ensure_tarball_cached() {
    local dest_name="$1" url="$2"
    local cache_dir; cache_dir="$(_fq_writable_tarball_dir "${FQ_TARBALL_CACHE_DIR}")"
    local tarball="${cache_dir}/${dest_name}"

    if [[ -s "$tarball" ]]; then
        info "tarball: using cached ${dest_name}" >&2
        printf '%s\n' "$tarball"
        return 0
    fi

    mkdir -p "$cache_dir"

    local upstream; upstream="$(basename "$url")"
    local search_dirs=()
    local extra
    while IFS= read -r extra; do
        search_dirs+=("$extra")
    done < <(_fq_staging_dirs)
    [[ " ${search_dirs[*]} " != *" ${cache_dir} "* ]] && search_dirs+=("$cache_dir")

    local root rel candidate
    for root in "${search_dirs[@]}"; do
        for rel in "$dest_name" "$upstream" "mysql/${upstream}" "influx/${upstream}"; do
            candidate="${root}/${rel}"
            if [[ -s "$candidate" ]]; then
                if [[ "$candidate" -ef "$tarball" ]]; then
                    info "tarball: using staged ${rel}" >&2
                elif ln -sf "$candidate" "$tarball" 2>/dev/null; then
                    info "tarball: linked ${rel} → ${dest_name}" >&2
                elif cp -f "$candidate" "$tarball" 2>/dev/null; then
                    info "tarball: copied ${rel} → ${dest_name}" >&2
                else
                    err "tarball: cannot link/copy staged ${candidate}"
                    return 1
                fi
                printf '%s\n' "$tarball"
                return 0
            fi
        done
    done

    # compat-packages often ships upstream full names (glibc2.28) rather than
    # fq-* aliases or the minimal tarball URL basename.
    local _glob _globs=()
    case "$dest_name" in
        fq-mysql-*.tar.xz)
            _globs=("mysql-*.tar.xz" "mysql/mysql-*.tar.xz")
            ;;
        fq-influxdb-*.tar.gz)
            _globs=("influxdb3-core-*.tar.gz" "influx/influxdb3-core-*.tar.gz")
            ;;
    esac
    for root in "${search_dirs[@]}"; do
        for _glob in "${_globs[@]}"; do
            local _candidates=()
            shopt -s nullglob
            _candidates=("${root}/${_glob}")
            shopt -u nullglob
            for candidate in "${_candidates[@]}"; do
                [[ -s "$candidate" ]] || continue
                if ln -sf "$candidate" "$tarball" 2>/dev/null; then
                    info "tarball: linked $(basename "$candidate") → ${dest_name}" >&2
                elif cp -f "$candidate" "$tarball" 2>/dev/null; then
                    info "tarball: copied $(basename "$candidate") → ${dest_name}" >&2
                else
                    err "tarball: cannot link/copy staged ${candidate}"
                    return 1
                fi
                printf '%s\n' "$tarball"
                return 0
            done
            shopt -u nullglob
        done
    done

    _download_with_retry "$url" "$tarball"
    printf '%s\n' "$tarball"
}

# MySQL tarball on Linux needs libaio; port probes need nc on minimal images.
_ensure_engine_deps_apt_index() {
    command -v apt-get &>/dev/null || return 0
    [[ "${FQ_ENGINE_DEPS_APT_UPDATED:-0}" == "1" ]] && return 0

    info "refreshing apt package index for engine OS deps ..."
    if _apt_update_retry 2; then
        export FQ_ENGINE_DEPS_APT_UPDATED=1
    else
        warn "apt package index refresh failed; trying engine OS deps install with existing index."
    fi
}

_libaio_present() {
    ldconfig -p 2>/dev/null | grep -q 'libaio\.so' && return 0
    find /usr/lib /lib -maxdepth 4 -name 'libaio.so*' 2>/dev/null | grep -q .
}

_libaio_so1_present() {
    ldconfig -p 2>/dev/null | grep -qE 'libaio\.so\.1($| )' && return 0
    [[ -e /usr/lib/x86_64-linux-gnu/libaio.so.1 ]] && return 0
    find /usr/lib /lib -maxdepth 4 -name 'libaio.so.1' 2>/dev/null | grep -q .
}

_ensure_libaio_so1_link() {
    _libaio_so1_present && return 0

    local _src
    _src=$(find /usr/lib /lib -maxdepth 4 -name 'libaio.so.1*' \
               ! -name 'libaio.so.1' 2>/dev/null | head -1)
    if [[ -n "$_src" ]]; then
        local _dir; _dir=$(dirname "$_src")
        ln -sf "$_src" "${_dir}/libaio.so.1" 2>/dev/null || true
        ldconfig 2>/dev/null || true
        info "engine OS deps: created libaio.so.1 -> $_src"
    fi

    _libaio_so1_present
}

_install_libaio_deb() {
    local _deb="$1"
    [[ -s "$_deb" ]] || return 1

    info "engine OS deps: installing local $(basename "$_deb") ..."
    if command -v dpkg &>/dev/null; then
        DEBIAN_FRONTEND=noninteractive dpkg -i --force-depends "$_deb" >/dev/null 2>&1 || true
        DEBIAN_FRONTEND=noninteractive dpkg --configure -a >/dev/null 2>&1 || true
    fi
    if ! _libaio_present && command -v dpkg-deb &>/dev/null; then
        dpkg-deb -x "$_deb" / >/dev/null 2>&1 || true
    fi
    ldconfig 2>/dev/null || true
    _libaio_present
}

_install_libaio_from_local_cache() {
    local _d _deb _cache _tmp

    for _d in /var/cache/apt/archives; do
        [[ -d "$_d" ]] || continue
        _deb=$(find "$_d" -maxdepth 1 -type f \
                   \( -name 'libaio1t64_*.deb' -o -name 'libaio1_*.deb' \) \
                   2>/dev/null | head -1)
        [[ -n "$_deb" ]] && _install_libaio_deb "$_deb" && return 0
    done

    while IFS= read -r _d; do
        _deb=$(find "$_d" -maxdepth 3 -type f \
                   \( -name 'libaio1t64_*.deb' -o -name 'libaio1_*.deb' \) \
                   2>/dev/null | head -1)
        [[ -n "$_deb" ]] && _install_libaio_deb "$_deb" && return 0
    done < <(_fq_staging_dirs)

    while IFS= read -r _d; do
        while IFS= read -r _cache; do
            _tmp="$(mktemp -d)"
            tar -xzf "$_cache" -C "$_tmp" >/dev/null 2>&1 || { rm -rf "$_tmp"; continue; }
            _deb=$(find "$_tmp" -type f \
                       \( -name 'libaio1t64_*.deb' -o -name 'libaio1_*.deb' \) \
                       2>/dev/null | head -1)
            if [[ -n "$_deb" ]] && _install_libaio_deb "$_deb"; then
                rm -rf "$_tmp"
                return 0
            fi
            rm -rf "$_tmp"
        done < <(find "$_d" -maxdepth 1 -type f -name 'fq-apt-*.tar.gz' 2>/dev/null)
    done < <(_fq_staging_dirs)

    return 1
}

_libaio_direct_deb_url() {
    if [[ -n "${FQ_LIBAIO_DEB_URL:-}" ]]; then
        printf '%s\n' "${FQ_LIBAIO_DEB_URL}"
        return 0
    fi

    local _os_id=""
    if [[ -f /etc/os-release ]]; then
        _os_id="$(. /etc/os-release && echo "${ID:-}")"
    fi
    [[ "$_os_id" == "debian" ]] || return 1

    local _arch=""
    if command -v dpkg >/dev/null 2>&1; then
        _arch="$(dpkg --print-architecture 2>/dev/null || true)"
    fi
    if [[ -z "$_arch" ]]; then
        case "$ARCH" in
            x86_64)        _arch="amd64" ;;
            aarch64|arm64) _arch="arm64" ;;
            *)             return 1 ;;
        esac
    fi

    local _base _codename _repo _pkg
    _base="${FQ_APT_MIRROR_BASE:-https://nexus.tdengine.net/repository}"
    _base="${_base%/}"
    _codename="$(_apt_codename)"
    case "$_codename" in
        trixie|"")
            _repo="${_base}/debian13"
            _pkg="libaio1t64_0.3.113-8+b1_${_arch}.deb"
            ;;
        bookworm)
            _repo="${_base}/debian12"
            _pkg="libaio1_0.3.113-4_${_arch}.deb"
            ;;
        *)
            return 1
            ;;
    esac

    printf '%s\n' "${_repo}/pool/main/liba/libaio/${_pkg}"
}

_install_libaio_from_direct_deb() {
    command -v curl >/dev/null 2>&1 || return 1

    local _url _cache_dir _deb
    _url="$(_libaio_direct_deb_url)" || return 1
    _cache_dir="$(_fq_writable_tarball_dir "${FQ_TARBALL_CACHE_DIR}")"
    _deb="${_cache_dir}/$(basename "$_url")"

    if [[ ! -s "$_deb" ]]; then
        info "engine OS deps: downloading libaio fallback $(basename "$_deb") ..."
        _download_with_retry "$_url" "$_deb" 2 || return 1
    fi
    _install_libaio_deb "$_deb"
}

_libnuma_present() {
    ldconfig -p 2>/dev/null | grep -q 'libnuma\.so' && return 0
    find /usr/lib /lib -maxdepth 4 -name 'libnuma.so*' 2>/dev/null | grep -q .
}

_install_libnuma_deb() {
    local _deb="$1"
    [[ -s "$_deb" ]] || return 1

    info "engine OS deps: installing local $(basename "$_deb") ..."
    if command -v dpkg &>/dev/null; then
        DEBIAN_FRONTEND=noninteractive dpkg -i --force-depends "$_deb" >/dev/null 2>&1 || true
        DEBIAN_FRONTEND=noninteractive dpkg --configure -a >/dev/null 2>&1 || true
    fi
    if ! _libnuma_present && command -v dpkg-deb &>/dev/null; then
        dpkg-deb -x "$_deb" / >/dev/null 2>&1 || true
    fi
    ldconfig 2>/dev/null || true
    _libnuma_present
}

_install_libnuma_from_local_cache() {
    local _d _deb _cache _tmp

    for _d in /var/cache/apt/archives; do
        [[ -d "$_d" ]] || continue
        _deb=$(find "$_d" -maxdepth 1 -type f -name 'libnuma1_*.deb' 2>/dev/null | head -1)
        [[ -n "$_deb" ]] && _install_libnuma_deb "$_deb" && return 0
    done

    while IFS= read -r _d; do
        _deb=$(find "$_d" -maxdepth 3 -type f -name 'libnuma1_*.deb' 2>/dev/null | head -1)
        [[ -n "$_deb" ]] && _install_libnuma_deb "$_deb" && return 0
    done < <(_fq_staging_dirs)

    while IFS= read -r _d; do
        while IFS= read -r _cache; do
            _tmp="$(mktemp -d)"
            tar -xzf "$_cache" -C "$_tmp" >/dev/null 2>&1 || { rm -rf "$_tmp"; continue; }
            _deb=$(find "$_tmp" -type f -name 'libnuma1_*.deb' 2>/dev/null | head -1)
            if [[ -n "$_deb" ]] && _install_libnuma_deb "$_deb"; then
                rm -rf "$_tmp"
                return 0
            fi
            rm -rf "$_tmp"
        done < <(find "$_d" -maxdepth 1 -type f -name 'fq-apt-*.tar.gz' 2>/dev/null)
    done < <(_fq_staging_dirs)

    return 1
}

_libnuma_direct_deb_url() {
    if [[ -n "${FQ_LIBNUMA_DEB_URL:-}" ]]; then
        printf '%s\n' "${FQ_LIBNUMA_DEB_URL}"
        return 0
    fi

    local _os_id=""
    if [[ -f /etc/os-release ]]; then
        _os_id="$(. /etc/os-release && echo "${ID:-}")"
    fi
    [[ "$_os_id" == "debian" ]] || return 1

    local _arch=""
    if command -v dpkg >/dev/null 2>&1; then
        _arch="$(dpkg --print-architecture 2>/dev/null || true)"
    fi
    if [[ -z "$_arch" ]]; then
        case "$ARCH" in
            x86_64)        _arch="amd64" ;;
            aarch64|arm64) _arch="arm64" ;;
            *)             return 1 ;;
        esac
    fi

    local _base _codename _repo _pkg
    _base="${FQ_APT_MIRROR_BASE:-https://nexus.tdengine.net/repository}"
    _base="${_base%/}"
    _codename="$(_apt_codename)"
    case "$_codename" in
        trixie|"")
            _repo="${_base}/debian13"
            _pkg="libnuma1_2.0.19-1_${_arch}.deb"
            ;;
        bookworm)
            _repo="${_base}/debian12"
            _pkg="libnuma1_2.0.16-1_${_arch}.deb"
            ;;
        *)
            return 1
            ;;
    esac

    printf '%s\n' "${_repo}/pool/main/n/numactl/${_pkg}"
}

_install_libnuma_from_direct_deb() {
    command -v curl >/dev/null 2>&1 || return 1

    local _url _cache_dir _deb
    _url="$(_libnuma_direct_deb_url)" || return 1
    _cache_dir="$(_fq_writable_tarball_dir "${FQ_TARBALL_CACHE_DIR}")"
    _deb="${_cache_dir}/$(basename "$_url")"

    if [[ ! -s "$_deb" ]]; then
        info "engine OS deps: downloading libnuma fallback $(basename "$_deb") ..."
        _download_with_retry "$_url" "$_deb" 2 || return 1
    fi
    _install_libnuma_deb "$_deb"
}

# apt-get with hard network bounds, mirroring _apt_update_retry: every caller
# has an offline fallback, so a stalled mirror must fail fast instead of
# holding the env run hostage.
_apt_get_install_bounded() {
    timeout "${FQ_APT_NET_TIMEOUT_S:-180}" \
        env DEBIAN_FRONTEND=noninteractive \
        apt-get install -y -q --no-install-recommends \
        -o Acquire::http::Timeout=15 -o Acquire::https::Timeout=15 \
        -o Acquire::Retries=1 "$@"
}

_ensure_linux_engine_deps() {
    [[ "$OS" != "Linux" ]] && return 0
    command -v apt-get &>/dev/null || return 0

    # CI containers normally run as root and may install missing packages.
    # Local pytest runs commonly use an unprivileged account; repeatedly
    # invoking apt-get there only produces permission errors and delays setup.
    if [[ "$(id -u)" -ne 0 ]]; then
        _libaio_present || warn "engine OS deps: libaio is missing; install it as root before running MySQL tests."
        _libnuma_present || warn "engine OS deps: libnuma is missing; install it as root before running MySQL tests."
        return 0
    fi

    # Refresh the apt index only when a needed package is actually missing:
    # in CI the refresh is the slowest part of every env run and can stall
    # (see _apt_update_retry).  When libaio / libnuma / nc already work there
    # is nothing to install, so skip it entirely.
    if ! { _libaio_present && _libnuma_present && command -v nc >/dev/null 2>&1; }; then
        _ensure_engine_deps_apt_index
    fi

    # Install libaio separately so we can fall back from libaio1t64 to libaio1.
    # Modern Debian (trixie+) renamed the package to libaio1t64; older distros
    # use libaio1.  A batch install that includes an unknown package name causes
    # the whole apt-get call to fail — so we try them individually.
    if ! _libaio_present; then
        info "installing engine OS deps: libaio (libaio1t64 or libaio1) ..."
        if ! _apt_get_install_bounded libaio1t64; then
            _apt_get_install_bounded libaio1 \
                || _install_libaio_from_local_cache \
                || _install_libaio_from_direct_deb \
                || warn "engine OS deps: libaio could not be installed (tried apt, local cache, and direct deb)."
        fi
    fi

    local _need=()
    if ! _libnuma_present; then
        info "installing engine OS deps: libnuma1 ..."
        _apt_get_install_bounded libnuma1 \
            || _libnuma_present \
            || _install_libnuma_from_local_cache \
            || _install_libnuma_from_direct_deb \
            || warn "engine OS deps: libnuma1 could not be installed (tried apt, local cache, and direct deb)."
    fi
    if ! _libnuma_present; then
        if ! compgen -G "/usr/lib/*/libnuma.so*" >/dev/null 2>&1; then
            warn "engine OS deps: libnuma.so.1 is still missing; MySQL may fail to start."
        fi
    fi
    command -v nc &>/dev/null || _need+=(netcat-openbsd)

    if [[ ${#_need[@]} -gt 0 ]]; then
        info "installing engine OS deps: ${_need[*]} ..."
        _apt_get_install_bounded "${_need[@]}" 2>/dev/null || warn "engine OS deps install had warnings."
    fi

    # Ensure libaio.so.1 symlink exists.  The t64 ABI-transition packages ship
    # libaio.so.1t64 (not libaio.so.1), so MySQL cannot dlopen it without the
    # compat symlink.  Search the common lib directories rather than hardcoding
    # one path so this works on multi-arch or non-standard prefix installations.
    _ensure_libaio_so1_link || warn "engine OS deps: libaio.so.1 is still missing; MySQL may fail to start."
}

# True when CI compat-packages are present on pure Debian (tdengine-ci).
# Ubuntu/macOS/RHEL and local dev without staged packages fall back to public mirrors.
_compat_packages_staged() {
    local d
    while IFS= read -r d; do
        _compat_packages_staged_in "$d" && return 0
    done < <(_fq_staging_dirs)
    return 1
}

_use_nexus_apt() {
    case "${FQ_USE_NEXUS_APT:-}" in
        0|no|false) return 1 ;;
        1|yes|true) ;;
        *)
            # auto: only tdengine-ci-like Debian with compat-packages mount
            [[ "$DISTRO" == "debian" ]] || return 1
            local id=""; [[ -f /etc/os-release ]] && id="$(. /etc/os-release && echo "${ID}")"
            [[ "$id" == "debian" ]] || return 1
            _compat_packages_staged || return 1
            ;;
    esac
    return 0
}

# Route Debian apt through internal Nexus proxies (no credentials required).
_configure_apt_mirrors() {
    _use_nexus_apt || return 0
    [[ "$DISTRO" != "debian" ]] && return 0
    command -v apt-get &>/dev/null || return 0
    [[ "${FQ_APT_MIRROR_CONFIGURED:-0}" == "1" ]] && return 0

    local base="${FQ_APT_MIRROR_BASE:-https://nexus.tdengine.net/repository}"
    base="${base%/}"
    local codename; codename="$(_apt_codename)"
    [[ -z "$codename" ]] && { warn "apt: cannot detect codename; Nexus mirror skipped."; return 0; }

    local debian_repo debian_sec_repo
    case "$codename" in
        trixie)   debian_repo="debian13"; debian_sec_repo="debian13-security" ;;
        bookworm) debian_repo="debian12"; debian_sec_repo="debian12-security" ;;
        *)        debian_repo="debian13"; debian_sec_repo="debian13-security" ;;
    esac

    mkdir -p /etc/apt/apt.conf.d
    echo 'Acquire::https::nexus.tdengine.net::Verify-Peer "false";' \
        > /etc/apt/apt.conf.d/99fq-nexus-ssl

    if [[ -f /etc/apt/sources.list.d/debian.sources ]]; then
        sed -i \
            "s|URIs: http://deb.debian.org/debian$|URIs: ${base}/${debian_repo}|; \
             s|URIs: http://deb.debian.org/debian-security|URIs: ${base}/${debian_sec_repo}|" \
            /etc/apt/sources.list.d/debian.sources
    fi
    if [[ -f /etc/apt/sources.list ]]; then
        sed -i \
            "s|http://deb.debian.org/debian|${base}/${debian_repo}|g; \
             s|http://security.debian.org/debian-security|${base}/${debian_sec_repo}|g" \
            /etc/apt/sources.list
    fi

    export FQ_APT_MIRROR_CONFIGURED=1
    info "apt: Nexus mirrors enabled (debian=${debian_repo}, security=${debian_sec_repo})"
}

# ──────────────────────────────────────────────────────────────────────────────
# 7.  OS / distro detection
# ──────────────────────────────────────────────────────────────────────────────
_distro() {
    # Returns: debian | rhel | alpine | arch | suse | macos | unknown
    if [[ "$OS" == "Darwin" ]]; then echo "macos"; return; fi
    if [[ -f /etc/os-release ]]; then
        local id
        id=$(. /etc/os-release && echo "${ID_LIKE:-$ID}" | tr '[:upper:]' '[:lower:]')
        case "$id" in
            *debian*|*ubuntu*) echo "debian" ;;
            *rhel*|*fedora*|*centos*|*rocky*|*alma*) echo "rhel" ;;
            *alpine*) echo "alpine" ;;
            *arch*)   echo "arch" ;;
            *suse*)   echo "suse" ;;
            *)
                local id2
                id2=$(. /etc/os-release && echo "${ID}" | tr '[:upper:]' '[:lower:]')
                case "$id2" in
                    ubuntu|debian|linuxmint) echo "debian" ;;
                    centos|rhel|fedora|rocky|almalinux) echo "rhel" ;;
                    alpine) echo "alpine" ;;
                    arch|manjaro) echo "arch" ;;
                    *) echo "unknown" ;;
                esac ;;
        esac
        return
    fi
    echo "unknown"
}

DISTRO="$(_distro)"

# Install system packages (best-effort; caller adds repo if needed)
_pkg_install() {
    local packages=("$@")
    case "$DISTRO" in
        debian)
            apt-get install -y --no-install-recommends "${packages[@]}" 2>/dev/null ;;
        rhel)
            if command -v dnf &>/dev/null; then
                dnf install -y "${packages[@]}" 2>/dev/null
            else
                yum install -y "${packages[@]}" 2>/dev/null
            fi ;;
        alpine)
            apk add --no-cache "${packages[@]}" 2>/dev/null ;;
        arch)
            pacman -Sy --noconfirm "${packages[@]}" 2>/dev/null ;;
        macos)
            if command -v brew &>/dev/null; then
                brew install "${packages[@]}" 2>/dev/null
            else
                warn "Homebrew not found; cannot auto-install: ${packages[*]}"
            fi ;;
        *)
            warn "Unknown distro; cannot auto-install: ${packages[*]}" ;;
    esac
}

# Get the codename for apt repo lines (Ubuntu/Debian)
_apt_codename() {
    if command -v lsb_release &>/dev/null; then
        lsb_release -cs 2>/dev/null
    elif [[ -f /etc/os-release ]]; then
        . /etc/os-release && echo "${VERSION_CODENAME:-${UBUNTU_CODENAME:-}}"
    fi
}

_apt_update_retry() {
    local max_attempts="${1:-3}"
    local attempt=1
    local wait_s=2

    while [[ "$attempt" -le "$max_attempts" ]]; do
        # Hard network bounds: the CI Nexus apt mirror can stall under parallel
        # load, and an unbounded apt-get update has held whole env runs inside
        # the case timeout (observed: cases stuck at "refreshing apt package
        # index" for the full 1200 s).  Failing fast lets every caller fall
        # back to its offline path.
        if timeout "${FQ_APT_NET_TIMEOUT_S:-120}" apt-get update -qq \
                -o Acquire::http::Timeout=15 -o Acquire::https::Timeout=15 \
                -o Acquire::Retries=1; then
            return 0
        fi
        warn "apt-get update failed (attempt ${attempt}/${max_attempts}); retrying in ${wait_s}s ..."
        sleep "$wait_s"
        wait_s=$(( wait_s * 2 ))
        attempt=$(( attempt + 1 ))
    done

    return 1
}

# Apt deb cache is distro-specific (Debian trixie debs ≠ Ubuntu jammy).
_apt_cache_name() {
    local _key="$1" _codename
    _codename="$(_apt_codename)"
    if [[ -n "$_codename" ]]; then
        echo "fq-apt-${_key}-${_codename}.tar.gz"
    else
        echo "fq-apt-${_key}.tar.gz"
    fi
}

_apt_cache_file() {
    local _key="$1" _name _f _d
    _name="$(_apt_cache_name "$_key")"
    _f="${FQ_TARBALL_CACHE_DIR}/${_name}"
    [[ -s "$_f" ]] && { echo "$_f"; return 0; }
    while IFS= read -r _d; do
        [[ "$_d" == "$FQ_TARBALL_CACHE_DIR" ]] && continue
        _f="${_d}/${_name}"
        [[ -s "$_f" ]] && { echo "$_f"; return 0; }
    done < <(_fq_staging_dirs)
    return 1
}

# Verify cached debs actually run on this OS (reject cross-distro cache hits).
_apt_verify_cached_install() {
    local _key="$1"; shift
    case "$_key" in
        postgis-pg*)
            # Use the extension control file as the ground truth.  The postinst for
            # postgresql-N-postgis-3 may exit non-zero when no PG cluster exists yet,
            # leaving the package in 'iF' state even though all files were installed.
            # The control file is always present once dpkg copies the package files.
            local _ver="${_key#postgis-pg}"
            _apt_postgis_core_installed "$_ver"
            ;;
        pg*)
            # Only check that the binary file exists.  Running 'postgres --version'
            # through _fq_env_clean can exit 127 when required libraries (e.g. libpq5)
            # are not yet on ldconfig's cache, even though the package IS installed.
            local _ver="${_key#pg}"
            local _pg_bin="/usr/lib/postgresql/${_ver}/bin/postgres"
            [[ -x "$_pg_bin" ]]
            ;;
        *)
            return 0
            ;;
    esac
}

# PostGIS scripts package is optional for FQ vtable tests; core extension is enough.
# Use the extension control file rather than dpkg status: the postinst may fail
# before marking 'ii' when no PG cluster exists yet, but the files are present.
_apt_postgis_core_installed() {
    local _ver="$1"
    [[ -f "/usr/share/postgresql/${_ver}/extension/postgis.control" ]]
}

_apt_packages_installed() {
    local _key="$1"; shift
    local _pkgs=("$@")
    local _p _all_ok=1

    case "$_key" in
        postgis-pg*)
            local _ver="${_key#postgis-pg}"
            _apt_postgis_core_installed "$_ver"
            return
            ;;
    esac

    for _p in "${_pkgs[@]}"; do
        dpkg -l "$_p" 2>/dev/null | grep -q "^ii" || { _all_ok=0; break; }
    done
    [[ "$_all_ok" -eq 1 ]]
}

# _apt_install_cached <cache-key> <pkg1> [pkg2 ...]
#
# Install apt packages with .deb file caching.  Mirrors the MySQL/InfluxDB
# tarball-cache pattern so any machine (container, VM, physical) downloads
# each package group exactly once; subsequent runs are fully offline.
#
# Decision tree:
#   1. dpkg -l shows all pkgs installed  → return 0 immediately (no I/O)
#   2. FQ_TARBALL_CACHE_DIR/fq-apt-<key>-<codename>.tar.gz exists
#                                        → dpkg -i from cached .deb files
#   3. Otherwise: apt-get install (network), then tar pkg + all transitive
#      deps into the cache file for future offline reuse.
_apt_install_cached() {
    local _key="$1"; shift
    local _pkgs=("$@")
    local _cache=""
    _cache="$(_apt_cache_file "$_key" || true)"

    # 1. Fast path: all packages already installed and binaries runnable.
    if _apt_packages_installed "$_key" "${_pkgs[@]}" \
            && _apt_verify_cached_install "$_key" "${_pkgs[@]}"; then
        return 0
    fi
    if _apt_packages_installed "$_key" "${_pkgs[@]}"; then
        warn "apt [${_key}]: packages listed as installed but unusable; reinstalling."
    fi

    # 2. Cache hit: install from saved .deb files — no network, no apt index.
    # Prefer compat-packages fq-apt-*.tar.gz even when Nexus apt is enabled.
    if [[ -n "$_cache" && -s "$_cache" ]]; then
        info "apt [${_key}]: installing from deb cache (offline): $(basename "$_cache") ..."
        local _tmp; _tmp="$(mktemp -d)"
        tar -xzf "$_cache" -C "$_tmp"
        DEBIAN_FRONTEND=noninteractive dpkg -i --force-depends "$_tmp"/*.deb 2>/dev/null || true
        # Configure any packages left in iU (unpacked-but-not-configured) state.
        # dpkg -i --force-depends can leave packages as 'iU'; dpkg -l then fails
        # the "^ii" grep, causing _apt_verify_cached_install to fall through to
        # the (offline-failing) network install path.
        DEBIAN_FRONTEND=noninteractive dpkg --configure -a 2>/dev/null || true
        DEBIAN_FRONTEND=noninteractive apt-get -f install -y -q --no-install-recommends 2>/dev/null || true
        rm -rf "$_tmp"
        if _apt_packages_installed "$_key" "${_pkgs[@]}" \
                && _apt_verify_cached_install "$_key" "${_pkgs[@]}"; then
            info "apt [${_key}]: installed from cache."
            return 0
        fi
        # Offline postgis cache may omit -scripts; core extension is sufficient.
        case "$_key" in
            postgis-pg*)
                local _pgver="${_key#postgis-pg}"
                if _apt_postgis_core_installed "$_pgver"; then
                    info "apt [${_key}]: postgis core installed from cache."
                    return 0
                fi
                ;;
        esac
        warn "apt [${_key}]: cache install incomplete or incompatible; retrying via apt-get."
    fi

    # 3. Network install: ensure apt index knows the packages, then install.
    if ! _apt_verify_cached_install "$_key" "${_pkgs[@]}"; then
        for _p in "${_pkgs[@]}"; do
            if dpkg -l "$_p" 2>/dev/null | grep -q "^ii"; then
                info "apt [${_key}]: removing incompatible packages before fresh install ..."
                case "$_key" in
                    postgis-pg*)
                        local _pgver="${_key#postgis-pg}"
                        DEBIAN_FRONTEND=noninteractive apt-get purge -y \
                            "postgresql-${_pgver}-postgis-3" \
                            "postgresql-${_pgver}-postgis-3-scripts" \
                            "${_pkgs[@]}" 2>/dev/null || true
                        ;;
                    pg*)
                        local _pgver="${_key#pg}"
                        DEBIAN_FRONTEND=noninteractive apt-get purge -y \
                            postgresql-common \
                            "postgresql-${_pgver}" \
                            "postgresql-client-${_pgver}" \
                            "${_pkgs[@]}" 2>/dev/null || true
                        ;;
                    *)
                        DEBIAN_FRONTEND=noninteractive apt-get remove -y "${_pkgs[@]}" 2>/dev/null || true
                        ;;
                esac
                DEBIAN_FRONTEND=noninteractive apt-get -f install -y 2>/dev/null || true
                DEBIAN_FRONTEND=noninteractive apt-get autoremove -y 2>/dev/null || true
                break
            fi
        done
    fi

    local _need_refresh=0
    for _p in "${_pkgs[@]}"; do
        apt-cache show "$_p" >/dev/null 2>&1 || { _need_refresh=1; break; }
    done
    if [[ "$_need_refresh" -eq 1 ]]; then
        info "apt [${_key}]: package not in local index, refreshing ..."
        apt-get update -qq 2>/dev/null || true
    fi

    # Download to isolated temp dir to capture pkg + all transitive deps.
    local _dl_dir; _dl_dir="$(mktemp -d)"
    DEBIAN_FRONTEND=noninteractive apt-get install -y -q \
        --no-install-recommends \
        -o Dir::Cache::archives="$_dl_dir" \
        --download-only "${_pkgs[@]}" 2>/dev/null || true

    case "$_key" in
        postgis-pg*)
            local _pgver="${_key#postgis-pg}"
            if _apt_postgis_core_installed "$_pgver"; then
                info "apt [${_key}]: postgis core already installed."
                rm -rf "$_dl_dir"
                return 0
            fi
            ;;
    esac

    if DEBIAN_FRONTEND=noninteractive apt-get install -y -q \
            --no-install-recommends "${_pkgs[@]}" 2>/dev/null; then
        # Persist downloaded .debs (pkg + all transitive deps) to cache.
        if ! _use_nexus_apt && ls "$_dl_dir"/*.deb &>/dev/null 2>&1; then
            local _cache_dir; _cache_dir="$(_fq_writable_tarball_dir "${FQ_TARBALL_CACHE_DIR}")"
            [[ -n "$_cache" ]] || _cache="${_cache_dir}/$(_apt_cache_name "$_key")"
            mkdir -p "$_cache_dir"
            tar -czf "$_cache" -C "$_dl_dir" . 2>/dev/null \
                && info "apt [${_key}]: debs cached → $(basename "$_cache")."
        fi
        rm -rf "$_dl_dir"
        return 0
    fi

    err "apt [${_key}]: apt-get install failed for: ${_pkgs[*]}"
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        "${_pkgs[@]}" 2>&1 | tail -15 >&2 || true
    rm -rf "$_dl_dir"
    return 1
}

# ──────────────────────────────────────────────────────────────────────────────
# 8.  Version → port mapping (no associative arrays for bash 3 compat; use case)
#     NOTE: We still require bash 4+ (checked at top), but keeping case-style
#     port lookup makes the code trivially backportable.
# ──────────────────────────────────────────────────────────────────────────────
mysql_port() {
    local ver="$1" tag
    tag="${ver//./}"
    local envvar="FQ_MYSQL_PORT_${tag}"
    local envval="${!envvar:-}"
    if [[ -n "$envval" ]]; then echo "$envval"; return; fi
    case "$tag" in
        57) echo 13305 ;;
        80) echo 13306 ;;
        84) echo 13307 ;;
        *)  echo 13306 ;;
    esac
}

pg_port() {
    local ver="$1" tag
    tag="${ver//./}"
    local envvar="FQ_PG_PORT_${tag}"
    local envval="${!envvar:-}"
    if [[ -n "$envval" ]]; then echo "$envval"; return; fi
    case "$tag" in
        14) echo 15433 ;;
        15) echo 15435 ;;
        16) echo 15434 ;;
        17) echo 15436 ;;
        *)  echo 15434 ;;
    esac
}

influx_port() {
    local ver="$1" tag
    tag="${ver//./}"
    local envvar="FQ_INFLUX_PORT_${tag}"
    local envval="${!envvar:-}"
    if [[ -n "$envval" ]]; then echo "$envval"; return; fi
    case "$tag" in
        18) echo 18085 ;;  # InfluxDB 1.x
        30) echo 18086 ;;
        35) echo 18087 ;;
        *)  echo 18086 ;;
    esac
}

# ──────────────────────────────────────────────────────────────────────────────
# 9.  MySQL
# ──────────────────────────────────────────────────────────────────────────────
_mysql_tarball_url() {
    local ver="$1"
    local major minor patch glibc arch_str
    major="$(echo "$ver" | cut -d. -f1)"
    minor="$(echo "$ver" | cut -d. -f2)"
    # Pinned stable patch releases
    case "$ver" in
        5.7) patch="5.7.44"; glibc="glibc2.12" ;;
        8.0) patch="8.0.45"; glibc="glibc2.28"  ;;
        8.4) patch="8.4.5";  glibc="glibc2.28"  ;;
        *)   patch="${ver}.0"; glibc="glibc2.28" ;;
    esac
    arch_str="x86_64"
    if [[ "$ARCH" == "aarch64" || "$ARCH" == "arm64" ]]; then
        arch_str="aarch64"
    fi
    local tag="${ver//./}"
    local override="FQ_MYSQL_TARBALL_${tag}"
    local override_val="${!override:-}"
    if [[ -n "$override_val" ]]; then echo "$override_val"; return; fi
    local base="${FQ_MYSQL_MIRROR:-https://cdn.mysql.com/Downloads/MySQL-${major}.${minor}}"
    echo "${base}/mysql-${patch}-linux-${glibc}-${arch_str}.tar.xz"
}

_release_mysql_lock() {
    local lockdir="$1"
    rm -f "${lockdir}/pid" 2>/dev/null || true
    rmdir "${lockdir}" 2>/dev/null || true
}

ensure_mysql() {
    local ver="$1"
    local port; port="$(mysql_port "$ver")"
    local base="${FQ_BASE_DIR}/mysql/${ver}"
    local bin="${base}/bin"

    info "MySQL ${ver}: port=${port}, base=${base}"

    # ── concurrency guard (mkdir-based: no fd, child daemons cannot inherit) ──
    # Prevent two simultaneous ensure_mysql calls from racing (e.g. concurrent
    # pytest sessions pkill-9'ing each other's mysqld during initialization).
    # Unlike flock fd-based locking, mkdir creates no file descriptor, so the
    # background mysqld process started inside cannot inadvertently hold the lock.
    local _lockdir="/tmp/fq_mysql_${ver}.lockdir"
    # Remove a stale lock left by a hard-killed process (> 15 min old).
    if [[ -d "${_lockdir}" ]] && find "${_lockdir}" -maxdepth 0 -mmin +15 2>/dev/null | grep -q .; then
        warn "MySQL ${ver}: removing stale lock dir ${_lockdir} (> 15 min old)"
        rm -f "${_lockdir}/pid" 2>/dev/null || true
        rmdir "${_lockdir}" 2>/dev/null || true
    fi
    if ! mkdir "${_lockdir}" 2>/dev/null; then
        local _owner_pid=""
        if [[ -f "${_lockdir}/pid" ]]; then
            _owner_pid="$(cat "${_lockdir}/pid" 2>/dev/null || true)"
        fi
        if [[ -n "${_owner_pid}" ]] && ! kill -0 "${_owner_pid}" 2>/dev/null; then
            warn "MySQL ${ver}: removing stale lock ${_lockdir} (pid ${_owner_pid} is not running)."
            rm -f "${_lockdir}/pid" 2>/dev/null || true
            rmdir "${_lockdir}" 2>/dev/null || true
        fi
        if ! mkdir "${_lockdir}" 2>/dev/null; then
            err "MySQL ${ver}: another ensure_mysql is already running (${_lockdir}). Aborting to avoid race."
            return 1
        fi
    fi

    echo "$$" > "${_lockdir}/pid"
    trap "_release_mysql_lock '${_lockdir}'" EXIT
    trap "_release_mysql_lock '${_lockdir}'; exit 129" HUP
    trap "_release_mysql_lock '${_lockdir}'; exit 130" INT
    trap "_release_mysql_lock '${_lockdir}'; exit 143" TERM

    # Delegate to helper so the lock dir is always cleaned up on any return path.
    local _rc=0
    _ensure_mysql_body "$ver" "$port" "$base" "$bin" || _rc=$?
    _release_mysql_lock "${_lockdir}"
    trap - EXIT INT TERM HUP
    return $_rc
}

_ensure_mysql_body() {
    local ver="$1" port="$2" base="$3" bin="$4"

    # ── install binary if not present ────────────────────────────────────────
    if [[ ! -x "${bin}/mysqld" ]]; then
        case "$OS" in
            Darwin)
                info "MySQL ${ver}: installing via Homebrew ..."
                brew install "mysql@${ver}" 2>/dev/null \
                    || brew install mysql 2>/dev/null \
                    || { err "MySQL ${ver}: brew install failed."; OVERALL_OK=1; return 1; }
                local brew_prefix; brew_prefix="$(brew --prefix)"
                local brew_bin="${brew_prefix}/opt/mysql@${ver}/bin"
                [[ -d "$brew_bin" ]] || brew_bin="${brew_prefix}/opt/mysql/bin"
                mkdir -p "${base}/bin"
                for f in mysqld mysql mysqladmin; do
                    [[ -x "${brew_bin}/${f}" ]] && ln -sf "${brew_bin}/${f}" "${base}/bin/${f}"
                done
                ;;
            *)
                _ensure_linux_engine_deps
                info "MySQL ${ver}: resolving tarball (staged cache or download) ..."
                local url tarball
                url="$(_mysql_tarball_url "$ver")"
                tarball="$(_ensure_tarball_cached "fq-mysql-${ver}.tar.xz" "$url")"
                mkdir -p "$base"
                info "MySQL ${ver}: extracting $(basename "$tarball") ..."
                _fq_env_clean tar -xJf "$tarball" --strip-components=1 -C "$base" \
                    || { err "MySQL ${ver}: failed to extract $(basename "$tarball")"; return 1; }
                info "MySQL ${ver}: tarball extracted."
                # MySQL official tarballs use /lib64/ld-linux-x86-64.so.2 as the ELF
                # interpreter.  Some minimal CI containers (e.g. tdengine-ci) omit the
                # /lib64 symlink, causing exec to silently fail with exit=127.
                # Create the symlink proactively so mysqld can be executed.
                if [[ "$(uname -m)" == "x86_64" ]] && [[ ! -e /lib64/ld-linux-x86-64.so.2 ]]; then
                    local _ld_try
                    for _ld_try in /lib/x86_64-linux-gnu/ld-linux-x86-64.so.2 \
                                   /usr/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2; do
                        if [[ -f "$_ld_try" ]]; then
                            mkdir -p /lib64 2>/dev/null || true
                            ln -sf "$_ld_try" /lib64/ld-linux-x86-64.so.2 2>/dev/null \
                                && info "MySQL ${ver}: created /lib64/ld-linux-x86-64.so.2 → $_ld_try" \
                                && break
                        fi
                    done
                fi
                ;;
        esac
    fi

    # ── quick restart mode: only (re)start MySQL, preserve existing data ─────
    if [[ "${FQ_MYSQL_QUICK_RESTART:-0}" == "1" ]] && ! _mysql_data_ready "$base"; then
        info "MySQL ${ver}: no initialized data; performing full setup instead of quick restart."
        _mysql_reset_env "$ver" "$port" "$base"
        return $?
    fi

    if [[ "${FQ_MYSQL_QUICK_RESTART:-0}" == "1" ]]; then
        info "MySQL ${ver}: quick restart (existing data preserved) ..."
        _kill_matching "mysqld.*port=${port}" KILL
        _kill_matching "mysqld.*--port=${port}" KILL
        # Verify: all mysqld processes for this port are gone before touching files
        _wait_procs_gone "mysqld.*port=${port}" 100 \
            || warn "MySQL ${ver}: some processes may linger; continuing..."
        rm -f "${base}/run/mysqld.sock" "${base}/run/mysqld.sock.lock"
        rm -f /tmp/mysqlx.sock /tmp/mysqlx.sock.lock
        # Verify: socket files are gone (they must be absent before new start)
        [[ ! -e "${base}/run/mysqld.sock" ]] \
            || warn "MySQL ${ver}: socket file may linger; mysqld will try to remove it"
        # Wait for TCP port to be released (probe-based, no busy-spin)
        _wait_port_free "$port" 30 \
            || warn "MySQL ${ver}: port ${port} still open; continuing..."
        _mysql_start "$ver" "$port" "$base"
        if ! wait_port "$port" 90; then
            warn "MySQL ${ver}: timed out on port ${port} after quick restart; falling back to hard reset."
            _mysql_reset_env "$ver" "$port" "$base"
            return $?
        fi
        _mysql_setup_auth "$ver" "$port" "$base" || true
        # Connectivity probe: confirm MySQL is actually ready for queries
        local _qpi=0
        while [[ $_qpi -lt 20 ]]; do
            if _mysql_py_exec 127.0.0.1 "$port" "$MYSQL_USER" "$MYSQL_PASS" "SELECT 1;" >/dev/null 2>&1; then
                info "MySQL ${ver}: quick restart complete."
                return 0
            fi
            _qpi=$((_qpi + 1))
            sleep 1
        done
        warn "MySQL ${ver}: connectivity probe failed after quick restart; falling back to hard reset."
        _mysql_reset_env "$ver" "$port" "$base"
        return $?
    fi

    # ── hard reset: kill-9 → wipe data → reinit → start → probe ─────────────
    _mysql_reset_env "$ver" "$port" "$base"
}

_mysql_data_ready() {
    local base="$1"
    [[ -d "${base}/data/mysql" || -f "${base}/data/ibdata1" ]]
}

_mysql_init() {
    local ver="$1" base="$2"
    local data="${base}/data" run="${base}/run" log="${base}/log"
    local mysqld="${base}/bin/mysqld"
    mkdir -p "$data" "$run" "$log"

    # MySQL tarball bundles private libs (protobuf, etc.) under lib/private/.
    # Use inline env override so LD_LIBRARY_PATH is NOT leaked to the parent shell.
    local lib_private="${base}/lib/private"
    local _ldlp_prefix=""
    [[ -d "$lib_private" ]] && _ldlp_prefix="${lib_private}"

    # mysqld refuses to run as 'root' unless --user=root is explicit
    local user_opt="--user=${CURRENT_USER}"
    [[ "$CURRENT_USER" == "root" ]] && user_opt="--user=root"

    # Truncate (not append) init.log so a previous stuck run cannot fill disk.
    : > "${log}/init.log"

    # --initialize-insecure: root@localhost with empty password.  MySQL 8.x
    # initialization can exceed 120s in constrained CI containers.
    local init_timeout="${FQ_MYSQL_INIT_TIMEOUT_S:-300}"
    local _init_rc=0
    if [[ -n "$_ldlp_prefix" ]]; then
        # --innodb-use-native-aio=0: CI hosts run many case containers in
        # parallel and InnoDB's native AIO exhausts the host's fs.aio-max-nr
        # (io_setup EAGAIN), which aborts init/start and failed whole suites.
        _fq_env_clean env LD_LIBRARY_PATH="$_ldlp_prefix" timeout "$init_timeout" "$mysqld" --initialize-insecure \
            --innodb-use-native-aio=0 \
            --basedir="$base" \
            --datadir="$data" \
            $user_opt \
            2>"${log}/init.log" || _init_rc=$?
    else
        _fq_env_clean timeout "$init_timeout" "$mysqld" --initialize-insecure \
            --innodb-use-native-aio=0 \
            --basedir="$base" \
            --datadir="$data" \
            $user_opt \
            2>"${log}/init.log" || _init_rc=$?
    fi
    if [[ "$_init_rc" -ne 0 ]]; then
        err "MySQL ${ver}: initdb failed (exit=${_init_rc}, timeout=${init_timeout}s)"
        if [[ -s "${log}/init.log" ]]; then
            info "MySQL ${ver}: init.log tail:"
            tail -30 "${log}/init.log" | while IFS= read -r _line; do
                echo "[fq-env]   ${_line}"
            done
        else
            # exit=127 + empty init.log means the binary itself could not be exec'd
            # (e.g. missing ELF interpreter or binary not found).
            info "MySQL ${ver}: init.log is EMPTY (binary exec failed at kernel level)"
            info "MySQL ${ver}: mysqld path: ${mysqld}  exists=$([ -f "$mysqld" ] && echo yes || echo NO)"
            info "MySQL ${ver}: mysqld mode: $(ls -la "$mysqld" 2>&1)"
            info "MySQL ${ver}: /lib64: $(ls /lib64/ 2>&1 | head -5 | tr '\n' ' ')"
            local _vout
            _vout=$(_fq_env_clean env LD_LIBRARY_PATH="${_ldlp_prefix:-}" \
                       "$mysqld" --version 2>&1) \
                && info "MySQL ${ver}: --version OK: ${_vout}" \
                || info "MySQL ${ver}: --version ALSO fails: ${_vout}"
        fi
        OVERALL_OK=1
        return 1
    fi
}

_mysql_start() {
    local ver="$1" port="$2" base="$3"
    local data="${base}/data" run="${base}/run" log="${base}/log"
    local mysqld="${base}/bin/mysqld"
    local pidfile="${run}/mysqld.pid"
    local socket="${run}/mysqld.sock"
    mkdir -p "$run" "$log"

    # MySQL tarball bundles private libs (protobuf, etc.) under lib/private/.
    # Use inline env override so LD_LIBRARY_PATH is NOT leaked to the parent shell.
    local lib_private="${base}/lib/private"
    local _ldlp_prefix=""
    [[ -d "$lib_private" ]] && _ldlp_prefix="${lib_private}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

    local user_opt="--user=${CURRENT_USER}"
    [[ "$CURRENT_USER" == "root" ]] && user_opt="--user=root"

    # TLS options if certs already deployed
    local tls_args=()
    local cert_dst="${base}/certs"
    if [[ -f "${cert_dst}/ca.pem" ]]; then
        tls_args+=(
            "--ssl-ca=${cert_dst}/ca.pem"
            "--ssl-cert=${cert_dst}/server.pem"
            "--ssl-key=${cert_dst}/server-key.pem"
        )
    fi

    # Launch mysqld; inject LD_LIBRARY_PATH only into the subprocess env.
    # Fix timezone: tests assume MySQL interprets DATETIME values as CST (UTC+8).
    # Without this, a UTC container would mis-align timestamps vs TDengine epochs.
    local tz_arg="--default-time-zone=+08:00"

    if [[ -n "$_ldlp_prefix" ]]; then
        _start_daemon "$pidfile" "${log}/mysqld.log" \
            env LD_LIBRARY_PATH="$_ldlp_prefix" \
            "$mysqld" \
                --innodb-use-native-aio=0 \
                --basedir="$base" \
                --datadir="$data" \
                --port="$port" \
                --bind-address=127.0.0.1 \
                --socket="$socket" \
                --pid-file="$pidfile" \
                --log-error="${log}/error.log" \
                $user_opt \
                $tz_arg \
                "${tls_args[@]}"
    else
        _start_daemon "$pidfile" "${log}/mysqld.log" \
            "$mysqld" \
                --innodb-use-native-aio=0 \
                --basedir="$base" \
                --datadir="$data" \
                --port="$port" \
                --bind-address=127.0.0.1 \
                --socket="$socket" \
                --pid-file="$pidfile" \
                --log-error="${log}/error.log" \
                $user_opt \
                $tz_arg \
                "${tls_args[@]}"
    fi
}

_mysql_setup_auth() {
    local ver="$1" port="$2" base="$3"
    local socket="${base}/run/mysqld.sock"
    local major; major="$(echo "$ver" | cut -d. -f1)"

    # Idempotent: if password already works, skip
    if _mysql_py_exec 127.0.0.1 "$port" root "$MYSQL_PASS" "SELECT 1;" >/dev/null 2>&1; then
        info "MySQL ${ver}: auth already configured."
        return 0
    fi

    info "MySQL ${ver}: configuring root auth ..."
    local auth_sql
    if [[ "$major" -ge 8 ]]; then
        auth_sql="ALTER USER IF EXISTS 'root'@'localhost'
                    IDENTIFIED WITH mysql_native_password BY '${MYSQL_PASS}';
                  CREATE USER IF NOT EXISTS 'root'@'%'
                    IDENTIFIED WITH mysql_native_password BY '${MYSQL_PASS}';
                  GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
                  FLUSH PRIVILEGES;"
    else
        auth_sql="UPDATE mysql.user
                    SET authentication_string=PASSWORD('${MYSQL_PASS}'),
                        plugin='mysql_native_password'
                    WHERE User='root';
                  DROP USER IF EXISTS 'root'@'%';
                  CREATE USER 'root'@'%' IDENTIFIED BY '${MYSQL_PASS}';
                  GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
                  FLUSH PRIVILEGES;"
    fi

    # Retry loop: mysqld opens the TCP port before all internal startup tasks
    # finish.  wait_port() returning does NOT guarantee the server is ready for
    # SQL connections.
    local _auth_pi=0
    while [[ $_auth_pi -lt 30 ]]; do
        if _mysql_py_exec 127.0.0.1 "$port" root "" "$auth_sql" >/dev/null 2>&1; then
            info "MySQL ${ver}: auth configured via TCP (no-password)."
            return 0
        fi
        if _mysql_py_exec 127.0.0.1 "$port" root "$MYSQL_PASS" "$auth_sql" >/dev/null 2>&1; then
            info "MySQL ${ver}: auth configured via TCP."
            return 0
        fi
        _auth_pi=$((_auth_pi + 1))
        sleep 1
    done
    warn "MySQL ${ver}: could not configure auth automatically after 30s (socket=${socket})."
    # Print last 10 lines of error log to stdout so CI captures them.
    if [[ -f "${base}/log/error.log" ]]; then
        info "MySQL ${ver}: last error.log lines:"
        tail -10 "${base}/log/error.log" | while IFS= read -r _line; do
            echo "[fq-env]   $_line"
        done
    fi
    warn "MySQL ${ver}: could not configure auth automatically (socket=${socket})."
    return 1
}

_mysql_ensure_timezone_tables() {
    local ver="$1" port="$2" base="$3"
    local tzinfo_bin="${base}/bin/mysql_tzinfo_to_sql"
    if [[ ! -x "$tzinfo_bin" ]] && command -v mysql_tzinfo_to_sql >/dev/null 2>&1; then
        tzinfo_bin="$(command -v mysql_tzinfo_to_sql)"
    fi
    if [[ ! -x "$tzinfo_bin" ]] && [[ "$OS" == "Darwin" ]] && command -v brew >/dev/null 2>&1; then
        local mysql_pkg mysql_prefix
        for mysql_pkg in "mysql@${ver}" mysql-client mysql; do
            mysql_prefix="$(brew --prefix "$mysql_pkg" 2>/dev/null || true)"
            if [[ -x "${mysql_prefix}/bin/mysql_tzinfo_to_sql" ]]; then
                tzinfo_bin="${mysql_prefix}/bin/mysql_tzinfo_to_sql"
                break
            fi
        done
    fi
    if [[ ! -x "$tzinfo_bin" ]]; then
        err "MySQL ${ver}: timezone-table setup failed, mysql_tzinfo_to_sql missing: ${tzinfo_bin}"
        OVERALL_OK=1
        return 1
    fi

    local zone_name=""
    local zone_file=""
    while IFS='|' read -r zone_name zone_file; do
        [[ -n "$zone_name" ]] || continue
        if [[ ! -f "$zone_file" ]]; then
            err "MySQL ${ver}: timezone-table setup failed, zoneinfo missing: ${zone_file}"
            OVERALL_OK=1
            return 1
        fi

        local loaded=""
        loaded=$(_mysql_py_scalar 127.0.0.1 "$port" "$MYSQL_USER" "$MYSQL_PASS" \
            "SELECT COUNT(*) FROM mysql.time_zone_name WHERE Name='${zone_name}';" 2>/dev/null || true)
        if [[ "$loaded" == "1" ]]; then
            info "MySQL ${ver}: timezone ${zone_name} already loaded."
            continue
        fi

        info "MySQL ${ver}: loading timezone ${zone_name} from ${zone_file} ..."
        if ! _fq_mysql_run "$base" "$tzinfo_bin" "$zone_file" "$zone_name" \
                | _mysql_py_exec_stdin 127.0.0.1 "$port" "$MYSQL_USER" "$MYSQL_PASS" \
                    >/dev/null 2>&1; then
            err "MySQL ${ver}: failed to load timezone ${zone_name} into mysql.time_zone_* tables."
            OVERALL_OK=1
            return 1
        fi

        loaded=$(_mysql_py_scalar 127.0.0.1 "$port" "$MYSQL_USER" "$MYSQL_PASS" \
            "SELECT COUNT(*) FROM mysql.time_zone_name WHERE Name='${zone_name}';" 2>/dev/null || true)
        if [[ "$loaded" != "1" ]]; then
            err "MySQL ${ver}: timezone ${zone_name} still missing after load."
            OVERALL_OK=1
            return 1
        fi
    done <<'EOF'
Asia/Shanghai|/usr/share/zoneinfo/Asia/Shanghai
UTC|/usr/share/zoneinfo/UTC
EOF

    local conv_probe=""
    conv_probe=$(_mysql_py_scalar 127.0.0.1 "$port" "$MYSQL_USER" "$MYSQL_PASS" \
        "SELECT CONCAT_WS('|', \
            IF(CONVERT_TZ('2024-01-01 00:00:00','Asia/Shanghai','+00:00') IS NULL, 'NULL', 'OK'), \
            IF(CONVERT_TZ('2024-01-01 00:00:00','UTC','+00:00') IS NULL, 'NULL', 'OK'));" 2>/dev/null || true)
    if [[ "$conv_probe" != "OK|OK" ]]; then
        err "MySQL ${ver}: timezone conversion probe failed after tz-table load (probe=${conv_probe:-<empty>})."
        OVERALL_OK=1
        return 1
    fi

    info "MySQL ${ver}: timezone tables ready (Asia/Shanghai, UTC)."
    return 0
}

_mysql_apply_tls() {
    local ver="$1" port="$2" base="$3"
    local cert_dst="${base}/certs"
    local major; major="$(echo "$ver" | cut -d. -f1)"

    info "MySQL ${ver}: deploying TLS certificates ..."
    mkdir -p "$cert_dst"
    cp "${CERT_SRC}/ca.pem"              "${cert_dst}/ca.pem"
    cp "${CERT_SRC}/mysql/server.pem"    "${cert_dst}/server.pem"
    cp "${CERT_SRC}/mysql/client.pem"    "${cert_dst}/client.pem"

    # MySQL 8's OpenSSL binding requires traditional PKCS#1 RSA format for
    # private keys ("BEGIN RSA PRIVATE KEY").  Our PKI emits PKCS#8 format
    # ("BEGIN PRIVATE KEY").  Convert in-place so mysqld can read them.
    openssl rsa -in "${CERT_SRC}/mysql/server-key.pem" -traditional \
        -out "${cert_dst}/server-key.pem" 2>/dev/null \
        || cp "${CERT_SRC}/mysql/server-key.pem" "${cert_dst}/server-key.pem"
    openssl rsa -in "${CERT_SRC}/mysql/client-key.pem" -traditional \
        -out "${cert_dst}/client-key.pem" 2>/dev/null \
        || cp "${CERT_SRC}/mysql/client-key.pem" "${cert_dst}/client-key.pem"

    chmod 640 "${cert_dst}/server-key.pem" "${cert_dst}/client-key.pem"
    chmod 644 "${cert_dst}/ca.pem" "${cert_dst}/server.pem" "${cert_dst}/client.pem"

    if [[ "$major" -ge 8 ]]; then
        # MySQL 8: persist the SSL paths and hot-reload the TLS context.
        # ALTER INSTANCE RELOAD TLS avoids a full mysqld restart.
        if _mysql_py_exec 127.0.0.1 "$port" "$MYSQL_USER" "$MYSQL_PASS" \
                "SET PERSIST ssl_ca='${cert_dst}/ca.pem';
                 SET PERSIST ssl_cert='${cert_dst}/server.pem';
                 SET PERSIST ssl_key='${cert_dst}/server-key.pem';" \
            >/dev/null 2>&1; then
            info "MySQL ${ver}: TLS SET PERSIST applied."
        else
            warn "MySQL ${ver}: SET PERSIST failed."
        fi

        if _mysql_py_exec 127.0.0.1 "$port" "$MYSQL_USER" "$MYSQL_PASS" \
                "ALTER INSTANCE RELOAD TLS;" \
            >/dev/null 2>&1; then
            info "MySQL ${ver}: TLS hot-reload succeeded."
            return 0
        fi

        # RELOAD TLS failed (e.g. first-time setup before cert files existed);
        # fall back to a full restart so mysqld picks up the --ssl-* arguments
        # that _mysql_start already passes when cert files are present.
        info "MySQL ${ver}: TLS hot-reload unavailable, restarting ..."
        local pidfile="${base}/run/mysqld.pid"
        _stop_daemon "$pidfile" "mysqld.*port=${port}"
        _wait_procs_gone "mysqld.*${base}" 100 || true
        rm -f "${base}/run/mysqld.sock" "${base}/run/mysqld.sock.lock"
        _wait_port_free "$port" 30 || true
        _mysql_start "$ver" "$port" "$base"

        if wait_port "$port" 30; then
            local _tls_pi=0
            while [[ $_tls_pi -lt 20 ]]; do
                if _mysql_py_exec 127.0.0.1 "$port" "$MYSQL_USER" "$MYSQL_PASS" "SELECT 1;" >/dev/null 2>&1; then
                    info "MySQL ${ver}: TLS restart complete."
                    return 0
                fi
                _tls_pi=$((_tls_pi + 1))
                sleep 1
            done
            warn "MySQL ${ver}: connectivity probe failed after TLS restart."
        else
            warn "MySQL ${ver}: did not come back after TLS restart."
        fi
    else
        # MySQL 5.7: no SET PERSIST; write option file and restart.
        cat > "${base}/my-tls.cnf" <<MYCNF
[mysqld]
ssl_ca=${cert_dst}/ca.pem
ssl_cert=${cert_dst}/server.pem
ssl_key=${cert_dst}/server-key.pem
MYCNF
        local pidfile="${base}/run/mysqld.pid"
        _stop_daemon "$pidfile" "mysqld.*port=${port}"
        _wait_procs_gone "mysqld.*${base}" 100 || true
        rm -f "${base}/run/mysqld.sock" "${base}/run/mysqld.sock.lock"
        _wait_port_free "$port" 30 || true
        _mysql_start "$ver" "$port" "$base"
        if wait_port "$port" 30; then
            local _tls_pi=0
            while [[ $_tls_pi -lt 20 ]]; do
                if _mysql_py_exec 127.0.0.1 "$port" "$MYSQL_USER" "$MYSQL_PASS" "SELECT 1;" >/dev/null 2>&1; then
                    info "MySQL ${ver}: TLS restart complete."
                    return 0
                fi
                _tls_pi=$((_tls_pi + 1))
                sleep 1
            done
            warn "MySQL ${ver}: connectivity probe failed after TLS restart."
        else
            warn "MySQL ${ver}: did not come back after TLS restart."
        fi
    fi
}

_mysql_reset_env() {
    local ver="$1" port="$2" base="$3"
    local pidfile="${base}/run/mysqld.pid"
    local log="${base}/log"

    info "MySQL ${ver} @ ${port}: hard reset (kill-9 → wipe data → reinit → restart) ..."

    # 0. Kill any Docker container binding to our target port — Docker-proxied
    #    MySQL survives pkill and holds the TCP port, blocking our local mysqld.
    if command -v docker &>/dev/null; then
        local _cid
        _cid=$(docker ps -q --filter "publish=${port}" 2>/dev/null || true)
        if [[ -n "$_cid" ]]; then
            warn "MySQL ${ver}: Docker container(s) binding port ${port} — killing: ${_cid}"
            docker rm -f $_cid >/dev/null 2>&1 || true
            sleep 1
        fi
    fi

    # 1. Kill -9 all mysqld processes on this port OR using this basedir,
    #    including any stuck --initialize-insecure.
    if [[ -f "$pidfile" ]]; then
        kill -9 "$(cat "$pidfile")" 2>/dev/null || true
        rm -f "$pidfile"
    fi
    _kill_matching "mysqld.*port=${port}" KILL
    _kill_matching "mysqld.*--port=${port}" KILL
    # Also kill ANY mysqld using this basedir (may be running on a different port)
    _kill_matching "mysqld.*${base}" KILL
    _kill_matching "mysqld.*initialize-insecure.*${base}" KILL
    _kill_matching "mysqld.*${base}.*initialize-insecure" KILL
    # Verify: wait until all mysqld processes for this base are truly gone.
    # Retry kill if processes survive the first attempt.
    if ! _wait_procs_gone "mysqld.*${base}" 100; then
        warn "MySQL ${ver}: processes survived first kill; retrying with individual SIGKILL ..."
        _list_matching_pids "mysqld.*${base}" | xargs -r kill -9 2>/dev/null || true
        if ! _wait_procs_gone "mysqld.*${base}" 100; then
            err "MySQL ${ver}: FATAL — mysqld processes still alive after two kill rounds"
            ps ax -o pid= -o command= 2>/dev/null | awk -v pat="mysqld.*${base}" '$0 ~ pat && $0 !~ /awk/ {print}' >&2 || true
            OVERALL_OK=1; return 1
        fi
    fi
    # Truncate init.log now so stale open file handles cannot keep consuming disk
    mkdir -p "${log}"
    : > "${log}/init.log"

    # Remove socket and lock files so mysqld does not refuse to start
    # ("Another process with pid N is using unix socket file")
    rm -f "${base}/run/mysqld.sock" "${base}/run/mysqld.sock.lock"
    # mysqlx plugin default socket is /tmp/mysqlx.sock; clean it too
    rm -f /tmp/mysqlx.sock /tmp/mysqlx.sock.lock 2>/dev/null || true
    # Verify: socket files must be absent before new mysqld starts
    [[ ! -e "${base}/run/mysqld.sock" ]] \
        || { err "MySQL ${ver}: cannot remove socket file"; return 1; }

    # Wait for TCP port to be released — FATAL if it remains occupied.
    if ! _wait_port_free "$port" 60; then
        # Identify what is holding the port
        err "MySQL ${ver}: FATAL — port ${port} still occupied after kill."
        ss -tlnp "sport = :${port}" 2>/dev/null >&2 || true
        OVERALL_OK=1; return 1
    fi

    # 2. Wipe data dir + reinit
    rm -rf "${base}/data"
    # Verify: data directory must be gone before initdb runs
    _verify_absent "${base}/data" "MySQL ${ver} data dir" || return 1
    _mysql_init "$ver" "$base" || return 1

    # 3. Start
    _mysql_start "$ver" "$port" "$base"
    if ! wait_port "$port" 90; then
        err "MySQL ${ver}: timed out on port ${port} after reset."
        tail -20 "${log}/error.log" 2>/dev/null >&2 || true
        OVERALL_OK=1; return 1
    fi

    # 3a. Verify OUR mysqld is the process listening on the port (not Docker or other)
    local _listening_pid
    _listening_pid=$(ss -tlnp "sport = :${port}" 2>/dev/null | sed -nE 's/.*pid=([0-9]+).*/\1/p' | head -1)
    if [[ -n "$_listening_pid" ]]; then
        local _listening_cmd
        _listening_cmd=$(tr '\0' ' ' < "/proc/${_listening_pid}/cmdline" 2>/dev/null || echo "")
        if [[ "$_listening_cmd" != *"${base}"* ]]; then
            err "MySQL ${ver}: port ${port} answered but by unexpected process (pid=${_listening_pid}): ${_listening_cmd}"
            OVERALL_OK=1; return 1
        fi
    fi

    # 4. Auth + TLS setup
    _mysql_setup_auth "$ver" "$port" "$base" || { OVERALL_OK=1; return 1; }
    _mysql_ensure_timezone_tables "$ver" "$port" "$base" || { OVERALL_OK=1; return 1; }
    _mysql_apply_tls  "$ver" "$port" "$base"

    # 5. Create test users (tls_user, pool test user)
    local pool_user="${FQ_POOL_TEST_USER:-fq_pool_test}"
    local pool_pass="${FQ_POOL_TEST_PASS:-taosdata}"
    local pool_max_conn="${FQ_POOL_TEST_MAX_CONN:-1}"
    local user_sql
    user_sql="$(printf "DROP USER IF EXISTS 'tls_user'@'%%';\n\
CREATE USER 'tls_user'@'%%' IDENTIFIED BY 'tls_pwd' REQUIRE SSL;\n\
GRANT ALL PRIVILEGES ON *.* TO 'tls_user'@'%%';\n\
DROP USER IF EXISTS '%s'@'%%';\n\
CREATE USER '%s'@'%%' IDENTIFIED BY '%s' WITH MAX_USER_CONNECTIONS %s;\n\
GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%';\n\
FLUSH PRIVILEGES;" \
        "$pool_user" "$pool_user" "$pool_pass" "$pool_max_conn" "$pool_user")"
    _mysql_py_exec 127.0.0.1 "$port" "$MYSQL_USER" "$MYSQL_PASS" "$user_sql" >/dev/null 2>&1 \
        || warn "MySQL ${ver}: test user setup had warnings."

    # 6. Connectivity probe (actual SQL connection, with pause between retries)
    local _pi=0
    while [[ $_pi -lt 30 ]]; do
        if _mysql_py_exec 127.0.0.1 "$port" "$MYSQL_USER" "$MYSQL_PASS" "SELECT 1;" >/dev/null 2>&1; then
            info "MySQL ${ver} @ ${port}: reset complete."
            return 0
        fi
        _pi=$((_pi + 1))
        sleep 1
    done
    err "MySQL ${ver}: connectivity probe failed after reset."
    OVERALL_OK=1; return 1
}

# ──────────────────────────────────────────────────────────────────────────────
# 10.  PostgreSQL
# ──────────────────────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------
# SQL Server (native process; byte-identical engine build to the former
# container image so every recorded remote-side expectation still holds).
#
# Microsoft ships the engine as deb/rpm packages only, so extract the package
# contents under ${FQ_BASE_DIR}/sqlserver/<ver> and run
# ${base}/server/opt/mssql/bin/sqlservr directly — the same model the other
# three engines use.  sqlcmd comes from the mssql-tools18 package extracted
# next to it; it loads the ODBC driver that travels with the dependency
# package (DS 3.3: no system odbcinst registration required).
# ---------------------------------------------------------------------------
SQLSERVER_PKG_VERSION="${FQ_SQLSERVER_PKG_VERSION:-16.0.4295.3}"
SQLSERVER_SERVER_URL="${FQ_SQLSERVER_SERVER_URL:-https://packages.microsoft.com/ubuntu/22.04/mssql-server-2022/pool/main/m/mssql-server/mssql-server_${SQLSERVER_PKG_VERSION}-1_amd64.deb}"
SQLSERVER_TOOLS_URL="${FQ_SQLSERVER_TOOLS_URL:-https://packages.microsoft.com/ubuntu/22.04/prod/pool/main/m/mssql-tools18/mssql-tools18_18.6.1.1-1_amd64.deb}"
SQLSERVER_OPENLDAP_URL="${FQ_SQLSERVER_OPENLDAP_URL:-http://archive.ubuntu.com/ubuntu/pool/main/o/openldap/libldap-2.5-0_2.5.20+dfsg-0ubuntu0.22.04.1_amd64.deb}"

# CI keeps every engine installer in a host package cache (compat-packages,
# mounted read-only as /usr/local/src inside the case containers) and the other
# three engines resolve their tarballs from there, so no case container ever
# touches the public network for them.  SQL Server must follow the same order:
# without it every single case downloads the ~276 MB server deb from
# packages.microsoft.com, which is slow enough to exhaust the per-case timeout
# and fails outright on network hiccups (mass "engine not reachable" and
# 1200s timeouts across all ext-source suites in CI).
# Resolution order per package:
#   1. FQ_SQLSERVER_PKG_CACHE — files left by an earlier install
#   2. staged bundle fq-sqlserver-<ver>.tar.gz, or the individual .deb files,
#      in FQ_TARBALL_CACHE_DIR / FQ_STAGING_DIR / /usr/local/src /
#      /data0/compat-packages
#   3. internal mirror registry (same generic package as msodbcsql18-*.tar.gz)
#   4. upstream public URL (last resort)
FQ_SQLSERVER_MIRROR_URL="${FQ_SQLSERVER_MIRROR_URL:-${FQ_MIRROR_URL:-https://git.tdengine.net/api/v4/projects/70/packages/generic/externals/latest}}"
FQ_SQLSERVER_BUNDLE_NAME="${FQ_SQLSERVER_BUNDLE_NAME:-fq-sqlserver-${SQLSERVER_PKG_VERSION}.tar.gz}"

_sqlserver_stage_dirs() {
    local seen="" d
    for d in "$(_fq_writable_tarball_dir "${FQ_TARBALL_CACHE_DIR}" 2>/dev/null)" $(_fq_staging_dirs); do
        [[ -n "${d}" && -d "${d}" ]] || continue
        [[ " ${seen} " == *" ${d} "* ]] && continue
        seen+=" ${d}"
        printf '%s\n' "${d}"
    done
}

# Extract a staged bundle into the package cache when one is available.
_sqlserver_stage_bundle() {
    local cache="$1" d f
    while IFS= read -r d; do
        for f in "${d}/${FQ_SQLSERVER_BUNDLE_NAME}" "${d}"/fq-sqlserver-*.tar.gz; do
            [[ -s "${f}" ]] || continue
            if tar -xzf "${f}" -C "${cache}" 2>/dev/null; then
                info "SQL Server: extracted staged bundle $(basename "${f}")"
                return 0
            fi
        done
    done < <(_sqlserver_stage_dirs)
    return 1
}

# Staged file → internal mirror → upstream public URL.  The public host is
# deliberately last: it is the one that made CI unusable.
_sqlserver_resolve_pkg() {
    local dest="$1" url="$2" base d f
    [[ -s "${dest}" ]] && return 0
    base="$(basename "${url}")"
    while IFS= read -r d; do
        f="${d}/${base}"
        [[ -s "${f}" ]] || continue
        if cp -f "${f}" "${dest}" 2>/dev/null || ln -sf "${f}" "${dest}" 2>/dev/null; then
            info "SQL Server: using staged ${base}"
            return 0
        fi
    done < <(_sqlserver_stage_dirs)
    if [[ "${FQ_SQLSERVER_USE_MIRROR:-1}" == "1" ]] \
            && curl -fsI --connect-timeout 8 --max-time 20 "${FQ_SQLSERVER_MIRROR_URL}/${base}" >/dev/null 2>&1; then
        _download_with_retry "${FQ_SQLSERVER_MIRROR_URL}/${base}" "${dest}" 2 && return 0
        warn "SQL Server: mirror fetch of ${base} failed; falling back to upstream."
    fi
    _download_with_retry "${url}" "${dest}"
}

_sqlserver_extract_deb() {
    local deb="$1" dir="$2"
    mkdir -p "${dir}"
    if command -v dpkg-deb >/dev/null 2>&1; then
        dpkg-deb -x "${deb}" "${dir}"
    else
        ( cd "${dir}" && ar x "${deb}" && tar -xf data.tar.* )
    fi
}

_sqlserver_install() {
    local ver="$1" base="$2"
    local cache="${FQ_SQLSERVER_PKG_CACHE:-${FQ_BASE_DIR}/sqlserver/packages}"
    mkdir -p "${cache}"
    local srv="${cache}/$(basename "${SQLSERVER_SERVER_URL}")"
    local tools="${cache}/$(basename "${SQLSERVER_TOOLS_URL}")"
    local ldap="${cache}/$(basename "${SQLSERVER_OPENLDAP_URL}")"
    if [[ ! -s "${srv}" || ! -s "${tools}" || ! -s "${ldap}" ]]; then
        _sqlserver_stage_bundle "${cache}" || true
    fi
    [[ -s "${srv}" ]]   || _sqlserver_resolve_pkg "${srv}"   "${SQLSERVER_SERVER_URL}"   || return 1
    [[ -s "${tools}" ]] || _sqlserver_resolve_pkg "${tools}" "${SQLSERVER_TOOLS_URL}"   || return 1
    [[ -s "${ldap}" ]]  || _sqlserver_resolve_pkg "${ldap}"  "${SQLSERVER_OPENLDAP_URL}" || return 1
    info "SQL Server ${ver}: extracting packages ..."
    _sqlserver_extract_deb "${srv}"   "${base}/server" || return 1
    _sqlserver_extract_deb "${tools}" "${base}/tools"  || return 1
    _sqlserver_extract_deb "${ldap}"  "${base}/server/deps" || return 1
    [[ -x "${base}/server/opt/mssql/bin/sqlservr" ]] || return 1
}

# Runtime-lib directories that carry the staged provider libraries
# (libodbc*/libmsodbcsql*).  Candidates in order: FQ_SQLSERVER_LIB_DIR (the
# caller's resolved driver dir), the build lib dir next to TAOS_BIN_PATH
# (fallback <repo>/debug/build/lib), any LD_LIBRARY_PATH entry, and
# FQ_BASE_DIR/sqlserver/lib.
_sqlserver_lib_dirs() {
    # Candidate directories that may hold the staged ODBC runtime
    # (libmsodbcsql18 + its unixODBC driver manager).  In CI the driver only
    # exists in the build tree's lib dir, so TAOS_BIN_PATH alone is not
    # enough: the framework passes the dir it resolved via
    # FQ_SQLSERVER_LIB_DIR / LD_LIBRARY_PATH.
    local bin="${TAOS_BIN_PATH:-}"
    if [[ -z "${bin}" ]]; then
        local root; root="$(cd "${SCRIPT_DIR}/../../../../.." 2>/dev/null && pwd -P)"
        bin="${root}/debug/build/bin"
    fi
    local cands=()
    [[ -n "${FQ_SQLSERVER_LIB_DIR:-}" ]] && cands+=("${FQ_SQLSERVER_LIB_DIR}")
    cands+=("$(dirname "${bin}")/lib")
    local ld IFS_OLD="${IFS}"
    IFS=':'
    for ld in ${LD_LIBRARY_PATH:-}; do
        [[ -n "${ld}" ]] && cands+=("${ld}")
    done
    IFS="${IFS_OLD}"
    cands+=("${FQ_BASE_DIR}/sqlserver/lib")
    local d out=""
    for d in "${cands[@]}"; do
        [[ -n "${d}" && -d "${d}" ]] || continue
        case ":${out}:" in *":${d}:"*) continue ;; esac
        out="${out:+${out}:}${d}"
    done
    [[ -z "${out}" ]] || printf '%s\n' "${out}" | tr ':' '\n'
}

_sqlserver_driver_path() {
    local lib hit
    while IFS= read -r lib; do
        hit="$(ls "${lib}"/libmsodbcsql-18.*.so* 2>/dev/null | head -1)"
        if [[ -n "${hit}" ]]; then
            printf '%s\n' "${hit}"
            return 0
        fi
    done < <(_sqlserver_lib_dirs)
    return 1
}

# Private ODBC registration for sqlcmd: point it at the driver shipped with
# the dependency package (no system odbcinst registration, DS 3.3).
_sqlserver_odbc_conf() {
    local conf_dir="${FQ_BASE_DIR}/sqlserver/odbc"
    mkdir -p "${conf_dir}"
    local cur=""
    [[ -s "${conf_dir}/odbcinst.ini" ]] && \
        cur="$(sed -n 's/^Driver=//p' "${conf_dir}/odbcinst.ini" | head -1)"
    # Rewrite when absent — or stale: a recorded Driver path that no longer
    # exists makes the driver manager fail the handle allocation (observed as
    # "Can't open lib ... file not found" / HENV errors) even though a valid
    # driver is staged elsewhere.
    if [[ -z "${cur}" || ! -e "${cur}" ]]; then
        local hit
        hit="$(_sqlserver_driver_path)" || true
        if [[ -n "${hit}" ]]; then
            printf '[ODBC Driver 18 for SQL Server]\nDriver=%s\nUsageCount=1\n' \
                "${hit}" > "${conf_dir}/odbcinst.ini"
        fi
    fi
    echo "${conf_dir}"
}

_sqlserver_run_sql() {
    local ver="$1" db="$2" sql="$3"
    # Lookup failures must be loud: silently returning 1 here made the whole
    # sqlcmd path (including --sqlserver-exec callers) fail with an empty
    # message, which is undiagnosable from CI logs.
    local cmd
    if ! cmd="$(sqlserver_sqlcmd_path "${ver}")"; then
        echo "sqlcmd not found for SQL Server ${ver} (searched under $(sqlserver_base "${ver}")/tools)" >&2
        return 1
    fi
    local port; port="$(sqlserver_port "${ver}")"
    local odbc_dir; odbc_dir="$(_sqlserver_odbc_conf)"
    local driver
    if ! driver="$(_sqlserver_driver_path)"; then
        echo "libmsodbcsql18 (ODBC Driver 18) not found; searched: $(_sqlserver_lib_dirs | tr '\n' ' ')" >&2
        return 1
    fi
    local lib_dir; lib_dir="$(dirname "${driver}")"
    # sqlcmd itself loads libodbc* from the same staged directory.
    LD_LIBRARY_PATH="${lib_dir}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}" \
        ODBCSYSINI="${odbc_dir}" "${cmd}" -S "127.0.0.1,${port}" \
        -U "${SQLSERVER_USER}" -P "${SQLSERVER_PASS}" -C -b -d "${db}" -Q "${sql}"
}

_sqlserver_start() {
    local ver="$1"
    local base; base="$(sqlserver_base "${ver}")"
    local deps; deps="$(sqlserver_deps_dir "${ver}")"
    # Cap the engine and disable its memory-pressure self-shutdown.  CI hosts
    # run many case containers at once; with no cgroup limit the engine sizes
    # its caches to the *host's* memory and then either gets OOM-killed or
    # shuts itself down under pressure seconds after becoming ready (observed
    # as "engine not reachable" right after a successful probe).  The FQ test
    # workload needs very little; 2 GB is ample.
    ACCEPT_EULA=Y MSSQL_PID=Developer MSSQL_SA_PASSWORD="${SQLSERVER_PASS}" \
        MSSQL_TCP_PORT="$(sqlserver_port "${ver}")" \
        MSSQL_MEMORY_LIMIT_MB="${FQ_SQLSERVER_MEMORYLIMIT_MB:-2048}" \
        MSSQL_DISABLE_MEMORY_PRESSURE=1 \
        LD_LIBRARY_PATH="${deps}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}" \
        setsid nohup "${base}/server/opt/mssql/bin/sqlservr" \
        >"${base}/sqlservr.log" 2>&1 &
}

_sqlserver_stop_procs() {
    local ver="$1" sig="${2:-TERM}"
    _kill_matching "$(sqlserver_bin "${ver}")" "${sig}"
}

# True when the engine process for this version is alive.
_sqlserver_running() {
    local bin; bin="$(sqlserver_bin "$1")"
    pgrep -f "${bin}" >/dev/null 2>&1
}

# Any sqlservr whose executable is not under this version's base is an orphan
# from an earlier run (a scratch validation directory, another payload).  The
# engine always keeps its system databases in the fixed /var/opt/mssql path, so
# a foreign instance owns them and makes our instance fail to bootstrap
# ("BootstrapSystemDataDirectories() failure ... Access is denied") — observed
# in practice after a validation run left an engine behind.  Scan /proc rather
# than pgrep -f: the path may contain pattern characters and pgrep would also
# match the launcher shell.
_sqlserver_foreign_pids() {
    local ver="$1"
    local base; base="$(sqlserver_base "${ver}")"
    local dir exe pid
    for dir in /proc/[0-9]*; do
        pid="${dir#/proc/}"
        exe="$(readlink -f "${dir}/exe" 2>/dev/null)" || continue
        exe="${exe% (deleted)}"
        [[ "$(basename "${exe}")" == "sqlservr" ]] || continue
        case "${exe}" in
            "${base}/"*) continue ;;   # our own instance
        esac
        printf '%s\n' "${pid}"
    done
}

# Terminate orphan instances so they release /var/opt/mssql before we start.
_sqlserver_kill_foreign() {
    local ver="$1"
    local pids; pids="$(_sqlserver_foreign_pids "${ver}")"
    [[ -n "${pids}" ]] || return 0
    warn "SQL Server ${ver}: terminating orphan sqlservr process(es) from another path: $(printf '%s ' ${pids})"
    local pid i alive
    for pid in ${pids}; do kill -TERM "${pid}" 2>/dev/null || true; done
    for i in $(seq 1 20); do
        alive=""
        for pid in ${pids}; do
            kill -0 "${pid}" 2>/dev/null && alive=1
        done
        [[ -z "${alive}" ]] && break
        sleep 0.5
    done
    for pid in ${pids}; do kill -KILL "${pid}" 2>/dev/null || true; done
    # Let the kernel release the data-directory locks before we (re)start.
    sleep 1
}

# Diagnosability: dump the engine log tail and any unresolved shared libraries
# so a failing environment is analysable from the test log alone (the container
# that ran the engine no longer exists afterwards).
_sqlserver_log_failure() {
    local ver="$1" base="$2"
    err "SQL Server ${ver}: probe failed after start (see ${base}/sqlservr.log)"
    local _line
    if [[ -f "${base}/sqlservr.log" ]]; then
        while IFS= read -r _line; do err "  [sqlservr] ${_line}"; done \
            < <(tail -n 30 "${base}/sqlservr.log")
    fi
    local _missing
    _missing="$(ldd "$(sqlserver_bin "${ver}")" 2>/dev/null | grep "not found" || true)"
    [[ -n "${_missing}" ]] && while IFS= read -r _line; do err "  [ldd] ${_line}"; done <<< "${_missing}"
}

# Engine runtime dependencies.  Microsoft's packages target Ubuntu; other
# distributions (the CI test image is Debian-based) may lack some of the
# libraries they declare.  Install them tolerantly, one package at a time,
# so one unknown name cannot abort the batch.
_sqlserver_ensure_deps() {
    command -v apt-get >/dev/null 2>&1 || return 0
    [[ "$(id -u)" -eq 0 ]] || return 0
    local pkg
    for pkg in libc++1 libc++abi1 libsss-nss-idmap0 libsasl2-2 libgssapi-krb5-2; do
        DEBIAN_FRONTEND=noninteractive apt-get install -y -q \
            --no-install-recommends "${pkg}" >/dev/null 2>&1 \
            || warn "SQL Server: runtime package ${pkg} not available (continuing)"
    done
}

ensure_sqlserver() {
    local ver="$1"
    local base; base="$(sqlserver_base "${ver}")"

    info "SQL Server ${ver}: port=$(sqlserver_port "${ver}"), base=${base}"

    # SQL Server for Linux ships x86-64 binaries only.  On other architectures
    # the engine can only run under emulation, which needs the x86-64 ELF
    # interpreter plus a translation layer (rosetta/qemu).  Fail fast without
    # them instead of burning the probe timeout on an engine that can never
    # start ("rosetta error: failed to open elf at /lib64/ld-linux-x86-64.so.2").
    if [[ "$(uname -m)" != "x86_64" ]] \
       && [[ ! -e /lib64/ld-linux-x86-64.so.2 ]] \
       && ! command -v qemu-x86_64 >/dev/null 2>&1 \
       && ! command -v qemu-x86_64-static >/dev/null 2>&1; then
        err "SQL Server ${ver}: engine cannot run on $(uname -m) without an x86-64 interpreter/emulator; skipping."
        return 1
    fi

    if [[ ! -x "${base}/server/opt/mssql/bin/sqlservr" ]]; then
        _sqlserver_install "${ver}" "${base}" \
            || { err "SQL Server ${ver}: package install failed"; return 1; }
    fi
    _sqlserver_ensure_deps

    # Orphans from an earlier run (any path outside this version's base) hold
    # the fixed /var/opt/mssql directory; clear them before any (re)start.
    _sqlserver_kill_foreign "${ver}"

    # ── quick restart mode: (re)start the engine, preserve user databases ────
    # The engine must be STARTED before probing: a disruption test stops it and
    # then expects the same databases to come back.  Probing a stopped engine
    # would fall through to the hard reset (which drops every user database) and
    # break the recovery assertions.
    if [[ "${FQ_SQLSERVER_QUICK_RESTART:-0}" == "1" ]]; then
        info "SQL Server ${ver}: quick restart (existing data preserved) ..."
        if ! _sqlserver_running "${ver}"; then
            _sqlserver_start "${ver}"
        fi
        if _sqlserver_probe "${ver}"; then
            info "SQL Server ${ver}: quick restart complete."
            return 0
        fi
        warn "SQL Server ${ver}: quick restart probe failed; falling back to hard reset."
    fi

    # ── legacy cleanup: a container from an older run may still hold the port ─
    # (same guard as _mysql_reset_env: docker-proxied listeners survive pkill).
    if command -v docker >/dev/null 2>&1; then
        local _port _cid
        _port="$(sqlserver_port "${ver}")"
        _cid=$(docker ps -q --filter "publish=${_port}" 2>/dev/null || true)
        if [[ -n "${_cid}" ]]; then
            warn "SQL Server ${ver}: Docker container(s) binding port ${_port} — killing: ${_cid}"
            docker rm -f ${_cid} >/dev/null 2>&1 || true
            sleep 1
        fi
    fi

    # ── hard reset: restart the engine and drop every user database ──────────
    # The caller only needs a clean *data* state; dropping the databases is the
    # process-mode equivalent of the other engines' data-directory wipe.
    info "SQL Server ${ver}: hard reset (restart engine → drop user databases)..."
    _sqlserver_stop_procs "${ver}" KILL
    _sqlserver_start "${ver}"
    if ! _sqlserver_probe "${ver}"; then
        _sqlserver_log_failure "${ver}" "${base}"
        # A foreign instance that was holding the fixed /var/opt/mssql
        # directory (killed above) or a half-wiped directory leaves the
        # bootstrap failing with "Access is denied".  Wipe and retry once so a
        # dirty host cannot fail the environment.
        if grep -qE "BootstrapSystemDataDirectories|Access is denied|0x80070005" \
                "${base}/sqlservr.log" 2>/dev/null; then
            warn "SQL Server ${ver}: bootstrap failure (dirty /var/opt/mssql); wiping and retrying once."
            _sqlserver_kill_foreign "${ver}"
            _sqlserver_stop_procs "${ver}" KILL
            rm -rf /var/opt/mssql
            _sqlserver_start "${ver}"
            if ! _sqlserver_probe "${ver}"; then
                _sqlserver_log_failure "${ver}" "${base}"
                return 1
            fi
        else
            return 1
        fi
    fi
    _sqlserver_drop_user_dbs "${ver}" || warn "SQL Server ${ver}: could not wipe user databases"
    info "SQL Server ${ver}: reset complete."
}

# Drop every user database (system databases have database_id <= 4) so the next
# test file starts from a clean slate.
_sqlserver_drop_user_dbs() {
    local ver="$1"
    _sqlserver_run_sql "${ver}" master \
        "DECLARE @sql NVARCHAR(MAX) = N''; SELECT @sql += N'ALTER DATABASE [' + name + N'] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [' + name + N'];' FROM sys.databases WHERE database_id > 4; EXEC sp_executesql @sql;" \
        >/dev/null 2>&1
}

# Wait for the engine to accept logins and run a probe query.
_sqlserver_probe() {
    local ver="$1"
    local port; port="$(sqlserver_port "${ver}")"
    local i=0 err=""

    while [[ ${i} -lt 120 ]]; do
        if port_open "${port}"; then
            if err="$(_sqlserver_run_sql "${ver}" master "SELECT 1" 2>&1 >/dev/null)"; then
                return 0
            fi
        fi
        i=$((i + 1))
        sleep 1
    done
    # Failure is tolerated for SQL Server only (main() keeps MySQL/PG/Influx
    # coverage alive), so record *why* for the CI log — otherwise the cause is
    # invisible until a test trips over it much later.
    [[ -z "${err}" ]] || warn "SQL Server ${ver}: probe failed: ${err}"
    return 1
}

ensure_pg() {
    local ver="$1"
    local port; port="$(pg_port "$ver")"
    local base="${FQ_BASE_DIR}/pg/${ver}"
    local bin="${base}/bin"

    info "PostgreSQL ${ver}: port=${port}, base=${base}"

    # ── install binary if not present ────────────────────────────────────────
    if [[ ! -x "${bin}/pg_ctl" ]]; then
        _pg_install "$ver" "$base" || return 1
    fi

    # ── ensure PostGIS extension package is installed ──────────────────────────
    # Required for FQ-03 type-mapping tests (CASE-036: GEOMETRY type).
    # Idempotent: skips silently if the package is already installed.
    # Fatal: if the install fails, ensure_pg() fails so dependent tests also fail.
    _pg_install_postgis "$ver" || return 1

    # ── quick restart mode: only (re)start PG, preserve existing data ────────
    if [[ "${FQ_PG_QUICK_RESTART:-0}" == "1" ]]; then
        info "PostgreSQL ${ver}: quick restart (existing data preserved) ..."
        # Kill any stale postgres process before starting fresh
        _kill_matching "postgres.*-p ${port}" KILL
        _kill_matching "postgres.*${base}" KILL
        # Verify: wait until all postgres processes for this base are truly gone
        _wait_procs_gone "postgres.*${base}" 100 \
            || warn "PostgreSQL ${ver}: some processes may linger; continuing..."
        # Remove socket/lock files and stale pid file (left by SIGKILL)
        rm -f "/tmp/.s.PGSQL.${port}" "/tmp/.s.PGSQL.${port}.lock"
        rm -f "${base}/data/postmaster.pid"
        # Remove POSIX shared memory segments left by SIGKILL (prevents
        # "another server might be running" error on restart)
        rm -f /dev/shm/PostgreSQL.* 2>/dev/null || true
        # Wait for TCP port to be released (probe-based, no busy-spin)
        _wait_port_free "$port" 30 \
            || warn "PostgreSQL ${ver}: port ${port} still open; continuing..."
        _pg_start "$ver" "$port" "$base"
        if ! wait_port "$port" 90; then
            warn "PostgreSQL ${ver}: timed out on port ${port} after quick restart; falling back to hard reset."
            _pg_reset_env "$ver" "$port" "$base"
            return $?
        fi
        # Connectivity probe: confirm PG is actually ready for queries
        local _qpi=0
        local _qpsql="${base}/bin/psql"
        while [[ $_qpi -lt 20 ]]; do
            if PGPASSWORD="$PG_PASS" PGCONNECT_TIMEOUT=3 "$_qpsql" \
                    -h 127.0.0.1 -p "$port" -U "$PG_USER" -d postgres \
                    -c "SELECT 1;" >/dev/null 2>&1; then
                info "PostgreSQL ${ver}: quick restart complete."
                return 0
            fi
            _qpi=$((_qpi + 1))
            sleep 1
        done
        warn "PostgreSQL ${ver}: connectivity probe failed after quick restart; falling back to hard reset."
        _pg_reset_env "$ver" "$port" "$base"
        return $?
    fi

    # ── hard reset: kill-9 → clean locks → wipe data → reinit → start → probe
    _pg_reset_env "$ver" "$port" "$base"
}

_pg_pgdg_share_dir() {
    echo "${FQ_PGDG_SHARE_DIR:-/usr/share/postgresql-common/pgdg}"
}

_pg_pgdg_sources_dir() {
    echo "${FQ_PGDG_SOURCES_DIR:-/etc/apt/sources.list.d}"
}

_pg_pgdg_sources_file() {
    echo "$(_pg_pgdg_sources_dir)/pgdg.sources"
}

_pg_pgdg_legacy_list_file() {
    echo "$(_pg_pgdg_sources_dir)/pgdg.list"
}

_pg_pgdg_key_file() {
    echo "$(_pg_pgdg_share_dir)/apt.postgresql.org.asc"
}

_pg_system_bin_dir() {
    local ver="$1"
    local root="${FQ_PG_SYSTEM_BIN_ROOT:-/usr/lib/postgresql}"
    echo "${root}/${ver}/bin"
}

_pg_configure_pgdg_repo() {
    local codename="$1"
    local share_dir; share_dir="$(_pg_pgdg_share_dir)"
    local sources_dir; sources_dir="$(_pg_pgdg_sources_dir)"
    local sources_file; sources_file="$(_pg_pgdg_sources_file)"
    local legacy_file; legacy_file="$(_pg_pgdg_legacy_list_file)"
    local key_file; key_file="$(_pg_pgdg_key_file)"
    local arch

    arch="$(dpkg --print-architecture 2>/dev/null || echo amd64)"

    install -d "$share_dir" "$sources_dir"
    rm -f "$legacy_file"

    curl -fsSL "https://www.postgresql.org/media/keys/ACCC4CF8.asc" \
        -o "$key_file"

    local pg_apt_base="https://apt.postgresql.org/pub/repos/apt"
    if _use_nexus_apt; then
        local _apt_base="${FQ_APT_MIRROR_BASE:-https://nexus.tdengine.net/repository}"
        pg_apt_base="${FQ_APT_PG_MIRROR:-${_apt_base%/}/apt-postgresql-org}"
    fi

    cat > "$sources_file" <<EOF
Types: deb deb-src
URIs: ${pg_apt_base}
Suites: ${codename}-pgdg
Architectures: ${arch}
Components: main
Signed-By: ${key_file}
EOF
}

_pg_install() {
    local ver="$1" base="$2"
    mkdir -p "$base"

    case "$OS" in
        Darwin)
            info "PostgreSQL ${ver}: installing via Homebrew ..."
            brew install "postgresql@${ver}" 2>/dev/null \
                || { err "PostgreSQL ${ver}: brew install failed."; OVERALL_OK=1; return 1; }
            local brew_prefix; brew_prefix="$(brew --prefix)"
            local brew_bin="${brew_prefix}/opt/postgresql@${ver}/bin"
            mkdir -p "${base}/bin"
            for f in pg_ctl initdb psql postgres createdb dropdb pg_config; do
                [[ -x "${brew_bin}/${f}" ]] && ln -sf "${brew_bin}/${f}" "${base}/bin/${f}"
            done
            return 0
            ;;
        Linux)
            if command -v apt-get &>/dev/null; then
                local _pg_cache_key="pg${ver//./}"
                local _pg_deb_cache=""
                _pg_deb_cache="$(_apt_cache_file "$_pg_cache_key" || true)"
                # Check if version available in default apt cache
                if [[ -z "$_pg_deb_cache" ]] \
                        && ! apt-cache show "postgresql-${ver}" &>/dev/null 2>&1; then
                    info "PostgreSQL ${ver}: adding PGDG apt repository ..."
                    _pkg_install curl ca-certificates gnupg
                    local codename; codename="$(_apt_codename)"
                    if [[ -z "$codename" ]]; then
                        warn "Cannot determine apt codename; PGDG repo may fail."
                        codename="jammy"
                    fi
                    _pg_configure_pgdg_repo "$codename"
                    apt-get update -qq
                fi
                info "PostgreSQL ${ver}: installing via apt ..."
                _apt_install_cached "pg${ver//./}" "postgresql-${ver}" \
                    || warn "PostgreSQL ${ver}: apt install failed; trying tarball fallback."
                local sys_bin
                sys_bin="$(_pg_system_bin_dir "$ver")"
                if [[ -d "$sys_bin" ]]; then
                    mkdir -p "${base}/bin"
                    # ln -sfn works on Linux; on macOS use individual links
                    ln -sfn "${sys_bin}"/* "${base}/bin/" 2>/dev/null || \
                        for f in pg_ctl initdb psql postgres createdb dropdb pg_config; do
                            [[ -x "${sys_bin}/${f}" ]] && ln -sf "${sys_bin}/${f}" "${base}/bin/${f}"
                        done
                    return 0
                fi
            elif command -v dnf &>/dev/null || command -v yum &>/dev/null; then
                info "PostgreSQL ${ver}: installing via dnf/yum ..."
                # Add PGDG RPM repository
                local rpm_url="https://download.postgresql.org/pub/repos/yum/reporpms/EL-$(rpm -E %{rhel})-x86_64/pgdg-redhat-repo-latest.noarch.rpm"
                rpm -q pgdg-redhat-repo &>/dev/null || \
                    (command -v dnf &>/dev/null && dnf install -y "$rpm_url" || yum install -y "$rpm_url") 2>/dev/null || true
                _pkg_install "postgresql${ver}-server" "postgresql${ver}"
                local sys_bin="/usr/pgsql-${ver}/bin"
                if [[ -d "$sys_bin" ]]; then
                    mkdir -p "${base}/bin"
                    for f in pg_ctl initdb psql postgres createdb dropdb pg_config; do
                        [[ -x "${sys_bin}/${f}" ]] && ln -sf "${sys_bin}/${f}" "${base}/bin/${f}"
                    done
                    return 0
                fi
            elif command -v apk &>/dev/null; then
                info "PostgreSQL ${ver}: installing via apk ..."
                _pkg_install "postgresql${ver}" "postgresql${ver}-client"
                local sys_bin="/usr/libexec/postgresql${ver}"
                [[ -d "$sys_bin" ]] || sys_bin="/usr/bin"
                mkdir -p "${base}/bin"
                for f in pg_ctl initdb psql postgres pg_config; do
                    [[ -x "${sys_bin}/${f}" ]] && ln -sf "${sys_bin}/${f}" "${base}/bin/${f}"
                done
                return 0
            fi
            ;;
    esac

    # Last resort: prebuilt tarball via FQ_PG_TARBALL_<ver-nodots>
    local tag="${ver//./}"
    local tarball_var="FQ_PG_TARBALL_${tag}"
    local url="${!tarball_var:-}"
    if [[ -z "$url" ]]; then
        err "PostgreSQL ${ver}: could not install via pkg manager and FQ_PG_TARBALL_${tag} not set."
        OVERALL_OK=1; return 1
    fi
    local tarball="${FQ_TARBALL_CACHE_DIR}/fq-pg-${ver}.tar.bz2"
    [[ -s "$tarball" ]] || _download_with_retry "$url" "$tarball"
    mkdir -p "$base"
    _fq_env_clean tar -xjf "$tarball" --strip-components=1 -C "$base"
}

_pg_init() {
    local ver="$1" base="$2"
    local data="${base}/data" log="${base}/log"
    local initdb="${base}/bin/initdb"
    mkdir -p "$data" "$log"

    # System-installed initdb refuses to run as root.
    # When we are root, create/use a dedicated 'fqtest' OS user for PG.
    local pg_os_user="${CURRENT_USER}"
    if [[ "$CURRENT_USER" == "root" ]]; then
        pg_os_user="postgres"
        # Create system postgres user if missing (non-fatal if it exists)
        id "$pg_os_user" &>/dev/null || useradd -r -s /bin/false "$pg_os_user" 2>/dev/null || true
        chown -R "${pg_os_user}" "$data" "$log" 2>/dev/null || true
    fi

    # --pwfile avoids leaking password in process list;
    # use a temp file instead of process substitution for portability
    local pwfile; pwfile="$(mktemp)"
    echo "$PG_PASS" > "$pwfile"
    chmod 644 "$pwfile"

    # Truncate (not append) initdb.log so a previous stuck run cannot fill disk.
    : > "${log}/initdb.log"

    # --auth trust: force trust authentication for all local/TCP connections
    # so that pg_hba.conf is deterministic regardless of the PG binary's
    # compiled default (PG ≤13 defaults trust; PG 14+ defaults scram-sha-256).
    # FQ-06-002 relies on PG rejecting a completely non-existent role even
    # in trust mode (FATAL: role does not exist → SQLSTATE 28000 → EXT_AUTH_FAILED).
    local initdb_cmd=("$initdb" -D "$data" -U "$PG_USER" --pwfile="$pwfile" --auth trust --encoding=UTF8 --locale=C)
    # timeout 120s: if initdb hangs, abort and return an error.
    if [[ "$CURRENT_USER" == "root" ]]; then
        _fq_env_clean su -s /bin/sh "$pg_os_user" -c "timeout 120 ${initdb_cmd[*]}" \
            2>"${log}/initdb.log" \
            || { err "PostgreSQL ${ver}: initdb failed (exit=$?); check ${log}/initdb.log"; rm -f "$pwfile"; OVERALL_OK=1; return 1; }
    else
        _fq_env_clean timeout 120 "${initdb_cmd[@]}" \
            2>"${log}/initdb.log" \
            || { err "PostgreSQL ${ver}: initdb failed (exit=$?); check ${log}/initdb.log"; rm -f "$pwfile"; OVERALL_OK=1; return 1; }
    fi
    rm -f "$pwfile"
}

# Install the PostGIS extension package for a given PostgreSQL major version.
# Called from ensure_pg() unconditionally; idempotent (checks before installing).
# Fatal for callers that require PostGIS to be present.
# Required by FQ-03 type-mapping tests (CASE-036: GEOMETRY type mapping).
_pg_install_postgis() {
    local ver="$1"

    case "$OS" in
        Darwin)
            local _pg_config="$(_pg_config_path "$ver")"
            local _pg_ext_dir; _pg_ext_dir="$("$_pg_config" --sharedir)/extension"
            local _pg_lib_dir; _pg_lib_dir="$("$_pg_config" --pkglibdir)"
            if [[ -f "${_pg_ext_dir}/postgis.control" && -f "${_pg_lib_dir}/postgis-3.dylib" ]]; then
                info "PostgreSQL ${ver}: PostGIS already installed for this PostgreSQL runtime."
                return 0
            fi
            _pg_build_postgis_darwin "$ver" "$_pg_config"
            if [[ -f "${_pg_ext_dir}/postgis.control" && -f "${_pg_lib_dir}/postgis-3.dylib" ]]; then
                info "PostgreSQL ${ver}: PostGIS installed for this PostgreSQL runtime."
                return 0
            fi
            err "PostgreSQL ${ver}: PostGIS install did not produce ${_pg_ext_dir}/postgis.control and ${_pg_lib_dir}/postgis-3.dylib."
            OVERALL_OK=1
            return 1
            ;;
        Linux)
            ;;
        *)
            return 0
            ;;
    esac

    if command -v apt-get &>/dev/null; then
        info "PostgreSQL ${ver}: installing PostGIS via apt ..."
        if ! _apt_install_cached "postgis-pg${ver}" \
                "postgresql-${ver}-postgis-3" \
                "postgresql-${ver}-postgis-3-scripts"; then
            warn "PostgreSQL ${ver}: PostGIS apt install failed. Tests using PostGIS GEOMETRY type (e.g. CASE-036) will fail."
            return 1
        fi
        info "PostgreSQL ${ver}: PostGIS installed."
    elif command -v dnf &>/dev/null || command -v yum &>/dev/null; then
        local _pkgmgr; command -v dnf &>/dev/null && _pkgmgr="dnf" || _pkgmgr="yum"
        # PGDG naming: postgis<postgis-major><postgis-minor>_<pg-ver>
        # e.g. postgis34_16 (PostGIS 3.4 for PG 16)
        local _pg_nodots="${ver//./}"
        local _installed=0
        for _pver in 34 33 32; do
            if "$_pkgmgr" install -y --quiet "postgis${_pver}_${_pg_nodots}" 2>/dev/null; then
                info "PostgreSQL ${ver}: PostGIS ${_pver} installed via ${_pkgmgr}."
                _installed=1
                break
            fi
        done
        if [[ "$_installed" -ne 1 ]]; then
            warn "PostgreSQL ${ver}: PostGIS install via ${_pkgmgr} failed. Tests using PostGIS GEOMETRY type (e.g. CASE-036) will fail."
            return 1
        fi
    elif command -v apk &>/dev/null; then
        if apk info postgis &>/dev/null 2>&1; then
            info "PostgreSQL ${ver}: PostGIS already installed (apk)."
            return 0
        fi
        info "PostgreSQL ${ver}: installing PostGIS via apk ..."
        if ! apk add --no-cache postgis 2>/dev/null; then
            warn "PostgreSQL ${ver}: PostGIS apk install failed. Tests using PostGIS GEOMETRY type (e.g. CASE-036) will fail."
            return 1
        fi
    fi
}

_pg_config_path() {
    local ver="$1"
    local base="${FQ_BASE_DIR}/pg/${ver}"
    if [[ -x "${base}/bin/pg_config" ]]; then
        printf '%s\n' "${base}/bin/pg_config"
        return 0
    fi
    if command -v brew >/dev/null 2>&1; then
        local brew_prefix; brew_prefix="$(brew --prefix)"
        if [[ -x "${brew_prefix}/opt/postgresql@${ver}/bin/pg_config" ]]; then
            printf '%s\n' "${brew_prefix}/opt/postgresql@${ver}/bin/pg_config"
            return 0
        fi
    fi
    if command -v pg_config >/dev/null 2>&1; then
        command -v pg_config
        return 0
    fi
    err "PostgreSQL ${ver}: pg_config not found."
    return 1
}

_brew_prefix_or_install() {
    local pkg="$1"
    if ! command -v brew >/dev/null 2>&1; then
        err "Homebrew not found; cannot install ${pkg}."
        return 1
    fi
    if brew list --versions "$pkg" >/dev/null 2>&1; then
        local prefix; prefix="$(brew --prefix "$pkg")"
        if [[ -d "$prefix" ]]; then
            printf '%s\n' "$prefix"
            return 0
        fi
        warn "Homebrew package ${pkg} is listed but ${prefix} is missing; reinstalling."
        brew reinstall "$pkg" >/dev/null 2>&1 || return 1
        brew --prefix "$pkg"
        return 0
    fi
    brew install "$pkg" >/dev/null 2>&1 || return 1
    brew --prefix "$pkg"
}

_pg_build_postgis_darwin() {
    local ver="$1" pg_config="$2"
    info "PostgreSQL ${ver}: building PostGIS ${FQ_POSTGIS_VERSION} for this PostgreSQL runtime ..."

    local cache_dir; cache_dir="$(_fq_writable_tarball_dir "${FQ_TARBALL_CACHE_DIR}")"
    local tarball="${cache_dir}/postgis-${FQ_POSTGIS_VERSION}.tar.gz"
    if [[ -s "$tarball" ]] && ! tar -tzf "$tarball" >/dev/null 2>&1; then
        warn "PostGIS tarball cache is invalid; re-downloading: ${tarball}"
        rm -f "$tarball"
    fi
    if [[ ! -s "$tarball" ]]; then
        _download_with_retry "$FQ_POSTGIS_URL" "$tarball" 2 \
            || { rm -f "$tarball"; _download_with_retry "$FQ_POSTGIS_FALLBACK_URL" "$tarball" 2; }
    fi
    tar -tzf "$tarball" >/dev/null 2>&1 \
        || { err "PostGIS tarball is not a valid gzip archive: ${tarball}"; return 1; }

    local proj_prefix json_prefix protobuf_prefix libpq_prefix gettext_prefix
    local pkgconf_prefix autoconf_prefix automake_prefix libtool_prefix
    proj_prefix="$(_brew_prefix_or_install proj)" || return 1
    json_prefix="$(_brew_prefix_or_install json-c)" || return 1
    protobuf_prefix="$(_brew_prefix_or_install protobuf-c)" || return 1
    libpq_prefix="$(_brew_prefix_or_install libpq)" || return 1
    gettext_prefix="$(_brew_prefix_or_install gettext)" || return 1
    pkgconf_prefix="$(_brew_prefix_or_install pkgconf)" || return 1
    _brew_prefix_or_install geos >/dev/null || return 1
    _brew_prefix_or_install gdal >/dev/null || return 1
    _brew_prefix_or_install sfcgal >/dev/null || return 1
    autoconf_prefix="$(_brew_prefix_or_install autoconf)" || return 1
    automake_prefix="$(_brew_prefix_or_install automake)" || return 1
    libtool_prefix="$(_brew_prefix_or_install libtool)" || return 1

    local build_root="${FQ_BASE_DIR}/deps/postgis-${FQ_POSTGIS_VERSION}-pg${ver}"
    local src_dir="${build_root}/src"
    local install_dir="${FQ_BASE_DIR}/pg/${ver}/postgis"
    rm -rf "$build_root"
    mkdir -p "$src_dir" "${install_dir}/bin" "${install_dir}/doc" "${install_dir}/man"
    _fq_env_clean tar -xzf "$tarball" --strip-components=1 -C "$src_dir"

    local pg_bindir pg_sharedir pg_pkglibdir jobs
    pg_bindir="$("$pg_config" --bindir)"
    pg_sharedir="$("$pg_config" --sharedir)"
    pg_pkglibdir="$("$pg_config" --pkglibdir)"
    jobs="${FQ_POSTGIS_BUILD_JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)}"
    [[ "$jobs" =~ ^[0-9]+$ ]] || jobs=4

    ln -sf "${pg_bindir}/postgres" "${install_dir}/bin/postgres"
    (
        cd "$src_dir"
        export PATH="${gettext_prefix}/bin:${pkgconf_prefix}/bin:${autoconf_prefix}/bin:${automake_prefix}/bin:${libtool_prefix}/bin:${PATH}"
        if [[ ! -x ./configure ]]; then
            if [[ -x ./autogen.sh ]]; then
                ./autogen.sh
            elif [[ -f ./autogen.sh ]]; then
                sh ./autogen.sh
            else
                err "PostGIS source has no configure or autogen.sh."
                exit 1
            fi
        fi
        CPPFLAGS="${CPPFLAGS:-} -I${protobuf_prefix}/include -I${gettext_prefix}/include" \
        LDFLAGS="${LDFLAGS:-} -L${protobuf_prefix}/lib -L${gettext_prefix}/lib" \
        CXXFLAGS="${CXXFLAGS:-} -std=c++17" \
        ./configure \
            "--prefix=${install_dir}" \
            "--with-pgconfig=${pg_config}" \
            "--with-projdir=${proj_prefix}" \
            "--with-jsondir=${json_prefix}" \
            "--with-protobufdir=${protobuf_prefix}" \
            || exit 1
        make -j"$jobs" \
            "PGSQL_FE_CPPFLAGS=-I${libpq_prefix}/include" \
            "PGSQL_FE_LDFLAGS=-L${libpq_prefix}/lib -lpq" \
            || exit 1
        make install \
            "bindir=${install_dir}/bin" \
            "docdir=${install_dir}/doc" \
            "mandir=${install_dir}/man" \
            "pkglibdir=${pg_pkglibdir}" \
            "datadir=${pg_sharedir}" \
            "PG_SHAREDIR=${pg_sharedir}" \
            || exit 1
    )
}

_pg_start() {
    local ver="$1" port="$2" base="$3"
    local data="${base}/data" log="${base}/log"
    local pg_ctl="${base}/bin/pg_ctl"
    mkdir -p "$log"

    # Apply TLS config if certs already present
    local cert_dst="${base}/data/certs"
    _pg_write_base_conf "$data"
    if [[ -d "$cert_dst" ]]; then
        _pg_write_ssl_conf "$data" "$cert_dst"
    fi

    # When running as root, pg_ctl refuses to start postgres.
    # Use 'su' to run pg_ctl as the system postgres user.
    if [[ "$CURRENT_USER" == "root" ]]; then
        local pg_os_user="postgres"
        chown -R "${pg_os_user}" "$data" "$log" 2>/dev/null || true
        # Build a shell-safe command string for su -c
        local start_cmd="${pg_ctl} -D ${data} -l ${log}/pg.log -o '-p ${port} -k /tmp' start"
        su -s /bin/sh "$pg_os_user" -c "$start_cmd" \
            2>>"${log}/pg_ctl.log" || true
    else
        "$pg_ctl" -D "$data" -l "${log}/pg.log" \
            -o "-p ${port} -k /tmp" \
            start 2>>"${log}/pg_ctl.log" || true
    fi
}

_pg_write_base_conf() {
    local data="$1"
    local conf="${data}/postgresql.conf"
    local tmp="${conf}.tmp.$$"

    [[ -f "$conf" ]] || return 0
    awk -v tz="$PG_TIMEZONE" '
        BEGIN { done = 0 }
        /^[[:space:]]*timezone[[:space:]]*=/ {
            print "timezone = \047" tz "\047"
            done = 1
            next
        }
        { print }
        END {
            if (!done) {
                print "timezone = \047" tz "\047"
            }
        }
    ' "$conf" > "$tmp" && mv "$tmp" "$conf"
}

_pg_write_ssl_conf() {
    local data="$1" cert_dst="$2"
    local conf="${data}/postgresql.conf"
    local hba="${data}/pg_hba.conf"
    # Idempotent for SSL-specific config.
    grep -q "^ssl = on" "$conf" 2>/dev/null && return
    cat >> "$conf" <<PGCONF
ssl = on
ssl_ca_file = '${cert_dst}/ca.pem'
ssl_cert_file = '${cert_dst}/server.pem'
ssl_key_file = '${cert_dst}/server.key'
PGCONF
    grep -q "hostssl.*cert" "$hba" 2>/dev/null || \
        printf '\nhostssl all all 0.0.0.0/0 cert clientcert=verify-full\n' >> "$hba"
}

_pg_reset_env() {
    local ver="$1" port="$2" base="$3"
    local data="${base}/data" log="${base}/log"
    local psql="${base}/bin/psql"

    info "PostgreSQL ${ver} @ ${port}: hard reset (kill-9 → wipe data → reinit → restart) ..."

    # 1. Kill -9 all postgres processes on this port / data dir,
    #    including any stuck initdb process from a previous failed reset.
    if [[ -f "${data}/postmaster.pid" ]]; then
        local _pg_pid; _pg_pid="$(head -1 "${data}/postmaster.pid" 2>/dev/null || true)"
        [[ -n "$_pg_pid" ]] && kill -9 "$_pg_pid" 2>/dev/null || true
    fi
    _kill_matching "postgres.*-p ${port}" KILL
    _kill_matching "postgres.*${base}" KILL
    _kill_matching "initdb.*${base}" KILL
    # Verify: wait until all postgres/initdb processes for this base are truly gone
    _wait_procs_gone "postgres.*${base}" 100 \
        || warn "PostgreSQL ${ver}: some processes may linger; continuing..."
    _wait_procs_gone "initdb.*${base}" 50 || true
    # Truncate initdb.log now so stale open file handles cannot keep consuming disk
    mkdir -p "${log}"
    : > "${log}/initdb.log"

    # Remove socket/lock files and shared-memory segments from previous run
    rm -f "/tmp/.s.PGSQL.${port}" "/tmp/.s.PGSQL.${port}.lock"
    rm -f /dev/shm/PostgreSQL.* 2>/dev/null || true
    # Verify: socket files must be absent before new postgres starts
    [[ ! -e "/tmp/.s.PGSQL.${port}" ]] \
        || { err "PostgreSQL ${ver}: cannot remove socket file"; return 1; }

    # Wait for TCP port to be released (probe-based, no busy-spin)
    _wait_port_free "$port" 30 \
        || warn "PostgreSQL ${ver}: port ${port} still open; continuing..."

    # 2. Wipe data dir + reinit
    rm -rf "$data"
    # Verify: data directory must be gone before initdb runs
    _verify_absent "$data" "PostgreSQL ${ver} data dir" || return 1
    mkdir -p "$log"
    _pg_init "$ver" "$base" || return 1

    # 3. Start
    _pg_start "$ver" "$port" "$base"
    if ! wait_port "$port" 90; then
        err "PostgreSQL ${ver}: timed out on port ${port} after reset."
        tail -20 "${log}/pg.log" 2>/dev/null >&2 || true
        OVERALL_OK=1; return 1
    fi

    # 4. Deploy TLS certs + write ssl config + reload
    local cert_dst="${data}/certs"
    info "PostgreSQL ${ver}: deploying TLS certificates ..."
    mkdir -p "$cert_dst"
    cp "${CERT_SRC}/ca.pem"              "${cert_dst}/ca.pem"
    cp "${CERT_SRC}/pg/server.pem"       "${cert_dst}/server.pem"
    cp "${CERT_SRC}/pg/server.key"       "${cert_dst}/server.key"
    cp "${CERT_SRC}/pg/client.pem"       "${cert_dst}/client.pem"
    cp "${CERT_SRC}/pg/client-key.pem"   "${cert_dst}/client-key.pem"
    chmod 600 "${cert_dst}/server.key" "${cert_dst}/client-key.pem"
    # Ensure cert files are owned by the postgres OS user so PG can read them
    if [[ "$CURRENT_USER" == "root" ]]; then
        chown -R postgres:postgres "${cert_dst}" 2>/dev/null || true
    fi
    _pg_write_ssl_conf "$data" "$cert_dst"
    PGPASSWORD="$PG_PASS" "$psql" -h 127.0.0.1 -p "$port" -U "$PG_USER" \
        -d postgres -c "SELECT pg_reload_conf();" >/dev/null 2>&1 || true

    # 5. Connectivity probe (actual psql connection, with pause between retries)
    local _pi=0
    while [[ $_pi -lt 30 ]]; do
        if PGPASSWORD="$PG_PASS" PGCONNECT_TIMEOUT=3 "$psql" \
                -h 127.0.0.1 -p "$port" -U "$PG_USER" -d postgres \
                -c "SELECT 1;" >/dev/null 2>&1; then
            info "PostgreSQL ${ver} @ ${port}: reset complete."
            return 0
        fi
        _pi=$((_pi + 1))
        sleep 1
    done
    err "PostgreSQL ${ver}: connectivity probe failed after reset."
    OVERALL_OK=1; return 1
}

# ──────────────────────────────────────────────────────────────────────────────
# 11.  InfluxDB v3
# ──────────────────────────────────────────────────────────────────────────────
_influx_binary_url() {
    local ver="$1"
    local tag="${ver//./}"
    local override="FQ_INFLUX_TARBALL_${tag}"
    local override_val="${!override:-}"
    if [[ -n "$override_val" ]]; then echo "$override_val"; return; fi

    local patch arch_str

    # InfluxDB 1.x — completely different binary name and URL format
    case "$ver" in
        1.*)
            case "$ver" in
                1.8) patch="1.8.10" ;;
                *)   patch="${ver}.0" ;;
            esac
            case "${OS}-${ARCH}" in
                Linux-x86_64)  arch_str="linux_amd64" ;;
                Linux-aarch64) arch_str="linux_arm64" ;;
                Darwin-x86_64) arch_str="darwin_amd64" ;;
                Darwin-arm64)  arch_str="darwin_arm64" ;;
                *)             arch_str="linux_amd64" ;;
            esac
            local base_v1="${FQ_INFLUX_MIRROR:-https://dl.influxdata.com/influxdb/releases}"
            echo "${base_v1}/influxdb-${patch}_${arch_str}.tar.gz"
            return ;;
    esac

    # InfluxDB 3.x — Map logical version to pinned stable patch releases
    # Note: v3.0.0 was never released on dl.influxdata.com; earliest is 3.0.1
    case "$ver" in
        3.0) patch="3.0.3" ;;
        3.5) patch="3.4.0" ;;
        *)   patch="${ver}.0" ;;
    esac

    # Platform-specific naming (dl.influxdata.com convention)
    case "${OS}-${ARCH}" in
        Linux-x86_64)   arch_str="linux_amd64" ;;
        Linux-aarch64)  arch_str="linux_arm64" ;;
        Darwin-x86_64)  arch_str="darwin_amd64" ;;
        Darwin-arm64)   arch_str="darwin_arm64" ;;
        *)              arch_str="linux_amd64" ;;
    esac

    local base="${FQ_INFLUX_MIRROR:-https://dl.influxdata.com/influxdb/releases}"
    echo "${base}/influxdb3-core-${patch}_${arch_str}.tar.gz"
}

ensure_influx() {
    local ver="$1"
    local port; port="$(influx_port "$ver")"
    local base="${FQ_BASE_DIR}/influxdb/${ver}"
    local bin="${base}/bin"
    local log="${base}/log"

    info "InfluxDB ${ver}: port=${port}, base=${base}"

    # FQ_INFLUX_QUICK_RESTART=1: restart in-place without wiping the data dir.
    # Used by start_influx_instance() for in-test stop/start cycles so that
    # the double-restart (start → _influx_reset_env kill → wipe → restart)
    # is avoided.  The IOx catalog may accumulate entries across many calls
    # but a single test-suite run stays well within the 2000-entry limit.
    if [[ "${FQ_INFLUX_QUICK_RESTART:-0}" == "1" ]]; then
        info "InfluxDB ${ver}: quick restart (existing data preserved) ..."
        # Kill any stale process before starting fresh
        local pidfile="${base}/run/influxd.pid"
        if [[ -f "$pidfile" ]]; then
            kill -9 "$(cat "$pidfile")" 2>/dev/null || true
            rm -f "$pidfile"
        fi
        _kill_matching "influxdb3 serve.*${port}" KILL
        # Verify: wait until all influxdb3 processes for this port are truly gone
        _wait_procs_gone "influxdb3 serve.*${port}" 100 \
            || warn "InfluxDB ${ver}: process may linger; continuing..."
        # Wait for TCP port to be released (probe-based, no busy-spin)
        _wait_port_free "$port" 30 \
            || warn "InfluxDB ${ver}: port ${port} still open; continuing..."
        _influx_start "$ver" "$port" "$base"
        if ! wait_port "$port" 90; then
            warn "InfluxDB ${ver}: timed out on port ${port} after quick restart; falling back to hard reset."
            unset FQ_INFLUX_QUICK_RESTART
            _influx_reset_env "$ver" "$port" "$base"
            return $?
        fi
        # HTTP health probe: confirm InfluxDB is actually serving requests.
        # Accept 200 with pass/ok body OR 401 (auth enabled — server is up).
        local _qi2=0
        while [[ $_qi2 -lt 30 ]]; do
            local _qhc
            _qhc="$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "http://127.0.0.1:${port}/health" 2>/dev/null || echo "000")"
            if [[ "$_qhc" == "401" ]] || { [[ "$_qhc" == "200" ]] && curl -sf --max-time 3 "http://127.0.0.1:${port}/health" 2>/dev/null \
                    | grep -qiE '"status":"(pass|ok)"|^OK$'; }; then
                _influx_ensure_token "$ver" "$port" "$base"
                info "InfluxDB ${ver}: quick restart complete."
                return 0
            fi
            _qi2=$((_qi2+1))
            sleep 1
        done
        # Health endpoint may not exist on all InfluxDB 3 builds; port open is sufficient
        warn "InfluxDB ${ver}: /health did not return pass/ok after quick restart; falling back to hard reset."
        unset FQ_INFLUX_QUICK_RESTART
        _influx_reset_env "$ver" "$port" "$base"
        return $?
    fi

    if port_open "$port"; then
        # Port open — always do a full hard reset (kill → wipe data → restart).
        # run_warm_test.sh captures the new admin token from admin_token.txt
        # BEFORE launching pytest, so _I_TOKEN at Python import time is always
        # the current token.  Soft reset is intentionally not used here: it was
        # fragile (CLI stdin hijacking) and unnecessary when the token is passed
        # in correctly via the FQ_INFLUX_TOKEN env var.
        info "InfluxDB ${ver}: port ${port} open — hard reset (kill → wipe → restart)."
        _influx_reset_env "$ver" "$port" "$base"
        return 0
    fi

    if [[ -x "${bin}/influxdb3" ]] || [[ -x "${bin}/influxd" ]]; then
        # Binary found but not running — hard reset handles start + wipe + token.
        info "InfluxDB ${ver}: installation found — hard reset (wipe → start)."
        _influx_reset_env "$ver" "$port" "$base"
        return 0
    fi

    _influx_install "$ver" "$base"
    _influx_start   "$ver" "$port" "$base"

    if ! wait_port "$port" 120; then
        err "InfluxDB ${ver}: timed out on port ${port}."
        tail -20 "${log}/influxd.log" 2>/dev/null >&2 || true
        OVERALL_OK=1; return 1
    fi

    # Health check
    local deadline=$(( SECONDS + 30 ))
    until curl -sf --max-time 3 \
               "http://127.0.0.1:${port}/health" 2>/dev/null \
               | grep -qiE '"status":"(pass|ok)"|^OK$'; do
        if [[ "$SECONDS" -gt "$deadline" ]]; then
            warn "InfluxDB ${ver}: health endpoint not passing (non-fatal)."
            break
        fi
        sleep 2
    done

    _influx_reset_env "$ver" "$port" "$base"
    info "InfluxDB ${ver}: ready."
}

_influx_install() {
    local ver="$1" base="$2"
    local url; url="$(_influx_binary_url "$ver")"
    local tarball="${FQ_TARBALL_CACHE_DIR}/fq-influxdb-${ver}.tar.gz"

    # macOS: try Homebrew first
    if [[ "$OS" == "Darwin" ]] && command -v brew &>/dev/null; then
        info "InfluxDB ${ver}: trying Homebrew ..."
        brew install influxdb 2>/dev/null || true
    fi

    mkdir -p "${base}/bin" "${base}/data" "${base}/log"
    [[ -s "$tarball" ]] || _download_with_retry "$url" "$tarball"

    # Strip top-level directory if present
    local top; top="$(tar -tzf "$tarball" 2>/dev/null | head -1 | cut -d/ -f1)"
    if [[ -n "$top" && "$top" != "influxdb3" && "$top" != "influxd" ]]; then
        tar -xzf "$tarball" --strip-components=1 -C "${base}/bin" 2>/dev/null || \
            tar -xzf "$tarball" -C "${base}/bin" 2>/dev/null || true
    else
        tar -xzf "$tarball" -C "${base}/bin" 2>/dev/null || true
    fi

    # Promote nested binaries to bin/
    find "${base}/bin" -mindepth 2 \( -name "influxdb3" -o -name "influxd" \) 2>/dev/null | \
        while read -r b; do mv -n "$b" "${base}/bin/" 2>/dev/null || true; done
    chmod +x "${base}/bin/influxdb3" "${base}/bin/influxd" 2>/dev/null || true
}

_influx3_soft_reset() {
    # Drop all databases in an already-running influxdb3 without wiping the
    # catalog or token.  This keeps the admin token stable across test file
    # invocations within the same warm container session, which is critical
    # because Python test modules read the token at import time.
    #
    # The IOx catalog is append-only: each create/drop adds an entry.  Soft
    # reset accumulates entries across runs, but a single CI session stays well
    # within the 2000-entry limit.  A full hard reset is only needed when the
    # container itself is restarted (start_warm.sh calls ensure_ext_env.sh
    # before any tests run, so the token is established before Python imports).
    local ver="$1" port="$2" base="$3" token="$4"

    local influxd
    influxd="$(find "${base}/bin" -name "influxdb3" 2>/dev/null | head -1)"
    if [[ -z "$influxd" ]]; then
        warn "InfluxDB ${ver}: no binary found for soft reset; falling back to hard reset."
        _influx_reset_env "$ver" "$port" "$base"
        return
    fi

    # List all user databases via HTTP API and delete each one.
    # NOTE: We intentionally use the HTTP API instead of `influxdb3 delete
    # database` CLI because the CLI always prompts for interactive confirmation
    # and cannot be used non-interactively.  The HTTP DELETE API requires no
    # confirmation and returns 200/204 on success, 404 if already gone.
    local api_url="http://127.0.0.1:${port}/api/v3/configure/database"
    local auth_hdr="Authorization: Bearer ${token}"
    local db_count=0

    # Retry the list API up to 5 times (1 s apart) to tolerate InfluxDB being
    # slow to become ready right after a restart.  We capture BOTH the HTTP
    # status code and the response body so we can distinguish:
    #   • HTTP 200 + body "[]"  → server is up, zero user databases  (OK)
    #   • HTTP 200 + body "[…]" → server is up, databases to delete  (OK)
    #   • HTTP 4xx/5xx          → auth or server error               (warn, return)
    #   • empty body + no code  → connection refused / timeout       (retry)
    local dbs_json _list_http _list_attempt
    for _list_attempt in 1 2 3 4 5; do
        _list_http="$(curl -s -o /tmp/_influx_list_$$.json \
            -w '%{http_code}' --max-time 5 \
            "${api_url}?format=json" \
            -H "${auth_hdr}" 2>/dev/null || echo "000")"
        if [[ "$_list_http" == "200" ]]; then
            dbs_json="$(cat /tmp/_influx_list_$$.json 2>/dev/null || true)"
            rm -f /tmp/_influx_list_$$.json
            break
        fi
        rm -f /tmp/_influx_list_$$.json
        if [[ "$_list_http" =~ ^[45] ]]; then
            warn "InfluxDB ${ver}: list databases returned HTTP ${_list_http}; cannot soft reset."
            return 1
        fi
        # Connection refused / timeout (000) — wait and retry.
        sleep 1
    done

    if [[ -z "$dbs_json" ]]; then
        warn "InfluxDB ${ver}: list databases did not return HTTP 200 after 5 attempts; cannot soft reset."
        return 1
    fi

    local dbs
    dbs="$(python3 -c "
import json, sys
try:
    dbs = json.loads(sys.argv[1])
    for d in dbs:
        name = d.get('iox::database', '')
        if name and name != '_internal':
            print(name)
except Exception:
    pass
" "$dbs_json" 2>/dev/null || true)"

    while IFS= read -r db; do
        [[ -z "$db" ]] && continue
        curl -sf -X DELETE "${api_url}" \
            -H "${auth_hdr}" \
            -G --data-urlencode "db=${db}" \
            --max-time 5 \
            -o /dev/null 2>/dev/null || true
        db_count=$((db_count + 1))
    done <<< "$dbs"

    info "InfluxDB ${ver}: soft reset complete (${db_count} databases dropped, token preserved)."
}

_influx_ensure_token() {
    # Create an admin token via the influxdb3 CLI and write it to
    # ${base}/admin_token.txt so Python tests can read it via
    # federated_query_common.py → INFLUX_TOKEN → _I_TOKEN.
    #
    # influxdb3 3.x runs in auth-enabled mode (no --without-auth): tokens are
    # stored in the catalog and are random (apiv3_...).  We create one admin
    # token per fresh data directory.  The token file is wiped on hard reset
    # (along with the data dir), so we always create a fresh token after restart.
    #
    # In the test_fq_01 baseline comparison, apiv3_... tokens are normalised
    # to "test-token" for stable comparison across test runs.
    local ver="$1" port="$2" base="$3"
    local token_file="${base}/admin_token.txt"

    # Quick path: token file already present with a valid apiv3_ token.
    if [[ -f "$token_file" ]] && grep -q "^apiv3_" "$token_file" 2>/dev/null; then
        info "InfluxDB ${ver}: admin token file already present at ${token_file}"
        return 0
    fi

    # Find the influxdb3 binary.
    local influxd
    influxd="$(find "${base}/bin" -name "influxdb3" 2>/dev/null | head -1)"
    if [[ -z "$influxd" ]]; then
        err "InfluxDB ${ver}: no influxdb3 binary found in ${base}/bin."
        OVERALL_OK=1; return 1
    fi

    # Create admin token via influxdb3 CLI (bootstrap endpoint, no auth needed).
    local token_output token_value
    token_output="$("$influxd" create token --admin --host "http://127.0.0.1:${port}" 2>&1)"
    token_value="$(echo "$token_output" | grep -Eo 'apiv3_[A-Za-z0-9_-]+' | head -1)"
    if [[ -z "$token_value" ]]; then
        err "InfluxDB ${ver}: failed to create admin token. Output: ${token_output}"
        OVERALL_OK=1; return 1
    fi

    echo "$token_value" > "$token_file"
    chmod 600 "$token_file"
    info "InfluxDB ${ver}: admin token created and written to ${token_file}"
}

_influx_start() {
    local ver="$1" port="$2" base="$3"
    local data="${base}/data" log="${base}/log"
    local influxd pidfile="${base}/run/influxd.pid"
    mkdir -p "${base}/run" "$log"

    influxd="$(find "${base}/bin" \( -name "influxdb3" -o -name "influxd" \) 2>/dev/null | head -1)"
    if [[ -z "$influxd" ]]; then
        err "InfluxDB ${ver}: no binary found in ${base}/bin."
        OVERALL_OK=1; return 1
    fi

    if [[ "$(basename "$influxd")" == "influxdb3" ]]; then
        # InfluxDB 3.x: run in default auth-enabled mode (no --without-auth).
        # Auth IS enforced: only catalog-stored tokens are accepted.  The admin
        # token is created via `influxdb3 create token --admin` in
        # _influx_ensure_token() after startup and written to admin_token.txt.
        # This allows FQ-06-002 to verify that wrong credentials produce
        # EXT_AUTH_FAILED, while FQ-01 uses the real admin token for valid
        # queries.  The baseline comparison normalises the dynamic apiv3_...
        # token back to "test-token" for stable comparison.
        _start_daemon "$pidfile" "${log}/influxd.log" \
            "$influxd" serve \
            --node-id "fq-test-node" \
            --http-bind "127.0.0.1:${port}" \
            --object-store file \
            --data-dir "$data"
    else
        case "$ver" in
            1.*)
                # InfluxDB 1.x: requires a TOML config file; does not accept
                # data/log paths as CLI flags like v2 does.
                mkdir -p "${data}/meta" "${data}/data" "${data}/wal"
                local cfg_file="${base}/influxdb.conf"
                cat > "$cfg_file" <<EOF
[meta]
  dir = "${data}/meta"

[data]
  dir = "${data}/data"
  wal-dir = "${data}/wal"

[http]
  bind-address = "127.0.0.1:${port}"
  auth-enabled = false
  log-enabled = false
EOF
                _start_daemon "$pidfile" "${log}/influxd.log" \
                    "$influxd" -config "$cfg_file"
                ;;
            *)
                # influxd v2 fallback
                _start_daemon "$pidfile" "${log}/influxd.log" \
                    "$influxd" \
                    --http-bind-address "127.0.0.1:${port}" \
                    --storage-wal-directory "${data}/wal" \
                    --storage-data-path "$data"
                ;;
        esac
    fi
}

_influx_reset_env() {
    local ver="$1" port="$2" base="$3"

    info "InfluxDB ${ver} @ ${port}: resetting test databases ..."

    case "$ver" in
        1.*)
            # InfluxDB 1.x: use the v1 query API to list and drop databases
            local dbs db
            dbs=$(curl -sf --max-time 5 \
                "http://127.0.0.1:${port}/query?q=SHOW+DATABASES" 2>/dev/null | \
                sed -nE 's/.*"values":\[\["([^"]+)".*/\1/p' || true)
            for db in $dbs; do
                [[ "$db" == "_internal" ]] && continue
                curl -sf -X POST "http://127.0.0.1:${port}/query" \
                    --data-urlencode "q=DROP DATABASE \"${db}\"" \
                    -o /dev/null 2>/dev/null || true
            done
            info "InfluxDB ${ver} @ ${port}: reset complete."
            return ;;
    esac

    # InfluxDB 3.x hard reset:
    # The IOx catalog is append-only: every create/delete writes a new entry.
    # "drop database" only soft-deletes entries; the global table count never
    # decreases.  After 2000 accumulated table entries new writes fail with
    # "exceed number of tables limit".  The only reliable reset is:
    #   kill → wipe data directory → restart.
    local pidfile="${base}/run/influxd.pid"
    info "InfluxDB ${ver} @ ${port}: hard reset (kill → wipe data → restart) ..."

    # 1. Kill all influxdb3 processes bound to this port (SIGKILL — instant, no graceful shutdown)
    if [[ -f "$pidfile" ]]; then
        kill -9 "$(cat "$pidfile")" 2>/dev/null || true
        rm -f "$pidfile"
    fi
    # Also kill any stale instances that may have been started by earlier sessions
    _kill_matching "influxdb3 serve.*${port}" KILL
    # Verify: wait until all influxdb3 processes for this port are truly gone
    _wait_procs_gone "influxdb3 serve.*${port}" 100 \
        || warn "InfluxDB ${ver}: process may linger; continuing..."
    # Wait for TCP port to be released (probe-based, no busy-spin).
    _wait_port_free "$port" 30 \
        || warn "InfluxDB ${ver}: port ${port} still open; continuing..."

    # 2. Wipe the data directory (removes catalog + all test data, including DB-side tokens)
    rm -f "${base}/admin_token.txt"   # token is gone after data wipe; must recreate
    rm -rf "${base}/data"
    # Verify: data directory must be absent before restarting InfluxDB
    _verify_absent "${base}/data" "InfluxDB ${ver} data dir" || return 1
    mkdir -p "${base}/data"

    # 3. Restart InfluxDB
    _influx_start "$ver" "$port" "$base"
    if ! wait_port "$port" 60; then
        err "InfluxDB ${ver}: failed to restart after hard reset."
        OVERALL_OK=1; return 1
    fi

    # 4. HTTP health probe: confirm InfluxDB is actually serving requests.
    # Accept 200 with pass/ok body (no-auth mode) OR 401 (auth enabled — server is up).
    local _hi=0
    while [[ $_hi -lt 30 ]]; do
        local _hc
        _hc="$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "http://127.0.0.1:${port}/health" 2>/dev/null || echo "000")"
        if [[ "$_hc" == "401" ]] || { [[ "$_hc" == "200" ]] && curl -sf --max-time 3 "http://127.0.0.1:${port}/health" 2>/dev/null \
                | grep -qiE '"status":"(pass|ok)"|^OK$'; }; then
            _influx_ensure_token "$ver" "$port" "$base"
            info "InfluxDB ${ver} @ ${port}: reset complete (data wiped, restarted)."
            return 0
        fi
        _hi=$((_hi+1))
        sleep 1
    done
    # Health endpoint may not exist on all InfluxDB 3 builds; port open is sufficient
    warn "InfluxDB ${ver}: /health did not return pass/ok after reset (non-fatal)."
    _influx_ensure_token "$ver" "$port" "$base"
    info "InfluxDB ${ver} @ ${port}: reset complete (data wiped, restarted)."
}

# ──────────────────────────────────────────────────────────────────────────────
# 12.  Teardown: stop all external DBs and rotate oversized log files.
#      Called at the END of each FQ test class (not at init time).
# ──────────────────────────────────────────────────────────────────────────────

# Rotate (truncate) a single log file if it exceeds LOG_ROTATE_MAX_BYTES.
# Keeps exactly one rotated copy (.1); replaces any existing .1.
_teardown_rotate_log() {
    local f="$1"
    [[ -f "$f" ]] || return 0
    local size
    size=$(stat -c%s "$f" 2>/dev/null || echo 0)
    if (( size > LOG_ROTATE_MAX_BYTES )); then
        mv -f "$f" "${f}.1" 2>/dev/null || true
        : > "$f"
        info "log rotated: $f (was ${size} bytes)"
    fi
}

_teardown_mysql() {
    local ver="$1"
    local port; port=$(mysql_port "$ver")
    local base="${FQ_BASE_DIR}/mysql/${ver}"
    local pidfile="${base}/run/mysqld.pid"
    info "MySQL ${ver} @ ${port}: stopping ..."
    _stop_daemon "$pidfile" "mysqld.*port=${port}"
    # Remove all files in the log directory (keep the directory itself).
    # Enumerate nothing by name so future log files are also covered.
    find "${base}/log" -maxdepth 1 -type f -delete 2>/dev/null || true
    info "MySQL ${ver}: stopped and logs cleared."
}

_teardown_pg() {
    local ver="$1"
    local port; port=$(pg_port "$ver")
    local base="${FQ_BASE_DIR}/pg/${ver}"
    local datadir="${base}/data"
    local pg_ctl_bin="${base}/bin/pg_ctl"
    info "PostgreSQL ${ver} @ ${port}: stopping ..."
    if [[ -x "$pg_ctl_bin" && -d "$datadir" ]]; then
        "$pg_ctl_bin" stop -D "$datadir" -m fast -w 2>/dev/null || true
    fi
    # Fallback: kill by port pattern
    _kill_matching "postgres.*-p ${port}" TERM
    # Wait for graceful shutdown before escalating to SIGKILL
    _wait_procs_gone "postgres.*-p ${port}" 50 \
        || _kill_matching "postgres.*-p ${port}" KILL
    # Remove all files in the log directory (keep the directory itself).
    find "${base}/log" -maxdepth 1 -type f -delete 2>/dev/null || true
    info "PostgreSQL ${ver}: stopped and logs cleared."
}

_teardown_influx() {
    local ver="$1"
    local port; port=$(influx_port "$ver")
    local base="${FQ_BASE_DIR}/influxdb/${ver}"
    local pidfile="${base}/run/influxd.pid"
    info "InfluxDB ${ver} @ ${port}: stopping ..."
    _stop_daemon "$pidfile" "influxdb3.*${base}"
    # Remove all files in the log directory (keep the directory itself).
    find "${base}/log" -maxdepth 1 -type f -delete 2>/dev/null || true
    info "InfluxDB ${ver}: stopped and logs cleared."
}

teardown_all() {
    log "========================================================"
    log "FederatedQuery external-source teardown (stop + rotate logs)"
    log "========================================================"
    local ver
    for ver in "${MYSQL_VERSIONS[@]}";  do _teardown_mysql  "$ver" || true; done
    for ver in "${PG_VERSIONS[@]}";     do _teardown_pg     "$ver" || true; done
    for ver in "${INFLUX_VERSIONS[@]}"; do _teardown_influx "$ver" || true; done
    for ver in "${SQLSERVER_VERSIONS[@]}"; do _teardown_sqlserver "$ver" || true; done
    log "Teardown complete."
}

# SQL Server teardown: stop the engine process (the data directory is kept for
# the next run, like the other engines' stop semantics; ensure_sqlserver wipes
# its databases).
_teardown_sqlserver() {
    local ver="$1"
    _sqlserver_stop_procs "${ver}" TERM
    _sqlserver_kill_foreign "${ver}"
    info "SQL Server ${ver}: engine stopped."
}

_probe_mysql_ready() {
    local ver="$1"
    local port; port="$(mysql_port "$ver")"
    _mysql_py_exec 127.0.0.1 "$port" "$MYSQL_USER" "$MYSQL_PASS" "SELECT 1;" >/dev/null 2>&1
}

_probe_pg_ready() {
    local ver="$1"
    local port; port="$(pg_port "$ver")"
    local base="${FQ_BASE_DIR}/pg/${ver}"
    local psql="${base}/bin/psql"

    [[ -x "$psql" ]] || return 1
    PGPASSWORD="$PG_PASS" PGCONNECT_TIMEOUT=5 "$psql" \
        -h 127.0.0.1 -p "$port" -U "$PG_USER" -d postgres \
        -c "SELECT 1;" >/dev/null 2>&1
}

_probe_influx_ready() {
    local ver="$1"
    local port; port="$(influx_port "$ver")"
    local code
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:${port}/health" 2>/dev/null || echo "000")"
    [[ "$code" == "200" || "$code" == "204" || "$code" == "401" ]]
}

_verify_and_recover_once() {
    local _services="$1"
    local ver

    # OVERALL_OK may already be 1 from a transient failure in the first
    # ensure_* pass in main() (e.g. an initdb timeout under CI load) that
    # this recovery pass goes on to fix.  Reset it here so the outcome of
    # THIS function — the actual post-recovery probe results below — is
    # what decides success, instead of a stale flag from a problem that
    # no longer exists.
    OVERALL_OK=0

    if [[ "$_services" == *pg* ]]; then
        for ver in "${PG_VERSIONS[@]}"; do
            if ! _probe_pg_ready "$ver"; then
                warn "PostgreSQL ${ver}: probe failed after setup, trying quick restart once ..."
                FQ_PG_QUICK_RESTART=1 ensure_pg "$ver" || OVERALL_OK=1
            fi
            _probe_pg_ready "$ver" || { err "PostgreSQL ${ver}: still unreachable after recovery."; OVERALL_OK=1; }
        done
    fi

    if [[ "$_services" == *mysql* ]]; then
        for ver in "${MYSQL_VERSIONS[@]}"; do
            if ! _probe_mysql_ready "$ver"; then
                warn "MySQL ${ver}: probe failed after setup, trying recovery once ..."
                ensure_mysql "$ver" || OVERALL_OK=1
            fi
            _probe_mysql_ready "$ver" || { err "MySQL ${ver}: still unreachable after recovery."; OVERALL_OK=1; }
        done
    fi

    if [[ "$_services" == *influx* ]]; then
        for ver in "${INFLUX_VERSIONS[@]}"; do
            if ! _probe_influx_ready "$ver"; then
                warn "InfluxDB ${ver}: probe failed after setup, trying quick restart once ..."
                FQ_INFLUX_QUICK_RESTART=1 ensure_influx "$ver" || OVERALL_OK=1
            fi
            _probe_influx_ready "$ver" || { err "InfluxDB ${ver}: still unreachable after recovery."; OVERALL_OK=1; }
        done
    fi
}

# ──────────────────────────────────────────────────────────────────────────────
# 12.5 Final readiness probe
# Ensure all selected services are truly queryable before returning success.
# This runs AFTER all ensure_* calls to catch cross-service side effects.
# ──────────────────────────────────────────────────────────────────────────────

_final_probe_mysql() {
    local ver="$1"
    local port; port="$(mysql_port "$ver")"
    local base="${FQ_BASE_DIR}/mysql/${ver}"
    local log_file="${base}/log/error.log"
    local ok=0 i=0
    while [[ $i -lt 30 ]]; do
        # Require consecutive successful probes to avoid transient false-ready.
        if _mysql_py_exec 127.0.0.1 "$port" "$MYSQL_USER" "$MYSQL_PASS" \
                "SELECT 1; SHOW DATABASES;" >/dev/null 2>&1 \
           && _mysql_py_check_timezone 127.0.0.1 "$port" "$MYSQL_USER" "$MYSQL_PASS"; then
            ok=$((ok + 1))
            if [[ $ok -ge 3 ]]; then
                info "MySQL ${ver} @ ${port}: final probe passed."
                return 0
            fi
        else
            ok=0
        fi
        i=$((i + 1))
        sleep 1
    done

    err "MySQL ${ver} @ ${port}: final probe failed (query path not stable)."
    tail -20 "$log_file" 2>/dev/null >&2 || true
    return 1
}

_final_probe_pg() {
    local ver="$1"
    local port; port="$(pg_port "$ver")"
    local base="${FQ_BASE_DIR}/pg/${ver}"
    local psql="${base}/bin/psql"
    local log_file="${base}/log/pg.log"

    if [[ ! -x "$psql" ]]; then
        err "PostgreSQL ${ver}: final probe failed, psql missing: ${psql}"
        return 1
    fi

    local ok=0 i=0
    while [[ $i -lt 30 ]]; do
        # Verify both connection and a catalog query.
        if PGPASSWORD="$PG_PASS" PGCONNECT_TIMEOUT=3 "$psql" \
               -h 127.0.0.1 -p "$port" -U "$PG_USER" -d postgres \
               -Atc "SELECT 1;" >/dev/null 2>&1 \
           && PGPASSWORD="$PG_PASS" PGCONNECT_TIMEOUT=3 "$psql" \
               -h 127.0.0.1 -p "$port" -U "$PG_USER" -d postgres \
               -Atc "SELECT datname FROM pg_database LIMIT 1;" >/dev/null 2>&1; then
            ok=$((ok + 1))
            if [[ $ok -ge 3 ]]; then
                info "PostgreSQL ${ver} @ ${port}: final probe passed."
                return 0
            fi
        else
            ok=0
        fi
        i=$((i + 1))
        sleep 1
    done

    err "PostgreSQL ${ver} @ ${port}: final probe failed (query path not stable)."
    tail -20 "$log_file" 2>/dev/null >&2 || true
    return 1
}

_final_probe_influx() {
    local ver="$1"
    local port; port="$(influx_port "$ver")"
    local base="${FQ_BASE_DIR}/influxdb/${ver}"
    local log_file="${base}/log/influxd.log"
    local ok=0 i=0

    while [[ $i -lt 30 ]]; do
        local good=1
        case "$ver" in
            1.*)
                # v1: query API must be reachable and executable.
                curl -sf --max-time 3 \
                    "http://127.0.0.1:${port}/query?q=SHOW+DATABASES" >/dev/null 2>&1 || good=0
                ;;
            *)
                # v3: auth is enabled. Probe with a valid admin token to verify
                # API reachability on the real authenticated query path.
                local token_file="${base}/admin_token.txt"
                local token=""

                if [[ -f "$token_file" ]]; then
                    token="$(tr -d '\n' < "$token_file")"
                fi
                if [[ ! "$token" =~ ^apiv3_ ]]; then
                    _influx_ensure_token "$ver" "$port" "$base" >/dev/null 2>&1 || true
                    if [[ -f "$token_file" ]]; then
                        token="$(tr -d '\n' < "$token_file")"
                    fi
                fi

                if [[ "$token" =~ ^apiv3_ ]]; then
                    curl -sf --max-time 3 \
                        -H "Authorization: Bearer ${token}" \
                        "http://127.0.0.1:${port}/api/v3/configure/database?format=json" >/dev/null 2>&1 || good=0
                else
                    good=0
                fi
                ;;
        esac

        if [[ $good -eq 1 ]]; then
            ok=$((ok + 1))
            if [[ $ok -ge 3 ]]; then
                info "InfluxDB ${ver} @ ${port}: final probe passed."
                return 0
            fi
        else
            ok=0
        fi

        i=$((i + 1))
        sleep 1
    done

    err "InfluxDB ${ver} @ ${port}: final probe failed (API path not stable)."
    tail -20 "$log_file" 2>/dev/null >&2 || true
    return 1
}

final_probe_selected_services() {
    local services="$1"
    local ver

    log "Running final cross-engine readiness probe ..."
    [[ "$services" == *mysql*  ]] && for ver in "${MYSQL_VERSIONS[@]}";  do _final_probe_mysql  "$ver" || return 1; done
    [[ "$services" == *pg*     ]] && for ver in "${PG_VERSIONS[@]}";     do _final_probe_pg     "$ver" || return 1; done
    [[ "$services" == *influx* ]] && for ver in "${INFLUX_VERSIONS[@]}"; do _final_probe_influx "$ver" || return 1; done
    [[ "$services" == *sqlserver* ]] && for ver in "${SQLSERVER_VERSIONS[@]}"; do
        _sqlserver_probe "${ver}" \
            || err "SQL Server ${ver}: readiness probe failed (non-fatal)"
    done

    log "Final cross-engine readiness probe passed."
    return 0
}

_is_single_service_mode() {
    local services="$1"
    local selected=($services)
    [[ "${#selected[@]}" -eq 1 ]]
}

_is_single_service_quick_restart() {
    local services="$1"
    local selected=($services)

    _is_single_service_mode "$services" || return 1

    case "${selected[0]}" in
        mysql)  [[ "${FQ_MYSQL_QUICK_RESTART:-0}" == "1" ]] ;;
        pg)     [[ "${FQ_PG_QUICK_RESTART:-0}" == "1" ]] ;;
        influx) [[ "${FQ_INFLUX_QUICK_RESTART:-0}" == "1" ]] ;;
        sqlserver) [[ "${FQ_SQLSERVER_QUICK_RESTART:-0}" == "1" ]] ;;
        *)      return 1 ;;
    esac
}

# ──────────────────────────────────────────────────────────────────────────────
# 13.  Main
# ──────────────────────────────────────────────────────────────────────────────

# Allow the script to be sourced by test harnesses without running main.
# Set FQ_SOURCE_ONLY=1 before sourcing to suppress execution.
main() {
    sanitize_foreign_engine_env

    # FQ_SERVICES_TO_RESET controls which services are processed.
    # Default: all three. In-test restarts set this to a single service.
    local _services="${FQ_SERVICES_TO_RESET:-mysql pg influx sqlserver}"

    log "========================================================"
    log "FederatedQuery external-source setup"
    log "  OS       : ${OS} (${DISTRO}) / ${ARCH}"
    log "  User     : ${CURRENT_USER}"
    log "  Base dir : ${FQ_BASE_DIR}"
    log "  Cert src : ${CERT_SRC}"
    log "  MySQL    : ${MYSQL_VERSIONS[*]}"
    log "  PG       : ${PG_VERSIONS[*]}"
    log "  InfluxDB : ${INFLUX_VERSIONS[*]}"
    log "  SQLServer: ${SQLSERVER_VERSIONS[*]}"
    log "========================================================"

    if [[ "$_services" == *mysql* ]]; then
        _check_pymysql_dependency || exit 1
    fi

    mkdir -p "$FQ_BASE_DIR"
    _configure_apt_mirrors
    _ensure_linux_engine_deps

    local ver
    # Run PG first because package/setup steps are heavier and may transiently
    # impact other daemons in constrained containers.
    [[ "$_services" == *pg*      ]] && for ver in "${PG_VERSIONS[@]}";     do ensure_pg     "$ver" || OVERALL_OK=1; done
    [[ "$_services" == *mysql*   ]] && for ver in "${MYSQL_VERSIONS[@]}";  do ensure_mysql  "$ver" || OVERALL_OK=1; done
    [[ "$_services" == *influx*  ]] && for ver in "${INFLUX_VERSIONS[@]}"; do ensure_influx "$ver" || OVERALL_OK=1; done
    if [[ "$_services" == *sqlserver* ]]; then
        # Native-process mode runs one engine version at a time: sqlservr keeps
        # its system databases in the fixed /var/opt/mssql path, so a second
        # concurrent version cannot initialize.  Multi-version checks
        # (fq_12's optional 2019+2022 matrix) therefore need separate hosts.
        if [[ ${#SQLSERVER_VERSIONS[@]} -gt 1 ]]; then
            warn "SQL Server: native process mode provisions one version at a time — using ${SQLSERVER_VERSIONS[0]} only"
        fi
        for ver in "${SQLSERVER_VERSIONS[@]:0:1}"; do
            ensure_sqlserver "$ver" || _sqlserver_env_failed=1
        done
        # Non-fatal: SQL Server scenarios fail on their own when the instance is
        # unavailable; MySQL/PG/Influx coverage must not be reduced by it.
        [[ "${_sqlserver_env_failed:-0}" -eq 0 ]] || err "SQL Server environment not ready; SQL Server scenarios will fail."
    fi

    if _is_single_service_quick_restart "$_services"; then
        log "Single-service quick restart complete; caller will run readiness probe."
        [[ "$OVERALL_OK" -eq 0 ]] || exit 1
        return 0
    fi

    if _is_single_service_mode "$_services"; then
        if [[ "$OVERALL_OK" -ne 0 ]]; then
            err "Single-service setup failed; see messages above."
            exit 1
        fi
        log "Single-service setup complete; caller will run readiness probe."
        return 0
    fi

    # Final one-shot liveness verification catches daemons that die after setup
    # (e.g. transient OOM kills) without triggering repeated package installs.
    _verify_and_recover_once "$_services"

    if [[ "$OVERALL_OK" -ne 0 ]]; then
        err "One or more engines failed to start. See messages above."
        exit 1
    fi

    if ! final_probe_selected_services "$_services"; then
        err "Final readiness probe failed. One or more engines are not query-ready."
        exit 1
    fi

    log "All engines ready."
}

# Run main only when executed directly (not when sourced)
if [[ "${FQ_SOURCE_ONLY:-0}" != "1" && "${BASH_SOURCE[0]}" == "$0" ]]; then
    case "${1:-}" in
        --teardown) teardown_all ;;
        --sqlserver-exec)
            # Internal helper: run one SQL batch through the exact sqlcmd
            # invocation this script uses (see _sqlserver_run_sql).  The Python
            # FQ fixtures call this instead of rebuilding the environment
            # themselves — a replicated environment failed in CI containers
            # with "Driver's SQLAllocHandle on SQL_HANDLE_HENV failed" while
            # this path works there (and everywhere else).
            [[ $# -eq 4 ]] || { echo "usage: $0 --sqlserver-exec <version> <database> <sql>" >&2; exit 2; }
            _sqlserver_run_sql "$2" "$3" "$4"
            exit $?
            ;;
        *)          main "$@"    ;;
    esac
fi
