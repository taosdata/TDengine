#!/usr/bin/env bash
# ensure_ext_env.sh  ─ FederatedQuery integration-test external-source setup
#
# COMPATIBILITY TARGETS
#   OS     : Linux (Ubuntu/Debian, RHEL/CentOS/Rocky/AlmaLinux, Alpine, Arch)
#            macOS 12+ (Homebrew required for some engines)
#   Arch   : x86_64, aarch64 (arm64 on macOS)
#   Bash   : 4.0+  (associative arrays, pipefail)
#            macOS ships bash 3.2; run via /usr/local/bin/bash from Homebrew.
#   User   : root or non-root (engines run as invoking user; mysqld allows root
#            only with explicit --user=root)
#   Shell  : Must be invoked as `bash ensure_ext_env.sh`; not POSIX sh / zsh
#
# WINDOWS : Use ensure_ext_env.ps1 (PowerShell 5.1+).  Not supported here.
#
# WHAT IT DOES (idempotent per-engine-version)
#   1. Port open?           → reset test DBs (already running)
#   2. Installed, stopped?  → start; if still failing re-init data dir
#   3. Not installed?       → download → install → init → start → configure
#   4. First start:         → copy TLS certs, apply config, reset test DBs
#
# ENVIRONMENT VARIABLES (all optional, defaults match federated_query_common.py)
#   FQ_BASE_DIR            install/data root      default /opt/taostest/fq
#                                                   (macOS: ~/taostest/fq)
#   FQ_MYSQL_VERSIONS      comma list             default "8.0"
#   FQ_PG_VERSIONS         comma list             default "16"
#   FQ_INFLUX_VERSIONS     comma list             default "3.0"
#   FQ_MYSQL_MIRROR        base URL for MySQL tarballs
#   FQ_PG_TARBALL_<VV>     full URL for PG prebuilt tarball (fallback if no pkg)
#   FQ_INFLUX_MIRROR       base URL for InfluxDB releases
#   FQ_TARBALL_CACHE_DIR   tarball/deb cache dir              auto-detect:
#                          /usr/local/src or /data0/compat-packages when
#                          MySQL/Influx tarballs are staged; else /tmp
#   FQ_USE_NEXUS_APT       force Nexus apt on/off             unset=auto-detect;
#                          Ubuntu/macOS always use public apt for PG;
#                          only pure Debian CI (tdengine-ci) uses Nexus apt
#   FQ_APT_MIRROR_BASE     Nexus apt proxy base               default https://nexus.tdengine.net/repository
#   FQ_APT_PG_MIRROR       PGDG apt proxy URL                 default ${FQ_APT_MIRROR_BASE}/apt-postgresql-org
#   FQ_LIBAIO_DEB_URL      direct libaio .deb URL override    default derived from Nexus Debian mirror
#   FQ_LIBNUMA_DEB_URL     direct libnuma .deb URL override   default derived from Nexus Debian mirror
#   FQ_MYSQL_TARBALL_<VV>  full URL override per MySQL version (VV = 57/80/84)
#   FQ_INFLUX_TARBALL_<VV> full URL override per InfluxDB version (VV = 30/35)
#   FQ_CERT_DIR            cert source dir        default <script_dir>/certs
#   FQ_MYSQL_USER/PASS     credentials            default root / taosdata
#   FQ_MYSQL_INIT_TIMEOUT_S mysqld --initialize timeout default 300
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

_default_fq_base_dir() {
    case "$OS" in
        Darwin) printf '%s\n' "${HOME}/taostest/fq" ;;
        *)      printf '%s\n' "/opt/taostest/fq" ;;
    esac
}

FQ_BASE_DIR="${FQ_BASE_DIR:-$(_default_fq_base_dir)}"
CERT_SRC="${FQ_CERT_DIR:-${SCRIPT_DIR}/certs}"

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

MYSQL_USER="${FQ_MYSQL_USER:-root}"
MYSQL_PASS="${FQ_MYSQL_PASS:-taosdata}"
PG_USER="${FQ_PG_USER:-postgres}"
PG_PASS="${FQ_PG_PASS:-taosdata}"
PG_TIMEZONE="${FQ_PG_TIMEZONE:-Asia/Shanghai}"
export TZ="${TZ:-$PG_TIMEZONE}"
export PGTZ="${PGTZ:-$PG_TIMEZONE}"
INFLUX_TOKEN="${FQ_INFLUX_TOKEN:-test-token}"
INFLUX_ORG="${FQ_INFLUX_ORG:-test-org}"

CURRENT_USER="$(id -un)"   # portable alternative to whoami

OVERALL_OK=0

# ──────────────────────────────────────────────────────────────────────────────
# 2.  Logging
# ──────────────────────────────────────────────────────────────────────────────
log()  { echo "[fq-env] $*"; }
info() { echo "[fq-env] INFO  $*"; }
# warn/err go to both stdout and stderr: stdout is needed so that CI log
# viewers that only capture stdout (pytest, run_case.sh) can see failures.
warn() { local _m="[fq-env] WARN  $*"; echo "$_m"; echo "$_m" >&2; }
err()  { local _m="[fq-env] ERROR $*"; echo "$_m"; echo "$_m" >&2; }

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
    # Prefer nc (netcat).  Do NOT use curl telnet:// — it can hang indefinitely
    # on HTTP servers (e.g. influxdb3) even when --connect-timeout is set.
    if command -v nc &>/dev/null; then
        nc -z -w 2 127.0.0.1 "$port" 2>/dev/null && return 0
    fi
    if command -v ncat &>/dev/null; then
        ncat -z -w 2 127.0.0.1 "$port" 2>/dev/null && return 0
    fi
    # bash /dev/tcp probe (bounded; safe under ASAN CI)
    if command -v timeout &>/dev/null; then
        timeout 2 bash -c "exec 3<>/dev/tcp/127.0.0.1/${port} && exec 3<&- 3>&-" 2>/dev/null \
            && return 0
    elif (exec 3<>/dev/tcp/127.0.0.1/"${port}") 2>/dev/null; then
        exec 3<&- 3>&-
        return 0
    fi
    # Kernel socket table (no network I/O)
    if command -v ss &>/dev/null; then
        ss -ltn 2>/dev/null | grep -qE ":${port}[[:space:]]"
        return
    fi
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
# Kill processes whose command line matches a pattern.
_kill_matching() {
    local pattern="$1"
    local sig="${2:-TERM}"
    # pkill on most Linux + macOS; on BusyBox pkill may lack -f
    if pkill -"$sig" -f "$pattern" 2>/dev/null; then
        return 0
    fi
    # Fallback: pgrep -f + kill
    if command -v pgrep &>/dev/null; then
        local pids
        pids=$(pgrep -f "$pattern" 2>/dev/null || true)
        if [[ -n "$pids" ]]; then
            # shellcheck disable=SC2086
            kill -"$sig" $pids 2>/dev/null || true
            return 0
        fi
    fi
    # Last resort: use ps + awk
    local pids
    pids=$(ps aux 2>/dev/null | awk -v pat="$pattern" '$0 ~ pat && !/awk/ {print $2}' || true)
    if [[ -n "$pids" ]]; then
        # shellcheck disable=SC2086
        kill -"$sig" $pids 2>/dev/null || true
    fi
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

# Run a command without CI ASAN LD_PRELOAD and without TDengine LD_LIBRARY_PATH.
# External DB binaries (postgres, mysql, influxdb3) must not inherit either:
#   LD_PRELOAD  — ASAN interceptors crash non-instrumented binaries
#   LD_LIBRARY_PATH — TDengine's bundled libcrypto/libssl shadow system libs
_fq_env_clean() {
    local _runner=(env)
    [[ -n "${LD_PRELOAD:-}" ]] && _runner+=( -u LD_PRELOAD )
    [[ -n "${LD_LIBRARY_PATH:-}" ]] && _runner+=( -u LD_LIBRARY_PATH )
    "${_runner[@]}" "$@"
}

_fq_mysql_ld_path() {
    local base="$1"
    local lib_private="${base}/lib/private"
    if [[ -d "$lib_private" ]]; then
        echo "$lib_private"
    fi
}

# MySQL tarball clients need lib/private on LD_LIBRARY_PATH and must not inherit ASAN LD_PRELOAD.
_fq_mysql_run() {
    local base="$1"; shift
    local _ldlp; _ldlp="$(_fq_mysql_ld_path "$base")"
    if [[ -n "$_ldlp" ]]; then
        _fq_env_clean env LD_LIBRARY_PATH="$_ldlp" "$@"
    else
        _fq_env_clean "$@"
    fi
}

# Start a daemon via nohup, record PID in pidfile, return immediately
# Usage: _start_daemon <pidfile> <logfile> <cmd> [args...]
_start_daemon() {
    local pidfile="$1" logfile="$2"
    shift 2
    mkdir -p "$(dirname "$pidfile")" "$(dirname "$logfile")"
    _rotate_log "$logfile"
    # setsid is missing on macOS; nohup + & is sufficient there.
    if command -v setsid >/dev/null 2>&1; then
        _fq_env_clean setsid nohup "$@" >> "$logfile" 2>&1 &
    else
        _fq_env_clean nohup "$@" >> "$logfile" 2>&1 &
    fi
    echo "$!" > "$pidfile"
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
    if pgrep -f "$pattern" >/dev/null 2>&1; then
        _kill_matching "$pattern" TERM
        local _di=0
        while pgrep -f "$pattern" >/dev/null 2>&1 && [[ $_di -lt 50 ]]; do
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
    while pgrep -f "$pattern" >/dev/null 2>&1; do
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

_cleanup_orphan_sysv_shm() {
    local owner_filter="${1:-$CURRENT_USER}"

    # On macOS PostgreSQL can leave a tiny SysV shared-memory segment behind if
    # the test reset has to SIGKILL it.  The default shmmni limit is only 32, so
    # repeated --clean runs eventually make initdb fail even though disk is fine.
    [[ "$OS" == "Darwin" ]] || return 0
    command -v ipcs >/dev/null 2>&1 || return 0
    command -v ipcrm >/dev/null 2>&1 || return 0

    local -A owners cpids lpids nattchs
    local t id key mode owner group cpid lpid nattch
    while read -r t id key mode owner group cpid lpid; do
        [[ "$t" == "m" ]] || continue
        owners["$id"]="$owner"
        cpids["$id"]="$cpid"
        lpids["$id"]="$lpid"
    done < <(ipcs -m -p 2>/dev/null || true)

    while read -r t id key mode owner group nattch; do
        [[ "$t" == "m" ]] || continue
        nattchs["$id"]="$nattch"
    done < <(ipcs -m -o 2>/dev/null || true)

    local removed=0 c_alive l_alive
    for id in "${!owners[@]}"; do
        [[ "${owners[$id]}" == "$owner_filter" ]] || continue
        [[ "${nattchs[$id]:-1}" == "0" ]] || continue

        c_alive=0
        l_alive=0
        if [[ -n "${cpids[$id]:-}" ]] && kill -0 "${cpids[$id]}" 2>/dev/null; then
            c_alive=1
        fi
        if [[ -n "${lpids[$id]:-}" ]] && kill -0 "${lpids[$id]}" 2>/dev/null; then
            l_alive=1
        fi
        [[ "$c_alive" == "0" && "$l_alive" == "0" ]] || continue

        if ipcrm -m "$id" 2>/dev/null; then
            removed=$((removed + 1))
        fi
    done

    if [[ "$removed" -gt 0 ]]; then
        info "Cleaned ${removed} orphan SysV shared-memory segment(s) owned by ${owner_filter}."
    fi
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
                "$url" 2>/dev/null; then
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

_ensure_linux_engine_deps() {
    [[ "$OS" != "Linux" ]] && return 0
    command -v apt-get &>/dev/null || return 0
    _ensure_engine_deps_apt_index

    # Install libaio separately so we can fall back from libaio1t64 to libaio1.
    # Modern Debian (trixie+) renamed the package to libaio1t64; older distros
    # use libaio1.  A batch install that includes an unknown package name causes
    # the whole apt-get call to fail — so we try them individually.
    if ! _libaio_present; then
        info "installing engine OS deps: libaio (libaio1t64 or libaio1) ..."
        if ! DEBIAN_FRONTEND=noninteractive apt-get install -y -q \
                --no-install-recommends libaio1t64; then
            DEBIAN_FRONTEND=noninteractive apt-get install -y -q \
                --no-install-recommends libaio1 \
                || _install_libaio_from_local_cache \
                || _install_libaio_from_direct_deb \
                || warn "engine OS deps: libaio could not be installed (tried apt, local cache, and direct deb)."
        fi
    fi

    local _need=()
    if ! _libnuma_present; then
        info "installing engine OS deps: libnuma1 ..."
        DEBIAN_FRONTEND=noninteractive apt-get install -y -q --no-install-recommends libnuma1 \
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
        DEBIAN_FRONTEND=noninteractive apt-get install -y -q --no-install-recommends \
            "${_need[@]}" 2>/dev/null || warn "engine OS deps install had warnings."
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
        if apt-get update -qq; then
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

    flavor=""
    # Prefer full glibc2.28 tarball when compat-packages ships it; minimal is
    # only used as a download fallback URL basename.
    if [[ "$ver" == "8.0" && "$arch_str" == "x86_64" ]]; then
        local _full="mysql-${patch}-linux-glibc2.28-${arch_str}.tar.xz"
        local _root _found=""
        while IFS= read -r _root; do
            if [[ -s "${_root}/${_full}" ]]; then
                _found="${_root}/${_full}"
                break
            fi
        done < <(_fq_staging_dirs)
        if [[ -z "$_found" ]]; then
            glibc="glibc2.17"
            flavor="-minimal"
        fi
    fi

    local tag="${ver//./}"
    local override="FQ_MYSQL_TARBALL_${tag}"
    local override_val="${!override:-}"
    if [[ -n "$override_val" ]]; then echo "$override_val"; return; fi
    local base="${FQ_MYSQL_MIRROR:-https://cdn.mysql.com/Downloads/MySQL-${major}.${minor}}"
    echo "${base}/mysql-${patch}-linux-${glibc}-${arch_str}.tar.xz"
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
        rmdir "${_lockdir}" 2>/dev/null || true
    fi
    if ! mkdir "${_lockdir}" 2>/dev/null; then
        err "MySQL ${ver}: another ensure_mysql is already running (${_lockdir}). Aborting to avoid race."
        return 1
    fi

    # Delegate to helper so the lock dir is always cleaned up on any return path.
    local _rc=0
    _ensure_mysql_body "$ver" "$port" "$base" "$bin" || _rc=$?
    rmdir "${_lockdir}" 2>/dev/null || true
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
        pkill -9 -f "mysqld.*port=${port}" 2>/dev/null || true
        pkill -9 -f "mysqld.*--port=${port}" 2>/dev/null || true
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
        local _mysql_qcmd=(_fq_mysql_run "$base" "${base}/bin/mysql" -h 127.0.0.1 -P "$port"
            -u "${MYSQL_USER}" -p"${MYSQL_PASS}" --connect-timeout=5)
        while [[ $_qpi -lt 20 ]]; do
            if "${_mysql_qcmd[@]}" -e "SELECT 1;" >/dev/null 2>&1; then
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
        _fq_env_clean env LD_LIBRARY_PATH="$_ldlp_prefix" timeout "$init_timeout" "$mysqld" --initialize-insecure \
            --basedir="$base" \
            --datadir="$data" \
            $user_opt \
            2>"${log}/init.log" || _init_rc=$?
    else
        _fq_env_clean timeout "$init_timeout" "$mysqld" --initialize-insecure \
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
        OVERALL_OK=1; return 1
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
    [[ -d "$lib_private" ]] && _ldlp_prefix="${lib_private}"

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
    local mysql_bin="${base}/bin/mysql"
    local socket="${base}/run/mysqld.sock"
    local major; major="$(echo "$ver" | cut -d. -f1)"

    # Idempotent: if password already works, skip
    if _fq_mysql_run "$base" "$mysql_bin" -h 127.0.0.1 -P "$port" \
            -u root -p"${MYSQL_PASS}" \
            --connect-timeout=5 \
            -e "SELECT 1;" >/dev/null 2>&1; then
        info "MySQL ${ver}: auth already configured."
        return 0
    fi

    info "MySQL ${ver}: configuring root auth via UNIX socket ..."
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
    # SQL connections; the socket file may also not exist yet.  Retry for up to
    # 30 s, trying socket first (lower overhead), then TCP no-password.
    local _auth_pi=0
    while [[ $_auth_pi -lt 30 ]]; do
        # Try socket connection (no password, fresh --initialize-insecure)
        if [[ -S "$socket" ]] && _fq_mysql_run "$base" "$mysql_bin" \
                -u root -S "$socket" --connect-timeout=3 \
                -e "$auth_sql" 2>/dev/null; then
            info "MySQL ${ver}: auth configured via socket."
            return 0
        fi
        # Also try TCP no-password
        if _fq_mysql_run "$base" "$mysql_bin" -h 127.0.0.1 -P "$port" -u root \
                --connect-timeout=3 -e "$auth_sql" 2>/dev/null; then
            info "MySQL ${ver}: auth configured via TCP (no-password)."
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
    return 1
}

_mysql_apply_tls() {
    local ver="$1" port="$2" base="$3"
    local cert_dst="${base}/certs"
    local mysql_bin="${base}/bin/mysql"
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
        if _fq_mysql_run "$base" "$mysql_bin" -h 127.0.0.1 -P "$port" \
                -u "$MYSQL_USER" -p"$MYSQL_PASS" \
                --ssl-mode=DISABLED \
                --connect-timeout=5 \
                -e "SET PERSIST ssl_ca='${cert_dst}/ca.pem';
                    SET PERSIST ssl_cert='${cert_dst}/server.pem';
                    SET PERSIST ssl_key='${cert_dst}/server-key.pem';" \
            2>/dev/null; then
            info "MySQL ${ver}: TLS SET PERSIST applied."
        else
            warn "MySQL ${ver}: SET PERSIST failed."
        fi

        if _fq_mysql_run "$base" "$mysql_bin" -h 127.0.0.1 -P "$port" \
                -u "$MYSQL_USER" -p"$MYSQL_PASS" \
                --ssl-mode=DISABLED \
                --connect-timeout=5 \
                -e "ALTER INSTANCE RELOAD TLS;" \
            2>/dev/null; then
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
            local _tls_cmd=(_fq_mysql_run "$base" "${base}/bin/mysql" -h 127.0.0.1 -P "$port"
                -u "${MYSQL_USER}" -p"${MYSQL_PASS}" --connect-timeout=5)
            while [[ $_tls_pi -lt 20 ]]; do
                if "${_tls_cmd[@]}" -e "SELECT 1;" >/dev/null 2>&1; then
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
            local _tls_cmd=(_fq_mysql_run "$base" "${base}/bin/mysql" -h 127.0.0.1 -P "$port"
                -u "${MYSQL_USER}" -p"${MYSQL_PASS}" --connect-timeout=5)
            while [[ $_tls_pi -lt 20 ]]; do
                if "${_tls_cmd[@]}" -e "SELECT 1;" >/dev/null 2>&1; then
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
    local mysql_bin="${base}/bin/mysql"

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
    pkill -9 -f "mysqld.*port=${port}" 2>/dev/null || true
    pkill -9 -f "mysqld.*--port=${port}" 2>/dev/null || true
    # Also kill ANY mysqld using this basedir (may be running on a different port)
    pkill -9 -f "mysqld.*${base}" 2>/dev/null || true
    pkill -9 -f "mysqld.*initialize-insecure.*${base}" 2>/dev/null || true
    pkill -9 -f "mysqld.*${base}.*initialize-insecure" 2>/dev/null || true
    # Verify: wait until all mysqld processes for this base are truly gone.
    # Retry kill if processes survive the first attempt.
    if ! _wait_procs_gone "mysqld.*${base}" 100; then
        warn "MySQL ${ver}: processes survived first kill; retrying with individual SIGKILL ..."
        pgrep -f "mysqld.*${base}" 2>/dev/null | xargs -r kill -9 2>/dev/null || true
        if ! _wait_procs_gone "mysqld.*${base}" 100; then
            err "MySQL ${ver}: FATAL — mysqld processes still alive after two kill rounds"
            pgrep -af "mysqld.*${base}" 2>/dev/null >&2 || true
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
    _listening_pid=$(ss -tlnp "sport = :${port}" 2>/dev/null | sed -n 's/.*pid=\([0-9][0-9]*\).*/\1/p' | head -1)
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
    _mysql_apply_tls  "$ver" "$port" "$base"

    # 5. Create test users (tls_user, pool test user)
    local pool_user="${FQ_POOL_TEST_USER:-fq_pool_test}"
    local pool_pass="${FQ_POOL_TEST_PASS:-taosdata}"
    local pool_max_conn="${FQ_POOL_TEST_MAX_CONN:-1}"
    local mysql_cmd=(_fq_mysql_run "$base" "$mysql_bin" -h 127.0.0.1 -P "$port" -u "$MYSQL_USER" -p"$MYSQL_PASS" --connect-timeout=5)
    printf "DROP USER IF EXISTS 'tls_user'@'%%';\n\
CREATE USER 'tls_user'@'%%' IDENTIFIED BY 'tls_pwd' REQUIRE SSL;\n\
GRANT ALL PRIVILEGES ON *.* TO 'tls_user'@'%%';\n\
DROP USER IF EXISTS '%s'@'%%';\n\
CREATE USER '%s'@'%%' IDENTIFIED BY '%s' WITH MAX_USER_CONNECTIONS %s;\n\
GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%';\n\
FLUSH PRIVILEGES;" \
        "$pool_user" "$pool_user" "$pool_pass" "$pool_max_conn" "$pool_user" \
        | "${mysql_cmd[@]}" 2>/dev/null \
        || warn "MySQL ${ver}: test user setup had warnings."

    # 6. Connectivity probe (actual SQL connection, with pause between retries)
    local _pi=0
    while [[ $_pi -lt 30 ]]; do
        if "${mysql_cmd[@]}" -e "SELECT 1;" >/dev/null 2>&1; then
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
    if [[ "${FQ_PG_QUICK_RESTART:-0}" == "1" ]] && ! _pg_data_ready "$base"; then
        info "PostgreSQL ${ver}: no initialized data; performing full setup instead of quick restart."
        _pg_reset_env "$ver" "$port" "$base"
        return $?
    fi

    if [[ "${FQ_PG_QUICK_RESTART:-0}" == "1" ]]; then
        info "PostgreSQL ${ver}: quick restart (existing data preserved) ..."
        # Kill any stale postgres process before starting fresh
        pkill -9 -f "postgres.*-p ${port}" 2>/dev/null || true
        pkill -9 -f "postgres.*${base}" 2>/dev/null || true
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
            if PGPASSWORD="$PG_PASS" PGCONNECT_TIMEOUT=3 _fq_env_clean "$_qpsql" \
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

_pg_data_ready() {
    local base="$1"
    [[ -f "${base}/data/PG_VERSION" || -f "${base}/data/postgresql.conf" ]]
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
                local _pg_deb_cache=""; _pg_deb_cache="$(_apt_cache_file "$_pg_cache_key" || true)"
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
                    local keyring="/usr/share/postgresql-common/pgdg/apt.postgresql.org.gpg"
                    mkdir -p "$(dirname "$keyring")"
                    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
                        | gpg --dearmor -o "$keyring" 2>/dev/null \
                        || { warn "PGDG GPG key import failed; apt-key fallback ...";
                             curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
                                | apt-key add - 2>/dev/null; }
                    if [[ -s "$keyring" ]]; then
                        echo "deb [signed-by=${keyring}] https://apt.postgresql.org/pub/repos/apt ${codename}-pgdg main" \
                            > /etc/apt/sources.list.d/pgdg.list
                    else
                        echo "deb https://apt.postgresql.org/pub/repos/apt ${codename}-pgdg main" \
                            > /etc/apt/sources.list.d/pgdg.list
                    fi
                    apt-get update -qq
                fi
                info "PostgreSQL ${ver}: installing via apt ..."
                _apt_install_cached "pg${ver//./}" "postgresql-${ver}" \
                    || warn "PostgreSQL ${ver}: apt install failed; trying tarball fallback."
                local sys_bin="/usr/lib/postgresql/${ver}/bin"
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
        _fq_env_clean su -s /bin/sh "$pg_os_user" -c "$start_cmd" \
            2>>"${log}/pg_ctl.log" || true
    else
        _fq_env_clean "$pg_ctl" -D "$data" -l "${log}/pg.log" \
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
    # Idempotent
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
    pkill -9 -f "postgres.*-p ${port}" 2>/dev/null || true
    pkill -9 -f "postgres.*${base}" 2>/dev/null || true
    pkill -9 -f "initdb.*${base}" 2>/dev/null || true
    # Verify: wait until all postgres/initdb processes for this base are truly gone
    _wait_procs_gone "postgres.*${base}" 100 \
        || warn "PostgreSQL ${ver}: some processes may linger; continuing..."
    _wait_procs_gone "initdb.*${base}" 50 || true

    local _pg_shm_owner="${CURRENT_USER}"
    [[ "$CURRENT_USER" == "root" ]] && _pg_shm_owner="postgres"
    _cleanup_orphan_sysv_shm "$_pg_shm_owner"

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
    PGPASSWORD="$PG_PASS" _fq_env_clean "$psql" -h 127.0.0.1 -p "$port" -U "$PG_USER" \
        -d postgres -c "SELECT pg_reload_conf();" >/dev/null 2>&1 || true

    # 5. Connectivity probe (actual psql connection, with pause between retries)
    local _pi=0
    while [[ $_pi -lt 30 ]]; do
        if PGPASSWORD="$PG_PASS" PGCONNECT_TIMEOUT=3 _fq_env_clean "$psql" \
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
    if [[ "${FQ_INFLUX_QUICK_RESTART:-0}" == "1" ]] \
            && [[ ! -x "${bin}/influxdb3" && ! -x "${bin}/influxd" ]]; then
        info "InfluxDB ${ver}: no local binary; performing full setup instead of quick restart."
        FQ_INFLUX_QUICK_RESTART=0
    fi

    if [[ "${FQ_INFLUX_QUICK_RESTART:-0}" == "1" ]]; then
        info "InfluxDB ${ver}: quick restart (existing data preserved) ..."
        # Kill any stale process before starting fresh
        local pidfile="${base}/run/influxd.pid"
        if [[ -f "$pidfile" ]]; then
            kill -9 "$(cat "$pidfile")" 2>/dev/null || true
            rm -f "$pidfile"
        fi
        pkill -9 -f "influxdb3 serve.*${port}" 2>/dev/null || true
        # Verify: wait until all influxdb3 processes for this port are truly gone
        _wait_procs_gone "influxdb3 serve.*${port}" 100 \
            || warn "InfluxDB ${ver}: process may linger; continuing..."
        # Wait for TCP port to be released (probe-based, no busy-spin)
        _wait_port_free "$port" 30 \
            || warn "InfluxDB ${ver}: port ${port} still open; continuing..."
        _influx_start "$ver" "$port" "$base"
        if ! wait_port "$port" 90; then
            err "InfluxDB ${ver}: timed out on port ${port} after quick restart."
            OVERALL_OK=1; return 1
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
        warn "InfluxDB ${ver}: /health did not return pass/ok after quick restart (non-fatal)."
        _influx_ensure_token "$ver" "$port" "$base"
        info "InfluxDB ${ver}: quick restart complete."
        return 0
    fi

    if port_open "$port"; then
        # Port may be held by a stale process from another FQ_BASE_DIR session.
        if [[ ! -x "${bin}/influxdb3" && ! -x "${bin}/influxd" ]]; then
            info "InfluxDB ${ver}: port ${port} open but local binary missing — install then reset."
            pkill -9 -f "influxdb3 serve.*${port}" 2>/dev/null || true
            _wait_procs_gone "influxdb3 serve.*${port}" 100 || true
            _wait_port_free "$port" 30 || true
            _influx_install "$ver" "$base"
            _influx_reset_env "$ver" "$port" "$base"
            return 0
        fi
        # Port open — always do a full hard reset (kill → wipe data → restart).
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
    local url tarball
    url="$(_influx_binary_url "$ver")"

    # macOS: try Homebrew first
    if [[ "$OS" == "Darwin" ]] && command -v brew &>/dev/null; then
        info "InfluxDB ${ver}: trying Homebrew ..."
        brew install influxdb 2>/dev/null || true
    fi

    _ensure_linux_engine_deps
    mkdir -p "${base}/bin" "${base}/data" "${base}/log"
    tarball="$(_ensure_tarball_cached "fq-influxdb-${ver}.tar.gz" "$url")"

    # Strip top-level directory if present
    local top; top="$(tar -tzf "$tarball" 2>/dev/null | head -1 | cut -d/ -f1)"
    if [[ -n "$top" && "$top" != "influxdb3" && "$top" != "influxd" ]]; then
        _fq_env_clean tar -xzf "$tarball" --strip-components=1 -C "${base}/bin" 2>/dev/null || \
            _fq_env_clean tar -xzf "$tarball" -C "${base}/bin" 2>/dev/null || true
    else
        _fq_env_clean tar -xzf "$tarball" -C "${base}/bin" 2>/dev/null || true
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
    # Must use _fq_env_clean: influxdb3 crashes under ASAN LD_PRELOAD and may
    # pick up wrong libraries from TDengine's LD_LIBRARY_PATH.
    local token_output token_value
    token_output="$(_fq_env_clean "$influxd" create token --admin --host "http://127.0.0.1:${port}" 2>&1)"
    token_value="$(echo "$token_output" | grep -oE 'apiv3_[A-Za-z0-9_-]+' | head -1)"
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
                sed -n 's/.*"values":\[\["\([^"]*\)".*/\1/p' || true)
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
    pkill -9 -f "influxdb3 serve.*${port}" 2>/dev/null || true
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
    pkill -TERM -f "postgres.*-p ${port}" 2>/dev/null || true
    # Wait for graceful shutdown before escalating to SIGKILL
    _wait_procs_gone "postgres.*-p ${port}" 50 \
        || pkill -KILL -f "postgres.*-p ${port}" 2>/dev/null || true
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
    log "Teardown complete."
}

_probe_mysql_ready() {
    local ver="$1"
    local port; port="$(mysql_port "$ver")"
    local base="${FQ_BASE_DIR}/mysql/${ver}"
    local mysql_bin="${base}/bin/mysql"

    [[ -x "$mysql_bin" ]] || return 1
    _fq_mysql_run "$base" "$mysql_bin" -h 127.0.0.1 -P "$port" -u "$MYSQL_USER" -p"$MYSQL_PASS" \
        --connect-timeout=5 -e "SELECT 1;" >/dev/null 2>&1
}

_probe_pg_ready() {
    local ver="$1"
    local port; port="$(pg_port "$ver")"
    local base="${FQ_BASE_DIR}/pg/${ver}"
    local psql="${base}/bin/psql"

    [[ -x "$psql" ]] || return 1
    PGPASSWORD="$PG_PASS" PGCONNECT_TIMEOUT=5 _fq_env_clean "$psql" \
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
    local mysql_bin="${base}/bin/mysql"
    local log_file="${base}/log/error.log"

    if [[ ! -x "$mysql_bin" ]]; then
        err "MySQL ${ver}: final probe failed, mysql client missing: ${mysql_bin}"
        return 1
    fi

    local cmd=(_fq_mysql_run "$base" "$mysql_bin" -h 127.0.0.1 -P "$port" -u "$MYSQL_USER" -p"$MYSQL_PASS" --connect-timeout=5)
    local ok=0 i=0
    while [[ $i -lt 30 ]]; do
        # Require consecutive successful probes to avoid transient false-ready.
        if "${cmd[@]}" -Nse "SELECT 1;" >/dev/null 2>&1 \
           && "${cmd[@]}" -Nse "SHOW DATABASES;" >/dev/null 2>&1; then
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
        if PGPASSWORD="$PG_PASS" PGCONNECT_TIMEOUT=3 _fq_env_clean "$psql" \
               -h 127.0.0.1 -p "$port" -U "$PG_USER" -d postgres \
               -Atc "SELECT 1;" >/dev/null 2>&1 \
           && PGPASSWORD="$PG_PASS" PGCONNECT_TIMEOUT=3 _fq_env_clean "$psql" \
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
        *)      return 1 ;;
    esac
}

# ──────────────────────────────────────────────────────────────────────────────
# 13.  Main
# ──────────────────────────────────────────────────────────────────────────────

# Allow the script to be sourced by test harnesses without running main.
# Set FQ_SOURCE_ONLY=1 before sourcing to suppress execution.
main() {
    log "========================================================"
    log "FederatedQuery external-source setup"
    log "  OS       : ${OS} (${DISTRO}) / ${ARCH}"
    log "  User     : ${CURRENT_USER}"
    log "  Base dir : ${FQ_BASE_DIR}"
    log "  Cert src : ${CERT_SRC}"
    log "  MySQL    : ${MYSQL_VERSIONS[*]}"
    log "  PG       : ${PG_VERSIONS[*]}"
    log "  InfluxDB : ${INFLUX_VERSIONS[*]}"
    log "========================================================"

    mkdir -p "$FQ_BASE_DIR"
    _configure_apt_mirrors
    _ensure_linux_engine_deps

    local ver
    # FQ_SERVICES_TO_RESET controls which services are processed.
    # Default: all three.  In-test restarts set this to a single service.
    local _services="${FQ_SERVICES_TO_RESET:-mysql pg influx}"
    # Run PG first because package/setup steps are heavier and may transiently
    # impact other daemons in constrained containers.
    # Use if/fi instead of [[ ]] && ... to avoid set -e exit when the test is false.
    if [[ "$_services" == *pg* ]]; then
        for ver in "${PG_VERSIONS[@]}"; do ensure_pg "$ver" || OVERALL_OK=1; done
    fi
    if [[ "$_services" == *mysql* ]]; then
        for ver in "${MYSQL_VERSIONS[@]}"; do ensure_mysql "$ver" || OVERALL_OK=1; done
    fi
    if [[ "$_services" == *influx* ]]; then
        for ver in "${INFLUX_VERSIONS[@]}"; do ensure_influx "$ver" || OVERALL_OK=1; done
    fi

    if _is_single_service_quick_restart "$_services"; then
        log "Single-service quick restart complete; caller will run readiness probe."
        [[ "$OVERALL_OK" -eq 0 ]] || exit 1
        return 0
    fi

    # start_*_instance() sets FQ_SERVICES_TO_RESET to one engine and probes
    # readiness itself.  Skipping verify/recovery here avoids a second hard
    # reset while the first mysqld/PG/Influx init is still in flight.
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
        *)          main "$@"    ;;
    esac
fi
