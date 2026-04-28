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
#   FQ_INFLUX_TOKEN/ORG    credentials            default test-token / test-org
#   FQ_POOL_TEST_USER      pool-exhaustion test MySQL user   default fq_pool_test
#   FQ_POOL_TEST_PASS      pool-exhaustion test user password default taosdata
#   FQ_POOL_TEST_MAX_CONN  MAX_USER_CONNECTIONS for pool test user  default 1
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
# Tarball cache directory — mount a host path here to avoid re-downloading
FQ_TARBALL_CACHE_DIR="${FQ_TARBALL_CACHE_DIR:-/tmp}"

IFS=',' read -ra MYSQL_VERSIONS  <<< "${FQ_MYSQL_VERSIONS:-8.0}"
IFS=',' read -ra PG_VERSIONS     <<< "${FQ_PG_VERSIONS:-16}"
IFS=',' read -ra INFLUX_VERSIONS <<< "${FQ_INFLUX_VERSIONS:-3.0}"

MYSQL_USER="${FQ_MYSQL_USER:-root}"
MYSQL_PASS="${FQ_MYSQL_PASS:-taosdata}"
PG_USER="${FQ_PG_USER:-postgres}"
PG_PASS="${FQ_PG_PASS:-taosdata}"
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

# Start a daemon via nohup, record PID in pidfile, return immediately
# Usage: _start_daemon <pidfile> <logfile> <cmd> [args...]
_start_daemon() {
    local pidfile="$1" logfile="$2"
    shift 2
    mkdir -p "$(dirname "$pidfile")" "$(dirname "$logfile")"
    nohup "$@" >> "$logfile" 2>&1 &
    echo "$!" > "$pidfile"
}

# Stop a daemon by pidfile; fall back to pattern kill
_stop_daemon() {
    local pidfile="$1" pattern="$2"
    if [[ -f "$pidfile" ]]; then
        local pid
        pid=$(cat "$pidfile")
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
            sleep 2
            kill -0 "$pid" 2>/dev/null && kill -KILL "$pid" 2>/dev/null || true
        fi
        rm -f "$pidfile"
        return
    fi
    _kill_matching "$pattern" TERM
    sleep 2
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

ensure_mysql() {
    local ver="$1"
    local port; port="$(mysql_port "$ver")"
    local base="${FQ_BASE_DIR}/mysql/${ver}"
    local bin="${base}/bin"
    local log="${base}/log"

    info "MySQL ${ver}: port=${port}, base=${base}"

    # ── already running ───────────────────────────────────────────────────────
    if port_open "$port"; then
        info "MySQL ${ver}: port ${port} is open — already running, resetting test env."
        _mysql_reset_env "$ver" "$port" "$base"
        return 0
    fi

    # ── installed but stopped ────────────────────────────────────────────────
    if [[ -x "${bin}/mysqld" ]]; then
        info "MySQL ${ver}: installation found, attempting start ..."
        _mysql_start "$ver" "$port" "$base"
        if wait_port "$port" 30; then
            info "MySQL ${ver}: started OK."
            _mysql_reset_env "$ver" "$port" "$base"
            return 0
        fi
        warn "MySQL ${ver}: failed to start existing installation; reinitializing data dir."
        rm -rf "${base}/data"
    fi

    # ── fresh install ─────────────────────────────────────────────────────────
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
            info "MySQL ${ver}: downloading tarball ..."
            local url; url="$(_mysql_tarball_url "$ver")"
            local tarball="${FQ_TARBALL_CACHE_DIR}/fq-mysql-${ver}.tar.xz"
            [[ -s "$tarball" ]] || _download_with_retry "$url" "$tarball"
            mkdir -p "$base"
            tar -xJf "$tarball" --strip-components=1 -C "$base"
            ;;
    esac

    info "MySQL ${ver}: initializing data directory ..."
    _mysql_init "$ver" "$base"

    info "MySQL ${ver}: starting ..."
    _mysql_start "$ver" "$port" "$base"

    if ! wait_port "$port" 90; then
        err "MySQL ${ver}: timed out waiting for port ${port}."
        tail -20 "${log}/error.log" 2>/dev/null >&2 || true
        OVERALL_OK=1; return 1
    fi

    _mysql_setup_auth "$ver" "$port" "$base"
    _mysql_apply_tls  "$ver" "$port" "$base"
    _mysql_reset_env  "$ver" "$port" "$base"
    info "MySQL ${ver}: ready."
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
    [[ -d "$lib_private" ]] && _ldlp_prefix="${lib_private}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

    # mysqld refuses to run as 'root' unless --user=root is explicit
    local user_opt="--user=${CURRENT_USER}"
    [[ "$CURRENT_USER" == "root" ]] && user_opt="--user=root"

    # --initialize-insecure: root@localhost with empty password
    if [[ -n "$_ldlp_prefix" ]]; then
        LD_LIBRARY_PATH="$_ldlp_prefix" "$mysqld" --initialize-insecure \
            --basedir="$base" \
            --datadir="$data" \
            $user_opt \
            2>>"${log}/init.log" \
            || { err "MySQL ${ver}: initdb failed; check ${log}/init.log"; OVERALL_OK=1; return 1; }
    else
        "$mysqld" --initialize-insecure \
            --basedir="$base" \
            --datadir="$data" \
            $user_opt \
            2>>"${log}/init.log" \
            || { err "MySQL ${ver}: initdb failed; check ${log}/init.log"; OVERALL_OK=1; return 1; }
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
                "${tls_args[@]}"
    fi
}

_mysql_setup_auth() {
    local ver="$1" port="$2" base="$3"
    local mysql_bin="${base}/bin/mysql"
    local socket="${base}/run/mysqld.sock"
    local major; major="$(echo "$ver" | cut -d. -f1)"

    # Idempotent: if password already works, skip
    if "$mysql_bin" -h 127.0.0.1 -P "$port" \
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

    # Try socket connection (no password, fresh --initialize-insecure)
    if "$mysql_bin" -u root -S "$socket" --connect-timeout=10 \
            -e "$auth_sql" 2>/dev/null; then
        info "MySQL ${ver}: auth configured via socket."
        return 0
    fi
    # Also try with skip-grant-tables approach: just TCP no-password
    if "$mysql_bin" -h 127.0.0.1 -P "$port" -u root \
            --connect-timeout=5 -e "$auth_sql" 2>/dev/null; then
        info "MySQL ${ver}: auth configured via TCP (no-password)."
        return 0
    fi
    warn "MySQL ${ver}: could not configure auth automatically (socket=${socket})."
    return 1
}

_mysql_apply_tls() {
    local ver="$1" port="$2" base="$3"
    local cert_dst="${base}/certs"
    local mysql_bin="${base}/bin/mysql"
    local major; major="$(echo "$ver" | cut -d. -f1)"

    if [[ -f "${cert_dst}/ca.pem" ]]; then
        info "MySQL ${ver}: TLS certs already present, skipping."
        return 0
    fi

    info "MySQL ${ver}: deploying TLS certificates ..."
    mkdir -p "$cert_dst"
    cp "${CERT_SRC}/ca.pem"                   "${cert_dst}/ca.pem"
    cp "${CERT_SRC}/mysql/server.pem"          "${cert_dst}/server.pem"
    cp "${CERT_SRC}/mysql/server-key.pem"      "${cert_dst}/server-key.pem"
    cp "${CERT_SRC}/mysql/client.pem"          "${cert_dst}/client.pem"
    cp "${CERT_SRC}/mysql/client-key.pem"      "${cert_dst}/client-key.pem"
    chmod 640 "${cert_dst}/server.pem" "${cert_dst}/server-key.pem" \
              "${cert_dst}/client.pem" "${cert_dst}/client-key.pem"

    if [[ "$major" -ge 8 ]]; then
        "$mysql_bin" -h 127.0.0.1 -P "$port" \
                -u "$MYSQL_USER" -p"$MYSQL_PASS" \
                --connect-timeout=5 \
                -e "SET PERSIST ssl_ca='${cert_dst}/ca.pem';
                    SET PERSIST ssl_cert='${cert_dst}/server.pem';
                    SET PERSIST ssl_key='${cert_dst}/server-key.pem';" \
            2>/dev/null \
            && info "MySQL ${ver}: TLS SET PERSIST applied." \
            || warn "MySQL ${ver}: SET PERSIST failed – needs manual restart."
    else
        # MySQL 5.7: no SET PERSIST; write option file and restart
        cat > "${base}/my-tls.cnf" <<MYCNF
[mysqld]
ssl_ca=${cert_dst}/ca.pem
ssl_cert=${cert_dst}/server.pem
ssl_key=${cert_dst}/server-key.pem
MYCNF
        local pidfile="${base}/run/mysqld.pid"
        _stop_daemon "$pidfile" "mysqld.*port=${port}"
        sleep 1
        _mysql_start "$ver" "$port" "$base"
        wait_port "$port" 30 \
            || warn "MySQL ${ver}: did not come back after TLS restart."
    fi
}

_mysql_reset_env() {
    local ver="$1" port="$2" base="$3"
    local mysql_bin="${base}/bin/mysql"
    local mysql_cmd=("$mysql_bin" -h 127.0.0.1 -P "$port" -u "$MYSQL_USER" -p"$MYSQL_PASS" --connect-timeout=5)
    info "MySQL ${ver} @ ${port}: resetting test databases ..."

    # Discover all non-system databases and drop them
    local dbs
    dbs=$("${mysql_cmd[@]}" -N -e \
        "SELECT schema_name FROM information_schema.schemata \
         WHERE schema_name NOT IN ('mysql','information_schema','performance_schema','sys');" \
        2>/dev/null) || true
    local drop_sql=""
    local db
    for db in $dbs; do
        drop_sql+="DROP DATABASE IF EXISTS \`${db}\`;\n"
    done
    drop_sql+="DROP USER IF EXISTS 'tls_user'@'%';\n"
    drop_sql+="CREATE USER 'tls_user'@'%' IDENTIFIED BY 'tls_pwd' REQUIRE SSL;\n"
    drop_sql+="GRANT ALL PRIVILEGES ON *.* TO 'tls_user'@'%';\n"
    # Pool-exhaustion test user: limited to FQ_POOL_TEST_MAX_CONN concurrent connections.
    # Tests saturate this limit to trigger TSDB_CODE_EXT_RESOURCE_EXHAUSTED, then verify
    # the client-side delayed retry recovers automatically.
    local pool_user="${FQ_POOL_TEST_USER:-fq_pool_test}"
    local pool_pass="${FQ_POOL_TEST_PASS:-taosdata}"
    local pool_max_conn="${FQ_POOL_TEST_MAX_CONN:-1}"
    drop_sql+="DROP USER IF EXISTS '${pool_user}'@'%';\n"
    drop_sql+="CREATE USER '${pool_user}'@'%' IDENTIFIED BY '${pool_pass}' WITH MAX_USER_CONNECTIONS ${pool_max_conn};\n"
    drop_sql+="GRANT ALL PRIVILEGES ON *.* TO '${pool_user}'@'%';\n"
    drop_sql+="FLUSH PRIVILEGES;"

    echo -e "$drop_sql" | "${mysql_cmd[@]}" 2>/dev/null \
        && info "MySQL ${ver} @ ${port}: reset complete (dropped: ${dbs//$'\n'/ })." \
        || warn "MySQL ${ver} @ ${port}: reset had warnings."
}

# ──────────────────────────────────────────────────────────────────────────────
# 10.  PostgreSQL
# ──────────────────────────────────────────────────────────────────────────────
ensure_pg() {
    local ver="$1"
    local port; port="$(pg_port "$ver")"
    local base="${FQ_BASE_DIR}/pg/${ver}"
    local bin="${base}/bin"
    local log="${base}/log"

    info "PostgreSQL ${ver}: port=${port}, base=${base}"

    if port_open "$port"; then
        info "PostgreSQL ${ver}: port ${port} open — already running, resetting test env."
        _pg_reset_env "$ver" "$port" "$base"
        return 0
    fi

    if [[ -x "${bin}/pg_ctl" ]]; then
        info "PostgreSQL ${ver}: installation found, attempting start ..."
        _pg_start "$ver" "$port" "$base"
        if wait_port "$port" 30; then
            info "PostgreSQL ${ver}: started OK."
            return 0
        fi
        warn "PostgreSQL ${ver}: failed to start; reinitializing data dir."
        # Kill any lingering postgres process (e.g. when PG_VERSION is missing,
        # pg_ctl may have started but immediately exited; ensure no zombie holds
        # the data dir before we wipe it)
        if [[ -f "${base}/data/postmaster.pid" ]]; then
            local _pg_stale_pid; _pg_stale_pid="$(head -1 "${base}/data/postmaster.pid" 2>/dev/null || true)"
            if [[ -n "${_pg_stale_pid}" && "${_pg_stale_pid}" =~ ^[0-9]+$ ]]; then
                kill -TERM "${_pg_stale_pid}" 2>/dev/null || true
                sleep 1
                kill -0 "${_pg_stale_pid}" 2>/dev/null && kill -KILL "${_pg_stale_pid}" 2>/dev/null || true
            fi
        fi
        rm -rf "${base}/data"
    fi

    _pg_install "$ver" "$base"
    _pg_init    "$ver" "$base"
    _pg_start   "$ver" "$port" "$base"

    if ! wait_port "$port" 90; then
        err "PostgreSQL ${ver}: timed out on port ${port}."
        tail -20 "${log}/pg.log" 2>/dev/null >&2 || true
        OVERALL_OK=1; return 1
    fi

    _pg_reset_env "$ver" "$port" "$base"
    info "PostgreSQL ${ver}: ready."
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
            for f in pg_ctl initdb psql postgres createdb dropdb; do
                [[ -x "${brew_bin}/${f}" ]] && ln -sf "${brew_bin}/${f}" "${base}/bin/${f}"
            done
            return 0
            ;;
        Linux)
            if command -v apt-get &>/dev/null; then
                # Check if version available in default apt cache
                if ! apt-cache show "postgresql-${ver}" &>/dev/null 2>&1; then
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
                _pkg_install "postgresql-${ver}"
                local sys_bin="/usr/lib/postgresql/${ver}/bin"
                if [[ -d "$sys_bin" ]]; then
                    mkdir -p "${base}/bin"
                    # ln -sfn works on Linux; on macOS use individual links
                    ln -sfn "${sys_bin}"/* "${base}/bin/" 2>/dev/null || \
                        for f in pg_ctl initdb psql postgres createdb dropdb; do
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
                    for f in pg_ctl initdb psql postgres createdb dropdb; do
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
                for f in pg_ctl initdb psql postgres; do
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
    tar -xjf "$tarball" --strip-components=1 -C "$base"
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

    local initdb_cmd=("$initdb" -D "$data" -U "$PG_USER" --pwfile="$pwfile" --encoding=UTF8 --locale=C)
    if [[ "$CURRENT_USER" == "root" ]]; then
        su -s /bin/sh "$pg_os_user" -c "${initdb_cmd[*]}" \
            2>>"${log}/initdb.log" \
            || { err "PostgreSQL ${ver}: initdb failed; check ${log}/initdb.log"; rm -f "$pwfile"; OVERALL_OK=1; return 1; }
    else
        "${initdb_cmd[@]}" \
            2>>"${log}/initdb.log" \
            || { err "PostgreSQL ${ver}: initdb failed; check ${log}/initdb.log"; rm -f "$pwfile"; OVERALL_OK=1; return 1; }
    fi
    rm -f "$pwfile"
}

_pg_start() {
    local ver="$1" port="$2" base="$3"
    local data="${base}/data" log="${base}/log"
    local pg_ctl="${base}/bin/pg_ctl"
    mkdir -p "$log"

    # Apply TLS config if certs already present
    local cert_dst="${base}/data/certs"
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
    local psql="${base}/bin/psql"
    local cert_dst="${base}/data/certs"

    # Deploy certs on first call
    if [[ ! -d "$cert_dst" ]]; then
        info "PostgreSQL ${ver}: deploying TLS certificates ..."
        mkdir -p "$cert_dst"
        cp "${CERT_SRC}/ca.pem"              "${cert_dst}/ca.pem"
        cp "${CERT_SRC}/pg/server.pem"       "${cert_dst}/server.pem"
        cp "${CERT_SRC}/pg/server.key"       "${cert_dst}/server.key"
        cp "${CERT_SRC}/pg/client.pem"       "${cert_dst}/client.pem"
        cp "${CERT_SRC}/pg/client-key.pem"   "${cert_dst}/client-key.pem"
        chmod 600 "${cert_dst}/server.key" "${cert_dst}/client-key.pem"
        _pg_write_ssl_conf "${base}/data" "$cert_dst"
        PGPASSWORD="$PG_PASS" "$psql" -h 127.0.0.1 -p "$port" -U "$PG_USER" \
            -d postgres -c "SELECT pg_reload_conf();" >/dev/null 2>&1 || true
    fi

    info "PostgreSQL ${ver} @ ${port}: resetting test databases ..."
    # Discover all non-system databases and drop them
    local dbs
    dbs=$(PGPASSWORD="$PG_PASS" PGCONNECT_TIMEOUT=5 "$psql" \
        -h 127.0.0.1 -p "$port" -U "$PG_USER" -d postgres \
        -t -A \
        -c "SELECT datname FROM pg_database WHERE datistemplate = false AND datname <> 'postgres';" \
        2>/dev/null) || true
    local drop_sql=""
    local db
    for db in $dbs; do
        [[ -z "$db" ]] && continue
        # Terminate active connections before dropping
        drop_sql+="SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${db}' AND pid <> pg_backend_pid();\n"
        drop_sql+="DROP DATABASE IF EXISTS \"${db}\";\n"
    done
    if [[ -n "$drop_sql" ]]; then
        echo -e "$drop_sql" | PGPASSWORD="$PG_PASS" PGCONNECT_TIMEOUT=5 "$psql" \
            -h 127.0.0.1 -p "$port" -U "$PG_USER" -d postgres \
            >/dev/null 2>/dev/null
    fi
    info "PostgreSQL ${ver} @ ${port}: reset complete (dropped: ${dbs//$'\n'/ })."
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
    # Map logical version to pinned stable patch releases
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

    if port_open "$port"; then
        info "InfluxDB ${ver}: port ${port} open — already running, resetting test env."
        _influx_reset_env "$ver" "$port" "$base"
        return 0
    fi

    if [[ -x "${bin}/influxdb3" ]] || [[ -x "${bin}/influxd" ]]; then
        info "InfluxDB ${ver}: installation found, attempting start ..."
        _influx_start "$ver" "$port" "$base"
        if wait_port "$port" 30; then
            info "InfluxDB ${ver}: started OK."
            _influx_reset_env "$ver" "$port" "$base"
            return 0
        fi
        warn "InfluxDB ${ver}: failed to restart; re-installing ..."
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
               | grep -qE '"status":"(pass|ok)"'; do
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
        # InfluxDB 3.x: no --bearer-token at startup; run without auth for test env
        _start_daemon "$pidfile" "${log}/influxd.log" \
            "$influxd" serve \
            --node-id "fq-test-node" \
            --http-bind "127.0.0.1:${port}" \
            --object-store file \
            --data-dir "$data" \
            --without-auth
    else
        # influxd v2 fallback
        _start_daemon "$pidfile" "${log}/influxd.log" \
            "$influxd" \
            --http-bind-address "127.0.0.1:${port}" \
            --storage-wal-directory "${data}/wal" \
            --storage-data-path "$data"
    fi
}

_influx_reset_env() {
    local ver="$1" port="$2" base="$3"

    info "InfluxDB ${ver} @ ${port}: resetting test databases ..."
    # Discover all databases via REST API, drop everything except _internal
    local dbs_json db_list db
    dbs_json=$(curl -sf "http://127.0.0.1:${port}/api/v3/configure/database?format=json" 2>/dev/null) || true
    if [[ -n "$dbs_json" ]]; then
        # Parse JSON array: [{"iox::database":"name"}, ...]
        db_list=$(echo "$dbs_json" | sed 's/},{/}\n{/g' | grep -oP '"iox::database":"\K[^"]+' || true)
    fi
    local dropped=()
    for db in $db_list; do
        [[ "$db" == "_internal" ]] && continue
        curl -sf -X DELETE \
            "http://127.0.0.1:${port}/api/v3/configure/database?db=${db}" \
            -o /dev/null 2>/dev/null || true
        dropped+=("$db")
    done
    info "InfluxDB ${ver} @ ${port}: reset complete (dropped: ${dropped[*]})."
}

# ──────────────────────────────────────────────────────────────────────────────
# 12.  Main
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

    local ver
    for ver in "${MYSQL_VERSIONS[@]}";  do ensure_mysql  "$ver" || OVERALL_OK=1; done
    for ver in "${PG_VERSIONS[@]}";     do ensure_pg     "$ver" || OVERALL_OK=1; done
    for ver in "${INFLUX_VERSIONS[@]}"; do ensure_influx "$ver" || OVERALL_OK=1; done

    if [[ "$OVERALL_OK" -ne 0 ]]; then
        err "One or more engines failed to start. See messages above."
        exit 1
    fi
    log "All engines ready."
}

# Run main only when executed directly (not when sourced)
if [[ "${FQ_SOURCE_ONLY:-0}" != "1" && "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
