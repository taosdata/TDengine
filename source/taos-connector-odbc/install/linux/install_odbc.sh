#!/bin/bash
###############################################################################
# TDengine ODBC Connector - Installation Script
#
# This script installs the TDengine ODBC driver and configures DSN data sources.
#
# Prerequisites:
#   - TDengine client (libtaos.so) already installed
#   - unixODBC (libodbc, odbcinst) already installed
#   - libtaosws.so (optional, for WebSocket mode)
#
# Usage:
#   sudo ./install_odbc.sh [OPTIONS]
#
# Options:
#   --server HOST:PORT    TDengine server address (default: localhost:6030)
#   --ws-url  URL         WebSocket URL (default: http://localhost:6041)
#   --lib-dir DIR         Directory containing libtaos_odbc.so (default: ./lib)
#   --uninstall           Remove ODBC driver and DSN configuration
#   --help                Show this help message
###############################################################################

set -euo pipefail

# ============================= Configuration ==================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LIB_DIR="${SCRIPT_DIR}/lib"
INSTALL_LIB_DIR="/usr/local/lib"
DRIVER_SO="libtaos_odbc.so"
DRIVER_SO_VER="libtaos_odbc.so.0.1"
SERVER="localhost:6030"
WS_URL="http://localhost:6041"
UNINSTALL=0

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# ============================= Functions ======================================

usage() {
    sed -n '/^# Usage:/,/^###/p' "$0" | head -n -1 | sed 's/^# //'
    exit 0
}

log_info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

check_root() {
    if [ "$(id -u)" -ne 0 ]; then
        log_error "This script must be run as root (use sudo)."
        exit 1
    fi
}

check_prerequisites() {
    log_info "Checking prerequisites..."

    # Check unixODBC
    if ! command -v odbcinst &>/dev/null; then
        log_error "odbcinst not found. Please install unixODBC:"
        log_error "  Ubuntu/Debian: apt-get install unixodbc unixodbc-dev"
        log_error "  CentOS/RHEL:   yum install unixODBC unixODBC-devel"
        exit 1
    fi
    log_info "  unixODBC: $(odbcinst --version 2>/dev/null || echo 'installed')"

    # Check TDengine client
    if ! ldconfig -p 2>/dev/null | grep -q libtaos.so; then
        if [ ! -f /usr/lib/libtaos.so ] && [ ! -f /usr/local/lib/libtaos.so ]; then
            log_warn "libtaos.so not found in system library path."
            log_warn "TDengine client library may not be installed."
            log_warn "Continuing anyway - ensure libtaos.so is available at runtime."
        fi
    fi
    log_info "  TDengine client: OK"

    # Check driver library
    if [ ! -f "${LIB_DIR}/${DRIVER_SO_VER}" ] && [ ! -f "${LIB_DIR}/${DRIVER_SO}" ]; then
        log_error "ODBC driver library not found in ${LIB_DIR}/"
        log_error "Expected: ${LIB_DIR}/${DRIVER_SO_VER}"
        log_error ""
        log_error "Please copy the built libraries to ${LIB_DIR}/:"
        log_error "  mkdir -p ${LIB_DIR}"
        log_error "  cp <build_dir>/src/libtaos_odbc.so.0.1 ${LIB_DIR}/"
        log_error "  cp <build_dir>/src/libtaos_odbc.so ${LIB_DIR}/"
        exit 1
    fi
    log_info "  ODBC driver library: OK"
}

install_driver_library() {
    log_info "Installing ODBC driver library..."

    cp -f "${LIB_DIR}/${DRIVER_SO_VER}" "${INSTALL_LIB_DIR}/${DRIVER_SO_VER}"
    ln -sf "${DRIVER_SO_VER}" "${INSTALL_LIB_DIR}/${DRIVER_SO}"
    chmod 755 "${INSTALL_LIB_DIR}/${DRIVER_SO_VER}"

    # Update linker cache
    ldconfig 2>/dev/null || true

    log_info "  Installed: ${INSTALL_LIB_DIR}/${DRIVER_SO_VER}"
    log_info "  Symlink:   ${INSTALL_LIB_DIR}/${DRIVER_SO}"
}

configure_driver() {
    log_info "Registering ODBC driver..."

    local tmpdir
    tmpdir=$(mktemp -d)

    # Create odbcinst.ini for driver registration
    cat > "${tmpdir}/odbcinst.ini" <<EOF
[TDengine]
Description=TDengine 3.0 ODBC Driver
Driver=${INSTALL_LIB_DIR}/${DRIVER_SO}
CPTimeout=60

[TAOS_ODBC_DRIVER]
Description=TDengine 3.0 ODBC Driver
Driver=${INSTALL_LIB_DIR}/${DRIVER_SO}
CPTimeout=60
EOF

    # Uninstall existing entries (ignore errors)
    odbcinst -u -d -l -n TDengine 2>/dev/null || true
    odbcinst -u -d -l -n TAOS_ODBC_DRIVER 2>/dev/null || true

    # Install driver to system odbcinst.ini
    odbcinst -i -d -f "${tmpdir}/odbcinst.ini" -l
    log_info "  Driver 'TDengine' registered in /etc/odbcinst.ini"
    log_info "  Driver 'TAOS_ODBC_DRIVER' registered in /etc/odbcinst.ini"

    rm -rf "${tmpdir}"
}

configure_dsn() {
    log_info "Configuring DSN data sources..."

    local tmpdir
    tmpdir=$(mktemp -d)

    # Create odbc.ini for DSN configuration
    cat > "${tmpdir}/odbc_native.ini" <<EOF
[TAOS_ODBC_DSN]
Description=Native connection to TDengine 3.0
Driver=TAOS_ODBC_DRIVER
SERVER=${SERVER}
DB=
UNSIGNED_PROMOTION=
TIMESTAMP_AS_IS=
CHARSET_ENCODER_FOR_PARAM_BIND=
CHARSET_ENCODER_FOR_COL_BIND=
EOF

    cat > "${tmpdir}/odbc_ws.ini" <<EOF
[TAOS_ODBC_WS_DSN]
Description=Websocket connection to TDengine 3.0
Driver=TAOS_ODBC_DRIVER
SERVER=
URL=${WS_URL}
DB=
UNSIGNED_PROMOTION=
TIMESTAMP_AS_IS=
CHARSET_ENCODER_FOR_PARAM_BIND=
CHARSET_ENCODER_FOR_COL_BIND=
CONN_MODE=
EOF

    # Uninstall existing DSN entries (ignore errors)
    odbcinst -u -s -l -n TAOS_ODBC_DSN 2>/dev/null || true
    odbcinst -u -s -l -n TAOS_ODBC_WS_DSN 2>/dev/null || true
    odbcinst -u -s -h -n TAOS_ODBC_DSN 2>/dev/null || true
    odbcinst -u -s -h -n TAOS_ODBC_WS_DSN 2>/dev/null || true

    # Install DSN to system odbc.ini (/etc/odbc.ini)
    odbcinst -i -s -f "${tmpdir}/odbc_native.ini" -l
    odbcinst -i -s -f "${tmpdir}/odbc_ws.ini" -l
    log_info "  DSN 'TAOS_ODBC_DSN' configured in /etc/odbc.ini"
    log_info "  DSN 'TAOS_ODBC_WS_DSN' configured in /etc/odbc.ini"

    # Also install to user's home directory (~/.odbc.ini)
    odbcinst -i -s -f "${tmpdir}/odbc_native.ini" -h
    odbcinst -i -s -f "${tmpdir}/odbc_ws.ini" -h
    log_info "  DSN 'TAOS_ODBC_DSN' configured in ~/.odbc.ini"
    log_info "  DSN 'TAOS_ODBC_WS_DSN' configured in ~/.odbc.ini"

    rm -rf "${tmpdir}"
}

verify_installation() {
    log_info "Verifying installation..."

    # Check driver file exists
    if [ ! -f "${INSTALL_LIB_DIR}/${DRIVER_SO}" ]; then
        log_error "  Driver library not found at ${INSTALL_LIB_DIR}/${DRIVER_SO}"
        return 1
    fi
    log_info "  Driver library: OK"

    # Check driver registration
    if odbcinst -q -d -n TAOS_ODBC_DRIVER &>/dev/null; then
        log_info "  Driver registration: OK"
    else
        log_warn "  Driver registration: could not verify"
    fi

    # List registered drivers
    log_info "  Registered ODBC drivers:"
    odbcinst -q -d 2>/dev/null | while read -r line; do
        log_info "    $line"
    done

    # List configured DSN
    log_info "  Configured system DSN:"
    odbcinst -q -s -l 2>/dev/null | while read -r line; do
        log_info "    $line"
    done

    log_info "  Configured user DSN:"
    odbcinst -q -s -h 2>/dev/null | while read -r line; do
        log_info "    $line"
    done
}

do_uninstall() {
    log_info "Uninstalling TDengine ODBC connector..."

    # Remove DSN
    odbcinst -u -s -l -n TAOS_ODBC_DSN 2>/dev/null && log_info "  Removed system DSN: TAOS_ODBC_DSN" || true
    odbcinst -u -s -l -n TAOS_ODBC_WS_DSN 2>/dev/null && log_info "  Removed system DSN: TAOS_ODBC_WS_DSN" || true
    odbcinst -u -s -h -n TAOS_ODBC_DSN 2>/dev/null && log_info "  Removed user DSN: TAOS_ODBC_DSN" || true
    odbcinst -u -s -h -n TAOS_ODBC_WS_DSN 2>/dev/null && log_info "  Removed user DSN: TAOS_ODBC_WS_DSN" || true

    # Remove driver registration
    odbcinst -u -d -l -n TDengine 2>/dev/null && log_info "  Removed driver: TDengine" || true
    odbcinst -u -d -l -n TAOS_ODBC_DRIVER 2>/dev/null && log_info "  Removed driver: TAOS_ODBC_DRIVER" || true

    # Remove library files
    if [ -f "${INSTALL_LIB_DIR}/${DRIVER_SO_VER}" ]; then
        rm -f "${INSTALL_LIB_DIR}/${DRIVER_SO_VER}"
        rm -f "${INSTALL_LIB_DIR}/${DRIVER_SO}"
        ldconfig 2>/dev/null || true
        log_info "  Removed library files from ${INSTALL_LIB_DIR}/"
    fi

    log_info "Uninstall complete."
    exit 0
}

# ============================= Main ===========================================

# Parse arguments
while [ $# -gt 0 ]; do
    case "$1" in
        --server)
            SERVER="$2"; shift 2 ;;
        --ws-url)
            WS_URL="$2"; shift 2 ;;
        --lib-dir)
            LIB_DIR="$2"; shift 2 ;;
        --uninstall)
            UNINSTALL=1; shift ;;
        --help|-h)
            usage ;;
        *)
            log_error "Unknown option: $1"
            usage ;;
    esac
done

echo "============================================================"
echo "  TDengine ODBC Connector Installer"
echo "============================================================"

check_root

if [ "${UNINSTALL}" -eq 1 ]; then
    do_uninstall
fi

check_prerequisites
install_driver_library
configure_driver
configure_dsn
verify_installation

echo ""
echo "============================================================"
log_info "Installation complete!"
echo ""
log_info "DSN Configuration:"
log_info "  Native:    TAOS_ODBC_DSN    (SERVER=${SERVER})"
log_info "  WebSocket: TAOS_ODBC_WS_DSN (URL=${WS_URL})"
echo ""
log_info "Test with: isql -v TAOS_ODBC_DSN root taosdata"
echo "============================================================"
