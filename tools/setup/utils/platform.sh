#!/bin/bash
# ============================================================================
# platform.sh — OS / architecture / package manager detection
# ============================================================================

# ── detect_os() ──────────────────────────────────────────────────────────────
# Sets: SETUP_OS (linux | macos)
detect_os() {
    case "$(uname -s)" in
        Linux*)  SETUP_OS="linux" ;;
        Darwin*) SETUP_OS="macos" ;;
        *)       SETUP_OS="unknown" ;;
    esac
}

# ── detect_arch() ────────────────────────────────────────────────────────────
# Sets: SETUP_ARCH (amd64 | arm64)
detect_arch() {
    case "$(uname -m)" in
        x86_64)        SETUP_ARCH="amd64" ;;
        aarch64|arm64) SETUP_ARCH="arm64" ;;
        *)             SETUP_ARCH="$(uname -m)" ;;
    esac
}

# ── detect_distro() ─────────────────────────────────────────────────────────
# Sets: SETUP_DISTRO (ubuntu | debian | centos | rhel | fedora | alma | rocky | macos)
detect_distro() {
    if [[ "$SETUP_OS" == "macos" ]]; then
        SETUP_DISTRO="macos"
        return
    fi
    if [[ -f /etc/os-release ]]; then
        # shellcheck disable=SC1091
        . /etc/os-release
        case "$ID" in
            ubuntu)          SETUP_DISTRO="ubuntu" ;;
            debian)          SETUP_DISTRO="debian" ;;
            centos)          SETUP_DISTRO="centos" ;;
            rhel|redhat)     SETUP_DISTRO="rhel" ;;
            fedora)          SETUP_DISTRO="fedora" ;;
            almalinux|alma)  SETUP_DISTRO="alma" ;;
            rocky)           SETUP_DISTRO="rocky" ;;
            *)               SETUP_DISTRO="$ID" ;;
        esac
    else
        SETUP_DISTRO="unknown"
    fi
}

# ── detect_pkg_mgr() ────────────────────────────────────────────────────────
# Sets: PKG_MGR (apt | yum | dnf | brew)
detect_pkg_mgr() {
    if cmd_exists brew; then
        PKG_MGR="brew"
    elif cmd_exists dnf; then
        PKG_MGR="dnf"
    elif cmd_exists apt-get; then
        PKG_MGR="apt"
    elif cmd_exists yum; then
        PKG_MGR="yum"
    else
        PKG_MGR="unknown"
    fi
}

# ── pkg_install() — cross-platform package install ──────────────────────────
pkg_install() {
    case "$PKG_MGR" in
        apt)  sudo apt-get install -y "$@" ;;
        yum)  sudo yum install -y "$@" ;;
        dnf)  sudo dnf install -y "$@" ;;
        brew) brew install "$@" ;;
        *)
            fail "Unknown package manager: $PKG_MGR"
            fail "Please install manually: $*"
            return 1
            ;;
    esac
}

# ── init_platform() — run all detection steps ───────────────────────────────
init_platform() {
    detect_os
    detect_arch
    detect_distro
    detect_pkg_mgr
}
