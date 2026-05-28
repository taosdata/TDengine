#!/bin/bash
# ============================================================================
# modules/python.sh — Python3 + pip + maturin toolchain configuration
# ============================================================================

mod_python_check() {
    header "Python Toolchain"

    # python3
    if cmd_exists python3; then
        local ver
        ver=$(python3 --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
        if version_gte "$ver" "$REQUIRED_PYTHON_VERSION"; then
            ok "python3 $ver (>= $REQUIRED_PYTHON_VERSION)"
        else
            warn "python3 $ver (need >= $REQUIRED_PYTHON_VERSION)"
            ISSUES_FOUND=$((ISSUES_FOUND + 1))
        fi
    else
        fail "python3 not found"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    fi

    # pip
    if cmd_exists pip3 || python3 -m pip --version >/dev/null 2>&1; then
        local pip_ver
        pip_ver=$(pip3 --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+' | head -1) || \
        pip_ver=$(python3 -m pip --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+' | head -1)
        ok "pip $pip_ver"
    else
        warn "pip not found"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    fi

    # maturin (for taos-connector-python Rust bindings)
    if cmd_exists maturin || python3 -m maturin --version >/dev/null 2>&1; then
        local mat_ver
        mat_ver=$(maturin --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+') || \
        mat_ver=$(python3 -m maturin --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
        ok "maturin $mat_ver"
    else
        info "maturin not installed (needed for connector-python)"
    fi

    # pip index
    local pip_index
    pip_index=$(pip3 config get global.index-url 2>/dev/null || echo "")
    if [[ "$pip_index" == *"nora.tdengine.net"* ]]; then
        ok "pip index → internal Nora mirror"
    elif [[ -n "$pip_index" && "$pip_index" != "https://pypi.org/simple" ]]; then
        info "pip index: $pip_index"
    fi
}

mod_python_install() {
    # python3
    if ! cmd_exists python3; then
        if confirm "Install Python 3?"; then
            case "$PKG_MGR" in
                brew) brew install python ;;
                apt)  pkg_install python3 python3-pip python3-venv ;;
                yum|dnf) pkg_install python3 python3-pip ;;
            esac
            CHANGES_MADE=$((CHANGES_MADE + 1))
        fi
    fi

    # pip
    if ! cmd_exists pip3 && ! python3 -m pip --version >/dev/null 2>&1; then
        if confirm "Install pip?"; then
            case "$PKG_MGR" in
                brew) : ;; # brew python includes pip
                apt)  pkg_install python3-pip ;;
                yum|dnf) pkg_install python3-pip ;;
            esac
            CHANGES_MADE=$((CHANGES_MADE + 1))
        fi
    fi

    # maturin
    if ! cmd_exists maturin; then
        if confirm "Install maturin (for connector-python Rust bindings)?"; then
            pip3 install maturin --user 2>/dev/null || python3 -m pip install maturin --user
            CHANGES_MADE=$((CHANGES_MADE + 1))
        fi
    fi
}

mod_python_config() {
    if [[ "${TSDB_PUBLIC_DEPS:-0}" == "1" ]]; then
        ok "Public mode: using default PyPI"
        return 0
    fi

    # Configure pip index → internal Nora PyPI mirror
    # PYPI_MIRROR and PYPI_TRUSTED_HOST are set by config.sh from .build-args
    local nora_pypi_url="${PYPI_MIRROR:-https://nora.tdengine.net/simple/}"
    local trusted_host="${PYPI_TRUSTED_HOST:-nora.tdengine.net}"

    local current_index
    current_index=$(pip3 config get global.index-url 2>/dev/null || echo "")
    if [[ "$current_index" == *"nora.tdengine.net"* ]]; then
        return 0
    fi

    if confirm "Set pip index-url → internal Nora PyPI mirror?"; then
        pip3 config set global.index-url "$nora_pypi_url" 2>/dev/null || \
            python3 -m pip config set global.index-url "$nora_pypi_url"
        pip3 config set global.trusted-host "$trusted_host" 2>/dev/null || \
            python3 -m pip config set global.trusted-host "$trusted_host"
        ok "pip index set to $nora_pypi_url"
        CHANGES_MADE=$((CHANGES_MADE + 1))
    fi
}
