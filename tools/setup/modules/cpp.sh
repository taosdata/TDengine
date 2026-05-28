#!/bin/bash
# ============================================================================
# modules/cpp.sh — C/C++ toolchain: cmake, compiler, ccache, conan
# ============================================================================

mod_cpp_check() {
    header "C/C++ Toolchain"

    # cmake
    if cmd_exists cmake; then
        local ver
        ver=$(cmake --version | head -1 | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?')
        if version_gte "$ver" "$REQUIRED_CMAKE_VERSION"; then
            ok "cmake $ver (>= $REQUIRED_CMAKE_VERSION)"
        else
            warn "cmake $ver (need >= $REQUIRED_CMAKE_VERSION)"
            ISSUES_FOUND=$((ISSUES_FOUND + 1))
        fi
    else
        fail "cmake not found"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    fi

    # compiler
    if cmd_exists gcc; then
        ok "gcc $(gcc -dumpversion 2>/dev/null || echo '?')"
    elif cmd_exists clang; then
        ok "clang $(clang --version 2>/dev/null | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')"
    else
        fail "No C/C++ compiler found (gcc or clang)"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    fi

    # ccache
    if cmd_exists ccache; then
        ok "ccache $(ccache --version 2>/dev/null | head -1 | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?')"
        if rc_has_line "CMAKE_C_COMPILER_LAUNCHER=ccache"; then
            ok "CMAKE_*_COMPILER_LAUNCHER configured"
        else
            warn "ccache installed but CMAKE_*_COMPILER_LAUNCHER not set in $SHELL_RC"
            ISSUES_FOUND=$((ISSUES_FOUND + 1))
        fi
    else
        warn "ccache not installed (recommended for faster rebuilds)"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    fi

    # conan
    if cmd_exists conan; then
        ok "conan $(conan --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?')"
    else
        warn "conan not installed (required for taos-gen)"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    fi
}

mod_cpp_install() {
    # cmake
    if ! cmd_exists cmake || ! version_gte "$(cmake --version | head -1 | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?')" "$REQUIRED_CMAKE_VERSION"; then
        if confirm "Install/upgrade cmake?"; then
            case "$PKG_MGR" in
                brew) pkg_install cmake ;;
                apt)  pkg_install cmake build-essential ;;
                yum|dnf) pkg_install cmake gcc gcc-c++ make ;;
            esac
            CHANGES_MADE=$((CHANGES_MADE + 1))
        fi
    fi

    # compiler
    if ! cmd_exists gcc && ! cmd_exists clang; then
        if confirm "Install C/C++ compiler?"; then
            case "$PKG_MGR" in
                brew) xcode-select --install 2>/dev/null || true ;;
                apt)  pkg_install build-essential ;;
                yum|dnf) pkg_install gcc gcc-c++ make ;;
            esac
            CHANGES_MADE=$((CHANGES_MADE + 1))
        fi
    fi

    # ccache
    if ! cmd_exists ccache; then
        if confirm "Install ccache?"; then
            pkg_install ccache
            CHANGES_MADE=$((CHANGES_MADE + 1))
        fi
    fi

    # conan
    if ! cmd_exists conan; then
        if confirm "Install conan (via virtualenv)?"; then
            local conan_venv="$HOME/.local/share/tsdb-setup/conan-venv"
            case "$PKG_MGR" in
                apt) pkg_install python3-venv ;;
            esac
            python3 -m venv "$conan_venv"
            "$conan_venv/bin/pip" install --upgrade pip
            "$conan_venv/bin/pip" install conan
            rc_append "export PATH=${conan_venv}/bin:\$PATH" "cpp"
            CHANGES_MADE=$((CHANGES_MADE + 1))
        fi
    fi
}

mod_cpp_config() {
    # ccache CMAKE_*_COMPILER_LAUNCHER
    if cmd_exists ccache && ! rc_has_line "CMAKE_C_COMPILER_LAUNCHER=ccache"; then
        if confirm "Configure ccache as default cmake compiler launcher?"; then
            rc_append "export CMAKE_C_COMPILER_LAUNCHER=ccache" "cpp"
            rc_append "export CMAKE_CXX_COMPILER_LAUNCHER=ccache" "cpp"
            ok "CMAKE_*_COMPILER_LAUNCHER=ccache written to $SHELL_RC"
            CHANGES_MADE=$((CHANGES_MADE + 1))
        fi
    fi

    if [[ "${TSDB_PUBLIC_DEPS:-0}" == "1" ]]; then
        ok "Public mode: using Conan Center (default)"
        return 0
    fi

    # conan remote
    if cmd_exists conan; then
        local current_url
        current_url=$(conan remote list 2>/dev/null | grep -oE 'https?://[^ ]+' | head -1)
        if [[ "$current_url" == *"nexus.tdengine.net"* ]]; then
            ok "Conan remote already points to internal Nexus"
        else
            info "Conan remote: $CONAN_REMOTE_URL"
            if confirm "Configure Conan remote to internal Nexus?"; then
                conan remote add internal "$CONAN_REMOTE_URL" --force 2>/dev/null || true
                ok "Conan remote 'internal' added"
                CHANGES_MADE=$((CHANGES_MADE + 1))
            fi
        fi
    fi
}
