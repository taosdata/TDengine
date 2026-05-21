#!/bin/bash
# ============================================================================
# modules/go.sh — Go toolchain + GOPROXY configuration
# ============================================================================

mod_go_check() {
    header "Go Toolchain"

    if cmd_exists go; then
        local ver
        ver=$(go version | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?')
        if version_gte "$ver" "$REQUIRED_GO_VERSION"; then
            ok "go $ver (>= $REQUIRED_GO_VERSION)"
        else
            warn "go $ver (need >= $REQUIRED_GO_VERSION)"
            ISSUES_FOUND=$((ISSUES_FOUND + 1))
        fi
    else
        fail "go not found"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    fi

    # GOPROXY
    local current_goproxy="${GOPROXY:-}"
    if [[ "$current_goproxy" == *"nexus.tdengine.net"* ]] || \
       (rc_has_line "GOPROXY=" && grep -qF "$GO_PROXY" "$SHELL_RC" 2>/dev/null); then
        ok "GOPROXY configured to internal proxy"
    else
        warn "GOPROXY not set to internal proxy"
        info "Expected: ${GO_PROXY},direct"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    fi
}

mod_go_install() {
    if cmd_exists go && version_gte "$(go version | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?')" "$REQUIRED_GO_VERSION"; then
        return 0
    fi

    if ! confirm "Install/upgrade Go?"; then return 0; fi

    case "$PKG_MGR" in
        brew)
            brew install go
            ;;
        apt|yum|dnf)
            local go_arch="$SETUP_ARCH"
            [[ "$go_arch" == "arm64" ]] && go_arch="arm64"
            local go_url="https://go.dev/dl/go${REQUIRED_GO_VERSION}.linux-${go_arch}.tar.gz"
            info "Downloading Go from $go_url"
            curl -fsSL "$go_url" -o /tmp/go.tar.gz
            sudo rm -rf /usr/local/go
            sudo tar -C /usr/local -xzf /tmp/go.tar.gz
            rm -f /tmp/go.tar.gz
            rc_append 'export PATH=/usr/local/go/bin:$PATH' "go"
            ;;
    esac
    CHANGES_MADE=$((CHANGES_MADE + 1))
}

mod_go_config() {
    local expected="${GO_PROXY},direct"

    # GOPROXY
    if [[ "${GOPROXY:-}" == *"$GO_PROXY"* ]]; then
        return 0
    fi
    if rc_has_line "GOPROXY=" && grep -qF "$GO_PROXY" "$SHELL_RC" 2>/dev/null; then
        return 0
    fi

    if confirm "Set GOPROXY to internal proxy in $SHELL_RC?"; then
        # Remove old GOPROXY line if present
        if grep -q '^export GOPROXY=' "$SHELL_RC" 2>/dev/null; then
            sed -i.bak '/^export GOPROXY=/d' "$SHELL_RC"
        fi
        rc_append "export GOPROXY=${expected}" "go"
        ok "GOPROXY written to $SHELL_RC"
        CHANGES_MADE=$((CHANGES_MADE + 1))
    fi

    # GONOSUMDB / GONOSUMCHECK for internal modules
    if ! rc_has_line "GONOSUMDB"; then
        rc_append 'export GONOSUMDB="github.com/taosdata/*"' "go"
        rc_append 'export GONOSUMCHECK="github.com/taosdata/*"' "go"
    fi
}
