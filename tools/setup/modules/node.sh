#!/bin/bash
# ============================================================================
# modules/node.sh — Node.js + pnpm toolchain configuration
# ============================================================================

mod_node_check() {
    header "Node.js Toolchain"

    # Node.js
    if cmd_exists node; then
        local ver
        ver=$(node --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
        if version_gte "$ver" "$REQUIRED_NODE_VERSION"; then
            ok "node $ver (>= $REQUIRED_NODE_VERSION)"
        else
            warn "node $ver (need >= $REQUIRED_NODE_VERSION)"
            ISSUES_FOUND=$((ISSUES_FOUND + 1))
        fi
    else
        fail "node not found"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    fi

    # pnpm
    if cmd_exists pnpm; then
        ok "pnpm $(pnpm --version 2>/dev/null)"
    else
        warn "pnpm not found"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    fi

    # npm registry
    if cmd_exists npm; then
        local registry
        registry=$(npm config get registry 2>/dev/null)
        if [[ "$registry" == *"nexus.tdengine.net"* ]] || \
           [[ "$registry" == *"nora.tdengine.net"* ]]; then
            ok "npm registry → internal mirror"
        else
            info "npm registry: $registry (public)"
        fi
    fi
}

mod_node_install() {
    # Node.js
    if ! cmd_exists node || ! version_gte "$(node --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')" "$REQUIRED_NODE_VERSION"; then
        if confirm "Install/upgrade Node.js?"; then
            case "$PKG_MGR" in
                brew)
                    brew install node
                    ;;
                apt)
                    # Use NodeSource for up-to-date versions
                    if [[ ! -f /etc/apt/sources.list.d/nodesource.list ]]; then
                        info "Adding NodeSource repository..."
                        curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
                    fi
                    pkg_install nodejs
                    ;;
                yum|dnf)
                    if [[ ! -f /etc/yum.repos.d/nodesource*.repo ]]; then
                        info "Adding NodeSource repository..."
                        curl -fsSL https://rpm.nodesource.com/setup_22.x | sudo bash -
                    fi
                    pkg_install nodejs
                    ;;
            esac
            CHANGES_MADE=$((CHANGES_MADE + 1))
        fi
    fi

    # pnpm
    if ! cmd_exists pnpm; then
        if confirm "Install pnpm?"; then
            if cmd_exists corepack; then
                corepack enable
                corepack prepare pnpm@latest --activate 2>/dev/null || npm install -g pnpm
            else
                npm install -g pnpm
            fi
            CHANGES_MADE=$((CHANGES_MADE + 1))
        fi
    fi
}

mod_node_config() {
    # Configure npm/pnpm registry → internal Nora npm mirror
    # NPM_REGISTRY_URL is set by config.sh from .build-args
    local nora_npm_url="${NPM_REGISTRY_URL:-https://nora.tdengine.net/npm/}"

    if cmd_exists npm; then
        local current_registry
        current_registry=$(npm config get registry 2>/dev/null)
        if [[ "$current_registry" == *"nora.tdengine.net"* ]]; then
            return 0
        fi

        if confirm "Set npm registry → internal Nora mirror?"; then
            npm config set registry "$nora_npm_url"
            ok "npm registry set to $nora_npm_url"
            CHANGES_MADE=$((CHANGES_MADE + 1))
        fi
    fi
}
