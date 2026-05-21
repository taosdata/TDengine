#!/bin/bash
# ============================================================================
# modules/rust.sh — Rust toolchain + Nora registry + sccache
# ============================================================================

mod_rust_check() {
    header "Rust Toolchain"

    # rustc
    if cmd_exists rustc; then
        local ver
        ver=$(rustc --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
        if version_gte "$ver" "$REQUIRED_RUST_VERSION"; then
            ok "rustc $ver (>= $REQUIRED_RUST_VERSION)"
        else
            warn "rustc $ver (need >= $REQUIRED_RUST_VERSION)"
            ISSUES_FOUND=$((ISSUES_FOUND + 1))
        fi
    else
        fail "rustc not found"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    fi

    # cargo
    if cmd_exists cargo; then
        ok "cargo $(cargo --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')"
    else
        fail "cargo not found"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    fi

    # cargo config → Nora
    local cargo_config="$HOME/.cargo/config.toml"
    if [[ -f "$cargo_config" ]] && grep -qF "nora.tdengine.net" "$cargo_config"; then
        ok "Cargo registry → Nora (internal)"
    else
        warn "Cargo not configured for internal Nora registry"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    fi

    # protoc
    if cmd_exists protoc; then
        ok "protoc $(protoc --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?')"
    else
        warn "protoc not found (required for taosx gRPC)"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    fi

    # sccache (optional)
    if cmd_exists sccache; then
        ok "sccache $(sccache --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')"
    else
        info "sccache not installed (optional, for clean-build caching)"
    fi
}

mod_rust_install() {
    # rustup + toolchain
    if ! cmd_exists rustc || ! version_gte "$(rustc --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')" "$REQUIRED_RUST_VERSION"; then
        if confirm "Install/upgrade Rust via rustup?"; then
            if cmd_exists rustup; then
                rustup update stable
            else
                curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
                # shellcheck disable=SC1091
                [[ -f "$HOME/.cargo/env" ]] && source "$HOME/.cargo/env"
            fi
            CHANGES_MADE=$((CHANGES_MADE + 1))
        fi
    fi

    # protoc
    if ! cmd_exists protoc; then
        if confirm "Install protoc?"; then
            case "$PKG_MGR" in
                brew) pkg_install protobuf ;;
                apt)  pkg_install protobuf-compiler ;;
                yum|dnf) pkg_install protobuf-compiler ;;
            esac
            CHANGES_MADE=$((CHANGES_MADE + 1))
        fi
    fi
}

mod_rust_config() {
    local cargo_config="$HOME/.cargo/config.toml"

    # Cargo config.toml → Nora
    if [[ -f "$cargo_config" ]] && grep -qF "nora.tdengine.net" "$cargo_config"; then
        return 0
    fi

    if [[ -n "$CARGO_CONFIG_SRC" ]]; then
        info "Cargo config source: $CARGO_CONFIG_SRC"
    else
        info "Will write default Nora registry config"
    fi

    if confirm "Write Cargo config to $cargo_config?"; then
        mkdir -p "$(dirname "$cargo_config")"
        backup_file "$cargo_config"

        if [[ -n "$CARGO_CONFIG_SRC" ]]; then
            cp "$CARGO_CONFIG_SRC" "$cargo_config"
        else
            cat > "$cargo_config" <<'CARGO_EOF'
[source.crates-io]
replace-with = 'internal'

[source.internal]
registry = "sparse+https://nora.tdengine.net/cargo/index/"

[registries.internal]
index = "sparse+https://nora.tdengine.net/cargo/index/"

[http]
multiplexing = false
timeout = 120

[net]
git-fetch-with-cli = true
CARGO_EOF
        fi
        ok "Cargo config written to $cargo_config"
        CHANGES_MADE=$((CHANGES_MADE + 1))
    fi

    # sccache (optional, don't push)
    if ! cmd_exists sccache; then
        if confirm "Install sccache (optional, for clean-build caching)?"; then
            info "Installing sccache via cargo..."
            if cargo install sccache --locked 2>&1; then
                ok "sccache installed"
                CHANGES_MADE=$((CHANGES_MADE + 1))
            else
                fail "sccache installation failed (non-critical)"
            fi
        fi
    fi
}
