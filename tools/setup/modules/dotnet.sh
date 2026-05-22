#!/bin/bash
# ============================================================================
# modules/dotnet.sh — .NET SDK toolchain configuration
# ============================================================================

mod_dotnet_check() {
    header ".NET Toolchain"

    if cmd_exists dotnet; then
        local ver
        ver=$(dotnet --version 2>/dev/null)
        ok "dotnet SDK $ver"

        # List installed SDKs
        local sdk_count
        sdk_count=$(dotnet --list-sdks 2>/dev/null | wc -l | tr -d ' ')
        info "$sdk_count SDK(s) installed"
    else
        fail "dotnet not found"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    fi

    # NuGet source
    if cmd_exists dotnet; then
        if dotnet nuget list source 2>/dev/null | grep -qF "nora.tdengine.net"; then
            ok "NuGet source → internal mirror"
        else
            info "NuGet using default sources"
        fi
    fi
}

mod_dotnet_install() {
    if cmd_exists dotnet; then
        return 0
    fi

    if ! confirm "Install .NET SDK?"; then return 0; fi

    case "$PKG_MGR" in
        brew)
            brew install dotnet
            ;;
        apt)
            # Microsoft packages repository
            if [[ ! -f /etc/apt/sources.list.d/microsoft-prod.list ]]; then
                info "Adding Microsoft packages repository..."
                local distro_name
                distro_name=$(. /etc/os-release && echo "$VERSION_CODENAME")
                curl -fsSL "https://packages.microsoft.com/config/ubuntu/$(. /etc/os-release && echo "$VERSION_ID")/packages-microsoft-prod.deb" -o /tmp/ms-prod.deb
                sudo dpkg -i /tmp/ms-prod.deb
                rm -f /tmp/ms-prod.deb
                sudo apt-get update
            fi
            pkg_install dotnet-sdk-8.0
            ;;
        yum|dnf)
            pkg_install dotnet-sdk-8.0
            ;;
    esac
    CHANGES_MADE=$((CHANGES_MADE + 1))
}

mod_dotnet_config() {
    if ! cmd_exists dotnet; then
        return 0
    fi

    # NuGet source → internal mirror (if available)
    # NUGET_SOURCE_URL is set by config.sh from .build-args
    local nora_nuget_url="${NUGET_SOURCE_URL:-https://nora.tdengine.net/nuget/v3/index.json}"

    if dotnet nuget list source 2>/dev/null | grep -qF "nora.tdengine.net"; then
        return 0
    fi

    if confirm "Add internal NuGet source (Nora)?"; then
        dotnet nuget add source "$nora_nuget_url" \
            --name "tdengine-internal" 2>/dev/null || true
        ok "NuGet source added: $nora_nuget_url"
        CHANGES_MADE=$((CHANGES_MADE + 1))
    fi
}
