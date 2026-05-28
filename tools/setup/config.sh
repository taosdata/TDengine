#!/bin/bash
# ============================================================================
# config.sh — component→language mapping, version requirements, mirror URLs
#
# Reads from tools/tsdb-builder/.build-args as single source of truth.
# Provides fallback defaults for all values.
# ============================================================================

# ── Locate tsdb-builder config ──────────────────────────────────────────────
SETUP_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILDER_DIR="$(cd "$SETUP_SCRIPT_DIR/../tsdb-builder" 2>/dev/null && pwd)" || BUILDER_DIR=""

# ── Read mirror URLs from .build-args ───────────────────────────────────────
if [[ -n "$BUILDER_DIR" && -f "$BUILDER_DIR/.build-args" ]]; then
    _ba="$BUILDER_DIR/.build-args"
    GO_VERSION=$(grep '^GO_VERSION=' "$_ba" | cut -d= -f2-)
    GO_PROXY=$(grep '^GO_PROXY=' "$_ba" | cut -d= -f2-)
    CARGO_REGISTRY_URL=$(grep '^CARGO_REGISTRY_URL=' "$_ba" | cut -d= -f2-)
    CONAN_REMOTE_URL=$(grep '^CONAN_REMOTE_URL=' "$_ba" | cut -d= -f2-)
    NPM_REGISTRY_URL=$(grep '^NPM_REGISTRY_URL=' "$_ba" | cut -d= -f2-)
    MAVEN_MIRROR_URL=$(grep '^MAVEN_MIRROR_URL=' "$_ba" | cut -d= -f2-)
    NUGET_SOURCE_URL=$(grep '^NUGET_SOURCE_URL=' "$_ba" | cut -d= -f2-)
    PYPI_MIRROR=$(grep '^PYPI_INTERNAL_URL=' "$_ba" | cut -d= -f2-)
    PYPI_TRUSTED_HOST=$(grep '^PYPI_INTERNAL_HOST=' "$_ba" | cut -d= -f2-)
fi

# ── Public mode ─────────────────────────────────────────────────────────────
# Set TSDB_PUBLIC_DEPS=1 to use public (internet) dependency sources.
# This overrides all internal mirror URLs with public defaults.
if [[ "${TSDB_PUBLIC_DEPS:-0}" == "1" ]]; then
    if [[ -n "$BUILDER_DIR" && -f "$BUILDER_DIR/.build-args" ]]; then
        _ba="$BUILDER_DIR/.build-args"
        GO_PROXY=$(grep '^GO_PUBLIC_PROXY=' "$_ba" | cut -d= -f2-)
        NPM_REGISTRY_URL=$(grep '^NPM_PUBLIC_URL=' "$_ba" | cut -d= -f2-)
        MAVEN_MIRROR_URL=$(grep '^MAVEN_PUBLIC_URL=' "$_ba" | cut -d= -f2-)
        NUGET_SOURCE_URL=$(grep '^NUGET_PUBLIC_URL=' "$_ba" | cut -d= -f2-)
        PYPI_MIRROR=$(grep '^PYPI_PUBLIC_URL=' "$_ba" | cut -d= -f2-)
        PYPI_TRUSTED_HOST=$(grep '^PYPI_PUBLIC_HOST=' "$_ba" | cut -d= -f2-)
        CARGO_REGISTRY_URL=""  # empty = use default crates.io
        CONAN_REMOTE_URL=""    # empty = use built-in conancenter
    fi
fi

# Fallback defaults (public or internal depending on TSDB_PUBLIC_DEPS)
if [[ "${TSDB_PUBLIC_DEPS:-0}" == "1" ]]; then
    GO_PROXY="${GO_PROXY:-https://proxy.golang.org}"
    CARGO_REGISTRY_URL="${CARGO_REGISTRY_URL:-}"
    CONAN_REMOTE_URL="${CONAN_REMOTE_URL:-}"
    NPM_REGISTRY_URL="${NPM_REGISTRY_URL:-https://registry.npmjs.org/}"
    MAVEN_MIRROR_URL="${MAVEN_MIRROR_URL:-https://repo.maven.apache.org/maven2/}"
    NUGET_SOURCE_URL="${NUGET_SOURCE_URL:-https://api.nuget.org/v3/index.json}"
    PYPI_MIRROR="${PYPI_MIRROR:-https://pypi.org/simple/}"
    PYPI_TRUSTED_HOST="${PYPI_TRUSTED_HOST:-pypi.org}"
else
    GO_PROXY="${GO_PROXY:-https://nexus.tdengine.net/repository/goproxy/}"
    CARGO_REGISTRY_URL="${CARGO_REGISTRY_URL:-sparse+https://nora.tdengine.net/cargo/index/}"
    CONAN_REMOTE_URL="${CONAN_REMOTE_URL:-https://nexus.tdengine.net/repository/conan/}"
    NPM_REGISTRY_URL="${NPM_REGISTRY_URL:-https://nora.tdengine.net/npm/}"
    MAVEN_MIRROR_URL="${MAVEN_MIRROR_URL:-https://nexus.tdengine.net/repository/maven-public/}"
    NUGET_SOURCE_URL="${NUGET_SOURCE_URL:-https://nora.tdengine.net/nuget/v3/index.json}"
    PYPI_MIRROR="${PYPI_MIRROR:-https://nora.tdengine.net/simple/}"
    PYPI_TRUSTED_HOST="${PYPI_TRUSTED_HOST:-nora.tdengine.net}"
fi

# Cargo config source file (for direct copy)
CARGO_CONFIG_SRC=""
if [[ -n "$BUILDER_DIR" ]]; then
    if [[ "${TSDB_PUBLIC_DEPS:-0}" == "1" && -f "$BUILDER_DIR/.cargo/config.toml.public" ]]; then
        CARGO_CONFIG_SRC="$BUILDER_DIR/.cargo/config.toml.public"
    elif [[ -f "$BUILDER_DIR/.cargo/config.toml" ]]; then
        CARGO_CONFIG_SRC="$BUILDER_DIR/.cargo/config.toml"
    fi
fi

# ── Minimum version requirements ────────────────────────────────────────────
REQUIRED_CMAKE_VERSION="3.21"
REQUIRED_GO_VERSION="1.23"
GO_TOOLCHAIN_VERSION="${GO_VERSION:-1.23.4}"
REQUIRED_RUST_VERSION="1.90"
REQUIRED_JAVA_VERSION="17"
REQUIRED_NODE_VERSION="18"
REQUIRED_PYTHON_VERSION="3.10"

# ── Component → language mapping ────────────────────────────────────────────
# Usage: get_langs_for_component "taosx" → prints "rust"
#
# Implemented with indexed arrays for bash 3.x compatibility (macOS).
_COMPONENT_NAMES=(
    engine enterprise adapter keeper taosx gen insight
    connector-jdbc connector-go connector-node
    connector-python connector-rust connector-dotnet connector-odbc
)
_COMPONENT_LANGS=(
    "cpp"             # engine
    "cpp"             # enterprise
    "go"              # adapter
    "go"              # keeper
    "rust"            # taosx
    "cpp"             # gen
    "go node"         # insight
    "java"            # connector-jdbc
    "go"              # connector-go
    "node"            # connector-node
    "python rust"     # connector-python
    "rust"            # connector-rust
    "dotnet"          # connector-dotnet
    "cpp"             # connector-odbc
)

get_langs_for_component() {
    local comp="$1"
    local i
    for i in "${!_COMPONENT_NAMES[@]}"; do
        if [[ "${_COMPONENT_NAMES[$i]}" == "$comp" ]]; then
            echo "${_COMPONENT_LANGS[$i]}"
            return 0
        fi
    done
    echo ""
    return 1
}

# ── List all known components ────────────────────────────────────────────────
list_components() {
    local i
    for i in "${!_COMPONENT_NAMES[@]}"; do
        printf "  %-22s → %s\n" "${_COMPONENT_NAMES[$i]}" "${_COMPONENT_LANGS[$i]}"
    done
}

# ── All unique language modules ──────────────────────────────────────────────
ALL_LANG_MODULES="cpp go rust java node python dotnet"
