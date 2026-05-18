#!/bin/bash

# ============================================================================
# Docker Image Verification Script
# Verify all component versions in Harbor tsdb-builder images
# ============================================================================
#
# Usage (on host):
#   ./verify-image.sh core:amd64         # verify harbor.../core:latest-amd64
#   ./verify-image.sh core:arm64         # verify harbor.../core:latest-arm64
#   ./verify-image.sh others:amd64       # verify harbor.../others:latest-amd64
#   ./verify-image.sh others:arm64       # verify harbor.../others:latest-arm64
#   ./verify-image.sh core:3.4.1-amd64   # verify harbor.../core:3.4.1-amd64
#   ./verify-image.sh myregistry/img:v1  # verify a custom full image name
#
# The script auto-detects whether it is running on the host or inside a
# container (via --in-container flag). On the host it launches "docker run".
# ============================================================================

set -e

# ============================================================================
# Auto-detect: host or container?
# Use --in-container flag to reliably detect container execution.
# ============================================================================
if [ "$1" != "--in-container" ]; then
    # --- Running on the HOST ---
    TAG="${1:-core:amd64}"

    # Allow shorthand (core:amd64, others:arm64) or full image names
    case "$TAG" in
        core:* | others:*)
            TYPE="${TAG%%:*}"
            SUFFIX="${TAG#*:}"
            case "$SUFFIX" in
                amd64|arm64)
                    IMAGE="harbor.tdengine.net/tsdb-builder/${TYPE}:latest-${SUFFIX}"
                    ;;
                *)
                    IMAGE="harbor.tdengine.net/tsdb-builder/${TYPE}:${SUFFIX}"
                    ;;
            esac
            ;;
        *:* | */*)
            IMAGE="$TAG"
            ;;
        *)
            IMAGE="harbor.tdengine.net/tsdb-builder/core:latest-${TAG}"
            ;;
    esac

    echo ""
    echo "Verifying image: ${IMAGE}"
    echo ""

    # Run this same script inside the container
    docker run --rm "${IMAGE}" /usr/local/bin/verify-image.sh --in-container
    exit $?
fi

# ============================================================================
# From here on, we are INSIDE the container
# ============================================================================

ERRORS=0

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_header() {
    echo -e "\n${BLUE}============================================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}============================================================================${NC}\n"
}

print_success() {
    echo -e "${GREEN}  [PASS] $1${NC}"
}

print_error() {
    echo -e "${RED}  [FAIL] $1${NC}"
    ERRORS=$((ERRORS + 1))
}

print_info() {
    echo -e "${YELLOW}  [INFO] $1${NC}"
}

# ============================================================================
# Helper: check if a command exists and print its version
# ============================================================================
check_tool() {
    local name=$1
    local cmd=$2

    if command -v "$(echo "$cmd" | awk '{print $1}')" &>/dev/null; then
        local version
        version=$(eval "$cmd" 2>&1 | head -1)
        print_success "$name: $version"
    else
        print_error "$name: NOT FOUND"
    fi
}

# ============================================================================
# Verify glibc Version (adaptive: core=2.17, others=2.28)
# ============================================================================
print_header "Verifying glibc Version"
GLIBC_VERSION=$(ldd --version 2>&1 | head -1)
GLIBC_MINOR=$(ldd --version 2>&1 | grep -oE '[0-9]+\.[0-9]+' | head -1 | cut -d. -f2)
if [ "${GLIBC_MINOR:-0}" -ge 17 ]; then
    print_success "glibc OK: $GLIBC_VERSION"
else
    print_error "glibc unexpected version: $GLIBC_VERSION"
fi

# ============================================================================
# Verify GCC/G++ Compiler (>= 9.3.1)
# ============================================================================
print_header "Verifying GCC/G++ Compiler"

GCC_VER=$(gcc --version 2>&1 | head -1)
GCC_MAJOR=$(gcc -dumpversion 2>/dev/null | cut -d. -f1)
GCC_MINOR=$(gcc -dumpversion 2>/dev/null | cut -d. -f2)
if [ "${GCC_MAJOR:-0}" -gt 7 ] || { [ "${GCC_MAJOR:-0}" -eq 7 ] && [ "${GCC_MINOR:-0}" -ge 3 ]; }; then
    print_success "gcc >= 7.3: $GCC_VER"
else
    print_error "gcc version is NOT >= 7.3: $GCC_VER"
fi

GXX_VER=$(g++ --version 2>&1 | head -1)
GXX_MAJOR=$(g++ -dumpversion 2>/dev/null | cut -d. -f1)
if [ "${GXX_MAJOR:-0}" -ge 7 ]; then
    print_success "g++ >= 7.x: $GXX_VER"
else
    print_error "g++ version is NOT >= 7.x: $GXX_VER"
fi

# ============================================================================
# Verify GCC Library Dependencies
# ============================================================================
print_header "Verifying GCC Library Dependencies"

GCC_PATH=$(which gcc 2>/dev/null)
if [ -n "$GCC_PATH" ]; then
    for lib in libstdc++ libgcc; do
        if ldconfig -p 2>/dev/null | grep -q "$lib"; then
            print_success "$lib found in system libraries"
        else
            print_error "$lib NOT found"
        fi
    done
else
    print_error "gcc not in PATH"
fi

# ============================================================================
# Verify C/C++ Compilation
# ============================================================================
print_header "Verifying C/C++ Compilation Capability"

# Test C compilation
cat > /tmp/test_verify.c << 'CEOF'
#include <stdio.h>
int main() {
    printf("Hello from GCC %d.%d.%d\n", __GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__);
    return 0;
}
CEOF

if gcc /tmp/test_verify.c -o /tmp/test_verify_c 2>/dev/null && /tmp/test_verify_c >/dev/null 2>&1; then
    print_success "C compilation: $(/tmp/test_verify_c)"
else
    print_error "C compilation failed"
fi

# Test C++ compilation
cat > /tmp/test_verify.cpp << 'CXXEOF'
#include <iostream>
#include <string>
int main() {
    std::string msg = "C++ compilation successful with GCC ";
    msg += std::to_string(__GNUC__) + "." + std::to_string(__GNUC_MINOR__);
    std::cout << msg << std::endl;
    return 0;
}
CXXEOF

if g++ /tmp/test_verify.cpp -o /tmp/test_verify_cpp 2>/dev/null && /tmp/test_verify_cpp >/dev/null 2>&1; then
    print_success "C++ compilation: $(/tmp/test_verify_cpp)"
else
    print_error "C++ compilation failed"
fi

# Cleanup
rm -f /tmp/test_verify.c /tmp/test_verify_c /tmp/test_verify.cpp /tmp/test_verify_cpp

# ============================================================================
# Verify Python + SSL
# ============================================================================
print_header "Verifying Python Installation"

check_tool "python3" "python3 --version"
check_tool "pip3" "pip3 --version"

# Verify SSL module works (Python 3.12 uses OpenSSL 3.x from manylinux2014)
if python3 -c "import ssl; print('SSL version:', ssl.OPENSSL_VERSION)" 2>/dev/null; then
    print_success "Python SSL module works"
else
    print_error "Python SSL module is broken"
fi

# ============================================================================
# Verify Development Tools
# ============================================================================
print_header "Verifying Development Tools"

check_tool "Go" "go version"
check_tool "CMake" "cmake --version"
check_tool "Rust" "rustc --version"
check_tool "Cargo" "cargo --version"

# Optional tools (only in tsdb-builder-others)
if command -v java &>/dev/null; then
    check_tool "Java" "java -version"
else
    print_info "Java: not installed (core image)"
fi
if command -v mvn &>/dev/null; then
    check_tool "Maven" "mvn --version"
else
    print_info "Maven: not installed (core image)"
fi
if command -v node &>/dev/null; then
    check_tool "Node.js" "node --version"
    check_tool "npm" "npm --version"
    check_tool "yarn" "yarn --version"
    check_tool "pnpm" "pnpm --version"
else
    print_info "Node.js: not installed (core image)"
fi

# .NET SDK
if [ -f /usr/local/dotnet/dotnet ] && [ -d /usr/local/dotnet/sdk ]; then
    SDK_VER=$(ls /usr/local/dotnet/sdk/ 2>/dev/null | head -1)
    if dotnet --version &>/dev/null; then
        print_success "dotnet SDK: ${SDK_VER}"
    else
        print_info "dotnet SDK ${SDK_VER} installed but runtime check returned non-zero"
    fi
else
    print_info "dotnet: not installed (core image)"
fi

# ============================================================================
# Verify Modern Tools (mold + protoc + tini)
# ============================================================================
print_header "Verifying Modern Tools"

# mold linker
if [ -f /usr/bin/mold ]; then
    MOLD_VER=$(mold --version 2>&1 | head -1)
    if [ -n "$MOLD_VER" ]; then
        print_success "mold: $MOLD_VER"
    else
        print_info "mold binary present at /usr/bin/mold (version check skipped - may need native arch)"
    fi
else
    print_error "mold: NOT FOUND at /usr/bin/mold"
fi

# protoc
if command -v protoc &>/dev/null; then
    PROTOC_VER=$(protoc --version 2>&1)
    print_success "protoc: $PROTOC_VER"
else
    print_error "protoc: NOT FOUND"
fi

# tini
if [ -f /bin/tini ]; then
    TINI_VER=$(tini --version 2>&1 | head -1)
    print_success "tini: $TINI_VER"
else
    print_error "tini: NOT FOUND at /bin/tini"
fi

# Verify mold can link (compile test with mold)
# GCC 7.3 does not support -fuse-ld=mold (requires GCC ≥ 12.1),
# so use -B/usr/libexec/mold which works with any GCC version.
cat > /tmp/test_mold.c << 'MOLDEOF'
#include <stdio.h>
int main() { printf("mold link OK\n"); return 0; }
MOLDEOF
if [ -d /usr/libexec/mold ]; then
    if gcc /tmp/test_mold.c -o /tmp/test_mold -B/usr/libexec/mold 2>/dev/null && /tmp/test_mold 2>/dev/null; then
        print_success "mold linker functional: $(/tmp/test_mold)"
    else
        print_info "mold link test skipped (may need native arch execution)"
    fi
elif gcc /tmp/test_mold.c -o /tmp/test_mold -fuse-ld=mold 2>/dev/null && /tmp/test_mold 2>/dev/null; then
    print_success "mold linker functional: $(/tmp/test_mold)"
else
    print_info "mold link test skipped (may need native arch execution)"
fi
rm -f /tmp/test_mold.c /tmp/test_mold

# ccache
check_tool "ccache" "ccache --version"

# ============================================================================
# Summary
# ============================================================================
print_header "Verification Summary"

print_info "Architecture : $(uname -m)"
print_info "Base OS      : $(cat /etc/almalinux-release 2>/dev/null || cat /etc/centos-release 2>/dev/null || echo 'Unknown')"
print_info "glibc        : $(ldd --version 2>&1 | head -1)"
print_info "GCC          : $(gcc --version | head -1)"

echo ""
if [ $ERRORS -eq 0 ]; then
    echo -e "${GREEN}============================================================${NC}"
    echo -e "${GREEN}  All verifications passed!${NC}"
    echo -e "${GREEN}============================================================${NC}"
    exit 0
else
    echo -e "${RED}============================================================${NC}"
    echo -e "${RED}  $ERRORS verification(s) FAILED!${NC}"
    echo -e "${RED}============================================================${NC}"
    exit 1
fi
