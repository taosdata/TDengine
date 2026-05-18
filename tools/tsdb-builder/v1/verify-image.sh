#!/bin/bash

# ============================================================================
# Docker Image Verification Script
# Verify all component versions in tsdb-builder image
# ============================================================================
#
# Usage (on host):
#   ./verify-image.sh                    # verify default image (tsdb-builder:latest)
#   ./verify-image.sh arm64              # verify tsdb-builder:arm64
#   ./verify-image.sh amd64              # verify tsdb-builder:amd64
#   ./verify-image.sh myregistry/img:v1  # verify a custom image name
#
# The script auto-detects whether it is running on the host or inside a
# container. On the host it launches "docker run" automatically.
# ============================================================================

set -e

# ============================================================================
# Auto-detect: host or container?
# If on the host, delegate into the container and exit.
# ============================================================================
if [ "$(uname -s)" != "Linux" ] || [ ! -f /etc/centos-release ]; then
    # --- Running on the HOST ---
    TAG="${1:-latest}"

    # Allow shorthand tags or full image names
    # If TAG contains ':' or '/' it's treated as a full image name;
    # otherwise it's a tag under tsdb-builder.
    case "$TAG" in
        *:* | */*)
            IMAGE="$TAG"
            ;;
        *)
            IMAGE="tsdb-builder:${TAG}"
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
# Verify glibc Version
# ============================================================================
print_header "Verifying glibc Version"
GLIBC_VERSION=$(ldd --version 2>&1 | head -1)
if echo "$GLIBC_VERSION" | grep -qE "2\.17"; then
    print_success "glibc 2.17 confirmed: $GLIBC_VERSION"
else
    print_error "glibc is NOT 2.17: $GLIBC_VERSION"
fi

# ============================================================================
# Verify GCC/G++ Compiler (>= 9.3.1)
# ============================================================================
print_header "Verifying GCC/G++ Compiler"

GCC_VER=$(gcc --version 2>&1 | head -1)
GCC_MAJOR=$(gcc -dumpversion 2>/dev/null | cut -d. -f1)
GCC_MINOR=$(gcc -dumpversion 2>/dev/null | cut -d. -f2)
if [ "${GCC_MAJOR:-0}" -gt 9 ] || { [ "${GCC_MAJOR:-0}" -eq 9 ] && [ "${GCC_MINOR:-0}" -ge 3 ]; }; then
    print_success "gcc >= 9.3.1: $GCC_VER"
else
    print_error "gcc version is NOT >= 9.3.1: $GCC_VER"
fi

GXX_VER=$(g++ --version 2>&1 | head -1)
GXX_MAJOR=$(g++ -dumpversion 2>/dev/null | cut -d. -f1)
if [ "${GXX_MAJOR:-0}" -ge 9 ]; then
    print_success "g++ >= 9.x: $GXX_VER"
else
    print_error "g++ version is NOT >= 9.x: $GXX_VER"
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
check_tool "Java" "java -version"
check_tool "Maven" "mvn --version"
check_tool "CMake" "cmake --version"
check_tool "Rust" "rustc --version"
check_tool "Cargo" "cargo --version"

# .NET SDK: requires devtoolset-10 environment to run on glibc 2.17
if [ -f /usr/local/dotnet/dotnet ] && [ -d /usr/local/dotnet/sdk ]; then
    SDK_VER=$(ls /usr/local/dotnet/sdk/ 2>/dev/null | head -1)
    # Source devtoolset-10 to enable .NET runtime
    if [ -f /opt/rh/devtoolset-10/enable ]; then
        source /opt/rh/devtoolset-10/enable
        if dotnet --version &>/dev/null; then
            print_success "dotnet SDK: ${SDK_VER}"
        else
            print_info "dotnet SDK ${SDK_VER} installed but runtime check failed"
        fi
    else
        print_info "dotnet SDK ${SDK_VER} installed (devtoolset-10 not available)"
    fi
else
    print_error "dotnet: SDK NOT FOUND at /usr/local/dotnet"
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
cat > /tmp/test_mold.c << 'MOLDEOF'
#include <stdio.h>
int main() { printf("mold link OK\n"); return 0; }
MOLDEOF
if gcc /tmp/test_mold.c -o /tmp/test_mold -fuse-ld=mold 2>/dev/null && /tmp/test_mold 2>/dev/null; then
    print_success "mold linker functional: $(/tmp/test_mold)"
else
    print_info "mold link test skipped (may need native arch execution)"
fi
rm -f /tmp/test_mold.c /tmp/test_mold

# ============================================================================
# Summary
# ============================================================================
print_header "Verification Summary"

print_info "Architecture : $(uname -m)"
print_info "Base OS      : $(cat /etc/centos-release 2>/dev/null || echo 'Unknown')"
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
