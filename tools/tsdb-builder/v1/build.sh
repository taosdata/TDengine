#!/bin/bash
# ============================================================================
# Docker Build Script for TSDB Docker Builder
# ============================================================================
# This script provides convenient commands to build Docker images with
# different configurations and architectures.
#
# Base images:
#   amd64: manylinux2014_x86_64 (glibc 2.17, GCC 10.x / CentOS 7)
#   arm64: manylinux_2_28_aarch64 (glibc 2.28, GCC 12 / AlmaLinux 8)
#          Note: Node 22 arm64 requires glibc >= 2.25; manylinux_2_28 satisfies this
#
# Usage:
#   ./build.sh [command] [options]
#
# Commands:
#   build-arm64       Build for ARM64 architecture
#   build-amd64       Build for AMD64 architecture
#   build-all         Build both architectures sequentially
#   build-push        Build amd64+arm64 in parallel, push multi-arch manifest to registry
#   build-custom      Build with custom arguments
#   list-args         List all available build arguments
#   help              Show this help message
# ============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKERFILE="${SCRIPT_DIR}/Dockerfile"
BUILD_ARGS_FILE="${SCRIPT_DIR}/.build-args"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# ============================================================================
# Helper Functions
# ============================================================================

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Read .build-args and emit --build-arg KEY=VALUE flags (one per line).
# Lines starting with '#' and blank lines are skipped.
parse_build_args() {
    if [ ! -f "$BUILD_ARGS_FILE" ]; then
        print_warn ".build-args not found, using Dockerfile defaults."
        return
    fi
    grep -v '^#' "$BUILD_ARGS_FILE" | grep -v '^[[:space:]]*$' | while IFS= read -r line; do
        echo "--build-arg $line"
    done
}

# Ensure the current buildx builder supports linux/amd64 and linux/arm64.
# The default "docker" driver only supports the host's native architecture.
# If neither amd64 nor arm64 is supported, create a container-driver builder.
ensure_builder() {
    local platforms
    platforms=$(docker buildx inspect --bootstrap 2>/dev/null | grep -i "^Platforms:" | head -1)
    if echo "$platforms" | grep -q "linux/amd64" && echo "$platforms" | grep -q "linux/arm64"; then
        return 0
    fi
    print_info "Current buildx builder does not support both linux/amd64 and linux/arm64."
    if docker buildx inspect tsdb-multiarch &>/dev/null; then
        print_info "Switching to existing multi-platform builder: tsdb-multiarch"
        docker buildx use tsdb-multiarch
    else
        print_info "Creating multi-platform builder: tsdb-multiarch"
        docker buildx create --use --name tsdb-multiarch --driver docker-container --bootstrap
    fi
    print_info "Builder tsdb-multiarch is ready."
}

# ============================================================================
# Build Commands
# ============================================================================

build_arm64() {
    print_info "Building Docker image for ARM64 architecture..."
    print_info "Base: manylinux_2_28_aarch64 (glibc 2.28, GCC 12 / AlmaLinux 8)"

    # Read common args from .build-args; xargs safely handles multi-word splitting
    local build_args
    build_args=$(parse_build_args)

    # shellcheck disable=SC2086
    DOCKER_BUILDKIT=1 docker buildx build \
        --platform linux/arm64 \
        $build_args \
        --tag tsdb-builder:arm64 \
        --tag tsdb-builder:latest \
        --load \
        -f "$DOCKERFILE" \
        "$SCRIPT_DIR"

    print_info "Build completed successfully!"
    print_info "Image: tsdb-builder:arm64"
}

build_amd64() {
    print_info "Building Docker image for AMD64 architecture..."
    print_info "Base: manylinux2014_x86_64 (glibc 2.17, GCC 10.x)"

    local build_args
    build_args=$(parse_build_args)

    # shellcheck disable=SC2086
    DOCKER_BUILDKIT=1 docker buildx build \
        --platform linux/amd64 \
        $build_args \
        --tag tsdb-builder:amd64 \
        --load \
        -f "$DOCKERFILE" \
        "$SCRIPT_DIR"

    print_info "Build completed successfully!"
    print_info "Image: tsdb-builder:amd64"
}

build_all() {
    print_info "Building Docker images for both architectures..."
    build_arm64
    build_amd64
    print_info "All builds completed!"
}

build_push() {
    if [ -z "${REGISTRY_IMAGE:-}" ]; then
        print_error "REGISTRY_IMAGE is not set."
        print_error "Usage: REGISTRY_IMAGE=myregistry.io/tsdb-builder:latest ./build.sh build-push"
        exit 1
    fi

    print_info "Building multi-arch image (linux/amd64 + linux/arm64) and pushing..."
    print_info "Target: ${REGISTRY_IMAGE}"
    print_info "JDK version selection is automatic per architecture (handled in Dockerfile)."

    # Save active builder so we can restore it after the build
    local original_builder
    original_builder=$(docker buildx inspect 2>/dev/null | awk '/^Name:/{print $2; exit}')

    ensure_builder

    local build_args
    build_args=$(parse_build_args)

    # shellcheck disable=SC2086
    DOCKER_BUILDKIT=1 docker buildx build \
        --platform linux/amd64,linux/arm64 \
        $build_args \
        --tag "${REGISTRY_IMAGE}" \
        --push \
        -f "$DOCKERFILE" \
        "$SCRIPT_DIR"

    print_info "Build and push completed successfully!"
    print_info "Multi-arch manifest: ${REGISTRY_IMAGE}"
    print_info "  - linux/amd64 (JDK 8u144)"
    print_info "  - linux/arm64 (JDK 8u441)"

    # Restore original builder if ensure_builder switched away from it
    if [ -n "${original_builder}" ] && [ "${original_builder}" != "tsdb-multiarch" ]; then
        docker buildx use "${original_builder}" 2>/dev/null || true
    fi
}

build_custom() {
    print_info "Building Docker image with custom arguments..."

    local platform="${PLATFORM:-linux/arm64}"
    local tag="${TAG:-tsdb-builder:custom}"
    shift # Remove 'build-custom' from arguments

    local build_args
    build_args=$(parse_build_args)

    local extra_args=""
    while [ $# -gt 0 ]; do
        extra_args="$extra_args --build-arg $1"
        shift
    done

    # shellcheck disable=SC2086
    DOCKER_BUILDKIT=1 docker buildx build \
        --platform "$platform" \
        $build_args \
        $extra_args \
        --tag "$tag" \
        --load \
        -f "$DOCKERFILE" \
        "$SCRIPT_DIR"

    print_info "Build completed successfully!"
    print_info "Image: $tag"
}

list_args() {
    print_info "Build arguments loaded from .build-args (with current values):"
    echo ""
    if [ -f "$BUILD_ARGS_FILE" ]; then
        grep -v '^#' "$BUILD_ARGS_FILE" | grep -v '^[[:space:]]*$' | while IFS= read -r line; do
            echo "    $line"
        done
    else
        print_warn ".build-args not found."
    fi
    echo ""
    print_info "Architecture-specific values (auto-selected in Dockerfile builder stage):"
    echo "    AMD64 JDK: JDK_VERSION_AMD64 (default: 8u144, file: jdk-8u144-linux-x64.tar.gz)"
    echo "    ARM64 JDK: JDK_VERSION_ARM64 (default: 8u441, file: jdk-8u441-linux-aarch64.tar.gz)"
    echo ""
    print_info "Custom build environment variables:"
    echo "    PLATFORM=linux/arm64   (for build-custom; default: linux/arm64)"
    echo "    TAG=tsdb-builder:custom  (for build-custom)"
    echo ""
}

show_help() {
    cat <<EOF
Docker Build Script for TSDB Docker Builder

Base images:
  amd64: manylinux2014_x86_64 (glibc 2.17, GCC 10.x / CentOS 7)
  arm64: manylinux_2_28_aarch64 (glibc 2.28, GCC 12 / AlmaLinux 8)
         Node 22 arm64 requires glibc >= 2.25 — manylinux_2_28 satisfies this
Tools:      mold, protoc, tini, Go, JDK 8, Maven, CMake, Rust, Python 3.12, .NET 6

Usage: $0 [command] [options]

Commands:
  build-arm64       Build for ARM64 architecture (default when no command given)
  build-amd64       Build for AMD64 architecture (JDK version auto-selected)
  build-all         Build both architectures sequentially (local load)
  build-push        Build amd64+arm64 in parallel, push multi-arch manifest to registry
                      Requires: REGISTRY_IMAGE=myregistry.io/tsdb-builder:latest
  build-custom      Build with custom --build-arg overrides
                      Env vars: PLATFORM=linux/arm64, TAG=tsdb-builder:custom
                      Usage:    $0 build-custom KEY1=VALUE1 KEY2=VALUE2 ...
  list-args         List all build arguments from .build-args with current values
  help              Show this help message

Examples:
  # Build for ARM64 (default)
  $0 build-arm64

  # Build for AMD64
  $0 build-amd64

  # Build both architectures
  $0 build-all

  # Override a tool version
  $0 build-custom GO_VERSION=1.24.0

  # Build AMD64 with a custom tag
  PLATFORM=linux/amd64 TAG=tsdb-builder:amd64-dev $0 build-custom

  # Build both architectures in parallel and push to registry
  REGISTRY_IMAGE=myregistry.io/tsdb-builder:v1.0 $0 build-push

  # Verify a built image
  ./verify-image.sh arm64
  ./verify-image.sh amd64

Prerequisites:
  - Docker >= 20.10 with buildx support (included in Docker Desktop)
  - installers/ directory populated from NAS: /public/tsdb-builder
      amd64 JDK: installers/jdk-8u144-linux-x64.tar.gz
      arm64 JDK: installers/jdk-8u441-linux-aarch64.tar.gz

Mirrors (configured in .build-args):
  - Go proxy:  goproxy.cn
  - PyPI:      mirrors.aliyun.com
  - Rust:      rsproxy.cn

EOF
}

# ============================================================================
# Main
# ============================================================================

main() {
    if [ $# -eq 0 ]; then
        build_arm64
        return $?
    fi

    case "$1" in
        build-arm64)
            build_arm64
            ;;
        build-amd64)
            build_amd64
            ;;
        build-all)
            build_all
            ;;
        build-push)
            build_push
            ;;
        build-custom)
            build_custom "$@"
            ;;
        list-args)
            list_args
            ;;
        help|-h|--help)
            show_help
            ;;
        *)
            print_error "Unknown command: $1"
            echo ""
            show_help
            exit 1
            ;;
    esac
}

main "$@"
