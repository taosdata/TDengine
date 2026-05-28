#!/bin/bash
# ============================================================================
# Build tsdb-builder-dev Docker image
# Usage: ./build-dev-image.sh [--arch amd64|arm64] --version X.Y.Z [--packages /path/to/packages] [--local]
#
# Builds from Dockerfile.dev (manylinux2014, glibc 2.17, GCC 11.2.1/x86_64, GCC 10.2/arm64)
# Produces: harbor.tdengine.net/tsdb-builder/dev:<version>-<arch>
# ============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_ARGS_FILE="${SCRIPT_DIR}/.build-args"
ARCH="amd64"
VERSION=""
PACKAGES_DIR="${HOME}/packages"
DOCKER_BIN="${DOCKER_BIN:-docker}"
REPOSITORY="harbor.tdengine.net/tsdb-builder/dev"
ALLOW_EMULATION=0
LOCAL_ONLY=0

usage() {
    echo "Usage: $0 [--arch amd64|arm64] --version 3.4.1 [--packages /path/to/packages] [--allow-emulation] [--no-cache] [--local]"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --arch|-a)
            if [[ $# -lt 2 ]]; then echo "ERROR: --arch requires an argument"; exit 1; fi
            ARCH="$2"; shift 2 ;;
        --version|-v)
            if [[ $# -lt 2 ]]; then echo "ERROR: --version requires an argument"; exit 1; fi
            VERSION="$2"; shift 2 ;;
        --packages|-p)
            if [[ $# -lt 2 ]]; then echo "ERROR: --packages requires an argument"; exit 1; fi
            PACKAGES_DIR="$2"; shift 2 ;;
        --allow-emulation)
            ALLOW_EMULATION=1; shift ;;
        --no-cache)
            NO_CACHE="--no-cache"; shift ;;
        --local)
            LOCAL_ONLY=1; shift ;;
        amd64|arm64)
            # backward-compat: accept bare arch as positional arg
            ARCH="$1"; shift ;;
        *)
            echo "ERROR: Unknown argument '$1'"
            usage
            exit 1 ;;
    esac
done

if [ "$ARCH" != "amd64" ] && [ "$ARCH" != "arm64" ]; then
    echo "ERROR: Invalid architecture '${ARCH}'. Use 'amd64' or 'arm64'."
    exit 1
fi

if [[ -z "${VERSION}" ]]; then
    echo "ERROR: --version is required."
    usage
    exit 1
fi

# Refuse cross-architecture builds by default — QEMU emulation makes the
# manylinux2014 + mold-from-source build path 10-20x slower (e.g. 8min → 2h).
# Build natively per arch and let create_manifest_if_ready stitch the multi-arch
# manifest, or pass --allow-emulation to override.
HOST_ARCH=$(uname -m)
case "${HOST_ARCH}" in
    x86_64)  HOST_DOCKER_ARCH="amd64" ;;
    aarch64) HOST_DOCKER_ARCH="arm64" ;;
    riscv64) HOST_DOCKER_ARCH="riscv64" ;;
    *)       HOST_DOCKER_ARCH="${HOST_ARCH}" ;;
esac

if [ "${HOST_DOCKER_ARCH}" != "${ARCH}" ]; then
    if [ "${ALLOW_EMULATION}" -eq 1 ]; then
        echo "[WARN] Cross-architecture build under QEMU emulation: host=${HOST_DOCKER_ARCH}, target=${ARCH}"
        echo "[WARN] Expect a 10-20x slowdown (mold source compile + yum/pip/rustup all emulated)."
    else
        echo "ERROR: Cross-architecture build refused: host=${HOST_DOCKER_ARCH}, target=${ARCH}"
        echo "       Building ${ARCH} on a ${HOST_DOCKER_ARCH} host runs the entire image"
        echo "       under QEMU emulation and typically takes 10-20x longer (e.g. 2 hours"
        echo "       instead of 8 minutes) due to the from-source mold build."
        echo ""
        echo "       Recommended: build each arch natively on a matching host, then let"
        echo "       this script update the multi-arch manifest automatically."
        echo ""
        echo "       To proceed anyway, re-run with --allow-emulation."
        exit 1
    fi
fi

VERSION_TAG="${REPOSITORY}:${VERSION}-${ARCH}"
if [ "${LOCAL_ONLY}" -eq 1 ]; then
    LATEST_ARCH_TAG=""
else
    LATEST_ARCH_TAG="${REPOSITORY}:latest-${ARCH}"
fi

echo "[INFO] Building ${VERSION_TAG}..."
echo "[INFO] Base    : manylinux2014 (glibc 2.17, CentOS 7, GCC 11.2.1/x86_64, GCC 10.2/arm64)"
echo "[INFO] Packages: ${PACKAGES_DIR}"
echo "[INFO] Components: ENGINE, ENTERPRISE, ADAPTER, KEEPER, TOOLS, GEN, TAOSX (without EXPLORER_UI)"

# Read .build-args
build_args=""
if [ -f "$BUILD_ARGS_FILE" ]; then
    while IFS= read -r line; do
        build_args="$build_args --build-arg $line"
    done < <(grep -v '^#' "$BUILD_ARGS_FILE" | grep -v '^[[:space:]]*$')
else
    echo "[WARN] .build-args not found, using Dockerfile defaults."
fi

# shellcheck disable=SC2086
DOCKER_BUILDKIT=1 "${DOCKER_BIN}" buildx build \
    --platform "linux/${ARCH}" \
    $build_args \
    ${NO_CACHE:-} \
    --build-context packages="${PACKAGES_DIR}" \
    --tag "${VERSION_TAG}" \
    ${LATEST_ARCH_TAG:+--tag "${LATEST_ARCH_TAG}"} \
    --load \
    -f "${SCRIPT_DIR}/Dockerfile.dev" \
    "$SCRIPT_DIR"

push_or_die() {
    local image_ref="$1"
    if ! "${DOCKER_BIN}" push "${image_ref}"; then
        echo "ERROR: Failed to push ${image_ref}"
        echo "Run: docker login harbor.tdengine.net"
        exit 1
    fi
}

other_arch() {
    case "$1" in
        amd64) echo "arm64" ;;
        arm64) echo "amd64" ;;
    esac
}

warn_manifest_failure() {
    echo "[WARN] $1"
}

update_manifest() {
    local manifest_ref="$1"
    shift

    if ! "${DOCKER_BIN}" buildx imagetools create --tag "${manifest_ref}" "$@"; then
        warn_manifest_failure "Failed to create manifest ${manifest_ref}"
    fi
}

create_manifest_if_ready() {
    local sibling_arch sibling_tag version_manifest latest_manifest
    sibling_arch="$(other_arch "${ARCH}")"
    sibling_tag="${REPOSITORY}:${VERSION}-${sibling_arch}"
    version_manifest="${REPOSITORY}:${VERSION}"
    latest_manifest="${REPOSITORY}:latest"

    if ! "${DOCKER_BIN}" manifest inspect "${sibling_tag}" >/dev/null 2>&1; then
        echo "[INFO] Sibling image not found yet: ${sibling_tag}"
        echo "[INFO] Skipping manifest update for ${version_manifest} and ${latest_manifest}"
        return 0
    fi

    update_manifest "${version_manifest}" \
        "${REPOSITORY}:${VERSION}-amd64" \
        "${REPOSITORY}:${VERSION}-arm64"

    update_manifest "${latest_manifest}" \
        "${REPOSITORY}:latest-amd64" \
        "${REPOSITORY}:latest-arm64"
}

if [ "${LOCAL_ONLY}" -eq 1 ]; then
    echo "[INFO] Local-only build completed (no push)."
    echo "[INFO] Image: ${VERSION_TAG}"
    exit 0
fi

push_or_die "${VERSION_TAG}"
push_or_die "${LATEST_ARCH_TAG}"
create_manifest_if_ready

echo "[INFO] Build completed successfully!"
echo "[INFO] Image: ${VERSION_TAG}"
