#!/bin/bash
# ============================================================================
# Download packages for offline Dockerfile build
# Usage: ./scripts/download-packages.sh [--packages-dir DIR]
#
# Downloads all required packages to ~/packages (or specified directory)
# for offline Docker image builds.
# ============================================================================

set -e

PACKAGES_DIR="${HOME}/packages"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --packages-dir|-p)
            if [[ $# -lt 2 ]]; then echo "ERROR: --packages-dir requires an argument"; exit 1; fi
            PACKAGES_DIR="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [--packages-dir DIR]"
            echo ""
            echo "Options:"
            echo "  --packages-dir DIR    Download to specified directory (default: ~/packages)"
            echo "  --help                Show this help message"
            exit 0
            ;;
        *)
            echo "ERROR: Unknown argument '$1'"
            echo "Run '$0 --help' for usage"
            exit 1
            ;;
    esac
done

mkdir -p "${PACKAGES_DIR}"
cd "${PACKAGES_DIR}"

echo "============================================================"
echo "  Downloading packages to ${PACKAGES_DIR}"
echo "============================================================"
echo ""

# ============================================================================
# Go 1.23.4
# ============================================================================
GO_VERSION=1.23.4
echo "[1/20] Downloading go${GO_VERSION}.linux-amd64.tar.gz..."
if [[ -f go${GO_VERSION}.linux-amd64.tar.gz ]]; then
    echo "  ✓ Already exists, skipping"
else
    wget -c -q --show-progress https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz
    echo "  ✓ Downloaded ($(du -h go${GO_VERSION}.linux-amd64.tar.gz | cut -f1))"
fi

echo "[2/20] Downloading go${GO_VERSION}.linux-arm64.tar.gz..."
if [[ -f go${GO_VERSION}.linux-arm64.tar.gz ]]; then
    echo "  ✓ Already exists, skipping"
else
    wget -c -q --show-progress https://go.dev/dl/go${GO_VERSION}.linux-arm64.tar.gz
    echo "  ✓ Downloaded ($(du -h go${GO_VERSION}.linux-arm64.tar.gz | cut -f1))"
fi

# ============================================================================
# CMake 3.21.5
# ============================================================================
CMAKE_VERSION=3.21.5
echo "[3/20] Downloading cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz..."
if [[ -f cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz ]]; then
    echo "  ✓ Already exists, skipping"
else
    wget -c -q --show-progress https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz
    echo "  ✓ Downloaded ($(du -h cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz | cut -f1))"
fi

echo "[4/20] Downloading cmake-${CMAKE_VERSION}-linux-aarch64.tar.gz..."
if [[ -f cmake-${CMAKE_VERSION}-linux-aarch64.tar.gz ]]; then
    echo "  ✓ Already exists, skipping"
else
    wget -c -q --show-progress https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-aarch64.tar.gz
    echo "  ✓ Downloaded ($(du -h cmake-${CMAKE_VERSION}-linux-aarch64.tar.gz | cut -f1))"
fi

# ============================================================================
# mold 2.40.3
# ============================================================================
MOLD_VERSION=2.40.3
echo "[5/20] Downloading mold-${MOLD_VERSION}.tar.gz..."
if [[ -f mold-${MOLD_VERSION}.tar.gz ]]; then
    echo "  ✓ Already exists, skipping"
else
    wget -c -q --show-progress https://github.com/rui314/mold/archive/refs/tags/v${MOLD_VERSION}.tar.gz \
        -O mold-${MOLD_VERSION}.tar.gz
    echo "  ✓ Downloaded ($(du -h mold-${MOLD_VERSION}.tar.gz | cut -f1))"
fi

# ============================================================================
# GNU Make 4.2.1 (fixes parallel scheduling bug PR #12610)
# ============================================================================
echo "[6/20] Downloading make-4.2.1.tar.gz..."
if [[ -f make-4.2.1.tar.gz ]]; then
    echo "  ✓ Already exists, skipping"
else
    wget -c -q --show-progress https://ftp.gnu.org/gnu/make/make-4.2.1.tar.gz
    echo "  ✓ Downloaded ($(du -h make-4.2.1.tar.gz | cut -f1))"
fi

# ============================================================================
# ccache 4.10.2
# ============================================================================
echo "[7/20] Downloading ccache-4.10.2.tar.gz..."
if [[ -f ccache-4.10.2.tar.gz ]]; then
    echo "  ✓ Already exists, skipping"
else
    wget -c -q --show-progress https://github.com/ccache/ccache/releases/download/v4.10.2/ccache-4.10.2.tar.gz
    echo "  ✓ Downloaded ($(du -h ccache-4.10.2.tar.gz | cut -f1))"
fi

# ============================================================================
# sccache 0.15.0
# ============================================================================
echo "[8/20] Downloading sccache-v0.15.0-x86_64-unknown-linux-musl.tar.gz..."
if [[ -f sccache-v0.15.0-x86_64-unknown-linux-musl.tar.gz ]]; then
    echo "  ✓ Already exists, skipping"
else
    wget -c -q --show-progress https://github.com/mozilla/sccache/releases/download/v0.15.0/sccache-v0.15.0-x86_64-unknown-linux-musl.tar.gz
    echo "  ✓ Downloaded ($(du -h sccache-v0.15.0-x86_64-unknown-linux-musl.tar.gz | cut -f1))"
fi

echo "[9/20] Downloading sccache-v0.15.0-aarch64-unknown-linux-musl.tar.gz..."
if [[ -f sccache-v0.15.0-aarch64-unknown-linux-musl.tar.gz ]]; then
    echo "  ✓ Already exists, skipping"
else
    wget -c -q --show-progress https://github.com/mozilla/sccache/releases/download/v0.15.0/sccache-v0.15.0-aarch64-unknown-linux-musl.tar.gz
    echo "  ✓ Downloaded ($(du -h sccache-v0.15.0-aarch64-unknown-linux-musl.tar.gz | cut -f1))"
fi

# ============================================================================
# protoc 33.0
# ============================================================================
echo "[10/20] Downloading protoc-33.0-linux-x86_64.zip..."
if [[ -f protoc-33.0-linux-x86_64.zip ]]; then
    echo "  ✓ Already exists, skipping"
else
    wget -c -q --show-progress https://github.com/protocolbuffers/protobuf/releases/download/v33.0/protoc-33.0-linux-x86_64.zip
    echo "  ✓ Downloaded ($(du -h protoc-33.0-linux-x86_64.zip | cut -f1))"
fi

echo "[11/20] Downloading protoc-33.0-linux-aarch_64.zip..."
if [[ -f protoc-33.0-linux-aarch_64.zip ]]; then
    echo "  ✓ Already exists, skipping"
else
    wget -c -q --show-progress https://github.com/protocolbuffers/protobuf/releases/download/v33.0/protoc-33.0-linux-aarch_64.zip
    echo "  ✓ Downloaded ($(du -h protoc-33.0-linux-aarch_64.zip | cut -f1))"
fi

# ============================================================================
# tini v0.19.0
# ============================================================================
echo "[12/20] Downloading tini-amd64-v0.19.0..."
if [[ -f tini-amd64-v0.19.0 ]]; then
    echo "  ✓ Already exists, skipping"
else
    wget -c -q --show-progress -O tini-amd64-v0.19.0 https://github.com/krallin/tini/releases/download/v0.19.0/tini-amd64
    chmod +x tini-amd64-v0.19.0
    echo "  ✓ Downloaded ($(du -h tini-amd64-v0.19.0 | cut -f1))"
fi

echo "[13/20] Downloading tini-arm64-v0.19.0..."
if [[ -f tini-arm64-v0.19.0 ]]; then
    echo "  ✓ Already exists, skipping"
else
    wget -c -q --show-progress -O tini-arm64-v0.19.0 https://github.com/krallin/tini/releases/download/v0.19.0/tini-arm64
    chmod +x tini-arm64-v0.19.0
    echo "  ✓ Downloaded ($(du -h tini-arm64-v0.19.0 | cut -f1))"
fi

# ============================================================================
# bison 3.8.2
# ============================================================================
echo "[14/20] Downloading bison-3.8.2.tar.gz..."
if [[ -f bison-3.8.2.tar.gz ]]; then
    echo "  ✓ Already exists, skipping"
else
    wget -c -q --show-progress https://ftp.gnu.org/gnu/bison/bison-3.8.2.tar.gz
    echo "  ✓ Downloaded ($(du -h bison-3.8.2.tar.gz | cut -f1))"
fi

# ============================================================================
# Rust standalone installer (for offline Rust toolchain installation)
# ============================================================================
RUST_VERSION=1.90.0
echo "[15/20] Downloading rust-${RUST_VERSION}-x86_64-unknown-linux-gnu.tar.xz..."
if [[ -f rust-${RUST_VERSION}-x86_64-unknown-linux-gnu.tar.xz ]]; then
    echo "  ✓ Already exists, skipping"
else
    wget -c -q --show-progress https://static.rust-lang.org/dist/rust-${RUST_VERSION}-x86_64-unknown-linux-gnu.tar.xz
    echo "  ✓ Downloaded ($(du -h rust-${RUST_VERSION}-x86_64-unknown-linux-gnu.tar.xz | cut -f1))"
fi

echo "[16/20] Downloading rust-${RUST_VERSION}-aarch64-unknown-linux-gnu.tar.xz..."
if [[ -f rust-${RUST_VERSION}-aarch64-unknown-linux-gnu.tar.xz ]]; then
    echo "  ✓ Already exists, skipping"
else
    wget -c -q --show-progress https://static.rust-lang.org/dist/rust-${RUST_VERSION}-aarch64-unknown-linux-gnu.tar.xz
    echo "  ✓ Downloaded ($(du -h rust-${RUST_VERSION}-aarch64-unknown-linux-gnu.tar.xz | cut -f1))"
fi

# ============================================================================
# uv (Python package manager, prebuilt musl binary)
# ============================================================================
UV_VERSION=0.7.12
echo "[17/20] Downloading uv-${UV_VERSION}-x86_64-unknown-linux-musl.tar.gz..."
if [[ -f uv-${UV_VERSION}-x86_64-unknown-linux-musl.tar.gz ]]; then
    echo "  ✓ Already exists, skipping"
else
    wget -c -q --show-progress https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/uv-x86_64-unknown-linux-musl.tar.gz \
        -O uv-${UV_VERSION}-x86_64-unknown-linux-musl.tar.gz
    echo "  ✓ Downloaded ($(du -h uv-${UV_VERSION}-x86_64-unknown-linux-musl.tar.gz | cut -f1))"
fi

echo "[18/20] Downloading uv-${UV_VERSION}-aarch64-unknown-linux-musl.tar.gz..."
if [[ -f uv-${UV_VERSION}-aarch64-unknown-linux-musl.tar.gz ]]; then
    echo "  ✓ Already exists, skipping"
else
    wget -c -q --show-progress https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/uv-aarch64-unknown-linux-musl.tar.gz \
        -O uv-${UV_VERSION}-aarch64-unknown-linux-musl.tar.gz
    echo "  ✓ Downloaded ($(du -h uv-${UV_VERSION}-aarch64-unknown-linux-musl.tar.gz | cut -f1))"
fi

# ============================================================================
# ccache dependencies: zstd, hiredis, xxhash (source tarballs)
# ============================================================================
echo "[19/20] Downloading ccache build dependencies (zstd, hiredis, xxhash)..."

ZSTD_VERSION=1.5.6
if [[ -f zstd-${ZSTD_VERSION}.tar.gz ]]; then
    echo "  ✓ zstd-${ZSTD_VERSION}.tar.gz already exists, skipping"
else
    wget -c -q --show-progress https://github.com/facebook/zstd/releases/download/v${ZSTD_VERSION}/zstd-${ZSTD_VERSION}.tar.gz
    echo "  ✓ Downloaded zstd ($(du -h zstd-${ZSTD_VERSION}.tar.gz | cut -f1))"
fi

HIREDIS_VERSION=1.2.0
if [[ -f hiredis-${HIREDIS_VERSION}.tar.gz ]]; then
    echo "  ✓ hiredis-${HIREDIS_VERSION}.tar.gz already exists, skipping"
else
    wget -c -q --show-progress https://github.com/redis/hiredis/archive/refs/tags/v${HIREDIS_VERSION}.tar.gz \
        -O hiredis-${HIREDIS_VERSION}.tar.gz
    echo "  ✓ Downloaded hiredis ($(du -h hiredis-${HIREDIS_VERSION}.tar.gz | cut -f1))"
fi

XXHASH_VERSION=0.8.2
if [[ -f xxhash-${XXHASH_VERSION}.tar.gz ]]; then
    echo "  ✓ xxhash-${XXHASH_VERSION}.tar.gz already exists, skipping"
else
    wget -c -q --show-progress https://github.com/Cyan4973/xxhash/archive/refs/tags/v${XXHASH_VERSION}.tar.gz \
        -O xxhash-${XXHASH_VERSION}.tar.gz
    echo "  ✓ Downloaded xxhash ($(du -h xxhash-${XXHASH_VERSION}.tar.gz | cut -f1))"
fi

# ============================================================================
# pip wheels (offline Python packages for manylinux2014/manylinux_2_28 + cp312)
# Download separately per architecture to ensure complete coverage.
# manylinux2014 for core/dev images, manylinux_2_28 for others image.
# ============================================================================
echo "[20/20] Downloading pip wheels..."
mkdir -p pip
TAOSPY_VERSION=2.8.9
TAOS_WS_PY_VERSION=0.6.9
_pip_pkgs="taospy==${TAOSPY_VERSION} taos-ws-py==${TAOS_WS_PY_VERSION} conan"
# distro is a Linux-only conditional dep of conan, not resolved on macOS
_pip_extra_linux="distro"
# maturin is only needed by the others image
_pip_others="maturin"

for _plat in manylinux2014_x86_64 manylinux2014_aarch64 manylinux_2_28_x86_64 manylinux_2_28_aarch64; do
    echo "  Downloading wheels for ${_plat}..."
    pip3 download --no-cache-dir \
        --dest pip/ \
        --python-version 3.12 \
        --platform "${_plat}" \
        --platform "${_plat/2014/_2_17}" \
        --only-binary=:all: \
        ${_pip_pkgs} ${_pip_others} 2>&1 || true
done
# Fallback: download pure-python / sdist packages that have no platform-specific wheel
pip3 download --no-cache-dir \
    --dest pip/ \
    --no-deps \
    ${_pip_pkgs} ${_pip_extra_linux} ${_pip_others} 2>&1 || true
# Deduplicate: pip download may fetch the same noarch wheel twice
echo "  ✓ pip wheels downloaded ($(ls pip/ 2>/dev/null | wc -l) files)"

echo ""
echo "============================================================"
echo "  Download complete!"
echo "============================================================"
echo ""
echo "Downloaded packages:"
ls -lh "${PACKAGES_DIR}"/make-*.tar.gz \
       "${PACKAGES_DIR}"/ccache-*.tar.gz \
       "${PACKAGES_DIR}"/sccache-*.tar.gz \
       "${PACKAGES_DIR}"/protoc-*.zip \
       "${PACKAGES_DIR}"/tini-*-v0.19.0 \
       "${PACKAGES_DIR}"/bison-*.tar.gz \
       "${PACKAGES_DIR}"/rust-*.tar.xz \
       "${PACKAGES_DIR}"/uv-*.tar.gz \
       "${PACKAGES_DIR}"/zstd-*.tar.gz \
       "${PACKAGES_DIR}"/hiredis-*.tar.gz \
       "${PACKAGES_DIR}"/xxhash-*.tar.gz \
       2>/dev/null | awk '{print "  "$9" ("$5")"}'
echo "  pip/  ($(ls "${PACKAGES_DIR}"/pip/ 2>/dev/null | wc -l) wheel files)"

echo ""
echo "Total size:"
du -sh "${PACKAGES_DIR}" 2>/dev/null | awk '{print "  "$1}'

echo ""
echo "Next steps:"
echo "  1. Verify checksums (optional but recommended)"
echo "  2. Build Docker images with --packages ${PACKAGES_DIR}"
echo "  3. Example: ./build-dev-image.sh --arch amd64 --version 3.4.1 --packages ${PACKAGES_DIR}"
echo ""
