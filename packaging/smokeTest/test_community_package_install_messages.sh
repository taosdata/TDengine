#!/bin/bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
PACKAGING_DIR="${ROOT_DIR}/source/taos-community/packaging"
RELEASE_DIR="${ROOT_DIR}/source/taos-community/release"
IMAGE="harbor.tdengine.net/tsdb-builder/core:latest-arm64"
VERSION="${VERSION:-3.3.6.0}"
COMPILE_DIR="${COMPILE_DIR:-/work/debug}"
SERVER_PKG="${RELEASE_DIR}/TDengine-server-${VERSION}-Linux-arm64.tar.gz"

RED='\033[0;31m'
GREEN='\033[1;32m'
NC='\033[0m'

fail() {
  echo -e "${RED}FAIL${NC}: $1"
  exit 1
}

pass() {
  echo -e "${GREEN}PASS${NC}: $1"
}

echo "=== verify community package install messages ==="

docker run --rm \
  -v "${ROOT_DIR}:/work" \
  -w /work/source/taos-community/packaging \
  "${IMAGE}" \
  bash -lc "./pack_community_tar.sh -c ${COMPILE_DIR} -n ${VERSION}" >/dev/null

if [ ! -f "${SERVER_PKG}" ]; then
  fail "expected package not found: ${SERVER_PKG}"
fi

install_output="$(
  docker run --rm \
    -v "${RELEASE_DIR}:/release" \
    "${IMAGE}" \
    bash -lc "set -e; rm -rf /tmp/pkg && mkdir -p /tmp/pkg && cd /tmp/pkg && tar -xzf /release/TDengine-server-${VERSION}-Linux-arm64.tar.gz && cd TDengine-server-${VERSION} && bash ./install.sh -s" 2>&1
)"

echo "${install_output}"

if grep -q 'taoskeeper' <<<"${install_output}"; then
  fail "install output still mentions taoskeeper"
fi

if grep -q 'taos-explorer' <<<"${install_output}"; then
  fail "install output still mentions taos-explorer"
fi

if ! grep -q 'To start TDengine TSDB server' <<<"${install_output}"; then
  fail "install output did not include server start guidance"
fi

pass "install output excludes taoskeeper and taos-explorer"
