#!/bin/bash

# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Build libtaosws.so into a CMake debug tree.
# taos-ws-sys uses #![feature(c_variadic)] and must be built with nightly.
#
# Usage: build_libtaosws.sh <repo_root> <debug_dir>
# repo_root is /home/TDinternal or /home/TDengine inside the coverage image.

set -euo pipefail

REPO_ROOT="${1:?repo root}"
DEBUG_DIR="${2:?debug dir}"

if [ -f "${REPO_ROOT}/community/tools/taosws-rs/Cargo.toml" ]; then
    WS_DIR="${REPO_ROOT}/community/tools/taosws-rs"
elif [ -f "${REPO_ROOT}/tools/taosws-rs/Cargo.toml" ]; then
    WS_DIR="${REPO_ROOT}/tools/taosws-rs"
else
    if [ -d "${REPO_ROOT}/community" ]; then
        WS_DIR="${REPO_ROOT}/community/tools/taosws-rs"
    else
        WS_DIR="${REPO_ROOT}/tools/taosws-rs"
    fi
    echo "cloning taos-connector-rust into ${WS_DIR}"
    git clone --depth 1 https://github.com/taosdata/taos-connector-rust.git "${WS_DIR}"
fi

if ! rustup toolchain list | grep -q '^nightly'; then
    rustup toolchain install nightly --profile minimal
fi

# Current nightly denies a bare `...` in extern "C" (rust-lang/rust#145544).
# The connector still writes `fn taos_options(..., ...)`. Name the argument so
# the build does not depend on an older compiler.
sed -i -E 's/, \.\.\.\)/, _: ...)/g' "${WS_DIR}/taos-ws-sys/src/ws/mod.rs"

echo "building libtaosws.so from ${WS_DIR}"
(
    cd "${WS_DIR}"
    cargo build --release --locked -p taos-ws-sys
)

install -d "${DEBUG_DIR}/build/lib" "${DEBUG_DIR}/build/include"
cp -a "${WS_DIR}/target/release/libtaosws.so" "${DEBUG_DIR}/build/lib/libtaosws.so"
# cbindgen writes the header next to the build-script output, one level
# under target/release (target/release/build/taosws.h).
for header in \
    "${WS_DIR}/target/release/build/taosws.h" \
    "${WS_DIR}/target/release/taosws.h"
do
    if [ -f "${header}" ]; then
        cp -a "${header}" "${DEBUG_DIR}/build/include/taosws.h"
        break
    fi
done
echo "installed ${DEBUG_DIR}/build/lib/libtaosws.so"
