#!/bin/bash

# Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

set -e

_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_TEST_CI="$(cd "${_SCRIPT_DIR}/../.." && pwd)/ci"
if [ -f "${_TEST_CI}/setup_internal_mirrors.sh" ]; then
  # shellcheck source=../../ci/setup_internal_mirrors.sh
  source "${_TEST_CI}/setup_internal_mirrors.sh"
else
  _REPO_ROOT="$(cd "${_SCRIPT_DIR}/../../../../../" && pwd)"
  # shellcheck source=../../../../../tools/cicd/tsdb-test-pipeline/ci/setup_internal_mirrors.sh
  source "${_REPO_ROOT}/tools/cicd/tsdb-test-pipeline/ci/setup_internal_mirrors.sh"
fi
setup_internal_mirrors

_COMMUNITY_DIR="$(cd "${_SCRIPT_DIR}/../../.." && pwd)"
_RUST_EXAMPLES_DIR="${_COMMUNITY_DIR}/docs/examples/rust"

cleanup_rust_targets() {
    rm -rf "${_RUST_EXAMPLES_DIR}/nativeexample/target" \
        "${_RUST_EXAMPLES_DIR}/restexample/target"
}
trap cleanup_rust_targets EXIT

cargo_run_retry() {
    local max=3 attempt
    for attempt in $(seq 1 "${max}"); do
        if cargo "$@"; then
            return 0
        fi
        if [ "${attempt}" -lt "${max}" ]; then
            echo "[rust.sh] cargo $* failed (attempt ${attempt}/${max}), retry in 15s ..."
            sleep 15
        fi
    done
    return 1
}

taosd >>/dev/null 2>&1 &
taosadapter >>/dev/null 2>&1 &

sleep 5

cd "${_RUST_EXAMPLES_DIR}/nativeexample"

cargo_run_retry run --example bind_tags
cargo_run_retry run --example bind
cargo_run_retry run --example connect
cargo_run_retry run --example createdb
cargo_run_retry run --example insert
cargo_run_retry run --example query_pool
cargo_run_retry run --example query
cargo_run_retry run --example schemaless_insert_json
cargo_run_retry run --example schemaless_insert_line
cargo_run_retry run --example schemaless_insert_telnet
cargo_run_retry run --example schemaless
cargo_run_retry run --example stmt_all
cargo_run_retry run --example stmt_json_tag
cargo_run_retry run --example stmt
cargo_run_retry run --example subscribe_demo
cargo_run_retry run --example subscribe
cargo_run_retry run --example tmq

cd ../restexample

cargo_run_retry run --example connect
cargo_run_retry run --example createdb
cargo_run_retry run --example insert
cargo_run_retry run --example query
cargo_run_retry run --example schemaless
cargo_run_retry run --example stmt_all
cargo_run_retry run --example stmt
cargo_run_retry run --example subscribe_demo
cargo_run_retry run --example tmq
