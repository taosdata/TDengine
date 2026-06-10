#!/bin/bash

set -e

# Use internal Cargo mirror in CI (avoid mirrors.tuna.tsinghua.edu.cn timeouts).
setup_cargo_mirror() {
    export CARGO_HOME="${CARGO_HOME:-${HOME}/.cargo}"
    mkdir -p "${CARGO_HOME}"
    local _cfg="${CARGO_HOME}/config.toml"
    if [ ! -f "${_cfg}" ] || ! grep -q 'nora.tdengine.net' "${_cfg}" 2>/dev/null; then
        cat > "${_cfg}" <<'EOF'
[source.crates-io]
replace-with = 'internal'

[source.internal]
registry = "sparse+https://nora.tdengine.net/cargo/index/"

[registries.internal]
index = "sparse+https://nora.tdengine.net/cargo/index/"

[http]
multiplexing = false
timeout = 120

[net]
git-fetch-with-cli = true
EOF
    fi
}

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

setup_cargo_mirror

taosd >>/dev/null 2>&1 &
taosadapter >>/dev/null 2>&1 &

sleep 5

cd ../../docs/examples/rust/nativeexample

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
