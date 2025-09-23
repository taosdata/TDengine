#!/bin/bash

set -e

taosd >>/dev/null 2>&1 &
taosadapter >>/dev/null 2>&1 &

sleep 5

cd ../../docs/examples/rust/nativeexample

cargo run --example bind_tags
cargo run --example bind
cargo run --example connect
cargo run --example createdb
cargo run --example insert
cargo run --example query_pool
cargo run --example query
cargo run --example schemaless_insert_json
cargo run --example schemaless_insert_line
cargo run --example schemaless_insert_telnet
cargo run --example schemaless
cargo run --example stmt_all
cargo run --example stmt_json_tag
cargo run --example stmt
cargo run --example subscribe_demo
cargo run --example subscribe
cargo run --example tmq

cd ../restexample

cargo run --example connect
cargo run --example createdb
cargo run --example insert
cargo run --example query
cargo run --example schemaless
cargo run --example stmt_all
cargo run --example stmt
cargo run --example subscribe_demo
cargo run --example tmq
