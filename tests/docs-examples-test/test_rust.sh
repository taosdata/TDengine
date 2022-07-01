#!/bin/bash

set -e

pgrep taosd || taosd >> /dev/null 2>&1 &
pgrep taosadapter || taosadapter >> /dev/null 2>&1 &

cd ../../docs/examples/rust

cargo  run -p nativeexample --example connect
cargo  run -p restexample --example connect

taos -s "drop database if exists power"
cargo  run -p restexample --example insert_example

cargo  run -p restexample --example query_example


taos -s "drop database if exists power"
cargo  run -p nativeexample --example stmt_example

taos -s "drop database if exists test"
cargo  run -p schemalessexample --example influxdb_line_example

taos -s "drop database if exists test"
cargo  run -p schemalessexample --example opentsdb_telnet_example

taos -s "drop database if exists test"
cargo  run -p schemalessexample --example opentsdb_json_example