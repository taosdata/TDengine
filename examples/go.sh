#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

check_transactions() {
    local output
    local i
    for i in {1..30}; do
        output="$(taos -s "show transactions;" 2>/dev/null || true)"
        if [[ "${output}" == *"Query OK, 0 row(s)"* ]]; then
            echo "Success: No transactions are in progress."
            return 0
        fi
        sleep 1
    done

    echo "Error: Transactions are still in progress after 30 attempts."
    return 1
}

reset_cache() {
    local response
    response="$(curl --location -uroot:taosdata 'http://127.0.0.1:6041/rest/sql' --data 'reset query cache' || true)"
    if [[ "${response}" == \{\"code\":0* ]]; then
        echo "Success: Query cache reset successfully."
        return 0
    fi
    echo "Error: Failed to reset query cache. Response: ${response}"
    return 1
}

cleanup_resources() {
    local topics=(
        "topic_meters"
        "example_tmq_topic"
        "topic_failover_meters"
    )
    local dbs=(
        "power"
        "test"
        "example_tmq"
        "example_all_type_query"
        "example_stmt2"
        "restful_demo"
        "example_ws_stmt"
        "example_failover_query"
        "example_failover_schemaless"
        "example_failover_stmt"
        "example_failover_tmq"
    )
    local topic
    local db

    for topic in "${topics[@]}"; do
        taos -s "drop topic if exists ${topic}" >/dev/null 2>&1 || true
    done
    for db in "${dbs[@]}"; do
        taos -s "drop database if exists ${db}" >/dev/null 2>&1 || true
    done

    check_transactions
    reset_cache
}

run_example() {
    local entry="$1"
    local delay="${2:-0}"

    echo "==> go run ${entry}"
    cleanup_resources
    if [[ "${delay}" != "0" ]]; then
        sleep "${delay}"
    fi
    go run "${entry}"
}

run_docs_examples() {
    echo "==> running docs examples"
    run_example "./docs/connect/afconn/main.go"
    run_example "./docs/connect/cgoexample/main.go"
    run_example "./docs/connect/restexample/main.go"
    run_example "./docs/connect/connpool/main.go"
    run_example "./docs/connect/wsexample/main.go"

    run_example "./docs/sqlquery/main.go"
    run_example "./docs/queryreqid/main.go"
    run_example "./docs/stmt/native/main.go"
    run_example "./docs/stmt/ws/main.go"
    run_example "./docs/stmt2/native/main.go"
    run_example "./docs/schemaless/native/main.go" "3"
    run_example "./docs/schemaless/ws/main.go"
    run_example "./docs/tmq/native/main.go"
    run_example "./docs/tmq/ws/main.go"
    run_example "./docs/insert/json/main.go"
    run_example "./docs/insert/line/main.go"
    run_example "./docs/insert/sql/main.go"
    run_example "./docs/insert/stmt/main.go"
    run_example "./docs/insert/telnet/main.go"
    run_example "./docs/sub/main.go"
}

run_other_examples() {
    echo "==> running non-doc examples once"
    run_example "./all_type_query/native/main.go"
    run_example "./all_type_query/rest/main.go"
    run_example "./all_type_query/ws/main.go"
    run_example "./all_type_stmt/native/main.go"
    run_example "./all_type_stmt/ws/main.go"
    run_example "./schemaless/native/main.go"
    run_example "./schemaless/ws/main.go"
    run_example "./slog/main.go"
    run_example "./tmq/native/main.go"
    run_example "./tmq/ws/main.go"
    run_example "./failover/query/main.go"
    run_example "./failover/schemaless/main.go"
    run_example "./failover/stmt/main.go"
    run_example "./failover/tmq/main.go"
}

cleanup_on_exit() {
    set +e
    cleanup_resources >/dev/null 2>&1 || true
}

trap cleanup_on_exit EXIT

run_docs_examples
run_other_examples

echo "All examples completed."
