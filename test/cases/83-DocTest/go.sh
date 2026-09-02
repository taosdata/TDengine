#!/bin/bash

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

check_transactions() {
    for i in {1..30}
    do
        output=$(taos -s "show transactions;")
        if [[ $output == *"Query OK, 0 row(s)"* ]]; then
            echo "Success: No transactions are in progress."
            return 0
        fi
        sleep 1
    done

    echo "Error: Transactions are still in progress after 30 attempts."
    return 1
}

reset_cache() {
  response=$(curl --location -uroot:taosdata 'http://127.0.0.1:6041/rest/sql' --data 'reset query cache')

  if [[ $response == \{\"code\":0* ]]; then
    echo "Success: Query cache reset successfully."
  else
    echo "Error: Failed to reset query cache. Response: $response"
    return 1
  fi
}

cleanup_sql() {
  local sql="$1"
  local label="$2"
  local output=""

  for i in {1..10}
  do
    if output=$(taos -s "$sql" 2>&1); then
      echo "$output"
      check_transactions || return 1
      reset_cache || return 1
      return 0
    fi

    echo "$output"
    if [[ $output == *"VGroup is offline"* ]]; then
      echo "Warning: ${label} hit transient VGroup offline on attempt ${i}, retrying ..."
      sleep 1
      continue
    fi

    echo "Error: ${label} failed."
    return 1
  done

  echo "Error: ${label} failed after 10 attempts."
  return 1
}

taosd >>/dev/null 2>&1 &
taosadapter >>/dev/null 2>&1 &
sleep 1
cd ../../docs/examples/go

go mod tidy

go run ./connect/afconn/main.go
go run ./connect/cgoexample/main.go
go run ./connect/restexample/main.go
go run ./connect/connpool/main.go
go run ./connect/wsexample/main.go
go run ./connect/unified/main.go

cleanup_sql "drop database if exists power" "drop database power" || exit 1
go run ./sqlquery/main.go

cleanup_sql "drop database if exists power" "drop database power" || exit 1
go run ./queryreqid/main.go

cleanup_sql "drop database if exists power" "drop database power" || exit 1
go run ./stmt/native/main.go

cleanup_sql "drop database if exists power" "drop database power" || exit 1
go run ./stmt/ws/main.go

cleanup_sql "drop database if exists power" "drop database power" || exit 1
go run ./stmt/unified/main.go

cleanup_sql "drop database if exists power" "drop database power" || exit 1
go run ./stmt2/native/main.go

cleanup_sql "drop database if exists power" "drop database power" || exit 1
sleep 3
go run ./schemaless/native/main.go

cleanup_sql "drop database if exists power" "drop database power" || exit 1
go run ./schemaless/ws/main.go

cleanup_sql "drop database if exists power" "drop database power" || exit 1
go run ./schemaless/unified/main.go

cleanup_sql "drop topic if exists topic_meters" "drop topic topic_meters" || exit 1
cleanup_sql "drop database if exists power" "drop database power" || exit 1
go run ./tmq/native/main.go

cleanup_sql "drop topic if exists topic_meters" "drop topic topic_meters" || exit 1
cleanup_sql "drop database if exists power" "drop database power" || exit 1
go run ./tmq/ws/main.go


cleanup_sql "drop database if exists test" "drop database test" || exit 1
go run ./insert/json/main.go
cleanup_sql "drop database if exists test" "drop database test" || exit 1
go run ./insert/line/main.go
cleanup_sql "drop topic if exists topic_meters" "drop topic topic_meters" || exit 1
cleanup_sql "drop database if exists power" "drop database power" || exit 1
go run ./insert/sql/main.go
cleanup_sql "drop database if exists power" "drop database power" || exit 1
go run ./insert/stmt/main.go
cleanup_sql "drop database if exists test" "drop database test" || exit 1
go run ./insert/telnet/main.go

go run ./query/sync/main.go

cleanup_sql "drop topic if exists example_tmq_topic" "drop topic example_tmq_topic" || exit 1
cleanup_sql "drop database if exists example_tmq" "drop database example_tmq" || exit 1
go run ./sub/main.go
