#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-full}"
LOOP_COUNT="${LOOP_COUNT:-20}"
STMT_CORE_PATTERN='TestStmtDisconnectFixedBehavior|TestStmtResponseBeforeServerClose|TestSTMTReconnect'
STMT_RECONNECT_PATTERN='TestSTMTReconnect'
SCHEMALESS_RECONNECT_PATTERN='TestSchemalessReconnect'
TMQ_RECONNECT_PATTERN='TestReconnectStaleFailureDoesNotClearActiveClient|TestReconnectDeadReplacementDoesNotShortCircuit'

# unified cross-failover suite
CROSS_FAILOVER_TESTS=(
  "TestUnifiedCrossFailoverDisconnectDetectionAndImmediateReconnect"
  "TestUnifiedCrossConcurrentSendFailoverAndSwitchBack"
  "TestUnifiedCrossMultiNodeFailoverChainUnderConcurrency"
  "TestUnifiedCrossDualNodeJitterWithConcurrentSchemalessWrites"
  "TestUnifiedTMQCrossFailoverDisconnectDetectionAndImmediateReconnect"
  "TestUnifiedTMQCrossConcurrentPollFailoverAndSwitchBack"
  "TestUnifiedTMQCrossMultiNodeFailoverChainUnderConcurrency"
  "TestUnifiedTMQCrossDualNodeJitterWithConcurrentPoll"
  "TestUnifiedQueryCrossFailoverDisconnectDetectionAndImmediateReconnect"
  "TestUnifiedQueryResultStatefulFetchNoReconnectOnDisconnect"
  "TestUnifiedQueryCrossConcurrentExecFailoverAndSwitchBack"
  "TestUnifiedQueryCrossMultiNodeFailoverChainUnderConcurrency"
  "TestUnifiedQueryCrossDualNodeJitterWithConcurrentExec"
  "TestUnifiedStmtCrossFailoverDisconnectDetectionAndImmediateReconnect"
  "TestUnifiedStmtCrossConcurrentExecFailoverAndSwitchBack"
  "TestUnifiedStmtCrossMultiNodeFailoverChainUnderConcurrency"
  "TestUnifiedStmtCrossDualNodeJitterWithConcurrentExec"
  "TestUnifiedIPv6CrossFailoverDisconnectDetectionAndImmediateReconnect"
  "TestUnifiedIPv6CrossConcurrentSendFailoverAndSwitchBack"
  "TestUnifiedQueryIPv6CrossFailoverDisconnectDetectionAndImmediateReconnect"
  "TestUnifiedQueryIPv6CrossConcurrentExecFailoverAndSwitchBack"
  "TestUnifiedStmtIPv6CrossConcurrentExecFailoverAndSwitchBack"
  "TestUnifiedTMQIPv6CrossFailoverDisconnectDetectionAndImmediateReconnect"
  "TestUnifiedTMQIPv6CrossConcurrentPollFailoverAndSwitchBack"
)

LOOP_TESTS=(
  "TestUnifiedCrossDualNodeJitterLoop"
  "TestUnifiedTMQCrossDualNodeJitterLoop"
  "TestUnifiedQueryCrossDualNodeJitterLoop"
  "TestUnifiedStmtCrossDualNodeJitterLoop"
)

IPV6_SMOKE_FAILOVER_TESTS=(
  "TestUnifiedIPv6CrossFailoverDisconnectDetectionAndImmediateReconnect"
  "TestUnifiedQueryIPv6CrossFailoverDisconnectDetectionAndImmediateReconnect"
  "TestUnifiedTMQIPv6CrossFailoverDisconnectDetectionAndImmediateReconnect"
)

cd "${ROOT_DIR}"

preclean_unified_cross_residue() {
  local api
  local resp
  local item

  if ! command -v curl >/dev/null 2>&1; then
    return 0
  fi

  api="http://127.0.0.1:6041/rest/sql"

  resp="$(curl -fsS -u root:taosdata -X POST "${api}" -d "select topic_name from information_schema.ins_topics" 2>/dev/null || true)"
  while IFS= read -r item; do
    [[ -n "${item}" ]] || continue
    case "${item}" in
      tmq_cross_*|tmq_wrapper_topic_*)
        curl -fsS -u root:taosdata -X POST "${api}" -d "drop topic if exists ${item}" >/dev/null 2>&1 || true
        ;;
    esac
  done < <(printf '%s' "${resp}" | sed -n 's/.*"data":\[\(.*\)\],"rows".*/\1/p' | tr -d '[]' | tr ',' '\n' | sed -n 's/"\([^"]*\)".*/\1/p')

  resp="$(curl -fsS -u root:taosdata -X POST "${api}" -d "select name from information_schema.ins_databases where name like 'test_unified_cross_%' or name like 'test_ws_tmq_wrapper_cross%'" 2>/dev/null || true)"
  while IFS= read -r item; do
    [[ -n "${item}" ]] || continue
    curl -fsS -u root:taosdata -X POST "${api}" -d "drop database if exists ${item}" >/dev/null 2>&1 || true
  done < <(printf '%s' "${resp}" | sed -n 's/.*"data":\[\(.*\)\],"rows".*/\1/p' | tr -d '[]' | tr ',' '\n' | sed -n 's/"\([^"]*\)".*/\1/p')
}

preclean_unified_cross_residue

join_by_pipe() {
  local out=""
  local item
  for item in "$@"; do
    if [[ -z "${out}" ]]; then
      out="${item}"
    else
      out="${out}|${item}"
    fi
  done
  printf '%s' "${out}"
}

run_unified_cross_failover_smoke() {
  local pattern
  pattern="$(join_by_pipe \
    TestUnifiedCrossFailoverDisconnectDetectionAndImmediateReconnect \
    TestUnifiedTMQCrossFailoverDisconnectDetectionAndImmediateReconnect \
    TestUnifiedQueryCrossFailoverDisconnectDetectionAndImmediateReconnect \
    TestUnifiedStmtCrossFailoverDisconnectDetectionAndImmediateReconnect \
    "${IPV6_SMOKE_FAILOVER_TESTS[@]}")"
  go test ./ws/unified -run "${pattern}" -count=1
}

run_unified_cross_failover_once() {
  local pattern
  pattern="$(join_by_pipe "${CROSS_FAILOVER_TESTS[@]}")"
  go test ./ws/unified -run "${pattern}" -count=1
}

run_unified_cross_failover_loop() {
  local pattern
  pattern="$(join_by_pipe "${LOOP_TESTS[@]}")"
  LOOP_COUNT="${LOOP_COUNT}" go test ./ws/unified -run "${pattern}" -count=1
}

run_pkg_by_pkg() {
  go test -race ./ws/client -count=1
  go test -race ./ws/stmt -run "${STMT_CORE_PATTERN}" -count=1
  go test -race ./ws/schemaless -count=1
  go test -race ./ws/tmq -count=1
}

run_core_reconnect_regressions() {
  go test -race ./ws/tmq -run "${TMQ_RECONNECT_PATTERN}" -count=1
  go test -race ./ws/schemaless -run "${SCHEMALESS_RECONNECT_PATTERN}" -count=1
  go test -race ./ws/stmt -run "${STMT_RECONNECT_PATTERN}" -count=1
}

run_core_reconnect_regressions_loop() {
  go test -race ./ws/tmq -run "${TMQ_RECONNECT_PATTERN}" -count="${LOOP_COUNT}"
  go test -race ./ws/schemaless -run "${SCHEMALESS_RECONNECT_PATTERN}" -count="${LOOP_COUNT}"
  go test -race ./ws/stmt -run "${STMT_RECONNECT_PATTERN}" -count="${LOOP_COUNT}"
}

run_deterministic_full() {
  go test -race ./ws/client ./ws/schemaless ./ws/tmq ./ws/stmt -run "${STMT_CORE_PATTERN}" -count=1
}

run_full_base() {
  run_deterministic_full
  run_pkg_by_pkg
  run_core_reconnect_regressions
  run_core_reconnect_regressions_loop
  run_unified_cross_failover_once
}

run_full_integration() {
  go test -race ./ws/... -count=1
  run_full_base
  run_unified_cross_failover_loop
}

run_loop_full() {
  go test -race ./ws/tmq ./ws/schemaless ./ws/stmt -count="${LOOP_COUNT}"
  run_unified_cross_failover_loop
}

run_cross_mode() {
  local mode="${1}"
  case "${mode}" in
    cross-smoke)
      run_unified_cross_failover_smoke
      ;;
    cross-full)
      run_unified_cross_failover_once
      ;;
    cross-loop)
      run_unified_cross_failover_loop
      ;;
    cross-full-loop)
      run_unified_cross_failover_once
      run_unified_cross_failover_loop
      ;;
    *)
      echo "invalid cross mode: ${mode}" >&2
      return 1
      ;;
  esac
}

case "${MODE}" in
  cross-smoke|cross-full|cross-loop|cross-full-loop)
    run_cross_mode "${MODE}"
    exit 0
    ;;
esac

case "${MODE}" in
  quick)
    run_core_reconnect_regressions
    run_unified_cross_failover_smoke
    ;;
  loop)
    run_core_reconnect_regressions_loop
    run_unified_cross_failover_loop
    ;;
  loop-full)
    run_loop_full
    ;;
  full)
    run_full_base
    ;;
  full-integration)
    run_full_integration
    ;;
  *)
    echo "usage: ws/reliability_gate.sh [quick|loop|loop-full|full|full-integration|cross-smoke|cross-full|cross-loop|cross-full-loop]" >&2
    exit 1
    ;;
esac
