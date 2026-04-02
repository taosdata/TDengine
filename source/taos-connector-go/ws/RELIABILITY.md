# WebSocket Reliability Contract

This document defines what "safe to use" means for the `ws` stack and how to verify it.

## Scope

Packages in scope:

- `ws/client`
- `ws/stmt`
- `ws/schemaless`
- `ws/tmq`

## Lifecycle Contract

Every websocket client instance must follow this lifecycle:

1. `Init` -> client created and handlers installed.
2. `Running` -> read/write pumps active and requests accepted.
3. `Reconnecting` -> old client may fail requests, replacement is dialed.
4. `Closed` -> no new requests accepted, all waits unblocked.

Required guarantees:

- Only one active client pointer is published at a time.
- A stale reconnect flow must never clear a newer healthy client.
- Reconnect short-circuit is allowed only when replacement client is still running.
- Failed request waiters are removed from pending lists before return.

## Fault Matrix

Each case must have test coverage.

- Normal request/response path succeeds.
- Send returns closed/network error and auto-reconnect is enabled.
- Connection dies while waiting for response (`Done` closes).
- Message timeout is reached.
- Consumer/schemaless object is closed while request is in flight.
- Reconnect dial retries are exhausted.
- Reconnect succeeds but post-reconnect subscribe/bootstrap fails.
- Concurrent reconnect calls from multiple goroutines.
- Error context includes request summary only with sensitive fields redacted.

## Acceptance Gate

A change is release-ready only if all checks pass:

1. Deterministic race gate:
   - `./ws/reliability_gate.sh full`
2. Core reconnect loop:
   - `go test -race ./ws/tmq -run 'TestReconnectStaleFailureDoesNotClearActiveClient|TestReconnectDeadReplacementDoesNotShortCircuit' -count=20`
   - `go test -race ./ws/schemaless -run 'TestSchemalessReconnect' -count=20`
   - `go test -race ./ws/stmt -run 'TestSTMTReconnect' -count=20`
3. Full integration race gate (requires clean TDengine + taosadapter test env):
   - `./ws/reliability_gate.sh full-integration`
4. Nightly heavy smoke (optional):
   - `./ws/reliability_gate.sh loop-full`

## Unified Cross-Failover Suite

- Script: `ws/reliability_gate.sh`
- Modes:
  - `cross-smoke`: fast local verification
  - `cross-full`: run all cross-failover integration tests once (schemaless + tmq + query/fetch + stmt)
  - `cross-loop`: run jitter loop tests (`LOOP_COUNT` controls rounds)
  - `cross-full-loop`: run `cross-full` then `cross-loop`
- TMQ cross scenarios are aligned with other protocol adapters:
  - `TestUnifiedTMQCrossFailoverDisconnectDetectionAndImmediateReconnect`
  - `TestUnifiedTMQCrossConcurrentPollFailoverAndSwitchBack`
  - `TestUnifiedTMQCrossMultiNodeFailoverChainUnderConcurrency`
  - `TestUnifiedTMQCrossDualNodeJitterWithConcurrentPoll`
  - `TestUnifiedTMQCrossDualNodeJitterLoop`
- To add new cross tests later, append test names to `CROSS_FAILOVER_TESTS`/`LOOP_TESTS` in `ws/reliability_gate.sh`.
- Dedicated workflow: `.github/workflows/ws-unified-cross-failover.yml`
  - triggers: daily schedule + `workflow_dispatch`
  - scheduled command: `LOOP_COUNT=20 ./ws/reliability_gate.sh cross-full-loop`

## CI Report

- Main CI (stable) runs `LOOP_COUNT=20 ./ws/reliability_gate.sh full`.
- Scheduled/manual report workflow:
  - file: `.github/workflows/ws-reliability-report.yml`
  - trigger: daily schedule + `workflow_dispatch`
  - command: `LOOP_COUNT=20 ./ws/reliability_gate.sh full-integration`
  - artifacts: gate output, package summary, runtime logs

## Operational Notes

- Integration tests that create databases must always clean up in `t.Cleanup`.
- Use unique db names in reconnect/failure tests to avoid cross-test pollution.
- Exception: unified cross-failover helpers may reuse one fixed database to reduce vgroup/vnode pressure in stress loops.
- Temporary local artifacts (patch files, test binaries) must not be committed.
