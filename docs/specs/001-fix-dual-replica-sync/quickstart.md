# Quickstart: Validate Dual-Replica Recovery Fix

## 1) Build
```bash
cd /root/github/taosdata/TDinternal
cmake -S . -B build
cmake --build build -j
```

## 2) Prepare a 2-replica scenario
- Start a cluster with one vgroup configured for 2 replicas.
- Ensure both replicas are healthy and normal writes succeed.

## 3) Reproduce degraded-write and recovery flow
1. Stop replica R1.
2. Confirm R0 becomes `ASSIGNED_LEADER` and continue writing large data volume.
3. Restart R1.
4. Observe catchup phase (WAL replication and/or snapshot).

## 4) Validate acceptance criteria
- During catchup, writes do not stall beyond 30s.
- `CHECK_SYNC` does not flip to synced while lag is over `syncLogLagThreshold`.
- Progress logs appear every `syncCatchupLogIntervalMs` with lag values.
- Recovery transition includes term increment and election-based return to normal leader mode.

## 5) Suggested test execution paths
```bash
# System-test entry point
cd /root/github/taosdata/TDinternal/community/tests/system-test
./pytest.sh python3 test.py -f <target_case>

# Optional native test pass
cd /root/github/taosdata/TDinternal/build
ctest --output-on-failure
```

## 6) Config knobs to verify
- `syncLogLagThreshold` (default 1000, unit: log entries)
- `syncCatchupLogIntervalMs` (default 30000)

## 7) Regression scope
- Verify unchanged behavior for:
  - 3-replica vgroup
  - single-replica mode
  - normal dual-replica steady-state (no failure)
