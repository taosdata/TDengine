# Quickstart: Validate Restoring-Aware Stepdown Guard

## Scenario Index
- US1: 2-query/assigned_stepdown_restoring_guard.py
- US2: 2-query/assigned_stepdown_recovery_loop.py
- US3: 2-query/assigned_stepdown_healthy_baseline.py

## Prerequisites
- Built TDinternal binaries from current branch.
- Two-node test environment capable of dual-replica vgroup scheduling.
- Ability to force-stop one replica process (for example kill -9 in test environment).

## Scenario A: Reproduce old risky path and validate fix
1. Start dual-replica cluster and create workload on a target vgroup.
2. Force-stop one replica process.
3. Confirm surviving replica transitions to assigned leader and continues serving.
4. Restart failed replica and keep it in restoring phase.
5. Generate append-reply/replication traffic until assigned commit threshold would be met.
6. Verify assigned leader does not step down while peer is restoring.
7. Verify vgroup remains available for read/write during this interval.

## Scenario B: Verify normal handoff after peer ready
1. Continue from Scenario A.
2. Wait until recovering peer reaches non-restoring ready state.
3. Verify assigned leader can now step down under existing prerequisites.
4. Verify election/handoff completes without manual intervention.

## Scenario C: Non-regression in healthy state
1. Run baseline dual-replica healthy replication workflow.
2. Verify no unexpected delay or suppression in normal transitions.
3. Confirm existing election/replication tests retain baseline pass behavior.

## Observability Checks
- Inspect sync logs around append-entries-reply handling and stepdown decisions.
- Confirm logs indicate the reason when stepdown is blocked by peer restoring state.
- Confirm no repeated election loop during recovery window.

## Exact Commands
- Run US1 restoring guard scenario:
  - `cd community/tests/system-test && ./pytest.sh python3 ./test.py -f 2-query/assigned_stepdown_restoring_guard.py`
- Run US2 recovery-loop scenario once:
  - `cd community/tests/system-test && ./pytest.sh python3 ./test.py -f 2-query/assigned_stepdown_recovery_loop.py`
- Run US2 recovery-loop scenario repeatedly:
  - `cd community/tests/system-test && SYNC_ASSIGNED_STEPDOWN_GUARD_DEMO=1 ./loop.sh -t 20 -f "./pytest.sh python3 ./test.py -f 2-query/assigned_stepdown_recovery_loop.py"`
- Run US3 healthy baseline scenario:
  - `cd community/tests/system-test && ./pytest.sh python3 ./test.py -f 2-query/assigned_stepdown_healthy_baseline.py`
