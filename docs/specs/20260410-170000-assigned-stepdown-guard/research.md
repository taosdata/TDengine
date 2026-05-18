# Phase 0 Research: Prevent Restoring Stepdown Re-Election

## Decision 1: Gate assigned-leader stepdown on peer non-restoring readiness
- Decision: In assigned-leader mode, only allow stepdown when both conditions hold: commit progress threshold is satisfied and target peer is confirmed non-restoring/ready.
- Rationale: Current flow can step down on commit progress alone, which may trigger re-election while peer is still restoring and can lead to temporary no-service windows.
- Alternatives considered:
  - Keep commit-index-only gate: rejected because it reproduces the known outage path.
  - Delay all stepdowns for a fixed timeout: rejected because timeout-based behavior is brittle and can delay healthy handoff unnecessarily.
  - Require majority heartbeat stability window: rejected as over-broad for a dual-replica targeted bug fix.

## Decision 2: Use conservative fallback when peer readiness is unavailable
- Decision: If peer readiness cannot be determined at decision point, treat peer as not ready and do not step down.
- Rationale: Availability and correctness are safer with conservative gating in failure-recovery windows.
- Alternatives considered:
  - Optimistic fallback (assume ready): rejected because it can reintroduce restoring-leader election risk.
  - Hard failure on unknown state: rejected because transient state gaps are expected and should not crash control flow.

## Decision 3: Keep scope to sync append-reply stepdown path only
- Decision: Implement guard in the assigned leader stepdown decision path driven by append entries replies; do not redesign election framework.
- Rationale: This is the narrowest change that addresses the incident while minimizing regression surface.
- Alternatives considered:
  - Global election-rule rewrite: rejected as high risk and out of scope.
  - Vnode-level service gating only: rejected because root trigger is earlier in sync stepdown decision.

## Decision 4: Validate with dual-replica kill/restart recovery scenarios
- Decision: Verification will include force-kill one replica, recover peer in restoring, assert no stepdown, then assert normal handoff after peer ready.
- Rationale: Directly tests the user-reported fault path and non-regression expectations.
- Alternatives considered:
  - Unit-only validation: rejected; race and cluster-state behavior require integration/system-test coverage.

## End-to-End Checklist Validation Notes
- Added peer readiness helper and conservative unknown-state fallback for assigned stepdown gate.
- Added assigned-stepdown guard logs for blocked/allowed transitions.
- Added system-test scenario skeletons for restoring guard, recovery loop, and healthy baseline.
- Added explicit availability metric assertion path for SC-002 in US1 scenario.
