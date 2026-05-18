# Data Model: Prevent Restoring Stepdown Re-Election

## Entity: StepdownDecisionContext
- Purpose: Runtime decision context evaluated in assigned-leader append-reply handling.
- Fields:
  - role: current sync role (`ASSIGNED_LEADER`, `LEADER`, etc.)
  - commitIndex: effective commit progress value computed during append-reply processing
  - assignedCommitIndex: threshold at which assigned leader could step down
  - peerReplicaState: peer readiness signal including restoring/not-restoring
  - term: current raft term used in consistency checks
- Validation rules:
  - Stepdown eligibility requires role == `ASSIGNED_LEADER`.
  - Stepdown eligibility requires `commitIndex >= assignedCommitIndex`.
  - Stepdown eligibility requires peerReplicaState == non-restoring/ready.

## Entity: ReplicaReadinessState
- Purpose: Normalized view of peer readiness relevant to leader handoff decisions.
- Values:
  - Restoring: peer still replaying/recovering and not safe target for immediate re-election handoff.
  - Ready: peer restored and eligible as stable participant for election/handoff.
  - Unknown: state unavailable or stale; handled conservatively.
- Validation rules:
  - Unknown is treated as not-ready for stepdown gating.

## Entity: VGroupServiceAvailabilityWindow
- Purpose: Operational window capturing whether vgroup can continuously serve during recovery.
- Fields:
  - assignedLeaderActive: whether assigned leader continues serving requests
  - peerReadyTransitionObserved: whether recovering peer reached ready
  - electionTriggered: whether stepdown caused a re-election
- Validation rules:
  - While peer is restoring, electionTriggered should remain false from assigned-leader stepdown path.

## State Transitions
1. Peer failure -> surviving replica becomes assigned leader.
2. Peer restarting/restoring -> append replies arrive but peer readiness is not-ready.
3. Commit threshold reached while peer restoring -> assigned leader remains active (no stepdown).
4. Peer transitions to ready -> stepdown can proceed once existing prerequisites are met.
5. Re-election/handoff completes under normal behavior.
