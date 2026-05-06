# Research: Raft Election with Applied Progress Priority

## Decision 1: Keep last-log-term and last-log-index as the primary safety gate

- Decision: Preserve the current `lastLogTerm` and `lastLogIndex` vote eligibility logic as the first-stage gate, and only evaluate applied progress after the candidate passes that existing Raft recency check.
- Rationale: The current implementation in `syncRequestVote.c` enforces Raft-style log freshness. Replacing that check with applied progress would risk violating election safety and diverging from established sync behavior.
- Alternatives considered: Use applied progress as the sole election criterion. Rejected because applied progress does not encode log freshness or term ordering and would weaken the existing safety guard.

## Decision 2: Extend `SyncRequestVote` with candidate applied index

- Decision: Add a candidate applied-index field to the internal `SyncRequestVote` message and populate it during `syncNodeRequestVotePeers`.
- Rationale: Remote voters cannot prefer the most up-to-date candidate unless the candidate's applied progress is transmitted with the vote request. The existing contract only carries term and log recency metadata.
- Alternatives considered: Infer candidate applied progress from heartbeats or cached peer state. Rejected because cached progress may be stale or absent exactly when an election is needed.

## Decision 3: Source applied progress from the FSM callback already exposed by sync

- Decision: Use `SSyncFSM::FpAppliedIndexCb` as the authoritative provider for local applied progress during vote request construction and vote evaluation.
- Rationale: The sync layer already invokes this callback for observability in `syncUtil.c`, which makes it the least invasive and most consistent source of applied progress.
- Alternatives considered: Introduce a new state field on `SSyncNode` or reuse `commitIndex`. Rejected because that duplicates existing data flow or fails to represent actually applied progress.

## Decision 4: Define applied progress as a deterministic preference factor, not a liveness blocker

- Decision: Use applied progress to prefer the candidate with the highest applied index when log recency is acceptable, but retain deterministic fallback behavior when applied indexes are equal or unavailable.
- Rationale: The feature request requires higher applied progress to be preferred, while the specification also requires elections to remain live under ties and degraded conditions.
- Alternatives considered: Refuse all votes for candidates with missing or lower applied index. Rejected because it could deadlock elections during partial visibility or uniform lag.

## Decision 5: Treat mixed-version message compatibility as out of scope for this feature slice

- Decision: Plan this feature as a homogeneous-version cluster change that updates both message producer and consumer together.
- Rationale: `SyncRequestVote` is a raw internal message contract with fixed-size allocation in `syncMessage.c`; adding fields changes the on-wire layout. Supporting mixed-version clusters would require a broader version-negotiation or dual-decoding design not requested here.
- Alternatives considered: Add backward-compatible dual-format decoding in the same feature. Rejected because it expands scope materially beyond election preference behavior.

## Decision 6: Validate behavior primarily with sync unit tests, plus optional cluster-style follow-up coverage

- Decision: Add or update tests in `community/source/libs/sync/test` for message contract coverage, vote-grant decision logic, and election outcomes, with cluster-style tests only if a unit harness cannot express a scenario.
- Rationale: The sync test suite already contains focused executables for request-vote, vote-manager, and election behavior, which is the most efficient place to pin regressions.
- Alternatives considered: Rely solely on manual `traft` cluster programs. Rejected because they are slower and less precise for guarding edge cases like ties and unavailable applied progress.