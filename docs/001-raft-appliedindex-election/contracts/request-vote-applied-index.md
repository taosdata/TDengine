# Contract: RequestVote Applied-Index Preference

## Interface

- Interface type: Internal node-to-node sync RPC contract
- Producer: `syncNodeRequestVotePeers`
- Consumer: `syncNodeOnRequestVote`
- Message: `SyncRequestVote`

## Request Contract

Required fields for vote evaluation:

| Field | Meaning | Requirement |
|------|---------|-------------|
| `term` | Candidate term | Must equal sender current term |
| `lastLogTerm` | Candidate last log term | Must be populated from local sync log state |
| `lastLogIndex` | Candidate last log index | Must be populated from local sync log state |
| `candidateAppliedIndex` | Candidate applied progress snapshot | Must be populated from `FpAppliedIndexCb` at request-build time |

## Vote Evaluation Rules

1. Reject requests from nodes not in the raft group.
2. Update local term and step down if the request term is higher, preserving existing behavior.
3. Apply the existing log-recency gate based on `lastLogTerm` and `lastLogIndex`.
4. If the candidate fails the log-recency gate, reject the vote regardless of applied progress.
5. If the candidate passes the log-recency gate, compare `candidateAppliedIndex` with the receiver's local applied index.
6. Prefer granting votes to the candidate with greater or equal applied progress.
7. If either side reports an unavailable applied index, treat applied progress as unavailable for ordering and fall back to existing deterministic behavior.
8. If applied progress is equal, fall back to existing deterministic behavior.
9. Reset the election timer only when the vote is actually granted, preserving current liveness behavior.

## Observability Contract

- Vote-decision logs must include both log-recency inputs and applied-progress inputs.
- Logs for unequal applied indexes must reveal whether the applied-progress comparison influenced the grant result.
- Election outcome logs must allow operators to explain why the elected node was preferred.

## Compatibility Assumption

- This contract change assumes homogeneous cluster binaries for the feature rollout.
- Mixed-version compatibility is not included in this feature plan.
- Operational rollout guidance MUST document same-version deployment as a prerequisite.