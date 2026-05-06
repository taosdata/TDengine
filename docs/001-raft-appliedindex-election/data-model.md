# Data Model: Raft Election with Applied Progress Priority

## Candidate Vote Request

- Purpose: Internal request-vote message emitted by a candidate to each voting peer during an election round.
- Fields:
  - `srcId`: Candidate raft identity.
  - `destId`: Target voter identity.
  - `term`: Candidate term for the election round.
  - `lastLogIndex`: Candidate last log index used by the existing recency gate.
  - `lastLogTerm`: Candidate last log term used by the existing recency gate.
  - `candidateAppliedIndex`: Candidate applied progress snapshot used for preference ordering.
- Validation rules:
  - `term` must equal the candidate's current persisted term at send time.
  - `candidateAppliedIndex` must be derived from the FSM callback at the time the request is built.
  - Message consumers must tolerate equal applied-index values and preserve deterministic tie fallback.

## Local Applied Progress Snapshot

- Purpose: The receiving node's current view of its own applied progress at vote-evaluation time.
- Fields:
  - `localAppliedIndex`: Value returned by `FpAppliedIndexCb`.
  - `localLastLogIndex`: Last local log index.
  - `localLastLogTerm`: Last local log term.
- Validation rules:
  - `localLastLogTerm` and `localLastLogIndex` remain the first-stage election safety check.
  - `localAppliedIndex` is compared only after the candidate satisfies the safety gate.

## Election Decision Record

- Purpose: Structured evidence for why a vote was granted or rejected and why a leader was elected.
- Fields:
  - `term`
  - `candidateId`
  - `voterId`
  - `candidateLastLogTerm`
  - `candidateLastLogIndex`
  - `candidateAppliedIndex`
  - `localLastLogTerm`
  - `localLastLogIndex`
  - `localAppliedIndex`
  - `grantDecision`
  - `decisionReason`
- Validation rules:
  - Every unequal-applied-index vote decision must be explainable from logged decision factors.
  - Logging must not remove or obscure the existing recency decision context.

## Election Round

- Purpose: One leader-election attempt for a vgroup term.
- Fields:
  - `term`
  - `candidateSet`
  - `voteResponses`
  - `winner`
  - `winnerAppliedIndex`
  - `tieFallbackUsed`
- State transitions:
  - `Follower -> Candidate`: election starts and self-vote recorded.
  - `Candidate -> Leader`: quorum obtained, possibly influenced by applied-progress preference.
  - `Candidate/Follower -> Follower`: step-down on higher term or vote for another candidate.