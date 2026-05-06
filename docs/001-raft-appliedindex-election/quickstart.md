# Quickstart: Raft Election with Applied Progress Priority

## 1. Build targeted sync tests

From the repository root:

```sh
cmake --build build --target syncRequestVoteTest syncVotesGrantedTest syncElectTest
```

## 2. Implement the runtime change

Update the sync runtime in these areas:

- Add the applied-index field to `SyncRequestVote` in `community/source/libs/sync/inc/syncMessage.h`.
- Populate the field in `community/source/libs/sync/src/syncElection.c` when building vote requests.
- Read local applied progress from `FpAppliedIndexCb` in `community/source/libs/sync/src/syncRequestVote.c` and incorporate it after the existing log-recency gate.
- Extend request-vote logging in `community/source/libs/sync/src/syncUtil.c` or adjacent sync logging paths so operators can see the comparison inputs.

## 3. Add focused test coverage

Recommended first-pass coverage:

- `syncRequestVoteTest`: grant and reject decisions for higher, lower, equal, and unavailable applied-index cases.
- `syncVotesGrantedTest` or adjacent helpers: ensure tie handling remains deterministic.
- `syncElectTest`: verify a higher-applied candidate wins once all candidates are otherwise log-eligible.

## 4. Run targeted tests

Run the built binaries from the build output directory, for example:

```sh
./build/community/source/libs/sync/test/syncRequestVoteTest
./build/community/source/libs/sync/test/syncVotesGrantedTest
./build/community/source/libs/sync/test/syncElectTest
```

## 5. Capture failover baseline and comparison

- Record a baseline failover drill before enabling the new election preference.
- Re-run the same drill after the change using the same replica topology and workload.
- Compare median leader recovery completion time and keep the before/after timestamps with the test logs.
- If using `community/tests/pytest/cluster/syncingTest.py`, record total elapsed time around the relevant replica and schema-change sequence.

## 6. Verify observability and rollout assumptions

- Trigger an election scenario with unequal applied progress.
- Confirm logs include candidate/local applied index, last-log term/index, and the grant reason.
- Confirm tie scenarios still elect a single leader without repeated deadlock.
- Confirm unavailable applied index falls back to the existing deterministic path instead of blocking votes.
- Roll out only to same-version clusters because the RequestVote wire contract has changed.