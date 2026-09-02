/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef TDENGINE_STREAM_RECALC_TRACKER_H
#define TDENGINE_STREAM_RECALC_TRACKER_H

#include "streamMsg.h"

/* The tracker contract uses the stream state-transition error in this branch. */
#ifndef TSDB_CODE_INVALID_STATE
#define TSDB_CODE_INVALID_STATE TSDB_CODE_STREAM_INVALID_STATETRANS
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SStreamProgressRange {
  TSKEY start;
  TSKEY end;
} SStreamProgressRange;

typedef struct SStreamRecalcContributor {
  int64_t              recalcId;
  uint64_t             jobToken;  // Opaque tracker token; callers must not inspect or modify it.
  SStreamProgressRange requestedRange;
} SStreamRecalcContributor;

typedef struct SStreamRecalcTracker      SStreamRecalcTracker;
typedef struct SStreamRecalcAttemptState SStreamRecalcAttemptState;

typedef struct SStreamRecalcAttemptRef {
  uint64_t chainId;
  uint32_t executionOrdinal; /* 0..3 */
} SStreamRecalcAttemptRef;

typedef enum EStreamRecalcAttemptDecision {
  STREAM_RECALC_ATTEMPT_NONE = 0,
  STREAM_RECALC_ATTEMPT_RETRY = 1,
  STREAM_RECALC_ATTEMPT_EXHAUSTED = 2,
} EStreamRecalcAttemptDecision;

typedef struct SStreamRecalcAttemptOutcome {
  EStreamRecalcAttemptDecision decision;
  SStreamRecalcAttemptRef      attempt;
  int32_t                      errorCode;
} SStreamRecalcAttemptOutcome;

typedef struct SStreamRecalcDebugSnapshot {
  SStreamRecalcSnapshot snapshot;
  int32_t               fixedGroupCount;
  int64_t               terminalAtMs;
} SStreamRecalcDebugSnapshot;

/**
 * All ranges use the half-open form [start, end); end <= start is empty.
 * Array arguments are borrowed for the duration of a call. Registered groups
 * and step contributors are deep-copied and remain fixed afterward. Add binds
 * each contributor to the current job generation under the tracker lock;
 * callers must preserve the opaque jobToken. Merge rejects generation
 * conflicts, and BeginStep rejects stale queued contributors.
 *
 * The tracker serializes its operations internally. Its owner must stop all
 * concurrent calls before destroy. Snapshot arrays are owned by the caller.
 *
 * Reader and Runner tokens are registered explicitly before their first send.
 * SetTriggerDone closes Reader registration and declares how many
 * Runner registrations remain when it is called; a later new registration
 * consumes one declaration, while duplicate tokens consume none. Terminal
 * steps release full barrier state, and any later callback for a previously
 * issued step ID is an idempotent success. BeginStep changes Pending
 * contributors to Running; Running remains active and Finished/Failed remain
 * terminal. The last 100 terminal jobs are retained in terminal-transition
 * order; active jobs do not count toward this limit. Registering an evicted
 * recalc ID creates a new job because the tracker retains no evicted-ID
 * tombstones. Steps bind the internal registration generation, so callbacks
 * from the evicted generation cannot update the new job.
 */
int32_t stRecalcTrackerCreate(SStreamRecalcTracker **ppTracker);
void    stRecalcTrackerDestroy(SStreamRecalcTracker **ppTracker);
int32_t stRecalcTrackerRegisterJob(SStreamRecalcTracker *pTracker, int64_t recalcId,
                                   SStreamProgressRange requestedRange, const SArray *pGroupIds);
int32_t stRecalcTrackerMarkJobRunning(SStreamRecalcTracker *pTracker, int64_t recalcId);
/**
 * Fail a Pending job before execution publishes its first step. This path is
 * allocation-free so request construction can report allocation failures.
 * Repeating the call for a Failed job is idempotent; Running and Finished jobs
 * are rejected. errorCode must be nonzero.
 */
int32_t stRecalcTrackerFailJob(SStreamRecalcTracker *pTracker, int64_t recalcId, int32_t errorCode);
int32_t stRecalcContributorsAdd(SStreamRecalcTracker *pTracker, SArray **ppContributors, int64_t recalcId,
                                SStreamProgressRange requestedRange);
int32_t stRecalcContributorsMerge(SArray **ppDst, const SArray *pSrc);
int32_t stRecalcTrackerConfirmGroupPrefix(SStreamRecalcTracker *pTracker, int64_t gid, TSKEY confirmedThrough,
                                          const SArray *pContributors);
int32_t stRecalcAttemptCreate(size_t contributorCapacity, SStreamRecalcAttemptState **ppAttempt);
void    stRecalcAttemptDestroy(SStreamRecalcAttemptState **ppAttempt);
/*
 * Tracker calls are internally serialized; the tracker owner must prevent
 * destroy while calls are in flight. ActivateAttempt transfers *ppAttempt to
 * the tracker on success and clears it. Completion/failure APIs return the
 * operation status; when they return success, callers must also inspect the
 * supplied outcome decision to distinguish retry from exhaustion. The
 * execution ordinal is bounded by STREAM_RECALC_MAX_ATTEMPT_ORDINAL.
 */
int32_t stRecalcTrackerActivateAttempt(SStreamRecalcTracker *pTracker, SStreamRecalcAttemptState **ppAttempt,
                                       int64_t gid, SStreamProgressRange scanRange, SStreamProgressRange calcRange,
                                       const SArray *pContributors, SStreamRecalcAttemptRef *pRef);
int32_t stRecalcTrackerStartRetry(SStreamRecalcTracker *pTracker, uint64_t chainId, SStreamRecalcAttemptRef *pRef);
int32_t stRecalcTrackerRecordAttemptFailure(SStreamRecalcTracker *pTracker, SStreamRecalcAttemptRef attempt,
                                            int32_t errorCode, SStreamRecalcAttemptOutcome *pOutcome);
int32_t stRecalcTrackerBeginAttemptStep(SStreamRecalcTracker *pTracker, SStreamRecalcAttemptRef attempt, int64_t gid,
                                        SStreamProgressRange stepRange, const SArray *pContributors, uint64_t *pStepId);
int32_t stRecalcTrackerAddAttemptReader(SStreamRecalcTracker *pTracker, SStreamRecalcAttemptRef attempt,
                                        uint64_t stepId, uint64_t requestToken);
int32_t stRecalcTrackerCompleteAttemptReader(SStreamRecalcTracker *pTracker, SStreamRecalcAttemptRef attempt,
                                             uint64_t stepId, uint64_t requestToken, int32_t errorCode,
                                             SStreamRecalcAttemptOutcome *pOutcome);
int32_t stRecalcTrackerSetAttemptTriggerDone(SStreamRecalcTracker *pTracker, SStreamRecalcAttemptRef attempt,
                                             uint64_t stepId, int32_t pendingCalcParamCount, int32_t errorCode,
                                             SStreamRecalcAttemptOutcome *pOutcome);
int32_t stRecalcTrackerAddAttemptRunner(SStreamRecalcTracker *pTracker, SStreamRecalcAttemptRef attempt,
                                        uint64_t stepId, uint64_t requestToken);
int32_t stRecalcTrackerCompleteAttemptRunner(SStreamRecalcTracker *pTracker, SStreamRecalcAttemptRef attempt,
                                             uint64_t stepId, uint64_t requestToken, int32_t errorCode,
                                             SStreamRecalcAttemptOutcome *pOutcome);
int32_t stRecalcTrackerCompleteAttempt(SStreamRecalcTracker *pTracker, SStreamRecalcAttemptRef attempt,
                                       SStreamRecalcAttemptOutcome *pOutcome);
int32_t stRecalcTrackerBeginStep(SStreamRecalcTracker *pTracker, int64_t gid, SStreamProgressRange stepRange,
                                 const SArray *pContributors, uint64_t *pStepId);
int32_t stRecalcTrackerAddReader(SStreamRecalcTracker *pTracker, uint64_t stepId, uint64_t requestToken);
int32_t stRecalcTrackerCompleteReader(SStreamRecalcTracker *pTracker, uint64_t stepId, uint64_t requestToken);
int32_t stRecalcTrackerSetTriggerDone(SStreamRecalcTracker *pTracker, uint64_t stepId, int32_t pendingCalcParamCount);
int32_t stRecalcTrackerAddRunner(SStreamRecalcTracker *pTracker, uint64_t stepId, uint64_t requestToken);
int32_t stRecalcTrackerCompleteRunner(SStreamRecalcTracker *pTracker, uint64_t stepId, uint64_t requestToken);
int32_t stRecalcTrackerFailStep(SStreamRecalcTracker *pTracker, uint64_t stepId, int32_t errorCode);
int32_t stRecalcTrackerInitHistory(SStreamRecalcTracker *pTracker, bool enabled, SStreamProgressRange originalRange,
                                   bool checkpointFinished);
int32_t stRecalcTrackerConfirmHistoryPrefix(SStreamRecalcTracker *pTracker, TSKEY confirmedThrough);
int32_t stRecalcTrackerCommitHistoryThrough(SStreamRecalcTracker *pTracker, TSKEY committedThrough,
                                            bool terminalBarrierDone);
int32_t stRecalcTrackerCopySnapshot(SStreamRecalcTracker *pTracker, bool *pHistoryValid, int32_t *pHistoryProgressPct,
                                    SArray **ppRecalculates);
int32_t stRecalcTrackerCopySnapshotWithDetails(SStreamRecalcTracker *pTracker, bool *pHistoryValid,
                                               int32_t *pHistoryProgressPct, SArray **ppRecalculates,
                                               SArray **ppRecalcDetails);
/** Returned arrays contain deep-owned value snapshots and belong to the caller. */
int32_t stRecalcTrackerCopyDebugJobs(SStreamRecalcTracker *pTracker, SArray **ppJobs);
/**
 * Copies terminal events not previously taken and marks them taken atomically.
 * Allocation/copy failure leaves every event untaken for a later retry.
 */
int32_t stRecalcTrackerTakeTerminalEvents(SStreamRecalcTracker *pTracker, SArray **ppTerminals);
int32_t stRecalcTrackerGetDebugGauges(SStreamRecalcTracker *pTracker, int64_t *pActiveJobCount, bool *pHistoryValid,
                                      int32_t *pHistoryProgressPct);

#ifdef __cplusplus
}
#endif

#endif
