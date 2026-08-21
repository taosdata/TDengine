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

#include "streamRecalcTracker.h"

#include "os.h"
#include "taoserror.h"
#include "thash.h"

#define STREAM_RECALC_MAX_TERMINAL_JOBS 100

typedef struct SStreamWideUInt {
  uint64_t hi;
  uint64_t lo;
} SStreamWideUInt;

typedef struct SStreamRecalcGroupProgress {
  int64_t gid;
  TSKEY   committedThrough;
} SStreamRecalcGroupProgress;

typedef struct SStreamRecalcJob {
  int64_t              recalcId;
  uint64_t             generation;
  SStreamProgressRange requestedRange;
  SArray              *pGroups;
  int32_t              progressPct;
  EStreamRecalcStatus  status;
  int64_t              terminalAtMs;
  bool                 terminalDebugEmitted;
} SStreamRecalcJob;

typedef struct SStreamRecalcStep {
  uint64_t             stepId;
  int64_t              gid;
  SStreamProgressRange stepRange;
  SArray              *pContributors;
  SArray              *pReaderTokens;
  SArray              *pCompletedReaderTokens;
  SArray              *pRunnerTokens;
  SArray              *pCompletedRunnerTokens;
  bool                 triggerDone;
  int32_t              declaredPendingCalcParamCount;
  int32_t              pendingCalcParamCount;
  bool                 committed;
  bool                 failed;
} SStreamRecalcStep;

struct SStreamRecalcTracker {
  SRWLatch             lock;
  SHashObj            *pJobs;
  SHashObj            *pSteps;
  int64_t              terminalJobIds[STREAM_RECALC_MAX_TERMINAL_JOBS];
  uint32_t             terminalJobStart;
  uint32_t             terminalJobCount;
  uint64_t             nextJobGeneration;
  uint64_t             nextStepId;
  int64_t              activeJobCount;
  bool                 historyInitialized;
  bool                 historyEnabled;
  bool                 historyCheckpointFinished;
  SStreamProgressRange historyRange;
  TSKEY                historyConfirmedThrough;
  TSKEY                historyCommittedThrough;
  bool                 historyTerminalBarrierDone;
  int32_t              historyProgressPct;
};

static bool stRangeEquals(SStreamProgressRange lhs, SStreamProgressRange rhs) {
  return lhs.start == rhs.start && lhs.end == rhs.end;
}

static uint64_t stRangeLength(SStreamProgressRange range) {
  if (range.end <= range.start) return 0;
  return (uint64_t)range.end - (uint64_t)range.start;
}

static void stWideAdd(SStreamWideUInt *pValue, uint64_t delta) {
  uint64_t previous = pValue->lo;
  pValue->lo += delta;
  if (pValue->lo < previous) ++pValue->hi;
}

static bool stWideIsZero(SStreamWideUInt value) { return value.hi == 0 && value.lo == 0; }

#if defined(__SIZEOF_INT128__) && !defined(STREAM_RECALC_FORCE_WIDE_FALLBACK)
static int32_t stWidePercent(SStreamWideUInt completed, SStreamWideUInt total) {
  unsigned __int128 completedValue = ((unsigned __int128)completed.hi << 64) | completed.lo;
  unsigned __int128 totalValue = ((unsigned __int128)total.hi << 64) | total.lo;
  unsigned __int128 quotient = totalValue / 100;
  uint32_t          remainder = (uint32_t)(totalValue % 100);
  for (int32_t pct = 99; pct > 0; --pct) {
    unsigned __int128 threshold = quotient * (uint32_t)pct + (remainder * (uint32_t)pct + 99) / 100;
    if (completedValue >= threshold) return pct;
  }
  return 0;
}
#else
static SStreamWideUInt stWideMultiplySmall(SStreamWideUInt value, uint32_t multiplier) {
  uint64_t lowProduct = (value.lo & UINT32_MAX) * multiplier;
  uint64_t highProduct = (value.lo >> 32) * multiplier;
  uint64_t shiftedHigh = highProduct << 32;
  uint64_t low = lowProduct + shiftedHigh;
  uint64_t carry = (highProduct >> 32) + (low < lowProduct ? 1 : 0);
  return (SStreamWideUInt){.hi = value.hi * multiplier + carry, .lo = low};
}

static int32_t stWideCompare(SStreamWideUInt lhs, SStreamWideUInt rhs) {
  if (lhs.hi != rhs.hi) return lhs.hi < rhs.hi ? -1 : 1;
  if (lhs.lo == rhs.lo) return 0;
  return lhs.lo < rhs.lo ? -1 : 1;
}

static SStreamWideUInt stWideDivideSmall(SStreamWideUInt value, uint32_t divisor, uint32_t *pRemainder) {
  SStreamWideUInt quotient = {.hi = value.hi / divisor};
  uint32_t        remainder = (uint32_t)(value.hi % divisor);
  for (int32_t bit = 63; bit >= 0; --bit) {
    remainder = remainder * 2 + (uint32_t)((value.lo >> bit) & 1);
    if (remainder >= divisor) {
      remainder -= divisor;
      quotient.lo |= (uint64_t)1 << bit;
    }
  }
  *pRemainder = remainder;
  return quotient;
}

static int32_t stWidePercent(SStreamWideUInt completed, SStreamWideUInt total) {
  uint32_t        remainder = 0;
  SStreamWideUInt quotient = stWideDivideSmall(total, 100, &remainder);
  for (int32_t pct = 99; pct > 0; --pct) {
    SStreamWideUInt threshold = stWideMultiplySmall(quotient, (uint32_t)pct);
    stWideAdd(&threshold, (remainder * (uint32_t)pct + 99) / 100);
    if (stWideCompare(completed, threshold) >= 0) return pct;
  }
  return 0;
}
#endif

static int32_t stProgressPercent(SStreamWideUInt completed, SStreamWideUInt total, bool finished) {
  if (stWideIsZero(total) || finished) return 100;
  int32_t pct = stWidePercent(completed, total);
  return pct < 99 ? pct : 99;
}

static bool stRecalcStatusCanTransition(EStreamRecalcStatus from, EStreamRecalcStatus to) {
  return (from == STREAM_RECALC_STATUS_PENDING &&
          (to == STREAM_RECALC_STATUS_RUNNING || to == STREAM_RECALC_STATUS_FAILED)) ||
         (from == STREAM_RECALC_STATUS_RUNNING &&
          (to == STREAM_RECALC_STATUS_FINISHED || to == STREAM_RECALC_STATUS_FAILED));
}

static bool stTokenExists(const SArray *pTokens, uint64_t token) {
  for (size_t i = 0; i < taosArrayGetSize(pTokens); ++i) {
    const uint64_t *pCurrent = taosArrayGet(pTokens, i);
    if (*pCurrent == token) return true;
  }
  return false;
}

static SStreamRecalcJob *stGetJob(SStreamRecalcTracker *pTracker, int64_t recalcId) {
  SStreamRecalcJob **ppJob = taosHashGet(pTracker->pJobs, &recalcId, sizeof(recalcId));
  return ppJob == NULL ? NULL : *ppJob;
}

static void stDestroyJob(SStreamRecalcJob *pJob);

static void stRetainTerminalJob(SStreamRecalcTracker *pTracker, int64_t recalcId) {
  if (pTracker->terminalJobCount < STREAM_RECALC_MAX_TERMINAL_JOBS) {
    uint32_t index = (pTracker->terminalJobStart + pTracker->terminalJobCount) % STREAM_RECALC_MAX_TERMINAL_JOBS;
    pTracker->terminalJobIds[index] = recalcId;
    ++pTracker->terminalJobCount;
    return;
  }

  int64_t           evictedId = pTracker->terminalJobIds[pTracker->terminalJobStart];
  SStreamRecalcJob *pEvicted = stGetJob(pTracker, evictedId);
  if (pEvicted == NULL || taosHashRemove(pTracker->pJobs, &evictedId, sizeof(evictedId)) != TSDB_CODE_SUCCESS) return;
  stDestroyJob(pEvicted);
  pTracker->terminalJobIds[pTracker->terminalJobStart] = recalcId;
  pTracker->terminalJobStart = (pTracker->terminalJobStart + 1) % STREAM_RECALC_MAX_TERMINAL_JOBS;
}

static void stRecordJobTerminal(SStreamRecalcTracker *pTracker, SStreamRecalcJob *pJob) {
  pJob->terminalAtMs = taosGetTimestampMs();
  if (pTracker->activeJobCount > 0) --pTracker->activeJobCount;
  stRetainTerminalJob(pTracker, pJob->recalcId);
}

static SStreamRecalcStep *stGetStep(SStreamRecalcTracker *pTracker, uint64_t stepId) {
  SStreamRecalcStep **ppStep = taosHashGet(pTracker->pSteps, &stepId, sizeof(stepId));
  return ppStep == NULL ? NULL : *ppStep;
}

static int32_t stMissingStepCode(const SStreamRecalcTracker *pTracker, uint64_t stepId) {
  return stepId != 0 && stepId < pTracker->nextStepId ? TSDB_CODE_SUCCESS : TSDB_CODE_NOT_FOUND;
}

static SStreamRecalcGroupProgress *stGetGroupProgress(SStreamRecalcJob *pJob, int64_t gid) {
  for (size_t i = 0; i < taosArrayGetSize(pJob->pGroups); ++i) {
    SStreamRecalcGroupProgress *pGroup = taosArrayGet(pJob->pGroups, i);
    if (pGroup->gid == gid) return pGroup;
  }
  return NULL;
}

static void stRefreshJobProgress(SStreamRecalcJob *pJob) {
  SStreamWideUInt total = {0};
  SStreamWideUInt completed = {0};
  bool            finished = true;
  const uint64_t  rangeLength = stRangeLength(pJob->requestedRange);

  for (size_t i = 0; i < taosArrayGetSize(pJob->pGroups); ++i) {
    const SStreamRecalcGroupProgress *pGroup = taosArrayGet(pJob->pGroups, i);
    stWideAdd(&total, rangeLength);
    TSKEY completedThrough = pGroup->committedThrough;
    if (completedThrough < pJob->requestedRange.start) completedThrough = pJob->requestedRange.start;
    if (completedThrough > pJob->requestedRange.end) completedThrough = pJob->requestedRange.end;
    stWideAdd(&completed, stRangeLength((SStreamProgressRange){pJob->requestedRange.start, completedThrough}));
    if (rangeLength != 0 && pGroup->committedThrough < pJob->requestedRange.end) finished = false;
  }

  if (taosArrayGetSize(pJob->pGroups) == 0 || rangeLength == 0) finished = true;
  int32_t progressPct = stProgressPercent(completed, total, finished);
  if (progressPct > pJob->progressPct) pJob->progressPct = progressPct;
  if (finished && pJob->status == STREAM_RECALC_STATUS_RUNNING &&
      stRecalcStatusCanTransition(pJob->status, STREAM_RECALC_STATUS_FINISHED)) {
    pJob->status = STREAM_RECALC_STATUS_FINISHED;
  }
}

static void stAdvanceJob(SStreamRecalcTracker *pTracker, SStreamRecalcJob *pJob, int64_t gid,
                         SStreamProgressRange stepRange, SStreamProgressRange contributorRange) {
  if (pJob->status == STREAM_RECALC_STATUS_FINISHED || pJob->status == STREAM_RECALC_STATUS_FAILED) return;
  if (pJob->status == STREAM_RECALC_STATUS_PENDING &&
      stRecalcStatusCanTransition(pJob->status, STREAM_RECALC_STATUS_RUNNING)) {
    pJob->status = STREAM_RECALC_STATUS_RUNNING;
  }

  SStreamRecalcGroupProgress *pGroup = stGetGroupProgress(pJob, gid);
  if (pGroup == NULL) return;

  TSKEY intersectionStart = stepRange.start > contributorRange.start ? stepRange.start : contributorRange.start;
  if (intersectionStart < pJob->requestedRange.start) intersectionStart = pJob->requestedRange.start;
  TSKEY intersectionEnd = stepRange.end < contributorRange.end ? stepRange.end : contributorRange.end;
  if (intersectionEnd > pJob->requestedRange.end) intersectionEnd = pJob->requestedRange.end;
  if (intersectionEnd > intersectionStart && intersectionStart <= pGroup->committedThrough &&
      intersectionEnd > pGroup->committedThrough) {
    pGroup->committedThrough = intersectionEnd;
  }
  EStreamRecalcStatus previousStatus = pJob->status;
  stRefreshJobProgress(pJob);
  if (previousStatus != pJob->status && pJob->status == STREAM_RECALC_STATUS_FINISHED) {
    stRecordJobTerminal(pTracker, pJob);
  }
}

static bool stStepCanCommit(const SStreamRecalcStep *pStep) {
  return pStep->triggerDone && pStep->pendingCalcParamCount == 0 &&
         taosArrayGetSize(pStep->pCompletedReaderTokens) == taosArrayGetSize(pStep->pReaderTokens) &&
         taosArrayGetSize(pStep->pCompletedRunnerTokens) == taosArrayGetSize(pStep->pRunnerTokens);
}

static void stDestroyStep(SStreamRecalcStep *pStep);

static void stRetireStep(SStreamRecalcTracker *pTracker, SStreamRecalcStep *pStep) {
  if (taosHashRemove(pTracker->pSteps, &pStep->stepId, sizeof(pStep->stepId)) == TSDB_CODE_SUCCESS) {
    stDestroyStep(pStep);
  }
}

static void stCommitStep(SStreamRecalcTracker *pTracker, SStreamRecalcStep *pStep) {
  if (pStep->committed || pStep->failed || !stStepCanCommit(pStep)) return;
  pStep->committed = true;
  for (size_t i = 0; i < taosArrayGetSize(pStep->pContributors); ++i) {
    const SStreamRecalcContributor *pContributor = taosArrayGet(pStep->pContributors, i);
    SStreamRecalcJob               *pJob = stGetJob(pTracker, pContributor->recalcId);
    if (pJob != NULL && pJob->generation == pContributor->jobToken) {
      stAdvanceJob(pTracker, pJob, pStep->gid, pStep->stepRange, pContributor->requestedRange);
    }
  }
  stRetireStep(pTracker, pStep);
}

static void stDestroyJob(SStreamRecalcJob *pJob) {
  if (pJob == NULL) return;
  taosArrayDestroy(pJob->pGroups);
  taosMemoryFree(pJob);
}

static void stDestroyStep(SStreamRecalcStep *pStep) {
  if (pStep == NULL) return;
  taosArrayDestroy(pStep->pContributors);
  taosArrayDestroy(pStep->pReaderTokens);
  taosArrayDestroy(pStep->pCompletedReaderTokens);
  taosArrayDestroy(pStep->pRunnerTokens);
  taosArrayDestroy(pStep->pCompletedRunnerTokens);
  taosMemoryFree(pStep);
}

static void stDestroyJobs(SHashObj *pJobs) {
  SStreamRecalcJob **ppJob = taosHashIterate(pJobs, NULL);
  while (ppJob != NULL) {
    stDestroyJob(*ppJob);
    ppJob = taosHashIterate(pJobs, ppJob);
  }
}

static void stDestroySteps(SHashObj *pSteps) {
  SStreamRecalcStep **ppStep = taosHashIterate(pSteps, NULL);
  while (ppStep != NULL) {
    stDestroyStep(*ppStep);
    ppStep = taosHashIterate(pSteps, ppStep);
  }
}

int32_t stRecalcTrackerCreate(SStreamRecalcTracker **ppTracker) {
  if (ppTracker == NULL) return TSDB_CODE_INVALID_PARA;
  *ppTracker = taosMemoryCalloc(1, sizeof(**ppTracker));
  if (*ppTracker == NULL) return terrno;

  taosInitRWLatch(&(*ppTracker)->lock);
  (*ppTracker)->pJobs = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  (*ppTracker)->pSteps = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_NO_LOCK);
  if ((*ppTracker)->pJobs == NULL || (*ppTracker)->pSteps == NULL) {
    int32_t code = terrno;
    taosHashCleanup((*ppTracker)->pJobs);
    taosHashCleanup((*ppTracker)->pSteps);
    taosMemoryFreeClear(*ppTracker);
    return code;
  }
  (*ppTracker)->nextJobGeneration = 1;
  (*ppTracker)->nextStepId = 1;
  return TSDB_CODE_SUCCESS;
}

void stRecalcTrackerDestroy(SStreamRecalcTracker **ppTracker) {
  if (ppTracker == NULL || *ppTracker == NULL) return;
  SStreamRecalcTracker *pTracker = *ppTracker;
  *ppTracker = NULL;
  stDestroyJobs(pTracker->pJobs);
  stDestroySteps(pTracker->pSteps);
  taosHashCleanup(pTracker->pJobs);
  taosHashCleanup(pTracker->pSteps);
  taosMemoryFree(pTracker);
}

int32_t stRecalcTrackerRegisterJob(SStreamRecalcTracker *pTracker, int64_t recalcId,
                                   SStreamProgressRange requestedRange, const SArray *pGroupIds) {
  if (pTracker == NULL || (pGroupIds != NULL && pGroupIds->elemSize != sizeof(int64_t))) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  taosWLockLatch(&pTracker->lock);
  SStreamRecalcJob *pExisting = stGetJob(pTracker, recalcId);
  if (pExisting != NULL) {
    code = stRangeEquals(pExisting->requestedRange, requestedRange) ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_MSG;
    taosWUnLockLatch(&pTracker->lock);
    return code;
  }
  if (pTracker->nextJobGeneration == UINT64_MAX) {
    code = TSDB_CODE_OUT_OF_RANGE;
    goto _exit;
  }

  SStreamRecalcJob *pJob = taosMemoryCalloc(1, sizeof(*pJob));
  if (pJob == NULL) {
    code = terrno;
    goto _exit;
  }
  pJob->recalcId = recalcId;
  pJob->generation = pTracker->nextJobGeneration;
  pJob->requestedRange = requestedRange;
  pJob->status = STREAM_RECALC_STATUS_PENDING;
  pJob->pGroups =
      taosArrayInit(pGroupIds == NULL ? 0 : taosArrayGetSize(pGroupIds), sizeof(SStreamRecalcGroupProgress));
  if (pJob->pGroups == NULL) {
    code = terrno;
    stDestroyJob(pJob);
    goto _exit;
  }

  if (pGroupIds != NULL) {
    for (size_t i = 0; i < taosArrayGetSize(pGroupIds); ++i) {
      const int64_t             *pGid = taosArrayGet(pGroupIds, i);
      SStreamRecalcGroupProgress group = {.gid = *pGid, .committedThrough = requestedRange.start};
      if (taosArrayPush(pJob->pGroups, &group) == NULL) {
        code = terrno;
        stDestroyJob(pJob);
        goto _exit;
      }
    }
  }
  stRefreshJobProgress(pJob);
  if (pJob->progressPct == 100) {
    pJob->status = STREAM_RECALC_STATUS_FINISHED;
    pJob->terminalAtMs = taosGetTimestampMs();
  }
  code = taosHashPut(pTracker->pJobs, &recalcId, sizeof(recalcId), &pJob, sizeof(pJob));
  if (code != TSDB_CODE_SUCCESS) {
    stDestroyJob(pJob);
  } else {
    ++pTracker->nextJobGeneration;
    if (pJob->status == STREAM_RECALC_STATUS_FINISHED) {
      stRetainTerminalJob(pTracker, recalcId);
    } else {
      ++pTracker->activeJobCount;
    }
  }

_exit:
  taosWUnLockLatch(&pTracker->lock);
  return code;
}

int32_t stRecalcTrackerMarkJobRunning(SStreamRecalcTracker *pTracker, int64_t recalcId) {
  if (pTracker == NULL) return TSDB_CODE_INVALID_PARA;
  int32_t code = TSDB_CODE_SUCCESS;
  taosWLockLatch(&pTracker->lock);
  SStreamRecalcJob *pJob = stGetJob(pTracker, recalcId);
  if (pJob == NULL) {
    code = TSDB_CODE_NOT_FOUND;
  } else if (pJob->status == STREAM_RECALC_STATUS_RUNNING) {
    code = TSDB_CODE_SUCCESS;
  } else if (stRecalcStatusCanTransition(pJob->status, STREAM_RECALC_STATUS_RUNNING)) {
    pJob->status = STREAM_RECALC_STATUS_RUNNING;
  } else {
    code = TSDB_CODE_INVALID_MSG;
  }
  taosWUnLockLatch(&pTracker->lock);
  return code;
}

int32_t stRecalcTrackerFailJob(SStreamRecalcTracker *pTracker, int64_t recalcId, int32_t errorCode) {
  if (pTracker == NULL || errorCode == TSDB_CODE_SUCCESS) return TSDB_CODE_INVALID_PARA;

  int32_t code = TSDB_CODE_SUCCESS;
  taosWLockLatch(&pTracker->lock);
  SStreamRecalcJob *pJob = stGetJob(pTracker, recalcId);
  if (pJob == NULL) {
    code = TSDB_CODE_NOT_FOUND;
  } else if (pJob->status == STREAM_RECALC_STATUS_FAILED) {
    code = TSDB_CODE_SUCCESS;
  } else if (pJob->status != STREAM_RECALC_STATUS_PENDING) {
    code = TSDB_CODE_INVALID_MSG;
  } else {
    pJob->status = STREAM_RECALC_STATUS_FAILED;
    stRecordJobTerminal(pTracker, pJob);
  }
  taosWUnLockLatch(&pTracker->lock);
  return code;
}

int32_t stRecalcContributorsAdd(SStreamRecalcTracker *pTracker, SArray **ppContributors, int64_t recalcId,
                                SStreamProgressRange requestedRange) {
  if (pTracker == NULL || ppContributors == NULL ||
      (*ppContributors != NULL && (*ppContributors)->elemSize != sizeof(SStreamRecalcContributor))) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  taosRLockLatch(&pTracker->lock);
  SStreamRecalcJob *pJob = stGetJob(pTracker, recalcId);
  if (pJob == NULL || !stRangeEquals(pJob->requestedRange, requestedRange)) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }
  if (*ppContributors != NULL) {
    for (size_t i = 0; i < taosArrayGetSize(*ppContributors); ++i) {
      const SStreamRecalcContributor *pContributor = taosArrayGet(*ppContributors, i);
      if (pContributor->recalcId == recalcId && stRangeEquals(pContributor->requestedRange, requestedRange)) {
        code = pContributor->jobToken == pJob->generation ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_MSG;
        goto _exit;
      }
    }
  }

  bool created = false;
  if (*ppContributors == NULL) {
    *ppContributors = taosArrayInit(1, sizeof(SStreamRecalcContributor));
    if (*ppContributors == NULL) {
      code = terrno;
      goto _exit;
    }
    created = true;
  }
  SStreamRecalcContributor contributor = {
      .recalcId = recalcId, .jobToken = pJob->generation, .requestedRange = requestedRange};
  if (taosArrayPush(*ppContributors, &contributor) == NULL) {
    code = terrno;
    if (created) {
      taosArrayDestroy(*ppContributors);
      *ppContributors = NULL;
    }
  }

_exit:
  taosRUnLockLatch(&pTracker->lock);
  return code;
}

int32_t stRecalcContributorsMerge(SArray **ppDst, const SArray *pSrc) {
  if (ppDst == NULL || (*ppDst != NULL && (*ppDst)->elemSize != sizeof(SStreamRecalcContributor)) ||
      (pSrc != NULL && pSrc->elemSize != sizeof(SStreamRecalcContributor))) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pSrc == NULL) return TSDB_CODE_SUCCESS;
  for (size_t i = 0; i < taosArrayGetSize(pSrc); ++i) {
    const SStreamRecalcContributor *pContributor = taosArrayGet(pSrc, i);
    bool                            duplicate = false;
    if (*ppDst != NULL) {
      for (size_t j = 0; j < taosArrayGetSize(*ppDst); ++j) {
        const SStreamRecalcContributor *pExisting = taosArrayGet(*ppDst, j);
        if (pExisting->recalcId == pContributor->recalcId &&
            stRangeEquals(pExisting->requestedRange, pContributor->requestedRange)) {
          if (pExisting->jobToken != pContributor->jobToken) return TSDB_CODE_INVALID_MSG;
          duplicate = true;
          break;
        }
      }
    }
    if (duplicate) continue;
    if (*ppDst == NULL) {
      *ppDst = taosArrayInit(taosArrayGetSize(pSrc), sizeof(SStreamRecalcContributor));
      if (*ppDst == NULL) return terrno;
    }
    if (taosArrayPush(*ppDst, pContributor) == NULL) return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t stRecalcTrackerConfirmGroupPrefix(SStreamRecalcTracker *pTracker, int64_t gid, TSKEY confirmedThrough,
                                          const SArray *pContributors) {
  if (pTracker == NULL || (pContributors != NULL && pContributors->elemSize != sizeof(SStreamRecalcContributor))) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  taosWLockLatch(&pTracker->lock);
  for (size_t i = 0; i < taosArrayGetSize(pContributors); ++i) {
    const SStreamRecalcContributor *pContributor = taosArrayGet(pContributors, i);
    SStreamRecalcJob               *pJob = stGetJob(pTracker, pContributor->recalcId);
    if (pJob == NULL || !stRangeEquals(pJob->requestedRange, pContributor->requestedRange) ||
        pJob->generation != pContributor->jobToken || stGetGroupProgress(pJob, gid) == NULL) {
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }
  }

  for (size_t i = 0; i < taosArrayGetSize(pContributors); ++i) {
    const SStreamRecalcContributor *pContributor = taosArrayGet(pContributors, i);
    SStreamRecalcJob               *pJob = stGetJob(pTracker, pContributor->recalcId);
    stAdvanceJob(pTracker, pJob, gid, (SStreamProgressRange){pContributor->requestedRange.start, confirmedThrough},
                 pContributor->requestedRange);
  }

_exit:
  taosWUnLockLatch(&pTracker->lock);
  return code;
}

int32_t stRecalcTrackerBeginStep(SStreamRecalcTracker *pTracker, int64_t gid, SStreamProgressRange stepRange,
                                 const SArray *pContributors, uint64_t *pStepId) {
  if (pTracker == NULL || pStepId == NULL ||
      (pContributors != NULL && pContributors->elemSize != sizeof(SStreamRecalcContributor))) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  taosWLockLatch(&pTracker->lock);
  if (pTracker->nextStepId == UINT64_MAX) {
    code = TSDB_CODE_OUT_OF_RANGE;
    goto _exit;
  }
  if (pContributors != NULL) {
    for (size_t i = 0; i < taosArrayGetSize(pContributors); ++i) {
      const SStreamRecalcContributor *pContributor = taosArrayGet(pContributors, i);
      SStreamRecalcJob               *pJob = stGetJob(pTracker, pContributor->recalcId);
      if (pJob == NULL || !stRangeEquals(pJob->requestedRange, pContributor->requestedRange) ||
          pJob->generation != pContributor->jobToken || stGetGroupProgress(pJob, gid) == NULL) {
        code = TSDB_CODE_INVALID_MSG;
        goto _exit;
      }
    }
  }

  SStreamRecalcStep *pStep = taosMemoryCalloc(1, sizeof(*pStep));
  if (pStep == NULL) {
    code = terrno;
    goto _exit;
  }
  pStep->stepId = pTracker->nextStepId;
  pStep->gid = gid;
  pStep->stepRange = stepRange;
  pStep->pContributors =
      pContributors == NULL ? taosArrayInit(0, sizeof(SStreamRecalcContributor)) : taosArrayDup(pContributors, NULL);
  pStep->pReaderTokens = taosArrayInit(0, sizeof(uint64_t));
  pStep->pCompletedReaderTokens = taosArrayInit(0, sizeof(uint64_t));
  pStep->pRunnerTokens = taosArrayInit(0, sizeof(uint64_t));
  pStep->pCompletedRunnerTokens = taosArrayInit(0, sizeof(uint64_t));
  if (pStep->pContributors == NULL || pStep->pReaderTokens == NULL || pStep->pCompletedReaderTokens == NULL ||
      pStep->pRunnerTokens == NULL || pStep->pCompletedRunnerTokens == NULL) {
    code = terrno;
    stDestroyStep(pStep);
    goto _exit;
  }
  code = taosHashPut(pTracker->pSteps, &pStep->stepId, sizeof(pStep->stepId), &pStep, sizeof(pStep));
  if (code != TSDB_CODE_SUCCESS) {
    stDestroyStep(pStep);
    goto _exit;
  }
  *pStepId = pTracker->nextStepId++;
  for (size_t i = 0; i < taosArrayGetSize(pStep->pContributors); ++i) {
    const SStreamRecalcContributor *pContributor = taosArrayGet(pStep->pContributors, i);
    SStreamRecalcJob               *pJob = stGetJob(pTracker, pContributor->recalcId);
    if (pJob != NULL && stRecalcStatusCanTransition(pJob->status, STREAM_RECALC_STATUS_RUNNING)) {
      pJob->status = STREAM_RECALC_STATUS_RUNNING;
    }
  }

_exit:
  taosWUnLockLatch(&pTracker->lock);
  return code;
}

int32_t stRecalcTrackerAddReader(SStreamRecalcTracker *pTracker, uint64_t stepId, uint64_t requestToken) {
  if (pTracker == NULL || requestToken == 0) return TSDB_CODE_INVALID_PARA;
  int32_t code = TSDB_CODE_SUCCESS;
  taosWLockLatch(&pTracker->lock);
  SStreamRecalcStep *pStep = stGetStep(pTracker, stepId);
  if (pStep == NULL) {
    code = TSDB_CODE_NOT_FOUND;
  } else if (pStep->triggerDone || pStep->committed || pStep->failed) {
    code = TSDB_CODE_INVALID_MSG;
  } else if (stTokenExists(pStep->pReaderTokens, requestToken)) {
    code = TSDB_CODE_SUCCESS;
  } else if (taosArrayPush(pStep->pReaderTokens, &requestToken) == NULL) {
    code = terrno;
  }
  taosWUnLockLatch(&pTracker->lock);
  return code;
}

int32_t stRecalcTrackerCompleteReader(SStreamRecalcTracker *pTracker, uint64_t stepId, uint64_t requestToken) {
  if (pTracker == NULL || requestToken == 0) return TSDB_CODE_INVALID_PARA;
  int32_t code = TSDB_CODE_SUCCESS;
  taosWLockLatch(&pTracker->lock);
  SStreamRecalcStep *pStep = stGetStep(pTracker, stepId);
  if (pStep == NULL) {
    code = stMissingStepCode(pTracker, stepId);
  } else if (!stTokenExists(pStep->pReaderTokens, requestToken)) {
    code = TSDB_CODE_INVALID_MSG;
  } else if (stTokenExists(pStep->pCompletedReaderTokens, requestToken)) {
    code = TSDB_CODE_SUCCESS;
  } else if (taosArrayPush(pStep->pCompletedReaderTokens, &requestToken) == NULL) {
    code = terrno;
  } else {
    stCommitStep(pTracker, pStep);
  }
  taosWUnLockLatch(&pTracker->lock);
  return code;
}

int32_t stRecalcTrackerSetTriggerDone(SStreamRecalcTracker *pTracker, uint64_t stepId, int32_t pendingCalcParamCount) {
  if (pTracker == NULL || pendingCalcParamCount < 0) return TSDB_CODE_INVALID_PARA;
  int32_t code = TSDB_CODE_SUCCESS;
  taosWLockLatch(&pTracker->lock);
  SStreamRecalcStep *pStep = stGetStep(pTracker, stepId);
  if (pStep == NULL) {
    code = stMissingStepCode(pTracker, stepId);
  } else if (pStep->triggerDone) {
    code = pStep->declaredPendingCalcParamCount == pendingCalcParamCount ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_MSG;
  } else if (pStep->failed) {
    code = TSDB_CODE_INVALID_MSG;
  } else {
    pStep->triggerDone = true;
    pStep->declaredPendingCalcParamCount = pendingCalcParamCount;
    pStep->pendingCalcParamCount = pendingCalcParamCount;
    stCommitStep(pTracker, pStep);
  }
  taosWUnLockLatch(&pTracker->lock);
  return code;
}

int32_t stRecalcTrackerAddRunner(SStreamRecalcTracker *pTracker, uint64_t stepId, uint64_t requestToken) {
  if (pTracker == NULL || requestToken == 0) return TSDB_CODE_INVALID_PARA;
  int32_t code = TSDB_CODE_SUCCESS;
  taosWLockLatch(&pTracker->lock);
  SStreamRecalcStep *pStep = stGetStep(pTracker, stepId);
  if (pStep == NULL) {
    code = stMissingStepCode(pTracker, stepId);
  } else if (stTokenExists(pStep->pRunnerTokens, requestToken)) {
    code = TSDB_CODE_SUCCESS;
  } else if (pStep->committed || pStep->failed) {
    code = TSDB_CODE_INVALID_MSG;
  } else if (pStep->triggerDone && pStep->pendingCalcParamCount == 0) {
    code = TSDB_CODE_INVALID_MSG;
  } else if (taosArrayPush(pStep->pRunnerTokens, &requestToken) == NULL) {
    code = terrno;
  } else {
    if (pStep->triggerDone && pStep->pendingCalcParamCount > 0) --pStep->pendingCalcParamCount;
    stCommitStep(pTracker, pStep);
  }
  taosWUnLockLatch(&pTracker->lock);
  return code;
}

int32_t stRecalcTrackerCompleteRunner(SStreamRecalcTracker *pTracker, uint64_t stepId, uint64_t requestToken) {
  if (pTracker == NULL || requestToken == 0) return TSDB_CODE_INVALID_PARA;
  int32_t code = TSDB_CODE_SUCCESS;
  taosWLockLatch(&pTracker->lock);
  SStreamRecalcStep *pStep = stGetStep(pTracker, stepId);
  if (pStep == NULL) {
    code = stMissingStepCode(pTracker, stepId);
  } else if (!stTokenExists(pStep->pRunnerTokens, requestToken)) {
    code = TSDB_CODE_INVALID_MSG;
  } else if (stTokenExists(pStep->pCompletedRunnerTokens, requestToken)) {
    code = TSDB_CODE_SUCCESS;
  } else if (taosArrayPush(pStep->pCompletedRunnerTokens, &requestToken) == NULL) {
    code = terrno;
  } else {
    stCommitStep(pTracker, pStep);
  }
  taosWUnLockLatch(&pTracker->lock);
  return code;
}

int32_t stRecalcTrackerFailStep(SStreamRecalcTracker *pTracker, uint64_t stepId, int32_t errorCode) {
  if (pTracker == NULL || errorCode == TSDB_CODE_SUCCESS) return TSDB_CODE_INVALID_PARA;
  int32_t code = TSDB_CODE_SUCCESS;
  taosWLockLatch(&pTracker->lock);
  SStreamRecalcStep *pStep = stGetStep(pTracker, stepId);
  if (pStep == NULL) {
    code = stMissingStepCode(pTracker, stepId);
  } else if (errorCode == TSDB_CODE_NEED_RETRY || pStep->failed || pStep->committed) {
    code = TSDB_CODE_SUCCESS;
  } else {
    pStep->failed = true;
    for (size_t i = 0; i < taosArrayGetSize(pStep->pContributors); ++i) {
      const SStreamRecalcContributor *pContributor = taosArrayGet(pStep->pContributors, i);
      SStreamRecalcJob               *pJob = stGetJob(pTracker, pContributor->recalcId);
      if (pJob != NULL && pJob->generation == pContributor->jobToken &&
          stRecalcStatusCanTransition(pJob->status, STREAM_RECALC_STATUS_FAILED)) {
        pJob->status = STREAM_RECALC_STATUS_FAILED;
        stRecordJobTerminal(pTracker, pJob);
      }
    }
    stRetireStep(pTracker, pStep);
  }
  taosWUnLockLatch(&pTracker->lock);
  return code;
}

static TSKEY stClampThrough(SStreamProgressRange range, TSKEY through) {
  if (through < range.start) return range.start;
  if (through > range.end) return range.end;
  return through;
}

static void stRefreshHistoryProgress(SStreamRecalcTracker *pTracker) {
  if (!pTracker->historyEnabled) {
    pTracker->historyProgressPct = 0;
    return;
  }
  const bool finished =
      pTracker->historyRange.end <= pTracker->historyRange.start ||
      (pTracker->historyCommittedThrough >= pTracker->historyRange.end && pTracker->historyTerminalBarrierDone);
  SStreamWideUInt total = {0};
  SStreamWideUInt completed = {0};
  stWideAdd(&total, stRangeLength(pTracker->historyRange));
  stWideAdd(&completed,
            stRangeLength((SStreamProgressRange){pTracker->historyRange.start, pTracker->historyCommittedThrough}));
  int32_t progressPct = stProgressPercent(completed, total, finished);
  if (progressPct > pTracker->historyProgressPct) pTracker->historyProgressPct = progressPct;
}

int32_t stRecalcTrackerInitHistory(SStreamRecalcTracker *pTracker, bool enabled, SStreamProgressRange originalRange,
                                   bool checkpointFinished) {
  if (pTracker == NULL) return TSDB_CODE_INVALID_PARA;
  int32_t code = TSDB_CODE_SUCCESS;
  taosWLockLatch(&pTracker->lock);
  if (pTracker->historyInitialized) {
    if (pTracker->historyEnabled != enabled || !stRangeEquals(pTracker->historyRange, originalRange) ||
        pTracker->historyCheckpointFinished != checkpointFinished) {
      code = TSDB_CODE_INVALID_MSG;
    }
    taosWUnLockLatch(&pTracker->lock);
    return code;
  }
  pTracker->historyInitialized = true;
  pTracker->historyEnabled = enabled;
  pTracker->historyCheckpointFinished = checkpointFinished;
  pTracker->historyRange = originalRange;
  pTracker->historyConfirmedThrough = originalRange.start;
  pTracker->historyCommittedThrough = originalRange.start;
  pTracker->historyTerminalBarrierDone = false;
  pTracker->historyProgressPct = 0;
  if (enabled && (originalRange.end <= originalRange.start || checkpointFinished)) {
    pTracker->historyConfirmedThrough = originalRange.end;
    pTracker->historyCommittedThrough = originalRange.end;
    pTracker->historyTerminalBarrierDone = true;
  }
  stRefreshHistoryProgress(pTracker);
  taosWUnLockLatch(&pTracker->lock);
  return code;
}

int32_t stRecalcTrackerConfirmHistoryPrefix(SStreamRecalcTracker *pTracker, TSKEY confirmedThrough) {
  if (pTracker == NULL) return TSDB_CODE_INVALID_PARA;
  int32_t code = TSDB_CODE_SUCCESS;
  taosWLockLatch(&pTracker->lock);
  if (!pTracker->historyEnabled) {
    code = TSDB_CODE_INVALID_MSG;
  } else {
    TSKEY next = stClampThrough(pTracker->historyRange, confirmedThrough);
    if (next > pTracker->historyConfirmedThrough) pTracker->historyConfirmedThrough = next;
    if (next > pTracker->historyCommittedThrough) pTracker->historyCommittedThrough = next;
    stRefreshHistoryProgress(pTracker);
  }
  taosWUnLockLatch(&pTracker->lock);
  return code;
}

int32_t stRecalcTrackerCommitHistoryThrough(SStreamRecalcTracker *pTracker, TSKEY committedThrough,
                                            bool terminalBarrierDone) {
  if (pTracker == NULL) return TSDB_CODE_INVALID_PARA;
  int32_t code = TSDB_CODE_SUCCESS;
  taosWLockLatch(&pTracker->lock);
  if (!pTracker->historyEnabled) {
    code = TSDB_CODE_INVALID_MSG;
  } else {
    TSKEY next = stClampThrough(pTracker->historyRange, committedThrough);
    if (next > pTracker->historyCommittedThrough) pTracker->historyCommittedThrough = next;
    pTracker->historyTerminalBarrierDone = pTracker->historyTerminalBarrierDone || terminalBarrierDone;
    stRefreshHistoryProgress(pTracker);
  }
  taosWUnLockLatch(&pTracker->lock);
  return code;
}

int32_t stRecalcTrackerCopySnapshot(SStreamRecalcTracker *pTracker, bool *pHistoryValid, int32_t *pHistoryProgressPct,
                                    SArray **ppRecalculates) {
  if (pTracker == NULL || pHistoryValid == NULL || pHistoryProgressPct == NULL || ppRecalculates == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SArray *pSnapshots = NULL;
  taosRLockLatch(&pTracker->lock);
  pSnapshots = taosArrayInit(taosHashGetSize(pTracker->pJobs), sizeof(SStreamRecalcSnapshot));
  if (pSnapshots == NULL) {
    code = terrno;
    goto _exit;
  }
  SStreamRecalcJob **ppJob = taosHashIterate(pTracker->pJobs, NULL);
  while (ppJob != NULL) {
    const SStreamRecalcJob *pJob = *ppJob;
    SStreamRecalcSnapshot   snapshot = {
          .recalcId = pJob->recalcId,
          .start = pJob->requestedRange.start,
          .end = pJob->requestedRange.end,
          .progressPct = pJob->progressPct,
          .status = pJob->status,
    };
    if (taosArrayPush(pSnapshots, &snapshot) == NULL) {
      code = terrno;
      taosHashCancelIterate(pTracker->pJobs, ppJob);
      goto _exit;
    }
    ppJob = taosHashIterate(pTracker->pJobs, ppJob);
  }

  *pHistoryValid = pTracker->historyEnabled;
  *pHistoryProgressPct = pTracker->historyProgressPct;
  *ppRecalculates = pSnapshots;
  pSnapshots = NULL;

_exit:
  taosRUnLockLatch(&pTracker->lock);
  taosArrayDestroy(pSnapshots);
  return code;
}

static SStreamRecalcDebugSnapshot stBuildDebugSnapshot(const SStreamRecalcJob *pJob) {
  return (SStreamRecalcDebugSnapshot){
      .snapshot =
          {
              .recalcId = pJob->recalcId,
              .start = pJob->requestedRange.start,
              .end = pJob->requestedRange.end,
              .progressPct = pJob->progressPct,
              .status = pJob->status,
          },
      .fixedGroupCount = (int32_t)taosArrayGetSize(pJob->pGroups),
      .terminalAtMs = pJob->terminalAtMs,
  };
}

int32_t stRecalcTrackerCopyDebugJobs(SStreamRecalcTracker *pTracker, SArray **ppJobs) {
  if (pTracker == NULL || ppJobs == NULL) return TSDB_CODE_INVALID_PARA;
  *ppJobs = NULL;

  int32_t code = TSDB_CODE_SUCCESS;
  SArray *pJobs = NULL;
  taosRLockLatch(&pTracker->lock);
  pJobs = taosArrayInit(pTracker->activeJobCount, sizeof(SStreamRecalcDebugSnapshot));
  if (pJobs == NULL) {
    code = terrno;
    goto _exit;
  }
  SStreamRecalcJob **ppJob = taosHashIterate(pTracker->pJobs, NULL);
  while (ppJob != NULL) {
    const SStreamRecalcJob *pJob = *ppJob;
    if (pJob->status == STREAM_RECALC_STATUS_PENDING || pJob->status == STREAM_RECALC_STATUS_RUNNING) {
      SStreamRecalcDebugSnapshot snapshot = stBuildDebugSnapshot(pJob);
      if (taosArrayPush(pJobs, &snapshot) == NULL) {
        code = terrno;
        taosHashCancelIterate(pTracker->pJobs, ppJob);
        goto _exit;
      }
    }
    ppJob = taosHashIterate(pTracker->pJobs, ppJob);
  }
  *ppJobs = pJobs;
  pJobs = NULL;

_exit:
  taosRUnLockLatch(&pTracker->lock);
  taosArrayDestroy(pJobs);
  return code;
}

int32_t stRecalcTrackerTakeTerminalEvents(SStreamRecalcTracker *pTracker, SArray **ppTerminals) {
  if (pTracker == NULL || ppTerminals == NULL) return TSDB_CODE_INVALID_PARA;
  *ppTerminals = NULL;

  int32_t code = TSDB_CODE_SUCCESS;
  SArray *pTerminals = NULL;
  taosWLockLatch(&pTracker->lock);
  pTerminals = taosArrayInit(pTracker->terminalJobCount, sizeof(SStreamRecalcDebugSnapshot));
  if (pTerminals == NULL) {
    code = terrno;
    goto _exit;
  }
  for (uint32_t i = 0; i < pTracker->terminalJobCount; ++i) {
    uint32_t          index = (pTracker->terminalJobStart + i) % STREAM_RECALC_MAX_TERMINAL_JOBS;
    SStreamRecalcJob *pJob = stGetJob(pTracker, pTracker->terminalJobIds[index]);
    if (pJob == NULL || pJob->terminalDebugEmitted) continue;
    SStreamRecalcDebugSnapshot snapshot = stBuildDebugSnapshot(pJob);
    if (taosArrayPush(pTerminals, &snapshot) == NULL) {
      code = terrno;
      goto _exit;
    }
  }
  for (int32_t i = 0; i < TARRAY_SIZE(pTerminals); ++i) {
    const SStreamRecalcDebugSnapshot *pSnapshot = TARRAY_GET_ELEM(pTerminals, i);
    SStreamRecalcJob                 *pJob = stGetJob(pTracker, pSnapshot->snapshot.recalcId);
    if (pJob != NULL) pJob->terminalDebugEmitted = true;
  }
  *ppTerminals = pTerminals;
  pTerminals = NULL;

_exit:
  taosWUnLockLatch(&pTracker->lock);
  taosArrayDestroy(pTerminals);
  return code;
}

int32_t stRecalcTrackerGetDebugGauges(SStreamRecalcTracker *pTracker, int64_t *pActiveJobCount, bool *pHistoryValid,
                                      int32_t *pHistoryProgressPct) {
  if (pTracker == NULL || pActiveJobCount == NULL || pHistoryValid == NULL || pHistoryProgressPct == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  taosRLockLatch(&pTracker->lock);
  *pActiveJobCount = pTracker->activeJobCount;
  *pHistoryValid = pTracker->historyEnabled;
  *pHistoryProgressPct = pTracker->historyProgressPct;
  taosRUnLockLatch(&pTracker->lock);
  return TSDB_CODE_SUCCESS;
}
