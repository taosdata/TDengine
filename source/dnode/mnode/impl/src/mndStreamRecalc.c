/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "mndStreamRecalc.h"

#include "mndTrans.h"
#include "tdTestSeam.h"
#include "tlogKeyOp.h"

enum { MND_STREAM_RECALC_PULLUP_BATCH_SIZE = 8 };

typedef enum EStreamRecalcPersistOpType {
  MND_STREAM_RECALC_OP_ACCEPT = 1,
  MND_STREAM_RECALC_OP_FINISH = 2,
} EStreamRecalcPersistOpType;

typedef struct SStreamRecalcPersistOp {
  EStreamRecalcPersistOpType type;
  int64_t                    streamId;
  int64_t                    recalcId;
  uint64_t                   targetRevision;
  SStreamRecalcPersistReq    request;
  SStreamRecalcSnapshot      candidateSnapshot;
  int32_t                    candidateRetryOrdinal;
  int32_t                    candidateErrorCode;
  char                       candidateErrorText[MND_STREAM_RECALC_MESSAGE_LEN];
  SRpcHandleInfo             rpcInfo;
  int32_t                    completionCode;
  bool                       hasRpc;
  bool                       rpcTransferred;
  bool                       sendResponseOnCompletion;
} SStreamRecalcPersistOp;

typedef struct SStreamRecalcTransParam {
  int64_t  streamId;
  int64_t  recalcId;
  uint64_t targetRevision;
  int8_t   opType;
} SStreamRecalcTransParam;

typedef struct SStreamRecalcPendingTrans {
  SStreamObj             *pStream;
  SArray                 *pRequests;
  SStreamRecalcPersistOp   op;
  int64_t                 runtimeGeneration;
} SStreamRecalcPendingTrans;

typedef struct SStreamRecalcPostActions {
  SList pendingTrans;
  SList completedOps;
  bool  scheduleNext;
  bool  markPullupPending;
} SStreamRecalcPostActions;

static int32_t mndStreamRecalcStartClaimed(SMnode *pMnode, SStmStatus *pStatus, bool synchronousAccept,
                                           bool runtimeLocked, SStreamRecalcPostActions *pPostActions);
static int32_t mndStreamRecalcFinishImpl(SMnode *pMnode, int64_t streamId,
                                         const SStreamRecalcTerminalCandidate *pCandidate, bool *pStartDeferred);
static void    mndStreamRecalcPreparePending(SMnode *pMnode, SStreamRecalcPostActions *pPostActions);
static bool    mndStreamRecalcTerminal(EStreamRecalcStatus status);
static void    mndStreamRecalcSendResponse(const SRpcHandleInfo *pRpcInfo, int32_t code);
static bool    mndStreamRecalcCheckCommitted(SMnode *pMnode, const SStreamRecalcTransParam *pCallbackParam);
static void    mndStreamRecalcPublishCommittedLocked(SStmStatus *pStatus, const SStreamRecalcTransParam *pCallbackParam,
                                                     bool queueOwnsResponse, bool committed,
                                                     SStreamRecalcPostActions *pPostActions);
static void    mndStreamRecalcPublishCommitted(SMnode *pMnode, const SStreamRecalcTransParam *pCallbackParam,
                                               bool queueOwnsResponse, SStreamRecalcPostActions *pPostActions);

static int32_t mndStreamRecalcErrorCode(int32_t code) {
  if (code != TSDB_CODE_SUCCESS) return code;
  return terrno != TSDB_CODE_SUCCESS ? terrno : TSDB_CODE_INTERNAL_ERROR;
}

static SListNode *mndStreamRecalcAllocOpNode(const SStreamRecalcPersistOp *pOp) {
  SListNode *pNode = taosMemoryCalloc(1, sizeof(SListNode) + sizeof(*pOp));
  if (pNode == NULL) return NULL;
  memcpy(pNode->data, pOp, sizeof(*pOp));
  return pNode;
}

static SStreamRecalcPersistOp *mndStreamRecalcHeadOp(SStmStatus *pStatus) {
  SListNode *pNode = pStatus->recalcPersistOpsInitialized ? tdListGetHead(&pStatus->recalcPersistOps) : NULL;
  return pNode == NULL ? NULL : (SStreamRecalcPersistOp *)pNode->data;
}

void mndStreamRecalcInitStatus(SStmStatus *pStatus) {
  if (pStatus == NULL || pStatus->recalcPersistOpsInitialized) return;
  tdListInit(&pStatus->recalcPersistOps, sizeof(SStreamRecalcPersistOp));
  pStatus->recalcPersistOpsInitialized = true;
}

static int32_t mndStreamRecalcFindRecord(const SArray *pRecords, int64_t recalcId) {
  for (int32_t i = 0; i < taosArrayGetSize(pRecords); ++i) {
    const SStmRecalcRecord *pRecord = taosArrayGet(pRecords, i);
    if (pRecord != NULL && pRecord->snapshot.recalcId == recalcId) return i;
  }
  return -1;
}

static int32_t mndStreamRecalcFindRequest(const SArray *pRequests, int64_t recalcId) {
  for (int32_t i = 0; i < taosArrayGetSize(pRequests); ++i) {
    const SStreamRecalcPersistReq *pRequest = taosArrayGet(pRequests, i);
    if (pRequest != NULL && pRequest->recalcId == recalcId) return i;
  }
  return -1;
}

static SHashObj *mndStreamRecalcCreateIndex(int32_t size) {
  return taosHashInit(TMAX(size, 4), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
}

static int32_t mndStreamRecalcPutIndex(SHashObj *pIndex, int64_t recalcId, int32_t arrayIndex) {
  if (taosHashGet(pIndex, &recalcId, sizeof(recalcId)) != NULL) return TSDB_CODE_INVALID_MSG;
  return taosHashPut(pIndex, &recalcId, sizeof(recalcId), &arrayIndex, sizeof(arrayIndex));
}

static int32_t mndStreamRecalcGetIndex(SHashObj *pIndex, int64_t recalcId) {
  if (pIndex == NULL) return -1;
  const int32_t *pArrayIndex = taosHashGet(pIndex, &recalcId, sizeof(recalcId));
  return pArrayIndex == NULL ? -1 : *pArrayIndex;
}

static bool mndStreamRecalcPersisted(const SStreamObj *pStream, int64_t recalcId) {
  return mndStreamRecalcFindRequest(pStream->pIncompleteRecalcs, recalcId) >= 0;
}

static void mndStreamRecalcMarkPullupPending(void) { atomic_store_8(&mStreamMgmt.recalcPullupPending, 1); }

static int32_t mndStreamRecalcPostTimer(SMnode *pMnode) {
  int32_t    code = TSDB_CODE_SUCCESS;
  SMTimerReq timerReq = {0};
  int32_t    contLen = tSerializeSMTimerMsg(NULL, 0, &timerReq);
  if (contLen <= 0) return mndStreamRecalcErrorCode(contLen);

  void *pCont = rpcMallocCont(contLen);
  if (pCont == NULL) return mndStreamRecalcErrorCode(terrno);
  if (tSerializeSMTimerMsg(pCont, contLen, &timerReq) < 0) {
    code = mndStreamRecalcErrorCode(terrno);
    rpcFreeCont(pCont);
    return code;
  }

  SRpcMsg msg = {.msgType = TDMT_MND_TRANS_TIMER, .pCont = pCont, .contLen = contLen};
  msg.info.node = pMnode;
  return tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &msg);
}

void mndStreamRecalcSchedulePullupPostUnlock(SMnode *pMnode) {
  if (pMnode == NULL) return;
  mndStreamRecalcMarkPullupPending();
  int32_t code = mndStreamRecalcPostTimer(pMnode);
  if (code != TSDB_CODE_SUCCESS) {
    mWarn("failed to schedule stream recalculation persistence pullup since %s", tstrerror(code));
  }
}

static void mndStreamRecalcInitPostActions(SStreamRecalcPostActions *pPostActions) {
  tdListInit(&pPostActions->pendingTrans, sizeof(SStreamRecalcPendingTrans));
  tdListInit(&pPostActions->completedOps, sizeof(SStreamRecalcPersistOp));
  pPostActions->scheduleNext = false;
  pPostActions->markPullupPending = false;
}

static void mndStreamRecalcQueueCompletedAction(SStreamRecalcPostActions *pPostActions, SListNode *pCompleted,
                                                bool sendResponse, int32_t code) {
  if (pCompleted == NULL) return;
  SStreamRecalcPersistOp *pOp = (SStreamRecalcPersistOp *)pCompleted->data;
  pOp->completionCode = code;
  pOp->sendResponseOnCompletion = sendResponse;
  tdListAppendNode(&pPostActions->completedOps, pCompleted);
}

static void mndStreamRecalcRunPostActions(SMnode *pMnode, SStreamRecalcPostActions *pPostActions) {
  mndStreamRecalcPreparePending(pMnode, pPostActions);
  SListNode *pCompleted = NULL;
  while ((pCompleted = tdListPopHead(&pPostActions->completedOps)) != NULL) {
    const SStreamRecalcPersistOp *pOp = (const SStreamRecalcPersistOp *)pCompleted->data;
    SRpcHandleInfo                rpcInfo = pOp->rpcInfo;
    int32_t                       code = pOp->completionCode;
    bool                          sendResponse = pOp->sendResponseOnCompletion;
    listNodeFree(pCompleted);
    if (sendResponse) mndStreamRecalcSendResponse(&rpcInfo, code);
  }

  if (pPostActions->scheduleNext) {
    mndStreamRecalcSchedulePullupPostUnlock(pMnode);
  } else if (pPostActions->markPullupPending) {
    mndStreamRecalcMarkPullupPending();
  }
}

static int32_t mndStreamRecalcSnapshotWithRevision(const SStreamObj *pStream, SArray **ppRequests,
                                                   uint64_t *pRevision) {
  if (pStream == NULL || ppRequests == NULL || pRevision == NULL) return TSDB_CODE_INVALID_PARA;
  *ppRequests = NULL;
  *pRevision = 0;

  taosRLockLatch((SRWLatch *)&pStream->lock);
  SArray *pCopy = pStream->pIncompleteRecalcs == NULL ? taosArrayInit(0, sizeof(SStreamRecalcPersistReq))
                                                      : taosArrayDup(pStream->pIncompleteRecalcs, NULL);
  if (pCopy != NULL) *pRevision = pStream->recalcRevision;
  taosRUnLockLatch((SRWLatch *)&pStream->lock);
  if (pCopy == NULL) return mndStreamRecalcErrorCode(terrno);

  *ppRequests = pCopy;
  return TSDB_CODE_SUCCESS;
}

int32_t mndStreamRecalcSnapshot(const SStreamObj *pStream, SArray **ppRequests) {
  uint64_t revision = 0;
  return mndStreamRecalcSnapshotWithRevision(pStream, ppRequests, &revision);
}

static bool mndStreamRecalcRuntimeActive(const SStmRecalcRecord *pRecord) {
  return pRecord->snapshot.status == STREAM_RECALC_STATUS_PENDING ||
         pRecord->snapshot.status == STREAM_RECALC_STATUS_RUNNING;
}

static bool mndStreamRecalcSamePersistedTuple(const SStmRecalcRecord        *pRecord,
                                              const SStreamRecalcPersistReq *pRequest) {
  return pRecord->snapshot.recalcId == pRequest->recalcId && pRecord->snapshot.start == pRequest->start &&
         pRecord->snapshot.end == pRequest->end && pRecord->requestTimeMs == pRequest->requestTimeMs;
}

static SStmRecalcRecord mndStreamRecalcRestoredRecord(const SStreamRecalcPersistReq *pRequest, int64_t triggerTaskId,
                                                      int64_t triggerSeriousId) {
  SStmRecalcRecord record = {
      .snapshot =
          {
              .recalcId = pRequest->recalcId,
              .start = pRequest->start,
              .end = pRequest->end,
              .progressPct = 0,
              .status = STREAM_RECALC_STATUS_PENDING,
          },
      .requestTimeMs = pRequest->requestTimeMs,
      .triggerTaskId = triggerTaskId,
      .triggerSeriousId = triggerSeriousId,
      .typedStatusKnown = true,
      .visible = true,
  };
  return record;
}

int32_t mndStreamRecalcCaptureRecords(SStmStatus *status, SStreamRecalcRestore *restore) {
  taosRLockLatch(&status->userRecalcLock);
  restore->recordsVersion = status->recalcRestoreVersion;
  bool hasRecords = taosArrayGetSize(status->recalcRecords) > 0;
  restore->records = hasRecords ? taosArrayDup(status->recalcRecords, NULL) : NULL;
  taosRUnLockLatch(&status->userRecalcLock);
  return hasRecords && !restore->records ? mndStreamRecalcErrorCode(terrno) : TSDB_CODE_SUCCESS;
}

int32_t mndStreamRecalcPrepareRestore(const SStreamObj *pStream, SStreamRecalcRestore *restore, int64_t triggerTaskId,
                                      int64_t triggerSeriousId) {
  if (pStream == NULL || restore == NULL || triggerTaskId == 0 || triggerSeriousId == 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t   code = TSDB_CODE_SUCCESS;
  SArray   *pRequests = NULL;
  SArray   *pRecords = NULL;
  SHashObj *pRequestIndex = NULL;
  SHashObj *pRuntimeIndex = NULL;
  TAOS_CHECK_GOTO(mndStreamRecalcSnapshotWithRevision(pStream, &pRequests, &restore->revision), NULL, _exit);

#ifdef BUILD_TEST_SEAM
  if (gTdSeam_msmDeployPhase) gTdSeam_msmDeployPhase("recalc-restore", pStream->name);
#endif
  int32_t capacity = taosArrayGetSize(pRequests);
  pRecords = taosArrayInit(capacity, sizeof(SStmRecalcRecord));
  if (pRecords == NULL) {
    code = mndStreamRecalcErrorCode(terrno);
    goto _exit;
  }

  pRequestIndex = mndStreamRecalcCreateIndex(capacity);
  if (pRequestIndex == NULL) {
    code = mndStreamRecalcErrorCode(terrno);
    goto _exit;
  }
  for (int32_t i = 0; i < capacity; ++i) {
    const SStreamRecalcPersistReq *pRequest = taosArrayGet(pRequests, i);
    if (pRequest == NULL || pRequest->recalcId == 0 || pRequest->start >= pRequest->end ||
        pRequest->requestTimeMs <= 0) {
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }
    code = mndStreamRecalcPutIndex(pRequestIndex, pRequest->recalcId, i);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
  }

  int32_t runtimeCount = taosArrayGetSize(restore->records);
  pRuntimeIndex = mndStreamRecalcCreateIndex(runtimeCount);
  if (pRuntimeIndex == NULL) {
    code = mndStreamRecalcErrorCode(terrno);
    goto _exit;
  }
  for (int32_t i = 0; i < runtimeCount; ++i) {
    const SStmRecalcRecord *pRecord = taosArrayGet(restore->records, i);
    if (pRecord == NULL || pRecord->snapshot.recalcId == 0) {
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }
    code = mndStreamRecalcPutIndex(pRuntimeIndex, pRecord->snapshot.recalcId, i);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pRequests); ++i) {
    const SStreamRecalcPersistReq *pRequest = taosArrayGet(pRequests, i);
    int32_t                        index = mndStreamRecalcGetIndex(pRuntimeIndex, pRequest->recalcId);
    const SStmRecalcRecord        *pExisting = index < 0 ? NULL : taosArrayGet(restore->records, index);
    SStmRecalcRecord               restored = mndStreamRecalcRestoredRecord(pRequest, triggerTaskId, triggerSeriousId);
    // New triggers restart active work. The coordinator still owns uncommitted ACCEPT and FINISH records.
    bool pending =
        pExisting && (pExisting->hidden || pExisting->terminalCandidateValid || pExisting->terminalPersisting);
    const SStmRecalcRecord *pRecord =
        pending && mndStreamRecalcSamePersistedTuple(pExisting, pRequest) ? pExisting : &restored;
    if (taosArrayPush(pRecords, pRecord) == NULL) {
      code = mndStreamRecalcErrorCode(terrno);
      goto _exit;
    }
  }

  for (int32_t i = 0; i < taosArrayGetSize(restore->records); ++i) {
    const SStmRecalcRecord *pRecord = taosArrayGet(restore->records, i);
    if (pRecord == NULL || mndStreamRecalcGetIndex(pRequestIndex, pRecord->snapshot.recalcId) >= 0) continue;
    if (pRecord->requestTimeMs <= 0) continue;
    if (!pRecord->hidden && !pRecord->terminalCandidateValid && !pRecord->terminalPersisting &&
        mndStreamRecalcRuntimeActive(pRecord)) {
      continue;
    }
    if (taosArrayPush(pRecords, pRecord) == NULL) {
      code = mndStreamRecalcErrorCode(terrno);
      goto _exit;
    }
  }

  TSWAP(restore->records, pRecords);

_exit:
  taosHashCleanup(pRuntimeIndex);
  taosHashCleanup(pRequestIndex);
  taosArrayDestroy(pRecords);
  taosArrayDestroy(pRequests);
  return code;
}

bool mndStreamRecalcCommitRestore(const SStreamObj *stream, SStmStatus *status, SStreamRecalcRestore *restore) {
  if (taosRTryLockLatch((SRWLatch *)&stream->lock) != 0) return false;
  if (taosWTryLockLatch(&status->userRecalcLock) != 0) {
    taosRUnLockLatch((SRWLatch *)&stream->lock);
    return false;
  }
  bool current = stream->recalcRevision == restore->revision && status->recalcRestoreVersion == restore->recordsVersion;
  if (current) {
    TSWAP(status->recalcRecords, restore->records);
    ++status->recalcRestoreVersion;
  }
  taosWUnLockLatch(&status->userRecalcLock);
  taosRUnLockLatch((SRWLatch *)&stream->lock);
  return current;
}

static bool mndStreamRecalcDispatchable(const SStmRecalcRecord *pRecord) {
  return pRecord->visible && !pRecord->hidden && !pRecord->terminalCandidateValid &&
         mndStreamRecalcRuntimeActive(pRecord) && !pRecord->dispatchConfirmed;
}

int32_t mndStreamRecalcBuildDispatch(SStmStatus *pStatus, SArray **ppRequests) {
  if (pStatus == NULL || ppRequests == NULL) return TSDB_CODE_INVALID_PARA;
  *ppRequests = NULL;

  int32_t code = TSDB_CODE_SUCCESS;
  SArray *pRequests = NULL;
  taosWLockLatch(&pStatus->userRecalcLock);
  for (int32_t i = 0; i < taosArrayGetSize(pStatus->recalcRecords); ++i) {
    const SStmRecalcRecord *pRecord = taosArrayGet(pStatus->recalcRecords, i);
    if (pRecord == NULL || !mndStreamRecalcDispatchable(pRecord)) continue;
    if (pRequests == NULL) {
      pRequests = taosArrayInit(taosArrayGetSize(pStatus->recalcRecords), sizeof(SStreamRecalcReq));
      if (pRequests == NULL) {
        code = mndStreamRecalcErrorCode(terrno);
        break;
      }
    }
    SStreamRecalcReq request = {
        .recalcId = pRecord->snapshot.recalcId,
        .start = pRecord->snapshot.start,
        .end = pRecord->snapshot.end,
    };
    if (taosArrayPush(pRequests, &request) == NULL) {
      code = mndStreamRecalcErrorCode(terrno);
      break;
    }
  }
  taosWUnLockLatch(&pStatus->userRecalcLock);

  if (code != TSDB_CODE_SUCCESS) {
    taosArrayDestroy(pRequests);
    return code;
  }
  *ppRequests = pRequests;
  return TSDB_CODE_SUCCESS;
}

static bool mndStreamRecalcSnapshotValid(const SStreamRecalcSnapshot *pSnapshot) {
  if (pSnapshot == NULL || pSnapshot->recalcId == 0 || pSnapshot->start >= pSnapshot->end ||
      pSnapshot->progressPct < 0 || pSnapshot->progressPct > 100 || pSnapshot->status < STREAM_RECALC_STATUS_PENDING ||
      pSnapshot->status > STREAM_RECALC_STATUS_FAILED) {
    return false;
  }
  if (pSnapshot->status == STREAM_RECALC_STATUS_PENDING) return pSnapshot->progressPct == 0;
  if (pSnapshot->status == STREAM_RECALC_STATUS_FINISHED) return pSnapshot->progressPct == 100;
  // The trigger can cover the full range before the last attempt succeeds or fails.
  return true;
}

static bool mndStreamRecalcDetailValid(const SStreamRecalcDetail *pDetail) {
  if (pDetail == NULL || pDetail->recalcId == 0 || pDetail->retryOrdinal < 0 ||
      pDetail->retryOrdinal > STREAM_RECALC_MAX_ATTEMPT_ORDINAL ||
      (pDetail->errorCode == 0 && pDetail->errorText != NULL && pDetail->errorText[0] != 0) ||
      (pDetail->errorCode != 0 &&
       (pDetail->errorText == NULL || strcmp(pDetail->errorText, tstrerror(pDetail->errorCode)) != 0))) {
    return false;
  }
  return true;
}

static bool mndStreamRecalcRecordMatchesSnapshot(const SStmRecalcRecord      *pRecord,
                                                 const SStreamRecalcSnapshot *pSnapshot) {
  return pRecord->visible && !pRecord->hidden && pRecord->snapshot.start == pSnapshot->start &&
         pRecord->snapshot.end == pSnapshot->end;
}

static void mndStreamRecalcSetDetail(SStmRecalcRecord *pRecord, const SStreamRecalcDetail *pDetail) {
  pRecord->retryOrdinal = pDetail->retryOrdinal;
  pRecord->errorCode = pDetail->errorCode;
  tstrncpy(pRecord->errorText, pDetail->errorCode == 0 ? "" : tstrerror(pDetail->errorCode),
           sizeof(pRecord->errorText));
}

static void mndStreamRecalcClearDetail(SStmRecalcRecord *pRecord) {
  pRecord->retryOrdinal = 0;
  pRecord->errorCode = 0;
  pRecord->errorText[0] = 0;
}

static int32_t mndStreamRecalcApplySnapshotImpl(SMnode *pMnode, int64_t streamId, SStmStatus *pStatus,
                                                int64_t triggerTaskId, int64_t triggerSeriousId, bool completeSnapshot,
                                                EStreamRecalcDetailState detailState, const SArray *pSnapshots,
                                                const SArray *pDetails, bool *pStartDeferred) {
  if (pMnode == NULL || pStatus == NULL || streamId == 0) return TSDB_CODE_INVALID_PARA;
  if (pStatus->triggerTask == NULL || triggerTaskId != pStatus->triggerTask->id.taskId ||
      triggerSeriousId != pStatus->triggerTask->id.seriousId) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   snapshotNum = pSnapshots == NULL ? 0 : taosArrayGetSize(pSnapshots);
  bool      snapshotArrayValid = pSnapshots == NULL || pSnapshots->elemSize == sizeof(SStreamRecalcSnapshot);
  bool      allSnapshotsValid = snapshotArrayValid;
  bool      detailsValid = detailState == STREAM_RECALC_DETAIL_RECOGNIZED_VALID;
  SHashObj *pSnapshotIndex = mndStreamRecalcCreateIndex(snapshotNum);
  SHashObj *pDetailIndex = NULL;
  SHashObj *pRecordIndex = NULL;
  SArray   *pCandidates = NULL;
  if (pSnapshotIndex == NULL) return mndStreamRecalcErrorCode(terrno);

  for (int32_t i = 0; snapshotArrayValid && i < snapshotNum; ++i) {
    const SStreamRecalcSnapshot *pSnapshot = taosArrayGet(pSnapshots, i);
    if (pSnapshot == NULL) {
      allSnapshotsValid = false;
      continue;
    }
    if (!mndStreamRecalcSnapshotValid(pSnapshot)) allSnapshotsValid = false;
    int32_t *pExisting = taosHashGet(pSnapshotIndex, &pSnapshot->recalcId, sizeof(pSnapshot->recalcId));
    if (pExisting != NULL) {
      *pExisting = -1;
      allSnapshotsValid = false;
      continue;
    }
    code = taosHashPut(pSnapshotIndex, &pSnapshot->recalcId, sizeof(pSnapshot->recalcId), &i, sizeof(i));
    if (code != TSDB_CODE_SUCCESS) goto _exit;
  }

  if (detailsValid && pDetails != NULL && pDetails->elemSize != sizeof(SStreamRecalcDetail)) detailsValid = false;
  if (detailsValid) {
    int32_t detailNum = taosArrayGetSize(pDetails);
    pDetailIndex = mndStreamRecalcCreateIndex(detailNum);
    if (pDetailIndex == NULL) {
      code = mndStreamRecalcErrorCode(terrno);
      goto _exit;
    }
    for (int32_t i = 0; i < detailNum; ++i) {
      const SStreamRecalcDetail *pDetail = taosArrayGet(pDetails, i);
      int32_t snapshotIndex = pDetail == NULL ? -1 : mndStreamRecalcGetIndex(pSnapshotIndex, pDetail->recalcId);
      const SStreamRecalcSnapshot *pSnapshot = snapshotIndex < 0 ? NULL : taosArrayGet(pSnapshots, snapshotIndex);
      if (!mndStreamRecalcDetailValid(pDetail) || !mndStreamRecalcSnapshotValid(pSnapshot) ||
          taosHashGet(pDetailIndex, &pDetail->recalcId, sizeof(pDetail->recalcId)) != NULL) {
        detailsValid = false;
        break;
      }
      code = taosHashPut(pDetailIndex, &pDetail->recalcId, sizeof(pDetail->recalcId), &i, sizeof(i));
      if (code != TSDB_CODE_SUCCESS) goto _exit;
    }
  }

  bool detailAllowsComplete = detailState == STREAM_RECALC_DETAIL_ABSENT ||
                              (detailState == STREAM_RECALC_DETAIL_RECOGNIZED_VALID && detailsValid);
  completeSnapshot = completeSnapshot && allSnapshotsValid && detailAllowsComplete;
  bool clearMissingDetails = detailState == STREAM_RECALC_DETAIL_RECOGNIZED_VALID && detailsValid && allSnapshotsValid;
  bool applyRecognizedDetails = detailState == STREAM_RECALC_DETAIL_RECOGNIZED_VALID && detailsValid;

  pCandidates = taosArrayInit(snapshotNum, sizeof(SStreamRecalcTerminalCandidate));
  if (pCandidates == NULL) {
    code = mndStreamRecalcErrorCode(terrno);
    goto _exit;
  }

  taosWLockLatch(&pStatus->userRecalcLock);
  int32_t recordNum = taosArrayGetSize(pStatus->recalcRecords);
  pRecordIndex = mndStreamRecalcCreateIndex(recordNum);
  if (pRecordIndex == NULL) {
    code = mndStreamRecalcErrorCode(terrno);
    goto _unlock;
  }
  for (int32_t i = 0; i < recordNum; ++i) {
    const SStmRecalcRecord *pRecord = taosArrayGet(pStatus->recalcRecords, i);
    if (pRecord == NULL || pRecord->snapshot.recalcId == 0 ||
        taosHashGet(pRecordIndex, &pRecord->snapshot.recalcId, sizeof(pRecord->snapshot.recalcId)) != NULL) {
      code = TSDB_CODE_INVALID_MSG;
      goto _unlock;
    }
    code = taosHashPut(pRecordIndex, &pRecord->snapshot.recalcId, sizeof(pRecord->snapshot.recalcId), &i, sizeof(i));
    if (code != TSDB_CODE_SUCCESS) goto _unlock;
  }

  if (snapshotArrayValid) {
    for (int32_t i = 0; i < snapshotNum; ++i) {
      const SStreamRecalcSnapshot *pSnapshot = taosArrayGet(pSnapshots, i);
      if (!mndStreamRecalcSnapshotValid(pSnapshot) ||
          mndStreamRecalcGetIndex(pSnapshotIndex, pSnapshot->recalcId) != i) {
        continue;
      }
      int32_t index = mndStreamRecalcGetIndex(pRecordIndex, pSnapshot->recalcId);
      if (index < 0) continue;
      SStmRecalcRecord *pRecord = taosArrayGet(pStatus->recalcRecords, index);
      if (!mndStreamRecalcRecordMatchesSnapshot(pRecord, pSnapshot) ||
          mndStreamRecalcTerminal(pRecord->snapshot.status) || pRecord->terminalCandidateValid ||
          pRecord->terminalPersisting) {
        continue;
      }

      pRecord->dispatchConfirmed = true;
      if (pSnapshot->status == STREAM_RECALC_STATUS_RUNNING) {
        pRecord->snapshot.status = STREAM_RECALC_STATUS_RUNNING;
        pRecord->snapshot.progressPct = TMIN(TMAX(pRecord->snapshot.progressPct, pSnapshot->progressPct), 99);
      } else if (mndStreamRecalcTerminal(pSnapshot->status)) {
        pRecord->snapshot.status = STREAM_RECALC_STATUS_RUNNING;
        pRecord->snapshot.progressPct = TMIN(TMAX(pRecord->snapshot.progressPct, pSnapshot->progressPct), 99);
      }

      if (applyRecognizedDetails) {
        int32_t detailIndex = mndStreamRecalcGetIndex(pDetailIndex, pSnapshot->recalcId);
        if (detailIndex >= 0) {
          mndStreamRecalcSetDetail(pRecord, taosArrayGet(pDetails, detailIndex));
        } else if (clearMissingDetails) {
          mndStreamRecalcClearDetail(pRecord);
        }
      }

      if (!mndStreamRecalcTerminal(pSnapshot->status)) continue;
      bool                           finished = pSnapshot->status == STREAM_RECALC_STATUS_FINISHED;
      SStreamRecalcTerminalCandidate candidate = {
          .snapshot = *pSnapshot,
          .recordIndexHint = index,
          .retryOrdinal = finished ? 0 : pRecord->retryOrdinal,
          .errorCode = finished ? 0 : pRecord->errorCode,
          .errorText = finished || pRecord->errorCode == 0 ? "" : tstrerror(pRecord->errorCode),
      };
      if (taosArrayPush(pCandidates, &candidate) == NULL) {
        code = mndStreamRecalcErrorCode(terrno);
        goto _unlock;
      }
    }
  }

  if (completeSnapshot) {
    for (int32_t i = 0; i < taosArrayGetSize(pStatus->recalcRecords); ++i) {
      SStmRecalcRecord *pRecord = taosArrayGet(pStatus->recalcRecords, i);
      if (pRecord->requestTimeMs <= 0 || !pRecord->visible || pRecord->hidden ||
          !mndStreamRecalcRuntimeActive(pRecord) || pRecord->terminalCandidateValid || pRecord->terminalPersisting) {
        continue;
      }
      int32_t                      snapshotIndex = mndStreamRecalcGetIndex(pSnapshotIndex, pRecord->snapshot.recalcId);
      const SStreamRecalcSnapshot *pSnapshot = snapshotIndex < 0 ? NULL : taosArrayGet(pSnapshots, snapshotIndex);
      bool                         exactReported =
          mndStreamRecalcSnapshotValid(pSnapshot) && mndStreamRecalcRecordMatchesSnapshot(pRecord, pSnapshot);
      if (pRecord->dispatchConfirmed && !exactReported) pRecord->dispatchConfirmed = false;
    }
  }

_unlock:
  taosWUnLockLatch(&pStatus->userRecalcLock);
  if (code != TSDB_CODE_SUCCESS) goto _exit;

  for (int32_t i = 0; i < taosArrayGetSize(pCandidates); ++i) {
    const SStreamRecalcTerminalCandidate *pCandidate = taosArrayGet(pCandidates, i);
    int32_t                               finishCode = pStartDeferred == NULL
                                                           ? mndStreamRecalcFinish(pMnode, streamId, pCandidate)
                                                           : mndStreamRecalcFinishImpl(pMnode, streamId, pCandidate, pStartDeferred);
    if (code == TSDB_CODE_SUCCESS && finishCode != TSDB_CODE_SUCCESS) code = finishCode;
  }

_exit:
  taosHashCleanup(pRecordIndex);
  taosHashCleanup(pDetailIndex);
  taosHashCleanup(pSnapshotIndex);
  taosArrayDestroy(pCandidates);
  return code;
}

int32_t mndStreamRecalcApplySnapshot(SMnode *pMnode, int64_t streamId, SStmStatus *pStatus, int64_t triggerTaskId,
                                     int64_t triggerSeriousId, bool completeSnapshot,
                                     EStreamRecalcDetailState detailState, const SArray *pSnapshots,
                                     const SArray *pDetails) {
  return mndStreamRecalcApplySnapshotImpl(pMnode, streamId, pStatus, triggerTaskId, triggerSeriousId, completeSnapshot,
                                          detailState, pSnapshots, pDetails, NULL);
}

int32_t mndStreamRecalcApplySnapshotDeferred(SMnode *pMnode, int64_t streamId, SStmStatus *pStatus,
                                             int64_t triggerTaskId, int64_t triggerSeriousId, bool completeSnapshot,
                                             EStreamRecalcDetailState detailState, const SArray *pSnapshots,
                                             const SArray *pDetails, bool *pStartDeferred) {
  if (pStartDeferred == NULL) return TSDB_CODE_INVALID_PARA;
  return mndStreamRecalcApplySnapshotImpl(pMnode, streamId, pStatus, triggerTaskId, triggerSeriousId, completeSnapshot,
                                          detailState, pSnapshots, pDetails, pStartDeferred);
}

static int32_t mndStreamRecalcReserveAccept(SStmStatus *pStatus, const SStmRecalcRecord *pRecord, SListNode *pOpNode,
                                            bool *pClaimed) {
  int32_t code = TSDB_CODE_SUCCESS;
  SArray *pRecords = NULL;
  *pClaimed = false;

  while (true) {
    taosWLockLatch(&pStatus->userRecalcLock);
    int32_t recordCount = taosArrayGetSize(pStatus->recalcRecords);
    taosWUnLockLatch(&pStatus->userRecalcLock);

    pRecords = taosArrayInit(recordCount + 1, sizeof(SStmRecalcRecord));
    if (pRecords == NULL) return mndStreamRecalcErrorCode(terrno);

    taosWLockLatch(&pStatus->userRecalcLock);
    if (recordCount != taosArrayGetSize(pStatus->recalcRecords)) {
      taosWUnLockLatch(&pStatus->userRecalcLock);
      taosArrayDestroy(pRecords);
      pRecords = NULL;
      continue;
    }

    for (int32_t i = 0; i < recordCount; ++i) {
      const SStmRecalcRecord *pExisting = taosArrayGet(pStatus->recalcRecords, i);
      if (taosArrayPush(pRecords, pExisting) == NULL) {
        code = mndStreamRecalcErrorCode(terrno);
        goto _unlock;
      }
    }
    if (taosArrayPush(pRecords, pRecord) == NULL) {
      code = mndStreamRecalcErrorCode(terrno);
      goto _unlock;
    }

    SArray *pOldRecords = pStatus->recalcRecords;
    ++pStatus->recalcRestoreVersion;
    pStatus->recalcRecords = pRecords;
    pRecords = pOldRecords;
    mndStreamRecalcInitStatus(pStatus);
    tdListAppendNode(&pStatus->recalcPersistOps, pOpNode);
    if (!pStatus->recalcTransActive) {
      pStatus->recalcTransActive = true;
      *pClaimed = true;
    }

  _unlock:
    taosWUnLockLatch(&pStatus->userRecalcLock);
    taosArrayDestroy(pRecords);
    return code;
  }
}

static void mndStreamRecalcSendResponse(const SRpcHandleInfo *pRpcInfo, int32_t code) {
  SRpcMsg rsp = {.code = code, .info = *pRpcInfo};
  tmsgSendRsp(&rsp);
}

void mndStreamRecalcCancelPending(SStmStatus *pStatus, int32_t code) {
  if (pStatus == NULL) return;

  SList pending = {0};
  tdListInit(&pending, sizeof(SStreamRecalcPersistOp));
  bool sendResponses = atomic_load_8(&mStreamMgmt.active) != 0;

  taosWLockLatch(&pStatus->userRecalcLock);
  if (pStatus->recalcPersistOpsInitialized) tdListMove(&pStatus->recalcPersistOps, &pending);
  pStatus->recalcPersistOpsInitialized = false;
  pStatus->recalcTransActive = false;
  taosWUnLockLatch(&pStatus->userRecalcLock);

  SListNode *pNode = NULL;
  while ((pNode = tdListPopHead(&pending)) != NULL) {
    const SStreamRecalcPersistOp *pOp = (const SStreamRecalcPersistOp *)pNode->data;
    if (sendResponses && pOp->hasRpc && !pOp->rpcTransferred) {
      mndStreamRecalcSendResponse(&pOp->rpcInfo, code);
    }
    listNodeFree(pNode);
  }
}

static void mndStreamRecalcRemoveHiddenRecord(SStmStatus *pStatus, int64_t recalcId) {
  int32_t index = mndStreamRecalcFindRecord(pStatus->recalcRecords, recalcId);
  if (index < 0) return;
  const SStmRecalcRecord *pRecord = taosArrayGet(pStatus->recalcRecords, index);
  if (pRecord->hidden && !pRecord->visible) {
    taosArrayRemove(pStatus->recalcRecords, index);
    ++pStatus->recalcRestoreVersion;
  }
}

static void mndStreamRecalcStartFailed(SStmStatus *pStatus, const SStreamRecalcPersistOp *pExpected, int32_t code,
                                       bool synchronousAccept, SStreamRecalcPostActions *pPostActions) {
  SListNode *pCompleted = NULL;
  bool       sendResponse = false;
  bool       scheduleNext = false;
  bool       keepForRetry = false;

  taosWLockLatch(&pStatus->userRecalcLock);
  SStreamRecalcPersistOp *pHead = mndStreamRecalcHeadOp(pStatus);
  if (pHead == NULL || !pStatus->recalcTransActive || pHead->type != pExpected->type ||
      pHead->streamId != pExpected->streamId || pHead->recalcId != pExpected->recalcId) {
    taosWUnLockLatch(&pStatus->userRecalcLock);
    return;
  }

  pStatus->recalcTransActive = false;
  if (pHead->type == MND_STREAM_RECALC_OP_ACCEPT) {
    mndStreamRecalcRemoveHiddenRecord(pStatus, pHead->recalcId);
    if (!synchronousAccept && pHead->hasRpc) {
      sendResponse = true;
    }
    pCompleted = tdListPopHead(&pStatus->recalcPersistOps);
    scheduleNext = !isListEmpty(&pStatus->recalcPersistOps);
  } else {
    keepForRetry = true;
  }
  taosWUnLockLatch(&pStatus->userRecalcLock);

  mndStreamRecalcQueueCompletedAction(pPostActions, pCompleted, sendResponse, code);
  pPostActions->scheduleNext = pPostActions->scheduleNext || scheduleNext;
  pPostActions->markPullupPending = pPostActions->markPullupPending || keepForRetry;
}

static int32_t mndStreamRecalcPrepareTrans(SMnode *pMnode, const SStreamRecalcPendingTrans *pPending) {
  const SStreamRecalcPersistOp *pOp = &pPending->op;
  STrans                      *pTrans = NULL;
  SStreamRecalcTransParam      *pParam = taosMemoryMalloc(sizeof(*pParam));
  if (pParam == NULL) return mndStreamRecalcErrorCode(terrno);
  *pParam = (SStreamRecalcTransParam){.streamId = pOp->streamId,
                                    .recalcId = pOp->recalcId,
                                    .targetRevision = pOp->targetRevision,
                                    .opType = (int8_t)pOp->type};
  SRpcMsg transReq = {.msgType = TDMT_MND_RECALC_STREAM, .info = pOp->rpcInfo};
  const SRpcMsg *pTransReq = pOp->type == MND_STREAM_RECALC_OP_ACCEPT && pOp->hasRpc ? &transReq : NULL;
  int32_t code = mndStreamCreateTrans(pMnode, pPending->pStream, (SRpcMsg *)pTransReq, TRN_CONFLICT_DB_INSIDE,
                                      MND_STREAM_RECALC_NAME, &pTrans);
  if (code != TSDB_CODE_SUCCESS || pTrans == NULL) {
    code = code != TSDB_CODE_SUCCESS ? code : mndStreamRecalcErrorCode(terrno);
    goto _end;
  }
  code = mndStreamTransAppendRecalcUpdate(pPending->pStream, pOp->targetRevision, pPending->pRequests, pTrans,
                                          SDB_STATUS_READY);
  if (code != TSDB_CODE_SUCCESS) goto _end;
  mndTransSetCb(pTrans, 0, TRANS_STOP_FUNC_STREAM_RECALC, pParam, sizeof(*pParam));
  pParam = NULL;
  code = mndTransPrepare(pMnode, pTrans);

_end:
  mndTransDrop(pTrans);
  taosMemoryFree(pParam);
  return code;
}

// Prepare can wait for a sync callback that acquires runtimeLock. Queue an
// acquired stream and an immutable snapshot, not an allocated transaction ID:
// mndTransCreate does not reserve IDs until prepare writes the transaction.
static void mndStreamRecalcPreparePending(SMnode *pMnode, SStreamRecalcPostActions *pPostActions) {
  SListNode *pNode = NULL;
  while ((pNode = tdListPopHead(&pPostActions->pendingTrans)) != NULL) {
    SStreamRecalcPendingTrans *pPending = (SStreamRecalcPendingTrans *)pNode->data;
    int32_t code = mndStreamRecalcPrepareTrans(pMnode, pPending);
    mndReleaseStream(pMnode, pPending->pStream);
    taosArrayDestroy(pPending->pRequests);
    if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
      taosRLockLatch(&mStreamMgmt.runtimeLock);
      SHashObj *pStreamMap = mStreamMgmt.streamMap;
      // Transferred RPCs remain owned by this snapshot across runtime teardown.
      bool sendResponse = pPending->op.type == MND_STREAM_RECALC_OP_ACCEPT && pPending->op.hasRpc;
      if (atomic_load_8(&mStreamMgmt.active) != 0 && pStreamMap != NULL &&
          mStreamMgmt.stat.activeTimes == pPending->runtimeGeneration) {
        SStmStatus *pStatus = taosHashAcquire(pStreamMap, &pPending->op.streamId, sizeof(pPending->op.streamId));
        if (pStatus != NULL) {
          // The pending snapshot owns failure responses after RPC transfer.
          // Queue cleanup must not also respond, even when status still exists.
          mndStreamRecalcStartFailed(pStatus, &pPending->op, code, true, pPostActions);
          taosHashRelease(pStreamMap, pStatus);
        }
      }
      taosRUnLockLatch(&mStreamMgmt.runtimeLock);
      if (sendResponse) {
        mndStreamRecalcSendResponse(&pPending->op.rpcInfo, code);
      }
    }
    listNodeFree(pNode);
  }
}

static int32_t mndStreamRecalcSetTarget(SStmStatus *pStatus, const SStreamRecalcPersistOp *pExpected,
                                        uint64_t targetRevision) {
  int32_t code = TSDB_CODE_SUCCESS;
  taosWLockLatch(&pStatus->userRecalcLock);
  SStreamRecalcPersistOp *pHead = mndStreamRecalcHeadOp(pStatus);
  if (pHead == NULL || !pStatus->recalcTransActive || pHead->type != pExpected->type ||
      pHead->streamId != pExpected->streamId || pHead->recalcId != pExpected->recalcId) {
    code = TSDB_CODE_INVALID_MSG;
  } else {
    pHead->targetRevision = targetRevision;
  }
  taosWUnLockLatch(&pStatus->userRecalcLock);
  return code;
}

static int32_t mndStreamRecalcTransferRpc(SStmStatus *pStatus, const SStreamRecalcPersistOp *pExpected) {
  int32_t code = TSDB_CODE_SUCCESS;
  taosWLockLatch(&pStatus->userRecalcLock);
  SStreamRecalcPersistOp *pHead = mndStreamRecalcHeadOp(pStatus);
  if (pHead == NULL || !pStatus->recalcTransActive || pHead->type != pExpected->type ||
      pHead->streamId != pExpected->streamId || pHead->recalcId != pExpected->recalcId) {
    code = TSDB_CODE_INVALID_MSG;
  } else if (pHead->type == MND_STREAM_RECALC_OP_ACCEPT && pHead->hasRpc) {
    pHead->rpcTransferred = true;
  }
  taosWUnLockLatch(&pStatus->userRecalcLock);
  return code;
}

static int32_t mndStreamRecalcStartClaimed(SMnode *pMnode, SStmStatus *pStatus, bool synchronousAccept,
                                           bool runtimeLocked, SStreamRecalcPostActions *pPostActions) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  SStreamObj              *pStream = NULL;
  SArray                  *pRequests = NULL;
  SStreamRecalcPersistOp   op = {0};
  uint64_t                 baseRevision = 0;
  bool                     alreadyCommitted = false;

  taosWLockLatch(&pStatus->userRecalcLock);
  SStreamRecalcPersistOp *pHead = mndStreamRecalcHeadOp(pStatus);
  if (pHead == NULL || !pStatus->recalcTransActive) {
    taosWUnLockLatch(&pStatus->userRecalcLock);
    return TSDB_CODE_SUCCESS;
  }
  op = *pHead;
  taosWUnLockLatch(&pStatus->userRecalcLock);

  code = mndAcquireStreamById(pMnode, op.streamId, &pStream);
  if (code != TSDB_CODE_SUCCESS || pStream == NULL) {
    code = code != TSDB_CODE_SUCCESS ? code : TSDB_CODE_MND_STREAM_NOT_EXIST;
    goto _failed;
  }
  mInfo(KEYOP("stream-recalc", "START") "streamId:%" PRIx64 " recalcId:%" PRIx64 " opType:%d start claimed recalc",
        op.streamId, op.recalcId, op.type);
  TAOS_CHECK_GOTO(mndStreamRecalcSnapshotWithRevision(pStream, &pRequests, &baseRevision), NULL, _failed);

  int32_t requestIndex = mndStreamRecalcFindRequest(pRequests, op.recalcId);
  if (op.type == MND_STREAM_RECALC_OP_ACCEPT) {
    if (requestIndex >= 0) {
      const SStreamRecalcPersistReq *pExisting = taosArrayGet(pRequests, requestIndex);
      if (pExisting->start != op.request.start || pExisting->end != op.request.end ||
          pExisting->requestTimeMs != op.request.requestTimeMs) {
        code = TSDB_CODE_INVALID_MSG;
        goto _failed;
      }
      alreadyCommitted = true;
    } else if (taosArrayPush(pRequests, &op.request) == NULL) {
      code = mndStreamRecalcErrorCode(terrno);
      goto _failed;
    }
  } else if (requestIndex < 0) {
    alreadyCommitted = true;
  } else {
    taosArrayRemove(pRequests, requestIndex);
  }

  uint64_t targetRevision = alreadyCommitted ? baseRevision : baseRevision + 1;
  if (!alreadyCommitted && targetRevision == 0) {
    code = TSDB_CODE_OUT_OF_RANGE;
    goto _failed;
  }
  TAOS_CHECK_GOTO(mndStreamRecalcSetTarget(pStatus, &op, targetRevision), NULL, _failed);

  SStreamRecalcTransParam callbackParam = {
      .streamId = op.streamId,
      .recalcId = op.recalcId,
      .targetRevision = targetRevision,
      .opType = (int8_t)op.type,
  };
  if (alreadyCommitted) {
    mndReleaseStream(pMnode, pStream);
    pStream = NULL;
    taosArrayDestroy(pRequests);
    pRequests = NULL;
    if (runtimeLocked) {
      bool committed = mndStreamRecalcCheckCommitted(pMnode, &callbackParam);
      mndStreamRecalcPublishCommittedLocked(pStatus, &callbackParam, true, committed, pPostActions);
    } else {
      mndStreamRecalcPublishCommitted(pMnode, &callbackParam, true, pPostActions);
    }
    return TSDB_CODE_SUCCESS;
  }

  op.targetRevision = targetRevision;
  TAOS_CHECK_GOTO(mndStreamRecalcTransferRpc(pStatus, &op), NULL, _failed);
  SStreamRecalcPendingTrans pending = {.pStream = pStream, .pRequests = pRequests, .op = op};
  if (runtimeLocked) {
    pending.runtimeGeneration = mStreamMgmt.stat.activeTimes;
    code = tdListAppend(&pPostActions->pendingTrans, &pending);
    if (code != TSDB_CODE_SUCCESS) {
      goto _failed;
    }
    pStream = NULL;
    pRequests = NULL;
  } else {
    code = mndStreamRecalcPrepareTrans(pMnode, &pending);
  }
  if (code == TSDB_CODE_SUCCESS || code == TSDB_CODE_ACTION_IN_PROGRESS) code = TSDB_CODE_SUCCESS;

_failed:
  taosArrayDestroy(pRequests);
  if (pStream != NULL) mndReleaseStream(pMnode, pStream);
  if (code != TSDB_CODE_SUCCESS) {
    mndStreamRecalcStartFailed(pStatus, &op, code, synchronousAccept, pPostActions);
  }
  return code;
}

int32_t mndStreamRecalcAccept(SMnode *pMnode, SStreamObj *pStream, SStmStatus *pStatus, const STimeWindow *pRange,
                              const SRpcMsg *pReq) {
  if (pMnode == NULL || pStream == NULL || pStream->pCreate == NULL || pStatus == NULL || pRange == NULL ||
      pReq == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int64_t recalcId = 0;
  int32_t code = TSDB_CODE_SUCCESS;
  do {
    code = taosGetSystemUUIDU64((uint64_t *)&recalcId);
    if (code != TSDB_CODE_SUCCESS) return code;
  } while (recalcId == 0);

  int64_t          requestTimeMs = taosGetTimestampMs();
  SStmRecalcRecord record = {
      .snapshot =
          {
              .recalcId = recalcId,
              .start = pRange->skey,
              .end = pRange->ekey,
              .progressPct = 0,
              .status = STREAM_RECALC_STATUS_PENDING,
          },
      .requestTimeMs = requestTimeMs,
      .triggerTaskId = pStatus->triggerTask == NULL ? 0 : pStatus->triggerTask->id.taskId,
      .triggerSeriousId = pStatus->triggerTask == NULL ? 0 : pStatus->triggerTask->id.seriousId,
      .typedStatusKnown = true,
      .hidden = true,
      .visible = false,
  };
  SStreamRecalcPersistOp op = {
      .type = MND_STREAM_RECALC_OP_ACCEPT,
      .streamId = pStream->pCreate->streamId,
      .recalcId = recalcId,
      .request =
          {
              .recalcId = recalcId,
              .start = pRange->skey,
              .end = pRange->ekey,
              .requestTimeMs = requestTimeMs,
          },
      .rpcInfo = pReq->info,
      .hasRpc = true,
  };
  SListNode *pOpNode = mndStreamRecalcAllocOpNode(&op);
  if (pOpNode == NULL) return mndStreamRecalcErrorCode(terrno);

  bool claimed = false;
  code = mndStreamRecalcReserveAccept(pStatus, &record, pOpNode, &claimed);
  if (code != TSDB_CODE_SUCCESS) {
    listNodeFree(pOpNode);
    return code;
  }
  mInfo(KEYOP("stream-recalc", "STEP") "streamId:%" PRIx64 " recalcId:%" PRIx64
                                       " recalc request accepted, range:[%" PRId64 ",%" PRId64 "]",
        pStream->pCreate->streamId, recalcId, pRange->skey, pRange->ekey);
  if (!claimed) return TSDB_CODE_SUCCESS;
  SStreamRecalcPostActions postActions = {0};
  mndStreamRecalcInitPostActions(&postActions);
  code = mndStreamRecalcStartClaimed(pMnode, pStatus, true, false, &postActions);
  mndStreamRecalcRunPostActions(pMnode, &postActions);
  return code;
}

static bool mndStreamRecalcTerminal(EStreamRecalcStatus status) {
  return status == STREAM_RECALC_STATUS_FINISHED || status == STREAM_RECALC_STATUS_FAILED;
}

static bool mndStreamRecalcSameCandidate(const SStmRecalcRecord               *pRecord,
                                         const SStreamRecalcTerminalCandidate *pCandidate) {
  return pRecord->terminalCandidate.status == pCandidate->snapshot.status &&
         pRecord->terminalCandidate.progressPct == pCandidate->snapshot.progressPct &&
         pRecord->retryOrdinal == pCandidate->retryOrdinal && pRecord->errorCode == pCandidate->errorCode;
}

static int32_t mndStreamRecalcCandidateRecordIndexLocked(SStmStatus                           *pStatus,
                                                         const SStreamRecalcTerminalCandidate *pCandidate) {
  int32_t                 index = pCandidate->recordIndexHint;
  const SStmRecalcRecord *pRecord = index < 0 ? NULL : taosArrayGet(pStatus->recalcRecords, index);
  if (pRecord != NULL && pRecord->snapshot.recalcId == pCandidate->snapshot.recalcId) return index;
  return mndStreamRecalcFindRecord(pStatus->recalcRecords, pCandidate->snapshot.recalcId);
}

static int32_t mndStreamRecalcCheckFinishLocked(SStmStatus *pStatus, int64_t streamId,
                                                const SStreamRecalcTerminalCandidate *pCandidate, bool *pNeedsEnqueue,
                                                int32_t *pRecordIndex) {
  *pNeedsEnqueue = false;
  *pRecordIndex = mndStreamRecalcCandidateRecordIndexLocked(pStatus, pCandidate);
  if (*pRecordIndex < 0) return TSDB_CODE_INVALID_MSG;

  const SStmRecalcRecord *pRecord = taosArrayGet(pStatus->recalcRecords, *pRecordIndex);
  if (mndStreamRecalcTerminal(pRecord->snapshot.status)) return TSDB_CODE_SUCCESS;
  if (pRecord->snapshot.start != pCandidate->snapshot.start || pRecord->snapshot.end != pCandidate->snapshot.end) {
    return TSDB_CODE_INVALID_MSG;
  }
  if (pRecord->terminalCandidateValid) {
    if (!mndStreamRecalcSameCandidate(pRecord, pCandidate)) {
      mWarn("ignore conflicting terminal candidate for stream:%" PRIx64 " recalc:%" PRIx64, streamId,
            pCandidate->snapshot.recalcId);
    }
    return TSDB_CODE_SUCCESS;
  }

  *pNeedsEnqueue = true;
  return TSDB_CODE_SUCCESS;
}

static int32_t mndStreamRecalcFinishImpl(SMnode *pMnode, int64_t streamId,
                                         const SStreamRecalcTerminalCandidate *pCandidate, bool *pStartDeferred) {
  if (pMnode == NULL || pCandidate == NULL || pCandidate->snapshot.recalcId == 0 ||
      !mndStreamRecalcTerminal(pCandidate->snapshot.status) || pCandidate->snapshot.progressPct < 0 ||
      pCandidate->snapshot.progressPct > 100) {
    return TSDB_CODE_INVALID_PARA;
  }

  SStmStatus *pStatus =
      mStreamMgmt.streamMap == NULL ? NULL : taosHashAcquire(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (pStatus == NULL) return TSDB_CODE_INVALID_MSG;

  bool    needsEnqueue = false;
  int32_t recordIndex = -1;
  taosWLockLatch(&pStatus->userRecalcLock);
  int32_t code = mndStreamRecalcCheckFinishLocked(pStatus, streamId, pCandidate, &needsEnqueue, &recordIndex);
  taosWUnLockLatch(&pStatus->userRecalcLock);
  if (code != TSDB_CODE_SUCCESS || !needsEnqueue) goto _exit;

  SStreamRecalcPersistOp op = {
      .type = MND_STREAM_RECALC_OP_FINISH,
      .streamId = streamId,
      .recalcId = pCandidate->snapshot.recalcId,
      .candidateSnapshot = pCandidate->snapshot,
      .candidateRetryOrdinal = pCandidate->retryOrdinal,
      .candidateErrorCode = pCandidate->errorCode,
  };
  tstrncpy(op.candidateErrorText, pCandidate->errorCode == 0 ? "" : tstrerror(pCandidate->errorCode),
           sizeof(op.candidateErrorText));
  SListNode *pOpNode = mndStreamRecalcAllocOpNode(&op);
  if (pOpNode == NULL) {
    code = mndStreamRecalcErrorCode(terrno);
    goto _exit;
  }

  bool claimed = false;
  taosWLockLatch(&pStatus->userRecalcLock);
  code = mndStreamRecalcCheckFinishLocked(pStatus, streamId, pCandidate, &needsEnqueue, &recordIndex);
  if (code != TSDB_CODE_SUCCESS || !needsEnqueue) goto _unlock;

  SStmRecalcRecord *pRecord = taosArrayGet(pStatus->recalcRecords, recordIndex);

  {
    const char *keyOpPhase =
        (pCandidate->snapshot.status == STREAM_RECALC_STATUS_FINISHED) ? KEYOP_PHASE_END : KEYOP_PHASE_FAIL;
    mInfo(KEYOP_TAG_FMT "streamId:%" PRIx64 " recalcId:%" PRIx64 " progressPct:%d errorCode:0x%x", "stream-recalc",
          keyOpPhase, streamId, pCandidate->snapshot.recalcId, pCandidate->snapshot.progressPct,
          pCandidate->errorCode);
  }

  if (pRecord->snapshot.status == STREAM_RECALC_STATUS_PENDING) {
    pRecord->snapshot.status = STREAM_RECALC_STATUS_RUNNING;
  }
  op.candidateSnapshot.progressPct = TMAX(op.candidateSnapshot.progressPct, pRecord->snapshot.progressPct);
  ++pStatus->recalcRestoreVersion;
  pRecord->terminalCandidate = op.candidateSnapshot;
  pRecord->retryOrdinal = op.candidateRetryOrdinal;
  pRecord->errorCode = op.candidateErrorCode;
  tstrncpy(pRecord->errorText, op.candidateErrorText, sizeof(pRecord->errorText));
  pRecord->terminalCandidateValid = true;
  pRecord->terminalPersisting = true;
  memcpy(pOpNode->data, &op, sizeof(op));

  mndStreamRecalcInitStatus(pStatus);
  tdListAppendNode(&pStatus->recalcPersistOps, pOpNode);
  pOpNode = NULL;
  if (!pStatus->recalcTransActive) {
    if (pStartDeferred != NULL) {
      *pStartDeferred = true;
    } else {
      pStatus->recalcTransActive = true;
      claimed = true;
    }
  }

_unlock:
  taosWUnLockLatch(&pStatus->userRecalcLock);
  listNodeFree(pOpNode);
  if (code == TSDB_CODE_SUCCESS && claimed) {
    SStreamRecalcPostActions postActions = {0};
    mndStreamRecalcInitPostActions(&postActions);
    (void)mndStreamRecalcStartClaimed(pMnode, pStatus, false, false, &postActions);
    mndStreamRecalcRunPostActions(pMnode, &postActions);
  }

_exit:
  taosHashRelease(mStreamMgmt.streamMap, pStatus);
  return code;
}

int32_t mndStreamRecalcFinish(SMnode *pMnode, int64_t streamId, const SStreamRecalcTerminalCandidate *pCandidate) {
  return mndStreamRecalcFinishImpl(pMnode, streamId, pCandidate, NULL);
}

static bool mndStreamRecalcCheckCommitted(SMnode *pMnode, const SStreamRecalcTransParam *pCallbackParam) {
  SStreamObj *pStream = NULL;
  bool        committed = false;
  if (mndAcquireStreamById(pMnode, pCallbackParam->streamId, &pStream) == TSDB_CODE_SUCCESS && pStream != NULL) {
    taosRLockLatch(&pStream->lock);
    committed = pStream->recalcRevision >= pCallbackParam->targetRevision;
    if (pCallbackParam->opType == MND_STREAM_RECALC_OP_ACCEPT) {
      committed = committed && mndStreamRecalcPersisted(pStream, pCallbackParam->recalcId);
    } else {
      committed = committed && !mndStreamRecalcPersisted(pStream, pCallbackParam->recalcId);
    }
    taosRUnLockLatch(&pStream->lock);
    mndReleaseStream(pMnode, pStream);
  }
  return committed;
}

static void mndStreamRecalcPublishCommittedLocked(SStmStatus *pStatus, const SStreamRecalcTransParam *pCallbackParam,
                                                  bool queueOwnsResponse, bool committed,
                                                  SStreamRecalcPostActions *pPostActions) {
  SListNode *pCompleted = NULL;
  bool       sendResponse = false;
  bool       scheduleNext = false;
  taosWLockLatch(&pStatus->userRecalcLock);
  SStreamRecalcPersistOp *pHead = mndStreamRecalcHeadOp(pStatus);
  if (pHead == NULL || !pStatus->recalcTransActive || pHead->type != pCallbackParam->opType ||
      pHead->streamId != pCallbackParam->streamId || pHead->recalcId != pCallbackParam->recalcId ||
      pHead->targetRevision != pCallbackParam->targetRevision) {
    taosWUnLockLatch(&pStatus->userRecalcLock);
    return;
  }

  if (committed) {
    int32_t           recordIndex = mndStreamRecalcFindRecord(pStatus->recalcRecords, pCallbackParam->recalcId);
    SStmRecalcRecord *pRecord = recordIndex < 0 ? NULL : taosArrayGet(pStatus->recalcRecords, recordIndex);
    if (pRecord == NULL) committed = false;
    if (committed && pHead->type == MND_STREAM_RECALC_OP_ACCEPT) {
      pRecord->hidden = false;
      pRecord->visible = true;
      pRecord->dispatchConfirmed = false;
      sendResponse = queueOwnsResponse && pHead->hasRpc;
    } else if (committed) {
      pRecord->snapshot = pHead->candidateSnapshot;
      if (pRecord->snapshot.status == STREAM_RECALC_STATUS_FINISHED) pRecord->snapshot.progressPct = 100;
      pRecord->retryOrdinal = pHead->candidateRetryOrdinal;
      pRecord->errorCode = pHead->candidateErrorCode;
      tstrncpy(pRecord->errorText, pHead->candidateErrorText, sizeof(pRecord->errorText));
      pRecord->terminalObservedAtMs = taosGetTimestampMs();
      pRecord->terminalPersisting = false;
      pRecord->hidden = false;
      pRecord->visible = true;
    }
    ++pStatus->recalcRestoreVersion;
    (void)mstPruneRecalcRecordsLocked(pStatus->recalcRecords, taosGetTimestampMs());
  }

  pStatus->recalcTransActive = false;
  if (committed) pCompleted = tdListPopHead(&pStatus->recalcPersistOps);
  scheduleNext = !isListEmpty(&pStatus->recalcPersistOps);
  taosWUnLockLatch(&pStatus->userRecalcLock);

  mndStreamRecalcQueueCompletedAction(pPostActions, pCompleted, sendResponse, TSDB_CODE_SUCCESS);
  pPostActions->scheduleNext = pPostActions->scheduleNext || scheduleNext;
}

static void mndStreamRecalcPublishCommitted(SMnode *pMnode, const SStreamRecalcTransParam *pCallbackParam,
                                            bool queueOwnsResponse, SStreamRecalcPostActions *pPostActions) {
  if (pCallbackParam->opType != MND_STREAM_RECALC_OP_ACCEPT && pCallbackParam->opType != MND_STREAM_RECALC_OP_FINISH) {
    return;
  }

  bool committed = mndStreamRecalcCheckCommitted(pMnode, pCallbackParam);

  taosRLockLatch(&mStreamMgmt.runtimeLock);
  SHashObj *pStreamMap = mStreamMgmt.streamMap;
  if (atomic_load_8(&mStreamMgmt.active) == 0 || pStreamMap == NULL) {
    taosRUnLockLatch(&mStreamMgmt.runtimeLock);
    return;
  }

  SStmStatus *pStatus = taosHashAcquire(pStreamMap, &pCallbackParam->streamId, sizeof(pCallbackParam->streamId));
  if (pStatus == NULL) {
    taosRUnLockLatch(&mStreamMgmt.runtimeLock);
    return;
  }

  mndStreamRecalcPublishCommittedLocked(pStatus, pCallbackParam, queueOwnsResponse, committed, pPostActions);
  taosHashRelease(pStreamMap, pStatus);
  taosRUnLockLatch(&mStreamMgmt.runtimeLock);
}

void mndStreamRecalcTransStopped(SMnode *pMnode, void *param, int32_t paramLen) {
  if (pMnode == NULL || param == NULL || paramLen != sizeof(SStreamRecalcTransParam)) return;
  SStreamRecalcTransParam  callbackParam = *(SStreamRecalcTransParam *)param;
  SStreamRecalcPostActions postActions = {0};
  mndStreamRecalcInitPostActions(&postActions);
  mndStreamRecalcPublishCommitted(pMnode, &callbackParam, false, &postActions);
  mndStreamRecalcRunPostActions(pMnode, &postActions);
}

static bool mndStreamRecalcStatusClaimable(SStmStatus *pStatus) {
  bool claimable = false;
  taosWLockLatch(&pStatus->userRecalcLock);
  claimable = pStatus->recalcPersistOpsInitialized && !pStatus->recalcTransActive &&
              !isListEmpty(&pStatus->recalcPersistOps);
  taosWUnLockLatch(&pStatus->userRecalcLock);
  return claimable;
}

static int32_t mndStreamRecalcCountClaimable(SHashObj *pStreamMap) {
  int32_t count = 0;
  void   *pIter = NULL;
  while ((pIter = taosHashIterate(pStreamMap, pIter)) != NULL) {
    if (mndStreamRecalcStatusClaimable(pIter)) ++count;
  }
  return count;
}

static void mndStreamRecalcClaimPass(SMnode *pMnode, SHashObj *pStreamMap, bool afterCursor,
                                     int64_t cursor, bool cursorValid, int32_t claimLimit, int32_t *pClaimedCount,
                                     SStreamRecalcPostActions *pPostActions) {
  void *pIter = NULL;
  if (afterCursor && cursorValid) {
    // Acquire a fresh reference for this batch, then let the iterator release it
    // while advancing. No raw hash entry survives a runtime-lock boundary.
    pIter = taosHashAcquire(pStreamMap, &cursor, sizeof(cursor));
    if (pIter == NULL) return;  // The wrap pass handles a removed cursor.
  }
  while (*pClaimedCount < claimLimit &&
         (pIter = taosHashIterate(pStreamMap, pIter)) != NULL) {
    const int64_t streamId = *(int64_t *)taosHashGetKey(pIter, NULL);
    const bool    stopAtCursor = !afterCursor && cursorValid && streamId == cursor;
    SStmStatus *pStatus = pIter;
    bool        claimed = false;
    taosWLockLatch(&pStatus->userRecalcLock);
    if (pStatus->recalcPersistOpsInitialized && !pStatus->recalcTransActive &&
        !isListEmpty(&pStatus->recalcPersistOps)) {
      pStatus->recalcTransActive = true;
      claimed = true;
    }
    taosWUnLockLatch(&pStatus->userRecalcLock);
    if (!claimed) {
      if (stopAtCursor) break;
      continue;
    }

    (void)mndStreamRecalcStartClaimed(pMnode, pStatus, false, true, pPostActions);
    ++*pClaimedCount;
    mStreamMgmt.recalcPullupCursor = streamId;
    mStreamMgmt.recalcPullupCursorValid = 1;
    if (stopAtCursor) break;
  }
  if (pIter != NULL) taosHashCancelIterate(pStreamMap, pIter);
}

void mndStreamRecalcPullup(SMnode *pMnode) {
  if (pMnode == NULL) return;

  SStreamRecalcPostActions postActions = {0};
  mndStreamRecalcInitPostActions(&postActions);
  taosRLockLatch(&mStreamMgmt.runtimeLock);
  SHashObj *pStreamMap = mStreamMgmt.streamMap;
  if (atomic_load_8(&mStreamMgmt.active) == 0 || pStreamMap == NULL ||
      atomic_val_compare_exchange_8(&mStreamMgmt.recalcPullupPending, 1, 0) != 1) {
    taosRUnLockLatch(&mStreamMgmt.runtimeLock);
    return;
  }

  int32_t claimedCount = 0;
  if (mStreamMgmt.recalcPullupRoundRemaining <= 0) {
    mStreamMgmt.recalcPullupRoundRemaining = mndStreamRecalcCountClaimable(pStreamMap);
  }
  if (mStreamMgmt.recalcPullupRoundRemaining > 0) {
    const int64_t cursor = mStreamMgmt.recalcPullupCursor;
    const bool    cursorValid = mStreamMgmt.recalcPullupCursorValid != 0;
    const int32_t claimLimit = TMIN(MND_STREAM_RECALC_PULLUP_BATCH_SIZE,
                                    mStreamMgmt.recalcPullupRoundRemaining);
    mndStreamRecalcClaimPass(pMnode, pStreamMap, true, cursor, cursorValid, claimLimit, &claimedCount, &postActions);
    if (claimedCount < claimLimit && cursorValid) {
      mndStreamRecalcClaimPass(pMnode, pStreamMap, false, cursor, true, claimLimit, &claimedCount, &postActions);
    }
    mStreamMgmt.recalcPullupRoundRemaining -= claimedCount;
    // Continue only until every item present at the start of this traversal
    // round has been attempted once. Failures become eligible again in the
    // next periodic round instead of extending the current immediate loop.
    postActions.scheduleNext = postActions.scheduleNext ||
                               (claimedCount > 0 && mStreamMgmt.recalcPullupRoundRemaining > 0);
  }
  taosRUnLockLatch(&mStreamMgmt.runtimeLock);
  mndStreamRecalcRunPostActions(pMnode, &postActions);
}
