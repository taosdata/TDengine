/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#include "mndStream.h"
#include "audit.h"
#include "functionMgt.h"
#include "libs/new-stream/stream.h"
#include "mndDb.h"
#include "mndExtSource.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndStreamRecalc.h"
#include "mndTrans.h"
#include "mndTxn.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "osMemory.h"
#include "parser.h"
#include "taoserror.h"
#include "tglobal.h"
#include "tmisce.h"
#include "tname.h"
#include "tref.h"
#include "ttimer.h"

#define MND_STREAM_MAX_NUM 100000

typedef struct {
  int8_t placeHolder;  // // to fix windows compile error, define place holder
} SMStreamNodeCheckMsg;

typedef enum {
  STREAM_PREFLIGHT_PENDING = 0,
  STREAM_PREFLIGHT_ENQUEUEING,
  STREAM_PREFLIGHT_HANDED_OFF,
  STREAM_PREFLIGHT_PROCESSING,
  STREAM_PREFLIGHT_REPLIED,
  STREAM_PREFLIGHT_REPLIED_BY_QUEUE,
} EStreamPreflightState;

typedef struct {
  int32_t  refSetId;
  int64_t  refId;
  uint64_t nonce;
} SStreamPreflightToken;

typedef struct {
  TdThreadMutex          mutex;
  TdThreadCond           terminalCleanupCond;
  int32_t                refSetId;
  int64_t                refId;
  uint64_t               nonce;
  SMnode                *pMnode;
  SRpcHandleInfo         clientInfo;
  void                  *pCreateReq;
  int32_t                createReqLen;
  void                  *pMetaRsp;
  int32_t                metaRspLen;
  int64_t                transporterId;
  bool                   transporterPublished;
  bool                   transporterReleased;
  bool                   releasePending;
  bool                   removed;
  tmr_h                  timer;
  SStreamPreflightToken *pTimerToken;
  EStreamPreflightState  state;
  bool                   terminalCleanupDone;
} SStreamPreflightEntry;

typedef struct {
  const STableMetaRsp *pMeta;
  bool                 hasCompositePrimaryKey;
  bool                 partitionByTbname;
  bool                 partitionByTag;
  int32_t              code;
} SStreamColumnValidationCtx;

static int32_t  mndNodeCheckSentinel = 0;
SStmRuntime  mStreamMgmt = {0};
static int32_t       mndStreamPreflightRef = -1;
static void         *mndStreamPreflightTimer = NULL;
static int8_t        mndStreamPreflightStopping = 1;
static TdThreadOnce  mndStreamPreflightAdmissionOnce = PTHREAD_ONCE_INIT;
static TdThreadMutex mndStreamPreflightAdmissionMutex;
static int32_t       mndStreamPreflightAdmissionInitCode = TSDB_CODE_SUCCESS;

static int32_t mndStreamActionInsert(SSdb *pSdb, SStreamObj *pStream);
static int32_t mndStreamActionDelete(SSdb *pSdb, SStreamObj *pStream);
static int32_t mndStreamActionUpdate(SSdb *pSdb, SStreamObj *pOldStream, SStreamObj *pNewStream);
static int32_t mndProcessDropStreamReq(SRpcMsg *pReq);

static int32_t mndProcessCreateStreamReqFromMNode(SRpcMsg *pReq);
static int32_t mndCheckStreamOutTableConflict(SMnode *pMnode, const SStreamObj *pStream, txn_id_t txnId);
static int32_t mndMarkStreamTxnCreate(SMnode *pMnode, SRpcMsg *pReq, const SCMCreateStreamReq *pCreate,
                                       SUserObj *pUser, int32_t snodeId, txn_id_t txnId);
static int32_t mndMarkStreamTxnDrop(SMnode *pMnode, SRpcMsg *pReq, SStreamObj *pStream, txn_id_t txnId);
static int32_t mndProcessDropStreamReqFromMNode(SRpcMsg *pReq);
static int32_t mndProcessGetStreamCreateSqlReq(SRpcMsg *pReq);

static int32_t mndRetrieveStream(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextStream(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveStreamTask(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextStreamTask(SMnode *pMnode, void *pIter);
static int32_t mndProcessStopStreamReq(SRpcMsg *pReq);
static int32_t mndProcessStartStreamReq(SRpcMsg *pReq);

static SSdbRow *mndStreamActionDecode(SSdbRaw *pRaw);

static int32_t mndStreamDecodeRecalcPatch(SDecoder *pDecoder, SStreamObj *pStream) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t requestNum = 0;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pStream->name));
  TAOS_CHECK_EXIT(tDecodeU64(pDecoder, &pStream->recalcRevision));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &requestNum));
  if (requestNum < 0 || (uint32_t)requestNum > TD_CODER_REMAIN_CAPACITY(pDecoder) / (4 * sizeof(int64_t))) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }
  if (requestNum > 0) {
    pStream->pIncompleteRecalcs = taosArrayInit(requestNum, sizeof(SStreamRecalcPersistReq));
    TSDB_CHECK_NULL(pStream->pIncompleteRecalcs, code, lino, _exit, terrno);
  }
  for (int32_t i = 0; i < requestNum; ++i) {
    SStreamRecalcPersistReq request = {0};
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &request.recalcId));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &request.start));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &request.end));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &request.requestTimeMs));
    if (request.recalcId == 0 || request.end <= request.start || request.requestTimeMs <= 0) {
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    }
    if (taosArrayPush(pStream->pIncompleteRecalcs, &request) == NULL) TAOS_CHECK_EXIT(terrno);
  }

_exit:
  tEndDecode(pDecoder);
  return code;
}

SSdbRaw       *mndStreamSeqActionEncode(SStreamObj *pStream);
SSdbRow       *mndStreamSeqActionDecode(SSdbRaw *pRaw);
static int32_t mndStreamSeqActionInsert(SSdb *pSdb, SStreamSeq *pStream);
static int32_t mndStreamSeqActionDelete(SSdb *pSdb, SStreamSeq *pStream);
static int32_t mndStreamSeqActionUpdate(SSdb *pSdb, SStreamSeq *pOldStream, SStreamSeq *pNewStream);
static int32_t mndProcessCreateStreamReq(SRpcMsg *pReq);
static int32_t mndProcessCreateStreamContinuation(SRpcMsg *pReq);
static int32_t mndProcessCreateStreamFinal(SRpcMsg *pReq, SCMCreateStreamReq *pCreate);

static void mndStreamInitPreflightAdmission(void) {
  mndStreamPreflightAdmissionInitCode = taosThreadMutexInit(&mndStreamPreflightAdmissionMutex, NULL);
}

static int32_t mndStreamEnsurePreflightAdmission(void) {
  int32_t code = taosThreadOnce(&mndStreamPreflightAdmissionOnce, mndStreamInitPreflightAdmission);
  return code == TSDB_CODE_SUCCESS ? mndStreamPreflightAdmissionInitCode : code;
}

static void mndStreamDestroyPreflightEntry(void *pData) {
  SStreamPreflightEntry *pEntry = pData;
  if (pEntry == NULL) return;

  taosMemoryFreeClear(pEntry->pCreateReq);
  taosMemoryFreeClear(pEntry->pMetaRsp);
  (void)taosThreadCondDestroy(&pEntry->terminalCleanupCond);
  (void)taosThreadMutexDestroy(&pEntry->mutex);
  taosMemoryFree(pEntry);
}

static void mndStreamReleasePreflightToken(void *pData) {
  SStreamPreflightToken *pToken = pData;
  if (pToken == NULL) return;
  TAOS_UNUSED(taosReleaseRef(pToken->refSetId, pToken->refId));
  taosMemoryFree(pToken);
}

static SStreamPreflightToken *mndStreamCreatePreflightToken(SStreamPreflightEntry *pEntry) {
  SStreamPreflightToken *pToken = taosMemoryMalloc(sizeof(*pToken));
  if (pToken == NULL) return NULL;
  if (taosAcquireRef(pEntry->refSetId, pEntry->refId) == NULL) {
    taosMemoryFree(pToken);
    return NULL;
  }
  *pToken = (SStreamPreflightToken){
      .refSetId = pEntry->refSetId,
      .refId = pEntry->refId,
      .nonce = pEntry->nonce,
  };
  return pToken;
}

static SStreamPreflightEntry *mndStreamAcquirePreflight(const SStreamPreflightToken *pToken) {
  if (pToken == NULL) return NULL;
  SStreamPreflightEntry *pEntry = taosAcquireRef(pToken->refSetId, pToken->refId);
  if (pEntry == NULL) return NULL;
  if (pEntry->nonce != pToken->nonce) {
    TAOS_UNUSED(taosReleaseRef(pToken->refSetId, pToken->refId));
    return NULL;
  }
  return pEntry;
}

static void mndStreamSendPreflightResponse(const SRpcHandleInfo *pInfo, int32_t code) {
  SRpcMsg rsp = {.code = code, .info = *pInfo};
  TAOS_UNUSED(rpcSendResponse(&rsp));
}

static void mndStreamCleanupPreflight(SStreamPreflightEntry *pEntry, bool fromTimer, bool terminal) {
  int64_t                transporterId = 0;
  tmr_h                  timer = NULL;
  SStreamPreflightToken *pTimerToken = NULL;
  bool                   remove = false;

  (void)taosThreadMutexLock(&pEntry->mutex);
  if (fromTimer) {
    pEntry->timer = NULL;
    pEntry->pTimerToken = NULL;
  } else {
    timer = pEntry->timer;
    pTimerToken = pEntry->pTimerToken;
    pEntry->timer = NULL;
    pEntry->pTimerToken = NULL;
  }
  if (pEntry->transporterPublished) {
    if (!pEntry->transporterReleased && pEntry->transporterId > 0) {
      pEntry->transporterReleased = true;
      transporterId = pEntry->transporterId;
    }
  } else {
    pEntry->releasePending = true;
  }
  if (terminal && !pEntry->removed) {
    pEntry->removed = true;
    remove = true;
  }
  (void)taosThreadMutexUnlock(&pEntry->mutex);

  if (timer != NULL && pTimerToken != NULL) {
    if (taosTmrStopA(&timer)) {
      mndStreamReleasePreflightToken(pTimerToken);
    }
  }
  if (transporterId > 0) {
    (void)asyncFreeConnById(pEntry->pMnode->msgCb.clientRpc, transporterId);
  }
  if (remove) {
    TAOS_UNUSED(taosRemoveRef(pEntry->refSetId, pEntry->refId));
  }

  if (terminal) {
    (void)taosThreadMutexLock(&pEntry->mutex);
    pEntry->terminalCleanupDone = true;
    (void)taosThreadCondBroadcast(&pEntry->terminalCleanupCond);
    (void)taosThreadMutexUnlock(&pEntry->mutex);
  }
}

static void mndStreamCleanupPreflightTerminal(SStreamPreflightEntry *pEntry, bool fromTimer) {
  mndStreamCleanupPreflight(pEntry, fromTimer, true);
}

static void mndStreamCleanupPreflightHandoff(SStreamPreflightEntry *pEntry) {
  mndStreamCleanupPreflight(pEntry, false, false);
}

static bool mndStreamFailPendingPreflight(SStreamPreflightEntry *pEntry, int32_t code, bool fromTimer) {
  SRpcHandleInfo clientInfo = {0};
  bool           won = false;

  (void)taosThreadMutexLock(&pEntry->mutex);
  if (pEntry->state == STREAM_PREFLIGHT_PENDING) {
    pEntry->state = STREAM_PREFLIGHT_REPLIED;
    clientInfo = pEntry->clientInfo;
    won = true;
  }
  (void)taosThreadMutexUnlock(&pEntry->mutex);

  if (won) {
    mndStreamCleanupPreflightTerminal(pEntry, fromTimer);
    mndStreamSendPreflightResponse(&clientInfo, code);
  }
  return won;
}

static int32_t mndStreamBuildContinuationPayload(const SStreamPreflightEntry *pEntry, void **ppPayload,
                                                 int32_t *pPayloadLen) {
  if (pEntry == NULL || ppPayload == NULL || pPayloadLen == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }

  void *pPayload = rpcMallocCont(sizeof(SStreamPreflightToken));
  if (pPayload == NULL) return terrno;
  const SStreamPreflightToken token = {
      .refSetId = pEntry->refSetId,
      .refId = pEntry->refId,
      .nonce = pEntry->nonce,
  };
  memcpy(pPayload, &token, sizeof(token));
  *ppPayload = pPayload;
  *pPayloadLen = sizeof(SStreamPreflightToken);
  return TSDB_CODE_SUCCESS;
}

static int32_t mndStreamDecodeContinuationPayload(const void *pPayload, int32_t payloadLen,
                                                  SStreamPreflightToken *pToken) {
  if (pPayload == NULL || payloadLen != (int32_t)sizeof(SStreamPreflightToken) || pToken == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }
  memcpy(pToken, pPayload, sizeof(*pToken));
  if (pToken->refSetId != mndStreamPreflightRef || pToken->refId <= 0 || pToken->nonce == 0) {
    return TSDB_CODE_INVALID_MSG;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t mndStreamCreateQueuedPreflight(SMnode *pMnode, const SRpcHandleInfo *pClientInfo, const void *pCreateReq,
                                              int32_t createReqLen, const void *pMetaRsp, int32_t metaRspLen,
                                              SStreamPreflightEntry **ppEntry) {
  if (pMnode == NULL || pClientInfo == NULL || pCreateReq == NULL || createReqLen <= 0 || pMetaRsp == NULL ||
      metaRspLen <= 0 || ppEntry == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  *ppEntry = NULL;

  SStreamPreflightEntry *pEntry = taosMemoryCalloc(1, sizeof(*pEntry));
  if (pEntry == NULL) return terrno;
  if (taosThreadMutexInit(&pEntry->mutex, NULL) != 0) {
    taosMemoryFree(pEntry);
    return terrno;
  }
  if (taosThreadCondInit(&pEntry->terminalCleanupCond, NULL) != 0) {
    (void)taosThreadMutexDestroy(&pEntry->mutex);
    taosMemoryFree(pEntry);
    return terrno;
  }
  pEntry->pCreateReq = taosMemoryMalloc(createReqLen);
  pEntry->pMetaRsp = taosMemoryMalloc(metaRspLen);
  if (pEntry->pCreateReq == NULL || pEntry->pMetaRsp == NULL) {
    mndStreamDestroyPreflightEntry(pEntry);
    return terrno;
  }
  memcpy(pEntry->pCreateReq, pCreateReq, createReqLen);
  memcpy(pEntry->pMetaRsp, pMetaRsp, metaRspLen);
  pEntry->pMnode = pMnode;
  pEntry->clientInfo = *pClientInfo;
  pEntry->createReqLen = createReqLen;
  pEntry->metaRspLen = metaRspLen;
  pEntry->state = STREAM_PREFLIGHT_ENQUEUEING;
  pEntry->nonce = ((uint64_t)taosSafeRand() << 32) | taosSafeRand();
  if (pEntry->nonce == 0) pEntry->nonce = 1;

  (void)taosThreadMutexLock(&mndStreamPreflightAdmissionMutex);
  if (atomic_load_8(&mndStreamPreflightStopping)) {
    (void)taosThreadMutexUnlock(&mndStreamPreflightAdmissionMutex);
    mndStreamDestroyPreflightEntry(pEntry);
    return TSDB_CODE_APP_IS_STOPPING;
  }
  pEntry->refSetId = mndStreamPreflightRef;
  pEntry->refId = taosAddRef(pEntry->refSetId, pEntry);
  if (pEntry->refId <= 0) {
    (void)taosThreadMutexUnlock(&mndStreamPreflightAdmissionMutex);
    mndStreamDestroyPreflightEntry(pEntry);
    return terrno;
  }
  SStreamPreflightEntry *pLocalEntry = taosAcquireRef(pEntry->refSetId, pEntry->refId);
  if (pLocalEntry == NULL) {
    TAOS_UNUSED(taosRemoveRef(pEntry->refSetId, pEntry->refId));
    (void)taosThreadMutexUnlock(&mndStreamPreflightAdmissionMutex);
    return terrno;
  }
  (void)taosThreadMutexUnlock(&mndStreamPreflightAdmissionMutex);
  *ppEntry = pLocalEntry;
  return TSDB_CODE_SUCCESS;
}

static void mndStreamPreflightTimeout(void *param, void *tmrId) {
  SStreamPreflightToken *pToken = param;
  SStreamPreflightEntry *pEntry = mndStreamAcquirePreflight(pToken);
  if (pEntry != NULL) {
    (void)mndStreamFailPendingPreflight(pEntry, TSDB_CODE_RPC_TIMEOUT, true);
    TAOS_UNUSED(taosReleaseRef(pToken->refSetId, pToken->refId));
  }
  mndStreamReleasePreflightToken(pToken);
}

void mndCleanupStream(SMnode *pMnode) {
  mDebug("try to clean up stream");

  if (mndStreamEnsurePreflightAdmission() == TSDB_CODE_SUCCESS) {
    (void)taosThreadMutexLock(&mndStreamPreflightAdmissionMutex);
    atomic_store_8(&mndStreamPreflightStopping, 1);
    (void)taosThreadMutexUnlock(&mndStreamPreflightAdmissionMutex);
  } else {
    atomic_store_8(&mndStreamPreflightStopping, 1);
  }
  if (mndStreamPreflightRef >= 0) {
    SStreamPreflightEntry *pEntry = NULL;
    while ((pEntry = taosIterateRef(mndStreamPreflightRef, 0)) != NULL) {
      const int64_t refId = pEntry->refId;
      bool           sendResponse = false;
      bool           cleanup = false;
      SRpcHandleInfo clientInfo = {0};
      (void)taosThreadMutexLock(&pEntry->mutex);
      while (!pEntry->terminalCleanupDone && pEntry->state != STREAM_PREFLIGHT_PENDING &&
             pEntry->state != STREAM_PREFLIGHT_HANDED_OFF) {
        (void)taosThreadCondWait(&pEntry->terminalCleanupCond, &pEntry->mutex);
      }
      if (!pEntry->terminalCleanupDone && pEntry->state == STREAM_PREFLIGHT_PENDING) {
        pEntry->state = STREAM_PREFLIGHT_REPLIED;
        clientInfo = pEntry->clientInfo;
        sendResponse = true;
        cleanup = true;
      } else if (!pEntry->terminalCleanupDone && pEntry->state == STREAM_PREFLIGHT_HANDED_OFF) {
        pEntry->state = STREAM_PREFLIGHT_REPLIED_BY_QUEUE;
        cleanup = true;
      }
      (void)taosThreadMutexUnlock(&pEntry->mutex);
      if (cleanup) mndStreamCleanupPreflightTerminal(pEntry, false);
      if (sendResponse) mndStreamSendPreflightResponse(&clientInfo, TSDB_CODE_APP_IS_STOPPING);
      TAOS_UNUSED(taosReleaseRef(mndStreamPreflightRef, refId));
    }
  }
  if (mndStreamPreflightTimer != NULL) {
    taosTmrCleanUp(mndStreamPreflightTimer);
    mndStreamPreflightTimer = NULL;
  }
  if (mndStreamPreflightRef >= 0) {
    taosCloseRef(mndStreamPreflightRef);
    mndStreamPreflightRef = -1;
  }

  msmHandleBecomeNotLeader(pMnode);

  mDebug("mnd stream runtime info cleanup");
}

SSdbRow *mndStreamActionDecode(SSdbRaw *pRaw) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SSdbRow    *pRow = NULL;
  SStreamObj *pStream = NULL;
  void       *buf = NULL;
  int8_t      sver = 0;
  int32_t     tlen;
  int32_t     dataPos = 0;

  code = sdbGetRawSoftVer(pRaw, &sver);
  TSDB_CHECK_CODE(code, lino, _over);

  if (sver > MND_STREAM_VER_NUMBER) {
    mError("stream read invalid ver, data ver: %d, curr ver: %d", sver, MND_STREAM_VER_NUMBER);
    goto _over;
  }

  pRow = sdbAllocRow(sizeof(SStreamObj));
  TSDB_CHECK_NULL(pRow, code, lino, _over, terrno);

  pStream = sdbGetRowObj(pRow);
  TSDB_CHECK_NULL(pStream, code, lino, _over, terrno);

  SDB_GET_INT32(pRaw, dataPos, &tlen, _over);

  if (tlen <= 0 || tlen >= INT32_MAX || tlen > pRaw->dataLen - dataPos) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _over);
  }

  buf = taosMemoryMalloc(tlen + 1);
  TSDB_CHECK_NULL(buf, code, lino, _over, terrno);

  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, _over);

  int32_t remaining = pRaw->dataLen - dataPos;
  if (remaining == 0) {
    pStream->sdbRawUpdateKind = MND_STREAM_RAW_UPDATE_FULL;
  } else if (remaining == sizeof(int8_t)) {
    SDB_GET_INT8(pRaw, dataPos, &pStream->sdbRawUpdateKind, _over);
    if (pStream->sdbRawUpdateKind != MND_STREAM_RAW_UPDATE_FULL &&
        pStream->sdbRawUpdateKind != MND_STREAM_RAW_UPDATE_RECALC_PATCH) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, _over);
    }
  } else {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _over);
  }

  SDecoder decoder;
  tDecoderInit(&decoder, buf, tlen + 1);
  if (pStream->sdbRawUpdateKind == MND_STREAM_RAW_UPDATE_RECALC_PATCH) {
    code = mndStreamDecodeRecalcPatch(&decoder, pStream);
  } else {
    code = tDecodeSStreamObj(&decoder, pStream, sver);
  }
  tDecoderClear(&decoder);

  if (code < 0) {
    tFreeStreamObj(pStream);
  }

_over:
  taosMemoryFreeClear(buf);

  if (code != TSDB_CODE_SUCCESS) {
    char *p = (pStream == NULL || NULL == pStream->pCreate) ? "null" : pStream->pCreate->name;
    mError("stream:%s, failed to decode from raw:%p since %s at:%d", p, pRaw, tstrerror(code), lino);
    taosMemoryFreeClear(pRow);

    terrno = code;
    return NULL;
  } else {
    mTrace("stream:%s, decode from raw:%p, row:%p", pStream->name, pRaw, pStream);

    terrno = 0;
    return pRow;
  }
}

static int32_t mndStreamActionInsert(SSdb *pSdb, SStreamObj *pStream) {
  mTrace("stream:%s, perform insert action", pStream->name);
  if (pStream->sdbRawUpdateKind == MND_STREAM_RAW_UPDATE_RECALC_PATCH) {
    return TSDB_CODE_SDB_OBJ_NOT_THERE;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t mndStreamActionDelete(SSdb *pSdb, SStreamObj *pStream) {
  mInfo("stream:%s, perform delete action", pStream->name);
  tFreeStreamObj(pStream);
  return 0;
}

static int32_t mndStreamApplyRecalcPatch(SStreamObj *pOldStream, const SStreamObj *pPatch) {
  taosRLockLatch(&pOldStream->lock);
  bool newerRecalcRevision = pPatch->recalcRevision > pOldStream->recalcRevision;
  taosRUnLockLatch(&pOldStream->lock);
  if (!newerRecalcRevision) return TSDB_CODE_SUCCESS;

  SArray *pRequests = pPatch->pIncompleteRecalcs == NULL ? NULL : taosArrayDup(pPatch->pIncompleteRecalcs, NULL);
  if (pPatch->pIncompleteRecalcs != NULL && pRequests == NULL) return terrno;

  taosWLockLatch(&pOldStream->lock);
  if (pPatch->recalcRevision > pOldStream->recalcRevision) {
    SArray *pOldRequests = pOldStream->pIncompleteRecalcs;
    pOldStream->pIncompleteRecalcs = pRequests;
    pOldStream->recalcRevision = pPatch->recalcRevision;
    pRequests = NULL;
    taosArrayDestroy(pOldRequests);
  }
  taosWUnLockLatch(&pOldStream->lock);
  taosArrayDestroy(pRequests);

  return TSDB_CODE_SUCCESS;
}

static int32_t mndStreamActionUpdate(SSdb *pSdb, SStreamObj *pOldStream, SStreamObj *pNewStream) {
  mTrace("stream:%s, perform update action", pOldStream->pCreate->name);

  if (pNewStream->sdbRawUpdateKind == MND_STREAM_RAW_UPDATE_RECALC_PATCH) {
    return mndStreamApplyRecalcPatch(pOldStream, pNewStream);
  }

  taosRLockLatch(&pOldStream->lock);
  bool recalcMayWin = pNewStream->recalcRevision > pOldStream->recalcRevision;
  taosRUnLockLatch(&pOldStream->lock);

  SArray *pRequests = recalcMayWin && pNewStream->pIncompleteRecalcs != NULL
                          ? taosArrayDup(pNewStream->pIncompleteRecalcs, NULL)
                          : NULL;
  if (recalcMayWin && pNewStream->pIncompleteRecalcs != NULL && pRequests == NULL) return terrno;

  SArray *pOldRequests = NULL;
  taosWLockLatch(&pOldStream->lock);
  atomic_store_32(&pOldStream->mainSnodeId, pNewStream->mainSnodeId);
  atomic_store_8(&pOldStream->userStopped, atomic_load_8(&pNewStream->userStopped));
  pOldStream->ownerId = pNewStream->ownerId;
  pOldStream->updateTime = pNewStream->updateTime;
  if (recalcMayWin && pNewStream->recalcRevision > pOldStream->recalcRevision) {
    pOldRequests = pOldStream->pIncompleteRecalcs;
    pOldStream->pIncompleteRecalcs = pRequests;
    pOldStream->recalcRevision = pNewStream->recalcRevision;
    pRequests = NULL;
  }
  // batch-meta-txn (stream phase 2): the txn markers must be carried over, otherwise a
  // PRE_CREATE/PRE_DROP marker write - or the marker clear at COMMIT - lands in SDB but
  // is dropped from the in-memory object, leaving a stale txnId that makes every later
  // DDL on this stream fail with TSDB_CODE_TXN_RESOURCE_BUSY. Mirrors pOld->txnId =
  // pNew->txnId in mndStbActionUpdate.
  pOldStream->txnId = pNewStream->txnId;
  pOldStream->txnStatus = pNewStream->txnStatus;
  taosWUnLockLatch(&pOldStream->lock);

  taosArrayDestroy(pOldRequests);
  taosArrayDestroy(pRequests);
  return TSDB_CODE_SUCCESS;
}

int32_t mndAcquireStream(SMnode *pMnode, char *streamName, SStreamObj **pStream) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  (*pStream) = sdbAcquire(pSdb, SDB_STREAM, streamName);
  if ((*pStream) == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    code = TSDB_CODE_MND_STREAM_NOT_EXIST;
  }
  return code;
}

static bool mndStreamGetNameFromId(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SStreamObj* pStream = pObj;

  if (pStream->pCreate->streamId == *(int64_t*)p1) {
    tstrncpy((char *)p2, pStream->name, TSDB_STREAM_NAME_LEN);
    return false;
  }

  return true;
}

int32_t mndAcquireStreamById(SMnode *pMnode, int64_t streamId, SStreamObj **pStream) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  char streamName[TSDB_STREAM_NAME_LEN];
  streamName[0] = 0;
  
  sdbTraverse(pSdb, SDB_STREAM, mndStreamGetNameFromId, &streamId, streamName, NULL);
  if (streamName[0]) {
    (*pStream) = sdbAcquire(pSdb, SDB_STREAM, streamName);
    if ((*pStream) == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
      code = TSDB_CODE_MND_STREAM_NOT_EXIST;
    }
  }
  
  return code;
}

void mndReleaseStream(SMnode *pMnode, SStreamObj *pStream) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pStream);
}

SSdbRaw *mndStreamSeqActionEncode(SStreamObj *pStream) { return NULL; }
SSdbRow *mndStreamSeqActionDecode(SSdbRaw *pRaw) { return NULL; }
int32_t  mndStreamSeqActionInsert(SSdb *pSdb, SStreamSeq *pStream) { return 0; }
int32_t  mndStreamSeqActionDelete(SSdb *pSdb, SStreamSeq *pStream) { return 0; }
int32_t  mndStreamSeqActionUpdate(SSdb *pSdb, SStreamSeq *pOldStream, SStreamSeq *pNewStream) { return 0; }

static void mndStreamBuildObj(SMnode *pMnode, SStreamObj *pObj, SCMCreateStreamReq *pCreate, SUserObj *pOperUser,
                              int32_t snodeId) {
  int32_t code = 0;

  pObj->pCreate = pCreate;
  tstrncpy(pObj->name, pCreate->name, sizeof(pObj->name));
  (void)snprintf(pObj->createUser, sizeof(pObj->createUser), "%s", pOperUser->name);
  pObj->ownerId = pOperUser->uid;
  pObj->mainSnodeId = snodeId;

  pObj->userDropped = 0;
  pObj->userStopped = 0;

  pObj->createTime = taosGetTimestampMs();
  pObj->updateTime = pObj->createTime;

  // P1 B1: lift the taosc-side flag CREATE_STREAM_FLAG_REF_EXT_SOURCE
  // (set in buildCreateStreamReq when the stream references any EXTERNAL
  // SOURCE table) into the SStreamObj flag STREAM_FLAG_REF_EXT_SOURCE.
  // Downstream:
  //   - msmAssignTaskSnodeId (P1 B4) picks the snode with the least EXT
  //     reader count when this bit is set.
  //   - msmTDAdd*ReaderTasks (P1 B5) attach the matching extSpec to the
  //     STREAM_TASK_DEPLOY message.
  // See DS Sec 6.1.1 / Sec 6.1.2.
  if (pCreate->flags & CREATE_STREAM_FLAG_REF_EXT_SOURCE) {
    pObj->flags |= STREAM_FLAG_REF_EXT_SOURCE;
    mDebug("stream:%s flagged STREAM_FLAG_REF_EXT_SOURCE (federated query stream)", pObj->name);
  }

  if (pCreate->extSpecs != NULL) {
    mDebug("stream:%s references %d ext spec(s)", pObj->name, (int)taosArrayGetSize(pCreate->extSpecs));
  }

  mstLogSStreamObj("create stream", pObj);
}

static int32_t mndStreamCreateOutStb(SMnode *pMnode, STrans *pTrans, const SCMCreateStreamReq *pStream, const char *user,
                                     txn_id_t txnId) {
  SStbObj *pStb = NULL;
  SDbObj  *pDb = NULL;
  int32_t  code = 0;
  int32_t  lino = 0;

  SMCreateStbReq createReq = {0};
  // batch-meta-txn (stream phase 2): inside a txn the output STB must be created with the
  // txn markers set, so it stays invisible to other sessions and ROLLBACK can undo it.
  // mndBuildStbFromReq propagates txnId into SStbObj.txnId/txnStatus (mndStb.c:1301).
  createReq.txnId = (int64_t)txnId;
  TAOS_STRNCAT(createReq.name, pStream->outDB, TSDB_DB_FNAME_LEN);
  TAOS_STRNCAT(createReq.name, ".", 2);
  TAOS_STRNCAT(createReq.name,  pStream->outTblName, TSDB_TABLE_NAME_LEN);
  createReq.numOfColumns = taosArrayGetSize(pStream->outCols);
  createReq.numOfTags = pStream->outTags ? taosArrayGetSize(pStream->outTags) : 1;
  createReq.pColumns = taosArrayInit_s(sizeof(SFieldWithOptions), createReq.numOfColumns);
  TSDB_CHECK_NULL(createReq.pColumns, code, lino, _OVER, terrno);

  // build fields
  for (int32_t i = 0; i < createReq.numOfColumns; i++) {
    SFieldWithOptions *pField = taosArrayGet(createReq.pColumns, i);
    TSDB_CHECK_NULL(pField, code, lino, _OVER, terrno);
    SFieldWithOptions *pSrc = taosArrayGet(pStream->outCols, i);

    tstrncpy(pField->name, pSrc->name, TSDB_COL_NAME_LEN);
    pField->flags = pSrc->flags;
    pField->type = pSrc->type;
    pField->bytes = pSrc->bytes;
    pField->compress = createDefaultColCmprByType(pField->type);
    if (IS_DECIMAL_TYPE(pField->type)) {
      pField->typeMod = pSrc->typeMod;
      pField->flags |= COL_HAS_TYPE_MOD;
    }
  }

  if (NULL == pStream->outTags) {
    createReq.numOfTags = 1;
    createReq.pTags = taosArrayInit_s(sizeof(SField), 1);
    TSDB_CHECK_NULL(createReq.pTags, code, lino, _OVER, terrno);

    // build tags
    SField *pField = taosArrayGet(createReq.pTags, 0);
    TSDB_CHECK_NULL(pField, code, lino, _OVER, terrno);

    tstrncpy(pField->name, "group_id", sizeof(pField->name));
    pField->type = TSDB_DATA_TYPE_UBIGINT;
    pField->flags = 0;
    pField->bytes = 8;
  } else {
    createReq.numOfTags = taosArrayGetSize(pStream->outTags);
    createReq.pTags = taosArrayInit_s(sizeof(SField), createReq.numOfTags);
    TSDB_CHECK_NULL(createReq.pTags, code, lino, _OVER, terrno);

    for (int32_t i = 0; i < createReq.numOfTags; i++) {
      SField *pField = taosArrayGet(createReq.pTags, i);
      if (pField == NULL) {
        continue;
      }

      TAOS_FIELD_E *pSrc = taosArrayGet(pStream->outTags, i);
      pField->bytes = pSrc->bytes;
      pField->flags = 0;
      pField->type = pSrc->type;
      tstrncpy(pField->name, pSrc->name, TSDB_COL_NAME_LEN);
    }
  }

  if ((code = mndCheckCreateStbReq(&createReq)) != 0) {
    goto _OVER;
  }

  pStb = mndAcquireStb(pMnode, createReq.name);
  if (pStb != NULL) {
    code = TSDB_CODE_MND_STB_ALREADY_EXIST;
    goto _OVER;
  }

  pDb = mndAcquireDbByStb(pMnode, createReq.name);
  if (pDb == NULL) {
    code = TSDB_CODE_MND_DB_NOT_SELECTED;
    goto _OVER;
  }

  int32_t numOfStbs = -1;
  if (mndGetNumOfStbs(pMnode, pDb->name, &numOfStbs) != 0) {
    goto _OVER;
  }

  if (pDb->cfg.numOfStables == 1 && numOfStbs != 0) {
    code = TSDB_CODE_MND_SINGLE_STB_MODE_DB;
    goto _OVER;
  }

  SStbObj stbObj = {0};

  if (mndBuildStbFromReq(pMnode, &stbObj, &createReq, pDb) != 0) {
    goto _OVER;
  }

  stbObj.uid = pStream->outStbUid;

  if (mndAddStbToTrans(pMnode, pTrans, pDb, &stbObj) < 0) {
    mndFreeStb(&stbObj);
    goto _OVER;
  }

  mDebug("stream:%s create dst stable:%s, cols:%d", pStream->name, pStream->outTblName, createReq.numOfColumns);

  tFreeSMCreateStbReq(&createReq);
  mndFreeStb(&stbObj);
  mndReleaseStb(pMnode, pStb);
  mndReleaseDb(pMnode, pDb);
  return code;

_OVER:
  tFreeSMCreateStbReq(&createReq);
  mndReleaseStb(pMnode, pStb);
  mndReleaseDb(pMnode, pDb);

  mDebug("stream:%s failed to create dst stable:%s, line:%d code:%s", pStream->name, pStream->outTblName, lino,
         tstrerror(code));
  return code;
}

static int32_t mndStreamCreateOutTable(SMnode *pMnode, STrans *pTrans, const SCMCreateStreamReq *pStream,
                                       txn_id_t txnId) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SVgObj  *pVgroup = NULL;
  SDbObj  *pDb = NULL;
  SName    name = {0};
  char     dbFName[TSDB_DB_FNAME_LEN] = {0};

  // Parse database and table name
  if ((code = tNameFromString(&name, pStream->outDB, T_NAME_ACCT | T_NAME_DB)) != 0) {
    mError("stream:%s failed to parse outDB:%s, code:%s", pStream->name, pStream->outDB, tstrerror(code));
    return code;
  }
  if ((code = tNameGetFullDbName(&name, dbFName)) != 0) {
    mError("stream:%s failed to get full db name, code:%s", pStream->name, tstrerror(code));
    return code;
  }

  // Get database object
  pDb = mndAcquireDb(pMnode, dbFName);
  if (pDb == NULL) {
    code = TSDB_CODE_MND_DB_NOT_SELECTED;
    mError("stream:%s failed to acquire db:%s, code:%s", pStream->name, dbFName, tstrerror(code));
    return code;
  }

  // Set transaction db name and check conflict (similar to mndAddStbToTrans)
  mndTransSetDbName(pTrans, pDb->name, pStream->outTblName);
  code = mndTransCheckConflict(pMnode, pTrans);
  if (code != TSDB_CODE_SUCCESS) {
    mError("stream:%s failed to check conflict, code:%s", pStream->name, tstrerror(code));
    goto _OVER;
  }

  // Get vgroup by vgId
  if (pStream->outTblVgId <= 0) {
    mError("stream:%s invalid outTblVgId:%d", pStream->name, pStream->outTblVgId);
    code = TSDB_CODE_MND_VGROUP_NOT_EXIST;
    goto _OVER;
  }

  pVgroup = mndAcquireVgroup(pMnode, pStream->outTblVgId);
  if (pVgroup == NULL) {
    code = TSDB_CODE_MND_VGROUP_NOT_EXIST;
    mError("stream:%s failed to acquire vgroup:%d, code:%s", pStream->name, pStream->outTblVgId, tstrerror(code));
    goto _OVER;
  }

  // Verify vgroup belongs to the database
  if (pVgroup->dbUid != pDb->uid) {
    code = TSDB_CODE_MND_VGROUP_NOT_EXIST;
    mError("stream:%s vgroup:%d does not belong to db:%s", pStream->name, pStream->outTblVgId, dbFName);
    goto _OVER;
  }

  // Build SVCreateTbReq (reusing logic from buildNormalTableCreateReq)
  SVCreateTbReq createReq = {0};
  createReq.type = TSDB_NORMAL_TABLE;
  createReq.flags = TD_CREATE_NORMAL_TB_IN_STREAM | TD_CREATE_IF_NOT_EXISTS;
  createReq.uid = mndGenerateUid(pStream->outTblName, strlen(pStream->outTblName));
  createReq.btime = taosGetTimestampMs();
  createReq.ttl = TSDB_DEFAULT_TABLE_TTL;
  // batch-meta-txn (stream phase 2): inside a txn the output table must be created with
  // the txn marker, so vnodeProcessCreateTbReq writes it as META_TXN_PRE_CREATE - visible
  // to this txn only, undone by ROLLBACK. flags stay as-is; the txn branch in vnodeSvr.c
  // does not consult TD_CREATE_IF_NOT_EXISTS.
  createReq.txnId = (int64_t)txnId;
  createReq.commentLen = -1;
  createReq.name = taosStrdup(pStream->outTblName);
  if (createReq.name == NULL) {
    code = terrno;
    goto _OVER;
  }

  // Build schema from outCols (same logic as buildNormalTableCreateReq)
  int32_t numOfCols = taosArrayGetSize(pStream->outCols);
  createReq.ntb.schemaRow.nCols = numOfCols;
  createReq.ntb.schemaRow.version = 1;
  createReq.ntb.schemaRow.pSchema = taosMemoryCalloc(numOfCols, sizeof(SSchema));
  if (createReq.ntb.schemaRow.pSchema == NULL) {
    code = terrno;
    goto _OVER;
  }

  for (int32_t i = 0; i < numOfCols; i++) {
    SFieldWithOptions *pField = taosArrayGet(pStream->outCols, i);
    if (pField == NULL) {
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      goto _OVER;
    }

    createReq.ntb.schemaRow.pSchema[i].colId = i + 1;
    createReq.ntb.schemaRow.pSchema[i].type = pField->type;
    createReq.ntb.schemaRow.pSchema[i].bytes = pField->bytes;
    createReq.ntb.schemaRow.pSchema[i].flags = pField->flags;
    tstrncpy(createReq.ntb.schemaRow.pSchema[i].name, pField->name, TSDB_COL_NAME_LEN);

    if (IS_DECIMAL_TYPE(pField->type)) {
      if (createReq.pExtSchemas == NULL) {
        createReq.pExtSchemas = taosMemoryCalloc(numOfCols, sizeof(SExtSchema));
        if (createReq.pExtSchemas == NULL) {
          code = terrno;
          goto _OVER;
        }
      }
      createReq.pExtSchemas[i].typeMod = pField->typeMod;
    }
  }

  // Initialize colCmpr with default encode/compress/level per column type
  code = tInitDefaultSColCmprWrapperByCols(&createReq.colCmpr, numOfCols);
  if (code != TSDB_CODE_SUCCESS) {
    goto _OVER;
  }
  for (int32_t i = 0; i < numOfCols; i++) {
    SSchema *pSchema = &createReq.ntb.schemaRow.pSchema[i];
    createReq.colCmpr.pColCmpr[i].id = pSchema->colId;
    createReq.colCmpr.pColCmpr[i].alg = createDefaultColCmprByType(pSchema->type);
  }

  // Build SVCreateTbBatchReq (vnode expects batch request)
  SVCreateTbBatchReq batchReq = {0};
  batchReq.nReqs = 1;
  batchReq.pArray = taosArrayInit(1, sizeof(SVCreateTbReq));
  if (batchReq.pArray == NULL) {
    code = terrno;
    goto _OVER;
  }
  if (taosArrayPush(batchReq.pArray, &createReq) == NULL) {
    code = terrno;
    taosArrayDestroy(batchReq.pArray);
    goto _OVER;
  }
  batchReq.source = TD_REQ_FROM_APP;

  // Serialize the batch request
  int32_t contLen = 0;
  int32_t ret = 0;
  tEncodeSize(tEncodeSVCreateTbBatchReq, &batchReq, contLen, ret);
  if (ret < 0) {
    code = terrno;
    taosArrayDestroy(batchReq.pArray);
    goto _OVER;
  }

  contLen += sizeof(SMsgHead);

  SMsgHead *pHead = taosMemoryCalloc(1, contLen);
  if (pHead == NULL) {
    code = terrno;
    taosArrayDestroy(batchReq.pArray);
    goto _OVER;
  }
  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(pVgroup->vgId);

  SEncoder encoder = {0};
  void *pBuf = POINTER_SHIFT(pHead, sizeof(SMsgHead));
  tEncoderInit(&encoder, pBuf, contLen - sizeof(SMsgHead));
  code = tEncodeSVCreateTbBatchReq(&encoder, &batchReq);
  tEncoderClear(&encoder);
  taosArrayDestroy(batchReq.pArray);
  if (code < 0) {
    taosMemoryFree(pHead);
    goto _OVER;
  }

  // Add to transaction redo actions
  STransAction action = {0};
  action.mTraceId = pTrans->mTraceId;
  action.epSet = mndGetVgroupEpset(pMnode, pVgroup);
  action.pCont = pHead;
  action.contLen = contLen;
  action.msgType = TDMT_VND_CREATE_TABLE;
  action.acceptableCode = TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
  action.retryCode = TSDB_CODE_TDB_TABLE_NOT_EXIST;
  action.groupId = pVgroup->vgId;

  code = mndTransAppendRedoAction(pTrans, &action);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pHead);
    goto _OVER;
  }

  // Build undo action (drop table if transaction fails)
  SVDropTbReq dropReq = {0};
  dropReq.name = createReq.name;  // vnode metaCheckDropTableReq requires name
  dropReq.uid = createReq.uid;
  dropReq.igNotExists = 1;  // Ignore if table doesn't exist
  dropReq.isVirtual = 0;

  SVDropTbBatchReq dropBatchReq = {0};
  dropBatchReq.nReqs = 1;
  dropBatchReq.pArray = taosArrayInit(1, sizeof(SVDropTbReq));
  if (dropBatchReq.pArray == NULL) {
    code = terrno;
    goto _OVER;
  }
  if (taosArrayPush(dropBatchReq.pArray, &dropReq) == NULL) {
    code = terrno;
    taosArrayDestroy(dropBatchReq.pArray);
    goto _OVER;
  }

  // Serialize drop batch request
  int32_t dropContLen = 0;
  int32_t dropRet = 0;
  tEncodeSize(tEncodeSVDropTbBatchReq, &dropBatchReq, dropContLen, dropRet);
  if (dropRet < 0) {
    code = terrno;
    taosArrayDestroy(dropBatchReq.pArray);
    goto _OVER;
  }

  dropContLen += sizeof(SMsgHead);
  SMsgHead *pDropHead = taosMemoryCalloc(1, dropContLen);
  if (pDropHead == NULL) {
    code = terrno;
    taosArrayDestroy(dropBatchReq.pArray);
    goto _OVER;
  }
  pDropHead->contLen = htonl(dropContLen);
  pDropHead->vgId = htonl(pVgroup->vgId);

  SEncoder dropEncoder = {0};
  void *pDropBuf = POINTER_SHIFT(pDropHead, sizeof(SMsgHead));
  tEncoderInit(&dropEncoder, pDropBuf, dropContLen - sizeof(SMsgHead));
  code = tEncodeSVDropTbBatchReq(&dropEncoder, &dropBatchReq);
  tEncoderClear(&dropEncoder);
  taosArrayDestroy(dropBatchReq.pArray);
  if (code < 0) {
    taosMemoryFree(pDropHead);
    goto _OVER;
  }

  // Add undo action
  STransAction undoAction = {0};
  undoAction.epSet = mndGetVgroupEpset(pMnode, pVgroup);
  undoAction.pCont = pDropHead;
  undoAction.contLen = dropContLen;
  undoAction.msgType = TDMT_VND_DROP_TABLE;
  undoAction.acceptableCode = TSDB_CODE_TDB_TABLE_NOT_EXIST;

  code = mndTransAppendUndoAction(pTrans, &undoAction);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pDropHead);
    goto _OVER;
  }

  mInfo("stream:%s created output normal table:%s in vgroup:%d", pStream->name, pStream->outTblName, pVgroup->vgId);

_OVER:
  // Free resources (note: pHead is owned by transaction, don't free it here)
  if (createReq.name) taosMemoryFree(createReq.name);
  if (createReq.ntb.schemaRow.pSchema) taosMemoryFree(createReq.ntb.schemaRow.pSchema);
  if (createReq.pExtSchemas) taosMemoryFree(createReq.pExtSchemas);
  if (createReq.colCmpr.pColCmpr) taosMemoryFreeClear(createReq.colCmpr.pColCmpr);

  mndReleaseVgroup(pMnode, pVgroup);
  mndReleaseDb(pMnode, pDb);

  if (code != TSDB_CODE_SUCCESS) {
    mError("stream:%s failed to create output normal table:%s, line:%d code:%s", pStream->name,
              pStream->outTblName, lino, tstrerror(code));
  }

  return code;
}

int32_t mndValidateStreamTimeConfig(const SCMCreateStreamReq *pCreate) {
  if (pCreate->tsmaId != 0) return TSDB_CODE_SUCCESS;
  if (pCreate->timeConfig.timezone[0] == '\0' || pCreate->timeConfig.firstDayOfWeek < 0 ||
      pCreate->timeConfig.firstDayOfWeek > 6) {
    return TSDB_CODE_MND_STREAM_INVALID_TIME_CONFIG;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t mndStreamValidateCreate(SMnode *pMnode, SRpcMsg *pReq, SCMCreateStreamReq* pCreate) {
  int32_t code = 0, lino = 0;
  int64_t streamId = pCreate->streamId;
  char   *pUser = RPC_MSG_USER(pReq);

  code = mndValidateStreamTimeConfig(pCreate);
  TSDB_CHECK_CODE(code, lino, _OVER);

#ifdef TD_ENTERPRISE
  /* Reject EXT-driven stream creation when federatedQueryEnable=false on the server.
   * The parser checks the client-side flag, but ALTER ALL DNODES only updates the
   * server-side tsFederatedQueryEnable, so we must re-validate here in the mnode. */
  if (!tsFederatedQueryEnable && pCreate->numOfExtSpecs > 0) {
    code = TSDB_CODE_STREAM_EXT_DISABLED;
    mstsError("user %s failed to create stream %s since %s (federatedQueryEnable=false on server)",
              pUser, pCreate->name, tstrerror(code));
    TSDB_CHECK_CODE(code, lino, _OVER);
  }
#endif

  if (pCreate->streamDB) {
    // code = mndCheckDbPrivilegeByName(pMnode, pUser, MND_OPER_WRITE_DB, pCreate->streamDB);
    code = mndCheckDbPrivilegeByName(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_USE_DB,
                                     pCreate->streamDB, false);
    if (code) {
      if (code == TSDB_CODE_MND_NO_RIGHTS) code = TSDB_CODE_PAR_DB_USE_PERMISSION_DENIED;
      mstsError("user %s failed to create stream %s in db %s since %s", pUser, pCreate->name, pCreate->streamDB,
                tstrerror(code));
    }
    TSDB_CHECK_CODE(code, lino, _OVER);
  }

  if (pCreate->triggerDB) {
    // triggerDB may reference an external-source DB (federated query stream).
    // External-source DBs do not exist in TDengine's SDB, so mndAcquireDb would
    // return NULL and the privilege check would crash on a NULL realDbName inside
    // mndCheckDbPrivilegeByName.  Skip the check when the DB is not in SDB and
    // the stream references at least one external source (extSpecs non-empty).
    bool skipTriggerDbCheck = false;
#ifdef TD_ENTERPRISE
    if (pCreate->numOfExtSpecs > 0) {
      SDbObj *pTrigDb = mndAcquireDb(pMnode, pCreate->triggerDB);
      if (pTrigDb == NULL) {
        skipTriggerDbCheck = true;
        mDebug("stream:%s triggerDB '%s' not in sdb, treated as ext-source db - skipping privilege check",
               pCreate->name, pCreate->triggerDB);
      } else {
        mndReleaseDb(pMnode, pTrigDb);
      }
    }
#endif
    if (!skipTriggerDbCheck) {
      code = mndCheckDbPrivilegeByName(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_USE_DB,
                                       pCreate->triggerDB, false);
      if (code) {
        if (code == TSDB_CODE_MND_NO_RIGHTS) code = TSDB_CODE_PAR_DB_USE_PERMISSION_DENIED;
        mstsError("user %s failed to create stream %s using trigger db %s since %s", pUser, pCreate->name,
                  pCreate->triggerDB, tstrerror(code));
      }
      TSDB_CHECK_CODE(code, lino, _OVER);
    }
#if 0  // TODO check the owner of trigger table
    if (pCreate->triggerTblName) {
      // check trigger table privilege
      code = mndCheckObjPrivilegeRecF(pMnode, pUser, PRIV_TBL_SELECT, "", pCreate->triggerDB, pCreate->triggerTblName);
      if (code) {
        mstsError("user %s failed to create stream %s using trigger table %s.%s since %s", pUser, pCreate->name,
                  pCreate->triggerDB, pCreate->triggerTblName, tstrerror(code));
      }
      TSDB_CHECK_CODE(code, lino, _OVER);
    }
#endif
  }

  if (pCreate->calcDB) {
    int32_t dbNum = taosArrayGetSize(pCreate->calcDB);
    for (int32_t i = 0; i < dbNum; ++i) {
      char *calcDB = taosArrayGetP(pCreate->calcDB, i);
      // calcDB entries may also reference external-source DBs (federated calc readers).
      // Apply the same guard: skip privilege check if the DB is absent from SDB and
      // extSpecs are present.
      bool skipCalcDbCheck = false;
#ifdef TD_ENTERPRISE
      if (pCreate->numOfExtSpecs > 0) {
        SDbObj *pCDb = mndAcquireDb(pMnode, calcDB);
        if (pCDb == NULL) {
          skipCalcDbCheck = true;
          mDebug("stream:%s calcDB '%s' not in sdb, treated as ext-source db - skipping privilege check",
                 pCreate->name, calcDB);
        } else {
          mndReleaseDb(pMnode, pCDb);
        }
      }
#endif
      if (!skipCalcDbCheck) {
        code = mndCheckDbPrivilegeByName(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_USE_DB, calcDB,
                                         false);
        if (code) {
          if (code == TSDB_CODE_MND_NO_RIGHTS) code = TSDB_CODE_PAR_DB_USE_PERMISSION_DENIED;
          mstsError("user %s failed to create stream %s using calcDB %s since %s", pUser, pCreate->name, calcDB,
                    tstrerror(code));
        }
        TSDB_CHECK_CODE(code, lino, _OVER);
      }
    }
  }

  if (pCreate->outDB) {
    // code = mndCheckDbPrivilegeByName(pMnode, pUser, MND_OPER_WRITE_DB, pCreate->outDB);
    code = mndCheckDbPrivilegeByName(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_USE_DB, pCreate->outDB,
                                     false);
    if (code) {
      if (code == TSDB_CODE_MND_NO_RIGHTS) code = TSDB_CODE_PAR_DB_USE_PERMISSION_DENIED;
      mstsError("user %s failed to create stream %s using out db %s since %s", pUser, pCreate->name, pCreate->outDB,
                tstrerror(code));
    }
    TSDB_CHECK_CODE(code, lino, _OVER);
  }

  int32_t streamNum = sdbGetSize(pMnode->pSdb, SDB_STREAM);
  if (streamNum > MND_STREAM_MAX_NUM) {
    code = TSDB_CODE_MND_TOO_MANY_STREAMS;
    mstsError("failed to create stream %s since %s, stream number:%d", pCreate->name, tstrerror(code), streamNum);
    return code;
  }

_OVER:

  return code;
}

static int32_t mndStreamClassifyExtTrigger(const SCMCreateStreamReq *pCreate, const char **ppSourceName,
                                           int8_t *pSourceType, bool *pIsExtTrigger) {
  *ppSourceName = NULL;
  *pSourceType = 0;
  *pIsExtTrigger = false;
  if (pCreate->extSpecs == NULL) return pCreate->numOfExtSpecs == 0 ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_PARA;

  const int32_t numOfSpecs = taosArrayGetSize(pCreate->extSpecs);
  if (numOfSpecs != pCreate->numOfExtSpecs) return TSDB_CODE_INVALID_PARA;
  int32_t numOfCandidates = 0;
  for (int32_t i = 0; i < numOfSpecs; ++i) {
    SStreamExtTriggerSpec *pSpec = taosArrayGetP(pCreate->extSpecs, i);
    if (pSpec == NULL || pSpec->sourceName[0] == '\0') return TSDB_CODE_INVALID_PARA;
    const bool hasTable = pSpec->extTable[0] != '\0';
    const bool hasTsColumn = pSpec->tsColumn[0] != '\0';
    if (hasTable != hasTsColumn) return TSDB_CODE_INVALID_PARA;
    if (hasTable) {
      ++numOfCandidates;
      *ppSourceName = pSpec->sourceName;
      *pSourceType = pSpec->sourceType;
    }
  }
  if (numOfCandidates > 1) return TSDB_CODE_INVALID_PARA;
  *pIsExtTrigger = numOfCandidates == 1;
  return TSDB_CODE_SUCCESS;
}

#ifdef TD_ENTERPRISE
static int32_t mndStreamRejectAuthoritativeExtTrigger(SMnode *pMnode, const SCMCreateStreamReq *pCreate,
                                                      const char *pSourceName, int8_t sourceType) {
  SExtSourceObj *pSource = mndAcquireExtSource(pMnode, pSourceName);
  if (pSource == NULL) return TSDB_CODE_EXT_SOURCE_NOT_FOUND;
  const bool typeMatches = pSource->type == sourceType;
  mndReleaseExtSource(pMnode, pSource);
  if (!typeMatches) return TSDB_CODE_INVALID_PARA;

  SStreamWindowPlanValidationCtx ctx = {
      .isExtTrigger = true,
      .deleteRecalc = pCreate->deleteReCalc,
      .ignoreNoDataTrigger = pCreate->igNoDataTrigger,
      .flushOnOuterClose = BIT_FLAG_TEST_MASK(pCreate->addOptions, STREAM_OPTION_FLUSH_ON_OUTER_CLOSE),
      .eventTypes = pCreate->eventTypes,
  };
  return tValidateStreamWindowPlan(pCreate->pWindowPlan, &ctx);
}
#endif

static int32_t mndStreamBuildStbMetaSnapshot(SMnode *pMnode, const SCMCreateStreamReq *pCreate, SDbObj *pDb,
                                             SStbObj *pStb, STableMetaRsp *pMeta) {
  int32_t code = TSDB_CODE_SUCCESS;
  taosRLockLatch(&pStb->lock);
  if (pStb->numOfColumns <= 0 || pStb->numOfColumns > TSDB_MAX_COLUMNS || pStb->numOfTags < 0 ||
      pStb->numOfTags > TSDB_MAX_TAGS || pStb->numOfColumns > INT32_MAX - pStb->numOfTags || pStb->pColumns == NULL ||
      (pStb->numOfTags > 0 && pStb->pTags == NULL)) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  const int32_t totalCols = pStb->numOfColumns + pStb->numOfTags;
  pMeta->pSchemas = taosMemoryCalloc(totalCols, sizeof(SSchema));
  if (pMeta->pSchemas == NULL) {
    code = terrno;
    goto _exit;
  }
  pMeta->pSchemaExt = taosMemoryCalloc(pStb->numOfColumns, sizeof(SSchemaExt));
  if (pMeta->pSchemaExt == NULL) {
    code = terrno;
    goto _exit;
  }

  tstrncpy(pMeta->dbFName, pDb->name, sizeof(pMeta->dbFName));
  tstrncpy(pMeta->tbName, pCreate->triggerTblName, sizeof(pMeta->tbName));
  tstrncpy(pMeta->stbName, pCreate->triggerTblName, sizeof(pMeta->stbName));
  pMeta->dbId = pDb->uid;
  pMeta->numOfColumns = pStb->numOfColumns;
  pMeta->numOfTags = pStb->numOfTags;
  pMeta->precision = pDb->cfg.precision;
  pMeta->tableType = TSDB_SUPER_TABLE;
  pMeta->sversion = pStb->colVer;
  pMeta->tversion = pStb->tagVer;
  pMeta->suid = pStb->uid;
  pMeta->tuid = pStb->uid;
  pMeta->vgId = 0;
  pMeta->virtualStb = pStb->virtualStb;
  memcpy(pMeta->pSchemas, pStb->pColumns, sizeof(SSchema) * pStb->numOfColumns);
  if (pStb->numOfTags > 0) {
    memcpy(pMeta->pSchemas + pStb->numOfColumns, pStb->pTags, sizeof(SSchema) * pStb->numOfTags);
  }
  for (int32_t i = 0; i < pStb->numOfColumns; ++i) {
    pMeta->pSchemaExt[i].colId = pStb->pColumns[i].colId;
    if (pStb->pCmpr != NULL) {
      pMeta->pSchemaExt[i].compress = pStb->pCmpr[i].alg;
    }
  }

_exit:
  taosRUnLockLatch(&pStb->lock);
  return code;
}

static int32_t mndStreamFindAuthoritativeColumn(const STableMetaRsp *pMeta, const SColumnNode *pColumn, bool *pIsTag,
                                                const SSchema **ppSchema) {
  const int32_t totalCols = pMeta->numOfColumns + pMeta->numOfTags;
  int32_t       idIndex = -1;
  int32_t       nameIndex = -1;
  int32_t       idMatches = 0;
  int32_t       nameMatches = 0;
  for (int32_t i = 0; i < totalCols; ++i) {
    if (pMeta->pSchemas[i].colId == pColumn->colId) {
      idIndex = i;
      ++idMatches;
    }
    if (pColumn->colName[0] != '\0' && strcmp(pMeta->pSchemas[i].name, pColumn->colName) == 0) {
      nameIndex = i;
      ++nameMatches;
    }
  }
  if (idMatches != 1 || nameMatches != 1 || idIndex != nameIndex) return TSDB_CODE_INVALID_PARA;
  *pIsTag = idIndex >= pMeta->numOfColumns;
  *ppSchema = &pMeta->pSchemas[idIndex];
  return TSDB_CODE_SUCCESS;
}

static void mndStreamNormalizeColumnNode(SColumnNode *pColumn, const STableMetaRsp *pMeta, const SSchema *pSchema,
                                         bool isTag, bool hasCompositePrimaryKey) {
  pColumn->tableId = pMeta->tuid;
  pColumn->tableType = pMeta->tableType;
  pColumn->colId = pSchema->colId;
  pColumn->colType = isTag ? COLUMN_TYPE_TAG : COLUMN_TYPE_COLUMN;
  pColumn->isPrimTs = pSchema->colId == PRIMARYKEY_TIMESTAMP_COL_ID;
  pColumn->tableHasPk = hasCompositePrimaryKey;
  pColumn->numOfPKs = hasCompositePrimaryKey ? 1 : 0;
  pColumn->isPk = (pSchema->flags & COL_IS_KEY) != 0;
  pColumn->node.resType.type = pSchema->type;
  pColumn->node.resType.bytes = pSchema->bytes;
  tstrncpy(pColumn->colName, pSchema->name, sizeof(pColumn->colName));
}

static EDealRes mndStreamValidatePartitionNode(SNode *pNode, void *pContext) {
  SStreamColumnValidationCtx *pCtx = pContext;
  if (pCtx->code != TSDB_CODE_SUCCESS) return DEAL_RES_ERROR;

  switch (nodeType(pNode)) {
    case QUERY_NODE_COLUMN: {
      bool           isTag = false;
      const SSchema *pSchema = NULL;
      SColumnNode   *pColumn = (SColumnNode *)pNode;
      pCtx->code = mndStreamFindAuthoritativeColumn(pCtx->pMeta, pColumn, &isTag, &pSchema);
      if (pCtx->code != TSDB_CODE_SUCCESS || !isTag) {
        pCtx->code = TSDB_CODE_INVALID_PARA;
        return DEAL_RES_ERROR;
      }
      mndStreamNormalizeColumnNode(pColumn, pCtx->pMeta, pSchema, isTag, pCtx->hasCompositePrimaryKey);
      pCtx->partitionByTag = true;
      return DEAL_RES_CONTINUE;
    }
    case QUERY_NODE_FUNCTION: {
      SFunctionNode *pFunction = (SFunctionNode *)pNode;
      if (fmGetFuncInfo(pFunction, NULL, 0) != TSDB_CODE_SUCCESS) {
        pCtx->code = TSDB_CODE_INVALID_PARA;
        return DEAL_RES_ERROR;
      }
      if (pFunction->funcType != FUNCTION_TYPE_TBNAME && !fmIsScalarFunc(pFunction->funcId)) {
        pCtx->code = TSDB_CODE_INVALID_PARA;
        return DEAL_RES_ERROR;
      }
      return DEAL_RES_CONTINUE;
    }
    case QUERY_NODE_VALUE:
    case QUERY_NODE_NODE_LIST:
    case QUERY_NODE_OPERATOR:
    case QUERY_NODE_LOGIC_CONDITION:
    case QUERY_NODE_WHEN_THEN:
    case QUERY_NODE_CASE_WHEN:
      return DEAL_RES_CONTINUE;
    default:
      pCtx->code = TSDB_CODE_INVALID_PARA;
      return DEAL_RES_ERROR;
  }
}

static int32_t mndStreamValidateColumnLists(SCMCreateStreamReq *pCreate, const STableMetaRsp *pMeta,
                                            SStreamWindowPlanValidationCtx *pValidationCtx) {
  SNodeList *pList = NULL;
  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    lino = 0;
  if (pCreate->partitionCols != NULL) {
    TAOS_CHECK_GOTO(nodesStringToList(pCreate->partitionCols, &pList), &lino, _exit);
    SStreamColumnValidationCtx ctx = {
        .pMeta = pMeta,
        .hasCompositePrimaryKey = pValidationCtx->hasCompositePrimaryKey,
        .code = TSDB_CODE_SUCCESS,
    };
    SNode *pNode = NULL;
    FOREACH(pNode, pList) {
      if (fmIsCanonicalTbnameFunction(pNode)) ctx.partitionByTbname = true;
      nodesWalkExprPostOrder(pNode, mndStreamValidatePartitionNode, &ctx);
      if (ctx.code != TSDB_CODE_SUCCESS) {
        code = ctx.code;
        goto _exit;
      }
    }
    pValidationCtx->partitionByTbname = ctx.partitionByTbname;
    pValidationCtx->partitionByTag = ctx.partitionByTag;
    char   *pNormalized = NULL;
    int32_t normalizedLen = 0;
    TAOS_CHECK_GOTO(nodesListToString(pList, false, &pNormalized, &normalizedLen), &lino, _exit);
    taosMemoryFree(pCreate->partitionCols);
    pCreate->partitionCols = pNormalized;
    nodesDestroyList(pList);
    pList = NULL;
  }

  if (pCreate->rollupTagCols != NULL) {
    TAOS_CHECK_GOTO(nodesStringToList(pCreate->rollupTagCols, &pList), &lino, _exit);
    SNode *pNode = NULL;
    FOREACH(pNode, pList) {
      if (nodeType(pNode) != QUERY_NODE_COLUMN) {
        code = TSDB_CODE_INVALID_PARA;
        goto _exit;
      }
      bool           isTag = false;
      const SSchema *pSchema = NULL;
      TAOS_CHECK_GOTO(mndStreamFindAuthoritativeColumn(pMeta, (SColumnNode *)pNode, &isTag, &pSchema), &lino, _exit);
      if (!isTag || (pSchema->type != TSDB_DATA_TYPE_VARCHAR && pSchema->type != TSDB_DATA_TYPE_NCHAR)) {
        code = TSDB_CODE_INVALID_PARA;
        goto _exit;
      }
      mndStreamNormalizeColumnNode((SColumnNode *)pNode, pMeta, pSchema, isTag, pValidationCtx->hasCompositePrimaryKey);
    }
    pValidationCtx->hasRollup = LIST_LENGTH(pList) > 0;
    char   *pNormalized = NULL;
    int32_t normalizedLen = 0;
    TAOS_CHECK_GOTO(nodesListToString(pList, false, &pNormalized, &normalizedLen), &lino, _exit);
    taosMemoryFree(pCreate->rollupTagCols);
    pCreate->rollupTagCols = pNormalized;
  }

_exit:
  nodesDestroyList(pList);
  return code;
}

static int32_t mndStreamNormalizeAuthoritativeMetadata(SCMCreateStreamReq *pCreate, const STableMetaRsp *pMeta,
                                                       int64_t expectedDbUid, int8_t expectedDbPrecision,
                                                       int32_t expectedVgId, bool expectSuperTable) {
  if (pCreate == NULL || pCreate->pWindowPlan == NULL || pMeta == NULL || pMeta->numOfColumns <= 0 ||
      pMeta->numOfColumns > TSDB_MAX_COLUMNS || pMeta->numOfTags < 0 || pMeta->numOfTags > TSDB_MAX_TAGS ||
      pMeta->numOfColumns > INT32_MAX - pMeta->numOfTags || pMeta->pSchemas == NULL || pMeta->tuid == 0 ||
      strcmp(pMeta->tbName, pCreate->triggerTblName) != 0 || strcmp(pMeta->dbFName, pCreate->triggerDB) != 0 ||
      pMeta->dbId != expectedDbUid || pMeta->precision != expectedDbPrecision || pMeta->vgId != expectedVgId) {
    return TSDB_CODE_INVALID_MSG;
  }
  if (expectSuperTable) {
    if (pMeta->tableType != TSDB_SUPER_TABLE || pMeta->suid != pMeta->tuid || pMeta->stbName[0] == '\0') {
      return TSDB_CODE_INVALID_MSG;
    }
  } else {
    const bool isNormal = pMeta->tableType == TSDB_NORMAL_TABLE || pMeta->tableType == TSDB_VIRTUAL_NORMAL_TABLE;
    const bool isChild = pMeta->tableType == TSDB_CHILD_TABLE || pMeta->tableType == TSDB_VIRTUAL_CHILD_TABLE;
    if ((!isNormal && !isChild) || (isNormal && (pMeta->suid != 0 || pMeta->stbName[0] != '\0')) ||
        (isChild && (pMeta->suid == 0 || pMeta->suid == pMeta->tuid || pMeta->stbName[0] == '\0'))) {
      return TSDB_CODE_INVALID_MSG;
    }
  }

  const int32_t totalCols = pMeta->numOfColumns + pMeta->numOfTags;
  bool          hasCompositePrimaryKey = false;
  for (int32_t i = 0; i < totalCols; ++i) {
    const SSchema *pSchema = &pMeta->pSchemas[i];
    if (memchr(pSchema->name, '\0', sizeof(pSchema->name)) == NULL || pSchema->name[0] == '\0' || pSchema->bytes <= 0) {
      return TSDB_CODE_INVALID_MSG;
    }
    if (i > 0 && i < pMeta->numOfColumns && (pSchema->flags & COL_IS_KEY) != 0) hasCompositePrimaryKey = true;
    for (int32_t j = 0; j < i; ++j) {
      if (pMeta->pSchemas[j].colId == pSchema->colId || strcmp(pMeta->pSchemas[j].name, pSchema->name) == 0) {
        return TSDB_CODE_INVALID_MSG;
      }
    }
  }

  pCreate->triggerTblType = pMeta->tableType;
  pCreate->triggerTblUid = pMeta->tuid;
  pCreate->triggerTblSuid = pMeta->suid;
  pCreate->triggerTblVgId = pMeta->vgId;
  pCreate->triggerPrec = pMeta->precision;
  if (pMeta->virtualStb) {
    pCreate->flags |= CREATE_STREAM_FLAG_TRIGGER_VIRTUAL_STB;
  } else {
    pCreate->flags &= ~CREATE_STREAM_FLAG_TRIGGER_VIRTUAL_STB;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pCreate->pWindowPlan->pLayers); ++i) {
    SStreamWindowLayerSpec *pLayer = taosArrayGet(pCreate->pWindowPlan->pLayers, i);
    if (pLayer->triggerType == WINDOW_TYPE_INTERVAL) pLayer->trigger.sliding.precision = pMeta->precision;
  }
  if (pCreate->triggerType == WINDOW_TYPE_INTERVAL) pCreate->trigger.sliding.precision = pMeta->precision;

  SStreamWindowPlanValidationCtx validationCtx = {
      .hasCompositePrimaryKey = hasCompositePrimaryKey,
      .isSuperTable = pMeta->tableType == TSDB_SUPER_TABLE,
      .deleteRecalc = pCreate->deleteReCalc,
      .ignoreNoDataTrigger = pCreate->igNoDataTrigger,
      .flushOnOuterClose = BIT_FLAG_TEST_MASK(pCreate->addOptions, STREAM_OPTION_FLUSH_ON_OUTER_CLOSE),
      .eventTypes = pCreate->eventTypes,
  };
  int32_t code = mndStreamValidateColumnLists(pCreate, pMeta, &validationCtx);
  if (code == TSDB_CODE_SUCCESS) code = tValidateStreamWindowPlan(pCreate->pWindowPlan, &validationCtx);
  if (code == TSDB_CODE_SUCCESS) {
    code = tValidateStreamWindowPlanLeafProjection(pCreate->pWindowPlan, pCreate->triggerType, &pCreate->trigger);
  }
  return code;
}

static int32_t mndStreamQueueContinuation(SMnode *pMnode, const SRpcHandleInfo *pClientInfo, const void *pCreateReq,
                                          int32_t createReqLen, const void *pMetaRsp, int32_t metaRspLen) {
  SStreamPreflightEntry *pEntry = NULL;
  int32_t                code =
      mndStreamCreateQueuedPreflight(pMnode, pClientInfo, pCreateReq, createReqLen, pMetaRsp, metaRspLen, &pEntry);
  if (code != TSDB_CODE_SUCCESS) return code;

  SRpcMsg continuation = {
      .msgType = TDMT_MND_UNUSED1,
      .info = *pClientInfo,
  };
  continuation.info.rsp = NULL;
  continuation.info.rspLen = 0;
  code = mndStreamBuildContinuationPayload(pEntry, &continuation.pCont, &continuation.contLen);
  bool queueOwnsResponse = false;
  if (code == TSDB_CODE_SUCCESS) {
    queueOwnsResponse = true;
    code = tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &continuation);
  }

  bool terminalCleanup = false;
  (void)taosThreadMutexLock(&pEntry->mutex);
  if (pEntry->state == STREAM_PREFLIGHT_ENQUEUEING) {
    pEntry->state = code == TSDB_CODE_SUCCESS ? STREAM_PREFLIGHT_HANDED_OFF : STREAM_PREFLIGHT_REPLIED_BY_QUEUE;
    terminalCleanup = code != TSDB_CODE_SUCCESS;
  }
  (void)taosThreadCondBroadcast(&pEntry->terminalCleanupCond);
  (void)taosThreadMutexUnlock(&pEntry->mutex);
  if (terminalCleanup) mndStreamCleanupPreflightTerminal(pEntry, false);
  TAOS_UNUSED(taosReleaseRef(pEntry->refSetId, pEntry->refId));
  return queueOwnsResponse ? TSDB_CODE_ACTION_IN_PROGRESS : code;
}

static int32_t mndStreamPreflightCallback(void *param, SDataBuf *pMsg, int32_t code) {
  SStreamPreflightToken *pToken = param;
  SStreamPreflightEntry *pEntry = mndStreamAcquirePreflight(pToken);
  if (pEntry == NULL) {
    if (pMsg != NULL) taosMemoryFreeClear(pMsg->pData);
    return TSDB_CODE_SUCCESS;
  }

  if (code != TSDB_CODE_SUCCESS) {
    if (pMsg != NULL) taosMemoryFreeClear(pMsg->pData);
    (void)mndStreamFailPendingPreflight(pEntry, code, false);
    TAOS_UNUSED(taosReleaseRef(pToken->refSetId, pToken->refId));
    return TSDB_CODE_SUCCESS;
  }
  if (pMsg == NULL || pMsg->pData == NULL || pMsg->len <= 0) {
    if (pMsg != NULL) taosMemoryFreeClear(pMsg->pData);
    (void)mndStreamFailPendingPreflight(pEntry, TSDB_CODE_INVALID_MSG, false);
    TAOS_UNUSED(taosReleaseRef(pToken->refSetId, pToken->refId));
    return TSDB_CODE_SUCCESS;
  }

  (void)taosThreadMutexLock(&pEntry->mutex);
  if (pEntry->state != STREAM_PREFLIGHT_PENDING || pEntry->pMetaRsp != NULL) {
    (void)taosThreadMutexUnlock(&pEntry->mutex);
    taosMemoryFreeClear(pMsg->pData);
    TAOS_UNUSED(taosReleaseRef(pToken->refSetId, pToken->refId));
    return TSDB_CODE_SUCCESS;
  }
  pEntry->pMetaRsp = pMsg->pData;
  pEntry->metaRspLen = pMsg->len;
  pMsg->pData = NULL;
  (void)taosThreadMutexUnlock(&pEntry->mutex);

  void   *pPayload = NULL;
  int32_t payloadLen = 0;
  code = mndStreamBuildContinuationPayload(pEntry, &pPayload, &payloadLen);
  if (code != TSDB_CODE_SUCCESS) {
    (void)mndStreamFailPendingPreflight(pEntry, code, false);
    TAOS_UNUSED(taosReleaseRef(pToken->refSetId, pToken->refId));
    return TSDB_CODE_SUCCESS;
  }

  SRpcHandleInfo clientInfo = {0};
  bool           won = false;
  (void)taosThreadMutexLock(&pEntry->mutex);
  if (pEntry->state == STREAM_PREFLIGHT_PENDING) {
    pEntry->state = STREAM_PREFLIGHT_ENQUEUEING;
    clientInfo = pEntry->clientInfo;
    won = true;
  }
  (void)taosThreadMutexUnlock(&pEntry->mutex);
  if (!won) {
    rpcFreeCont(pPayload);
    TAOS_UNUSED(taosReleaseRef(pToken->refSetId, pToken->refId));
    return TSDB_CODE_SUCCESS;
  }

  SRpcMsg continuation = {
      .msgType = TDMT_MND_UNUSED1,
      .pCont = pPayload,
      .contLen = payloadLen,
      .info = clientInfo,
  };
  continuation.info.rsp = NULL;
  continuation.info.rspLen = 0;
  code = tmsgPutToQueue(&pEntry->pMnode->msgCb, WRITE_QUEUE, &continuation);

  bool handoffCleanup = false;
  bool terminalCleanup = false;
  (void)taosThreadMutexLock(&pEntry->mutex);
  if (pEntry->state == STREAM_PREFLIGHT_ENQUEUEING) {
    pEntry->state = code == TSDB_CODE_SUCCESS ? STREAM_PREFLIGHT_HANDED_OFF : STREAM_PREFLIGHT_REPLIED_BY_QUEUE;
    handoffCleanup = code == TSDB_CODE_SUCCESS;
    terminalCleanup = code != TSDB_CODE_SUCCESS;
  }
  (void)taosThreadCondBroadcast(&pEntry->terminalCleanupCond);
  (void)taosThreadMutexUnlock(&pEntry->mutex);
  if (handoffCleanup) {
    mndStreamCleanupPreflightHandoff(pEntry);
  } else if (terminalCleanup) {
    mndStreamCleanupPreflightTerminal(pEntry, false);
  }
  TAOS_UNUSED(taosReleaseRef(pToken->refSetId, pToken->refId));
  return TSDB_CODE_SUCCESS;
}

static int32_t mndStreamStartVnodePreflight(SMnode *pMnode, SRpcMsg *pReq, const SCMCreateStreamReq *pCreate,
                                            int32_t vgId, const SEpSet *pEpSet) {
  int32_t       code = TSDB_CODE_SUCCESS;
  STableInfoReq tableReq = {0};
  tableReq.header.vgId = vgId;
  tableReq.option = REQ_OPT_TBNAME;
  tstrncpy(tableReq.dbFName, pCreate->triggerDB, sizeof(tableReq.dbFName));
  tstrncpy(tableReq.tbName, pCreate->triggerTblName, sizeof(tableReq.tbName));
  int32_t requestLen = tSerializeSTableInfoReq(NULL, 0, &tableReq);
  if (requestLen <= 0) return requestLen;

  SStreamPreflightEntry *pEntry = taosMemoryCalloc(1, sizeof(*pEntry));
  if (pEntry == NULL) return terrno;
  if (taosThreadMutexInit(&pEntry->mutex, NULL) != 0) {
    taosMemoryFree(pEntry);
    return terrno;
  }
  if (taosThreadCondInit(&pEntry->terminalCleanupCond, NULL) != 0) {
    (void)taosThreadMutexDestroy(&pEntry->mutex);
    taosMemoryFree(pEntry);
    return terrno;
  }
  pEntry->pMnode = pMnode;
  pEntry->clientInfo = pReq->info;
  pEntry->state = STREAM_PREFLIGHT_PENDING;
  pEntry->createReqLen = pReq->contLen;
  pEntry->pCreateReq = taosMemoryMalloc(pReq->contLen);
  if (pEntry->pCreateReq == NULL) {
    mndStreamDestroyPreflightEntry(pEntry);
    return terrno;
  }
  memcpy(pEntry->pCreateReq, pReq->pCont, pReq->contLen);
  pEntry->nonce = ((uint64_t)taosSafeRand() << 32) | taosSafeRand();
  if (pEntry->nonce == 0) pEntry->nonce = 1;

  (void)taosThreadMutexLock(&mndStreamPreflightAdmissionMutex);
  if (atomic_load_8(&mndStreamPreflightStopping)) {
    (void)taosThreadMutexUnlock(&mndStreamPreflightAdmissionMutex);
    mndStreamDestroyPreflightEntry(pEntry);
    return TSDB_CODE_APP_IS_STOPPING;
  }
  pEntry->refSetId = mndStreamPreflightRef;
  pEntry->refId = taosAddRef(pEntry->refSetId, pEntry);
  if (pEntry->refId <= 0) {
    (void)taosThreadMutexUnlock(&mndStreamPreflightAdmissionMutex);
    mndStreamDestroyPreflightEntry(pEntry);
    return terrno;
  }

  SStreamPreflightEntry *pSenderEntry = taosAcquireRef(pEntry->refSetId, pEntry->refId);
  if (pSenderEntry == NULL) {
    TAOS_UNUSED(taosRemoveRef(pEntry->refSetId, pEntry->refId));
    (void)taosThreadMutexUnlock(&mndStreamPreflightAdmissionMutex);
    return terrno;
  }

  SStreamPreflightToken *pTimerToken = mndStreamCreatePreflightToken(pEntry);
  if (pTimerToken == NULL) {
    code = terrno;
    goto _fail;
  }
  (void)taosThreadMutexLock(&pEntry->mutex);
  pEntry->pTimerToken = pTimerToken;
  pEntry->timer = taosTmrStart(mndStreamPreflightTimeout, tsStatusSRTimeoutMs, pTimerToken, mndStreamPreflightTimer);
  tmr_h timer = pEntry->timer;
  if (timer == NULL) {
    pEntry->pTimerToken = NULL;
  }
  (void)taosThreadMutexUnlock(&pEntry->mutex);
  if (timer == NULL) {
    mndStreamReleasePreflightToken(pTimerToken);
    code = terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
    goto _fail;
  }

  SMsgSendInfo *pSendInfo = taosMemoryCalloc(1, sizeof(*pSendInfo));
  if (pSendInfo == NULL) {
    code = terrno;
    goto _fail;
  }
  pSendInfo->msgInfo.pData = taosMemoryMalloc(requestLen);
  if (pSendInfo->msgInfo.pData == NULL) {
    code = terrno;
    destroySendMsgInfo(pSendInfo);
    goto _fail;
  }
  if (tSerializeSTableInfoReq(pSendInfo->msgInfo.pData, requestLen, &tableReq) != requestLen) {
    code = TSDB_CODE_INVALID_MSG;
    destroySendMsgInfo(pSendInfo);
    goto _fail;
  }
  pSendInfo->fp = mndStreamPreflightCallback;
  pSendInfo->param = mndStreamCreatePreflightToken(pEntry);
  if (pSendInfo->param == NULL) {
    code = terrno;
    destroySendMsgInfo(pSendInfo);
    goto _fail;
  }
  pSendInfo->paramFreeFp = mndStreamReleasePreflightToken;
  pSendInfo->msgType = TDMT_VND_TABLE_META;
  pSendInfo->msgInfo.len = requestLen;
  pSendInfo->msgInfo.handle = NULL;

  int64_t transporterId = 0;
  code = asyncSendMsgToServer(pMnode->msgCb.clientRpc, (SEpSet *)pEpSet, &transporterId, pSendInfo);
  int64_t releaseId = 0;
  (void)taosThreadMutexLock(&pEntry->mutex);
  pEntry->transporterPublished = true;
  pEntry->transporterId = transporterId;
  if ((pEntry->releasePending || pEntry->state != STREAM_PREFLIGHT_PENDING) && !pEntry->transporterReleased &&
      transporterId > 0) {
    pEntry->transporterReleased = true;
    releaseId = transporterId;
  }
  (void)taosThreadMutexUnlock(&pEntry->mutex);
  if (releaseId > 0) (void)asyncFreeConnById(pMnode->msgCb.clientRpc, releaseId);
  if (code != TSDB_CODE_SUCCESS) goto _fail;

  (void)taosThreadMutexUnlock(&mndStreamPreflightAdmissionMutex);
  TAOS_UNUSED(taosReleaseRef(pEntry->refSetId, pEntry->refId));
  return TSDB_CODE_ACTION_IN_PROGRESS;

_fail:
  (void)mndStreamFailPendingPreflight(pEntry, code, false);
  (void)taosThreadMutexUnlock(&mndStreamPreflightAdmissionMutex);
  TAOS_UNUSED(taosReleaseRef(pEntry->refSetId, pEntry->refId));
  return TSDB_CODE_ACTION_IN_PROGRESS;
}

int32_t mndDropStreamByDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb) {
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  int32_t code = 0;

  while (1) {
    SStreamObj *pStream = NULL;
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) break;

    if (0 == strcmp(pStream->pCreate->streamDB, pDb->name)) {
      mInfo("start to drop stream %s in db %s", pStream->pCreate->name, pDb->name);
      
      pStream->updateTime = taosGetTimestampMs();
      
      atomic_store_8(&pStream->userDropped, 1);
      
      MND_STREAM_SET_LAST_TS(STM_EVENT_DROP_STREAM, pStream->updateTime);
      
      msmUndeployStream(pMnode, pStream->pCreate->streamId, pStream->pCreate->name);
      
      // drop stream
      code = mndStreamTransAppend(pStream, pTrans, SDB_STATUS_DROPPED);
      if (code) {
        mError("drop db trans:%d failed to append drop stream trans since %s", pTrans->id, tstrerror(code));
        sdbRelease(pSdb, pStream);
        sdbCancelFetch(pSdb, pIter);
        TAOS_RETURN(code);
      }
    }

    sdbRelease(pSdb, pStream);
  }

  return 0;
}

static int32_t mndRetrieveStream(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  int32_t     numOfRows = 0;
  SStreamObj *pStream = NULL;
  SUserObj   *pOperUser = NULL;
  int32_t     code = 0, lino = 0;
  bool        showAll = false;

  TAOS_CHECK_EXIT(mndAcquireUser(pMnode, RPC_MSG_USER(pReq), &pOperUser));
  showAll =
      (0 == mndCheckObjPrivilegeRec(pMnode, pOperUser, PRIV_CM_SHOW, PRIV_OBJ_STREAM, 0, pOperUser->acctId, "*", "*"));

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_STREAM, pShow->pIter, (void **)&pStream);
    if (pShow->pIter == NULL) break;

    // batch-meta-txn (stream phase 2): FS Sec.4.6 visibility.
    //   PRE_CREATE      -> only the owning txn sees it (reported as pending_create)
    //   PRE_CREATE_DROP -> nobody sees it: created and dropped in the same txn, net effect
    //                     is nothing, and it was never visible outside
    //   PRE_DROP        -> everyone sees it. The owning txn gets pending_drop; other
    //                     connections must still see it as normal, because the stream keeps
    //                     running until COMMIT and ROLLBACK has to be able to restore it.
    //                     This differs from tables (mndRetrieveStb hides a PRE_DROP table
    //                     from its own txn) - the stream keeps a visible definition instead.
    if (pStream->txnId != 0) {
      if (pStream->txnStatus == META_TXN_PRE_CREATE) {
        if (pStream->txnId != (txn_id_t)pShow->txnId) {
          sdbRelease(pSdb, pStream);
          continue;
        }
      } else if (pStream->txnStatus == META_TXN_PRE_CREATE_DROP) {
        sdbRelease(pSdb, pStream);
        continue;
      }
    }

    if (!showAll) {
      if ((mndCheckObjPrivilegeRecF(pMnode, pOperUser, PRIV_CM_SHOW, PRIV_OBJ_STREAM, pStream->ownerId,
                                    pStream->pCreate->streamDB, mndGetStableStr(pStream->pCreate->name)))) {
        sdbRelease(pSdb, pStream);
        continue;
      }
    }

    code = mstSetStreamAttrResBlock(pMnode, pStream, pBlock, numOfRows, pShow->txnId);
    if (code == 0) {
      numOfRows++;
    }
    sdbRelease(pSdb, pStream);
  }
  code = 0;
  pShow->numOfRows += numOfRows;
_exit:
  mndReleaseUser(pMnode, pOperUser);
  if (code != 0) {
    mError("failed to retrieve stream list at line %d since %s", lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}

static void mndCancelGetNextStream(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_STREAM);
}

static int32_t mndRetrieveStreamTask(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rowsCapacity) {
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  int32_t     numOfRows = 0;
  SStreamObj *pStream = NULL;
  int32_t     code = 0;

  while (numOfRows < rowsCapacity) {
    pShow->pIter = sdbFetch(pSdb, SDB_STREAM, pShow->pIter, (void **)&pStream);
    if (pShow->pIter == NULL) {
      break;
    }

    code = mstSetStreamTasksResBlock(pStream, pBlock, &numOfRows, rowsCapacity);

    sdbRelease(pSdb, pStream);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextStreamTask(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_STREAM);
}

static int32_t mndRetrieveStreamRecalculates(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rowsCapacity) {
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  int32_t     numOfRows = 0;
  SStreamObj *pStream = NULL;
  int32_t     code = 0;

  while (numOfRows < rowsCapacity) {
    pShow->pIter = sdbFetch(pSdb, SDB_STREAM, pShow->pIter, (void **)&pStream);
    if (pShow->pIter == NULL) {
      break;
    }

    code = mstSetStreamRecalculatesResBlock(pStream, pBlock, &numOfRows, rowsCapacity);

    sdbRelease(pSdb, pStream);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextStreamRecalculates(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_STREAM);
}

static void mndStreamMarkTagsFlag(void *pCols, const char *colName, SSchema *pTags, int32_t tagNum) {
  if (pCols == NULL) {
    return;
  }

  SNodeList *pList = NULL;
  int32_t    code = nodesStringToList(pCols, &pList);
  if (code) {
    nodesDestroyList(pList);
    mstError("%s [%s] nodesStringToList failed with error:%s", colName, (char *)pCols, tstrerror(code));
    return;
  }

  SNode *pNode = NULL;
  FOREACH(pNode, pList) {
    if (nodeType(pNode) != QUERY_NODE_COLUMN) {
      continue;
    }

    SColumnNode *pCol = (SColumnNode *)pNode;
    for (int32_t i = 0; i < tagNum; ++i) {
      if (pCol->colId == pTags[i].colId) {
        pTags[i].flags |= COL_REF_BY_STM;
        break;
      }
    }
  }

  nodesDestroyList(pList);
}

static bool mndStreamUpdateTagsFlag(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SStreamObj *pStream = pObj;
  if (atomic_load_8(&pStream->userDropped)) {
    return true;
  }

  if (TSDB_SUPER_TABLE != pStream->pCreate->triggerTblType && 
      TSDB_CHILD_TABLE != pStream->pCreate->triggerTblType && 
      TSDB_VIRTUAL_CHILD_TABLE != pStream->pCreate->triggerTblType) {
    return true;
  }

  if (pStream->pCreate->triggerTblSuid != *(uint64_t*)p1) {
    return true;
  }

  SSchema* pTags = (SSchema*)p2;
  int32_t* tagNum = (int32_t*)p3;

  mndStreamMarkTagsFlag(pStream->pCreate->partitionCols, "partitionCols", pTags, *tagNum);
  mndStreamMarkTagsFlag(pStream->pCreate->rollupTagCols, "rollupTagCols", pTags, *tagNum);

  return true;
}


void mndStreamUpdateTagsRefFlag(SMnode *pMnode, int64_t suid, SSchema* pTags, int32_t tagNum) {
  int32_t streamNum = sdbGetSize(pMnode->pSdb, SDB_STREAM);
  if (streamNum <= 0) {
    return;
  }

  sdbTraverse(pMnode->pSdb, SDB_STREAM, mndStreamUpdateTagsFlag, &suid, pTags, &tagNum);
}

static int32_t mndProcessStopStreamReq(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SStreamObj *pStream = NULL;
  SUserObj   *pOperUser = NULL;
  int32_t     code = 0;

  SMPauseStreamReq pauseReq = {0};
  if (tDeserializeSMPauseStreamReq(pReq->pCont, pReq->contLen, &pauseReq) < 0) {
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  code = mndAcquireStream(pMnode, pauseReq.name, &pStream);
  if (pStream == NULL || code != 0) {
    if (pauseReq.igNotExists) {
      mInfo("stream:%s, not exist, not stop stream", pauseReq.name);
      taosMemoryFree(pauseReq.name);
      return 0;
    } else {
      mError("stream:%s not exist, failed to stop stream", pauseReq.name);
      taosMemoryFree(pauseReq.name);
      TAOS_RETURN(TSDB_CODE_MND_STREAM_NOT_EXIST);
    }
  }

  taosMemoryFree(pauseReq.name);

  int64_t streamId = pStream->pCreate->streamId;
  
  mstsInfo("start to stop stream %s", pStream->name);

  // code = mndCheckDbPrivilegeByName(pMnode, RPC_MSG_USER(pReq), MND_OPER_WRITE_DB, pStream->pCreate->streamDB);
  if((code = mndAcquireUser(pMnode, RPC_MSG_USER(pReq), &pOperUser))) {
    mstsError("user %s failed to stop stream %s since %s", RPC_MSG_USER(pReq), pStream->name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }
  if ((code = mndCheckDbPrivilegeByName(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_USE_DB,
                                        pStream->pCreate->streamDB, false))) {
    if (code == TSDB_CODE_MND_NO_RIGHTS) code = TSDB_CODE_PAR_DB_USE_PERMISSION_DENIED;
  }
  if ((code != TSDB_CODE_SUCCESS) ||
      (code = mndCheckObjPrivilegeRecF(pMnode, pOperUser, PRIV_CM_STOP, PRIV_OBJ_STREAM, pStream->ownerId,
                                       pStream->pCreate->streamDB, mndGetStableStr(pStream->pCreate->name)))) {
    mstsError("user %s failed to stop stream %s since %s", RPC_MSG_USER(pReq), pStream->name, tstrerror(code));
    mndReleaseUser(pMnode, pOperUser);
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  mndReleaseUser(pMnode, pOperUser); // release user after privilege check

  // Batch meta txn (phase 2): a stream held by an uncommitted txn must not be
  // stopped/started/recalculated from outside - the txn owns the object until
  // COMMIT/ROLLBACK (FS Sec.4.6: outside DROP/STOP/START/RECALCULATE on a pending
  // stream returns TSDB_CODE_TXN_RESOURCE_BUSY). The client rejects these inside
  // a txn; this is the server-side guard for other sessions (non-owning session
  // or hand-crafted RPC).
  if (pStream->txnId != 0) {
    mstsError("stream:%s is owned by uncommitted txn:%" PRId64 ", cannot stop stream from a non-owning session",
              pStream->name, pStream->txnId);
    sdbRelease(pMnode->pSdb, pStream);
    return TSDB_CODE_TXN_RESOURCE_BUSY;
  }

  STrans *pTrans = NULL;
  code = mndStreamCreateTrans(pMnode, pStream, pReq, TRN_CONFLICT_DB_INSIDE, MND_STREAM_STOP_NAME, &pTrans);
  if (pTrans == NULL || code) {
    mstsError("failed to stop stream %s since %s", pStream->name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  if (atomic_load_8(&pStream->userDropped)) {
    code = TSDB_CODE_MND_STREAM_DROPPING;
    mstsError("user %s failed to stop stream %s since %s", RPC_MSG_USER(pReq), pStream->name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  int64_t updateTime = taosGetTimestampMs();
  code = mndStreamTransAppendLifecycleUpdate(pStream, 1, updateTime, pTrans);
  if (code != TSDB_CODE_SUCCESS) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  SStreamLifecycleTransParam *pParam = taosMemoryCalloc(1, sizeof(*pParam));
  if (pParam == NULL) {
    code = terrno;
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }
  pParam->streamId = streamId;
  pParam->action = MND_STREAM_LIFECYCLE_STOP;
  pParam->expectedUserStopped = 1;
  tstrncpy(pParam->streamName, pStream->name, sizeof(pParam->streamName));
  mndTransSetCb(pTrans, 0, TRANS_STOP_FUNC_STREAM_LIFECYCLE, pParam, sizeof(*pParam));

  code = mndTransPrepare(pMnode, pTrans);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("trans:%d, failed to prepare stop stream trans since %s", pTrans->id, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  MND_STREAM_SET_LAST_TS(STM_EVENT_STOP_STREAM, updateTime);

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);

  return TSDB_CODE_ACTION_IN_PROGRESS;
}


static int32_t mndProcessStartStreamReq(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SStreamObj *pStream = NULL;
  SUserObj   *pOperUser = NULL;
  int32_t     code = 0;

  if ((code = grantCheckExpire(TSDB_GRANT_STREAMS)) < 0) {
    return code;
  }

  SMResumeStreamReq resumeReq = {0};
  if (tDeserializeSMResumeStreamReq(pReq->pCont, pReq->contLen, &resumeReq) < 0) {
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  code = mndAcquireStream(pMnode, resumeReq.name, &pStream);
  if (pStream == NULL || code != 0) {
    if (resumeReq.igNotExists) {
      mInfo("stream:%s not exist, not start stream", resumeReq.name);
      taosMemoryFree(resumeReq.name);
      sdbRelease(pMnode->pSdb, pStream);
      return 0;
    } else {
      mError("stream:%s not exist, failed to start stream", resumeReq.name);
      taosMemoryFree(resumeReq.name);
      TAOS_RETURN(TSDB_CODE_MND_STREAM_NOT_EXIST);
    }
  }

  taosMemoryFree(resumeReq.name);

  int64_t streamId = pStream->pCreate->streamId;

  mstsInfo("start to start stream %s from stopped", pStream->name);

  // code = mndCheckDbPrivilegeByName(pMnode, RPC_MSG_USER(pReq), MND_OPER_WRITE_DB, pStream->pCreate->streamDB);
  if ((code = mndAcquireUser(pMnode, RPC_MSG_USER(pReq), &pOperUser))) {
    mstsError("user %s failed to start stream %s since %s", RPC_MSG_USER(pReq), pStream->name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }
  if ((code = mndCheckDbPrivilegeByName(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_USE_DB,
                                        pStream->pCreate->streamDB, false))) {
    if (code == TSDB_CODE_MND_NO_RIGHTS) code = TSDB_CODE_PAR_DB_USE_PERMISSION_DENIED;
  }
  if ((code != TSDB_CODE_SUCCESS) ||
      (code = mndCheckObjPrivilegeRecF(pMnode, pOperUser, PRIV_CM_START, PRIV_OBJ_STREAM, pStream->ownerId,
                                       pStream->pCreate->streamDB, mndGetStableStr(pStream->pCreate->name)))) {
    mstsError("user %s failed to start stream %s since %s", RPC_MSG_USER(pReq), pStream->name, tstrerror(code));
    mndReleaseUser(pMnode, pOperUser);
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  mndReleaseUser(pMnode, pOperUser); // release user after privilege check

  // Batch meta txn (phase 2): same guard as STOP - an uncommitted txn owns this
  // stream until COMMIT/ROLLBACK (FS Sec.4.6). The client rejects START inside a
  // txn; this is the server-side guard for other sessions (non-owning session
  // or hand-crafted RPC).
  if (pStream->txnId != 0) {
    mstsError("stream:%s is owned by uncommitted txn:%" PRId64 ", cannot start stream from a non-owning session",
              pStream->name, pStream->txnId);
    sdbRelease(pMnode->pSdb, pStream);
    return TSDB_CODE_TXN_RESOURCE_BUSY;
  }

  STrans *pTrans = NULL;
  code = mndStreamCreateTrans(pMnode, pStream, pReq, TRN_CONFLICT_DB_INSIDE, MND_STREAM_START_NAME, &pTrans);
  if (pTrans == NULL || code) {
    mstsError("failed to start stream %s since %s", pStream->name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  if (atomic_load_8(&pStream->userDropped)) {
    code = TSDB_CODE_MND_STREAM_DROPPING;
    mstsError("user %s failed to start stream %s since %s", RPC_MSG_USER(pReq), pStream->name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  if (0 == atomic_load_8(&pStream->userStopped)) {
    code = TSDB_CODE_MND_STREAM_NOT_STOPPED;
    mstsError("user %s failed to start stream %s since %s", RPC_MSG_USER(pReq), pStream->name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  int64_t updateTime = taosGetTimestampMs();
  code = mndStreamTransAppendLifecycleUpdate(pStream, 0, updateTime, pTrans);
  if (code != TSDB_CODE_SUCCESS) {
    mstsError("failed to start stream %s since %s", pStream->name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  SStreamLifecycleTransParam *pParam = taosMemoryCalloc(1, sizeof(*pParam));
  if (pParam == NULL) {
    code = terrno;
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }
  pParam->streamId = streamId;
  pParam->action = MND_STREAM_LIFECYCLE_START;
  pParam->expectedUserStopped = 0;
  tstrncpy(pParam->streamName, pStream->name, sizeof(pParam->streamName));
  mndTransSetCb(pTrans, 0, TRANS_STOP_FUNC_STREAM_LIFECYCLE, pParam, sizeof(*pParam));

  code = mndTransPrepare(pMnode, pTrans);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mstsError("trans:%d, failed to prepare start stream %s trans since %s", pTrans->id, pStream->name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  MND_STREAM_SET_LAST_TS(STM_EVENT_START_STREAM, updateTime);

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);

  return TSDB_CODE_ACTION_IN_PROGRESS;
}

/* Shared TSMA guard for DROP STREAM, used by both the txn and the non-txn path so the
 * two cannot diverge (DS Sec.6.1 requires the txn path's validation set to match the
 * non-txn path exactly).
 *
 * The stream->TSMA association is by NAME, not by uid: pCreate->tsmaId is a
 * parser-generated random UUID (buildCreateTSMAReqBuildNameAndUid assigns
 * pReq->uid = taosGetSystemUUIDU64 and buildCreateTSMAReqBuildCreateDropStreamReq
 * copies it into createreq.tsmaId), while SSmaObj.uid is mndGenerateUid(name) on
 * the mnode side - two values that never compare equal, so an uid-based guard could
 * never fire. A TSMA's internal stream carries tsmaId != 0 and its name is exactly the
 * TSMA name (mndTSMAGenerateOutputName: "<acctId>.<tname>"), so sdbAcquire(SDB_SMA,
 * name) is the correct existence check. sdbAcquire only returns READY objects, which
 * also lets the drop-tsma transaction's own drop-stream redo action through:
 * mndDropTSMA marks the TSMA DROPPING before sending that action.
 *
 * Returns true when the drop must be refused (TSMA still exists). */
static bool mndStreamTsmaGuardBlocks(SMnode *pMnode, const SStreamObj *pStream) {
  if (pStream->pCreate->tsmaId == 0) return false;
  SSmaObj *pSma = sdbAcquire(pMnode->pSdb, SDB_SMA, pStream->pCreate->name);
  if (pSma == NULL) return false;
  sdbRelease(pMnode->pSdb, pSma);
  return true;
}

static int32_t mndProcessDropStreamReq(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SStreamObj *pStream = NULL;
  SUserObj   *pOperUser = NULL;
  int32_t     code = 0;
  int32_t     notExistNum = 0;
  int64_t     lifecycleUpdateTime = 0;

  SMDropStreamReq dropReq = {0};
  int64_t         tss = taosGetTimestampMs();
  if (tDeserializeSMDropStreamReq(pReq->pCont, pReq->contLen, &dropReq) < 0) {
    mError("invalid drop stream msg recv, discarded");
    code = TSDB_CODE_INVALID_MSG;
    TAOS_RETURN(code);
  }

  mDebug("recv drop stream msg, count:%d", dropReq.count);

  // Acquire user object for privilege check
  code = mndAcquireUser(pMnode, RPC_MSG_USER(pReq), &pOperUser);
  if (code != 0) {
    tFreeMDropStreamReq(&dropReq);
    TAOS_RETURN(code);
  }

  // check if all streams exist
  if (!dropReq.igNotExists) {
    for (int32_t i = 0; i < dropReq.count; i++) {
      if (!sdbCheckExists(pMnode->pSdb, SDB_STREAM, dropReq.name[i])) {
        mError("stream:%s not exist failed to drop it", dropReq.name[i]);
        mndReleaseUser(pMnode, pOperUser);
        tFreeMDropStreamReq(&dropReq);
        TAOS_RETURN(TSDB_CODE_MND_STREAM_NOT_EXIST);
      }
    }
  }

  STrans *pTrans = NULL;
  // batch-meta-txn (stream phase 2): the txn path (dropReq.txnId != 0) never uses this
  // trans - every stream is marked PRE_DROP by mndMarkStreamTxnDrop through its own
  // changeless marker trans, so a batch trans created here would be prepared with an
  // empty commit log and dropped at the end. Create it only for the non-txn multi-key
  // path.
  if (dropReq.count != 1 && dropReq.txnId == 0) {
    // Keep the legacy multi-key transaction path unchanged.
    pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, MND_STREAM_DROP_NAME);
    if (pTrans == NULL) {
      mError("failed to create drop stream transaction since %s", tstrerror(terrno));
      code = terrno;
      mndReleaseUser(pMnode, pOperUser);
      tFreeMDropStreamReq(&dropReq);
      TAOS_RETURN(code);
    }
    pTrans->ableToBeKilled = true;
  }

  // Process all streams and add them to the transaction
  for (int32_t i = 0; i < dropReq.count; i++) {
    char *streamName = dropReq.name[i];
    mDebug("drop stream[%d/%d]: %s", i + 1, dropReq.count, streamName);

    code = mndAcquireStream(pMnode, streamName, &pStream);
    if (pStream == NULL || code != 0) {
      mWarn("stream:%s not exist, ignore not exist is set, drop stream exec done with success", streamName);
      sdbRelease(pMnode->pSdb, pStream);
      pStream = NULL;
      notExistNum++;
      continue;
    }

    int64_t streamId = pStream->pCreate->streamId;

    if ((code = mndCheckDbPrivilegeByName(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_USE_DB,
                                          pStream->pCreate->streamDB, true))) {
      if (code == TSDB_CODE_MND_NO_RIGHTS) code = TSDB_CODE_PAR_DB_USE_PERMISSION_DENIED;
    }
    if ((code != TSDB_CODE_SUCCESS) ||
        (code = mndCheckObjPrivilegeRecF(pMnode, pOperUser, PRIV_CM_DROP, PRIV_OBJ_STREAM, pStream->ownerId,
                                         pStream->pCreate->streamDB, mndGetStableStr(pStream->pCreate->name)))) {
      mstsError("user %s failed to drop stream %s since %s", pReq->info.conn.user, streamName, tstrerror(code));
      sdbRelease(pMnode->pSdb, pStream);
      pStream = NULL;
      mndTransDrop(pTrans);
      pTrans = NULL;
      goto _OVER;
    }

    // Batch meta txn (phase 2): conflict detection and txn branch.
    // Block if another txn owns this stream.
    if (pStream->txnId != 0 && pStream->txnId != (txn_id_t)dropReq.txnId) {
      code = TSDB_CODE_TXN_RESOURCE_BUSY;
      mstsError("stream:%s is owned by txn:%" PRId64 ", cannot drop in txn:%" PRId64,
                streamName, pStream->txnId, dropReq.txnId);
      sdbRelease(pMnode->pSdb, pStream);
      pStream = NULL;
      mndTransDrop(pTrans);
      pTrans = NULL;
      goto _OVER;
    }
    // RYOW: if this txn already dropped this stream, treat as not-exist.
    if (dropReq.txnId != 0 && pStream->txnId == (txn_id_t)dropReq.txnId &&
        (pStream->txnStatus == META_TXN_PRE_DROP || pStream->txnStatus == META_TXN_PRE_CREATE_DROP)) {
      if (dropReq.igNotExists) {
        mstsInfo("stream:%s already dropped in txn:%" PRId64 ", ignore exists set", streamName, dropReq.txnId);
        sdbRelease(pMnode->pSdb, pStream);
        pStream = NULL;
        notExistNum++;
        continue;
      } else {
        code = TSDB_CODE_MND_STREAM_NOT_EXIST;
        sdbRelease(pMnode->pSdb, pStream);
        pStream = NULL;
        mndTransDrop(pTrans);
        pTrans = NULL;
        goto _OVER;
      }
    }

    // Txn path: persist PRE_DROP marker and skip the immediate-drop logic below.
    if (dropReq.txnId != 0) {
      // TSMA guard must run on the txn path too - DS Sec.6.1 requires the validation set
      // of the txn path to match the non-txn path exactly. The original placement below
      // was after the txn branch, so a txn could drop a TSMA-related stream that a
      // non-txn DROP refuses with TSDB_CODE_TSMA_MUST_BE_DROPPED.
      if (mndStreamTsmaGuardBlocks(pMnode, pStream)) {
        sdbRelease(pMnode->pSdb, pStream);
        pStream = NULL;
        code = TSDB_CODE_TSMA_MUST_BE_DROPPED;

        mstsError("refused to drop tsma-related stream %s since tsma still exists", streamName);
        mndTransDrop(pTrans);
        pTrans = NULL;
        goto _OVER;
      }

      // The marker trans must NOT own this statement's RPC handle. There is one marker
      // trans per stream in the batch and each one completes independently, so whichever
      // finishes first would answer the client while the remaining streams are still
      // unmarked -- an immediate `show streams` then renders those streams with their
      // runtime status instead of pending_drop. Passing NULL keeps the request handle out
      // of every trans; the single response is sent by the dispatcher when this function
      // returns, i.e. after the whole batch has been marked (see the return-code logic at
      // the end of this function).
      code = mndMarkStreamTxnDrop(pMnode, NULL, pStream, (txn_id_t)dropReq.txnId);
      sdbRelease(pMnode->pSdb, pStream);
      pStream = NULL;
      if (code != 0) {
        mndTransDrop(pTrans);
        pTrans = NULL;
        goto _OVER;
      }
      mstsDebug("stream:%s PRE_DROP marker persisted in txn:%" PRId64, streamName, dropReq.txnId);
      continue;  // Skip to next stream in the batch.
    }

    // Non-txn path: immediate drop (existing logic).
    if (mndStreamTsmaGuardBlocks(pMnode, pStream)) {
      sdbRelease(pMnode->pSdb, pStream);
      pStream = NULL;
      code = TSDB_CODE_TSMA_MUST_BE_DROPPED;

      mstsError("refused to drop tsma-related stream %s since tsma still exists", streamName);
      mndTransDrop(pTrans);
      pTrans = NULL;
      goto _OVER;
    }

    mstsInfo("start to drop stream %s", pStream->pCreate->name);

    if (dropReq.count == 1) {
      code = mndStreamCreateTrans(pMnode, pStream, pReq, TRN_CONFLICT_DB_INSIDE, MND_STREAM_DROP_NAME, &pTrans);
      if (code != TSDB_CODE_SUCCESS || pTrans == NULL) {
        code = code != TSDB_CODE_SUCCESS ? code : terrno;
        sdbRelease(pMnode->pSdb, pStream);
        pStream = NULL;
        goto _OVER;
      }
      lifecycleUpdateTime = taosGetTimestampMs();
    } else {
      pStream->updateTime = taosGetTimestampMs();
      atomic_store_8(&pStream->userDropped, 1);
      MND_STREAM_SET_LAST_TS(STM_EVENT_DROP_STREAM, pStream->updateTime);
      msmUndeployStream(pMnode, streamId, pStream->pCreate->name);
    }

    // Append drop stream operation to the transaction
    code = mndStreamTransAppend(pStream, pTrans, SDB_STATUS_DROPPED);
    if (code) {
      mstsError("trans:%d, failed to append drop stream %s trans since %s", pTrans->id, streamName, tstrerror(code));
      sdbRelease(pMnode->pSdb, pStream);
      pStream = NULL;
      goto _OVER;
    }

    if (dropReq.count == 1) {
      SStreamLifecycleTransParam *pParam = taosMemoryCalloc(1, sizeof(*pParam));
      if (pParam == NULL) {
        code = terrno;
        sdbRelease(pMnode->pSdb, pStream);
        pStream = NULL;
        goto _OVER;
      }
      pParam->streamId = streamId;
      pParam->action = MND_STREAM_LIFECYCLE_DROP;
      tstrncpy(pParam->streamName, pStream->name, sizeof(pParam->streamName));
      mndTransSetCb(pTrans, 0, TRANS_STOP_FUNC_STREAM_LIFECYCLE, pParam, sizeof(*pParam));
    }

    sdbRelease(pMnode->pSdb, pStream);
    pStream = NULL;

    mstsDebug("drop stream %s added to transaction", streamName);
  }

  // Prepare and execute the transaction for all streams.
  // The txn path (dropReq.txnId != 0) has nothing to prepare here: every stream was
  // already marked PRE_DROP by mndMarkStreamTxnDrop through its own changeless marker
  // trans, and no batch trans was created for the txn path (see the creation site), so
  // pTrans is NULL throughout. Preparing NULL would fail with INVALID_PARA, so it must
  // be skipped.
  if (notExistNum < dropReq.count && dropReq.txnId == 0) {
    code = mndTransPrepare(pMnode, pTrans);
    if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
      mError("trans:%d, failed to prepare drop stream trans since %s", pTrans->id, tstrerror(code));
      goto _OVER;
    }
    mInfo("trans:%d, drop stream transaction prepared for %d streams", pTrans->id, dropReq.count - notExistNum);
    if (dropReq.count == 1) MND_STREAM_SET_LAST_TS(STM_EVENT_DROP_STREAM, lifecycleUpdateTime);
  } else if (dropReq.txnId != 0 && notExistNum < dropReq.count) {
    // Txn path: all drops went through changeless marker trans; nothing to prepare.
    // Those transes deliberately do not own the request (see the call site), so the
    // response is sent by the dispatcher once this function returns - that is, after
    // every PRE_DROP marker of the batch has been applied.
  } else {
    // All streams don't exist, no need to prepare transaction
    mndTransDrop(pTrans);
    pTrans = NULL;
  }

  if (tsAuditLevel >= AUDIT_LEVEL_DATABASE && notExistNum < dropReq.count) {
    int64_t tse = taosGetTimestampMs();
    double  duration = (double)(tse - tss);
    duration = duration / 1000;
    // Use first stream's database for audit (assuming all streams are from same db in batch).
    // NOTE: a cross-db batch DROP records only the first stream's db - the same limitation
    // as the pre-txn behavior; cross-db batch drop audits are not split per db (FS Sec.4.12).
    if (dropReq.count > 0) {
      SStreamObj *pFirstStream = NULL;
      if (mndAcquireStream(pMnode, dropReq.name[0], &pFirstStream) == 0 && pFirstStream != NULL) {
        auditRecord(pReq, pMnode->clusterId, "dropStream", "", pFirstStream->pCreate->streamDB, NULL, 0, duration, 0);
        sdbRelease(pMnode->pSdb, pFirstStream);
      }
    }
  }

  // Response ownership for the whole statement.
  //
  // Non-txn: the trans created above owns pReq->info, so report ACTION_IN_PROGRESS and let
  // it answer when it finishes (pre-existing behaviour). If every requested stream was
  // missing and igNotExists was set there is no trans at all, so answer success here.
  //
  // Txn: no trans owns the request - the per-stream PRE_DROP marker transes are created
  // WITHOUT pReq precisely so they cannot answer it. Letting the first marker trans answer
  // released the client while the rest of the batch was still unmarked, so an immediate
  // `show streams` saw those streams in their runtime status (Idle/Running) instead of
  // pending_drop (this was the intermittent s10 failure). Returning SUCCESS here makes the
  // dispatcher send the one and only response after the loop above; each mndTransPrepare in
  // that loop has already applied its marker synchronously by then, so a caller that reads
  // `show streams` right after the statement returns observes the whole batch as
  // pending_drop.
  if (dropReq.txnId != 0) {
    code = TSDB_CODE_SUCCESS;
  } else {
    code = (notExistNum < dropReq.count) ? TSDB_CODE_ACTION_IN_PROGRESS : TSDB_CODE_SUCCESS;
  }

_OVER:
  if (pStream) {
    sdbRelease(pMnode->pSdb, pStream);
  }
  if (pTrans) {
    mndTransDrop(pTrans);
  }
  mndReleaseUser(pMnode, pOperUser);
  tFreeMDropStreamReq(&dropReq);
  TAOS_RETURN(code);
}

/* Batch meta txn (phase 2): output-table checks for CREATE STREAM inside a txn.
 *
 * Only form A (the stream creates its own output STB) needs a check here: the name must
 * not already be taken. Referencing a pre-existing output table (form D) imposes NOTHING
 * on that table - FS Sec.4.5. The non-txn path has no such protection either
 * (mndCheckDropStbForStream is defined but never called), so holding the table inside a
 * txn would be stricter than outside one and would contradict FS Sec.4.9 "a stream reference
 * does not constrain table DDL".
 *
 * Returns 0 if no conflict, error code otherwise. */
static int32_t mndCheckStreamOutTableConflict(SMnode *pMnode, const SStreamObj *pStream, txn_id_t txnId) {
  int32_t code = 0;
  SStbObj *pStb = NULL;
  char stbFName[TSDB_TABLE_FNAME_LEN] = {0};

  // Target already exists -> form D: reference only, nothing to check or reserve.
  if (pStream->pCreate->outTblExists) return TSDB_CODE_SUCCESS;
  // Form B / C output tables live in a vnode; the vnode's own txn conflict detection
  // (vnodeTxnLockTable) covers them when the create request arrives.
  if (pStream->pCreate->outTblType != TSDB_SUPER_TABLE) return TSDB_CODE_SUCCESS;

  // Form A: this txn will create the output STB, so the name must be free.
  // outDB is already "<acctId>.<db>" and outTblName is a bare table name.
  snprintf(stbFName, sizeof(stbFName), "%s%s%s", pStream->pCreate->outDB, TS_PATH_DELIMITER,
           pStream->pCreate->outTblName);
  pStb = mndAcquireStb(pMnode, stbFName);
  if (pStb != NULL) {
    if (pStb->txnId != 0 && pStb->txnId != txnId) {
      code = TSDB_CODE_TXN_RESOURCE_BUSY;
      mError("stream:%s output STB:%s is owned by txn:%" PRId64, pStream->pCreate->name, stbFName, pStb->txnId);
    } else if (pStb->txnId == txnId &&
               (pStb->txnStatus == META_TXN_PRE_DROP || pStb->txnStatus == META_TXN_PRE_CREATE_DROP)) {
      // This txn already dropped this STB earlier in the txn: the SDB key stays occupied
      // so other sessions still see it and ROLLBACK can restore it, so a same-name
      // recreate cannot be represented. Report the precise reason rather than a
      // misleading "already exists" - consistent with mndProcessCreateStbReq (FS Sec.5).
      code = TSDB_CODE_TXN_RECREATE_NOT_SUPPORTED;
      mError("stream:%s output STB:%s was dropped in txn:%" PRId64
             ", cannot recreate with the same name until COMMIT/ROLLBACK",
             pStream->pCreate->name, stbFName, txnId);
    } else {
      code = TSDB_CODE_MND_STB_ALREADY_EXIST;
      mError("stream:%s output STB:%s already exists", pStream->pCreate->name, stbFName);
    }
  }

  mndReleaseStb(pMnode, pStb);
  return code;
}

/* Batch meta txn (phase 2): persist PRE_CREATE marker for a stream being created
 * in a transaction. Conflict detection and shadow-op registration happen here.
 * Returns 0 on success, error code otherwise. Caller wraps 0 -> ACTION_IN_PROGRESS. */
static int32_t mndMarkStreamTxnCreate(SMnode *pMnode, SRpcMsg *pReq, const SCMCreateStreamReq *pCreate,
                                       SUserObj *pUser, int32_t snodeId, txn_id_t txnId) {
  int32_t code = 0, lino = 0;
  STrans *pTrans = NULL;
  // Stack-local marker stream: zeroed, so its latch is 0 and never shared. It exists
  // only to carry the PRE_CREATE fields into mndStreamTransAppend (which R-latches it
  // briefly); nobody else can observe or contend on it. Do NOT free it - pCreate is
  // owned by the caller (mndStreamBuildObj only aliases it, no deep copy).
  SStreamObj markerStream = {0};
  SDbObj *pDb = NULL;
  int64_t tss = taosGetTimestampMs();

  // pCreate->streamDB is already a fully-qualified db name ("<acctId>.<db>", produced by
  // tNameGetFullDbName in buildCreateStreamReq) - do NOT prefix it again.
  pDb = mndAcquireDb(pMnode, pCreate->streamDB);
  if (pDb == NULL) {
    TAOS_CHECK_EXIT(terrno ? terrno : TSDB_CODE_MND_DB_NOT_SELECTED);
  }

  // Build marker stream object (same fields as the eventual committed stream).
  // mndStreamBuildObj takes non-const pointer but doesn't mutate pCreate in ways
  // that affect our marker, so cast is safe here.
  mndStreamBuildObj(pMnode, &markerStream, (SCMCreateStreamReq *)pCreate, pUser, snodeId);
  markerStream.txnId = txnId;
  markerStream.txnStatus = META_TXN_PRE_CREATE;

  // Output-table conflict detection (form A/B/C dispatch).
  code = mndCheckStreamOutTableConflict(pMnode, &markerStream, txnId);
  if (code != 0) goto _exit;

  // Create the output table, exactly as the non-txn path does (forms A/B: the stream owns
  // the table; form C: it already exists and we only reference it). Without this the
  // committed stream has no table to write into and turns Failed on its first output.
  // The table is created with the txn markers set, so it is invisible to other sessions
  // and the CREATE_STB shadow op undoes it on ROLLBACK.
  //
  // This needs its own trans: the marker trans below is changeless (SDB-only), while
  // creating a table has to distribute schema to vnodes via redo actions.
  // Dispatch on whether the INTO target already exists - NOT on outStbExists, which is
  // only set for an existing SUPER table and would misclassify an existing normal table
  // as "needs creating" (FS Sec.4.2 keys the forms off target existence alone).
  if (pCreate->outTblExists) {
    // Form D: target already exists. Neither create it nor reserve it (FS Sec.4.5).
    mInfo("stream:%s txn:%" PRId64 " output table:%s already exists, only referenced", pCreate->name, txnId,
          pCreate->outTblName);
  } else if (TSDB_SUPER_TABLE == pCreate->outTblType) {
    // Form A: output STB is created on the mnode, marked PRE_CREATE via txnId.
    STrans *pOutTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_DB_INSIDE, pReq,
                                       "create-stream-out-stb");
    TSDB_CHECK_NULL(pOutTrans, code, lino, _exit, terrno);

    ((SCMCreateStreamReq *)pCreate)->outStbUid =
        mndGenerateUid(pCreate->outTblName, strlen(pCreate->outTblName));

    char outStbFName[TSDB_TABLE_FNAME_LEN] = {0};
    snprintf(outStbFName, sizeof(outStbFName), "%s%s%s", pCreate->outDB, TS_PATH_DELIMITER, pCreate->outTblName);

    // Register the undo entry before the write, so a failure mid-way still rolls back.
    code = mndTxnAddShadowOp(pMnode, txnId, MND_SHADOW_OP_CREATE_STB, outStbFName, pCreate->outStbUid, pDb->name,
                             NULL, 0);
    if (code != 0) {
      mndTransDrop(pOutTrans);
      goto _exit;
    }

    code = mndStreamCreateOutStb(pMnode, pOutTrans, pCreate, RPC_MSG_USER(pReq), txnId);
    if (code != 0) {
      mError("stream:%s txn:%" PRId64 " failed to create output stb:%s, %s", pCreate->name, txnId,
             pCreate->outTblName, tstrerror(code));
      mndTransDrop(pOutTrans);
      goto _exit;
    }

    code = mndTransPrepare(pMnode, pOutTrans);
    mndTransDrop(pOutTrans);
    if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
      mError("stream:%s txn:%" PRId64 " failed to prepare output stb trans, %s", pCreate->name, txnId,
             tstrerror(code));
      goto _exit;
    }
    code = 0;
    mInfo("stream:%s txn:%" PRId64 " created output super table:%s as PRE_CREATE", pCreate->name, txnId,
          pCreate->outTblName);
  } else if (TSDB_NORMAL_TABLE == pCreate->outTblType && pCreate->nodelayCreateSubtable) {
    // Form B: NODELAY_CREATE_SUBTABLE means the user wants the output table usable right
    // away, so create it inside the txn rather than degrading that option (FS Sec.4.2). The
    // table lives in a vnode; txnId rides along in SVCreateTbReq and the vnode writes it
    // as META_TXN_PRE_CREATE, so it is visible to this txn only.
    STrans *pOutTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_DB_INSIDE, pReq,
                                       "create-stream-out-table");
    TSDB_CHECK_NULL(pOutTrans, code, lino, _exit, terrno);
    // mndStreamCreateOutTable only appends a vnode redo action; it writes no mnode SDB
    // record, so this trans legitimately has no commit action. Say so explicitly, or
    // mndTransCheckCommitActions rejects the prepare with TRANS_CLOG_IS_NULL. (The
    // form-A branch above does not need this: creating an STB writes an SDB commit log.)
    mndTransSetChangeless(pOutTrans);

    code = mndStreamCreateOutTable(pMnode, pOutTrans, pCreate, txnId);
    if (code != 0) {
      mError("stream:%s txn:%" PRId64 " failed to create output table:%s, %s", pCreate->name, txnId,
             pCreate->outTblName, tstrerror(code));
      mndTransDrop(pOutTrans);
      goto _exit;
    }

    // The client never routed this DDL, so its pTxnVgSet does not contain this vgId and
    // mndCollectTxnVgroupIds cannot derive it. Without registering it here the vnode
    // never receives COMMIT/ROLLBACK and the table is stuck in PRE_CREATE forever.
    //
    // Register BEFORE mndTransPrepare: if prepare succeeds but AddVgId fails (OOM),
    // the table was already created as PRE_CREATE and the txn would never be able to
    // finalize it - the vnode would not be in the participant set, so COMMIT/ROLLBACK
    // would never reach it. Ordering AddVgId before prepare narrows that window to
    // zero: if AddVgId fails, nothing has been prepared yet and the whole statement
    // fails cleanly with the txn still ABORTED-able.
    code = mndTxnAddVgId(pMnode, txnId, pCreate->outTblVgId);
    if (code != 0) {
      mError("stream:%s txn:%" PRId64 " failed to register output table vgId:%d, %s", pCreate->name, txnId,
             pCreate->outTblVgId, tstrerror(code));
      mndTransDrop(pOutTrans);
      goto _exit;
    }

    code = mndTransPrepare(pMnode, pOutTrans);
    mndTransDrop(pOutTrans);
    if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
      mError("stream:%s txn:%" PRId64 " failed to prepare output table trans, %s", pCreate->name, txnId,
             tstrerror(code));
      goto _exit;
    }
    code = 0;

    mInfo("stream:%s txn:%" PRId64 " created output normal table:%s as PRE_CREATE on vgId:%d", pCreate->name,
          txnId, pCreate->outTblName, pCreate->outTblVgId);
  } else {
    // Form C: no NODELAY option - the stream creates the table on its first output,
    // exactly as outside a txn. Nothing to create now, nothing for ROLLBACK to undo.
    mInfo("stream:%s txn:%" PRId64 " output normal table:%s left to lazy creation on first output", pCreate->name,
          txnId, pCreate->outTblName);
  }

  // Create changeless marker trans (single SDB write, no vnode redos/undos).
  TSDB_CHECK_NULL(
      (pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "mark-stream-precreate")),
      code, lino, _exit, terrno);
  mndTransSetChangeless(pTrans);

  // Encode marker stream and append to trans.
  code = mndStreamTransAppend(&markerStream, pTrans, SDB_STATUS_READY);
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("stream:%s txn:%" PRId64 " failed to append marker to trans, %s", pCreate->name, txnId, tstrerror(code));
    goto _exit;
  }

  // Register shadow op for COMMIT/ROLLBACK handlers.
  code = mndTxnAddShadowOp(pMnode, txnId, MND_SHADOW_OP_CREATE_STREAM, pCreate->name, pCreate->streamId,
                           pDb->name, NULL, 0);
  if (code != 0) goto _exit;

  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));
  mInfo("stream:%s txn:%" PRId64 " PRE_CREATE marker persisted, streamId:%" PRId64, pCreate->name, txnId, pCreate->streamId);

  // Audit at DDL execution time, consistent with the non-txn path (and with one-phase
  // table DDL: mndStb.c writes createStb/dropStb audits as the statement executes).
  // The txn may still ROLLBACK, but so may any other audit; the record documents the
  // statement, not the outcome. DS Sec.6.2's "audit at COMMIT" would double-write on
  // COMMIT and write nothing on ROLLBACK - inconsistent with the txn's DDL-timing
  // convention.
  if (tsAuditLevel >= AUDIT_LEVEL_DATABASE) {
    int64_t tse = taosGetTimestampMs();
    double  duration = (double)(tse - tss);
    duration = duration / 1000;
    auditRecord(pReq, pMnode->clusterId, "createStream", pCreate->streamDB, pCreate->name, pCreate->sql,
                strlen(pCreate->sql), duration, 0);
  }

_exit:
  mndReleaseDb(pMnode, pDb);
  mndTransDrop(pTrans);
  // tFreeStreamObj(&markerStream)
  // NOTE: do NOT call tFreeStreamObj(&markerStream) here. mndStreamBuildObj only
  // aliases pCreate into the object (pObj->pCreate = pCreate, no deep copy), and
  // pCreate is still owned by our caller, which frees it on its own _OVER path.
  // Freeing it here double-frees and segfaults right after the marker is persisted.
  TAOS_RETURN(code);
}

/* Batch meta txn (phase 2): persist PRE_DROP marker for a stream being dropped
 * in a transaction. Returns 0 on success, error code otherwise.
 *
 * pReq is the client statement this marker belongs to. The batch DROP path passes NULL on
 * purpose: every stream of the batch gets its own marker trans and they complete in
 * sequence, so attaching the request would let the first completed trans answer the client
 * before the remaining streams are marked. Only the response ownership of the statement is
 * affected - the marker itself is identical either way. */
static int32_t mndMarkStreamTxnDrop(SMnode *pMnode, SRpcMsg *pReq, SStreamObj *pStream, txn_id_t txnId) {
  int32_t code = 0, lino = 0;
  STrans *pTrans = NULL;
  // Stack-local marker stream: shallow copy of pStream with txn fields overridden.
  // Its latch is copied from pStream (held by the caller via sdbAcquire); it is used
  // only to encode the marker and is never published, so no lock is taken on it.
  SStreamObj markerStream = {0};
  SDbObj *pDb = NULL;

  // streamDB is already fully qualified ("<acctId>.<db>") - do not prefix it again.
  pDb = mndAcquireDb(pMnode, pStream->pCreate->streamDB);
  if (pDb == NULL) {
    TAOS_CHECK_EXIT(terrno ? terrno : TSDB_CODE_MND_DB_NOT_SELECTED);
  }

  // Build marker stream (shallow copy + override txn fields).
  memcpy(&markerStream, pStream, sizeof(SStreamObj));
  markerStream.txnId = txnId;
  markerStream.txnStatus = (markerStream.txnStatus == META_TXN_PRE_CREATE) ? META_TXN_PRE_CREATE_DROP : META_TXN_PRE_DROP;

  // Create changeless marker trans.
  TSDB_CHECK_NULL(
      (pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "mark-stream-predrop")),
      code, lino, _exit, terrno);
  mndTransSetChangeless(pTrans);

  // Encode and append marker.
  code = mndStreamTransAppend(&markerStream, pTrans, SDB_STATUS_READY);
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("stream:%s txn:%" PRId64 " failed to append marker to trans, %s", pStream->pCreate->name, txnId, tstrerror(code));
    goto _exit;
  }

  // Register shadow op.
  code = mndTxnAddShadowOp(pMnode, txnId, MND_SHADOW_OP_DROP_STREAM, pStream->pCreate->name, pStream->pCreate->streamId,
                           pDb->name, NULL, 0);
  if (code != 0) goto _exit;

  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));
  mInfo("stream:%s txn:%" PRId64 " PRE_DROP marker persisted, streamId:%" PRId64, pStream->pCreate->name, txnId, pStream->pCreate->streamId);

_exit:
  mndReleaseDb(pMnode, pDb);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndStreamDispatchNestedCreate(SRpcMsg *pReq, const SCMCreateStreamReq *pCreate) {
  SMnode     *pMnode = pReq->info.node;
  const char *pSourceName = NULL;
  int8_t      sourceType = 0;
  bool        isExtTrigger = false;
  int32_t     code = mndStreamClassifyExtTrigger(pCreate, &pSourceName, &sourceType, &isExtTrigger);
  if (code != TSDB_CODE_SUCCESS) return code;
  if (isExtTrigger) {
#ifdef TD_ENTERPRISE
    return mndStreamRejectAuthoritativeExtTrigger(pMnode, pCreate, pSourceName, sourceType);
#else
    return TSDB_CODE_STREAM_EXT_DISABLED;
#endif
  }

  if (pCreate->triggerDB == NULL || pCreate->triggerTblName == NULL) return TSDB_CODE_INVALID_PARA;
  SDbObj *pDb = mndAcquireDb(pMnode, pCreate->triggerDB);
  if (pDb == NULL) return TSDB_CODE_MND_DB_NOT_EXIST;

  char stbFName[TSDB_TABLE_FNAME_LEN] = {0};
  (void)snprintf(stbFName, sizeof(stbFName), "%s.%s", pCreate->triggerDB, pCreate->triggerTblName);
  SStbObj *pStb = mndAcquireStb(pMnode, stbFName);
  if (pStb != NULL) {
    STableMetaRsp meta = {0};
    code = mndStreamBuildStbMetaSnapshot(pMnode, pCreate, pDb, pStb, &meta);
    mndReleaseStb(pMnode, pStb);
    mndReleaseDb(pMnode, pDb);
    if (code != TSDB_CODE_SUCCESS) {
      tFreeSTableMetaRsp(&meta);
      return code;
    }
    const int32_t metaLen = tSerializeSTableMetaRsp(NULL, 0, &meta);
    void         *pMeta = metaLen > 0 ? taosMemoryMalloc(metaLen) : NULL;
    if (pMeta == NULL) {
      code = metaLen > 0 ? terrno : metaLen;
    } else if (tSerializeSTableMetaRsp(pMeta, metaLen, &meta) != metaLen) {
      code = TSDB_CODE_INVALID_MSG;
    } else {
      code = mndStreamQueueContinuation(pMnode, &pReq->info, pReq->pCont, pReq->contLen, pMeta, metaLen);
    }
    taosMemoryFree(pMeta);
    tFreeSTableMetaRsp(&meta);
    return code;
  }

  SSHashObj *pDbVgroups = NULL;
  int32_t    vgId = 0;
  code = mstBuildDBVgroupsMap(pMnode, &pDbVgroups);
  if (code == TSDB_CODE_SUCCESS) {
    code = mstGetTableVgId(pDbVgroups, pCreate->triggerDB, pCreate->triggerTblName, &vgId);
  }
  mstDestroyDbVgroupsHash(pDbVgroups);
  mndReleaseDb(pMnode, pDb);
  if (code != TSDB_CODE_SUCCESS) return code;

  SVgObj *pVgroup = mndAcquireVgroup(pMnode, vgId);
  if (pVgroup == NULL) return terrno != 0 ? terrno : TSDB_CODE_MND_VGROUP_NOT_EXIST;
  SEpSet epSet = mndGetVgroupEpset(pMnode, pVgroup);
  mndReleaseVgroup(pMnode, pVgroup);
  return mndStreamStartVnodePreflight(pMnode, pReq, pCreate, vgId, &epSet);
}

static int32_t mndProcessCreateStreamReq(SRpcMsg *pReq) {
  int32_t code = grantCheck(TSDB_GRANT_STREAMS);
  if (code < 0) return code;

  SCMCreateStreamReq *pCreate = taosMemoryCalloc(1, sizeof(*pCreate));
  if (pCreate == NULL) return terrno;
  code = tDeserializeSCMCreateStreamReq(pReq->pCont, pReq->contLen, pCreate);
  if (code != TSDB_CODE_SUCCESS) {
    tFreeSCMCreateStreamReq(pCreate);
    taosMemoryFree(pCreate);
    return code;
  }

  const bool nested = BIT_FLAG_TEST_MASK(pCreate->addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN);
  const bool flushOnOuterClose = BIT_FLAG_TEST_MASK(pCreate->addOptions, STREAM_OPTION_FLUSH_ON_OUTER_CLOSE);
  // batch-meta-txn (stream phase 2): nested-window streams are not supported inside a
  // transaction. Their creation goes through the async preflight queue (and may even be
  // deferred via mndStreamQueueContinuation), while a txn statement must complete within
  // the DDL execution phase - the preflight continuation lands in
  // mndProcessCreateStreamFinal with no txn context of its own. The client's
  // isTxnAllowedStmtType admits QUERY_NODE_CREATE_STREAM_STMT wholesale, so this is the
  // server-side guard for hand-crafted RPCs and older clients. FS Sec.4.1.
  if (pCreate->txnId != 0 && nested) {
    code = TSDB_CODE_TXN_INVALID_OPERATION;
  } else if (nested != (pCreate->pWindowPlan != NULL) || (flushOnOuterClose && (!nested || pCreate->pWindowPlan == NULL))) {
    code = TSDB_CODE_INVALID_PARA;
  } else if (nested) {
    code = atomic_load_8(&mndStreamPreflightStopping) ? TSDB_CODE_APP_IS_STOPPING
                                                      : mndStreamDispatchNestedCreate(pReq, pCreate);
  } else {
    return mndProcessCreateStreamFinal(pReq, pCreate);
  }

  tFreeSCMCreateStreamReq(pCreate);
  taosMemoryFree(pCreate);
  return code;
}

static int32_t mndProcessCreateStreamContinuation(SRpcMsg *pReq) {
  SStreamPreflightToken token = {0};
  int32_t               code = mndStreamDecodeContinuationPayload(pReq->pCont, pReq->contLen, &token);
  if (code != TSDB_CODE_SUCCESS) return code;

  SStreamPreflightEntry *pEntry = mndStreamAcquirePreflight(&token);
  if (pEntry == NULL) return TSDB_CODE_INVALID_MSG;
  bool claimed = false;
  (void)taosThreadMutexLock(&pEntry->mutex);
  if (pEntry->state == STREAM_PREFLIGHT_ENQUEUEING || pEntry->state == STREAM_PREFLIGHT_HANDED_OFF) {
    pEntry->state = STREAM_PREFLIGHT_PROCESSING;
    claimed = true;
  }
  (void)taosThreadCondBroadcast(&pEntry->terminalCleanupCond);
  (void)taosThreadMutexUnlock(&pEntry->mutex);
  if (!claimed) {
    TAOS_UNUSED(taosReleaseRef(token.refSetId, token.refId));
    return TSDB_CODE_INVALID_MSG;
  }

  SCMCreateStreamReq *pCreate = taosMemoryCalloc(1, sizeof(*pCreate));
  if (pCreate == NULL) {
    code = terrno;
    goto _finish;
  }
  STableMetaRsp meta = {0};
  code = tDeserializeSCMCreateStreamReq(pEntry->pCreateReq, pEntry->createReqLen, pCreate);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  if (!BIT_FLAG_TEST_MASK(pCreate->addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN) || pCreate->pWindowPlan == NULL) {
    code = TSDB_CODE_INVALID_PARA;
    goto _exit;
  }
  code = tDeserializeSTableMetaRsp(pEntry->pMetaRsp, pEntry->metaRspLen, &meta);
  if (code != TSDB_CODE_SUCCESS) goto _exit;

  SMnode *pMnode = pEntry->pMnode;
  SDbObj *pDb = mndAcquireDb(pMnode, pCreate->triggerDB);
  if (pDb == NULL) {
    code = TSDB_CODE_MND_DB_NOT_EXIST;
    goto _exit;
  }
  const int64_t dbUid = pDb->uid;
  const int8_t  dbPrecision = pDb->cfg.precision;
  mndReleaseDb(pMnode, pDb);

  bool    expectSuperTable = meta.tableType == TSDB_SUPER_TABLE;
  int32_t expectedVgId = 0;
  if (expectSuperTable) {
    char stbFName[TSDB_TABLE_FNAME_LEN] = {0};
    (void)snprintf(stbFName, sizeof(stbFName), "%s.%s", pCreate->triggerDB, pCreate->triggerTblName);
    SStbObj *pStb = mndAcquireStb(pMnode, stbFName);
    if (pStb == NULL || pStb->uid != meta.tuid) {
      if (pStb != NULL) mndReleaseStb(pMnode, pStb);
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }
    mndReleaseStb(pMnode, pStb);
  } else {
    SSHashObj *pDbVgroups = NULL;
    code = mstBuildDBVgroupsMap(pMnode, &pDbVgroups);
    if (code == TSDB_CODE_SUCCESS) {
      code = mstGetTableVgId(pDbVgroups, pCreate->triggerDB, pCreate->triggerTblName, &expectedVgId);
    }
    mstDestroyDbVgroupsHash(pDbVgroups);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
  }

  code = mndStreamNormalizeAuthoritativeMetadata(pCreate, &meta, dbUid, dbPrecision, expectedVgId, expectSuperTable);
  if (code != TSDB_CODE_SUCCESS) goto _exit;

  SRpcMsg createReq = {
      .msgType = TDMT_MND_CREATE_STREAM,
      .info = pEntry->clientInfo,
  };
  code = mndProcessCreateStreamFinal(&createReq, pCreate);
  pCreate = NULL;

_exit:
  tFreeSTableMetaRsp(&meta);
  if (pCreate != NULL) {
    tFreeSCMCreateStreamReq(pCreate);
    taosMemoryFree(pCreate);
  }
_finish:
  (void)taosThreadMutexLock(&pEntry->mutex);
  if (pEntry->state == STREAM_PREFLIGHT_PROCESSING) pEntry->state = STREAM_PREFLIGHT_REPLIED_BY_QUEUE;
  (void)taosThreadCondBroadcast(&pEntry->terminalCleanupCond);
  (void)taosThreadMutexUnlock(&pEntry->mutex);
  mndStreamCleanupPreflightTerminal(pEntry, false);
  TAOS_UNUSED(taosReleaseRef(token.refSetId, token.refId));
  return code;
}

static int32_t mndProcessCreateStreamFinal(SRpcMsg *pReq, SCMCreateStreamReq *pCreate) {
  SMnode     *pMnode = pReq->info.node;
  SStreamObj *pStream = NULL;
  SStreamObj  streamObj = {0};
  SUserObj    *pOperUser = NULL;
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  STrans     *pTrans = NULL;
  uint64_t            streamId = 0;
  int64_t             tss = taosGetTimestampMs();

  if ((code = grantCheck(TSDB_GRANT_STREAMS)) < 0) {
    goto _OVER;
  }

  streamId = pCreate->streamId;

  mstsInfo("start to create stream %s, sql:%s", pCreate->name, pCreate->sql);

  // Batch meta txn (phase 2): snode assignment must happen before conflict checks,
  // because txn path (mndMarkStreamTxnCreate) needs snodeId for the marker SStreamObj,
  // and we must fail-fast on no-snode before any Raft writes.
  int32_t snodeId = msmAssignRandomSnodeId(pMnode, streamId);
  if (!GOT_SNODE(snodeId)) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _OVER);
  }

  code = mndAcquireStream(pMnode, pCreate->name, &pStream);
  if (pStream != NULL && code == 0) {
    // Batch meta txn: classify by txn ownership and marker state (DS 5.7).
    if (pStream->txnId != 0) {
      if (pCreate->txnId != 0 && pStream->txnId == (txn_id_t)pCreate->txnId &&
          (pStream->txnStatus == META_TXN_PRE_DROP || pStream->txnStatus == META_TXN_PRE_CREATE_DROP)) {
        // This txn dropped it earlier: the SDB key is still occupied by the old object
        // (needed so other sessions still see it and ROLLBACK can restore it), so a
        // same-name recreate cannot be represented. Report the precise reason rather
        // than a misleading "already exists".
        code = TSDB_CODE_TXN_RECREATE_NOT_SUPPORTED;
        mstsError("stream %s was dropped in txn %" PRId64 ", cannot recreate with the same name", pCreate->name,
                  pCreate->txnId);
        mndReleaseStream(pMnode, pStream);
        goto _OVER;
      }
      if (pStream->txnId != (txn_id_t)pCreate->txnId) {
        // Owned by a different txn - invisible/untouchable from here.
        code = TSDB_CODE_TXN_RESOURCE_BUSY;
        mstsError("stream %s is owned by txn %" PRId64 ", cannot create in txn %" PRId64, pCreate->name,
                  pStream->txnId, pCreate->txnId);
        mndReleaseStream(pMnode, pStream);
        goto _OVER;
      }
      // Same txn, PRE_CREATE: read-your-own-write, it genuinely already exists.
      if (pCreate->igExists) {
        mstsInfo("stream %s already created in txn %" PRId64 ", ignore exists set", pCreate->name, pCreate->txnId);
        code = 0;
      } else {
        code = TSDB_CODE_MND_STREAM_ALREADY_EXIST;
      }
      mndReleaseStream(pMnode, pStream);
      goto _OVER;
    }

    if (pCreate->igExists) {
      mstsInfo("stream %s already exist, ignore exist is set", pCreate->name);
    } else {
      code = TSDB_CODE_MND_STREAM_ALREADY_EXIST;
    }

    mndReleaseStream(pMnode, pStream);
    goto _OVER;
  } else if (code != TSDB_CODE_MND_STREAM_NOT_EXIST) {
    goto _OVER;
  }

  code = mndAcquireUser(pMnode, RPC_MSG_USER(pReq), &pOperUser);
  if (pOperUser == NULL) {
    TSDB_CHECK_CODE(TSDB_CODE_MND_NO_USER_FROM_CONN, lino, _OVER);
  }

  code = mndStreamValidateCreate(pMnode, pReq, pCreate);
  TSDB_CHECK_CODE(code, lino, _OVER);

  /* === P1 B2: federated query  -  fill encryptedPassword into extSpecs ===
   * taosc leaves SStreamExtTriggerSpec.encryptedPassword zero on purpose
   * (DS Sec 6.1.1 "password fill responsibility split"). For each unique
   * sourceName, look up the AES-CBC ciphertext from mnode-local sdb
   * (SExtSourceObj, mndDef.h) and memcpy it into the spec. The snode
   * reader later calls decryptExtSourcePassword to recover the plaintext.
   * Failure (source missing or DROPped between parser cache fetch and
   * mnode arrival) returns TSDB_CODE_EXT_SOURCE_NOT_FOUND (0x6407,
   * MR 245). DS Sec 6.1.2 line 266. */
  #ifdef TD_ENTERPRISE
  if (pCreate->extSpecs != NULL && pCreate->numOfExtSpecs > 0) {
    int32_t n = (int32_t)taosArrayGetSize(pCreate->extSpecs);
    for (int32_t i = 0; i < n; ++i) {
      SStreamExtTriggerSpec* pSpec = *(SStreamExtTriggerSpec**)taosArrayGet(pCreate->extSpecs, i);
      if (pSpec == NULL) continue;
      SExtSourceObj* pSrcObj = mndAcquireExtSource(pMnode, pSpec->sourceName);
      if (pSrcObj == NULL) {
        mError("stream:%s ext source '%s' not found in sdb (race with DROP?)",
               pCreate->name, pSpec->sourceName);
        code = TSDB_CODE_EXT_SOURCE_NOT_FOUND;
        TSDB_CHECK_CODE(code, lino, _OVER);
      }
      memcpy(pSpec->encryptedPassword, pSrcObj->encryptedPassword,
             TSDB_EXT_SOURCE_ENC_PASSWORD_LEN);
      mDebug("stream:%s spec[%d] source=%s encryptedPassword filled",
             pCreate->name, i, pSpec->sourceName);
      mndReleaseExtSource(pMnode, pSrcObj);
    }
  }
  #endif

  // Batch meta txn (phase 2): if txnId != 0, take the marker path - persist
  // PRE_CREATE state to SDB and register a shadow op, then return IN_PROGRESS.
  // Output-table conflict detection and shadow-op registration happen inside
  // the marker helper. The immediate-create path (txnId == 0) is unchanged.
  if (pCreate->txnId != 0) {
    code = mndMarkStreamTxnCreate(pMnode, pReq, pCreate, pOperUser, snodeId, (txn_id_t)pCreate->txnId);
    if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;
    goto _OVER;
  }

  mndStreamBuildObj(pMnode, &streamObj, pCreate, pOperUser, snodeId);
  pCreate = NULL;

  pStream = &streamObj;

  code = mndStreamCreateTrans(pMnode, pStream, pReq, TRN_CONFLICT_DB, MND_STREAM_CREATE_NAME, &pTrans);
  if (pTrans == NULL || code) {
    goto _OVER;
  }

  // create output table for stream if it doesn't exist
  if (!pStream->pCreate->outStbExists) {
    if (TSDB_SUPER_TABLE == pStream->pCreate->outTblType) {
      // Create super table in mnode
      pStream->pCreate->outStbUid = mndGenerateUid(pStream->pCreate->outTblName, strlen(pStream->pCreate->outTblName));
      code = mndStreamCreateOutStb(pMnode, pTrans, pStream->pCreate, RPC_MSG_USER(pReq), 0);
      TSDB_CHECK_CODE(code, lino, _OVER);
      mstsInfo("stream:%s created output super table:%s", pStream->pCreate->name, pStream->pCreate->outTblName);
    } else if (TSDB_NORMAL_TABLE == pStream->pCreate->outTblType && pStream->pCreate->nodelayCreateSubtable) {
      // Create normal table in vnode
      code = mndStreamCreateOutTable(pMnode, pTrans, pStream->pCreate, 0);
      TSDB_CHECK_CODE(code, lino, _OVER);
      mstsInfo("stream:%s created output normal table:%s", pStream->pCreate->name, pStream->pCreate->outTblName);
    }
  } else {
    // Table exists, schema validation should have been done in client side
    mstsInfo("stream:%s output table:%s already exists, using existing table",
             pStream->pCreate->name, pStream->pCreate->outTblName);
  }

  // add stream to trans
  code = mndStreamTransAppend(pStream, pTrans, SDB_STATUS_READY);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mstsError("failed to persist stream %s since %s", pStream->pCreate->name, tstrerror(code));
    goto _OVER;
  }

  // execute creation
  code = mndTransPrepare(pMnode, pTrans);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mstsError("trans:%d, failed to prepare since %s", pTrans->id, tstrerror(code));
    goto _OVER;
  }
  code = TSDB_CODE_ACTION_IN_PROGRESS;

  if (tsAuditLevel >= AUDIT_LEVEL_DATABASE) {
    int64_t tse = taosGetTimestampMs();
    double  duration = (double)(tse - tss);
    duration = duration / 1000;
    auditRecord(pReq, pMnode->clusterId, "createStream", pStream->pCreate->streamDB, pStream->pCreate->name,
                pStream->pCreate->sql, strlen(pStream->pCreate->sql), duration, 0);
  }

  MND_STREAM_SET_LAST_TS(STM_EVENT_CREATE_STREAM, taosGetTimestampMs());

  mstPostStreamAction(mStreamMgmt.actionQ, streamId, pStream->pCreate->name, NULL, true, STREAM_ACT_DEPLOY);

_OVER:

  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    if (pStream && pStream->pCreate) {
      mstsError("failed to create stream %s at line:%d since %s", pStream->pCreate->name, lino, tstrerror(code));
    } else {
      mstsError("failed to create stream at line:%d since %s", lino, tstrerror(code));
    }
  } else {
    // pStream is NULL on the batch-meta-txn marker path (it returns before
    // mndStreamBuildObj assigns pStream = &streamObj), so it must be checked here too.
    mstsDebug("create stream %s half completed",
              (pStream && pStream->pCreate) ? pStream->pCreate->name : "unknown");
  }

  tFreeSCMCreateStreamReq(pCreate);
  taosMemoryFreeClear(pCreate);

  mndTransDrop(pTrans);
  tFreeStreamObj(&streamObj);
  mndReleaseUser(pMnode, pOperUser);

  return code;
}

static int32_t mndProcessRecalcStreamReq(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SStreamObj *pStream = NULL;
  SUserObj   *pOperUser = NULL;
  int32_t     code = 0;
  int64_t     tss = taosGetTimestampMs();

  if ((code = grantCheckExpire(TSDB_GRANT_STREAMS)) < 0) {
    return code;
  }

  SMRecalcStreamReq recalcReq = {0};
  if (tDeserializeSMRecalcStreamReq(pReq->pCont, pReq->contLen, &recalcReq) < 0) {
    tFreeMRecalcStreamReq(&recalcReq);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  code = mndAcquireStream(pMnode, recalcReq.name, &pStream);
  if (pStream == NULL || code != 0) {
    mError("stream:%s not exist, failed to recalc stream", recalcReq.name);
    tFreeMRecalcStreamReq(&recalcReq);
    TAOS_RETURN(TSDB_CODE_MND_STREAM_NOT_EXIST);
  }

  int64_t streamId = pStream->pCreate->streamId;
  
  mstsInfo("start to recalc stream %s", recalcReq.name);

  // code = mndCheckDbPrivilegeByName(pMnode, RPC_MSG_USER(pReq), MND_OPER_WRITE_DB, pStream->pCreate->streamDB);
  // if (code != TSDB_CODE_SUCCESS) {
  //   mstsError("user %s failed to recalc stream %s since %s", RPC_MSG_USER(pReq), recalcReq.name, tstrerror(code));
  //   sdbRelease(pMnode->pSdb, pStream);
  //   tFreeMRecalcStreamReq(&recalcReq);
  //   return code;
  // }

  if ((code = mndAcquireUser(pMnode, RPC_MSG_USER(pReq), &pOperUser))) {
    mstsError("user %s failed to recalc stream %s since %s", RPC_MSG_USER(pReq), pStream->name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    tFreeMRecalcStreamReq(&recalcReq);
    TAOS_RETURN(code);
  }

  if ((code = mndCheckDbPrivilegeByName(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_USE_DB,
                                        pStream->pCreate->streamDB, false))) {
    if (code == TSDB_CODE_MND_NO_RIGHTS) code = TSDB_CODE_PAR_DB_USE_PERMISSION_DENIED;
  }
  if ((code != TSDB_CODE_SUCCESS) ||
      (code = mndCheckObjPrivilegeRecF(pMnode, pOperUser, PRIV_CM_RECALC, PRIV_OBJ_STREAM, pStream->ownerId,
                                       pStream->pCreate->streamDB, mndGetStableStr(pStream->pCreate->name)))) {
    mstsError("user %s failed to recalc stream %s since %s", RPC_MSG_USER(pReq), pStream->name, tstrerror(code));
    mndReleaseUser(pMnode, pOperUser);
    sdbRelease(pMnode->pSdb, pStream);
    tFreeMRecalcStreamReq(&recalcReq);
    return code;
  }

  mndReleaseUser(pMnode, pOperUser); // release user after privilege check

  // Batch meta txn (phase 2): same guard as STOP/START - an uncommitted txn owns
  // this stream until COMMIT/ROLLBACK (FS Sec.4.6). The client rejects RECALCULATE
  // inside a txn; this is the server-side guard for other sessions (non-owning
  // session or hand-crafted RPC).
  if (pStream->txnId != 0) {
    mstsError("stream:%s is owned by uncommitted txn:%" PRId64 ", cannot recalculate stream from a non-owning session",
              pStream->name, pStream->txnId);
    sdbRelease(pMnode->pSdb, pStream);
    tFreeMRecalcStreamReq(&recalcReq);
    return TSDB_CODE_TXN_RESOURCE_BUSY;
  }

  if (atomic_load_8(&pStream->userDropped)) {
    code = TSDB_CODE_MND_STREAM_DROPPING;
    mstsError("user %s failed to recalc stream %s since %s", RPC_MSG_USER(pReq), recalcReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    tFreeMRecalcStreamReq(&recalcReq);
    return code;
  }

  if (atomic_load_8(&pStream->userStopped)) {
    code = TSDB_CODE_MND_STREAM_STOPPED;
    mstsError("user %s failed to recalc stream %s since %s", RPC_MSG_USER(pReq), recalcReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    tFreeMRecalcStreamReq(&recalcReq);
    return code;
  }

  if (WINDOW_TYPE_PERIOD == pStream->pCreate->triggerType) {
    code = TSDB_CODE_OPS_NOT_SUPPORT;
    mstsError("failed to recalc stream %s since %s", recalcReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    tFreeMRecalcStreamReq(&recalcReq);
    return code;
  }

  /*
  pStream->updateTime = taosGetTimestampMs();

  STrans *pTrans = NULL;
  code = mndStreamCreateTrans(pMnode, pStream, pReq, TRN_CONFLICT_NOTHING, MND_STREAM_RECALC_NAME, &pTrans);
  if (pTrans == NULL || code) {
    mstsError("failed to recalc stream %s since %s", recalcReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  // stop stream
  code = mndStreamTransAppend(pStream, pTrans, SDB_STATUS_READY);
  if (code != TSDB_CODE_SUCCESS) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  code = mndTransPrepare(pMnode, pTrans);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("trans:%d, failed to prepare stop stream trans since %s", pTrans->id, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }
*/

  SDbObj *pStreamDb = mndAcquireDb(pMnode, pStream->pCreate->streamDB);
  if (pStreamDb == NULL) {
    code = terrno != TSDB_CODE_SUCCESS ? terrno : TSDB_CODE_MND_DB_NOT_SELECTED;
    mstsError("failed to acquire stream db %s since %s", pStream->pCreate->streamDB, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    tFreeMRecalcStreamReq(&recalcReq);
    return code;
  }

  STimeWindow triggerRange = recalcReq.timeRange;
  mstConvertRecalcRangePrecision(&triggerRange, pStreamDb->cfg.precision, pStream->pCreate->triggerPrec);
  mndReleaseDb(pMnode, pStreamDb);

  code = msmRecalcStream(pMnode, pStream, &triggerRange, pReq);
  if (code != TSDB_CODE_SUCCESS) {
    sdbRelease(pMnode->pSdb, pStream);
    tFreeMRecalcStreamReq(&recalcReq);
    return code;
  }
  code = TSDB_CODE_ACTION_IN_PROGRESS;

  if (tsAuditLevel >= AUDIT_LEVEL_DATABASE){
    char buf[128];
    snprintf(buf, sizeof(buf), "start:%" PRId64 ", end:%" PRId64, recalcReq.timeRange.skey, recalcReq.timeRange.ekey);
    int64_t tse = taosGetTimestampMs();
    double  duration = (double)(tse - tss);
    duration = duration / 1000;
    auditRecord(pReq, pMnode->clusterId, "recalcStream", pStream->name, recalcReq.name, buf, strlen(buf), duration, 0);
  }  

  sdbRelease(pMnode->pSdb, pStream);
  tFreeMRecalcStreamReq(&recalcReq);
//  mndTransDrop(pTrans);

  return code;
}


int32_t mndInitStream(SMnode *pMnode) {
  int32_t   code = TSDB_CODE_SUCCESS;
  SSdbTable table = {
      .sdbType = SDB_STREAM,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndStreamActionEncode,
      .decodeFp = (SdbDecodeFp)mndStreamActionDecode,
      .insertFp = (SdbInsertFp)mndStreamActionInsert,
      .updateFp = (SdbUpdateFp)mndStreamActionUpdate,
      .deleteFp = (SdbDeleteFp)mndStreamActionDelete,
  };

  if (!tsDisableStream) {
    code = mndStreamEnsurePreflightAdmission();
    if (code != TSDB_CODE_SUCCESS) return code;
    mndStreamPreflightRef = taosOpenRef(4096, mndStreamDestroyPreflightEntry);
    if (mndStreamPreflightRef < 0) return terrno;
    mndStreamPreflightTimer = taosTmrInit(4096, 100, TMAX(tsStatusSRTimeoutMs, 100), "mnd-stream-preflight");
    if (mndStreamPreflightTimer == NULL) {
      taosCloseRef(mndStreamPreflightRef);
      mndStreamPreflightRef = -1;
      return terrno;
    }
    (void)taosThreadMutexLock(&mndStreamPreflightAdmissionMutex);
    atomic_store_8(&mndStreamPreflightStopping, 0);
    (void)taosThreadMutexUnlock(&mndStreamPreflightAdmissionMutex);
    mndSetMsgHandle(pMnode, TDMT_MND_CREATE_STREAM, mndProcessCreateStreamReq);
    mndSetMsgHandle(pMnode, TDMT_MND_UNUSED1, mndProcessCreateStreamContinuation);
    mndSetMsgHandle(pMnode, TDMT_MND_DROP_STREAM, mndProcessDropStreamReq);
    mndSetMsgHandle(pMnode, TDMT_MND_START_STREAM, mndProcessStartStreamReq);
    mndSetMsgHandle(pMnode, TDMT_MND_STOP_STREAM, mndProcessStopStreamReq);
    mndSetMsgHandle(pMnode, TDMT_MND_STREAM_HEARTBEAT, mndProcessStreamHb);  
    mndSetMsgHandle(pMnode, TDMT_MND_RECALC_STREAM, mndProcessRecalcStreamReq);
  }
  mndSetMsgHandle(pMnode, TDMT_MND_GET_STREAM_CREATE_SQL, mndProcessGetStreamCreateSqlReq);
  
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_STREAMS, mndRetrieveStream);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_STREAMS, mndCancelGetNextStream);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_STREAM_TASKS, mndRetrieveStreamTask);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_STREAM_TASKS, mndCancelGetNextStreamTask);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_STREAM_RECALCULATES, mndRetrieveStreamRecalculates);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_STREAM_RECALCULATES, mndCancelGetNextStreamRecalculates);

  code = sdbSetTable(pMnode->pSdb, table);
  if (code) {
    if (!tsDisableStream) mndCleanupStream(pMnode);
    return code;
  }

  //code = sdbSetTable(pMnode->pSdb, tableSeq);
  return code;
}

static int32_t mndProcessGetStreamCreateSqlReq(SRpcMsg* pReq) {
  int32_t                code = 0, lino = 0;
  SMnode*                pMnode = pReq->info.node;
  SStreamObj*            pStream = NULL;
  SGetStreamCreateSqlReq req = {0};
  SGetStreamCreateSqlRsp rsp = {0};
  void*                  pRsp = NULL;
  int32_t                contLen = 0;

  TAOS_CHECK_EXIT(tDeserializeGetStreamCreateSqlReq(pReq->pCont, pReq->contLen, &req));

  if (mndAcquireStream(pMnode, req.name, &pStream) != 0 || pStream == NULL) {
    TAOS_CHECK_EXIT(terrno ? terrno : TSDB_CODE_MND_STREAM_NOT_EXIST);
  }

  if (pStream->pCreate == NULL || pStream->pCreate->sql == NULL) {
    mError("stream:%s has no stored create sql, cannot answer get create sql request", req.name);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_NO_CREATE_SQL);
  }

  rsp.sql = taosStrdup(pStream->pCreate->sql);
  if (rsp.sql == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  if (pStream->pCreate->triggerDB) {
    rsp.triggerDB = taosStrdup(pStream->pCreate->triggerDB);
    if (rsp.triggerDB == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }
  if (pStream->pCreate->triggerTblName) {
    rsp.triggerTblName = taosStrdup(pStream->pCreate->triggerTblName);
    if (rsp.triggerTblName == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  if ((contLen = tSerializeGetStreamCreateSqlRsp(NULL, 0, &rsp)) < 0) {
    TAOS_CHECK_EXIT(contLen);
  }
  if (!(pRsp = rpcMallocCont(contLen))) {
    TAOS_CHECK_EXIT(terrno);
  }
  if ((contLen = tSerializeGetStreamCreateSqlRsp(pRsp, contLen, &rsp)) < 0) {
    TAOS_CHECK_EXIT(contLen);
  }

  pReq->info.rsp = pRsp;
  pReq->info.rspLen = contLen;

_exit:
  if (code != 0) {
    rpcFreeCont(pRsp);
  }
  if (pStream) mndReleaseStream(pMnode, pStream);
  tFreeGetStreamCreateSqlRsp(&rsp);
  TAOS_RETURN(code);
}
