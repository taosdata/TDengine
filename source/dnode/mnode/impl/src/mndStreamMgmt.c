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

#include "cmdnodes.h"
#include "functionMgt.h"
#include "libs/new-stream/stream.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndExtSource.h"
#include "mndMnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndSnode.h"
#include "mndStb.h"
#include "mndStream.h"
#include "mndStreamRecalc.h"
#include "mndTrans.h"
#include "mndVgroup.h"
#include "osMemory.h"
#include "parser.h"
#include "taoserror.h"
#include "tdTestSeam.h"
#include "tlogKeyOp.h"
#include "tmisce.h"
#include "tname.h"
#include "tuuid.h"
// #include "mndDnode.h"
// #include "mndVgroup.h"
// #include "mndSnode.h"
// #include "mndMnode.h"
// #include "cmdnodes.h"
// #include "mndExtSource.h"
#include "extConnector.h"
#include "extConnectorInt.h"
#include "extTypeMap.h"
#include "plannodes.h"

#include "monitor.h"
#include "tcompare.h"

typedef struct {
  int64_t vg, snode, dnode, mnode;
} SStmPlacementVersion;

#ifdef TD_ENTERPRISE
typedef struct {
  int64_t taskId;
  int64_t seriousId;
  int32_t nodeId;
} SStmCachedReader;

typedef struct {
  int32_t snodeId;
  int32_t refs;
} SStmSchemaNodeWait;

static size_t msmFindSchemaNodeWait(const SArray* bucket, int32_t snodeId) {
  size_t count = taosArrayGetSize(bucket);
  for (size_t i = 0; i < count; ++i) {
    const SStmSchemaNodeWait* wait = taosArrayGet(bucket, i);
    if (wait->snodeId == snodeId) return i;
  }
  return count;
}

static int32_t msmAddSchemaNodeWait(SMnode* mnode, int32_t* token, int32_t snodeId) {
  SArray** bucket = &mnode->streamSchemaNodes[(uint32_t)snodeId % tListLen(mnode->streamSchemaNodes)];
  int32_t  code = 0;
  taosWLockLatch(&mnode->streamSchemaNodeLock);
  size_t i = msmFindSchemaNodeWait(*bucket, snodeId);
  if (i < taosArrayGetSize(*bucket)) {
    ++((SStmSchemaNodeWait*)taosArrayGet(*bucket, i))->refs;
  } else {
    SStmSchemaNodeWait entry = {.snodeId = snodeId, .refs = 1};
    if (!*bucket) *bucket = taosArrayInit(1, sizeof(entry));
    if (!*bucket || !taosArrayPush(*bucket, &entry)) {
      code = terrno;
      if (*bucket && !taosArrayGetSize(*bucket)) {
        taosArrayDestroy(*bucket);
        *bucket = NULL;
      }
    }
  }
  if (!code) atomic_store_32(token, snodeId);
  taosWUnLockLatch(&mnode->streamSchemaNodeLock);
  return code;
}

static void msmRemoveSchemaNodeWait(SMnode* mnode, int32_t* token) {
  int32_t snodeId = atomic_exchange_32(token, 0);
  if (!snodeId) return;
  SArray** bucket = &mnode->streamSchemaNodes[(uint32_t)snodeId % tListLen(mnode->streamSchemaNodes)];
  taosWLockLatch(&mnode->streamSchemaNodeLock);
  size_t              i = msmFindSchemaNodeWait(*bucket, snodeId);
  SStmSchemaNodeWait* wait = taosArrayGet(*bucket, i);
  if (--wait->refs == 0) {
    taosArrayRemove(*bucket, i);
    if (!taosArrayGetSize(*bucket)) {
      taosArrayDestroy(*bucket);
      *bucket = NULL;
    }
  }
  taosWUnLockLatch(&mnode->streamSchemaNodeLock);
}

static bool msmSchemaNodeNeedsReports(SMnode* mnode, int32_t snodeId) {
  taosRLockLatch(&mnode->streamSchemaNodeLock);
  const SArray* bucket = mnode->streamSchemaNodes[(uint32_t)snodeId % tListLen(mnode->streamSchemaNodes)];
  bool          needed = msmFindSchemaNodeWait(bucket, snodeId) < taosArrayGetSize(bucket);
  taosRUnLockLatch(&mnode->streamSchemaNodeLock);
  return needed;
}

typedef struct SStmExtSchema {
  int32_t                    queuedNode;
  struct SStmSchemaBatch*    batch;
  struct SStmExtSchema*      previous;  // Previous action for the same registry in this batch.
  SExtSourceObj*             source;
  int64_t                    version;
  SStmTaskToDeployExt        entry;
  SStreamExtOrigTblReaderRsp result;
  SStmTaskToDeployExt        replayDeploy;
  SStmCachedReader           requiredReader;  // Successful CREATE, reuse, or PATCH. Cleanup has no live dependency.
  SStreamTaskUndeploy        replayUndeploy;
  int64_t                    failedSeriousId;  // Retire the failed install together with its guarded replacement.
  struct SStmExtSchema*      nextDispatch;
  int64_t                    pendingDispatchId;  // Reserved until the outgoing response has a body.
  int64_t                    previousRetryTs;
  bool                       firstDispatched;
  bool                       superseded;
  int8_t                     queued;
  int8_t                     legacyDispatched;
  int32_t                    code;
} SStmExtSchema;

enum { MST_SCHEMA_STARTED = 1, MST_SCHEMA_CANCELED = 2 };

typedef struct SStmSchemaBatch {
  int32_t                 localAckNode;
  int32_t                 refs;
  int8_t                  ready;
  bool                    publishing;
  bool                    published;
  bool                    sentLocalActions;
  bool                    localDispatchPending;  // The current response owns the first delivery of a local instance.
  bool                    rejected;  // A terminal reply can carry cleanup addresses before Readers confirm.
  int64_t                 reqId;
  int8_t                  workerState;
  bool                    explicitAck;
  bool                    guardedDeploy;
  int64_t                 firstReplyTs;
  int64_t                 localConfirmBy;
  int64_t                 unconfirmedSince;
  bool                    hasDeploy;
  bool                    remote;
  int32_t                 triggerNode;
  bool                    cached;
  struct SStmSchemaBatch* nextResult;
  int64_t                 bytes;
  SStreamMgmtRsp          reply;
  int32_t                 code;
  SMnode*                 mnode;
  SStreamMgmtReq*         request;
  SArray*                 schemas;
  SSHashObj*              readerRegistry;  // Requested IDs only. Populated by the schema worker.
  SArray*                 pairedCleanups;  // Retire these instances only with their PATCH deployment. Sorted by taskId.
  SArray*                 cachedReaders;  // Exact Reader addresses required to return a cached legacy result.
  int64_t                 buildVersion;
  int64_t                 generation;
  int64_t                 seriousId;
  int64_t                 taskId;
  int64_t                 epoch;
  int32_t                 snode;
  SStmPlacementVersion    placement;
  SStmDeployInput*        input;
  int64_t                 streamId;
  SStreamObj*             stream;
} SStmSchemaBatch;

typedef struct {
  SStmSchemaBatch* batch;
  int32_t          rspIndex;
  SStmExtSchema*   dispatches;
  bool             localDispatch;
} SStmSchemaReply;

static SStmSchemaReply* msmPinSchemaReply(SStmGrpCtx* ctx, SStmSchemaBatch* batch) {
  if (!ctx->schemaReplies) ctx->schemaReplies = taosArrayInit(1, sizeof(SStmSchemaReply));
  if (!ctx->schemaReplies) return NULL;
  SStmSchemaReply  pin = {.batch = batch, .rspIndex = -1};
  SStmSchemaReply* borrowed = taosArrayPush(ctx->schemaReplies, &pin);
  if (borrowed) (void)atomic_add_fetch_32(&batch->refs, 1);
  return borrowed;
}

static void msmRemoveSchemaResult(SStmSchemaBatch** link) {
  SStmSchemaBatch* result = *link;
  *link = result->nextResult;
  msmReleaseSchemaBatch(result);
}

#endif

void msmDestroyActionQ() {
  SStmQNode* pQNode = NULL;

  if (NULL == mStreamMgmt.actionQ) {
    return;
  }

  while (mndStreamActionDequeue(mStreamMgmt.actionQ, &pQNode)) {
  }

  taosHashCleanup(mStreamMgmt.actionQ->queuedCreates);
  taosMemoryFreeClear(mStreamMgmt.actionQ->head);
  taosMemoryFreeClear(mStreamMgmt.actionQ);
}

void msmDestroySStmThreadCtx(SStmThreadCtx* pCtx) {
  for (int32_t m = 0; m < STREAM_MAX_GROUP_NUM; ++m) {
    taosHashCleanup(pCtx->deployStm[m]);
    taosHashCleanup(pCtx->actionStm[m]);
  }
}

void msmDestroyThreadCtxs() {
  if (NULL == mStreamMgmt.tCtx) {
    return;
  }
  
  for (int32_t i = 0; i < mStreamMgmt.threadNum; ++i) {
    msmDestroySStmThreadCtx(mStreamMgmt.tCtx + i);
  }
  taosMemoryFreeClear(mStreamMgmt.tCtx);
}


void msmDestroyRuntimeInfo(SMnode *pMnode) {
#ifdef TD_ENTERPRISE
  if (pMnode) {
    while (pMnode->streamSchemaResults) msmRemoveSchemaResult(&pMnode->streamSchemaResults);
  }
#endif
  mStreamMgmt.deployingStreamId = 0;
  taosHashCleanup(mStreamMgmt.watch.deferredStreams);
  mStreamMgmt.watch.deferredStreams = NULL;
  msmDestroyActionQ();
  msmDestroyThreadCtxs();

  taosHashCleanup(mStreamMgmt.toUpdateScanMap);
  mStreamMgmt.toUpdateScanMap = NULL;
  mStreamMgmt.toUpdateScanNum = 0;
  taosHashCleanup(mStreamMgmt.toDeployVgMap);
  mStreamMgmt.toDeployVgMap = NULL;
  mStreamMgmt.toDeployVgTaskNum = 0;
  taosHashCleanup(mStreamMgmt.toDeploySnodeMap);
  mStreamMgmt.toDeploySnodeMap = NULL;
  mStreamMgmt.toDeploySnodeTaskNum = 0;

  taosHashCleanup(mStreamMgmt.dnodeMap);
  mStreamMgmt.dnodeMap = NULL;
  taosHashCleanup(mStreamMgmt.snodeMap);
  mStreamMgmt.snodeMap = NULL;
  taosHashCleanup(mStreamMgmt.vgroupMap);
  mStreamMgmt.vgroupMap = NULL;
  taosHashCleanup(mStreamMgmt.taskMap);
  mStreamMgmt.taskMap = NULL;
  taosHashCleanup(mStreamMgmt.streamMap);
  mStreamMgmt.streamMap = NULL;

  memset(mStreamMgmt.lastTs, 0, sizeof(mStreamMgmt.lastTs));

  mstInfo("mnode stream mgmt destroyed");  
}

static int64_t msmAdvanceBuildVersion(SStmStatus* status) {
  atomic_store_64(&status->deferredTaskId, 0);
  return atomic_add_fetch_64(&status->buildVersion, 1);
}

void msmStopStreamByError(int64_t streamId, SStmStatus* pStatus, int32_t errCode, int64_t currTs) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmStatus* pStream = NULL;
  bool        locked = false;

  mstsInfo("try to stop stream for error: %s", tstrerror(errCode));

  if (NULL == pStatus) {
    pStream = (SStmStatus*)taosHashAcquire(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
    if (NULL == pStream) {
      mstsInfo("stream already not in streamMap, error:%s", tstrerror(terrno));
      goto _exit;
    }

    pStatus = pStream;
  }

  (void)mstWaitLock(&pStatus->resetLock, false);
  locked = true;
  int8_t stopped = atomic_load_8(&pStatus->stopped);
  if (stopped) {
    mstsDebug("stream already stopped %d, ignore stop", stopped);
    goto _exit;
  }

  if (pStatus->triggerTask && pStatus->triggerTask->runningStartTs && (currTs - pStatus->triggerTask->runningStartTs) > 2 * MST_ISOLATION_DURATION) {
    pStatus->fatalRetryTimes = 0;
    mstsDebug("reset stream retryTimes, running duation:%" PRId64 "ms", currTs - pStatus->triggerTask->runningStartTs);
  }

  pStatus->fatalRetryTimes++;
  pStatus->fatalError = errCode;
  TAOS_CHECK_EXIT(mstSetExtraErrMsg(&pStatus->extraErrMsg, NULL));
  pStatus->fatalRetryDuration = (pStatus->fatalRetryTimes > 10) ? MST_MAX_RETRY_DURATION : MST_ISOLATION_DURATION;
  pStatus->fatalRetryTs = currTs + pStatus->fatalRetryDuration;

  pStatus->stat.lastError = errCode;
    
  if (0 == atomic_val_compare_exchange_8(&pStatus->stopped, 0, 1)) {
    (void)msmAdvanceBuildVersion(pStatus);
#ifdef TD_ENTERPRISE
    // Read-only heartbeats also hold resetLock when acknowledging the pending batch.
    SStmSchemaBatch* pending = pStatus->pendingSchema;
    pStatus->pendingSchema = NULL;
    msmCancelSchemaBatch(pending);
#endif
    MND_STREAM_SET_LAST_TS(STM_EVENT_STM_TERR, currTs);
    monReportStreamFailure(currTs, streamId, pStatus->streamName, errCode);
  }

_exit:

  if (locked) taosWUnLockLatch(&pStatus->resetLock);
  taosHashRelease(mStreamMgmt.streamMap, pStream);

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
}


static void msmSetInitRuntimeState(int8_t state) {
  switch (state) {
    case MND_STM_STATE_WATCH:
      taosHashCleanup(mStreamMgmt.watch.deferredStreams);
      mStreamMgmt.watch.deferredStreams = NULL;
      mStreamMgmt.watch.ending = 0;
      mStreamMgmt.watch.taskRemains = 0;
      mStreamMgmt.watch.processing = 0;
      mstInfo("switch to WATCH state");
      break;
    case MND_STM_STATE_NORMAL:
      MND_STREAM_SET_LAST_TS(STM_EVENT_NORMAL_BEGIN, taosGetTimestampMs());
      mstInfo("switch to NORMAL state");
      break;
    default:
      return;
  }
  
  atomic_store_8(&mStreamMgmt.state, state);
}

void msmSTDeleteSnodeFromMap(int32_t snodeId) {
  int32_t code = taosHashRemove(mStreamMgmt.snodeMap, &snodeId, sizeof(snodeId));
  if (code) {
    mstWarn("remove snode %d from snodeMap failed, error:%s", snodeId, tstrerror(code));
  } else {
    mstInfo("snode %d removed from snodeMap", snodeId);
  }
}

static int32_t msmSTAddSnodesToMap(SMnode* pMnode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmSnodeStatus tasks = {0};
  SSnodeObj *pSnode = NULL;
  void *pIter = NULL;
  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_SNODE, pIter, (void **)&pSnode);
    if (pIter == NULL) {
      break;
    }

    tasks.lastUpTs = taosGetTimestampMs();
    code = taosHashPut(mStreamMgmt.snodeMap, &pSnode->id, sizeof(pSnode->id), &tasks, sizeof(tasks));
    if (code && TSDB_CODE_DUP_KEY != code) {
      sdbRelease(pMnode->pSdb, pSnode);
      sdbCancelFetch(pMnode->pSdb, pIter);
      pSnode = NULL;
      TAOS_CHECK_EXIT(code);
    }

    code = TSDB_CODE_SUCCESS;
  
    sdbRelease(pMnode->pSdb, pSnode);
  }

  pSnode = NULL;

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmSTAddDnodesToMap(SMnode* pMnode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t lastUpTs = INT64_MIN;
  SDnodeObj *pDnode = NULL;
  void *pIter = NULL;
  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_DNODE, pIter, (void **)&pDnode);
    if (pIter == NULL) {
      break;
    }

    code = taosHashPut(mStreamMgmt.dnodeMap, &pDnode->id, sizeof(pDnode->id), &lastUpTs, sizeof(lastUpTs));
    if (code && TSDB_CODE_DUP_KEY != code) {
      sdbRelease(pMnode->pSdb, pDnode);
      sdbCancelFetch(pMnode->pSdb, pIter);
      pDnode = NULL;
      TAOS_CHECK_EXIT(code);
    }

    code = TSDB_CODE_SUCCESS;
    sdbRelease(pMnode->pSdb, pDnode);
  }

  pDnode = NULL;

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

typedef enum { MST_REG_TASK, MST_REG_VGROUP, MST_REG_SNODE } EStmRegistration;

// A runner can move to another node while an older deployment is still queued.
static int32_t msmRememberRegistration(int64_t streamId, EStmRegistration type, int64_t id) {
  if (!mStreamMgmt.streamMap) return 0;
  SStmStatus* stream = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (!stream) return 0;
  if (!stream->registrations) {
    stream->registrations = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    if (!stream->registrations) return terrno;
  }
  int64_t key[2] = {type, id};
  if (taosHashGet(stream->registrations, key, sizeof(key))) return 0;
  uint8_t present = 1;
  return taosHashPut(stream->registrations, key, sizeof(key), &present, sizeof(present));
}

static int32_t msmRemoveRegisteredTask(int64_t streamId, int64_t taskId) {
  int64_t     key[2] = {streamId, taskId};
  int32_t     code = taosHashRemove(mStreamMgmt.taskMap, key, sizeof(key));
  SStmStatus* stream = mStreamMgmt.streamMap ? taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId)) : NULL;
  if (!code && stream && stream->registrations) {
    key[0] = MST_REG_TASK;
    TAOS_UNUSED(taosHashRemove(stream->registrations, key, sizeof(key)));
  }
  return code;
}

static int32_t msmSTAddToTaskMap(SStmGrpCtx* pCtx, int64_t streamId, SArray* pTasks, SList* pList, SStmTaskStatus* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t taskNum = pTask ? 1 : (pList ? MST_LIST_SIZE(pList) : taosArrayGetSize(pTasks));
  int64_t key[2] = {streamId, 0};
  SListNode* pNode = pList ? listHead(pList) : NULL;
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskStatus* pStatus = pTask ? pTask : (pList ? (SStmTaskStatus*)pNode->data : taosArrayGet(pTasks, i));
    key[1] = pStatus->id.taskId;
    TAOS_CHECK_EXIT(taosHashPut(mStreamMgmt.taskMap, key, sizeof(key), &pStatus, POINTER_BYTES));
    code = msmRememberRegistration(streamId, MST_REG_TASK, key[1]);
    if (code) {
      TAOS_UNUSED(taosHashRemove(mStreamMgmt.taskMap, key, sizeof(key)));
      TAOS_CHECK_EXIT(code);
    }
    mstsDebug("task %" PRIx64" tidx %d added to taskMap", pStatus->id.taskId, pStatus->id.taskIdx);
    if (pNode) {
      pNode = TD_DLIST_NODE_NEXT(pNode);
    }
  }
  
_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}

static int32_t msmSTAddToVgStreamHash(SHashObj* pHash, int64_t streamId, SStmTaskStatus* pStatus, bool trigReader) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmVgStreamStatus* pStream = taosHashGet(pHash, &streamId, sizeof(streamId));
  if (NULL == pStream) {
    SStmVgStreamStatus stream = {0};
    if (trigReader) {
      stream.trigReaders = taosArrayInit(1, POINTER_BYTES);
      TSDB_CHECK_NULL(stream.trigReaders, code, lino, _exit, terrno);
      TSDB_CHECK_NULL(taosArrayPush(stream.trigReaders, &pStatus), code, lino, _exit, terrno);
    } else {
      stream.calcReaders = taosArrayInit(2, POINTER_BYTES);
      TSDB_CHECK_NULL(stream.calcReaders, code, lino, _exit, terrno);
      TSDB_CHECK_NULL(taosArrayPush(stream.calcReaders, &pStatus), code, lino, _exit, terrno);
    }
    TAOS_CHECK_EXIT(taosHashPut(pHash, &streamId, sizeof(streamId), &stream, sizeof(stream)));
    goto _exit;
  }
  
  if (trigReader) {
    if (NULL == pStream->trigReaders) {
      pStream->trigReaders = taosArrayInit(1, POINTER_BYTES);
      TSDB_CHECK_NULL(pStream->trigReaders, code, lino, _exit, terrno);
    }
    
    TSDB_CHECK_NULL(taosArrayPush(pStream->trigReaders, &pStatus), code, lino, _exit, terrno);
    goto _exit;
  }
  
  if (NULL == pStream->calcReaders) {
    pStream->calcReaders = taosArrayInit(1, POINTER_BYTES);
    TSDB_CHECK_NULL(pStream->calcReaders, code, lino, _exit, terrno);
  }

  TSDB_CHECK_NULL(taosArrayPush(pStream->calcReaders, &pStatus), code, lino, _exit, terrno);

_exit:

  if (code) {
    mstsError("%s task %" PRIx64 " SID:%" PRIx64 " failed to add to vgroup %d streamHash in %s at line %d, error:%s", 
        trigReader ? "trigReader" : "calcReader", pStatus->id.taskId, pStatus->id.seriousId, pStatus->id.nodeId, __FUNCTION__, lino, tstrerror(code));
  } else {
    mstsDebug("%s task %" PRIx64 " SID:%" PRIx64 " added to vgroup %d streamHash", 
        trigReader ? "trigReader" : "calcReader", pStatus->id.taskId, pStatus->id.seriousId, pStatus->id.nodeId);
  }

  return code;
}

static int32_t msmSTAddToVgroupMapImpl(int64_t streamId, SStmTaskStatus* pStatus, bool trigReader) {
  TAOS_CHECK_RETURN(msmRememberRegistration(streamId, MST_REG_VGROUP, pStatus->id.nodeId));
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmVgroupStatus vg = {0};

  SStmVgroupStatus* pVg = taosHashGet(mStreamMgmt.vgroupMap, &pStatus->id.nodeId, sizeof(pStatus->id.nodeId));
  if (NULL == pVg) {
    vg.streamTasks = taosHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    TSDB_CHECK_NULL(vg.streamTasks, code, lino, _exit, terrno);
    taosHashSetFreeFp(vg.streamTasks, mstDestroySStmVgStreamStatus);

    vg.lastUpTs = taosGetTimestampMs();
    TAOS_CHECK_EXIT(msmSTAddToVgStreamHash(vg.streamTasks, streamId, pStatus, trigReader));
    TAOS_CHECK_EXIT(taosHashPut(mStreamMgmt.vgroupMap, &pStatus->id.nodeId, sizeof(pStatus->id.nodeId), &vg, sizeof(vg)));
  } else {
    TAOS_CHECK_EXIT(msmSTAddToVgStreamHash(pVg->streamTasks, streamId, pStatus, trigReader));
  }
  
_exit:

  if (code) {
    mstDestroyVgroupStatus(&vg);
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    mstsDebug("task %" PRIx64 " tidx %d added to vgroupMap %d", pStatus->id.taskId, pStatus->id.taskIdx, pStatus->id.nodeId);
  }

  return code;
}

static int32_t msmTDAddToVgroupMap(SHashObj* pVgMap, SStmTaskDeploy* pDeploy, int64_t streamId) {
  TAOS_CHECK_RETURN(msmRememberRegistration(streamId, MST_REG_VGROUP, pDeploy->task.nodeId));
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmVgTasksToDeploy vg = {0};
  SStreamTask* pTask = &pDeploy->task;
  SStmTaskToDeployExt ext = {0};
  ext.deploy = *pDeploy;

  while (true) {
    SStmVgTasksToDeploy* pVg = taosHashAcquire(pVgMap, &pDeploy->task.nodeId, sizeof(pDeploy->task.nodeId));
    if (NULL == pVg) {
      vg.taskList = taosArrayInit(20, sizeof(SStmTaskToDeployExt));
      TSDB_CHECK_NULL(vg.taskList, code, lino, _return, terrno);
      TSDB_CHECK_NULL(taosArrayPush(vg.taskList, &ext), code, lino, _return, terrno);
      code = taosHashPut(pVgMap, &pDeploy->task.nodeId, sizeof(pDeploy->task.nodeId), &vg, sizeof(SStmVgTasksToDeploy));
      if (TSDB_CODE_SUCCESS == code) {
        goto _return;
      }

      if (TSDB_CODE_DUP_KEY != code) {
        goto _return;
      }    

      taosArrayDestroy(vg.taskList);
      vg.taskList = NULL;
      code = TSDB_CODE_SUCCESS;
      continue;
    }

    taosWLockLatch(&pVg->lock);
    if (NULL == pVg->taskList) {
      pVg->taskList = taosArrayInit(20, sizeof(SStmTaskToDeployExt));
    }
    void* added = pVg->taskList ? taosArrayPush(pVg->taskList, &ext) : NULL;
    taosWUnLockLatch(&pVg->lock);
    taosHashRelease(pVgMap, pVg);
    TSDB_CHECK_NULL(added, code, lino, _return, terrno);
    break;
  }
  
_return:

  if (code) {
    taosArrayDestroy(vg.taskList);
    msttError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    int32_t num = atomic_add_fetch_32(&mStreamMgmt.toDeployVgTaskNum, 1);
    msttDebug("task added to toDeployVgTaskNum, vgToDeployTaskNum:%d", num);
  }

  return code;
}


static int32_t msmSTAddToSnodeStreamHash(SHashObj* pHash, int64_t streamId, SStmTaskStatus* pStatus, int32_t deployId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmSnodeStreamStatus* pStream = taosHashGet(pHash, &streamId, sizeof(streamId));
  if (NULL == pStream) {
    SStmSnodeStreamStatus stream = {0};
    if (deployId < 0) {
      stream.trigger = pStatus;
    } else {
      stream.runners[deployId] = taosArrayInit(2, POINTER_BYTES);
      TSDB_CHECK_NULL(stream.runners[deployId], code, lino, _exit, terrno);
      TSDB_CHECK_NULL(taosArrayPush(stream.runners[deployId], &pStatus), code, lino, _exit, terrno);
    }
    
    TAOS_CHECK_EXIT(taosHashPut(pHash, &streamId, sizeof(streamId), &stream, sizeof(stream)));
    goto _exit;
  }
  
  if (deployId < 0) {
    if (NULL != pStream->trigger) {
      mstsWarn("stream already got trigger task %" PRIx64 " SID:%" PRIx64 " in snode %d, replace it with task %" PRIx64 " SID:%" PRIx64, 
          pStream->trigger->id.taskId, pStream->trigger->id.seriousId, pStatus->id.nodeId, pStatus->id.taskId, pStatus->id.seriousId);
    }
    
    pStream->trigger = pStatus;
    goto _exit;
  }
  
  if (NULL == pStream->runners[deployId]) {
    pStream->runners[deployId] = taosArrayInit(2, POINTER_BYTES);
    TSDB_CHECK_NULL(pStream->runners[deployId], code, lino, _exit, terrno);
  }

  TSDB_CHECK_NULL(taosArrayPush(pStream->runners[deployId], &pStatus), code, lino, _exit, terrno);

_exit:

  if (code) {
    mstsError("%s task %" PRIx64 " SID:%" PRIx64 " failed to add to snode %d streamHash deployId:%d in %s at line %d, error:%s", 
        (deployId < 0) ? "trigger" : "runner", pStatus->id.taskId, pStatus->id.seriousId, pStatus->id.nodeId, deployId, __FUNCTION__, lino, tstrerror(code));
  } else {
    mstsDebug("%s task %" PRIx64 " SID:%" PRIx64 " added to snode %d streamHash deployId:%d", 
        (deployId < 0) ? "trigger" : "runner", pStatus->id.taskId, pStatus->id.seriousId, pStatus->id.nodeId, deployId);
  }

  return code;
}


static int32_t msmSTAddToSnodeMapImpl(int64_t streamId, SStmTaskStatus* pStatus, int32_t deployId) {
  TAOS_CHECK_RETURN(msmRememberRegistration(streamId, MST_REG_SNODE, pStatus->id.nodeId));
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SStmSnodeStatus* pSnode = taosHashGet(mStreamMgmt.snodeMap, &pStatus->id.nodeId, sizeof(pStatus->id.nodeId));
  if (NULL == pSnode) {
    mstsWarn("snode %d no longer exists in snodeMap, may be dropped", pStatus->id.nodeId);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  } else {
    if (NULL == pSnode->streamTasks) {
      pSnode->streamTasks = taosHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
      TSDB_CHECK_NULL(pSnode->streamTasks, code, lino, _exit, terrno);
      taosHashSetFreeFp(pSnode->streamTasks, mstDestroySStmSnodeStreamStatus);
    }
    
    TAOS_CHECK_EXIT(msmSTAddToSnodeStreamHash(pSnode->streamTasks, streamId, pStatus, deployId));
  }
  
_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    mstsDebug("%s task %" PRIx64 " tidx %d added to snodeMap, snodeId:%d", (deployId < 0) ? "trigger" : "runner", 
        pStatus->id.taskId, pStatus->id.taskIdx, pStatus->id.nodeId);
  }

  return code;
}

static void msmFreeCopiedExtSpec(SStreamExtTriggerSpec* pSpec) {
  if (pSpec == NULL) {
    return;
  }

  tCleanupSStreamExtTriggerSpec(pSpec);
  taosMemoryFree(pSpec);
}

static int32_t msmCopyExtSpecStringArray(const SArray* pSrc, SArray** ppDst) {
  if (pSrc == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  *ppDst = taosArrayDup(pSrc, NULL);
  if (*ppDst == NULL) {
    return terrno;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t msmCopyExtSpecMappingArray(const SExtColTypeMapping* pSrc, int32_t count,
                                          SExtColTypeMapping** ppDst) {
  if (count <= 0) {
    return TSDB_CODE_SUCCESS;
  }
  if (pSrc == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }

  *ppDst = taosMemoryMalloc(sizeof(SExtColTypeMapping) * count);
  if (*ppDst == NULL) {
    return terrno;
  }
  memcpy(*ppDst, pSrc, sizeof(SExtColTypeMapping) * count);

  return TSDB_CODE_SUCCESS;
}

static int32_t msmCopyExtSpecStringPtrArray(const SArray* pSrc, SArray** ppDst) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (pSrc == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t num = taosArrayGetSize(pSrc);
  *ppDst = taosArrayInit_s(POINTER_BYTES, num);
  TSDB_CHECK_NULL(*ppDst, code, lino, _exit, terrno);
  for (int32_t i = 0; i < num; ++i) {
    char*  pExpr = taosArrayGetP(pSrc, i);
    char** ppExpr = taosArrayGet(*ppDst, i);
    if (pExpr != NULL) {
      *ppExpr = taosStrdup(pExpr);
      TSDB_CHECK_NULL(*ppExpr, code, lino, _exit, terrno);
    }
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    taosArrayDestroyP(*ppDst, NULL);
    *ppDst = NULL;
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  return code;
}

static int32_t msmCopyExtSpecForDeploy(SStmTaskToDeployExt* pExt, const char* streamName, bool privateBuild) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStreamExtTriggerSpec* pSrc = pExt->deploy.msg.reader.pExtSpec;
  SStreamExtTriggerSpec* pDst = taosMemoryCalloc(1, sizeof(SStreamExtTriggerSpec));
  if (pDst == NULL) {
    return terrno;
  }

  // Redeployment refreshes the inline credential and options under actionQLock.
  // The retained stream row owns immutable mappings and expressions for the rest of this copy.
  if (privateBuild) (void)mstWaitLock(&mStreamMgmt.actionQLock, true);
  memcpy(pDst, pSrc, sizeof(SStreamExtTriggerSpec));
  if (privateBuild) taosRUnLockLatch(&mStreamMgmt.actionQLock);
  pDst->triggerColumns = NULL;
  pDst->pColMappings = NULL;
  pDst->calcColumns = NULL;
  pDst->pCalcMappings = NULL;
  pDst->prefilter = NULL;
  pDst->triggerPrefilter = NULL;
  pDst->triggerPrefilterAst = NULL;
  pDst->tagCondJson = NULL;
  pDst->partitionTagCols = NULL;
  pDst->partitionTagExprs = NULL;

  if (pSrc->prefilter != NULL) {
    pDst->prefilter = taosStrdup(pSrc->prefilter);
    if (pDst->prefilter == NULL) {
      msmFreeCopiedExtSpec(pDst);
      return terrno;
    }
  }

  if (pSrc->triggerPrefilter != NULL) {
    pDst->triggerPrefilter = taosStrdup(pSrc->triggerPrefilter);
    if (pDst->triggerPrefilter == NULL) {
      msmFreeCopiedExtSpec(pDst);
      return terrno;
    }
  }

  if (pSrc->triggerPrefilterAst != NULL) {
    pDst->triggerPrefilterAst = taosStrdup(pSrc->triggerPrefilterAst);
    if (pDst->triggerPrefilterAst == NULL) {
      msmFreeCopiedExtSpec(pDst);
      return terrno;
    }
  }

  if (pSrc->tagCondJson != NULL) {
    pDst->tagCondJson = taosStrdup(pSrc->tagCondJson);
    if (pDst->tagCondJson == NULL) {
      msmFreeCopiedExtSpec(pDst);
      return terrno;
    }
  }

  TAOS_CHECK_EXIT(msmCopyExtSpecStringArray(pSrc->triggerColumns, &pDst->triggerColumns));
  TAOS_CHECK_EXIT(msmCopyExtSpecMappingArray(pSrc->pColMappings, pSrc->numColMappings, &pDst->pColMappings));
#ifdef BUILD_TEST_SEAM
  if (gTdSeam_msmDeployPhase) gTdSeam_msmDeployPhase("ext-reader-copy", streamName);
#endif
  TAOS_CHECK_EXIT(msmCopyExtSpecStringArray(pSrc->calcColumns, &pDst->calcColumns));
  TAOS_CHECK_EXIT(msmCopyExtSpecMappingArray(pSrc->pCalcMappings, pSrc->numCalcMappings, &pDst->pCalcMappings));

  if (pSrc->partitionTagCols != NULL) {
    pDst->partitionTagCols = taosArrayDup(pSrc->partitionTagCols, NULL);
    if (pDst->partitionTagCols == NULL) {
      msmFreeCopiedExtSpec(pDst);
      return terrno;
    }
  }

  TAOS_CHECK_EXIT(msmCopyExtSpecStringPtrArray(pSrc->partitionTagExprs, &pDst->partitionTagExprs));

  pExt->deploy.msg.reader.pExtSpec = pDst;
  pExt->deploy.msg.reader.ownsExtSpec = true;
  pExt->ownsExtSpec = true;
  return TSDB_CODE_SUCCESS;

_exit:
  msmFreeCopiedExtSpec(pDst);
  return code;
}

static int32_t msmTDAddTriggerToSnodeMap(SStmTaskDeploy* pDeploy, SStreamObj* pStream) {
  TAOS_CHECK_RETURN(msmRememberRegistration(pDeploy->task.streamId, MST_REG_SNODE, pDeploy->task.nodeId));
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStmSnodeTasksDeploy snode = {0};
  SStmTaskToDeployExt ext = {0};
  SStreamTask* pTask = &pDeploy->task;

  ext.deploy = *pDeploy;
  ext.deployed = false;
  if (pDeploy->task.type == STREAM_READER_TASK && pDeploy->msg.reader.pExtSpec != NULL) {
    code = msmCopyExtSpecForDeploy(&ext, pStream->name, false);
    if (code != TSDB_CODE_SUCCESS) {
      goto _return;
    }
  }

  while (true) {
    SStmSnodeTasksDeploy* pSnode = taosHashAcquire(mStreamMgmt.toDeploySnodeMap, &pDeploy->task.nodeId, sizeof(pDeploy->task.nodeId));
    if (NULL == pSnode) {
      snode.triggerList = taosArrayInit(10, sizeof(SStmTaskToDeployExt));
      TSDB_CHECK_NULL(snode.triggerList, code, lino, _return, terrno);

      TSDB_CHECK_NULL(taosArrayPush(snode.triggerList, &ext), code, lino, _return, terrno);

      code = taosHashPut(mStreamMgmt.toDeploySnodeMap, &pDeploy->task.nodeId, sizeof(pDeploy->task.nodeId), &snode, sizeof(snode));
      if (TSDB_CODE_SUCCESS == code) {
        goto _return;
      }

      if (TSDB_CODE_DUP_KEY != code) {
        goto _return;
      }    

      taosArrayDestroy(snode.triggerList);
      snode.triggerList = NULL;
      code = TSDB_CODE_SUCCESS;
      continue;
    }
    
    taosWLockLatch(&pSnode->lock);
    if (NULL == pSnode->triggerList) {
      pSnode->triggerList = taosArrayInit(10, sizeof(SStmTaskToDeployExt));
      if (NULL == pSnode->triggerList) {
        taosWUnLockLatch(&pSnode->lock);
        taosHashRelease(mStreamMgmt.toDeploySnodeMap, pSnode);
        TSDB_CHECK_NULL(pSnode->triggerList, code, lino, _return, terrno);
      }
    }
    
    if (NULL == taosArrayPush(pSnode->triggerList, &ext)) {
      taosWUnLockLatch(&pSnode->lock);
      taosHashRelease(mStreamMgmt.toDeploySnodeMap, pSnode);
      TSDB_CHECK_NULL(NULL, code, lino, _return, terrno);
    }
    taosWUnLockLatch(&pSnode->lock);
    
    taosHashRelease(mStreamMgmt.toDeploySnodeMap, pSnode);
    break;
  }
  
_return:

  if (code != TSDB_CODE_SUCCESS && snode.triggerList != NULL) {
    taosArrayDestroy(snode.triggerList);
    snode.triggerList = NULL;
  }
  if (code == TSDB_CODE_SUCCESS && pDeploy->task.type == STREAM_TRIGGER_TASK) {
    pDeploy->msg.trigger.pWindowPlan = NULL;
    pDeploy->msg.trigger.pTimeAlign = NULL;
  }

  if (code) {
    taosArrayDestroy(snode.triggerList);
    if (ext.ownsExtSpec) {
      msmFreeCopiedExtSpec(ext.deploy.msg.reader.pExtSpec);
    }
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    msttDebug("trigger task added to toDeploySnodeMap, tidx:%d", pTask->taskIdx);
  }

  return code;
}

static int32_t msmTDAddRunnerToSnodeMap(SStmTaskDeploy* pDeploy, SStreamObj* pStream) {
  TAOS_CHECK_RETURN(msmRememberRegistration(pDeploy->task.streamId, MST_REG_SNODE, pDeploy->task.nodeId));
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStmSnodeTasksDeploy snode = {0};
  SStmTaskToDeployExt ext = {0};
  SStreamTask* pTask = &pDeploy->task;

  while (true) {
    SStmSnodeTasksDeploy* pSnode = taosHashAcquire(mStreamMgmt.toDeploySnodeMap, &pDeploy->task.nodeId, sizeof(pDeploy->task.nodeId));
    if (NULL == pSnode) {
      snode.runnerList = taosArrayInit(10, sizeof(SStmTaskToDeployExt));
      TSDB_CHECK_NULL(snode.runnerList, code, lino, _return, terrno);

      ext.deploy = *pDeploy;
      ext.deployed = false;
      TSDB_CHECK_NULL(taosArrayPush(snode.runnerList, &ext), code, lino, _return, terrno);

      code = taosHashPut(mStreamMgmt.toDeploySnodeMap, &pDeploy->task.nodeId, sizeof(pDeploy->task.nodeId), &snode, sizeof(snode));
      if (TSDB_CODE_SUCCESS == code) {
        goto _return;
      }

      if (TSDB_CODE_DUP_KEY != code) {
        goto _return;
      }    

      taosArrayDestroy(snode.runnerList);
      snode.runnerList = NULL;
      code = TSDB_CODE_SUCCESS;
      continue;
    }
    
    taosWLockLatch(&pSnode->lock);
    if (NULL == pSnode->runnerList) {
      pSnode->runnerList = taosArrayInit(10, sizeof(SStmTaskToDeployExt));
      if (NULL == pSnode->runnerList) {
        taosWUnLockLatch(&pSnode->lock);
        taosHashRelease(mStreamMgmt.toDeploySnodeMap, pSnode);
        TSDB_CHECK_NULL(NULL, code, lino, _return, terrno);
      }
    }
    
    ext.deploy = *pDeploy;
    ext.deployed = false;
    
    if (NULL == taosArrayPush(pSnode->runnerList, &ext)) {
      taosWUnLockLatch(&pSnode->lock);
      taosHashRelease(mStreamMgmt.toDeploySnodeMap, pSnode);
      TSDB_CHECK_NULL(NULL, code, lino, _return, terrno);
    }
    taosWUnLockLatch(&pSnode->lock);
    
    taosHashRelease(mStreamMgmt.toDeploySnodeMap, pSnode);
    break;
  }
  
_return:

  if (code) {
    taosArrayDestroy(snode.runnerList);
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    msttDebug("task added to toDeploySnodeMap, tidx:%d", pTask->taskIdx);
  }

  return code;
}

// Route an EXT-source calc reader task to toDeploySnodeMap so snode heartbeat
// picks it up.  The deploy info must have task.nodeId set to the real snodeId
// (not SNODE_HANDLE) before calling this function.
static int32_t msmTDAddCalcReaderToSnodeMap(SStmTaskToDeployExt* entry) {
  SStreamTask* pTask = &entry->deploy.task;
  TAOS_CHECK_RETURN(msmRememberRegistration(pTask->streamId, MST_REG_SNODE, pTask->nodeId));
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmSnodeTasksDeploy snode = {0};

  while (true) {
    SStmSnodeTasksDeploy* pSnode = taosHashAcquire(mStreamMgmt.toDeploySnodeMap, &pTask->nodeId, sizeof(pTask->nodeId));
    if (NULL == pSnode) {
      snode.calcReaderList = taosArrayInit(10, sizeof(SStmTaskToDeployExt));
      TSDB_CHECK_NULL(snode.calcReaderList, code, lino, _return, terrno);

      TSDB_CHECK_NULL(taosArrayPush(snode.calcReaderList, entry), code, lino, _return, terrno);

      code = taosHashPut(mStreamMgmt.toDeploySnodeMap, &pTask->nodeId, sizeof(pTask->nodeId), &snode, sizeof(snode));
      if (TSDB_CODE_SUCCESS == code) {
        goto _return;
      }
      if (TSDB_CODE_DUP_KEY != code) {
        goto _return;
      }
      taosArrayDestroy(snode.calcReaderList);
      snode.calcReaderList = NULL;
      code = TSDB_CODE_SUCCESS;
      continue;
    }

    taosWLockLatch(&pSnode->lock);
    if (NULL == pSnode->calcReaderList) {
      pSnode->calcReaderList = taosArrayInit(10, sizeof(SStmTaskToDeployExt));
    }
    void* added = pSnode->calcReaderList ? taosArrayPush(pSnode->calcReaderList, entry) : NULL;
    taosWUnLockLatch(&pSnode->lock);
    taosHashRelease(mStreamMgmt.toDeploySnodeMap, pSnode);
    TSDB_CHECK_NULL(added, code, lino, _return, terrno);
    break;
  }

_return:
  if (code) {
    taosArrayDestroy(snode.calcReaderList);
    mstError("msmTDAddCalcReaderToSnodeMap failed at line %d, error:%s", lino, tstrerror(code));
  } else {
    (void)atomic_add_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);
    msttDebug("EXT calc reader task added to toDeploySnodeMap, snodeId:%d tidx:%d", pTask->nodeId, pTask->taskIdx);
  }
  return code;
}

typedef struct SStmReaderBinding {
  int64_t taskId;
  int32_t nodeId;
  int32_t taskIdx;
} SStmReaderBinding;

typedef struct SStmMetadataRecovery {
  int32_t groupId;
  int32_t skippedPartitions;
} SStmMetadataRecovery;

typedef struct SStmDeployDraft {
  SHashObj* scanMap;
  SHashObj* checkedMetadataGroups;
  SHashObj* metadataCoverage;
  SHashObj* metadataScanIds;
  SHashObj* metadataCoverageCounts;
  SHashObj* boundMetadataVgroups;
  uint8_t   metadataSnapshotId;  // A draft is validated at most twice (one restore retry).
  int32_t   metadataBindingsSeen;
  SArray*   tasks;  // SStmTaskToDeployExt, owned until publication.
  int64_t   epoch;
  int32_t   preferredExtSnode;  // Sampled once per draft; -1 means use the normal placement policy.
  SHashObj* endpoints;  // (SDB table, node id) -> endpoint set embedded by the builder.
  int64_t   triggerDbUid;
  int32_t   triggerVgVersion;
} SStmDeployDraft;

static int32_t msmRememberEndpoint(SStmGrpCtx* ctx, ESdbType type, int32_t nodeId, const SEpSet* ep) {
  if (!ctx || !ctx->draft) return 0;
  SStmDeployDraft* draft = ctx->draft;
  if (!draft->endpoints) {
    draft->endpoints = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    if (!draft->endpoints) return terrno;
  }
  int32_t key[2] = {type, nodeId};
  SEpSet* seen = taosHashGet(draft->endpoints, key, sizeof(key));
  if (seen) return isEpsetEqual(seen, ep) ? 0 : TSDB_CODE_MND_STREAM_NOT_AVAILABLE;
  return taosHashPut(draft->endpoints, key, sizeof(key), ep, sizeof(*ep));
}

static int32_t msmStageTask(SStmDeployDraft* draft, SStmTaskDeploy* task) {
  SStmTaskToDeployExt entry = {.deploy = *task};
  return taosArrayPush(draft->tasks, &entry) ? TSDB_CODE_SUCCESS : terrno;
}

// The queue takes the entire payload only after a successful append.
static int32_t msmPublishPreparedReader(SStmTaskToDeployExt* entry) {
  SStmTaskDeploy* task = &entry->deploy;
  if (entry->ownsExtSpec) {
    TAOS_CHECK_RETURN(msmTDAddCalcReaderToSnodeMap(entry));
  } else {
    TAOS_CHECK_RETURN(msmTDAddToVgroupMap(mStreamMgmt.toDeployVgMap, task, task->task.streamId));
  }
  *entry = (SStmTaskToDeployExt){.deployed = true};
  return TSDB_CODE_SUCCESS;
}

static int32_t msmQueueReader(SStmGrpCtx* ctx, SStmTaskDeploy* task, SStreamObj* stream) {
  SStmTaskToDeployExt entry = {.deploy = *task};
#ifdef TD_ENTERPRISE
  if (task->msg.reader.pExtSpec && stream) {
    TAOS_CHECK_RETURN(msmCopyExtSpecForDeploy(&entry, stream->name, ctx && ctx->draft));
  }
#endif
  int32_t code;
  if (ctx && ctx->draft) {
    code = taosArrayPush(ctx->draft->tasks, &entry) ? TSDB_CODE_SUCCESS : terrno;
  } else {
    code = msmPublishPreparedReader(&entry);
  }
  // The caller still owns the task's scan plan when staging or publication fails.
  if (code && entry.ownsExtSpec) msmFreeCopiedExtSpec(entry.deploy.msg.reader.pExtSpec);
  return code;
}

// After plan serialization the temporary task list owns every string until staged.
static int32_t msmStageRunnerTasks(SStmDeployDraft* draft, SArray* tasks) {
  for (int32_t i = 0; i < taosArrayGetSize(tasks); ++i) {
    int32_t code = msmStageTask(draft, taosArrayGet(tasks, i));
    if (code) {
      for (; i < taosArrayGetSize(tasks); ++i) {
        SStmTaskDeploy* task = taosArrayGet(tasks, i);
        taosMemoryFreeClear(task->msg.runner.pPlan);
      }
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t msmUpdateSnodeUpTs(SStmGrpCtx* pCtx) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  SStmSnodeStatus* pStatus = NULL;
  bool     noExists = false;

  while (true) {
    pStatus = taosHashGet(mStreamMgmt.snodeMap, &pCtx->pReq->snodeId, sizeof(pCtx->pReq->snodeId));
    if (NULL == pStatus) {
      if (noExists) {
        mstWarn("snode %d does not exist in snodeMap, may be dropped, ignore it", pCtx->pReq->snodeId);
        TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_NODE_NOT_EXISTS);
      }

      noExists = true;
      TAOS_CHECK_EXIT(msmSTAddSnodesToMap(pCtx->pMnode));

      continue;
    }

    atomic_store_32(&pStatus->runnerThreadNum, pCtx->pReq->runnerThreadNum);
    
    while (true) {
      int64_t lastTsValue = atomic_load_64(&pStatus->lastUpTs);
      if (pCtx->currTs > lastTsValue) {
        if (lastTsValue == atomic_val_compare_exchange_64(&pStatus->lastUpTs, lastTsValue, pCtx->currTs)) {
          mstDebug("snode %d lastUpTs updated", pCtx->pReq->snodeId);
          return code;
        }

        continue;
      }

      return code;
    }

    break;
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;  
}

void msmUpdateVgroupUpTs(SStmGrpCtx* pCtx, int32_t vgId) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  SStmVgroupStatus* pStatus = taosHashGet(mStreamMgmt.vgroupMap, &vgId, sizeof(vgId));
  if (NULL == pStatus) {
    mstDebug("vgroup %d does not exist in vgroupMap, ignore update upTs", vgId);
    return;
  }

  while (true) {
    int64_t lastTsValue = atomic_load_64(&pStatus->lastUpTs);
    if (pCtx->currTs > lastTsValue) {
      if (lastTsValue == atomic_val_compare_exchange_64(&pStatus->lastUpTs, lastTsValue, pCtx->currTs)) {
        mstDebug("vgroup %d lastUpTs updated to %" PRId64, vgId, pCtx->currTs);
        return;
      }

      continue;
    }

    return;
  }  
}

int32_t msmUpdateVgroupsUpTs(SStmGrpCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t vgNum = taosArrayGetSize(pCtx->pReq->pVgLeaders);

  mstDebug("start to update vgroups upTs");
  
  for (int32_t i = 0; i < vgNum; ++i) {
    int32_t* vgId = taosArrayGet(pCtx->pReq->pVgLeaders, i);

    msmUpdateVgroupUpTs(pCtx, *vgId);
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}



void* msmSearchCalcCacheScanPlan(SArray* pList) {
  int32_t num = taosArrayGetSize(pList);
  for (int32_t i = 0; i < num; ++i) {
    SStreamCalcScan* pScan = taosArrayGet(pList, i);
    if (pScan->readFromCache) {
      return pScan->scanPlan;
    }
  }

  return NULL;
}

/* For EXT-source (federated) streams the calc scan plans have readFromCache=false.
 * Search for any scan plan regardless of readFromCache so the trigger reader can
 * build calcColumns/pCalcMappings for %%trows processing. */
void* msmSearchCalcExtScanPlan(SArray* pList) {
  int32_t num = taosArrayGetSize(pList);
  for (int32_t i = 0; i < num; ++i) {
    SStreamCalcScan* pScan = taosArrayGet(pList, i);
    if (!pScan->readFromCache && pScan->scanPlan != NULL) {
      return pScan->scanPlan;
    }
  }

  return NULL;
}

int32_t msmBuildReaderDeployInfo(SStmTaskDeploy* pDeploy, void* calcScanPlan, SStmStatus* pInfo, bool triggerReader) {
  SStreamReaderDeployMsg* pMsg = &pDeploy->msg.reader;
  pMsg->triggerReader = triggerReader;
  // Calc readers can be built without a create request (legacy/default charset).
  pDeploy->task.charset[0] = 0;
  if (pInfo->pCreate != NULL) {
    tstrncpy(pDeploy->task.charset, pInfo->pCreate->charset, sizeof(pDeploy->task.charset));
  }
  
  if (triggerReader) {
    SStreamReaderDeployFromTrigger* pTrigger = &pMsg->msg.trigger;
    pTrigger->triggerTblName = pInfo->pCreate->triggerTblName;
    pTrigger->triggerTblUid = pInfo->pCreate->triggerTblUid;
    pTrigger->triggerTblSuid = pInfo->pCreate->triggerTblSuid;
    pTrigger->triggerTblType = pInfo->pCreate->triggerTblType;
    pTrigger->isTriggerTblVirt = STREAM_IS_VIRTUAL_TABLE(pInfo->pCreate->triggerTblType, pInfo->pCreate->flags);
    pTrigger->requireLocalVtableRefs = pInfo->pCreate->pTimeAlign != NULL;
    pTrigger->timeAlignConsumer = pTrigger->requireLocalVtableRefs && pInfo->pCreate->triggerType != WINDOW_TYPE_PERIOD;
    pTrigger->deleteReCalc = pInfo->pCreate->deleteReCalc;
    pTrigger->deleteOutTbl = pInfo->pCreate->deleteOutTbl;
    pTrigger->partitionCols = pInfo->pCreate->partitionCols;
    pTrigger->rollupTagCols = pInfo->pCreate->rollupTagCols;
    pTrigger->triggerCols = pInfo->pCreate->triggerCols;
    //pTrigger->triggerPrevFilter = pStream->pCreate->triggerPrevFilter;
    pTrigger->triggerScanPlan = pInfo->pCreate->triggerScanPlan;
    pTrigger->calcCacheScanPlan = msmSearchCalcCacheScanPlan(pInfo->pCreate->calcScanPlanList);
  } else {
    if (mstRunnerSnapshotUnknown(pInfo)) {
      return TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
    }

    SStreamReaderDeployFromCalc* pCalc = &pMsg->msg.calc;
    pCalc->execReplica = pInfo->runnerDeploys * atomic_load_32(&pInfo->runnerReplica);
    if (calcScanPlan) {
      pCalc->calcScanPlan = taosStrdup(calcScanPlan);
      if (NULL == pCalc->calcScanPlan) {
        return terrno;
      }
      pCalc->freeScanPlan = true;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t msmBuildTriggerRunnerTargets(SMnode* pMnode, SStmStatus* pInfo, int64_t streamId, SArray** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (mstRunnerSnapshotUnknown(pInfo)) {
    return TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
  }

  if (pInfo->runnerDeploys > 0) {
    *ppRes = taosArrayInit(pInfo->runnerDeploys, sizeof(SStreamRunnerTarget));
    TSDB_CHECK_NULL(*ppRes, code, lino, _exit, terrno);
  }

  for (int32_t i = 0; i < pInfo->runnerDeploys; ++i) {
    SStmTaskStatus* pStatus = taosArrayGetLast(pInfo->runners[i]);
    TSDB_CHECK_NULL(pStatus, code, lino, _exit, terrno);

    if (!STREAM_IS_TOP_RUNNER(pStatus->flags)) {
      mstsError("the last runner task %" PRIx64 " SID:%" PRId64 " tidx:%d in deploy %d is not top runner", 
          pStatus->id.taskId, pStatus->id.seriousId, pStatus->id.taskIdx, i);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);    
    }
    
    SStreamRunnerTarget runner;
    runner.addr.taskId = pStatus->id.taskId;
    runner.addr.nodeId = pStatus->id.nodeId;
    runner.addr.epset = mndGetDnodeEpsetById(pMnode, pStatus->id.nodeId);
    runner.execReplica = atomic_load_32(&pInfo->runnerReplica);
    TSDB_CHECK_NULL(taosArrayPush(*ppRes, &runner), code, lino, _exit, terrno);
    mstsDebug("the %dth runner target added to trigger's runnerList, TASK:%" PRIx64 , i, runner.addr.taskId);
  }

_exit:

  if (code) {
    taosArrayDestroy(*ppRes);
    *ppRes = NULL;
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmBuildStreamSnodeInfo(SMnode* pMnode, SStreamObj* pStream, SStreamSnodeInfo* pInfo) {
  int64_t streamId = pStream->pCreate->streamId;
  int32_t leaderSnodeId = atomic_load_32(&pStream->mainSnodeId);
  SSnodeObj* pSnode = mndAcquireSnode(pMnode, leaderSnodeId);
  if (NULL == pSnode) {
    mstsError("snode %d not longer exists, ignore build stream snode info", leaderSnodeId);
    return TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
  }
  
  pInfo->leaderSnodeId = leaderSnodeId;
  pInfo->replicaSnodeId = pSnode->replicaId;

  mndReleaseSnode(pMnode, pSnode);

  pInfo->leaderEpSet = mndGetDnodeEpsetById(pMnode, pInfo->leaderSnodeId);
  if (GOT_SNODE(pInfo->replicaSnodeId)) {
    pInfo->replicaEpSet = mndGetDnodeEpsetById(pMnode, pInfo->replicaSnodeId);
  }

  return TSDB_CODE_SUCCESS;
}

static EDealRes msmCollectPrecreateOutputOptions(SNode* pNode, void* pContext) {
  int32_t* pOptions = pContext;
  if (nodeType(pNode) == QUERY_NODE_COLUMN) {
    BIT_FLAG_SET_MASK(*pOptions, STREAM_OPTION_OUTPUT_NEEDS_GROUP_VALUES);
  }
  if (nodeType(pNode) != QUERY_NODE_FUNCTION) return DEAL_RES_CONTINUE;

  switch (((SFunctionNode*)pNode)->funcType) {
    case FUNCTION_TYPE_TBNAME:
    case FUNCTION_TYPE_PLACEHOLDER_COLUMN:
    case FUNCTION_TYPE_PLACEHOLDER_TBNAME:
    case FUNCTION_TYPE_PLACEHOLDER_ROLLUP_TAG:
      BIT_FLAG_SET_MASK(*pOptions, STREAM_OPTION_OUTPUT_NEEDS_GROUP_VALUES);
      break;
    case FUNCTION_TYPE_TPREV_TS:
    case FUNCTION_TYPE_TCURRENT_TS:
    case FUNCTION_TYPE_TNEXT_TS:
    case FUNCTION_TYPE_TWSTART:
    case FUNCTION_TYPE_TWEND:
    case FUNCTION_TYPE_TPREV_WSTART:
    case FUNCTION_TYPE_TPREV_WEND:
    case FUNCTION_TYPE_TWDURATION:
    case FUNCTION_TYPE_TWROWNUM:
    case FUNCTION_TYPE_TPREV_LOCALTIME:
    case FUNCTION_TYPE_TLOCALTIME:
    case FUNCTION_TYPE_TNEXT_LOCALTIME:
    case FUNCTION_TYPE_TIDLESTART:
    case FUNCTION_TYPE_TIDLEEND:
    case FUNCTION_TYPE_TEVENT_CONDITION_PATH:
      BIT_FLAG_SET_MASK(*pOptions, STREAM_OPTION_OUTPUT_NEEDS_TRIGGER_VALUES);
      break;
    default:
      break;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t msmSetPrecreateOutputOptions(const SCMCreateStreamReq* pCreate, int32_t* pOptions) {
  SNode* pName = NULL;
  SNodeList* pTags = NULL;
  int32_t outputOptions = STREAM_OPTION_PRECREATE_OUTPUT_ANALYZED;
  int32_t code = TSDB_CODE_SUCCESS;

  // Serialized output expressions are available even in requests from older clients.
  if (pCreate->subTblNameExpr != NULL) {
    TAOS_CHECK_GOTO(nodesStringToNode(pCreate->subTblNameExpr, &pName), NULL, _exit);
    nodesWalkExpr(pName, msmCollectPrecreateOutputOptions, &outputOptions);
  }
  if (pCreate->tagValueExpr != NULL) {
    TAOS_CHECK_GOTO(nodesStringToList(pCreate->tagValueExpr, &pTags), NULL, _exit);
    nodesWalkExprs(pTags, msmCollectPrecreateOutputOptions, &outputOptions);
  }
  BIT_FLAG_UNSET_MASK(*pOptions, STREAM_OPTION_PRECREATE_OUTPUT_ANALYZED | STREAM_OPTION_OUTPUT_NEEDS_TRIGGER_VALUES |
                                   STREAM_OPTION_OUTPUT_NEEDS_GROUP_VALUES);
  *pOptions |= outputOptions;

_exit:
  nodesDestroyNode(pName);
  nodesDestroyList(pTags);
  return code;
}

int32_t msmBuildTriggerDeployInfo(SMnode* pMnode, SStmStatus* pInfo, SStmTaskDeploy* pDeploy, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStreamTriggerDeployMsg* pMsg = &pDeploy->msg.trigger;
  
  pMsg->triggerType = pStream->pCreate->triggerType;
  pMsg->igDisorder = pStream->pCreate->igDisorder;
  pMsg->fillHistory = pStream->pCreate->fillHistory;
  pMsg->fillHistoryFirst = pStream->pCreate->fillHistoryFirst;
  pMsg->lowLatencyCalc = pStream->pCreate->lowLatencyCalc;
  pMsg->igNoDataTrigger = pStream->pCreate->igNoDataTrigger;
  pMsg->enableMultiGroupCalc = pStream->pCreate->enableMultiGroupCalc;
  pMsg->isTriggerTblVirt = STREAM_IS_VIRTUAL_TABLE(pStream->pCreate->triggerTblType, pStream->pCreate->flags);
  pMsg->triggerHasPF = pStream->pCreate->triggerHasPF;
  pMsg->isTriggerTblStb = (pStream->pCreate->triggerTblType == TSDB_SUPER_TABLE);
  pMsg->precision = pStream->pCreate->triggerPrec;
  pMsg->partitionCols = pInfo->pCreate->partitionCols;
  pMsg->rollupTagCols = pInfo->pCreate->rollupTagCols;

  pMsg->pNotifyAddrUrls = pInfo->pCreate->pNotifyAddrUrls;
  pMsg->notifyEventTypes = pStream->pCreate->notifyEventTypes;
  pMsg->addOptions = pStream->pCreate->addOptions;
  pMsg->notifyHistory = pStream->pCreate->notifyHistory;
  if (BIT_FLAG_TEST_MASK(pMsg->addOptions, STREAM_OPTION_TIME_ALIGN) != (pStream->pCreate->pTimeAlign != NULL)) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PARA);
  }
  TAOS_CHECK_EXIT(tCloneStreamTimeAlignSpec(pStream->pCreate->pTimeAlign, &pMsg->pTimeAlign));
  if (BIT_FLAG_TEST_MASK(pMsg->addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN)) {
    if (pStream->pCreate->pWindowPlan == NULL) {
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PARA);
    }
    TAOS_CHECK_EXIT(tCloneStreamWindowPlan(pStream->pCreate->pWindowPlan, &pMsg->pWindowPlan));
  } else if (pStream->pCreate->pWindowPlan != NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PARA);
  }

  pMsg->maxDelay = pStream->pCreate->maxDelay;
  pMsg->maxDelayTagColId = pStream->pCreate->maxDelayTagColId;
  pMsg->fillHistoryStartTime = pStream->pCreate->fillHistoryStartTime;
  pMsg->watermark = pStream->pCreate->watermark;
  pMsg->expiredTime = pStream->pCreate->expiredTime;
  pMsg->idleTimeoutMs = pStream->pCreate->idleTimeoutMs;
  pMsg->trigger = pInfo->pCreate->trigger;

  pMsg->eventTypes = pStream->pCreate->eventTypes;
  pMsg->placeHolderBitmap = pStream->pCreate->placeHolderBitmap;
  pMsg->calcTsSlotId = pStream->pCreate->calcTsSlotId;
  pMsg->triTsSlotId = pStream->pCreate->triTsSlotId;
  pMsg->calcPkSlotId = pStream->pCreate->calcPkSlotId;
  pMsg->triPkSlotId = pStream->pCreate->triPkSlotId;
  pMsg->triggerPrevFilter = pInfo->pCreate->triggerPrevFilter;
  bool nestedCacheRows = !STREAM_IS_VIRTUAL_TABLE(pStream->pCreate->triggerTblType, pStream->pCreate->flags) &&
                         !STREAM_IS_REF_EXT_SOURCE(pDeploy->task.flags) &&
                         BIT_FLAG_TEST_MASK(pStream->pCreate->addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN) &&
                         BIT_FLAG_TEST_MASK(pStream->pCreate->placeHolderBitmap, PLACE_HOLDER_PARTITION_ROWS);
  if (STREAM_IS_VIRTUAL_TABLE(pStream->pCreate->triggerTblType, pStream->pCreate->flags) || nestedCacheRows) {
    pMsg->triggerScanPlan = pInfo->pCreate->triggerScanPlan;
    pMsg->calcCacheScanPlan = msmSearchCalcCacheScanPlan(pInfo->pCreate->calcScanPlanList);
  }

  SStreamTaskAddr addr;
  int32_t triggerReaderNum = taosArrayGetSize(pInfo->trigReaders);
  if (triggerReaderNum > 0) {
    pMsg->readerList = taosArrayInit(triggerReaderNum, sizeof(SStreamTaskAddr));
    TSDB_CHECK_NULL(pMsg->readerList, code, lino, _exit, terrno);
  }
  
  for (int32_t i = 0; i < triggerReaderNum; ++i) {
    SStmTaskStatus* pStatus = taosArrayGet(pInfo->trigReaders, i);
    addr.taskId = pStatus->id.taskId;
    addr.nodeId = pStatus->id.nodeId;
    /* Resolve each trigger reader by its own deployment type.  Stream-level EXT
     * flags also cover external calc readers and cannot identify this address. */
    if (STREAM_IS_REF_EXT_SOURCE(pStatus->flags)) {
      addr.epset = mndGetDnodeEpsetById(pMnode, pStatus->id.nodeId);
      mstsDebug("the %dth ext trigReader src added to trigger's readerList via dnode epset, "
                "TASK:%" PRIx64 " snodeId:%d", i, addr.taskId, addr.nodeId);
    } else {
      addr.epset = mndGetVgroupEpsetById(pMnode, pStatus->id.nodeId);
      mstsDebug("the %dth local trigReader src added to trigger's readerList via vgroup epset, "
                "TASK:%" PRIx64 " nodeId:%d",
                i, addr.taskId, addr.nodeId);
    }
    TSDB_CHECK_NULL(taosArrayPush(pMsg->readerList, &addr), code, lino, _exit, terrno);
  }

  pMsg->leaderSnodeId = pStream->mainSnodeId;
  pMsg->streamName = pInfo->streamName;
  // The create-stream transaction already creates a fixed normal output table.
  pMsg->nodelayCreateSubtable = pStream->pCreate->nodelayCreateSubtable &&
                                (!pMsg->isTriggerTblVirt || pStream->pCreate->outTblType == TSDB_SUPER_TABLE);
  if (pMsg->nodelayCreateSubtable && pMsg->isTriggerTblVirt) {
    TAOS_CHECK_EXIT(msmSetPrecreateOutputOptions(pInfo->pCreate, &pMsg->addOptions));
  }
  pMsg->timeConfig = pStream->pCreate->timeConfig;
  tstrncpy(pDeploy->task.charset, pStream->pCreate->charset, sizeof(pDeploy->task.charset));

  if (0 == pInfo->runnerNum) {
    mstsDebug("no runner task, skip set trigger's runner list, deployNum:%d", pInfo->runnerDeploys);
    return code;
  }

  TAOS_CHECK_EXIT(msmBuildTriggerRunnerTargets(pMnode, pInfo, streamId, &pMsg->runnerList));

_exit:

  if (code) {
    taosArrayDestroy(pMsg->readerList);
    pMsg->readerList = NULL;
    taosArrayDestroy(pMsg->runnerList);
    pMsg->runnerList = NULL;
    tDestroyStreamWindowPlan(&pMsg->pWindowPlan);
    tDestroyStreamTimeAlignSpec(&pMsg->pTimeAlign);
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    mstsDebug("trigger deploy info built, readerNum:%d, runnerNum:%d", (int32_t)taosArrayGetSize(pMsg->readerList), (int32_t)taosArrayGetSize(pMsg->runnerList));
  }

  return code;
}

int32_t msmBuildRunnerDeployInfo(SStmTaskDeploy* pDeploy, SSubplan* plan, SStreamObj* pStream, SStmStatus* pInfo,
                                 bool topPlan) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (mstRunnerSnapshotUnknown(pInfo)) {
    return TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
  }

  int64_t                 streamId = pStream->pCreate->streamId;
  SStreamRunnerDeployMsg* pMsg = &pDeploy->msg.runner;
  tstrncpy(pDeploy->task.charset, pStream->pCreate->charset, sizeof(pDeploy->task.charset));
  //TAOS_CHECK_EXIT(qSubPlanToString(plan, &pMsg->pPlan, NULL));

  pMsg->execReplica = atomic_load_32(&pInfo->runnerReplica);
  pMsg->streamName = pInfo->streamName;
  //TAOS_CHECK_EXIT(nodesCloneNode((SNode*)plan, (SNode**)&pMsg->pPlan));
  pMsg->pPlan = plan;
  pMsg->outDBFName = pInfo->pCreate->outDB;
  pMsg->outTblName = pInfo->pCreate->outTblName;
  pMsg->outTblType = pStream->pCreate->outTblType;
  pMsg->lowLatencyCalc = pStream->pCreate->lowLatencyCalc;
  pMsg->calcNotifyOnly = pStream->pCreate->calcNotifyOnly;
  pMsg->topPlan = topPlan;
  pMsg->pNotifyAddrUrls = pInfo->pCreate->pNotifyAddrUrls;
  pMsg->addOptions = pStream->pCreate->addOptions;
  // The Runner enforces admission from deployment, including for create-table retries.
  // PERIOD consumes only metadata and never owns a bounded input round.
  if (pStream->pCreate->triggerType == WINDOW_TYPE_PERIOD)
    BIT_FLAG_UNSET_MASK(pMsg->addOptions, STREAM_OPTION_TIME_ALIGN);
  const int64_t openCloseEvents = EVENT_WINDOW_OPEN | EVENT_WINDOW_CLOSE;
  // OPEN and CLOSE calculations share the window's starting row.
  if ((WINDOW_TYPE_INTERVAL == pStream->pCreate->triggerType && pStream->pCreate->trigger.sliding.overlap) ||
      (WINDOW_TYPE_COUNT == pStream->pCreate->triggerType &&
       pStream->pCreate->trigger.count.sliding < pStream->pCreate->trigger.count.countVal) ||
      (pStream->pCreate->eventTypes & openCloseEvents) == openCloseEvents) {
    pMsg->addOptions |= CALC_SLIDING_OVERLAP;
  }
  pMsg->outCols = pInfo->pCreate->outCols;
  pMsg->outStbFullCols = pInfo->pCreate->outStbFullCols;
  pMsg->outStbFullColCids = pInfo->pCreate->outStbFullColCids;
  pMsg->outTags = pInfo->pCreate->outTags;
  pMsg->outStbUid = pStream->pCreate->outStbUid;
  pMsg->outStbSversion = pStream->pCreate->outStbSversion;
  
  pMsg->subTblNameExpr = pInfo->pCreate->subTblNameExpr;
  pMsg->tagValueExpr = pInfo->pCreate->tagValueExpr;
  pMsg->forceOutCols = pInfo->pCreate->forceOutCols;

  pMsg->colCids = pInfo->pCreate->colCids;
  pMsg->tagCids = pInfo->pCreate->tagCids;
_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}

static int32_t msmSTAddToVgroupMap(SStmGrpCtx* pCtx, int64_t streamId, SArray* pTasks, SList* pList, SStmTaskStatus* pTask, bool trigReader) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t taskNum = pTask ? 1 : (pList ? MST_LIST_SIZE(pList) :taosArrayGetSize(pTasks));
  SListNode* pNode = pList ? listHead(pList) : NULL;
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskStatus* pStatus = pTask ? pTask : (pNode ? (SStmTaskStatus*)pNode->data : taosArrayGet(pTasks, i));
    TAOS_CHECK_EXIT(msmSTAddToVgroupMapImpl(streamId, pStatus, trigReader));
    if (pNode) {
      pNode = TD_DLIST_NODE_NEXT(pNode);
    }
  }
  
_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}


static int32_t msmSTAddToSnodeMap(SStmGrpCtx* pCtx, int64_t streamId, SArray* pTasks, SStmTaskStatus* pTask, int32_t taskNum, int32_t deployId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t rtaskNum = (taskNum > 0) ? taskNum : taosArrayGetSize(pTasks);
  int32_t taskType = (deployId < 0) ? STREAM_TRIGGER_TASK : STREAM_RUNNER_TASK;
  
  for (int32_t i = 0; i < rtaskNum; ++i) {
    SStmTaskStatus* pStatus = (taskNum > 0) ? (pTask + i) : taosArrayGet(pTasks, i);
    TAOS_CHECK_EXIT(msmSTAddToSnodeMapImpl(streamId, pStatus, deployId));
  }
  
_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int64_t msmAssignTaskId(void) {
  return atomic_fetch_add_64(&mStreamMgmt.lastTaskId, 1);
}

int64_t msmAssignTaskSeriousId(void) {
  return taosGetTimestampNs();
}


int32_t msmIsSnodeAlive(SMnode* pMnode, int32_t snodeId, int64_t streamId, bool* alive) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  bool     noExists = false;
  SStmSnodeStatus* pStatus = NULL;

  while (true) {
    pStatus = taosHashGet(mStreamMgmt.snodeMap, &snodeId, sizeof(snodeId));
    if (NULL == pStatus) {
      if (noExists) {
        mstsError("snode %d does not exist in snodeMap", snodeId);
        TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      }

      noExists = true;
      TAOS_CHECK_EXIT(msmSTAddSnodesToMap(pMnode));

      continue;
    }

    *alive = (pStatus->runnerThreadNum >= 0);
    break;
  }

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmRetrieveStaticSnodeId(SMnode* pMnode, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  bool alive = false;
  int32_t mainSnodeId = atomic_load_32(&pStream->mainSnodeId);
  int32_t snodeId = mainSnodeId;
  int64_t streamId = pStream->pCreate->streamId;

  while (true) {
    TAOS_CHECK_EXIT(msmIsSnodeAlive(pMnode, snodeId, streamId, &alive));

    if (alive) {
      return snodeId;
    }
    
    if (snodeId == mainSnodeId) {
      SSnodeObj* pSnode = mndAcquireSnode(pMnode, snodeId);
      if (NULL == pSnode) {
        stsWarn("snode %d not longer exists, ignore assign snode", snodeId);
        return 0;
      }
      
      if (pSnode->replicaId <= 0) {
        mstsError("no available snode now, mainSnodeId:%d, replicaId:%d", mainSnodeId, pSnode->replicaId);
        mndReleaseSnode(pMnode, pSnode);
        return 0;
      }

      snodeId = pSnode->replicaId;
      mndReleaseSnode(pMnode, pSnode);
      
      continue;
    }

    mstsError("no available snode now, mainSnodeId:%d, followerSnodeId:%d", mainSnodeId, snodeId);
    return 0;
  }

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return 0;
}

int32_t msmAssignRandomSnodeId(SMnode* pMnode, int64_t streamId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t snodeIdx = 0;
  int32_t snodeId = 0;
  void      *pIter = NULL;
  SSnodeObj *pObj = NULL;
  bool alive = false;
  int32_t snodeNum = sdbGetSize(pMnode->pSdb, SDB_SNODE);
  if (snodeNum <= 0) {
    mstsInfo("no available snode now, num:%d", snodeNum);
    goto _exit;
  }

  int32_t snodeTarget = taosRand() % snodeNum;

  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_SNODE, pIter, (void **)&pObj);
    if (pIter == NULL) {
      if (0 == snodeId) {
        mstsError("no alive snode now, snodeNum:%d", snodeNum);
        break;
      }
      
      snodeId = 0;
      continue;
    }

    code = msmIsSnodeAlive(pMnode, pObj->id, streamId, &alive);
    if (code) {
      sdbRelease(pMnode->pSdb, pObj);
      sdbCancelFetch(pMnode->pSdb, pIter);
      pObj = NULL;
      TAOS_CHECK_EXIT(code);
    }
    
    if (!alive) {
      sdbRelease(pMnode->pSdb, pObj);
      continue;
    }

    snodeId = pObj->id;
    if (snodeIdx == snodeTarget) {
      sdbRelease(pMnode->pSdb, pObj);
      sdbCancelFetch(pMnode->pSdb, pIter);
      pObj = NULL;
      goto _exit;
    }

    sdbRelease(pMnode->pSdb, pObj);
    snodeIdx++;
  }

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  if (0 == snodeId) {
    terrno = TSDB_CODE_SNODE_NO_AVAILABLE_NODE;
  }

  return snodeId;
}

// Only SDB references and private counters are needed for the catalog scan.
static SHashObj* msmCountExtStreamsBySnode(SMnode* mnode) {
  SHashObj* counts = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (!counts) return NULL;
  void*       iter = NULL;
  SStreamObj* row = NULL;
  while ((iter = sdbFetch(mnode->pSdb, SDB_STREAM, iter, (void**)&row))) {
    taosRLockLatch(&row->lock);
    bool    external = STREAM_IS_REF_EXT_SOURCE(row->flags);
    int32_t id = row->mainSnodeId;
    taosRUnLockLatch(&row->lock);
    sdbRelease(mnode->pSdb, row);
    if (external) {
      int32_t* count = taosHashGet(counts, &id, sizeof(id));
      if (count) {
        ++*count;
      } else {
        int32_t one = 1;
        if (taosHashPut(counts, &id, sizeof(id), &one, sizeof(one))) {
          sdbCancelFetch(mnode->pSdb, iter);
          taosHashCleanup(counts);
          return NULL;
        }
      }
    }
  }
  return counts;
}

static int32_t msmAssignSnodeIdLeastExtReader(SMnode* pMnode, SHashObj* pCnt) {
  int32_t    bestId  = 0;
  int32_t    bestCnt = INT32_MAX;
  SSnodeObj* pObj    = NULL;
  bool       alive   = false;
  void*      pIter = NULL;
  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_SNODE, pIter, (void**)&pObj);
    if (pIter == NULL) break;
    int32_t code = msmIsSnodeAlive(pMnode, pObj->id, 0, &alive);
    if (code || !alive) {
      sdbRelease(pMnode->pSdb, pObj);
      continue;
    }
    int32_t  id = pObj->id;
    int32_t* pv = (int32_t*)taosHashGet(pCnt, &id, sizeof(id));
    int32_t  v  = pv ? *pv : 0;
    if (v < bestCnt) {
      bestCnt = v;
      bestId  = id;
    }
    sdbRelease(pMnode->pSdb, pObj);
  }

  mDebug("LeastExtReader: pick snode=%d (ext stream count=%d)", bestId,
         bestCnt == INT32_MAX ? 0 : bestCnt);
  return bestId;
}

int32_t msmAssignTaskSnodeId(SMnode* pMnode, SStreamObj* pStream, bool isStatic) {
  int64_t streamId = pStream->pCreate->streamId;
  int32_t snodeNum = sdbGetSize(pMnode->pSdb, SDB_SNODE);
  int32_t snodeId = 0;
  if (snodeNum <= 0) {
    mstsInfo("no available snode now, num:%d", snodeNum);
    goto _exit;
  }

  snodeId = isStatic ? msmRetrieveStaticSnodeId(pMnode, pStream) : msmAssignRandomSnodeId(pMnode, streamId);

_exit:

  if (0 == snodeId) {
    terrno = TSDB_CODE_SNODE_NO_AVAILABLE_NODE;
  }

  return snodeId;
}

static int32_t msmSelectBuildSnode(SStmGrpCtx* ctx, SStreamObj* stream, bool isStatic) {
  SStmDeployDraft* draft = ctx->draft;
  if (draft->preferredExtSnode > 0) return draft->preferredExtSnode;

  bool      sample = STREAM_IS_REF_EXT_SOURCE(stream->flags) && draft->preferredExtSnode == 0;
  SHashObj* loads = sample ? msmCountExtStreamsBySnode(ctx->pMnode) : NULL;
  int32_t   node = 0;
  taosRLockLatch(&mStreamMgmt.runtimeLock);
  if (atomic_load_8(&mStreamMgmt.active) && mStreamMgmt.stat.activeTimes == draft->epoch) {
    (void)mstWaitLock(&mStreamMgmt.actionQLock, false);
    if (sample) {
      node = loads ? msmAssignSnodeIdLeastExtReader(ctx->pMnode, loads) : 0;
      draft->preferredExtSnode = node ? node : -1;
    }
    if (!node) node = msmAssignTaskSnodeId(ctx->pMnode, stream, isStatic);
    taosWUnLockLatch(&mStreamMgmt.actionQLock);
  } else {
    terrno = TSDB_CODE_MND_STREAM_NOT_AVAILABLE;
  }
  taosRUnLockLatch(&mStreamMgmt.runtimeLock);
  taosHashCleanup(loads);
  return node;
}

static int32_t msmBuildTriggerTasks(SStmGrpCtx* pCtx, SStmStatus* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  bool           deployOwned = false;
  SStmTaskDeploy info = {0};

  pInfo->triggerTask = taosMemoryCalloc(1, sizeof(SStmTaskStatus));
  TSDB_CHECK_NULL(pInfo->triggerTask, code, lino, _exit, terrno);

  pInfo->triggerTask->id.taskId = pCtx->triggerTaskId;
  pInfo->triggerTask->id.deployId = 0;
  pInfo->triggerTask->id.seriousId = msmAssignTaskSeriousId();
  pInfo->triggerTask->id.nodeId = pCtx->triggerNodeId;
  pInfo->triggerTask->id.taskIdx = 0;
  pInfo->triggerTask->type = STREAM_TRIGGER_TASK;
  pInfo->triggerTask->lastUpTs = pCtx->currTs;
  pInfo->triggerTask->pStream = pInfo;

  info.task.type = pInfo->triggerTask->type;
  info.task.streamId = streamId;
  info.task.taskId =  pInfo->triggerTask->id.taskId;
  info.task.seriousId = pInfo->triggerTask->id.seriousId;
  info.task.nodeId =  pInfo->triggerTask->id.nodeId;
  info.task.taskIdx =  pInfo->triggerTask->id.taskIdx;
  /* Only an external trigger reader requires the trigger task to use EXT pulls.
   * A local trigger with an external calc reader must keep the normal WAL path. */
  bool extTrigger = false;
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->trigReaders); ++i) {
    SStmTaskStatus *pReader = taosArrayGet(pInfo->trigReaders, i);
    if (STREAM_IS_REF_EXT_SOURCE(pReader->flags)) {
      extTrigger = true;
      break;
    }
  }
  if (extTrigger) {
    info.task.flags |= STREAM_FLAG_REF_EXT_SOURCE;
    mDebug("stream:%s trigger task uses EXT pull path", pStream->name);
  } else if (STREAM_IS_REF_EXT_SOURCE(pStream->flags)) {
    mDebug("stream:%s keeps WAL trigger path with external calc readers", pStream->name);
  }
  deployOwned = true;
  TAOS_CHECK_EXIT(msmBuildTriggerDeployInfo(pCtx->pMnode, pInfo, &info, pStream));
  for (int32_t i = 0; i < taosArrayGetSize(info.msg.trigger.readerList); ++i) {
    SStmTaskStatus*  reader = taosArrayGet(pInfo->trigReaders, i);
    SStreamTaskAddr* addr = taosArrayGet(info.msg.trigger.readerList, i);
    ESdbType         type = STREAM_IS_REF_EXT_SOURCE(reader->flags) ? SDB_DNODE : SDB_VGROUP;
    TAOS_CHECK_EXIT(msmRememberEndpoint(pCtx, type, addr->nodeId, &addr->epset));
  }
  for (int32_t i = 0; i < taosArrayGetSize(info.msg.trigger.runnerList); ++i) {
    SStreamRunnerTarget* runner = taosArrayGet(info.msg.trigger.runnerList, i);
    TAOS_CHECK_EXIT(msmRememberEndpoint(pCtx, SDB_DNODE, runner->addr.nodeId, &runner->addr.epset));
  }
  TAOS_CHECK_EXIT(msmStageTask(pCtx->draft, &info));
  deployOwned = false;

_exit:

  if (code) {
    if (deployOwned) {
      SStmTaskToDeployExt ext = {.deploy = info};
      mstDestroySStmTaskToDeployExt(&ext);
    }
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static void msmInitTrigReaderStatus(SStmGrpCtx* pCtx, SStmTaskStatus* pState, int32_t nodeId, SStmStatus* pInfo,
                                    int64_t streamId, bool external) {
  pState->id.taskId = msmAssignTaskId();
  pState->id.deployId = 0;
  pState->id.seriousId = msmAssignTaskSeriousId();
  pState->id.nodeId = nodeId;
  pState->id.taskIdx = 0;
  pState->type = STREAM_READER_TASK;
  pState->flags = STREAM_FLAG_TRIGGER_READER;
  if (external) {
    pState->flags |= STREAM_FLAG_REF_EXT_SOURCE;
    mstsDebug("external trigger reader task:%" PRIx64 " assigned to snode:%d", pState->id.taskId, nodeId);
  }
  pState->status = STREAM_STATUS_UNDEPLOYED;
  pState->lastUpTs = pCtx->currTs;
  pState->pStream = pInfo;
}

static int32_t msmTDAddSingleTrigReader(SStmGrpCtx* pCtx, SStmTaskStatus* pState, int32_t nodeId, SStmStatus* pInfo,
                                        int64_t streamId, SStreamExtTriggerSpec* pExtSpec, bool ownsExtSpec,
                                        SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  msmInitTrigReaderStatus(pCtx, pState, nodeId, pInfo, streamId, pExtSpec != NULL);

  SStmTaskDeploy info = {0};
  info.task.type = pState->type;
  info.task.streamId = streamId;
  info.task.taskId = pState->id.taskId;
  info.task.flags = pState->flags;
  info.task.seriousId = pState->id.seriousId;
  info.task.nodeId = pState->id.nodeId;
  info.task.taskIdx = pState->id.taskIdx;
  TAOS_CHECK_EXIT(msmBuildReaderDeployInfo(&info, NULL, pInfo, true));
  info.msg.reader.pExtSpec = pExtSpec;  // P1 B5: carry ext spec to vnode for federated reads
  info.msg.reader.ownsExtSpec = ownsExtSpec;

  TAOS_CHECK_EXIT(msmQueueReader(pCtx, &info, pStream));

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

/* P1 B5: resolve the external-source spec matching the trigger table for a
 * trigger-reader deploy (EXT-source streams only).  The trigger table of a
 * federated stream is an EXT table; match by extTable name.  Returns NULL for
 * non-federated streams or when no spec matches.  The returned pointer borrows
 * into pStream->pCreate->extSpecs, which stays valid as long as the stream is resident
 * in SDB (mndReleaseStream only drops the acquire refcount, it does not free
 * the object), so callers may release the stream right after the deploy call. */
static SStreamExtTriggerSpec* msmGetTrigExtSpec(SStreamObj* pStream) {
  if (!STREAM_IS_REF_EXT_SOURCE(pStream->flags) || pStream->pCreate->extSpecs == NULL) return NULL;
  int32_t nSpecs = (int32_t)taosArrayGetSize(pStream->pCreate->extSpecs);
  for (int32_t si = 0; si < nSpecs; ++si) {
    SStreamExtTriggerSpec* pSpec = *(SStreamExtTriggerSpec**)taosArrayGet(pStream->pCreate->extSpecs, si);
    if (pSpec == NULL) continue;
    /* Both sides carry the exact remote spelling: the parser resolved the
     * reference with TDengine's identifier grammar and the connector echoed
     * the catalog spelling, so the comparison is byte for byte.  The
     * trigger-reader deploy is not user visible, so a mismatch would silently
     * leave the stream without a trigger reader and its task stuck below
     * Running. */
    bool matched = false;
    if (EXT_SOURCE_ORACLE == (EExtSourceType)pSpec->sourceType) {
      matched = extColumnNameEqualReference(EXT_SOURCE_ORACLE, pSpec->extTable,
                                            pStream->pCreate->triggerTblName);
    } else {
      matched = (0 == strcmp(pSpec->extTable, pStream->pCreate->triggerTblName));
    }
    if (matched) {
      return pSpec;
    }
  }
  return NULL;
}

static int32_t msmTDAddTrigReaderTasks(SStmGrpCtx* pCtx, SStmStatus* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SSdb   *pSdb = pCtx->pMnode->pSdb;
  SStmTaskStatus* pState = NULL;
  SVgObj *pVgroup = NULL;
  SDbObj* pDb = NULL;

  /* P1 B5: find the ext spec matching the trigger table (EXT-source streams only).
   * Non-federated streams leave pTrigExtSpec = NULL. */
  SStreamExtTriggerSpec* pTrigExtSpec = msmGetTrigExtSpec(pStream);
  if (STREAM_IS_REF_EXT_SOURCE(pStream->flags) && pStream->pCreate->extSpecs != NULL) {
    mstsDebug("msmTDAddTrigReaderTasks: stream %" PRId64 " extSpec %s for trigger table %s",
              streamId, pTrigExtSpec ? pTrigExtSpec->sourceName : "(not found)",
              pStream->pCreate->triggerTblName);
  }

  switch (pStream->pCreate->triggerTblType) {
    case TSDB_NORMAL_TABLE:
    case TSDB_CHILD_TABLE:
    case TSDB_VIRTUAL_CHILD_TABLE:
    case TSDB_VIRTUAL_NORMAL_TABLE: {
      pInfo->trigReaders = taosArrayInit_s(sizeof(SStmTaskStatus), 1);
      TSDB_CHECK_NULL(pInfo->trigReaders, code, lino, _exit, terrno);
      pState = taosArrayGet(pInfo->trigReaders, 0);

      // External-source trigger readers are deployed on snode, not on a vgroup.
      // triggerTblVgId is set to 0 by the parser for EXT tables (no vgroup).
      // Use mainSnodeId as nodeId so toDeployVgMap / vgroupMap are keyed on the
      // real snode ID that appears in the heartbeat's snodeId field.
      int32_t trigNodeId = pStream->pCreate->triggerTblVgId;
#ifdef TD_ENTERPRISE
      if (pTrigExtSpec != NULL) {
        trigNodeId = atomic_load_32(&pStream->mainSnodeId);
        mstsDebug("msmTDAddTrigReaderTasks: EXT trigger reader nodeId overridden "
                  "from triggerTblVgId(%d) to snodeId(%d)",
                  pStream->pCreate->triggerTblVgId, trigNodeId);
      }
#endif
      TAOS_CHECK_EXIT(msmTDAddSingleTrigReader(pCtx, pState, trigNodeId, pInfo, streamId, pTrigExtSpec, false, pStream));
      break;
    }
    case TSDB_SUPER_TABLE: {
#ifdef TD_ENTERPRISE
      if (pTrigExtSpec != NULL) {
        pInfo->trigReaders = taosArrayInit_s(sizeof(SStmTaskStatus), 1);
        TSDB_CHECK_NULL(pInfo->trigReaders, code, lino, _exit, terrno);
        pState = taosArrayGet(pInfo->trigReaders, 0);
        int32_t trigNodeId = atomic_load_32(&pStream->mainSnodeId);
        mstsDebug("msmTDAddTrigReaderTasks: EXT super-table trigger reader uses snodeId=%d for stream %" PRId64,
                  trigNodeId, streamId);
        TAOS_CHECK_EXIT(msmTDAddSingleTrigReader(pCtx, pState, trigNodeId, pInfo, streamId, pTrigExtSpec, false, pStream));
        break;
      }
#endif
      pDb = mndAcquireDb(pCtx->pMnode, pStream->pCreate->triggerDB);
      if (NULL == pDb) {
        code = terrno;
        mstsError("failed to acquire db %s, error:%s", pStream->pCreate->triggerDB, terrstr());
        goto _exit;
      }

      taosRLockLatch(&pDb->lock);
      pCtx->draft->triggerDbUid = pDb->uid;
      pCtx->draft->triggerVgVersion = pDb->vgVersion;
      int32_t vgCount = pDb->cfg.numOfVgroups;
      taosRUnLockLatch(&pDb->lock);
      pInfo->trigReaders = taosArrayInit(vgCount, sizeof(SStmTaskStatus));
      TSDB_CHECK_NULL(pInfo->trigReaders, code, lino, _exit, terrno);

      void *pIter = NULL;
      while (1) {
        SStmTaskDeploy info = {0};
        pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
        if (pIter == NULL) {
          break;
        }
      
        if (pVgroup->dbUid == pDb->uid && !pVgroup->isTsma) {
          pState = taosArrayReserve(pInfo->trigReaders, 1);

          code = msmTDAddSingleTrigReader(pCtx, pState, pVgroup->vgId, pInfo, streamId, pTrigExtSpec, false, pStream);
          if (code) {
            sdbRelease(pSdb, pVgroup);
            sdbCancelFetch(pSdb, pIter);
            pVgroup = NULL;
            TAOS_CHECK_EXIT(code);
          }
        }

        sdbRelease(pSdb, pVgroup);
      }
      break;
    }
    default:
      mstsDebug("%s ignore triggerTblType %d", __FUNCTION__, pStream->pCreate->triggerTblType);
      break;
  }

_exit:

  mndReleaseDb(pCtx->pMnode, pDb);

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static bool msmIsVirtualMetadataScan(SSubplan* plan) {
  if (!plan || nodeType(plan) != QUERY_NODE_PHYSICAL_SUBPLAN || !plan->pNode ||
      nodeType(plan->pNode) != QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN) return false;
  SScanPhysiNode* scan = &((SSystemTableScanPhysiNode*)plan->pNode)->scan;
  return scan->virtualStableScan && strcmp(scan->tableName.dbname, TSDB_INFORMATION_SCHEMA_DB) == 0 &&
         strcmp(scan->tableName.tname, TSDB_INS_TABLE_VC_COLS) == 0;
}

static bool msmScanHasVgroupReaders(const SStreamCalcScan* scan) {
  if (scan->readFromCache) return false;
  for (size_t i = 0; i < taosArrayGetSize(scan->vgList); ++i) {
    if (*(int32_t*)taosArrayGet(scan->vgList, i) > MNODE_HANDLE) return true;
  }
  return false;
}

static int64_t msmScanIdentity(int32_t groupId, int32_t subplanId) {
  return (int64_t)(((uint64_t)(uint32_t)groupId << 32) | (uint32_t)subplanId);
}

static int32_t msmBuildMetadataCoverage(SStmDeployDraft* draft, SStreamObj* stream) {
  // Build once on the exceptional path; healthy deployments never decode this index.
  if (draft->metadataCoverage) return 0;
  int32_t code = 0;
  SHashObj* covered = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (!covered) return terrno;
  SHashObj* scans = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (!scans) {
    taosHashCleanup(covered);
    return terrno;
  }
#ifdef BUILD_TEST_SEAM
  if (gTdSeam_msmDeployPhase) gTdSeam_msmDeployPhase("metadata-coverage-begin", stream->name);
#endif
  for (size_t index = 0; !code && index < taosArrayGetSize(stream->pCreate->calcScanPlanList); ++index) {
    SStreamCalcScan* scan = taosArrayGet(stream->pCreate->calcScanPlanList, index);
    if (!msmScanHasVgroupReaders(scan)) continue;
    SSubplan* plan = NULL;
    code = nodesStringToNode(scan->scanPlan, (SNode**)&plan);
    if (!code && msmIsVirtualMetadataScan(plan)) {
      int64_t id = msmScanIdentity(plan->id.groupId, plan->id.subplanId);
      code = taosHashPut(scans, &id, sizeof(id), &id, sizeof(id));
      for (size_t partition = 0; !code && partition < taosArrayGetSize(scan->vgList); ++partition) {
        int32_t* vgId = taosArrayGet(scan->vgList, partition);
        int32_t key[2] = {plan->id.groupId, *vgId};
        code = taosHashPut(covered, key, sizeof(key), vgId, sizeof(*vgId));
      }
    }
    nodesDestroyNode((SNode*)plan);
  }
#ifdef BUILD_TEST_SEAM
  if (gTdSeam_msmDeployPhase) gTdSeam_msmDeployPhase("metadata-coverage-end", stream->name);
#endif
  if (code) {
    taosHashCleanup(covered);
    taosHashCleanup(scans);
  } else {
    draft->metadataCoverage = covered;
    draft->metadataScanIds = scans;
  }
  return code;
}

static int32_t msmPrepareMetadataSnapshot(SStmDeployDraft* draft) {
  if (!draft->checkedMetadataGroups) return 0;
  draft->metadataCoverageCounts = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  draft->boundMetadataVgroups = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (!draft->metadataCoverageCounts || !draft->boundMetadataVgroups) return terrno;
  int32_t code = 0;
  void* iter = NULL;
  // Count each group's coverage before acquiring publication locks. A vgroup is
  // in the intersection exactly when its count equals the number of recovered groups.
  while ((iter = taosHashIterate(draft->metadataCoverage, iter))) {
    int32_t* key = taosHashGetKey(iter, NULL);
    if (!taosHashGet(draft->checkedMetadataGroups, key, sizeof(*key))) continue;
    int32_t* count = taosHashGet(draft->metadataCoverageCounts, &key[1], sizeof(key[1]));
    if (count) {
      ++*count;
    } else {
      int32_t one = 1;
      code = taosHashPut(draft->metadataCoverageCounts, &key[1], sizeof(key[1]), &one, sizeof(one));
      if (code) break;
    }
  }
  if (code) {
    taosHashCancelIterate(draft->metadataCoverage, iter);
    return code;
  }
  while ((iter = taosHashIterate(draft->scanMap, iter))) {
    const char* key = taosHashGetKey(iter, NULL);
    int64_t scanId = 0;
    memcpy(&scanId, key + sizeof(int64_t), sizeof(scanId));
    if (!taosHashGet(draft->metadataScanIds, &scanId, sizeof(scanId))) continue;
    SArray* sources = *(SArray**)iter;
    for (size_t i = 0; i < taosArrayGetSize(sources); ++i) {
      SStmTaskSrcAddr* addr = taosArrayGet(sources, i);
      // Another metadata group may lose its binding after construction even if
      // that group did not itself skip a partition. Validate every metadata binding.
      if (addr->isFromCache || addr->vgId <= MNODE_HANDLE) continue;
      if (taosHashGet(draft->boundMetadataVgroups, &addr->vgId, sizeof(addr->vgId))) continue;
      uint8_t seen = 0;
      code = taosHashPut(draft->boundMetadataVgroups, &addr->vgId, sizeof(addr->vgId), &seen, sizeof(seen));
      if (code) break;
    }
    if (code) break;
  }
  if (code) taosHashCancelIterate(draft->scanMap, iter);
  return code;
}

static bool msmCheckMetadataVgroup(SMnode* mnode, void* obj, void* arg, void* streamArg, void* codeArg) {
  SStmDeployDraft* draft = arg;
  SStreamObj* stream = streamArg;
  SVgObj* vg = obj;
  int32_t* code = codeArg;
  if (vg->isTsma) return true;
  uint8_t* seen = taosHashGet(draft->boundMetadataVgroups, &vg->vgId, sizeof(vg->vgId));
  if (seen) {
    *seen = draft->metadataSnapshotId;
    ++draft->metadataBindingsSeen;
  }
  int32_t* count = taosHashGet(draft->metadataCoverageCounts, &vg->vgId, sizeof(vg->vgId));
  if (count && *count == taosHashGetSize(draft->checkedMetadataGroups)) return true;
  // Only a failed intersection lookup needs a per-group diagnostic.
  void* iter = NULL;
  while ((iter = taosHashIterate(draft->checkedMetadataGroups, iter))) {
    int32_t groupId = ((SStmMetadataRecovery*)iter)->groupId;
    int32_t key[2] = {groupId, vg->vgId};
    if (!taosHashGet(draft->metadataCoverage, key, sizeof(key))) {
      mstError("stream:%" PRIx64 " cannot recover metadata group:%d, current vgroup:%d db:%s is absent from"
               " persisted scan coverage; replan required", stream->pCreate->streamId, groupId, vg->vgId, vg->dbName);
      *code = TSDB_CODE_MND_STREAM_NOT_AVAILABLE;
      taosHashCancelIterate(draft->checkedMetadataGroups, iter);
      return false;
    }
  }
  return true;
}

static int32_t msmValidateMetadataCoverage(SMnode* mnode, SStmDeployDraft* draft, SStreamObj* stream) {
  if (!draft->checkedMetadataGroups) return 0;
  int32_t code = 0;
  void* iter = NULL;
  // Full deployment permits one restore retry; Runner repair validates once.
  ++draft->metadataSnapshotId;
  draft->metadataBindingsSeen = 0;
  // READY visibility precedes tableVer updates. Validate under the catalog's read
  // lock, with no plan decoding or row acquisition inside the traversal callback.
  terrno = 0;
  sdbTraverse(mnode->pSdb, SDB_VGROUP, msmCheckMetadataVgroup, draft, stream, &code);
  if (!code) code = terrno;
  if (code) return code;
  if (draft->metadataBindingsSeen == taosHashGetSize(draft->boundMetadataVgroups)) return 0;
  // Successful snapshots do not walk the bindings. Only failure needs an ID.
  while ((iter = taosHashIterate(draft->boundMetadataVgroups, iter))) {
    if (*(uint8_t*)iter == draft->metadataSnapshotId) continue;
    int32_t vgId = 0;
    memcpy(&vgId, taosHashGetKey(iter, NULL), sizeof(vgId));
    mstError("stream:%" PRIx64 " cannot publish metadata recovery, bound vgroup:%d disappeared from"
             " catalog snapshot; rebuild required", stream->pCreate->streamId, vgId);
    taosHashCancelIterate(draft->boundMetadataVgroups, iter);
    return TSDB_CODE_MND_STREAM_NOT_AVAILABLE;
  }
  return 0;
}

static int32_t msmPrepareMetadataCoverage(SStmDeployDraft* draft, SStreamObj* stream, int32_t groupId) {
  if (!draft) return TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
  SStmMetadataRecovery* recovery = taosHashGet(draft->checkedMetadataGroups, &groupId, sizeof(groupId));
  if (recovery) {
    ++recovery->skippedPartitions;
    return 0;
  }
  int32_t code = msmBuildMetadataCoverage(draft, stream);
  if (!code && !draft->checkedMetadataGroups) {
    draft->checkedMetadataGroups = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
    if (!draft->checkedMetadataGroups) code = terrno;
  }
  SStmMetadataRecovery first = {.groupId = groupId, .skippedPartitions = 1};
  if (!code) code = taosHashPut(draft->checkedMetadataGroups, &groupId, sizeof(groupId), &first, sizeof(first));
  return code;
}

static void msmLogMetadataRecovery(SStmDeployDraft* draft, int64_t streamId) {
  SStmMetadataRecovery* recovery = NULL;
  while ((recovery = taosHashIterate(draft->checkedMetadataGroups, recovery))) {
    mstInfo("stream:%" PRIx64 " metadata group:%d prepared to skip %d removed partitions; publication still requires validation",
            streamId, recovery->groupId, recovery->skippedPartitions);
  }
}

static int32_t msmSkipRemovedMetadataScan(SStmGrpCtx* pCtx, SStreamObj* pStream, SSubplan* plan,
                                         int32_t vgId, int32_t lookupCode, bool rebuildOnRemoval, bool* skipped) {
  *skipped = false;
  if (!msmIsVirtualMetadataScan(plan)) return 0;
  SScanPhysiNode* scan = &((SSystemTableScanPhysiNode*)plan->pNode)->scan;

  if (!lookupCode) return 0;  // An existing but unavailable vgroup must not be discarded.
  if (lookupCode != TSDB_CODE_MND_VGROUP_NOT_EXIST) {
    mstError("stream:%" PRIx64 " cannot check metadata vgroup:%d, subplan:%d, group:%d, error:0x%x(%s)",
             pStream->pCreate->streamId, vgId, plan->id.subplanId, plan->id.groupId, lookupCode, tstrerror(lookupCode));
    return lookupCode;
  }

  if (rebuildOnRemoval) {
    // The caller will discard this repair draft and validate a full deployment.
    *skipped = true;
    return 0;
  }
  TAOS_CHECK_RETURN(msmPrepareMetadataCoverage(pCtx->draft, pStream, plan->id.groupId));
  SHashObj* map = pCtx->draft->scanMap;
  int64_t key[2] = {pStream->pCreate->streamId, msmScanIdentity(plan->id.groupId, plan->id.subplanId)};
  if (taosHashGet(map, key, sizeof(key)) == NULL) {
    SArray* empty = NULL;
    TAOS_CHECK_RETURN(taosHashPut(map, key, sizeof(key), &empty, POINTER_BYTES));
  }
  *skipped = true;
  mstDebug("stream:%" PRIx64 " skip removed metadata vgroup:%d, subplan:%d, group:%d, table:%s.%s",
          key[0], vgId, plan->id.subplanId, plan->id.groupId, scan->tableName.dbname, scan->tableName.tname);
  return 0;
}

static int32_t msmBindScanTask(SStmGrpCtx* pCtx, SStreamObj* pStream, SSubplan* pSubplan, int32_t vgId,
                               int64_t* readerId, bool allocateReader, bool rebuildOnRemoval, bool* skipped) {
  *skipped = false;
  SHashObj*       scanMap = pCtx && pCtx->draft ? pCtx->draft->scanMap : mStreamMgmt.toUpdateScanMap;
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t taskId = *readerId;
  bool missingVgroupEndpoint = false;
  int64_t streamId = pStream->pCreate->streamId;
  int64_t key[2] = {streamId, 0};
  SStmTaskSrcAddr addr = {0};  /* zero-init: leave no field (e.g. isExt) as stack garbage */

  // Security hardening (see "item 194"): scanPlan is a client-supplied JSON
  // string; nodesStringToNode returns SUCCESS and leaves pSubplan NULL when the
  // key is missing, and it builds whatever NodeType the JSON names. Both cases
  // would be dereferenced below (pSubplan->id.groupId / subplanId) and crash the
  // mnode. Reject non-physical-subplan results up front.
  if (NULL == pSubplan || QUERY_NODE_PHYSICAL_SUBPLAN != nodeType((SNode*)pSubplan)) {
    mstsError("stream:%" PRId64 " scanPlan decoded to invalid node (null:%d, type:%d)", streamId, (pSubplan == NULL),
              (pSubplan ? nodeType((SNode*)pSubplan) : -1));
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  }

  addr.isFromCache = false;
  /* P4-E2: mark EXT-source readers so msmUpdateLowestPlanSourceAddr can
   * select TDMT_STREAM_FETCH_EXT instead of TDMT_STREAM_FETCH. */
  addr.isExt = STREAM_IS_REF_EXT_SOURCE(pStream->flags);
  
  if (MNODE_HANDLE == vgId) {
    mndGetMnodeEpSet(pCtx->pMnode, &addr.epset);
    TAOS_CHECK_EXIT(msmRememberEndpoint(pCtx, SDB_MNODE, vgId, &addr.epset));
  } else if (vgId > MNODE_HANDLE) {
    int32_t lookupCode = 0;
    addr.epset = mndGetVgroupEpsetByIdWithStatus(pCtx->pMnode, vgId, &lookupCode);
    if (addr.epset.numOfEps == 0) {
      TAOS_CHECK_EXIT(msmSkipRemovedMetadataScan(pCtx, pStream, pSubplan, vgId, lookupCode, rebuildOnRemoval, skipped));
      if (*skipped) goto _exit;
      missingVgroupEndpoint = true;
    }
    TAOS_CHECK_EXIT(msmRememberEndpoint(pCtx, SDB_VGROUP, vgId, &addr.epset));
  } else if (SNODE_HANDLE == vgId) {
    // External-source calc reader: deployed on snode, not on a vgroup.
    // The planner sets vgId=SNODE_HANDLE as a sentinel; replace it with the
    // stream's assigned mainSnodeId so the runner can reach the reader.
    int32_t snodeId = atomic_load_32(&pStream->mainSnodeId);
    addr.epset = mndGetDnodeEpsetById(pCtx->pMnode, snodeId);
    TAOS_CHECK_EXIT(msmRememberEndpoint(pCtx, SDB_DNODE, snodeId, &addr.epset));
    vgId = snodeId;  // reuse vgId so the generic addr.vgId assignment below is correct
    mDebug("stream:%" PRId64 " ext-source calc reader vgId sentinel replaced with snodeId:%d",
           pStream->pCreate->streamId, snodeId);
  } else {
    mstsError("invalid vgId %d in scanPlan", vgId);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  }
  
  if (allocateReader) *readerId = taskId = msmAssignTaskId();
  if (taskId == 0) {
    mstError("stream:%" PRIx64 " cannot bind calcReader, vgroup:%d, subplan:%d, group:%d; reader missing",
             streamId, vgId, pSubplan->id.subplanId, pSubplan->id.groupId);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  }
  if (missingVgroupEndpoint) {
    mstError("stream:%" PRIx64 " calcReader:%" PRIx64
               " has no vgroup endpoint, vgroup:%d, subplan:%d, group:%d; dependency retained",
               streamId, taskId, vgId, pSubplan->id.subplanId, pSubplan->id.groupId);
  }
  addr.taskId = taskId;
  addr.vgId = vgId;
  addr.groupId = pSubplan->id.groupId;

  key[1] = msmScanIdentity(pSubplan->id.groupId, pSubplan->id.subplanId);

  SArray** ppRes = taosHashGet(scanMap, key, sizeof(key));
  if (NULL == ppRes || NULL == *ppRes) {
    SArray* pRes = taosArrayInit(1, sizeof(addr));
    TSDB_CHECK_NULL(pRes, code, lino, _exit, terrno);
    if (!taosArrayPush(pRes, &addr)) {
      code = terrno;
    } else if (ppRes) {
      *ppRes = pRes;
    } else {
      code = taosHashPut(scanMap, key, sizeof(key), &pRes, POINTER_BYTES);
    }
    if (code) {
      taosArrayDestroy(pRes);
      TAOS_CHECK_EXIT(code);
    }
  } else {
    TSDB_CHECK_NULL(taosArrayPush(*ppRes, &addr), code, lino, _exit, terrno);
  }

  mstsDebug("calcReader %" PRIx64 " added to toUpdateScan, vgId:%d, groupId:%d, subplanId:%d", taskId, vgId, pSubplan->id.groupId, pSubplan->id.subplanId);

  if (!pCtx || !pCtx->draft) (void)atomic_add_fetch_32(&mStreamMgmt.toUpdateScanNum, 1);

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmUPAddScanTask(SStmGrpCtx* ctx, SStreamObj* stream, char* scanPlan, int32_t vgId,
                                int64_t taskId, bool rebuildOnRemoval, bool* skipped) {
  SSubplan* plan = NULL;
  int32_t code = nodesStringToNode(scanPlan, (SNode**)&plan);
  if (!code) code = msmBindScanTask(ctx, stream, plan, vgId, &taskId, false, rebuildOnRemoval, skipped);
  nodesDestroyNode((SNode*)plan);
  return code;
}

int32_t msmUPAddCacheTask(SStmGrpCtx* pCtx, SStreamCalcScan* pScan, SStreamObj* pStream) {
  SHashObj* scanMap = pCtx && pCtx->draft ? pCtx->draft->scanMap : mStreamMgmt.toUpdateScanMap;
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SSubplan* pSubplan = NULL;
  int64_t streamId = pStream->pCreate->streamId;
  int64_t key[2] = {streamId, 0};
  TAOS_CHECK_EXIT(nodesStringToNode(pScan->scanPlan, (SNode**)&pSubplan));

  // Security hardening (see "item 194"): identical NULL/type-confusion risk as
  // msmUPAddScanTask. pScan->scanPlan is an optional JSON key that may be NULL or
  // decode to a non-physical-subplan node; reject it before dereferencing below.
  if (NULL == pSubplan || QUERY_NODE_PHYSICAL_SUBPLAN != nodeType((SNode*)pSubplan)) {
    mstsError("stream:%" PRId64 " cache scanPlan decoded to invalid node (null:%d, type:%d)", streamId,
              (pSubplan == NULL), (pSubplan ? nodeType((SNode*)pSubplan) : -1));
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  }

  /* Zero-init so addr.isExt (and any future fields) are not left as stack
   * garbage. A cache reader is always a normal TDengine source, never an EXT
   * (federated) source; a garbage isExt here would make the runner send
   * TDMT_STREAM_FETCH_EXT and crash handleExtFetchReq via a type-confused
   * pTask->info. */
  SStmTaskSrcAddr addr = {0};
  addr.isFromCache = true;
  addr.isExt       = false;
  addr.epset = mndGetDnodeEpsetById(pCtx->pMnode, pCtx->triggerNodeId);
  TAOS_CHECK_EXIT(msmRememberEndpoint(pCtx, SDB_DNODE, pCtx->triggerNodeId, &addr.epset));
  addr.taskId = pCtx->triggerTaskId;
  addr.vgId = pCtx->triggerNodeId;
  addr.groupId = pSubplan->id.groupId;

  key[1] = msmScanIdentity(pSubplan->id.groupId, pSubplan->id.subplanId);
  SArray** ppRes = taosHashGet(scanMap, key, sizeof(key));
  if (NULL == ppRes || NULL == *ppRes) {
    SArray* pRes = taosArrayInit(1, sizeof(addr));
    TSDB_CHECK_NULL(pRes, code, lino, _exit, terrno);
    if (!taosArrayPush(pRes, &addr)) {
      code = terrno;
    } else if (ppRes) {
      *ppRes = pRes;
    } else {
      code = taosHashPut(scanMap, key, sizeof(key), &pRes, POINTER_BYTES);
    }
    if (code) {
      taosArrayDestroy(pRes);
      TAOS_CHECK_EXIT(code);
    }
  } else {
    TSDB_CHECK_NULL(taosArrayPush(*ppRes, &addr), code, lino, _exit, terrno);
  }

  if (!pCtx || !pCtx->draft) (void)atomic_add_fetch_32(&mStreamMgmt.toUpdateScanNum, 1);

_exit:

  nodesDestroyNode((SNode*)pSubplan);
  
  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


/* Resolve the connection spec for a calc scan by external source name, mirroring
 * msmGetTrigExtSpec (which matches by extTable). Returns a borrowed pointer into
 * pStream->pCreate->extSpecs (sdb-resident), or NULL when no source matches. */
static SStreamExtTriggerSpec* msmGetCalcExtSpecByName(SStreamObj* pStream, const char* sourceName) {
  if (!STREAM_IS_REF_EXT_SOURCE(pStream->flags) || pStream->pCreate->extSpecs == NULL) return NULL;
  if (sourceName == NULL || sourceName[0] == '\0') return NULL;
  int32_t n = (int32_t)taosArrayGetSize(pStream->pCreate->extSpecs);
  for (int32_t i = 0; i < n; ++i) {
    SStreamExtTriggerSpec* pSpec = *(SStreamExtTriggerSpec**)taosArrayGet(pStream->pCreate->extSpecs, i);
    if (pSpec != NULL && strcmp(pSpec->sourceName, sourceName) == 0) return pSpec;
  }
  return NULL;
}

/* Resolve the connection spec for one calc scan.  Each calc reader reads a single
 * external table; the table-level identity (extTable/tsColumn) travels per-scan in
 * the deploy msg, while this spec only supplies the source connection
 * (host/user/encryptedPassword/options/db).  Multi-source JOINs pick the spec by
 * pScan->sourceName; an old stream whose scan carries no sourceName falls back to
 * extSpecs[0] (single-source behavior).  Returns a borrowed pointer (sdb-resident)
 * or NULL for non-federated streams / when a named source is not found. */
static SStreamExtTriggerSpec* msmResolveCalcExtSpec(SStreamObj* pStream, const SStreamCalcScan* pScan) {
  if (!STREAM_IS_REF_EXT_SOURCE(pStream->flags) || pStream->pCreate->extSpecs == NULL) return NULL;
  if (taosArrayGetSize(pStream->pCreate->extSpecs) == 0) return NULL;
  if (pScan != NULL && pScan->sourceName[0] != '\0') {
    SStreamExtTriggerSpec* pSpec = msmGetCalcExtSpecByName(pStream, pScan->sourceName);
    if (pSpec == NULL) {
      mError("msmResolveCalcExtSpec: no ext spec matches calc scan source=%s (stream %" PRId64 ")",
             pScan->sourceName, pStream->pCreate->streamId);
    }
    return pSpec;
  }
  /* Old stream (no per-scan sourceName): single-source fallback. */
  return *(SStreamExtTriggerSpec**)taosArrayGet(pStream->pCreate->extSpecs, 0);
}

static int32_t msmInitCalcReader(SStmGrpCtx* pCtx, SStmTaskStatus* pState, int32_t taskIdx, int32_t nodeId,
                                 int64_t uid, SStmStatus* pInfo, int64_t streamId,
                                 SStreamExtTriggerSpec* pExtSpec, SStreamObj* pStream) {
  pState->id.uid = uid;

#ifdef TD_ENTERPRISE
  // EXT-source calc readers: the planner uses SNODE_HANDLE as a sentinel.
  // Replace it with the real snodeId so toDeploySnodeMap can be keyed correctly
  // and the runner's exchange node gets the snode's epset.
  if (SNODE_HANDLE == nodeId && pExtSpec != NULL && pStream != NULL) {
    nodeId = atomic_load_32(&pStream->mainSnodeId);
    mstsDebug("EXT calc reader: resolved SNODE_HANDLE to snodeId:%d for stream:%" PRId64, nodeId, streamId);
  }
#endif

  pState->id.deployId = 0;
  pState->id.seriousId = msmAssignTaskSeriousId();
  pState->id.nodeId = nodeId;
  pState->id.taskIdx = taskIdx;
  pState->type = STREAM_READER_TASK;
  pState->flags = 0;
  pState->status = STREAM_STATUS_UNDEPLOYED;
  pState->lastUpTs = pCtx->currTs;
  pState->pStream = pInfo;

  return TSDB_CODE_SUCCESS;
}

static int32_t msmQueueCalcReader(SStmGrpCtx* pCtx, SStmTaskStatus* pState, SStmStatus* pInfo, int64_t streamId,
                                  void* calcScanPlan, const SStreamCalcScan* pScan, SStreamExtTriggerSpec* pExtSpec,
                                  SStreamObj* pStream) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SStmTaskDeploy info = {0};
  info.task.type = pState->type;
  info.task.streamId = streamId;
  info.task.taskId = pState->id.taskId;
  info.task.flags = pState->flags;
  info.task.seriousId = pState->id.seriousId;
  info.task.nodeId = pState->id.nodeId;
  info.task.taskIdx = pState->id.taskIdx;
  TAOS_CHECK_EXIT(msmBuildReaderDeployInfo(&info, calcScanPlan, pInfo, false));
  info.msg.reader.pExtSpec = pExtSpec;
  /* Per-scan ext table identity: the reader overrides its (source-level) ext spec
   * with these so each calc reader scans its own table for multi-source JOINs. */
  if (pScan->extTable[0] != '\0') {
    tstrncpy(info.msg.reader.msg.calc.extTable, pScan->extTable, sizeof(info.msg.reader.msg.calc.extTable));
  }
  if (pScan->tsColumn[0] != '\0') {
    tstrncpy(info.msg.reader.msg.calc.tsColumn, pScan->tsColumn, sizeof(info.msg.reader.msg.calc.tsColumn));
  }

  TAOS_CHECK_EXIT(msmQueueReader(pCtx, &info, pStream));

_exit:

  if (code) {
    taosMemoryFree(info.msg.reader.msg.calc.calcScanPlan);
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmTDAddCalcReaderTasks(SStmGrpCtx* pCtx, SStmStatus* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t calcTasksNum = taosArrayGetSize(pStream->pCreate->calcScanPlanList);
  int64_t streamId = pStream->pCreate->streamId;
  SStmTaskStatus* pState = NULL;
  SSubplan* plan = NULL;
  pInfo->calcReaders = tdListNew(sizeof(SStmTaskStatus));
  TSDB_CHECK_NULL(pInfo->calcReaders, code, lino, _exit, terrno);

  for (int32_t i = 0; i < calcTasksNum; ++i) {
    SStreamCalcScan* pScan = taosArrayGet(pInfo->pCreate->calcScanPlanList, i);
    if (pScan->readFromCache) {
      TAOS_CHECK_EXIT(msmUPAddCacheTask(pCtx, pScan, pStream));
      continue;
    }

    /* Bind this calc scan to its own external source (multi-source JOIN): the
     * connection spec is matched by pScan->sourceName; extTable/tsColumn travel
     * per-scan in the deploy msg. */
    SStreamExtTriggerSpec* pCalcExtSpec = msmResolveCalcExtSpec(pStream, pScan);
    if (STREAM_IS_REF_EXT_SOURCE(pStream->flags) && pCalcExtSpec == NULL) {
      mstsError("stream:%" PRId64 " calc reader deploy: no ext spec for calc scan source=%s",
                streamId, pScan->sourceName);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }

    int32_t vgNum = taosArrayGetSize(pScan->vgList);
    if (vgNum == 0) continue;
    int64_t uid = 0;
    TAOS_CHECK_EXIT(nodesStringToNode(pScan->scanPlan, (SNode**)&plan));
    TAOS_CHECK_EXIT(mstGetScanUidFromSubplan(streamId, plan, &uid));
    for (int32_t m = 0; m < vgNum; ++m) {
      int32_t scanNodeId = *(int32_t*)taosArrayGet(pScan->vgList, m);
      int64_t taskId = 0;
      // Keep the original sentinel for external readers. Only publish a reader
      // after its source has been bound or positively classified as removed.
      bool skipped = false;
      TAOS_CHECK_EXIT(msmBindScanTask(pCtx, pStream, plan, scanNodeId, &taskId, true, false, &skipped));
      if (skipped) continue;
      pState = tdListReserve(pInfo->calcReaders);
      TSDB_CHECK_NULL(pState, code, lino, _exit, terrno);
      pState->id.taskId = taskId;
      TAOS_CHECK_EXIT(msmInitCalcReader(pCtx, pState, i, scanNodeId, uid, pInfo, streamId, pCalcExtSpec, pStream));
#ifdef TD_ENTERPRISE
      if (pCalcExtSpec != NULL) {
        TAOS_CHECK_EXIT(
            msmQueueCalcReader(pCtx, pState, pInfo, streamId, pScan->scanPlan, pScan, pCalcExtSpec, pStream));
      }
#endif
    }
    nodesDestroyNode((SNode*)plan);
    plan = NULL;
  }

  pInfo->calcReaderNum = MST_LIST_SIZE(pInfo->calcReaders);

_exit:

  nodesDestroyNode((SNode*)plan);

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmUPPrepareReaderTasks(SStmGrpCtx* pCtx, SArray* readerIds, SStreamObj* pStream,
                                      bool* rebuildStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SHashObj* readers = NULL;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t calcTasksNum = taosArrayGetSize(pStream->pCreate->calcScanPlanList);
  if (calcTasksNum <= 0) {
    mstsDebug("no calc scan plan, ignore parepare reader tasks, readerNum:%d", (int32_t)taosArrayGetSize(readerIds));
    return code;    
  }

  if (taosArrayGetSize(readerIds) > 0) {
    readers = taosHashInit(TMAX(taosArrayGetSize(readerIds), 4),
                           taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    TSDB_CHECK_NULL(readers, code, lino, _exit, terrno);
  }
  for (size_t n = 0; n < taosArrayGetSize(readerIds); ++n) {
    SStmReaderBinding* reader = taosArrayGet(readerIds, n);
    if (reader->taskIdx < 0 || reader->taskIdx >= calcTasksNum) {
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }
    SStreamCalcScan* scan = taosArrayGet(pStream->pCreate->calcScanPlanList, reader->taskIdx);
    int32_t nodeId = reader->nodeId;
    // External readers store the resolved snode ID; plans retain the sentinel.
    if (taosArrayGetSize(scan->vgList) == 1) {
      int32_t sentinel = *(int32_t*)taosArrayGet(scan->vgList, 0);
      if (sentinel <= MNODE_HANDLE) nodeId = sentinel;
    }
    int32_t key[2] = {reader->taskIdx, nodeId};
    TAOS_CHECK_EXIT(taosHashPut(readers, key, sizeof(key), &reader->taskId, sizeof(reader->taskId)));
  }

  for (int32_t i = 0; i < calcTasksNum; ++i) {
    SStreamCalcScan* pScan = taosArrayGet(pStream->pCreate->calcScanPlanList, i);
    if (pScan->readFromCache) {
      TAOS_CHECK_EXIT(msmUPAddCacheTask(pCtx, pScan, pStream));
      continue;
    }
    
    int32_t vgNum = taosArrayGetSize(pScan->vgList);
    for (int32_t m = 0; m < vgNum; ++m) {
      // Use the original planner sentinel (e.g. SNODE_HANDLE) from vgList so
      // that msmUPAddScanTask can correctly resolve the snode epset.  After
      // msmInitCalcReader resolves SNODE_HANDLE -> real snodeId in
      // pReader->id.nodeId, using nodeId directly would hit the wrong branch
      // (vgId > MNODE_HANDLE) and produce a vgroup epset instead of snode.
      int32_t scanSentinel = *(int32_t*)taosArrayGet(pScan->vgList, m);
      int32_t key[2] = {i, scanSentinel};
      void* readerId = readers ? taosHashGet(readers, key, sizeof(key)) : NULL;
      int64_t taskId = 0;
      if (readerId) memcpy(&taskId, readerId, sizeof(taskId));
      bool skipped = false;
      TAOS_CHECK_EXIT(msmUPAddScanTask(pCtx, pStream, pScan->scanPlan, scanSentinel,
                                     taskId, readerId != NULL, &skipped));
      if (skipped && readerId) {
        *rebuildStream = true;
        mstInfo("stream:%" PRIx64 " removed metadata vgroup:%d still owns calcReader:%" PRIx64
                "; rebuild all stream tasks", streamId, scanSentinel, taskId);
        goto _exit;
      }
    }
  }

_exit:

  taosHashCleanup(readers);

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmBuildReaderTasks(SStmGrpCtx* pCtx, SStmStatus* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  
  TAOS_CHECK_EXIT(msmTDAddTrigReaderTasks(pCtx, pInfo, pStream));
  TAOS_CHECK_EXIT(msmTDAddCalcReaderTasks(pCtx, pInfo, pStream));

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}

int32_t msmUpdatePlanSourceAddr(SStreamTask* pTask, int64_t streamId, SSubplan* plan, int64_t clientId, SStmTaskSrcAddr* pSrc, int32_t msgType, int64_t srcSubplanId) {
  SDownstreamSourceNode source = {
      .type = QUERY_NODE_DOWNSTREAM_SOURCE,
      .clientId = clientId,
      .taskId = pSrc->taskId,
      .sId = 0,
      .execId = 0,
      .fetchMsgType = msgType,
      .localExec = false,
  };

  source.addr.epSet = pSrc->epset;
  source.addr.nodeId = pSrc->vgId;

  // Security hardening (see "item 194"): a NULL plan, or a client subplan JSON that
  // omits RootNode (leaving plan->pNode NULL), would crash in qSetSubplanExecutionNode
  // -> nodeType(NULL). The msttDebug below dereferences plan (and pTask via the macro),
  // so this check MUST run BEFORE the log; otherwise a NULL plan still crashes before
  // the guard is reached and the defensive check becomes ineffective.
  if (NULL == plan || NULL == plan->pNode) {
    mstsError("stream:%" PRId64 " subplan %d has NULL plan/pNode, cannot set execution source", streamId,
              (plan ? plan->id.subplanId : -1));
    return TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
  }

  msttDebug("try to update subplan %d grp %d sourceAddr from subplan %" PRId64 ", clientId:%" PRIx64 ", srcTaskId:%" PRIx64 ", srcNodeId:%d, msgType:%s", 
      plan->id.subplanId, pSrc->groupId, srcSubplanId, source.clientId, source.taskId, source.addr.nodeId, TMSG_INFO(source.fetchMsgType));

  return qSetSubplanExecutionNode(plan, pSrc->groupId, &source);
}

int32_t msmGetTaskIdFromSubplanId(SStreamObj* pStream, SArray* pRunners, int32_t beginIdx, int32_t subplanId, int64_t* taskId, SStreamTask** ppParent) {
  int64_t streamId = pStream->pCreate->streamId;
  int32_t runnerNum = taosArrayGetSize(pRunners);
  for (int32_t i = beginIdx; i < runnerNum; ++i) {
    SStmTaskDeploy* pDeploy = taosArrayGet(pRunners, i);
    SSubplan* pPlan = pDeploy->msg.runner.pPlan;
    if (pPlan->id.subplanId == subplanId) {
      *taskId = pDeploy->task.taskId;
      *ppParent = &pDeploy->task;
      return TSDB_CODE_SUCCESS;
    }
  }

  mstsError("subplanId %d not found in runner list", subplanId);

  return TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
}

int32_t msmUpdateLowestPlanSourceAddr(SSubplan* pPlan, SStmTaskDeploy* pDeploy, int64_t streamId, SHashObj* scanMap) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SHashObj* sourceGroups = NULL;
  int64_t key[2] = {streamId, -1};
  SNode* pNode = NULL;
  SStreamTask* pTask = &pDeploy->task;
  FOREACH(pNode, pPlan->pChildren) {
    if (QUERY_NODE_VALUE != nodeType(pNode)) {
      msttDebug("node type %d is not valueNode, skip it", nodeType(pNode));
      continue;
    }
    
    SValueNode* pVal = (SValueNode*)pNode;
    if (TSDB_DATA_TYPE_BIGINT != pVal->node.resType.type) {
      msttWarn("invalid value node data type %d for runner's child subplan", pVal->node.resType.type);
      continue;
    }

    key[1] = pVal->datum.i;

    SArray** ppRes = taosHashGet(scanMap, key, sizeof(key));
    if (NULL == ppRes) {
      msttError("lowest runner subplan ID:%d,%d cannot get its child ID:%" PRId64 " addr", pPlan->id.groupId, pPlan->id.subplanId, key[1]);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }

    int32_t childrenNum = taosArrayGetSize(*ppRes);
    if (childrenNum == 0) {
      // A removed metadata partition is empty, but its Exchange must still
      // have a surviving partition in the same group. Do not publish an
      // empty or unrelated Exchange when the entire metadata source is gone.
      if (sourceGroups == NULL) {
        sourceGroups = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
        TSDB_CHECK_NULL(sourceGroups, code, lino, _exit, terrno);
        SNode* child = NULL;
        FOREACH(child, pPlan->pChildren) {
          if (nodeType(child) != QUERY_NODE_VALUE) continue;
          SValueNode* ref = (SValueNode*)child;
          if (ref->node.resType.type != TSDB_DATA_TYPE_BIGINT) continue;
          int64_t otherKey[2] = {streamId, ref->datum.i};
          SArray** sources = taosHashGet(scanMap, otherKey, sizeof(otherKey));
          if (sources && taosArrayGetSize(*sources) > 0) {
            int32_t groupId = (int32_t)((uint64_t)ref->datum.i >> 32);
            if (taosHashGet(sourceGroups, &groupId, sizeof(groupId)) == NULL) {
              TAOS_CHECK_EXIT(taosHashPut(sourceGroups, &groupId, sizeof(groupId), &groupId, sizeof(groupId)));
            }
          }
        }
      }
      int32_t groupId = (int32_t)((uint64_t)pVal->datum.i >> 32);
      if (taosHashGet(sourceGroups, &groupId, sizeof(groupId)) == NULL) {
        mstError("stream:%" PRIx64 " cannot bind runner:%" PRIx64
                 " subplan:%d, metadata group:%d has no surviving source after removing stale vgroups",
                 streamId, pDeploy->task.taskId, pPlan->id.subplanId, groupId);
        TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      }
    }
    for (int32_t i = 0; i < childrenNum; ++i) {
      SStmTaskSrcAddr* pAddr = taosArrayGet(*ppRes, i);
      TAOS_CHECK_EXIT(msmUpdatePlanSourceAddr(pTask, streamId, pPlan, pDeploy->task.taskId, pAddr,
          pAddr->isExt        ? TDMT_STREAM_FETCH_EXT :
          pAddr->isFromCache  ? TDMT_STREAM_FETCH_FROM_CACHE : TDMT_STREAM_FETCH,
          key[1]));
    }
  }

_exit:

  taosHashCleanup(sourceGroups);

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmUpdateRunnerPlan(SStmGrpCtx* pCtx, SArray* pRunners, int32_t beginIdx, SStmTaskDeploy* pDeploy, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SSubplan* pPlan = pDeploy->msg.runner.pPlan;
  SStreamTask* pTask = &pDeploy->task;
  SStreamTask* parentTask = NULL;
  int64_t streamId = pStream->pCreate->streamId;

  TAOS_CHECK_EXIT(msmUpdateLowestPlanSourceAddr(
      pPlan, pDeploy, streamId, pCtx && pCtx->draft ? pCtx->draft->scanMap : mStreamMgmt.toUpdateScanMap));

  SNode* pTmp = NULL;
  WHERE_EACH(pTmp, pPlan->pChildren) {
    if (QUERY_NODE_VALUE == nodeType(pTmp)) {
      ERASE_NODE(pPlan->pChildren);
      continue;
    }
    WHERE_NEXT;
  }
  nodesClearList(pPlan->pChildren);
  pPlan->pChildren = NULL;

  if (NULL == pPlan->pParents) {
    goto _exit;
  }

  SNode* pNode = NULL;
  int64_t parentTaskId = 0;
  SStmTaskSrcAddr addr = {0};
  addr.taskId = pDeploy->task.taskId;
  addr.vgId = pDeploy->task.nodeId;
  addr.groupId = pPlan->id.groupId;
  addr.epset = mndGetDnodeEpsetById(pCtx->pMnode, pDeploy->task.nodeId);
  TAOS_CHECK_EXIT(msmRememberEndpoint(pCtx, SDB_DNODE, pDeploy->task.nodeId, &addr.epset));
  FOREACH(pNode, pPlan->pParents) {
    SSubplan* pSubplan = (SSubplan*)pNode;
    // Security hardening (see "item 194"): elements of pPlan->pParents are taken as
    // SSubplan with no type check. Reject wrong-typed nodes before walking them.
    if (QUERY_NODE_PHYSICAL_SUBPLAN != nodeType(pNode)) {
      mstsError("stream:%" PRId64 " parent subplan node type %d is not QUERY_NODE_PHYSICAL_SUBPLAN", streamId,
                nodeType(pNode));
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }
    TAOS_CHECK_EXIT(msmGetTaskIdFromSubplanId(pStream, pRunners, beginIdx, pSubplan->id.subplanId, &parentTaskId, &parentTask));
    TAOS_CHECK_EXIT(msmUpdatePlanSourceAddr(parentTask, streamId, pSubplan, parentTaskId, &addr, TDMT_STREAM_FETCH_FROM_RUNNER, pPlan->id.subplanId));
  }
  
_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmUpdateRunnerPlans(SStmGrpCtx* pCtx, SArray* pRunners, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t runnerNum = taosArrayGetSize(pRunners);
  SNodeList* pReaders = NULL;
  int32_t    encoded = 0;

  // Cache sources are not Readers. Their unresolved Exchange references keep reuse disabled.
  for (int32_t i = 0; runnerNum > 0 && i < taosArrayGetSize(pStream->pCreate->calcScanPlanList); ++i) {
    SStreamCalcScan* pScan = taosArrayGet(pStream->pCreate->calcScanPlanList, i);
    if (pScan->readFromCache) {
      continue;
    }
    SNode* pReader = NULL;
    TSDB_CHECK_NULL(pScan->scanPlan, code, lino, _exit, TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    TAOS_CHECK_EXIT(nodesStringToNode(pScan->scanPlan, &pReader));
    TAOS_CHECK_EXIT(nodesListMakeStrictAppend(&pReaders, pReader));
  }

  // Preserve every Runner's complete child identities before endpoint updates erase them.
  for (int32_t i = 0; i < runnerNum; ++i) {
    SStmTaskDeploy* pDeploy = taosArrayGet(pRunners, i);
    TAOS_CHECK_EXIT(qSetStreamBroadcastReuse(pDeploy->msg.runner.pPlan, pReaders));
  }

  for (int32_t i = 0; i < runnerNum; ++i) {
    SStmTaskDeploy* pDeploy = taosArrayGet(pRunners, i);
    TAOS_CHECK_EXIT(msmUpdateRunnerPlan(pCtx, pRunners, i, pDeploy, pStream));
    char* text = NULL;
    TAOS_CHECK_EXIT(nodesNodeToString((SNode*)pDeploy->msg.runner.pPlan, false, &text, NULL));
    TSDB_CHECK_NULL(text, code, lino, _exit, TSDB_CODE_OUT_OF_MEMORY);
    pDeploy->msg.runner.pPlan = text;
    ++encoded;

    SStreamTask* pTask = &pDeploy->task;
    msttDebugL("runner updated task plan:%s", (const char*)pDeploy->msg.runner.pPlan);
  }

_exit:

  nodesDestroyList(pReaders);

  if (code) {
    for (int32_t i = 0; i < encoded; ++i) {
      SStmTaskDeploy* task = taosArrayGet(pRunners, i);
      taosMemoryFreeClear(task->msg.runner.pPlan);
    }
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmBuildRunnerTasksImpl(SStmGrpCtx* pCtx, int32_t dagIdx, SStmStatus* pInfo, SStreamObj* pStream, SQueryPlan** pRoot, SNodeList** subEP) {
  int32_t code = 0;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SArray* deployTaskList = NULL;
  SArray* deployList = NULL;
  int32_t deployNodeId = 0;
  SStmTaskStatus* pState = NULL;
  int32_t taskIdx = 0;
  SNodeListNode *plans = NULL;
  SQueryPlan    *pDag = NULL;
  int32_t        taskNum = 0;
  int32_t        totalTaskNum = 0;
  bool           subQ = false;

  if (dagIdx >= 0) {
    subQ = true;
    pDag = (SQueryPlan *)nodesListGetNode((*pRoot)->pChildren, dagIdx);
  } else {
    pDag = *pRoot;
  }

  if (pDag->numOfSubplans <= 0) {
    mstsError("invalid subplan num:%d", pDag->numOfSubplans);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  }

  if ((*pRoot)->numOfSubplans != pStream->pCreate->numOfCalcSubplan) {
    mstsError("numOfCalcSubplan %d mismatch with numOfSubplans %d", pStream->pCreate->numOfCalcSubplan, pDag->numOfSubplans);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  }

  int32_t levelNum = (int32_t)LIST_LENGTH(pDag->pSubplans);
  if (levelNum <= 0) {
    mstsError("invalid level num:%d", levelNum);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  }

  int32_t        lowestLevelIdx = levelNum - 1;
  
  plans = (SNodeListNode *)nodesListGetNode(pDag->pSubplans, 0);
  if (QUERY_NODE_NODE_LIST != nodeType(plans)) {
    mstsError("invalid level plan, level:0, planNodeType:%d", nodeType(plans));
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  }
  
  taskNum = (int32_t)LIST_LENGTH(plans->pNodeList);
  if (taskNum != 1) {
    mstsError("invalid level plan number:%d, level:0", taskNum);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  }

  deployTaskList = taosArrayInit_s(sizeof(SStmTaskDeploy), pDag->numOfSubplans);
  TSDB_CHECK_NULL(deployTaskList, code, lino, _exit, terrno);
  
  for (int32_t deployId = 0; deployId < pInfo->runnerDeploys; ++deployId) {
    totalTaskNum = 0;

    deployList = pInfo->runners[deployId];
    deployNodeId = msmSelectBuildSnode(pCtx, pStream, (0 == deployId) ? true : false);
    if (!GOT_SNODE(deployNodeId)) {
      TAOS_CHECK_EXIT(terrno);
    }

    taskIdx = 0;

    SListCell* pSubplanCell = nodesListGetCell(pDag->pSubplans, lowestLevelIdx);
    for (int32_t i = lowestLevelIdx; i >= 0;
         --i, pSubplanCell = (NULL != pSubplanCell ? pSubplanCell->pPrev : NULL)) {
      plans = (SNodeListNode *)(NULL != pSubplanCell ? pSubplanCell->pNode : NULL);
      if (NULL == plans) {
        mstsError("empty level plan, level:%d", i);
        TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      }

      if (QUERY_NODE_NODE_LIST != nodeType(plans)) {
        mstsError("invalid level plan, level:%d, planNodeType:%d", i, nodeType(plans));
        TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      }

      taskNum = (int32_t)LIST_LENGTH(plans->pNodeList);
      if (taskNum <= 0) {
        mstsError("invalid level plan number:%d, level:%d", taskNum, i);
        TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      }

      totalTaskNum += taskNum;
      if (totalTaskNum > pDag->numOfSubplans) {
        mstsError("current totalTaskNum %d is bigger than numOfSubplans %d, level:%d", totalTaskNum, pDag->numOfSubplans, i);
        TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      }

      SListCell* pNodeCell = NULL != plans->pNodeList ? plans->pNodeList->pHead : NULL;
      for (int32_t n = 0; n < taskNum;
           ++n, pNodeCell = (NULL != pNodeCell ? pNodeCell->pNext : NULL)) {
        SSubplan *plan = (SSubplan *)(NULL != pNodeCell ? pNodeCell->pNode : NULL);
        // Security hardening (see "item 194"): list elements are taken as SSubplan
        // with no type check. Reject wrong-typed nodes before walking them.
        if (QUERY_NODE_PHYSICAL_SUBPLAN != nodeType((SNode*)plan)) {
          mstsError("stream:%" PRId64 " level:%d subplan[%d] node type %d is not QUERY_NODE_PHYSICAL_SUBPLAN",
                    streamId, i, n, nodeType((SNode*)plan));
          TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
        }
        pState = taosArrayReserve(deployList, 1);

        pState->id.taskId = msmAssignTaskId();
        pState->id.deployId = deployId;
        pState->id.seriousId = msmAssignTaskSeriousId();
        pState->id.nodeId = deployNodeId;
        pState->id.taskIdx = MND_SET_RUNNER_TASKIDX(i, n);
        pState->type = STREAM_RUNNER_TASK;
        pState->flags = (0 == i) ? STREAM_FLAG_TOP_RUNNER : 0;
        pState->status = STREAM_STATUS_UNDEPLOYED;
        pState->lastUpTs = pCtx->currTs;
        pState->pStream = pInfo;

        SStmTaskDeploy* pDeploy = taosArrayGet(deployTaskList, taskIdx++);
        pDeploy->task.type = pState->type;
        pDeploy->task.streamId = streamId;
        pDeploy->task.taskId = pState->id.taskId;
        pDeploy->task.flags = pState->flags;
        pDeploy->task.seriousId = pState->id.seriousId;
        pDeploy->task.deployId = pState->id.deployId;
        pDeploy->task.nodeId = pState->id.nodeId;
        pDeploy->task.taskIdx = pState->id.taskIdx;
        TAOS_CHECK_EXIT(msmBuildRunnerDeployInfo(pDeploy, plan, pStream, pInfo, 0 == i && !subQ));

        SStreamTask* pTask = &pDeploy->task;
        msttDebug("runner task deploy built, subplan level:%d, taskIdx:%d, groupId:%d, subplanId:%d",
            i, pTask->taskIdx, plan->id.groupId, plan->id.subplanId);

        if (subQ) {
          if (i == 0) {
            SStmTaskSrcAddr addr = {0};
            SDownstreamSourceNode* pSource = NULL;

            addr.taskId = pDeploy->task.taskId;
            addr.vgId = pDeploy->task.nodeId;
            addr.groupId = plan->id.groupId;
            addr.epset = mndGetDnodeEpsetById(pCtx->pMnode, pDeploy->task.nodeId);
            TAOS_CHECK_EXIT(msmRememberEndpoint(pCtx, SDB_DNODE, pDeploy->task.nodeId, &addr.epset));

            TAOS_CHECK_EXIT(nodesMakeNode(QUERY_NODE_DOWNSTREAM_SOURCE, (SNode**)&pSource));
            pSource->addr.epSet = addr.epset;
            pSource->addr.nodeId = addr.vgId;

            pSource->clientId = streamId;
            pSource->taskId = pDeploy->task.taskId;
            pSource->sId = 0;
            pSource->execId = 0;
            pSource->fetchMsgType = TDMT_STREAM_FETCH_FROM_RUNNER;
            pSource->localExec = false;

            TAOS_CHECK_EXIT(nodesListMakeStrictAppend(subEP, (SNode *)pSource));
	  }
        } else {
          TAOS_CHECK_EXIT(nodesCloneList(*subEP, &plan->pSubQ));
        }
      }

      mstsDebug("deploy %d level %d initialized, taskNum:%d", deployId, i, taskNum);
    }

    if (totalTaskNum != pDag->numOfSubplans) {
      mstsError("totalTaskNum %d mismatch with numOfSubplans %d", totalTaskNum, pDag->numOfSubplans);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }

    TAOS_CHECK_EXIT(msmUpdateRunnerPlans(pCtx, deployTaskList, pStream));

    TAOS_CHECK_EXIT(msmStageRunnerTasks(pCtx->draft, deployTaskList));

    nodesDestroyNode((SNode *)(*pRoot));
    *pRoot = NULL;
    
    TAOS_CHECK_EXIT(nodesStringToNode(pStream->pCreate->calcPlan, (SNode**)pRoot));
    // Security hardening (see "item 194"): re-decoded calcPlan must be a physical
    // plan; otherwise (*pRoot)->pChildren / pDag->numOfSubplans reads below are a
    // type-confused over-read. nodesStringToNode may return SUCCESS while leaving
    // *pRoot NULL (e.g. empty input), so NULL-check first, then type-check - the
    // same ordering used by the other hardening points (msmUPAddScanTask etc.).
    if (NULL == *pRoot || QUERY_NODE_PHYSICAL_PLAN != nodeType((SNode*)*pRoot)) {
      mstsError("stream:%" PRId64 " calcPlan root node invalid (null:%d, type:%d)", streamId, (*pRoot == NULL),
                (*pRoot ? nodeType((SNode*)*pRoot) : -1));
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }
    if (subQ) {
      pDag = (SQueryPlan *)nodesListGetNode((*pRoot)->pChildren, dagIdx);
    } else {
      pDag = *pRoot;
    }

    mstsDebug("total %d runner tasks added for deploy %d", totalTaskNum, deployId);
  }

  pInfo->runnerNum += totalTaskNum;
  
_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  taosArrayDestroy(deployTaskList);

  return code;
}

static int32_t msmReBuildRunnerTasks(SStmGrpCtx* pCtx, SQueryPlan* pDag, SStmStatus* pInfo, SStreamObj* pStream,
                                     SStmTaskAction* pAction) {
  int32_t code = 0;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t newNodeId = 0;
  SArray* deployTaskList = NULL;
  // Security hardening (see "item 194"): pDag is a client plan decoded with no type
  // check. Reading pDag->pSubplans below would be a type-confused over-read for a
  // non-physical-plan root node. nodesStringToNode may leave pDag NULL on empty
  // input, so NULL-check first, then type-check.
  if (NULL == pDag || QUERY_NODE_PHYSICAL_PLAN != nodeType((SNode*)pDag)) {
    mstsError("stream:%" PRId64 " rebuild calcPlan root node invalid (null:%d, type:%d)", streamId, (pDag == NULL),
              (pDag ? nodeType((SNode*)pDag) : -1));
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  }
  int32_t levelNum = (int32_t)LIST_LENGTH(pDag->pSubplans);
  int32_t        lowestLevelIdx = levelNum - 1;
  SNodeListNode *plans = NULL;
  int32_t        taskNum = 0;
  int32_t        totalTaskNum = 0;
  int32_t        deployId = 0;
  SStmTaskStatus* pRunner = NULL;

  int32_t taskIdx = 0;
  deployTaskList = taosArrayInit_s(sizeof(SStmTaskDeploy), pDag->numOfSubplans);
  TSDB_CHECK_NULL(deployTaskList, code, lino, _exit, terrno);

  for (int32_t r = 0; r < pAction->deployNum; ++r) {
    deployId = pAction->deployId[r];

    pRunner = taosArrayGet(pInfo->runners[deployId], 0);

    if (!pRunner || taosArrayGetSize(pInfo->runners[deployId]) != pDag->numOfSubplans) {
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }
    totalTaskNum = 0;

    newNodeId = msmSelectBuildSnode(pCtx, pStream, r == 0);
    if (!GOT_SNODE(newNodeId)) {
      TAOS_CHECK_EXIT(terrno);
    }

    taskIdx = 0;
    
    SListCell* pSubplanCell = nodesListGetCell(pDag->pSubplans, lowestLevelIdx);
    for (int32_t i = lowestLevelIdx; i >= 0;
         --i, pSubplanCell = (NULL != pSubplanCell ? pSubplanCell->pPrev : NULL)) {
      plans = (SNodeListNode *)(NULL != pSubplanCell ? pSubplanCell->pNode : NULL);
      taskNum = (int32_t)LIST_LENGTH(plans->pNodeList);
      totalTaskNum += taskNum;
      if (totalTaskNum > pDag->numOfSubplans) TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);

      SListCell* pNodeCell = NULL != plans->pNodeList ? plans->pNodeList->pHead : NULL;
      for (int32_t n = 0; n < taskNum;
           ++n, pNodeCell = (NULL != pNodeCell ? pNodeCell->pNext : NULL)) {
        SSubplan *plan = (SSubplan *)(NULL != pNodeCell ? pNodeCell->pNode : NULL);
        // Security hardening (see "item 194"): list elements are taken as SSubplan
        // with no type check. Reject wrong-typed nodes before walking them.
        if (QUERY_NODE_PHYSICAL_SUBPLAN != nodeType((SNode*)plan)) {
          mstsError("stream:%" PRId64 " rebuild subplan[%d] node type %d is not QUERY_NODE_PHYSICAL_SUBPLAN",
                    streamId, n, nodeType((SNode*)plan));
          TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
        }

        int32_t newTaskIdx = MND_SET_RUNNER_TASKIDX(i, n);
        if (pRunner->id.taskIdx != newTaskIdx) {
          mstsError("runner TASK:%" PRId64 " taskIdx %d mismatch with newTaskIdx:%d", pRunner->id.taskId, pRunner->id.taskIdx, newTaskIdx);
          TAOS_CHECK_EXIT(TSDB_CODE_STREAM_INTERNAL_ERROR);
        }

        pRunner->id.nodeId = newNodeId;

        SStmTaskDeploy* pDeploy = taosArrayGet(deployTaskList, taskIdx++);
        pDeploy->task.type = pRunner->type;
        pDeploy->task.streamId = streamId;
        pDeploy->task.taskId = pRunner->id.taskId;
        pDeploy->task.deployId = deployId;
        pDeploy->task.flags = pRunner->flags | STREAM_FLAG_REDEPLOY_RUNNER;
        pDeploy->task.seriousId = pRunner->id.seriousId;
        pDeploy->task.nodeId = pRunner->id.nodeId;
        pDeploy->task.taskIdx = pRunner->id.taskIdx;
        TAOS_CHECK_EXIT(msmBuildRunnerDeployInfo(pDeploy, plan, pStream, pInfo, 0 == i));

        pRunner++;
      }

      mstsDebug("level %d initialized, taskNum:%d", i, taskNum);
    }

    if (totalTaskNum != pDag->numOfSubplans) TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
#ifdef BUILD_TEST_SEAM
    if (gTdSeam_msmDeployPhase) gTdSeam_msmDeployPhase("runner-encode", pStream->name);
#endif
    TAOS_CHECK_EXIT(msmUpdateRunnerPlans(pCtx, deployTaskList, pStream));

    TAOS_CHECK_EXIT(msmStageRunnerTasks(pCtx->draft, deployTaskList));

    nodesDestroyNode((SNode *)pDag);
    pDag = NULL;

    if (r + 1 == pAction->deployNum) break;
    TAOS_CHECK_EXIT(nodesStringToNode(pStream->pCreate->calcPlan, (SNode**)&pDag));
    // Security hardening (see "item 194"): the re-decoded pDag for the next loop
    // iteration must be validated too - otherwise the entry check above is bypassed
    // when deployNum > 1 and pDag->pSubplans is dereferenced unvalidated next round.
    // Same NULL + type guard as the function entry.
    if (NULL == pDag || QUERY_NODE_PHYSICAL_PLAN != nodeType((SNode*)pDag)) {
      mstsError("stream:%" PRId64 " rebuild re-decoded calcPlan root node invalid (null:%d, type:%d)", streamId,
                (pDag == NULL), (pDag ? nodeType((SNode*)pDag) : -1));
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }
  }

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  nodesDestroyNode((SNode *)pDag);
  taosArrayDestroy(deployTaskList);

  return code;
}

int32_t msmSetStreamRunnerExecReplica(int64_t streamId, SStmStatus* pInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  // STREAMTODO

  int32_t runnerDeploys = atomic_load_32(&tsNumOfStreamRunnerDeploys);
  int32_t runnerReplicas = atomic_load_32(&tsNumOfStreamRunnerReplicas);

  pInfo->runnerDeploys = TMIN(TMAX(runnerDeploys, 1), MND_STREAM_RUNNER_DEPLOY_NUM);
  atomic_store_32(&pInfo->runnerReplica, TMAX(runnerReplicas, 1));

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

// The caller owns subEP through serialization. Restore the decoded plan's original list before freeing it.
static int32_t msmBindCalcReaderPlan(const char* scanPlan, SNodeList* subEP, char** result) {
  SNode*  node = NULL;
  int32_t code = nodesStringToNode(scanPlan, &node);
  if (code == TSDB_CODE_SUCCESS) {
    if (node == NULL || nodeType(node) != QUERY_NODE_PHYSICAL_SUBPLAN) {
      code = TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
    } else {
      SSubplan*  plan = (SSubplan*)node;
      SNodeList* saved = plan->pSubQ;
      plan->pSubQ = subEP;
      code = nodesNodeToString(node, false, result, NULL);
      plan->pSubQ = saved;
      if (code == TSDB_CODE_SUCCESS && *result == NULL) code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  nodesDestroyNode(node);
  return code;
}

int32_t msmQueueVgroupCalcReaders(SStmGrpCtx* pCtx, SStreamObj* pStream, SStmStatus* pInfo, SNodeList* subEP) {
#ifdef TD_ENTERPRISE
  // External calc readers are already queued on their snode with the source-specific deploy spec.
  if (STREAM_IS_REF_EXT_SOURCE(pStream->flags)) return TSDB_CODE_SUCCESS;
#endif
  int32_t                code = TSDB_CODE_SUCCESS;
  int32_t                lino = 0;
  int64_t                streamId = pStream->pCreate->streamId;
  const SStreamCalcScan* previous = NULL;
  char*                  boundPlan = NULL;
  // Full deployment creates calcReaders in scan order. All vgroups for one scan use the same endpoints.
  for (SListNode* n = pInfo->calcReaders ? listHead(pInfo->calcReaders) : NULL; n; n = TD_DLIST_NODE_NEXT(n)) {
    SStmTaskStatus*        reader = (SStmTaskStatus*)n->data;
    const SStreamCalcScan* scan = taosArrayGet(pInfo->pCreate->calcScanPlanList, reader->id.taskIdx);
    TSDB_CHECK_NULL(scan, code, lino, _exit, TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    if (subEP != NULL && previous != scan) {
      taosMemoryFreeClear(boundPlan);
#ifdef BUILD_TEST_SEAM
      if (pCtx && pCtx->draft && gTdSeam_msmDeployPhase) gTdSeam_msmDeployPhase("reader-bind", pStream->name);
#endif
      TAOS_CHECK_EXIT(msmBindCalcReaderPlan(scan->scanPlan, subEP, &boundPlan));
      previous = scan;
    }
    TAOS_CHECK_EXIT(
        msmQueueCalcReader(pCtx, reader, pInfo, streamId, boundPlan ? boundPlan : scan->scanPlan, scan, NULL, pStream));
  }
_exit:
  taosMemoryFree(boundPlan);
  if (code) mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  return code;
}

static int32_t msmBuildRunnerTasks(SStmGrpCtx* pCtx, SStmStatus* pInfo, SStreamObj* pStream) {
  if (NULL == pStream->pCreate->calcPlan) {
    return msmQueueVgroupCalcReaders(pCtx, pStream, pInfo, NULL);
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SQueryPlan* pPlan = NULL;
  SNodeList*  pSubEP = NULL;
  SNode*      pNode = NULL;
  int32_t     subQueryPlans = 0;

  TAOS_CHECK_EXIT(nodesStringToNode(pStream->pCreate->calcPlan, (SNode**)&pPlan));

  // Security hardening (see "item 194"): the client calcPlan JSON is decoded with
  // no type check; a non-physical-plan root (e.g. a tiny LeftValue node) makes the
  // pPlan->pChildren / numOfSubplans reads below a heap over-read. nodesStringToNode
  // may leave pPlan NULL on empty input, so NULL-check first, then type-check.
  if (NULL == pPlan || QUERY_NODE_PHYSICAL_PLAN != nodeType((SNode*)pPlan)) {
    mstsError("stream:%" PRId64 " calcPlan root node invalid (null:%d, type:%d)", streamId, (pPlan == NULL),
              (pPlan ? nodeType((SNode*)pPlan) : -1));
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  }

  FOREACH(pNode, pPlan->pChildren) {
    SQueryPlan *calcSubQPlan = (SQueryPlan *)pNode;

    subQueryPlans += calcSubQPlan->numOfSubplans;
  }

  for (int32_t i = 0; i < pInfo->runnerDeploys; ++i) {
    pInfo->runners[i] = taosArrayInit(pPlan->numOfSubplans + subQueryPlans, sizeof(SStmTaskStatus));
    TSDB_CHECK_NULL(pInfo->runners[i], code, lino, _exit, terrno);
  }

  for (int32_t i = 0; i < LIST_LENGTH(pPlan->pChildren); ++i) {
    code = msmBuildRunnerTasksImpl(pCtx, i, pInfo, pStream, &pPlan, &pSubEP);
    TAOS_CHECK_EXIT(code);
  }

  code = msmBuildRunnerTasksImpl(pCtx, -1, pInfo, pStream, &pPlan, &pSubEP);
  TAOS_CHECK_EXIT(code);

  TAOS_CHECK_EXIT(msmQueueVgroupCalcReaders(pCtx, pStream, pInfo, pSubEP));

_exit:
  nodesDestroyNode((SNode *)pPlan);
  if (pSubEP) {
    nodesDestroyList(pSubEP);
  }

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmBuildStreamTasks(SStmGrpCtx* pCtx, SStmStatus* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;

  mstsInfo("start to deploy stream tasks, deployTimes:%" PRId64, pInfo->deployTimes);

  pCtx->triggerTaskId = msmAssignTaskId();
  pCtx->triggerNodeId = msmSelectBuildSnode(pCtx, pStream, true);
  if (!GOT_SNODE(pCtx->triggerNodeId)) {
    TAOS_CHECK_EXIT(terrno);
  }

  TAOS_CHECK_EXIT(msmBuildReaderTasks(pCtx, pInfo, pStream));
  TAOS_CHECK_EXIT(msmBuildRunnerTasks(pCtx, pInfo, pStream));
  TAOS_CHECK_EXIT(msmBuildTriggerTasks(pCtx, pInfo, pStream));
  
_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmInitTrigReaderList(SStmGrpCtx* pCtx, SStmStatus* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SSdb   *pSdb = pCtx->pMnode->pSdb;
  SStmTaskStatus* pState = NULL;
  SDbObj* pDb = NULL;
  SStreamExtTriggerSpec* pTrigExtSpec = msmGetTrigExtSpec(pStream);
  
  switch (pStream->pCreate->triggerTblType) {
    case TSDB_NORMAL_TABLE:
    case TSDB_CHILD_TABLE:
    case TSDB_VIRTUAL_CHILD_TABLE:
    case TSDB_VIRTUAL_NORMAL_TABLE: {
      pInfo->trigReaders = taosArrayInit_s(sizeof(SStmTaskStatus), 1);
      TSDB_CHECK_NULL(pInfo->trigReaders, code, lino, _exit, terrno);
      pInfo->trigReaderNum = 1;
      break;
    }
    case TSDB_SUPER_TABLE: {
#ifdef TD_ENTERPRISE
      if (pTrigExtSpec != NULL) {
        pInfo->trigReaders = taosArrayInit_s(sizeof(SStmTaskStatus), 1);
        TSDB_CHECK_NULL(pInfo->trigReaders, code, lino, _exit, terrno);
        pInfo->trigReaderNum = 1;
        mstsDebug("msmInitTrigReaderList: EXT super-table trigger readers set to 1 for stream %" PRId64,
                  streamId);
        break;
      }
#endif
      pDb = mndAcquireDb(pCtx->pMnode, pStream->pCreate->triggerDB);
      if (NULL == pDb) {
        code = terrno;
        mstsError("failed to acquire db %s, error:%s", pStream->pCreate->triggerDB, terrstr());
        goto _exit;
      }

      pInfo->trigReaders = taosArrayInit(pDb->cfg.numOfVgroups, sizeof(SStmTaskStatus));
      TSDB_CHECK_NULL(pInfo->trigReaders, code, lino, _exit, terrno);
      pInfo->trigReaderNum = pDb->cfg.numOfVgroups;
      mndReleaseDb(pCtx->pMnode, pDb);
      pDb = NULL;
      break;
    }
    default:
      pInfo->trigReaderNum = 0;
      mstsDebug("%s ignore triggerTblType %d", __FUNCTION__, pStream->pCreate->triggerTblType);
      break;
  }

_exit:

  if (code) {
    mndReleaseDb(pCtx->pMnode, pDb);
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmInitDeployInput(SStmStatus* status) {
  SStmDeployInput* input = taosMemoryCalloc(1, sizeof(*input));
  if (!input) return terrno;
  input->create = status->pCreate;
  input->name = status->streamName;
  input->refs = 1;
  status->deployInput = input;
  return TSDB_CODE_SUCCESS;
}

static int32_t msmInitStmStatus(SStmGrpCtx* pCtx, SStmStatus* pStatus, SStreamObj* pStream, bool initList) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;

  pStatus->lastActionTs = INT64_MIN;

  if (!pStatus->recalcPersistOpsInitialized) {
    mndStreamRecalcInitStatus(pStatus);
  }

  if (NULL == pStatus->streamName) {
    pStatus->streamName = taosStrdup(pStream->name);
    TSDB_CHECK_NULL(pStatus->streamName, code, lino, _exit, terrno);
  }

  TAOS_CHECK_EXIT(tCloneStreamCreateDeployPointers(pStream->pCreate, &pStatus->pCreate));
  TAOS_CHECK_EXIT(msmInitDeployInput(pStatus));

  if (pStream->pCreate->numOfCalcSubplan > 0) {
    pStatus->runnerNum = pStream->pCreate->numOfCalcSubplan;
    atomic_store_32(&pStatus->runnerReplica, MND_STREAM_RUNNER_REPLICA_UNKNOWN);
  }

  if (initList) {
    TAOS_CHECK_EXIT(msmInitTrigReaderList(pCtx, pStatus, pStream));

    int32_t subPlanNum = taosArrayGetSize(pStream->pCreate->calcScanPlanList);
    if (subPlanNum > 0) {
      // Cache scans bind directly to the Trigger; every non-cache partition owns one Reader.
      int64_t readers = 0;
      for (int32_t i = 0; i < subPlanNum; ++i) {
        SStreamCalcScan* scan = taosArrayGet(pStream->pCreate->calcScanPlanList, i);
        if (!scan->readFromCache) readers += taosArrayGetSize(scan->vgList);
      }
      if (readers > INT32_MAX) TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      pStatus->calcReaderNum = (int32_t)readers;
      pStatus->calcReaders = tdListNew(sizeof(SStmTaskStatus));
      TSDB_CHECK_NULL(pStatus->calcReaders, code, lino, _exit, terrno);
    }
  }

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static void msmCancelQueuedStreamTasks(int64_t streamId, SArray* tasks, int32_t* deployed) {
  int32_t count = taosArrayGetSize(tasks);
  if (atomic_load_32(deployed) == count) return;
  for (int32_t i = 0; i < count; ++i) {
    SStmTaskToDeployExt* entry = taosArrayGet(tasks, i);
    if (entry->deployed || entry->deploy.task.streamId != streamId) continue;
    mstDestroySStmTaskToDeployExt(entry);
    entry->deployed = true;
    (void)atomic_add_fetch_32(deployed, 1);
  }
}

static void msmCancelQueuedSnodeTasks(int64_t streamId, SStmSnodeTasksDeploy* node) {
  (void)mstWaitLock(&node->lock, false);
  msmCancelQueuedStreamTasks(streamId, node->triggerList, &node->triggerDeployed);
  msmCancelQueuedStreamTasks(streamId, node->runnerList, &node->runnerDeployed);
  msmCancelQueuedStreamTasks(streamId, node->calcReaderList, &node->calcReaderDeployed);
  taosWUnLockLatch(&node->lock);
}

static void msmCancelQueuedVgTasks(int64_t streamId, SStmVgTasksToDeploy* vg) {
  (void)mstWaitLock(&vg->lock, false);
  msmCancelQueuedStreamTasks(streamId, vg->taskList, &vg->deployed);
  taosWUnLockLatch(&vg->lock);
}

static void msmRemoveStreamRegistrations(int64_t streamId, SStmStatus* stream) {
  void* it = NULL;
  while ((it = taosHashIterate(stream->registrations, it))) {
    int64_t key[2];
    memcpy(key, taosHashGetKey(it, NULL), sizeof(key));
    int32_t nodeId = (int32_t)key[1];
    if (key[0] == MST_REG_TASK) {
      int64_t taskKey[2] = {streamId, key[1]};
      TAOS_UNUSED(taosHashRemove(mStreamMgmt.taskMap, taskKey, sizeof(taskKey)));
    } else if (key[0] == MST_REG_SNODE) {
      SStmSnodeStatus* node = taosHashGet(mStreamMgmt.snodeMap, &nodeId, sizeof(nodeId));
      if (node && node->streamTasks) TAOS_UNUSED(taosHashRemove(node->streamTasks, &streamId, sizeof(streamId)));
      SStmSnodeTasksDeploy* queue = taosHashGet(mStreamMgmt.toDeploySnodeMap, &nodeId, sizeof(nodeId));
      if (queue) msmCancelQueuedSnodeTasks(streamId, queue);
    } else {
      SStmVgroupStatus* vg = taosHashGet(mStreamMgmt.vgroupMap, &nodeId, sizeof(nodeId));
      if (vg && vg->streamTasks) TAOS_UNUSED(taosHashRemove(vg->streamTasks, &streamId, sizeof(streamId)));
      SStmVgTasksToDeploy* queue = taosHashGet(mStreamMgmt.toDeployVgMap, &nodeId, sizeof(nodeId));
      if (queue) msmCancelQueuedVgTasks(streamId, queue);
    }
  }
  taosHashClear(stream->registrations);
}

static void msmAdoptBuiltTask(SStmStatus* stream, SStmTaskStatus* task, int64_t now) {
  task->pStream = stream;
  task->lastUpTs = now;
}

static int32_t msmPublishBuiltTasks(SStmGrpCtx* ctx, SStreamObj* stream, SStmStatus* live, SStmStatus* built,
                                    SArray* tasks) {
  int64_t id = stream->pCreate->streamId;
  int64_t published = taosGetTimestampMs();
  live->trigReaders = built->trigReaders;
  live->calcReaders = built->calcReaders;
  live->calcReaderNum = built->calcReaderNum;
  live->triggerTask = built->triggerTask;
  built->trigReaders = NULL;
  built->calcReaders = NULL;
  built->triggerTask = NULL;
  live->runnerNum = built->runnerNum;
  live->runnerDeploys = built->runnerDeploys;
  atomic_store_32(&live->runnerReplica, built->runnerReplica);
  msmAdoptBuiltTask(live, live->triggerTask, published);
  for (int32_t i = 0; i < taosArrayGetSize(live->trigReaders); ++i) {
    msmAdoptBuiltTask(live, taosArrayGet(live->trigReaders, i), published);
  }
  for (SListNode* n = live->calcReaders ? listHead(live->calcReaders) : NULL; n; n = TD_DLIST_NODE_NEXT(n)) {
    msmAdoptBuiltTask(live, (SStmTaskStatus*)n->data, published);
  }
  for (int32_t i = 0; i < live->runnerDeploys; ++i) {
    live->runners[i] = built->runners[i];
    built->runners[i] = NULL;
    for (int32_t j = 0; j < taosArrayGetSize(live->runners[i]); ++j) {
      msmAdoptBuiltTask(live, taosArrayGet(live->runners[i], j), published);
    }
  }

  TAOS_CHECK_RETURN(msmSTAddToTaskMap(ctx, id, live->trigReaders, NULL, NULL));
  TAOS_CHECK_RETURN(msmSTAddToTaskMap(ctx, id, NULL, live->calcReaders, NULL));
  TAOS_CHECK_RETURN(msmSTAddToVgroupMap(ctx, id, live->trigReaders, NULL, NULL, true));
  TAOS_CHECK_RETURN(msmSTAddToVgroupMap(ctx, id, NULL, live->calcReaders, NULL, false));
  for (int32_t i = 0; i < live->runnerDeploys; ++i) {
    TAOS_CHECK_RETURN(msmSTAddToTaskMap(ctx, id, live->runners[i], NULL, NULL));
    TAOS_CHECK_RETURN(msmSTAddToSnodeMap(ctx, id, live->runners[i], NULL, 0, i));
  }
  TAOS_CHECK_RETURN(msmSTAddToTaskMap(ctx, id, NULL, NULL, live->triggerTask));
  TAOS_CHECK_RETURN(msmSTAddToSnodeMap(ctx, id, NULL, live->triggerTask, 1, -1));

  for (int32_t i = 0; i < taosArrayGetSize(tasks); ++i) {
    SStmTaskToDeployExt* entry = taosArrayGet(tasks, i);
    SStmTaskDeploy*      task = &entry->deploy;
    if (task->task.type == STREAM_READER_TASK) {
      TAOS_CHECK_RETURN(msmPublishPreparedReader(entry));
    } else {
      int32_t code = task->task.type == STREAM_TRIGGER_TASK ? msmTDAddTriggerToSnodeMap(task, stream)
                                                            : msmTDAddRunnerToSnodeMap(task, stream);
      TAOS_CHECK_RETURN(code);
      (void)atomic_add_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);
    }
    entry->deployed = true;  // The live queue owns the payload.
  }
  return TSDB_CODE_SUCCESS;
}

static SStmPlacementVersion msmPlacementVersion(SSdb* db) {
  return (SStmPlacementVersion){.vg = sdbGetTableVer(db, SDB_VGROUP),
                                .snode = sdbGetTableVer(db, SDB_SNODE),
                                .dnode = sdbGetTableVer(db, SDB_DNODE),
                                .mnode = sdbGetTableVer(db, SDB_MNODE)};
}

static int32_t msmValidateDeploySnode(SMnode* mnode, int32_t nodeId, SStmPlacementVersion start,
                                      SStmPlacementVersion now) {
  SStmSnodeStatus* node = taosHashGet(mStreamMgmt.snodeMap, &nodeId, sizeof(nodeId));
  if (!node || node->runnerThreadNum < 0) return TSDB_CODE_SNODE_NO_AVAILABLE_NODE;
  if (start.snode != now.snode) {
    SSnodeObj* row = mndAcquireSnode(mnode, nodeId);
    if (!row) return TSDB_CODE_SNODE_NO_AVAILABLE_NODE;
    mndReleaseSnode(mnode, row);
  }
  if (start.dnode != now.dnode) {
    SDnodeObj* row = mndAcquireDnode(mnode, nodeId);
    if (!row) return TSDB_CODE_SNODE_NO_AVAILABLE_NODE;
    mndReleaseDnode(mnode, row);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t msmValidateDraftPlacement(SMnode* mnode, SStmDeployDraft* draft, SStmPlacementVersion start,
                                         SStreamObj* stream) {
  SStmPlacementVersion now = msmPlacementVersion(mnode->pSdb);
  TAOS_CHECK_RETURN(msmValidateMetadataCoverage(mnode, draft, stream));
  for (int32_t i = 0; i < taosArrayGetSize(draft->tasks); ++i) {
    SStmTaskDeploy* task = &((SStmTaskToDeployExt*)taosArrayGet(draft->tasks, i))->deploy;
    if (task->task.type == STREAM_READER_TASK && !task->msg.reader.pExtSpec) continue;
    TAOS_CHECK_RETURN(msmValidateDeploySnode(mnode, task->task.nodeId, start, now));
  }
  if (start.vg != now.vg && draft->triggerDbUid) {
    SDbObj* db = mndAcquireDb(mnode, stream->pCreate->triggerDB);
    if (!db) return TSDB_CODE_MND_STREAM_NOT_AVAILABLE;
    taosRLockLatch(&db->lock);
    bool changed = db->uid != draft->triggerDbUid || db->vgVersion != draft->triggerVgVersion;
    taosRUnLockLatch(&db->lock);
    mndReleaseDb(mnode, db);
    if (changed) return TSDB_CODE_MND_STREAM_NOT_AVAILABLE;
  }
  if (start.vg == now.vg && start.mnode == now.mnode && start.dnode == now.dnode) return 0;
  void* it = NULL;
  while ((it = taosHashIterate(draft->endpoints, it))) {
    int32_t key[2];
    memcpy(key, taosHashGetKey(it, NULL), sizeof(key));
    SEpSet ep = {0};
    if (key[0] == SDB_VGROUP && (start.vg != now.vg || start.dnode != now.dnode)) {
      ep = mndGetVgroupEpsetById(mnode, key[1]);
    } else if (key[0] == SDB_DNODE && start.dnode != now.dnode) {
      ep = mndGetDnodeEpsetById(mnode, key[1]);
    } else if (key[0] == SDB_MNODE && (start.mnode != now.mnode || start.dnode != now.dnode)) {
      mndGetMnodeEpSet(mnode, &ep);
    } else {
      continue;
    }
    if (!ep.numOfEps || !isEpsetEqual(it, &ep)) {
      taosHashCancelIterate(draft->endpoints, it);
      return TSDB_CODE_MND_STREAM_NOT_AVAILABLE;
    }
  }
  return 0;
}

static void msmUnlockActionBuild(void) {
  taosWUnLockLatch(&mStreamMgmt.actionQLock);
  taosRUnLockLatch(&mStreamMgmt.runtimeLock);
  taosRUnLockLatch(&mStreamMgmt.heartbeatLock);
}

static void msmLockActionBuild(void) {
  (void)mstWaitLock(&mStreamMgmt.heartbeatLock, true);
  (void)mstWaitLock(&mStreamMgmt.runtimeLock, true);
  (void)mstWaitLock(&mStreamMgmt.actionQLock, false);
}

static bool msmSameBuildEpoch(int64_t epoch) {
  return atomic_load_8(&mStreamMgmt.active) && mStreamMgmt.stat.activeTimes == epoch;
}

static void msmSnapshotStreamRow(SStreamObj* stream, SStreamObj* snapshot) {
  taosRLockLatch(&stream->lock);
  snapshot->flags = stream->flags;
  snapshot->mainSnodeId = stream->mainSnodeId;
  snapshot->pCreate = stream->pCreate;
  tstrncpy(snapshot->name, stream->name, sizeof(snapshot->name));
  taosRUnLockLatch(&stream->lock);
}

// Call with the action queue write lock or exclusive runtime ownership.
static void msmStopStreamByGrant(int64_t streamId, SStmStatus* status, int32_t code) {
  if (!status || MST_IS_GRANT_STOPPED(atomic_load_8(&status->stopped))) return;
  (void)msmAdvanceBuildVersion(status);
  atomic_store_8(&status->stopped, 4);
#ifdef TD_ENTERPRISE
  msmCancelSchemaBatch(status->pendingSchema);
  status->pendingSchema = NULL;
#endif
  mstsInfo("set stream stopped since %s", tstrerror(code));
}

static bool msmStopIfGrantExpired(int64_t streamId, SStmStatus* status) {
  int32_t code = grantCheckExpire(TSDB_GRANT_STREAMS);
  if (code >= 0) return false;
  msmStopStreamByGrant(streamId, status, code);
  return true;
}

static int32_t msmDeployStreamTasks(SStmGrpCtx* ctx, SStreamObj* stream, SStmStatus* live,
                                    const SStmStreamAction* action) {
  int32_t    code = 0;
  int64_t    id = stream->pCreate->streamId;
  SStmStatus initial = {0}, built = {0};
  bool       redeploy = live != NULL && !action->firstDeploy;
  int64_t    epoch = mStreamMgmt.stat.activeTimes;
  if (!live) {
    // No runtime state is visible while copying a first deployment's plan. A STOP/DROP
    // cancels the owner's ID even if START follows before the copy returns.
    msmUnlockActionBuild();
#ifdef BUILD_TEST_SEAM
    if (gTdSeam_msmDeployPhase) gTdSeam_msmDeployPhase("first-copy", stream->name);
#endif
    code = msmInitStmStatus(ctx, &initial, stream, false);
    msmLockActionBuild();
    if (!msmSameBuildEpoch(epoch) || mStreamMgmt.deployingStreamId != id || atomic_load_8(&stream->userStopped) ||
        atomic_load_8(&stream->userDropped) || msmStopIfGrantExpired(id, NULL)) {
      mstDestroySStmStatus(&initial);
      return TSDB_CODE_SUCCESS;
    }
    if (!code) code = taosHashPut(mStreamMgmt.streamMap, &id, sizeof(id), &initial, sizeof(initial));
    if (code) {
      mstDestroySStmStatus(&initial);
      return code;
    }
    live = taosHashGet(mStreamMgmt.streamMap, &id, sizeof(id));
  }
  // Pin only immutable deploy fields. Health and leadership changes may destroy the status while building.
  if (!live->deployInput) TAOS_CHECK_RETURN(msmInitDeployInput(live));
  SStmDeployInput* input = live->deployInput;
  (void)atomic_add_fetch_32(&input->refs, 1);
  int64_t              version = msmAdvanceBuildVersion(live);
  int8_t          stopped = atomic_load_8(&live->stopped);
  SStmPlacementVersion placement = msmPlacementVersion(ctx->pMnode->pSdb);
  SStmDeployDraft draft = {.epoch = mStreamMgmt.stat.activeTimes};
  SStmGrpCtx      buildCtx = {.pMnode = ctx->pMnode, .currTs = ctx->currTs, .draft = &draft};
  // The caller owns an SDB row reference. Its plan is immutable after insertion.
  // Copy mutable placement fields under the row latch before doing any plan work.
  SStreamObj snapshot = {0};
  msmSnapshotStreamRow(stream, &snapshot);
  built.pCreate = input->create;
  built.streamName = input->name;
  built.deployTimes = live->deployTimes + redeploy;
  built.runnerNum = stream->pCreate->numOfCalcSubplan;
  live = NULL;

  msmUnlockActionBuild();
  draft.tasks = taosArrayInit(4, sizeof(SStmTaskToDeployExt));
  draft.scanMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (!draft.tasks || !draft.scanMap) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  } else {
    taosHashSetFreeFp(draft.scanMap, mstDestroyScanAddrList);
    if (built.runnerNum > 0) code = msmSetStreamRunnerExecReplica(id, &built);
    if (!code) code = msmBuildStreamTasks(&buildCtx, &built, &snapshot);
  }
  msmLogMetadataRecovery(&draft, id);
  if (!code) code = msmPrepareMetadataSnapshot(&draft);
  taosHashCleanup(draft.scanMap);

  SStreamRecalcRestore restore = {0};
  bool                 retried = false;
_prepare_recalcs:
  if (!code) {
    // A health check drains heartbeat readers before registering a runtime writer.
    (void)mstWaitLock(&mStreamMgmt.heartbeatLock, true);
    (void)mstWaitLock(&mStreamMgmt.runtimeLock, true);
    live = msmSameBuildEpoch(epoch) ? taosHashGet(mStreamMgmt.streamMap, &id, sizeof(id)) : NULL;
    bool capture = live && live->deployInput == input && atomic_load_64(&live->buildVersion) == version;
    if (capture) code = mndStreamRecalcCaptureRecords(live, &restore);
    taosRUnLockLatch(&mStreamMgmt.runtimeLock);
    taosRUnLockLatch(&mStreamMgmt.heartbeatLock);
    live = NULL;
    if (capture && !code) {
      code = mndStreamRecalcPrepareRestore(stream, &restore, built.triggerTask->id.taskId,
                                           built.triggerTask->id.seriousId);
    }
  }

  // Drain active heartbeat handlers before publishing new indexes and replacing the old task arrays.
  (void)mstWaitLock(&mStreamMgmt.heartbeatLock, false);
  (void)mstWaitLock(&mStreamMgmt.runtimeLock, false);
  if (atomic_load_8(&mStreamMgmt.active) && mStreamMgmt.stat.activeTimes == draft.epoch) {
    live = taosHashGet(mStreamMgmt.streamMap, &id, sizeof(id));
  }
  SStreamObj* current = NULL;
  bool        owned = live && live->deployInput == input;
  bool valid = owned && atomic_load_64(&live->buildVersion) == version && atomic_load_8(&live->stopped) == stopped &&
               mndAcquireStream(ctx->pMnode, snapshot.name, &current) == 0 && current == stream &&
               !atomic_load_8(&stream->userStopped) && !atomic_load_8(&stream->userDropped);
  if (valid && msmStopIfGrantExpired(id, live)) valid = false;
  if (valid && atomic_load_32(&stream->mainSnodeId) != snapshot.mainSnodeId) {
    code = TSDB_CODE_MND_STREAM_NOT_AVAILABLE;
  }
  if (valid && !code) code = msmValidateDraftPlacement(ctx->pMnode, &draft, placement, &snapshot);
  if (valid && !code && !mndStreamRecalcCommitRestore(current, live, &restore)) {
    if (!retried) {
      retried = true;
      mndReleaseStream(ctx->pMnode, current);
      taosWUnLockLatch(&mStreamMgmt.runtimeLock);
      taosWUnLockLatch(&mStreamMgmt.heartbeatLock);
      taosArrayDestroy(restore.records);
      restore.records = NULL;
      live = NULL;
      goto _prepare_recalcs;
    }
    SStmQNode resume = {.type = STREAM_ACT_DEPLOY, .streamAct = true};
    resume.action.stream = *action;
    resume.action.stream.retryVersion = version;
    resume.action.stream.deferredTaskId = built.triggerTask->id.taskId;
    resume.action.stream.firstDeploy = !redeploy;
    resume.action.stream.runnerSnapshotRedeployOwner = false;  // The current action releases its claim.
    int64_t ahead = atomic_load_64(&mStreamMgmt.actionQ->qRemainNum);
    code = mstEnqueueAction(mStreamMgmt.actionQ, &resume);
    if (!code) {
      atomic_store_64(&live->deferredTaskId, resume.action.stream.deferredTaskId);
      if (ctx->actionsBeforeDeferred < 0) ctx->actionsBeforeDeferred = ahead;
      goto _release_draft;
    }
  }
  if (valid) {
    if (redeploy) {
      msmRemoveStreamRegistrations(id, live);
      // The prepared records already exclude the replaced trigger's task-owned metrics.
      if (!code && live->triggerTask) live->triggerTask->pStream = NULL;
      mstResetSStmStatus(live);
      ++live->deployTimes;
    }
    atomic_store_8(&live->stopped, 0);
    if (!code) {
      code = msmPublishBuiltTasks(ctx, current, live, &built, draft.tasks);
      if (code) msmRemoveStreamRegistrations(id, live);
    }
    if (code) {
      msmStopStreamByError(id, live, code, taosGetTimestampMs());
    } else {
      mInfo(KEYOP("stream-create", "STEP") "stream:%" PRId64 " stream fully deployed, taskId:%" PRIx64, id,
            live->triggerTask->id.taskId);
    }
  } else {
    code = TSDB_CODE_SUCCESS;  // Obsolete work has no effect on the current stream.
  }
_release_draft:
  if (current) mndReleaseStream(ctx->pMnode, current);
  taosWUnLockLatch(&mStreamMgmt.runtimeLock);
  taosWUnLockLatch(&mStreamMgmt.heartbeatLock);
  taosArrayDestroy(restore.records);
  taosHashCleanup(draft.checkedMetadataGroups);
  taosHashCleanup(draft.metadataCoverage);
  taosHashCleanup(draft.metadataScanIds);
  taosHashCleanup(draft.metadataCoverageCounts);
  taosHashCleanup(draft.boundMetadataVgroups);
  taosHashCleanup(draft.endpoints);
  taosArrayDestroyEx(draft.tasks, mstDestroySStmTaskToDeployExt);
  mstResetSStmStatus(&built);
  mstReleaseDeployInput(input);
  msmLockActionBuild();
  return code;
}

static int32_t msmSTRemoveStream(int64_t streamId, bool fromStreamMap) {
  int32_t code = TSDB_CODE_SUCCESS;
  void* pIter = NULL;

  while ((pIter = taosHashIterate(mStreamMgmt.toDeployVgMap, pIter))) {
    msmCancelQueuedVgTasks(streamId, pIter);
  }
  while ((pIter = taosHashIterate(mStreamMgmt.toDeploySnodeMap, pIter))) {
    msmCancelQueuedSnodeTasks(streamId, pIter);
  }

  while ((pIter = taosHashIterate(mStreamMgmt.snodeMap, pIter))) {
    SStmSnodeStatus* pSnode = (SStmSnodeStatus*)pIter;
    code = taosHashRemove(pSnode->streamTasks, &streamId, sizeof(streamId));
    if (TSDB_CODE_SUCCESS == code) {
      mstsDebug("stream removed from snodeMap %d, remainStreams:%d", *(int32_t*)taosHashGetKey(pIter, NULL), (int32_t)taosHashGetSize(pSnode->streamTasks));
    }
  }

  while ((pIter = taosHashIterate(mStreamMgmt.vgroupMap, pIter))) {
    SStmVgroupStatus* pVg = (SStmVgroupStatus*)pIter;
    code = taosHashRemove(pVg->streamTasks, &streamId, sizeof(streamId));
    if (TSDB_CODE_SUCCESS == code) {
      mstsDebug("stream removed from vgroupMap %d, remainStreams:%d", *(int32_t*)taosHashGetKey(pIter, NULL), (int32_t)taosHashGetSize(pVg->streamTasks));
    }
  }

  size_t keyLen = 0;
  while ((pIter = taosHashIterate(mStreamMgmt.taskMap, pIter))) {
    int64_t* pStreamId = taosHashGetKey(pIter, &keyLen);
    if (*pStreamId == streamId) {
      int64_t taskId = *(pStreamId + 1);
      code = taosHashRemove(mStreamMgmt.taskMap, pStreamId, keyLen);
      if (code) {
        mstsError("TASK:%" PRIx64 " remove from taskMap failed, error:%s", taskId, tstrerror(code));
      } else {
        mstsDebug("TASK:%" PRIx64 " removed from taskMap", taskId);
      }
    }
  }

  if (fromStreamMap) {
    code = taosHashRemove(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
    if (code) {
      mstsError("stream remove from streamMap failed, error:%s", tstrerror(code));
    } else {
      mstsDebug("stream removed from streamMap, remains:%d", taosHashGetSize(mStreamMgmt.streamMap));
    }
  }
  
  return code;
}

/* P6 g2: On each (re)deploy, refresh encryptedPassword in pStream->pCreate->extSpecs
 * from the live SDB so that a post-ALTER-EXTERNAL-SOURCE redeploy picks up
 * the latest credential rather than the one frozen into the stream object at
 * CREATE STREAM time.  Only compiled for TD_ENTERPRISE because
 * mndAcquireExtSource / SExtSourceObj are enterprise-only constructs.
 * Community builds are not affected (no extSpecs ever populated). */
#ifdef TD_ENTERPRISE
static int32_t msmRefreshExtSpecPasswords(SMnode *pMnode, SStreamObj *pStream) {
  if (!STREAM_IS_REF_EXT_SOURCE(pStream->flags) || pStream->pCreate->extSpecs == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t n = (int32_t)taosArrayGetSize(pStream->pCreate->extSpecs);
  for (int32_t i = 0; i < n; ++i) {
    SStreamExtTriggerSpec *pSpec = *(SStreamExtTriggerSpec **)taosArrayGet(pStream->pCreate->extSpecs, i);
    if (pSpec == NULL) continue;
    SExtSourceObj *pSrcObj = mndAcquireExtSource(pMnode, pSpec->sourceName);
    if (pSrcObj == NULL) {
      mWarn("stream %" PRId64 " spec[%d] source=%s not found in sdb during redeploy, password not refreshed",
               pStream->pCreate->streamId, i, pSpec->sourceName);
      /* Non-fatal: continue with the stale password; reader will fail again
       * and trigger another redeploy cycle.  Do not abort the deploy. */
      continue;
    }
    sdbReadLock(pMnode->pSdb, SDB_EXT_SOURCE);
    (void)memcpy(pSpec->encryptedPassword, pSrcObj->encryptedPassword,
                 TSDB_EXT_SOURCE_ENC_PASSWORD_LEN);
    tstrncpy(pSpec->options, pSrcObj->options, sizeof(pSpec->options));
    sdbUnLock(pMnode->pSdb, SDB_EXT_SOURCE);
    mDebug("stream %" PRId64 " spec[%d] source=%s encryptedPassword refreshed for redeploy",
              pStream->pCreate->streamId, i, pSpec->sourceName);
    mndReleaseExtSource(pMnode, pSrcObj);
  }
  return TSDB_CODE_SUCCESS;
}
#endif  /* TD_ENTERPRISE */

static int32_t msmLaunchStreamDeployAction(SStmGrpCtx* pCtx, SStmStreamAction* pAction) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pAction->streamId;
  char* streamName = pAction->streamName;
  SStreamObj* pStream = NULL;
  bool        ownsRunnerSnapshotRedeploy = false;

  SStmStatus* pStatus = (SStmStatus*)taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (pAction->deferredTaskId) {
    if (!pStatus || atomic_load_64(&pStatus->deferredTaskId) != pAction->deferredTaskId) return TSDB_CODE_SUCCESS;
    atomic_store_64(&pStatus->deferredTaskId, 0);
    if (atomic_load_64(&pStatus->buildVersion) != pAction->retryVersion) return TSDB_CODE_SUCCESS;
  }
  if (pAction->retryStopped) {
    if (!pAction->deferredTaskId) {
      if (!pStatus || !pStatus->retryQueued) return TSDB_CODE_SUCCESS;
      pStatus->retryQueued = false;
    }
    if (atomic_load_8(&pStatus->stopped) != pAction->retryStopped ||
        atomic_load_64(&pStatus->buildVersion) != pAction->retryVersion ||
        (MST_IS_ERROR_STOPPED(pAction->retryStopped) && taosGetTimestampMs() < pStatus->fatalRetryTs)) {
      return TSDB_CODE_SUCCESS;
    }
  }
  if (pStatus && pStatus->runnerNum > 0) {
    if (pAction->runnerSnapshotRedeployOwner) {
      ownsRunnerSnapshotRedeploy = true;
      if (!mstRunnerSnapshotUnknown(pStatus)) {
        atomic_store_8(&pStatus->runnerSnapshotRedeployPending, 0);
        mstsDebug("%s", "skip redundant runner snapshot redeploy since Replica is known");
        return TSDB_CODE_SUCCESS;
      }
    } else if (MST_RUNNER_SNAPSHOT_CLAIM_ACQUIRED == mstClaimRunnerSnapshotRedeploy(pStatus)) {
      ownsRunnerSnapshotRedeploy = true;
    }
  }

  if (pStatus && MST_IS_USER_STOPPED(atomic_load_8(&pStatus->stopped)) && !pAction->userAction) {
    if (ownsRunnerSnapshotRedeploy) atomic_store_8(&pStatus->runnerSnapshotRedeployPending, 0);
    return TSDB_CODE_SUCCESS;
  }
  int64_t epoch = mStreamMgmt.stat.activeTimes;

  code = mndAcquireStream(pCtx->pMnode, streamName, &pStream);
  if (TSDB_CODE_MND_STREAM_NOT_EXIST == code) {
    mstsWarn("stream %s no longer exists, ignore deploy", streamName);
    if (pStatus && ownsRunnerSnapshotRedeploy) {
      atomic_store_8(&pStatus->runnerSnapshotRedeployPending, 0);
    }
    return TSDB_CODE_SUCCESS;
  }

  TAOS_CHECK_EXIT(code);

  if (pStream->pCreate->streamId != streamId) {
    mstsWarn("stream %s already dropped by user, ignore deploy it", pAction->streamName);
    if (pStatus) {
      atomic_store_8(&pStatus->stopped, 2);
      mstsInfo("set stream %s stopped by user since streamId mismatch", streamName);
    }
    goto _exit;
  }

  int8_t userStopped = atomic_load_8(&pStream->userStopped);
  int8_t userDropped = atomic_load_8(&pStream->userDropped);
  if (userStopped || userDropped) {
    mstsWarn("stream %s is stopped %d or removing %d, ignore deploy", streamName, userStopped, userDropped);
    goto _exit;
  }

  if (msmStopIfGrantExpired(streamId, pStatus)) goto _exit;

#ifdef TD_ENTERPRISE
  /* P6 g2: refresh encrypted credentials so that a post-ALTER-EXT-SOURCE
   * redeploy picks up the latest password rather than the stale copy frozen
   * into the stream SDB record at CREATE STREAM time. */
  TAOS_CHECK_EXIT(msmRefreshExtSpecPasswords(pCtx->pMnode, pStream));
#endif

  mStreamMgmt.deployingStreamId = streamId;
  code = msmDeployStreamTasks(pCtx, pStream, pStatus, pAction);
  if (msmSameBuildEpoch(epoch)) mStreamMgmt.deployingStreamId = 0;
  TAOS_CHECK_EXIT(code);

_exit:

  mndReleaseStream(pCtx->pMnode, pStream);
  pStatus = mStreamMgmt.active && mStreamMgmt.stat.activeTimes == epoch
                ? taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId))
                : NULL;
  if (pStatus && ownsRunnerSnapshotRedeploy) {
    atomic_store_8(&pStatus->runnerSnapshotRedeployPending, 0);
  }

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int64_t msmPublishedTaskGeneration(const SStmStatus* status) {
  return status->triggerTask ? status->triggerTask->id.taskId : 0;
}

static void msmReleaseReaderRepair(int64_t streamId, int64_t taskId, int64_t seriousId, bool dispatched) {
  int64_t          key[] = {streamId, taskId};
  SStmTaskStatus** task = taosHashGet(mStreamMgmt.taskMap, key, sizeof(key));
  if (task && (*task)->id.seriousId == seriousId && atomic_load_64(&(*task)->readerRepairRetryTs) == INT64_MAX) {
    int64_t retryTs = dispatched ? taosGetTimestampMs() + MST_ISOLATION_DURATION : 0;
    (void)atomic_val_compare_exchange_64(&(*task)->readerRepairRetryTs, INT64_MAX, retryTs);
  }
}

static int32_t msmReLaunchReaderTask(SMnode* mnode, SStreamObj* pStream, SStmTaskAction* pAction, SStmStatus* pStatus,
                                     bool* readerQueued) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  int64_t          streamId = pAction->streamId;
  int64_t          key[] = {streamId, pAction->id.taskId};
  SStmTaskStatus** ppTask = taosHashGet(mStreamMgmt.taskMap, key, sizeof(key));
  if (NULL == ppTask) {
    mstsError("TASK:%" PRId64 " not in taskMap, remain:%d", pAction->id.taskId, taosHashGetSize(mStreamMgmt.taskMap));
    TAOS_CHECK_EXIT(TSDB_CODE_STREAM_INTERNAL_ERROR);
  }

  if ((*ppTask)->id.seriousId != pAction->id.seriousId) return TSDB_CODE_SUCCESS;

  if (STREAM_IS_REF_EXT_SOURCE(pAction->flag)) {
    // External-source readers run on the snode and their connector spec is not
    // retained on mnode after deployment, so this timeout path cannot rebuild
    // them; enqueueing them as vgroup readers with a snode id as vgId would
    // never recover the task.  Stop the stream instead: the retry lifecycle
    // redeploys the whole stream and the trigger re-registers the external
    // reader through the heartbeat mgmt requests.
    mstsWarn("EXT reader TASK:%" PRId64 " status not updated, trigger full stream redeploy, streamId:%" PRId64,
             pAction->id.taskId, streamId);
    msmStopStreamByError(streamId, pStatus, TSDB_CODE_MND_STREAM_TASK_LOST, mStreamMgmt.hCtx.currentTs);
    return TSDB_CODE_SUCCESS;
  }

  bool             trigger = STREAM_IS_TRIGGER_READER(pAction->flag);
  SStreamCalcScan* scan = NULL;
  if (!trigger) {
    scan = taosArrayGet(pStatus->pCreate->calcScanPlanList, pAction->id.taskIdx);
    if (!scan) {
      mstsError("missing scan plan for Reader TASK:%" PRId64 " taskIdx:%d", pAction->id.taskId, pAction->id.taskIdx);
      TAOS_CHECK_EXIT(TSDB_CODE_STREAM_INTERNAL_ERROR);
    }
  }

  if (!pStatus->deployInput) TAOS_CHECK_EXIT(msmInitDeployInput(pStatus));
  SStmDeployInput* input = pStatus->deployInput;
  (void)atomic_add_fetch_32(&input->refs, 1);
  int64_t             epoch = mStreamMgmt.stat.activeTimes;
  int64_t             version = atomic_load_64(&pStatus->buildVersion);
  SStmStatus          snapshot = {.pCreate = input->create,
                                  .runnerNum = pStatus->runnerNum,
                                  .runnerDeploys = pStatus->runnerDeploys,
                                  .runnerReplica = atomic_load_32(&pStatus->runnerReplica)};
  SStmTaskToDeployExt entry = {0};
  entry.deploy.task.type = pAction->type;
  entry.deploy.task.streamId = streamId;
  entry.deploy.task.taskId = pAction->id.taskId;
  entry.deploy.task.seriousId = pAction->id.seriousId;
  entry.deploy.task.nodeId = pAction->id.nodeId;
  entry.deploy.task.taskIdx = pAction->id.taskIdx;
  msmUnlockActionBuild();
  code = msmBuildReaderDeployInfo(&entry.deploy, scan ? scan->scanPlan : NULL, &snapshot, trigger);
  msmLockActionBuild();

  SStmStatus* live = msmSameBuildEpoch(epoch) ? taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId)) : NULL;
  SStreamObj* current = NULL;
  bool        valid = live && live->deployInput == input && atomic_load_64(&live->buildVersion) == version &&
               !atomic_load_8(&live->stopped) && msmPublishedTaskGeneration(live) == pAction->generation &&
               mndAcquireStream(mnode, input->name, &current) == 0 && current == pStream &&
               !atomic_load_8(&current->userStopped) && !atomic_load_8(&current->userDropped);
  if (valid && msmStopIfGrantExpired(streamId, live)) valid = false;
  if (valid) {
    ppTask = taosHashGet(mStreamMgmt.taskMap, key, sizeof(key));
    valid = ppTask && (*ppTask)->id.seriousId == pAction->id.seriousId;
  }
  if (!valid) {
    code = TSDB_CODE_SUCCESS;
  } else if (!code) {
    code = msmTDAddToVgroupMap(mStreamMgmt.toDeployVgMap, &entry.deploy, streamId);
    entry.deployed = !code;  // The queue owns the message after a successful append.
    *readerQueued = !code;
  }
  if (current) mndReleaseStream(mnode, current);
  mstDestroySStmTaskToDeployExt(&entry);
  mstReleaseDeployInput(input);

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

/*
static int32_t msmReLaunchTriggerTask(SStmGrpCtx* pCtx, SStreamObj* pStream, SStmTaskAction* pAction, SStmStatus* pStatus) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pAction->streamId;
  SStmTaskStatus** ppTask = taosHashGet(mStreamMgmt.taskMap, &pAction->streamId, sizeof(pAction->streamId) + sizeof(pAction->id.taskId));
  if (NULL == ppTask) {
    mstsError("TASK:%" PRId64 " not in taskMap, remain:%d", pAction->id.taskId, taosHashGetSize(mStreamMgmt.taskMap));
    TAOS_CHECK_EXIT(TSDB_CODE_STREAM_INTERNAL_ERROR);
  }
  
  (*ppTask)->id.nodeId = msmAssignTaskSnodeId(pCtx->pMnode, pStream, true);
  if (!GOT_SNODE((*ppTask)->id.nodeId)) {
    mstsError("no avaible snode for deploying trigger task, seriousId: %" PRId64, (*ppTask)->id.seriousId);
    return TSDB_CODE_SUCCESS;
  }
  
  SStmTaskDeploy info = {0};
  info.task.type = pAction->type;
  info.task.streamId = streamId;
  info.task.taskId = pAction->id.taskId;
  info.task.seriousId = (*ppTask)->id.seriousId;
  info.task.nodeId = (*ppTask)->id.nodeId;
  info.task.taskIdx = pAction->id.taskIdx;
  
  TAOS_CHECK_EXIT(msmBuildTriggerDeployInfo(pCtx->pMnode, pStatus, &info, pStream));
  TAOS_CHECK_EXIT(msmTDAddTriggerToSnodeMap(&info, pStream));
  TAOS_CHECK_EXIT(msmSTAddToSnodeMap(pCtx, streamId, NULL, *ppTask, 1, -1));
  
  atomic_add_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}
*/

// Only the identities used to bind plans cross the lock boundary. Runtime status, metrics
// and node indexes remain live and can change while the worker is preparing this repair.
static int32_t msmCopyRunnerRepairInputs(SStmStatus* live, SStmTaskAction* action, SStmStatus* copy,
                                         SArray** readerIds) {
  size_t count = MST_LIST_SIZE(live->calcReaders);
  if (count) {
    *readerIds = taosArrayInit(count, sizeof(SStmReaderBinding));
    if (!*readerIds) return terrno;
    for (SListNode* n = listHead(live->calcReaders); n; n = TD_DLIST_NODE_NEXT(n)) {
      SStmTaskStatus* reader = (SStmTaskStatus*)n->data;
      SStmReaderBinding binding = {reader->id.taskId, reader->id.nodeId, reader->id.taskIdx};
      if (!taosArrayPush(*readerIds, &binding)) return terrno;
    }
  }
  for (int32_t r = 0; r < action->deployNum; ++r) {
    int32_t slot = action->deployId[r];
    size_t  runners = taosArrayGetSize(live->runners[slot]);
    if (!runners) return TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
    copy->runners[slot] = taosArrayInit_s(sizeof(SStmTaskStatus), runners);
    if (!copy->runners[slot]) return terrno;
    for (size_t i = 0; i < runners; ++i) {
      SStmTaskStatus* old = taosArrayGet(live->runners[slot], i);
      SStmTaskStatus* dst = taosArrayGet(copy->runners[slot], i);
      dst->id = old->id;
      dst->type = old->type;
      dst->flags = old->flags;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t msmReLaunchRunnerDeploy(SStmGrpCtx* ctx, SStreamObj* stream, SStmTaskAction* action, SStmStatus* live,
                                       bool* rebuildStream) {
  if (!live->deployInput) TAOS_CHECK_RETURN(msmInitDeployInput(live));
  SStmDeployInput* input = live->deployInput;
  (void)atomic_add_fetch_32(&input->refs, 1);
  SStmStatus           copy = {.pCreate = input->create,
                               .streamName = input->name,
                               .runnerNum = live->runnerNum,
                               .runnerReplica = atomic_load_32(&live->runnerReplica)};
  int64_t              version = atomic_load_64(&live->buildVersion);
  SStmPlacementVersion placement = msmPlacementVersion(ctx->pMnode->pSdb);
  SStmDeployDraft      draft = {.epoch = mStreamMgmt.stat.activeTimes};
  SStmGrpCtx           build = {.pMnode = ctx->pMnode,
                                .currTs = ctx->currTs,
                                .draft = &draft,
                                .triggerTaskId = live->triggerTask->id.taskId,
                                .triggerNodeId = live->triggerTask->id.nodeId};
  SStreamObj           snapshot = {0};
  msmSnapshotStreamRow(stream, &snapshot);
  bool    rebuildAll = false;
  SArray* readerIds = NULL;
  int32_t code = msmCopyRunnerRepairInputs(live, action, &copy, &readerIds);
  live = NULL;
  msmUnlockActionBuild();
  if (!code) {
    draft.tasks = taosArrayInit(4, sizeof(SStmTaskToDeployExt));
    draft.scanMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    if (!draft.tasks || !draft.scanMap) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    } else {
      taosHashSetFreeFp(draft.scanMap, mstDestroyScanAddrList);
      SQueryPlan* plan = NULL;
      code = nodesStringToNode(snapshot.pCreate->calcPlan, (SNode**)&plan);
      if (!code && (!plan || nodeType((SNode*)plan) != QUERY_NODE_PHYSICAL_PLAN))
        code = TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
      if (!code) rebuildAll = LIST_LENGTH(plan->pChildren) > 0;
      // Reader and surviving runner plans embed the addresses of nested subqueries.
      // Rebind them as a unit when any of those runners has to move.
      if (!code && !rebuildAll) code = msmUPPrepareReaderTasks(&build, readerIds, &snapshot, &rebuildAll);
      if (!code && !rebuildAll) {
        code = msmReBuildRunnerTasks(&build, plan, &copy, &snapshot, action);
      } else {
        nodesDestroyNode((SNode*)plan);
      }
    }
  }
  msmLogMetadataRecovery(&draft, action->streamId);
  if (!code && !rebuildAll) code = msmPrepareMetadataSnapshot(&draft);
  taosArrayDestroy(readerIds);
  taosHashCleanup(draft.scanMap);
  msmLockActionBuild();
  int64_t id = action->streamId;
  if (msmSameBuildEpoch(draft.epoch)) live = taosHashGet(mStreamMgmt.streamMap, &id, sizeof(id));
  SStreamObj* current = NULL;
  bool        valid = live && live->deployInput == input && atomic_load_64(&live->buildVersion) == version &&
               !atomic_load_8(&live->stopped) && mndAcquireStream(ctx->pMnode, snapshot.name, &current) == 0 &&
               current == stream && !atomic_load_8(&stream->userStopped) && !atomic_load_8(&stream->userDropped);
  if (valid && msmStopIfGrantExpired(id, live)) valid = false;
  if (valid && !code && !rebuildAll) {
    code = atomic_load_32(&stream->mainSnodeId) == snapshot.mainSnodeId
               ? msmValidateDraftPlacement(ctx->pMnode, &draft, placement, &snapshot)
               : TSDB_CODE_MND_STREAM_NOT_AVAILABLE;
  }
  // A second node loss may already have advanced the serial number of one of these runners.
  for (int32_t i = 0; valid && !code && i < taosArrayGetSize(draft.tasks); ++i) {
    SStmTaskToDeployExt* entry = taosArrayGet(draft.tasks, i);
    SStreamTask*         task = &entry->deploy.task;
    int64_t              key[] = {id, task->taskId};
    SStmTaskStatus**     found = taosHashGet(mStreamMgmt.taskMap, key, sizeof(key));
    if (!found || (*found)->id.seriousId != task->seriousId) valid = false;
  }
  if (valid) {
    int64_t published = taosGetTimestampMs();
    for (int32_t i = 0; !code && i < taosArrayGetSize(draft.tasks); ++i) {
      SStmTaskToDeployExt* entry = taosArrayGet(draft.tasks, i);
      SStreamTask*         task = &entry->deploy.task;
      int64_t              key[] = {id, task->taskId};
      SStmTaskStatus*      status = *(SStmTaskStatus**)taosHashGet(mStreamMgmt.taskMap, key, sizeof(key));
      status->id.nodeId = task->nodeId;
      status->flags = task->flags;
      status->status = STREAM_STATUS_UNDEPLOYED;
      status->lastUpTs = published;
      code = msmSTAddToSnodeMap(ctx, id, NULL, status, 1, task->deployId);
      if (!code) code = msmTDAddRunnerToSnodeMap(&entry->deploy, stream);
      if (code) {
        // Heartbeats must not collect a partial batch while private cleanup is unlocked.
        msmStopStreamByError(id, live, code, ctx->currTs);
        code = TSDB_CODE_SUCCESS;
        break;
      }
      if (!code) {
        entry->deployed = true;
        (void)atomic_add_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);
      }
    }
  } else {
    code = TSDB_CODE_SUCCESS;
  }
  *rebuildStream = valid && rebuildAll && !code;
  if (current) mndReleaseStream(ctx->pMnode, current);
  msmUnlockActionBuild();
#ifdef BUILD_TEST_SEAM
  if (gTdSeam_msmDeployPhase) gTdSeam_msmDeployPhase("runner-cleanup", snapshot.name);
#endif
  taosHashCleanup(draft.checkedMetadataGroups);
  taosHashCleanup(draft.metadataCoverage);
  taosHashCleanup(draft.metadataScanIds);
  taosHashCleanup(draft.metadataCoverageCounts);
  taosHashCleanup(draft.boundMetadataVgroups);
  taosHashCleanup(draft.endpoints);
  taosArrayDestroyEx(draft.tasks, mstDestroySStmTaskToDeployExt);
  for (int32_t i = 0; i < MND_STREAM_RUNNER_DEPLOY_NUM; ++i) taosArrayDestroy(copy.runners[i]);
  msmLockActionBuild();
  live = msmSameBuildEpoch(draft.epoch) ? taosHashGet(mStreamMgmt.streamMap, &id, sizeof(id)) : NULL;
  if (!live || live->deployInput != input || atomic_load_64(&live->buildVersion) != version ||
      atomic_load_8(&live->stopped)) {
    code = TSDB_CODE_SUCCESS;
    *rebuildStream = false;
  }
  mstReleaseDeployInput(input);
  return code;
}

static int32_t msmLaunchTaskDeployAction(SStmGrpCtx* pCtx, SStmTaskAction* pAction, bool* readerQueued) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pAction->streamId;
  int64_t taskId = pAction->id.taskId;
  SStreamObj* pStream = NULL;

  mstsDebug("start to handle stream tasks action, action task type:%s, taskId:0x%" PRIx64 " qRemainNum:%" PRIu64,
            gStreamTaskTypeStr[pAction->type], taskId, atomic_load_64(&mStreamMgmt.actionQ->qRemainNum));

  SStmStatus* pStatus = taosHashGet(mStreamMgmt.streamMap, &pAction->streamId, sizeof(pAction->streamId));
  if (NULL == pStatus) {
    mstsWarn("stream not in streamMap, remain:%d", taosHashGetSize(mStreamMgmt.streamMap));
    return TSDB_CODE_SUCCESS;
  }

  if (pAction->generation != msmPublishedTaskGeneration(pStatus)) return TSDB_CODE_SUCCESS;

  int8_t stopped = atomic_load_8(&pStatus->stopped);
  if (stopped) {
    mstsWarn("stream %s is already stopped %d, ignore task deploy", pStatus->streamName, stopped);
    return TSDB_CODE_SUCCESS;
  }

  if (msmStopIfGrantExpired(streamId, pStatus)) return TSDB_CODE_SUCCESS;

  if (mstTaskDeployNeedsRunnerSnapshot(pStatus, pAction)) {
    return mstPostRunnerSnapshotRedeploy(mStreamMgmt.actionQ, streamId, pStatus);
  }

  code = mndAcquireStream(pCtx->pMnode, pStatus->streamName, &pStream);
  if (TSDB_CODE_MND_STREAM_NOT_EXIST == code) {
    mstsWarn("stream %s no longer exists, ignore task deploy", pStatus->streamName);
    return TSDB_CODE_SUCCESS;
  }

  TAOS_CHECK_EXIT(code);

  int8_t userStopped = atomic_load_8(&pStream->userStopped);
  int8_t userDropped = atomic_load_8(&pStream->userDropped);
  if (userStopped || userDropped) {
    mstsWarn("stream %s is stopped %d or removing %d, ignore task deploy", pStatus->streamName, userStopped, userDropped);
    goto _exit;
  }

  switch (pAction->type) {
    case STREAM_READER_TASK:
      TAOS_CHECK_EXIT(msmReLaunchReaderTask(pCtx->pMnode, pStream, pAction, pStatus, readerQueued));
      break;
/*
    case STREAM_TRIGGER_TASK:
      TAOS_CHECK_EXIT(msmReLaunchTriggerTask(pCtx, pStream, pAction, pStatus));
      break;
*/
    case STREAM_RUNNER_TASK:
      if (pAction->multiRunner) {
        bool rebuildStream = false;
        code = msmReLaunchRunnerDeploy(pCtx, pStream, pAction, pStatus, &rebuildStream);
        // Private build and cleanup can replace the runtime status while unlocked.
        pStatus = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
        TAOS_CHECK_EXIT(code);
        if (rebuildStream) {
          SStmStreamAction action = {.streamId = streamId};
          tstrncpy(action.streamName, pStream->name, sizeof(action.streamName));
          mndReleaseStream(pCtx->pMnode, pStream);
          return msmLaunchStreamDeployAction(pCtx, &action);
        }
      } else {
        mstsError("runner TASK:%" PRId64 " requires relaunch", pAction->id.taskId);
        TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      }
      break;
    default:
      mstsError("TASK:%" PRId64 " invalid task type:%d", pAction->id.taskId, pAction->type);
      TAOS_CHECK_EXIT(TSDB_CODE_STREAM_INTERNAL_ERROR);
      break;
  }

_exit:

  if (pStream) {
    mndReleaseStream(pCtx->pMnode, pStream);
  }

  if (code) {
    msmStopStreamByError(streamId, pStatus, code, pCtx->currTs);
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmTDRemoveStream(int64_t streamId) {
  void* pIter = NULL;
  
  if (atomic_load_32(&mStreamMgmt.toDeployVgTaskNum) > 0) {
    while ((pIter = taosHashIterate(mStreamMgmt.toDeployVgMap, pIter))) {
      SStmVgTasksToDeploy* pVg = (SStmVgTasksToDeploy*)pIter;
      int32_t taskNum = taosArrayGetSize(pVg->taskList);
      if (atomic_load_32(&pVg->deployed) == taskNum) {
        continue;
      }
      
      for (int32_t i = 0; i < taskNum; ++i) {
        SStmTaskToDeployExt* pExt = taosArrayGet(pVg->taskList, i);
        if (pExt->deploy.task.streamId == streamId && !pExt->deployed) {
          pExt->deployed = true;
          (void)atomic_add_fetch_32(&pVg->deployed, 1);
        }
      }
    }
  }

  if (atomic_load_32(&mStreamMgmt.toDeploySnodeTaskNum) > 0) {
    while ((pIter = taosHashIterate(mStreamMgmt.toDeploySnodeMap, pIter))) {
      SStmSnodeTasksDeploy* pSnode = (SStmSnodeTasksDeploy*)pIter;
      int32_t taskNum = taosArrayGetSize(pSnode->triggerList);
      if (atomic_load_32(&pSnode->triggerDeployed) != taskNum) {
        for (int32_t i = 0; i < taskNum; ++i) {
          SStmTaskToDeployExt* pExt = taosArrayGet(pSnode->triggerList, i);
          if (pExt->deploy.task.streamId == streamId && !pExt->deployed) {
            pExt->deployed = true;
            (void)atomic_add_fetch_32(&pSnode->triggerDeployed, 1);
          }
        }
      }

      taskNum = taosArrayGetSize(pSnode->runnerList);
      if (atomic_load_32(&pSnode->runnerDeployed) != taskNum) {
        for (int32_t i = 0; i < taskNum; ++i) {
          SStmTaskToDeployExt* pExt = taosArrayGet(pSnode->runnerList, i);
          if (pExt->deploy.task.streamId == streamId && !pExt->deployed) {
            pExt->deployed = true;
            (void)atomic_add_fetch_32(&pSnode->runnerDeployed, 1);
          }
        }
      }

      taskNum = taosArrayGetSize(pSnode->calcReaderList);
      if (atomic_load_32(&pSnode->calcReaderDeployed) != taskNum) {
        for (int32_t i = 0; i < taskNum; ++i) {
          SStmTaskToDeployExt* pExt = taosArrayGet(pSnode->calcReaderList, i);
          if (pExt->deploy.task.streamId == streamId && !pExt->deployed) {
            pExt->deployed = true;
            (void)atomic_add_fetch_32(&pSnode->calcReaderDeployed, 1);
          }
        }
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t msmRemoveStreamFromMaps(SMnode* pMnode, int64_t streamId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  mstsInfo("start to remove stream from maps, current stream num:%d", taosHashGetSize(mStreamMgmt.streamMap));

  TAOS_CHECK_EXIT(msmSTRemoveStream(streamId, true));

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    mstsInfo("end remove stream from maps, current stream num:%d", taosHashGetSize(mStreamMgmt.streamMap));
  }

  return code;
}

void msmInvalidateStreamDeployment(int64_t streamId, char* streamName) {
  if (mStreamMgmt.deployingStreamId == streamId) mStreamMgmt.deployingStreamId = 0;
  SStmStatus* status = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (status) {
    (void)msmAdvanceBuildVersion(status);
    atomic_store_8(&status->stopped, 2);
#ifdef TD_ENTERPRISE
    msmCancelSchemaBatch(status->pendingSchema);
    status->pendingSchema = NULL;
#endif
    mstsInfo("set stream %s stopped by user", streamName);
  }
}

void msmUndeployStream(SMnode* pMnode, int64_t streamId, char* streamName) {
  // Full publication takes runtimeLock exclusively. Heartbeat claims and partial
  // publication take actionQLock, which the external schema fetch releases.
  (void)mstWaitLock(&mStreamMgmt.runtimeLock, true);
  (void)mstWaitLock(&mStreamMgmt.actionQLock, false);
  if (atomic_load_8(&mStreamMgmt.active) && mStreamMgmt.state == MND_STM_STATE_NORMAL) {
    msmInvalidateStreamDeployment(streamId, streamName);
  }
  taosWUnLockLatch(&mStreamMgmt.actionQLock);
  taosRUnLockLatch(&mStreamMgmt.runtimeLock);
}

int32_t msmRecalcStream(SMnode* pMnode, SStreamObj* pStreamObj, const STimeWindow* pRange, const SRpcMsg* pReq) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStreamObj->pCreate->streamId;

  int8_t active = atomic_load_8(&mStreamMgmt.active), state = atomic_load_8(&mStreamMgmt.state);
  if (0 == active || MND_STM_STATE_NORMAL != state) {
    mstsError("stream mgmt not available since active:%d state:%d", active, state);
    return TSDB_CODE_MND_STREAM_NOT_AVAILABLE;
  }

  SStmStatus* pStream = (SStmStatus*)taosHashAcquire(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStream || !STREAM_IS_RUNNING(pStream)) {
    code = TSDB_CODE_MND_STREAM_NOT_RUNNING;
    mstsInfo("stream still not in streamMap, streamRemains:%d", taosHashGetSize(mStreamMgmt.streamMap));
    goto _exit;
  }

  TAOS_CHECK_EXIT(mndStreamRecalcAccept(pMnode, pStreamObj, pStream, pRange, pReq));

_exit:

  taosHashRelease(mStreamMgmt.streamMap, pStream);

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static void msmHandleStreamActions(SStmGrpCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmQNode* pQNode = NULL;

  int64_t epoch = mStreamMgmt.stat.activeTimes;
  // After the first deferral, service its predecessors and leave retries to the next heartbeat.
  pCtx->actionsBeforeDeferred = -1;
  while (atomic_load_8(&mStreamMgmt.active) && !atomic_load_8(&pCtx->pMnode->streamDeployStopping) &&
         mStreamMgmt.actionQ && mStreamMgmt.stat.activeTimes == epoch && pCtx->actionsBeforeDeferred != 0 &&
         mndStreamActionDequeue(mStreamMgmt.actionQ, &pQNode)) {
    if (pCtx->actionsBeforeDeferred > 0) --pCtx->actionsBeforeDeferred;
    switch (pQNode->type) {
      case STREAM_ACT_DEPLOY_IF_MISSING:
      case STREAM_ACT_DEPLOY:
        if (pQNode->streamAct) {
          mstDebug("start to handle stream deploy action");
          SStmStreamAction action = pQNode->action.stream;
          if (pQNode->type == STREAM_ACT_DEPLOY_IF_MISSING &&
              taosHashGet(mStreamMgmt.streamMap, &action.streamId, sizeof(action.streamId)))
            break;
          TAOS_CHECK_EXIT(msmLaunchStreamDeployAction(pCtx, &action));
        } else {
          mstDebug("start to handle task deploy action");
          SStmTaskAction action = pQNode->action.task;
          bool           readerQueued = false;
          code = msmLaunchTaskDeployAction(pCtx, &action, &readerQueued);
          if (action.type == STREAM_READER_TASK && !readerQueued && msmSameBuildEpoch(epoch)) {
            msmReleaseReaderRepair(action.streamId, action.id.taskId, action.id.seriousId, false);
          }
          TAOS_CHECK_EXIT(code);
        }
        break;
      default:
        break;
    }
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
}

void mndStreamLifecycleTransStopped(SMnode* pMnode, void* param, int32_t paramLen) {
  if (pMnode == NULL || param == NULL || paramLen != sizeof(SStreamLifecycleTransParam)) return;

  const SStreamLifecycleTransParam* pParam = param;
  if (pParam->action == MND_STREAM_LIFECYCLE_DROP) {
    SStreamObj* pStream = NULL;
    if (mndAcquireStreamById(pMnode, pParam->streamId, &pStream) == TSDB_CODE_SUCCESS && pStream != NULL) {
      mndReleaseStream(pMnode, pStream);
      return;
    }
  } else {
    SStreamObj* pStream = NULL;
    if (mndAcquireStreamById(pMnode, pParam->streamId, &pStream) != TSDB_CODE_SUCCESS || pStream == NULL) return;
    bool committed = atomic_load_8(&pStream->userStopped) == pParam->expectedUserStopped;
    mndReleaseStream(pMnode, pStream);
    if (!committed) return;
  }

  if (pParam->action == MND_STREAM_LIFECYCLE_START) {
    taosRLockLatch(&mStreamMgmt.runtimeLock);
    if (atomic_load_8(&mStreamMgmt.active) != 0 && mStreamMgmt.actionQ != NULL) {
      mstPostStreamAction(mStreamMgmt.actionQ, pParam->streamId, (char*)pParam->streamName, NULL, true,
                          STREAM_ACT_DEPLOY);
    }
    taosRUnLockLatch(&mStreamMgmt.runtimeLock);
  } else if (pParam->action == MND_STREAM_LIFECYCLE_STOP || pParam->action == MND_STREAM_LIFECYCLE_DROP) {
    msmUndeployStream(pMnode, pParam->streamId, (char*)pParam->streamName);
  }
}

void msmStopAllStreamsByGrant(int32_t errCode) {
  // A first deployment may still be copying its plan and have no streamMap entry to invalidate.
  mStreamMgmt.deployingStreamId = 0;
  void* it = NULL;
  while ((it = taosHashIterate(mStreamMgmt.streamMap, it))) {
    int64_t id = *(int64_t*)taosHashGetKey(it, NULL);
    msmStopStreamByGrant(id, it, errCode);
  }
}

int32_t msmHandleGrantExpired(SMnode *pMnode, int32_t errCode) {
  mstWarn("stream grant expired");

  if (0 == atomic_load_8(&mStreamMgmt.active)) {
    mstWarn("mnode stream is NOT active, ignore handling");
    return errCode;
  }

  (void)mstWaitLock(&mStreamMgmt.runtimeLock, true);

  (void)mstWaitLock(&mStreamMgmt.actionQLock, false);
  msmStopAllStreamsByGrant(errCode);
  taosWUnLockLatch(&mStreamMgmt.actionQLock);

  taosRUnLockLatch(&mStreamMgmt.runtimeLock);
  
  return errCode;
}

static int32_t msmInitStreamDeploy(SStmStreamDeploy* pStream, SStmTaskDeploy* pDeploy) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pDeploy->task.streamId;
  
  switch (pDeploy->task.type) {
    case STREAM_READER_TASK:
      if (NULL == pStream->readerTasks) {
        pStream->streamId = streamId;
        pStream->readerTasks = taosArrayInit(20, sizeof(SStmTaskDeploy));
        TSDB_CHECK_NULL(pStream->readerTasks, code, lino, _exit, terrno);
      }
      
      TSDB_CHECK_NULL(taosArrayPush(pStream->readerTasks, pDeploy), code, lino, _exit, terrno);
      break;
    case STREAM_TRIGGER_TASK:
      if (pStream->triggerTask != NULL) TAOS_CHECK_EXIT(TSDB_CODE_DUP_KEY);
      pStream->streamId = streamId;
      pStream->triggerTask = taosMemoryMalloc(sizeof(SStmTaskDeploy));
      TSDB_CHECK_NULL(pStream->triggerTask, code, lino, _exit, terrno);
      memcpy(pStream->triggerTask, pDeploy, sizeof(SStmTaskDeploy));
      break;
    case STREAM_RUNNER_TASK:
      if (NULL == pStream->runnerTasks) {
        pStream->streamId = streamId;
        pStream->runnerTasks = taosArrayInit(20, sizeof(SStmTaskDeploy));
        TSDB_CHECK_NULL(pStream->runnerTasks, code, lino, _exit, terrno);
      }      
      TSDB_CHECK_NULL(taosArrayPush(pStream->runnerTasks, pDeploy), code, lino, _exit, terrno);
      break;
    default:
      break;
  }

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmGrpAddDeployTask(SHashObj* pHash, SStmTaskDeploy* pDeploy) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pDeploy->task.streamId;
  SStreamTask*      pTask = &pDeploy->task;
  SStmStreamDeploy* pStream = NULL;
   
  while (true) {
    pStream = taosHashAcquire(pHash, &streamId, sizeof(streamId));
    if (NULL == pStream) {
      SStmStreamDeploy streamDeploy = {0};
      code = msmInitStreamDeploy(&streamDeploy, pDeploy);
      if (!code) code = taosHashPut(pHash, &streamId, sizeof(streamId), &streamDeploy, sizeof(streamDeploy));
      if (TSDB_CODE_SUCCESS == code) {
        if (pDeploy->task.type == STREAM_TRIGGER_TASK) {
          pDeploy->msg.trigger.pWindowPlan = NULL;
          pDeploy->msg.trigger.pTimeAlign = NULL;
        }
        goto _exit;
      }

      // Until insertion succeeds the queued entry still owns all nested task data.
      taosArrayDestroy(streamDeploy.readerTasks);
      taosArrayDestroy(streamDeploy.runnerTasks);
      taosMemoryFree(streamDeploy.triggerTask);
      if (code != TSDB_CODE_DUP_KEY) goto _exit;
      continue;
    }

    TAOS_CHECK_EXIT(msmInitStreamDeploy(pStream, pDeploy));
    if (pDeploy->task.type == STREAM_TRIGGER_TASK) {
      pDeploy->msg.trigger.pWindowPlan = NULL;
      pDeploy->msg.trigger.pTimeAlign = NULL;
    }

    break;
  }
  
_exit:

  taosHashRelease(pHash, pStream);

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    msttDebug("task added to GRP deployMap, taskIdx:%d", pTask->taskIdx);
  }

  return code;
}

static int32_t msmPinDeployInput(SArray** inputs, SStmStatus* status) {
  if (!status || !status->deployInput) return TSDB_CODE_MND_STREAM_NOT_AVAILABLE;
  if (!*inputs) {
    *inputs = taosArrayInit(1, sizeof(SStmDeployInput*));
    if (!*inputs) return terrno;
  }
  if (!taosArrayPush(*inputs, &status->deployInput)) return terrno;
  (void)atomic_add_fetch_32(&status->deployInput->refs, 1);
  return TSDB_CODE_SUCCESS;
}

#ifdef TD_ENTERPRISE
// Changing lifetime invalidates every old installation in O(1). A delayed report
// from a previous process cannot echo a challenge issued after that transition.
static int64_t msmConfirmReaderRuntime(SStmGrpCtx* ctx) {
  const SStreamHbMsg* hb = ctx->pReq;
  if (!GOT_SNODE(hb->snodeId)) return 0;
  SStmSnodeStatus* node = taosHashGet(mStreamMgmt.snodeMap, &hb->snodeId, sizeof(hb->snodeId));
  if (!node) return 0;
  taosWLockLatch(&node->readerRuntimeLock);
  // A delayed heartbeat from a pre-upgrade peer has no challenge. Fence the
  // cached observations immediately, and allow a downgrade after one isolation interval.
  if (!hb->readerRuntime.id && node->readerRuntime.id &&
      ctx->currTs - node->lastReaderRuntimeTs < MST_ISOLATION_DURATION) {
    ctx->deferReaderDeploy = true;
    atomic_store_64(&node->readerRuntime.challenge, 0);
    taosWUnLockLatch(&node->readerRuntimeLock);
    return 0;
  }
  if (hb->readerRuntime.id && node->readerRuntime.id && hb->readerRuntime.id != node->readerRuntime.id &&
      hb->readerRuntime.challenge) {
    // An echo from a previous lifetime cannot displace the current one. A live sender
    // carrying another mnode's challenge will introduce itself again with a zero echo.
    ctx->pRsp->readerRuntime = (SStreamReaderRuntime){.id = hb->readerRuntime.id};
    ctx->deferReaderDeploy = true;
    taosWUnLockLatch(&node->readerRuntimeLock);
    return 0;
  }
  if (hb->readerRuntime.id) node->lastReaderRuntimeTs = ctx->currTs;
  // Legacy senders emit an empty group-zero heartbeat after a cycle with no tasks.
  bool lostLegacyReaders =
      !hb->readerRuntime.id && !hb->readerDeployEpoch && !hb->streamGId && !taosArrayGetSize(hb->pStreamStatus);
  if (lostLegacyReaders || !node->readerRuntime.challenge || node->readerRuntime.id != hb->readerRuntime.id ||
      (!hb->readerRuntime.id && node->legacyReaderEpoch != hb->readerDeployEpoch)) {
    node->readerRuntime.id = hb->readerRuntime.id;
    node->legacyReaderEpoch = hb->readerDeployEpoch;
    atomic_store_64(&node->readerRuntime.challenge, tGenIdPI64());
  }
  int64_t confirmation = atomic_load_64(&node->readerRuntime.challenge);
  if (hb->readerRuntime.id) {
    ctx->pRsp->readerRuntime = (SStreamReaderRuntime){hb->readerRuntime.id, confirmation};
    if (hb->readerRuntime.challenge != confirmation) confirmation = 0;
  }
  taosWUnLockLatch(&node->readerRuntimeLock);
  return confirmation;
}

// A negative confirmation fences this incarnation even if an older healthy report arrives later.
#define MST_READER_INSTALL_FAILED INT64_C(-1)

// A different heartbeat group can change the snode lifetime after this packet's challenge check.
// Fence or repeat removal only while the runtime that reported the failure is still current.
static bool msmAcceptReaderRemoval(const SStmGrpCtx* ctx, SStmTaskStatus* reader, bool failed) {
  if (!ctx->readerConfirmation) return false;
  SStmSnodeStatus* node = taosHashGet(mStreamMgmt.snodeMap, &reader->id.nodeId, sizeof(reader->id.nodeId));
  if (!node) return false;
  taosRLockLatch(&node->readerRuntimeLock);
  bool current = ctx->readerConfirmation == atomic_load_64(&node->readerRuntime.challenge);
  if (current && failed) atomic_store_64(&reader->remoteSchemaConfirmation, MST_READER_INSTALL_FAILED);
  taosRUnLockLatch(&node->readerRuntimeLock);
  return current;
}

static const bool* msmReportedSchemaReader(const SStmGrpCtx* ctx, int64_t streamId, const SStmCachedReader* target) {
  if (!ctx->reportedSchemaReaders || ctx->pReq->snodeId != target->nodeId) return NULL;
  int64_t key[] = {streamId, target->taskId, target->seriousId};
  return taosHashGet(ctx->reportedSchemaReaders, key, sizeof(key));
}

static bool msmReaderConfirmedOnNode(const SStmGrpCtx* ctx, int64_t streamId, SStmTaskStatus* reader) {
  SStmCachedReader target = {reader->id.taskId, reader->id.seriousId, reader->id.nodeId};
  const bool*      report = msmReportedSchemaReader(ctx, streamId, &target);
  int64_t          confirmed = atomic_load_64(&reader->remoteSchemaConfirmation);
  if (confirmed == MST_READER_INSTALL_FAILED) return false;
  if (report) confirmed = *report ? ctx->readerConfirmation : 0;
  if (!confirmed) return false;
  SStmSnodeStatus* node = taosHashGet(mStreamMgmt.snodeMap, &reader->id.nodeId, sizeof(reader->id.nodeId));
  return node && confirmed == atomic_load_64(&node->readerRuntime.challenge);
}

static SStmTaskStatus* msmFindSchemaReader(int64_t streamId, const SStmCachedReader* target) {
  int64_t          key[] = {streamId, target->taskId};
  SStmTaskStatus** live = taosHashGet(mStreamMgmt.taskMap, key, sizeof(key));
  if (!live || (*live)->id.seriousId != target->seriousId || (*live)->id.nodeId != target->nodeId) return NULL;
  return *live;
}

static bool msmRemoteSchemaReaderInstalled(const SStmGrpCtx* ctx, int64_t streamId, const SStmCachedReader* target) {
  SStmTaskStatus* reader = msmFindSchemaReader(streamId, target);
  return reader && msmReaderConfirmedOnNode(ctx, streamId, reader);
}

static int32_t msmIndexReportedSchemaReaders(SStmGrpCtx* ctx) {
  const SStreamHbMsg* hb = ctx->pReq;
  size_t              count = taosArrayGetSize(hb->pStreamStatus);
  if (!count || !GOT_SNODE(hb->snodeId) || !ctx->pMnode) return 0;
  if (!atomic_load_32(&ctx->pMnode->streamSchemaBatchCount) || !msmSchemaNodeNeedsReports(ctx->pMnode, hb->snodeId)) {
    // Without a target queue or a local implicit acknowledgment, only a mixed request
    // can consult this packet's Reader observations ahead of the normal status pass.
    size_t i = 0;
    for (; i < taosArrayGetSize(hb->pStreamReq); ++i) {
      int32_t index = *(int32_t*)taosArrayGet(hb->pStreamReq, i);
      if (index < 0 || (size_t)index >= count) continue;
      const SStmTaskStatusMsg* task = taosArrayGet(hb->pStreamStatus, index);
      if (task->pMgmtReq && task->pMgmtReq->type == STREAM_MGMT_REQ_TRIGGER_MIXED_ORIGTBL_READER) break;
    }
    if (i == taosArrayGetSize(hb->pStreamReq)) {
      ctx->unindexedReaderPoll = true;
      return 0;
    }
    if (count == 1) return 0;  // The only report is the Trigger requesting the schema.
  }

  for (size_t i = 0; i < count; ++i) {
    const SStmTaskStatusMsg* reader = taosArrayGet(hb->pStreamStatus, i);
    if (reader->type != STREAM_READER_TASK || reader->nodeId != hb->snodeId || !STREAM_IS_REF_EXT_SOURCE(reader->flags))
      continue;
    if (!ctx->reportedSchemaReaders) {
      ctx->reportedSchemaReaders =
          taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
      if (!ctx->reportedSchemaReaders) return terrno;
    }
    int64_t key[] = {reader->streamId, reader->taskId, reader->seriousId};
    bool    healthy = reader->status == STREAM_STATUS_INIT || reader->status == STREAM_STATUS_RUNNING;
    // An unhealthy report of the same incarnation can schedule removal in this response.
    if (healthy && taosHashGet(ctx->reportedSchemaReaders, key, sizeof(key))) continue;
    TAOS_CHECK_RETURN(taosHashPut(ctx->reportedSchemaReaders, key, sizeof(key), &healthy, sizeof(healthy)));
  }
  return 0;
}
// A rejected reply has already exposed its successful sibling addresses to the Trigger for cleanup.
static bool msmUnclaimedReaderExpired(const SStmExtSchema* schema, int64_t now) {
  const SStmSchemaBatch* batch = schema->batch;
  return batch->published && !batch->rejected && !schema->firstDispatched && !schema->failedSeriousId &&
         now - batch->firstReplyTs >= MST_ISOLATION_DURATION;
}

// The response holds a batch reference. Keep only the incarnation and deadline across unlocked encoding.
static void msmReserveSchemaDispatch(SStmSchemaReply* pin, SStmExtSchema* schema, SStmTaskStatus* first) {
  schema->nextDispatch = pin->dispatches;
  pin->dispatches = schema;
  schema->pendingDispatchId = schema->requiredReader.seriousId;
  if (first) schema->previousRetryTs = atomic_exchange_64(&first->readerRepairRetryTs, INT64_MAX);
}

static void msmFinishSchemaDispatch(SStmExtSchema* schema, bool sent, bool legacy, int64_t deadline) {
  if (schema->requiredReader.seriousId == schema->pendingDispatchId && msmSameBuildEpoch(schema->batch->epoch)) {
    SStmTaskStatus* reader = msmFindSchemaReader(schema->batch->streamId, &schema->requiredReader);
    if (reader && !schema->firstDispatched) {
      int64_t next = schema->previousRetryTs;
      if (sent) next = next == INT64_MAX ? deadline : TMAX(deadline, next);
      (void)atomic_val_compare_exchange_64(&reader->readerRepairRetryTs, INT64_MAX, next);
      schema->firstDispatched = sent;
    }
    if (sent && legacy) atomic_store_8(&schema->legacyDispatched, 1);
  }
  schema->pendingDispatchId = 0;
}

static void msmFinishSchemaResponse(SStmGrpCtx* ctx, bool sent) {
  int64_t deadline = 0;
  for (size_t i = 0; i < taosArrayGetSize(ctx->schemaReplies); ++i) {
    SStmSchemaReply* pin = taosArrayGet(ctx->schemaReplies, i);
    if (!pin->dispatches && !pin->localDispatch) continue;
    if (!deadline) {
      msmLockActionBuild();
      deadline = taosGetTimestampMs() + MST_ISOLATION_DURATION;
    }
    while (pin->dispatches) {
      SStmExtSchema* schema = pin->dispatches;
      pin->dispatches = schema->nextDispatch;
      schema->nextDispatch = NULL;
      msmFinishSchemaDispatch(schema, sent, !pin->localDispatch && !ctx->pReq->readerDeployEpoch, deadline);
    }
    if (pin->localDispatch) {
      SStmSchemaBatch* batch = pin->batch;
      batch->localDispatchPending = false;
      if (sent) {
        batch->sentLocalActions = true;
        if (!batch->guardedDeploy) batch->localConfirmBy = deadline;
      }
    }
  }
  if (deadline) msmUnlockActionBuild();
}
#endif

static int32_t msmGrpAddDeployTasks(SStmGrpCtx* ctx, SArray* pTasks, int32_t* deployed, SArray** inputs) {
  SHashObj* pHash = ctx->deployStm;
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t taskNum = taosArrayGetSize(pTasks);

  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskToDeployExt* pExt = taosArrayGet(pTasks, i);
    if (pExt->deployed) {
      continue;
    }

    int64_t     streamId = pExt->deploy.task.streamId;
    SStmStatus* status = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
    bool        discard = status && atomic_load_8(&status->stopped);
    SStmTaskStatus* firstDispatch = NULL;
#ifdef TD_ENTERPRISE
    SStreamTaskUndeploy cleanup = {0};
    SStreamTaskUndeploy failed = {0};
    SStmSchemaReply*    pin = NULL;
    if (!discard && pExt->remoteSchema) {
      SStmSchemaBatch* batch = pExt->remoteSchema->batch;
      if (batch->remote) {
        // An old poll from before an upgrade cannot consume the target's one-shot legacy deployment.
        if (ctx->deferReaderDeploy || ctx->unindexedReaderPoll) continue;
        if (pExt->remoteSchema->failedSeriousId && !ctx->pReq->readerDeployEpoch) continue;
        const SStreamTask* target = &pExt->deploy.task;
        int64_t            key[] = {streamId, target->taskId};
        SStmTaskStatus**   live = taosHashGet(mStreamMgmt.taskMap, key, sizeof(key));
        discard = !live || (*live)->id.seriousId != target->seriousId ||
                  atomic_load_64(&(*live)->remoteSchemaConfirmation) == MST_READER_INSTALL_FAILED ||
                  msmReaderConfirmedOnNode(ctx, streamId, *live);
        if (!discard && !pExt->remoteSchema->firstDispatched) {
          if (msmUnclaimedReaderExpired(pExt->remoteSchema, taosGetTimestampMs())) {
            discard = true;
          } else {
            firstDispatch = *live;
          }
        }
      }
      if (!discard && (!pExt->ownsExtSpec || firstDispatch || !ctx->pReq->readerDeployEpoch)) {
        pin = msmPinSchemaReply(ctx, batch);
        if (!pin) return terrno;
      }
      bool guarded = batch->remote && ctx->pReq->readerDeployEpoch != 0;
      if (guarded)
        pExt->deploy.task.flags |= STREAM_FLAG_REMOTE_READER_DEPLOY;
      else
        pExt->deploy.task.flags &= ~STREAM_FLAG_REMOTE_READER_DEPLOY;
      cleanup = pExt->remoteSchema->replayUndeploy;
      if (guarded) cleanup.task.flags |= STREAM_FLAG_REMOTE_READER_DEPLOY;
      if (pExt->remoteSchema->failedSeriousId) {
        failed.task = pExt->deploy.task;
        failed.task.seriousId = pExt->remoteSchema->failedSeriousId;
        failed.undeployMsg.doCleanup = true;
      }
    }
#endif
    if (discard) {
      mstDestroySStmTaskToDeployExt(pExt);
      pExt->deployed = true;
      (void)atomic_add_fetch_32(deployed, 1);
      continue;
    }

    if (inputs && !taosHashGet(pHash, &streamId, sizeof(streamId))) {
      int32_t pinCode = msmPinDeployInput(inputs, status);
      if (pinCode && taosHashGetSize(pHash) > 0) {
        // Deliver earlier claims and leave this task queued for the next heartbeat.
        mstWarn("defer deployment for stream 0x%" PRIx64 ", pin failed:%s", streamId, tstrerror(pinCode));
        continue;
      }
      TAOS_CHECK_EXIT(pinCode);
    }
#ifdef TD_ENTERPRISE
    // Reserve the paired cleanup only after this deployment has pinned its input.
    SArray** actions = &ctx->pRsp->undeploy.taskList;
    int32_t  cleanups = (cleanup.task.taskId != 0) + (failed.task.taskId != 0);
    if (cleanups) {
      if (!*actions) *actions = taosArrayInit(cleanups, sizeof(cleanup));
      if (!*actions) TAOS_CHECK_EXIT(terrno);
      SStreamTaskUndeploy* slots = taosArrayReserve(*actions, cleanups);
      if (!slots) TAOS_CHECK_EXIT(terrno);
      if (cleanup.task.taskId) *slots++ = cleanup;
      if (failed.task.taskId) *slots = failed;
    }
#endif
    code = msmGrpAddDeployTask(pHash, &pExt->deploy);
#ifdef TD_ENTERPRISE
    if (code && cleanups) taosArrayPopTailBatch(*actions, cleanups);
#endif
    TAOS_CHECK_EXIT(code);
#ifdef TD_ENTERPRISE
    if (pExt->remoteSchema && (firstDispatch || !ctx->pReq->readerDeployEpoch)) {
      msmReserveSchemaDispatch(pin, pExt->remoteSchema, firstDispatch);
    }
#endif
    SStreamTask* task = &pExt->deploy.task;
    if (task->type == STREAM_READER_TASK && !firstDispatch) {
      msmReleaseReaderRepair(streamId, task->taskId, task->seriousId, true);
    }
    if (pExt->ownsExtSpec && pExt->deploy.task.type == STREAM_READER_TASK) {
      mstDebug("transfer dynamic external reader spec to heartbeat deployment");
      pExt->deploy.msg.reader.pExtSpec = NULL;
      pExt->deploy.msg.reader.ownsExtSpec = false;
      pExt->ownsExtSpec = false;
    }
    pExt->deployed = true;

    (void)atomic_add_fetch_32(deployed, 1);
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmGrpAddDeployVgTasks(SStmGrpCtx* pCtx, SArray** inputs) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t vgNum = taosArrayGetSize(pCtx->pReq->pVgLeaders);
  SStmVgTasksToDeploy* pVg = NULL;
  //int32_t tidx = streamGetThreadIdx(mStreamMgmt.threadNum, pCtx->pReq->streamGId);

  mstDebug("start to add stream vgroup tasks deploy");
  
  for (int32_t i = 0; i < vgNum; ++i) {
    int32_t* vgId = taosArrayGet(pCtx->pReq->pVgLeaders, i);

    msmUpdateVgroupUpTs(pCtx, *vgId);

    pVg = taosHashAcquire(mStreamMgmt.toDeployVgMap, vgId, sizeof(*vgId));
    if (NULL == pVg) {
      continue;
    }

    if (taosWTryLockLatch(&pVg->lock)) {
      taosHashRelease(mStreamMgmt.toDeployVgMap, pVg);
      pVg = NULL;
      continue;
    }

    if (atomic_load_32(&pVg->deployed) == taosArrayGetSize(pVg->taskList)) {
      taosWUnLockLatch(&pVg->lock);
      taosHashRelease(mStreamMgmt.toDeployVgMap, pVg);
      pVg = NULL;
      continue;
    }

    TAOS_CHECK_EXIT(msmGrpAddDeployTasks(pCtx, pVg->taskList, &pVg->deployed, inputs));
    taosWUnLockLatch(&pVg->lock);
    taosHashRelease(mStreamMgmt.toDeployVgMap, pVg);
    pVg = NULL;
  }

_exit:

  if (code) {
    if (pVg) {
      taosWUnLockLatch(&pVg->lock);
      taosHashRelease(mStreamMgmt.toDeployVgMap, pVg);
    }

    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmGrpAddDeploySnodeTasks(SStmGrpCtx* pCtx, SArray** inputs) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmSnodeTasksDeploy* pSnode = NULL;
  SStreamHbMsg* pReq = pCtx->pReq;

  mstDebug("start to add stream snode tasks deploy, snodeId:%d", pReq->snodeId);

  pSnode = taosHashAcquire(mStreamMgmt.toDeploySnodeMap, &pReq->snodeId, sizeof(pReq->snodeId));
  if (NULL == pSnode) {
    mstDebug("no toDeploySnodeMap entry for snodeId:%d, nothing to deploy this round", pReq->snodeId);
    return TSDB_CODE_SUCCESS;
  }

  (void)mstWaitLock(&pSnode->lock, false);

  mstDebug("snodeId:%d trigger deployed:%d/%d, runner deployed:%d/%d, calcReader deployed:%d/%d", pReq->snodeId,
           atomic_load_32(&pSnode->triggerDeployed), (int32_t)taosArrayGetSize(pSnode->triggerList),
           atomic_load_32(&pSnode->runnerDeployed), (int32_t)taosArrayGetSize(pSnode->runnerList),
           atomic_load_32(&pSnode->calcReaderDeployed), (int32_t)taosArrayGetSize(pSnode->calcReaderList));

  if (atomic_load_32(&pSnode->triggerDeployed) < taosArrayGetSize(pSnode->triggerList)) {
    TAOS_CHECK_EXIT(msmGrpAddDeployTasks(pCtx, pSnode->triggerList, &pSnode->triggerDeployed, inputs));
  }

  if (atomic_load_32(&pSnode->runnerDeployed) < taosArrayGetSize(pSnode->runnerList)) {
    TAOS_CHECK_EXIT(msmGrpAddDeployTasks(pCtx, pSnode->runnerList, &pSnode->runnerDeployed, inputs));
  }

  // EXT-source calc readers also run on snode; deploy them via calcReaderList.
  if (pSnode->calcReaderList != NULL &&
      atomic_load_32(&pSnode->calcReaderDeployed) < taosArrayGetSize(pSnode->calcReaderList)) {
    TAOS_CHECK_EXIT(msmGrpAddDeployTasks(pCtx, pSnode->calcReaderList, &pSnode->calcReaderDeployed, inputs));
  }

  taosWUnLockLatch(&pSnode->lock);

_exit:

  if (code) {
    if (pSnode) {
      taosWUnLockLatch(&pSnode->lock);
    }

    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  taosHashRelease(mStreamMgmt.toDeploySnodeMap, pSnode);
  return code;
}

int32_t msmUpdateStreamLastActTs(int64_t streamId, int64_t currTs) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmStatus* pStatus = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStatus) {
    mstsWarn("stream no longer exists in streamMap, mapSize:%d", taosHashGetSize(mStreamMgmt.streamMap));
    return TSDB_CODE_SUCCESS;
  }
  
  pStatus->lastActionTs = currTs;

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmRspAddStreamsDeploy(SStmGrpCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t streamNum = taosHashGetSize(pCtx->deployStm);
  void* pIter = NULL;

  mstDebug("start to add group %d deploy streams, streamNum:%d", pCtx->pReq->streamGId, taosHashGetSize(pCtx->deployStm));

  if (!pCtx->pRsp->deploy.streamList)
    pCtx->pRsp->deploy.streamList = taosArrayInit(streamNum, sizeof(SStmStreamDeploy));
  TSDB_CHECK_NULL(pCtx->pRsp->deploy.streamList, code, lino, _exit, terrno);

  while (1) {
    pIter = taosHashIterate(pCtx->deployStm, pIter);
    if (pIter == NULL) {
      break;
    }
    
    SStmStreamDeploy *pDeploy = (SStmStreamDeploy *)pIter;
    TSDB_CHECK_NULL(taosArrayPush(pCtx->pRsp->deploy.streamList, pDeploy), code, lino, _exit, terrno);

    int64_t streamId = pDeploy->streamId;
    mstsDebug("stream DEPLOY added to dnode %d hb rsp, readerTasks:%zu, triggerTask:%d, runnerTasks:%zu", 
        pCtx->pReq->dnodeId, taosArrayGetSize(pDeploy->readerTasks), pDeploy->triggerTask ? 1 : 0, taosArrayGetSize(pDeploy->runnerTasks));

    mstClearSStmStreamDeploy(pDeploy);
    
    TAOS_CHECK_EXIT(msmUpdateStreamLastActTs(pDeploy->streamId, pCtx->currTs));
  }
  
_exit:

  if (pIter) {
    taosHashCancelIterate(pCtx->deployStm, pIter);
  }

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}

void msmCleanDeployedVgTasks(SArray* pVgLeaders) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t vgNum = taosArrayGetSize(pVgLeaders);
  SStmVgTasksToDeploy* pVg = NULL;
  
  for (int32_t i = 0; i < vgNum; ++i) {
    int32_t* vgId = taosArrayGet(pVgLeaders, i);
    pVg = taosHashAcquire(mStreamMgmt.toDeployVgMap, vgId, sizeof(*vgId));
    if (NULL == pVg) {
      continue;
    }

    if (taosWTryLockLatch(&pVg->lock)) {
      taosHashRelease(mStreamMgmt.toDeployVgMap, pVg);
      continue;
    }
    
    if (atomic_load_32(&pVg->deployed) <= 0) {
      taosWUnLockLatch(&pVg->lock);
      taosHashRelease(mStreamMgmt.toDeployVgMap, pVg);
      continue;
    }

    int32_t taskNum = taosArrayGetSize(pVg->taskList);
    if (atomic_load_32(&pVg->deployed) == taskNum) {
      (void)atomic_sub_fetch_32(&mStreamMgmt.toDeployVgTaskNum, taskNum);
      taosArrayDestroyEx(pVg->taskList, mstDestroySStmTaskToDeployExt);
      pVg->taskList = NULL;
      TAOS_UNUSED(taosHashRemove(mStreamMgmt.toDeployVgMap, vgId, sizeof(*vgId)));
      taosWUnLockLatch(&pVg->lock);
      taosHashRelease(mStreamMgmt.toDeployVgMap, pVg);
      continue;
    }

    for (int32_t m = taskNum - 1; m >= 0; --m) {
      SStmTaskToDeployExt* pExt = taosArrayGet(pVg->taskList, m);
      if (!pExt->deployed) {
        continue;
      }

      mstDestroySStmTaskToDeployExt(pExt);

      taosArrayRemove(pVg->taskList, m);
      (void)atomic_sub_fetch_32(&mStreamMgmt.toDeployVgTaskNum, 1);
    }
    atomic_store_32(&pVg->deployed, 0);
    taosWUnLockLatch(&pVg->lock);
    taosHashRelease(mStreamMgmt.toDeployVgMap, pVg);
  }

_exit:

  if (code) {
    if (pVg) {
      taosWUnLockLatch(&pVg->lock);
    }

    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
}

struct SStmSchemaBatch;

static void msmCompactSnodeDeployList(SArray** list, int32_t* deployed, const struct SStmSchemaBatch* expired,
                                      bool lostNode) {
  if (!*list) return;
  int32_t total = taosArrayGetSize(*list);
  if (total && !atomic_load_32(deployed) && !expired && !lostNode) return;

  int32_t kept = 0;
  for (int32_t i = 0; i < total; ++i) {
    SStmTaskToDeployExt* entry = taosArrayGet(*list, i);
    bool                 remove = entry->deployed;
#ifdef TD_ENTERPRISE
    if (entry->remoteSchema && (lostNode || (expired && entry->remoteSchema->batch == expired))) remove = true;
#endif
    if (remove) {
      mstDestroySStmTaskToDeployExt(entry);
    } else {
      if (kept != i) taosArraySet(*list, kept, entry);
      ++kept;
    }
  }

  // Ownership moved to the compacted prefix. Truncate without destroying duplicate tail slots.
  if (kept) {
    taosArrayPopTailBatch(*list, total - kept);
  } else {
    taosArrayDestroy(*list);
    *list = NULL;
  }
  (void)atomic_sub_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, total - kept);
  atomic_store_32(deployed, 0);
}

static void msmCleanSnodeDeploys(int32_t snodeId, const struct SStmSchemaBatch* expired, bool lostNode) {
  if (!GOT_SNODE(snodeId)) return;
  SStmSnodeTasksDeploy* node = taosHashAcquire(mStreamMgmt.toDeploySnodeMap, &snodeId, sizeof(snodeId));
  if (!node) return;

  if (expired || lostNode) {
    // A withdrawal must release the queue references before dropping the owning batch.
    taosWLockLatch(&node->lock);
  } else if (taosWTryLockLatch(&node->lock)) {
    taosHashRelease(mStreamMgmt.toDeploySnodeMap, node);
    return;
  }

  if (expired || lostNode || atomic_load_32(&node->triggerDeployed) || atomic_load_32(&node->runnerDeployed) ||
      atomic_load_32(&node->calcReaderDeployed)) {
    msmCompactSnodeDeployList(&node->triggerList, &node->triggerDeployed, NULL, false);
    msmCompactSnodeDeployList(&node->runnerList, &node->runnerDeployed, NULL, false);
    msmCompactSnodeDeployList(&node->calcReaderList, &node->calcReaderDeployed, expired, lostNode);
    if (!node->triggerList && !node->runnerList && !node->calcReaderList) {
      TAOS_UNUSED(taosHashRemove(mStreamMgmt.toDeploySnodeMap, &snodeId, sizeof(snodeId)));
    }
  }

  taosWUnLockLatch(&node->lock);
  taosHashRelease(mStreamMgmt.toDeploySnodeMap, node);
}

void msmCleanDeployedSnodeTasks(int32_t snodeId) { msmCleanSnodeDeploys(snodeId, NULL, false); }

void msmClearStreamToDeployMaps(SStreamHbMsg* pHb) {
  if (!atomic_load_32(&mStreamMgmt.toDeployVgTaskNum) && !atomic_load_32(&mStreamMgmt.toDeploySnodeTaskNum)) return;
  // A partial deployment can append to an existing queue. Exclude it until fully
  // claimed queues are unlinked, including the gap between hash lookup and node locking.
  (void)mstWaitLock(&mStreamMgmt.actionQLock, true);
  if (atomic_load_32(&mStreamMgmt.toDeployVgTaskNum) > 0) {
    msmCleanDeployedVgTasks(pHb->pVgLeaders);
  }

  if (atomic_load_32(&mStreamMgmt.toDeploySnodeTaskNum) > 0) {
    msmCleanDeployedSnodeTasks(pHb->snodeId);
  }
  taosRUnLockLatch(&mStreamMgmt.actionQLock);
}

void msmCleanStreamGrpCtx(SStreamHbMsg* pHb) {
  // SECURITY: this helper dereferences deployStm[gid]/actionStm[gid] as SHashObj (taosHashClear
  // frees through them). Never index the fixed-size arrays with a group id that is out of range,
  // even if a caller forgot to validate the wire-supplied gid.
  if (pHb->streamGId < 0 || pHb->streamGId >= STREAM_MAX_GROUP_NUM) {
    mstError("invalid stream gid %d, skip cleaning the stream grp ctx", pHb->streamGId);
    return;
  }

  int32_t tidx = streamGetThreadIdx(mStreamMgmt.threadNum, pHb->streamGId);
  if (mStreamMgmt.tCtx) {
    taosHashClear(mStreamMgmt.tCtx[tidx].actionStm[pHb->streamGId]);
    taosHashClear(mStreamMgmt.tCtx[tidx].deployStm[pHb->streamGId]);
  }
}

int32_t msmGrpAddActionStart(SHashObj* pHash, int64_t streamId, SStmTaskId* pId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t action = STREAM_ACT_START;
  SStmAction *pAction = taosHashGet(pHash, &streamId, sizeof(streamId));
  if (pAction) {
    pAction->actions |= action;
    pAction->start.triggerId = *pId;
    mstsDebug("stream append START action, actions:%x", pAction->actions);
  } else {
    SStmAction newAction = {0};
    newAction.actions = action;
    newAction.start.triggerId = *pId;
    TAOS_CHECK_EXIT(taosHashPut(pHash, &streamId, sizeof(streamId), &newAction, sizeof(newAction)));
    mstsDebug("stream add START action, actions:%x", newAction.actions);
  }

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmGrpAddActionUpdateTrigger(SHashObj* pHash, int64_t streamId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t action = STREAM_ACT_UPDATE_TRIGGER;
  
  SStmAction *pAction = taosHashGet(pHash, &streamId, sizeof(streamId));
  if (pAction) {
    pAction->actions |= action;
    mstsDebug("stream append UPDATE_TRIGGER action, actions:%x", pAction->actions);
  } else {
    SStmAction newAction = {0};
    newAction.actions = action;
    TAOS_CHECK_EXIT(taosHashPut(pHash, &streamId, sizeof(streamId), &newAction, sizeof(newAction)));
    mstsDebug("stream add UPDATE_TRIGGER action, actions:%x", newAction.actions);
  }

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}



int32_t msmGrpAddActionUndeploy(SStmGrpCtx* pCtx, int64_t streamId, SStreamTask* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t action = STREAM_ACT_UNDEPLOY;
  bool    dropped = false;

  TAOS_CHECK_EXIT(mstIsStreamDropped(pCtx->pMnode, streamId, &dropped));
  mstsDebug("stream dropped: %d", dropped);
  
  SStmAction *pAction = taosHashGet(pCtx->actionStm, &streamId, sizeof(streamId));
  if (pAction) {
    pAction->actions |= action;
    if (NULL == pAction->undeploy.taskList) {
      pAction->undeploy.taskList = taosArrayInit(pCtx->taskNum, POINTER_BYTES);
      TSDB_CHECK_NULL(pAction->undeploy.taskList, code, lino, _exit, terrno);
    }

    TSDB_CHECK_NULL(taosArrayPush(pAction->undeploy.taskList, &pTask), code, lino, _exit, terrno);
    if (pAction->undeploy.doCheckpoint) {
      pAction->undeploy.doCheckpoint = dropped ? false : true;
    }
    if (!pAction->undeploy.doCleanup) {
      pAction->undeploy.doCleanup = dropped ? true : false;
    }
    
    msttDebug("task append UNDEPLOY action[%d,%d], actions:%x", pAction->undeploy.doCheckpoint, pAction->undeploy.doCleanup, pAction->actions);
  } else {
    SStmAction newAction = {0};
    newAction.actions = action;
    newAction.undeploy.doCheckpoint = dropped ? false : true;
    newAction.undeploy.doCleanup = dropped ? true : false;
    newAction.undeploy.taskList = taosArrayInit(pCtx->taskNum, POINTER_BYTES);
    TSDB_CHECK_NULL(newAction.undeploy.taskList, code, lino, _exit, terrno);
    TSDB_CHECK_NULL(taosArrayPush(newAction.undeploy.taskList, &pTask), code, lino, _exit, terrno);
    TAOS_CHECK_EXIT(taosHashPut(pCtx->actionStm, &streamId, sizeof(streamId), &newAction, sizeof(newAction)));
    
    msttDebug("task add UNDEPLOY action[%d,%d]", newAction.undeploy.doCheckpoint, newAction.undeploy.doCleanup);
  }

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmGrpAddActionRecalc(SStmGrpCtx* pCtx, int64_t streamId, SArray* recalcList) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t action = STREAM_ACT_RECALC;
  SStmAction newAction = {0};
  
  SStmAction *pAction = taosHashGet(pCtx->actionStm, &streamId, sizeof(streamId));
  if (pAction) {
    taosArrayDestroy(pAction->recalc.recalcList);
    pAction->actions |= action;
    pAction->recalc.recalcList = recalcList;

    mstsDebug("stream append recalc action, listSize:%d, actions:%x", (int32_t)taosArrayGetSize(recalcList), pAction->actions);
  } else {
    newAction.actions = action;
    newAction.recalc.recalcList = recalcList;
    
    TAOS_CHECK_EXIT(taosHashPut(pCtx->actionStm, &streamId, sizeof(streamId), &newAction, sizeof(newAction)));
    newAction.recalc.recalcList = NULL;

    mstsDebug("stream add recalc action, listSize:%d", (int32_t)taosArrayGetSize(recalcList));
  }

_exit:

  if (code) {
    newAction.recalc.recalcList = NULL;
    mstDestroySStmAction(&newAction);
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

bool msmCheckStreamStartCond(int64_t streamId, int32_t snodeId) {
  SStmStatus* pStream = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStream) {
    return false;
  }

  if (pStream->triggerTask->id.nodeId != snodeId || STREAM_STATUS_INIT != pStream->triggerTask->status) {
    return false;
  }

  int32_t readerNum = taosArrayGetSize(pStream->trigReaders);
  for (int32_t i = 0; i < readerNum; ++i) {
    SStmTaskStatus* pStatus = taosArrayGet(pStream->trigReaders, i);
    if (STREAM_STATUS_INIT != pStatus->status && STREAM_STATUS_RUNNING != pStatus->status) {
      return false;
    }
  }

  SStmOrigReaderIter iter = {0};
  SStmTaskStatus*    reader;
  while ((reader = msmNextTrigOReader(pStream->trigOReaders, &iter))) {
    if (!reader->extReaderCleaned && STREAM_STATUS_INIT != reader->status && STREAM_STATUS_RUNNING != reader->status) {
      return false;
    }
  }

  readerNum = MST_LIST_SIZE(pStream->calcReaders);
  SListNode* pNode = listHead(pStream->calcReaders);
  for (int32_t i = 0; i < readerNum; ++i) {
    SStmTaskStatus* pStatus = (SStmTaskStatus*)pNode->data;
    if (STREAM_STATUS_INIT != pStatus->status && STREAM_STATUS_RUNNING != pStatus->status) {
      return false;
    }
    pNode = TD_DLIST_NODE_NEXT(pNode);
  }

  for (int32_t i = 0; i < pStream->runnerDeploys; ++i) {
    int32_t runnerNum = taosArrayGetSize(pStream->runners[i]);
    for (int32_t m = 0; m < runnerNum; ++m) {
      SStmTaskStatus* pStatus = taosArrayGet(pStream->runners[i], m);
      if (STREAM_STATUS_INIT != pStatus->status && STREAM_STATUS_RUNNING != pStatus->status) {
        return false;
      }
    }
  }
  
  return true;
}


void msmHandleTaskAbnormalStatus(SStmGrpCtx* pCtx, SStmTaskStatusMsg* pMsg, SStmTaskStatus* pTaskStatus) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t action = 0;
  int64_t streamId = pMsg->streamId;
  SStreamTask* pTask = (SStreamTask*)pMsg;
  int8_t  stopped = 0;

  msttDebug("start to handle task abnormal status %d", pTask->status);
  
  SStmStatus* pStatus = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStatus) {
    msttInfo("stream no longer exists in streamMap, try to undeploy current task, idx:%d", pMsg->taskIdx);
    TAOS_CHECK_EXIT(msmGrpAddActionUndeploy(pCtx, streamId, pTask));
    return;
  }

  stopped = atomic_load_8(&pStatus->stopped);
  if (stopped) {
    msttInfo("stream stopped %d, try to undeploy current task, idx:%d", stopped, pMsg->taskIdx);
    TAOS_CHECK_EXIT(msmGrpAddActionUndeploy(pCtx, streamId, pTask));
    return;
  }
  
  switch (pMsg->status) {
    case STREAM_STATUS_INIT:      
      if (STREAM_TRIGGER_TASK != pMsg->type) {
        msttTrace("task status is INIT and not trigger task, ignore it, currTs:%" PRId64 ", lastTs:%" PRId64, pCtx->currTs, pStatus->lastActionTs);
        return;
      }
      
      if (INT64_MIN == pStatus->lastActionTs) {
        msttDebug("task still not deployed, ignore it, currTs:%" PRId64 ", lastTs:%" PRId64, pCtx->currTs, pStatus->lastActionTs);
        return;
      }
      
      if ((pCtx->currTs - pStatus->lastActionTs) < STREAM_ACT_MIN_DELAY_MSEC) {
        msttDebug("task wait not enough between actions, currTs:%" PRId64 ", lastTs:%" PRId64, pCtx->currTs, pStatus->lastActionTs);
        return;
      }

      if (STREAM_IS_RUNNING(pStatus)) {
        msttDebug("stream already running, ignore status: %s", gStreamStatusStr[pTask->status]);
      } else if (GOT_SNODE(pCtx->pReq->snodeId) && msmCheckStreamStartCond(streamId, pCtx->pReq->snodeId)) {
        TAOS_CHECK_EXIT(msmGrpAddActionStart(pCtx->actionStm, streamId, &pStatus->triggerTask->id));
      }
      break;
    case STREAM_STATUS_FAILED:
      if (STREAM_RUNNER_TASK == pTask->type || STREAM_TRIGGER_TASK == pTask->type) {
        msttWarn("task failed with error:%s, try to undeploy whole stream, idx:%d", tstrerror(pMsg->errorCode),
                 pMsg->taskIdx);
        msmStopStreamByError(streamId, pStatus, pMsg->errorCode, pCtx->currTs);
        (void)mstWaitLock(&pStatus->resetLock, false);
        code = mstSetExtraErrMsg(&pStatus->extraErrMsg, pMsg->extraErrMsg);
        taosWUnLockLatch(&pStatus->resetLock);
        TAOS_CHECK_EXIT(code);
      } else {
        msttInfo("task failed with error:%s, try to undeploy current task, idx:%d", tstrerror(pMsg->errorCode),
                 pMsg->taskIdx);
        TAOS_CHECK_EXIT(msmGrpAddActionUndeploy(pCtx, streamId, pTask));
      }
      break;
    default:
      break;
  }

_exit:

  if (code) {
    msmStopStreamByError(streamId, pStatus, code, pCtx->currTs);
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
}

void msmHandleStreamTaskErr(SStmGrpCtx* pCtx, EStmErrType err, SStmTaskStatusMsg* pStatus) {
  if (pCtx->runtimeExpired) return;
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStreamTask* pTask = (SStreamTask*)pStatus;
  int64_t streamId = pStatus->streamId;

  msttInfo("start to handle task error, type: %d", err);

  TAOS_CHECK_EXIT(msmGrpAddActionUndeploy(pCtx, streamId, pTask));

_exit:

  if (code) {
    // IGNORE STOP STREAM BY ERROR  
    msttError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
}

void msmChkHandleTriggerOperations(SStmGrpCtx* pCtx, SStmTaskStatusMsg* pTask, SStmTaskStatus* pStatus) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmStatus* pStream = (SStmStatus*)pStatus->pStream;
  SArray*     pRecalcList = NULL;

  // Use plain mstInfo (not msttInfo) here: pTask is typed SStmTaskStatusMsg*, but msttXxx
  // blindly casts its implicit pTask to SStreamTask* - that cast is wrong for this function's
  // parameter type, so avoid the macro entirely rather than copy the mismatch.
  mstInfo(KEYOP("stream-trigger-fire", "START") "streamId:%" PRIx64 " taskId:%" PRIx64 " checking trigger operations",
          pTask->streamId, pTask->taskId);

  TAOS_CHECK_EXIT(mndStreamRecalcBuildDispatch(pStream, &pRecalcList));
  if (taosArrayGetSize(pRecalcList) > 0) {
    TAOS_CHECK_EXIT(msmGrpAddActionRecalc(pCtx, pTask->streamId, pRecalcList));
    pRecalcList = NULL;
  }

  if (1 == atomic_val_compare_exchange_8(&pStream->triggerNeedUpdate, 1, 0)) {
    TAOS_CHECK_EXIT(msmGrpAddActionUpdateTrigger(pCtx->actionStm, pTask->streamId));
  }

  if (pTask->detailStatus >= 0 && pCtx->pReq->pTriggerStatus) {
    SSTriggerRuntimeStatus* pTrigger = taosArrayGet(pCtx->pReq->pTriggerStatus, pTask->detailStatus);
    if (pTrigger == NULL) {
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    }

    TAOS_CHECK_EXIT(mstCopyTriggerRuntimeStatus(pStatus, pTrigger));
  }

_exit:

  taosArrayDestroy(pRecalcList);

  if (code) {
    // IGNORE STOP STREAM BY ERROR
    msttError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    // This runs every heartbeat round, not once per trigger's lifetime, so STEP (not END) is the
    // correct phase - "trigger firing" has no single terminal state to mark as END here.
    mstInfo(KEYOP("stream-trigger-fire", "STEP") "streamId:%" PRIx64 " taskId:%" PRIx64
                                                  " trigger operations handled, detailStatus:%d",
            pTask->streamId, pTask->taskId, pTask->detailStatus);
  }
}

static bool msmTaskStatusAcceptsHeartbeatMetrics(EStreamStatus status) {
  return status == STREAM_STATUS_RUNNING || status == STREAM_STATUS_INIT;
}

static SStmTaskStatus* msmGetHeartbeatTaskStatus(const SStmTaskStatusMsg* pTask) {
  if (!msmTaskStatusAcceptsHeartbeatMetrics(pTask->status)) {
    return NULL;
  }

  SStmTaskStatus** ppStatus =
      taosHashGet(mStreamMgmt.taskMap, &pTask->streamId, sizeof(pTask->streamId) + sizeof(pTask->taskId));
  if (ppStatus == NULL || *ppStatus == NULL) {
    return NULL;
  }

  SStmStatus* pStream = (SStmStatus*)(*ppStatus)->pStream;
  if (pStream == NULL || atomic_load_8(&pStream->stopped) || pTask->seriousId != (*ppStatus)->id.seriousId ||
      pTask->nodeId != (*ppStatus)->id.nodeId) {
    return NULL;
  }

  return *ppStatus;
}

static void msmLatchHeartbeatRecalcCapability(SStmGrpCtx* pCtx) {
  if (pCtx->pReq->observabilityVersion != STREAM_HB_OBSERVABILITY_VERSION_V1) {
    return;
  }

  int32_t statusNum = taosArrayGetSize(pCtx->pReq->pStreamStatus);
  for (int32_t i = 0; i < statusNum; ++i) {
    SStmTaskStatusMsg* pTask = taosArrayGet(pCtx->pReq->pStreamStatus, i);
    if (pTask == NULL || pTask->type != STREAM_TRIGGER_TASK) {
      continue;
    }

    SStmTaskStatus* pStatus = msmGetHeartbeatTaskStatus(pTask);
    if (pStatus == NULL || pStatus->type != STREAM_TRIGGER_TASK) {
      continue;
    }

    (void)mstWaitLock(&pStatus->detailStatusLock, false);
    pStatus->metrics.applicableMask |= STREAM_METRIC_RECALCULATES;
    taosWUnLockLatch(&pStatus->detailStatusLock);
  }
}

static void msmApplyHeartbeatTaskMetrics(SStmGrpCtx* pCtx, bool* pRecalcStartDeferred) {
  int32_t statusNum = taosArrayGetSize(pCtx->pReq->pStreamStatus);
  for (int32_t i = 0; i < statusNum; ++i) {
    SStmTaskStatusMsg* pTask = taosArrayGet(pCtx->pReq->pStreamStatus, i);
    SStmTaskStatus*    pStatus = pTask == NULL ? NULL : msmGetHeartbeatTaskStatus(pTask);
    if (pStatus != NULL) {
      mstInvalidateTaskMetrics(pStatus);
    }
  }

  if (pCtx->pReq->observabilityVersion != STREAM_HB_OBSERVABILITY_VERSION_V1) {
    return;
  }

  int32_t entryNum = taosArrayGetSize(pCtx->pReq->pTaskMetrics);
  if (entryNum <= 0 || statusNum <= 0) {
    return;
  }

  uint8_t* pIndexCounts = taosMemoryCalloc(statusNum, sizeof(*pIndexCounts));
  if (pIndexCounts == NULL) {
    mstError("failed to allocate stream task metric index counts, error:%s", tstrerror(terrno));
    return;
  }

  for (int32_t i = 0; i < entryNum; ++i) {
    SStreamTaskMetricsEntry* pEntry = taosArrayGet(pCtx->pReq->pTaskMetrics, i);
    if (pEntry != NULL && pEntry->taskStatusIndex >= 0 && pEntry->taskStatusIndex < statusNum &&
        pIndexCounts[pEntry->taskStatusIndex] < UINT8_MAX) {
      ++pIndexCounts[pEntry->taskStatusIndex];
    }
  }

  for (int32_t i = 0; i < entryNum; ++i) {
    SStreamTaskMetricsEntry* pEntry = taosArrayGet(pCtx->pReq->pTaskMetrics, i);
    if (pEntry == NULL || pEntry->taskStatusIndex < 0 || pEntry->taskStatusIndex >= statusNum ||
        pIndexCounts[pEntry->taskStatusIndex] != 1) {
      mstError("invalid or duplicate stream task metric index:%d", pEntry == NULL ? -1 : pEntry->taskStatusIndex);
      continue;
    }

    SStmTaskStatusMsg* pTask = taosArrayGet(pCtx->pReq->pStreamStatus, pEntry->taskStatusIndex);
    if (pTask != NULL && !msmTaskStatusAcceptsHeartbeatMetrics(pTask->status)) {
      continue;
    }

    SStmTaskStatus* pStatus = pTask == NULL ? NULL : msmGetHeartbeatTaskStatus(pTask);
    if (pStatus == NULL) {
      mstError("failed to resolve stream task metric index:%d", pEntry->taskStatusIndex);
      continue;
    }

    int32_t code = mstApplyTaskMetrics(pStatus, pEntry->taskStatusIndex, pTask->streamId, pEntry);
    if (code != TSDB_CODE_SUCCESS) {
      mstError("failed to apply stream task metrics, code:%s", tstrerror(code));
      bool fixedRecalcUsable = pEntry->decodeCode == TSDB_CODE_INVALID_MSG &&
                               pEntry->recalcDetailState == STREAM_RECALC_DETAIL_INVALID &&
                               pEntry->streamId == pTask->streamId && pEntry->taskId == pStatus->id.taskId &&
                               pEntry->seriousId == pStatus->id.seriousId;
      if (!fixedRecalcUsable) continue;
    }

    SStmStatus* pStream = pStatus->type == STREAM_TRIGGER_TASK ? pStatus->pStream : NULL;
    if (pStream != NULL && (pEntry->snapshot.validMask & STREAM_METRIC_RECALCULATES) != 0) {
      bool completeSnapshot = pEntry->decodeCode == TSDB_CODE_SUCCESS && pStream->triggerTask != NULL &&
                              pEntry->taskId == pStream->triggerTask->id.taskId &&
                              pEntry->seriousId == pStream->triggerTask->id.seriousId;
      if (pRecalcStartDeferred == NULL) {
        code = mndStreamRecalcApplySnapshot(pCtx->pMnode, pTask->streamId, pStream, pEntry->taskId, pEntry->seriousId,
                                            completeSnapshot, pEntry->recalcDetailState, pEntry->snapshot.pRecalculates,
                                            pEntry->snapshot.pRecalcDetails);
      } else {
        code = mndStreamRecalcApplySnapshotDeferred(pCtx->pMnode, pTask->streamId, pStream, pEntry->taskId,
                                                    pEntry->seriousId, completeSnapshot, pEntry->recalcDetailState,
                                                    pEntry->snapshot.pRecalculates, pEntry->snapshot.pRecalcDetails,
                                                    pRecalcStartDeferred);
      }
      if (code != TSDB_CODE_SUCCESS) {
        mstError("failed to apply stream recalculation metrics, code:%s", tstrerror(code));
      }
    }
  }

  taosMemoryFree(pIndexCounts);
}

#ifdef TD_ENTERPRISE
static int32_t msmQueueExtOrigTblReaderUndeploy(SStmGrpCtx* ctx, int64_t streamId, const SStmTaskStatus* reader);

static bool msmReaderCleanupBelongsToPatch(SStmStatus* owner, const SStmTaskStatusMsg* task) {
  if (!owner || task->type != STREAM_READER_TASK || !STREAM_IS_REF_EXT_SOURCE(task->flags)) return false;
  taosRLockLatch(&owner->resetLock);
  SStmSchemaBatch*        batch = owner->pendingSchema;
  const SStmCachedReader* reader = batch && batch->published && taosArrayGetSize(batch->pairedCleanups)
                                       ? taosArraySearch(batch->pairedCleanups, &task->taskId, compareInt64Val, TD_EQ)
                                       : NULL;
  bool                    paired = reader && reader->seriousId == task->seriousId && reader->nodeId == task->nodeId;
  taosRUnLockLatch(&owner->resetLock);
  return paired;
}

// A PATCH carries its own cleanup. A standalone retirement repeats until the Reader disappears.
static bool msmHandleRetiredReaderReport(SStmGrpCtx* ctx, const SStmTaskStatusMsg* task) {
  if (task->type != STREAM_READER_TASK || !STREAM_IS_REF_EXT_SOURCE(task->flags) || task->nodeId != ctx->pReq->snodeId)
    return false;
  SStmStatus* owner = taosHashGet(mStreamMgmt.streamMap, &task->streamId, sizeof(task->streamId));
  if (msmReaderCleanupBelongsToPatch(owner, task)) return true;
  if (!owner || !owner->retiredExtReaders) return false;
  SStmTaskStatus** reader = taosHashGet(owner->retiredExtReaders, &task->taskId, sizeof(task->taskId));
  if (!reader || (*reader)->id.seriousId != task->seriousId || (*reader)->id.nodeId != task->nodeId) return false;
  (void)msmQueueExtOrigTblReaderUndeploy(ctx, task->streamId, *reader);
  return true;
}

static bool msmHeartbeatReportsSchemaReaders(const SStmGrpCtx* ctx, const SStmSchemaBatch* batch) {
  for (size_t i = 0; i < taosArrayGetSize(batch->schemas); ++i) {
    const SStmExtSchema*    schema = taosArrayGet(batch->schemas, i);
    const SStmCachedReader* expected = &schema->requiredReader;
    if (!expected->taskId || schema->superseded) continue;
    if (batch->remote) {
      if (!msmRemoteSchemaReaderInstalled(ctx, batch->streamId, expected)) return false;
    } else {
      const bool* report = msmReportedSchemaReader(ctx, batch->streamId, expected);
      if (!report || !*report) return false;
      int64_t          key[] = {batch->streamId, expected->taskId};
      SStmTaskStatus** live = taosHashGet(mStreamMgmt.taskMap, key, sizeof(key));
      if (!live || (*live)->id.seriousId != expected->seriousId ||
          atomic_load_64(&(*live)->remoteSchemaConfirmation) == MST_READER_INSTALL_FAILED)
        return false;
    }
  }
  return true;
}

static bool msmHeartbeatHasRequestIndex(const SStreamHbMsg* hb, int32_t index) {
  for (size_t i = 0; i < taosArrayGetSize(hb->pStreamReq); ++i) {
    if (*(int32_t*)taosArrayGet(hb->pStreamReq, i) == index) return true;
  }
  return false;
}
#endif

static int32_t msmNormalHandleStatusUpdateImpl(SStmGrpCtx* pCtx, bool* pRecalcStartDeferred) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t num = taosArrayGetSize(pCtx->pReq->pStreamStatus);

  mstDebug("NORMAL: start to handle stream group %d tasks status, taskNum:%d", pCtx->pReq->streamGId, num);

  msmLatchHeartbeatRecalcCapability(pCtx);

  for (int32_t i = 0; i < num; ++i) {
    SStmTaskStatusMsg* pTask = taosArrayGet(pCtx->pReq->pStreamStatus, i);
    msttDebug("task status %s got on dnode %d, taskIdx:%d", gStreamStatusStr[pTask->status], pCtx->pReq->dnodeId, pTask->taskIdx);

#ifdef TD_ENTERPRISE
    // A fenced target must not schedule cleanup for now-missing tasks either.
    if (pCtx->deferReaderDeploy && pTask->type == STREAM_READER_TASK && STREAM_IS_REF_EXT_SOURCE(pTask->flags) &&
        pTask->nodeId == pCtx->pReq->snodeId)
      continue;
#endif
    SStmTaskStatus** ppStatus = taosHashGet(mStreamMgmt.taskMap, &pTask->streamId, sizeof(pTask->streamId) + sizeof(pTask->taskId));
    if (NULL == ppStatus) {
#ifdef TD_ENTERPRISE
      if (msmHandleRetiredReaderReport(pCtx, pTask)) continue;
#endif
      msttWarn("task no longer exists in taskMap, streamId:0x%" PRIx64 " taskId:0x%" PRIx64 " type:%s status:%s taskIdx:%d",
          (uint64_t)pTask->streamId, (uint64_t)pTask->taskId, gStreamTaskTypeStr[pTask->type], gStreamStatusStr[pTask->status], pTask->taskIdx);
      msmHandleStreamTaskErr(pCtx, STM_ERR_TASK_NOT_EXISTS, pTask);
      continue;
    }

    SStmStatus* pStream = (SStmStatus*)(*ppStatus)->pStream;
    int8_t stopped = atomic_load_8(&pStream->stopped);
    if (stopped) {
      msttWarn("stream already stopped %d, will try to undeploy current task, taskIdx:%d", stopped, pTask->taskIdx);
      msmHandleStreamTaskErr(pCtx, STM_ERR_STREAM_STOPPED, pTask);
      continue;
    }

    if ((pTask->seriousId != (*ppStatus)->id.seriousId) || (pTask->nodeId != (*ppStatus)->id.nodeId)) {
#ifdef TD_ENTERPRISE
      if (msmReaderCleanupBelongsToPatch(pStream, pTask)) continue;
#endif
      msttInfo("task mismatch with it in taskMap, will try to rm it, current seriousId:%" PRId64 ", nodeId:%d",
               (*ppStatus)->id.seriousId, (*ppStatus)->id.nodeId);

      msmHandleStreamTaskErr(pCtx, STM_ERR_TASK_NOT_EXISTS, pTask);
      continue;
    }

    (void)mstWaitLock(&pStream->resetLock, false);
#ifdef TD_ENTERPRISE
    if (pTask->type == STREAM_READER_TASK && STREAM_IS_REF_EXT_SOURCE(pTask->flags) &&
        pTask->nodeId == pCtx->pReq->snodeId) {
      bool failed = pTask->status == STREAM_STATUS_FAILED;
      if (failed || atomic_load_64(&(*ppStatus)->remoteSchemaConfirmation) == MST_READER_INSTALL_FAILED) {
        bool current = msmAcceptReaderRemoval(pCtx, *ppStatus, failed);
        if (!current || !failed) {
          taosWUnLockLatch(&pStream->resetLock);
          // Repeat a lost uninstall without refreshing the failed Reader's health-check deadline.
          if (current) msmHandleStreamTaskErr(pCtx, STM_ERR_TASK_NOT_EXISTS, pTask);
          continue;
        }
      } else if (pCtx->readerConfirmation) {
        SStmCachedReader target = {pTask->taskId, pTask->seriousId, pTask->nodeId};
        const bool*      report = msmReportedSchemaReader(pCtx, pTask->streamId, &target);
        bool             healthy = report ? *report : msmTaskStatusAcceptsHeartbeatMetrics(pTask->status);
        atomic_store_64(&(*ppStatus)->remoteSchemaConfirmation, healthy ? pCtx->readerConfirmation : 0);
      }
    }
    SStmSchemaBatch* pending = pStream->pendingSchema;
    if (pTask->type == STREAM_TRIGGER_TASK && pending && pending->published && !pTask->pMgmtReq &&
        pending->taskId == pTask->taskId && pending->seriousId == pTask->seriousId &&
        !msmHeartbeatHasRequestIndex(pCtx->pReq, i)) {
      bool confirmed = pTask->ackedSchemaReqId > 0 && pTask->ackedSchemaReqId == pending->reqId;
      if (!confirmed && !pending->remote && !pending->explicitAck && pending->hasDeploy) {
        confirmed = msmHeartbeatReportsSchemaReaders(pCtx, pending);
      }
      if (confirmed || (!pending->remote && !pending->explicitAck && !pending->hasDeploy)) {
        if (confirmed) (*ppStatus)->ackedSchemaReqId = pending->reqId;
        pStream->pendingSchema = NULL;
        msmRemoveSchemaNodeWait(pending->mnode, &pending->localAckNode);
        msmReleaseSchemaBatch(pending);
      }
    }
#endif
    if ((*ppStatus)->status != pTask->status) {
      if (STREAM_STATUS_RUNNING == pTask->status) {
        (*ppStatus)->runningStartTs = pCtx->currTs;
      } else if (MST_IS_RUNNER_GETTING_READY(pTask) && STREAM_IS_REDEPLOY_RUNNER((*ppStatus)->flags)) {
        if (pStream->triggerTask) {
          atomic_store_8(&pStream->triggerNeedUpdate, 1);
        }
        
        STREAM_CLR_FLAG((*ppStatus)->flags, STREAM_FLAG_REDEPLOY_RUNNER);
      }

      if (STREAM_TRIGGER_TASK == pTask->type && STREAM_STATUS_RUNNING == pTask->status) {
        mstInfo("streamId:0x%" PRIx64 " trigger task status updated to Running in streamMap", (uint64_t)pTask->streamId);
      }
    }
    
    (*ppStatus)->errCode = pTask->errorCode;
    code = mstSetExtraErrMsg(&(*ppStatus)->extraErrMsg, pTask->extraErrMsg);
    (*ppStatus)->status = pTask->status;
    (*ppStatus)->lastUpTs = pCtx->currTs;

    taosWUnLockLatch(&pStream->resetLock);
    TAOS_CHECK_EXIT(code);
    if (STREAM_STATUS_RUNNING != pTask->status) {
      msmHandleTaskAbnormalStatus(pCtx, pTask, *ppStatus);
    }
    
    if (STREAM_TRIGGER_TASK == pTask->type) {
      msmChkHandleTriggerOperations(pCtx, pTask, *ppStatus);
    }
  }

  msmApplyHeartbeatTaskMetrics(pCtx, pRecalcStartDeferred);

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmNormalHandleStatusUpdate(SStmGrpCtx* pCtx) {
#ifdef TD_ENTERPRISE
  int32_t code = msmIndexReportedSchemaReaders(pCtx);
  if (!code) code = msmNormalHandleStatusUpdateImpl(pCtx, NULL);
  taosHashCleanup(pCtx->reportedSchemaReaders);
  pCtx->reportedSchemaReaders = NULL;
  return code;
#else
  return msmNormalHandleStatusUpdateImpl(pCtx, NULL);
#endif
}

static int32_t msmWatchAttachTask(SStmGrpCtx* pCtx, SStmTaskStatusMsg* pTask, SStmStatus* pStatus) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t         streamId = pTask->streamId;
  SStmTaskStatus* pNewTask = NULL;
  switch (pTask->type) {
    case STREAM_READER_TASK: {
      void* pList = STREAM_IS_TRIGGER_READER(pTask->flags) ? (void*)pStatus->trigReaders : (void*)pStatus->calcReaders;
      if (NULL == pList) {
        mstsError("%sReader list is NULL", STREAM_IS_TRIGGER_READER(pTask->flags) ? "trig" : "calc");
        TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      }
      int32_t readerSize = STREAM_IS_TRIGGER_READER(pTask->flags) ? pStatus->trigReaderNum : pStatus->calcReaderNum;
      if ((STREAM_IS_TRIGGER_READER(pTask->flags) && taosArrayGetSize(pList) >= readerSize) ||
          (!STREAM_IS_TRIGGER_READER(pTask->flags) && MST_LIST_SIZE((SList*)pList) >= readerSize)) {
        mstsError("%sReader list is already full, size:%d, expSize:%d", STREAM_IS_TRIGGER_READER(pTask->flags) ? "trig" : "calc",
            (int32_t)taosArrayGetSize(pList), readerSize);
        TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      }

      SStmTaskStatus taskStatus = {0};
      taskStatus.pStream = pStatus;
      TAOS_CHECK_EXIT(mstSetTaskStatusFromMsg(pCtx, &taskStatus, pTask));
      if (STREAM_IS_TRIGGER_READER(pTask->flags)) {
        pNewTask = taosArrayPush(pList, &taskStatus);
        if (pNewTask == NULL) {
          mstDestroySStmTaskStatus(&taskStatus);
          TSDB_CHECK_NULL(pNewTask, code, lino, _exit, terrno);
        }
      } else {
        code = tdListAppend(pStatus->calcReaders, &taskStatus);
        if (code != TSDB_CODE_SUCCESS) {
          mstDestroySStmTaskStatus(&taskStatus);
          TAOS_CHECK_EXIT(code);
        }
        SListNode* pTailNode = tdListGetTail(pStatus->calcReaders);
        QUERY_CHECK_NULL(pTailNode, code, lino, _exit, TSDB_CODE_INTERNAL_ERROR);
        pNewTask = (SStmTaskStatus*)pTailNode->data;
      }
      
      TAOS_CHECK_EXIT(msmSTAddToTaskMap(pCtx, streamId, NULL, NULL, pNewTask));
      TAOS_CHECK_EXIT(msmSTAddToVgroupMapImpl(streamId, pNewTask, STREAM_IS_TRIGGER_READER(pTask->flags)));
      break;
    }
    case STREAM_TRIGGER_TASK: {
      mstDestroySStmTaskStatus(pStatus->triggerTask);
      taosMemoryFreeClear(pStatus->triggerTask);
      pStatus->triggerTask = taosMemoryCalloc(1, sizeof(*pStatus->triggerTask));
      TSDB_CHECK_NULL(pStatus->triggerTask, code, lino, _exit, terrno);
      pStatus->triggerTask->pStream = pStatus;
      TAOS_CHECK_EXIT(mstSetTaskStatusFromMsg(pCtx, pStatus->triggerTask, pTask));
      pNewTask = pStatus->triggerTask;

      TAOS_CHECK_EXIT(msmSTAddToTaskMap(pCtx, streamId, NULL, NULL, pNewTask));
      TAOS_CHECK_EXIT(msmSTAddToSnodeMapImpl(streamId, pNewTask, 0));
      break;
    }
    case STREAM_RUNNER_TASK: {
      TAOS_CHECK_EXIT(mstRestoreRunnerDeploy(pStatus, pTask->deployId));

      if (taosArrayGetSize(pStatus->runners[pTask->deployId]) >= pStatus->runnerNum) {
        mstsError("deploy %d runner list is already full, size:%d, expSize:%d", pTask->deployId,
                  (int32_t)taosArrayGetSize(pStatus->runners[pTask->deployId]), pStatus->runnerNum);
        TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      }

      SStmTaskStatus taskStatus = {0};
      taskStatus.pStream = pStatus;
      TAOS_CHECK_EXIT(mstSetTaskStatusFromMsg(pCtx, &taskStatus, pTask));
      pNewTask = taosArrayPush(pStatus->runners[pTask->deployId], &taskStatus);
      if (pNewTask == NULL) {
        mstDestroySStmTaskStatus(&taskStatus);
        TSDB_CHECK_NULL(pNewTask, code, lino, _exit, terrno);
      }

      TAOS_CHECK_EXIT(msmSTAddToTaskMap(pCtx, streamId, NULL, NULL, pNewTask));
      TAOS_CHECK_EXIT(msmSTAddToSnodeMapImpl(streamId, pNewTask, pTask->deployId));
      break;
    }
    default: {
      msttError("invalid task type:%d in task status", pTask->type);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      break;
    }
  }

_exit:

  if (code) {
    msttError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    msttDebug("new task recorded to taskMap/streamMap, task status:%s", gStreamStatusStr[pTask->status]);
  }

  return code;
}

static int32_t msmWatchHasVirtualMetadata(SStreamObj* stream, bool* found) {
  *found = false;
  // Old requests may omit vtableCalc when only a subquery reads a virtual table.
  // Such plans also need full deployment: their persisted partition count can
  // include removed metadata Readers. Decode outside the publication lock.
  for (size_t i = 0; i < taosArrayGetSize(stream->pCreate->calcScanPlanList); ++i) {
    SStreamCalcScan* scan = taosArrayGet(stream->pCreate->calcScanPlanList, i);
    if (!msmScanHasVgroupReaders(scan)) continue;
    SSubplan* plan = NULL;
    int32_t code = nodesStringToNode(scan->scanPlan, (SNode**)&plan);
    if (!code) *found = msmIsVirtualMetadataScan(plan);
    nodesDestroyNode((SNode*)plan);
    if (code || *found) return code;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t msmWatchRecordNewTask(SStmGrpCtx* pCtx, SStmTaskStatusMsg* pTask) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int64_t              streamId = pTask->streamId;
  int64_t              key[] = {streamId, pTask->taskId};
  bool                 trigger = pTask->type == STREAM_TRIGGER_TASK && pTask->taskId && pTask->seriousId;
  SStreamObj*          stream = NULL;
  SStmStatus           initial = {0};
  SStreamRecalcRestore restore = {0};

  // The heartbeat's runtime read lock pins the epoch. WATCH ending waits for all processing
  // heartbeats before clearing its maps, so an observed stream status survives private preparation.
  (void)mstWaitLock(&mStreamMgmt.actionQLock, false);
  if (pTask->taskId >= atomic_load_64(&mStreamMgmt.lastTaskId)) {
    atomic_store_64(&mStreamMgmt.lastTaskId, pTask->taskId + 1);
  }
  for (;;) {
    SStmTaskStatus** existing = taosHashGet(mStreamMgmt.taskMap, key, sizeof(key));
    if (existing) {
      (*existing)->status = pTask->status;
      (*existing)->lastUpTs = pCtx->currTs;
      break;
    }
    SStmStatus* observed = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
    if (!observed && taosHashGet(mStreamMgmt.watch.deferredStreams, &streamId, sizeof(streamId))) break;
    if (observed && atomic_load_8(&observed->stopped)) break;
    if (observed && observed->triggerTask && pTask->type == STREAM_TRIGGER_TASK) {
      mstsWarn("conflicting WATCH triggers %" PRIx64 " and %" PRIx64, observed->triggerTask->id.taskId, pTask->taskId);
      msmStopStreamByError(streamId, observed, TSDB_CODE_MND_STREAM_TASK_LOST, pCtx->currTs);
      break;
    }
    if ((!observed && !initial.deployInput) || (trigger && !restore.records)) {
      taosWUnLockLatch(&mStreamMgmt.actionQLock);
      if (!stream) {
        code = observed ? mndAcquireStream(pCtx->pMnode, observed->streamName, &stream)
                        : mndAcquireStreamById(pCtx->pMnode, streamId, &stream);
        if (code || !stream) {
          if (!code) code = TSDB_CODE_MND_STREAM_NOT_EXIST;
          goto _exit;
        }
      }
      if (stream->pCreate->streamId != streamId) goto _exit;
      if (!observed && !initial.deployInput) {
        bool defer = STREAM_IS_VIRTUAL_TABLE(stream->pCreate->triggerTblType, stream->pCreate->flags) ||
                     stream->pCreate->vtableCalc;
        if (!defer) code = msmWatchHasVirtualMetadata(stream, &defer);
        if (code || defer) {
          // A failed classification affects only this stream. Normal deployment retries it after WATCH;
          // never discard successfully adopted streams because another stream's plan cannot be decoded.
#ifdef BUILD_TEST_SEAM
          if (gTdSeam_msmDeployPhase) gTdSeam_msmDeployPhase("watch-defer", stream->name);
#endif
          (void)mstWaitLock(&mStreamMgmt.actionQLock, false);
          if (taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId))) {
            code = TSDB_CODE_SUCCESS;
            continue;  // Another heartbeat adopted it while this one prepared outside the lock.
          }
          if (taosHashGet(mStreamMgmt.watch.deferredStreams, &streamId, sizeof(streamId))) {
            code = TSDB_CODE_SUCCESS;
            break;
          }
          if (code) mstsWarn("defer WATCH adoption to normal deployment, scan classification failed, error:%s", tstrerror(code));
          if (!mStreamMgmt.watch.deferredStreams) {
            mStreamMgmt.watch.deferredStreams =
                taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
          }
          int8_t deferred = 1;
          int32_t cacheCode = mStreamMgmt.watch.deferredStreams
                                  ? taosHashPut(mStreamMgmt.watch.deferredStreams, &streamId, sizeof(streamId),
                                                &deferred, sizeof(deferred))
                                  : TSDB_CODE_OUT_OF_MEMORY;
          if (cacheCode) mstsWarn("failed to cache WATCH deferral, will retry classification, error:%s", tstrerror(cacheCode));
          code = TSDB_CODE_SUCCESS;
          break;
        }
#ifdef BUILD_TEST_SEAM
        if (gTdSeam_msmDeployPhase) gTdSeam_msmDeployPhase("watch-copy", stream->name);
#endif
        code = msmInitStmStatus(pCtx, &initial, stream, true);
      }
      if (!code && trigger) {
        code = mndStreamRecalcCaptureRecords(observed ? observed : &initial, &restore);
        if (!code) code = mndStreamRecalcPrepareRestore(stream, &restore, pTask->taskId, pTask->seriousId);
      }
      if (code) goto _exit;
      (void)mstWaitLock(&mStreamMgmt.actionQLock, false);
      continue;
    }
    if (stream) {
      SStreamObj* current = NULL;
      code = mndAcquireStream(pCtx->pMnode, stream->name, &current);
      bool same =
          !code && current == stream && !atomic_load_8(&stream->userStopped) && !atomic_load_8(&stream->userDropped);
      if (current) mndReleaseStream(pCtx->pMnode, current);
      if (!same) {
        if (code == TSDB_CODE_MND_STREAM_NOT_EXIST) code = TSDB_CODE_SUCCESS;
        break;
      }
    }
    if (msmStopIfGrantExpired(streamId, observed)) break;
    if (!observed) {
      code = taosHashPut(mStreamMgmt.streamMap, &streamId, sizeof(streamId), &initial, sizeof(initial));
      if (code) break;
      memset(&initial, 0, sizeof(initial));
      observed = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
    }
    if (trigger && !mndStreamRecalcCommitRestore(stream, observed, &restore)) {
      // The next WATCH report retries adoption. If WATCH ends first, its missing-trigger check schedules recovery.
      mstsDebug("defer WATCH trigger %" PRIx64 " after recalculation restore conflict", pTask->taskId);
      break;
    }
    code = msmWatchAttachTask(pCtx, pTask, observed);
    break;
  }
  taosWUnLockLatch(&mStreamMgmt.actionQLock);
_exit:
  if (stream) mndReleaseStream(pCtx->pMnode, stream);
  if (initial.streamName) mstDestroySStmStatus(&initial);
  taosArrayDestroy(restore.records);
  return code;
}

int32_t msmWatchHandleStatusUpdate(SStmGrpCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t num = taosArrayGetSize(pCtx->pReq->pStreamStatus);

  mstDebug("WATCH: start to handle stream group %d tasks status, taskNum:%d", pCtx->pReq->streamGId, num);

  for (int32_t i = 0; i < num; ++i) {
    SStmTaskStatusMsg* pTask = taosArrayGet(pCtx->pReq->pStreamStatus, i);
    msttDebug("task status %s got, taskIdx:%d", gStreamStatusStr[pTask->status], pTask->taskIdx);

    TAOS_CHECK_EXIT(msmWatchRecordNewTask(pCtx, pTask));
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

void msmRspAddStreamStart(int64_t streamId, SStmGrpCtx* pCtx, int32_t streamNum, SStmAction *pAction) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (NULL == pCtx->pRsp->start.taskList) {
    pCtx->pRsp->start.taskList = taosArrayInit(streamNum, sizeof(SStreamTaskStart));
    TSDB_CHECK_NULL(pCtx->pRsp->start.taskList, code, lino, _exit, terrno);
  }

  SStmTaskId* pId = &pAction->start.triggerId;
  SStreamTaskStart start = {0};
  start.task.type = STREAM_TRIGGER_TASK;
  start.task.streamId = streamId;
  start.task.taskId = pId->taskId;
  start.task.seriousId = pId->seriousId;
  start.task.nodeId = pId->nodeId;
  start.task.taskIdx = pId->taskIdx;

  TSDB_CHECK_NULL(taosArrayPush(pCtx->pRsp->start.taskList, &start), code, lino, _exit, terrno);
  TAOS_CHECK_EXIT(msmUpdateStreamLastActTs(streamId, pCtx->currTs));

  mstsDebug("stream START added to dnode %d hb rsp, triggerTaskId:%" PRIx64, pId->nodeId, pId->taskId);

  return;

_exit:

  mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
}


void msmRspAddStreamUndeploy(int64_t streamId, SStmGrpCtx* pCtx, SStmAction *pAction) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t dropNum = taosArrayGetSize(pAction->undeploy.taskList);
  if (NULL == pCtx->pRsp->undeploy.taskList) {
    pCtx->pRsp->undeploy.taskList = taosArrayInit(dropNum, sizeof(SStreamTaskUndeploy));
    TSDB_CHECK_NULL(pCtx->pRsp->undeploy.taskList, code, lino, _exit, terrno);
  }

  SStreamTaskUndeploy undeploy;
  for (int32_t i = 0; i < dropNum; ++i) {
    SStreamTask* pTask = (SStreamTask*)taosArrayGetP(pAction->undeploy.taskList, i);
    undeploy.task = *pTask;
    undeploy.undeployMsg.doCheckpoint = pAction->undeploy.doCheckpoint;
    undeploy.undeployMsg.doCleanup = pAction->undeploy.doCleanup;

    TSDB_CHECK_NULL(taosArrayPush(pCtx->pRsp->undeploy.taskList, &undeploy), code, lino, _exit, terrno);
    TAOS_CHECK_EXIT(msmUpdateStreamLastActTs(streamId, pCtx->currTs));

    msttDebug("task UNDEPLOY added to hb rsp, doCheckpoint:%d, doCleanup:%d", undeploy.undeployMsg.doCheckpoint, undeploy.undeployMsg.doCleanup);
  }

  return;

_exit:

  mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
}

void msmRspAddTriggerUpdate(SMnode * pMnode, int64_t streamId, SStmGrpCtx* pCtx, SStmAction *pAction) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SStmStatus* pStream = (SStmStatus*)taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStream) {
    mstsDebug("stream no longer exists in streamMap, ignore trigger update, streamRemain:%d", taosHashGetSize(mStreamMgmt.streamMap));
    return;
  }

  if (NULL == pStream->triggerTask) {
    mstsWarn("no triggerTask exists, ignore trigger update, stopped:%d", atomic_load_8(&pStream->stopped));
    return;
  }

  if (mstRunnerSnapshotUnknown(pStream)) {
    code = mstPostRunnerSnapshotRedeploy(mStreamMgmt.actionQ, streamId, pStream);
    if (code) {
      atomic_store_8(&pStream->triggerNeedUpdate, 1);
      mstsError("failed to queue unknown runner snapshot refresh since %s", tstrerror(code));
    }
    return;
  }

  SStreamMgmtRsp rsp = {0};
  rsp.reqId = INT64_MIN;
  rsp.header.msgType = STREAM_MSG_UPDATE_RUNNER;
  rsp.task.streamId = streamId;
  rsp.task.taskId = pStream->triggerTask->id.taskId;

  TAOS_CHECK_EXIT(msmBuildTriggerRunnerTargets(pMnode, pStream, streamId, &rsp.cont.runnerList));  

  if (NULL == pCtx->pRsp->rsps.rspList) {
    pCtx->pRsp->rsps.rspList = taosArrayInit(2, sizeof(SStreamMgmtRsp));
    TSDB_CHECK_NULL(pCtx->pRsp->rsps.rspList, code, lino, _exit, terrno);
  }

  TSDB_CHECK_NULL(taosArrayPush(pCtx->pRsp->rsps.rspList, &rsp), code, lino, _exit, terrno);

_exit:

  if (code) {
    tFreeSStreamMgmtRsp(&rsp);
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    mstsDebug("trigger update rsp added, runnerNum:%d", (int32_t)taosArrayGetSize(rsp.cont.runnerList));
  }
}

void msmRspAddUserRecalc(SMnode * pMnode, int64_t streamId, SStmGrpCtx* pCtx, SStmAction *pAction) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SStmStatus* pStream = (SStmStatus*)taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStream) {
    mstsDebug("stream no longer exists in streamMap, ignore trigger update, streamRemain:%d", taosHashGetSize(mStreamMgmt.streamMap));
    return;
  }

  if (NULL == pStream->triggerTask) {
    mstsWarn("no triggerTask exists, ignore trigger update, stopped:%d", atomic_load_8(&pStream->stopped));
    return;
  }

  SStreamMgmtRsp rsp = {0};
  rsp.reqId = INT64_MIN;
  rsp.header.msgType = STREAM_MSG_USER_RECALC;
  rsp.task.streamId = streamId;
  rsp.task.taskId = pStream->triggerTask->id.taskId;
  TSWAP(rsp.cont.recalcList, pAction->recalc.recalcList);

  if (NULL == pCtx->pRsp->rsps.rspList) {
    pCtx->pRsp->rsps.rspList = taosArrayInit(2, sizeof(SStreamMgmtRsp));
    TSDB_CHECK_NULL(pCtx->pRsp->rsps.rspList, code, lino, _exit, terrno);
  }

  TSDB_CHECK_NULL(taosArrayPush(pCtx->pRsp->rsps.rspList, &rsp), code, lino, _exit, terrno);

_exit:

  if (code) {
    tFreeSStreamMgmtRsp(&rsp);
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    mstsDebug("user recalc rsp added, recalcNum:%d", (int32_t)taosArrayGetSize(rsp.cont.recalcList));
  }
}


int32_t msmHandleHbPostActions(SStmGrpCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void* pIter = NULL;
  int32_t streamNum = taosHashGetSize(pCtx->actionStm);

  mstDebug("start to handle stream group %d post actions", pCtx->pReq->streamGId);

  while (1) {
    pIter = taosHashIterate(pCtx->actionStm, pIter);
    if (pIter == NULL) {
      break;
    }

    int64_t* pStreamId = taosHashGetKey(pIter, NULL);
    SStmAction *pAction = (SStmAction *)pIter;
    
    if (STREAM_ACT_UNDEPLOY & pAction->actions) {
      msmRspAddStreamUndeploy(*pStreamId, pCtx, pAction);
      continue;
    }

    if (STREAM_ACT_UPDATE_TRIGGER & pAction->actions) {
      msmRspAddTriggerUpdate(pCtx->pMnode, *pStreamId, pCtx, pAction);
    }

    if (STREAM_ACT_RECALC & pAction->actions) {
      msmRspAddUserRecalc(pCtx->pMnode, *pStreamId, pCtx, pAction);
    }

    if (STREAM_ACT_START & pAction->actions) {
      msmRspAddStreamStart(*pStreamId, pCtx, streamNum, pAction);
    }
  }
  
_exit:

  if (pIter) {
    taosHashCancelIterate(pCtx->actionStm, pIter);
  }

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmCheckUpdateDnodeTs(SStmGrpCtx* pCtx) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int64_t* lastTs = NULL;
  bool     noExists = false;

  while (true) {
    lastTs = taosHashGet(mStreamMgmt.dnodeMap, &pCtx->pReq->dnodeId, sizeof(pCtx->pReq->dnodeId));
    if (NULL == lastTs) {
      if (noExists) {
        mstWarn("Got unknown dnode %d hb msg, may be dropped", pCtx->pReq->dnodeId);
        TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_NODE_NOT_EXISTS);
      }

      noExists = true;
      TAOS_CHECK_EXIT(msmSTAddDnodesToMap(pCtx->pMnode));
      
      continue;
    }

    while (true) {
      int64_t lastTsValue = atomic_load_64(lastTs);
      if (pCtx->currTs > lastTsValue) {
        if (lastTsValue == atomic_val_compare_exchange_64(lastTs, lastTsValue, pCtx->currTs)) {
          mstDebug("dnode %d lastUpTs updated", pCtx->pReq->dnodeId);
          return code;
        }

        continue;
      }

      return code;
    }

    break;
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;  
}

void msmWatchCheckStreamMap(SStmGrpCtx* pCtx) {
  SStmStatus* pStatus = NULL;
  int32_t trigReaderNum = 0;
  int32_t calcReaderNum = 0;
  int32_t runnerNum = 0;
  int64_t streamId = 0;
  void* pIter = NULL;
  while (true) {
    pIter = taosHashIterate(mStreamMgmt.streamMap, pIter);
    if (NULL == pIter) {
      return;
    }

    streamId = *(int64_t*)taosHashGetKey(pIter, NULL);
    pStatus = (SStmStatus*)pIter;

    if (NULL == pStatus->triggerTask) {
      mstsWarn("no trigger task recored, deployTimes:%" PRId64, pStatus->deployTimes);
      msmStopStreamByError(streamId, pStatus, TSDB_CODE_MND_STREAM_TASK_LOST, pCtx->currTs);
      continue;
    }
    
    trigReaderNum = taosArrayGetSize(pStatus->trigReaders);
    if (pStatus->trigReaderNum != trigReaderNum) {
      mstsWarn("trigReaderNum %d mismatch with expected %d", trigReaderNum, pStatus->trigReaderNum);
      msmStopStreamByError(streamId, pStatus, TSDB_CODE_MND_STREAM_TASK_LOST, pCtx->currTs);
      continue;
    }

    calcReaderNum = MST_LIST_SIZE(pStatus->calcReaders);
    if (pStatus->calcReaderNum != calcReaderNum) {
      mstsWarn("calcReaderNum %d mismatch with expected %d", calcReaderNum, pStatus->calcReaderNum);
      msmStopStreamByError(streamId, pStatus, TSDB_CODE_MND_STREAM_TASK_LOST, pCtx->currTs);
      continue;
    }

    if (pStatus->runnerNum > 0 && pStatus->runnerDeploys == 0) {
      mstsWarn("no runner task recorded while %d runners per deploy expected", pStatus->runnerNum);
      msmStopStreamByError(streamId, pStatus, TSDB_CODE_MND_STREAM_TASK_LOST, pCtx->currTs);
      continue;
    }

    for (int32_t i = 0; i < pStatus->runnerDeploys; ++i) {
      runnerNum = taosArrayGetSize(pStatus->runners[i]);
      if (runnerNum != pStatus->runnerNum) {
        mstsWarn("runner deploy %d runnerNum %d mismatch with expected %d", i, runnerNum, pStatus->runnerNum);
        msmStopStreamByError(streamId, pStatus, TSDB_CODE_MND_STREAM_TASK_LOST, pCtx->currTs);
        continue;
      }
    }
  }
}

int32_t msmWatchHandleEnding(SStmGrpCtx* pCtx, bool watchError) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t minVal = watchError ? 0 : 1;
  int64_t nextTaskId;

  if (0 != atomic_val_compare_exchange_8(&mStreamMgmt.watch.ending, 0, 1)) {
    return code;
  }

  while (atomic_load_32(&mStreamMgmt.watch.processing) > minVal) {
    (void)sched_yield();
  }

  (void)mstWaitLock(&mStreamMgmt.actionQLock, false);
  if (watchError) {
    taosHashClear(mStreamMgmt.vgroupMap);
    taosHashClear(mStreamMgmt.snodeMap);
    taosHashClear(mStreamMgmt.taskMap);
    taosHashClear(mStreamMgmt.streamMap);
    mstInfo("watch error happens, clear all maps");
    goto _exit;
  }

  if (0 == atomic_load_8(&mStreamMgmt.watch.taskRemains)) {
    mstInfo("no stream tasks remain during watch state");
    goto _exit;
  }

  msmWatchCheckStreamMap(pCtx);

_exit:

  taosHashCleanup(mStreamMgmt.watch.deferredStreams);
  mStreamMgmt.watch.deferredStreams = NULL;
  nextTaskId = atomic_add_fetch_64(&mStreamMgmt.lastTaskId, 100000);

  mstInfo("watch state end, new taskId begin from:%" PRIx64, nextTaskId);

  msmSetInitRuntimeState(MND_STM_STATE_NORMAL);
  taosWUnLockLatch(&mStreamMgmt.actionQLock);

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


int32_t msmWatchHandleHbMsg(SStmGrpCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStreamHbMsg* pReq = pCtx->pReq;

  (void)atomic_add_fetch_32(&mStreamMgmt.watch.processing, 1);
  
  if (atomic_load_8(&mStreamMgmt.watch.ending)) {
    goto _exit;
  }

  TAOS_CHECK_EXIT(msmCheckUpdateDnodeTs(pCtx));
  if (GOT_SNODE(pReq->snodeId)) {
    TAOS_CHECK_EXIT(msmUpdateSnodeUpTs(pCtx));
  }

  if (taosArrayGetSize(pReq->pStreamStatus) > 0) {
    atomic_store_8(&mStreamMgmt.watch.taskRemains, 1);
    TAOS_CHECK_EXIT(msmWatchHandleStatusUpdate(pCtx));
  }

  if ((pCtx->currTs - MND_STREAM_GET_LAST_TS(STM_EVENT_ACTIVE_BEGIN)) > MST_SHORT_ISOLATION_DURATION) {
    TAOS_CHECK_EXIT(msmWatchHandleEnding(pCtx, false));
  }

_exit:

  atomic_sub_fetch_32(&mStreamMgmt.watch.processing, 1);
  
  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

    (void)msmWatchHandleEnding(pCtx, true);
  }

  return code;
}

int32_t msmGetTrigOReaderSize(SArray* pOReaders) {
  int32_t listSize = taosArrayGetSize(pOReaders);
  int32_t totalSize = 0;
  
  for (int32_t i = 0; i < listSize; ++i) {
    SArray* pList = taosArrayGetP(pOReaders, i);
    totalSize += taosArrayGetSize(pList);
  }

  return totalSize;
}

SStmTaskStatus* msmNextTrigOReader(SArray* blocks, SStmOrigReaderIter* iter) {
  while (iter->block < taosArrayGetSize(blocks)) {
    SArray* readers = taosArrayGetP(blocks, iter->block);
    if (iter->offset < taosArrayGetSize(readers)) return taosArrayGet(readers, iter->offset++);
    ++iter->block;
    iter->offset = 0;
  }
  return NULL;
}

int32_t msmEnsureGetOReaderList(int64_t streamId, SStmStatus* pStatus, SArray** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (NULL == pStatus->trigOReaders) {
    pStatus->trigOReaders = taosArrayInit(10, POINTER_BYTES);
    TSDB_CHECK_NULL(pStatus->trigOReaders, code, lino, _exit, terrno);
  }

  while (true) {
    SArray** ppOReaderList = taosArrayGetLast(pStatus->trigOReaders);

    if (NULL == ppOReaderList || (*ppOReaderList)->size >= (*ppOReaderList)->capacity) {
      SArray* pOReaderList = taosArrayInit(MST_ORIGINAL_READER_LIST_SIZE, sizeof(SStmTaskStatus));
      TSDB_CHECK_NULL(pOReaderList, code, lino, _exit, terrno);

      TSDB_CHECK_NULL(taosArrayPush(pStatus->trigOReaders, &pOReaderList), code, lino, _exit, terrno);
      continue;
    }

    *ppRes = *ppOReaderList;
    break;
  }

_exit:

  if (code) {
    mstsError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

// A heartbeat can outlive the runtime that received it while waiting for a reader plan or provider.
static SStmStatus* msmReaderRequestOwner(const SStmTaskStatusMsg* task) {
  int64_t          key[] = {task->streamId, task->taskId};
  SStmTaskStatus** found = taosHashGet(mStreamMgmt.taskMap, key, sizeof(key));
  if (!found || (*found)->id.seriousId != task->seriousId || (*found)->id.nodeId != task->nodeId ||
      (*found)->type != task->type)
    return NULL;
  SStmStatus* live = (*found)->pStream;
  return live && !atomic_load_8(&live->stopped) ? live : NULL;
}

static bool msmReaderStreamIsCurrent(SMnode* mnode, SStreamObj* row, int32_t snodeId) {
  if (!row || atomic_load_8(&row->userStopped) || atomic_load_8(&row->userDropped) ||
      atomic_load_32(&row->mainSnodeId) != snodeId)
    return false;
  SStreamObj* current = NULL;
  bool        same = mndAcquireStream(mnode, row->name, &current) == 0 && current == row;
  if (current) mndReleaseStream(mnode, current);
  return same;
}

static void msmResumeReaderRequest(SStmGrpCtx* ctx, int64_t epoch) {
  msmLockActionBuild();
  ctx->runtimeExpired = !msmSameBuildEpoch(epoch);
}

static SStreamObj* msmAcquireTriggerReaderStream(SMnode* mnode, SStmStatus* owner, const SStmTaskStatusMsg* task) {
  SStreamObj* row = NULL;
  if (owner && owner->triggerTask && owner->triggerTask->id.taskId == task->taskId && owner->streamName &&
      mndAcquireStream(mnode, owner->streamName, &row) == 0 && row && row->pCreate->streamId == task->streamId) {
    return row;
  }
  if (row) mndReleaseStream(mnode, row);
  return NULL;
}

int32_t msmCheckDeployTrigReader(SStmGrpCtx* pCtx, SStmStatus* pStatus, SStmTaskStatusMsg* pTask, int32_t vgId,
                                 SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  bool    readerExists = false;
  int64_t streamId = pTask->streamId;

  int32_t readerNum = taosArrayGetSize(pStatus->trigReaders);
  for (int32_t i = 0; i < readerNum; ++i) {
    SStmTaskStatus* pReader = (SStmTaskStatus*)taosArrayGet(pStatus->trigReaders, i);
    if (pReader->id.nodeId == vgId) {
      readerExists = true;
      break;
    }
  }

  if (!readerExists && pStatus->trigOReaders != NULL) {
    SStmOrigReaderIter iter = {0};
    SStmTaskStatus*    pReader;
    while ((pReader = msmNextTrigOReader(pStatus->trigOReaders, &iter))) {
      if (STREAM_IS_REF_EXT_SOURCE(pReader->flags)) continue;
      if (pReader->id.nodeId == vgId) {
        readerExists = true;
        break;
      }
    }
  }

  if (!readerExists) {
    SArray* pReaderList = NULL;
    TAOS_CHECK_EXIT(msmEnsureGetOReaderList(streamId, pStatus, &pReaderList));
    
    SStmTaskStatus* pState = taosArrayReserve(pReaderList, 1);
    TSDB_CHECK_NULL(pState, code, lino, _exit, terrno);
    TAOS_CHECK_EXIT(
        msmTDAddSingleTrigReader(pCtx, pState, vgId, pStatus, streamId, msmGetTrigExtSpec(pStream), false, pStream));

    TAOS_CHECK_EXIT(msmSTAddToTaskMap(pCtx, streamId, NULL, NULL, pState));
    TAOS_CHECK_EXIT(msmSTAddToVgroupMap(pCtx, streamId, NULL, NULL, pState, true));
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

typedef struct {
  SVGroupHashInfo route;
  char*           dbName;  // Borrowed from the request until publication completes.
  int16_t         hashPrefix, hashSuffix;
  int8_t          hashMethod;
} SStmTriggerVgroup;

// Destroy the full routing map before returning to the locked publication path.
static int32_t msmPrepareTriggerVgroups(SMnode* mnode, SArray* tables, SArray** vgIds, SSHashObj** unique) {
  int32_t    code = TSDB_CODE_SUCCESS, lino = 0;
  int32_t    count = taosArrayGetSize(tables);
  SSHashObj* dbVgroups = NULL;
  TAOS_CHECK_EXIT(mstBuildDBVgroupsMap(mnode, &dbVgroups));
  *vgIds = taosArrayInit(count, sizeof(int32_t));
  TSDB_CHECK_NULL(*vgIds, code, lino, _exit, terrno);
  *unique = tSimpleHashInit(count, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  TSDB_CHECK_NULL(*unique, code, lino, _exit, terrno);
  for (int32_t i = 0; i < count; ++i) {
    SStreamDbTableName* name = taosArrayGet(tables, i);
    int32_t             vgId = 0;
    TAOS_CHECK_EXIT(mstGetTableVgId(dbVgroups, name->dbFName, name->tbName, &vgId));
    TSDB_CHECK_NULL(taosArrayPush(*vgIds, &vgId), code, lino, _exit, terrno);
    SStmTriggerVgroup selected = {.dbName = name->dbFName};
    TAOS_CHECK_EXIT(tSimpleHashPut(*unique, &vgId, sizeof(vgId), &selected, sizeof(selected)));
  }
  // Save the selected ranges from the same map that resolved the tables. A concurrent
  // catalog change then needs only indexed checks of the selected vgroups and databases.
  int32_t        iter = 0;
  SDBVgHashInfo* db = NULL;
  while ((db = tSimpleHashIterate(dbVgroups, db, &iter))) {
    if (!db->vgSorted) continue;  // mstGetTableVgId sorted only the requested databases.
    for (int32_t i = 0; i < taosArrayGetSize(db->vgArray); ++i) {
      SVGroupHashInfo*   route = taosArrayGet(db->vgArray, i);
      SStmTriggerVgroup* selected = tSimpleHashGet(*unique, &route->vgId, sizeof(route->vgId));
      if (!selected) continue;
      selected->route = *route;
      selected->hashPrefix = db->hashPrefix;
      selected->hashSuffix = db->hashSuffix;
      selected->hashMethod = db->hashMethod;
    }
  }
_exit:
  mstDestroyDbVgroupsHash(dbVgroups);
  return code;
}

static bool msmTriggerPlacementIsCurrent(SMnode* mnode, SSHashObj* vgroups, bool checkVgs, bool checkDbs) {
  if (!checkVgs && !checkDbs) return true;
  int32_t            iter = 0;
  SStmTriggerVgroup* selected = NULL;
  while ((selected = tSimpleHashIterate(vgroups, selected, &iter))) {
    if (checkVgs) {
      SVgObj* vg = mndAcquireVgroup(mnode, selected->route.vgId);
      if (!vg) return false;
      sdbReadLock(mnode->pSdb, SDB_VGROUP);
      bool same = vg->hashBegin == selected->route.hashBegin && vg->hashEnd == selected->route.hashEnd &&
                  strcmp(vg->dbName, selected->dbName) == 0;
      sdbUnLock(mnode->pSdb, SDB_VGROUP);
      mndReleaseVgroup(mnode, vg);
      if (!same) return false;
    }
    if (checkDbs) {
      SDbObj* db = mndAcquireDb(mnode, selected->dbName);
      if (!db) return false;
      bool same = db->cfg.hashMethod == selected->hashMethod && db->cfg.hashPrefix == selected->hashPrefix &&
                  db->cfg.hashSuffix == selected->hashSuffix;
      mndReleaseDb(mnode, db);
      if (!same) return false;
    }
  }
  return true;
}

int32_t msmDeployTriggerOrigReader(SStmGrpCtx* pCtx, SStmTaskStatusMsg* pTask) {
  int32_t code = TSDB_CODE_SUCCESS, finalCode = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  int64_t streamId = pTask->streamId;
  SArray* pTbs = pTask->pMgmtReq->cont.pReqs;
  int32_t tbNum = taosArrayGetSize(pTbs);
  SStreamObj*    pStream = NULL;
  SStreamMgmtRsp rsp = {0};
  rsp.reqId = pTask->pMgmtReq->reqId;
  rsp.header.msgType = STREAM_MSG_ORIGTBL_READER_INFO;
  int32_t iter = 0;
  void* p = NULL;
  SSHashObj* pVgs = NULL;
  SStreamMgmtReq* pMgmtReq = NULL;
  int8_t          stopped = 0;

  if (NULL == pCtx->pRsp->rsps.rspList) {
    pCtx->pRsp->rsps.rspList = taosArrayInit(2, sizeof(SStreamMgmtRsp));
    TSDB_CHECK_NULL(pCtx->pRsp->rsps.rspList, finalCode, lino, _final, terrno);
  }
  
  TSWAP(pTask->pMgmtReq, pMgmtReq);
  rsp.task = *(SStreamTask*)pTask;

  SStmStatus* pStatus = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStatus) {
    mstsError("stream not deployed, remainStreams:%d", taosHashGetSize(mStreamMgmt.streamMap));
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_NOT_RUNNING);
  }

  stopped = atomic_load_8(&pStatus->stopped);
  if (stopped) {
    msttInfo("stream stopped %d, ignore deploy trigger reader", stopped);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_STOPPED);
  }

  if (tbNum <= 0) {
    mstsWarn("empty table list in origReader req, array:%p", pTbs);
    goto _exit;
  }

  if (msmReaderRequestOwner(pTask) != pStatus) TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_NOT_RUNNING);
  pStream = msmAcquireTriggerReaderStream(pCtx->pMnode, pStatus, pTask);
  TSDB_CHECK_NULL(pStream, code, lino, _exit, TSDB_CODE_MND_STREAM_NOT_RUNNING);
  int32_t snode = atomic_load_32(&pStream->mainSnodeId);
  int64_t epoch = mStreamMgmt.stat.activeTimes;
  int64_t version = atomic_load_64(&pStatus->buildVersion);
  int64_t generation = msmPublishedTaskGeneration(pStatus);
  int64_t lastReqId = pStatus->lastTrigMgmtReqId;
  SSdb*   sdb = pCtx->pMnode->pSdb;
  int64_t vgVersion = sdbGetTableVer(sdb, SDB_VGROUP);
  int64_t dbVersion = sdbGetTableVer(sdb, SDB_DB);

  msmUnlockActionBuild();
  code = msmPrepareTriggerVgroups(pCtx->pMnode, pTbs, &rsp.cont.vgIds, &pVgs);
  msmResumeReaderRequest(pCtx, epoch);

  pStatus = pCtx->runtimeExpired ? NULL : msmReaderRequestOwner(pTask);
  if (!pStatus || atomic_load_64(&pStatus->buildVersion) != version ||
      msmPublishedTaskGeneration(pStatus) != generation || !msmReaderStreamIsCurrent(pCtx->pMnode, pStream, snode)) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_NOT_RUNNING);
  }
  TAOS_CHECK_EXIT(code);
  if ((pStatus->lastTrigMgmtReqId != lastReqId && pStatus->lastTrigMgmtReqId != rsp.reqId) ||
      !msmTriggerPlacementIsCurrent(pCtx->pMnode, pVgs, sdbGetTableVer(sdb, SDB_VGROUP) != vgVersion,
                                    sdbGetTableVer(sdb, SDB_DB) != dbVersion)) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_NOT_AVAILABLE);
  }
  if (rsp.reqId != pStatus->lastTrigMgmtReqId) {
    while (true) {
      p = tSimpleHashIterate(pVgs, p, &iter);
      if (NULL == p) {
        break;
      }

      SStmTriggerVgroup* selected = p;
      code = msmCheckDeployTrigReader(pCtx, pStatus, pTask, selected->route.vgId, pStream);
      if (code) {
        // Partially registered readers must be cleaned up before another deployment attempt.
        msmStopStreamByError(streamId, pStatus, code, pCtx->currTs);
        TAOS_CHECK_EXIT(code);
      }
    }
    atomic_store_64(&pStatus->lastTrigMgmtReqId, rsp.reqId);
  }

  int32_t vgNum = msmGetTrigOReaderSize(pStatus->trigOReaders);
  rsp.cont.readerList = taosArrayInit(vgNum, sizeof(SStreamTaskAddr));
  TSDB_CHECK_NULL(rsp.cont.readerList, code, lino, _exit, terrno);

  SStreamTaskAddr    addr;
  SStmOrigReaderIter readers = {0};
  SStmTaskStatus*    pOTask;
  while ((pOTask = msmNextTrigOReader(pStatus->trigOReaders, &readers))) {
    if (STREAM_IS_REF_EXT_SOURCE(pOTask->flags)) continue;
    addr.taskId = pOTask->id.taskId;
    addr.nodeId = pOTask->id.nodeId;
    addr.epset = mndGetVgroupEpsetById(pCtx->pMnode, pOTask->id.nodeId);
    TSDB_CHECK_NULL(taosArrayPush(rsp.cont.readerList, &addr), code, lino, _exit, terrno);
    mstsDebug("otrigReader src added to trigger's virtual orig readerList, TASK:%" PRIx64 " nodeId:%d", addr.taskId,
              addr.nodeId);
  }

_exit:
  tFreeSStreamMgmtReq(pMgmtReq);
  taosMemoryFree(pMgmtReq);

  tSimpleHashCleanup(pVgs);
  if (pStream) mndReleaseStream(pCtx->pMnode, pStream);

  if (code) {
    rsp.code = code;
    
    TSDB_CHECK_NULL(taosArrayPush(pCtx->pRsp->rsps.rspList, &rsp), finalCode, lino, _final, terrno);

    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    TSDB_CHECK_NULL(taosArrayPush(pCtx->pRsp->rsps.rspList, &rsp), finalCode, lino, _final, terrno);
  }

_final:

  if (finalCode) {
    tFreeSStreamMgmtRsp(&rsp);
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code)); 
    msmHandleStreamTaskErr(pCtx, STM_ERR_PROCESSING_ERR, pTask);
  }

  return finalCode;
}

static SStmTaskStatus* msmFindCalcReader(SStmStatus* live, int32_t vgId, int64_t uid) {
  for (SListNode* n = live->calcReaders ? listHead(live->calcReaders) : NULL; n; n = TD_DLIST_NODE_NEXT(n)) {
    SStmTaskStatus* reader = (SStmTaskStatus*)n->data;
    if (reader->id.nodeId == vgId && reader->id.uid == uid) return reader;
  }
  return NULL;
}

typedef struct {
  int64_t          uid;
  SStreamTaskAddr* addr;  // The response owns the address slot throughout preparation.
} SStmMissingCalcReader;

static int32_t msmPrepareRequestedCalcReaders(SStmGrpCtx* build, SStreamObj* stream, SStmStatus* snapshot,
                                              SArray* missing) {
  int64_t                streamId = stream->pCreate->streamId;
  int32_t                scanIdx = -1;
  SStreamCalcScan*       scan = NULL;
  SStreamExtTriggerSpec* spec = NULL;
  int64_t                previousUid = 0;
  for (int32_t i = 0; i < taosArrayGetSize(missing); ++i) {
    SStmMissingCalcReader* item = taosArrayGet(missing, i);
    if (!scan || previousUid != item->uid) {
      scan = NULL;
      for (scanIdx = 0; scanIdx < taosArrayGetSize(snapshot->pCreate->calcScanPlanList); ++scanIdx) {
        SStreamCalcScan* candidate = taosArrayGet(snapshot->pCreate->calcScanPlanList, scanIdx);
        int64_t          uid = 0;
        TAOS_CHECK_RETURN(mstGetScanUidFromPlan(streamId, candidate->scanPlan, &uid));
        if (uid && uid == item->uid) {
          scan = candidate;
          break;
        }
      }
      if (!scan) return TSDB_CODE_STREAM_INTERNAL_ERROR;
      previousUid = item->uid;
      spec = msmResolveCalcExtSpec(stream, scan);
      if (!spec && STREAM_IS_REF_EXT_SOURCE(stream->flags)) return TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
    }
    SStmTaskStatus reader = {.id = {.nodeId = item->addr->nodeId, .taskIdx = scanIdx}, .type = STREAM_READER_TASK};
#ifdef TD_ENTERPRISE
    if (spec && reader.id.nodeId == SNODE_HANDLE) reader.id.nodeId = stream->mainSnodeId;
#endif
    item->addr->nodeId = reader.id.nodeId;
    item->addr->epset = spec ? mndGetDnodeEpsetById(build->pMnode, reader.id.nodeId)
                             : mndGetVgroupEpsetById(build->pMnode, reader.id.nodeId);
    if (!item->addr->epset.numOfEps) return TSDB_CODE_MND_STREAM_NOT_AVAILABLE;
    TAOS_CHECK_RETURN(msmRememberEndpoint(build, spec ? SDB_DNODE : SDB_VGROUP, reader.id.nodeId, &item->addr->epset));
    TAOS_CHECK_RETURN(msmQueueCalcReader(build, &reader, snapshot, streamId, scan->scanPlan, scan, spec, stream));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t msmDeployRequestedCalcReaders(SStmGrpCtx* ctx, SStmTaskStatusMsg* task, SStmStatus* live,
                                             SArray* missing) {
  int64_t streamId = task->streamId;
  if (msmReaderRequestOwner(task) != live) return TSDB_CODE_MND_STREAM_NOT_RUNNING;
  if (!live->deployInput) TAOS_CHECK_RETURN(msmInitDeployInput(live));
  SStmDeployInput* input = live->deployInput;
  (void)atomic_add_fetch_32(&input->refs, 1);
  int64_t              epoch = mStreamMgmt.stat.activeTimes;
  int64_t              version = atomic_load_64(&live->buildVersion);
  int64_t              generation = msmPublishedTaskGeneration(live);
  SStmStatus           snapshot = {.pCreate = input->create,
                                   .runnerNum = live->runnerNum,
                                   .runnerDeploys = live->runnerDeploys,
                                   .runnerReplica = atomic_load_32(&live->runnerReplica)};
  SStmDeployDraft      draft = {0};
  SStmGrpCtx           build = {.pMnode = ctx->pMnode, .currTs = ctx->currTs, .draft = &draft};
  SStreamObj*          row = NULL;
  SStreamObj           stream = {0};
  SStmPlacementVersion placement = msmPlacementVersion(ctx->pMnode->pSdb);

  msmUnlockActionBuild();
  int32_t code = mndAcquireStream(build.pMnode, input->name, &row);
  if (!code && (!row || row->pCreate->streamId != streamId)) code = TSDB_CODE_MND_STREAM_NOT_RUNNING;
  if (!code) {
    msmSnapshotStreamRow(row, &stream);
    draft.tasks = taosArrayInit(taosArrayGetSize(missing), sizeof(SStmTaskToDeployExt));
    code = draft.tasks ? msmPrepareRequestedCalcReaders(&build, &stream, &snapshot, missing) : terrno;
  }
  msmResumeReaderRequest(ctx, epoch);
  live = ctx->runtimeExpired ? NULL : msmReaderRequestOwner(task);
  if (!live || live->deployInput != input || atomic_load_64(&live->buildVersion) != version ||
      msmPublishedTaskGeneration(live) != generation || !msmReaderStreamIsCurrent(ctx->pMnode, row, stream.mainSnodeId))
    code = TSDB_CODE_MND_STREAM_NOT_RUNNING;
  if (!code) code = msmValidateDraftPlacement(ctx->pMnode, &draft, placement, &stream);
  if (!code && !live->calcReaders) {
    live->calcReaders = tdListNew(sizeof(SStmTaskStatus));
    if (!live->calcReaders) code = terrno;
  }
  for (int32_t i = 0; !code && i < taosArrayGetSize(missing); ++i) {
    SStmMissingCalcReader* item = taosArrayGet(missing, i);
    SStmTaskToDeployExt*   entry = taosArrayGet(draft.tasks, i);
    SStreamTask*           deploy = &entry->deploy.task;
    SStmTaskStatus*        reader = msmFindCalcReader(live, deploy->nodeId, item->uid);
    if (!reader) {
      reader = tdListReserve(live->calcReaders);
      if (!reader) {
        code = terrno;
        break;
      }
      reader->type = deploy->type;
      reader->status = STREAM_STATUS_UNDEPLOYED;
      reader->pStream = live;
      reader->lastUpTs = ctx->currTs;
      reader->id.uid = item->uid;
      reader->id.nodeId = deploy->nodeId;
      reader->id.taskIdx = deploy->taskIdx;
      reader->id.taskId = deploy->taskId = msmAssignTaskId();
      reader->id.seriousId = deploy->seriousId = msmAssignTaskSeriousId();
      code = msmSTAddToTaskMap(ctx, streamId, NULL, NULL, reader);
      if (!code) code = msmSTAddToVgroupMapImpl(streamId, reader, false);
      if (!code) code = msmPublishPreparedReader(entry);
      if (code) {
        msmStopStreamByError(streamId, live, code, ctx->currTs);
        break;
      }
    }
    item->addr->taskId = reader->id.taskId;
  }
  taosHashCleanup(draft.endpoints);
  taosArrayDestroyEx(draft.tasks, mstDestroySStmTaskToDeployExt);
  if (row) mndReleaseStream(build.pMnode, row);
  mstReleaseDeployInput(input);
  return code;
}

int32_t msmDeployRunnerOrigReader(SStmGrpCtx* pCtx, SStmTaskStatusMsg* pTask) {
  int32_t code = TSDB_CODE_SUCCESS, finalCode = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t vgId = 0;
  int64_t streamId = pTask->streamId;
  SArray* pReqs = pTask->pMgmtReq->cont.pReqs;
  int32_t reqNum = taosArrayGetSize(pReqs);
  SStreamOReaderDeployReq* pReq = NULL;
  SStreamOReaderDeployRsp* pRsp = NULL;
  SStreamMgmtRsp rsp = {0};
  rsp.reqId = pTask->pMgmtReq->reqId;
  rsp.header.msgType = STREAM_MSG_RUNNER_ORIGTBL_READER;
  SStreamMgmtReq* pMgmtReq = NULL;
  int8_t stopped = 0;
  int32_t vgNum = 0;
  SStreamTaskAddr* pAddr = NULL;
  SArray*          missing = NULL;
  SSHashObj*       firstAddrBySource = NULL;  // (uid, requested vgId) -> first response slot.

  if (NULL == pCtx->pRsp->rsps.rspList) {
    pCtx->pRsp->rsps.rspList = taosArrayInit(2, sizeof(SStreamMgmtRsp));
    TSDB_CHECK_NULL(pCtx->pRsp->rsps.rspList, finalCode, lino, _final, terrno);
  }
  
  TSWAP(pTask->pMgmtReq, pMgmtReq);
  rsp.task = *(SStreamTask*)pTask;

  SStmStatus* pStatus = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStatus) {
    mstsError("stream not deployed, remainStreams:%d", taosHashGetSize(mStreamMgmt.streamMap));
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_NOT_RUNNING);
  }

  stopped = atomic_load_8(&pStatus->stopped);
  if (stopped) {
    msttInfo("stream stopped %d, ignore deploy trigger reader, vgId:%d", stopped, vgId);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_STOPPED);
  }

  if (mstRunnerSnapshotUnknown(pStatus)) {
    TAOS_CHECK_EXIT(mstPostRunnerSnapshotRedeploy(mStreamMgmt.actionQ, streamId, pStatus));
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_NOT_RUNNING);
  }

  if (reqNum <= 0) {
    // Empty req list means the request was corrupted before it reached the
    // mnode. Fail the request explicitly so the snode side can log the real
    // error instead of interpreting a success response without deploy records
    // as a broken redeploy chain.
    mstsWarn("empty req list in origReader req, array:%p", pReqs);
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }

  rsp.cont.execRspList = taosArrayInit_s(sizeof(SStreamOReaderDeployRsp), reqNum);
  TSDB_CHECK_NULL(rsp.cont.execRspList, code, lino, _exit, terrno);

  for (int32_t i = 0; i < reqNum; ++i) {
    pReq = (SStreamOReaderDeployReq*)taosArrayGet(pReqs, i);
    pRsp = (SStreamOReaderDeployRsp*)taosArrayGet(rsp.cont.execRspList, i);
    pRsp->execId = pReq->execId;
    vgNum = taosArrayGetSize(pReq->vgIds);
    pRsp->vgList = taosArrayInit_s(sizeof(SStreamTaskAddr), vgNum);
    TSDB_CHECK_NULL(pRsp->vgList, code, lino, _exit, terrno);
    if (vgNum && !firstAddrBySource) {
      firstAddrBySource = tSimpleHashInit(vgNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
      TSDB_CHECK_NULL(firstAddrBySource, code, lino, _exit, terrno);
    }

    for (int32_t n = 0; n < vgNum; ++n) {
      vgId = *(int32_t*)taosArrayGet(pReq->vgIds, n);
      pAddr = taosArrayGet(pRsp->vgList, n);
      pAddr->nodeId = vgId;
      int64_t key[] = {pReq->uid, vgId};
      if (tSimpleHashGet(firstAddrBySource, key, sizeof(key))) continue;
      TAOS_CHECK_EXIT(tSimpleHashPut(firstAddrBySource, key, sizeof(key), &pAddr, sizeof(pAddr)));
      SStmTaskStatus* reader = msmFindCalcReader(pStatus, vgId, pReq->uid);
      if (reader) {
        pAddr->taskId = reader->id.taskId;
      } else {
        if (!missing) {
          missing = taosArrayInit(vgNum, sizeof(SStmMissingCalcReader));
          TSDB_CHECK_NULL(missing, code, lino, _exit, terrno);
        }
        SStmMissingCalcReader item = {.uid = pReq->uid, .addr = pAddr};
        TSDB_CHECK_NULL(taosArrayPush(missing, &item), code, lino, _exit, terrno);
      }
    }
  }
  if (missing) TAOS_CHECK_EXIT(msmDeployRequestedCalcReaders(pCtx, pTask, pStatus, missing));
  for (int32_t i = 0; i < reqNum; ++i) {
    pReq = taosArrayGet(pReqs, i);
    pRsp = taosArrayGet(rsp.cont.execRspList, i);
    for (int32_t n = 0; n < taosArrayGetSize(pRsp->vgList); ++n) {
      pAddr = taosArrayGet(pRsp->vgList, n);
      if (pAddr->epset.numOfEps) continue;
      int64_t           key[] = {pReq->uid, *(int32_t*)taosArrayGet(pReq->vgIds, n)};
      SStreamTaskAddr** first = tSimpleHashGet(firstAddrBySource, key, sizeof(key));
      if (*first != pAddr) {
        *pAddr = **first;
      } else {
        pAddr->epset = mndGetVgroupEpsetById(pCtx->pMnode, pAddr->nodeId);
      }
    }
  }

_exit:
  tSimpleHashCleanup(firstAddrBySource);
  taosArrayDestroy(missing);
  tFreeSStreamMgmtReq(pMgmtReq);
  taosMemoryFree(pMgmtReq);

  if (code) {
    rsp.code = code;
    
    TSDB_CHECK_NULL(taosArrayPush(pCtx->pRsp->rsps.rspList, &rsp), finalCode, lino, _final, terrno);
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    TSDB_CHECK_NULL(taosArrayPush(pCtx->pRsp->rsps.rspList, &rsp), finalCode, lino, _final, terrno);
  }


_final:

  if (finalCode) {
    tFreeSStreamMgmtRsp(&rsp);
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code)); 
    msmHandleStreamTaskErr(pCtx, STM_ERR_PROCESSING_ERR, pTask);
  }

  return finalCode;
}

static int32_t msmHandleTriggerExtSourceIdentity(SStmGrpCtx* pCtx, SStmTaskStatusMsg* pTask) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  int32_t         lookupCode = TSDB_CODE_SUCCESS;
  int64_t         streamId = pTask->streamId;
  SStreamMgmtReq *pMgmtReq = NULL;
  SStreamExtSourceIdentityReq *pIdentityReq = NULL;
  SStreamMgmtRsp  rsp = {0};

  TSDB_CHECK_NULL(pTask, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pTask->pMgmtReq, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  rsp.reqId = pTask->pMgmtReq->reqId;
  rsp.header.msgType = STREAM_MSG_EXT_SOURCE_IDENTITY_INFO;
  if (pCtx->pRsp->rsps.rspList == NULL) {
    pCtx->pRsp->rsps.rspList = taosArrayInit(2, sizeof(SStreamMgmtRsp));
    TSDB_CHECK_NULL(pCtx->pRsp->rsps.rspList, code, lino, _exit, terrno);
  }

  TSWAP(pTask->pMgmtReq, pMgmtReq);
  TSDB_CHECK_NULL(pMgmtReq, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  rsp.task = *(SStreamTask*)pTask;
  rsp.cont.pExtSourceIdentity = taosMemoryCalloc(1, sizeof(SStreamExtSourceIdentityRsp));
  TSDB_CHECK_NULL(rsp.cont.pExtSourceIdentity, code, lino, _exit, terrno);
  TSDB_CHECK_NULL(pMgmtReq->cont.pExtSourceIdentity, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  TAOS_CHECK_EXIT(tCloneSStreamExtSourceIdentityReq(pMgmtReq->cont.pExtSourceIdentity, &pIdentityReq));
  rsp.cont.pExtSourceIdentity->req = *pIdentityReq;
  taosMemoryFree(pIdentityReq);
  pIdentityReq = NULL;

#ifdef TD_ENTERPRISE
  SExtSourceObj *pSource = mndAcquireExtSource(pCtx->pMnode, pMgmtReq->cont.pExtSourceIdentity->sourceName);
  if (pSource == NULL) {
    lookupCode = TSDB_CODE_EXT_SOURCE_NOT_FOUND;
    mstsWarn("external source identity lookup failed, source:%s, code:%s",
             pMgmtReq->cont.pExtSourceIdentity->sourceName, tstrerror(lookupCode));
  } else {
    rsp.cont.pExtSourceIdentity->sourceType = pSource->type;
    rsp.cont.pExtSourceIdentity->sourceConfigVersion = pSource->metaVersion;
    mndReleaseExtSource(pCtx->pMnode, pSource);
    mstsDebug("external source identity resolved, source:%s, type:%d, version:%" PRId64,
              rsp.cont.pExtSourceIdentity->req.sourceName, rsp.cont.pExtSourceIdentity->sourceType,
              rsp.cont.pExtSourceIdentity->sourceConfigVersion);
  }
#else
  lookupCode = TSDB_CODE_OPS_NOT_SUPPORT;
  mstsWarn("external source identity lookup is unavailable in community build, source:%s",
           pMgmtReq->cont.pExtSourceIdentity->sourceName);
#endif
  rsp.cont.pExtSourceIdentity->code = lookupCode;

_exit:
  tFreeSStreamExtSourceIdentityReq(pIdentityReq);
  taosMemoryFree(pIdentityReq);
  tFreeSStreamMgmtReq(pMgmtReq);
  taosMemoryFree(pMgmtReq);
  if (code != TSDB_CODE_SUCCESS) {
    rsp.code = code;
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  if (pCtx->pRsp->rsps.rspList != NULL) {
    int32_t pushCode = taosArrayPush(pCtx->pRsp->rsps.rspList, &rsp) == NULL ? terrno : TSDB_CODE_SUCCESS;
    if (pushCode != TSDB_CODE_SUCCESS) {
      tFreeSStreamMgmtRsp(&rsp);
      msmHandleStreamTaskErr(pCtx, STM_ERR_PROCESSING_ERR, pTask);
      return pushCode;
    }
  }
  return code;
}


#ifdef TD_ENTERPRISE
int32_t msmBuildExtOrigTblReaderSpec(const SExtSourceObj *pSource,
                                     const SStreamExtOrigTblReaderReq *pReq,
                                     const SExtTableMeta *pMeta,
                                     SStreamExtTriggerSpec **ppSpec,
                                     SStreamExtOrigTblReaderRsp *pRsp);

static void msmInitExtOrigTblReaderRsp(const SStreamExtOrigTblReaderReq *pReq, SStreamExtOrigTblReaderRsp *pRsp) {
  pRsp->registryEntryId = pReq->registryEntryId;
  pRsp->deploymentGeneration = pReq->deploymentGeneration;
  pRsp->protocolVersion = pReq->protocolVersion;
  pRsp->capabilities = pReq->capabilities;
  pRsp->identity = pReq->identity;
}

static int32_t msmValidateExtOrigTblReaderCapabilities(int64_t streamId, const SStreamExtOrigTblReaderReq *pReq) {
  if (pReq == NULL) return TSDB_CODE_INVALID_PARA;

  bool patchRequested = (pReq->capabilities & STREAM_EXT_ORIGTBL_READER_CAP_PATCH) != 0;
  if (patchRequested) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t required = STREAM_EXT_ORIGTBL_READER_CAP_CREATE | STREAM_EXT_ORIGTBL_READER_CAP_START;
  if ((pReq->capabilities & required) != required) {
    mstsWarn("external original reader capability missing, stream:%" PRId64 ", registry:%" PRId64
             ", capabilities:%d, required:%d",
             streamId, pReq->registryEntryId, pReq->capabilities, required);
    return TSDB_CODE_OPS_NOT_SUPPORT;
  }

  return TSDB_CODE_SUCCESS;
}

SStmTaskStatus* msmFindExtOrigTblReader(SStmStatus* status, int64_t registryEntryId) {
  SStmOrigReaderIter iter = {0};
  SStmTaskStatus*    reader;
  while ((reader = msmNextTrigOReader(status->trigOReaders, &iter))) {
    if (STREAM_IS_REF_EXT_SOURCE(reader->flags) && reader->id.uid == registryEntryId) return reader;
  }
  return NULL;
}

typedef struct {
  SStmExtSchema*  last;
  SStmTaskStatus* reader;
} SStmExtReaderKey;

typedef struct {
  SSHashObj*         keys;
  SStmOrigReaderIter iter;
  bool               exhausted;
} SStmExtReaderLookup;

static int32_t msmAddExtReaderKey(SSHashObj* keys, int64_t uid, SStmExtSchema* schema) {
  SStmExtReaderKey* key = tSimpleHashGet(keys, &uid, sizeof(uid));
  if (key) {
    if (schema) schema->previous = key->last;
    key->last = schema;
    return 0;
  }
  SStmExtReaderKey entry = {.last = schema};
  return tSimpleHashPut(keys, &uid, sizeof(uid), &entry, sizeof(entry));
}

// Scan only as far as this lookup needs. Record the first external match for each requested ID,
// including retired slots. Full blocks never grow, and cleanup/recreation reuses the same slot.
static SStmTaskStatus** msmLookupExtReader(SStmExtReaderLookup* lookup, SStmStatus* live, int64_t uid,
                                           SStmTaskStatus** single) {
  if (!lookup || !lookup->keys) {
    *single = msmFindExtOrigTblReader(live, uid);
    return single;
  }
  SStmExtReaderKey* wanted = tSimpleHashGet(lookup->keys, &uid, sizeof(uid));
  if (!wanted) return NULL;
  while (!wanted->reader && !lookup->exhausted) {
    SStmTaskStatus* reader = msmNextTrigOReader(live->trigOReaders, &lookup->iter);
    if (!reader) {
      lookup->exhausted = true;
    } else if (STREAM_IS_REF_EXT_SOURCE(reader->flags)) {
      SStmExtReaderKey* key = tSimpleHashGet(lookup->keys, &reader->id.uid, sizeof(reader->id.uid));
      if (key && !key->reader) key->reader = reader;
    }
  }
  return &wanted->reader;
}

static void msmFillExtOrigTblReaderAddr(SStmGrpCtx *pCtx, const SStmTaskStatus *pReader,
                                        SStreamExtOrigTblReaderRsp *pRsp) {
  pRsp->readerTaskAddr.taskId = pReader->id.taskId;
  pRsp->readerTaskAddr.nodeId = pReader->id.nodeId;
  if (pCtx != NULL && pCtx->pMnode != NULL) {
    pRsp->readerTaskAddr.epset = mndGetDnodeEpsetById(pCtx->pMnode, pReader->id.nodeId);
  }
}

static void msmRollbackExtOrigTblReaderDeploy(const SStmTaskStatus *pReader) {
  if (pReader == NULL || mStreamMgmt.toDeploySnodeMap == NULL) {
    return;
  }

  SStmSnodeTasksDeploy *pSnode =
      taosHashAcquire(mStreamMgmt.toDeploySnodeMap, &pReader->id.nodeId, sizeof(pReader->id.nodeId));
  if (pSnode == NULL) {
    return;
  }

  bool removed = false;
  taosWLockLatch(&pSnode->lock);
  int32_t taskNum = pSnode->calcReaderList == NULL ? 0 : taosArrayGetSize(pSnode->calcReaderList);
  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskToDeployExt *pExt = taosArrayGet(pSnode->calcReaderList, i);
    if (pExt != NULL && pExt->deploy.task.taskId == pReader->id.taskId) {
      mstDestroySStmTaskToDeployExt(pExt);
      taosArrayRemove(pSnode->calcReaderList, i);
      removed = true;
      break;
    }
  }
  taosWUnLockLatch(&pSnode->lock);
  taosHashRelease(mStreamMgmt.toDeploySnodeMap, pSnode);

  if (removed) {
    (void)atomic_sub_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);
    mstDebug("rollback external original reader deploy, task:%" PRId64 ", node:%d",
             pReader->id.taskId, pReader->id.nodeId);
  }
}

static void msmFillExtOrigTblReaderTask(int64_t streamId, const SStmTaskStatus *pReader, SStreamTask *pTask) {
  pTask->type = pReader->type;
  pTask->streamId = streamId;
  pTask->taskId = pReader->id.taskId;
  pTask->seriousId = pReader->id.seriousId;
  pTask->flags = pReader->flags;
  pTask->deployId = pReader->id.deployId;
  pTask->nodeId = pReader->id.nodeId;
  pTask->taskIdx = pReader->id.taskIdx;
  pTask->status = pReader->status;
}

static int32_t msmQueueExtOrigTblReaderUndeploy(SStmGrpCtx *pCtx, int64_t streamId,
                                                const SStmTaskStatus *pReader) {
  if (pCtx == NULL || pCtx->pRsp == NULL || pReader == NULL) return TSDB_CODE_INVALID_PARA;
  if (pCtx->pRsp->undeploy.taskList == NULL) {
    pCtx->pRsp->undeploy.taskList = taosArrayInit(1, sizeof(SStreamTaskUndeploy));
    if (pCtx->pRsp->undeploy.taskList == NULL) return terrno;
  }
  SStreamTaskUndeploy undeploy = {0};
  msmFillExtOrigTblReaderTask(streamId, pReader, &undeploy.task);
  undeploy.undeployMsg.doCheckpoint = false;
  undeploy.undeployMsg.doCleanup = true;
  if (taosArrayPush(pCtx->pRsp->undeploy.taskList, &undeploy) == NULL) return terrno;
  mstsDebug("queue external original reader cleanup, registry:%" PRId64 ", task:%" PRId64 ", node:%d",
            pReader->id.uid, pReader->id.taskId, pReader->id.nodeId);
  return TSDB_CODE_SUCCESS;
}

static void msmRemoveExtOrigTblReaderFromSnodeMap(int64_t streamId, const SStmTaskStatus *pReader) {
  if (mStreamMgmt.snodeMap == NULL || pReader == NULL) return;
  SStmSnodeStatus *pSnode =
      taosHashGet(mStreamMgmt.snodeMap, &pReader->id.nodeId, sizeof(pReader->id.nodeId));
  if (pSnode == NULL || pSnode->streamTasks == NULL) return;
  SStmSnodeStreamStatus *pStream = taosHashGet(pSnode->streamTasks, &streamId, sizeof(streamId));
  if (pStream == NULL || pReader->id.deployId < 0 || pReader->id.deployId >= MND_STREAM_RUNNER_DEPLOY_NUM) {
    return;
  }
  SArray *pReaders = pStream->runners[pReader->id.deployId];
  int32_t readerNum = pReaders == NULL ? 0 : taosArrayGetSize(pReaders);
  for (int32_t i = 0; i < readerNum; ++i) {
    SStmTaskStatus **ppStatus = taosArrayGet(pReaders, i);
    if (ppStatus != NULL && *ppStatus == pReader) {
      taosArrayRemove(pReaders, i);
      break;
    }
  }
  mstsDebug("remove external original reader from snode map, stream:%" PRId64 ", registry:%" PRId64
            ", task:%" PRId64 ", node:%d",
            streamId, pReader->id.uid, pReader->id.taskId, pReader->id.nodeId);
}

static int32_t msmCleanupExtOrigTblReader(SStmGrpCtx* pCtx, SStmStatus* pStatus, SStreamObj* pStream,
                                          const SStreamExtOrigTblReaderReq* pReq, SStmTaskStatus* pReader,
                                          SStreamExtOrigTblReaderRsp* pRsp) {
  int64_t streamId = pStream->pCreate->streamId;
  if (pReader == NULL) {
    pRsp->code = TSDB_CODE_INVALID_MSG;
    mstsWarn("external original reader cleanup target not found, registry:%" PRId64, pReq->registryEntryId);
    return pRsp->code;
  }
  msmFillExtOrigTblReaderAddr(pCtx, pReader, pRsp);
  int32_t code = 0;
  bool    queued = false;
  // A standalone remote cleanup is delivered by the retired Reader's own heartbeat.
  // A prepared mixed request also records the action to supersede any earlier install.
  if (pCtx->extSchema || !pCtx->pReq || !GOT_SNODE(pCtx->pReq->snodeId) || pCtx->pReq->snodeId == pReader->id.nodeId) {
    code = msmQueueExtOrigTblReaderUndeploy(pCtx, streamId, pReader);
    if (code != TSDB_CODE_SUCCESS) {
      pRsp->code = code;
      return code;
    }
    queued = true;
  }
  // A retry needs the same address and undeploy action even after the registration was removed.
  if (pReader->extReaderCleaned) return TSDB_CODE_SUCCESS;
  if (!pStatus->retiredExtReaders) {
    pStatus->retiredExtReaders =
        taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  }
  code = pStatus->retiredExtReaders ? taosHashPut(pStatus->retiredExtReaders, &pReader->id.taskId,
                                                  sizeof(pReader->id.taskId), &pReader, sizeof(pReader))
                                    : terrno;
  if (code) goto _error;
  if (mStreamMgmt.taskMap != NULL) {
    code = msmRemoveRegisteredTask(streamId, pReader->id.taskId);
    if (code != TSDB_CODE_SUCCESS) {
      TAOS_UNUSED(taosHashRemove(pStatus->retiredExtReaders, &pReader->id.taskId, sizeof(pReader->id.taskId)));
      goto _error;
    }
  }
  msmRemoveExtOrigTblReaderFromSnodeMap(streamId, pReader);
  // Keep the slot for the next CREATE. Compacting this array would move live peers indexed by address.
  pReader->extReaderCleaned = 1;
  pReader->status = STREAM_STATUS_UNDEPLOYED;
  mstsDebug("cleanup external original reader, registry:%" PRId64 ", task:%" PRId64 ", node:%d",
            pReq->registryEntryId, pRsp->readerTaskAddr.taskId, pRsp->readerTaskAddr.nodeId);
  return TSDB_CODE_SUCCESS;

_error:
  if (queued) taosArrayPop(pCtx->pRsp->undeploy.taskList);
  return pRsp->code = code;
}

static void msmRollbackExtOrigTblReaderRuntime(int64_t streamId, SArray *pReaderList, int32_t readerListSize,
                                               SStmTaskStatus *pReader, bool taskMapAdded, bool deployQueued) {
  if (pReader == NULL) {
    return;
  }

  int64_t taskId = pReader->id.taskId;
  if (taskMapAdded && mStreamMgmt.taskMap != NULL) {
    int32_t code = msmRemoveRegisteredTask(streamId, taskId);
    mstsDebug("rollback external original reader task map, code:%d task:%" PRId64, code, taskId);
  }

  if (deployQueued) {
    msmRollbackExtOrigTblReaderDeploy(pReader);
  }

  if (pReaderList != NULL && taosArrayGetSize(pReaderList) > readerListSize) {
    taosArrayPopTailBatch(pReaderList, taosArrayGetSize(pReaderList) - readerListSize);
    mstsDebug("rollback external original reader status, task:%" PRId64, taskId);
  }
}

static bool msmNeedsExtSchema(const SStreamExtOrigTblReaderReq* req) {
  // CREATE/START with no bound columns still needs the timestamp for COUNT(*).
  return !(req->capabilities & STREAM_EXT_ORIGTBL_READER_CAP_PATCH) ||
         (req->capabilities & (STREAM_EXT_ORIGTBL_READER_CAP_CREATE | STREAM_EXT_ORIGTBL_READER_CAP_START)) ||
         taosArrayGetSize(req->bindings) > 0;
}

int32_t msmPrepareExtOrigTblReader(int64_t streamId, SCMCreateStreamReq* create, const SExtSourceObj* source,
                                   const SStreamExtOrigTblReaderReq* req, const SExtTableMeta* meta,
                                   SStmTaskToDeployExt* entry, SStreamExtOrigTblReaderRsp* rsp) {
  msmInitExtOrigTblReaderRsp(req, rsp);
  if (!msmNeedsExtSchema(req)) return 0;
  if (req->sourceConfigVersion && req->sourceConfigVersion != source->metaVersion) return TSDB_CODE_EXT_SOURCE_CHANGED;
  TAOS_CHECK_RETURN(msmValidateExtOrigTblReaderCapabilities(streamId, req));
  SStmStatus snapshot = {.pCreate = create};
  entry->deploy.task.type = STREAM_READER_TASK;
  TAOS_CHECK_RETURN(msmBuildReaderDeployInfo(&entry->deploy, NULL, &snapshot, true));
  TAOS_CHECK_RETURN(msmBuildExtOrigTblReaderSpec(source, req, meta, &entry->deploy.msg.reader.pExtSpec, rsp));
  entry->deploy.msg.reader.pExtSpec->precision = create->triggerPrec;
  entry->deploy.msg.reader.ownsExtSpec = true;
  entry->ownsExtSpec = true;
  return TSDB_CODE_SUCCESS;
}

static int32_t msmQueueRemoteSchemaReader(SStmExtSchema* schema, SStmTaskToDeployExt entry) {
  entry.remoteSchema = schema;
  TAOS_CHECK_RETURN(msmAddSchemaNodeWait(schema->batch->mnode, &schema->queuedNode, entry.deploy.task.nodeId));
  int32_t code = msmTDAddCalcReaderToSnodeMap(&entry);
  if (code) {
    msmRemoveSchemaNodeWait(schema->batch->mnode, &schema->queuedNode);
    return code;
  }
  (void)atomic_add_fetch_32(&schema->batch->refs, 1);
  atomic_store_8(&schema->queued, 1);
  return 0;
}

void msmReleaseQueuedSchema(SStmExtSchema* schema) {
  msmRemoveSchemaNodeWait(schema->batch->mnode, &schema->queuedNode);
  atomic_store_8(&schema->queued, 0);
  msmReleaseSchemaBatch(schema->batch);
}

static int32_t msmPublishSchemaReader(SStmGrpCtx* ctx, SStmTaskToDeployExt* entry) {
  SStmExtSchema* schema = ctx->extSchema;
  if (!schema || !schema->batch->publishing) return msmPublishPreparedReader(entry);
  if (entry->deploy.task.nodeId == schema->batch->triggerNode) {
    // A later request can release actionQLock before this heartbeat drains the snode queue.
    // Keep the local install private until all changes for this registry have been applied.
    entry->deployed = true;
    return 0;
  }
  TAOS_CHECK_RETURN(msmQueueRemoteSchemaReader(ctx->extSchema, *entry));
  *entry = (SStmTaskToDeployExt){.deployed = true};
  return 0;
}

int32_t msmPublishExtOrigTblReader(SStmGrpCtx* pCtx, SStmStatus* pStatus, SStreamObj* pStream,
                                   const SStreamExtOrigTblReaderReq* pReq, SStmTaskToDeployExt* entry,
                                   SStmTaskStatus** slot, SStreamExtOrigTblReaderRsp* pRsp) {
  SStmTaskStatus* pReader = *slot;
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  int64_t         streamId = pStream->pCreate->streamId;
  SArray*         pReaderList = NULL;
  SStmTaskStatus* pNewReader = NULL;
  SStmTaskStatus  candidate = {0};
  SStmTaskId      previousId = {0};
  int32_t         readerListSize = 0;
  bool            deployQueued = false;
  bool            taskMapAdded = false;

  if (!msmNeedsExtSchema(pReq)) return msmCleanupExtOrigTblReader(pCtx, pStatus, pStream, pReq, pReader, pRsp);
  if (pReader != NULL && !pReader->extReaderCleaned) {
    if (pReq->capabilities & STREAM_EXT_ORIGTBL_READER_CAP_PATCH) {
      // Add the undeploy to this private heartbeat response first. If publishing the
      // replacement fails, pop this exact undeploy without touching older queue entries.
      TAOS_CHECK_EXIT(msmQueueExtOrigTblReaderUndeploy(pCtx, streamId, pReader));
      msmFillExtOrigTblReaderTask(streamId, pReader, &entry->deploy.task);
      int64_t oldTaskId = pReader->id.taskId;
      bool    newAddress = pCtx->extSchema && (!pCtx->extSchema->batch->explicitAck ||
                                            pReader->id.nodeId != pCtx->extSchema->batch->triggerNode);
      if (newAddress) {
        // Older nodes remove by taskId alone. Give their replacement a distinct address so a
        // retransmitted cleanup of the previous incarnation cannot remove the new one.
        pReader->id.taskId = msmAssignTaskId();
        code = msmSTAddToTaskMap(pCtx, streamId, NULL, NULL, pReader);
        if (code) {
          pReader->id.taskId = oldTaskId;
          taosArrayPop(pCtx->pRsp->undeploy.taskList);
          goto _exit;
        }
        entry->deploy.task.taskId = pReader->id.taskId;
      }
      entry->deploy.task.seriousId = msmAssignTaskSeriousId();
      int64_t replacement = entry->deploy.task.seriousId;
      code = msmPublishSchemaReader(pCtx, entry);
      if (code) {
        if (newAddress) {
          (void)msmRemoveRegisteredTask(streamId, pReader->id.taskId);
          pReader->id.taskId = oldTaskId;
        }
        taosArrayPop(pCtx->pRsp->undeploy.taskList);
        goto _exit;
      }
      if (newAddress) (void)msmRemoveRegisteredTask(streamId, oldTaskId);
      pReader->id.seriousId = replacement;
      atomic_store_64(&pReader->readerRepairRetryTs, 0);
      atomic_store_64(&pReader->remoteSchemaConfirmation, 0);
      pReader->status = STREAM_STATUS_UNDEPLOYED;
    }
    msmFillExtOrigTblReaderAddr(pCtx, pReader, pRsp);
    mstsDebug("reuse external original reader, registry:%" PRId64 ", task:%" PRId64 ", node:%d", pReq->registryEntryId,
              pReader->id.taskId, pReader->id.nodeId);
    goto _exit;
  }

  int32_t snodeId = atomic_load_32(&pStream->mainSnodeId);
  if (snodeId <= 0) {
    code = TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
    goto _exit;
  }

  if (pReader == NULL) {
    TAOS_CHECK_EXIT(msmEnsureGetOReaderList(streamId, pStatus, &pReaderList));
    readerListSize = taosArrayGetSize(pReaderList);
    pReader = taosArrayReserve(pReaderList, 1);
    TSDB_CHECK_NULL(pReader, code, lino, _exit, terrno);
  }
  pNewReader = pReader;
  msmInitTrigReaderStatus(pCtx, &candidate, snodeId, pStatus, streamId, true);
  candidate.id.uid = pReq->registryEntryId;
  // The indexes retain this slot's address. Its previous contents remain owned until both insertions succeed.
  previousId = pReader->id;
  pReader->id = candidate.id;
  msmFillExtOrigTblReaderTask(streamId, &candidate, &entry->deploy.task);
  TAOS_CHECK_EXIT(msmPublishSchemaReader(pCtx, entry));
  deployQueued = !pCtx->extSchema || !pCtx->extSchema->batch->publishing || atomic_load_8(&pCtx->extSchema->queued);
  TAOS_CHECK_EXIT(msmSTAddToTaskMap(pCtx, streamId, NULL, NULL, pReader));
  taskMapAdded = true;
  TAOS_CHECK_EXIT(msmSTAddToSnodeMap(pCtx, streamId, NULL, pReader, 1, pReader->id.deployId));
  if (pReader->extReaderCleaned) {
    TAOS_UNUSED(taosHashRemove(pStatus->retiredExtReaders, &previousId.taskId, sizeof(previousId.taskId)));
    mstDestroySStmTaskStatus(pReader);
  }
  *pReader = candidate;
  *slot = pReader;  // Expose a new slot to later requests only after both registrations succeed.
  msmFillExtOrigTblReaderAddr(pCtx, pReader, pRsp);
  pNewReader = NULL;
  mstsDebug("deploy external original reader, registry:%" PRId64 ", task:%" PRId64 ", snode:%d", pReq->registryEntryId,
            pReader->id.taskId, pReader->id.nodeId);

_exit:
  if (code != TSDB_CODE_SUCCESS && pNewReader != NULL) {
    msmRollbackExtOrigTblReaderRuntime(streamId, pReaderList, readerListSize, pNewReader, taskMapAdded, deployQueued);
    if (!pReaderList) pNewReader->id = previousId;
  } else if (code == TSDB_CODE_SUCCESS && entry->deployed) {
    msmFillExtOrigTblReaderTask(streamId, pReader, &entry->deploy.task);
  }
  if (!code && pCtx->extSchema && pCtx->extSchema->batch->publishing) {
    SStmExtSchema* schema = pCtx->extSchema;
    schema->requiredReader = (SStmCachedReader){pReader->id.taskId, pReader->id.seriousId, pReader->id.nodeId};
    // A reused address has no install in the Trigger response. Its target must confirm it too.
    if (!entry->deployed) schema->batch->remote = true;
  }
  if (code != TSDB_CODE_SUCCESS) {
    pRsp->code = code;
    mstsError("%s failed at line %d, registry:%" PRId64 ", error:%s", __FUNCTION__, lino,
              pReq == NULL ? 0 : pReq->registryEntryId, tstrerror(code));
  }
  return code;
}

static int32_t msmResolveExtOrigTblReaderNamespace(const SExtSourceObj *pSource,
                                                   const SStreamExtOrigTblReaderReq *pReq,
                                                   const char **ppDatabase, const char **ppSchema) {
  const char *database = pReq->identity.dbName;
  const char *schema = pReq->identity.schemaName;

  // A four-part SQL Server column reference stores its schema in dbName.
  // Preserve the wire identity used for registry matching and resolve only the connector namespace.
  // Mirrors the SQL Server branch of normalizeExternalRefNamespace() in parTranslater.c: the reader
  // must resolve the same database/schema the DDL path persisted, so keep the two in sync.
  //
  // Only the shape written by the pre-normalization ALTER path reaches this branch: dbName holds a
  // schema and schemaName is empty. The other pre-normalization shape (CREATE persisted the path
  // schema in dbName and the source default schema in schemaName) is intentionally NOT
  // reinterpreted: it is indistinguishable from an explicit
  // source.otherdb.<source default schema>.table.column reference, so guessing would corrupt
  // valid five-part references. Such bindings are re-normalised when the column is re-bound with
  // ALTER ... ALTER COLUMN ... SET.
  if (pSource->type == EXT_SOURCE_SQLSERVER && schema[0] == '\0' && database[0] != '\0') {
    schema = database;
    database = "";
  }
  // Oracle has exactly one user namespace level: the schema.  A column path's middle
  // segment is that schema and never a database; the Easy Connect service name is a
  // connection attribute that only comes from the source (DS 5.3.10 item 5).  A stream
  // binding persisted with the folded schema in dbName would otherwise be dialled as
  // service=<schema> and fail with ORA-12514, so re-map it here - this mirrors the
  // Oracle branch of resolveExtPathSegments() and createExtTableRefNode() in
  // parTranslater.c.  The returned database therefore always falls back to the
  // source's default, which holds the service name for Oracle.
  if (pSource->type == EXT_SOURCE_ORACLE) {
    if (schema[0] == '\0') {
      schema = database;
    }
    database = "";
  }
  *ppDatabase = database[0] ? database : pSource->defaultDatabase;
  *ppSchema = schema[0] ? schema : pSource->defaultSchema;
  // A relational table path needs a namespace, and Oracle has only the schema: without one the
  // reader would render "<service>.<table>", which names nothing (ORA-00942) and hides the real
  // cause. ctgAsync.c already refuses such a source with TSDB_CODE_EXT_DEFAULT_NS_MISSING and no
  // implicit fallback to the login user (DS 5.3.3), so the stream reader has to agree instead of
  // letting the provider fail the query.
  if (pSource->type == EXT_SOURCE_ORACLE && (*ppSchema)[0] == '\0') {
    mstWarn("external reader Oracle source has no schema, source:%s, registry:%" PRId64,
            pSource->sourceName, pReq->registryEntryId);
    return TSDB_CODE_EXT_DEFAULT_NS_MISSING;
  }
  mstDebug("resolved external reader namespace, registry:%" PRId64 ", source:%s, type:%d, database:%s, schema:%s",
           pReq->registryEntryId, pSource->sourceName, pSource->type, *ppDatabase, *ppSchema);
  return TSDB_CODE_SUCCESS;
}

static int32_t msmSchemaFetchStatus(SStmSchemaBatch* batch) {
  if (atomic_load_8(&batch->mnode->streamSchemaStopping)) return TSDB_CODE_APP_IS_STOPPING;
  if (atomic_load_8(&batch->workerState) & MST_SCHEMA_CANCELED) return TSDB_CODE_MND_STREAM_NOT_RUNNING;
  return TSDB_CODE_SUCCESS;
}

static int32_t msmFetchExtOrigTblReaderMeta(SStmSchemaBatch* batch, const SExtSourceObj* pSource,
                                            const SStreamExtOrigTblReaderReq* pReq, SExtTableMeta** ppMeta) {
  int32_t code = TSDB_CODE_SUCCESS;
  SExtConnectorHandle *pHandle = NULL;
  char plainPassword[TSDB_EXT_SOURCE_PASSWORD_LEN] = {0};
  SExtSourceCfg cfg = {0};
  SExtTableNode table = {0};

  if (pSource == NULL || pReq == NULL || ppMeta == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  *ppMeta = NULL;
  if (pReq->identity.tableName[0] == '\0') {
    return TSDB_CODE_INVALID_PARA;
  }

  tstrncpy(cfg.source_name, pSource->sourceName, sizeof(cfg.source_name));
  cfg.source_type = (EExtSourceType)pSource->type;
  tstrncpy(cfg.host, pSource->host, sizeof(cfg.host));
  cfg.port = pSource->port;
  tstrncpy(cfg.user, pSource->user, sizeof(cfg.user));
  decryptExtSourcePassword(pSource->encryptedPassword, plainPassword);
  tstrncpy(cfg.password, plainPassword, sizeof(cfg.password));
  const char *database = NULL;
  const char *schema = NULL;
  code = msmResolveExtOrigTblReaderNamespace(pSource, pReq, &database, &schema);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }
  tstrncpy(cfg.default_database, database, sizeof(cfg.default_database));
  tstrncpy(cfg.default_schema, schema, sizeof(cfg.default_schema));
  tstrncpy(cfg.options, pSource->options, sizeof(cfg.options));
  cfg.meta_version = pSource->metaVersion;

  if (cfg.default_database[0] == '\0') {
    code = TSDB_CODE_EXT_DEFAULT_NS_MISSING;
    goto _exit;
  }

  table.table.node.type = QUERY_NODE_EXTERNAL_TABLE;
  tstrncpy(table.sourceName, pReq->identity.sourceName, sizeof(table.sourceName));
  // Oracle has no database level, so the service name that cfg.default_database
  // carries for the connection must not leak into the path as a database
  // qualifier (DS 5.3.3).  createExtTableRefNode() keeps it empty for the same
  // reason, and the Oracle provider warns about any non-empty value it sees.
  tstrncpy(table.table.dbName, pSource->type == EXT_SOURCE_ORACLE ? "" : cfg.default_database,
           sizeof(table.table.dbName));
  tstrncpy(table.schemaName, cfg.default_schema, sizeof(table.schemaName));
  tstrncpy(table.table.tableName, pReq->identity.tableName, sizeof(table.table.tableName));

  code = msmSchemaFetchStatus(batch);
  if (code) goto _exit;
  code = extConnectorOpen(&cfg, &pHandle);
  if (code != TSDB_CODE_SUCCESS) {
    mstWarn("external reader schema connector open failed, source:%s, table:%s, code:%s",
            pReq->identity.sourceName, pReq->identity.tableName, tstrerror(code));
    goto _exit;
  }
  code = msmSchemaFetchStatus(batch);
  if (code) goto _exit;
  code = extConnectorGetTableSchema(pHandle, &table, ppMeta);
  if (code != TSDB_CODE_SUCCESS) {
    mstWarn("external reader schema fetch failed, source:%s, table:%s, code:%s",
            pReq->identity.sourceName, pReq->identity.tableName, tstrerror(code));
  }

_exit:
  if (pHandle != NULL) {
    extConnectorClose(pHandle);
  }
  memset(plainPassword, 0, sizeof(plainPassword));
  memset(cfg.password, 0, sizeof(cfg.password));
  return code;
}

static int32_t msmPinSchemaInput(SStmSchemaBatch* batch, SStmStatus* owner, SStreamObj* stream,
                                 const SStmTaskStatusMsg* task) {
  if (!owner->deployInput) TAOS_CHECK_RETURN(msmInitDeployInput(owner));
  batch->input = owner->deployInput;
  (void)atomic_add_fetch_32(&batch->input->refs, 1);
  batch->stream = stream;
  batch->streamId = task->streamId;
  batch->snode = atomic_load_32(&stream->mainSnodeId);
  batch->placement = msmPlacementVersion(batch->mnode->pSdb);
  batch->epoch = mStreamMgmt.stat.activeTimes;
  batch->taskId = task->taskId;
  batch->seriousId = task->seriousId;
  batch->buildVersion = atomic_load_64(&owner->buildVersion);
  batch->generation = msmPublishedTaskGeneration(owner);
  return TSDB_CODE_SUCCESS;
}

static void msmPrepareExtReaderSchema(SStmSchemaBatch* batch, const SStreamExtOrigTblReaderReq* req,
                                      SStmExtSchema* schema) {
  schema->batch = batch;
  if (!msmNeedsExtSchema(req)) return;
  schema->source = mndAcquireExtSource(batch->mnode, req->identity.sourceName);
  if (!schema->source) {
    schema->code = TSDB_CODE_EXT_SOURCE_NOT_FOUND;
    return;
  }
  SExtSourceObj snapshot;
  sdbReadLock(batch->mnode->pSdb, SDB_EXT_SOURCE);
  snapshot = *schema->source;
  sdbUnLock(batch->mnode->pSdb, SDB_EXT_SOURCE);
  schema->version = snapshot.metaVersion;
#ifdef BUILD_TEST_SEAM
  if (gTdSeam_msmDeployPhase) gTdSeam_msmDeployPhase("ext-schema", batch->input->name);
#endif
  SExtTableMeta* meta = NULL;
  schema->code = msmFetchExtOrigTblReaderMeta(batch, &snapshot, req, &meta);
  if (!schema->code) schema->code = msmSchemaFetchStatus(batch);
  if (!schema->code) {
    schema->code = msmPrepareExtOrigTblReader(batch->streamId, batch->input->create, &snapshot, req, meta,
                                              &schema->entry, &schema->result);
  }
  extConnectorFreeTableSchema(meta);
  if (!schema->code && batch->request) {
    // The initial deploy queue consumes entry. Keep an immutable copy for a lost heartbeat reply.
    schema->replayDeploy.deploy = schema->entry.deploy;
    schema->code = msmCopyExtSpecForDeploy(&schema->replayDeploy, batch->input->name, false);
    if (schema->code) schema->replayDeploy = (SStmTaskToDeployExt){0};
  }
}

static void msmReleaseExtReaderSchema(SMnode* mnode, SStmExtSchema* schema) {
  tFreeSStreamExtOrigTblReaderRsp(&schema->result);
  mstDestroySStmTaskToDeployExt(&schema->entry);
  mstDestroySStmTaskToDeployExt(&schema->replayDeploy);
  if (schema->source) mndReleaseExtSource(mnode, schema->source);
}

// The trigger repeats the same management request until it receives a matching reply.
// Keep only preparation on this worker. The next heartbeat publishes the result together
// with its deploy/undeploy actions, in the usual order.
void msmReleaseSchemaBatch(SStmSchemaBatch* batch) {
  if (!batch || atomic_sub_fetch_32(&batch->refs, 1) != 0) return;
  msmRemoveSchemaNodeWait(batch->mnode, &batch->localAckNode);
  for (int32_t i = 0; i < taosArrayGetSize(batch->schemas); ++i) {
    SStmExtSchema* schema = taosArrayGet(batch->schemas, i);
    msmReleaseExtReaderSchema(batch->mnode, schema);
  }
  if (batch->input) mstReleaseDeployInput(batch->input);
  if (batch->stream) mndReleaseStream(batch->mnode, batch->stream);
  tFreeSStreamMgmtRsp(&batch->reply);
  taosArrayDestroy(batch->schemas);
  tSimpleHashCleanup(batch->readerRegistry);
  taosArrayDestroy(batch->cachedReaders);
  taosArrayDestroy(batch->pairedCleanups);
  tFreeSStreamMgmtReq(batch->request);
  taosMemoryFree(batch->request);
  (void)atomic_sub_fetch_64(&batch->mnode->streamSchemaBatchBytes, batch->bytes);
  if (!batch->cached && atomic_load_8(&batch->workerState) != MST_SCHEMA_CANCELED) {
    (void)atomic_sub_fetch_32(&batch->mnode->streamSchemaBatchCount, 1);
  }
  taosFreeQitem(batch);
}

void msmCancelSchemaBatch(SStmSchemaBatch* batch) {
  if (batch) msmRemoveSchemaNodeWait(batch->mnode, &batch->localAckNode);
  if (batch && atomic_fetch_or_8(&batch->workerState, MST_SCHEMA_CANCELED) == 0) {
    // Only the empty queue item remains. Release the admission slot and charge its remaining bytes.
    tFreeSStreamMgmtReq(batch->request);
    taosMemoryFreeClear(batch->request);
    if (batch->input) mstReleaseDeployInput(batch->input);
    if (batch->stream) mndReleaseStream(batch->mnode, batch->stream);
    batch->input = NULL;
    batch->stream = NULL;
    (void)atomic_sub_fetch_64(&batch->mnode->streamSchemaBatchBytes, batch->bytes - sizeof(*batch));
    batch->bytes = sizeof(*batch);
    (void)atomic_sub_fetch_32(&batch->mnode->streamSchemaBatchCount, 1);
  }
  msmReleaseSchemaBatch(batch);
}

static void msmFetchSchemaBatch(SQueueInfo* info, SStmSchemaBatch* batch) {
  if (atomic_val_compare_exchange_8(&batch->workerState, 0, MST_SCHEMA_STARTED) != 0) {
    msmReleaseSchemaBatch(batch);
    return;
  }
  SArray* requests = batch->request->cont.pExtOrigTblReaders;
  int32_t count = taosArrayGetSize(requests);
  batch->code = msmSchemaFetchStatus(batch);
  if (!batch->code) {
    batch->schemas = taosArrayInit_s(sizeof(SStmExtSchema), count);
    if (!batch->schemas) batch->code = terrno;
  }
  if (batch->schemas && count > 1) {
    batch->readerRegistry = tSimpleHashInit(count, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    if (!batch->readerRegistry) batch->code = terrno;
  }
  for (int32_t i = 0; !batch->code && i < count; ++i) {
    batch->code = msmSchemaFetchStatus(batch);
    if (batch->code) break;
    SStreamExtOrigTblReaderReq* req = taosArrayGet(requests, i);
    if (!batch->pairedCleanups && (req->capabilities & STREAM_EXT_ORIGTBL_READER_CAP_PATCH)) {
      batch->pairedCleanups = taosArrayInit_s(sizeof(SStmCachedReader), count);
      if (!batch->pairedCleanups) {
        batch->code = terrno;
        break;
      }
    }
    SStmExtSchema* schema = taosArrayGet(batch->schemas, i);
    if (batch->readerRegistry) {
      batch->code = msmAddExtReaderKey(batch->readerRegistry, req->registryEntryId, schema);
      if (batch->code) break;
    }
    msmPrepareExtReaderSchema(batch, req, schema);
  }
  atomic_store_8(&batch->ready, 1);
  msmReleaseSchemaBatch(batch);
}

int32_t msmInitSchemaWorker(SMnode* mnode) {
  SSingleWorkerCfg cfg = {.min = tsNumOfMnodeStreamMgmtThreads,
                          .max = tsNumOfMnodeStreamMgmtThreads,
                          .name = "mnode-st-schema",
                          .fp = (FItem)msmFetchSchemaBatch,
                          .param = mnode,
                          .threadCategory = THREAD_CAT_MANAGEMENT};
  return tSingleWorkerInit(&mnode->streamSchemaWorker, &cfg);
}

void msmCleanupSchemaWorker(SMnode* mnode) { tSingleWorkerCleanup(&mnode->streamSchemaWorker); }

static int32_t msmCommitExtReaderSchema(SStmGrpCtx* ctx, SStmTaskStatusMsg* task, SStreamObj* stream,
                                        const SStreamExtOrigTblReaderReq* req, SStmExtSchema* schema,
                                        SStmExtReaderLookup* lookup, SStreamExtOrigTblReaderRsp* rsp) {
  SStmSchemaBatch* batch = schema->batch;
  // A failed or reused entry has no install to retire. Its predecessor was already resolved.
  if (schema->previous && !schema->previous->replayDeploy.deploy.task.taskId)
    schema->previous = schema->previous->previous;
  SStmStatus*      live = ctx->runtimeExpired ? NULL : msmReaderRequestOwner(task);
  if (!live || (batch->publishing && live->pendingSchema != batch) || !msmSameBuildEpoch(batch->epoch) ||
      live->deployInput != batch->input || atomic_load_64(&live->buildVersion) != batch->buildVersion ||
      msmPublishedTaskGeneration(live) != batch->generation || batch->stream != stream ||
      !msmReaderStreamIsCurrent(ctx->pMnode, stream, batch->snode)) {
    return TSDB_CODE_MND_STREAM_NOT_RUNNING;
  }
  SStmTaskStatus*  single = NULL;
  SStmTaskStatus** slot = NULL;
  if (msmNeedsExtSchema(req)) {
    if (!schema->source) return schema->code;
    // ALTER changes the retained row in place. DROP + CREATE can reuse its version.
    SExtSourceObj* current = mndAcquireExtSource(ctx->pMnode, req->identity.sourceName);
    sdbReadLock(ctx->pMnode->pSdb, SDB_EXT_SOURCE);
    bool same = current == schema->source && current->metaVersion == schema->version;
    sdbUnLock(ctx->pMnode->pSdb, SDB_EXT_SOURCE);
    if (current) mndReleaseExtSource(ctx->pMnode, current);
    if (!same) return TSDB_CODE_EXT_SOURCE_CHANGED;
    if (!schema->code) {
      // Bind the task address only now. An earlier item in this same request can
      // have just created the registry's reader; PATCH must reuse its current address.
      slot = msmLookupExtReader(lookup, live, req->registryEntryId, &single);
      if (!slot) return TSDB_CODE_INVALID_MSG;
      int32_t node = *slot && !(*slot)->extReaderCleaned ? (*slot)->id.nodeId : batch->snode;
      TAOS_CHECK_RETURN(
          msmValidateDeploySnode(ctx->pMnode, node, batch->placement, msmPlacementVersion(ctx->pMnode->pSdb)));
    }
    if (schema->result.results) {
      *rsp = schema->result;
      schema->result.results = NULL;
    }
    if (schema->code) return schema->code;
  } else {
    slot = msmLookupExtReader(lookup, live, req->registryEntryId, &single);
    if (!slot) return TSDB_CODE_INVALID_MSG;
  }

  size_t  before = taosArrayGetSize(ctx->pRsp->undeploy.taskList);
  int32_t code = msmPublishExtOrigTblReader(ctx, live, stream, req, &schema->entry, slot, rsp);
  if (code || !batch->publishing) return code;
  if (taosArrayGetSize(ctx->pRsp->undeploy.taskList) > before) {
    schema->replayUndeploy = *(SStreamTaskUndeploy*)taosArrayGetLast(ctx->pRsp->undeploy.taskList);
    taosArrayPop(ctx->pRsp->undeploy.taskList);
    SStmExtSchema* previous = schema->previous;
    if (previous && schema->replayUndeploy.task.taskId == previous->replayDeploy.deploy.task.taskId &&
        schema->replayUndeploy.task.seriousId == previous->replayDeploy.deploy.task.seriousId) {
      // This intermediate instance was never dispatched. Retire the physical
      // instance that preceded the first PATCH along with the final replacement.
      schema->replayUndeploy = previous->replayUndeploy;
      previous->replayUndeploy = (SStreamTaskUndeploy){0};
    }
  }
  if (schema->entry.deployed && schema->replayDeploy.ownsExtSpec) {
    schema->replayDeploy.deploy.task = schema->entry.deploy.task;
    batch->hasDeploy = true;
    if (schema->entry.deploy.task.nodeId != ctx->pReq->snodeId) batch->remote = true;
  }
  return 0;
}

static int32_t msmDeployExtOrigTblReader(SStmGrpCtx* ctx, SStmTaskStatusMsg* task, SStreamObj* stream,
                                         const SStreamExtOrigTblReaderReq* req, SStmExtReaderLookup* lookup,
                                         SStreamExtOrigTblReaderRsp* rsp) {
  msmInitExtOrigTblReaderRsp(req, rsp);
  if (ctx->extSchema) return rsp->code = msmCommitExtReaderSchema(ctx, task, stream, req, ctx->extSchema, lookup, rsp);
  SStmStatus* live = ctx->runtimeExpired ? NULL : msmReaderRequestOwner(task);
  if (!live || stream->pCreate->streamId != task->streamId || atomic_load_8(&stream->userStopped) ||
      atomic_load_8(&stream->userDropped))
    return rsp->code = TSDB_CODE_MND_STREAM_NOT_RUNNING;
  if (!msmNeedsExtSchema(req)) {
    SStmTaskStatus*  single = NULL;
    SStmTaskStatus** slot = msmLookupExtReader(lookup, live, req->registryEntryId, &single);
    if (!slot) return rsp->code = TSDB_CODE_INVALID_MSG;
    return rsp->code = msmCleanupExtOrigTblReader(ctx, live, stream, req, *slot, rsp);
  }

  SStmSchemaBatch batch = {.mnode = ctx->pMnode};
  SStmExtSchema   schema = {0};
  int32_t         code = msmPinSchemaInput(&batch, live, stream, task);
  if (code) return rsp->code = code;
  msmUnlockActionBuild();
  msmPrepareExtReaderSchema(&batch, req, &schema);
  msmResumeReaderRequest(ctx, batch.epoch);
  code = msmCommitExtReaderSchema(ctx, task, stream, req, &schema, NULL, rsp);
  msmReleaseExtReaderSchema(ctx->pMnode, &schema);
  mstReleaseDeployInput(batch.input);
  return rsp->code = code;
}

int32_t msmBuildExtOrigTblReaderSpec(const SExtSourceObj *pSource,
                                     const SStreamExtOrigTblReaderReq *pReq,
                                     const SExtTableMeta *pMeta,
                                     SStreamExtTriggerSpec **ppSpec,
                                     SStreamExtOrigTblReaderRsp *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  SStreamExtTriggerSpec *pSpec = NULL;
  int32_t bindingNum = pReq == NULL || pReq->bindings == NULL ? 0 : taosArrayGetSize(pReq->bindings);

  // A SERIES-bound virtual table legitimately produces zero bindings when the stream
  // projection references no external column (for example COUNT(*) over %%trows): the
  // reader still needs the external timestamp column to advance the watermark and emit
  // rows, and the pre-filled timestamp mapping below covers that case.
  if (ppSpec == NULL || pRsp == NULL || pSource == NULL || pReq == NULL || pMeta == NULL ||
      pMeta->pCols == NULL || pMeta->numOfCols <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  pSpec = taosMemoryCalloc(1, sizeof(*pSpec));
  if (pSpec == NULL) return terrno;
  // The source object is authoritative for the provider type: the same value selects the
  // connector namespace, the type mapper and the identifier matching rule below, while
  // pReq->sourceType is only the identity echoed back to the trigger.  Keep one source of
  // truth so a stale identity cannot make the checks disagree with the deployed spec.
  const int8_t sourceType = pSource->type;
  pSpec->sourceType = sourceType;
  tstrncpy(pSpec->sourceName, pSource->sourceName, sizeof(pSpec->sourceName));
  tstrncpy(pSpec->host, pSource->host, sizeof(pSpec->host));
  pSpec->port = (uint16_t)pSource->port;
  tstrncpy(pSpec->user, pSource->user, sizeof(pSpec->user));
  memcpy(pSpec->encryptedPassword, pSource->encryptedPassword, sizeof(pSpec->encryptedPassword));
  const char *database = NULL;
  const char *schema = NULL;
  code = msmResolveExtOrigTblReaderNamespace(pSource, pReq, &database, &schema);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pSpec);
    return code;
  }
  tstrncpy(pSpec->extDb, database, sizeof(pSpec->extDb));
  tstrncpy(pSpec->extSchema, schema, sizeof(pSpec->extSchema));
  // Oracle folds every unquoted identifier to upper case and the reader renders the
  // name quoted, which makes the remote side case-sensitive: the spelling used in the
  // column path ("same_a") would then name nothing (ORA-00942) while the data
  // dictionary holds "SAME_A".  Use the name the schema fetch resolved, which is the
  // same rule createExtTableRefNode() applies to the schema (DS 5.3.6 R1).
  const char *remoteTable = pReq->identity.tableName;
  if (sourceType == EXT_SOURCE_ORACLE) {
    // Both spellings go through the same Oracle rule: strip the representation quotes
    // the connector reports and collapse doubled quotes.  Falling back to a verbatim
    // copy would leave "same_a" (quotes included) in the spec, and the reader adds its
    // own quoting when it renders the table reference, so the remote side would be
    // dialled as """same_a""" (ORA-00942).
    if (pMeta->remoteTableName[0] != '\0') {
      remoteTable = pMeta->remoteTableName;
    }
    extCopyRemoteTableSpelling(remoteTable, pSpec->extTable, sizeof(pSpec->extTable));
  } else {
    tstrncpy(pSpec->extTable, remoteTable, sizeof(pSpec->extTable));
  }
  tstrncpy(pSpec->options, pSource->options, sizeof(pSpec->options));
  pSpec->connCfgVersion = (uint64_t)pSource->metaVersion;
  pSpec->tagCondLen = pReq->tagCondLen;
  if (pReq->tagCondLen > 0) {
    pSpec->tagCondJson = taosStrdup(pReq->tagCondJson);
    if (pSpec->tagCondJson == NULL) {
      code = terrno;
      goto _error;
    }
  }
  int32_t maxColumns = TMIN(pMeta->numOfCols, STREAM_EXT_ORIGTBL_READER_MAX_COLUMNS);
  // Each projection also reads the primary timestamp, including when it has no bindings.
  int32_t triggerCapacity = 1, calcCapacity = 1;
  for (int32_t i = 0; i < bindingNum; ++i) {
    const SStreamExtOrigTblReaderBinding* binding = taosArrayGet(pReq->bindings, i);
    if (binding->kind == STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER && triggerCapacity < maxColumns) ++triggerCapacity;
    if (binding->kind == STREAM_EXT_ORIGTBL_READER_BINDING_CALC && calcCapacity < maxColumns) ++calcCapacity;
  }
  pSpec->triggerColumns = taosArrayInit(triggerCapacity, TSDB_COL_NAME_LEN);
  pSpec->calcColumns = taosArrayInit(calcCapacity, TSDB_COL_NAME_LEN);
  pSpec->pColMappings = taosMemoryCalloc(triggerCapacity, sizeof(SExtColTypeMapping));
  pSpec->pCalcMappings = taosMemoryCalloc(calcCapacity, sizeof(SExtColTypeMapping));
  pRsp->results = taosArrayInit(bindingNum, sizeof(SStreamExtOrigTblReaderResult));
  if (pSpec->triggerColumns == NULL || pSpec->calcColumns == NULL || pSpec->pColMappings == NULL ||
      pSpec->pCalcMappings == NULL || pRsp->results == NULL) {
    code = terrno;
    goto _error;
  }

  int32_t tsIndex = -1;
  int32_t tsCount = 0;
  for (int32_t i = 0; i < pMeta->numOfCols; ++i) {
    if (pMeta->pCols[i].isPrimaryKey || pMeta->pCols[i].isRemotePrimaryKey) {
      SDataType mappedType = {0};
      code = extTypeNameToTDengineType((EExtSourceType)sourceType, pMeta->pCols[i].extTypeName,
                                      pMeta->pCols[i].extCharsetName, &mappedType);
      if (code != TSDB_CODE_SUCCESS) goto _error;
      if (mappedType.type == TSDB_DATA_TYPE_TIMESTAMP) {
        tsIndex = i;
        ++tsCount;
      }
    }
  }
  if (tsIndex >= 0) {
    mstDebug("external reader timestamp column selected, registry:%" PRId64 ", table:%s, column:%s, remotePrimary:%d",
             pReq->registryEntryId, pReq->identity.tableName, pMeta->pCols[tsIndex].colName,
             pMeta->pCols[tsIndex].isRemotePrimaryKey);
  }
  if (tsCount != 1 || tsIndex < 0 || pMeta->pCols[tsIndex].colName[0] == '\0') {
    mstWarn("external reader requires one timestamp primary key, registry:%" PRId64 ", table:%s, count:%d",
            pReq->registryEntryId, pReq->identity.tableName, tsCount);
    code = TSDB_CODE_EXT_NO_TS_PRIMARY_KEY;
    goto _error;
  }
  // The reader advances a microsecond watermark over the external timestamp, so
  // the column must not carry fractional seconds finer than a microsecond: the
  // connector truncates them, and a row that only differs below one microsecond
  // from the current watermark can neither be distinguished from the consumed
  // row (endless re-poll) nor be skipped safely.  SQL Server datetime2(7) /
  // datetimeoffset(7), including the default precision 7, are the only declared
  // types with this property; virtual tables may still reference them, but a
  // stream reader rejects the binding instead of silently losing rows.
  if (extTimestampTypeHasSubMicroFraction((EExtSourceType)sourceType, pMeta->pCols[tsIndex].extTypeName)) {
    mstWarn("external reader timestamp column has sub-microsecond precision, registry:%" PRId64
            ", table:%s, column:%s, type:%s",
            pReq->registryEntryId, pReq->identity.tableName, pMeta->pCols[tsIndex].colName,
            pMeta->pCols[tsIndex].extTypeName);
    code = TSDB_CODE_STREAM_EXT_TS_PRECISION_NOT_SUPPORTED;
    goto _error;
  }
  tstrncpy(pSpec->tsColumn, pMeta->pCols[tsIndex].colName, sizeof(pSpec->tsColumn));
  for (int32_t kind = STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER;
       kind <= STREAM_EXT_ORIGTBL_READER_BINDING_CALC; ++kind) {
    SArray *pColumns = kind == STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER ? pSpec->triggerColumns
                                                                         : pSpec->calcColumns;
    SExtColTypeMapping *pMapping = kind == STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER ? pSpec->pColMappings
                                                                                      : pSpec->pCalcMappings;
    char *pColumn = taosArrayReserve(pColumns, 1);
    if (pColumn == NULL) {
      code = terrno;
      goto _error;
    }
    tstrncpy(pColumn, pMeta->pCols[tsIndex].colName, TSDB_COL_NAME_LEN);
    code = extTypeNameToTDengineType((EExtSourceType)sourceType, pMeta->pCols[tsIndex].extTypeName,
                                     pMeta->pCols[tsIndex].extCharsetName, &pMapping->tdType);
    if (code != TSDB_CODE_SUCCESS) goto _error;
    if (pMapping->tdType.type != TSDB_DATA_TYPE_TIMESTAMP) {
      mstWarn("external reader primary key is not timestamp, registry:%" PRId64 ", table:%s, column:%s, type:%s",
              pReq->registryEntryId, pReq->identity.tableName, pMeta->pCols[tsIndex].colName,
              pMeta->pCols[tsIndex].extTypeName);
      code = TSDB_CODE_EXT_NO_TS_PRIMARY_KEY;
      goto _error;
    }
    tstrncpy(pMapping->extTypeName, pMeta->pCols[tsIndex].extTypeName, sizeof(pMapping->extTypeName));
    tstrncpy(pMapping->colName, pMeta->pCols[tsIndex].colName, sizeof(pMapping->colName));
    pMapping->tsPrecision = pMeta->tsPrecision;
    if (kind == STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER) {
      pSpec->numColMappings = 1;
    } else {
      pSpec->numCalcMappings = 1;
    }
  }

  for (int32_t i = 0; i < bindingNum; ++i) {
    const SStreamExtOrigTblReaderBinding *pBinding = taosArrayGet(pReq->bindings, i);
    SStreamExtOrigTblReaderResult result = {.binding = *pBinding, .code = TSDB_CODE_SUCCESS};
    // extFindColumnIndex() is the rule the parser and the stored filter resolve references with:
    // the exact spelling wins across the catalog and only a reference without an exact candidate
    // falls back to the provider fold.  Folding first would rebind a reference the DDL accepted
    // for a lower-case column to an upper-case sibling that merely comes first in the catalog, and
    // both spellings normally share the TDengine type, so no later type check can notice.
    int32_t              matchedColIdx =
        extFindColumnIndex((EExtSourceType)sourceType, pMeta, pBinding->externalColumn);
    const SExtColumnDef *pCol = matchedColIdx >= 0 ? pMeta->pCols + matchedColIdx : NULL;
    // A virtual column may legitimately reference the external table's timestamp column: the
    // source timestamp already occupies mapping slot 0 (see the trigger/calc mapping pre-fill
    // above), so such a binding has to resolve to slot 0 instead of being rejected as an unmapped
    // primary key. Primary keys other than the timestamp stay unavailable.
    bool isTsCol = matchedColIdx >= 0 && matchedColIdx == tsIndex;
    if (pBinding->kind != STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER &&
        pBinding->kind != STREAM_EXT_ORIGTBL_READER_BINDING_CALC) {
      result.code = TSDB_CODE_INVALID_MSG;
    } else if (pCol == NULL || (pCol->isPrimaryKey && !isTsCol) || pBinding->externalColumn[0] == '\0') {
      result.code = TSDB_CODE_EXT_COLUMN_NOT_EXIST;
    } else {
      int32_t *pMapCount = pBinding->kind == STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER ? &pSpec->numColMappings
                                                                                     : &pSpec->numCalcMappings;
      SExtColTypeMapping *pMappings = pBinding->kind == STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER
                                          ? pSpec->pColMappings : pSpec->pCalcMappings;
      // Both sides are resolved catalog spellings, so the slot is matched by exact identity.
      // Re-applying the provider's fold rule here would merge two distinct Oracle columns whose
      // names differ only by case into one slot - a table holding TS and a quoted "ts" would read
      // TS twice - while the binding lookup above already selected the column with that rule and
      // reported it back to the reader as this very spelling.
      int32_t mapIndex = 0;
      while (mapIndex < *pMapCount && 0 != strcmp(pMappings[mapIndex].colName, pCol->colName)) {
        ++mapIndex;
      }
      bool newMapping = mapIndex == *pMapCount;
      if (newMapping && mapIndex >= maxColumns) {
        mstWarn("external reader exceeds physical column limit, registry:%" PRId64 ", columns:%d",
                pReq->registryEntryId, mapIndex + 1);
        code = TSDB_CODE_INVALID_MSG;
        goto _error;
      }
      SExtColTypeMapping *pMapping = &pMappings[mapIndex];
      result.code = extTypeNameToTDengineType((EExtSourceType)sourceType, pCol->extTypeName,
                                               pCol->extCharsetName, &pMapping->tdType);
      if (result.code == TSDB_CODE_SUCCESS && pMapping->tdType.type == TSDB_DATA_TYPE_TIMESTAMP) {
        // The provider decodes every timestamp column at the table's transport precision, not only
        // the primary-key column, and a mapping that leaves tsPrecision at 0 is decoded as
        // microseconds. An Oracle TIMESTAMP(7..9) non-key column would then lose its last three
        // digits at fetch time; normalizing the block afterwards can only pad zeros. Carry the same
        // source precision the timestamp mapping above uses (DS 5.3.7).
        pMapping->tsPrecision = pMeta->tsPrecision;
      }
      if (result.code == TSDB_CODE_SUCCESS && IS_DECIMAL_TYPE(pMapping->tdType.type)) {
        // Providers decode DECIMAL mappings through extractDecimalTypeInfoFromBytes(), which expects
        // bytes packed as (physicalWidth << 24) | (precision << 8) | scale. The raw physical width
        // stored by extTypeNameToTDengineType() would decode as a zero-width column and the first
        // fetched block would be rejected.
        fillBytesForDecimalType(&pMapping->tdType.bytes, pMapping->tdType.type, pMapping->tdType.precision,
                                pMapping->tdType.scale);
      }
      if (result.code == TSDB_CODE_SUCCESS && pBinding->expectedType != TSDB_DATA_TYPE_NULL &&
          pBinding->expectedType != pMapping->tdType.type) {
        result.code = TSDB_CODE_EXT_TYPE_NOT_MAPPABLE;
      }
      if (result.code == TSDB_CODE_SUCCESS && IS_DECIMAL_TYPE(pMapping->tdType.type) &&
          (pBinding->expectedPrecision != pMapping->tdType.precision ||
           pBinding->expectedScale != pMapping->tdType.scale)) {
        // The virtual column keeps its declared DECIMAL precision/scale while the reader encodes
        // values with the remote mapping. Same type enum + different scale would silently
        // re-interpret the integer payload (1.230 as 1230 -> 12.30), so reject the redeploy
        // instead; variable-length columns are unaffected and keep their legal width differences.
        mstWarn("external reader decimal precision/scale mismatch, registry:%" PRId64 ", column:%s"
                ", expected:(%d,%d) actual:(%d,%d)",
                pReq->registryEntryId, pBinding->externalColumn, pBinding->expectedPrecision,
                pBinding->expectedScale, pMapping->tdType.precision, pMapping->tdType.scale);
        result.code = TSDB_CODE_EXT_TYPE_NOT_MAPPABLE;
      }
      if (result.code == TSDB_CODE_SUCCESS) {
        result.binding.resultColumnSlot = mapIndex;
        if (newMapping) {
          SArray *pColumns = pBinding->kind == STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER ? pSpec->triggerColumns
                                                                                       : pSpec->calcColumns;
          char *pColumn = taosArrayReserve(pColumns, 1);
          if (pColumn == NULL) {
            code = terrno;
            goto _error;
          }
          tstrncpy(pColumn, pCol->colName, TSDB_COL_NAME_LEN);
          tstrncpy(pMapping->extTypeName, pCol->extTypeName, sizeof(pMapping->extTypeName));
          tstrncpy(pMapping->colName, pCol->colName, sizeof(pMapping->colName));
          ++*pMapCount;
        }
        result.actualType = pMapping->tdType.type;
        if (pBinding->kind == STREAM_EXT_ORIGTBL_READER_BINDING_TRIGGER) {
          pRsp->acceptedTriggerBindingCount++;
        } else {
          pRsp->acceptedCalcBindingCount++;
        }
      }
    }
    if (taosArrayPush(pRsp->results, &result) == NULL) {
      code = terrno;
      goto _error;
    }
    if (result.code != TSDB_CODE_SUCCESS) code = result.code;
  }
  pRsp->code = code;
  if (code != TSDB_CODE_SUCCESS) goto _error;
  *ppSpec = pSpec;
  mstDebug("external reader spec built, registry:%" PRId64 ", bindings:%d, triggerColumns:%d, calcColumns:%d",
           pReq->registryEntryId, bindingNum, pSpec->numColMappings, pSpec->numCalcMappings);
  return TSDB_CODE_SUCCESS;

_error:
  pRsp->code = code;
  tFreeSStreamExtTriggerSpec(pSpec);
  return code;
}
#endif

static int32_t msmDeployTriggerMixedOrigReader(SStmGrpCtx* pCtx, SStmTaskStatusMsg* pTask, SArray* schemas,
                                               SSHashObj* registry) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  int64_t          streamId = pTask->streamId;
  SStreamMgmtReq*  pMgmtReq = NULL;
  SStreamMgmtReq*  pMixedReq = NULL;
  SStreamMgmtReq*  pOrigReq = pTask->pMgmtReq;
  SStreamMgmtRsp   rsp = {0};
  SArray*          pExtReqs = pOrigReq->cont.pExtOrigTblReaders;
  int32_t          extNum = taosArrayGetSize(pExtReqs);
  SStreamObj*      pStream = NULL;
  bool             internalRspPushed = false;

  // Initialize the response identity up front so every exit path (including the internal-branch
  // failure paths below) pushes a well-formed entry instead of a zeroed reqId/msgType one.
  rsp.reqId = pOrigReq->reqId;
  rsp.header.msgType = STREAM_MSG_MIXED_ORIGTBL_READER_INFO;

#ifdef TD_ENTERPRISE
  SStmExtReaderLookup lookup = {.keys = registry};
  if (!pCtx->runtimeExpired) {
    pStream = msmAcquireTriggerReaderStream(pCtx->pMnode, msmReaderRequestOwner(pTask), pTask);
  }
#endif

  if (taosArrayGetSize(pOrigReq->cont.pReqs) > 0) {
    // Take ownership of the request immediately: any early _exit below frees pMixedReq, and the
    // heartbeat deep-clean (tCleanupStreamHbMsg) would free pTask->pMgmtReq a second time if the
    // pointer were left dangling.
    pMixedReq = pOrigReq;
    pTask->pMgmtReq = NULL;
    SStreamMgmtReq* pInternalReq = taosMemoryCalloc(1, sizeof(SStreamMgmtReq));
    TSDB_CHECK_NULL(pInternalReq, code, lino, _exit, terrno);
    mstsDebug("deploy mixed original readers, internal:%d external:%d", (int32_t)taosArrayGetSize(pMixedReq->cont.pReqs),
              extNum);
    *pInternalReq = *pMixedReq;
    pInternalReq->type = STREAM_MGMT_REQ_TRIGGER_ORIGTBL_READER;
    pInternalReq->cont.pExtOrigTblReaders = NULL;
    pMixedReq->cont.pReqs = NULL;
    pTask->pMgmtReq = pInternalReq;

    // The internal handler appends exactly one response entry on success and on failure, so mark
    // it as pushed BEFORE the call: a failure must propagate onto that entry instead of appending
    // a second response for the same reqId (with a zeroed task identity).
    rsp.task = *(SStreamTask *)pTask;
    internalRspPushed = true;
    code = msmDeployTriggerOrigReader(pCtx, pTask);
    TAOS_CHECK_EXIT(code);
    SStreamMgmtRsp* pInternalRsp =
        taosArrayGet(pCtx->pRsp->rsps.rspList, taosArrayGetSize(pCtx->pRsp->rsps.rspList) - 1);
    TSDB_CHECK_NULL(pInternalRsp, code, lino, _exit, TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    // From here on this request's response is the internal handler's entry in
    // rspList (now converted to MIXED); the shared _exit must propagate later
    // failures onto it instead of appending a second response.
    pInternalRsp->header.msgType = STREAM_MSG_MIXED_ORIGTBL_READER_INFO;
    pInternalRsp->cont.pExtOrigTblReaders = taosArrayInit_s(sizeof(SStreamExtOrigTblReaderRsp), extNum);
    TSDB_CHECK_NULL(pInternalRsp->cont.pExtOrigTblReaders, code, lino, _exit, terrno);
    for (int32_t i = 0; i < extNum; ++i) {
      SStreamExtOrigTblReaderReq* pReq = taosArrayGet(pExtReqs, i);
      SStreamExtOrigTblReaderRsp* pExtRsp = taosArrayGet(pInternalRsp->cont.pExtOrigTblReaders, i);
      TSDB_CHECK_NULL(pReq, code, lino, _exit, TSDB_CODE_INVALID_PARA);
      pExtRsp->registryEntryId = pReq->registryEntryId;
      pExtRsp->deploymentGeneration = pReq->deploymentGeneration;
      pExtRsp->protocolVersion = pReq->protocolVersion;
      pExtRsp->capabilities = pReq->capabilities;
      pExtRsp->identity = pReq->identity;
      if (pInternalRsp->code) {
        pExtRsp->code = pInternalRsp->code;
        continue;
      }
#ifdef TD_ENTERPRISE
      if (pStream != NULL) {
        pCtx->extSchema = schemas ? taosArrayGet(schemas, i) : NULL;
        (void)msmDeployExtOrigTblReader(pCtx, pTask, pStream, pReq, &lookup, pExtRsp);
        pCtx->extSchema = NULL;
      } else {
        pExtRsp->code = TSDB_CODE_MND_STREAM_NOT_RUNNING;
      }
#else
      pExtRsp->code = TSDB_CODE_OPS_NOT_SUPPORT;
#endif
    }
    mstsDebug("completed mixed original reader response, internal:%d external:%d",
              (int32_t)taosArrayGetSize(pInternalRsp->cont.vgIds),
              extNum);
    tFreeSStreamMgmtReq(pMixedReq);
    taosMemoryFree(pMixedReq);
    pMixedReq = NULL;
#ifdef TD_ENTERPRISE
    if (pStream) {
      mndReleaseStream(pCtx->pMnode, pStream);
    }
#endif
    return TSDB_CODE_SUCCESS;
  }

  rsp.reqId = pOrigReq->reqId;
  rsp.header.msgType = STREAM_MSG_MIXED_ORIGTBL_READER_INFO;
  if (pCtx->pRsp->rsps.rspList == NULL) {
    pCtx->pRsp->rsps.rspList = taosArrayInit(2, sizeof(SStreamMgmtRsp));
    TSDB_CHECK_NULL(pCtx->pRsp->rsps.rspList, code, lino, _exit, terrno);
  }

  TSWAP(pTask->pMgmtReq, pMgmtReq);
  rsp.task = *(SStreamTask*)pTask;
  rsp.cont.pExtOrigTblReaders = taosArrayInit_s(sizeof(SStreamExtOrigTblReaderRsp), extNum);
  TSDB_CHECK_NULL(rsp.cont.pExtOrigTblReaders, code, lino, _exit, terrno);

  for (int32_t i = 0; i < extNum; ++i) {
    SStreamExtOrigTblReaderReq* pReq = taosArrayGet(pExtReqs, i);
    SStreamExtOrigTblReaderRsp* pExtRsp = taosArrayGet(rsp.cont.pExtOrigTblReaders, i);
    TSDB_CHECK_NULL(pReq, code, lino, _exit, TSDB_CODE_INVALID_PARA);
    TSDB_CHECK_NULL(pExtRsp, code, lino, _exit, TSDB_CODE_INVALID_PARA);

    pExtRsp->registryEntryId = pReq->registryEntryId;
    pExtRsp->deploymentGeneration = pReq->deploymentGeneration;
    pExtRsp->protocolVersion = pReq->protocolVersion;
    pExtRsp->capabilities = pReq->capabilities;
    pExtRsp->identity = pReq->identity;
#ifdef TD_ENTERPRISE
    if (pStream != NULL) {
      pCtx->extSchema = schemas ? taosArrayGet(schemas, i) : NULL;
      (void)msmDeployExtOrigTblReader(pCtx, pTask, pStream, pReq, &lookup, pExtRsp);
      pCtx->extSchema = NULL;
    } else {
      pExtRsp->code = TSDB_CODE_MND_STREAM_NOT_RUNNING;
    }
#else
    pExtRsp->code = TSDB_CODE_OPS_NOT_SUPPORT;
    mstsDebug("mixed external reader deployment is unavailable in community build, registry:%" PRId64,
              pReq->registryEntryId);
#endif
  }

_exit:
  tFreeSStreamMgmtReq(pMixedReq);
  taosMemoryFree(pMixedReq);
  tFreeSStreamMgmtReq(pMgmtReq);
  taosMemoryFree(pMgmtReq);
  if (code != TSDB_CODE_SUCCESS) {
    rsp.code = code;
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  if (pCtx->pRsp->rsps.rspList != NULL) {
    if (internalRspPushed) {
      // The internal reader deploy handler already appended this request's
      // response (now typed MIXED).  Propagate a later failure onto that single
      // entry; appending another response for the same reqId would make the
      // trigger process a truncated success followed by the real error.
      if (code != TSDB_CODE_SUCCESS) {
        SStreamMgmtRsp *pInternalRsp = NULL;
        if (taosArrayGetSize(pCtx->pRsp->rsps.rspList) > 0) {
          pInternalRsp =
              taosArrayGet(pCtx->pRsp->rsps.rspList, taosArrayGetSize(pCtx->pRsp->rsps.rspList) - 1);
        }
        if (pInternalRsp != NULL) {
          pInternalRsp->code = code;
        } else {
          // The internal handler failed before appending its entry: fall back to the local
          // response so the trigger always receives one reply per request.
          int32_t pushCode = taosArrayPush(pCtx->pRsp->rsps.rspList, &rsp) == NULL ? terrno : TSDB_CODE_SUCCESS;
          if (pushCode != TSDB_CODE_SUCCESS) {
            tFreeSStreamMgmtRsp(&rsp);
            msmHandleStreamTaskErr(pCtx, STM_ERR_PROCESSING_ERR, pTask);
            code = (code != TSDB_CODE_SUCCESS) ? code : pushCode;
            mstsError("failed to append mixed original reader fallback response, error:%s",
                      tstrerror(pushCode));
          }
        }
      }
    } else {
      int32_t pushCode = taosArrayPush(pCtx->pRsp->rsps.rspList, &rsp) == NULL ? terrno : TSDB_CODE_SUCCESS;
      if (pushCode != TSDB_CODE_SUCCESS) {
        tFreeSStreamMgmtRsp(&rsp);
        msmHandleStreamTaskErr(pCtx, STM_ERR_PROCESSING_ERR, pTask);
        code = pushCode;
        mstsError("failed to append mixed original reader response, error:%s", tstrerror(code));
      }
    }
  }
#ifdef TD_ENTERPRISE
  if (pStream) {
    mndReleaseStream(pCtx->pMnode, pStream);
  }
#endif
  return code;
}

int32_t msmHandleTaskMgmtReq(SStmGrpCtx* pCtx, SStmTaskStatusMsg* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  switch (pTask->pMgmtReq->type) {
    case STREAM_MGMT_REQ_TRIGGER_ORIGTBL_READER:
      msmDeployTriggerOrigReader(pCtx, pTask);
      break;
    case STREAM_MGMT_REQ_RUNNER_ORIGTBL_READER:
      msmDeployRunnerOrigReader(pCtx, pTask);
      break;
    case STREAM_MGMT_REQ_TRIGGER_EXT_SOURCE_IDENTITY:
      code = msmHandleTriggerExtSourceIdentity(pCtx, pTask);
      break;
    case STREAM_MGMT_REQ_TRIGGER_MIXED_ORIGTBL_READER:
      if (taosArrayGetSize(pTask->pMgmtReq->cont.pExtOrigTblReaders) == 0) {
        code = msmDeployTriggerOrigReader(pCtx, pTask);
      } else {
        code = msmDeployTriggerMixedOrigReader(pCtx, pTask, NULL, NULL);
      }
      break;
    default:
      msttError("Invalid mgmtReq type:%d", pTask->pMgmtReq->type);
      code = TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
      break;
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

#ifdef TD_ENTERPRISE
// The reservation lasts through worker execution, result replay and any heartbeat borrowing it.
// The count also bounds catalog/deployment references, whose storage is shared with the runtime.
#define MST_SCHEMA_MAX_BATCHES 32
#define MST_SCHEMA_MAX_BYTES   (INT64_C(256) * 1024 * 1024)

static int64_t msmSchemaBatchBytes(const SStreamMgmtReq* req, const SStmStatus* owner) {
  const SArray* local = req->cont.pReqs;
  const SArray* external = req->cont.pExtOrigTblReaders;
  // A Trigger or a queued Reader may be the first waiter in its node bucket.
  const int64_t nodeWaitBytes = sizeof(SArray) + TARRAY_MIN_SIZE * sizeof(SStmSchemaNodeWait);
  int64_t       bytes = 2 * sizeof(SStmSchemaBatch) + sizeof(*req) + nodeWaitBytes;
  if (local) {
    bytes += sizeof(SArray) + (int64_t)local->capacity * (sizeof(SStreamDbTableName) + sizeof(int32_t));
    bytes += sizeof(SArray) + (int64_t)(msmGetTrigOReaderSize(owner->trigOReaders) + local->size + TARRAY_MIN_SIZE) *
                                  sizeof(SStreamTaskAddr);
  }
  bytes += 2 * sizeof(SArray) +
           (int64_t)external->capacity * (sizeof(SStreamExtOrigTblReaderReq) + sizeof(SStmCachedReader));
  for (int32_t i = 0; i < external->size; ++i) {
    const SStreamExtOrigTblReaderReq* table = taosArrayGet(external, i);
    int64_t                           bindings = taosArrayGetSize(table->bindings);
    int64_t slots = bindings + 2 * TARRAY_MIN_SIZE;  // Both timestamp slots and minimum SArray capacities.
    bytes += nodeWaitBytes + sizeof(SStmExtSchema) + sizeof(SStreamExtOrigTblReaderRsp) + sizeof(SExtSourceObj) +
             2 * sizeof(SStreamExtTriggerSpec) + 8 * sizeof(SArray) +
             2 * slots * (TSDB_COL_NAME_LEN + sizeof(SExtColTypeMapping)) +
             (bindings + TARRAY_MIN_SIZE) * sizeof(SStreamExtOrigTblReaderResult);
    if (table->tagCondLen > 0) {
      // The decoded request owns one string. Preparation and reply replay each keep another spec.
      int64_t copies = msmNeedsExtSchema(table) ? 3 : 1;
      bytes += copies * (table->tagCondLen + 1);
    }
    if (table->bindings) bytes += (int64_t)table->bindings->capacity * sizeof(SStreamExtOrigTblReaderBinding);
    if (bytes > MST_SCHEMA_MAX_BYTES) break;
  }
  return bytes;
}

static void msmSchemaRequestError(SStmGrpCtx* ctx, SStmTaskStatusMsg* task, int32_t code) {
  SStreamMgmtReq* req = task->pMgmtReq;
  SStreamMgmtRsp  rsp = {.reqId = req->reqId, .header.msgType = STREAM_MSG_MIXED_ORIGTBL_READER_INFO, .task = *task};
  rsp.task.pMgmtReq = NULL;
  int32_t count = taosArrayGetSize(req->cont.pExtOrigTblReaders);
  rsp.cont.pExtOrigTblReaders = taosArrayInit_s(sizeof(SStreamExtOrigTblReaderRsp), count);
  if (rsp.cont.pExtOrigTblReaders) {
    for (int32_t i = 0; i < count; ++i) {
      SStreamExtOrigTblReaderRsp* ext = taosArrayGet(rsp.cont.pExtOrigTblReaders, i);
      msmInitExtOrigTblReaderRsp(taosArrayGet(req->cont.pExtOrigTblReaders, i), ext);
      ext->code = code;
    }
  } else {
    rsp.code = code;
  }
  if (!taosArrayPush(ctx->pRsp->rsps.rspList, &rsp)) tFreeSStreamMgmtRsp(&rsp);
}

// An idle legacy heartbeat may predate the request. Once the remote Readers confirm
// installation, keep just the exact result for a lost Trigger reply. Completed results
// yield their shared byte reservation to new work and never hold a worker slot.
static int64_t msmSchemaArrayBytes(const SArray* array) {
  return array ? sizeof(*array) + (int64_t)array->capacity * array->elemSize : 0;
}

static bool msmCacheRemoteSchemaResult(SMnode* mnode, SStmStatus* owner, SStmSchemaReply* pin) {
  SStmSchemaBatch* batch = pin->batch;
  SStmSchemaBatch* result = NULL;
  if (taosAllocateQitem(sizeof(*result), DEF_QITEM, 0, (void**)&result)) return false;
  SStreamMgmtRspCont* cont = &batch->reply.cont;
  int64_t bytes = sizeof(*batch) + msmSchemaArrayBytes(cont->vgIds) + msmSchemaArrayBytes(cont->readerList) +
                  msmSchemaArrayBytes(cont->pExtOrigTblReaders);
  for (size_t i = 0; i < taosArrayGetSize(cont->pExtOrigTblReaders); ++i) {
    SStreamExtOrigTblReaderRsp* ext = taosArrayGet(cont->pExtOrigTblReaders, i);
    bytes += msmSchemaArrayBytes(ext->results);
  }
  for (size_t i = 0; !batch->rejected && i < taosArrayGetSize(batch->schemas); ++i) {
    const SStmExtSchema* schema = taosArrayGet(batch->schemas, i);
    if (!schema->requiredReader.taskId || schema->superseded) continue;
    if (!result->cachedReaders) result->cachedReaders = taosArrayInit(1, sizeof(SStmCachedReader));
    if (!result->cachedReaders || !taosArrayPush(result->cachedReaders, &schema->requiredReader)) {
      taosArrayDestroy(result->cachedReaders);
      taosFreeQitem(result);
      return false;
    }
  }
  bytes += msmSchemaArrayBytes(result->cachedReaders);
  // A target heartbeat may still be encoding its borrowed Reader spec. Keep its
  // pin and reservation on the original batch; only the reply moves to the cache.
  result->mnode = mnode;
  result->refs = 2;  // The result cache and this Trigger heartbeat.
  result->published = result->remote = result->cached = true;
  result->reqId = batch->reqId;
  result->streamId = batch->streamId;
  result->taskId = batch->taskId;
  result->seriousId = batch->seriousId;
  result->generation = batch->generation;
  result->buildVersion = batch->buildVersion;
  result->bytes = bytes;
  result->reply = batch->reply;
  batch->reply.cont = (SStreamMgmtRspCont){0};
  batch->bytes -= bytes;
  owner->triggerTask->legacySchemaResultId = result->reqId;
  owner->pendingSchema = NULL;
  pin->batch = result;
  msmReleaseSchemaBatch(batch);  // owner
  msmReleaseSchemaBatch(batch);  // Trigger heartbeat
  SStmSchemaBatch** tail = &mnode->streamSchemaResults;
  int32_t           count = 0;
  while (*tail) {
    ++count;
    tail = &(*tail)->nextResult;
  }
  *tail = result;
  if (count == MST_SCHEMA_MAX_BATCHES) msmRemoveSchemaResult(&mnode->streamSchemaResults);
  return true;
}

static void msmQueueSchemaRequest(SStmGrpCtx* ctx, SStmTaskStatusMsg* task, SStmStatus* owner) {
  SMnode* mnode = ctx->pMnode;
  if (atomic_load_8(&mnode->streamSchemaStopping)) {
    msmSchemaRequestError(ctx, task, TSDB_CODE_APP_IS_STOPPING);
    return;
  }
  if (!mnode->streamSchemaWorker.queue) {
    msmSchemaRequestError(ctx, task, TSDB_CODE_MND_STREAM_NOT_AVAILABLE);
    return;
  }
  if (atomic_load_32(&mnode->streamSchemaBatchCount) >= MST_SCHEMA_MAX_BATCHES) return;
  int64_t bytes = msmSchemaBatchBytes(task->pMgmtReq, owner);
  if (bytes > MST_SCHEMA_MAX_BYTES) {
    msmSchemaRequestError(ctx, task, TSDB_CODE_OUT_OF_RPC_MEMORY_QUEUE);
    return;
  }
  // Admission is serialized by actionQLock. A full queue leaves the management request at the trigger.
  while (atomic_load_64(&mnode->streamSchemaBatchBytes) > MST_SCHEMA_MAX_BYTES - bytes) {
    if (!mnode->streamSchemaResults) return;
    msmRemoveSchemaResult(&mnode->streamSchemaResults);
  }
  SStmSchemaBatch* batch = NULL;
  int32_t          code = taosAllocateQitem(sizeof(*batch), DEF_QITEM, bytes, (void**)&batch);
  if (!code) {
    batch->refs = 2;
    batch->mnode = mnode;
    batch->bytes = bytes;
    batch->reqId = task->pMgmtReq->reqId;
    batch->explicitAck = task->ackedSchemaReqId != 0;
    batch->triggerNode = task->nodeId;
    const SArray* external = task->pMgmtReq->cont.pExtOrigTblReaders;
    batch->guardedDeploy = true;
    for (int32_t i = 0; i < taosArrayGetSize(external); ++i) {
      const SStreamExtOrigTblReaderReq* entry = taosArrayGet(external, i);
      if (!(entry->capabilities & STREAM_EXT_ORIGTBL_READER_CAP_GUARDED_DEPLOY)) {
        batch->guardedDeploy = false;
        break;
      }
    }
    (void)atomic_add_fetch_32(&mnode->streamSchemaBatchCount, 1);
    (void)atomic_add_fetch_64(&mnode->streamSchemaBatchBytes, bytes);
    batch->stream = msmAcquireTriggerReaderStream(mnode, owner, task);
    code = batch->stream ? msmPinSchemaInput(batch, owner, batch->stream, task) : TSDB_CODE_MND_STREAM_NOT_RUNNING;
    if (!code) {
      batch->request = task->pMgmtReq;
      code = taosWriteQitem(mnode->streamSchemaWorker.queue, batch);
      if (!code) {
        owner->pendingSchema = batch;
        task->pMgmtReq = NULL;
        return;
      }
      batch->request = NULL;
    }
    batch->refs = 1;
    msmReleaseSchemaBatch(batch);
  }
  msmSchemaRequestError(ctx, task, code);
}

// Called under actionQLock exclusively. Queue/allocation failure leaves the new desired
// incarnation unconfirmed, with the same cleanup and install available for the next retry.
static void msmPrepareSchemaReaderReplay(SStmExtSchema* schema, SStmTaskStatus* reader) {
  if (atomic_load_64(&reader->remoteSchemaConfirmation) != MST_READER_INSTALL_FAILED) return;
  SStreamTask* target = &schema->replayDeploy.deploy.task;
  schema->failedSeriousId = target->seriousId;
  schema->firstDispatched = false;
  int64_t next = msmAssignTaskSeriousId();
  target->seriousId = next > target->seriousId ? next : target->seriousId + 1;
  reader->id.seriousId = target->seriousId;
  atomic_store_64(&reader->remoteSchemaConfirmation, 0);
  // Bound the wait for an unsent replacement. Its first successful dispatch starts the confirmation window.
  atomic_store_64(&reader->readerRepairRetryTs, taosGetTimestampMs() + MST_ISOLATION_DURATION);
  schema->requiredReader.seriousId = target->seriousId;
}

static void msmExpireRemoteSchemaBatch(SStmStatus* owner, SStmSchemaBatch* batch) {
  for (size_t i = 0; i < taosArrayGetSize(batch->schemas); ++i) {
    SStmExtSchema* schema = taosArrayGet(batch->schemas, i);
    int32_t        nodeId = atomic_load_32(&schema->queuedNode);
    // Withdrawing a node's entries clears queuedNode on every matching schema.
    if (nodeId) msmCleanSnodeDeploys(nodeId, batch, false);
  }
  owner->triggerTask->lostSchemaReqId = batch->reqId;
  owner->pendingSchema = NULL;
  msmReleaseSchemaBatch(batch);
}

static int32_t msmRetryRemoteSchemaReaders(const SStmGrpCtx* ctx, SStmStatus* owner, SStmSchemaBatch* batch) {
  int64_t now = taosGetTimestampMs();
  bool    expired = false;
  for (size_t i = 0; i < taosArrayGetSize(batch->schemas); ++i) {
    SStmExtSchema* schema = taosArrayGet(batch->schemas, i);
    if (!schema->requiredReader.taskId || schema->superseded || schema->pendingDispatchId) continue;
    SStmTaskStatus* reader = msmFindSchemaReader(batch->streamId, &schema->requiredReader);
    if (reader && msmReaderConfirmedOnNode(ctx, batch->streamId, reader)) continue;
    if (!reader && batch->rejected) continue;
    if (!reader || (schema->replayDeploy.deploy.task.taskId && msmUnclaimedReaderExpired(schema, now))) {
      msmExpireRemoteSchemaBatch(owner, batch);
      return TSDB_CODE_MND_STREAM_TASK_LOST;
    }
    bool queued = atomic_load_8(&schema->queued);
    if (!queued && schema->replayDeploy.deploy.task.taskId && !atomic_load_8(&schema->legacyDispatched)) {
      msmPrepareSchemaReaderReplay(schema, reader);
      SStmTaskToDeployExt entry = {.deploy = schema->replayDeploy.deploy};
      entry.deploy.msg.reader.ownsExtSpec = false;
      TAOS_CHECK_RETURN(msmQueueRemoteSchemaReader(schema, entry));
      queued = true;
    }
    if (schema->replayDeploy.deploy.task.taskId) {
      if (now >= atomic_load_64(&reader->readerRepairRetryTs)) expired = true;
    } else if (!queued && now - batch->firstReplyTs >= MST_ISOLATION_DURATION) {
      expired = true;  // Reuse or a legacy target with an unguarded copy in flight.
    }
  }
  return expired ? TSDB_CODE_MND_STREAM_TASK_LOST : 0;
}

static int32_t msmAppendLocalSchemaActions(SStmGrpCtx* ctx, SStmSchemaBatch* batch, SStmSchemaReply* pin) {
  if (!batch->remote && batch->hasDeploy && ctx->pReq->snodeId != batch->triggerNode)
    return TSDB_CODE_MND_STREAM_NOT_AVAILABLE;
  SStmStreamDeploy readers = {.streamId = batch->streamId};
  SArray**         undeploy = &ctx->pRsp->undeploy.taskList;
  size_t           initial = taosArrayGetSize(*undeploy);
  size_t           count = taosArrayGetSize(batch->schemas);
  size_t           firstUnsent = count;
  int32_t          code = 0;
  for (size_t i = 0; !code && i < count; ++i) {
    SStmExtSchema* schema = taosArrayGet(batch->schemas, i);
    SStmTaskDeploy deploy = schema->replayDeploy.deploy;
    // Mixed batches send installs and matching cleanups through the confirmed target queue.
    if (schema->superseded || (batch->remote && deploy.task.taskId)) continue;
    bool guarded = deploy.task.taskId && ctx->pReq->readerDeployEpoch;
    if (guarded && batch->sentLocalActions) {
      SStmTaskStatus* reader = msmFindSchemaReader(batch->streamId, &schema->requiredReader);
      if (!reader) {
        code = TSDB_CODE_MND_STREAM_TASK_LOST;
        break;
      }
      msmPrepareSchemaReaderReplay(schema, reader);
      deploy = schema->replayDeploy.deploy;
    }
    if (schema->failedSeriousId) {
      if (!guarded) {
        code = TSDB_CODE_MND_STREAM_TASK_LOST;
        break;
      }
      SStreamTaskUndeploy failed = {.task = deploy.task, .undeployMsg.doCleanup = true};
      failed.task.seriousId = schema->failedSeriousId;
      failed.task.flags |= STREAM_FLAG_REMOTE_READER_DEPLOY;
      if (!*undeploy) *undeploy = taosArrayInit(1, sizeof(failed));
      if (!*undeploy || !taosArrayPush(*undeploy, &failed)) {
        code = terrno;
        break;
      }
    }
    if (schema->replayUndeploy.task.taskId && schema->replayUndeploy.task.nodeId == ctx->pReq->snodeId) {
      SStreamTaskUndeploy cleanup = schema->replayUndeploy;
      if (guarded) cleanup.task.flags |= STREAM_FLAG_REMOTE_READER_DEPLOY;
      if (!*undeploy) *undeploy = taosArrayInit(1, sizeof(SStreamTaskUndeploy));
      if (!*undeploy || !taosArrayPush(*undeploy, &cleanup)) code = terrno;
    }
    if (!code && deploy.task.taskId && ctx->pReq->snodeId == deploy.task.nodeId) {
      if (guarded) deploy.task.flags |= STREAM_FLAG_REMOTE_READER_DEPLOY;
      deploy.msg.reader.ownsExtSpec = false;
      code = msmInitStreamDeploy(&readers, &deploy);
      if (!schema->firstDispatched && firstUnsent == count) firstUnsent = i;
    }
  }
  if (!code && readers.readerTasks) {
    SArray** streams = &ctx->pRsp->deploy.streamList;
    if (!*streams) *streams = taosArrayInit(1, sizeof(SStmStreamDeploy));
    if (!*streams || !taosArrayPush(*streams, &readers)) code = terrno;
  }
  bool firstLegacyReply = !batch->sentLocalActions && !batch->remote && !batch->guardedDeploy && batch->hasDeploy;
  if (code) {
    taosArrayDestroy(readers.readerTasks);
    if (*undeploy) taosArrayPopTailBatch(*undeploy, taosArrayGetSize(*undeploy) - initial);
  } else if (firstUnsent < count || firstLegacyReply) {
    // A complete group can still fail to encode. Reserve it until the response is ready.
    pin->localDispatch = true;
    batch->localDispatchPending = true;
    for (size_t i = firstUnsent; i < count; ++i) {
      SStmExtSchema*     schema = taosArrayGet(batch->schemas, i);
      const SStreamTask* target = &schema->replayDeploy.deploy.task;
      if (schema->firstDispatched || schema->superseded || !target->taskId || target->nodeId != ctx->pReq->snodeId)
        continue;
      SStmTaskStatus* reader = msmFindSchemaReader(batch->streamId, &schema->requiredReader);
      if (reader) msmReserveSchemaDispatch(pin, schema, reader);
    }
  }
  return code;
}

static void msmReplySchemaBatch(SStmGrpCtx* ctx, SStmTaskStatusMsg* task, SStmStatus* owner, SStmSchemaBatch* batch) {
  if (batch->code) {
    // Preparation failed before any reader action. A retransmission can retry immediately.
    msmSchemaRequestError(ctx, task, batch->code);
    owner->pendingSchema = NULL;
    msmReleaseSchemaBatch(batch);
    return;
  }
  if (batch->localDispatchPending) return;
  // Older receivers execute deployments before checking the request number. Never send them
  // a second copy that could arrive after cleanup. A matching report proves delivery of the first.
  if (batch->cached) {
    for (size_t i = 0; i < taosArrayGetSize(batch->cachedReaders); ++i) {
      const SStmCachedReader* reader = taosArrayGet(batch->cachedReaders, i);
      if (!msmRemoteSchemaReaderInstalled(ctx, batch->streamId, reader)) {
        // A runtime change needs a fresh report before this address can be returned.
        if (!batch->unconfirmedSince) batch->unconfirmedSince = ctx->currTs;
        if (ctx->currTs - batch->unconfirmedSince < MST_ISOLATION_DURATION) return;
        msmSchemaRequestError(ctx, task, TSDB_CODE_MND_STREAM_TASK_LOST);
        return;
      }
    }
    batch->unconfirmedSince = 0;
  }
  bool resultOnly =
      batch->published && (batch->remote || (batch->sentLocalActions && !batch->guardedDeploy && batch->hasDeploy));
  if (resultOnly && !batch->rejected && !batch->cached && !msmHeartbeatReportsSchemaReaders(ctx, batch)) {
    bool expired = batch->remote ? msmRetryRemoteSchemaReaders(ctx, owner, batch) == TSDB_CODE_MND_STREAM_TASK_LOST
                                 : ctx->currTs >= batch->localConfirmBy;
    if (expired) {
      msmSchemaRequestError(ctx, task, TSDB_CODE_MND_STREAM_TASK_LOST);
    }
    return;
  }
  SStmSchemaReply* borrowed = msmPinSchemaReply(ctx, batch);
  if (!borrowed) return;
  SArray* replies = ctx->pRsp->rsps.rspList;
  size_t  idx = taosArrayGetSize(replies);
  if (taosArrayEnsureCap(replies, idx + 1)) return;
  if (!batch->published) {
    // Reserve implicit-ack tracking before publication can install or retire any Reader.
    if (!batch->explicitAck && !atomic_load_32(&batch->localAckNode) &&
        msmAddSchemaNodeWait(ctx->pMnode, &batch->localAckNode, batch->triggerNode))
      return;
    batch->publishing = true;
    TSWAP(task->pMgmtReq, batch->request);
    (void)msmDeployTriggerMixedOrigReader(ctx, task, batch->schemas, batch->readerRegistry);
    batch->publishing = false;
    if (idx == taosArrayGetSize(replies)) {
      // Retry with the retained request. Its next publication must rebind every live address under actionQLock.
      int32_t           iter = 0;
      SStmExtReaderKey* key = NULL;
      while ((key = tSimpleHashIterate(batch->readerRegistry, key, &iter))) key->reader = NULL;
      return;
    }
    tSimpleHashCleanup(batch->readerRegistry);
    batch->readerRegistry = NULL;
    batch->reply = *(SStreamMgmtRsp*)taosArrayGet(replies, idx);
    batch->rejected = batch->reply.code != 0;
    batch->reply.task.pMgmtReq = NULL;
    batch->reply.task.extraErrMsg = NULL;
    batch->published = true;
    batch->firstReplyTs = taosGetTimestampMs();
    // Replayed reader deploy messages borrow trigger and scan plans from the pinned input.
    // Source rows and the decoded request are only needed for preparation and publication.
    tFreeSStreamMgmtReq(batch->request);
    taosMemoryFreeClear(batch->request);
    if (batch->stream) mndReleaseStream(batch->mnode, batch->stream);
    batch->stream = NULL;
    size_t cleanupCount = 0;
    for (int32_t i = 0; i < taosArrayGetSize(batch->schemas); ++i) {
      SStmExtSchema* schema = taosArrayGet(batch->schemas, i);
      // A later PATCH or cleanup in this batch may have retired this install.
      SStreamExtOrigTblReaderRsp* result = i < taosArrayGetSize(batch->reply.cont.pExtOrigTblReaders)
                                               ? taosArrayGet(batch->reply.cont.pExtOrigTblReaders, i)
                                               : NULL;
      const SStmCachedReader* required = &schema->requiredReader;
      if (required->taskId) {
        int64_t          key[] = {batch->streamId, required->taskId};
        SStmTaskStatus** live = taosHashGet(mStreamMgmt.taskMap, key, sizeof(key));
        bool             sameAddress = live && (*live)->id.nodeId == required->nodeId;
        schema->superseded = !sameAddress || (*live)->id.seriousId != required->seriousId;
        if (!schema->superseded && schema->replayDeploy.deploy.task.taskId) {
          // The queue and the health check give this incarnation the same initial window.
          atomic_store_64(&(*live)->readerRepairRetryTs, batch->firstReplyTs + MST_ISOLATION_DURATION);
        }
        if (!sameAddress && result) {
          result->readerTaskAddr = (SStreamTaskAddr){0};
          result->code = TSDB_CODE_MND_STREAM_TASK_LOST;
        }
      }
      if (result && result->code) batch->rejected = true;
      const SStreamTask* old = &schema->replayUndeploy.task;
      if (!schema->superseded && schema->replayDeploy.deploy.task.taskId && old->taskId) {
        SStmCachedReader cleanup = {old->taskId, old->seriousId, old->nodeId};
        taosArraySet(batch->pairedCleanups, cleanupCount++, &cleanup);
      }
      if (schema->source) mndReleaseExtSource(batch->mnode, schema->source);
      schema->source = NULL;
      mstDestroySStmTaskToDeployExt(&schema->entry);
      tFreeSStreamExtOrigTblReaderRsp(&schema->result);
    }
    if (batch->pairedCleanups) {
      taosArrayPopTailBatch(batch->pairedCleanups, taosArrayGetSize(batch->pairedCleanups) - cleanupCount);
      if (cleanupCount > 1) taosArraySort(batch->pairedCleanups, compareInt64Val);
    }
    if (batch->remote || !batch->hasDeploy || batch->rejected)
      msmRemoveSchemaNodeWait(ctx->pMnode, &batch->localAckNode);
    if (batch->remote) {
      // Mixed batches also need the Trigger node's Reader confirmed before success.
      (void)msmRetryRemoteSchemaReaders(ctx, owner, batch);
      if (!batch->rejected) {
        (void)msmAppendLocalSchemaActions(ctx, batch, borrowed);
        taosArrayPop(replies);
        return;
      }
    }
  } else if (!taosArrayPush(replies, &batch->reply)) {
    return;
  }
  if (!resultOnly || (batch->rejected && !batch->sentLocalActions)) {
    if (msmAppendLocalSchemaActions(ctx, batch, borrowed)) {
      taosArrayPop(replies);
      return;
    }
    if (!borrowed->localDispatch) batch->sentLocalActions = true;
  }
  if (batch->remote && !batch->explicitAck && !batch->cached &&
      !msmCacheRemoteSchemaResult(ctx->pMnode, owner, borrowed)) {
    taosArrayPop(replies);
    return;
  }
  borrowed->rspIndex = idx;
}
#endif

static int32_t msmHandleHeartbeatTaskRequest(SStmGrpCtx* ctx, SStmTaskStatusMsg* task) {
#ifdef TD_ENTERPRISE
  SStreamMgmtReq* req = task->pMgmtReq;
  if (task->type == STREAM_TRIGGER_TASK) {
    SStmStatus* owner = msmReaderRequestOwner(task);
    if (owner && (!owner->triggerTask || owner->triggerTask->id.taskId != task->taskId ||
                  owner->triggerTask->id.seriousId != task->seriousId))
      owner = NULL;
    SStmSchemaBatch* batch = owner ? owner->pendingSchema : NULL;
    if (owner && req->reqId > 0) {
      if (req->reqId <= owner->triggerTask->ackedSchemaReqId) return 0;
      if (req->reqId == owner->triggerTask->lostSchemaReqId) {
        msmSchemaRequestError(ctx, task, TSDB_CODE_MND_STREAM_TASK_LOST);
        return 0;
      }
    }
    if (owner && task->status == STREAM_STATUS_FAILED) {
      owner->pendingSchema = NULL;
      msmCancelSchemaBatch(batch);
      return 0;
    }
    if (owner && owner->triggerTask->legacySchemaResultId > 0) {
      int64_t           last = owner->triggerTask->legacySchemaResultId;
      SStmSchemaBatch** link = &ctx->pMnode->streamSchemaResults;
      while (*link && ((*link)->streamId != task->streamId || (*link)->taskId != task->taskId ||
                       (*link)->seriousId != task->seriousId || (*link)->reqId != last))
        link = &(*link)->nextResult;
      if (req->reqId == last) {
        if (*link && (*link)->generation == msmPublishedTaskGeneration(owner) &&
            (*link)->buildVersion == atomic_load_64(&owner->buildVersion))
          msmReplySchemaBatch(ctx, task, owner, *link);
        else
          msmSchemaRequestError(ctx, task, TSDB_CODE_MND_STREAM_NOT_AVAILABLE);
        return 0;
      }
      if (req->reqId > last) {
        if (*link) msmRemoveSchemaResult(link);
        owner->triggerTask->legacySchemaResultId = 0;
      }
    }
    if (batch) {
      bool current = batch->seriousId == task->seriousId && batch->taskId == task->taskId &&
                     batch->buildVersion == atomic_load_64(&owner->buildVersion) &&
                     batch->generation == msmPublishedTaskGeneration(owner);
      if (current && req->reqId < batch->reqId) return 0;
      if (current && req->reqId == batch->reqId) {
        if (!batch->publishing && atomic_load_8(&batch->ready)) msmReplySchemaBatch(ctx, task, owner, batch);
        return 0;
      }
      owner->pendingSchema = NULL;
      msmCancelSchemaBatch(batch);
    }
    // A newer request (including a different type) acknowledges all earlier trigger requests.
    if (owner && req->reqId > 0) owner->triggerTask->ackedSchemaReqId = req->reqId - 1;
    if (owner && req->type == STREAM_MGMT_REQ_TRIGGER_MIXED_ORIGTBL_READER) {
      const SArray* external = req->cont.pExtOrigTblReaders;
      int32_t       count = taosArrayGetSize(external);
      for (int32_t i = 0; i < count; ++i) {
        if (msmNeedsExtSchema(taosArrayGet(external, i))) {
          msmQueueSchemaRequest(ctx, task, owner);
          return 0;
        }
      }
      if (count > 1) {
        // Cleanup-only requests bypass the schema worker. Reserve every key before retiring any Reader.
        SSHashObj* registry = tSimpleHashInit(count, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
        int32_t    code = registry ? 0 : terrno;
        for (int32_t i = 0; !code && i < count; ++i) {
          const SStreamExtOrigTblReaderReq* table = taosArrayGet(external, i);
          code = msmAddExtReaderKey(registry, table->registryEntryId, NULL);
        }
        if (code) {
          tSimpleHashCleanup(registry);
          msmSchemaRequestError(ctx, task, code);
          return 0;
        }
        code = msmDeployTriggerMixedOrigReader(ctx, task, NULL, registry);
        tSimpleHashCleanup(registry);
        return code;
      }
    }
  } else if (req->type == STREAM_MGMT_REQ_TRIGGER_MIXED_ORIGTBL_READER) {
    return TSDB_CODE_INVALID_MSG;
  }
#endif
  return msmHandleTaskMgmtReq(ctx, task);
}

int32_t msmHandleStreamRequests(SStmGrpCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStreamHbMsg* pReq = pCtx->pReq;
  SStmTaskStatusMsg* pTask = NULL;
  
  int32_t reqNum = taosArrayGetSize(pReq->pStreamReq);
  if (reqNum > 0 && NULL == pCtx->pRsp->rsps.rspList) {
    pCtx->pRsp->rsps.rspList = taosArrayInit(reqNum, sizeof(SStreamMgmtRsp));
    TSDB_CHECK_NULL(pCtx->pRsp->rsps.rspList, code, lino, _exit, terrno);
  }
  
  for (int32_t i = 0; i < reqNum; ++i) {
    int32_t idx = *(int32_t*)taosArrayGet(pReq->pStreamReq, i);
    pTask = (SStmTaskStatusMsg*)taosArrayGet(pReq->pStreamStatus, idx);
    if (NULL == pTask) {
      mstError("idx %d is NULL, reqNum:%d", idx, reqNum);
      continue;
    }

    if (NULL == pTask->pMgmtReq) {
      msttError("idx %d without mgmtReq", idx);
      continue;
    }

    TAOS_CHECK_EXIT(msmHandleHeartbeatTaskRequest(pCtx, pTask));
    if (pCtx->runtimeExpired) TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_NOT_AVAILABLE);
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmNormalHandleHbMsgImpl(SStmGrpCtx* pCtx, bool* pRecalcStartDeferred, SArray** inputs) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStreamHbMsg* pReq = pCtx->pReq;
  bool          actionQLockRead = false;
  bool          actionQLockWrite = false;

#ifdef TD_ENTERPRISE
  pCtx->readerConfirmation = msmConfirmReaderRuntime(pCtx);
#endif
  if (taosArrayGetSize(pReq->pStreamReq) > 0 && mstWaitLock(&mStreamMgmt.actionQLock, false)) {
    actionQLockWrite = true;
    code = msmHandleStreamRequests(pCtx);
    TAOS_CHECK_EXIT(code);
  }

  // Gate queued deploy/reset until this heartbeat finishes runtime status and post-actions.
  if (!actionQLockWrite) {
    (void)mstWaitLock(&mStreamMgmt.actionQLock, true);
    actionQLockRead = true;
  }

  if (atomic_load_32(&mStreamMgmt.toDeployVgTaskNum) > 0) {
    TAOS_CHECK_EXIT(msmGrpAddDeployVgTasks(pCtx, inputs));
  } else {
    TAOS_CHECK_EXIT(msmUpdateVgroupsUpTs(pCtx));
  }

  int32_t toDeploySnodeNum = atomic_load_32(&mStreamMgmt.toDeploySnodeTaskNum);
  if (toDeploySnodeNum > 0) {
    if (GOT_SNODE(pReq->snodeId)) {
      TAOS_CHECK_EXIT(msmGrpAddDeploySnodeTasks(pCtx, inputs));
    } else {
      mstDebug("toDeploySnodeTaskNum:%d pending but hb not from snode, snodeId:%d, dnodeId:%d", toDeploySnodeNum,
                pReq->snodeId, pReq->dnodeId);
    }
  }

  if (taosHashGetSize(pCtx->deployStm) > 0) {
    TAOS_CHECK_EXIT(msmRspAddStreamsDeploy(pCtx));
  }

  if (taosArrayGetSize(pReq->pStreamStatus) > 0) {
    TAOS_CHECK_EXIT(msmNormalHandleStatusUpdateImpl(pCtx, pRecalcStartDeferred));
  }

  if (taosHashGetSize(pCtx->actionStm) > 0) {
    TAOS_CHECK_EXIT(msmHandleHbPostActions(pCtx));
  }

_exit:

  if (actionQLockRead) {
    taosRUnLockLatch(&mStreamMgmt.actionQLock);
  }
  if (actionQLockWrite) {
    taosWUnLockLatch(&mStreamMgmt.actionQLock);
  }

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

// The semaphore belongs to the mnode. A leadership change can replace the action queue
// while a private build is running, so only reset the flag for the epoch we drained.
void msmDrainStreamActions(SMnode* mnode) {
  (void)mstWaitLock(&mStreamMgmt.heartbeatLock, true);
  (void)mstWaitLock(&mStreamMgmt.runtimeLock, true);
  if (atomic_load_8(&mStreamMgmt.active) && mStreamMgmt.actionQ && atomic_load_8(&mStreamMgmt.actionQ->draining)) {
    int64_t    epoch = mStreamMgmt.stat.activeTimes;
    SStmGrpCtx ctx = {.pMnode = mnode, .currTs = taosGetTimestampMs()};
    (void)mstWaitLock(&mStreamMgmt.actionQLock, false);
    msmHandleStreamActions(&ctx);
    taosWUnLockLatch(&mStreamMgmt.actionQLock);
    if (mStreamMgmt.actionQ && epoch == mStreamMgmt.stat.activeTimes) atomic_store_8(&mStreamMgmt.actionQ->draining, 0);
  }
  taosRUnLockLatch(&mStreamMgmt.runtimeLock);
  taosRUnLockLatch(&mStreamMgmt.heartbeatLock);
}

static void* msmDeployWorker(void* param) {
  SMnode* mnode = param;
  setThreadName("mnode-st-deploy");
  taosSetCpuAffinity(THREAD_CAT_MANAGEMENT);
  while (tsem_wait(&mnode->streamDeploySem) == 0 && !atomic_load_8(&mnode->streamDeployStopping)) {
    msmDrainStreamActions(mnode);
  }
  return NULL;
}

int32_t msmInitDeployWorker(SMnode* mnode) {
  TAOS_CHECK_RETURN(tsem_init(&mnode->streamDeploySem, 0, 0));
  atomic_store_8(&mnode->streamDeployStopping, 0);
  int32_t code = taosThreadCreate(&mnode->streamDeployThread, NULL, msmDeployWorker, mnode);
  if (code) TAOS_UNUSED(tsem_destroy(&mnode->streamDeploySem));
  return code;
}

void msmCleanupDeployWorker(SMnode* mnode) {
  if (!taosCheckPthreadValid(mnode->streamDeployThread)) return;
  atomic_store_8(&mnode->streamDeployStopping, 1);
  TAOS_UNUSED(tsem_post(&mnode->streamDeploySem));
  (void)taosThreadJoin(mnode->streamDeployThread, NULL);
  taosThreadClear(&mnode->streamDeployThread);
  TAOS_UNUSED(tsem_destroy(&mnode->streamDeploySem));
}

static int32_t msmPrepareHeartbeat(SMnode* mnode, int64_t now, SStreamHbMsg* req) {
  SStmGrpCtx ctx = {.pMnode = mnode, .currTs = now, .pReq = req};
  TAOS_CHECK_RETURN(msmCheckUpdateDnodeTs(&ctx));
  if (GOT_SNODE(req->snodeId)) TAOS_CHECK_RETURN(msmUpdateSnodeUpTs(&ctx));
  if (mStreamMgmt.actionQ && atomic_load_64(&mStreamMgmt.actionQ->qRemainNum) &&
      atomic_val_compare_exchange_8(&mStreamMgmt.actionQ->draining, 0, 1) == 0) {
    int32_t code = tsem_post(&mnode->streamDeploySem);
    if (code) {
      atomic_store_8(&mStreamMgmt.actionQ->draining, 0);
      mstError("failed to wake stream deployment worker since %s", tstrerror(code));
    }
  }
  return TSDB_CODE_SUCCESS;
}

void msmEncodeStreamHbRsp(int32_t code, SRpcHandleInfo *pRpcInfo, SMStreamHbRspMsg* pRsp, SRpcMsg* pMsg) {
  int32_t lino = 0;
  int32_t tlen = 0;
  void   *buf = NULL;

  if (TSDB_CODE_SUCCESS != code) {
    goto _exit;
  }

  tEncodeSize(tEncodeStreamHbRsp, pRsp, tlen, code);
  if (code < 0) {
    mstError("encode stream hb msg rsp failed, code:%s", tstrerror(code));
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);    
  }

  buf = rpcMallocCont(tlen + sizeof(SStreamMsgGrpHeader));
  if (buf == NULL) {
    mstError("encode stream hb msg rsp failed, code:%s", tstrerror(terrno));
    TAOS_CHECK_EXIT(terrno);    
  }

  ((SStreamMsgGrpHeader *)buf)->streamGid = pRsp->streamGId;
  void *abuf = POINTER_SHIFT(buf, sizeof(SStreamMsgGrpHeader));

  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
  if ((code = tEncodeStreamHbRsp(&encoder, pRsp)) < 0) {
    rpcFreeCont(buf);
    buf = NULL;
    tEncoderClear(&encoder);
    mstError("encode stream hb msg rsp failed, code:%s", tstrerror(code));
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);    
  }
  tEncoderClear(&encoder);

_exit:

  pMsg->code = code;
  pMsg->info = *pRpcInfo;
  if (TSDB_CODE_SUCCESS == code) {
    pMsg->contLen = tlen + sizeof(SStreamMsgGrpHeader);
    pMsg->pCont = buf;
  }
}

int32_t msmHandleStreamHbMsg(SMnode* pMnode, int64_t currTs, SStreamHbMsg* pHb, SRpcMsg *pReq, SRpcMsg* pRspMsg) {
  int32_t          code = TSDB_CODE_SUCCESS;
  bool             recalcStartDeferred = false;
  SArray*          inputs = NULL;
  SStmGrpCtx       ctx = {.pMnode = pMnode, .currTs = currTs, .pReq = pHb};
  SMStreamHbRspMsg rsp = {0};
  rsp.streamGId = pHb->streamGId;
  rsp.readerDeployEpoch = pHb->readerDeployEpoch;

#ifdef TD_ENTERPRISE
  // Publishing another stream's deployment needs both latches exclusively.
  // Index only this heartbeat's immutable reports before joining the active handlers.
  int32_t schemaIndexCode = msmIndexReportedSchemaReaders(&ctx);
#endif
  (void)mstWaitLock(&mStreamMgmt.heartbeatLock, true);
  (void)mstWaitLock(&mStreamMgmt.runtimeLock, true);

  if (0 == atomic_load_8(&mStreamMgmt.active)) {
    mstWarn("mnode stream become NOT active, ignore stream hb from dnode %d streamGid %d", pHb->dnodeId, pHb->streamGId);
    goto _exit;
  }

  if (mStreamMgmt.state == MND_STM_STATE_NORMAL) {
#ifdef TD_ENTERPRISE
    if (schemaIndexCode) {
      code = schemaIndexCode;
      goto _exit;
    }
#endif
    int64_t epoch = mStreamMgmt.stat.activeTimes;
    code = msmPrepareHeartbeat(pMnode, currTs, pHb);
    if (code) goto _exit;
    if (!msmSameBuildEpoch(epoch)) {
      ctx.runtimeExpired = true;
      code = TSDB_CODE_MND_STREAM_NOT_AVAILABLE;
      goto _exit;
    }
  }

  ctx.tidx = streamGetThreadIdx(mStreamMgmt.threadNum, pHb->streamGId);
  ctx.pRsp = &rsp;
  ctx.deployStm = mStreamMgmt.tCtx[ctx.tidx].deployStm[pHb->streamGId];
  ctx.actionStm = mStreamMgmt.tCtx[ctx.tidx].actionStm[pHb->streamGId];

  switch (atomic_load_8(&mStreamMgmt.state)) {
    case MND_STM_STATE_WATCH:
      code = msmWatchHandleHbMsg(&ctx);
      break;
    case MND_STM_STATE_NORMAL:
      code = msmNormalHandleHbMsgImpl(&ctx, &recalcStartDeferred, &inputs);
      break;
    default:
      mstError("Invalid stream state: %d", mStreamMgmt.state);
      code = TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
      break;
  }

_exit:

  if (!ctx.runtimeExpired) {
    msmCleanStreamGrpCtx(pHb);
    msmClearStreamToDeployMaps(pHb);
  }

  taosRUnLockLatch(&mStreamMgmt.runtimeLock);
  taosRUnLockLatch(&mStreamMgmt.heartbeatLock);
  msmEncodeStreamHbRsp(code, &pReq->info, &rsp, pRspMsg);
#ifdef TD_ENTERPRISE
  msmFinishSchemaResponse(&ctx, pRspMsg->code == 0 && pRspMsg->pCont != NULL);
  taosHashCleanup(ctx.reportedSchemaReaders);
  for (int32_t i = 0; i < taosArrayGetSize(ctx.schemaReplies); ++i) {
    SStmSchemaReply* pin = taosArrayGet(ctx.schemaReplies, i);
    if (pin->rspIndex >= 0) {
      SStreamMgmtRsp* borrowed = taosArrayGet(rsp.rsps.rspList, pin->rspIndex);
      borrowed->cont = (SStreamMgmtRspCont){0};
    }
    msmReleaseSchemaBatch(pin->batch);
  }
  taosArrayDestroy(ctx.schemaReplies);
#endif
  for (int32_t i = 0; i < taosArrayGetSize(inputs); ++i) mstReleaseDeployInput(taosArrayGetP(inputs, i));
  taosArrayDestroy(inputs);
  if (recalcStartDeferred) mndStreamRecalcSchedulePullupPostUnlock(pMnode);

  tFreeSMStreamHbRspMsg(&rsp);

  return code;
}

void msmHandleBecomeLeader(SMnode *pMnode) {
  // Offline one-shot tools (e.g. -fMnode/mndDump.c) replay the wal through a mini-mnode that
  // never calls streamInit(), so the global stream mgmt state stays unallocated and touching it
  // here would dereference a NULL vgLeaders/vgroupMap. Gate on that instead of tsDisableStream:
  // an online dnode always calls streamInit(), even when disableStream is set, so that streams
  // created before the config was enabled can still be stopped and dropped.
  if (!streamMgmtInited()) {
    mstWarn("stream mgmt is not initialized, skip mnode become leader processing");
    return;
  }

  mstInfo("start to process mnode become leader");

  int32_t code = 0;
  streamAddVnodeLeader(MNODE_HANDLE);
  
  taosWLockLatch(&mStreamMgmt.runtimeLock);
  msmDestroyRuntimeInfo(pMnode);
  code = msmInitRuntimeInfo(pMnode);
  taosWUnLockLatch(&mStreamMgmt.runtimeLock);

  if (TSDB_CODE_SUCCESS == code) {
    atomic_store_8(&mStreamMgmt.active, 1);
  }

  mstInfo("mnode stream mgmt active:%d", atomic_load_8(&mStreamMgmt.active));
}

void msmHandleBecomeNotLeader(SMnode *pMnode) {  
  // Same guard as msmHandleBecomeLeader(): the mini-mnode used by offline tools never calls
  // streamInit(), and streamRemoveVnodeLeader() would otherwise touch a NULL gStreamMgmt.
  if (!streamMgmtInited()) {
    mstWarn("stream mgmt is not initialized, skip mnode become not leader processing");
    return;
  }

  mstInfo("start to process mnode become not leader");

  streamRemoveVnodeLeader(MNODE_HANDLE);

  if (atomic_val_compare_exchange_8(&mStreamMgmt.active, 1, 0)) {
    taosWLockLatch(&mStreamMgmt.runtimeLock);
    msmDestroyRuntimeInfo(pMnode);
    mStreamMgmt.stat.inactiveTimes++;
    taosWUnLockLatch(&mStreamMgmt.runtimeLock);
  }
}


static void msmRedeployStream(int64_t streamId, SStmStatus* pStatus) {
  if (1 == atomic_val_compare_exchange_8(&pStatus->stopped, 1, 0)) {
    mstsInfo("try to reset and redeploy stream, deployTimes:%" PRId64, pStatus->deployTimes);
    mstPostStreamAction(mStreamMgmt.actionQ, streamId, pStatus->streamName, NULL, false, STREAM_ACT_DEPLOY);
  } else {
    mstsWarn("stream stopped %d already changed", atomic_load_8(&pStatus->stopped));
  }
}

static bool msmCheckStreamAssign(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStreamObj* pStream = pObj;
  SSnodeObj* pSnode = p1;
  SArray** ppRes = p2;

  // The caller creates *ppRes before sdbTraverse. Do not call sdbGetSize(SDB_STREAM) here:
  // this callback already runs under that table's read lock, and the lock is not recursive.
  if (pStream->mainSnodeId == pSnode->id) {
    TSDB_CHECK_NULL(*ppRes, code, lino, _exit, TSDB_CODE_INVALID_PARA);
    TSDB_CHECK_NULL(taosArrayPush(*ppRes, &pStream), code, lino, _exit, terrno);
  }

  return true;

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }  

  *(int32_t*)p3 = code;

  return false;
}


int32_t msmCheckSnodeReassign(SMnode *pMnode, SSnodeObj* pSnode, SArray** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  // Initial capacity is only a hint. Querying the live size inside the traverse callback
  // deadlocks on the non-recursive SDB_STREAM read lock.
  *ppRes = taosArrayInit(MND_STREAM_DEFAULT_NUM, POINTER_BYTES);
  TSDB_CHECK_NULL(*ppRes, code, lino, _exit, terrno);

  sdbTraverse(pMnode->pSdb, SDB_STREAM, msmCheckStreamAssign, pSnode, ppRes, &code);
  TAOS_CHECK_EXIT(code);

  int32_t streamNum = taosArrayGetSize(*ppRes);
  if (streamNum > 0 && 0 == pSnode->replicaId) {
    mstError("snode %d has no replica while %d streams assigned", pSnode->id, streamNum);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_SNODE_IN_USE);
  }

  //STREAMTODO CHECK REPLICA UPDATED OR NOT

_exit:

  if (code) {
    // The array is created before the traverse now, so a failure must not hand a
    // half-built list back to the caller. The caller only destroys it on success.
    taosArrayDestroy(*ppRes);
    *ppRes = NULL;
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static bool msmCheckLoopStreamSdb(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SStreamObj* pStream = pObj;
  int64_t streamId = pStream->pCreate->streamId;
  SStmStatus* pStatus = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  SStmCheckStatusCtx* pCtx = (SStmCheckStatusCtx*)p1;
  int8_t userDropped = atomic_load_8(&pStream->userDropped), userStopped = atomic_load_8(&pStream->userStopped);

  if ((mStreamMgmt.deployingStreamId == streamId || (pStatus && atomic_load_64(&pStatus->deferredTaskId))) &&
      !userStopped && !userDropped) {
    return true;
  }

  // Batch meta txn (stream phase 2): a stream created inside a transaction is written to
  // SDB immediately as PRE_CREATE, but must not deploy until COMMIT clears the marker.
  // Deploying early would run a stream the user can still ROLLBACK, and on rollback the
  // undo path assumes it was never deployed (mndTxn.c, undo CREATE_STREAM does no
  // snode-side compensation). Skip it explicitly rather than relying on the isolation
  // window, which is a timing parameter and would let a long-running txn's stream through.
  if (pStream->txnId != 0 && (pStream->txnStatus == META_TXN_PRE_CREATE ||
                              pStream->txnStatus == META_TXN_PRE_CREATE_DROP)) {
    mstsDebug("stream is uncommitted in txn:%" PRId64 " (txnStatus:%d), skip deploy until COMMIT",
              pStream->txnId, pStream->txnStatus);
    return true;
  }

  if ((userDropped || userStopped) && (NULL == pStatus)) {
    mstsDebug("stream userDropped %d userStopped %d and not in streamMap, ignore it", userDropped, userStopped);
    return true;
  }
  
  if (pStatus && !MST_STM_PASS_ISOLATION(pStream, pStatus)) {
    mstsDebug("stream not pass isolation time, updateTime:%" PRId64 ", lastActionTs:%" PRId64 ", currentTs %" PRId64 ", ignore check it", 
        pStream->updateTime, pStatus->lastActionTs, mStreamMgmt.hCtx.currentTs);
    return true;
  }

  if (NULL == pStatus && !MST_STM_STATIC_PASS_SHORT_ISOLATION(pStream)) {
    mstsDebug("stream not pass static isolation time, updateTime:%" PRId64 ", currentTs %" PRId64 ", ignore check it", 
        pStream->updateTime, mStreamMgmt.hCtx.currentTs);
    return true;
  }  

  if (pStatus) {
    if (userDropped || userStopped || MST_IS_USER_STOPPED(atomic_load_8(&pStatus->stopped))) {
      (void)msmRemoveStreamFromMaps(pMnode, streamId);
    }

    return true;
  }

  mstPostStreamAction(mStreamMgmt.actionQ, streamId, pStream->pCreate->name, NULL, false, STREAM_ACT_DEPLOY_IF_MISSING);

  return true;
}

void msmCheckLoopStreamMap(SMnode *pMnode) {
  SStmStatus* pStatus = NULL;
  void* pIter = NULL;
  int8_t stopped = 0;
  int64_t streamId = 0;
  
  while (true) {
    pIter = taosHashIterate(mStreamMgmt.streamMap, pIter);
    if (NULL == pIter) {
      break;
    }

    pStatus = (SStmStatus*)pIter;

    streamId = *(int64_t*)taosHashGetKey(pIter, NULL);
    if (mStreamMgmt.deployingStreamId == streamId || atomic_load_64(&pStatus->deferredTaskId)) continue;
    stopped = atomic_load_8(&pStatus->stopped);
    if (MST_IS_USER_STOPPED(stopped)) {
      mstsDebug("stream already stopped by user, deployTimes:%" PRId64, pStatus->deployTimes);
      (void)msmRemoveStreamFromMaps(pMnode, streamId);
      continue;
    }

    if (!sdbCheckExists(pMnode->pSdb, SDB_STREAM, pStatus->streamName)) {
      mstsDebug("stream no longer exists, deployTimes:%" PRId64, pStatus->deployTimes);
      (void)msmRemoveStreamFromMaps(pMnode, *(int64_t*)taosHashGetKey(pIter, NULL));
      continue;
    }

    if (MST_IS_ERROR_STOPPED(stopped)) {
      if (mStreamMgmt.hCtx.currentTs < pStatus->fatalRetryTs) {
        mstsDebug("stream already stopped by error %s, retried times:%" PRId64 ", next time not reached, currTs:%" PRId64 ", nextRetryTs:%" PRId64,
            tstrerror(pStatus->fatalError), pStatus->fatalRetryTimes, mStreamMgmt.hCtx.currentTs, pStatus->fatalRetryTs);
            
        MND_STREAM_SET_LAST_TS(STM_EVENT_STM_TERR, mStreamMgmt.hCtx.currentTs);
        continue;
      }

      (void)mstPostStreamRetry(mStreamMgmt.actionQ, streamId, pStatus, stopped);
      continue;
    }

    if (MST_IS_GRANT_STOPPED(stopped) && TSDB_CODE_SUCCESS == grantCheckExpire(TSDB_GRANT_STREAMS)) {
      (void)mstPostStreamRetry(mStreamMgmt.actionQ, streamId, pStatus, stopped);
      continue;
    }
  }
}

// Walk SDB_STREAM one row at a time. sdbFetch holds the table read lock only while it
// advances the iterator and bumps the row refcount, then unlocks before returning.
// Map lookup, stream removal and deploy enqueue therefore no longer sit in front of
// the snapshot writer's write lock. The row stays alive until sdbRelease.
static void msmCheckLoopStreamSdbUnlocked(SMnode* pMnode, SStmCheckStatusCtx* pCtx) {
  void*   pIter = NULL;
  int32_t checked = 0;
  int64_t loopStartMs = taosGetMonoTimestampMs();

  while (1) {
    SStreamObj* pStream = NULL;
    int64_t     fetchStartMs = taosGetMonoTimestampMs();
    pIter = sdbFetch(pMnode->pSdb, SDB_STREAM, pIter, (void**)&pStream);
    int64_t fetchWaitMs = taosGetMonoTimestampMs() - fetchStartMs;
    if (fetchWaitMs >= 100) {
      mstInfo("stream health check waited %" PRId64 " ms to fetch the next stream, checked:%d", fetchWaitMs, checked);
    }
    if (pIter == NULL) {
      break;
    }

    // The callback always continues. Keep the return value so an early stop still
    // releases the hash iterator instead of leaking its node refcount.
    bool cont = msmCheckLoopStreamSdb(pMnode, pStream, pCtx, NULL, NULL);
    sdbRelease(pMnode->pSdb, pStream);
    checked++;
    if (!cont) {
      sdbCancelFetch(pMnode->pSdb, pIter);
      mstInfo("stream health check stopped early after checking %d streams", checked);
      break;
    }
  }

  int64_t elapsedMs = taosGetMonoTimestampMs() - loopStartMs;
  if (elapsedMs >= 100) {
    mstInfo("stream health check finished, checked:%d, elapsed:%" PRId64 " ms", checked, elapsedMs);
  } else {
    mstDebug("stream health check finished, checked:%d, elapsed:%" PRId64 " ms", checked, elapsedMs);
  }
}

void msmCheckStreamsStatus(SMnode *pMnode) {
  SStmCheckStatusCtx ctx = {0};

  mstDebug("start to check streams status, currTs:%" PRId64, mStreamMgmt.hCtx.currentTs);
  
  if (MST_READY_FOR_SDB_LOOP()) {
    mstDebug("ready to check sdb loop, lastLoopSdbTs:%" PRId64, mStreamMgmt.lastTs[STM_EVENT_LOOP_SDB].ts);
    msmCheckLoopStreamSdbUnlocked(pMnode, &ctx);
    MND_STREAM_SET_LAST_TS(STM_EVENT_LOOP_SDB, mStreamMgmt.hCtx.currentTs);
  }

  if (MST_READY_FOR_MAP_LOOP()) {
    mstDebug("ready to check map loop, lastLoopMapTs:%" PRId64, mStreamMgmt.lastTs[STM_EVENT_LOOP_MAP].ts);
    msmCheckLoopStreamMap(pMnode);
    MND_STREAM_SET_LAST_TS(STM_EVENT_LOOP_MAP, mStreamMgmt.hCtx.currentTs);
  }
}

void msmCheckTaskListStatus(int64_t streamId, SStmTaskStatus** pList, int32_t taskNum) {
  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskStatus* pTask = *(pList + i);

    if (atomic_load_8(&((SStmStatus*)pTask->pStream)->stopped)) {
      continue;
    }

    if (!MST_PASS_ISOLATION(pTask->lastUpTs, 1) || mStreamMgmt.hCtx.currentTs < pTask->readerRepairRetryTs) {
      continue;
    }

    int64_t noUpTs = mStreamMgmt.hCtx.currentTs - pTask->lastUpTs;
    if (STREAM_RUNNER_TASK == pTask->type || STREAM_TRIGGER_TASK == pTask->type) {
      mstsWarn("%s TASK:%" PRIx64 " status not updated for %" PRId64 "ms, will try to redeploy it", 
          gStreamTaskTypeStr[pTask->type], pTask->id.taskId, noUpTs);
          
      msmStopStreamByError(streamId, NULL, TSDB_CODE_MND_STREAM_TASK_LOST, mStreamMgmt.hCtx.currentTs);
      break;
    }

    mstsInfo("%s TASK:%" PRIx64 " status not updated for %" PRId64 "ms, will try to redeploy it", 
        gStreamTaskTypeStr[pTask->type], pTask->id.taskId, noUpTs);

    SStmTaskAction task = {0};
    task.streamId = streamId;
    task.generation = msmPublishedTaskGeneration(pTask->pStream);
    task.id = pTask->id;
    ++task.id.seriousId;
    task.flag = pTask->flags;
    task.type = pTask->type;
    // Health owns runtimeLock exclusively. Commit the new serial only after enqueue succeeds.
    if (!mstPostTaskAction(mStreamMgmt.actionQ, &task, STREAM_ACT_DEPLOY)) {
      (void)mstBumpTaskSeriousId(pTask);
      pTask->readerRepairRetryTs = INT64_MAX;
    }
  }
}

void msmCheckVgroupStreamStatus(SHashObj* pStreams) {
  void* pIter = NULL;
  SStmVgStreamStatus* pVg = NULL;
  int64_t streamId = 0;
  
  while (true) {
    pIter = taosHashIterate(pStreams, pIter);
    if (NULL == pIter) {
      break;
    }

    streamId = *(int64_t*)taosHashGetKey(pIter, NULL);
    pVg = (SStmVgStreamStatus*)pIter;

    int32_t taskNum = taosArrayGetSize(pVg->trigReaders);
    if (taskNum > 0) {
      msmCheckTaskListStatus(streamId, taosArrayGet(pVg->trigReaders, 0), taskNum);
    }

    taskNum = taosArrayGetSize(pVg->calcReaders);
    if (taskNum > 0) {
      msmCheckTaskListStatus(streamId, taosArrayGet(pVg->calcReaders, 0), taskNum);
    }
  }
}

void msmHandleVgroupLost(SMnode *pMnode, int32_t vgId, SStmVgroupStatus* pVg) {
  int64_t streamId = 0;
  void* pIter = NULL;
  SStmVgStreamStatus* pStream = NULL;

  if (!MST_PASS_ISOLATION(pVg->lastUpTs, 5)) {
    mstDebug("vgroup %d lost and still in watch time, lastUpTs:%" PRId64 ", streamNum:%d", vgId, pVg->lastUpTs, (int32_t)taosHashGetSize(pVg->streamTasks));
    return;
  }

  
  while (true) {
    pIter = taosHashIterate(pVg->streamTasks, pIter);
    if (NULL == pIter) {
      break;
    }

    streamId = *(int64_t*)taosHashGetKey(pIter, NULL);
    
    msmStopStreamByError(streamId, NULL, TSDB_CODE_MND_STREAM_VGROUP_LOST, mStreamMgmt.hCtx.currentTs);
  }

  taosHashClear(pVg->streamTasks);
}


void msmCheckVgroupStatus(SMnode *pMnode) {
  void* pIter = NULL;
  int32_t code = 0;
  
  while (true) {
    pIter = taosHashIterate(mStreamMgmt.vgroupMap, pIter);
    if (NULL == pIter) {
      break;
    }

    int32_t vgId = *(int32_t*)taosHashGetKey(pIter, NULL);
    if ((vgId % MND_STREAM_ISOLATION_PERIOD_NUM) != mStreamMgmt.hCtx.slotIdx) {
      continue;
    }
    
    SStmVgroupStatus* pVg = (SStmVgroupStatus*)pIter;

    if (MST_PASS_ISOLATION(pVg->lastUpTs, 1)) {
      SVgObj *pVgroup = mndAcquireVgroup(pMnode, vgId);
      if (NULL == pVgroup) {
        mstDebug("vgroup %d no longer exits, will remove all %d tasks in it", vgId, (int32_t)taosHashGetSize(pVg->streamTasks));
        code = taosHashRemove(mStreamMgmt.vgroupMap, &vgId, sizeof(vgId));
        if (code) {
          mstWarn("remove vgroup %d from vgroupMap failed since %s", vgId, tstrerror(code));
        }
        continue;
      }
      mndReleaseVgroup(pMnode, pVgroup);
      
      mstWarn("vgroup %d lost, lastUpTs:%" PRId64 ", streamNum:%d", vgId, pVg->lastUpTs, (int32_t)taosHashGetSize(pVg->streamTasks));
      
      msmHandleVgroupLost(pMnode, vgId, pVg);
      continue;
    }

    mstDebug("vgroup %d online, try to check tasks status, currTs:%" PRId64 ", lastUpTs:%" PRId64, vgId, mStreamMgmt.hCtx.currentTs, pVg->lastUpTs);

    msmCheckVgroupStreamStatus(pVg->streamTasks);
  }
}

void msmHandleRunnerRedeploy(int64_t streamId, SStmSnodeStreamStatus* pStream, int32_t* deployNum, int32_t* deployId) {
  *deployNum = 0;
  
  for (int32_t i = 0; i < MND_STREAM_RUNNER_DEPLOY_NUM; ++i) {
    if (pStream->runners[i]) {
      int32_t taskNum = taosArrayGetSize(pStream->runners[i]);
      for (int32_t t = 0; t < taskNum; ++t) {
        SStmTaskStatus* pTask = taosArrayGetP(pStream->runners[i], t);
        int8_t stopped = atomic_load_8(&((SStmStatus*)pTask->pStream)->stopped);
        if (stopped) {
          mstsDebug("stream already stopped %d, ignore it", stopped);
          *deployNum = 0;
          return;
        }

        int64_t newSid = mstBumpTaskSeriousId(pTask);
        mstsDebug("task %" PRIx64 " SID updated to %" PRIx64, pTask->id.taskId, newSid);
      }
      
      deployId[*deployNum] = i;
      (*deployNum)++;
    }
  }
}

static void msmHandleSnodeLost(SMnode* pMnode, int32_t snodeId, SStmSnodeStatus* pSnode) {
  pSnode->runnerThreadNum = -1;

  (void)msmSTAddSnodesToMap(pMnode);

#ifdef TD_ENTERPRISE
  // Clear this node's queued references once before each owner withdraws its remaining targets.
  msmCleanSnodeDeploys(snodeId, NULL, true);
#endif

  int64_t streamId = 0;
  void* pIter = NULL;
  SStmSnodeStreamStatus* pStream = NULL;
  int32_t deployNum = 0;
  SStmTaskAction task = {0};
  
  while (true) {
    pIter = taosHashIterate(pSnode->streamTasks, pIter);
    if (NULL == pIter) {
      break;
    }

    streamId = *(int64_t*)taosHashGetKey(pIter, NULL);
    
    task.streamId = streamId;
    
    pStream = (SStmSnodeStreamStatus*)pIter;
    if (pStream->trigger) {
      int8_t stopped = atomic_load_8(&((SStmStatus*)pStream->trigger->pStream)->stopped);
      if (stopped) {
        mstsDebug("stream already stopped %d, ignore it", stopped);
        continue;
      }

      mstsInfo("snode lost with trigger task %" PRIx64 ", will try to restart current stream", pStream->trigger->id.taskId);
      
      msmStopStreamByError(streamId, NULL, TSDB_CODE_MND_STREAM_SNODE_LOST, mStreamMgmt.hCtx.currentTs);
    } else {
      msmHandleRunnerRedeploy(streamId, pStream, &task.deployNum, task.deployId);
      SStmStatus* owner = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
#ifdef TD_ENTERPRISE
      SStmSchemaBatch* pending = owner ? owner->pendingSchema : NULL;
      if (pending && pending->published && pending->remote && !pending->rejected) {
        for (size_t i = 0; i < taosArrayGetSize(pending->schemas); ++i) {
          SStmExtSchema* schema = taosArrayGet(pending->schemas, i);
          if (!schema->superseded && schema->requiredReader.taskId &&
              !msmFindSchemaReader(streamId, &schema->requiredReader)) {
            msmExpireRemoteSchemaBatch(owner, pending);
            break;
          }
        }
      }
#endif
      if (task.deployNum > 0) {
        if (!owner) continue;
        task.generation = msmPublishedTaskGeneration(owner);
        task.multiRunner = true;
        task.type = STREAM_RUNNER_TASK;
        
        mstPostTaskAction(mStreamMgmt.actionQ, &task, STREAM_ACT_DEPLOY);
        
        mstsInfo("runner tasks %d redeploys added to actionQ", task.deployNum);
      }
    }
  }

  taosHashClear(pSnode->streamTasks);
}

void msmCheckSnodeStreamStatus(SHashObj* pStreams) {
  void* pIter = NULL;
  SStmSnodeStreamStatus* pSnode = NULL;
  int64_t streamId = 0;
  
  while (true) {
    pIter = taosHashIterate(pStreams, pIter);
    if (NULL == pIter) {
      break;
    }

    streamId = *(int64_t*)taosHashGetKey(pIter, NULL);
    pSnode = (SStmSnodeStreamStatus*)pIter;

    if (NULL != pSnode->trigger) {
      msmCheckTaskListStatus(streamId, &pSnode->trigger, 1);
    }

    for (int32_t i = 0; i < MND_STREAM_RUNNER_DEPLOY_NUM; ++i) {
      int32_t taskNum = taosArrayGetSize(pSnode->runners[i]);
      if (taskNum > 0) {
        msmCheckTaskListStatus(streamId, taosArrayGet(pSnode->runners[i], 0), taskNum);
      }
    }
  }
}


void msmCheckSnodeStatus(SMnode *pMnode) {
  void* pIter = NULL;
  
  while (true) {
    pIter = taosHashIterate(mStreamMgmt.snodeMap, pIter);
    if (NULL == pIter) {
      break;
    }

    int32_t snodeId = *(int32_t*)taosHashGetKey(pIter, NULL);
    if ((snodeId % MND_STREAM_ISOLATION_PERIOD_NUM) != mStreamMgmt.hCtx.slotIdx) {
      continue;
    }

    mstDebug("start to check snode %d status, currTs:%" PRId64, snodeId, mStreamMgmt.hCtx.currentTs);
    
    SStmSnodeStatus* pSnode = (SStmSnodeStatus*)pIter;
    if (NULL == pSnode->streamTasks) {
      mstDebug("ignore snode %d health check since empty tasks", snodeId);
      continue;
    }
    
    if (MST_PASS_ISOLATION(pSnode->lastUpTs, 1)) {
      mstInfo("snode %d lost, lastUpTs:%" PRId64 ", runnerThreadNum:%d, streamNum:%d", 
          snodeId, pSnode->lastUpTs, pSnode->runnerThreadNum, (int32_t)taosHashGetSize(pSnode->streamTasks));

      msmHandleSnodeLost(pMnode, snodeId, pSnode);
      continue;
    }
    
    mstDebug("snode %d online, try to check tasks status, currTs:%" PRId64 ", lastUpTs:%" PRId64, snodeId, mStreamMgmt.hCtx.currentTs, pSnode->lastUpTs);

    msmCheckSnodeStreamStatus(pSnode->streamTasks);
  }
}


void msmCheckTasksStatus(SMnode *pMnode) {
  mstDebug("start to check tasks status, currTs:%" PRId64, mStreamMgmt.hCtx.currentTs);

  msmCheckVgroupStatus(pMnode);
  msmCheckSnodeStatus(pMnode);
}

void msmCheckSnodesState(SMnode *pMnode) {
  if (!MST_READY_FOR_SNODE_LOOP()) {
    return;
  }

  mstDebug("ready to check snode loop, lastTs:%" PRId64, mStreamMgmt.lastTs[STM_EVENT_LOOP_SNODE].ts);

  void* pIter = NULL;
  int32_t snodeId = 0;
  while (true) {
    pIter = taosHashIterate(mStreamMgmt.snodeMap, pIter);
    if (NULL == pIter) {
      break;
    }

    snodeId = *(int32_t*)taosHashGetKey(pIter, NULL);
    if (sdbCheckExists(pMnode->pSdb, SDB_SNODE, &snodeId)) {
      continue;
    }

    SStmSnodeStatus* pSnode = (SStmSnodeStatus*)pIter;
    if (NULL == pSnode->streamTasks) {
      mstDebug("snode %d already cleanup, try to rm it", snodeId);
      TAOS_UNUSED(taosHashRemove(mStreamMgmt.snodeMap, &snodeId, sizeof(snodeId)));
      continue;
    }
    
    mstWarn("snode %d lost while streams remain, will redeploy all and rm it, lastUpTs:%" PRId64 ", runnerThreadNum:%d, streamNum:%d", 
        snodeId, pSnode->lastUpTs, pSnode->runnerThreadNum, (int32_t)taosHashGetSize(pSnode->streamTasks));

    msmHandleSnodeLost(pMnode, snodeId, pSnode);
  }

  MND_STREAM_SET_LAST_TS(STM_EVENT_LOOP_MAP, mStreamMgmt.hCtx.currentTs);
}

bool msmCheckNeedHealthCheck(SMnode *pMnode) {
  int8_t active = atomic_load_8(&mStreamMgmt.active), state = atomic_load_8(&mStreamMgmt.state);
  if (0 == active || MND_STM_STATE_NORMAL != state) {
    mstTrace("ignore health check since active:%d state:%d", active, state);
    return false;
  }

  if (sdbGetSize(pMnode->pSdb, SDB_STREAM) <= 0) {
    mstTrace("ignore health check since no stream now");
    return false;
  }

  return true;
}

void msmHealthCheck(SMnode *pMnode) {
  if (!msmCheckNeedHealthCheck(pMnode)) {
    return;
  }

  mstDebug("start wait health check, currentTs:%" PRId64,  taosGetTimestampMs());

  // Drain in-flight heartbeats before registering a runtime writer. Status queries and logins
  // sharing the mnode read queue can progress while the remaining shared heartbeat work drains.
  (void)mstWaitLock(&mStreamMgmt.heartbeatLock, false);
  (void)mstWaitLock(&mStreamMgmt.runtimeLock, false);
  if (!msmCheckNeedHealthCheck(pMnode)) {
    taosWUnLockLatch(&mStreamMgmt.runtimeLock);
    taosWUnLockLatch(&mStreamMgmt.heartbeatLock);
    return;
  }
  
  mStreamMgmt.hCtx.slotIdx = (mStreamMgmt.hCtx.slotIdx + 1) % MND_STREAM_ISOLATION_PERIOD_NUM;
  mStreamMgmt.hCtx.currentTs = taosGetTimestampMs();

  mstDebug("start health check, soltIdx:%d, checkStartTs:%" PRId64, mStreamMgmt.hCtx.slotIdx, mStreamMgmt.hCtx.currentTs);
  
  msmCheckStreamsStatus(pMnode);
  msmCheckTasksStatus(pMnode);
  msmCheckSnodesState(pMnode);

  taosWUnLockLatch(&mStreamMgmt.runtimeLock);
  taosWUnLockLatch(&mStreamMgmt.heartbeatLock);

  mstDebug("end health check, soltIdx:%d, checkStartTs:%" PRId64, mStreamMgmt.hCtx.slotIdx, mStreamMgmt.hCtx.currentTs);
}

static bool msmUpdateProfileStreams(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SStreamObj *pStream = pObj;
  if (atomic_load_8(&pStream->userDropped) || atomic_load_8(&pStream->userStopped)) {
    return true;
  }
  
  pStream->updateTime = *(int64_t*)p1;
  
  (*(int32_t*)p2)++;
  
  return true;
}

int32_t msmGetTriggerTaskAddr(SMnode *pMnode, int64_t streamId, SStreamTaskAddr* pAddr) {
  int32_t code = 0;
  int8_t  stopped = 0;
  
  (void)mstWaitLock(&mStreamMgmt.runtimeLock, true);
  
  SStmStatus* pStatus = (SStmStatus*)taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStatus) {
    mstsError("stream does not exist in streamMap, streamRemains:%d", taosHashGetSize(mStreamMgmt.streamMap));
    code = TSDB_CODE_MND_STREAM_NOT_RUNNING;
    goto _exit;
  }

  stopped = atomic_load_8(&pStatus->stopped);
  if (stopped) {
    mstsError("stream already stopped, stopped:%d", stopped);
    code = TSDB_CODE_MND_STREAM_NOT_RUNNING;
    goto _exit;
  }

  if (pStatus->triggerTask && STREAM_STATUS_RUNNING == pStatus->triggerTask->status) {
    pAddr->taskId = pStatus->triggerTask->id.taskId;
    pAddr->nodeId = pStatus->triggerTask->id.nodeId;
    pAddr->epset = mndGetDnodeEpsetById(pMnode, pAddr->nodeId);
    mstsDebug("stream trigger task %" PRIx64 " got with nodeId %d", pAddr->taskId, pAddr->nodeId);
    goto _exit;
  }

  mstsError("trigger task %p not running, status:%s", pStatus->triggerTask, pStatus->triggerTask ? gStreamStatusStr[pStatus->triggerTask->status] : "unknown");
  code = TSDB_CODE_MND_STREAM_NOT_RUNNING;

_exit:
  
  taosRUnLockLatch(&mStreamMgmt.runtimeLock);

  return code;
}

int32_t msmInitRuntimeInfo(SMnode *pMnode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t vnodeNum = sdbGetSize(pMnode->pSdb, SDB_VGROUP);
  int32_t snodeNum = sdbGetSize(pMnode->pSdb, SDB_SNODE);
  int32_t dnodeNum = sdbGetSize(pMnode->pSdb, SDB_DNODE);

  atomic_store_8(&mStreamMgmt.recalcPullupPending, 0);
  mStreamMgmt.recalcPullupCursorValid = 0;
  mStreamMgmt.recalcPullupCursor = 0;
  mStreamMgmt.recalcPullupRoundRemaining = 0;

  MND_STREAM_SET_LAST_TS(STM_EVENT_ACTIVE_BEGIN, taosGetTimestampMs());

  mStreamMgmt.stat.activeTimes++;
  mStreamMgmt.threadNum = tsNumOfMnodeStreamMgmtThreads;
  mStreamMgmt.tCtx = taosMemoryCalloc(mStreamMgmt.threadNum, sizeof(SStmThreadCtx));
  if (NULL == mStreamMgmt.tCtx) {
    code = terrno;
    mstError("failed to initialize the stream runtime tCtx, threadNum:%d, error:%s", mStreamMgmt.threadNum, tstrerror(code));
    goto _exit;
  }

  mStreamMgmt.actionQ = taosMemoryCalloc(1, sizeof(SStmActionQ));
  if (mStreamMgmt.actionQ == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime actionQ, error:%s", tstrerror(code));
    goto _exit;
  }
  
  mStreamMgmt.actionQ->head = taosMemoryCalloc(1, sizeof(SStmQNode));
  TSDB_CHECK_NULL(mStreamMgmt.actionQ->head, code, lino, _exit, terrno);
  
  mStreamMgmt.actionQ->tail = mStreamMgmt.actionQ->head;
  mStreamMgmt.actionQ->queuedCreates =
      taosHashInit(MND_STREAM_DEFAULT_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  TSDB_CHECK_NULL(mStreamMgmt.actionQ->queuedCreates, code, lino, _exit, terrno);

  for (int32_t i = 0; i < mStreamMgmt.threadNum; ++i) {
    SStmThreadCtx* pCtx = mStreamMgmt.tCtx + i;

    for (int32_t m = 0; m < STREAM_MAX_GROUP_NUM; ++m) {
      pCtx->deployStm[m] = taosHashInit(snodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
      if (pCtx->deployStm[m] == NULL) {
        code = terrno;
        mError("failed to initialize the stream runtime deployStm[%d][%d], error:%s", i, m, tstrerror(code));
        goto _exit;
      }
      // Staged outgoing groups own the same fields as the finished heartbeat response.
      taosHashSetFreeFp(pCtx->deployStm[m], tFreeSStmStreamDeploy);

      pCtx->actionStm[m] = taosHashInit(snodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
      if (pCtx->actionStm[m] == NULL) {
        code = terrno;
        mError("failed to initialize the stream runtime actionStm[%d][%d], error:%s", i, m, tstrerror(code));
        goto _exit;
      }
      taosHashSetFreeFp(pCtx->actionStm[m], mstDestroySStmAction);
    }
  }
  
  mStreamMgmt.streamMap = taosHashInit(MND_STREAM_DEFAULT_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.streamMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime streamMap, error:%s", tstrerror(code));
    goto _exit;
  }
  taosHashSetFreeFp(mStreamMgmt.streamMap, mstDestroySStmStatus);
  
  mStreamMgmt.taskMap = taosHashInit(MND_STREAM_DEFAULT_TASK_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.taskMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime taskMap, error:%s", tstrerror(code));
    goto _exit;
  }
  
  mStreamMgmt.vgroupMap = taosHashInit(vnodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.vgroupMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime vgroupMap, error:%s", tstrerror(code));
    goto _exit;
  }
  taosHashSetFreeFp(mStreamMgmt.vgroupMap, mstDestroySStmVgroupStatus);

  mStreamMgmt.snodeMap = taosHashInit(snodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.snodeMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime snodeMap, error:%s", tstrerror(code));
    goto _exit;
  }
  taosHashSetFreeFp(mStreamMgmt.snodeMap, mstDestroySStmSnodeStatus);
  
  mStreamMgmt.dnodeMap = taosHashInit(dnodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.dnodeMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime dnodeMap, error:%s", tstrerror(code));
    goto _exit;
  }

  mStreamMgmt.toDeployVgMap = taosHashInit(vnodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.toDeployVgMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime toDeployVgMap, error:%s", tstrerror(code));
    goto _exit;
  }
  taosHashSetFreeFp(mStreamMgmt.toDeployVgMap, mstDestroySStmVgTasksToDeploy);
  
  mStreamMgmt.toDeploySnodeMap = taosHashInit(snodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.toDeploySnodeMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime toDeploySnodeMap, error:%s", tstrerror(code));
    goto _exit;
  }
  taosHashSetFreeFp(mStreamMgmt.toDeploySnodeMap, mstDestroySStmSnodeTasksDeploy);

  mStreamMgmt.toUpdateScanMap = taosHashInit(snodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (mStreamMgmt.toUpdateScanMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime toUpdateScanMap, error:%s", tstrerror(code));
    goto _exit;
  }
  taosHashSetFreeFp(mStreamMgmt.toUpdateScanMap, mstDestroyScanAddrList);

  TAOS_CHECK_EXIT(msmSTAddSnodesToMap(pMnode));
  TAOS_CHECK_EXIT(msmSTAddDnodesToMap(pMnode));

  atomic_store_64(&mStreamMgmt.lastTaskId, 1);

  int32_t activeStreamNum = 0;
  sdbTraverse(pMnode->pSdb, SDB_STREAM, msmUpdateProfileStreams, &MND_STREAM_GET_LAST_TS(STM_EVENT_ACTIVE_BEGIN), &activeStreamNum, NULL);

  if (activeStreamNum > 0) {
    msmSetInitRuntimeState(MND_STM_STATE_WATCH);
  } else {
    msmSetInitRuntimeState(MND_STM_STATE_NORMAL);
  }

_exit:

  if (code) {
    msmDestroyRuntimeInfo(pMnode);
    mstError("%s failed at line %d since %s", __FUNCTION__, lino, tstrerror(code));
  } else {
    mstInfo("mnode stream runtime init done");
  }

  return code;
}
