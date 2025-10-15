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
#include "mndDb.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndTrans.h"
#include "osMemory.h"
#include "parser.h"
#include "taoserror.h"
#include "tmisce.h"
#include "tname.h"
#include "mndDnode.h"
#include "mndVgroup.h"
#include "mndSnode.h"
#include "mndMnode.h"
#include "cmdnodes.h"

void msmDestroyActionQ() {
  SStmQNode* pQNode = NULL;

  if (NULL == mStreamMgmt.actionQ) {
    return;
  }

  while (mndStreamActionDequeue(mStreamMgmt.actionQ, &pQNode)) {
  }

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


void msmStopStreamByError(int64_t streamId, SStmStatus* pStatus, int32_t errCode, int64_t currTs) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmStatus* pStream = NULL;

  mstsInfo("try to stop stream for error: %s", tstrerror(errCode));

  if (NULL == pStatus) {
    pStream = (SStmStatus*)taosHashAcquire(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
    if (NULL == pStream) {
      mstsInfo("stream already not in streamMap, error:%s", tstrerror(terrno));
      goto _exit;
    }

    pStatus = pStream;
  }

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
  pStatus->fatalRetryDuration = (pStatus->fatalRetryTimes > 10) ? MST_MAX_RETRY_DURATION : MST_ISOLATION_DURATION;
  pStatus->fatalRetryTs = currTs + pStatus->fatalRetryDuration;

  pStatus->stat.lastError = errCode;
    
  if (0 == atomic_val_compare_exchange_8(&pStatus->stopped, 0, 1)) {
    MND_STREAM_SET_LAST_TS(STM_EVENT_STM_TERR, currTs);
  }

_exit:

  taosHashRelease(mStreamMgmt.streamMap, pStream);

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
}


static void msmSetInitRuntimeState(int8_t state) {
  switch (state) {
    case MND_STM_STATE_WATCH:
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
      continue;
    }

    taosWLockLatch(&pVg->lock);
    if (NULL == pVg->taskList) {
      pVg->taskList = taosArrayInit(20, sizeof(SStmTaskToDeployExt));
      TSDB_CHECK_NULL(pVg->taskList, code, lino, _return, terrno);
    }
    if (NULL == taosArrayPush(pVg->taskList, &ext)) {
      taosWUnLockLatch(&pVg->lock);
      TSDB_CHECK_NULL(NULL, code, lino, _return, terrno);
    }
    taosWUnLockLatch(&pVg->lock);
    
    taosHashRelease(pVgMap, pVg);
    break;
  }
  
_return:

  if (code) {
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
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SStmSnodeStatus* pSnode = taosHashGet(mStreamMgmt.snodeMap, &pStatus->id.nodeId, sizeof(pStatus->id.nodeId));
  if (NULL == pSnode) {
    mstsWarn("snode %d not exists in snodeMap anymore, may be dropped", pStatus->id.nodeId);
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



static int32_t msmTDAddTriggerToSnodeMap(SStmTaskDeploy* pDeploy, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStmSnodeTasksDeploy snode = {0};
  SStmTaskToDeployExt ext;
  SStreamTask* pTask = &pDeploy->task;

  while (true) {
    SStmSnodeTasksDeploy* pSnode = taosHashAcquire(mStreamMgmt.toDeploySnodeMap, &pDeploy->task.nodeId, sizeof(pDeploy->task.nodeId));
    if (NULL == pSnode) {
      snode.triggerList = taosArrayInit(10, sizeof(SStmTaskToDeployExt));
      TSDB_CHECK_NULL(snode.triggerList, code, lino, _return, terrno);

      ext.deploy = *pDeploy;
      ext.deployed = false;
      TSDB_CHECK_NULL(taosArrayPush(snode.triggerList, &ext), code, lino, _return, terrno);

      code = taosHashPut(mStreamMgmt.toDeploySnodeMap, &pDeploy->task.nodeId, sizeof(pDeploy->task.nodeId), &snode, sizeof(snode));
      if (TSDB_CODE_SUCCESS == code) {
        goto _return;
      }

      if (TSDB_CODE_DUP_KEY != code) {
        goto _return;
      }    

      taosArrayDestroy(snode.triggerList);
      continue;
    }
    
    taosWLockLatch(&pSnode->lock);
    if (NULL == pSnode->triggerList) {
      pSnode->triggerList = taosArrayInit(10, sizeof(SStmTaskToDeployExt));
      if (NULL == pSnode->triggerList) {
        taosWUnLockLatch(&pSnode->lock);
        TSDB_CHECK_NULL(pSnode->triggerList, code, lino, _return, terrno);
      }
    }
    
    ext.deploy = *pDeploy;
    ext.deployed = false;
    
    if (NULL == taosArrayPush(pSnode->triggerList, &ext)) {
      taosWUnLockLatch(&pSnode->lock);
      TSDB_CHECK_NULL(NULL, code, lino, _return, terrno);
    }
    taosWUnLockLatch(&pSnode->lock);
    
    taosHashRelease(mStreamMgmt.toDeploySnodeMap, pSnode);
    break;
  }
  
_return:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    msttDebug("trigger task added to toDeploySnodeMap, tidx:%d", pTask->taskIdx);
  }

  return code;
}

static int32_t msmTDAddRunnerToSnodeMap(SStmTaskDeploy* pDeploy, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStmSnodeTasksDeploy snode = {0};
  SStmTaskToDeployExt ext;
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
      continue;
    }
    
    taosWLockLatch(&pSnode->lock);
    if (NULL == pSnode->runnerList) {
      pSnode->runnerList = taosArrayInit(10, sizeof(SStmTaskToDeployExt));
      if (NULL == pSnode->runnerList) {
        taosWUnLockLatch(&pSnode->lock);
        TSDB_CHECK_NULL(pSnode->runnerList, code, lino, _return, terrno);
      }
    }
    
    ext.deploy = *pDeploy;
    ext.deployed = false;
    
    if (NULL == taosArrayPush(pSnode->runnerList, &ext)) {
      taosWUnLockLatch(&pSnode->lock);
      TSDB_CHECK_NULL(NULL, code, lino, _return, terrno);
    }
    taosWUnLockLatch(&pSnode->lock);
    
    taosHashRelease(mStreamMgmt.toDeploySnodeMap, pSnode);
    break;
  }
  
_return:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    msttDebug("task added to toDeploySnodeMap, tidx:%d", pTask->taskIdx);
  }

  return code;
}

static int32_t msmTDAddRunnersToSnodeMap(SArray* runnerList, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t runnerNum = taosArrayGetSize(runnerList);
  SStmTaskDeploy* pDeploy = NULL;
  int64_t streamId = pStream->pCreate->streamId;

  for (int32_t i = 0; i < runnerNum; ++i) {
    pDeploy = taosArrayGet(runnerList, i);
    
    TAOS_CHECK_EXIT(msmTDAddRunnerToSnodeMap(pDeploy, pStream));
    
    (void)atomic_add_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);    
  }

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
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
        mstWarn("snode %d not exists in snodeMap, may be dropped, ignore it", pCtx->pReq->snodeId);
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
    mstDebug("vgroup %d not exists in vgroupMap, ignore update upTs", vgId);
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

int32_t msmBuildReaderDeployInfo(SStmTaskDeploy* pDeploy, void* calcScanPlan, SStmStatus* pInfo, bool triggerReader) {
  SStreamReaderDeployMsg* pMsg = &pDeploy->msg.reader;
  pMsg->triggerReader = triggerReader;
  
  if (triggerReader) {
    SStreamReaderDeployFromTrigger* pTrigger = &pMsg->msg.trigger;
    pTrigger->triggerTblName = pInfo->pCreate->triggerTblName;
    pTrigger->triggerTblUid = pInfo->pCreate->triggerTblUid;
    pTrigger->triggerTblSuid = pInfo->pCreate->triggerTblSuid;
    pTrigger->triggerTblType = pInfo->pCreate->triggerTblType;
    pTrigger->deleteReCalc = pInfo->pCreate->deleteReCalc;
    pTrigger->deleteOutTbl = pInfo->pCreate->deleteOutTbl;
    pTrigger->partitionCols = pInfo->pCreate->partitionCols;
    pTrigger->triggerCols = pInfo->pCreate->triggerCols;
    //pTrigger->triggerPrevFilter = pStream->pCreate->triggerPrevFilter;
    pTrigger->triggerScanPlan = pInfo->pCreate->triggerScanPlan;
    pTrigger->calcCacheScanPlan = msmSearchCalcCacheScanPlan(pInfo->pCreate->calcScanPlanList);
  } else {
    SStreamReaderDeployFromCalc* pCalc = &pMsg->msg.calc;
    pCalc->execReplica = pInfo->runnerDeploys * pInfo->runnerReplica;
    pCalc->calcScanPlan = calcScanPlan;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t msmBuildTriggerRunnerTargets(SMnode* pMnode, SStmStatus* pInfo, int64_t streamId, SArray** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

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
    runner.execReplica = pInfo->runnerReplica; 
    TSDB_CHECK_NULL(taosArrayPush(*ppRes, &runner), code, lino, _exit, terrno);
    mstsDebug("the %dth runner target added to trigger's runnerList, TASK:%" PRIx64 , i, runner.addr.taskId);
  }

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return TSDB_CODE_SUCCESS;
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
  pMsg->isTriggerTblVirt = STREAM_IS_VIRTUAL_TABLE(pStream->pCreate->triggerTblType, pStream->pCreate->flags);
  pMsg->triggerHasPF = pStream->pCreate->triggerHasPF;
  pMsg->isTriggerTblStb = (pStream->pCreate->triggerTblType == TSDB_SUPER_TABLE);
  pMsg->partitionCols = pStream->pCreate->partitionCols;

  pMsg->pNotifyAddrUrls = pInfo->pCreate->pNotifyAddrUrls;
  pMsg->notifyEventTypes = pStream->pCreate->notifyEventTypes;
  pMsg->addOptions = pStream->pCreate->addOptions;
  pMsg->notifyHistory = pStream->pCreate->notifyHistory;

  pMsg->maxDelay = pStream->pCreate->maxDelay;
  pMsg->fillHistoryStartTime = pStream->pCreate->fillHistoryStartTime;
  pMsg->watermark = pStream->pCreate->watermark;
  pMsg->expiredTime = pStream->pCreate->expiredTime;
  pMsg->trigger = pInfo->pCreate->trigger;

  pMsg->eventTypes = pStream->pCreate->eventTypes;
  pMsg->placeHolderBitmap = pStream->pCreate->placeHolderBitmap;
  pMsg->calcTsSlotId = pStream->pCreate->calcTsSlotId;
  pMsg->triTsSlotId = pStream->pCreate->triTsSlotId;
  pMsg->triggerPrevFilter = pInfo->pCreate->triggerPrevFilter;
  if (STREAM_IS_VIRTUAL_TABLE(pStream->pCreate->triggerTblType, pStream->pCreate->flags)) {
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
    addr.epset = mndGetVgroupEpsetById(pMnode, pStatus->id.nodeId);
    TSDB_CHECK_NULL(taosArrayPush(pMsg->readerList, &addr), code, lino, _exit, terrno);
    mstsDebug("the %dth trigReader src added to trigger's readerList, TASK:%" PRIx64 " nodeId:%d", i, addr.taskId, addr.nodeId);
  }

  pMsg->leaderSnodeId = pStream->mainSnodeId;
  pMsg->streamName = pInfo->streamName;

  if (0 == pInfo->runnerNum) {
    mstsDebug("no runner task, skip set trigger's runner list, deployNum:%d", pInfo->runnerDeploys);
    return code;
  }

  TAOS_CHECK_EXIT(msmBuildTriggerRunnerTargets(pMnode, pInfo, streamId, &pMsg->runnerList));

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    mstsDebug("trigger deploy info built, readerNum:%d, runnerNum:%d", (int32_t)taosArrayGetSize(pMsg->readerList), (int32_t)taosArrayGetSize(pMsg->runnerList));
  }
  
  return TSDB_CODE_SUCCESS;
}


int32_t msmBuildRunnerDeployInfo(SStmTaskDeploy* pDeploy, SSubplan *plan, SStreamObj* pStream, SStmStatus* pInfo, bool topPlan) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStreamRunnerDeployMsg* pMsg = &pDeploy->msg.runner;
  //TAOS_CHECK_EXIT(qSubPlanToString(plan, &pMsg->pPlan, NULL));

  pMsg->execReplica = pInfo->runnerReplica;
  pMsg->streamName = pInfo->streamName;
  //TAOS_CHECK_EXIT(nodesCloneNode((SNode*)plan, (SNode**)&pMsg->pPlan));
  pMsg->pPlan = plan;
  pMsg->outDBFName = pInfo->pCreate->outDB;
  pMsg->outTblName = pInfo->pCreate->outTblName;
  pMsg->outTblType = pStream->pCreate->outTblType;
  pMsg->calcNotifyOnly = pStream->pCreate->calcNotifyOnly;
  pMsg->topPlan = topPlan;
  pMsg->pNotifyAddrUrls = pInfo->pCreate->pNotifyAddrUrls;
  pMsg->addOptions = pStream->pCreate->addOptions;
  if(pStream->pCreate->trigger.sliding.overlap) {
    pMsg->addOptions |= CALC_SLIDING_OVERLAP;
  }
  pMsg->outCols = pInfo->pCreate->outCols;
  pMsg->outTags = pInfo->pCreate->outTags;
  pMsg->outStbUid = pStream->pCreate->outStbUid;
  pMsg->outStbSversion = pStream->pCreate->outStbSversion;
  
  pMsg->subTblNameExpr = pInfo->pCreate->subTblNameExpr;
  pMsg->tagValueExpr = pInfo->pCreate->tagValueExpr;
  pMsg->forceOutCols = pInfo->pCreate->forceOutCols;

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
        mstsError("snode %d not exists in snodeMap", snodeId);
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


static int32_t msmBuildTriggerTasks(SStmGrpCtx* pCtx, SStmStatus* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;

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

  SStmTaskDeploy info = {0};
  info.task.type = pInfo->triggerTask->type;
  info.task.streamId = streamId;
  info.task.taskId =  pInfo->triggerTask->id.taskId;
  info.task.seriousId = pInfo->triggerTask->id.seriousId;
  info.task.nodeId =  pInfo->triggerTask->id.nodeId;
  info.task.taskIdx =  pInfo->triggerTask->id.taskIdx;
  TAOS_CHECK_EXIT(msmBuildTriggerDeployInfo(pCtx->pMnode, pInfo, &info, pStream));
  TAOS_CHECK_EXIT(msmTDAddTriggerToSnodeMap(&info, pStream));
  
  (void)atomic_add_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);

  TAOS_CHECK_EXIT(msmSTAddToTaskMap(pCtx, streamId, NULL, NULL, pInfo->triggerTask));
  TAOS_CHECK_EXIT(msmSTAddToSnodeMap(pCtx, streamId, NULL, pInfo->triggerTask, 1, -1));

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmTDAddSingleTrigReader(SStmGrpCtx* pCtx, SStmTaskStatus* pState, int32_t nodeId, SStmStatus* pInfo, int64_t streamId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  pState->id.taskId = msmAssignTaskId();
  pState->id.deployId = 0;
  pState->id.seriousId = msmAssignTaskSeriousId();
  pState->id.nodeId = nodeId;
  pState->id.taskIdx = 0;
  pState->type = STREAM_READER_TASK;
  pState->flags = STREAM_FLAG_TRIGGER_READER;
  pState->status = STREAM_STATUS_UNDEPLOYED;
  pState->lastUpTs = pCtx->currTs;
  pState->pStream = pInfo;
  
  SStmTaskDeploy info = {0};
  info.task.type = pState->type;
  info.task.streamId = streamId;
  info.task.taskId = pState->id.taskId;
  info.task.flags = pState->flags;
  info.task.seriousId = pState->id.seriousId;
  info.task.nodeId = pState->id.nodeId;
  info.task.taskIdx = pState->id.taskIdx;
  TAOS_CHECK_EXIT(msmBuildReaderDeployInfo(&info, NULL, pInfo, true));
  TAOS_CHECK_EXIT(msmTDAddToVgroupMap(mStreamMgmt.toDeployVgMap, &info, streamId));

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmTDAddTrigReaderTasks(SStmGrpCtx* pCtx, SStmStatus* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SSdb   *pSdb = pCtx->pMnode->pSdb;
  SStmTaskStatus* pState = NULL;
  SVgObj *pVgroup = NULL;
  SDbObj* pDb = NULL;
  
  switch (pStream->pCreate->triggerTblType) {
    case TSDB_NORMAL_TABLE:
    case TSDB_CHILD_TABLE:
    case TSDB_VIRTUAL_CHILD_TABLE:
    case TSDB_VIRTUAL_NORMAL_TABLE: {
      pInfo->trigReaders = taosArrayInit_s(sizeof(SStmTaskStatus), 1);
      TSDB_CHECK_NULL(pInfo->trigReaders, code, lino, _exit, terrno);
      pState = taosArrayGet(pInfo->trigReaders, 0);
      
      TAOS_CHECK_EXIT(msmTDAddSingleTrigReader(pCtx, pState, pStream->pCreate->triggerTblVgId, pInfo, streamId));
      break;
    }
    case TSDB_SUPER_TABLE: {
      pDb = mndAcquireDb(pCtx->pMnode, pStream->pCreate->triggerDB);
      if (NULL == pDb) {
        code = terrno;
        mstsError("failed to acquire db %s, error:%s", pStream->pCreate->triggerDB, terrstr());
        goto _exit;
      }

      pInfo->trigReaders = taosArrayInit(pDb->cfg.numOfVgroups, sizeof(SStmTaskStatus));
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

          code = msmTDAddSingleTrigReader(pCtx, pState, pVgroup->vgId, pInfo, streamId);
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

int32_t msmUPAddScanTask(SStmGrpCtx* pCtx, SStreamObj* pStream, char* scanPlan, int32_t vgId, int64_t taskId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SSubplan* pSubplan = NULL;
  int64_t streamId = pStream->pCreate->streamId;
  int64_t key[2] = {streamId, 0};
  SStmTaskSrcAddr addr;
  TAOS_CHECK_EXIT(nodesStringToNode(scanPlan, (SNode**)&pSubplan));
  addr.isFromCache = false;
  
  if (MNODE_HANDLE == vgId) {
    mndGetMnodeEpSet(pCtx->pMnode, &addr.epset);
  } else if (vgId > MNODE_HANDLE) {
    addr.epset = mndGetVgroupEpsetById(pCtx->pMnode, vgId);
  } else {
    mstsError("invalid vgId %d in scanPlan", vgId);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  }
  
  addr.taskId = taskId;
  addr.vgId = vgId;
  addr.groupId = pSubplan->id.groupId;

  key[1] = pSubplan->id.subplanId;

  SArray** ppRes = taosHashGet(mStreamMgmt.toUpdateScanMap, key, sizeof(key));
  if (NULL == ppRes) {
    SArray* pRes = taosArrayInit(1, sizeof(addr));
    TSDB_CHECK_NULL(pRes, code, lino, _exit, terrno);
    TSDB_CHECK_NULL(taosArrayPush(pRes, &addr), code, lino, _exit, terrno);
    TAOS_CHECK_EXIT(taosHashPut(mStreamMgmt.toUpdateScanMap, key, sizeof(key), &pRes, POINTER_BYTES));
  } else {
    TSDB_CHECK_NULL(taosArrayPush(*ppRes, &addr), code, lino, _exit, terrno);
  }

  mstsDebug("calcReader %" PRIx64 " added to toUpdateScan, vgId:%d, groupId:%d, subplanId:%d", taskId, vgId, pSubplan->id.groupId, pSubplan->id.subplanId);
  
  (void)atomic_add_fetch_32(&mStreamMgmt.toUpdateScanNum, 1);
  
_exit:

  nodesDestroyNode((SNode*)pSubplan);

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmUPAddCacheTask(SStmGrpCtx* pCtx, SStreamCalcScan* pScan, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SSubplan* pSubplan = NULL;
  int64_t streamId = pStream->pCreate->streamId;
  int64_t key[2] = {streamId, 0};
  TAOS_CHECK_EXIT(nodesStringToNode(pScan->scanPlan, (SNode**)&pSubplan));

  SStmTaskSrcAddr addr;
  addr.isFromCache = true;
  addr.epset = mndGetDnodeEpsetById(pCtx->pMnode, pCtx->triggerNodeId);
  addr.taskId = pCtx->triggerTaskId;
  addr.vgId = pCtx->triggerNodeId;
  addr.groupId = pSubplan->id.groupId;

  key[1] = pSubplan->id.subplanId;
  SArray** ppRes = taosHashGet(mStreamMgmt.toUpdateScanMap, key, sizeof(key));
  if (NULL == ppRes) {
    SArray* pRes = taosArrayInit(1, sizeof(addr));
    TSDB_CHECK_NULL(pRes, code, lino, _exit, terrno);
    TSDB_CHECK_NULL(taosArrayPush(pRes, &addr), code, lino, _exit, terrno);
    TAOS_CHECK_EXIT(taosHashPut(mStreamMgmt.toUpdateScanMap, key, sizeof(key), &pRes, POINTER_BYTES));
  } else {
    TSDB_CHECK_NULL(taosArrayPush(*ppRes, &addr), code, lino, _exit, terrno);
  }
  
  (void)atomic_add_fetch_32(&mStreamMgmt.toUpdateScanNum, 1);
  
_exit:

  nodesDestroyNode((SNode*)pSubplan);
  
  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


static int32_t msmTDAddSingleCalcReader(SStmGrpCtx* pCtx, SStmTaskStatus* pState, int32_t taskIdx, int32_t nodeId, void* calcScanPlan, SStmStatus* pInfo, int64_t streamId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TAOS_CHECK_EXIT(mstGetScanUidFromPlan(streamId, calcScanPlan, &pState->id.uid));

  pState->id.taskId = msmAssignTaskId();
  pState->id.deployId = 0;
  pState->id.seriousId = msmAssignTaskSeriousId();
  pState->id.nodeId = nodeId;
  pState->id.taskIdx = taskIdx;
  pState->type = STREAM_READER_TASK;
  pState->flags = 0;
  pState->status = STREAM_STATUS_UNDEPLOYED;
  pState->lastUpTs = pCtx->currTs;
  pState->pStream = pInfo;
  
  SStmTaskDeploy info = {0};
  info.task.type = pState->type;
  info.task.streamId = streamId;
  info.task.taskId = pState->id.taskId;
  info.task.flags = pState->flags;
  info.task.seriousId = pState->id.seriousId;
  info.task.nodeId = pState->id.nodeId;
  info.task.taskIdx = pState->id.taskIdx;
  TAOS_CHECK_EXIT(msmBuildReaderDeployInfo(&info, calcScanPlan, pInfo, false));
  TAOS_CHECK_EXIT(msmTDAddToVgroupMap(mStreamMgmt.toDeployVgMap, &info, streamId));

_exit:

  if (code) {
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
  pInfo->calcReaders = tdListNew(sizeof(SStmTaskStatus));
  TSDB_CHECK_NULL(pInfo->calcReaders, code, lino, _exit, terrno);

  
  for (int32_t i = 0; i < calcTasksNum; ++i) {
    SStreamCalcScan* pScan = taosArrayGet(pInfo->pCreate->calcScanPlanList, i);
    if (pScan->readFromCache) {
      TAOS_CHECK_EXIT(msmUPAddCacheTask(pCtx, pScan, pStream));
      continue;
    }
    
    int32_t vgNum = taosArrayGetSize(pScan->vgList);
    for (int32_t m = 0; m < vgNum; ++m) {
      pState = tdListReserve(pInfo->calcReaders);
      TSDB_CHECK_NULL(pState, code, lino, _exit, terrno);

      TAOS_CHECK_EXIT(msmTDAddSingleCalcReader(pCtx, pState, i, *(int32_t*)taosArrayGet(pScan->vgList, m), pScan->scanPlan, pInfo, streamId));
      TAOS_CHECK_EXIT(msmUPAddScanTask(pCtx, pStream, pScan->scanPlan, pState->id.nodeId, pState->id.taskId));
    }
  }

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}



static int32_t msmUPPrepareReaderTasks(SStmGrpCtx* pCtx, SStmStatus* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t calcTasksNum = taosArrayGetSize(pStream->pCreate->calcScanPlanList);
  if (calcTasksNum <= 0) {
    mstsDebug("no calc scan plan, ignore parepare reader tasks, readerNum:%d", (int32_t)MST_LIST_SIZE(pInfo->calcReaders));
    return code;    
  }
  
  SListNode* pNode = listHead(pInfo->calcReaders);
  
  for (int32_t i = 0; i < calcTasksNum; ++i) {
    SStreamCalcScan* pScan = taosArrayGet(pStream->pCreate->calcScanPlanList, i);
    if (pScan->readFromCache) {
      TAOS_CHECK_EXIT(msmUPAddCacheTask(pCtx, pScan, pStream));
      continue;
    }
    
    int32_t vgNum = taosArrayGetSize(pScan->vgList);
    for (int32_t m = 0; m < vgNum; ++m) {
      SStmTaskStatus* pReader = (SStmTaskStatus*)pNode->data;
      TAOS_CHECK_EXIT(msmUPAddScanTask(pCtx, pStream, pScan->scanPlan, pReader->id.nodeId, pReader->id.taskId));
      pNode = TD_DLIST_NODE_NEXT(pNode);
    }
  }

_exit:

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

  TAOS_CHECK_EXIT(msmSTAddToTaskMap(pCtx, streamId, pInfo->trigReaders, NULL, NULL));
  TAOS_CHECK_EXIT(msmSTAddToTaskMap(pCtx, streamId, NULL, pInfo->calcReaders, NULL));
  
  TAOS_CHECK_EXIT(msmSTAddToVgroupMap(pCtx, streamId, pInfo->trigReaders, NULL, NULL, true));
  TAOS_CHECK_EXIT(msmSTAddToVgroupMap(pCtx, streamId, NULL, pInfo->calcReaders, NULL, false));
  
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

int32_t msmUpdateLowestPlanSourceAddr(SStmGrpCtx* pCtx, SSubplan* pPlan, SStmTaskDeploy* pDeploy, int64_t streamId) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  int64_t      key[2] = {streamId, -1};
  SNode*       pNode = NULL;
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

    key[1] = MND_GET_RUNNER_SUBPLANID(pVal->datum.i);

    SArray** ppRes = taosHashGet(mStreamMgmt.toUpdateScanMap, key, sizeof(key));
    if (NULL == ppRes) {
      msttError("lowest runner subplan ID:%d,%d can't get its child ID:%" PRId64 " addr", pPlan->id.groupId,
                pPlan->id.subplanId, key[1]);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }

    int32_t childrenNum = taosArrayGetSize(*ppRes);
    for (int32_t i = 0; i < childrenNum; ++i) {
      SStmTaskSrcAddr* pAddr = taosArrayGet(*ppRes, i);
      if (pAddr->isFromCache) {
        SStmTaskSrcAddr addr = {0};
        addr.taskId = pDeploy->task.taskId;
        addr.vgId = pDeploy->task.nodeId;
        addr.groupId = pPlan->id.groupId;
        addr.epset = mndGetDnodeEpsetById(pCtx->pMnode, pDeploy->task.nodeId);

        msttInfo("fixtaskid update %" PRIx64, addr.taskId);
        TAOS_CHECK_EXIT(msmUpdatePlanSourceAddr(pTask, streamId, pPlan, pDeploy->task.taskId, &addr,
                                                TDMT_STREAM_FETCH_FROM_CACHE, key[1]));
      } else {
        TAOS_CHECK_EXIT(
            msmUpdatePlanSourceAddr(pTask, streamId, pPlan, pDeploy->task.taskId, pAddr, pAddr->isFromCache ? TDMT_STREAM_FETCH_FROM_CACHE : TDMT_STREAM_FETCH, key[1]));
      }
    }
  }

_exit:

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

  TAOS_CHECK_EXIT(msmUpdateLowestPlanSourceAddr(pCtx, pPlan, pDeploy, streamId));

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
  FOREACH(pNode, pPlan->pParents) {
    SSubplan* pSubplan = (SSubplan*)pNode;
    TAOS_CHECK_EXIT(msmGetTaskIdFromSubplanId(pStream, pRunners, beginIdx, pSubplan->id.subplanId, &parentTaskId, &parentTask));
    mstsInfo("fixtaskid set subplan exec node for parent task, parent taskid: %" PRIx64 ", child taskid: %" PRIx64,
             parentTaskId, pTask->taskId);
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
  
  for (int32_t i = 0; i < runnerNum; ++i) {
    SStmTaskDeploy* pDeploy = taosArrayGet(pRunners, i);
    TAOS_CHECK_EXIT(msmUpdateRunnerPlan(pCtx, pRunners, i, pDeploy, pStream));
    TAOS_CHECK_EXIT(nodesNodeToString((SNode*)pDeploy->msg.runner.pPlan, false, (char**)&pDeploy->msg.runner.pPlan, NULL));

    SStreamTask* pTask = &pDeploy->task;
    msttDebugL("runner updated task plan:%s", (const char*)pDeploy->msg.runner.pPlan);
  }

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmBuildRunnerTasksImpl(SStmGrpCtx* pCtx, SQueryPlan* pDag, SStmStatus* pInfo, SStreamObj* pStream) {
  int32_t code = 0;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SArray* deployTaskList = NULL;
  SArray* deployList = NULL;
  int32_t deployNodeId = 0;
  SStmTaskStatus* pState = NULL;
  int32_t taskIdx = 0;
  SNodeListNode *plans = NULL;
  int32_t        taskNum = 0;
  int32_t        totalTaskNum = 0;

  if (pDag->numOfSubplans <= 0) {
    mstsError("invalid subplan num:%d", pDag->numOfSubplans);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  }

  if (pDag->numOfSubplans != pStream->pCreate->numOfCalcSubplan) {
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
    deployNodeId = msmAssignTaskSnodeId(pCtx->pMnode, pStream, (0 == deployId) ? true : false);
    if (!GOT_SNODE(deployNodeId)) {
      TAOS_CHECK_EXIT(terrno);
    }

    taskIdx = 0;
    
    for (int32_t i = lowestLevelIdx; i >= 0; --i) {
      plans = (SNodeListNode *)nodesListGetNode(pDag->pSubplans, i);
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

      for (int32_t n = 0; n < taskNum; ++n) {
        SSubplan *plan = (SSubplan *)nodesListGetNode(plans->pNodeList, n);
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
        TAOS_CHECK_EXIT(msmBuildRunnerDeployInfo(pDeploy, plan, pStream, pInfo, 0 == i));

        SStreamTask* pTask = &pDeploy->task;
        msttDebug("runner task deploy built, subplan level:%d, taskIdx:%d, groupId:%d, subplanId:%d",
            i, pTask->taskIdx, plan->id.groupId, plan->id.subplanId);
      }

      mstsDebug("deploy %d level %d initialized, taskNum:%d", deployId, i, taskNum);
    }

    if (totalTaskNum != pDag->numOfSubplans) {
      mstsError("totalTaskNum %d mis-match with numOfSubplans %d", totalTaskNum, pDag->numOfSubplans);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }

    TAOS_CHECK_EXIT(msmUpdateRunnerPlans(pCtx, deployTaskList, pStream));

    TAOS_CHECK_EXIT(msmTDAddRunnersToSnodeMap(deployTaskList, pStream));

    nodesDestroyNode((SNode *)pDag);
    pDag = NULL;
    
    TAOS_CHECK_EXIT(nodesStringToNode(pStream->pCreate->calcPlan, (SNode**)&pDag));

    mstsDebug("total %d runner tasks added for deploy %d", totalTaskNum, deployId);
  }

  for (int32_t i = 0; i < pInfo->runnerDeploys; ++i) {
    TAOS_CHECK_EXIT(msmSTAddToTaskMap(pCtx, streamId, pInfo->runners[i], NULL, NULL));
    TAOS_CHECK_EXIT(msmSTAddToSnodeMap(pCtx, streamId, pInfo->runners[i], NULL, 0, i));
  }
  
  pInfo->runnerNum = totalTaskNum;
  
_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  taosArrayDestroy(deployTaskList);
  nodesDestroyNode((SNode *)pDag);

  return code;
}

int32_t msmReBuildRunnerTasks(SStmGrpCtx* pCtx, SQueryPlan* pDag, SStmStatus* pInfo, SStreamObj* pStream, SStmTaskAction* pAction) {
  int32_t code = 0;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t newNodeId = 0;
  int32_t levelNum = (int32_t)LIST_LENGTH(pDag->pSubplans);
  int32_t        lowestLevelIdx = levelNum - 1;
  SNodeListNode *plans = NULL;
  int32_t        taskNum = 0;
  int32_t        totalTaskNum = 0;
  int32_t        deployId = 0;
  SStmTaskStatus* pRunner = NULL;
  SStmTaskStatus* pStartRunner = NULL;
  int32_t taskIdx = 0;
  SArray* deployTaskList = taosArrayInit_s(sizeof(SStmTaskDeploy), pDag->numOfSubplans);
  TSDB_CHECK_NULL(deployTaskList, code, lino, _exit, terrno);

  for (int32_t r = 0; r < pAction->deployNum; ++r) {
    deployId = pAction->deployId[r];

    pRunner = taosArrayGet(pInfo->runners[deployId], 0);

    pStartRunner = pRunner;
    totalTaskNum = 0;

    newNodeId = msmAssignTaskSnodeId(pCtx->pMnode, pStream, (0 == r) ? true : false);
    if (!GOT_SNODE(newNodeId)) {
      TAOS_CHECK_EXIT(terrno);
    }

    taskIdx = 0;
    
    for (int32_t i = lowestLevelIdx; i >= 0; --i) {
      plans = (SNodeListNode *)nodesListGetNode(pDag->pSubplans, i);
      taskNum = (int32_t)LIST_LENGTH(plans->pNodeList);
      totalTaskNum += taskNum;

      pRunner->flags &= STREAM_FLAG_REDEPLOY_RUNNER;
      
      for (int32_t n = 0; n < taskNum; ++n) {
        SSubplan *plan = (SSubplan *)nodesListGetNode(plans->pNodeList, n);

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
        pDeploy->task.flags = pRunner->flags;
        pDeploy->task.seriousId = pRunner->id.seriousId;
        pDeploy->task.nodeId = pRunner->id.nodeId;
        pDeploy->task.taskIdx = pRunner->id.taskIdx;
        TAOS_CHECK_EXIT(msmBuildRunnerDeployInfo(pDeploy, plan, pStream, pInfo, 0 == i));

        pRunner++;
      }

      mstsDebug("level %d initialized, taskNum:%d", i, taskNum);
    }

    TAOS_CHECK_EXIT(msmUpdateRunnerPlans(pCtx, deployTaskList, pStream));

    TAOS_CHECK_EXIT(msmTDAddRunnersToSnodeMap(deployTaskList, pStream));

    TAOS_CHECK_EXIT(msmSTAddToSnodeMap(pCtx, streamId, pInfo->runners[deployId], NULL, 0, deployId));

    nodesDestroyNode((SNode *)pDag);
    pDag = NULL;

    TAOS_CHECK_EXIT(nodesStringToNode(pStream->pCreate->calcPlan, (SNode**)&pDag));
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
  //STREAMTODO 
  
  pInfo->runnerDeploys = MND_STREAM_RUNNER_DEPLOY_NUM;
  pInfo->runnerReplica = MND_STREAM_RUNNER_REPLICA_NUM;

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


static int32_t msmBuildRunnerTasks(SStmGrpCtx* pCtx, SStmStatus* pInfo, SStreamObj* pStream) {
  if (NULL == pStream->pCreate->calcPlan) {
    return TSDB_CODE_SUCCESS;
  }
  
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SQueryPlan* pPlan = NULL;

  TAOS_CHECK_EXIT(nodesStringToNode(pStream->pCreate->calcPlan, (SNode**)&pPlan));

  for (int32_t i = 0; i < pInfo->runnerDeploys; ++i) {
    pInfo->runners[i] = taosArrayInit(pPlan->numOfSubplans, sizeof(SStmTaskStatus));
    TSDB_CHECK_NULL(pInfo->runners[i], code, lino, _exit, terrno);
  }

  code = msmBuildRunnerTasksImpl(pCtx, pPlan, pInfo, pStream);
  pPlan = NULL;
  
  TAOS_CHECK_EXIT(code);

  taosHashClear(mStreamMgmt.toUpdateScanMap);
  mStreamMgmt.toUpdateScanNum = 0;

_exit:

  nodesDestroyNode((SNode *)pPlan);

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
  pCtx->triggerNodeId = msmAssignTaskSnodeId(pCtx->pMnode, pStream, true);
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


static int32_t msmInitStmStatus(SStmGrpCtx* pCtx, SStmStatus* pStatus, SStreamObj* pStream, bool initList) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;

  pStatus->lastActionTs = INT64_MIN;

  if (NULL == pStatus->streamName) {
    pStatus->streamName = taosStrdup(pStream->name);
    TSDB_CHECK_NULL(pStatus->streamName, code, lino, _exit, terrno);
  }

  TAOS_CHECK_EXIT(tCloneStreamCreateDeployPointers(pStream->pCreate, &pStatus->pCreate));
  
  if (pStream->pCreate->numOfCalcSubplan > 0) {
    pStatus->runnerNum = pStream->pCreate->numOfCalcSubplan;
    
    TAOS_CHECK_EXIT(msmSetStreamRunnerExecReplica(streamId, pStatus));
  }

  if (initList) {
    TAOS_CHECK_EXIT(msmInitTrigReaderList(pCtx, pStatus, pStream));

    int32_t subPlanNum = taosArrayGetSize(pStream->pCreate->calcScanPlanList);
    if (subPlanNum > 0) {
      pStatus->calcReaderNum = subPlanNum;
      pStatus->calcReaders = tdListNew(sizeof(SStmTaskStatus));
      TSDB_CHECK_NULL(pStatus->calcReaders, code, lino, _exit, terrno);
    }

    if (pStatus->runnerNum > 0) {
      for (int32_t i = 0; i < pStatus->runnerDeploys; ++i) {
        pStatus->runners[i] = taosArrayInit(pStatus->runnerNum, sizeof(SStmTaskStatus));
        TSDB_CHECK_NULL(pStatus->runners[i], code, lino, _exit, terrno);
      }
    }
  }
  
_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmDeployStreamTasks(SStmGrpCtx* pCtx, SStreamObj* pStream, SStmStatus* pStatus) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStmStatus info = {0};

  if (NULL == pStatus) {
    TAOS_CHECK_EXIT(msmInitStmStatus(pCtx, &info, pStream, false));

    TAOS_CHECK_EXIT(taosHashPut(mStreamMgmt.streamMap, &streamId, sizeof(streamId), &info, sizeof(info)));

    pStatus = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  }
  
  TAOS_CHECK_EXIT(msmBuildStreamTasks(pCtx, pStatus, pStream));

  mstLogSStmStatus("stream deployed", streamId, pStatus);

_exit:

  if (code) {
    if (NULL != pStatus) {
      msmStopStreamByError(streamId, pStatus, code, pCtx->currTs);
      mstsError("stream build error:%s, will try to stop current stream", tstrerror(code));
    }
    
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


static int32_t msmSTRemoveStream(int64_t streamId, bool fromStreamMap) {
  int32_t code = TSDB_CODE_SUCCESS;
  void* pIter = NULL;

  while ((pIter = taosHashIterate(mStreamMgmt.toDeployVgMap, pIter))) {
    SStmVgTasksToDeploy* pVg = (SStmVgTasksToDeploy*)pIter;
    (void)mstWaitLock(&pVg->lock, true);

    int32_t taskNum = taosArrayGetSize(pVg->taskList);
    if (atomic_load_32(&pVg->deployed) == taskNum) {
      taosRUnLockLatch(&pVg->lock);
      continue;
    }

    for (int32_t i = 0; i < taskNum; ++i) {
      SStmTaskToDeployExt* pExt = taosArrayGet(pVg->taskList, i);
      if (pExt->deployed || pExt->deploy.task.streamId != streamId) {
        continue;
      }

      mstDestroySStmTaskToDeployExt(pExt);
      pExt->deployed = true;
    }
    
    taosRUnLockLatch(&pVg->lock);
  }

  while ((pIter = taosHashIterate(mStreamMgmt.toDeploySnodeMap, pIter))) {
    SStmSnodeTasksDeploy* pSnode = (SStmSnodeTasksDeploy*)pIter;
    (void)mstWaitLock(&pSnode->lock, true);

    int32_t taskNum = taosArrayGetSize(pSnode->triggerList);
    if (atomic_load_32(&pSnode->triggerDeployed) != taskNum) {
      for (int32_t i = 0; i < taskNum; ++i) {
        SStmTaskToDeployExt* pExt = taosArrayGet(pSnode->triggerList, i);
        if (pExt->deployed || pExt->deploy.task.streamId != streamId) {
          continue;
        }
        
        mstDestroySStmTaskToDeployExt(pExt);
        pExt->deployed = true;
      }
    }

    taskNum = taosArrayGetSize(pSnode->runnerList);
    if (atomic_load_32(&pSnode->runnerDeployed) != taskNum) {
      for (int32_t i = 0; i < taskNum; ++i) {
        SStmTaskToDeployExt* pExt = taosArrayGet(pSnode->runnerList, i);
        if (pExt->deployed || pExt->deploy.task.streamId != streamId) {
          continue;
        }
        
        mstDestroySStmTaskToDeployExt(pExt);
        pExt->deployed = true;
      }
    }

    taosRUnLockLatch(&pSnode->lock);
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

static void msmResetStreamForRedeploy(int64_t streamId, SStmStatus* pStatus) {
  mstsInfo("try to reset stream for redeploy, stopped:%d, current deployTimes:%" PRId64, atomic_load_8(&pStatus->stopped), pStatus->deployTimes);
  
  (void)msmSTRemoveStream(streamId, false);  

  mstResetSStmStatus(pStatus);

  pStatus->deployTimes++;
}

static int32_t msmLaunchStreamDeployAction(SStmGrpCtx* pCtx, SStmStreamAction* pAction) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pAction->streamId;
  char* streamName = pAction->streamName;
  SStreamObj* pStream = NULL;
  int8_t stopped = 0;

  SStmStatus* pStatus = (SStmStatus*)taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (pStatus) {
    stopped = atomic_load_8(&pStatus->stopped);
    if (0 == stopped) {
      mstsDebug("stream %s will try to reset and redeploy it", pAction->streamName);
      msmResetStreamForRedeploy(streamId, pStatus);
    } else {
      if (MST_IS_USER_STOPPED(stopped) && !pAction->userAction) {
        mstsWarn("stream %s already stopped by user, stopped:%d, ignore deploy it", pAction->streamName, stopped);
        return code;
      }
      
      if (stopped == atomic_val_compare_exchange_8(&pStatus->stopped, stopped, 0)) {
        mstsDebug("stream %s will try to reset and redeploy it from stopped %d", pAction->streamName, stopped);
        msmResetStreamForRedeploy(streamId, pStatus);
      }
    }
  }

  code = mndAcquireStream(pCtx->pMnode, streamName, &pStream);
  if (TSDB_CODE_MND_STREAM_NOT_EXIST == code) {
    mstsWarn("stream %s no longer exists, ignore deploy", streamName);
    return TSDB_CODE_SUCCESS;
  }

  TAOS_CHECK_EXIT(code);

  if (pStatus && pStream->pCreate->streamId != streamId) {
    mstsWarn("stream %s already dropped by user, ignore deploy it", pAction->streamName);
    atomic_store_8(&pStatus->stopped, 2);
    mstsInfo("set stream %s stopped by user since streamId mismatch", streamName);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_NOT_EXIST);
  }

  int8_t userStopped = atomic_load_8(&pStream->userStopped);
  int8_t userDropped = atomic_load_8(&pStream->userDropped);
  if (userStopped || userDropped) {
    mstsWarn("stream %s is stopped %d or removing %d, ignore deploy", streamName, userStopped, userDropped);
    goto _exit;
  }
  
  TAOS_CHECK_EXIT(msmDeployStreamTasks(pCtx, pStream, pStatus));

_exit:

  mndReleaseStream(pCtx->pMnode, pStream);

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmReLaunchReaderTask(SStreamObj* pStream, SStmTaskAction* pAction, SStmStatus* pStatus) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pAction->streamId;
  SStmTaskStatus** ppTask = taosHashGet(mStreamMgmt.taskMap, &pAction->streamId, sizeof(pAction->streamId) + sizeof(pAction->id.taskId));
  if (NULL == ppTask) {
    mstsError("TASK:%" PRId64 " not in taskMap, remain:%d", pAction->id.taskId, taosHashGetSize(mStreamMgmt.taskMap));
    TAOS_CHECK_EXIT(TSDB_CODE_STREAM_INTERNAL_ERROR);
  }
  
  SStmTaskDeploy info = {0};
  info.task.type = pAction->type;
  info.task.streamId = pAction->streamId;
  info.task.taskId = pAction->id.taskId;
  info.task.seriousId = (*ppTask)->id.seriousId;
  info.task.nodeId = pAction->id.nodeId;
  info.task.taskIdx = pAction->id.taskIdx;
  
  bool isTriggerReader = STREAM_IS_TRIGGER_READER(pAction->flag);
  SStreamCalcScan* scanPlan = NULL;
  if (!isTriggerReader) {
    scanPlan = taosArrayGet(pStatus->pCreate->calcScanPlanList, pAction->id.taskIdx);
    if (NULL == scanPlan) {
      mstsError("fail to get TASK:%" PRId64 " scanPlan, taskIdx:%d, scanPlanNum:%zu", 
          pAction->id.taskId, pAction->id.taskIdx, taosArrayGetSize(pStatus->pCreate->calcScanPlanList));
      TAOS_CHECK_EXIT(TSDB_CODE_STREAM_INTERNAL_ERROR);
    }
  }
  
  TAOS_CHECK_EXIT(msmBuildReaderDeployInfo(&info, scanPlan ? scanPlan->scanPlan : NULL, pStatus, isTriggerReader));
  TAOS_CHECK_EXIT(msmTDAddToVgroupMap(mStreamMgmt.toDeployVgMap, &info, pAction->streamId));

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

static int32_t msmReLaunchRunnerDeploy(SStmGrpCtx* pCtx, SStreamObj* pStream, SStmTaskAction* pAction, SStmStatus* pStatus) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pAction->streamId;
  
/*
  if (pAction->triggerStatus) {
    pCtx->triggerTaskId = pAction->triggerStatus->id.taskId;
    pAction->triggerStatus->id.nodeId = msmAssignTaskSnodeId(pCtx->pMnode, pStream, true);
    if (!GOT_SNODE(pAction->triggerStatus->id.nodeId)) {
      mstsError("no avaible snode for deploying trigger task, seriousId:%" PRId64, pAction->triggerStatus->id.seriousId);
      return TSDB_CODE_SUCCESS;
    }
  
    pCtx->triggerNodeId = pAction->triggerStatus->id.nodeId;
  } else {
*/
  pCtx->triggerTaskId = pStatus->triggerTask->id.taskId;
  pCtx->triggerNodeId = pStatus->triggerTask->id.nodeId;
//  }
  
  TAOS_CHECK_EXIT(msmUPPrepareReaderTasks(pCtx, pStatus, pStream));
  
  SQueryPlan* pPlan = NULL;
  TAOS_CHECK_EXIT(nodesStringToNode(pStream->pCreate->calcPlan, (SNode**)&pPlan));
  
  TAOS_CHECK_EXIT(msmReBuildRunnerTasks(pCtx, pPlan, pStatus, pStream, pAction));
  
  taosHashClear(mStreamMgmt.toUpdateScanMap);
  mStreamMgmt.toUpdateScanNum = 0;
  
/*
  if (pAction->triggerStatus) {
    SStmTaskDeploy info = {0};
    info.task.type = STREAM_TRIGGER_TASK;
    info.task.streamId = streamId;
    info.task.taskId = pCtx->triggerTaskId;
    info.task.seriousId = pAction->triggerStatus->id.seriousId;
    info.task.nodeId = pCtx->triggerNodeId;
    info.task.taskIdx = 0;
  
    TAOS_CHECK_EXIT(msmBuildTriggerDeployInfo(pCtx->pMnode, pStatus, &info, pStream));
    TAOS_CHECK_EXIT(msmTDAddTriggerToSnodeMap(&info, pStream));
    TAOS_CHECK_EXIT(msmSTAddToSnodeMap(pCtx, streamId, NULL, pAction->triggerStatus, 1, -1));
    
    atomic_add_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);
  }
*/

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


static int32_t msmLaunchTaskDeployAction(SStmGrpCtx* pCtx, SStmTaskAction* pAction) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pAction->streamId;
  int64_t taskId = pAction->id.taskId;
  SStreamObj* pStream = NULL;

  mstsDebug("start to handle stream tasks action, action task type:%s", gStreamTaskTypeStr[pAction->type]);

  SStmStatus* pStatus = taosHashGet(mStreamMgmt.streamMap, &pAction->streamId, sizeof(pAction->streamId));
  if (NULL == pStatus) {
    mstsWarn("stream not in streamMap, remain:%d", taosHashGetSize(mStreamMgmt.streamMap));
    return TSDB_CODE_SUCCESS;
  }

  int8_t stopped = atomic_load_8(&pStatus->stopped);
  if (stopped) {
    mstsWarn("stream %s is already stopped %d, ignore task deploy", pStatus->streamName, stopped);
    return TSDB_CODE_SUCCESS;
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
      TAOS_CHECK_EXIT(msmReLaunchReaderTask(pStream, pAction, pStatus));
      break;
/*
    case STREAM_TRIGGER_TASK:
      TAOS_CHECK_EXIT(msmReLaunchTriggerTask(pCtx, pStream, pAction, pStatus));
      break;
*/
    case STREAM_RUNNER_TASK:
      if (pAction->multiRunner) {
        TAOS_CHECK_EXIT(msmReLaunchRunnerDeploy(pCtx, pStream, pAction, pStatus));
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
          }
        }
      }

      taskNum = taosArrayGetSize(pSnode->runnerList);
      if (atomic_load_32(&pSnode->runnerDeployed) != taskNum) {
        for (int32_t i = 0; i < taskNum; ++i) {
          SStmTaskToDeployExt* pExt = taosArrayGet(pSnode->runnerList, i);
          if (pExt->deploy.task.streamId == streamId && !pExt->deployed) {
            pExt->deployed = true;
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

void msmUndeployStream(SMnode* pMnode, int64_t streamId, char* streamName) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  int8_t active = atomic_load_8(&mStreamMgmt.active), state = atomic_load_8(&mStreamMgmt.state);
  if (0 == active || MND_STM_STATE_NORMAL != state) {
    mstsError("stream mgmt not available since active:%d state:%d", active, state);
    return;
  }

  SStmStatus* pStream = (SStmStatus*)taosHashAcquire(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStream) {
    mstsInfo("stream %s already not in streamMap", streamName);
    goto _exit;
  }

  atomic_store_8(&pStream->stopped, 2);

  mstsInfo("set stream %s stopped by user", streamName);

_exit:

  taosHashRelease(mStreamMgmt.streamMap, pStream);

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return;
}

int32_t msmRecalcStream(SMnode* pMnode, int64_t streamId, STimeWindow* timeRange) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

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

  TAOS_CHECK_EXIT(mstAppendNewRecalcRange(streamId, pStream, timeRange));

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

  while (mndStreamActionDequeue(mStreamMgmt.actionQ, &pQNode)) {
    switch (pQNode->type) {
      case STREAM_ACT_DEPLOY:
        if (pQNode->streamAct) {
          mstDebug("start to handle stream deploy action");
          TAOS_CHECK_EXIT(msmLaunchStreamDeployAction(pCtx, &pQNode->action.stream));
        } else {
          mstDebug("start to handle task deploy action");
          TAOS_CHECK_EXIT(msmLaunchTaskDeployAction(pCtx, &pQNode->action.task));
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

void msmStopAllStreamsByGrant(int32_t errCode) {
  SStmStatus* pStatus = NULL;
  void* pIter = NULL;
  int64_t streamId = 0;
  
  while (true) {
    pIter = taosHashIterate(mStreamMgmt.streamMap, pIter);
    if (NULL == pIter) {
      break;
    }

    pStatus = (SStmStatus*)pIter;

    streamId = *(int64_t*)taosHashGetKey(pIter, NULL);
    atomic_store_8(&pStatus->stopped, 4);

    mstsInfo("set stream stopped since %s", tstrerror(errCode));
  }
}

int32_t msmHandleGrantExpired(SMnode *pMnode, int32_t errCode) {
  mstInfo("stream grant expired");

  if (0 == atomic_load_8(&mStreamMgmt.active)) {
    mstWarn("mnode stream is NOT active, ignore handling");
    return errCode;
  }

  (void)mstWaitLock(&mStreamMgmt.runtimeLock, true);

  msmStopAllStreamsByGrant(errCode);

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
  SStreamTask* pTask = &pDeploy->task;
  SStmStreamDeploy streamDeploy = {0};
  SStmStreamDeploy* pStream = NULL;
   
  while (true) {
    pStream = taosHashAcquire(pHash, &streamId, sizeof(streamId));
    if (NULL == pStream) {
      TAOS_CHECK_EXIT(msmInitStreamDeploy(&streamDeploy, pDeploy));
      code = taosHashPut(pHash, &streamId, sizeof(streamId), &streamDeploy, sizeof(streamDeploy));
      if (TSDB_CODE_SUCCESS == code) {
        goto _exit;
      }

      if (TSDB_CODE_DUP_KEY != code) {
        goto _exit;
      }    

      tFreeSStmStreamDeploy(&streamDeploy);
      continue;
    }

    TAOS_CHECK_EXIT(msmInitStreamDeploy(pStream, pDeploy));
    
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


int32_t msmGrpAddDeployTasks(SHashObj* pHash, SArray* pTasks, int32_t* deployed) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t taskNum = taosArrayGetSize(pTasks);

  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskToDeployExt* pExt = taosArrayGet(pTasks, i);
    if (pExt->deployed) {
      continue;
    }

    TAOS_CHECK_EXIT(msmGrpAddDeployTask(pHash, &pExt->deploy));
    pExt->deployed = true;

    (void)atomic_add_fetch_32(deployed, 1);
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmGrpAddDeployVgTasks(SStmGrpCtx* pCtx) {
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

    if (taosRTryLockLatch(&pVg->lock)) {
      continue;
    }
    
    if (atomic_load_32(&pVg->deployed) == taosArrayGetSize(pVg->taskList)) {
      taosRUnLockLatch(&pVg->lock);
      continue;
    }
    
    TAOS_CHECK_EXIT(msmGrpAddDeployTasks(pCtx->deployStm, pVg->taskList, &pVg->deployed));
    taosRUnLockLatch(&pVg->lock);
  }

_exit:

  if (code) {
    if (pVg) {
      taosRUnLockLatch(&pVg->lock);
    }

    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmGrpAddDeploySnodeTasks(SStmGrpCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmSnodeTasksDeploy* pSnode = NULL;
  SStreamHbMsg* pReq = pCtx->pReq;

  mstDebug("start to add stream snode tasks deploy");
  
  pSnode = taosHashAcquire(mStreamMgmt.toDeploySnodeMap, &pReq->snodeId, sizeof(pReq->snodeId));
  if (NULL == pSnode) {
    return TSDB_CODE_SUCCESS;
  }

  (void)mstWaitLock(&pSnode->lock, false);
  
  if (atomic_load_32(&pSnode->triggerDeployed) < taosArrayGetSize(pSnode->triggerList)) {
    TAOS_CHECK_EXIT(msmGrpAddDeployTasks(pCtx->deployStm, pSnode->triggerList, &pSnode->triggerDeployed));
  }

  if (atomic_load_32(&pSnode->runnerDeployed) < taosArrayGetSize(pSnode->runnerList)) {
    TAOS_CHECK_EXIT(msmGrpAddDeployTasks(pCtx->deployStm, pSnode->runnerList, &pSnode->runnerDeployed));
  }
  
  taosWUnLockLatch(&pSnode->lock);

_exit:

  if (code) {
    if (pSnode) {
      taosWUnLockLatch(&pSnode->lock);
    }

    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmUpdateStreamLastActTs(int64_t streamId, int64_t currTs) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmStatus* pStatus = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStatus) {
    mstsWarn("stream already not exists in streamMap, mapSize:%d", taosHashGetSize(mStreamMgmt.streamMap));
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

void msmCleanDeployedSnodeTasks (int32_t snodeId) {
  if (!GOT_SNODE(snodeId)) {
    return;
  }
  
  int32_t code = TSDB_CODE_SUCCESS;
  SStmSnodeTasksDeploy* pSnode = taosHashAcquire(mStreamMgmt.toDeploySnodeMap, &snodeId, sizeof(snodeId));
  if (NULL == pSnode) {
    return;
  }

  if (taosWTryLockLatch(&pSnode->lock)) {
    taosHashRelease(mStreamMgmt.toDeploySnodeMap, pSnode);
    return;
  }

  int32_t triggerNum = taosArrayGetSize(pSnode->triggerList);
  int32_t runnerNum = taosArrayGetSize(pSnode->runnerList);
  
  if (atomic_load_32(&pSnode->triggerDeployed) <= 0 && atomic_load_32(&pSnode->runnerDeployed) <= 0) {
    taosWUnLockLatch(&pSnode->lock);
    taosHashRelease(mStreamMgmt.toDeploySnodeMap, pSnode);
    return;
  }

  if (atomic_load_32(&pSnode->triggerDeployed) == triggerNum) {
    (void)atomic_sub_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, triggerNum);
    taosArrayDestroyEx(pSnode->triggerList, mstDestroySStmTaskToDeployExt);
    pSnode->triggerList = NULL;
  }

  if (atomic_load_32(&pSnode->runnerDeployed) == runnerNum) {
    (void)atomic_sub_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, runnerNum);
    taosArrayDestroyEx(pSnode->runnerList, mstDestroySStmTaskToDeployExt);
    pSnode->runnerList = NULL;
  }

  if (NULL == pSnode->triggerList && NULL == pSnode->runnerList) {
    TAOS_UNUSED(taosHashRemove(mStreamMgmt.toDeploySnodeMap, &snodeId, sizeof(snodeId)));
    taosWUnLockLatch(&pSnode->lock);
    taosHashRelease(mStreamMgmt.toDeploySnodeMap, pSnode);
    return;
  }

  if (atomic_load_32(&pSnode->triggerDeployed) > 0 && pSnode->triggerList) {
    for (int32_t m = triggerNum - 1; m >= 0; --m) {
      SStmTaskToDeployExt* pExt = taosArrayGet(pSnode->triggerList, m);
      if (!pExt->deployed) {
        continue;
      }

      mstDestroySStmTaskToDeployExt(pExt);
      (void)atomic_sub_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);
      taosArrayRemove(pSnode->triggerList, m);
    }
    
    pSnode->triggerDeployed = 0;
  }

  if (atomic_load_32(&pSnode->runnerDeployed) > 0 && pSnode->runnerList) {
    for (int32_t m = runnerNum - 1; m >= 0; --m) {
      SStmTaskToDeployExt* pExt = taosArrayGet(pSnode->runnerList, m);
      if (!pExt->deployed) {
        continue;
      }

      mstDestroySStmTaskToDeployExt(pExt);
      (void)atomic_sub_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);
      taosArrayRemove(pSnode->runnerList, m);
    }
    
    pSnode->runnerDeployed = 0;
  }
  
  taosWUnLockLatch(&pSnode->lock);
  taosHashRelease(mStreamMgmt.toDeploySnodeMap, pSnode);
}

void msmClearStreamToDeployMaps(SStreamHbMsg* pHb) {
  if (atomic_load_32(&mStreamMgmt.toDeployVgTaskNum) > 0) {
    msmCleanDeployedVgTasks(pHb->pVgLeaders);
  }

  if (atomic_load_32(&mStreamMgmt.toDeploySnodeTaskNum) > 0) {
    msmCleanDeployedSnodeTasks(pHb->snodeId);
  }
}

void msmCleanStreamGrpCtx(SStreamHbMsg* pHb) {
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
    pAction->actions |= action;
    pAction->recalc.recalcList = recalcList;

    mstsDebug("stream append recalc action, listSize:%d, actions:%x", (int32_t)taosArrayGetSize(recalcList), pAction->actions);
  } else {
    newAction.actions = action;
    newAction.recalc.recalcList = recalcList;
    
    TAOS_CHECK_EXIT(taosHashPut(pCtx->actionStm, &streamId, sizeof(streamId), &newAction, sizeof(newAction)));
    
    mstsDebug("stream add recalc action, listSize:%d", (int32_t)taosArrayGetSize(recalcList));
  }

_exit:

  if (code) {
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

  readerNum = taosArrayGetSize(pStream->trigOReaders);
  for (int32_t i = 0; i < readerNum; ++i) {
    SStmTaskStatus* pStatus = taosArrayGet(pStream->trigOReaders, i);
    if (STREAM_STATUS_INIT != pStatus->status && STREAM_STATUS_RUNNING != pStatus->status) {
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
      //STREAMTODO ADD ERRCODE HANDLE
      msttInfo("task failed with error:%s, try to undeploy current task, idx:%d", tstrerror(pMsg->errorCode), pMsg->taskIdx);
      TAOS_CHECK_EXIT(msmGrpAddActionUndeploy(pCtx, streamId, pTask));
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

void msmHandleStatusUpdateErr(SStmGrpCtx* pCtx, EStmErrType err, SStmTaskStatusMsg* pStatus) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStreamTask* pTask = (SStreamTask*)pStatus;
  int64_t streamId = pStatus->streamId;

  msttInfo("start to handle task status update exception, type: %d", err);
  
  // STREAMTODO

  if (STM_ERR_TASK_NOT_EXISTS == err || STM_ERR_STREAM_STOPPED == err) {
    TAOS_CHECK_EXIT(msmGrpAddActionUndeploy(pCtx, streamId, pTask));
  }

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

  if (1 == atomic_val_compare_exchange_8(&pStream->triggerNeedUpdate, 1, 0)) {
    TAOS_CHECK_EXIT(msmGrpAddActionUpdateTrigger(pCtx->actionStm, pTask->streamId));
  }
  
  SArray* userRecalcList = NULL;
  if (atomic_load_ptr(&pStream->userRecalcList)) {
    taosWLockLatch(&pStream->userRecalcLock);
    if (pStream->userRecalcList) {
      userRecalcList = pStream->userRecalcList;
      pStream->userRecalcList = NULL;
    }
    taosWUnLockLatch(&pStream->userRecalcLock);
    
    if (userRecalcList) {
      TAOS_CHECK_EXIT(msmGrpAddActionRecalc(pCtx, pTask->streamId, userRecalcList));
    }
  }

  if (pTask->detailStatus >= 0 && pCtx->pReq->pTriggerStatus) {
    (void)mstWaitLock(&pStatus->detailStatusLock, false);
    if (NULL == pStatus->detailStatus) {
      pStatus->detailStatus = taosMemoryCalloc(1, sizeof(SSTriggerRuntimeStatus));
      if (NULL == pStatus->detailStatus) {
        taosWUnLockLatch(&pStatus->detailStatusLock);
        TSDB_CHECK_NULL(pStatus->detailStatus, code, lino, _exit, terrno);
      }
    }
    
    memcpy(pStatus->detailStatus, taosArrayGet(pCtx->pReq->pTriggerStatus, pTask->detailStatus), sizeof(SSTriggerRuntimeStatus));
    taosWUnLockLatch(&pStatus->detailStatusLock);
  }

_exit:

  if (code) {
    // IGNORE STOP STREAM BY ERROR
    msttError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
}

int32_t msmNormalHandleStatusUpdate(SStmGrpCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t num = taosArrayGetSize(pCtx->pReq->pStreamStatus);

  mstDebug("NORMAL: start to handle stream group %d tasks status, taskNum:%d", pCtx->pReq->streamGId, num);

  for (int32_t i = 0; i < num; ++i) {
    SStmTaskStatusMsg* pTask = taosArrayGet(pCtx->pReq->pStreamStatus, i);
    msttDebug("task status %s got on dnode %d, taskIdx:%d", gStreamStatusStr[pTask->status], pCtx->pReq->dnodeId, pTask->taskIdx);
    
    SStmTaskStatus** ppStatus = taosHashGet(mStreamMgmt.taskMap, &pTask->streamId, sizeof(pTask->streamId) + sizeof(pTask->taskId));
    if (NULL == ppStatus) {
      msttWarn("task no longer exists in taskMap, will try to undeploy current task, taskIdx:%d", pTask->taskIdx);
      msmHandleStatusUpdateErr(pCtx, STM_ERR_TASK_NOT_EXISTS, pTask);
      continue;
    }

    SStmStatus* pStream = (SStmStatus*)(*ppStatus)->pStream;
    int8_t stopped = atomic_load_8(&pStream->stopped);
    if (stopped) {
      msttWarn("stream already stopped %d, will try to undeploy current task, taskIdx:%d", stopped, pTask->taskIdx);
      msmHandleStatusUpdateErr(pCtx, STM_ERR_STREAM_STOPPED, pTask);
      continue;
    }

    if ((pTask->seriousId != (*ppStatus)->id.seriousId) || (pTask->nodeId != (*ppStatus)->id.nodeId)) {
      msttInfo("task mismatch with it in taskMap, will try to rm it, current seriousId:%" PRId64 ", nodeId:%d", 
          (*ppStatus)->id.seriousId, (*ppStatus)->id.nodeId);
          
      msmHandleStatusUpdateErr(pCtx, STM_ERR_TASK_NOT_EXISTS, pTask);
      continue;
    }

    if ((*ppStatus)->status != pTask->status) {
      if (STREAM_STATUS_RUNNING == pTask->status) {
        (*ppStatus)->runningStartTs = pCtx->currTs;
      } else if (MST_IS_RUNNER_GETTING_READY(pTask) && STREAM_IS_REDEPLOY_RUNNER((*ppStatus)->flags)) {
        if (pStream->triggerTask) {
          atomic_store_8(&pStream->triggerNeedUpdate, 1);
        }
        
        STREAM_CLR_FLAG((*ppStatus)->flags, STREAM_FLAG_REDEPLOY_RUNNER);
      }
    }
    
    (*ppStatus)->errCode = pTask->errorCode;
    (*ppStatus)->status = pTask->status;
    (*ppStatus)->lastUpTs = pCtx->currTs;
    
    if (STREAM_STATUS_RUNNING != pTask->status) {
      msmHandleTaskAbnormalStatus(pCtx, pTask, *ppStatus);
    }
    
    if (STREAM_TRIGGER_TASK == pTask->type) {
      msmChkHandleTriggerOperations(pCtx, pTask, *ppStatus);
    }
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmWatchRecordNewTask(SStmGrpCtx* pCtx, SStmTaskStatusMsg* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pTask->streamId;
  SStreamObj* pStream = NULL;

  SStmStatus* pStatus = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStatus) {
    SStmStatus status = {0};
    TAOS_CHECK_EXIT(mndAcquireStreamById(pCtx->pMnode, streamId, &pStream));
    TSDB_CHECK_NULL(pStream, code, lino, _exit, TSDB_CODE_MND_STREAM_NOT_EXIST);
    if (STREAM_IS_VIRTUAL_TABLE(pStream->pCreate->triggerTblType, pStream->pCreate->flags) || pStream->pCreate->vtableCalc) {
      mndReleaseStream(pCtx->pMnode, pStream);
      msttDebug("virtual table task ignored, triggerTblType:%d, vtableCalc:%dstatus:%s", 
          pStream->pCreate->triggerTblType, pStream->pCreate->vtableCalc, gStreamStatusStr[pTask->status]);
      return code;
    }

    TAOS_CHECK_EXIT(msmInitStmStatus(pCtx, &status, pStream, true));
    mndReleaseStream(pCtx->pMnode, pStream);

    TAOS_CHECK_EXIT(taosHashPut(mStreamMgmt.streamMap, &streamId, sizeof(streamId), &status, sizeof(status)));
    pStatus = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
    TSDB_CHECK_NULL(pStatus, code, lino, _exit, terrno);
    msttDebug("stream added to streamMap cause of new task status:%s", gStreamStatusStr[pTask->status]);
  }

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
          MST_LIST_SIZE((SList*)pList) >= readerSize){
        mstsError("%sReader list is already full, size:%d, expSize:%d", STREAM_IS_TRIGGER_READER(pTask->flags) ? "trig" : "calc",
            (int32_t)taosArrayGetSize(pList), readerSize);
        TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      }
      
      SStmTaskStatus taskStatus = {0};
      taskStatus.pStream = pStatus;
      mstSetTaskStatusFromMsg(pCtx, &taskStatus, pTask);
      if (STREAM_IS_TRIGGER_READER(pTask->flags)) {
        pNewTask = taosArrayPush(pList, &taskStatus);
        TSDB_CHECK_NULL(pNewTask, code, lino, _exit, terrno);
      } else {
        TAOS_CHECK_EXIT(tdListAppend(pStatus->calcReaders, &taskStatus));
      }
      
      TAOS_CHECK_EXIT(msmSTAddToTaskMap(pCtx, streamId, NULL, NULL, pNewTask));
      TAOS_CHECK_EXIT(msmSTAddToVgroupMapImpl(streamId, pNewTask, STREAM_IS_TRIGGER_READER(pTask->flags)));
      break;
    }
    case STREAM_TRIGGER_TASK: {
      taosMemoryFreeClear(pStatus->triggerTask);
      pStatus->triggerTask = taosMemoryCalloc(1, sizeof(*pStatus->triggerTask));
      TSDB_CHECK_NULL(pStatus->triggerTask, code, lino, _exit, terrno);
      pStatus->triggerTask->pStream = pStatus;
      mstSetTaskStatusFromMsg(pCtx, pStatus->triggerTask, pTask);
      pNewTask = pStatus->triggerTask;

      TAOS_CHECK_EXIT(msmSTAddToTaskMap(pCtx, streamId, NULL, NULL, pNewTask));
      TAOS_CHECK_EXIT(msmSTAddToSnodeMapImpl(streamId, pNewTask, 0));
      break;
    }
    case STREAM_RUNNER_TASK:{
      if (NULL == pStatus->runners[pTask->deployId]) {
        mstsError("deploy %d runner list is NULL", pTask->deployId);
        TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      }
      if (taosArrayGetSize(pStatus->runners[pTask->deployId]) >= pStatus->runnerNum) {
        mstsError("deploy %d runner list is already full, size:%d, expSize:%d", pTask->deployId, 
            (int32_t)taosArrayGetSize(pStatus->runners[pTask->deployId]), pStatus->runnerNum);
        TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      }    
      
      SStmTaskStatus taskStatus = {0};
      taskStatus.pStream = pStatus;
      mstSetTaskStatusFromMsg(pCtx, &taskStatus, pTask);
      pNewTask = taosArrayPush(pStatus->runners[pTask->deployId], &taskStatus);
      TSDB_CHECK_NULL(pNewTask, code, lino, _exit, terrno);

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
    msttDebug("new task recored to taskMap/streamMap, task status:%s", gStreamStatusStr[pTask->status]);
  }

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

    if (pTask->taskId >= mStreamMgmt.lastTaskId) {
      mStreamMgmt.lastTaskId = pTask->taskId + 1;
    }
    
    SStmTaskStatus** ppStatus = taosHashGet(mStreamMgmt.taskMap, &pTask->streamId, sizeof(pTask->streamId) + sizeof(pTask->taskId));
    if (NULL == ppStatus) {
      msttInfo("task still not in taskMap, will try to add it, taskIdx:%d", pTask->taskIdx);
      
      TAOS_CHECK_EXIT(msmWatchRecordNewTask(pCtx, pTask));
      
      continue;
    }
    
    (*ppStatus)->status = pTask->status;
    (*ppStatus)->lastUpTs = pCtx->currTs;
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
    mstsDebug("stream already not exists in streamMap, ignore trigger update, streamRemain:%d", taosHashGetSize(mStreamMgmt.streamMap));
    return;
  }

  if (NULL == pStream->triggerTask) {
    mstsWarn("no triggerTask exists, ignore trigger update, stopped:%d", atomic_load_8(&pStream->stopped));
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
    mstsDebug("stream already not exists in streamMap, ignore trigger update, streamRemain:%d", taosHashGetSize(mStreamMgmt.streamMap));
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
      mstsWarn("trigReaderNum %d mis-match with expected %d", trigReaderNum, pStatus->trigReaderNum);
      msmStopStreamByError(streamId, pStatus, TSDB_CODE_MND_STREAM_TASK_LOST, pCtx->currTs);
      continue;
    }

    calcReaderNum = MST_LIST_SIZE(pStatus->calcReaders);
    if (pStatus->calcReaderNum != calcReaderNum) {
      mstsWarn("calcReaderNum %d mis-match with expected %d", calcReaderNum, pStatus->calcReaderNum);
      msmStopStreamByError(streamId, pStatus, TSDB_CODE_MND_STREAM_TASK_LOST, pCtx->currTs);
      continue;
    }

    for (int32_t i = 0; i < pStatus->runnerDeploys; ++i) {
      runnerNum = taosArrayGetSize(pStatus->runners[i]);
      if (runnerNum != pStatus->runnerNum) {
        mstsWarn("runner deploy %d runnerNum %d mis-match with expected %d", i, runnerNum, pStatus->runnerNum);
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

  if (0 != atomic_val_compare_exchange_8(&mStreamMgmt.watch.ending, 0, 1)) {
    return code;
  }

  while (atomic_load_32(&mStreamMgmt.watch.processing) > minVal) {
    (void)sched_yield();
  }

  if (watchError) {
    taosHashClear(mStreamMgmt.vgroupMap);
    taosHashClear(mStreamMgmt.snodeMap);
    taosHashClear(mStreamMgmt.taskMap);
    taosHashClear(mStreamMgmt.streamMap);
    mstInfo("watch error happends, clear all maps");
    goto _exit;
  }

  if (0 == atomic_load_8(&mStreamMgmt.watch.taskRemains)) {
    mstInfo("no stream tasks remain during watch state");
    goto _exit;
  }

  msmWatchCheckStreamMap(pCtx);

_exit:

  mStreamMgmt.lastTaskId += 100000;

  mstInfo("watch state end, new taskId begin from:%" PRIx64, mStreamMgmt.lastTaskId);

  msmSetInitRuntimeState(MND_STM_STATE_NORMAL);

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

int32_t msmCheckDeployTrigReader(SStmGrpCtx* pCtx, SStmStatus* pStatus, SStmTaskStatusMsg* pTask, int32_t vgId, int32_t vgNum) {
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

  if (!readerExists) {
    if (NULL == pStatus->trigOReaders) {
      pStatus->trigOReaders = taosArrayInit(vgNum, sizeof(SStmTaskStatus));
      TSDB_CHECK_NULL(pStatus->trigOReaders, code, lino, _exit, terrno);
    }
    
    SStmTaskStatus* pState = taosArrayReserve(pStatus->trigOReaders, 1);
    TAOS_CHECK_EXIT(msmTDAddSingleTrigReader(pCtx, pState, vgId, pStatus, streamId));
    TAOS_CHECK_EXIT(msmSTAddToTaskMap(pCtx, streamId, NULL, NULL, pState));
    TAOS_CHECK_EXIT(msmSTAddToVgroupMap(pCtx, streamId, NULL, NULL, pState, true));
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmDeployTriggerOrigReader(SStmGrpCtx* pCtx, SStmTaskStatusMsg* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t vgId = 0;
  int64_t streamId = pTask->streamId;
  SArray* pTbs = pTask->pMgmtReq->cont.pReqs;
  int32_t tbNum = taosArrayGetSize(pTbs);
  SStreamDbTableName* pName = NULL;
  SSHashObj* pDbVgroups = NULL;
  SStreamMgmtRsp rsp = {0};
  rsp.reqId = pTask->pMgmtReq->reqId;
  rsp.header.msgType = STREAM_MSG_ORIGTBL_READER_INFO;
  int32_t iter = 0;
  void* p = NULL;
  SSHashObj* pVgs = NULL;
  SStreamMgmtReq* pMgmtReq = NULL;
  int8_t stopped = 0;
  
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

  if (tbNum <= 0) {
    mstsWarn("empty table list in origReader req, array:%p", pTbs);
    goto _exit;
  }

  int32_t oReaderNum = taosArrayGetSize(pStatus->trigOReaders);
  if (oReaderNum > 0) {
    mstsWarn("origReaders already exits, num:%d", oReaderNum);
    goto _exit;
  }

  TAOS_CHECK_EXIT(mstBuildDBVgroupsMap(pCtx->pMnode, &pDbVgroups));
  rsp.cont.vgIds = taosArrayInit(tbNum, sizeof(int32_t));
  TSDB_CHECK_NULL(rsp.cont.vgIds, code, lino, _exit, terrno);

  pVgs = tSimpleHashInit(tbNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  TSDB_CHECK_NULL(pVgs, code, lino, _exit, terrno);
  
  for (int32_t i = 0; i < tbNum; ++i) {
    pName = (SStreamDbTableName*)taosArrayGet(pTbs, i);
    TAOS_CHECK_EXIT(mstGetTableVgId(pDbVgroups, pName->dbFName, pName->tbName, &vgId));
    TSDB_CHECK_NULL(taosArrayPush(rsp.cont.vgIds, &vgId), code, lino, _exit, terrno);
    TAOS_CHECK_EXIT(tSimpleHashPut(pVgs, &vgId, sizeof(vgId), &vgId, sizeof(vgId)));
  }

  int32_t vgNum = tSimpleHashGetSize(pVgs);
  while (true) {
    p = tSimpleHashIterate(pVgs, p, &iter);
    if (NULL == p) {
      break;
    }
    
    TAOS_CHECK_EXIT(msmCheckDeployTrigReader(pCtx, pStatus, pTask, *(int32_t*)p, vgNum));
  }
  
  vgNum = taosArrayGetSize(pStatus->trigOReaders);
  rsp.cont.readerList = taosArrayInit(vgNum, sizeof(SStreamTaskAddr));
  TSDB_CHECK_NULL(rsp.cont.readerList, code, lino, _exit, terrno);

  SStreamTaskAddr addr;
  for (int32_t i = 0; i < vgNum; ++i) {
    SStmTaskStatus* pOTask = taosArrayGet(pStatus->trigOReaders, i);
    addr.taskId = pOTask->id.taskId;
    addr.nodeId = pOTask->id.nodeId;
    addr.epset = mndGetVgroupEpsetById(pCtx->pMnode, pOTask->id.nodeId);
    TSDB_CHECK_NULL(taosArrayPush(rsp.cont.readerList, &addr), code, lino, _exit, terrno);
    mstsDebug("the %dth otrigReader src added to trigger's virtual orig readerList, TASK:%" PRIx64 " nodeId:%d", i, addr.taskId, addr.nodeId);
  }

  if (NULL == pCtx->pRsp->rsps.rspList) {
    pCtx->pRsp->rsps.rspList = taosArrayInit(2, sizeof(SStreamMgmtRsp));
    TSDB_CHECK_NULL(pCtx->pRsp->rsps.rspList, code, lino, _exit, terrno);
  }

  TSDB_CHECK_NULL(taosArrayPush(pCtx->pRsp->rsps.rspList, &rsp), code, lino, _exit, terrno);

_exit:

  tFreeSStreamMgmtReq(pMgmtReq);
  taosMemoryFree(pMgmtReq);

  tSimpleHashCleanup(pVgs);

  if (code) {
    tFreeSStreamMgmtRsp(&rsp);
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  mstDestroyDbVgroupsHash(pDbVgroups);

  return code;
}

int32_t msmGetCalcScanFromList(int64_t streamId, SArray* pList, int64_t uid, SStreamCalcScan** ppRes) {
  int32_t num = taosArrayGetSize(pList);
  SStreamCalcScan* pScan = NULL;
  int32_t code = TSDB_CODE_SUCCESS, lino = 0;
  int64_t planUid = 0;
  for (int32_t i = 0; i < num; ++i) {
    pScan = (SStreamCalcScan*)taosArrayGet(pList, i);
    TAOS_CHECK_EXIT(mstGetScanUidFromPlan(streamId, pScan->scanPlan, &planUid));
    if (0 != planUid && planUid == uid) {
      *ppRes = pScan;
      break;
    }
  }

_exit:

  if (code) {
    mstsError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmCheckDeployCalcReader(SStmGrpCtx* pCtx, SStmStatus* pStatus, SStmTaskStatusMsg* pTask, int32_t vgId, int64_t uid, SStreamTaskAddr* pAddr) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  bool    readerExists = false;
  int64_t streamId = pTask->streamId;
  SListNode* pNode = listHead(pStatus->calcReaders);
  SStmTaskStatus* pReader = NULL;
  int32_t taskIdx = 0;

  int32_t readerNum = MST_LIST_SIZE(pStatus->calcReaders);
  for (int32_t i = 0; i < readerNum; ++i, pNode = TD_DLIST_NODE_NEXT(pNode)) {
    pReader = (SStmTaskStatus*)pNode->data;
    if (pReader->id.nodeId == vgId && pReader->id.uid == uid) {
      readerExists = true;
      pAddr->taskId = pReader->id.taskId;
      break;
    }
  }

  if (!readerExists) {
    if (NULL == pStatus->calcReaders) {
      pStatus->calcReaders = tdListNew(sizeof(SStmTaskStatus));
      TSDB_CHECK_NULL(pStatus->calcReaders, code, lino, _exit, terrno);
      taskIdx = 0;
    } else {
      pNode = listTail(pStatus->calcReaders);
      pReader = (SStmTaskStatus*)pNode->data;
      taskIdx = pReader->id.taskIdx + 1;
    }

    SStreamCalcScan* pScan = NULL;
    TAOS_CHECK_EXIT(msmGetCalcScanFromList(streamId, pStatus->pCreate->calcScanPlanList, uid, &pScan));
    TSDB_CHECK_NULL(pScan, code, lino, _exit, TSDB_CODE_STREAM_INTERNAL_ERROR);
    pReader = tdListReserve(pStatus->calcReaders);
    TAOS_CHECK_EXIT(msmTDAddSingleCalcReader(pCtx, pReader, taskIdx, vgId, pScan->scanPlan, pStatus, streamId));
    TAOS_CHECK_EXIT(msmSTAddToTaskMap(pCtx, streamId, NULL, NULL, pReader));
    TAOS_CHECK_EXIT(msmSTAddToVgroupMap(pCtx, streamId, NULL, NULL, pReader, false));
    pAddr->taskId = pReader->id.taskId;
  }

  pAddr->epset = mndGetVgroupEpsetById(pCtx->pMnode, vgId);
  pAddr->nodeId = vgId;

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


int32_t msmDeployRunnerOrigReader(SStmGrpCtx* pCtx, SStmTaskStatusMsg* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
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

  if (reqNum <= 0) {
    mstsWarn("empty req list in origReader req, array:%p", pReqs);
    goto _exit;
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
    
    for (int32_t n = 0; n < vgNum; ++n) {
      vgId = *(int32_t*)taosArrayGet(pReq->vgIds, n);
      pAddr = taosArrayGet(pRsp->vgList, n);
      TAOS_CHECK_EXIT(msmCheckDeployCalcReader(pCtx, pStatus, pTask, vgId, pReq->uid, pAddr));
    }
  }

  if (NULL == pCtx->pRsp->rsps.rspList) {
    pCtx->pRsp->rsps.rspList = taosArrayInit(2, sizeof(SStreamMgmtRsp));
    TSDB_CHECK_NULL(pCtx->pRsp->rsps.rspList, code, lino, _exit, terrno);
  }

  TSDB_CHECK_NULL(taosArrayPush(pCtx->pRsp->rsps.rspList, &rsp), code, lino, _exit, terrno);

_exit:

  tFreeSStreamMgmtReq(pMgmtReq);
  taosMemoryFree(pMgmtReq);

  if (code) {
    tFreeSStreamMgmtRsp(&rsp);
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


int32_t msmHandleTaskMgmtReq(SStmGrpCtx* pCtx, SStmTaskStatusMsg* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  switch (pTask->pMgmtReq->type) {
    case STREAM_MGMT_REQ_TRIGGER_ORIGTBL_READER:
      TAOS_CHECK_EXIT(msmDeployTriggerOrigReader(pCtx, pTask));
      break;
    case STREAM_MGMT_REQ_RUNNER_ORIGTBL_READER:
      TAOS_CHECK_EXIT(msmDeployRunnerOrigReader(pCtx, pTask));
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

    TAOS_CHECK_EXIT(msmHandleTaskMgmtReq(pCtx, pTask));
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmNormalHandleHbMsg(SStmGrpCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStreamHbMsg* pReq = pCtx->pReq;

  TAOS_CHECK_EXIT(msmCheckUpdateDnodeTs(pCtx));
  if (GOT_SNODE(pReq->snodeId)) {
    TAOS_CHECK_EXIT(msmUpdateSnodeUpTs(pCtx));
  }
  
  if (atomic_load_64(&mStreamMgmt.actionQ->qRemainNum) > 0 && 0 == taosWTryLockLatch(&mStreamMgmt.actionQLock)) {
    msmHandleStreamActions(pCtx);
    taosWUnLockLatch(&mStreamMgmt.actionQLock);
  }

  if (taosArrayGetSize(pReq->pStreamReq) > 0 && mstWaitLock(&mStreamMgmt.actionQLock, false)) {
    code = msmHandleStreamRequests(pCtx);
    taosWUnLockLatch(&mStreamMgmt.actionQLock);
    TAOS_CHECK_EXIT(code);
  }

  if (atomic_load_32(&mStreamMgmt.toDeployVgTaskNum) > 0) {
    TAOS_CHECK_EXIT(msmGrpAddDeployVgTasks(pCtx));
  } else {
    TAOS_CHECK_EXIT(msmUpdateVgroupsUpTs(pCtx));
  }

  if (atomic_load_32(&mStreamMgmt.toDeploySnodeTaskNum) > 0 && GOT_SNODE(pReq->snodeId)) {
    TAOS_CHECK_EXIT(msmGrpAddDeploySnodeTasks(pCtx));
  }

  if (taosHashGetSize(pCtx->deployStm) > 0) {
    TAOS_CHECK_EXIT(msmRspAddStreamsDeploy(pCtx));
  }

  if (taosArrayGetSize(pReq->pStreamStatus) > 0) {
    TAOS_CHECK_EXIT(msmNormalHandleStatusUpdate(pCtx));
  }

  if (taosHashGetSize(pCtx->actionStm) > 0) {
    TAOS_CHECK_EXIT(msmHandleHbPostActions(pCtx));
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
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
  int32_t code = TSDB_CODE_SUCCESS;
  SMStreamHbRspMsg rsp = {0};
  rsp.streamGId = pHb->streamGId;

  (void)mstWaitLock(&mStreamMgmt.runtimeLock, true);

  if (0 == atomic_load_8(&mStreamMgmt.active)) {
    mstWarn("mnode stream become NOT active, ignore stream hb from dnode %d streamGid %d", pHb->dnodeId, pHb->streamGId);
    goto _exit;
  }

  int32_t tidx = streamGetThreadIdx(mStreamMgmt.threadNum, pHb->streamGId);
  SStmGrpCtx* pCtx = &mStreamMgmt.tCtx[tidx].grpCtx[pHb->streamGId];

  pCtx->tidx = tidx;
  pCtx->pMnode = pMnode;
  pCtx->currTs = currTs;
  pCtx->pReq = pHb;
  pCtx->pRsp = &rsp;
  pCtx->deployStm = mStreamMgmt.tCtx[pCtx->tidx].deployStm[pHb->streamGId];
  pCtx->actionStm = mStreamMgmt.tCtx[pCtx->tidx].actionStm[pHb->streamGId];
  
  switch (atomic_load_8(&mStreamMgmt.state)) {
    case MND_STM_STATE_WATCH:
      code = msmWatchHandleHbMsg(pCtx);
      break;
    case MND_STM_STATE_NORMAL:
      code = msmNormalHandleHbMsg(pCtx);
      break;
    default:
      mstError("Invalid stream state: %d", mStreamMgmt.state);
      code = TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
      break;
  }

_exit:

  msmEncodeStreamHbRsp(code, &pReq->info, &rsp, pRspMsg);

  msmCleanStreamGrpCtx(pHb);
  msmClearStreamToDeployMaps(pHb);

  taosRUnLockLatch(&mStreamMgmt.runtimeLock);
  
  tFreeSMStreamHbRspMsg(&rsp);

  return code;
}

void msmHandleBecomeLeader(SMnode *pMnode) {
  if (tsDisableStream) {
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
  if (tsDisableStream) {
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

  if (pStream->mainSnodeId == pSnode->id) {
    if (NULL == *ppRes) {
      int32_t streamNum = sdbGetSize(pMnode->pSdb, SDB_STREAM);
      *ppRes = taosArrayInit(streamNum, POINTER_BYTES);
      TSDB_CHECK_NULL(*ppRes, code, lino, _exit, terrno);
    }

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

  mstPostStreamAction(mStreamMgmt.actionQ, streamId, pStream->pCreate->name, NULL, false, STREAM_ACT_DEPLOY);

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
    stopped = atomic_load_8(&pStatus->stopped);
    if (MST_IS_USER_STOPPED(stopped)) {
      mstsDebug("stream already stopped by user, deployTimes:%" PRId64, pStatus->deployTimes);
      (void)msmRemoveStreamFromMaps(pMnode, streamId);
      continue;
    }

    if (!sdbCheckExists(pMnode->pSdb, SDB_STREAM, pStatus->streamName)) {
      mstsDebug("stream already not exists, deployTimes:%" PRId64, pStatus->deployTimes);
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

      mstPostStreamAction(mStreamMgmt.actionQ, *(int64_t*)taosHashGetKey(pIter, NULL), pStatus->streamName, NULL, false, STREAM_ACT_DEPLOY);
      continue;
    }

    if (MST_IS_GRANT_STOPPED(stopped) && TSDB_CODE_SUCCESS == grantCheckExpire(TSDB_GRANT_STREAMS)) {
      mstPostStreamAction(mStreamMgmt.actionQ, *(int64_t*)taosHashGetKey(pIter, NULL), pStatus->streamName, NULL, false, STREAM_ACT_DEPLOY);
      continue;
    }
  }
}

void msmCheckStreamsStatus(SMnode *pMnode) {
  SStmCheckStatusCtx ctx = {0};

  mstDebug("start to check streams status, currTs:%" PRId64, mStreamMgmt.hCtx.currentTs);
  
  if (MST_READY_FOR_SDB_LOOP()) {
    mstDebug("ready to check sdb loop, lastLoopSdbTs:%" PRId64, mStreamMgmt.lastTs[STM_EVENT_LOOP_SDB].ts);
    sdbTraverse(pMnode->pSdb, SDB_STREAM, msmCheckLoopStreamSdb, &ctx, NULL, NULL);
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
    
    if (!MST_PASS_ISOLATION(pTask->lastUpTs, 1)) {
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

    int64_t newSid = atomic_add_fetch_64(&pTask->id.seriousId, 1);
    mstsDebug("task %" PRIx64 " SID updated to %" PRIx64, pTask->id.taskId, newSid);

    SStmTaskAction task = {0};
    task.streamId = streamId;
    task.id = pTask->id;
    task.flag = pTask->flags;
    task.type = pTask->type;
    
    mstPostTaskAction(mStreamMgmt.actionQ, &task, STREAM_ACT_DEPLOY);
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

        int64_t newSid = atomic_add_fetch_64(&pTask->id.seriousId, 1);
        mstsDebug("task %" PRIx64 " SID updated to %" PRIx64, pTask->id.taskId, newSid);
      }
      
      deployId[*deployNum] = i;
      (*deployNum)++;
    }
  }
}

void msmHandleSnodeLost(SMnode *pMnode, SStmSnodeStatus* pSnode) {
  pSnode->runnerThreadNum = -1;

  (void)msmSTAddSnodesToMap(pMnode);

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
      
      if (task.deployNum > 0) {
        //task.triggerStatus = pStream->trigger;
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
      
      msmHandleSnodeLost(pMnode, pSnode);
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
    
    msmHandleSnodeLost(pMnode, pSnode);
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
  
  (void)mstWaitLock(&mStreamMgmt.runtimeLock, false);
  if (!msmCheckNeedHealthCheck(pMnode)) {
    taosWUnLockLatch(&mStreamMgmt.runtimeLock);
    return;
  }
  
  mStreamMgmt.hCtx.slotIdx = (mStreamMgmt.hCtx.slotIdx + 1) % MND_STREAM_ISOLATION_PERIOD_NUM;
  mStreamMgmt.hCtx.currentTs = taosGetTimestampMs();

  mstDebug("start health check, soltIdx:%d, checkStartTs:%" PRId64, mStreamMgmt.hCtx.slotIdx, mStreamMgmt.hCtx.currentTs);
  
  msmCheckStreamsStatus(pMnode);
  msmCheckTasksStatus(pMnode);
  msmCheckSnodesState(pMnode);

  taosWUnLockLatch(&mStreamMgmt.runtimeLock);

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
    mstsError("stream not exists in streamMap, streamRemains:%d", taosHashGetSize(mStreamMgmt.streamMap));
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
  
  for (int32_t i = 0; i < mStreamMgmt.threadNum; ++i) {
    SStmThreadCtx* pCtx = mStreamMgmt.tCtx + i;

    for (int32_t m = 0; m < STREAM_MAX_GROUP_NUM; ++m) {
      pCtx->deployStm[m] = taosHashInit(snodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
      if (pCtx->deployStm[m] == NULL) {
        code = terrno;
        mError("failed to initialize the stream runtime deployStm[%d][%d], error:%s", i, m, tstrerror(code));
        goto _exit;
      }
      taosHashSetFreeFp(pCtx->deployStm[m], tDeepFreeSStmStreamDeploy);
      
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

  mStreamMgmt.lastTaskId = 1;

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



