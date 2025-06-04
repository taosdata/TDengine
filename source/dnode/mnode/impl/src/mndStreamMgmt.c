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

void msmDestroyRuntimeInfo(SMnode *pMnode) {
  // TODO

  memset(&mStreamMgmt, 0, sizeof(mStreamMgmt));
}


static void msmSetInitRuntimeState(int8_t state) {
  switch (state) {
    case MND_STM_STATE_WATCH:
      mStreamMgmt.watch.ending = 0;
      mStreamMgmt.watch.taskRemains = 0;
      mStreamMgmt.watch.processing = 0;
      break;
    case MND_STM_STATE_NORMAL:
      MND_STREAM_SET_LAST_TS(STM_OP_NORMAL_BEGIN, taosGetTimestampMs());
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

    code = taosHashPut(mStreamMgmt.snodeMap, &pSnode->id, sizeof(pSnode->id), &tasks, sizeof(tasks));
    if (code && TSDB_CODE_DUP_KEY != code) {
      TSDB_CHECK_CODE(code, lino, _return);
    }

    code = TSDB_CODE_SUCCESS;
  
    sdbRelease(pMnode->pSdb, pSnode);
  }

  pSnode = NULL;

_return:

  sdbRelease(pMnode->pSdb, pSnode);

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
      TSDB_CHECK_CODE(code, lino, _return);
    }

    code = TSDB_CODE_SUCCESS;
    sdbRelease(pMnode->pSdb, pDnode);
  }

  pDnode = NULL;

_return:

  sdbRelease(pMnode->pSdb, pDnode);

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}



static int32_t msmSTAddToTaskMap(SStmGrpCtx* pCtx, int64_t streamId, SArray* pTasks, SStmTaskStatus* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t taskNum = pTask ? 1 : taosArrayGetSize(pTasks);
  int64_t key[2] = {streamId, 0};
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskStatus* pStatus = pTask ? pTask : taosArrayGet(pTasks, i);
    key[1] = pStatus->id.taskId;
    TAOS_CHECK_EXIT(taosHashPut(mStreamMgmt.taskMap, key, sizeof(key), &pStatus, POINTER_BYTES));
    mstsDebug("task %"PRId64" tidx %d added to taskMap", pStatus->id.taskId, pStatus->id.taskIdx);
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
  SStmSnodeStatus snode = {0};

  SStmSnodeStatus* pSnode = taosHashGet(mStreamMgmt.snodeMap, &pStatus->id.nodeId, sizeof(pStatus->id.nodeId));
  if (NULL == pSnode) {
    mstsWarn("snode %d not exists in snodeMap anymore, may be dropped", pStatus->id.nodeId);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  } else {
    if (NULL == pSnode->streamTasks) {
      pSnode->streamTasks = taosHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
      TSDB_CHECK_NULL(pSnode->streamTasks, code, lino, _exit, terrno);
      taosHashSetFreeFp(pSnode->streamTasks, mstDestroySStmSnodeStreamStatus);
    }
    
    TAOS_CHECK_EXIT(msmSTAddToSnodeStreamHash(pSnode->streamTasks, streamId, pStatus, deployId));
  }
  
_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    mstsDebug("%s task %" PRId64 " tidx %d added to snodeMap, snodeId:%d", (deployId < 0) ? "trigger" : "runner", 
        pStatus->id.taskId, pStatus->id.taskIdx, pStatus->id.nodeId);
  }

  return code;
}



static int32_t msmTDAddSnodeTask(SHashObj* pHash, SStmTaskDeploy* pDeploy, SStreamObj* pStream, bool triggerTask, bool lowestRunner) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStmSnodeTasksDeploy snode = {0};
  SStmTaskToDeployExt ext;
  SStreamTask* pTask = &pDeploy->task;
  SArray** ppList = triggerTask ? &snode.triggerList : &snode.runnerList;

  ext.lowestRunner = lowestRunner;

  while (true) {
    SStmSnodeTasksDeploy* pSnode = taosHashAcquire(pHash, &pDeploy->task.nodeId, sizeof(pDeploy->task.nodeId));
    if (NULL == pSnode) {
      *ppList = taosArrayInit(10, sizeof(SStmTaskToDeployExt));
      TSDB_CHECK_NULL(*ppList, code, lino, _return, terrno);

      ext.deploy = *pDeploy;
      ext.deployed = false;
      TSDB_CHECK_NULL(taosArrayPush(*ppList, &ext), code, lino, _return, terrno);

      code = taosHashPut(pHash, &pDeploy->task.nodeId, sizeof(pDeploy->task.nodeId), &snode, sizeof(snode));
      if (TSDB_CODE_SUCCESS == code) {
        goto _return;
      }

      if (TSDB_CODE_DUP_KEY != code) {
        goto _return;
      }    

      taosArrayDestroy(*ppList);
      continue;
    }

    ppList = triggerTask ? &pSnode->triggerList : &pSnode->runnerList;
    
    taosWLockLatch(&pSnode->lock);
    if (NULL == *ppList) {
      *ppList = taosArrayInit(10, sizeof(SStmTaskToDeployExt));
      if (NULL == *ppList) {
        taosWUnLockLatch(&pSnode->lock);
        TSDB_CHECK_NULL(*ppList, code, lino, _return, terrno);
      }
    }
    
    ext.deploy = *pDeploy;
    ext.deployed = false;
    
    if (NULL == taosArrayPush(*ppList, &ext)) {
      taosWUnLockLatch(&pSnode->lock);
      TSDB_CHECK_NULL(NULL, code, lino, _return, terrno);
    }
    taosWUnLockLatch(&pSnode->lock);
    
    taosHashRelease(pHash, pSnode);
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

int32_t msmBuildReaderDeployInfo(SStmTaskDeploy* pDeploy, SStreamObj* pStream, void* calcScanPlan, SStmStatus* pInfo, bool triggerReader) {
  SStreamReaderDeployMsg* pMsg = &pDeploy->msg.reader;
  pMsg->triggerReader = triggerReader;
  
  if (triggerReader) {
    if (NULL == pStream) {
      return TSDB_CODE_SUCCESS;
    }
    
    SStreamReaderDeployFromTrigger* pTrigger = &pMsg->msg.trigger;
    pTrigger->triggerTblName = pStream->pCreate->triggerTblName;
    pTrigger->triggerTblUid = pStream->pCreate->triggerTblUid;
    pTrigger->triggerTblType = pStream->pCreate->triggerTblType;
    pTrigger->deleteReCalc = pStream->pCreate->deleteReCalc;
    pTrigger->deleteOutTbl = pStream->pCreate->deleteOutTbl;
    pTrigger->partitionCols = pStream->pCreate->partitionCols;
    pTrigger->triggerCols = pStream->pCreate->triggerCols;
    //pTrigger->triggerPrevFilter = pStream->pCreate->triggerPrevFilter;
    pTrigger->triggerScanPlan = pStream->pCreate->triggerScanPlan;
    pTrigger->calcCacheScanPlan = msmSearchCalcCacheScanPlan(pStream->pCreate->calcScanPlanList);
  } else {
    SStreamReaderDeployFromCalc* pCalc = &pMsg->msg.calc;
    pCalc->execReplica = pInfo->runnerDeploys * pInfo->runnerReplica;
    pCalc->calcScanPlan = calcScanPlan;
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

  pMsg->pNotifyAddrUrls = pStream->pCreate->pNotifyAddrUrls;
  pMsg->notifyEventTypes = pStream->pCreate->notifyEventTypes;
  pMsg->notifyErrorHandle = pStream->pCreate->notifyErrorHandle;
  pMsg->notifyHistory = pStream->pCreate->notifyHistory;

  pMsg->maxDelay = pStream->pCreate->maxDelay;
  pMsg->fillHistoryStartTime = pStream->pCreate->fillHistoryStartTime;
  pMsg->watermark = pStream->pCreate->watermark;
  pMsg->expiredTime = pStream->pCreate->expiredTime;
  pMsg->trigger = pStream->pCreate->trigger;

  pMsg->eventTypes = pStream->pCreate->eventTypes;
  pMsg->placeHolderBitmap = pStream->pCreate->placeHolderBitmap;
  pMsg->tsSlotId = pStream->pCreate->tsSlotId;
  pMsg->partitionCols = pStream->pCreate->partitionCols;
  if (STREAM_IS_VIRTUAL_TABLE(pStream->pCreate->triggerTblType, pStream->pCreate->flags)) {
    pMsg->calcPlan = pStream->pCreate->calcPlan;
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

  if (0 == pInfo->runnerNum) {
    mstsDebug("no runner task, skip set trigger's runner list, deployNum:%d", pInfo->runnerDeploys);
    return code;
  }

  if (pInfo->runnerDeploys > 0) {
    pMsg->runnerList = taosArrayInit(pInfo->runnerDeploys, sizeof(SStreamTaskAddr));
    TSDB_CHECK_NULL(pMsg->runnerList, code, lino, _exit, terrno);
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
    runner.addr.epset = mndGetDnodeEpsetById(pMnode, pStatus->id.nodeId);
    runner.execReplica = pInfo->runnerReplica; 
    TSDB_CHECK_NULL(taosArrayPush(pMsg->runnerList, &runner), code, lino, _exit, terrno);
    mstsDebug("the %dth runner target added to trigger's runnerList, TASK:%" PRIx64 , i, runner.addr.taskId);
  }

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return TSDB_CODE_SUCCESS;
}


int32_t msmBuildRunnerDeployInfo(SStmTaskDeploy* pDeploy, SSubplan *plan, SStreamObj* pStream, int32_t replica, bool topPlan) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStreamRunnerDeployMsg* pMsg = &pDeploy->msg.runner;
  //TAOS_CHECK_EXIT(qSubPlanToString(plan, &pMsg->pPlan, NULL));

  pMsg->execReplica = replica;
  //TAOS_CHECK_EXIT(nodesCloneNode((SNode*)plan, (SNode**)&pMsg->pPlan));
  pMsg->pPlan = plan;
  pMsg->outDBFName = pStream->pCreate->outDB;
  pMsg->outTblName = pStream->pCreate->outTblName;
  pMsg->outTblType = pStream->pCreate->outTblType;
  pMsg->calcNotifyOnly = pStream->pCreate->calcNotifyOnly;
  pMsg->topPlan = topPlan;
  pMsg->pNotifyAddrUrls = pStream->pCreate->pNotifyAddrUrls;
  pMsg->notifyErrorHandle = pStream->pCreate->notifyErrorHandle;
  pMsg->outCols = pStream->pCreate->outCols;
  pMsg->outTags = pStream->pCreate->outTags;
  pMsg->outStbUid = pStream->pCreate->outStbUid;
  pMsg->outStbSversion = pStream->pCreate->outStbSversion;
  
  pMsg->subTblNameExpr = pStream->pCreate->subTblNameExpr;
  pMsg->tagValueExpr = pStream->pCreate->tagValueExpr;
  pMsg->forceOutCols = pStream->pCreate->forceOutCols;

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}


static int32_t msmSTAddToVgroupMap(SStmGrpCtx* pCtx, int64_t streamId, SArray* pTasks, SStmTaskStatus* pTask, bool trigReader) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t taskNum = pTask ? 1 : taosArrayGetSize(pTasks);
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskStatus* pStatus = pTask ? pTask : taosArrayGet(pTasks, i);
    TAOS_CHECK_EXIT(msmSTAddToVgroupMapImpl(streamId, pStatus, trigReader));
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
    TSDB_CHECK_CODE(msmSTAddToSnodeMapImpl(streamId, pStatus, deployId), lino, _return);
  }
  
_return:

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
  int32_t snodeNum = sdbGetSize(pMnode->pSdb, SDB_SNODE);
  if (snodeNum <= 0) {
    mstsInfo("no available snode now, num:%d", snodeNum);
    return 0;
  }

  int32_t snodeTarget = taosRand() % snodeNum;
  int32_t snodeIdx = 0;
  int32_t snodeId = 0;
  void      *pIter = NULL;
  SSnodeObj *pObj = NULL;
  bool alive = false;

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

    TAOS_CHECK_EXIT(msmIsSnodeAlive(pMnode, pObj->id, streamId, &alive));

    if (!alive) {
      continue;
    }

    snodeId = pObj->id;
    if (snodeIdx == snodeTarget) {
      sdbRelease(pMnode->pSdb, pObj);
      sdbCancelFetch(pMnode->pSdb, pIter);
      return snodeId;
    }

    snodeIdx++;
  }

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
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
  TSDB_CHECK_NULL(pInfo->triggerTask, code, lino, _return, terrno);

  pInfo->triggerTask->id.taskId = pCtx->triggerTaskId;
  pInfo->triggerTask->id.deployId = -1;
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
  TSDB_CHECK_CODE(msmBuildTriggerDeployInfo(pCtx->pMnode, pInfo, &info, pStream), lino, _return);
  TSDB_CHECK_CODE(msmTDAddSnodeTask(mStreamMgmt.toDeploySnodeMap, &info, pStream, true, false), lino, _return);
  
  atomic_add_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);

  TSDB_CHECK_CODE(msmSTAddToTaskMap(pCtx, streamId, NULL, pInfo->triggerTask), lino, _return);
  TSDB_CHECK_CODE(msmSTAddToSnodeMap(pCtx, streamId, NULL, pInfo->triggerTask, 1, -1), lino, _return);

_return:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmTDAddSingleTrigReader(SStmGrpCtx* pCtx, SStmTaskStatus* pState, int32_t nodeId, SStmStatus* pInfo, SStreamObj* pStream, int64_t streamId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  pState->id.taskId = msmAssignTaskId();
  pState->id.deployId = -1;
  pState->id.seriousId = msmAssignTaskSeriousId();
  pState->id.nodeId = nodeId;
  pState->id.taskIdx = 0;
  pState->type = STREAM_READER_TASK;
  pState->flags = STREAM_FLAG_TRIGGER_READER;
  pState->status = STREAM_STATUS_NA;
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
  TSDB_CHECK_CODE(msmBuildReaderDeployInfo(&info, pStream, NULL, pInfo, true), lino, _exit);
  TSDB_CHECK_CODE(msmTDAddToVgroupMap(mStreamMgmt.toDeployVgMap, &info, streamId), lino, _exit);

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
  
  switch (pStream->pCreate->triggerTblType) {
    case TSDB_NORMAL_TABLE:
    case TSDB_CHILD_TABLE:
    case TSDB_VIRTUAL_CHILD_TABLE:
    case TSDB_VIRTUAL_NORMAL_TABLE: {
      pInfo->trigReaders = taosArrayInit_s(sizeof(SStmTaskStatus), 1);
      TSDB_CHECK_NULL(pInfo->trigReaders, code, lino, _exit, terrno);
      pState = taosArrayGet(pInfo->trigReaders, 0);
      
      TAOS_CHECK_EXIT(msmTDAddSingleTrigReader(pCtx, pState, pStream->pCreate->triggerTblVgId, pInfo, pStream, streamId));
      break;
    }
    case TSDB_SUPER_TABLE: {
      SDbObj* pDb = mndAcquireDb(pCtx->pMnode, pStream->pCreate->triggerDB);
      if (NULL == pDb) {
        code = terrno;
        mstsError("failed to acquire db %s, error:%s", pStream->pCreate->triggerDB, terrstr());
        goto _exit;
      }

      pInfo->trigReaders = taosArrayInit(pDb->cfg.numOfVgroups, sizeof(SStmTaskStatus));
      TSDB_CHECK_NULL(pInfo->trigReaders, code, lino, _exit, terrno);
      
      void *pIter = NULL;
      while (1) {
        SVgObj *pVgroup = NULL;
        SStmTaskDeploy info = {0};
        pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
        if (pIter == NULL) {
          break;
        }
      
        if (pVgroup->dbUid == pDb->uid && !pVgroup->isTsma) {
          pState = taosArrayReserve(pInfo->trigReaders, 1);

          TAOS_CHECK_EXIT(msmTDAddSingleTrigReader(pCtx, pState, pVgroup->vgId, pInfo, pStream, streamId));
          sdbRelease(pSdb, pVgroup);
        }
      }
      break;
    }
    default:
      mstsDebug("%s ignore triggerTblType %d", __FUNCTION__, pStream->pCreate->triggerTblType);
      break;
  }

_exit:

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
  addr.epset = mndGetVgroupEpsetById(pCtx->pMnode, vgId);
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
  
  atomic_add_fetch_32(&mStreamMgmt.toUpdateScanNum, 1);
  
_exit:

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
  
  atomic_add_fetch_32(&mStreamMgmt.toUpdateScanNum, 1);
  
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
  pInfo->calcReaders = taosArrayInit(calcTasksNum, sizeof(SStmTaskStatus));
  TSDB_CHECK_NULL(pInfo->calcReaders, code, lino, _exit, terrno);
  
  for (int32_t i = 0; i < calcTasksNum; ++i) {
    SStreamCalcScan* pScan = taosArrayGet(pStream->pCreate->calcScanPlanList, i);
    if (pScan->readFromCache) {
      TSDB_CHECK_CODE(msmUPAddCacheTask(pCtx, pScan, pStream), lino, _exit);
      continue;
    }
    
    int32_t vgNum = taosArrayGetSize(pScan->vgList);
    for (int32_t m = 0; m < vgNum; ++m) {
      pState = taosArrayReserve(pInfo->calcReaders, 1);

      pState->id.taskId = msmAssignTaskId();
      pState->id.deployId = -1;
      pState->id.seriousId = msmAssignTaskSeriousId();
      pState->id.nodeId = *(int32_t*)taosArrayGet(pScan->vgList, m);
      pState->id.taskIdx = i;
      pState->type = STREAM_READER_TASK;
      pState->flags = STREAM_FLAG_TRIGGER_READER;
      pState->status = STREAM_STATUS_NA;
      pState->lastUpTs = pCtx->currTs;
      pState->pStream = pInfo;

      SStmTaskDeploy info = {0};
      info.task.type = pState->type;
      info.task.streamId = streamId;
      info.task.taskId = pState->id.taskId;
      info.task.seriousId = pState->id.seriousId;
      info.task.nodeId = pState->id.nodeId;
      info.task.taskIdx = pState->id.taskIdx;
      TSDB_CHECK_CODE(msmBuildReaderDeployInfo(&info, pStream, pScan->scanPlan, pInfo, false), lino, _exit);
      TSDB_CHECK_CODE(msmUPAddScanTask(pCtx, pStream, pScan->scanPlan, pState->id.nodeId, pState->id.taskId), lino, _exit);
      TSDB_CHECK_CODE(msmTDAddToVgroupMap(mStreamMgmt.toDeployVgMap, &info, streamId), lino, _exit);
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
    mstsDebug("no calc scan plan, ignore parepare reader tasks, readerNum:%d", (int32_t)taosArrayGetSize(pInfo->calcReaders));
    return code;    
  }
  
  SStmTaskStatus* pReader = taosArrayGet(pInfo->calcReaders, 0);
  
  for (int32_t i = 0; i < calcTasksNum; ++i) {
    SStreamCalcScan* pScan = taosArrayGet(pStream->pCreate->calcScanPlanList, i);
    if (pScan->readFromCache) {
      TSDB_CHECK_CODE(msmUPAddCacheTask(pCtx, pScan, pStream), lino, _return);
      continue;
    }
    
    int32_t vgNum = taosArrayGetSize(pScan->vgList);
    for (int32_t m = 0; m < vgNum; ++m) {
      TSDB_CHECK_CODE(msmUPAddScanTask(pCtx, pStream, pScan->scanPlan, pReader->id.nodeId, pReader->id.taskId), lino, _return);
      pReader++;
    }
  }

_return:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}



static void msmCalcReadersNum(SMnode* pMnode, SStreamObj* pStream, int32_t* taskNum) {
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t streamId = pStream->pCreate->streamId;
  
  switch (pStream->pCreate->triggerTblType) {
    case TSDB_NORMAL_TABLE:
    case TSDB_CHILD_TABLE: {
      *taskNum = 1;
      break;
    }
    case TSDB_SUPER_TABLE: {
      SDbObj* pDb = mndAcquireDb(pMnode, pStream->pCreate->triggerDB);
      if (NULL == pDb) {
        code = terrno;
        mstsError("failed to acquire db %s, error:%s", pStream->pCreate->triggerDB, terrstr());
        break;
      }

      *taskNum = pDb->cfg.numOfVgroups;
      break;
    }
    case TSDB_VIRTUAL_CHILD_TABLE:
    case TSDB_VIRTUAL_NORMAL_TABLE: {
      //STREAMTODO
      break;
    }
    default:
      *taskNum = 0;
      mstsDebug("%s ignore triggerTblType %d", __FUNCTION__, pStream->pCreate->triggerTblType);
      break;
  }

  int32_t calcTasksNum = taosArrayGetSize(pStream->pCreate->calcScanPlanList);
  for (int32_t i = 0; i < calcTasksNum; ++i) {
    SStreamCalcScan* pScan = taosArrayGet(pStream->pCreate->calcScanPlanList, i);
    *taskNum += taosArrayGetSize(pScan->vgList);
  }
}

static int32_t msmBuildReaderTasks(SStmGrpCtx* pCtx, SStmStatus* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  
  TSDB_CHECK_CODE(msmTDAddTrigReaderTasks(pCtx, pInfo, pStream), lino, _exit);
  TSDB_CHECK_CODE(msmTDAddCalcReaderTasks(pCtx, pInfo, pStream), lino, _exit);

  TSDB_CHECK_CODE(msmSTAddToTaskMap(pCtx, streamId, pInfo->trigReaders, NULL), lino, _exit);
  TSDB_CHECK_CODE(msmSTAddToTaskMap(pCtx, streamId, pInfo->calcReaders, NULL), lino, _exit);
  
  TSDB_CHECK_CODE(msmSTAddToVgroupMap(pCtx, streamId, pInfo->trigReaders, NULL, true), lino, _exit);
  TSDB_CHECK_CODE(msmSTAddToVgroupMap(pCtx, streamId, pInfo->calcReaders, NULL, false), lino, _exit);
  
_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}

int32_t msmUpdatePlanSourceAddr(int64_t streamId, SSubplan* plan, int64_t clientId, SStmTaskSrcAddr* pSrc, int32_t msgType) {
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

  mstsDebug("try to update subplan %d sourceAddr, clientId:%" PRId64 ", taskId:%" PRId64 ", msgType:%d", 
      plan->id.subplanId, source.clientId, source.taskId, source.fetchMsgType);
  
  return qSetSubplanExecutionNode(plan, pSrc->groupId, &source);
}

int32_t msmGetTaskIdFromSubplanId(SStreamObj* pStream, SArray* pRunners, int32_t beginIdx, int32_t subplanId, int64_t* taskId) {
  int64_t streamId = pStream->pCreate->streamId;
  int32_t runnerNum = taosArrayGetSize(pRunners);
  for (int32_t i = beginIdx; i < runnerNum; ++i) {
    SStmTaskToDeployExt* pExt = taosArrayGet(pRunners, i);
    SSubplan* pPlan = pExt->deploy.msg.runner.pPlan;
    if (pPlan->id.subplanId == subplanId) {
      *taskId = pExt->deploy.task.taskId;
      return TSDB_CODE_SUCCESS;
    }
  }

  mstsError("subplanId %d not found in runner list", subplanId);

  return TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
}

int32_t msmUpdateLowestPlanSourceAddr(SSubplan* pPlan, SStmTaskDeploy* pDeploy, int64_t streamId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t key[2] = {streamId, -1};
  SNode* pNode = NULL;
  FOREACH(pNode, pPlan->pChildren) {
    if (QUERY_NODE_VALUE != nodeType(pNode)) {
      mstsError("invalid node type %d for runner's child subplan", nodeType(pNode));
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }
    SValueNode* pVal = (SValueNode*)pNode;
    if (TSDB_DATA_TYPE_BIGINT != pVal->node.resType.type) {
      mstsError("invalid value node data type %d for runner's child subplan", pVal->node.resType.type);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }

    key[1] = MND_GET_RUNNER_SUBPLANID(pVal->datum.i);

    SArray** ppRes = taosHashGet(mStreamMgmt.toUpdateScanMap, key, sizeof(key));
    if (NULL == ppRes) {
      mstsError("lowest runner subplan ID:%d,%d can't get its child ID:%" PRId64 " addr", pPlan->id.groupId, pPlan->id.subplanId, key[1]);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }

    int32_t childrenNum = taosArrayGetSize(*ppRes);
    for (int32_t i = 0; i < childrenNum; ++i) {
      SStmTaskSrcAddr* pAddr = taosArrayGet(*ppRes, i);
      TAOS_CHECK_EXIT(msmUpdatePlanSourceAddr(streamId, pPlan, pDeploy->task.taskId, pAddr, pAddr->isFromCache ? TDMT_STREAM_FETCH_FROM_CACHE : TDMT_STREAM_FETCH));
    }
  }

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmUpdateRunnerPlan(SStmGrpCtx* pCtx, SArray* pRunners, int32_t beginIdx, SStmTaskToDeployExt* pExt, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmTaskDeploy* pDeploy = &pExt->deploy;
  SSubplan* pPlan = pDeploy->msg.runner.pPlan;
  int64_t streamId = pStream->pCreate->streamId;

  if (pExt->lowestRunner) {
    TAOS_CHECK_EXIT(msmUpdateLowestPlanSourceAddr(pPlan, pDeploy, streamId));
  }
  
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
    TAOS_CHECK_EXIT(msmGetTaskIdFromSubplanId(pStream, pRunners, beginIdx, pSubplan->id.subplanId, &parentTaskId));
    TAOS_CHECK_EXIT(msmUpdatePlanSourceAddr(streamId, pSubplan, parentTaskId, &addr, TDMT_STREAM_FETCH_FROM_RUNNER));
  }
  
_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmUpdateRunnerPlans(SStmGrpCtx* pCtx, SArray* pRunners, SStreamObj* pStream, int32_t taskNum) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t runnerNum = taosArrayGetSize(pRunners);
  
  for (int32_t i = runnerNum - taskNum; i <= runnerNum - 1; ++i) {
    SStmTaskToDeployExt* pExt = taosArrayGet(pRunners, i);
    TAOS_CHECK_EXIT(msmUpdateRunnerPlan(pCtx, pRunners, i, pExt, pStream));
    TAOS_CHECK_EXIT(nodesNodeToString((SNode*)pExt->deploy.msg.runner.pPlan, false, (char**)&pExt->deploy.msg.runner.pPlan, NULL));

    SStreamTask* pTask = &pExt->deploy.task;
    msttDebugL("runner updated task plan:%s", (const char*)pExt->deploy.msg.runner.pPlan);
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
  SNodeListNode *plans = NULL;
  int32_t        taskNum = 0;
  int32_t        totalTaskNum = 0;
  
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

  SArray* deployList = NULL;
  int32_t deployNodeId = 0;
  SStmTaskStatus* pState = NULL;
  
  for (int32_t deployId = 0; deployId < pInfo->runnerDeploys; ++deployId) {
    totalTaskNum = 0;

    deployList = pInfo->runners[deployId];
    deployNodeId = msmAssignTaskSnodeId(pCtx->pMnode, pStream, (0 == deployId) ? true : false);
    if (!GOT_SNODE(deployNodeId)) {
      TAOS_CHECK_EXIT(terrno);
    }

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
        pState->status = STREAM_STATUS_NA;
        pState->lastUpTs = pCtx->currTs;
        pState->pStream = pInfo;

        SStmTaskDeploy info = {0};
        info.task.type = pState->type;
        info.task.streamId = streamId;
        info.task.taskId = pState->id.taskId;
        info.task.flags = pState->flags;
        info.task.seriousId = pState->id.seriousId;
        info.task.deployId = pState->id.deployId;
        info.task.nodeId = pState->id.nodeId;
        info.task.taskIdx = pState->id.taskIdx;
        TSDB_CHECK_CODE(msmBuildRunnerDeployInfo(&info, plan, pStream, pInfo->runnerReplica, 0 == i), lino, _exit);

        TSDB_CHECK_CODE(msmTDAddSnodeTask(mStreamMgmt.toDeploySnodeMap, &info, pStream, false, i == lowestLevelIdx), lino, _exit);
        
        atomic_add_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);        
      }

      mstsDebug("deploy %d level %d initialized, taskNum:%d", deployId, i, taskNum);
    }

    if (totalTaskNum != pDag->numOfSubplans) {
      mstsError("totalTaskNum %d mis-match with numOfSubplans %d", totalTaskNum, pDag->numOfSubplans);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }

    SStmSnodeTasksDeploy* pSnodeDeploy = taosHashGet(mStreamMgmt.toDeploySnodeMap, &pState->id.nodeId, sizeof(pState->id.nodeId));
    TSDB_CHECK_NULL(pSnodeDeploy, code, lino, _exit, terrno);
    
    TAOS_CHECK_EXIT(msmUpdateRunnerPlans(pCtx, pSnodeDeploy->runnerList, pStream, totalTaskNum));

    TAOS_CHECK_EXIT(nodesStringToNode(pStream->pCreate->calcPlan, (SNode**)&pDag));

    mstsDebug("total %d runner tasks added for deploy %d", totalTaskNum, deployId);
  }

  for (int32_t i = 0; i < pInfo->runnerDeploys; ++i) {
    TSDB_CHECK_CODE(msmSTAddToTaskMap(pCtx, streamId, pInfo->runners[i], NULL), lino, _exit);
    TSDB_CHECK_CODE(msmSTAddToSnodeMap(pCtx, streamId, pInfo->runners[i], NULL, 0, i), lino, _exit);
  }
  
  pInfo->runnerNum = totalTaskNum;
  
_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

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

  plans = (SNodeListNode *)nodesListGetNode(pDag->pSubplans, 0);
  taskNum = (int32_t)LIST_LENGTH(plans->pNodeList);

  for (int32_t r = 0; r < pAction->deployNum; ++r) {
    deployId = pAction->deployId[r];

    pRunner = taosArrayGet(pInfo->runners[deployId], 0);

    pStartRunner = pRunner;
    totalTaskNum = 0;

    newNodeId = msmAssignTaskSnodeId(pCtx->pMnode, pStream, (0 == r) ? true : false);
    if (!GOT_SNODE(newNodeId)) {
      TAOS_CHECK_EXIT(terrno);
    }

    for (int32_t i = lowestLevelIdx; i >= 0; --i) {
      plans = (SNodeListNode *)nodesListGetNode(pDag->pSubplans, i);
      taskNum = (int32_t)LIST_LENGTH(plans->pNodeList);
      totalTaskNum += taskNum;

      for (int32_t n = 0; n < taskNum; ++n) {
        SSubplan *plan = (SSubplan *)nodesListGetNode(plans->pNodeList, n);

        int32_t newTaskIdx = MND_SET_RUNNER_TASKIDX(i, n);
        if (pRunner->id.taskIdx != newTaskIdx) {
          mstsError("runner TASK:%" PRId64 " taskIdx %d mismatch with newTaskIdx:%d", pRunner->id.taskId, pRunner->id.taskIdx, newTaskIdx);
          TAOS_CHECK_EXIT(TSDB_CODE_STREAM_INTERNAL_ERROR);
        }

        pRunner->id.nodeId = newNodeId;

        SStmTaskDeploy info = {0};
        info.task.type = pRunner->type;
        info.task.streamId = streamId;
        info.task.taskId = pRunner->id.taskId;
        info.task.flags = pRunner->flags;
        info.task.seriousId = pRunner->id.seriousId;
        info.task.nodeId = pRunner->id.nodeId;
        info.task.taskIdx = pRunner->id.taskIdx;
        TSDB_CHECK_CODE(msmBuildRunnerDeployInfo(&info, plan, pStream, pInfo->runnerReplica, 0 == i), lino, _exit);

        TSDB_CHECK_CODE(msmTDAddSnodeTask(mStreamMgmt.toDeploySnodeMap, &info, pStream, false, i == lowestLevelIdx), lino, _exit);
        
        atomic_add_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);     

        pRunner++;
      }

      mstsDebug("level %d initialized, taskNum:%d", i, taskNum);
    }

    SStmSnodeTasksDeploy* pSnodeDeploy = taosHashGet(mStreamMgmt.toDeploySnodeMap, &newNodeId, sizeof(newNodeId));
    TSDB_CHECK_NULL(pSnodeDeploy, code, lino, _exit, terrno);
    
    TAOS_CHECK_EXIT(msmUpdateRunnerPlans(pCtx, pSnodeDeploy->runnerList, pStream, totalTaskNum));

    int32_t num = ((uint64_t)pRunner - (uint64_t)pStartRunner) / sizeof(SStmTaskStatus);
    TAOS_CHECK_EXIT(msmSTAddToSnodeMap(pCtx, streamId, NULL, pStartRunner, num, r));

    TAOS_CHECK_EXIT(nodesStringToNode(pStream->pCreate->calcPlan, (SNode**)&pDag));
  }

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


int32_t msmSetStreamRunnerExecReplica(int64_t streamId, SStmStatus* pInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  //STREAMTODO 
  
  pInfo->runnerDeploys = MND_STREAM_RUNNER_DEPLOY_NUM;
  pInfo->runnerReplica = 3;

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

  TAOS_CHECK_EXIT(msmBuildRunnerTasksImpl(pCtx, pPlan, pInfo, pStream));

  taosHashClear(mStreamMgmt.toUpdateScanMap);
  mStreamMgmt.toUpdateScanNum = 0;

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


static int32_t msmBuildStreamTasks(SStmGrpCtx* pCtx, SStmStatus* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;

  pCtx->triggerTaskId = msmAssignTaskId();
  pCtx->triggerNodeId = msmAssignTaskSnodeId(pCtx->pMnode, pStream, true);
  if (!GOT_SNODE(pCtx->triggerNodeId)) {
    TAOS_CHECK_EXIT(terrno);
  }

  TSDB_CHECK_CODE(msmBuildReaderTasks(pCtx, pInfo, pStream), lino, _exit);
  TSDB_CHECK_CODE(msmBuildRunnerTasks(pCtx, pInfo, pStream), lino, _exit);
  TSDB_CHECK_CODE(msmBuildTriggerTasks(pCtx, pInfo, pStream), lino, _exit);
  
_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmInitStmStatus(SStmStatus* pStatus, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;

  pStatus->lastActionTs = INT64_MIN;
  
  pStatus->streamName = taosStrdup(pStream->name);
  TSDB_CHECK_NULL(pStatus->streamName, code, lino, _exit, terrno);

  if (pStream->pCreate->numOfCalcSubplan > 0) {
    pStatus->runnerNum = pStream->pCreate->numOfCalcSubplan;
    
    TAOS_CHECK_EXIT(msmSetStreamRunnerExecReplica(streamId, pStatus));
  }
  
_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmDeployStreamTasks(SStmGrpCtx* pCtx, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStmStatus info = {0};
  SStmStatus* pStatus = NULL;

  TAOS_CHECK_EXIT(msmInitStmStatus(&info, pStream));

  TAOS_CHECK_EXIT(taosHashPut(mStreamMgmt.streamMap, &streamId, sizeof(streamId), &info, sizeof(info)));

  pStatus = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  
  TAOS_CHECK_EXIT(msmBuildStreamTasks(pCtx, pStatus, pStream));

  mndStreamLogSStmStatus("stream deployed", streamId, pStatus);

_exit:

  if (code) {
    if (NULL != pStatus) {
      atomic_store_8(&pStatus->stopped, 1);
      mstsError("stream build error:%s, will try to stop current stream", tstrerror(code));
    }
    
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


static int32_t msmLaunchStreamDepolyAction(SStmGrpCtx* pCtx, SStmStreamAction* pAction) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pAction->streamId;
  char* streamName = pAction->streamName;
  SStreamObj* pStream = NULL;

  SStmStatus** ppStream = (SStmStatus**)taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (ppStream) {
    mstsError("stream %s already in streamMap", pAction->streamName);
    code = TSDB_CODE_STREAM_INTERNAL_ERROR;
    goto _exit;
  }

  code = mndAcquireStream(pCtx->pMnode, streamName, &pStream);
  if (TSDB_CODE_MND_STREAM_NOT_EXIST == code) {
    mstsWarn("stream %s no longer exists, ignore deploy", streamName);
    return TSDB_CODE_SUCCESS;
  }

  TAOS_CHECK_EXIT(code);

  int8_t userStopped = atomic_load_8(&pStream->userStopped);
  int8_t userDropped = atomic_load_8(&pStream->userDropped);
  if (userStopped || userDropped) {
    mstsWarn("stream %s is stopped %d or removing %d, ignore deploy", streamName, userStopped, userDropped);
    goto _exit;
  }
  
  TAOS_CHECK_EXIT(msmDeployStreamTasks(pCtx, pStream));

_exit:

  mndReleaseStream(pCtx->pMnode, pStream);

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmLaunchTaskDepolyAction(SStmGrpCtx* pCtx, SStmTaskAction* pAction) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pAction->streamId;
  int64_t taskId = pAction->id.taskId;
  SStreamObj* pStream = NULL;

  mstsDebug("start to handle stream tasks action, action task type:%s", gStreamTaskTypeStr[pAction->type]);

  SStmStatus* pStatus = taosHashGet(mStreamMgmt.streamMap, &pAction->streamId, sizeof(pAction->streamId));
  if (NULL == pStatus) {
    mstsError("stream not in streamMap, remain:%d", taosHashGetSize(mStreamMgmt.streamMap));
    TAOS_CHECK_EXIT(TSDB_CODE_STREAM_INTERNAL_ERROR);
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
    case STREAM_READER_TASK: {
      SStmTaskStatus** ppTask = taosHashGet(mStreamMgmt.taskMap, &pAction->streamId, sizeof(pAction->streamId) + sizeof(pAction->id.taskId));
      if (NULL == ppTask) {
        mstsError("TASK:%" PRId64 " not in taskMap, remain:%d", pAction->id.taskId, taosHashGetSize(mStreamMgmt.taskMap));
        TAOS_CHECK_EXIT(TSDB_CODE_STREAM_INTERNAL_ERROR);
      }

      SStmTaskDeploy info = {0};
      info.task.type = pAction->type;
      info.task.streamId = streamId;
      info.task.taskId = pAction->id.taskId;
      info.task.seriousId = (*ppTask)->id.seriousId;
      info.task.nodeId = pAction->id.nodeId;
      info.task.taskIdx = pAction->id.taskIdx;

      bool isTriggerReader = STREAM_IS_TRIGGER_READER(pAction->flag);
      void* scanPlan = NULL;
      if (!isTriggerReader) {
        scanPlan = taosArrayGet(pStream->pCreate->calcScanPlanList, pAction->id.taskIdx);
        if (NULL == scanPlan) {
          mstsError("fail to get TASK:%" PRId64 " scanPlan, taskIdx:%d, scanPlanNum:%zu", 
              pAction->id.taskId, pAction->id.taskIdx, taosArrayGetSize(pStream->pCreate->calcScanPlanList));
          TAOS_CHECK_EXIT(TSDB_CODE_STREAM_INTERNAL_ERROR);
        }
      }
      
      TAOS_CHECK_EXIT(msmBuildReaderDeployInfo(&info, pStream, scanPlan, pStatus, isTriggerReader));
      TAOS_CHECK_EXIT(msmTDAddToVgroupMap(mStreamMgmt.toDeployVgMap, &info, streamId));
      break;
    }
    case STREAM_TRIGGER_TASK: {
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
      TAOS_CHECK_EXIT(msmTDAddSnodeTask(mStreamMgmt.toDeploySnodeMap, &info, pStream, true, false));
      TAOS_CHECK_EXIT(msmSTAddToSnodeMap(pCtx, streamId, NULL, *ppTask, 1, -1));
      
      atomic_add_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);
      break;
    }
    case STREAM_RUNNER_TASK: {
      if (pAction->triggerStatus) {
        pCtx->triggerTaskId = pAction->triggerStatus->id.taskId;
        pAction->triggerStatus->id.nodeId = msmAssignTaskSnodeId(pCtx->pMnode, pStream, true);
        if (!GOT_SNODE(pAction->triggerStatus->id.nodeId)) {
          mstsError("no avaible snode for deploying trigger task, seriousId:%" PRId64, pAction->triggerStatus->id.seriousId);
          return TSDB_CODE_SUCCESS;
        }

        pCtx->triggerNodeId = pAction->triggerStatus->id.nodeId;
      } else {
        pCtx->triggerTaskId = pStatus->triggerTask->id.taskId;
        pCtx->triggerNodeId = pStatus->triggerTask->id.nodeId;
      }

      TAOS_CHECK_EXIT(msmUPPrepareReaderTasks(pCtx, pStatus, pStream));
      
      SQueryPlan* pPlan = NULL;
      TAOS_CHECK_EXIT(nodesStringToNode(pStream->pCreate->calcPlan, (SNode**)&pPlan));
      
      TAOS_CHECK_EXIT(msmReBuildRunnerTasks(pCtx, pPlan, pStatus, pStream, pAction));
      
      taosHashClear(mStreamMgmt.toUpdateScanMap);
      mStreamMgmt.toUpdateScanNum = 0;

      if (pAction->triggerStatus) {
        SStmTaskDeploy info = {0};
        info.task.type = STREAM_TRIGGER_TASK;
        info.task.streamId = streamId;
        info.task.taskId = pCtx->triggerTaskId;
        info.task.seriousId = pAction->triggerStatus->id.seriousId;
        info.task.nodeId = pCtx->triggerNodeId;
        info.task.taskIdx = 0;

        TAOS_CHECK_EXIT(msmBuildTriggerDeployInfo(pCtx->pMnode, pStatus, &info, pStream));
        TAOS_CHECK_EXIT(msmTDAddSnodeTask(mStreamMgmt.toDeploySnodeMap, &info, pStream, true, false));
        TAOS_CHECK_EXIT(msmSTAddToSnodeMap(pCtx, streamId, NULL, pAction->triggerStatus, 1, -1));
        
        atomic_add_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);
      }
      
      break;
    }
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

static int32_t msmSTRemoveStream(int64_t streamId) {
  int32_t code = TSDB_CODE_SUCCESS;
  void* pIter = NULL;
  
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
        mstsError("TASK:%" PRId64 " remove from taskMap failed, error:%s", taskId, tstrerror(code));
      } else {
        mstsDebug("TASK:%" PRId64 " removed from taskMap", taskId);
      }
    }
  }

  code = taosHashRemove(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (code) {
    mstsError("stream remove from streamMap failed, error:%s", tstrerror(code));
  } else {
    mstsDebug("stream removed from streamMap, remains:%d", taosHashGetSize(mStreamMgmt.streamMap));
  }
  
  return code;
}

static int32_t msmRemoveStreamFromMaps(SMnode* pMnode, int64_t streamId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  mstsInfo("start to remove stream from maps, current stream num:%d", taosHashGetSize(mStreamMgmt.streamMap));

  //TAOS_CHECK_EXIT(msmTDRemoveStream(streamId));
  TAOS_CHECK_EXIT(msmSTRemoveStream(streamId));

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    mstsInfo("end remove stream from maps, current stream num:%d", taosHashGetSize(mStreamMgmt.streamMap));
  }

  return code;
}

int32_t msmUndeployStream(SMnode* pMnode, int64_t streamId, char* streamName) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SStmStatus** ppStream = (SStmStatus**)taosHashAcquire(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == ppStream) {
    mstsInfo("stream %s already not in streamMap", streamName);
    goto _exit;
  }

  atomic_store_8(&(*ppStream)->stopped, 1);

_exit:

  taosHashRelease(mStreamMgmt.streamMap, ppStream);

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmHandleStreamActions(SStmGrpCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmQNode* pQNode = NULL;

  while (mndStreamActionDequeue(mStreamMgmt.actionQ, &pQNode)) {
    switch (pQNode->type) {
      case STREAM_ACT_DEPLOY:
        if (pQNode->streamAct) {
          mstDebug("start to handle stream deploy action");
          TAOS_CHECK_EXIT(msmLaunchStreamDepolyAction(pCtx, &pQNode->action.stream));
        } else {
          mstDebug("start to handle task deploy action");
          TAOS_CHECK_EXIT(msmLaunchTaskDepolyAction(pCtx, &pQNode->action.task));
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

  return code;
}

int32_t msmHandleGrantExpired(SMnode *pMnode) {
  //STREAMTODO
  return TSDB_CODE_SUCCESS;
}

void msmDestroyStreamDeploy(SStmStreamDeploy* pStream) {
  taosArrayDestroy(pStream->readerTasks);
  taosArrayDestroy(pStream->runnerTasks);
}

static int32_t msmInitStreamDeploy(SStmStreamDeploy* pStream, SStmTaskDeploy* pDeploy) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pDeploy->task.streamId;
  
  switch (pDeploy->task.type) {
    case STREAM_READER_TASK:
      if (NULL == pStream->readerTasks) {
        pStream->streamId = streamId;
        pStream->readerTasks = taosArrayInit(20, POINTER_BYTES);
        TSDB_CHECK_NULL(pStream->readerTasks, code, lino, _exit, terrno);
      }
      
      TSDB_CHECK_NULL(taosArrayPush(pStream->readerTasks, &pDeploy), code, lino, _exit, terrno);
      break;
    case STREAM_TRIGGER_TASK:
      pStream->streamId = streamId;
      pStream->triggerTask = pDeploy;
      break;
    case STREAM_RUNNER_TASK:
      if (NULL == pStream->runnerTasks) {
        pStream->streamId = streamId;
        pStream->runnerTasks = taosArrayInit(20, POINTER_BYTES);
        TSDB_CHECK_NULL(pStream->runnerTasks, code, lino, _exit, terrno);
      }      
      TSDB_CHECK_NULL(taosArrayPush(pStream->runnerTasks, &pDeploy), code, lino, _exit, terrno);
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

      msmDestroyStreamDeploy(&streamDeploy);
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

    atomic_add_fetch_32(deployed, 1);
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
  int32_t tidx = streamGetThreadIdx(mStreamMgmt.threadNum, pCtx->pReq->streamGId);

  mstDebug("start to add stream vgroup tasks deploy");
  
  for (int32_t i = 0; i < vgNum; ++i) {
    int32_t* vgId = taosArrayGet(pCtx->pReq->pVgLeaders, i);
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

  if (taosRTryLockLatch(&pSnode->lock)) {
    return TSDB_CODE_SUCCESS;
  }
  
  if (atomic_load_32(&pSnode->triggerDeployed) < taosArrayGetSize(pSnode->triggerList)) {
    TAOS_CHECK_EXIT(msmGrpAddDeployTasks(pCtx->deployStm, pSnode->triggerList, &pSnode->triggerDeployed));
  }

  if (atomic_load_32(&pSnode->runnerDeployed) < taosArrayGetSize(pSnode->runnerList)) {
    TAOS_CHECK_EXIT(msmGrpAddDeployTasks(pCtx->deployStm, pSnode->runnerList, &pSnode->runnerDeployed));
  }
  
  taosRUnLockLatch(&pSnode->lock);

_exit:

  if (code) {
    if (pSnode) {
      taosRUnLockLatch(&pSnode->lock);
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
    TAOS_CHECK_EXIT(msmUpdateStreamLastActTs(pDeploy->streamId, pCtx->currTs));

    int64_t streamId = pDeploy->streamId;
    mstsDebug("stream DEPLOY added to dnode %d hb rsp, readerTasks:%zu, triggerTask:%d, runnerTasks:%zu", 
        pCtx->pReq->dnodeId, taosArrayGetSize(pDeploy->readerTasks), pDeploy->triggerTask ? 1 : 0, taosArrayGetSize(pDeploy->runnerTasks));
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
      atomic_sub_fetch_32(&mStreamMgmt.toDeployVgTaskNum, taskNum);
      taosArrayDestroy(pVg->taskList);
      taosHashRemove(mStreamMgmt.toDeployVgMap, vgId, sizeof(*vgId));
      taosWUnLockLatch(&pVg->lock);
      taosHashRelease(mStreamMgmt.toDeployVgMap, pVg);
      continue;
    }

    for (int32_t m = taskNum - 1; m >= 0; --m) {
      SStmTaskToDeployExt* pExt = taosArrayGet(pVg->taskList, m);
      if (!pExt->deployed) {
        continue;
      }
      taosArrayRemove(pVg->taskList, m);
      atomic_sub_fetch_32(&mStreamMgmt.toDeployVgTaskNum, 1);
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
    atomic_sub_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, triggerNum);
    taosArrayDestroy(pSnode->triggerList);
    pSnode->triggerList = NULL;
  }

  if (atomic_load_32(&pSnode->runnerDeployed) == runnerNum) {
    atomic_sub_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, runnerNum);
    taosArrayDestroy(pSnode->runnerList);
    pSnode->runnerList = NULL;
  }

  if (NULL == pSnode->triggerList && NULL == pSnode->runnerList) {
    taosHashRemove(mStreamMgmt.toDeploySnodeMap, &snodeId, sizeof(snodeId));
    taosWUnLockLatch(&pSnode->lock);
    taosHashRelease(mStreamMgmt.toDeploySnodeMap, pSnode);
    return;
  }

  if (atomic_load_32(&pSnode->triggerDeployed) > 0) {
    for (int32_t m = triggerNum - 1; m >= 0; --m) {
      SStmTaskToDeployExt* pExt = taosArrayGet(pSnode->triggerList, m);
      if (!pExt->deployed) {
        continue;
      }

      atomic_sub_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);
      taosArrayRemove(pSnode->triggerList, m);
    }
    
    pSnode->triggerDeployed = 0;
  }

  if (atomic_load_32(&pSnode->runnerDeployed) > 0) {
    for (int32_t m = runnerNum - 1; m >= 0; --m) {
      SStmTaskToDeployExt* pExt = taosArrayGet(pSnode->runnerList, m);
      if (!pExt->deployed) {
        continue;
      }

      atomic_sub_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);
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
  taosHashClear(mStreamMgmt.tCtx[tidx].actionStm[pHb->streamGId]);
  taosHashClear(mStreamMgmt.tCtx[tidx].deployStm[pHb->streamGId]);
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


int32_t msmGrpAddActionUndeploy(SStmGrpCtx* pCtx, int64_t streamId, SStreamTask* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t action = STREAM_ACT_UNDEPLOY;
  bool    dropped = false;

  TAOS_CHECK_EXIT(mstIsStreamDropped(pCtx->pMnode, streamId, &dropped));
  
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

  readerNum = taosArrayGetSize(pStream->calcReaders);
  for (int32_t i = 0; i < readerNum; ++i) {
    SStmTaskStatus* pStatus = taosArrayGet(pStream->calcReaders, i);
    if (STREAM_STATUS_INIT != pStatus->status && STREAM_STATUS_RUNNING != pStatus->status) {
      return false;
    }
  }

  for (int32_t i = 0; i < pStream->runnerDeploys; ++i) {
    int32_t runnerNum = taosArrayGetSize(pStream->runners[i]);
    for (int32_t i = 0; i < runnerNum; ++i) {
      SStmTaskStatus* pStatus = taosArrayGet(pStream->runners[i], i);
      if (STREAM_STATUS_INIT != pStatus->status && STREAM_STATUS_RUNNING != pStatus->status) {
        return false;
      }
    }
  }
  
  return true;
}


int32_t msmHandleTaskAbnormalStatus(SStmGrpCtx* pCtx, SStmTaskStatusMsg* pMsg) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t action = 0;
  int64_t streamId = pMsg->streamId;
  SStreamTask* pTask = (SStreamTask*)pMsg;

  msttDebug("start to handle task abnormal status %d", pTask->status);
  
  SStmStatus* pStatus = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStatus) {
    msttInfo("stream no longer exists in streamMap, try to undeploy current task, idx:%d", pMsg->taskIdx);
    return msmGrpAddActionUndeploy(pCtx, streamId, pTask);
  }

  if (atomic_load_8(&pStatus->stopped)) {
    msttInfo("stream stopped, try to undeploy current task, idx:%d", pMsg->taskIdx);
    return msmGrpAddActionUndeploy(pCtx, streamId, pTask);
  }
  
  switch (pMsg->status) {
    case STREAM_STATUS_INIT:
      if (STREAM_TRIGGER_TASK != pMsg->type) {
        msttTrace("task status is INIT and not trigger task, ignore it, currTs:%" PRId64 ", lastTs:%" PRId64, pCtx->currTs, pStatus->lastActionTs);
        return code;
      }
      
      if (INT64_MIN == pStatus->lastActionTs) {
        msttDebug("task still not deployed, ignore it, currTs:%" PRId64 ", lastTs:%" PRId64, pCtx->currTs, pStatus->lastActionTs);
        return code;
      }
      
      if ((pCtx->currTs - pStatus->lastActionTs) < STREAM_ACT_MIN_DELAY_MSEC) {
        msttDebug("task wait not enough between actions, currTs:%" PRId64 ", lastTs:%" PRId64, pCtx->currTs, pStatus->lastActionTs);
        return code;
      }

      if (STREAM_IS_RUNNING(pStatus->triggerTask->status)) {
        msttDebug("stream already running, ignore status: %s", gStreamStatusStr[pTask->status]);
      } else if (GOT_SNODE(pCtx->pReq->snodeId) && msmCheckStreamStartCond(streamId, pCtx->pReq->snodeId)) {
        TAOS_CHECK_EXIT(msmGrpAddActionStart(pCtx->actionStm, streamId, &pStatus->triggerTask->id));
      }
      break;
    case STREAM_STATUS_STOPPED:
    case STREAM_STATUS_FAILED:
    default:
      break;
  }

_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

void msmHandleStatusUpdateErr(SStmGrpCtx* pCtx, EStmErrType err, SStmTaskStatusMsg* pStatus) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStreamTask* pTask = (SStreamTask*)pStatus;
  int64_t streamId = pStatus->streamId;

  msttInfo("start to handle task status update error: %d", err);
  
  // STREAMTODO

  if (STM_ERR_TASK_NOT_EXISTS == err || STM_ERR_STREAM_STOPPED == err) {
    TAOS_CHECK_EXIT(msmGrpAddActionUndeploy(pCtx, streamId, pTask));
  }

_exit:

  if (code) {
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
    msttDebug("task status %s got, taskIdx:%d", gStreamStatusStr[pTask->status], pTask->taskIdx);
    
    SStmTaskStatus** ppStatus = taosHashGet(mStreamMgmt.taskMap, &pTask->streamId, sizeof(pTask->streamId) + sizeof(pTask->taskId));
    if (NULL == ppStatus) {
      msttWarn("task no longer exists in taskMap, will try to undeploy current task, taskIdx:%d", pTask->taskIdx);
      msmHandleStatusUpdateErr(pCtx, STM_ERR_TASK_NOT_EXISTS, pTask);
      continue;
    }

    SStmStatus* pStream = (SStmStatus*)(*ppStatus)->pStream;
    if (pStream->stopped) {
      msttWarn("stream already stopped, will try to undeploy current task, taskIdx:%d", pTask->taskIdx);
      msmHandleStatusUpdateErr(pCtx, STM_ERR_STREAM_STOPPED, pTask);
      continue;
    }

    if ((pTask->seriousId != (*ppStatus)->id.seriousId) || (pTask->nodeId != (*ppStatus)->id.nodeId)) {
      msttInfo("task mismatch with it in taskMap, will try to rm it, current seriousId:%" PRId64 ", nodeId:%d", 
          (*ppStatus)->id.seriousId, (*ppStatus)->id.nodeId);
          
      msmHandleStatusUpdateErr(pCtx, STM_ERR_TASK_NOT_EXISTS, pTask);
      continue;
    }
    
    (*ppStatus)->status = pTask->status;
    (*ppStatus)->lastUpTs = pCtx->currTs;
    
    if (STREAM_STATUS_RUNNING != pTask->status) {
      TAOS_CHECK_EXIT(msmHandleTaskAbnormalStatus(pCtx, pTask));
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
    TAOS_CHECK_EXIT(msmInitStmStatus(&status, pStream));
    TAOS_CHECK_EXIT(taosHashPut(mStreamMgmt.streamMap, &streamId, sizeof(streamId), &status, sizeof(status)));
    pStatus = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
    TSDB_CHECK_NULL(pStatus, code, lino, _exit, terrno);
    msttDebug("stream added to streamMap cause of new task status:%s", gStreamStatusStr[pTask->status]);
  }

  SStmTaskStatus* pNewTask = NULL;
  switch (pTask->type) {
    case STREAM_READER_TASK: {
      SArray** ppList = STREAM_IS_TRIGGER_READER(pTask->flags) ? &pStatus->trigReaders : &pStatus->calcReaders;
      if (NULL == *ppList) {
        *ppList = taosArrayInit(2, sizeof(SStmTaskStatus));
        TSDB_CHECK_NULL(*ppList, code, lino, _exit, terrno);
      }
      SStmTaskStatus taskStatus;
      mstSetTaskStatusFromMsg(pCtx, &taskStatus, pTask);
      pNewTask = taosArrayPush(*ppList, &taskStatus);
      TSDB_CHECK_NULL(pNewTask, code, lino, _exit, terrno);

      TAOS_CHECK_EXIT(msmSTAddToTaskMap(pCtx, streamId, NULL, pNewTask));
      TAOS_CHECK_EXIT(msmSTAddToVgroupMapImpl(streamId, pNewTask, STREAM_IS_TRIGGER_READER(pTask->flags)));
      break;
    }
    case STREAM_TRIGGER_TASK: {
      taosMemoryFreeClear(pStatus->triggerTask);
      pStatus->triggerTask = taosMemoryMalloc(sizeof(*pStatus->triggerTask));
      TSDB_CHECK_NULL(pStatus->triggerTask, code, lino, _exit, terrno);
      mstSetTaskStatusFromMsg(pCtx, pStatus->triggerTask, pTask);
      pNewTask = pStatus->triggerTask;

      TAOS_CHECK_EXIT(msmSTAddToTaskMap(pCtx, streamId, NULL, pNewTask));
      TAOS_CHECK_EXIT(msmSTAddToSnodeMapImpl(streamId, pNewTask, 0));
      break;
    }
    case STREAM_RUNNER_TASK:{
      if (NULL == pStatus->runners[pTask->deployId]) {
        pStatus->runners[pTask->deployId] = taosArrayInit(2, sizeof(SStmTaskStatus));
        TSDB_CHECK_NULL(pStatus->runners[pTask->deployId], code, lino, _exit, terrno);
      }
      SStmTaskStatus taskStatus;
      mstSetTaskStatusFromMsg(pCtx, &taskStatus, pTask);
      pNewTask = taosArrayPush(pStatus->runners[pTask->deployId], &taskStatus);
      TSDB_CHECK_NULL(pNewTask, code, lino, _exit, terrno);

      TAOS_CHECK_EXIT(msmSTAddToTaskMap(pCtx, streamId, NULL, pNewTask));
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
  SStreamTaskStart start;
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
    }

    if ((STREAM_ACT_START & pAction->actions)) {
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

void msmUndeployNodeAllStreams(SStmGrpCtx* pCtx) {
  //STREAMTODO
}

int32_t msmCheckUpdateSnodeTs(SStmGrpCtx* pCtx) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  SStmSnodeStatus* pStatus = NULL;
  bool     noExists = false;

  while (true) {
    pStatus = taosHashGet(mStreamMgmt.snodeMap, &pCtx->pReq->snodeId, sizeof(pCtx->pReq->snodeId));
    if (NULL == pStatus) {
      if (noExists) {
        mstWarn("snode %d not exists in snodeMap, may be dropped, try to undeploy all", pCtx->pReq->snodeId);
        msmUndeployNodeAllStreams(pCtx);
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

int32_t msmWatchHandleEnding(SStmGrpCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (0 == atomic_load_8(&mStreamMgmt.watch.taskRemains)) {
    mstInfo("no stream tasks remain during watch state");
    goto _exit;
  }

  while (atomic_load_32(&mStreamMgmt.watch.processing) > 1) {
    (void)sched_yield();
  }

_exit:

  mStreamMgmt.lastTaskId += 1000;

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

  atomic_add_fetch_32(&mStreamMgmt.watch.processing, 1);
  
  if (atomic_load_8(&mStreamMgmt.watch.ending)) {
    goto _exit;
  }

  TAOS_CHECK_EXIT(msmCheckUpdateDnodeTs(pCtx));
  if (GOT_SNODE(pReq->snodeId)) {
    TAOS_CHECK_EXIT(msmCheckUpdateSnodeTs(pCtx));
  }

  if (taosArrayGetSize(pReq->pStreamStatus) > 0) {
    atomic_store_8(&mStreamMgmt.watch.taskRemains, 1);
    TAOS_CHECK_EXIT(msmWatchHandleStatusUpdate(pCtx));
  }

  if (((pCtx->currTs - MND_STREAM_GET_LAST_TS(STM_OP_ACTIVE_BEGIN)) > MST_ISOLATION_DURATION) &&
      (0 == atomic_val_compare_exchange_8(&mStreamMgmt.watch.ending, 0, 1))) {
    TAOS_CHECK_EXIT(msmWatchHandleEnding(pCtx));
  }

_exit:

  atomic_sub_fetch_32(&mStreamMgmt.watch.processing, 1);
  
  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmCheckDeployTrigReader(SStmGrpCtx* pCtx, SStmTaskStatusMsg* pTask, int32_t vgId, SStreamMgmtRsp* pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  bool    readerExists = false;
  int64_t streamId = pTask->streamId;
  SStmStatus* pStatus = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStatus) {
    mstsError("stream not deployed, remainStreams:%d", taosHashGetSize(mStreamMgmt.streamMap));
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_NOT_IN_DEPLOY);
  }

  if (atomic_load_8(&pStatus->stopped)) {
    msttInfo("stream stopped, ignore deploy trigger reader, vgId:%d", vgId);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_STOPPED);
  }

  int32_t readerNum = taosArrayGetSize(pStatus->trigReaders);
  for (int32_t i = 0; i < readerNum; ++i) {
    SStmTaskStatus* pReader = (SStmTaskStatus*)taosArrayGet(pStatus->trigReaders, i);
    if (pReader->id.nodeId == vgId) {
      TSDB_CHECK_NULL(taosArrayPush(pRsp->cont.vgIds, &vgId), code, lino, _exit, terrno);
      readerExists = true;
      break;
    }
  }

  if (!readerExists) {
    SStmTaskStatus* pState = taosArrayReserve(pStatus->trigReaders, 1);
    TAOS_CHECK_EXIT(msmTDAddSingleTrigReader(pCtx, pState, vgId, pStatus, NULL, streamId));
    TSDB_CHECK_CODE(msmSTAddToTaskMap(pCtx, streamId, NULL, pState), lino, _exit);
    TSDB_CHECK_CODE(msmSTAddToVgroupMap(pCtx, streamId, NULL, pState, true), lino, _exit);
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmProcessDeployOrigReader(SStmGrpCtx* pCtx, SStmTaskStatusMsg* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t vgId = 0;
  SArray* pTbs = pTask->pMgmtReq->cont.fullTableNames;
  int32_t tbNum = taosArrayGetSize(pTbs);
  SStreamDbTableName* pName = NULL;
  SSHashObj* pDbVgroups = NULL;
  SStreamMgmtRsp rsp = {0};
  rsp.reqId = pTask->pMgmtReq->reqId;
  rsp.header.msgType = STREAM_MSG_ORIGTBL_READER_INFO;

  pTask->pMgmtReq = NULL;
  rsp.task = *(SStreamTask*)pTask;

  if (tbNum > 0) {
    TAOS_CHECK_EXIT(mndStreamBuildDBVgroupsMap(pCtx->pMnode, &pDbVgroups));
    rsp.cont.vgIds = taosArrayInit(tbNum, sizeof(int32_t));
    TSDB_CHECK_NULL(rsp.cont.vgIds, code, lino, _exit, terrno);
    rsp.cont.readerList = taosArrayInit(tbNum, sizeof(SStreamTaskAddr));
    TSDB_CHECK_NULL(rsp.cont.readerList, code, lino, _exit, terrno);
  }
  
  for (int32_t i = 0; i < tbNum; ++i) {
    pName = (SStreamDbTableName*)taosArrayGet(pTbs, i);
    TAOS_CHECK_EXIT(mndStreamGetTableVgId(pDbVgroups, pName->dbFName, pName->tbName, &vgId));
    TAOS_CHECK_EXIT(msmCheckDeployTrigReader(pCtx, pTask, vgId, &rsp));
  }

  TSDB_CHECK_NULL(taosArrayPush(pCtx->pRsp->rsps.rspList, &rsp), code, lino, _exit, terrno);

_exit:

  if (code) {
    mndStreamDestroySStreamMgmtRsp(&rsp);
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  mndStreamDestroyDbVgroupsHash(pDbVgroups);
  taosArrayDestroy(pTbs);

  return code;
}

int32_t msmHandleTaskMgmtReq(SStmGrpCtx* pCtx, SStmTaskStatusMsg* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  switch (pTask->pMgmtReq->type) {
    case STREAM_MGMT_REQ_TRIGGER_ORIGTBL_READER:
      TAOS_CHECK_EXIT(msmProcessDeployOrigReader(pCtx, pTask));
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
  if (reqNum > 0) {
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
    TAOS_CHECK_EXIT(msmCheckUpdateSnodeTs(pCtx));
  }
  
  if (atomic_load_64(&mStreamMgmt.actionQ->qRemainNum) > 0 && 0 == taosWTryLockLatch(&mStreamMgmt.actionQLock)) {
    code = msmHandleStreamActions(pCtx);
    if (code) {
      taosWUnLockLatch(&mStreamMgmt.actionQLock);
      TAOS_CHECK_EXIT(code);
    }
    if (taosArrayGetSize(pReq->pStreamReq) > 0) {
      code = msmHandleStreamRequests(pCtx);
    }
    taosWUnLockLatch(&mStreamMgmt.actionQLock);
    TAOS_CHECK_EXIT(code);
  }

  if (atomic_load_32(&mStreamMgmt.toDeployVgTaskNum) > 0) {
    TAOS_CHECK_EXIT(msmGrpAddDeployVgTasks(pCtx));
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


int32_t msmHandleStreamHbMsg(SMnode* pMnode, int64_t currTs, SStreamHbMsg* pHb, SMStreamHbRspMsg* pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (0 == atomic_load_8(&mStreamMgmt.active)) {
    mstWarn("mnode stream is NOT active, ignore stream hb from dnode %d streamGid %d", pHb->dnodeId, pHb->streamGId);
    return code;
  }

  mstWaitRLock(&mStreamMgmt.runtimeLock);

  int32_t tidx = streamGetThreadIdx(mStreamMgmt.threadNum, pHb->streamGId);
  SStmGrpCtx* pCtx = &mStreamMgmt.tCtx[tidx].grpCtx[pHb->streamGId];

  pCtx->tidx = tidx;
  pCtx->pMnode = pMnode;
  pCtx->currTs = currTs;
  pCtx->pReq = pHb;
  pCtx->pRsp = pRsp;
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

  taosRUnLockLatch(&mStreamMgmt.runtimeLock);

  return code;
}

void msmHandleBecomeLeader(SMnode *pMnode) {
  taosWLockLatch(&mStreamMgmt.runtimeLock);
  msmInitRuntimeInfo(pMnode);
  taosWUnLockLatch(&mStreamMgmt.runtimeLock);
  atomic_store_8(&mStreamMgmt.active, 1);
}

void msmHandleBecomeNotLeader(SMnode *pMnode) {  
  if (atomic_val_compare_exchange_8(&mStreamMgmt.active, 1, 0)) {
    taosWLockLatch(&mStreamMgmt.runtimeLock);
    msmDestroyRuntimeInfo(pMnode);
    taosWUnLockLatch(&mStreamMgmt.runtimeLock);
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

  if (NULL == pStatus && !MST_STM_STATIC_PASS_ISOLATION(pStream)) {
    mstsDebug("stream not pass static isolation time, updateTime:%" PRId64 ", currentTs %" PRId64 ", ignore check it", 
        pStream->updateTime, mStreamMgmt.hCtx.currentTs);
    return true;
  }  

  if (pStatus) {
    if (userDropped || userStopped || atomic_load_8(&pStatus->stopped)) {
      (void)msmRemoveStreamFromMaps(pMnode, streamId);
    }

    return true;
  }

  mndStreamPostAction(mStreamMgmt.actionQ, streamId, pStream->pCreate->name, STREAM_ACT_DEPLOY);

  return true;
}

void msmCheckLoopStreamMap(SMnode *pMnode) {
  SStmStatus* pStatus = NULL;
  void* pIter = NULL;
  while (true) {
    pIter = taosHashIterate(mStreamMgmt.streamMap, pIter);
    if (NULL == pIter) {
      break;
    }

    pStatus = (SStmStatus*)pIter;
    if (atomic_load_8(&pStatus->stopped)) {
      (void)msmRemoveStreamFromMaps(pMnode, *(int64_t*)taosHashGetKey(pIter, NULL));
      continue;
    }

    if (!sdbCheckExists(pMnode->pSdb, SDB_STREAM, pStatus->streamName)) {
      (void)msmRemoveStreamFromMaps(pMnode, *(int64_t*)taosHashGetKey(pIter, NULL));
      continue;
    }
  }
}

void msmCheckStreamsStatus(SMnode *pMnode) {
  SStmCheckStatusCtx ctx = {0};
  if (MST_READY_FOR_SDB_LOOP()) {
    mstDebug("ready to check sdb loop, lastTs:%" PRId64, mStreamMgmt.lastTs[STM_OP_LOOP_SDB].ts);
    sdbTraverse(pMnode->pSdb, SDB_STREAM, msmCheckLoopStreamSdb, &ctx, NULL, NULL);
    MND_STREAM_SET_LAST_TS(STM_OP_LOOP_SDB, mStreamMgmt.hCtx.currentTs);
  }

  if (MST_READY_FOR_MAP_LOOP()) {
    mstDebug("ready to check map loop, lastTs:%" PRId64, mStreamMgmt.lastTs[STM_OP_LOOP_MAP].ts);
    msmCheckLoopStreamMap(pMnode);
    MND_STREAM_SET_LAST_TS(STM_OP_LOOP_MAP, mStreamMgmt.hCtx.currentTs);
  }
}

void msmCheckTaskListStatus(int64_t streamId, SStmTaskStatus** pList, int32_t taskNum) {
  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskStatus* pTask = *(pList + i);
    
    int64_t noUpTs = mStreamMgmt.hCtx.currentTs - pTask->lastUpTs;
    if (noUpTs < MST_ISOLATION_DURATION) {
      continue;
    }

    mstsInfo("TASK:%" PRId64 " status not updated for %" PRId64 "ms, will try to redeploy it", 
        pTask->id.taskId, noUpTs);

    int64_t newSid = atomic_add_fetch_64(&pTask->id.seriousId, 1);
    mstsDebug("task %" PRIx64 " SID updated to %" PRIx64, pTask->id.taskId, newSid);

    SStmTaskAction task = {0};
    task.streamId = streamId;
    task.id = pTask->id;
    task.flag = pTask->flags;
    task.type = pTask->type;
    
    mndStreamPostTaskAction(mStreamMgmt.actionQ, &task, STREAM_ACT_DEPLOY);
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

void msmCheckVgroupStatus(SMnode *pMnode) {
  void* pIter = NULL;
  
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
    msmHandleRunnerRedeploy(streamId, pStream, &task.deployNum, task.deployId);
    if (task.deployNum > 0) {
      task.triggerStatus = pStream->trigger;
      task.multiRunner = true;
      task.type = STREAM_RUNNER_TASK;
      
      mndStreamPostTaskAction(mStreamMgmt.actionQ, &task, STREAM_ACT_DEPLOY);
      
      mstsDebug("runner tasks %d redeploys added to actionQ", task.deployNum);
    } else if (pStream->trigger) {
      int64_t newSid = atomic_add_fetch_64(&pStream->trigger->id.seriousId, 1);
      mstsDebug("task %" PRIx64 " SID updated from to %" PRIx64, task.id.taskId, newSid);

      task.id = pStream->trigger->id;
      task.flag = pStream->trigger->flags;
      task.type = pStream->trigger->type;
      
      mndStreamPostTaskAction(mStreamMgmt.actionQ, &task, STREAM_ACT_DEPLOY);
      mstsDebug("trigger task %" PRIx64 " redeploy added to actionQ", task.id.taskId);
    }
  }

  taosHashCleanup(pSnode->streamTasks);
  pSnode->streamTasks = NULL;
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
    
    SStmSnodeStatus* pSnode = (SStmSnodeStatus*)pIter;
    if (NULL == pSnode->streamTasks) {
      mstDebug("ignore snode %d health check since empty tasks", snodeId);
      continue;
    }
    
    int64_t snodeNoUpTs = mStreamMgmt.hCtx.currentTs - pSnode->lastUpTs;
    if (snodeNoUpTs >= MST_ISOLATION_DURATION) {
      stInfo("snode %d lost, lastUpTs:%" PRId64 ", runnerThreadNum:%d, streamNum:%d", 
          snodeId, pSnode->lastUpTs, pSnode->runnerThreadNum, (int32_t)taosHashGetSize(pSnode->streamTasks));
      
      msmHandleSnodeLost(pMnode, pSnode);
      continue;
    }

    msmCheckSnodeStreamStatus(pSnode->streamTasks);
  }
}


void msmCheckTasksStatus(SMnode *pMnode) {
  msmCheckVgroupStatus(pMnode);
  msmCheckSnodeStatus(pMnode);
}

void msmCheckSnodesState(SMnode *pMnode) {
  if (!MST_READY_FOR_SNODE_LOOP()) {
    return;
  }

  mstDebug("ready to check snode loop, lastTs:%" PRId64, mStreamMgmt.lastTs[STM_OP_LOOP_SNODE].ts);

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
      taosHashRemove(mStreamMgmt.snodeMap, &snodeId, sizeof(snodeId));
      continue;
    }
    
    mstWarn("snode %d lost while streams remain, will redeploy all and rm it, lastUpTs:%" PRId64 ", runnerThreadNum:%d, streamNum:%d", 
        snodeId, pSnode->lastUpTs, pSnode->runnerThreadNum, (int32_t)taosHashGetSize(pSnode->streamTasks));
    
    msmHandleSnodeLost(pMnode, pSnode);
  }

  MND_STREAM_SET_LAST_TS(STM_OP_LOOP_MAP, mStreamMgmt.hCtx.currentTs);
}

void msmHealthCheck(SMnode *pMnode) {
  int8_t active = atomic_load_8(&mStreamMgmt.active), state = atomic_load_8(&mStreamMgmt.state);
  if (0 == active || MND_STM_STATE_NORMAL != state) {
    mstTrace("ignore health check since active:%d state:%d", active, state);
    return;
  }

  if (sdbGetSize(pMnode->pSdb, SDB_STREAM) <= 0) {
    mstTrace("ignore health check since no stream now");
    return;
  }
  
  mStreamMgmt.hCtx.slotIdx = (mStreamMgmt.hCtx.slotIdx + 1) % MND_STREAM_ISOLATION_PERIOD_NUM;
  mStreamMgmt.hCtx.currentTs = taosGetTimestampMs();

  mstDebug("start health check, soltIdx:%d, checkStartTs:%" PRId64, mStreamMgmt.hCtx.slotIdx, mStreamMgmt.hCtx.currentTs);

  taosWLockLatch(&mStreamMgmt.runtimeLock);
  
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


int32_t msmInitRuntimeInfo(SMnode *pMnode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t vnodeNum = sdbGetSize(pMnode->pSdb, SDB_VGROUP);
  int32_t snodeNum = sdbGetSize(pMnode->pSdb, SDB_SNODE);
  int32_t dnodeNum = sdbGetSize(pMnode->pSdb, SDB_DNODE);

  MND_STREAM_SET_LAST_TS(STM_OP_ACTIVE_BEGIN, taosGetTimestampMs());

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
      pCtx->actionStm[m] = taosHashInit(snodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
      if (pCtx->actionStm[m] == NULL) {
        code = terrno;
        mError("failed to initialize the stream runtime actionStm[%d][%d], error:%s", i, m, tstrerror(code));
        goto _exit;
      }
    }
  }
  
  mStreamMgmt.streamMap = taosHashInit(MND_STREAM_DEFAULT_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.streamMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime streamMap, error:%s", tstrerror(code));
    goto _exit;
  }
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
  mStreamMgmt.snodeMap = taosHashInit(snodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.snodeMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime snodeMap, error:%s", tstrerror(code));
    goto _exit;
  }
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
  mStreamMgmt.toDeploySnodeMap = taosHashInit(snodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.toDeploySnodeMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime toDeploySnodeMap, error:%s", tstrerror(code));
    goto _exit;
  }

  mStreamMgmt.toUpdateScanMap = taosHashInit(snodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (mStreamMgmt.toUpdateScanMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime toUpdateScanMap, error:%s", tstrerror(code));
    goto _exit;
  }

  TAOS_CHECK_EXIT(msmSTAddSnodesToMap(pMnode));
  TAOS_CHECK_EXIT(msmSTAddDnodesToMap(pMnode));

  mStreamMgmt.activeStreamNum = 0;
  
  sdbTraverse(pMnode->pSdb, SDB_STREAM, msmUpdateProfileStreams, &MND_STREAM_GET_LAST_TS(STM_OP_ACTIVE_BEGIN), &mStreamMgmt.activeStreamNum, NULL);

  mStreamMgmt.lastTaskId = 1;

  if (mStreamMgmt.activeStreamNum > 0) {
    msmSetInitRuntimeState(MND_STM_STATE_WATCH);
  } else {
    msmSetInitRuntimeState(MND_STM_STATE_NORMAL);
  }

  //taosHashSetFreeFp(mStreamMgmt.nodeMap, freeTaskList);
  //taosHashSetFreeFp(mStreamMgmt.streamMap, freeTaskList);


_exit:

  if (code) {
    msmDestroyRuntimeInfo(pMnode);
  }

  return code;
}



