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
      break;
    case MND_STM_STATE_RESTART:
      break;
    default:
      return;
  }
  
  atomic_store_8(&mStreamMgmt.state, state);
}


static int32_t msmSTAddSnodesToMap(SMnode* pMnode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmSnodeTasksStatus tasks = {0};
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



static int32_t msmSTAddToTaskMap(SStmGrpCtx* pCtx, SStreamObj* pStream, SArray* pTasks, SStmTaskStatus* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t taskNum = pTasks ? taosArrayGetSize(pTasks) : 1;
  int64_t key[2] = {pStream->pCreate->streamId, 0};
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskStatus* pStatus = pTasks ? taosArrayGet(pTasks, i) : pTask;
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

}

static int32_t msmSTAddToVgroupMapImpl(SHashObj* pVgMap, SStmTaskStatus* pStatus, int64_t streamId, bool trigReader) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmVgroupStatus vg = {0};

  SStmVgroupStatus* pVg = taosHashGet(pVgMap, &pStatus->id.nodeId, sizeof(pStatus->id.nodeId));
  if (NULL == pVg) {
    vg.streamTasks = taosHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    taosHashSetFreeFp(vg.streamTasks, mstDestroySStmVgStreamStatus);
    TSDB_CHECK_NULL(vg.streamTasks, code, lino, _return, terrno);
    
    TAOS_CHECK_EXIT(msmSTAddToVgStreamHash(vg.streamTasks, streamId, pStatus, trigReader));
    TAOS_CHECK_EXIT(taosHashPut(pVgMap, &pStatus->id.nodeId, sizeof(pStatus->id.nodeId), &vg, sizeof(vg)));
  }

  TAOS_CHECK_EXIT(msmSTAddToVgStreamHash(pVg->streamTasks, streamId, pStatus, trigReader));
  
_return:

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


static int32_t msmSTAddToSnodeMapImpl(SHashObj* pHash, SStmTaskStatus* pStatus, SStreamObj* pStream, bool triggerTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStmSnodeTasksStatus snode = {0};
  SStmTaskStatusExt ext = {streamId, pStatus};
  SArray** ppList = NULL;

  while (true) {
    SStmSnodeTasksStatus* pSnode = taosHashAcquire(pHash, &pStatus->id.nodeId, sizeof(pStatus->id.nodeId));
    if (NULL == pSnode) {
      if (triggerTask) {
        snode.triggerList = taosArrayInit(20, sizeof(SStmTaskStatusExt));
        TSDB_CHECK_NULL(snode.triggerList, code, lino, _return, terrno);
        TSDB_CHECK_NULL(taosArrayPush(snode.triggerList, &ext), code, lino, _return, terrno);
        ppList = &snode.triggerList;
      } else {
        snode.runnerList = taosArrayInit(20, sizeof(SStmTaskStatusExt));
        TSDB_CHECK_NULL(snode.runnerList, code, lino, _return, terrno);
        TSDB_CHECK_NULL(taosArrayPush(snode.runnerList, &ext), code, lino, _return, terrno);
        ppList = &snode.runnerList;
      }
      
      code = taosHashPut(pHash, &pStatus->id.nodeId, sizeof(pStatus->id.nodeId), &snode, sizeof(snode));
      if (TSDB_CODE_SUCCESS == code) {
        goto _return;
      }

      if (TSDB_CODE_DUP_KEY != code) {
        goto _return;
      }    

      taosArrayDestroy(triggerTask ? snode.triggerList : snode.runnerList);
      continue;
    }

    //taosWLockLatch(&pSnode->lock);
    ppList = triggerTask ? &pSnode->triggerList : &pSnode->runnerList;
    if (NULL == *ppList) {
      *ppList = taosArrayInit(20, sizeof(SStmTaskStatusExt));
      if (NULL == *ppList) {
        code = terrno;
        taosWUnLockLatch(&pSnode->lock);        
        TSDB_CHECK_NULL(*ppList, code, lino, _return, code);
      }
    }
    if (NULL == taosArrayPush(*ppList, &ext)) {
      code = terrno;
      taosWUnLockLatch(&pSnode->lock);
      TSDB_CHECK_NULL(NULL, code, lino, _return, code);
    }
    //taosWUnLockLatch(&pSnode->lock);
    
    taosHashRelease(pHash, pSnode);
    break;
  }
  
_return:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    mstsDebug("%s task %" PRId64 " tidx %d added to snodeMap, snodeId:%d, taskNum:%d", triggerTask ? "trigger" : "runner", 
        pStatus->id.taskId, pStatus->id.taskIdx, pStatus->id.nodeId, (int32_t)taosArrayGetSize(*ppList));
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

void msmGetReaderFromTriggerNum(SStmStatus* pInfo, int32_t* triggerReaderNum) {
  *triggerReaderNum = 0;

  int32_t readerNum = taosArrayGetSize(pInfo->readerList);
  if (NULL == pInfo->readerList || readerNum <= 0) {
    return;
  }

  for (int32_t i = 0; i < readerNum; ++i) {
    SStmTaskStatus* pStatus = taosArrayGet(pInfo->readerList, i);
    if (STREAM_IS_TRIGGER_READER(pStatus->flags)) {
      (*triggerReaderNum)++;
      continue;
    }

    break;
  }
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

  SStreamTaskAddr addr;
  int32_t triggerReaderNum = 0;
  msmGetReaderFromTriggerNum(pInfo, &triggerReaderNum);
  if (triggerReaderNum > 0) {
    pMsg->readerList = taosArrayInit(triggerReaderNum, sizeof(SStreamTaskAddr));
    TSDB_CHECK_NULL(pMsg->readerList, code, lino, _exit, terrno);
  }
  
  for (int32_t i = 0; i < triggerReaderNum; ++i) {
    SStmTaskStatus* pStatus = taosArrayGet(pInfo->readerList, i);
    addr.taskId = pStatus->id.taskId;
    addr.nodeId = pStatus->id.nodeId;
    addr.epset = mndGetVgroupEpsetById(pMnode, pStatus->id.nodeId);
    TSDB_CHECK_NULL(taosArrayPush(pMsg->readerList, &addr), code, lino, _exit, terrno);
  }

  if (NULL == pInfo->runnerTopIdx) {
    mstsDebug("no runner topIdx, skip set trigger's runner list, num:%"PRIu64, (int64_t)taosArrayGetSize(pInfo->runnerList));
    return code;
  }

  if (pInfo->runnerDeploys > 0) {
    pMsg->runnerList = taosArrayInit(pInfo->runnerDeploys, sizeof(SStreamTaskAddr));
    TSDB_CHECK_NULL(pMsg->runnerList, code, lino, _exit, terrno);
  }
  
  for (int32_t i = 0; i < pInfo->runnerDeploys; ++i) {
    int32_t* idx = taosArrayGet(pInfo->runnerTopIdx, i);
    TSDB_CHECK_NULL(idx, code, lino, _exit, terrno);
    
    SStmTaskStatus* pStatus = taosArrayGet(pInfo->runnerList, *idx);
    TSDB_CHECK_NULL(pStatus, code, lino, _exit, terrno);
    
    SStreamRunnerTarget runner;
    runner.addr.taskId = pStatus->id.taskId;
    runner.addr.epset = mndGetDnodeEpsetById(pMnode, pStatus->id.nodeId);
    runner.execReplica = pInfo->runnerReplica; 
    TSDB_CHECK_NULL(taosArrayPush(pMsg->runnerList, &runner), code, lino, _exit, terrno);
  }

  pMsg->pVSubTables = pStream->pCreate->pVSubTables;

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


static int32_t msmSTAddToVgroupMap(SStmGrpCtx* pCtx, SStreamObj* pStream, SArray* pTasks, bool trigReader) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t taskNum = taosArrayGetSize(pTasks);
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskStatus* pStatus = taosArrayGet(pTasks, i);
    TAOS_CHECK_EXIT(msmSTAddToVgroupMapImpl(mStreamMgmt.vgroupMap, pStatus, streamId, trigReader));
  }
  
_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}


static int32_t msmSTAddToSnodeMap(SStmGrpCtx* pCtx, SStreamObj* pStream, SArray* pTasks, SStmTaskStatus* pTask, int32_t taskNum, bool triggerTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t rtaskNum = (taskNum > 0) ? taskNum : taosArrayGetSize(pTasks);
  int32_t taskType = triggerTask ? STREAM_TRIGGER_TASK : STREAM_RUNNER_TASK;
  
  for (int32_t i = 0; i < rtaskNum; ++i) {
    SStmTaskStatus* pStatus = (taskNum > 0) ? (pTask + i) : taosArrayGet(pTasks, i);
    TSDB_CHECK_CODE(msmSTAddToSnodeMapImpl(mStreamMgmt.snodeMap, pStatus, pStream, triggerTask), lino, _return);
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
  uint64_t uuid = 0;
  if (taosGetSystemUUIDU64(&uuid)) {
    return taosGetTimestampNs();
  }

  return (int64_t)uuid;
}


int32_t msmIsSnodeAlive(SMnode* pMnode, int32_t snodeId, int64_t streamId, bool* alive) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  bool     noExists = false;
  SStmSnodeTasksStatus* pStatus = NULL;

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
    terrno = TSDB_CODE_SNODE_NOT_DEPLOYED;
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

  if (0 == snodeId) {
    terrno = TSDB_CODE_SNODE_NO_ALIVE_NODE;
  }

  return snodeId;
}

int32_t msmAssignTaskSnodeId(SMnode* pMnode, SStreamObj* pStream, bool isStatic) {
  int64_t streamId = pStream->pCreate->streamId;
  int32_t snodeNum = sdbGetSize(pMnode->pSdb, SDB_SNODE);
  if (snodeNum <= 0) {
    mstsInfo("no available snode now, num:%d", snodeNum);
    return 0;
  }

  if (isStatic) {
    return msmRetrieveStaticSnodeId(pMnode, pStream);
  }

  return msmAssignRandomSnodeId(pMnode, streamId);
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

  TSDB_CHECK_CODE(msmSTAddToTaskMap(pCtx, pStream, NULL, pInfo->triggerTask), lino, _return);
  TSDB_CHECK_CODE(msmSTAddToSnodeMap(pCtx, pStream, NULL, pInfo->triggerTask, 1, true), lino, _return);

_return:

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
    case TSDB_CHILD_TABLE: {
      pInfo->trigReaders = taosArrayInit_s(sizeof(SStmTaskStatus), 1);
      TSDB_CHECK_NULL(pInfo->trigReaders, code, lino, _exit, terrno);
      pState = taosArrayGet(pInfo->trigReaders, 0);
      
      pState->id.taskId = msmAssignTaskId();
      pState->id.deployId = 0;
      pState->id.seriousId = msmAssignTaskSeriousId();
      pState->id.nodeId = pStream->pCreate->triggerTblVgId;
      pState->id.taskIdx = 0;
      pState->type = STREAM_READER_TASK;
      pState->flags = STREAM_FLAG_TRIGGER_READER;
      pState->status = STREAM_STATUS_NA;
      pState->lastUpTs = pCtx->currTs;

      SStmTaskDeploy info = {0};
      info.task.type = pState->type;
      info.task.streamId = streamId;
      info.task.taskId = pState->id.taskId;
      info.task.flags = pState->flags;
      info.task.seriousId = pState->id.seriousId;
      info.task.nodeId = pState->id.nodeId;
      info.task.taskIdx = pState->id.taskIdx;
      TSDB_CHECK_CODE(msmBuildReaderDeployInfo(&info, pStream, NULL, pInfo, true), lino, _return);
      TSDB_CHECK_CODE(msmTDAddToVgroupMap(mStreamMgmt.toDeployVgMap, &info, streamId), lino, _return);
      break;
    }
    case TSDB_SUPER_TABLE: {
      SDbObj* pDb = mndAcquireDb(pCtx->pMnode, pStream->pCreate->triggerDB);
      if (NULL == pDb) {
        code = terrno;
        mstsError("failed to acquire db %s, error:%s", pStream->pCreate->triggerDB, terrstr());
        goto _return;
      }

      pInfo->trigReaders = taosArrayInit(pDb->cfg.numOfVgroups, sizeof(SStmTaskStatus));
      TSDB_CHECK_NULL(pInfo->trigReaders, code, lino, _exit, terrno);
      
      void *pIter = NULL;
      while (1) {
        SVgObj *pVgroup = NULL;
        pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
        if (pIter == NULL) {
          break;
        }
      
        if (pVgroup->dbUid == pDb->uid && !pVgroup->isTsma) {
          pState = taosArrayReserve(pInfo->trigReaders, 1);
          
          pState->id.taskId = msmAssignTaskId();
          pState->id.deployId = 0;
          pState->id.seriousId = msmAssignTaskSeriousId();
          pState->id.nodeId = pVgroup->vgId;
          pState->id.taskIdx = 0;
          pState->type = STREAM_READER_TASK;
          pState->flags = STREAM_FLAG_TRIGGER_READER;
          pState->status = STREAM_STATUS_NA;
          pState->lastUpTs = pCtx->currTs;

          SStmTaskDeploy info = {0};
          info.task.type = pState->type;
          info.task.streamId = streamId;
          info.task.taskId = pState->id.taskId;
          info.task.flags = pState->flags;
          info.task.seriousId = pState->id.seriousId;
          info.task.nodeId = pState->id.nodeId;
          info.task.taskIdx = pState->id.taskIdx;
          TSDB_CHECK_CODE(msmBuildReaderDeployInfo(&info, pStream, NULL, pInfo, true), lino, _return);
          TSDB_CHECK_CODE(msmTDAddToVgroupMap(mStreamMgmt.toDeployVgMap, &info, streamId), lino, _return);
        }
      
        sdbRelease(pSdb, pVgroup);
      }
      break;
    }
    case TSDB_VIRTUAL_CHILD_TABLE:
    case TSDB_VIRTUAL_NORMAL_TABLE: {
      //STREAMTODO
      break;
    }
    default:
      mstsDebug("%s ignore triggerTblType %d", __FUNCTION__, pStream->pCreate->triggerTblType);
      break;
  }

_return:

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
      TSDB_CHECK_CODE(msmUPAddCacheTask(pCtx, pScan, pStream), lino, _return);
      continue;
    }
    
    int32_t vgNum = taosArrayGetSize(pScan->vgList);
    for (int32_t m = 0; m < vgNum; ++m) {
      pState = taosArrayReserve(pInfo->trigReaders, 1);

      pState->id.taskId = msmAssignTaskId();
      pState->id.deployId = 0;
      pState->id.seriousId = msmAssignTaskSeriousId();
      pState->id.nodeId = *(int32_t*)taosArrayGet(pScan->vgList, m);
      pState->id.taskIdx = i;
      pState->type = STREAM_READER_TASK;
      pState->flags = STREAM_FLAG_TRIGGER_READER;
      pState->status = STREAM_STATUS_NA;
      pState->lastUpTs = pCtx->currTs;

      SStmTaskDeploy info = {0};
      info.task.type = pState->type;
      info.task.streamId = streamId;
      info.task.taskId = pState->id.taskId;
      info.task.seriousId = pState->id.seriousId;
      info.task.nodeId = pState->id.nodeId;
      info.task.taskIdx = pState->id.taskIdx;
      TSDB_CHECK_CODE(msmBuildReaderDeployInfo(&info, pStream, pScan->scanPlan, pInfo, false), lino, _return);
      TSDB_CHECK_CODE(msmUPAddScanTask(pCtx, pStream, pScan->scanPlan, pState->id.nodeId, pState->id.taskId), lino, _return);
      TSDB_CHECK_CODE(msmTDAddToVgroupMap(mStreamMgmt.toDeployVgMap, &info, streamId), lino, _return);
    }
  }

_return:

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

  TSDB_CHECK_CODE(msmSTAddToTaskMap(pCtx, pStream, pInfo->trigReaders, NULL), lino, _exit);
  TSDB_CHECK_CODE(msmSTAddToTaskMap(pCtx, pStream, pInfo->calcReaders, NULL), lino, _exit);
  
  TSDB_CHECK_CODE(msmSTAddToVgroupMap(pCtx, pStream, pInfo->trigReaders, true), lino, _exit);
  TSDB_CHECK_CODE(msmSTAddToVgroupMap(pCtx, pStream, pInfo->calcReaders, false), lino, _exit);
  
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
  SStmTaskStatus state = {{INT64_MIN, -1, 1, INT32_MIN, 0}, STREAM_RUNNER_TASK, 0, 0, pCtx->currTs};

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

  for (int32_t r = 0; r < pInfo->runnerDeploys; ++r) {
    totalTaskNum = 0;
    state.id.nodeId = msmAssignTaskSnodeId(pCtx->pMnode, pStream, (0 == r) ? true : false);
    state.id.deployId = r;

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

      state.flags = 0;

      for (int32_t n = 0; n < taskNum; ++n) {
        SSubplan *plan = (SSubplan *)nodesListGetNode(plans->pNodeList, n);

        state.id.taskId = msmAssignTaskId();
        state.id.taskIdx = MND_SET_RUNNER_TASKIDX(i, n);
        state.type = STREAM_RUNNER_TASK;        
        state.id.seriousId = msmAssignTaskSeriousId();
        TSDB_CHECK_NULL(taosArrayPush(pInfo->runnerList, &state), code, lino, _exit, terrno);
        
        if (0 == i) {
          int32_t idx = taosArrayGetSize(pInfo->runnerList) - 1;
          TSDB_CHECK_NULL(taosArrayPush(pInfo->runnerTopIdx, &idx), code, lino, _exit, terrno);
          state.flags = STREAM_FLAG_TOP_RUNNER;
        }

        SStmTaskDeploy info = {0};
        info.task.type = state.type;
        info.task.streamId = streamId;
        info.task.taskId = state.id.taskId;
        info.task.flags = state.flags;
        info.task.seriousId = state.id.seriousId;
        info.task.deployId = state.id.deployId;
        info.task.nodeId = state.id.nodeId;
        info.task.taskIdx = state.id.taskIdx;
        TSDB_CHECK_CODE(msmBuildRunnerDeployInfo(&info, plan, pStream, pInfo->runnerReplica, 0 == i), lino, _exit);

        TSDB_CHECK_CODE(msmTDAddSnodeTask(mStreamMgmt.toDeploySnodeMap, &info, pStream, false, i == lowestLevelIdx), lino, _exit);
        
        atomic_add_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);        
      }

      mstsDebug("level %d initialized, taskNum:%d", i, taskNum);
    }

    if (totalTaskNum != pDag->numOfSubplans) {
      mstsError("totalTaskNum %d mis-match with numOfSubplans %d", totalTaskNum, pDag->numOfSubplans);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }

    SStmSnodeTasksDeploy* pSnodeDeploy = taosHashGet(mStreamMgmt.toDeploySnodeMap, &state.id.nodeId, sizeof(state.id.nodeId));
    TSDB_CHECK_NULL(pSnodeDeploy, code, lino, _exit, terrno);
    
    TAOS_CHECK_EXIT(msmUpdateRunnerPlans(pCtx, pSnodeDeploy->runnerList, pStream, totalTaskNum));

    TAOS_CHECK_EXIT(nodesStringToNode(pStream->pCreate->calcPlan, (SNode**)&pDag));
  }

  TSDB_CHECK_CODE(msmSTAddToTaskMap(pCtx, pStream, pInfo->runnerList, NULL), lino, _exit);
  TSDB_CHECK_CODE(msmSTAddToSnodeMap(pCtx, pStream, pInfo->runnerList, NULL, 0, false), lino, _exit);

  pInfo->runnerNum = totalTaskNum;
  
_exit:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmSearchGetStreamRunnerTask(int64_t streamId, SStmStatus* pInfo, int32_t deployId, SStmTaskStatus** ppRunner) {
  int32_t taskNum = taosArrayGetSize(pInfo->runnerList);
  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskStatus* pTask = taosArrayGet(pInfo->runnerList, i);
    if (pTask->id.deployId != deployId) {
      continue;
    }

    *ppRunner = pTask;
    return TSDB_CODE_SUCCESS;
  }

  mstsError("deployId %d not found in runner list, runnerNum:%d", deployId, taskNum);

  return TSDB_CODE_STREAM_INTERNAL_ERROR;
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

    TAOS_CHECK_EXIT(msmSearchGetStreamRunnerTask(streamId, pInfo, deployId, &pRunner));

    pStartRunner = pRunner;
    totalTaskNum = 0;

    newNodeId = msmAssignTaskSnodeId(pCtx->pMnode, pStream, (0 == r) ? true : false);

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
    TAOS_CHECK_EXIT(msmSTAddToSnodeMap(pCtx, pStream, NULL, pStartRunner, num, false));

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

  pInfo->runnerTopIdx = taosArrayInit(pInfo->runnerDeploys, sizeof(int32_t));
  TSDB_CHECK_NULL(pInfo->runnerTopIdx, code, lino, _exit, terrno);

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

  pInfo->runnerList = taosArrayInit(pPlan->numOfSubplans, sizeof(SStmTaskStatus));
  TSDB_CHECK_NULL(pInfo->runnerList, code, lino, _exit, terrno);

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

  TSDB_CHECK_CODE(msmBuildReaderTasks(pCtx, pInfo, pStream), lino, _return);
  TSDB_CHECK_CODE(msmBuildRunnerTasks(pCtx, pInfo, pStream), lino, _return);
  TSDB_CHECK_CODE(msmBuildTriggerTasks(pCtx, pInfo, pStream), lino, _return);

  pInfo->allTaskBuilt = true;
  
_return:

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmInitStmStatus(SStmStatus* pStatus, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;

  pStatus->lastActTs = INT64_MIN;
  
  pStatus->streamName = taosStrdup(pStream->name);
  TSDB_CHECK_NULL(pStatus->streamName, code, lino, _exit, terrno);

  TAOS_CHECK_EXIT(msmSetStreamRunnerExecReplica(streamId, pStatus));

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

  TAOS_CHECK_EXIT(msmInitStmStatus(&info, pStream));

  TAOS_CHECK_EXIT(taosHashPut(mStreamMgmt.streamMap, &streamId, sizeof(streamId), &info, sizeof(info)));
  
  TAOS_CHECK_EXIT(msmBuildStreamTasks(pCtx, &info, pStream));

  SStmStatus* pStatus = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));

  memcpy(pStatus, &info, sizeof(info));

  mstsDebug("stream added to streamMap, readerNum:%d, triggerNum:%d, runnerNum:%d", 
      (int32_t)taosArrayGetSize(info.readerList), info.triggerTask ? 1 : 0, (int32_t)taosArrayGetSize(info.runnerList));

_exit:

  if (code) {
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
      TAOS_CHECK_EXIT(msmSTAddToSnodeMap(pCtx, pStream, NULL, *ppTask, 1, true));
      
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
        TAOS_CHECK_EXIT(msmSTAddToSnodeMap(pCtx, pStream, NULL, pAction->triggerStatus, 1, true));
        
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
    SStmSnodeTasksStatus* pSnode = (SStmSnodeTasksStatus*)pIter;
    int32_t taskNum = taosArrayGetSize(pSnode->triggerList);
    for (int32_t i = taskNum - 1; i >= 0; --i) {
      SStmTaskStatusExt* pExt = taosArrayGet(pSnode->triggerList, i);
      if (pExt->streamId == streamId) {
        taosArrayRemove(pSnode->triggerList, i);
        mstsDebug("trigger TASK:%" PRId64 " removed from snodeMap, remainTrigger:%d", pExt->status->id.taskId, (int32_t)taosArrayGetSize(pSnode->triggerList));
      }
    }

    taskNum = taosArrayGetSize(pSnode->runnerList);
    for (int32_t i = taskNum - 1; i >= 0; --i) {
      SStmTaskStatusExt* pExt = taosArrayGet(pSnode->runnerList, i);
      if (pExt->streamId == streamId) {
        taosArrayRemove(pSnode->runnerList, i);
        mstsDebug("runner TASK:%" PRId64 " removed from snodeMap, remainRunner:%d", pExt->status->id.taskId, (int32_t)taosArrayGetSize(pSnode->runnerList));
      }
    }
  }

  while ((pIter = taosHashIterate(mStreamMgmt.vgroupMap, pIter))) {
    SStmVgroupStatus* pVg = (SStmVgroupStatus*)pIter;
    int32_t taskNum = taosArrayGetSize(pVg->taskList);
    for (int32_t i = taskNum - 1; i >= 0; --i) {
      SStmTaskStatusExt* pExt = taosArrayGet(pVg->taskList, i);
      if (pExt->streamId == streamId) {
        mstsDebug("TASK:%" PRId64 " removed from vgroupMap", pExt->status->id.taskId);
        //taosWLockLatch(&pVg->lock);
        taosArrayRemove(pVg->taskList, i);
        //taosWUnLockLatch(&pVg->lock);
      }
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

  //TAOS_CHECK_EXIT(msmRemoveStreamFromActionQ(streamId));
  TAOS_CHECK_EXIT(msmTDRemoveStream(streamId));
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

  taosWLockLatch(&mStreamMgmt.runtimeLock);

  SStmStatus** ppStream = (SStmStatus**)taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == ppStream) {
    mstsInfo("stream %s already not in streamMap", streamName);
    goto _exit;
  }

  TAOS_CHECK_EXIT(msmRemoveStreamFromMaps(pMnode, streamId));

_exit:

  taosWUnLockLatch(&mStreamMgmt.runtimeLock);

  if (code) {
    mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmHandleStreamActions(SStmGrpCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmQNode* pQNode = NULL;

  mstDebug("start to handle stream actions");
  
  while (mndStreamActionDequeue(mStreamMgmt.actionQ, &pQNode)) {
    switch (pQNode->type) {
      case STREAM_ACT_DEPLOY:
        if (pQNode->streamAct) {
          TAOS_CHECK_EXIT(msmLaunchStreamDepolyAction(pCtx, &pQNode->action.stream));
        } else {
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
    mstsError("stream not exists in streamMap, mapSize:%d", taosHashGetSize(mStreamMgmt.streamMap));
    TSDB_CHECK_NULL(pStatus, code, lino, _exit, TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  }
  
  pStatus->lastActTs = currTs;

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

int32_t msmGrpAddActionStart(SHashObj* pHash, int64_t streamId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t action = STREAM_ACT_START;
  SStmAction *pAction = taosHashGet(pHash, &streamId, sizeof(streamId));
  if (pAction) {
    pAction->actions |= action;
    mstsDebug("stream append START action, actions:%x", pAction->actions);
  } else {
    SStmAction newAction = {0};
    newAction.actions = action;
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


int32_t msmHandleTaskAbnormalStatus(SStmGrpCtx* pCtx, SStmTaskStatusMsg* pMsg) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t action = 0;
  int64_t streamId = pMsg->streamId;
  SStreamTask* pTask = (SStreamTask*)pMsg;

  msttDebug("start to handle task abnormal status %d", pTask->status);
  
  SStmStatus* pStatus = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStatus) {
    msttError("stream no longer exists in streamMap, try to undeploy it, idx:%d", pMsg->taskIdx);
    return msmGrpAddActionUndeploy(pCtx, streamId, pTask);
  }
  
  switch (pMsg->status) {
    case STREAM_STATUS_INIT:
      if (INT64_MIN == pStatus->lastActTs) {
        msttDebug("task still not deployed, ignore it, currTs:%" PRId64 ", lastTs:%" PRId64, pCtx->currTs, pStatus->lastActTs);
        return code;
      }
      
      if ((pCtx->currTs - pStatus->lastActTs) < STREAM_ACT_MIN_DELAY_MSEC) {
        msttDebug("task wait not enough between actions, currTs:%" PRId64 ", lastTs:%" PRId64, pCtx->currTs, pStatus->lastActTs);
        return code;
      }

      if (STREAM_IS_RUNNING(pStatus->triggerTask->status)) {
        msttDebug("stream already running, ignore status: %s", gStreamStatusStr[pTask->status]);
      } else {
        TAOS_CHECK_EXIT(msmGrpAddActionStart(pCtx->actionStm, streamId));
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

  if (STM_ERR_TASK_NOT_EXISTS == err) {
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
      msttInfo("task no longer exists in taskMap, will try to rm it, taskIdx:%d", pTask->taskIdx);
      
      msmHandleStatusUpdateErr(pCtx, STM_ERR_TASK_NOT_EXISTS, pTask);
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

  SStmStatus* pStatus = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStatus) {
    SStmStatus status = {0};
    status.lastActTs = INT64_MIN;
    TAOS_CHECK_EXIT(msmSetStreamRunnerExecReplica(streamId, &status));
    TAOS_CHECK_EXIT(taosHashPut(mStreamMgmt.streamMap, &streamId, sizeof(streamId), &status, sizeof(status)));
    pStatus = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
    TSDB_CHECK_NULL(pStatus, code, lino, _exit, terrno);
    msttDebug("stream added to streamMap cause of new task status:%s", gStreamStatusStr[pTask->status]);
  }

  SStmTaskStatus* pNewTask = NULL;
  switch (pTask->type) {
    case STREAM_READER_TASK: {
      if (NULL == pStatus->readerList) {
        pStatus->readerList = taosArrayInit(2, sizeof(SStmTaskStatus));
        TSDB_CHECK_NULL(pStatus->readerList, code, lino, _exit, terrno);
      }
      SStmTaskStatus taskStatus;
      mstSetTaskStatusFromMsg(pCtx, &taskStatus, pTask);
      pNewTask = taosArrayPush(pStatus->readerList, &taskStatus);
      TSDB_CHECK_NULL(pNewTask, code, lino, _exit, terrno);
      break;
    }
    case STREAM_TRIGGER_TASK: {
      taosMemoryFreeClear(pStatus->triggerTask);
      pStatus->triggerTask = taosMemoryMalloc(sizeof(*pStatus->triggerTask));
      TSDB_CHECK_NULL(pStatus->triggerTask, code, lino, _exit, terrno);
      mstSetTaskStatusFromMsg(pCtx, pStatus->triggerTask, pTask);
      pNewTask = pStatus->triggerTask;
      break;
    }
    case STREAM_RUNNER_TASK:{
      if (NULL == pStatus->runnerList) {
        pStatus->runnerList = taosArrayInit(2, sizeof(SStmTaskStatus));
        TSDB_CHECK_NULL(pStatus->runnerList, code, lino, _exit, terrno);
      }
      SStmTaskStatus taskStatus;
      mstSetTaskStatusFromMsg(pCtx, &taskStatus, pTask);
      pNewTask = taosArrayPush(pStatus->runnerList, &taskStatus);
      TSDB_CHECK_NULL(pNewTask, code, lino, _exit, terrno);
      pStatus->runnerNum++;
      break;
    }
    default: {
      msttError("invalid task type:%d in task status", pTask->type);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      break;
    }
  }

  TAOS_CHECK_EXIT(taosHashPut(mStreamMgmt.taskMap, &pTask->streamId, sizeof(pTask->streamId) + sizeof(pTask->taskId), &pNewTask, POINTER_BYTES));

_exit:

  if (code) {
    msttError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    msttDebug("task added to taskMap cause of new task status:%s", gStreamStatusStr[pTask->status]);
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


bool msmCheckStreamStartCond(int64_t streamId, int32_t snodeId, SStmTaskId* id) {
  SStmStatus* pStream = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStream) {
    return false;
  }

  if (pStream->triggerTask->id.nodeId != snodeId || STREAM_STATUS_INIT != pStream->triggerTask->status) {
    return false;
  }

  int32_t readerNum = taosArrayGetSize(pStream->readerList);
  for (int32_t i = 0; i < readerNum; ++i) {
    SStmTaskStatus* pStatus = taosArrayGet(pStream->readerList, i);
    if (STREAM_STATUS_INIT != pStatus->status && STREAM_STATUS_RUNNING != pStatus->status) {
      return false;
    }
  }

  int32_t runnerNum = taosArrayGetSize(pStream->runnerList);
  for (int32_t i = 0; i < runnerNum; ++i) {
    SStmTaskStatus* pStatus = taosArrayGet(pStream->runnerList, i);
    if (STREAM_STATUS_INIT != pStatus->status && STREAM_STATUS_RUNNING != pStatus->status) {
      return false;
    }
  }

  *id = pStream->triggerTask->id;
  
  return true;
}

void msmRspAddStreamStart(int64_t streamId, SMStreamHbRspMsg* pRsp, int32_t streamNum, SStmAction *pAction, SStmTaskId* id) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (NULL == pRsp->start.taskList) {
    pRsp->start.taskList = taosArrayInit(streamNum, sizeof(SStreamTaskStart));
    TSDB_CHECK_NULL(pRsp->start.taskList, code, lino, _exit, terrno);
  }

  SStreamTaskStart start;
  start.task.type = STREAM_TRIGGER_TASK;
  start.task.streamId = streamId;
  start.task.taskId = id->taskId;
  start.task.seriousId = id->seriousId;
  start.task.nodeId = id->nodeId;
  start.task.taskIdx = id->taskIdx;

  TSDB_CHECK_NULL(taosArrayPush(pRsp->start.taskList, &start), code, lino, _exit, terrno);

  mstsDebug("stream START added to dnode %d hb rsp, triggerTaskId:%"PRId64, id->nodeId, id->taskId);

  return;

_exit:

  mstsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
}


void msmRspAddStreamUndeploy(int64_t streamId, SMStreamHbRspMsg* pRsp, SStmAction *pAction) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t dropNum = taosArrayGetSize(pAction->undeploy.taskList);
  if (NULL == pRsp->undeploy.taskList) {
    pRsp->undeploy.taskList = taosArrayInit(dropNum, sizeof(SStreamTaskUndeploy));
    TSDB_CHECK_NULL(pRsp->undeploy.taskList, code, lino, _exit, terrno);
  }

  SStreamTaskUndeploy undeploy;
  for (int32_t i = 0; i < dropNum; ++i) {
    SStreamTask* pTask = (SStreamTask*)taosArrayGetP(pAction->undeploy.taskList, i);
    undeploy.task = *pTask;
    undeploy.undeployMsg.doCheckpoint = pAction->undeploy.doCheckpoint;
    undeploy.undeployMsg.doCleanup = pAction->undeploy.doCleanup;

    TSDB_CHECK_NULL(taosArrayPush(pRsp->undeploy.taskList, &undeploy), code, lino, _exit, terrno);

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
    SStmTaskId id = {0};
    
    if (STREAM_ACT_UNDEPLOY & pAction->actions) {
      msmRspAddStreamUndeploy(*pStreamId, pCtx->pRsp, pAction);
    }

    if ((STREAM_ACT_START & pAction->actions) && GOT_SNODE(pCtx->pReq->snodeId) && msmCheckStreamStartCond(*pStreamId, pCtx->pReq->snodeId, &id)) {
      msmRspAddStreamStart(*pStreamId, pCtx->pRsp, streamNum, pAction, &id);
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
        mstError("Got unknown dnode %d hb msg", pCtx->pReq->dnodeId);
        TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
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


int32_t msmCheckUpdateSnodeTs(SStmGrpCtx* pCtx) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  SStmSnodeTasksStatus* pStatus = NULL;
  bool     noExists = false;

  while (true) {
    pStatus = taosHashGet(mStreamMgmt.snodeMap, &pCtx->pReq->snodeId, sizeof(pCtx->pReq->snodeId));
    if (NULL == pStatus) {
      if (noExists) {
        mstError("snode %d not exists in snodeMap", pCtx->pReq->snodeId);
        TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
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

int32_t msmWatchRebuildMaps(SStmGrpCtx* pCtx) {
  void* pIter = NULL;
  while (true) {
    pIter = taosHashIterate(mStreamMgmt.streamMap, pIter);
    SStmStatus* pStatus = (SStmStatus*)pIter;
    // 排序重建 pStatus
  }
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

  TAOS_CHECK_EXIT(msmWatchRebuildMaps(pCtx));

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
    
    msmSetInitRuntimeState(MND_STM_STATE_RESTART);
  } else {
    msmSetInitRuntimeState(MND_STM_STATE_NORMAL);
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

  if (((pCtx->currTs - mStreamMgmt.activeBeginTs) > MND_STREAM_WATCH_DURATION) &&
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


int32_t msmRestartHandleHbMsg(SStmGrpCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TAOS_CHECK_EXIT(msmCheckUpdateDnodeTs(pCtx));
  
  if (taosArrayGetSize(pCtx->pReq->pStreamStatus) > 0) {
    pCtx->pRsp->undeploy.undeployAll = 1;
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
    case MND_STM_STATE_RESTART:
      code = msmRestartHandleHbMsg(pCtx);
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
  if (0 == atomic_val_compare_exchange_8(&mStreamMgmt.active, 0, 1)) {
    taosWLockLatch(&mStreamMgmt.runtimeLock);
    msmInitRuntimeInfo(pMnode);
    taosWUnLockLatch(&mStreamMgmt.runtimeLock);
  }
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

void msmCheckStreamConsistency(SStmStatus* pStatus, SStreamObj* pStream) {
  if (pStatus->allTaskBuilt) {

  }
}

static bool msmCheckStreamStatus(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SStreamObj* pStream = pObj;
  SStmCheckStatusCtx* pCtx = (SStmCheckStatusCtx*)p1;
  int64_t streamId = pStream->pCreate->streamId;
  int8_t userDropped = atomic_load_8(&pStream->userDropped), userStopped = atomic_load_8(&pStream->userStopped);
  
  if (userDropped || userStopped) {
    mstsDebug("stream userDropped %d userStopped %d, ignore check it", userDropped, userStopped);
    return true;
  }

  pCtx->handledNum++;
  
  if ((!pCtx->checkAll) && (pStream->updateTime + MND_STREAM_WATCH_DURATION) > mStreamMgmt.healthCtx.currentTs) {
    mstsDebug("stream updateTime %" PRId64 " is too new, currentTs %" PRId64 ", ignore check it", pStream->updateTime, mStreamMgmt.healthCtx.currentTs);
    return true;
  }

  pCtx->checkedNum++;
  
  void* p = taosHashGet(mStreamMgmt.streamMap, &pStream->pCreate->streamId, sizeof(pStream->pCreate->streamId));
  if (p) {
    mstsDebug("stream status check OK, updateTime %" PRId64, pStream->updateTime);
    msmCheckStreamConsistency((SStmStatus*)p, pStream);
    return true;
  }

  mndStreamPostAction(mStreamMgmt.actionQ, streamId, pStream->pCreate->name, STREAM_ACT_DEPLOY);

  return true;
}

void msmCheckStreamsStatus(SMnode *pMnode) {
  SStmCheckStatusCtx ctx = {0};
  sdbTraverse(pMnode->pSdb, SDB_STREAM, msmCheckStreamStatus, &ctx, NULL, NULL);
}

void msmCheckTaskStatusInList(SArray* pList) {
  int32_t vgTaskNum = taosArrayGetSize(pList);
  for (int32_t i = 0; i < vgTaskNum; ++i) {
    SStmTaskStatusExt* pTask = taosArrayGet(pList, i);
    int64_t noUpTs = mStreamMgmt.healthCtx.currentTs - pTask->status->lastUpTs;
    if (noUpTs < MND_STREAM_WATCH_DURATION) {
      continue;
    }

    atomic_add_fetch_64(&pTask->status->id.seriousId, 1);

    int64_t streamId = pTask->streamId;
    if (STREAM_RUNNER_TASK == pTask->status->type) {
      mstsInfo("runner TASK:%" PRId64 " status not updated for %" PRId64 "ms, will try to undeploy current deployId %d", 
          pTask->status->id.taskId, noUpTs, pTask->status->id.deployId);
      // STREAMTODO
      continue;
    }
  
    mstsInfo("TASK:%" PRId64 " status not updated for %" PRId64 "ms, will try to redeploy it", 
        pTask->status->id.taskId, noUpTs);
        
    SStmTaskAction task;
    task.streamId = streamId;
    task.id = pTask->status->id;
    task.flag = pTask->status->flags;
    task.type = pTask->status->type;
    
    mndStreamPostTaskAction(mStreamMgmt.actionQ, &task, STREAM_ACT_DEPLOY);
  }
}

void msmCheckVgTasksStatus(SMnode *pMnode) {
  void* pIter = NULL;
  
  while (true) {
    pIter = taosHashIterate(mStreamMgmt.vgroupMap, pIter);
    if (NULL == pIter) {
      break;
    }

    int32_t vgId = *(int32_t*)taosHashGetKey(pIter, NULL);
    if ((vgId % MND_STREAM_ISOLATION_PERIOD_NUM) != mStreamMgmt.healthCtx.slotIdx) {
      continue;
    }
    
    SStmVgroupStatus* pVg = (SStmVgroupStatus*)pIter;

    msmCheckTaskStatusInList(pVg->taskList);
  }
}

void msmReDeploySnodeTriggerTasks(SMnode *pMnode, SArray* pList) {
  int32_t snodeNum = sdbGetSize(pMnode->pSdb, SDB_SNODE);
  int32_t taskNum = taosArrayGetSize(pList);
  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskStatusExt* pExt = taosArrayGet(pList, i);

    atomic_add_fetch_64(&pExt->status->id.seriousId, 1);

    int64_t streamId = pExt->streamId;
    if (snodeNum <= 0) {
      mstsError("trigger TASK:%" PRId64 " seriousId:%" PRId64 " will NOT be redeployed since invalid snodeNum: %d", 
          pExt->status->id.taskId, pExt->status->id.seriousId, snodeNum);
      continue;    
    }

    mstsInfo("trigger TASK:%" PRId64 " will try to redeploy since snode lost", pExt->status->id.taskId);
        
    SStmTaskAction task;
    task.streamId = streamId;
    task.id = pExt->status->id;
    task.flag = pExt->status->flags;
    task.type = pExt->status->type;
    
    mndStreamPostTaskAction(mStreamMgmt.actionQ, &task, STREAM_ACT_DEPLOY);
  }
}

void msmRemoveTriggerFromList(SArray* pTriggers, int64_t streamId, SStmTaskStatus** ppStatus) {
  int32_t taskNum = taosArrayGetSize(pTriggers);
  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskStatusExt* pExt = taosArrayGet(pTriggers, i);
    if (streamId == pExt->streamId) {
      mstsDebug("stream trigger TASK:%" PRId64 " need to be redeploy, deployId:%d, seriousId:%" PRId64 ", nodeId:%d", 
          pExt->status->id.taskId, pExt->status->id.deployId, pExt->status->id.seriousId, pExt->status->id.nodeId);

      *ppStatus = pExt->status;

      atomic_add_fetch_64(&(*ppStatus)->id.seriousId, 1);
      
      taosArrayRemove(pTriggers, i);

      return;
    }
  }

  *ppStatus = NULL;
  
  mstsDebug("stream trigger TASK not in snode list, triggerNum:%d", taskNum);

  return;
}

void msmReDeploySnodeRunnerTasks(SMnode *pMnode, SArray* pRunners, SArray* pTriggers) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = 0;
  int64_t lastStreamId = 0;
  int32_t lastDeployId = -1;
  int32_t *pNum = NULL;
  int32_t streamDeploy[MND_STREAM_RUNNER_DEPLOY_NUM + 1] = {0};
  
  if (NULL == mStreamMgmt.healthCtx.streamRunners) {
    mStreamMgmt.healthCtx.streamRunners = taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
    TSDB_CHECK_NULL(mStreamMgmt.healthCtx.streamRunners, code, lino, _exit, terrno);
  } else {
    taosHashClear(mStreamMgmt.healthCtx.streamRunners);
  }

  int32_t snodeNum = sdbGetSize(pMnode->pSdb, SDB_SNODE);
  SHashObj* pHash = mStreamMgmt.healthCtx.streamRunners;
  int32_t taskNum = taosArrayGetSize(pRunners);
  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskStatusExt* pExt = taosArrayGet(pRunners, i);

    atomic_add_fetch_64(&pExt->status->id.seriousId, 1);

    if (snodeNum <= 0) {
      mstsError("runner TASK:%" PRId64 " deployId:%d seriousId:%" PRId64 " will NOT be redeployed since invalid snodeNum: %d", 
          pExt->status->id.taskId, pExt->status->id.deployId, pExt->status->id.seriousId, snodeNum);
      continue;    
    }
    
    if (lastStreamId == pExt->streamId && lastDeployId == pExt->status->id.deployId) {
      mstsDebug("TASK:%" PRId64 " deployId:%d stream already exists in streamRunners, ignore it", pExt->status->id.taskId, pExt->status->id.deployId);
      continue;
    }
    
    streamId = pExt->streamId;
    lastStreamId = pExt->streamId;
    lastDeployId = pExt->status->id.deployId;
    
    mstsInfo("runner tasks will try to redeploy since snode lost, deployId: %d", lastDeployId);

    pNum = taosHashGet(pHash, &streamId, sizeof(streamId));
    if (NULL != pNum) {
      (*pNum)++;
      *(pNum + (*pNum)) = lastDeployId;
      
      mstsDebug("stream runner deployNum updated to %d, newDeployId: %d", *pNum, lastDeployId);
      continue;
    }

    streamDeploy[0] = 1;
    streamDeploy[1] = lastDeployId;
    
    TAOS_CHECK_EXIT(taosHashPut(pHash, &streamId, sizeof(streamId), streamDeploy, sizeof(streamDeploy)));

    mstsDebug("stream runner deployNum updated to %d, newDeployId: %d", streamDeploy[0], streamDeploy[1]);
  }


  void* pIter = NULL;
  while (true) {
    pIter = taosHashIterate(pHash, pIter);
    if (NULL == pIter) {
      break;
    }

    streamId = *(int64_t*)taosHashGetKey(pIter, NULL);
    mstsDebug("stream debug %d", 1);
    
    SStmTaskAction task;
    task.streamId = streamId;
    task.deployNum = *(int32_t*)pIter;
    for (int32_t i = 0; i < task.deployNum; ++i) {
      task.deployId[i] = *(int32_t*)((int32_t*)pIter + i + 1);
    }
    mstsDebug("stream debug %d", 2);
    
    msmRemoveTriggerFromList(pTriggers, streamId, &task.triggerStatus);
    task.type = STREAM_RUNNER_TASK;

    mstsDebug("stream debug %d", 3);
    
    mndStreamPostTaskAction(mStreamMgmt.actionQ, &task, STREAM_ACT_DEPLOY);

    mstsDebug("runner tasks %d redeploys added to actionQ", task.deployNum);
  }

  taosArrayClear(pRunners);
  
_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
}

void msmHandleSnodeLost(SMnode *pMnode, SStmSnodeTasksStatus* pSnode) {
  pSnode->runnerThreadNum = -1;

  (void)msmSTAddSnodesToMap(pMnode);

  if (pSnode->runnerList && taosArrayGetSize(pSnode->runnerList) > 0) {
    msmReDeploySnodeRunnerTasks(pMnode, pSnode->runnerList, pSnode->triggerList);
    taosArrayClear(pSnode->runnerList);
  }  

  if (pSnode->triggerList && taosArrayGetSize(pSnode->triggerList) > 0) {
    msmReDeploySnodeTriggerTasks(pMnode, pSnode->triggerList);
    taosArrayClear(pSnode->triggerList);
  }
}

void msmCheckSnodeTasksStatus(SMnode *pMnode) {
  void* pIter = NULL;
  
  while (true) {
    pIter = taosHashIterate(mStreamMgmt.snodeMap, pIter);
    if (NULL == pIter) {
      break;
    }

    int32_t snodeId = *(int32_t*)taosHashGetKey(pIter, NULL);
    if ((snodeId % MND_STREAM_ISOLATION_PERIOD_NUM) != mStreamMgmt.healthCtx.slotIdx) {
      continue;
    }
    
    SStmSnodeTasksStatus* pSnode = (SStmSnodeTasksStatus*)pIter;
    if (pSnode->runnerThreadNum < 0) {
      continue;
    }
    
    int64_t snodeNoUpTs = mStreamMgmt.healthCtx.currentTs - pSnode->lastUpTs;
    if (snodeNoUpTs >= MND_STREAM_WATCH_DURATION) {
      stInfo("snode %d lost, lastUpTs:%" PRId64 ", runnerThreadNum:%d, triggerTaskNum:%d, runnerTaskNum:%d", 
          snodeId, pSnode->lastUpTs, pSnode->runnerThreadNum, (int32_t)taosArrayGetSize(pSnode->triggerList), (int32_t)taosArrayGetSize(pSnode->runnerList));
      
      msmHandleSnodeLost(pMnode, pSnode);
      continue;
    }

    msmCheckTaskStatusInList(pSnode->triggerList);

    msmCheckTaskStatusInList(pSnode->runnerList);
  }
}


void msmCheckTasksStatus(SMnode *pMnode) {
  msmCheckVgTasksStatus(pMnode);
  msmCheckSnodeTasksStatus(pMnode);
}

void msmHealthCheck(SMnode *pMnode) {
  mStreamMgmt.healthCtx.slotIdx = (mStreamMgmt.healthCtx.slotIdx + 1) % MND_STREAM_ISOLATION_PERIOD_NUM;
  mStreamMgmt.healthCtx.currentTs = taosGetTimestampMs();

  mstDebug("start health check, soltIdx:%d, currentTs:%" PRId64, mStreamMgmt.healthCtx.slotIdx, mStreamMgmt.healthCtx.currentTs);

  taosWLockLatch(&mStreamMgmt.runtimeLock);
  //msmCheckStreamsStatus(pMnode);
  msmCheckTasksStatus(pMnode);
  taosWUnLockLatch(&mStreamMgmt.runtimeLock);
}

static bool msmUpdateProfileStreams(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SStreamObj *pStream = pObj;
  if (atomic_load_8(&pStream->userDropped) || atomic_load_8(&pStream->userStopped)) {
    return true;
  }
  
  pStream->updateTime = *(int64_t*)p1;
  
  (*(int64_t*)p2) ^= pStream->pCreate->streamId;
  (*(int32_t*)p3)++;
  
  return true;
}


int32_t msmInitRuntimeInfo(SMnode *pMnode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t vnodeNum = sdbGetSize(pMnode->pSdb, SDB_VGROUP);
  int32_t snodeNum = sdbGetSize(pMnode->pSdb, SDB_SNODE);
  int32_t dnodeNum = sdbGetSize(pMnode->pSdb, SDB_DNODE);

  mStreamMgmt.activeBeginTs = taosGetTimestampMs();
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

  mStreamMgmt.profile = mStreamMgmt.activeBeginTs;
  mStreamMgmt.activeStreamNum = 0;
  
  sdbTraverse(pMnode->pSdb, SDB_STREAM, msmUpdateProfileStreams, &mStreamMgmt.activeBeginTs, &mStreamMgmt.profile, &mStreamMgmt.activeStreamNum);

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



