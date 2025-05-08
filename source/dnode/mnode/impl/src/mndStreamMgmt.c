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

void msmDestroyRuntimeInfo(SMnode *pMnode) {
  // TODO

  memset(&mStreamMgmt, 0, sizeof(mStreamMgmt));
}

static int32_t msmSTAddToTaskMap(SMnode* pMnode, SStreamObj* pStream, SArray* pTasks, SStmTaskStatus* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t taskNum = pTasks ? taosArrayGetSize(pTasks) : 1;
  int64_t key[2] = {pStream->pCreate->streamId, 0};
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskStatus* pStatus = pTasks ? taosArrayGet(pTasks, i) : pTask;
    key[1] = pStatus->id.taskId;
    TAOS_CHECK_EXIT(taosHashPut(mStreamMgmt.taskMap, key, sizeof(key), &pStatus, POINTER_BYTES));
    mstDebug("task %"PRId64" tidx %d added to taskMap", pStatus->id.taskId, pStatus->id.taskIdx);
  }
  
_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}

static int32_t msmSTAddToVgroupMapImpl(SHashObj* pVgMap, SStmTaskStatus* pStatus, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStmVgroupTasksStatus vg = {0};
  SStmTaskStatusExt ext = {streamId, pStatus};

  while (true) {
    SStmVgroupTasksStatus* pVg = taosHashAcquire(pVgMap, &pStatus->id.nodeId, sizeof(pStatus->id.nodeId));
    if (NULL == pVg) {
      vg.taskList = taosArrayInit(20, sizeof(SStmTaskStatusExt));
      TSDB_CHECK_NULL(vg.taskList, code, lino, _return, terrno);
      TSDB_CHECK_NULL(taosArrayPush(vg.taskList, &ext), code, lino, _return, terrno);
      code = taosHashPut(pVgMap, &pStatus->id.nodeId, sizeof(pStatus->id.nodeId), &vg, sizeof(vg));
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
      taosHashRelease(pVgMap, pVg);
      TSDB_CHECK_NULL(NULL, code, lino, _return, terrno);
    }
    taosWUnLockLatch(&pVg->lock);
    
    taosHashRelease(pVgMap, pVg);
    break;
  }
  
_return:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    mstDebug("task %"PRId64" tidx %d added to vgroupMap", pStatus->id.taskId, pStatus->id.taskIdx);
  }

  return code;
}

static int32_t msmTDAddToVgroupMap(SHashObj* pVgMap, SStmTaskDeploy* pDeploy, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStmVgroupTasksDeploy vg = {0};
  SStreamTask* pTask = &pDeploy->task;
  SStmTaskDeployExt ext = {0};
  ext.deploy = *pDeploy;

  while (true) {
    SStmVgroupTasksDeploy* pVg = taosHashAcquire(pVgMap, &pDeploy->task.nodeId, sizeof(pDeploy->task.nodeId));
    if (NULL == pVg) {
      vg.taskList = taosArrayInit(20, sizeof(SStmTaskDeployExt));
      TSDB_CHECK_NULL(vg.taskList, code, lino, _return, terrno);
      TSDB_CHECK_NULL(taosArrayPush(vg.taskList, &ext), code, lino, _return, terrno);
      code = taosHashPut(pVgMap, &pDeploy->task.nodeId, sizeof(pDeploy->task.nodeId), &vg, sizeof(SStmVgroupTasksDeploy));
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
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    int32_t num = atomic_add_fetch_32(&mStreamMgmt.toDeployVgTaskNum, 1);
    ST_TASK_DLOG("task added to toDeployVgTaskNum, vgToDeployTaskNum:%d", num);
  }

  return code;
}


static int32_t msmSTAddToSnodeMapImpl(SHashObj* pHash, SStmTaskStatus* pStatus, SStreamObj* pStream, bool triggerTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStmSnodeTasksStatus snode = {0};
  SStmTaskStatusExt ext = {streamId, pStatus};

  while (true) {
    SStmSnodeTasksStatus* pSnode = taosHashAcquire(pHash, &pStatus->id.nodeId, sizeof(pStatus->id.nodeId));
    if (NULL == pSnode) {
      if (triggerTask) {
        snode.triggerList = taosArrayInit(20, sizeof(SStmTaskStatusExt));
        TSDB_CHECK_NULL(snode.triggerList, code, lino, _return, terrno);
        TSDB_CHECK_NULL(taosArrayPush(snode.triggerList, &ext), code, lino, _return, terrno);
      } else {
        snode.runnerList = taosArrayInit(20, sizeof(SStmTaskStatusExt));
        TSDB_CHECK_NULL(snode.runnerList, code, lino, _return, terrno);
        TSDB_CHECK_NULL(taosArrayPush(snode.runnerList, &ext), code, lino, _return, terrno);
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

    taosWLockLatch(&pSnode->lock);
    SArray** ppList = triggerTask ? &pSnode->triggerList : &pSnode->runnerList;
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
    taosWUnLockLatch(&pSnode->lock);
    
    taosHashRelease(pHash, pSnode);
    break;
  }
  
_return:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    mstDebug("task %"PRId64" tidx %d added to snodeMap, snodeId:%d", pStatus->id.taskId, pStatus->id.taskIdx, pStatus->id.nodeId);
  }

  return code;
}



static int32_t msmTDAddSnodeTask(SHashObj* pHash, SStmTaskDeploy* pDeploy, SStreamObj* pStream, bool triggerTask, bool lowestRunner) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStmSnodeTasksDeploy snode = {0};
  SStmTaskDeployExt ext;
  SStreamTask* pTask = &pDeploy->task;
  SArray** ppList = triggerTask ? &snode.triggerList : &snode.runnerList;

  ext.lowestRunner = lowestRunner;

  while (true) {
    SStmSnodeTasksDeploy* pSnode = taosHashAcquire(pHash, &pDeploy->task.nodeId, sizeof(pDeploy->task.nodeId));
    if (NULL == pSnode) {
      *ppList = taosArrayInit(10, sizeof(SStmTaskDeployExt));
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
      *ppList = taosArrayInit(10, sizeof(SStmTaskDeployExt));
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
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    ST_TASK_DLOG("task added to toDeploySnodeMap, tidx:%d", pTask->taskIdx);
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

int32_t msmBuildReaderDeployInfo(SStmTaskDeploy* pDeploy, SStreamObj* pStream, void* calcScanPlan, bool triggerReader) {
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
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return TSDB_CODE_SUCCESS;
}


int32_t msmBuildRunnerDeployInfo(SStmTaskDeploy* pDeploy, SSubplan *plan, SStreamObj* pStream, int32_t replica) {
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
  pMsg->pNotifyAddrUrls = pStream->pCreate->pNotifyAddrUrls;
  pMsg->notifyErrorHandle = pStream->pCreate->notifyErrorHandle;
  pMsg->outCols = pStream->pCreate->outCols;
  pMsg->outTags = pStream->pCreate->outTags;
  pMsg->outStbUid = pStream->pCreate->outStbUid;
  
  pMsg->subTblNameExpr = pStream->pCreate->subTblNameExpr;
  pMsg->tagValueExpr = pStream->pCreate->tagValueExpr;
  pMsg->forceOutCols = pStream->pCreate->forceOutCols;

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}


static int32_t msmSTAddToVgroupMap(SMnode* pMnode, SStreamObj* pStream, SArray* pTasks) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t taskNum = taosArrayGetSize(pTasks);
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskStatus* pStatus = taosArrayGet(pTasks, i);
    TSDB_CHECK_CODE(msmSTAddToVgroupMapImpl(mStreamMgmt.vgroupMap, pStatus, pStream), lino, _return);
  }
  
_return:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}


static int32_t msmSTAddToSnodeMap(SMnode* pMnode, SStreamObj* pStream, SArray* pTasks, SStmTaskStatus* pTask, bool triggerTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t taskNum = triggerTask ? 1 : taosArrayGetSize(pTasks);
  int32_t taskType = triggerTask ? STREAM_TRIGGER_TASK : STREAM_RUNNER_TASK;
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskStatus* pStatus = triggerTask ? pTask : taosArrayGet(pTasks, i);
    TSDB_CHECK_CODE(msmSTAddToSnodeMapImpl(mStreamMgmt.snodeMap, pStatus, pStream, triggerTask), lino, _return);
  }
  
_return:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int64_t msmAssignTaskId(void) {
  return atomic_fetch_add_64(&mStreamMgmt.lastTaskId, 1);
}

int32_t msmAssignTaskSnodeId(SMnode* pMnode, int64_t streamId, bool baseOnStmId) {
  int32_t snodeNum = sdbGetSize(pMnode->pSdb, SDB_SNODE);
  int32_t snodeTarget = baseOnStmId ? (streamId % snodeNum) : taosRand() % snodeNum;
  int32_t snodeIdx = 0;
  int32_t snodeId = 0;
  void      *pIter = NULL;
  SSnodeObj *pObj = NULL;

  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_SNODE, pIter, (void **)&pObj);
    if (pIter == NULL) {
      break;
    }

    snodeId = pObj->id;
    if (snodeIdx == snodeTarget) {
      sdbRelease(pMnode->pSdb, pObj);
      sdbCancelFetch(pMnode->pSdb, pIter);
      return snodeId;
    }

    snodeIdx++;
  }

  return snodeId;
}


static int32_t msmBuildTriggerTasks(SMnode* pMnode, SStmStatus* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;

  pInfo->triggerTask = taosMemoryCalloc(1, sizeof(SStmTaskStatus));
  TSDB_CHECK_NULL(pInfo->triggerTask, code, lino, _return, terrno);

  pInfo->triggerTask->id.taskId = msmAssignTaskId();
  pInfo->triggerTask->id.nodeId = msmAssignTaskSnodeId(pMnode, pStream->pCreate->streamId, true);
  pInfo->triggerTask->id.taskIdx = 0;
  pInfo->triggerTask->lastUpTs = INT64_MIN;

  SStmTaskDeploy info = {0};
  info.task.type = STREAM_TRIGGER_TASK;
  info.task.streamId = streamId;
  info.task.taskId =  pInfo->triggerTask->id.taskId;
  info.task.nodeId =  pInfo->triggerTask->id.nodeId;
  info.task.taskIdx =  pInfo->triggerTask->id.taskIdx;
  TSDB_CHECK_CODE(msmBuildTriggerDeployInfo(pMnode, pInfo, &info, pStream), lino, _return);
  TSDB_CHECK_CODE(msmTDAddSnodeTask(mStreamMgmt.toDeploySnodeMap, &info, pStream, true, false), lino, _return);
  
  atomic_add_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);

  TSDB_CHECK_CODE(msmSTAddToTaskMap(pMnode, pStream, NULL, pInfo->triggerTask), lino, _return);
  TSDB_CHECK_CODE(msmSTAddToSnodeMap(pMnode, pStream, NULL, pInfo->triggerTask, true), lino, _return);

_return:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmTDAddReaderTriggerTasks(SMnode* pMnode, SStmStatus* pInfo, SStreamObj* pStream, int32_t* taskIdx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SSdb   *pSdb = pMnode->pSdb;
  SStmTaskStatus state = {{INT64_MIN, INT32_MIN, *taskIdx}, 0, 0, INT64_MIN};
  SArray* pReader = pInfo->readerList;
  
  switch (pStream->pCreate->triggerTblType) {
    case TSDB_NORMAL_TABLE:
    case TSDB_CHILD_TABLE: {
      state.id.taskId = msmAssignTaskId();
      state.id.nodeId = pStream->pCreate->triggerTblVgId;
      state.id.taskIdx = 0;
      state.flags = STREAM_FLAG_TRIGGER_READER;
      TSDB_CHECK_NULL(taosArrayPush(pReader, &state), code, lino, _return, terrno);

      SStmTaskDeploy info = {0};
      info.task.type = STREAM_READER_TASK;
      info.task.streamId = streamId;
      info.task.taskId = state.id.taskId;
      info.task.nodeId = state.id.nodeId;
      info.task.taskIdx = state.id.taskIdx;
      TSDB_CHECK_CODE(msmBuildReaderDeployInfo(&info, pStream, NULL, true), lino, _return);
      TSDB_CHECK_CODE(msmTDAddToVgroupMap(mStreamMgmt.toDeployVgMap, &info, pStream), lino, _return);
      break;
    }
    case TSDB_SUPER_TABLE: {
      SDbObj* pDb = mndAcquireDb(pMnode, pStream->pCreate->triggerDB);
      if (NULL == pDb) {
        code = terrno;
        mstError("failed to acquire db %s, error:%s", pStream->pCreate->triggerDB, terrstr());
        goto _return;
      }
      
      void *pIter = NULL;
      while (1) {
        SVgObj *pVgroup = NULL;
        pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
        if (pIter == NULL) {
          break;
        }
      
        if (pVgroup->dbUid == pDb->uid && !pVgroup->isTsma) {
          state.id.taskId = msmAssignTaskId();
          state.id.nodeId = pVgroup->vgId;
          state.id.taskIdx++;
          TSDB_CHECK_NULL(taosArrayPush(pReader, &state), code, lino, _return, terrno);

          SStmTaskDeploy info = {0};
          info.task.type = STREAM_READER_TASK;
          info.task.streamId = streamId;
          info.task.taskId = state.id.taskId;
          info.task.nodeId = state.id.nodeId;
          info.task.taskIdx = state.id.taskIdx;
          TSDB_CHECK_CODE(msmBuildReaderDeployInfo(&info, pStream, NULL, true), lino, _return);
          TSDB_CHECK_CODE(msmTDAddToVgroupMap(mStreamMgmt.toDeployVgMap, &info, pStream), lino, _return);
        }
      
        sdbRelease(pSdb, pVgroup);
      }
      break;
    }
    case TSDB_VIRTUAL_CHILD_TABLE:
    case TSDB_VIRTUAL_NORMAL_TABLE: {

    }
    default:
      code = TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
      mstError("invalid triggerTblType %d", pStream->pCreate->triggerTblType);
      break;
  }

_return:

  *taskIdx = state.id.taskIdx;

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmUPAddTask(SMnode* pMnode, SStreamObj* pStream, char* scanPlan, int32_t vgId, int64_t taskId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SSubplan* pSubplan = NULL;
  int64_t streamId = pStream->pCreate->streamId;
  int64_t key[2] = {streamId, 0};
  SStmTaskSrcAddr addr;
  TAOS_CHECK_EXIT(nodesStringToNode(scanPlan, (SNode**)&pSubplan));
  addr.epset = mndGetVgroupEpsetById(pMnode, vgId);
  addr.taskId = taskId;
  addr.vgId = vgId;
  addr.groupId = pSubplan->id.groupId;

  key[1] = pSubplan->id.subplanId;
  
  TAOS_CHECK_EXIT(taosHashPut(mStreamMgmt.toUpdateScanMap, key, sizeof(key), &addr, sizeof(addr)));
  
  atomic_add_fetch_32(&mStreamMgmt.toUpdateScanNum, 1);
  
_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmTDAddReaderRunnerTasks(SMnode* pMnode, SStmStatus* pInfo, SStreamObj* pStream, int32_t* taskIdx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t calcTasksNum = taosArrayGetSize(pStream->pCreate->calcScanPlanList);
  int64_t streamId = pStream->pCreate->streamId;
  SStmTaskStatus state = {{INT64_MIN, INT32_MIN, *taskIdx}, 0, 0, INT64_MIN};
  SArray* pReader = pInfo->readerList;
  
  for (int32_t i = 0; i < calcTasksNum; ++i) {
    SStreamCalcScan* pScanList = taosArrayGet(pStream->pCreate->calcScanPlanList, i);
    if (pScanList->readFromCache) {
      continue;
    }
    
    int32_t vgNum = taosArrayGetSize(pScanList->vgList);
    for (int32_t m = 0; m < vgNum; ++m) {
      state.id.taskId = msmAssignTaskId();
      state.id.nodeId = *(int32_t*)taosArrayGet(pScanList->vgList, i);
      state.id.taskIdx++;
      TSDB_CHECK_NULL(taosArrayPush(pReader, &state), code, lino, _return, terrno);

      SStmTaskDeploy info = {0};
      info.task.type = STREAM_READER_TASK;
      info.task.streamId = streamId;
      info.task.taskId = state.id.taskId;
      info.task.nodeId = state.id.nodeId;
      info.task.taskIdx = state.id.taskIdx;
      TSDB_CHECK_CODE(msmBuildReaderDeployInfo(&info, pStream, pScanList->scanPlan, false), lino, _return);
      TSDB_CHECK_CODE(msmUPAddTask(pMnode, pStream, pScanList->scanPlan, state.id.nodeId, state.id.taskId), lino, _return);
      TSDB_CHECK_CODE(msmTDAddToVgroupMap(mStreamMgmt.toDeployVgMap, &info, pStream), lino, _return);
    }
  }

_return:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
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
        mstError("failed to acquire db %s, error:%s", pStream->pCreate->triggerDB, terrstr());
        break;
      }

      *taskNum = pDb->cfg.numOfVgroups;
      break;
    }
    case TSDB_VIRTUAL_CHILD_TABLE:
    case TSDB_VIRTUAL_NORMAL_TABLE: {

    }
    default:
      code = TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
      mstError("invalid triggerTblType %d", pStream->pCreate->triggerTblType);
      break;
  }

  int32_t calcTasksNum = taosArrayGetSize(pStream->pCreate->calcScanPlanList);
  for (int32_t i = 0; i < calcTasksNum; ++i) {
    SStreamCalcScan* pScan = taosArrayGet(pStream->pCreate->calcScanPlanList, i);
    *taskNum += taosArrayGetSize(pScan->vgList);
  }
}

static int32_t msmBuildReaderTasks(SMnode* pMnode, SStmStatus* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t taskIdx = -1;
  int32_t taskNum = 0;

  msmCalcReadersNum(pMnode, pStream, &taskNum);

  pInfo->readerList = taosArrayInit(taskNum, sizeof(SStmTaskStatus));
  TSDB_CHECK_NULL(pInfo->readerList, code, lino, _exit, terrno);
  
  TSDB_CHECK_CODE(msmTDAddReaderTriggerTasks(pMnode, pInfo, pStream, &taskIdx), lino, _exit);
  TSDB_CHECK_CODE(msmTDAddReaderRunnerTasks(pMnode, pInfo, pStream, &taskIdx), lino, _exit);

  TSDB_CHECK_CODE(msmSTAddToTaskMap(pMnode, pStream, pInfo->readerList, NULL), lino, _exit);
  TSDB_CHECK_CODE(msmSTAddToVgroupMap(pMnode, pStream, pInfo->readerList), lino, _exit);
  
_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}

int32_t msmUpdatePlanSourceAddr(SSubplan* plan, int64_t clientId, SStmTaskSrcAddr* pSrc, int32_t msgType) {
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
  
  return qSetSubplanExecutionNode(plan, pSrc->groupId, &source);
}

int32_t msmGetTaskIdFromSubplanId(SStreamObj* pStream, SArray* pRunners, int32_t beginIdx, int32_t subplanId, int64_t* taskId) {
  int64_t streamId = pStream->pCreate->streamId;
  int32_t runnerNum = taosArrayGetSize(pRunners);
  for (int32_t i = beginIdx; i < runnerNum; ++i) {
    SStmTaskDeploy* pDeploy = taosArrayGet(pRunners, i);
    SSubplan* pPlan = pDeploy->msg.runner.pPlan;
    if (pPlan->id.subplanId == subplanId) {
      *taskId = pDeploy->task.taskId;
      return TSDB_CODE_SUCCESS;
    }
  }

  mstError("subplanId %d not found in runner list", subplanId);

  return TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
}

int32_t msmUpdateLowestPlanSourceAddr(SSubplan* pPlan, SStmTaskDeploy* pDeploy, int64_t streamId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t key[2] = {streamId, -1};
  SNode* pNode = NULL;
  FOREACH(pNode, pPlan->pChildren) {
    if (QUERY_NODE_VALUE != nodeType(pNode)) {
      mstError("invalid node type %d for runner's child subplan", nodeType(pNode));
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }
    SValueNode* pVal = (SValueNode*)pNode;
    if (TSDB_DATA_TYPE_BIGINT != pVal->node.resType.type) {
      mstError("invalid value node data type %d for runner's child subplan", pVal->node.resType.type);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }

    key[1] = MND_GET_RUNNER_SUBPLANID(pVal->datum.i);

    SStmTaskSrcAddr* pAddr = taosHashGet(mStreamMgmt.toUpdateScanMap, key, sizeof(key));
    if (NULL == pAddr) {
      mstError("lowest runner subplan ID:%d,%d can't get its child ID:%" PRId64 " addr", pPlan->id.groupId, pPlan->id.subplanId, key[1]);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }

    TAOS_CHECK_EXIT(msmUpdatePlanSourceAddr(pPlan, pDeploy->task.taskId, pAddr, TDMT_STREAM_FETCH));
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmUpdateRunnerPlan(SMnode* pMnode, SArray* pRunners, int32_t beginIdx, SStmTaskDeployExt* pExt, SStreamObj* pStream) {
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
  addr.epset = mndGetDnodeEpsetById(pMnode, pDeploy->task.nodeId);
  FOREACH(pNode, pPlan->pParents) {
    SSubplan* pSubplan = (SSubplan*)pNode;
    TAOS_CHECK_EXIT(msmGetTaskIdFromSubplanId(pStream, pRunners, beginIdx, pSubplan->id.subplanId, &parentTaskId));
    TAOS_CHECK_EXIT(msmUpdatePlanSourceAddr(pSubplan, parentTaskId, &addr, TDMT_STREAM_FETCH_FROM_RUNNER));
  }
  
_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmUpdateRunnerPlans(SMnode* pMnode, SArray* pRunners, SStreamObj* pStream, int32_t taskNum) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t runnerNum = taosArrayGetSize(pRunners);
  
  for (int32_t i = runnerNum - taskNum; i <= runnerNum - 1; ++i) {
    SStmTaskDeployExt* pExt = taosArrayGet(pRunners, i);
    TAOS_CHECK_EXIT(msmUpdateRunnerPlan(pMnode, pRunners, i, pExt, pStream));
    TAOS_CHECK_EXIT(nodesNodeToString((SNode*)pExt->deploy.msg.runner.pPlan, false, (char**)&pExt->deploy.msg.runner.pPlan, NULL));

    SStreamTask* pTask = &pExt->deploy.task;
    ST_TASK_DLOG("runner task plan:%s", (const char*)pExt->deploy.msg.runner.pPlan);
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmBuildRunnerTasksImpl(SMnode* pMnode, SQueryPlan* pDag, SStmStatus* pInfo, SStreamObj* pStream) {
  int32_t code = 0;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;

  if (pDag->numOfSubplans <= 0) {
    mstError("invalid subplan num:%d", pDag->numOfSubplans);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  }

  int32_t levelNum = (int32_t)LIST_LENGTH(pDag->pSubplans);
  if (levelNum <= 0) {
    mstError("invalid level num:%d", levelNum);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  }

  int32_t        lowestLevelIdx = levelNum - 1;
  SNodeListNode *plans = NULL;
  int32_t        taskNum = 0;
  int32_t        totalTaskNum = 0;
  SStmTaskStatus state = {{INT64_MIN, INT32_MIN, 0}, 0, 0, INT64_MIN};

  plans = (SNodeListNode *)nodesListGetNode(pDag->pSubplans, 0);
  if (QUERY_NODE_NODE_LIST != nodeType(plans)) {
    mstError("invalid level plan, level:0, planNodeType:%d", nodeType(plans));
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  }
  
  taskNum = (int32_t)LIST_LENGTH(plans->pNodeList);
  if (taskNum != 1) {
    mstError("invalid level plan number:%d, level:0", taskNum);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  }

  for (int32_t r = 0; r < pInfo->runnerDeploys; ++r) {
    totalTaskNum = 0;
    state.id.nodeId = msmAssignTaskSnodeId(pMnode, pStream->pCreate->streamId, (0 == r) ? true : false);

    for (int32_t i = lowestLevelIdx; i >= 0; --i) {
      plans = (SNodeListNode *)nodesListGetNode(pDag->pSubplans, i);
      if (NULL == plans) {
        mstError("empty level plan, level:%d", i);
        TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      }

      if (QUERY_NODE_NODE_LIST != nodeType(plans)) {
        mstError("invalid level plan, level:%d, planNodeType:%d", i, nodeType(plans));
        TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      }

      taskNum = (int32_t)LIST_LENGTH(plans->pNodeList);
      if (taskNum <= 0) {
        mstError("invalid level plan number:%d, level:%d", taskNum, i);
        TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      }

      totalTaskNum += taskNum;
      if (totalTaskNum > pDag->numOfSubplans) {
        mstError("current totalTaskNum %d is bigger than numOfSubplans %d, level:%d", totalTaskNum, pDag->numOfSubplans, i);
        TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      }

      for (int32_t n = 0; n < taskNum; ++n) {
        SSubplan *plan = (SSubplan *)nodesListGetNode(plans->pNodeList, n);

        state.id.taskId = msmAssignTaskId();
        state.id.taskIdx = MND_SET_RUNNER_TASKIDX(i, n);
        TSDB_CHECK_NULL(taosArrayPush(pInfo->runnerList, &state), code, lino, _exit, terrno);
        
        if (0 == i) {
          int32_t idx = taosArrayGetSize(pInfo->runnerList) - 1;
          TSDB_CHECK_NULL(taosArrayPush(pInfo->runnerTopIdx, &idx), code, lino, _exit, terrno);
        }

        SStmTaskDeploy info = {0};
        info.task.type = STREAM_RUNNER_TASK;
        info.task.streamId = streamId;
        info.task.taskId = state.id.taskId;
        info.task.nodeId = state.id.nodeId;
        info.task.taskIdx = state.id.taskIdx;
        TSDB_CHECK_CODE(msmBuildRunnerDeployInfo(&info, plan, pStream, pInfo->runnerReplica), lino, _exit);

        TSDB_CHECK_CODE(msmTDAddSnodeTask(mStreamMgmt.toDeploySnodeMap, &info, pStream, false, i == lowestLevelIdx), lino, _exit);
        
        atomic_add_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);        
      }

      mstDebug("level %d initialized, taskNum:%d", i, taskNum);
    }

    if (totalTaskNum != pDag->numOfSubplans) {
      mstError("totalTaskNum %d mis-match with numOfSubplans %d", totalTaskNum, pDag->numOfSubplans);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }

    SStmSnodeTasksDeploy* pSnodeDeploy = taosHashGet(mStreamMgmt.toDeploySnodeMap, &state.id.nodeId, sizeof(state.id.nodeId));
    TSDB_CHECK_NULL(pSnodeDeploy, code, lino, _exit, terrno);
    
    TAOS_CHECK_EXIT(msmUpdateRunnerPlans(pMnode, pSnodeDeploy->runnerList, pStream, totalTaskNum));

    TAOS_CHECK_EXIT(nodesStringToNode(pStream->pCreate->calcPlan, (SNode**)&pDag));
  }

  TSDB_CHECK_CODE(msmSTAddToTaskMap(pMnode, pStream, pInfo->runnerList, NULL), lino, _exit);
  TSDB_CHECK_CODE(msmSTAddToSnodeMap(pMnode, pStream, pInfo->runnerList, NULL, false), lino, _exit);

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
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
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


static int32_t msmBuildRunnerTasks(SMnode* pMnode, SStmStatus* pInfo, SStreamObj* pStream) {
  if (NULL == pStream->pCreate->calcPlan) {
    return TSDB_CODE_SUCCESS;
  }
  
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SQueryPlan* pPlan = NULL;

  TAOS_CHECK_EXIT(nodesStringToNode(pStream->pCreate->calcPlan, (SNode**)&pPlan));

  msmSetStreamRunnerExecReplica(streamId, pInfo);

  pInfo->runnerList = taosArrayInit(pPlan->numOfSubplans, sizeof(SStmTaskStatus));
  TSDB_CHECK_NULL(pInfo->runnerList, code, lino, _exit, terrno);

  TAOS_CHECK_EXIT(msmBuildRunnerTasksImpl(pMnode, pPlan, pInfo, pStream));

  taosHashClear(mStreamMgmt.toUpdateScanMap);
  mStreamMgmt.toUpdateScanNum = 0;

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


static int32_t msmBuildStreamTasks(SMnode* pMnode, SStmStatus* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;

  TSDB_CHECK_CODE(msmBuildReaderTasks(pMnode, pInfo, pStream), lino, _return);
  TSDB_CHECK_CODE(msmBuildRunnerTasks(pMnode, pInfo, pStream), lino, _return);
  TSDB_CHECK_CODE(msmBuildTriggerTasks(pMnode, pInfo, pStream), lino, _return);
  
_return:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
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
    TSDB_CHECK_CODE(code, lino, _return);
  
    sdbRelease(pMnode->pSdb, pSnode);
  }

  pSnode = NULL;

_return:

  sdbRelease(pMnode->pSdb, pSnode);

  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
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
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


static int32_t msmDeployStreamTasks(SMnode* pMnode, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStmStatus info = {0};
  info.lastActTs = INT64_MIN;
  
  TAOS_CHECK_EXIT(msmBuildStreamTasks(pMnode, &info, pStream));

  TAOS_CHECK_EXIT(taosHashPut(mStreamMgmt.streamMap, &streamId, sizeof(streamId), &info, sizeof(info)));

  mstDebug("stream added to streamMap, readerNum:%d, triggerNum:%d, runnerNum:%d", 
      (int32_t)taosArrayGetSize(info.readerList), info.triggerTask ? 1 : 0, (int32_t)taosArrayGetSize(info.runnerList));

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


static int32_t msmLaunchStreamDepolyAction(SMnode* pMnode, SStmQNode* pQNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pQNode->streamId;
  char* streamName = pQNode->streamName;
  SStreamObj* pStream = NULL;

  SStmStatus** ppStream = (SStmStatus**)taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (ppStream) {
    mstError("stream %s already in streamMap", pQNode->streamName);
    code = TSDB_CODE_STREAM_INTERNAL_ERROR;
    goto _exit;
  }

  code = mndAcquireStream(pMnode, streamName, &pStream);
  if (TSDB_CODE_MND_STREAM_NOT_EXIST == code) {
    mstWarn("stream %s no longer exists, ignore deploy", streamName);
    return TSDB_CODE_SUCCESS;
  }

  TAOS_CHECK_EXIT(code);

  int8_t userStopped = atomic_load_8(&pStream->userStopped);
  int8_t userDropped = atomic_load_8(&pStream->userDropped);
  if (userStopped || userDropped) {
    mstWarn("stream %s is stopped %d or removing %d, ignore deploy", streamName, userStopped, userDropped);
    goto _exit;
  }
  
  TAOS_CHECK_EXIT(msmDeployStreamTasks(pMnode, pStream));


_exit:

  mndReleaseStream(pMnode, pStream);

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmRemoveStreamFromActionQ(int64_t streamId) {
  if (0 >= atomic_load_64(&mStreamMgmt.actionQ->qRemainNum)) {
    return TSDB_CODE_SUCCESS;
  }

  SStmQNode *node = mStreamMgmt.actionQ->head->next;

  while (node) {
    if (streamId == node->streamId) {
      //STREAMTODO MAY NOT USED, ACTION HANDLE NEED TO ENSURE STREAM NOT DROPPING

      atomic_sub_fetch_64(&mStreamMgmt.actionQ->qRemainNum, 1);
    }

    node = node->next;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t msmTDRemoveStream(int64_t streamId) {
  void* pIter = NULL;
  
  if (atomic_load_32(&mStreamMgmt.toDeployVgTaskNum) > 0) {
    while ((pIter = taosHashIterate(mStreamMgmt.toDeployVgMap, pIter))) {
      SStmVgroupTasksDeploy* pVg = (SStmVgroupTasksDeploy*)pIter;
      int32_t taskNum = taosArrayGetSize(pVg->taskList);
      if (atomic_load_32(&pVg->deployed) == taskNum) {
        continue;
      }
      
      for (int32_t i = 0; i < taskNum; ++i) {
        SStmTaskDeployExt* pExt = taosArrayGet(pVg->taskList, i);
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
          SStmTaskDeployExt* pExt = taosArrayGet(pSnode->triggerList, i);
          if (pExt->deploy.task.streamId == streamId && !pExt->deployed) {
            pExt->deployed = true;
          }
        }
      }

      taskNum = taosArrayGetSize(pSnode->runnerList);
      if (atomic_load_32(&pSnode->runnerDeployed) != taskNum) {
        for (int32_t i = 0; i < taskNum; ++i) {
          SStmTaskDeployExt* pExt = taosArrayGet(pSnode->runnerList, i);
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
      }
    }

    taskNum = taosArrayGetSize(pSnode->runnerList);
    for (int32_t i = taskNum - 1; i >= 0; --i) {
      SStmTaskStatusExt* pExt = taosArrayGet(pSnode->runnerList, i);
      if (pExt->streamId == streamId) {
        taosArrayRemove(pSnode->runnerList, i);
      }
    }
  }

  while ((pIter = taosHashIterate(mStreamMgmt.vgroupMap, pIter))) {
    SStmVgroupTasksStatus* pVg = (SStmVgroupTasksStatus*)pIter;
    int32_t taskNum = taosArrayGetSize(pVg->taskList);
    for (int32_t i = taskNum - 1; i >= 0; --i) {
      SStmTaskStatusExt* pExt = taosArrayGet(pVg->taskList, i);
      if (pExt->streamId == streamId) {
        taosArrayRemove(pVg->taskList, i);
      }
    }
  }

  size_t keyLen = 0;
  while ((pIter = taosHashIterate(mStreamMgmt.taskMap, pIter))) {
    int64_t* pStreamId = taosHashGetKey(pIter, &keyLen);
    if (*pStreamId == streamId) {
      taosHashRemove(mStreamMgmt.taskMap, pStreamId, keyLen);
    }
  }

  while ((pIter = taosHashIterate(mStreamMgmt.streamMap, pIter))) {
    int64_t* pStreamId = taosHashGetKey(pIter, &keyLen);
    if (*pStreamId == streamId) {
      taosHashRemove(mStreamMgmt.streamMap, pStreamId, keyLen);
    }
  }

  return code;
}

static int32_t msmRemoveStreamFromMaps(SMnode* pMnode, int64_t streamId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  mstInfo("start to remove stream from maps, current stream num:%d", taosHashGetSize(mStreamMgmt.streamMap));

  //TAOS_CHECK_EXIT(msmRemoveStreamFromActionQ(streamId));
  TAOS_CHECK_EXIT(msmTDRemoveStream(streamId));
  TAOS_CHECK_EXIT(msmSTRemoveStream(streamId));

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    mstInfo("end remove stream from maps, current stream num:%d", taosHashGetSize(mStreamMgmt.streamMap));
  }

  return code;
}

int32_t msmUndeployStream(SMnode* pMnode, int64_t streamId, char* streamName) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  taosWLockLatch(&mStreamMgmt.runtimeLock);

  SStmStatus** ppStream = (SStmStatus**)taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == ppStream) {
    mstInfo("stream %s already not in streamMap", streamName);
    goto _exit;
  }

  TAOS_CHECK_EXIT(msmRemoveStreamFromMaps(pMnode, streamId));

_exit:

  taosWUnLockLatch(&mStreamMgmt.runtimeLock);

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmHandleStreamActions(SStmGrpCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmQNode* pQNode = NULL;

  stDebug("start to handle stream actions");
  
  while (mndStreamActionDequeue(mStreamMgmt.actionQ, &pQNode)) {
    switch (pQNode->action) {
      case STREAM_ACT_DEPLOY:
        TAOS_CHECK_EXIT(msmLaunchStreamDepolyAction(pCtx->pMnode, pQNode));
        break;
      default:
        break;
    }
  }

_exit:

  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
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
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
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
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    ST_TASK_DLOG("task added to GRP deployMap, taskIdx:%d", pTask->taskIdx);
  }

  return code;
}


int32_t msmGrpAddDeployTasks(SHashObj* pHash, SArray* pTasks, int32_t* deployed) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t taskNum = taosArrayGetSize(pTasks);

  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskDeployExt* pExt = taosArrayGet(pTasks, i);
    if (pExt->deployed) {
      continue;
    }

    TAOS_CHECK_EXIT(msmGrpAddDeployTask(pHash, &pExt->deploy));
    pExt->deployed = true;

    atomic_add_fetch_32(deployed, 1);
  }

_exit:

  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmGrpAddDeployVgTasks(SStmGrpCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t vgNum = taosArrayGetSize(pCtx->pReq->pVgLeaders);
  SStmVgroupTasksDeploy* pVg = NULL;
  int32_t tidx = streamGetThreadIdx(mStreamMgmt.threadNum, pCtx->pReq->streamGId);

  stDebug("start to add stream vgroup tasks deploy");
  
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

    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmGrpAddDeploySnodeTasks(SStmGrpCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmSnodeTasksDeploy* pSnode = NULL;
  SStreamHbMsg* pReq = pCtx->pReq;

  stDebug("start to add stream snode tasks deploy");
  
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

    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmUpdateStreamLastActTs(int64_t streamId, int64_t currTs) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmStatus* pStatus = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStatus) {
    mstError("stream not exists in streamMap, mapSize:%d", taosHashGetSize(mStreamMgmt.streamMap));
    TSDB_CHECK_NULL(pStatus, code, lino, _exit, TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  }
  
  pStatus->lastActTs = currTs;

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmRspAddStreamsDeploy(SStmGrpCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t streamNum = taosHashGetSize(pCtx->deployStm);
  void* pIter = NULL;

  stDebug("start to add group %d deploy streams, streamNum:%d", pCtx->pReq->streamGId, taosHashGetSize(pCtx->deployStm));
  
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
    mstDebug("stream DEPLOY added to dnode %d hb rsp, readerTasks:%zu, triggerTask:%d, runnerTasks:%zu", 
        pCtx->pReq->dnodeId, taosArrayGetSize(pDeploy->readerTasks), pDeploy->triggerTask ? 1 : 0, taosArrayGetSize(pDeploy->runnerTasks));
  }
  
_exit:

  if (pIter) {
    taosHashCancelIterate(pCtx->deployStm, pIter);
  }

  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}

void msmCleanDeployedVgTasks(SArray* pVgLeaders) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t vgNum = taosArrayGetSize(pVgLeaders);
  SStmVgroupTasksDeploy* pVg = NULL;
  
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
      SStmTaskDeployExt* pExt = taosArrayGet(pVg->taskList, m);
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

    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
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
      SStmTaskDeployExt* pExt = taosArrayGet(pSnode->triggerList, m);
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
      SStmTaskDeployExt* pExt = taosArrayGet(pSnode->runnerList, m);
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

int32_t msmGrpAddActionStart(SHashObj* pHash, int64_t streamId, SStreamTask* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t action = STREAM_ACT_START;
  SStmAction *pAction = taosHashGet(pHash, &streamId, sizeof(streamId));
  if (pAction) {
    pAction->actions |= action;
    ST_TASK_DLOG("task append START action, actions:%x", pAction->actions);
  } else {
    SStmAction newAction = {0};
    newAction.actions = action;
    TAOS_CHECK_EXIT(taosHashPut(pHash, &streamId, sizeof(streamId), &newAction, sizeof(newAction)));
    ST_TASK_DLOG("task add START action, actions:%x", newAction.actions);
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
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
    TSDB_CHECK_NULL(taosArrayPush(pAction->undeploy.taskList, &pTask), code, lino, _exit, terrno);
    if (pAction->undeploy.doCheckpoint) {
      pAction->undeploy.doCheckpoint = dropped ? false : true;
    }
    if (!pAction->undeploy.doCleanup) {
      pAction->undeploy.doCleanup = dropped ? true : false;
    }
    
    ST_TASK_DLOG("task append UNDEPLOY action[%d,%d], actions:%x", pAction->undeploy.doCheckpoint, pAction->undeploy.doCleanup, pAction->actions);
  } else {
    SStmAction newAction = {0};
    newAction.actions = action;
    newAction.undeploy.doCheckpoint = dropped ? false : true;
    newAction.undeploy.doCleanup = dropped ? true : false;
    newAction.undeploy.taskList = taosArrayInit(pCtx->taskNum, POINTER_BYTES);
    TSDB_CHECK_NULL(newAction.undeploy.taskList, code, lino, _exit, terrno);
    TSDB_CHECK_NULL(taosArrayPush(newAction.undeploy.taskList, &pTask), code, lino, _exit, terrno);
    TAOS_CHECK_EXIT(taosHashPut(pCtx->actionStm, &streamId, sizeof(streamId), &newAction, sizeof(newAction)));
    
    ST_TASK_DLOG("task add UNDEPLOY action[%d,%d]", newAction.undeploy.doCheckpoint, newAction.undeploy.doCleanup);
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


int32_t msmHandleTaskAbnormalStatus(SStmGrpCtx* pCtx, SStmTaskStatusMsg* pMsg) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t action = 0;
  int64_t streamId = pMsg->streamId;
  SStreamTask* pTask = (SStreamTask*)pMsg;

  ST_TASK_DLOG("start to handle task abnormal status %d", pTask->status);
  
  SStmStatus* pStatus = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStatus) {
    ST_TASK_ELOG("stream no longer exists in streamMap, try to undeploy it, idx:%d", pMsg->taskIdx);
    return msmGrpAddActionUndeploy(pCtx, streamId, pTask);
  }
  
  switch (pMsg->status) {
    case STREAM_STATUS_INIT:
      if ((pCtx->currTs - pStatus->lastActTs) < STREAM_ACT_MIN_DELAY_MSEC) {
        ST_TASK_DLOG("task wait not enough between actions, currTs:%" PRId64 ", lastTs:%" PRId64, pCtx->currTs, pStatus->lastActTs);
        return code;
      }

      TAOS_CHECK_EXIT(msmGrpAddActionStart(pCtx->actionStm, streamId, pTask));
      break;
    case STREAM_STATUS_STOPPED:
    case STREAM_STATUS_FAILED:
    default:
      break;
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

void msmHandleStatusUpdateErr(SStmGrpCtx* pCtx, EStmErrType err, SStmTaskStatusMsg* pStatus) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStreamTask* pTask = (SStreamTask*)pStatus;
  int64_t streamId = pStatus->streamId;

  ST_TASK_ILOG("start to handle task status update error: %d", err);
  
  // STREAMTODO

  if (STM_ERR_TASK_NOT_EXISTS == err) {
    TAOS_CHECK_EXIT(msmGrpAddActionUndeploy(pCtx, streamId, pTask));
  }

_exit:

  if (code) {
    ST_TASK_ELOG("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
}

int32_t msmHandleStreamStatusUpdate(SStmGrpCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t num = taosArrayGetSize(pCtx->pReq->pStreamStatus);

  stDebug("start to handle stream group %d tasks status, taskNum:%d", pCtx->pReq->streamGId, num);

  for (int32_t i = 0; i < num; ++i) {
    SStmTaskStatusMsg* pStatus = taosArrayGet(pCtx->pReq->pStreamStatus, i);
    SStmTaskStatus** ppStatus = taosHashGet(mStreamMgmt.taskMap, &pStatus->streamId, sizeof(pStatus->streamId) + sizeof(pStatus->taskId));
    if (NULL == ppStatus) {
      msmHandleStatusUpdateErr(pCtx, STM_ERR_TASK_NOT_EXISTS, pStatus);
      continue;
    }
    
    (*ppStatus)->status = pStatus->status;
    (*ppStatus)->lastUpTs = pCtx->currTs;
    
    if (STREAM_STATUS_RUNNING != pStatus->status) {
      TAOS_CHECK_EXIT(msmHandleTaskAbnormalStatus(pCtx, pStatus));
    }
  }

_exit:

  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
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
  start.task.nodeId = id->nodeId;
  start.task.taskIdx = id->taskIdx;

  TSDB_CHECK_NULL(taosArrayPush(pRsp->start.taskList, &start), code, lino, _exit, terrno);

  mstDebug("stream START added to dnode %d hb rsp, triggerTaskId:%"PRId64, id->nodeId, id->taskId);

  return;

_exit:

  mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
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

    ST_TASK_DLOG("task UNDEPLOY added to hb rsp, doCheckpoint:%d, doCleanup:%d", undeploy.undeployMsg.doCheckpoint, undeploy.undeployMsg.doCleanup);
  }

  return;

_exit:

  mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
}


int32_t msmHandleHbPostActions(SStmGrpCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void* pIter = NULL;
  int32_t streamNum = taosHashGetSize(pCtx->actionStm);

  stDebug("start to handle stream group %d post actions", pCtx->pReq->streamGId);

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

    if (STREAM_ACT_START & pAction->actions && msmCheckStreamStartCond(*pStreamId, pCtx->pReq->snodeId, &id)) {
      msmRspAddStreamStart(*pStreamId, pCtx->pRsp, streamNum, pAction, &id);
    }
  }
  
_exit:

  if (pIter) {
    taosHashCancelIterate(pCtx->actionStm, pIter);
  }

  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
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
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;  
}

int32_t msmHandleNormalHbMsg(SStmGrpCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStreamHbMsg* pReq = pCtx->pReq;

  TAOS_CHECK_EXIT(msmCheckUpdateDnodeTs(pCtx));
  
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
    TAOS_CHECK_EXIT(msmHandleStreamStatusUpdate(pCtx));
  }

  if (taosHashGetSize(pCtx->actionStm) > 0 && GOT_SNODE(pReq->snodeId)) {
    TAOS_CHECK_EXIT(msmHandleHbPostActions(pCtx));
  }

_exit:

  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


int32_t msmHandleCleanupHbMsg(SStmGrpCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TAOS_CHECK_EXIT(msmCheckUpdateDnodeTs(pCtx));
  
  if (taosArrayGetSize(pCtx->pReq->pStreamStatus) > 0) {
    pCtx->pRsp->undeploy.undeployAll = 1;
  }

_exit:

  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


int32_t msmHandleStreamHbMsg(SMnode* pMnode, int64_t currTs, SStreamHbMsg* pHb, SMStreamHbRspMsg* pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (taosRTryLockLatch(&mStreamMgmt.runtimeLock)) {
    stInfo("stream runtimeLocked, ignore hb req from grp %d", pHb->streamGId);
    return code;
  }

  int32_t tidx = streamGetThreadIdx(mStreamMgmt.threadNum, pHb->streamGId);
  SStmGrpCtx* pCtx = &mStreamMgmt.tCtx[tidx].grpCtx[pHb->streamGId];

  pCtx->tidx = tidx;
  pCtx->pMnode = pMnode;
  pCtx->currTs = currTs;
  pCtx->pReq = pHb;
  pCtx->pRsp = pRsp;
  pCtx->deployStm = mStreamMgmt.tCtx[pCtx->tidx].deployStm[pHb->streamGId];
  pCtx->actionStm = mStreamMgmt.tCtx[pCtx->tidx].actionStm[pHb->streamGId];
  
  switch (mStreamMgmt.phase) {
    case MND_STM_PHASE_WATCH:
      break;
    case MND_STM_PHASE_NORMAL:
      code = msmHandleNormalHbMsg(pCtx);
      break;
    case MND_STM_PHASE_CLEANUP:
      code = msmHandleCleanupHbMsg(pCtx);
      break;
    default:
      stError("Invalid stream phase: %d", mStreamMgmt.phase);
      code = TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
      break;
  }

  taosRUnLockLatch(&mStreamMgmt.runtimeLock);

  return code;
}

void msmHandleBecomeLeader(SMnode *pMnode) {
  taosWLockLatch(&mStreamMgmt.runtimeLock);

  if (mStreamMgmt.initialized) {
    msmDestroyRuntimeInfo(pMnode);
  }

  msmInitRuntimeInfo(pMnode);

  taosWUnLockLatch(&mStreamMgmt.runtimeLock);
}

void msmHandleBecomeNotLeader(SMnode *pMnode) {
  taosWLockLatch(&mStreamMgmt.runtimeLock);
  
  mStreamMgmt.isLeader = false;

  if (mStreamMgmt.initialized) {
    msmDestroyRuntimeInfo(pMnode);
  }

  taosWUnLockLatch(&mStreamMgmt.runtimeLock);
}


void msmHealthCheck(SMnode *pMnode) {

}

int32_t msmInitRuntimeInfo(SMnode *pMnode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t vnodeNum = sdbGetSize(pMnode->pSdb, SDB_VGROUP);
  int32_t snodeNum = sdbGetSize(pMnode->pSdb, SDB_SNODE);
  int32_t dnodeNum = sdbGetSize(pMnode->pSdb, SDB_DNODE);

  mStreamMgmt.startTs = taosGetTimestampMs();
  mStreamMgmt.threadNum = tsNumOfMnodeStreamMgmtThreads;
  mStreamMgmt.tCtx = taosMemoryCalloc(mStreamMgmt.threadNum, sizeof(SStmThreadCtx));
  if (NULL == mStreamMgmt.tCtx) {
    code = terrno;
    stError("failed to initialize the stream runtime tCtx, threadNum:%d, error:%s", mStreamMgmt.threadNum, tstrerror(code));
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

  mStreamMgmt.lastTaskId = 1;
  mStreamMgmt.initialized = true;
  mStreamMgmt.phase = MND_STM_PHASE_NORMAL;

  //taosHashSetFreeFp(mStreamMgmt.nodeMap, freeTaskList);
  //taosHashSetFreeFp(mStreamMgmt.streamMap, freeTaskList);


_exit:

  if (code) {
    msmDestroyRuntimeInfo(pMnode);
  }

  return code;
}



