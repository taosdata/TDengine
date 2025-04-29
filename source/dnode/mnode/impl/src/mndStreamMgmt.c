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

static int32_t msmAddTaskToTaskMap(SMnode* pMnode, SStreamObj* pStream, SArray* pTasks, SStmTaskStatus* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t taskNum = pTasks ? taosArrayGetSize(pTasks) : 1;
  int64_t key[2] = {pStream->pCreate->streamId, 0};
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskStatus* pStatus = pTasks ? taosArrayGet(pTasks, i) : pTask;
    key[1] = pStatus->id.taskId;
    TAOS_CHECK_EXIT(taosHashPut(mStreamMgmt.taskMap, key, sizeof(key), &pStatus, POINTER_BYTES));
  }
  
_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}

static int32_t msmAddTaskToVgroupMapImpl(SHashObj* pVgMap, SStmTaskStatus* pState, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStmVgroupTasksStatus vg = {0};

  while (true) {
    SStmVgroupTasksStatus* pVg = taosHashAcquire(pVgMap, &pState->id.nodeId, sizeof(pState->id.nodeId));
    if (NULL == pVg) {
      vg.taskList = taosArrayInit(20, POINTER_BYTES);
      TSDB_CHECK_NULL(vg.taskList, code, lino, _return, terrno);
      TSDB_CHECK_NULL(taosArrayPush(vg.taskList, &pState), code, lino, _return, terrno);
      code = taosHashPut(pVgMap, &pState->id.nodeId, sizeof(pState->id.nodeId), &pState, POINTER_BYTES);
      if (TSDB_CODE_SUCCESS == code) {
        return code;
      }

      if (TSDB_CODE_DUP_KEY != code) {
        goto _return;
      }    

      taosArrayDestroy(vg.taskList);
      continue;
    }

    taosWLockLatch(&pVg->lock);
    if (NULL == taosArrayPush(pVg->taskList, &pState)) {
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
  }

  return code;
}

static int32_t msmAddTaskToVgroupDeployMap(SHashObj* pVgMap, SStmTaskDeploy* pDeploy, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStmVgroupTasksDeploy vg = {0};
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
        return code;
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
  }

  return code;
}


static int32_t msmAddTaskToSnodeMapImpl(SHashObj* pHash, SStmTaskStatus* pState, SStreamObj* pStream, bool triggerTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStmSnodeTasksStatus snode = {0};

  while (true) {
    SStmSnodeTasksStatus* pSnode = taosHashAcquire(pHash, &pState->id.nodeId, sizeof(pState->id.nodeId));
    if (NULL == pSnode) {
      if (triggerTask) {
        snode.triggerList = taosArrayInit(20, POINTER_BYTES);
        TSDB_CHECK_NULL(snode.triggerList, code, lino, _return, terrno);
        TSDB_CHECK_NULL(taosArrayPush(snode.triggerList, &pState), code, lino, _return, terrno);
      } else {
        snode.runnerList = taosArrayInit(20, POINTER_BYTES);
        TSDB_CHECK_NULL(snode.runnerList, code, lino, _return, terrno);
        TSDB_CHECK_NULL(taosArrayPush(snode.runnerList, &pState), code, lino, _return, terrno);
      }
      
      code = taosHashPut(pHash, &pState->id.nodeId, sizeof(pState->id.nodeId), &snode, sizeof(snode));
      if (TSDB_CODE_SUCCESS == code) {
        return code;
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
      *ppList = taosArrayInit(20, POINTER_BYTES);
      if (NULL == *ppList) {
        code = terrno;
        taosWUnLockLatch(&pSnode->lock);        
        TSDB_CHECK_NULL(*ppList, code, lino, _return, code);
      }
    }
    if (NULL == taosArrayPush(*ppList, &pState)) {
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
  }

  return code;
}



static int32_t msmAddTaskToSnodeDeploy(SHashObj* pHash, SStmTaskDeploy* pDeploy, SStreamObj* pStream, bool triggerTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStmSnodeTasksDeploy snode = {0};
  SStmTaskDeployExt ext;
  SArray** ppList = triggerTask ? &snode.triggerList : &snode.runnerList;
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
        return code;
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
  }

  return code;
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
    runner.addr.epset = mndGetVgroupEpsetById(pMnode, pStatus->id.nodeId);
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
  TAOS_CHECK_EXIT(nodesCloneNode((SNode*)plan, (SNode**)&pMsg->pPlan));
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


static int32_t msmAddTaskToVgroupMap(SMnode* pMnode, SStreamObj* pStream, SArray* pTasks) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t taskNum = taosArrayGetSize(pTasks);
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskStatus* pState = taosArrayGet(pTasks, i);
    TSDB_CHECK_CODE(msmAddTaskToVgroupMapImpl(mStreamMgmt.vgroupMap, pState, pStream), lino, _return);
    atomic_add_fetch_32(&mStreamMgmt.toDeployVgTaskNum, 1);
  }
  
_return:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}


static int32_t msmAddTaskToSnodeMaps(SMnode* pMnode, SStreamObj* pStream, SArray* pTasks, SStmTaskStatus* pTask, bool triggerTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t taskNum = triggerTask ? 1 : taosArrayGetSize(pTasks);
  int32_t taskType = triggerTask ? STREAM_TRIGGER_TASK : STREAM_RUNNER_TASK;
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskStatus* pState = triggerTask ? pTask : taosArrayGet(pTasks, i);
    TSDB_CHECK_CODE(msmAddTaskToSnodeMapImpl(mStreamMgmt.snodeMap, pState, pStream, triggerTask), lino, _return);
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

  pInfo->triggerTask.id.taskId = msmAssignTaskId();
  pInfo->triggerTask.id.nodeId = msmAssignTaskSnodeId(pMnode, pStream->pCreate->streamId, true);
  pInfo->triggerTask.id.taskIdx = 0;
  pInfo->triggerTask.lastUpTs = INT64_MIN;

  SStmTaskDeploy info = {0};
  info.task.type = STREAM_TRIGGER_TASK;
  info.task.streamId = streamId;
  info.task.taskId =  pInfo->triggerTask.id.taskId;
  info.task.nodeId =  pInfo->triggerTask.id.nodeId;
  info.task.taskIdx =  pInfo->triggerTask.id.taskIdx;
  TSDB_CHECK_CODE(msmBuildTriggerDeployInfo(pMnode, pInfo, &info, pStream), lino, _return);
  TSDB_CHECK_CODE(msmAddTaskToSnodeDeploy(mStreamMgmt.toDeploySnodeMap, &info, pStream, true), lino, _return);
  
  atomic_add_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);

  TSDB_CHECK_CODE(msmAddTaskToTaskMap(pMnode, pStream, NULL, &pInfo->triggerTask), lino, _return);
  TSDB_CHECK_CODE(msmAddTaskToSnodeMaps(pMnode, pStream, NULL, &pInfo->triggerTask, true), lino, _return);

_return:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmAppendReaderTriggerTasks(SMnode* pMnode, SStmStatus* pInfo, SStreamObj* pStream, int32_t* taskIdx) {
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
      TSDB_CHECK_CODE(msmAddTaskToVgroupDeployMap(mStreamMgmt.toDeployVgMap, &info, pStream), lino, _return);
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
          TSDB_CHECK_CODE(msmAddTaskToVgroupDeployMap(mStreamMgmt.toDeployVgMap, &info, pStream), lino, _return);
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

int32_t msmAddRunnerToUpdateMap(SMnode* pMnode, SStreamObj* pStream, char* scanPlan, int32_t vgId, int64_t taskId) {
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
  
  SNode* pNode = NULL;
  FOREACH(pNode, pSubplan->pParents) {
    if (QUERY_NODE_VALUE != nodeType(pNode)) {
      mstError("invalid node type %d for reader's parent subplan", nodeType(pNode));
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }
    SValueNode* pVal = (SValueNode*)pNode;
    if (TSDB_DATA_TYPE_BIGINT != pVal->node.resType.type) {
      mstError("invalid value node data type %d for reader's parent subplan", pVal->node.resType.type);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }

    key[1] = MND_GET_RUNNER_SUBPLANID(pVal->datum.i);

    TAOS_CHECK_EXIT(taosHashPut(mStreamMgmt.toUpdateRunnerMap, key, sizeof(key), &addr, sizeof(addr)));

    atomic_add_fetch_32(&mStreamMgmt.toUpdateRunnerNum, 1);
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmAppendReaderRunnerTasks(SMnode* pMnode, SStmStatus* pInfo, SStreamObj* pStream, int32_t* taskIdx) {
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
      TSDB_CHECK_CODE(msmAddRunnerToUpdateMap(pMnode, pStream, pScanList->scanPlan, state.id.nodeId, state.id.taskId), lino, _return);
      TSDB_CHECK_CODE(msmAddTaskToVgroupDeployMap(mStreamMgmt.toDeployVgMap, &info, pStream), lino, _return);
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
  
  TSDB_CHECK_CODE(msmAppendReaderTriggerTasks(pMnode, pInfo, pStream, &taskIdx), lino, _exit);
  TSDB_CHECK_CODE(msmAppendReaderRunnerTasks(pMnode, pInfo, pStream, &taskIdx), lino, _exit);

  TSDB_CHECK_CODE(msmAddTaskToTaskMap(pMnode, pStream, pInfo->readerList, NULL), lino, _exit);
  TSDB_CHECK_CODE(msmAddTaskToVgroupMap(pMnode, pStream, pInfo->readerList), lino, _exit);
  
_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}

int32_t msmUpdatePlanSourceAddr(SSubplan* plan, int64_t clientId, SStmTaskSrcAddr* pSrc) {
  SDownstreamSourceNode source = {
      .type = QUERY_NODE_DOWNSTREAM_SOURCE,
      .clientId = clientId,
      .taskId = pSrc->taskId,
      .sId = 0,
      .execId = 0,
      .fetchMsgType = TDMT_STREAM_FETCH,
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

int32_t msmUpdateRunnerPlan(SMnode* pMnode, SArray* pRunners, int32_t beginIdx, SStmTaskDeploy* pDeploy, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SSubplan* pPlan = pDeploy->msg.runner.pPlan;
  int64_t key[2] = {pStream->pCreate->streamId, pPlan->id.subplanId};
  int64_t streamId = pStream->pCreate->streamId;

  SStmTaskSrcAddr* pAddr = taosHashGet(mStreamMgmt.toUpdateRunnerMap, key, sizeof(key));
  if (NULL != pAddr) {
    TAOS_CHECK_EXIT(msmUpdatePlanSourceAddr(pPlan, pDeploy->task.taskId, pAddr));
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
    TAOS_CHECK_EXIT(msmUpdatePlanSourceAddr(pSubplan, parentTaskId, &addr));
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
    TAOS_CHECK_EXIT(msmUpdateRunnerPlan(pMnode, pRunners, i, &pExt->deploy, pStream));
    TAOS_CHECK_EXIT(nodesNodeToString((SNode*)pExt->deploy.msg.runner.pPlan, false, (char**)&pExt->deploy.msg.runner.pPlan, NULL));
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

        TSDB_CHECK_CODE(msmAddTaskToSnodeDeploy(mStreamMgmt.toDeploySnodeMap, &info, pStream, false), lino, _exit);
        
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
  }

  TSDB_CHECK_CODE(msmAddTaskToTaskMap(pMnode, pStream, pInfo->runnerList, NULL), lino, _exit);
  TSDB_CHECK_CODE(msmAddTaskToSnodeMaps(pMnode, pStream, pInfo->runnerList, NULL, false), lino, _exit);

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

static int32_t msmAppendNewSnodesToMap(SMnode* pMnode) {
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

static int32_t msmAppendNewDnodesToMap(SMnode* pMnode) {
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


static int32_t msmAddStreamTasksToMap(SMnode* pMnode, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStmStatus info = {0};
  info.lastActTs = INT64_MIN;
  
  TSDB_CHECK_CODE(msmBuildStreamTasks(pMnode, &info, pStream), lino, _return);

  TSDB_CHECK_CODE(taosHashPut(mStreamMgmt.streamMap, &streamId, sizeof(streamId), &info, sizeof(info)), lino, _return);

_return:

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

  TAOS_CHECK_EXIT(mndAcquireStream(pMnode, streamName, &pStream));

  TAOS_CHECK_EXIT(msmAddStreamTasksToMap(pMnode, pStream));


_exit:

  mndReleaseStream(pMnode, pStream);

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmLaunchStreamDropAction(SRpcMsg *pReq) {
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t streamId = *(int64_t*)pReq->pCont;
  char* streamName = (char*)pReq->pCont + sizeof(streamId);

  //STREAMTODO

  return code;
}

static int32_t msmHandleStreamActions(SMnode* pMnode, SStmActionQ* pQ) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmQNode* pQNode = NULL;

  stDebug("start to handle stream actions");
  
  while (mndStreamActionDequeue(pQ, &pQNode)) {
    switch (pQNode->action) {
      case STREAM_ACT_DEPLOY:
        TAOS_CHECK_EXIT(msmLaunchStreamDepolyAction(pMnode, pQNode));
        break;
      case STREAM_ACT_UNDEPLOY:
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
        pStream->readerTasks = taosArrayInit(20, POINTER_BYTES);
        TSDB_CHECK_NULL(pStream->readerTasks, code, lino, _exit, terrno);
      }
      
      TSDB_CHECK_NULL(taosArrayPush(pStream->readerTasks, &pDeploy), code, lino, _exit, terrno);
      break;
    case STREAM_TRIGGER_TASK:
      pStream->triggerTask = pDeploy;
      break;
    case STREAM_RUNNER_TASK:
      if (NULL == pStream->runnerTasks) {
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

static int32_t msmAddTaskToStreamDeployMap(SHashObj* pHash, SStmTaskDeploy* pDeploy) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pDeploy->task.streamId;
  SStmStreamDeploy streamDeploy = {0};
  SStmStreamDeploy* pStream = NULL;
   
  while (true) {
    pStream = taosHashAcquire(pHash, &streamId, sizeof(streamId));
    if (NULL == pStream) {
      TAOS_CHECK_EXIT(msmInitStreamDeploy(&streamDeploy, pDeploy));
      code = taosHashPut(pHash, &streamId, sizeof(streamId), &streamDeploy, sizeof(streamDeploy));
      if (TSDB_CODE_SUCCESS == code) {
        return code;
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
  }

  return code;
}


int32_t msmAddGrpDeployTasks(SHashObj* pHash, SArray* pTasks, int32_t* deployed) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t taskNum = taosArrayGetSize(pTasks);

  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskDeployExt* pExt = taosArrayGet(pTasks, i);
    if (pExt->deployed) {
      continue;
    }

    TAOS_CHECK_EXIT(msmAddTaskToStreamDeployMap(pHash, &pExt->deploy));
    pExt->deployed = true;

    atomic_add_fetch_32(deployed, 1);
  }

_exit:

  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmAddGrpDeployVgTasks(SMnode* pMnode, SArray* pVgLeaders, int32_t gid) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t vgNum = taosArrayGetSize(pVgLeaders);
  SStmVgroupTasksDeploy* pVg = NULL;
  int32_t tidx = streamGetThreadIdx(mStreamMgmt.threadNum, gid);

  stDebug("start to add stream vgroup tasks deploy");
  
  for (int32_t i = 0; i < vgNum; ++i) {
    int32_t* vgId = taosArrayGet(pVgLeaders, i);
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
    
    TAOS_CHECK_EXIT(msmAddGrpDeployTasks(mStreamMgmt.tCtx[tidx].deployStm[gid], pVg->taskList, &pVg->deployed));
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

int32_t msmAddGrpDeploySnodeTasks(SMnode* pMnode, int32_t snodeId, int32_t gid) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmSnodeTasksDeploy* pSnode = NULL;
  int32_t tidx = streamGetThreadIdx(mStreamMgmt.threadNum, gid);

  stDebug("start to add stream snode tasks deploy");
  
  pSnode = taosHashAcquire(mStreamMgmt.toDeploySnodeMap, &snodeId, sizeof(snodeId));
  if (NULL == pSnode) {
    return TSDB_CODE_SUCCESS;
  }

  if (taosRTryLockLatch(&pSnode->lock)) {
    return TSDB_CODE_SUCCESS;
  }
  
  if (atomic_load_32(&pSnode->triggerDeployed) < taosArrayGetSize(pSnode->triggerList)) {
    TAOS_CHECK_EXIT(msmAddGrpDeployTasks(mStreamMgmt.tCtx[tidx].deployStm[gid], pSnode->triggerList, &pSnode->triggerDeployed));
  }

  if (atomic_load_32(&pSnode->runnerDeployed) < taosArrayGetSize(pSnode->runnerList)) {
    TAOS_CHECK_EXIT(msmAddGrpDeployTasks(mStreamMgmt.tCtx[tidx].deployStm[gid], pSnode->runnerList, &pSnode->runnerDeployed));
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
  TSDB_CHECK_NULL(pStatus, code, lino, _exit, terrno);
  
  pStatus->lastActTs = currTs;

_exit:

  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmAddHbRspDeploy(SHashObj* pStreams, int32_t gid, int64_t currTs, SMStreamHbRspMsg* pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t streamNum = taosHashGetSize(pStreams);
  void* pIter = NULL;

  stDebug("start to add stream group %d deploy tasks, taskNum:%d", gid, taosHashGetSize(pStreams));
  
  pRsp->deploy.streamList = taosArrayInit(streamNum, sizeof(SStmStreamDeploy));
  TSDB_CHECK_NULL(pRsp->deploy.streamList, code, lino, _exit, terrno);

  while (1) {
    pIter = taosHashIterate(pStreams, pIter);
    if (pIter == NULL) {
      break;
    }
    
    SStmStreamDeploy *pDeploy = (SStmStreamDeploy *)pIter;
    TSDB_CHECK_NULL(taosArrayPush(pRsp->deploy.streamList, pDeploy), code, lino, _exit, terrno);
    TAOS_CHECK_EXIT(msmUpdateStreamLastActTs(pDeploy->streamId, currTs));
  }
  
_exit:

  if (pIter) {
    taosHashCancelIterate(pStreams, pIter);
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

void msmCleanStreamGrpCtx(SStreamHbMsg* pHb) {
  if (atomic_load_32(&mStreamMgmt.toDeployVgTaskNum) > 0) {
    msmCleanDeployedVgTasks(pHb->pVgLeaders);
  }

  if (atomic_load_32(&mStreamMgmt.toDeploySnodeTaskNum) > 0) {
    msmCleanDeployedSnodeTasks(pHb->snodeId);
  }

  int32_t tidx = streamGetThreadIdx(mStreamMgmt.threadNum, pHb->streamGId);
  taosHashClear(mStreamMgmt.tCtx[tidx].actionStm[pHb->streamGId]);
  taosHashClear(mStreamMgmt.tCtx[tidx].deployStm[pHb->streamGId]);
}

void msmHandleTaskAbnormalStatus(int32_t tidx, int32_t gid, SStmTaskStatusMsg* pMsg, int64_t currTs) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t action = 0;
  int64_t streamId = pMsg->streamId;
  SHashObj* pHash = mStreamMgmt.tCtx[tidx].actionStm[gid];
  SStreamTask* pTask = (SStreamTask*)pMsg;

  ST_TASK_DLOG("start to handle task abnormal status %d", pTask->status);
  
  SStmStatus* pStatus = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStatus) {
    ST_TASK_ELOG("stream no longer exists in streamMap, try to undeploy it, idx:%d", pMsg->taskIdx);
    action = STREAM_ACT_UNDEPLOY;
    TAOS_CHECK_EXIT(taosHashPut(pHash, &streamId, sizeof(streamId), &action, sizeof(action)));
  }
  
  switch (pMsg->status) {
    case STREAM_STATUS_INIT:
      if ((currTs - pStatus->lastActTs) < STREAM_ACT_MIN_DELAY_MSEC) {
        ST_TASK_DLOG("task wait not enough between actions, currTs:%" PRId64 ", lastTs:%" PRId64, currTs, pStatus->lastActTs);
        return;
      }

      int32_t *pAction = taosHashGet(pHash, &streamId, sizeof(streamId));
      if (pAction) {
        *pAction |= STREAM_ACT_START;
        ST_TASK_DLOG("task append START action, action:%x", *pAction);
      } else {
        action = STREAM_ACT_START;
        TAOS_CHECK_EXIT(taosHashPut(pHash, &streamId, sizeof(streamId), &action, sizeof(action)));
        ST_TASK_DLOG("task add START action, action:%x", action);
      }
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
}

void msmHandleStatusUpdateErr(int32_t tidx, int32_t gid, EStmStatusErrType err, SStmTaskStatusMsg* pStatus) {
  SStreamTask* pTask = (SStreamTask*)pStatus;

  ST_TASK_ILOG("start to handle task status update errot: %d", err);
  
  // STREAMTODO
}

int32_t msmHandleStreamStatusUpdate(SMnode* pMnode, int32_t tidx, int32_t gid, int64_t currTs, SArray* pStatusList) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t num = taosArrayGetSize(pStatusList);

  stDebug("start to handle stream group %d tasks status, taskNum:%d", gid, num);

  for (int32_t i = 0; i < num; ++i) {
    SStmTaskStatusMsg* pStatus = taosArrayGet(pStatusList, i);
    SStmTaskStatus** ppStatus = taosHashGet(mStreamMgmt.taskMap, &pStatus->streamId, sizeof(pStatus->streamId) + sizeof(pStatus->taskId));
    if (NULL == ppStatus) {
      msmHandleStatusUpdateErr(tidx, gid, STM_ERR_TASK_NOT_EXISTS, pStatus);
      continue;
    }
    
    (*ppStatus)->status = pStatus->status;
    (*ppStatus)->lastUpTs = currTs;
    
    if (STREAM_STATUS_RUNNING != pStatus->status) {
      msmHandleTaskAbnormalStatus(tidx, gid, pStatus, currTs);
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

  if (pStream->triggerTask.id.nodeId != snodeId || STREAM_STATUS_INIT != pStream->triggerTask.status) {
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

  *id = pStream->triggerTask.id;
  
  return true;
}

void msmAddHbRspStart(int64_t streamId, SMStreamHbRspMsg* pRsp, int32_t streamNum, SStmTaskId* id) {
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

  return;

_exit:

  mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
}

int32_t msmHandleStreamUpdatePostActions(SMnode* pMnode, int32_t tidx, int32_t gid, int32_t snodeId, SMStreamHbRspMsg* pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void* pIter = NULL;
  int32_t streamNum = taosHashGetSize(mStreamMgmt.tCtx[tidx].actionStm[gid]);
  SHashObj* pHash = mStreamMgmt.tCtx[tidx].actionStm[gid];

  stDebug("start to handle stream group %d post actions", gid);

  while (1) {
    pIter = taosHashIterate(pHash, pIter);
    if (pIter == NULL) {
      break;
    }

    int64_t* pStreamId = taosHashGetKey(pIter, NULL);
    int32_t *actions = (int32_t *)pIter;
    
    SStmTaskId id = {0};
    if (STREAM_ACT_START == *actions && msmCheckStreamStartCond(*pStreamId, snodeId, &id)) {
      msmAddHbRspStart(*pStreamId, pRsp, streamNum, &id);
    }
  }
  
_exit:

  if (pIter) {
    taosHashCancelIterate(pHash, pIter);
  }

  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmCheckUpdateDnodeTs(SMnode* pMnode, int64_t currTs, int32_t dnodeId) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int64_t* lastTs = NULL;
  bool     noExists = false;

  while (true) {
    lastTs = taosHashGet(mStreamMgmt.dnodeMap, &dnodeId, sizeof(dnodeId));
    if (NULL == lastTs) {
      if (noExists) {
        TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
      }

      noExists = true;
      TAOS_CHECK_EXIT(msmAppendNewDnodesToMap(pMnode));
      
      continue;
    }

    while (true) {
      int64_t lastTsValue = atomic_load_64(lastTs);
      if (currTs > lastTsValue) {
        if (lastTsValue == atomic_val_compare_exchange_64(lastTs, lastTsValue, currTs)) {
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

int32_t msmHandleNormalHbMsg(SMnode* pMnode, int64_t currTs, SStreamHbMsg* pHb, SMStreamHbRspMsg* pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t tidx = streamGetThreadIdx(mStreamMgmt.threadNum, pHb->streamGId);

  TAOS_CHECK_EXIT(msmCheckUpdateDnodeTs(pMnode, currTs, pHb->dnodeId));
  
  if (atomic_load_64(&mStreamMgmt.tCtx[tidx].actionQ->qRemainNum) > 0) {
    TAOS_CHECK_EXIT(msmHandleStreamActions(pMnode, mStreamMgmt.tCtx[tidx].actionQ));
  }

  if (atomic_load_32(&mStreamMgmt.toDeployVgTaskNum) > 0) {
    TAOS_CHECK_EXIT(msmAddGrpDeployVgTasks(pMnode, pHb->pVgLeaders, pHb->streamGId));
  }

  if (atomic_load_32(&mStreamMgmt.toDeploySnodeTaskNum) > 0 && GOT_SNODE(pHb->snodeId)) {
    TAOS_CHECK_EXIT(msmAddGrpDeploySnodeTasks(pMnode, pHb->snodeId, pHb->streamGId));
  }

  if (taosHashGetSize(mStreamMgmt.tCtx[tidx].deployStm[pHb->streamGId]) > 0) {
    TAOS_CHECK_EXIT(msmAddHbRspDeploy(mStreamMgmt.tCtx[tidx].deployStm[pHb->streamGId], pHb->streamGId, currTs, pRsp));
  }

  if (taosArrayGetSize(pHb->pStreamStatus) > 0) {
    TAOS_CHECK_EXIT(msmHandleStreamStatusUpdate(pMnode, tidx, pHb->streamGId, currTs, pHb->pStreamStatus));
  }

  if (taosHashGetSize(mStreamMgmt.tCtx[tidx].actionStm[pHb->streamGId]) > 0 && GOT_SNODE(pHb->snodeId)) {
    TAOS_CHECK_EXIT(msmHandleStreamUpdatePostActions(pMnode, tidx, pHb->streamGId, pHb->snodeId, pRsp));
  }

_exit:

  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


int32_t msmHandleCleanupHbMsg(SMnode* pMnode, int64_t currTs, SStreamHbMsg* pHb, SMStreamHbRspMsg* pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t tidx = streamGetThreadIdx(mStreamMgmt.threadNum, pHb->streamGId);

  TAOS_CHECK_EXIT(msmCheckUpdateDnodeTs(pMnode, currTs, pHb->dnodeId));
  
  if (taosArrayGetSize(pHb->pStreamStatus) > 0) {
    pRsp->undeploy.undeployAll = 1;
  }

_exit:

  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


int32_t msmHandleStreamHbMsg(SMnode* pMnode, int64_t currTs, SStreamHbMsg* pHb, SMStreamHbRspMsg* pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  
  switch (mStreamMgmt.phase) {
    case MND_STM_PHASE_WATCH:
      break;
    case MND_STM_PHASE_NORMAL:
      return msmHandleNormalHbMsg(pMnode, currTs, pHb, pRsp);
    case MND_STM_PHASE_CLEANUP:
      return msmHandleCleanupHbMsg(pMnode, currTs, pHb, pRsp);
    default:
      stError("Invalid stream phase: %d", mStreamMgmt.phase);
      return TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
  }

  return code;
}

void msmHandleBecomeLeader(SMnode *pMnode) {
  if (mStreamMgmt.initialized) {
    msmDestroyRuntimeInfo(pMnode);
  }

  msmInitRuntimeInfo(pMnode);
}

void msmHandleBecomeNotLeader(SMnode *pMnode) {
  mStreamMgmt.isLeader = false;
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
  
  for (int32_t i = 0; i < mStreamMgmt.threadNum; ++i) {
    SStmThreadCtx* pCtx = mStreamMgmt.tCtx + i;
    pCtx->actionQ = taosMemoryCalloc(1, sizeof(SStmActionQ));
    if (pCtx->actionQ == NULL) {
      code = terrno;
      mError("failed to initialize the stream runtime actionQ[%d], error:%s", i, tstrerror(code));
      goto _exit;
    }

    pCtx->actionQ->head = taosMemoryCalloc(1, sizeof(SStmQNode));
    TSDB_CHECK_NULL(pCtx->actionQ->head, code, lino, _exit, terrno);
    
    pCtx->actionQ->tail = pCtx->actionQ->head;

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

  TAOS_CHECK_EXIT(msmAppendNewSnodesToMap(pMnode));
  TAOS_CHECK_EXIT(msmAppendNewDnodesToMap(pMnode));

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



