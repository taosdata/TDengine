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
#include "mndScheduler.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndTrans.h"
#include "osMemory.h"
#include "parser.h"
#include "taoserror.h"
#include "tmisce.h"
#include "tname.h"

void msmDestroyRuntimeInfo(SMnode *pMnode) {
  // TODO

  memset(&mStreamMgmt, 0, sizeof(mStreamMgmt));
}

int32_t msmInitRuntimeInfo(SMnode *pMnode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t vnodeNum = sdbGetSize(pMnode->pSdb, SDB_VGROUP);
  int32_t snodeNum = sdbGetSize(pMnode->pSdb, SDB_SNODE);
  int32_t dnodeNum = sdbGetSize(pMnode->pSdb, SDB_DNODE);

  mStreamMgmt.threadNum = tsNumOfMnodeStreamMgmtThreads;
  mStreamMgmt.tCtx = taosMemoryCalloc(mStreamMgmt.threadNum, sizeof(SStmThreadCtx));
  if (NULL == mStreamMgmt.tCtx) {
    code = terrno;
    mError("failed to initialize the stream runtime tCtx, threadNum:%d, error:%s", (mStreamMgmt.threadNum, tstrerror(code));
    goto _return;
  }
  
  for (int32_t i = 0; i < mStreamMgmt.threadNum; ++i) {
    SStmThreadCtx* pCtx = mStreamMgmt.tCtx + i;
    pCtx->actionQ = taosMemoryCalloc(1, sizeof(SStmActionQ));
    if (pCtx->actionQ == NULL) {
      code = terrno;
      mError("failed to initialize the stream runtime actionQ[%d], error:%s", i, tstrerror(code));
      goto _return;
    }

    for (int32_t m = 0; m < STREAM_MAX_GROUP_NUM; ++m) {
      pCtx->deployStm[m] = taosHashInit(snodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
      if (pCtx->deployStm[m] == NULL) {
        code = terrno;
        mError("failed to initialize the stream runtime deployStm[%d][%d], error:%s", i, m, tstrerror(code));
        goto _return;
      }
      pCtx->actionStm[m] = taosHashInit(snodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
      if (pCtx->actionStm[m] == NULL) {
        code = terrno;
        mError("failed to initialize the stream runtime actionStm[%d][%d], error:%s", i, m, tstrerror(code));
        goto _return;
      }
    }
  }
  
  mStreamMgmt.streamMap = taosHashInit(MND_STREAM_DEFAULT_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.streamMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime streamMap, error:%s", tstrerror(code));
    goto _return;
  }
  mStreamMgmt.taskMap = taosHashInit(MND_STREAM_DEFAULT_TASK_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.taskMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime taskMap, error:%s", tstrerror(code));
    goto _return;
  }
  mStreamMgmt.vgroupMap = taosHashInit(vnodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.vgroupMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime vgroupMap, error:%s", tstrerror(code));
    goto _return;
  }
  mStreamMgmt.snodeMap = taosHashInit(snodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.snodeMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime snodeMap, error:%s", tstrerror(code));
    goto _return;
  }
  mStreamMgmt.dnodeMap = taosHashInit(dnodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.dnodeMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime dnodeMap, error:%s", tstrerror(code));
    goto _return;
  }

  mStreamMgmt.toDeployVgMap = taosHashInit(vnodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.toDeployVgMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime toDeployVgMap, error:%s", tstrerror(code));
    goto _return;
  }
  mStreamMgmt.toDeploySnodeMap = taosHashInit(snodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.toDeploySnodeMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime toDeploySnodeMap, error:%s", tstrerror(code));
    goto _return;
  }

  mStreamMgmt.lastTaskId = 1;
  mStreamMgmt.initialized = true;

  //taosHashSetFreeFp(mStreamMgmt.nodeMap, freeTaskList);
  //taosHashSetFreeFp(mStreamMgmt.streamMap, freeTaskList);


_return:

  if (code) {
    msmDestroyRuntimeInfo(pMnode);
  }

  return code;
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
    TSDB_CHECK_CODE(taosHashPut(mStreamMgmt.taskMap, key, sizeof(key), &pStatus, POINTER_BYTES), lino, _return);
  }
  
_return:

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
      
      code = taosHashPut(pHash, &pState->id.nodeId, sizeof(pState->id.nodeId), &pState, POINTER_BYTES);
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
    if (NULL == taosArrayPush(triggerTask ? snode.triggerList : snode.runnerList, &pState)) {
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



static int32_t msmAddTaskToSnodeDeploy(SHashObj* pHash, SStmTaskDeploy* pDeploy, SStreamObj* pStream, bool triggerTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStmSnodeTasksDeploy snode = {0};
  SArray** ppList = triggerTask ? &snode.triggerList : &snode.runnerList;
  while (true) {
    SStmSnodeTasksDeploy* pSnode = taosHashAcquire(pHash, &pDeploy->task.nodeId, sizeof(pDeploy->task.nodeId));
    if (NULL == pSnode) {
      *ppList = taosArrayInit(10, sizeof(SStmTaskDeploy));
      TSDB_CHECK_NULL(*ppList, code, lino, _return, terrno);
      TSDB_CHECK_NULL(taosArrayPush(*ppList, pDeploy), code, lino, _return, terrno);
      
      code = taosHashPut(pHash, &pDeploy->task.nodeId, sizeof(pDeploy->task.nodeId), &snode, sizeof(snode));
      if (TSDB_CODE_SUCCESS == code) {
        return code;
      }

      if (TSDB_CODE_DUP_KEY != code) {
        goto _return;
      }    

      taosArrayDestroy(snode.triggerList);
      continue;
    }

    ppList = triggerTask ? &pSnode->triggerList : &pSnode->runnerList;
    
    taosWLockLatch(&pSnode->lock);
    if (NULL == *ppList) {
      *ppList = taosArrayInit(10, sizeof(SStmTaskDeploy));
      if (NULL == *ppList) {
        taosWUnLockLatch(&pSnode->lock);
        TSDB_CHECK_NULL(*ppList, code, lino, _return, terrno);
      }
    }
    if (NULL == taosArrayPush*ppList, pDeploy)) {
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

static int32_t msmAddTaskToVgroupMap(SMnode* pMnode, SStreamObj* pStream, SArray* pTasks) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t taskNum = taosArrayGetSize(pTasks);
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskStatus* pState = taosArrayGet(pTasks, i);
    TSDB_CHECK_CODE(msmAddTaskToVgroupMapImpl(mStreamMgmt.vgroupMap, pState, pStream), lino, _return);
    SStmTaskDeploy info = {0};
    TSDB_CHECK_CODE(msmGetTaskDeployInfo(&info, streamId, &pState->id, pStream), lino, _return);
    TSDB_CHECK_CODE(msmAddTaskToVgroupDeployMap(mStreamMgmt.toDeployVgMap, &info, pStream), lino, _return);
    atomic_add_fetch_32(&mStreamMgmt.toDeployVgTaskNum, 1);
  }
  
_return:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}

int32_t msmGetTaskDeployInfo(SStmTaskDeploy* pDeploy, int64_t streamId, SStmTaskId* pId, SStreamObj* pStream) {
  // TODO
}

static int32_t msmAddTaskToSnodeMap(SMnode* pMnode, SStreamObj* pStream, SArray* pTasks, SStmTaskStatus* pTask, bool triggerTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t taskNum = triggerTask ? 1 : taosArrayGetSize(pTasks);
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SStmTaskStatus* pState = triggerTask ? pTask : taosArrayGet(pTasks, i);
    TSDB_CHECK_CODE(msmAddTaskToSnodeMapImpl(mStreamMgmt.snodeMap, pState, pStream, triggerTask), lino, _return);

    SStmTaskDeploy info = {0};
    TSDB_CHECK_CODE(msmGetTaskDeployInfo(&info, streamId, &pState->id, pStream), lino, _return);
    TSDB_CHECK_CODE(msmAddTaskToSnodeDeploy(mStreamMgmt.toDeploySnodeMap, &info, pStream, triggerTask), lino, _return);
    
    atomic_add_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);
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

  pInfo->triggerTask.id.taskId = msmAssignTaskId(pStream->pCreate->streamId);
  pInfo->triggerTask.id.nodeId = msmAssignTaskSnodeId(pMnode, pStream->pCreate->streamId, true);
  pInfo->triggerTask.id.taskIdx = 0;
  pInfo->triggerTask.lastUpTs = INT64_MIN;

  TSDB_CHECK_CODE(msmAddTaskToTaskMap(pMnode, pStream, NULL, pInfo->triggerTask), lino, _return);
  TSDB_CHECK_CODE(msmAddTaskToSnodeMap(pMnode, pStream, NULL, pInfo->triggerTask, true), lino, _return);

_return:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmAppendReaderTriggerTasks(SMnode* pMnode, SArray* pReader, SStreamObj* pStream, int16_t* taskIdx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SSdb   *pSdb = pMnode->pSdb;
  SStmTaskStatus state = {{INT64_MIN, INT64_MIN, *taskIdx}, 0, INT64_MIN};
  
  switch (pStream->pCreate->triggerTblType) {
    case TSDB_NORMAL_TABLE:
    case TSDB_CHILD_TABLE: {
      state.id.taskId = msmAssignTaskId();
      state.id.nodeId = pStream->pCreate->triggerTblVgId;
      state.id.taskIdx = 0;
      TSDB_CHECK_NULL(taosArrayPush(pReader, &state), code, lino, _return, terrno);
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

static int32_t msmAppendReaderCalcTasks(SMnode* pMnode, SArray* pReader, SStreamObj* pStream, int16_t* taskIdx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t calcTasksNum = taosArrayGetSize(pStream->pCreate->calcScanPlanList);
  int64_t streamId = pStream->pCreate->streamId;
  SStmTaskStatus state = {{INT64_MIN, INT64_MIN, *taskIdx}, 0, INT64_MIN};

  for (int32_t i = 0; i < calcTasksNum; ++i) {
    SStreamCalcScan* pScanList = taosArrayGet(pStream->pCreate->calcScanPlanList, i);
    int32_t vgNum = taosArrayGetSize(pScanList->vgList);
    for (int32_t m = 0; m < vgNum; ++m) {
      state.id.taskId = msmAssignTaskId();
      state.id.nodeId = *(int32_t*)taosArrayGet(pScanList->vgList, i);
      state.id.taskIdx++;
      TSDB_CHECK_NULL(taosArrayPush(pReader, &state), code, lino, _return, terrno);
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
  int16_t taskIdx = -1;
  int32_t taskNum = 0;

  msmCalcReadersNum(pStream, &taskNum);

  pInfo->readerList = taosArrayInit(taskNum, sizeof(SStmTaskStatus));
  TSDB_CHECK_NULL(pInfo->readerList, code, lino, _exit, terrno);
  
  TSDB_CHECK_CODE(msmAppendReaderTriggerTasks(pMnode, pInfo->readerList, pStream, &taskIdx), lino, _exit);
  TSDB_CHECK_CODE(msmAppendReaderCalcTasks(pMnode, pInfo->readerList, pStream, &taskIdx), lino, _exit);

  TSDB_CHECK_CODE(msmAddTaskToTaskMap(pMnode, pStream, pInfo->readerList, NULL), lino, _exit);
  TSDB_CHECK_CODE(msmAddTaskToVgroupMap(pMnode, pStream, pInfo->readerList), lino, _exit);
  
_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}

static int32_t msmBuildRunnerTasks(SMnode* pMnode, SStmStatus* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SSdb   *pSdb = pMnode->pSdb;
  SStmTaskStatus state = {{INT64_MIN, INT64_MIN, 0}, 0, INT64_MIN};

  pInfo->runnerList = taosArrayInit(mStreamMgmt.runnerMulti, sizeof(SStmTaskStatus));
  TSDB_CHECK_NULL(pInfo->runnerList, code, lino, _return, terrno);

  for (int32_t i = 0; i < mStreamMgmt.runnerMulti; ++i) {
    state.id.taskId = msmAssignTaskId(pStream->pCreate->streamId);
    state.id.nodeId = msmAssignTaskSnodeId(pMnode, pStream->pCreate->streamId, (0 == i) ? true : false);
    TSDB_CHECK_NULL(taosArrayPush(pInfo->runnerList, &state), code, lino, _return, terrno);
    
    TSDB_CHECK_CODE(msmAddTaskToTaskMap(pMnode, pStream, pInfo->runnerList, NULL), lino, _return);
    TSDB_CHECK_CODE(msmAddTaskToSnodeMap(pMnode, pStream, pInfo->runnerList, NULL, false), lino, _return);
  }
  
_return:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


static int32_t msmBuildStreamTasks(SMnode* pMnode, SStmStatus* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_CODE(msmBuildTriggerTasks(pMnode, pInfo, pStream), lino, _return);
  TSDB_CHECK_CODE(msmBuildReaderTasks(pMnode, pInfo, pStream), lino, _return);
  TSDB_CHECK_CODE(msmBuildRunnerTasks(pMnode, pInfo, pStream), lino, _return);
  
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
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
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
    TSDB_CHECK_CODE(code, lino, _return);
  
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

  if (taosHashGetSize(mStreamMgmt.snodeMap) <= 0) {
    TSDB_CHECK_CODE(msmAppendNewSnodesToMap(pMnode), lino, _return);
  }
  if (taosHashGetSize(mStreamMgmt.dnodeMap) <= 0) {
    TSDB_CHECK_CODE(msmAppendNewDnodesToMap(pMnode), lino, _return);
  }

  SStmStatus** ppStream = (SStmStatus**)taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (ppStream) {

  }


  TSDB_CHECK_CODE(mndAcquireStream(pMnode, streamName, &pStream), lino, _return);

  TSDB_CHECK_CODE(msmAddStreamTasksToMap(pMnode, pStream), lino, _return);


_return:

  mndReleaseStream(pMnode, pStream);

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static int32_t msmLaunchStreamDropAction(SRpcMsg *pReq) {
  int64_t streamId = *(int64_t*)pReq->pCont;
  char* streamName = (char*)pReq->pCont + sizeof(streamId);
}

static int32_t msmHandleStreamActions(SMnode* pMnode, SStmActionQ* pQ) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmQNode* pQNode = NULL;
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
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmHandleGrantExpired(SMnode *pMnode) {

}

void msmDestroyStreamDeploy(SStmStreamDeploy* pStream) {
  taosArrayDestroy(pStream->readerTasks);
  taosArrayDestroy(pStream->runnerTasks);
}

static int32_t msmInitStreamDeploy(SStmStreamDeploy* pStream, SStmTaskDeploy* pDeploy) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  
  switch (pDeploy->task.type) {
    case STREAM_READER_TASK:
      pStream->readerTasks = taosArrayInit(20, POINTER_BYTES);
      TSDB_CHECK_NULL(pStream->readerTasks, code, lino, _exit, terrno);
      
      TSDB_CHECK_NULL(taosArrayPush(pStream->readerTasks, &pDeploy), code, lino, _exit, terrno);
      break;
    case STREAM_TRIGGER_TASK:
      pStream->triggerTask = pDeploy;
      break;
    case STREAM_RUNNER_TASK:
      pStream->runnerTasks = taosArrayInit(20, POINTER_BYTES);
      TSDB_CHECK_NULL(pStream->runnerTasks, code, lino, _exit, terrno);
      
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
        goto _return;
      }    

      msmDestroyStreamDeploy(&streamDeploy);
      continue;
    }

    switch (pDeploy->task.type) {
      case STREAM_READER_TASK:
        TSDB_CHECK_NULL(taosArrayPush(pStream->readerTasks, &pDeploy), code, lino, _exit, terrno);
        break;
      case STREAM_TRIGGER_TASK:
        pStream->triggerTask = pDeploy;
        break;
      case STREAM_RUNNER_TASK:
        TSDB_CHECK_NULL(taosArrayPush(pStream->runnerTasks, &pDeploy), code, lino, _exit, terrno);
        break;
      default:
        break;
    }
    
    break;
  }
  
_return:

  taosHashRelease(pHash, pStream);

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


int32_t msmAddGrpDeployTasks(SHashObj* pHash, SArray* pTasks, int32_t* deployed) {
  int32_t code = TSDB_CODE_SUCCESS;
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
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmAddGrpDeployVgTasks(SMnode* pMnode, SArray* pVgLeaders, int32_t gid) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t vgNum = taosArrayGetSize(pVgLeaders);
  SStmVgroupTasksDeploy* pVg = NULL;
  int32_t tidx = streamGetThreadIdx(mStreamMgmt.threadNum, gid);
  
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

    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmAddGrpDeploySnodeTasks(SMnode* pMnode, int32_t snodeId, int32_t gid) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStmSnodeTasksDeploy* pSnode = NULL;
  int32_t tidx = streamGetThreadIdx(mStreamMgmt.threadNum, gid);
  
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

    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t msmAddHbRspDeploy(SHashObj* pStreams, SMStreamHbRspMsg* pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t streamNum = taosHashGetSize(pStreams);
  void* pIter = NULL;
  
  pRsp->deploy.streamList = taosArrayInit(streamNum, sizeof(SStmStreamDeploy));
  TSDB_CHECK_NULL(pRsp->deploy.streamList, code, lino, _exit, terrno);

  while (1) {
    pIter = taosHashIterate(pStreams, pIter);
    if (pIter == NULL) {
      break;
    }
    
    SStmStreamDeploy *pDeploy = (SStmStreamDeploy *)pIter;
    TSDB_CHECK_NULL(taosArrayPush(pRsp->deploy.streamList, pDeploy));
  }
  
_exit:

  if (pIter) {
    taosHashCancelIterate(pStreams, pIter);
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
    TAOS_CHECK_EXIT(msmCleanDeployedVgTasks(pHb->pVgLeaders));
  }

  if (atomic_load_32(&mStreamMgmt.toDeploySnodeTaskNum) > 0) {
    TAOS_CHECK_EXIT(msmCleanDeployedSnodeTasks(pHb->snodeId));
  }

  int32_t tidx = streamGetThreadIdx(mStreamMgmt.threadNum, pHb->streamGId);
  taosHashClear(mStreamMgmt.tCtx[tidx].actionStm[pHb->streamGId]);
  taosHashClear(mStreamMgmt.tCtx[tidx].deployStm[pHb->streamGId]);
}

void msmHandleAbnormalStatusUpdate(int32_t tidx, int32_t gid, SStmTaskStatusMsg* pMsg, int64_t currTs) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t action = 0;
  int64_t streamId = pMsg->streamId;
  SHashObj* pHash = mStreamMgmt.tCtx[tidx].actionStm[gid];
  SStmStatus* pStatus = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStatus) {
    mstWarn("stream no longer exists in streamMap, try to undeploy it, TASK:%" PRId64 , pMsg->taskId);
    action = STREAM_ACT_UNDEPLOY;
    TAOS_CHECK_EXIT(taosHashPut(pHash, &streamId, sizeof(streamId), &action, sizeof(action)));
  }
  
  switch (pMsg->status) {
    case STREAM_STATUS_INIT:
      if ((currTs - pStatus->lastActTs) < STREAM_ACT_MIN_DELAY_MSEC) {
        return;
      }

      int32_t *pAction = taosHashGet(pHash, &streamId, sizeof(streamId));
      if (pAction) {
        *pAction |= STREAM_ACT_START;
      } else {
        action = STREAM_ACT_START;
        TAOS_CHECK_EXIT(taosHashPut(pHash, &streamId, sizeof(streamId), &action, sizeof(action)));
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
  // TODO
}

int32_t msmHandleStreamStatusUpdate(SMnode* pMnode, int32_t tidx, int32_t gid, int64_t currTs, SArray* pStatusList) {
  int32_t num = taosArrayGetSize(pStatusList);
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
      msmHandleAbnormalStatusUpdate(tidx, gid, pStatus, currTs);
    }
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

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
    pRsp->start.taskList = taosArrayInit(streamNum, sizeof(SStreamTasksStart));
    TSDB_CHECK_NULL(pRsp->start.taskList, code, lino, _exit, terrno);
  }

  SStreamTasksStart start;
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
  void* pIter = NULL;
  int32_t streamNum = taosHashGetSize(mStreamMgmt.tCtx[tidx].actionStm[gid]);
  SHashObj* pHash = mStreamMgmt.tCtx[tidx].actionStm[gid];
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
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
}

int32_t msmHandleStreamHbMsg(SMnode* pMnode, int64_t currTs, SStreamHbMsg* pHb, SMStreamHbRspMsg* pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t tidx = streamGetThreadIdx(mStreamMgmt.threadNum, pHb->streamGId);
  
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
    TAOS_CHECK_EXIT(msmAddHbRspDeploy(pMnode, mStreamMgmt.tCtx[tidx].deployStm[pHb->streamGId], pRsp));
  }

  if (taosArrayGetSize(pHb->pStreamStatus) > 0) {
    TAOS_CHECK_EXIT(msmHandleStreamStatusUpdate(pMnode, tidx, pHb->streamGId, currTs, pHb->pStreamStatus));
  }

  if (taosHashGetSize(mStreamMgmt.tCtx[tidx].actionStm[pHb->streamGId]) > 0 && GOT_SNODE(pHb->snodeId)) {
    TAOS_CHECK_EXIT(msmHandleStreamUpdatePostActions(pMnode, tidx, pHb->streamGId, pHb->snodeId, pRsp));
  }

_exit:

  if (code) {
    mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
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



