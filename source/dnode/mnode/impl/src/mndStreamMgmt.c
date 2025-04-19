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
  mStreamMgmt.threadCtx = taosMemoryCalloc(mStreamMgmt.threadNum, sizeof(SStreamThreadCtx));
  if (NULL == mStreamMgmt.threadCtx) {
    code = terrno;
    mError("failed to initialize the stream runtime threadCtx, threadNum:%d, error:%s", (mStreamMgmt.threadNum, tstrerror(code));
    goto _return;
  }
  
  for (int32_t i = 0; i < mStreamMgmt.threadNum; ++i) {
    SStreamThreadCtx* pCtx = mStreamMgmt.threadCtx + i;
    pCtx->actionQ = taosMemoryCalloc(1, sizeof(SStreamActionQ));
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
      pCtx->toHandleStm[m] = taosHashInit(snodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
      if (pCtx->toHandleStm[m] == NULL) {
        code = terrno;
        mError("failed to initialize the stream runtime toHandleStm[%d][%d], error:%s", i, m, tstrerror(code));
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

static int32_t msmAppendTasksFromObj(SMnode* pMnode, SStreamObj* pStream, SArray* pTasks, SStreamTaskStatus* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t taskNum = pTasks ? taosArrayGetSize(pTasks) : 1;
  int64_t key[2] = {pStream->pCreate->streamId, 0};
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SStreamTaskStatus* pStatus = pTasks ? taosArrayGet(pTasks, i) : pTask;
    key[1] = pStatus->id.taskId;
    TSDB_CHECK_CODE(taosHashPut(mStreamMgmt.taskMap, key, sizeof(key), &pStatus, POINTER_BYTES), lino, _return);
  }
  
_return:

  return code;
}

static int32_t msmAppendVgReaderTaskToMap(SHashObj* pVgMap, SStreamTaskStatus* pState, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStreamVgReaderTasksStatus vg = {0};

  while (true) {
    SStreamVgReaderTasksStatus* pVg = taosHashAcquire(pVgMap, &pState->id.nodeId, sizeof(pState->id.nodeId));
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

  return code;
}

static int32_t msmAppendVgReaderTaskToDeploy(SHashObj* pVgMap, SStreamReaderDeployInfo* pDeploy, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStreamVgReaderTasksDeploy vg = {0};

  while (true) {
    SStreamVgReaderTasksDeploy* pVg = taosHashAcquire(pVgMap, &pDeploy->id.nodeId, sizeof(pDeploy->id.nodeId));
    if (NULL == pVg) {
      vg.taskList = taosArrayInit(20, POINTER_BYTES);
      TSDB_CHECK_NULL(vg.taskList, code, lino, _return, terrno);
      TSDB_CHECK_NULL(taosArrayPush(vg.taskList, &pDeploy), code, lino, _return, terrno);
      code = taosHashPut(pVgMap, &pDeploy->id.nodeId, sizeof(pDeploy->id.nodeId), &pDeploy, POINTER_BYTES);
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
    if (NULL == taosArrayPush(pVg->taskList, &pDeploy)) {
      taosWUnLockLatch(&pVg->lock);
      TSDB_CHECK_NULL(NULL, code, lino, _return, terrno);
    }
    taosWUnLockLatch(&pVg->lock);
    
    taosHashRelease(pVgMap, pVg);
    break;
  }
  
_return:

  return code;
}


static int32_t msmAppendSnodeTaskToMap(SHashObj* pHash, SStreamTaskStatus* pState, SStreamObj* pStream, bool triggerTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStreamSnodeTasksStatus snode = {0};

  while (true) {
    SStreamSnodeTasksStatus* pSnode = taosHashAcquire(pHash, &pState->id.nodeId, sizeof(pState->id.nodeId));
    if (NULL == pSnode) {
      if (triggerTask) {
        snode.triggerTaskList = taosArrayInit(20, POINTER_BYTES);
        TSDB_CHECK_NULL(snode.triggerTaskList, code, lino, _return, terrno);
        TSDB_CHECK_NULL(taosArrayPush(snode.triggerTaskList, &pState), code, lino, _return, terrno);
      } else {
        snode.runnerTaskList = taosArrayInit(20, POINTER_BYTES);
        TSDB_CHECK_NULL(snode.runnerTaskList, code, lino, _return, terrno);
        TSDB_CHECK_NULL(taosArrayPush(snode.runnerTaskList, &pState), code, lino, _return, terrno);
      }
      
      code = taosHashPut(pHash, &pState->id.nodeId, sizeof(pState->id.nodeId), &pState, POINTER_BYTES);
      if (TSDB_CODE_SUCCESS == code) {
        return code;
      }

      if (TSDB_CODE_DUP_KEY != code) {
        goto _return;
      }    

      taosArrayDestroy(triggerTask ? snode.triggerTaskList : snode.runnerTaskList);
      continue;
    }

    taosWLockLatch(&pSnode->lock);
    if (NULL == taosArrayPush(triggerTask ? snode.triggerTaskList : snode.runnerTaskList, &pState)) {
      taosWUnLockLatch(&pSnode->lock);
      TSDB_CHECK_NULL(NULL, code, lino, _return, terrno);
    }
    taosWUnLockLatch(&pSnode->lock);
    
    taosHashRelease(pHash, pSnode);
    break;
  }
  
_return:

  return code;
}



static int32_t msmAppendSnodeTriggerTaskToDeploy(SHashObj* pHash, SStreamTriggerDeployInfo* pDeploy, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStreamSnodeTasksStatus snode = {0};

  while (true) {
    SStreamSnodeTasksStatus* pSnode = taosHashAcquire(pHash, &pDeploy->id.nodeId, sizeof(pDeploy->id.nodeId));
    if (NULL == pSnode) {
      snode.triggerTaskList = taosArrayInit(20, POINTER_BYTES);
      TSDB_CHECK_NULL(snode.triggerTaskList, code, lino, _return, terrno);
      TSDB_CHECK_NULL(taosArrayPush(snode.triggerTaskList, &pDeploy), code, lino, _return, terrno);
      
      code = taosHashPut(pHash, &pDeploy->id.nodeId, sizeof(pDeploy->id.nodeId), &pDeploy, POINTER_BYTES);
      if (TSDB_CODE_SUCCESS == code) {
        return code;
      }

      if (TSDB_CODE_DUP_KEY != code) {
        goto _return;
      }    

      taosArrayDestroy(snode.triggerTaskList);
      continue;
    }

    taosWLockLatch(&pSnode->lock);
    if (NULL == taosArrayPush(snode.triggerTaskList, &pDeploy)) {
      taosWUnLockLatch(&pSnode->lock);
      TSDB_CHECK_NULL(NULL, code, lino, _return, terrno);
    }
    taosWUnLockLatch(&pSnode->lock);
    
    taosHashRelease(pHash, pSnode);
    break;
  }
  
_return:

  return code;
}

static int32_t msmAppendSnodeRunnerTaskToDeploy(SHashObj* pHash, SStreamRunnerDeployInfo* pDeploy, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStreamSnodeTasksStatus snode = {0};

  while (true) {
    SStreamSnodeTasksStatus* pSnode = taosHashAcquire(pHash, &pDeploy->id.nodeId, sizeof(pDeploy->id.nodeId));
    if (NULL == pSnode) {
      snode.runnerTaskList = taosArrayInit(20, POINTER_BYTES);
      TSDB_CHECK_NULL(snode.runnerTaskList, code, lino, _return, terrno);
      TSDB_CHECK_NULL(taosArrayPush(snode.runnerTaskList, &pDeploy), code, lino, _return, terrno);
      
      code = taosHashPut(pHash, &pDeploy->id.nodeId, sizeof(pDeploy->id.nodeId), &pDeploy, POINTER_BYTES);
      if (TSDB_CODE_SUCCESS == code) {
        return code;
      }

      if (TSDB_CODE_DUP_KEY != code) {
        goto _return;
      }    

      taosArrayDestroy(snode.runnerTaskList);
      continue;
    }

    taosWLockLatch(&pSnode->lock);
    if (NULL == taosArrayPush(snode.runnerTaskList, &pDeploy)) {
      taosWUnLockLatch(&pSnode->lock);
      TSDB_CHECK_NULL(NULL, code, lino, _return, terrno);
    }
    taosWUnLockLatch(&pSnode->lock);
    
    taosHashRelease(pHash, pSnode);
    break;
  }
  
_return:

  return code;
}


static int32_t msmAppendVgReaderTasksFromObj(SMnode* pMnode, SStreamObj* pStream, SArray* pTasks) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t taskNum = taosArrayGetSize(pTasks);
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SStreamTaskStatus* pState = taosArrayGet(pTasks, i);
    TSDB_CHECK_CODE(msmAppendVgReaderTaskToMap(mStreamMgmt.vgroupMap, pState, pStream), lino, _return);
    SStreamDeployTaskInfo info = {0};
    TSDB_CHECK_CODE(msmGetStreamTaskDeployInfo(&info, streamId, &pState->id, pStream), lino, _return);
    TSDB_CHECK_CODE(msmAppendVgReaderTaskToDeploy(mStreamMgmt.toDeployVgMap, &info, pStream), lino, _return);
    atomic_add_fetch_32(&mStreamMgmt.toDeployVgTaskNum, 1);
  }
  
_return:

  return code;
}

static int32_t msmAppendSnodeTasksFromObj(SMnode* pMnode, SStreamObj* pStream, SArray* pTasks, SStreamTaskStatus* pTask, bool triggerTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t taskNum = triggerTask ? 1 : taosArrayGetSize(pTasks);
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SStreamTaskStatus* pState = triggerTask ? pTask : taosArrayGet(pTasks, i);
    TSDB_CHECK_CODE(msmAppendSnodeTaskToMap(mStreamMgmt.snodeMap, pState, pStream, triggerTask), lino, _return);
    if (triggerTask) {
      SStreamDeployTaskInfo info = {0};
      TSDB_CHECK_CODE(msmGetStreamTaskDeployInfo(&info, streamId, &pState->id, pStream), lino, _return);
      TSDB_CHECK_CODE(msmAppendSnodeTriggerTaskToDeploy(mStreamMgmt.toDeploySnodeMap, &info, pStream), lino, _return);
    } else {
      SStreamDeployTaskInfo info = {0};
      TSDB_CHECK_CODE(msmGetStreamTaskDeployInfo(&info, streamId, &pState->id, pStream), lino, _return);
      TSDB_CHECK_CODE(msmAppendSnodeRunnerTaskToDeploy(mStreamMgmt.toDeploySnodeMap, &info, pStream), lino, _return);
    }
    
    atomic_add_fetch_32(&mStreamMgmt.toDeploySnodeTaskNum, 1);
  }
  
_return:

  return code;
}

int64_t msmAssignStreamTaskId(void) {
  return atomic_fetch_add_64(&mStreamMgmt.lastTaskId, 1);
}

int32_t msmAssignStreamTriggerSnodeId(SMnode* pMnode, int64_t streamId) {
  int32_t snodeNum = sdbGetSize(pMnode->pSdb, SDB_SNODE);
  int32_t snodeTarget = streamId % snodeNum;
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

static int32_t msmAppendTriggerTasks(SMnode* pMnode, SStreamStatus* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;

  pInfo->triggerTask.id.taskId = msmAssignStreamTaskId(pStream->pCreate->streamId);
  pInfo->triggerTask.id.nodeId = msmAssignStreamTriggerSnodeId(pMnode, pStream->pCreate->streamId);
  pInfo->triggerTask.id.taskIdx = 0;

  TSDB_CHECK_CODE(msmAppendTasksFromObj(pMnode, pStream, NULL, pInfo->triggerTask), lino, _return);
  TSDB_CHECK_CODE(msmAppendSnodeTasksFromObj(pMnode, pStream, NULL, pInfo->triggerTask, true), lino, _return);

_return:

  return code;
}

static int32_t msmAppendReaderTriggerTasks(SMnode* pMnode, SArray* pReader, SStreamObj* pStream, int16_t* taskIdx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SSdb   *pSdb = pMnode->pSdb;
  SStreamTaskStatus state = {{INT64_MIN, INT64_MIN, *taskIdx}, 0, INT64_MIN};
  
  switch (pStream->pCreate->triggerTblType) {
    case TSDB_NORMAL_TABLE:
    case TSDB_CHILD_TABLE: {
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

  return code;
}

static int32_t msmAppendReaderCalcTasks(SMnode* pMnode, SArray* pReader, SStreamObj* pStream, int16_t* taskIdx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t calcTasksNum = taosArrayGetSize(pStream->pCreate->calcScanPlanList);
  int64_t streamId = pStream->pCreate->streamId;
  SStreamTaskStatus state = {{INT64_MIN, INT64_MIN, *taskIdx}, 0, INT64_MIN};

  for (int32_t i = 0; i < calcTasksNum; ++i) {
    SStreamCalcScan* pScanList = taosArrayGet(pStream->pCreate->calcScanPlanList, i);
    int32_t vgNum = taosArrayGetSize(pScanList->vgList);
    for (int32_t m = 0; m < vgNum; ++m) {
      state.id.nodeId = *(int32_t*)taosArrayGet(pScanList->vgList, i);
      state.id.taskIdx++;
      TSDB_CHECK_NULL(taosArrayPush(pReader, &state), code, lino, _return, terrno);
    }
  }

_return:

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

static int32_t msmAppendReaderTasks(SMnode* pMnode, SStreamStatus* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int16_t taskIdx = -1;
  int32_t taskNum = 0;

  msmCalcReadersNum(pStream, &taskNum);

  pInfo->readerTaskList = taosArrayInit(taskNum, sizeof(SStreamTaskStatus));
  TSDB_CHECK_NULL(pInfo->readerTaskList, code, lino, _return, terrno);
  
  TSDB_CHECK_CODE(msmAppendReaderTriggerTasks(pMnode, pInfo->readerTaskList, pStream, &taskIdx), lino, _return);
  TSDB_CHECK_CODE(msmAppendReaderCalcTasks(pMnode, pInfo->readerTaskList, pStream, &taskIdx), lino, _return);

  int32_t taskListNum = taosArrayGetSize(pInfo->readerTaskList);
  for (int32_t i = 0; i < taskListNum; ++i) {
    SStmReadersStatus* pTasks = taosArrayGet(pInfo->readerTaskList, i);
    TSDB_CHECK_CODE(msmAppendTasksFromObj(pMnode, pStream, pTasks->vgList), lino, _return);
    TSDB_CHECK_CODE(msmAppendVgReaderTasksFromObj(pMnode, pStream, pTasks->vgList), lino, _return);
  }
  
_return:

  return code;
}

static int32_t msmAppendRunnerTasks(SMnode* pMnode, SStreamStatus* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SSdb   *pSdb = pMnode->pSdb;
  SStreamTaskStatus state = {.taskId = INT64_MIN, .nodeId = INT64_MIN, .taskIdx = -1, .lastUpTs = INT64_MIN};

  pInfo->runnerTaskList = taosArrayInit(1, sizeof(SStreamTaskStatus));
  TSDB_CHECK_NULL(pInfo->runnerTaskList, code, lino, _return, terrno);
  state.nodeId = msmAssignStreamRunnerSnodeId(pStream->pCreate->streamId);
  state.taskIdx++;
  TSDB_CHECK_NULL(taosArrayPush(pInfo->runnerTaskList, &state), code, lino, _return, terrno);
  TSDB_CHECK_CODE(msmAppendTasksFromObj(pMnode, pStream, pInfo->runnerTaskList, NULL), lino, _return);
  TSDB_CHECK_CODE(msmAppendSnodeTasksFromObj(pMnode, pStream, pInfo->runnerTaskList, NULL, false), lino, _return);

_return:

  return code;
}


static int32_t msmBuildStreamTasksFromObj(SMnode* pMnode, SStreamStatus* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_CODE(msmAppendTriggerTasks(pMnode, pInfo, pStream), lino, _return);
  TSDB_CHECK_CODE(msmAppendReaderTasks(pMnode, pInfo, pStream), lino, _return);
  TSDB_CHECK_CODE(msmAppendRunnerTasks(pMnode, pInfo, pStream), lino, _return);
  
_return:

  return code;
}

static int32_t msmAppendNewSnodesToMap(SMnode* pMnode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStreamSnodeTasksStatus tasks = {0};
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

  return code;
}


static int32_t msmAppendStreamTasksFromObj(SMnode* pMnode, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SStreamStatus info = {0};

  TSDB_CHECK_CODE(msmBuildStreamTasksFromObj(pMnode, &info, pStream), lino, _return);

  TSDB_CHECK_CODE(taosHashPut(mStreamMgmt.streamMap, &streamId, sizeof(streamId), &info, sizeof(info)), lino, _return);

_return:

  return code;
}


static int32_t msmLaunchStreamDepolyAction(SMnode* pMnode, SStreamQNode* pQNode) {
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

  SStreamStatus** ppStream = (SStreamStatus**)taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (ppStream) {

  }


  TSDB_CHECK_CODE(mndAcquireStream(pMnode, streamName, &pStream), lino, _return);

  TSDB_CHECK_CODE(msmAppendStreamTasksFromObj(pMnode, pStream), lino, _return);


_return:

  mndReleaseStream(pMnode, pStream);

  return code;
}

static int32_t msmLaunchStreamDropAction(SRpcMsg *pReq) {
  int64_t streamId = *(int64_t*)pReq->pCont;
  char* streamName = (char*)pReq->pCont + sizeof(streamId);
}

static int32_t msmHandleStreamActions(SMnode* pMnode, SStreamActionQ* pQ) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStreamQNode* pQNode = NULL;
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

  return code;
}

int32_t msmHandleGrantExpired(SMnode *pMnode) {

}

void msmDestroyStreamTasksDeploySet(SStreamTasksDeploy* pStream) {
  taosArrayDestroy(pStream->readerTasks);
  taosArrayDestroy(pStream->triggerTasks);
  taosArrayDestroy(pStream->runnerTasks);
}

static int32_t msmInitStreamTasksDeploySet(SStreamTasksDeploy* pStream, SStreamDeployTaskInfo* pDeploy) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SArray* pTasks = taosArrayInit(20, sizeof(SStreamDeployTaskInfo));
  TSDB_CHECK_NULL(pTasks, code, lino, _exit, terrno);

  TSDB_CHECK_NULL(taosArrayPush(pTasks, pDeploy), code, lino, _exit, terrno);
  
  switch (pDeploy->task.type) {
    case STREAM_READER_TASK:
      pStream->readerTasks = pTasks;
      break;
    case STREAM_TRIGGER_TASK:
      pStream->triggerTasks = pTasks;
      break;
    case STREAM_RUNNER_TASK:
      pStream->runnerTasks = pTasks;
      break;
    default:
      break;
  }

_exit:

  return code;
}

static int32_t msmAddTaskToStreamDeployMap(SHashObj* pHash, SStreamDeployTaskInfo* pDeploy) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pDeploy->task.streamId;
  SStreamTasksDeploy streamDeploy = {0};

  while (true) {
    SStreamTasksDeploy* pStream = taosHashAcquire(pHash, &streamId, sizeof(streamId));
    if (NULL == pStream) {
      TAOS_CHECK_EXIT(msmInitStreamTasksDeploySet(&streamDeploy, pDeploy));
      code = taosHashPut(pHash, &streamId, sizeof(streamId), &streamDeploy, sizeof(streamDeploy));
      if (TSDB_CODE_SUCCESS == code) {
        return code;
      }

      if (TSDB_CODE_DUP_KEY != code) {
        goto _return;
      }    

      msmDestroyStreamTasksDeploySet(&streamDeploy);
      continue;
    }

    SArray* taskList = (STREAM_READER_TASK == pDeploy->task.type) ? pStream->readerTasks : ((STREAM_TRIGGER_TASK == pDeploy->task.type) ? pStream->triggerTasks : pStream->runnerTasks);

    if (NULL == taosArrayPush(taskList, pDeploy)) {
      TSDB_CHECK_NULL(NULL, code, lino, _return, terrno);
    }
    
    taosHashRelease(pHash, pStream);
    break;
  }
  
_return:

  return code;
}


int32_t msmAddDeployTaskToDnode(SHashObj* pHash, SArray* pTasks, int32_t* deployed) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t taskNum = taosArrayGetSize(pTasks);

  for (int32_t i = 0; i < taskNum; ++i) {
    SStreamDeployTaskExtInfo* pExt = taosArrayGet(pTasks, i);
    if (pExt->deployed) {
      continue;
    }

    TAOS_CHECK_EXIT(msmAddTaskToStreamDeployMap(pHash, &pExt->deploy));
    pExt->deployed = true;
    (*deployed)++;
  }

_exit:

  return code;
}

int32_t msmAddTasksBasedOnVgLeader(SMnode* pMnode, SArray* pVgLeaders, int32_t gid) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t vgNum = taosArrayGetSize(pVgLeaders);
  SStreamVgReaderTasksDeploy* pVg = NULL;
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
    
    TAOS_CHECK_EXIT(msmAddDeployTaskToDnode(mStreamMgmt.threadCtx[tidx].deployStm[gid], pVg->taskList, &pVg->deployed));
    taosRUnLockLatch(&pVg->lock);
  }

_exit:

  if (code && pVg) {
    taosRUnLockLatch(&pVg->lock);
  }

  return code;
}

int32_t msmAddTasksBasedOnSnode(SMnode* pMnode, int32_t snodeId, int32_t gid) {
  int32_t code = TSDB_CODE_SUCCESS;
  SStreamSnodeTasksDeploy* pSnode = NULL;
  int32_t tidx = streamGetThreadIdx(mStreamMgmt.threadNum, gid);
  
  pSnode = taosHashAcquire(mStreamMgmt.toDeploySnodeMap, &snodeId, sizeof(snodeId));
  if (NULL == pSnode) {
    return TSDB_CODE_SUCCESS;
  }

  if (taosRTryLockLatch(&pSnode->lock)) {
    return TSDB_CODE_SUCCESS;
  }
  
  if (atomic_load_32(&pSnode->triggerDeployed) < taosArrayGetSize(pSnode->triggerTaskList)) {
    TAOS_CHECK_EXIT(msmAddDeployTaskToDnode(mStreamMgmt.threadCtx[tidx].deployStm[gid], pSnode->triggerTaskList, &pSnode->triggerDeployed));
  }

  if (atomic_load_32(&pSnode->runnerDeployed) < taosArrayGetSize(pSnode->runnerTaskList)) {
    TAOS_CHECK_EXIT(msmAddDeployTaskToDnode(mStreamMgmt.threadCtx[tidx].deployStm[gid], pSnode->runnerTaskList, &pSnode->runnerDeployed));
  }
  
  taosRUnLockLatch(&pSnode->lock);

_exit:

  if (code && pSnode) {
    taosRUnLockLatch(&pSnode->lock);
  }

  return code;
}

int32_t msmAddHbRspDeploy(SHashObj* pStreams, SMStreamHbRspMsg* pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t streamNum = taosHashGetSize(pStreams);
  void* pIter = NULL;
  
  pRsp->deploy.taskList = taosArrayInit(streamNum, sizeof(SStreamTasksDeploy));
  TSDB_CHECK_NULL(pRsp->deploy.taskList, code, lino, _exit, terrno);

  while (1) {
    pIter = taosHashIterate(pStreams, pIter);
    if (pIter == NULL) {
      break;
    }
    
    SStreamTasksDeploy *pDeploy = (SStreamTasksDeploy *)pIter;
    TSDB_CHECK_NULL(taosArrayPush(pRsp->deploy.taskList, pDeploy));
  }
  
_exit:

  if (pIter) {
    taosHashCancelIterate(pStreams, pIter);
  }
  
  return code;
}

void msmCleanDeployedVgTasks(SArray* pVgLeaders) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t vgNum = taosArrayGetSize(pVgLeaders);
  SStreamVgReaderTasksDeploy* pVg = NULL;
  
  for (int32_t i = 0; i < vgNum; ++i) {
    int32_t* vgId = taosArrayGet(pVgLeaders, i);
    pVg = taosHashAcquire(mStreamMgmt.toDeployVgMap, vgId, sizeof(*vgId));
    if (NULL == pVg) {
      continue;
    }

    if (taosWTryLockLatch(&pVg->lock)) {
      continue;
    }
    
    if (atomic_load_32(&pVg->deployed) <= 0) {
      taosWUnLockLatch(&pVg->lock);
      continue;
    }

    int32_t taskNum = taosArrayGetSize(pVg->taskList);
    if (atomic_load_32(&pVg->deployed) == taskNum) {
      atomic_sub_fetch_32(&mStreamMgmt.toDeployVgTaskNum, taskNum);
      taosArrayDestroy(pVg->taskList);
      taosHashRemove(mStreamMgmt.toDeployVgMap, vgId, sizeof(*vgId));
      taosWUnLockLatch(&pVg->lock);
      continue;
    }

    for (int32_t m = taskNum - 1; m >= 0; --m) {
      SStreamDeployTaskExtInfo* pExt = taosArrayGet(pVg->taskList, m);
      if (!pExt->deployed) {
        continue;
      }
      taosArrayRemove(pVg->taskList, m);
    }
    atomic_store_32(&pVg->deployed, 0);
    taosWUnLockLatch(&pVg->lock);
  }

_exit:

  if (code && pVg) {
    taosRUnLockLatch(&pVg->lock);
  }
}

void msmCleanDeployedSnodeTasks (int32_t snodeId) {
  int32_t code = TSDB_CODE_SUCCESS;
  SStreamSnodeTasksDeploy* pSnode = NULL;
  
  pSnode = taosHashAcquire(mStreamMgmt.toDeploySnodeMap, &snodeId, sizeof(snodeId));
  if (NULL == pSnode) {
    return;
  }

  if (taosWTryLockLatch(&pSnode->lock)) {
    return;
  }
  
  if (atomic_load_32(&pSnode->deployed) <= 0) {
    taosWUnLockLatch(&pSnode->lock);
    return;
  }
  
  int32_t taskNum = taosArrayGetSize(pSnode->triggerTaskList);
  
  for (int32_t m = taskNum - 1; m >= 0; --m) {
    SStreamDeployTaskExtInfo* pExt = taosArrayGet(pSnode->triggerTaskList, m);
    if (!pExt->deployed) {
      continue;
    }
    taosArrayRemove(pSnode->triggerTaskList, m);
  }

  taskNum = taosArrayGetSize(pSnode->runnerTaskList);
  for (int32_t m = taskNum - 1; m >= 0; --m) {
    SStreamDeployTaskExtInfo* pExt = taosArrayGet(pSnode->runnerTaskList, m);
    if (!pExt->deployed) {
      continue;
    }
    taosArrayRemove(pSnode->runnerTaskList, m);
  }
  
  taosWUnLockLatch(&pSnode->lock);
}

void msmCleanStreamGrpCtx(SStreamHbMsg* pHb) {
  if (atomic_load_32(&mStreamMgmt.toDeployVgTaskNum) > 0) {
    TAOS_CHECK_EXIT(msmCleanDeployedVgTasks(pHb->pVgLeaders));
  }

  if (atomic_load_32(&mStreamMgmt.toDeploySnodeTaskNum) > 0) {
    TAOS_CHECK_EXIT(msmCleanDeployedSnodeTasks(pHb->snodeId));
  }

  int32_t tidx = streamGetThreadIdx(mStreamMgmt.threadNum, pHb->streamGId);
  taosHashClear(mStreamMgmt.threadCtx[tidx].toHandleStm[pHb->streamGId]);
  taosHashClear(mStreamMgmt.threadCtx[tidx].deployStm[pHb->streamGId]);
}

void msmHandleStatusUpdateSta(int32_t tidx, int32_t gid, SStmTaskStatusMsg* pMsg, int64_t currTs) {
  SStreamStatus* pStatus = taosHashGet(mStreamMgmt.streamMap, &pMsg->streamId, sizeof(pMsg->streamId));
  if ((currTs - pStatus->lastActTs) < STREAM_ACT_MIN_DELAY_MSEC) {
    return;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t action = 0;
  SHashObj* pHash = mStreamMgmt.threadCtx[tidx].toHandleStm[gid];
  switch (pMsg->status) {
    case STREAM_STATUS_INIT:
      int32_t *pAction = taosHashGet(pHash, &pMsg->streamId, sizeof(pMsg->streamId));
      if (pAction) {
        *pAction |= STREAM_ACT_START;
      } else {
        action = STREAM_ACT_START;
        TAOS_CHECK_EXIT(taosHashPut(pHash, &pMsg->streamId, sizeof(pMsg->streamId), &action, sizeof(action)));
      }
      break;
    case STREAM_STATUS_STOPPED:
    case STREAM_STATUS_FAILED:
    default:
      break;
  }

_exit:

  int64_t streamId = pMsg->streamId;
  
  mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
}

int32_t msmHandleStreamStatusUpdate(SMnode* pMnode, int32_t tidx, int32_t gid, int64_t currTs, SArray* pStatusList) {
  int32_t num = taosArrayGetSize(pStatusList);
  for (int32_t i = 0; i < num; ++i) {
    SStmTaskStatusMsg* pStatus = taosArrayGet(pStatusList, i);
    SStreamTaskStatus** ppStatus = taosHashGet(mStreamMgmt.taskMap, &pStatus->streamId, sizeof(pStatus->streamId) + sizeof(pStatus->taskId));
    if (NULL == ppStatus) {
      msmHandleStatusUpdateErr(tidx, gid, STM_ERR_TASK_NOT_EXISTS, pStatus);
      continue;
    }
    (*ppStatus)->status = pStatus->status;
    (*ppStatus)->lastUpTs = currTs;
    
    if (STREAM_STATUS_RUNNING != pStatus->status) {
      msmHandleStatusUpdateSta(tidx, gid, pStatus, currTs);
    }
  }
}

bool msmCheckStreamStartCond(int64_t streamId, int32_t snodeId, SStreamTaskId* id) {
  SStreamStatus* pStream = taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (NULL == pStream) {
    return false;
  }

  if (pStream->triggerTask.id.nodeId != snodeId || STREAM_STATUS_INIT != pStream->triggerTask.status) {
    return false;
  }

  int32_t readerNum = taosArrayGetSize(pStream->readerTaskList);
  for (int32_t i = 0; i < readerNum; ++i) {
    SStreamTaskStatus* pStatus = taosArrayGet(pStream->readerTaskList, i);
    if (STREAM_STATUS_INIT != pStatus->status && STREAM_STATUS_RUNNING != pStatus->status) {
      return false;
    }
  }

  int32_t runnerNum = taosArrayGetSize(pStream->runnerTaskList);
  for (int32_t i = 0; i < runnerNum; ++i) {
    SStreamTaskStatus* pStatus = taosArrayGet(pStream->runnerTaskList, i);
    if (STREAM_STATUS_INIT != pStatus->status && STREAM_STATUS_RUNNING != pStatus->status) {
      return false;
    }
  }

  *id = pStream->triggerTask.id;
  
  return true;
}

void msmAppendStartToHbRspMsg(int64_t streamId, SMStreamHbRspMsg* pRsp, int32_t streamNum, SStreamTaskId* id) {
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
  int32_t streamNum = taosHashGetSize(mStreamMgmt.threadCtx[tidx].toHandleStm[gid]);
  SHashObj* pHash = mStreamMgmt.threadCtx[tidx].toHandleStm[gid];
  while (1) {
    pIter = taosHashIterate(pHash, pIter);
    if (pIter == NULL) {
      break;
    }

    int64_t* pStreamId = taosHashGetKey(pIter, NULL);
    int32_t *actions = (int32_t *)pIter;
    
    SStreamTaskId id = {0};
    if (STREAM_ACT_START == *actions && msmCheckStreamStartCond(*pStreamId, snodeId, &id)) {
      msmAppendStartToHbRspMsg(*pStreamId, pRsp, streamNum, &id);
    }
  }
  
_exit:

  if (pIter) {
    taosHashCancelIterate(pHash, pIter);
  }
  
}

int32_t msmHandleStreamHbMsg(SMnode* pMnode, int64_t currTs, SStreamHbMsg* pHb, SMStreamHbRspMsg* pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t tidx = streamGetThreadIdx(mStreamMgmt.threadNum, pHb->streamGId);
  
  if (atomic_load_64(&mStreamMgmt.threadCtx[tidx].actionQ->qRemainNum) > 0) {
    TAOS_CHECK_EXIT(msmHandleStreamActions(pMnode, mStreamMgmt.threadCtx[tidx].actionQ));
  }

  if (atomic_load_32(&mStreamMgmt.toDeployVgTaskNum) > 0) {
    TAOS_CHECK_EXIT(msmAddTasksBasedOnVgLeader(pMnode, pHb->pVgLeaders, pHb->streamGId));
  }

  if (atomic_load_32(&mStreamMgmt.toDeploySnodeTaskNum) > 0) {
    TAOS_CHECK_EXIT(msmAddTasksBasedOnSnode(pMnode, pHb->snodeId, pHb->streamGId));
  }

  if (taosHashGetSize(mStreamMgmt.threadCtx[tidx].deployStm[pHb->streamGId]) > 0) {
    TAOS_CHECK_EXIT(msmAddHbRspDeploy(pMnode, mStreamMgmt.threadCtx[tidx].deployStm[pHb->streamGId], pRsp));
  }

  if (taosArrayGetSize(pHb->pStreamStatus) > 0) {
    TAOS_CHECK_EXIT(msmHandleStreamStatusUpdate(pMnode, tidx, pHb->streamGId, currTs, pHb->pStreamStatus));
  }

  if (taosHashGetSize(mStreamMgmt.threadCtx[tidx].toHandleStm[pHb->streamGId]) > 0 && GOT_SNODE(pHb->snodeId)) {
    TAOS_CHECK_EXIT(msmHandleStreamUpdatePostActions(pMnode, tidx, pHb->streamGId, pHb->snodeId, pRsp));
  }

_exit:

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



