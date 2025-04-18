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

void msmDestroyRuntimeInfo() {

}

int32_t msmInitRuntimeInfo(SMnode *pMnode) {
  int32_t code = taosThreadMutexInit(&mStreamMgmt.lock, NULL);
  if (code) {
    return code;
  }

  int32_t vnodeNum = sdbGetSize(pMnode->pSdb, SDB_VGROUP);
  int32_t snodeNum = sdbGetSize(pMnode->pSdb, SDB_SNODE);
  int32_t dnodeNum = sdbGetSize(pMnode->pSdb, SDB_DNODE);

  mStreamMgmt.threadNum = tsNumOfMnodeStreamMgmtThreads;
  for (int32_t i = 0; i < mStreamMgmt.threadNum; ++i) {
    mStreamMgmt.threadGrp[i].actionQ = taosMemoryCalloc(1, sizeof(SStreamActionQ));
    if (mStreamMgmt.threadGrp[i].actionQ == NULL) {
      code = terrno;
      mError("failed to initialize the stream runtime actionQ[%d], code:%s", i, tstrerror(terrno));
      goto _return;
    }

    for (int32_t m = 0; m < STREAM_MAX_GROUP_NUM; ++m) {
      mStreamMgmt.threadGrp[i].deployStm[m] = taosHashInit(snodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
      if (mStreamMgmt.threadGrp[i].deployStm[m] == NULL) {
        code = terrno;
        mError("failed to initialize the stream runtime deployStm[%d][%d], code:%s", i, m, tstrerror(terrno));
        goto _return;
      }
    }
  }
  mStreamMgmt.streamMap = taosHashInit(MND_STREAM_DEFAULT_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.streamMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime streamMap, code:%s", tstrerror(terrno));
    goto _return;
  }
  mStreamMgmt.taskMap = taosHashInit(MND_STREAM_DEFAULT_TASK_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.taskMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime taskMap, code:%s", tstrerror(terrno));
    goto _return;
  }
  mStreamMgmt.vgroupMap = taosHashInit(vnodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.vgroupMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime vgroupMap, code:%s", tstrerror(terrno));
    goto _return;
  }
  mStreamMgmt.snodeMap = taosHashInit(snodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.snodeMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime snodeMap, code:%s", tstrerror(terrno));
    goto _return;
  }
  mStreamMgmt.dnodeMap = taosHashInit(dnodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.dnodeMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime dnodeMap, code:%s", tstrerror(terrno));
    goto _return;
  }

  mStreamMgmt.toDeployVgMap = taosHashInit(vnodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.toDeployVgMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime toDeployVgMap, code:%s", tstrerror(terrno));
    goto _return;
  }
  mStreamMgmt.toDeploySnodeMap = taosHashInit(snodeNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (mStreamMgmt.toDeploySnodeMap == NULL) {
    code = terrno;
    mError("failed to initialize the stream runtime toDeploySnodeMap, code:%s", tstrerror(terrno));
    goto _return;
  }

  taosHashSetFreeFp(mStreamMgmt.nodeMap, freeTaskList);
  taosHashSetFreeFp(mStreamMgmt.streamMap, freeTaskList);

_return:

  if (code) {
    msmDestroyRuntimeInfo();
  }

  return code;
}

static int32_t msmAppendTasksFromObj(SMnode* pMnode, SStreamObj* pStream, SArray* pTasks) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t taskNum = taosArrayGetSize(pTasks);
  int64_t key[2] = {pStream->pCreate->streamId, 0};
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SStreamTaskState* pState = taosArrayGet(pTasks, i);
    key[1] = pState->taskId;
    TSDB_CHECK_CODE(taosHashPut(mStreamMgmt.taskMap, key, sizeof(key), &pState, POINTER_BYTES), lino, _return);
  }
  
_return:

  return code;
}

static int32_t msmAppendVgReaderTaskToMap(SHashObj* pVgMap, SStreamTaskState* pState, SStreamObj* pStream) {
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


static int32_t msmAppendSnodeTaskToMap(SHashObj* pHash, SStreamTaskState* pState, SStreamObj* pStream, bool triggerTask) {
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
    SStreamTaskState* pState = taosArrayGet(pTasks, i);
    TSDB_CHECK_CODE(msmAppendVgReaderTaskToMap(mStreamMgmt.vgroupMap, pState, pStream), lino, _return);
    SStreamDeployTaskInfo info = {0};
    TSDB_CHECK_CODE(msmGetStreamTaskDeployInfo(&info, streamId, &pState->id, pStream), lino, _return);
    TSDB_CHECK_CODE(msmAppendVgReaderTaskToDeploy(mStreamMgmt.toDeployVgMap, &info, pStream), lino, _return);
    atomic_add_fetch_32(&mStreamMgmt.toDeployVgTaskNum, 1);
  }
  
_return:

  return code;
}

static int32_t msmAppendSnodeTasksFromObj(SMnode* pMnode, SStreamObj* pStream, SArray* pTasks, bool triggerTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  int32_t taskNum = taosArrayGetSize(pTasks);
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SStreamTaskState* pState = taosArrayGet(pTasks, i);
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


static int32_t msmAppendTriggerTasks(SMnode* pMnode, SStreamTasksInfo* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SSdb   *pSdb = pMnode->pSdb;
  SStreamTaskState state = {.taskId = INT64_MIN, .nodeId = INT64_MIN, .taskIdx = -1, .lastUpTs = INT64_MIN};
  SStreamTaskState* pState = NULL;

  pInfo->triggerTaskList = taosArrayInit(1, sizeof(SStreamTaskState));
  TSDB_CHECK_NULL(pInfo->triggerTaskList, code, lino, _return, terrno);
  state.nodeId = msmAssignStreamTriggerSnodeId(pStream->pCreate->streamId);
  state.taskIdx = 0;
  pState = taosArrayPush(pInfo->triggerTaskList, &state);
  TSDB_CHECK_NULL(pState, code, lino, _return, terrno);

  TSDB_CHECK_CODE(msmAppendTasksFromObj(pMnode, pStream, pInfo->triggerTaskList), lino, _return);
  TSDB_CHECK_CODE(msmAppendSnodeTasksFromObj(pMnode, pStream, pInfo->triggerTaskList, true), lino, _return);

_return:

  return code;
}

static int32_t msmAppendReaderTriggerTasks(SMnode* pMnode, SArray* pReader, SStreamObj* pStream, int16_t* taskIdx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SSdb   *pSdb = pMnode->pSdb;
  SStreamTaskState state = {.taskId = INT64_MIN, .nodeId = INT64_MIN, .taskIdx = *taskIdx, .lastUpTs = INT64_MIN};
  SStreamReaderTasksState tasks = {0};
  
  switch (pStream->pCreate->triggerTblType) {
    case TSDB_NORMAL_TABLE:
    case TSDB_CHILD_TABLE: {
      tasks.vgList = taosArrayInit(1, sizeof(SStreamTaskState));
      TSDB_CHECK_NULL(tasks.vgList, code, lino, _return, terrno);
      state.nodeId = pStream->pCreate->triggerTblVgId;
      state.taskIdx = 0;
      TSDB_CHECK_NULL(taosArrayPush(tasks.vgList, &state), code, lino, _return, terrno);
      break;
    }
    case TSDB_SUPER_TABLE: {
      SDbObj* pDb = mndAcquireDb(pMnode, pStream->pCreate->triggerDB);
      if (NULL == pDb) {
        code = terrno;
        mstError("failed to acquire db %s, error:%s", pStream->pCreate->triggerDB, terrstr());
        goto _return;
      }

      tasks.vgList = taosArrayInit(pDb->cfg.numOfVgroups, sizeof(SStreamTaskState));
      TSDB_CHECK_NULL(tasks.vgList, code, lino, _return, terrno);
      
      void *pIter = NULL;
      while (1) {
        SVgObj *pVgroup = NULL;
        pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
        if (pIter == NULL) {
          break;
        }
      
        if (pVgroup->dbUid == pDb->uid && !pVgroup->isTsma) {
          state.nodeId = pVgroup->vgId;
          state.taskIdx++;
          TSDB_CHECK_NULL(taosArrayPush(tasks.vgList, &state), code, lino, _return, terrno);
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

  TSDB_CHECK_NULL(taosArrayPush(pReader, &tasks), code, lino, _return, terrno);

_return:

  *taskIdx = state.taskIdx;

  return code;
}

static int32_t msmAppendReaderCalcTasks(SMnode* pMnode, SArray* pReader, SStreamObj* pStream, int16_t* taskIdx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t calcTasksNum = taosArrayGetSize(pStream->pCreate->calcScanPlanList);
  int64_t streamId = pStream->pCreate->streamId;
  SStreamTaskState state = {.taskId = INT64_MIN, .nodeId = INT64_MIN, .taskIdx = *taskIdx, .lastUpTs = INT64_MIN};
  SStreamReaderTasksState tasks = {0};

  for (int32_t i = 0; i < calcTasksNum; ++i) {
    SStreamCalcScan* pScanList = taosArrayGet(pStream->pCreate->calcScanPlanList, i);
    int32_t vgNum = taosArrayGetSize(pScanList->vgList);
    tasks.vgList = taosArrayInit(vgNum, sizeof(SStreamTaskState));
    TSDB_CHECK_NULL(tasks.vgList, code, lino, _return, terrno);
    for (int32_t m = 0; m < vgNum; ++m) {
      state.nodeId = *(int32_t*)taosArrayGet(pScanList->vgList, i);
      state.taskIdx++;
      TSDB_CHECK_NULL(taosArrayPush(tasks.vgList, &state), code, lino, _return, terrno);
    }
    
    TSDB_CHECK_NULL(taosArrayPush(pReader, &tasks), code, lino, _return, terrno);
  }

_return:

  return code;
}


static int32_t msmAppendReaderTasks(SMnode* pMnode, SStreamTasksInfo* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t calcTasksNum = taosArrayGetSize(pStream->pCreate->calcScanPlanList);
  int64_t streamId = pStream->pCreate->streamId;
  int16_t taskIdx = -1;

  pInfo->readerTaskList = taosArrayInit(1 + calcTasksNum, sizeof(SStreamReaderTasksState));
  TSDB_CHECK_NULL(pInfo->readerTaskList, code, lino, _return, terrno);
  
  TSDB_CHECK_CODE(msmAppendReaderTriggerTasks(pMnode, pInfo->readerTaskList, pStream, &taskIdx), lino, _return);
  TSDB_CHECK_CODE(msmAppendReaderCalcTasks(pMnode, pInfo->readerTaskList, pStream, &taskIdx), lino, _return);

  int32_t taskListNum = taosArrayGetSize(pInfo->readerTaskList);
  for (int32_t i = 0; i < taskListNum; ++i) {
    SStreamReaderTasksState* pTasks = taosArrayGet(pInfo->readerTaskList, i);
    TSDB_CHECK_CODE(msmAppendTasksFromObj(pMnode, pStream, pTasks->vgList), lino, _return);
    TSDB_CHECK_CODE(msmAppendVgReaderTasksFromObj(pMnode, pStream, pTasks->vgList), lino, _return);
  }
  
_return:

  return code;
}

static int32_t msmAppendRunnerTasks(SMnode* pMnode, SStreamTasksInfo* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;
  SSdb   *pSdb = pMnode->pSdb;
  SStreamTaskState state = {.taskId = INT64_MIN, .nodeId = INT64_MIN, .taskIdx = -1, .lastUpTs = INT64_MIN};

  pInfo->runnerTaskList = taosArrayInit(1, sizeof(SStreamTaskState));
  TSDB_CHECK_NULL(pInfo->runnerTaskList, code, lino, _return, terrno);
  state.nodeId = msmAssignStreamRunnerSnodeId(pStream->pCreate->streamId);
  state.taskIdx++;
  TSDB_CHECK_NULL(taosArrayPush(pInfo->runnerTaskList, &state), code, lino, _return, terrno);
  TSDB_CHECK_CODE(msmAppendTasksFromObj(pMnode, pStream, pInfo->runnerTaskList), lino, _return);
  TSDB_CHECK_CODE(msmAppendSnodeTasksFromObj(pMnode, pStream, pInfo->runnerTaskList, false), lino, _return);

_return:

  return code;
}


static int32_t msmBuildStreamTasksFromObj(SMnode* pMnode, SStreamTasksInfo* pInfo, SStreamObj* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStreamTaskState state = {.taskId = INT64_MIN, .nodeId = INT64_MIN, .lastUpTs = INT64_MIN};

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
  SStreamTasksInfo info = {0};

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

  SStreamTasksInfo** ppStream = (SStreamTasksInfo**)taosHashGet(mStreamMgmt.streamMap, &streamId, sizeof(streamId));
  if (ppStream) {

  }


  TSDB_CHECK_CODE(mndAcquireStream(pMnode, streamName, &pStream), lino, _return);

  TSDB_CHECK_CODE(msmAppendStreamTasksFromObj(pMnode, pStream), lino, _return);


_return:

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
      case STREAM_ACTION_DEPLOY:
        TSDB_CHECK_CODE(msmLaunchStreamDepolyAction(pMnode, pQNode), lino, _return);
        break;
      case STREAM_ACTION_UNDEPLOY:
      default:
        break;
    }
  }

_return:

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
    
    TAOS_CHECK_EXIT(msmAddDeployTaskToDnode(mStreamMgmt.threadGrp[tidx].deployStm[gid], pVg->taskList, &pVg->deployed));
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
    TAOS_CHECK_EXIT(msmAddDeployTaskToDnode(mStreamMgmt.threadGrp[tidx].deployStm[gid], pSnode->triggerTaskList, &pSnode->triggerDeployed));
  }

  if (atomic_load_32(&pSnode->runnerDeployed) < taosArrayGetSize(pSnode->runnerTaskList)) {
    TAOS_CHECK_EXIT(msmAddDeployTaskToDnode(mStreamMgmt.threadGrp[tidx].deployStm[gid], pSnode->runnerTaskList, &pSnode->runnerDeployed));
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

void msmCleanDeployedTasks(SStreamHbMsg* pHb) {
  if (atomic_load_32(&mStreamMgmt.toDeployVgTaskNum) > 0) {
    TAOS_CHECK_EXIT(msmCleanDeployedVgTasks(pHb->pVgLeaders));
  }

  if (atomic_load_32(&mStreamMgmt.toDeploySnodeTaskNum) > 0) {
    TAOS_CHECK_EXIT(msmCleanDeployedSnodeTasks(pHb->snodeId));
  }
}

int32_t msmHandleStreamHbMsg(SMnode* pMnode, SStreamHbMsg* pHb, SMStreamHbRspMsg* pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t tidx = streamGetThreadIdx(mStreamMgmt.threadNum, pHb->streamGId);
  
  if (atomic_load_64(&mStreamMgmt.threadGrp[tidx].actionQ->qRemainNum) > 0) {
    TAOS_CHECK_EXIT(msmHandleStreamActions(pMnode, mStreamMgmt.threadGrp[tidx].actionQ));
  }

  if (atomic_load_32(&mStreamMgmt.toDeployVgTaskNum) > 0) {
    TAOS_CHECK_EXIT(msmAddTasksBasedOnVgLeader(pMnode, pHb->pVgLeaders, pHb->streamGId));
  }

  if (atomic_load_32(&mStreamMgmt.toDeploySnodeTaskNum) > 0) {
    TAOS_CHECK_EXIT(msmAddTasksBasedOnSnode(pMnode, pHb->snodeId, pHb->streamGId));
  }

  if (taosHashGetSize(mStreamMgmt.threadGrp[tidx].deployStm[pHb->streamGId]) > 0) {
    TAOS_CHECK_EXIT(msmAddHbRspDeploy(pMnode, mStreamMgmt.threadGrp[tidx].deployStm[pHb->streamGId], pRsp));
  }

_exit:

  return code;
}


