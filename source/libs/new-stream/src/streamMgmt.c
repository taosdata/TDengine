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

#include "executor.h"
#include "streamInt.h"
#include "tmisce.h"
#include "tref.h"
#include "stream.h"
#include "ttimer.h"
#include "wal.h"



static int32_t smAddTaskToVgroupMap(SStreamReaderTask* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pTask->task.streamId;
  SStreamVgReaderTasks vg = {0};

  while (true) {
    SStreamVgReaderTasks* pVg = taosHashAcquire(gStreamMgmt.vgroupMap, &pTask->task.nodeId, sizeof(pTask->task.nodeId));
    if (NULL == pVg) {
      vg.taskList = taosArrayInit(20, POINTER_BYTES);
      TSDB_CHECK_NULL(vg.taskList, code, lino, _return, terrno);
      TSDB_CHECK_NULL(taosArrayPush(vg.taskList, &pTask), code, lino, _return, terrno);
      code = taosHashPut(gStreamMgmt.vgroupMap, &pTask->task.nodeId, sizeof(pTask->task.nodeId), &pTask, POINTER_BYTES);
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
    if (NULL == taosArrayPush(pVg->taskList, &pTask)) {
      taosWUnLockLatch(&pVg->lock);
      TSDB_CHECK_NULL(NULL, code, lino, _return, terrno);
    }
    taosWUnLockLatch(&pVg->lock);
    
    taosHashRelease(gStreamMgmt.vgroupMap, pVg);
    break;
  }
  
_return:

  mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

  return code;
}

static int32_t smAddTaskToSnodeList(SStreamTask* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pTask->streamId;

  taosWLockLatch(&gStreamMgmt.snodeLock);
  TSDB_CHECK_NULL(taosArrayPush(gStreamMgmt.snodeTasks, &pTask), code, lino, _return, terrno);
  taosWUnLockLatch(&gStreamMgmt.snodeLock);

  return code;
  
_return:

  taosWUnLockLatch(&gStreamMgmt.snodeLock);

  mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

  return code;
}


int32_t smDeployStreamTasks(SStmStreamDeploy* pDeploy) {
  int64_t streamId = pDeploy->streamId;
  int32_t gid = STREAM_GID(streamId);
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SHashObj* pGrp = gStreamMgmt.stmGrp[gid];
  
  if (NULL == pGrp) {
    gStreamMgmt.stmGrp[gid] = taosHashInit(STREAM_GRP_STREAM_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    TSDB_CHECK_NULL(gStreamMgmt.stmGrp[gid], code, lino, _exit, terrno);
    pGrp = gStreamMgmt.stmGrp[gid];
  }
  
  SStreamTasksInfo* pStream = taosHashGet(pGrp, &streamId, sizeof(streamId));
  if (NULL != pStream) {
    //STREAMTODO
    //mstError("stream already exists, ignore new deploy");
    goto _exit;
  }

  SStreamTasksInfo stream = {0};
  if (pDeploy->readerTasks) {
    int32_t readerNum = taosArrayGetSize(pDeploy->readerTasks);
    stream.readerList = taosArrayInit(readerNum, sizeof(SStreamReaderTask));
    TSDB_CHECK_NULL(stream.readerList, code, lino, _exit, terrno);
    
    for (int32_t i = 0; i < readerNum; ++i) {
      SStreamReaderTask* pTask = taosArrayReserve(stream.readerList, 1);
      SStmTaskDeploy* pReader = taosArrayGet(pDeploy->readerTasks, i);
      pTask->task = pReader->task;
      //STREAMTODO
      //TAOS_CHECK_EXIT(stReaderTaskDeploy(pTask, &pReader->msg));

      TAOS_CHECK_EXIT(taosHashPut(gStreamMgmt.taskMap, &pTask->task.streamId, sizeof(pTask->task.streamId) + sizeof(pTask->task.taskId), &pTask, POINTER_BYTES));

      TAOS_CHECK_EXIT(smAddTaskToVgroupMap(pTask));

      ST_TASK_DLOG("task deploy succeed, tidx:%d", pTask->task.taskIdx);      
    }

    stream.taskNum += readerNum;
  }

  if (pDeploy->triggerTask) {
    stream.triggerTask = taosMemoryCalloc(1, sizeof(SStreamTriggerTask));
    TSDB_CHECK_NULL(stream.triggerTask, code, lino, _exit, terrno);

    SStreamTask* pTask = &pDeploy->triggerTask->task;
    stream.triggerTask->task = *pTask;
    //STREAMTODO
    //TAOS_CHECK_EXIT(stTriggerTaskDeploy(stream.triggerTask, &pDeploy->triggerTask->msg));

    TAOS_CHECK_EXIT(taosHashPut(gStreamMgmt.taskMap, &pTask->streamId, sizeof(pTask->streamId) + sizeof(pTask->taskId), &stream.triggerTask, POINTER_BYTES));

    //TAOS_CHECK_EXIT(smAddTaskToSnodeList(stream.triggerTask));

    ST_TASK_DLOG("task deploy succeed, tidx:%d", pTask->taskIdx);      

    stream.taskNum++;
  }

  if (pDeploy->runnerTasks) {
    int32_t runnerNum = taosArrayGetSize(pDeploy->runnerTasks);
    stream.runnerList = taosArrayInit(runnerNum, sizeof(SStreamRunnerTask));
    TSDB_CHECK_NULL(stream.runnerList, code, lino, _exit, terrno);
    
    for (int32_t i = 0; i < runnerNum; ++i) {
      SStreamRunnerTask* pTask = taosArrayReserve(stream.runnerList, 1);
      SStmTaskDeploy* pRunner = taosArrayGet(pDeploy->runnerTasks, i);
      pTask->task = pRunner->task;
      TAOS_CHECK_EXIT(stRunnerTaskDeploy(pTask, &pRunner->msg.runner));

      TAOS_CHECK_EXIT(taosHashPut(gStreamMgmt.taskMap, &pTask->task.streamId, sizeof(pTask->task.streamId) + sizeof(pTask->task.taskId), &pTask, POINTER_BYTES));

      ST_TASK_DLOG("task deploy succeed, tidx:%d", pTask->task.taskIdx);      
    }

    stream.taskNum += runnerNum;    
  }  

  TAOS_CHECK_EXIT(taosHashPut(pGrp, &streamId, sizeof(streamId), &stream, sizeof(stream)));

  mstInfo("stream deploy succeed, taskNum:%d", stream.taskNum);

  return code;

_exit:

  mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

  return code;
}

int32_t smDeployTasks(SStreamDeployActions* actions) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = 0;
  int32_t listNum = taosArrayGetSize(actions->streamList);
  
  for (int32_t i = 0; i < listNum; ++i) {
    SStmStreamDeploy* pDeploy = taosArrayGet(actions->streamList, i);
    streamId = pDeploy->streamId;

    TAOS_CHECK_EXIT(smDeployStreamTasks(pDeploy));
  }

  return code;

_exit:

  mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

  return code;
}



int32_t smStartStreamTasks(SStreamTasksStart* pStart) {
  int64_t streamId = pStart->task.streamId;
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  
  SStreamTask* pTask = taosHashGet(gStreamMgmt.taskMap, &pStart->task.streamId, sizeof(pStart->task.streamId) + sizeof(pStart->task.taskId));
  if (NULL == pTask) {
    mstError("stream not exists while try to start task %" PRId64, pStart->task.taskId);
    goto _exit;
  }

  //STREAMTODO
  //TAOS_CHECK_EXIT(stTriggerTaskExecute(pTask, &pStart->startMsg));

  ST_TASK_DLOG("stream start succeed, tidx:%d", pTask->taskIdx);

  return code;

_exit:

  mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

  return code;
}

void smRemoveReaderTask(SStreamTask* pTask) {
  SStreamVgReaderTasks* pTasks = taosHashGet(gStreamMgmt.vgroupMap, &pTask->nodeId, sizeof(pTask->nodeId));
  if (NULL == pTasks) {
    ST_TASK_WLOG("vgroup not exists, tidx:%d", pTask->taskIdx);
    return;
  }

  taosWLockLatch(&pTasks->lock);
  int32_t taskNum = taosArrayGetSize(pTasks->taskList);
  for (int32_t i = 0; i < taskNum; ++i) {
    SStreamTask** ppTask = taosArrayGet(pTasks->taskList, i);
    if ((*ppTask)->taskId == pTask->taskId) {
      ST_TASK_ILOG("task removed from vgroupMap, tidx:%d", pTask->taskIdx);
      taosArrayRemove(pTasks->taskList, i);
      break;
    }
  }
  taosWUnLockLatch(&pTasks->lock);
}

void smRemoveTaskFromStreamMap(SStreamTask* pTask) {

}

int32_t smUndeployStreamTasks(SStreamTasksUndeploy* pUndeploy) {
  int64_t streamId = pUndeploy->task.streamId;
  int64_t key[2] = {streamId, pUndeploy->task.taskId};
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  
  SStreamTask* pTask = taosHashGet(gStreamMgmt.taskMap, key, sizeof(key));
  if (NULL == pTask) {
    mstError("stream not exists while try to undeploy task %" PRId64, key[1]);
    goto _exit;
  }

  if (STREAM_READER_TASK == pTask->type) {
    smRemoveReaderTask(pTask);
  }

  (void)taosHashRemove(gStreamMgmt.taskMap, key, sizeof(key));

  smRemoveTaskFromStreamMap(&pUndeploy->task);

  switch (pTask->type) {
    case STREAM_READER_TASK:
      //STREAMTODO
      //TAOS_CHECK_EXIT(stReadderTaskUndeploy(pTask, &pUndeploy->undeployMsg));
      break;
    case STREAM_TRIGGER_TASK:
      //STREAMTODO
      //TAOS_CHECK_EXIT(stTriggerTaskUndeploy(pTask, &pUndeploy->undeployMsg));
      break;
    case STREAM_RUNNER_TASK:
      TAOS_CHECK_EXIT(stRunnerTaskUndeploy((SStreamRunnerTask*)pTask, &pUndeploy->undeployMsg));
      break;
    default:
      TAOS_CHECK_EXIT(TSDB_CODE_STREAM_INTERNAL_ERROR);
      break;
  }
  
  ST_TASK_ILOG("stream start succeed, tidx:%d", pTask->taskIdx);

  return code;

_exit:

  mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

  return code;
}



int32_t smStartTasks(SStreamStartActions* actions) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = 0;
  int32_t listNum = taosArrayGetSize(actions->taskList);
  
  for (int32_t i = 0; i < listNum; ++i) {
    SStreamTasksStart* pStart = taosArrayGet(actions->taskList, i);
    streamId = pStart->task.streamId;

    TAOS_CHECK_EXIT(smStartStreamTasks(pStart));
  }

  return code;

_exit:

  mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

  return code;
}

void smUndeployAllTasks(void) {
  //STREAMTODO
}

int32_t smUndeployTasks(SStreamUndeployActions* actions) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = 0;
  int32_t listNum = taosArrayGetSize(actions->taskList);
  
  for (int32_t i = 0; i < listNum; ++i) {
    SStreamTasksUndeploy* pUndeploy = taosArrayGet(actions->taskList, i);
    streamId = pUndeploy->task.streamId;
    
    TAOS_CHECK_EXIT(smUndeployStreamTasks(pUndeploy));
  }

  return code;

_exit:

  mstError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

  return code;
}



