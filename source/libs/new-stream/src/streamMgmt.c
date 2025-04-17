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



static int32_t smAppendVgReaderTask(SStreamReaderTask* pTask) {
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

  return code;
}

static int32_t smAppendSnodeTask(SStreamReaderTask* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pTask->task.streamId;

  taosWLockLatch(&gStreamMgmt.snodeLock);
  TSDB_CHECK_NULL(taosArrayPush(gStreamMgmt.snodeTasks, &pTask), code, lino, _return, terrno);
      
_return:

  taosWUnLockLatch(&gStreamMgmt.snodeLock);

  return code;
}


int32_t smDeployStreamTasks(SStreamTasksDeploy* pDeploy) {
  int64_t streamId = pDeploy->streamId;
  int64_t key[2] = {streamId, 0};
  int32_t streamGrp = STREAM_GID(streamId);
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SHashObj* pGrp = gStreamMgmt.streamGrp[streamGrp];
  
  if (NULL == pGrp) {
    gStreamMgmt.streamGrp[streamGrp] = taosHashInit(STREAM_GRP_DEFAULT_STREAM_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    TSDB_CHECK_NULL(gStreamMgmt.streamGrp[streamGrp], code, lino, _exit, terrno);
    pGrp = gStreamMgmt.streamGrp[streamGrp];
  }
  
  SStreamTask* pTask = taosHashGet(pGrp, &streamId, sizeof(streamId));
  if (NULL != pTask) {
    ST_TASK_WLOG("stream already exists, ignore new deploy");
    goto _exit;
  }

  SStreamTasksInfo tasks = {0};
  if (pDeploy->readerTasks) {
    int32_t readerNum = taosArrayGetSize(pDeploy->readerTasks);
    tasks.readerTaskList = taosArrayInit(readerNum, sizeof(SStreamReaderTask));
    TSDB_CHECK_NULL(tasks.readerTaskList, code, lino, _exit, terrno);
    for (int32_t i = 0; i < readerNum; ++i) {
      SStreamReaderTask* pTask = taosArrayReserve(tasks.readerTaskList, 1);
      SStreamDeployTaskInfo* pReader = taosArrayGet(pDeploy->readerTasks, i);
      pTask->task = task;
      TAOS_CHECK_EXIT(stReaderTaskDeploy(pTask, &pReader->msg));

      key[1] = pReader->task.taskId;
      TAOS_CHECK_EXIT(taosHashPut(gStreamMgmt.taskMap, key, sizeof(key), &pTask, sizeof(pTask)));

      TAOS_CHECK_EXIT(smAppendVgReaderTask(pTask));
    }
  }

  if (pDeploy->triggerTasks) {
    int32_t triggerNum = taosArrayGetSize(pDeploy->triggerTasks);
    tasks.triggerTaskList = taosArrayInit(triggerNum, sizeof(SStreamTriggerTask));
    TSDB_CHECK_NULL(tasks.triggerTaskList, code, lino, _exit, terrno);
    for (int32_t i = 0; i < triggerNum; ++i) {
      SStreamTriggerTask* pTask = taosArrayReserve(tasks.triggerTaskList, 1);
      SStreamDeployTaskInfo* pTrigger = taosArrayGet(pDeploy->triggerTasks, i);
      pTask->task = task;
      TAOS_CHECK_EXIT(stTriggerTaskDeploy(pTask, &pTrigger->msg));

      key[1] = pTrigger->task.taskId;
      TAOS_CHECK_EXIT(taosHashPut(gStreamMgmt.taskMap, key, sizeof(key), &pTask, sizeof(pTask)));

      TAOS_CHECK_EXIT(smAppendSnodeTask(pTask));
    }
  }

  if (pDeploy->runnerTasks) {
    int32_t runnerNum = taosArrayGetSize(pDeploy->runnerTasks);
    tasks.runnerTaskList = taosArrayInit(runnerNum, sizeof(SStreamRunnerTask));
    TSDB_CHECK_NULL(tasks.runnerTaskList, code, lino, _exit, terrno);
    for (int32_t i = 0; i < runnerNum; ++i) {
      SStreamRunnerTask* pTask = taosArrayReserve(tasks.runnerTaskList, 1);
      SStreamDeployTaskInfo* pRunner = taosArrayGet(pDeploy->runnerTasks, i);
      pTask->task = task;
      TAOS_CHECK_EXIT(stRunnerTaskDeploy(pTask, &pRunner->msg));

      key[1] = pRunner->task.taskId;
      TAOS_CHECK_EXIT(taosHashPut(gStreamMgmt.taskMap, key, sizeof(key), &pTask, sizeof(pTask)));
    }
  }  

  TAOS_CHECK_EXIT(taosHashPut(pGrp, &streamId, sizeof(streamId), &tasks, sizeof(tasks)));

  mstInfo("stream deploy succeed");

_exit:

  return code;
}

int32_t streamMgmtDeployTasks(SStreamDeployActions* actions) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t listNum = taosArrayGetSize(actions->taskList);
  
  for (int32_t i = 0; i < listNum; ++i) {
    SStreamTasksDeploy* pDeploy = taosArrayGet(actions->taskList, i);

    TAOS_CHECK_EXIT(smDeployStreamTasks(pDeploy));
  }

_exit:

  return code;
}



int32_t smStartStreamTasks(SStreamTasksStart* pStart) {
  int64_t streamId = pStart->task.streamId;
  int64_t key[2] = {streamId, pStart->task.taskId};
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  
  SStreamTask* pTask = taosHashGet(gStreamMgmt.taskMap, key, sizeof(key));
  if (NULL == pTask) {
    mstError("stream not exists while try to start task %" PRId64, key[1]);
    goto _exit;
  }

  TAOS_CHECK_EXIT(stTriggerTaskExecute(pTask, &pStart->startMsg));

  ST_TASK_ILOG("stream start succeed");

_exit:

  return code;
}

void smRemoveReaderTask(SStreamTask* pTask) {
  SStreamVgReaderTasks* pTasks = taosHashGet(gStreamMgmt.vgroupMap, &pTask->nodeId, sizeof(pTask->nodeId));
  if (NULL == pTasks) {
    ST_TASK_WLOG("vgroup not exists");
    return;
  }

  taosWLockLatch(&pTasks->lock);
  int32_t taskNum = taosArrayGetSize(pTasks->taskList);
  for (int32_t i = 0; i < taskNum; ++i) {
    SStreamTask** ppTask = taosArrayGet(pTasks->taskList, i);
    if ((*ppTask)->taskId == pTask->taskId) {
      taosArrayRemove(pTasks->taskList, i);
      ST_TASK_ILOG("task removed from vgroupMap");
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
  
  TAOS_CHECK_EXIT(stRunnerTaskUndeploy(pTask, &pUndeploy->undeployMsg, ));

  ST_TASK_ILOG("stream start succeed");

_exit:

  return code;
}



int32_t streamMgmtStartTasks(SStreamStartActions* actions) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t listNum = taosArrayGetSize(actions->taskList);
  
  for (int32_t i = 0; i < listNum; ++i) {
    SStreamTasksStart* pStart = taosArrayGet(actions->taskList, i);

    TAOS_CHECK_EXIT(smStartStreamTasks(pStart));
  }

_exit:

  return code;
}

int32_t streamMgmtUndeployTasks(SStreamUndeployActions* actions) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t listNum = taosArrayGetSize(actions->taskList);
  
  for (int32_t i = 0; i < listNum; ++i) {
    SStreamTasksUndeploy* pUndeploy = taosArrayGet(actions->taskList, i);

    TAOS_CHECK_EXIT(smUndeployStreamTasks(pUndeploy));
  }

_exit:

  return code;
}



