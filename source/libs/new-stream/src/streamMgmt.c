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
#include "streamReader.h"


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
      code = taosHashPut(gStreamMgmt.vgroupMap, &pTask->task.nodeId, sizeof(pTask->task.nodeId), &vg, sizeof(vg));
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
      taosHashRelease(gStreamMgmt.vgroupMap, pVg);
      TSDB_CHECK_NULL(NULL, code, lino, _return, terrno);
    }
    taosWUnLockLatch(&pVg->lock);
    
    taosHashRelease(gStreamMgmt.vgroupMap, pVg);
    break;
  }
  
_return:

  if (code) {
    stsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
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

  stsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

  return code;
}

int32_t smAddTasksToStreamMap(SStmStreamDeploy* pDeploy, SStreamTasksInfo* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pDeploy->streamId;
  int32_t readerNum = 0, triggerNum = 0, runnerNum = 0;

  taosRLockLatch(&pStream->taskLock);

  if (pDeploy->readerTasks) {
    readerNum = taosArrayGetSize(pDeploy->readerTasks);
    if (NULL == pStream->readerList) {
      pStream->readerList = taosArrayInit(readerNum, sizeof(SStreamReaderTask));
      TSDB_CHECK_NULL(pStream->readerList, code, lino, _exit, terrno);
    }
    
    for (int32_t i = 0; i < readerNum; ++i) {
      SStreamReaderTask* pTask = taosArrayReserve(pStream->readerList, 1);
      SStmTaskDeploy* pReader = taosArrayGet(pDeploy->readerTasks, i);
      pTask->task = pReader->task;

      TAOS_CHECK_EXIT(stReaderTaskDeploy(pTask, &pReader->msg.reader));

      TAOS_CHECK_EXIT(taosHashPut(gStreamMgmt.taskMap, &pTask->task.streamId, sizeof(pTask->task.streamId) + sizeof(pTask->task.taskId), &pTask, POINTER_BYTES));

      TAOS_CHECK_EXIT(smAddTaskToVgroupMap(pTask));

      ST_TASK_DLOG("reader task deploy succeed, tidx:%d", pTask->task.taskIdx);      

      atomic_add_fetch_32(&pStream->taskNum, 1);
    }
  }

  if (pDeploy->triggerTask) {
    triggerNum = 1;
    SStreamTask* pTask = &pDeploy->triggerTask->task;
    if (NULL == pStream->triggerTask) {
      pStream->triggerTask = taosMemoryCalloc(1, sizeof(SStreamTriggerTask));
      TSDB_CHECK_NULL(pStream->triggerTask, code, lino, _exit, terrno);

      pStream->triggerTask->task = *pTask;
      
      TAOS_CHECK_EXIT(stTriggerTaskDeploy(pStream->triggerTask, &pDeploy->triggerTask->msg.trigger));
      
      TAOS_CHECK_EXIT(taosHashPut(gStreamMgmt.taskMap, &pTask->streamId, sizeof(pTask->streamId) + sizeof(pTask->taskId), &pStream->triggerTask, POINTER_BYTES));
      
      //TAOS_CHECK_EXIT(smAddTaskToSnodeList(stream.triggerTask));
      
      ST_TASK_DLOG("trigger task deploy succeed, tidx:%d", pTask->taskIdx);      
      
      atomic_add_fetch_32(&pStream->taskNum, 1);
    } else {
      ST_TASK_ELOG("trigger task already exists, taskId:%" PRId64 ", tidx:%d", pTask->taskId, pTask->taskIdx);      
    }    
  }

  if (pDeploy->runnerTasks) {
    runnerNum = taosArrayGetSize(pDeploy->runnerTasks);
    if (NULL == pStream->runnerList) {
      pStream->runnerList = taosArrayInit(runnerNum, sizeof(SStreamRunnerTask));
      TSDB_CHECK_NULL(pStream->runnerList, code, lino, _exit, terrno);
    }
    
    for (int32_t i = 0; i < runnerNum; ++i) {
      SStreamRunnerTask* pTask = taosArrayReserve(pStream->runnerList, 1);
      SStmTaskDeploy* pRunner = taosArrayGet(pDeploy->runnerTasks, i);
      pTask->task = pRunner->task;
      
      TAOS_CHECK_EXIT(stRunnerTaskDeploy(pTask, &pRunner->msg.runner));

      TAOS_CHECK_EXIT(taosHashPut(gStreamMgmt.taskMap, &pTask->task.streamId, sizeof(pTask->task.streamId) + sizeof(pTask->task.taskId), &pTask, POINTER_BYTES));

      ST_TASK_DLOG("runner task deploy succeed, tidx:%d", pTask->task.taskIdx);      

      atomic_add_fetch_32(&pStream->taskNum, 1);
    }
  }  

_exit:

  taosRUnLockLatch(&pStream->taskLock);

  if (code) {
    stsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    stsDebug("all %d deploy tasks added to streamMap&taskMap", readerNum + triggerNum + runnerNum);
  }
  
  return code;
}

int32_t smDeployTasks(SStmStreamDeploy* pDeploy) {
  int64_t streamId = pDeploy->streamId;
  int32_t gid = STREAM_GID(streamId);
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SHashObj* pGrp = gStreamMgmt.stmGrp[gid];

  stsInfo("start to deploy stream, readerNum:%zu, triggerNum:%d, runnerNum:%zu", 
      taosArrayGetSize(pDeploy->readerTasks), pDeploy->triggerTask ? 1 : 0, taosArrayGetSize(pDeploy->runnerTasks));      
  
  if (NULL == pGrp) {
    gStreamMgmt.stmGrp[gid] = taosHashInit(STREAM_GRP_STREAM_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    TSDB_CHECK_NULL(gStreamMgmt.stmGrp[gid], code, lino, _exit, terrno);
    pGrp = gStreamMgmt.stmGrp[gid];
  }
  
  int32_t taskNum = 0;
  SStreamTasksInfo* pStream = taosHashGet(pGrp, &streamId, sizeof(streamId));
  if (NULL != pStream) {
    stsDebug("stream already exists, remain taskNum:%d", pStream->taskNum);
    TAOS_CHECK_EXIT(smAddTasksToStreamMap(pDeploy, pStream));
    taskNum = atomic_load_32(&pStream->taskNum);
  } else {
    SStreamTasksInfo stream = {0};

    code = smAddTasksToStreamMap(pDeploy, &stream);
    if (TSDB_CODE_SUCCESS != code) {
      stmDestroySStreamTasksInfo(&stream);
      TAOS_CHECK_EXIT(code);
    }

    TAOS_CHECK_EXIT(taosHashPut(pGrp, &streamId, sizeof(streamId), &stream, sizeof(stream)));
    taskNum = stream.taskNum;
  }
  
  stsInfo("stream deploy succeed, current taskNum:%d", taskNum);

  return code;

_exit:

  stsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

  return code;
}

int32_t smDeployStreams(SStreamDeployActions* actions) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = 0;
  int32_t listNum = taosArrayGetSize(actions->streamList);
  
  for (int32_t i = 0; i < listNum; ++i) {
    SStmStreamDeploy* pDeploy = taosArrayGet(actions->streamList, i);
    streamId = pDeploy->streamId;

    TAOS_CHECK_EXIT(smDeployTasks(pDeploy));
  }

  return code;

_exit:

  stsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

  return code;
}



int32_t smStartStreamTasks(SStreamTaskStart* pStart) {
  int64_t streamId = pStart->task.streamId;
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  
  SStreamTask** ppTask = taosHashGet(gStreamMgmt.taskMap, &pStart->task.streamId, sizeof(pStart->task.streamId) + sizeof(pStart->task.taskId));
  if (NULL == ppTask) {
    stsError("stream not exists while try to start task %" PRId64, pStart->task.taskId);
    goto _exit;
  }

  SStreamTask* pTask = *ppTask;

  pStart->startMsg.header.msgType = STREAM_MSG_START;
  TAOS_CHECK_EXIT(stTriggerTaskExecute((SStreamTriggerTask *)pTask, (SStreamMsg *)&pStart->startMsg));

  ST_TASK_ILOG("stream start succeed, tidx:%d", pTask->taskIdx);

  return code;

_exit:

  stsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

  return code;
}

void smRemoveReaderFromVgMap(SStreamTask* pTask) {
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


/*
void smRemoveTriggerTask(SStreamTask* pTask) {
  taosWLockLatch(&gStreamMgmt.snodeLock);
  int32_t taskNum = taosArrayGetSize(gStreamMgmt.snodeTasks);
  for (int32_t i = 0; i < taskNum; ++i) {
    SStreamTask** ppTask = taosArrayGet(gStreamMgmt.snodeTasks, i);
    if ((*ppTask)->taskId == pTask->taskId) {
      ST_TASK_ILOG("task removed from snodeTasks, tidx:%d", pTask->taskIdx);
      taosArrayRemove(gStreamMgmt.snodeTasks, i);
      break;
    }
  }
  taosWUnLockLatch(&gStreamMgmt.snodeLock);
}
*/

void smRemoveTaskPostCheck(int64_t streamId, SStreamTasksInfo* pStream, bool* isLastTask) {
  bool taskRemains = false;
  int32_t remainTasks = atomic_sub_fetch_32(&pStream->taskNum, 1);
  if (0 >= remainTasks) {
    int32_t readerNum = taosArrayGetSize(pStream->readerList);
    int32_t runnerNum = taosArrayGetSize(pStream->runnerList);
    if (readerNum > 0) {
      stsError("remain readerNum %d while taskNum %d", readerNum, remainTasks);
      taskRemains = true;
    }
    if (pStream->triggerTask) {
      stsError("trigger task remain while taskNum %d", readerNum);
      taskRemains = true;
    }
    if (runnerNum > 0) {
      stsError("remain runnerNum %d while taskNum %d", runnerNum, remainTasks);
      taskRemains = true;
    }

    if (!taskRemains) {
      *isLastTask = true;
      return;
    }
  }

  *isLastTask = false;
}


void smRemoveTaskCb(void* param) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStreamTask* pTask = *(SStreamTask**)param;
  int32_t gid = STREAM_GID(pTask->streamId);
  SHashObj* pGrp = gStreamMgmt.stmGrp[gid];
  int64_t streamId = pTask->streamId;

  if (STREAM_READER_TASK == pTask->type) {
    smRemoveReaderFromVgMap(pTask);
  }

  if (NULL == pGrp) {
    ST_TASK_ELOG("stream grp is null, gid:%d", gid);
    goto _exit;
  }
  
  SStreamTasksInfo* pStream = taosHashAcquire(pGrp, &streamId, sizeof(streamId));
  if (NULL == pStream) {
    ST_TASK_ELOG("stream not in streamGrp, gid:%d", gid);
    goto _exit;
  }

  bool isLastTask = false;
  switch (pTask->type){
    case STREAM_READER_TASK:
      if (NULL == pStream->undeployReaders) {
        int32_t num = taosArrayGetSize(pStream->readerList);
        SArray* pReaders = taosArrayInit(num, sizeof(int32_t));
        TSDB_CHECK_NULL(pReaders, code, lino, _exit, terrno);
        if (NULL != atomic_val_compare_exchange_ptr(&pStream->undeployReaders, NULL, pReaders)) {
          taosArrayDestroy(pReaders);
        }
      }
      taosWLockLatch(&pStream->undeployReadersLock);
      taosArrayPush(pStream->undeployReaders, &pTask->taskId);
      taosWUnLockLatch(&pStream->undeployReadersLock);
      break;
    case STREAM_TRIGGER_TASK:
      taosWLockLatch(&pStream->triggerTaskLock);
      if (pStream->triggerTask->task.taskId != pTask->taskId) {
        ST_TASK_ELOG("trigger task mismatch with current trigger taskId:%"PRId64, pStream->triggerTask->task.taskId);
        goto _exit;
      }
      
      smRemoveTaskPostCheck(streamId, pStream, &isLastTask);

      if (isLastTask) {
        int32_t code = taosHashRemove(pGrp, &streamId, sizeof(streamId));
        if (TSDB_CODE_SUCCESS == code) {
          stsInfo("stream removed from streamGrpHash %d, remainStream:%d", gid, taosHashGetSize(pGrp));
        } else {
          stsWarn("stream remove from streamGrpHash %d failed, remainStream:%d, error:%s", 
              gid, taosHashGetSize(pGrp), tstrerror(code));
        }
      }

      taosMemoryFreeClear(pStream->triggerTask);
      taosWUnLockLatch(&pStream->triggerTaskLock);

      taosHashRelease(gStreamMgmt.taskMap, param);
      
      taosHashRelease(pGrp, pStream);

      return;
      break;
    case STREAM_RUNNER_TASK:
      if (NULL == pStream->undeployRunners) {
        int32_t num = taosArrayGetSize(pStream->runnerList);
        SArray* pRunners = taosArrayInit(num, sizeof(int32_t));
        TSDB_CHECK_NULL(pRunners, code, lino, _exit, terrno);
        if (NULL != atomic_val_compare_exchange_ptr(&pStream->undeployRunners, NULL, pRunners)) {
          taosArrayDestroy(pRunners);
        }
      }
      taosWLockLatch(&pStream->undeployRunnersLock);
      taosArrayPush(pStream->undeployRunners, &pTask->taskId);
      taosWUnLockLatch(&pStream->undeployRunnersLock);
      break;
    default:
      ST_TASK_ELOG("Invalid task type:%d", pTask->type);
      TAOS_CHECK_EXIT(TSDB_CODE_STREAM_INTERNAL_ERROR);
      break;
  }

_exit:

  if (code) {
    ST_TASK_ELOG("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    ST_TASK_DLOG("task pre-removed from stream task list, tidx:%d", pTask->taskIdx);
  }

  taosWUnLockLatch(&pTask->lock);

  taosHashRelease(gStreamMgmt.taskMap, param);

  taosHashRelease(pGrp, pStream);
}


int32_t smUndeployTask(SStreamTaskUndeploy* pUndeploy) {
  int64_t streamId = pUndeploy->task.streamId;
  int64_t key[2] = {streamId, pUndeploy->task.taskId};
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  
  SStreamTask** ppTask = taosHashAcquire(gStreamMgmt.taskMap, key, sizeof(key));
  if (NULL == ppTask) {
    stsWarn("TASK:%" PRIx64 " already not exists in taskMap while try to undeploy it", key[1]);
    return code;
  }

  SStreamTask task = **ppTask;
  SStreamTask* pTask= &task;

  (void)taosHashRemove(gStreamMgmt.taskMap, key, sizeof(key));

  if (taosWTryLockLatch(&(*ppTask)->lock)) {
    goto _exit;
  }
  
  switch (pTask->type) {
    case STREAM_READER_TASK:
      code = stReaderTaskUndeploy((SStreamReaderTask**)ppTask, &pUndeploy->undeployMsg, smRemoveTaskCb);
      break;
    case STREAM_TRIGGER_TASK:
      code = stTriggerTaskUndeploy((SStreamTriggerTask**)ppTask, &pUndeploy->undeployMsg, smRemoveTaskCb);
      break;
    case STREAM_RUNNER_TASK:
      code = stRunnerTaskUndeploy((SStreamRunnerTask**)ppTask, &pUndeploy->undeployMsg, smRemoveTaskCb);
      break;
    default:
      code = TSDB_CODE_STREAM_INTERNAL_ERROR;
      break;
  }
  
_exit:

  if (code) {
    stsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    ST_TASK_ILOG("task undeploy succeed, tidx:%d", pTask->taskIdx);
  }
  
  return code;
}


void smUndeployVgTasks(int32_t vgId) {
  SStreamVgReaderTasks* pVg = taosHashAcquire(gStreamMgmt.vgroupMap, &vgId, sizeof(vgId));
  if (NULL == pVg) {
    stDebug("no tasks in vgourp %d, ignore undeploy vg tasks", vgId);
    return;
  }

  SStreamTaskUndeploy undeploy = {0};
  undeploy.undeployMsg.doCheckpoint = true;
  undeploy.undeployMsg.doCleanup = false;
  
  taosWLockLatch(&pVg->lock);
  int32_t taskNum = taosArrayGetSize(pVg->taskList);
  for (int32_t i = 0; i < taskNum; ++i) {
    SStreamTask* pTask = taosArrayGetP(pVg->taskList, i);
    undeploy.task = *pTask;
    (void)smUndeployTask(&undeploy);
  }
  taosWUnLockLatch(&pVg->lock);
}

void smHandleRemovedTask(SStreamTasksInfo* pStream, int64_t streamId, int32_t gid, bool isReader) {
  SRWLatch* pLock = isReader ? &pStream->undeployReadersLock : &pStream->undeployRunnersLock;
  SArray* pList = isReader ? pStream->undeployReaders : pStream->undeployRunners;
  SArray* pSrc = isReader ? pStream->readerList : pStream->runnerList;
  bool isLastTask = false;
  
  taosWLockLatch(pLock);
  int32_t unNum = taosArrayGetSize(pList);
  for (int32_t i = 0; i < unNum; ++i) {
    int32_t* taskId = taosArrayGet(pList, i);
    int32_t num = taosArrayGetSize(pSrc);
    for (int32_t m = 0; m < num; ++m) {
      SStreamTask* pTask = taosArrayGet(pSrc, m);
      if (pTask->taskId == *taskId) {
        taosArrayRemove(pSrc, m);
        ST_TASK_DLOG("task removed from stream taskList, remain:%d", num - 1);
        smRemoveTaskPostCheck(streamId, pStream, &isLastTask);
        break;
      }
    }
  }
  taosArrayClear(pList);
  taosWUnLockLatch(pLock);

  if (!isLastTask) {
    return;
  }
  
  int32_t code = taosHashRemove(gStreamMgmt.stmGrp[gid], &streamId, sizeof(streamId));
  if (TSDB_CODE_SUCCESS == code) {
    stsInfo("stream removed from streamGrpHash %d, remainStream:%d", gid, taosHashGetSize(gStreamMgmt.stmGrp[gid]));
  } else {
    stsWarn("stream remove from streamGrpHash %d failed, remainStream:%d, error:%s", 
        gid, taosHashGetSize(gStreamMgmt.stmGrp[gid]), tstrerror(code));
  }
}



int32_t smStartTasks(SStreamStartActions* actions) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = 0;
  int32_t listNum = taosArrayGetSize(actions->taskList);
  
  for (int32_t i = 0; i < listNum; ++i) {
    SStreamTaskStart* pStart = taosArrayGet(actions->taskList, i);
    streamId = pStart->task.streamId;

    TAOS_CHECK_EXIT(smStartStreamTasks(pStart));
  }

  return code;

_exit:

  stsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

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
    SStreamTaskUndeploy* pUndeploy = taosArrayGet(actions->taskList, i);
    streamId = pUndeploy->task.streamId;
    
    smUndeployTask(pUndeploy);
  }

  return code;

_exit:

  stsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

  return code;
}



