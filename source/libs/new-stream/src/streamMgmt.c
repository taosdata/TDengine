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

void smRemoveReaderFromVgMap(SStreamTask* pTask) {
  SStreamVgReaderTasks* pVg = taosHashAcquire(gStreamMgmt.vgroupMap, &pTask->nodeId, sizeof(pTask->nodeId));
  if (NULL == pVg) {
    ST_TASK_WLOG("vgroup not exists, tidx:%d", pTask->taskIdx);
    return;
  }

  taosWLockLatch(&pVg->lock);
  int32_t taskNum = taosArrayGetSize(pVg->taskList);
  for (int32_t i = 0; i < taskNum; ++i) {
    SStreamTask** ppTask = taosArrayGet(pVg->taskList, i);
    if ((*ppTask)->taskId == pTask->taskId && (*ppTask)->seriousId == pTask->seriousId) {
      ST_TASK_ILOG("task %p removed from vgroupMap, tidx:%d", *ppTask, pTask->taskIdx);
      taosArrayRemove(pVg->taskList, i);
      break;
    }
  }

  taosWUnLockLatch(&pVg->lock);
  taosHashRelease(gStreamMgmt.vgroupMap, pVg);  
}


static int32_t smAddTaskToVgroupMap(SStreamReaderTask* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pTask->task.streamId;
  SStreamVgReaderTasks vg = {0};

  while (true) {
    SStreamVgReaderTasks* pVg = taosHashAcquire(gStreamMgmt.vgroupMap, &pTask->task.nodeId, sizeof(pTask->task.nodeId));
    if (NULL == pVg) {
      vg.taskList = taosArrayInit(20, POINTER_BYTES);
      TSDB_CHECK_NULL(vg.taskList, code, lino, _exit, terrno);
      TSDB_CHECK_NULL(taosArrayPush(vg.taskList, &pTask), code, lino, _exit, terrno);
      code = taosHashPut(gStreamMgmt.vgroupMap, &pTask->task.nodeId, sizeof(pTask->task.nodeId), &vg, sizeof(vg));
      if (TSDB_CODE_SUCCESS == code) {
        return code;
      }

      if (TSDB_CODE_DUP_KEY != code) {
        TAOS_CHECK_EXIT(code);
      }    

      taosArrayDestroy(vg.taskList);
      continue;
    }

    taosWLockLatch(&pVg->lock);
    if (pVg->inactive) {
      ST_TASK_WLOG("vnode not leader or closed, ignore deploy, taskIdx:%d", pTask->task.taskIdx);
      taosWUnLockLatch(&pVg->lock);
      taosHashRelease(gStreamMgmt.vgroupMap, pVg);
      TAOS_CHECK_EXIT(TSDB_CODE_STREAM_NOT_LEADER);
    }
    
    if (NULL == pVg->taskList) {
      pVg->taskList = taosArrayInit(20, POINTER_BYTES);
      TSDB_CHECK_NULL(pVg->taskList, code, lino, _exit, terrno);
    }
    
    if (NULL == taosArrayPush(pVg->taskList, &pTask)) {
      taosWUnLockLatch(&pVg->lock);
      taosHashRelease(gStreamMgmt.vgroupMap, pVg);
      TSDB_CHECK_NULL(NULL, code, lino, _exit, terrno);
    }
    taosWUnLockLatch(&pVg->lock);
    
    taosHashRelease(gStreamMgmt.vgroupMap, pVg);
    break;
  }
  
_exit:

  if (code) {
    stsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    stsDebug("reader task %p added to vgroupMap", pTask);
  }
  
  return code;
}

/*
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
*/

int32_t smAddTasksToStreamMap(SStmStreamDeploy* pDeploy, SStreamInfo* pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pDeploy->streamId;
  int32_t readerNum = 0, triggerNum = 0, runnerNum = 0;
  void*   taskAddr = NULL;

  taosRLockLatch(&pStream->lock);

  if (pDeploy->readerTasks) {
    readerNum = taosArrayGetSize(pDeploy->readerTasks);
    if (NULL == pStream->readerList) {
      pStream->readerList = tdListNew(sizeof(SStreamReaderTask));
      TSDB_CHECK_NULL(pStream->readerList, code, lino, _exit, terrno);
    }
    
    for (int32_t i = 0; i < readerNum; ++i) {
      SStreamReaderTask* pTask = tdListReserve(pStream->readerList);
      TSDB_CHECK_NULL(pTask, code, lino, _exit, terrno);

      SStmTaskDeploy* pReader = taosArrayGet(pDeploy->readerTasks, i);
      pTask->task = pReader->task;

      code = taosHashPut(gStreamMgmt.taskMap, &pTask->task.streamId, sizeof(pTask->task.streamId) + sizeof(pTask->task.taskId), &pTask, POINTER_BYTES);
      if (code) {
        ST_TASK_ELOG("reader task fail to add to taskMap, error:%s", tstrerror(code));
        taosMemoryFree(tdListPopTail(pStream->readerList));
        continue;
      }

      code = streamAcquireTask(pReader->task.streamId, pReader->task.taskId, (SStreamTask**)&pTask, &taskAddr);
      if (code) {
        ST_TASK_ELOG("reader task no longer exists, error:%s", tstrerror(code));
        continue;
      }

      code = smAddTaskToVgroupMap(pTask);
      if (code) {
        ST_TASK_ELOG("reader task fail to add to vgroup map, error:%s", tstrerror(code));
        (void)taosHashRemove(gStreamMgmt.taskMap, &pTask->task.streamId, sizeof(pTask->task.streamId) + sizeof(pTask->task.taskId));
        streamReleaseTask(taskAddr);
        taosMemoryFree(tdListPopTail(pStream->readerList));
        continue;
      }

      code = stReaderTaskDeploy(pTask, &pReader->msg.reader);
      if (code) {
        ST_TASK_ELOG("reader task deploy failed, error:%s", tstrerror(code));
        smRemoveReaderFromVgMap((SStreamTask*)pTask);
        (void)taosHashRemove(gStreamMgmt.taskMap, &pTask->task.streamId, sizeof(pTask->task.streamId) + sizeof(pTask->task.taskId));
        streamReleaseTask(taskAddr);
        taosMemoryFree(tdListPopTail(pStream->readerList));
        continue;
      }

      ST_TASK_DLOG("%sReader task deploy succeed, tidx:%d", STREAM_IS_TRIGGER_READER(pTask->task.flags) ? "trig" : "calc", pTask->task.taskIdx);      

      (void)atomic_add_fetch_32(&pStream->taskNum, 1);
      streamReleaseTask(taskAddr);
    }
  }

  if (pDeploy->triggerTask) {
    if (NULL == pStream->triggerList) {
      pStream->triggerList = tdListNew(sizeof(SStreamTriggerTask));
      TSDB_CHECK_NULL(pStream->triggerList, code, lino, _exit, terrno);
    } else if (TD_DLIST_NELES(pStream->triggerList) > 0) {
      stsWarn("%d trigger tasks already exist, will try to undeploy them", TD_DLIST_NELES(pStream->triggerList));      
      smUndeployStreamTriggerTasks(pStream, streamId);
    }
    
    SStreamTask* pSrc = &pDeploy->triggerTask->task;
    SStreamTriggerTask* pTask = tdListPreReserve(pStream->triggerList);
    TSDB_CHECK_NULL(pTask, code, lino, _exit, terrno);
    
    pStream->triggerTaskId = pSrc->taskId;
    pTask->task = *pSrc;

    code = taosHashPut(gStreamMgmt.taskMap, &pSrc->streamId, sizeof(pSrc->streamId) + sizeof(pSrc->taskId), &pTask, POINTER_BYTES);
    if (code) {
      ST_TASK_ELOG("trigger task fail to add to taskMap, error:%s", tstrerror(code));
      taosMemoryFree(tdListPopHead(pStream->triggerList));
      TAOS_CHECK_EXIT(code);
    }

    code = streamAcquireTask(pSrc->streamId, pSrc->taskId, (SStreamTask**)&pTask, &taskAddr);
    if (code) {
      ST_TASK_ELOG("trigger task no longer exists, error:%s", tstrerror(code));
      TAOS_CHECK_EXIT(code);
    }

    code = stTriggerTaskDeploy(pTask, &pDeploy->triggerTask->msg.trigger);
    if (code) {
      ST_TASK_ELOG("trigger task fail to deploy, error:%s", tstrerror(code));
      (void)taosHashRemove(gStreamMgmt.taskMap, &pSrc->streamId, sizeof(pSrc->streamId) + sizeof(pSrc->taskId));
      streamReleaseTask(taskAddr);
      taosMemoryFree(tdListPopHead(pStream->triggerList));
      TAOS_CHECK_EXIT(code);
    }

    ST_TASK_DLOG("trigger task deploy succeed, tidx:%d", pSrc->taskIdx);   
    
    (void)atomic_add_fetch_32(&pStream->taskNum, 1);
    streamReleaseTask(taskAddr);
  }

  if (pDeploy->runnerTasks) {
    runnerNum = taosArrayGetSize(pDeploy->runnerTasks);
    if (NULL == pStream->runnerList) {
      pStream->runnerList = tdListNew(sizeof(SStreamRunnerTask));
      TSDB_CHECK_NULL(pStream->runnerList, code, lino, _exit, terrno);
    }
    
    for (int32_t i = 0; i < runnerNum; ++i) {
      SStreamRunnerTask* pTask = tdListReserve(pStream->runnerList);
      TSDB_CHECK_NULL(pTask, code, lino, _exit, terrno);

      SStmTaskDeploy* pRunner = taosArrayGet(pDeploy->runnerTasks, i);
      pTask->task = pRunner->task;

      code = taosHashPut(gStreamMgmt.taskMap, &pTask->task.streamId, sizeof(pTask->task.streamId) + sizeof(pTask->task.taskId), &pTask, POINTER_BYTES);
      if (code) {
        ST_TASK_ELOG("runner task fail to add to taskMap, error:%s", tstrerror(code));
        taosMemoryFree(tdListPopTail(pStream->runnerList));
        continue;
      }

      code = streamAcquireTask(pTask->task.streamId, pTask->task.taskId, (SStreamTask**)&pTask, &taskAddr);
      if (code) {
        ST_TASK_ELOG("runner task no longer exists, error:%s", tstrerror(code));
        continue;
      }
      
      code = stRunnerTaskDeploy(pTask, &pRunner->msg.runner);
      if (code) {
        ST_TASK_ELOG("runner task fail to deploy, error:%s", tstrerror(code));
        (void)taosHashRemove(gStreamMgmt.taskMap, &pTask->task.streamId, sizeof(pTask->task.streamId) + sizeof(pTask->task.taskId));
        streamReleaseTask(taskAddr);
        taosMemoryFree(tdListPopTail(pStream->runnerList));
        continue;
      }
      
      ST_TASK_DLOG("runner task deploy succeed, tidx:%d", pTask->task.taskIdx);      
      (void)atomic_add_fetch_32(&pStream->taskNum, 1);
      streamReleaseTask(taskAddr);
    }
  }  

_exit:

  taosRUnLockLatch(&pStream->lock);

  if (code) {
    stsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    stsDebug("all %d deploy tasks added to streamMap&taskMap", readerNum + triggerNum + runnerNum);
  }
  
  return TSDB_CODE_SUCCESS; // ALWAYS SUCCESS
}



int32_t smStartStreamTasks(SStreamTaskStart* pStart) {
  int64_t streamId = pStart->task.streamId;
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStreamTask* pTask= NULL;
  void*   taskAddr = NULL;
  
  TAOS_CHECK_EXIT(streamAcquireTask(streamId, pStart->task.taskId, (SStreamTask**)&pTask, &taskAddr));

  pStart->startMsg.header.msgType = STREAM_MSG_START;
  STM_CHK_SET_ERROR_EXIT(stTriggerTaskExecute((SStreamTriggerTask *)pTask, (SStreamMsg *)&pStart->startMsg));

_exit:

  if (code) {
    stsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    ST_TASK_ILOG("stream start succeed, tidx:%d", pTask->taskIdx);
  }

  streamReleaseTask(taskAddr);

  return code;
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

void smRemoveTaskPostCheck(int64_t streamId, SStreamInfo* pStream, bool* isLastTask) {
  bool taskRemains = false;
  int32_t remainTasks = atomic_sub_fetch_32(&pStream->taskNum, 1);
  if (0 >= remainTasks) {
    int32_t readerNum = pStream->readerList ? TD_DLIST_NELES(pStream->readerList) : 0;
    int32_t triggerNum = pStream->triggerList ? TD_DLIST_NELES(pStream->triggerList) : 0;
    int32_t runnerNum = pStream->runnerList ? TD_DLIST_NELES(pStream->runnerList) : 0;
    if (readerNum > 0) {
      stsError("remain readerNum %d while taskNum %d", readerNum, remainTasks);
      taskRemains = true;
    }
    if (triggerNum > 0) {
      stsError("remain triggerNum %d while taskNum %d", triggerNum, remainTasks);
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
  SStreamInfo* pStream = NULL;

  if (NULL == pGrp) {
    ST_TASK_ELOG("stream grp is null, gid:%d", gid);
    goto _exit;
  }
  
  pStream = taosHashAcquire(pGrp, &streamId, sizeof(streamId));
  if (NULL == pStream) {
    ST_TASK_ELOG("stream not in streamGrp, gid:%d", gid);
    goto _exit;
  }

  bool isLastTask = false;
  switch (pTask->type){
    case STREAM_READER_TASK:
      if (NULL == pStream->undeployReaders) {
        int32_t num = pStream->readerList ? TD_DLIST_NELES(pStream->readerList) : 0;
        SArray* pReaders = taosArrayInit(num, sizeof(int64_t) * 2);
        TSDB_CHECK_NULL(pReaders, code, lino, _exit, terrno);
        if (NULL != atomic_val_compare_exchange_ptr(&pStream->undeployReaders, NULL, pReaders)) {
          taosArrayDestroy(pReaders);
        }
      }
      taosWLockLatch(&pStream->undeployLock);
      (void)taosArrayPush(pStream->undeployReaders, &pTask->taskId);
      taosWUnLockLatch(&pStream->undeployLock);
      break;
    case STREAM_TRIGGER_TASK: {
      if (NULL == pStream->undeployTriggers) {
        int32_t num = pStream->triggerList ? TD_DLIST_NELES(pStream->triggerList) : 0;
        SArray* pTriggers = taosArrayInit(num, sizeof(int64_t) * 2);
        TSDB_CHECK_NULL(pTriggers, code, lino, _exit, terrno);
        if (NULL != atomic_val_compare_exchange_ptr(&pStream->undeployTriggers, NULL, pTriggers)) {
          taosArrayDestroy(pTriggers);
        }
      }
      taosWLockLatch(&pStream->undeployLock);
      (void)taosArrayPush(pStream->undeployTriggers, &pTask->taskId);
      taosWUnLockLatch(&pStream->undeployLock);
      break;
    }
    case STREAM_RUNNER_TASK:
      if (NULL == pStream->undeployRunners) {
        int32_t num = pStream->runnerList ? TD_DLIST_NELES(pStream->runnerList) : 0;
        SArray* pRunners = taosArrayInit(num, sizeof(int64_t) * 2);
        TSDB_CHECK_NULL(pRunners, code, lino, _exit, terrno);
        if (NULL != atomic_val_compare_exchange_ptr(&pStream->undeployRunners, NULL, pRunners)) {
          taosArrayDestroy(pRunners);
        }
      }
      taosWLockLatch(&pStream->undeployLock);
      (void)taosArrayPush(pStream->undeployRunners, &pTask->taskId);
      taosWUnLockLatch(&pStream->undeployLock);
      break;
    default:
      ST_TASK_ELOG("Invalid task type:%d", pTask->type);
      TAOS_CHECK_EXIT(TSDB_CODE_STREAM_INTERNAL_ERROR);
      break;
  }

_exit:

  if (code) {
    ST_TASK_ELOG("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else if (pTask && STREAM_TRIGGER_TASK != pTask->type){
    ST_TASK_DLOG("task pre-removed from stream task list, tidx:%d", pTask->taskIdx);
  }

  taosHashRelease(gStreamMgmt.taskMap, param);

  taosHashRelease(pGrp, pStream);
}


int32_t smUndeployTask(SStreamTaskUndeploy* pUndeploy, bool rmFromVg) {
  int64_t streamId = pUndeploy->task.streamId;
  int64_t key[2] = {streamId, pUndeploy->task.taskId};
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStreamTask* pTask= &pUndeploy->task;
  
  ST_TASK_ILOG("start to undeploy task, tidx:%d", pTask->taskIdx);
  
  SStreamTask** ppTask = taosHashAcquire(gStreamMgmt.taskMap, key, sizeof(key));
  if (NULL == ppTask) {
    ST_TASK_WLOG("task already not exists in taskMap while try to undeploy it, tidx:%d", pTask->taskIdx);
    return code;
  }

  if (1 == atomic_val_compare_exchange_8(&(*ppTask)->deployed, 0, 1)) {
    ST_TASK_ILOG("task already be undeploying, tidx:%d", pTask->taskIdx);
    taosHashRelease(gStreamMgmt.taskMap, ppTask);
    return code;
  }

  SStreamTask task = **ppTask;
  pTask= &task;

  code = taosHashRemove(gStreamMgmt.taskMap, key, sizeof(key));
  if (code) {
    ST_TASK_WLOG("task remove from taskMap failed, error:%s, tidx:%d", tstrerror(code), pTask->taskIdx);
  }

  (*ppTask)->undeployMsg = pUndeploy->undeployMsg;
  (*ppTask)->undeployCb = smRemoveTaskCb;
  
  switch (pTask->type) {
    case STREAM_READER_TASK:
      if (rmFromVg) {
        smRemoveReaderFromVgMap(pTask);
      }
      code = stReaderTaskUndeploy((SStreamReaderTask**)ppTask, false);
      break;
    case STREAM_TRIGGER_TASK:
      code = stTriggerTaskUndeploy((SStreamTriggerTask**)ppTask, false);
      break;
    case STREAM_RUNNER_TASK:
      code = stRunnerTaskUndeploy((SStreamRunnerTask**)ppTask, false);
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


void smUndeployVgTasks(int32_t vgId, bool cleanup) {
  stInfo("start to undeploy vgroup tasks, vgId:%d", vgId);

  SStreamVgReaderTasks* pVg = taosHashAcquire(gStreamMgmt.vgroupMap, &vgId, sizeof(vgId));
  if (NULL == pVg) {
    stDebug("no tasks in vgourp %d, ignore undeploy vg tasks", vgId);
    return;
  }

  SStreamTaskUndeploy undeploy = {0};
  undeploy.undeployMsg.doCheckpoint = true;
  undeploy.undeployMsg.doCleanup = false;

  if (cleanup) {
    (void)taosHashRemove(gStreamMgmt.vgroupMap, &vgId, sizeof(vgId));
  } else {
    atomic_store_8(&pVg->inactive, 1);
  }
  
  taosWLockLatch(&pVg->lock);
  int32_t taskNum = taosArrayGetSize(pVg->taskList);
  for (int32_t i = 0; i < taskNum; ++i) {
    SStreamTask* pTask = taosArrayGetP(pVg->taskList, i);
    undeploy.task = *pTask;
    ST_TASK_DLOG("task %p removed from vgroupMap for vgroup reason, totalNum:%d", pTask, taskNum);
    (void)smUndeployTask(&undeploy, false);
  }
  taosArrayDestroy(pVg->taskList);
  pVg->taskList = NULL;
  taosWUnLockLatch(&pVg->lock);

  taosHashRelease(gStreamMgmt.vgroupMap, pVg);
}

void smUndeployStreamTriggerTasks(SStreamInfo* pStream, int64_t streamId) {
  stsDebug("start to undeploy stream all trigger tasks, num:%d", TD_DLIST_NELES(pStream->triggerList));

  SStreamTaskUndeploy undeploy = {0};
  undeploy.undeployMsg.doCheckpoint = true;
  undeploy.undeployMsg.doCleanup = false;

  SListIter iter = {0};
  SListNode* listNode = NULL;  
  tdListInitIter(pStream->triggerList, &iter, TD_LIST_FORWARD);
  while ((listNode = tdListNext(&iter)) != NULL) {
    SStreamTask* pTask = (SStreamTask*)listNode->data;
    undeploy.task = *pTask;
    (void)smUndeployTask(&undeploy, false);
  }
}

void smHandleRemovedTask(SStreamInfo* pStream, int64_t streamId, int32_t gid, EStreamTaskType type, SArray* pUndeployList, SList* pTaskList) {
  bool isLastTask = false;
  SListNode* listNode = NULL;

  stsDebug("start to handle removed %s tasks", gStreamTaskTypeStr[type]);
  
  taosWLockLatch(&pStream->undeployLock);
  
  int32_t unNum = taosArrayGetSize(pUndeployList);
  for (int32_t i = 0; i < unNum; ++i) {
    int64_t* taskId = taosArrayGet(pUndeployList, i);
    int64_t* seriousId = taskId + 1;
    SListIter iter = {0};
    tdListInitIter(pTaskList, &iter, TD_LIST_FORWARD);
    while ((listNode = tdListNext(&iter)) != NULL) {
      SStreamTask* pTask = (SStreamTask*)listNode->data;
      if (pTask->taskId == *taskId && pTask->seriousId == *seriousId) {
        SListNode* tmp = tdListPopNode(pTaskList, listNode);
        ST_TASK_DLOG("task removed from stream taskList, remain:%d", TD_DLIST_NELES(pTaskList));
        taosMemoryFreeClear(tmp);
        smRemoveTaskPostCheck(streamId, pStream, &isLastTask);
        break;
      }
    }
  }
  taosArrayClear(pUndeployList);
  taosWUnLockLatch(&pStream->undeployLock);

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

void smUndeployGrpSnodeTasks(SHashObj* pGrp, bool cleanup) {
  void* pIter = NULL;
  SStreamInfo* pStream = NULL;
  SStreamTaskUndeploy undeploy = {0};
  undeploy.undeployMsg.doCheckpoint = !cleanup;
  undeploy.undeployMsg.doCleanup = cleanup;

  while (true) {
    pIter = taosHashIterate(pGrp, pIter);
    if (NULL == pIter) {
      break;
    }

    pStream = (SStreamInfo*)pIter;
    
    taosWLockLatch(&pStream->lock);
    SListIter iter = {0};
    SListNode* listNode = NULL;
    tdListInitIter(pStream->triggerList, &iter, TD_LIST_FORWARD);
    while ((listNode = tdListNext(&iter)) != NULL) {
      SStreamTriggerTask* pTrigger = (SStreamTriggerTask*)listNode->data;
      undeploy.task = pTrigger->task;
      (void)smUndeployTask(&undeploy, false);
    }

    memset(&iter, 0, sizeof(iter));
    tdListInitIter(pStream->runnerList, &iter, TD_LIST_FORWARD);
    while ((listNode = tdListNext(&iter)) != NULL) {
      SStreamRunnerTask* pRunner = (SStreamRunnerTask*)listNode->data;
      undeploy.task = pRunner->task;
      (void)smUndeployTask(&undeploy, false);
    }
    taosWUnLockLatch(&pStream->lock);
  }
}

void smUndeploySnodeTasks(bool cleanup) {
  SHashObj* pHash = NULL;

  stInfo("start to undeploy snode tasks, cleanup:%d", cleanup);
  
  for (int32_t i = 0; i < STREAM_MAX_GROUP_NUM; ++i) {
    pHash = gStreamMgmt.stmGrp[i];
    smUndeployGrpSnodeTasks(pHash, cleanup);
  } 
}

void smUndeployAllVgTasks() {
  void* pIter = NULL;

  stInfo("start to undeploy ALL vg tasks, remainVgNum:%d", taosHashGetSize(gStreamMgmt.vgroupMap));
  
  while (true) {
    pIter = taosHashIterate(gStreamMgmt.vgroupMap, pIter);
    if (NULL == pIter) {
      break;
    }

    smUndeployVgTasks(*(int32_t*)taosHashGetKey(pIter, NULL), true);
  }
}

void smUndeployAllTasks(void) {
  SStreamTaskUndeploy undeploy = {0};
  undeploy.undeployMsg.doCheckpoint = true;
  undeploy.undeployMsg.doCleanup = false;
  SStreamTask* pTask = NULL;
  void* pIter = NULL;

  stInfo("start to undeploy ALL tasks, remains:%d", taosHashGetSize(gStreamMgmt.taskMap));

  smUndeployAllVgTasks();

  while (true) {
    pIter = taosHashIterate(gStreamMgmt.taskMap, pIter);
    if (NULL == pIter) {
      break;
    }

    pTask = *(SStreamTask**)pIter;
    undeploy.task = *pTask;
    (void)smUndeployTask(&undeploy, false);
  }
}

int32_t smUndeployTasks(SStreamUndeployActions* actions) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = 0;
  int32_t listNum = taosArrayGetSize(actions->taskList);
  
  for (int32_t i = 0; i < listNum; ++i) {
    SStreamTaskUndeploy* pUndeploy = taosArrayGet(actions->taskList, i);
    streamId = pUndeploy->task.streamId;
    
    (void)smUndeployTask(pUndeploy, true);
  }

  return code;

_exit:

  stsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

  return code;
}


void smUndeployFreeStreamTasks(SStreamInfo* pStream) {
  taosRLockLatch(&pStream->lock);

  SStreamTaskUndeploy undeploy = {0};
  undeploy.undeployMsg.doCheckpoint = true;
  undeploy.undeployMsg.doCleanup = false;

  SListIter iter = {0};
  SListNode* listNode = NULL;  
  if (pStream->readerList) {
    tdListInitIter(pStream->readerList, &iter, TD_LIST_FORWARD);
    while ((listNode = tdListNext(&iter)) != NULL) {
      SStreamTask* pTask = (SStreamTask*)listNode->data;
      undeploy.task = *pTask;
      ST_TASK_DLOG("task %p will be undeployed for stream reason", pTask);
      (void)smUndeployTask(&undeploy, true);
    }
  }

  if (pStream->triggerList) {
    memset(&iter, 0, sizeof(iter));
    tdListInitIter(pStream->triggerList, &iter, TD_LIST_FORWARD);
    while ((listNode = tdListNext(&iter)) != NULL) {
      SStreamTask* pTask = (SStreamTask*)listNode->data;
      undeploy.task = *pTask;
      ST_TASK_DLOG("task %p will be undeployed for stream reason", pTask);
      (void)smUndeployTask(&undeploy, true);
    }
  }

  if (pStream->runnerList) {
    memset(&iter, 0, sizeof(iter));
    tdListInitIter(pStream->runnerList, &iter, TD_LIST_FORWARD);
    while ((listNode = tdListNext(&iter)) != NULL) {
      SStreamTask* pTask = (SStreamTask*)listNode->data;
      undeploy.task = *pTask;
      ST_TASK_DLOG("task %p will be undeployed for stream reason", pTask);
      (void)smUndeployTask(&undeploy, true);
    }
  }
  
  //stmDestroySStreamInfo(pStream);

  taosRUnLockLatch(&pStream->lock);  
}


int32_t smHandleTaskMgmtRsp(SStreamMgmtRsp* pRsp) {
  int64_t streamId = pRsp->task.streamId;
  int64_t key[2] = {streamId, pRsp->task.taskId};
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SStreamTask* pTask= NULL;
  void*   taskAddr = NULL;
  
  TAOS_CHECK_EXIT(streamAcquireTask(streamId, pRsp->task.taskId, (SStreamTask**)&pTask, &taskAddr));
      
  switch (pRsp->header.msgType) {
    case STREAM_MSG_ORIGTBL_READER_INFO: {
      SStreamMgmtReq* pReq = atomic_load_ptr(&pTask->pMgmtReq);
      if (pReq && pReq->reqId == pRsp->reqId && pReq == atomic_val_compare_exchange_ptr(&pTask->pMgmtReq, pReq, NULL)) {
        taosWLockLatch(&pTask->mgmtReqLock);
        stmDestroySStreamMgmtReq(pReq);
        taosWUnLockLatch(&pTask->mgmtReqLock);
        taosMemoryFree(pReq);
      }
      STM_CHK_SET_ERROR_EXIT(stTriggerTaskExecute((SStreamTriggerTask*)pTask, &pRsp->header));
      break;
    }
    case STREAM_MSG_UPDATE_RUNNER:
    case STREAM_MSG_USER_RECALC: {
      STM_CHK_SET_ERROR_EXIT(stTriggerTaskExecute((SStreamTriggerTask*)pTask, &pRsp->header));
      break;
    }
    default:
      ST_TASK_ELOG("Invalid mgmtRsp msgType %d", pRsp->header.msgType);
      code = TSDB_CODE_STREAM_INTERNAL_ERROR;
      break;
  }
  
_exit:

  if (code) {
    stsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    ST_TASK_ILOG("handle task mgmt rsp succeed, tidx:%d", pTask->taskIdx);
  }

  streamReleaseTask(taskAddr);
  
  return code;
}

int32_t smHandleMgmtRsp(SStreamMgmtRsps* rsps) {
  int32_t rspNum = taosArrayGetSize(rsps->rspList);
  for (int32_t i = 0; i < rspNum; ++i) {
    SStreamMgmtRsp* pRsp = taosArrayGet(rsps->rspList, i);
    (void)smHandleTaskMgmtRsp(pRsp);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t smDeployTasks(SStmStreamDeploy* pDeploy) {
  int64_t streamId = pDeploy->streamId;
  int32_t gid = STREAM_GID(streamId);
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SHashObj* pGrp = gStreamMgmt.stmGrp[gid];
  SStreamInfo stream = {0};

  stsInfo("start to deploy stream, readerNum:%zu, triggerNum:%d, runnerNum:%zu", 
      taosArrayGetSize(pDeploy->readerTasks), pDeploy->triggerTask ? 1 : 0, taosArrayGetSize(pDeploy->runnerTasks));      
  
  if (NULL == pGrp) {
    gStreamMgmt.stmGrp[gid] = taosHashInit(STREAM_GRP_STREAM_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    TSDB_CHECK_NULL(gStreamMgmt.stmGrp[gid], code, lino, _exit, terrno);
    taosHashSetFreeFp(gStreamMgmt.stmGrp[gid], stmDestroySStreamInfo);
    pGrp = gStreamMgmt.stmGrp[gid];
  }
  
  int32_t taskNum = 0;
  SStreamInfo* pStream = taosHashAcquire(pGrp, &streamId, sizeof(streamId));
  if (NULL != pStream) {
    stsDebug("stream already exists, remain taskNum:%d", pStream->taskNum);
    TAOS_CHECK_EXIT(smAddTasksToStreamMap(pDeploy, pStream));
    taskNum = atomic_load_32(&pStream->taskNum);
  } else {
    TAOS_CHECK_EXIT(smAddTasksToStreamMap(pDeploy, &stream));
    TAOS_CHECK_EXIT(taosHashPut(pGrp, &streamId, sizeof(streamId), &stream, sizeof(stream)));
    taskNum = stream.taskNum;
  }
  
  stsInfo("stream deploy succeed, current taskNum:%d", taskNum);

_exit:

  taosHashRelease(pGrp, pStream);

  if (code) {
    smUndeployFreeStreamTasks(&stream);
    stsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
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

    (void)smDeployTasks(pDeploy);
  }

  return code;

_exit:

  stsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

  return code;
}


void smEnableVgDeploy(int32_t vgId) {
  stInfo("start to enable vgroup deploy, vgId:%d", vgId);

  SStreamVgReaderTasks* pVg = taosHashAcquire(gStreamMgmt.vgroupMap, &vgId, sizeof(vgId));
  if (NULL == pVg) {
    stDebug("vg %d still not in vgroupMap, ignore enable", vgId);
    return;
  }

  atomic_store_8(&pVg->inactive, 0);

  taosHashRelease(gStreamMgmt.vgroupMap, pVg);
}


