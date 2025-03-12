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
#include "streamBackendRocksdb.h"
#include "streamInt.h"
#include "tmisce.h"
#include "tref.h"
#include "tsched.h"
#include "tstream.h"
#include "ttimer.h"
#include "wal.h"

typedef struct STaskInitTs {
  int64_t start;
  int64_t end;
  bool    success;
} STaskInitTs;

static int32_t prepareBeforeStartTasks(SStreamMeta* pMeta, SArray** pList, int64_t now);
static bool    allCheckDownstreamRsp(SStreamMeta* pMeta, STaskStartInfo* pStartInfo, int32_t numOfTotal);
static void    displayStatusInfo(SStreamMeta* pMeta, SHashObj* pTaskSet, bool succ);

// restore the checkpoint id by negotiating the latest consensus checkpoint id
int32_t streamMetaStartAllTasks(SStreamMeta* pMeta) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t vgId = pMeta->vgId;
  int64_t now = taosGetTimestampMs();
  SArray* pTaskList = NULL;

  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  stInfo("vgId:%d start to consensus checkpointId for all %d task(s), start ts:%" PRId64, vgId, numOfTasks, now);

  if (numOfTasks == 0) {
    stInfo("vgId:%d no tasks exist, quit from consensus checkpointId", pMeta->vgId);

    streamMetaWLock(pMeta);
    streamMetaResetStartInfo(&pMeta->startInfo, vgId);
    streamMetaWUnLock(pMeta);
    return TSDB_CODE_SUCCESS;
  }

  code = prepareBeforeStartTasks(pMeta, &pTaskList, now);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_SUCCESS;  // ignore the error and return directly
  }

  // broadcast the check downstream tasks msg only for tasks with related fill-history tasks.
  numOfTasks = taosArrayGetSize(pTaskList);

  // prepare the fill-history task before starting all stream tasks, to avoid fill-history tasks are started without
  // initialization, when the operation of check downstream tasks status is executed far quickly.
  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId* pTaskId = taosArrayGet(pTaskList, i);
    SStreamTask*   pTask = NULL;
    code = streamMetaAcquireTask(pMeta, pTaskId->streamId, pTaskId->taskId, &pTask);
    if ((pTask == NULL) || (code != 0)) {
      stError("vgId:%d failed to acquire task:0x%x during start task, it may be dropped", pMeta->vgId, pTaskId->taskId);
      int32_t ret = streamMetaAddFailedTask(pMeta, pTaskId->streamId, pTaskId->taskId);
      if (ret) {
        stError("s-task:0x%x add check downstream failed, core:%s", pTaskId->taskId, tstrerror(ret));
      }
      continue;
    }

    if ((pTask->pBackend == NULL) && ((pTask->info.fillHistory == 1) || HAS_RELATED_FILLHISTORY_TASK(pTask))) {
      code = pMeta->expandTaskFn(pTask);
      if (code != TSDB_CODE_SUCCESS) {
        stError("s-task:0x%x vgId:%d failed to expand stream backend", pTaskId->taskId, vgId);
        streamMetaAddFailedTaskSelf(pTask, pTask->execInfo.readyTs);
      }
    }

    streamMetaReleaseTask(pMeta, pTask);
  }

  // Tasks, with related fill-history task or without any checkpoint yet, can be started directly here.
  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId* pTaskId = taosArrayGet(pTaskList, i);

    SStreamTask* pTask = NULL;
    code = streamMetaAcquireTask(pMeta, pTaskId->streamId, pTaskId->taskId, &pTask);
    if ((pTask == NULL )|| (code != 0)) {
      stError("vgId:%d failed to acquire task:0x%x during start tasks", pMeta->vgId, pTaskId->taskId);
      int32_t ret = streamMetaAddFailedTask(pMeta, pTaskId->streamId, pTaskId->taskId);
      if (ret) {
        stError("s-task:0x%x failed add check downstream failed, core:%s", pTaskId->taskId, tstrerror(ret));
      }

      continue;
    }

    STaskExecStatisInfo* pInfo = &pTask->execInfo;

    // fill-history task can only be launched by related stream tasks.
    if (pTask->info.fillHistory == 1) {
      stDebug("s-task:%s fill-history task wait related stream task start", pTask->id.idStr);
      streamMetaReleaseTask(pMeta, pTask);
      continue;
    }

    // ready now, start the related fill-history task
    if (pTask->status.downstreamReady == 1) {
      if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
        stDebug("s-task:%s downstream ready, no need to check downstream, check only related fill-history task",
                pTask->id.idStr);
        code = streamLaunchFillHistoryTask(pTask);  // todo: how about retry launch fill-history task?
        if (code) {
          stError("s-task:%s failed to launch history task, code:%s", pTask->id.idStr, tstrerror(code));
        }
      }

      code = streamMetaAddTaskLaunchResult(pMeta, pTaskId->streamId, pTaskId->taskId, pInfo->checkTs, pInfo->readyTs,
                                           true);
      streamMetaReleaseTask(pMeta, pTask);
      continue;
    }

    if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
      int32_t ret = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_INIT);
      if (ret != TSDB_CODE_SUCCESS) {
        stError("vgId:%d failed to handle event:%d", pMeta->vgId, TASK_EVENT_INIT);
        code = ret;

        // do no added into result hashmap if it is failed due to concurrently starting of this stream task.
        if (code != TSDB_CODE_STREAM_CONFLICT_EVENT) {
          streamMetaAddFailedTaskSelf(pTask, pInfo->readyTs);
        }
      }

      streamMetaReleaseTask(pMeta, pTask);
      continue;
    }

    // negotiate the consensus checkpoint id for current task
    code = streamTaskSendNegotiateChkptIdMsg(pTask);

    // this task may has no checkpoint, but others tasks may generate checkpoint already?
    streamMetaReleaseTask(pMeta, pTask);
  }

  // prepare the fill-history task before starting all stream tasks, to avoid fill-history tasks are started without
  // initialization, when the operation of check downstream tasks status is executed far quickly.
  stInfo("vgId:%d start all task(s) completed", pMeta->vgId);
  taosArrayDestroy(pTaskList);
  return code;
}

int32_t prepareBeforeStartTasks(SStreamMeta* pMeta, SArray** pList, int64_t now) {
  streamMetaWLock(pMeta);

  if (pMeta->closeFlag) {
    streamMetaWUnLock(pMeta);
    stError("vgId:%d vnode is closed, not start check task(s) downstream status", pMeta->vgId);
    return TSDB_CODE_FAILED;
  }

  *pList = taosArrayDup(pMeta->pTaskList, NULL);
  if (*pList == NULL) {
    return terrno;
  }

  taosHashClear(pMeta->startInfo.pReadyTaskSet);
  taosHashClear(pMeta->startInfo.pFailedTaskSet);
  pMeta->startInfo.startTs = now;

  int32_t code = streamMetaResetTaskStatus(pMeta);
  streamMetaWUnLock(pMeta);

  return code;
}

void streamMetaResetStartInfo(STaskStartInfo* pStartInfo, int32_t vgId) {
  taosHashClear(pStartInfo->pReadyTaskSet);
  taosHashClear(pStartInfo->pFailedTaskSet);
  pStartInfo->tasksWillRestart = 0;
  pStartInfo->readyTs = 0;
  pStartInfo->elapsedTime = 0;

  // reset the sentinel flag value to be 0
  pStartInfo->startAllTasks = 0;
  stDebug("vgId:%d clear start-all-task info", vgId);
}

int32_t streamMetaAddTaskLaunchResult(SStreamMeta* pMeta, int64_t streamId, int32_t taskId, int64_t startTs,
                                      int64_t endTs, bool ready) {
  STaskStartInfo* pStartInfo = &pMeta->startInfo;
  STaskId         id = {.streamId = streamId, .taskId = taskId};
  int32_t         vgId = pMeta->vgId;
  bool            allRsp = true;
  SStreamTask*    p = NULL;

  streamMetaWLock(pMeta);
  int32_t code = streamMetaAcquireTaskUnsafe(pMeta, &id, &p);
  if (code != 0) {  // task does not exist in current vnode, not record the complete info
    stError("vgId:%d s-task:0x%x not exists discard the check downstream info", vgId, taskId);
    streamMetaWUnLock(pMeta);
    return 0;
  }

  streamMetaReleaseTask(pMeta, p);

  if (pStartInfo->startAllTasks != 1) {
    int64_t el = endTs - startTs;
    stDebug(
        "vgId:%d not in start all task(s) process, not record launch result status, s-task:0x%x launch succ:%d elapsed "
        "time:%" PRId64 "ms",
        vgId, taskId, ready, el);
    streamMetaWUnLock(pMeta);
    return 0;
  }

  STaskInitTs initTs = {.start = startTs, .end = endTs, .success = ready};
  SHashObj*   pDst = ready ? pStartInfo->pReadyTaskSet : pStartInfo->pFailedTaskSet;
  code = taosHashPut(pDst, &id, sizeof(id), &initTs, sizeof(STaskInitTs));
  if (code) {
    if (code == TSDB_CODE_DUP_KEY) {
      stError("vgId:%d record start task result failed, s-task:0x%" PRIx64
              " already exist start results in meta start task result hashmap",
              vgId, id.taskId);
    } else {
      stError("vgId:%d failed to record start task:0x%" PRIx64 " results, start all tasks failed", vgId, id.taskId);
    }
    streamMetaWUnLock(pMeta);
    return code;
  }

  int32_t numOfTotal = streamMetaGetNumOfTasks(pMeta);
  int32_t numOfRecv = taosHashGetSize(pStartInfo->pReadyTaskSet) + taosHashGetSize(pStartInfo->pFailedTaskSet);

  allRsp = allCheckDownstreamRsp(pMeta, pStartInfo, numOfTotal);
  if (allRsp) {
    pStartInfo->readyTs = taosGetTimestampMs();
    pStartInfo->elapsedTime = (pStartInfo->startTs != 0) ? pStartInfo->readyTs - pStartInfo->startTs : 0;

    stDebug("vgId:%d all %d task(s) check downstream completed, last completed task:0x%x (succ:%d) startTs:%" PRId64
                ", readyTs:%" PRId64 " total elapsed time:%.2fs",
            vgId, numOfTotal, taskId, ready, pStartInfo->startTs, pStartInfo->readyTs,
            pStartInfo->elapsedTime / 1000.0);

    // print the initialization elapsed time and info
    displayStatusInfo(pMeta, pStartInfo->pReadyTaskSet, true);
    displayStatusInfo(pMeta, pStartInfo->pFailedTaskSet, false);
    streamMetaResetStartInfo(pStartInfo, vgId);
    streamMetaWUnLock(pMeta);

    code = pStartInfo->completeFn(pMeta);
  } else {
    streamMetaWUnLock(pMeta);
    stDebug("vgId:%d recv check downstream results, s-task:0x%x succ:%d, received:%d, total:%d", vgId, taskId, ready,
            numOfRecv, numOfTotal);
  }

  return code;
}

// check all existed tasks are received rsp
bool allCheckDownstreamRsp(SStreamMeta* pMeta, STaskStartInfo* pStartInfo, int32_t numOfTotal) {
  for (int32_t i = 0; i < numOfTotal; ++i) {
    SStreamTaskId* pTaskId = taosArrayGet(pMeta->pTaskList, i);
    if (pTaskId == NULL) {
      continue;
    }

    STaskId idx = {.streamId = pTaskId->streamId, .taskId = pTaskId->taskId};
    void*   px = taosHashGet(pStartInfo->pReadyTaskSet, &idx, sizeof(idx));
    if (px == NULL) {
      px = taosHashGet(pStartInfo->pFailedTaskSet, &idx, sizeof(idx));
      if (px == NULL) {
        return false;
      }
    }
  }

  return true;
}

void displayStatusInfo(SStreamMeta* pMeta, SHashObj* pTaskSet, bool succ) {
  int32_t vgId = pMeta->vgId;
  void*   pIter = NULL;
  size_t  keyLen = 0;

  stInfo("vgId:%d %d tasks check-downstream completed, %s", vgId, taosHashGetSize(pTaskSet),
         succ ? "success" : "failed");

  while ((pIter = taosHashIterate(pTaskSet, pIter)) != NULL) {
    STaskInitTs* pInfo = pIter;
    void*        key = taosHashGetKey(pIter, &keyLen);
    SStreamTask* pTask = NULL;
    int32_t      code = streamMetaAcquireTaskUnsafe(pMeta, key, &pTask);
    if (code == 0) {
      stInfo("s-task:%s level:%d vgId:%d, init:%" PRId64 ", initEnd:%" PRId64 ", %s", pTask->id.idStr,
             pTask->info.taskLevel, vgId, pInfo->start, pInfo->end, pInfo->success ? "success" : "failed");
      streamMetaReleaseTask(pMeta, pTask);
    } else {
      stInfo("s-task:0x%x is dropped already, %s", (int32_t)((STaskId*)key)->taskId, succ ? "success" : "failed");
    }
  }
}

int32_t streamMetaInitStartInfo(STaskStartInfo* pStartInfo) {
  _hash_fn_t fp = taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR);

  pStartInfo->pReadyTaskSet = taosHashInit(64, fp, false, HASH_NO_LOCK);
  if (pStartInfo->pReadyTaskSet == NULL) {
    return terrno;
  }

  pStartInfo->pFailedTaskSet = taosHashInit(4, fp, false, HASH_NO_LOCK);
  if (pStartInfo->pFailedTaskSet == NULL) {
    return terrno;
  }

  return 0;
}

void streamMetaClearStartInfo(STaskStartInfo* pStartInfo) {
  taosHashCleanup(pStartInfo->pReadyTaskSet);
  taosHashCleanup(pStartInfo->pFailedTaskSet);
  pStartInfo->readyTs = 0;
  pStartInfo->elapsedTime = 0;
  pStartInfo->startTs = 0;
  pStartInfo->startAllTasks = 0;
  pStartInfo->tasksWillRestart = 0;
  pStartInfo->restartCount = 0;
}

int32_t streamMetaStartOneTask(SStreamMeta* pMeta, int64_t streamId, int32_t taskId) {
  int32_t      code = 0;
  int32_t      vgId = pMeta->vgId;
  SStreamTask* pTask = NULL;
  bool         continueExec = true;

  stInfo("vgId:%d start task:0x%x by checking it's downstream status", vgId, taskId);

  code = streamMetaAcquireTask(pMeta, streamId, taskId, &pTask);
  if ((pTask == NULL) || (code != 0)) {
    stError("vgId:%d failed to acquire task:0x%x when starting task", vgId, taskId);
    int32_t ret = streamMetaAddFailedTask(pMeta, streamId, taskId);
    if (ret) {
      stError("s-task:0x%x add check downstream failed, core:%s", taskId, tstrerror(ret));
    }

    return TSDB_CODE_STREAM_TASK_IVLD_STATUS;
  }

  // fill-history task can only be launched by related stream tasks.
  STaskExecStatisInfo* pInfo = &pTask->execInfo;
  if (pTask->info.fillHistory == 1) {
    stError("s-task:0x%x vgId:%d fill-history task, not start here", taskId, vgId);
    streamMetaReleaseTask(pMeta, pTask);
    return TSDB_CODE_SUCCESS;
  }

  // the start all tasks procedure may happen to start the newly deployed stream task, and results in the
  // concurrently start this task by two threads.
  streamMutexLock(&pTask->lock);

  SStreamTaskState status = streamTaskGetStatus(pTask);
  if (status.state != TASK_STATUS__UNINIT) {
    stError("s-task:0x%x vgId:%d status:%s not uninit status, not start stream task", taskId, vgId, status.name);
    continueExec = false;
  } else {
    continueExec = true;
  }
  streamMutexUnlock(&pTask->lock);

  if (!continueExec) {
    streamMetaReleaseTask(pMeta, pTask);
    return TSDB_CODE_STREAM_TASK_IVLD_STATUS;
  }

  if(pTask->status.downstreamReady != 0) {
    stFatal("s-task:0x%x downstream should be not ready, but it ready here, internal error happens", taskId);
    streamMetaReleaseTask(pMeta, pTask);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  // avoid initialization and destroy running concurrently.
  streamMutexLock(&pTask->lock);
  if (pTask->pBackend == NULL) {
    code = pMeta->expandTaskFn(pTask);
    streamMutexUnlock(&pTask->lock);

    if (code != TSDB_CODE_SUCCESS) {
      streamMetaAddFailedTaskSelf(pTask, pInfo->readyTs);
    }
  } else {
    streamMutexUnlock(&pTask->lock);
  }

  // concurrently start task may cause the latter started task be failed, and also failed to added into meta result.
  if (code == TSDB_CODE_SUCCESS) {
    code = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_INIT);
    if (code != TSDB_CODE_SUCCESS) {
      stError("s-task:%s vgId:%d failed to handle event:init-task, code:%s", pTask->id.idStr, pMeta->vgId,
              tstrerror(code));

      // do no added into result hashmap if it is failed due to concurrently starting of this stream task.
      if (code != TSDB_CODE_STREAM_CONFLICT_EVENT) {
        streamMetaAddFailedTaskSelf(pTask, pInfo->readyTs);
      }
    }
  }

  streamMetaReleaseTask(pMeta, pTask);
  return code;
}

int32_t streamMetaStopAllTasks(SStreamMeta* pMeta) {
  streamMetaRLock(pMeta);

  SArray* pTaskList = NULL;
  int32_t num = taosArrayGetSize(pMeta->pTaskList);
  stDebug("vgId:%d stop all %d stream task(s)", pMeta->vgId, num);

  if (num == 0) {
    stDebug("vgId:%d stop all %d task(s) completed, elapsed time:0 Sec.", pMeta->vgId, num);
    streamMetaRUnLock(pMeta);
    return TSDB_CODE_SUCCESS;
  }

  int64_t st = taosGetTimestampMs();

  // send hb msg to mnode before closing all tasks.
  int32_t code = streamMetaSendMsgBeforeCloseTasks(pMeta, &pTaskList);
  if (code != TSDB_CODE_SUCCESS) {
    streamMetaRUnLock(pMeta);
    return code;
  }

  int32_t numOfTasks = taosArrayGetSize(pTaskList);
  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId* pTaskId = taosArrayGet(pTaskList, i);
    SStreamTask*   pTask = NULL;

    code = streamMetaAcquireTaskNoLock(pMeta, pTaskId->streamId, pTaskId->taskId, &pTask);
    if (code != TSDB_CODE_SUCCESS) {
      continue;
    }

    int32_t ret = streamTaskStop(pTask);
    if (ret) {
      stError("s-task:0x%x failed to stop task, code:%s", pTaskId->taskId, tstrerror(ret));
    }

    streamMetaReleaseTask(pMeta, pTask);
  }

  taosArrayDestroy(pTaskList);

  double el = (taosGetTimestampMs() - st) / 1000.0;
  stDebug("vgId:%d stop all %d task(s) completed, elapsed time:%.2f Sec.", pMeta->vgId, num, el);

  streamMetaRUnLock(pMeta);
  return code;
}

int32_t streamTaskCheckIfReqConsenChkptId(SStreamTask* pTask, int64_t ts) {
  SConsenChkptInfo* pConChkptInfo = &pTask->status.consenChkptInfo;

  int32_t vgId = pTask->pMeta->vgId;
  if (pConChkptInfo->status == TASK_CONSEN_CHKPT_REQ) {
    // mark the sending of req consensus checkpoint request.
    pConChkptInfo->status = TASK_CONSEN_CHKPT_SEND;
    pConChkptInfo->statusTs = ts;
    stDebug("s-task:%s vgId:%d set requiring consensus-chkptId in hbMsg, ts:%" PRId64, pTask->id.idStr,
            vgId, pConChkptInfo->statusTs);
    return 1;
  } else {
    int32_t el = (ts - pConChkptInfo->statusTs) / 1000;

    // not recv consensus-checkpoint rsp for 60sec, send it again in hb to mnode
    if ((pConChkptInfo->status == TASK_CONSEN_CHKPT_SEND) && el > 60) {
      pConChkptInfo->statusTs = ts;

      stWarn(
          "s-task:%s vgId:%d not recv consensus-chkptId for %ds(more than 60s), set requiring in Hb again, ts:%" PRId64,
          pTask->id.idStr, vgId, el, pConChkptInfo->statusTs);
      return 1;
    }
  }

  return 0;
}

void streamTaskSetConsenChkptIdRecv(SStreamTask* pTask, int32_t transId, int64_t ts) {
  SConsenChkptInfo* pInfo = &pTask->status.consenChkptInfo;
  pInfo->consenChkptTransId = transId;
  pInfo->status = TASK_CONSEN_CHKPT_RECV;
  pInfo->statusTs = ts;

  stInfo("s-task:%s set recv consen-checkpointId, transId:%d", pTask->id.idStr, transId);
}

void streamTaskSetReqConsenChkptId(SStreamTask* pTask, int64_t ts) {
  SConsenChkptInfo* pInfo = &pTask->status.consenChkptInfo;
  int32_t           prevTrans = pInfo->consenChkptTransId;

  pInfo->status = TASK_CONSEN_CHKPT_REQ;
  pInfo->statusTs = ts;
  pInfo->consenChkptTransId = 0;

  stDebug("s-task:%s set req consen-checkpointId flag, prev transId:%d, ts:%" PRId64, pTask->id.idStr, prevTrans, ts);
}

int32_t streamMetaAddFailedTask(SStreamMeta* pMeta, int64_t streamId, int32_t taskId) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int64_t      now = taosGetTimestampMs();
  int64_t      startTs = 0;
  bool         hasFillhistoryTask = false;
  STaskId      hId = {0};
  STaskId      id = {.streamId = streamId, .taskId = taskId};
  SStreamTask* pTask = NULL;

  stDebug("vgId:%d add start failed task:0x%x", pMeta->vgId, taskId);

  streamMetaRLock(pMeta);

  code = streamMetaAcquireTaskUnsafe(pMeta, &id, &pTask);
  if (code == 0) {
    startTs = pTask->taskCheckInfo.startTs;
    hasFillhistoryTask = HAS_RELATED_FILLHISTORY_TASK(pTask);
    hId = pTask->hTaskInfo.id;
    streamMetaReleaseTask(pMeta, pTask);

    streamMetaRUnLock(pMeta);

    // add the failed task info, along with the related fill-history task info into tasks list.
    code = streamMetaAddTaskLaunchResult(pMeta, streamId, taskId, startTs, now, false);
    if (hasFillhistoryTask) {
      code = streamMetaAddTaskLaunchResult(pMeta, hId.streamId, hId.taskId, startTs, now, false);
    }
  } else {
    streamMetaRUnLock(pMeta);

    stError("failed to locate the stream task:0x%" PRIx64 "-0x%x (vgId:%d), it may have been destroyed or stopped",
            streamId, taskId, pMeta->vgId);
    code = TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  return code;
}

void streamMetaAddFailedTaskSelf(SStreamTask* pTask, int64_t failedTs) {
  int32_t startTs = pTask->execInfo.checkTs;
  int32_t code = streamMetaAddTaskLaunchResult(pTask->pMeta, pTask->id.streamId, pTask->id.taskId, startTs, failedTs, false);
  if (code) {
    stError("s-task:%s failed to add self task failed to start, code:%s", pTask->id.idStr, tstrerror(code));
  }

  // automatically set the related fill-history task to be failed.
  if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
    STaskId* pId = &pTask->hTaskInfo.id;
    code = streamMetaAddTaskLaunchResult(pTask->pMeta, pId->streamId, pId->taskId, startTs, failedTs, false);
    if (code) {
      stError("s-task:0x%" PRIx64 " failed to add self task failed to start, code:%s", pId->taskId, tstrerror(code));
    }
  }
}
