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

#include "catalog.h"
#include "command.h"
#include "query.h"
#include "qworker.h"
#include "schInt.h"
#include "tglobal.h"
#include "tmsg.h"
#include "tref.h"
#include "trpc.h"

void schFreeTask(SSchJob *pJob, SSchTask *pTask) {
  schDeregisterTaskHb(pJob, pTask);

  if (pTask->candidateAddrs) {
    taosArrayDestroy(pTask->candidateAddrs);
  }

  taosMemoryFreeClear(pTask->msg);

  if (pTask->children) {
    taosArrayDestroy(pTask->children);
  }

  if (pTask->parents) {
    taosArrayDestroy(pTask->parents);
  }

  if (pTask->execNodes) {
    taosHashCleanup(pTask->execNodes);
  }

  taosArrayDestroy(pTask->profile.execTime);
}

void schInitTaskRetryTimes(SSchJob *pJob, SSchTask *pTask, SSchLevel *pLevel) {
  if (SCH_IS_DATA_BIND_TASK(pTask) || (!SCH_IS_QUERY_JOB(pJob)) || (SCH_ALL != schMgmt.cfg.schPolicy)) {
    pTask->maxRetryTimes = SCH_DEFAULT_MAX_RETRY_NUM;
  } else {
    int32_t nodeNum = taosArrayGetSize(pJob->nodeList);
    pTask->maxRetryTimes = TMAX(nodeNum, SCH_DEFAULT_MAX_RETRY_NUM);
  }

  pTask->maxExecTimes = pTask->maxRetryTimes * (pLevel->level + 1);
}

int32_t schInitTask(SSchJob *pJob, SSchTask *pTask, SSubplan *pPlan, SSchLevel *pLevel) {
  int32_t code = 0;

  pTask->plan = pPlan;
  pTask->level = pLevel;
  pTask->execId = -1;
  pTask->failedExecId = -2;
  pTask->timeoutUsec = SCH_DEFAULT_TASK_TIMEOUT_USEC;
  pTask->taskId = schGenTaskId();

  schInitTaskRetryTimes(pJob, pTask, pLevel);

  pTask->execNodes =
      taosHashInit(pTask->maxExecTimes, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  pTask->profile.execTime = taosArrayInit(pTask->maxExecTimes, sizeof(int64_t));
  if (NULL == pTask->execNodes || NULL == pTask->profile.execTime) {
    SCH_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_INIT);

  SCH_TASK_DLOG("task initialized, max times %d:%d", pTask->maxRetryTimes, pTask->maxExecTimes);

  return TSDB_CODE_SUCCESS;

_return:

  taosArrayDestroy(pTask->profile.execTime);
  taosHashCleanup(pTask->execNodes);

  SCH_RET(code);
}

int32_t schRecordTaskSucceedNode(SSchJob *pJob, SSchTask *pTask) {
  if (SCH_IS_LOCAL_EXEC_TASK(pJob, pTask)) {
    return TSDB_CODE_SUCCESS;
  }

  SQueryNodeAddr *addr = taosArrayGet(pTask->candidateAddrs, pTask->candidateIdx);
  if (NULL == addr) {
    SCH_TASK_ELOG("taosArrayGet candidate addr failed, idx:%d, size:%d", pTask->candidateIdx,
                  (int32_t)taosArrayGetSize(pTask->candidateAddrs));
    SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
  }

  pTask->succeedAddr = *addr;

  return TSDB_CODE_SUCCESS;
}

int32_t schAppendTaskExecNode(SSchJob *pJob, SSchTask *pTask, SQueryNodeAddr *addr, int32_t execId) {
  SSchNodeInfo nodeInfo = {.addr = *addr, .handle = SCH_GET_TASK_HANDLE(pTask)};

  if (taosHashPut(pTask->execNodes, &execId, sizeof(execId), &nodeInfo, sizeof(nodeInfo))) {
    SCH_TASK_ELOG("taosHashPut nodeInfo to execNodes failed, errno:%d", errno);
    SCH_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCH_TASK_DLOG("task execNode added, execId:%d, handle:%p", execId, nodeInfo.handle);

  return TSDB_CODE_SUCCESS;
}

int32_t schDropTaskExecNode(SSchJob *pJob, SSchTask *pTask, void *handle, int32_t execId) {
  if (NULL == pTask->execNodes) {
    return TSDB_CODE_SUCCESS;
  }

  if (taosHashRemove(pTask->execNodes, &execId, sizeof(execId))) {
    SCH_TASK_DLOG("execId %d already not in execNodeList", execId);
    SCH_ERR_RET(TSDB_CODE_SCH_IGNORE_ERROR);
  } else {
    SCH_TASK_DLOG("execId %d removed from execNodeList", execId);
  }

  if ((execId != pTask->execId) || pTask->waitRetry) {  // ignore it
    SCH_TASK_DLOG("execId %d is already not current execId %d, waitRetry %d", execId, pTask->execId, pTask->waitRetry);
    SCH_ERR_RET(TSDB_CODE_SCH_IGNORE_ERROR);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schUpdateTaskExecNode(SSchJob *pJob, SSchTask *pTask, void *handle, int32_t execId) {
  if (taosHashGetSize(pTask->execNodes) <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SSchNodeInfo *nodeInfo = taosHashGet(pTask->execNodes, &execId, sizeof(execId));
  if (NULL == nodeInfo) {  // ignore it
    SCH_TASK_DLOG("handle not updated since execId %d already not exist, current execId %d, waitRetry %d", execId,
                  pTask->execId, pTask->waitRetry);
    return TSDB_CODE_SUCCESS;
  }

  nodeInfo->handle = handle;

  SCH_TASK_DLOG("handle updated to %p for execId %d", handle, execId);

  return TSDB_CODE_SUCCESS;
}

int32_t schUpdateTaskHandle(SSchJob *pJob, SSchTask *pTask, bool dropExecNode, void *handle, int32_t execId) {
  if (dropExecNode) {
    SCH_RET(schDropTaskExecNode(pJob, pTask, handle, execId));
  }

  schUpdateTaskExecNode(pJob, pTask, handle, execId);

  if ((execId != pTask->execId || execId <= pTask->failedExecId) || pTask->waitRetry) {  // ignore it
    SCH_TASK_DLOG("handle not updated since execId %d is already not current execId %d, waitRetry %d", execId,
                  pTask->execId, pTask->waitRetry);
    SCH_ERR_RET(TSDB_CODE_SCH_IGNORE_ERROR);
  }

  SCH_SET_TASK_HANDLE(pTask, handle);

  return TSDB_CODE_SUCCESS;
}

int32_t schProcessOnTaskFailure(SSchJob *pJob, SSchTask *pTask, int32_t errCode) {
  if (TSDB_CODE_SCH_IGNORE_ERROR == errCode) {
    return TSDB_CODE_SCH_IGNORE_ERROR;
  }

  pTask->failedExecId = pTask->execId;

  int8_t jobStatus = 0;
  if (schJobNeedToStop(pJob, &jobStatus)) {
    SCH_TASK_DLOG("no more task failure processing cause of job status %s", jobTaskStatusStr(jobStatus));
    SCH_ERR_RET(TSDB_CODE_SCH_IGNORE_ERROR);
  }

  int8_t taskStatus = SCH_GET_TASK_STATUS(pTask);
  if (taskStatus == JOB_TASK_STATUS_FAIL || taskStatus == JOB_TASK_STATUS_SUCC) {
    SCH_TASK_ELOG("task already done, status:%s", jobTaskStatusStr(taskStatus));
    SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  if (errCode == TSDB_CODE_SCH_TIMEOUT_ERROR) {
    SCH_LOG_TASK_WAIT_TS(pTask);
  } else {
    SCH_LOG_TASK_END_TS(pTask);
  }

  bool    needRetry = false;
  bool    moved = false;
  int32_t taskDone = 0;

  SCH_TASK_DLOG("taskOnFailure, code:%s", tstrerror(errCode));

  SCH_ERR_RET(schTaskCheckSetRetry(pJob, pTask, errCode, &needRetry));

  if (!needRetry) {
    SCH_TASK_ELOG("task failed and no more retry, code:%s", tstrerror(errCode));

    SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_FAIL);

    if (SCH_JOB_NEED_WAIT(pJob)) {
      SCH_LOCK(SCH_WRITE, &pTask->level->lock);
      pTask->level->taskFailed++;
      taskDone = pTask->level->taskSucceed + pTask->level->taskFailed;
      SCH_UNLOCK(SCH_WRITE, &pTask->level->lock);

      schUpdateJobErrCode(pJob, errCode);

      if (taskDone < pTask->level->taskNum) {
        SCH_TASK_DLOG("need to wait other tasks, doneNum:%d, allNum:%d", taskDone, pTask->level->taskNum);
        SCH_RET(TSDB_CODE_SCH_IGNORE_ERROR);
      }

      SCH_RET(atomic_load_32(&pJob->errCode));
    }
  } else {
    SCH_ERR_RET(schHandleTaskRetry(pJob, pTask));

    return TSDB_CODE_SUCCESS;
  }

  SCH_RET(errCode);
}

int32_t schProcessOnTaskSuccess(SSchJob *pJob, SSchTask *pTask) {
  bool    moved = false;
  int32_t code = 0;

  SCH_TASK_DLOG("taskOnSuccess, status:%s", SCH_GET_TASK_STATUS_STR(pTask));

  SCH_LOG_TASK_END_TS(pTask);

  SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_PART_SUCC);

  SCH_ERR_RET(schRecordTaskSucceedNode(pJob, pTask));

  SCH_ERR_RET(schLaunchTasksInFlowCtrlList(pJob, pTask));

  int32_t parentNum = pTask->parents ? (int32_t)taosArrayGetSize(pTask->parents) : 0;
  if (parentNum == 0) {
    int32_t taskDone = 0;
    if (SCH_JOB_NEED_WAIT(pJob)) {
      SCH_LOCK(SCH_WRITE, &pTask->level->lock);
      pTask->level->taskSucceed++;
      taskDone = pTask->level->taskSucceed + pTask->level->taskFailed;
      SCH_UNLOCK(SCH_WRITE, &pTask->level->lock);

      if (taskDone < pTask->level->taskNum) {
        SCH_TASK_DLOG("wait all tasks, done:%d, all:%d", taskDone, pTask->level->taskNum);
        return TSDB_CODE_SUCCESS;
      } else if (taskDone > pTask->level->taskNum) {
        SCH_TASK_ELOG("taskDone number invalid, done:%d, total:%d", taskDone, pTask->level->taskNum);
      }

      if (pTask->level->taskFailed > 0) {
        SCH_RET(schHandleJobFailure(pJob, pJob->errCode));
      } else {
        SCH_RET(schSwitchJobStatus(pJob, JOB_TASK_STATUS_PART_SUCC, NULL));
      }
    } else {
      pJob->resNode = pTask->succeedAddr;
    }

    pJob->fetchTask = pTask;

    SCH_RET(schSwitchJobStatus(pJob, JOB_TASK_STATUS_PART_SUCC, NULL));
  }

  /*
    if (SCH_IS_DATA_SRC_TASK(task) && job->dataSrcEps.numOfEps < SCH_MAX_CANDIDATE_EP_NUM) {
      strncpy(job->dataSrcEps.fqdn[job->dataSrcEps.numOfEps], task->execAddr.fqdn, sizeof(task->execAddr.fqdn));
      job->dataSrcEps.port[job->dataSrcEps.numOfEps] = task->execAddr.port;

      ++job->dataSrcEps.numOfEps;
    }
  */

  for (int32_t i = 0; i < parentNum; ++i) {
    SSchTask *parent = *(SSchTask **)taosArrayGet(pTask->parents, i);

    SCH_LOCK(SCH_WRITE, &parent->planLock);
    SDownstreamSourceNode source = {
        .type = QUERY_NODE_DOWNSTREAM_SOURCE,
        .taskId = pTask->taskId,
        .schedId = schMgmt.sId,
        .execId = pTask->execId,
        .addr = pTask->succeedAddr,
        .fetchMsgType = SCH_FETCH_TYPE(pTask),
        .localExec = SCH_IS_LOCAL_EXEC_TASK(pJob, pTask),
    };
    qSetSubplanExecutionNode(parent->plan, pTask->plan->id.groupId, &source);
    SCH_UNLOCK(SCH_WRITE, &parent->planLock);

    int32_t readyNum = atomic_add_fetch_32(&parent->childReady, 1);

    if (SCH_TASK_READY_FOR_LAUNCH(readyNum, parent)) {
      SCH_TASK_DLOG("all %d children task done, start to launch parent task 0x%" PRIx64, readyNum, parent->taskId);
      SCH_ERR_RET(schLaunchTask(pJob, parent));
    }
  }

  SCH_ERR_RET(schLaunchJobLowerLevel(pJob, pTask));

  return TSDB_CODE_SUCCESS;
}

int32_t schRescheduleTask(SSchJob *pJob, SSchTask *pTask) {
  if (!schMgmt.cfg.enableReSchedule) {
    return TSDB_CODE_SUCCESS;
  }

  if (SCH_IS_DATA_BIND_TASK(pTask)) {
    return TSDB_CODE_SUCCESS;
  }

  if (SCH_TASK_TIMEOUT(pTask) && JOB_TASK_STATUS_EXEC == pTask->status && pJob->fetchTask != pTask &&
      taosArrayGetSize(pTask->candidateAddrs) > 1) {
    SCH_TASK_DLOG("task execId %d will be rescheduled now", pTask->execId);
    schDropTaskOnExecNode(pJob, pTask);
    taosHashClear(pTask->execNodes);

    SCH_ERR_RET(schProcessOnTaskFailure(pJob, pTask, TSDB_CODE_SCH_TIMEOUT_ERROR));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schChkUpdateRedirectCtx(SSchJob *pJob, SSchTask *pTask, SEpSet *pEpSet, int32_t rspCode) {
  SSchRedirectCtx *pCtx = &pTask->redirectCtx;
  if (!pCtx->inRedirect) {
    pCtx->inRedirect = true;
    pCtx->periodMs = tsRedirectPeriod;
    pCtx->startTs = taosGetTimestampMs();

    if (SCH_IS_DATA_BIND_TASK(pTask)) {
      if (pEpSet) {
        pCtx->roundTotal = pEpSet->numOfEps;
      } else {
        SQueryNodeAddr *pAddr = taosArrayGet(pTask->candidateAddrs, 0);
        pCtx->roundTotal = pAddr->epSet.numOfEps;
      }
    } else {
      pCtx->roundTotal = 1;
    }

    goto _return;
  }

  pCtx->totalTimes++;
  pCtx->roundTimes++;

  if (SCH_IS_DATA_BIND_TASK(pTask) && pEpSet) {
    pCtx->roundTotal = pEpSet->numOfEps;
  }

  if (pCtx->roundTimes >= pCtx->roundTotal) {
    int64_t nowTs = taosGetTimestampMs();
    int64_t lastTime = nowTs - pCtx->startTs;
    if (lastTime > tsMaxRetryWaitTime) {
      SCH_TASK_DLOG("task no more redirect retry since timeout, now:%" PRId64 ", start:%" PRId64 ", max:%d, total:%d",
                    nowTs, pCtx->startTs, tsMaxRetryWaitTime, pCtx->totalTimes);
      pJob->noMoreRetry = true;
      SCH_ERR_RET(SCH_GET_REDIRECT_CODE(pJob, rspCode));
    }

    pCtx->periodMs *= tsRedirectFactor;
    if (pCtx->periodMs > tsRedirectMaxPeriod) {
      pCtx->periodMs = tsRedirectMaxPeriod;
    }

    int64_t leftTime = tsMaxRetryWaitTime - lastTime;
    pTask->delayExecMs = leftTime < pCtx->periodMs ? leftTime : pCtx->periodMs;

    pCtx->roundTimes = 0;

    goto _return;
  }

  pTask->delayExecMs = 0;

_return:

  SCH_TASK_DLOG("task start %d/%d/%d redirect retry, delayExec:%d", pCtx->roundTimes, pCtx->roundTotal,
                pCtx->totalTimes, pTask->delayExecMs);

  return TSDB_CODE_SUCCESS;
}

void schResetTaskForRetry(SSchJob *pJob, SSchTask *pTask) {
  pTask->waitRetry = true;

  schDropTaskOnExecNode(pJob, pTask);
  if (pTask->delayTimer) {
    taosTmrStopA(&pTask->delayTimer);
  }
  taosHashClear(pTask->execNodes);
  schRemoveTaskFromExecList(pJob, pTask);
  schDeregisterTaskHb(pJob, pTask);
  taosMemoryFreeClear(pTask->msg);
  pTask->msgLen = 0;
  pTask->lastMsgType = 0;
  pTask->childReady = 0;
  memset(&pTask->succeedAddr, 0, sizeof(pTask->succeedAddr));
}

int32_t schDoTaskRedirect(SSchJob *pJob, SSchTask *pTask, SDataBuf *pData, int32_t rspCode) {
  int32_t code = 0;

  SCH_TASK_DLOG("task will be redirected now, status:%s, code:%s", SCH_GET_TASK_STATUS_STR(pTask), tstrerror(rspCode));

  if (!NO_RET_REDIRECT_ERROR(rspCode)) {
    SCH_UPDATE_REDIRECT_CODE(pJob, rspCode);
  }

  SCH_ERR_JRET(schChkUpdateRedirectCtx(pJob, pTask, pData ? pData->pEpSet : NULL, rspCode));

  schResetTaskForRetry(pJob, pTask);

  if (SCH_IS_DATA_BIND_TASK(pTask)) {
    if (pData && pData->pEpSet) {
      SCH_ERR_JRET(schUpdateTaskCandidateAddr(pJob, pTask, pData->pEpSet));
    } else if (SYNC_SELF_LEADER_REDIRECT_ERROR(rspCode)) {
      SQueryNodeAddr *addr = taosArrayGet(pTask->candidateAddrs, pTask->candidateIdx);
      SEp *           pEp = &addr->epSet.eps[addr->epSet.inUse];
      SCH_TASK_DLOG("task retry node %d current ep, idx:%d/%d,%s:%d, code:%s", addr->nodeId, addr->epSet.inUse,
                    addr->epSet.numOfEps, pEp->fqdn, pEp->port, tstrerror(rspCode));
    } else {
      SQueryNodeAddr *addr = taosArrayGet(pTask->candidateAddrs, pTask->candidateIdx);
      SCH_SWITCH_EPSET(addr);
      SCH_TASK_DLOG("switch task target node %d epset to %d/%d", addr->nodeId, addr->epSet.inUse, addr->epSet.numOfEps);
    }

    SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_INIT);

    SCH_ERR_JRET(schDelayLaunchTask(pJob, pTask));

    return TSDB_CODE_SUCCESS;
  }

  // merge plan

  pTask->childReady = 0;

  qClearSubplanExecutionNode(pTask->plan);

  // Note: current error task and upper level merge task
  if ((pData && 0 == pData->len) || NULL == pData) {
    SCH_ERR_JRET(schSwitchTaskCandidateAddr(pJob, pTask));
  }

  SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_INIT);

  int32_t childrenNum = taosArrayGetSize(pTask->children);
  for (int32_t i = 0; i < childrenNum; ++i) {
    SSchTask *pChild = taosArrayGetP(pTask->children, i);
    SCH_LOCK_TASK(pChild);
    schDoTaskRedirect(pJob, pChild, NULL, rspCode);
    SCH_UNLOCK_TASK(pChild);
  }

  return TSDB_CODE_SUCCESS;

_return:

  SCH_RET(schProcessOnTaskFailure(pJob, pTask, code));
}

int32_t schHandleTaskSetRetry(SSchJob *pJob, SSchTask *pTask, SDataBuf *pData, int32_t rspCode) {
  int32_t code = 0;

  SCH_ERR_JRET(schChkResetJobRetry(pJob, rspCode));

  if (SYNC_OTHER_LEADER_REDIRECT_ERROR(rspCode)) {
    if (NULL == pData->pEpSet) {
      SCH_TASK_ELOG("epset updating excepted, error:%s", tstrerror(rspCode));
      code = TSDB_CODE_INVALID_MSG;
      goto _return;
    }
  }

  SCH_TASK_DLOG("start to redirect current task set cause of error: %s", tstrerror(rspCode));

  for (int32_t i = 0; i < pJob->levelNum; ++i) {
    SSchLevel *pLevel = taosArrayGet(pJob->levels, i);

    pLevel->taskExecDoneNum = 0;
    pLevel->taskLaunchedNum = 0;
  }

  SCH_RESET_JOB_LEVEL_IDX(pJob);

  code = schDoTaskRedirect(pJob, pTask, pData, rspCode);

  taosMemoryFreeClear(pData->pData);
  taosMemoryFreeClear(pData->pEpSet);

  SCH_RET(code);

_return:

  taosMemoryFreeClear(pData->pData);
  taosMemoryFreeClear(pData->pEpSet);

  SCH_RET(schProcessOnTaskFailure(pJob, pTask, code));
}

int32_t schPushTaskToExecList(SSchJob *pJob, SSchTask *pTask) {
  int32_t code = taosHashPut(pJob->execTasks, &pTask->taskId, sizeof(pTask->taskId), &pTask, POINTER_BYTES);
  if (0 != code) {
    if (HASH_NODE_EXIST(code)) {
      SCH_TASK_ELOG("task already in execTask list, code:%x", code);
      SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
    }

    SCH_TASK_ELOG("taosHashPut task to execTask list failed, errno:%d", errno);
    SCH_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCH_TASK_DLOG("task added to execTask list, numOfTasks:%d", taosHashGetSize(pJob->execTasks));

  return TSDB_CODE_SUCCESS;
}

/*
int32_t schMoveTaskToSuccList(SSchJob *pJob, SSchTask *pTask, bool *moved) {
  if (0 != taosHashRemove(pJob->execTasks, &pTask->taskId, sizeof(pTask->taskId))) {
    SCH_TASK_WLOG("remove task from execTask list failed, may not exist, status:%s", SCH_GET_TASK_STATUS_STR(pTask));
  } else {
    SCH_TASK_DLOG("task removed from execTask list, numOfTasks:%d", taosHashGetSize(pJob->execTasks));
  }

  int32_t code = taosHashPut(pJob->succTasks, &pTask->taskId, sizeof(pTask->taskId), &pTask, POINTER_BYTES);
  if (0 != code) {
    if (HASH_NODE_EXIST(code)) {
      *moved = true;
      SCH_TASK_ELOG("task already in succTask list, status:%s", SCH_GET_TASK_STATUS_STR(pTask));
      SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
    }

    SCH_TASK_ELOG("taosHashPut task to succTask list failed, errno:%d", errno);
    SCH_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  *moved = true;

  SCH_TASK_DLOG("task moved to succTask list, numOfTasks:%d", taosHashGetSize(pJob->succTasks));

  return TSDB_CODE_SUCCESS;
}

int32_t schMoveTaskToFailList(SSchJob *pJob, SSchTask *pTask, bool *moved) {
  *moved = false;

  if (0 != taosHashRemove(pJob->execTasks, &pTask->taskId, sizeof(pTask->taskId))) {
    SCH_TASK_WLOG("remove task from execTask list failed, may not exist, status:%s", SCH_GET_TASK_STATUS_STR(pTask));
  }

  int32_t code = taosHashPut(pJob->failTasks, &pTask->taskId, sizeof(pTask->taskId), &pTask, POINTER_BYTES);
  if (0 != code) {
    if (HASH_NODE_EXIST(code)) {
      *moved = true;

      SCH_TASK_WLOG("task already in failTask list, status:%s", SCH_GET_TASK_STATUS_STR(pTask));
      SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
    }

    SCH_TASK_ELOG("taosHashPut task to failTask list failed, errno:%d", errno);
    SCH_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  *moved = true;

  SCH_TASK_DLOG("task moved to failTask list, numOfTasks:%d", taosHashGetSize(pJob->failTasks));

  return TSDB_CODE_SUCCESS;
}

int32_t schMoveTaskToExecList(SSchJob *pJob, SSchTask *pTask, bool *moved) {
  if (0 != taosHashRemove(pJob->succTasks, &pTask->taskId, sizeof(pTask->taskId))) {
    SCH_TASK_WLOG("remove task from succTask list failed, may not exist, status:%s", SCH_GET_TASK_STATUS_STR(pTask));
  }

  int32_t code = taosHashPut(pJob->execTasks, &pTask->taskId, sizeof(pTask->taskId), &pTask, POINTER_BYTES);
  if (0 != code) {
    if (HASH_NODE_EXIST(code)) {
      *moved = true;

      SCH_TASK_ELOG("task already in execTask list, status:%s", SCH_GET_TASK_STATUS_STR(pTask));
      SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
    }

    SCH_TASK_ELOG("taosHashPut task to execTask list failed, errno:%d", errno);
    SCH_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  *moved = true;

  SCH_TASK_DLOG("task moved to execTask list, numOfTasks:%d", taosHashGetSize(pJob->execTasks));

  return TSDB_CODE_SUCCESS;
}
*/

int32_t schTaskCheckSetRetry(SSchJob *pJob, SSchTask *pTask, int32_t errCode, bool *needRetry) {
  if (pJob->noMoreRetry) {
    *needRetry = false;
    SCH_TASK_DLOG("task no more retry since job no more retry, retryTimes:%d/%d", pTask->retryTimes,
                  pTask->maxRetryTimes);
    return TSDB_CODE_SUCCESS;
  }

  if (TSDB_CODE_SCH_TIMEOUT_ERROR == errCode) {
    pTask->maxExecTimes++;
    pTask->maxRetryTimes++;
    if (pTask->timeoutUsec < SCH_MAX_TASK_TIMEOUT_USEC) {
      pTask->timeoutUsec *= 2;
      if (pTask->timeoutUsec > SCH_MAX_TASK_TIMEOUT_USEC) {
        pTask->timeoutUsec = SCH_MAX_TASK_TIMEOUT_USEC;
      }
    }
  }

  if ((pTask->retryTimes + 1) > pTask->maxRetryTimes) {
    *needRetry = false;
    SCH_TASK_DLOG("task no more retry since reach max retry times, retryTimes:%d/%d", pTask->retryTimes,
                  pTask->maxRetryTimes);
    return TSDB_CODE_SUCCESS;
  }

  if ((pTask->execId + 1) >= pTask->maxExecTimes) {
    *needRetry = false;
    SCH_TASK_DLOG("task no more retry since reach max exec times, execId:%d/%d", pTask->execId, pTask->maxExecTimes);
    return TSDB_CODE_SUCCESS;
  }

  if (!SCH_TASK_NEED_RETRY(pTask->lastMsgType, errCode)) {
    *needRetry = false;
    SCH_TASK_DLOG("task no more retry cause of errCode, errCode:%x - %s", errCode, tstrerror(errCode));
    return TSDB_CODE_SUCCESS;
  }

  /*
    if (SCH_IS_DATA_BIND_TASK(pTask)) {
      if ((pTask->execId + 1) >= SCH_TASK_NUM_OF_EPS(&pTask->plan->execNode)) {
        *needRetry = false;
        SCH_TASK_DLOG("task no more retry since all ep tried, execId:%d, epNum:%d", pTask->execId,
                      SCH_TASK_NUM_OF_EPS(&pTask->plan->execNode));
        return TSDB_CODE_SUCCESS;
      }
    } else {
      int32_t candidateNum = taosArrayGetSize(pTask->candidateAddrs);

      if ((pTask->candidateIdx + 1) >= candidateNum && (TSDB_CODE_SCH_TIMEOUT_ERROR != errCode)) {
        *needRetry = false;
        SCH_TASK_DLOG("task no more retry since all candiates tried, candidateIdx:%d, candidateNum:%d",
                      pTask->candidateIdx, candidateNum);
        return TSDB_CODE_SUCCESS;
      }
    }
  */

  *needRetry = true;
  SCH_TASK_DLOG("task need the %dth retry, errCode:%x - %s", pTask->execId + 1, errCode, tstrerror(errCode));

  return TSDB_CODE_SUCCESS;
}

int32_t schHandleTaskRetry(SSchJob *pJob, SSchTask *pTask) {
  atomic_sub_fetch_32(&pTask->level->taskLaunchedNum, 1);

  schRemoveTaskFromExecList(pJob, pTask);
  SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_INIT);

  if (SCH_TASK_NEED_FLOW_CTRL(pJob, pTask)) {
    SCH_ERR_RET(schLaunchTasksInFlowCtrlList(pJob, pTask));
  }

  schDeregisterTaskHb(pJob, pTask);

  if (SCH_IS_DATA_BIND_TASK(pTask)) {
    SQueryNodeAddr *addr = taosArrayGet(pTask->candidateAddrs, pTask->candidateIdx);
    SCH_SWITCH_EPSET(addr);
  } else {
    SCH_ERR_RET(schSwitchTaskCandidateAddr(pJob, pTask));
  }

  SCH_ERR_RET(schLaunchTask(pJob, pTask));

  return TSDB_CODE_SUCCESS;
}

int32_t schSetAddrsFromNodeList(SSchJob *pJob, SSchTask *pTask) {
  int32_t addNum = 0;
  int32_t nodeNum = 0;

  if (pJob->nodeList) {
    nodeNum = taosArrayGetSize(pJob->nodeList);

    for (int32_t i = 0; i < nodeNum; ++i) {
      SQueryNodeLoad *nload = taosArrayGet(pJob->nodeList, i);
      SQueryNodeAddr *naddr = &nload->addr;

      if (NULL == taosArrayPush(pTask->candidateAddrs, naddr)) {
        SCH_TASK_ELOG("taosArrayPush execNode to candidate addrs failed, addNum:%d, errno:%d", addNum, errno);
        SCH_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
      }

      SCH_TASK_TLOG("set %dth candidate addr, id %d, inUse:%d/%d, fqdn:%s, port:%d", i, naddr->nodeId,
                    naddr->epSet.inUse, naddr->epSet.numOfEps, SCH_GET_CUR_EP(naddr)->fqdn,
                    SCH_GET_CUR_EP(naddr)->port);

      ++addNum;
    }
  }

  if (addNum <= 0) {
    SCH_TASK_ELOG("no available execNode as candidates, nodeNum:%d", nodeNum);
    SCH_ERR_RET(TSDB_CODE_TSC_NO_EXEC_NODE);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schSetTaskCandidateAddrs(SSchJob *pJob, SSchTask *pTask) {
  if (NULL != pTask->candidateAddrs) {
    return TSDB_CODE_SUCCESS;
  }

  pTask->candidateAddrs = taosArrayInit(SCHEDULE_DEFAULT_MAX_NODE_NUM, sizeof(SQueryNodeAddr));
  if (NULL == pTask->candidateAddrs) {
    SCH_TASK_ELOG("taosArrayInit %d condidate addrs failed", SCHEDULE_DEFAULT_MAX_NODE_NUM);
    SCH_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  if (pTask->plan->execNode.epSet.numOfEps > 0) {
    if (NULL == taosArrayPush(pTask->candidateAddrs, &pTask->plan->execNode)) {
      SCH_TASK_ELOG("taosArrayPush execNode to candidate addrs failed, errno:%d", errno);
      SCH_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }

    SCH_TASK_DLOG("use execNode in plan as candidate addr, numOfEps:%d", pTask->plan->execNode.epSet.numOfEps);

    return TSDB_CODE_SUCCESS;
  }

  if (SCH_IS_DATA_BIND_TASK(pTask)) {
    SCH_TASK_ELOG("no execNode specifed for data src task, numOfEps:%d", pTask->plan->execNode.epSet.numOfEps);
    SCH_ERR_RET(TSDB_CODE_MND_INVALID_SCHEMA_VER);
  }

  SCH_ERR_RET(schSetAddrsFromNodeList(pJob, pTask));

  pTask->candidateIdx = taosRand() % taosArrayGetSize(pTask->candidateAddrs);

  /*
    for (int32_t i = 0; i < job->dataSrcEps.numOfEps && addNum < SCH_MAX_CANDIDATE_EP_NUM; ++i) {
      strncpy(epSet->fqdn[epSet->numOfEps], job->dataSrcEps.fqdn[i], sizeof(job->dataSrcEps.fqdn[i]));
      epSet->port[epSet->numOfEps] = job->dataSrcEps.port[i];

      ++epSet->numOfEps;
    }
  */

  return TSDB_CODE_SUCCESS;
}

int32_t schUpdateTaskCandidateAddr(SSchJob *pJob, SSchTask *pTask, SEpSet *pEpSet) {
  if (NULL == pTask->candidateAddrs || 1 != taosArrayGetSize(pTask->candidateAddrs)) {
    SCH_TASK_ELOG("not able to update cndidate addr, addr num %d",
                  (int32_t)(pTask->candidateAddrs ? taosArrayGetSize(pTask->candidateAddrs) : 0));
    SCH_ERR_RET(TSDB_CODE_APP_ERROR);
  }

  SQueryNodeAddr *pAddr = taosArrayGet(pTask->candidateAddrs, 0);

  char *origEpset = schDumpEpSet(&pAddr->epSet);
  char *newEpset = schDumpEpSet(pEpSet);

  SCH_TASK_DLOG("update task target node %d epset from %s to %s", pAddr->nodeId, origEpset, newEpset);

  taosMemoryFree(origEpset);
  taosMemoryFree(newEpset);

  memcpy(&pAddr->epSet, pEpSet, sizeof(pAddr->epSet));

  return TSDB_CODE_SUCCESS;
}

int32_t schSwitchTaskCandidateAddr(SSchJob *pJob, SSchTask *pTask) {
  int32_t candidateNum = taosArrayGetSize(pTask->candidateAddrs);
  if (candidateNum <= 1) {
    goto _return;
  }

  switch (schMgmt.cfg.schPolicy) {
    case SCH_LOAD_SEQ:
    case SCH_ALL:
    default:
      if (++pTask->candidateIdx >= candidateNum) {
        pTask->candidateIdx = 0;
      }
      break;
    case SCH_RANDOM: {
      int32_t lastIdx = pTask->candidateIdx;
      while (lastIdx == pTask->candidateIdx) {
        pTask->candidateIdx = taosRand() % candidateNum;
      }
      break;
    }
  }

_return:

  SCH_TASK_DLOG("switch task candiateIdx to %d/%d", pTask->candidateIdx, candidateNum);

  return TSDB_CODE_SUCCESS;
}

int32_t schRemoveTaskFromExecList(SSchJob *pJob, SSchTask *pTask) {
  int32_t code = taosHashRemove(pJob->execTasks, &pTask->taskId, sizeof(pTask->taskId));
  if (code) {
    SCH_TASK_WLOG("task already not in execTask list, code:%x", code);
  }

  return TSDB_CODE_SUCCESS;
}

void schDropTaskOnExecNode(SSchJob *pJob, SSchTask *pTask) {
  if (NULL == pTask->execNodes) {
    SCH_TASK_DLOG("no exec address, status:%s", SCH_GET_TASK_STATUS_STR(pTask));
    return;
  }

  int32_t size = (int32_t)taosHashGetSize(pTask->execNodes);

  if (size <= 0) {
    SCH_TASK_DLOG("task has no execNodes, no need to drop it, status:%s", SCH_GET_TASK_STATUS_STR(pTask));
    return;
  }

  int32_t       i = 0;
  SSchNodeInfo *nodeInfo = taosHashIterate(pTask->execNodes, NULL);
  while (nodeInfo) {
    if (nodeInfo->handle) {
      SCH_SET_TASK_HANDLE(pTask, nodeInfo->handle);
      void *pExecId = taosHashGetKey(nodeInfo, NULL);
      schBuildAndSendMsg(pJob, pTask, &nodeInfo->addr, TDMT_SCH_DROP_TASK, pExecId);
      SCH_TASK_DLOG("start to drop task's %dth execNode", i);
    } else {
      SCH_TASK_DLOG("no need to drop task %dth execNode", i);
    }

    ++i;
    nodeInfo = taosHashIterate(pTask->execNodes, nodeInfo);
  }

  SCH_TASK_DLOG("task has been dropped on %d exec nodes", size);
}

int32_t schNotifyTaskOnExecNode(SSchJob *pJob, SSchTask *pTask, ETaskNotifyType type) {
  int32_t size = (int32_t)taosHashGetSize(pTask->execNodes);
  if (size <= 0) {
    SCH_TASK_DLOG("task no exec address to notify, status:%s", SCH_GET_TASK_STATUS_STR(pTask));
    return TSDB_CODE_SUCCESS;
  }

  int32_t       i = 0;
  SSchNodeInfo *nodeInfo = taosHashIterate(pTask->execNodes, NULL);
  while (nodeInfo) {
    if (nodeInfo->handle) {
      SCH_SET_TASK_HANDLE(pTask, nodeInfo->handle);
      SCH_ERR_RET(schBuildAndSendMsg(pJob, pTask, &nodeInfo->addr, TDMT_SCH_TASK_NOTIFY, &type));
      SCH_TASK_DLOG("start to notify %d to task's %dth execNode", type, i);
    } else {
      SCH_TASK_DLOG("no need to notify %d to task %dth execNode", type, i);
    }

    ++i;
    nodeInfo = taosHashIterate(pTask->execNodes, nodeInfo);
  }

  SCH_TASK_DLOG("task has been notified %d on %d exec nodes", type, size);
  return TSDB_CODE_SUCCESS;
}

int32_t schProcessOnTaskStatusRsp(SQueryNodeEpId *pEpId, SArray *pStatusList) {
  int32_t   taskNum = (int32_t)taosArrayGetSize(pStatusList);
  SSchTask *pTask = NULL;
  SSchJob * pJob = NULL;

  qDebug("%d task status in hb rsp from nodeId:%d, fqdn:%s, port:%d", taskNum, pEpId->nodeId, pEpId->ep.fqdn,
         pEpId->ep.port);

  for (int32_t i = 0; i < taskNum; ++i) {
    STaskStatus *pStatus = taosArrayGet(pStatusList, i);
    int32_t      code = 0;

    qDebug("QID:0x%" PRIx64 ",TID:0x%" PRIx64 ",EID:%d task status in server: %s", pStatus->queryId, pStatus->taskId,
           pStatus->execId, jobTaskStatusStr(pStatus->status));

    if (schProcessOnCbBegin(&pJob, &pTask, pStatus->queryId, pStatus->refId, pStatus->taskId)) {
      continue;
    }

    if (pStatus->execId != pTask->execId) {
      // TODO
      SCH_TASK_DLOG("execId %d mis-match current execId %d", pStatus->execId, pTask->execId);
      schProcessOnCbEnd(pJob, pTask, 0);
      continue;
    }

    if (pStatus->status == JOB_TASK_STATUS_FAIL) {
      // RECORD AND HANDLE ERROR!!!!
      schProcessOnCbEnd(pJob, pTask, 0);
      continue;
    }

    if (pStatus->status == JOB_TASK_STATUS_INIT) {
      code = schRescheduleTask(pJob, pTask);
    }

    schProcessOnCbEnd(pJob, pTask, code);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schHandleExplainRes(SArray *pExplainRes) {
  int32_t code = 0;
  int32_t resNum = taosArrayGetSize(pExplainRes);
  if (resNum <= 0) {
    goto _return;
  }

  SSchTask *pTask = NULL;
  SSchJob * pJob = NULL;

  for (int32_t i = 0; i < resNum; ++i) {
    SExplainLocalRsp *localRsp = taosArrayGet(pExplainRes, i);

    qDebug("QID:0x%" PRIx64 ",TID:0x%" PRIx64 ", begin to handle LOCAL explain rsp msg", localRsp->qId, localRsp->tId);

    pJob = schAcquireJob(localRsp->rId);
    if (NULL == pJob) {
      qWarn("QID:0x%" PRIx64 ",TID:0x%" PRIx64 "job no exist, may be dropped, refId:0x%" PRIx64, localRsp->qId,
            localRsp->tId, localRsp->rId);
      SCH_ERR_JRET(TSDB_CODE_QRY_JOB_NOT_EXIST);
    }

    int8_t status = 0;
    if (schJobNeedToStop(pJob, &status)) {
      SCH_TASK_DLOG("will not do further processing cause of job status %s", jobTaskStatusStr(status));
      schReleaseJob(pJob->refId);
      SCH_ERR_JRET(TSDB_CODE_SCH_IGNORE_ERROR);
    }

    code = schGetTaskInJob(pJob, localRsp->tId, &pTask);

    if (TSDB_CODE_SUCCESS == code) {
      code = schProcessExplainRsp(pJob, pTask, &localRsp->rsp);
    }

    schReleaseJob(pJob->refId);

    qDebug("QID:0x%" PRIx64 ",TID:0x%" PRIx64 ", end to handle LOCAL explain rsp msg, code:%x", localRsp->qId,
           localRsp->tId, code);

    SCH_ERR_JRET(code);

    localRsp->rsp.numOfPlans = 0;
    localRsp->rsp.subplanInfo = NULL;
    pTask = NULL;
  }

_return:

  for (int32_t i = 0; i < resNum; ++i) {
    SExplainLocalRsp *localRsp = taosArrayGet(pExplainRes, i);
    tFreeSExplainRsp(&localRsp->rsp);
  }

  taosArrayDestroy(pExplainRes);

  SCH_RET(code);
}

int32_t schLaunchRemoteTask(SSchJob *pJob, SSchTask *pTask) {
  SSubplan *plan = pTask->plan;
  int32_t   code = 0;

  if (NULL == pTask->msg) {  // TODO add more detailed reason for failure
    code = qSubPlanToMsg(plan, &pTask->msg, &pTask->msgLen);
    if (TSDB_CODE_SUCCESS != code) {
      SCH_TASK_ELOG("failed to create physical plan, code:%s, msg:%p, len:%d", tstrerror(code), pTask->msg,
                    pTask->msgLen);
      SCH_ERR_RET(code);
    } else if (tsQueryPlannerTrace) {
      char *  msg = NULL;
      int32_t msgLen = 0;
      SCH_ERR_RET(qSubPlanToString(plan, &msg, &msgLen));
      SCH_TASK_DLOGL("physical plan len:%d, %s", msgLen, msg);
      taosMemoryFree(msg);
    }
  }

  SCH_ERR_RET(schSetTaskCandidateAddrs(pJob, pTask));

  if (SCH_IS_QUERY_JOB(pJob)) {
    SCH_ERR_RET(schEnsureHbConnection(pJob, pTask));
  }

  SCH_RET(schBuildAndSendMsg(pJob, pTask, NULL, plan->msgType, NULL));
}

int32_t schLaunchLocalTask(SSchJob *pJob, SSchTask *pTask) {
  // SCH_ERR_JRET(schSetTaskCandidateAddrs(pJob, pTask));
  if (NULL == schMgmt.queryMgmt) {
    SCH_ERR_RET(qWorkerInit(NODE_TYPE_CLIENT, CLIENT_HANDLE, (void **)&schMgmt.queryMgmt, NULL));
  }

  SArray *explainRes = NULL;
  int32_t code = 0;
  SQWMsg  qwMsg = {0};
  qwMsg.msgInfo.taskType = TASK_TYPE_TEMP;
  qwMsg.msgInfo.explain = SCH_IS_EXPLAIN_JOB(pJob);
  qwMsg.msgInfo.needFetch = SCH_TASK_NEED_FETCH(pTask);
  qwMsg.msg = pTask->plan;
  qwMsg.msgType = pTask->plan->msgType;
  qwMsg.connInfo.handle = pJob->conn.pTrans;

  if (SCH_IS_EXPLAIN_JOB(pJob)) {
    explainRes = taosArrayInit(pJob->taskNum, sizeof(SExplainLocalRsp));
  }

  SCH_ERR_JRET(qWorkerProcessLocalQuery(schMgmt.queryMgmt, schMgmt.sId, pJob->queryId, pTask->taskId, pJob->refId,
                                        pTask->execId, &qwMsg, explainRes));

  if (SCH_IS_EXPLAIN_JOB(pJob)) {
    SCH_ERR_RET(schHandleExplainRes(explainRes));
    explainRes = NULL;
  }

  SCH_ERR_RET(schProcessOnTaskSuccess(pJob, pTask));

_return:

  taosArrayDestroy(explainRes);

  SCH_RET(code);
}

int32_t schLaunchTaskImpl(void *param) {
  SSchTaskCtx *pCtx = (SSchTaskCtx *)param;
  SSchJob *    pJob = schAcquireJob(pCtx->jobRid);
  if (NULL == pJob) {
    qDebug("job refId 0x%" PRIx64 " already not exist", pCtx->jobRid);
    taosMemoryFree(param);
    SCH_RET(TSDB_CODE_SCH_JOB_IS_DROPPING);
  }

  SSchTask *pTask = pCtx->pTask;

  if (pCtx->asyncLaunch) {
    SCH_LOCK_TASK(pTask);
  }

  int8_t  status = 0;
  int32_t code = 0;

  atomic_add_fetch_32(&pTask->level->taskLaunchedNum, 1);
  pTask->execId++;
  pTask->retryTimes++;
  pTask->waitRetry = false;

  SCH_TASK_DLOG("start to launch %s task, execId %d, retry %d",
                SCH_IS_LOCAL_EXEC_TASK(pJob, pTask) ? "LOCAL" : "REMOTE", pTask->execId, pTask->retryTimes);

  SCH_LOG_TASK_START_TS(pTask);

  if (schJobNeedToStop(pJob, &status)) {
    SCH_TASK_DLOG("no need to launch task cause of job status %s", jobTaskStatusStr(status));
    SCH_ERR_JRET(TSDB_CODE_SCH_IGNORE_ERROR);
  }

  if (SCH_GET_TASK_STATUS(pTask) != JOB_TASK_STATUS_EXEC) {
    SCH_ERR_JRET(schPushTaskToExecList(pJob, pTask));
    SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_EXEC);
  }

  if (SCH_IS_LOCAL_EXEC_TASK(pJob, pTask)) {
    SCH_ERR_JRET(schLaunchLocalTask(pJob, pTask));
  } else {
    SCH_ERR_JRET(schLaunchRemoteTask(pJob, pTask));
  }

#if 0
  if (SCH_IS_QUERY_JOB(pJob)) {
    SCH_ERR_JRET(schEnsureHbConnection(pJob, pTask));
  }
#endif

_return:

  if (pJob->taskNum >= SCH_MIN_AYSNC_EXEC_NUM) {
    if (code) {
      code = schProcessOnTaskFailure(pJob, pTask, code);
    }
    if (code) {
      code = schHandleJobFailure(pJob, code);
    }
  }

  if (pCtx->asyncLaunch) {
    SCH_UNLOCK_TASK(pTask);
  }

  schReleaseJob(pJob->refId);

  taosMemoryFree(param);

  SCH_RET(code);
}

int32_t schAsyncLaunchTaskImpl(SSchJob *pJob, SSchTask *pTask) {
  SSchTaskCtx *param = taosMemoryCalloc(1, sizeof(SSchTaskCtx));
  if (NULL == param) {
    SCH_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  param->jobRid = pJob->refId;
  param->pTask = pTask;

  if (pJob->taskNum >= SCH_MIN_AYSNC_EXEC_NUM) {
    param->asyncLaunch = true;
    taosAsyncExec(schLaunchTaskImpl, param, NULL);
  } else {
    SCH_ERR_RET(schLaunchTaskImpl(param));
  }

  return TSDB_CODE_SUCCESS;
}

// Note: no more error processing, handled in function internal
int32_t schLaunchTask(SSchJob *pJob, SSchTask *pTask) {
  bool    enough = false;
  int32_t code = 0;

  SCH_SET_TASK_HANDLE(pTask, NULL);

  if (SCH_TASK_NEED_FLOW_CTRL(pJob, pTask)) {
    SCH_ERR_JRET(schCheckIncTaskFlowQuota(pJob, pTask, &enough));

    if (enough) {
      SCH_ERR_JRET(schAsyncLaunchTaskImpl(pJob, pTask));
    }
  } else {
    SCH_ERR_JRET(schAsyncLaunchTaskImpl(pJob, pTask));
  }

  return TSDB_CODE_SUCCESS;

_return:

  SCH_RET(schProcessOnTaskFailure(pJob, pTask, code));
}

void schHandleTimerEvent(void *param, void *tmrId) {
  SSchTimerParam *pTimerParam = (SSchTimerParam *)param;
  SSchTask *      pTask = NULL;
  SSchJob *       pJob = NULL;
  int32_t         code = 0;

  int64_t  rId = pTimerParam->rId;
  uint64_t queryId = pTimerParam->queryId;
  uint64_t taskId = pTimerParam->taskId;
  taosMemoryFree(pTimerParam);

  if (schProcessOnCbBegin(&pJob, &pTask, queryId, rId, taskId)) {
    return;
  }

  code = schLaunchTask(pJob, pTask);

  schProcessOnCbEnd(pJob, pTask, code);
}

int32_t schDelayLaunchTask(SSchJob *pJob, SSchTask *pTask) {
  if (pTask->delayExecMs > 0) {
    SSchTimerParam *param = taosMemoryMalloc(sizeof(SSchTimerParam));
    if (NULL == param) {
      SCH_TASK_ELOG("taosMemoryMalloc %d failed", (int)sizeof(SSchTimerParam));
      SCH_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }

    param->rId = pJob->refId;
    param->queryId = pJob->queryId;
    param->taskId = pTask->taskId;

    if (NULL == pTask->delayTimer) {
      pTask->delayTimer = taosTmrStart(schHandleTimerEvent, pTask->delayExecMs, (void *)param, schMgmt.timer);
      if (NULL == pTask->delayTimer) {
        SCH_TASK_ELOG("start delay timer failed, handle:%p", schMgmt.timer);
        SCH_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
      }

      return TSDB_CODE_SUCCESS;
    }

    taosTmrReset(schHandleTimerEvent, pTask->delayExecMs, (void *)param, schMgmt.timer, &pTask->delayTimer);

    return TSDB_CODE_SUCCESS;
  }

  SCH_RET(schLaunchTask(pJob, pTask));
}

int32_t schLaunchLevelTasks(SSchJob *pJob, SSchLevel *level) {
  SCH_ERR_RET(schChkJobNeedFlowCtrl(pJob, level));

  for (int32_t i = 0; i < level->taskNum; ++i) {
    SSchTask *pTask = taosArrayGet(level->subTasks, i);

    SCH_ERR_RET(schLaunchTask(pJob, pTask));
  }

  return TSDB_CODE_SUCCESS;
}

void schDropTaskInHashList(SSchJob *pJob, SHashObj *list) {
  if (!SCH_JOB_NEED_DROP(pJob)) {
    return;
  }

  void *pIter = taosHashIterate(list, NULL);
  while (pIter) {
    SSchTask *pTask = *(SSchTask **)pIter;

    SCH_LOCK_TASK(pTask);
    if (pTask->delayTimer) {
      taosTmrStopA(&pTask->delayTimer);
    }
    schDropTaskOnExecNode(pJob, pTask);
    SCH_UNLOCK_TASK(pTask);

    pIter = taosHashIterate(list, pIter);
  }
}

int32_t schNotifyTaskInHashList(SSchJob *pJob, SHashObj *list, ETaskNotifyType type, SSchTask *pCurrTask) {
  int32_t code = TSDB_CODE_SUCCESS;

  SCH_ERR_RET(schNotifyTaskOnExecNode(pJob, pCurrTask, type));

  void *pIter = taosHashIterate(list, NULL);
  while (pIter) {
    SSchTask *pTask = *(SSchTask **)pIter;
    if (pTask != pCurrTask) {
      SCH_LOCK_TASK(pTask);
      code = schNotifyTaskOnExecNode(pJob, pTask, type);
      SCH_UNLOCK_TASK(pTask);

      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }

    pIter = taosHashIterate(list, pIter);
  }

  SCH_RET(code);
}

int32_t schExecRemoteFetch(SSchJob *pJob, SSchTask *pTask) {
  SCH_RET(schBuildAndSendMsg(pJob, pJob->fetchTask, &pJob->resNode, SCH_FETCH_TYPE(pJob->fetchTask), NULL));
}

int32_t schExecLocalFetch(SSchJob *pJob, SSchTask *pTask) {
  void *  pRsp = NULL;
  int32_t code = 0;
  SArray *explainRes = NULL;

  if (SCH_IS_EXPLAIN_JOB(pJob)) {
    explainRes = taosArrayInit(pJob->taskNum, sizeof(SExplainLocalRsp));
  }

  SCH_ERR_JRET(qWorkerProcessLocalFetch(schMgmt.queryMgmt, schMgmt.sId, pJob->queryId, pTask->taskId, pJob->refId,
                                        pTask->execId, &pRsp, explainRes));

  if (SCH_IS_EXPLAIN_JOB(pJob)) {
    SCH_ERR_RET(schHandleExplainRes(explainRes));
    explainRes = NULL;
  }

  SCH_ERR_RET(schProcessFetchRsp(pJob, pTask, pRsp, TSDB_CODE_SUCCESS));

_return:

  taosArrayDestroy(explainRes);

  SCH_RET(code);
}

// Note: no more error processing, handled in function internal
int32_t schLaunchFetchTask(SSchJob *pJob) {
  int32_t code = 0;

  void *fetchRes = atomic_load_ptr(&pJob->fetchRes);
  if (fetchRes) {
    SCH_JOB_DLOG("res already fetched, res:%p", fetchRes);
    return TSDB_CODE_SUCCESS;
  }

  SCH_SET_TASK_STATUS(pJob->fetchTask, JOB_TASK_STATUS_FETCH);

  if (SCH_IS_LOCAL_EXEC_TASK(pJob, pJob->fetchTask)) {
    SCH_ERR_JRET(schExecLocalFetch(pJob, pJob->fetchTask));
  } else {
    SCH_ERR_JRET(schExecRemoteFetch(pJob, pJob->fetchTask));
  }

  return TSDB_CODE_SUCCESS;

_return:

  SCH_RET(schProcessOnTaskFailure(pJob, pJob->fetchTask, code));
}
