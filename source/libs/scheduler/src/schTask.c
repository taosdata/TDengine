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
#include "schedulerInt.h"
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
}


int32_t schInitTask(SSchJob *pJob, SSchTask *pTask, SSubplan *pPlan, SSchLevel *pLevel) {
  pTask->plan = pPlan;
  pTask->level = pLevel;
  pTask->execId = -1;
  pTask->maxExecTimes = SCH_TASK_MAX_EXEC_TIMES;
  pTask->timeoutUsec = SCH_DEFAULT_TASK_TIMEOUT_USEC;
  SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_INIT);
  pTask->taskId = schGenTaskId();
  pTask->execNodes = taosHashInit(SCH_MAX_CANDIDATE_EP_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (NULL == pTask->execNodes) {
    SCH_TASK_ELOG("taosHashInit %d execNodes failed", SCH_MAX_CANDIDATE_EP_NUM);
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schRecordTaskSucceedNode(SSchJob *pJob, SSchTask *pTask) {
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
  SSchNodeInfo nodeInfo = {.addr = *addr, .handle = NULL};

  if (taosHashPut(pTask->execNodes, &execId, sizeof(execId), &nodeInfo, sizeof(nodeInfo))) {
    SCH_TASK_ELOG("taosHashPut nodeInfo to execNodes failed, errno:%d", errno);
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SCH_TASK_DLOG("task execNode added, execId:%d", execId);

  return TSDB_CODE_SUCCESS;
}

int32_t schDropTaskExecNode(SSchJob *pJob, SSchTask *pTask, void *handle, int32_t execId) {
  if (NULL == pTask->execNodes) {
    return TSDB_CODE_SUCCESS;
  }

  if (taosHashRemove(pTask->execNodes, &execId, sizeof(execId))) {
    SCH_TASK_ELOG("fail to remove execId %d from execNodeList", execId);
  } else {
    SCH_TASK_DLOG("execId %d removed from execNodeList", execId);
  }
  
  if (execId != pTask->execId) {     // ignore it
    SCH_TASK_DLOG("execId %d is not current execId %d", execId, pTask->execId);
    SCH_RET(TSDB_CODE_SCH_IGNORE_ERROR);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schUpdateTaskExecNode(SSchJob *pJob, SSchTask *pTask, void *handle, int32_t execId) {
  if (taosHashGetSize(pTask->execNodes) <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SSchNodeInfo *nodeInfo = taosHashGet(pTask->execNodes, &execId, sizeof(execId));
  nodeInfo->handle = handle;

  SCH_TASK_DLOG("handle updated to %p for execId %d", handle, execId);

  return TSDB_CODE_SUCCESS;
}

int32_t schUpdateTaskHandle(SSchJob *pJob, SSchTask *pTask, bool dropExecNode, void *handle, int32_t execId) {
  if (dropExecNode) {
    SCH_RET(schDropTaskExecNode(pJob, pTask, handle, execId));
  }

  SCH_SET_TASK_HANDLE(pTask, handle);

  schUpdateTaskExecNode(pJob, pTask, handle, execId);

  return TSDB_CODE_SUCCESS;
}

// Note: no more task error processing, handled in function internal
int32_t schProcessOnTaskFailure(SSchJob *pJob, SSchTask *pTask, int32_t errCode) {
  int8_t status = 0;

  if (errCode == TSDB_CODE_SCH_TIMEOUT_ERROR) {
    SCH_LOG_TASK_WAIT_TS(pTask);
  } else {
    SCH_LOG_TASK_END_TS(pTask);
  }
  
  if (schJobNeedToStop(pJob, &status)) {
    SCH_TASK_DLOG("task failed not processed cause of job status, job status:%s", jobTaskStatusStr(status));
    SCH_RET(atomic_load_32(&pJob->errCode));
  }

  bool    needRetry = false;
  bool    moved = false;
  int32_t taskDone = 0;
  int32_t code = 0;

  SCH_TASK_DLOG("taskOnFailure, code:%s", tstrerror(errCode));

  SCH_ERR_JRET(schTaskCheckSetRetry(pJob, pTask, errCode, &needRetry));

  if (!needRetry) {
    SCH_TASK_ELOG("task failed and no more retry, code:%s", tstrerror(errCode));

    if (SCH_GET_TASK_STATUS(pTask) != JOB_TASK_STATUS_EXEC) {
      SCH_TASK_ELOG("task not in executing list, status:%s", SCH_GET_TASK_STATUS_STR(pTask));
      SCH_ERR_JRET(TSDB_CODE_SCH_STATUS_ERROR);
    }

    SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_FAIL);

    if (SCH_IS_WAIT_ALL_JOB(pJob)) {
      SCH_LOCK(SCH_WRITE, &pTask->level->lock);
      pTask->level->taskFailed++;
      taskDone = pTask->level->taskSucceed + pTask->level->taskFailed;
      SCH_UNLOCK(SCH_WRITE, &pTask->level->lock);

      schUpdateJobErrCode(pJob, errCode);

      if (taskDone < pTask->level->taskNum) {
        SCH_TASK_DLOG("need to wait other tasks, doneNum:%d, allNum:%d", taskDone, pTask->level->taskNum);
        SCH_RET(errCode);
      }
    }
  } else {
    SCH_ERR_JRET(schHandleTaskRetry(pJob, pTask));

    return TSDB_CODE_SUCCESS;
  }

_return:

  SCH_RET(schProcessOnJobFailure(pJob, errCode));
}



// Note: no more task error processing, handled in function internal
int32_t schProcessOnTaskSuccess(SSchJob *pJob, SSchTask *pTask) {
  bool    moved = false;
  int32_t code = 0;

  SCH_TASK_DLOG("taskOnSuccess, status:%s", SCH_GET_TASK_STATUS_STR(pTask));

  SCH_LOG_TASK_END_TS(pTask);

  SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_PART_SUCC);

  SCH_ERR_JRET(schRecordTaskSucceedNode(pJob, pTask));

  SCH_ERR_JRET(schLaunchTasksInFlowCtrlList(pJob, pTask));

  int32_t parentNum = pTask->parents ? (int32_t)taosArrayGetSize(pTask->parents) : 0;
  if (parentNum == 0) {
    int32_t taskDone = 0;
    if (SCH_IS_WAIT_ALL_JOB(pJob)) {
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
        SCH_RET(schProcessOnJobFailure(pJob, 0));
      } else {
        SCH_RET(schProcessOnJobPartialSuccess(pJob));
      }
    } else {
      pJob->resNode = pTask->succeedAddr;
    }

    pJob->fetchTask = pTask;

    SCH_RET(schProcessOnJobPartialSuccess(pJob));
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
    int32_t   readyNum = atomic_add_fetch_32(&parent->childReady, 1);

    SCH_LOCK(SCH_WRITE, &parent->lock);
    SDownstreamSourceNode source = {.type = QUERY_NODE_DOWNSTREAM_SOURCE,
                                    .taskId = pTask->taskId,
                                    .schedId = schMgmt.sId,
                                    .execId = pTask->execId,
                                    .addr = pTask->succeedAddr};
    qSetSubplanExecutionNode(parent->plan, pTask->plan->id.groupId, &source);
    SCH_UNLOCK(SCH_WRITE, &parent->lock);

    if (SCH_TASK_READY_FOR_LAUNCH(readyNum, parent)) {
      SCH_TASK_DLOG("all %d children task done, start to launch parent task 0x%" PRIx64, readyNum, parent->taskId);
      SCH_ERR_RET(schLaunchTask(pJob, parent));
    }
  }

  SCH_ERR_RET(schLaunchJobLowerLevel(pJob, pTask));

  return TSDB_CODE_SUCCESS;

_return:

  SCH_RET(schProcessOnJobFailure(pJob, code));
}

int32_t schRescheduleTask(SSchJob *pJob, SSchTask *pTask) {
  if (SCH_IS_DATA_SRC_QRY_TASK(pTask)) {
    return TSDB_CODE_SUCCESS;
  }

  SCH_LOCK_TASK(pTask);
  if (SCH_TASK_TIMEOUT(pTask) && JOB_TASK_STATUS_EXEC == pTask->status && 
      pJob->fetchTask != pTask && taosArrayGetSize(pTask->candidateAddrs) > 1) {
    SCH_TASK_DLOG("task execId %d will be rescheduled now", pTask->execId);
    schDropTaskOnExecNode(pJob, pTask);
    taosHashClear(pTask->execNodes);
    schProcessOnTaskFailure(pJob, pTask, TSDB_CODE_SCH_TIMEOUT_ERROR);
  }
  SCH_UNLOCK_TASK(pTask);

  return TSDB_CODE_SUCCESS;
}

int32_t schDoTaskRedirect(SSchJob *pJob, SSchTask *pTask, SDataBuf* pData, int32_t rspCode) {
  int32_t code = 0;
  
  if ((pTask->execId + 1) >= pTask->maxExecTimes) {
    SCH_TASK_DLOG("task no more retry since reach max try times, execId:%d", pTask->execId);
    schProcessOnJobFailure(pJob, rspCode);
    return TSDB_CODE_SUCCESS;
  }

  SCH_TASK_DLOG("task will be redirected now, status:%s", SCH_GET_TASK_STATUS_STR(pTask));
  
  schDropTaskOnExecNode(pJob, pTask);
  taosHashClear(pTask->execNodes);
  SCH_ERR_JRET(schRemoveTaskFromExecList(pJob, pTask));
  schDeregisterTaskHb(pJob, pTask);
  atomic_sub_fetch_32(&pTask->level->taskLaunchedNum, 1);
  taosMemoryFreeClear(pTask->msg);
  pTask->msgLen = 0;
  pTask->lastMsgType = 0;
  memset(&pTask->succeedAddr, 0, sizeof(pTask->succeedAddr));

  if (SCH_IS_DATA_SRC_QRY_TASK(pTask)) {
    if (pData) {
      SCH_ERR_JRET(schUpdateTaskCandidateAddr(pJob, pTask, pData->pEpSet));
    }
  
    if (SCH_TASK_NEED_FLOW_CTRL(pJob, pTask)) {
      if (JOB_TASK_STATUS_EXEC == SCH_GET_TASK_STATUS(pTask)) {
        SCH_ERR_JRET(schLaunchTasksInFlowCtrlList(pJob, pTask));
      }
    }    

    SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_INIT);
    
    SCH_ERR_JRET(schLaunchTask(pJob, pTask));

    return TSDB_CODE_SUCCESS;
  }


  // merge plan
  
  pTask->childReady = 0;
  
  qClearSubplanExecutionNode(pTask->plan);

  SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_INIT);
  
  int32_t childrenNum = taosArrayGetSize(pTask->children);
  for (int32_t i = 0; i < childrenNum; ++i) {
    SSchTask* pChild = taosArrayGetP(pTask->children, i);
    SCH_LOCK_TASK(pChild);
    schDoTaskRedirect(pJob, pChild, NULL, rspCode);
    SCH_UNLOCK_TASK(pChild);
  }

  return TSDB_CODE_SUCCESS;

_return:

  code = schProcessOnTaskFailure(pJob, pTask, code);

  SCH_RET(code);  
}

int32_t schHandleRedirect(SSchJob *pJob, SSchTask *pTask, SDataBuf* pData, int32_t rspCode) {
  int32_t code = 0;

  if (SCH_IS_DATA_SRC_QRY_TASK(pTask)) {
    if (NULL == pData->pEpSet) {
      SCH_TASK_ELOG("no epset updated while got error %s", tstrerror(rspCode));
      SCH_ERR_JRET(rspCode);
    }
  }

  SCH_RET(schDoTaskRedirect(pJob, pTask, pData, rspCode));

_return:

  schProcessOnTaskFailure(pJob, pTask, code);
  
  SCH_RET(code);
}

int32_t schPushTaskToExecList(SSchJob *pJob, SSchTask *pTask) {
  int32_t code = taosHashPut(pJob->execTasks, &pTask->taskId, sizeof(pTask->taskId), &pTask, POINTER_BYTES);
  if (0 != code) {
    if (HASH_NODE_EXIST(code)) {
      SCH_TASK_ELOG("task already in execTask list, code:%x", code);
      SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
    }

    SCH_TASK_ELOG("taosHashPut task to execTask list failed, errno:%d", errno);
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
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
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
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
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
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
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  *moved = true;

  SCH_TASK_DLOG("task moved to execTask list, numOfTasks:%d", taosHashGetSize(pJob->execTasks));

  return TSDB_CODE_SUCCESS;
}
*/

int32_t schTaskCheckSetRetry(SSchJob *pJob, SSchTask *pTask, int32_t errCode, bool *needRetry) {
  if (TSDB_CODE_SCH_TIMEOUT_ERROR == errCode) {
    pTask->maxExecTimes++;
    if (pTask->timeoutUsec < SCH_MAX_TASK_TIMEOUT_USEC) {
      pTask->timeoutUsec *= 2;
      if (pTask->timeoutUsec > SCH_MAX_TASK_TIMEOUT_USEC) {
        pTask->timeoutUsec = SCH_MAX_TASK_TIMEOUT_USEC;
      }
    }
  }

  if ((pTask->execId + 1) >= pTask->maxExecTimes) {
    *needRetry = false;
    SCH_TASK_DLOG("task no more retry since reach max try times, execId:%d", pTask->execId);
    return TSDB_CODE_SUCCESS;
  }

  if (!SCH_NEED_RETRY(pTask->lastMsgType, errCode)) {
    *needRetry = false;
    SCH_TASK_DLOG("task no more retry cause of errCode, errCode:%x - %s", errCode, tstrerror(errCode));
    return TSDB_CODE_SUCCESS;
  }

  if (SCH_IS_DATA_SRC_TASK(pTask)) {
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

  *needRetry = true;
  SCH_TASK_DLOG("task need the %dth retry, errCode:%x - %s", pTask->execId + 1, errCode, tstrerror(errCode));

  return TSDB_CODE_SUCCESS;
}

int32_t schHandleTaskRetry(SSchJob *pJob, SSchTask *pTask) {
  atomic_sub_fetch_32(&pTask->level->taskLaunchedNum, 1);

  SCH_ERR_RET(schRemoveTaskFromExecList(pJob, pTask));
  SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_INIT);
  
  if (SCH_TASK_NEED_FLOW_CTRL(pJob, pTask)) {
    SCH_ERR_RET(schLaunchTasksInFlowCtrlList(pJob, pTask));
  }

  schDeregisterTaskHb(pJob, pTask);

  if (SCH_IS_DATA_SRC_TASK(pTask)) {
    SCH_SWITCH_EPSET(&pTask->plan->execNode);
  } else {
    int32_t candidateNum = taosArrayGetSize(pTask->candidateAddrs);  
    if (++pTask->candidateIdx >= candidateNum) {
      pTask->candidateIdx = 0;
    }
  }

  SCH_ERR_RET(schLaunchTask(pJob, pTask));

  return TSDB_CODE_SUCCESS;
}

int32_t schSetAddrsFromNodeList(SSchJob *pJob, SSchTask *pTask) {
  int32_t addNum = 0;
  int32_t nodeNum = 0;
  
  if (pJob->nodeList) {
    nodeNum = taosArrayGetSize(pJob->nodeList);

    for (int32_t i = 0; i < nodeNum && addNum < SCH_MAX_CANDIDATE_EP_NUM; ++i) {
      SQueryNodeLoad *nload = taosArrayGet(pJob->nodeList, i);
      SQueryNodeAddr *naddr = &nload->addr;
      
      if (NULL == taosArrayPush(pTask->candidateAddrs, naddr)) {
        SCH_TASK_ELOG("taosArrayPush execNode to candidate addrs failed, addNum:%d, errno:%d", addNum, errno);
        SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      SCH_TASK_DLOG("set %dth candidate addr, id %d, fqdn:%s, port:%d", i, naddr->nodeId, SCH_GET_CUR_EP(naddr)->fqdn, SCH_GET_CUR_EP(naddr)->port);

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

  pTask->candidateIdx = 0;
  pTask->candidateAddrs = taosArrayInit(SCH_MAX_CANDIDATE_EP_NUM, sizeof(SQueryNodeAddr));
  if (NULL == pTask->candidateAddrs) {
    SCH_TASK_ELOG("taosArrayInit %d condidate addrs failed", SCH_MAX_CANDIDATE_EP_NUM);
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  if (pTask->plan->execNode.epSet.numOfEps > 0) {
    if (NULL == taosArrayPush(pTask->candidateAddrs, &pTask->plan->execNode)) {
      SCH_TASK_ELOG("taosArrayPush execNode to candidate addrs failed, errno:%d", errno);
      SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    SCH_TASK_DLOG("use execNode in plan as candidate addr, numOfEps:%d", pTask->plan->execNode.epSet.numOfEps);

    return TSDB_CODE_SUCCESS;
  }

  SCH_ERR_RET(schSetAddrsFromNodeList(pJob, pTask));

  /*
    for (int32_t i = 0; i < job->dataSrcEps.numOfEps && addNum < SCH_MAX_CANDIDATE_EP_NUM; ++i) {
      strncpy(epSet->fqdn[epSet->numOfEps], job->dataSrcEps.fqdn[i], sizeof(job->dataSrcEps.fqdn[i]));
      epSet->port[epSet->numOfEps] = job->dataSrcEps.port[i];

      ++epSet->numOfEps;
    }
  */

  return TSDB_CODE_SUCCESS;
}

int32_t schUpdateTaskCandidateAddr(SSchJob *pJob, SSchTask *pTask, SEpSet* pEpSet) {
  if (NULL == pTask->candidateAddrs || 1 != taosArrayGetSize(pTask->candidateAddrs)) {
    SCH_TASK_ELOG("not able to update cndidate addr, addr num %d", (int32_t)(pTask->candidateAddrs ? taosArrayGetSize(pTask->candidateAddrs): 0));
    SCH_ERR_RET(TSDB_CODE_APP_ERROR);
  }

  SQueryNodeAddr* pAddr = taosArrayGet(pTask->candidateAddrs, 0);

  SEp* pOld = &pAddr->epSet.eps[pAddr->epSet.inUse];
  SEp* pNew = &pEpSet->eps[pEpSet->inUse];

  SCH_TASK_DLOG("update task ep from %s:%d to %s:%d", pOld->fqdn, pOld->port, pNew->fqdn, pNew->port);

  memcpy(&pAddr->epSet, pEpSet, sizeof(pAddr->epSet));

  return TSDB_CODE_SUCCESS;
}


int32_t schRemoveTaskFromExecList(SSchJob *pJob, SSchTask *pTask) {
  int32_t code = taosHashRemove(pJob->execTasks, &pTask->taskId, sizeof(pTask->taskId));
  if (code) {
    SCH_TASK_ELOG("task failed to rm from execTask list, code:%x", code);
    SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
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

  SSchNodeInfo *nodeInfo = taosHashIterate(pTask->execNodes, NULL);
  while (nodeInfo) {
    SCH_SET_TASK_HANDLE(pTask, nodeInfo->handle);

    schBuildAndSendMsg(pJob, pTask, &nodeInfo->addr, TDMT_SCH_DROP_TASK);

    nodeInfo = taosHashIterate(pTask->execNodes, nodeInfo);
  }

  SCH_TASK_DLOG("task has been dropped on %d exec nodes", size);
}



int32_t schProcessOnTaskStatusRsp(SQueryNodeEpId* pEpId, SArray* pStatusList) {
  int32_t taskNum = (int32_t)taosArrayGetSize(pStatusList);
  SSchTask *pTask = NULL;

  qDebug("%d task status in hb rsp from nodeId:%d, fqdn:%s, port:%d", taskNum, pEpId->nodeId, pEpId->ep.fqdn, pEpId->ep.port);

  for (int32_t i = 0; i < taskNum; ++i) {
    STaskStatus *taskStatus = taosArrayGet(pStatusList, i);

    qDebug("QID:0x%" PRIx64 ",TID:0x%" PRIx64 ",EID:%d task status in server: %s", 
      taskStatus->queryId, taskStatus->taskId, taskStatus->execId, jobTaskStatusStr(taskStatus->status));

    SSchJob *pJob = schAcquireJob(taskStatus->refId);
    if (NULL == pJob) {
      qWarn("job not found, refId:0x%" PRIx64 ",QID:0x%" PRIx64 ",TID:0x%" PRIx64, taskStatus->refId,
            taskStatus->queryId, taskStatus->taskId);
      // TODO DROP TASK FROM SERVER!!!!
      continue;
    }

    pTask = NULL;
    schGetTaskInJob(pJob, taskStatus->taskId, &pTask);
    if (NULL == pTask) {
      // TODO DROP TASK FROM SERVER!!!!
      schReleaseJob(taskStatus->refId);
      continue;
    }

    if (taskStatus->execId != pTask->execId) {
      // TODO DROP TASK FROM SERVER!!!!
      SCH_TASK_DLOG("EID %d in hb rsp mis-match", taskStatus->execId);
      schReleaseJob(taskStatus->refId);
      continue;      
    }
    
    if (taskStatus->status == JOB_TASK_STATUS_FAIL) {
      // RECORD AND HANDLE ERROR!!!!
      schReleaseJob(taskStatus->refId);
      continue;
    }

    if (taskStatus->status == JOB_TASK_STATUS_INIT) {
      schRescheduleTask(pJob, pTask);
    }

    schReleaseJob(taskStatus->refId);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schLaunchTaskImpl(SSchJob *pJob, SSchTask *pTask) {
  int8_t  status = 0;
  int32_t code = 0;

  atomic_add_fetch_32(&pTask->level->taskLaunchedNum, 1);
  pTask->execId++;

  SCH_TASK_DLOG("start to launch task's %dth exec", pTask->execId);

  SCH_LOG_TASK_START_TS(pTask);

  if (schJobNeedToStop(pJob, &status)) {
    SCH_TASK_DLOG("no need to launch task cause of job status, job status:%s", jobTaskStatusStr(status));

    SCH_RET(atomic_load_32(&pJob->errCode));
  }

  // NOTE: race condition: the task should be put into the hash table before send msg to server
  if (SCH_GET_TASK_STATUS(pTask) != JOB_TASK_STATUS_EXEC) {
    SCH_ERR_RET(schPushTaskToExecList(pJob, pTask));
    SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_EXEC);
  }

  SSubplan *plan = pTask->plan;

  if (NULL == pTask->msg) {  // TODO add more detailed reason for failure
    code = qSubPlanToString(plan, &pTask->msg, &pTask->msgLen);
    if (TSDB_CODE_SUCCESS != code) {
      SCH_TASK_ELOG("failed to create physical plan, code:%s, msg:%p, len:%d", tstrerror(code), pTask->msg,
                    pTask->msgLen);
      SCH_ERR_RET(code);
    } else {
      SCH_TASK_DLOGL("physical plan len:%d, %s", pTask->msgLen, pTask->msg);
    }
  }

  SCH_ERR_RET(schSetTaskCandidateAddrs(pJob, pTask));

  if (SCH_IS_QUERY_JOB(pJob)) {
    SCH_ERR_RET(schEnsureHbConnection(pJob, pTask));
  }

  SCH_ERR_RET(schBuildAndSendMsg(pJob, pTask, NULL, plan->msgType));

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
      SCH_ERR_JRET(schLaunchTaskImpl(pJob, pTask));
    }
  } else {
    SCH_ERR_JRET(schLaunchTaskImpl(pJob, pTask));
  }

  return TSDB_CODE_SUCCESS;

_return:

  SCH_RET(schProcessOnTaskFailure(pJob, pTask, code));
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
  if (!SCH_IS_NEED_DROP_JOB(pJob)) {
    return;
  }

  void *pIter = taosHashIterate(list, NULL);
  while (pIter) {
    SSchTask *pTask = *(SSchTask **)pIter;

    schDropTaskOnExecNode(pJob, pTask);

    pIter = taosHashIterate(list, pIter);
  }
}


// Note: no more error processing, handled in function internal
int32_t schLaunchFetchTask(SSchJob *pJob) {
  int32_t code = 0;

  void *resData = atomic_load_ptr(&pJob->resData);
  if (resData) {
    SCH_JOB_DLOG("res already fetched, res:%p", resData);
    return TSDB_CODE_SUCCESS;
  }

  SCH_ERR_JRET(schBuildAndSendMsg(pJob, pJob->fetchTask, &pJob->resNode, TDMT_SCH_FETCH));

  return TSDB_CODE_SUCCESS;

_return:

  SCH_RET(schProcessOnTaskFailure(pJob, pJob->fetchTask, code));
}


