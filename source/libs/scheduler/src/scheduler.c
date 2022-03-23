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
#include "query.h"
#include "schedulerInt.h"
#include "tmsg.h"
#include "tref.h"

SSchedulerMgmt schMgmt = {0};

FORCE_INLINE SSchJob *schAcquireJob(int64_t refId) { return (SSchJob *)taosAcquireRef(schMgmt.jobRef, refId); }

FORCE_INLINE int32_t schReleaseJob(int64_t refId) { return taosReleaseRef(schMgmt.jobRef, refId); }

uint64_t schGenTaskId(void) { return atomic_add_fetch_64(&schMgmt.taskId, 1); }

#if 0
uint64_t schGenUUID(void) {
  static uint64_t hashId = 0;
  static int32_t requestSerialId = 0;

  if (hashId == 0) {
    char    uid[64];
    int32_t code = taosGetSystemUUID(uid, tListLen(uid));
    if (code != TSDB_CODE_SUCCESS) {
      qError("Failed to get the system uid, reason:%s", tstrerror(TAOS_SYSTEM_ERROR(errno)));
    } else {
      hashId = MurmurHash3_32(uid, strlen(uid));
    }
  }

  int64_t ts      = taosGetTimestampMs();
  uint64_t pid    = taosGetPId();
  int32_t val     = atomic_add_fetch_32(&requestSerialId, 1);

  uint64_t id = ((hashId & 0x0FFF) << 52) | ((pid & 0x0FFF) << 40) | ((ts & 0xFFFFFF) << 16) | (val & 0xFFFF);
  return id;
}
#endif

int32_t schInitTask(SSchJob *pJob, SSchTask *pTask, SSubplan *pPlan, SSchLevel *pLevel) {
  pTask->plan = pPlan;
  pTask->level = pLevel;
  SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_NOT_START);
  pTask->taskId = schGenTaskId();
  pTask->execAddrs = taosArrayInit(SCH_MAX_CANDIDATE_EP_NUM, sizeof(SQueryNodeAddr));
  if (NULL == pTask->execAddrs) {
    SCH_TASK_ELOG("taosArrayInit %d exec addrs failed", SCH_MAX_CANDIDATE_EP_NUM);
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  return TSDB_CODE_SUCCESS;
}

void schFreeTask(SSchTask *pTask) {
  if (pTask->candidateAddrs) {
    taosArrayDestroy(pTask->candidateAddrs);
  }

  tfree(pTask->msg);

  if (pTask->children) {
    taosArrayDestroy(pTask->children);
  }

  if (pTask->parents) {
    taosArrayDestroy(pTask->parents);
  }

  if (pTask->execAddrs) {
    taosArrayDestroy(pTask->execAddrs);
  }
}

static FORCE_INLINE bool schJobNeedToStop(SSchJob *pJob, int8_t *pStatus) {
  int8_t status = SCH_GET_JOB_STATUS(pJob);
  if (pStatus) {
    *pStatus = status;
  }

  return (status == JOB_TASK_STATUS_FAILED || status == JOB_TASK_STATUS_CANCELLED ||
          status == JOB_TASK_STATUS_CANCELLING || status == JOB_TASK_STATUS_DROPPING ||
          status == JOB_TASK_STATUS_SUCCEED);
}

int32_t schValidateTaskReceivedMsgType(SSchJob *pJob, SSchTask *pTask, int32_t msgType) {
  int32_t lastMsgType = SCH_GET_TASK_LASTMSG_TYPE(pTask);
  int32_t taskStatus = SCH_GET_TASK_STATUS(pTask);

  switch (msgType) {
    case TDMT_VND_CREATE_TABLE_RSP:
    case TDMT_VND_SUBMIT_RSP:
    case TDMT_VND_QUERY_RSP:
    case TDMT_VND_RES_READY_RSP:
    case TDMT_VND_FETCH_RSP:
    case TDMT_VND_DROP_TASK:
      if (lastMsgType != (msgType - 1)) {
        SCH_TASK_ELOG("rsp msg type mis-match, last sent msgType:%s, rspType:%s", TMSG_INFO(lastMsgType),
                      TMSG_INFO(msgType));
        SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
      }

      if (taskStatus != JOB_TASK_STATUS_EXECUTING && taskStatus != JOB_TASK_STATUS_PARTIAL_SUCCEED) {
        SCH_TASK_ELOG("rsp msg conflicted with task status, status:%s, rspType:%s", jobTaskStatusStr(taskStatus), TMSG_INFO(msgType));
        SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
      }

      break;
    default:
      SCH_TASK_ELOG("unknown rsp msg, type:%s, status:%s", TMSG_INFO(msgType), jobTaskStatusStr(taskStatus));
      SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SCH_SET_TASK_LASTMSG_TYPE(pTask, -1);

  return TSDB_CODE_SUCCESS;
}

int32_t schCheckAndUpdateJobStatus(SSchJob *pJob, int8_t newStatus) {
  int32_t code = 0;

  int8_t oriStatus = 0;

  while (true) {
    oriStatus = SCH_GET_JOB_STATUS(pJob);

    if (oriStatus == newStatus) {
      SCH_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
    }

    switch (oriStatus) {
      case JOB_TASK_STATUS_NULL:
        if (newStatus != JOB_TASK_STATUS_NOT_START) {
          SCH_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
        }

        break;
      case JOB_TASK_STATUS_NOT_START:
        if (newStatus != JOB_TASK_STATUS_EXECUTING) {
          SCH_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
        }

        break;
      case JOB_TASK_STATUS_EXECUTING:
        if (newStatus != JOB_TASK_STATUS_PARTIAL_SUCCEED && newStatus != JOB_TASK_STATUS_FAILED &&
            newStatus != JOB_TASK_STATUS_CANCELLING && newStatus != JOB_TASK_STATUS_CANCELLED &&
            newStatus != JOB_TASK_STATUS_DROPPING) {
          SCH_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
        }

        break;
      case JOB_TASK_STATUS_PARTIAL_SUCCEED:
        if (newStatus != JOB_TASK_STATUS_FAILED && newStatus != JOB_TASK_STATUS_SUCCEED &&
            newStatus != JOB_TASK_STATUS_DROPPING) {
          SCH_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
        }

        break;
      case JOB_TASK_STATUS_SUCCEED:
      case JOB_TASK_STATUS_FAILED:
      case JOB_TASK_STATUS_CANCELLING:
        if (newStatus != JOB_TASK_STATUS_DROPPING) {
          SCH_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
        }

        break;
      case JOB_TASK_STATUS_CANCELLED:
      case JOB_TASK_STATUS_DROPPING:
        SCH_ERR_JRET(TSDB_CODE_QRY_JOB_FREED);
        break;

      default:
        SCH_JOB_ELOG("invalid job status:%s", jobTaskStatusStr(oriStatus));
        SCH_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
    }

    if (oriStatus != atomic_val_compare_exchange_8(&pJob->status, oriStatus, newStatus)) {
      continue;
    }

    SCH_JOB_DLOG("job status updated from %s to %s", jobTaskStatusStr(oriStatus), jobTaskStatusStr(newStatus));

    break;
  }

  return TSDB_CODE_SUCCESS;

_return:

  SCH_JOB_ELOG("invalid job status update, from %s to %s", jobTaskStatusStr(oriStatus), jobTaskStatusStr(newStatus));
  SCH_ERR_RET(code);
}

int32_t schBuildTaskRalation(SSchJob *pJob, SHashObj *planToTask) {
  for (int32_t i = 0; i < pJob->levelNum; ++i) {
    SSchLevel *pLevel = taosArrayGet(pJob->levels, i);

    for (int32_t m = 0; m < pLevel->taskNum; ++m) {
      SSchTask *pTask = taosArrayGet(pLevel->subTasks, m);
      SSubplan *pPlan = pTask->plan;
      int32_t   childNum = pPlan->pChildren ? (int32_t)LIST_LENGTH(pPlan->pChildren) : 0;
      int32_t   parentNum = pPlan->pParents ? (int32_t)LIST_LENGTH(pPlan->pParents) : 0;

      if (childNum > 0) {
        if (pJob->levelIdx == pLevel->level) {
          SCH_JOB_ELOG("invalid query plan, lowest level, childNum:%d", childNum);
          SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
        }

        pTask->children = taosArrayInit(childNum, POINTER_BYTES);
        if (NULL == pTask->children) {
          SCH_TASK_ELOG("taosArrayInit %d children failed", childNum);
          SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
        }
      }

      for (int32_t n = 0; n < childNum; ++n) {
        SSubplan  *child = (SSubplan *)nodesListGetNode(pPlan->pChildren, n);
        SSchTask **childTask = taosHashGet(planToTask, &child, POINTER_BYTES);
        if (NULL == childTask || NULL == *childTask) {
          SCH_TASK_ELOG("subplan children relationship error, level:%d, taskIdx:%d, childIdx:%d", i, m, n);
          SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
        }

        if (NULL == taosArrayPush(pTask->children, childTask)) {
          SCH_TASK_ELOG("taosArrayPush childTask failed, level:%d, taskIdx:%d, childIdx:%d", i, m, n);
          SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
        }
      }

      if (parentNum > 0) {
        if (0 == pLevel->level) {
          SCH_TASK_ELOG("invalid task info, level:0, parentNum:%d", parentNum);
          SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
        }

        pTask->parents = taosArrayInit(parentNum, POINTER_BYTES);
        if (NULL == pTask->parents) {
          SCH_TASK_ELOG("taosArrayInit %d parents failed", parentNum);
          SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
        }
      } else {
        if (0 != pLevel->level) {
          SCH_TASK_ELOG("invalid task info, level:%d, parentNum:%d", pLevel->level, parentNum);
          SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
        }
      }

      for (int32_t n = 0; n < parentNum; ++n) {
        SSubplan  *parent = (SSubplan *)nodesListGetNode(pPlan->pParents, n);
        SSchTask **parentTask = taosHashGet(planToTask, &parent, POINTER_BYTES);
        if (NULL == parentTask || NULL == *parentTask) {
          SCH_TASK_ELOG("subplan parent relationship error, level:%d, taskIdx:%d, childIdx:%d", i, m, n);
          SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
        }

        if (NULL == taosArrayPush(pTask->parents, parentTask)) {
          SCH_TASK_ELOG("taosArrayPush parentTask failed, level:%d, taskIdx:%d, childIdx:%d", i, m, n);
          SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
        }
      }

      SCH_TASK_DLOG("level:%d, parentNum:%d, childNum:%d", i, parentNum, childNum);
    }
  }

  SSchLevel *pLevel = taosArrayGet(pJob->levels, 0);
  if (SCH_IS_QUERY_JOB(pJob) && pLevel->taskNum > 1) {
    SCH_JOB_ELOG("invalid query plan, level:0, taskNum:%d", pLevel->taskNum);
    SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
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

int32_t schRecordTaskExecNode(SSchJob *pJob, SSchTask *pTask, SQueryNodeAddr *addr) {
  if (NULL == taosArrayPush(pTask->execAddrs, addr)) {
    SCH_TASK_ELOG("taosArrayPush addr to execAddr list failed, errno:%d", errno);
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schValidateAndBuildJob(SQueryPlan *pDag, SSchJob *pJob) {
  int32_t code = 0;
  pJob->queryId = pDag->queryId;

  if (pDag->numOfSubplans <= 0) {
    SCH_JOB_ELOG("invalid subplan num:%d", pDag->numOfSubplans);
    SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t levelNum = (int32_t)LIST_LENGTH(pDag->pSubplans);
  if (levelNum <= 0) {
    SCH_JOB_ELOG("invalid level num:%d", levelNum);
    SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SHashObj *planToTask = taosHashInit(
      SCHEDULE_DEFAULT_MAX_TASK_NUM,
      taosGetDefaultHashFunction(POINTER_BYTES == sizeof(int64_t) ? TSDB_DATA_TYPE_BIGINT : TSDB_DATA_TYPE_INT), false,
      HASH_NO_LOCK);
  if (NULL == planToTask) {
    SCH_JOB_ELOG("taosHashInit %d failed", SCHEDULE_DEFAULT_MAX_TASK_NUM);
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  pJob->levels = taosArrayInit(levelNum, sizeof(SSchLevel));
  if (NULL == pJob->levels) {
    SCH_JOB_ELOG("taosArrayInit %d failed", levelNum);
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  pJob->levelNum = levelNum;
  pJob->levelIdx = levelNum - 1;

  pJob->subPlans = pDag->pSubplans;

  SSchLevel      level = {0};
  SNodeListNode *plans = NULL;
  int32_t        taskNum = 0;
  SSchLevel     *pLevel = NULL;

  level.status = JOB_TASK_STATUS_NOT_START;

  for (int32_t i = 0; i < levelNum; ++i) {
    if (NULL == taosArrayPush(pJob->levels, &level)) {
      SCH_JOB_ELOG("taosArrayPush level failed, level:%d", i);
      SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    pLevel = taosArrayGet(pJob->levels, i);
    pLevel->level = i;

    plans = (SNodeListNode *)nodesListGetNode(pDag->pSubplans, i);
    if (NULL == plans) {
      SCH_JOB_ELOG("empty level plan, level:%d", i);
      SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
    }

    taskNum = (int32_t)LIST_LENGTH(plans->pNodeList);
    if (taskNum <= 0) {
      SCH_JOB_ELOG("invalid level plan number:%d, level:%d", taskNum, i);
      SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
    }

    pLevel->taskNum = taskNum;

    pLevel->subTasks = taosArrayInit(taskNum, sizeof(SSchTask));
    if (NULL == pLevel->subTasks) {
      SCH_JOB_ELOG("taosArrayInit %d failed", taskNum);
      SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    for (int32_t n = 0; n < taskNum; ++n) {
      SSubplan *plan = (SSubplan *)nodesListGetNode(plans->pNodeList, n);

      SCH_SET_JOB_TYPE(pJob, plan->subplanType);

      SSchTask  task = {0};
      SSchTask *pTask = &task;

      SCH_ERR_JRET(schInitTask(pJob, &task, plan, pLevel));

      void *p = taosArrayPush(pLevel->subTasks, &task);
      if (NULL == p) {
        SCH_TASK_ELOG("taosArrayPush task to level failed, level:%d, taskIdx:%d", pLevel->level, n);
        SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      if (0 != taosHashPut(planToTask, &plan, POINTER_BYTES, &p, POINTER_BYTES)) {
        SCH_TASK_ELOG("taosHashPut to planToTaks failed, taskIdx:%d", n);
        SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      ++pJob->taskNum;
    }

    SCH_JOB_DLOG("level initialized, taskNum:%d", taskNum);
  }

  SCH_ERR_JRET(schBuildTaskRalation(pJob, planToTask));

_return:
  if (planToTask) {
    taosHashCleanup(planToTask);
  }

  SCH_RET(code);
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

    SCH_TASK_DLOG("use execNode from plan as candidate addr, numOfEps:%d", pTask->plan->execNode.epSet.numOfEps);

    return TSDB_CODE_SUCCESS;
  }

  int32_t addNum = 0;
  int32_t nodeNum = 0;
  if (pJob->nodeList) {
    nodeNum = taosArrayGetSize(pJob->nodeList);

    for (int32_t i = 0; i < nodeNum && addNum < SCH_MAX_CANDIDATE_EP_NUM; ++i) {
      SQueryNodeAddr *naddr = taosArrayGet(pJob->nodeList, i);

      if (NULL == taosArrayPush(pTask->candidateAddrs, naddr)) {
        SCH_TASK_ELOG("taosArrayPush execNode to candidate addrs failed, addNum:%d, errno:%d", addNum, errno);
        SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      ++addNum;
    }
  }

  if (addNum <= 0) {
    SCH_TASK_ELOG("no available execNode as candidates, nodeNum:%d", nodeNum);
    SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  /*
    for (int32_t i = 0; i < job->dataSrcEps.numOfEps && addNum < SCH_MAX_CANDIDATE_EP_NUM; ++i) {
      strncpy(epSet->fqdn[epSet->numOfEps], job->dataSrcEps.fqdn[i], sizeof(job->dataSrcEps.fqdn[i]));
      epSet->port[epSet->numOfEps] = job->dataSrcEps.port[i];

      ++epSet->numOfEps;
    }
  */

  return TSDB_CODE_SUCCESS;
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

int32_t schTaskCheckSetRetry(SSchJob *pJob, SSchTask *pTask, int32_t errCode, bool *needRetry) {
  int8_t status = 0;
  ++pTask->tryTimes;
  
  if (schJobNeedToStop(pJob, &status)) {
    *needRetry = false;
    SCH_TASK_DLOG("task no more retry cause of job status, job status:%s", jobTaskStatusStr(status));
    return TSDB_CODE_SUCCESS;
  }

  if (pTask->tryTimes >= REQUEST_MAX_TRY_TIMES) {
    *needRetry = false;
    SCH_TASK_DLOG("task no more retry since reach max try times, tryTimes:%d", pTask->tryTimes);
    return TSDB_CODE_SUCCESS;
  }
  
  if (!NEED_SCHEDULER_RETRY_ERROR(errCode)) {
    *needRetry = false;
    SCH_TASK_DLOG("task no more retry cause of errCode, errCode:%x - %s", errCode, tstrerror(errCode));
    return TSDB_CODE_SUCCESS;
  }

  // TODO CHECK epList/condidateList
  if (SCH_IS_DATA_SRC_TASK(pTask)) {
    if (pTask->tryTimes >= SCH_TASK_NUM_OF_EPS(&pTask->plan->execNode)) {
      *needRetry = false;
      SCH_TASK_DLOG("task no more retry since all ep tried, tryTimes:%d, epNum:%d", pTask->tryTimes, SCH_TASK_NUM_OF_EPS(&pTask->plan->execNode));
      return TSDB_CODE_SUCCESS;
    }
  } else {
    int32_t candidateNum = taosArrayGetSize(pTask->candidateAddrs);

    if ((pTask->candidateIdx + 1) >= candidateNum) {
      *needRetry = false;
      SCH_TASK_DLOG("task no more retry since all candiates tried, candidateIdx:%d, candidateNum:%d", pTask->candidateIdx, candidateNum);
      return TSDB_CODE_SUCCESS;
    }
  }

  *needRetry = true;
  SCH_TASK_DLOG("task need the %dth retry, errCode:%x - %s", pTask->tryTimes, errCode, tstrerror(errCode));
  
  return TSDB_CODE_SUCCESS;
}

int32_t schHandleTaskRetry(SSchJob *pJob, SSchTask *pTask) {
  atomic_sub_fetch_32(&pTask->level->taskLaunchedNum, 1);

  if (SCH_TASK_NEED_FLOW_CTRL(pJob, pTask)) {
    SCH_ERR_RET(schDecTaskFlowQuota(pJob, pTask));
    SCH_ERR_RET(schLaunchTasksInFlowCtrlList(pJob, pTask));
  }

  if (SCH_IS_DATA_SRC_TASK(pTask)) {
    SCH_SWITCH_EPSET(&pTask->plan->execNode);
  } else {
    ++pTask->candidateIdx;
  }

  SCH_ERR_RET(schLaunchTask(pJob, pTask));

  return TSDB_CODE_SUCCESS;
}

int32_t schUpdateHbConnection(SQueryNodeEpId *epId, SSchHbTrans *trans) {
  int32_t      code = 0;
  SSchHbTrans *hb = NULL;

  while (true) {
    hb = taosHashGet(schMgmt.hbConnections, epId, sizeof(SQueryNodeEpId));
    if (NULL == hb) {
      code = taosHashPut(schMgmt.hbConnections, epId, sizeof(SQueryNodeEpId), trans, sizeof(SSchHbTrans));
      if (code) {
        if (HASH_NODE_EXIST(code)) {
          continue;
        }

        qError("taosHashPut hb trans failed, nodeId:%d, fqdn:%s, port:%d", epId->nodeId, epId->ep.fqdn, epId->ep.port);
        SCH_ERR_RET(code);
      }

      qDebug("hb connection updated, seqId:%" PRIx64 ", sId:%" PRIx64
             ", nodeId:%d, fqdn:%s, port:%d, instance:%p, connection:%p",
             trans->seqId, schMgmt.sId, epId->nodeId, epId->ep.fqdn, epId->ep.port, trans->trans.transInst,
             trans->trans.transHandle);

      return TSDB_CODE_SUCCESS;
    }

    break;
  }

  SCH_LOCK(SCH_WRITE, &hb->lock);

  if (hb->seqId >= trans->seqId) {
    qDebug("hb trans seqId is old, seqId:%" PRId64 ", currentId:%" PRId64 ", nodeId:%d, fqdn:%s, port:%d", trans->seqId,
           hb->seqId, epId->nodeId, epId->ep.fqdn, epId->ep.port);

    SCH_UNLOCK(SCH_WRITE, &hb->lock);
    return TSDB_CODE_SUCCESS;
  }

  hb->seqId = trans->seqId;
  memcpy(&hb->trans, &trans->trans, sizeof(trans->trans));

  SCH_UNLOCK(SCH_WRITE, &hb->lock);

  qDebug("hb connection updated, seqId:%" PRIx64 ", sId:%" PRIx64
         ", nodeId:%d, fqdn:%s, port:%d, instance:%p, connection:%p",
         trans->seqId, schMgmt.sId, epId->nodeId, epId->ep.fqdn, epId->ep.port, trans->trans.transInst,
         trans->trans.transHandle);

  return TSDB_CODE_SUCCESS;
}

void schUpdateJobErrCode(SSchJob *pJob, int32_t errCode) {
  if (TSDB_CODE_SUCCESS == errCode) {
    return;
  }

  int32_t origCode = atomic_load_32(&pJob->errCode);
  if (TSDB_CODE_SUCCESS == origCode) {
    if (origCode == atomic_val_compare_exchange_32(&pJob->errCode, origCode, errCode)) {
      goto _return;
    }

    origCode = atomic_load_32(&pJob->errCode);
  }

  if (NEED_CLIENT_HANDLE_ERROR(origCode)) {
    return;
  }
  
  if (NEED_CLIENT_HANDLE_ERROR(errCode)) {
    atomic_store_32(&pJob->errCode, errCode);
    goto _return;
  }

  return;
  
_return:  

  SCH_JOB_DLOG("job errCode updated to %x - %s", errCode, tstrerror(errCode));
}

int32_t schProcessOnJobFailureImpl(SSchJob *pJob, int32_t status, int32_t errCode) {
  // if already FAILED, no more processing
  SCH_ERR_RET(schCheckAndUpdateJobStatus(pJob, status));

  schUpdateJobErrCode(pJob, errCode);

  if (atomic_load_8(&pJob->userFetch) || pJob->attr.syncSchedule) {
    tsem_post(&pJob->rspSem);
  }

  int32_t code = atomic_load_32(&pJob->errCode);

  SCH_JOB_DLOG("job failed with error: %s", tstrerror(code));

  SCH_RET(code);
}

// Note: no more task error processing, handled in function internal
int32_t schProcessOnJobFailure(SSchJob *pJob, int32_t errCode) {
  SCH_RET(schProcessOnJobFailureImpl(pJob, JOB_TASK_STATUS_FAILED, errCode));
}

// Note: no more error processing, handled in function internal
int32_t schProcessOnJobDropped(SSchJob *pJob, int32_t errCode) {
  SCH_RET(schProcessOnJobFailureImpl(pJob, JOB_TASK_STATUS_DROPPING, errCode));
}

// Note: no more task error processing, handled in function internal
int32_t schProcessOnJobPartialSuccess(SSchJob *pJob) {
  int32_t code = 0;

  SCH_ERR_RET(schCheckAndUpdateJobStatus(pJob, JOB_TASK_STATUS_PARTIAL_SUCCEED));

  if (pJob->attr.syncSchedule) {
    tsem_post(&pJob->rspSem);
  }

  if (atomic_load_8(&pJob->userFetch)) {
    SCH_ERR_JRET(schFetchFromRemote(pJob));
  }

  return TSDB_CODE_SUCCESS;

_return:

  SCH_RET(schProcessOnJobFailure(pJob, code));
}

int32_t schProcessOnDataFetched(SSchJob *job) {
  atomic_val_compare_exchange_32(&job->remoteFetch, 1, 0);
  tsem_post(&job->rspSem);
}

// Note: no more task error processing, handled in function internal
int32_t schProcessOnTaskFailure(SSchJob *pJob, SSchTask *pTask, int32_t errCode) {
  int8_t status = 0;

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

    if (SCH_GET_TASK_STATUS(pTask) == JOB_TASK_STATUS_EXECUTING) {
      SCH_ERR_JRET(schMoveTaskToFailList(pJob, pTask, &moved));
    } else {
      SCH_TASK_ELOG("task not in executing list, status:%s", SCH_GET_TASK_STATUS_STR(pTask));
      SCH_ERR_JRET(TSDB_CODE_SCH_STATUS_ERROR);
    }

    SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_FAILED);
    
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

  SCH_ERR_JRET(schMoveTaskToSuccList(pJob, pTask, &moved));

  SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_PARTIAL_SUCCEED);

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

    SCH_ERR_JRET(schMoveTaskToExecList(pJob, pTask, &moved));

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
    SSchTask *par = *(SSchTask **)taosArrayGet(pTask->parents, i);
    int32_t   readyNum = atomic_add_fetch_32(&par->childReady, 1);

    SCH_LOCK(SCH_WRITE, &par->lock);
    SDownstreamSourceNode source = {.type = QUERY_NODE_DOWNSTREAM_SOURCE,
                                    .taskId = pTask->taskId,
                                    .schedId = schMgmt.sId,
                                    .addr = pTask->succeedAddr};
    qSetSubplanExecutionNode(par->plan, pTask->plan->id.groupId, &source);
    SCH_UNLOCK(SCH_WRITE, &par->lock);

    if (SCH_TASK_READY_TO_LUNCH(readyNum, par)) {
      SCH_ERR_RET(schLaunchTaskImpl(pJob, par));
    }
  }

  return TSDB_CODE_SUCCESS;

_return:

  SCH_RET(schProcessOnJobFailure(pJob, code));
}

// Note: no more error processing, handled in function internal
int32_t schFetchFromRemote(SSchJob *pJob) {
  int32_t code = 0;

  if (atomic_val_compare_exchange_32(&pJob->remoteFetch, 0, 1) != 0) {
    SCH_JOB_ELOG("prior fetching not finished, remoteFetch:%d", atomic_load_32(&pJob->remoteFetch));
    return TSDB_CODE_SUCCESS;
  }

  void *resData = atomic_load_ptr(&pJob->resData);
  if (resData) {
    atomic_val_compare_exchange_32(&pJob->remoteFetch, 1, 0);

    SCH_JOB_DLOG("res already fetched, res:%p", resData);
    return TSDB_CODE_SUCCESS;
  }

  SCH_ERR_JRET(schBuildAndSendMsg(pJob, pJob->fetchTask, &pJob->resNode, TDMT_VND_FETCH));

  return TSDB_CODE_SUCCESS;

_return:

  atomic_val_compare_exchange_32(&pJob->remoteFetch, 1, 0);

  SCH_RET(schProcessOnTaskFailure(pJob, pJob->fetchTask, code));
}

// Note: no more task error processing, handled in function internal
int32_t schHandleResponseMsg(SSchJob *pJob, SSchTask *pTask, int32_t msgType, char *msg, int32_t msgSize,
                             int32_t rspCode) {
  int32_t code = 0;
  int8_t  status = 0;

  if (schJobNeedToStop(pJob, &status)) {
    SCH_TASK_ELOG("rsp not processed cause of job status, job status:%s, rspCode:0x%x", jobTaskStatusStr(status), rspCode);
    SCH_RET(atomic_load_32(&pJob->errCode));
  }

  SCH_ERR_JRET(schValidateTaskReceivedMsgType(pJob, pTask, msgType));

  switch (msgType) {
    case TDMT_VND_CREATE_TABLE_RSP: {
      SVCreateTbBatchRsp batchRsp = {0};
      if (msg) {
        tDeserializeSVCreateTbBatchRsp(msg, msgSize, &batchRsp);
        if (batchRsp.rspList) {
          int32_t num = taosArrayGetSize(batchRsp.rspList);
          for (int32_t i = 0; i < num; ++i) {
            SVCreateTbRsp *rsp = taosArrayGet(batchRsp.rspList, i);
            if (NEED_CLIENT_HANDLE_ERROR(rsp->code)) {
              taosArrayDestroy(batchRsp.rspList);
              SCH_ERR_JRET(rsp->code);
            }
          }
          
          taosArrayDestroy(batchRsp.rspList);
        }
      }        
      
      SCH_ERR_JRET(rspCode);
      SCH_ERR_RET(schProcessOnTaskSuccess(pJob, pTask));
      break;
    }
    case TDMT_VND_SUBMIT_RSP: {
      if (msg) {
        SSubmitRsp *rsp = (SSubmitRsp *)msg;
        SCH_ERR_JRET(rsp->code);
      }
      SCH_ERR_JRET(rspCode);

      SSubmitRsp *rsp = (SSubmitRsp *)msg;
      if (rsp) {
        pJob->resNumOfRows += rsp->affectedRows;
      }

      SCH_ERR_RET(schProcessOnTaskSuccess(pJob, pTask));

      break;
    }
    case TDMT_VND_QUERY_RSP: {
        SQueryTableRsp rsp = {0};
        if (msg) {
          tDeserializeSQueryTableRsp(msg, msgSize, &rsp);
          SCH_ERR_JRET(rsp.code);
        }
        
        SCH_ERR_JRET(rspCode);
        
        if (NULL == msg) {
          SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
        }
        
        SCH_ERR_JRET(schBuildAndSendMsg(pJob, pTask, NULL, TDMT_VND_RES_READY));
        
        break;
    }
    case TDMT_VND_RES_READY_RSP: {
      SResReadyRsp *rsp = (SResReadyRsp *)msg;

      SCH_ERR_JRET(rspCode);
      if (NULL == msg) {
        SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
      }
      SCH_ERR_JRET(rsp->code);
      SCH_ERR_RET(schProcessOnTaskSuccess(pJob, pTask));

      break;
    }
    case TDMT_VND_FETCH_RSP: {
      SRetrieveTableRsp *rsp = (SRetrieveTableRsp *)msg;

      SCH_ERR_JRET(rspCode);
      if (NULL == msg) {
        SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
      }

      if (pJob->resData) {
        SCH_TASK_ELOG("got fetch rsp while res already exists, res:%p", pJob->resData);
        tfree(rsp);
        SCH_ERR_JRET(TSDB_CODE_SCH_STATUS_ERROR);
      }

      atomic_store_ptr(&pJob->resData, rsp);
      atomic_add_fetch_32(&pJob->resNumOfRows, htonl(rsp->numOfRows));

      if (rsp->completed) {
        SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_SUCCEED);
      }

      SCH_TASK_DLOG("got fetch rsp, rows:%d, complete:%d", htonl(rsp->numOfRows), rsp->completed);

      schProcessOnDataFetched(pJob);
      break;
    }
    case TDMT_VND_DROP_TASK_RSP: {
      // SHOULD NEVER REACH HERE
      SCH_TASK_ELOG("invalid status to handle drop task rsp, refId:%" PRIx64, pJob->refId);
      SCH_ERR_JRET(TSDB_CODE_SCH_INTERNAL_ERROR);
      break;
    }
    default:
      SCH_TASK_ELOG("unknown rsp msg, type:%d, status:%s", msgType, SCH_GET_TASK_STATUS_STR(pTask));
      SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  return TSDB_CODE_SUCCESS;

_return:

  SCH_RET(schProcessOnTaskFailure(pJob, pTask, code));
}

int32_t schHandleCallback(void *param, const SDataBuf *pMsg, int32_t msgType, int32_t rspCode) {
  int32_t            code = 0;
  SSchCallbackParam *pParam = (SSchCallbackParam *)param;
  SSchTask          *pTask = NULL;

  SSchJob *pJob = schAcquireJob(pParam->refId);
  if (NULL == pJob) {
    qError("QID:0x%" PRIx64 ",TID:0x%" PRIx64 "taosAcquireRef job failed, may be dropped, refId:%" PRIx64,
           pParam->queryId, pParam->taskId, pParam->refId);
    SCH_ERR_JRET(TSDB_CODE_QRY_JOB_FREED);
  }

  int32_t s = taosHashGetSize(pJob->execTasks);
  if (s <= 0) {
    SCH_JOB_ELOG("empty execTask list, refId:%" PRIx64 ", taskId:%" PRIx64, pParam->refId, pParam->taskId);
    SCH_ERR_JRET(TSDB_CODE_SCH_INTERNAL_ERROR);
  }

  SSchTask **task = taosHashGet(pJob->execTasks, &pParam->taskId, sizeof(pParam->taskId));
  if (NULL == task || NULL == (*task)) {
    SCH_JOB_ELOG("task not found in execTask list, refId:%" PRIx64 ", taskId:%" PRIx64, pParam->refId, pParam->taskId);
    SCH_ERR_JRET(TSDB_CODE_SCH_INTERNAL_ERROR);
  }

  pTask = *task;
  SCH_TASK_DLOG("rsp msg received, type:%s, code:%s", TMSG_INFO(msgType), tstrerror(rspCode));

  pTask->handle = pMsg->handle;
  SCH_ERR_JRET(schHandleResponseMsg(pJob, pTask, msgType, pMsg->pData, pMsg->len, rspCode));

_return:

  if (pJob) {
    schReleaseJob(pParam->refId);
  }

  tfree(param);
  SCH_RET(code);
}

int32_t schHandleSubmitCallback(void *param, const SDataBuf *pMsg, int32_t code) {
  return schHandleCallback(param, pMsg, TDMT_VND_SUBMIT_RSP, code);
}

int32_t schHandleCreateTableCallback(void *param, const SDataBuf *pMsg, int32_t code) {
  return schHandleCallback(param, pMsg, TDMT_VND_CREATE_TABLE_RSP, code);
}

int32_t schHandleQueryCallback(void *param, const SDataBuf *pMsg, int32_t code) {
  return schHandleCallback(param, pMsg, TDMT_VND_QUERY_RSP, code);
}

int32_t schHandleFetchCallback(void *param, const SDataBuf *pMsg, int32_t code) {
  return schHandleCallback(param, pMsg, TDMT_VND_FETCH_RSP, code);
}

int32_t schHandleReadyCallback(void *param, const SDataBuf *pMsg, int32_t code) {
  return schHandleCallback(param, pMsg, TDMT_VND_RES_READY_RSP, code);
}

int32_t schHandleDropCallback(void *param, const SDataBuf *pMsg, int32_t code) {
  SSchCallbackParam *pParam = (SSchCallbackParam *)param;
  qDebug("QID:%" PRIx64 ",TID:%" PRIx64 " drop task rsp received, code:%x", pParam->queryId, pParam->taskId, code);
}

int32_t schHandleHbCallback(void *param, const SDataBuf *pMsg, int32_t code) {
  if (code) {
    qError("hb rsp error:%s", tstrerror(code));
    SCH_ERR_RET(code);
  }

  SSchedulerHbRsp rsp = {0};

  SSchCallbackParam *pParam = (SSchCallbackParam *)param;

  if (tDeserializeSSchedulerHbRsp(pMsg->pData, pMsg->len, &rsp)) {
    qError("invalid hb rsp msg, size:%d", pMsg->len);
    SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  if (rsp.seqId != (uint64_t)-1) {
    SSchHbTrans trans = {0};
    trans.seqId = rsp.seqId;
    trans.trans.transInst = pParam->transport;
    trans.trans.transHandle = pMsg->handle;

    SCH_RET(schUpdateHbConnection(&rsp.epId, &trans));
  }

  int32_t taskNum = (int32_t)taosArrayGetSize(rsp.taskStatus);
  for (int32_t i = 0; i < taskNum; ++i) {
    STaskStatus *taskStatus = taosArrayGet(rsp.taskStatus, i);

    SSchJob *pJob = schAcquireJob(taskStatus->refId);
    if (NULL == pJob) {
      qWarn("job not found, refId:0x%" PRIx64 ",QID:0x%" PRIx64 ",TID:0x%" PRIx64, taskStatus->refId,
            taskStatus->queryId, taskStatus->taskId);
      // TODO DROP TASK FROM SERVER!!!!
      continue;
    }

    // TODO

    schReleaseJob(taskStatus->refId);
  }

_return:

  tFreeSSchedulerHbRsp(&rsp);

  SCH_RET(code);
}

int32_t schGetCallbackFp(int32_t msgType, __async_send_cb_fn_t *fp) {
  switch (msgType) {
    case TDMT_VND_CREATE_TABLE:
      *fp = schHandleCreateTableCallback;
      break;
    case TDMT_VND_SUBMIT:
      *fp = schHandleSubmitCallback;
      break;
    case TDMT_VND_QUERY:
      *fp = schHandleQueryCallback;
      break;
    case TDMT_VND_RES_READY:
      *fp = schHandleReadyCallback;
      break;
    case TDMT_VND_FETCH:
      *fp = schHandleFetchCallback;
      break;
    case TDMT_VND_DROP_TASK:
      *fp = schHandleDropCallback;
      break;
    case TDMT_VND_QUERY_HEARTBEAT:
      *fp = schHandleHbCallback;
      break;
    default:
      qError("unknown msg type for callback, msgType:%d", msgType);
      SCH_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schAsyncSendMsg(SSchJob *pJob, SSchTask *pTask, void *transport, SEpSet *epSet, int32_t msgType, void *msg,
                        uint32_t msgSize) {
  int32_t code = 0;

  SSchTrans *trans = (SSchTrans *)transport;

  SMsgSendInfo *pMsgSendInfo = calloc(1, sizeof(SMsgSendInfo));
  if (NULL == pMsgSendInfo) {
    SCH_TASK_ELOG("calloc %d failed", (int32_t)sizeof(SMsgSendInfo));
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SSchCallbackParam *param = calloc(1, sizeof(SSchCallbackParam));
  if (NULL == param) {
    SCH_TASK_ELOG("calloc %d failed", (int32_t)sizeof(SSchCallbackParam));
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  __async_send_cb_fn_t fp = NULL;
  SCH_ERR_JRET(schGetCallbackFp(msgType, &fp));

  param->queryId = pJob->queryId;
  param->refId = pJob->refId;
  param->taskId = SCH_TASK_ID(pTask);
  param->transport = trans->transInst;

  pMsgSendInfo->param = param;
  pMsgSendInfo->msgInfo.pData = msg;
  pMsgSendInfo->msgInfo.len = msgSize;
  pMsgSendInfo->msgInfo.handle = trans->transHandle;
  pMsgSendInfo->msgType = msgType;
  pMsgSendInfo->fp = fp;

  int64_t transporterId = 0;
  code = asyncSendMsgToServer(trans->transInst, epSet, &transporterId, pMsgSendInfo);
  if (code) {
    SCH_ERR_JRET(code);
  }

  SCH_TASK_DLOG("req msg sent, refId:%" PRIx64 ", type:%d, %s", pJob->refId, msgType, TMSG_INFO(msgType));
  return TSDB_CODE_SUCCESS;

_return:

  tfree(param);
  tfree(pMsgSendInfo);
  SCH_RET(code);
}

int32_t schBuildAndSendMsg(SSchJob *pJob, SSchTask *pTask, SQueryNodeAddr *addr, int32_t msgType) {
  uint32_t msgSize = 0;
  void    *msg = NULL;
  int32_t  code = 0;
  bool     isCandidateAddr = false;
  if (NULL == addr) {
    addr = taosArrayGet(pTask->candidateAddrs, pTask->candidateIdx);
    isCandidateAddr = true;
  }

  SEpSet epSet = addr->epSet;

  switch (msgType) {
    case TDMT_VND_CREATE_TABLE:
    case TDMT_VND_SUBMIT: {
      msgSize = pTask->msgLen;
      msg = calloc(1, msgSize);
      if (NULL == msg) {
        SCH_TASK_ELOG("calloc %d failed", msgSize);
        SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      memcpy(msg, pTask->msg, msgSize);
      break;
    }

    case TDMT_VND_QUERY: {
      uint32_t len = strlen(pJob->sql);

      msgSize = sizeof(SSubQueryMsg) + pTask->msgLen + len;
      msg = calloc(1, msgSize);
      if (NULL == msg) {
        SCH_TASK_ELOG("calloc %d failed", msgSize);
        SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      SSubQueryMsg *pMsg = msg;
      pMsg->header.vgId = htonl(addr->nodeId);
      pMsg->sId = htobe64(schMgmt.sId);
      pMsg->queryId = htobe64(pJob->queryId);
      pMsg->taskId = htobe64(pTask->taskId);
      pMsg->refId = htobe64(pJob->refId);
      pMsg->taskType = TASK_TYPE_TEMP;
      pMsg->phyLen = htonl(pTask->msgLen);
      pMsg->sqlLen = htonl(len);

      memcpy(pMsg->msg, pJob->sql, len);
      memcpy(pMsg->msg + len, pTask->msg, pTask->msgLen);
      
      break;
    }

    case TDMT_VND_RES_READY: {
      msgSize = sizeof(SResReadyReq);
      msg = calloc(1, msgSize);
      if (NULL == msg) {
        SCH_TASK_ELOG("calloc %d failed", msgSize);
        SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      SResReadyReq *pMsg = msg;

      pMsg->header.vgId = htonl(addr->nodeId);

      pMsg->sId = htobe64(schMgmt.sId);
      pMsg->queryId = htobe64(pJob->queryId);
      pMsg->taskId = htobe64(pTask->taskId);
      break;
    }
    case TDMT_VND_FETCH: {
      msgSize = sizeof(SResFetchReq);
      msg = calloc(1, msgSize);
      if (NULL == msg) {
        SCH_TASK_ELOG("calloc %d failed", msgSize);
        SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      SResFetchReq *pMsg = msg;

      pMsg->header.vgId = htonl(addr->nodeId);

      pMsg->sId = htobe64(schMgmt.sId);
      pMsg->queryId = htobe64(pJob->queryId);
      pMsg->taskId = htobe64(pTask->taskId);
      break;
    }
    case TDMT_VND_DROP_TASK: {
      msgSize = sizeof(STaskDropReq);
      msg = calloc(1, msgSize);
      if (NULL == msg) {
        SCH_TASK_ELOG("calloc %d failed", msgSize);
        SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      STaskDropReq *pMsg = msg;

      pMsg->header.vgId = htonl(addr->nodeId);

      pMsg->sId = htobe64(schMgmt.sId);
      pMsg->queryId = htobe64(pJob->queryId);
      pMsg->taskId = htobe64(pTask->taskId);
      pMsg->refId = htobe64(pJob->refId);
      break;
    }
    case TDMT_VND_QUERY_HEARTBEAT: {
      SSchedulerHbReq req = {0};
      req.sId = schMgmt.sId;
      req.header.vgId = addr->nodeId;
      req.epId.nodeId = addr->nodeId;
      memcpy(&req.epId.ep, SCH_GET_CUR_EP(addr), sizeof(SEp));

      msgSize = tSerializeSSchedulerHbReq(NULL, 0, &req);
      if (msgSize < 0) {
        SCH_JOB_ELOG("tSerializeSSchedulerHbReq hbReq failed, size:%d", msgSize);
        SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }
      msg = calloc(1, msgSize);
      if (NULL == msg) {
        SCH_JOB_ELOG("calloc %d failed", msgSize);
        SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }
      if (tSerializeSSchedulerHbReq(msg, msgSize, &req) < 0) {
        SCH_JOB_ELOG("tSerializeSSchedulerHbReq hbReq failed, size:%d", msgSize);
        SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }
      break;
    }
    default:
      SCH_TASK_ELOG("unknown msg type to send, msgType:%d", msgType);
      SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
      break;
  }

  SCH_SET_TASK_LASTMSG_TYPE(pTask, msgType);

  SSchTrans trans = {.transInst = pJob->transport, .transHandle = pTask ? pTask->handle : NULL};
  SCH_ERR_JRET(schAsyncSendMsg(pJob, pTask, &trans, &epSet, msgType, msg, msgSize));

  if (isCandidateAddr) {
    SCH_ERR_RET(schRecordTaskExecNode(pJob, pTask, addr));
  }

  return TSDB_CODE_SUCCESS;

_return:

  SCH_SET_TASK_LASTMSG_TYPE(pTask, -1);

  tfree(msg);
  SCH_RET(code);
}

int32_t schEnsureHbConnection(SSchJob *pJob, SSchTask *pTask) {
  SQueryNodeAddr *addr = taosArrayGet(pTask->candidateAddrs, pTask->candidateIdx);
  SQueryNodeEpId  epId = {0};

  epId.nodeId = addr->nodeId;
  memcpy(&epId.ep, SCH_GET_CUR_EP(addr), sizeof(SEp));

  SSchHbTrans *hb = taosHashGet(schMgmt.hbConnections, &epId, sizeof(SQueryNodeEpId));
  if (NULL == hb) {
    SCH_ERR_RET(schBuildAndSendMsg(pJob, NULL, addr, TDMT_VND_QUERY_HEARTBEAT));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schLaunchTaskImpl(SSchJob *pJob, SSchTask *pTask) {
  int8_t  status = 0;
  int32_t code = 0;

  atomic_add_fetch_32(&pTask->level->taskLaunchedNum, 1);

  if (schJobNeedToStop(pJob, &status)) {
    SCH_TASK_DLOG("no need to launch task cause of job status, job status:%s", jobTaskStatusStr(status));
    
    SCH_RET(atomic_load_32(&pJob->errCode));
  }

  // NOTE: race condition: the task should be put into the hash table before send msg to server
  if (SCH_GET_TASK_STATUS(pTask) != JOB_TASK_STATUS_EXECUTING) {
    SCH_ERR_RET(schPushTaskToExecList(pJob, pTask));
    SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_EXECUTING);
  }
  
  SSubplan *plan = pTask->plan;

  if (NULL == pTask->msg) {  // TODO add more detailed reason for failure
    code = qSubPlanToString(plan, &pTask->msg, &pTask->msgLen);
    if (TSDB_CODE_SUCCESS != code) {
      SCH_TASK_ELOG("failed to create physical plan, code:%s, msg:%p, len:%d", tstrerror(code), pTask->msg,
                    pTask->msgLen);
      SCH_ERR_RET(code);
    } else {
      SCH_TASK_DLOG("physical plan len:%d, %s", pTask->msgLen, pTask->msg);
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
  for (int32_t i = 0; i < level->taskNum; ++i) {
    SSchTask *pTask = taosArrayGet(level->subTasks, i);

    SCH_ERR_RET(schLaunchTask(pJob, pTask));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schLaunchJob(SSchJob *pJob) {
  SSchLevel *level = taosArrayGet(pJob->levels, pJob->levelIdx);

  SCH_ERR_RET(schCheckAndUpdateJobStatus(pJob, JOB_TASK_STATUS_EXECUTING));

  SCH_ERR_RET(schCheckJobNeedFlowCtrl(pJob, level));

  SCH_ERR_RET(schLaunchLevelTasks(pJob, level));

  return TSDB_CODE_SUCCESS;
}

void schDropTaskOnExecutedNode(SSchJob *pJob, SSchTask *pTask) {
  if (NULL == pTask->execAddrs) {
    SCH_TASK_DLOG("no exec address, status:%s", SCH_GET_TASK_STATUS_STR(pTask));
    return;
  }

  int32_t size = (int32_t)taosArrayGetSize(pTask->execAddrs);

  if (size <= 0) {
    SCH_TASK_DLOG("task has no exec address, no need to drop it, status:%s", SCH_GET_TASK_STATUS_STR(pTask));
    return;
  }

  SQueryNodeAddr *addr = NULL;
  for (int32_t i = 0; i < size; ++i) {
    addr = (SQueryNodeAddr *)taosArrayGet(pTask->execAddrs, i);

    schBuildAndSendMsg(pJob, pTask, addr, TDMT_VND_DROP_TASK);
  }

  SCH_TASK_DLOG("task has %d exec address", size);
}

void schDropTaskInHashList(SSchJob *pJob, SHashObj *list) {
  if (!SCH_IS_NEED_DROP_JOB(pJob)) {
    return;
  }

  void *pIter = taosHashIterate(list, NULL);
  while (pIter) {
    SSchTask *pTask = *(SSchTask **)pIter;

    schDropTaskOnExecutedNode(pJob, pTask);

    pIter = taosHashIterate(list, pIter);
  }
}

void schDropJobAllTasks(SSchJob *pJob) {
  schDropTaskInHashList(pJob, pJob->execTasks);
  schDropTaskInHashList(pJob, pJob->succTasks);
  schDropTaskInHashList(pJob, pJob->failTasks);
}

int32_t schCancelJob(SSchJob *pJob) {
  // TODO

  // TODO MOVE ALL TASKS FROM EXEC LIST TO FAIL LIST
}

void schFreeJobImpl(void *job) {
  if (NULL == job) {
    return;
  }

  SSchJob *pJob = job;
  uint64_t queryId = pJob->queryId;
  int64_t  refId = pJob->refId;

  if (pJob->status == JOB_TASK_STATUS_EXECUTING) {
    schCancelJob(pJob);
  }

  schDropJobAllTasks(pJob);

  pJob->subPlans = NULL;  // it is a reference to pDag->pSubplans

  int32_t numOfLevels = taosArrayGetSize(pJob->levels);
  for (int32_t i = 0; i < numOfLevels; ++i) {
    SSchLevel *pLevel = taosArrayGet(pJob->levels, i);

    schFreeFlowCtrl(pLevel);

    int32_t numOfTasks = taosArrayGetSize(pLevel->subTasks);
    for (int32_t j = 0; j < numOfTasks; ++j) {
      SSchTask *pTask = taosArrayGet(pLevel->subTasks, j);
      schFreeTask(pTask);
    }

    taosArrayDestroy(pLevel->subTasks);
  }

  taosHashCleanup(pJob->execTasks);
  taosHashCleanup(pJob->failTasks);
  taosHashCleanup(pJob->succTasks);

  taosArrayDestroy(pJob->levels);
  taosArrayDestroy(pJob->nodeList);
  
  tfree(pJob->resData);
  tfree(pJob);

  qDebug("QID:0x%" PRIx64 " job freed, refId:%" PRIx64 ", pointer:%p", queryId, refId, pJob);
}

static int32_t schExecJobImpl(void *transport, SArray *pNodeList, SQueryPlan *pDag, int64_t *job, const char *sql,
                              bool syncSchedule) {
  qDebug("QID:0x%" PRIx64 " job started", pDag->queryId);

  if (pNodeList == NULL || (pNodeList && taosArrayGetSize(pNodeList) <= 0)) {
    qDebug("QID:0x%" PRIx64 " input exec nodeList is empty", pDag->queryId);
  }

  int32_t  code = 0;
  SSchJob *pJob = calloc(1, sizeof(SSchJob));
  if (NULL == pJob) {
    qError("QID:%" PRIx64 " calloc %d failed", pDag->queryId, (int32_t)sizeof(SSchJob));
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  pJob->attr.syncSchedule = syncSchedule;
  pJob->transport = transport;
  pJob->sql = sql;

  if (pNodeList != NULL) {
    pJob->nodeList = taosArrayDup(pNodeList);
  }

  SCH_ERR_JRET(schValidateAndBuildJob(pDag, pJob));

  pJob->execTasks =
      taosHashInit(pDag->numOfSubplans, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == pJob->execTasks) {
    SCH_JOB_ELOG("taosHashInit %d execTasks failed", pDag->numOfSubplans);
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  pJob->succTasks =
      taosHashInit(pDag->numOfSubplans, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == pJob->succTasks) {
    SCH_JOB_ELOG("taosHashInit %d succTasks failed", pDag->numOfSubplans);
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  pJob->failTasks =
      taosHashInit(pDag->numOfSubplans, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == pJob->failTasks) {
    SCH_JOB_ELOG("taosHashInit %d failTasks failed", pDag->numOfSubplans);
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  tsem_init(&pJob->rspSem, 0, 0);

  pJob->refId = taosAddRef(schMgmt.jobRef, pJob);
  if (pJob->refId < 0) {
    SCH_JOB_ELOG("taosHashPut job failed, error:%s", tstrerror(terrno));
    SCH_ERR_JRET(terrno);
  }

  SCH_JOB_DLOG("job refId:%" PRIx64, pJob->refId);

  pJob->status = JOB_TASK_STATUS_NOT_START;
  SCH_ERR_JRET(schLaunchJob(pJob));

  schAcquireJob(pJob->refId);

  *job = pJob->refId;

  if (syncSchedule) {
    SCH_JOB_DLOG("will wait for rsp now, job status:%s", SCH_GET_JOB_STATUS_STR(pJob));
    tsem_wait(&pJob->rspSem);
  }

  SCH_JOB_DLOG("job exec done, job status:%s", SCH_GET_JOB_STATUS_STR(pJob));

  schReleaseJob(pJob->refId);

  return TSDB_CODE_SUCCESS;

_return:

  schFreeJobImpl(pJob);
  SCH_RET(code);
}

int32_t schedulerInit(SSchedulerCfg *cfg) {
  if (schMgmt.jobRef) {
    qError("scheduler already initialized");
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  if (cfg) {
    schMgmt.cfg = *cfg;

    if (schMgmt.cfg.maxJobNum == 0) {
      schMgmt.cfg.maxJobNum = SCHEDULE_DEFAULT_MAX_JOB_NUM;
    }
    if (schMgmt.cfg.maxNodeTableNum <= 0) {
      schMgmt.cfg.maxNodeTableNum = SCHEDULE_DEFAULT_MAX_NODE_TABLE_NUM;
    }
  } else {
    schMgmt.cfg.maxJobNum = SCHEDULE_DEFAULT_MAX_JOB_NUM;
    schMgmt.cfg.maxNodeTableNum = SCHEDULE_DEFAULT_MAX_NODE_TABLE_NUM;
  }

  schMgmt.jobRef = taosOpenRef(schMgmt.cfg.maxJobNum, schFreeJobImpl);
  if (schMgmt.jobRef < 0) {
    qError("init schduler jobRef failed, num:%u", schMgmt.cfg.maxJobNum);
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  schMgmt.hbConnections = taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == schMgmt.hbConnections) {
    qError("taosHashInit hb connections failed");
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  if (taosGetSystemUUID((char *)&schMgmt.sId, sizeof(schMgmt.sId))) {
    qError("generate schdulerId failed, errno:%d", errno);
    SCH_ERR_RET(TSDB_CODE_QRY_SYS_ERROR);
  }

  qInfo("scheduler %" PRIx64 " initizlized, maxJob:%u", schMgmt.sId, schMgmt.cfg.maxJobNum);

  return TSDB_CODE_SUCCESS;
}

int32_t schedulerExecJob(void *transport, SArray *nodeList, SQueryPlan *pDag, int64_t *pJob, const char *sql,
                         SQueryResult *pRes) {
  if (NULL == transport || NULL == pDag || NULL == pDag->pSubplans || NULL == pJob || NULL == pRes) {
    SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SCH_ERR_RET(schExecJobImpl(transport, nodeList, pDag, pJob, sql, true));

  SSchJob *job = schAcquireJob(*pJob);

  pRes->code = atomic_load_32(&job->errCode);
  pRes->numOfRows = job->resNumOfRows;
  
  schReleaseJob(*pJob);

  return TSDB_CODE_SUCCESS;
}

int32_t schedulerAsyncExecJob(void *transport, SArray *pNodeList, SQueryPlan *pDag, const char *sql, int64_t *pJob) {
  if (NULL == transport || NULL == pDag || NULL == pDag->pSubplans || NULL == pJob) {
    SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SCH_ERR_RET(schExecJobImpl(transport, pNodeList, pDag, pJob, sql, false));

  return TSDB_CODE_SUCCESS;
}

#if 0
int32_t schedulerConvertDagToTaskList(SQueryPlan* pDag, SArray **pTasks) {
  if (NULL == pDag || pDag->numOfSubplans <= 0 || LIST_LENGTH(pDag->pSubplans) == 0) {
    SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t levelNum = LIST_LENGTH(pDag->pSubplans);
  if (1 != levelNum) {
    qError("invalid level num: %d", levelNum);
    SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SNodeListNode *plans = (SNodeListNode*)nodesListGetNode(pDag->pSubplans, 0);
  int32_t taskNum = LIST_LENGTH(plans->pNodeList);
  if (taskNum <= 0) {
    qError("invalid task num: %d", taskNum);
    SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SArray *info = taosArrayInit(taskNum, sizeof(STaskInfo));
  if (NULL == info) {
    qError("taosArrayInit %d taskInfo failed", taskNum);
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  STaskInfo tInfo = {0};
  char *msg = NULL;
  int32_t msgLen = 0;
  int32_t code = 0;
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SSubplan *plan = (SSubplan*)nodesListGetNode(plans->pNodeList, i);
    tInfo.addr = plan->execNode;

    code = qSubPlanToString(plan, &msg, &msgLen);
    if (TSDB_CODE_SUCCESS != code) {
      qError("subplanToString error, code:%x, msg:%p, len:%d", code, msg, msgLen);
      SCH_ERR_JRET(code);
    }

    int32_t msgSize = sizeof(SSubQueryMsg) + msgLen;
    if (NULL == msg) {
      qError("calloc %d failed", msgSize);
      SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }
    
    SSubQueryMsg* pMsg = calloc(1, msgSize);
    
    pMsg->header.vgId = tInfo.addr.nodeId;
    
    pMsg->sId      = schMgmt.sId;
    pMsg->queryId  = plan->id.queryId;
    pMsg->taskId   = schGenUUID();
    pMsg->taskType = TASK_TYPE_PERSISTENT;
    pMsg->phyLen   = msgLen;
    pMsg->sqlLen   = 0;
    memcpy(pMsg->msg, msg, msgLen);
    /*memcpy(pMsg->msg, ((SSubQueryMsg*)msg)->msg, msgLen);*/

    tInfo.msg = pMsg;

    if (NULL == taosArrayPush(info, &tInfo)) {
      qError("taosArrayPush failed, idx:%d", i);
      free(msg);
      SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }
  }

  *pTasks = info;
  info = NULL;
  
_return:
  schedulerFreeTaskList(info);
  SCH_RET(code);
}

int32_t schedulerCopyTask(STaskInfo *src, SArray **dst, int32_t copyNum) {
  if (NULL == src || NULL == dst || copyNum <= 0) {
    SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t code = 0;

  *dst = taosArrayInit(copyNum, sizeof(STaskInfo));
  if (NULL == *dst) {
    qError("taosArrayInit %d taskInfo failed", copyNum);
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  int32_t msgSize = src->msg->phyLen + sizeof(*src->msg);
  STaskInfo info = {0};

  info.addr = src->addr;

  for (int32_t i = 0; i < copyNum; ++i) {
    info.msg = malloc(msgSize);
    if (NULL == info.msg) {
      qError("malloc %d failed", msgSize);
      SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    memcpy(info.msg, src->msg, msgSize);

    info.msg->taskId = schGenUUID();

    if (NULL == taosArrayPush(*dst, &info)) {
      qError("taosArrayPush failed, idx:%d", i);
      free(info.msg);
      SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }
  }

  return TSDB_CODE_SUCCESS;

_return:

  schedulerFreeTaskList(*dst);
  *dst = NULL;

  SCH_RET(code);
}
#endif

int32_t schedulerFetchRows(int64_t job, void **pData) {
  if (NULL == pData) {
    SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t  code = 0;
  SSchJob *pJob = schAcquireJob(job);
  if (NULL == pJob) {
    qError("acquire job from jobRef list failed, may be dropped, refId:%" PRIx64, job);
    SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  int8_t status = SCH_GET_JOB_STATUS(pJob);
  if (status == JOB_TASK_STATUS_DROPPING) {
    SCH_JOB_ELOG("job is dropping, status:%s", jobTaskStatusStr(status));
    schReleaseJob(job);
    SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  if (!SCH_JOB_NEED_FETCH(pJob)) {
    SCH_JOB_ELOG("no need to fetch data, status:%s", SCH_GET_JOB_STATUS_STR(pJob));
    schReleaseJob(job);
    SCH_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  if (atomic_val_compare_exchange_8(&pJob->userFetch, 0, 1) != 0) {
    SCH_JOB_ELOG("prior fetching not finished, userFetch:%d", atomic_load_8(&pJob->userFetch));
    schReleaseJob(job);
    SCH_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  if (JOB_TASK_STATUS_FAILED == status || JOB_TASK_STATUS_DROPPING == status) {
    SCH_JOB_ELOG("job failed or dropping, status:%s", jobTaskStatusStr(status));
    SCH_ERR_JRET(atomic_load_32(&pJob->errCode));
  } else if (status == JOB_TASK_STATUS_SUCCEED) {
    SCH_JOB_DLOG("job already succeed, status:%s", jobTaskStatusStr(status));
    goto _return;
  } else if (status == JOB_TASK_STATUS_PARTIAL_SUCCEED) {
    SCH_ERR_JRET(schFetchFromRemote(pJob));
  }

  tsem_wait(&pJob->rspSem);

  status = SCH_GET_JOB_STATUS(pJob);

  if (JOB_TASK_STATUS_FAILED == status || JOB_TASK_STATUS_DROPPING == status) {
    SCH_JOB_ELOG("job failed or dropping, status:%s", jobTaskStatusStr(status));
    SCH_ERR_JRET(atomic_load_32(&pJob->errCode));
  }
  
  if (pJob->resData && ((SRetrieveTableRsp *)pJob->resData)->completed) {
    SCH_ERR_JRET(schCheckAndUpdateJobStatus(pJob, JOB_TASK_STATUS_SUCCEED));
  }

  while (true) {
    *pData = atomic_load_ptr(&pJob->resData);
    if (*pData != atomic_val_compare_exchange_ptr(&pJob->resData, *pData, NULL)) {
      continue;
    }

    break;
  }

  if (NULL == *pData) {
    SRetrieveTableRsp *rsp = (SRetrieveTableRsp *)calloc(1, sizeof(SRetrieveTableRsp));
    if (rsp) {
      rsp->completed = 1;
    }

    *pData = rsp;
    SCH_JOB_DLOG("empty res and set query complete, code:%x", code);
  }

  SCH_JOB_DLOG("fetch done, totalRows:%d, code:%s", pJob->resNumOfRows, tstrerror(code));

_return:

  atomic_val_compare_exchange_8(&pJob->userFetch, 1, 0);

  schReleaseJob(job);

  SCH_RET(code);
}

int32_t scheduleCancelJob(int64_t job) {
  SSchJob *pJob = schAcquireJob(job);
  if (NULL == pJob) {
    qError("acquire job from jobRef list failed, may be dropped, refId:%" PRIx64, job);
    SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  int32_t code = schCancelJob(pJob);

  schReleaseJob(job);

  SCH_RET(code);
}

void schedulerFreeJob(int64_t job) {
  SSchJob *pJob = schAcquireJob(job);
  if (NULL == pJob) {
    qError("acquire job from jobRef list failed, may be dropped, refId:%" PRIx64, job);
    return;
  }

  if (atomic_load_8(&pJob->userFetch) > 0) {
    schProcessOnJobDropped(pJob, TSDB_CODE_QRY_JOB_FREED);
  }

  SCH_JOB_DLOG("start to remove job from jobRef list, refId:%" PRIx64, job);

  if (taosRemoveRef(schMgmt.jobRef, job)) {
    SCH_JOB_ELOG("remove job from job list failed, refId:%" PRIx64, job);
  }

  schReleaseJob(job);
}

void schedulerFreeTaskList(SArray *taskList) {
  if (NULL == taskList) {
    return;
  }

  int32_t taskNum = taosArrayGetSize(taskList);
  for (int32_t i = 0; i < taskNum; ++i) {
    STaskInfo *info = taosArrayGet(taskList, i);
    tfree(info->msg);
  }

  taosArrayDestroy(taskList);
}

void schedulerDestroy(void) {
  if (schMgmt.jobRef) {
    SSchJob *pJob = taosIterateRef(schMgmt.jobRef, 0);

    while (pJob) {
      taosRemoveRef(schMgmt.jobRef, pJob->refId);

      pJob = taosIterateRef(schMgmt.jobRef, pJob->refId);
    }

    taosCloseRef(schMgmt.jobRef);
    schMgmt.jobRef = 0;
  }
}

