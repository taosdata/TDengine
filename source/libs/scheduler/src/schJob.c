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

FORCE_INLINE SSchJob *schAcquireJob(int64_t refId) { return (SSchJob *)taosAcquireRef(schMgmt.jobRef, refId); }

FORCE_INLINE int32_t schReleaseJob(int64_t refId) { return taosReleaseRef(schMgmt.jobRef, refId); }

int32_t schInitTask(SSchJob *pJob, SSchTask *pTask, SSubplan *pPlan, SSchLevel *pLevel) {
  pTask->plan = pPlan;
  pTask->level = pLevel;
  SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_NOT_START);
  pTask->taskId = schGenTaskId();
  pTask->execNodes = taosArrayInit(SCH_MAX_CANDIDATE_EP_NUM, sizeof(SSchNodeInfo));
  if (NULL == pTask->execNodes) {
    SCH_TASK_ELOG("taosArrayInit %d execNodes failed", SCH_MAX_CANDIDATE_EP_NUM);
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schInitJob(SSchJob **pSchJob, SQueryPlan *pDag, void *pTrans, SArray *pNodeList, const char *sql,
                   SSchResInfo *pRes, int64_t startTs, bool syncSchedule) {
  int32_t  code = 0;
  int64_t  refId = -1;
  SSchJob *pJob = taosMemoryCalloc(1, sizeof(SSchJob));
  if (NULL == pJob) {
    qError("QID:%" PRIx64 " calloc %d failed", pDag->queryId, (int32_t)sizeof(SSchJob));
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  pJob->attr.explainMode = pDag->explainInfo.mode;
  pJob->attr.syncSchedule = syncSchedule;
  pJob->pTrans = pTrans;
  pJob->sql = sql;
  if (pRes) {
    pJob->userRes = *pRes;
  }
  
  if (pNodeList != NULL) {
    pJob->nodeList = taosArrayDup(pNodeList);
  }

  SCH_ERR_JRET(schValidateAndBuildJob(pDag, pJob));

  if (SCH_IS_EXPLAIN_JOB(pJob)) {
    SCH_ERR_JRET(qExecExplainBegin(pDag, &pJob->explainCtx, startTs));
  }

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

  refId = taosAddRef(schMgmt.jobRef, pJob);
  if (refId < 0) {
    SCH_JOB_ELOG("taosAddRef job failed, error:%s", tstrerror(terrno));
    SCH_ERR_JRET(terrno);
  }

  atomic_add_fetch_32(&schMgmt.jobNum, 1);

  if (NULL == schAcquireJob(refId)) {
    SCH_JOB_ELOG("schAcquireJob job failed, refId:%" PRIx64, refId);
    SCH_ERR_JRET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  pJob->refId = refId;

  SCH_JOB_DLOG("job refId:%" PRIx64, pJob->refId);

  pJob->status = JOB_TASK_STATUS_NOT_START;

  *pSchJob = pJob;

  return TSDB_CODE_SUCCESS;

_return:

  if (refId < 0) {
    schFreeJobImpl(pJob);
  } else {
    taosRemoveRef(schMgmt.jobRef, refId);
  }
  SCH_RET(code);
}

void schFreeTask(SSchTask *pTask) {
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
    taosArrayDestroy(pTask->execNodes);
  }
}

FORCE_INLINE bool schJobNeedToStop(SSchJob *pJob, int8_t *pStatus) {
  int8_t status = SCH_GET_JOB_STATUS(pJob);
  if (pStatus) {
    *pStatus = status;
  }

  return (status == JOB_TASK_STATUS_FAILED || status == JOB_TASK_STATUS_CANCELLED ||
          status == JOB_TASK_STATUS_CANCELLING || status == JOB_TASK_STATUS_DROPPING ||
          status == JOB_TASK_STATUS_SUCCEED);
}

int32_t schChkUpdateJobStatus(SSchJob *pJob, int8_t newStatus) {
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
  return TSDB_CODE_SUCCESS;
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

        SCH_TASK_DLOG("children info, the %d child TID %" PRIx64, n, (*childTask)->taskId);
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

        SCH_TASK_DLOG("parents info, the %d parent TID %" PRIx64, n, (*parentTask)->taskId);        
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

int32_t schRecordTaskExecNode(SSchJob *pJob, SSchTask *pTask, SQueryNodeAddr *addr, void *handle) {
  SSchNodeInfo nodeInfo = {.addr = *addr, .handle = handle};

  if (NULL == taosArrayPush(pTask->execNodes, &nodeInfo)) {
    SCH_TASK_ELOG("taosArrayPush nodeInfo to execNodes list failed, errno:%d", errno);
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SCH_TASK_DLOG("task execNode recorded, handle:%p", handle);

  return TSDB_CODE_SUCCESS;
}

int32_t schRecordQueryDataSrc(SSchJob *pJob, SSchTask *pTask) {
  if (!SCH_IS_DATA_SRC_QRY_TASK(pTask)) {
    return TSDB_CODE_SUCCESS;
  }

  taosArrayPush(pJob->dataSrcTasks, &pTask);

  return TSDB_CODE_SUCCESS;
}


int32_t schValidateAndBuildJob(SQueryPlan *pDag, SSchJob *pJob) {
  int32_t code = 0;
  pJob->queryId = pDag->queryId;

  if (pDag->numOfSubplans <= 0) {
    SCH_JOB_ELOG("invalid subplan num:%d", pDag->numOfSubplans);
    SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  pJob->dataSrcTasks = taosArrayInit(pDag->numOfSubplans, POINTER_BYTES);
  if (NULL == pJob->dataSrcTasks) {
    SCH_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
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

      SCH_ERR_JRET(schRecordQueryDataSrc(pJob, p));

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

int32_t schRemoveTaskFromExecList(SSchJob *pJob, SSchTask *pTask) {
  int32_t code = taosHashRemove(pJob->execTasks, &pTask->taskId, sizeof(pTask->taskId));
  if (code) {
    SCH_TASK_ELOG("task failed to rm from execTask list, code:%x", code);
    SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
  }

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
      SCH_TASK_DLOG("task no more retry since all ep tried, tryTimes:%d, epNum:%d", pTask->tryTimes,
                    SCH_TASK_NUM_OF_EPS(&pTask->plan->execNode));
      return TSDB_CODE_SUCCESS;
    }
  } else {
    int32_t candidateNum = taosArrayGetSize(pTask->candidateAddrs);

    if ((pTask->candidateIdx + 1) >= candidateNum) {
      *needRetry = false;
      SCH_TASK_DLOG("task no more retry since all candiates tried, candidateIdx:%d, candidateNum:%d",
                    pTask->candidateIdx, candidateNum);
      return TSDB_CODE_SUCCESS;
    }
  }

  *needRetry = true;
  SCH_TASK_DLOG("task need the %dth retry, errCode:%x - %s", pTask->tryTimes, errCode, tstrerror(errCode));

  return TSDB_CODE_SUCCESS;
}

int32_t schHandleTaskRetry(SSchJob *pJob, SSchTask *pTask) {
  atomic_sub_fetch_32(&pTask->level->taskLaunchedNum, 1);

  SCH_ERR_RET(schRemoveTaskFromExecList(pJob, pTask));
  SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_NOT_START);
  
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


int32_t schSetJobQueryRes(SSchJob* pJob, SQueryResult* pRes) {
  pRes->code = atomic_load_32(&pJob->errCode);
  pRes->numOfRows = pJob->resNumOfRows;
  pRes->res = pJob->queryRes;
  pJob->queryRes = NULL;

  return TSDB_CODE_SUCCESS;
}

int32_t schSetJobFetchRes(SSchJob* pJob, void** pData) {
  int32_t code = 0;
  if (pJob->resData && ((SRetrieveTableRsp *)pJob->resData)->completed) {
    SCH_ERR_RET(schChkUpdateJobStatus(pJob, JOB_TASK_STATUS_SUCCEED));
  }

  while (true) {
    *pData = atomic_load_ptr(&pJob->resData);
    if (*pData != atomic_val_compare_exchange_ptr(&pJob->resData, *pData, NULL)) {
      continue;
    }

    break;
  }

  if (NULL == *pData) {
    SRetrieveTableRsp *rsp = (SRetrieveTableRsp *)taosMemoryCalloc(1, sizeof(SRetrieveTableRsp));
    if (rsp) {
      rsp->completed = 1;
    }

    *pData = rsp;
    SCH_JOB_DLOG("empty res and set query complete, code:%x", code);
  }

  SCH_JOB_DLOG("fetch done, totalRows:%d", pJob->resNumOfRows);

  return TSDB_CODE_SUCCESS;
}

int32_t schNotifyUserQueryRes(SSchJob* pJob) {
  pJob->userRes.queryRes = taosMemoryCalloc(1, sizeof(*pJob->userRes.queryRes));
  if (pJob->userRes.queryRes) {
    schSetJobQueryRes(pJob, pJob->userRes.queryRes);
  }

  (*pJob->userRes.execFp)(pJob->userRes.queryRes, pJob->userRes.userParam, atomic_load_32(&pJob->errCode));

  pJob->userRes.queryRes = NULL;

  return TSDB_CODE_SUCCESS;
}

int32_t schNotifyUserFetchRes(SSchJob* pJob) {
  void* pRes = NULL;
  
  SCH_ERR_RET(schSetJobFetchRes(pJob, &pRes));

  (*pJob->userRes.fetchFp)(pRes, pJob->userRes.userParam, atomic_load_32(&pJob->errCode));

  return TSDB_CODE_SUCCESS;
}

int32_t schProcessOnJobFailureImpl(SSchJob *pJob, int32_t status, int32_t errCode) {
  // if already FAILED, no more processing
  SCH_ERR_RET(schChkUpdateJobStatus(pJob, status));

  schUpdateJobErrCode(pJob, errCode);

  if (atomic_load_8(&pJob->userFetch) || pJob->attr.syncSchedule) {
    tsem_post(&pJob->rspSem);
  }

  int32_t code = atomic_load_32(&pJob->errCode);

  SCH_JOB_DLOG("job failed with error: %s", tstrerror(code));

  if (!pJob->attr.syncSchedule) {
    if (SCH_EXEC_CB == atomic_val_compare_exchange_32(&pJob->userCb, SCH_EXEC_CB, 0)) {
      schNotifyUserQueryRes(pJob);
    } else if (SCH_FETCH_CB == atomic_val_compare_exchange_32(&pJob->userCb, SCH_FETCH_CB, 0)) {
      schNotifyUserFetchRes(pJob);
    }
  }

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

  SCH_ERR_RET(schChkUpdateJobStatus(pJob, JOB_TASK_STATUS_PARTIAL_SUCCEED));

  if (pJob->attr.syncSchedule) {
    tsem_post(&pJob->rspSem);
  } else if (SCH_EXEC_CB == atomic_val_compare_exchange_32(&pJob->userCb, SCH_EXEC_CB, 0)) {
    schNotifyUserQueryRes(pJob);
  } else if (SCH_FETCH_CB == atomic_val_compare_exchange_32(&pJob->userCb, SCH_FETCH_CB, 0)) {
    schNotifyUserFetchRes(pJob);
  }

  if (atomic_load_8(&pJob->userFetch)) {
    SCH_ERR_JRET(schFetchFromRemote(pJob));
  }

  return TSDB_CODE_SUCCESS;

_return:

  SCH_RET(schProcessOnJobFailure(pJob, code));
}

void schProcessOnDataFetched(SSchJob *job) {
  atomic_val_compare_exchange_32(&job->remoteFetch, 1, 0);

  if (job->attr.syncSchedule) {
    tsem_post(&job->rspSem);
  } else if (SCH_FETCH_CB == atomic_val_compare_exchange_32(&job->userCb, SCH_FETCH_CB, 0)) {
    schNotifyUserFetchRes(job);
  }
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

int32_t schLaunchNextLevelTasks(SSchJob *pJob, SSchTask *pTask) {
  if (!SCH_IS_QUERY_JOB(pJob)) {
    return TSDB_CODE_SUCCESS;
  }

  SSchLevel *pLevel = pTask->level;
  int32_t doneNum = atomic_add_fetch_32(&pLevel->taskDoneNum, 1);
  if (doneNum == pLevel->taskNum) {
    pJob->levelIdx--;

    pLevel = taosArrayGet(pJob->levels, pJob->levelIdx);
    for (int32_t i = 0; i < pLevel->taskNum; ++i) {
      SSchTask *pTask = taosArrayGet(pLevel->subTasks, i);

      if (pTask->children && taosArrayGetSize(pTask->children) > 0) {
        continue;
      }
      
      SCH_ERR_RET(schLaunchTask(pJob, pTask));
    }
  }

  return TSDB_CODE_SUCCESS;
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

    if (SCH_TASK_READY_FOR_LAUNCH(readyNum, par)) {
      SCH_ERR_RET(schLaunchTask(pJob, par));
    }
  }

  SCH_ERR_RET(schLaunchNextLevelTasks(pJob, pTask));

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

int32_t schProcessOnExplainDone(SSchJob *pJob, SSchTask *pTask, SRetrieveTableRsp *pRsp) {
  SCH_TASK_DLOG("got explain rsp, rows:%d, complete:%d", htonl(pRsp->numOfRows), pRsp->completed);

  atomic_store_32(&pJob->resNumOfRows, htonl(pRsp->numOfRows));
  atomic_store_ptr(&pJob->resData, pRsp);

  SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_SUCCEED);

  schProcessOnDataFetched(pJob);

  return TSDB_CODE_SUCCESS;
}

int32_t schSaveJobQueryRes(SSchJob *pJob, SQueryTableRsp *rsp) {
  if (rsp->tbFName[0]) {
    if (NULL == pJob->queryRes) {
      pJob->queryRes = taosArrayInit(pJob->taskNum, sizeof(STbVerInfo));
      if (NULL == pJob->queryRes) {
        SCH_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
      }
    }

    STbVerInfo tbInfo;
    strcpy(tbInfo.tbFName, rsp->tbFName);
    tbInfo.sversion = rsp->sversion;
    tbInfo.tversion = rsp->tversion;

    taosArrayPush((SArray *)pJob->queryRes, &tbInfo);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schGetTaskFromTaskList(SHashObj *pTaskList, uint64_t taskId, SSchTask **pTask) {
  int32_t s = taosHashGetSize(pTaskList);
  if (s <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SSchTask **task = taosHashGet(pTaskList, &taskId, sizeof(taskId));
  if (NULL == task || NULL == (*task)) {
    return TSDB_CODE_SUCCESS;
  }

  *pTask = *task;

  return TSDB_CODE_SUCCESS;
}

int32_t schUpdateTaskExecNodeHandle(SSchTask *pTask, void *handle, int32_t rspCode) {
  if (rspCode || NULL == pTask->execNodes || taosArrayGetSize(pTask->execNodes) > 1 ||
      taosArrayGetSize(pTask->execNodes) <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SSchNodeInfo *nodeInfo = taosArrayGet(pTask->execNodes, 0);
  nodeInfo->handle = handle;

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
  for (int32_t i = 0; i < level->taskNum; ++i) {
    SSchTask *pTask = taosArrayGet(level->subTasks, i);

    SCH_ERR_RET(schLaunchTask(pJob, pTask));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schLaunchJob(SSchJob *pJob) {
  SSchLevel *level = taosArrayGet(pJob->levels, pJob->levelIdx);

  SCH_ERR_RET(schChkUpdateJobStatus(pJob, JOB_TASK_STATUS_EXECUTING));

  SCH_ERR_RET(schChkJobNeedFlowCtrl(pJob, level));

  SCH_ERR_RET(schLaunchLevelTasks(pJob, level));

  return TSDB_CODE_SUCCESS;
}

void schDropTaskOnExecNode(SSchJob *pJob, SSchTask *pTask) {
  if (NULL == pTask->execNodes) {
    SCH_TASK_DLOG("no exec address, status:%s", SCH_GET_TASK_STATUS_STR(pTask));
    return;
  }

  int32_t size = (int32_t)taosArrayGetSize(pTask->execNodes);

  if (size <= 0) {
    SCH_TASK_DLOG("task has no execNodes, no need to drop it, status:%s", SCH_GET_TASK_STATUS_STR(pTask));
    return;
  }

  SSchNodeInfo *nodeInfo = NULL;
  for (int32_t i = 0; i < size; ++i) {
    nodeInfo = (SSchNodeInfo *)taosArrayGet(pTask->execNodes, i);
    SCH_SET_TASK_HANDLE(pTask, nodeInfo->handle);

    schBuildAndSendMsg(pJob, pTask, &nodeInfo->addr, TDMT_VND_DROP_TASK);
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

    schDropTaskOnExecNode(pJob, pTask);

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
  return TSDB_CODE_SUCCESS;
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

    int32_t numOfTasks = taosArrayGetSize(pLevel->subTasks);
    for (int32_t j = 0; j < numOfTasks; ++j) {
      SSchTask *pTask = taosArrayGet(pLevel->subTasks, j);
      schFreeTask(pTask);
    }

    taosArrayDestroy(pLevel->subTasks);
  }

  schFreeFlowCtrl(pJob);

  taosHashCleanup(pJob->execTasks);
  taosHashCleanup(pJob->failTasks);
  taosHashCleanup(pJob->succTasks);

  taosArrayDestroy(pJob->levels);
  taosArrayDestroy(pJob->nodeList);
  taosArrayDestroy(pJob->dataSrcTasks);

  qExplainFreeCtx(pJob->explainCtx);

  if (SCH_IS_QUERY_JOB(pJob)) {
    taosArrayDestroy((SArray *)pJob->queryRes);
  } else {
    tFreeSSubmitRsp((SSubmitRsp*)pJob->queryRes);
  }

  taosMemoryFreeClear(pJob->userRes.queryRes);
  taosMemoryFreeClear(pJob->resData);
  taosMemoryFreeClear(pJob);

  qDebug("QID:0x%" PRIx64 " job freed, refId:%" PRIx64 ", pointer:%p", queryId, refId, pJob);

  atomic_sub_fetch_32(&schMgmt.jobNum, 1);

  schCloseJobRef();
}

int32_t schExecJobImpl(void *pTrans, SArray *pNodeList, SQueryPlan *pDag, int64_t *job, const char *sql,
                              SSchResInfo *pRes, int64_t startTs, bool sync) {
  qDebug("QID:0x%" PRIx64 " job started", pDag->queryId);

  if (pNodeList == NULL || taosArrayGetSize(pNodeList) <= 0) {
    qDebug("QID:0x%" PRIx64 " input exec nodeList is empty", pDag->queryId);
  }

  int32_t  code = 0;
  SSchJob *pJob = NULL;
  SCH_ERR_RET(schInitJob(&pJob, pDag, pTrans, pNodeList, sql, pRes, startTs, sync));

  *job = pJob->refId;

  SCH_ERR_JRET(schLaunchJob(pJob));

  if (sync) {
    SCH_JOB_DLOG("will wait for rsp now, job status:%s", SCH_GET_JOB_STATUS_STR(pJob));
    tsem_wait(&pJob->rspSem);
  } else {
    pJob->userCb = SCH_EXEC_CB; 
  }

  SCH_JOB_DLOG("job exec done, job status:%s", SCH_GET_JOB_STATUS_STR(pJob));

_return:

  schReleaseJob(pJob->refId);
  
  SCH_RET(code);
}

int32_t schExecJob(void *pTrans, SArray *pNodeList, SQueryPlan *pDag, int64_t *pJob, const char *sql,
                         int64_t startTs, SSchResInfo *pRes) {
  int32_t code = 0;
  
  *pJob = 0;
  
  if (EXPLAIN_MODE_STATIC == pDag->explainInfo.mode) {
    SCH_ERR_JRET(schExecStaticExplainJob(pTrans, pNodeList, pDag, pJob, sql, NULL, true));
  } else {
    SCH_ERR_JRET(schExecJobImpl(pTrans, pNodeList, pDag, pJob, sql, NULL, startTs, true));
  }

_return:

  if (*pJob) {
    SSchJob *job = schAcquireJob(*pJob);
    schSetJobQueryRes(job, pRes->queryRes);
    schReleaseJob(*pJob);
  }

  return code;
}

int32_t schAsyncExecJob(void *pTrans, SArray *pNodeList, SQueryPlan *pDag, int64_t *pJob, const char *sql,
                                int64_t startTs, SSchResInfo *pRes) {
  int32_t code = 0;

  *pJob = 0;

  if (EXPLAIN_MODE_STATIC == pDag->explainInfo.mode) {
    SCH_ERR_RET(schExecStaticExplainJob(pTrans, pNodeList, pDag, pJob, sql, pRes, false));
  } else {
    SCH_ERR_RET(schExecJobImpl(pTrans, pNodeList, pDag, pJob, sql, pRes, startTs, false));
  }

  return code;
}

int32_t schExecStaticExplainJob(void *pTrans, SArray *pNodeList, SQueryPlan *pDag, int64_t *job, const char *sql,
                             SSchResInfo *pRes, bool sync) {
  qDebug("QID:0x%" PRIx64 " job started", pDag->queryId);

  int32_t  code = 0;
  SSchJob *pJob = taosMemoryCalloc(1, sizeof(SSchJob));
  if (NULL == pJob) {
    qError("QID:%" PRIx64 " calloc %d failed", pDag->queryId, (int32_t)sizeof(SSchJob));
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  pJob->sql = sql;
  pJob->attr.queryJob = true;
  pJob->attr.syncSchedule = sync;
  pJob->attr.explainMode = pDag->explainInfo.mode;
  pJob->queryId = pDag->queryId;
  pJob->subPlans = pDag->pSubplans;
  if (pRes) {
    pJob->userRes = *pRes;
  }
  
  SCH_ERR_JRET(qExecStaticExplain(pDag, (SRetrieveTableRsp **)&pJob->resData));

  int64_t refId = taosAddRef(schMgmt.jobRef, pJob);
  if (refId < 0) {
    SCH_JOB_ELOG("taosAddRef job failed, error:%s", tstrerror(terrno));
    SCH_ERR_JRET(terrno);
  }

  if (NULL == schAcquireJob(refId)) {
    SCH_JOB_ELOG("schAcquireJob job failed, refId:%" PRIx64, refId);
    SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  pJob->refId = refId;

  SCH_JOB_DLOG("job refId:%" PRIx64, pJob->refId);

  pJob->status = JOB_TASK_STATUS_PARTIAL_SUCCEED;
  
  *job = pJob->refId;
  SCH_JOB_DLOG("job exec done, job status:%s", SCH_GET_JOB_STATUS_STR(pJob));

  if (!pJob->attr.syncSchedule) {
    code = schNotifyUserQueryRes(pJob);
  }

  schReleaseJob(pJob->refId);

  SCH_RET(code);

_return:

  schFreeJobImpl(pJob);
  SCH_RET(code);
}

int32_t schFetchRows(SSchJob *pJob) {
  int32_t  code = 0;

  int8_t status = SCH_GET_JOB_STATUS(pJob);
  if (status == JOB_TASK_STATUS_DROPPING) {
    SCH_JOB_ELOG("job is dropping, status:%s", jobTaskStatusStr(status));
    SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  if (!SCH_JOB_NEED_FETCH(pJob)) {
    SCH_JOB_ELOG("no need to fetch data, status:%s", SCH_GET_JOB_STATUS_STR(pJob));
    SCH_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  if (atomic_val_compare_exchange_8(&pJob->userFetch, 0, 1) != 0) {
    SCH_JOB_ELOG("prior fetching not finished, userFetch:%d", atomic_load_8(&pJob->userFetch));
    SCH_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  if (JOB_TASK_STATUS_FAILED == status || JOB_TASK_STATUS_DROPPING == status) {
    SCH_JOB_ELOG("job failed or dropping, status:%s", jobTaskStatusStr(status));
    SCH_ERR_JRET(atomic_load_32(&pJob->errCode));
  } else if (status == JOB_TASK_STATUS_SUCCEED) {
    SCH_JOB_DLOG("job already succeed, status:%s", jobTaskStatusStr(status));
    goto _return;
  } else if (status != JOB_TASK_STATUS_PARTIAL_SUCCEED) {
    SCH_JOB_ELOG("job status error for fetch, status:%s", jobTaskStatusStr(status));
    SCH_ERR_JRET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  if (!(pJob->attr.explainMode == EXPLAIN_MODE_STATIC)) {
    SCH_ERR_JRET(schFetchFromRemote(pJob));
    tsem_wait(&pJob->rspSem);
  
    status = SCH_GET_JOB_STATUS(pJob);      
    if (JOB_TASK_STATUS_FAILED == status || JOB_TASK_STATUS_DROPPING == status) {
      SCH_JOB_ELOG("job failed or dropping, status:%s", jobTaskStatusStr(status));
      SCH_ERR_JRET(atomic_load_32(&pJob->errCode));
    }
  }

  SCH_ERR_JRET(schSetJobFetchRes(pJob, pJob->userRes.fetchRes));

_return:

  atomic_val_compare_exchange_8(&pJob->userFetch, 1, 0);

  SCH_RET(code);
}

int32_t schAsyncFetchRows(SSchJob *pJob) {
  int32_t  code = 0;

  int8_t status = SCH_GET_JOB_STATUS(pJob);
  if (status == JOB_TASK_STATUS_DROPPING) {
    SCH_JOB_ELOG("job is dropping, status:%s", jobTaskStatusStr(status));
    SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  if (!SCH_JOB_NEED_FETCH(pJob)) {
    SCH_JOB_ELOG("no need to fetch data, status:%s", SCH_GET_JOB_STATUS_STR(pJob));
    SCH_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  if (atomic_val_compare_exchange_8(&pJob->userFetch, 0, 1) != 0) {
    SCH_JOB_ELOG("prior fetching not finished, userFetch:%d", atomic_load_8(&pJob->userFetch));
    SCH_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  if (JOB_TASK_STATUS_FAILED == status || JOB_TASK_STATUS_DROPPING == status) {
    SCH_JOB_ELOG("job failed or dropping, status:%s", jobTaskStatusStr(status));
    SCH_ERR_JRET(atomic_load_32(&pJob->errCode));
  } else if (status == JOB_TASK_STATUS_SUCCEED) {
    SCH_JOB_DLOG("job already succeed, status:%s", jobTaskStatusStr(status));
    goto _return;
  } else if (status != JOB_TASK_STATUS_PARTIAL_SUCCEED) {
    SCH_JOB_ELOG("job status error for fetch, status:%s", jobTaskStatusStr(status));
    SCH_ERR_JRET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  if (pJob->attr.explainMode == EXPLAIN_MODE_STATIC) {
    SCH_ERR_JRET(schNotifyUserFetchRes(pJob));
    
    atomic_val_compare_exchange_8(&pJob->userFetch, 1, 0);
  } else {
    pJob->userCb = SCH_FETCH_CB;
    
    SCH_ERR_JRET(schFetchFromRemote(pJob));
  }

  return TSDB_CODE_SUCCESS;

_return:

  atomic_val_compare_exchange_8(&pJob->userFetch, 1, 0);

  SCH_RET(code);
}


