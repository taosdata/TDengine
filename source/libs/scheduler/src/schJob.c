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

FORCE_INLINE SSchJob *schAcquireJob(int64_t refId) { qDebug("sch acquire jobId:0x%"PRIx64, refId); return (SSchJob *)taosAcquireRef(schMgmt.jobRef, refId); }

FORCE_INLINE int32_t schReleaseJob(int64_t refId) { qDebug("sch release jobId:0x%"PRIx64, refId); return taosReleaseRef(schMgmt.jobRef, refId); }

int32_t schInitTask(SSchJob *pJob, SSchTask *pTask, SSubplan *pPlan, SSchLevel *pLevel) {
  pTask->plan = pPlan;
  pTask->level = pLevel;
  pTask->execId = -1;
  pTask->maxExecTimes = SCH_TASK_MAX_EXEC_TIMES;
  pTask->timeoutUsec = SCH_DEFAULT_TASK_TIMEOUT_USEC;
  SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_NOT_START);
  pTask->taskId = schGenTaskId();
  pTask->execNodes = taosHashInit(SCH_MAX_CANDIDATE_EP_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (NULL == pTask->execNodes) {
    SCH_TASK_ELOG("taosHashInit %d execNodes failed", SCH_MAX_CANDIDATE_EP_NUM);
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schInitJob(SSchedulerReq *pReq, SSchJob **pSchJob) {
  int32_t  code = 0;
  int64_t  refId = -1;
  SSchJob *pJob = taosMemoryCalloc(1, sizeof(SSchJob));
  if (NULL == pJob) {
    qError("QID:0x%" PRIx64 " calloc %d failed", pReq->pDag->queryId, (int32_t)sizeof(SSchJob));
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  pJob->attr.explainMode = pReq->pDag->explainInfo.mode;
  pJob->conn = *pReq->pConn;
  pJob->sql = pReq->sql;
  pJob->pDag = pReq->pDag;
  pJob->chkKillFp = pReq->chkKillFp;
  pJob->chkKillParam = pReq->chkKillParam;
  pJob->userRes.execFp = pReq->execFp;
  pJob->userRes.userParam = pReq->execParam;

  if (pReq->pNodeList == NULL || taosArrayGetSize(pReq->pNodeList) <= 0) {
    qDebug("QID:0x%" PRIx64 " input exec nodeList is empty", pReq->pDag->queryId);
  } else {
    pJob->nodeList = taosArrayDup(pReq->pNodeList);
  }
  
  pJob->taskList =
      taosHashInit(pReq->pDag->numOfSubplans, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == pJob->taskList) {
    SCH_JOB_ELOG("taosHashInit %d taskList failed", pReq->pDag->numOfSubplans);
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SCH_ERR_JRET(schValidateAndBuildJob(pReq->pDag, pJob));

  if (SCH_IS_EXPLAIN_JOB(pJob)) {
    SCH_ERR_JRET(qExecExplainBegin(pReq->pDag, &pJob->explainCtx, pReq->startTs));
  }

  pJob->execTasks =
      taosHashInit(pReq->pDag->numOfSubplans, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == pJob->execTasks) {
    SCH_JOB_ELOG("taosHashInit %d execTasks failed", pReq->pDag->numOfSubplans);
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
    SCH_JOB_ELOG("schAcquireJob job failed, refId:0x%" PRIx64, refId);
    SCH_ERR_JRET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  pJob->refId = refId;

  SCH_JOB_DLOG("job refId:0x%" PRIx64" created", pJob->refId);

  schUpdateJobStatus(pJob, JOB_TASK_STATUS_NOT_START);

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



FORCE_INLINE bool schJobNeedToStop(SSchJob *pJob, int8_t *pStatus) {
  int8_t status = SCH_GET_JOB_STATUS(pJob);
  if (pStatus) {
    *pStatus = status;
  }

  if ((*pJob->chkKillFp)(pJob->chkKillParam)) {
    schUpdateJobStatus(pJob, JOB_TASK_STATUS_DROPPING);
    schUpdateJobErrCode(pJob, TSDB_CODE_TSC_QUERY_KILLED);

    return true;
  }  

  return (status == JOB_TASK_STATUS_FAILED || status == JOB_TASK_STATUS_DROPPING ||
          status == JOB_TASK_STATUS_SUCCEED);
}

int32_t schUpdateJobStatus(SSchJob *pJob, int8_t newStatus) {
  int32_t code = 0;

  int8_t oriStatus = 0;

  while (true) {
    oriStatus = SCH_GET_JOB_STATUS(pJob);

    if (oriStatus == newStatus) {
      if (newStatus == JOB_TASK_STATUS_DROPPING) {
        SCH_ERR_JRET(TSDB_CODE_SCH_JOB_IS_DROPPING);
      }
      
      SCH_ERR_JRET(TSDB_CODE_SCH_IGNORE_ERROR);
    }

    switch (oriStatus) {
      case JOB_TASK_STATUS_NULL:
        if (newStatus != JOB_TASK_STATUS_NOT_START) {
          SCH_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
        }

        break;
      case JOB_TASK_STATUS_NOT_START:
        if (newStatus != JOB_TASK_STATUS_EXECUTING && newStatus != JOB_TASK_STATUS_DROPPING) {
          SCH_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
        }

        break;
      case JOB_TASK_STATUS_EXECUTING:
        if (newStatus != JOB_TASK_STATUS_PARTIAL_SUCCEED && newStatus != JOB_TASK_STATUS_FAILED &&
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
        if (newStatus != JOB_TASK_STATUS_DROPPING) {
          SCH_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
        }

        break;
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
  SCH_RET(code);
}


void schEndOperation(SSchJob *pJob) {
  int32_t op = atomic_load_32(&pJob->opStatus.op);
  if (SCH_OP_NULL == op) {
    SCH_JOB_DLOG("job already not in any operation, status:%s", jobTaskStatusStr(pJob->status));
    return;
  }

  atomic_store_32(&pJob->opStatus.op, SCH_OP_NULL);
  
  SCH_JOB_DLOG("job end %s operation", schGetOpStr(op));
}

int32_t schBeginOperation(SSchJob *pJob, SCH_OP_TYPE type, bool sync) {
  int32_t code = 0;
  int8_t status = 0;
  
  if (schJobNeedToStop(pJob, &status)) {
    SCH_JOB_ELOG("abort op %s cause of job need to stop", schGetOpStr(type));
    SCH_ERR_JRET(pJob->errCode);
  }
      
  if (SCH_OP_NULL != atomic_val_compare_exchange_32(&pJob->opStatus.op, SCH_OP_NULL, type)) {
    SCH_JOB_ELOG("job already in %s operation", schGetOpStr(pJob->opStatus.op));
    SCH_ERR_JRET(TSDB_CODE_TSC_APP_ERROR);
  }

  SCH_JOB_DLOG("job start %s operation", schGetOpStr(pJob->opStatus.op));

  pJob->opStatus.sync = sync;

  switch (type) {
    case SCH_OP_EXEC:
      SCH_ERR_JRET(schUpdateJobStatus(pJob, JOB_TASK_STATUS_EXECUTING));
      break;
    case SCH_OP_FETCH:
      if (!SCH_JOB_NEED_FETCH(pJob)) {
        SCH_JOB_ELOG("no need to fetch data, status:%s", SCH_GET_JOB_STATUS_STR(pJob));
        SCH_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }

      if (status != JOB_TASK_STATUS_PARTIAL_SUCCEED) {
        SCH_JOB_ELOG("job status error for fetch, status:%s", jobTaskStatusStr(status));
        SCH_ERR_JRET(TSDB_CODE_SCH_STATUS_ERROR);
      }
      break;
    default:
      SCH_JOB_ELOG("unknown operation type %d", type);
      SCH_ERR_JRET(TSDB_CODE_TSC_APP_ERROR);
  }

  return TSDB_CODE_SUCCESS;

_return:

  schEndOperation(pJob);

  SCH_RET(code);
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

        SCH_TASK_DLOG("children info, the %d child TID 0x%" PRIx64, n, (*childTask)->taskId);
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

        SCH_TASK_DLOG("parents info, the %d parent TID 0x%" PRIx64, n, (*parentTask)->taskId);        
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
      SCH_ERR_JRET(schInitTask(pJob, &task, plan, pLevel));

      SSchTask *pTask = taosArrayPush(pLevel->subTasks, &task);
      if (NULL == pTask) {
        SCH_TASK_ELOG("taosArrayPush task to level failed, level:%d, taskIdx:%d", pLevel->level, n);
        SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      SCH_ERR_JRET(schRecordQueryDataSrc(pJob, pTask));

      if (0 != taosHashPut(planToTask, &plan, POINTER_BYTES, &pTask, POINTER_BYTES)) {
        SCH_TASK_ELOG("taosHashPut to planToTaks failed, taskIdx:%d", n);
        SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      if (0 != taosHashPut(pJob->taskList, &pTask->taskId, sizeof(pTask->taskId), &pTask, POINTER_BYTES)) {
        SCH_TASK_ELOG("taosHashPut to taskList failed, taskIdx:%d", n);
        SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      ++pJob->taskNum;
    }

    SCH_JOB_DLOG("level %d initialized, taskNum:%d", i, taskNum);
  }

  SCH_ERR_JRET(schBuildTaskRalation(pJob, planToTask));

_return:
  
  if (planToTask) {
    taosHashCleanup(planToTask);
  }

  SCH_RET(code);
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

  if (SCH_IS_DATA_SRC_QRY_TASK(pTask)) {
    SCH_TASK_ELOG("no execNode specifed for data src task, numOfEps:%d", pTask->plan->execNode.epSet.numOfEps);
    SCH_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
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
  int8_t status = 0;

  if (schJobNeedToStop(pJob, &status)) {
    *needRetry = false;
    SCH_TASK_DLOG("task no more retry cause of job status, job status:%s", jobTaskStatusStr(status));
    return TSDB_CODE_SUCCESS;
  }

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
  SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_NOT_START);
  
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

int32_t schSetJobQueryRes(SSchJob* pJob, SQueryResult* pRes) {
  pRes->code = atomic_load_32(&pJob->errCode);
  pRes->numOfRows = pJob->resNumOfRows;
  pRes->res = pJob->execRes;
  pJob->execRes.res = NULL;

  return TSDB_CODE_SUCCESS;
}

int32_t schSetJobFetchRes(SSchJob* pJob, void** pData) {
  int32_t code = 0;
  if (pJob->resData && ((SRetrieveTableRsp *)pJob->resData)->completed) {
    SCH_ERR_RET(schUpdateJobStatus(pJob, JOB_TASK_STATUS_SUCCEED));
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

int32_t schNotifyUserExecRes(SSchJob* pJob) {
  SQueryResult* pRes = taosMemoryCalloc(1, sizeof(SQueryResult));
  if (pRes) {
    schSetJobQueryRes(pJob, pRes);
  }

  schEndOperation(pJob);

  SCH_JOB_DLOG("sch start to invoke exec cb, code: %s", tstrerror(pJob->errCode));
  (*pJob->userRes.execFp)(pRes, pJob->userRes.userParam, atomic_load_32(&pJob->errCode));
  SCH_JOB_DLOG("sch end from query cb, code: %s", tstrerror(pJob->errCode));

  return TSDB_CODE_SUCCESS;
}

int32_t schNotifyUserFetchRes(SSchJob* pJob) {
  void* pRes = NULL;
  
  schSetJobFetchRes(pJob, &pRes);

  schEndOperation(pJob);

  SCH_JOB_DLOG("sch start to invoke fetch cb, code: %s", tstrerror(pJob->errCode));
  (*pJob->userRes.fetchFp)(pRes, pJob->userRes.userParam, atomic_load_32(&pJob->errCode));
  SCH_JOB_DLOG("sch end from fetch cb, code: %s", tstrerror(pJob->errCode));

  return TSDB_CODE_SUCCESS;
}

void schPostJobRes(SSchJob *pJob, SCH_OP_TYPE op) {
  if (SCH_OP_NULL == pJob->opStatus.op) {
    SCH_JOB_DLOG("job not in any op, no need to post job res, status:%s", jobTaskStatusStr(pJob->status));
    return;
  }
  
  if (op && pJob->opStatus.op != op) {
    SCH_JOB_ELOG("job in op %s mis-match with expected %s", schGetOpStr(pJob->opStatus.op), schGetOpStr(op));
    return;
  }
  
  if (SCH_JOB_IN_SYNC_OP(pJob)) {
    tsem_post(&pJob->rspSem);
  } else if (SCH_JOB_IN_ASYNC_EXEC_OP(pJob)) {
    schNotifyUserExecRes(pJob);
  } else if (SCH_JOB_IN_ASYNC_FETCH_OP(pJob)) {
    schNotifyUserFetchRes(pJob);
  } else {
    SCH_JOB_ELOG("job not in any operation, status:%s", jobTaskStatusStr(pJob->status));
  }
}

int32_t schProcessOnJobFailureImpl(SSchJob *pJob, int32_t status, int32_t errCode) {
  // if already FAILED, no more processing
  SCH_ERR_RET(schUpdateJobStatus(pJob, status));

  schUpdateJobErrCode(pJob, errCode);
  
  int32_t code = atomic_load_32(&pJob->errCode);
  if (code) {
    SCH_JOB_DLOG("job failed with error: %s", tstrerror(code));
  }

  schPostJobRes(pJob, 0);

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

  SCH_ERR_RET(schUpdateJobStatus(pJob, JOB_TASK_STATUS_PARTIAL_SUCCEED));

  schPostJobRes(pJob, SCH_OP_EXEC);

  return TSDB_CODE_SUCCESS;

_return:

  SCH_RET(schProcessOnJobFailure(pJob, code));
}

void schProcessOnDataFetched(SSchJob *pJob) {
  schPostJobRes(pJob, SCH_OP_FETCH);
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

    if (SCH_GET_TASK_STATUS(pTask) != JOB_TASK_STATUS_EXECUTING) {
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

  SCH_LOG_TASK_END_TS(pTask);

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

  SCH_ERR_RET(schLaunchNextLevelTasks(pJob, pTask));

  return TSDB_CODE_SUCCESS;

_return:

  SCH_RET(schProcessOnJobFailure(pJob, code));
}

// Note: no more error processing, handled in function internal
int32_t schFetchFromRemote(SSchJob *pJob) {
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

int32_t schProcessOnExplainDone(SSchJob *pJob, SSchTask *pTask, SRetrieveTableRsp *pRsp) {
  SCH_TASK_DLOG("got explain rsp, rows:%d, complete:%d", htonl(pRsp->numOfRows), pRsp->completed);

  atomic_store_32(&pJob->resNumOfRows, htonl(pRsp->numOfRows));
  atomic_store_ptr(&pJob->resData, pRsp);

  SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_SUCCEED);

  schProcessOnDataFetched(pJob);

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


int32_t schRescheduleTask(SSchJob *pJob, SSchTask *pTask) {
  if (SCH_IS_DATA_SRC_QRY_TASK(pTask)) {
    return TSDB_CODE_SUCCESS;
  }

  SCH_LOCK_TASK(pTask);
  if (SCH_TASK_TIMEOUT(pTask) && JOB_TASK_STATUS_EXECUTING == pTask->status && 
      pJob->fetchTask != pTask && taosArrayGetSize(pTask->candidateAddrs) > 1) {
    SCH_TASK_DLOG("task execId %d will be rescheduled now", pTask->execId);
    schDropTaskOnExecNode(pJob, pTask);
    taosHashClear(pTask->execNodes);
    schProcessOnTaskFailure(pJob, pTask, TSDB_CODE_SCH_TIMEOUT_ERROR);
  }
  SCH_UNLOCK_TASK(pTask);

  return TSDB_CODE_SUCCESS;
}

int32_t schProcessOnTaskStatusRsp(SQueryNodeEpId* pEpId, SArray* pStatusList) {
  int32_t taskNum = (int32_t)taosArrayGetSize(pStatusList);
  SSchTask *pTask = NULL;

  qDebug("%d task status in hb rsp from nodeId:%d, fqdn:%s, port:%d", taskNum, pEpId->nodeId, pEpId->ep.fqdn, pEpId->ep.port);

  for (int32_t i = 0; i < taskNum; ++i) {
    STaskStatus *taskStatus = taosArrayGet(pStatusList, i);

    qDebug("QID:%" PRIx64 ",TID:0x%" PRIx64 ",EID:%d task status in server: %s", 
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
    
    if (taskStatus->status == JOB_TASK_STATUS_FAILED) {
      // RECORD AND HANDLE ERROR!!!!
      schReleaseJob(taskStatus->refId);
      continue;
    }

    if (taskStatus->status == JOB_TASK_STATUS_NOT_START) {
      schRescheduleTask(pJob, pTask);
    }

    schReleaseJob(taskStatus->refId);
  }

  return TSDB_CODE_SUCCESS;
}


int32_t schSaveJobQueryRes(SSchJob *pJob, SQueryTableRsp *rsp) {
  if (rsp->tbFName[0]) {
    if (NULL == pJob->execRes.res) {
      pJob->execRes.res = taosArrayInit(pJob->taskNum, sizeof(STbVerInfo));
      if (NULL == pJob->execRes.res) {
        SCH_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
      }
    }

    STbVerInfo tbInfo;
    strcpy(tbInfo.tbFName, rsp->tbFName);
    tbInfo.sversion = rsp->sversion;
    tbInfo.tversion = rsp->tversion;

    taosArrayPush((SArray *)pJob->execRes.res, &tbInfo);
    pJob->execRes.msgType = TDMT_SCH_QUERY;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schGetTaskFromList(SHashObj *pTaskList, uint64_t taskId, SSchTask **pTask) {
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

int32_t schGetTaskInJob(SSchJob *pJob, uint64_t taskId, SSchTask **pTask) {
  schGetTaskFromList(pJob->taskList, taskId, pTask);
  if (NULL == *pTask) {
    SCH_JOB_ELOG("task not found in job task list, taskId:0x%" PRIx64, taskId);
    SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
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

  SCH_ERR_RET(schChkJobNeedFlowCtrl(pJob, level));

  SCH_ERR_RET(schLaunchLevelTasks(pJob, level));

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

void schDropJobAllTasks(SSchJob *pJob) {
  schDropTaskInHashList(pJob, pJob->execTasks);
//  schDropTaskInHashList(pJob, pJob->succTasks);
//  schDropTaskInHashList(pJob, pJob->failTasks);
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

  int32_t numOfLevels = taosArrayGetSize(pJob->levels);
  for (int32_t i = 0; i < numOfLevels; ++i) {
    SSchLevel *pLevel = taosArrayGet(pJob->levels, i);

    int32_t numOfTasks = taosArrayGetSize(pLevel->subTasks);
    for (int32_t j = 0; j < numOfTasks; ++j) {
      SSchTask *pTask = taosArrayGet(pLevel->subTasks, j);
      schFreeTask(pJob, pTask);
    }

    taosArrayDestroy(pLevel->subTasks);
  }

  schFreeFlowCtrl(pJob);

  taosHashCleanup(pJob->execTasks);
//  taosHashCleanup(pJob->failTasks);
//  taosHashCleanup(pJob->succTasks);
  taosHashCleanup(pJob->taskList);
  
  taosArrayDestroy(pJob->levels);
  taosArrayDestroy(pJob->nodeList);
  taosArrayDestroy(pJob->dataSrcTasks);

  qExplainFreeCtx(pJob->explainCtx);

  destroyQueryExecRes(&pJob->execRes);

  qDestroyQueryPlan(pJob->pDag);

  taosMemoryFreeClear(pJob->userRes.queryRes);
  taosMemoryFreeClear(pJob->resData);
  taosMemoryFree(pJob);

  qDebug("QID:0x%" PRIx64 " sch job freed, refId:0x%" PRIx64 ", pointer:%p", queryId, refId, pJob);

  int32_t jobNum = atomic_sub_fetch_32(&schMgmt.jobNum, 1);
  if (jobNum == 0) {
    schCloseJobRef();
  }
}

int32_t schLaunchStaticExplainJob(SSchedulerReq *pReq, SSchJob *pJob, bool sync) {
  qDebug("QID:0x%" PRIx64 " job started", pReq->pDag->queryId);

  int32_t  code = 0;
/*  
  SSchJob *pJob = taosMemoryCalloc(1, sizeof(SSchJob));
  if (NULL == pJob) {
    qError("QID:0x%" PRIx64 " calloc %d failed", pReq->pDag->queryId, (int32_t)sizeof(SSchJob));
    code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    pReq->fp(NULL, pReq->cbParam, code);
    SCH_ERR_RET(code);
  }

  pJob->sql = pReq->sql;
  pJob->reqKilled = pReq->reqKilled;
  pJob->pDag = pReq->pDag;
  pJob->attr.queryJob = true;
  pJob->attr.explainMode = pReq->pDag->explainInfo.mode;
  pJob->queryId = pReq->pDag->queryId;
  pJob->userRes.execFp = pReq->fp;
  pJob->userRes.userParam = pReq->cbParam;
  
  schUpdateJobStatus(pJob, JOB_TASK_STATUS_NOT_START);

  code = schBeginOperation(pJob, SCH_OP_EXEC, sync);
  if (code) {
    pReq->fp(NULL, pReq->cbParam, code);
    schFreeJobImpl(pJob);
    SCH_ERR_RET(code);
  }
*/

  SCH_ERR_JRET(qExecStaticExplain(pReq->pDag, (SRetrieveTableRsp **)&pJob->resData));

/*
  int64_t refId = taosAddRef(schMgmt.jobRef, pJob);
  if (refId < 0) {
    SCH_JOB_ELOG("taosAddRef job failed, error:%s", tstrerror(terrno));
    SCH_ERR_JRET(terrno);
  }

  if (NULL == schAcquireJob(refId)) {
    SCH_JOB_ELOG("schAcquireJob job failed, refId:0x%" PRIx64, refId);
    SCH_ERR_JRET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  pJob->refId = refId;

  SCH_JOB_DLOG("job refId:0x%" PRIx64, pJob->refId);
*/

  pJob->status = JOB_TASK_STATUS_PARTIAL_SUCCEED;
  
  SCH_JOB_DLOG("job exec done, job status:%s", SCH_GET_JOB_STATUS_STR(pJob));
  
  if (!sync) {
    schPostJobRes(pJob, SCH_OP_EXEC);
  } else {
    schEndOperation(pJob);
  }

//  schReleaseJob(pJob->refId);

  SCH_RET(code);

_return:

  schEndOperation(pJob);
  if (!sync) {
    pReq->execFp(NULL, pReq->execParam, code);
  }
  
  schFreeJobImpl(pJob);
  
  SCH_RET(code);
}

int32_t schFetchRows(SSchJob *pJob) {
  int32_t  code = 0;

  if (!(pJob->attr.explainMode == EXPLAIN_MODE_STATIC)) {
    SCH_ERR_JRET(schFetchFromRemote(pJob));
    tsem_wait(&pJob->rspSem);
  }

  SCH_ERR_JRET(schSetJobFetchRes(pJob, pJob->userRes.fetchRes));

_return:

  schEndOperation(pJob);

  SCH_RET(code);
}

int32_t schAsyncFetchRows(SSchJob *pJob) {
  int32_t  code = 0;

  if (pJob->attr.explainMode == EXPLAIN_MODE_STATIC) {
    schPostJobRes(pJob, SCH_OP_FETCH);
    return TSDB_CODE_SUCCESS;
  }
  
  SCH_ERR_RET(schFetchFromRemote(pJob));

  return TSDB_CODE_SUCCESS;
}


int32_t schExecJobImpl(SSchedulerReq *pReq, SSchJob *pJob, bool sync) {
  int32_t  code = 0;

  qDebug("QID:0x%" PRIx64 " sch job refId 0x%"PRIx64 " started", pReq->pDag->queryId, pJob->refId);

  SCH_ERR_JRET(schBeginOperation(pJob, SCH_OP_EXEC, sync));

  if (EXPLAIN_MODE_STATIC == pReq->pDag->explainInfo.mode) {
    code = schLaunchStaticExplainJob(pReq, pJob, sync);
  } else {
    code = schLaunchJob(pJob);
    if (sync) {
      SCH_JOB_DLOG("will wait for rsp now, job status:%s", SCH_GET_JOB_STATUS_STR(pJob));
      tsem_wait(&pJob->rspSem);
      
      schEndOperation(pJob);
    } else if (code) {
      schPostJobRes(pJob, SCH_OP_EXEC);
    }
  }

  SCH_JOB_DLOG("job exec done, job status:%s, jobId:0x%" PRIx64, SCH_GET_JOB_STATUS_STR(pJob), pJob->refId);
  
  SCH_RET(code);

_return:

  if (!sync) {
    pReq->execFp(NULL, pReq->execParam, code);
  }
  
  SCH_RET(code);
}

int32_t schDoTaskRedirect(SSchJob *pJob, SSchTask *pTask, SDataBuf* pData, int32_t rspCode) {
  int32_t code = 0;
  int8_t  status = 0;
  if (schJobNeedToStop(pJob, &status)) {
    SCH_TASK_ELOG("redirect will no continue cause of job status %s", jobTaskStatusStr(status));
    SCH_RET(atomic_load_32(&pJob->errCode));
  }
  
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
      if (JOB_TASK_STATUS_EXECUTING == SCH_GET_TASK_STATUS(pTask)) {
        SCH_ERR_JRET(schLaunchTasksInFlowCtrlList(pJob, pTask));
      }
    }    

    SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_NOT_START);
    
    SCH_ERR_JRET(schLaunchTask(pJob, pTask));

    return TSDB_CODE_SUCCESS;
  }


  // merge plan
  
  pTask->childReady = 0;
  
  qClearSubplanExecutionNode(pTask->plan);

  SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_NOT_START);
  
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



