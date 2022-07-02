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

int32_t schInitJob(SSchJob **pSchJob, SSchedulerReq *pReq) {
  int32_t  code = 0;
  int64_t  refId = -1;
  SSchJob *pJob = taosMemoryCalloc(1, sizeof(SSchJob));
  if (NULL == pJob) {
    qError("QID:0x%" PRIx64 " calloc %d failed", pReq->pDag->queryId, (int32_t)sizeof(SSchJob));
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  pJob->attr.explainMode = pReq->pDag->explainInfo.mode;
  pJob->conn = *pReq->pConn;
  pJob->sql = pReq->sql;
  pJob->pDag = pReq->pDag;
  pJob->chkKillFp = pReq->chkKillFp;
  pJob->chkKillParam = pReq->chkKillParam;
  pJob->userRes.execFp = pReq->execFp;
  pJob->userRes.userParam = pReq->execParam;
  pJob->opStatus.op = SCH_OP_EXEC;
  pJob->opStatus.syncReq = pReq->syncReq;

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

  *pSchJob = pJob;

  return TSDB_CODE_SUCCESS;

_return:

  if (NULL == pJob) {
    qDestroyQueryPlan(pReq->pDag);
  } else if (refId < 0) {
    schFreeJobImpl(pJob);
  } else {
    taosRemoveRef(schMgmt.jobRef, refId);
  }
  
  SCH_RET(code);
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
    schUpdateJobErrCode(pJob, TSDB_CODE_TSC_QUERY_KILLED);
    return true;
  }  

  return (status == JOB_TASK_STATUS_FAIL || status == JOB_TASK_STATUS_DROP ||
          status == JOB_TASK_STATUS_SUCC);
}

int32_t schUpdateJobStatus(SSchJob *pJob, int8_t newStatus) {
  int32_t code = 0;

  int8_t oriStatus = 0;

  while (true) {
    oriStatus = SCH_GET_JOB_STATUS(pJob);

    if (oriStatus == newStatus) {
      if (newStatus == JOB_TASK_STATUS_DROP) {
        SCH_ERR_JRET(TSDB_CODE_SCH_JOB_IS_DROPPING);
      }
      
      SCH_ERR_JRET(TSDB_CODE_SCH_IGNORE_ERROR);
    }

    switch (oriStatus) {
      case JOB_TASK_STATUS_NULL:
        if (newStatus != JOB_TASK_STATUS_INIT) {
          SCH_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
        }

        break;
      case JOB_TASK_STATUS_INIT:
        if (newStatus != JOB_TASK_STATUS_EXEC && newStatus != JOB_TASK_STATUS_DROP) {
          SCH_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
        }

        break;
      case JOB_TASK_STATUS_EXEC:
        if (newStatus != JOB_TASK_STATUS_PART_SUCC && newStatus != JOB_TASK_STATUS_FAIL &&
            newStatus != JOB_TASK_STATUS_DROP) {
          SCH_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
        }

        break;
      case JOB_TASK_STATUS_PART_SUCC:
        if (newStatus != JOB_TASK_STATUS_FAIL && newStatus != JOB_TASK_STATUS_SUCC &&
            newStatus != JOB_TASK_STATUS_DROP) {
          SCH_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
        }

        break;
      case JOB_TASK_STATUS_SUCC:
      case JOB_TASK_STATUS_FAIL:
        if (newStatus != JOB_TASK_STATUS_DROP) {
          SCH_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
        }

        break;
      case JOB_TASK_STATUS_DROP:
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

  pJob->opStatus.syncReq = sync;

  switch (type) {
    case SCH_OP_EXEC:
      SCH_ERR_JRET(schUpdateJobStatus(pJob, JOB_TASK_STATUS_EXEC));
      break;
    case SCH_OP_FETCH:
      if (!SCH_JOB_NEED_FETCH(pJob)) {
        SCH_JOB_ELOG("no need to fetch data, status:%s", SCH_GET_JOB_STATUS_STR(pJob));
        SCH_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }

      if (status != JOB_TASK_STATUS_PART_SUCC) {
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


int32_t schAppendJobDataSrc(SSchJob *pJob, SSchTask *pTask) {
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

  level.status = JOB_TASK_STATUS_INIT;

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

      SCH_ERR_JRET(schAppendJobDataSrc(pJob, pTask));

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
    SCH_ERR_RET(schUpdateJobStatus(pJob, JOB_TASK_STATUS_SUCC));
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
  SCH_RET(schProcessOnJobFailureImpl(pJob, JOB_TASK_STATUS_FAIL, errCode));
}

// Note: no more error processing, handled in function internal
int32_t schProcessOnJobDropped(SSchJob *pJob, int32_t errCode) {
  SCH_RET(schProcessOnJobFailureImpl(pJob, JOB_TASK_STATUS_DROP, errCode));
}

// Note: no more task error processing, handled in function internal
int32_t schProcessOnJobPartialSuccess(SSchJob *pJob) {
  int32_t code = 0;

  SCH_ERR_RET(schUpdateJobStatus(pJob, JOB_TASK_STATUS_PART_SUCC));

  schPostJobRes(pJob, SCH_OP_EXEC);

  return TSDB_CODE_SUCCESS;

_return:

  SCH_RET(schProcessOnJobFailure(pJob, code));
}

void schProcessOnDataFetched(SSchJob *pJob) {
  schPostJobRes(pJob, SCH_OP_FETCH);
}

int32_t schProcessOnExplainDone(SSchJob *pJob, SSchTask *pTask, SRetrieveTableRsp *pRsp) {
  SCH_TASK_DLOG("got explain rsp, rows:%d, complete:%d", htonl(pRsp->numOfRows), pRsp->completed);

  atomic_store_32(&pJob->resNumOfRows, htonl(pRsp->numOfRows));
  atomic_store_ptr(&pJob->resData, pRsp);

  SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_SUCC);

  schProcessOnDataFetched(pJob);

  return TSDB_CODE_SUCCESS;
}


int32_t schLaunchJobLowerLevel(SSchJob *pJob, SSchTask *pTask) {
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

int32_t schGetTaskInJob(SSchJob *pJob, uint64_t taskId, SSchTask **pTask) {
  schGetTaskFromList(pJob->taskList, taskId, pTask);
  if (NULL == *pTask) {
    SCH_JOB_ELOG("task not found in job task list, taskId:0x%" PRIx64, taskId);
    SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
  }

  return TSDB_CODE_SUCCESS;
}


int32_t schLaunchJob(SSchJob *pJob) {
  if (EXPLAIN_MODE_STATIC == pJob->attr.explainMode) {
    SCH_ERR_RET(qExecStaticExplain(pJob->pDag, (SRetrieveTableRsp **)&pJob->resData));
    SCH_ERR_RET(schJobStatusEnter(&pJob, JOB_TASK_STATUS_PART_SUCC, NULL));
  } else {
    SSchLevel *level = taosArrayGet(pJob->levels, pJob->levelIdx);
    SCH_ERR_RET(schLaunchLevelTasks(pJob, level));
  }

  return TSDB_CODE_SUCCESS;
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

  qDebug("QID:0x%" PRIx64 " begin to free sch job, refId:0x%" PRIx64 ", pointer:%p", queryId, refId, pJob);

  if (pJob->status == JOB_TASK_STATUS_EXEC) {
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

  int32_t jobNum = atomic_sub_fetch_32(&schMgmt.jobNum, 1);
  if (jobNum == 0) {
    schCloseJobRef();
  }

  qDebug("QID:0x%" PRIx64 " sch job freed, refId:0x%" PRIx64 ", pointer:%p", queryId, refId, pJob);
}

int32_t schJobFetchRows(SSchJob *pJob) {
  int32_t  code = 0;

  if (!(pJob->attr.explainMode == EXPLAIN_MODE_STATIC)) {
    SCH_ERR_JRET(schLaunchFetchTask(pJob));
    tsem_wait(&pJob->rspSem);
  }

  SCH_ERR_JRET(schSetJobFetchRes(pJob, pJob->userRes.fetchRes));

_return:

  schEndOperation(pJob);

  SCH_RET(code);
}

int32_t schJobFetchRowsA(SSchJob *pJob) {
  int32_t  code = 0;

  if (pJob->attr.explainMode == EXPLAIN_MODE_STATIC) {
    schPostJobRes(pJob, SCH_OP_FETCH);
    return TSDB_CODE_SUCCESS;
  }
  
  SCH_ERR_RET(schLaunchFetchTask(pJob));

  return TSDB_CODE_SUCCESS;
}

int32_t schExecJob(SSchJob *pJob, SSchedulerReq *pReq) {
  int32_t code = 0;
  qDebug("QID:0x%" PRIx64 " sch job refId 0x%"PRIx64 " started", pReq->pDag->queryId, pJob->refId);

  SCH_ERR_JRET(schLaunchJob(pJob));
  
  if (pReq->syncReq) {
    SCH_JOB_DLOG("sync wait for rsp now, job status:%s", SCH_GET_JOB_STATUS_STR(pJob));
    tsem_wait(&pJob->rspSem);
  }

  SCH_JOB_DLOG("job exec done, job status:%s, jobId:0x%" PRIx64, SCH_GET_JOB_STATUS_STR(pJob), pJob->refId);
  
  return TSDB_CODE_SUCCESS;

_return:
  
  SCH_RET(schProcessOnJobFailure(pJob, code));
}

int32_t schJobStatusEnter(SSchJob** job, int32_t status, void* param) {
  SCH_ERR_RET(schUpdateJobStatus(*job, status));

  switch (status) {
    case JOB_TASK_STATUS_INIT:
      SCH_RET(schInitJob(job, param));
    case JOB_TASK_STATUS_EXEC:
      SCH_RET(schExecJob(job, param));
    case JOB_TASK_STATUS_PART_SUCC:
    default: {
      SSchJob* pJob = *job;
      SCH_JOB_ELOG("enter unknown job status %d", status);
      SCH_RET(TSDB_CODE_SCH_STATUS_ERROR);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schJobStatusEvent() {
    
  schEndOperation(pJob);
}




