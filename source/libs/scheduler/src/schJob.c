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
#include "schInt.h"
#include "tmsg.h"
#include "tref.h"
#include "trpc.h"

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

  SCH_JOB_DLOG("job errCode updated to %s", tstrerror(errCode));
}

bool schJobDone(SSchJob *pJob) {
  int8_t status = SCH_GET_JOB_STATUS(pJob);

  return (status == JOB_TASK_STATUS_FAIL || status == JOB_TASK_STATUS_DROP || status == JOB_TASK_STATUS_SUCC);
}

FORCE_INLINE bool schJobNeedToStop(SSchJob *pJob, int8_t *pStatus) {
  int8_t status = SCH_GET_JOB_STATUS(pJob);
  if (pStatus) {
    *pStatus = status;
  }

  if (schJobDone(pJob)) {
    return true;
  }

  if (pJob->chkKillFp && (*pJob->chkKillFp)(pJob->chkKillParam)) {
    schUpdateJobErrCode(pJob, TSDB_CODE_TSC_QUERY_KILLED);
    return true;
  }

  return false;
}

int32_t schUpdateJobStatus(SSchJob *pJob, int8_t newStatus) {
  int32_t code = 0;

  int8_t oriStatus = 0;

  while (true) {
    oriStatus = SCH_GET_JOB_STATUS(pJob);

    if (oriStatus == newStatus) {
      if (JOB_TASK_STATUS_FETCH == newStatus) {
        return code;
      }

      SCH_ERR_JRET(TSDB_CODE_SCH_IGNORE_ERROR);
    }

    switch (oriStatus) {
      case JOB_TASK_STATUS_NULL:
        if (newStatus != JOB_TASK_STATUS_INIT) {
          SCH_ERR_JRET(TSDB_CODE_APP_ERROR);
        }

        break;
      case JOB_TASK_STATUS_INIT:
        if (newStatus != JOB_TASK_STATUS_EXEC && newStatus != JOB_TASK_STATUS_DROP) {
          SCH_ERR_JRET(TSDB_CODE_APP_ERROR);
        }

        break;
      case JOB_TASK_STATUS_EXEC:
        if (newStatus != JOB_TASK_STATUS_PART_SUCC && newStatus != JOB_TASK_STATUS_FAIL &&
            newStatus != JOB_TASK_STATUS_DROP) {
          SCH_ERR_JRET(TSDB_CODE_APP_ERROR);
        }

        break;
      case JOB_TASK_STATUS_PART_SUCC:
        if (newStatus != JOB_TASK_STATUS_FAIL && newStatus != JOB_TASK_STATUS_SUCC &&
            newStatus != JOB_TASK_STATUS_DROP && newStatus != JOB_TASK_STATUS_EXEC &&
            newStatus != JOB_TASK_STATUS_FETCH) {
          SCH_ERR_JRET(TSDB_CODE_APP_ERROR);
        }

        break;
      case JOB_TASK_STATUS_FETCH:
        if (newStatus != JOB_TASK_STATUS_FAIL && newStatus != JOB_TASK_STATUS_SUCC &&
            newStatus != JOB_TASK_STATUS_DROP && newStatus != JOB_TASK_STATUS_EXEC &&
            newStatus != JOB_TASK_STATUS_FETCH) {
          SCH_ERR_JRET(TSDB_CODE_APP_ERROR);
        }

        break;
      case JOB_TASK_STATUS_SUCC:
      case JOB_TASK_STATUS_FAIL:
        if (newStatus != JOB_TASK_STATUS_DROP) {
          SCH_ERR_JRET(TSDB_CODE_APP_ERROR);
        }

        break;
      case JOB_TASK_STATUS_DROP:
        SCH_ERR_JRET(TSDB_CODE_QRY_JOB_FREED);
        break;

      default:
        SCH_JOB_ELOG("invalid job status:%s", jobTaskStatusStr(oriStatus));
        SCH_ERR_JRET(TSDB_CODE_APP_ERROR);
    }

    if (oriStatus != atomic_val_compare_exchange_8(&pJob->status, oriStatus, newStatus)) {
      continue;
    }

    SCH_JOB_DLOG("job status updated from %s to %s", jobTaskStatusStr(oriStatus), jobTaskStatusStr(newStatus));

    break;
  }

  return TSDB_CODE_SUCCESS;

_return:

  if (TSDB_CODE_SCH_IGNORE_ERROR == code) {
    SCH_JOB_DLOG("ignore job status update, from %s to %s", jobTaskStatusStr(oriStatus), jobTaskStatusStr(newStatus));
  } else {
    SCH_JOB_ELOG("invalid job status update, from %s to %s", jobTaskStatusStr(oriStatus), jobTaskStatusStr(newStatus));
  }
  SCH_RET(code);
}

int32_t schBuildTaskRalation(SSchJob *pJob, SHashObj *planToTask) {
  for (int32_t i = 0; i < pJob->levelNum; ++i) {
    SSchLevel *pLevel = taosArrayGet(pJob->levels, i);
    if (NULL == pLevel) {
      SCH_JOB_ELOG("fail to get the %dth level, levelNum: %d", i, pJob->levelNum);
      SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
    }

    for (int32_t m = 0; m < pLevel->taskNum; ++m) {
      SSchTask *pTask = taosArrayGet(pLevel->subTasks, m);
      if (NULL == pTask) {
        SCH_JOB_ELOG("fail to get the %dth task in level %d, taskNum: %d", m, pLevel->level, pLevel->taskNum);
        SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
      }

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
          SCH_ERR_RET(terrno);
        }
      }

      for (int32_t n = 0; n < childNum; ++n) {
        SSubplan *child = (SSubplan *)nodesListGetNode(pPlan->pChildren, n);
        if (NULL == child) {
          SCH_JOB_ELOG("fail to get the %dth child subplan, childNum: %d", n, childNum);
          SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
        }

        if (QUERY_NODE_PHYSICAL_SUBPLAN != nodeType(child)) {
          SCH_JOB_ELOG("invalid subplan type for the %dth child, level:%d, subplanNodeType:%d", n, i, nodeType(child));
          SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
        }

        SSchTask **childTask = taosHashGet(planToTask, &child, POINTER_BYTES);
        if (NULL == childTask || NULL == *childTask) {
          SCH_TASK_ELOG("subplan children relationship error, level:%d, taskIdx:%d, childIdx:%d", i, m, n);
          SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
        }

        if (NULL == taosArrayPush(pTask->children, childTask)) {
          SCH_TASK_ELOG("taosArrayPush childTask failed, level:%d, taskIdx:%d, childIdx:%d", i, m, n);
          SCH_ERR_RET(terrno);
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
          SCH_ERR_RET(terrno);
        }
      } else {
        if (0 != pLevel->level) {
          SCH_TASK_ELOG("invalid task info, level:%d, parentNum:%d", pLevel->level, parentNum);
          SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
        }
      }

      for (int32_t n = 0; n < parentNum; ++n) {
        SSubplan *parent = (SSubplan *)nodesListGetNode(pPlan->pParents, n);
        if (NULL == parent) {
          SCH_JOB_ELOG("fail to get the %dth parent subplan, parentNum: %d", n, parentNum);
          SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
        }

        if (QUERY_NODE_PHYSICAL_SUBPLAN != nodeType(parent)) {
          SCH_JOB_ELOG("invalid subplan type for the %dth parent, level:%d, subplanNodeType:%d", n, i, nodeType(parent));
          SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
        }

        SSchTask **parentTask = taosHashGet(planToTask, &parent, POINTER_BYTES);
        if (NULL == parentTask || NULL == *parentTask) {
          SCH_TASK_ELOG("subplan parent relationship error, level:%d, taskIdx:%d, childIdx:%d", i, m, n);
          SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
        }

        if (NULL == taosArrayPush(pTask->parents, parentTask)) {
          SCH_TASK_ELOG("taosArrayPush parentTask failed, level:%d, taskIdx:%d, childIdx:%d", i, m, n);
          SCH_ERR_RET(terrno);
        }

        SCH_TASK_DLOG("parents info, the %d parent TID 0x%" PRIx64, n, (*parentTask)->taskId);
      }

      SCH_TASK_TLOG("level:%d, parentNum:%d, childNum:%d", i, parentNum, childNum);
    }
  }

  SSchLevel *pLevel = taosArrayGet(pJob->levels, 0);
  if (NULL == pLevel) {
    SCH_JOB_ELOG("fail to get level 0 level, levelNum:%d", (int32_t)taosArrayGetSize(pJob->levels));
    SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
  }

  if (SCH_IS_QUERY_JOB(pJob)) {
    if (pLevel->taskNum > 1) {
      SCH_JOB_ELOG("invalid query plan, level:0, taskNum:%d", pLevel->taskNum);
      SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
    }

    SSchTask *pTask = taosArrayGet(pLevel->subTasks, 0);
    if (NULL == pLevel) {
      SCH_JOB_ELOG("fail to get the first task in level 0, taskNum:%d", (int32_t)taosArrayGetSize(pLevel->subTasks));
      SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
    }

    if (SUBPLAN_TYPE_MODIFY != pTask->plan->subplanType || EXPLAIN_MODE_DISABLE != pJob->attr.explainMode) {
      pJob->attr.needFetch = true;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schAppendJobDataSrc(SSchJob *pJob, SSchTask *pTask) {
  if (!SCH_IS_DATA_BIND_QRY_TASK(pTask)) {
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == taosArrayPush(pJob->dataSrcTasks, &pTask)) {
    SCH_ERR_RET(terrno);
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

  pJob->dataSrcTasks = taosArrayInit(SCH_GET_TASK_CAPACITY(pDag->numOfSubplans), POINTER_BYTES);
  if (NULL == pJob->dataSrcTasks) {
    SCH_ERR_RET(terrno);
  }

  int32_t levelNum = (int32_t)LIST_LENGTH(pDag->pSubplans);
  if (levelNum <= 0) {
    SCH_JOB_ELOG("invalid level num:%d", levelNum);
    SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SHashObj *planToTask = taosHashInit(
      SCH_GET_TASK_CAPACITY(pDag->numOfSubplans),
      taosGetDefaultHashFunction(POINTER_BYTES == sizeof(int64_t) ? TSDB_DATA_TYPE_BIGINT : TSDB_DATA_TYPE_INT), false,
      HASH_NO_LOCK);
  if (NULL == planToTask) {
    SCH_JOB_ELOG("taosHashInit %d failed", SCHEDULE_DEFAULT_MAX_TASK_NUM);
    SCH_ERR_JRET(terrno);
  }

  pJob->levels = taosArrayInit(levelNum, sizeof(SSchLevel));
  if (NULL == pJob->levels) {
    SCH_JOB_ELOG("taosArrayInit %d failed", levelNum);
    SCH_ERR_JRET(terrno);
  }

  pJob->levelNum = levelNum;
  SCH_RESET_JOB_LEVEL_IDX(pJob);

  (void)atomic_add_fetch_64(&pJob->seriousId, 1);
  qDebug("QID:0x%" PRIx64 ", job seriousId set to SID:%" PRId64 ", levelIdx:%d", pJob->queryId, pJob->seriousId,
         pJob->levelIdx);

  SSchLevel      level = {0};
  SNodeListNode *plans = NULL;
  int32_t        taskNum = 0;
  int32_t        totalTaskNum = 0;
  SSchLevel     *pLevel = NULL;

  level.status = JOB_TASK_STATUS_INIT;

  for (int32_t i = 0; i < levelNum; ++i) {
    if (NULL == taosArrayPush(pJob->levels, &level)) {
      SCH_JOB_ELOG("taosArrayPush level failed, level:%d", i);
      SCH_ERR_JRET(terrno);
    }

    pLevel = taosArrayGet(pJob->levels, i);
    if (NULL == pLevel) {
      SCH_JOB_ELOG("fail to get the %dth level, levelNum: %d", i, levelNum);
      SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
    }

    pLevel->level = i;

    plans = (SNodeListNode *)nodesListGetNode(pDag->pSubplans, i);
    if (NULL == plans) {
      SCH_JOB_ELOG("empty level plan, level:%d", i);
      SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
    }

    if (QUERY_NODE_NODE_LIST != nodeType(plans)) {
      SCH_JOB_ELOG("invalid level plan, level:%d, planNodeType:%d", i, nodeType(plans));
      SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
    }

    taskNum = (int32_t)LIST_LENGTH(plans->pNodeList);
    if (taskNum <= 0) {
      SCH_JOB_ELOG("invalid level plan number:%d, level:%d", taskNum, i);
      SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
    }

    totalTaskNum += taskNum;
    if (totalTaskNum > pDag->numOfSubplans) {
      SCH_JOB_ELOG("current totalTaskNum %d is bigger than numOfSubplans %d, level:%d", totalTaskNum, pDag->numOfSubplans, i);
      SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
    }
    
    pLevel->taskNum = taskNum;

    pLevel->subTasks = taosArrayInit(taskNum, sizeof(SSchTask));
    if (NULL == pLevel->subTasks) {
      SCH_JOB_ELOG("taosArrayInit %d failed", taskNum);
      SCH_ERR_JRET(terrno);
    }

    for (int32_t n = 0; n < taskNum; ++n) {
      SSubplan *plan = (SSubplan *)nodesListGetNode(plans->pNodeList, n);

      SCH_ERR_JRET(schValidateSubplan(pJob, plan, pLevel->level, n, taskNum));
      
      SCH_SET_JOB_TYPE(pJob, plan->subplanType);

      SSchTask  task = {0};
      SSchTask *pTask = taosArrayPush(pLevel->subTasks, &task);
      if (NULL == pTask) {
        SCH_TASK_ELOG("taosArrayPush task to level failed, level:%d, taskIdx:%d", pLevel->level, n);
        SCH_ERR_JRET(terrno);
      }

      SCH_ERR_JRET(schInitTask(pJob, pTask, plan, pLevel));

      SCH_ERR_JRET(schAppendJobDataSrc(pJob, pTask));

      code = taosHashPut(planToTask, &plan, POINTER_BYTES, &pTask, POINTER_BYTES);
      if (0 != code) {
        SCH_TASK_ELOG("taosHashPut to planToTaks failed, taskIdx:%d, error:%s", n, tstrerror(code));
        SCH_ERR_JRET(code);
      }

      code = taosHashPut(pJob->taskList, &pTask->taskId, sizeof(pTask->taskId), &pTask, POINTER_BYTES);
      if (0 != code) {
        SCH_TASK_ELOG("taosHashPut to taskList failed, taskIdx:%d, error:%s", n, tstrerror(code));
        SCH_ERR_JRET(code);
      }

      ++pJob->taskNum;
    }

    SCH_JOB_TLOG("level %d initialized, taskNum:%d", i, taskNum);
  }

  if (totalTaskNum != pDag->numOfSubplans) {
    SCH_JOB_ELOG("totalTaskNum %d mis-match with numOfSubplans %d", totalTaskNum, pDag->numOfSubplans);
    SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SCH_ERR_JRET(schBuildTaskRalation(pJob, planToTask));

_return:

  if (planToTask) {
    taosHashCleanup(planToTask);
  }

  SCH_RET(code);
}

void schDumpJobExecRes(SSchJob *pJob, SExecResult *pRes) {
  pRes->code = atomic_load_32(&pJob->errCode);
  pRes->numOfRows = pJob->resNumOfRows;

  SCH_LOCK(SCH_WRITE, &pJob->resLock);
  pRes->res = pJob->execRes.res;
  pRes->msgType = pJob->execRes.msgType;
  pRes->numOfBytes = pJob->execRes.numOfBytes;
  pJob->execRes.res = NULL;
  SCH_UNLOCK(SCH_WRITE, &pJob->resLock);

  SCH_JOB_DLOG("exec result dumped, code:%s", tstrerror(pRes->code));
}

int32_t schDumpJobFetchRes(SSchJob *pJob, void **pData) {
  int32_t code = 0;

  SCH_LOCK(SCH_WRITE, &pJob->resLock);

  pJob->fetched = true;

  if (pJob->fetchRes && ((SRetrieveTableRsp *)pJob->fetchRes)->completed) {
    SCH_ERR_JRET(schSwitchJobStatus(pJob, JOB_TASK_STATUS_SUCC, NULL));
  }

  while (true) {
    *pData = atomic_load_ptr(&pJob->fetchRes);
    if (*pData != atomic_val_compare_exchange_ptr(&pJob->fetchRes, *pData, NULL)) {
      continue;
    }

    break;
  }

  if (NULL == *pData) {
    SRetrieveTableRsp *rsp = (SRetrieveTableRsp *)taosMemoryCalloc(1, sizeof(SRetrieveTableRsp));
    if (NULL == rsp) {
      SCH_JOB_ELOG("malloc SRetrieveTableRsp %d failed, code:%x", (int32_t)sizeof(SRetrieveTableRsp), terrno);
      SCH_ERR_JRET(terrno);
    }

    rsp->completed = 1;

    *pData = rsp;
    SCH_JOB_DLOG("empty res and set query complete, code:%x", code);
  }

  SCH_JOB_DLOG("fetch done, totalRows:%" PRId64, pJob->resNumOfRows);

_return:

  SCH_UNLOCK(SCH_WRITE, &pJob->resLock);

  return code;
}

int32_t schNotifyUserExecRes(SSchJob *pJob) {
  SExecResult *pRes = taosMemoryCalloc(1, sizeof(SExecResult));
  if (NULL == pRes) {
    qError("malloc execResult %d failed, error: %x", (int32_t)sizeof(SExecResult), terrno);
    SCH_RET(terrno);
  }

  schDumpJobExecRes(pJob, pRes);

  SCH_JOB_TLOG("sch start to invoke exec cb, code:%s", tstrerror(pJob->errCode));
  (*pJob->userRes.execFp)(pRes, pJob->userRes.cbParam, atomic_load_32(&pJob->errCode));
  SCH_JOB_TLOG("sch end from exec cb, code:%s", tstrerror(pJob->errCode));

  return TSDB_CODE_SUCCESS;
}

int32_t schNotifyUserFetchRes(SSchJob *pJob) {
  void *pRes = NULL;

  int32_t code = schDumpJobFetchRes(pJob, &pRes);
  if (TSDB_CODE_SUCCESS != code && TSDB_CODE_SUCCESS == atomic_load_32(&pJob->errCode)) {
    atomic_store_32(&pJob->errCode, code);
  }

  SCH_JOB_DLOG("sch start to invoke fetch cb, code:%s", tstrerror(pJob->errCode));
  (*pJob->userRes.fetchFp)(pRes, pJob->userRes.cbParam, atomic_load_32(&pJob->errCode));
  SCH_JOB_DLOG("sch end from fetch cb, code:%s", tstrerror(pJob->errCode));

  return TSDB_CODE_SUCCESS;
}

void schPostJobRes(SSchJob *pJob, SCH_OP_TYPE op) {
  int32_t code = 0;
  SCH_LOCK(SCH_WRITE, &pJob->opStatus.lock);

  if (SCH_OP_NULL == pJob->opStatus.op) {
    SCH_JOB_TLOG("job not in any operation, no need to post job res, status:%s", jobTaskStatusStr(pJob->status));
    goto _return;
  }

  if (SCH_OP_NULL != op && pJob->opStatus.op != op) {
    SCH_JOB_ELOG("job in operation %s mis-match with expected %s", schGetOpStr(pJob->opStatus.op), schGetOpStr(op));
    goto _return;
  }

  if (SCH_JOB_IN_SYNC_OP(pJob)) {
    SCH_UNLOCK(SCH_WRITE, &pJob->opStatus.lock);
    code = tsem_post(&pJob->rspSem);
    if (code) {
      SCH_JOB_ELOG("tsem_post failed for syncOp, error:%s", tstrerror(code));
    }
  } else if (SCH_JOB_IN_ASYNC_EXEC_OP(pJob)) {
    SCH_UNLOCK(SCH_WRITE, &pJob->opStatus.lock);
    (void)schNotifyUserExecRes(pJob);  // ignore error
  } else if (SCH_JOB_IN_ASYNC_FETCH_OP(pJob)) {
    SCH_UNLOCK(SCH_WRITE, &pJob->opStatus.lock);
    (void)schNotifyUserFetchRes(pJob);  // ignore error
  } else {
    SCH_UNLOCK(SCH_WRITE, &pJob->opStatus.lock);
    SCH_JOB_ELOG("job not in any operation, status:%s", jobTaskStatusStr(pJob->status));
  }

  return;

_return:

  SCH_UNLOCK(SCH_WRITE, &pJob->opStatus.lock);
}

int32_t schProcessOnJobFailure(SSchJob *pJob, int32_t errCode) {
  if (TSDB_CODE_SCH_IGNORE_ERROR == errCode) {
    schPostJobRes(pJob, 0);
    return TSDB_CODE_SCH_IGNORE_ERROR;
  }

  schUpdateJobErrCode(pJob, errCode);

  int32_t code = atomic_load_32(&pJob->errCode);
  if (code) {
    SCH_JOB_DLOG("job failed with error %s", tstrerror(code));
  }

  schPostJobRes(pJob, 0);

  SCH_RET(TSDB_CODE_SCH_IGNORE_ERROR);
}

int32_t schHandleJobFailure(SSchJob *pJob, int32_t errCode) {
  if (TSDB_CODE_SCH_IGNORE_ERROR == errCode) {
    return TSDB_CODE_SCH_IGNORE_ERROR;
  }

  (void)schSwitchJobStatus(pJob, JOB_TASK_STATUS_FAIL, &errCode);  // ignore error

  return TSDB_CODE_SCH_IGNORE_ERROR;
}

int32_t schProcessOnJobDropped(SSchJob *pJob, int32_t errCode) { SCH_RET(schProcessOnJobFailure(pJob, errCode)); }

int32_t schHandleJobDrop(SSchJob *pJob, int32_t errCode) {
  if (TSDB_CODE_SCH_IGNORE_ERROR == errCode) {
    return TSDB_CODE_SCH_IGNORE_ERROR;
  }

  (void)schSwitchJobStatus(pJob, JOB_TASK_STATUS_DROP, &errCode);  // ignore error

  return TSDB_CODE_SCH_IGNORE_ERROR;
}

int32_t schProcessOnJobPartialSuccess(SSchJob *pJob) {
  if (schChkCurrentOp(pJob, SCH_OP_FETCH, -1)) {
    SCH_ERR_RET(schLaunchFetchTask(pJob));
  } else {
    schPostJobRes(pJob, 0);
  }

  return TSDB_CODE_SUCCESS;
}

void schProcessOnDataFetched(SSchJob *pJob) { schPostJobRes(pJob, SCH_OP_FETCH); }

int32_t schProcessOnExplainDone(SSchJob *pJob, SSchTask *pTask, SRetrieveTableRsp *pRsp) {
  SCH_TASK_DLOG("got explain rsp, rows:%" PRId64 ", complete:%d", htobe64(pRsp->numOfRows), pRsp->completed);

  atomic_store_64(&pJob->resNumOfRows, htobe64(pRsp->numOfRows));
  atomic_store_ptr(&pJob->fetchRes, pRsp);

  SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_SUCC);

  if (!SCH_IS_INSERT_JOB(pJob)) {
    schProcessOnDataFetched(pJob);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schLaunchJobLowerLevel(SSchJob *pJob, SSchTask *pTask) {
  if (!SCH_IS_QUERY_JOB(pJob)) {
    return TSDB_CODE_SUCCESS;
  }

  SSchLevel *pLevel = pTask->level;
  int32_t    doneNum = atomic_load_32(&pLevel->taskExecDoneNum);
  if (doneNum == pLevel->taskNum) {
    (void)atomic_sub_fetch_32(&pJob->levelIdx, 1);

    pLevel = taosArrayGet(pJob->levels, pJob->levelIdx);
    if (NULL == pLevel) {
      SCH_JOB_ELOG("fail to get the %dth level, levelNum:%d", pJob->levelIdx, (int32_t)taosArrayGetSize(pJob->levels));
      SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
    }

    for (int32_t i = 0; i < pLevel->taskNum; ++i) {
      SSchTask *pTask = taosArrayGet(pLevel->subTasks, i);
      if (NULL == pTask) {
        SCH_JOB_ELOG("fail to get the %dth task in level %d, taskNum:%d", i, pLevel->level, pLevel->taskNum);
        SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
      }

      if (pTask->children && taosArrayGetSize(pTask->children) > 0) {
        continue;
      }

      if (SCH_TASK_ALREADY_LAUNCHED(pTask)) {
        continue;
      }

      SCH_ERR_RET(schDelayLaunchTask(pJob, pTask));
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schSaveJobExecRes(SSchJob *pJob, SQueryTableRsp *rsp) {
  if (rsp->tbVerInfo) {
    SCH_LOCK(SCH_WRITE, &pJob->resLock);

    if (NULL == pJob->execRes.res) {
      pJob->execRes.res = taosArrayInit(pJob->taskNum, sizeof(STbVerInfo));
      if (NULL == pJob->execRes.res) {
        SCH_UNLOCK(SCH_WRITE, &pJob->resLock);
        SCH_ERR_RET(terrno);
      }
    }

    if (NULL == taosArrayAddBatch((SArray *)pJob->execRes.res, taosArrayGet(rsp->tbVerInfo, 0),
                                  taosArrayGetSize(rsp->tbVerInfo))) {
      SCH_UNLOCK(SCH_WRITE, &pJob->resLock);
      SCH_ERR_RET(terrno);
    }

    taosArrayDestroy(rsp->tbVerInfo);

    pJob->execRes.msgType = TDMT_SCH_QUERY;

    SCH_UNLOCK(SCH_WRITE, &pJob->resLock);
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
    SCH_ERR_RET(qExecStaticExplain(pJob->pDag, (SRetrieveTableRsp **)&pJob->fetchRes));
    SCH_ERR_RET(schSwitchJobStatus(pJob, JOB_TASK_STATUS_PART_SUCC, NULL));
  } else {
    SSchLevel *level = taosArrayGet(pJob->levels, pJob->levelIdx);
    if (NULL == level) {
      SCH_JOB_ELOG("fail to get the %dth level, levelNum:%d", pJob->levelIdx, (int32_t)taosArrayGetSize(pJob->levels));
      SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
    }

    SCH_ERR_RET(schLaunchLevelTasks(pJob, level));
  }

  return TSDB_CODE_SUCCESS;
}

void schDropJobAllTasks(SSchJob *pJob) {
  schDropTaskInHashList(pJob, pJob->execTasks);
  //  schDropTaskInHashList(pJob, pJob->succTasks);
  //  schDropTaskInHashList(pJob, pJob->failTasks);
}

int32_t schNotifyJobAllTasks(SSchJob *pJob, SSchTask *pTask, ETaskNotifyType type) {
  SCH_RET(schNotifyTaskInHashList(pJob, pJob->execTasks, type, pTask));
}

void schFreeJobImpl(void *job) {
  if (NULL == job) {
    return;
  }

  SSchJob *pJob = job;
  uint64_t queryId = pJob->queryId;
  int64_t  refId = pJob->refId;

  qTrace("QID:0x%" PRIx64 ", begin to free sch job, jobId:0x%" PRIx64 ", pointer:%p", queryId, refId, pJob);

  schDropJobAllTasks(pJob);

  int32_t numOfLevels = taosArrayGetSize(pJob->levels);
  for (int32_t i = 0; i < numOfLevels; ++i) {
    SSchLevel *pLevel = taosArrayGet(pJob->levels, i);
    if (NULL == pLevel) {
      SCH_JOB_ELOG("fail to get the %dth level, levelNum:%d", i, numOfLevels);
      continue;
    }

    int32_t numOfTasks = taosArrayGetSize(pLevel->subTasks);
    for (int32_t j = 0; j < numOfTasks; ++j) {
      SSchTask *pTask = taosArrayGet(pLevel->subTasks, j);
      if (NULL == pLevel) {
        SCH_JOB_ELOG("fail to get the %dth task, taskNum:%d", j, numOfTasks);
        continue;
      }

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
  (void)nodesReleaseAllocatorWeakRef(pJob->allocatorRefId);  // ignore error

  taosMemoryFreeClear(pJob->userRes.execRes);
  taosMemoryFreeClear(pJob->fetchRes);
  taosMemoryFreeClear(pJob->sql);
  int32_t code = tsem_destroy(&pJob->rspSem);
  if (code) {
    qError("tsem_destroy failed, error:%s", tstrerror(code));
  }
  taosMemoryFree(pJob);

  if (refId > 0) {
    int32_t jobNum = atomic_sub_fetch_32(&schMgmt.jobNum, 1);
    if (jobNum == 0) {
      schCloseJobRef();
    }
  }

  qTrace("QID:0x%" PRIx64 ", sch job freed, jobId:0x%" PRIx64 ", pointer:%p", queryId, refId, pJob);
}

int32_t schJobFetchRows(SSchJob *pJob) {
  int32_t code = 0;

  if (!(pJob->attr.explainMode == EXPLAIN_MODE_STATIC) && !(SCH_IS_EXPLAIN_JOB(pJob) && SCH_IS_INSERT_JOB(pJob))) {
    SCH_ERR_RET(schLaunchFetchTask(pJob));

    if (schChkCurrentOp(pJob, SCH_OP_FETCH, true)) {
      SCH_JOB_DLOG("sync wait for rsp now, job status:%s", SCH_GET_JOB_STATUS_STR(pJob));
      code = tsem_wait(&pJob->rspSem);
      if (code) {
        qError("tsem_wait for fetch rspSem failed, error:%s", tstrerror(code));
        SCH_RET(code);
      }

      SCH_RET(schDumpJobFetchRes(pJob, pJob->userRes.fetchRes));
    }
  } else {
    if (schChkCurrentOp(pJob, SCH_OP_FETCH, true)) {
      SCH_RET(schDumpJobFetchRes(pJob, pJob->userRes.fetchRes));
    } else {
      schPostJobRes(pJob, SCH_OP_FETCH);
    }
  }

  SCH_RET(code);
}

int32_t schInitJob(int64_t *pJobId, SSchedulerReq *pReq) {
  int32_t  code = 0;
  int64_t  refId = -1;
  SSchJob *pJob = taosMemoryCalloc(1, sizeof(SSchJob));
  if (NULL == pJob) {
    qError("QID:0x%" PRIx64 ", calloc %d failed", pReq->pDag->queryId, (int32_t)sizeof(SSchJob));
    SCH_ERR_JRET(terrno);
  }

  pJob->attr.explainMode = pReq->pDag->explainInfo.mode;
  pJob->attr.localExec = pReq->localReq;
  pJob->conn = *pReq->pConn;
  qInfo("QID:0x%" PRIx64 " init with pTrans:%p", pReq->pDag->queryId, pJob->conn.pTrans);
  
  if (pReq->sql) {
    pJob->sql = taosStrdup(pReq->sql);
    if (NULL == pJob->sql) {
      qError("QID:0x%" PRIx64 ", strdup sql %s failed", pReq->pDag->queryId, pReq->sql);
      SCH_ERR_JRET(terrno);
    }
  }
  pJob->pDag = pReq->pDag;
  if (pReq->allocatorRefId > 0) {
    pJob->allocatorRefId = nodesMakeAllocatorWeakRef(pReq->allocatorRefId);
    if (pJob->allocatorRefId <= 0) {
      qError("QID:0x%" PRIx64 ", nodesMakeAllocatorWeakRef failed", pReq->pDag->queryId);
      SCH_ERR_JRET(terrno);
    }
  }
  pJob->chkKillFp = pReq->chkKillFp;
  pJob->chkKillParam = pReq->chkKillParam;
  pJob->userRes.execFp = pReq->execFp;
  pJob->userRes.cbParam = pReq->cbParam;
  pJob->source = pReq->source;
  pJob->pWorkerCb = pReq->pWorkerCb;

  if (pReq->pNodeList == NULL || taosArrayGetSize(pReq->pNodeList) <= 0) {
    qTrace("QID:0x%" PRIx64 ", input exec nodeList is empty", pReq->pDag->queryId);
  } else {
    pJob->nodeList = taosArrayDup(pReq->pNodeList, NULL);
    if (NULL == pJob->nodeList) {
      qError("QID:0x%" PRIx64 ", taosArrayDup failed, origNum:%d", pReq->pDag->queryId,
             (int32_t)taosArrayGetSize(pReq->pNodeList));
      SCH_ERR_JRET(terrno);
    }
  }

  pJob->taskList = taosHashInit(SCH_GET_TASK_CAPACITY(pReq->pDag->numOfSubplans), taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false,
                                HASH_ENTRY_LOCK);
  if (NULL == pJob->taskList) {
    SCH_JOB_ELOG("taosHashInit %d taskList failed", SCH_GET_TASK_CAPACITY(pReq->pDag->numOfSubplans));
    SCH_ERR_JRET(terrno);
  }

  SCH_ERR_JRET(schValidateAndBuildJob(pReq->pDag, pJob));

  if (SCH_IS_EXPLAIN_JOB(pJob)) {
    SCH_ERR_JRET(qExecExplainBegin(pReq->pDag, &pJob->explainCtx, pReq->startTs));
  }

  pJob->execTasks = taosHashInit(pReq->pDag->numOfSubplans, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false,
                                 HASH_ENTRY_LOCK);
  if (NULL == pJob->execTasks) {
    SCH_JOB_ELOG("taosHashInit %d execTasks failed", pReq->pDag->numOfSubplans);
    SCH_ERR_JRET(terrno);
  }

  if (tsem_init(&pJob->rspSem, 0, 0)) {
    SCH_JOB_ELOG("tsem_init failed, errno:%d", ERRNO);
    SCH_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  pJob->refId = taosAddRef(schMgmt.jobRef, pJob);
  if (pJob->refId < 0) {
    SCH_JOB_ELOG("taosAddRef job failed, error:%s", tstrerror(terrno));
    SCH_ERR_JRET(terrno);
  }

  (void)atomic_add_fetch_32(&schMgmt.jobNum, 1);

  *pJobId = pJob->refId;

  SCH_JOB_TLOG("jobId:0x%" PRIx64 ", job created", pJob->refId);

  return TSDB_CODE_SUCCESS;

_return:

  if (NULL == pJob) {
    qDestroyQueryPlan(pReq->pDag);
  } else if (pJob->refId <= 0) {
    schFreeJobImpl(pJob);
  } else {
    code = taosRemoveRef(schMgmt.jobRef, pJob->refId);
    if (code) {
      SCH_JOB_DLOG("jobId:0x%" PRIx64 ", taosRemoveRef job from jobRef, error:%s", pJob->refId, tstrerror(code));
    }
  }

  SCH_RET(code);
}

int32_t schExecJob(SSchJob *pJob, SSchedulerReq *pReq) {
  int32_t code = 0;
  qTrace("QID:0x%" PRIx64 ", jobId:0x%" PRIx64 ", sch job started", pReq->pDag->queryId, pJob->refId);

  SCH_ERR_RET(schLaunchJob(pJob));

  if (pReq->syncReq) {
    SCH_JOB_DLOG("sync wait for rsp now, job status:%s", SCH_GET_JOB_STATUS_STR(pJob));
    code = tsem_wait(&pJob->rspSem);
    if (code) {
      qError("QID:0x%" PRIx64 ", tsem_wait sync rspSem failed, error:%s", pReq->pDag->queryId, tstrerror(code));
      SCH_ERR_RET(code);
    }
  }

  SCH_JOB_DLOG("job exec done, job status:%s, jobId:0x%" PRIx64, SCH_GET_JOB_STATUS_STR(pJob), pJob->refId);

  return TSDB_CODE_SUCCESS;
}

void schDirectPostJobRes(SSchedulerReq *pReq, int32_t errCode) {
  if (NULL == pReq || pReq->syncReq) {
    return;
  }

  if (pReq->execFp) {
    (*pReq->execFp)(NULL, pReq->cbParam, errCode);
  } else if (pReq->fetchFp) {
    (*pReq->fetchFp)(NULL, pReq->cbParam, errCode);
  }
}

int32_t schChkResetJobRetry(SSchJob *pJob, int32_t rspCode) {
  if (pJob->status >= JOB_TASK_STATUS_PART_SUCC) {
    SCH_LOCK(SCH_WRITE, &pJob->resLock);
    if (pJob->fetched) {
      SCH_UNLOCK(SCH_WRITE, &pJob->resLock);
      pJob->noMoreRetry = true;
      SCH_JOB_ELOG("already fetched while got error %s", tstrerror(rspCode));
      SCH_ERR_RET(rspCode);
    }
    SCH_UNLOCK(SCH_WRITE, &pJob->resLock);

    SCH_ERR_RET(schUpdateJobStatus(pJob, JOB_TASK_STATUS_EXEC));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schResetJobForRetry(SSchJob *pJob, SSchTask *pTask, int32_t rspCode, bool *inRetry) {
  while (true) {
    if (pTask->seriousId < atomic_load_64(&pJob->seriousId)) {
      SCH_TASK_DLOG("task sId %" PRId64 " is smaller than current job sId %" PRId64, pTask->seriousId, pJob->seriousId);
      return TSDB_CODE_SCH_IGNORE_ERROR;
    }

    int8_t origInRetry = atomic_val_compare_exchange_8(&pJob->inRetry, 0, 1);
    if (0 != origInRetry) {
      SCH_JOB_DLOG("job already in retry, origInRetry: %d", pJob->inRetry);
      taosUsleep(1);
      continue;
    }

    if (pTask->seriousId < atomic_load_64(&pJob->seriousId)) {
      SCH_TASK_DLOG("task sId %" PRId64 " is smaller than current job sId %" PRId64, pTask->seriousId, pJob->seriousId);
      return TSDB_CODE_SCH_IGNORE_ERROR;
    }

    break;
  }

  *inRetry = true;

  SCH_ERR_RET(schChkResetJobRetry(pJob, rspCode));

  (void)atomic_add_fetch_64(&pJob->seriousId, 1);

  int32_t code = 0;
  int32_t numOfLevels = taosArrayGetSize(pJob->levels);
  for (int32_t i = 0; i < numOfLevels; ++i) {
    SSchLevel *pLevel = taosArrayGet(pJob->levels, i);
    if (NULL == pLevel) {
      SCH_JOB_ELOG("fail to get the %dth level, levelNum:%d", i, numOfLevels);
      SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
    }

    pLevel->taskExecDoneNum = 0;
    pLevel->taskLaunchedNum = 0;

    int32_t numOfTasks = taosArrayGetSize(pLevel->subTasks);
    for (int32_t j = 0; j < numOfTasks; ++j) {
      SSchTask *pTask = taosArrayGet(pLevel->subTasks, j);
      if (NULL == pTask) {
        SCH_JOB_ELOG("fail to get the %dth task in level %d, taskNum:%d", j, i, numOfTasks);
        SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
      }

      SCH_LOCK_TASK(pTask);
      code = schChkUpdateRedirectCtx(pJob, pTask, NULL, rspCode);
      if (TSDB_CODE_SUCCESS != code) {
        SCH_UNLOCK_TASK(pTask);
        SCH_RET(code);
      }
      schResetTaskForRetry(pJob, pTask);

      SCH_LOCK(SCH_WRITE, &pTask->planLock);
      qClearSubplanExecutionNode(pTask->plan);
      SCH_UNLOCK(SCH_WRITE, &pTask->planLock);

      SCH_UNLOCK_TASK(pTask);
    }
  }

  SCH_RESET_JOB_LEVEL_IDX(pJob);
  
  SCH_JOB_DLOG("update job sId to %" PRId64 ", levelIdx:%d", pJob->seriousId, pJob->levelIdx);

  return TSDB_CODE_SUCCESS;
}

int32_t schHandleJobRetry(SSchJob *pJob, SSchTask *pTask, SDataBuf *pMsg, int32_t rspCode) {
  int32_t code = 0;
  bool    inRetry = false;

  taosMemoryFreeClear(pMsg->pData);
  taosMemoryFreeClear(pMsg->pEpSet);

  SCH_UNLOCK_TASK(pTask);

  SCH_TASK_DLOG("start to redirect all job tasks cause of error: %s", tstrerror(rspCode));

  SCH_ERR_JRET(schResetJobForRetry(pJob, pTask, rspCode, &inRetry));

  SCH_ERR_JRET(schLaunchJob(pJob));

  SCH_LOCK_TASK(pTask);

  atomic_store_8(&pJob->inRetry, 0);

  SCH_RET(code);

_return:

  SCH_LOCK_TASK(pTask);

  code = schProcessOnTaskFailure(pJob, pTask, code);

  if (inRetry) {
    atomic_store_8(&pJob->inRetry, 0);
  }

  SCH_RET(code);
}

bool schChkCurrentOp(SSchJob *pJob, int32_t op, int8_t sync) {
  bool r = false;
  SCH_LOCK(SCH_READ, &pJob->opStatus.lock);
  if (sync >= 0) {
    r = (pJob->opStatus.op == op) && (pJob->opStatus.syncReq == sync);
  } else {
    r = (pJob->opStatus.op == op);
  }
  SCH_UNLOCK(SCH_READ, &pJob->opStatus.lock);

  return r;
}

void schProcessOnOpEnd(SSchJob *pJob, SCH_OP_TYPE type, SSchedulerReq *pReq, int32_t errCode) {
  int32_t op = 0;

  switch (type) {
    case SCH_OP_EXEC:
      if (pReq && pReq->syncReq) {
        SCH_LOCK(SCH_WRITE, &pJob->opStatus.lock);
        op = atomic_val_compare_exchange_32(&pJob->opStatus.op, type, SCH_OP_NULL);
        if (SCH_OP_NULL == op || op != type) {
          SCH_JOB_ELOG("job not in %s operation, op:%s, status:%s", schGetOpStr(type), schGetOpStr(op),
                       jobTaskStatusStr(pJob->status));
        }
        SCH_UNLOCK(SCH_WRITE, &pJob->opStatus.lock);
        schDumpJobExecRes(pJob, pReq->pExecRes);
      }
      break;
    case SCH_OP_FETCH:
      if (pReq && pReq->syncReq) {
        SCH_LOCK(SCH_WRITE, &pJob->opStatus.lock);
        op = atomic_val_compare_exchange_32(&pJob->opStatus.op, type, SCH_OP_NULL);
        if (SCH_OP_NULL == op || op != type) {
          SCH_JOB_ELOG("job not in %s operation, op:%s, status:%s", schGetOpStr(type), schGetOpStr(op),
                       jobTaskStatusStr(pJob->status));
        }
        SCH_UNLOCK(SCH_WRITE, &pJob->opStatus.lock);
      }
      break;
    case SCH_OP_GET_STATUS:
      errCode = TSDB_CODE_SUCCESS;
      break;
    default:
      break;
  }

  if (errCode) {
    (void)schHandleJobFailure(pJob, errCode);  // handle internal
  }

  SCH_JOB_TLOG("job end %s operation with code:%s", schGetOpStr(type), tstrerror(errCode));
}

int32_t schProcessOnOpBegin(SSchJob *pJob, SCH_OP_TYPE type, SSchedulerReq *pReq) {
  int32_t code = 0;
  int8_t  status = SCH_GET_JOB_STATUS(pJob);

  switch (type) {
    case SCH_OP_EXEC:
      SCH_LOCK(SCH_WRITE, &pJob->opStatus.lock);
      if (SCH_OP_NULL != atomic_val_compare_exchange_32(&pJob->opStatus.op, SCH_OP_NULL, type)) {
        SCH_JOB_ELOG("job already in %s operation", schGetOpStr(pJob->opStatus.op));
        SCH_UNLOCK(SCH_WRITE, &pJob->opStatus.lock);
        schDirectPostJobRes(pReq, TSDB_CODE_APP_ERROR);
        SCH_ERR_RET(TSDB_CODE_APP_ERROR);
      }

      SCH_JOB_DLOG("job start %s operation", schGetOpStr(pJob->opStatus.op));

      pJob->opStatus.syncReq = pReq->syncReq;
      SCH_UNLOCK(SCH_WRITE, &pJob->opStatus.lock);
      break;
    case SCH_OP_FETCH:
      SCH_LOCK(SCH_WRITE, &pJob->opStatus.lock);
      if (SCH_OP_NULL != atomic_val_compare_exchange_32(&pJob->opStatus.op, SCH_OP_NULL, type)) {
        SCH_JOB_ELOG("job already in %s operation", schGetOpStr(pJob->opStatus.op));
        SCH_UNLOCK(SCH_WRITE, &pJob->opStatus.lock);
        schDirectPostJobRes(pReq, TSDB_CODE_APP_ERROR);
        SCH_ERR_RET(TSDB_CODE_APP_ERROR);
      }

      SCH_JOB_DLOG("job start %s operation", schGetOpStr(pJob->opStatus.op));

      pJob->userRes.fetchRes = pReq->pFetchRes;
      pJob->userRes.fetchFp = pReq->fetchFp;
      pJob->userRes.cbParam = pReq->cbParam;

      pJob->opStatus.syncReq = pReq->syncReq;
      SCH_UNLOCK(SCH_WRITE, &pJob->opStatus.lock);

      if (!SCH_JOB_NEED_FETCH(pJob)) {
        SCH_JOB_ELOG("no need to fetch data, status:%s", SCH_GET_JOB_STATUS_STR(pJob));
        SCH_ERR_RET(TSDB_CODE_APP_ERROR);
      }

      if (status != JOB_TASK_STATUS_PART_SUCC && status != JOB_TASK_STATUS_FETCH) {
        SCH_JOB_ELOG("job status error for fetch, status:%s", jobTaskStatusStr(status));
        SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
      }

      break;
    case SCH_OP_GET_STATUS:
      if (pJob->status < JOB_TASK_STATUS_INIT || pJob->levelNum <= 0 || NULL == pJob->levels) {
        qDebug("jobId:0x%" PRIx64 ", job not initialized or not executable job", pJob->refId);
        SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
      }
      return TSDB_CODE_SUCCESS;
    default:
      SCH_JOB_ELOG("unknown operation type %d", type);
      SCH_ERR_RET(TSDB_CODE_APP_ERROR);
  }

  if (schJobNeedToStop(pJob, &status)) {
    SCH_JOB_ELOG("abort op %s cause of job need to stop, status:%s", schGetOpStr(type), jobTaskStatusStr(status));
    SCH_ERR_RET(TSDB_CODE_SCH_IGNORE_ERROR);
  }

  return TSDB_CODE_SUCCESS;
}

void schProcessOnCbEnd(SSchJob *pJob, SSchTask *pTask, int32_t errCode) {
  if (pTask) {
    SCH_UNLOCK_TASK(pTask);
  }

  if (errCode) {
    (void)schHandleJobFailure(pJob, errCode);  // ignore error
  }

  if (pJob) {
    (void)schReleaseJob(pJob->refId);  // ignore error
  }
}

int32_t schProcessOnCbBegin(SSchJob **job, SSchTask **task, uint64_t qId, int64_t rId, uint64_t tId) {
  int32_t code = 0;
  int8_t  status = 0;

  SSchTask *pTask = NULL;
  SSchJob  *pJob = NULL;

  (void)schAcquireJob(rId, &pJob);
  if (NULL == pJob) {
    qWarn("QID:0x%" PRIx64 ", TID:0x%" PRIx64 "job no exist, may be dropped, jobId:0x%" PRIx64, qId, tId, rId);
    SCH_ERR_RET(TSDB_CODE_QRY_JOB_NOT_EXIST);
  }

  if (schJobNeedToStop(pJob, &status)) {
    SCH_TASK_DLOG("will not do further processing cause of job status %s", jobTaskStatusStr(status));
    SCH_ERR_JRET(TSDB_CODE_SCH_IGNORE_ERROR);
  }

  SCH_ERR_JRET(schGetTaskInJob(pJob, tId, &pTask));

  SCH_LOCK_TASK(pTask);

  *job = pJob;
  *task = pTask;

  return TSDB_CODE_SUCCESS;

_return:

  if (pTask) {
    SCH_UNLOCK_TASK(pTask);
  }
  if (pJob) {
    (void)schReleaseJob(rId);  // ignore error
  }

  SCH_RET(code);
}
