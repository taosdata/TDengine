/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include "plannodes.h"
#include "query.h"
#include "schInt.h"
#include "schPhase.h"
#include "tmsg.h"
#include "tref.h"
#include "trpc.h"

static void schFreeCorrelationValues(SArray* pValues) {
  if (NULL == pValues) {
    return;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pValues); ++i) {
    SCorrelationWireValue* pValue = taosArrayGet(pValues, i);
    taosMemoryFree(pValue->pData);
  }
  taosArrayDestroy(pValues);
}

static int32_t schCopyCorrelationValues(const SArray* pSource, SArray** ppValues) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SArray* pValues = NULL;
  void*   pPendingData = NULL;
  QUERY_CHECK_NULL(ppValues, code, lino, _return, TSDB_CODE_INVALID_PARA);
  *ppValues = NULL;
  if (NULL == pSource) {
    goto _return;
  }

  pValues = taosArrayInit(taosArrayGetSize(pSource), sizeof(SCorrelationWireValue));
  QUERY_CHECK_NULL(pValues, code, lino, _return, terrno);
  for (int32_t i = 0; i < taosArrayGetSize(pSource); ++i) {
    SCorrelationWireValue value = *(const SCorrelationWireValue*)taosArrayGet(pSource, i);
    value.pData = NULL;
    if (value.dataLen > 0) {
      value.pData = taosMemoryMalloc(value.dataLen);
      QUERY_CHECK_NULL(value.pData, code, lino, _return, terrno);
      pPendingData = value.pData;
      memcpy(value.pData, ((const SCorrelationWireValue*)taosArrayGet(pSource, i))->pData, value.dataLen);
    }
    void* pPushed = taosArrayPush(pValues, &value);
    QUERY_CHECK_NULL(pPushed, code, lino, _return, terrno);
    pPendingData = NULL;
  }
  *ppValues = pValues;
  pValues = NULL;

_return:
  taosMemoryFree(pPendingData);
  schFreeCorrelationValues(pValues);
  if (TSDB_CODE_SUCCESS != code) {
    if (lino > 0) {
      qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    } else {
      qError("%s failed since %s", __func__, tstrerror(code));
    }
  }
  return code;
}

static int32_t schCopySubquerySources(const SArray* pSource, SArray** ppSources) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SArray* pCopied = NULL;
  SNode*  pPending = NULL;
  QUERY_CHECK_NULL(ppSources, code, lino, _return, TSDB_CODE_INVALID_PARA);
  *ppSources = NULL;
  if (0 == taosArrayGetSize(pSource)) {
    return code;
  }

  pCopied = taosArrayInit(taosArrayGetSize(pSource), POINTER_BYTES);
  QUERY_CHECK_NULL(pCopied, code, lino, _return, terrno);
  for (int32_t i = 0; i < taosArrayGetSize(pSource); ++i) {
    const SNode* pNode = taosArrayGetP(pSource, i);
    QUERY_CHECK_NULL(pNode, code, lino, _return, TSDB_CODE_QRY_INVALID_CORRELATION_NODE);
    QUERY_CHECK_CONDITION(QUERY_NODE_DOWNSTREAM_SOURCE == nodeType(pNode), code, lino, _return,
                          TSDB_CODE_QRY_INVALID_CORRELATION_NODE);
    SCH_ERR_JRET(nodesCloneNode(pNode, &pPending));
    QUERY_CHECK_NULL(taosArrayPush(pCopied, &pPending), code, lino, _return, terrno);
    pPending = NULL;
  }
  *ppSources = pCopied;
  pCopied = NULL;

_return:
  nodesDestroyNode(pPending);
  taosArrayDestroyP(pCopied, (FDelete)nodesDestroyNode);
  if (TSDB_CODE_SUCCESS != code) {
    if (lino > 0) {
      qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    } else {
      qError("%s failed since %s", __func__, tstrerror(code));
    }
  }
  return code;
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

  if (SCH_IS_SUBQ_JOB(pJob)) {
    schUpdateJobErrCode((SSchJob*)pJob->parent, errCode);
  }

  SCH_JOB_DLOG("job errCode updated to %s", tstrerror(errCode));
}

bool schJobDone(SSchJob *pJob) {
  int8_t status = SCH_GET_JOB_STATUS(pJob);

  if (status == JOB_TASK_STATUS_FAIL || status == JOB_TASK_STATUS_DROP || status == JOB_TASK_STATUS_SUCC) {
    return true;
  }

  if (SCH_IS_SUBQ_JOB(pJob)) {
    return schJobDone((SSchJob*)pJob->parent);
  }

  return false;
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
    SSchLevel* pLevel = taosArrayGet(pJob->levels, i);
    if (NULL == pLevel) {
      SCH_JOB_ELOG("fail to get the %dth level, levelNum: %d", i, pJob->levelNum);
      SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
    }

    for (int32_t m = 0; m < pLevel->taskNum; ++m) {
      SSchTask* pTask = taosArrayGet(pLevel->subTasks, m);
      if (NULL == pTask) {
        SCH_JOB_ELOG("fail to get the %dth task in level %d, taskNum: %d", m, pLevel->level, pLevel->taskNum);
        SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
      }

      SSubplan* pPlan = pTask->plan;
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

      int32_t n = 0;
      SNode*  childNode = NULL;
      FOREACH(childNode, pPlan->pChildren) {
        SSubplan* child = (SSubplan*)childNode;
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
        ++n;
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

      n = 0;
      SNode* parentNode = NULL;
      FOREACH(parentNode, pPlan->pParents) {
        SSubplan* parent = (SSubplan*)parentNode;
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
        ++n;
      }

      SCH_TASK_DLOG("level:%d, parentNum:%d, childNum:%d", i, parentNum, childNum);
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

  qDebug("QID:0x%" PRIx64 ", job seriesId set to SID:%" PRId64 ", levelIdx:%d", pJob->queryId, pJob->seriesId,
         pJob->levelIdx);

  SSchLevel      level = {0};
  SNodeListNode *plans = NULL;
  int32_t        taskNum = 0;
  int32_t        totalTaskNum = 0;
  SSchLevel     *pLevel = NULL;

  level.status = JOB_TASK_STATUS_INIT;

  int32_t i = 0;
  SNode*  levelNode = NULL;
  FOREACH(levelNode, pDag->pSubplans) {
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

    plans = (SNodeListNode*)levelNode;
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

    int32_t n = 0;
    SNode*  planNode = NULL;
    FOREACH(planNode, plans->pNodeList) {
      SSubplan* plan = (SSubplan*)planNode;

      SCH_ERR_JRET(schValidateSubplan(pJob, plan, pLevel->level, n, taskNum));
      schSetJobType(pJob, plan->subplanType);

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

      if (NULL != pJob->parent) {
        code = taosHashPut(((SSchJob*)pJob->parent)->taskList, &pTask->taskId, sizeof(pTask->taskId), &pTask, POINTER_BYTES);
        if (0 != code) {
          SCH_TASK_ELOG("taosHashPut to parent taskList failed, taskIdx:%d, error:%s", n, tstrerror(code));
          SCH_ERR_JRET(code);
        }
      }
      
      ++pJob->taskNum;
      ++n;
    }

    SCH_JOB_DLOG("level %d initialized, taskNum:%d", i, taskNum);
    ++i;
  }

  if (!SCH_JOB_GOT_SUB_JOBS(pJob) && totalTaskNum != pDag->numOfSubplans) {
    SCH_JOB_ELOG("totalTaskNum %d mismatch with numOfSubplans %d", totalTaskNum, pDag->numOfSubplans);
    SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SCH_ERR_JRET(schBuildTaskRalation(pJob, planToTask));

_return:

  if (planToTask) {
    taosHashCleanup(planToTask);
  }

  SCH_RET(code);
}

void schDumpSubJobsExecRes(SSchJob *pParent, SExecResult *pRes) {
  int32_t subNum = taosArrayGetSize(pParent->subJobs);
  for (int32_t i = 0; i < subNum; ++i) {
    SSchJob* pJob = taosArrayGetP(pParent->subJobs, i);
    if (NULL == pJob) {
      continue;
    }
    
    SCH_LOCK(SCH_WRITE, &pJob->resLock);
    pRes->execDispatched |= atomic_load_8(&pJob->execDispatched) != 0;
    if (pJob->execRes.res) {
      if (pRes->res) {
        void* p = taosArrayAddAll((SArray*)pRes->res, (SArray*)pJob->execRes.res);
        SCH_JOB_DLOG("copy sub job %p execRes %p to output", pJob, pJob->execRes.res);
      } else {
        TSWAP(pRes->res, pJob->execRes.res);
        pRes->msgType = pJob->execRes.msgType;
        SCH_JOB_DLOG("move sub job %p execRes %p to output", pJob, pRes->res);
      }
    }

    if (NULL == pRes->extErrMsg && NULL != pJob->execRes.extErrMsg) {
      TSWAP(pRes->extErrMsg, pJob->execRes.extErrMsg);
    }
    if (NULL == pRes->extPushdownDiag && NULL != pJob->execRes.extPushdownDiag) {
      TSWAP(pRes->extPushdownDiag, pJob->execRes.extPushdownDiag);
    }

    SCH_UNLOCK(SCH_WRITE, &pJob->resLock);
  }
}

void schDumpJobExecRes(SSchJob *pJob, SExecResult *pRes) {
  pRes->code = atomic_load_32(&pJob->errCode);
  pRes->execDispatched = atomic_load_8(&pJob->execDispatched) != 0;
  pRes->numOfRows = pJob->resNumOfRows;
  pRes->numTouchedObjects = atomic_load_32(&pJob->resNumTouchedObjects);

  SCH_LOCK(SCH_WRITE, &pJob->resLock);
  pRes->res = pJob->execRes.res;
  pRes->msgType = pJob->execRes.msgType;
  pRes->numOfBytes = pJob->execRes.numOfBytes;
  pRes->extErrMsg = pJob->execRes.extErrMsg;
  pRes->extPushdownDiag = pJob->execRes.extPushdownDiag;
  pJob->execRes.res = NULL;
  pJob->execRes.extErrMsg = NULL;
  pJob->execRes.extPushdownDiag = NULL;
  
  SCH_JOB_DLOG("move job %p execRes %p to output", pJob, pRes->res);
  
  if (SCH_JOB_GOT_SUB_JOBS(pJob)) {
    schDumpSubJobsExecRes(pJob, pRes);
  }
  SCH_UNLOCK(SCH_WRITE, &pJob->resLock);

  SCH_JOB_DLOG("exec result dumped, code:%s", tstrerror(pRes->code));
}

int32_t schDumpJobFetchRes(SSchJob *pJob, void **pData) {
  int32_t code = 0;

  SCH_LOCK(SCH_WRITE, &pJob->resLock);

  pJob->fetched = true;

  if (pJob->fetchRes && ((SRetrieveTableRsp *)pJob->fetchRes)->completed) {
    SCH_UPDATE_JOB_PHASE_IF_CHANGED(pJob, QUERY_PHASE_DONE);
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

// Defined below with the other phase helpers.
static void schPhaseWriteBegin(SSchJob *pJob);
static void schPhaseWriteEnd(SSchJob *pJob);

// Enter the fetch main phase and stamp its start time once. Every path that
// resumes or starts fetching must go through here: the client-driven fetch
// entry and the retry recovery path (a job that failed before its first block
// is re-executed, and fetching resumes from schLaunchFetchTask without a new
// fetch request). Without the latter, the resumed fetch keeps the re-execution
// phase and its start time, so show queries attributes the fetch window to
// execute.
//
// The phase is refreshed only when the job is not already in the fetch stage:
// one large result is fetched block by block and re-entering IN_PROGRESS would
// keep resetting phaseStartTime although the reported phase never changed.
//
// The phase is read and updated inside one phaseWriteLock critical section.
// Sampling it first and CAS-ing afterwards would leave a window in which a
// concurrent transition (a retry re-executing the query, or the previous fetch
// moving to RETURNED/DONE) makes the CAS fail silently and the fetch stage
// never gets stamped.
void schEnterFetchPhase(SSchJob *pJob) {
  if (pJob == NULL) {
    return;
  }
  schPhaseWriteBegin(pJob);
  int32_t curPhase = atomic_load_32(&pJob->execPhase);
  if (curPhase == QUERY_PHASE_DONE) {
    // DONE is terminal: a finished query must not be resurrected to fetch.
  } else if (curPhase == QUERY_PHASE_FETCH_RETURNED) {
    // A later block arrives after the previous one was reported as returned:
    // move back to in_progress but keep the single fetch stage timer, so the
    // request does not stay in returned while it is fetching again.
    atomic_store_32(&pJob->execPhase, QUERY_PHASE_FETCH_IN_PROGRESS);
  } else if (!QUERY_PHASE_IS_FETCH_STAGE(curPhase)) {
    // Entering the stage from a non-fetch phase stamps a new start time; a
    // retry that resumes fetching keeps the original fetch start time because
    // the whole fetch stage is reported as one main phase.
    atomic_store_32(&pJob->execPhase, QUERY_PHASE_FETCH_IN_PROGRESS);
    atomic_store_64(&pJob->phaseStartTime, taosGetTimestampMs());
  }
  schPhaseWriteEnd(pJob);
}

int32_t schNotifyUserFetchRes(SSchJob *pJob) {
  void *pRes = NULL;

  int32_t code = schDumpJobFetchRes(pJob, &pRes);
  if (TSDB_CODE_SUCCESS != code && TSDB_CODE_SUCCESS == atomic_load_32(&pJob->errCode)) {
    atomic_store_32(&pJob->errCode, code);
  }
  // Fetch response received, transition to RETURNED state. Keep the fetch stage
  // timer: show queries reports the whole fetch stage as one main phase, so the
  // sub-state switch must not restart phaseStartTime. Do not overwrite the
  // terminal DONE phase when this response completed the last fetch; otherwise
  // a finished query would report fetch/returned forever. When the phase is
  // already DONE the fetch timer is final and is not touched again.
  //
  // A failed fetch response is not reported as returned: the phase is only
  // advanced when the result was actually handed over (code == SUCCESS).
  // The read and the update happen in one critical section so a phase sampled
  // before a concurrent DONE transition cannot overwrite it.
  if (TSDB_CODE_SUCCESS == code) {
    schPhaseWriteBegin(pJob);
    int32_t curPhase = atomic_load_32(&pJob->execPhase);
    if (QUERY_PHASE_IS_FETCH_STAGE(curPhase) && curPhase != QUERY_PHASE_FETCH_RETURNED) {
      atomic_store_32(&pJob->execPhase, QUERY_PHASE_FETCH_RETURNED);
    }
    schPhaseWriteEnd(pJob);
  }

  SCH_JOB_DLOG("sch start to invoke fetch cb, code:%s", tstrerror(pJob->errCode));
  (*pJob->userRes.fetchFp)(pRes, pJob->userRes.cbParam, atomic_load_32(&pJob->errCode));
  SCH_JOB_DLOG("sch end from fetch cb, code:%s", tstrerror(pJob->errCode));

  return TSDB_CODE_SUCCESS;
}

void schPostJobRes(SSchJob *pJob, SCH_OP_TYPE op) {
  int32_t code = 0;
  if (!SCH_IS_PARENT_JOB(pJob)) {
    return;
  }
  
  SCH_LOCK(SCH_WRITE, &pJob->opStatus.lock);

  if (SCH_OP_NULL == pJob->opStatus.op) {
    SCH_JOB_TLOG("job not in any operation, no need to post job res, status:%s", jobTaskStatusStr(pJob->status));
    goto _return;
  }

  if (SCH_OP_NULL != op && pJob->opStatus.op != op) {
    SCH_JOB_ELOG("job in operation %s mismatch with expected %s", schGetOpStr(pJob->opStatus.op), schGetOpStr(op));
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

  // A user callback can remove the job from the reference registry.
  // Retain the cleanup reference before publishing the terminal result.
  schStopQueryTasks(pJob);
  schPostJobRes(pJob, 0);

  SCH_RET(TSDB_CODE_SCH_IGNORE_ERROR);
}

int32_t schHandleJobFailure(SSchJob *pJob, int32_t errCode) {
  if (TSDB_CODE_SCH_IGNORE_ERROR == errCode) {
    return TSDB_CODE_SCH_IGNORE_ERROR;
  }

  (void)schSwitchJobStatus(pJob, JOB_TASK_STATUS_FAIL, &errCode);  // ignore error
  if (SCH_IS_SUBQ_JOB(pJob)) {
    SCH_JOB_DLOG("set parent job FAILED since the %dth subJob failed, error:%s", pJob->subJobId, tstrerror(errCode));
    (void)schSwitchJobStatus((SSchJob*)pJob->parent, JOB_TASK_STATUS_FAIL, &errCode);  // ignore error
  }

  schStopQueryTasks(pJob);

  return TSDB_CODE_SCH_IGNORE_ERROR;
}

int32_t schProcessOnJobDropped(SSchJob *pJob, int32_t errCode) { SCH_RET(schProcessOnJobFailure(pJob, errCode)); }

int32_t schHandleJobDrop(SSchJob *pJob, int32_t errCode) {
  if (TSDB_CODE_SCH_IGNORE_ERROR == errCode) {
    return TSDB_CODE_SCH_IGNORE_ERROR;
  }

  (void)schSwitchJobStatus(pJob, JOB_TASK_STATUS_DROP, &errCode);  // ignore error
  schStopQueryTasks(pJob);

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
  if (NULL == rsp->tbVerInfo && NULL == rsp->extErrMsg && NULL == rsp->extPushdownDiag) {
    return TSDB_CODE_SUCCESS;
  }

  SCH_LOCK(SCH_WRITE, &pJob->resLock);

  if (rsp->tbVerInfo) {
    if (NULL == pJob->execRes.res) {
      pJob->execRes.res = taosArrayInit(pJob->taskNum, sizeof(STbVerInfo));
      if (NULL == pJob->execRes.res) {
        SCH_UNLOCK(SCH_WRITE, &pJob->resLock);
        SCH_ERR_RET(terrno);
      }
      SCH_JOB_DLOG("init job %p execRes %p", pJob, pJob->execRes.res);
    }

    pJob->execRes.msgType = TDMT_SCH_QUERY;

    if (NULL == taosArrayAddBatch((SArray *)pJob->execRes.res, taosArrayGet(rsp->tbVerInfo, 0),
                                  taosArrayGetSize(rsp->tbVerInfo))) {
      SCH_UNLOCK(SCH_WRITE, &pJob->resLock);
      SCH_ERR_RET(terrno);
    }

    taosArrayDestroy(rsp->tbVerInfo);
    rsp->tbVerInfo = NULL;
  }

  if (NULL != rsp->extErrMsg) {
    if (NULL == pJob->execRes.extErrMsg) {
      TSWAP(pJob->execRes.extErrMsg, rsp->extErrMsg);
    } else {
      taosMemoryFreeClear(rsp->extErrMsg);
    }
  }

  if (NULL != rsp->extPushdownDiag) {
    if (NULL == pJob->execRes.extPushdownDiag) {
      TSWAP(pJob->execRes.extPushdownDiag, rsp->extPushdownDiag);
    } else {
      taosMemoryFreeClear(rsp->extPushdownDiag);
    }
  }

  SCH_UNLOCK(SCH_WRITE, &pJob->resLock);

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

// Serialize phase writers and publish (execPhase, phaseStartTime) through a
// seqlock: phaseSeq is odd while a writer updates the pair and even when it is
// stable. Readers never observe a new phase paired with an old start time.
//
// The critical section only performs atomics (no allocation, callback or other
// lock), so waiting writers make progress as soon as the holder finishes its
// few instructions. Writers must not call another phase writer from inside the
// critical section (all helpers below are non-reentrant), and readers must not
// call schReadJobPhase() from a critical section either.
static void schPhaseLockAcquire(SSchJob *pJob) {
  int32_t spins = 0;
  while (atomic_val_compare_exchange_32(&pJob->phaseWriteLock, 0, 1) != 0) {
    ++spins;
    if (spins > SCH_PHASE_LOCK_SPIN_TIMES) {
      // The holder is not making progress within the expected window, so it
      // was most likely preempted. Back off instead of hammering the CPU and
      // report the stall once per acquisition; the lock is not abandoned
      // because a reader still needs a consistent pair to return.
      if (spins == SCH_PHASE_LOCK_SPIN_TIMES + 1) {
        qError("jobId:0x%" PRIx64 ", phase write lock still held after %d attempts, the holder was likely "
               "preempted",
               pJob->refId, SCH_PHASE_LOCK_SPIN_TIMES);
      }
      taosMsleep(10);
    } else {
      // Yield the CPU instead of a no-op sleep: taosMsleep(0) does not reliably
      // give the writer holding the lock a chance to run.
      taosMsleep(1);
    }
  }
}

static void schPhaseWriteBegin(SSchJob *pJob) {
  schPhaseLockAcquire(pJob);
  // Increment immediately after acquiring the lock: from here on phaseSeq is
  // odd and readers retry. The few instructions between the CAS and this add
  // are safe because no data store has happened yet, so a reader that passes
  // the check reads the previous fully published pair. Each write performs two
  // increments, so a reader that observes a writer at all sees a different
  // sequence value and retries.
  //
  // The field is int32_t (the type of the atomic API). It is only compared for
  // equality/parity, and casting to uint32_t for those comparisons makes a
  // wrap-around harmless.
  (void)atomic_add_fetch_32(&pJob->phaseSeq, 1);
}

static void schPhaseWriteEnd(SSchJob *pJob) {
  (void)atomic_add_fetch_32(&pJob->phaseSeq, 1);
  atomic_store_32(&pJob->phaseWriteLock, 0);
}

void schPublishJobPhase(SSchJob *pJob, int32_t phase) {
  if (pJob == NULL) {
    return;
  }
  schPhaseWriteBegin(pJob);
  atomic_store_32(&pJob->execPhase, phase);
  atomic_store_64(&pJob->phaseStartTime, taosGetTimestampMs());
  schPhaseWriteEnd(pJob);
}

void schPublishJobPhaseIfChanged(SSchJob *pJob, int32_t newPhase, bool keepTime) {
  if (pJob == NULL) {
    return;
  }
  schPhaseWriteBegin(pJob);
  if (atomic_load_32(&pJob->execPhase) != newPhase) {
    atomic_store_32(&pJob->execPhase, newPhase);
    if (!keepTime) {
      atomic_store_64(&pJob->phaseStartTime, taosGetTimestampMs());
    }
  }
  schPhaseWriteEnd(pJob);
}

bool schCasJobPhase(SSchJob *pJob, int32_t expected, int32_t newPhase, bool keepTime) {
  if (pJob == NULL) {
    return false;
  }
  bool swapped = false;
  schPhaseWriteBegin(pJob);
  if (atomic_val_compare_exchange_32(&pJob->execPhase, expected, newPhase) == expected) {
    if (!keepTime) {
      atomic_store_64(&pJob->phaseStartTime, taosGetTimestampMs());
    }
    swapped = true;
  }
  schPhaseWriteEnd(pJob);
  return swapped;
}

void schReadJobPhase(SSchJob *pJob, int32_t *pPhase, int64_t *pStartTime) {
  if (pJob == NULL || pPhase == NULL || pStartTime == NULL) {
    return;
  }
  for (int32_t i = 0; i < SCH_PHASE_READ_RETRY_TIMES; ++i) {
    uint32_t seq1 = (uint32_t)atomic_load_32(&pJob->phaseSeq);
    if (seq1 & 1U) {
      taosMsleep(1);
      continue;
    }
    int32_t phase = atomic_load_32(&pJob->execPhase);
    int64_t start = atomic_load_64(&pJob->phaseStartTime);
    uint32_t seq2 = (uint32_t)atomic_load_32(&pJob->phaseSeq);
    if (seq1 == seq2) {
      *pPhase = phase;
      *pStartTime = start;
      return;
    }
    // A writer was active while the pair was read: yield before retrying.
    taosMsleep(1);
  }
  // Retries exhausted (a writer stayed active for the whole window). Reading
  // the pair while holding the writer lock still returns a consistent
  // snapshot; two independent lock-free loads could return a new phase paired
  // with an old start time, which is exactly what this reader exists to avoid.
  // Readers must never be invoked from inside a phase write critical section:
  // phaseWriteLock is not recursive, so the acquisition below would wait on
  // the lock the caller already holds.
  qWarn("jobId:0x%" PRIx64 ", phase snapshot unstable after %d retries, read it under the writer lock",
        pJob->refId, SCH_PHASE_READ_RETRY_TIMES);
  schPhaseLockAcquire(pJob);
  // Follow the writer protocol on this fallback path as well: phaseSeq is odd
  // while the pair is read under the lock and even again afterwards, so the
  // degraded read is indistinguishable from a regular publication.
  (void)atomic_add_fetch_32(&pJob->phaseSeq, 1);
  *pPhase = atomic_load_32(&pJob->execPhase);
  *pStartTime = atomic_load_64(&pJob->phaseStartTime);
  (void)atomic_add_fetch_32(&pJob->phaseSeq, 1);
  atomic_store_32(&pJob->phaseWriteLock, 0);
}

/* Main phase of a phase code. Sub-phases (4x/5x/6x) belong to their parent.
 *
 * This mapping relies on the sub-phase values of one main phase being
 * contiguous and on the three ranges not overlapping. The static assertions
 * below pin the boundary ordering so a future enum change cannot silently
 * route a sub-phase to the wrong main phase.
 */
// Compile-time checks. Adding or reordering a sub-phase in EQueryExecPhase
// requires updating these assertions and schGetMainPhase() together; the build
// fails otherwise.
//
// NOTE: SCH_PHASE_CHECK is for this C file only. Do not move it into a public
// header: the fallback form is a negative-size array, which in C++ becomes a
// variable-length array extension instead of a hard error, and it relies on C
// enum constants.
#if defined(__GNUC__) || defined(__clang__)
// GCC/Clang provide _Static_assert as an extension in gnu99, and this form
// makes the checks participate in the build instead of being an unused type.
#define SCH_PHASE_CHECK(_name, _cond) _Static_assert((_cond), #_name)
#else
#define SCH_PHASE_CHECK(_name, _cond) typedef char schPhaseCheck##_name[(_cond) ? 1 : -1]
#endif

// The whole schedule range ends before the execute range starts, the execute
// range ends before fetch starts, and the fetch range stays below DONE. These
// three pairs pin the ranges that schGetMainPhase() routes on as disjoint.
SCH_PHASE_CHECK(ScheduleEndExecOrder, QUERY_PHASE_SCHEDULE_NODE_SELECTION < QUERY_PHASE_EXEC_DATA_QUERY);
SCH_PHASE_CHECK(ExecEndFetchOrder, QUERY_PHASE_EXEC_WAITING_CHILDREN < QUERY_PHASE_FETCH_IN_PROGRESS);
// Main phases must stay ordered, schGetMainPhase relies on it.
SCH_PHASE_CHECK(MainOrder, QUERY_PHASE_SCHEDULE < QUERY_PHASE_EXECUTE && QUERY_PHASE_EXECUTE < QUERY_PHASE_FETCH &&
                               QUERY_PHASE_FETCH < QUERY_PHASE_DONE);
// Sub-phases must not fall below their own main phase.
SCH_PHASE_CHECK(ScheduleInnerOrder, QUERY_PHASE_SCHEDULE_ANALYSIS > QUERY_PHASE_SCHEDULE &&
                                        QUERY_PHASE_SCHEDULE_ANALYSIS <= QUERY_PHASE_SCHEDULE_NODE_SELECTION);
SCH_PHASE_CHECK(ExecInnerOrder, QUERY_PHASE_EXEC_DATA_QUERY <= QUERY_PHASE_EXEC_WAITING_CHILDREN);
SCH_PHASE_CHECK(FetchInnerOrder, QUERY_PHASE_FETCH_IN_PROGRESS <= QUERY_PHASE_FETCH_RETURNED);
// schGetMainPhase routes by range, so the sub-phases of one main phase must be
// contiguous: a gap would silently leave the missing value unmapped.
SCH_PHASE_CHECK(ScheduleInnerContiguous, QUERY_PHASE_SCHEDULE_PLANNING == QUERY_PHASE_SCHEDULE_ANALYSIS + 1 &&
                                             QUERY_PHASE_SCHEDULE_NODE_SELECTION == QUERY_PHASE_SCHEDULE_PLANNING + 1);
SCH_PHASE_CHECK(ExecInnerContiguous, QUERY_PHASE_EXEC_MERGE_QUERY == QUERY_PHASE_EXEC_DATA_QUERY + 1 &&
                                         QUERY_PHASE_EXEC_WAITING_CHILDREN == QUERY_PHASE_EXEC_MERGE_QUERY + 1);
SCH_PHASE_CHECK(FetchInnerContiguous, QUERY_PHASE_FETCH_RETURNED == QUERY_PHASE_FETCH_IN_PROGRESS + 1);

int32_t schGetMainPhase(int32_t phase) {
  // The fetch main phase value itself must map to fetch as well, so this
  // routing stays consistent with QUERY_PHASE_IS_FETCH_STAGE().
  if (phase == QUERY_PHASE_FETCH) {
    return QUERY_PHASE_FETCH;
  }
  if (phase >= QUERY_PHASE_SCHEDULE_ANALYSIS && phase <= QUERY_PHASE_SCHEDULE_NODE_SELECTION) {
    return QUERY_PHASE_SCHEDULE;
  }
  if (phase >= QUERY_PHASE_EXEC_DATA_QUERY && phase <= QUERY_PHASE_EXEC_WAITING_CHILDREN) {
    return QUERY_PHASE_EXECUTE;
  }
  if (phase >= QUERY_PHASE_FETCH_IN_PROGRESS && phase <= QUERY_PHASE_FETCH_RETURNED) {
    return QUERY_PHASE_FETCH;
  }
  return phase;
}

/* Subjobs are not registered in jobRef: their phases are stamped on the subjob
 * object, while heartbeat reads the parent job. Without this promotion the
 * parent would keep reporting schedule/planning for the whole subjob execution.
 *
 * Priority rule: while a subjob runs the parent only reports the coarse
 * execute/waiting_children phase. A subjob phase is never copied into the
 * parent, and the parent phase never moves back to an earlier main phase
 * (e.g. from execute back to schedule) when the subjobs finish.
 */
void schPromoteParentForSubJobs(SSchJob *pParent) {
  if (pParent == NULL) {
    return;
  }
  // Only a parent that actually reached a schedule sub-phase is promoted: NONE,
  // PARSE/CATALOG/PLAN (not yet scheduled), the coarse SCHEDULE main-phase value
  // itself and DONE must keep their phase. The explicit sub-phase range matches
  // the schedule range used by schLaunchJobImpl(), so the two places cannot
  // disagree on what "still scheduling" means.
  // The promotion happens when the first subjob query message is actually
  // dispatched, not when its tasks are merely queued, so the phase means
  // "a subjob is running". Later subjobs find the parent already in execute
  // and do not restart the timer: the phase is a coarse marker for the whole
  // subjob chain, stamped once.
  //
  // Compare-and-swap so concurrent dispatches cannot move the parent backwards
  // or clobber a phase another thread already advanced. Sampling and CAS are
  // not atomic: a CAS failure is retried with a fresh sample so the promotion
  // is not lost when another path merely moved the parent inside the execute
  // main phase (e.g. to EXEC_DATA_QUERY) between the two steps. Retrying from
  // that sample still never moves the parent to an earlier main phase.
  for (int32_t attempt = 0; attempt < SCH_PHASE_PROMOTE_RETRY_TIMES; ++attempt) {
    int32_t curPhase = SCH_GET_JOB_PHASE(pParent);
    if (curPhase == QUERY_PHASE_EXEC_WAITING_CHILDREN) {
      return;
    }
    bool promoting = curPhase >= QUERY_PHASE_SCHEDULE_ANALYSIS && curPhase <= QUERY_PHASE_SCHEDULE_NODE_SELECTION;
    // A sample that already left the schedule main phase is authoritative and
    // must not be overwritten. An execute sub-phase however is exactly the
    // "stuck between execute sub-phases" case: promotion within the same main
    // phase is still monotonic, so it is retried from the fresh sample.
    bool retryExecute = !pParent->earlyMainTasks && !promoting && schGetMainPhase(curPhase) == QUERY_PHASE_EXECUTE;
    if (!promoting && !retryExecute) {
      qDebug("QID:0x%" PRIx64 ", parent phase %d is not in schedule/execute, skip promotion", pParent->queryId,
             curPhase);
      return;
    }
    if (schCasJobPhase(pParent, curPhase, QUERY_PHASE_EXEC_WAITING_CHILDREN, false)) {
      return;
    }
    qDebug("QID:0x%" PRIx64 ", parent promotion retry, phase %d changed concurrently", pParent->queryId, curPhase);
  }
  qDebug("QID:0x%" PRIx64 ", parent promotion skipped after %d retries", pParent->queryId,
         SCH_PHASE_PROMOTE_RETRY_TIMES);
}

int32_t schInitJobDependencies(SSchJob *pJob) {
  if (pJob->attr.inlineTaskLaunch || !pJob->pDag->subqueryDepsKnown || !SCH_JOB_GOT_SUB_JOBS(pJob)) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t count = taosArrayGetSize(pJob->subJobs);
  for (int32_t i = 0; i < count; ++i) {
    SSchJob* pSub = taosArrayGetP(pJob->subJobs, i);
    if (!pSub->pDag->subqueryDepsKnown) return TSDB_CODE_SUCCESS;
  }
  for (int32_t i = 0; i <= count; ++i) {
    SSchJob* pSub = i == count ? pJob : taosArrayGetP(pJob->subJobs, i);
    SArray* pDeps = pSub->pDag->pSubqueryDeps;
    pSub->pendingDependencies = taosArrayGetSize(pDeps);
    for (int32_t j = 0; j < taosArrayGetSize(pDeps); ++j) {
      int32_t producer = *(int32_t*)taosArrayGet(pDeps, j);
      if (producer < 0 || producer >= i) return TSDB_CODE_PLAN_INTERNAL_ERROR;
      for (int32_t k = 0; k < j; ++k) {
        if (producer == *(int32_t*)taosArrayGet(pDeps, k)) return TSDB_CODE_PLAN_INTERNAL_ERROR;
      }
      if (i == count) continue;
      SSchJob* pProducer = taosArrayGetP(pJob->subJobs, producer);
      if (NULL == pProducer->dependentJobs) {
        pProducer->dependentJobs = taosArrayInit(4, sizeof(int32_t));
        if (NULL == pProducer->dependentJobs) return terrno;
      }
      if (NULL == taosArrayPush(pProducer->dependentJobs, &i)) return terrno;
    }
  }
  if (SCH_IS_QUERY_JOB(pJob) && pJob->attr.explainMode == EXPLAIN_MODE_DISABLE) {
    for (int32_t i = 0; i < taosArrayGetSize(pJob->levels); ++i) {
      SSchLevel* level = taosArrayGet(pJob->levels, i);
      for (int32_t j = 0; j < level->taskNum; ++j) {
        SSchTask* task = taosArrayGet(level->subTasks, j);
        if (!task->plan->subqueryDepsKnown) continue;
        for (int32_t k = 0; k < taosArrayGetSize(task->plan->pSubqueryDeps); ++k) {
          int32_t index = *(int32_t*)taosArrayGet(task->plan->pSubqueryDeps, k);
          if (index < 0 || index >= count) return TSDB_CODE_PLAN_INTERNAL_ERROR;
        }
        pJob->earlyMainTasks = true;
      }
    }
  }
  pJob->concurrentSubJobs = true;
  return TSDB_CODE_SUCCESS;
}

int32_t schLaunchReadySubJobs(SSchJob *pJob) {
  SSchJob* pParent = SCH_PARENT_JOB(pJob);
  int32_t count = taosArrayGetSize(pParent->subJobs);
  int32_t candidates = pJob == pParent ? count : taosArrayGetSize(pJob->dependentJobs);
  SArray* pReady = taosArrayInit(candidates + 1, POINTER_BYTES);
  if (NULL == pReady) return terrno;
  int32_t code = TSDB_CODE_SUCCESS;

  SCH_LOCK(SCH_WRITE, &pParent->subJobLock);
  if (!schJobNeedToStop(pParent, NULL)) {
    // Reserve the whole batch before calling any launcher. An inline success
    // callback can otherwise select the same sibling a second time.
    for (int32_t i = 0; i < candidates; ++i) {
      int32_t index = pJob == pParent ? i : *(int32_t*)taosArrayGet(pJob->dependentJobs, i);
      SSchJob* pSub = taosArrayGetP(pParent->subJobs, index);
      if (!pSub->subJobStarted && 0 == pSub->pendingDependencies) {
        if (NULL == taosArrayPush(pReady, &pSub)) {
          code = terrno;
          goto _unlock;
        }
      }
    }
    if ((pParent->earlyMainTasks || pParent->subJobDoneNum == count) && !pParent->subJobStarted) {
      if (NULL == taosArrayPush(pReady, &pParent)) {
        code = terrno;
        goto _unlock;
      }
    }
    // Commit launch flags only after every ready job has been added.
    for (int32_t i = 0; i < taosArrayGetSize(pReady); ++i) {
      SSchJob* pReadyJob = taosArrayGetP(pReady, i);
      pReadyJob->subJobStarted = true;
      if (pReadyJob == pParent) {
        pParent->launchedSeriesId = pParent->seriesId;
      }
    }
  }

_unlock:
  SCH_UNLOCK(SCH_WRITE, &pParent->subJobLock);
  SCH_ERR_JRET(code);

  for (int32_t i = 0; i < taosArrayGetSize(pReady); ++i) {
    SSchJob* pReadyJob = taosArrayGetP(pReady, i);
    if (schJobNeedToStop(pParent, NULL)) break;
    code = schLaunchJobImpl(pReadyJob);
    if (TSDB_CODE_SUCCESS != code) {
      (void)schHandleJobFailure(pReadyJob, code);
      break;
    }
  }

_return:
  taosArrayDestroy(pReady);
  return code;
}

static int32_t schStopQueryTasksImpl(void* param) {
  SSchJob* pParent = param;
  schDropJobAllTasks(pParent);
  for (int32_t i = 0; i < taosArrayGetSize(pParent->subJobs); ++i) {
    SSchJob* pSub = taosArrayGetP(pParent->subJobs, i);
    schDropJobAllTasks(pSub);
  }
  (void)schReleaseJob(pParent->refId);
  return TSDB_CODE_SUCCESS;
}

void schStopQueryTasks(SSchJob* pJob) {
  SSchJob* pParent = SCH_PARENT_JOB(pJob);
  if (!pParent->concurrentSubJobs || !schJobDone(pParent) ||
      0 != atomic_val_compare_exchange_8(&pParent->stopScheduled, 0, 1)) return;
  SSchJob* pRef = NULL;
  if (TSDB_CODE_SUCCESS != schAcquireJob(pParent->refId, &pRef)) {
    atomic_store_8(&pParent->stopScheduled, 0);
    return;
  }
  // Failures also arrive with task locks held. Drop tasks on the worker pool
  // so no callback waits for another task while holding its own task lock.
  int32_t code = taosAsyncExec(schStopQueryTasksImpl, pRef, NULL);
  if (TSDB_CODE_SUCCESS != code) {
    atomic_store_8(&pParent->stopScheduled, 0);
    (void)schReleaseJob(pParent->refId);
    SCH_JOB_ELOG("schedule query task cleanup failed, code:%s", tstrerror(code));
  }
}

int32_t schLaunchJobImpl(SSchJob *pJob) {
  if (schJobNeedToStop(pJob, NULL)) return TSDB_CODE_SCH_IGNORE_ERROR;
  SSchLevel *level = taosArrayGet(pJob->levels, pJob->levelIdx);
  if (NULL == level) {
    SCH_JOB_ELOG("fail to get the %dth level, levelNum:%d", pJob->levelIdx, (int32_t)taosArrayGetSize(pJob->levels));
    SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
  }

  // Do not move the phase back to node selection once the job has already
  // entered the execute stage (e.g. the parent job resumes after its subjobs).
  // CAS on the sampled value: a plain load-then-store could write node
  // selection over a phase another thread advanced in between. If the CAS
  // fails, the newer phase is authoritative and is kept, which is intended.
  // schGetMainPhase() also maps the coarse SCHEDULE main-phase value to
  // SCHEDULE; that value is never stored in execPhase, so only NONE,
  // PARSE/CATALOG/PLAN and the schedule sub-phases match here.
  // Note on retry: schHandleJobRetry re-enters here with the job already in
  // EXECUTE, so the condition below is false and the schedule timer is kept;
  // a retried job is not counted as a new schedule round.
  int32_t curPhase = SCH_GET_JOB_PHASE(pJob);
  if (curPhase != QUERY_PHASE_SCHEDULE_NODE_SELECTION && schGetMainPhase(curPhase) <= QUERY_PHASE_SCHEDULE) {
    if (!schCasJobPhase(pJob, curPhase, QUERY_PHASE_SCHEDULE_NODE_SELECTION, false)) {
      SCH_JOB_DLOG("schedule phase stamp skipped, phase %d changed concurrently", curPhase);
    }
  }

  if (pJob->earlyMainTasks) {
    // Each leaf has its own exchange and subquery prerequisites. Internal tasks are
    // launched by their last child and do not need the legacy level-by-level walk.
    uint64_t series = atomic_load_64(&pJob->seriesId);
    for (int32_t i = pJob->levelNum - 1; i >= 0; --i) {
      SSchLevel* leafLevel = taosArrayGet(pJob->levels, i);
      for (int32_t j = 0; j < leafLevel->taskNum; ++j) {
        SSchTask* task = taosArrayGet(leafLevel->subTasks, j);
        if (taosArrayGetSize(task->children)) continue;
        SCH_LOCK_TASK(task);
        if (series != atomic_load_64(&pJob->seriesId) || schJobNeedToStop(pJob, NULL)) {
          SCH_UNLOCK_TASK(task);
          return TSDB_CODE_SUCCESS;
        }
        task->failedSeriesId = series - 1;
        task->seriesId = series;
        int32_t code = schDelayLaunchTask(pJob, task);
        SCH_UNLOCK_TASK(task);
        SCH_ERR_RET(code);
      }
    }
  } else {
    SCH_ERR_RET(schLaunchLevelTasks(pJob, level));
  }

  // The parent is promoted when a subjob query message is actually dispatched
  // (schBuildAndSendMsg), not when its tasks are queued here.

  // Note: We don't set RESOURCE_ALLOC phase here because tasks may still be
  // in flow control queue and not actually launched yet. The EXEC phase will
  // be set when actual query messages are sent in schBuildAndSendMsg.

  return TSDB_CODE_SUCCESS;
}

int32_t schLaunchJob(SSchJob *pJob) {
  if (EXPLAIN_MODE_STATIC == pJob->attr.explainMode) {
    SCH_ERR_RET(qExecStaticExplain(pJob->pDag, (SRetrieveTableRsp **)&pJob->fetchRes));
    SCH_ERR_RET(schSwitchJobStatus(pJob, JOB_TASK_STATUS_PART_SUCC, NULL));

    return TSDB_CODE_SUCCESS;
  }

  if (SCH_IS_PARENT_JOB(pJob) && pJob->concurrentSubJobs) {
    // Child callbacks select the first main-job launch. Only this entry,
    // called after retry reset, may select another execution series.
    SCH_LOCK(SCH_WRITE, &pJob->subJobLock);
    bool restart = pJob->subJobStarted && pJob->launchedSeriesId != pJob->seriesId;
    if (restart) {
      pJob->launchedSeriesId = pJob->seriesId;
    }
    SCH_UNLOCK(SCH_WRITE, &pJob->subJobLock);
    if (restart) {
      return schLaunchJobImpl(pJob);
    }
    return schLaunchReadySubJobs(pJob);
  }

  if (SCH_IS_PARENT_JOB(pJob) && SCH_JOB_GOT_SUB_JOBS(pJob) && pJob->subJobExecIdx < taosArrayGetSize(pJob->subJobs)) {
    SCH_ERR_RET(schLaunchJobImpl(taosArrayGetP(pJob->subJobs, pJob->subJobExecIdx++)));

    return TSDB_CODE_SUCCESS;
  }

  return schLaunchJobImpl(pJob);
}

void schDropJobAllTasks(SSchJob *pJob) {
  schDropTaskInHashList(pJob, pJob->execTasks);
  //  schDropTaskInHashList(pJob, pJob->succTasks);
  //  schDropTaskInHashList(pJob, pJob->failTasks);
}

int32_t schNotifyJobAllTasks(SSchJob *pJob, SSchTask *pTask, ETaskNotifyType type) {
  int32_t code = TSDB_CODE_SUCCESS;
  
  SCH_ERR_RET(schNotifyTaskInHashList(pJob, pJob->execTasks, type, pTask));

  if (!SCH_JOB_GOT_SUB_JOBS(pJob)) {
    return TSDB_CODE_SUCCESS;
  }
  
  for (int32_t i = 0; i < pJob->subJobs->size; ++i) {
    SSchJob* pSub = taosArrayGetP(pJob->subJobs, i);
    if (NULL == pSub) {
      continue;
    }

    SCH_ERR_RET(schNotifyTaskInHashList(pSub, pSub->execTasks, type, NULL));
  }  

  return code;
}

void schFreeJobImpl(void *job) {
  if (NULL == job) {
    return;
  }

  SSchJob *pJob = job;
  uint64_t queryId = pJob->queryId;
  int64_t  refId = pJob->refId;
  bool     isParentJob = SCH_IS_PARENT_JOB(pJob);
  void (*cbParamFreeFp)(void*) = isParentJob ? pJob->cbParamFreeFp : NULL;
  void* cbParam = pJob->userRes.cbParam;

  qDebug("QID:0x%" PRIx64 ", begin to free sch job, jobId:0x%" PRIx64 ", pointer:%p, isParent:%d", queryId, refId, pJob, isParentJob);

  schDropJobAllTasks(pJob);

  if (SCH_JOB_GOT_SUB_JOBS(pJob)) {
    taosArrayDestroyP(pJob->subJobs, schFreeJobImpl);
  }

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
  taosArrayDestroy(pJob->dependentJobs);
  taosArrayDestroy(pJob->waitingMainTasks);

  taosHashCleanup(pJob->execTasks);
  //  taosHashCleanup(pJob->failTasks);
  //  taosHashCleanup(pJob->succTasks);
  taosHashCleanup(pJob->taskList);

  taosArrayDestroy(pJob->levels);
  taosArrayDestroy(pJob->dataSrcTasks);

  qExplainFreeCtx(pJob->explainCtx);

  destroyQueryExecRes(&pJob->execRes);
  taosMemoryFree(pJob->pCorrelationTemplates);

  if (isParentJob) {
    taosArrayDestroyP(pJob->pExternalSubquerySources, (FDelete)nodesDestroyNode);
    taosArrayDestroy(pJob->pCorrelationParamDefs);
    schFreeCorrelationValues(pJob->pCorrelationParamValues);
    taosMemoryFree(pJob->pCorrelationTimeInit);
    taosArrayDestroy(pJob->nodeList);
    qDestroyQueryPlan(pJob->pDag);
    qDebug("QID:0x%" PRIx64 " pDag: %p destroyed", queryId, pJob->pDag);
    (void)nodesReleaseAllocatorWeakRef(pJob->allocatorRefId);  // ignore error
  }
  
  taosMemoryFreeClear(pJob->userRes.execRes);
  taosMemoryFreeClear(pJob->fetchRes);
  taosMemoryFreeClear(pJob->sql);
  int32_t code = tsem_destroy(&pJob->rspSem);
  if (code) {
    qError("tsem_destroy failed, error:%s", tstrerror(code));
  }
  taosMemoryFree(pJob);

  if (refId > 0 && isParentJob) {
    int32_t jobNum = atomic_sub_fetch_32(&schMgmt.jobNum, 1);
    if (jobNum == 0) {
      schCloseJobRef();
    }
  }

  if (NULL != cbParamFreeFp) {
    cbParamFreeFp(cbParam);
  }

  qDebug("QID:0x%" PRIx64 ", sch job freed, jobId:0x%" PRIx64 ", pointer:%p, isParent:%d", queryId, refId, pJob, isParentJob);
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

int32_t schInitSubJob(SSchJob* pParent, SQueryPlan* pDag, int32_t subJobId, SSchJob** ppRes, SSchedulerReq *pReq) {
  int32_t  code = 0;
  int64_t  refId = -1;
  SSchJob *pJob = taosMemoryCalloc(1, sizeof(SSchJob));
  if (NULL == pJob) {
    qError("QID:0x%" PRIx64 ", calloc %d failed", pDag->queryId, (int32_t)sizeof(SSchJob));
    SCH_ERR_JRET(terrno);
  }

  if (pDag->subSql) {
    pJob->sql = taosStrdup(pDag->subSql);
    if (NULL == pJob->sql) {
      qError("QID:0x%" PRIx64 ", strdup subSql %s failed", pDag->queryId, pDag->subSql);
      SCH_ERR_JRET(terrno);
    }
  }

  pJob->parent = pParent;
  pJob->queryId = pParent->queryId;
  pJob->seriesId = pParent->seriesId;
  pJob->refId = pParent->refId;
  pJob->subJobId = subJobId;
  pJob->attr.explainMode = pParent->attr.explainMode;
  pJob->attr.localExec = false;
  pJob->attr.inlineTaskLaunch = pParent->attr.inlineTaskLaunch;
  pJob->conn = pParent->conn;
  pJob->firstDayOfWeek = pParent->firstDayOfWeek;
  
  // TODO COPY SQL
  /*
  pJob->sql = taosStrdup(pReq->sql);
  if (NULL == pJob->sql) {
    qError("QID:0x%" PRIx64 ", strdup sql %s failed", pReq->pDag->queryId, pReq->sql);
    SCH_ERR_JRET(terrno);
  }
  */
  
  pJob->pDag = pDag;
  pJob->chkKillFp = pParent->chkKillFp;
  pJob->chkKillParam = pParent->chkKillParam;
  pJob->userRes.execFp = pParent->userRes.execFp;
  pJob->userRes.cbParam = pParent->userRes.cbParam;
  pJob->source       = pParent->source;
  pJob->secureDelete = pParent->secureDelete;
  pJob->pWorkerCb    = pParent->pWorkerCb;
  pJob->nodeList = pParent->nodeList;
  pJob->correlation = pParent->correlation;
  pJob->txnId = pParent->txnId;
  pJob->pCorrelationParamDefs = pParent->pCorrelationParamDefs;
  pJob->pCorrelationParamValues = pParent->pCorrelationParamValues;
  pJob->pCorrelationTimeInit = pParent->pCorrelationTimeInit;
  pJob->correlationTimeInitLen = pParent->correlationTimeInitLen;
  if (NULL != pDag->pCorrelationTemplates) {
    SNodeListNode templates = {0};
    templates.node.type = QUERY_NODE_NODE_LIST;
    templates.pNodeList = pDag->pCorrelationTemplates;
    int32_t len = 0;
    SCH_ERR_JRET(nodesNodeToMsg((SNode*)&templates, &pJob->pCorrelationTemplates, &len));
    if (len <= 0) {
      SCH_ERR_JRET(TSDB_CODE_INVALID_MSG);
    }
    pJob->correlationTemplatesLen = len;
  }

  qDebug("QID:0x%" PRIx64 " subJob %d init with pTrans:%p, pJob:%p, pDag:%p, subQType:%d", 
    pParent->queryId, subJobId, pJob->conn.pTrans, pJob, pJob->pDag, pJob->pDag->subQType);

  pJob->taskList = taosHashInit(SCH_GET_TASK_CAPACITY(pDag->numOfSubplans), taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false,
                                HASH_ENTRY_LOCK);
  if (NULL == pJob->taskList) {
    SCH_JOB_ELOG("taosHashInit %d taskList failed", SCH_GET_TASK_CAPACITY(pDag->numOfSubplans));
    SCH_ERR_JRET(terrno);
  }

  SCH_ERR_JRET(schValidateAndBuildJob(pDag, pJob));

  pJob->execTasks = taosHashInit(pDag->numOfSubplans, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false,
                                 HASH_ENTRY_LOCK);
  if (NULL == pJob->execTasks) {
    SCH_JOB_ELOG("taosHashInit %d execTasks failed", pDag->numOfSubplans);
    SCH_ERR_JRET(terrno);
  }

  if (tsem_init(&pJob->rspSem, 0, 0)) {
    SCH_JOB_ELOG("tsem_init failed, errno:%d", ERRNO);
    SCH_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCH_JOB_TLOG("jobId:0x%" PRIx64 "-%d, job created", pJob->refId, subJobId);

  *ppRes = pJob;

  return TSDB_CODE_SUCCESS;

_return:

  if (NULL == pJob) {
    qDestroyQueryPlan(pDag);
  } else {
    schFreeJobImpl(pJob);
  }
  
  SCH_RET(code);
}

int32_t schInitJob(int64_t *pJobId, SSchedulerReq *pReq) {
  int32_t  code = 0;
  int32_t  i = 0;
  int64_t  refId = -1;
  if (NULL != pReq->pPlanTaken) {
    *pReq->pPlanTaken = false;
  }
  SSchJob *pJob = taosMemoryCalloc(1, sizeof(SSchJob));
  if (NULL == pJob) {
    qError("QID:0x%" PRIx64 ", calloc %d failed", pReq->pDag->queryId, (int32_t)sizeof(SSchJob));
    SCH_ERR_JRET(terrno);
  }

  pJob->pDag = pReq->pDag;
  pJob->userRes.cbParam = pReq->cbParam;
  pJob->cbParamFreeFp = pReq->cbParamFreeFp;
  if (NULL != pReq->pPlanTaken) {
    *pReq->pPlanTaken = true;
  }

  pJob->attr.explainMode = pReq->pDag->explainInfo.mode;
  pJob->attr.localExec = pReq->localReq;
  pJob->attr.inlineTaskLaunch = pReq->inlineTaskLaunch;
  pJob->conn = *pReq->pConn;
  pJob->firstDayOfWeek = pReq->firstDayOfWeek;
  
  if (pReq->sql) {
    pJob->sql = taosStrdup(pReq->sql);
    if (NULL == pJob->sql) {
      qError("QID:0x%" PRIx64 ", strdup sql %s failed", pReq->pDag->queryId, pReq->sql);
      SCH_ERR_JRET(terrno);
    }
  }
  pJob->correlation = pReq->correlation;
  SCH_ERR_JRET(schCopySubquerySources(pReq->pExternalSubquerySources, &pJob->pExternalSubquerySources));
  SCH_ERR_JRET(tValidateCorrelationParams(pReq->pCorrelationParamDefs, pReq->pCorrelationParamValues));
  if (NULL != pReq->pCorrelationParamDefs) {
    pJob->pCorrelationParamDefs = taosArrayDup(pReq->pCorrelationParamDefs, NULL);
    if (NULL == pJob->pCorrelationParamDefs) {
      SCH_ERR_JRET(terrno);
    }
  }
  SCH_ERR_JRET(schCopyCorrelationValues(pReq->pCorrelationParamValues, &pJob->pCorrelationParamValues));
  if (NULL != pReq->pDag->pCorrelationTemplates) {
    SNodeListNode templates = {0};
    templates.node.type = QUERY_NODE_NODE_LIST;
    templates.pNodeList = pReq->pDag->pCorrelationTemplates;
    int32_t len = 0;
    SCH_ERR_JRET(nodesNodeToMsg((SNode*)&templates, &pJob->pCorrelationTemplates, &len));
    if (len <= 0) {
      SCH_ERR_JRET(TSDB_CODE_INVALID_MSG);
    }
    pJob->correlationTemplatesLen = len;
  }
  if (NULL != pReq->pCorrelationTimeRangeInit) {
    SNodeListNode timeInit = {0};
    timeInit.node.type = QUERY_NODE_NODE_LIST;
    timeInit.pNodeList = (SNodeList*)pReq->pCorrelationTimeRangeInit;
    int32_t len = 0;
    SCH_ERR_JRET(nodesNodeToMsg((SNode*)&timeInit, &pJob->pCorrelationTimeInit, &len));
    if (len <= 0) {
      SCH_ERR_JRET(TSDB_CODE_INVALID_MSG);
    }
    pJob->correlationTimeInitLen = len;
  }
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
  pJob->source       = pReq->source;
  pJob->secureDelete = pReq->secureDelete;
  pJob->pWorkerCb    = pReq->pWorkerCb;
  pJob->txnId = pReq->txnId;
  pJob->subJobId = -1;
  pJob->queryId = pReq->pDag->queryId;
  (void)atomic_add_fetch_64(&pJob->seriesId, 1);

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

  pJob->refId = taosAddRef(schMgmt.jobRef, pJob);
  if (pJob->refId < 0) {
    SCH_JOB_ELOG("taosAddRef job failed, error:%s", tstrerror(terrno));
    SCH_ERR_JRET(terrno);
  }

  if (pReq->pDag->pChildren && pReq->pDag->pChildren->length > 0) {
    if (pReq->localReq) {
      SCH_JOB_ELOG("local policy not supported for query with subJobs, subJobNum:%d", pReq->pDag->pChildren->length);
      SCH_ERR_JRET(TSDB_CODE_PAR_INVALID_SCALAR_SUBQ_POLICY);
    }
    
    pJob->subJobs = taosArrayInit_s(POINTER_BYTES, pReq->pDag->pChildren->length);
    if (NULL == pJob->subJobs) {
      SCH_JOB_ELOG("taosArrayInit %d subJobs failed", pReq->pDag->pChildren->length);
      SCH_ERR_JRET(terrno);
    }
    
    SNode* pNode = NULL;
    SSchJob* pSubJob = NULL;
    FOREACH(pNode, pReq->pDag->pChildren) {
      SCH_ERR_JRET(schInitSubJob(pJob, (SQueryPlan*)pNode, i, &pSubJob, pReq));
      taosArraySet(pJob->subJobs, i, &pSubJob);
      i++;
    }
  }

  qDebug("QID:0x%" PRIx64 " subJob %d init with pTrans:%p, pJob:%p, subJobNum:%d, pDag:%p", 
    pJob->queryId, pJob->subJobId, pJob->conn.pTrans, pJob, (int32_t)taosArrayGetSize(pJob->subJobs), pJob->pDag);
  
  SCH_ERR_JRET(schValidateAndBuildJob(pReq->pDag, pJob));

  SCH_ERR_JRET(schInitJobDependencies(pJob));
  if (pJob->concurrentSubJobs) {
    SCH_ERR_JRET(schInitJobFlowCtrl(pJob));
  }

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

  (void)atomic_add_fetch_32(&schMgmt.jobNum, 1);

  *pJobId = pJob->refId;

  SCH_JOB_TLOG("jobId:0x%" PRIx64 ", job created", pJob->refId);

  return TSDB_CODE_SUCCESS;

_return:

  if (NULL == pJob) {
    if (NULL == pReq->pPlanTaken) {
      qDestroyQueryPlan(pReq->pDag);
    }
  } else if (pJob->refId <= 0) {
    schFreeJobImpl(pJob);
  } else {
    int32_t tmpCode = taosRemoveRef(schMgmt.jobRef, pJob->refId);
    if (tmpCode) {
      SCH_JOB_DLOG("jobId:0x%" PRIx64 ", taosRemoveRef job from jobRef, error:%s", pJob->refId, tstrerror(tmpCode));
    }
  }

  SCH_RET(code);
}

int32_t schExecJob(SSchJob *pJob, SSchedulerReq *pReq) {
  int32_t code = 0;
  qTrace("QID:0x%" PRIx64 ", jobId:0x%" PRIx64 ", sch job started", pReq->pDag->queryId, pJob->refId);

  code = schLaunchJob(pJob);
  schDrainInlineFlowTasks(pJob);
  SCH_ERR_RET(code);

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

// Called with the task locked. A different retry can replace its execution while we wait.
static bool schClaimJobRetry(SSchJob* pJob, SSchTask* pTask) {
  uint64_t seriesId = pTask->seriesId;
  int32_t execId = pTask->execId;
  while (atomic_val_compare_exchange_8(&pJob->inRetry, 0, 1) != 0) {
    if (seriesId != atomic_load_64(&pJob->seriesId)) return false;
    SCH_UNLOCK_TASK(pTask);
    taosUsleep(1);
    SCH_LOCK_TASK(pTask);
  }
  if (seriesId == atomic_load_64(&pJob->seriesId) && seriesId == pTask->seriesId && execId == pTask->execId) {
    if (pJob->earlyMainTasks) {
      SCH_LOCK(SCH_WRITE, &pJob->subJobLock);
      pJob->invalidatedMainSeries = seriesId;
      pJob->pendingMainResult.task = NULL;
      SCH_UNLOCK(SCH_WRITE, &pJob->subJobLock);
    }
    return true;
  }
  atomic_store_8(&pJob->inRetry, 0);
  return false;
}

static int32_t schResetJobForRetry(SSchJob *pJob, SSchTask *pTask, int32_t rspCode) {
  int32_t code = 0;
  int32_t numOfLevels = taosArrayGetSize(pJob->levels);
  SSchJob* pParent = SCH_PARENT_JOB(pJob);
  // Early execution can publish from a deferred child callback. Hold the same
  // root-task lock for status reset and invalidation as for result delivery.
  SSchTask* root = NULL;
  if (pJob->earlyMainTasks) {
    SSchLevel* top = taosArrayGet(pJob->levels, 0);
    root = top ? taosArrayGet(top->subTasks, 0) : NULL;
    if (!root) return TSDB_CODE_SCH_INTERNAL_ERROR;
    SCH_LOCK_TASK(root);
  }
  code = schChkResetJobRetry(pJob, rspCode);
  if (TSDB_CODE_SUCCESS == code) {
    SCH_LOCK(SCH_WRITE, &pParent->subJobLock);
    (void)atomic_add_fetch_64(&pJob->seriesId, 1);
    pJob->pendingMainResult.task = NULL;
    SCH_UNLOCK(SCH_WRITE, &pParent->subJobLock);
  }
  if (root) SCH_UNLOCK_TASK(root);
  SCH_ERR_RET(code);

  // Every task in every level is reset and re-launched below, so the per-vgroup results already
  // summed into this job are stale by definition - the re-run reports them again. Without this
  // the counters double-count on every job-level retry: resNumTouchedObjects inflates the
  // client's per-object txnDdlCount (over-charging the txn budget until it is exhausted early),
  // resNumOfRows inflates the reported affected rows. Storing 0 simply discards the stale sum
  // accumulated before the retry; the accumulators (schRemote.c) add fresh values afterwards.
  (void)atomic_store_32(&pJob->resNumTouchedObjects, 0);
  (void)atomic_store_64(&pJob->resNumOfRows, 0);

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
      SSchTask *p = taosArrayGet(pLevel->subTasks, j);
      if (NULL == p) {
        SCH_JOB_ELOG("fail to get the %dth task in level %d, taskNum:%d", j, i, numOfTasks);
        SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
      }

      SCH_LOCK_TASK(p);

      code = schChkUpdateRedirectCtx(pJob, p, (p->taskId != pTask->taskId));
      if (TSDB_CODE_SUCCESS != code) {
        SCH_UNLOCK_TASK(p);
        SCH_RET(code);
      }

      schResetTaskForRetry(pJob, p);

      SCH_LOCK(SCH_WRITE, &p->planLock);
      qClearSubplanExecutionNode(p->plan);
      SCH_UNLOCK(SCH_WRITE, &p->planLock);

      SCH_UNLOCK_TASK(p);
    }
  }

  SCH_RESET_JOB_LEVEL_IDX(pJob);
  if (pParent->concurrentSubJobs && SCH_IS_SUBQ_JOB(pJob)) {
    SCH_LOCK(SCH_WRITE, &pParent->subJobLock);
    pJob->subJobRestarting = false;
    SCH_UNLOCK(SCH_WRITE, &pParent->subJobLock);
  }
  
  SCH_JOB_DLOG("update job sId:%" PRId64 ", levelIdx:%d", pJob->seriesId, pJob->levelIdx);

  return TSDB_CODE_SUCCESS;
}

int32_t schHandleJobRetry(SSchJob *pJob, SSchTask *pTask, SDataBuf *pMsg, int32_t rspCode) {
  taosMemoryFreeClear(pMsg->pData);
  taosMemoryFreeClear(pMsg->pEpSet);
  if (!schClaimJobRetry(pJob, pTask)) return TSDB_CODE_SCH_IGNORE_ERROR;

  int32_t code = schFailedTaskNeedRetry(pTask, pJob, rspCode);
  SSchJob* pParent = SCH_PARENT_JOB(pJob);
  bool published = false;
  if (pParent->concurrentSubJobs && SCH_IS_SUBQ_JOB(pJob)) {
    SCH_LOCK(SCH_WRITE, &pParent->subJobLock);
    published = pJob->resultReady;
    if (!published && TSDB_CODE_SUCCESS == code) pJob->subJobRestarting = true;
    SCH_UNLOCK(SCH_WRITE, &pParent->subJobLock);
  }

  if (published) {
    SCH_UNLOCK_TASK(pTask);
    code = schHandleJobFailure(pJob, rspCode);
    SCH_LOCK_TASK(pTask);
  } else {
    if (TSDB_CODE_SUCCESS == code) {
      SCH_TASK_DLOG("start to restart all tasks by error:%s from TID:0x%" PRIx64, tstrerror(rspCode), pTask->taskId);
      SCH_UNLOCK_TASK(pTask);
      code = schResetJobForRetry(pJob, pTask, rspCode);
      if (TSDB_CODE_SUCCESS == code) code = schLaunchJob(pJob);
      SCH_LOCK_TASK(pTask);
    }
    if (TSDB_CODE_SUCCESS != code) code = schProcessOnTaskFailure(pJob, pTask, code);
  }
  atomic_store_8(&pJob->inRetry, 0);
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
    schDrainInlineFlowTasks(pJob);
    (void)schReleaseJob(pJob->refId);  // ignore error
  }
}

int32_t schProcessOnCbBegin(SSchJob **job, SSchTask **task, uint64_t qId, int64_t rId, int32_t sjId, uint64_t tId) {
  int32_t code = 0;
  int8_t  status = 0;

  SSchTask *pTask = NULL;
  SSchJob  *pJob = NULL;

  (void)schAcquireJob(rId, &pJob);
  if (NULL == pJob) {
    qWarn("QID:0x%" PRIx64 ", TID:0x%" PRIx64 " job doesn't exist, may be dropped, jobId:0x%" PRIx64, qId, tId, rId);
    SCH_ERR_RET(TSDB_CODE_QRY_JOB_NOT_EXIST);
  }

  if (!pJob->concurrentSubJobs && schJobNeedToStop(pJob, &status)) {
    SCH_TASK_DLOG("will not do further processing cause of job status %s", jobTaskStatusStr(status));
    SCH_ERR_JRET(TSDB_CODE_SCH_IGNORE_ERROR);
  }

  if (sjId >= 0) {
    pJob = taosArrayGetP(pJob->subJobs, sjId);
    if (NULL == pJob) {
      qWarn("QID:0x%" PRIx64 ", TID:0x%" PRIx64 " SJID:%d sub job does not exist, jobId:0x%" PRIx64, qId, tId, sjId, rId);
      SCH_ERR_JRET(TSDB_CODE_QRY_JOB_NOT_EXIST);
    }

    if (!SCH_PARENT_JOB(pJob)->concurrentSubJobs && schJobNeedToStop(pJob, &status)) {
      SCH_TASK_DLOG("will not do further processing cause of sub job status %s", jobTaskStatusStr(status));
      SCH_ERR_JRET(TSDB_CODE_SCH_IGNORE_ERROR);
    }
  }

  SCH_ERR_JRET(schGetTaskInJob(pJob, tId, &pTask));

  SCH_LOCK_TASK(pTask);

  if (SCH_PARENT_JOB(pJob)->concurrentSubJobs && schJobNeedToStop(pJob, &status)) {
    schDropTaskOnExecNode(pJob, pTask);
    SCH_ERR_JRET(TSDB_CODE_SCH_IGNORE_ERROR);
  }

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

void schSetJobType(SSchJob *pJob, ESubplanType type) {
  if (type == SUBPLAN_TYPE_MODIFY) {
    pJob->attr.type |= JOB_TYPE_INSERT;
  } else {
    if (type == SUBPLAN_TYPE_HSYSSCAN) {
      pJob->attr.type |= JOB_TYPE_HQUERY;
    } else {
      pJob->attr.type |= JOB_TYPE_QUERY;
    }
  }
}
