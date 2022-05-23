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

SSchedulerMgmt schMgmt = {
    .jobRef = -1,
};

int32_t schedulerInit(SSchedulerCfg *cfg) {
  if (schMgmt.jobRef >= 0) {
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

int32_t schedulerExecJob(void *pTrans, SArray *pNodeList, SQueryPlan *pDag, int64_t *pJob, const char *sql,
                         int64_t startTs, SQueryResult *pRes) {
  if (NULL == pTrans || NULL == pDag || NULL == pDag->pSubplans || NULL == pJob || NULL == pRes) {
    SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SSchResInfo resInfo = {.queryRes = *pRes};                      
  SCH_RET(schExecJob(pTrans, pNodeList, pDag, pJob, sql, startTs, &resInfo, true));
}

int32_t schedulerAsyncExecJob(void *pTrans, SArray *pNodeList, SQueryPlan *pDag, int64_t *pJob, const char *sql,
                         int64_t startTs, SQueryResult *pRes, schedulerCallback fp, void* param) {
   if (NULL == pTrans || NULL == pDag || NULL == pDag->pSubplans || NULL == pJob || NULL == pRes || NULL == fp || NULL == param) {
     SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
   }
   
   SSchResInfo resInfo = {.queryRes = *pRes, .userFp = fp, .userParam = param};                      
   SCH_RET(schExecJob(pTrans, pNodeList, pDag, pJob, sql, startTs, &resInfo, false));
}

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
    if (!(pJob->attr.explainMode == EXPLAIN_MODE_STATIC)) {
      SCH_ERR_JRET(schFetchFromRemote(pJob));
      tsem_wait(&pJob->rspSem);
    }
  } else {
    SCH_JOB_ELOG("job status error for fetch, status:%s", jobTaskStatusStr(status));
    SCH_ERR_JRET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  status = SCH_GET_JOB_STATUS(pJob);

  if (JOB_TASK_STATUS_FAILED == status || JOB_TASK_STATUS_DROPPING == status) {
    SCH_JOB_ELOG("job failed or dropping, status:%s", jobTaskStatusStr(status));
    SCH_ERR_JRET(atomic_load_32(&pJob->errCode));
  }

  if (pJob->resData && ((SRetrieveTableRsp *)pJob->resData)->completed) {
    SCH_ERR_JRET(schChkUpdateJobStatus(pJob, JOB_TASK_STATUS_SUCCEED));
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

  SCH_JOB_DLOG("fetch done, totalRows:%d, code:%s", pJob->resNumOfRows, tstrerror(code));

_return:

  atomic_val_compare_exchange_8(&pJob->userFetch, 1, 0);

  schReleaseJob(job);

  SCH_RET(code);
}

int32_t schedulerGetTasksStatus(int64_t job, SArray *pSub) {
  int32_t  code = 0;
  SSchJob *pJob = schAcquireJob(job);
  if (NULL == pJob) {
    qDebug("acquire job from jobRef list failed, may not started or dropped, refId:%" PRIx64, job);
    SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  if (pJob->status < JOB_TASK_STATUS_NOT_START || pJob->levelNum <= 0 || NULL == pJob->levels) {
    qDebug("job not initialized or not executable job, refId:%" PRIx64, job);
    SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  for (int32_t i = pJob->levelNum - 1; i >= 0; --i) {
    SSchLevel *pLevel = taosArrayGet(pJob->levels, i);

    for (int32_t m = 0; m < pLevel->taskNum; ++m) {
      SSchTask     *pTask = taosArrayGet(pLevel->subTasks, m);
      SQuerySubDesc subDesc = {.tid = pTask->taskId, .status = pTask->status};

      taosArrayPush(pSub, &subDesc);
    }
  }

  return TSDB_CODE_SUCCESS;
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
    qDebug("acquire job from jobRef list failed, may be dropped, refId:%" PRIx64, job);
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

void schedulerDestroy(void) {
  atomic_store_8((int8_t *)&schMgmt.exit, 1);

  if (schMgmt.jobRef >= 0) {
    SSchJob *pJob = taosIterateRef(schMgmt.jobRef, 0);
    int64_t  refId = 0;

    while (pJob) {
      refId = pJob->refId;
      if (refId == 0) {
        break;
      }
      taosRemoveRef(schMgmt.jobRef, pJob->refId);

      pJob = taosIterateRef(schMgmt.jobRef, refId);
    }
  }

  if (schMgmt.hbConnections) {
    void *pIter = taosHashIterate(schMgmt.hbConnections, NULL);
    while (pIter != NULL) {
      SSchHbTrans *hb = pIter;
      schFreeRpcCtx(&hb->rpcCtx);
      pIter = taosHashIterate(schMgmt.hbConnections, pIter);
    }
    taosHashCleanup(schMgmt.hbConnections);
    schMgmt.hbConnections = NULL;
  }
}
