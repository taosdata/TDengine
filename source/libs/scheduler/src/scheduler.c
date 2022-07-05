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

  qInfo("scheduler 0x%" PRIx64 " initizlized, maxJob:%u", schMgmt.sId, schMgmt.cfg.maxJobNum);

  return TSDB_CODE_SUCCESS;
}

int32_t schedulerExecJob(SSchedulerReq *pReq, int64_t *pJobId, SQueryResult *pRes) {
  qDebug("scheduler sync exec job start");

  int32_t code = 0;  
  SSchJob *pJob = NULL;
  SCH_ERR_JRET(schInitJob(pReq, &pJob));

  *pJobId = pJob->refId;
  
  SCH_ERR_JRET(schExecJobImpl(pReq, pJob, true));

_return:

  if (code && NULL == pJob) {
    qDestroyQueryPlan(pReq->pDag);
  }
  
  if (pJob) {
    schSetJobQueryRes(pJob, pRes);
    schReleaseJob(pJob->refId);
  }

  return code;
}

int32_t schedulerAsyncExecJob(SSchedulerReq *pReq, int64_t *pJobId) {
  qDebug("scheduler async exec job start");

  int32_t code = 0;  
  SSchJob *pJob = NULL;
  SCH_ERR_JRET(schInitJob(pReq, &pJob));

  *pJobId = pJob->refId;
  
  SCH_ERR_JRET(schExecJobImpl(pReq, pJob, false));

_return:

  if (code && NULL == pJob) {
    qDestroyQueryPlan(pReq->pDag);
  }
  
  if (pJob) {
    schReleaseJob(pJob->refId);
  }

  return code;
}

int32_t schedulerFetchRows(int64_t job, void **pData) {
  qDebug("scheduler sync fetch rows start");

  if (NULL == pData) {
    SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t  code = 0;
  SSchJob *pJob = schAcquireJob(job);
  if (NULL == pJob) {
    qError("acquire job from jobRef list failed, may be dropped, jobId:0x%" PRIx64, job);
    SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  SCH_ERR_RET(schBeginOperation(pJob, SCH_OP_FETCH, true));

  pJob->userRes.fetchRes = pData;
  code = schFetchRows(pJob);

  schReleaseJob(job);

  SCH_RET(code);
}

void schedulerAsyncFetchRows(int64_t job, schedulerFetchFp fp, void* param) {
  qDebug("scheduler async fetch rows start");

  int32_t code = 0;
  if (NULL == fp || NULL == param) {
    SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SSchJob *pJob = schAcquireJob(job);
  if (NULL == pJob) {
    qError("acquire sch job from job list failed, may be dropped, jobId:0x%" PRIx64, job);
    SCH_ERR_JRET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  SCH_ERR_JRET(schBeginOperation(pJob, SCH_OP_FETCH, false));
  
  pJob->userRes.fetchFp = fp;
  pJob->userRes.userParam = param;
  
  SCH_ERR_JRET(schAsyncFetchRows(pJob));

_return:

  if (code) {
    fp(NULL, param, code);
  }
  
  schReleaseJob(job);
}

int32_t schedulerGetTasksStatus(int64_t job, SArray *pSub) {
  int32_t  code = 0;
  SSchJob *pJob = schAcquireJob(job);
  if (NULL == pJob) {
    qDebug("acquire job from jobRef list failed, may not started or dropped, refId:0x%" PRIx64, job);
    SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  if (pJob->status < JOB_TASK_STATUS_NOT_START || pJob->levelNum <= 0 || NULL == pJob->levels) {
    qDebug("job not initialized or not executable job, refId:0x%" PRIx64, job);
    SCH_ERR_JRET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  for (int32_t i = pJob->levelNum - 1; i >= 0; --i) {
    SSchLevel *pLevel = taosArrayGet(pJob->levels, i);

    for (int32_t m = 0; m < pLevel->taskNum; ++m) {
      SSchTask     *pTask = taosArrayGet(pLevel->subTasks, m);
      SQuerySubDesc subDesc = {0};
      subDesc.tid = pTask->taskId;
      strcpy(subDesc.status, jobTaskStatusStr(pTask->status));

      taosArrayPush(pSub, &subDesc);
    }
  }

_return:

  schReleaseJob(job);

  SCH_RET(code);
}

int32_t scheduleCancelJob(int64_t job) {
  SSchJob *pJob = schAcquireJob(job);
  if (NULL == pJob) {
    qError("acquire job from jobRef list failed, may be dropped, jobId:0x%" PRIx64, job);
    SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  int32_t code = schCancelJob(pJob);

  schReleaseJob(job);

  SCH_RET(code);
}

void schedulerStopQueryHb(void *pTrans) {
  if (NULL == pTrans) {
    return;
  }

  schCleanClusterHb(pTrans);
}

void schedulerFreeJob(int64_t* job, int32_t errCode) {
  if (0 == *job) {
    return;
  }
  
  SSchJob *pJob = schAcquireJob(*job);
  if (NULL == pJob) {
    qError("acquire sch job failed, may be dropped, jobId:0x%" PRIx64, *job);
    *job = 0;
    return;
  }

  int32_t code = schProcessOnJobDropped(pJob, errCode);
  if (TSDB_CODE_SCH_JOB_IS_DROPPING == code) {
    SCH_JOB_DLOG("sch job is already dropping, refId:0x%" PRIx64, *job);
    *job = 0;
    return;
  }

  SCH_JOB_DLOG("start to remove job from jobRef list, refId:0x%" PRIx64, *job);

  if (taosRemoveRef(schMgmt.jobRef, *job)) {
    SCH_JOB_ELOG("remove job from job list failed, refId:0x%" PRIx64, *job);
  }

  schReleaseJob(*job);
  *job = 0;
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

  SCH_LOCK(SCH_WRITE, &schMgmt.hbLock);
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
  SCH_UNLOCK(SCH_WRITE, &schMgmt.hbLock);
}
