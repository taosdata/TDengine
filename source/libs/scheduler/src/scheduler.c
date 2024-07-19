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

#include "query.h"
#include "qworker.h"
#include "schInt.h"
#include "tmsg.h"
#include "tref.h"
#include "tglobal.h"

SSchedulerMgmt schMgmt = {
    .jobRef = -1,
};

int32_t schedulerInit() {
  if (schMgmt.jobRef >= 0) {
    qError("scheduler already initialized");
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  schMgmt.cfg.maxJobNum = SCHEDULE_DEFAULT_MAX_JOB_NUM;
  schMgmt.cfg.maxNodeTableNum = tsQueryMaxConcurrentTables;
  schMgmt.cfg.schPolicy = SCHEDULE_DEFAULT_POLICY;
  schMgmt.cfg.enableReSchedule = false;

  qDebug("schedule init, policy: %d, maxNodeTableNum: %" PRId64", reSchedule:%d",
    schMgmt.cfg.schPolicy, schMgmt.cfg.maxNodeTableNum, schMgmt.cfg.enableReSchedule);

  schMgmt.jobRef = taosOpenRef(schMgmt.cfg.maxJobNum, schFreeJobImpl);
  if (schMgmt.jobRef < 0) {
    qError("init schduler jobRef failed, num:%u", schMgmt.cfg.maxJobNum);
    SCH_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  schMgmt.hbConnections = taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == schMgmt.hbConnections) {
    qError("taosHashInit hb connections failed");
    SCH_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  schMgmt.timer = taosTmrInit(0, 0, 0, "scheduler");
  if (NULL == schMgmt.timer) {
    qError("init timer failed, error:%s", tstrerror(terrno));
    SCH_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  if (taosGetSystemUUID((char *)&schMgmt.sId, sizeof(schMgmt.sId))) {
    qError("generate schedulerId failed, errno:%d", errno);
    SCH_ERR_RET(TSDB_CODE_QRY_SYS_ERROR);
  }

  qInfo("scheduler 0x%" PRIx64 " initialized, maxJob:%u", schMgmt.sId, schMgmt.cfg.maxJobNum);

  return TSDB_CODE_SUCCESS;
}

int32_t schedulerExecJob(SSchedulerReq *pReq, int64_t *pJobId) {
  qDebug("scheduler %s exec job start", pReq->syncReq ? "SYNC" : "ASYNC");

  int32_t  code = 0;
  SSchJob *pJob = NULL;

  SCH_ERR_JRET(schInitJob(pJobId, pReq));

  SCH_ERR_JRET(schHandleOpBeginEvent(*pJobId, &pJob, SCH_OP_EXEC, pReq));

  SCH_ERR_JRET(schSwitchJobStatus(pJob, JOB_TASK_STATUS_INIT, pReq));

  SCH_ERR_JRET(schSwitchJobStatus(pJob, JOB_TASK_STATUS_EXEC, pReq));

_return:

  SCH_RET(schHandleOpEndEvent(pJob, SCH_OP_EXEC, pReq, code));
}

int32_t schedulerFetchRows(int64_t jobId, SSchedulerReq *pReq) {
  qDebug("scheduler %s fetch rows start", pReq->syncReq ? "SYNC" : "ASYNC");

  int32_t  code = 0;
  SSchJob *pJob = NULL;

  SCH_ERR_JRET(schHandleOpBeginEvent(jobId, &pJob, SCH_OP_FETCH, pReq));

  SCH_ERR_JRET(schSwitchJobStatus(pJob, JOB_TASK_STATUS_FETCH, pReq));

_return:

  SCH_RET(schHandleOpEndEvent(pJob, SCH_OP_FETCH, pReq, code));
}

int32_t schedulerGetTasksStatus(int64_t jobId, SArray *pSub) {
  int32_t  code = 0;
  SSchJob *pJob = NULL;

  SCH_ERR_JRET(schHandleOpBeginEvent(jobId, &pJob, SCH_OP_GET_STATUS, NULL));

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

  SCH_RET(schHandleOpEndEvent(pJob, SCH_OP_GET_STATUS, NULL, code));
}

void schedulerStopQueryHb(void *pTrans) {
  if (NULL == pTrans) {
    return;
  }

  schCleanClusterHb(pTrans);
}

int32_t schedulerUpdatePolicy(int32_t policy) {
  switch (policy) {
    case SCH_LOAD_SEQ:
    case SCH_RANDOM:
    case SCH_ALL:
      schMgmt.cfg.schPolicy = policy;
      qDebug("schedule policy updated to %d", schMgmt.cfg.schPolicy);
      break;
    default:
      return TSDB_CODE_TSC_INVALID_INPUT;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schedulerEnableReSchedule(bool enableResche) {
  schMgmt.cfg.enableReSchedule = enableResche;
  return TSDB_CODE_SUCCESS;
}

void schedulerFreeJob(int64_t *jobId, int32_t errCode) {
  if (0 == *jobId) {
    return;
  }

  SSchJob *pJob = schAcquireJob(*jobId);
  if (NULL == pJob) {
    qDebug("Acquire sch job failed, may be dropped, jobId:0x%" PRIx64, *jobId);
    return;
  }

  SCH_JOB_DLOG("start to free job 0x%" PRIx64 ", code:%s", *jobId, tstrerror(errCode));
  schHandleJobDrop(pJob, errCode);

  schReleaseJob(*jobId);
  *jobId = 0;
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

  qWorkerDestroy(&schMgmt.queryMgmt);
  schMgmt.queryMgmt = NULL;
}
