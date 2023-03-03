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

#ifndef _TD_SCHEDULER_H_
#define _TD_SCHEDULER_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "catalog.h"
#include "planner.h"

extern tsem_t schdRspSem;

typedef struct SQueryProfileSummary {
  int64_t startTs;  // Object created and added into the message queue
  int64_t endTs;    // the timestamp when the task is completed
  int64_t cputime;  // total cpu cost, not execute elapsed time

  int64_t loadRemoteDataDuration;  // remote io time
  int64_t loadNativeDataDuration;  // native disk io time

  uint64_t loadNativeData;  // blocks + SMA + header files
  uint64_t loadRemoteData;  // remote data acquired by exchange operator.

  uint64_t waitDuration;  // the time to waiting to be scheduled in queue does matter, so we need to record it
  int64_t  addQTs;  // the time to be added into the message queue, used to calculate the waiting duration in queue.

  uint64_t totalRows;
  uint64_t loadRows;
  uint32_t totalBlocks;
  uint32_t loadBlocks;
  uint32_t loadBlockAgg;
  uint32_t skipBlocks;
  uint64_t resultSize;  // generated result size in Kb.
} SQueryProfileSummary;

typedef struct STaskInfo {
  SQueryNodeAddr addr;
  SSubQueryMsg*  msg;
} STaskInfo;

typedef struct SSchdFetchParam {
  void**   pData;
  int32_t* code;
} SSchdFetchParam;

typedef void (*schedulerExecFp)(SExecResult* pResult, void* param, int32_t code);
typedef void (*schedulerFetchFp)(void* pResult, void* param, int32_t code);
typedef bool (*schedulerChkKillFp)(void* param);

typedef struct SSchedulerReq {
  bool               syncReq;
  bool               localReq;
  SRequestConnInfo*  pConn;
  SArray*            pNodeList;
  SQueryPlan*        pDag;
  int64_t            allocatorRefId;
  const char*        sql;
  int64_t            startTs;
  schedulerExecFp    execFp;
  schedulerFetchFp   fetchFp;
  void*              cbParam;
  schedulerChkKillFp chkKillFp;
  void*              chkKillParam;
  SExecResult*       pExecRes;
  void**             pFetchRes;
} SSchedulerReq;

int32_t schedulerInit(void);

int32_t schedulerExecJob(SSchedulerReq* pReq, int64_t* pJob);

int32_t schedulerFetchRows(int64_t jobId, SSchedulerReq* pReq);

void schedulerFetchRowsA(int64_t job, schedulerFetchFp fp, void* param);

int32_t schedulerGetTasksStatus(int64_t job, SArray* pSub);

void schedulerStopQueryHb(void* pTrans);

int32_t schedulerUpdatePolicy(int32_t policy);
int32_t schedulerEnableReSchedule(bool enableResche);

/**
 * Cancel query job
 * @param pJob
 * @return
 */
// int32_t scheduleCancelJob(void *pJob);

/**
 * Free the query job
 * @param pJob
 */
void schedulerFreeJob(int64_t* job, int32_t errCode);

void schedulerDestroy(void);

void schdExecCallback(SExecResult* pResult, void* param, int32_t code);

#ifdef __cplusplus
}
#endif

#endif /*_TD_SCHEDULER_H_*/
