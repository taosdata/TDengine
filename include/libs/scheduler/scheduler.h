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

typedef struct SSchedulerCfg {
  uint32_t maxJobNum;
  int32_t  maxNodeTableNum;
} SSchedulerCfg;

typedef struct SQueryProfileSummary {
  int64_t startTs;      // Object created and added into the message queue
  int64_t endTs;        // the timestamp when the task is completed
  int64_t cputime;      // total cpu cost, not execute elapsed time

  int64_t loadRemoteDataDuration;       // remote io time
  int64_t loadNativeDataDuration;       // native disk io time

  uint64_t loadNativeData; // blocks + SMA + header files
  uint64_t loadRemoteData; // remote data acquired by exchange operator.

  uint64_t waitDuration; // the time to waiting to be scheduled in queue does matter, so we need to record it
  int64_t  addQTs;       // the time to be added into the message queue, used to calculate the waiting duration in queue.

  uint64_t totalRows;
  uint64_t loadRows;
  uint32_t totalBlocks;
  uint32_t loadBlocks;
  uint32_t loadBlockAgg;
  uint32_t skipBlocks;
  uint64_t resultSize;   // generated result size in Kb.
} SQueryProfileSummary;

typedef struct SQueryResult {
  int32_t         code;
  uint64_t        numOfRows;
  void           *res;
} SQueryResult;

typedef struct STaskInfo {
  SQueryNodeAddr addr;
  SSubQueryMsg  *msg;
} STaskInfo;

typedef void (*schedulerExecCallback)(SQueryResult* pResult, void* param, int32_t code);
typedef void (*schedulerFetchCallback)(void* pResult, void* param, int32_t code);


int32_t schedulerInit(SSchedulerCfg *cfg);

/**
 * Process the query job, generated according to the query physical plan.
 * This is a synchronized API, and is also thread-safety.
 * @param nodeList  Qnode/Vnode address list, element is SQueryNodeAddr
 * @return
 */
int32_t schedulerExecJob(void *transport, SArray *nodeList, SQueryPlan *pDag, int64_t *pJob, const char *sql, int64_t startTs, SQueryResult *pRes);

/**
 * Process the query job, generated according to the query physical plan.
 * This is a asynchronized API, and is also thread-safety.
 * @param pNodeList  Qnode/Vnode address list, element is SQueryNodeAddr
 * @return
 */
  int32_t schedulerAsyncExecJob(void *pTrans, SArray *pNodeList, SQueryPlan *pDag, int64_t *pJob, const char *sql,
                           int64_t startTs, schedulerExecCallback fp, void* param);

/**
 * Fetch query result from the remote query executor
 * @param pJob
 * @param data
 * @return
 */
int32_t schedulerFetchRows(int64_t job, void **data);

int32_t schedulerAsyncFetchRows(int64_t job, schedulerFetchCallback fp, void* param);

int32_t schedulerGetTasksStatus(int64_t job, SArray *pSub);


/**
 * Cancel query job
 * @param pJob
 * @return
 */
//int32_t scheduleCancelJob(void *pJob);

/**
 * Free the query job
 * @param pJob
 */
void schedulerFreeJob(int64_t job);

void schedulerDestroy(void);

/**
 * convert dag to task list
 * @param pDag
 * @param pTasks SArray**<STaskInfo>
 * @return
 */
int32_t schedulerConvertDagToTaskList(SQueryPlan* pDag, SArray **pTasks);

/**
 * make one task info's multiple copies
 * @param src
 * @param dst SArray**<STaskInfo>
 * @return
 */
int32_t schedulerCopyTask(STaskInfo *src, SArray **dst, int32_t copyNum);

void schedulerFreeTaskList(SArray *taskList);


#ifdef __cplusplus
}
#endif

#endif /*_TD_SCHEDULER_H_*/
