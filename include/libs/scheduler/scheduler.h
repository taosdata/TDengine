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

#include "planner.h"
#include "catalog.h"

typedef struct SSchedulerCfg {
  int32_t clusterType;
  int32_t maxJobNum;
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

int32_t schedulerInit(SSchedulerCfg *cfg);

/**
 * Process the query job, generated according to the query physical plan.
 * This is a synchronized API, and is also thread-safety.
 * @param qnodeList  Qnode address list, element is SEpAddr
 * @return
 */
int32_t scheduleExecJob(void *transport, SArray *qnodeList, SQueryDag* pDag, void** pJob, uint64_t *numOfRows);

/**
 * Process the query job, generated according to the query physical plan.
 * This is a asynchronized API, and is also thread-safety.
 * @param qnodeList  Qnode address list, element is SEpAddr
 * @return
 */
int32_t scheduleAsyncExecJob(void *transport, SArray *qnodeList, SQueryDag* pDag, void** pJob);

int32_t scheduleFetchRows(void *pJob, void **data);


/**
 * Cancel query job
 * @param pJob
 * @return
 */
int32_t scheduleCancelJob(void *pJob);

void scheduleFreeJob(void *pJob);

void schedulerDestroy(void);

#ifdef __cplusplus
}
#endif

#endif /*_TD_SCHEDULER_H_*/
