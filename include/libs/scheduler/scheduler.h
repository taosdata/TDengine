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

typedef struct SQueryTask {
  uint64_t            queryId; // query id
  uint64_t            taskId;  // task id
  char     *pSubplan;   // operator tree
  uint64_t            status;  // task status
  SQueryProfileSummary summary; // task execution summary
  void               *pOutputHandle; // result buffer handle, to temporarily keep the output result for next stage
} SQueryTask;

typedef struct SQueryJob {
  SArray  **pSubtasks;
  // todo
} SQueryJob;

/**
 * Process the query job, generated according to the query physical plan.
 * This is a synchronized API, and is also thread-safety.
 * @param pJob
 * @return
 */
int32_t qProcessQueryJob(struct SQueryJob* pJob);

/**
 * The SSqlObj should not be here????
 * @param pSql
 * @param pVgroupId
 * @param pRetVgroupId
 * @return
 */
//SArray* qGetInvolvedVgroupIdList(struct SSqlObj* pSql, SArray* pVgroupId, SArray* pRetVgroupId);

/**
 * Cancel query job
 * @param pJob
 * @return
 */
int32_t qKillQueryJob(struct SQueryJob* pJob);

#ifdef __cplusplus
}
#endif

#endif /*_TD_SCHEDULER_H_*/