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

#ifndef _TD_SCHEDULER_INT_H_
#define _TD_SCHEDULER_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "tarray.h"
#include "planner.h"
#include "scheduler.h"

typedef struct SQuery {
  SArray     **pSubquery;
  int32_t      numOfLevels;
  int32_t      currentLevel;
} SQuery;

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

#ifdef __cplusplus
}
#endif

#endif /*_TD_SCHEDULER_INT_H_*/
