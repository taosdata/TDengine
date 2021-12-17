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
#include "thash.h"

#define SCHEDULE_DEFAULT_JOB_NUMBER 1000

enum {
  SCH_STATUS_NOT_START = 1,
  SCH_STATUS_EXECUTING,
  SCH_STATUS_SUCCEED,
  SCH_STATUS_FAILED,
  SCH_STATUS_CANCELLING,
  SCH_STATUS_CANCELLED
};

typedef struct SSchedulerMgmt {
  SHashObj *Jobs;  // key: queryId, value: SQueryJob*
} SSchedulerMgmt;

typedef struct SQueryTask {
  uint64_t             taskId;  // task id
  char                *pSubplan;   // operator tree
  int8_t               status;  // task status
  SQueryProfileSummary summary; // task execution summary
} SQueryTask;

typedef struct SQueryLevel {
  int8_t  status;
  int32_t taskNum;
  
  SArray *subTasks;  // Element is SQueryTask
  SArray *subPlans;  // Element is SSubplan
} SQueryLevel;

typedef struct SQueryJob {
  uint64_t  queryId;
  int32_t   levelNum;
  int32_t   levelIdx;
  int8_t    status;
  SQueryProfileSummary summary;
  
  SArray  *levels;    // Element is SQueryLevel, starting from 0.
  SArray  *subPlans;  // Element is SArray*, and nested element is SSubplan. The execution level of subplan, starting from 0.
} SQueryJob;


#define SCH_ERR_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { return _code; } } while (0)
#define SCH_ERR_LRET(c,...) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { qError(__VA_ARGS__); return _code; } } while (0)
#define SCH_ERR_JRET(c) do { code = c; if (code != TSDB_CODE_SUCCESS) { goto _return; } } while (0)


#ifdef __cplusplus
}
#endif

#endif /*_TD_SCHEDULER_INT_H_*/
