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
#define SCHEDULE_DEFAULT_TASK_NUMBER 1000

#define SCH_MAX_CONDIDATE_EP_NUM TSDB_MAX_REPLICA

enum {
  SCH_STATUS_NOT_START = 1,
  SCH_STATUS_EXECUTING,
  SCH_STATUS_SUCCEED,
  SCH_STATUS_FAILED,
  SCH_STATUS_CANCELLING,
  SCH_STATUS_CANCELLED
};

typedef struct SSchedulerMgmt {
  uint64_t  taskId;
  SSchedulerCfg cfg;
  SHashObj *Jobs;  // key: queryId, value: SQueryJob*
} SSchedulerMgmt;

typedef struct SQueryTask {
  uint64_t             taskId;     // task id
  SSubplan            *plan;       // subplan
  char                *msg;        // operator tree
  int8_t               status;     // task status
  SEpAddr              execAddr;   // task actual executed node address
  SQueryProfileSummary summary;    // task execution summary
  int32_t              childReady; // child task ready number
  SArray              *children;   // the datasource tasks,from which to fetch the result, element is SQueryTask*
  SArray              *parents;    // the data destination tasks, get data from current task, element is SQueryTask*
} SQueryTask;

typedef struct SQueryLevel {
  int32_t level;
  int8_t  status;
  int32_t taskNum;
  SArray *subTasks;  // Element is SQueryTask
} SQueryLevel;

typedef struct SQueryJob {
  uint64_t  queryId;
  int32_t   levelNum;
  int32_t   levelIdx;
  int8_t    status;
  SQueryProfileSummary summary;
  SEpSet    dataSrcEps;
  SEpAddr   resEp;
  struct SCatalog *catalog;
  void            *rpc;
  SEpSet          *mgmtEpSet;
  tsem_t           rspSem;
  int32_t          userFetch;
  int32_t          remoteFetch;
  void            *res;

  SHashObj *execTasks; // executing tasks, key:taskid, value:SQueryTask*
  SHashObj *succTasks; // succeed tasks, key:taskid, value:SQueryTask*
    
  SArray   *levels;    // Element is SQueryLevel, starting from 0.
  SArray   *subPlans;  // Element is SArray*, and nested element is SSubplan. The execution level of subplan, starting from 0.
} SQueryJob;

#define SCH_HAS_QNODE_IN_CLUSTER(type) (false) //TODO CLUSTER TYPE
#define SCH_TASK_READY_TO_LUNCH(task) ((task)->childReady >= taosArrayGetSize((task)->children))   // MAY NEED TO ENHANCE
#define SCH_IS_DATA_SRC_TASK(task) (task->plan->type == QUERY_TYPE_SCAN)

#define SCH_JOB_ERR_LOG(param, ...) qError("QID:%"PRIx64 param, job->queryId, __VA_ARGS__)
#define SCH_TASK_ERR_LOG(param, ...) qError("QID:%"PRIx64",TID:%"PRIx64 param, job->queryId, task->taskId, __VA_ARGS__)

#define SCH_ERR_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; return _code; } } while (0)
#define SCH_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; } return _code; } while (0)
#define SCH_ERR_LRET(c,...) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { qError(__VA_ARGS__); terrno = _code; return _code; } } while (0)
#define SCH_ERR_JRET(c) do { code = c; if (code != TSDB_CODE_SUCCESS) { terrno = code; goto _return; } } while (0)


extern int32_t schTaskRun(SQueryJob *job, SQueryTask *task);

#ifdef __cplusplus
}
#endif

#endif /*_TD_SCHEDULER_INT_H_*/
