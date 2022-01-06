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
  SCH_READ = 1,
  SCH_WRITE,
};

typedef struct SSchedulerMgmt {
  uint64_t  taskId; 
  uint64_t  sId;
  SSchedulerCfg cfg;
  SHashObj *jobs;  // key: queryId, value: SQueryJob*
} SSchedulerMgmt;

typedef struct SSchCallbackParam {
  uint64_t queryId;
  uint64_t taskId;
} SSchCallbackParam;

typedef struct SSchLevel {
  int32_t  level;
  int8_t   status;
  SRWLatch lock;
  int32_t  taskFailed;
  int32_t  taskSucceed;
  int32_t  taskNum;
  SArray  *subTasks;  // Element is SQueryTask
} SSchLevel;


typedef struct SSchTask {
  uint64_t             taskId;         // task id
  SSchLevel           *level;          // level
  SSubplan            *plan;           // subplan
  char                *msg;            // operator tree
  int32_t              msgLen;         // msg length
  int8_t               status;         // task status
  SQueryNodeAddr       execAddr;       // task actual executed node address
  int8_t               condidateIdx;   // current try condidation index
  SArray              *condidateAddrs; // condidate node addresses, element is SQueryNodeAddr
  SQueryProfileSummary summary;        // task execution summary
  int32_t              childReady;     // child task ready number
  SArray              *children;       // the datasource tasks,from which to fetch the result, element is SQueryTask*
  SArray              *parents;        // the data destination tasks, get data from current task, element is SQueryTask*
} SSchTask;

typedef struct SSchJobAttr {
  bool needFetch;
  bool syncSchedule;
  bool queryJob;
} SSchJobAttr;

typedef struct SSchJob {
  uint64_t  queryId;
  int32_t   levelNum;
  int32_t   levelIdx;
  int8_t    status;
  SSchJobAttr    attr;
  SQueryProfileSummary summary;
  SEpSet           dataSrcEps;
  SEpAddr          resEp;
  void            *transport;
  SArray          *nodeList;   // qnode/vnode list, element is SQueryNodeAddr
  tsem_t           rspSem;
  int32_t          userFetch;
  int32_t          remoteFetch;

  SSchTask        *fetchTask;
  int32_t          errCode;
  void            *res;
  int32_t          resNumOfRows;
  
  SHashObj *execTasks; // executing tasks, key:taskid, value:SQueryTask*
  SHashObj *succTasks; // succeed tasks, key:taskid, value:SQueryTask*
  SHashObj *failTasks; // failed tasks, key:taskid, value:SQueryTask*
    
  SArray   *levels;    // Element is SQueryLevel, starting from 0.
  SArray   *subPlans;  // Element is SArray*, and nested element is SSubplan. The execution level of subplan, starting from 0.
} SSchJob;

#define SCH_HAS_QNODE_IN_CLUSTER(type) (false) //TODO CLUSTER TYPE
#define SCH_TASK_READY_TO_LUNCH(task) ((task)->childReady >= taosArrayGetSize((task)->children))   // MAY NEED TO ENHANCE
#define SCH_IS_DATA_SRC_TASK(task) ((task)->plan->type == QUERY_TYPE_SCAN)
#define SCH_TASK_NEED_WAIT_ALL(task) ((task)->plan->type == QUERY_TYPE_MODIFY)

#define SCH_JOB_ELOG(param, ...) qError("QID:% "PRIx64 param, job->queryId, __VA_ARGS__)
#define SCH_TASK_ELOG(param, ...) qError("QID:%"PRIx64",TID:% "PRIx64 param, job->queryId, task->taskId, __VA_ARGS__)
#define SCH_TASK_DLOG(param, ...) qDebug("QID:%"PRIx64",TID:% "PRIx64 param, job->queryId, task->taskId, __VA_ARGS__)

#define SCH_ERR_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; return _code; } } while (0)
#define SCH_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; } return _code; } while (0)
#define SCH_ERR_LRET(c,...) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { qError(__VA_ARGS__); terrno = _code; return _code; } } while (0)
#define SCH_ERR_JRET(c) do { code = c; if (code != TSDB_CODE_SUCCESS) { terrno = code; goto _return; } } while (0)

#define SCH_LOCK(type, _lock) (SCH_READ == (type) ? taosRLockLatch(_lock) : taosWLockLatch(_lock))
#define SCH_UNLOCK(type, _lock) (SCH_READ == (type) ? taosRUnLockLatch(_lock) : taosWUnLockLatch(_lock))


extern int32_t schLaunchTask(SSchJob *job, SSchTask *task);
extern int32_t schBuildAndSendMsg(SSchJob *job, SSchTask *task, int32_t msgType);

#ifdef __cplusplus
}
#endif

#endif /*_TD_SCHEDULER_INT_H_*/
