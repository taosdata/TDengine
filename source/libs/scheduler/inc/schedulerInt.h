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

#define SCHEDULE_DEFAULT_MAX_JOB_NUM 1000
#define SCHEDULE_DEFAULT_MAX_TASK_NUM 1000
#define SCHEDULE_DEFAULT_MAX_NODE_TABLE_NUM 200  // unit is TSDB_TABLE_NUM_UNIT

#define SCH_MAX_CANDIDATE_EP_NUM TSDB_MAX_REPLICA

enum {
  SCH_READ = 1,
  SCH_WRITE,
};

typedef struct SSchTrans {
  void *transInst;
  void *transHandle;
} SSchTrans;

typedef struct SSchHbTrans {
  SRWLatch  lock;
  uint64_t  seqId;
  SSchTrans trans;
} SSchHbTrans;

typedef struct SSchApiStat {

} SSchApiStat;

typedef struct SSchRuntimeStat {

} SSchRuntimeStat;

typedef struct SSchJobStat {

} SSchJobStat;

typedef struct SSchedulerStat {
  SSchApiStat      api;
  SSchRuntimeStat  runtime;
  SSchJobStat      job;
} SSchedulerStat;


typedef struct SSchedulerMgmt {
  uint64_t        taskId; // sequential taksId
  uint64_t        sId;    // schedulerId
  SSchedulerCfg   cfg;
  int32_t         jobRef;
  SSchedulerStat  stat;
  SHashObj       *hbConnections;
} SSchedulerMgmt;

typedef struct SSchCallbackParam {
  uint64_t queryId;
  int64_t  refId;
  uint64_t taskId;
  void    *transport;
} SSchCallbackParam;

typedef struct SSchFlowControl {
  SRWLatch  lock;
  bool      sorted;
  int32_t   tableNumSum;
  uint32_t  execTaskNum;
  SArray   *taskList;      // Element is SSchTask*
} SSchFlowControl;

typedef struct SSchLevel {
  int32_t         level;
  int8_t          status;
  SRWLatch        lock;
  int32_t         taskFailed;
  int32_t         taskSucceed;
  int32_t         taskNum;
  int32_t         taskLaunchedNum;
  SHashObj       *flowCtrl;      // key is ep, element is SSchFlowControl
  SArray         *subTasks;      // Element is SQueryTask
} SSchLevel;

typedef struct SSchTask {
  uint64_t             taskId;         // task id
  SRWLatch             lock;           // task lock
  SSchLevel           *level;          // level
  SSubplan            *plan;           // subplan
  char                *msg;            // operator tree
  int32_t              msgLen;         // msg length
  int8_t               status;         // task status
  int32_t              lastMsgType;    // last sent msg type
  int32_t              tryTimes;       // task already tried times
  SQueryNodeAddr       succeedAddr;    // task executed success node address
  int8_t               candidateIdx;   // current try condidation index
  SArray              *candidateAddrs; // condidate node addresses, element is SQueryNodeAddr
  SArray              *execAddrs;      // all tried node for current task, element is SQueryNodeAddr
  SQueryProfileSummary summary;        // task execution summary
  int32_t              childReady;     // child task ready number
  SArray              *children;       // the datasource tasks,from which to fetch the result, element is SQueryTask*
  SArray              *parents;        // the data destination tasks, get data from current task, element is SQueryTask*
  void*                handle;          // task send handle 
} SSchTask;

typedef struct SSchJobAttr {
  bool needFetch;
  bool syncSchedule;
  bool queryJob;
  bool needFlowCtrl;
} SSchJobAttr;

typedef struct SSchJob {
  int64_t          refId;
  uint64_t         queryId;
  SSchJobAttr      attr;
  int32_t          levelNum;
  int32_t          taskNum;
  void            *transport;
  SArray          *nodeList;   // qnode/vnode list, element is SQueryNodeAddr
  SArray          *levels;    // Element is SQueryLevel, starting from 0. SArray<SSchLevel>
  SNodeList       *subPlans;  // subplan pointer copied from DAG, no need to free it in scheduler

  int32_t          levelIdx;
  SEpSet           dataSrcEps;
  SHashObj        *execTasks; // executing tasks, key:taskid, value:SQueryTask*
  SHashObj        *succTasks; // succeed tasks, key:taskid, value:SQueryTask*
  SHashObj        *failTasks; // failed tasks, key:taskid, value:SQueryTask*

  int8_t           status;  
  SQueryNodeAddr   resNode;
  tsem_t           rspSem;
  int8_t           userFetch;
  int32_t          remoteFetch;
  SSchTask        *fetchTask;
  int32_t          errCode;
  SArray          *errList;    // SArray<SQueryErrorInfo>
  void            *resData;         //TODO free it or not
  int32_t          resNumOfRows;
  const char      *sql;
  SQueryProfileSummary summary;
} SSchJob;

extern SSchedulerMgmt schMgmt;

#define SCH_TASK_READY_TO_LUNCH(readyNum, task) ((readyNum) >= taosArrayGetSize((task)->children))

#define SCH_TASK_ID(_task) ((_task) ? (_task)->taskId : -1)
#define SCH_SET_TASK_LASTMSG_TYPE(_task, _type) do { if(_task) { atomic_store_32(&(_task)->lastMsgType, _type); } } while (0)
#define SCH_GET_TASK_LASTMSG_TYPE(_task) ((_task) ? atomic_load_32(&(_task)->lastMsgType) : -1)

#define SCH_IS_DATA_SRC_QRY_TASK(task) ((task)->plan->subplanType == SUBPLAN_TYPE_SCAN)
#define SCH_IS_DATA_SRC_TASK(task) (((task)->plan->subplanType == SUBPLAN_TYPE_SCAN) || ((task)->plan->subplanType == SUBPLAN_TYPE_MODIFY))
#define SCH_IS_LEAF_TASK(_job, _task) (((_task)->level->level + 1) == (_job)->levelNum)

#define SCH_SET_TASK_STATUS(task, st) atomic_store_8(&(task)->status, st)
#define SCH_GET_TASK_STATUS(task) atomic_load_8(&(task)->status)
#define SCH_GET_TASK_STATUS_STR(task) jobTaskStatusStr(SCH_GET_TASK_STATUS(task))


#define SCH_SET_JOB_STATUS(job, st) atomic_store_8(&(job)->status, st)
#define SCH_GET_JOB_STATUS(job) atomic_load_8(&(job)->status)
#define SCH_GET_JOB_STATUS_STR(job) jobTaskStatusStr(SCH_GET_JOB_STATUS(job))

#define SCH_SET_JOB_NEED_FLOW_CTRL(_job) (_job)->attr.needFlowCtrl = true
#define SCH_JOB_NEED_FLOW_CTRL(_job) ((_job)->attr.needFlowCtrl)
#define SCH_TASK_NEED_FLOW_CTRL(_job, _task) (SCH_IS_DATA_SRC_QRY_TASK(_task) && SCH_JOB_NEED_FLOW_CTRL(_job) && SCH_IS_LEAF_TASK(_job, _task) && SCH_IS_LEVEL_UNFINISHED((_task)->level))

#define SCH_SET_JOB_TYPE(_job, type) (_job)->attr.queryJob = ((type) != SUBPLAN_TYPE_MODIFY)
#define SCH_IS_QUERY_JOB(_job) ((_job)->attr.queryJob) 
#define SCH_JOB_NEED_FETCH(_job) SCH_IS_QUERY_JOB(_job)
#define SCH_IS_WAIT_ALL_JOB(_job) (!SCH_IS_QUERY_JOB(_job))
#define SCH_IS_NEED_DROP_JOB(_job) (SCH_IS_QUERY_JOB(_job))

#define SCH_IS_LEVEL_UNFINISHED(_level) ((_level)->taskLaunchedNum < (_level)->taskNum)
#define SCH_GET_CUR_EP(_addr) (&(_addr)->epSet.eps[(_addr)->epSet.inUse])
#define SCH_SWITCH_EPSET(_addr) ((_addr)->epSet.inUse = ((_addr)->epSet.inUse + 1) % (_addr)->epSet.numOfEps)
#define SCH_TASK_NUM_OF_EPS(_addr) ((_addr)->epSet.numOfEps)

#define SCH_JOB_ELOG(param, ...) qError("QID:0x%" PRIx64 " " param, pJob->queryId, __VA_ARGS__)
#define SCH_JOB_DLOG(param, ...) qDebug("QID:0x%" PRIx64 " " param, pJob->queryId, __VA_ARGS__)

#define SCH_TASK_ELOG(param, ...) \
  qError("QID:0x%" PRIx64 ",TID:0x%" PRIx64 " " param, pJob->queryId, SCH_TASK_ID(pTask), __VA_ARGS__)
#define SCH_TASK_DLOG(param, ...) \
  qDebug("QID:0x%" PRIx64 ",TID:0x%" PRIx64 " " param, pJob->queryId, SCH_TASK_ID(pTask), __VA_ARGS__)
#define SCH_TASK_WLOG(param, ...) \
  qWarn("QID:0x%" PRIx64 ",TID:0x%" PRIx64 " " param, pJob->queryId, SCH_TASK_ID(pTask), __VA_ARGS__)

#define SCH_ERR_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; return _code; } } while (0)
#define SCH_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; } return _code; } while (0)
#define SCH_ERR_JRET(c) do { code = c; if (code != TSDB_CODE_SUCCESS) { terrno = code; goto _return; } } while (0)

#define SCH_LOCK(type, _lock) (SCH_READ == (type) ? taosRLockLatch(_lock) : taosWLockLatch(_lock))
#define SCH_UNLOCK(type, _lock) (SCH_READ == (type) ? taosRUnLockLatch(_lock) : taosWUnLockLatch(_lock))


int32_t schLaunchTask(SSchJob *job, SSchTask *task);
int32_t schBuildAndSendMsg(SSchJob *job, SSchTask *task, SQueryNodeAddr *addr, int32_t msgType);
SSchJob *schAcquireJob(int64_t refId);
int32_t schReleaseJob(int64_t refId);
void schFreeFlowCtrl(SSchLevel *pLevel);
int32_t schCheckJobNeedFlowCtrl(SSchJob *pJob, SSchLevel *pLevel);
int32_t schDecTaskFlowQuota(SSchJob *pJob, SSchTask *pTask);
int32_t schCheckIncTaskFlowQuota(SSchJob *pJob, SSchTask *pTask, bool *enough);
int32_t schLaunchTasksInFlowCtrlList(SSchJob *pJob, SSchTask *pTask);
int32_t schLaunchTaskImpl(SSchJob *pJob, SSchTask *pTask);
int32_t schFetchFromRemote(SSchJob *pJob);
int32_t schProcessOnTaskFailure(SSchJob *pJob, SSchTask *pTask, int32_t errCode);


#ifdef __cplusplus
}
#endif

#endif /*_TD_SCHEDULER_INT_H_*/
