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

#include "command.h"
#include "os.h"
#include "planner.h"
#include "scheduler.h"
#include "tarray.h"
#include "thash.h"
#include "trpc.h"
#include "ttimer.h"

enum {
  SCH_READ = 1,
  SCH_WRITE,
};

enum {
  SCH_EXEC_CB = 1,
  SCH_FETCH_CB,
};

typedef enum {
  SCH_OP_NULL = 0,
  SCH_OP_EXEC,
  SCH_OP_FETCH,
  SCH_OP_GET_STATUS,
} SCH_OP_TYPE;

typedef enum {
  SCH_LOAD_SEQ = 1,
  SCH_RANDOM,
  SCH_ALL,
} SCH_POLICY;

#define SCHEDULE_DEFAULT_MAX_JOB_NUM  1000
#define SCHEDULE_DEFAULT_MAX_TASK_NUM 1000
#define SCHEDULE_DEFAULT_POLICY       SCH_LOAD_SEQ
#define SCHEDULE_DEFAULT_MAX_NODE_NUM 20

#define SCH_DEFAULT_TASK_TIMEOUT_USEC 30000000
#define SCH_MAX_TASK_TIMEOUT_USEC     300000000
#define SCH_DEFAULT_MAX_RETRY_NUM     6
#define SCH_MIN_AYSNC_EXEC_NUM        3

typedef struct SSchDebug {
  bool lockEnable;
  bool apiEnable;
} SSchDebug;

typedef struct SSchTrans {
  void   *pTrans;
  void   *pHandle;
  int64_t pHandleId;
} SSchTrans;

typedef struct SSchHbTrans {
  SRWLatch  lock;
  int64_t   taskNum;
  SRpcCtx   rpcCtx;
  SSchTrans trans;
} SSchHbTrans;

typedef struct SSchApiStat {
#if defined(WINDOWS) || defined(_TD_DARWIN_64)
  size_t avoidCompilationErrors;
#endif

} SSchApiStat;

typedef struct SSchRuntimeStat {
#if defined(WINDOWS) || defined(_TD_DARWIN_64)
  size_t avoidCompilationErrors;
#endif

} SSchRuntimeStat;

typedef struct SSchJobStat {
#if defined(WINDOWS) || defined(_TD_DARWIN_64)
  size_t avoidCompilationErrors;
#endif

} SSchJobStat;

typedef struct SSchStat {
  SSchApiStat     api;
  SSchRuntimeStat runtime;
  SSchJobStat     job;
} SSchStat;

typedef struct SSchResInfo {
  SExecResult     *execRes;
  void           **fetchRes;
  schedulerExecFp  execFp;
  schedulerFetchFp fetchFp;
  void            *cbParam;
} SSchResInfo;

typedef struct SSchOpEvent {
  SCH_OP_TYPE    type;
  bool           begin;
  SSchedulerReq *pReq;
} SSchOpEvent;

typedef int32_t (*schStatusEnterFp)(void *pHandle, void *pParam);
typedef int32_t (*schStatusLeaveFp)(void *pHandle, void *pParam);
typedef int32_t (*schStatusEventFp)(void *pHandle, void *pParam, void *pEvent);

typedef struct SSchStatusFps {
  EJobTaskType     status;
  schStatusEnterFp enterFp;
  schStatusLeaveFp leaveFp;
  schStatusEventFp eventFp;
} SSchStatusFps;

typedef struct SSchedulerCfg {
  uint32_t   maxJobNum;
  int64_t    maxNodeTableNum;
  SCH_POLICY schPolicy;
  bool       enableReSchedule;
} SSchedulerCfg;

typedef struct SSchedulerMgmt {
  uint64_t      taskId;  // sequential taksId
  uint64_t      sId;     // schedulerId
  SSchedulerCfg cfg;
  bool          exit;
  int32_t       jobRef;
  int32_t       jobNum;
  SSchStat      stat;
  void         *timer;
  SRWLatch      hbLock;
  SHashObj     *hbConnections;
  void         *queryMgmt;
} SSchedulerMgmt;

typedef struct SSchCallbackParamHeader {
  bool isHbParam;
} SSchCallbackParamHeader;

typedef struct SSchTaskCallbackParam {
  SSchCallbackParamHeader head;
  uint64_t                queryId;
  int64_t                 refId;
  uint64_t                taskId;
  int32_t                 execId;
  void                   *pTrans;
} SSchTaskCallbackParam;

typedef struct SSchHbCallbackParam {
  SSchCallbackParamHeader head;
  SQueryNodeEpId          nodeEpId;
  void                   *pTrans;
} SSchHbCallbackParam;

typedef struct SSchFlowControl {
  SRWLatch lock;
  bool     sorted;
  int64_t  tableNumSum;
  uint32_t execTaskNum;
  SArray  *taskList;  // Element is SSchTask*
} SSchFlowControl;

typedef struct SSchNodeInfo {
  SQueryNodeAddr addr;
  void          *handle;
} SSchNodeInfo;

typedef struct SSchLevel {
  int32_t  level;
  int8_t   status;
  SRWLatch lock;
  int32_t  taskFailed;
  int32_t  taskSucceed;
  int32_t  taskNum;
  int32_t  taskLaunchedNum;
  int32_t  taskExecDoneNum;
  SArray  *subTasks;  // Element is SSchTask
} SSchLevel;

typedef struct SSchTaskProfile {
  int64_t startTs;
  SArray *execTime;
  int64_t waitTime;
  int64_t endTs;
} SSchTaskProfile;

typedef struct SSchRedirectCtx {
  int32_t periodMs;
  bool    inRedirect;
  int32_t totalTimes;
  int32_t roundTotal;
  int32_t roundTimes;  // retry times in current round
  int64_t startTs;
} SSchRedirectCtx;

typedef struct SSchTimerParam {
  int64_t  rId;
  uint64_t queryId;
  uint64_t taskId;
} SSchTimerParam;

typedef struct SSchTask {
  uint64_t        taskId;          // task id
  SRWLatch        lock;            // task reentrant lock
  int32_t         maxExecTimes;    // task max exec times
  int32_t         maxRetryTimes;   // task max retry times
  int32_t         retryTimes;      // task retry times
  int32_t         delayExecMs;     // task execution delay time
  tmr_h           delayTimer;      // task delay execution timer
  SSchRedirectCtx redirectCtx;     // task redirect context
  bool            waitRetry;       // wait for retry
  int32_t         execId;          // task current execute index
  int32_t         failedExecId;    // last failed task execute index
  SSchLevel      *level;           // level
  SRWLatch        planLock;        // task update plan lock
  SSubplan       *plan;            // subplan
  char           *msg;             // operator tree
  int32_t         msgLen;          // msg length
  int8_t          status;          // task status
  int32_t         lastMsgType;     // last sent msg type
  int64_t         timeoutUsec;     // task timeout useconds before reschedule
  SQueryNodeAddr  succeedAddr;     // task executed success node address
  int32_t         candidateIdx;    // current try condidation index
  SArray         *candidateAddrs;  // condidate node addresses, element is SQueryNodeAddr
  SHashObj       *execNodes;       // all tried node for current task, element is SSchNodeInfo
  SSchTaskProfile profile;         // task execution profile
  int32_t         childReady;      // child task ready number
  SArray         *children;        // the datasource tasks,from which to fetch the result, element is SQueryTask*
  SArray         *parents;         // the data destination tasks, get data from current task, element is SQueryTask*
  void           *handle;          // task send handle
  bool            registerdHb;     // registered in hb
} SSchTask;

typedef struct SSchJobAttr {
  EExplainMode explainMode;
  bool         queryJob;
  bool         insertJob;
  bool         needFetch;
  bool         needFlowCtrl;
  bool         localExec;
} SSchJobAttr;

typedef struct {
  int32_t  op;
  SRWLatch lock;
  bool     syncReq;
} SSchOpStatus;

typedef struct SSchJob {
  int64_t          refId;
  uint64_t         queryId;
  SSchJobAttr      attr;
  int32_t          levelNum;
  int32_t          taskNum;
  SRequestConnInfo conn;
  SArray          *nodeList;  // qnode/vnode list, SArray<SQueryNodeLoad>
  SArray          *levels;    // starting from 0. SArray<SSchLevel>
  SQueryPlan      *pDag;
  int64_t          allocatorRefId;

  SArray   *dataSrcTasks;  // SArray<SQueryTask*>
  int32_t   levelIdx;
  SEpSet    dataSrcEps;
  SHashObj *taskList;
  SHashObj *execTasks;  // executing and executed tasks, key:taskid, value:SQueryTask*
  SHashObj *flowCtrl;   // key is ep, element is SSchFlowControl

  SExplainCtx         *explainCtx;
  int8_t               status;
  SQueryNodeAddr       resNode;
  tsem_t               rspSem;
  SSchOpStatus         opStatus;
  schedulerChkKillFp   chkKillFp;
  void                *chkKillParam;
  SSchTask            *fetchTask;
  int32_t              errCode;
  int32_t              redirectCode;
  SRWLatch             resLock;
  SExecResult          execRes;
  void                *fetchRes;  // TODO free it or not
  bool                 fetched;
  bool                 noMoreRetry;
  int64_t              resNumOfRows;  // from int32_t to int64_t
  SSchResInfo          userRes;
  char                *sql;
  SQueryProfileSummary summary;
} SSchJob;

typedef struct SSchTaskCtx {
  int64_t   jobRid;
  SSchTask *pTask;
  bool      asyncLaunch;
} SSchTaskCtx;

extern SSchedulerMgmt schMgmt;

#define SCH_TASK_TIMEOUT(_task) \
  ((taosGetTimestampUs() - *(int64_t *)taosArrayGet((_task)->profile.execTime, (_task)->execId)) > (_task)->timeoutUsec)

#define SCH_TASK_READY_FOR_LAUNCH(readyNum, task) ((readyNum) >= taosArrayGetSize((task)->children))

#define SCH_LOCK_TASK(_task)   SCH_LOCK(SCH_WRITE, &(_task)->lock)
#define SCH_UNLOCK_TASK(_task) SCH_UNLOCK(SCH_WRITE, &(_task)->lock)

#define SCH_TASK_ID(_task)  ((_task) ? (_task)->taskId : -1)
#define SCH_TASK_EID(_task) ((_task) ? (_task)->execId : -1)

#define SCH_IS_DATA_BIND_QRY_TASK(task) ((task)->plan->subplanType == SUBPLAN_TYPE_SCAN)
#define SCH_IS_DATA_BIND_TASK(task) \
  (((task)->plan->subplanType == SUBPLAN_TYPE_SCAN) || ((task)->plan->subplanType == SUBPLAN_TYPE_MODIFY))
#define SCH_IS_LEAF_TASK(_job, _task) (((_task)->level->level + 1) == (_job)->levelNum)
#define SCH_IS_DATA_MERGE_TASK(task)  (!SCH_IS_DATA_BIND_TASK(task))
#define SCH_IS_LOCAL_EXEC_TASK(_job, _task)                                          \
  ((_job)->attr.localExec && SCH_IS_QUERY_JOB(_job) && (!SCH_IS_INSERT_JOB(_job)) && \
   (!SCH_IS_DATA_BIND_QRY_TASK(_task)))

#define SCH_UPDATE_REDIRECT_CODE(job, _code) atomic_val_compare_exchange_32(&((job)->redirectCode), 0, _code)
#define SCH_GET_REDIRECT_CODE(job, _code) \
  (((!NO_RET_REDIRECT_ERROR(_code)) || (job)->redirectCode == 0) ? (_code) : (job)->redirectCode)

#define SCH_SET_TASK_STATUS(task, st) atomic_store_8(&(task)->status, st)
#define SCH_GET_TASK_STATUS(task)     atomic_load_8(&(task)->status)
#define SCH_GET_TASK_STATUS_STR(task) jobTaskStatusStr(SCH_GET_TASK_STATUS(task))

#define SCH_TASK_ALREADY_LAUNCHED(task) (SCH_GET_TASK_STATUS(task) >= JOB_TASK_STATUS_EXEC)
#define SCH_TASK_EXEC_DONE(task)        (SCH_GET_TASK_STATUS(task) >= JOB_TASK_STATUS_PART_SUCC)

#define SCH_GET_TASK_HANDLE(_task)          ((_task) ? (_task)->handle : NULL)
#define SCH_SET_TASK_HANDLE(_task, _handle) ((_task)->handle = (_handle))

#define SCH_SET_JOB_STATUS(job, st) atomic_store_8(&(job)->status, st)
#define SCH_GET_JOB_STATUS(job)     atomic_load_8(&(job)->status)
#define SCH_GET_JOB_STATUS_STR(job) jobTaskStatusStr(SCH_GET_JOB_STATUS(job))

#define SCH_JOB_IN_SYNC_OP(job) ((job)->opStatus.op && (job)->opStatus.syncReq)
#define SCH_JOB_IN_ASYNC_EXEC_OP(job)                                                                \
  ((SCH_OP_EXEC == atomic_val_compare_exchange_32(&(job)->opStatus.op, SCH_OP_EXEC, SCH_OP_NULL)) && \
   (!(job)->opStatus.syncReq))
#define SCH_JOB_IN_ASYNC_FETCH_OP(job)                                                                 \
  ((SCH_OP_FETCH == atomic_val_compare_exchange_32(&(job)->opStatus.op, SCH_OP_FETCH, SCH_OP_NULL)) && \
   (!(job)->opStatus.syncReq))

#define SCH_SET_JOB_NEED_FLOW_CTRL(_job) (_job)->attr.needFlowCtrl = true
#define SCH_JOB_NEED_FLOW_CTRL(_job)     ((_job)->attr.needFlowCtrl)
#define SCH_TASK_NEED_FLOW_CTRL(_job, _task) \
  (SCH_IS_DATA_BIND_QRY_TASK(_task) && SCH_JOB_NEED_FLOW_CTRL(_job) && SCH_IS_LEVEL_UNFINISHED((_task)->level))
#define SCH_FETCH_TYPE(_pSrcTask)      (SCH_IS_DATA_BIND_QRY_TASK(_pSrcTask) ? TDMT_SCH_FETCH : TDMT_SCH_MERGE_FETCH)
#define SCH_TASK_NEED_FETCH(_task)     ((_task)->plan->subplanType != SUBPLAN_TYPE_MODIFY)
#define SCH_MULTI_LEVEL_LAUNCHED(_job) ((_job)->levelIdx != ((_job)->levelNum - 1))

#define SCH_SET_JOB_TYPE(_job, type)     \
  do {                                   \
    if ((type) != SUBPLAN_TYPE_MODIFY) { \
      (_job)->attr.queryJob = true;      \
    } else {                             \
      (_job)->attr.insertJob = true;     \
    }                                    \
  } while (0)
#define SCH_IS_QUERY_JOB(_job)   ((_job)->attr.queryJob)
#define SCH_IS_INSERT_JOB(_job)  ((_job)->attr.insertJob)
#define SCH_JOB_NEED_FETCH(_job) ((_job)->attr.needFetch)
#define SCH_JOB_NEED_WAIT(_job)  (!SCH_IS_QUERY_JOB(_job))
#define SCH_JOB_NEED_DROP(_job)  (SCH_IS_QUERY_JOB(_job))
#define SCH_IS_EXPLAIN_JOB(_job) (EXPLAIN_MODE_ANALYZE == (_job)->attr.explainMode)
#define SCH_NETWORK_ERR(_code)                                                         \
  ((_code) == TSDB_CODE_RPC_BROKEN_LINK || (_code) == TSDB_CODE_RPC_NETWORK_UNAVAIL || \
   (_code) == TSDB_CODE_RPC_SOMENODE_NOT_CONNECTED)
#define SCH_REDIRECT_MSGTYPE(_msgType)                                                                         \
  ((_msgType) == TDMT_SCH_LINK_BROKEN || (_msgType) == TDMT_SCH_QUERY || (_msgType) == TDMT_SCH_MERGE_QUERY || \
   (_msgType) == TDMT_SCH_FETCH || (_msgType) == TDMT_SCH_MERGE_FETCH)
#define SCH_LOW_LEVEL_NETWORK_ERR(_job, _task, _code) \
  (SCH_NETWORK_ERR(_code) && ((_task)->level->level == (_job)->levelIdx))
#define SCH_TOP_LEVEL_NETWORK_ERR(_job, _task, _code) \
  (SCH_NETWORK_ERR(_code) && ((_task)->level->level > (_job)->levelIdx))
#define SCH_TASK_RETRY_NETWORK_ERR(_task, _code) (SCH_NETWORK_ERR(_code) && (_task)->redirectCtx.inRedirect)

#define SCH_JOB_NEED_RETRY(_job, _task, _msgType, _code) \
  (SCH_REDIRECT_MSGTYPE(_msgType) && SCH_TOP_LEVEL_NETWORK_ERR(_job, _task, _code))
#define SCH_TASKSET_NEED_RETRY(_job, _task, _msgType, _code)                                       \
  (SCH_REDIRECT_MSGTYPE(_msgType) &&                                                               \
   (NEED_SCHEDULER_REDIRECT_ERROR(_code) || SCH_LOW_LEVEL_NETWORK_ERR((_job), (_task), (_code)) || \
    SCH_TASK_RETRY_NETWORK_ERR((_task), (_code))))
#define SCH_TASK_NEED_RETRY(_msgType, _code) \
  ((SCH_REDIRECT_MSGTYPE(_msgType) && SCH_NETWORK_ERR(_code)) || (_code) == TSDB_CODE_SCH_TIMEOUT_ERROR)

#define SCH_IS_LEVEL_UNFINISHED(_level) ((_level)->taskLaunchedNum < (_level)->taskNum)
#define SCH_GET_CUR_EP(_addr)           (&(_addr)->epSet.eps[(_addr)->epSet.inUse])
#define SCH_SWITCH_EPSET(_addr)         ((_addr)->epSet.inUse = ((_addr)->epSet.inUse + 1) % (_addr)->epSet.numOfEps)
#define SCH_TASK_NUM_OF_EPS(_addr)      ((_addr)->epSet.numOfEps)

#define SCH_LOG_TASK_START_TS(_task)               \
  do {                                             \
    int64_t us = taosGetTimestampUs();             \
    taosArrayPush((_task)->profile.execTime, &us); \
    if (0 == (_task)->execId) {                    \
      (_task)->profile.startTs = us;               \
    }                                              \
  } while (0)

#define SCH_LOG_TASK_WAIT_TS(_task)                                                                         \
  do {                                                                                                      \
    int64_t us = taosGetTimestampUs();                                                                      \
    (_task)->profile.waitTime += us - *(int64_t *)taosArrayGet((_task)->profile.execTime, (_task)->execId); \
  } while (0)

#define SCH_LOG_TASK_END_TS(_task)                                               \
  do {                                                                           \
    int64_t  us = taosGetTimestampUs();                                          \
    int32_t  idx = (_task)->execId % (_task)->maxExecTimes;                      \
    int64_t *startts = taosArrayGet((_task)->profile.execTime, (_task)->execId); \
    *startts = us - *startts;                                                    \
    (_task)->profile.endTs = us;                                                 \
  } while (0)

#define SCH_JOB_ELOG(param, ...) qError("QID:0x%" PRIx64 " " param, pJob->queryId, __VA_ARGS__)
#define SCH_JOB_DLOG(param, ...) qDebug("QID:0x%" PRIx64 " " param, pJob->queryId, __VA_ARGS__)

#define SCH_TASK_ELOG(param, ...)                                                                                     \
  qError("QID:0x%" PRIx64 ",TID:0x%" PRIx64 ",EID:%d " param, pJob->queryId, SCH_TASK_ID(pTask), SCH_TASK_EID(pTask), \
         __VA_ARGS__)
#define SCH_TASK_DLOG(param, ...)                                                                                     \
  qDebug("QID:0x%" PRIx64 ",TID:0x%" PRIx64 ",EID:%d " param, pJob->queryId, SCH_TASK_ID(pTask), SCH_TASK_EID(pTask), \
         __VA_ARGS__)
#define SCH_TASK_TLOG(param, ...)                                                                                     \
  qTrace("QID:0x%" PRIx64 ",TID:0x%" PRIx64 ",EID:%d " param, pJob->queryId, SCH_TASK_ID(pTask), SCH_TASK_EID(pTask), \
         __VA_ARGS__)
#define SCH_TASK_DLOGL(param, ...)                                                                                     \
  qDebugL("QID:0x%" PRIx64 ",TID:0x%" PRIx64 ",EID:%d " param, pJob->queryId, SCH_TASK_ID(pTask), SCH_TASK_EID(pTask), \
          __VA_ARGS__)
#define SCH_TASK_WLOG(param, ...)                                                                                    \
  qWarn("QID:0x%" PRIx64 ",TID:0x%" PRIx64 ",EID:%d " param, pJob->queryId, SCH_TASK_ID(pTask), SCH_TASK_EID(pTask), \
        __VA_ARGS__)

#define SCH_SET_ERRNO(_err)                     \
  do {                                          \
    if (TSDB_CODE_SCH_IGNORE_ERROR != (_err)) { \
      terrno = (_err);                          \
    }                                           \
  } while (0)
#define SCH_ERR_RET(c)                \
  do {                                \
    int32_t _code = c;                \
    if (_code != TSDB_CODE_SUCCESS) { \
      SCH_SET_ERRNO(_code);           \
      return _code;                   \
    }                                 \
  } while (0)
#define SCH_RET(c)                    \
  do {                                \
    int32_t _code = c;                \
    if (_code != TSDB_CODE_SUCCESS) { \
      SCH_SET_ERRNO(_code);           \
    }                                 \
    return _code;                     \
  } while (0)
#define SCH_ERR_JRET(c)              \
  do {                               \
    code = c;                        \
    if (code != TSDB_CODE_SUCCESS) { \
      SCH_SET_ERRNO(code);           \
      goto _return;                  \
    }                                \
  } while (0)

#define SCH_LOCK_DEBUG(...)     \
  do {                          \
    if (gSCHDebug.lockEnable) { \
      qDebug(__VA_ARGS__);      \
    }                           \
  } while (0)

#define TD_RWLATCH_WRITE_FLAG_COPY 0x40000000

#define SCH_LOCK(type, _lock)                                                                              \
  do {                                                                                                     \
    if (SCH_READ == (type)) {                                                                              \
      ASSERTS(atomic_load_32(_lock) >= 0, "invalid lock value before read lock");                          \
      SCH_LOCK_DEBUG("SCH RLOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);       \
      taosRLockLatch(_lock);                                                                               \
      SCH_LOCK_DEBUG("SCH RLOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);       \
      ASSERTS(atomic_load_32(_lock) > 0, "invalid lock value after read lock");                            \
    } else {                                                                                               \
      ASSERTS(atomic_load_32(_lock) >= 0, "invalid lock value before write lock");                         \
      SCH_LOCK_DEBUG("SCH WLOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);       \
      taosWLockLatch(_lock);                                                                               \
      SCH_LOCK_DEBUG("SCH WLOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);       \
      ASSERTS(atomic_load_32(_lock) == TD_RWLATCH_WRITE_FLAG_COPY, "invalid lock value after write lock"); \
    }                                                                                                      \
  } while (0)

#define SCH_UNLOCK(type, _lock)                                                                                \
  do {                                                                                                         \
    if (SCH_READ == (type)) {                                                                                  \
      ASSERTS(atomic_load_32((_lock)) > 0, "invalid lock value before read unlock");                           \
      SCH_LOCK_DEBUG("SCH RULOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);          \
      taosRUnLockLatch(_lock);                                                                                 \
      SCH_LOCK_DEBUG("SCH RULOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);          \
      ASSERTS(atomic_load_32((_lock)) >= 0, "invalid lock value after read unlock");                           \
    } else {                                                                                                   \
      ASSERTS(atomic_load_32((_lock)) & TD_RWLATCH_WRITE_FLAG_COPY, "invalid lock value before write unlock"); \
      SCH_LOCK_DEBUG("SCH WULOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);          \
      taosWUnLockLatch(_lock);                                                                                 \
      SCH_LOCK_DEBUG("SCH WULOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);          \
      ASSERTS(atomic_load_32((_lock)) >= 0, "invalid lock value after write unlock");                          \
    }                                                                                                          \
  } while (0)

#define SCH_RESET_JOB_LEVEL_IDX(_job)                         \
  do {                                                        \
    (_job)->levelIdx = (_job)->levelNum - 1;                  \
    SCH_JOB_DLOG("set job levelIdx to %d", (_job)->levelIdx); \
  } while (0)

void     schDeregisterTaskHb(SSchJob *pJob, SSchTask *pTask);
void     schCleanClusterHb(void *pTrans);
int32_t  schLaunchTask(SSchJob *job, SSchTask *task);
int32_t  schDelayLaunchTask(SSchJob *pJob, SSchTask *pTask);
int32_t  schBuildAndSendMsg(SSchJob *job, SSchTask *task, SQueryNodeAddr *addr, int32_t msgType, void *param);
SSchJob *schAcquireJob(int64_t refId);
int32_t  schReleaseJob(int64_t refId);
void     schFreeFlowCtrl(SSchJob *pJob);
int32_t  schChkJobNeedFlowCtrl(SSchJob *pJob, SSchLevel *pLevel);
int32_t  schDecTaskFlowQuota(SSchJob *pJob, SSchTask *pTask);
int32_t  schCheckIncTaskFlowQuota(SSchJob *pJob, SSchTask *pTask, bool *enough);
int32_t  schLaunchTasksInFlowCtrlList(SSchJob *pJob, SSchTask *pTask);
int32_t  schAsyncLaunchTaskImpl(SSchJob *pJob, SSchTask *pTask);
int32_t  schLaunchFetchTask(SSchJob *pJob);
int32_t  schProcessOnTaskFailure(SSchJob *pJob, SSchTask *pTask, int32_t errCode);
int32_t  schBuildAndSendHbMsg(SQueryNodeEpId *nodeEpId, SArray *taskAction);
int32_t  schCloneSMsgSendInfo(void *src, void **dst);
int32_t  schValidateAndBuildJob(SQueryPlan *pDag, SSchJob *pJob);
void     schFreeJobImpl(void *job);
int32_t  schMakeHbRpcCtx(SSchJob *pJob, SSchTask *pTask, SRpcCtx *pCtx);
int32_t  schEnsureHbConnection(SSchJob *pJob, SSchTask *pTask);
int32_t  schUpdateHbConnection(SQueryNodeEpId *epId, SSchTrans *trans);
int32_t  schHandleHbCallback(void *param, SDataBuf *pMsg, int32_t code);
void     schFreeRpcCtx(SRpcCtx *pCtx);
int32_t  schGetCallbackFp(int32_t msgType, __async_send_cb_fn_t *fp);
bool     schJobNeedToStop(SSchJob *pJob, int8_t *pStatus);
int32_t  schProcessOnTaskSuccess(SSchJob *pJob, SSchTask *pTask);
int32_t  schSaveJobExecRes(SSchJob *pJob, SQueryTableRsp *rsp);
int32_t  schProcessOnExplainDone(SSchJob *pJob, SSchTask *pTask, SRetrieveTableRsp *pRsp);
void     schProcessOnDataFetched(SSchJob *job);
int32_t  schGetTaskInJob(SSchJob *pJob, uint64_t taskId, SSchTask **pTask);
void     schFreeRpcCtxVal(const void *arg);
int32_t  schMakeBrokenLinkVal(SSchJob *pJob, SSchTask *pTask, SRpcBrokenlinkVal *brokenVal, bool isHb);
int32_t  schAppendTaskExecNode(SSchJob *pJob, SSchTask *pTask, SQueryNodeAddr *addr, int32_t execId);
int32_t  schExecStaticExplainJob(SSchedulerReq *pReq, int64_t *job, bool sync);
int32_t  schUpdateJobStatus(SSchJob *pJob, int8_t newStatus);
int32_t  schCancelJob(SSchJob *pJob);
int32_t  schProcessOnJobDropped(SSchJob *pJob, int32_t errCode);
uint64_t schGenTaskId(void);
void     schCloseJobRef(void);
int32_t  schAsyncExecJob(SSchedulerReq *pReq, int64_t *pJob);
int32_t  schJobFetchRows(SSchJob *pJob);
int32_t  schJobFetchRowsA(SSchJob *pJob);
int32_t  schUpdateTaskHandle(SSchJob *pJob, SSchTask *pTask, bool dropExecNode, void *handle, int32_t execId);
int32_t  schProcessOnTaskStatusRsp(SQueryNodeEpId *pEpId, SArray *pStatusList);
char    *schDumpEpSet(SEpSet *pEpSet);
char    *schGetOpStr(SCH_OP_TYPE type);
int32_t  schBeginOperation(SSchJob *pJob, SCH_OP_TYPE type, bool sync);
int32_t  schInitJob(int64_t *pJobId, SSchedulerReq *pReq);
int32_t  schExecJob(SSchJob *pJob, SSchedulerReq *pReq);
int32_t  schDumpJobExecRes(SSchJob *pJob, SExecResult *pRes);
int32_t  schUpdateTaskCandidateAddr(SSchJob *pJob, SSchTask *pTask, SEpSet *pEpSet);
int32_t  schHandleTaskSetRetry(SSchJob *pJob, SSchTask *pTask, SDataBuf *pData, int32_t rspCode);
void     schProcessOnOpEnd(SSchJob *pJob, SCH_OP_TYPE type, SSchedulerReq *pReq, int32_t errCode);
int32_t  schProcessOnOpBegin(SSchJob *pJob, SCH_OP_TYPE type, SSchedulerReq *pReq);
void     schProcessOnCbEnd(SSchJob *pJob, SSchTask *pTask, int32_t errCode);
int32_t  schProcessOnCbBegin(SSchJob **job, SSchTask **task, uint64_t qId, int64_t rId, uint64_t tId);
void     schDropTaskOnExecNode(SSchJob *pJob, SSchTask *pTask);
bool     schJobDone(SSchJob *pJob);
int32_t  schRemoveTaskFromExecList(SSchJob *pJob, SSchTask *pTask);
int32_t  schLaunchJobLowerLevel(SSchJob *pJob, SSchTask *pTask);
int32_t  schSwitchJobStatus(SSchJob *pJob, int32_t status, void *param);
int32_t  schHandleOpBeginEvent(int64_t jobId, SSchJob **job, SCH_OP_TYPE type, SSchedulerReq *pReq);
int32_t  schHandleOpEndEvent(SSchJob *pJob, SCH_OP_TYPE type, SSchedulerReq *pReq, int32_t errCode);
int32_t  schHandleTaskRetry(SSchJob *pJob, SSchTask *pTask);
void     schUpdateJobErrCode(SSchJob *pJob, int32_t errCode);
int32_t  schTaskCheckSetRetry(SSchJob *pJob, SSchTask *pTask, int32_t errCode, bool *needRetry);
int32_t  schProcessOnJobFailure(SSchJob *pJob, int32_t errCode);
int32_t  schProcessOnJobPartialSuccess(SSchJob *pJob);
void     schFreeTask(SSchJob *pJob, SSchTask *pTask);
void     schDropTaskInHashList(SSchJob *pJob, SHashObj *list);
int32_t  schNotifyTaskInHashList(SSchJob *pJob, SHashObj *list, ETaskNotifyType type, SSchTask *pTask);
int32_t  schLaunchLevelTasks(SSchJob *pJob, SSchLevel *level);
int32_t  schGetTaskFromList(SHashObj *pTaskList, uint64_t taskId, SSchTask **pTask);
int32_t  schInitTask(SSchJob *pJob, SSchTask *pTask, SSubplan *pPlan, SSchLevel *pLevel);
int32_t  schSwitchTaskCandidateAddr(SSchJob *pJob, SSchTask *pTask);
void     schDirectPostJobRes(SSchedulerReq *pReq, int32_t errCode);
int32_t  schHandleJobFailure(SSchJob *pJob, int32_t errCode);
int32_t  schHandleJobDrop(SSchJob *pJob, int32_t errCode);
bool     schChkCurrentOp(SSchJob *pJob, int32_t op, int8_t sync);
int32_t  schProcessFetchRsp(SSchJob *pJob, SSchTask *pTask, char *msg, int32_t rspCode);
int32_t  schProcessExplainRsp(SSchJob *pJob, SSchTask *pTask, SExplainRsp *rsp);
int32_t  schHandleJobRetry(SSchJob *pJob, SSchTask *pTask, SDataBuf *pMsg, int32_t rspCode);
int32_t  schChkResetJobRetry(SSchJob *pJob, int32_t rspCode);
void     schResetTaskForRetry(SSchJob *pJob, SSchTask *pTask);
int32_t  schChkUpdateRedirectCtx(SSchJob *pJob, SSchTask *pTask, SEpSet *pEpSet, int32_t rspCode);
int32_t  schNotifyJobAllTasks(SSchJob *pJob, SSchTask *pTask, ETaskNotifyType type);

extern SSchDebug gSCHDebug;

#ifdef __cplusplus
}
#endif

#endif /*_TD_SCHEDULER_INT_H_*/
