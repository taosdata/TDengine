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
#include "trpc.h"
#include "command.h"

#define SCHEDULE_DEFAULT_MAX_JOB_NUM 1000
#define SCHEDULE_DEFAULT_MAX_TASK_NUM 1000
#define SCHEDULE_DEFAULT_MAX_NODE_TABLE_NUM 200  // unit is TSDB_TABLE_NUM_UNIT

#define SCH_MAX_CANDIDATE_EP_NUM TSDB_MAX_REPLICA

enum {
  SCH_READ = 1,
  SCH_WRITE,
};

enum {
  SCH_EXEC_CB = 1,
  SCH_FETCH_CB,
};

typedef struct SSchTrans {
  void *pTrans;
  void *pHandle;
} SSchTrans;

typedef struct SSchHbTrans {
  SRWLatch  lock;
  SRpcCtx   rpcCtx;
  SSchTrans trans;
} SSchHbTrans;

typedef struct SSchApiStat {

#ifdef WINDOWS
  size_t avoidCompilationErrors;
#endif

} SSchApiStat;

typedef struct SSchRuntimeStat {

#ifdef WINDOWS
  size_t avoidCompilationErrors;
#endif

} SSchRuntimeStat;

typedef struct SSchJobStat {

#ifdef WINDOWS
  size_t avoidCompilationErrors;
#endif

} SSchJobStat;

typedef struct SSchStat {
  SSchApiStat      api;
  SSchRuntimeStat  runtime;
  SSchJobStat      job;
} SSchStat;

typedef struct SSchResInfo {
  SQueryResult*          queryRes;
  void**                 fetchRes;
  schedulerExecCallback  execFp; 
  schedulerFetchCallback fetchFp; 
  void*                  userParam;
} SSchResInfo;

typedef struct SSchedulerMgmt {
  uint64_t        taskId; // sequential taksId
  uint64_t        sId;    // schedulerId
  SSchedulerCfg   cfg;
  SRWLatch        lock;
  bool            exit;
  int32_t         jobRef;
  int32_t         jobNum;
  SSchStat        stat;
  SHashObj       *hbConnections;
} SSchedulerMgmt;

typedef struct SSchCallbackParamHeader {
  bool isHbParam;
} SSchCallbackParamHeader;

typedef struct SSchTaskCallbackParam {
  SSchCallbackParamHeader head;
  uint64_t                queryId;
  int64_t                 refId;
  uint64_t                taskId;
  void                   *transport;
} SSchTaskCallbackParam;

typedef struct SSchHbCallbackParam {
  SSchCallbackParamHeader head;
  SQueryNodeEpId          nodeEpId;
  void                   *pTrans;
} SSchHbCallbackParam;

typedef struct SSchFlowControl {
  SRWLatch  lock;
  bool      sorted;
  int32_t   tableNumSum;
  uint32_t  execTaskNum;
  SArray   *taskList;      // Element is SSchTask*
} SSchFlowControl;

typedef struct SSchNodeInfo {
  SQueryNodeAddr addr;
  void          *handle;
} SSchNodeInfo;

typedef struct SSchLevel {
  int32_t         level;
  int8_t          status;
  SRWLatch        lock;
  int32_t         taskFailed;
  int32_t         taskSucceed;
  int32_t         taskNum;
  int32_t         taskLaunchedNum;
  int32_t         taskDoneNum;
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
  SArray              *execNodes;      // all tried node for current task, element is SSchNodeInfo
  SQueryProfileSummary summary;        // task execution summary
  int32_t              childReady;     // child task ready number
  SArray              *children;       // the datasource tasks,from which to fetch the result, element is SQueryTask*
  SArray              *parents;        // the data destination tasks, get data from current task, element is SQueryTask*
  void*                handle;          // task send handle 
} SSchTask;

typedef struct SSchJobAttr {
  EExplainMode explainMode;
  bool         syncSchedule;
  bool         queryJob;
  bool         needFlowCtrl;
} SSchJobAttr;

typedef struct SSchJob {
  int64_t          refId;
  uint64_t         queryId;
  SSchJobAttr      attr;
  int32_t          levelNum;
  int32_t          taskNum;
  void            *pTrans;
  SArray          *nodeList;   // qnode/vnode list, SArray<SQueryNodeAddr>
  SArray          *levels;    // starting from 0. SArray<SSchLevel>
  SNodeList       *subPlans;  // subplan pointer copied from DAG, no need to free it in scheduler

  SArray          *dataSrcTasks; // SArray<SQueryTask*>
  int32_t          levelIdx;
  SEpSet           dataSrcEps;
  SHashObj        *execTasks; // executing tasks, key:taskid, value:SQueryTask*
  SHashObj        *succTasks; // succeed tasks, key:taskid, value:SQueryTask*
  SHashObj        *failTasks; // failed tasks, key:taskid, value:SQueryTask*
  SHashObj        *flowCtrl;  // key is ep, element is SSchFlowControl

  SExplainCtx     *explainCtx;
  int8_t           status;  
  SQueryNodeAddr   resNode;
  tsem_t           rspSem;
  int8_t           userFetch;
  int32_t          remoteFetch;
  SSchTask        *fetchTask;
  int32_t          errCode;
  SRWLatch         resLock;
  void            *queryRes;
  void            *resData;         //TODO free it or not
  int32_t          resNumOfRows;
  SSchResInfo      userRes;
  const char      *sql;
  int32_t          userCb;
  SQueryProfileSummary summary;
} SSchJob;

extern SSchedulerMgmt schMgmt;

#define SCH_TASK_READY_FOR_LAUNCH(readyNum, task) ((readyNum) >= taosArrayGetSize((task)->children))

#define SCH_TASK_ID(_task) ((_task) ? (_task)->taskId : -1)
#define SCH_SET_TASK_LASTMSG_TYPE(_task, _type) do { if(_task) { atomic_store_32(&(_task)->lastMsgType, _type); } } while (0)
#define SCH_GET_TASK_LASTMSG_TYPE(_task) ((_task) ? atomic_load_32(&(_task)->lastMsgType) : -1)

#define SCH_IS_DATA_SRC_QRY_TASK(task) ((task)->plan->subplanType == SUBPLAN_TYPE_SCAN)
#define SCH_IS_DATA_SRC_TASK(task) (((task)->plan->subplanType == SUBPLAN_TYPE_SCAN) || ((task)->plan->subplanType == SUBPLAN_TYPE_MODIFY))
#define SCH_IS_LEAF_TASK(_job, _task) (((_task)->level->level + 1) == (_job)->levelNum)

#define SCH_SET_TASK_STATUS(task, st) atomic_store_8(&(task)->status, st)
#define SCH_GET_TASK_STATUS(task) atomic_load_8(&(task)->status)
#define SCH_GET_TASK_STATUS_STR(task) jobTaskStatusStr(SCH_GET_TASK_STATUS(task))

#define SCH_GET_TASK_HANDLE(_task) ((_task) ? (_task)->handle : NULL)
#define SCH_SET_TASK_HANDLE(_task, _handle) ((_task)->handle = (_handle))

#define SCH_SET_JOB_STATUS(job, st) atomic_store_8(&(job)->status, st)
#define SCH_GET_JOB_STATUS(job) atomic_load_8(&(job)->status)
#define SCH_GET_JOB_STATUS_STR(job) jobTaskStatusStr(SCH_GET_JOB_STATUS(job))

#define SCH_SET_JOB_NEED_FLOW_CTRL(_job) (_job)->attr.needFlowCtrl = true
#define SCH_JOB_NEED_FLOW_CTRL(_job) ((_job)->attr.needFlowCtrl)
#define SCH_TASK_NEED_FLOW_CTRL(_job, _task) (SCH_IS_DATA_SRC_QRY_TASK(_task) && SCH_JOB_NEED_FLOW_CTRL(_job) && SCH_IS_LEVEL_UNFINISHED((_task)->level))

#define SCH_SET_JOB_TYPE(_job, type) (_job)->attr.queryJob = ((type) != SUBPLAN_TYPE_MODIFY)
#define SCH_IS_QUERY_JOB(_job) ((_job)->attr.queryJob) 
#define SCH_JOB_NEED_FETCH(_job) SCH_IS_QUERY_JOB(_job)
#define SCH_IS_WAIT_ALL_JOB(_job) (!SCH_IS_QUERY_JOB(_job))
#define SCH_IS_NEED_DROP_JOB(_job) (SCH_IS_QUERY_JOB(_job))
#define SCH_IS_EXPLAIN_JOB(_job) (EXPLAIN_MODE_ANALYZE == (_job)->attr.explainMode)

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
#define SCH_TASK_DLOGL(param, ...) \
  qDebugL("QID:0x%" PRIx64 ",TID:0x%" PRIx64 " " param, pJob->queryId, SCH_TASK_ID(pTask), __VA_ARGS__)
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
void schFreeFlowCtrl(SSchJob *pJob);
int32_t schChkJobNeedFlowCtrl(SSchJob *pJob, SSchLevel *pLevel);
int32_t schDecTaskFlowQuota(SSchJob *pJob, SSchTask *pTask);
int32_t schCheckIncTaskFlowQuota(SSchJob *pJob, SSchTask *pTask, bool *enough);
int32_t schLaunchTasksInFlowCtrlList(SSchJob *pJob, SSchTask *pTask);
int32_t schLaunchTaskImpl(SSchJob *pJob, SSchTask *pTask);
int32_t schFetchFromRemote(SSchJob *pJob);
int32_t schProcessOnTaskFailure(SSchJob *pJob, SSchTask *pTask, int32_t errCode);
int32_t schBuildAndSendHbMsg(SQueryNodeEpId *nodeEpId);
int32_t schCloneSMsgSendInfo(void *src, void **dst);
int32_t schValidateAndBuildJob(SQueryPlan *pDag, SSchJob *pJob);
void schFreeJobImpl(void *job);
int32_t schMakeHbCallbackParam(SSchJob *pJob, SSchTask *pTask, void **pParam);
int32_t schMakeHbRpcCtx(SSchJob *pJob, SSchTask *pTask, SRpcCtx *pCtx);
int32_t schEnsureHbConnection(SSchJob *pJob, SSchTask *pTask);
int32_t schUpdateHbConnection(SQueryNodeEpId *epId, SSchTrans *trans);
int32_t schHandleHbCallback(void *param, const SDataBuf *pMsg, int32_t code);
void schFreeRpcCtx(SRpcCtx *pCtx);
int32_t schGetCallbackFp(int32_t msgType, __async_send_cb_fn_t *fp);
bool schJobNeedToStop(SSchJob *pJob, int8_t *pStatus);
int32_t schProcessOnTaskSuccess(SSchJob *pJob, SSchTask *pTask);
int32_t schSaveJobQueryRes(SSchJob *pJob, SQueryTableRsp *rsp);
int32_t schProcessOnExplainDone(SSchJob *pJob, SSchTask *pTask, SRetrieveTableRsp *pRsp);
void schProcessOnDataFetched(SSchJob *job);
int32_t schGetTaskFromTaskList(SHashObj *pTaskList, uint64_t taskId, SSchTask **pTask);
int32_t schUpdateTaskExecNodeHandle(SSchTask *pTask, void *handle, int32_t rspCode);
void schFreeRpcCtxVal(const void *arg);
int32_t schMakeBrokenLinkVal(SSchJob *pJob, SSchTask *pTask, SRpcBrokenlinkVal *brokenVal, bool isHb);
int32_t schRecordTaskExecNode(SSchJob *pJob, SSchTask *pTask, SQueryNodeAddr *addr, void *handle);
int32_t schExecStaticExplainJob(void *pTrans, SArray *pNodeList, SQueryPlan *pDag, int64_t *job, const char *sql,
                             SSchResInfo *pRes, bool sync);
int32_t schExecJobImpl(void *pTrans, SArray *pNodeList, SQueryPlan *pDag, int64_t *job, const char *sql,
                              SSchResInfo *pRes, int64_t startTs, bool sync);
int32_t schChkUpdateJobStatus(SSchJob *pJob, int8_t newStatus);
int32_t schCancelJob(SSchJob *pJob);
int32_t schProcessOnJobDropped(SSchJob *pJob, int32_t errCode);
uint64_t schGenTaskId(void);
void schCloseJobRef(void);
int32_t schExecJob(void *pTrans, SArray *pNodeList, SQueryPlan *pDag, int64_t *pJob, const char *sql,
                         int64_t startTs, SSchResInfo *pRes);
int32_t schAsyncExecJob(void *pTrans, SArray *pNodeList, SQueryPlan *pDag, int64_t *pJob, const char *sql,
                                int64_t startTs, SSchResInfo *pRes);
int32_t schFetchRows(SSchJob *pJob);
int32_t schAsyncFetchRows(SSchJob *pJob);


#ifdef __cplusplus
}
#endif

#endif /*_TD_SCHEDULER_INT_H_*/
