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

#ifndef _TD_MND_STREAM_H_
#define _TD_MND_STREAM_H_
#include "stream.h"
#include "mndInt.h"
#include "mndTrans.h"

#ifdef __cplusplus
extern "C" {
#endif

bool mstEventPassIsolation(int32_t num, int32_t event);
bool mstEventHandledChkSet(int32_t event);

typedef enum {
  STM_ERR_TASK_NOT_EXISTS = 1,
  STM_ERR_STREAM_STOPPED,
  STM_ERR_PROCESSING_ERR,
} EStmErrType;


typedef enum {
  STM_EVENT_ACTIVE_BEGIN = 0,
  STM_EVENT_NORMAL_BEGIN,
  STM_EVENT_CREATE_STREAM,
  STM_EVENT_DROP_STREAM,
  STM_EVENT_STOP_STREAM,
  STM_EVENT_START_STREAM,
  STM_EVENT_CREATE_SNODE,
  STM_EVENT_DROP_SNODE,
  STM_EVENT_LOOP_SDB,
  STM_EVENT_LOOP_MAP,
  STM_EVENT_LOOP_SNODE,
  STM_EVENT_STM_TERR,
  STM_EVENT_MAX_VALUE
} SStmLastEvent;

static const char* gMndStreamEvent[] = {"ACTIVE_BEGIN", "NORMAL_BEGIN", "CREATE_STREAM", "DROP_STREAM", "STOP_STREAM", "START_STREAM",
    "CREATE_SNODE", "DROP_SNODE", "LOOP_SDB", "LOOP_MAP", "LOOP_SNODE", "STREAM_TERR", "MAX_VALUE"};

#define MND_STM_STATE_WATCH   1
#define MND_STM_STATE_NORMAL  2

static const char* gMndStreamState[] = {"X", "W", "N"};


#define MND_STREAM_RUNNER_DEPLOY_NUM 3
#define MND_STREAM_RUNNER_REPLICA_NUM 5
#define MND_STREAM_ISOLATION_PERIOD_NUM 10
#define MND_STREAM_REPORT_PERIOD  (STREAM_HB_INTERVAL_MS * STREAM_MAX_GROUP_NUM)
#define MST_SHORT_ISOLATION_DURATION (MND_STREAM_REPORT_PERIOD * MND_STREAM_ISOLATION_PERIOD_NUM / 3)
#define MST_ISOLATION_DURATION (MND_STREAM_REPORT_PERIOD * MND_STREAM_ISOLATION_PERIOD_NUM)
#define MND_STREAM_HEALTH_CHECK_PERIOD_SEC (MND_STREAM_REPORT_PERIOD / 1000)
#define MST_MAX_RETRY_DURATION (MST_ISOLATION_DURATION * 20)

#define MST_PASS_ISOLATION(_ts, _n) (((_ts) + (_n) * MST_ISOLATION_DURATION) <= mStreamMgmt.hCtx.currentTs)
#define MST_PASS_SHORT_ISOLATION(_ts, _n) (((_ts) + (_n) * MST_SHORT_ISOLATION_DURATION) <= mStreamMgmt.hCtx.currentTs)

#define MST_STM_STATIC_PASS_SHORT_ISOLATION(_s) (MST_PASS_SHORT_ISOLATION((_s)->updateTime, 1))

#define MST_STM_STATIC_PASS_ISOLATION(_s) (MST_PASS_ISOLATION((_s)->updateTime, 1))
#define MST_STM_PROC_PASS_ISOLATION(_d) (MST_PASS_ISOLATION((_d)->lastActionTs, 1))
#define MST_STM_PASS_ISOLATION(_s, _d) (MST_STM_STATIC_PASS_ISOLATION(_s) && MST_STM_PROC_PASS_ISOLATION(_d))

#define MST_EVENT_HANDLED_CHECK_SET(_ev) (0 == atomic_val_compare_exchange_8((int8_t*)&mStreamMgmt.lastTs[(_ev)].handled, 0, 1))
#define MST_NORMAL_STATUS_NEED_HANDLE() (mstEventPassIsolation(1, STM_EVENT_NORMAL_BEGIN) && mstEventHandledChkSet(STM_EVENT_NORMAL_BEGIN))
#define MST_USER_OP_NEED_HANDLE(_op) (mstEventPassIsolation(5, (_op)) && mstEventHandledChkSet(_op))
#define MST_CREATE_START_STM_NEED_HANDLE() (MST_USER_OP_NEED_HANDLE(STM_EVENT_CREATE_STREAM) || MST_USER_OP_NEED_HANDLE(STM_EVENT_START_STREAM))
#define MST_DROP_STOP_STM_NEED_HANDLE() (MST_USER_OP_NEED_HANDLE(STM_EVENT_DROP_STREAM) || MST_USER_OP_NEED_HANDLE(STM_EVENT_STOP_STREAM))
#define MST_OP_TIMEOUT_NEED_HANDLE(_op) (mstEventPassIsolation(100, (_op)) && mstEventHandledChkSet(_op))
#define MST_STREAM_ERROR_NEED_HANDLE() (mstEventPassIsolation(1, STM_EVENT_STM_TERR) && mstEventHandledChkSet(STM_EVENT_STM_TERR))
#define MST_READY_FOR_SDB_LOOP() (MST_NORMAL_STATUS_NEED_HANDLE() || MST_CREATE_START_STM_NEED_HANDLE() || MST_DROP_STOP_STM_NEED_HANDLE() || MST_OP_TIMEOUT_NEED_HANDLE(STM_EVENT_LOOP_SDB))
#define MST_READY_FOR_MAP_LOOP() (MST_STREAM_ERROR_NEED_HANDLE() || MST_DROP_STOP_STM_NEED_HANDLE() || MST_OP_TIMEOUT_NEED_HANDLE(STM_EVENT_LOOP_MAP))
#define MST_READY_FOR_SNODE_LOOP() (MST_USER_OP_NEED_HANDLE(STM_EVENT_DROP_SNODE) || MST_OP_TIMEOUT_NEED_HANDLE(STM_EVENT_LOOP_SNODE))

#define STREAM_RUNNER_MAX_DEPLOYS 
#define STREAM_RUNNER_MAX_REPLICA 

#define STREAM_ACT_DEPLOY     (1 << 0)
#define STREAM_ACT_UNDEPLOY   (1 << 1)
#define STREAM_ACT_START      (1 << 2)
#define STREAM_ACT_UPDATE_TRIGGER (1 << 3)
#define STREAM_ACT_RECALC     (1 << 4)

#define MND_STREAM_RESERVE_SIZE      64
#define MND_STREAM_VER_NUMBER        8
#define MND_STREAM_COMPATIBLE_VER_NUMBER 7
#define MND_STREAM_TRIGGER_NAME_SIZE 20
#define MND_STREAM_DEFAULT_NUM       100
#define MND_STREAM_DEFAULT_TASK_NUM  200

#define MND_SET_RUNNER_TASKIDX(_level, _idx) (((_level) << 16) & (_idx))

#define MND_GET_RUNNER_SUBPLANID(_id) ((_id) &0xFFFFFFFF)

#define MND_STREAM_CREATE_NAME       "stream-create"
#define MND_STREAM_START_NAME        "stream-start"
#define MND_STREAM_DROP_NAME         "stream-drop"
#define MND_STREAM_STOP_NAME         "stream-stop"
#define MND_STREAM_RECALC_NAME       "stream-recalc"

#define GOT_SNODE(_snodeId) ((_snodeId) != 0)
#define STREAM_IS_RUNNING(_ss) ((_ss)->triggerTask && STREAM_STATUS_RUNNING == (_ss)->triggerTask->status)
#define STREAM_IS_VIRTUAL_TABLE(_type, _flags) (TSDB_VIRTUAL_CHILD_TABLE == (_type) || TSDB_VIRTUAL_NORMAL_TABLE == (_type) || (TSDB_SUPER_TABLE == (_type) && BIT_FLAG_TEST_MASK((_flags), CREATE_STREAM_FLAG_TRIGGER_VIRTUAL_STB)))
#define MST_IS_RUNNER_GETTING_READY(_t) (STREAM_STATUS_INIT == (_t)->status && STREAM_RUNNER_TASK == (_t)->type)

#define MST_COPY_STR(_p) ((_p) ? (taosStrdup(_p)) : NULL)

#define MST_LIST_SIZE(_l) ((_l) ? TD_DLIST_NELES(_l) : 0)

// clang-format off
#define mstFatal(...) do { if (stDebugFlag & DEBUG_FATAL) { taosPrintLog("MSTM FATAL ", DEBUG_FATAL, 255,         __VA_ARGS__); }} while(0)
#define mstError(...) do { if (stDebugFlag & DEBUG_ERROR) { taosPrintLog("MSTM ERROR ", DEBUG_ERROR, 255,         __VA_ARGS__); }} while(0)
#define mstWarn(...)  do { if (stDebugFlag & DEBUG_WARN)  { taosPrintLog("MSTM WARN  ", DEBUG_WARN,  255,         __VA_ARGS__); }} while(0)
#define mstInfo(...)  do { if (stDebugFlag & DEBUG_INFO)  { taosPrintLog("MSTM INFO  ", DEBUG_INFO,  255,         __VA_ARGS__); }} while(0)
#define mstDebug(...) do { if (stDebugFlag & DEBUG_DEBUG) { taosPrintLog("MSTM DEBUG ", DEBUG_DEBUG, stDebugFlag, __VA_ARGS__); }} while(0)
#define mstDebugL(...) do { if (stDebugFlag & DEBUG_DEBUG) { taosPrintLongString("MSTM DEBUG ", DEBUG_DEBUG, stDebugFlag, __VA_ARGS__); }} while(0)
#define mstTrace(...) do { if (stDebugFlag & DEBUG_TRACE) { taosPrintLog("MSTM TRACE ", DEBUG_TRACE, stDebugFlag, __VA_ARGS__); }} while(0)
// clang-format on

#define msttFatal(param, ...)                                                                               \
  mstFatal("%s NODE:%d %" PRIx64 " %s TASK:%" PRIx64 " SID:%" PRId64 " " param, gMndStreamState[mStreamMgmt.state],\
          ((SStreamTask *)pTask)->nodeId, ((SStreamTask *)pTask)->streamId, gStreamTaskTypeStr[((SStreamTask *)pTask)->type],   \
           ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->seriousId, __VA_ARGS__)
#define msttError(param, ...)                                                                               \
  mstError("%s NODE:%d %" PRIx64 " %s TASK:%" PRIx64 " SID:%" PRId64 " " param, gMndStreamState[mStreamMgmt.state],\
          ((SStreamTask *)pTask)->nodeId, ((SStreamTask *)pTask)->streamId, gStreamTaskTypeStr[((SStreamTask *)pTask)->type],   \
           ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->seriousId, __VA_ARGS__)
#define msttWarn(param, ...)                                                                              \
  mstWarn("%s NODE:%d %" PRIx64 " %s TASK:%" PRIx64 " SID:%" PRId64 " " param, gMndStreamState[mStreamMgmt.state],\
          ((SStreamTask *)pTask)->nodeId, ((SStreamTask *)pTask)->streamId, gStreamTaskTypeStr[((SStreamTask *)pTask)->type],   \
           ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->seriousId, __VA_ARGS__)
#define msttInfo(param, ...)                                                                              \
  mstInfo("%s NODE:%d %" PRIx64 " %s TASK:%" PRIx64 " SID:%" PRId64 " " param, gMndStreamState[mStreamMgmt.state],\
          ((SStreamTask *)pTask)->nodeId, ((SStreamTask *)pTask)->streamId, gStreamTaskTypeStr[((SStreamTask *)pTask)->type],   \
           ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->seriousId, __VA_ARGS__)
#define msttDebug(param, ...)                                                                               \
  mstDebug("%s NODE:%d %" PRIx64 " %s TASK:%" PRIx64 " SID:%" PRId64 " " param, gMndStreamState[mStreamMgmt.state],\
          ((SStreamTask *)pTask)->nodeId, ((SStreamTask *)pTask)->streamId, gStreamTaskTypeStr[((SStreamTask *)pTask)->type],   \
           ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->seriousId, __VA_ARGS__)
#define msttDebugL(param, ...)                                                                               \
  mstDebugL("%s NODE:%d %" PRIx64 " %s TASK:%" PRIx64 " SID:%" PRId64 " " param, gMndStreamState[mStreamMgmt.state],\
          ((SStreamTask *)pTask)->nodeId, ((SStreamTask *)pTask)->streamId, gStreamTaskTypeStr[((SStreamTask *)pTask)->type],   \
           ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->seriousId, __VA_ARGS__)
#define msttTrace(param, ...)                                                                               \
  mstTrace("%s NODE:%d %" PRIx64 " %s TASK:%" PRIx64 " SID:%" PRId64 " " param, gMndStreamState[mStreamMgmt.state],\
          ((SStreamTask *)pTask)->nodeId, ((SStreamTask *)pTask)->streamId, gStreamTaskTypeStr[((SStreamTask *)pTask)->type],   \
           ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->seriousId, __VA_ARGS__)

#define mstsFatal(param, ...) mstFatal("%s %" PRIx64 " " param, gMndStreamState[mStreamMgmt.state], streamId, __VA_ARGS__)
#define mstsError(param, ...) mstError("%s %" PRIx64 " " param, gMndStreamState[mStreamMgmt.state], streamId, __VA_ARGS__)
#define mstsWarn(param, ...)  mstWarn("%s %" PRIx64 " " param, gMndStreamState[mStreamMgmt.state], streamId, __VA_ARGS__)
#define mstsInfo(param, ...)  mstInfo("%s %" PRIx64 " " param, gMndStreamState[mStreamMgmt.state], streamId, __VA_ARGS__)
#define mstsDebug(param, ...) mstDebug("%s %" PRIx64 " " param, gMndStreamState[mStreamMgmt.state], streamId, __VA_ARGS__)
#define mstsDebugL(param, ...) mstDebugL("%s %" PRIx64 " " param, gMndStreamState[mStreamMgmt.state], streamId, __VA_ARGS__)
#define mstsTrace(param, ...) mstTrace("%s %" PRIx64 " " param, gMndStreamState[mStreamMgmt.state], streamId, __VA_ARGS__)


typedef struct SStmStreamAction {
  int64_t              streamId;
  char                 streamName[TSDB_STREAM_FNAME_LEN];
  bool                 userAction;
  void*                actionParam;
} SStmStreamAction;


typedef struct SStmTaskId {
  int64_t taskId;      // KEEP IT FIRST
  int32_t deployId;    // only for runner task
  int64_t seriousId;
  int32_t nodeId;
  int32_t taskIdx;
  int64_t uid;
} SStmTaskId;


typedef struct SStmTaskStatus {
  SStmTaskId      id;
  void*           pStream;
  EStreamTaskType type;
  int64_t         flags;
  EStreamStatus   status;
  SRWLatch        detailStatusLock;
  void*           detailStatus;     // SSTriggerRuntimeStatus*, only for trigger task now
  int32_t         errCode;
  int64_t         runningStartTs;
  int64_t         lastUpTs;
} SStmTaskStatus;


typedef struct SStmTaskAction {
  // KEEP IT TOGETHER
  int64_t    streamId; 
  SStmTaskId id;
  // KEEP IT TOGETHER

  // for snode runner redeploy
  int32_t         deployNum;
  int32_t         deployId[MND_STREAM_RUNNER_DEPLOY_NUM];
  //SStmTaskStatus* triggerStatus;
  
  EStreamTaskType type;
  bool            multiRunner;
  
  int64_t         flag;
} SStmTaskAction;

typedef union {
  SStmStreamAction stream;
  SStmTaskAction   task;
} SStmQAction;

typedef struct SStmQNode {
  int32_t              type;
  bool                 streamAct;
  SStmQAction          action;
  void*                next;
} SStmQNode;

typedef struct SStmActionQ {
  bool          stopQueue;
  SRWLatch      lock;
  SStmQNode*    head;
  SStmQNode*    tail;
  uint64_t      qRemainNum;
} SStmActionQ;


typedef struct SVgroupChangeInfo {
  SHashObj *pDBMap;
  SArray   *pUpdateNodeList;  // SArray<SNodeUpdateInfo>
} SVgroupChangeInfo;



typedef struct SStmTaskSrcAddr {
  bool    isFromCache;
  int64_t taskId;
  int32_t vgId;
  int32_t groupId;
  SEpSet  epset;
} SStmTaskSrcAddr;

typedef struct SStmStat {
  int32_t lastError;
  
} SStmStat;

typedef struct SStmStatus {
  // static part
  char*               streamName;
  SCMCreateStreamReq* pCreate;
  
  int32_t           trigReaderNum;
  int32_t           calcReaderNum;
  int32_t           runnerNum;        // task num for one deploy
  int32_t           runnerDeploys;
  int32_t           runnerReplica;

  int8_t            stopped;         // 1:runtime error stopped, 2:user stopped, 3:user dropped, 4:grant expired

  int64_t           deployTimes;
  int64_t           lastActionTs;
  int32_t           fatalError;
  int64_t           fatalRetryDuration;
  int64_t           fatalRetryTs;
  int64_t           fatalRetryTimes;

  SRWLatch          resetLock;
  SArray*           trigReaders;        // SArray<SStmTaskStatus>
  SArray*           trigOReaders;       // SArray<SStmTaskStatus>, virtable table only
  SList*            calcReaders;        // SList<SStmTaskStatus>
  SStmTaskStatus*   triggerTask;
  SArray*           runners[MND_STREAM_RUNNER_DEPLOY_NUM];  // SArray<SStmTaskStatus>

  int8_t            triggerNeedUpdate;
  
  SRWLatch          userRecalcLock;
  SArray*           userRecalcList; // SArray<SStreamRecalcReq>

  SStmStat          stat;
} SStmStatus;

#define MST_IS_USER_STOPPED(_s) (2 == (_s) || 3 == (_s))
#define MST_IS_ERROR_STOPPED(_s) (1 == (_s))
#define MST_IS_GRANT_STOPPED(_s) (4 == (_s))

typedef struct SStmTaskStatusExt{
  int64_t         streamId;
  SStmTaskStatus* status;
} SStmTaskStatusExt;

typedef struct SStmSnodeStreamStatus {
  SStmTaskStatus*  trigger;       
  SArray*          runners[MND_STREAM_RUNNER_DEPLOY_NUM];       // SArray<SStmTaskStatus*>
} SStmSnodeStreamStatus;

typedef struct SStmSnodeStatus {
  int32_t  runnerThreadNum; // runner thread num in snode
  int64_t  lastUpTs;
  SHashObj* streamTasks;   // streamId => SStmSnodeStreamStatus
} SStmSnodeStatus;

typedef struct SStmVgStreamStatus {
  SArray*  trigReaders;       // SArray<SStmTaskStatus*>
  SArray*  calcReaders;       // SArray<SStmTaskStatus*>
} SStmVgStreamStatus;

typedef struct SStmVgroupStatus {
  int64_t   lastUpTs;
  SHashObj* streamTasks;   // streamId => SStmVgStreamStatus
} SStmVgroupStatus;

typedef struct SStmTaskToDeployExt {
  volatile bool   deployed;
  int32_t         deployId;     // only for runner task
  SStmTaskDeploy  deploy;
} SStmTaskToDeployExt;

typedef struct SStmVgTasksToDeploy {
  SRWLatch lock;
  int64_t  streamVer;
  int32_t  deployed;
  SArray*  taskList;       // SArray<SStmTaskToDeployExt>
} SStmVgTasksToDeploy;

typedef struct SStmSnodeTasksDeploy {
  SRWLatch lock;
  int32_t  triggerDeployed;
  int32_t  runnerDeployed;
  SArray*  triggerList;  // SArray<SStmTaskToDeployExt>
  SArray*  runnerList;   // SArray<SStmTaskToDeployExt>
} SStmSnodeTasksDeploy;

typedef struct SStmStreamUndeploy{
  SArray*     taskList;      // SArray<SStreamTask*>
  int8_t      doCheckpoint;
  int8_t      doCleanup;
} SStmStreamUndeploy;

typedef struct SStmStreamStart {
  SStmTaskId      triggerId;
} SStmStreamStart;

typedef struct SStmStreamRecalc {
  SArray*     recalcList;     // SArray<SStreamRecalcReq>
} SStmStreamRecalc;


typedef struct SStmAction {
  int32_t            actions;
  SStmStreamStart    start;
  SStmStreamDeploy   deploy;
  SStmStreamUndeploy undeploy;
  SStmStreamRecalc   recalc;
} SStmAction;

typedef struct SStmGrpCtx {
  SMnode*           pMnode;
  int64_t           currTs; 
  SStreamHbMsg*     pReq;
  SMStreamHbRspMsg* pRsp;

  int32_t           tidx;

  // status update
  int32_t          taskNum;

  // reserved for trigger task deploy
  int64_t          triggerTaskId;
  int32_t          triggerNodeId;
  
  SHashObj*        deployStm;
  SHashObj*        actionStm;
} SStmGrpCtx;

typedef struct SStmThreadCtx {
  SStmGrpCtx       grpCtx[STREAM_MAX_GROUP_NUM];
  SHashObj*        deployStm[STREAM_MAX_GROUP_NUM];    // streamId => SStmStreamDeploy
  SHashObj*        actionStm[STREAM_MAX_GROUP_NUM];    // streamId => SStmAction
} SStmThreadCtx;

typedef struct SStmHealthCheckCtx {
  int32_t   slotIdx;
  int64_t   currentTs;
} SStmHealthCheckCtx;

typedef struct SStmRuntimeStat {
  int64_t  activeTimes;
  int64_t  inactiveTimes;
  
} SStmRuntimeStat;

typedef struct SStmWatchCtx {
  int8_t   ending;
  int8_t   taskRemains;
  int32_t  processing;
} SStmWatchCtx;

typedef struct SStmCheckStatusCtx {
  int32_t code;
  int32_t handledNum;
  int32_t checkedNum;
} SStmCheckStatusCtx;

typedef struct SStmLastTs {
  int64_t ts;
  bool    handled;
} SStmLastTs;

#define MND_STREAM_SET_LAST_TS(_op, _ts) do {        \
  mStreamMgmt.lastTs[(_op)].ts = (_ts);         \
  mStreamMgmt.lastTs[(_op)].handled = false;    \
  mstDebug("update event %s lastTs to %" PRId64, gMndStreamEvent[(_op)], (_ts));   \
} while (0)

#define MND_STREAM_GET_LAST_TS(_op) mStreamMgmt.lastTs[(_op)].ts
#define MND_STREAM_CLR_LAST_TS(_op) mStreamMgmt.lastTs[(_op)].handled = true


typedef struct SStmRuntime {
  int8_t           active;
  int8_t           grantExpired;
  SRWLatch         runtimeLock;

  SStmLastTs       lastTs[STM_EVENT_MAX_VALUE];
  int8_t           state;
  int64_t          lastTaskId;
    
  SRWLatch         actionQLock;
  SStmActionQ*     actionQ;
  
  int32_t           threadNum;
  SStmThreadCtx*    tCtx;

  // ST
  SHashObj*        streamMap;  // streamId => SStmStatus
  SHashObj*        taskMap;    // streamId + taskId => SStmTaskStatus*
  SHashObj*        vgroupMap;  // vgId => SStmVgroupStatus (only reader tasks)
  SHashObj*        snodeMap;   // snodeId => SStmSnodeStatus (only trigger and runner tasks)
  SHashObj*        dnodeMap;   // dnodeId => lastUpTs

  // TD
  int32_t          toDeployVgTaskNum;
  SHashObj*        toDeployVgMap;        // vgId => SStmVgTasksToDeploy (only reader tasks)
  int32_t          toDeploySnodeTaskNum;
  SHashObj*        toDeploySnodeMap;     // snodeId => SStmSnodeTasksDeploy (only trigger and runner tasks)

  // UP
  int32_t          toUpdateScanNum;
  SHashObj*        toUpdateScanMap;   // streamId + subplanId => SStmTaskSrcAddr (only scan's target runner tasks)

  // HEALTH
  SStmHealthCheckCtx hCtx;

  SStmWatchCtx       watch;
  SStmRuntimeStat    stat;
} SStmRuntime;

extern SStmRuntime         mStreamMgmt;

int32_t mndInitStream(SMnode *pMnode);
void    mndCleanupStream(SMnode *pMnode);
int32_t mndAcquireStream(SMnode *pMnode, char *streamName, SStreamObj **pStream);
int32_t mndAcquireStreamById(SMnode *pMnode, int64_t streamId, SStreamObj **pStream);
void    mndReleaseStream(SMnode *pMnode, SStreamObj *pStream);
int32_t mndDropStreamByDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb);

int32_t  mstGetStreamsNumInDb(SMnode *pMnode, char *dbName, int32_t *pNumOfStreams);
int32_t  setTransAction(STrans *pTrans, void *pCont, int32_t contLen, int32_t msgType, const SEpSet *pEpset,
                        int32_t retryCode, int32_t acceptCode);
int32_t  doCreateTrans(SMnode *pMnode, SStreamObj *pStream, SRpcMsg *pReq, ETrnConflct conflict, const char *name,
                       const char *pMsg, STrans **pTrans1);
int32_t  mndPersistTransLog(SStreamObj *pStream, STrans *pTrans, int32_t status);
SSdbRaw *mndStreamActionEncode(SStreamObj *pStream);

int32_t mndGetStreamObj(SMnode *pMnode, int64_t streamId, SStreamObj **pStream);
int32_t mndCheckForSnode(SMnode *pMnode, SDbObj *pSrcDb);

int32_t mndProcessStreamHb(SRpcMsg *pReq);
int32_t mndStreamSetDropAction(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream);
int32_t mndStreamSetDropActionFromList(SMnode *pMnode, STrans *pTrans, SArray *pList);
int32_t mndStreamSetStopStreamTasksActions(SMnode* pMnode, STrans *pTrans, uint64_t dbUid);

int32_t msmInitRuntimeInfo(SMnode *pMnode);
int32_t mndStreamTransAppend(SStreamObj *pStream, STrans *pTrans, int32_t status);
int32_t mndStreamCreateTrans(SMnode *pMnode, SStreamObj *pStream, SRpcMsg *pReq, ETrnConflct conflict, const char *name, STrans **ppTrans);
int32_t mstSetStreamAttrResBlock(SMnode *pMnode, SStreamObj *pStream, SSDataBlock *pBlock, int32_t numOfRows);
int32_t mstSetStreamTasksResBlock(SStreamObj* pStream, SSDataBlock* pBlock, int32_t* numOfRows, int32_t rowsCapacity);
int32_t mstSetStreamRecalculatesResBlock(SStreamObj* pStream, SSDataBlock* pBlock, int32_t* numOfRows, int32_t rowsCapacity);
int32_t mstGetScanUidFromPlan(int64_t streamId, void* scanPlan, int64_t* uid);
int32_t mstAppendNewRecalcRange(int64_t streamId, SStmStatus *pStream, STimeWindow* pRange);
int32_t mstCheckSnodeExists(SMnode *pMnode);
void mstSetTaskStatusFromMsg(SStmGrpCtx* pCtx, SStmTaskStatus* pTask, SStmTaskStatusMsg* pMsg);
void msmClearStreamToDeployMaps(SStreamHbMsg* pHb);
void msmCleanStreamGrpCtx(SStreamHbMsg* pHb);
int32_t msmHandleStreamHbMsg(SMnode* pMnode, int64_t currTs, SStreamHbMsg* pHb, SRpcMsg *pReq, SRpcMsg* pRspMsg);
void msmEncodeStreamHbRsp(int32_t code, SRpcHandleInfo *pRpcInfo, SMStreamHbRspMsg* pRsp, SRpcMsg* pMsg);
int32_t msmHandleGrantExpired(SMnode *pMnode, int32_t errCode);
bool mndStreamActionDequeue(SStmActionQ* pQueue, SStmQNode **param);
void msmHandleBecomeLeader(SMnode *pMnode);
void msmHandleBecomeNotLeader(SMnode *pMnode);
void msmUndeployStream(SMnode* pMnode, int64_t streamId, char* streamName);
int32_t msmRecalcStream(SMnode* pMnode, int64_t streamId, STimeWindow* timeRange);
int32_t mstIsStreamDropped(SMnode *pMnode, int64_t streamId, bool* dropped);
bool mstWaitLock(SRWLatch* pLock, bool readLock);
void msmHealthCheck(SMnode *pMnode);
void mstPostStreamAction(SStmActionQ*       actionQ, int64_t streamId, char* streamName, void* param, bool userAction, int32_t action);
void mstPostTaskAction(SStmActionQ*        actionQ, SStmTaskAction* pAction, int32_t action);
int32_t msmAssignRandomSnodeId(SMnode* pMnode, int64_t streamId);
int32_t msmCheckSnodeReassign(SMnode *pMnode, SSnodeObj* pSnode, SArray** ppRes);
void mstLogSStreamObj(char* tips, SStreamObj* p);
void mstLogSStmStatus(char* tips, int64_t streamId, SStmStatus* p);
void mstDestroySStmVgStreamStatus(void* p);
void mstDestroyVgroupStatus(SStmVgroupStatus* pVgStatus);
void mstDestroySStmSnodeStreamStatus(void* p);
int32_t mstBuildDBVgroupsMap(SMnode* pMnode, SSHashObj** ppRes);
int32_t mstGetTableVgId(SSHashObj* pDbVgroups, char* dbFName, char *tbName, int32_t* vgId);
void mstDestroyDbVgroupsHash(SSHashObj *pDbVgs);
void mndStreamUpdateTagsRefFlag(SMnode *pMnode, int64_t suid, SSchema* pTags, int32_t tagNum);
void mstCheckDbInUse(SMnode *pMnode, char *dbFName, bool *dbStream, bool *vtableStream, bool ignoreCurrDb);
void mstDestroySStmSnodeTasksDeploy(void* param);
void mstResetSStmStatus(SStmStatus* pStatus);
void mstDestroySStmStatus(void* param);
void mstDestroySStmAction(void* param);
void mstClearSStmStreamDeploy(SStmStreamDeploy* pDeploy);
void mstDestroySStmVgroupStatus(void* param);
void mstDestroySStmSnodeStatus(void* param);
void mstDestroySStmVgTasksToDeploy(void* param);
void mstDestroySStmTaskToDeployExt(void* param);
void mstDestroyScanAddrList(void* param);
int32_t msmGetTriggerTaskAddr(SMnode *pMnode, int64_t streamId, SStreamTaskAddr* pAddr);
void msmDestroyRuntimeInfo(SMnode *pMnode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_STREAM_H_*/
