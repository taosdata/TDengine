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

#ifndef TDENGINE_TSTREAM_H
#define TDENGINE_TSTREAM_H

#include "os.h"
#include "streamMsg.h"
#include "streamState.h"
#include "tdatablock.h"
#include "tdbInt.h"
#include "tmsg.h"
#include "tmsgcb.h"
#include "tqueue.h"
#include "ttimer.h"

#ifdef __cplusplus
extern "C" {
#endif

#define ONE_MiB_F       (1048576.0)
#define ONE_KiB_F       (1024.0)
#define SIZE_IN_MiB(_v) ((_v) / ONE_MiB_F)
#define SIZE_IN_KiB(_v) ((_v) / ONE_KiB_F)

#define TASK_DOWNSTREAM_READY      0x0
#define TASK_DOWNSTREAM_NOT_READY  0x1
#define TASK_DOWNSTREAM_NOT_LEADER 0x2
#define TASK_UPSTREAM_NEW_STAGE    0x3

#define NODE_ROLE_UNINIT   0x1
#define NODE_ROLE_LEADER   0x2
#define NODE_ROLE_FOLLOWER 0x3

#define HAS_RELATED_FILLHISTORY_TASK(_t) ((_t)->hTaskInfo.id.taskId != 0)
#define CLEAR_RELATED_FILLHISTORY_TASK(_t) \
  do {                                     \
    (_t)->hTaskInfo.id.taskId = 0;         \
    (_t)->hTaskInfo.id.streamId = 0;       \
  } while (0)

#define STREAM_EXEC_T_EXTRACT_WAL_DATA  (-1)
#define STREAM_EXEC_T_START_ALL_TASKS   (-2)
#define STREAM_EXEC_T_START_ONE_TASK    (-3)
#define STREAM_EXEC_T_RESTART_ALL_TASKS (-4)
#define STREAM_EXEC_T_STOP_ALL_TASKS    (-5)
#define STREAM_EXEC_T_RESUME_TASK       (-6)
#define STREAM_EXEC_T_ADD_FAILED_TASK   (-7)

typedef struct SStreamTask           SStreamTask;
typedef struct SStreamQueue          SStreamQueue;
typedef struct SStreamTaskSM         SStreamTaskSM;
typedef struct SStreamQueueItem      SStreamQueueItem;
typedef struct SActiveCheckpointInfo SActiveCheckpointInfo;

#define SSTREAM_TASK_VER                  4
#define SSTREAM_TASK_INCOMPATIBLE_VER     1
#define SSTREAM_TASK_NEED_CONVERT_VER     2
#define SSTREAM_TASK_SUBTABLE_CHANGED_VER 3

extern int32_t streamMetaRefPool;
extern int32_t streamTaskRefPool;

enum {
  STREAM_STATUS__NORMAL = 0,
  STREAM_STATUS__STOP,
  STREAM_STATUS__INIT,
  STREAM_STATUS__FAILED,
  STREAM_STATUS__RECOVER,
  STREAM_STATUS__PAUSE,
};

typedef enum ETaskStatus {
  TASK_STATUS__READY = 0,
  TASK_STATUS__DROPPING,
  TASK_STATUS__UNINIT,  // not used, an placeholder
  TASK_STATUS__STOP,
  TASK_STATUS__SCAN_HISTORY,  // stream task scan history data by using tsdbread in the stream scanner
  TASK_STATUS__HALT,          // pause, but not be manipulated by user command
  TASK_STATUS__PAUSE,         // pause
  TASK_STATUS__CK,            // stream task is in checkpoint status, no data are allowed to put into inputQ anymore
} ETaskStatus;

enum {
  TASK_SCHED_STATUS__INACTIVE = 1,
  TASK_SCHED_STATUS__WAITING,
  TASK_SCHED_STATUS__ACTIVE,
  TASK_SCHED_STATUS__DROPPING,
};

enum {
  TASK_INPUT_STATUS__NORMAL = 1,
  TASK_INPUT_STATUS__BLOCKED,
  TASK_INPUT_STATUS__FAILED,
  TASK_INPUT_STATUS__REFUSED,
};

enum {
  TASK_OUTPUT_STATUS__NORMAL = 1,
  TASK_OUTPUT_STATUS__WAIT,
};

enum {
  TASK_TRIGGER_STATUS__INACTIVE = 1,
  TASK_TRIGGER_STATUS__MAY_ACTIVE,
};

typedef enum {
  TASK_LEVEL__SOURCE = 1,
  TASK_LEVEL__AGG,
  TASK_LEVEL__SINK,
  TASK_LEVEL_SMA,
} ETASK_LEVEL;

enum {
  TASK_OUTPUT__FIXED_DISPATCH = 1,
  TASK_OUTPUT__SHUFFLE_DISPATCH,
  TASK_OUTPUT__TABLE,
  TASK_OUTPUT__SMA,
  TASK_OUTPUT__FETCH,
};

enum {
  STREAM_QUEUE__SUCESS = 1,
  STREAM_QUEUE__FAILED,
  STREAM_QUEUE__PROCESSING,
};

typedef enum EStreamTaskEvent {
  TASK_EVENT_INIT = 0x1,
  TASK_EVENT_INIT_SCANHIST = 0x2,
  TASK_EVENT_SCANHIST_DONE = 0x3,
  TASK_EVENT_STOP = 0x4,
  TASK_EVENT_GEN_CHECKPOINT = 0x5,
  TASK_EVENT_CHECKPOINT_DONE = 0x6,
  TASK_EVENT_PAUSE = 0x7,
  TASK_EVENT_RESUME = 0x8,
  TASK_EVENT_HALT = 0x9,
  TASK_EVENT_DROPPING = 0xA,
} EStreamTaskEvent;

typedef void    FTbSink(SStreamTask* pTask, void* vnode, void* data);
typedef void    FSmaSink(void* vnode, int64_t smaId, const SArray* data);
typedef int32_t FTaskBuild(void* ahandle, SStreamTask* pTask, int64_t ver);
typedef int32_t FTaskExpand(SStreamTask* pTask);

typedef struct {
  int8_t      type;
  int64_t     ver;
  SPackedData submit;
} SStreamDataSubmit;

typedef struct {
  int8_t  type;
  int64_t ver;
  SArray* submits;  // SArray<SPackedSubmit>
} SStreamMergedSubmit;

typedef struct {
  int8_t  type;
  int64_t nodeId;  // nodeId, from SStreamMeta
  int32_t srcVgId;
  int32_t srcTaskId;
  int32_t childId;
  int64_t sourceVer;
  int64_t reqId;
  SArray* blocks;  // SArray<SSDataBlock>
} SStreamDataBlock;

// ref data block, for delete
typedef struct {
  int8_t       type;
  SSDataBlock* pBlock;
} SStreamRefDataBlock;

int32_t streamDataSubmitNew(SPackedData* pData, int32_t type, SStreamDataSubmit** pSubmit);
void    streamDataSubmitDestroy(SStreamDataSubmit* pDataSubmit);

typedef struct {
  char*              qmsg;
  void*              pExecutor;   // not applicable to encoder and decoder
  struct SWalReader* pWalReader;  // not applicable to encoder and decoder
} STaskExec;

typedef struct {
  int32_t taskId;
  int32_t nodeId;
  SEpSet  epSet;
} STaskDispatcherFixed;

typedef struct {
  char      stbFullName[TSDB_TABLE_FNAME_LEN];
  SUseDbRsp dbInfo;
} STaskDispatcherShuffle;

typedef struct {
  int32_t nodeId;
  SEpSet  epset;
} SDownstreamTaskEpset;

typedef enum {
  TASK_SCANHISTORY_CONT = 0x1,
  TASK_SCANHISTORY_QUIT = 0x2,
  TASK_SCANHISTORY_REXEC = 0x3,
} EScanHistoryCode;

typedef struct {
  EScanHistoryCode ret;
  int32_t          idleTime;
} SScanhistoryDataInfo;

typedef struct {
  int32_t idleDuration;  // idle time before use time slice the continue execute scan-history
  int32_t numOfTicks;
  tmr_h   pTimer;
  int32_t execCount;
} SScanhistorySchedInfo;

typedef struct {
  int64_t         stbUid;
  char            stbFullName[TSDB_TABLE_FNAME_LEN];
  SSchemaWrapper* pSchemaWrapper;
  SSchemaWrapper* pTagSchema;
  bool            autoCreateCtb;
  void*           vnode;  // not available to encoder and decoder
  FTbSink*        tbSinkFunc;
  STSchema*       pTSchema;
  SSHashObj*      pTbInfo;
} STaskSinkTb;

typedef struct {
  int64_t smaId;
  // following are not applicable to encoder and decoder
  void*     vnode;
  FSmaSink* smaSink;
} STaskSinkSma;

typedef struct {
  int8_t reserved;
} STaskSinkFetch;

typedef struct STaskId {
  int64_t streamId;
  int64_t taskId;
} STaskId;

typedef struct SStreamTaskId {
  int64_t     streamId;
  int32_t     taskId;
  int64_t     refId;
  const char* idStr;
} SStreamTaskId;

typedef struct SCheckpointInfo {
  int64_t                startTs;
  int64_t                checkpointId;    // latest checkpoint id
  int64_t                checkpointVer;   // latest checkpoint offset in wal
  int64_t                checkpointTime;  // latest checkpoint time
  int64_t                processedVer;
  int64_t                nextProcessVer;  // current offset in WAL, not serialize it
  int64_t                msgVer;
  SActiveCheckpointInfo* pActiveInfo;
} SCheckpointInfo;

typedef enum {
  TASK_CONSEN_CHKPT_REQ = 0x1,
  TASK_CONSEN_CHKPT_SEND = 0x2,
  TASK_CONSEN_CHKPT_RECV = 0x3,
} EConsenChkptStatus;

typedef struct SConsenChkptInfo {
  EConsenChkptStatus status;
  int64_t            statusTs;
  int32_t            consenChkptTransId;
} SConsenChkptInfo;

typedef struct SStreamStatus {
  SStreamTaskSM*   pSM;
  int8_t           taskStatus;
  int8_t           downstreamReady;  // downstream tasks are all ready now, if this flag is set
  int8_t           schedStatus;
  int8_t           statusBackup;
  int32_t          schedIdleTime;  // idle time before invoke again
  int64_t          lastExecTs;     // last exec time stamp
  int32_t          inScanHistorySentinel;
  bool             appendTranstateBlock;  // has appended the transfer state data block already
  bool             removeBackendFiles;    // remove backend files on disk when free stream tasks
  SConsenChkptInfo consenChkptInfo;
  STimeWindow      latestForceWindow;     // latest generated time window, only valid in
} SStreamStatus;

typedef struct SDataRange {
  SVersionRange range;
  STimeWindow   window;
} SDataRange;

typedef struct SSTaskBasicInfo {
  int32_t   nodeId;  // vgroup id or snode id
  SEpSet    epSet;
  SEpSet    mnodeEpset;  // mnode epset for send heartbeat
  int32_t   selfChildId;
  int32_t   trigger;
  int8_t    taskLevel;
  int8_t    fillHistory;      // is fill history task or not
  int64_t   delaySchedParam;  // in msec
  int64_t   watermark;        // extracted from operators
  SInterval interval;
} SSTaskBasicInfo;

typedef struct SStreamRetrieveReq SStreamRetrieveReq;
typedef struct SStreamDispatchReq SStreamDispatchReq;
typedef struct STokenBucket       STokenBucket;
typedef struct SMetaHbInfo        SMetaHbInfo;

typedef struct SDispatchMsgInfo {
  SStreamDispatchReq* pData;  // current dispatch data

  int8_t        dispatchMsgType;
  int64_t       checkpointId;  // checkpoint id msg
  int32_t       transId;       // transId for current checkpoint
  int16_t       msgType;       // dispatch msg type
  int32_t       msgId;
  int64_t       startTs;    // dispatch start time, record total elapsed time for dispatch
  int64_t       rspTs;      // latest rsp time
  void*         pRetryTmr;  // used to dispatch data after a given time duration
  TdThreadMutex lock;
  int8_t        inMonitor;
  SArray*       pSendInfo;  //  SArray<SDispatchEntry>
} SDispatchMsgInfo;

typedef struct STaskQueue {
  int8_t        status;
  SStreamQueue* queue;
} STaskQueue;

typedef struct STaskSchedInfo {
  int8_t status;
  tmr_h  pDelayTimer;
  tmr_h  pIdleTimer;
} STaskSchedInfo;

typedef struct SSinkRecorder {
  int64_t numOfSubmit;
  int64_t numOfBlocks;
  int64_t numOfRows;
  int64_t dataSize;
} SSinkRecorder;

typedef struct STaskExecStatisInfo {
  int64_t created;
  int64_t checkTs;
  int64_t readyTs;
  int64_t startCheckpointId;
  int64_t startCheckpointVer;

  int64_t       step1Start;
  double        step1El;
  int64_t       step2Start;
  double        step2El;
  int32_t       updateCount;
  int64_t       latestUpdateTs;
  int32_t       inputDataBlocks;
  int64_t       inputDataSize;
  double        procsThroughput;
  int64_t       outputDataBlocks;
  int64_t       outputDataSize;
  double        outputThroughput;
  int32_t       dispatch;
  int64_t       dispatchDataSize;
  int32_t       checkpoint;
  SSinkRecorder sink;
} STaskExecStatisInfo;

typedef struct SHistoryTaskInfo {
  STaskId id;
  void*   pTimer;
  int32_t tickCount;
  int32_t retryTimes;
  int32_t waitInterval;
  int64_t haltVer;       // offset in wal when halt the stream task
  bool    operatorOpen;  // false by default
} SHistoryTaskInfo;

typedef struct STaskOutputInfo {
  union {
    STaskDispatcherFixed   fixedDispatcher;
    STaskDispatcherShuffle shuffleDispatcher;

    STaskSinkTb    tbSink;
    STaskSinkSma   smaSink;
    STaskSinkFetch fetchSink;
  };
  int8_t        type;
  STokenBucket* pTokenBucket;
  SArray*       pNodeEpsetUpdateList;
} STaskOutputInfo;

typedef struct SUpstreamInfo {
  SArray* pList;
  int32_t numOfClosed;
} SUpstreamInfo;

typedef struct SDownstreamStatusInfo {
  int64_t reqId;
  int32_t taskId;
  int32_t vgId;
  int64_t rspTs;
  int32_t status;
} SDownstreamStatusInfo;

typedef struct STaskCheckInfo {
  SArray*       pList;
  int64_t       startTs;
  int64_t       timeoutStartTs;
  int32_t       notReadyTasks;
  int32_t       inCheckProcess;
  int32_t       stopCheckProcess;
  int32_t       notReadyRetryCount;
  int32_t       timeoutRetryCount;
  tmr_h         checkRspTmr;
  TdThreadMutex checkInfoLock;
} STaskCheckInfo;

struct SStreamTask {
  int64_t             ver;
  SStreamTaskId       id;
  SSTaskBasicInfo     info;
  STaskQueue          outputq;
  STaskQueue          inputq;
  STaskSchedInfo      schedInfo;
  STaskOutputInfo     outputInfo;
  SDispatchMsgInfo    msgInfo;
  SStreamStatus       status;
  SCheckpointInfo     chkInfo;
  STaskExec           exec;
  SDataRange          dataRange;
  SVersionRange       step2Range;  // version range used to scan wal, information in dataRange should not modified.
  SHistoryTaskInfo    hTaskInfo;
  STaskId             streamTaskId;
  STaskExecStatisInfo execInfo;
  TdThreadMutex       lock;    // secure the operation of set task status and puting data into inputQ
  SMsgCb*             pMsgCb;  // msg handle
  SStreamState*       pState;  // state backend
  SUpstreamInfo       upstreamInfo;
  STaskCheckInfo      taskCheckInfo;

  // the followings attributes don't be serialized
  SScanhistorySchedInfo schedHistoryInfo;
  int32_t               transferStateAlignCnt;
  struct SStreamMeta*   pMeta;
  SSHashObj*            pNameMap;
  void*                 pBackend;
  int8_t                subtableWithoutMd5;
  char                  reserve[256];
  char*                 backendPath;
};

typedef int32_t (*startComplete_fn_t)(struct SStreamMeta*);

typedef struct STaskStartInfo {
  int64_t            startTs;
  int64_t            readyTs;
  int32_t            tasksWillRestart;
  int32_t            startAllTasks;   // restart flag, sentinel to guard the restart procedure.
  SHashObj*          pReadyTaskSet;   // tasks that are all ready for running stream processing
  SHashObj*          pFailedTaskSet;  // tasks that are done the check downstream process, may be successful or failed
  int64_t            elapsedTime;
  int32_t            restartCount;  // restart task counter
  startComplete_fn_t completeFn;    // complete callback function
} STaskStartInfo;

typedef struct STaskUpdateInfo {
  SHashObj* pTasks;
  int32_t   activeTransId;
  int32_t   completeTransId;
  int64_t   completeTs;
} STaskUpdateInfo;

typedef struct SScanWalInfo {
  int32_t scanCounter;
  tmr_h   scanTimer;
} SScanWalInfo;

typedef struct SFatalErrInfo {
  int32_t code;
  int64_t ts;
  int32_t threadId;
  int32_t line;
  char    func[128];
} SFatalErrInfo;

// meta
typedef struct SStreamMeta {
  char*           path;
  TDB*            db;
  TTB*            pTaskDb;
  TTB*            pCheckpointDb;
  SHashObj*       pTasksMap;
  SArray*         pTaskList;  // SArray<STaskId*>
  void*           ahandle;
  TXN*            txn;
  FTaskBuild*     buildTaskFn;
  FTaskExpand*    expandTaskFn;
  int32_t         vgId;
  int64_t         stage;
  int32_t         role;
  bool            closeFlag;
  bool            sendMsgBeforeClosing;  // send hb to mnode before close all tasks when switch to follower.
  STaskStartInfo  startInfo;
  TdThreadRwlock  lock;
  SScanWalInfo    scanInfo;
  void*           streamBackend;
  int64_t         streamBackendRid;
  SHashObj*       pTaskDbUnique;
  TdThreadMutex   backendMutex;
  SMetaHbInfo*    pHbInfo;
  STaskUpdateInfo updateInfo;
  int32_t         numOfStreamTasks;  // this value should be increased when a new task is added into the meta
  int32_t         numOfPausedTasks;
  int64_t         rid;
  SFatalErrInfo   fatalInfo;  // fatal error occurs, stream stop to execute
  int64_t         chkpId;
  int32_t         chkpCap;
  SArray*         chkpSaved;
  SArray*         chkpInUse;
  SRWLatch        chkpDirLock;
  void*           bkdChkptMgt;
} SStreamMeta;

typedef struct STaskUpdateEntry {
  int64_t streamId;
  int32_t taskId;
  int32_t transId;
} STaskUpdateEntry;

typedef int32_t (*__state_trans_user_fn)(SStreamTask*, void* param);

int32_t tNewStreamTask(int64_t streamId, int8_t taskLevel, SEpSet* pEpset, bool fillHistory, int32_t trigger,
                       int64_t triggerParam, SArray* pTaskList, bool hasFillhistory, int8_t subtableWithoutMd5,
                       SStreamTask** pTask);
void    tFreeStreamTask(void* pTask);
int32_t tEncodeStreamTask(SEncoder* pEncoder, const SStreamTask* pTask);
int32_t tDecodeStreamTask(SDecoder* pDecoder, SStreamTask* pTask);
int32_t streamTaskInit(SStreamTask* pTask, SStreamMeta* pMeta, SMsgCb* pMsgCb, int64_t ver);
void    streamFreeTaskState(SStreamTask* pTask, int8_t remove);

int32_t tDecodeStreamTaskChkInfo(SDecoder* pDecoder, SCheckpointInfo* pChkpInfo);
int32_t tDecodeStreamTaskId(SDecoder* pDecoder, STaskId* pTaskId);

// stream task queue related API
int32_t streamTaskPutDataIntoInputQ(SStreamTask* pTask, SStreamQueueItem* pItem);
int32_t streamTaskPutDataIntoOutputQ(SStreamTask* pTask, SStreamDataBlock* pBlock);
int32_t streamTaskPutTranstateIntoInputQ(SStreamTask* pTask);
bool    streamQueueIsFull(const SStreamQueue* pQueue);

typedef struct {
  int64_t streamId;
  int32_t upstreamNodeId;
  int32_t upstreamTaskId;
  int32_t downstreamNodeId;
  int32_t downstreamTaskId;
  int32_t msgId;
  int8_t  inputStatus;
  int64_t stage;
} SStreamDispatchRsp;

typedef struct {
  int64_t streamId;
  int32_t childId;
  int32_t rspFromTaskId;
  int32_t rspToTaskId;
} SStreamRetrieveRsp;

typedef struct {
  SMsgHead msgHead;
  int64_t  streamId;
  int32_t  taskId;
  int8_t   igUntreated;
} SStreamScanHistoryReq;

typedef struct STaskCkptInfo {
  int64_t latestId;          // saved checkpoint id
  int64_t latestVer;         // saved checkpoint ver
  int64_t latestTime;        // latest checkpoint time
  int64_t latestSize;        // latest checkpoint size
  int8_t  remoteBackup;      // latest checkpoint backup done
  int64_t activeId;          // current active checkpoint id
  int32_t activeTransId;     // checkpoint trans id
  int8_t  failed;            // denote if the checkpoint is failed or not
  int8_t  consensusChkptId;  // required the consensus-checkpointId
  int64_t consensusTs;       //
} STaskCkptInfo;

typedef struct STaskStatusEntry {
  STaskId       id;
  int32_t       status;
  int32_t       statusLastDuration;  // to record the last duration of current status
  int64_t       stage;
  int32_t       nodeId;
  SVersionRange verRange;      // start/end version in WAL, only valid for source task
  int64_t       processedVer;  // only valid for source task
  double        inputQUsed;    // in MiB
  double        inputRate;
  double        procsThroughput;   // duration between one element put into input queue and being processed.
  double        procsTotal;        // duration between one element put into input queue and being processed.
  double        outputThroughput;  // the size of dispatched result blocks in bytes
  double        outputTotal;       // the size of dispatched result blocks in bytes
  double        sinkQuota;         // existed quota size for sink task
  double        sinkDataSize;      // sink to dst data size
  int64_t       startTime;
  int64_t       startCheckpointId;
  int64_t       startCheckpointVer;
  int64_t       hTaskId;
  STaskCkptInfo checkpointInfo;
} STaskStatusEntry;

typedef struct SNodeUpdateInfo {
  int32_t nodeId;
  SEpSet  prevEp;
  SEpSet  newEp;
} SNodeUpdateInfo;

typedef struct SStreamTaskState {
  ETaskStatus state;
  char*       name;
} SStreamTaskState;

typedef struct SCheckpointConsensusInfo {
  SArray* pTaskList;
  int32_t numOfTasks;
  int64_t streamId;
} SCheckpointConsensusInfo;

void    streamSetupScheduleTrigger(SStreamTask* pTask);

// dispatch related
int32_t streamProcessDispatchMsg(SStreamTask* pTask, SStreamDispatchReq* pReq, SRpcMsg* pMsg);
int32_t streamProcessDispatchRsp(SStreamTask* pTask, SStreamDispatchRsp* pRsp, int32_t code);

void streamTaskGetUpstreamTaskEpInfo(SStreamTask* pTask, int32_t taskId, SStreamUpstreamEpInfo** pEpInfo);
#if 0
SEpSet* streamTaskGetDownstreamEpInfo(SStreamTask* pTask, int32_t taskId);
#endif

void streamTaskInputFail(SStreamTask* pTask);

bool streamTaskShouldStop(const SStreamTask* pStatus);
bool streamTaskShouldPause(const SStreamTask* pStatus);
bool streamTaskIsIdle(const SStreamTask* pTask);
bool streamTaskReadyToRun(const SStreamTask* pTask, char** pStatus);

int32_t          createStreamTaskIdStr(int64_t streamId, int32_t taskId, const char** pId);
SStreamTaskState streamTaskGetStatus(const SStreamTask* pTask);
const char*      streamTaskGetStatusStr(ETaskStatus status);
void             streamTaskResetStatus(SStreamTask* pTask);
void             streamTaskSetStatusReady(SStreamTask* pTask);
ETaskStatus      streamTaskGetPrevStatus(const SStreamTask* pTask);
const char*      streamTaskGetExecType(int32_t type);
int32_t          streamTaskAllocRefId(SStreamTask* pTask, int64_t** pRefId);
void             streamTaskFreeRefId(int64_t* pRefId);

bool streamTaskUpdateEpsetInfo(SStreamTask* pTask, SArray* pNodeList);
void streamTaskResetUpstreamStageInfo(SStreamTask* pTask);

//  stream task sched
bool    streamTaskIsAllUpstreamClosed(SStreamTask* pTask);
bool    streamTaskSetSchedStatusWait(SStreamTask* pTask);
int8_t  streamTaskSetSchedStatusActive(SStreamTask* pTask);
int8_t  streamTaskSetSchedStatusInactive(SStreamTask* pTask);
int32_t streamTaskClearHTaskAttr(SStreamTask* pTask, int32_t clearRelHalt);

int32_t streamExecTask(SStreamTask* pTask);
int32_t streamResumeTask(SStreamTask* pTask);
int32_t streamTrySchedExec(SStreamTask* pTask);
int32_t streamTaskSchedTask(SMsgCb* pMsgCb, int32_t vgId, int64_t streamId, int32_t taskId, int32_t execType);
void    streamTaskResumeInFuture(SStreamTask* pTask);
void    streamTaskClearSchedIdleInfo(SStreamTask* pTask);
void    streamTaskSetIdleInfo(SStreamTask* pTask, int32_t idleTime);

// check downstream status
int32_t streamTaskCheckStatus(SStreamTask* pTask, int32_t upstreamId, int32_t vgId, int64_t stage, int64_t* oldStage);
void    streamTaskSendCheckMsg(SStreamTask* pTask);
void    streamTaskProcessCheckMsg(SStreamMeta* pMeta, SStreamTaskCheckReq* pReq, SStreamTaskCheckRsp* pRsp);
int32_t streamTaskSendCheckRsp(const SStreamMeta* pMeta, int32_t vgId, SStreamTaskCheckRsp* pRsp,
                               SRpcHandleInfo* pRpcInfo, int32_t taskId);
int32_t streamTaskProcessCheckRsp(SStreamTask* pTask, const SStreamTaskCheckRsp* pRsp);

// check downstream status
void streamTaskStartMonitorCheckRsp(SStreamTask* pTask);
void streamTaskStopMonitorCheckRsp(STaskCheckInfo* pInfo, const char* id);
void streamTaskCleanupCheckInfo(STaskCheckInfo* pInfo);

// fill-history task
int32_t streamLaunchFillHistoryTask(SStreamTask* pTask);
int32_t streamStartScanHistoryAsync(SStreamTask* pTask, int8_t igUntreated);
void    streamExecScanHistoryInFuture(SStreamTask* pTask, int32_t idleDuration);
bool    streamHistoryTaskSetVerRangeStep2(SStreamTask* pTask, int64_t latestVer);

// checkpoint related
void    streamTaskGetActiveCheckpointInfo(const SStreamTask* pTask, int32_t* pTransId, int64_t* pCheckpointId);
int32_t streamTaskSetActiveCheckpointInfo(SStreamTask* pTask, int64_t activeCheckpointId);
void    streamTaskSetFailedChkptInfo(SStreamTask* pTask, int32_t transId, int64_t checkpointId);
bool    streamTaskAlreadySendTrigger(SStreamTask* pTask, int32_t downstreamNodeId);
void    streamTaskGetTriggerRecvStatus(SStreamTask* pTask, int32_t* pRecved, int32_t* pTotal);
int32_t streamTaskInitTriggerDispatchInfo(SStreamTask* pTask);
void    streamTaskSetTriggerDispatchConfirmed(SStreamTask* pTask, int32_t vgId);
int32_t streamTaskSendCheckpointTriggerMsg(SStreamTask* pTask, int32_t dstTaskId, int32_t downstreamNodeId,
                                           SRpcHandleInfo* pInfo, int32_t code);

int32_t streamQueueGetNumOfItems(const SStreamQueue* pQueue);
int32_t streamQueueGetNumOfUnAccessedItems(const SStreamQueue* pQueue);

// common
void    streamTaskPause(SStreamTask* pTask);
void    streamTaskResume(SStreamTask* pTask);
int32_t streamTaskStop(SStreamTask* pTask);
int32_t streamTaskSetUpstreamInfo(SStreamTask* pTask, const SStreamTask* pUpstreamTask);
void    streamTaskSetFixedDownstreamInfo(SStreamTask* pTask, const SStreamTask* pDownstreamTask);
int32_t streamTaskReleaseState(SStreamTask* pTask);
int32_t streamTaskReloadState(SStreamTask* pTask);
void    streamTaskOpenUpstreamInput(SStreamTask* pTask, int32_t taskId);
void    streamTaskCloseUpstreamInput(SStreamTask* pTask, int32_t taskId);
void    streamTaskOpenAllUpstreamInput(SStreamTask* pTask);
int32_t streamTaskSetDb(SStreamMeta* pMeta, SStreamTask* pTask, const char* key);
bool    streamTaskIsSinkTask(const SStreamTask* pTask);
void    streamTaskSetRemoveBackendFiles(SStreamTask* pTask);

void             streamTaskStatusInit(STaskStatusEntry* pEntry, const SStreamTask* pTask);
void             streamTaskStatusCopy(STaskStatusEntry* pDst, const STaskStatusEntry* pSrc);
STaskStatusEntry streamTaskGetStatusEntry(SStreamTask* pTask);

// source level
int32_t streamSetParamForStreamScannerStep1(SStreamTask* pTask, SVersionRange* pVerRange, STimeWindow* pWindow);
int32_t streamSetParamForStreamScannerStep2(SStreamTask* pTask, SVersionRange* pVerRange, STimeWindow* pWindow);
SScanhistoryDataInfo streamScanHistoryData(SStreamTask* pTask, int64_t st);

// stream task meta
void    streamMetaInit();
void    streamMetaCleanup();
int32_t streamMetaOpen(const char* path, void* ahandle, FTaskBuild expandFunc, FTaskExpand expandTaskFn, int32_t vgId,
                       int64_t stage, startComplete_fn_t fn, SStreamMeta** pMeta);
void    streamMetaClose(SStreamMeta* streamMeta);
int32_t streamMetaSaveTask(SStreamMeta* pMeta, SStreamTask* pTask);  // save to stream meta store
int32_t streamMetaRemoveTask(SStreamMeta* pMeta, STaskId* pKey);
int32_t streamMetaRegisterTask(SStreamMeta* pMeta, int64_t ver, SStreamTask* pTask, bool* pAdded);
int32_t streamMetaUnregisterTask(SStreamMeta* pMeta, int64_t streamId, int32_t taskId);
int32_t streamMetaGetNumOfTasks(SStreamMeta* pMeta);
int32_t streamMetaAcquireTaskNoLock(SStreamMeta* pMeta, int64_t streamId, int32_t taskId, SStreamTask** pTask);
int32_t streamMetaAcquireTaskUnsafe(SStreamMeta* pMeta, STaskId* pId, SStreamTask** pTask);
int32_t streamMetaAcquireTask(SStreamMeta* pMeta, int64_t streamId, int32_t taskId, SStreamTask** pTask);
void    streamMetaReleaseTask(SStreamMeta* pMeta, SStreamTask* pTask);
void    streamMetaClear(SStreamMeta* pMeta);
void    streamMetaInitBackend(SStreamMeta* pMeta);
int32_t streamMetaCommit(SStreamMeta* pMeta);
int64_t streamMetaGetLatestCheckpointId(SStreamMeta* pMeta);
void    streamMetaNotifyClose(SStreamMeta* pMeta);
void    streamMetaStartHb(SStreamMeta* pMeta);
int32_t streamMetaAddTaskLaunchResult(SStreamMeta* pMeta, int64_t streamId, int32_t taskId, int64_t startTs,
                                      int64_t endTs, bool ready);
int32_t streamMetaInitStartInfo(STaskStartInfo* pStartInfo);
void    streamMetaClearStartInfo(STaskStartInfo* pStartInfo);

int32_t streamMetaResetTaskStatus(SStreamMeta* pMeta);
int32_t streamMetaAddFailedTask(SStreamMeta* pMeta, int64_t streamId, int32_t taskId);
void    streamMetaAddFailedTaskSelf(SStreamTask* pTask, int64_t failedTs);
void    streamMetaAddIntoUpdateTaskList(SStreamMeta* pMeta, SStreamTask* pTask, SStreamTask* pHTask, int32_t transId,
                                        int64_t startTs);
void    streamMetaClearSetUpdateTaskListComplete(SStreamMeta* pMeta);
bool    streamMetaInitUpdateTaskList(SStreamMeta* pMeta, int32_t transId);

void    streamMetaRLock(SStreamMeta* pMeta);
void    streamMetaRUnLock(SStreamMeta* pMeta);
void    streamMetaWLock(SStreamMeta* pMeta);
void    streamMetaWUnLock(SStreamMeta* pMeta);
void    streamSetFatalError(SStreamMeta* pMeta, int32_t code, const char* funcName, int32_t lino);
int32_t streamGetFatalError(const SStreamMeta* pMeta);

void    streamMetaResetStartInfo(STaskStartInfo* pMeta, int32_t vgId);
int32_t streamMetaSendMsgBeforeCloseTasks(SStreamMeta* pMeta, SArray** pTaskList);
void    streamMetaUpdateStageRole(SStreamMeta* pMeta, int64_t stage, bool isLeader);
void    streamMetaLoadAllTasks(SStreamMeta* pMeta);
int32_t streamMetaStartAllTasks(SStreamMeta* pMeta);
int32_t streamMetaStopAllTasks(SStreamMeta* pMeta);
int32_t streamMetaStartOneTask(SStreamMeta* pMeta, int64_t streamId, int32_t taskId);
bool    streamMetaAllTasksReady(const SStreamMeta* pMeta);
int32_t streamTaskSendNegotiateChkptIdMsg(SStreamTask* pTask);
int32_t streamTaskCheckIfReqConsenChkptId(SStreamTask* pTask, int64_t ts);
void    streamTaskSetConsenChkptIdRecv(SStreamTask* pTask, int32_t transId, int64_t ts);
void    streamTaskSetReqConsenChkptId(SStreamTask* pTask, int64_t ts);

// timer
int32_t streamTimerGetInstance(tmr_h* pTmr);
void    streamTmrStart(TAOS_TMR_CALLBACK fp, int32_t mseconds, void* pParam, void* pHandle, tmr_h* pTmrId, int32_t vgId,
                       const char* pMsg);
void    streamTmrStop(tmr_h tmrId);

// checkpoint
int32_t streamProcessCheckpointSourceReq(SStreamTask* pTask, SStreamCheckpointSourceReq* pReq);
int32_t streamTaskProcessCheckpointTriggerRsp(SStreamTask* pTask, SCheckpointTriggerRsp* pRsp);
int32_t streamProcessCheckpointReadyMsg(SStreamTask* pTask, int64_t checkpointId, int32_t downstreamNodeId,
                                        int32_t downstreamTaskId);
int32_t streamTaskProcessCheckpointReadyRsp(SStreamTask* pTask, int32_t upstreamTaskId, int64_t checkpointId);
int32_t streamTaskBuildCheckpoint(SStreamTask* pTask);
void    streamTaskClearCheckInfo(SStreamTask* pTask, bool clearChkpReadyMsg);
int32_t streamAlignTransferState(SStreamTask* pTask);
int32_t streamBuildAndSendDropTaskMsg(SMsgCb* pMsgCb, int32_t vgId, SStreamTaskId* pTaskId, int64_t resetRelHalt);
int32_t streamAddCheckpointSourceRspMsg(SStreamCheckpointSourceReq* pReq, SRpcHandleInfo* pRpcInfo, SStreamTask* pTask);
int32_t streamTaskBuildCheckpointSourceRsp(SStreamCheckpointSourceReq* pReq, SRpcHandleInfo* pRpcInfo, SRpcMsg* pMsg,
                                           int32_t setCode);
int32_t streamSendChkptReportMsg(SStreamTask* pTask, SCheckpointInfo* pCheckpointInfo, int8_t dropRelHTask);
int32_t streamTaskUpdateTaskCheckpointInfo(SStreamTask* pTask, bool restored, SVUpdateCheckpointInfoReq* pReq);
int32_t streamTaskCreateActiveChkptInfo(SActiveCheckpointInfo** pRes);
void    streamTaskSetCheckpointFailed(SStreamTask* pTask);

// stream task state machine, and event handling
int32_t streamCreateStateMachine(SStreamTask* pTask);
void    streamDestroyStateMachine(SStreamTaskSM* pSM);
int32_t streamTaskHandleEvent(SStreamTaskSM* pSM, EStreamTaskEvent event);
int32_t streamTaskHandleEventAsync(SStreamTaskSM* pSM, EStreamTaskEvent event, __state_trans_user_fn callbackFn,
                                   void* param);
int32_t streamTaskOnHandleEventSuccess(SStreamTaskSM* pSM, EStreamTaskEvent event, __state_trans_user_fn callbackFn,
                                       void* param);
int32_t streamTaskRestoreStatus(SStreamTask* pTask);

// stream task retrieve related API
int32_t streamProcessRetrieveReq(SStreamTask* pTask, SStreamRetrieveReq* pReq);
int32_t streamTaskBroadcastRetrieveReq(SStreamTask* pTask, SStreamRetrieveReq* req);
void    streamTaskSendRetrieveRsp(SStreamRetrieveReq* pReq, SRpcMsg* pRsp);

int32_t streamProcessHeartbeatRsp(SStreamMeta* pMeta, SMStreamHbRspMsg* pRsp);
int32_t streamTaskSendCheckpointsourceRsp(SStreamTask* pTask);

void streamMutexLock(TdThreadMutex* pMutex);
void streamMutexUnlock(TdThreadMutex* pMutex);
void streamMutexDestroy(TdThreadMutex* pMutex);

#ifdef __cplusplus
}
#endif

#endif /* ifndef TDENGINE_TSTREAM_H */
