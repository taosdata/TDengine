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

#ifndef _STREAM_H_
#define _STREAM_H_

#include "os.h"
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
#define STREAM_EXEC_T_UPDATE_TASK_EPSET (-7)

typedef struct SStreamTask   SStreamTask;
typedef struct SStreamQueue  SStreamQueue;
typedef struct SStreamTaskSM SStreamTaskSM;

#define SSTREAM_TASK_VER                  3
#define SSTREAM_TASK_INCOMPATIBLE_VER     1
#define SSTREAM_TASK_NEED_CONVERT_VER     2
#define SSTREAM_TASK_SUBTABLE_CHANGED_VER 3

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
  TASK_TRIGGER_STATUS__ACTIVE,
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

enum {
  STREAM_META_WILL_STOP = 1,
  STREAM_META_OK_TO_STOP = 2,
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

typedef struct {
  int8_t type;
} SStreamQueueItem;

typedef void    FTbSink(SStreamTask* pTask, void* vnode, void* data);
typedef void    FSmaSink(void* vnode, int64_t smaId, const SArray* data);
typedef int32_t FTaskExpand(void* ahandle, SStreamTask* pTask, int64_t ver);

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

typedef struct SStreamQueueNode SStreamQueueNode;

struct SStreamQueueNode {
  SStreamQueueItem* item;
  SStreamQueueNode* next;
};

typedef struct {
  SStreamQueueNode* head;
  int64_t           size;
} SStreamQueueRes;

#if 0
bool              streamQueueResEmpty(const SStreamQueueRes* pRes);
int64_t           streamQueueResSize(const SStreamQueueRes* pRes);
SStreamQueueNode* streamQueueResFront(SStreamQueueRes* pRes);
SStreamQueueNode* streamQueueResPop(SStreamQueueRes* pRes);
void              streamQueueResClear(SStreamQueueRes* pRes);
SStreamQueueRes   streamQueueBuildRes(SStreamQueueNode* pNode);
#endif

typedef struct {
  SStreamQueueNode* pHead;
} SStreamQueue1;

#if 0
bool            streamQueueHasTask(const SStreamQueue1* pQueue);
int32_t         streamQueuePush(SStreamQueue1* pQueue, SStreamQueueItem* pItem);
SStreamQueueRes streamQueueGetRes(SStreamQueue1* pQueue);
#endif

SStreamDataSubmit* streamDataSubmitNew(SPackedData* pData, int32_t type);
void               streamDataSubmitDestroy(SStreamDataSubmit* pDataSubmit);

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
  int32_t   waitingRspCnt;
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
} EScanHistoryRet;

typedef struct {
  EScanHistoryRet ret;
  int32_t         idleTime;
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
  void*           vnode;  // not available to encoder and decoder
  FTbSink*        tbSinkFunc;
  STSchema*       pTSchema;
  SSHashObj*      pTblInfo;
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

typedef struct SStreamChildEpInfo {
  int32_t nodeId;
  int32_t childId;
  int32_t taskId;
  SEpSet  epSet;
  bool    dataAllowed;  // denote if the data from this upstream task is allowed to put into inputQ, not serialize it
  int64_t stage;  // upstream task stage value, to denote if the upstream node has restart/replica changed/transfer
} SStreamChildEpInfo;

typedef struct STaskId {
  int64_t streamId;
  int64_t taskId;
} STaskId;

typedef struct SStreamTaskId {
  int64_t     streamId;
  int32_t     taskId;
  const char* idStr;
} SStreamTaskId;

typedef struct SCheckpointInfo {
  int64_t startTs;
  int64_t checkpointId;

  int64_t checkpointVer;  // latest checkpointId version
  int64_t processedVer;
  int64_t nextProcessVer;  // current offset in WAL, not serialize it
  int64_t failedId;        // record the latest failed checkpoint id
  int64_t checkpointingId;
  int32_t downstreamAlignNum;
  int32_t numOfNotReady;
  bool    dispatchCheckpointTrigger;
  int64_t msgVer;
  int32_t transId;
} SCheckpointInfo;

typedef struct SStreamStatus {
  SStreamTaskSM* pSM;
  int8_t         taskStatus;
  int8_t         downstreamReady;  // downstream tasks are all ready now, if this flag is set
  int8_t         schedStatus;
  int8_t         statusBackup;
  int32_t        schedIdleTime;  // idle time before invoke again
  int32_t        timerActive;    // timer is active
  int64_t        lastExecTs;     // last exec time stamp
  int32_t        inScanHistorySentinel;
  bool           appendTranstateBlock;  // has append the transfer state data block already
  bool           supplementaryWalscan;  // complete the supplementary wal scan or not
} SStreamStatus;

typedef struct SDataRange {
  SVersionRange range;
  STimeWindow   window;
} SDataRange;

typedef struct SSTaskBasicInfo {
  int32_t nodeId;  // vgroup id or snode id
  SEpSet  epSet;
  SEpSet  mnodeEpset;  // mnode epset for send heartbeat
  int32_t selfChildId;
  int32_t totalLevel;
  int8_t  taskLevel;
  int8_t  fillHistory;   // is fill history task or not
  int64_t triggerParam;  // in msec
} SSTaskBasicInfo;

typedef struct SStreamDispatchReq SStreamDispatchReq;
typedef struct STokenBucket       STokenBucket;
typedef struct SMetaHbInfo        SMetaHbInfo;

typedef struct SDispatchMsgInfo {
  SStreamDispatchReq* pData;  // current dispatch data
  int8_t              dispatchMsgType;
  int16_t             msgType;     // dispatch msg type
  int32_t             retryCount;  // retry send data count
  int64_t             startTs;     // dispatch start time, record total elapsed time for dispatch
  SArray*             pRetryList;  // current dispatch successfully completed node of downstream
  void*               pTimer;      // used to dispatch data after a given time duration
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
  int64_t       created;
  int64_t       init;
  int64_t       start;
  int64_t       step1Start;
  double        step1El;
  int64_t       step2Start;
  double        step2El;
  int32_t       updateCount;
  int64_t       latestUpdateTs;
  int32_t       processDataBlocks;
  int64_t       processDataSize;
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
  SArray*       pDownstreamUpdateList;
} STaskOutputInfo;

typedef struct SUpstreamInfo {
  SArray* pList;
  int32_t numOfClosed;
} SUpstreamInfo;

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
  SHistoryTaskInfo    hTaskInfo;
  STaskId             streamTaskId;
  STaskExecStatisInfo execInfo;
  SArray*             pReadyMsgList;  // SArray<SStreamChkptReadyInfo*>
  TdThreadMutex       lock;           // secure the operation of set task status and puting data into inputQ
  SMsgCb*             pMsgCb;         // msg handle
  SStreamState*       pState;         // state backend
  SArray*             pRspMsgList;
  SUpstreamInfo       upstreamInfo;

  // the followings attributes don't be serialized
  SScanhistorySchedInfo schedHistoryInfo;

  int32_t             notReadyTasks;
  int32_t             numOfWaitingUpstream;
  int64_t             checkReqId;
  SArray*             checkReqIds;  // shuffle
  int32_t             refCnt;
  int32_t             transferStateAlignCnt;
  struct SStreamMeta* pMeta;
  SSHashObj*          pNameMap;
  void*               pBackend;
  int8_t              subtableWithoutMd5;
  char                reserve[255];
};

typedef int32_t (*startComplete_fn_t)(struct SStreamMeta*);

typedef struct STaskStartInfo {
  int64_t            startTs;
  int64_t            readyTs;
  int32_t            tasksWillRestart;
  int32_t            taskStarting;    // restart flag, sentinel to guard the restart procedure.
  SHashObj*          pReadyTaskSet;   // tasks that are all ready for running stream processing
  SHashObj*          pFailedTaskSet;  // tasks that are done the check downstream process, may be successful or failed
  int64_t            elapsedTime;
  int32_t            restartCount;  // restart task counter
  startComplete_fn_t completeFn;    // complete callback function
} STaskStartInfo;

typedef struct STaskUpdateInfo {
  SHashObj* pTasks;
  int32_t   transId;
} STaskUpdateInfo;

typedef struct SScanWalInfo {
  int32_t scanCounter;
  tmr_h   scanTimer;
} SScanWalInfo;

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
  FTaskExpand*    expandFunc;
  int32_t         vgId;
  int64_t         stage;
  int32_t         role;
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
  SHashObj*       pUpdateTaskSet;
  int32_t         numOfStreamTasks;  // this value should be increased when a new task is added into the meta
  int32_t         numOfPausedTasks;
  int64_t         rid;

  int64_t  chkpId;
  int32_t  chkpCap;
  SArray*  chkpSaved;
  SArray*  chkpInUse;
  SRWLatch chkpDirLock;
  void*    qHandle;
  void*    bkdChkptMgt;
} SStreamMeta;

int32_t tEncodeStreamEpInfo(SEncoder* pEncoder, const SStreamChildEpInfo* pInfo);
int32_t tDecodeStreamEpInfo(SDecoder* pDecoder, SStreamChildEpInfo* pInfo);

SStreamTask* tNewStreamTask(int64_t streamId, int8_t taskLevel, SEpSet* pEpset, bool fillHistory, int64_t triggerParam,
                            SArray* pTaskList, bool hasFillhistory, int8_t subtableWithoutMd5);
int32_t      tEncodeStreamTask(SEncoder* pEncoder, const SStreamTask* pTask);
int32_t      tDecodeStreamTask(SDecoder* pDecoder, SStreamTask* pTask);
void         tFreeStreamTask(SStreamTask* pTask);
int32_t      streamTaskInit(SStreamTask* pTask, SStreamMeta* pMeta, SMsgCb* pMsgCb, int64_t ver);

int32_t tDecodeStreamTaskChkInfo(SDecoder* pDecoder, SCheckpointInfo* pChkpInfo);
int32_t tDecodeStreamTaskId(SDecoder* pDecoder, STaskId* pTaskId);

int32_t streamTaskPutDataIntoInputQ(SStreamTask* pTask, SStreamQueueItem* pItem);
int32_t streamTaskPutDataIntoOutputQ(SStreamTask* pTask, SStreamDataBlock* pBlock);
int32_t streamTaskPutTranstateIntoInputQ(SStreamTask* pTask);
bool    streamQueueIsFull(const SStreamQueue* pQueue);

typedef struct {
  SMsgHead head;
  int64_t  streamId;
  int32_t  taskId;
  int32_t  reqType;
} SStreamTaskRunReq;

struct SStreamDispatchReq {
  int32_t type;
  int64_t stage;  // nodeId from upstream task
  int64_t streamId;
  int32_t taskId;
  int32_t msgId;  // msg id to identify if the incoming msg from the same sender
  int32_t srcVgId;
  int32_t upstreamTaskId;
  int32_t upstreamChildId;
  int32_t upstreamNodeId;
  int32_t upstreamRelTaskId;
  int32_t blockNum;
  int64_t totalLen;
  SArray* dataLen;  // SArray<int32_t>
  SArray* data;     // SArray<SRetrieveTableRsp*>
};

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
  int64_t            streamId;
  int64_t            reqId;
  int32_t            srcTaskId;
  int32_t            srcNodeId;
  int32_t            dstTaskId;
  int32_t            dstNodeId;
  int32_t            retrieveLen;
  SRetrieveTableRsp* pRetrieve;
} SStreamRetrieveReq;

typedef struct {
  int64_t streamId;
  int32_t childId;
  int32_t rspFromTaskId;
  int32_t rspToTaskId;
} SStreamRetrieveRsp;

typedef struct {
  int64_t reqId;
  int64_t stage;
  int64_t streamId;
  int32_t upstreamNodeId;
  int32_t upstreamTaskId;
  int32_t downstreamNodeId;
  int32_t downstreamTaskId;
  int32_t childId;
} SStreamTaskCheckReq;

typedef struct {
  int64_t reqId;
  int64_t streamId;
  int32_t upstreamNodeId;
  int32_t upstreamTaskId;
  int32_t downstreamNodeId;
  int32_t downstreamTaskId;
  int32_t childId;
  int64_t oldStage;
  int8_t  status;
} SStreamTaskCheckRsp;

typedef struct {
  SMsgHead msgHead;
  int64_t  streamId;
  int32_t  taskId;
  int8_t   igUntreated;
} SStreamScanHistoryReq;

// mndTrigger: denote if this checkpoint is triggered by mnode or as requested from tasks when transfer-state finished
typedef struct {
  int64_t streamId;
  int64_t checkpointId;
  int32_t taskId;
  int32_t nodeId;
  SEpSet  mgmtEps;
  int32_t mnodeId;
  int32_t transId;
  int8_t  mndTrigger;
  int64_t expireTime;
} SStreamCheckpointSourceReq;

typedef struct {
  int64_t streamId;
  int64_t checkpointId;
  int32_t taskId;
  int32_t nodeId;
  int32_t mnodeId;
  int64_t expireTime;
  int8_t  success;
} SStreamCheckpointSourceRsp;

int32_t tEncodeStreamCheckpointSourceReq(SEncoder* pEncoder, const SStreamCheckpointSourceReq* pReq);
int32_t tDecodeStreamCheckpointSourceReq(SDecoder* pDecoder, SStreamCheckpointSourceReq* pReq);

int32_t tEncodeStreamCheckpointSourceRsp(SEncoder* pEncoder, const SStreamCheckpointSourceRsp* pRsp);

typedef struct {
  SMsgHead msgHead;
  int64_t  streamId;
  int64_t  checkpointId;
  int32_t  downstreamTaskId;
  int32_t  downstreamNodeId;
  int32_t  upstreamTaskId;
  int32_t  upstreamNodeId;
  int32_t  childId;
} SStreamCheckpointReadyMsg;

int32_t tEncodeStreamCheckpointReadyMsg(SEncoder* pEncoder, const SStreamCheckpointReadyMsg* pRsp);
int32_t tDecodeStreamCheckpointReadyMsg(SDecoder* pDecoder, SStreamCheckpointReadyMsg* pRsp);

typedef struct STaskStatusEntry {
  STaskId id;
  int32_t status;
  int32_t statusLastDuration;  // to record the last duration of current status
  int64_t stage;
  int32_t nodeId;
  int64_t verStart;          // start version in WAL, only valid for source task
  int64_t verEnd;            // end version in WAL, only valid for source task
  int64_t processedVer;      // only valid for source task
  int64_t checkpointId;      // current active checkpoint id
  int32_t chkpointTransId;   // checkpoint trans id
  int8_t  checkpointFailed;  // denote if the checkpoint is failed or not
  bool    inputQChanging;    // inputQ is changing or not
  int64_t inputQUnchangeCounter;
  double  inputQUsed;  // in MiB
  double  inputRate;
  double  sinkQuota;     // existed quota size for sink task
  double  sinkDataSize;  // sink to dst data size
} STaskStatusEntry;

typedef struct SStreamHbMsg {
  int32_t vgId;
  int32_t numOfTasks;
  SArray* pTaskStatus;   // SArray<STaskStatusEntry>
  SArray* pUpdateNodes;  // SArray<int32_t>, needs update the epsets in stream tasks for those nodes.
} SStreamHbMsg;

int32_t tEncodeStreamHbMsg(SEncoder* pEncoder, const SStreamHbMsg* pRsp);
int32_t tDecodeStreamHbMsg(SDecoder* pDecoder, SStreamHbMsg* pRsp);
void    streamMetaClearHbMsg(SStreamHbMsg* pMsg);

typedef struct SNodeUpdateInfo {
  int32_t nodeId;
  SEpSet  prevEp;
  SEpSet  newEp;
} SNodeUpdateInfo;

typedef struct SStreamTaskNodeUpdateMsg {
  int32_t transId;  // to identify the msg
  int64_t streamId;
  int32_t taskId;
  SArray* pNodeList;  // SArray<SNodeUpdateInfo>
} SStreamTaskNodeUpdateMsg;

int32_t tEncodeStreamTaskUpdateMsg(SEncoder* pEncoder, const SStreamTaskNodeUpdateMsg* pMsg);
int32_t tDecodeStreamTaskUpdateMsg(SDecoder* pDecoder, SStreamTaskNodeUpdateMsg* pMsg);

typedef struct SStreamTaskState {
  ETaskStatus state;
  char*       name;
} SStreamTaskState;

int32_t tEncodeStreamTaskCheckReq(SEncoder* pEncoder, const SStreamTaskCheckReq* pReq);
int32_t tDecodeStreamTaskCheckReq(SDecoder* pDecoder, SStreamTaskCheckReq* pReq);

int32_t tEncodeStreamTaskCheckRsp(SEncoder* pEncoder, const SStreamTaskCheckRsp* pRsp);
int32_t tDecodeStreamTaskCheckRsp(SDecoder* pDecoder, SStreamTaskCheckRsp* pRsp);

int32_t tEncodeStreamDispatchReq(SEncoder* pEncoder, const SStreamDispatchReq* pReq);
int32_t tDecodeStreamDispatchReq(SDecoder* pDecoder, SStreamDispatchReq* pReq);
void    tDeleteStreamDispatchReq(SStreamDispatchReq* pReq);

int32_t tEncodeStreamRetrieveReq(SEncoder* pEncoder, const SStreamRetrieveReq* pReq);
int32_t tDecodeStreamRetrieveReq(SDecoder* pDecoder, SStreamRetrieveReq* pReq);
void    tDeleteStreamRetrieveReq(SStreamRetrieveReq* pReq);

typedef struct SStreamTaskCheckpointReq {
  int64_t streamId;
  int32_t taskId;
  int32_t nodeId;
} SStreamTaskCheckpointReq;

int32_t tEncodeStreamTaskCheckpointReq(SEncoder* pEncoder, const SStreamTaskCheckpointReq* pReq);
int32_t tDecodeStreamTaskCheckpointReq(SDecoder* pDecoder, SStreamTaskCheckpointReq* pReq);

int32_t streamSetupScheduleTrigger(SStreamTask* pTask);

int32_t streamProcessDispatchMsg(SStreamTask* pTask, SStreamDispatchReq* pReq, SRpcMsg* pMsg);
int32_t streamProcessDispatchRsp(SStreamTask* pTask, SStreamDispatchRsp* pRsp, int32_t code);

int32_t             streamProcessRetrieveReq(SStreamTask* pTask, SStreamRetrieveReq* pReq);
SStreamChildEpInfo* streamTaskGetUpstreamTaskEpInfo(SStreamTask* pTask, int32_t taskId);

void    streamTaskInputFail(SStreamTask* pTask);
int32_t streamExecTask(SStreamTask* pTask);
int32_t streamResumeTask(SStreamTask* pTask);
int32_t streamSchedExec(SStreamTask* pTask);
bool    streamTaskShouldStop(const SStreamTask* pStatus);
bool    streamTaskShouldPause(const SStreamTask* pStatus);
bool    streamTaskIsIdle(const SStreamTask* pTask);
bool    streamTaskReadyToRun(const SStreamTask* pTask, char** pStatus);

char*             createStreamTaskIdStr(int64_t streamId, int32_t taskId);
SStreamTaskState* streamTaskGetStatus(const SStreamTask* pTask);
const char*       streamTaskGetStatusStr(ETaskStatus status);
void              streamTaskResetStatus(SStreamTask* pTask);
void              streamTaskSetStatusReady(SStreamTask* pTask);
ETaskStatus       streamTaskGetPrevStatus(const SStreamTask* pTask);

void initRpcMsg(SRpcMsg* pMsg, int32_t msgType, void* pCont, int32_t contLen);

// recover and fill history
void streamTaskCheckDownstream(SStreamTask* pTask);

int32_t streamTaskCheckStatus(SStreamTask* pTask, int32_t upstreamId, int32_t vgId, int64_t stage, int64_t* oldStage);
int32_t streamTaskUpdateEpsetInfo(SStreamTask* pTask, SArray* pNodeList);
void    streamTaskResetUpstreamStageInfo(SStreamTask* pTask);
bool    streamTaskIsAllUpstreamClosed(SStreamTask* pTask);
bool    streamTaskSetSchedStatusWait(SStreamTask* pTask);
int8_t  streamTaskSetSchedStatusActive(SStreamTask* pTask);
int8_t  streamTaskSetSchedStatusInactive(SStreamTask* pTask);
int32_t streamTaskClearHTaskAttr(SStreamTask* pTask, int32_t clearRelHalt, bool metaLock);

int32_t streamTaskHandleEvent(SStreamTaskSM* pSM, EStreamTaskEvent event);
int32_t streamTaskOnHandleEventSuccess(SStreamTaskSM* pSM, EStreamTaskEvent event);
void    streamTaskRestoreStatus(SStreamTask* pTask);

int32_t streamSendCheckRsp(const SStreamMeta* pMeta, const SStreamTaskCheckReq* pReq, SStreamTaskCheckRsp* pRsp,
                           SRpcHandleInfo* pRpcInfo, int32_t taskId);
int32_t streamProcessCheckRsp(SStreamTask* pTask, const SStreamTaskCheckRsp* pRsp);
int32_t streamLaunchFillHistoryTask(SStreamTask* pTask);
int32_t streamStartScanHistoryAsync(SStreamTask* pTask, int8_t igUntreated);
int32_t streamReExecScanHistoryFuture(SStreamTask* pTask, int32_t idleDuration);
bool    streamHistoryTaskSetVerRangeStep2(SStreamTask* pTask, int64_t latestVer);

int32_t streamQueueGetNumOfItems(const SStreamQueue* pQueue);

// common
void    streamTaskPause(SStreamMeta* pMeta, SStreamTask* pTask);
void    streamTaskResume(SStreamTask* pTask);
int32_t streamTaskStop(SStreamTask* pTask);
int32_t streamTaskSetUpstreamInfo(SStreamTask* pTask, const SStreamTask* pUpstreamTask);
void    streamTaskUpdateUpstreamInfo(SStreamTask* pTask, int32_t nodeId, const SEpSet* pEpSet);
void    streamTaskUpdateDownstreamInfo(SStreamTask* pTask, int32_t nodeId, const SEpSet* pEpSet);
void    streamTaskSetFixedDownstreamInfo(SStreamTask* pTask, const SStreamTask* pDownstreamTask);
int32_t streamTaskReleaseState(SStreamTask* pTask);
int32_t streamTaskReloadState(SStreamTask* pTask);
void    streamTaskCloseUpstreamInput(SStreamTask* pTask, int32_t taskId);
void    streamTaskOpenAllUpstreamInput(SStreamTask* pTask);
int32_t streamTaskSetDb(SStreamMeta* pMeta, void* pTask, char* key);
bool    streamTaskIsSinkTask(const SStreamTask* pTask);
int32_t streamTaskSendCheckpointReq(SStreamTask* pTask);

void streamTaskStatusInit(STaskStatusEntry* pEntry, const SStreamTask* pTask);
void streamTaskStatusCopy(STaskStatusEntry* pDst, const STaskStatusEntry* pSrc);

// source level
int32_t streamSetParamForStreamScannerStep1(SStreamTask* pTask, SVersionRange* pVerRange, STimeWindow* pWindow);
int32_t streamSetParamForStreamScannerStep2(SStreamTask* pTask, SVersionRange* pVerRange, STimeWindow* pWindow);
SScanhistoryDataInfo streamScanHistoryData(SStreamTask* pTask, int64_t st);

// stream task meta
void         streamMetaInit();
void         streamMetaCleanup();
SStreamMeta* streamMetaOpen(const char* path, void* ahandle, FTaskExpand expandFunc, int32_t vgId, int64_t stage,
                            startComplete_fn_t fn);
void         streamMetaClose(SStreamMeta* streamMeta);
int32_t      streamMetaSaveTask(SStreamMeta* pMeta, SStreamTask* pTask);  // save to stream meta store
int32_t      streamMetaRemoveTask(SStreamMeta* pMeta, STaskId* pKey);
int32_t      streamMetaRegisterTask(SStreamMeta* pMeta, int64_t ver, SStreamTask* pTask, bool* pAdded);
int32_t      streamMetaUnregisterTask(SStreamMeta* pMeta, int64_t streamId, int32_t taskId);
int32_t      streamMetaGetNumOfTasks(SStreamMeta* pMeta);
SStreamTask* streamMetaAcquireTaskNoLock(SStreamMeta* pMeta, int64_t streamId, int32_t taskId);
SStreamTask* streamMetaAcquireTask(SStreamMeta* pMeta, int64_t streamId, int32_t taskId);
void         streamMetaReleaseTask(SStreamMeta* pMeta, SStreamTask* pTask);
SStreamTask* streamMetaAcquireOneTask(SStreamTask* pTask);
void         streamMetaClear(SStreamMeta* pMeta);
void         streamMetaInitBackend(SStreamMeta* pMeta);
int32_t      streamMetaCommit(SStreamMeta* pMeta);
int64_t      streamMetaGetLatestCheckpointId(SStreamMeta* pMeta);
void         streamMetaNotifyClose(SStreamMeta* pMeta);
void         streamMetaStartHb(SStreamMeta* pMeta);
bool         streamMetaTaskInTimer(SStreamMeta* pMeta);
int32_t      streamMetaAddTaskLaunchResult(SStreamMeta* pMeta, int64_t streamId, int32_t taskId, int64_t startTs,
                                           int64_t endTs, bool ready);
int32_t      streamMetaResetTaskStatus(SStreamMeta* pMeta);

void    streamMetaRLock(SStreamMeta* pMeta);
void    streamMetaRUnLock(SStreamMeta* pMeta);
void    streamMetaWLock(SStreamMeta* pMeta);
void    streamMetaWUnLock(SStreamMeta* pMeta);
void    streamMetaResetStartInfo(STaskStartInfo* pMeta);
SArray* streamMetaSendMsgBeforeCloseTasks(SStreamMeta* pMeta);
void    streamMetaUpdateStageRole(SStreamMeta* pMeta, int64_t stage, bool isLeader);
int32_t streamMetaLoadAllTasks(SStreamMeta* pMeta);
int32_t streamMetaStartAllTasks(SStreamMeta* pMeta);
int32_t streamMetaStopAllTasks(SStreamMeta* pMeta);
int32_t streamMetaStartOneTask(SStreamMeta* pMeta, int64_t streamId, int32_t taskId);
bool    streamMetaAllTasksReady(const SStreamMeta* pMeta);
tmr_h   streamTimerGetInstance();

// checkpoint
int32_t streamProcessCheckpointSourceReq(SStreamTask* pTask, SStreamCheckpointSourceReq* pReq);
int32_t streamProcessCheckpointReadyMsg(SStreamTask* pTask);
int32_t streamTaskBuildCheckpoint(SStreamTask* pTask);
void    streamTaskClearCheckInfo(SStreamTask* pTask, bool clearChkpReadyMsg);
int32_t streamAlignTransferState(SStreamTask* pTask);
int32_t streamBuildAndSendDropTaskMsg(SMsgCb* pMsgCb, int32_t vgId, SStreamTaskId* pTaskId, int64_t resetRelHalt);
int32_t streamAddCheckpointSourceRspMsg(SStreamCheckpointSourceReq* pReq, SRpcHandleInfo* pRpcInfo, SStreamTask* pTask,
                                        int8_t isSucceed);
int32_t buildCheckpointSourceRsp(SStreamCheckpointSourceReq* pReq, SRpcHandleInfo* pRpcInfo, SRpcMsg* pMsg,
                                 int8_t isSucceed);

SStreamTaskSM* streamCreateStateMachine(SStreamTask* pTask);
void*          streamDestroyStateMachine(SStreamTaskSM* pSM);

int32_t broadcastRetrieveMsg(SStreamTask* pTask, SStreamRetrieveReq *req);
void    sendRetrieveRsp(SStreamRetrieveReq *pReq, SRpcMsg* pRsp);

#ifdef __cplusplus
}
#endif

#endif /* ifndef _STREAM_H_ */