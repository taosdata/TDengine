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

#ifndef _STREAM_H_
#define _STREAM_H_

typedef struct SStreamTask SStreamTask;

#define SSTREAM_TASK_VER 2
enum {
  STREAM_STATUS__NORMAL = 0,
  STREAM_STATUS__STOP,
  STREAM_STATUS__INIT,
  STREAM_STATUS__FAILED,
  STREAM_STATUS__RECOVER,
  STREAM_STATUS__PAUSE,
};

enum {
  TASK_STATUS__NORMAL = 0,
  TASK_STATUS__DROPPING,
  TASK_STATUS__FAIL,
  TASK_STATUS__STOP,
  TASK_STATUS__SCAN_HISTORY,  // stream task scan history data by using tsdbread in the stream scanner
  TASK_STATUS__HALT,          // pause, but not be manipulated by user command
  TASK_STATUS__PAUSE,         // pause
  TASK_STATUS__CK,            // stream task is in checkpoint status, no data are allowed to put into inputQ anymore
};

enum {
  TASK_SCHED_STATUS__INACTIVE = 1,
  TASK_SCHED_STATUS__WAITING,
  TASK_SCHED_STATUS__ACTIVE,
  TASK_SCHED_STATUS__FAILED,
  TASK_SCHED_STATUS__DROPPING,
};

enum {
  TASK_INPUT_STATUS__NORMAL = 1,
  TASK_INPUT_STATUS__BLOCKED,
  TASK_INPUT_STATUS__FAILED,
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

typedef struct {
  int8_t type;
} SStreamQueueItem;

typedef void    FTbSink(SStreamTask* pTask, void* vnode, void* data);
typedef int32_t FTaskExpand(void* ahandle, SStreamTask* pTask, int64_t ver);

typedef struct {
  int8_t      type;
  int64_t     ver;
  SPackedData submit;
} SStreamDataSubmit;

typedef struct {
  int8_t  type;
  int64_t ver;
  SArray* submits;   // SArray<SPackedSubmit>
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

typedef struct {
  int8_t       type;
  SSDataBlock* pBlock;
} SStreamTrigger;

typedef struct SStreamQueueNode SStreamQueueNode;

struct SStreamQueueNode {
  SStreamQueueItem* item;
  SStreamQueueNode* next;
};

typedef struct {
  SStreamQueueNode* head;
  int64_t           size;
} SStreamQueueRes;

void streamFreeQitem(SStreamQueueItem* data);

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

typedef struct {
  STaosQueue* pQueue;
  STaosQall*  qall;
  void*       qItem;
  int8_t      status;
} SStreamQueue;

int32_t streamInit();
void    streamCleanUp();

SStreamQueue* streamQueueOpen(int64_t cap);
void          streamQueueClose(SStreamQueue* pQueue, int32_t taskId);
void          streamQueueProcessSuccess(SStreamQueue* queue);
void          streamQueueProcessFail(SStreamQueue* queue);
void*         streamQueueNextItem(SStreamQueue* pQueue);

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
} STaskDispatcherFixedEp;

typedef struct {
  char      stbFullName[TSDB_TABLE_FNAME_LEN];
  int32_t   waitingRspCnt;
  SUseDbRsp dbInfo;
} STaskDispatcherShuffle;

typedef struct {
  int64_t         stbUid;
  char            stbFullName[TSDB_TABLE_FNAME_LEN];
  SSchemaWrapper* pSchemaWrapper;
  void*           vnode;  // not available to encoder and decoder
  FTbSink*        tbSinkFunc;
  STSchema*       pTSchema;
  SSHashObj*      pTblInfo;
} STaskSinkTb;

typedef void FSmaSink(void* vnode, int64_t smaId, const SArray* data);

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

typedef struct SStreamTaskKey {
  int64_t streamId;
  int32_t taskId;
} SStreamTaskKey;

typedef struct SStreamTaskId {
  int64_t     streamId;
  int32_t     taskId;
  const char* idStr;
} SStreamTaskId;

typedef struct SCheckpointInfo {
  int64_t checkpointId;
  int64_t checkpointVer;  // latest checkpointId version
  int64_t nextProcessVer;     // current offset in WAL, not serialize it
} SCheckpointInfo;

typedef struct SStreamStatus {
  int8_t taskStatus;
  int8_t downstreamReady;  // downstream tasks are all ready now, if this flag is set
  int8_t schedStatus;
  int8_t keepTaskStatus;
  bool   appendTranstateBlock;  // has append the transfer state data block already, todo: remove it
  int8_t timerActive;           // timer is active
  int8_t pauseAllowed;          // allowed task status to be set to be paused
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
  int8_t  fillHistory;  // is fill history task or not
  int64_t triggerParam; // in msec
} SSTaskBasicInfo;

typedef struct SDispatchMsgInfo {
  void*   pData;       // current dispatch data
  int16_t msgType;     // dispatch msg type
  int32_t retryCount;  // retry send data count
  int64_t blockingTs;  // output blocking timestamp
} SDispatchMsgInfo;

typedef struct STaskOutputInfo {
  int8_t        type;
  int8_t        status;
  SStreamQueue* queue;
} STaskOutputInfo;

typedef struct STaskInputInfo {
  int8_t status;
  SStreamQueue* queue;
} STaskInputInfo;

typedef struct STaskSchedInfo {
  int8_t  status;
  void*   pTimer;
} STaskSchedInfo;

typedef struct SSinkTaskRecorder {
  int64_t numOfSubmit;
  int64_t numOfBlocks;
  int64_t numOfRows;
} SSinkTaskRecorder;

typedef struct {
  int64_t created;
  int64_t init;
  int64_t step1Start;
  int64_t step2Start;
  int64_t sinkStart;
} STaskTimestamp;

typedef struct STokenBucket {
    int32_t capacity;     // total capacity
    int64_t fillTimestamp;// fill timestamp
    int32_t numOfToken;   // total available tokens
    int32_t rate;         // number of token per second
} STokenBucket;

struct SStreamTask {
  int64_t          ver;
  SStreamTaskId    id;
  SSTaskBasicInfo  info;
  STaskOutputInfo  outputInfo;
  STaskInputInfo   inputInfo;
  STaskSchedInfo   schedInfo;
  SDispatchMsgInfo msgInfo;
  SStreamStatus    status;
  SCheckpointInfo  chkInfo;
  STaskExec        exec;
  SDataRange       dataRange;
  SStreamTaskId    historyTaskId;
  SStreamTaskId    streamTaskId;
  STaskTimestamp   tsInfo;
  SArray*          pReadyMsgList;  // SArray<SStreamChkptReadyInfo*>
  TdThreadMutex    lock;           // secure the operation of set task status and puting data into inputQ
  SArray*          pUpstreamInfoList;

  // output
  union {
    STaskDispatcherFixedEp fixedEpDispatcher;
    STaskDispatcherShuffle shuffleDispatcher;
    STaskSinkTb            tbSink;
    STaskSinkSma           smaSink;
    STaskSinkFetch         fetchSink;
  };
  SSinkTaskRecorder sinkRecorder;
  STokenBucket      tokenBucket;

  void*         launchTaskTimer;
  SMsgCb*       pMsgCb;  // msg handle
  SStreamState* pState;  // state backend
  SArray*       pRspMsgList;

  // the followings attributes don't be serialized
  int32_t             notReadyTasks;
  int32_t             numOfWaitingUpstream;
  int64_t             checkReqId;
  SArray*             checkReqIds;  // shuffle
  int32_t             refCnt;
  int64_t             checkpointingId;
  int32_t             checkpointAlignCnt;
  int32_t             checkpointNotReadyTasks;
  int32_t             transferStateAlignCnt;
  struct SStreamMeta* pMeta;
  SSHashObj*          pNameMap;
  char                reserve[256];
};

typedef struct SMetaHbInfo {
  tmr_h   hbTmr;
  int32_t stopFlag;
  int32_t tickCounter;
} SMetaHbInfo;

// meta
typedef struct SStreamMeta {
  char*         path;
  TDB*          db;
  TTB*          pTaskDb;
  TTB*          pCheckpointDb;
  SHashObj*     pTasks;
  SArray*       pTaskList;  // SArray<task_id*>
  void*         ahandle;
  TXN*          txn;
  FTaskExpand*  expandFunc;
  int32_t       vgId;
  int64_t       stage;
  SRWLatch      lock;
  int32_t       walScanCounter;
  void*         streamBackend;
  int64_t       streamBackendRid;
  SHashObj*     pTaskBackendUnique;
  TdThreadMutex backendMutex;
  SMetaHbInfo   hbInfo;
  int32_t       closedTask;
  int32_t       totalTasks;  // this value should be increased when a new task is added into the meta
  int32_t       chkptNotReadyTasks;
  int64_t       rid;

  int64_t  chkpId;
  SArray*  chkpSaved;
  SArray*  chkpInUse;
  int32_t  chkpCap;
  SRWLatch chkpDirLock;
  int32_t  pauseTaskNum;
} SStreamMeta;

int32_t tEncodeStreamEpInfo(SEncoder* pEncoder, const SStreamChildEpInfo* pInfo);
int32_t tDecodeStreamEpInfo(SDecoder* pDecoder, SStreamChildEpInfo* pInfo);

SStreamTask* tNewStreamTask(int64_t streamId, int8_t taskLevel, int8_t fillHistory, int64_t triggerParam,
                            SArray* pTaskList);
int32_t      tEncodeStreamTask(SEncoder* pEncoder, const SStreamTask* pTask);
int32_t      tDecodeStreamTask(SDecoder* pDecoder, SStreamTask* pTask);
void         tFreeStreamTask(SStreamTask* pTask);
int32_t      streamTaskInit(SStreamTask* pTask, SStreamMeta* pMeta, SMsgCb* pMsgCb, int64_t ver);

int32_t tDecodeStreamTaskChkInfo(SDecoder* pDecoder, SCheckpointInfo* pChkpInfo);
int32_t tDecodeStreamTaskId(SDecoder* pDecoder, SStreamTaskId* pTaskId);

int32_t streamTaskPutDataIntoInputQ(SStreamTask* pTask, SStreamQueueItem* pItem);
int32_t streamTaskPutDataIntoOutputQ(SStreamTask* pTask, SStreamDataBlock* pBlock);
int32_t streamTaskPutTranstateIntoInputQ(SStreamTask* pTask);
bool    streamQueueIsFull(const STaosQueue* pQueue, bool inputQ);

typedef struct {
  SMsgHead head;
  int64_t  streamId;
  int32_t  taskId;
} SStreamTaskRunReq;

typedef struct {
  int32_t type;
  int64_t stage;  // nodeId from upstream task
  int64_t streamId;
  int32_t taskId;
  int32_t srcVgId;
  int32_t upstreamTaskId;
  int32_t upstreamChildId;
  int32_t upstreamNodeId;
  int32_t blockNum;
  int64_t totalLen;
  SArray* dataLen;  // SArray<int32_t>
  SArray* data;     // SArray<SRetrieveTableRsp*>
} SStreamDispatchReq;

typedef struct {
  int64_t streamId;
  int32_t upstreamNodeId;
  int32_t upstreamTaskId;
  int32_t downstreamNodeId;
  int32_t downstreamTaskId;
  int8_t  inputStatus;
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
  int32_t oldStage;
  int8_t  status;
} SStreamTaskCheckRsp;

typedef struct {
  SMsgHead msgHead;
  int64_t  streamId;
  int32_t  taskId;
  int8_t   igUntreated;
} SStreamScanHistoryReq;

typedef struct {
  int64_t streamId;
  int32_t upstreamTaskId;
  int32_t downstreamTaskId;
  int32_t upstreamNodeId;
  int32_t childId;
} SStreamScanHistoryFinishReq, SStreamTransferReq;

int32_t tEncodeStreamScanHistoryFinishReq(SEncoder* pEncoder, const SStreamScanHistoryFinishReq* pReq);
int32_t tDecodeStreamScanHistoryFinishReq(SDecoder* pDecoder, SStreamScanHistoryFinishReq* pReq);

typedef struct {
  int64_t streamId;
  int64_t checkpointId;
  int32_t taskId;
  int32_t nodeId;
  SEpSet  mgmtEps;
  int32_t mnodeId;
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
int32_t tDecodeStreamCheckpointSourceRsp(SDecoder* pDecoder, SStreamCheckpointSourceRsp* pRsp);

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
  int64_t streamId;
  int32_t taskId;
  int32_t status;
} STaskStatusEntry;

typedef struct SStreamHbMsg {
  int32_t vgId;
  int32_t numOfTasks;
  SArray* pTaskStatus;  // SArray<SStreamTaskStatusEntry>
} SStreamHbMsg;

int32_t tEncodeStreamHbMsg(SEncoder* pEncoder, const SStreamHbMsg* pRsp);
int32_t tDecodeStreamHbMsg(SDecoder* pDecoder, SStreamHbMsg* pRsp);

typedef struct {
  int64_t streamId;
  int32_t upstreamTaskId;
  int32_t upstreamNodeId;
  int32_t downstreamId;
  int32_t downstreamNode;
} SStreamCompleteHistoryMsg;

int32_t tEncodeCompleteHistoryDataMsg(SEncoder* pEncoder, const SStreamCompleteHistoryMsg* pReq);
int32_t tDecodeCompleteHistoryDataMsg(SDecoder* pDecoder, SStreamCompleteHistoryMsg* pReq);

typedef struct SNodeUpdateInfo {
  int32_t nodeId;
  SEpSet  prevEp;
  SEpSet  newEp;
} SNodeUpdateInfo;

typedef struct SStreamTaskNodeUpdateMsg {
  int64_t streamId;
  int32_t taskId;
  SArray* pNodeList;  // SArray<SNodeUpdateInfo>
} SStreamTaskNodeUpdateMsg;

int32_t tEncodeStreamTaskUpdateMsg(SEncoder* pEncoder, const SStreamTaskNodeUpdateMsg* pMsg);
int32_t tDecodeStreamTaskUpdateMsg(SDecoder* pDecoder, SStreamTaskNodeUpdateMsg* pMsg);

typedef struct {
  int64_t streamId;
  int32_t downstreamTaskId;
  int32_t taskId;
} SStreamRecoverDownstreamReq;

typedef struct {
  int64_t streamId;
  int32_t downstreamTaskId;
  int32_t taskId;
  SArray* checkpointVer;  // SArray<SStreamCheckpointInfo>
} SStreamRecoverDownstreamRsp;

int32_t tEncodeStreamTaskCheckReq(SEncoder* pEncoder, const SStreamTaskCheckReq* pReq);
int32_t tDecodeStreamTaskCheckReq(SDecoder* pDecoder, SStreamTaskCheckReq* pReq);

int32_t tEncodeStreamTaskCheckRsp(SEncoder* pEncoder, const SStreamTaskCheckRsp* pRsp);
int32_t tDecodeStreamTaskCheckRsp(SDecoder* pDecoder, SStreamTaskCheckRsp* pRsp);

int32_t tEncodeStreamDispatchReq(SEncoder* pEncoder, const SStreamDispatchReq* pReq);
int32_t tDecodeStreamDispatchReq(SDecoder* pDecoder, SStreamDispatchReq* pReq);

int32_t tDecodeStreamRetrieveReq(SDecoder* pDecoder, SStreamRetrieveReq* pReq);
void    tDeleteStreamRetrieveReq(SStreamRetrieveReq* pReq);
void    tDeleteStreamDispatchReq(SStreamDispatchReq* pReq);

int32_t streamSetupScheduleTrigger(SStreamTask* pTask);

int32_t streamProcessRunReq(SStreamTask* pTask);
int32_t streamProcessDispatchMsg(SStreamTask* pTask, SStreamDispatchReq* pReq, SRpcMsg* pMsg, bool exec);
int32_t streamProcessDispatchRsp(SStreamTask* pTask, SStreamDispatchRsp* pRsp, int32_t code);

int32_t             streamProcessRetrieveReq(SStreamTask* pTask, SStreamRetrieveReq* pReq, SRpcMsg* pMsg);
SStreamChildEpInfo* streamTaskGetUpstreamTaskEpInfo(SStreamTask* pTask, int32_t taskId);

void    streamTaskInputFail(SStreamTask* pTask);
int32_t streamTryExec(SStreamTask* pTask);
int32_t streamSchedExec(SStreamTask* pTask);
bool    streamTaskShouldStop(const SStreamStatus* pStatus);
bool    streamTaskShouldPause(const SStreamStatus* pStatus);
bool    streamTaskIsIdle(const SStreamTask* pTask);

void    initRpcMsg(SRpcMsg* pMsg, int32_t msgType, void* pCont, int32_t contLen);

char* createStreamTaskIdStr(int64_t streamId, int32_t taskId);

// recover and fill history
void    streamTaskCheckDownstream(SStreamTask* pTask);
int32_t streamTaskLaunchScanHistory(SStreamTask* pTask);
int32_t streamTaskCheckStatus(SStreamTask* pTask, int32_t upstreamTaskId, int32_t vgId, int64_t stage);
int32_t streamTaskUpdateEpsetInfo(SStreamTask* pTask, SArray* pNodeList);
void    streamTaskResetUpstreamStageInfo(SStreamTask* pTask);

int32_t streamTaskStop(SStreamTask* pTask);
int32_t streamSendCheckRsp(const SStreamMeta* pMeta, const SStreamTaskCheckReq* pReq, SStreamTaskCheckRsp* pRsp,
                           SRpcHandleInfo* pRpcInfo, int32_t taskId);
int32_t streamProcessCheckRsp(SStreamTask* pTask, const SStreamTaskCheckRsp* pRsp);
int32_t streamLaunchFillHistoryTask(SStreamTask* pTask);
int32_t streamTaskScanHistoryDataComplete(SStreamTask* pTask);
int32_t streamStartScanHistoryAsync(SStreamTask* pTask, int8_t igUntreated);
bool    streamHistoryTaskSetVerRangeStep2(SStreamTask* pTask, int64_t latestVer);
int32_t streamQueueGetNumOfItems(const SStreamQueue* pQueue);

// common
int32_t     streamRestoreParam(SStreamTask* pTask);
int32_t     streamSetStatusNormal(SStreamTask* pTask);
const char* streamGetTaskStatusStr(int32_t status);
void        streamTaskPause(SStreamTask* pTask, SStreamMeta* pMeta);
void        streamTaskResume(SStreamTask* pTask, SStreamMeta* pMeta);
void        streamTaskHalt(SStreamTask* pTask);
void        streamTaskResumeFromHalt(SStreamTask* pTask);
void        streamTaskDisablePause(SStreamTask* pTask);
void        streamTaskEnablePause(SStreamTask* pTask);
int32_t     streamTaskSetUpstreamInfo(SStreamTask* pTask, const SStreamTask* pUpstreamTask);
void        streamTaskUpdateUpstreamInfo(SStreamTask* pTask, int32_t nodeId, const SEpSet* pEpSet);
void        streamTaskUpdateDownstreamInfo(SStreamTask* pTask, int32_t nodeId, const SEpSet* pEpSet);
void        streamTaskSetFixedDownstreamInfo(SStreamTask* pTask, const SStreamTask* pDownstreamTask);
int32_t     streamTaskReleaseState(SStreamTask* pTask);
int32_t     streamTaskReloadState(SStreamTask* pTask);
void        streamTaskCloseUpstreamInput(SStreamTask* pTask, int32_t taskId);
void        streamTaskOpenAllUpstreamInput(SStreamTask* pTask);

// source level
int32_t streamSetParamForStreamScannerStep1(SStreamTask* pTask, SVersionRange* pVerRange, STimeWindow* pWindow);
int32_t streamSetParamForStreamScannerStep2(SStreamTask* pTask, SVersionRange* pVerRange, STimeWindow* pWindow);
int32_t streamScanHistoryData(SStreamTask* pTask);
int32_t streamDispatchScanHistoryFinishMsg(SStreamTask* pTask);

// agg level
int32_t streamProcessScanHistoryFinishReq(SStreamTask* pTask, SStreamScanHistoryFinishReq* pReq,
                                          SRpcHandleInfo* pRpcInfo);
int32_t streamProcessScanHistoryFinishRsp(SStreamTask* pTask);

// stream task meta
void         streamMetaInit();
void         streamMetaCleanup();
SStreamMeta* streamMetaOpen(const char* path, void* ahandle, FTaskExpand expandFunc, int32_t vgId, int64_t stage);
void         streamMetaClose(SStreamMeta* streamMeta);
int32_t      streamMetaSaveTask(SStreamMeta* pMeta, SStreamTask* pTask);  // save to stream meta store
int32_t      streamMetaRemoveTask(SStreamMeta* pMeta, int64_t* pKey);
int32_t      streamMetaRegisterTask(SStreamMeta* pMeta, int64_t ver, SStreamTask* pTask, bool* pAdded);
int32_t      streamMetaUnregisterTask(SStreamMeta* pMeta, int64_t streamId, int32_t taskId);
int32_t      streamMetaGetNumOfTasks(SStreamMeta* pMeta);
int32_t      streamMetaGetNumOfStreamTasks(SStreamMeta* pMeta);
SStreamTask* streamMetaAcquireTask(SStreamMeta* pMeta, int64_t streamId, int32_t taskId);
void         streamMetaReleaseTask(SStreamMeta* pMeta, SStreamTask* pTask);
int32_t      streamMetaReopen(SStreamMeta* pMeta, int64_t chkpId);
int32_t      streamMetaCommit(SStreamMeta* pMeta);
int32_t      streamMetaLoadAllTasks(SStreamMeta* pMeta);
void         streamMetaNotifyClose(SStreamMeta* pMeta);

// checkpoint
int32_t streamProcessCheckpointSourceReq(SStreamTask* pTask, SStreamCheckpointSourceReq* pReq);
int32_t streamProcessCheckpointReadyMsg(SStreamTask* pTask);

int32_t streamAlignTransferState(SStreamTask* pTask);

int32_t streamAddCheckpointSourceRspMsg(SStreamCheckpointSourceReq* pReq, SRpcHandleInfo* pRpcInfo, SStreamTask* pTask,
                                        int8_t isSucceed);
int32_t buildCheckpointSourceRsp(SStreamCheckpointSourceReq* pReq, SRpcHandleInfo* pRpcInfo, SRpcMsg* pMsg,
                                 int8_t isSucceed);

#ifdef __cplusplus
}
#endif

#endif /* ifndef _STREAM_H_ */
