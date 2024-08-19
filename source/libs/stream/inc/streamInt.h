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

#ifndef _STREAM_INC_H_
#define _STREAM_INC_H_

#include "executor.h"
#include "query.h"
#include "streamBackendRocksdb.h"
#include "trpc.h"
#include "tstream.h"

#ifdef __cplusplus
extern "C" {
#endif

#define CHECK_RSP_CHECK_INTERVAL           300
#define LAUNCH_HTASK_INTERVAL              100
#define WAIT_FOR_MINIMAL_INTERVAL          100.00
#define MAX_RETRY_LAUNCH_HISTORY_TASK      40
#define RETRY_LAUNCH_INTERVAL_INC_RATE     1.2
#define MAX_BLOCK_NAME_NUM                 1024
#define DISPATCH_RETRY_INTERVAL_MS         300
#define META_HB_CHECK_INTERVAL             200
#define META_HB_SEND_IDLE_COUNTER          25  // send hb every 5 sec
#define STREAM_TASK_KEY_LEN                ((sizeof(int64_t)) << 1)
#define STREAM_TASK_QUEUE_CAPACITY         20480
#define STREAM_TASK_QUEUE_CAPACITY_IN_SIZE (30)

// clang-format off
#define stFatal(...) do { if (stDebugFlag & DEBUG_FATAL) { taosPrintLog("STM FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}     while(0)
#define stError(...) do { if (stDebugFlag & DEBUG_ERROR) { taosPrintLog("STM ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}     while(0)
#define stWarn(...)  do { if (stDebugFlag & DEBUG_WARN)  { taosPrintLog("STM WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}       while(0)
#define stInfo(...)  do { if (stDebugFlag & DEBUG_INFO)  { taosPrintLog("STM ", DEBUG_INFO, 255, __VA_ARGS__); }}            while(0)
#define stDebug(...) do { if (stDebugFlag & DEBUG_DEBUG) { taosPrintLog("STM ", DEBUG_DEBUG, stDebugFlag, __VA_ARGS__); }} while(0)
#define stTrace(...) do { if (stDebugFlag & DEBUG_TRACE) { taosPrintLog("STM ", DEBUG_TRACE, stDebugFlag, __VA_ARGS__); }} while(0)
// clang-format on

typedef struct SStreamTmrInfo {
  int32_t activeCounter;  // make sure only launch one checkpoint trigger check tmr
  tmr_h   tmrHandle;
  int64_t launchChkptId;
  int8_t  isActive;
} SStreamTmrInfo;

struct SActiveCheckpointInfo {
  TdThreadMutex  lock;
  int32_t        transId;
  int64_t        firstRecvTs;  // first time to recv checkpoint trigger info
  int64_t        activeId;     // current active checkpoint id
  int64_t        failedId;
  bool           dispatchTrigger;
  SArray*        pDispatchTriggerList;  // SArray<STaskTriggerSendInfo>
  SArray*        pReadyMsgList;         // SArray<STaskCheckpointReadyInfo*>
  int8_t         allUpstreamTriggerRecv;
  SArray*        pCheckpointReadyRecvList;  // SArray<STaskDownstreamReadyInfo>
  SStreamTmrInfo chkptTriggerMsgTmr;
  SStreamTmrInfo chkptReadyMsgTmr;
};

int32_t streamCleanBeforeQuitTmr(SStreamTmrInfo* pInfo, SStreamTask* pTask);

typedef struct {
  int8_t       type;
  SSDataBlock* pBlock;
} SStreamTrigger;

typedef struct SStreamContinueExecInfo {
  SEpSet  epset;
  int32_t taskId;
  SRpcMsg msg;
} SStreamContinueExecInfo;

typedef struct {
  int64_t streamId;
  int64_t taskId;
  int64_t chkpId;
  char*   dbPrefixPath;
} SStreamTaskSnap;

struct STokenBucket {
  int32_t numCapacity;         // total capacity, available token per second
  int32_t numOfToken;          // total available tokens
  int32_t numRate;             // number of token per second
  double  quotaCapacity;       // available capacity for maximum input size, KiloBytes per Second
  double  quotaRemain;         // not consumed bytes per second
  double  quotaRate;           // number of token per second
  int64_t tokenFillTimestamp;  // fill timestamp
  int64_t quotaFillTimestamp;  // fill timestamp
};

typedef struct {
  int32_t upstreamTaskId;
  SEpSet  upstreamNodeEpset;
  int32_t upstreamNodeId;
  int32_t transId;
  int32_t childId;
  SRpcMsg msg;  // for mnode checkpoint-source rsp
  int64_t checkpointId;
  int64_t recvTs;
  int32_t sendCompleted;
} STaskCheckpointReadyInfo;

typedef struct {
  int64_t sendTs;
  int64_t recvTs;
  bool    recved;
  int32_t nodeId;
  int32_t taskId;
} STaskTriggerSendInfo;

typedef struct {
  int32_t nodeId;
  int32_t status;
  int64_t sendTs;
  int64_t rspTs;
  int32_t retryCount;
} SDispatchEntry;

typedef struct {
  int64_t streamId;
  int64_t recvTs;
  int32_t downstreamNodeId;
  int32_t downstreamTaskId;
  int64_t checkpointId;
  int32_t transId;
} STaskDownstreamReadyInfo;

struct SStreamQueue {
  STaosQueue* pQueue;
  STaosQall*  qall;
  void*       qItem;
  int8_t      status;
};

struct SStreamQueueItem {
  int8_t type;
};

typedef enum {
  EXEC_CONTINUE = 0x0,
  EXEC_AFTER_IDLE = 0x1,
} EExtractDataCode;

typedef enum ECHECKPOINT_BACKUP_TYPE {
  DATA_UPLOAD_DISABLE = -1,
  DATA_UPLOAD_S3 = 0,
  DATA_UPLOAD_RSYNC = 1,
} ECHECKPOINT_BACKUP_TYPE;

extern void*   streamTimer;
extern int32_t streamBackendId;
extern int32_t streamBackendCfWrapperId;
extern int32_t taskDbWrapperId;

int32_t streamTimerInit();
void    streamTimerCleanUp();

void initRpcMsg(SRpcMsg* pMsg, int32_t msgType, void* pCont, int32_t contLen);

void    streamStartMonitorDispatchData(SStreamTask* pTask, int64_t waitDuration);
int32_t streamDispatchStreamBlock(SStreamTask* pTask);
void    destroyDispatchMsg(SStreamDispatchReq* pReq, int32_t numOfVgroups);
void    clearBufferedDispatchMsg(SStreamTask* pTask);

int32_t           streamProcessCheckpointTriggerBlock(SStreamTask* pTask, SStreamDataBlock* pBlock);
SStreamDataBlock* createStreamBlockFromDispatchMsg(const SStreamDispatchReq* pReq, int32_t blockType, int32_t srcVg);
SStreamDataBlock* createStreamBlockFromResults(SStreamQueueItem* pItem, SStreamTask* pTask, int64_t resultSize,
                                               SArray* pRes);
void              destroyStreamDataBlock(SStreamDataBlock* pBlock);

int32_t streamRetrieveReqToData(const SStreamRetrieveReq* pReq, SStreamDataBlock* pData, const char* idstr);
int32_t streamBroadcastToUpTasks(SStreamTask* pTask, const SSDataBlock* pBlock);

int32_t streamSendCheckMsg(SStreamTask* pTask, const SStreamTaskCheckReq* pReq, int32_t nodeId, SEpSet* pEpSet);

int32_t streamAddCheckpointReadyMsg(SStreamTask* pTask, int32_t srcTaskId, int32_t index, int64_t checkpointId);
int32_t streamTaskSendCheckpointReadyMsg(SStreamTask* pTask);
int32_t streamTaskSendCheckpointSourceRsp(SStreamTask* pTask);
int32_t streamTaskSendCheckpointReq(SStreamTask* pTask);

void    streamTaskSetFailedCheckpointId(SStreamTask* pTask);
int32_t streamTaskGetNumOfDownstream(const SStreamTask* pTask);
int32_t streamTaskGetNumOfUpstream(const SStreamTask* pTask);
int32_t streamTaskInitTokenBucket(STokenBucket* pBucket, int32_t numCap, int32_t numRate, float quotaRate, const char*);
STaskId streamTaskGetTaskId(const SStreamTask* pTask);
void    streamTaskInitForLaunchHTask(SHistoryTaskInfo* pInfo);
void    streamTaskSetRetryInfoForLaunch(SHistoryTaskInfo* pInfo);
int32_t streamTaskResetTimewindowFilter(SStreamTask* pTask);
void    streamTaskClearActiveInfo(SActiveCheckpointInfo* pInfo);

void              streamClearChkptReadyMsg(SActiveCheckpointInfo* pActiveInfo);
EExtractDataCode  streamTaskGetDataFromInputQ(SStreamTask* pTask, SStreamQueueItem** pInput, int32_t* numOfBlocks,
                                              int32_t* blockSize);
int32_t           streamQueueItemGetSize(const SStreamQueueItem* pItem);
void              streamQueueItemIncSize(const SStreamQueueItem* pItem, int32_t size);
const char*       streamQueueItemGetTypeStr(int32_t type);
SStreamQueueItem* streamQueueMergeQueueItem(SStreamQueueItem* dst, SStreamQueueItem* pElem);
int32_t           streamTransferStatePrepare(SStreamTask* pTask);

SStreamQueue* streamQueueOpen(int64_t cap);
void          streamQueueClose(SStreamQueue* pQueue, int32_t taskId);
void          streamQueueProcessSuccess(SStreamQueue* queue);
void          streamQueueProcessFail(SStreamQueue* queue);
void*         streamQueueNextItem(SStreamQueue* pQueue);
void          streamFreeQitem(SStreamQueueItem* data);
int32_t       streamQueueGetItemSize(const SStreamQueue* pQueue);

void streamMetaRemoveDB(void* arg, char* key);

ECHECKPOINT_BACKUP_TYPE streamGetCheckpointBackupType();

int32_t streamTaskDownloadCheckpointData(const char* id, char* path);
int32_t streamTaskOnNormalTaskReady(SStreamTask* pTask);
int32_t streamTaskOnScanHistoryTaskReady(SStreamTask* pTask);

int32_t initCheckpointReadyInfo(STaskCheckpointReadyInfo* pReadyInfo, int32_t upstreamNodeId, int32_t upstreamTaskId,
                                int32_t childId, SEpSet* pEpset, int64_t checkpointId);
int32_t initCheckpointReadyMsg(SStreamTask* pTask, int32_t upstreamNodeId, int32_t upstreamTaskId, int32_t childId,
                               int64_t checkpointId, SRpcMsg* pMsg);

typedef int32_t (*__stream_async_exec_fn_t)(void* param);

int32_t streamMetaAsyncExec(SStreamMeta* pMeta, __stream_async_exec_fn_t fn, void* param, int32_t* code);
void    flushStateDataInExecutor(SStreamTask* pTask, SStreamQueueItem* pCheckpointBlock);

#ifdef __cplusplus
}
#endif

#endif /* ifndef _STREAM_INC_H_ */
