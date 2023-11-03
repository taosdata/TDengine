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
#include "tstream.h"
#include "streamBackendRocksdb.h"
#include "trpc.h"

#ifdef __cplusplus
extern "C" {
#endif

#define CHECK_DOWNSTREAM_INTERVAL      100
#define LAUNCH_HTASK_INTERVAL          100
#define WAIT_FOR_MINIMAL_INTERVAL      100.00
#define MAX_RETRY_LAUNCH_HISTORY_TASK  40
#define RETRY_LAUNCH_INTERVAL_INC_RATE 1.2

#define MAX_BLOCK_NAME_NUM             1024
#define DISPATCH_RETRY_INTERVAL_MS     300
#define MAX_CONTINUE_RETRY_COUNT       5

#define META_HB_CHECK_INTERVAL         200
#define META_HB_SEND_IDLE_COUNTER      25  // send hb every 5 sec
#define STREAM_TASK_KEY_LEN            ((sizeof(int64_t)) << 1)

#define STREAM_TASK_QUEUE_CAPACITY         20480
#define STREAM_TASK_QUEUE_CAPACITY_IN_SIZE (30)

// clang-format off
#define stFatal(...) do { if (stDebugFlag & DEBUG_FATAL) { taosPrintLog("STM FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}     while(0)
#define stError(...) do { if (stDebugFlag & DEBUG_ERROR) { taosPrintLog("STM ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}     while(0)
#define stWarn(...)  do { if (stDebugFlag & DEBUG_WARN)  { taosPrintLog("STM WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}       while(0)
#define stInfo(...)  do { if (stDebugFlag & DEBUG_INFO)  { taosPrintLog("STM ", DEBUG_INFO, 255, __VA_ARGS__); }}            while(0)
#define stDebug(...) do { if (stDebugFlag & DEBUG_DEBUG) { taosPrintLog("STM ", DEBUG_DEBUG, tqDebugFlag, __VA_ARGS__); }} while(0)
#define stTrace(...) do { if (stDebugFlag & DEBUG_TRACE) { taosPrintLog("STM ", DEBUG_TRACE, tqDebugFlag, __VA_ARGS__); }} while(0)
// clang-format on

typedef struct {
  int8_t       type;
  SSDataBlock* pBlock;
} SStreamTrigger;

typedef struct SStreamGlobalEnv {
  int8_t inited;
  void*  timer;
} SStreamGlobalEnv;

typedef struct SStreamContinueExecInfo {
  SEpSet  epset;
  int32_t taskId;
  SRpcMsg msg;
} SStreamContinueExecInfo;

struct STokenBucket {
  int32_t numCapacity;    // total capacity, available token per second
  int32_t numOfToken;     // total available tokens
  int32_t numRate;        // number of token per second
  double  quotaCapacity;  // available capacity for maximum input size, KiloBytes per Second
  double  quotaRemain;    // not consumed bytes per second
  double  quotaRate;      // number of token per second
  int64_t fillTimestamp;  // fill timestamp
};

struct SStreamQueue {
  STaosQueue* pQueue;
  STaosQall*  qall;
  void*       qItem;
  int8_t      status;
};

extern SStreamGlobalEnv streamEnv;
extern int32_t streamBackendId;
extern int32_t streamBackendCfWrapperId;

void        streamRetryDispatchData(SStreamTask* pTask, int64_t waitDuration);
int32_t     streamDispatchStreamBlock(SStreamTask* pTask);
void        destroyDispatchMsg(SStreamDispatchReq* pReq, int32_t numOfVgroups);
int32_t     getNumOfDispatchBranch(SStreamTask* pTask);

int32_t           streamProcessCheckpointBlock(SStreamTask* pTask, SStreamDataBlock* pBlock);
SStreamDataBlock* createStreamBlockFromDispatchMsg(const SStreamDispatchReq* pReq, int32_t blockType, int32_t srcVg);
SStreamDataBlock* createStreamBlockFromResults(SStreamQueueItem* pItem, SStreamTask* pTask, int64_t resultSize,
                                               SArray* pRes);
void              destroyStreamDataBlock(SStreamDataBlock* pBlock);

int32_t streamRetrieveReqToData(const SStreamRetrieveReq* pReq, SStreamDataBlock* pData);
int32_t streamBroadcastToChildren(SStreamTask* pTask, const SSDataBlock* pBlock);

int32_t tEncodeStreamRetrieveReq(SEncoder* pEncoder, const SStreamRetrieveReq* pReq);

int32_t streamSaveAllTaskStatus(SStreamMeta* pMeta, int64_t checkpointId);
int32_t streamTaskBuildCheckpoint(SStreamTask* pTask);
int32_t streamSendCheckMsg(SStreamTask* pTask, const SStreamTaskCheckReq* pReq, int32_t nodeId, SEpSet* pEpSet);

int32_t streamAddCheckpointReadyMsg(SStreamTask* pTask, int32_t srcTaskId, int32_t index, int64_t checkpointId);
int32_t streamTaskSendCheckpointReadyMsg(SStreamTask* pTask);
int32_t streamTaskSendCheckpointSourceRsp(SStreamTask* pTask);
int32_t streamTaskGetNumOfDownstream(const SStreamTask* pTask);

int32_t     streamTaskGetDataFromInputQ(SStreamTask* pTask, SStreamQueueItem** pInput, int32_t* numOfBlocks, int32_t* blockSize);
int32_t     streamQueueItemGetSize(const SStreamQueueItem* pItem);
void        streamQueueItemIncSize(const SStreamQueueItem* pItem, int32_t size);
const char* streamQueueItemGetTypeStr(int32_t type);

SStreamQueueItem* streamMergeQueueItem(SStreamQueueItem* dst, SStreamQueueItem* pElem);

int32_t streamTaskBuildScanhistoryRspMsg(SStreamTask* pTask, SStreamScanHistoryFinishReq* pReq, void** pBuffer, int32_t* pLen);
int32_t streamAddEndScanHistoryMsg(SStreamTask* pTask, SRpcHandleInfo* pRpcInfo, SStreamScanHistoryFinishReq* pReq);
int32_t streamNotifyUpstreamContinue(SStreamTask* pTask);
int32_t streamTaskFillHistoryFinished(SStreamTask* pTask);
int32_t streamTransferStateToStreamTask(SStreamTask* pTask);

int32_t streamTaskInitTokenBucket(STokenBucket* pBucket, int32_t numCap, int32_t numRate, float quotaRate);
STaskId streamTaskExtractKey(const SStreamTask* pTask);
void    streamTaskInitForLaunchHTask(SHistoryTaskInfo* pInfo);
void    streamTaskSetRetryInfoForLaunch(SHistoryTaskInfo* pInfo);

void    streamMetaResetStartInfo(STaskStartInfo* pMeta);

SStreamQueue* streamQueueOpen(int64_t cap);
void          streamQueueClose(SStreamQueue* pQueue, int32_t taskId);
void          streamQueueProcessSuccess(SStreamQueue* queue);
void          streamQueueProcessFail(SStreamQueue* queue);
void*         streamQueueNextItem(SStreamQueue* pQueue);
void          streamFreeQitem(SStreamQueueItem* data);
int32_t       streamQueueGetItemSize(const SStreamQueue* pQueue);

#ifdef __cplusplus
}
#endif

#endif /* ifndef _STREAM_INC_H_ */
