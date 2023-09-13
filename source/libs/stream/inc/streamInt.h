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

#define ONE_MB_F       (1048576.0)
#define SIZE_IN_MB(_v) ((_v) / ONE_MB_F)

typedef struct {
  int8_t inited;
  void*  timer;
} SStreamGlobalEnv;

typedef struct {
  SEpSet  epset;
  int32_t taskId;
  SRpcMsg msg;
} SStreamContinueExecInfo;

extern SStreamGlobalEnv streamEnv;
extern int32_t streamBackendId;
extern int32_t streamBackendCfWrapperId;

const char* streamGetBlockTypeStr(int32_t type);
void        streamRetryDispatchStreamBlock(SStreamTask* pTask, int64_t waitDuration);
int32_t     streamDispatchStreamBlock(SStreamTask* pTask);

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
int32_t streamDispatchCheckMsg(SStreamTask* pTask, const SStreamTaskCheckReq* pReq, int32_t nodeId, SEpSet* pEpSet);

int32_t streamAddCheckpointReadyMsg(SStreamTask* pTask, int32_t srcTaskId, int32_t index, int64_t checkpointId);
int32_t streamTaskSendCheckpointReadyMsg(SStreamTask* pTask);
int32_t streamTaskSendCheckpointSourceRsp(SStreamTask* pTask);
int32_t streamTaskGetNumOfDownstream(const SStreamTask* pTask);

int32_t streamTaskGetDataFromInputQ(SStreamTask* pTask, SStreamQueueItem** pInput, int32_t* numOfBlocks);
SStreamQueueItem* streamMergeQueueItem(SStreamQueueItem* dst, SStreamQueueItem* pElem);

int32_t streamTaskBuildScanhistoryRspMsg(SStreamTask* pTask, SStreamScanHistoryFinishReq* pReq, void** pBuffer, int32_t* pLen);
int32_t streamAddEndScanHistoryMsg(SStreamTask* pTask, SRpcHandleInfo* pRpcInfo, SStreamScanHistoryFinishReq* pReq);
int32_t streamNotifyUpstreamContinue(SStreamTask* pTask);
int32_t streamTaskFillHistoryFinished(SStreamTask* pTask);
int32_t streamTransferStateToStreamTask(SStreamTask* pTask);

int32_t streamTaskInitTokenBucket(STokenBucket* pBucket, int32_t cap, int32_t rate);

#ifdef __cplusplus
}
#endif

#endif /* ifndef _STREAM_INC_H_ */
