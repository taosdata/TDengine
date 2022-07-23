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

#include "executor.h"
#include "os.h"
#include "query.h"
#include "tdatablock.h"
#include "tmsg.h"
#include "tmsgcb.h"
#include "tqueue.h"
#include "trpc.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifndef _STREAM_H_
#define _STREAM_H_

typedef struct SStreamTask SStreamTask;

enum {
  STREAM_STATUS__NORMAL = 0,
  STREAM_STATUS__RECOVER,
};

enum {
  TASK_STATUS__NORMAL = 0,
  TASK_STATUS__DROPPING,
  TASK_STATUS__FAIL,
  TASK_STATUS__STOP,
  TASK_STATUS__PREPARE_RECOVER,
  TASK_STATUS__RECOVERING,
};

enum {
  TASK_EXEC_STATUS__IDLE = 1,
  TASK_EXEC_STATUS__EXECUTING,
  TASK_EXEC_STATUS__CLOSING,
};

enum {
  TASK_INPUT_STATUS__NORMAL = 1,
  TASK_INPUT_STATUS__BLOCKED,
  TASK_INPUT_STATUS__RECOVER,
  TASK_INPUT_STATUS__PROCESSING,
  TASK_INPUT_STATUS__STOP,
  TASK_INPUT_STATUS__FAILED,
};

enum {
  TASK_OUTPUT_STATUS__NORMAL = 1,
  TASK_OUTPUT_STATUS__WAIT,
  TASK_OUTPUT_STATUS__BLOCKED,
};

typedef struct {
  int8_t type;
} SStreamQueueItem;

typedef struct {
  int8_t      type;
  int64_t     ver;
  int32_t*    dataRef;
  SSubmitReq* data;
} SStreamDataSubmit;

typedef struct {
  int8_t  type;
  int64_t ver;
  SArray* dataRefs;  // SArray<int32_t*>
  SArray* reqs;      // SArray<SSubmitReq*>
} SStreamMergedSubmit;

typedef struct {
  int8_t type;

  int32_t srcVgId;
  int32_t childId;
  int64_t sourceVer;
  int64_t reqId;

  SArray* blocks;  // SArray<SSDataBlock*>
} SStreamDataBlock;

typedef struct {
  int8_t type;
} SStreamCheckpoint;

typedef struct {
  int8_t       type;
  SSDataBlock* pBlock;
} SStreamTrigger;

enum {
  STREAM_QUEUE__SUCESS = 1,
  STREAM_QUEUE__FAILED,
  STREAM_QUEUE__PROCESSING,
};

typedef struct {
  STaosQueue* queue;
  STaosQall*  qall;
  void*       qItem;
  int8_t      status;
} SStreamQueue;

int32_t streamInit();
void    streamCleanUp();

SStreamQueue* streamQueueOpen();
void          streamQueueClose(SStreamQueue* queue);

static FORCE_INLINE void streamQueueProcessSuccess(SStreamQueue* queue) {
  ASSERT(atomic_load_8(&queue->status) == STREAM_QUEUE__PROCESSING);
  queue->qItem = NULL;
  atomic_store_8(&queue->status, STREAM_QUEUE__SUCESS);
}

static FORCE_INLINE void streamQueueProcessFail(SStreamQueue* queue) {
  ASSERT(atomic_load_8(&queue->status) == STREAM_QUEUE__PROCESSING);
  atomic_store_8(&queue->status, STREAM_QUEUE__FAILED);
}

static FORCE_INLINE void* streamQueueCurItem(SStreamQueue* queue) { return queue->qItem; }

static FORCE_INLINE void* streamQueueNextItem(SStreamQueue* queue) {
  int8_t dequeueFlag = atomic_exchange_8(&queue->status, STREAM_QUEUE__PROCESSING);
  if (dequeueFlag == STREAM_QUEUE__FAILED) {
    ASSERT(queue->qItem != NULL);
    return streamQueueCurItem(queue);
  } else {
    queue->qItem = NULL;
    taosGetQitem(queue->qall, &queue->qItem);
    if (queue->qItem == NULL) {
      taosReadAllQitems(queue->queue, queue->qall);
      taosGetQitem(queue->qall, &queue->qItem);
    }
    return streamQueueCurItem(queue);
  }
}

SStreamDataSubmit* streamDataSubmitNew(SSubmitReq* pReq);

void streamDataSubmitRefDec(SStreamDataSubmit* pDataSubmit);

SStreamDataSubmit* streamSubmitRefClone(SStreamDataSubmit* pSubmit);

typedef struct {
  char* qmsg;
  // followings are not applicable to encoder and decoder
  void* executor;
} STaskExec;

typedef struct {
  int32_t taskId;
  int32_t nodeId;
  SEpSet  epSet;
} STaskDispatcherFixedEp;

typedef struct {
  // int8_t  hashMethod;
  char      stbFullName[TSDB_TABLE_FNAME_LEN];
  SUseDbRsp dbInfo;
} STaskDispatcherShuffle;

typedef void FTbSink(SStreamTask* pTask, void* vnode, int64_t ver, void* data);

typedef struct {
  int64_t         stbUid;
  char            stbFullName[TSDB_TABLE_FNAME_LEN];
  SSchemaWrapper* pSchemaWrapper;
  // not applicable to encoder and decoder
  void*     vnode;
  FTbSink*  tbSinkFunc;
  STSchema* pTSchema;
  SHashObj* pHash;  // groupId to tbuid
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

enum {
  TASK_SOURCE__SCAN = 1,
  TASK_SOURCE__PIPE,
  TASK_SOURCE__MERGE,
};

enum {
  TASK_EXEC__NONE = 1,
  TASK_EXEC__PIPE,
  TASK_EXEC__MERGE,
};

enum {
  TASK_DISPATCH__NONE = 1,
  TASK_DISPATCH__FIXED,
  TASK_DISPATCH__SHUFFLE,
};

enum {
  TASK_SINK__NONE = 1,
  TASK_SINK__TABLE,
  TASK_SINK__SMA,
  TASK_SINK__FETCH,
};

enum {
  TASK_INPUT_TYPE__SUMBIT_BLOCK = 1,
  TASK_INPUT_TYPE__DATA_BLOCK,
};

enum {
  TASK_TRIGGER_STATUS__IN_ACTIVE = 1,
  TASK_TRIGGER_STATUS__ACTIVE,
};

typedef struct {
  int32_t nodeId;
  int32_t childId;
  int32_t taskId;
  int64_t checkpointVer;
  int64_t processedVer;
  SEpSet  epSet;
} SStreamChildEpInfo;

typedef struct SStreamTask {
  int64_t streamId;
  int32_t taskId;
  int8_t  isDataScan;
  int8_t  execType;
  int8_t  sinkType;
  int8_t  dispatchType;
  int8_t  isStreamDistributed;
  int16_t dispatchMsgType;

  int8_t taskStatus;
  int8_t execStatus;

  // node info
  int32_t selfChildId;
  int32_t nodeId;
  SEpSet  epSet;

  // used for semi or single task,
  // while final task should have processedVer for each child
  int64_t recoverSnapVer;
  int64_t startVer;
  int64_t checkpointVer;
  int64_t processedVer;
  int32_t numOfVgroups;

  // children info
  SArray* childEpInfo;  // SArray<SStreamChildEpInfo*>

  // exec
  STaskExec exec;

  // TODO: unify sink and dispatch

  //  local sink
  union {
    STaskSinkTb    tbSink;
    STaskSinkSma   smaSink;
    STaskSinkFetch fetchSink;
  };

  // remote dispatcher
  union {
    STaskDispatcherFixedEp fixedEpDispatcher;
    STaskDispatcherShuffle shuffleDispatcher;
  };

  int8_t inputStatus;
  int8_t outputStatus;

  SStreamQueue* inputQueue;
  SStreamQueue* outputQueue;

  // trigger
  int8_t  triggerStatus;
  int64_t triggerParam;
  void*   timer;

  // application storage
  // void* ahandle;

  // msg handle
  SMsgCb* pMsgCb;
} SStreamTask;

int32_t tEncodeStreamEpInfo(SEncoder* pEncoder, const SStreamChildEpInfo* pInfo);
int32_t tDecodeStreamEpInfo(SDecoder* pDecoder, SStreamChildEpInfo* pInfo);

SStreamTask* tNewSStreamTask(int64_t streamId);
int32_t      tEncodeSStreamTask(SEncoder* pEncoder, const SStreamTask* pTask);
int32_t      tDecodeSStreamTask(SDecoder* pDecoder, SStreamTask* pTask);
void         tFreeSStreamTask(SStreamTask* pTask);

static FORCE_INLINE int32_t streamTaskInput(SStreamTask* pTask, SStreamQueueItem* pItem) {
#if 0
  while (1) {
    int8_t inputStatus =
        atomic_val_compare_exchange_8(&pTask->inputStatus, TASK_INPUT_STATUS__NORMAL, TASK_INPUT_STATUS__PROCESSING);
    if (inputStatus == TASK_INPUT_STATUS__NORMAL) {
      break;
    }
    ASSERT(0);
  }
#endif

  if (pItem->type == STREAM_INPUT__DATA_SUBMIT) {
    SStreamDataSubmit* pSubmitClone = streamSubmitRefClone((SStreamDataSubmit*)pItem);
    if (pSubmitClone == NULL) {
      qDebug("task %d %p submit enqueue failed since out of memory", pTask->taskId, pTask);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      atomic_store_8(&pTask->inputStatus, TASK_INPUT_STATUS__FAILED);
      return -1;
    }
    qDebug("task %d %p submit enqueue %p %p %p", pTask->taskId, pTask, pItem, pSubmitClone, pSubmitClone->data);
    taosWriteQitem(pTask->inputQueue->queue, pSubmitClone);
    // qStreamInput(pTask->exec.executor, pSubmitClone);
  } else if (pItem->type == STREAM_INPUT__DATA_BLOCK || pItem->type == STREAM_INPUT__DATA_RETRIEVE) {
    taosWriteQitem(pTask->inputQueue->queue, pItem);
    // qStreamInput(pTask->exec.executor, pItem);
  } else if (pItem->type == STREAM_INPUT__CHECKPOINT) {
    taosWriteQitem(pTask->inputQueue->queue, pItem);
    // qStreamInput(pTask->exec.executor, pItem);
  } else if (pItem->type == STREAM_INPUT__GET_RES) {
    taosWriteQitem(pTask->inputQueue->queue, pItem);
    // qStreamInput(pTask->exec.executor, pItem);
  }

  if (pItem->type != STREAM_INPUT__GET_RES && pItem->type != STREAM_INPUT__CHECKPOINT && pTask->triggerParam != 0) {
    atomic_val_compare_exchange_8(&pTask->triggerStatus, TASK_TRIGGER_STATUS__IN_ACTIVE, TASK_TRIGGER_STATUS__ACTIVE);
  }

#if 0
  // TODO: back pressure
  atomic_store_8(&pTask->inputStatus, TASK_INPUT_STATUS__NORMAL);
#endif
  return 0;
}

static FORCE_INLINE void streamTaskInputFail(SStreamTask* pTask) {
  atomic_store_8(&pTask->inputStatus, TASK_INPUT_STATUS__FAILED);
}

static FORCE_INLINE int32_t streamTaskOutput(SStreamTask* pTask, SStreamDataBlock* pBlock) {
  if (pTask->sinkType == TASK_SINK__TABLE) {
    ASSERT(pTask->dispatchType == TASK_DISPATCH__NONE);
    pTask->tbSink.tbSinkFunc(pTask, pTask->tbSink.vnode, 0, pBlock->blocks);
    taosArrayDestroyEx(pBlock->blocks, (FDelete)blockDataFreeRes);
    taosFreeQitem(pBlock);
  } else if (pTask->sinkType == TASK_SINK__SMA) {
    ASSERT(pTask->dispatchType == TASK_DISPATCH__NONE);
    pTask->smaSink.smaSink(pTask->smaSink.vnode, pTask->smaSink.smaId, pBlock->blocks);
    taosArrayDestroyEx(pBlock->blocks, (FDelete)blockDataFreeRes);
    taosFreeQitem(pBlock);
  } else {
    ASSERT(pTask->dispatchType != TASK_DISPATCH__NONE);
    taosWriteQitem(pTask->outputQueue->queue, pBlock);
  }
  return 0;
}

typedef struct {
  int32_t reserved;
} SStreamTaskDeployRsp;

typedef struct {
  // SMsgHead     head;
  SStreamTask* task;
} SStreamTaskDeployReq;

typedef struct {
  SMsgHead head;
  int64_t  streamId;
  int32_t  taskId;
} SStreamTaskRunReq;

typedef struct {
  int64_t streamId;
  int32_t taskId;
  int32_t dataSrcVgId;
  int32_t upstreamTaskId;
  int32_t upstreamChildId;
  int32_t upstreamNodeId;
#if 0
  int64_t sourceVer;
#endif
  int32_t blockNum;
  SArray* dataLen;  // SArray<int32_t>
  SArray* data;     // SArray<SRetrieveTableRsp*>
} SStreamDispatchReq;

typedef struct {
  int64_t streamId;
  int32_t taskId;
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
  int64_t streamId;
  int32_t taskId;
  int32_t sourceTaskId;
  int32_t sourceVg;
} SStreamTaskRecoverReq;

typedef struct {
  int64_t streamId;
  int32_t taskId;
  int8_t  inputStatus;
} SStreamTaskRecoverRsp;

int32_t tEncodeStreamTaskRecoverReq(SEncoder* pEncoder, const SStreamTaskRecoverReq* pReq);
int32_t tDecodeStreamTaskRecoverReq(SDecoder* pDecoder, SStreamTaskRecoverReq* pReq);

int32_t tEncodeStreamTaskRecoverRsp(SEncoder* pEncoder, const SStreamTaskRecoverRsp* pRsp);
int32_t tDecodeStreamTaskRecoverRsp(SDecoder* pDecoder, SStreamTaskRecoverRsp* pRsp);

typedef struct {
  int64_t streamId;
  int32_t taskId;
} SMStreamTaskRecoverReq;

typedef struct {
  int64_t streamId;
  int32_t taskId;
} SMStreamTaskRecoverRsp;

int32_t tEncodeSMStreamTaskRecoverReq(SEncoder* pEncoder, const SMStreamTaskRecoverReq* pReq);
int32_t tDecodeSMStreamTaskRecoverReq(SDecoder* pDecoder, SMStreamTaskRecoverReq* pReq);

int32_t tEncodeSMStreamTaskRecoverRsp(SEncoder* pEncoder, const SMStreamTaskRecoverRsp* pRsp);
int32_t tDecodeSMStreamTaskRecoverRsp(SDecoder* pDecoder, SMStreamTaskRecoverRsp* pRsp);

typedef struct {
  int64_t streamId;
} SPStreamTaskRecoverReq;

typedef struct {
  int8_t reserved;
} SPStreamTaskRecoverRsp;

int32_t tDecodeStreamDispatchReq(SDecoder* pDecoder, SStreamDispatchReq* pReq);
int32_t tDecodeStreamRetrieveReq(SDecoder* pDecoder, SStreamRetrieveReq* pReq);

int32_t streamLaunchByWrite(SStreamTask* pTask, int32_t vgId);
int32_t streamSetupTrigger(SStreamTask* pTask);

int32_t streamProcessRunReq(SStreamTask* pTask);
int32_t streamProcessDispatchReq(SStreamTask* pTask, SStreamDispatchReq* pReq, SRpcMsg* pMsg);
int32_t streamProcessDispatchRsp(SStreamTask* pTask, SStreamDispatchRsp* pRsp);
int32_t streamProcessRecoverReq(SStreamTask* pTask, SStreamTaskRecoverReq* pReq, SRpcMsg* pMsg);
int32_t streamProcessRecoverRsp(SStreamTask* pTask, SStreamTaskRecoverRsp* pRsp);

int32_t streamProcessRetrieveReq(SStreamTask* pTask, SStreamRetrieveReq* pReq, SRpcMsg* pMsg);
int32_t streamProcessRetrieveRsp(SStreamTask* pTask, SStreamRetrieveRsp* pRsp);

#ifdef __cplusplus
}
#endif

#endif /* ifndef _STREAM_H_ */
