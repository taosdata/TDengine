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
#include "streamState.h"
#include "tdatablock.h"
#include "tdbInt.h"
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
  STREAM_STATUS__STOP,
  STREAM_STATUS__INIT,
  STREAM_STATUS__FAILED,
  STREAM_STATUS__RECOVER,
};

enum {
  TASK_STATUS__NORMAL = 0,
  TASK_STATUS__DROPPING,
  TASK_STATUS__FAIL,
  TASK_STATUS__STOP,
  TASK_STATUS__WAIT_DOWNSTREAM,
  TASK_STATUS__RECOVER_PREPARE,
  TASK_STATUS__RECOVER1,
  TASK_STATUS__RECOVER2,
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
  TASK_INPUT_STATUS__RECOVER,
  TASK_INPUT_STATUS__STOP,
  TASK_INPUT_STATUS__FAILED,
};

enum {
  TASK_OUTPUT_STATUS__NORMAL = 1,
  TASK_OUTPUT_STATUS__WAIT,
  TASK_OUTPUT_STATUS__BLOCKED,
};

enum {
  TASK_TRIGGER_STATUS__INACTIVE = 1,
  TASK_TRIGGER_STATUS__ACTIVE,
};

enum {
  TASK_LEVEL__SOURCE = 1,
  TASK_LEVEL__AGG,
  TASK_LEVEL__SINK,
};

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

  SArray* blocks;  // SArray<SSDataBlock>
} SStreamDataBlock;

// ref data block, for delete
typedef struct {
  int8_t       type;
  int64_t      ver;
  int32_t*     dataRef;
  SSDataBlock* pBlock;
} SStreamRefDataBlock;

typedef struct {
  int8_t type;
} SStreamCheckpoint;

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

bool              streamQueueResEmpty(const SStreamQueueRes* pRes);
int64_t           streamQueueResSize(const SStreamQueueRes* pRes);
SStreamQueueNode* streamQueueResFront(SStreamQueueRes* pRes);
SStreamQueueNode* streamQueueResPop(SStreamQueueRes* pRes);
void              streamQueueResClear(SStreamQueueRes* pRes);
SStreamQueueRes   streamQueueBuildRes(SStreamQueueNode* pNode);

typedef struct {
  SStreamQueueNode* pHead;
} SStreamQueue1;

bool            streamQueueHasTask(const SStreamQueue1* pQueue);
int32_t         streamQueuePush(SStreamQueue1* pQueue, SStreamQueueItem* pItem);
SStreamQueueRes streamQueueGetRes(SStreamQueue1* pQueue);

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

static FORCE_INLINE void* streamQueueCurItem(SStreamQueue* queue) {
  //
  return queue->qItem;
}

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
  char      stbFullName[TSDB_TABLE_FNAME_LEN];
  int32_t   waitingRspCnt;
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

typedef struct {
  int32_t nodeId;
  int32_t childId;
  int32_t taskId;
  SEpSet  epSet;
} SStreamChildEpInfo;

typedef struct SStreamTask {
  int64_t streamId;
  int32_t taskId;
  int32_t totalLevel;
  int8_t  taskLevel;
  int8_t  outputType;
  int16_t dispatchMsgType;

  int8_t taskStatus;
  int8_t schedStatus;

  // node info
  int32_t selfChildId;
  int32_t nodeId;
  SEpSet  epSet;

  int64_t recoverSnapVer;
  int64_t startVer;

  // fill history
  int8_t fillHistory;

  // children info
  SArray* childEpInfo;  // SArray<SStreamChildEpInfo*>
  int32_t nextCheckId;
  SArray* checkpointInfo;  // SArray<SStreamCheckpointInfo>

  // exec
  STaskExec exec;

  // output
  union {
    STaskDispatcherFixedEp fixedEpDispatcher;
    STaskDispatcherShuffle shuffleDispatcher;
    STaskSinkTb            tbSink;
    STaskSinkSma           smaSink;
    STaskSinkFetch         fetchSink;
  };

  int8_t inputStatus;
  int8_t outputStatus;

  // STaosQueue*   inputQueue1;
  // STaosQall*    inputQall;
  SStreamQueue* inputQueue;
  SStreamQueue* outputQueue;

  // trigger
  int8_t  triggerStatus;
  int64_t triggerParam;
  void*   timer;

  // msg handle
  SMsgCb* pMsgCb;

  // state backend
  SStreamState* pState;

  // do not serialize
  int32_t recoverTryingDownstream;
  int32_t recoverWaitingUpstream;
  int64_t checkReqId;
  SArray* checkReqIds;  // shuffle
  int32_t refCnt;

  int64_t checkpointingId;
  int32_t checkpointAlignCnt;

} SStreamTask;

int32_t tEncodeStreamEpInfo(SEncoder* pEncoder, const SStreamChildEpInfo* pInfo);
int32_t tDecodeStreamEpInfo(SDecoder* pDecoder, SStreamChildEpInfo* pInfo);

SStreamTask* tNewSStreamTask(int64_t streamId);
int32_t      tEncodeSStreamTask(SEncoder* pEncoder, const SStreamTask* pTask);
int32_t      tDecodeSStreamTask(SDecoder* pDecoder, SStreamTask* pTask);
void         tFreeSStreamTask(SStreamTask* pTask);

static FORCE_INLINE int32_t streamTaskInput(SStreamTask* pTask, SStreamQueueItem* pItem) {
  int8_t type = pItem->type;
  if (type == STREAM_INPUT__DATA_SUBMIT) {
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
  } else if (type == STREAM_INPUT__DATA_BLOCK || type == STREAM_INPUT__DATA_RETRIEVE ||
             type == STREAM_INPUT__REF_DATA_BLOCK) {
    taosWriteQitem(pTask->inputQueue->queue, pItem);
    // qStreamInput(pTask->exec.executor, pItem);
  } else if (type == STREAM_INPUT__CHECKPOINT) {
    taosWriteQitem(pTask->inputQueue->queue, pItem);
    // qStreamInput(pTask->exec.executor, pItem);
  } else if (type == STREAM_INPUT__GET_RES) {
    taosWriteQitem(pTask->inputQueue->queue, pItem);
    // qStreamInput(pTask->exec.executor, pItem);
  }

  if (type != STREAM_INPUT__GET_RES && type != STREAM_INPUT__CHECKPOINT && pTask->triggerParam != 0) {
    atomic_val_compare_exchange_8(&pTask->triggerStatus, TASK_TRIGGER_STATUS__INACTIVE, TASK_TRIGGER_STATUS__ACTIVE);
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
  int32_t blockNum;
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
  int8_t  status;
} SStreamTaskCheckRsp;

typedef struct {
  SMsgHead msgHead;
  int64_t  streamId;
  int32_t  taskId;
} SStreamRecoverStep1Req, SStreamRecoverStep2Req;

typedef struct {
  int64_t streamId;
  int32_t taskId;
  int32_t childId;
} SStreamRecoverFinishReq;

int32_t tEncodeSStreamRecoverFinishReq(SEncoder* pEncoder, const SStreamRecoverFinishReq* pReq);
int32_t tDecodeSStreamRecoverFinishReq(SDecoder* pDecoder, SStreamRecoverFinishReq* pReq);

typedef struct {
  int64_t streamId;
  int64_t checkpointId;
  int32_t taskId;
  int32_t nodeId;
  int64_t expireTime;
} SStreamCheckpointSourceReq;

typedef struct {
  int64_t streamId;
  int64_t checkpointId;
  int32_t taskId;
  int32_t nodeId;
  int64_t expireTime;
} SStreamCheckpointSourceRsp;

int32_t tEncodeSStreamCheckpointSourceReq(SEncoder* pEncoder, const SStreamCheckpointSourceReq* pReq);
int32_t tDecodeSStreamCheckpointSourceReq(SDecoder* pDecoder, SStreamCheckpointSourceReq* pReq);

int32_t tEncodeSStreamCheckpointSourceRsp(SEncoder* pEncoder, const SStreamCheckpointSourceRsp* pRsp);
int32_t tDecodeSStreamCheckpointSourceRsp(SDecoder* pDecoder, SStreamCheckpointSourceRsp* pRsp);

typedef struct {
  SMsgHead msgHead;
  int64_t  streamId;
  int64_t  checkpointId;
  int32_t  downstreamTaskId;
  int32_t  downstreamNodeId;
  int32_t  upstreamTaskId;
  int32_t  upstreamNodeId;
  int32_t  childId;
  int64_t  expireTime;
  int8_t   taskLevel;
} SStreamCheckpointReq;

typedef struct {
  SMsgHead msgHead;
  int64_t  streamId;
  int64_t  checkpointId;
  int32_t  downstreamTaskId;
  int32_t  downstreamNodeId;
  int32_t  upstreamTaskId;
  int32_t  upstreamNodeId;
  int32_t  childId;
  int64_t  expireTime;
  int8_t   taskLevel;
} SStreamCheckpointRsp;

int32_t tEncodeSStreamCheckpointReq(SEncoder* pEncoder, const SStreamCheckpointReq* pReq);
int32_t tDecodeSStreamCheckpointReq(SDecoder* pDecoder, SStreamCheckpointReq* pReq);

int32_t tEncodeSStreamCheckpointRsp(SEncoder* pEncoder, const SStreamCheckpointRsp* pRsp);
int32_t tDecodeSStreamCheckpointRsp(SDecoder* pDecoder, SStreamCheckpointRsp* pRsp);

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

int32_t tEncodeSStreamTaskCheckReq(SEncoder* pEncoder, const SStreamTaskCheckReq* pReq);
int32_t tDecodeSStreamTaskCheckReq(SDecoder* pDecoder, SStreamTaskCheckReq* pReq);

int32_t tEncodeSStreamTaskCheckRsp(SEncoder* pEncoder, const SStreamTaskCheckRsp* pRsp);
int32_t tDecodeSStreamTaskCheckRsp(SDecoder* pDecoder, SStreamTaskCheckRsp* pRsp);

int32_t tEncodeSStreamTaskRecoverReq(SEncoder* pEncoder, const SStreamRecoverDownstreamReq* pReq);
int32_t tDecodeSStreamTaskRecoverReq(SDecoder* pDecoder, SStreamRecoverDownstreamReq* pReq);

int32_t tEncodeSStreamTaskRecoverRsp(SEncoder* pEncoder, const SStreamRecoverDownstreamRsp* pRsp);
int32_t tDecodeSStreamTaskRecoverRsp(SDecoder* pDecoder, SStreamRecoverDownstreamRsp* pRsp);

int32_t tDecodeStreamDispatchReq(SDecoder* pDecoder, SStreamDispatchReq* pReq);
int32_t tDecodeStreamRetrieveReq(SDecoder* pDecoder, SStreamRetrieveReq* pReq);
void    tDeleteStreamRetrieveReq(SStreamRetrieveReq* pReq);

void tDeleteStreamDispatchReq(SStreamDispatchReq* pReq);

int32_t streamSetupTrigger(SStreamTask* pTask);

int32_t streamProcessRunReq(SStreamTask* pTask);
int32_t streamProcessDispatchReq(SStreamTask* pTask, SStreamDispatchReq* pReq, SRpcMsg* pMsg, bool exec);
int32_t streamProcessDispatchRsp(SStreamTask* pTask, SStreamDispatchRsp* pRsp, int32_t code);

int32_t streamProcessRetrieveReq(SStreamTask* pTask, SStreamRetrieveReq* pReq, SRpcMsg* pMsg);
int32_t streamProcessRetrieveRsp(SStreamTask* pTask, SStreamRetrieveRsp* pRsp);

int32_t streamTryExec(SStreamTask* pTask);
int32_t streamSchedExec(SStreamTask* pTask);
int32_t streamTaskOutput(SStreamTask* pTask, SStreamDataBlock* pBlock);

int32_t streamScanExec(SStreamTask* pTask, int32_t batchSz);

// recover and fill history
int32_t streamTaskCheckDownstream(SStreamTask* pTask, int64_t version);
int32_t streamTaskLaunchRecover(SStreamTask* pTask, int64_t version);
int32_t streamProcessTaskCheckReq(SStreamTask* pTask, const SStreamTaskCheckReq* pReq);
int32_t streamProcessTaskCheckRsp(SStreamTask* pTask, const SStreamTaskCheckRsp* pRsp, int64_t version);
// common
int32_t streamSetParamForRecover(SStreamTask* pTask);
int32_t streamRestoreParam(SStreamTask* pTask);
int32_t streamSetStatusNormal(SStreamTask* pTask);
// source level
int32_t streamSourceRecoverPrepareStep1(SStreamTask* pTask, int64_t ver);
int32_t streamBuildSourceRecover1Req(SStreamTask* pTask, SStreamRecoverStep1Req* pReq);
int32_t streamSourceRecoverScanStep1(SStreamTask* pTask);
int32_t streamBuildSourceRecover2Req(SStreamTask* pTask, SStreamRecoverStep2Req* pReq);
int32_t streamSourceRecoverScanStep2(SStreamTask* pTask, int64_t ver);
int32_t streamDispatchRecoverFinishReq(SStreamTask* pTask);
// agg level
int32_t streamAggRecoverPrepare(SStreamTask* pTask);
// int32_t streamAggChildrenRecoverFinish(SStreamTask* pTask);
int32_t streamProcessRecoverFinishReq(SStreamTask* pTask, int32_t childId);

// expand and deploy
typedef int32_t FTaskExpand(void* ahandle, SStreamTask* pTask, int64_t ver);

// meta
typedef struct SStreamMeta {
  char*        path;
  TDB*         db;
  TTB*         pTaskDb;
  TTB*         pCheckpointDb;
  SHashObj*    pTasks;
  SHashObj*    pRecoverStatus;
  void*        ahandle;
  TXN*         txn;
  FTaskExpand* expandFunc;
  int32_t      vgId;
  SRWLatch     lock;
} SStreamMeta;

SStreamMeta* streamMetaOpen(const char* path, void* ahandle, FTaskExpand expandFunc, int32_t vgId);
void         streamMetaClose(SStreamMeta* streamMeta);

int32_t      streamMetaAddTask(SStreamMeta* pMeta, int64_t ver, SStreamTask* pTask);
int32_t      streamMetaAddSerializedTask(SStreamMeta* pMeta, int64_t startVer, char* msg, int32_t msgLen);
SStreamTask* streamMetaGetTask(SStreamMeta* pMeta, int32_t taskId);

SStreamTask* streamMetaAcquireTask(SStreamMeta* pMeta, int32_t taskId);
void         streamMetaReleaseTask(SStreamMeta* pMeta, SStreamTask* pTask);
void         streamMetaRemoveTask(SStreamMeta* pMeta, int32_t taskId);

int32_t streamMetaBegin(SStreamMeta* pMeta);
int32_t streamMetaCommit(SStreamMeta* pMeta);
int32_t streamMetaRollBack(SStreamMeta* pMeta);
int32_t streamLoadTasks(SStreamMeta* pMeta);

// checkpoint
int32_t streamProcessCheckpointSourceReq(SStreamMeta* pMeta, SStreamTask* pTask, SStreamCheckpointSourceReq* pReq);
int32_t streamProcessCheckpointReq(SStreamMeta* pMeta, SStreamTask* pTask, SStreamCheckpointReq* pReq);
int32_t streamProcessCheckpointRsp(SStreamMeta* pMeta, SStreamTask* pTask, SStreamCheckpointRsp* pRsp);

#ifdef __cplusplus
}
#endif

#endif /* ifndef _STREAM_H_ */
