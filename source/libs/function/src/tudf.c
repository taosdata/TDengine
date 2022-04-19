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
#include "uv.h"
#include "os.h"
#include "tlog.h"
#include "tudf.h"
#include "tudfInt.h"
#include "tarray.h"
#include "tdatablock.h"

//TODO: when startup, set thread poll size. add it to cfg
//TODO: test for udfd restart
//TODO: udfd restart when exist or aborts
//TODO: deal with uv task that has been started and then udfd core dumped
//TODO: network error processing.
//TODO: add unit test
//TODO: include all global variable under context struct
/* Copyright (c) 2013, Ben Noordhuis <info@bnoordhuis.nl>
 * The QUEUE is copied from queue.h under libuv
 * */

typedef void *QUEUE[2];

/* Private macros. */
#define QUEUE_NEXT(q)       (*(QUEUE **) &((*(q))[0]))
#define QUEUE_PREV(q)       (*(QUEUE **) &((*(q))[1]))
#define QUEUE_PREV_NEXT(q)  (QUEUE_NEXT(QUEUE_PREV(q)))
#define QUEUE_NEXT_PREV(q)  (QUEUE_PREV(QUEUE_NEXT(q)))

/* Public macros. */
#define QUEUE_DATA(ptr, type, field)                                          \
  ((type *) ((char *) (ptr) - offsetof(type, field)))

/* Important note: mutating the list while QUEUE_FOREACH is
 * iterating over its elements results in undefined behavior.
 */
#define QUEUE_FOREACH(q, h)                                                   \
  for ((q) = QUEUE_NEXT(h); (q) != (h); (q) = QUEUE_NEXT(q))

#define QUEUE_EMPTY(q)                                                        \
  ((const QUEUE *) (q) == (const QUEUE *) QUEUE_NEXT(q))

#define QUEUE_HEAD(q)                                                         \
  (QUEUE_NEXT(q))

#define QUEUE_INIT(q)                                                         \
  do {                                                                        \
    QUEUE_NEXT(q) = (q);                                                      \
    QUEUE_PREV(q) = (q);                                                      \
  }                                                                           \
  while (0)

#define QUEUE_ADD(h, n)                                                       \
  do {                                                                        \
    QUEUE_PREV_NEXT(h) = QUEUE_NEXT(n);                                       \
    QUEUE_NEXT_PREV(n) = QUEUE_PREV(h);                                       \
    QUEUE_PREV(h) = QUEUE_PREV(n);                                            \
    QUEUE_PREV_NEXT(h) = (h);                                                 \
  }                                                                           \
  while (0)

#define QUEUE_SPLIT(h, q, n)                                                  \
  do {                                                                        \
    QUEUE_PREV(n) = QUEUE_PREV(h);                                            \
    QUEUE_PREV_NEXT(n) = (n);                                                 \
    QUEUE_NEXT(n) = (q);                                                      \
    QUEUE_PREV(h) = QUEUE_PREV(q);                                            \
    QUEUE_PREV_NEXT(h) = (h);                                                 \
    QUEUE_PREV(q) = (n);                                                      \
  }                                                                           \
  while (0)

#define QUEUE_MOVE(h, n)                                                      \
  do {                                                                        \
    if (QUEUE_EMPTY(h))                                                       \
      QUEUE_INIT(n);                                                          \
    else {                                                                    \
      QUEUE* q = QUEUE_HEAD(h);                                               \
      QUEUE_SPLIT(h, q, n);                                                   \
    }                                                                         \
  }                                                                           \
  while (0)

#define QUEUE_INSERT_HEAD(h, q)                                               \
  do {                                                                        \
    QUEUE_NEXT(q) = QUEUE_NEXT(h);                                            \
    QUEUE_PREV(q) = (h);                                                      \
    QUEUE_NEXT_PREV(q) = (q);                                                 \
    QUEUE_NEXT(h) = (q);                                                      \
  }                                                                           \
  while (0)

#define QUEUE_INSERT_TAIL(h, q)                                               \
  do {                                                                        \
    QUEUE_NEXT(q) = (h);                                                      \
    QUEUE_PREV(q) = QUEUE_PREV(h);                                            \
    QUEUE_PREV_NEXT(q) = (q);                                                 \
    QUEUE_PREV(h) = (q);                                                      \
  }                                                                           \
  while (0)

#define QUEUE_REMOVE(q)                                                       \
  do {                                                                        \
    QUEUE_PREV_NEXT(q) = QUEUE_NEXT(q);                                       \
    QUEUE_NEXT_PREV(q) = QUEUE_PREV(q);                                       \
  }                                                                           \
  while (0)


enum {
  UV_TASK_CONNECT = 0,
  UV_TASK_REQ_RSP = 1,
  UV_TASK_DISCONNECT = 2
};

typedef struct SUdfUvSession {
  int64_t severHandle;
  uv_pipe_t *udfSvcPipe;
} SUdfUvSession;

typedef struct SClientUvTaskNode {
  int8_t type;
  int errCode;

  uv_pipe_t *pipe;

  int64_t seqNum;
  uv_buf_t reqBuf;

  uv_sem_t taskSem;
  uv_buf_t rspBuf;

  QUEUE recvTaskQueue;
  QUEUE procTaskQueue;
  QUEUE connTaskQueue;
} SClientUvTaskNode;

typedef struct SClientUdfTask {
  int8_t type;

  SUdfUvSession *session;

  int32_t errCode;

  union {
    struct {
      SUdfSetupRequest req;
      SUdfSetupResponse rsp;
    } _setup;
    struct {
      SUdfCallRequest req;
      SUdfCallResponse rsp;
    } _call;
    struct {
      SUdfTeardownRequest req;
      SUdfTeardownResponse rsp;
    } _teardown;
  };


} SClientUdfTask;

typedef struct SClientConnBuf {
  char *buf;
  int32_t len;
  int32_t cap;
  int32_t total;
} SClientConnBuf;

typedef struct SClientUvConn {
  uv_pipe_t *pipe;
  QUEUE taskQueue;
  SClientConnBuf readBuf;
} SClientUvConn;

uv_process_t gUdfdProcess;

uv_barrier_t gUdfInitBarrier;

uv_loop_t gUdfdLoop;
uv_thread_t gUdfLoopThread;
uv_async_t gUdfLoopTaskAync;

uv_async_t gUdfLoopStopAsync;

uv_mutex_t gUdfTaskQueueMutex;
int64_t gUdfTaskSeqNum = 0;

enum {
  UDFC_STATE_INITAL = 0, // initial state
  UDFC_STATE_STARTNG, // starting after startUdfService
  UDFC_STATE_READY, // started and begin to receive quests
  UDFC_STATE_RESTARTING, // udfd abnormal exit. cleaning up and restart.
  UDFC_STATE_STOPPING, // stopping after stopUdfService
  UDFC_STATUS_FINAL, // stopped
};
int8_t gUdfcState = UDFC_STATE_INITAL;

//double circular linked list

QUEUE gUdfTaskQueue = {0};

QUEUE gUvProcTaskQueue = {0};

int32_t encodeUdfSetupRequest(void **buf, const SUdfSetupRequest *setup) {
  int32_t len = 0;
  len += taosEncodeBinary(buf, setup->udfName, TSDB_FUNC_NAME_LEN);
  len += taosEncodeSEpSet(buf, &setup->epSet);
  return len;
}

void* decodeUdfSetupRequest(const void* buf, SUdfSetupRequest *request) {
  buf = taosDecodeBinaryTo(buf, request->udfName, TSDB_FUNC_NAME_LEN);
  buf = taosDecodeSEpSet((void*)buf, &request->epSet);
  return (void*)buf;
}

int32_t encodeUdfInterBuf(void **buf, const SUdfInterBuf* state) {
  int32_t len = 0;
  len += taosEncodeFixedI32(buf, state->bufLen);
  len += taosEncodeBinary(buf, state->buf, state->bufLen);
  return len;
}

void* decodeUdfInterBuf(const void* buf, SUdfInterBuf* state) {
  buf = taosDecodeFixedI32(buf, &state->bufLen);
  buf = taosDecodeBinary(buf, (void**)&state->buf, state->bufLen);
  return (void*)buf;
}

int32_t encodeUdfCallRequest(void **buf, const SUdfCallRequest *call) {
  int32_t len = 0;
  len += taosEncodeFixedI64(buf, call->udfHandle);
  len += taosEncodeFixedI8(buf, call->callType);
  if (call->callType == TSDB_UDF_CALL_SCALA_PROC) {
    len += tEncodeDataBlock(buf, &call->block);
  } else if (call->callType == TSDB_UDF_CALL_AGG_INIT) {
    len += taosEncodeFixedI8(buf, call->initFirst);
  } else if (call->callType == TSDB_UDF_CALL_AGG_PROC) {
    len += tEncodeDataBlock(buf, &call->block);
    len += encodeUdfInterBuf(buf, &call->interBuf);
  } else if (call->callType == TSDB_UDF_CALL_AGG_MERGE) {
    len += encodeUdfInterBuf(buf, &call->interBuf);
    len += encodeUdfInterBuf(buf, &call->interBuf2);
  } else if (call->callType == TSDB_UDF_CALL_AGG_FIN) {
    len += encodeUdfInterBuf(buf, &call->interBuf);
  }
  return len;
}

void* decodeUdfCallRequest(const void* buf, SUdfCallRequest* call) {
  buf = taosDecodeFixedI64(buf, &call->udfHandle);
  buf = taosDecodeFixedI8(buf, &call->callType);
  switch (call->callType) {
    case TSDB_UDF_CALL_SCALA_PROC:
      buf = tDecodeDataBlock(buf, &call->block);
      break;
    case TSDB_UDF_CALL_AGG_INIT:
      buf = taosDecodeFixedI8(buf, &call->initFirst);
      break;
    case TSDB_UDF_CALL_AGG_PROC:
      buf = tDecodeDataBlock(buf, &call->block);
      buf = decodeUdfInterBuf(buf, &call->interBuf);
      break;
    case TSDB_UDF_CALL_AGG_MERGE:
      buf = decodeUdfInterBuf(buf, &call->interBuf);
      buf = decodeUdfInterBuf(buf, &call->interBuf2);
      break;
    case TSDB_UDF_CALL_AGG_FIN:
      buf = decodeUdfInterBuf(buf, &call->interBuf);
      break;
  }
  return (void*)buf;
}

int32_t encodeUdfTeardownRequest(void **buf, const SUdfTeardownRequest *teardown) {
  int32_t len = 0;
  len += taosEncodeFixedI64(buf, teardown->udfHandle);
  return len;
}

void* decodeUdfTeardownRequest(const void* buf, SUdfTeardownRequest *teardown) {
  buf = taosDecodeFixedI64(buf, &teardown->udfHandle);
  return (void*)buf;
}

int32_t encodeUdfRequest(void** buf, const SUdfRequest* request) {
  int32_t len = 0;
  if (buf == NULL) {
    len += sizeof(request->msgLen);
  } else {
    *(int32_t*)(*buf) = request->msgLen;
    *buf = POINTER_SHIFT(*buf, sizeof(request->msgLen));
  }
  len += taosEncodeFixedI64(buf, request->seqNum);
  len += taosEncodeFixedI8(buf, request->type);
  if (request->type == UDF_TASK_SETUP) {
    len += encodeUdfSetupRequest(buf, &request->setup);
  } else if (request->type == UDF_TASK_CALL) {
    len += encodeUdfCallRequest(buf, &request->call);
  } else if (request->type == UDF_TASK_TEARDOWN) {
    len += encodeUdfTeardownRequest(buf, &request->teardown);
  }
  return len;
}

void* decodeUdfRequest(const void* buf, SUdfRequest* request) {
  request->msgLen = *(int32_t*)(buf);
  POINTER_SHIFT(buf, sizeof(request->msgLen));

  buf = taosDecodeFixedI64(buf, &request->seqNum);
  buf = taosDecodeFixedI8(buf, &request->type);

  if (request->type == UDF_TASK_SETUP) {
    buf = decodeUdfSetupRequest(buf, &request->setup);
  } else if (request->type == UDF_TASK_CALL) {
    buf = decodeUdfCallRequest(buf, &request->call);
  } else if (request->type == UDF_TASK_TEARDOWN) {
    buf = decodeUdfTeardownRequest(buf, &request->teardown);
  }
  return (void*)buf;
}

int32_t encodeUdfSetupResponse(void **buf, const SUdfSetupResponse *setupRsp) {
  int32_t len = 0;
  len += taosEncodeFixedI64(buf, setupRsp->udfHandle);
  return len;
}

void* decodeUdfSetupResponse(const void* buf, SUdfSetupResponse* setupRsp) {
  buf = taosDecodeFixedI64(buf, &setupRsp->udfHandle);
  return (void*)buf;
}

int32_t encodeUdfCallResponse(void **buf, const SUdfCallResponse *callRsp) {
  int32_t len = 0;
  len += taosEncodeFixedI8(buf, callRsp->callType);
  switch (callRsp->callType) {
    case TSDB_UDF_CALL_SCALA_PROC:
      len += tEncodeDataBlock(buf, &callRsp->resultData);
      break;
    case TSDB_UDF_CALL_AGG_INIT:
      len += encodeUdfInterBuf(buf, &callRsp->resultBuf);
      break;
    case TSDB_UDF_CALL_AGG_PROC:
      len += encodeUdfInterBuf(buf, &callRsp->resultBuf);
      break;
    case TSDB_UDF_CALL_AGG_MERGE:
      len += encodeUdfInterBuf(buf, &callRsp->resultBuf);
      break;
    case TSDB_UDF_CALL_AGG_FIN:
      len += tEncodeDataBlock(buf, &callRsp->resultData);
      break;
  }
  return len;
}

void* decodeUdfCallResponse(const void* buf, SUdfCallResponse* callRsp) {
  buf = taosDecodeFixedI8(buf, &callRsp->callType);
  switch (callRsp->callType) {
    case TSDB_UDF_CALL_SCALA_PROC:
      buf = tDecodeDataBlock(buf, &callRsp->resultData);
      break;
    case TSDB_UDF_CALL_AGG_INIT:
      buf = decodeUdfInterBuf(buf, &callRsp->resultBuf);
      break;
    case TSDB_UDF_CALL_AGG_PROC:
      buf = decodeUdfInterBuf(buf, &callRsp->resultBuf);
      break;
    case TSDB_UDF_CALL_AGG_MERGE:
      buf = decodeUdfInterBuf(buf, &callRsp->resultBuf);
      break;
    case TSDB_UDF_CALL_AGG_FIN:
      buf = tDecodeDataBlock(buf, &callRsp->resultData);
      break;
  }
  return (void*)buf;
}

int32_t encodeUdfTeardownResponse(void** buf, const SUdfTeardownResponse* teardownRsp) {
  return 0;
}

void* decodeUdfTeardownResponse(const void* buf, SUdfTeardownResponse* teardownResponse) {
  return (void*)buf;
}

int32_t encodeUdfResponse(void** buf, const SUdfResponse* rsp) {
  int32_t len = 0;
  if (buf == NULL) {
    len += sizeof(rsp->msgLen);
  } else {
    *(int32_t*)(*buf) = rsp->msgLen;
    *buf = POINTER_SHIFT(*buf, sizeof(rsp->msgLen));
  }

  len += taosEncodeFixedI64(buf, rsp->seqNum);
  len += taosEncodeFixedI8(buf, rsp->type);
  len += taosEncodeFixedI32(buf, rsp->code);

  switch (rsp->type) {
    case UDF_TASK_SETUP:
      len += encodeUdfSetupResponse(buf, &rsp->setupRsp);
      break;
    case UDF_TASK_CALL:
      len += encodeUdfCallResponse(buf, &rsp->callRsp);
      break;
    case UDF_TASK_TEARDOWN:
      len += encodeUdfTeardownResponse(buf, &rsp->teardownRsp);
      break;
    default:
      //TODO: log error
      break;
  }
  return len;
}

void* decodeUdfResponse(const void* buf, SUdfResponse* rsp) {
  rsp->msgLen = *(int32_t*)(buf);
  POINTER_SHIFT(buf, sizeof(rsp->msgLen));
  buf = taosDecodeFixedI64(buf, &rsp->seqNum);
  buf = taosDecodeFixedI8(buf, &rsp->type);
  buf = taosDecodeFixedI32(buf, &rsp->code);

  switch (rsp->type) {
    case UDF_TASK_SETUP:
      buf = decodeUdfSetupResponse(buf, &rsp->setupRsp);
      break;
    case UDF_TASK_CALL:
      buf = decodeUdfCallResponse(buf, &rsp->callRsp);
      break;
    case UDF_TASK_TEARDOWN:
      buf = decodeUdfTeardownResponse(buf, &rsp->teardownRsp);
      break;
    default:
      //TODO: log error
      break;
  }
  return (void*)buf;
}

void freeUdfColumnData(SUdfColumnData *data) {
  if (data->varLengthColumn) {
    taosMemoryFree(data->varOffsets);
    data->varOffsets = NULL;
    taosMemoryFree(data->payload);
    data->payload = NULL;
  } else {
    taosMemoryFree(data->nullBitmap);
    data->nullBitmap = NULL;
    taosMemoryFree(data->data);
    data->data = NULL;
  }
}

void freeUdfColumn(SUdfColumn* col) {
  freeUdfColumnData(&col->colData);
}

void freeUdfDataDataBlock(SUdfDataBlock *block) {
  for (int32_t i = 0; i < block->numOfCols; ++i) {
    freeUdfColumn(block->udfCols[i]);
    taosMemoryFree(block->udfCols[i]);
    block->udfCols[i] = NULL;
  }
  taosMemoryFree(block->udfCols);
  block->udfCols = NULL;
}

void freeUdfInterBuf(SUdfInterBuf *buf) {
  taosMemoryFree(buf->buf);
  buf->buf = NULL;
}


int32_t convertDataBlockToUdfDataBlock(SSDataBlock *block, SUdfDataBlock *udfBlock) {
  udfBlock->numOfRows = block->info.rows;
  udfBlock->numOfCols = block->info.numOfCols;
  udfBlock->udfCols = taosMemoryMalloc(sizeof(SUdfColumn*) * udfBlock->numOfCols);
  for (int32_t i = 0; i < udfBlock->numOfCols; ++i) {
    udfBlock->udfCols[i] = taosMemoryMalloc(sizeof(SUdfColumn));
    SColumnInfoData *col= (SColumnInfoData*)taosArrayGet(block->pDataBlock, i);
    SUdfColumn *udfCol = udfBlock->udfCols[i];
    udfCol->colMeta.type = col->info.type;
    udfCol->colMeta.bytes = col->info.bytes;
    udfCol->colMeta.scale = col->info.scale;
    udfCol->colMeta.precision = col->info.precision;
    udfCol->colData.numOfRows = udfBlock->numOfRows;
    udfCol->colData.varLengthColumn = IS_VAR_DATA_TYPE(udfCol->colMeta.type);
    if (udfCol->colData.varLengthColumn) {
      udfCol->colData.varOffsetsLen = sizeof(int32_t) * udfBlock->numOfRows;
      udfCol->colData.varOffsets = taosMemoryMalloc(udfCol->colData.varOffsetsLen);
      memcpy(udfCol->colData.varOffsets, col->varmeta.offset, udfCol->colData.varOffsetsLen);
      udfCol->colData.payloadLen = colDataGetLength(col, udfBlock->numOfRows);
      udfCol->colData.payload = taosMemoryMalloc(udfCol->colData.payloadLen);
      memcpy(udfCol->colData.payload, col->pData, udfCol->colData.payloadLen);
    } else {
      udfCol->colData.nullBitmapLen = BitmapLen(udfCol->colData.numOfRows);
      udfCol->colData.nullBitmap = taosMemoryMalloc(udfCol->colData.nullBitmapLen);
      memcpy(udfCol->colData.nullBitmap, col->nullbitmap, udfCol->colData.nullBitmapLen);
      udfCol->colData.dataLen = colDataGetLength(col, udfBlock->numOfRows);
      udfCol->colData.data = taosMemoryMalloc(udfCol->colData.dataLen);
      memcpy(udfCol->colData.data, col->pData, udfCol->colData.dataLen);
    }
  }
  return 0;
}

int32_t convertUdfColumnToDataBlock(SUdfColumn *udfCol, SSDataBlock *block) {
  block->info.numOfCols = 1;
  block->info.rows = udfCol->colData.numOfRows;
  block->info.hasVarCol = udfCol->colData.varLengthColumn;

  block->pDataBlock = taosArrayInit(1, sizeof(SColumnInfoData));
  taosArraySetSize(block->pDataBlock, 1);
  SColumnInfoData *col = taosArrayGet(block->pDataBlock, 0);
  SUdfColumnMeta *meta = &udfCol->colMeta;
  col->info.precision = meta->precision;
  col->info.bytes = meta->bytes;
  col->info.scale = meta->scale;
  col->info.type = meta->type;
  SUdfColumnData *data = &udfCol->colData;

  if (!IS_VAR_DATA_TYPE(meta->type)) {
    col->nullbitmap = taosMemoryMalloc(data->nullBitmapLen);
    memcpy(col->nullbitmap, data->nullBitmap, data->nullBitmapLen);
    col->pData = taosMemoryMalloc(data->dataLen);
    memcpy(col->pData, data->payload, data->dataLen);
  } else {
    col->varmeta.offset = taosMemoryMalloc(data->varOffsetsLen);
    memcpy(col->varmeta.offset, data->varOffsets, data->varOffsetsLen);
    col->pData = taosMemoryMalloc(data->payloadLen);
    memcpy(col->pData, data->payload, data->payloadLen);
  }
  return 0;
}


void onUdfcPipeClose(uv_handle_t *handle) {
  SClientUvConn *conn = handle->data;
  if (!QUEUE_EMPTY(&conn->taskQueue)) {
    QUEUE* h = QUEUE_HEAD(&conn->taskQueue);
    SClientUvTaskNode *task = QUEUE_DATA(h, SClientUvTaskNode, connTaskQueue);
    task->errCode = 0;
    uv_sem_post(&task->taskSem);
    QUEUE_REMOVE(&task->procTaskQueue);
  }

  taosMemoryFree(conn->readBuf.buf);
  taosMemoryFree(conn);
  taosMemoryFree((uv_pipe_t *) handle);

}

int32_t udfcGetUvTaskResponseResult(SClientUdfTask *task, SClientUvTaskNode *uvTask) {
  debugPrint("%s", "get uv task result");
  if (uvTask->type == UV_TASK_REQ_RSP) {
    if (uvTask->rspBuf.base != NULL) {
      SUdfResponse rsp;
      void* buf = decodeUdfResponse(uvTask->rspBuf.base, &rsp);
      assert(uvTask->rspBuf.len == POINTER_DISTANCE(buf, uvTask->rspBuf.base));
      task->errCode = rsp.code;

      switch (task->type) {
        case UDF_TASK_SETUP: {
          //TODO: copy or not
          task->_setup.rsp = rsp.setupRsp;
          break;
        }
        case UDF_TASK_CALL: {
          task->_call.rsp = rsp.callRsp;
          //TODO: copy or not
          break;
        }
        case UDF_TASK_TEARDOWN: {
          task->_teardown.rsp = rsp.teardownRsp;
          //TODO: copy or not?
          break;
        }
        default: {
          break;
        }
      }

      // TODO: the call buffer is setup and freed by udf invocation
      taosMemoryFree(uvTask->rspBuf.base);
    } else {
      task->errCode = uvTask->errCode;
    }
  } else if (uvTask->type == UV_TASK_CONNECT) {
    task->errCode = uvTask->errCode;
  } else if (uvTask->type == UV_TASK_DISCONNECT) {
    task->errCode = uvTask->errCode;
  }
  return 0;
}

void udfcAllocateBuffer(uv_handle_t *handle, size_t suggestedSize, uv_buf_t *buf) {
  debugPrint("%s", "client allocate buffer to receive from pipe");
  SClientUvConn *conn = handle->data;
  SClientConnBuf *connBuf = &conn->readBuf;

  int32_t msgHeadSize = sizeof(int32_t) + sizeof(int64_t);
  if (connBuf->cap == 0) {
    connBuf->buf = taosMemoryMalloc(msgHeadSize);
    if (connBuf->buf) {
      connBuf->len = 0;
      connBuf->cap = msgHeadSize;
      connBuf->total = -1;

      buf->base = connBuf->buf;
      buf->len = connBuf->cap;
    } else {
      //TODO: log error
      buf->base = NULL;
      buf->len = 0;
    }
  } else {
    connBuf->cap = connBuf->total > connBuf->cap ? connBuf->total : connBuf->cap;
    void *resultBuf = taosMemoryRealloc(connBuf->buf, connBuf->cap);
    if (resultBuf) {
      connBuf->buf = resultBuf;
      buf->base = connBuf->buf + connBuf->len;
      buf->len = connBuf->cap - connBuf->len;
    } else {
      //TODO: log error free connBuf->buf
      buf->base = NULL;
      buf->len = 0;
    }
  }

  debugPrint("\tconn buf cap - len - total : %d - %d - %d", connBuf->cap, connBuf->len, connBuf->total);

}

bool isUdfcUvMsgComplete(SClientConnBuf *connBuf) {
  if (connBuf->total == -1 && connBuf->len >= sizeof(int32_t)) {
    connBuf->total = *(int32_t *) (connBuf->buf);
  }
  if (connBuf->len == connBuf->cap && connBuf->total == connBuf->cap) {
    return true;
  }
  return false;
}

void udfcUvHandleRsp(SClientUvConn *conn) {
  SClientConnBuf *connBuf = &conn->readBuf;
  int64_t seqNum = *(int64_t *) (connBuf->buf + sizeof(int32_t)); // msglen int32_t then seqnum

  if (QUEUE_EMPTY(&conn->taskQueue)) {
    //LOG error
    return;
  }
  bool found = false;
  SClientUvTaskNode *taskFound = NULL;
  QUEUE* h = QUEUE_NEXT(&conn->taskQueue);
  SClientUvTaskNode *task = QUEUE_DATA(h, SClientUvTaskNode, connTaskQueue);

  while (h != &conn->taskQueue) {
    if (task->seqNum == seqNum) {
      if (found == false) {
        found = true;
        taskFound = task;
      } else {
        //LOG error;
        continue;
      }
    }
    h = QUEUE_NEXT(h);
    task = QUEUE_DATA(h, SClientUvTaskNode, connTaskQueue);
  }

  if (taskFound) {
    taskFound->rspBuf = uv_buf_init(connBuf->buf, connBuf->len);
    QUEUE_REMOVE(&taskFound->connTaskQueue);
    uv_sem_post(&taskFound->taskSem);
    QUEUE_REMOVE(&taskFound->procTaskQueue);
  } else {
    //TODO: LOG error
  }
  connBuf->buf = NULL;
  connBuf->total = -1;
  connBuf->len = 0;
  connBuf->cap = 0;
}

void udfcUvHandleError(SClientUvConn *conn) {
  while (!QUEUE_EMPTY(&conn->taskQueue)) {
    QUEUE* h = QUEUE_HEAD(&conn->taskQueue);
    SClientUvTaskNode *task = QUEUE_DATA(h, SClientUvTaskNode, connTaskQueue);
    task->errCode = UDFC_CODE_PIPE_READ_ERR;
    uv_sem_post(&task->taskSem);
    QUEUE_REMOVE(&task->procTaskQueue);
  }

  uv_close((uv_handle_t *) conn->pipe, NULL);
  taosMemoryFree(conn->pipe);
  taosMemoryFree(conn->readBuf.buf);
  taosMemoryFree(conn);
}

void onUdfcRead(uv_stream_t *client, ssize_t nread, const uv_buf_t *buf) {
  debugPrint("%s, nread: %zd", "client read from pipe", nread);
  if (nread == 0) return;

  SClientUvConn *conn = client->data;
  SClientConnBuf *connBuf = &conn->readBuf;
  if (nread > 0) {
    connBuf->len += nread;
    if (isUdfcUvMsgComplete(connBuf)) {
      udfcUvHandleRsp(conn);
    }

  }
  if (nread < 0) {
    debugPrint("\tclient read error: %s", uv_strerror(nread));
    if (nread == UV_EOF) {
      //TODO:
    }
    udfcUvHandleError(conn);
  }

}

void onUdfClientWrite(uv_write_t *write, int status) {
  debugPrint("%s", "after writing to pipe");
  SClientUvTaskNode *uvTask = write->data;
  if (status == 0) {
    uv_pipe_t *pipe = uvTask->pipe;
    SClientUvConn *conn = pipe->data;
    QUEUE_INSERT_TAIL(&conn->taskQueue, &uvTask->connTaskQueue);
  } else {
    //TODO Log error;
  }
  debugPrint("\tlength:%zu", uvTask->reqBuf.len);
  taosMemoryFree(write);
  taosMemoryFree(uvTask->reqBuf.base);
}

void onUdfClientConnect(uv_connect_t *connect, int status) {
  SClientUvTaskNode *uvTask = connect->data;
  uvTask->errCode = status;
  if (status != 0) {
    //TODO: LOG error
  }
  uv_read_start((uv_stream_t *) uvTask->pipe, udfcAllocateBuffer, onUdfcRead);
  taosMemoryFree(connect);
  uv_sem_post(&uvTask->taskSem);
  QUEUE_REMOVE(&uvTask->procTaskQueue);
}

int32_t createUdfcUvTask(SClientUdfTask *task, int8_t uvTaskType, SClientUvTaskNode **pUvTask) {
  SClientUvTaskNode *uvTask = taosMemoryCalloc(1, sizeof(SClientUvTaskNode));
  uvTask->type = uvTaskType;

  if (uvTaskType == UV_TASK_CONNECT) {
  } else if (uvTaskType == UV_TASK_REQ_RSP) {
    uvTask->pipe = task->session->udfSvcPipe;
    SUdfRequest request;
    request.type = task->type;
    request.seqNum = gUdfTaskSeqNum++;

    if (task->type == UDF_TASK_SETUP) {
      request.setup = task->_setup.req;
      request.type = UDF_TASK_SETUP;
    } else if (task->type == UDF_TASK_CALL) {
      request.call = task->_call.req;
      request.type = UDF_TASK_CALL;
    } else if (task->type == UDF_TASK_TEARDOWN) {
      request.teardown = task->_teardown.req;
      request.type = UDF_TASK_TEARDOWN;
    } else {
      //TODO log and return error
    }
    int32_t bufLen = encodeUdfRequest(NULL, &request);
    request.msgLen = bufLen;
    void *buf = taosMemoryMalloc(bufLen);
    encodeUdfRequest(&buf, &request);
    uvTask->reqBuf = uv_buf_init(buf, bufLen);
    uvTask->seqNum = request.seqNum;
  } else if (uvTaskType == UV_TASK_DISCONNECT) {
    uvTask->pipe = task->session->udfSvcPipe;
  }
  uv_sem_init(&uvTask->taskSem, 0);

  *pUvTask = uvTask;
  return 0;
}

int32_t queueUvUdfTask(SClientUvTaskNode *uvTask) {
  debugPrint("%s, %d", "queue uv task", uvTask->type);

  uv_mutex_lock(&gUdfTaskQueueMutex);
  QUEUE_INSERT_TAIL(&gUdfTaskQueue, &uvTask->recvTaskQueue);
  uv_mutex_unlock(&gUdfTaskQueueMutex);
  uv_async_send(&gUdfLoopTaskAync);

  uv_sem_wait(&uvTask->taskSem);
  uv_sem_destroy(&uvTask->taskSem);

  return 0;
}

int32_t startUvUdfTask(SClientUvTaskNode *uvTask) {
  debugPrint("%s, type %d", "start uv task ", uvTask->type);
  switch (uvTask->type) {
    case UV_TASK_CONNECT: {
      uv_pipe_t *pipe = taosMemoryMalloc(sizeof(uv_pipe_t));
      uv_pipe_init(&gUdfdLoop, pipe, 0);
      uvTask->pipe = pipe;

      SClientUvConn *conn = taosMemoryMalloc(sizeof(SClientUvConn));
      conn->pipe = pipe;
      conn->readBuf.len = 0;
      conn->readBuf.cap = 0;
      conn->readBuf.buf = 0;
      conn->readBuf.total = -1;
      QUEUE_INIT(&conn->taskQueue);

      pipe->data = conn;

      uv_connect_t *connReq = taosMemoryMalloc(sizeof(uv_connect_t));
      connReq->data = uvTask;

      uv_pipe_connect(connReq, pipe, "udf.sock", onUdfClientConnect);
      break;
    }
    case UV_TASK_REQ_RSP: {
      uv_pipe_t *pipe = uvTask->pipe;
      uv_write_t *write = taosMemoryMalloc(sizeof(uv_write_t));
      write->data = uvTask;
      uv_write(write, (uv_stream_t *) pipe, &uvTask->reqBuf, 1, onUdfClientWrite);
      break;
    }
    case UV_TASK_DISCONNECT: {
      SClientUvConn *conn = uvTask->pipe->data;
      QUEUE_INSERT_TAIL(&conn->taskQueue, &uvTask->connTaskQueue);
      uv_close((uv_handle_t *) uvTask->pipe, onUdfcPipeClose);
      break;
    }
    default: {
      break;
    }
  }

  return 0;
}

void udfClientAsyncCb(uv_async_t *async) {
  QUEUE wq;

  uv_mutex_lock(&gUdfTaskQueueMutex);
  QUEUE_MOVE(&gUdfTaskQueue, &wq);
  uv_mutex_unlock(&gUdfTaskQueueMutex);

  while (!QUEUE_EMPTY(&wq)) {
    QUEUE* h = QUEUE_HEAD(&wq);
    QUEUE_REMOVE(h);
    SClientUvTaskNode *task = QUEUE_DATA(h, SClientUvTaskNode, recvTaskQueue);
    startUvUdfTask(task);
    QUEUE_INSERT_TAIL(&gUvProcTaskQueue, &task->procTaskQueue);
  }

}

void cleanUpUvTasks() {
  QUEUE wq;

  uv_mutex_lock(&gUdfTaskQueueMutex);
  QUEUE_MOVE(&gUdfTaskQueue, &wq);
  uv_mutex_unlock(&gUdfTaskQueueMutex);

  while (!QUEUE_EMPTY(&wq)) {
    QUEUE* h = QUEUE_HEAD(&wq);
    QUEUE_REMOVE(h);
    SClientUvTaskNode *task = QUEUE_DATA(h, SClientUvTaskNode, recvTaskQueue);
    if (gUdfcState == UDFC_STATE_STOPPING) {
      task->errCode = UDFC_CODE_STOPPING;
    } else if (gUdfcState == UDFC_STATE_RESTARTING) {
      task->errCode = UDFC_CODE_RESTARTING;
    }
    uv_sem_post(&task->taskSem);
  }

  // TODO: deal with tasks that are waiting result.
  while (!QUEUE_EMPTY(&gUvProcTaskQueue)) {
    QUEUE* h = QUEUE_HEAD(&gUvProcTaskQueue);
    QUEUE_REMOVE(h);
    SClientUvTaskNode *task = QUEUE_DATA(h, SClientUvTaskNode, procTaskQueue);
    if (gUdfcState == UDFC_STATE_STOPPING) {
      task->errCode = UDFC_CODE_STOPPING;
    } else if (gUdfcState == UDFC_STATE_RESTARTING) {
      task->errCode = UDFC_CODE_RESTARTING;
    }
    uv_sem_post(&task->taskSem);
  }
}

void udfStopAsyncCb(uv_async_t *async) {
  cleanUpUvTasks();
  if (gUdfcState == UDFC_STATE_STOPPING) {
    uv_stop(&gUdfdLoop);
  }
}

int32_t startUdfd();

void onUdfdExit(uv_process_t *req, int64_t exit_status, int term_signal) {
  //TODO: pipe close will be first received
  debugPrint("Process exited with status %" PRId64 ", signal %d", exit_status, term_signal);
  uv_close((uv_handle_t *) req, NULL);
  //TODO: restart the udfd process
  if (gUdfcState == UDFC_STATE_STOPPING) {
    if (term_signal != SIGINT) {
      //TODO: log error
    }
  }
  if (gUdfcState == UDFC_STATE_READY) {
    gUdfcState = UDFC_STATE_RESTARTING;
    //TODO: asynchronous without blocking. how to do it
    cleanUpUvTasks();
    startUdfd();
  }
}

int32_t startUdfd() {
  //TODO: path
  uv_process_options_t options = {0};
  static char path[256] = {0};
  size_t cwdSize;
  uv_cwd(path, &cwdSize);
  strcat(path, "/udfd");
  char* args[2] = {path, NULL};
  options.args = args;
  options.file = path;
  options.exit_cb = onUdfdExit;
  options.stdio_count = 3;
  uv_stdio_container_t child_stdio[3];
  child_stdio[0].flags = UV_IGNORE;
  child_stdio[1].flags = UV_INHERIT_FD;
  child_stdio[1].data.fd = 1;
  child_stdio[2].flags = UV_INHERIT_FD;
  child_stdio[2].data.fd = 2;
  options.stdio = child_stdio;
  //TODO spawn error
  int err = uv_spawn(&gUdfdLoop, &gUdfdProcess, &options);
  if (err != 0) {
    debugPrint("can not spawn udfd. path: %s, error: %s", path, uv_strerror(err));
  }
  return err;
}

void constructUdfService(void *argsThread) {
  uv_loop_init(&gUdfdLoop);

  //TODO spawn error
  startUdfd();

  uv_async_init(&gUdfdLoop, &gUdfLoopTaskAync, udfClientAsyncCb);
  uv_async_init(&gUdfdLoop, &gUdfLoopStopAsync, udfStopAsyncCb);
  uv_mutex_init(&gUdfTaskQueueMutex);
  QUEUE_INIT(&gUdfTaskQueue);
  QUEUE_INIT(&gUvProcTaskQueue);
  uv_barrier_wait(&gUdfInitBarrier);
  //TODO return value of uv_run
  uv_run(&gUdfdLoop, UV_RUN_DEFAULT);
  uv_loop_close(&gUdfdLoop);
}


int32_t startUdfService() {
  gUdfcState = UDFC_STATE_STARTNG;
  uv_barrier_init(&gUdfInitBarrier, 2);
  uv_thread_create(&gUdfLoopThread, constructUdfService, 0);
  uv_barrier_wait(&gUdfInitBarrier);  gUdfcState = UDFC_STATE_READY;
  return 0;
}

int32_t stopUdfService() {
  gUdfcState = UDFC_STATE_STOPPING;
  uv_barrier_destroy(&gUdfInitBarrier);
  if (gUdfcState == UDFC_STATE_STOPPING) {
    uv_process_kill(&gUdfdProcess, SIGINT);
  }
  uv_async_send(&gUdfLoopStopAsync);
  uv_thread_join(&gUdfLoopThread);
  uv_mutex_destroy(&gUdfTaskQueueMutex);
  gUdfcState = UDFC_STATUS_FINAL;
  return 0;
}

int32_t udfcRunUvTask(SClientUdfTask *task, int8_t uvTaskType) {
  SClientUvTaskNode *uvTask = NULL;

  createUdfcUvTask(task, uvTaskType, &uvTask);
  queueUvUdfTask(uvTask);
  udfcGetUvTaskResponseResult(task, uvTask);
  if (uvTaskType == UV_TASK_CONNECT) {
    task->session->udfSvcPipe = uvTask->pipe;
  }  taosMemoryFree(uvTask);
  uvTask = NULL;
  return task->errCode;
}

int32_t setupUdf(char udfName[], SEpSet *epSet, UdfHandle *handle) {
  debugPrint("%s", "client setup udf");
  SClientUdfTask *task = taosMemoryMalloc(sizeof(SClientUdfTask));
  task->errCode = 0;
  task->session = taosMemoryMalloc(sizeof(SUdfUvSession));
  task->type = UDF_TASK_SETUP;

  SUdfSetupRequest *req = &task->_setup.req;
  memcpy(req->udfName, udfName, TSDB_FUNC_NAME_LEN);

  int32_t errCode = udfcRunUvTask(task, UV_TASK_CONNECT);
  if (errCode != 0) {
    //TODO: log error
    return -1;
  }

  udfcRunUvTask(task, UV_TASK_REQ_RSP);

  SUdfSetupResponse *rsp = &task->_setup.rsp;
  task->session->severHandle = rsp->udfHandle;
  *handle = task->session;
  int32_t err = task->errCode;
  taosMemoryFree(task);
  return err;
}

int32_t callUdf(UdfHandle handle, int8_t callType, SSDataBlock *input, SUdfInterBuf *state, SUdfInterBuf *state2,
                SSDataBlock* output, SUdfInterBuf *newState) {
  debugPrint("%s", "client call udf");

  SClientUdfTask *task = taosMemoryMalloc(sizeof(SClientUdfTask));
  task->errCode = 0;
  task->session = (SUdfUvSession *) handle;
  task->type = UDF_TASK_CALL;

  SUdfCallRequest *req = &task->_call.req;
  switch (callType) {
    case TSDB_UDF_CALL_AGG_INIT: {
      req->initFirst = 1;
      break;
    }
    case TSDB_UDF_CALL_AGG_PROC: {
      req->block = *input;
      req->interBuf = *state;
      break;
    }
    case TSDB_UDF_CALL_AGG_MERGE: {
      req->interBuf = *state;
      req->interBuf2 = *state2;
      break;
    }
    case TSDB_UDF_CALL_AGG_FIN: {
      req->interBuf = *state;
      break;
    }
    case TSDB_UDF_CALL_SCALA_PROC: {
      req->block = *input;
      break;
    }
  }

  udfcRunUvTask(task, UV_TASK_REQ_RSP);

  SUdfCallResponse *rsp = &task->_call.rsp;
  switch (callType) {
    case TSDB_UDF_CALL_AGG_INIT: {
      *newState = rsp->resultBuf;
      break;
    }
    case TSDB_UDF_CALL_AGG_PROC: {
      *newState = rsp->resultBuf;
      break;
    }
    case TSDB_UDF_CALL_AGG_MERGE: {
      *newState = rsp->resultBuf;
      break;
    }
    case TSDB_UDF_CALL_AGG_FIN: {
      *newState = rsp->resultBuf;
      break;
    }
    case TSDB_UDF_CALL_SCALA_PROC: {
      *output = rsp->resultData;
      break;
    }
  }

  taosMemoryFree(task);
  return task->errCode;
}

//TODO: translate these calls to callUdf
int32_t callUdfAggInit(UdfHandle handle, SUdfInterBuf *interBuf) {
  int8_t callType = TSDB_UDF_CALL_AGG_INIT;

  int32_t err = callUdf(handle, callType, NULL, NULL, NULL, NULL, interBuf);

  return err;
}

// input: block, state
// output: interbuf,
int32_t callUdfAggProcess(UdfHandle handle, SSDataBlock *block, SUdfInterBuf *state, SUdfInterBuf *newState) {
  int8_t callType = TSDB_UDF_CALL_AGG_PROC;
  int32_t err = callUdf(handle, callType, block, state, NULL, NULL, newState);
  return err;
}

// input: interbuf1, interbuf2
// output: resultBuf
int32_t callUdfAggMerge(UdfHandle handle, SUdfInterBuf *interBuf1, SUdfInterBuf *interBuf2, SUdfInterBuf *resultBuf) {
  int8_t callType = TSDB_UDF_CALL_AGG_MERGE;
  int32_t err = callUdf(handle, callType, NULL, interBuf1, interBuf2, NULL, resultBuf);
  return err;
}

// input: interBuf
// output: resultData
int32_t callUdfAggFinalize(UdfHandle handle, SUdfInterBuf *interBuf, SUdfInterBuf *resultData) {
  int8_t callType = TSDB_UDF_CALL_AGG_PROC;
  int32_t err = callUdf(handle, callType, NULL, interBuf, NULL, NULL, resultData);
  return err;
}

// input: block
// output: resultData
int32_t callUdfScalaProcess(UdfHandle handle, SSDataBlock *block, SSDataBlock *resultData) {
  int8_t callType = TSDB_UDF_CALL_SCALA_PROC;
  int32_t err = callUdf(handle, callType, block, NULL, NULL, resultData, NULL);
  return err;
}

int32_t teardownUdf(UdfHandle handle) {
  debugPrint("%s", "client teardown udf");

  SClientUdfTask *task = taosMemoryMalloc(sizeof(SClientUdfTask));
  task->errCode = 0;
  task->session = (SUdfUvSession *) handle;
  task->type = UDF_TASK_TEARDOWN;

  SUdfTeardownRequest *req = &task->_teardown.req;
  req->udfHandle = task->session->severHandle;

  udfcRunUvTask(task, UV_TASK_REQ_RSP);


  SUdfTeardownResponse *rsp = &task->_teardown.rsp;

  int32_t err = task->errCode;

  udfcRunUvTask(task, UV_TASK_DISCONNECT);

  taosMemoryFree(task->session);
  taosMemoryFree(task);

  return err;
}
