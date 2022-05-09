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
#include "fnLog.h"
#include "tudf.h"
#include "tudfInt.h"
#include "tarray.h"
#include "tdatablock.h"
#include "querynodes.h"
#include "builtinsimpl.h"
#include "functionMgt.h"

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

int64_t gUdfTaskSeqNum = 0;
typedef struct SUdfdProxy {
  char udfdPipeName[PATH_MAX + UDF_LISTEN_PIPE_NAME_LEN + 2];
  uv_barrier_t gUdfInitBarrier;

  uv_loop_t gUdfdLoop;
  uv_thread_t gUdfLoopThread;
  uv_async_t gUdfLoopTaskAync;

  uv_async_t gUdfLoopStopAsync;

  uv_mutex_t gUdfTaskQueueMutex;
  int8_t gUdfcState;
  QUEUE gUdfTaskQueue;
  QUEUE gUvProcTaskQueue;

  int8_t initialized;
} SUdfdProxy;

SUdfdProxy gUdfdProxy = {0};

typedef struct SClientUdfUvSession {
  SUdfdProxy *udfc;
  int64_t severHandle;
  uv_pipe_t *udfUvPipe;

  int8_t  outputType;
  int32_t outputLen;
  int32_t bufSize;
} SClientUdfUvSession;

typedef struct SClientUvTaskNode {
  SUdfdProxy *udfc;
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

  SClientUdfUvSession *session;

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
  SClientUdfUvSession *session;
} SClientUvConn;

enum {
  UDFC_STATE_INITAL = 0, // initial state
  UDFC_STATE_STARTNG, // starting after udfcOpen
  UDFC_STATE_READY, // started and begin to receive quests
  UDFC_STATE_STOPPING, // stopping after udfcClose
};

int32_t getUdfdPipeName(char* pipeName, int32_t size) {
  char    dnodeId[8] = {0};
  size_t  dnodeIdSize = sizeof(dnodeId);
  int32_t err = uv_os_getenv(UDF_DNODE_ID_ENV_NAME, dnodeId, &dnodeIdSize);
  if (err != 0) {
    fnError("get dnode id from env. error: %s.", uv_err_name(err));
    dnodeId[0] = '1';
  }
#ifdef _WIN32
  snprintf(pipeName, size, "%s%s", UDF_LISTEN_PIPE_NAME_PREFIX, dnodeId);
#else
  snprintf(pipeName, size, "%s/%s%s", tsDataDir, UDF_LISTEN_PIPE_NAME_PREFIX, dnodeId);
#endif
  fnInfo("get dnode id from env. dnode id: %s. pipe path: %s", dnodeId, pipeName);
  return 0;
}

int32_t encodeUdfSetupRequest(void **buf, const SUdfSetupRequest *setup) {
  int32_t len = 0;
  len += taosEncodeBinary(buf, setup->udfName, TSDB_FUNC_NAME_LEN);
  return len;
}

void* decodeUdfSetupRequest(const void* buf, SUdfSetupRequest *request) {
  buf = taosDecodeBinaryTo(buf, request->udfName, TSDB_FUNC_NAME_LEN);
  return (void*)buf;
}

int32_t encodeUdfInterBuf(void **buf, const SUdfInterBuf* state) {
  int32_t len = 0;
  len += taosEncodeFixedI8(buf, state->numOfResult);
  len += taosEncodeFixedI32(buf, state->bufLen);
  len += taosEncodeBinary(buf, state->buf, state->bufLen);
  return len;
}

void* decodeUdfInterBuf(const void* buf, SUdfInterBuf* state) {
  buf = taosDecodeFixedI8(buf, &state->numOfResult);
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
  buf = POINTER_SHIFT(buf, sizeof(request->msgLen));

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
  len += taosEncodeFixedI8(buf, setupRsp->outputType);
  len += taosEncodeFixedI32(buf, setupRsp->outputLen);
  len += taosEncodeFixedI32(buf, setupRsp->bufSize);
  return len;
}

void* decodeUdfSetupResponse(const void* buf, SUdfSetupResponse* setupRsp) {
  buf = taosDecodeFixedI64(buf, &setupRsp->udfHandle);
  buf = taosDecodeFixedI8(buf, &setupRsp->outputType);
  buf = taosDecodeFixedI32(buf, &setupRsp->outputLen);
  buf = taosDecodeFixedI32(buf, &setupRsp->bufSize);
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
      len += encodeUdfInterBuf(buf, &callRsp->resultBuf);
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
      buf = decodeUdfInterBuf(buf, &callRsp->resultBuf);
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

  if (buf == NULL) {
    len += sizeof(rsp->seqNum);
  } else {
    *(int64_t*)(*buf) = rsp->seqNum;
    *buf = POINTER_SHIFT(*buf, sizeof(rsp->seqNum));
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
  buf = POINTER_SHIFT(buf, sizeof(rsp->msgLen));
  rsp->seqNum = *(int64_t*)(buf);
  buf = POINTER_SHIFT(buf, sizeof(rsp->seqNum));
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

void freeUdfColumnData(SUdfColumnData *data, SUdfColumnMeta *meta) {
  if (IS_VAR_DATA_TYPE(meta->type)) {
    taosMemoryFree(data->varLenCol.varOffsets);
    data->varLenCol.varOffsets = NULL;
    taosMemoryFree(data->varLenCol.payload);
    data->varLenCol.payload = NULL;
  } else {
    taosMemoryFree(data->fixLenCol.nullBitmap);
    data->fixLenCol.nullBitmap = NULL;
    taosMemoryFree(data->fixLenCol.data);
    data->fixLenCol.data = NULL;
  }
}

void freeUdfColumn(SUdfColumn* col) {
  freeUdfColumnData(&col->colData, &col->colMeta);
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
  udfBlock->udfCols = taosMemoryCalloc(udfBlock->numOfCols, sizeof(SUdfColumn*));
  for (int32_t i = 0; i < udfBlock->numOfCols; ++i) {
    udfBlock->udfCols[i] = taosMemoryCalloc(1, sizeof(SUdfColumn));
    SColumnInfoData *col= (SColumnInfoData*)taosArrayGet(block->pDataBlock, i);
    SUdfColumn *udfCol = udfBlock->udfCols[i];
    udfCol->colMeta.type = col->info.type;
    udfCol->colMeta.bytes = col->info.bytes;
    udfCol->colMeta.scale = col->info.scale;
    udfCol->colMeta.precision = col->info.precision;
    udfCol->colData.numOfRows = udfBlock->numOfRows;
    if (IS_VAR_DATA_TYPE(udfCol->colMeta.type)) {
      udfCol->colData.varLenCol.varOffsetsLen = sizeof(int32_t) * udfBlock->numOfRows;
      udfCol->colData.varLenCol.varOffsets = taosMemoryMalloc(udfCol->colData.varLenCol.varOffsetsLen);
      memcpy(udfCol->colData.varLenCol.varOffsets, col->varmeta.offset, udfCol->colData.varLenCol.varOffsetsLen);
      udfCol->colData.varLenCol.payloadLen = colDataGetLength(col, udfBlock->numOfRows);
      udfCol->colData.varLenCol.payload = taosMemoryMalloc(udfCol->colData.varLenCol.payloadLen);
      memcpy(udfCol->colData.varLenCol.payload, col->pData, udfCol->colData.varLenCol.payloadLen);
    } else {
      udfCol->colData.fixLenCol.nullBitmapLen = BitmapLen(udfCol->colData.numOfRows);
      int32_t bitmapLen = udfCol->colData.fixLenCol.nullBitmapLen;
      udfCol->colData.fixLenCol.nullBitmap = taosMemoryMalloc(udfCol->colData.fixLenCol.nullBitmapLen);
      char* bitmap = udfCol->colData.fixLenCol.nullBitmap;
      memcpy(bitmap, col->nullbitmap, bitmapLen);
      udfCol->colData.fixLenCol.dataLen = colDataGetLength(col, udfBlock->numOfRows);
      int32_t dataLen = udfCol->colData.fixLenCol.dataLen;
      udfCol->colData.fixLenCol.data = taosMemoryMalloc(udfCol->colData.fixLenCol.dataLen);
      char* data = udfCol->colData.fixLenCol.data;
      memcpy(data, col->pData, dataLen);
    }
  }
  return 0;
}

int32_t convertUdfColumnToDataBlock(SUdfColumn *udfCol, SSDataBlock *block) {
  block->info.numOfCols = 1;
  block->info.rows = udfCol->colData.numOfRows;
  block->info.hasVarCol = IS_VAR_DATA_TYPE(udfCol->colMeta.type);

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
    col->nullbitmap = taosMemoryMalloc(data->fixLenCol.nullBitmapLen);
    memcpy(col->nullbitmap, data->fixLenCol.nullBitmap, data->fixLenCol.nullBitmapLen);
    col->pData = taosMemoryMalloc(data->fixLenCol.dataLen);
    memcpy(col->pData, data->fixLenCol.data, data->fixLenCol.dataLen);
  } else {
    col->varmeta.offset = taosMemoryMalloc(data->varLenCol.varOffsetsLen);
    memcpy(col->varmeta.offset, data->varLenCol.varOffsets, data->varLenCol.varOffsetsLen);
    col->pData = taosMemoryMalloc(data->varLenCol.payloadLen);
    memcpy(col->pData, data->varLenCol.payload, data->varLenCol.payloadLen);
  }
  return 0;
}

int32_t convertScalarParamToDataBlock(SScalarParam *input, int32_t numOfCols, SSDataBlock *output) {
  output->info.rows = input->numOfRows;
  output->info.numOfCols = numOfCols;
  bool hasVarCol = false;
  for (int32_t i = 0; i < numOfCols; ++i) {
    if (IS_VAR_DATA_TYPE((input+i)->columnData->info.type)) {
      hasVarCol = true;
      break;
    }
  }
  output->info.hasVarCol = hasVarCol;

  //TODO: free the array output->pDataBlock
  output->pDataBlock = taosArrayInit(numOfCols, sizeof(SColumnInfoData));
  for (int32_t i = 0; i < numOfCols; ++i) {
    taosArrayPush(output->pDataBlock, (input + i)->columnData);
  }
  return 0;
}

int32_t convertDataBlockToScalarParm(SSDataBlock *input, SScalarParam *output) {
  if (input->info.numOfCols != 1) {
    fnError("scalar function only support one column");
    return -1;
  }
  output->numOfRows = input->info.rows;
  //TODO: memory
  output->columnData = taosArrayGet(input->pDataBlock, 0);
  return 0;
}

void onUdfcPipeClose(uv_handle_t *handle) {
  SClientUvConn *conn = handle->data;
  if (!QUEUE_EMPTY(&conn->taskQueue)) {
    QUEUE* h = QUEUE_HEAD(&conn->taskQueue);
    SClientUvTaskNode *task = QUEUE_DATA(h, SClientUvTaskNode, connTaskQueue);
    task->errCode = 0;
    QUEUE_REMOVE(&task->procTaskQueue);
    uv_sem_post(&task->taskSem);
  }
  conn->session->udfUvPipe = NULL;
  taosMemoryFree(conn->readBuf.buf);
  taosMemoryFree(conn);
  taosMemoryFree((uv_pipe_t *) handle);
}

int32_t udfcGetUdfTaskResultFromUvTask(SClientUdfTask *task, SClientUvTaskNode *uvTask) {
  fnDebug("udfc get uv task result. task: %p, uvTask: %p", task, uvTask);
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
      fnError("udfc allocate buffer failure. size: %d", msgHeadSize);
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
      fnError("udfc re-allocate buffer failure. size: %d", connBuf->cap);
      buf->base = NULL;
      buf->len = 0;
    }
  }

  fnTrace("conn buf cap - len - total : %d - %d - %d", connBuf->cap, connBuf->len, connBuf->total);

}

bool isUdfcUvMsgComplete(SClientConnBuf *connBuf) {
  if (connBuf->total == -1 && connBuf->len >= sizeof(int32_t)) {
    connBuf->total = *(int32_t *) (connBuf->buf);
  }
  if (connBuf->len == connBuf->cap && connBuf->total == connBuf->cap) {
    fnTrace("udfc complete message is received, now handle it");
    return true;
  }
  return false;
}

void udfcUvHandleRsp(SClientUvConn *conn) {
  SClientConnBuf *connBuf = &conn->readBuf;
  int64_t seqNum = *(int64_t *) (connBuf->buf + sizeof(int32_t)); // msglen then seqnum

  if (QUEUE_EMPTY(&conn->taskQueue)) {
    fnError("udfc no task waiting for response on connection");
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
        fnError("udfc more than one task waiting for the same response");
        continue;
      }
    }
    h = QUEUE_NEXT(h);
    task = QUEUE_DATA(h, SClientUvTaskNode, connTaskQueue);
  }

  if (taskFound) {
    taskFound->rspBuf = uv_buf_init(connBuf->buf, connBuf->len);
    QUEUE_REMOVE(&taskFound->connTaskQueue);
    QUEUE_REMOVE(&taskFound->procTaskQueue);
    uv_sem_post(&taskFound->taskSem);
  } else {
    fnError("no task is waiting for the response.");
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
    QUEUE_REMOVE(&task->connTaskQueue);
    QUEUE_REMOVE(&task->procTaskQueue);
    uv_sem_post(&task->taskSem);
  }

  uv_close((uv_handle_t *) conn->pipe, onUdfcPipeClose);
}

void onUdfcRead(uv_stream_t *client, ssize_t nread, const uv_buf_t *buf) {
  fnTrace("udfc client %p, client read from pipe. nread: %zd", client, nread);
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
    fnError("udfc client pipe %p read error: %zd, %s.", client, nread, uv_strerror(nread));
    if (nread == UV_EOF) {
      fnError("\tudfc client pipe %p closed", client);
    }
    udfcUvHandleError(conn);
  }

}

void onUdfClientWrite(uv_write_t *write, int status) {
  SClientUvTaskNode *uvTask = write->data;
  uv_pipe_t *pipe = uvTask->pipe;
  if (status == 0) {
    SClientUvConn *conn = pipe->data;
    QUEUE_INSERT_TAIL(&conn->taskQueue, &uvTask->connTaskQueue);
  } else {
    fnError("udfc client %p write error.", pipe);
  }
  fnTrace("udfc client %p write length:%zu", pipe, uvTask->reqBuf.len);
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

int32_t udfcCreateUvTask(SClientUdfTask *task, int8_t uvTaskType, SClientUvTaskNode **pUvTask) {
  SClientUvTaskNode *uvTask = taosMemoryCalloc(1, sizeof(SClientUvTaskNode));
  uvTask->type = uvTaskType;
  uvTask->udfc = task->session->udfc;

  if (uvTaskType == UV_TASK_CONNECT) {
  } else if (uvTaskType == UV_TASK_REQ_RSP) {
    uvTask->pipe = task->session->udfUvPipe;
    SUdfRequest request;
    request.type = task->type;
    request.seqNum =   atomic_fetch_add_64(&gUdfTaskSeqNum, 1);

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
    void *bufBegin = taosMemoryMalloc(bufLen);
    void *buf = bufBegin;
    encodeUdfRequest(&buf, &request);
    uvTask->reqBuf = uv_buf_init(bufBegin, bufLen);
    uvTask->seqNum = request.seqNum;
  } else if (uvTaskType == UV_TASK_DISCONNECT) {
    uvTask->pipe = task->session->udfUvPipe;
  }
  uv_sem_init(&uvTask->taskSem, 0);

  *pUvTask = uvTask;
  return 0;
}

int32_t udfcQueueUvTask(SClientUvTaskNode *uvTask) {
  fnTrace("queue uv task to event loop, task: %d, %p", uvTask->type, uvTask);
  SUdfdProxy *udfc = uvTask->udfc;
  uv_mutex_lock(&udfc->gUdfTaskQueueMutex);
  QUEUE_INSERT_TAIL(&udfc->gUdfTaskQueue, &uvTask->recvTaskQueue);
  uv_mutex_unlock(&udfc->gUdfTaskQueueMutex);
  uv_async_send(&udfc->gUdfLoopTaskAync);

  uv_sem_wait(&uvTask->taskSem);
  fnInfo("udfc uv task finished. task: %d, %p", uvTask->type, uvTask);
  uv_sem_destroy(&uvTask->taskSem);

  return 0;
}

int32_t udfcStartUvTask(SClientUvTaskNode *uvTask) {
  fnTrace("event loop start uv task. task: %d, %p", uvTask->type, uvTask);
  switch (uvTask->type) {
    case UV_TASK_CONNECT: {
      uv_pipe_t *pipe = taosMemoryMalloc(sizeof(uv_pipe_t));
      uv_pipe_init(&uvTask->udfc->gUdfdLoop, pipe, 0);
      uvTask->pipe = pipe;

      SClientUvConn *conn = taosMemoryCalloc(1, sizeof(SClientUvConn));
      conn->pipe = pipe;
      conn->readBuf.len = 0;
      conn->readBuf.cap = 0;
      conn->readBuf.buf = 0;
      conn->readBuf.total = -1;
      QUEUE_INIT(&conn->taskQueue);

      pipe->data = conn;

      uv_connect_t *connReq = taosMemoryMalloc(sizeof(uv_connect_t));
      connReq->data = uvTask;
      uv_pipe_connect(connReq, pipe, uvTask->udfc->udfdPipeName, onUdfClientConnect);
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
  SUdfdProxy *udfc = async->data;
  QUEUE wq;

  uv_mutex_lock(&udfc->gUdfTaskQueueMutex);
  QUEUE_MOVE(&udfc->gUdfTaskQueue, &wq);
  uv_mutex_unlock(&udfc->gUdfTaskQueueMutex);

  while (!QUEUE_EMPTY(&wq)) {
    QUEUE* h = QUEUE_HEAD(&wq);
    QUEUE_REMOVE(h);
    SClientUvTaskNode *task = QUEUE_DATA(h, SClientUvTaskNode, recvTaskQueue);
    udfcStartUvTask(task);
    QUEUE_INSERT_TAIL(&udfc->gUvProcTaskQueue, &task->procTaskQueue);
  }

}

void cleanUpUvTasks(SUdfdProxy *udfc) {
  fnDebug("clean up uv tasks")
  QUEUE wq;

  uv_mutex_lock(&udfc->gUdfTaskQueueMutex);
  QUEUE_MOVE(&udfc->gUdfTaskQueue, &wq);
  uv_mutex_unlock(&udfc->gUdfTaskQueueMutex);

  while (!QUEUE_EMPTY(&wq)) {
    QUEUE* h = QUEUE_HEAD(&wq);
    QUEUE_REMOVE(h);
    SClientUvTaskNode *task = QUEUE_DATA(h, SClientUvTaskNode, recvTaskQueue);
    if (udfc->gUdfcState == UDFC_STATE_STOPPING) {
      task->errCode = UDFC_CODE_STOPPING;
    }
    uv_sem_post(&task->taskSem);
  }

  while (!QUEUE_EMPTY(&udfc->gUvProcTaskQueue)) {
    QUEUE* h = QUEUE_HEAD(&udfc->gUvProcTaskQueue);
    QUEUE_REMOVE(h);
    SClientUvTaskNode *task = QUEUE_DATA(h, SClientUvTaskNode, procTaskQueue);
    if (udfc->gUdfcState == UDFC_STATE_STOPPING) {
      task->errCode = UDFC_CODE_STOPPING;
    }
    uv_sem_post(&task->taskSem);
  }
}

void udfStopAsyncCb(uv_async_t *async) {
  SUdfdProxy *udfc = async->data;
  cleanUpUvTasks(udfc);
  if (udfc->gUdfcState == UDFC_STATE_STOPPING) {
    uv_stop(&udfc->gUdfdLoop);
  }
}

void constructUdfService(void *argsThread) {
  SUdfdProxy *udfc = (SUdfdProxy*)argsThread;
  uv_loop_init(&udfc->gUdfdLoop);

  uv_async_init(&udfc->gUdfdLoop, &udfc->gUdfLoopTaskAync, udfClientAsyncCb);
  udfc->gUdfLoopTaskAync.data = udfc;
  uv_async_init(&udfc->gUdfdLoop, &udfc->gUdfLoopStopAsync, udfStopAsyncCb);
  udfc->gUdfLoopStopAsync.data = udfc;
  uv_mutex_init(&udfc->gUdfTaskQueueMutex);
  QUEUE_INIT(&udfc->gUdfTaskQueue);
  QUEUE_INIT(&udfc->gUvProcTaskQueue);
  uv_barrier_wait(&udfc->gUdfInitBarrier);
  //TODO return value of uv_run
  uv_run(&udfc->gUdfdLoop, UV_RUN_DEFAULT);
  uv_loop_close(&udfc->gUdfdLoop);
}

int32_t udfcOpen() {
  int8_t old = atomic_val_compare_exchange_8(&gUdfdProxy.initialized, 0, 1);
  if (old == 1) {
    return 0;
  }
  SUdfdProxy *proxy = &gUdfdProxy;
  getUdfdPipeName(proxy->udfdPipeName, sizeof(proxy->udfdPipeName));
  proxy->gUdfcState = UDFC_STATE_STARTNG;
  uv_barrier_init(&proxy->gUdfInitBarrier, 2);
  uv_thread_create(&proxy->gUdfLoopThread, constructUdfService, proxy);
  atomic_store_8(&proxy->gUdfcState, UDFC_STATE_READY);
  proxy->gUdfcState = UDFC_STATE_READY;
  uv_barrier_wait(&proxy->gUdfInitBarrier);
  fnInfo("udfc initialized")
  return 0;
}

int32_t udfcClose() {
  int8_t old = atomic_val_compare_exchange_8(&gUdfdProxy.initialized, 1, 0);
  if (old == 0) {
    return 0;
  }

  SUdfdProxy *udfc = &gUdfdProxy;
  udfc->gUdfcState = UDFC_STATE_STOPPING;
  uv_async_send(&udfc->gUdfLoopStopAsync);
  uv_thread_join(&udfc->gUdfLoopThread);
  uv_mutex_destroy(&udfc->gUdfTaskQueueMutex);
  uv_barrier_destroy(&udfc->gUdfInitBarrier);
  udfc->gUdfcState = UDFC_STATE_INITAL;
  fnInfo("udfc cleaned up");
  return 0;
}

int32_t udfcRunUdfUvTask(SClientUdfTask *task, int8_t uvTaskType) {
  SClientUvTaskNode *uvTask = NULL;

  udfcCreateUvTask(task, uvTaskType, &uvTask);
  udfcQueueUvTask(uvTask);
  udfcGetUdfTaskResultFromUvTask(task, uvTask);
  if (uvTaskType == UV_TASK_CONNECT) {
    task->session->udfUvPipe = uvTask->pipe;
    SClientUvConn *conn = uvTask->pipe->data;
    conn->session = task->session;
  }
  taosMemoryFree(uvTask);
  uvTask = NULL;
  return task->errCode;
}

int32_t setupUdf(char udfName[], UdfcFuncHandle *funcHandle) {
  fnInfo("udfc setup udf. udfName: %s", udfName);
  if (gUdfdProxy.gUdfcState != UDFC_STATE_READY) {
    return UDFC_CODE_INVALID_STATE;
  }
  SClientUdfTask *task = taosMemoryCalloc(1,sizeof(SClientUdfTask));
  task->errCode = 0;
  task->session = taosMemoryCalloc(1, sizeof(SClientUdfUvSession));
  task->session->udfc = &gUdfdProxy;
  task->type = UDF_TASK_SETUP;

  SUdfSetupRequest *req = &task->_setup.req;
  memcpy(req->udfName, udfName, TSDB_FUNC_NAME_LEN);

  int32_t errCode = udfcRunUdfUvTask(task, UV_TASK_CONNECT);
  if (errCode != 0) {
    fnError("failed to connect to pipe. udfName: %s, pipe: %s", udfName, (&gUdfdProxy)->udfdPipeName);
    return UDFC_CODE_CONNECT_PIPE_ERR;
  }

  udfcRunUdfUvTask(task, UV_TASK_REQ_RSP);

  SUdfSetupResponse *rsp = &task->_setup.rsp;
  task->session->severHandle = rsp->udfHandle;
  task->session->outputType = rsp->outputType;
  task->session->outputLen = rsp->outputLen;
  task->session->bufSize = rsp->bufSize;
  if (task->errCode != 0) {
    fnError("failed to setup udf. err: %d", task->errCode)
  } else {
    fnInfo("sucessfully setup udf func handle. handle: %p", task->session);
    *funcHandle = task->session;
  }
  int32_t err = task->errCode;
  taosMemoryFree(task);
  return err;
}

int32_t callUdf(UdfcFuncHandle handle, int8_t callType, SSDataBlock *input, SUdfInterBuf *state, SUdfInterBuf *state2,
                SSDataBlock* output, SUdfInterBuf *newState) {
  fnTrace("udfc call udf. callType: %d, funcHandle: %p", callType, handle);
  SClientUdfUvSession *session = (SClientUdfUvSession *) handle;
  if (session->udfUvPipe == NULL) {
    fnError("No pipe to udfd");
    return UDFC_CODE_NO_PIPE;
  }
  SClientUdfTask *task = taosMemoryCalloc(1, sizeof(SClientUdfTask));
  task->errCode = 0;
  task->session = (SClientUdfUvSession *) handle;
  task->type = UDF_TASK_CALL;

  SUdfCallRequest *req = &task->_call.req;
  req->udfHandle = task->session->severHandle;
  req->callType = callType;

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

  udfcRunUdfUvTask(task, UV_TASK_REQ_RSP);

  if (task->errCode != 0) {
    fnError("call udf failure. err: %d", task->errCode);
  } else {
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
  };
  int err = task->errCode;
  taosMemoryFree(task);
  return err;
}

int32_t callUdfAggInit(UdfcFuncHandle handle, SUdfInterBuf *interBuf) {
  int8_t callType = TSDB_UDF_CALL_AGG_INIT;

  int32_t err = callUdf(handle, callType, NULL, NULL, NULL, NULL, interBuf);

  return err;
}

// input: block, state
// output: interbuf,
int32_t callUdfAggProcess(UdfcFuncHandle handle, SSDataBlock *block, SUdfInterBuf *state, SUdfInterBuf *newState) {
  int8_t callType = TSDB_UDF_CALL_AGG_PROC;
  int32_t err = callUdf(handle, callType, block, state, NULL, NULL, newState);
  return err;
}

// input: interbuf1, interbuf2
// output: resultBuf
int32_t callUdfAggMerge(UdfcFuncHandle handle, SUdfInterBuf *interBuf1, SUdfInterBuf *interBuf2, SUdfInterBuf *resultBuf) {
  int8_t callType = TSDB_UDF_CALL_AGG_MERGE;
  int32_t err = callUdf(handle, callType, NULL, interBuf1, interBuf2, NULL, resultBuf);
  return err;
}

// input: interBuf
// output: resultData
int32_t callUdfAggFinalize(UdfcFuncHandle handle, SUdfInterBuf *interBuf, SUdfInterBuf *resultData) {
  int8_t callType = TSDB_UDF_CALL_AGG_FIN;
  int32_t err = callUdf(handle, callType, NULL, interBuf, NULL, NULL, resultData);
  return err;
}

int32_t callUdfScalarFunc(UdfcFuncHandle handle, SScalarParam *input, int32_t numOfCols, SScalarParam* output) {
  int8_t callType = TSDB_UDF_CALL_SCALA_PROC;
  SSDataBlock inputBlock = {0};
  convertScalarParamToDataBlock(input, numOfCols, &inputBlock);
  SSDataBlock resultBlock = {0};
  int32_t err = callUdf(handle, callType, &inputBlock, NULL, NULL, &resultBlock, NULL);
  if (err == 0) {
    convertDataBlockToScalarParm(&resultBlock, output);
  }
  return err;
}

int32_t teardownUdf(UdfcFuncHandle handle) {
  fnInfo("tear down udf. udf func handle: %p", handle);

  SClientUdfUvSession *session = (SClientUdfUvSession *) handle;
  if (session->udfUvPipe == NULL) {
    fnError("pipe to udfd does not exist");
    return UDFC_CODE_NO_PIPE;
  }

  SClientUdfTask *task = taosMemoryCalloc(1, sizeof(SClientUdfTask));
  task->errCode = 0;
  task->session = session;
  task->type = UDF_TASK_TEARDOWN;

  SUdfTeardownRequest *req = &task->_teardown.req;
  req->udfHandle = task->session->severHandle;

  udfcRunUdfUvTask(task, UV_TASK_REQ_RSP);

  SUdfTeardownResponse *rsp = &task->_teardown.rsp;

  int32_t err = task->errCode;

  udfcRunUdfUvTask(task, UV_TASK_DISCONNECT);

  taosMemoryFree(task->session);
  taosMemoryFree(task);

  return err;
}

//memory layout |---SUdfAggRes----|-----final result-----|---inter result----|
typedef struct SUdfAggRes {
  SClientUdfUvSession *session;
  int8_t finalResNum;
  int8_t interResNum;
  char* finalResBuf;
  char* interResBuf;
} SUdfAggRes;

bool udfAggGetEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  if (fmIsScalarFunc(pFunc->funcId)) {
    return false;
  }
  pEnv->calcMemSize = sizeof(SUdfAggRes) + pFunc->node.resType.bytes + pFunc->udfBufSize;
  return true;
}

bool udfAggInit(struct SqlFunctionCtx *pCtx, struct SResultRowEntryInfo* pResultCellInfo) {
  if (functionSetup(pCtx, pResultCellInfo) != true) {
    return false;
  }
  UdfcFuncHandle handle;
  if (setupUdf((char*)pCtx->udfName, &handle) != 0) {
    return false;
  }
  SClientUdfUvSession *session = (SClientUdfUvSession *)handle;
  SUdfAggRes *udfRes = (SUdfAggRes*)GET_ROWCELL_INTERBUF(pResultCellInfo);
  int32_t envSize = sizeof(SUdfAggRes) + session->outputLen + session->bufSize;
  memset(udfRes, 0, envSize);

  udfRes->finalResBuf = (char*)udfRes + sizeof(SUdfAggRes);
  udfRes->interResBuf = (char*)udfRes + sizeof(SUdfAggRes) + session->outputLen;

  udfRes->session = (SClientUdfUvSession *)handle;
  SUdfInterBuf buf = {0};
  if (callUdfAggInit(handle, &buf) != 0) {
    return false;
  }
  udfRes->interResNum = buf.numOfResult;
  memcpy(udfRes->interResBuf, buf.buf, buf.bufLen);
  return true;
}

int32_t udfAggProcess(struct SqlFunctionCtx *pCtx) {
  SInputColumnInfoData* pInput = &pCtx->input;
  int32_t numOfCols = pInput->numOfInputCols;

  SUdfAggRes* udfRes = (SUdfAggRes *)GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  SClientUdfUvSession *session = udfRes->session;
  udfRes->finalResBuf = (char*)udfRes + sizeof(SUdfAggRes);
  udfRes->interResBuf = (char*)udfRes + sizeof(SUdfAggRes) + session->outputLen;

  int32_t start = pInput->startRowIndex;
  int32_t numOfRows = pInput->numOfRows;


  SSDataBlock tempBlock = {0};
  tempBlock.info.numOfCols = numOfCols;
  tempBlock.info.rows = numOfRows;
  tempBlock.info.uid = pInput->uid;
  bool hasVarCol = false;
  tempBlock.pDataBlock = taosArrayInit(numOfCols, sizeof(SColumnInfoData));

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData *col = pInput->pData[i];
    if (IS_VAR_DATA_TYPE(col->info.type)) {
      hasVarCol = true;
    }
    taosArrayPush(tempBlock.pDataBlock, col);
  }
  tempBlock.info.hasVarCol = hasVarCol;

  SSDataBlock *inputBlock = blockDataExtractBlock(&tempBlock, start, numOfRows);

  SUdfInterBuf state = {.buf = udfRes->interResBuf,
                        .bufLen = session->bufSize,
                        .numOfResult = udfRes->interResNum};
  SUdfInterBuf newState = {0};

  callUdfAggProcess(session, inputBlock, &state, &newState);

  udfRes->interResNum = newState.numOfResult;
  memcpy(udfRes->interResBuf, newState.buf, newState.bufLen);

  if (newState.numOfResult == 1 || state.numOfResult == 1) {
    GET_RES_INFO(pCtx)->numOfRes = 1;
  }

  blockDataDestroy(inputBlock);

  taosArrayDestroy(tempBlock.pDataBlock);

  taosMemoryFree(newState.buf);
  return 0;
}

int32_t udfAggFinalize(struct SqlFunctionCtx *pCtx, SSDataBlock* pBlock) {
  SUdfAggRes* udfRes = (SUdfAggRes *)GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  SClientUdfUvSession *session = udfRes->session;
  udfRes->finalResBuf = (char*)udfRes + sizeof(SUdfAggRes);
  udfRes->interResBuf = (char*)udfRes + sizeof(SUdfAggRes) + session->outputLen;


  SUdfInterBuf resultBuf = {0};
  SUdfInterBuf state = {.buf = udfRes->interResBuf,
                        .bufLen = session->bufSize,
                        .numOfResult = udfRes->interResNum};
  callUdfAggFinalize(session, &state, &resultBuf);

  udfRes->finalResBuf = resultBuf.buf;
  udfRes->finalResNum = resultBuf.numOfResult;

  teardownUdf(session);

  if (resultBuf.numOfResult == 1) {
    GET_RES_INFO(pCtx)->numOfRes = 1;
  }
  return functionFinalizeWithResultBuf(pCtx, pBlock, udfRes->finalResBuf);
}