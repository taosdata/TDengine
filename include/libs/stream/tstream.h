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
#include "tdatablock.h"
#include "tmsg.h"
#include "tmsgcb.h"
#include "tqueue.h"
#include "trpc.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifndef _TSTREAM_H_
#define _TSTREAM_H_

typedef struct SStreamTask SStreamTask;

enum {
  TASK_STATUS__IDLE = 1,
  TASK_STATUS__EXECUTING,
  TASK_STATUS__CLOSING,
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
  STREAM_CREATED_BY__USER = 1,
  STREAM_CREATED_BY__SMA,
};

enum {
  STREAM_INPUT__DATA_SUBMIT = 1,
  STREAM_INPUT__DATA_BLOCK,
  STREAM_INPUT__CHECKPOINT,
};

typedef struct {
  int8_t type;

  int32_t sourceVg;
  int64_t sourceVer;

  int32_t*    dataRef;
  SSubmitReq* data;
} SStreamDataSubmit;

typedef struct {
  int8_t type;

  int32_t sourceVg;
  int64_t sourceVer;

  SArray* blocks;  // SArray<SSDataBlock*>
} SStreamDataBlock;

typedef struct {
  int8_t type;
} SStreamCheckpoint;

static FORCE_INLINE SStreamDataSubmit* streamDataSubmitNew(SSubmitReq* pReq) {
  SStreamDataSubmit* pDataSubmit = (SStreamDataSubmit*)taosAllocateQitem(sizeof(SStreamDataSubmit), DEF_QITEM);
  if (pDataSubmit == NULL) return NULL;
  pDataSubmit->dataRef = (int32_t*)taosMemoryMalloc(sizeof(int32_t));
  if (pDataSubmit->dataRef == NULL) goto FAIL;
  pDataSubmit->data = pReq;
  *pDataSubmit->dataRef = 1;
  pDataSubmit->type = STREAM_INPUT__DATA_SUBMIT;
  return pDataSubmit;
FAIL:
  taosFreeQitem(pDataSubmit);
  return NULL;
}

static FORCE_INLINE void streamDataSubmitRefInc(SStreamDataSubmit* pDataSubmit) {
  //
  atomic_add_fetch_32(pDataSubmit->dataRef, 1);
}

static FORCE_INLINE void streamDataSubmitRefDec(SStreamDataSubmit* pDataSubmit) {
  int32_t ref = atomic_sub_fetch_32(pDataSubmit->dataRef, 1);
  ASSERT(ref >= 0);
  if (ref == 0) {
    taosMemoryFree(pDataSubmit->data);
    taosMemoryFree(pDataSubmit->dataRef);
  }
}

int32_t streamDataBlockEncode(void** buf, const SStreamDataBlock* pOutput);
void*   streamDataBlockDecode(const void* buf, SStreamDataBlock* pInput);

typedef struct {
  int8_t parallelizable;
  char*  qmsg;
  // followings are not applicable to encoder and decoder
  void* inputHandle;
  void* executor;
} STaskExec;

typedef struct {
  int32_t taskId;
} STaskDispatcherInplace;

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
  TASK_DISPATCH__INPLACE,
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

struct SStreamTask {
  int64_t streamId;
  int32_t taskId;
  int8_t  inputType;
  int8_t  status;

  int8_t  sourceType;
  int8_t  execType;
  int8_t  sinkType;
  int8_t  dispatchType;
  int16_t dispatchMsgType;

  int32_t nodeId;
  SEpSet  epSet;

  // source preprocess

  // exec
  STaskExec exec;

  // local sink
  union {
    STaskSinkTb    tbSink;
    STaskSinkSma   smaSink;
    STaskSinkFetch fetchSink;
  };

  // dispatch
  union {
    STaskDispatcherInplace inplaceDispatcher;
    STaskDispatcherFixedEp fixedEpDispatcher;
    STaskDispatcherShuffle shuffleDispatcher;
  };

  int8_t inputStatus;
  int8_t outputStatus;

  STaosQueue* inputQ;
  STaosQall*  inputQAll;
  STaosQueue* outputQ;
  STaosQall*  outputQAll;

  // application storage
  void* ahandle;
};

SStreamTask* tNewSStreamTask(int64_t streamId);
int32_t      tEncodeSStreamTask(SEncoder* pEncoder, const SStreamTask* pTask);
int32_t      tDecodeSStreamTask(SDecoder* pDecoder, SStreamTask* pTask);
void         tFreeSStreamTask(SStreamTask* pTask);

typedef struct {
  // SMsgHead     head;
  SStreamTask* task;
} SStreamTaskDeployReq;

typedef struct {
  int32_t reserved;
} SStreamTaskDeployRsp;

typedef struct {
  // SMsgHead head;
  int64_t streamId;
  int32_t taskId;
  SArray* data;  // SArray<SSDataBlock>
} SStreamTaskExecReq;

int32_t tEncodeSStreamTaskExecReq(void** buf, const SStreamTaskExecReq* pReq);
void*   tDecodeSStreamTaskExecReq(const void* buf, SStreamTaskExecReq* pReq);
void    tFreeSStreamTaskExecReq(SStreamTaskExecReq* pReq);

typedef struct {
  int32_t reserved;
} SStreamTaskExecRsp;

typedef struct {
  // SMsgHead head;
  int64_t streamId;
  int64_t version;
  SArray* res;  // SArray<SSDataBlock>
} SStreamSinkReq;

typedef struct {
  SMsgHead head;
  int64_t  streamId;
  int32_t  taskId;
} SStreamTaskRunReq;

typedef struct {
  int64_t streamId;
  int32_t taskId;
  int32_t sourceTaskId;
  int32_t sourceVg;
#if 0
  int64_t sourceVer;
#endif
  SArray* data;  // SArray<SSDataBlock>
} SStreamDispatchReq;

typedef struct {
  int64_t streamId;
  int32_t taskId;
  int8_t  inputStatus;
} SStreamDispatchRsp;

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

int32_t streamEnqueueDataSubmit(SStreamTask* pTask, SStreamDataSubmit* input);
int32_t streamEnqueueDataBlk(SStreamTask* pTask, SStreamDataBlock* input);
int32_t streamDequeueOutput(SStreamTask* pTask, void** output);

int32_t streamTaskRun(SStreamTask* pTask);

int32_t streamTaskHandleInput(SStreamTask* pTask, void* data);

int32_t streamTaskProcessRunReq(SStreamTask* pTask, SMsgCb* pMsgCb);
int32_t streamProcessDispatchReq(SStreamTask* pTask, SMsgCb* pMsgCb, SStreamDispatchReq* pReq, SRpcMsg* pMsg);
int32_t streamProcessDispatchRsp(SStreamTask* pTask, SMsgCb* pMsgCb, SStreamDispatchRsp* pRsp);
int32_t streamProcessRecoverReq(SStreamTask* pTask, SMsgCb* pMsgCb, SStreamTaskRecoverReq* pReq, SRpcMsg* pMsg);
int32_t streamProcessRecoverRsp(SStreamTask* pTask, SStreamTaskRecoverRsp* pRsp);

#ifdef __cplusplus
}
#endif

#endif /* ifndef _TSTREAM_H_ */
