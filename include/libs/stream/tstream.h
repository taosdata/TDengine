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

#include "tdatablock.h"
#include "tmsg.h"
#include "tmsgcb.h"
#include "trpc.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifndef _TSTREAM_H_
#define _TSTREAM_H_

typedef struct SStreamTask SStreamTask;

enum {
  STREAM_TASK_STATUS__RUNNING = 1,
  STREAM_TASK_STATUS__STOP,
};

enum {
  STREAM_CREATED_BY__USER = 1,
  STREAM_CREATED_BY__SMA,
};

typedef struct {
  int32_t nodeId;  // 0 for snode
  SEpSet  epSet;
} SStreamTaskEp;

typedef struct {
  void* inputHandle;
  void* executor;
} SStreamRunner;

typedef struct {
  int8_t parallelizable;
  char*  qmsg;
  // followings are not applicable to encoder and decoder
  int8_t         numOfRunners;
  SStreamRunner* runners;
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

struct SStreamTask {
  int64_t streamId;
  int32_t taskId;
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

int32_t streamExecTask(SStreamTask* pTask, SMsgCb* pMsgCb, const void* input, int32_t inputType, int32_t workId);

#ifdef __cplusplus
}
#endif

#endif /* ifndef _TSTREAM_H_ */
