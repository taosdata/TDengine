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

#include "tstream.h"
#include "executor.h"

int32_t streamExecTask(SStreamTask* pTask, SMsgCb* pMsgCb, const void* input, int32_t inputType, int32_t workId) {
  SArray* pRes = NULL;
  // source
  if (inputType == STREAM_DATA_TYPE_SUBMIT_BLOCK && pTask->sourceType != TASK_SOURCE__SCAN) return 0;

  // exec
  if (pTask->execType == TASK_EXEC__EXEC) {
    ASSERT(workId < pTask->exec.numOfRunners);
    void* exec = pTask->exec.runners[workId].executor;
    pRes = taosArrayInit(0, sizeof(SSDataBlock));
    if (inputType == STREAM_DATA_TYPE_SUBMIT_BLOCK) {
      qSetStreamInput(exec, input, inputType);
      while (1) {
        SSDataBlock* output;
        uint64_t     ts;
        if (qExecTask(exec, &output, &ts) < 0) {
          ASSERT(false);
        }
        if (output == NULL) {
          break;
        }
        taosArrayPush(pRes, output);
      }
    } else if (inputType == STREAM_DATA_TYPE_SSDATA_BLOCK) {
      const SArray* blocks = (const SArray*)input;
      int32_t       sz = taosArrayGetSize(blocks);
      for (int32_t i = 0; i < sz; i++) {
        SSDataBlock* pBlock = taosArrayGet(blocks, i);
        qSetStreamInput(exec, pBlock, inputType);
        while (1) {
          SSDataBlock* output;
          uint64_t     ts;
          if (qExecTask(exec, &output, &ts) < 0) {
            ASSERT(false);
          }
          if (output == NULL) {
            break;
          }
          taosArrayPush(pRes, output);
        }
      }
    } else {
      ASSERT(0);
    }
  } else {
    ASSERT(inputType == STREAM_DATA_TYPE_SSDATA_BLOCK);
    pRes = (SArray*)input;
  }

  // sink
  if (pTask->sinkType == TASK_SINK__TABLE) {
    //
  } else if (pTask->sinkType == TASK_SINK__SMA) {
    //
  } else if (pTask->sinkType == TASK_SINK__FETCH) {
    //
  } else if (pTask->sinkType == TASK_SINK__SHOW) {
    blockDebugShowData(pRes);
  } else {
    ASSERT(pTask->sinkType == TASK_SINK__NONE);
  }

  // dispatch
  if (pTask->dispatchType != TASK_DISPATCH__NONE) {
    SStreamTaskExecReq req = {
        .streamId = pTask->streamId,
        .taskId = pTask->taskId,
        .data = pRes,
    };

    int32_t tlen = sizeof(SMsgHead) + tEncodeSStreamTaskExecReq(NULL, &req);
    void*   buf = rpcMallocCont(tlen);

    if (buf == NULL) {
      return -1;
    }
    void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
    tEncodeSStreamTaskExecReq(&abuf, &req);

    SRpcMsg dispatchMsg = {
        .pCont = buf,
        .contLen = tlen,
        .code = 0,
        .msgType = pTask->dispatchMsgType,
    };
    if (pTask->dispatchType == TASK_DISPATCH__INPLACE) {
      int32_t qType;
      if (pTask->dispatchMsgType == TDMT_VND_TASK_PIPE_EXEC || pTask->dispatchMsgType == TDMT_SND_TASK_PIPE_EXEC) {
        qType = FETCH_QUEUE;
      } else if (pTask->dispatchMsgType == TDMT_VND_TASK_MERGE_EXEC ||
                 pTask->dispatchMsgType == TDMT_SND_TASK_MERGE_EXEC) {
        qType = MERGE_QUEUE;
      } else if (pTask->dispatchMsgType == TDMT_VND_TASK_WRITE_EXEC) {
        qType = WRITE_QUEUE;
      } else {
        ASSERT(0);
      }
      tmsgPutToQueue(pMsgCb, qType, &dispatchMsg);
    } else if (pTask->dispatchType == TASK_DISPATCH__FIXED) {
      ((SMsgHead*)buf)->vgId = pTask->fixedEpDispatcher.nodeId;
      SEpSet* pEpSet = &pTask->fixedEpDispatcher.epSet;
      tmsgSendReq(pMsgCb, pEpSet, &dispatchMsg);
    } else if (pTask->dispatchType == TASK_DISPATCH__SHUFFLE) {
      // TODO
    } else {
      ASSERT(0);
    }
  }
  return 0;
}

int32_t tEncodeSStreamTaskExecReq(void** buf, const SStreamTaskExecReq* pReq) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, pReq->streamId);
  tlen += taosEncodeFixedI32(buf, pReq->taskId);
  tlen += tEncodeDataBlocks(buf, pReq->data);
  return tlen;
}

void* tDecodeSStreamTaskExecReq(const void* buf, SStreamTaskExecReq* pReq) {
  buf = taosDecodeFixedI64(buf, &pReq->streamId);
  buf = taosDecodeFixedI32(buf, &pReq->taskId);
  buf = tDecodeDataBlocks(buf, &pReq->data);
  return (void*)buf;
}

void tFreeSStreamTaskExecReq(SStreamTaskExecReq* pReq) { taosArrayDestroy(pReq->data); }

SStreamTask* tNewSStreamTask(int64_t streamId) {
  SStreamTask* pTask = (SStreamTask*)taosMemoryCalloc(1, sizeof(SStreamTask));
  if (pTask == NULL) {
    return NULL;
  }
  pTask->taskId = tGenIdPI32();
  pTask->streamId = streamId;
  pTask->status = STREAM_TASK_STATUS__RUNNING;
  /*pTask->qmsg = NULL;*/
  return pTask;
}

int32_t tEncodeSStreamTask(SCoder* pEncoder, const SStreamTask* pTask) {
  /*if (tStartEncode(pEncoder) < 0) return -1;*/
  if (tEncodeI64(pEncoder, pTask->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pTask->taskId) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->status) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->sourceType) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->execType) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->sinkType) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->dispatchType) < 0) return -1;
  if (tEncodeI16(pEncoder, pTask->dispatchMsgType) < 0) return -1;
  if (tEncodeI32(pEncoder, pTask->downstreamTaskId) < 0) return -1;

  if (pTask->execType == TASK_EXEC__EXEC) {
    if (tEncodeI8(pEncoder, pTask->exec.parallelizable) < 0) return -1;
    if (tEncodeCStr(pEncoder, pTask->exec.qmsg) < 0) return -1;
  }

  if (pTask->sinkType != TASK_SINK__NONE) {
    // TODO: wrap
    if (tEncodeI8(pEncoder, pTask->tbSink.reserved) < 0) return -1;
  }

  if (pTask->dispatchType == TASK_DISPATCH__INPLACE) {
    if (tEncodeI8(pEncoder, pTask->inplaceDispatcher.reserved) < 0) return -1;
  } else if (pTask->dispatchType == TASK_DISPATCH__FIXED) {
    if (tEncodeI32(pEncoder, pTask->fixedEpDispatcher.nodeId) < 0) return -1;
    if (tEncodeSEpSet(pEncoder, &pTask->fixedEpDispatcher.epSet) < 0) return -1;
  } else if (pTask->dispatchType == TASK_DISPATCH__SHUFFLE) {
    if (tEncodeI8(pEncoder, pTask->shuffleDispatcher.hashMethod) < 0) return -1;
  }

  /*tEndEncode(pEncoder);*/
  return pEncoder->pos;
}

int32_t tDecodeSStreamTask(SCoder* pDecoder, SStreamTask* pTask) {
  /*if (tStartDecode(pDecoder) < 0) return -1;*/
  if (tDecodeI64(pDecoder, &pTask->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pTask->taskId) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->status) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->sourceType) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->execType) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->sinkType) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->dispatchType) < 0) return -1;
  if (tDecodeI16(pDecoder, &pTask->dispatchMsgType) < 0) return -1;
  if (tDecodeI32(pDecoder, &pTask->downstreamTaskId) < 0) return -1;

  if (pTask->execType == TASK_EXEC__EXEC) {
    if (tDecodeI8(pDecoder, &pTask->exec.parallelizable) < 0) return -1;
    if (tDecodeCStrAlloc(pDecoder, &pTask->exec.qmsg) < 0) return -1;
  }

  if (pTask->sinkType != TASK_SINK__NONE) {
    if (tDecodeI8(pDecoder, &pTask->tbSink.reserved) < 0) return -1;
  }

  if (pTask->dispatchType == TASK_DISPATCH__INPLACE) {
    if (tDecodeI8(pDecoder, &pTask->inplaceDispatcher.reserved) < 0) return -1;
  } else if (pTask->dispatchType == TASK_DISPATCH__FIXED) {
    if (tDecodeI32(pDecoder, &pTask->fixedEpDispatcher.nodeId) < 0) return -1;
    if (tDecodeSEpSet(pDecoder, &pTask->fixedEpDispatcher.epSet) < 0) return -1;
  } else if (pTask->dispatchType == TASK_DISPATCH__SHUFFLE) {
    if (tDecodeI8(pDecoder, &pTask->shuffleDispatcher.hashMethod) < 0) return -1;
  }

  /*tEndDecode(pDecoder);*/
  return 0;
}

void tFreeSStreamTask(SStreamTask* pTask) {
  // TODO
  /*taosMemoryFree(pTask->qmsg);*/
  /*taosMemoryFree(pTask->executor);*/
  /*taosMemoryFree(pTask);*/
}

#if 0
int32_t tEncodeSStreamTaskExecReq(SCoder* pEncoder, const SStreamTaskExecReq* pReq) {
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->taskId) < 0) return -1;
  /*if (tEncodeDataBlocks(buf, pReq->streamId) < 0) return -1;*/
  return pEncoder->size;
}
int32_t tDecodeSStreamTaskExecReq(SCoder* pDecoder, SStreamTaskExecReq* pReq) {
  return pEncoder->size;
}
void    tFreeSStreamTaskExecReq(SStreamTaskExecReq* pReq) {
  taosArrayDestroyEx(pReq->data, tDeleteSSDataBlock);
}
#endif
