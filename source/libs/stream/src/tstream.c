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

int32_t streamDataBlockEncode(void** buf, const SStreamDataBlock* pOutput) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI8(buf, pOutput->type);
  tlen += taosEncodeFixedI32(buf, pOutput->sourceVg);
  tlen += taosEncodeFixedI64(buf, pOutput->sourceVer);
  ASSERT(pOutput->type == STREAM_INPUT__DATA_BLOCK);
  tlen += tEncodeDataBlocks(buf, pOutput->blocks);
  return tlen;
}

void* streamDataBlockDecode(const void* buf, SStreamDataBlock* pInput) {
  buf = taosDecodeFixedI8(buf, &pInput->type);
  buf = taosDecodeFixedI32(buf, &pInput->sourceVg);
  buf = taosDecodeFixedI64(buf, &pInput->sourceVer);
  ASSERT(pInput->type == STREAM_INPUT__DATA_BLOCK);
  buf = tDecodeDataBlocks(buf, &pInput->blocks);
  return (void*)buf;
}

static int32_t streamBuildExecMsg(SStreamTask* pTask, SArray* data, SRpcMsg* pMsg, SEpSet** ppEpSet) {
  SStreamTaskExecReq req = {
      .streamId = pTask->streamId,
      .data = data,
  };

  int32_t tlen = sizeof(SMsgHead) + tEncodeSStreamTaskExecReq(NULL, &req);
  void*   buf = rpcMallocCont(tlen);

  if (buf == NULL) {
    return -1;
  }

  if (pTask->dispatchType == TASK_DISPATCH__INPLACE) {
    ((SMsgHead*)buf)->vgId = 0;
    req.taskId = pTask->inplaceDispatcher.taskId;

  } else if (pTask->dispatchType == TASK_DISPATCH__FIXED) {
    ((SMsgHead*)buf)->vgId = htonl(pTask->fixedEpDispatcher.nodeId);
    *ppEpSet = &pTask->fixedEpDispatcher.epSet;
    req.taskId = pTask->fixedEpDispatcher.taskId;

  } else if (pTask->dispatchType == TASK_DISPATCH__SHUFFLE) {
    // TODO use general name rule of schemaless
    char ctbName[TSDB_TABLE_FNAME_LEN + 22];
    // all groupId must be the same in an array
    SSDataBlock* pBlock = taosArrayGet(data, 0);
    sprintf(ctbName, "%s:%ld", pTask->shuffleDispatcher.stbFullName, pBlock->info.groupId);

    // TODO: get hash function by hashMethod

    // get groupId, compute hash value
    uint32_t hashValue = MurmurHash3_32(ctbName, strlen(ctbName));

    // get node
    // TODO: optimize search process
    SArray* vgInfo = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;
    int32_t sz = taosArrayGetSize(vgInfo);
    int32_t nodeId = 0;
    for (int32_t i = 0; i < sz; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
      if (hashValue >= pVgInfo->hashBegin && hashValue <= pVgInfo->hashEnd) {
        nodeId = pVgInfo->vgId;
        req.taskId = pVgInfo->taskId;
        *ppEpSet = &pVgInfo->epSet;
        break;
      }
    }
    ASSERT(nodeId != 0);
    ((SMsgHead*)buf)->vgId = htonl(nodeId);
  }

  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  tEncodeSStreamTaskExecReq(&abuf, &req);

  pMsg->pCont = buf;
  pMsg->contLen = tlen;
  pMsg->code = 0;
  pMsg->msgType = pTask->dispatchMsgType;
  pMsg->info.noResp = 1;

  return 0;
}

static int32_t streamShuffleDispatch(SStreamTask* pTask, SMsgCb* pMsgCb, SHashObj* data) {
  void* pIter = NULL;
  while (1) {
    pIter = taosHashIterate(data, pIter);
    if (pIter == NULL) return 0;
    SArray* pData = *(SArray**)pIter;
    SRpcMsg dispatchMsg = {0};
    SEpSet* pEpSet;
    if (streamBuildExecMsg(pTask, pData, &dispatchMsg, &pEpSet) < 0) {
      ASSERT(0);
      return -1;
    }
    tmsgSendReq(pEpSet, &dispatchMsg);
  }
  return 0;
}

int32_t streamEnqueueDataSubmit(SStreamTask* pTask, SStreamDataSubmit* input) {
  ASSERT(pTask->inputType == TASK_INPUT_TYPE__SUMBIT_BLOCK);
  int8_t inputStatus = atomic_load_8(&pTask->inputStatus);
  if (inputStatus == TASK_INPUT_STATUS__NORMAL) {
    streamDataSubmitRefInc(input);
    taosWriteQitem(pTask->inputQ, input);
  }
  return inputStatus;
}

int32_t streamEnqueueDataBlk(SStreamTask* pTask, SStreamDataBlock* input) {
  ASSERT(pTask->inputType == TASK_INPUT_TYPE__DATA_BLOCK);
  taosWriteQitem(pTask->inputQ, input);
  int8_t inputStatus = atomic_load_8(&pTask->inputStatus);
  return inputStatus;
}

static int32_t streamTaskExecImpl(SStreamTask* pTask, void* data, SArray* pRes) {
  void* exec = pTask->exec.executor;

  // set input
  if (pTask->inputType == STREAM_INPUT__DATA_SUBMIT) {
    SStreamDataSubmit* pSubmit = (SStreamDataSubmit*)data;
    ASSERT(pSubmit->type == STREAM_INPUT__DATA_SUBMIT);

    qSetStreamInput(exec, pSubmit->data, STREAM_DATA_TYPE_SUBMIT_BLOCK);
  } else if (pTask->inputType == STREAM_INPUT__DATA_BLOCK) {
    SStreamDataBlock* pBlock = (SStreamDataBlock*)data;
    ASSERT(pBlock->type == STREAM_INPUT__DATA_BLOCK);

    SArray* blocks = pBlock->blocks;
    qSetMultiStreamInput(exec, blocks->pData, blocks->size, STREAM_DATA_TYPE_SSDATA_BLOCK);
  }

  // exec
  while (1) {
    SSDataBlock* output = NULL;
    uint64_t     ts = 0;
    if (qExecTask(exec, &output, &ts) < 0) {
      ASSERT(false);
    }
    if (output == NULL) break;
    // TODO: do we need free memory?
    SSDataBlock* outputCopy = createOneDataBlock(output, true);
    taosArrayPush(pRes, outputCopy);
  }

  // destroy
  if (pTask->inputType == STREAM_INPUT__DATA_SUBMIT) {
    streamDataSubmitRefDec((SStreamDataSubmit*)data);
  } else {
    taosArrayDestroyEx(((SStreamDataBlock*)data)->blocks, (FDelete)tDeleteSSDataBlock);
    taosFreeQitem(data);
  }
  return 0;
}

// TODO: handle version
int32_t streamExec(SStreamTask* pTask, SMsgCb* pMsgCb) {
  SArray* pRes = taosArrayInit(0, sizeof(SSDataBlock));
  if (pRes == NULL) return -1;
  while (1) {
    int8_t execStatus = atomic_val_compare_exchange_8(&pTask->status, TASK_STATUS__IDLE, TASK_STATUS__EXECUTING);
    void*  exec = pTask->exec.executor;
    if (execStatus == TASK_STATUS__IDLE) {
      // first run, from qall, handle failure from last exec
      while (1) {
        void* data = NULL;
        taosGetQitem(pTask->inputQAll, &data);
        if (data == NULL) break;

        streamTaskExecImpl(pTask, data, pRes);

        /*taosFreeQitem(data);*/

        if (taosArrayGetSize(pRes) != 0) {
          SStreamDataBlock* resQ = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM);
          resQ->type = STREAM_INPUT__DATA_BLOCK;
          resQ->blocks = pRes;
          taosWriteQitem(pTask->outputQ, resQ);
          pRes = taosArrayInit(0, sizeof(SSDataBlock));
          if (pRes == NULL) goto FAIL;
        }
      }
      // second run, from inputQ
      taosReadAllQitems(pTask->inputQ, pTask->inputQAll);
      while (1) {
        void* data = NULL;
        taosGetQitem(pTask->inputQAll, &data);
        if (data == NULL) break;

        streamTaskExecImpl(pTask, data, pRes);

        /*taosFreeQitem(data);*/

        if (taosArrayGetSize(pRes) != 0) {
          SStreamDataBlock* resQ = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM);
          resQ->type = STREAM_INPUT__DATA_BLOCK;
          resQ->blocks = pRes;
          taosWriteQitem(pTask->outputQ, resQ);
          pRes = taosArrayInit(0, sizeof(SSDataBlock));
          if (pRes == NULL) goto FAIL;
        }
      }
      // set status closing
      atomic_store_8(&pTask->status, TASK_STATUS__CLOSING);
      // third run, make sure all inputQ is cleared
      taosReadAllQitems(pTask->inputQ, pTask->inputQAll);
      while (1) {
        void* data = NULL;
        taosGetQitem(pTask->inputQAll, &data);
        if (data == NULL) break;

        streamTaskExecImpl(pTask, data, pRes);

        /*taosFreeQitem(data);*/

        if (taosArrayGetSize(pRes) != 0) {
          SStreamDataBlock* resQ = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM);
          resQ->type = STREAM_INPUT__DATA_BLOCK;
          resQ->blocks = pRes;
          taosWriteQitem(pTask->outputQ, resQ);
          pRes = taosArrayInit(0, sizeof(SSDataBlock));
          if (pRes == NULL) goto FAIL;
        }
      }
      // set status closing
      atomic_store_8(&pTask->status, TASK_STATUS__CLOSING);
      // third run, make sure all inputQ is cleared
      taosReadAllQitems(pTask->inputQ, pTask->inputQAll);
      while (1) {
        void* data = NULL;
        taosGetQitem(pTask->inputQAll, &data);
        if (data == NULL) break;

        streamTaskExecImpl(pTask, data, pRes);

        taosFreeQitem(data);

        if (taosArrayGetSize(pRes) != 0) {
          SStreamDataBlock* resQ = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM);
          resQ->type = STREAM_INPUT__DATA_BLOCK;
          resQ->blocks = pRes;
          taosWriteQitem(pTask->outputQ, resQ);
          pRes = taosArrayInit(0, sizeof(SSDataBlock));
          if (pRes == NULL) goto FAIL;
        }
      }

      atomic_store_8(&pTask->status, TASK_STATUS__IDLE);
      break;
    } else if (execStatus == TASK_STATUS__CLOSING) {
      continue;
    } else if (execStatus == TASK_STATUS__EXECUTING) {
      break;
    } else {
      ASSERT(0);
    }
  }
  return 0;
FAIL:
  atomic_store_8(&pTask->status, TASK_STATUS__IDLE);
  return -1;
}

int32_t streamSink(SStreamTask* pTask, SMsgCb* pMsgCb) {
  bool firstRun = 1;
  while (1) {
    SStreamDataBlock* pBlock = NULL;
    if (!firstRun) {
      taosReadAllQitems(pTask->outputQ, pTask->outputQAll);
    }
    taosGetQitem(pTask->outputQAll, (void**)&pBlock);
    if (pBlock == NULL) {
      if (firstRun) {
        firstRun = 0;
        continue;
      } else {
        break;
      }
    }

    SArray* pRes = pBlock->blocks;

    // sink
    if (pTask->sinkType == TASK_SINK__TABLE) {
      // blockDebugShowData(pRes);
      pTask->tbSink.tbSinkFunc(pTask, pTask->tbSink.vnode, 0, pRes);
    } else if (pTask->sinkType == TASK_SINK__SMA) {
      pTask->smaSink.smaSink(pTask->ahandle, pTask->smaSink.smaId, pRes);
      //
    } else if (pTask->sinkType == TASK_SINK__FETCH) {
      //
    } else {
      ASSERT(pTask->sinkType == TASK_SINK__NONE);
    }

    // dispatch
    // TODO dispatch guard
    int8_t outputStatus = atomic_load_8(&pTask->outputStatus);
    if (outputStatus == TASK_OUTPUT_STATUS__NORMAL) {
      if (pTask->dispatchType == TASK_DISPATCH__INPLACE) {
        SRpcMsg dispatchMsg = {0};
        if (streamBuildExecMsg(pTask, pRes, &dispatchMsg, NULL) < 0) {
          ASSERT(0);
          return -1;
        }

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
        SRpcMsg dispatchMsg = {0};
        SEpSet* pEpSet = NULL;
        if (streamBuildExecMsg(pTask, pRes, &dispatchMsg, &pEpSet) < 0) {
          ASSERT(0);
          return -1;
        }

        tmsgSendReq(pEpSet, &dispatchMsg);

      } else if (pTask->dispatchType == TASK_DISPATCH__SHUFFLE) {
        SHashObj* pShuffleRes = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
        if (pShuffleRes == NULL) {
          return -1;
        }

        int32_t sz = taosArrayGetSize(pRes);
        for (int32_t i = 0; i < sz; i++) {
          SSDataBlock* pDataBlock = taosArrayGet(pRes, i);
          SArray*      pArray = taosHashGet(pShuffleRes, &pDataBlock->info.groupId, sizeof(int64_t));
          if (pArray == NULL) {
            pArray = taosArrayInit(0, sizeof(SSDataBlock));
            if (pArray == NULL) {
              return -1;
            }
            taosHashPut(pShuffleRes, &pDataBlock->info.groupId, sizeof(int64_t), &pArray, sizeof(void*));
          }
          taosArrayPush(pArray, pDataBlock);
        }

        if (streamShuffleDispatch(pTask, pMsgCb, pShuffleRes) < 0) {
          return -1;
        }

      } else {
        ASSERT(pTask->dispatchType == TASK_DISPATCH__NONE);
      }
    }
  }
  return 0;
}

int32_t streamTaskEnqueue(SStreamTask* pTask, SStreamDispatchReq* pReq, SRpcMsg* pRsp) {
  SStreamDataBlock* pBlock = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM);
  int8_t            status;

  // 1.1 update status
  // TODO cal backpressure
  if (pBlock == NULL) {
    atomic_store_8(&pTask->inputStatus, TASK_INPUT_STATUS__FAILED);
    status = TASK_INPUT_STATUS__FAILED;
  } else {
    status = atomic_load_8(&pTask->inputStatus);
  }

  // 1.2 enqueue
  pBlock->type = STREAM_DATA_TYPE_SSDATA_BLOCK;
  pBlock->sourceVg = pReq->sourceVg;
  /*pBlock->sourceVer = pReq->sourceVer;*/
  taosWriteQitem(pTask->inputQ, pBlock);

  // 1.3 rsp by input status
  SStreamDispatchRsp* pCont = rpcMallocCont(sizeof(SStreamDispatchRsp));
  pCont->inputStatus = status;
  pCont->streamId = pReq->streamId;
  pCont->taskId = pReq->sourceTaskId;
  pRsp->pCont = pCont;
  pRsp->contLen = sizeof(SStreamDispatchRsp);
  tmsgSendRsp(pRsp);

  return 0;
}

int32_t streamProcessDispatchReq(SStreamTask* pTask, SMsgCb* pMsgCb, SStreamDispatchReq* pReq, SRpcMsg* pRsp) {
  // 1. handle input
  streamTaskEnqueue(pTask, pReq, pRsp);

  // 2. try exec
  // 2.1. idle: exec
  // 2.2. executing: return
  // 2.3. closing: keep trying
  streamExec(pTask, pMsgCb);

  // 3. handle output
  // 3.1 check and set status
  // 3.2 dispatch / sink
  streamSink(pTask, pMsgCb);

  return 0;
}

int32_t streamProcessDispatchRsp(SStreamTask* pTask, SMsgCb* pMsgCb, SStreamDispatchRsp* pRsp) {
  atomic_store_8(&pTask->inputStatus, pRsp->inputStatus);
  if (pRsp->inputStatus == TASK_INPUT_STATUS__BLOCKED) {
    // TODO: init recover timer
  }
  // continue dispatch
  streamSink(pTask, pMsgCb);
  return 0;
}

int32_t streamTaskProcessRunReq(SStreamTask* pTask, SMsgCb* pMsgCb) {
  streamExec(pTask, pMsgCb);
  streamSink(pTask, pMsgCb);
  return 0;
}

int32_t streamProcessRecoverReq(SStreamTask* pTask, SMsgCb* pMsgCb, SStreamTaskRecoverReq* pReq, SRpcMsg* pMsg) {
  //
  return 0;
}

int32_t streamProcessRecoverRsp(SStreamTask* pTask, SStreamTaskRecoverRsp* pRsp) {
  //
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
  pTask->status = TASK_STATUS__IDLE;

  return pTask;
}

int32_t tEncodeSStreamTask(SEncoder* pEncoder, const SStreamTask* pTask) {
  /*if (tStartEncode(pEncoder) < 0) return -1;*/
  if (tEncodeI64(pEncoder, pTask->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pTask->taskId) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->inputType) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->status) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->sourceType) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->execType) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->sinkType) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->dispatchType) < 0) return -1;
  if (tEncodeI16(pEncoder, pTask->dispatchMsgType) < 0) return -1;

  if (tEncodeI32(pEncoder, pTask->nodeId) < 0) return -1;
  if (tEncodeSEpSet(pEncoder, &pTask->epSet) < 0) return -1;

  if (pTask->execType != TASK_EXEC__NONE) {
    if (tEncodeI8(pEncoder, pTask->exec.parallelizable) < 0) return -1;
    if (tEncodeCStr(pEncoder, pTask->exec.qmsg) < 0) return -1;
  }

  if (pTask->sinkType == TASK_SINK__TABLE) {
    if (tEncodeI64(pEncoder, pTask->tbSink.stbUid) < 0) return -1;
    if (tEncodeCStr(pEncoder, pTask->tbSink.stbFullName) < 0) return -1;
    if (tEncodeSSchemaWrapper(pEncoder, pTask->tbSink.pSchemaWrapper) < 0) return -1;
  } else if (pTask->sinkType == TASK_SINK__SMA) {
    if (tEncodeI64(pEncoder, pTask->smaSink.smaId) < 0) return -1;
  } else if (pTask->sinkType == TASK_SINK__FETCH) {
    if (tEncodeI8(pEncoder, pTask->fetchSink.reserved) < 0) return -1;
  } else {
    ASSERT(pTask->sinkType == TASK_SINK__NONE);
  }

  if (pTask->dispatchType == TASK_DISPATCH__INPLACE) {
    if (tEncodeI32(pEncoder, pTask->inplaceDispatcher.taskId) < 0) return -1;
  } else if (pTask->dispatchType == TASK_DISPATCH__FIXED) {
    if (tEncodeI32(pEncoder, pTask->fixedEpDispatcher.taskId) < 0) return -1;
    if (tEncodeI32(pEncoder, pTask->fixedEpDispatcher.nodeId) < 0) return -1;
    if (tEncodeSEpSet(pEncoder, &pTask->fixedEpDispatcher.epSet) < 0) return -1;
  } else if (pTask->dispatchType == TASK_DISPATCH__SHUFFLE) {
    if (tSerializeSUseDbRspImp(pEncoder, &pTask->shuffleDispatcher.dbInfo) < 0) return -1;
    /*if (tEncodeI8(pEncoder, pTask->shuffleDispatcher.hashMethod) < 0) return -1;*/
  }

  /*tEndEncode(pEncoder);*/
  return pEncoder->pos;
}

int32_t tDecodeSStreamTask(SDecoder* pDecoder, SStreamTask* pTask) {
  /*if (tStartDecode(pDecoder) < 0) return -1;*/
  if (tDecodeI64(pDecoder, &pTask->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pTask->taskId) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->inputType) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->status) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->sourceType) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->execType) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->sinkType) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->dispatchType) < 0) return -1;
  if (tDecodeI16(pDecoder, &pTask->dispatchMsgType) < 0) return -1;

  if (tDecodeI32(pDecoder, &pTask->nodeId) < 0) return -1;
  if (tDecodeSEpSet(pDecoder, &pTask->epSet) < 0) return -1;

  if (pTask->execType != TASK_EXEC__NONE) {
    if (tDecodeI8(pDecoder, &pTask->exec.parallelizable) < 0) return -1;
    if (tDecodeCStrAlloc(pDecoder, &pTask->exec.qmsg) < 0) return -1;
  }

  if (pTask->sinkType == TASK_SINK__TABLE) {
    if (tDecodeI64(pDecoder, &pTask->tbSink.stbUid) < 0) return -1;
    if (tDecodeCStrTo(pDecoder, pTask->tbSink.stbFullName) < 0) return -1;
    pTask->tbSink.pSchemaWrapper = taosMemoryCalloc(1, sizeof(SSchemaWrapper));
    if (pTask->tbSink.pSchemaWrapper == NULL) return -1;
    if (tDecodeSSchemaWrapper(pDecoder, pTask->tbSink.pSchemaWrapper) < 0) return -1;
  } else if (pTask->sinkType == TASK_SINK__SMA) {
    if (tDecodeI64(pDecoder, &pTask->smaSink.smaId) < 0) return -1;
  } else if (pTask->sinkType == TASK_SINK__FETCH) {
    if (tDecodeI8(pDecoder, &pTask->fetchSink.reserved) < 0) return -1;
  } else {
    ASSERT(pTask->sinkType == TASK_SINK__NONE);
  }

  if (pTask->dispatchType == TASK_DISPATCH__INPLACE) {
    if (tDecodeI32(pDecoder, &pTask->inplaceDispatcher.taskId) < 0) return -1;
  } else if (pTask->dispatchType == TASK_DISPATCH__FIXED) {
    if (tDecodeI32(pDecoder, &pTask->fixedEpDispatcher.taskId) < 0) return -1;
    if (tDecodeI32(pDecoder, &pTask->fixedEpDispatcher.nodeId) < 0) return -1;
    if (tDecodeSEpSet(pDecoder, &pTask->fixedEpDispatcher.epSet) < 0) return -1;
  } else if (pTask->dispatchType == TASK_DISPATCH__SHUFFLE) {
    /*if (tDecodeI8(pDecoder, &pTask->shuffleDispatcher.hashMethod) < 0) return -1;*/
    if (tDeserializeSUseDbRspImp(pDecoder, &pTask->shuffleDispatcher.dbInfo) < 0) return -1;
  }

  /*tEndDecode(pDecoder);*/
  return 0;
}

void tFreeSStreamTask(SStreamTask* pTask) {
  taosCloseQueue(pTask->inputQ);
  taosCloseQueue(pTask->outputQ);
  // TODO
  if (pTask->exec.qmsg) taosMemoryFree(pTask->exec.qmsg);
  qDestroyTask(pTask->exec.executor);
  taosMemoryFree(pTask);
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
