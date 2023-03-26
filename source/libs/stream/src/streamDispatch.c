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

#include "streamInc.h"

int32_t tEncodeStreamDispatchReq(SEncoder* pEncoder, const SStreamDispatchReq* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->dataSrcVgId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamChildId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->blockNum) < 0) return -1;
  ASSERT(taosArrayGetSize(pReq->data) == pReq->blockNum);
  ASSERT(taosArrayGetSize(pReq->dataLen) == pReq->blockNum);
  for (int32_t i = 0; i < pReq->blockNum; i++) {
    int32_t len = *(int32_t*)taosArrayGet(pReq->dataLen, i);
    void*   data = taosArrayGetP(pReq->data, i);
    if (tEncodeI32(pEncoder, len) < 0) return -1;
    if (tEncodeBinary(pEncoder, data, len) < 0) return -1;
  }
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamDispatchReq(SDecoder* pDecoder, SStreamDispatchReq* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->dataSrcVgId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamChildId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->blockNum) < 0) return -1;
  ASSERT(pReq->blockNum > 0);
  pReq->data = taosArrayInit(pReq->blockNum, sizeof(void*));
  pReq->dataLen = taosArrayInit(pReq->blockNum, sizeof(int32_t));
  for (int32_t i = 0; i < pReq->blockNum; i++) {
    int32_t  len1;
    uint64_t len2;
    void*    data;
    if (tDecodeI32(pDecoder, &len1) < 0) return -1;
    if (tDecodeBinaryAlloc(pDecoder, &data, &len2) < 0) return -1;
    ASSERT(len1 == len2);
    taosArrayPush(pReq->dataLen, &len1);
    taosArrayPush(pReq->data, &data);
  }
  tEndDecode(pDecoder);
  return 0;
}

void tDeleteStreamDispatchReq(SStreamDispatchReq* pReq) {
  taosArrayDestroyP(pReq->data, taosMemoryFree);
  taosArrayDestroy(pReq->dataLen);
}

int32_t tEncodeStreamRetrieveReq(SEncoder* pEncoder, const SStreamRetrieveReq* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->reqId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->dstNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->dstTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->srcNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->srcTaskId) < 0) return -1;
  if (tEncodeBinary(pEncoder, (const uint8_t*)pReq->pRetrieve, pReq->retrieveLen) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamRetrieveReq(SDecoder* pDecoder, SStreamRetrieveReq* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->reqId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->dstNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->dstTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->srcNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->srcTaskId) < 0) return -1;
  uint64_t len = 0;
  if (tDecodeBinaryAlloc(pDecoder, (void**)&pReq->pRetrieve, &len) < 0) return -1;
  pReq->retrieveLen = (int32_t)len;
  tEndDecode(pDecoder);
  return 0;
}

void tDeleteStreamRetrieveReq(SStreamRetrieveReq* pReq) { taosMemoryFree(pReq->pRetrieve); }

int32_t streamBroadcastToChildren(SStreamTask* pTask, const SSDataBlock* pBlock) {
  int32_t            code = -1;
  SRetrieveTableRsp* pRetrieve = NULL;
  void*              buf = NULL;
  int32_t            dataStrLen = sizeof(SRetrieveTableRsp) + blockGetEncodeSize(pBlock);

  pRetrieve = taosMemoryCalloc(1, dataStrLen);
  if (pRetrieve == NULL) return -1;

  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  pRetrieve->useconds = 0;
  pRetrieve->precision = TSDB_DEFAULT_PRECISION;
  pRetrieve->compressed = 0;
  pRetrieve->completed = 1;
  pRetrieve->streamBlockType = pBlock->info.type;
  pRetrieve->numOfRows = htobe64((int64_t)pBlock->info.rows);
  pRetrieve->numOfCols = htonl(numOfCols);
  pRetrieve->skey = htobe64(pBlock->info.window.skey);
  pRetrieve->ekey = htobe64(pBlock->info.window.ekey);
  pRetrieve->version = htobe64(pBlock->info.version);

  int32_t actualLen = blockEncode(pBlock, pRetrieve->data, numOfCols);

  SStreamRetrieveReq req = {
      .streamId = pTask->streamId,
      .srcNodeId = pTask->nodeId,
      .srcTaskId = pTask->taskId,
      .pRetrieve = pRetrieve,
      .retrieveLen = dataStrLen,
  };

  int32_t sz = taosArrayGetSize(pTask->childEpInfo);
  ASSERT(sz > 0);
  for (int32_t i = 0; i < sz; i++) {
    req.reqId = tGenIdPI64();
    SStreamChildEpInfo* pEpInfo = taosArrayGetP(pTask->childEpInfo, i);
    req.dstNodeId = pEpInfo->nodeId;
    req.dstTaskId = pEpInfo->taskId;
    int32_t code;
    int32_t len;
    tEncodeSize(tEncodeStreamRetrieveReq, &req, len, code);
    if (code < 0) {
      ASSERT(0);
      return -1;
    }

    buf = rpcMallocCont(sizeof(SMsgHead) + len);
    if (buf == NULL) {
      goto CLEAR;
    }

    ((SMsgHead*)buf)->vgId = htonl(pEpInfo->nodeId);
    void*    abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
    SEncoder encoder;
    tEncoderInit(&encoder, abuf, len);
    tEncodeStreamRetrieveReq(&encoder, &req);
    tEncoderClear(&encoder);

    SRpcMsg rpcMsg = {
        .code = 0,
        .msgType = TDMT_STREAM_RETRIEVE,
        .pCont = buf,
        .contLen = sizeof(SMsgHead) + len,
    };

    if (tmsgSendReq(&pEpInfo->epSet, &rpcMsg) < 0) {
      ASSERT(0);
      goto CLEAR;
    }
    buf = NULL;

    qDebug("task %d(child %d) send retrieve req to task %d at node %d, reqId %" PRId64, pTask->taskId,
           pTask->selfChildId, pEpInfo->taskId, pEpInfo->nodeId, req.reqId);
  }
  code = 0;
CLEAR:
  taosMemoryFree(pRetrieve);
  rpcFreeCont(buf);
  return code;
}

static int32_t streamAddBlockToDispatchMsg(const SSDataBlock* pBlock, SStreamDispatchReq* pReq) {
  int32_t dataStrLen = sizeof(SRetrieveTableRsp) + blockGetEncodeSize(pBlock);
  void*   buf = taosMemoryCalloc(1, dataStrLen);
  if (buf == NULL) return -1;

  SRetrieveTableRsp* pRetrieve = (SRetrieveTableRsp*)buf;
  pRetrieve->useconds = 0;
  pRetrieve->precision = TSDB_DEFAULT_PRECISION;
  pRetrieve->compressed = 0;
  pRetrieve->completed = 1;
  pRetrieve->streamBlockType = pBlock->info.type;
  pRetrieve->numOfRows = htobe64((int64_t)pBlock->info.rows);
  pRetrieve->skey = htobe64(pBlock->info.window.skey);
  pRetrieve->ekey = htobe64(pBlock->info.window.ekey);
  pRetrieve->version = htobe64(pBlock->info.version);
  pRetrieve->watermark = htobe64(pBlock->info.watermark);
  memcpy(pRetrieve->parTbName, pBlock->info.parTbName, TSDB_TABLE_NAME_LEN);

  int32_t numOfCols = (int32_t)taosArrayGetSize(pBlock->pDataBlock);
  pRetrieve->numOfCols = htonl(numOfCols);

  int32_t actualLen = blockEncode(pBlock, pRetrieve->data, numOfCols);
  actualLen += sizeof(SRetrieveTableRsp);
  ASSERT(actualLen <= dataStrLen);
  taosArrayPush(pReq->dataLen, &actualLen);
  taosArrayPush(pReq->data, &buf);

  return 0;
}

int32_t streamDispatchOneCheckReq(SStreamTask* pTask, const SStreamTaskCheckReq* pReq, int32_t nodeId, SEpSet* pEpSet) {
  void*   buf = NULL;
  int32_t code = -1;
  SRpcMsg msg = {0};

  int32_t tlen;
  tEncodeSize(tEncodeSStreamTaskCheckReq, pReq, tlen, code);
  if (code < 0) {
    return -1;
  }

  buf = rpcMallocCont(sizeof(SMsgHead) + tlen);
  if (buf == NULL) {
    return -1;
  }

  ((SMsgHead*)buf)->vgId = htonl(nodeId);
  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
  if ((code = tEncodeSStreamTaskCheckReq(&encoder, pReq)) < 0) {
    goto FAIL;
  }
  tEncoderClear(&encoder);

  msg.contLen = tlen + sizeof(SMsgHead);
  msg.pCont = buf;
  msg.msgType = TDMT_STREAM_TASK_CHECK;

  qDebug("dispatch from task %d to task %d node %d: check msg", pTask->taskId, pReq->downstreamTaskId, nodeId);

  tmsgSendReq(pEpSet, &msg);

  return 0;
FAIL:
  if (buf) rpcFreeCont(buf);
  return code;
}

int32_t streamDispatchOneRecoverFinishReq(SStreamTask* pTask, const SStreamRecoverFinishReq* pReq, int32_t vgId,
                                          SEpSet* pEpSet) {
  void*   buf = NULL;
  int32_t code = -1;
  SRpcMsg msg = {0};

  int32_t tlen;
  tEncodeSize(tEncodeSStreamRecoverFinishReq, pReq, tlen, code);
  if (code < 0) {
    return -1;
  }

  buf = rpcMallocCont(sizeof(SMsgHead) + tlen);
  if (buf == NULL) {
    return -1;
  }

  ((SMsgHead*)buf)->vgId = htonl(vgId);
  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
  if ((code = tEncodeSStreamRecoverFinishReq(&encoder, pReq)) < 0) {
    goto FAIL;
  }
  tEncoderClear(&encoder);

  msg.contLen = tlen + sizeof(SMsgHead);
  msg.pCont = buf;
  msg.msgType = TDMT_STREAM_RECOVER_FINISH;
  msg.info.noResp = 1;

  tmsgSendReq(pEpSet, &msg);

  qDebug("dispatch from task %d to task %d node %d: recover finish msg", pTask->taskId, pReq->taskId, vgId);

  return 0;
FAIL:
  if (buf) rpcFreeCont(buf);
  return code;
}

int32_t streamDispatchOneDataReq(SStreamTask* pTask, const SStreamDispatchReq* pReq, int32_t vgId, SEpSet* pEpSet) {
  void*   buf = NULL;
  int32_t code = -1;
  SRpcMsg msg = {0};

  // serialize
  int32_t tlen;
  tEncodeSize(tEncodeStreamDispatchReq, pReq, tlen, code);
  if (code < 0) goto FAIL;
  code = -1;
  buf = rpcMallocCont(sizeof(SMsgHead) + tlen);
  if (buf == NULL) {
    goto FAIL;
  }

  ((SMsgHead*)buf)->vgId = htonl(vgId);
  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
  if ((code = tEncodeStreamDispatchReq(&encoder, pReq)) < 0) {
    goto FAIL;
  }
  tEncoderClear(&encoder);

  msg.contLen = tlen + sizeof(SMsgHead);
  msg.pCont = buf;
  msg.msgType = pTask->dispatchMsgType;

  qDebug("dispatch from task %d to task %d node %d: data msg", pTask->taskId, pReq->taskId, vgId);

  tmsgSendReq(pEpSet, &msg);

  code = 0;
  return 0;
FAIL:
  if (buf) rpcFreeCont(buf);
  return code;
}

int32_t streamSearchAndAddBlock(SStreamTask* pTask, SStreamDispatchReq* pReqs, SSDataBlock* pDataBlock, int32_t vgSz,
                                int64_t groupId) {
  char* ctbName = taosMemoryCalloc(1, TSDB_TABLE_FNAME_LEN);
  if (ctbName == NULL) {
    return -1;
  }

  if (pDataBlock->info.parTbName[0]) {
    snprintf(ctbName, TSDB_TABLE_NAME_LEN, "%s.%s", pTask->shuffleDispatcher.dbInfo.db, pDataBlock->info.parTbName);
  } else {
    char* ctbShortName = buildCtbNameByGroupId(pTask->shuffleDispatcher.stbFullName, groupId);
    snprintf(ctbName, TSDB_TABLE_NAME_LEN, "%s.%s", pTask->shuffleDispatcher.dbInfo.db, ctbShortName);
    taosMemoryFree(ctbShortName);
  }

  SArray* vgInfo = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;

  /*uint32_t hashValue = MurmurHash3_32(ctbName, strlen(ctbName));*/
  SUseDbRsp* pDbInfo = &pTask->shuffleDispatcher.dbInfo;
  uint32_t   hashValue =
      taosGetTbHashVal(ctbName, strlen(ctbName), pDbInfo->hashMethod, pDbInfo->hashPrefix, pDbInfo->hashSuffix);
  taosMemoryFree(ctbName);

  bool found = false;
  // TODO: optimize search
  int32_t j;
  for (j = 0; j < vgSz; j++) {
    SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, j);
    ASSERT(pVgInfo->vgId > 0);
    if (hashValue >= pVgInfo->hashBegin && hashValue <= pVgInfo->hashEnd) {
      if (streamAddBlockToDispatchMsg(pDataBlock, &pReqs[j]) < 0) {
        return -1;
      }
      if (pReqs[j].blockNum == 0) {
        atomic_add_fetch_32(&pTask->shuffleDispatcher.waitingRspCnt, 1);
      }
      pReqs[j].blockNum++;
      found = true;
      break;
    }
  }
  ASSERT(found);
  return 0;
}

int32_t streamDispatchAllBlocks(SStreamTask* pTask, const SStreamDataBlock* pData) {
  int32_t code = -1;
  int32_t blockNum = taosArrayGetSize(pData->blocks);
  ASSERT(blockNum != 0);

  if (pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH) {
    SStreamDispatchReq req = {
        .streamId = pTask->streamId,
        .dataSrcVgId = pData->srcVgId,
        .upstreamTaskId = pTask->taskId,
        .upstreamChildId = pTask->selfChildId,
        .upstreamNodeId = pTask->nodeId,
        .blockNum = blockNum,
    };

    req.data = taosArrayInit(blockNum, sizeof(void*));
    req.dataLen = taosArrayInit(blockNum, sizeof(int32_t));
    if (req.data == NULL || req.dataLen == NULL) {
      goto FAIL_FIXED_DISPATCH;
    }

    for (int32_t i = 0; i < blockNum; i++) {
      SSDataBlock* pDataBlock = taosArrayGet(pData->blocks, i);
      if (streamAddBlockToDispatchMsg(pDataBlock, &req) < 0) {
        goto FAIL_FIXED_DISPATCH;
      }
    }
    int32_t vgId = pTask->fixedEpDispatcher.nodeId;
    SEpSet* pEpSet = &pTask->fixedEpDispatcher.epSet;
    int32_t downstreamTaskId = pTask->fixedEpDispatcher.taskId;

    req.taskId = downstreamTaskId;

    qDebug("dispatch from task %d (child id %d) to down stream task %d in vnode %d", pTask->taskId, pTask->selfChildId,
           downstreamTaskId, vgId);

    if (streamDispatchOneDataReq(pTask, &req, vgId, pEpSet) < 0) {
      goto FAIL_FIXED_DISPATCH;
    }
    code = 0;
  FAIL_FIXED_DISPATCH:
    taosArrayDestroyP(req.data, taosMemoryFree);
    taosArrayDestroy(req.dataLen);
    return code;

  } else if (pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    int32_t rspCnt = atomic_load_32(&pTask->shuffleDispatcher.waitingRspCnt);
    ASSERT(rspCnt == 0);

    SArray*             vgInfo = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;
    int32_t             vgSz = taosArrayGetSize(vgInfo);
    SStreamDispatchReq* pReqs = taosMemoryCalloc(vgSz, sizeof(SStreamDispatchReq));
    if (pReqs == NULL) {
      return -1;
    }

    for (int32_t i = 0; i < vgSz; i++) {
      pReqs[i].streamId = pTask->streamId;
      pReqs[i].dataSrcVgId = pData->srcVgId;
      pReqs[i].upstreamTaskId = pTask->taskId;
      pReqs[i].upstreamChildId = pTask->selfChildId;
      pReqs[i].upstreamNodeId = pTask->nodeId;
      pReqs[i].blockNum = 0;
      pReqs[i].data = taosArrayInit(0, sizeof(void*));
      pReqs[i].dataLen = taosArrayInit(0, sizeof(int32_t));
      if (pReqs[i].data == NULL || pReqs[i].dataLen == NULL) {
        goto FAIL_SHUFFLE_DISPATCH;
      }
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
      pReqs[i].taskId = pVgInfo->taskId;
    }

    for (int32_t i = 0; i < blockNum; i++) {
      SSDataBlock* pDataBlock = taosArrayGet(pData->blocks, i);

      // TODO: do not use broadcast
      if (pDataBlock->info.type == STREAM_DELETE_RESULT) {
        for (int32_t j = 0; j < vgSz; j++) {
          if (streamAddBlockToDispatchMsg(pDataBlock, &pReqs[j]) < 0) {
            goto FAIL_SHUFFLE_DISPATCH;
          }
          if (pReqs[j].blockNum == 0) {
            atomic_add_fetch_32(&pTask->shuffleDispatcher.waitingRspCnt, 1);
          }
          pReqs[j].blockNum++;
        }
        continue;
      }

      if (streamSearchAndAddBlock(pTask, pReqs, pDataBlock, vgSz, pDataBlock->info.id.groupId) < 0) {
        goto FAIL_SHUFFLE_DISPATCH;
      }
    }

    for (int32_t i = 0; i < vgSz; i++) {
      if (pReqs[i].blockNum > 0) {
        // send
        SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
        if (streamDispatchOneDataReq(pTask, &pReqs[i], pVgInfo->vgId, &pVgInfo->epSet) < 0) {
          goto FAIL_SHUFFLE_DISPATCH;
        }
      }
    }
    code = 0;
  FAIL_SHUFFLE_DISPATCH:
    if (pReqs) {
      for (int32_t i = 0; i < vgSz; i++) {
        taosArrayDestroyP(pReqs[i].data, taosMemoryFree);
        taosArrayDestroy(pReqs[i].dataLen);
      }
      taosMemoryFree(pReqs);
    }
    return code;
  }
  return 0;
}

int32_t streamDispatch(SStreamTask* pTask) {
  ASSERT(pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH || pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH);

  int8_t old =
      atomic_val_compare_exchange_8(&pTask->outputStatus, TASK_OUTPUT_STATUS__NORMAL, TASK_OUTPUT_STATUS__WAIT);
  if (old != TASK_OUTPUT_STATUS__NORMAL) {
    return 0;
  }

  SStreamDataBlock* pBlock = streamQueueNextItem(pTask->outputQueue);
  if (pBlock == NULL) {
    qDebug("stream stop dispatching since no output: task %d", pTask->taskId);
    atomic_store_8(&pTask->outputStatus, TASK_OUTPUT_STATUS__NORMAL);
    return 0;
  }
  ASSERT(pBlock->type == STREAM_INPUT__DATA_BLOCK);

  qDebug("stream dispatching: task %d", pTask->taskId);

  int32_t code = 0;
  if (streamDispatchAllBlocks(pTask, pBlock) < 0) {
    code = -1;
    streamQueueProcessFail(pTask->outputQueue);
    atomic_store_8(&pTask->outputStatus, TASK_OUTPUT_STATUS__NORMAL);
    goto FREE;
  }
FREE:
  taosArrayDestroyEx(pBlock->blocks, (FDelete)blockDataFreeRes);
  taosFreeQitem(pBlock);
  return code;
}
