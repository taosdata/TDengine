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

int32_t tEncodeStreamRetrieveReq(SEncoder* pEncoder, const SStreamRetrieveReq* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
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

int32_t streamBroadcastToChildren(SStreamTask* pTask, const SSDataBlock* pBlock) {
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
  pRetrieve->numOfRows = htonl(pBlock->info.rows);
  pRetrieve->numOfCols = htonl(numOfCols);
  pRetrieve->skey = htobe64(pBlock->info.window.skey);
  pRetrieve->ekey = htobe64(pBlock->info.window.ekey);

  int32_t actualLen = 0;
  blockEncode(pBlock, pRetrieve->data, &actualLen, numOfCols, false);

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
      goto FAIL;
    }

    ((SMsgHead*)buf)->vgId = htonl(pEpInfo->nodeId);
    void*    abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
    SEncoder encoder;
    tEncoderInit(&encoder, abuf, len);
    tEncodeStreamRetrieveReq(&encoder, &req);

    SRpcMsg rpcMsg = {
        .code = 0,
        .msgType = TDMT_STREAM_RETRIEVE,
        .pCont = buf,
        .contLen = sizeof(SMsgHead) + len,
    };

    if (tmsgSendReq(&pEpInfo->epSet, &rpcMsg) < 0) {
      ASSERT(0);
      return -1;
    }
  }
  return 0;
FAIL:
  if (pRetrieve) taosMemoryFree(pRetrieve);
  if (buf) taosMemoryFree(buf);
  return -1;
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
  pRetrieve->numOfRows = htonl(pBlock->info.rows);
  pRetrieve->skey = htobe64(pBlock->info.window.skey);
  pRetrieve->ekey = htobe64(pBlock->info.window.ekey);

  int32_t numOfCols = (int32_t)taosArrayGetSize(pBlock->pDataBlock);
  pRetrieve->numOfCols = htonl(numOfCols);

  int32_t actualLen = 0;
  blockEncode(pBlock, pRetrieve->data, &actualLen, numOfCols, false);
  actualLen += sizeof(SRetrieveTableRsp);
  ASSERT(actualLen <= dataStrLen);
  taosArrayPush(pReq->dataLen, &actualLen);
  taosArrayPush(pReq->data, &buf);

  return 0;
}

int32_t streamBuildDispatchMsg(SStreamTask* pTask, const SStreamDataBlock* data, SRpcMsg* pMsg, SEpSet** ppEpSet) {
  void*   buf = NULL;
  int32_t code = -1;
  int32_t blockNum = taosArrayGetSize(data->blocks);
  ASSERT(blockNum != 0);

  SStreamDispatchReq req = {
      .streamId = pTask->streamId,
      .dataSrcVgId = data->srcVgId,
      .upstreamTaskId = pTask->taskId,
      .upstreamChildId = pTask->selfChildId,
      .upstreamNodeId = pTask->nodeId,
      .blockNum = blockNum,
  };

  req.data = taosArrayInit(blockNum, sizeof(void*));
  req.dataLen = taosArrayInit(blockNum, sizeof(int32_t));
  if (req.data == NULL || req.dataLen == NULL) {
    goto FAIL;
  }
  for (int32_t i = 0; i < blockNum; i++) {
    SSDataBlock* pDataBlock = taosArrayGet(data->blocks, i);
    if (streamAddBlockToDispatchMsg(pDataBlock, &req) < 0) {
      goto FAIL;
    }
  }
  int32_t vgId = 0;
  int32_t downstreamTaskId = 0;
  // find ep
  if (pTask->dispatchType == TASK_DISPATCH__FIXED) {
    vgId = pTask->fixedEpDispatcher.nodeId;
    *ppEpSet = &pTask->fixedEpDispatcher.epSet;
    downstreamTaskId = pTask->fixedEpDispatcher.taskId;
  } else if (pTask->dispatchType == TASK_DISPATCH__SHUFFLE) {
    // TODO get ctbName for each block
    SSDataBlock* pBlock = taosArrayGet(data->blocks, 0);
    char*        ctbName = buildCtbNameByGroupId(pTask->shuffleDispatcher.stbFullName, pBlock->info.groupId);
    // TODO: get hash function by hashMethod

    // get groupId, compute hash value
    uint32_t hashValue = MurmurHash3_32(ctbName, strlen(ctbName));

    // get node
    // TODO: optimize search process
    SArray* vgInfo = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;
    int32_t sz = taosArrayGetSize(vgInfo);
    for (int32_t i = 0; i < sz; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
      ASSERT(pVgInfo->vgId > 0);
      if (hashValue >= pVgInfo->hashBegin && hashValue <= pVgInfo->hashEnd) {
        vgId = pVgInfo->vgId;
        downstreamTaskId = pVgInfo->taskId;
        *ppEpSet = &pVgInfo->epSet;
        break;
      }
    }
  }

  ASSERT(vgId > 0 || vgId == SNODE_HANDLE);
  req.taskId = downstreamTaskId;

  qDebug("dispatch from task %d (child id %d) to down stream task %d in vnode %d", pTask->taskId, pTask->selfChildId,
         downstreamTaskId, vgId);

  // serialize
  int32_t tlen;
  tEncodeSize(tEncodeStreamDispatchReq, &req, tlen, code);
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
  if ((code = tEncodeStreamDispatchReq(&encoder, &req)) < 0) {
    goto FAIL;
  }
  tEncoderClear(&encoder);

  pMsg->contLen = tlen + sizeof(SMsgHead);
  pMsg->pCont = buf;
  pMsg->msgType = pTask->dispatchMsgType;

  code = 0;
FAIL:
  if (code < 0 && buf) rpcFreeCont(buf);
  if (req.data) taosArrayDestroyP(req.data, (FDelete)taosMemoryFree);
  if (req.dataLen) taosArrayDestroy(req.dataLen);
  return code;
}

int32_t streamDispatch(SStreamTask* pTask, SMsgCb* pMsgCb) {
  ASSERT(pTask->dispatchType != TASK_DISPATCH__NONE);
#if 1
  int8_t old =
      atomic_val_compare_exchange_8(&pTask->outputStatus, TASK_OUTPUT_STATUS__NORMAL, TASK_OUTPUT_STATUS__WAIT);
  if (old != TASK_OUTPUT_STATUS__NORMAL) {
    return 0;
  }
#endif

  SStreamDataBlock* pBlock = streamQueueNextItem(pTask->outputQueue);
  if (pBlock == NULL) {
    qDebug("stream stop dispatching since no output: task %d", pTask->taskId);
    atomic_store_8(&pTask->outputStatus, TASK_OUTPUT_STATUS__NORMAL);
    return 0;
  }
  ASSERT(pBlock->type == STREAM_INPUT__DATA_BLOCK);

  qDebug("stream continue dispatching: task %d", pTask->taskId);

  SRpcMsg dispatchMsg = {0};
  SEpSet* pEpSet = NULL;
  if (streamBuildDispatchMsg(pTask, pBlock, &dispatchMsg, &pEpSet) < 0) {
    ASSERT(0);
    atomic_store_8(&pTask->outputStatus, TASK_OUTPUT_STATUS__NORMAL);
    return -1;
  }
  taosArrayDestroyEx(pBlock->blocks, (FDelete)blockDataFreeRes);
  taosFreeQitem(pBlock);

  tmsgSendReq(pEpSet, &dispatchMsg);
  return 0;
}
