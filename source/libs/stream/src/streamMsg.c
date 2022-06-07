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

int32_t tEncodeStreamDispatchReq(SEncoder* pEncoder, const SStreamDispatchReq* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->sourceTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->sourceVg) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->sourceChildId) < 0) return -1;
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
  return 0;
}

int32_t tDecodeStreamDispatchReq(SDecoder* pDecoder, SStreamDispatchReq* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->sourceTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->sourceVg) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->sourceChildId) < 0) return -1;
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

int32_t streamBuildDispatchMsg(SStreamTask* pTask, SArray* data, SRpcMsg* pMsg, SEpSet** ppEpSet) {
  SStreamDispatchReq req = {
      .streamId = pTask->streamId,
      .data = data,
  };
  return 0;
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
    char ctbName[TSDB_TABLE_FNAME_LEN + 22] = {0};
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
