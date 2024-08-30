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

#include "streamMsg.h"
#include "os.h"
#include "tstream.h"
#include "streamInt.h"

int32_t tEncodeStreamEpInfo(SEncoder* pEncoder, const SStreamUpstreamEpInfo* pInfo) {
  if (tEncodeI32(pEncoder, pInfo->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pInfo->nodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pInfo->childId) < 0) return -1;
  if (tEncodeSEpSet(pEncoder, &pInfo->epSet) < 0) return -1;
  if (tEncodeI64(pEncoder, pInfo->stage) < 0) return -1;
  return 0;
}

int32_t tDecodeStreamEpInfo(SDecoder* pDecoder, SStreamUpstreamEpInfo* pInfo) {
  if (tDecodeI32(pDecoder, &pInfo->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pInfo->nodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pInfo->childId) < 0) return -1;
  if (tDecodeSEpSet(pDecoder, &pInfo->epSet) < 0) return -1;
  if (tDecodeI64(pDecoder, &pInfo->stage) < 0) return -1;
  return 0;
}

int32_t tEncodeStreamCheckpointSourceReq(SEncoder* pEncoder, const SStreamCheckpointSourceReq* pReq) {
  if (tStartEncode(pEncoder) != 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->checkpointId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->nodeId) < 0) return -1;
  if (tEncodeSEpSet(pEncoder, &pReq->mgmtEps) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->mnodeId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->expireTime) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->transId) < 0) return -1;
  if (tEncodeI8(pEncoder, pReq->mndTrigger) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamCheckpointSourceReq(SDecoder* pDecoder, SStreamCheckpointSourceReq* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->checkpointId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->nodeId) < 0) return -1;
  if (tDecodeSEpSet(pDecoder, &pReq->mgmtEps) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->mnodeId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->expireTime) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->transId) < 0) return -1;
  if (tDecodeI8(pDecoder, &pReq->mndTrigger) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeStreamCheckpointSourceRsp(SEncoder* pEncoder, const SStreamCheckpointSourceRsp* pRsp) {
  if (tStartEncode(pEncoder) != 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->streamId) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->checkpointId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->nodeId) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->expireTime) < 0) return -1;
  if (tEncodeI8(pEncoder, pRsp->success) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tEncodeStreamTaskUpdateMsg(SEncoder* pEncoder, const SStreamTaskNodeUpdateMsg* pMsg) {
  if (tStartEncode(pEncoder) != 0) return -1;
  if (tEncodeI64(pEncoder, pMsg->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pMsg->taskId) < 0) return -1;

  int32_t size = taosArrayGetSize(pMsg->pNodeList);
  if (tEncodeI32(pEncoder, size) < 0) return -1;

  for (int32_t i = 0; i < size; ++i) {
    SNodeUpdateInfo* pInfo = taosArrayGet(pMsg->pNodeList, i);
    if (pInfo == NULL) {
      return terrno;
    }

    if (tEncodeI32(pEncoder, pInfo->nodeId) < 0) return -1;
    if (tEncodeSEpSet(pEncoder, &pInfo->prevEp) < 0) return -1;
    if (tEncodeSEpSet(pEncoder, &pInfo->newEp) < 0) return -1;
  }

  // todo this new attribute will be result in being incompatible with previous version
  if (tEncodeI32(pEncoder, pMsg->transId) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamTaskUpdateMsg(SDecoder* pDecoder, SStreamTaskNodeUpdateMsg* pMsg) {
  int32_t code = 0;

  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pMsg->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pMsg->taskId) < 0) return -1;

  int32_t size = 0;
  if (tDecodeI32(pDecoder, &size) < 0) return -1;
  pMsg->pNodeList = taosArrayInit(size, sizeof(SNodeUpdateInfo));
  for (int32_t i = 0; i < size; ++i) {
    SNodeUpdateInfo info = {0};
    if (tDecodeI32(pDecoder, &info.nodeId) < 0) return -1;
    if (tDecodeSEpSet(pDecoder, &info.prevEp) < 0) return -1;
    if (tDecodeSEpSet(pDecoder, &info.newEp) < 0) return -1;

    void* p = taosArrayPush(pMsg->pNodeList, &info);
    if (p == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (tDecodeI32(pDecoder, &pMsg->transId) < 0) return -1;

  tEndDecode(pDecoder);
  return code;
}

int32_t tEncodeStreamTaskCheckReq(SEncoder* pEncoder, const SStreamTaskCheckReq* pReq) {
  if (tStartEncode(pEncoder) != 0) return -1;
  if (tEncodeI64(pEncoder, pReq->reqId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->downstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->downstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->childId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->stage) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamTaskCheckReq(SDecoder* pDecoder, SStreamTaskCheckReq* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->reqId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->downstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->downstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->childId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->stage) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeStreamTaskCheckRsp(SEncoder* pEncoder, const SStreamTaskCheckRsp* pRsp) {
  if (tStartEncode(pEncoder) != 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->reqId) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->upstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->upstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->downstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->downstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->childId) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->oldStage) < 0) return -1;
  if (tEncodeI8(pEncoder, pRsp->status) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamTaskCheckRsp(SDecoder* pDecoder, SStreamTaskCheckRsp* pRsp) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->reqId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->upstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->upstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->downstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->downstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->childId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->oldStage) < 0) return -1;
  if (tDecodeI8(pDecoder, &pRsp->status) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeStreamCheckpointReadyMsg(SEncoder* pEncoder, const SStreamCheckpointReadyMsg* pReq) {
  if (tStartEncode(pEncoder) != 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->checkpointId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->downstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->downstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->childId) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamCheckpointReadyMsg(SDecoder* pDecoder, SStreamCheckpointReadyMsg* pRsp) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->streamId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->checkpointId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->downstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->downstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->upstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->upstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->childId) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeStreamDispatchReq(SEncoder* pEncoder, const SStreamDispatchReq* pReq) {
  if (tStartEncode(pEncoder) != 0) return -1;
  if (tEncodeI64(pEncoder, pReq->stage) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->msgId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->srcVgId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->type) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->type) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamChildId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamRelTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->blockNum) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->totalLen) < 0) return -1;

  if (taosArrayGetSize(pReq->data) != pReq->blockNum || taosArrayGetSize(pReq->dataLen) != pReq->blockNum) {
    stError("invalid dispatch req msg");
    return TSDB_CODE_INVALID_MSG;
  }

  for (int32_t i = 0; i < pReq->blockNum; i++) {
    int32_t* pLen = taosArrayGet(pReq->dataLen, i);
    void*    data = taosArrayGetP(pReq->data, i);
    if (data == NULL || pLen == NULL) {
      return terrno;
    }

    if (tEncodeI32(pEncoder, *pLen) < 0) return -1;
    if (tEncodeBinary(pEncoder, data, *pLen) < 0) return -1;
  }
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamDispatchReq(SDecoder* pDecoder, SStreamDispatchReq* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->stage) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->msgId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->srcVgId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->type) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->type) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamChildId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamRelTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->blockNum) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->totalLen) < 0) return -1;

  pReq->data = taosArrayInit(pReq->blockNum, sizeof(void*));
  pReq->dataLen = taosArrayInit(pReq->blockNum, sizeof(int32_t));
  for (int32_t i = 0; i < pReq->blockNum; i++) {
    int32_t  len1;
    uint64_t len2;
    void*    data;
    if (tDecodeI32(pDecoder, &len1) < 0) return -1;
    if (tDecodeBinaryAlloc(pDecoder, &data, &len2) < 0) return -1;

    if (len1 != len2) {
      return TSDB_CODE_INVALID_MSG;
    }

    void* p = taosArrayPush(pReq->dataLen, &len1);
    if (p == NULL) {
      tEndDecode(pDecoder);
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    p = taosArrayPush(pReq->data, &data);
    if (p == NULL) {
      tEndDecode(pDecoder);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  tEndDecode(pDecoder);
  return 0;
}

void tCleanupStreamDispatchReq(SStreamDispatchReq* pReq) {
  taosArrayDestroyP(pReq->data, taosMemoryFree);
  taosArrayDestroy(pReq->dataLen);
}

int32_t tEncodeStreamRetrieveReq(SEncoder* pEncoder, const SStreamRetrieveReq* pReq) {
  if (tStartEncode(pEncoder) != 0) return -1;
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

void tCleanupStreamRetrieveReq(SStreamRetrieveReq* pReq) { taosMemoryFree(pReq->pRetrieve); }

int32_t tEncodeStreamTaskCheckpointReq(SEncoder* pEncoder, const SStreamTaskCheckpointReq* pReq) {
  if (tStartEncode(pEncoder) != 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->nodeId) < 0) return -1;
  tEndEncode(pEncoder);
  return 0;
}

int32_t tDecodeStreamTaskCheckpointReq(SDecoder* pDecoder, SStreamTaskCheckpointReq* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->nodeId) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeStreamHbMsg(SEncoder* pEncoder, const SStreamHbMsg* pReq) {
  if (tStartEncode(pEncoder) != 0) return -1;
  if (tEncodeI32(pEncoder, pReq->vgId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->numOfTasks) < 0) return -1;

  for (int32_t i = 0; i < pReq->numOfTasks; ++i) {
    STaskStatusEntry* ps = taosArrayGet(pReq->pTaskStatus, i);
    if (ps == NULL) {
      return terrno;
    }

    if (tEncodeI64(pEncoder, ps->id.streamId) < 0) return -1;
    if (tEncodeI32(pEncoder, ps->id.taskId) < 0) return -1;
    if (tEncodeI32(pEncoder, ps->status) < 0) return -1;
    if (tEncodeI64(pEncoder, ps->stage) < 0) return -1;
    if (tEncodeI32(pEncoder, ps->nodeId) < 0) return -1;
    if (tEncodeDouble(pEncoder, ps->inputQUsed) < 0) return -1;
    if (tEncodeDouble(pEncoder, ps->inputRate) < 0) return -1;
    if (tEncodeDouble(pEncoder, ps->procsTotal) < 0) return -1;
    if (tEncodeDouble(pEncoder, ps->procsThroughput) < 0) return -1;
    if (tEncodeDouble(pEncoder, ps->outputTotal) < 0) return -1;
    if (tEncodeDouble(pEncoder, ps->outputThroughput) < 0) return -1;
    if (tEncodeDouble(pEncoder, ps->sinkQuota) < 0) return -1;
    if (tEncodeDouble(pEncoder, ps->sinkDataSize) < 0) return -1;
    if (tEncodeI64(pEncoder, ps->processedVer) < 0) return -1;
    if (tEncodeI64(pEncoder, ps->verRange.minVer) < 0) return -1;
    if (tEncodeI64(pEncoder, ps->verRange.maxVer) < 0) return -1;
    if (tEncodeI64(pEncoder, ps->checkpointInfo.activeId) < 0) return -1;
    if (tEncodeI8(pEncoder, ps->checkpointInfo.failed) < 0) return -1;
    if (tEncodeI32(pEncoder, ps->checkpointInfo.activeTransId) < 0) return -1;
    if (tEncodeI64(pEncoder, ps->checkpointInfo.latestId) < 0) return -1;
    if (tEncodeI64(pEncoder, ps->checkpointInfo.latestVer) < 0) return -1;
    if (tEncodeI64(pEncoder, ps->checkpointInfo.latestTime) < 0) return -1;
    if (tEncodeI64(pEncoder, ps->checkpointInfo.latestSize) < 0) return -1;
    if (tEncodeI8(pEncoder, ps->checkpointInfo.remoteBackup) < 0) return -1;
    if (tEncodeI8(pEncoder, ps->checkpointInfo.consensusChkptId) < 0) return -1;
    if (tEncodeI64(pEncoder, ps->checkpointInfo.consensusTs) < 0) return -1;
    if (tEncodeI64(pEncoder, ps->startTime) < 0) return -1;
    if (tEncodeI64(pEncoder, ps->startCheckpointId) < 0) return -1;
    if (tEncodeI64(pEncoder, ps->startCheckpointVer) < 0) return -1;
    if (tEncodeI64(pEncoder, ps->hTaskId) < 0) return -1;
  }

  int32_t numOfVgs = taosArrayGetSize(pReq->pUpdateNodes);
  if (tEncodeI32(pEncoder, numOfVgs) < 0) return -1;

  for (int j = 0; j < numOfVgs; ++j) {
    int32_t* pVgId = taosArrayGet(pReq->pUpdateNodes, j);
    if (pVgId == NULL) {
      return terrno;
    }

    if (tEncodeI32(pEncoder, *pVgId) < 0) return -1;
  }

  if (tEncodeI32(pEncoder, pReq->msgId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->ts) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamHbMsg(SDecoder* pDecoder, SStreamHbMsg* pReq) {
  int32_t code = 0;

  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->vgId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->numOfTasks) < 0) return -1;

  pReq->pTaskStatus = taosArrayInit(pReq->numOfTasks, sizeof(STaskStatusEntry));
  for (int32_t i = 0; i < pReq->numOfTasks; ++i) {
    int32_t          taskId = 0;
    STaskStatusEntry entry = {0};

    if (tDecodeI64(pDecoder, &entry.id.streamId) < 0) return -1;
    if (tDecodeI32(pDecoder, &taskId) < 0) return -1;
    if (tDecodeI32(pDecoder, &entry.status) < 0) return -1;
    if (tDecodeI64(pDecoder, &entry.stage) < 0) return -1;
    if (tDecodeI32(pDecoder, &entry.nodeId) < 0) return -1;
    if (tDecodeDouble(pDecoder, &entry.inputQUsed) < 0) return -1;
    if (tDecodeDouble(pDecoder, &entry.inputRate) < 0) return -1;
    if (tDecodeDouble(pDecoder, &entry.procsTotal) < 0) return -1;
    if (tDecodeDouble(pDecoder, &entry.procsThroughput) < 0) return -1;
    if (tDecodeDouble(pDecoder, &entry.outputTotal) < 0) return -1;
    if (tDecodeDouble(pDecoder, &entry.outputThroughput) < 0) return -1;
    if (tDecodeDouble(pDecoder, &entry.sinkQuota) < 0) return -1;
    if (tDecodeDouble(pDecoder, &entry.sinkDataSize) < 0) return -1;
    if (tDecodeI64(pDecoder, &entry.processedVer) < 0) return -1;
    if (tDecodeI64(pDecoder, &entry.verRange.minVer) < 0) return -1;
    if (tDecodeI64(pDecoder, &entry.verRange.maxVer) < 0) return -1;
    if (tDecodeI64(pDecoder, &entry.checkpointInfo.activeId) < 0) return -1;
    if (tDecodeI8(pDecoder, &entry.checkpointInfo.failed) < 0) return -1;
    if (tDecodeI32(pDecoder, &entry.checkpointInfo.activeTransId) < 0) return -1;

    if (tDecodeI64(pDecoder, &entry.checkpointInfo.latestId) < 0) return -1;
    if (tDecodeI64(pDecoder, &entry.checkpointInfo.latestVer) < 0) return -1;
    if (tDecodeI64(pDecoder, &entry.checkpointInfo.latestTime) < 0) return -1;
    if (tDecodeI64(pDecoder, &entry.checkpointInfo.latestSize) < 0) return -1;
    if (tDecodeI8(pDecoder, &entry.checkpointInfo.remoteBackup) < 0) return -1;
    if (tDecodeI8(pDecoder, &entry.checkpointInfo.consensusChkptId) < 0) return -1;
    if (tDecodeI64(pDecoder, &entry.checkpointInfo.consensusTs) < 0) return -1;
    if (tDecodeI64(pDecoder, &entry.startTime) < 0) return -1;
    if (tDecodeI64(pDecoder, &entry.startCheckpointId) < 0) return -1;
    if (tDecodeI64(pDecoder, &entry.startCheckpointVer) < 0) return -1;
    if (tDecodeI64(pDecoder, &entry.hTaskId) < 0) return -1;

    entry.id.taskId = taskId;
    void* p = taosArrayPush(pReq->pTaskStatus, &entry);
    if (p == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

  int32_t numOfVgs = 0;
  if (tDecodeI32(pDecoder, &numOfVgs) < 0) return -1;

  pReq->pUpdateNodes = taosArrayInit(numOfVgs, sizeof(int32_t));

  for (int j = 0; j < numOfVgs; ++j) {
    int32_t vgId = 0;
    if (tDecodeI32(pDecoder, &vgId) < 0) return -1;
    void* p = taosArrayPush(pReq->pUpdateNodes, &vgId);
    if (p == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

  if (tDecodeI32(pDecoder, &pReq->msgId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->ts) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;

  _err:
  tEndDecode(pDecoder);
  return code;
}

void tCleanupStreamHbMsg(SStreamHbMsg* pMsg) {
  if (pMsg == NULL) {
    return;
  }

  if (pMsg->pUpdateNodes != NULL) {
    taosArrayDestroy(pMsg->pUpdateNodes);
    pMsg->pUpdateNodes = NULL;
  }

  if (pMsg->pTaskStatus != NULL) {
    taosArrayDestroy(pMsg->pTaskStatus);
    pMsg->pTaskStatus = NULL;
  }

  pMsg->msgId = -1;
  pMsg->vgId = -1;
  pMsg->numOfTasks = -1;
}

int32_t tEncodeStreamTask(SEncoder* pEncoder, const SStreamTask* pTask) {
  if (tStartEncode(pEncoder) != 0) return -1;
  if (tEncodeI64(pEncoder, pTask->ver) < 0) return -1;
  if (tEncodeI64(pEncoder, pTask->id.streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pTask->id.taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pTask->info.totalLevel) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->info.taskLevel) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->outputInfo.type) < 0) return -1;
  if (tEncodeI16(pEncoder, pTask->msgInfo.msgType) < 0) return -1;

  if (tEncodeI8(pEncoder, pTask->status.taskStatus) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->status.schedStatus) < 0) return -1;

  if (tEncodeI32(pEncoder, pTask->info.selfChildId) < 0) return -1;
  if (tEncodeI32(pEncoder, pTask->info.nodeId) < 0) return -1;
  if (tEncodeSEpSet(pEncoder, &pTask->info.epSet) < 0) return -1;
  if (tEncodeSEpSet(pEncoder, &pTask->info.mnodeEpset) < 0) return -1;

  if (tEncodeI64(pEncoder, pTask->chkInfo.checkpointId) < 0) return -1;
  if (tEncodeI64(pEncoder, pTask->chkInfo.checkpointVer) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->info.fillHistory) < 0) return -1;

  if (tEncodeI64(pEncoder, pTask->hTaskInfo.id.streamId)) return -1;
  int32_t taskId = pTask->hTaskInfo.id.taskId;
  if (tEncodeI32(pEncoder, taskId)) return -1;

  if (tEncodeI64(pEncoder, pTask->streamTaskId.streamId)) return -1;
  taskId = pTask->streamTaskId.taskId;
  if (tEncodeI32(pEncoder, taskId)) return -1;

  if (tEncodeU64(pEncoder, pTask->dataRange.range.minVer)) return -1;
  if (tEncodeU64(pEncoder, pTask->dataRange.range.maxVer)) return -1;
  if (tEncodeI64(pEncoder, pTask->dataRange.window.skey)) return -1;
  if (tEncodeI64(pEncoder, pTask->dataRange.window.ekey)) return -1;

  int32_t epSz = taosArrayGetSize(pTask->upstreamInfo.pList);
  if (tEncodeI32(pEncoder, epSz) < 0) return -1;
  for (int32_t i = 0; i < epSz; i++) {
    SStreamUpstreamEpInfo* pInfo = taosArrayGetP(pTask->upstreamInfo.pList, i);
    if (tEncodeStreamEpInfo(pEncoder, pInfo) < 0) return -1;
  }

  if (pTask->info.taskLevel != TASK_LEVEL__SINK) {
    if (tEncodeCStr(pEncoder, pTask->exec.qmsg) < 0) return -1;
  }

  if (pTask->outputInfo.type == TASK_OUTPUT__TABLE) {
    if (tEncodeI64(pEncoder, pTask->outputInfo.tbSink.stbUid) < 0) return -1;
    if (tEncodeCStr(pEncoder, pTask->outputInfo.tbSink.stbFullName) < 0) return -1;
    if (tEncodeSSchemaWrapper(pEncoder, pTask->outputInfo.tbSink.pSchemaWrapper) < 0) return -1;
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SMA) {
    if (tEncodeI64(pEncoder, pTask->outputInfo.smaSink.smaId) < 0) return -1;
  } else if (pTask->outputInfo.type == TASK_OUTPUT__FETCH) {
    if (tEncodeI8(pEncoder, pTask->outputInfo.fetchSink.reserved) < 0) return -1;
  } else if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    if (tEncodeI32(pEncoder, pTask->outputInfo.fixedDispatcher.taskId) < 0) return -1;
    if (tEncodeI32(pEncoder, pTask->outputInfo.fixedDispatcher.nodeId) < 0) return -1;
    if (tEncodeSEpSet(pEncoder, &pTask->outputInfo.fixedDispatcher.epSet) < 0) return -1;
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    if (tSerializeSUseDbRspImp(pEncoder, &pTask->outputInfo.shuffleDispatcher.dbInfo) < 0) return -1;
    if (tEncodeCStr(pEncoder, pTask->outputInfo.shuffleDispatcher.stbFullName) < 0) return -1;
  }
  if (tEncodeI64(pEncoder, pTask->info.delaySchedParam) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->subtableWithoutMd5) < 0) return -1;
  if (tEncodeCStrWithLen(pEncoder, pTask->reserve, sizeof(pTask->reserve) - 1) < 0) return -1;

  tEndEncode(pEncoder);
  return 0;
}

int32_t tDecodeStreamTask(SDecoder* pDecoder, SStreamTask* pTask) {
  int32_t taskId = 0;

  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pTask->ver) < 0) return -1;
  if (pTask->ver <= SSTREAM_TASK_INCOMPATIBLE_VER || pTask->ver > SSTREAM_TASK_VER) return -1;

  if (tDecodeI64(pDecoder, &pTask->id.streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pTask->id.taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pTask->info.totalLevel) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->info.taskLevel) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->outputInfo.type) < 0) return -1;
  if (tDecodeI16(pDecoder, &pTask->msgInfo.msgType) < 0) return -1;

  if (tDecodeI8(pDecoder, &pTask->status.taskStatus) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->status.schedStatus) < 0) return -1;

  if (tDecodeI32(pDecoder, &pTask->info.selfChildId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pTask->info.nodeId) < 0) return -1;
  if (tDecodeSEpSet(pDecoder, &pTask->info.epSet) < 0) return -1;
  if (tDecodeSEpSet(pDecoder, &pTask->info.mnodeEpset) < 0) return -1;

  if (tDecodeI64(pDecoder, &pTask->chkInfo.checkpointId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pTask->chkInfo.checkpointVer) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->info.fillHistory) < 0) return -1;

  if (tDecodeI64(pDecoder, &pTask->hTaskInfo.id.streamId)) return -1;
  if (tDecodeI32(pDecoder, &taskId)) return -1;
  pTask->hTaskInfo.id.taskId = taskId;

  if (tDecodeI64(pDecoder, &pTask->streamTaskId.streamId)) return -1;
  if (tDecodeI32(pDecoder, &taskId)) return -1;
  pTask->streamTaskId.taskId = taskId;

  if (tDecodeU64(pDecoder, (uint64_t*)&pTask->dataRange.range.minVer)) return -1;
  if (tDecodeU64(pDecoder, (uint64_t*)&pTask->dataRange.range.maxVer)) return -1;
  if (tDecodeI64(pDecoder, &pTask->dataRange.window.skey)) return -1;
  if (tDecodeI64(pDecoder, &pTask->dataRange.window.ekey)) return -1;

  int32_t epSz = -1;
  if (tDecodeI32(pDecoder, &epSz) < 0) return -1;

  pTask->upstreamInfo.pList = taosArrayInit(epSz, POINTER_BYTES);
  for (int32_t i = 0; i < epSz; i++) {
    SStreamUpstreamEpInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamUpstreamEpInfo));
    if (pInfo == NULL) return -1;
    if (tDecodeStreamEpInfo(pDecoder, pInfo) < 0) {
      taosMemoryFreeClear(pInfo);
      return -1;
    }
    void* p = taosArrayPush(pTask->upstreamInfo.pList, &pInfo);
    if (p == NULL) {
      tEndDecode(pDecoder);
      return -1;
    }
  }

  if (pTask->info.taskLevel != TASK_LEVEL__SINK) {
    if (tDecodeCStrAlloc(pDecoder, &pTask->exec.qmsg) < 0) return -1;
  }

  if (pTask->outputInfo.type == TASK_OUTPUT__TABLE) {
    if (tDecodeI64(pDecoder, &pTask->outputInfo.tbSink.stbUid) < 0) return -1;
    if (tDecodeCStrTo(pDecoder, pTask->outputInfo.tbSink.stbFullName) < 0) return -1;
    pTask->outputInfo.tbSink.pSchemaWrapper = taosMemoryCalloc(1, sizeof(SSchemaWrapper));
    if (pTask->outputInfo.tbSink.pSchemaWrapper == NULL) return -1;
    if (tDecodeSSchemaWrapper(pDecoder, pTask->outputInfo.tbSink.pSchemaWrapper) < 0) return -1;
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SMA) {
    if (tDecodeI64(pDecoder, &pTask->outputInfo.smaSink.smaId) < 0) return -1;
  } else if (pTask->outputInfo.type == TASK_OUTPUT__FETCH) {
    if (tDecodeI8(pDecoder, &pTask->outputInfo.fetchSink.reserved) < 0) return -1;
  } else if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    if (tDecodeI32(pDecoder, &pTask->outputInfo.fixedDispatcher.taskId) < 0) return -1;
    if (tDecodeI32(pDecoder, &pTask->outputInfo.fixedDispatcher.nodeId) < 0) return -1;
    if (tDecodeSEpSet(pDecoder, &pTask->outputInfo.fixedDispatcher.epSet) < 0) return -1;
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    if (tDeserializeSUseDbRspImp(pDecoder, &pTask->outputInfo.shuffleDispatcher.dbInfo) < 0) return -1;
    if (tDecodeCStrTo(pDecoder, pTask->outputInfo.shuffleDispatcher.stbFullName) < 0) return -1;
  }
  if (tDecodeI64(pDecoder, &pTask->info.delaySchedParam) < 0) return -1;
  if (pTask->ver >= SSTREAM_TASK_SUBTABLE_CHANGED_VER) {
    if (tDecodeI8(pDecoder, &pTask->subtableWithoutMd5) < 0) return -1;
  }
  if (tDecodeCStrTo(pDecoder, pTask->reserve) < 0) return -1;

  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeStreamTaskChkptReport(SEncoder* pEncoder, const SCheckpointReport* pReq) {
  if (tStartEncode(pEncoder) != 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->nodeId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->checkpointId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->checkpointVer) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->checkpointTs) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->transId) < 0) return -1;
  if (tEncodeI8(pEncoder, pReq->dropHTask) < 0) return -1;
  tEndEncode(pEncoder);
  return 0;
}

int32_t tDecodeStreamTaskChkptReport(SDecoder* pDecoder, SCheckpointReport* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->nodeId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->checkpointId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->checkpointVer) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->checkpointTs) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->transId) < 0) return -1;
  if (tDecodeI8(pDecoder, &pReq->dropHTask) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeRestoreCheckpointInfo (SEncoder* pEncoder, const SRestoreCheckpointInfo* pReq) {
  if (tStartEncode(pEncoder) != 0) return -1;
  if (tEncodeI64(pEncoder, pReq->startTs) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->checkpointId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->transId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->nodeId) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeRestoreCheckpointInfo(SDecoder* pDecoder, SRestoreCheckpointInfo* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->startTs) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->checkpointId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->transId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->nodeId) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}
