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

int32_t tEncodeStreamTaskRecoverReq(SEncoder* pEncoder, const SStreamTaskRecoverReq* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamNodeId) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamTaskRecoverReq(SDecoder* pDecoder, SStreamTaskRecoverReq* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamNodeId) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeStreamTaskRecoverRsp(SEncoder* pEncoder, const SStreamTaskRecoverRsp* pRsp) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->reqTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->rspTaskId) < 0) return -1;
  if (tEncodeI8(pEncoder, pRsp->inputStatus) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamTaskRecoverRsp(SDecoder* pDecoder, SStreamTaskRecoverRsp* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->reqTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->rspTaskId) < 0) return -1;
  if (tDecodeI8(pDecoder, &pReq->inputStatus) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeSMStreamTaskRecoverReq(SEncoder* pEncoder, const SMStreamTaskRecoverReq* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->taskId) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeSMStreamTaskRecoverReq(SDecoder* pDecoder, SMStreamTaskRecoverReq* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->taskId) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeSMStreamTaskRecoverRsp(SEncoder* pEncoder, const SMStreamTaskRecoverRsp* pRsp) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->taskId) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeSMStreamTaskRecoverRsp(SDecoder* pDecoder, SMStreamTaskRecoverRsp* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->taskId) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

typedef struct {
  int32_t vgId;
  int32_t childId;
  int64_t ver;
} SStreamVgVerCheckpoint;

int32_t tEncodeSStreamVgVerCheckpoint(SEncoder* pEncoder, const SStreamVgVerCheckpoint* pCheckpoint) {
  if (tEncodeI32(pEncoder, pCheckpoint->vgId) < 0) return -1;
  if (tEncodeI32(pEncoder, pCheckpoint->childId) < 0) return -1;
  if (tEncodeI64(pEncoder, pCheckpoint->ver) < 0) return -1;
  return 0;
}

int32_t tDecodeSStreamVgVerCheckpoint(SDecoder* pDecoder, SStreamVgVerCheckpoint* pCheckpoint) {
  if (tDecodeI32(pDecoder, &pCheckpoint->vgId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pCheckpoint->childId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pCheckpoint->ver) < 0) return -1;
  return 0;
}

typedef struct {
  int64_t streamId;
  int64_t checkTs;
  int64_t checkpointId;
  int32_t taskId;
  SArray* checkpointVer;  // SArray<SStreamVgCheckpointVer>
} SStreamAggVerCheckpoint;

int32_t tEncodeSStreamAggVerCheckpoint(SEncoder* pEncoder, const SStreamAggVerCheckpoint* pCheckpoint) {
  if (tEncodeI64(pEncoder, pCheckpoint->streamId) < 0) return -1;
  if (tEncodeI64(pEncoder, pCheckpoint->checkTs) < 0) return -1;
  if (tEncodeI64(pEncoder, pCheckpoint->checkpointId) < 0) return -1;
  if (tEncodeI32(pEncoder, pCheckpoint->taskId) < 0) return -1;
  int32_t sz = taosArrayGetSize(pCheckpoint->checkpointVer);
  if (tEncodeI32(pEncoder, sz) < 0) return -1;
  for (int32_t i = 0; i < sz; i++) {
    SStreamVgVerCheckpoint* pOneVgCkpoint = taosArrayGet(pCheckpoint->checkpointVer, i);
    if (tEncodeSStreamVgVerCheckpoint(pEncoder, pOneVgCkpoint) < 0) return -1;
  }
  return 0;
}

int32_t tDecodeSStreamAggVerCheckpoint(SDecoder* pDecoder, SStreamAggVerCheckpoint* pCheckpoint) {
  if (tDecodeI64(pDecoder, &pCheckpoint->streamId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pCheckpoint->checkTs) < 0) return -1;
  if (tDecodeI64(pDecoder, &pCheckpoint->checkpointId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pCheckpoint->taskId) < 0) return -1;
  int32_t sz;
  if (tDecodeI32(pDecoder, &sz) < 0) return -1;
  for (int32_t i = 0; i < sz; i++) {
    SStreamVgVerCheckpoint oneVgCheckpoint;
    if (tDecodeSStreamVgVerCheckpoint(pDecoder, &oneVgCheckpoint) < 0) return -1;
    taosArrayPush(pCheckpoint->checkpointVer, &oneVgCheckpoint);
  }
  return 0;
}

int32_t streamRecoverSinkLevel(SStreamMeta* pMeta, SStreamTask* pTask) {
  ASSERT(pTask->taskLevel == TASK_LEVEL__SINK);
  // load status
  void*   pVal = NULL;
  int32_t vLen = 0;
  if (tdbTbGet(pMeta->pStateDb, &pTask->taskId, sizeof(void*), &pVal, &vLen) < 0) {
    return -1;
  }
  SDecoder decoder;
  tDecoderInit(&decoder, pVal, vLen);
  SStreamAggVerCheckpoint aggCheckpoint;
  tDecodeSStreamAggVerCheckpoint(&decoder, &aggCheckpoint);
  /*pTask->*/
  return 0;
}

int32_t streamRecoverTask(SStreamTask* pTask) {
  //
  return 0;
}
