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

#include "executor.h"
#include "tstream.h"

SStreamTask* tNewSStreamTask(int64_t streamId) {
  SStreamTask* pTask = (SStreamTask*)taosMemoryCalloc(1, sizeof(SStreamTask));
  if (pTask == NULL) {
    return NULL;
  }
  pTask->taskId = tGenIdPI32();
  pTask->streamId = streamId;
  pTask->status = TASK_STATUS__IDLE;
  pTask->inputStatus = TASK_INPUT_STATUS__NORMAL;
  pTask->outputStatus = TASK_OUTPUT_STATUS__NORMAL;

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

  if (tEncodeI32(pEncoder, pTask->childId) < 0) return -1;
  if (tEncodeI32(pEncoder, pTask->nodeId) < 0) return -1;
  if (tEncodeSEpSet(pEncoder, &pTask->epSet) < 0) return -1;

  if (pTask->execType != TASK_EXEC__NONE) {
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
    if (tEncodeCStr(pEncoder, pTask->shuffleDispatcher.stbFullName) < 0) return -1;
  }
  if (tEncodeI64(pEncoder, pTask->triggerParam) < 0) return -1;

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

  if (tDecodeI32(pDecoder, &pTask->childId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pTask->nodeId) < 0) return -1;
  if (tDecodeSEpSet(pDecoder, &pTask->epSet) < 0) return -1;

  if (pTask->execType != TASK_EXEC__NONE) {
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
    if (tDeserializeSUseDbRspImp(pDecoder, &pTask->shuffleDispatcher.dbInfo) < 0) return -1;
    if (tDecodeCStrTo(pDecoder, pTask->shuffleDispatcher.stbFullName) < 0) return -1;
  }
  if (tDecodeI64(pDecoder, &pTask->triggerParam) < 0) return -1;

  /*tEndDecode(pDecoder);*/
  return 0;
}

void tFreeSStreamTask(SStreamTask* pTask) {
  streamQueueClose(pTask->inputQueue);
  streamQueueClose(pTask->outputQueue);
  if (pTask->exec.qmsg) taosMemoryFree(pTask->exec.qmsg);
  qDestroyTask(pTask->exec.executor);
  taosMemoryFree(pTask);
}
