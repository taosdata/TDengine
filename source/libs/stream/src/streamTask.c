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
#include "wal.h"

static int32_t mndAddToTaskset(SArray* pArray, SStreamTask* pTask) {
  int32_t childId = taosArrayGetSize(pArray);
  pTask->selfChildId = childId;
  taosArrayPush(pArray, &pTask);
  return 0;
}

SStreamTask* tNewStreamTask(int64_t streamId, int8_t taskLevel, int8_t fillHistory, int64_t triggerParam, SArray* pTaskList) {
  SStreamTask* pTask = (SStreamTask*)taosMemoryCalloc(1, sizeof(SStreamTask));
  if (pTask == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pTask->id.taskId = tGenIdPI32();
  pTask->id.streamId = streamId;
  pTask->taskLevel = taskLevel;
  pTask->fillHistory = fillHistory;
  pTask->triggerParam = triggerParam;

  char buf[128] = {0};
  sprintf(buf, "0x%" PRIx64 "-%d", pTask->id.streamId, pTask->id.taskId);

  pTask->id.idStr = taosStrdup(buf);
  pTask->status.schedStatus = TASK_SCHED_STATUS__INACTIVE;
  pTask->inputStatus = TASK_INPUT_STATUS__NORMAL;
  pTask->outputStatus = TASK_OUTPUT_STATUS__NORMAL;

  mndAddToTaskset(pTaskList, pTask);
  return pTask;
}

int32_t tEncodeStreamEpInfo(SEncoder* pEncoder, const SStreamChildEpInfo* pInfo) {
  if (tEncodeI32(pEncoder, pInfo->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pInfo->nodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pInfo->childId) < 0) return -1;
  /*if (tEncodeI64(pEncoder, pInfo->processedVer) < 0) return -1;*/
  if (tEncodeSEpSet(pEncoder, &pInfo->epSet) < 0) return -1;
  return 0;
}

int32_t tDecodeStreamEpInfo(SDecoder* pDecoder, SStreamChildEpInfo* pInfo) {
  if (tDecodeI32(pDecoder, &pInfo->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pInfo->nodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pInfo->childId) < 0) return -1;
  /*if (tDecodeI64(pDecoder, &pInfo->processedVer) < 0) return -1;*/
  if (tDecodeSEpSet(pDecoder, &pInfo->epSet) < 0) return -1;
  return 0;
}

int32_t tEncodeStreamTask(SEncoder* pEncoder, const SStreamTask* pTask) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pTask->id.streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pTask->id.taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pTask->totalLevel) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->taskLevel) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->outputType) < 0) return -1;
  if (tEncodeI16(pEncoder, pTask->dispatchMsgType) < 0) return -1;

  if (tEncodeI8(pEncoder, pTask->status.taskStatus) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->status.schedStatus) < 0) return -1;

  if (tEncodeI32(pEncoder, pTask->selfChildId) < 0) return -1;
  if (tEncodeI32(pEncoder, pTask->nodeId) < 0) return -1;
  if (tEncodeSEpSet(pEncoder, &pTask->epSet) < 0) return -1;

  if (tEncodeI64(pEncoder, pTask->chkInfo.id) < 0) return -1;
  if (tEncodeI64(pEncoder, pTask->chkInfo.version) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->fillHistory) < 0) return -1;

  int32_t epSz = taosArrayGetSize(pTask->childEpInfo);
  if (tEncodeI32(pEncoder, epSz) < 0) return -1;
  for (int32_t i = 0; i < epSz; i++) {
    SStreamChildEpInfo* pInfo = taosArrayGetP(pTask->childEpInfo, i);
    if (tEncodeStreamEpInfo(pEncoder, pInfo) < 0) return -1;
  }

  if (pTask->taskLevel != TASK_LEVEL__SINK) {
    if (tEncodeCStr(pEncoder, pTask->exec.qmsg) < 0) return -1;
  }

  if (pTask->outputType == TASK_OUTPUT__TABLE) {
    if (tEncodeI64(pEncoder, pTask->tbSink.stbUid) < 0) return -1;
    if (tEncodeCStr(pEncoder, pTask->tbSink.stbFullName) < 0) return -1;
    if (tEncodeSSchemaWrapper(pEncoder, pTask->tbSink.pSchemaWrapper) < 0) return -1;
  } else if (pTask->outputType == TASK_OUTPUT__SMA) {
    if (tEncodeI64(pEncoder, pTask->smaSink.smaId) < 0) return -1;
  } else if (pTask->outputType == TASK_OUTPUT__FETCH) {
    if (tEncodeI8(pEncoder, pTask->fetchSink.reserved) < 0) return -1;
  } else if (pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH) {
    if (tEncodeI32(pEncoder, pTask->fixedEpDispatcher.taskId) < 0) return -1;
    if (tEncodeI32(pEncoder, pTask->fixedEpDispatcher.nodeId) < 0) return -1;
    if (tEncodeSEpSet(pEncoder, &pTask->fixedEpDispatcher.epSet) < 0) return -1;
  } else if (pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    if (tSerializeSUseDbRspImp(pEncoder, &pTask->shuffleDispatcher.dbInfo) < 0) return -1;
    if (tEncodeCStr(pEncoder, pTask->shuffleDispatcher.stbFullName) < 0) return -1;
  }
  if (tEncodeI64(pEncoder, pTask->triggerParam) < 0) return -1;

  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamTask(SDecoder* pDecoder, SStreamTask* pTask) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pTask->id.streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pTask->id.taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pTask->totalLevel) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->taskLevel) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->outputType) < 0) return -1;
  if (tDecodeI16(pDecoder, &pTask->dispatchMsgType) < 0) return -1;

  if (tDecodeI8(pDecoder, &pTask->status.taskStatus) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->status.schedStatus) < 0) return -1;

  if (tDecodeI32(pDecoder, &pTask->selfChildId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pTask->nodeId) < 0) return -1;
  if (tDecodeSEpSet(pDecoder, &pTask->epSet) < 0) return -1;

  if (tDecodeI64(pDecoder, &pTask->chkInfo.id) < 0) return -1;
  if (tDecodeI64(pDecoder, &pTask->chkInfo.version) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->fillHistory) < 0) return -1;

  int32_t epSz;
  if (tDecodeI32(pDecoder, &epSz) < 0) return -1;
  pTask->childEpInfo = taosArrayInit(epSz, sizeof(void*));
  for (int32_t i = 0; i < epSz; i++) {
    SStreamChildEpInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamChildEpInfo));
    if (pInfo == NULL) return -1;
    if (tDecodeStreamEpInfo(pDecoder, pInfo) < 0) {
      taosMemoryFreeClear(pInfo);
      return -1;
    }
    taosArrayPush(pTask->childEpInfo, &pInfo);
  }

  if (pTask->taskLevel != TASK_LEVEL__SINK) {
    if (tDecodeCStrAlloc(pDecoder, &pTask->exec.qmsg) < 0) return -1;
  }

  if (pTask->outputType == TASK_OUTPUT__TABLE) {
    if (tDecodeI64(pDecoder, &pTask->tbSink.stbUid) < 0) return -1;
    if (tDecodeCStrTo(pDecoder, pTask->tbSink.stbFullName) < 0) return -1;
    pTask->tbSink.pSchemaWrapper = taosMemoryCalloc(1, sizeof(SSchemaWrapper));
    if (pTask->tbSink.pSchemaWrapper == NULL) return -1;
    if (tDecodeSSchemaWrapper(pDecoder, pTask->tbSink.pSchemaWrapper) < 0) return -1;
  } else if (pTask->outputType == TASK_OUTPUT__SMA) {
    if (tDecodeI64(pDecoder, &pTask->smaSink.smaId) < 0) return -1;
  } else if (pTask->outputType == TASK_OUTPUT__FETCH) {
    if (tDecodeI8(pDecoder, &pTask->fetchSink.reserved) < 0) return -1;
  } else if (pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH) {
    if (tDecodeI32(pDecoder, &pTask->fixedEpDispatcher.taskId) < 0) return -1;
    if (tDecodeI32(pDecoder, &pTask->fixedEpDispatcher.nodeId) < 0) return -1;
    if (tDecodeSEpSet(pDecoder, &pTask->fixedEpDispatcher.epSet) < 0) return -1;
  } else if (pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    if (tDeserializeSUseDbRspImp(pDecoder, &pTask->shuffleDispatcher.dbInfo) < 0) return -1;
    if (tDecodeCStrTo(pDecoder, pTask->shuffleDispatcher.stbFullName) < 0) return -1;
  }
  if (tDecodeI64(pDecoder, &pTask->triggerParam) < 0) return -1;

  tEndDecode(pDecoder);
  return 0;
}

void tFreeStreamTask(SStreamTask* pTask) {
  qDebug("free s-task:%s", pTask->id.idStr);
  int32_t status = atomic_load_8((int8_t*)&(pTask->status.taskStatus));
  if (pTask->inputQueue) {
    streamQueueClose(pTask->inputQueue);
  }
  if (pTask->outputQueue) {
    streamQueueClose(pTask->outputQueue);
  }
  if (pTask->exec.qmsg) {
    taosMemoryFree(pTask->exec.qmsg);
  }

  if (pTask->exec.pExecutor) {
    qDestroyTask(pTask->exec.pExecutor);
    pTask->exec.pExecutor = NULL;
  }

  if (pTask->exec.pWalReader != NULL) {
    walCloseReader(pTask->exec.pWalReader);
  }

  taosArrayDestroyP(pTask->childEpInfo, taosMemoryFree);
  if (pTask->outputType == TASK_OUTPUT__TABLE) {
    tDeleteSchemaWrapper(pTask->tbSink.pSchemaWrapper);
    taosMemoryFree(pTask->tbSink.pTSchema);
    tSimpleHashCleanup(pTask->tbSink.pTblInfo);
  }

  if (pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    taosArrayDestroy(pTask->shuffleDispatcher.dbInfo.pVgroupInfos);
    taosArrayDestroy(pTask->checkReqIds);
    pTask->checkReqIds = NULL;
  }

  if (pTask->pState) {
    streamStateClose(pTask->pState, status == TASK_STATUS__DROPPING);
  }

  if (pTask->id.idStr != NULL) {
    taosMemoryFree((void*)pTask->id.idStr);
  }

  taosMemoryFree(pTask);
}
