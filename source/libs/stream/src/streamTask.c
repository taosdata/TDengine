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

#include <libs/transport/trpc.h>
#include <streamInt.h>
#include "executor.h"
#include "tstream.h"
#include "wal.h"

static int32_t addToTaskset(SArray* pArray, SStreamTask* pTask) {
  int32_t childId = taosArrayGetSize(pArray);
  pTask->info.selfChildId = childId;
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
  pTask->info.taskLevel = taskLevel;
  pTask->info.fillHistory = fillHistory;
  pTask->triggerParam = triggerParam;

  char buf[128] = {0};
  sprintf(buf, "0x%" PRIx64 "-%d", pTask->id.streamId, pTask->id.taskId);

  pTask->id.idStr = taosStrdup(buf);
  pTask->status.schedStatus = TASK_SCHED_STATUS__INACTIVE;
  pTask->status.taskStatus = TASK_STATUS__SCAN_HISTORY;
  pTask->inputStatus = TASK_INPUT_STATUS__NORMAL;
  pTask->outputInfo.status = TASK_OUTPUT_STATUS__NORMAL;

  addToTaskset(pTaskList, pTask);
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
  if (tEncodeI32(pEncoder, pTask->info.totalLevel) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->info.taskLevel) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->outputInfo.type) < 0) return -1;
  if (tEncodeI16(pEncoder, pTask->msgInfo.msgType) < 0) return -1;

  if (tEncodeI8(pEncoder, pTask->status.taskStatus) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->status.schedStatus) < 0) return -1;

  if (tEncodeI32(pEncoder, pTask->info.selfChildId) < 0) return -1;
  if (tEncodeI32(pEncoder, pTask->info.nodeId) < 0) return -1;
  if (tEncodeSEpSet(pEncoder, &pTask->info.epSet) < 0) return -1;

  if (tEncodeI64(pEncoder, pTask->chkInfo.id) < 0) return -1;
  if (tEncodeI64(pEncoder, pTask->chkInfo.version) < 0) return -1;
  if (tEncodeI8(pEncoder, pTask->info.fillHistory) < 0) return -1;

  if (tEncodeI64(pEncoder, pTask->historyTaskId.streamId)) return -1;
  if (tEncodeI32(pEncoder, pTask->historyTaskId.taskId)) return -1;
  if (tEncodeI64(pEncoder, pTask->streamTaskId.streamId)) return -1;
  if (tEncodeI32(pEncoder, pTask->streamTaskId.taskId)) return -1;

  if (tEncodeU64(pEncoder, pTask->dataRange.range.minVer)) return -1;
  if (tEncodeU64(pEncoder, pTask->dataRange.range.maxVer)) return -1;
  if (tEncodeI64(pEncoder, pTask->dataRange.window.skey)) return -1;
  if (tEncodeI64(pEncoder, pTask->dataRange.window.ekey)) return -1;

  int32_t epSz = taosArrayGetSize(pTask->pUpstreamEpInfoList);
  if (tEncodeI32(pEncoder, epSz) < 0) return -1;
  for (int32_t i = 0; i < epSz; i++) {
    SStreamChildEpInfo* pInfo = taosArrayGetP(pTask->pUpstreamEpInfoList, i);
    if (tEncodeStreamEpInfo(pEncoder, pInfo) < 0) return -1;
  }

  if (pTask->info.taskLevel != TASK_LEVEL__SINK) {
    if (tEncodeCStr(pEncoder, pTask->exec.qmsg) < 0) return -1;
  }

  if (pTask->outputInfo.type == TASK_OUTPUT__TABLE) {
    if (tEncodeI64(pEncoder, pTask->tbSink.stbUid) < 0) return -1;
    if (tEncodeCStr(pEncoder, pTask->tbSink.stbFullName) < 0) return -1;
    if (tEncodeSSchemaWrapper(pEncoder, pTask->tbSink.pSchemaWrapper) < 0) return -1;
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SMA) {
    if (tEncodeI64(pEncoder, pTask->smaSink.smaId) < 0) return -1;
  } else if (pTask->outputInfo.type == TASK_OUTPUT__FETCH) {
    if (tEncodeI8(pEncoder, pTask->fetchSink.reserved) < 0) return -1;
  } else if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    if (tEncodeI32(pEncoder, pTask->fixedEpDispatcher.taskId) < 0) return -1;
    if (tEncodeI32(pEncoder, pTask->fixedEpDispatcher.nodeId) < 0) return -1;
    if (tEncodeSEpSet(pEncoder, &pTask->fixedEpDispatcher.epSet) < 0) return -1;
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
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
  if (tDecodeI32(pDecoder, &pTask->info.totalLevel) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->info.taskLevel) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->outputInfo.type) < 0) return -1;
  if (tDecodeI16(pDecoder, &pTask->msgInfo.msgType) < 0) return -1;

  if (tDecodeI8(pDecoder, &pTask->status.taskStatus) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->status.schedStatus) < 0) return -1;

  if (tDecodeI32(pDecoder, &pTask->info.selfChildId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pTask->info.nodeId) < 0) return -1;
  if (tDecodeSEpSet(pDecoder, &pTask->info.epSet) < 0) return -1;

  if (tDecodeI64(pDecoder, &pTask->chkInfo.id) < 0) return -1;
  if (tDecodeI64(pDecoder, &pTask->chkInfo.version) < 0) return -1;
  if (tDecodeI8(pDecoder, &pTask->info.fillHistory) < 0) return -1;

  if (tDecodeI64(pDecoder, &pTask->historyTaskId.streamId)) return -1;
  if (tDecodeI32(pDecoder, &pTask->historyTaskId.taskId)) return -1;
  if (tDecodeI64(pDecoder, &pTask->streamTaskId.streamId)) return -1;
  if (tDecodeI32(pDecoder, &pTask->streamTaskId.taskId)) return -1;

  if (tDecodeU64(pDecoder, &pTask->dataRange.range.minVer)) return -1;
  if (tDecodeU64(pDecoder, &pTask->dataRange.range.maxVer)) return -1;
  if (tDecodeI64(pDecoder, &pTask->dataRange.window.skey)) return -1;
  if (tDecodeI64(pDecoder, &pTask->dataRange.window.ekey)) return -1;

  int32_t epSz;
  if (tDecodeI32(pDecoder, &epSz) < 0) return -1;

  pTask->pUpstreamEpInfoList = taosArrayInit(epSz, POINTER_BYTES);
  for (int32_t i = 0; i < epSz; i++) {
    SStreamChildEpInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamChildEpInfo));
    if (pInfo == NULL) return -1;
    if (tDecodeStreamEpInfo(pDecoder, pInfo) < 0) {
      taosMemoryFreeClear(pInfo);
      return -1;
    }
    taosArrayPush(pTask->pUpstreamEpInfoList, &pInfo);
  }

  if (pTask->info.taskLevel != TASK_LEVEL__SINK) {
    if (tDecodeCStrAlloc(pDecoder, &pTask->exec.qmsg) < 0) return -1;
  }

  if (pTask->outputInfo.type == TASK_OUTPUT__TABLE) {
    if (tDecodeI64(pDecoder, &pTask->tbSink.stbUid) < 0) return -1;
    if (tDecodeCStrTo(pDecoder, pTask->tbSink.stbFullName) < 0) return -1;
    pTask->tbSink.pSchemaWrapper = taosMemoryCalloc(1, sizeof(SSchemaWrapper));
    if (pTask->tbSink.pSchemaWrapper == NULL) return -1;
    if (tDecodeSSchemaWrapper(pDecoder, pTask->tbSink.pSchemaWrapper) < 0) return -1;
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SMA) {
    if (tDecodeI64(pDecoder, &pTask->smaSink.smaId) < 0) return -1;
  } else if (pTask->outputInfo.type == TASK_OUTPUT__FETCH) {
    if (tDecodeI8(pDecoder, &pTask->fetchSink.reserved) < 0) return -1;
  } else if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    if (tDecodeI32(pDecoder, &pTask->fixedEpDispatcher.taskId) < 0) return -1;
    if (tDecodeI32(pDecoder, &pTask->fixedEpDispatcher.nodeId) < 0) return -1;
    if (tDecodeSEpSet(pDecoder, &pTask->fixedEpDispatcher.epSet) < 0) return -1;
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    if (tDeserializeSUseDbRspImp(pDecoder, &pTask->shuffleDispatcher.dbInfo) < 0) return -1;
    if (tDecodeCStrTo(pDecoder, pTask->shuffleDispatcher.stbFullName) < 0) return -1;
  }
  if (tDecodeI64(pDecoder, &pTask->triggerParam) < 0) return -1;

  tEndDecode(pDecoder);
  return 0;
}

static void freeItem(void* p) {
  SStreamContinueExecInfo* pInfo = p;
  rpcFreeCont(pInfo->msg.pCont);
}

void tFreeStreamTask(SStreamTask* pTask) {
  qDebug("free s-task:%s", pTask->id.idStr);

  int32_t status = atomic_load_8((int8_t*)&(pTask->status.taskStatus));
  if (pTask->inputQueue) {
    streamQueueClose(pTask->inputQueue);
  }

  if (pTask->outputInfo.queue) {
    streamQueueClose(pTask->outputInfo.queue);
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

  taosArrayDestroyP(pTask->pUpstreamEpInfoList, taosMemoryFree);
  if (pTask->outputInfo.type == TASK_OUTPUT__TABLE) {
    tDeleteSchemaWrapper(pTask->tbSink.pSchemaWrapper);
    taosMemoryFree(pTask->tbSink.pTSchema);
    tSimpleHashCleanup(pTask->tbSink.pTblInfo);
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
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

  if (pTask->pNameMap) {
    tSimpleHashCleanup(pTask->pNameMap);
  }

  if (pTask->pRspMsgList != NULL) {
    taosArrayDestroyEx(pTask->pRspMsgList, freeItem);
    pTask->pRspMsgList = NULL;
  }

  taosThreadMutexDestroy(&pTask->lock);
  taosMemoryFree(pTask);
}
