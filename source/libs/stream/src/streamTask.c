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
#include "streamInt.h"
#include "tmisce.h"
#include "tstream.h"
#include "ttimer.h"
#include "wal.h"

static int32_t addToTaskset(SArray* pArray, SStreamTask* pTask) {
  int32_t childId = taosArrayGetSize(pArray);
  pTask->info.selfChildId = childId;
  taosArrayPush(pArray, &pTask);
  return 0;
}

SStreamTask* tNewStreamTask(int64_t streamId, int8_t taskLevel, int8_t fillHistory, int64_t triggerParam,
                            SArray* pTaskList) {
  SStreamTask* pTask = (SStreamTask*)taosMemoryCalloc(1, sizeof(SStreamTask));
  if (pTask == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  pTask->ver = SSTREAM_TASK_VER;
  pTask->id.taskId = tGenIdPI32();
  pTask->id.streamId = streamId;
  pTask->info.taskLevel = taskLevel;
  pTask->info.fillHistory = fillHistory;
  pTask->info.triggerParam = triggerParam;

  char buf[128] = {0};
  sprintf(buf, "0x%" PRIx64 "-%d", pTask->id.streamId, pTask->id.taskId);

  pTask->id.idStr = taosStrdup(buf);
  pTask->status.schedStatus = TASK_SCHED_STATUS__INACTIVE;
  pTask->status.taskStatus = TASK_STATUS__SCAN_HISTORY;
  pTask->inputInfo.status = TASK_INPUT_STATUS__NORMAL;
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
  if (tEncodeI64(pEncoder, pInfo->stage) < 0) return -1;
  return 0;
}

int32_t tDecodeStreamEpInfo(SDecoder* pDecoder, SStreamChildEpInfo* pInfo) {
  if (tDecodeI32(pDecoder, &pInfo->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pInfo->nodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pInfo->childId) < 0) return -1;
  /*if (tDecodeI64(pDecoder, &pInfo->processedVer) < 0) return -1;*/
  if (tDecodeSEpSet(pDecoder, &pInfo->epSet) < 0) return -1;
  if (tDecodeI64(pDecoder, &pInfo->stage) < 0) return -1;
  return 0;
}

int32_t tEncodeStreamTask(SEncoder* pEncoder, const SStreamTask* pTask) {
  if (tStartEncode(pEncoder) < 0) return -1;
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

  if (tEncodeI64(pEncoder, pTask->historyTaskId.streamId)) return -1;
  if (tEncodeI32(pEncoder, pTask->historyTaskId.taskId)) return -1;
  if (tEncodeI64(pEncoder, pTask->streamTaskId.streamId)) return -1;
  if (tEncodeI32(pEncoder, pTask->streamTaskId.taskId)) return -1;

  if (tEncodeU64(pEncoder, pTask->dataRange.range.minVer)) return -1;
  if (tEncodeU64(pEncoder, pTask->dataRange.range.maxVer)) return -1;
  if (tEncodeI64(pEncoder, pTask->dataRange.window.skey)) return -1;
  if (tEncodeI64(pEncoder, pTask->dataRange.window.ekey)) return -1;

  int32_t epSz = taosArrayGetSize(pTask->pUpstreamInfoList);
  if (tEncodeI32(pEncoder, epSz) < 0) return -1;
  for (int32_t i = 0; i < epSz; i++) {
    SStreamChildEpInfo* pInfo = taosArrayGetP(pTask->pUpstreamInfoList, i);
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
  if (tEncodeI64(pEncoder, pTask->info.triggerParam) < 0) return -1;
  if (tEncodeCStrWithLen(pEncoder, pTask->reserve, sizeof(pTask->reserve) - 1) < 0) return -1;

  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamTask(SDecoder* pDecoder, SStreamTask* pTask) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pTask->ver) < 0) return -1;
  if (pTask->ver != SSTREAM_TASK_VER) return -1;

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

  if (tDecodeI64(pDecoder, &pTask->historyTaskId.streamId)) return -1;
  if (tDecodeI32(pDecoder, &pTask->historyTaskId.taskId)) return -1;
  if (tDecodeI64(pDecoder, &pTask->streamTaskId.streamId)) return -1;
  if (tDecodeI32(pDecoder, &pTask->streamTaskId.taskId)) return -1;

  if (tDecodeU64(pDecoder, &pTask->dataRange.range.minVer)) return -1;
  if (tDecodeU64(pDecoder, &pTask->dataRange.range.maxVer)) return -1;
  if (tDecodeI64(pDecoder, &pTask->dataRange.window.skey)) return -1;
  if (tDecodeI64(pDecoder, &pTask->dataRange.window.ekey)) return -1;

  int32_t epSz = -1;
  if (tDecodeI32(pDecoder, &epSz) < 0) return -1;

  pTask->pUpstreamInfoList = taosArrayInit(epSz, POINTER_BYTES);
  for (int32_t i = 0; i < epSz; i++) {
    SStreamChildEpInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamChildEpInfo));
    if (pInfo == NULL) return -1;
    if (tDecodeStreamEpInfo(pDecoder, pInfo) < 0) {
      taosMemoryFreeClear(pInfo);
      return -1;
    }
    taosArrayPush(pTask->pUpstreamInfoList, &pInfo);
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
  if (tDecodeI64(pDecoder, &pTask->info.triggerParam) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pTask->reserve) < 0) return -1;

  tEndDecode(pDecoder);
  return 0;
}

int32_t tDecodeStreamTaskChkInfo(SDecoder* pDecoder, SCheckpointInfo* pChkpInfo) {
  int64_t ver;
  int64_t skip64;
  int8_t  skip8;
  int32_t skip32;
  int16_t skip16;
  SEpSet  epSet;

  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &ver) < 0) return -1;

  if (ver != SSTREAM_TASK_VER) return -1;

  if (tDecodeI64(pDecoder, &skip64) < 0) return -1;
  if (tDecodeI32(pDecoder, &skip32) < 0) return -1;
  if (tDecodeI32(pDecoder, &skip32) < 0) return -1;
  if (tDecodeI8(pDecoder, &skip8) < 0) return -1;
  if (tDecodeI8(pDecoder, &skip8) < 0) return -1;
  if (tDecodeI16(pDecoder, &skip16) < 0) return -1;

  if (tDecodeI8(pDecoder, &skip8) < 0) return -1;
  if (tDecodeI8(pDecoder, &skip8) < 0) return -1;

  if (tDecodeI32(pDecoder, &skip32) < 0) return -1;
  if (tDecodeI32(pDecoder, &skip32) < 0) return -1;
  if (tDecodeSEpSet(pDecoder, &epSet) < 0) return -1;
  if (tDecodeSEpSet(pDecoder, &epSet) < 0) return -1;

  if (tDecodeI64(pDecoder, &pChkpInfo->checkpointId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pChkpInfo->checkpointVer) < 0) return -1;

  tEndDecode(pDecoder);
  return 0;
}
int32_t tDecodeStreamTaskId(SDecoder* pDecoder, SStreamTaskId* pTaskId) {
  int64_t ver;
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &ver) < 0) return -1;
  if (ver != SSTREAM_TASK_VER) return -1;

  if (tDecodeI64(pDecoder, &pTaskId->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pTaskId->taskId) < 0) return -1;

  tEndDecode(pDecoder);
  return 0;
}

static void freeItem(void* p) {
  SStreamContinueExecInfo* pInfo = p;
  rpcFreeCont(pInfo->msg.pCont);
}

static void freeUpstreamItem(void* p) {
  SStreamChildEpInfo** pInfo = p;
  taosMemoryFree(*pInfo);
}

void tFreeStreamTask(SStreamTask* pTask) {
  int32_t taskId = pTask->id.taskId;

  qDebug("free s-task:0x%x, %p, state:%p", taskId, pTask, pTask->pState);

  // remove the ref by timer
  while (pTask->status.timerActive > 0) {
    qDebug("s-task:%s wait for task stop timer activities", pTask->id.idStr);
    taosMsleep(10);
  }

  if (pTask->schedInfo.pTimer != NULL) {
    taosTmrStop(pTask->schedInfo.pTimer);
    pTask->schedInfo.pTimer = NULL;
  }

  if (pTask->launchTaskTimer != NULL) {
    taosTmrStop(pTask->launchTaskTimer);
    pTask->launchTaskTimer = NULL;
  }

  int32_t status = atomic_load_8((int8_t*)&(pTask->status.taskStatus));
  if (pTask->inputInfo.queue) {
    streamQueueClose(pTask->inputInfo.queue, pTask->id.taskId);
  }

  if (pTask->outputInfo.queue) {
    streamQueueClose(pTask->outputInfo.queue, pTask->id.taskId);
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

  if (pTask->outputInfo.type == TASK_OUTPUT__TABLE) {
    tDeleteSchemaWrapper(pTask->tbSink.pSchemaWrapper);
    taosMemoryFree(pTask->tbSink.pTSchema);
    tSimpleHashCleanup(pTask->tbSink.pTblInfo);
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    taosArrayDestroy(pTask->shuffleDispatcher.dbInfo.pVgroupInfos);
    pTask->checkReqIds = taosArrayDestroy(pTask->checkReqIds);
  }

  if (pTask->pState) {
    qDebug("s-task:0x%x start to free task state", taskId);
    streamStateClose(pTask->pState, status == TASK_STATUS__DROPPING);
  }

  pTask->pReadyMsgList = taosArrayDestroy(pTask->pReadyMsgList);
  taosThreadMutexDestroy(&pTask->lock);
  if (pTask->msgInfo.pData != NULL) {
    destroyStreamDataBlock(pTask->msgInfo.pData);
    pTask->msgInfo.pData = NULL;
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

  if (pTask->pUpstreamInfoList != NULL) {
    taosArrayDestroyEx(pTask->pUpstreamInfoList, freeUpstreamItem);
    pTask->pUpstreamInfoList = NULL;
  }

  taosThreadMutexDestroy(&pTask->lock);
  taosMemoryFree(pTask);

  qDebug("s-task:0x%x free task completed", taskId);
}

int32_t streamTaskInit(SStreamTask* pTask, SStreamMeta* pMeta, SMsgCb* pMsgCb, int64_t ver) {
  pTask->id.idStr = createStreamTaskIdStr(pTask->id.streamId, pTask->id.taskId);
  pTask->refCnt = 1;
  pTask->status.schedStatus = TASK_SCHED_STATUS__INACTIVE;
  pTask->status.timerActive = 0;
  pTask->inputInfo.queue = streamQueueOpen(512 << 10);
  pTask->outputInfo.queue = streamQueueOpen(512 << 10);

  if (pTask->inputInfo.queue == NULL || pTask->outputInfo.queue == NULL) {
    qError("s-task:%s failed to prepare the input/output queue, initialize task failed", pTask->id.idStr);
    return -1;
  }

  pTask->tsInfo.created = taosGetTimestampMs();
  pTask->inputInfo.status = TASK_INPUT_STATUS__NORMAL;
  pTask->outputInfo.status = TASK_OUTPUT_STATUS__NORMAL;
  pTask->pMeta = pMeta;

  pTask->chkInfo.nextProcessVer = ver;
  pTask->dataRange.range.maxVer = ver;
  pTask->dataRange.range.minVer = ver;
  pTask->pMsgCb = pMsgCb;

  streamTaskInitTokenBucket(&pTask->tokenBucket, 150, 100);
  taosThreadMutexInit(&pTask->lock, NULL);
  streamTaskOpenAllUpstreamInput(pTask);

  return TSDB_CODE_SUCCESS;
}

int32_t streamTaskGetNumOfDownstream(const SStreamTask* pTask) {
  if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
    return 0;
  } else {
    int32_t type = pTask->outputInfo.type;
    if (type == TASK_OUTPUT__FIXED_DISPATCH || type == TASK_OUTPUT__TABLE) {
      return 1;
    } else {
      SArray* vgInfo = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;
      return taosArrayGetSize(vgInfo);
    }
  }
}

static SStreamChildEpInfo* createStreamTaskEpInfo(const SStreamTask* pTask) {
  SStreamChildEpInfo* pEpInfo = taosMemoryMalloc(sizeof(SStreamChildEpInfo));
  if (pEpInfo == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pEpInfo->childId = pTask->info.selfChildId;
  pEpInfo->epSet = pTask->info.epSet;
  pEpInfo->nodeId = pTask->info.nodeId;
  pEpInfo->taskId = pTask->id.taskId;
  pEpInfo->stage = -1;

  return pEpInfo;
}

int32_t streamTaskSetUpstreamInfo(SStreamTask* pTask, const SStreamTask* pUpstreamTask) {
  SStreamChildEpInfo* pEpInfo = createStreamTaskEpInfo(pUpstreamTask);
  if (pEpInfo == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (pTask->pUpstreamInfoList == NULL) {
    pTask->pUpstreamInfoList = taosArrayInit(4, POINTER_BYTES);
  }

  taosArrayPush(pTask->pUpstreamInfoList, &pEpInfo);
  return TSDB_CODE_SUCCESS;
}

void streamTaskUpdateUpstreamInfo(SStreamTask* pTask, int32_t nodeId, const SEpSet* pEpSet) {
  char buf[512] = {0};
  EPSET_TO_STR(pEpSet, buf);

  int32_t numOfUpstream = taosArrayGetSize(pTask->pUpstreamInfoList);
  for (int32_t i = 0; i < numOfUpstream; ++i) {
    SStreamChildEpInfo* pInfo = taosArrayGetP(pTask->pUpstreamInfoList, i);
    if (pInfo->nodeId == nodeId) {
      epsetAssign(&pInfo->epSet, pEpSet);
      qDebug("s-task:0x%x update the upstreamInfo, nodeId:%d newEpset:%s", pTask->id.taskId, nodeId, buf);
      break;
    }
  }
}

void streamTaskSetFixedDownstreamInfo(SStreamTask* pTask, const SStreamTask* pDownstreamTask) {
  STaskDispatcherFixedEp* pDispatcher = &pTask->fixedEpDispatcher;
  pDispatcher->taskId = pDownstreamTask->id.taskId;
  pDispatcher->nodeId = pDownstreamTask->info.nodeId;
  pDispatcher->epSet = pDownstreamTask->info.epSet;

  pTask->outputInfo.type = TASK_OUTPUT__FIXED_DISPATCH;
  pTask->msgInfo.msgType = TDMT_STREAM_TASK_DISPATCH;
}

void streamTaskUpdateDownstreamInfo(SStreamTask* pTask, int32_t nodeId, const SEpSet* pEpSet) {
  char buf[512] = {0};
  EPSET_TO_STR(pEpSet, buf);

  int8_t type = pTask->outputInfo.type;
  if (type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    SArray* pVgs = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;

    int32_t numOfVgroups = taosArrayGetSize(pVgs);
    for (int32_t i = 0; i < numOfVgroups; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(pVgs, i);

      if (pVgInfo->vgId == nodeId) {
        epsetAssign(&pVgInfo->epSet, pEpSet);
        qDebug("s-task:0x%x update the dispatch info, nodeId:%d newEpset:%s", pTask->id.taskId, nodeId, buf);
        break;
      }
    }
  } else if (type == TASK_OUTPUT__FIXED_DISPATCH) {
    STaskDispatcherFixedEp* pDispatcher = &pTask->fixedEpDispatcher;
    if (pDispatcher->nodeId == nodeId) {
      epsetAssign(&pDispatcher->epSet, pEpSet);
      qDebug("s-task:0x%x update the dispatch info, nodeId:%d newEpSet:%s", pTask->id.taskId, nodeId, buf);
    }
  } else {
    // do nothing
  }
}

int32_t streamTaskStop(SStreamTask* pTask) {
  SStreamMeta* pMeta = pTask->pMeta;
  int64_t      st = taosGetTimestampMs();
  const char*  id = pTask->id.idStr;

  pTask->status.taskStatus = TASK_STATUS__STOP;
  qKillTask(pTask->exec.pExecutor, TSDB_CODE_SUCCESS);

  while (/*pTask->status.schedStatus != TASK_SCHED_STATUS__INACTIVE */ !streamTaskIsIdle(pTask)) {
    qDebug("s-task:%s level:%d wait for task to be idle, check again in 100ms", id, pTask->info.taskLevel);
    taosMsleep(100);
  }

  pTask->tsInfo.init = 0;
  int64_t el = taosGetTimestampMs() - st;
  qDebug("vgId:%d s-task:%s is closed in %" PRId64 " ms, and reset init ts", pMeta->vgId, pTask->id.idStr, el);
  return 0;
}

int32_t doUpdateTaskEpset(SStreamTask* pTask, int32_t nodeId, SEpSet* pEpSet) {
  char buf[512] = {0};

  if (pTask->info.nodeId == nodeId) {  // execution task should be moved away
    epsetAssign(&pTask->info.epSet, pEpSet);
    EPSET_TO_STR(pEpSet, buf)
    qDebug("s-task:0x%x (vgId:%d) self node epset is updated %s", pTask->id.taskId, nodeId, buf);
  }

  // check for the dispath info and the upstream task info
  int32_t level = pTask->info.taskLevel;
  if (level == TASK_LEVEL__SOURCE) {
    streamTaskUpdateDownstreamInfo(pTask, nodeId, pEpSet);
  } else if (level == TASK_LEVEL__AGG) {
    streamTaskUpdateUpstreamInfo(pTask, nodeId, pEpSet);
    streamTaskUpdateDownstreamInfo(pTask, nodeId, pEpSet);
  } else {  // TASK_LEVEL__SINK
    streamTaskUpdateUpstreamInfo(pTask, nodeId, pEpSet);
  }

  return 0;
}

int32_t streamTaskUpdateEpsetInfo(SStreamTask* pTask, SArray* pNodeList) {
  for (int32_t i = 0; i < taosArrayGetSize(pNodeList); ++i) {
    SNodeUpdateInfo* pInfo = taosArrayGet(pNodeList, i);
    doUpdateTaskEpset(pTask, pInfo->nodeId, &pInfo->newEp);
  }
  return 0;
}

void streamTaskResetUpstreamStageInfo(SStreamTask* pTask) {
  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    return;
  }

  int32_t size = taosArrayGetSize(pTask->pUpstreamInfoList);
  for (int32_t i = 0; i < size; ++i) {
    SStreamChildEpInfo* pInfo = taosArrayGetP(pTask->pUpstreamInfoList, i);
    pInfo->stage = -1;
  }

  qDebug("s-task:%s reset all upstream tasks stage info", pTask->id.idStr);
}
