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
#include "streamsm.h"
#include "tmisce.h"
#include "tstream.h"
#include "ttimer.h"
#include "wal.h"

static void streamTaskDestroyUpstreamInfo(SUpstreamInfo* pUpstreamInfo);

static int32_t addToTaskset(SArray* pArray, SStreamTask* pTask) {
  int32_t childId = taosArrayGetSize(pArray);
  pTask->info.selfChildId = childId;
  taosArrayPush(pArray, &pTask);
  return 0;
}

SStreamTask* tNewStreamTask(int64_t streamId, int8_t taskLevel, bool fillHistory, int64_t triggerParam,
                            SArray* pTaskList, bool hasFillhistory) {
  SStreamTask* pTask = (SStreamTask*)taosMemoryCalloc(1, sizeof(SStreamTask));
  if (pTask == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    stError("s-task:0x%" PRIx64 " failed malloc new stream task, size:%d, code:%s", streamId,
            (int32_t)sizeof(SStreamTask), tstrerror(terrno));
    return NULL;
  }

  pTask->ver = SSTREAM_TASK_VER;
  pTask->id.taskId = tGenIdPI32();
  pTask->id.streamId = streamId;
  pTask->info.taskLevel = taskLevel;
  pTask->info.fillHistory = fillHistory;
  pTask->info.triggerParam = triggerParam;

  pTask->status.pSM = streamCreateStateMachine(pTask);
  if (pTask->status.pSM == NULL) {
    taosMemoryFreeClear(pTask);
    return NULL;
  }

  char buf[128] = {0};
  sprintf(buf, "0x%" PRIx64 "-%d", pTask->id.streamId, pTask->id.taskId);

  pTask->id.idStr = taosStrdup(buf);
  pTask->status.schedStatus = TASK_SCHED_STATUS__INACTIVE;
  pTask->status.taskStatus = (fillHistory || hasFillhistory) ? TASK_STATUS__SCAN_HISTORY : TASK_STATUS__READY;
  pTask->inputq.status = TASK_INPUT_STATUS__NORMAL;
  pTask->outputq.status = TASK_OUTPUT_STATUS__NORMAL;

  if (fillHistory) {
    ASSERT(hasFillhistory);
  }

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
    SStreamChildEpInfo* pInfo = taosArrayGetP(pTask->upstreamInfo.pList, i);
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
  if (tEncodeI64(pEncoder, pTask->info.triggerParam) < 0) return -1;
  if (tEncodeCStrWithLen(pEncoder, pTask->reserve, sizeof(pTask->reserve) - 1) < 0) return -1;

  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamTask(SDecoder* pDecoder, SStreamTask* pTask) {
  int32_t taskId = 0;

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

  if (tDecodeI64(pDecoder, &pTask->hTaskInfo.id.streamId)) return -1;
  if (tDecodeI32(pDecoder, &taskId)) return -1;
  pTask->hTaskInfo.id.taskId = taskId;

  if (tDecodeI64(pDecoder, &pTask->streamTaskId.streamId)) return -1;
  if (tDecodeI32(pDecoder, &taskId)) return -1;
  pTask->streamTaskId.taskId = taskId;

  if (tDecodeU64(pDecoder, &pTask->dataRange.range.minVer)) return -1;
  if (tDecodeU64(pDecoder, &pTask->dataRange.range.maxVer)) return -1;
  if (tDecodeI64(pDecoder, &pTask->dataRange.window.skey)) return -1;
  if (tDecodeI64(pDecoder, &pTask->dataRange.window.ekey)) return -1;

  int32_t epSz = -1;
  if (tDecodeI32(pDecoder, &epSz) < 0) return -1;

  pTask->upstreamInfo.pList = taosArrayInit(epSz, POINTER_BYTES);
  for (int32_t i = 0; i < epSz; i++) {
    SStreamChildEpInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamChildEpInfo));
    if (pInfo == NULL) return -1;
    if (tDecodeStreamEpInfo(pDecoder, pInfo) < 0) {
      taosMemoryFreeClear(pInfo);
      return -1;
    }
    taosArrayPush(pTask->upstreamInfo.pList, &pInfo);
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

int32_t tDecodeStreamTaskId(SDecoder* pDecoder, STaskId* pTaskId) {
  int64_t ver;
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &ver) < 0) return -1;
  if (ver != SSTREAM_TASK_VER) return -1;

  if (tDecodeI64(pDecoder, &pTaskId->streamId) < 0) return -1;

  int32_t taskId = 0;
  if (tDecodeI32(pDecoder, &taskId) < 0) return -1;

  pTaskId->taskId = taskId;
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

  STaskExecStatisInfo* pStatis = &pTask->execInfo;
  stDebug("start to free s-task:0x%x, %p, state:%p", taskId, pTask, pTask->pState);

  stDebug("s-task:0x%x task exec summary: create:%" PRId64 ", init:%" PRId64 ", start:%" PRId64
          ", updateCount:%d latestUpdate:%" PRId64 ", latestCheckPoint:%" PRId64 ", ver:%" PRId64
          " nextProcessVer:%" PRId64 ", checkpointCount:%d",
          taskId, pStatis->created, pStatis->init, pStatis->start, pStatis->updateCount, pStatis->latestUpdateTs,
          pTask->chkInfo.checkpointId, pTask->chkInfo.checkpointVer, pTask->chkInfo.nextProcessVer,
          pStatis->checkpoint);

  // remove the ref by timer
  while (pTask->status.timerActive > 0) {
    stDebug("s-task:%s wait for task stop timer activities, ref:%d", pTask->id.idStr, pTask->status.timerActive);
    taosMsleep(100);
  }

  if (pTask->schedInfo.pTimer != NULL) {
    taosTmrStop(pTask->schedInfo.pTimer);
    pTask->schedInfo.pTimer = NULL;
  }

  if (pTask->hTaskInfo.pTimer != NULL) {
    taosTmrStop(pTask->hTaskInfo.pTimer);
    pTask->hTaskInfo.pTimer = NULL;
  }

  if (pTask->msgInfo.pTimer != NULL) {
    taosTmrStop(pTask->msgInfo.pTimer);
    pTask->msgInfo.pTimer = NULL;
  }

  int32_t status = atomic_load_8((int8_t*)&(pTask->status.taskStatus));
  if (pTask->inputq.queue) {
    streamQueueClose(pTask->inputq.queue, pTask->id.taskId);
  }

  if (pTask->outputq.queue) {
    streamQueueClose(pTask->outputq.queue, pTask->id.taskId);
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

  streamClearChkptReadyMsg(pTask);
  pTask->pReadyMsgList = taosArrayDestroy(pTask->pReadyMsgList);

  if (pTask->msgInfo.pData != NULL) {
    destroyDispatchMsg(pTask->msgInfo.pData, getNumOfDispatchBranch(pTask));
    pTask->msgInfo.pData = NULL;
    pTask->msgInfo.dispatchMsgType = 0;
  }

  if (pTask->outputInfo.type == TASK_OUTPUT__TABLE) {
    tDeleteSchemaWrapper(pTask->outputInfo.tbSink.pSchemaWrapper);
    taosMemoryFree(pTask->outputInfo.tbSink.pTSchema);
    tSimpleHashCleanup(pTask->outputInfo.tbSink.pTblInfo);
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    taosArrayDestroy(pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos);
    pTask->checkReqIds = taosArrayDestroy(pTask->checkReqIds);
  }

  if (pTask->pState) {
    stDebug("s-task:0x%x start to free task state", taskId);
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

  pTask->status.pSM = streamDestroyStateMachine(pTask->status.pSM);

  streamTaskDestroyUpstreamInfo(&pTask->upstreamInfo);

  pTask->msgInfo.pRetryList = taosArrayDestroy(pTask->msgInfo.pRetryList);
  taosMemoryFree(pTask->outputInfo.pTokenBucket);
  taosThreadMutexDestroy(&pTask->lock);

  pTask->outputInfo.pDownstreamUpdateList = taosArrayDestroy(pTask->outputInfo.pDownstreamUpdateList);

  taosMemoryFree(pTask);
  stDebug("s-task:0x%x free task completed", taskId);
}

int32_t streamTaskInit(SStreamTask* pTask, SStreamMeta* pMeta, SMsgCb* pMsgCb, int64_t ver) {
  pTask->id.idStr = createStreamTaskIdStr(pTask->id.streamId, pTask->id.taskId);
  pTask->refCnt = 1;
  pTask->status.schedStatus = TASK_SCHED_STATUS__INACTIVE;
  pTask->status.timerActive = 0;
  pTask->inputq.queue = streamQueueOpen(512 << 10);
  pTask->outputq.queue = streamQueueOpen(512 << 10);

  if (pTask->inputq.queue == NULL || pTask->outputq.queue == NULL) {
    stError("s-task:%s failed to prepare the input/output queue, initialize task failed", pTask->id.idStr);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pTask->status.pSM = streamCreateStateMachine(pTask);
  if (pTask->status.pSM == NULL) {
    stError("s-task:%s failed create state-machine for stream task, initialization failed, code:%s", pTask->id.idStr,
            tstrerror(terrno));
    return terrno;
  }

  pTask->execInfo.created = taosGetTimestampMs();
  pTask->inputq.status = TASK_INPUT_STATUS__NORMAL;
  pTask->outputq.status = TASK_OUTPUT_STATUS__NORMAL;
  pTask->pMeta = pMeta;

  pTask->chkInfo.checkpointVer = ver - 1;  // only update when generating checkpoint
  pTask->chkInfo.processedVer = ver - 1;   // already processed version

  pTask->chkInfo.nextProcessVer = ver;  // next processed version
  pTask->dataRange.range.maxVer = ver;
  pTask->dataRange.range.minVer = ver;
  pTask->pMsgCb = pMsgCb;
  pTask->msgInfo.pRetryList = taosArrayInit(4, sizeof(int32_t));

  pTask->outputInfo.pTokenBucket = taosMemoryCalloc(1, sizeof(STokenBucket));
  if (pTask->outputInfo.pTokenBucket == NULL) {
    stError("s-task:%s failed to prepare the tokenBucket, code:%s", pTask->id.idStr,
            tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  // 2MiB per second for sink task
  // 50 times sink operator per second
  streamTaskInitTokenBucket(pTask->outputInfo.pTokenBucket, 50, 50, tsSinkDataRate, pTask->id.idStr);

  TdThreadMutexAttr attr = {0};
  int               code = taosThreadMutexAttrInit(&attr);
  if (code != 0) {
    stError("s-task:%s initElapsed mutex attr failed, code:%s", pTask->id.idStr, tstrerror(code));
    return code;
  }

  code = taosThreadMutexAttrSetType(&attr, PTHREAD_MUTEX_RECURSIVE);
  if (code != 0) {
    stError("s-task:%s set mutex attr recursive, code:%s", pTask->id.idStr, tstrerror(code));
    return code;
  }

  taosThreadMutexInit(&pTask->lock, &attr);
  streamTaskOpenAllUpstreamInput(pTask);

  pTask->outputInfo.pDownstreamUpdateList = taosArrayInit(4, sizeof(SDownstreamTaskEpset));
  if (pTask->outputInfo.pDownstreamUpdateList == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t streamTaskGetNumOfDownstream(const SStreamTask* pTask) {
  if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
    return 0;
  } else {
    int32_t type = pTask->outputInfo.type;
    if (type == TASK_OUTPUT__TABLE) {
      return 0;
    } else if (type == TASK_OUTPUT__FIXED_DISPATCH) {
      return 1;
    } else {
      SArray* vgInfo = pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos;
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

  if (pTask->upstreamInfo.pList == NULL) {
    pTask->upstreamInfo.pList = taosArrayInit(4, POINTER_BYTES);
  }

  taosArrayPush(pTask->upstreamInfo.pList, &pEpInfo);
  return TSDB_CODE_SUCCESS;
}

void streamTaskUpdateUpstreamInfo(SStreamTask* pTask, int32_t nodeId, const SEpSet* pEpSet) {
  char buf[512] = {0};
  EPSET_TO_STR(pEpSet, buf);

  int32_t numOfUpstream = taosArrayGetSize(pTask->upstreamInfo.pList);
  for (int32_t i = 0; i < numOfUpstream; ++i) {
    SStreamChildEpInfo* pInfo = taosArrayGetP(pTask->upstreamInfo.pList, i);
    if (pInfo->nodeId == nodeId) {
      epsetAssign(&pInfo->epSet, pEpSet);
      stDebug("s-task:0x%x update the upstreamInfo taskId:0x%x(nodeId:%d) newEpset:%s", pTask->id.taskId, pInfo->taskId,
              nodeId, buf);
      break;
    }
  }
}

void streamTaskDestroyUpstreamInfo(SUpstreamInfo* pUpstreamInfo) {
  if (pUpstreamInfo->pList != NULL) {
    taosArrayDestroyEx(pUpstreamInfo->pList, freeUpstreamItem);
    pUpstreamInfo->numOfClosed = 0;
    pUpstreamInfo->pList = NULL;
  }
}

void streamTaskSetFixedDownstreamInfo(SStreamTask* pTask, const SStreamTask* pDownstreamTask) {
  STaskDispatcherFixed* pDispatcher = &pTask->outputInfo.fixedDispatcher;
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
    SArray* pVgs = pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos;

    int32_t numOfVgroups = taosArrayGetSize(pVgs);
    for (int32_t i = 0; i < numOfVgroups; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(pVgs, i);

      if (pVgInfo->vgId == nodeId) {
        epsetAssign(&pVgInfo->epSet, pEpSet);
        stDebug("s-task:0x%x update the dispatch info, task:0x%x(nodeId:%d) newEpset:%s", pTask->id.taskId,
                pVgInfo->taskId, nodeId, buf);
        break;
      }
    }
  } else if (type == TASK_OUTPUT__FIXED_DISPATCH) {
    STaskDispatcherFixed* pDispatcher = &pTask->outputInfo.fixedDispatcher;
    if (pDispatcher->nodeId == nodeId) {
      epsetAssign(&pDispatcher->epSet, pEpSet);
      stDebug("s-task:0x%x update the dispatch info, task:0x%x(nodeId:%d) newEpSet:%s", pTask->id.taskId,
              pDispatcher->taskId, nodeId, buf);
    }
  } else {
    // do nothing
  }
}

int32_t streamTaskStop(SStreamTask* pTask) {
  int32_t     vgId = pTask->pMeta->vgId;
  int64_t     st = taosGetTimestampMs();
  const char* id = pTask->id.idStr;

  streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_STOP);
  qKillTask(pTask->exec.pExecutor, TSDB_CODE_SUCCESS);
  while (/*pTask->status.schedStatus != TASK_SCHED_STATUS__INACTIVE */ !streamTaskIsIdle(pTask)) {
    stDebug("s-task:%s level:%d wait for task to be idle and then close, check again in 100ms", id,
            pTask->info.taskLevel);
    taosMsleep(100);
  }

  int64_t el = taosGetTimestampMs() - st;
  stDebug("vgId:%d s-task:%s is closed in %" PRId64 " ms", vgId, id, el);
  return 0;
}

int32_t doUpdateTaskEpset(SStreamTask* pTask, int32_t nodeId, SEpSet* pEpSet) {
  char buf[512] = {0};

  if (pTask->info.nodeId == nodeId) {  // execution task should be moved away
    epsetAssign(&pTask->info.epSet, pEpSet);
    EPSET_TO_STR(pEpSet, buf)
    stDebug("s-task:0x%x (vgId:%d) self node epset is updated %s", pTask->id.taskId, nodeId, buf);
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
  STaskExecStatisInfo* p = &pTask->execInfo;

  int32_t numOfNodes = taosArrayGetSize(pNodeList);
  int64_t prevTs = p->latestUpdateTs;

  p->latestUpdateTs = taosGetTimestampMs();
  p->updateCount += 1;
  stDebug("s-task:0x%x update task nodeEp epset, updatedNodes:%d, updateCount:%d, prevTs:%" PRId64, pTask->id.taskId,
          numOfNodes, p->updateCount, prevTs);

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

  int32_t size = taosArrayGetSize(pTask->upstreamInfo.pList);
  for (int32_t i = 0; i < size; ++i) {
    SStreamChildEpInfo* pInfo = taosArrayGetP(pTask->upstreamInfo.pList, i);
    pInfo->stage = -1;
  }

  stDebug("s-task:%s reset all upstream tasks stage info", pTask->id.idStr);
}

bool streamTaskAllUpstreamClosed(SStreamTask* pTask) {
  return pTask->upstreamInfo.numOfClosed == taosArrayGetSize(pTask->upstreamInfo.pList);
}

bool streamTaskSetSchedStatusWait(SStreamTask* pTask) {
  bool ret = false;

  // double check
  if (pTask->status.schedStatus == TASK_SCHED_STATUS__INACTIVE) {
    taosThreadMutexLock(&pTask->lock);
    if (pTask->status.schedStatus == TASK_SCHED_STATUS__INACTIVE) {
      pTask->status.schedStatus = TASK_SCHED_STATUS__WAITING;
      ret = true;
    }
    taosThreadMutexUnlock(&pTask->lock);
  }

  return ret;
}

int8_t streamTaskSetSchedStatusActive(SStreamTask* pTask) {
  taosThreadMutexLock(&pTask->lock);
  int8_t status = pTask->status.schedStatus;
  if (status == TASK_SCHED_STATUS__WAITING) {
    pTask->status.schedStatus = TASK_SCHED_STATUS__ACTIVE;
  }
  taosThreadMutexUnlock(&pTask->lock);

  return status;
}

int8_t streamTaskSetSchedStatusInactive(SStreamTask* pTask) {
  taosThreadMutexLock(&pTask->lock);
  int8_t status = pTask->status.schedStatus;
  ASSERT(status == TASK_SCHED_STATUS__WAITING || status == TASK_SCHED_STATUS__ACTIVE ||
         status == TASK_SCHED_STATUS__INACTIVE);
  pTask->status.schedStatus = TASK_SCHED_STATUS__INACTIVE;
  taosThreadMutexUnlock(&pTask->lock);

  return status;
}

int32_t streamTaskClearHTaskAttr(SStreamTask* pTask) {
  SStreamMeta* pMeta = pTask->pMeta;
  if (pTask->info.fillHistory == 0) {
    return TSDB_CODE_SUCCESS;
  }

  STaskId       sTaskId = {.streamId = pTask->streamTaskId.streamId, .taskId = pTask->streamTaskId.taskId};
  SStreamTask** ppStreamTask = (SStreamTask**)taosHashGet(pMeta->pTasksMap, &sTaskId, sizeof(sTaskId));

  if (ppStreamTask != NULL) {
    CLEAR_RELATED_FILLHISTORY_TASK((*ppStreamTask));
    streamMetaSaveTask(pMeta, *ppStreamTask);
    stDebug("s-task:%s clear the related stream task:0x%x attr to fill-history task", pTask->id.idStr,
            (int32_t)sTaskId.taskId);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t streamBuildAndSendDropTaskMsg(SMsgCb* pMsgCb, int32_t vgId, SStreamTaskId* pTaskId) {
  SVDropStreamTaskReq* pReq = rpcMallocCont(sizeof(SVDropStreamTaskReq));
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pReq->head.vgId = vgId;
  pReq->taskId = pTaskId->taskId;
  pReq->streamId = pTaskId->streamId;

  SRpcMsg msg = {.msgType = TDMT_STREAM_TASK_DROP, .pCont = pReq, .contLen = sizeof(SVDropStreamTaskReq)};
  int32_t code = tmsgPutToQueue(pMsgCb, WRITE_QUEUE, &msg);
  if (code != TSDB_CODE_SUCCESS) {
    stError("vgId:%d failed to send drop task:0x%x msg, code:%s", vgId, pTaskId->taskId, tstrerror(code));
    return code;
  }

  stDebug("vgId:%d build and send drop task:0x%x msg", vgId, pTaskId->taskId);
  return code;
}

STaskId streamTaskExtractKey(const SStreamTask* pTask) {
  STaskId id = {.streamId = pTask->id.streamId, .taskId = pTask->id.taskId};
  return id;
}

void streamTaskInitForLaunchHTask(SHistoryTaskInfo* pInfo) {
  pInfo->waitInterval = LAUNCH_HTASK_INTERVAL;
  pInfo->tickCount = ceil(LAUNCH_HTASK_INTERVAL / WAIT_FOR_MINIMAL_INTERVAL);
  pInfo->retryTimes = 0;
}

void streamTaskSetRetryInfoForLaunch(SHistoryTaskInfo* pInfo) {
  ASSERT(pInfo->tickCount == 0);

  pInfo->waitInterval *= RETRY_LAUNCH_INTERVAL_INC_RATE;
  pInfo->tickCount = ceil(pInfo->waitInterval / WAIT_FOR_MINIMAL_INTERVAL);
  pInfo->retryTimes += 1;
}

void streamTaskStatusInit(STaskStatusEntry* pEntry, const SStreamTask* pTask) {
  pEntry->id.streamId = pTask->id.streamId;
  pEntry->id.taskId = pTask->id.taskId;
  pEntry->stage = -1;
  pEntry->nodeId = pTask->info.nodeId;
  pEntry->status = TASK_STATUS__STOP;
}

void streamTaskStatusCopy(STaskStatusEntry* pDst, const STaskStatusEntry* pSrc) {
  pDst->stage = pSrc->stage;
  pDst->inputQUsed = pSrc->inputQUsed;
  pDst->inputRate = pSrc->inputRate;
  pDst->processedVer = pSrc->processedVer;
  pDst->verStart = pSrc->verStart;
  pDst->verEnd = pSrc->verEnd;
  pDst->sinkQuota = pSrc->sinkQuota;
  pDst->sinkDataSize = pSrc->sinkDataSize;
  pDst->activeCheckpointId = pSrc->activeCheckpointId;
  pDst->checkpointFailed = pSrc->checkpointFailed;
}

int32_t streamTaskStartAsync(SStreamMeta* pMeta, SMsgCb* cb, bool restart) {
  int32_t      vgId = pMeta->vgId;

  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  if (numOfTasks == 0) {
    stDebug("vgId:%d no stream tasks existed to run", vgId);
    return 0;
  }

  SStreamTaskRunReq* pRunReq = rpcMallocCont(sizeof(SStreamTaskRunReq));
  if (pRunReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    stError("vgId:%d failed to create msg to start wal scanning to launch stream tasks, code:%s", vgId, terrstr());
    return -1;
  }

  stDebug("vgId:%d start all %d stream task(s) async", vgId, numOfTasks);
  pRunReq->head.vgId = vgId;
  pRunReq->streamId = 0;
  pRunReq->taskId = restart? STREAM_EXEC_RESTART_ALL_TASKS_ID:STREAM_EXEC_START_ALL_TASKS_ID;

  SRpcMsg msg = {.msgType = TDMT_STREAM_TASK_RUN, .pCont = pRunReq, .contLen = sizeof(SStreamTaskRunReq)};
  tmsgPutToQueue(cb, STREAM_QUEUE, &msg);
  return 0;
}

int32_t streamTaskProcessUpdateReq(SStreamMeta* pMeta, SMsgCb* cb, SRpcMsg* pMsg, bool restored) {
  int32_t      vgId = pMeta->vgId;
  char*        msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t      len = pMsg->contLen - sizeof(SMsgHead);
  SRpcMsg      rsp = {.info = pMsg->info, .code = TSDB_CODE_SUCCESS};

  SStreamTaskNodeUpdateMsg req = {0};

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, len);
  if (tDecodeStreamTaskUpdateMsg(&decoder, &req) < 0) {
    rsp.code = TSDB_CODE_MSG_DECODE_ERROR;
    stError("vgId:%d failed to decode task update msg, code:%s", vgId, tstrerror(rsp.code));
    tDecoderClear(&decoder);
    return rsp.code;
  }

  tDecoderClear(&decoder);

  // update the nodeEpset when it exists
  streamMetaWLock(pMeta);

  // the task epset may be updated again and again, when replaying the WAL, the task may be in stop status.
  STaskId id = {.streamId = req.streamId, .taskId = req.taskId};
  SStreamTask** ppTask = (SStreamTask**)taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
  if (ppTask == NULL || *ppTask == NULL) {
    stError("vgId:%d failed to acquire task:0x%x when handling update, it may have been dropped already", pMeta->vgId,
             req.taskId);
    rsp.code = TSDB_CODE_SUCCESS;
    streamMetaWUnLock(pMeta);

    taosArrayDestroy(req.pNodeList);
    return rsp.code;
  }

  SStreamTask* pTask = *ppTask;

  if (pMeta->updateInfo.transId != req.transId) {
    pMeta->updateInfo.transId = req.transId;
    stInfo("s-task:%s receive new trans to update nodeEp msg from mnode, transId:%d", pTask->id.idStr, req.transId);
    // info needs to be kept till the new trans to update the nodeEp arrived.
    taosHashClear(pMeta->updateInfo.pTasks);
  } else {
    stDebug("s-task:%s recv trans to update nodeEp from mnode, transId:%d", pTask->id.idStr, req.transId);
  }

  STaskUpdateEntry entry = {.streamId = req.streamId, .taskId = req.taskId, .transId = req.transId};
  void* exist = taosHashGet(pMeta->updateInfo.pTasks, &entry, sizeof(STaskUpdateEntry));
  if (exist != NULL) {
    stDebug("s-task:%s (vgId:%d) already update in trans:%d, discard the nodeEp update msg", pTask->id.idStr, vgId,
             req.transId);
    rsp.code = TSDB_CODE_SUCCESS;
    streamMetaWUnLock(pMeta);
    taosArrayDestroy(req.pNodeList);
    return rsp.code;
  }

  streamMetaWUnLock(pMeta);

  // the following two functions should not be executed within the scope of meta lock to avoid deadlock
  streamTaskUpdateEpsetInfo(pTask, req.pNodeList);
  streamTaskResetStatus(pTask);

  // continue after lock the meta again
  streamMetaWLock(pMeta);

  SStreamTask** ppHTask = NULL;
  if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
    ppHTask = (SStreamTask**)taosHashGet(pMeta->pTasksMap, &pTask->hTaskInfo.id, sizeof(pTask->hTaskInfo.id));
    if (ppHTask == NULL || *ppHTask == NULL) {
      stError("vgId:%d failed to acquire fill-history task:0x%x when handling update, it may have been dropped already",
               pMeta->vgId, req.taskId);
      CLEAR_RELATED_FILLHISTORY_TASK(pTask);
    } else {
      stDebug("s-task:%s fill-history task update nodeEp along with stream task", (*ppHTask)->id.idStr);
      streamTaskUpdateEpsetInfo(*ppHTask, req.pNodeList);
    }
  }

  {
    streamMetaSaveTask(pMeta, pTask);
    if (ppHTask != NULL) {
      streamMetaSaveTask(pMeta, *ppHTask);
    }

    if (streamMetaCommit(pMeta) < 0) {
      //     persist to disk
    }
  }

  streamTaskStop(pTask);

  // keep the already handled info
  taosHashPut(pMeta->updateInfo.pTasks, &entry, sizeof(entry), NULL, 0);

  if (ppHTask != NULL) {
    streamTaskStop(*ppHTask);
    stDebug("s-task:%s task nodeEp update completed, streamTask and related fill-history task closed", pTask->id.idStr);
    taosHashPut(pMeta->updateInfo.pTasks, &(*ppHTask)->id, sizeof(pTask->id), NULL, 0);
  } else {
    stDebug("s-task:%s task nodeEp update completed, streamTask closed", pTask->id.idStr);
  }

  rsp.code = 0;

  // possibly only handle the stream task.
  int32_t numOfTasks = streamMetaGetNumOfTasks(pMeta);
  int32_t updateTasks = taosHashGetSize(pMeta->updateInfo.pTasks);

  pMeta->startInfo.tasksWillRestart = 1;

  if (updateTasks < numOfTasks) {
    stDebug("vgId:%d closed tasks:%d, unclosed:%d, all tasks will be started when nodeEp update completed", vgId,
             updateTasks, (numOfTasks - updateTasks));
    streamMetaWUnLock(pMeta);
  } else {
    if (!restored) {
      stDebug("vgId:%d vnode restore not completed, not restart the tasks, clear the start after nodeUpdate flag", vgId);
      pMeta->startInfo.tasksWillRestart = 0;
      streamMetaWUnLock(pMeta);
    } else {
      stDebug("vgId:%d all %d task(s) nodeEp updated and closed", vgId, numOfTasks);
#if 1
      streamTaskStartAsync(pMeta, cb, true);
      streamMetaWUnLock(pMeta);
#else
      streamMetaWUnLock(pMeta);

      // For debug purpose.
      // the following procedure consume many CPU resource, result in the re-election of leader
      // with high probability. So we employ it as a test case for the stream processing framework, with
      // checkpoint/restart/nodeUpdate etc.
      while (1) {
        int32_t startVal = atomic_val_compare_exchange_32(&pMeta->startInfo.taskStarting, 0, 1);
        if (startVal == 0) {
          break;
        }

        tqDebug("vgId:%d in start stream tasks procedure, wait for 500ms and recheck", vgId);
        taosMsleep(500);
      }

      while (streamMetaTaskInTimer(pMeta)) {
        tqDebug("vgId:%d some tasks in timer, wait for 100ms and recheck", pMeta->vgId);
        taosMsleep(100);
      }

      streamMetaWLock(pMeta);

      int32_t code = streamMetaReopen(pMeta);
      if (code != 0) {
        tqError("vgId:%d failed to reopen stream meta", vgId);
        streamMetaWUnLock(pMeta);
        taosArrayDestroy(req.pNodeList);
        return -1;
      }

      streamMetaInitBackend(pMeta);

      if (streamMetaLoadAllTasks(pTq->pStreamMeta) < 0) {
        tqError("vgId:%d failed to load stream tasks", vgId);
        streamMetaWUnLock(pMeta);
        taosArrayDestroy(req.pNodeList);
        return -1;
      }

      if (vnodeIsRoleLeader(pTq->pVnode) && !tsDisableStream) {
        tqInfo("vgId:%d start all stream tasks after all being updated", vgId);
        resetStreamTaskStatus(pTq->pStreamMeta);
        tqStartStreamTaskAsync(pTq, false);
      } else {
        tqInfo("vgId:%d, follower node not start stream tasks", vgId);
      }
      streamMetaWUnLock(pMeta);
#endif
    }
  }

  taosArrayDestroy(req.pNodeList);
  return rsp.code;
}

int32_t streamTaskProcessDispatchReq(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  char*   msgStr = pMsg->pCont;
  char*   msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);

  SStreamDispatchReq req = {0};

  SDecoder           decoder;
  tDecoderInit(&decoder, (uint8_t*)msgBody, msgLen);
  if (tDecodeStreamDispatchReq(&decoder, &req) < 0) {
    tDecoderClear(&decoder);
    return TSDB_CODE_MSG_DECODE_ERROR;
  }
  tDecoderClear(&decoder);

  stDebug("s-task:0x%x recv dispatch msg from 0x%x(vgId:%d)", req.taskId, req.upstreamTaskId, req.upstreamNodeId);

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, req.streamId, req.taskId);
  if (pTask) {
    SRpcMsg rsp = {.info = pMsg->info, .code = 0};
    if (streamProcessDispatchMsg(pTask, &req, &rsp) != 0){
      return -1;
    }
    tDeleteStreamDispatchReq(&req);
    streamMetaReleaseTask(pMeta, pTask);
    return 0;
  } else {
    stError("vgId:%d failed to find task:0x%x to handle the dispatch req, it may have been destroyed already",
            pMeta->vgId, req.taskId);

    SMsgHead* pRspHead = rpcMallocCont(sizeof(SMsgHead) + sizeof(SStreamDispatchRsp));
    if (pRspHead == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      stError("s-task:0x%x send dispatch error rsp, out of memory", req.taskId);
      return -1;
    }

    pRspHead->vgId = htonl(req.upstreamNodeId);
    ASSERT(pRspHead->vgId != 0);

    SStreamDispatchRsp* pRsp = POINTER_SHIFT(pRspHead, sizeof(SMsgHead));
    pRsp->streamId = htobe64(req.streamId);
    pRsp->upstreamTaskId = htonl(req.upstreamTaskId);
    pRsp->upstreamNodeId = htonl(req.upstreamNodeId);
    pRsp->downstreamNodeId = htonl(pMeta->vgId);
    pRsp->downstreamTaskId = htonl(req.taskId);
    pRsp->msgId = htonl(req.msgId);
    pRsp->stage = htobe64(req.stage);
    pRsp->inputStatus = TASK_OUTPUT_STATUS__NORMAL;

    int32_t len = sizeof(SMsgHead) + sizeof(SStreamDispatchRsp);
    SRpcMsg rsp = {.code = TSDB_CODE_STREAM_TASK_NOT_EXIST, .info = pMsg->info, .contLen = len, .pCont = pRspHead};
    stError("s-task:0x%x send dispatch error rsp, no task", req.taskId);

    tmsgSendRsp(&rsp);
    tDeleteStreamDispatchReq(&req);

    return 0;
  }
}

int32_t streamTaskProcessDispatchRsp(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  SStreamDispatchRsp* pRsp = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));

  int32_t vgId = pMeta->vgId;
  pRsp->upstreamTaskId = htonl(pRsp->upstreamTaskId);
  pRsp->streamId = htobe64(pRsp->streamId);
  pRsp->downstreamTaskId = htonl(pRsp->downstreamTaskId);
  pRsp->downstreamNodeId = htonl(pRsp->downstreamNodeId);
  pRsp->stage = htobe64(pRsp->stage);
  pRsp->msgId = htonl(pRsp->msgId);

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, pRsp->streamId, pRsp->upstreamTaskId);
  if (pTask) {
    streamProcessDispatchRsp(pTask, pRsp, pMsg->code);
    streamMetaReleaseTask(pMeta, pTask);
    return TSDB_CODE_SUCCESS;
  } else {
    stDebug("vgId:%d failed to handle the dispatch rsp, since find task:0x%x failed", vgId, pRsp->upstreamTaskId);
    terrno = TSDB_CODE_STREAM_TASK_NOT_EXIST;
    return terrno;
  }
}

int32_t streamTaskProcessRetrieveReq(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  char*    msgStr = pMsg->pCont;
  char*    msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t  msgLen = pMsg->contLen - sizeof(SMsgHead);
  SDecoder decoder;

  SStreamRetrieveReq req;
  tDecoderInit(&decoder, (uint8_t*)msgBody, msgLen);
  tDecodeStreamRetrieveReq(&decoder, &req);
  tDecoderClear(&decoder);

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, req.streamId, req.dstTaskId);
  if (pTask == NULL) {
    stError("vgId:%d process retrieve req, failed to acquire task:0x%x, it may have been dropped already", pMeta->vgId,
            req.dstTaskId);
    return -1;
  }

  SRpcMsg rsp = {.info = pMsg->info, .code = 0};
  streamProcessRetrieveReq(pTask, &req, &rsp);

  streamMetaReleaseTask(pMeta, pTask);
  tDeleteStreamRetrieveReq(&req);
  return 0;
}

int32_t streamTaskProcessScanHistoryFinishReq(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  char*   msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);

  // deserialize
  SStreamScanHistoryFinishReq req = {0};

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, msgLen);
  tDecodeStreamScanHistoryFinishReq(&decoder, &req);
  tDecoderClear(&decoder);

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, req.streamId, req.downstreamTaskId);
  if (pTask == NULL) {
    stError("vgId:%d process scan history finish msg, failed to find task:0x%x, it may be destroyed",
            pMeta->vgId, req.downstreamTaskId);
    return -1;
  }

  stDebug("s-task:%s receive scan-history finish msg from task:0x%x", pTask->id.idStr, req.upstreamTaskId);

  int32_t code = streamProcessScanHistoryFinishReq(pTask, &req, &pMsg->info);
  streamMetaReleaseTask(pMeta, pTask);
  return code;
}

int32_t streamTaskProcessScanHistoryFinishRsp(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  char*   msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);

  // deserialize
  SStreamCompleteHistoryMsg req = {0};

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, msgLen);
  tDecodeCompleteHistoryDataMsg(&decoder, &req);
  tDecoderClear(&decoder);

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, req.streamId, req.upstreamTaskId);
  if (pTask == NULL) {
    stError("vgId:%d process scan history finish rsp, failed to find task:0x%x, it may be destroyed",
            pMeta->vgId, req.upstreamTaskId);
    return -1;
  }

  int32_t remain = atomic_sub_fetch_32(&pTask->notReadyTasks, 1);
  if (remain > 0) {
    stDebug("s-task:%s scan-history finish rsp received from downstream task:0x%x, unfinished remain:%d",
            pTask->id.idStr, req.downstreamId, remain);
  } else {
    stDebug(
        "s-task:%s scan-history finish rsp received from downstream task:0x%x, all downstream tasks rsp scan-history "
        "completed msg",
        pTask->id.idStr, req.downstreamId);
    streamProcessScanHistoryFinishRsp(pTask);
  }

  streamMetaReleaseTask(pMeta, pTask);
  return 0;
}

int32_t streamTaskProcessCheckReq(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  char*   msgStr = pMsg->pCont;
  char*   msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);

  SStreamTaskCheckReq req;
  SDecoder            decoder;

  tDecoderInit(&decoder, (uint8_t*)msgBody, msgLen);
  tDecodeStreamTaskCheckReq(&decoder, &req);
  tDecoderClear(&decoder);

  int32_t taskId = req.downstreamTaskId;

  SStreamTaskCheckRsp rsp = {
      .reqId = req.reqId,
      .streamId = req.streamId,
      .childId = req.childId,
      .downstreamNodeId = req.downstreamNodeId,
      .downstreamTaskId = req.downstreamTaskId,
      .upstreamNodeId = req.upstreamNodeId,
      .upstreamTaskId = req.upstreamTaskId,
  };

  // only the leader node handle the check request
  if (pMeta->role == NODE_ROLE_FOLLOWER) {
    stError("s-task:0x%x invalid check msg from upstream:0x%x(vgId:%d), vgId:%d is follower, not handle check status msg",
            taskId, req.upstreamTaskId, req.upstreamNodeId, pMeta->vgId);
    rsp.status = TASK_DOWNSTREAM_NOT_LEADER;
  } else {
    SStreamTask* pTask = streamMetaAcquireTask(pMeta, req.streamId, taskId);
    if (pTask != NULL) {
      rsp.status = streamTaskCheckStatus(pTask, req.upstreamTaskId, req.upstreamNodeId, req.stage, &rsp.oldStage);
      streamMetaReleaseTask(pMeta, pTask);

      char* p = NULL;
      streamTaskGetStatus(pTask, &p);
      stDebug("s-task:%s status:%s, stage:%"PRId64" recv task check req(reqId:0x%" PRIx64 ") task:0x%x (vgId:%d), check_status:%d",
              pTask->id.idStr, p, rsp.oldStage, rsp.reqId, rsp.upstreamTaskId, rsp.upstreamNodeId, rsp.status);
    } else {
      rsp.status = TASK_DOWNSTREAM_NOT_READY;
      stDebug("tq recv task check(taskId:0x%" PRIx64 "-0x%x not built yet) req(reqId:0x%" PRIx64
              ") from task:0x%x (vgId:%d), rsp check_status %d",
              req.streamId, taskId, rsp.reqId, rsp.upstreamTaskId, rsp.upstreamNodeId, rsp.status);
    }
  }

  return streamSendCheckRsp(pMeta, &req, &rsp, &pMsg->info, taskId);
}

int32_t streamTaskProcessCheckRsp(SStreamMeta* pMeta, SRpcMsg* pMsg, bool isLeader) {
  char*   pReq = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t len = pMsg->contLen - sizeof(SMsgHead);
  int32_t vgId = pMeta->vgId;

  int32_t             code;
  SStreamTaskCheckRsp rsp;

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)pReq, len);
  code = tDecodeStreamTaskCheckRsp(&decoder, &rsp);
  if (code < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    tDecoderClear(&decoder);
    stError("vgId:%d failed to parse check rsp msg, code:%s", vgId, tstrerror(terrno));
    return -1;
  }

  tDecoderClear(&decoder);
  stDebug("tq task:0x%x (vgId:%d) recv check rsp(reqId:0x%" PRIx64 ") from 0x%x (vgId:%d) status %d",
          rsp.upstreamTaskId, rsp.upstreamNodeId, rsp.reqId, rsp.downstreamTaskId, rsp.downstreamNodeId, rsp.status);

  if (!isLeader) {
    stError("vgId:%d not leader, task:0x%x not handle the check rsp, downstream:0x%x (vgId:%d)", vgId,
            rsp.upstreamTaskId, rsp.downstreamTaskId, rsp.downstreamNodeId);
    return code;
  }

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, rsp.streamId, rsp.upstreamTaskId);
  if (pTask == NULL) {
    stError("tq failed to locate the stream task:0x%" PRIx64 "-0x%x (vgId:%d), it may have been destroyed or stopped",
            rsp.streamId, rsp.upstreamTaskId, vgId);
    terrno = TSDB_CODE_STREAM_TASK_NOT_EXIST;
    return -1;
  }

  code = streamProcessCheckRsp(pTask, &rsp);
  streamMetaReleaseTask(pMeta, pTask);
  return code;
}

int32_t streamTaskProcessCheckpointReadyMsg(SStreamMeta* pMeta, SRpcMsg* pMsg) {
  int32_t      vgId = pMeta->vgId;
  char*        msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t      len = pMsg->contLen - sizeof(SMsgHead);
  int32_t      code = 0;

  SStreamCheckpointReadyMsg req = {0};

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, len);
  if (tDecodeStreamCheckpointReadyMsg(&decoder, &req) < 0) {
    code = TSDB_CODE_MSG_DECODE_ERROR;
    tDecoderClear(&decoder);
    return code;
  }
  tDecoderClear(&decoder);

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, req.streamId, req.upstreamTaskId);
  if (pTask == NULL) {
    stError("vgId:%d failed to find s-task:0x%x, it may have been destroyed already", vgId, req.downstreamTaskId);
    return code;
  }

  stDebug("vgId:%d s-task:%s received the checkpoint ready msg from task:0x%x (vgId:%d), handle it", vgId,
          pTask->id.idStr, req.downstreamTaskId, req.downstreamNodeId);

  streamProcessCheckpointReadyMsg(pTask);
  streamMetaReleaseTask(pMeta, pTask);
  return code;
}

int32_t streamTaskProcessDeployReq(SStreamMeta* pMeta, int64_t sversion, char* msg, int32_t msgLen, bool isLeader, bool restored) {
  int32_t code = 0;
  int32_t vgId = pMeta->vgId;

  if (tsDisableStream) {
    stInfo("vgId:%d stream disabled, not deploy stream tasks", vgId);
    return code;
  }

  stDebug("vgId:%d receive new stream task deploy msg, start to build stream task", vgId);

  // 1.deserialize msg and build task
  int32_t size = sizeof(SStreamTask);
  SStreamTask* pTask = taosMemoryCalloc(1, size);
  if (pTask == NULL) {
    stError("vgId:%d failed to create stream task due to out of memory, alloc size:%d", vgId, size);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, msgLen);
  code = tDecodeStreamTask(&decoder, pTask);
  tDecoderClear(&decoder);

  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pTask);
    return TSDB_CODE_INVALID_MSG;
  }

  // 2.save task, use the latest commit version as the initial start version of stream task.
  int32_t taskId = pTask->id.taskId;
  int64_t streamId = pTask->id.streamId;
  bool    added = false;

  streamMetaWLock(pMeta);
  code = streamMetaRegisterTask(pMeta, sversion, pTask, &added);
  int32_t numOfTasks = streamMetaGetNumOfTasks(pMeta);
  streamMetaWUnLock(pMeta);

  if (code < 0) {
    stError("failed to add s-task:0x%x into vgId:%d meta, total:%d, code:%s", vgId, taskId, numOfTasks, tstrerror(code));
    tFreeStreamTask(pTask);
    return code;
  }

  // added into meta store, pTask cannot be reference since it may have been destroyed by other threads already now if
  // it is added into the meta store
  if (added) {
    // only handled in the leader node
    if (isLeader) {
      stDebug("vgId:%d s-task:0x%x is deployed and add into meta, numOfTasks:%d", vgId, taskId, numOfTasks);
      SStreamTask* p = streamMetaAcquireTask(pMeta, streamId, taskId);

      if (p != NULL && restored && p->info.fillHistory == 0) {
        EStreamTaskEvent event = (HAS_RELATED_FILLHISTORY_TASK(p)) ? TASK_EVENT_INIT_STREAM_SCANHIST : TASK_EVENT_INIT;
        streamTaskHandleEvent(p->status.pSM, event);
      } else if (!restored) {
        stWarn("s-task:%s not launched since vnode(vgId:%d) not ready", p->id.idStr, vgId);
      }

      if (p != NULL) {
        streamMetaReleaseTask(pMeta, p);
      }
    } else {
      stDebug("vgId:%d not leader, not launch stream task s-task:0x%x", vgId, taskId);
    }
  } else {
    stWarn("vgId:%d failed to add s-task:0x%x, since already exists in meta store", vgId, taskId);
    tFreeStreamTask(pTask);
  }

  return code;
}

int32_t streamTaskProcessDropReq(SStreamMeta* pMeta, char* msg, int32_t msgLen) {
  SVDropStreamTaskReq* pReq = (SVDropStreamTaskReq*)msg;

  int32_t      vgId = pMeta->vgId;
  stDebug("vgId:%d receive msg to drop s-task:0x%x", vgId, pReq->taskId);

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, pReq->streamId, pReq->taskId);
  if (pTask != NULL) {
    // drop the related fill-history task firstly
    if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
      STaskId* pHTaskId = &pTask->hTaskInfo.id;
      streamMetaUnregisterTask(pMeta, pHTaskId->streamId, pHTaskId->taskId);
      stDebug("vgId:%d drop fill-history task:0x%x dropped firstly", vgId, (int32_t)pHTaskId->taskId);
    }
    streamMetaReleaseTask(pMeta, pTask);
  }

  // drop the stream task now
  streamMetaUnregisterTask(pMeta, pReq->streamId, pReq->taskId);

  // commit the update
  streamMetaWLock(pMeta);
  int32_t numOfTasks = streamMetaGetNumOfTasks(pMeta);
  stDebug("vgId:%d task:0x%x dropped, remain tasks:%d", vgId, pReq->taskId, numOfTasks);

  if (streamMetaCommit(pMeta) < 0) {
    // persist to disk
  }
  streamMetaWUnLock(pMeta);

  return 0;
}

int32_t startStreamTasks(SStreamMeta* pMeta) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      vgId = pMeta->vgId;

  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  stDebug("vgId:%d start to check all %d stream task(s) downstream status", vgId, numOfTasks);
  if (numOfTasks == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SArray* pTaskList = NULL;
  streamMetaWLock(pMeta);
  pTaskList = taosArrayDup(pMeta->pTaskList, NULL);
  taosHashClear(pMeta->startInfo.pReadyTaskSet);
  taosHashClear(pMeta->startInfo.pFailedTaskSet);
  pMeta->startInfo.startTs = taosGetTimestampMs();
  streamMetaWUnLock(pMeta);

  // broadcast the check downstream tasks msg
  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId* pTaskId = taosArrayGet(pTaskList, i);
    SStreamTask*   pTask = streamMetaAcquireTask(pMeta, pTaskId->streamId, pTaskId->taskId);
    if (pTask == NULL) {
      continue;
    }

    // fill-history task can only be launched by related stream tasks.
    if (pTask->info.fillHistory == 1) {
      streamMetaReleaseTask(pMeta, pTask);
      continue;
    }

    if (pTask->status.downstreamReady == 1) {
      if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
        stDebug("s-task:%s downstream ready, no need to check downstream, check only related fill-history task",
                pTask->id.idStr);
        streamLaunchFillHistoryTask(pTask);
      }

      streamMetaUpdateTaskDownstreamStatus(pTask, pTask->execInfo.init, pTask->execInfo.start, true);
      streamMetaReleaseTask(pMeta, pTask);
      continue;
    }

    EStreamTaskEvent event = (HAS_RELATED_FILLHISTORY_TASK(pTask)) ? TASK_EVENT_INIT_STREAM_SCANHIST : TASK_EVENT_INIT;
    int32_t ret = streamTaskHandleEvent(pTask->status.pSM, event);
    if (ret != TSDB_CODE_SUCCESS) {
      code = ret;
    }

    streamMetaReleaseTask(pMeta, pTask);
  }

  taosArrayDestroy(pTaskList);
  return code;
}

int32_t resetStreamTaskStatus(SStreamMeta* pMeta) {
  int32_t      vgId = pMeta->vgId;
  int32_t      numOfTasks = taosArrayGetSize(pMeta->pTaskList);

  stDebug("vgId:%d reset all %d stream task(s) status to be uninit", vgId, numOfTasks);
  if (numOfTasks == 0) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId* pTaskId = taosArrayGet(pMeta->pTaskList, i);

    STaskId id = {.streamId = pTaskId->streamId, .taskId = pTaskId->taskId};
    SStreamTask** pTask = taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
    streamTaskResetStatus(*pTask);
  }

  return 0;
}

static int32_t restartStreamTasks(SStreamMeta* pMeta, bool isLeader) {
  int32_t      vgId = pMeta->vgId;
  int32_t      code = 0;
  int64_t      st = taosGetTimestampMs();

  while(1) {
    int32_t startVal = atomic_val_compare_exchange_32(&pMeta->startInfo.taskStarting, 0, 1);
    if (startVal == 0) {
      break;
    }

    stDebug("vgId:%d in start stream tasks procedure, wait for 500ms and recheck", vgId);
    taosMsleep(500);
  }

  terrno = 0;
  stInfo("vgId:%d tasks are all updated and stopped, restart all tasks, triggered by transId:%d", vgId,
         pMeta->updateInfo.transId);

  while (streamMetaTaskInTimer(pMeta)) {
    stDebug("vgId:%d some tasks in timer, wait for 100ms and recheck", pMeta->vgId);
    taosMsleep(100);
  }

  streamMetaWLock(pMeta);
  code = streamMetaReopen(pMeta);
  if (code != TSDB_CODE_SUCCESS) {
    stError("vgId:%d failed to reopen stream meta", vgId);
    streamMetaWUnLock(pMeta);
    code = terrno;
    return code;
  }

  streamMetaInitBackend(pMeta);
  int64_t el = taosGetTimestampMs() - st;

  stInfo("vgId:%d close&reload state elapsed time:%.3fs", vgId, el/1000.);

  code = streamMetaLoadAllTasks(pMeta);
  if (code != TSDB_CODE_SUCCESS) {
    stError("vgId:%d failed to load stream tasks, code:%s", vgId, tstrerror(terrno));
    streamMetaWUnLock(pMeta);
    code = terrno;
    return code;
  }

  if (isLeader && !tsDisableStream) {
    stInfo("vgId:%d restart all stream tasks after all tasks being updated", vgId);
    resetStreamTaskStatus(pMeta);

    streamMetaWUnLock(pMeta);
    startStreamTasks(pMeta);
  } else {
    streamMetaResetStartInfo(&pMeta->startInfo);
    streamMetaWUnLock(pMeta);
    stInfo("vgId:%d, follower node not start stream tasks", vgId);
  }

  code = terrno;
  return code;
}

int32_t streamTaskProcessRunReq(SStreamMeta* pMeta, SRpcMsg* pMsg, bool isLeader) {
  SStreamTaskRunReq* pReq = pMsg->pCont;

  int32_t taskId = pReq->taskId;
  int32_t vgId = pMeta->vgId;

  if (taskId == STREAM_EXEC_START_ALL_TASKS_ID) {
    startStreamTasks(pMeta);
    return 0;
  } else if (taskId == STREAM_EXEC_RESTART_ALL_TASKS_ID) {
    restartStreamTasks(pMeta, isLeader);
    return 0;
  }

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, pReq->streamId, taskId);
  if (pTask != NULL) { // even in halt status, the data in inputQ must be processed
    char* p = NULL;
    if (streamTaskReadyToRun(pTask, &p)) {
      stDebug("vgId:%d s-task:%s start to process block from inputQ, next checked ver:%" PRId64, vgId, pTask->id.idStr,
              pTask->chkInfo.nextProcessVer);
      streamExecTask(pTask);
    } else {
      int8_t status = streamTaskSetSchedStatusInactive(pTask);
      stDebug("vgId:%d s-task:%s ignore run req since not in ready state, status:%s, sched-status:%d", vgId,
              pTask->id.idStr, p, status);
    }

    streamMetaReleaseTask(pMeta, pTask);
    return 0;
  } else {  // NOTE: pTask->status.schedStatus is not updated since it is not be handled by the run exec.
    // todo add one function to handle this
    stError("vgId:%d failed to found s-task, taskId:0x%x may have been dropped", vgId, taskId);
    return -1;
  }
}
