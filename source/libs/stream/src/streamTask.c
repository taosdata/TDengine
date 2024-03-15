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

static int32_t doUpdateTaskEpset(SStreamTask* pTask, int32_t nodeId, SEpSet* pEpSet) {
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

static void freeItem(void* p) {
  SStreamContinueExecInfo* pInfo = p;
  rpcFreeCont(pInfo->msg.pCont);
}

static void freeUpstreamItem(void* p) {
  SStreamChildEpInfo** pInfo = p;
  taosMemoryFree(*pInfo);
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

SStreamTask* tNewStreamTask(int64_t streamId, int8_t taskLevel, SEpSet* pEpset, bool fillHistory, int64_t triggerParam,
                            SArray* pTaskList, bool hasFillhistory, int8_t subtableWithoutMd5) {
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
  pTask->subtableWithoutMd5 = subtableWithoutMd5;

  pTask->status.pSM = streamCreateStateMachine(pTask);
  if (pTask->status.pSM == NULL) {
    taosMemoryFreeClear(pTask);
    return NULL;
  }

  char buf[128] = {0};
  sprintf(buf, "0x%" PRIx64 "-0x%x", pTask->id.streamId, pTask->id.taskId);

  pTask->id.idStr = taosStrdup(buf);
  pTask->status.schedStatus = TASK_SCHED_STATUS__INACTIVE;
  pTask->status.taskStatus = fillHistory? TASK_STATUS__SCAN_HISTORY : TASK_STATUS__READY;
  pTask->inputq.status = TASK_INPUT_STATUS__NORMAL;
  pTask->outputq.status = TASK_OUTPUT_STATUS__NORMAL;

  if (fillHistory) {
    ASSERT(hasFillhistory);
  }

  epsetAssign(&(pTask->info.mnodeEpset), pEpset);

  addToTaskset(pTaskList, pTask);
  return pTask;
}

int32_t tEncodeStreamEpInfo(SEncoder* pEncoder, const SStreamChildEpInfo* pInfo) {
  if (tEncodeI32(pEncoder, pInfo->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pInfo->nodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pInfo->childId) < 0) return -1;
  if (tEncodeSEpSet(pEncoder, &pInfo->epSet) < 0) return -1;
  if (tEncodeI64(pEncoder, pInfo->stage) < 0) return -1;
  return 0;
}

int32_t tDecodeStreamEpInfo(SDecoder* pDecoder, SStreamChildEpInfo* pInfo) {
  if (tDecodeI32(pDecoder, &pInfo->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pInfo->nodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pInfo->childId) < 0) return -1;
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
  if (tEncodeI8(pEncoder, pTask->subtableWithoutMd5) < 0) return -1;
  if (tEncodeCStrWithLen(pEncoder, pTask->reserve, sizeof(pTask->reserve) - 1) < 0) return -1;

  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamTask(SDecoder* pDecoder, SStreamTask* pTask) {
  int32_t taskId = 0;

  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pTask->ver) < 0) return -1;
  if (pTask->ver <= SSTREAM_TASK_INCOMPATIBLE_VER) return -1;

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
  if (tDecodeI8(pDecoder, &pTask->subtableWithoutMd5) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pTask->reserve) < 0) return -1;

  tEndDecode(pDecoder);
  return 0;
}

int32_t tDecodeStreamTaskChkInfo(SDecoder* pDecoder, SCheckpointInfo* pChkpInfo) {
  int64_t skip64;
  int8_t  skip8;
  int32_t skip32;
  int16_t skip16;
  SEpSet  epSet;

  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pChkpInfo->msgVer) < 0) return -1;
  // if (ver <= SSTREAM_TASK_INCOMPATIBLE_VER) return -1;

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
  if (ver <= SSTREAM_TASK_INCOMPATIBLE_VER) return -1;

  if (tDecodeI64(pDecoder, &pTaskId->streamId) < 0) return -1;

  int32_t taskId = 0;
  if (tDecodeI32(pDecoder, &taskId) < 0) return -1;

  pTaskId->taskId = taskId;
  tEndDecode(pDecoder);
  return 0;
}

void tFreeStreamTask(SStreamTask* pTask) {
  char*                p = NULL;
  int32_t              taskId = pTask->id.taskId;
  STaskExecStatisInfo* pStatis = &pTask->execInfo;

  ETaskStatus status1 = TASK_STATUS__UNINIT;
  taosThreadMutexLock(&pTask->lock);
  if (pTask->status.pSM != NULL) {
    SStreamTaskState* pStatus = streamTaskGetStatus(pTask);
    p = pStatus->name;
    status1 = pStatus->state;
  }
  taosThreadMutexUnlock(&pTask->lock);

  stDebug("start to free s-task:0x%x, %p, state:%s", taskId, pTask, p);

  SCheckpointInfo* pCkInfo = &pTask->chkInfo;
  stDebug("s-task:0x%x task exec summary: create:%" PRId64 ", init:%" PRId64 ", start:%" PRId64
          ", updateCount:%d latestUpdate:%" PRId64 ", latestCheckPoint:%" PRId64 ", ver:%" PRId64
          " nextProcessVer:%" PRId64 ", checkpointCount:%d",
          taskId, pStatis->created, pStatis->init, pStatis->start, pStatis->updateCount, pStatis->latestUpdateTs,
          pCkInfo->checkpointId, pCkInfo->checkpointVer, pCkInfo->nextProcessVer, pStatis->checkpoint);

  // remove the ref by timer
  while (pTask->status.timerActive > 0) {
    stDebug("s-task:%s wait for task stop timer activities, ref:%d", pTask->id.idStr, pTask->status.timerActive);
    taosMsleep(100);
  }

  if (pTask->schedInfo.pDelayTimer != NULL) {
    taosTmrStop(pTask->schedInfo.pDelayTimer);
    pTask->schedInfo.pDelayTimer = NULL;
  }

  if (pTask->hTaskInfo.pTimer != NULL) {
    taosTmrStop(pTask->hTaskInfo.pTimer);
    pTask->hTaskInfo.pTimer = NULL;
  }

  if (pTask->msgInfo.pTimer != NULL) {
    taosTmrStop(pTask->msgInfo.pTimer);
    pTask->msgInfo.pTimer = NULL;
  }

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
    streamStateClose(pTask->pState, status1 == TASK_STATUS__DROPPING);
    taskDbRemoveRef(pTask->pBackend);
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

  pTask->inputq.status = TASK_INPUT_STATUS__NORMAL;
  pTask->outputq.status = TASK_OUTPUT_STATUS__NORMAL;
  pTask->inputq.queue = streamQueueOpen(512 << 10);
  pTask->outputq.queue = streamQueueOpen(512 << 10);
  if (pTask->inputq.queue == NULL || pTask->outputq.queue == NULL) {
    stError("s-task:%s failed to prepare the input/output queue, initialize task failed", pTask->id.idStr);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pTask->status.schedStatus = TASK_SCHED_STATUS__INACTIVE;
  pTask->status.timerActive = 0;
  pTask->status.pSM = streamCreateStateMachine(pTask);
  if (pTask->status.pSM == NULL) {
    stError("s-task:%s failed create state-machine for stream task, initialization failed, code:%s", pTask->id.idStr,
            tstrerror(terrno));
    return terrno;
  }

  pTask->execInfo.created = taosGetTimestampMs();
  SCheckpointInfo* pChkInfo = &pTask->chkInfo;
  SDataRange*      pRange = &pTask->dataRange;

  // only set the version info for stream tasks without fill-history task
  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    if ((pTask->info.fillHistory == 0) && (!HAS_RELATED_FILLHISTORY_TASK(pTask))) {
      pChkInfo->checkpointVer = ver - 1;  // only update when generating checkpoint
      pChkInfo->processedVer = ver - 1;   // already processed version
      pChkInfo->nextProcessVer = ver;     // next processed version

      pRange->range.maxVer = ver;
      pRange->range.minVer = ver;
    } else {
      // the initial value of processedVer/nextProcessVer/checkpointVer for stream task with related fill-history task
      // is set at the mnode.
      if (pTask->info.fillHistory == 1) {
        pChkInfo->checkpointVer = pRange->range.maxVer;
        pChkInfo->processedVer = pRange->range.maxVer;
        pChkInfo->nextProcessVer = pRange->range.maxVer + 1;
      } else {
        pChkInfo->checkpointVer = pRange->range.minVer - 1;
        pChkInfo->processedVer = pRange->range.minVer - 1;
        pChkInfo->nextProcessVer = pRange->range.minVer;

        { // for compatible purpose, remove it later
          if (pRange->range.minVer == 0) {
            pChkInfo->checkpointVer = 0;
            pChkInfo->processedVer = 0;
            pChkInfo->nextProcessVer = 1;
            stDebug("s-task:%s update the processedVer to 0 from -1 due to compatible purpose", pTask->id.idStr);
          }
        }
      }
    }
  }

  pTask->pMeta = pMeta;
  pTask->pMsgCb = pMsgCb;
  pTask->msgInfo.pRetryList = taosArrayInit(4, sizeof(int32_t));

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
  taosThreadMutexAttrDestroy(&attr);
  streamTaskOpenAllUpstreamInput(pTask);

  STaskOutputInfo* pOutputInfo = &pTask->outputInfo;
  pOutputInfo->pTokenBucket = taosMemoryCalloc(1, sizeof(STokenBucket));
  if (pOutputInfo->pTokenBucket == NULL) {
    stError("s-task:%s failed to prepare the tokenBucket, code:%s", pTask->id.idStr,
            tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  // 2MiB per second for sink task
  // 50 times sink operator per second
  streamTaskInitTokenBucket(pOutputInfo->pTokenBucket, 35, 35, tsSinkDataRate, pTask->id.idStr);
  pOutputInfo->pDownstreamUpdateList = taosArrayInit(4, sizeof(SDownstreamTaskEpset));
  if (pOutputInfo->pDownstreamUpdateList == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t streamTaskGetNumOfDownstream(const SStreamTask* pTask) {
  if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
    return 0;
  }

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
  while (!streamTaskIsIdle(pTask)) {
    stDebug("s-task:%s level:%d wait for task to be idle and then close, check again in 100ms", id,
            pTask->info.taskLevel);
    taosMsleep(100);
  }

  int64_t el = taosGetTimestampMs() - st;
  stDebug("vgId:%d s-task:%s is closed in %" PRId64 " ms", vgId, id, el);
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

void streamTaskOpenAllUpstreamInput(SStreamTask* pTask) {
  int32_t num = taosArrayGetSize(pTask->upstreamInfo.pList);
  if (num == 0) {
    return;
  }

  for (int32_t i = 0; i < num; ++i) {
    SStreamChildEpInfo* pInfo = taosArrayGetP(pTask->upstreamInfo.pList, i);
    pInfo->dataAllowed = true;
  }

  pTask->upstreamInfo.numOfClosed = 0;
  stDebug("s-task:%s opening up inputQ for %d upstream tasks", pTask->id.idStr, num);
}

void streamTaskCloseUpstreamInput(SStreamTask* pTask, int32_t taskId) {
  SStreamChildEpInfo* pInfo = streamTaskGetUpstreamTaskEpInfo(pTask, taskId);
  if (pInfo != NULL) {
    pInfo->dataAllowed = false;
  }
}

bool streamTaskIsAllUpstreamClosed(SStreamTask* pTask) {
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

int32_t streamTaskClearHTaskAttr(SStreamTask* pTask, int32_t resetRelHalt, bool metaLock) {
  if (pTask == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SStreamMeta* pMeta = pTask->pMeta;
  STaskId      sTaskId = {.streamId = pTask->streamTaskId.streamId, .taskId = pTask->streamTaskId.taskId};
  if (pTask->info.fillHistory == 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (metaLock) {
    streamMetaWLock(pMeta);
  }

  SStreamTask** ppStreamTask = (SStreamTask**)taosHashGet(pMeta->pTasksMap, &sTaskId, sizeof(sTaskId));
  if (ppStreamTask != NULL) {
    stDebug("s-task:%s clear the related stream task:0x%x attr to fill-history task", pTask->id.idStr,
            (int32_t)sTaskId.taskId);

    taosThreadMutexLock(&(*ppStreamTask)->lock);
    CLEAR_RELATED_FILLHISTORY_TASK((*ppStreamTask));

    if (resetRelHalt) {
      (*ppStreamTask)->status.taskStatus = TASK_STATUS__READY;
      stDebug("s-task:0x%" PRIx64 " set the status to be ready", sTaskId.taskId);
    }

    streamMetaSaveTask(pMeta, *ppStreamTask);
    taosThreadMutexUnlock(&(*ppStreamTask)->lock);
  }

  if (metaLock) {
    streamMetaWUnLock(pMeta);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t streamBuildAndSendDropTaskMsg(SMsgCb* pMsgCb, int32_t vgId, SStreamTaskId* pTaskId, int64_t resetRelHalt) {
  SVDropStreamTaskReq* pReq = rpcMallocCont(sizeof(SVDropStreamTaskReq));
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pReq->head.vgId = vgId;
  pReq->taskId = pTaskId->taskId;
  pReq->streamId = pTaskId->streamId;
  pReq->resetRelHalt = resetRelHalt;

  SRpcMsg msg = {.msgType = TDMT_STREAM_TASK_DROP, .pCont = pReq, .contLen = sizeof(SVDropStreamTaskReq)};
  int32_t code = tmsgPutToQueue(pMsgCb, WRITE_QUEUE, &msg);
  if (code != TSDB_CODE_SUCCESS) {
    stError("vgId:%d failed to send drop task:0x%x msg, code:%s", vgId, pTaskId->taskId, tstrerror(code));
    return code;
  }

  stDebug("vgId:%d build and send drop task:0x%x msg", vgId, pTaskId->taskId);
  return code;
}

STaskId streamTaskGetTaskId(const SStreamTask* pTask) {
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
  pDst->checkpointId = pSrc->checkpointId;
  pDst->checkpointFailed = pSrc->checkpointFailed;
  pDst->chkpointTransId = pSrc->chkpointTransId;
}

void streamTaskPause(SStreamMeta* pMeta, SStreamTask* pTask) {
  streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_PAUSE);

  int32_t num = atomic_add_fetch_32(&pMeta->numOfPausedTasks, 1);
  stInfo("vgId:%d s-task:%s pause stream task. pause task num:%d", pMeta->vgId, pTask->id.idStr, num);

  // in case of fill-history task, stop the tsdb file scan operation.
  if (pTask->info.fillHistory == 1) {
    void* pExecutor = pTask->exec.pExecutor;
    qKillTask(pExecutor, TSDB_CODE_SUCCESS);
  }

  stDebug("vgId:%d s-task:%s set pause flag and pause task", pMeta->vgId, pTask->id.idStr);
}

void streamTaskResume(SStreamTask* pTask) {
  SStreamTaskState prevState = *streamTaskGetStatus(pTask);
  SStreamMeta*     pMeta = pTask->pMeta;

  if (prevState.state == TASK_STATUS__PAUSE || prevState.state == TASK_STATUS__HALT) {
    streamTaskRestoreStatus(pTask);

    char* pNew = streamTaskGetStatus(pTask)->name;
    if (prevState.state == TASK_STATUS__PAUSE) {
      int32_t num = atomic_sub_fetch_32(&pMeta->numOfPausedTasks, 1);
      stInfo("s-task:%s status:%s resume from %s, paused task(s):%d", pTask->id.idStr, pNew, prevState.name, num);
    } else {
      stInfo("s-task:%s status:%s resume from %s", pTask->id.idStr, pNew, prevState.name);
    }
  } else {
    stDebug("s-task:%s status:%s not in pause/halt status, no need to resume", pTask->id.idStr, prevState.name);
  }
}

bool streamTaskIsSinkTask(const SStreamTask* pTask) { return pTask->info.taskLevel == TASK_LEVEL__SINK; }

int32_t streamTaskSendCheckpointReq(SStreamTask* pTask) {
  int32_t     code;
  int32_t     tlen = 0;
  int32_t     vgId = pTask->pMeta->vgId;
  const char* id = pTask->id.idStr;

  SStreamTaskCheckpointReq req = {.streamId = pTask->id.streamId, .taskId = pTask->id.taskId, .nodeId = vgId};
  tEncodeSize(tEncodeStreamTaskCheckpointReq, &req, tlen, code);
  if (code < 0) {
    stError("s-task:%s vgId:%d encode stream task req checkpoint failed, code:%s", id, vgId, tstrerror(code));
    return -1;
  }

  void* buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    stError("s-task:%s vgId:%d encode stream task req checkpoint msg failed, code:%s", id, vgId,
            tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    return -1;
  }

  SEncoder encoder;
  tEncoderInit(&encoder, buf, tlen);
  if ((code = tEncodeStreamTaskCheckpointReq(&encoder, &req)) < 0) {
    rpcFreeCont(buf);
    stError("s-task:%s vgId:%d encode stream task req checkpoint msg failed, code:%s", id, vgId, tstrerror(code));
    return -1;
  }
  tEncoderClear(&encoder);

  SRpcMsg msg = {0};
  initRpcMsg(&msg, TDMT_MND_STREAM_REQ_CHKPT, buf, tlen);
  stDebug("s-task:%s vgId:%d build and send task checkpoint req", id, vgId);

  tmsgSendReq(&pTask->info.mnodeEpset, &msg);
  return 0;
}
