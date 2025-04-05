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
#include "osDir.h"
#include "osMemory.h"
#include "streamInt.h"
#include "streamMsg.h"
#include "streamsm.h"
#include "tmisce.h"
#include "tstream.h"
#include "ttimer.h"
#include "wal.h"

static void    streamTaskDestroyUpstreamInfo(SUpstreamInfo* pUpstreamInfo);
static int32_t streamTaskUpdateUpstreamInfo(SStreamTask* pTask, int32_t nodeId, const SEpSet* pEpSet, bool* pUpdated);
static int32_t streamTaskUpdateDownstreamInfo(SStreamTask* pTask, int32_t nodeId, const SEpSet* pEpSet, bool* pUpdate);
static void    streamTaskDestroyActiveChkptInfo(SActiveCheckpointInfo* pInfo);

static int32_t addToTaskset(SArray* pArray, SStreamTask* pTask) {
  int32_t childId = taosArrayGetSize(pArray);
  pTask->info.selfChildId = childId;
  void* p = taosArrayPush(pArray, &pTask);
  return (p == NULL) ? terrno : TSDB_CODE_SUCCESS;
}

static int32_t doUpdateTaskEpset(SStreamTask* pTask, int32_t nodeId, SEpSet* pEpSet, bool* pUpdated) {
  int32_t code = 0;
  char    buf[512] = {0};

  if (pTask->info.nodeId == nodeId) {  // execution task should be moved away
    bool isEqual = isEpsetEqual(&pTask->info.epSet, pEpSet);
    code = epsetToStr(pEpSet, buf, tListLen(buf));
    if (code) {  // print error and continue
      stError("%s failed to convert epset to str, code:%s", pTask->id.idStr, tstrerror(code));
      return code;
    }

    if (!isEqual) {
      (*pUpdated) = true;
      char tmp[512] = {0};
      code = epsetToStr(&pTask->info.epSet, tmp, tListLen(tmp));  // only for log file, ignore errors
      if (code) {                                                 // print error and continue
        stError("%s failed to convert epset to str, code:%s", pTask->id.idStr, tstrerror(code));
        return code;
      }

      epsetAssign(&pTask->info.epSet, pEpSet);
      stDebug("s-task:0x%x (vgId:%d) self node epset is updated %s, old:%s", pTask->id.taskId, nodeId, buf, tmp);
    } else {
      stDebug("s-task:0x%x (vgId:%d) not updated task epset, since epset identical, %s", pTask->id.taskId, nodeId, buf);
    }
  }

  // check for the dispatch info and the upstream task info
  int32_t level = pTask->info.taskLevel;
  if (level == TASK_LEVEL__SOURCE) {
    code = streamTaskUpdateDownstreamInfo(pTask, nodeId, pEpSet, pUpdated);
  } else if (level == TASK_LEVEL__AGG) {
    code = streamTaskUpdateUpstreamInfo(pTask, nodeId, pEpSet, pUpdated);
    code = streamTaskUpdateDownstreamInfo(pTask, nodeId, pEpSet, pUpdated);
  } else {  // TASK_LEVEL__SINK
    code = streamTaskUpdateUpstreamInfo(pTask, nodeId, pEpSet, pUpdated);
  }

  return code;
}

static void freeItem(void* p) {
  SStreamContinueExecInfo* pInfo = p;
  rpcFreeCont(pInfo->msg.pCont);
}

static void freeUpstreamItem(void* p) {
  SStreamUpstreamEpInfo** pInfo = p;
  taosMemoryFree(*pInfo);
}

static SStreamUpstreamEpInfo* createStreamTaskEpInfo(const SStreamTask* pTask) {
  SStreamUpstreamEpInfo* pEpInfo = taosMemoryMalloc(sizeof(SStreamUpstreamEpInfo));
  if (pEpInfo == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pEpInfo->childId = pTask->info.selfChildId;
  pEpInfo->epSet = pTask->info.epSet;
  pEpInfo->nodeId = pTask->info.nodeId;
  pEpInfo->taskId = pTask->id.taskId;
  pEpInfo->dataAllowed = true;
  pEpInfo->stage = -1;
  pEpInfo->lastMsgId = -1;

  return pEpInfo;
}

int32_t tNewStreamTask(int64_t streamId, int8_t taskLevel, SEpSet* pEpset, EStreamTaskType type, int32_t trigger,
                       int64_t triggerParam, SArray* pTaskList, bool hasFillhistory, int8_t subtableWithoutMd5,
                       int8_t hasAggTasks, SStreamTask** p) {
  *p = NULL;

  SStreamTask* pTask = (SStreamTask*)taosMemoryCalloc(1, sizeof(SStreamTask));
  if (pTask == NULL) {
    stError("s-task:0x%" PRIx64 " failed malloc new stream task, size:%d, code:%s", streamId,
            (int32_t)sizeof(SStreamTask), tstrerror(terrno));
    return terrno;
  }

  pTask->ver = SSTREAM_TASK_VER;
  pTask->id.taskId = tGenIdPI32();
  pTask->id.streamId = streamId;

  pTask->info.taskLevel = taskLevel;
  pTask->info.fillHistory = type;
  pTask->info.trigger = trigger;
  pTask->info.hasAggTasks = hasAggTasks;
  pTask->info.delaySchedParam = triggerParam;
  pTask->subtableWithoutMd5 = subtableWithoutMd5;

  int32_t code = streamCreateStateMachine(pTask);
  if (pTask->status.pSM == NULL || code != TSDB_CODE_SUCCESS) {
    taosMemoryFreeClear(pTask);
    return code;
  }

  char    buf[128] = {0};
  int32_t ret = snprintf(buf, tListLen(buf), "0x%" PRIx64 "-0x%x", pTask->id.streamId, pTask->id.taskId);
  if (ret < 0 || ret >= tListLen(buf)) {
    stError("s-task:0x%x failed to set the taskIdstr, code: out of buffer", pTask->id.taskId);
    return TSDB_CODE_OUT_OF_BUFFER;
  }

  pTask->id.idStr = taosStrdup(buf);
  if (pTask->id.idStr == NULL) {
    stError("s-task:0x%x failed to build task id, code: out of memory", pTask->id.taskId);
    return terrno;
  }

  pTask->status.schedStatus = TASK_SCHED_STATUS__INACTIVE;
  pTask->status.taskStatus =
      (pTask->info.fillHistory == STREAM_HISTORY_TASK) ? TASK_STATUS__SCAN_HISTORY : TASK_STATUS__READY;
  pTask->inputq.status = TASK_INPUT_STATUS__NORMAL;
  pTask->outputq.status = TASK_OUTPUT_STATUS__NORMAL;

  pTask->taskCheckInfo.pList = taosArrayInit(4, sizeof(SDownstreamStatusInfo));

  if ((pTask->info.fillHistory == STREAM_HISTORY_TASK) && !hasFillhistory) {
    stError("s-task:0x%x create task failed, due to inconsistent fill-history flag", pTask->id.taskId);
    return TSDB_CODE_INVALID_PARA;
  }

  epsetAssign(&(pTask->info.mnodeEpset), pEpset);

  code = addToTaskset(pTaskList, pTask);
  *p = pTask;

  return code;
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

void tFreeStreamTask(void* pParam) {
  char*        p = NULL;
  SStreamTask* pTask = pParam;
  int32_t      taskId = pTask->id.taskId;

  STaskExecStatisInfo* pStatis = &pTask->execInfo;

  ETaskStatus status1 = TASK_STATUS__UNINIT;
  if (pTask->status.pSM != NULL) {
    streamMutexLock(&pTask->lock);
    SStreamTaskState status = streamTaskGetStatus(pTask);
    p = status.name;
    status1 = status.state;
    streamMutexUnlock(&pTask->lock);
  }

  stDebug("start to free s-task:0x%x %p, state:%s, refId:%" PRId64, taskId, pTask, p, pTask->id.refId);

  SCheckpointInfo* pCkInfo = &pTask->chkInfo;
  stDebug("s-task:0x%x task exec summary: create:%" PRId64 ", init:%" PRId64 ", start:%" PRId64
          ", updateCount:%d latestUpdate:%" PRId64 ", latestCheckPoint:%" PRId64 ", ver:%" PRId64
          " nextProcessVer:%" PRId64 ", checkpointCount:%d",
          taskId, pStatis->created, pStatis->checkTs, pStatis->readyTs, pStatis->updateCount, pStatis->latestUpdateTs,
          pCkInfo->checkpointId, pCkInfo->checkpointVer, pCkInfo->nextProcessVer, pStatis->checkpoint);

  if (pTask->schedInfo.pDelayTimer != NULL) {
    streamTmrStop(pTask->schedInfo.pDelayTimer);
    pTask->schedInfo.pDelayTimer = NULL;
  }

  if (pTask->hTaskInfo.pTimer != NULL) {
    streamTmrStop(pTask->hTaskInfo.pTimer);
    pTask->hTaskInfo.pTimer = NULL;
  }

  if (pTask->msgInfo.pRetryTmr != NULL) {
    streamTmrStop(pTask->msgInfo.pRetryTmr);
    pTask->msgInfo.pRetryTmr = NULL;
  }

  if (pTask->inputq.queue) {
    streamQueueClose(pTask->inputq.queue, pTask->id.taskId);
    pTask->inputq.queue = NULL;
  }

  if (pTask->outputq.queue) {
    streamQueueClose(pTask->outputq.queue, pTask->id.taskId);
    pTask->outputq.queue = NULL;
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
    pTask->exec.pWalReader = NULL;
  }

  streamClearChkptReadyMsg(pTask->chkInfo.pActiveInfo);

  if (pTask->msgInfo.pData != NULL) {
    clearBufferedDispatchMsg(pTask);
  }

  if (pTask->outputInfo.type == TASK_OUTPUT__TABLE) {
    tDeleteSchemaWrapper(pTask->outputInfo.tbSink.pSchemaWrapper);
    taosMemoryFree(pTask->outputInfo.tbSink.pTSchema);
    tSimpleHashCleanup(pTask->outputInfo.tbSink.pTbInfo);
    tDeleteSchemaWrapper(pTask->outputInfo.tbSink.pTagSchema);
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    taosArrayDestroy(pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos);
  } else if (pTask->outputInfo.type == TASK_OUTPUT__VTABLE_MAP) {
    taosArrayDestroy(pTask->outputInfo.vtableMapDispatcher.taskInfos);
    tSimpleHashCleanup(pTask->outputInfo.vtableMapDispatcher.vtableMap);
  }

  streamTaskCleanupCheckInfo(&pTask->taskCheckInfo);
  streamFreeTaskState(pTask, pTask->status.removeBackendFiles ? 1 : 0);

  if (pTask->pNameMap) {
    tSimpleHashCleanup(pTask->pNameMap);
  }

  if (pTask->status.pSM != NULL) {
    streamMutexDestroy(&pTask->lock);
    streamMutexDestroy(&pTask->msgInfo.lock);
    streamMutexDestroy(&pTask->taskCheckInfo.checkInfoLock);
  }

  taosArrayDestroy(pTask->pVTables);
  pTask->pVTables = NULL;

  streamDestroyStateMachine(pTask->status.pSM);
  pTask->status.pSM = NULL;

  streamTaskDestroyUpstreamInfo(&pTask->upstreamInfo);

  taosMemoryFree(pTask->outputInfo.pTokenBucket);

  taosArrayDestroy(pTask->msgInfo.pSendInfo);
  pTask->msgInfo.pSendInfo = NULL;

  taosArrayDestroy(pTask->outputInfo.pNodeEpsetUpdateList);
  pTask->outputInfo.pNodeEpsetUpdateList = NULL;

  if (pTask->id.idStr != NULL) {
    taosMemoryFree((void*)pTask->id.idStr);
  }

  streamTaskDestroyActiveChkptInfo(pTask->chkInfo.pActiveInfo);
  pTask->chkInfo.pActiveInfo = NULL;

  taosArrayDestroyP(pTask->notifyInfo.pNotifyAddrUrls, NULL);
  taosMemoryFreeClear(pTask->notifyInfo.streamName);
  taosMemoryFreeClear(pTask->notifyInfo.stbFullName);
  tDeleteSchemaWrapper(pTask->notifyInfo.pSchemaWrapper);

  pTask->notifyEventStat = (STaskNotifyEventStat){0};

  taosMemoryFree(pTask);
  stDebug("s-task:0x%x free task completed", taskId);
}

void streamFreeTaskState(SStreamTask* pTask, int8_t remove) {
  stDebug("s-task:0x%x start to free task state/backend", pTask->id.taskId);
  if (pTask->pState != NULL) {
    stDebug("s-task:0x%x start to free task state", pTask->id.taskId);
    streamStateClose(pTask->pState, remove);

    if (remove) taskDbSetClearFileFlag(pTask->pBackend);
    taskDbRemoveRef(pTask->pBackend);
    pTask->pBackend = NULL;
    pTask->pState = NULL;

  } else {
    stDebug("s-task:0x%x task state is NULL, may del backend:%s", pTask->id.taskId,
            pTask->backendPath ? pTask->backendPath : "NULL");
    if (remove) {
      if (pTask->backendPath != NULL) {
        stDebug("s-task:0x%x task state is NULL, do del backend:%s", pTask->id.taskId, pTask->backendPath);
        taosRemoveDir(pTask->backendPath);
      }
    }
  }

  if (pTask->backendPath != NULL) {
    taosMemoryFree(pTask->backendPath);
    pTask->backendPath = NULL;
  }
  // clear recal backend

  if (pTask->pRecalState != NULL) {
    stDebug("s-task:0x%x start to free recal task state", pTask->id.taskId);
    streamStateClose(pTask->pRecalState, remove);

    if (remove) taskDbSetClearFileFlag(pTask->pRecalBackend);
    taskDbRemoveRef(pTask->pRecalBackend);
    pTask->pRecalBackend = NULL;
    pTask->pRecalState = NULL;

  }  // else {
     //  stDebug("s-task:0x%x task state is NULL, may del backend:%s", pTask->id.taskId,
     //          pTask->backendPath ? pTask->backendPath : "NULL");
     //  if (remove) {
     //    if (pTask->backendPath != NULL) {
     //      stDebug("s-task:0x%x task state is NULL, do del backend:%s", pTask->id.taskId, pTask->backendPath);
     //      taosRemoveDir(pTask->backendPath);
     //    }
     //  }
  //}
  // if (pTask->backendPath != NULL) {
  //   taosMemoryFree(pTask->backendPath);
  //   pTask->backendPath = NULL;
  // }
}

static void setInitialVersionInfo(SStreamTask* pTask, int64_t ver) {
  SCheckpointInfo* pChkInfo = &pTask->chkInfo;
  SDataRange*      pRange = &pTask->dataRange;

  // only set the version info for stream tasks without fill-history task
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

      {  // for compatible purpose, remove it later
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

int32_t streamTaskSetBackendPath(SStreamTask* pTask) {
  int64_t streamId = 0;
  int32_t taskId = 0;

  if (pTask->info.fillHistory) {
    streamId = pTask->streamTaskId.streamId;
    taskId = pTask->streamTaskId.taskId;
  } else {
    streamId = pTask->id.streamId;
    taskId = pTask->id.taskId;
  }

  char    id[128] = {0};
  int32_t nBytes = snprintf(id, tListLen(id), "0x%" PRIx64 "-0x%x", streamId, taskId);
  if (nBytes < 0 || nBytes >= sizeof(id)) {
    return TSDB_CODE_OUT_OF_BUFFER;
  }

  int32_t len = strlen(pTask->pMeta->path);
  pTask->backendPath = (char*)taosMemoryMalloc(len + nBytes + 2);
  if (pTask->backendPath == NULL) {
    return terrno;
  }

  int32_t code = snprintf(pTask->backendPath, len + nBytes + 2, "%s%s%s", pTask->pMeta->path, TD_DIRSEP, id);
  if (code < 0 || code >= len + nBytes + 2) {
    stError("s-task:%s failed to set backend path:%s, code: out of buffer", pTask->id.idStr, pTask->backendPath);
    return TSDB_CODE_OUT_OF_BUFFER;
  } else {
    stDebug("s-task:%s set backend path:%s", pTask->id.idStr, pTask->backendPath);
    return 0;
  }
}

int32_t streamTaskInit(SStreamTask* pTask, SStreamMeta* pMeta, SMsgCb* pMsgCb, int64_t ver) {
  int32_t code = createStreamTaskIdStr(pTask->id.streamId, pTask->id.taskId, &pTask->id.idStr);
  if (code) {
    stError("0x%x failed create stream task id str, code:%s", pTask->id.taskId, tstrerror(code));
    return code;
  }

  pTask->id.refId = 0;
  pTask->inputq.status = TASK_INPUT_STATUS__NORMAL;
  pTask->outputq.status = TASK_OUTPUT_STATUS__NORMAL;

  int32_t code1 = streamQueueOpen(512 << 10, &pTask->inputq.queue);
  int32_t code2 = streamQueueOpen(512 << 10, &pTask->outputq.queue);
  if (code1 || code2) {
    stError("s-task:%s failed to prepare the input/output queue, initialize task failed", pTask->id.idStr);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pTask->status.schedStatus = TASK_SCHED_STATUS__INACTIVE;

  code = streamCreateStateMachine(pTask);
  if (pTask->status.pSM == NULL || code != TSDB_CODE_SUCCESS) {
    stError("s-task:%s failed create state-machine for stream task, initialization failed, code:%s", pTask->id.idStr,
            tstrerror(code));
    return code;
  }

  pTask->execInfo.created = taosGetTimestampMs();
  setInitialVersionInfo(pTask, ver);

  pTask->pMeta = pMeta;
  pTask->pMsgCb = pMsgCb;
  pTask->msgInfo.pSendInfo = taosArrayInit(4, sizeof(SDispatchEntry));
  if (pTask->msgInfo.pSendInfo == NULL) {
    stError("s-task:%s failed to create sendInfo struct for stream task, code:Out of memory", pTask->id.idStr);
    return terrno;
  }

  code = taosThreadMutexInit(&pTask->msgInfo.lock, NULL);
  if (code) {
    stError("s-task:0x%x failed to init msgInfo mutex, code:%s", pTask->id.taskId, tstrerror(code));
    return code;
  }

  TdThreadMutexAttr attr = {0};
  code = taosThreadMutexAttrInit(&attr);
  if (code != 0) {
    stError("s-task:%s initElapsed mutex attr failed, code:%s", pTask->id.idStr, tstrerror(code));
    return code;
  }

  code = taosThreadMutexAttrSetType(&attr, PTHREAD_MUTEX_RECURSIVE);
  if (code != 0) {
    stError("s-task:%s set mutex attr recursive, code:%s", pTask->id.idStr, tstrerror(code));
    return code;
  }

  code = taosThreadMutexInit(&pTask->lock, &attr);
  if (code) {
    return code;
  }

  code = taosThreadMutexAttrDestroy(&attr);
  if (code) {
    return code;
  }

  streamTaskOpenAllUpstreamInput(pTask);

  STaskOutputInfo* pOutputInfo = &pTask->outputInfo;
  pOutputInfo->pTokenBucket = taosMemoryCalloc(1, sizeof(STokenBucket));
  if (pOutputInfo->pTokenBucket == NULL) {
    stError("s-task:%s failed to prepare the tokenBucket, code:%s", pTask->id.idStr, tstrerror(terrno));
    return terrno;
  }

  // 2MiB per second for sink task
  // 50 times sink operator per second
  code = streamTaskInitTokenBucket(pOutputInfo->pTokenBucket, 35, 35, tsSinkDataRate, pTask->id.idStr);
  if (code) {
    return code;
  }

  pOutputInfo->pNodeEpsetUpdateList = taosArrayInit(4, sizeof(SDownstreamTaskEpset));
  if (pOutputInfo->pNodeEpsetUpdateList == NULL) {
    stError("s-task:%s failed to prepare downstreamUpdateList, code:%s", pTask->id.idStr, tstrerror(terrno));
    return terrno;
  }

  pTask->taskCheckInfo.pList = taosArrayInit(4, sizeof(SDownstreamStatusInfo));
  if (pTask->taskCheckInfo.pList == NULL) {
    stError("s-task:%s failed to prepare taskCheckInfo list, code:%s", pTask->id.idStr, tstrerror(terrno));
    return terrno;
  }

  code = taosThreadMutexInit(&pTask->taskCheckInfo.checkInfoLock, NULL);
  if (code) {
    return code;
  }

  if (pTask->chkInfo.pActiveInfo == NULL) {
    code = streamTaskCreateActiveChkptInfo(&pTask->chkInfo.pActiveInfo);
    if (code) {
      stError("s-task:%s failed to create active checkpoint info, code:%s", pTask->id.idStr, tstrerror(code));
      return code;
    }
  }

  return streamTaskSetBackendPath(pTask);
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
  } else if (type == TASK_OUTPUT__VTABLE_MAP) {
    SArray* pTaskInfos = pTask->outputInfo.vtableMapDispatcher.taskInfos;
    return taosArrayGetSize(pTaskInfos);
  } else {
    SArray* vgInfo = pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos;
    return taosArrayGetSize(vgInfo);
  }
}

int32_t streamTaskGetNumOfUpstream(const SStreamTask* pTask) { return taosArrayGetSize(pTask->upstreamInfo.pList); }

int32_t streamTaskSetUpstreamInfo(SStreamTask* pTask, const SStreamTask* pUpstreamTask) {
  SStreamUpstreamEpInfo* pEpInfo = createStreamTaskEpInfo(pUpstreamTask);
  if (pEpInfo == NULL) {
    return terrno;
  }

  if (pTask->upstreamInfo.pList == NULL) {
    pTask->upstreamInfo.pList = taosArrayInit(4, POINTER_BYTES);
  }

  void* p = taosArrayPush(pTask->upstreamInfo.pList, &pEpInfo);
  return (p == NULL) ? terrno : TSDB_CODE_SUCCESS;
}

int32_t streamTaskUpdateUpstreamInfo(SStreamTask* pTask, int32_t nodeId, const SEpSet* pEpSet, bool* pUpdated) {
  int32_t code = 0;
  char    buf[512] = {0};
  code = epsetToStr(pEpSet, buf, tListLen(buf));  // ignore error since it is only for log file.
  if (code != 0) {                                // print error and continue
    stError("%s failed to convert epset to str, code:%s", pTask->id.idStr, tstrerror(code));
    return code;
  }

  int32_t numOfUpstream = taosArrayGetSize(pTask->upstreamInfo.pList);
  for (int32_t i = 0; i < numOfUpstream; ++i) {
    SStreamUpstreamEpInfo* pInfo = taosArrayGetP(pTask->upstreamInfo.pList, i);
    if (pInfo->nodeId == nodeId) {
      bool equal = isEpsetEqual(&pInfo->epSet, pEpSet);
      if (!equal) {
        *pUpdated = true;

        char tmp[512] = {0};
        code = epsetToStr(&pInfo->epSet, tmp, tListLen(tmp));
        if (code != 0) {  // print error and continue
          stError("%s failed to convert epset to str, code:%s", pTask->id.idStr, tstrerror(code));
          return code;
        }

        epsetAssign(&pInfo->epSet, pEpSet);
        stDebug("s-task:0x%x update the upstreamInfo taskId:0x%x(nodeId:%d) newEpset:%s old:%s", pTask->id.taskId,
                pInfo->taskId, nodeId, buf, tmp);
      } else {
        stDebug("s-task:0x%x not update upstreamInfo, since identical, task:0x%x(nodeId:%d) epset:%s", pTask->id.taskId,
                pInfo->taskId, nodeId, buf);
      }

      break;
    }
  }

  return code;
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

int32_t streamTaskUpdateDownstreamInfo(SStreamTask* pTask, int32_t nodeId, const SEpSet* pEpSet, bool* pUpdated) {
  char    buf[512] = {0};
  int32_t code = epsetToStr(pEpSet, buf, tListLen(buf));  // ignore the error since only for log files.
  if (code != 0) {                                        // print error and continue
    stError("%s failed to convert epset to str, code:%s", pTask->id.idStr, tstrerror(code));
    return code;
  }

  int32_t id = pTask->id.taskId;
  int8_t  type = pTask->outputInfo.type;

  if (type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    SArray* pVgs = pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos;

    for (int32_t i = 0; i < taosArrayGetSize(pVgs); i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(pVgs, i);
      if (pVgInfo == NULL) {
        continue;
      }

      if (pVgInfo->vgId == nodeId) {
        bool isEqual = isEpsetEqual(&pVgInfo->epSet, pEpSet);
        if (!isEqual) {
          *pUpdated = true;

          char tmp[512] = {0};
          code = epsetToStr(&pVgInfo->epSet, tmp, tListLen(tmp));
          if (code != 0) {  // print error and continue
            stError("%s failed to convert epset to str, code:%s", pTask->id.idStr, tstrerror(code));
            return code;
          }

          epsetAssign(&pVgInfo->epSet, pEpSet);
          stDebug("s-task:0x%x update dispatch info, task:0x%x(nodeId:%d) newEpset:%s old:%s", id, pVgInfo->taskId,
                  nodeId, buf, tmp);
        } else {
          stDebug("s-task:0x%x not update dispatch info, since identical, task:0x%x(nodeId:%d) epset:%s", id,
                  pVgInfo->taskId, nodeId, buf);
        }
        break;
      }
    }
  } else if (type == TASK_OUTPUT__FIXED_DISPATCH) {
    STaskDispatcherFixed* pDispatcher = &pTask->outputInfo.fixedDispatcher;
    if (pDispatcher->nodeId == nodeId) {
      bool equal = isEpsetEqual(&pDispatcher->epSet, pEpSet);
      if (!equal) {
        *pUpdated = true;

        char tmp[512] = {0};
        code = epsetToStr(&pDispatcher->epSet, tmp, tListLen(tmp));
        if (code != 0) {  // print error and continue
          stError("%s failed to convert epset to str, code:%s", pTask->id.idStr, tstrerror(code));
          return code;
        }

        epsetAssign(&pDispatcher->epSet, pEpSet);
        stDebug("s-task:0x%x update dispatch info, task:0x%x(nodeId:%d) newEpset:%s old:%s", id, pDispatcher->taskId,
                nodeId, buf, tmp);
      } else {
        stDebug("s-task:0x%x not update dispatch info, since identical, task:0x%x(nodeId:%d) epset:%s", id,
                pDispatcher->taskId, nodeId, buf);
      }
    }
  } else if (type == TASK_OUTPUT__VTABLE_MAP) {
    SArray* pTaskInfos = pTask->outputInfo.vtableMapDispatcher.taskInfos;

    for (int32_t i = 0; i < taosArrayGetSize(pTaskInfos); ++i) {
      STaskDispatcherFixed* pAddr = taosArrayGet(pTaskInfos, i);
      if (pAddr == NULL) {
        continue;
      }

      if (pAddr->nodeId == nodeId) {
        bool isEqual = isEpsetEqual(&pAddr->epSet, pEpSet);
        if (!isEqual) {
          *pUpdated = true;

          char tmp[512] = {0};
          code = epsetToStr(&pAddr->epSet, tmp, tListLen(tmp));
          if (code != 0) {  // print error and continue
            stError("%s failed to convert epset to str, code:%s", pTask->id.idStr, tstrerror(code));
            return code;
          }

          epsetAssign(&pAddr->epSet, pEpSet);
          stDebug("s-task:0x%x update dispatch info, task:0x%x(nodeId:%d) newEpset:%s old:%s", id, pAddr->taskId,
                  nodeId, buf, tmp);
        } else {
          stDebug("s-task:0x%x not update dispatch info, since identical, task:0x%x(nodeId:%d) epset:%s", id,
                  pAddr->taskId, nodeId, buf);
        }
        break;
      }
    }
  }

  return code;
}

int32_t streamTaskStop(SStreamTask* pTask) {
  int32_t     vgId = pTask->pMeta->vgId;
  int64_t     st = taosGetTimestampMs();
  const char* id = pTask->id.idStr;

  int32_t code = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_STOP);
  if (code) {
    stError("failed to handle STOP event, s-task:%s, code:%s", id, tstrerror(code));
    return code;
  }

  if (pTask->info.taskLevel != TASK_LEVEL__SINK && pTask->exec.pExecutor != NULL) {
    code = qKillTask(pTask->exec.pExecutor, TSDB_CODE_SUCCESS, 5000);
    if (code != TSDB_CODE_SUCCESS) {
      stError("s-task:%s failed to kill task related query handle, code:%s", id, tstrerror(code));
    }
  }

  while (!streamTaskIsIdle(pTask)) {
    stDebug("s-task:%s level:%d wait for task to be idle and then close, check again in 100ms", id,
            pTask->info.taskLevel);
    taosMsleep(100);
  }

  int64_t el = taosGetTimestampMs() - st;
  stDebug("vgId:%d s-task:%s is closed in %" PRId64 " ms", vgId, id, el);
  return code;
}

bool streamTaskUpdateEpsetInfo(SStreamTask* pTask, SArray* pNodeList) {
  STaskExecStatisInfo* p = &pTask->execInfo;

  int32_t numOfNodes = taosArrayGetSize(pNodeList);
  int64_t prevTs = p->latestUpdateTs;

  p->latestUpdateTs = taosGetTimestampMs();
  p->updateCount += 1;
  stDebug("s-task:0x%x update task nodeEp epset, updatedNodes:%d, updateCount:%d, prevTs:%" PRId64, pTask->id.taskId,
          numOfNodes, p->updateCount, prevTs);

  bool updated = false;
  for (int32_t i = 0; i < numOfNodes; ++i) {
    SNodeUpdateInfo* pInfo = taosArrayGet(pNodeList, i);
    if (pInfo == NULL) {
      continue;
    }

    int32_t code = doUpdateTaskEpset(pTask, pInfo->nodeId, &pInfo->newEp, &updated);
    if (code) {
      stError("s-task:0x%x failed to update the task nodeEp epset, code:%s", pTask->id.taskId, tstrerror(code));
    }
  }

  return updated;
}

void streamTaskResetUpstreamStageInfo(SStreamTask* pTask) {
  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    return;
  }

  int32_t size = taosArrayGetSize(pTask->upstreamInfo.pList);
  for (int32_t i = 0; i < size; ++i) {
    SStreamUpstreamEpInfo* pInfo = taosArrayGetP(pTask->upstreamInfo.pList, i);
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
    SStreamUpstreamEpInfo* pInfo = taosArrayGetP(pTask->upstreamInfo.pList, i);
    pInfo->dataAllowed = true;
  }

  pTask->upstreamInfo.numOfClosed = 0;
  stDebug("s-task:%s opening up inputQ for %d upstream tasks", pTask->id.idStr, num);
}

void streamTaskCloseUpstreamInput(SStreamTask* pTask, int32_t taskId) {
  SStreamUpstreamEpInfo* pInfo = NULL;
  streamTaskGetUpstreamTaskEpInfo(pTask, taskId, &pInfo);

  if ((pInfo != NULL) && pInfo->dataAllowed) {
    pInfo->dataAllowed = false;
    if (pTask->upstreamInfo.numOfClosed < streamTaskGetNumOfUpstream(pTask)) {
      int32_t t = atomic_add_fetch_32(&pTask->upstreamInfo.numOfClosed, 1);
    } else {
      stError("s-task:%s not inc closed input, since they have been all closed already", pTask->id.idStr);
    }
  }
}

void streamTaskOpenUpstreamInput(SStreamTask* pTask, int32_t taskId) {
  SStreamUpstreamEpInfo* pInfo = NULL;
  streamTaskGetUpstreamTaskEpInfo(pTask, taskId, &pInfo);

  if (pInfo != NULL && (!pInfo->dataAllowed)) {
    int32_t t = atomic_sub_fetch_32(&pTask->upstreamInfo.numOfClosed, 1);
    stDebug("s-task:%s open inputQ for upstream:0x%x, remain closed:%d", pTask->id.idStr, taskId, t);
    pInfo->dataAllowed = true;
  }
}

bool streamTaskIsAllUpstreamClosed(SStreamTask* pTask) {
  return pTask->upstreamInfo.numOfClosed == taosArrayGetSize(pTask->upstreamInfo.pList);
}

bool streamTaskSetSchedStatusWait(SStreamTask* pTask) {
  bool ret = false;

  streamMutexLock(&pTask->lock);
  if (pTask->status.schedStatus == TASK_SCHED_STATUS__INACTIVE) {
    pTask->status.schedStatus = TASK_SCHED_STATUS__WAITING;
    ret = true;
  }

  streamMutexUnlock(&pTask->lock);
  return ret;
}

int8_t streamTaskSetSchedStatusActive(SStreamTask* pTask) {
  streamMutexLock(&pTask->lock);
  int8_t status = pTask->status.schedStatus;
  if (status == TASK_SCHED_STATUS__WAITING) {
    pTask->status.schedStatus = TASK_SCHED_STATUS__ACTIVE;
  }
  streamMutexUnlock(&pTask->lock);

  return status;
}

int8_t streamTaskSetSchedStatusInactive(SStreamTask* pTask) {
  streamMutexLock(&pTask->lock);
  int8_t status = pTask->status.schedStatus;
  pTask->status.schedStatus = TASK_SCHED_STATUS__INACTIVE;
  streamMutexUnlock(&pTask->lock);

  return status;
}

int32_t streamTaskClearHTaskAttr(SStreamTask* pTask, int32_t resetRelHalt) {
  int32_t      code = 0;
  SStreamMeta* pMeta = pTask->pMeta;
  SStreamTask* pStreamTask = NULL;

  if (pTask->info.fillHistory == 0) {
    return code;
  }

  code = streamMetaAcquireTaskUnsafe(pMeta, &pTask->streamTaskId, &pStreamTask);
  if (code == 0) {
    stDebug("s-task:%s clear the related stream task:0x%x attr to fill-history task", pTask->id.idStr,
            (int32_t)pTask->streamTaskId.taskId);

    streamMutexLock(&(pStreamTask->lock));
    CLEAR_RELATED_FILLHISTORY_TASK(pStreamTask);

    if (resetRelHalt) {
      stDebug("s-task:0x%" PRIx64 " set the persistent status attr to be ready, prev:%s, status in sm:%s",
              pTask->streamTaskId.taskId, streamTaskGetStatusStr(pStreamTask->status.taskStatus),
              streamTaskGetStatus(pStreamTask).name);
      pStreamTask->status.taskStatus = TASK_STATUS__READY;
    }

    code = streamMetaSaveTaskInMeta(pMeta, pStreamTask);
    streamMutexUnlock(&(pStreamTask->lock));

    streamMetaReleaseTask(pMeta, pStreamTask);
  }

  return code;
}

int32_t streamBuildAndSendDropTaskMsg(SMsgCb* pMsgCb, int32_t vgId, SStreamTaskId* pTaskId, int64_t resetRelHalt) {
  SVDropStreamTaskReq* pReq = rpcMallocCont(sizeof(SVDropStreamTaskReq));
  if (pReq == NULL) {
    return terrno;
  }

  pReq->head.vgId = vgId;
  pReq->taskId = pTaskId->taskId;
  pReq->streamId = pTaskId->streamId;
  pReq->resetRelHalt = resetRelHalt;

  SRpcMsg msg = {.msgType = TDMT_STREAM_TASK_DROP, .pCont = pReq, .contLen = sizeof(SVDropStreamTaskReq)};
  int32_t code = tmsgPutToQueue(pMsgCb, WRITE_QUEUE, &msg);
  if (code != TSDB_CODE_SUCCESS) {
    stError("vgId:%d failed to send drop task:0x%x msg, code:%s", vgId, pTaskId->taskId, tstrerror(code));
  } else {
    stDebug("vgId:%d build and send drop task:0x%x msg", vgId, pTaskId->taskId);
  }

  return code;
}

int32_t streamSendChkptReportMsg(SStreamTask* pTask, SCheckpointInfo* pCheckpointInfo, int8_t dropRelHTask) {
  int32_t                code = 0;
  int32_t                tlen = 0;
  int32_t                vgId = pTask->pMeta->vgId;
  const char*            id = pTask->id.idStr;
  SActiveCheckpointInfo* pActive = pCheckpointInfo->pActiveInfo;

  SCheckpointReport req = {.streamId = pTask->id.streamId,
                           .taskId = pTask->id.taskId,
                           .nodeId = vgId,
                           .dropHTask = dropRelHTask,
                           .transId = pActive->transId,
                           .checkpointId = pActive->activeId,
                           .checkpointVer = pCheckpointInfo->processedVer,
                           .checkpointTs = pCheckpointInfo->startTs};

  tEncodeSize(tEncodeStreamTaskChkptReport, &req, tlen, code);
  if (code < 0) {
    stError("s-task:%s vgId:%d encode stream task checkpoint-report failed, code:%s", id, vgId, tstrerror(code));
    return -1;
  }

  void* buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    stError("s-task:%s vgId:%d encode stream task checkpoint-report msg failed, code:%s", id, vgId,
            tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    return -1;
  }

  SEncoder encoder;
  tEncoderInit(&encoder, buf, tlen);
  if ((code = tEncodeStreamTaskChkptReport(&encoder, &req)) < 0) {
    rpcFreeCont(buf);
    tEncoderClear(&encoder);
    stError("s-task:%s vgId:%d encode stream task checkpoint-report msg failed, code:%s", id, vgId, tstrerror(code));
    return -1;
  }
  tEncoderClear(&encoder);

  SRpcMsg msg = {0};
  initRpcMsg(&msg, TDMT_MND_STREAM_CHKPT_REPORT, buf, tlen);
  stDebug("s-task:%s vgId:%d build and send task checkpoint-report to mnode", id, vgId);

  return tmsgSendReq(&pTask->info.mnodeEpset, &msg);
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
  pDst->procsTotal = pSrc->procsTotal;
  pDst->procsThroughput = pSrc->procsThroughput;
  pDst->outputTotal = pSrc->outputTotal;
  pDst->outputThroughput = pSrc->outputThroughput;
  pDst->processedVer = pSrc->processedVer;
  pDst->verRange = pSrc->verRange;
  pDst->sinkQuota = pSrc->sinkQuota;
  pDst->sinkDataSize = pSrc->sinkDataSize;
  pDst->checkpointInfo = pSrc->checkpointInfo;
  pDst->startCheckpointId = pSrc->startCheckpointId;
  pDst->startCheckpointVer = pSrc->startCheckpointVer;
  pDst->status = pSrc->status;

  pDst->startTime = pSrc->startTime;
  pDst->hTaskId = pSrc->hTaskId;
  pDst->notifyEventStat = pSrc->notifyEventStat;
}

STaskStatusEntry streamTaskGetStatusEntry(SStreamTask* pTask) {
  SStreamMeta*         pMeta = pTask->pMeta;
  STaskExecStatisInfo* pExecInfo = &pTask->execInfo;

  STaskStatusEntry entry = {
      .id = streamTaskGetTaskId(pTask),
      .status = streamTaskGetStatus(pTask).state,
      .nodeId = pMeta->vgId,
      .stage = pMeta->stage,

      .inputQUsed = SIZE_IN_MiB(streamQueueGetItemSize(pTask->inputq.queue)),
      .startTime = pExecInfo->readyTs,
      .checkpointInfo.latestId = pTask->chkInfo.checkpointId,
      .checkpointInfo.latestVer = pTask->chkInfo.checkpointVer,
      .checkpointInfo.latestTime = pTask->chkInfo.checkpointTime,
      .checkpointInfo.latestSize = 0,
      .checkpointInfo.remoteBackup = 0,
      .checkpointInfo.consensusChkptId = 0,
      .checkpointInfo.consensusTs = 0,
      .hTaskId = pTask->hTaskInfo.id.taskId,
      .procsTotal = SIZE_IN_MiB(pExecInfo->inputDataSize),
      .outputTotal = SIZE_IN_MiB(pExecInfo->outputDataSize),
      .procsThroughput = SIZE_IN_KiB(pExecInfo->procsThroughput),
      .outputThroughput = SIZE_IN_KiB(pExecInfo->outputThroughput),
      .startCheckpointId = pExecInfo->startCheckpointId,
      .startCheckpointVer = pExecInfo->startCheckpointVer,
      .notifyEventStat = pTask->notifyEventStat,
  };
  return entry;
}

static int32_t taskPauseCallback(SStreamTask* pTask, void* param) {
  SStreamMeta* pMeta = pTask->pMeta;
  int32_t      code = 0;

  int32_t num = atomic_add_fetch_32(&pMeta->numOfPausedTasks, 1);
  stInfo("vgId:%d s-task:%s pause stream task. paused task num:%d", pMeta->vgId, pTask->id.idStr, num);

  // in case of fill-history task, stop the tsdb file scan operation.
  if (pTask->info.fillHistory == 1) {
    void* pExecutor = pTask->exec.pExecutor;
    code = qKillTask(pExecutor, TSDB_CODE_SUCCESS, 10000);
  }

  stDebug("vgId:%d s-task:%s set pause flag and pause task", pMeta->vgId, pTask->id.idStr);
  return code;
}

void streamTaskPause(SStreamTask* pTask) {
  int32_t code = streamTaskHandleEventAsync(pTask->status.pSM, TASK_EVENT_PAUSE, taskPauseCallback, NULL);
  if (code) {
    stError("s-task:%s failed handle pause event async, code:%s", pTask->id.idStr, tstrerror(code));
  }
}

void streamTaskResume(SStreamTask* pTask) {
  SStreamTaskState prevState = streamTaskGetStatus(pTask);

  SStreamMeta* pMeta = pTask->pMeta;
  int32_t      code = streamTaskRestoreStatus(pTask);
  if (code == TSDB_CODE_SUCCESS) {
    char*   pNew = streamTaskGetStatus(pTask).name;
    int32_t num = atomic_sub_fetch_32(&pMeta->numOfPausedTasks, 1);
    stInfo("s-task:%s status:%s resume from %s, paused task(s):%d", pTask->id.idStr, pNew, prevState.name, num);
  } else {
    stInfo("s-task:%s status:%s no need to resume, paused task(s):%d", pTask->id.idStr, prevState.name,
           pMeta->numOfPausedTasks);
  }
}

bool streamTaskIsSinkTask(const SStreamTask* pTask) { return pTask->info.taskLevel == TASK_LEVEL__SINK; }

// this task must success
int32_t streamTaskSendCheckpointReq(SStreamTask* pTask) {
  int32_t     code;
  int32_t     tlen = 0;
  int32_t     vgId = pTask->pMeta->vgId;
  const char* id = pTask->id.idStr;

  SStreamTaskCheckpointReq req = {.streamId = pTask->id.streamId, .taskId = pTask->id.taskId, .nodeId = vgId};
  tEncodeSize(tEncodeStreamTaskCheckpointReq, &req, tlen, code);
  if (code < 0) {
    stError("s-task:%s vgId:%d encode stream task req checkpoint failed, code:%s", id, vgId, tstrerror(code));
    return TSDB_CODE_INVALID_MSG;
  }

  void* buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    stError("s-task:%s vgId:%d encode stream task req checkpoint msg failed, code:Out of memory", id, vgId);
    return terrno;
  }

  SEncoder encoder;
  tEncoderInit(&encoder, buf, tlen);
  if ((code = tEncodeStreamTaskCheckpointReq(&encoder, &req)) < 0) {
    rpcFreeCont(buf);
    tEncoderClear(&encoder);
    stError("s-task:%s vgId:%d encode stream task req checkpoint msg failed, code:%s", id, vgId, tstrerror(code));
    return code;
  }

  tEncoderClear(&encoder);

  SRpcMsg msg = {0};
  initRpcMsg(&msg, TDMT_MND_STREAM_REQ_CHKPT, buf, tlen);
  stDebug("s-task:%s vgId:%d build and send task checkpoint req", id, vgId);

  return tmsgSendReq(&pTask->info.mnodeEpset, &msg);
}

void streamTaskGetUpstreamTaskEpInfo(SStreamTask* pTask, int32_t taskId, SStreamUpstreamEpInfo** pEpInfo) {
  *pEpInfo = NULL;

  int32_t num = taosArrayGetSize(pTask->upstreamInfo.pList);
  for (int32_t i = 0; i < num; ++i) {
    SStreamUpstreamEpInfo* pInfo = taosArrayGetP(pTask->upstreamInfo.pList, i);
    if (pInfo == NULL) {
      return;
    }

    if (pInfo->taskId == taskId) {
      *pEpInfo = pInfo;
      return;
    }
  }

  stError("s-task:%s failed to find upstream task:0x%x", pTask->id.idStr, taskId);
}

SEpSet* streamTaskGetDownstreamEpInfo(SStreamTask* pTask, int32_t taskId) {
  if (pTask->info.taskLevel == TASK_OUTPUT__FIXED_DISPATCH) {
    if (pTask->outputInfo.fixedDispatcher.taskId == taskId) {
      return &pTask->outputInfo.fixedDispatcher.epSet;
    }
  } else if (pTask->info.taskLevel == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    SArray* pList = pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos;
    for (int32_t i = 0; i < taosArrayGetSize(pList); ++i) {
      SVgroupInfo* pVgInfo = taosArrayGet(pList, i);
      if (pVgInfo == NULL) {
        continue;
      }

      if (pVgInfo->taskId == taskId) {
        return &pVgInfo->epSet;
      }
    }
  } else if (pTask->info.taskLevel == TASK_OUTPUT__VTABLE_MAP) {
    SArray* pTaskInfos = pTask->outputInfo.vtableMapDispatcher.taskInfos;
    for (int32_t i = 0; i < taosArrayGetSize(pTaskInfos); ++i) {
      STaskDispatcherFixed* pAddr = taosArrayGet(pTaskInfos, i);
      if (pAddr == NULL) {
        continue;
      }

      if (pAddr->taskId == taskId) {
        return &pAddr->epSet;
      }
    }
  }

  return NULL;
}

int32_t createStreamTaskIdStr(int64_t streamId, int32_t taskId, const char** pId) {
  char    buf[128] = {0};
  int32_t code = snprintf(buf, tListLen(buf), "0x%" PRIx64 "-0x%x", streamId, taskId);
  if (code < 0 || code >= tListLen(buf)) {
    return TSDB_CODE_OUT_OF_BUFFER;
  }

  *pId = taosStrdup(buf);

  if (*pId == NULL) {
    return terrno;
  } else {
    return TSDB_CODE_SUCCESS;
  }
}

static int32_t streamTaskEnqueueRetrieve(SStreamTask* pTask, SStreamRetrieveReq* pReq) {
  int32_t           code;
  SStreamDataBlock* pData;

  code = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, sizeof(SStreamDataBlock), (void**)&pData);
  if (code) {
    stError("s-task:%s failed to allocated retrieve-block", pTask->id.idStr);
    return terrno = code;
  }

  pData->type = STREAM_INPUT__DATA_RETRIEVE;
  pData->srcVgId = 0;

  code = streamRetrieveReqToData(pReq, pData, pTask->id.idStr);
  if (code != TSDB_CODE_SUCCESS) {
    stError("s-task:%s failed to convert retrieve-data to block, code:%s", pTask->id.idStr, tstrerror(code));
    taosFreeQitem(pData);
    return code;
  }

  code = streamTaskPutDataIntoInputQ(pTask, (SStreamQueueItem*)pData);
  if (code != TSDB_CODE_SUCCESS) {
    stError("s-task:%s failed to put retrieve-block into inputQ, inputQ is full, discard the retrieve msg",
            pTask->id.idStr);
  }

  return code;
}

int32_t streamProcessRetrieveReq(SStreamTask* pTask, SStreamRetrieveReq* pReq) {
  int32_t code = streamTaskEnqueueRetrieve(pTask, pReq);
  if (code != 0) {
    return code;
  }
  return streamTrySchedExec(pTask, false);
}

void streamTaskSetRemoveBackendFiles(SStreamTask* pTask) { pTask->status.removeBackendFiles = true; }

void streamTaskGetActiveCheckpointInfo(const SStreamTask* pTask, int32_t* pTransId, int64_t* pCheckpointId) {
  if (pTransId != NULL) {
    *pTransId = pTask->chkInfo.pActiveInfo->transId;
  }

  if (pCheckpointId != NULL) {
    *pCheckpointId = pTask->chkInfo.pActiveInfo->activeId;
  }
}

int32_t streamTaskSetActiveCheckpointInfo(SStreamTask* pTask, int64_t activeCheckpointId) {
  pTask->chkInfo.pActiveInfo->activeId = activeCheckpointId;
  return TSDB_CODE_SUCCESS;
}

void streamTaskSetFailedChkptInfo(SStreamTask* pTask, int32_t transId, int64_t checkpointId) {
  pTask->chkInfo.pActiveInfo->transId = transId;
  pTask->chkInfo.pActiveInfo->activeId = checkpointId;
  pTask->chkInfo.pActiveInfo->failedId = checkpointId;
  stDebug("s-task:%s set failed checkpointId:%" PRId64, pTask->id.idStr, checkpointId);
}

int32_t streamTaskCreateActiveChkptInfo(SActiveCheckpointInfo** pRes) {
  SActiveCheckpointInfo* pInfo = taosMemoryCalloc(1, sizeof(SActiveCheckpointInfo));
  if (pInfo == NULL) {
    return terrno;
  }

  int32_t code = taosThreadMutexInit(&pInfo->lock, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pInfo->pDispatchTriggerList = taosArrayInit(4, sizeof(STaskTriggerSendInfo));
  pInfo->pReadyMsgList = taosArrayInit(4, sizeof(STaskCheckpointReadyInfo));
  pInfo->pCheckpointReadyRecvList = taosArrayInit(4, sizeof(STaskDownstreamReadyInfo));

  *pRes = pInfo;
  return code;
}

void streamTaskDestroyActiveChkptInfo(SActiveCheckpointInfo* pInfo) {
  if (pInfo == NULL) {
    return;
  }

  streamMutexDestroy(&pInfo->lock);
  taosArrayDestroy(pInfo->pDispatchTriggerList);
  pInfo->pDispatchTriggerList = NULL;
  taosArrayDestroy(pInfo->pReadyMsgList);
  pInfo->pReadyMsgList = NULL;
  taosArrayDestroy(pInfo->pCheckpointReadyRecvList);
  pInfo->pCheckpointReadyRecvList = NULL;

  SStreamTmrInfo* pTriggerTmr = &pInfo->chkptTriggerMsgTmr;
  if (pTriggerTmr->tmrHandle != NULL) {
    streamTmrStop(pTriggerTmr->tmrHandle);
    pTriggerTmr->tmrHandle = NULL;
  }

  SStreamTmrInfo* pReadyTmr = &pInfo->chkptReadyMsgTmr;
  if (pReadyTmr->tmrHandle != NULL) {
    streamTmrStop(pReadyTmr->tmrHandle);
    pReadyTmr->tmrHandle = NULL;
  }

  taosMemoryFree(pInfo);
}

// NOTE: clear the checkpoint id, and keep the failed id
// failedId for a task will increase as the checkpoint I.D. increases.
void streamTaskClearActiveInfo(SActiveCheckpointInfo* pInfo) {
  pInfo->activeId = 0;
  pInfo->transId = 0;
  pInfo->allUpstreamTriggerRecv = 0;
  pInfo->dispatchTrigger = false;

  taosArrayClear(pInfo->pDispatchTriggerList);
  taosArrayClear(pInfo->pCheckpointReadyRecvList);
}

const char* streamTaskGetExecType(int32_t type) {
  switch (type) {
    case STREAM_EXEC_T_EXTRACT_WAL_DATA:
      return "scan-wal-file";
    case STREAM_EXEC_T_START_ALL_TASKS:
      return "start-all-tasks";
    case STREAM_EXEC_T_START_ONE_TASK:
      return "start-one-task";
    case STREAM_EXEC_T_RESTART_ALL_TASKS:
      return "restart-all-tasks";
    case STREAM_EXEC_T_STOP_ALL_TASKS:
      return "stop-all-tasks";
    case STREAM_EXEC_T_RESUME_TASK:
      return "resume-task-from-idle";
    case STREAM_EXEC_T_ADD_FAILED_TASK:
      return "record-start-failed-task";
    case STREAM_EXEC_T_STOP_ONE_TASK:
      return "stop-one-task";
    case 0:
      return "exec-all-tasks";
    default:
      return "invalid-exec-type";
  }
}

int32_t streamTaskAllocRefId(SStreamTask* pTask, int64_t** pRefId) {
  *pRefId = taosMemoryMalloc(sizeof(int64_t));
  if (*pRefId != NULL) {
    **pRefId = pTask->id.refId;
    int32_t code = metaRefMgtAdd(pTask->pMeta->vgId, *pRefId);
    if (code != 0) {
      stError("s-task:%s failed to add refId:%" PRId64 " into refId-mgmt, code:%s", pTask->id.idStr, pTask->id.refId,
              tstrerror(code));
    }
    return code;
  } else {
    stError("s-task:%s failed to alloc new ref id, code:%s", pTask->id.idStr, tstrerror(terrno));
    return terrno;
  }
}

void streamTaskFreeRefId(int64_t* pRefId) {
  if (pRefId == NULL) {
    return;
  }

  metaRefMgtRemove(pRefId);
}

static int32_t tEncodeStreamNotifyInfo(SEncoder* pEncoder, const SNotifyInfo* info) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  QUERY_CHECK_NULL(pEncoder, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(info, code, lino, _exit, TSDB_CODE_INVALID_PARA);

  int32_t addrSize = taosArrayGetSize(info->pNotifyAddrUrls);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, addrSize));
  for (int32_t i = 0; i < addrSize; ++i) {
    const char* url = taosArrayGetP(info->pNotifyAddrUrls, i);
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, url));
  }
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, info->notifyEventTypes));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, info->notifyErrorHandle));
  if (addrSize > 0) {
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, info->streamName));
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, info->stbFullName));
    TAOS_CHECK_EXIT(tEncodeSSchemaWrapper(pEncoder, info->pSchemaWrapper));
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tDecodeStreamNotifyInfo(SDecoder* pDecoder, SNotifyInfo* info) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  QUERY_CHECK_NULL(pDecoder, code, lino, _exit, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(info, code, lino, _exit, TSDB_CODE_INVALID_PARA);

  int32_t addrSize = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &addrSize));
  info->pNotifyAddrUrls = taosArrayInit(addrSize, POINTER_BYTES);
  QUERY_CHECK_NULL(info->pNotifyAddrUrls, code, lino, _exit, terrno);
  for (int32_t i = 0; i < addrSize; ++i) {
    char* url = NULL;
    TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &url));
    url = taosStrndup(url, TSDB_STREAM_NOTIFY_URL_LEN);
    QUERY_CHECK_NULL(url, code, lino, _exit, terrno);
    if (taosArrayPush(info->pNotifyAddrUrls, &url) == NULL) {
      taosMemoryFree(url);
      TAOS_CHECK_EXIT(terrno);
    }
  }
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &info->notifyEventTypes));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &info->notifyErrorHandle));
  if (addrSize > 0) {
    char* name = NULL;
    TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &name));
    info->streamName = taosStrndup(name, TSDB_STREAM_FNAME_LEN + 1);
    QUERY_CHECK_NULL(info->streamName, code, lino, _exit, terrno);
    TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &name));
    info->stbFullName = taosStrndup(name, TSDB_STREAM_FNAME_LEN + 1);
    QUERY_CHECK_NULL(info->stbFullName, code, lino, _exit, terrno);
    info->pSchemaWrapper = taosMemoryCalloc(1, sizeof(SSchemaWrapper));
    if (info->pSchemaWrapper == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeSSchemaWrapper(pDecoder, info->pSchemaWrapper));
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tEncodeVTablesInfo(SEncoder* pEncoder, SArray* pVTables) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SVCTableMergeInfo* pMergeInfo = NULL;
  int32_t mergeNum = taosArrayGetSize(pVTables);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, mergeNum));
  for (int32_t i = 0; i < mergeNum; ++i) {
    pMergeInfo = (SVCTableMergeInfo*)taosArrayGet(pVTables, i);
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pMergeInfo->uid));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pMergeInfo->numOfSrcTbls));
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tDecodeVTablesInfo(SDecoder* pDecoder, SArray** pTables) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  int32_t mergeNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &mergeNum));
  if (mergeNum > 0) {
    *pTables = taosArrayInit(mergeNum, sizeof(SVCTableMergeInfo));
    QUERY_CHECK_NULL(*pTables, code, lino, _exit, terrno);
  }

  SVCTableMergeInfo mergeInfo;
  for (int32_t i = 0; i < mergeNum; ++i) {
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &mergeInfo.uid));
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &mergeInfo.numOfSrcTbls));
    if (taosArrayPush(*pTables, &mergeInfo) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tSerializeDispatcherTaskInfo(SEncoder* pEncoder, const SArray* pTaskInfos) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t nTasks = taosArrayGetSize(pTaskInfos);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, nTasks));
  for (int32_t i = 0; i < nTasks; ++i) {
    STaskDispatcherFixed* pAddr = taosArrayGet(pTaskInfos, i);
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pAddr->taskId));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pAddr->nodeId));
    TAOS_CHECK_EXIT(tEncodeSEpSet(pEncoder, &pAddr->epSet));
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tSerializeDispatcherVtableMap(SEncoder* pEncoder, const SSHashObj* pVtables) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t tbNum = tSimpleHashGetSize(pVtables);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, tbNum));
  int32_t iter = 0;
  void*   p = tSimpleHashIterate(pVtables, NULL, &iter);
  while (p != NULL) {
    int64_t* pUid = tSimpleHashGetKey(p, NULL);
    int32_t* pIdx = p;
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, *pUid));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, *pIdx));
    p = tSimpleHashIterate(pVtables, p, &iter);
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tDeserializeDispatcherTaskInfo(SDecoder* pDecoder, SArray** ppTaskInfos) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t nTasks = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &nTasks));
  if (nTasks <= 0) {
    return code;
  }

  *ppTaskInfos = taosArrayInit(nTasks, sizeof(STaskDispatcherFixed));
  QUERY_CHECK_NULL(*ppTaskInfos, code, lino, _exit, terrno);

  STaskDispatcherFixed addr;
  for (int32_t i = 0; i < nTasks; ++i) {
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &addr.taskId));
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &addr.nodeId));
    TAOS_CHECK_EXIT(tDecodeSEpSet(pDecoder, &addr.epSet));
    void* px = taosArrayPush(*ppTaskInfos, &addr);
    QUERY_CHECK_NULL(px, code, lino, _exit, terrno);
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tDeserializeDispatcherVtableMap(SDecoder* pDecoder, SSHashObj** ppVtables) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t tbNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &tbNum));
  if (tbNum <= 0) {
    return code;
  }

  *ppVtables = tSimpleHashInit(tbNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(*ppVtables, code, lino, _exit, terrno);

  uint64_t uid = 0;
  int32_t  idx = 0;
  for (int32_t i = 0; i < tbNum; ++i) {
    TAOS_CHECK_EXIT(tDecodeU64(pDecoder, &uid));
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &idx));
    TAOS_CHECK_EXIT(tSimpleHashPut(*ppVtables, &uid, sizeof(uid), &idx, sizeof(idx)));
  }

_exit:

  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tEncodeStreamTask(SEncoder* pEncoder, const SStreamTask* pTask) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->ver));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->id.streamId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTask->id.taskId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTask->info.trigger));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pTask->info.taskLevel));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pTask->outputInfo.type));
  TAOS_CHECK_EXIT(tEncodeI16(pEncoder, pTask->msgInfo.msgType));

  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pTask->status.taskStatus));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pTask->status.schedStatus));

  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTask->info.selfChildId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTask->info.nodeId));
  TAOS_CHECK_EXIT(tEncodeSEpSet(pEncoder, &pTask->info.epSet));
  TAOS_CHECK_EXIT(tEncodeSEpSet(pEncoder, &pTask->info.mnodeEpset));

  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->chkInfo.checkpointId));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->chkInfo.checkpointVer));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pTask->info.fillHistory));

  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->hTaskInfo.id.streamId));
  int32_t taskId = pTask->hTaskInfo.id.taskId;
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, taskId));

  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->streamTaskId.streamId));
  taskId = pTask->streamTaskId.taskId;
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, taskId));

  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pTask->dataRange.range.minVer));
  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pTask->dataRange.range.maxVer));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->dataRange.window.skey));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->dataRange.window.ekey));

  int32_t epSz = taosArrayGetSize(pTask->upstreamInfo.pList);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, epSz));
  for (int32_t i = 0; i < epSz; i++) {
    SStreamUpstreamEpInfo* pInfo = taosArrayGetP(pTask->upstreamInfo.pList, i);
    TAOS_CHECK_EXIT(tEncodeStreamEpInfo(pEncoder, pInfo));
  }

  if (pTask->info.taskLevel != TASK_LEVEL__SINK) {
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pTask->exec.qmsg));
  }

  if (pTask->outputInfo.type == TASK_OUTPUT__TABLE) {
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->outputInfo.tbSink.stbUid));
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pTask->outputInfo.tbSink.stbFullName));
    TAOS_CHECK_EXIT(tEncodeSSchemaWrapper(pEncoder, pTask->outputInfo.tbSink.pSchemaWrapper));
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SMA) {
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->outputInfo.smaSink.smaId));
  } else if (pTask->outputInfo.type == TASK_OUTPUT__FETCH) {
    TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pTask->outputInfo.fetchSink.reserved));
  } else if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTask->outputInfo.fixedDispatcher.taskId));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTask->outputInfo.fixedDispatcher.nodeId));
    TAOS_CHECK_EXIT(tEncodeSEpSet(pEncoder, &pTask->outputInfo.fixedDispatcher.epSet));
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    TAOS_CHECK_EXIT(tSerializeSUseDbRspImp(pEncoder, &pTask->outputInfo.shuffleDispatcher.dbInfo));
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pTask->outputInfo.shuffleDispatcher.stbFullName));
  } else if (pTask->outputInfo.type == TASK_OUTPUT__VTABLE_MAP) {
    TAOS_CHECK_EXIT(tSerializeDispatcherTaskInfo(pEncoder, pTask->outputInfo.vtableMapDispatcher.taskInfos));
    TAOS_CHECK_EXIT(tSerializeDispatcherVtableMap(pEncoder, pTask->outputInfo.vtableMapDispatcher.vtableMap));
  }
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTask->info.delaySchedParam));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pTask->subtableWithoutMd5));
  TAOS_CHECK_EXIT(tEncodeCStrWithLen(pEncoder, pTask->reserve, sizeof(pTask->reserve) - 1));

  if (pTask->ver >= SSTREAM_TASK_ADD_NOTIFY_VER) {
    TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pTask->info.hasAggTasks));
    TAOS_CHECK_EXIT(tEncodeStreamNotifyInfo(pEncoder, &pTask->notifyInfo));
    TAOS_CHECK_EXIT(tEncodeVTablesInfo(pEncoder, pTask->pVTables));
  }

  tEndEncode(pEncoder);
_exit:
  return code;
}

int32_t tDecodeStreamTask(SDecoder* pDecoder, SStreamTask* pTask) {
  int32_t taskId = 0;
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->ver));
  if (pTask->ver <= SSTREAM_TASK_INCOMPATIBLE_VER || pTask->ver > SSTREAM_TASK_VER) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }

  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->id.streamId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pTask->id.taskId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pTask->info.trigger));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pTask->info.taskLevel));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pTask->outputInfo.type));
  TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &pTask->msgInfo.msgType));

  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pTask->status.taskStatus));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pTask->status.schedStatus));

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pTask->info.selfChildId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pTask->info.nodeId));
  TAOS_CHECK_EXIT(tDecodeSEpSet(pDecoder, &pTask->info.epSet));
  TAOS_CHECK_EXIT(tDecodeSEpSet(pDecoder, &pTask->info.mnodeEpset));

  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->chkInfo.checkpointId));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->chkInfo.checkpointVer));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pTask->info.fillHistory));

  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->hTaskInfo.id.streamId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &taskId));
  pTask->hTaskInfo.id.taskId = taskId;

  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->streamTaskId.streamId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &taskId));
  pTask->streamTaskId.taskId = taskId;

  TAOS_CHECK_EXIT(tDecodeU64(pDecoder, (uint64_t*)&pTask->dataRange.range.minVer));
  TAOS_CHECK_EXIT(tDecodeU64(pDecoder, (uint64_t*)&pTask->dataRange.range.maxVer));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->dataRange.window.skey));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->dataRange.window.ekey));

  int32_t epSz = -1;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &epSz) < 0);

  if ((pTask->upstreamInfo.pList = taosArrayInit(epSz, POINTER_BYTES)) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int32_t i = 0; i < epSz; i++) {
    SStreamUpstreamEpInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamUpstreamEpInfo));
    if (pInfo == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    if ((code = tDecodeStreamEpInfo(pDecoder, pInfo)) < 0) {
      taosMemoryFreeClear(pInfo);
      goto _exit;
    }
    if (taosArrayPush(pTask->upstreamInfo.pList, &pInfo) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  if (pTask->info.taskLevel != TASK_LEVEL__SINK) {
    TAOS_CHECK_EXIT(tDecodeCStrAlloc(pDecoder, &pTask->exec.qmsg));
  }

  if (pTask->outputInfo.type == TASK_OUTPUT__TABLE) {
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->outputInfo.tbSink.stbUid));
    TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pTask->outputInfo.tbSink.stbFullName));
    pTask->outputInfo.tbSink.pSchemaWrapper = taosMemoryCalloc(1, sizeof(SSchemaWrapper));
    if (pTask->outputInfo.tbSink.pSchemaWrapper == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeSSchemaWrapper(pDecoder, pTask->outputInfo.tbSink.pSchemaWrapper));
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SMA) {
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->outputInfo.smaSink.smaId));
  } else if (pTask->outputInfo.type == TASK_OUTPUT__FETCH) {
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pTask->outputInfo.fetchSink.reserved));
  } else if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pTask->outputInfo.fixedDispatcher.taskId));
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pTask->outputInfo.fixedDispatcher.nodeId));
    TAOS_CHECK_EXIT(tDecodeSEpSet(pDecoder, &pTask->outputInfo.fixedDispatcher.epSet));
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    TAOS_CHECK_EXIT(tDeserializeSUseDbRspImp(pDecoder, &pTask->outputInfo.shuffleDispatcher.dbInfo));
    TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pTask->outputInfo.shuffleDispatcher.stbFullName));
  } else if (pTask->outputInfo.type == TASK_OUTPUT__VTABLE_MAP) {
    TAOS_CHECK_EXIT(tDeserializeDispatcherTaskInfo(pDecoder, &pTask->outputInfo.vtableMapDispatcher.taskInfos));
    TAOS_CHECK_EXIT(tDeserializeDispatcherVtableMap(pDecoder, &pTask->outputInfo.vtableMapDispatcher.vtableMap));
  }
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTask->info.delaySchedParam));
  if (pTask->ver >= SSTREAM_TASK_SUBTABLE_CHANGED_VER) {
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pTask->subtableWithoutMd5));
  }
  TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pTask->reserve));

  if (pTask->ver >= SSTREAM_TASK_ADD_NOTIFY_VER) {
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pTask->info.hasAggTasks));
    TAOS_CHECK_EXIT(tDecodeStreamNotifyInfo(pDecoder, &pTask->notifyInfo));
    TAOS_CHECK_EXIT(tDecodeVTablesInfo(pDecoder, &pTask->pVTables));
  }

  tEndDecode(pDecoder);

_exit:
  return code;
}
