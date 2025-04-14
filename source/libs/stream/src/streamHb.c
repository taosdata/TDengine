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
#include "tref.h"
#include "tstream.h"
#include "ttimer.h"
#include "wal.h"

int32_t streamMetaRefPool = 0;

struct SMetaHbInfo {
  tmr_h        hbTmr;
  int32_t      tickCounter;
  int32_t      hbCount;
  int64_t      hbStart;
  int64_t      msgSendTs;
  SStreamHbMsg hbMsg;
};

static bool waitForEnoughDuration(SMetaHbInfo* pInfo) {
  if ((++pInfo->tickCounter) >= META_HB_SEND_IDLE_COUNTER) {  // reset the counter
    pInfo->tickCounter = 0;
    return true;
  }
  return false;
}

static bool existInHbMsg(SStreamHbMsg* pMsg, SDownstreamTaskEpset* pTaskEpset) {
  int32_t numOfExisted = taosArrayGetSize(pMsg->pUpdateNodes);
  for (int32_t k = 0; k < numOfExisted; ++k) {
    if (pTaskEpset->nodeId == *(int32_t*)taosArrayGet(pMsg->pUpdateNodes, k)) {
      return true;
    }
  }
  return false;
}

static void addUpdateNodeIntoHbMsg(SStreamTask* pTask, SStreamHbMsg* pMsg) {
  SStreamMeta* pMeta = pTask->pMeta;

  streamMutexLock(&pTask->lock);

  int32_t num = taosArrayGetSize(pTask->outputInfo.pNodeEpsetUpdateList);
  for (int32_t j = 0; j < num; ++j) {
    SDownstreamTaskEpset* pTaskEpset = taosArrayGet(pTask->outputInfo.pNodeEpsetUpdateList, j);

    bool exist = existInHbMsg(pMsg, pTaskEpset);
    if (!exist) {
      void* p = taosArrayPush(pMsg->pUpdateNodes, &pTaskEpset->nodeId);
      if (p == NULL) {
        stError("failed to set the updateNode info in hbMsg, vgId:%d", pMeta->vgId);
      }

      stDebug("vgId:%d nodeId:%d added into hbMsg update list, total:%d", pMeta->vgId, pTaskEpset->nodeId,
              (int32_t)taosArrayGetSize(pMsg->pUpdateNodes));
    }
  }

  taosArrayClear(pTask->outputInfo.pNodeEpsetUpdateList);
  streamMutexUnlock(&pTask->lock);
}

static void setProcessProgress(SStreamTask* pTask, STaskStatusEntry* pEntry) {
  if (pTask->info.taskLevel != TASK_LEVEL__SOURCE) {
    return;
  }

  if (pTask->info.trigger == STREAM_TRIGGER_FORCE_WINDOW_CLOSE) {
    pEntry->processedVer = pTask->status.latestForceWindow.skey;
  } else {
    if (pTask->exec.pWalReader != NULL) {
      pEntry->processedVer = walReaderGetCurrentVer(pTask->exec.pWalReader) - 1;
      if (pEntry->processedVer < 0) {
        pEntry->processedVer = pTask->chkInfo.processedVer;
      }

      walReaderValidVersionRange(pTask->exec.pWalReader, &pEntry->verRange.minVer, &pEntry->verRange.maxVer);
    }
  }
}

static int32_t doSendHbMsgInfo(SStreamHbMsg* pMsg, SStreamMeta* pMeta, SEpSet* pEpset) {
  int32_t code = 0;
  int32_t tlen = 0;

  tEncodeSize(tEncodeStreamHbMsg, pMsg, tlen, code);
  if (code < 0) {
    stError("vgId:%d encode stream hb msg failed, code:%s", pMeta->vgId, tstrerror(code));
    return TSDB_CODE_FAILED;
  }

  void* buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    stError("vgId:%d encode stream hb msg failed, code:%s", pMeta->vgId, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    return TSDB_CODE_FAILED;
  }

  SEncoder encoder;
  tEncoderInit(&encoder, buf, tlen);
  if ((code = tEncodeStreamHbMsg(&encoder, pMsg)) < 0) {
    rpcFreeCont(buf);
    tEncoderClear(&encoder);
    stError("vgId:%d encode stream hb msg failed, code:%s", pMeta->vgId, tstrerror(code));
    return TSDB_CODE_FAILED;
  }
  tEncoderClear(&encoder);

  stDebug("vgId:%d send hb to mnode, numOfTasks:%d msgId:%d", pMeta->vgId, pMsg->numOfTasks, pMsg->msgId);

  SRpcMsg msg = {0};
  initRpcMsg(&msg, TDMT_MND_STREAM_HEARTBEAT, buf, tlen);
  return tmsgSendReq(pEpset, &msg);
}

static int32_t streamTaskGetMndEpset(SStreamMeta* pMeta, SEpSet* pEpSet) {
  int32_t numOfTasks = streamMetaGetNumOfTasks(pMeta);
  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId* pId = taosArrayGet(pMeta->pTaskList, i);
    STaskId        id = {.streamId = pId->streamId, .taskId = pId->taskId};
    SStreamTask*   pTask = NULL;

    int32_t code = streamMetaAcquireTaskUnsafe(pMeta, &id, &pTask);
    if (code != 0) {
      continue;
    }

    if (pTask->info.fillHistory == 1) {
      streamMetaReleaseTask(pMeta, pTask);
      continue;
    }

    epsetAssign(pEpSet, &pTask->info.mnodeEpset);
    streamMetaReleaseTask(pMeta, pTask);
    return TSDB_CODE_SUCCESS;
  }

  return TSDB_CODE_FAILED;
}

static void streamTaskUpdateMndEpset(SStreamMeta* pMeta, SEpSet* pEpSet) {
  int32_t numOfTasks = streamMetaGetNumOfTasks(pMeta);

  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId* pId = taosArrayGet(pMeta->pTaskList, i);
    STaskId        id = {.streamId = pId->streamId, .taskId = pId->taskId};
    SStreamTask*   pTask = NULL;

    int32_t code = streamMetaAcquireTaskUnsafe(pMeta, &id, &pTask);
    if (code != 0) {
      stError("vgId:%d s-task:0x%x failed to acquire it for updating mnode epset, code:%s", pMeta->vgId, pId->taskId,
              tstrerror(code));
      continue;
    }

    // ignore this error since it is only for log file
    char    buf[256] = {0};
    int32_t ret = epsetToStr(&pTask->info.mnodeEpset, buf, tListLen(buf));
    if (ret != 0) {  // print error and continue
      stError("failed to convert epset to str, code:%s", tstrerror(ret));
    }

    char newBuf[256] = {0};
    ret = epsetToStr(pEpSet, newBuf, tListLen(newBuf));
    if (ret != 0) {
      stError("failed to convert epset to str, code:%s", tstrerror(ret));
    }

    epsetAssign(&pTask->info.mnodeEpset, pEpSet);
    stInfo("s-task:0x%x update mnd epset, from %s to %s", pId->taskId, buf, newBuf);
    streamMetaReleaseTask(pMeta, pTask);
  }

  stDebug("vgId:%d update mnd epset for %d tasks completed", pMeta->vgId, numOfTasks);
}

// NOTE: this task should be executed within the SStreamMeta lock region.
int32_t streamMetaSendHbHelper(SStreamMeta* pMeta) {
  SEpSet       epset = {0};
  bool         hasMnodeEpset = false;
  int32_t      numOfTasks = streamMetaGetNumOfTasks(pMeta);
  SMetaHbInfo* pInfo = pMeta->pHbInfo;
  int32_t      code = 0;
  bool         setReqCheckpointId = false;

  // not recv the hb msg rsp yet, send current hb msg again
  if (pInfo->msgSendTs > 0) {
    stDebug("vgId:%d hbMsg rsp not recv, send current hbMsg, msgId:%d, total:%d again", pMeta->vgId, pInfo->hbMsg.msgId,
            pInfo->hbCount);

    code = streamTaskGetMndEpset(pMeta, &epset);
    if (code != 0) {
      stError("vgId:%d failed to get the mnode epset, not retrying sending hbMsg, msgId:%d", pMeta->vgId,
              pInfo->hbMsg.msgId);
      return code;
    }

    pInfo->msgSendTs = taosGetTimestampMs();
    return doSendHbMsgInfo(&pInfo->hbMsg, pMeta, &epset);
  }

  SStreamHbMsg* pMsg = &pInfo->hbMsg;
  pMsg->vgId = pMeta->vgId;
  pMsg->msgId = pMeta->pHbInfo->hbCount;
  pMsg->ts = taosGetTimestampMs();

  stDebug("vgId:%d build stream hbMsg, leader:%d HbMsgId:%d, HbMsgTs:%" PRId64, pMeta->vgId,
          (pMeta->role == NODE_ROLE_LEADER), pMsg->msgId, pMsg->ts);

  pMsg->pTaskStatus = taosArrayInit(numOfTasks, sizeof(STaskStatusEntry));
  pMsg->pUpdateNodes = taosArrayInit(numOfTasks, sizeof(int32_t));

  if (pMsg->pTaskStatus == NULL || pMsg->pUpdateNodes == NULL) {
    return terrno;
  }

  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId* pId = taosArrayGet(pMeta->pTaskList, i);

    STaskId      id = {.streamId = pId->streamId, .taskId = pId->taskId};
    SStreamTask* pTask = NULL;
    code = streamMetaAcquireTaskUnsafe(pMeta, &id, &pTask);
    if (code != 0) {
      continue;
    }

    // not report the status of fill-history task
    if (pTask->info.fillHistory != STREAM_NORMAL_TASK) {
      streamMetaReleaseTask(pMeta, pTask);
      continue;
    }

    // todo: this lock may be blocked by lock in streamMetaStartOneTask function, which may lock a very long time when
    // trying to load remote checkpoint data
    streamMutexLock(&pTask->lock);
    STaskStatusEntry entry = streamTaskGetStatusEntry(pTask);
    streamMutexUnlock(&pTask->lock);

    entry.inputRate = entry.inputQUsed * 100.0 / (2 * STREAM_TASK_QUEUE_CAPACITY_IN_SIZE);
    if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
      entry.sinkQuota = pTask->outputInfo.pTokenBucket->quotaRate;
      entry.sinkDataSize = SIZE_IN_MiB(pTask->execInfo.sink.dataSize);
    }

    SActiveCheckpointInfo* p = pTask->chkInfo.pActiveInfo;
    if (p->activeId != 0) {
      entry.checkpointInfo.failed = (p->failedId >= p->activeId) ? 1 : 0;
      entry.checkpointInfo.activeId = p->activeId;
      entry.checkpointInfo.activeTransId = p->transId;

      if (entry.checkpointInfo.failed) {
        stInfo("s-task:%s set kill checkpoint trans in hbMsg, transId:%d, clear the active checkpointInfo",
               pTask->id.idStr, p->transId);

        streamMutexLock(&pTask->lock);
        streamTaskClearCheckInfo(pTask, true);
        streamMutexUnlock(&pTask->lock);
      }
    }

    streamMutexLock(&pTask->lock);
    entry.checkpointInfo.consensusChkptId = streamTaskCheckIfReqConsenChkptId(pTask, pMsg->ts);
    if (entry.checkpointInfo.consensusChkptId) {
      entry.checkpointInfo.consensusTs = pTask->status.consenChkptInfo.statusTs;
      setReqCheckpointId = true;
    }
    streamMutexUnlock(&pTask->lock);

    setProcessProgress(pTask, &entry);
    addUpdateNodeIntoHbMsg(pTask, pMsg);

    p = taosArrayPush(pMsg->pTaskStatus, &entry);
    if (p == NULL) {
      stError("failed to add taskInfo:0x%x in hbMsg, vgId:%d", pTask->id.taskId, pMeta->vgId);
    }

    if (!hasMnodeEpset) {
      epsetAssign(&epset, &pTask->info.mnodeEpset);
      hasMnodeEpset = true;
    }

    streamMetaReleaseTask(pMeta, pTask);
  }

  if (setReqCheckpointId) {
    if (pMeta->startInfo.curStage != START_MARK_REQ_CHKPID) {
      stError("vgId:%d internal unknown error, current stage is:%d expected:%d", pMeta->vgId, pMeta->startInfo.curStage,
              START_MARK_REQ_CHKPID);
    }

    pMeta->startInfo.curStage = START_WAIT_FOR_CHKPTID;
    SStartTaskStageInfo info = {.stage = pMeta->startInfo.curStage, .ts = pMsg->ts};
    taosArrayPush(pMeta->startInfo.pStagesList, &info);

    stDebug("vgId:%d mark_req stage -> wait_for_chkptId stage, reqTs:%" PRId64 " , numOfStageHist:%d", pMeta->vgId,
            info.ts, (int32_t)taosArrayGetSize(pMeta->startInfo.pStagesList));
  }

  pMsg->numOfTasks = taosArrayGetSize(pMsg->pTaskStatus);

  if (hasMnodeEpset) {
    pInfo->msgSendTs = taosGetTimestampMs();
    code = doSendHbMsgInfo(pMsg, pMeta, &epset);
  } else {
    stDebug("vgId:%d no tasks or no mnd epset, not send stream hb to mnode", pMeta->vgId);
    tCleanupStreamHbMsg(&pInfo->hbMsg);
    pInfo->msgSendTs = -1;
  }

  return code;
}

void streamMetaHbToMnode(void* param, void* tmrId) {
  int64_t rid = *(int64_t*)param;
  int32_t code = 0;
  int32_t vgId = 0;
  int32_t role = 0;

  SStreamMeta* pMeta = taosAcquireRef(streamMetaRefPool, rid);
  if (pMeta == NULL) {
    stError("invalid meta rid:%" PRId64 " failed to acquired stream-meta", rid);
    return;
  }

  vgId = pMeta->vgId;
  role = pMeta->role;

  // need to stop, stop now
  if (pMeta->closeFlag) {
    pMeta->pHbInfo->hbStart = 0;
    code = taosReleaseRef(streamMetaRefPool, rid);
    if (code == TSDB_CODE_SUCCESS) {
      stInfo("vgId:%d jump out of meta timer since closed", vgId);
    } else {
      stError("vgId:%d jump out of meta timer, failed to release the meta rid:%" PRId64, vgId, rid);
    }
    return;
  }

  // not leader not send msg
  if (pMeta->role != NODE_ROLE_LEADER) {
    pMeta->pHbInfo->hbStart = 0;
    code = taosReleaseRef(streamMetaRefPool, rid);
    if (code == TSDB_CODE_SUCCESS) {
      stInfo("vgId:%d role:%d not leader not send hb to mnode", vgId, role);
    } else {
      stError("vgId:%d role:%d not leader not send hb to mnode, failed to release meta rid:%" PRId64, vgId, role, rid);
    }
    return;
  }

  if (!waitForEnoughDuration(pMeta->pHbInfo)) {
    streamTmrStart(streamMetaHbToMnode, META_HB_CHECK_INTERVAL, param, streamTimer, &pMeta->pHbInfo->hbTmr, vgId,
                   "meta-hb-tmr");

    code = taosReleaseRef(streamMetaRefPool, rid);
    if (code) {
      stError("vgId:%d in meta timer, failed to release the meta rid:%" PRId64, vgId, rid);
    }
    return;
  }

  // set the hb start time
  if (pMeta->pHbInfo->hbStart == 0) {
    pMeta->pHbInfo->hbStart = taosGetTimestampMs();
  }

  // NOTE: stream task in restart procedure. not generate the hb now, try to acquire the lock may cause stuck this timer.
  int32_t count = 30;
  bool    send = false;
  while ((--count) >= 0) {
    int32_t ret = streamMetaTryRlock(pMeta);
    if (ret != 0) {
      taosMsleep(10);
    } else {
      send = true;
      code = streamMetaSendHbHelper(pMeta);
      streamMetaRUnLock(pMeta);
      break;
    }
  }

  if (!send) {
    stError("vgId:%d failed to send hbMsg to mnode due to acquire lock failure, retry again in 5s", pMeta->vgId);
  }
  if (code) {
    stError("vgId:%d failed to send hbMsg to mnode, retry in 5, code:%s", pMeta->vgId, tstrerror(code));
  }

  streamTmrStart(streamMetaHbToMnode, META_HB_CHECK_INTERVAL, param, streamTimer, &pMeta->pHbInfo->hbTmr, pMeta->vgId,
                 "meta-hb-tmr");

  code = taosReleaseRef(streamMetaRefPool, rid);
  if (code) {
    stError("vgId:%d in meta timer, failed to release the meta rid:%" PRId64, vgId, rid);
  }
}

int32_t createMetaHbInfo(int64_t* pRid, SMetaHbInfo** pRes) {
  *pRes = NULL;
  SMetaHbInfo* pInfo = taosMemoryCalloc(1, sizeof(SMetaHbInfo));
  if (pInfo == NULL) {
    return terrno;
  }

  pInfo->tickCounter = 0;
  pInfo->msgSendTs = -1;
  pInfo->hbCount = 0;

  *pRes = pInfo;

  streamTmrStart(streamMetaHbToMnode, META_HB_CHECK_INTERVAL, pRid, streamTimer, &pInfo->hbTmr, 0, "stream-hb");
  return TSDB_CODE_SUCCESS;
}

void destroyMetaHbInfo(SMetaHbInfo* pInfo) {
  if (pInfo != NULL) {
    tCleanupStreamHbMsg(&pInfo->hbMsg);

    if (pInfo->hbTmr != NULL) {
      streamTmrStop(pInfo->hbTmr);
      pInfo->hbTmr = NULL;
    }

    taosMemoryFree(pInfo);
  }
}

void streamMetaWaitForHbTmrQuit(SStreamMeta* pMeta) {
  // wait for the stream meta hb function stopping
  if (pMeta->role == NODE_ROLE_LEADER) {
    taosMsleep(3 * META_HB_CHECK_INTERVAL);
    stDebug("vgId:%d wait for meta to stop timer", pMeta->vgId);
  }
}

void streamMetaGetHbSendInfo(SMetaHbInfo* pInfo, int64_t* pStartTs, int32_t* pSendCount) {
  *pStartTs = 0;
  *pSendCount = 0;

  if (pInfo == NULL) {
    return;
  }

  *pStartTs = pInfo->hbStart;
  *pSendCount = pInfo->hbCount;
}

int32_t streamProcessHeartbeatRsp(SStreamMeta* pMeta, SMStreamHbRspMsg* pRsp) {
  SMetaHbInfo* pInfo = pMeta->pHbInfo;
  SEpSet       epset = {0};
  int32_t      code = 0;

  stDebug("vgId:%d process hbMsg rsp, msgId:%d rsp confirmed", pMeta->vgId, pRsp->msgId);
  streamMetaWLock(pMeta);

  // current waiting rsp recved
  if (pRsp->msgId == pInfo->hbCount) {
    tCleanupStreamHbMsg(&pInfo->hbMsg);
    stDebug("vgId:%d hbMsg msgId:%d sendTs:%" PRId64 " recved confirmed", pMeta->vgId, pRsp->msgId, pInfo->msgSendTs);

    pInfo->hbCount += 1;
    pInfo->msgSendTs = -1;

    code = streamTaskGetMndEpset(pMeta, &epset);
    if (!isEpsetEqual(&pRsp->mndEpset, &epset) && (code == 0)) {
      // we need to update the mnode epset for each tasks
      stInfo("vgId:%d mnode epset updated, update mnode epset for all tasks", pMeta->vgId);
      streamTaskUpdateMndEpset(pMeta, &pRsp->mndEpset);
    }
  } else {
    stWarn("vgId:%d recv expired hb rsp, msgId:%d, discarded", pMeta->vgId, pRsp->msgId);
  }

  streamMetaWUnLock(pMeta);
  return TSDB_CODE_SUCCESS;
}