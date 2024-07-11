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

int32_t streamMetaId = 0;

struct SMetaHbInfo {
  tmr_h        hbTmr;
  int32_t      stopFlag;
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
  for (int k = 0; k < numOfExisted; ++k) {
    if (pTaskEpset->nodeId == *(int32_t*)taosArrayGet(pMsg->pUpdateNodes, k)) {
      return true;
    }
  }
  return false;
}

static void addUpdateNodeIntoHbMsg(SStreamTask* pTask, SStreamHbMsg* pMsg) {
  SStreamMeta* pMeta = pTask->pMeta;

  taosThreadMutexLock(&pTask->lock);

  int32_t num = taosArrayGetSize(pTask->outputInfo.pNodeEpsetUpdateList);
  for (int j = 0; j < num; ++j) {
    SDownstreamTaskEpset* pTaskEpset = taosArrayGet(pTask->outputInfo.pNodeEpsetUpdateList, j);

    bool exist = existInHbMsg(pMsg, pTaskEpset);
    if (!exist) {
      taosArrayPush(pMsg->pUpdateNodes, &pTaskEpset->nodeId);
      stDebug("vgId:%d nodeId:%d added into hbMsg update list, total:%d", pMeta->vgId, pTaskEpset->nodeId,
              (int32_t)taosArrayGetSize(pMsg->pUpdateNodes));
    }
  }

  taosArrayClear(pTask->outputInfo.pNodeEpsetUpdateList);
  taosThreadMutexUnlock(&pTask->lock);
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
    stError("vgId:%d encode stream hb msg failed, code:%s", pMeta->vgId, tstrerror(code));
    return TSDB_CODE_FAILED;
  }
  tEncoderClear(&encoder);

  stDebug("vgId:%d send hb to mnode, numOfTasks:%d msgId:%d", pMeta->vgId, pMsg->numOfTasks, pMsg->msgId);

  SRpcMsg msg = {0};
  initRpcMsg(&msg, TDMT_MND_STREAM_HEARTBEAT, buf, tlen);
  tmsgSendReq(pEpset, &msg);

  return TSDB_CODE_SUCCESS;
}

// NOTE: this task should be executed within the SStreamMeta lock region.
int32_t streamMetaSendHbHelper(SStreamMeta* pMeta) {
  SEpSet       epset = {0};
  bool         hasMnodeEpset = false;
  int32_t      numOfTasks = streamMetaGetNumOfTasks(pMeta);
  SMetaHbInfo* pInfo = pMeta->pHbInfo;

  // not recv the hb msg rsp yet, send current hb msg again
  if (pInfo->msgSendTs > 0) {
    stDebug("vgId:%d hbMsg rsp not recv, send current hbMsg, msgId:%d, total:%d again", pMeta->vgId, pInfo->hbMsg.msgId,
            pInfo->hbCount);

    for(int32_t i = 0; i < numOfTasks; ++i) {
      SStreamTaskId* pId = taosArrayGet(pMeta->pTaskList, i);
      STaskId       id = {.streamId = pId->streamId, .taskId = pId->taskId};
      SStreamTask** pTask = taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
      if (pTask == NULL) {
        continue;
      }

      if ((*pTask)->info.fillHistory == 1) {
        continue;
      }

      epsetAssign(&epset, &(*pTask)->info.mnodeEpset);
      break;
    }

    pInfo->msgSendTs = taosGetTimestampMs();
    doSendHbMsgInfo(&pInfo->hbMsg, pMeta, &epset);
    return TSDB_CODE_SUCCESS;
  }

  SStreamHbMsg* pMsg = &pInfo->hbMsg;
  stDebug("vgId:%d build stream hbMsg, leader:%d msgId:%d", pMeta->vgId, (pMeta->role == NODE_ROLE_LEADER),
          pMeta->pHbInfo->hbCount);

  pMsg->vgId = pMeta->vgId;
  pMsg->msgId = pMeta->pHbInfo->hbCount;

  pMsg->pTaskStatus = taosArrayInit(numOfTasks, sizeof(STaskStatusEntry));
  pMsg->pUpdateNodes = taosArrayInit(numOfTasks, sizeof(int32_t));

  if (pMsg->pTaskStatus == NULL || pMsg->pUpdateNodes == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId* pId = taosArrayGet(pMeta->pTaskList, i);

    STaskId       id = {.streamId = pId->streamId, .taskId = pId->taskId};
    SStreamTask** pTask = taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
    if (pTask == NULL) {
      continue;
    }

    // not report the status of fill-history task
    if ((*pTask)->info.fillHistory == 1) {
      continue;
    }

    taosThreadMutexLock(&(*pTask)->lock);
    STaskStatusEntry entry = streamTaskGetStatusEntry(*pTask);
    taosThreadMutexUnlock(&(*pTask)->lock);

    entry.inputRate = entry.inputQUsed * 100.0 / (2 * STREAM_TASK_QUEUE_CAPACITY_IN_SIZE);
    if ((*pTask)->info.taskLevel == TASK_LEVEL__SINK) {
      entry.sinkQuota = (*pTask)->outputInfo.pTokenBucket->quotaRate;
      entry.sinkDataSize = SIZE_IN_MiB((*pTask)->execInfo.sink.dataSize);
    }

    SActiveCheckpointInfo* p = (*pTask)->chkInfo.pActiveInfo;
    if (p->activeId != 0) {
      entry.checkpointInfo.failed = (p->failedId >= p->activeId) ? 1 : 0;
      entry.checkpointInfo.activeId = p->activeId;
      entry.checkpointInfo.activeTransId = p->transId;

      if (entry.checkpointInfo.failed) {
        stInfo("s-task:%s set kill checkpoint trans in hbMsg, transId:%d, clear the active checkpointInfo",
               (*pTask)->id.idStr, p->transId);

        taosThreadMutexLock(&(*pTask)->lock);
        streamTaskClearCheckInfo((*pTask), true);
        taosThreadMutexUnlock(&(*pTask)->lock);
      }
    }

    if ((*pTask)->status.requireConsensusChkptId) {
      entry.checkpointInfo.consensusChkptId = 1;
      (*pTask)->status.requireConsensusChkptId = false;
      stDebug("s-task:%s vgId:%d set the require consensus-checkpointId in hbMsg", (*pTask)->id.idStr, pMeta->vgId);
    }

    if ((*pTask)->exec.pWalReader != NULL) {
      entry.processedVer = walReaderGetCurrentVer((*pTask)->exec.pWalReader) - 1;
      if (entry.processedVer < 0) {
        entry.processedVer = (*pTask)->chkInfo.processedVer;
      }

      walReaderValidVersionRange((*pTask)->exec.pWalReader, &entry.verRange.minVer, &entry.verRange.maxVer);
    }

    addUpdateNodeIntoHbMsg(*pTask, pMsg);
    taosArrayPush(pMsg->pTaskStatus, &entry);
    if (!hasMnodeEpset) {
      epsetAssign(&epset, &(*pTask)->info.mnodeEpset);
      hasMnodeEpset = true;
    }
  }

  pMsg->numOfTasks = taosArrayGetSize(pMsg->pTaskStatus);

  if (hasMnodeEpset) {
    pInfo->msgSendTs = taosGetTimestampMs();
    doSendHbMsgInfo(pMsg, pMeta, &epset);
  } else {
    stDebug("vgId:%d no tasks or no mnd epset, not send stream hb to mnode", pMeta->vgId);
    tCleanupStreamHbMsg(&pInfo->hbMsg);
    pInfo->msgSendTs = -1;
  }

  return TSDB_CODE_SUCCESS;
}

void streamMetaHbToMnode(void* param, void* tmrId) {
  int64_t rid = *(int64_t*)param;

  SStreamMeta* pMeta = taosAcquireRef(streamMetaId, rid);
  if (pMeta == NULL) {
    stError("invalid rid:%" PRId64 " failed to acquired stream-meta", rid);
    return;
  }

  // need to stop, stop now
  if (pMeta->pHbInfo->stopFlag == STREAM_META_WILL_STOP) {  // todo refactor: not need this now, use closeFlag in Meta
    pMeta->pHbInfo->stopFlag = STREAM_META_OK_TO_STOP;
    stDebug("vgId:%d jump out of meta timer", pMeta->vgId);
    taosReleaseRef(streamMetaId, rid);
    return;
  }

  // not leader not send msg
  if (pMeta->role != NODE_ROLE_LEADER) {
    stInfo("vgId:%d role:%d not leader not send hb to mnode", pMeta->vgId, pMeta->role);
    taosReleaseRef(streamMetaId, rid);
    pMeta->pHbInfo->hbStart = 0;
    return;
  }

  // set the hb start time
  if (pMeta->pHbInfo->hbStart == 0) {
    pMeta->pHbInfo->hbStart = taosGetTimestampMs();
  }

  if (!waitForEnoughDuration(pMeta->pHbInfo)) {
    taosTmrReset(streamMetaHbToMnode, META_HB_CHECK_INTERVAL, param, streamTimer, &pMeta->pHbInfo->hbTmr);
    taosReleaseRef(streamMetaId, rid);
    return;
  }

  streamMetaRLock(pMeta);
  streamMetaSendHbHelper(pMeta);
  streamMetaRUnLock(pMeta);

  taosTmrReset(streamMetaHbToMnode, META_HB_CHECK_INTERVAL, param, streamTimer, &pMeta->pHbInfo->hbTmr);
  taosReleaseRef(streamMetaId, rid);
}

SMetaHbInfo* createMetaHbInfo(int64_t* pRid) {
  SMetaHbInfo* pInfo = taosMemoryCalloc(1, sizeof(SMetaHbInfo));
  if (pInfo == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return pInfo;
  }

  pInfo->hbTmr = taosTmrStart(streamMetaHbToMnode, META_HB_CHECK_INTERVAL, pRid, streamTimer);
  pInfo->tickCounter = 0;
  pInfo->stopFlag = 0;
  pInfo->msgSendTs = -1;
  pInfo->hbCount = 0;
  return pInfo;
}

void* destroyMetaHbInfo(SMetaHbInfo* pInfo) {
  if (pInfo != NULL) {
    tCleanupStreamHbMsg(&pInfo->hbMsg);

    if (pInfo->hbTmr != NULL) {
      taosTmrStop(pInfo->hbTmr);
      pInfo->hbTmr = NULL;
    }

    taosMemoryFree(pInfo);
  }

  return NULL;
}

void streamMetaWaitForHbTmrQuit(SStreamMeta* pMeta) {
  // wait for the stream meta hb function stopping
  if (pMeta->role == NODE_ROLE_LEADER) {
    pMeta->pHbInfo->stopFlag = STREAM_META_WILL_STOP;
    while (pMeta->pHbInfo->stopFlag != STREAM_META_OK_TO_STOP) {
      taosMsleep(100);
      stDebug("vgId:%d wait for meta to stop timer", pMeta->vgId);
    }
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
  stDebug("vgId:%d process hbMsg rsp, msgId:%d rsp confirmed", pMeta->vgId, pRsp->msgId);
  SMetaHbInfo* pInfo = pMeta->pHbInfo;

  streamMetaWLock(pMeta);

  // current waiting rsp recved
  if (pRsp->msgId == pInfo->hbCount) {
    tCleanupStreamHbMsg(&pInfo->hbMsg);
    stDebug("vgId:%d hbMsg msgId:%d sendTs:%" PRId64 " recved confirmed", pMeta->vgId, pRsp->msgId, pInfo->msgSendTs);

    pInfo->hbCount += 1;
    pInfo->msgSendTs = -1;
  } else {
    stWarn("vgId:%d recv expired hb rsp, msgId:%d, discarded", pMeta->vgId, pRsp->msgId);
  }

  streamMetaWUnLock(pMeta);
  return TSDB_CODE_SUCCESS;
}