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
  tmr_h   hbTmr;
  int32_t stopFlag;
  int32_t tickCounter;
  int32_t hbCount;
  int64_t hbStart;
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
      stDebug("vgId:%d nodeId:%d added into hb update list, total:%d", pMeta->vgId, pTaskEpset->nodeId,
              (int32_t)taosArrayGetSize(pMsg->pUpdateNodes));
    }
  }

  taosArrayClear(pTask->outputInfo.pNodeEpsetUpdateList);
  taosThreadMutexUnlock(&pTask->lock);
}

int32_t streamMetaSendHbHelper(SStreamMeta* pMeta) {
  SStreamHbMsg hbMsg = {0};
  SEpSet       epset = {0};
  bool         hasMnodeEpset = false;
  int32_t      numOfTasks = streamMetaGetNumOfTasks(pMeta);

  hbMsg.vgId = pMeta->vgId;
  hbMsg.pTaskStatus = taosArrayInit(numOfTasks, sizeof(STaskStatusEntry));
  hbMsg.pUpdateNodes = taosArrayInit(numOfTasks, sizeof(int32_t));

  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId* pId = taosArrayGet(pMeta->pTaskList, i);

    STaskId id = {.streamId = pId->streamId, .taskId = pId->taskId};
    SStreamTask** pTask = taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
    if (pTask == NULL) {
      continue;
    }

    // not report the status of fill-history task
    if ((*pTask)->info.fillHistory == 1) {
      continue;
    }

    STaskStatusEntry entry = streamTaskGetStatusEntry(*pTask);

    entry.inputRate = entry.inputQUsed * 100.0 / (2 * STREAM_TASK_QUEUE_CAPACITY_IN_SIZE);
    if ((*pTask)->info.taskLevel == TASK_LEVEL__SINK) {
      entry.sinkQuota = (*pTask)->outputInfo.pTokenBucket->quotaRate;
      entry.sinkDataSize = SIZE_IN_MiB((*pTask)->execInfo.sink.dataSize);
    }

    if ((*pTask)->chkInfo.pActiveInfo->activeId != 0) {
      entry.checkpointInfo.failed = ((*pTask)->chkInfo.pActiveInfo->failedId >= (*pTask)->chkInfo.pActiveInfo->activeId) ? 1 : 0;
      entry.checkpointInfo.activeId = (*pTask)->chkInfo.pActiveInfo->activeId;
      entry.checkpointInfo.activeTransId = (*pTask)->chkInfo.pActiveInfo->transId;

      if (entry.checkpointInfo.failed) {
        stInfo("s-task:%s set kill checkpoint trans in hb, transId:%d", (*pTask)->id.idStr, (*pTask)->chkInfo.pActiveInfo->transId);
      }
    }

    if ((*pTask)->exec.pWalReader != NULL) {
      entry.processedVer = walReaderGetCurrentVer((*pTask)->exec.pWalReader) - 1;
      if (entry.processedVer < 0) {
        entry.processedVer = (*pTask)->chkInfo.processedVer;
      }

      walReaderValidVersionRange((*pTask)->exec.pWalReader, &entry.verRange.minVer, &entry.verRange.maxVer);
    }

    addUpdateNodeIntoHbMsg(*pTask, &hbMsg);
    taosArrayPush(hbMsg.pTaskStatus, &entry);
    if (!hasMnodeEpset) {
      epsetAssign(&epset, &(*pTask)->info.mnodeEpset);
      hasMnodeEpset = true;
    }
  }

  hbMsg.numOfTasks = taosArrayGetSize(hbMsg.pTaskStatus);

  if (hasMnodeEpset) {
    int32_t code = 0;
    int32_t tlen = 0;

    tEncodeSize(tEncodeStreamHbMsg, &hbMsg, tlen, code);
    if (code < 0) {
      stError("vgId:%d encode stream hb msg failed, code:%s", pMeta->vgId, tstrerror(code));
      goto _end;
    }

    void* buf = rpcMallocCont(tlen);
    if (buf == NULL) {
      stError("vgId:%d encode stream hb msg failed, code:%s", pMeta->vgId, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
      goto _end;
    }

    SEncoder encoder;
    tEncoderInit(&encoder, buf, tlen);
    if ((code = tEncodeStreamHbMsg(&encoder, &hbMsg)) < 0) {
      rpcFreeCont(buf);
      stError("vgId:%d encode stream hb msg failed, code:%s", pMeta->vgId, tstrerror(code));
      goto _end;
    }
    tEncoderClear(&encoder);

    SRpcMsg msg = {0};
    initRpcMsg(&msg, TDMT_MND_STREAM_HEARTBEAT, buf, tlen);

    pMeta->pHbInfo->hbCount += 1;
    stDebug("vgId:%d build and send hb to mnode, numOfTasks:%d total:%d", pMeta->vgId, hbMsg.numOfTasks,
            pMeta->pHbInfo->hbCount);

    tmsgSendReq(&epset, &msg);
  } else {
    stDebug("vgId:%d no tasks and no mnd epset, not send stream hb to mnode", pMeta->vgId);
  }

  _end:
  tCleanupStreamHbMsg(&hbMsg);
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

  stDebug("vgId:%d build stream task hb, leader:%d", pMeta->vgId, (pMeta->role == NODE_ROLE_LEADER));
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

  return pInfo;
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