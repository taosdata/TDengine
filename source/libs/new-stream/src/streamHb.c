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

static int32_t streamHbSendRequestMsg(SStreamHbMsg* pMsg, SEpSet* pEpset) {
  int32_t code = 0;
  int32_t tlen = 0;

  tEncodeSize(tEncodeStreamHbMsg, pMsg, tlen, code);
  if (code < 0) {
    stError("vgId:%d encode stream hb msg failed, code:%s", tstrerror(code));
    return TSDB_CODE_FAILED;
  }

  void* buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    stError("vgId:%d encode stream hb msg failed, code:%s", tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    return TSDB_CODE_FAILED;
  }

  SEncoder encoder;
  tEncoderInit(&encoder, buf, tlen);
  if ((code = tEncodeStreamHbMsg(&encoder, pMsg)) < 0) {
    rpcFreeCont(buf);
    tEncoderClear(&encoder);
    stError("vgId:%d encode stream hb msg failed, code:%s", tstrerror(code));
    return TSDB_CODE_FAILED;
  }
  tEncoderClear(&encoder);

  stDebug("vgId:%d send hb to mnode, numOfTasks:%d msgId:%d", pMsg->numOfTasks, pMsg->msgId);

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

int32_t streamHbBuildRequestMsg(SStreamHbMsg* pMsg) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  
  pMsg.dnodeId = gStreamMgmt.dnodeId;
  pMsg.snodeId = gStreamMgmt.snodeId;
  pMsg.streamGId = streamAddFetchStreamGrpId();
  pMsg.pVgLeaders = taosArrayDup(gStreamMgmt.vgroupLeaders, NULL);
  TSDB_CHECK_NULL(pMsg.pVgLeaders, code, lino, _exit, terrno);
  
  TAOS_CHECK_EXIT(streamBuildStreamsStatus(pMsg->pStreamStatus, pMsg->streamGId));

_exit:

  return code;
}

void streamHbStart(void* param, void* tmrId) {
  int32_t code = 0;
  SStreamHbMsg reqMsg = {0};
  
  TAOS_CHECK_EXIT(streamHbBuildRequestMsg(&reqMsg));

  TAOS_CHECK_EXIT(streamHbSendRequestMsg(reqMsg));


_exit:

  streamTmrStart(streamHbStart, STREAM_HB_INTERVAL_MS, NULL, gStreamMgmt.timer, &gStreamMgmt.hb.hbTmr, 0, "stream-hb");
  
  if (code) {
    stError("vgId:%d in meta timer, failed to release the meta rid:%" PRId64, vgId, rid);
  }
}

int32_t streamHbInit(int64_t* pRid, SStreamHbInfo* pHb) {
  streamTmrStart(streamHbStart, STREAM_HB_INTERVAL_MS, NULL, gStreamMgmt.timer, &pHb->hbTmr, 0, "stream-hb");
  
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

int32_t streamHbProcessRspMsg(SMStreamHbRspMsg* pRsp) {
  int32_t      code = 0;

  if (pRsp->deploy.taskList) {
    TAOS_CHECK_EXIT(streamMgmtDeployTasks(&pRsp->deploy));
  }

  if (pRsp->start.taskList) {
    TAOS_CHECK_EXIT(streamMgmtStartTasks(&pRsp->start));
  }

  if (pRsp->undeploy.taskList) {
    TAOS_CHECK_EXIT(streamMgmtUndeployTasks(&pRsp->undeploy));
  }

_exit:

  stDebug("vgId:%d process hbMsg rsp, msgId:%d rsp confirmed", pRsp->msgId);

  return code;
}
