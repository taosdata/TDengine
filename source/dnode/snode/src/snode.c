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
#include "rsync.h"
#include "sndInt.h"
#include "tqCommon.h"
#include "tuuid.h"

#define sndError(...)                                                     \
  do {                                                                    \
    if (sndDebugFlag & DEBUG_ERROR) {                                     \
      taosPrintLog("SND ERROR ", DEBUG_ERROR, sndDebugFlag, __VA_ARGS__); \
    }                                                                     \
  } while (0)

#define sndInfo(...)                                                    \
  do {                                                                  \
    if (sndDebugFlag & DEBUG_INFO) {                                    \
      taosPrintLog("SND INFO ", DEBUG_INFO, sndDebugFlag, __VA_ARGS__); \
    }                                                                   \
  } while (0)

#define sndDebug(...)                                               \
  do {                                                              \
    if (sndDebugFlag & DEBUG_DEBUG) {                               \
      taosPrintLog("SND ", DEBUG_DEBUG, sndDebugFlag, __VA_ARGS__); \
    }                                                               \
  } while (0)

int32_t sndExpandTask(SSnode *pSnode, SStreamTask *pTask, int64_t nextProcessVer) {
  ASSERT(pTask->info.taskLevel == TASK_LEVEL__AGG && taosArrayGetSize(pTask->upstreamInfo.pList) != 0);
  int32_t code = streamTaskInit(pTask, pSnode->pMeta, &pSnode->msgCb, nextProcessVer);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  pTask->pBackend = NULL;

  streamTaskOpenAllUpstreamInput(pTask);

  SStreamTask *pSateTask = pTask;
  SStreamTask  task = {0};
  if (pTask->info.fillHistory) {
    task.id.streamId = pTask->streamTaskId.streamId;
    task.id.taskId = pTask->streamTaskId.taskId;
    task.pMeta = pTask->pMeta;
    pSateTask = &task;
  }

  pTask->pState = streamStateOpen(pSnode->path, pSateTask, false, -1, -1);
  if (pTask->pState == NULL) {
    sndError("s-task:%s failed to open state for task", pTask->id.idStr);
    return -1;
  } else {
    sndDebug("s-task:%s state:%p", pTask->id.idStr, pTask->pState);
  }
  

  int32_t     numOfVgroups = (int32_t)taosArrayGetSize(pTask->upstreamInfo.pList);
  SReadHandle handle = {
      .checkpointId = pTask->chkInfo.checkpointId,
      .vnode = NULL,
      .numOfVgroups = numOfVgroups,
      .pStateBackend = pTask->pState,
      .fillHistory = pTask->info.fillHistory,
      .winRange = pTask->dataRange.window,
  };
  initStreamStateAPI(&handle.api);

  pTask->exec.pExecutor = qCreateStreamExecTaskInfo(pTask->exec.qmsg, &handle, SNODE_HANDLE, pTask->id.taskId);
  ASSERT(pTask->exec.pExecutor);
  qSetTaskId(pTask->exec.pExecutor, pTask->id.taskId, pTask->id.streamId);

  streamTaskResetUpstreamStageInfo(pTask);
  streamSetupScheduleTrigger(pTask);

  SCheckpointInfo *pChkInfo = &pTask->chkInfo;
  // checkpoint ver is the kept version, handled data should be the next version.
  if (pTask->chkInfo.checkpointId != 0) {
    pTask->chkInfo.nextProcessVer = pTask->chkInfo.checkpointVer + 1;
    sndInfo("s-task:%s restore from the checkpointId:%" PRId64 " ver:%" PRId64 " nextProcessVer:%" PRId64, pTask->id.idStr,
           pChkInfo->checkpointId, pChkInfo->checkpointVer, pChkInfo->nextProcessVer);
  }

  char *p = NULL;
  streamTaskGetStatus(pTask, &p);

  if (pTask->info.fillHistory) {
    sndInfo("vgId:%d expand stream task, s-task:%s, checkpointId:%" PRId64 " checkpointVer:%" PRId64
           " nextProcessVer:%" PRId64
           " child id:%d, level:%d, status:%s fill-history:%d, related stream task:0x%x trigger:%" PRId64 " ms",
           SNODE_HANDLE, pTask->id.idStr, pChkInfo->checkpointId, pChkInfo->checkpointVer, pChkInfo->nextProcessVer,
           pTask->info.selfChildId, pTask->info.taskLevel, p, pTask->info.fillHistory,
           (int32_t)pTask->streamTaskId.taskId, pTask->info.triggerParam);
  } else {
    sndInfo("vgId:%d expand stream task, s-task:%s, checkpointId:%" PRId64 " checkpointVer:%" PRId64
           " nextProcessVer:%" PRId64
           " child id:%d, level:%d, status:%s fill-history:%d, related fill-task:0x%x trigger:%" PRId64 " ms",
           SNODE_HANDLE, pTask->id.idStr, pChkInfo->checkpointId, pChkInfo->checkpointVer, pChkInfo->nextProcessVer,
           pTask->info.selfChildId, pTask->info.taskLevel, p, pTask->info.fillHistory,
           (int32_t)pTask->hTaskInfo.id.taskId, pTask->info.triggerParam);
  }
  return 0;
}

SSnode *sndOpen(const char *path, const SSnodeOpt *pOption) {
  SSnode *pSnode = taosMemoryCalloc(1, sizeof(SSnode));
  if (pSnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  pSnode->path = taosStrdup(path);
  if (pSnode->path == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto FAIL;
  }

  pSnode->msgCb = pOption->msgCb;
  pSnode->pMeta = streamMetaOpen(path, pSnode, (FTaskExpand *)sndExpandTask, SNODE_HANDLE, taosGetTimestampMs(), tqStartTaskCompleteCallback);
  if (pSnode->pMeta == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto FAIL;
  }

  if (streamMetaLoadAllTasks(pSnode->pMeta) < 0) {
    goto FAIL;
  }

  stopRsync();
  startRsync();

  return pSnode;

FAIL:
  taosMemoryFree(pSnode->path);
  taosMemoryFree(pSnode);
  return NULL;
}

int32_t sndInit(SSnode * pSnode) {
  tqStreamTaskResetStatus(pSnode->pMeta);
  startStreamTasks(pSnode->pMeta);
  return 0;
}

void sndClose(SSnode *pSnode) {
  stopRsync();
  streamMetaNotifyClose(pSnode->pMeta);
  streamMetaCommit(pSnode->pMeta);
  streamMetaClose(pSnode->pMeta);
  taosMemoryFree(pSnode->path);
  taosMemoryFree(pSnode);
}

int32_t sndProcessStreamMsg(SSnode *pSnode, SRpcMsg *pMsg) {
  switch (pMsg->msgType) {
    case TDMT_STREAM_TASK_RUN:
      return tqStreamTaskProcessRunReq(pSnode->pMeta, pMsg, true);
    case TDMT_STREAM_TASK_DISPATCH:
      return tqStreamTaskProcessDispatchReq(pSnode->pMeta, pMsg);
    case TDMT_STREAM_TASK_DISPATCH_RSP:
      return tqStreamTaskProcessDispatchRsp(pSnode->pMeta, pMsg);
    case TDMT_STREAM_RETRIEVE:
      return tqStreamTaskProcessRetrieveReq(pSnode->pMeta, pMsg);
    case TDMT_STREAM_RETRIEVE_RSP:  // 1036
      break;
    case TDMT_VND_STREAM_SCAN_HISTORY_FINISH:
      return tqStreamTaskProcessScanHistoryFinishReq(pSnode->pMeta, pMsg);
    case TDMT_VND_STREAM_SCAN_HISTORY_FINISH_RSP:
      return tqStreamTaskProcessScanHistoryFinishRsp(pSnode->pMeta, pMsg);
    case TDMT_VND_STREAM_TASK_CHECK:
      return tqStreamTaskProcessCheckReq(pSnode->pMeta, pMsg);
    case TDMT_VND_STREAM_TASK_CHECK_RSP:
      return tqStreamTaskProcessCheckRsp(pSnode->pMeta, pMsg, true);
    case TDMT_STREAM_TASK_CHECKPOINT_READY:
      return tqStreamTaskProcessCheckpointReadyMsg(pSnode->pMeta, pMsg);
    default:
      sndError("invalid snode msg:%d", pMsg->msgType);
      ASSERT(0);
  }
  return 0;
}

int32_t sndProcessWriteMsg(SSnode *pSnode, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  switch (pMsg->msgType) {
    case TDMT_STREAM_TASK_DEPLOY: {
      void *  pReq = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
      int32_t len = pMsg->contLen - sizeof(SMsgHead);
      return tqStreamTaskProcessDeployReq(pSnode->pMeta, -1, pReq, len, true, true);
    }

    case TDMT_STREAM_TASK_DROP:
      return tqStreamTaskProcessDropReq(pSnode->pMeta, pMsg->pCont, pMsg->contLen);
    case TDMT_VND_STREAM_TASK_UPDATE:
      return tqStreamTaskProcessUpdateReq(pSnode->pMeta, &pSnode->msgCb, pMsg, true);
    case TDMT_VND_STREAM_TASK_RESET:
      return tqStreamTaskProcessTaskResetReq(pSnode->pMeta, pMsg);
    default:
      ASSERT(0);
  }
  return 0;
}