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

// clang-format off
#define sndError(...) do {  if (sndDebugFlag & DEBUG_ERROR) {taosPrintLog("SND ERROR ", DEBUG_ERROR, sndDebugFlag, __VA_ARGS__);}} while (0)
#define sndInfo(...)  do {   if (sndDebugFlag & DEBUG_INFO) { taosPrintLog("SND INFO ", DEBUG_INFO, sndDebugFlag, __VA_ARGS__);}} while (0)
#define sndDebug(...) do {  if (sndDebugFlag & DEBUG_DEBUG) { taosPrintLog("SND ", DEBUG_DEBUG, sndDebugFlag, __VA_ARGS__);}} while (0)
// clang-format on

int32_t sndBuildStreamTask(SSnode *pSnode, SStreamTask *pTask, int64_t nextProcessVer) {
  if (!(pTask->info.taskLevel == TASK_LEVEL__AGG && taosArrayGetSize(pTask->upstreamInfo.pList) != 0)) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = streamTaskInit(pTask, pSnode->pMeta, &pSnode->msgCb, nextProcessVer);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pTask->pBackend = NULL;
  streamTaskOpenAllUpstreamInput(pTask);

  streamTaskResetUpstreamStageInfo(pTask);
  if (streamSetupScheduleTrigger(pTask) != 0) {
    sndError("failed to setup schedule trigger for task:%s", pTask->id.idStr);
  }

  SCheckpointInfo *pChkInfo = &pTask->chkInfo;
  tqSetRestoreVersionInfo(pTask);

  char *p = streamTaskGetStatus(pTask).name;
  if (pTask->info.fillHistory) {
    sndInfo("vgId:%d build stream task, s-task:%s, checkpointId:%" PRId64 " checkpointVer:%" PRId64
            " nextProcessVer:%" PRId64
            " child id:%d, level:%d, status:%s fill-history:%d, related stream task:0x%x trigger:%" PRId64 " ms",
            SNODE_HANDLE, pTask->id.idStr, pChkInfo->checkpointId, pChkInfo->checkpointVer, pChkInfo->nextProcessVer,
            pTask->info.selfChildId, pTask->info.taskLevel, p, pTask->info.fillHistory,
            (int32_t)pTask->streamTaskId.taskId, pTask->info.delaySchedParam);
  } else {
    sndInfo("vgId:%d build stream task, s-task:%s, checkpointId:%" PRId64 " checkpointVer:%" PRId64
            " nextProcessVer:%" PRId64
            " child id:%d, level:%d, status:%s fill-history:%d, related fill-task:0x%x trigger:%" PRId64 " ms",
            SNODE_HANDLE, pTask->id.idStr, pChkInfo->checkpointId, pChkInfo->checkpointVer, pChkInfo->nextProcessVer,
            pTask->info.selfChildId, pTask->info.taskLevel, p, pTask->info.fillHistory,
            (int32_t)pTask->hTaskInfo.id.taskId, pTask->info.delaySchedParam);
  }
  return 0;
}

SSnode *sndOpen(const char *path, const SSnodeOpt *pOption) {
  int32_t code = 0;
  SSnode *pSnode = taosMemoryCalloc(1, sizeof(SSnode));
  if (pSnode == NULL) {
    return NULL;
  }

  stopRsync();
  code = startRsync();
  if (code != 0) {
    terrno = code;
    goto FAIL;
  }

  pSnode->msgCb = pOption->msgCb;
  code = streamMetaOpen(path, pSnode, (FTaskBuild *)sndBuildStreamTask, tqExpandStreamTask, SNODE_HANDLE,
                        taosGetTimestampMs(), tqStartTaskCompleteCallback, &pSnode->pMeta);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    goto FAIL;
  }

  streamMetaLoadAllTasks(pSnode->pMeta);
  return pSnode;

FAIL:
  taosMemoryFree(pSnode);
  return NULL;
}

int32_t sndInit(SSnode *pSnode) {
  if (streamTaskSchedTask(&pSnode->msgCb, pSnode->pMeta->vgId, 0, 0, STREAM_EXEC_T_START_ALL_TASKS) != 0) {
    sndError("failed to start all tasks");
  }
  return 0;
}

void sndClose(SSnode *pSnode) {
  stopRsync();
  streamMetaNotifyClose(pSnode->pMeta);
  if (streamMetaCommit(pSnode->pMeta) != 0) {
    sndError("failed to commit stream meta");
  }
  streamMetaClose(pSnode->pMeta);
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
    case TDMT_VND_STREAM_TASK_CHECK:
      return tqStreamTaskProcessCheckReq(pSnode->pMeta, pMsg);
    case TDMT_VND_STREAM_TASK_CHECK_RSP:
      return tqStreamTaskProcessCheckRsp(pSnode->pMeta, pMsg, true);
    case TDMT_STREAM_TASK_CHECKPOINT_READY:
      return tqStreamTaskProcessCheckpointReadyMsg(pSnode->pMeta, pMsg);
    case TDMT_MND_STREAM_HEARTBEAT_RSP:
      return tqStreamProcessStreamHbRsp(pSnode->pMeta, pMsg);
    case TDMT_MND_STREAM_REQ_CHKPT_RSP:
      return tqStreamProcessReqCheckpointRsp(pSnode->pMeta, pMsg);
    case TDMT_STREAM_TASK_CHECKPOINT_READY_RSP:
      return tqStreamProcessCheckpointReadyRsp(pSnode->pMeta, pMsg);
    case TDMT_MND_STREAM_CHKPT_REPORT_RSP:
      return tqStreamProcessChkptReportRsp(pSnode->pMeta, pMsg);
    case TDMT_STREAM_RETRIEVE_TRIGGER:
      return tqStreamTaskProcessRetrieveTriggerReq(pSnode->pMeta, pMsg);
    case TDMT_STREAM_RETRIEVE_TRIGGER_RSP:
      return tqStreamTaskProcessRetrieveTriggerRsp(pSnode->pMeta, pMsg);
    default:
      sndError("invalid snode msg:%d", pMsg->msgType);
      return TSDB_CODE_INVALID_MSG;
  }
  return 0;
}

int32_t sndProcessWriteMsg(SSnode *pSnode, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  switch (pMsg->msgType) {
    case TDMT_STREAM_TASK_DEPLOY: {
      void   *pReq = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
      int32_t len = pMsg->contLen - sizeof(SMsgHead);
      return tqStreamTaskProcessDeployReq(pSnode->pMeta, &pSnode->msgCb, pMsg->info.conn.applyIndex, pReq, len, true,
                                          true);
    }

    case TDMT_STREAM_TASK_DROP:
      return tqStreamTaskProcessDropReq(pSnode->pMeta, pMsg->pCont, pMsg->contLen);
    case TDMT_VND_STREAM_TASK_UPDATE:
      return tqStreamTaskProcessUpdateReq(pSnode->pMeta, &pSnode->msgCb, pMsg, true);
    case TDMT_VND_STREAM_TASK_RESET:
      return tqStreamTaskProcessTaskResetReq(pSnode->pMeta, pMsg->pCont);
    case TDMT_STREAM_TASK_PAUSE:
      return tqStreamTaskProcessTaskPauseReq(pSnode->pMeta, pMsg->pCont);
    case TDMT_STREAM_TASK_RESUME:
      return tqStreamTaskProcessTaskResumeReq(pSnode->pMeta, pMsg->info.conn.applyIndex, pMsg->pCont, false);
    case TDMT_STREAM_TASK_UPDATE_CHKPT:
      return tqStreamTaskProcessUpdateCheckpointReq(pSnode->pMeta, true, pMsg->pCont);
    case TDMT_STREAM_CONSEN_CHKPT:
      return tqStreamTaskProcessConsenChkptIdReq(pSnode->pMeta, pMsg);
    default:
      return TSDB_CODE_INVALID_MSG;
  }
  return 0;
}
