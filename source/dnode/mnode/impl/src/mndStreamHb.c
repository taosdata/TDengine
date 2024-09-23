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

#include "mndStream.h"
#include "mndTrans.h"

typedef struct SFailedCheckpointInfo {
  int64_t streamUid;
  int64_t checkpointId;
  int32_t transId;
} SFailedCheckpointInfo;

static int32_t mndStreamSendUpdateChkptInfoMsg(SMnode *pMnode);
static int32_t mndSendDropOrphanTasksMsg(SMnode *pMnode, SArray *pList);
static int32_t mndSendResetFromCheckpointMsg(SMnode *pMnode, int64_t streamId, int32_t transId);
static void    updateStageInfo(STaskStatusEntry *pTaskEntry, int64_t stage);
static void    addIntoFailedChkptList(SArray *pList, const SFailedCheckpointInfo *pInfo);
static int32_t setNodeEpsetExpiredFlag(const SArray *pNodeList);
static int32_t suspendAllStreams(SMnode *pMnode, SRpcHandleInfo *info);
static bool    validateHbMsg(const SArray *pNodeList, int32_t vgId);
static void    cleanupAfterProcessHbMsg(SStreamHbMsg *pReq, SArray *pFailedChkptList, SArray *pOrphanTasks);
static void    doSendHbMsgRsp(int32_t code, SRpcHandleInfo *pRpcInfo, int32_t vgId, int32_t msgId);
static void    checkforOrphanTask(SMnode* pMnode, STaskStatusEntry* p, SArray* pOrphanTasks);

void updateStageInfo(STaskStatusEntry *pTaskEntry, int64_t stage) {
  int32_t numOfNodes = taosArrayGetSize(execInfo.pNodeList);
  for (int32_t j = 0; j < numOfNodes; ++j) {
    SNodeEntry *pNodeEntry = taosArrayGet(execInfo.pNodeList, j);
    if (pNodeEntry == NULL) {
      continue;
    }

    if (pNodeEntry->nodeId == pTaskEntry->nodeId) {
      mInfo("vgId:%d stage updated from %" PRId64 " to %" PRId64 ", nodeUpdate trigger by s-task:0x%" PRIx64,
            pTaskEntry->nodeId, pTaskEntry->stage, stage, pTaskEntry->id.taskId);

      pNodeEntry->stageUpdated = true;
      pTaskEntry->stage = stage;
      break;
    }
  }
}

void addIntoFailedChkptList(SArray *pList, const SFailedCheckpointInfo *pInfo) {
  int32_t num = taosArrayGetSize(pList);
  for (int32_t i = 0; i < num; ++i) {
    SFailedCheckpointInfo *p = taosArrayGet(pList, i);
    if (p && (p->transId == pInfo->transId)) {
      return;
    }
  }

  void* p = taosArrayPush(pList, pInfo);
  if (p == NULL) {
    mError("failed to push failed checkpoint info checkpointId:%" PRId64 " in list", pInfo->checkpointId);
  }
}

int32_t mndCreateStreamResetStatusTrans(SMnode *pMnode, SStreamObj *pStream) {
  STrans *pTrans = NULL;
  int32_t code = doCreateTrans(pMnode, pStream, NULL, TRN_CONFLICT_NOTHING, MND_STREAM_TASK_RESET_NAME,
                               " reset from failed checkpoint", &pTrans);
  if (pTrans == NULL || code) {
    sdbRelease(pMnode->pSdb, pStream);
    return terrno;
  }

  code = mndStreamRegisterTrans(pTrans, MND_STREAM_TASK_RESET_NAME, pStream->uid);
  if (code) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  code = mndStreamSetResetTaskAction(pMnode, pTrans, pStream);
  if (code) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  code = mndPersistTransLog(pStream, pTrans, SDB_STATUS_READY);
  if (code != TSDB_CODE_SUCCESS) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  code = mndTransPrepare(pMnode, pTrans);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("trans:%d, failed to prepare update stream trans since %s", pTrans->id, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);

  if (code == 0) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }
  return code;
}

int32_t mndSendResetFromCheckpointMsg(SMnode *pMnode, int64_t streamId, int32_t transId) {
  int32_t size = sizeof(SStreamTaskResetMsg);

  int32_t num = taosArrayGetSize(execInfo.pKilledChkptTrans);
  for(int32_t i = 0; i < num; ++i) {
    SStreamTaskResetMsg* p = taosArrayGet(execInfo.pKilledChkptTrans, i);
    if (p == NULL) {
      continue;
    }

    if (p->transId == transId && p->streamId == streamId) {
      mDebug("already reset stream:0x%" PRIx64 ", not send reset-msg again for transId:%d", streamId, transId);
      return TSDB_CODE_SUCCESS;
    }
  }

  if (num >= 10) {
    taosArrayRemove(execInfo.pKilledChkptTrans, 0);  // remove this first, append new reset trans in the tail
  }

  SStreamTaskResetMsg p = {.streamId = streamId, .transId = transId};

  void *px = taosArrayPush(execInfo.pKilledChkptTrans, &p);
  if (px == NULL) {
    mError("failed to push reset-msg trans:%d into the killed chkpt trans list, size:%d", transId, num - 1);
    return terrno;
  }

  SStreamTaskResetMsg *pReq = rpcMallocCont(size);
  if (pReq == NULL) {
    return terrno;
  }

  pReq->streamId = streamId;
  pReq->transId = transId;

  SRpcMsg rpcMsg = {.msgType = TDMT_MND_STREAM_TASK_RESET, .pCont = pReq, .contLen = size};
  int32_t code = tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
  if (code) {
    mError("failed to put reset-task msg into write queue, code:%s", tstrerror(code));
  } else {
    mDebug("send reset task status msg for transId:%d succ", transId);
  }

  return code;
}

int32_t mndStreamSendUpdateChkptInfoMsg(SMnode *pMnode) {  // here reuse the doCheckpointmsg
  int32_t size = sizeof(SMStreamDoCheckpointMsg);
  void   *pMsg = rpcMallocCont(size);
  if (pMsg == NULL) {
    return terrno;
  }

  SRpcMsg rpcMsg = {.msgType = TDMT_MND_STREAM_UPDATE_CHKPT_EVT, .pCont = pMsg, .contLen = size};
  int32_t code = tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
  if (code) {
    mError("failed to put update-checkpoint-info msg into write queue, code:%s", tstrerror(code));
  } else {
    mDebug("send update checkpoint-info msg succ");
  }

  return code;
}

int32_t mndSendDropOrphanTasksMsg(SMnode *pMnode, SArray *pList) {
  SMStreamDropOrphanMsg msg = {.pList = pList};

  int32_t num = taosArrayGetSize(pList);
  int32_t contLen = tSerializeDropOrphanTaskMsg(NULL, 0, &msg);
  if (contLen <= 0) {
    return terrno;
  }

  void *pReq = rpcMallocCont(contLen);
  if (pReq == NULL) {
    return terrno;
  }

  int32_t code = tSerializeDropOrphanTaskMsg(pReq, contLen, &msg);
  if (code <= 0) {
    mError("failed to serialize the drop orphan task msg, code:%s", tstrerror(code));
  }

  SRpcMsg rpcMsg = {.msgType = TDMT_MND_STREAM_DROP_ORPHANTASKS, .pCont = pReq, .contLen = contLen};
  code = tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
  if (code) {
    mError("failed to put drop-orphan task msg into write queue, code:%s", tstrerror(code));
  } else {
    mDebug("send drop %d orphan tasks msg succ", num);
  }

  return code;
}

int32_t mndProcessResetStatusReq(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  int32_t     code = TSDB_CODE_SUCCESS;
  SStreamObj *pStream = NULL;

  SStreamTaskResetMsg* pMsg = pReq->pCont;
  mndKillTransImpl(pMnode, pMsg->transId, "");

  streamMutexLock(&execInfo.lock);
  code = mndResetChkptReportInfo(execInfo.pChkptStreams, pMsg->streamId);   // do thing if failed
  streamMutexUnlock(&execInfo.lock);

  code = mndGetStreamObj(pMnode, pMsg->streamId, &pStream);
  if (pStream == NULL || code != 0) {
    code = TSDB_CODE_STREAM_TASK_NOT_EXIST;
    mError("failed to acquire the streamObj:0x%" PRIx64 " to reset checkpoint, may have been dropped", pStream->uid);
  } else {
    code = mndStreamTransConflictCheck(pMnode, pStream->uid, MND_STREAM_TASK_RESET_NAME, false);
    if (code) {
      mError("stream:%s other trans exists in DB:%s, dstTable:%s failed to start reset-status trans", pStream->name,
             pStream->sourceDb, pStream->targetSTbName);
    } else {
      mDebug("stream:%s (0x%" PRIx64 ") reset checkpoint procedure, transId:%d, create reset trans", pStream->name,
             pStream->uid, pMsg->transId);
      code = mndCreateStreamResetStatusTrans(pMnode, pStream);
    }
  }

  mndReleaseStream(pMnode, pStream);
  return code;
}

int32_t setNodeEpsetExpiredFlag(const SArray *pNodeList) {
  int32_t num = taosArrayGetSize(pNodeList);
  mInfo("set node expired for %d nodes", num);

  for (int k = 0; k < num; ++k) {
    int32_t *pVgId = taosArrayGet(pNodeList, k);
    if (pVgId == NULL) {
      continue;
    }

    mInfo("set node expired for nodeId:%d, total:%d", *pVgId, num);

    bool    setFlag = false;
    int32_t numOfNodes = taosArrayGetSize(execInfo.pNodeList);

    for (int i = 0; i < numOfNodes; ++i) {
      SNodeEntry *pNodeEntry = taosArrayGet(execInfo.pNodeList, i);
      if ((pNodeEntry) && (pNodeEntry->nodeId == *pVgId)) {
        mInfo("vgId:%d expired for some stream tasks, needs update nodeEp", *pVgId);
        pNodeEntry->stageUpdated = true;
        setFlag = true;
        break;
      }
    }

    if (!setFlag) {
      mError("failed to set nodeUpdate flag, nodeId:%d not exists in nodelist", *pVgId);
      return TSDB_CODE_FAILED;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t suspendAllStreams(SMnode *pMnode, SRpcHandleInfo *info) {
  SSdb       *pSdb = pMnode->pSdb;
  SStreamObj *pStream = NULL;
  void       *pIter = NULL;
  int32_t     code = 0;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) break;

    if (pStream->status != STREAM_STATUS__PAUSE) {
      SMPauseStreamReq reqPause = {0};
      strcpy(reqPause.name, pStream->name);
      reqPause.igNotExists = 1;

      int32_t contLen = tSerializeSMPauseStreamReq(NULL, 0, &reqPause);
      void   *pHead = rpcMallocCont(contLen);
      if (pHead == NULL) {
        code = terrno;
        sdbRelease(pSdb, pStream);
        continue;
      }

      code = tSerializeSMPauseStreamReq(pHead, contLen, &reqPause);
      if (code) {
        sdbRelease(pSdb, pStream);
        continue;
      }

      SRpcMsg rpcMsg = {
          .msgType = TDMT_MND_PAUSE_STREAM,
          .pCont = pHead,
          .contLen = contLen,
          .info = *info,
      };

      code = tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
      mInfo("receive pause stream:%s, %s, %" PRId64 ", because grant expired, code:%s", pStream->name, reqPause.name,
            pStream->uid, tstrerror(code));
    }

    sdbRelease(pSdb, pStream);
  }
  return code;
}

int32_t mndProcessStreamHb(SRpcMsg *pReq) {
  SMnode      *pMnode = pReq->info.node;
  SStreamHbMsg req = {0};
  SArray      *pFailedChkpt = NULL;
  SArray      *pOrphanTasks = NULL;
  int32_t      code = 0;

  if ((code = grantCheckExpire(TSDB_GRANT_STREAMS)) < 0) {
    if (suspendAllStreams(pMnode, &pReq->info) < 0) {
      return code;
    }
  }

  SDecoder decoder = {0};
  tDecoderInit(&decoder, pReq->pCont, pReq->contLen);

  if (tDecodeStreamHbMsg(&decoder, &req) < 0) {
    tCleanupStreamHbMsg(&req);
    tDecoderClear(&decoder);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }
  tDecoderClear(&decoder);

  mDebug("receive stream-meta hb from vgId:%d, active numOfTasks:%d, HbMsgId:%d, HbMsgTs:%" PRId64, req.vgId,
         req.numOfTasks, req.msgId, req.ts);

  pFailedChkpt = taosArrayInit(4, sizeof(SFailedCheckpointInfo));
  pOrphanTasks = taosArrayInit(4, sizeof(SOrphanTask));
  if (pFailedChkpt == NULL || pOrphanTasks == NULL) {
    taosArrayDestroy(pFailedChkpt);
    taosArrayDestroy(pOrphanTasks);
    TAOS_RETURN(terrno);
  }

  streamMutexLock(&execInfo.lock);

  mndInitStreamExecInfo(pMnode, &execInfo);
  if (!validateHbMsg(execInfo.pNodeList, req.vgId)) {
    mError("vgId:%d not exists in nodeList buf, discarded", req.vgId);

    doSendHbMsgRsp(terrno, &pReq->info, req.vgId, req.msgId);

    streamMutexUnlock(&execInfo.lock);
    cleanupAfterProcessHbMsg(&req, pFailedChkpt, pOrphanTasks);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  for(int32_t i = 0; i < taosArrayGetSize(execInfo.pNodeList); ++i) {
    SNodeEntry* pEntry = taosArrayGet(execInfo.pNodeList, i);
    if (pEntry == NULL) {
      continue;
    }

    if (pEntry->nodeId != req.vgId) {
      continue;
    }

    if ((pEntry->lastHbMsgId == req.msgId) && (pEntry->lastHbMsgTs == req.ts)) {
      mError("vgId:%d HbMsgId:%d already handled, bh msg discard", pEntry->nodeId, req.msgId);

      terrno = TSDB_CODE_INVALID_MSG;
      doSendHbMsgRsp(terrno, &pReq->info, req.vgId, req.msgId);

      streamMutexUnlock(&execInfo.lock);
      cleanupAfterProcessHbMsg(&req, pFailedChkpt, pOrphanTasks);
      return terrno;
    } else {
      pEntry->lastHbMsgId = req.msgId;
      pEntry->lastHbMsgTs = req.ts;
    }
  }

  int32_t numOfUpdated = taosArrayGetSize(req.pUpdateNodes);
  if (numOfUpdated > 0) {
    mDebug("%d stream node(s) need updated from hbMsg(vgId:%d)", numOfUpdated, req.vgId);
    int32_t unused = setNodeEpsetExpiredFlag(req.pUpdateNodes);
  }

  bool snodeChanged = false;
  for (int32_t i = 0; i < req.numOfTasks; ++i) {
    STaskStatusEntry *p = taosArrayGet(req.pTaskStatus, i);
    if (p == NULL) {
      continue;
    }

    STaskStatusEntry *pTaskEntry = taosHashGet(execInfo.pTaskMap, &p->id, sizeof(p->id));
    if (pTaskEntry == NULL) {
      checkforOrphanTask(pMnode, p, pOrphanTasks);
      continue;
    }

    STaskCkptInfo *pChkInfo = &p->checkpointInfo;
    if (pChkInfo->consensusChkptId != 0) {
      SRestoreCheckpointInfo cp = {
          .streamId = p->id.streamId,
          .taskId = p->id.taskId,
          .checkpointId = p->checkpointInfo.latestId,
          .startTs = pChkInfo->consensusTs,
      };

      SStreamObj *pStream = NULL;
      code = mndGetStreamObj(pMnode, p->id.streamId, &pStream);
      if (code) {
        mError("stream:0x%" PRIx64 " not exist, failed to handle consensus checkpoint-info req for task:0x%x, code:%s",
               p->id.streamId, (int32_t)p->id.taskId, tstrerror(code));
        continue;
      }

      int32_t numOfTasks = mndGetNumOfStreamTasks(pStream);

      SCheckpointConsensusInfo *pInfo = NULL;
      code = mndGetConsensusInfo(execInfo.pStreamConsensus, p->id.streamId, numOfTasks, &pInfo);
      if (code == 0) {
        mndAddConsensusTasks(pInfo, &cp);
      } else {
        mError("failed to get consensus checkpoint-info for stream:0x%" PRIx64, p->id.streamId);
      }

      mndReleaseStream(pMnode, pStream);
    }

    if (pTaskEntry->stage != p->stage && pTaskEntry->stage != -1) {
      updateStageInfo(pTaskEntry, p->stage);
      if (pTaskEntry->nodeId == SNODE_HANDLE) {
        snodeChanged = true;
      }
    } else {
      streamTaskStatusCopy(pTaskEntry, p);

      if ((pChkInfo->activeId != 0) && pChkInfo->failed) {
        mError("stream task:0x%" PRIx64 " checkpointId:%" PRId64 " transId:%d failed, kill it", p->id.taskId,
               pChkInfo->activeId, pChkInfo->activeTransId);

        SFailedCheckpointInfo info = {
            .transId = pChkInfo->activeTransId, .checkpointId = pChkInfo->activeId, .streamUid = p->id.streamId};
        addIntoFailedChkptList(pFailedChkpt, &info);

        // remove failed trans from pChkptStreams
        code = mndResetChkptReportInfo(execInfo.pChkptStreams, p->id.streamId);
        if (code) {
          mError("failed to remove stream:0x%"PRIx64" in checkpoint stream list", p->id.streamId);
        }
      }
    }

    if (p->status != TASK_STATUS__READY) {
      mDebug("received s-task:0x%" PRIx64 " not in ready status:%s", p->id.taskId, streamTaskGetStatusStr(p->status));
    }
  }

  // current checkpoint is failed, rollback from the checkpoint trans
  // kill the checkpoint trans and then set all tasks status to be normal
  if (taosArrayGetSize(pFailedChkpt) > 0) {
    bool allReady = true;

    if (pMnode != NULL) {
      SArray *p = NULL;
      code = mndTakeVgroupSnapshot(pMnode, &allReady, &p);
      taosArrayDestroy(p);
      if (code) {
        mError("failed to get the vgroup snapshot, ignore it and continue");
      }
    } else {
      allReady = false;
    }

    if (allReady || snodeChanged) {
      // if the execInfo.activeCheckpoint == 0, the checkpoint is restoring from wal
      for (int32_t i = 0; i < taosArrayGetSize(pFailedChkpt); ++i) {
        SFailedCheckpointInfo *pInfo = taosArrayGet(pFailedChkpt, i);
        if (pInfo == NULL) {
          continue;
        }

        mInfo("checkpointId:%" PRId64 " transId:%d failed, issue task-reset trans to reset all tasks status",
              pInfo->checkpointId, pInfo->transId);

        code = mndSendResetFromCheckpointMsg(pMnode, pInfo->streamUid, pInfo->transId);
        if (code) {
          mError("failed to create reset task trans, code:%s", tstrerror(code));
        }
      }
    } else {
      mInfo("not all vgroups are ready, wait for next HB from stream tasks to reset the task status");
    }
  }

  // handle the orphan tasks that are invalid but not removed in some vnodes or snode due to some unknown errors.
  if (taosArrayGetSize(pOrphanTasks) > 0) {
    code = mndSendDropOrphanTasksMsg(pMnode, pOrphanTasks);
    if (code) {
      mError("failed to send drop orphan tasks msg, code:%s, try next time", tstrerror(code));
    }
  }

  if (pMnode != NULL) {  // make sure that the unit test case can work
    code = mndStreamSendUpdateChkptInfoMsg(pMnode);
    if (code) {
      mError("failed to send update checkpointInfo msg, code:%s, try next time", tstrerror(code));
    }
  }

  streamMutexUnlock(&execInfo.lock);

  doSendHbMsgRsp(TSDB_CODE_SUCCESS, &pReq->info, req.vgId, req.msgId);
  cleanupAfterProcessHbMsg(&req, pFailedChkpt, pOrphanTasks);

  return code;
}

bool validateHbMsg(const SArray *pNodeList, int32_t vgId) {
  for (int32_t i = 0; i < taosArrayGetSize(pNodeList); ++i) {
    SNodeEntry *pEntry = taosArrayGet(pNodeList, i);
    if ((pEntry) && (pEntry->nodeId == vgId)) {
      return true;
    }
  }

  return false;
}

void cleanupAfterProcessHbMsg(SStreamHbMsg *pReq, SArray *pFailedChkptList, SArray *pOrphanTasks) {
  tCleanupStreamHbMsg(pReq);
  taosArrayDestroy(pFailedChkptList);
  taosArrayDestroy(pOrphanTasks);
}

void doSendHbMsgRsp(int32_t code, SRpcHandleInfo *pRpcInfo, int32_t vgId, int32_t msgId) {
  SRpcMsg rsp = {.code = code, .info = *pRpcInfo, .contLen = sizeof(SMStreamHbRspMsg)};
  rsp.pCont = rpcMallocCont(rsp.contLen);

  SMStreamHbRspMsg *pMsg = rsp.pCont;
  pMsg->head.vgId = htonl(vgId);
  pMsg->msgId = msgId;

  tmsgSendRsp(&rsp);
  pRpcInfo->handle = NULL;  // disable auto rsp
}

void checkforOrphanTask(SMnode* pMnode, STaskStatusEntry* p, SArray* pOrphanTasks) {
  SStreamObj *pStream = NULL;

  int32_t code = mndGetStreamObj(pMnode, p->id.streamId, &pStream);
  if (code) {
    mError("stream:0x%" PRIx64 " not exists, s-task:0x%" PRIx64 " not found in task list, add into orphan list",
           p->id.streamId, p->id.taskId);

    SOrphanTask oTask = {.streamId = p->id.streamId, .taskId = p->id.taskId, .nodeId = p->nodeId};
    void       *px = taosArrayPush(pOrphanTasks, &oTask);
    if (px == NULL) {
      mError("failed to put task into orphan list, taskId:0x%" PRIx64", code:%s", p->id.taskId, tstrerror(terrno));
    }
  } else {
    if (pStream != NULL) {
      mndReleaseStream(pMnode, pStream);
    }

    mError("s-task:0x%" PRIx64 " not found in task list but exists in mnode meta, data inconsistent, not drop yet",
           p->id.taskId);
  }
}