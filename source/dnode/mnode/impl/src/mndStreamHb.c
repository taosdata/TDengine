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

static void    mndStreamStartUpdateCheckpointInfo(SMnode *pMnode);
static void    updateStageInfo(STaskStatusEntry *pTaskEntry, int64_t stage);
static void    addIntoCheckpointList(SArray *pList, const SFailedCheckpointInfo *pInfo);
static int32_t mndResetStatusFromCheckpoint(SMnode *pMnode, int64_t streamId, int32_t transId);
static int32_t setNodeEpsetExpiredFlag(const SArray *pNodeList);
static int32_t mndDropOrphanTasks(SMnode *pMnode, SArray *pList);
static int32_t suspendAllStreams(SMnode *pMnode, SRpcHandleInfo *info);
static bool    validateHbMsg(const SArray *pNodeList, int32_t vgId);
static void    cleanupAfterProcessHbMsg(SStreamHbMsg *pReq, SArray *pFailedChkptList, SArray *pOrphanTasks);
static void    doSendHbMsgRsp(int32_t code, SRpcHandleInfo *pRpcInfo, int32_t vgId, int32_t msgId);

void updateStageInfo(STaskStatusEntry *pTaskEntry, int64_t stage) {
  int32_t numOfNodes = taosArrayGetSize(execInfo.pNodeList);
  for (int32_t j = 0; j < numOfNodes; ++j) {
    SNodeEntry *pNodeEntry = taosArrayGet(execInfo.pNodeList, j);
    if (pNodeEntry->nodeId == pTaskEntry->nodeId) {
      mInfo("vgId:%d stage updated from %" PRId64 " to %" PRId64 ", nodeUpdate trigger by s-task:0x%" PRIx64,
            pTaskEntry->nodeId, pTaskEntry->stage, stage, pTaskEntry->id.taskId);

      pNodeEntry->stageUpdated = true;
      pTaskEntry->stage = stage;
      break;
    }
  }
}

void addIntoCheckpointList(SArray *pList, const SFailedCheckpointInfo *pInfo) {
  int32_t num = taosArrayGetSize(pList);
  for (int32_t i = 0; i < num; ++i) {
    SFailedCheckpointInfo *p = taosArrayGet(pList, i);
    if (p->transId == pInfo->transId) {
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
  if (code != 0) {
    mError("trans:%d, failed to prepare update stream trans since %s", pTrans->id, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);

  return TSDB_CODE_ACTION_IN_PROGRESS;
}

int32_t mndResetStatusFromCheckpoint(SMnode *pMnode, int64_t streamId, int32_t transId) {
  int32_t code = TSDB_CODE_SUCCESS;
  mndKillTransImpl(pMnode, transId, "");

  SStreamObj *pStream = NULL;
  code = mndGetStreamObj(pMnode, streamId, &pStream);
  if (pStream == NULL || code != 0) {
    code = TSDB_CODE_STREAM_TASK_NOT_EXIST;
    mError("failed to acquire the streamObj:0x%" PRIx64 " to reset checkpoint, may have been dropped", pStream->uid);
  } else {
    bool conflict = mndStreamTransConflictCheck(pMnode, pStream->uid, MND_STREAM_TASK_RESET_NAME, false);
    if (conflict) {
      mError("stream:%s other trans exists in DB:%s, dstTable:%s failed to start reset-status trans", pStream->name,
             pStream->sourceDb, pStream->targetSTbName);
    } else {
      mDebug("stream:%s (0x%" PRIx64 ") reset checkpoint procedure, transId:%d, create reset trans", pStream->name,
             pStream->uid, transId);
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
    mInfo("set node expired for nodeId:%d, total:%d", *pVgId, num);

    bool    setFlag = false;
    int32_t numOfNodes = taosArrayGetSize(execInfo.pNodeList);

    for (int i = 0; i < numOfNodes; ++i) {
      SNodeEntry *pNodeEntry = taosArrayGet(execInfo.pNodeList, i);

      if (pNodeEntry->nodeId == *pVgId) {
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

int32_t mndDropOrphanTasks(SMnode *pMnode, SArray *pList) {
  SOrphanTask *pTask = taosArrayGet(pList, 0);

  // check if it is conflict with other trans in both sourceDb and targetDb.
  bool conflict = mndStreamTransConflictCheck(pMnode, pTask->streamId, MND_STREAM_DROP_NAME, false);
  if (conflict) {
    return -1;
  }

  SStreamObj dummyObj = {.uid = pTask->streamId, .sourceDb = "", .targetSTbName = ""};
  STrans    *pTrans = NULL;
  int32_t    code =
      doCreateTrans(pMnode, &dummyObj, NULL, TRN_CONFLICT_NOTHING, MND_STREAM_DROP_NAME, "drop stream", &pTrans);
  if (pTrans == NULL || code != 0) {
    mError("failed to create trans to drop orphan tasks since %s", terrstr());
    return code;
  }

  code = mndStreamRegisterTrans(pTrans, MND_STREAM_DROP_NAME, pTask->streamId);
  if (code) {
    return code;
  }
  // drop all tasks
  if ((code = mndStreamSetDropActionFromList(pMnode, pTrans, pList)) < 0) {
    mError("failed to create trans to drop orphan tasks since %s", terrstr());
    mndTransDrop(pTrans);
    return code;
  }

  // drop stream
  if ((code = mndPersistTransLog(&dummyObj, pTrans, SDB_STATUS_DROPPED)) < 0) {
    mndTransDrop(pTrans);
    return code;
  }

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mError("trans:%d, failed to prepare drop stream trans since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return code;
  }

  mndTransDrop(pTrans);
  return code;
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
        code = TSDB_CODE_OUT_OF_MEMORY;
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
  int32_t code = 0;

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
    code = terrno = TSDB_CODE_INVALID_MSG;
    return code;
  }
  tDecoderClear(&decoder);

  mDebug("receive stream-meta hb from vgId:%d, active numOfTasks:%d, msgId:%d", req.vgId, req.numOfTasks, req.msgId);

  pFailedChkpt = taosArrayInit(4, sizeof(SFailedCheckpointInfo));
  pOrphanTasks = taosArrayInit(4, sizeof(SOrphanTask));

  streamMutexLock(&execInfo.lock);

  mndInitStreamExecInfo(pMnode, &execInfo);
  if (!validateHbMsg(execInfo.pNodeList, req.vgId)) {
    mError("vgId:%d not exists in nodeList buf, discarded", req.vgId);

    code = terrno = TSDB_CODE_INVALID_MSG;
    doSendHbMsgRsp(terrno, &pReq->info, req.vgId, req.msgId);

    streamMutexUnlock(&execInfo.lock);
    cleanupAfterProcessHbMsg(&req, pFailedChkpt, pOrphanTasks);
    return code;
  }

  int32_t numOfUpdated = taosArrayGetSize(req.pUpdateNodes);
  if (numOfUpdated > 0) {
    mDebug("%d stream node(s) need updated from hbMsg(vgId:%d)", numOfUpdated, req.vgId);
    (void) setNodeEpsetExpiredFlag(req.pUpdateNodes);
  }

  bool snodeChanged = false;
  for (int32_t i = 0; i < req.numOfTasks; ++i) {
    STaskStatusEntry *p = taosArrayGet(req.pTaskStatus, i);

    STaskStatusEntry *pTaskEntry = taosHashGet(execInfo.pTaskMap, &p->id, sizeof(p->id));
    if (pTaskEntry == NULL) {
      mError("s-task:0x%" PRIx64 " not found in mnode task list", p->id.taskId);

      SOrphanTask oTask = {.streamId = p->id.streamId, .taskId = p->id.taskId, .nodeId = p->nodeId};
      void* px = taosArrayPush(pOrphanTasks, &oTask);
      if (px == NULL) {
        mError("Failed to put task into list, taskId:0x%" PRIx64, p->id.taskId);
      }
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
        code = TSDB_CODE_STREAM_TASK_NOT_EXIST;
        continue;
      }

      int32_t numOfTasks = mndGetNumOfStreamTasks(pStream);
      SCheckpointConsensusInfo *pInfo = NULL;

      code = mndGetConsensusInfo(execInfo.pStreamConsensus, p->id.streamId, numOfTasks, &pInfo);
      if (code == 0) {
        mndAddConsensusTasks(pInfo, &cp);
      } else {
        mError("failed to get consensus checkpoint-info");
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
        mError("stream task:0x%" PRIx64 " checkpointId:%" PRIx64 " transId:%d failed, kill it", p->id.taskId,
               pChkInfo->activeId, pChkInfo->activeTransId);

        SFailedCheckpointInfo info = {
            .transId = pChkInfo->activeTransId, .checkpointId = pChkInfo->activeId, .streamUid = p->id.streamId};
        addIntoCheckpointList(pFailedChkpt, &info);

        // remove failed trans from pChkptStreams
        code = taosHashRemove(execInfo.pChkptStreams, &p->id.streamId, sizeof(p->id.streamId));
        if (code) {
          mError("failed to remove stream:0x%"PRIx64" in checkpoint stream list", p->id.streamId);
        }
      }
    }

    if (p->status == pTaskEntry->status) {
      pTaskEntry->statusLastDuration++;
    } else {
      pTaskEntry->status = p->status;
      pTaskEntry->statusLastDuration = 0;
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
        mInfo("checkpointId:%" PRId64 " transId:%d failed, issue task-reset trans to reset all tasks status",
              pInfo->checkpointId, pInfo->transId);

        code = mndResetStatusFromCheckpoint(pMnode, pInfo->streamUid, pInfo->transId);
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
    code = mndDropOrphanTasks(pMnode, pOrphanTasks);
  }

  if (pMnode != NULL) {  // make sure that the unit test case can work
    mndStreamStartUpdateCheckpointInfo(pMnode);
  }

  streamMutexUnlock(&execInfo.lock);

  doSendHbMsgRsp(TSDB_CODE_SUCCESS, &pReq->info, req.vgId, req.msgId);

  cleanupAfterProcessHbMsg(&req, pFailedChkpt, pOrphanTasks);
  return code;
}

void mndStreamStartUpdateCheckpointInfo(SMnode *pMnode) {  // here reuse the doCheckpointmsg
  SMStreamDoCheckpointMsg *pMsg = rpcMallocCont(sizeof(SMStreamDoCheckpointMsg));
  if (pMsg != NULL) {
    int32_t size = sizeof(SMStreamDoCheckpointMsg);
    SRpcMsg rpcMsg = {.msgType = TDMT_MND_STREAM_UPDATE_CHKPT_EVT, .pCont = pMsg, .contLen = size};
    int32_t code = tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
    if (code) {
      mError("failed to put into write Queue, code:%s", tstrerror(code));
    }
  }
}

bool validateHbMsg(const SArray *pNodeList, int32_t vgId) {
  for (int32_t i = 0; i < taosArrayGetSize(pNodeList); ++i) {
    SNodeEntry *pEntry = taosArrayGet(pNodeList, i);
    if (pEntry->nodeId == vgId) {
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
