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

static void doExtractTasksFromStream(SMnode *pMnode) {
  SSdb       *pSdb = pMnode->pSdb;
  SStreamObj *pStream = NULL;
  void       *pIter = NULL;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) {
      break;
    }

    saveStreamTasksInfo(pStream, &execInfo);
    sdbRelease(pSdb, pStream);
  }
}

static void updateStageInfo(STaskStatusEntry *pTaskEntry, int64_t stage) {
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

static void addIntoCheckpointList(SArray* pList, const SFailedCheckpointInfo* pInfo) {
  int32_t num = taosArrayGetSize(pList);
  for(int32_t i = 0; i < num; ++i) {
    SFailedCheckpointInfo* p = taosArrayGet(pList, i);
    if (p->transId == pInfo->transId) {
      return;
    }
  }

  taosArrayPush(pList, pInfo);
}

static int32_t createStreamResetStatusTrans(SMnode *pMnode, SStreamObj *pStream) {
  STrans *pTrans = doCreateTrans(pMnode, pStream, NULL, MND_STREAM_TASK_RESET_NAME, " reset from failed checkpoint");
  if (pTrans == NULL) {
    return terrno;
  }

  taosWLockLatch(&pStream->lock);
  int32_t numOfLevels = taosArrayGetSize(pStream->tasks);

  for (int32_t j = 0; j < numOfLevels; ++j) {
    SArray *pLevel = taosArrayGetP(pStream->tasks, j);

    int32_t numOfTasks = taosArrayGetSize(pLevel);
    for (int32_t k = 0; k < numOfTasks; ++k) {
      SStreamTask *pTask = taosArrayGetP(pLevel, k);

      // todo extract method, with pause stream task
      SVResetStreamTaskReq *pReq = taosMemoryCalloc(1, sizeof(SVResetStreamTaskReq));
      if (pReq == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        mError("failed to malloc in reset stream, size:%" PRIzu ", code:%s", sizeof(SVResetStreamTaskReq),
               tstrerror(TSDB_CODE_OUT_OF_MEMORY));
        taosWUnLockLatch(&pStream->lock);
        return terrno;
      }

      pReq->head.vgId = htonl(pTask->info.nodeId);
      pReq->taskId = pTask->id.taskId;
      pReq->streamId = pTask->id.streamId;

      SEpSet  epset = {0};
      bool    hasEpset = false;
      int32_t code = extractNodeEpset(pMnode, &epset, &hasEpset, pTask->id.taskId, pTask->info.nodeId);
      if (code != TSDB_CODE_SUCCESS || !hasEpset) {
        taosMemoryFree(pReq);
        continue;
      }

      code = setTransAction(pTrans, pReq, sizeof(SVResetStreamTaskReq), TDMT_VND_STREAM_TASK_RESET, &epset, 0);
      if (code != 0) {
        taosMemoryFree(pReq);
        taosWUnLockLatch(&pStream->lock);
        mndTransDrop(pTrans);
        return terrno;
      }
    }
  }

  taosWUnLockLatch(&pStream->lock);

  int32_t code = mndPersistTransLog(pStream, pTrans, SDB_STATUS_READY);
  if (code != TSDB_CODE_SUCCESS) {
    sdbRelease(pMnode->pSdb, pStream);
    return -1;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare update stream trans since %s", pTrans->id, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return -1;
  }

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);

  return TSDB_CODE_ACTION_IN_PROGRESS;
}

static int32_t mndResetStatusFromCheckpoint(SMnode *pMnode, int64_t streamId, int32_t transId) {
  int32_t code = TSDB_CODE_SUCCESS;
  mndKillTransImpl(pMnode, transId, "");

  SStreamObj *pStream = mndGetStreamObj(pMnode, streamId);
  if (pStream == NULL) {
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
      code = createStreamResetStatusTrans(pMnode, pStream);
    }
  }

  mndReleaseStream(pMnode, pStream);
  return code;
}

static int32_t setNodeEpsetExpiredFlag(const SArray *pNodeList) {
  int32_t num = taosArrayGetSize(pNodeList);
  mInfo("set node expired for %d nodes", num);

  for (int k = 0; k < num; ++k) {
    int32_t *pVgId = taosArrayGet(pNodeList, k);
    mInfo("set node expired for nodeId:%d, total:%d", *pVgId, num);

    int32_t numOfNodes = taosArrayGetSize(execInfo.pNodeList);
    for (int i = 0; i < numOfNodes; ++i) {
      SNodeEntry *pNodeEntry = taosArrayGet(execInfo.pNodeList, i);

      if (pNodeEntry->nodeId == *pVgId) {
        mInfo("vgId:%d expired for some stream tasks, needs update nodeEp", *pVgId);
        pNodeEntry->stageUpdated = true;
        break;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mndProcessStreamHb(SRpcMsg *pReq) {
  SMnode      *pMnode = pReq->info.node;
  SStreamHbMsg req = {0};
  SArray      *pList = taosArrayInit(4, sizeof(SFailedCheckpointInfo));

  SDecoder decoder = {0};
  tDecoderInit(&decoder, pReq->pCont, pReq->contLen);

  if (tDecodeStreamHbMsg(&decoder, &req) < 0) {
    streamMetaClearHbMsg(&req);
    tDecoderClear(&decoder);
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }
  tDecoderClear(&decoder);

  mTrace("receive stream-meta hb from vgId:%d, active numOfTasks:%d", req.vgId, req.numOfTasks);

  taosThreadMutexLock(&execInfo.lock);

  // extract stream task list
  int32_t numOfExisted = taosHashGetSize(execInfo.pTaskMap);
  if (numOfExisted == 0) {
    doExtractTasksFromStream(pMnode);
  }

  initStreamNodeList(pMnode);

  int32_t numOfUpdated = taosArrayGetSize(req.pUpdateNodes);
  if (numOfUpdated > 0) {
    mDebug("%d stream node(s) need updated from report of hbMsg(vgId:%d)", numOfUpdated, req.vgId);
    setNodeEpsetExpiredFlag(req.pUpdateNodes);
  }

  bool snodeChanged = false;
  for (int32_t i = 0; i < req.numOfTasks; ++i) {
    STaskStatusEntry *p = taosArrayGet(req.pTaskStatus, i);

    STaskStatusEntry *pTaskEntry = taosHashGet(execInfo.pTaskMap, &p->id, sizeof(p->id));
    if (pTaskEntry == NULL) {
      mError("s-task:0x%" PRIx64 " not found in mnode task list", p->id.taskId);
      continue;
    }

    if (pTaskEntry->stage != p->stage && pTaskEntry->stage != -1) {
      updateStageInfo(pTaskEntry, p->stage);
      if (pTaskEntry->nodeId == SNODE_HANDLE) {
        snodeChanged = true;
      }
    } else {
      // task is idle for more than 50 sec.
      if (fabs(pTaskEntry->inputQUsed - p->inputQUsed) <= DBL_EPSILON) {
        if (!pTaskEntry->inputQChanging) {
          pTaskEntry->inputQUnchangeCounter++;
        } else {
          pTaskEntry->inputQChanging = false;
        }
      } else {
        pTaskEntry->inputQChanging = true;
        pTaskEntry->inputQUnchangeCounter = 0;
      }

      streamTaskStatusCopy(pTaskEntry, p);
      if (p->checkpointId != 0) {
        if (p->checkpointFailed) {
          mError("stream task:0x%" PRIx64 " checkpointId:%" PRIx64 " transId:%d failed, kill it", p->id.taskId,
              p->checkpointId, p->chkpointTransId);

          SFailedCheckpointInfo info = {
              .transId = p->chkpointTransId, .checkpointId = p->checkpointId, .streamUid = p->id.streamId};
          addIntoCheckpointList(pList, &info);
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
  if (taosArrayGetSize(pList) > 0) {
    bool    allReady = true;
    SArray *p = mndTakeVgroupSnapshot(pMnode, &allReady);
    taosArrayDestroy(p);

    if (allReady || snodeChanged) {
      // if the execInfo.activeCheckpoint == 0, the checkpoint is restoring from wal
      for(int32_t i = 0; i < taosArrayGetSize(pList); ++i) {
        SFailedCheckpointInfo *pInfo = taosArrayGet(pList, i);
        mInfo("checkpointId:%" PRId64 " transId:%d failed, issue task-reset trans to reset all tasks status",
            pInfo->checkpointId, pInfo->transId);

        mndResetStatusFromCheckpoint(pMnode, pInfo->streamUid, pInfo->transId);
      }
    } else {
      mInfo("not all vgroups are ready, wait for next HB from stream tasks to reset the task status");
    }
  }

  taosThreadMutexUnlock(&execInfo.lock);
  streamMetaClearHbMsg(&req);

  taosArrayDestroy(pList);
  return TSDB_CODE_SUCCESS;
}
