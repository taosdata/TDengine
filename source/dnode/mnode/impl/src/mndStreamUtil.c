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
#include "tmisce.h"
#include "mndVgroup.h"

struct SStreamTaskIter {
  SStreamObj  *pStream;
  int32_t      level;
  int32_t      ordinalIndex;
  int32_t      totalLevel;
  SStreamTask *pTask;
};

int32_t doRemoveTasks(SStreamExecInfo *pExecNode, STaskId *pRemovedId);

int32_t createStreamTaskIter(SStreamObj* pStream, SStreamTaskIter** pIter) {
  *pIter = taosMemoryCalloc(1, sizeof(SStreamTaskIter));
  if (*pIter == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  (*pIter)->level = -1;
  (*pIter)->ordinalIndex = 0;
  (*pIter)->pStream = pStream;
  (*pIter)->totalLevel = taosArrayGetSize(pStream->tasks);
  (*pIter)->pTask = NULL;

  return 0;
}

bool streamTaskIterNextTask(SStreamTaskIter* pIter) {
  if (pIter->level >= pIter->totalLevel) {
    pIter->pTask = NULL;
    return false;
  }

  if (pIter->level == -1) {
    pIter->level += 1;
  }

  while(pIter->level < pIter->totalLevel) {
    SArray *pList = taosArrayGetP(pIter->pStream->tasks, pIter->level);
    if (pIter->ordinalIndex >= taosArrayGetSize(pList)) {
      pIter->level += 1;
      pIter->ordinalIndex = 0;
      pIter->pTask = NULL;
      continue;
    }

    pIter->pTask = taosArrayGetP(pList, pIter->ordinalIndex);
    pIter->ordinalIndex += 1;
    return true;
  }

  pIter->pTask = NULL;
  return false;
}

int32_t streamTaskIterGetCurrent(SStreamTaskIter* pIter, SStreamTask** pTask) {
  if (pTask) {
    *pTask = pIter->pTask;
    if (*pTask != NULL) {
      return TSDB_CODE_SUCCESS;
    }
  }

  return TSDB_CODE_INVALID_PARA;
}

void destroyStreamTaskIter(SStreamTaskIter* pIter) {
  taosMemoryFree(pIter);
}

int32_t mndTakeVgroupSnapshot(SMnode *pMnode, bool *allReady, SArray** pList) {
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  SVgObj *pVgroup = NULL;
  int32_t replica = -1;   // do the replica check
  int32_t code = 0;

  *allReady = true;
  SArray *pVgroupList = taosArrayInit(4, sizeof(SNodeEntry));

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) {
      break;
    }

    SNodeEntry entry = {.nodeId = pVgroup->vgId, .hbTimestamp = pVgroup->updateTime};
    entry.epset = mndGetVgroupEpset(pMnode, pVgroup);

    if (replica == -1) {
      replica = pVgroup->replica;
    } else {
      if (replica != pVgroup->replica) {
        mInfo("vgId:%d replica:%d inconsistent with other vgroups replica:%d, not ready for stream operations",
              pVgroup->vgId, pVgroup->replica, replica);
        *allReady = false;
        sdbRelease(pSdb, pVgroup);
        break;
      }
    }

    // if not all ready till now, no need to check the remaining vgroups.
    if (*allReady) {
      for (int32_t i = 0; i < pVgroup->replica; ++i) {
        if (!pVgroup->vnodeGid[i].syncRestore) {
          mInfo("vgId:%d not restored, not ready for checkpoint or other operations", pVgroup->vgId);
          *allReady = false;
          break;
        }

        ESyncState state = pVgroup->vnodeGid[i].syncState;
        if (state == TAOS_SYNC_STATE_OFFLINE || state == TAOS_SYNC_STATE_ERROR || state == TAOS_SYNC_STATE_LEARNER ||
            state == TAOS_SYNC_STATE_CANDIDATE) {
          mInfo("vgId:%d state:%d , not ready for checkpoint or other operations, not check other vgroups",
                pVgroup->vgId, state);
          *allReady = false;
          break;
        }
      }
    }

    char buf[256] = {0};
    (void) epsetToStr(&entry.epset, buf, tListLen(buf));

    void* p = taosArrayPush(pVgroupList, &entry);
    if (p == NULL) {
      mError("failed to put entry in vgroup list, nodeId:%d code:out of memory", entry.nodeId);
    } else {
      mDebug("take node snapshot, nodeId:%d %s", entry.nodeId, buf);
    }

    sdbRelease(pSdb, pVgroup);
  }

  SSnodeObj *pObj = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_SNODE, pIter, (void **)&pObj);
    if (pIter == NULL) {
      break;
    }

    SNodeEntry entry = {.nodeId = SNODE_HANDLE};
    code = addEpIntoEpSet(&entry.epset, pObj->pDnode->fqdn, pObj->pDnode->port);
    if (code) {
      sdbRelease(pSdb, pObj);
      continue;
    }

    char buf[256] = {0};
    (void) epsetToStr(&entry.epset, buf, tListLen(buf));

    void* p = taosArrayPush(pVgroupList, &entry);
    if (p == NULL) {
      mError("failed to put entry in vgroup list, nodeId:%d code:out of memory", entry.nodeId);
    } else {
      mDebug("take snode snapshot, nodeId:%d %s", entry.nodeId, buf);
    }

    sdbRelease(pSdb, pObj);
  }

  *pList = pVgroupList;
  return code;
}

int32_t mndGetStreamObj(SMnode *pMnode, int64_t streamId, SStreamObj **pStream) {
  void *pIter = NULL;
  SSdb *pSdb = pMnode->pSdb;
  *pStream = NULL;

  SStreamObj *p = NULL;
  while ((pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&p)) != NULL) {
    if (p->uid == streamId) {
      sdbCancelFetch(pSdb, pIter);
      *pStream = p;
      return TSDB_CODE_SUCCESS;
    }
    sdbRelease(pSdb, p);
  }

  return TSDB_CODE_STREAM_TASK_NOT_EXIST;
}

void mndKillTransImpl(SMnode *pMnode, int32_t transId, const char *pDbName) {
  STrans *pTrans = mndAcquireTrans(pMnode, transId);
  if (pTrans != NULL) {
    mInfo("kill active transId:%d in Db:%s", transId, pDbName);
    int32_t code = mndKillTrans(pMnode, pTrans);
    mndReleaseTrans(pMnode, pTrans);
    if (code) {
      mError("failed to kill trans:%d", pTrans->id);
    }
  } else {
    mError("failed to acquire trans in Db:%s, transId:%d", pDbName, transId);
  }
}

int32_t extractNodeEpset(SMnode *pMnode, SEpSet *pEpSet, bool *hasEpset, int32_t taskId, int32_t nodeId) {
  *hasEpset = false;

  pEpSet->numOfEps = 0;
  if (nodeId == SNODE_HANDLE) {
    SSnodeObj *pObj = NULL;
    void      *pIter = NULL;

    pIter = sdbFetch(pMnode->pSdb, SDB_SNODE, pIter, (void **)&pObj);
    if (pIter != NULL) {
      int32_t code = addEpIntoEpSet(pEpSet, pObj->pDnode->fqdn, pObj->pDnode->port);
      sdbRelease(pMnode->pSdb, pObj);
      sdbCancelFetch(pMnode->pSdb, pIter);
      if (code) {
        *hasEpset = false;
        mError("failed to set epset");
      } else {
        *hasEpset = true;
      }
      return code;
    } else {
      mError("failed to acquire snode epset");
      return TSDB_CODE_INVALID_PARA;
    }
  } else {
    SVgObj *pVgObj = mndAcquireVgroup(pMnode, nodeId);
    if (pVgObj != NULL) {
      SEpSet epset = mndGetVgroupEpset(pMnode, pVgObj);
      mndReleaseVgroup(pMnode, pVgObj);

      epsetAssign(pEpSet, &epset);
      *hasEpset = true;
      return TSDB_CODE_SUCCESS;
    } else {
      mDebug("orphaned task:0x%x need to be dropped, nodeId:%d, no redo action", taskId, nodeId);
      return TSDB_CODE_SUCCESS;
    }
  }
}

static int32_t doSetResumeAction(STrans *pTrans, SMnode *pMnode, SStreamTask *pTask, int8_t igUntreated) {
  terrno = 0;

  SVResumeStreamTaskReq *pReq = taosMemoryCalloc(1, sizeof(SVResumeStreamTaskReq));
  if (pReq == NULL) {
    mError("failed to malloc in resume stream, size:%" PRIzu ", code:%s", sizeof(SVResumeStreamTaskReq),
           tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  pReq->head.vgId = htonl(pTask->info.nodeId);
  pReq->taskId = pTask->id.taskId;
  pReq->streamId = pTask->id.streamId;
  pReq->igUntreated = igUntreated;

  SEpSet  epset = {0};
  bool    hasEpset = false;
  int32_t code = extractNodeEpset(pMnode, &epset, &hasEpset, pTask->id.taskId, pTask->info.nodeId);
  if (code != TSDB_CODE_SUCCESS || (!hasEpset)) {
    terrno = code;
    taosMemoryFree(pReq);
    return terrno;
  }

  code = setTransAction(pTrans, pReq, sizeof(SVResumeStreamTaskReq), TDMT_STREAM_TASK_RESUME, &epset, 0, 0);
  if (code != 0) {
    taosMemoryFree(pReq);
    return terrno;
  }

  mDebug("set the resume action for trans:%d", pTrans->id);
  return 0;
}

int32_t mndGetStreamTask(STaskId *pId, SStreamObj *pStream, SStreamTask **pTask) {
  *pTask = NULL;

  SStreamTask     *p = NULL;
  SStreamTaskIter *pIter = NULL;
  int32_t          code = createStreamTaskIter(pStream, &pIter);
  if (code) {
    mError("failed to create stream task iter:%s", pStream->name);
    return code;
  }

  while (streamTaskIterNextTask(pIter)) {
    code = streamTaskIterGetCurrent(pIter, &p);
    if (code) {
      continue;
    }

    if (p->id.taskId == pId->taskId) {
      destroyStreamTaskIter(pIter);
      *pTask = p;
      return 0;
    }
  }

  destroyStreamTaskIter(pIter);
  return TSDB_CODE_FAILED;
}

int32_t mndGetNumOfStreamTasks(const SStreamObj *pStream) {
  int32_t num = 0;
  for(int32_t i = 0; i < taosArrayGetSize(pStream->tasks); ++i) {
    SArray* pLevel = taosArrayGetP(pStream->tasks, i);
    num += taosArrayGetSize(pLevel);
  }

  return num;
}

int32_t mndStreamSetResumeAction(STrans *pTrans, SMnode *pMnode, SStreamObj *pStream, int8_t igUntreated) {
  SStreamTaskIter *pIter = NULL;
  int32_t          code = createStreamTaskIter(pStream, &pIter);
  if (code) {
    mError("failed to create stream task iter:%s", pStream->name);
    return code;
  }

  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = NULL;
    code = streamTaskIterGetCurrent(pIter, &pTask);
    if (code || pTask == NULL) {
      destroyStreamTaskIter(pIter);
      return code;
    }

    code = doSetResumeAction(pTrans, pMnode, pTask, igUntreated);
    if (code) {
      destroyStreamTaskIter(pIter);
      return code;
    }

    if (atomic_load_8(&pTask->status.taskStatus) == TASK_STATUS__PAUSE) {
      atomic_store_8(&pTask->status.taskStatus, pTask->status.statusBackup);
    }
  }
  destroyStreamTaskIter(pIter);
  return 0;
}

static int32_t doSetPauseAction(SMnode *pMnode, STrans *pTrans, SStreamTask *pTask) {
  SVPauseStreamTaskReq *pReq = taosMemoryCalloc(1, sizeof(SVPauseStreamTaskReq));
  if (pReq == NULL) {
    mError("failed to malloc in pause stream, size:%" PRIzu ", code:%s", sizeof(SVPauseStreamTaskReq),
           tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  pReq->head.vgId = htonl(pTask->info.nodeId);
  pReq->taskId = pTask->id.taskId;
  pReq->streamId = pTask->id.streamId;

  SEpSet  epset = {0};
  bool    hasEpset = false;
  int32_t code = extractNodeEpset(pMnode, &epset, &hasEpset, pTask->id.taskId, pTask->info.nodeId);
  if (code != TSDB_CODE_SUCCESS || !hasEpset) {
    terrno = code;
    taosMemoryFree(pReq);
    return code;
  }

  char buf[256] = {0};
  (void) epsetToStr(&epset, buf, tListLen(buf));
  mDebug("pause stream task in node:%d, epset:%s", pTask->info.nodeId, buf);

  code = setTransAction(pTrans, pReq, sizeof(SVPauseStreamTaskReq), TDMT_STREAM_TASK_PAUSE, &epset, 0, 0);
  if (code != 0) {
    taosMemoryFree(pReq);
    return code;
  }
  return 0;
}

int32_t mndStreamSetPauseAction(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream) {
  SStreamTaskIter *pIter = NULL;

  int32_t code = createStreamTaskIter(pStream, &pIter);
  if (code) {
    mError("failed to create stream task iter:%s", pStream->name);
    return code;
  }

  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = NULL;
    code = streamTaskIterGetCurrent(pIter, &pTask);
    if (code) {
      destroyStreamTaskIter(pIter);
      return code;
    }

    code = doSetPauseAction(pMnode, pTrans, pTask);
    if (code) {
      destroyStreamTaskIter(pIter);
      return code;
    }

    if (atomic_load_8(&pTask->status.taskStatus) != TASK_STATUS__PAUSE) {
      atomic_store_8(&pTask->status.statusBackup, pTask->status.taskStatus);
      atomic_store_8(&pTask->status.taskStatus, TASK_STATUS__PAUSE);
    }
  }

  destroyStreamTaskIter(pIter);
  return code;
}

static int32_t doSetDropAction(SMnode *pMnode, STrans *pTrans, SStreamTask *pTask) {
  SVDropStreamTaskReq *pReq = taosMemoryCalloc(1, sizeof(SVDropStreamTaskReq));
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  pReq->head.vgId = htonl(pTask->info.nodeId);
  pReq->taskId = pTask->id.taskId;
  pReq->streamId = pTask->id.streamId;

  SEpSet  epset = {0};
  bool    hasEpset = false;
  int32_t code = extractNodeEpset(pMnode, &epset, &hasEpset, pTask->id.taskId, pTask->info.nodeId);
  if (code != TSDB_CODE_SUCCESS || !hasEpset) {  // no valid epset, return directly without redoAction
    return code;
  }

  // The epset of nodeId of this task may have been expired now, let's use the newest epset from mnode.
  code = setTransAction(pTrans, pReq, sizeof(SVDropStreamTaskReq), TDMT_STREAM_TASK_DROP, &epset, 0, 0);
  if (code != 0) {
    taosMemoryFree(pReq);
    return code;
  }

  return 0;
}

int32_t mndStreamSetDropAction(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream) {
  SStreamTaskIter *pIter = NULL;

  int32_t code = createStreamTaskIter(pStream, &pIter);
  if (code) {
    mError("failed to create stream task iter:%s", pStream->name);
    return code;
  }

  while(streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = NULL;
    code = streamTaskIterGetCurrent(pIter, &pTask);
    if (code) {
      destroyStreamTaskIter(pIter);
      return code;
    }

    code = doSetDropAction(pMnode, pTrans, pTask);
    if (code) {
      destroyStreamTaskIter(pIter);
      return code;
    }
  }
  destroyStreamTaskIter(pIter);
  return 0;
}

static int32_t doSetDropActionFromId(SMnode *pMnode, STrans *pTrans, SOrphanTask* pTask) {
  SVDropStreamTaskReq *pReq = taosMemoryCalloc(1, sizeof(SVDropStreamTaskReq));
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  pReq->head.vgId = htonl(pTask->nodeId);
  pReq->taskId = pTask->taskId;
  pReq->streamId = pTask->streamId;

  SEpSet  epset = {0};
  bool    hasEpset = false;
  int32_t code = extractNodeEpset(pMnode, &epset, &hasEpset, pTask->taskId, pTask->nodeId);
  if (code != TSDB_CODE_SUCCESS || (!hasEpset)) {  // no valid epset, return directly without redoAction
    taosMemoryFree(pReq);
    return code;
  }

  // The epset of nodeId of this task may have been expired now, let's use the newest epset from mnode.
  code = setTransAction(pTrans, pReq, sizeof(SVDropStreamTaskReq), TDMT_STREAM_TASK_DROP, &epset, 0, 0);
  if (code != 0) {
    taosMemoryFree(pReq);
    return code;
  }

  return 0;
}

int32_t mndStreamSetDropActionFromList(SMnode *pMnode, STrans *pTrans, SArray* pList) {
  for(int32_t i = 0; i < taosArrayGetSize(pList); ++i) {
    SOrphanTask* pTask = taosArrayGet(pList, i);
    int32_t code = doSetDropActionFromId(pMnode, pTrans, pTask);
    if (code != 0) {
      return code;
    } else {
      mDebug("add drop task:0x%x action to drop orphan task", pTask->taskId);
    }
  }
  return 0;
}

static void initNodeUpdateMsg(SStreamTaskNodeUpdateMsg *pMsg, const SVgroupChangeInfo *pInfo, SStreamTaskId *pId,
                              int32_t transId) {
  int32_t code = 0;

  pMsg->streamId = pId->streamId;
  pMsg->taskId = pId->taskId;
  pMsg->transId = transId;
  pMsg->pNodeList = taosArrayInit(taosArrayGetSize(pInfo->pUpdateNodeList), sizeof(SNodeUpdateInfo));
  if (pMsg->pNodeList == NULL) {
    mError("failed to prepare node list, code:out of memory");
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

  if (code == 0) {
    void *p = taosArrayAddAll(pMsg->pNodeList, pInfo->pUpdateNodeList);
    if (p == NULL) {
      mError("failed to add update node list into nodeList");
    }
  }
}

static int32_t doBuildStreamTaskUpdateMsg(void **pBuf, int32_t *pLen, SVgroupChangeInfo *pInfo, int32_t nodeId,
                                          SStreamTaskId *pId, int32_t transId) {
  SStreamTaskNodeUpdateMsg req = {0};
  initNodeUpdateMsg(&req, pInfo, pId, transId);

  int32_t code = 0;
  int32_t blen;

  tEncodeSize(tEncodeStreamTaskUpdateMsg, &req, blen, code);
  if (code < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosArrayDestroy(req.pNodeList);
    return terrno;
  }

  int32_t tlen = sizeof(SMsgHead) + blen;

  void *buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosArrayDestroy(req.pNodeList);
    return terrno;
  }

  void    *abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
  code = tEncodeStreamTaskUpdateMsg(&encoder, &req);
  if (code == -1) {
    tEncoderClear(&encoder);
    taosArrayDestroy(req.pNodeList);
    return code;
  }

  SMsgHead *pMsgHead = (SMsgHead *)buf;
  pMsgHead->contLen = htonl(tlen);
  pMsgHead->vgId = htonl(nodeId);

  tEncoderClear(&encoder);

  *pBuf = buf;
  *pLen = tlen;

  taosArrayDestroy(req.pNodeList);
  return TSDB_CODE_SUCCESS;
}

static int32_t doSetUpdateTaskAction(SMnode *pMnode, STrans *pTrans, SStreamTask *pTask, SVgroupChangeInfo *pInfo) {
  void   *pBuf = NULL;
  int32_t len = 0;
  int32_t code = streamTaskUpdateEpsetInfo(pTask, pInfo->pUpdateNodeList);
  if (code) {
    return code;
  }

  code = doBuildStreamTaskUpdateMsg(&pBuf, &len, pInfo, pTask->info.nodeId, &pTask->id, pTrans->id);
  if (code) {
    return code;
  }

  SEpSet  epset = {0};
  bool    hasEpset = false;
  code = extractNodeEpset(pMnode, &epset, &hasEpset, pTask->id.taskId, pTask->info.nodeId);
  if (code != TSDB_CODE_SUCCESS || !hasEpset) {
    return code;
  }

  code = setTransAction(pTrans, pBuf, len, TDMT_VND_STREAM_TASK_UPDATE, &epset, TSDB_CODE_VND_INVALID_VGROUP_ID, 0);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pBuf);
  }

  return code;
}

// build trans to update the epset
int32_t mndStreamSetUpdateEpsetAction(SMnode *pMnode, SStreamObj *pStream, SVgroupChangeInfo *pInfo, STrans *pTrans) {
  mDebug("stream:0x%" PRIx64 " set tasks epset update action", pStream->uid);
  SStreamTaskIter *pIter = NULL;

  taosWLockLatch(&pStream->lock);
  int32_t code = createStreamTaskIter(pStream, &pIter);
  if (code) {
    taosWUnLockLatch(&pStream->lock);
    mError("failed to create stream task iter:%s", pStream->name);
    return code;
  }

  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = NULL;
    code = streamTaskIterGetCurrent(pIter, &pTask);
    if (code) {
      destroyStreamTaskIter(pIter);
      taosWUnLockLatch(&pStream->lock);
      return code;
    }

    code = doSetUpdateTaskAction(pMnode, pTrans, pTask, pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      destroyStreamTaskIter(pIter);
      taosWUnLockLatch(&pStream->lock);
      return code;
    }
  }

  destroyStreamTaskIter(pIter);
  taosWUnLockLatch(&pStream->lock);
  return 0;
}

static int32_t doSetResetAction(SMnode *pMnode, STrans *pTrans, SStreamTask *pTask) {
  SVResetStreamTaskReq *pReq = taosMemoryCalloc(1, sizeof(SVResetStreamTaskReq));
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to malloc in reset stream, size:%" PRIzu ", code:%s", sizeof(SVResetStreamTaskReq),
           tstrerror(TSDB_CODE_OUT_OF_MEMORY));
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
    return code;
  }

  code = setTransAction(pTrans, pReq, sizeof(SVResetStreamTaskReq), TDMT_VND_STREAM_TASK_RESET, &epset, 0, 0);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pReq);
  }

  return code;
}

int32_t mndStreamSetResetTaskAction(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream) {
  SStreamTaskIter *pIter = NULL;

  taosWLockLatch(&pStream->lock);
  int32_t code = createStreamTaskIter(pStream, &pIter);
  if (code) {
    taosWUnLockLatch(&pStream->lock);
    mError("failed to create stream task iter:%s", pStream->name);
    return code;
  }

  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = NULL;
    code = streamTaskIterGetCurrent(pIter, &pTask);
    if (code) {
      destroyStreamTaskIter(pIter);
      taosWUnLockLatch(&pStream->lock);
      return code;
    }

    code = doSetResetAction(pMnode, pTrans, pTask);
    if (code != TSDB_CODE_SUCCESS) {
      destroyStreamTaskIter(pIter);
      taosWUnLockLatch(&pStream->lock);
      return code;
    }
  }

  destroyStreamTaskIter(pIter);
  taosWUnLockLatch(&pStream->lock);
  return 0;
}

static void freeTaskList(void* param) {
  SArray** pList = (SArray **)param;
  taosArrayDestroy(*pList);
}

int32_t mndInitExecInfo() {
  int32_t code = taosThreadMutexInit(&execInfo.lock, NULL);
  if (code) {
    return code;
  }

  _hash_fn_t fn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR);

  execInfo.pTaskList = taosArrayInit(4, sizeof(STaskId));
  execInfo.pTaskMap = taosHashInit(64, fn, true, HASH_NO_LOCK);
  execInfo.transMgmt.pDBTrans = taosHashInit(32, fn, true, HASH_NO_LOCK);
  execInfo.pTransferStateStreams = taosHashInit(32, fn, true, HASH_NO_LOCK);
  execInfo.pChkptStreams = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  execInfo.pStreamConsensus = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  execInfo.pNodeList = taosArrayInit(4, sizeof(SNodeEntry));

  taosHashSetFreeFp(execInfo.pTransferStateStreams, freeTaskList);
  taosHashSetFreeFp(execInfo.pChkptStreams, freeTaskList);
  taosHashSetFreeFp(execInfo.pStreamConsensus, freeTaskList);
  return 0;
}

void removeExpiredNodeInfo(const SArray *pNodeSnapshot) {
  SArray *pValidList = taosArrayInit(4, sizeof(SNodeEntry));
  int32_t size = taosArrayGetSize(pNodeSnapshot);
  int32_t oldSize = taosArrayGetSize(execInfo.pNodeList);

  for (int32_t i = 0; i < oldSize; ++i) {
    SNodeEntry *p = taosArrayGet(execInfo.pNodeList, i);

    for (int32_t j = 0; j < size; ++j) {
      SNodeEntry *pEntry = taosArrayGet(pNodeSnapshot, j);
      if (pEntry->nodeId == p->nodeId) {
        void* px = taosArrayPush(pValidList, p);
        if (px == NULL) {
          mError("failed to put node into list, nodeId:%d", p->nodeId);
        }
        break;
      }
    }
  }

  taosArrayDestroy(execInfo.pNodeList);
  execInfo.pNodeList = pValidList;

  mDebug("remain %d valid node entries after clean expired nodes info, prev size:%d",
         (int32_t)taosArrayGetSize(pValidList), oldSize);
}

int32_t doRemoveTasks(SStreamExecInfo *pExecNode, STaskId *pRemovedId) {
  void *p = taosHashGet(pExecNode->pTaskMap, pRemovedId, sizeof(*pRemovedId));
  if (p == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = taosHashRemove(pExecNode->pTaskMap, pRemovedId, sizeof(*pRemovedId));
  if (code) {
    return code;
  }

  for (int32_t k = 0; k < taosArrayGetSize(pExecNode->pTaskList); ++k) {
    STaskId *pId = taosArrayGet(pExecNode->pTaskList, k);
    if (pId->taskId == pRemovedId->taskId && pId->streamId == pRemovedId->streamId) {
      taosArrayRemove(pExecNode->pTaskList, k);

      int32_t num = taosArrayGetSize(pExecNode->pTaskList);
      mInfo("s-task:0x%x removed from buffer, remain:%d in buffer list", (int32_t)pRemovedId->taskId, num);
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

void removeTasksInBuf(SArray *pTaskIds, SStreamExecInfo* pExecInfo) {
  for (int32_t i = 0; i < taosArrayGetSize(pTaskIds); ++i) {
    STaskId *pId = taosArrayGet(pTaskIds, i);
    int32_t code = doRemoveTasks(pExecInfo, pId);
    if (code) {
      mError("failed to remove task in buffer list, 0x%"PRIx64, pId->taskId);
    }
  }
}

void removeStreamTasksInBuf(SStreamObj *pStream, SStreamExecInfo *pExecNode) {
  SStreamTaskIter *pIter = NULL;
  streamMutexLock(&pExecNode->lock);

  // 1. remove task entries
  int32_t code = createStreamTaskIter(pStream, &pIter);
  if (code) {
    streamMutexUnlock(&pExecNode->lock);
    mError("failed to create stream task iter:%s", pStream->name);
    return;
  }

  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = NULL;
    code = streamTaskIterGetCurrent(pIter, &pTask);
    if (code) {
      continue;
    }

    STaskId id = {.streamId = pTask->id.streamId, .taskId = pTask->id.taskId};
    code = doRemoveTasks(pExecNode, &id);
    if (code) {
      mError("failed to remove task in buffer list, 0x%"PRIx64, id.taskId);
    }
  }

  ASSERT(taosHashGetSize(pExecNode->pTaskMap) == taosArrayGetSize(pExecNode->pTaskList));

  // 2. remove stream entry in consensus hash table
  (void) mndClearConsensusCheckpointId(execInfo.pStreamConsensus, pStream->uid);

  streamMutexUnlock(&pExecNode->lock);
  destroyStreamTaskIter(pIter);
}

static bool taskNodeExists(SArray *pList, int32_t nodeId) {
  size_t num = taosArrayGetSize(pList);

  for (int32_t i = 0; i < num; ++i) {
    SNodeEntry *pEntry = taosArrayGet(pList, i);
    if (pEntry->nodeId == nodeId) {
      return true;
    }
  }

  return false;
}

int32_t removeExpiredNodeEntryAndTaskInBuf(SArray *pNodeSnapshot) {
  SArray *pRemovedTasks = taosArrayInit(4, sizeof(STaskId));

  int32_t numOfTask = taosArrayGetSize(execInfo.pTaskList);
  for (int32_t i = 0; i < numOfTask; ++i) {
    STaskId *pId = taosArrayGet(execInfo.pTaskList, i);

    STaskStatusEntry *pEntry = taosHashGet(execInfo.pTaskMap, pId, sizeof(*pId));
    if (pEntry->nodeId == SNODE_HANDLE) {
      continue;
    }

    bool existed = taskNodeExists(pNodeSnapshot, pEntry->nodeId);
    if (!existed) {
      void* p = taosArrayPush(pRemovedTasks, pId);
      if (p == NULL) {
        mError("failed to put task entry into remove list, taskId:0x%" PRIx64, pId->taskId);
      }
    }
  }

  removeTasksInBuf(pRemovedTasks, &execInfo);

  mDebug("remove invalid stream tasks:%d, remain:%d", (int32_t)taosArrayGetSize(pRemovedTasks),
         (int32_t)taosArrayGetSize(execInfo.pTaskList));

  removeExpiredNodeInfo(pNodeSnapshot);

  taosArrayDestroy(pRemovedTasks);
  return 0;
}

static int32_t doSetUpdateChkptAction(SMnode *pMnode, STrans *pTrans, SStreamTask *pTask) {
  SVUpdateCheckpointInfoReq *pReq = taosMemoryCalloc(1, sizeof(SVUpdateCheckpointInfoReq));
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to malloc in reset stream, size:%" PRIzu ", code:%s", sizeof(SVUpdateCheckpointInfoReq),
           tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    return terrno;
  }

  pReq->head.vgId = htonl(pTask->info.nodeId);
  pReq->taskId = pTask->id.taskId;
  pReq->streamId = pTask->id.streamId;

  SArray **pReqTaskList = (SArray **)taosHashGet(execInfo.pChkptStreams, &pTask->id.streamId, sizeof(pTask->id.streamId));
  ASSERT(pReqTaskList);

  int32_t size = taosArrayGetSize(*pReqTaskList);
  for(int32_t i = 0; i < size; ++i) {
    STaskChkptInfo* pInfo = taosArrayGet(*pReqTaskList, i);
    if (pInfo->taskId == pTask->id.taskId) {
      pReq->checkpointId = pInfo->checkpointId;
      pReq->checkpointVer = pInfo->version;
      pReq->checkpointTs = pInfo->ts;
      pReq->dropRelHTask = pInfo->dropHTask;
      pReq->transId = pInfo->transId;
      pReq->hStreamId = pTask->hTaskInfo.id.streamId;
      pReq->hTaskId = pTask->hTaskInfo.id.taskId;
    }
  }

  SEpSet  epset = {0};
  bool    hasEpset = false;
  int32_t code = extractNodeEpset(pMnode, &epset, &hasEpset, pTask->id.taskId, pTask->info.nodeId);
  if (code != TSDB_CODE_SUCCESS || !hasEpset) {
    taosMemoryFree(pReq);
    return code;
  }

  code = setTransAction(pTrans, pReq, sizeof(SVUpdateCheckpointInfoReq), TDMT_STREAM_TASK_UPDATE_CHKPT, &epset, 0, TSDB_CODE_VND_INVALID_VGROUP_ID);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pReq);
  }

  return code;
}

int32_t mndStreamSetUpdateChkptAction(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream) {
  SStreamTaskIter *pIter = NULL;

  taosWLockLatch(&pStream->lock);
  int32_t code = createStreamTaskIter(pStream, &pIter);
  if (code) {
    taosWUnLockLatch(&pStream->lock);
    mError("failed to create stream task iter:%s", pStream->name);
    return code;
  }

  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = NULL;
    code = streamTaskIterGetCurrent(pIter, &pTask);
    if (code) {
      destroyStreamTaskIter(pIter);
      taosWUnLockLatch(&pStream->lock);
      return code;
    }

    code = doSetUpdateChkptAction(pMnode, pTrans, pTask);
    if (code != TSDB_CODE_SUCCESS) {
      destroyStreamTaskIter(pIter);
      taosWUnLockLatch(&pStream->lock);
      return code;
    }
  }

  destroyStreamTaskIter(pIter);
  taosWUnLockLatch(&pStream->lock);
  return code;
}

int32_t mndScanCheckpointReportInfo(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  void   *pIter = NULL;
  SArray *pDropped = taosArrayInit(4, sizeof(int64_t));
  int32_t code = 0;

  mDebug("start to scan checkpoint report info");

  while ((pIter = taosHashIterate(execInfo.pChkptStreams, pIter)) != NULL) {
    SArray *pList = *(SArray **)pIter;

    STaskChkptInfo *pInfo = taosArrayGet(pList, 0);
    SStreamObj     *pStream = NULL;
    code = mndGetStreamObj(pMnode, pInfo->streamId, &pStream);
    if (pStream == NULL || code != 0) {
      mDebug("failed to acquire stream:0x%" PRIx64 " remove it from checkpoint-report list", pInfo->streamId);
      void* p = taosArrayPush(pDropped, &pInfo->streamId);
      if (p == NULL) {
        mError("failed to put stream into drop list:0x%" PRIx64, pInfo->streamId);
      }

      continue;
    }

    int32_t total = mndGetNumOfStreamTasks(pStream);
    int32_t existed = (int32_t)taosArrayGetSize(pList);

    if (total == existed) {
      mDebug("stream:0x%" PRIx64 " %s all %d tasks send checkpoint-report, start to update checkpoint-info",
             pStream->uid, pStream->name, total);

      bool conflict = mndStreamTransConflictCheck(pMnode, pStream->uid, MND_STREAM_CHKPT_UPDATE_NAME, false);
      if (!conflict) {
        code = mndCreateStreamChkptInfoUpdateTrans(pMnode, pStream, pList);
        if (code == TSDB_CODE_SUCCESS || code == TSDB_CODE_ACTION_IN_PROGRESS) {  // remove this entry
          void* p = taosArrayPush(pDropped, &pInfo->streamId);
          if (p == NULL) {
            mError("failed to remove stream:0x%" PRIx64, pInfo->streamId);
          } else {
            mDebug("stream:0x%" PRIx64 " removed", pInfo->streamId);
          }
        } else {
          mDebug("stream:0x%" PRIx64 " not launch chkpt-meta update trans, due to checkpoint not finished yet",
                 pInfo->streamId);
        }
        break;
      } else {
        mDebug("stream:0x%" PRIx64 " active checkpoint trans not finished yet, wait", pInfo->streamId);
      }
    } else {
      mDebug("stream:0x%" PRIx64 " %s %d/%d tasks send checkpoint-report, %d not send", pInfo->streamId, pStream->name,
             existed, total, total - existed);
    }

    sdbRelease(pMnode->pSdb, pStream);
  }

  int32_t size = taosArrayGetSize(pDropped);
  if (size > 0) {
    for (int32_t i = 0; i < size; ++i) {
      int64_t streamId = *(int64_t *)taosArrayGet(pDropped, i);
      code = taosHashRemove(execInfo.pChkptStreams, &streamId, sizeof(streamId));
      if (code) {
        mError("failed to remove stream in buf:0x%"PRIx64, streamId);
      }
    }

    int32_t numOfStreams = taosHashGetSize(execInfo.pChkptStreams);
    mDebug("drop %d stream(s) in checkpoint-report list, remain:%d", size, numOfStreams);
  }

  taosArrayDestroy(pDropped);
  return TSDB_CODE_SUCCESS;
}

static int32_t mndStreamSetChkptIdAction(SMnode *pMnode, STrans *pTrans, SStreamTask* pTask, int64_t checkpointId, int64_t ts) {
  SRestoreCheckpointInfo req = {
      .taskId = pTask->id.taskId,
      .streamId = pTask->id.streamId,
      .checkpointId = checkpointId,
      .startTs = ts,
      .nodeId = pTask->info.nodeId,
      .transId = pTrans->id,
  };

  int32_t code = 0;
  int32_t blen;
  tEncodeSize(tEncodeRestoreCheckpointInfo, &req, blen, code);
  if (code < 0) {
    return terrno = TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t tlen = sizeof(SMsgHead) + blen;

  void *pBuf = taosMemoryMalloc(tlen);
  if (pBuf == NULL) {
    return terrno = TSDB_CODE_OUT_OF_MEMORY;
  }

  void    *abuf = POINTER_SHIFT(pBuf, sizeof(SMsgHead));
  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
  code = tEncodeRestoreCheckpointInfo(&encoder, &req);
  tEncoderClear(&encoder);
  if (code == -1) {
    taosMemoryFree(pBuf);
    return code;
  }

  SMsgHead *pMsgHead = (SMsgHead *)pBuf;
  pMsgHead->contLen = htonl(tlen);
  pMsgHead->vgId = htonl(pTask->info.nodeId);

  SEpSet  epset = {0};
  bool    hasEpset = false;
  code = extractNodeEpset(pMnode, &epset, &hasEpset, pTask->id.taskId, pTask->info.nodeId);
  if (code != TSDB_CODE_SUCCESS || !hasEpset) {
    taosMemoryFree(pBuf);
    return code;
  }

  code = setTransAction(pTrans, pBuf, tlen, TDMT_STREAM_CONSEN_CHKPT, &epset, 0, TSDB_CODE_VND_INVALID_VGROUP_ID);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pBuf);
  }

  return code;
}

int32_t mndCreateSetConsensusChkptIdTrans(SMnode *pMnode, SStreamObj *pStream, int32_t taskId, int64_t checkpointId,
                                          int64_t ts) {
  char msg[128] = {0};
  snprintf(msg, tListLen(msg), "set consen-chkpt-id for task:0x%x", taskId);

  STrans *pTrans = NULL;
  int32_t code = doCreateTrans(pMnode, pStream, NULL, TRN_CONFLICT_NOTHING, MND_STREAM_CHKPT_CONSEN_NAME, msg, &pTrans);
  if (pTrans == NULL || code != 0) {
    return terrno;
  }

  STaskId      id = {.streamId = pStream->uid, .taskId = taskId};
  SStreamTask *pTask = NULL;
  code = mndGetStreamTask(&id, pStream, &pTask);
  if (code) {
    mError("failed to get task:0x%x in stream:%s, failed to create consensus-checkpointId", taskId, pStream->name);
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  code = mndStreamRegisterTrans(pTrans, MND_STREAM_CHKPT_CONSEN_NAME, pStream->uid);
  if (code) {
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  code = mndStreamSetChkptIdAction(pMnode, pTrans, pTask, checkpointId, ts);
  if (code != 0) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  code = mndPersistTransLog(pStream, pTrans, SDB_STATUS_READY);
  if (code) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  code = mndTransPrepare(pMnode, pTrans);
  if (code) {
    mError("trans:%d, failed to prepare set consensus-chkptId trans since %s", pTrans->id, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);

  return TSDB_CODE_ACTION_IN_PROGRESS;
}

int32_t mndGetConsensusInfo(SHashObj* pHash, int64_t streamId, int32_t numOfTasks, SCheckpointConsensusInfo **pInfo) {
  *pInfo = NULL;

  void* px = taosHashGet(pHash, &streamId, sizeof(streamId));
  if (px != NULL) {
    *pInfo = px;
    return 0;
  }

  SCheckpointConsensusInfo p = {
      .pTaskList = taosArrayInit(4, sizeof(SCheckpointConsensusEntry)),
      .numOfTasks = numOfTasks,
      .streamId = streamId,
  };

  int32_t code = taosHashPut(pHash, &streamId, sizeof(streamId), &p, sizeof(p));
  if (code == 0) {
    void *pChkptInfo = (SCheckpointConsensusInfo *)taosHashGet(pHash, &streamId, sizeof(streamId));
    *pInfo = pChkptInfo;
  } else {
    *pInfo = NULL;
  }
  return code;
}

// no matter existed or not, add the request into info list anyway, since we need to send rsp mannually
// discard the msg may lead to the lost of connections.
void mndAddConsensusTasks(SCheckpointConsensusInfo *pInfo, const SRestoreCheckpointInfo *pRestoreInfo) {
  SCheckpointConsensusEntry info = {.ts = taosGetTimestampMs()};
  memcpy(&info.req, pRestoreInfo, sizeof(info.req));

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pTaskList); ++i) {
    SCheckpointConsensusEntry *p = taosArrayGet(pInfo->pTaskList, i);
    if (p->req.taskId == info.req.taskId) {
      mDebug("s-task:0x%x already in consensus-checkpointId list for stream:0x%" PRIx64 ", update ts %" PRId64
             "->%" PRId64 " total existed:%d",
             pRestoreInfo->taskId, pRestoreInfo->streamId, p->req.startTs, info.req.startTs,
             (int32_t)taosArrayGetSize(pInfo->pTaskList));
      p->req.startTs = info.req.startTs;
      return;
    }
  }

  void *p = taosArrayPush(pInfo->pTaskList, &info);
  if (p == NULL) {
    mError("s-task:0x%x failed to put task into consensus-checkpointId list, code: out of memory", info.req.taskId);
  } else {
    int32_t num = taosArrayGetSize(pInfo->pTaskList);
    mDebug("s-task:0x%x checkpointId:%" PRId64 " added into consensus-checkpointId list, stream:0x%" PRIx64
           " waiting tasks:%d",
           pRestoreInfo->taskId, pRestoreInfo->checkpointId, pRestoreInfo->streamId, num);
  }
}

void mndClearConsensusRspEntry(SCheckpointConsensusInfo* pInfo) {
  taosArrayDestroy(pInfo->pTaskList);
  pInfo->pTaskList = NULL;
}

int64_t mndClearConsensusCheckpointId(SHashObj* pHash, int64_t streamId) {
  int32_t code = 0;
  int32_t numOfStreams = taosHashGetSize(pHash);
  if (numOfStreams == 0) {
    return TSDB_CODE_SUCCESS;
  }

  code = taosHashRemove(pHash, &streamId, sizeof(streamId));
  if (code == 0) {
    mDebug("drop stream:0x%" PRIx64 " in consensus-checkpointId list, remain:%d", streamId, numOfStreams);
  } else {
    mError("failed to remove stream:0x%"PRIx64" in consensus-checkpointId list, remain:%d", streamId, numOfStreams);
  }

  return code;
}