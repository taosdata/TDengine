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

#include "mndDb.h"
#include "mndStb.h"
#include "mndStream.h"
#include "mndTrans.h"
#include "mndVgroup.h"
#include "taoserror.h"
#include "tmisce.h"

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

static bool checkStatusForEachReplica(SVgObj *pVgroup) {
  for (int32_t i = 0; i < pVgroup->replica; ++i) {
    if (!pVgroup->vnodeGid[i].syncRestore) {
      mInfo("vgId:%d not restored, not ready for checkpoint or other operations", pVgroup->vgId);
      return false;
    }

    ESyncState state = pVgroup->vnodeGid[i].syncState;
    if (state == TAOS_SYNC_STATE_OFFLINE || state == TAOS_SYNC_STATE_ERROR || state == TAOS_SYNC_STATE_LEARNER ||
        state == TAOS_SYNC_STATE_CANDIDATE) {
      mInfo("vgId:%d state:%d , not ready for checkpoint or other operations, not check other vgroups", pVgroup->vgId,
            state);
      return false;
    }
  }

  return true;
}

int32_t mndTakeVgroupSnapshot(SMnode *pMnode, bool *allReady, SArray **pList) {
  SSdb     *pSdb = pMnode->pSdb;
  void     *pIter = NULL;
  SVgObj   *pVgroup = NULL;
  int32_t   code = 0;
  SArray   *pVgroupList = NULL;
  SHashObj *pHash = NULL;

  pVgroupList = taosArrayInit(4, sizeof(SNodeEntry));
  if (pVgroupList == NULL) {
    mError("failed to prepare arraylist during take vgroup snapshot, code:%s", tstrerror(terrno));
    code = terrno;
    goto _err;
  }

  pHash = taosHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (pHash == NULL) {
    mError("failed to prepare hashmap during take vgroup snapshot, code:%s", tstrerror(terrno));
    code = terrno;
    goto _err;
  }

  *allReady = true;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) {
      break;
    }

    SNodeEntry entry = {.nodeId = pVgroup->vgId, .hbTimestamp = pVgroup->updateTime};
    entry.epset = mndGetVgroupEpset(pMnode, pVgroup);

    int8_t *pReplica = taosHashGet(pHash, &pVgroup->dbUid, sizeof(pVgroup->dbUid));
    if (pReplica == NULL) {  // not exist, add it into hash map
      code = taosHashPut(pHash, &pVgroup->dbUid, sizeof(pVgroup->dbUid), &pVgroup->replica, sizeof(pVgroup->replica));
      if (code) {
        mError("failed to put info into hashmap during task vgroup snapshot, code:%s", tstrerror(code));
        sdbRelease(pSdb, pVgroup);
        sdbCancelFetch(pSdb, pIter);
        goto _err;  // take snapshot failed, and not all ready
      }
    } else {
      if (*pReplica != pVgroup->replica) {
        mInfo("vgId:%d replica:%d inconsistent with other vgroups replica:%d, not ready for stream operations",
              pVgroup->vgId, pVgroup->replica, *pReplica);
        *allReady = false;  // task snap success, but not all ready
      }
    }

    // if not all ready till now, no need to check the remaining vgroups.
    // but still we need to put the info of the existed vgroups into the snapshot list
    if (*allReady) {
      *allReady = checkStatusForEachReplica(pVgroup);
    }

    char buf[256] = {0};
    (void)epsetToStr(&entry.epset, buf, tListLen(buf));

    void *p = taosArrayPush(pVgroupList, &entry);
    if (p == NULL) {
      mError("failed to put entry in vgroup list, nodeId:%d code:out of memory", entry.nodeId);
      code = terrno;
      sdbRelease(pSdb, pVgroup);
      sdbCancelFetch(pSdb, pIter);
      goto _err;
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
      sdbCancelFetch(pSdb, pIter);
      mError("failed to extract epset for fqdn:%s during task vgroup snapshot", pObj->pDnode->fqdn);
      goto _err;
    }

    char buf[256] = {0};
    (void)epsetToStr(&entry.epset, buf, tListLen(buf));

    void *p = taosArrayPush(pVgroupList, &entry);
    if (p == NULL) {
      code = terrno;
      sdbRelease(pSdb, pObj);
      sdbCancelFetch(pSdb, pIter);
      mError("failed to put entry in vgroup list, nodeId:%d code:%s", entry.nodeId, tstrerror(code));
      goto _err;
    } else {
      mDebug("take snode snapshot, nodeId:%d %s", entry.nodeId, buf);
    }

    sdbRelease(pSdb, pObj);
  }

  *pList = pVgroupList;
  taosHashCleanup(pHash);
  return code;

_err:
  *allReady = false;
  taosArrayDestroy(pVgroupList);
  taosHashCleanup(pHash);

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

  code = setTransAction(pTrans, pReq, sizeof(SVResumeStreamTaskReq), TDMT_STREAM_TASK_RESUME, &epset, 0, TSDB_CODE_VND_INVALID_VGROUP_ID);
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

  code = setTransAction(pTrans, pReq, sizeof(SVPauseStreamTaskReq), TDMT_STREAM_TASK_PAUSE, &epset, 0, TSDB_CODE_VND_INVALID_VGROUP_ID);
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
  code = setTransAction(pTrans, pReq, sizeof(SVDropStreamTaskReq), TDMT_STREAM_TASK_DROP, &epset, 0, TSDB_CODE_VND_INVALID_VGROUP_ID);
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
  code = setTransAction(pTrans, pReq, sizeof(SVDropStreamTaskReq), TDMT_STREAM_TASK_DROP, &epset, 0, TSDB_CODE_VND_INVALID_VGROUP_ID);
  if (code != 0) {
    taosMemoryFree(pReq);
    return code;
  }

  return 0;
}

int32_t mndStreamSetDropActionFromList(SMnode *pMnode, STrans *pTrans, SArray* pList) {
  for(int32_t i = 0; i < taosArrayGetSize(pList); ++i) {
    SOrphanTask* pTask = taosArrayGet(pList, i);
    if (pTask == NULL) {
      return terrno;
    }

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
    mError("failed to prepare node list, code:%s", tstrerror(terrno));
    code = terrno;
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
  (void)streamTaskUpdateEpsetInfo(pTask, pInfo->pUpdateNodeList);

  int32_t code = doBuildStreamTaskUpdateMsg(&pBuf, &len, pInfo, pTask->info.nodeId, &pTask->id, pTrans->id);
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
    mError("failed to malloc in reset stream, size:%" PRIzu ", code:%s", sizeof(SVResetStreamTaskReq),
           tstrerror(terrno));
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

  code = setTransAction(pTrans, pReq, sizeof(SVResetStreamTaskReq), TDMT_VND_STREAM_TASK_RESET, &epset, 0, TSDB_CODE_VND_INVALID_VGROUP_ID);
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
  execInfo.pKilledChkptTrans = taosArrayInit(4, sizeof(SStreamTaskResetMsg));

  if (execInfo.pTaskList == NULL || execInfo.pTaskMap == NULL || execInfo.transMgmt.pDBTrans == NULL ||
      execInfo.pTransferStateStreams == NULL || execInfo.pChkptStreams == NULL || execInfo.pStreamConsensus == NULL ||
      execInfo.pNodeList == NULL || execInfo.pKilledChkptTrans == NULL) {
    mError("failed to initialize the stream runtime env, code:%s", tstrerror(terrno));
    return terrno;
  }

  execInfo.role = NODE_ROLE_UNINIT;
  execInfo.switchFromFollower = false;

  taosHashSetFreeFp(execInfo.pTransferStateStreams, freeTaskList);
  taosHashSetFreeFp(execInfo.pChkptStreams, freeTaskList);
  taosHashSetFreeFp(execInfo.pStreamConsensus, freeTaskList);
  return 0;
}

void removeExpiredNodeInfo(const SArray *pNodeSnapshot) {
  SArray *pValidList = taosArrayInit(4, sizeof(SNodeEntry));
  if (pValidList == NULL) {  // not continue
    return;
  }

  int32_t size = taosArrayGetSize(pNodeSnapshot);
  int32_t oldSize = taosArrayGetSize(execInfo.pNodeList);

  for (int32_t i = 0; i < oldSize; ++i) {
    SNodeEntry *p = taosArrayGet(execInfo.pNodeList, i);
    if (p == NULL) {
      continue;
    }

    for (int32_t j = 0; j < size; ++j) {
      SNodeEntry *pEntry = taosArrayGet(pNodeSnapshot, j);
      if (pEntry == NULL) {
        continue;
      }

      if (pEntry->nodeId == p->nodeId) {
        p->hbTimestamp = pEntry->hbTimestamp;

        void* px = taosArrayPush(pValidList, p);
        if (px == NULL) {
          mError("failed to put node into list, nodeId:%d", p->nodeId);
        } else {
          mDebug("vgId:%d ts:%" PRId64 " HbMsgId:%d is valid", p->nodeId, p->hbTimestamp, p->lastHbMsgId);
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
    if (pId == NULL) {
      continue;
    }

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
    if (pId == NULL) {
      continue;
    }

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

  if (taosHashGetSize(pExecNode->pTaskMap) != taosArrayGetSize(pExecNode->pTaskList)) {
    streamMutexUnlock(&pExecNode->lock);
    destroyStreamTaskIter(pIter);
    mError("task map size:%d, task list size:%" PRId64 " not equal", taosHashGetSize(pExecNode->pTaskMap),
           taosArrayGetSize(pExecNode->pTaskList));
    return;
  }

  // 2. remove stream entry in consensus hash table and checkpoint-report hash table
  (void) mndClearConsensusCheckpointId(execInfo.pStreamConsensus, pStream->uid);
  (void) mndClearChkptReportInfo(execInfo.pChkptStreams, pStream->uid);

  streamMutexUnlock(&pExecNode->lock);
  destroyStreamTaskIter(pIter);
}

static bool taskNodeExists(SArray *pList, int32_t nodeId) {
  size_t num = taosArrayGetSize(pList);

  for (int32_t i = 0; i < num; ++i) {
    SNodeEntry *pEntry = taosArrayGet(pList, i);
    if (pEntry == NULL) {
      continue;
    }

    if (pEntry->nodeId == nodeId) {
      return true;
    }
  }

  return false;
}

int32_t removeExpiredNodeEntryAndTaskInBuf(SArray *pNodeSnapshot) {
  SArray *pRemovedTasks = taosArrayInit(4, sizeof(STaskId));
  if (pRemovedTasks == NULL) {
    return terrno;
  }

  int32_t numOfTask = taosArrayGetSize(execInfo.pTaskList);
  for (int32_t i = 0; i < numOfTask; ++i) {
    STaskId *pId = taosArrayGet(execInfo.pTaskList, i);
    if (pId == NULL) {
      continue;
    }

    STaskStatusEntry *pEntry = taosHashGet(execInfo.pTaskMap, pId, sizeof(*pId));
    if (pEntry == NULL) {
      continue;
    }

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
    mError("failed to malloc in reset stream, size:%" PRIzu ", code:%s", sizeof(SVUpdateCheckpointInfoReq),
           tstrerror(terrno));
    return terrno;
  }

  pReq->head.vgId = htonl(pTask->info.nodeId);
  pReq->taskId = pTask->id.taskId;
  pReq->streamId = pTask->id.streamId;

  SChkptReportInfo *pStreamItem = (SChkptReportInfo*)taosHashGet(execInfo.pChkptStreams, &pTask->id.streamId, sizeof(pTask->id.streamId));
  if (pStreamItem == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t size = taosArrayGetSize(pStreamItem->pTaskList);
  for(int32_t i = 0; i < size; ++i) {
    STaskChkptInfo* pInfo = taosArrayGet(pStreamItem->pTaskList, i);
    if (pInfo == NULL) {
      continue;
    }

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
  int32_t code = 0;
  SArray *pDropped = taosArrayInit(4, sizeof(int64_t));
  if (pDropped == NULL) {
    return terrno;
  }

  mDebug("start to scan checkpoint report info");
  streamMutexLock(&execInfo.lock);

  while ((pIter = taosHashIterate(execInfo.pChkptStreams, pIter)) != NULL) {
    SChkptReportInfo* px = (SChkptReportInfo *)pIter;
    if (taosArrayGetSize(px->pTaskList) == 0) {
      continue;
    }

    STaskChkptInfo *pInfo = taosArrayGet(px->pTaskList, 0);
    if (pInfo == NULL) {
      continue;
    }

    SStreamObj *pStream = NULL;
    code = mndGetStreamObj(pMnode, pInfo->streamId, &pStream);
    if (pStream == NULL || code != 0) {
      mDebug("failed to acquire stream:0x%" PRIx64 " remove it from checkpoint-report list", pInfo->streamId);
      void *p = taosArrayPush(pDropped, &pInfo->streamId);
      if (p == NULL) {
        mError("failed to put stream into drop list:0x%" PRIx64, pInfo->streamId);
      }
      continue;
    }

    int32_t total = mndGetNumOfStreamTasks(pStream);
    int32_t existed = (int32_t)taosArrayGetSize(px->pTaskList);

    if (total == existed) {
      mDebug("stream:0x%" PRIx64 " %s all %d tasks send checkpoint-report, start to update checkpoint-info",
             pStream->uid, pStream->name, total);

      bool conflict = mndStreamTransConflictCheck(pMnode, pStream->uid, MND_STREAM_CHKPT_UPDATE_NAME, false);
      if (!conflict) {
        code = mndCreateStreamChkptInfoUpdateTrans(pMnode, pStream, px->pTaskList);
        if (code == TSDB_CODE_SUCCESS || code == TSDB_CODE_ACTION_IN_PROGRESS) {  // remove this entry
          taosArrayClear(px->pTaskList);
          px->reportChkpt = pInfo->checkpointId;
          mDebug("stream:0x%" PRIx64 " clear checkpoint-report list", pInfo->streamId);
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
      int64_t* pStreamId = (int64_t *)taosArrayGet(pDropped, i);
      if (pStreamId == NULL) {
        continue;
      }

      code = taosHashRemove(execInfo.pChkptStreams, pStreamId, sizeof(*pStreamId));
      if (code) {
        mError("failed to remove stream in buf:0x%"PRIx64, *pStreamId);
      }
    }

    int32_t numOfStreams = taosHashGetSize(execInfo.pChkptStreams);
    mDebug("drop %d stream(s) in checkpoint-report list, remain:%d", size, numOfStreams);
  }

  streamMutexUnlock(&execInfo.lock);

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
    return terrno;
  }

  int32_t tlen = sizeof(SMsgHead) + blen;

  void *pBuf = taosMemoryMalloc(tlen);
  if (pBuf == NULL) {
    return terrno;
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
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
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

  if (p.pTaskList == NULL) {
    return terrno;
  }

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
    if (p == NULL) {
      continue;
    }

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
    return code;
  }

  code = taosHashRemove(pHash, &streamId, sizeof(streamId));
  if (code == 0) {
    mDebug("drop stream:0x%" PRIx64 " in consensus-checkpointId list, remain:%d", streamId, numOfStreams);
  } else {
    mError("failed to remove stream:0x%"PRIx64" in consensus-checkpointId list, remain:%d", streamId, numOfStreams);
  }

  return code;
}

int64_t mndClearChkptReportInfo(SHashObj* pHash, int64_t streamId) {
  int32_t code = 0;
  int32_t numOfStreams = taosHashGetSize(pHash);
  if (numOfStreams == 0) {
    return code;
  }

  code = taosHashRemove(pHash, &streamId, sizeof(streamId));
  if (code == 0) {
    mDebug("drop stream:0x%" PRIx64 " in chkpt-report list, remain:%d", streamId, numOfStreams);
  } else {
    mError("failed to remove stream:0x%"PRIx64" in chkpt-report list, remain:%d", streamId, numOfStreams);
  }

  return code;
}

int32_t mndResetChkptReportInfo(SHashObj* pHash, int64_t streamId) {
  SChkptReportInfo* pInfo = taosHashGet(pHash, &streamId, sizeof(streamId));
  if (pInfo != NULL) {
    taosArrayClear(pInfo->pTaskList);
    mDebug("stream:0x%" PRIx64 " checkpoint-report list cleared, prev report checkpointId:%" PRId64, streamId,
           pInfo->reportChkpt);
    return 0;
  }

  return TSDB_CODE_MND_STREAM_NOT_EXIST;
}

static void mndShowStreamStatus(char *dst, SStreamObj *pStream) {
  int8_t status = atomic_load_8(&pStream->status);
  if (status == STREAM_STATUS__NORMAL) {
    strcpy(dst, "ready");
  } else if (status == STREAM_STATUS__STOP) {
    strcpy(dst, "stop");
  } else if (status == STREAM_STATUS__FAILED) {
    strcpy(dst, "failed");
  } else if (status == STREAM_STATUS__RECOVER) {
    strcpy(dst, "recover");
  } else if (status == STREAM_STATUS__PAUSE) {
    strcpy(dst, "paused");
  }
}

static void mndShowStreamTrigger(char *dst, SStreamObj *pStream) {
  int8_t trigger = pStream->conf.trigger;
  if (trigger == STREAM_TRIGGER_AT_ONCE) {
    strcpy(dst, "at once");
  } else if (trigger == STREAM_TRIGGER_WINDOW_CLOSE) {
    strcpy(dst, "window close");
  } else if (trigger == STREAM_TRIGGER_MAX_DELAY) {
    strcpy(dst, "max delay");
  }
}

static void int64ToHexStr(int64_t id, char *pBuf, int32_t bufLen) {
  memset(pBuf, 0, bufLen);
  pBuf[2] = '0';
  pBuf[3] = 'x';

  int32_t len = tintToHex(id, &pBuf[4]);
  varDataSetLen(pBuf, len + 2);
}

int32_t setStreamAttrInResBlock(SStreamObj *pStream, SSDataBlock *pBlock, int32_t numOfRows) {
  int32_t code = 0;
  int32_t cols = 0;
  int32_t lino = 0;

  char streamName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(streamName, mndGetDbStr(pStream->name), sizeof(streamName));
  SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)streamName, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // create time
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pStream->createTime, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // stream id
  char buf[128] = {0};
  int64ToHexStr(pStream->uid, buf, tListLen(buf));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, buf, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // related fill-history stream id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  if (pStream->hTaskUid != 0) {
    int64ToHexStr(pStream->hTaskUid, buf, tListLen(buf));
    code = colDataSetVal(pColInfo, numOfRows, buf, false);
  } else {
    code = colDataSetVal(pColInfo, numOfRows, buf, true);
  }
  TSDB_CHECK_CODE(code, lino, _end);

  // related fill-history stream id
  char sql[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(sql, pStream->sql, sizeof(sql));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)sql, false);
  TSDB_CHECK_CODE(code, lino, _end);

  char status[20 + VARSTR_HEADER_SIZE] = {0};
  char status2[20] = {0};
  mndShowStreamStatus(status2, pStream);
  STR_WITH_MAXSIZE_TO_VARSTR(status, status2, sizeof(status));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&status, false);
  TSDB_CHECK_CODE(code, lino, _end);

  char sourceDB[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(sourceDB, mndGetDbStr(pStream->sourceDb), sizeof(sourceDB));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&sourceDB, false);
  TSDB_CHECK_CODE(code, lino, _end);

  char targetDB[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(targetDB, mndGetDbStr(pStream->targetDb), sizeof(targetDB));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&targetDB, false);
  TSDB_CHECK_CODE(code, lino, _end);

  if (pStream->targetSTbName[0] == 0) {
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

    code = colDataSetVal(pColInfo, numOfRows, NULL, true);
  } else {
    char targetSTB[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(targetSTB, mndGetStbStr(pStream->targetSTbName), sizeof(targetSTB));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

    code = colDataSetVal(pColInfo, numOfRows, (const char *)&targetSTB, false);
  }
  TSDB_CHECK_CODE(code, lino, _end);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pStream->conf.watermark, false);
  TSDB_CHECK_CODE(code, lino, _end);

  char trigger[20 + VARSTR_HEADER_SIZE] = {0};
  char trigger2[20] = {0};
  mndShowStreamTrigger(trigger2, pStream);
  STR_WITH_MAXSIZE_TO_VARSTR(trigger, trigger2, sizeof(trigger));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&trigger, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // sink_quota
  char sinkQuota[20 + VARSTR_HEADER_SIZE] = {0};
  sinkQuota[0] = '0';
  char dstStr[20] = {0};
  STR_TO_VARSTR(dstStr, sinkQuota)
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)dstStr, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // checkpoint interval
  char tmp[20 + VARSTR_HEADER_SIZE] = {0};
  sprintf(varDataVal(tmp), "%d sec", tsStreamCheckpointInterval);
  varDataSetLen(tmp, strlen(varDataVal(tmp)));

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)tmp, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // checkpoint backup type
  char backup[20 + VARSTR_HEADER_SIZE] = {0};
  STR_TO_VARSTR(backup, "none")
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)backup, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // history scan idle
  char scanHistoryIdle[20 + VARSTR_HEADER_SIZE] = {0};
  strcpy(scanHistoryIdle, "100a");

  memset(dstStr, 0, tListLen(dstStr));
  STR_TO_VARSTR(dstStr, scanHistoryIdle)
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)dstStr, false);

_end:
  if (code) {
    mError("error happens when build stream attr result block, lino:%d, code:%s", lino, tstrerror(code));
  }
  return code;
}

int32_t setTaskAttrInResBlock(SStreamObj *pStream, SStreamTask *pTask, SSDataBlock *pBlock, int32_t numOfRows) {
  SColumnInfoData *pColInfo = NULL;
  int32_t          cols = 0;
  int32_t          code = 0;
  int32_t          lino = 0;

  STaskId id = {.streamId = pTask->id.streamId, .taskId = pTask->id.taskId};

  STaskStatusEntry *pe = taosHashGet(execInfo.pTaskMap, &id, sizeof(id));
  if (pe == NULL) {
    mError("task:0x%" PRIx64 " not exists in any vnodes, streamName:%s, streamId:0x%" PRIx64 " createTs:%" PRId64
               " no valid status/stage info",
           id.taskId, pStream->name, pStream->uid, pStream->createTime);
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  // stream name
  char streamName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(streamName, mndGetDbStr(pStream->name), sizeof(streamName));

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)streamName, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // task id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  char idstr[128] = {0};
  int64ToHexStr(pTask->id.taskId, idstr, tListLen(idstr));
  code = colDataSetVal(pColInfo, numOfRows, idstr, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // node type
  char nodeType[20 + VARSTR_HEADER_SIZE] = {0};
  varDataSetLen(nodeType, 5);
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  if (pTask->info.nodeId > 0) {
    memcpy(varDataVal(nodeType), "vnode", 5);
  } else {
    memcpy(varDataVal(nodeType), "snode", 5);
  }
  code = colDataSetVal(pColInfo, numOfRows, nodeType, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // node id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  int64_t nodeId = TMAX(pTask->info.nodeId, 0);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)&nodeId, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // level
  char level[20 + VARSTR_HEADER_SIZE] = {0};
  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    memcpy(varDataVal(level), "source", 6);
    varDataSetLen(level, 6);
  } else if (pTask->info.taskLevel == TASK_LEVEL__AGG) {
    memcpy(varDataVal(level), "agg", 3);
    varDataSetLen(level, 3);
  } else if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
    memcpy(varDataVal(level), "sink", 4);
    varDataSetLen(level, 4);
  }

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)level, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // status
  char status[20 + VARSTR_HEADER_SIZE] = {0};

  const char *pStatus = streamTaskGetStatusStr(pe->status);
  STR_TO_VARSTR(status, pStatus);

  // status
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)status, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // stage
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pe->stage, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // input queue
  char        vbuf[40] = {0};
  char        buf[38] = {0};
  const char *queueInfoStr = "%4.2f MiB (%6.2f%)";
  snprintf(buf, tListLen(buf), queueInfoStr, pe->inputQUsed, pe->inputRate);
  STR_TO_VARSTR(vbuf, buf);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)vbuf, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // input total
  const char *formatTotalMb = "%7.2f MiB";
  const char *formatTotalGb = "%7.2f GiB";
  if (pe->procsTotal < 1024) {
    snprintf(buf, tListLen(buf), formatTotalMb, pe->procsTotal);
  } else {
    snprintf(buf, tListLen(buf), formatTotalGb, pe->procsTotal / 1024);
  }

  memset(vbuf, 0, tListLen(vbuf));
  STR_TO_VARSTR(vbuf, buf);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)vbuf, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // process throughput
  const char *formatKb = "%7.2f KiB/s";
  const char *formatMb = "%7.2f MiB/s";
  if (pe->procsThroughput < 1024) {
    snprintf(buf, tListLen(buf), formatKb, pe->procsThroughput);
  } else {
    snprintf(buf, tListLen(buf), formatMb, pe->procsThroughput / 1024);
  }

  memset(vbuf, 0, tListLen(vbuf));
  STR_TO_VARSTR(vbuf, buf);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)vbuf, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // output total
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
    colDataSetNULL(pColInfo, numOfRows);
  } else {
    sprintf(buf, formatTotalMb, pe->outputTotal);
    memset(vbuf, 0, tListLen(vbuf));
    STR_TO_VARSTR(vbuf, buf);

    code = colDataSetVal(pColInfo, numOfRows, (const char *)vbuf, false);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  // output throughput
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
    colDataSetNULL(pColInfo, numOfRows);
  } else {
    if (pe->outputThroughput < 1024) {
      snprintf(buf, tListLen(buf), formatKb, pe->outputThroughput);
    } else {
      snprintf(buf, tListLen(buf), formatMb, pe->outputThroughput / 1024);
    }

    memset(vbuf, 0, tListLen(vbuf));
    STR_TO_VARSTR(vbuf, buf);

    code = colDataSetVal(pColInfo, numOfRows, (const char *)vbuf, false);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  // output queue
  //          sprintf(buf, queueInfoStr, pe->outputQUsed, pe->outputRate);
  //        STR_TO_VARSTR(vbuf, buf);

  //        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  //        colDataSetVal(pColInfo, numOfRows, (const char*)vbuf, false);

  // info
  if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
    const char *sinkStr = "%.2f MiB";
    snprintf(buf, tListLen(buf), sinkStr, pe->sinkDataSize);
  } else if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    // offset info
    const char *offsetStr = "%" PRId64 " [%" PRId64 ", %" PRId64 "]";
    snprintf(buf, tListLen(buf), offsetStr, pe->processedVer, pe->verRange.minVer, pe->verRange.maxVer);
  } else {
    memset(buf, 0, tListLen(buf));
  }

  STR_TO_VARSTR(vbuf, buf);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)vbuf, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // start_time
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pe->startTime, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // start id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pe->startCheckpointId, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // start ver
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pe->startCheckpointVer, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // checkpoint time
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  if (pe->checkpointInfo.latestTime != 0) {
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pe->checkpointInfo.latestTime, false);
  } else {
    code = colDataSetVal(pColInfo, numOfRows, 0, true);
  }
  TSDB_CHECK_CODE(code, lino, _end);

  // checkpoint_id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pe->checkpointInfo.latestId, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // checkpoint version
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pe->checkpointInfo.latestVer, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // checkpoint size
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  colDataSetNULL(pColInfo, numOfRows);

  // checkpoint backup status
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, 0, true);
  TSDB_CHECK_CODE(code, lino, _end);

  // ds_err_info
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, 0, true);
  TSDB_CHECK_CODE(code, lino, _end);

  // history_task_id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  if (pe->hTaskId != 0) {
    int64ToHexStr(pe->hTaskId, idstr, tListLen(idstr));
    code = colDataSetVal(pColInfo, numOfRows, idstr, false);
  } else {
    code = colDataSetVal(pColInfo, numOfRows, 0, true);
  }
  TSDB_CHECK_CODE(code, lino, _end);

  // history_task_status
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, 0, true);
  TSDB_CHECK_CODE(code, lino, _end);

  _end:
  if (code) {
    mError("error happens during build task attr result blocks, lino:%d, code:%s", lino, tstrerror(code));
  }
  return code;
}