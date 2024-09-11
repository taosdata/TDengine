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

SStreamTaskIter* createStreamTaskIter(SStreamObj* pStream) {
  SStreamTaskIter* pIter = taosMemoryCalloc(1, sizeof(SStreamTaskIter));
  if (pIter == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pIter->level = -1;
  pIter->ordinalIndex = 0;
  pIter->pStream = pStream;
  pIter->totalLevel = taosArrayGetSize(pStream->tasks);
  pIter->pTask = NULL;

  return pIter;
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

SStreamTask* streamTaskIterGetCurrent(SStreamTaskIter* pIter) {
  return pIter->pTask;
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

// NOTE: choose the version in 3.0 branch.
SArray* mndTakeVgroupSnapshot(SMnode *pMnode, bool *allReady) {
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
    addEpIntoEpSet(&entry.epset, pObj->pDnode->fqdn, pObj->pDnode->port);
//    if (code) {
//      sdbRelease(pSdb, pObj);
//      mError("failed to extract epset for fqdn:%s during task vgroup snapshot", pObj->pDnode->fqdn);
//      goto _err;
//    }

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

  taosHashCleanup(pHash);
  return pVgroupList;

  _err:
  *allReady = false;
  taosArrayDestroy(pVgroupList);
  taosHashCleanup(pHash);

  return NULL;
}

SStreamObj *mndGetStreamObj(SMnode *pMnode, int64_t streamId) {
  void       *pIter = NULL;
  SSdb       *pSdb = pMnode->pSdb;
  SStreamObj *pStream = NULL;

  while ((pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream)) != NULL) {
    if (pStream->uid == streamId) {
      sdbCancelFetch(pSdb, pIter);
      return pStream;
    }
    sdbRelease(pSdb, pStream);
  }

  return NULL;
}

void mndKillTransImpl(SMnode *pMnode, int32_t transId, const char *pDbName) {
  STrans *pTrans = mndAcquireTrans(pMnode, transId);
  if (pTrans != NULL) {
    mInfo("kill active transId:%d in Db:%s", transId, pDbName);
    mndKillTrans(pMnode, pTrans);
    mndReleaseTrans(pMnode, pTrans);
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
      addEpIntoEpSet(pEpSet, pObj->pDnode->fqdn, pObj->pDnode->port);
      sdbRelease(pMnode->pSdb, pObj);
      sdbCancelFetch(pMnode->pSdb, pIter);
      *hasEpset = true;
      return TSDB_CODE_SUCCESS;
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
  SVResumeStreamTaskReq *pReq = taosMemoryCalloc(1, sizeof(SVResumeStreamTaskReq));
  if (pReq == NULL) {
    mError("failed to malloc in resume stream, size:%" PRIzu ", code:%s", sizeof(SVResumeStreamTaskReq),
           tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
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
    return -1;
  }

  code = setTransAction(pTrans, pReq, sizeof(SVResumeStreamTaskReq), TDMT_STREAM_TASK_RESUME, &epset, 0, 0);
  if (code != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  mDebug("set the resume action for trans:%d", pTrans->id);
  return 0;
}

SStreamTask *mndGetStreamTask(STaskId *pId, SStreamObj *pStream) {
  SStreamTaskIter *pIter = createStreamTaskIter(pStream);
  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = streamTaskIterGetCurrent(pIter);
    if (pTask->id.taskId == pId->taskId) {
      destroyStreamTaskIter(pIter);
      return pTask;
    }
  }

  destroyStreamTaskIter(pIter);
  return NULL;
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
  SStreamTaskIter *pIter = createStreamTaskIter(pStream);

  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = streamTaskIterGetCurrent(pIter);
    if (doSetResumeAction(pTrans, pMnode, pTask, igUntreated) < 0) {
      destroyStreamTaskIter(pIter);
      return -1;
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
    return -1;
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
  epsetToStr(&epset, buf, tListLen(buf));
  mDebug("pause stream task in node:%d, epset:%s", pTask->info.nodeId, buf);

  code = setTransAction(pTrans, pReq, sizeof(SVPauseStreamTaskReq), TDMT_STREAM_TASK_PAUSE, &epset, 0, 0);
  if (code != 0) {
    taosMemoryFree(pReq);
    return -1;
  }
  return 0;
}

int32_t mndStreamSetPauseAction(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream) {
  SStreamTaskIter *pIter = createStreamTaskIter(pStream);

  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = streamTaskIterGetCurrent(pIter);
    if (doSetPauseAction(pMnode, pTrans, pTask) < 0) {
      destroyStreamTaskIter(pIter);
      return -1;
    }

    if (atomic_load_8(&pTask->status.taskStatus) != TASK_STATUS__PAUSE) {
      atomic_store_8(&pTask->status.statusBackup, pTask->status.taskStatus);
      atomic_store_8(&pTask->status.taskStatus, TASK_STATUS__PAUSE);
    }
  }

  destroyStreamTaskIter(pIter);
  return 0;
}

static int32_t doSetDropAction(SMnode *pMnode, STrans *pTrans, SStreamTask *pTask) {
  SVDropStreamTaskReq *pReq = taosMemoryCalloc(1, sizeof(SVDropStreamTaskReq));
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pReq->head.vgId = htonl(pTask->info.nodeId);
  pReq->taskId = pTask->id.taskId;
  pReq->streamId = pTask->id.streamId;

  SEpSet  epset = {0};
  bool    hasEpset = false;
  int32_t code = extractNodeEpset(pMnode, &epset, &hasEpset, pTask->id.taskId, pTask->info.nodeId);
  if (code != TSDB_CODE_SUCCESS || !hasEpset) {  // no valid epset, return directly without redoAction
    terrno = code;
    return -1;
  }

  // The epset of nodeId of this task may have been expired now, let's use the newest epset from mnode.
  code = setTransAction(pTrans, pReq, sizeof(SVDropStreamTaskReq), TDMT_STREAM_TASK_DROP, &epset, 0, 0);
  if (code != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

int32_t mndStreamSetDropAction(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream) {
  SStreamTaskIter *pIter = createStreamTaskIter(pStream);

  while(streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = streamTaskIterGetCurrent(pIter);
    if (doSetDropAction(pMnode, pTrans, pTask) < 0) {
      destroyStreamTaskIter(pIter);
      return -1;
    }
  }
  destroyStreamTaskIter(pIter);
  return 0;
}

static int32_t doSetDropActionFromId(SMnode *pMnode, STrans *pTrans, SOrphanTask* pTask) {
  SVDropStreamTaskReq *pReq = taosMemoryCalloc(1, sizeof(SVDropStreamTaskReq));
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pReq->head.vgId = htonl(pTask->nodeId);
  pReq->taskId = pTask->taskId;
  pReq->streamId = pTask->streamId;

  SEpSet  epset = {0};
  bool    hasEpset = false;
  int32_t code = extractNodeEpset(pMnode, &epset, &hasEpset, pTask->taskId, pTask->nodeId);
  if (code != TSDB_CODE_SUCCESS || (!hasEpset)) {  // no valid epset, return directly without redoAction
    terrno = code;
    taosMemoryFree(pReq);
    return -1;
  }

  // The epset of nodeId of this task may have been expired now, let's use the newest epset from mnode.
  code = setTransAction(pTrans, pReq, sizeof(SVDropStreamTaskReq), TDMT_STREAM_TASK_DROP, &epset, 0, 0);
  if (code != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

int32_t mndStreamSetDropActionFromList(SMnode *pMnode, STrans *pTrans, SArray* pList) {
  for(int32_t i = 0; i < taosArrayGetSize(pList); ++i) {
    SOrphanTask* pTask = taosArrayGet(pList, i);
    mDebug("add drop task:0x%x action to drop orphan task", pTask->taskId);
    doSetDropActionFromId(pMnode, pTrans, pTask);
  }
  return 0;
}

static void initNodeUpdateMsg(SStreamTaskNodeUpdateMsg *pMsg, const SVgroupChangeInfo *pInfo, SStreamTaskId *pId,
                              int32_t transId) {
  pMsg->streamId = pId->streamId;
  pMsg->taskId = pId->taskId;
  pMsg->transId = transId;
  pMsg->pNodeList = taosArrayInit(taosArrayGetSize(pInfo->pUpdateNodeList), sizeof(SNodeUpdateInfo));
  taosArrayAddAll(pMsg->pNodeList, pInfo->pUpdateNodeList);
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
    return -1;
  }

  int32_t tlen = sizeof(SMsgHead) + blen;

  void *buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosArrayDestroy(req.pNodeList);
    return -1;
  }

  void    *abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
  tEncodeStreamTaskUpdateMsg(&encoder, &req);

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
  streamTaskUpdateEpsetInfo(pTask, pInfo->pUpdateNodeList);

  doBuildStreamTaskUpdateMsg(&pBuf, &len, pInfo, pTask->info.nodeId, &pTask->id, pTrans->id);

  SEpSet  epset = {0};
  bool    hasEpset = false;
  int32_t code = extractNodeEpset(pMnode, &epset, &hasEpset, pTask->id.taskId, pTask->info.nodeId);
  if (code != TSDB_CODE_SUCCESS || !hasEpset) {
    terrno = code;
    return code;
  }

  code = setTransAction(pTrans, pBuf, len, TDMT_VND_STREAM_TASK_UPDATE, &epset, 0, TSDB_CODE_VND_INVALID_VGROUP_ID);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pBuf);
  }

  return code;
}

// build trans to update the epset
int32_t mndStreamSetUpdateEpsetAction(SMnode *pMnode, SStreamObj *pStream, SVgroupChangeInfo *pInfo, STrans *pTrans) {
  mDebug("stream:0x%" PRIx64 " set tasks epset update action", pStream->uid);
  taosWLockLatch(&pStream->lock);

  SStreamTaskIter *pIter = createStreamTaskIter(pStream);
  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = streamTaskIterGetCurrent(pIter);
    int32_t      code = doSetUpdateTaskAction(pMnode, pTrans, pTask, pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      destroyStreamTaskIter(pIter);
      taosWUnLockLatch(&pStream->lock);
      return -1;
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
  taosWLockLatch(&pStream->lock);

  SStreamTaskIter *pIter = createStreamTaskIter(pStream);
  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = streamTaskIterGetCurrent(pIter);
    int32_t      code = doSetResetAction(pMnode, pTrans, pTask);
    if (code != TSDB_CODE_SUCCESS) {
      destroyStreamTaskIter(pIter);
      taosWUnLockLatch(&pStream->lock);
      return -1;
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

void mndInitExecInfo() {
  taosThreadMutexInit(&execInfo.lock, NULL);
  _hash_fn_t fn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR);

  execInfo.pTaskList = taosArrayInit(4, sizeof(STaskId));
  execInfo.pTaskMap = taosHashInit(64, fn, true, HASH_NO_LOCK);
  execInfo.transMgmt.pDBTrans = taosHashInit(32, fn, true, HASH_NO_LOCK);
  execInfo.pTransferStateStreams = taosHashInit(32, fn, true, HASH_NO_LOCK);
  execInfo.pChkptStreams = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  execInfo.pNodeList = taosArrayInit(4, sizeof(SNodeEntry));

  taosHashSetFreeFp(execInfo.pTransferStateStreams, freeTaskList);
  taosHashSetFreeFp(execInfo.pChkptStreams, freeTaskList);
}

void removeExpiredNodeInfo(const SArray *pNodeSnapshot) {
  SArray *pValidList = taosArrayInit(4, sizeof(SNodeEntry));
  int32_t size = taosArrayGetSize(pNodeSnapshot);

  for (int32_t i = 0; i < taosArrayGetSize(execInfo.pNodeList); ++i) {
    SNodeEntry *p = taosArrayGet(execInfo.pNodeList, i);

    for (int32_t j = 0; j < size; ++j) {
      SNodeEntry *pEntry = taosArrayGet(pNodeSnapshot, j);
      if (pEntry->nodeId == p->nodeId) {
        taosArrayPush(pValidList, p);
        break;
      }
    }
  }

  taosArrayDestroy(execInfo.pNodeList);
  execInfo.pNodeList = pValidList;

  mDebug("remain %d valid node entries after clean expired nodes info", (int32_t)taosArrayGetSize(pValidList));
}

int32_t doRemoveTasks(SStreamExecInfo *pExecNode, STaskId *pRemovedId) {
  void *p = taosHashGet(pExecNode->pTaskMap, pRemovedId, sizeof(*pRemovedId));
  if (p == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  taosHashRemove(pExecNode->pTaskMap, pRemovedId, sizeof(*pRemovedId));

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
    doRemoveTasks(pExecInfo, pId);
  }
}

void removeStreamTasksInBuf(SStreamObj *pStream, SStreamExecInfo *pExecNode) {
  taosThreadMutexLock(&pExecNode->lock);

  SStreamTaskIter *pIter = createStreamTaskIter(pStream);
  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = streamTaskIterGetCurrent(pIter);

    STaskId id = {.streamId = pTask->id.streamId, .taskId = pTask->id.taskId};
    doRemoveTasks(pExecNode, &id);
  }

  ASSERT(taosHashGetSize(pExecNode->pTaskMap) == taosArrayGetSize(pExecNode->pTaskList));
  taosThreadMutexUnlock(&pExecNode->lock);

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
      taosArrayPush(pRemovedTasks, pId);
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
  taosWLockLatch(&pStream->lock);

  SStreamTaskIter *pIter = createStreamTaskIter(pStream);
  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = streamTaskIterGetCurrent(pIter);

    int32_t code = doSetUpdateChkptAction(pMnode, pTrans, pTask);
    if (code != TSDB_CODE_SUCCESS) {
      destroyStreamTaskIter(pIter);
      taosWUnLockLatch(&pStream->lock);
      return -1;
    }
  }

  destroyStreamTaskIter(pIter);
  taosWUnLockLatch(&pStream->lock);
  return 0;
}

int32_t mndScanCheckpointReportInfo(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  void   *pIter = NULL;
  SArray *pDropped = taosArrayInit(4, sizeof(int64_t));

  mDebug("start to scan checkpoint report info");

  while ((pIter = taosHashIterate(execInfo.pChkptStreams, pIter)) != NULL) {
    SArray *pList = *(SArray **)pIter;

    STaskChkptInfo* pInfo = taosArrayGet(pList, 0);
    SStreamObj* pStream = mndGetStreamObj(pMnode, pInfo->streamId);
    if (pStream == NULL) {
      mDebug("failed to acquire stream:0x%" PRIx64 " remove it from checkpoint-report list", pInfo->streamId);
      taosArrayPush(pDropped, &pInfo->streamId);
      continue;
    }

    int32_t total = mndGetNumOfStreamTasks(pStream);
    int32_t existed = (int32_t) taosArrayGetSize(pList);

    if (total == existed) {
      mDebug("stream:0x%" PRIx64 " %s all %d tasks send checkpoint-report, start to update checkpoint-info",
             pStream->uid, pStream->name, total);

      bool conflict = mndStreamTransConflictCheck(pMnode, pStream->uid, MND_STREAM_CHKPT_UPDATE_NAME, false);
      if (!conflict) {
        int32_t code = mndCreateStreamChkptInfoUpdateTrans(pMnode, pStream, pList);
        if (code == TSDB_CODE_SUCCESS || code == TSDB_CODE_ACTION_IN_PROGRESS) { // remove this entry
          taosArrayPush(pDropped, &pInfo->streamId);
          mDebug("stream:0x%" PRIx64 " removed", pInfo->streamId);
        } else {
          mDebug("stream:0x%" PRIx64 " not launch chkpt-meta update trans, due to checkpoint not finished yet",
                 pInfo->streamId);
        }
        break;
      } else {
        mDebug("stream:0x%"PRIx64" active checkpoint trans not finished yet, wait", pInfo->streamId);
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
      taosHashRemove(execInfo.pChkptStreams, &streamId, sizeof(streamId));
    }

    int32_t numOfStreams = taosHashGetSize(execInfo.pChkptStreams);
    mDebug("drop %d stream(s) in checkpoint-report list, remain:%d", size, numOfStreams);
  }

  taosArrayDestroy(pDropped);
  return TSDB_CODE_SUCCESS;
}