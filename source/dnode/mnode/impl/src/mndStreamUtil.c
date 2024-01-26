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

SArray *mndTakeVgroupSnapshot(SMnode *pMnode, bool *allReady) {
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  SVgObj *pVgroup = NULL;

  *allReady = true;
  SArray *pVgroupListSnapshot = taosArrayInit(4, sizeof(SNodeEntry));

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) {
      break;
    }

    SNodeEntry entry = {.nodeId = pVgroup->vgId, .hbTimestamp = pVgroup->updateTime};
    entry.epset = mndGetVgroupEpset(pMnode, pVgroup);

    // if not all ready till now, no need to check the remaining vgroups.
    if (*allReady) {
      for (int32_t i = 0; i < pVgroup->replica; ++i) {
        if (!pVgroup->vnodeGid[i].syncRestore) {
          mInfo("vgId:%d not restored, not ready for checkpoint or other operations", pVgroup->vgId);
          *allReady = false;
          break;
        }

        ESyncState state = pVgroup->vnodeGid[i].syncState;
        if (state == TAOS_SYNC_STATE_OFFLINE || state == TAOS_SYNC_STATE_ERROR) {
          mInfo("vgId:%d offline/err, not ready for checkpoint or other operations", pVgroup->vgId);
          *allReady = false;
          break;
        }
      }
    }

    char buf[256] = {0};
    EPSET_TO_STR(&entry.epset, buf);
    mDebug("take node snapshot, nodeId:%d %s", entry.nodeId, buf);
    taosArrayPush(pVgroupListSnapshot, &entry);
    sdbRelease(pSdb, pVgroup);
  }

  SSnodeObj *pObj = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_SNODE, pIter, (void **)&pObj);
    if (pIter == NULL) {
      break;
    }

    SNodeEntry entry = {0};
    addEpIntoEpSet(&entry.epset, pObj->pDnode->fqdn, pObj->pDnode->port);
    entry.nodeId = SNODE_HANDLE;

    char buf[256] = {0};
    EPSET_TO_STR(&entry.epset, buf);
    mDebug("take snode snapshot, nodeId:%d %s", entry.nodeId, buf);
    taosArrayPush(pVgroupListSnapshot, &entry);
    sdbRelease(pSdb, pObj);
  }

  return pVgroupListSnapshot;
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

static int32_t doResumeStreamTask(STrans *pTrans, SMnode *pMnode, SStreamTask *pTask, int8_t igUntreated) {
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
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    taosMemoryFree(pReq);
    return -1;
  }

  STransAction action = {0};
  initTransAction(&action, pReq, sizeof(SVResumeStreamTaskReq), TDMT_STREAM_TASK_RESUME, &epset, 0);
  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }
  return 0;
}

SStreamTask *mndGetStreamTask(STaskId *pId, SStreamObj *pStream) {
  for (int32_t i = 0; i < taosArrayGetSize(pStream->tasks); i++) {
    SArray *pLevel = taosArrayGetP(pStream->tasks, i);

    int32_t numOfLevels = taosArrayGetSize(pLevel);
    for (int32_t j = 0; j < numOfLevels; j++) {
      SStreamTask *pTask = taosArrayGetP(pLevel, j);
      if (pTask->id.taskId == pId->taskId) {
        return pTask;
      }
    }
  }

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

int32_t mndResumeStreamTasks(STrans *pTrans, SMnode *pMnode, SStreamObj *pStream, int8_t igUntreated) {
  int32_t size = taosArrayGetSize(pStream->tasks);
  for (int32_t i = 0; i < size; i++) {
    SArray *pTasks = taosArrayGetP(pStream->tasks, i);
    int32_t sz = taosArrayGetSize(pTasks);
    for (int32_t j = 0; j < sz; j++) {
      SStreamTask *pTask = taosArrayGetP(pTasks, j);
      if (doResumeStreamTask(pTrans, pMnode, pTask, igUntreated) < 0) {
        return -1;
      }

      if (atomic_load_8(&pTask->status.taskStatus) == TASK_STATUS__PAUSE) {
        atomic_store_8(&pTask->status.taskStatus, pTask->status.statusBackup);
      }
    }
  }
  return 0;
}

static int32_t doPauseStreamTask(SMnode *pMnode, STrans *pTrans, SStreamTask *pTask) {
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

  SEpSet epset = {0};
  mDebug("pause node:%d, epset:%d", pTask->info.nodeId, epset.numOfEps);
  bool    hasEpset = false;
  int32_t code = extractNodeEpset(pMnode, &epset, &hasEpset, pTask->id.taskId, pTask->info.nodeId);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    taosMemoryFree(pReq);
    return -1;
  }

  // no valid epset, return directly without redoAction
  if (!hasEpset) {
    taosMemoryFree(pReq);
    return TSDB_CODE_SUCCESS;
  }

  STransAction action = {0};
  initTransAction(&action, pReq, sizeof(SVPauseStreamTaskReq), TDMT_STREAM_TASK_PAUSE, &epset, 0);
  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }
  return 0;
}

int32_t mndPauseStreamTasks(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream) {
  SArray *tasks = pStream->tasks;

  int32_t size = taosArrayGetSize(tasks);
  for (int32_t i = 0; i < size; i++) {
    SArray *pTasks = taosArrayGetP(tasks, i);
    int32_t sz = taosArrayGetSize(pTasks);
    for (int32_t j = 0; j < sz; j++) {
      SStreamTask *pTask = taosArrayGetP(pTasks, j);
      if (doPauseStreamTask(pMnode, pTrans, pTask) < 0) {
        return -1;
      }

      if (atomic_load_8(&pTask->status.taskStatus) != TASK_STATUS__PAUSE) {
        atomic_store_8(&pTask->status.statusBackup, pTask->status.taskStatus);
        atomic_store_8(&pTask->status.taskStatus, TASK_STATUS__PAUSE);
      }
    }
  }
  return 0;
}