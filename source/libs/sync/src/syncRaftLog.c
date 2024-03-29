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

#define _DEFAULT_SOURCE
#include "syncRaftLog.h"
#include "syncRaftCfg.h"
#include "syncRaftStore.h"
#include "syncUtil.h"

// log[m .. n]

// public function
static int32_t   raftLogRestoreFromSnapshot(struct SSyncLogStore* pLogStore, SyncIndex snapshotIndex);
static int32_t   raftLogAppendEntry(struct SSyncLogStore* pLogStore, SSyncRaftEntry* pEntry, bool forceSync);
static int32_t   raftLogTruncate(struct SSyncLogStore* pLogStore, SyncIndex fromIndex);
static bool      raftLogExist(struct SSyncLogStore* pLogStore, SyncIndex index);
static int32_t   raftLogUpdateCommitIndex(SSyncLogStore* pLogStore, SyncIndex index);
static SyncIndex raftlogCommitIndex(SSyncLogStore* pLogStore);
static int32_t   raftLogGetLastEntry(SSyncLogStore* pLogStore, SSyncRaftEntry** ppLastEntry);

SSyncLogStore* logStoreCreate(SSyncNode* pSyncNode) {
  SSyncLogStore* pLogStore = taosMemoryCalloc(1, sizeof(SSyncLogStore));
  if (pLogStore == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  // pLogStore->pCache = taosLRUCacheInit(10 * 1024 * 1024, 1, .5);
  pLogStore->pCache = taosLRUCacheInit(30 * 1024 * 1024, 1, .5);
  if (pLogStore->pCache == NULL) {
    taosMemoryFree(pLogStore);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pLogStore->cacheHit = 0;
  pLogStore->cacheMiss = 0;

  taosLRUCacheSetStrictCapacity(pLogStore->pCache, false);

  pLogStore->data = taosMemoryMalloc(sizeof(SSyncLogStoreData));
  ASSERT(pLogStore->data != NULL);

  SSyncLogStoreData* pData = pLogStore->data;
  pData->pSyncNode = pSyncNode;
  pData->pWal = pSyncNode->pWal;
  ASSERT(pData->pWal != NULL);

  taosThreadMutexInit(&(pData->mutex), NULL);
  pData->pWalHandle = walOpenReader(pData->pWal, NULL, 0);
  ASSERT(pData->pWalHandle != NULL);

  pLogStore->syncLogUpdateCommitIndex = raftLogUpdateCommitIndex;
  pLogStore->syncLogCommitIndex = raftlogCommitIndex;
  pLogStore->syncLogRestoreFromSnapshot = raftLogRestoreFromSnapshot;
  pLogStore->syncLogBeginIndex = raftLogBeginIndex;
  pLogStore->syncLogEndIndex = raftLogEndIndex;
  pLogStore->syncLogIsEmpty = raftLogIsEmpty;
  pLogStore->syncLogEntryCount = raftLogEntryCount;
  pLogStore->syncLogLastIndex = raftLogLastIndex;
  pLogStore->syncLogIndexRetention = raftLogIndexRetention;
  pLogStore->syncLogLastTerm = raftLogLastTerm;
  pLogStore->syncLogAppendEntry = raftLogAppendEntry;
  pLogStore->syncLogGetEntry = raftLogGetEntry;
  pLogStore->syncLogTruncate = raftLogTruncate;
  pLogStore->syncLogWriteIndex = raftLogWriteIndex;
  pLogStore->syncLogExist = raftLogExist;

  return pLogStore;
}

void logStoreDestory(SSyncLogStore* pLogStore) {
  if (pLogStore != NULL) {
    SSyncLogStoreData* pData = pLogStore->data;

    taosThreadMutexLock(&(pData->mutex));
    if (pData->pWalHandle != NULL) {
      walCloseReader(pData->pWalHandle);
      pData->pWalHandle = NULL;
    }
    taosThreadMutexUnlock(&(pData->mutex));
    taosThreadMutexDestroy(&(pData->mutex));

    taosMemoryFree(pLogStore->data);

    taosLRUCacheEraseUnrefEntries(pLogStore->pCache);
    taosLRUCacheCleanup(pLogStore->pCache);

    taosMemoryFree(pLogStore);
  }
}

// log[m .. n]
static int32_t raftLogRestoreFromSnapshot(struct SSyncLogStore* pLogStore, SyncIndex snapshotIndex) {
  ASSERT(snapshotIndex >= 0);

  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  int32_t            code = walRestoreFromSnapshot(pWal, snapshotIndex);
  if (code != 0) {
    int32_t     err = terrno;
    const char* errStr = tstrerror(err);
    int32_t     sysErr = errno;
    const char* sysErrStr = strerror(errno);

    sNError(pData->pSyncNode,
            "wal restore from snapshot error, index:%" PRId64 ", err:0x%x, msg:%s, syserr:%d, sysmsg:%s", snapshotIndex,
            err, errStr, sysErr, sysErrStr);
    return -1;
  }

  return 0;
}

SyncIndex raftLogBeginIndex(struct SSyncLogStore* pLogStore) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  SyncIndex          firstVer = walGetFirstVer(pWal);
  return firstVer;
}

SyncIndex raftLogEndIndex(struct SSyncLogStore* pLogStore) { return raftLogLastIndex(pLogStore); }

bool raftLogIsEmpty(struct SSyncLogStore* pLogStore) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  return walIsEmpty(pWal);
}

int32_t raftLogEntryCount(struct SSyncLogStore* pLogStore) {
  SyncIndex beginIndex = raftLogBeginIndex(pLogStore);
  SyncIndex endIndex = raftLogEndIndex(pLogStore);
  int32_t   count = endIndex - beginIndex + 1;
  return count > 0 ? count : 0;
}

SyncIndex raftLogLastIndex(struct SSyncLogStore* pLogStore) {
  SyncIndex          lastIndex;
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  SyncIndex          lastVer = walGetLastVer(pWal);

  return lastVer;
}

SyncIndex raftLogIndexRetention(struct SSyncLogStore* pLogStore, int64_t bytes) {
  SyncIndex          lastIndex;
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  SyncIndex          lastVer = walGetVerRetention(pWal, bytes);

  return lastVer;
}

SyncIndex raftLogWriteIndex(struct SSyncLogStore* pLogStore) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  SyncIndex          lastVer = walGetLastVer(pWal);
  return lastVer + 1;
}

static bool raftLogExist(struct SSyncLogStore* pLogStore, SyncIndex index) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  bool               b = walLogExist(pWal, index);
  return b;
}

// if success, return last term
// if not log, return 0
// if error, return SYNC_TERM_INVALID
SyncTerm raftLogLastTerm(struct SSyncLogStore* pLogStore) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  if (walIsEmpty(pWal)) {
    return 0;
  } else {
    SSyncRaftEntry* pLastEntry;
    int32_t         code = raftLogGetLastEntry(pLogStore, &pLastEntry);
    if (code == 0 && pLastEntry != NULL) {
      SyncTerm lastTerm = pLastEntry->term;
      taosMemoryFree(pLastEntry);
      return lastTerm;
    } else {
      return SYNC_TERM_INVALID;
    }
  }

  // can not be here!
  return SYNC_TERM_INVALID;
}

static int32_t raftLogAppendEntry(struct SSyncLogStore* pLogStore, SSyncRaftEntry* pEntry, bool forceSync) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;

  SyncIndex    index = 0;
  SWalSyncInfo syncMeta = {0};
  syncMeta.isWeek = pEntry->isWeak;
  syncMeta.seqNum = pEntry->seqNum;
  syncMeta.term = pEntry->term;
  int64_t tsWriteBegin = taosGetTimestampNs();
  index = walAppendLog(pWal, pEntry->index, pEntry->originalRpcType, syncMeta, pEntry->data, pEntry->dataLen);
  int64_t tsWriteEnd = taosGetTimestampNs();
  int64_t tsElapsed = tsWriteEnd - tsWriteBegin;

  if (index < 0) {
    int32_t     err = terrno;
    const char* errStr = tstrerror(err);
    int32_t     sysErr = errno;
    const char* sysErrStr = strerror(errno);

    sNError(pData->pSyncNode, "wal write error, index:%" PRId64 ", err:0x%x, msg:%s, syserr:%d, sysmsg:%s",
            pEntry->index, err, errStr, sysErr, sysErrStr);
    return -1;
  }

  ASSERT(pEntry->index == index);

  walFsync(pWal, forceSync);

  sNTrace(pData->pSyncNode, "write index:%" PRId64 ", type:%s, origin type:%s, elapsed:%" PRId64, pEntry->index,
          TMSG_INFO(pEntry->msgType), TMSG_INFO(pEntry->originalRpcType), tsElapsed);
  return 0;
}

// entry found, return 0
// entry not found, return -1, terrno = TSDB_CODE_WAL_LOG_NOT_EXIST
// other error, return -1
int32_t raftLogGetEntry(struct SSyncLogStore* pLogStore, SyncIndex index, SSyncRaftEntry** ppEntry) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  int32_t            code = 0;

  *ppEntry = NULL;

  int64_t ts1 = taosGetTimestampNs();
  taosThreadMutexLock(&(pData->mutex));

  SWalReader* pWalHandle = pData->pWalHandle;
  if (pWalHandle == NULL) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    sError("vgId:%d, wal handle is NULL", pData->pSyncNode->vgId);
    taosThreadMutexUnlock(&(pData->mutex));
    return -1;
  }

  int64_t ts2 = taosGetTimestampNs();
  code = walReadVer(pWalHandle, index);
  walReadReset(pWalHandle);
  int64_t ts3 = taosGetTimestampNs();

  // code = walReadVerCached(pWalHandle, index);
  if (code != 0) {
    int32_t     err = terrno;
    const char* errStr = tstrerror(err);
    int32_t     sysErr = errno;
    const char* sysErrStr = strerror(errno);

    if (terrno == TSDB_CODE_WAL_LOG_NOT_EXIST) {
      sNTrace(pData->pSyncNode, "wal read not exist, index:%" PRId64 ", err:0x%x, msg:%s, syserr:%d, sysmsg:%s", index,
              err, errStr, sysErr, sysErrStr);
    } else {
      sNTrace(pData->pSyncNode, "wal read error, index:%" PRId64 ", err:0x%x, msg:%s, syserr:%d, sysmsg:%s", index, err,
              errStr, sysErr, sysErrStr);
    }

    /*
        int32_t saveErr = terrno;
        walCloseReadHandle(pWalHandle);
        terrno = saveErr;
    */

    taosThreadMutexUnlock(&(pData->mutex));
    return code;
  }

  *ppEntry = syncEntryBuild(pWalHandle->pHead->head.bodyLen);
  ASSERT(*ppEntry != NULL);
  (*ppEntry)->msgType = TDMT_SYNC_CLIENT_REQUEST;
  (*ppEntry)->originalRpcType = pWalHandle->pHead->head.msgType;
  (*ppEntry)->seqNum = pWalHandle->pHead->head.syncMeta.seqNum;
  (*ppEntry)->isWeak = pWalHandle->pHead->head.syncMeta.isWeek;
  (*ppEntry)->term = pWalHandle->pHead->head.syncMeta.term;
  (*ppEntry)->index = index;
  ASSERT((*ppEntry)->dataLen == pWalHandle->pHead->head.bodyLen);
  memcpy((*ppEntry)->data, pWalHandle->pHead->head.body, pWalHandle->pHead->head.bodyLen);

  /*
    int32_t saveErr = terrno;
    walCloseReadHandle(pWalHandle);
    terrno = saveErr;
  */

  taosThreadMutexUnlock(&(pData->mutex));
  int64_t ts4 = taosGetTimestampNs();

  int64_t tsElapsed = ts4 - ts1;
  int64_t tsElapsedLock = ts2 - ts1;
  int64_t tsElapsedRead = ts3 - ts2;
  int64_t tsElapsedBuild = ts4 - ts3;

  sNTrace(pData->pSyncNode,
          "read index:%" PRId64 ", elapsed:%" PRId64 ", elapsed-lock:%" PRId64 ", elapsed-read:%" PRId64
          ", elapsed-build:%" PRId64,
          index, tsElapsed, tsElapsedLock, tsElapsedRead, tsElapsedBuild);

  return code;
}

// truncate semantic
static int32_t raftLogTruncate(struct SSyncLogStore* pLogStore, SyncIndex fromIndex) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;

  int32_t code = walRollback(pWal, fromIndex);
  if (code != 0) {
    int32_t     err = terrno;
    const char* errStr = tstrerror(err);
    int32_t     sysErr = errno;
    const char* sysErrStr = strerror(errno);
    sError("vgId:%d, wal truncate error, from-index:%" PRId64 ", err:0x%x, msg:%s, syserr:%d, sysmsg:%s",
           pData->pSyncNode->vgId, fromIndex, err, errStr, sysErr, sysErrStr);
  }

  // event log
  sNTrace(pData->pSyncNode, "log truncate, from-index:%" PRId64, fromIndex);
  return code;
}

// entry found, return 0
// entry not found, return -1, terrno = TSDB_CODE_WAL_LOG_NOT_EXIST
// other error, return -1
static int32_t raftLogGetLastEntry(SSyncLogStore* pLogStore, SSyncRaftEntry** ppLastEntry) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  ASSERT(ppLastEntry != NULL);

  *ppLastEntry = NULL;
  if (walIsEmpty(pWal)) {
    terrno = TSDB_CODE_WAL_LOG_NOT_EXIST;
    return -1;
  } else {
    SyncIndex lastIndex = raftLogLastIndex(pLogStore);
    ASSERT(lastIndex >= SYNC_INDEX_BEGIN);
    int32_t code = raftLogGetEntry(pLogStore, lastIndex, ppLastEntry);
    return code;
  }

  return -1;
}

int32_t raftLogUpdateCommitIndex(SSyncLogStore* pLogStore, SyncIndex index) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;

  // need not update
  SyncIndex snapshotVer = walGetSnapshotVer(pWal);
  SyncIndex walCommitVer = walGetCommittedVer(pWal);
  SyncIndex wallastVer = walGetLastVer(pWal);

  if (index < snapshotVer || index > wallastVer) {
    // ignore
    return 0;
  }

  int32_t code = walCommit(pWal, index);
  if (code != 0) {
    int32_t     err = terrno;
    const char* errStr = tstrerror(err);
    int32_t     sysErr = errno;
    const char* sysErrStr = strerror(errno);
    sError("vgId:%d, wal update commit index error, index:%" PRId64 ", err:0x%x, msg:%s, syserr:%d, sysmsg:%s",
           pData->pSyncNode->vgId, index, err, errStr, sysErr, sysErrStr);
    return -1;
  }
  return 0;
}

SyncIndex raftlogCommitIndex(SSyncLogStore* pLogStore) {
  SSyncLogStoreData* pData = pLogStore->data;
  return pData->pSyncNode->commitIndex;
}

SyncIndex logStoreFirstIndex(SSyncLogStore* pLogStore) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  return walGetFirstVer(pWal);
}

SyncIndex logStoreWalCommitVer(SSyncLogStore* pLogStore) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  return walGetCommittedVer(pWal);
}
