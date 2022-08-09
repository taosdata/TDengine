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

#include "syncRaftLog.h"
#include "syncRaftCfg.h"
#include "syncRaftStore.h"

//-------------------------------
// log[m .. n]

// public function
static int32_t   raftLogRestoreFromSnapshot(struct SSyncLogStore* pLogStore, SyncIndex snapshotIndex);
static SyncIndex raftLogBeginIndex(struct SSyncLogStore* pLogStore);
static SyncIndex raftLogEndIndex(struct SSyncLogStore* pLogStore);
static SyncIndex raftLogWriteIndex(struct SSyncLogStore* pLogStore);
static bool      raftLogIsEmpty(struct SSyncLogStore* pLogStore);
static int32_t   raftLogEntryCount(struct SSyncLogStore* pLogStore);
static SyncIndex raftLogLastIndex(struct SSyncLogStore* pLogStore);
static SyncTerm  raftLogLastTerm(struct SSyncLogStore* pLogStore);
static int32_t   raftLogAppendEntry(struct SSyncLogStore* pLogStore, SSyncRaftEntry* pEntry);
static int32_t   raftLogGetEntry(struct SSyncLogStore* pLogStore, SyncIndex index, SSyncRaftEntry** ppEntry);
static int32_t   raftLogTruncate(struct SSyncLogStore* pLogStore, SyncIndex fromIndex);
static bool      raftLogExist(struct SSyncLogStore* pLogStore, SyncIndex index);

// private function
static int32_t raftLogGetLastEntry(SSyncLogStore* pLogStore, SSyncRaftEntry** ppLastEntry);

//-------------------------------
// log[0 .. n]
static SSyncRaftEntry* logStoreGetLastEntry(SSyncLogStore* pLogStore);
static SyncIndex       logStoreLastIndex(SSyncLogStore* pLogStore);
static SyncTerm        logStoreLastTerm(SSyncLogStore* pLogStore);
static SSyncRaftEntry* logStoreGetEntry(SSyncLogStore* pLogStore, SyncIndex index);
static int32_t         logStoreAppendEntry(SSyncLogStore* pLogStore, SSyncRaftEntry* pEntry);
static int32_t         logStoreTruncate(SSyncLogStore* pLogStore, SyncIndex fromIndex);
static int32_t         logStoreUpdateCommitIndex(SSyncLogStore* pLogStore, SyncIndex index);
static SyncIndex       logStoreGetCommitIndex(SSyncLogStore* pLogStore);

//-------------------------------
SSyncLogStore* logStoreCreate(SSyncNode* pSyncNode) {
  SSyncLogStore* pLogStore = taosMemoryMalloc(sizeof(SSyncLogStore));
  ASSERT(pLogStore != NULL);

  pLogStore->data = taosMemoryMalloc(sizeof(SSyncLogStoreData));
  ASSERT(pLogStore->data != NULL);

  SSyncLogStoreData* pData = pLogStore->data;
  pData->pSyncNode = pSyncNode;
  pData->pWal = pSyncNode->pWal;
  ASSERT(pData->pWal != NULL);

  taosThreadMutexInit(&(pData->mutex), NULL);
  pData->pWalHandle = walOpenReader(pData->pWal, NULL);
  ASSERT(pData->pWalHandle != NULL);

  pLogStore->appendEntry = logStoreAppendEntry;
  pLogStore->getEntry = logStoreGetEntry;
  pLogStore->truncate = logStoreTruncate;
  pLogStore->getLastIndex = logStoreLastIndex;
  pLogStore->getLastTerm = logStoreLastTerm;
  pLogStore->updateCommitIndex = logStoreUpdateCommitIndex;
  pLogStore->getCommitIndex = logStoreGetCommitIndex;

  pLogStore->syncLogRestoreFromSnapshot = raftLogRestoreFromSnapshot;
  pLogStore->syncLogBeginIndex = raftLogBeginIndex;
  pLogStore->syncLogEndIndex = raftLogEndIndex;
  pLogStore->syncLogIsEmpty = raftLogIsEmpty;
  pLogStore->syncLogEntryCount = raftLogEntryCount;
  pLogStore->syncLogLastIndex = raftLogLastIndex;
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
    taosMemoryFree(pLogStore);
  }
}

//-------------------------------
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

    char logBuf[128];
    snprintf(logBuf, sizeof(logBuf),
             "wal restore from snapshot error, index:%" PRId64 ", err:%d %X, msg:%s, syserr:%d, sysmsg:%s",
             snapshotIndex, err, err, errStr, sysErr, sysErrStr);
    syncNodeErrorLog(pData->pSyncNode, logBuf);

    return -1;
  }

  return 0;
}

static SyncIndex raftLogBeginIndex(struct SSyncLogStore* pLogStore) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  SyncIndex          firstVer = walGetFirstVer(pWal);
  return firstVer;
}

static SyncIndex raftLogEndIndex(struct SSyncLogStore* pLogStore) { return raftLogLastIndex(pLogStore); }

static bool raftLogIsEmpty(struct SSyncLogStore* pLogStore) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  return walIsEmpty(pWal);
}

static int32_t raftLogEntryCount(struct SSyncLogStore* pLogStore) {
  SyncIndex beginIndex = raftLogBeginIndex(pLogStore);
  SyncIndex endIndex = raftLogEndIndex(pLogStore);
  int32_t   count = endIndex - beginIndex + 1;
  return count > 0 ? count : 0;
}

static SyncIndex raftLogLastIndex(struct SSyncLogStore* pLogStore) {
  SyncIndex          lastIndex;
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  SyncIndex          lastVer = walGetLastVer(pWal);

  return lastVer;
}

static SyncIndex raftLogWriteIndex(struct SSyncLogStore* pLogStore) {
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
static SyncTerm raftLogLastTerm(struct SSyncLogStore* pLogStore) {
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

static int32_t raftLogAppendEntry(struct SSyncLogStore* pLogStore, SSyncRaftEntry* pEntry) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;

  SyncIndex    index = 0;
  SWalSyncInfo syncMeta = {0};
  syncMeta.isWeek = pEntry->isWeak;
  syncMeta.seqNum = pEntry->seqNum;
  syncMeta.term = pEntry->term;
  index = walAppendLog(pWal, pEntry->originalRpcType, syncMeta, pEntry->data, pEntry->dataLen);
  if (index < 0) {
    int32_t     err = terrno;
    const char* errStr = tstrerror(err);
    int32_t     sysErr = errno;
    const char* sysErrStr = strerror(errno);

    char logBuf[128];
    snprintf(logBuf, sizeof(logBuf), "wal write error, index:%" PRId64 ", err:%d %X, msg:%s, syserr:%d, sysmsg:%s",
             pEntry->index, err, err, errStr, sysErr, sysErrStr);
    syncNodeErrorLog(pData->pSyncNode, logBuf);

    ASSERT(0);
    return -1;
  }
  pEntry->index = index;

  do {
    char eventLog[128];
    snprintf(eventLog, sizeof(eventLog), "write index:%" PRId64 ", type:%s, origin type:%s", pEntry->index,
             TMSG_INFO(pEntry->msgType), TMSG_INFO(pEntry->originalRpcType));
    syncNodeEventLog(pData->pSyncNode, eventLog);
  } while (0);

  return 0;
}

// entry found, return 0
// entry not found, return -1, terrno = TSDB_CODE_WAL_LOG_NOT_EXIST
// other error, return -1
static int32_t raftLogGetEntry(struct SSyncLogStore* pLogStore, SyncIndex index, SSyncRaftEntry** ppEntry) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  int32_t            code;

  *ppEntry = NULL;

  // SWalReadHandle* pWalHandle = walOpenReadHandle(pWal);
  SWalReader* pWalHandle = pData->pWalHandle;
  if (pWalHandle == NULL) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }

  taosThreadMutexLock(&(pData->mutex));

  code = walReadVer(pWalHandle, index);
  if (code != 0) {
    int32_t     err = terrno;
    const char* errStr = tstrerror(err);
    int32_t     sysErr = errno;
    const char* sysErrStr = strerror(errno);

    do {
      char logBuf[128];
      snprintf(logBuf, sizeof(logBuf), "wal read error, index:%" PRId64 ", err:%d %X, msg:%s, syserr:%d, sysmsg:%s",
               index, err, err, errStr, sysErr, sysErrStr);
      if (terrno == TSDB_CODE_WAL_LOG_NOT_EXIST) {
        // syncNodeEventLog(pData->pSyncNode, logBuf);
      } else {
        syncNodeErrorLog(pData->pSyncNode, logBuf);
      }
    } while (0);

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
  return code;
}

// truncate semantic
static int32_t raftLogTruncate(struct SSyncLogStore* pLogStore, SyncIndex fromIndex) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;

  // need not truncate
  SyncIndex wallastVer = walGetLastVer(pWal);
  if (fromIndex > wallastVer) {
    return 0;
  }

  int32_t code = walRollback(pWal, fromIndex);
  if (code != 0) {
    int32_t     err = terrno;
    const char* errStr = tstrerror(err);
    int32_t     sysErr = errno;
    const char* sysErrStr = strerror(errno);
    sError("vgId:%d, wal truncate error, from-index:%" PRId64 ", err:%d %X, msg:%s, syserr:%d, sysmsg:%s",
           pData->pSyncNode->vgId, fromIndex, err, err, errStr, sysErr, sysErrStr);

    ASSERT(0);
  }

  // event log
  do {
    char logBuf[128];
    snprintf(logBuf, sizeof(logBuf), "log truncate, from-index:%" PRId64, fromIndex);
    syncNodeEventLog(pData->pSyncNode, logBuf);
  } while (0);

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

//-------------------------------
// log[0 .. n]

int32_t logStoreAppendEntry(SSyncLogStore* pLogStore, SSyncRaftEntry* pEntry) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;

  SyncIndex    index = 0;
  SWalSyncInfo syncMeta = {0};
  syncMeta.isWeek = pEntry->isWeak;
  syncMeta.seqNum = pEntry->seqNum;
  syncMeta.term = pEntry->term;

  index = walAppendLog(pWal, pEntry->originalRpcType, syncMeta, pEntry->data, pEntry->dataLen);
  if (index < 0) {
    int32_t     err = terrno;
    const char* errStr = tstrerror(err);
    int32_t     sysErr = errno;
    const char* sysErrStr = strerror(errno);

    char logBuf[128];
    snprintf(logBuf, sizeof(logBuf), "wal write error, index:%" PRId64 ", err:%d %X, msg:%s, syserr:%d, sysmsg:%s",
             pEntry->index, err, err, errStr, sysErr, sysErrStr);
    syncNodeErrorLog(pData->pSyncNode, logBuf);

    ASSERT(0);
    return -1;
  }
  pEntry->index = index;

  do {
    char eventLog[128];
    snprintf(eventLog, sizeof(eventLog), "write2 index:%" PRId64 ", type:%s, origin type:%s", pEntry->index,
             TMSG_INFO(pEntry->msgType), TMSG_INFO(pEntry->originalRpcType));
    syncNodeEventLog(pData->pSyncNode, eventLog);
  } while (0);

  return 0;
}

SSyncRaftEntry* logStoreGetEntry(SSyncLogStore* pLogStore, SyncIndex index) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;

  if (index >= SYNC_INDEX_BEGIN && index <= logStoreLastIndex(pLogStore)) {
    taosThreadMutexLock(&(pData->mutex));

    // SWalReadHandle* pWalHandle = walOpenReadHandle(pWal);
    SWalReader* pWalHandle = pData->pWalHandle;
    ASSERT(pWalHandle != NULL);

    int32_t code = walReadVer(pWalHandle, index);
    if (code != 0) {
      int32_t     err = terrno;
      const char* errStr = tstrerror(err);
      int32_t     sysErr = errno;
      const char* sysErrStr = strerror(errno);

      do {
        char logBuf[128];
        snprintf(logBuf, sizeof(logBuf), "wal read error, index:%" PRId64 ", err:%d %X, msg:%s, syserr:%d, sysmsg:%s",
                 index, err, err, errStr, sysErr, sysErrStr);
        if (terrno == TSDB_CODE_WAL_LOG_NOT_EXIST) {
          // syncNodeEventLog(pData->pSyncNode, logBuf);
        } else {
          syncNodeErrorLog(pData->pSyncNode, logBuf);
        }
      } while (0);

      ASSERT(0);
    }

    SSyncRaftEntry* pEntry = syncEntryBuild(pWalHandle->pHead->head.bodyLen);
    ASSERT(pEntry != NULL);

    pEntry->msgType = TDMT_SYNC_CLIENT_REQUEST;
    pEntry->originalRpcType = pWalHandle->pHead->head.msgType;
    pEntry->seqNum = pWalHandle->pHead->head.syncMeta.seqNum;
    pEntry->isWeak = pWalHandle->pHead->head.syncMeta.isWeek;
    pEntry->term = pWalHandle->pHead->head.syncMeta.term;
    pEntry->index = index;
    ASSERT(pEntry->dataLen == pWalHandle->pHead->head.bodyLen);
    memcpy(pEntry->data, pWalHandle->pHead->head.body, pWalHandle->pHead->head.bodyLen);

    /*
        int32_t saveErr = terrno;
        walCloseReadHandle(pWalHandle);
        terrno = saveErr;
    */

    taosThreadMutexUnlock(&(pData->mutex));
    return pEntry;

  } else {
    return NULL;
  }
}

int32_t logStoreTruncate(SSyncLogStore* pLogStore, SyncIndex fromIndex) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  // ASSERT(walRollback(pWal, fromIndex) == 0);
  int32_t code = walRollback(pWal, fromIndex);
  if (code != 0) {
    int32_t     err = terrno;
    const char* errStr = tstrerror(err);
    int32_t     sysErr = errno;
    const char* sysErrStr = strerror(errno);
    sError("vgId:%d, wal truncate error, from-index:%" PRId64 ", err:%d %X, msg:%s, syserr:%d, sysmsg:%s",
           pData->pSyncNode->vgId, fromIndex, err, err, errStr, sysErr, sysErrStr);

    ASSERT(0);
  }

  // event log
  do {
    char logBuf[128];
    snprintf(logBuf, sizeof(logBuf), "wal truncate, from-index:%" PRId64, fromIndex);
    syncNodeEventLog(pData->pSyncNode, logBuf);
  } while (0);

  return 0;
}

SyncIndex logStoreLastIndex(SSyncLogStore* pLogStore) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  SyncIndex          lastIndex = walGetLastVer(pWal);
  return lastIndex;
}

SyncTerm logStoreLastTerm(SSyncLogStore* pLogStore) {
  SyncTerm        lastTerm = 0;
  SSyncRaftEntry* pLastEntry = logStoreGetLastEntry(pLogStore);
  if (pLastEntry != NULL) {
    lastTerm = pLastEntry->term;
    taosMemoryFree(pLastEntry);
  }
  return lastTerm;
}

int32_t logStoreUpdateCommitIndex(SSyncLogStore* pLogStore, SyncIndex index) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  // ASSERT(walCommit(pWal, index) == 0);
  int32_t code = walCommit(pWal, index);
  if (code != 0) {
    int32_t     err = terrno;
    const char* errStr = tstrerror(err);
    int32_t     sysErr = errno;
    const char* sysErrStr = strerror(errno);
    sError("vgId:%d, wal update commit index error, index:%" PRId64 ", err:%d %X, msg:%s, syserr:%d, sysmsg:%s",
           pData->pSyncNode->vgId, index, err, err, errStr, sysErr, sysErrStr);

    ASSERT(0);
  }
  return 0;
}

SyncIndex logStoreGetCommitIndex(SSyncLogStore* pLogStore) {
  SSyncLogStoreData* pData = pLogStore->data;
  return pData->pSyncNode->commitIndex;
}

SSyncRaftEntry* logStoreGetLastEntry(SSyncLogStore* pLogStore) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  SyncIndex          lastIndex = walGetLastVer(pWal);

  SSyncRaftEntry* pEntry = NULL;
  if (lastIndex > 0) {
    pEntry = logStoreGetEntry(pLogStore, lastIndex);
  }
  return pEntry;
}

cJSON* logStore2Json(SSyncLogStore* pLogStore) {
  char               u64buf[128] = {0};
  SSyncLogStoreData* pData = (SSyncLogStoreData*)pLogStore->data;
  cJSON*             pRoot = cJSON_CreateObject();

  if (pData != NULL && pData->pWal != NULL) {
    snprintf(u64buf, sizeof(u64buf), "%p", pData->pSyncNode);
    cJSON_AddStringToObject(pRoot, "pSyncNode", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pData->pWal);
    cJSON_AddStringToObject(pRoot, "pWal", u64buf);

    SyncIndex beginIndex = raftLogBeginIndex(pLogStore);
    snprintf(u64buf, sizeof(u64buf), "%" PRId64, beginIndex);
    cJSON_AddStringToObject(pRoot, "beginIndex", u64buf);

    SyncIndex endIndex = raftLogEndIndex(pLogStore);
    snprintf(u64buf, sizeof(u64buf), "%" PRId64, endIndex);
    cJSON_AddStringToObject(pRoot, "endIndex", u64buf);

    int32_t count = raftLogEntryCount(pLogStore);
    cJSON_AddNumberToObject(pRoot, "entryCount", count);

    snprintf(u64buf, sizeof(u64buf), "%" PRId64, raftLogWriteIndex(pLogStore));
    cJSON_AddStringToObject(pRoot, "WriteIndex", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%d", raftLogIsEmpty(pLogStore));
    cJSON_AddStringToObject(pRoot, "IsEmpty", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%" PRId64, raftLogLastIndex(pLogStore));
    cJSON_AddStringToObject(pRoot, "LastIndex", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, raftLogLastTerm(pLogStore));
    cJSON_AddStringToObject(pRoot, "LastTerm", u64buf);

    cJSON* pEntries = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "pEntries", pEntries);

    if (!raftLogIsEmpty(pLogStore)) {
      for (SyncIndex i = beginIndex; i <= endIndex; ++i) {
        SSyncRaftEntry* pEntry = logStoreGetEntry(pLogStore, i);
        cJSON_AddItemToArray(pEntries, syncEntry2Json(pEntry));
        syncEntryDestory(pEntry);
      }
    }
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SSyncLogStore", pRoot);
  return pJson;
}

char* logStore2Str(SSyncLogStore* pLogStore) {
  cJSON* pJson = logStore2Json(pLogStore);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

cJSON* logStoreSimple2Json(SSyncLogStore* pLogStore) {
  char               u64buf[128] = {0};
  SSyncLogStoreData* pData = (SSyncLogStoreData*)pLogStore->data;
  cJSON*             pRoot = cJSON_CreateObject();

  if (pData != NULL && pData->pWal != NULL) {
    snprintf(u64buf, sizeof(u64buf), "%p", pData->pSyncNode);
    cJSON_AddStringToObject(pRoot, "pSyncNode", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pData->pWal);
    cJSON_AddStringToObject(pRoot, "pWal", u64buf);

    SyncIndex beginIndex = raftLogBeginIndex(pLogStore);
    snprintf(u64buf, sizeof(u64buf), "%" PRId64, beginIndex);
    cJSON_AddStringToObject(pRoot, "beginIndex", u64buf);

    SyncIndex endIndex = raftLogEndIndex(pLogStore);
    snprintf(u64buf, sizeof(u64buf), "%" PRId64, endIndex);
    cJSON_AddStringToObject(pRoot, "endIndex", u64buf);

    int32_t count = raftLogEntryCount(pLogStore);
    cJSON_AddNumberToObject(pRoot, "entryCount", count);

    snprintf(u64buf, sizeof(u64buf), "%" PRId64, raftLogWriteIndex(pLogStore));
    cJSON_AddStringToObject(pRoot, "WriteIndex", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%d", raftLogIsEmpty(pLogStore));
    cJSON_AddStringToObject(pRoot, "IsEmpty", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%" PRId64, raftLogLastIndex(pLogStore));
    cJSON_AddStringToObject(pRoot, "LastIndex", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, raftLogLastTerm(pLogStore));
    cJSON_AddStringToObject(pRoot, "LastTerm", u64buf);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SSyncLogStoreSimple", pRoot);
  return pJson;
}

char* logStoreSimple2Str(SSyncLogStore* pLogStore) {
  cJSON* pJson = logStoreSimple2Json(pLogStore);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
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

// for debug -----------------
void logStorePrint(SSyncLogStore* pLogStore) {
  char* serialized = logStore2Str(pLogStore);
  printf("logStorePrint | len:%" PRIu64 " | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void logStorePrint2(char* s, SSyncLogStore* pLogStore) {
  char* serialized = logStore2Str(pLogStore);
  printf("logStorePrint2 | len:%" PRIu64 " | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void logStoreLog(SSyncLogStore* pLogStore) {
  if (gRaftDetailLog) {
    char* serialized = logStore2Str(pLogStore);
    sTraceLong("logStoreLog | len:%" PRIu64 " | %s", strlen(serialized), serialized);
    taosMemoryFree(serialized);
  }
}

void logStoreLog2(char* s, SSyncLogStore* pLogStore) {
  if (gRaftDetailLog) {
    char* serialized = logStore2Str(pLogStore);
    sTraceLong("logStoreLog2 | len:%" PRIu64 " | %s | %s", strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

// for debug -----------------
void logStoreSimplePrint(SSyncLogStore* pLogStore) {
  char* serialized = logStoreSimple2Str(pLogStore);
  printf("logStoreSimplePrint | len:%" PRIu64 " | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void logStoreSimplePrint2(char* s, SSyncLogStore* pLogStore) {
  char* serialized = logStoreSimple2Str(pLogStore);
  printf("logStoreSimplePrint2 | len:%" PRIu64 " | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void logStoreSimpleLog(SSyncLogStore* pLogStore) {
  char* serialized = logStoreSimple2Str(pLogStore);
  sTrace("logStoreSimpleLog | len:%" PRIu64 " | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void logStoreSimpleLog2(char* s, SSyncLogStore* pLogStore) {
  if (gRaftDetailLog) {
    char* serialized = logStoreSimple2Str(pLogStore);
    sTrace("logStoreSimpleLog2 | len:%" PRIu64 " | %s | %s", strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}
