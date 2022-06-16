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
#include "wal.h"

// refactor, log[0 .. n] ==> log[m .. n]
static int32_t   raftLogSetBeginIndex(struct SSyncLogStore* pLogStore, SyncIndex beginIndex);
static SyncIndex raftLogBeginIndex(struct SSyncLogStore* pLogStore);
static SyncIndex raftLogEndIndex(struct SSyncLogStore* pLogStore);
static SyncIndex raftLogWriteIndex(struct SSyncLogStore* pLogStore);
static bool      raftLogIsEmpty(struct SSyncLogStore* pLogStore);
static int32_t   raftLogEntryCount(struct SSyncLogStore* pLogStore);
static bool      raftLogInRange(struct SSyncLogStore* pLogStore, SyncIndex index);
static SyncIndex raftLogLastIndex(struct SSyncLogStore* pLogStore);
static SyncTerm  raftLogLastTerm(struct SSyncLogStore* pLogStore);
static int32_t   raftLogAppendEntry(struct SSyncLogStore* pLogStore, SSyncRaftEntry* pEntry);
static int32_t   raftLogGetEntry(struct SSyncLogStore* pLogStore, SyncIndex index, SSyncRaftEntry** ppEntry);
static int32_t   raftLogTruncate(struct SSyncLogStore* pLogStore, SyncIndex fromIndex);

static int32_t raftLogGetLastEntry(SSyncLogStore* pLogStore, SSyncRaftEntry** ppLastEntry);

//-------------------------------
static SSyncRaftEntry* logStoreGetLastEntry(SSyncLogStore* pLogStore);
static SyncIndex       logStoreLastIndex(SSyncLogStore* pLogStore);
static SyncTerm        logStoreLastTerm(SSyncLogStore* pLogStore);
static SSyncRaftEntry* logStoreGetEntry(SSyncLogStore* pLogStore, SyncIndex index);
static int32_t         logStoreAppendEntry(SSyncLogStore* pLogStore, SSyncRaftEntry* pEntry);
static int32_t         logStoreTruncate(SSyncLogStore* pLogStore, SyncIndex fromIndex);
static int32_t         logStoreUpdateCommitIndex(SSyncLogStore* pLogStore, SyncIndex index);
static SyncIndex       logStoreGetCommitIndex(SSyncLogStore* pLogStore);

// refactor, log[0 .. n] ==> log[m .. n]
static int32_t raftLogSetBeginIndex(struct SSyncLogStore* pLogStore, SyncIndex beginIndex) {
  sTrace("raftLogSetBeginIndex beginIndex:%ld", beginIndex);

  // if beginIndex == 0, donot need call this funciton
  ASSERT(beginIndex > 0);

  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  pData->beginIndex = beginIndex;
  walRestoreFromSnapshot(pWal, beginIndex - 1);
  return 0;
}

int32_t raftLogResetBeginIndex(struct SSyncLogStore* pLogStore) { return 0; }

static SyncIndex raftLogBeginIndex(struct SSyncLogStore* pLogStore) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  return pData->beginIndex;
}

static SyncIndex raftLogEndIndex(struct SSyncLogStore* pLogStore) { return raftLogLastIndex(pLogStore); }

static bool raftLogIsEmpty(struct SSyncLogStore* pLogStore) {
  SyncIndex beginIndex = raftLogBeginIndex(pLogStore);
  SyncIndex endIndex = raftLogEndIndex(pLogStore);
  return (endIndex < beginIndex);
}

static int32_t raftLogEntryCount(struct SSyncLogStore* pLogStore) {
  SyncIndex beginIndex = raftLogBeginIndex(pLogStore);
  SyncIndex endIndex = raftLogEndIndex(pLogStore);
  int32_t   count = endIndex - beginIndex + 1;
  return count > 0 ? count : 0;
}

static bool raftLogInRange(struct SSyncLogStore* pLogStore, SyncIndex index) {
  SyncIndex beginIndex = raftLogBeginIndex(pLogStore);
  SyncIndex endIndex = raftLogEndIndex(pLogStore);
  if (index >= beginIndex && index <= endIndex) {
    return true;
  } else {
    return false;
  }
}

static SyncIndex raftLogLastIndex(struct SSyncLogStore* pLogStore) {
  SyncIndex          lastIndex;
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  SyncIndex          lastVer = walGetLastVer(pWal);
  SyncIndex          firstVer = walGetFirstVer(pWal);

  if (lastVer < firstVer) {
    // no record
    lastIndex = -1;

  } else {
    if (firstVer >= 0) {
      lastIndex = lastVer;
    } else if (firstVer == -1) {
      lastIndex = -1;
    } else {
      ASSERT(0);
    }
  }

  return lastIndex;
}

static SyncIndex raftLogWriteIndex(struct SSyncLogStore* pLogStore) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  SyncIndex          lastVer = walGetLastVer(pWal);
  return lastVer + 1;
}

static SyncTerm raftLogLastTerm(struct SSyncLogStore* pLogStore) {
  SyncTerm lastTerm = 0;
  if (raftLogEntryCount(pLogStore) == 0) {
    lastTerm = 0;
  } else {
    SSyncRaftEntry* pLastEntry;
    int32_t         code = raftLogGetLastEntry(pLogStore, &pLastEntry);
    ASSERT(code == 0);
    if (pLastEntry != NULL) {
      lastTerm = pLastEntry->term;
      taosMemoryFree(pLastEntry);
    }
  }
  return lastTerm;
}

static int32_t raftLogAppendEntry(struct SSyncLogStore* pLogStore, SSyncRaftEntry* pEntry) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;

  SyncIndex writeIndex = raftLogWriteIndex(pLogStore);
  ASSERT(pEntry->index == writeIndex);

  int          code = 0;
  SSyncLogMeta syncMeta;
  syncMeta.isWeek = pEntry->isWeak;
  syncMeta.seqNum = pEntry->seqNum;
  syncMeta.term = pEntry->term;
  code = walWriteWithSyncInfo(pWal, pEntry->index, pEntry->originalRpcType, syncMeta, pEntry->data, pEntry->dataLen);
  if (code != 0) {
    int32_t     err = terrno;
    const char* errStr = tstrerror(err);
    int32_t     linuxErr = errno;
    const char* linuxErrMsg = strerror(errno);
    sError("raftLogAppendEntry error, err:%d %X, msg:%s, linuxErr:%d, linuxErrMsg:%s", err, err, errStr, linuxErr,
           linuxErrMsg);
    ASSERT(0);
  }

  walFsync(pWal, true);

  sDebug("vgId:%d sync event write index:%ld, %s, isStandBy:%d, msgType:%s,%d, originalRpcType:%s,%d",
         pData->pSyncNode->vgId, pEntry->index, syncUtilState2String(pData->pSyncNode->state),
         pData->pSyncNode->pRaftCfg->isStandBy, TMSG_INFO(pEntry->msgType), pEntry->msgType,
         TMSG_INFO(pEntry->originalRpcType), pEntry->originalRpcType);

  return code;
}

static int32_t raftLogGetEntry(struct SSyncLogStore* pLogStore, SyncIndex index, SSyncRaftEntry** ppEntry) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  int32_t            code;

  *ppEntry = NULL;
  if (raftLogInRange(pLogStore, index)) {
    SWalReadHandle* pWalHandle = walOpenReadHandle(pWal);
    ASSERT(pWalHandle != NULL);

    code = walReadWithHandle(pWalHandle, index);
    if (code != 0) {
      int32_t     err = terrno;
      const char* errStr = tstrerror(err);
      int32_t     linuxErr = errno;
      const char* linuxErrMsg = strerror(errno);
      sError("raftLogGetEntry error, err:%d %X, msg:%s, linuxErr:%d, linuxErrMsg:%s", err, err, errStr, linuxErr,
             linuxErrMsg);
      ASSERT(0);
      walCloseReadHandle(pWalHandle);
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

    // need to hold, do not new every time!!
    walCloseReadHandle(pWalHandle);

  } else {
    // index not in range
    code = 0;
  }

  return code;
}

static int32_t raftLogTruncate(struct SSyncLogStore* pLogStore, SyncIndex fromIndex) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  int32_t            code = walRollback(pWal, fromIndex);
  if (code != 0) {
    int32_t     err = terrno;
    const char* errStr = tstrerror(err);
    int32_t     linuxErr = errno;
    const char* linuxErrMsg = strerror(errno);
    sError("raftLogTruncate error, err:%d %X, msg:%s, linuxErr:%d, linuxErrMsg:%s", err, err, errStr, linuxErr,
           linuxErrMsg);
    ASSERT(0);
  }
  return code;
}

static int32_t raftLogGetLastEntry(SSyncLogStore* pLogStore, SSyncRaftEntry** ppLastEntry) {
  *ppLastEntry = NULL;
  if (raftLogEntryCount(pLogStore) == 0) {
    return 0;
  }
  SyncIndex lastIndex = raftLogLastIndex(pLogStore);
  int32_t   code = raftLogGetEntry(pLogStore, lastIndex, ppLastEntry);
  return code;
}

//-------------------------------
SSyncLogStore* logStoreCreate(SSyncNode* pSyncNode) {
  SSyncLogStore* pLogStore = taosMemoryMalloc(sizeof(SSyncLogStore));
  assert(pLogStore != NULL);

  pLogStore->data = taosMemoryMalloc(sizeof(SSyncLogStoreData));
  assert(pLogStore->data != NULL);

  SSyncLogStoreData* pData = pLogStore->data;
  pData->pSyncNode = pSyncNode;
  pData->pWal = pSyncNode->pWal;

  SyncIndex firstVer = walGetFirstVer(pData->pWal);
  SyncIndex lastVer = walGetLastVer(pData->pWal);
  if (firstVer >= 0) {
    pData->beginIndex = firstVer;
  } else if (firstVer == -1) {
    pData->beginIndex = lastVer + 1;
  } else {
    ASSERT(0);
  }

  pLogStore->appendEntry = logStoreAppendEntry;
  pLogStore->getEntry = logStoreGetEntry;
  pLogStore->truncate = logStoreTruncate;
  pLogStore->getLastIndex = logStoreLastIndex;
  pLogStore->getLastTerm = logStoreLastTerm;
  pLogStore->updateCommitIndex = logStoreUpdateCommitIndex;
  pLogStore->getCommitIndex = logStoreGetCommitIndex;

  pLogStore->syncLogSetBeginIndex = raftLogSetBeginIndex;
  pLogStore->syncLogBeginIndex = raftLogBeginIndex;
  pLogStore->syncLogEndIndex = raftLogEndIndex;
  pLogStore->syncLogIsEmpty = raftLogIsEmpty;
  pLogStore->syncLogEntryCount = raftLogEntryCount;
  pLogStore->syncLogInRange = raftLogInRange;
  pLogStore->syncLogLastIndex = raftLogLastIndex;
  pLogStore->syncLogLastTerm = raftLogLastTerm;
  pLogStore->syncLogAppendEntry = raftLogAppendEntry;
  pLogStore->syncLogGetEntry = raftLogGetEntry;
  pLogStore->syncLogTruncate = raftLogTruncate;
  pLogStore->syncLogWriteIndex = raftLogWriteIndex;

  return pLogStore;
}

void logStoreDestory(SSyncLogStore* pLogStore) {
  if (pLogStore != NULL) {
    taosMemoryFree(pLogStore->data);
    taosMemoryFree(pLogStore);
  }
}

//-------------------------------
int32_t logStoreAppendEntry(SSyncLogStore* pLogStore, SSyncRaftEntry* pEntry) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;

  SyncIndex lastIndex = logStoreLastIndex(pLogStore);
  assert(pEntry->index == lastIndex + 1);

  int          code = 0;
  SSyncLogMeta syncMeta;
  syncMeta.isWeek = pEntry->isWeak;
  syncMeta.seqNum = pEntry->seqNum;
  syncMeta.term = pEntry->term;
  code = walWriteWithSyncInfo(pWal, pEntry->index, pEntry->originalRpcType, syncMeta, pEntry->data, pEntry->dataLen);
  if (code != 0) {
    int32_t     err = terrno;
    const char* errStr = tstrerror(err);
    int32_t     linuxErr = errno;
    const char* linuxErrMsg = strerror(errno);
    sError("walWriteWithSyncInfo error, err:%d %X, msg:%s, linuxErr:%d, linuxErrMsg:%s", err, err, errStr, linuxErr,
           linuxErrMsg);
    ASSERT(0);
  }
  // assert(code == 0);

  walFsync(pWal, true);

  sDebug("sync event old write wal: %ld", pEntry->index);
  return code;
}

SSyncRaftEntry* logStoreGetEntry(SSyncLogStore* pLogStore, SyncIndex index) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;

  if (index >= SYNC_INDEX_BEGIN && index <= logStoreLastIndex(pLogStore)) {
    SWalReadHandle* pWalHandle = walOpenReadHandle(pWal);
    ASSERT(pWalHandle != NULL);

    int32_t code = walReadWithHandle(pWalHandle, index);
    if (code != 0) {
      int32_t     err = terrno;
      const char* errStr = tstrerror(err);
      int32_t     linuxErr = errno;
      const char* linuxErrMsg = strerror(errno);
      sError("walReadWithHandle error, err:%d %X, msg:%s, linuxErr:%d, linuxErrMsg:%s", err, err, errStr, linuxErr,
             linuxErrMsg);
      ASSERT(0);
    }
    // assert(walReadWithHandle(pWalHandle, index) == 0);

    SSyncRaftEntry* pEntry = syncEntryBuild(pWalHandle->pHead->head.bodyLen);
    assert(pEntry != NULL);

    pEntry->msgType = TDMT_SYNC_CLIENT_REQUEST;
    pEntry->originalRpcType = pWalHandle->pHead->head.msgType;
    pEntry->seqNum = pWalHandle->pHead->head.syncMeta.seqNum;
    pEntry->isWeak = pWalHandle->pHead->head.syncMeta.isWeek;
    pEntry->term = pWalHandle->pHead->head.syncMeta.term;
    pEntry->index = index;
    assert(pEntry->dataLen == pWalHandle->pHead->head.bodyLen);
    memcpy(pEntry->data, pWalHandle->pHead->head.body, pWalHandle->pHead->head.bodyLen);

    // need to hold, do not new every time!!
    walCloseReadHandle(pWalHandle);
    return pEntry;

  } else {
    return NULL;
  }
}

int32_t logStoreTruncate(SSyncLogStore* pLogStore, SyncIndex fromIndex) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  // assert(walRollback(pWal, fromIndex) == 0);
  int32_t code = walRollback(pWal, fromIndex);
  if (code != 0) {
    int32_t     err = terrno;
    const char* errStr = tstrerror(err);
    int32_t     linuxErr = errno;
    const char* linuxErrMsg = strerror(errno);
    sError("walRollback error, err:%d %X, msg:%s, linuxErr:%d, linuxErrMsg:%s", err, err, errStr, linuxErr,
           linuxErrMsg);
    ASSERT(0);
  }
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
  // assert(walCommit(pWal, index) == 0);
  int32_t code = walCommit(pWal, index);
  if (code != 0) {
    int32_t     err = terrno;
    const char* errStr = tstrerror(err);
    int32_t     linuxErr = errno;
    const char* linuxErrMsg = strerror(errno);
    sError("walCommit error, err:%d %X, msg:%s, linuxErr:%d, linuxErrMsg:%s", err, err, errStr, linuxErr, linuxErrMsg);
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

    snprintf(u64buf, sizeof(u64buf), "%ld", pData->beginIndex);
    cJSON_AddStringToObject(pRoot, "beginIndex", u64buf);

    SyncIndex endIndex = raftLogEndIndex(pLogStore);
    snprintf(u64buf, sizeof(u64buf), "%ld", endIndex);
    cJSON_AddStringToObject(pRoot, "endIndex", u64buf);

    int32_t count = raftLogEntryCount(pLogStore);
    cJSON_AddNumberToObject(pRoot, "entryCount", count);

    snprintf(u64buf, sizeof(u64buf), "%ld", raftLogWriteIndex(pLogStore));
    cJSON_AddStringToObject(pRoot, "WriteIndex", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%d", raftLogIsEmpty(pLogStore));
    cJSON_AddStringToObject(pRoot, "IsEmpty", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%ld", raftLogLastIndex(pLogStore));
    cJSON_AddStringToObject(pRoot, "LastIndex", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%lu", raftLogLastTerm(pLogStore));
    cJSON_AddStringToObject(pRoot, "LastTerm", u64buf);

    cJSON* pEntries = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "pEntries", pEntries);

    for (SyncIndex i = pData->beginIndex; i <= endIndex; ++i) {
      SSyncRaftEntry* pEntry = logStoreGetEntry(pLogStore, i);
      cJSON_AddItemToArray(pEntries, syncEntry2Json(pEntry));
      syncEntryDestory(pEntry);
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

    snprintf(u64buf, sizeof(u64buf), "%ld", pData->beginIndex);
    cJSON_AddStringToObject(pRoot, "beginIndex", u64buf);

    SyncIndex endIndex = raftLogEndIndex(pLogStore);
    snprintf(u64buf, sizeof(u64buf), "%ld", endIndex);
    cJSON_AddStringToObject(pRoot, "endIndex", u64buf);

    int32_t count = raftLogEntryCount(pLogStore);
    cJSON_AddNumberToObject(pRoot, "entryCount", count);

    snprintf(u64buf, sizeof(u64buf), "%ld", raftLogWriteIndex(pLogStore));
    cJSON_AddStringToObject(pRoot, "WriteIndex", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%d", raftLogIsEmpty(pLogStore));
    cJSON_AddStringToObject(pRoot, "IsEmpty", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%ld", raftLogLastIndex(pLogStore));
    cJSON_AddStringToObject(pRoot, "LastIndex", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%lu", raftLogLastTerm(pLogStore));
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

// for debug -----------------
void logStorePrint(SSyncLogStore* pLogStore) {
  char* serialized = logStore2Str(pLogStore);
  printf("logStorePrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void logStorePrint2(char* s, SSyncLogStore* pLogStore) {
  char* serialized = logStore2Str(pLogStore);
  printf("logStorePrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void logStoreLog(SSyncLogStore* pLogStore) {
  if (gRaftDetailLog) {
    char* serialized = logStore2Str(pLogStore);
    sTraceLong("logStoreLog | len:%lu | %s", strlen(serialized), serialized);
    taosMemoryFree(serialized);
  }
}

void logStoreLog2(char* s, SSyncLogStore* pLogStore) {
  if (gRaftDetailLog) {
    char* serialized = logStore2Str(pLogStore);
    sTraceLong("logStoreLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

// for debug -----------------
void logStoreSimplePrint(SSyncLogStore* pLogStore) {
  char* serialized = logStoreSimple2Str(pLogStore);
  printf("logStoreSimplePrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void logStoreSimplePrint2(char* s, SSyncLogStore* pLogStore) {
  char* serialized = logStoreSimple2Str(pLogStore);
  printf("logStoreSimplePrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void logStoreSimpleLog(SSyncLogStore* pLogStore) {
  char* serialized = logStoreSimple2Str(pLogStore);
  sTrace("logStoreSimpleLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void logStoreSimpleLog2(char* s, SSyncLogStore* pLogStore) {
  if (gRaftDetailLog) {
    char* serialized = logStoreSimple2Str(pLogStore);
    sTrace("logStoreSimpleLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}
