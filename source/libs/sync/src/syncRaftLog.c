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
#include "wal.h"

SSyncLogStore* logStoreCreate(SSyncNode* pSyncNode) {
  SSyncLogStore* pLogStore = taosMemoryMalloc(sizeof(SSyncLogStore));
  assert(pLogStore != NULL);

  pLogStore->data = taosMemoryMalloc(sizeof(SSyncLogStoreData));
  assert(pLogStore->data != NULL);

  SSyncLogStoreData* pData = pLogStore->data;
  pData->pSyncNode = pSyncNode;
  pData->pWal = pSyncNode->pWal;

  pLogStore->appendEntry = logStoreAppendEntry;
  pLogStore->getEntry = logStoreGetEntry;
  pLogStore->truncate = logStoreTruncate;
  pLogStore->getLastIndex = logStoreLastIndex;
  pLogStore->getLastTerm = logStoreLastTerm;
  pLogStore->updateCommitIndex = logStoreUpdateCommitIndex;
  pLogStore->getCommitIndex = logStoreGetCommitIndex;
  return pLogStore;
}

void logStoreDestory(SSyncLogStore* pLogStore) {
  if (pLogStore != NULL) {
    taosMemoryFree(pLogStore->data);
    taosMemoryFree(pLogStore);
  }
}

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
  assert(code == 0);

  walFsync(pWal, true);
  return code;
}

SSyncRaftEntry* logStoreGetEntry(SSyncLogStore* pLogStore, SyncIndex index) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;

  if (index >= SYNC_INDEX_BEGIN && index <= logStoreLastIndex(pLogStore)) {
    SWalReadHandle* pWalHandle = walOpenReadHandle(pWal);
    assert(walReadWithHandle(pWalHandle, index) == 0);

    SSyncRaftEntry* pEntry = syncEntryBuild(pWalHandle->pHead->head.len);
    assert(pEntry != NULL);

    pEntry->msgType = TDMT_VND_SYNC_CLIENT_REQUEST;
    pEntry->originalRpcType = pWalHandle->pHead->head.msgType;
    pEntry->seqNum = pWalHandle->pHead->head.syncMeta.seqNum;
    pEntry->isWeak = pWalHandle->pHead->head.syncMeta.isWeek;
    pEntry->term = pWalHandle->pHead->head.syncMeta.term;
    pEntry->index = index;
    assert(pEntry->dataLen == pWalHandle->pHead->head.len);
    memcpy(pEntry->data, pWalHandle->pHead->head.body, pWalHandle->pHead->head.len);

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
  assert(walRollback(pWal, fromIndex) == 0);
  return 0;  // to avoid compiler error
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
  assert(walCommit(pWal, index) == 0);
  return 0;  // to avoid compiler error
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
  char               u64buf[128];
  SSyncLogStoreData* pData = (SSyncLogStoreData*)pLogStore->data;
  cJSON*             pRoot = cJSON_CreateObject();

  if (pData != NULL && pData->pWal != NULL) {
    snprintf(u64buf, sizeof(u64buf), "%p", pData->pSyncNode);
    cJSON_AddStringToObject(pRoot, "pSyncNode", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pData->pWal);
    cJSON_AddStringToObject(pRoot, "pWal", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%ld", logStoreLastIndex(pLogStore));
    cJSON_AddStringToObject(pRoot, "LastIndex", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%lu", logStoreLastTerm(pLogStore));
    cJSON_AddStringToObject(pRoot, "LastTerm", u64buf);

    cJSON* pEntries = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "pEntries", pEntries);
    SyncIndex lastIndex = logStoreLastIndex(pLogStore);
    for (SyncIndex i = 0; i <= lastIndex; ++i) {
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
  char               u64buf[128];
  SSyncLogStoreData* pData = (SSyncLogStoreData*)pLogStore->data;
  cJSON*             pRoot = cJSON_CreateObject();

  if (pData != NULL && pData->pWal != NULL) {
    snprintf(u64buf, sizeof(u64buf), "%p", pData->pSyncNode);
    cJSON_AddStringToObject(pRoot, "pSyncNode", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pData->pWal);
    cJSON_AddStringToObject(pRoot, "pWal", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%ld", logStoreLastIndex(pLogStore));
    cJSON_AddStringToObject(pRoot, "LastIndex", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%lu", logStoreLastTerm(pLogStore));
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
  char* serialized = logStore2Str(pLogStore);
  sTraceLong("logStoreLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void logStoreLog2(char* s, SSyncLogStore* pLogStore) {
  char* serialized = logStore2Str(pLogStore);
  sTraceLong("logStoreLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
  taosMemoryFree(serialized);
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
  char* serialized = logStoreSimple2Str(pLogStore);
  sTrace("logStoreSimpleLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
  taosMemoryFree(serialized);
}