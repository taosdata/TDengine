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
  SSyncLogStore* pLogStore = malloc(sizeof(SSyncLogStore));
  assert(pLogStore != NULL);

  pLogStore->data = malloc(sizeof(SSyncLogStoreData));
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
  return pLogStore; // to avoid compiler error
}

void logStoreDestory(SSyncLogStore* pLogStore) {
  if (pLogStore != NULL) {
    free(pLogStore->data);
    free(pLogStore);
  }
}

int32_t logStoreAppendEntry(SSyncLogStore* pLogStore, SSyncRaftEntry* pEntry) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;

  assert(pEntry->index == logStoreLastIndex(pLogStore) + 1);
  uint32_t len;
  char*    serialized = syncEntrySerialize(pEntry, &len);
  assert(serialized != NULL);

  int code;
  code = walWrite(pWal, pEntry->index, pEntry->msgType, serialized, len);
  assert(code == 0);

  walFsync(pWal, true);
  free(serialized);
  return code; // to avoid compiler error
}

SSyncRaftEntry* logStoreGetEntry(SSyncLogStore* pLogStore, SyncIndex index) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  SSyncRaftEntry*    pEntry;

  SWalReadHandle* pWalHandle = walOpenReadHandle(pWal);
  walReadWithHandle(pWalHandle, index);
  pEntry = syncEntryDeserialize(pWalHandle->pHead->head.body, pWalHandle->pHead->head.len);
  assert(pEntry != NULL);

  // need to hold, do not new every time!!
  walCloseReadHandle(pWalHandle);
  return pEntry;
}

int32_t logStoreTruncate(SSyncLogStore* pLogStore, SyncIndex fromIndex) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  walRollback(pWal, fromIndex);
  return 0; // to avoid compiler error
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
    free(pLastEntry);
  }
  return lastTerm;
}

int32_t logStoreUpdateCommitIndex(SSyncLogStore* pLogStore, SyncIndex index) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  walCommit(pWal, index);
  return 0; // to avoid compiler error
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
  char u64buf[128];

  SSyncLogStoreData* pData = (SSyncLogStoreData*)pLogStore->data;
  cJSON*             pRoot = cJSON_CreateObject();
  snprintf(u64buf, sizeof(u64buf), "%p", pData->pSyncNode);
  cJSON_AddStringToObject(pRoot, "pSyncNode", u64buf);
  snprintf(u64buf, sizeof(u64buf), "%p", pData->pWal);
  cJSON_AddStringToObject(pRoot, "pWal", u64buf);
  snprintf(u64buf, sizeof(u64buf), "%" PRId64 "", logStoreLastIndex(pLogStore));
  cJSON_AddStringToObject(pRoot, "LastIndex", u64buf);
  snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", logStoreLastTerm(pLogStore));
  cJSON_AddStringToObject(pRoot, "LastTerm", u64buf);

  cJSON* pEntries = cJSON_CreateArray();
  cJSON_AddItemToObject(pRoot, "pEntries", pEntries);
  SyncIndex lastIndex = logStoreLastIndex(pLogStore);
  for (SyncIndex i = 0; i <= lastIndex; ++i) {
    SSyncRaftEntry* pEntry = logStoreGetEntry(pLogStore, i);
    cJSON_AddItemToArray(pEntries, syncEntry2Json(pEntry));
    syncEntryDestory(pEntry);
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

// for debug -----------------
void logStorePrint(SSyncLogStore* pLogStore) {
  char* serialized = logStore2Str(pLogStore);
  printf("logStorePrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  free(serialized);
}

void logStorePrint2(char* s, SSyncLogStore* pLogStore) {
  char* serialized = logStore2Str(pLogStore);
  printf("logStorePrint | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  free(serialized);
}

void logStoreLog(SSyncLogStore* pLogStore) {
  char* serialized = logStore2Str(pLogStore);
  sTrace("logStorePrint | len:%lu | %s", strlen(serialized), serialized);
  free(serialized);
}

void logStoreLog2(char* s, SSyncLogStore* pLogStore) {
  char* serialized = logStore2Str(pLogStore);
  sTrace("logStorePrint | len:%lu | %s | %s", strlen(serialized), s, serialized);
  free(serialized);
}
