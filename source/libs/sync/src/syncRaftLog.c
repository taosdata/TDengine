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
}

void logStoreDestory(SSyncLogStore* pLogStore) {
  if (pLogStore != NULL) {
    free(pLogStore->data);
    free(pLogStore);
  }
}

// append one log entry
int32_t logStoreAppendEntry(SSyncLogStore* pLogStore, SSyncRaftEntry* pEntry) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  char*              buf = malloc(pEntry->bytes);

  syncEntrySerialize(pEntry, buf, pEntry->bytes);
  walWrite(pWal, pEntry->index, pEntry->msgType, buf, pEntry->bytes);
  walFsync(pWal, true);

  free(buf);
}

// get one log entry, user need to free pEntry->pCont
SSyncRaftEntry* logStoreGetEntry(SSyncLogStore* pLogStore, SyncIndex index) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  SSyncRaftEntry*    pEntry;

  SWalReadHandle* pWalHandle = walOpenReadHandle(pWal);
  walReadWithHandle(pWalHandle, index);

  // need to hold, do not new every time!!
  walCloseReadHandle(pWalHandle);
  return pEntry;
}

// truncate log with index, entries after the given index (>=index) will be deleted
int32_t logStoreTruncate(SSyncLogStore* pLogStore, SyncIndex fromIndex) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  walRollback(pWal, fromIndex);
}

// return index of last entry
SyncIndex logStoreLastIndex(SSyncLogStore* pLogStore) {
  SSyncRaftEntry* pLastEntry = logStoreGetLastEntry(pLogStore);
  SyncIndex       lastIndex = pLastEntry->index;
  free(pLastEntry);
  return lastIndex;
}

// return term of last entry
SyncTerm logStoreLastTerm(SSyncLogStore* pLogStore) {
  SSyncRaftEntry* pLastEntry = logStoreGetLastEntry(pLogStore);
  SyncTerm        lastTerm = pLastEntry->term;
  free(pLastEntry);
  return lastTerm;
}

// update log store commit index with "index"
int32_t logStoreUpdateCommitIndex(SSyncLogStore* pLogStore, SyncIndex index) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  walCommit(pWal, index);
}

// return commit index of log
SyncIndex logStoreGetCommitIndex(SSyncLogStore* pLogStore) {
  SSyncLogStoreData* pData = pLogStore->data;
  return pData->pSyncNode->commitIndex;
}

SSyncRaftEntry* logStoreGetLastEntry(SSyncLogStore* pLogStore) {
  SSyncLogStoreData* pData = pLogStore->data;
  SWal*              pWal = pData->pWal;
  SyncIndex          lastIndex = walGetLastVer(pWal);
  SSyncRaftEntry*    pEntry;
  pEntry = logStoreGetEntry(pLogStore, lastIndex);
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
  snprintf(u64buf, sizeof(u64buf), "%lu", logStoreLastIndex(pLogStore));
  cJSON_AddStringToObject(pRoot, "LastIndex", u64buf);
  snprintf(u64buf, sizeof(u64buf), "%lu", logStoreLastTerm(pLogStore));
  cJSON_AddStringToObject(pRoot, "LastTerm", u64buf);

  cJSON* pEntries = cJSON_CreateArray();
  cJSON_AddItemToObject(pRoot, "pEntries", pEntries);
  SyncIndex lastIndex = logStoreLastIndex(pLogStore);
  for (SyncIndex i = 1; i <= lastIndex; ++i) {
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

// for debug
void logStorePrint(SSyncLogStore* pLogStore) {
  char* s = logStore2Str(pLogStore);
  sTrace("%s", s);
  free(s);
}