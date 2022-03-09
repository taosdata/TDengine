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
int32_t logStoreAppendEntry(SSyncLogStore* pLogStore, SRpcMsg* pEntry) {}

// get one log entry, user need to free pEntry->pCont
int32_t logStoreGetEntry(SSyncLogStore* pLogStore, SyncIndex index, SRpcMsg* pEntry) {}

// truncate log with index, entries after the given index (>=index) will be deleted
int32_t logStoreTruncate(SSyncLogStore* pLogStore, SyncIndex fromIndex) {}

// return index of last entry
SyncIndex logStoreLastIndex(SSyncLogStore* pLogStore) {}

// return term of last entry
SyncTerm logStoreLastTerm(SSyncLogStore* pLogStore) {}

// update log store commit index with "index"
int32_t logStoreUpdateCommitIndex(SSyncLogStore* pLogStore, SyncIndex index) {}

// return commit index of log
SyncIndex logStoreGetCommitIndex(SSyncLogStore* pLogStore) {}

cJSON* logStore2Json(SSyncLogStore* pLogStore) {}

char* logStore2Str(SSyncLogStore* pLogStore) {
  cJSON* pJson = logStore2Json(pLogStore);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}