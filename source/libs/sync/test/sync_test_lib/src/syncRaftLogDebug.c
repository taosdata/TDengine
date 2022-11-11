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
#include "syncTest.h"

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
        SSyncRaftEntry* pEntry = NULL;
        raftLogGetEntry(pLogStore, i, &pEntry);

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

// for debug -----------------
void logStorePrint(SSyncLogStore* pLogStore) {
  char* serialized = logStore2Str(pLogStore);
  printf("logStorePrint | len:%d | %s \n", (int32_t)strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void logStorePrint2(char* s, SSyncLogStore* pLogStore) {
  char* serialized = logStore2Str(pLogStore);
  printf("logStorePrint2 | len:%d | %s | %s \n", (int32_t)strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void logStoreLog(SSyncLogStore* pLogStore) {
  if (gRaftDetailLog) {
    char* serialized = logStore2Str(pLogStore);
    sLTrace("logStoreLog | len:%d | %s", (int32_t)strlen(serialized), serialized);
    taosMemoryFree(serialized);
  }
}

void logStoreLog2(char* s, SSyncLogStore* pLogStore) {
  if (gRaftDetailLog) {
    char* serialized = logStore2Str(pLogStore);
    sLTrace("logStoreLog2 | len:%d | %s | %s", (int32_t)strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

// for debug -----------------
void logStoreSimplePrint(SSyncLogStore* pLogStore) {
  char* serialized = logStoreSimple2Str(pLogStore);
  printf("logStoreSimplePrint | len:%d | %s \n", (int32_t)strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void logStoreSimplePrint2(char* s, SSyncLogStore* pLogStore) {
  char* serialized = logStoreSimple2Str(pLogStore);
  printf("logStoreSimplePrint2 | len:%d | %s | %s \n", (int32_t)strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void logStoreSimpleLog(SSyncLogStore* pLogStore) {
  char* serialized = logStoreSimple2Str(pLogStore);
  sTrace("logStoreSimpleLog | len:%d | %s", (int32_t)strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void logStoreSimpleLog2(char* s, SSyncLogStore* pLogStore) {
  if (gRaftDetailLog) {
    char* serialized = logStoreSimple2Str(pLogStore);
    sTrace("logStoreSimpleLog2 | len:%d | %s | %s", (int32_t)strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}
