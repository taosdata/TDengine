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

cJSON* syncEntry2Json(const SSyncRaftEntry* pEntry) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pEntry != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pEntry->bytes);
    cJSON_AddNumberToObject(pRoot, "msgType", pEntry->msgType);
    cJSON_AddNumberToObject(pRoot, "originalRpcType", pEntry->originalRpcType);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pEntry->seqNum);
    cJSON_AddStringToObject(pRoot, "seqNum", u64buf);
    cJSON_AddNumberToObject(pRoot, "isWeak", pEntry->isWeak);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pEntry->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pEntry->index);
    cJSON_AddStringToObject(pRoot, "index", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pEntry->rid);
    cJSON_AddStringToObject(pRoot, "rid", u64buf);
    cJSON_AddNumberToObject(pRoot, "dataLen", pEntry->dataLen);

    char* s;
    s = syncUtilPrintBin((char*)(pEntry->data), pEntry->dataLen);
    cJSON_AddStringToObject(pRoot, "data", s);
    taosMemoryFree(s);

    s = syncUtilPrintBin2((char*)(pEntry->data), pEntry->dataLen);
    cJSON_AddStringToObject(pRoot, "data2", s);
    taosMemoryFree(s);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SSyncRaftEntry", pRoot);
  return pJson;
}

char* syncEntry2Str(const SSyncRaftEntry* pEntry) {
  cJSON* pJson = syncEntry2Json(pEntry);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// for debug ----------------------
void syncEntryPrint(const SSyncRaftEntry* pObj) {
  char* serialized = syncEntry2Str(pObj);
  printf("syncEntryPrint | len:%zu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncEntryPrint2(char* s, const SSyncRaftEntry* pObj) {
  char* serialized = syncEntry2Str(pObj);
  printf("syncEntryPrint2 | len:%zu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncEntryLog(const SSyncRaftEntry* pObj) {
  char* serialized = syncEntry2Str(pObj);
  sTrace("syncEntryLog | len:%zu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncEntryLog2(char* s, const SSyncRaftEntry* pObj) {
  char* serialized = syncEntry2Str(pObj);
  sTrace("syncEntryLog2 | len:%zu | %s | %s", strlen(serialized), s, serialized);
  taosMemoryFree(serialized);
}

//-----------------------------------
cJSON* raftCache2Json(SRaftEntryHashCache* pCache) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pCache != NULL) {
    taosThreadMutexLock(&pCache->mutex);

    snprintf(u64buf, sizeof(u64buf), "%p", pCache->pSyncNode);
    cJSON_AddStringToObject(pRoot, "pSyncNode", u64buf);
    cJSON_AddNumberToObject(pRoot, "currentCount", pCache->currentCount);
    cJSON_AddNumberToObject(pRoot, "maxCount", pCache->maxCount);
    cJSON* pEntries = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "entries", pEntries);

    SSyncRaftEntry* pIter = (SSyncRaftEntry*)taosHashIterate(pCache->pEntryHash, NULL);
    if (pIter != NULL) {
      SSyncRaftEntry* pEntry = (SSyncRaftEntry*)pIter;
      cJSON_AddItemToArray(pEntries, syncEntry2Json(pEntry));
    }
    while (pIter) {
      pIter = taosHashIterate(pCache->pEntryHash, pIter);
      if (pIter != NULL) {
        SSyncRaftEntry* pEntry = (SSyncRaftEntry*)pIter;
        cJSON_AddItemToArray(pEntries, syncEntry2Json(pEntry));
      }
    }

    taosThreadMutexUnlock(&pCache->mutex);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SRaftEntryHashCache", pRoot);
  return pJson;
}

char* raftCache2Str(SRaftEntryHashCache* pCache) {
  cJSON* pJson = raftCache2Json(pCache);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

void raftCachePrint(SRaftEntryHashCache* pCache) {
  char* serialized = raftCache2Str(pCache);
  printf("raftCachePrint | len:%d | %s \n", (int32_t)strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void raftCachePrint2(char* s, SRaftEntryHashCache* pCache) {
  char* serialized = raftCache2Str(pCache);
  printf("raftCachePrint2 | len:%d | %s | %s \n", (int32_t)strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void raftCacheLog(SRaftEntryHashCache* pCache) {
  char* serialized = raftCache2Str(pCache);
  sTrace("raftCacheLog | len:%d | %s", (int32_t)strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void raftCacheLog2(char* s, SRaftEntryHashCache* pCache) {
  if (gRaftDetailLog) {
    char* serialized = raftCache2Str(pCache);
    sLTrace("raftCacheLog2 | len:%d | %s | %s", (int32_t)strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

cJSON* raftEntryCache2Json(SRaftEntryCache* pCache) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pCache != NULL) {
    taosThreadMutexLock(&pCache->mutex);

    snprintf(u64buf, sizeof(u64buf), "%p", pCache->pSyncNode);
    cJSON_AddStringToObject(pRoot, "pSyncNode", u64buf);
    cJSON_AddNumberToObject(pRoot, "currentCount", pCache->currentCount);
    cJSON_AddNumberToObject(pRoot, "maxCount", pCache->maxCount);
    cJSON* pEntries = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "entries", pEntries);

    SSkipListIterator* pIter = tSkipListCreateIter(pCache->pSkipList);
    while (tSkipListIterNext(pIter)) {
      SSkipListNode* pNode = tSkipListIterGet(pIter);
      ASSERT(pNode != NULL);
      SSyncRaftEntry* pEntry = (SSyncRaftEntry*)SL_GET_NODE_DATA(pNode);
      cJSON_AddItemToArray(pEntries, syncEntry2Json(pEntry));
    }
    tSkipListDestroyIter(pIter);

    taosThreadMutexUnlock(&pCache->mutex);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SRaftEntryCache", pRoot);
  return pJson;
}

char* raftEntryCache2Str(SRaftEntryCache* pObj) {
  cJSON* pJson = raftEntryCache2Json(pObj);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

void raftEntryCachePrint(SRaftEntryCache* pObj) {
  char* serialized = raftEntryCache2Str(pObj);
  printf("raftEntryCachePrint | len:%d | %s \n", (int32_t)strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void raftEntryCachePrint2(char* s, SRaftEntryCache* pObj) {
  char* serialized = raftEntryCache2Str(pObj);
  printf("raftEntryCachePrint2 | len:%d | %s | %s \n", (int32_t)strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void raftEntryCacheLog(SRaftEntryCache* pObj) {
  char* serialized = raftEntryCache2Str(pObj);
  sTrace("raftEntryCacheLog | len:%d | %s", (int32_t)strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void raftEntryCacheLog2(char* s, SRaftEntryCache* pObj) {
  if (gRaftDetailLog) {
    char* serialized = raftEntryCache2Str(pObj);
    sLTrace("raftEntryCacheLog2 | len:%d | %s | %s", (int32_t)strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}
