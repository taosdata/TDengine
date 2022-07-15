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

#include "syncRaftEntry.h"
#include "syncUtil.h"

SSyncRaftEntry* syncEntryBuild(uint32_t dataLen) {
  uint32_t        bytes = sizeof(SSyncRaftEntry) + dataLen;
  SSyncRaftEntry* pEntry = taosMemoryMalloc(bytes);
  ASSERT(pEntry != NULL);
  memset(pEntry, 0, bytes);
  pEntry->bytes = bytes;
  pEntry->dataLen = dataLen;
  return pEntry;
}

// step 4. SyncClientRequest => SSyncRaftEntry, add term, index
SSyncRaftEntry* syncEntryBuild2(SyncClientRequest* pMsg, SyncTerm term, SyncIndex index) {
  SSyncRaftEntry* pEntry = syncEntryBuild3(pMsg, term, index);
  ASSERT(pEntry != NULL);

  return pEntry;
}

SSyncRaftEntry* syncEntryBuild3(SyncClientRequest* pMsg, SyncTerm term, SyncIndex index) {
  SSyncRaftEntry* pEntry = syncEntryBuild(pMsg->dataLen);
  ASSERT(pEntry != NULL);

  pEntry->msgType = pMsg->msgType;
  pEntry->originalRpcType = pMsg->originalRpcType;
  pEntry->seqNum = pMsg->seqNum;
  pEntry->isWeak = pMsg->isWeak;
  pEntry->term = term;
  pEntry->index = index;
  pEntry->dataLen = pMsg->dataLen;
  memcpy(pEntry->data, pMsg->data, pMsg->dataLen);

  return pEntry;
}

SSyncRaftEntry* syncEntryBuild4(SRpcMsg* pOriginalMsg, SyncTerm term, SyncIndex index) {
  SSyncRaftEntry* pEntry = syncEntryBuild(pOriginalMsg->contLen);
  ASSERT(pEntry != NULL);

  pEntry->msgType = TDMT_SYNC_CLIENT_REQUEST;
  pEntry->originalRpcType = pOriginalMsg->msgType;
  pEntry->seqNum = 0;
  pEntry->isWeak = 0;
  pEntry->term = term;
  pEntry->index = index;
  pEntry->dataLen = pOriginalMsg->contLen;
  memcpy(pEntry->data, pOriginalMsg->pCont, pOriginalMsg->contLen);

  return pEntry;
}

SSyncRaftEntry* syncEntryBuildNoop(SyncTerm term, SyncIndex index, int32_t vgId) {
  // init rpcMsg
  SMsgHead head;
  head.vgId = vgId;
  head.contLen = sizeof(SMsgHead);
  SRpcMsg rpcMsg;
  memset(&rpcMsg, 0, sizeof(SRpcMsg));
  rpcMsg.contLen = head.contLen;
  rpcMsg.pCont = rpcMallocCont(rpcMsg.contLen);
  rpcMsg.msgType = TDMT_SYNC_NOOP;
  memcpy(rpcMsg.pCont, &head, sizeof(head));

  SSyncRaftEntry* pEntry = syncEntryBuild(rpcMsg.contLen);
  ASSERT(pEntry != NULL);

  pEntry->msgType = TDMT_SYNC_CLIENT_REQUEST;
  pEntry->originalRpcType = TDMT_SYNC_NOOP;
  pEntry->seqNum = 0;
  pEntry->isWeak = 0;
  pEntry->term = term;
  pEntry->index = index;

  ASSERT(pEntry->dataLen == rpcMsg.contLen);
  memcpy(pEntry->data, rpcMsg.pCont, rpcMsg.contLen);
  rpcFreeCont(rpcMsg.pCont);

  return pEntry;
}

void syncEntryDestory(SSyncRaftEntry* pEntry) {
  if (pEntry != NULL) {
    taosMemoryFree(pEntry);
  }
}

// step 5. SSyncRaftEntry => bin, to raft log
char* syncEntrySerialize(const SSyncRaftEntry* pEntry, uint32_t* len) {
  char* buf = taosMemoryMalloc(pEntry->bytes);
  ASSERT(buf != NULL);
  memcpy(buf, pEntry, pEntry->bytes);
  if (len != NULL) {
    *len = pEntry->bytes;
  }
  return buf;
}

// step 6. bin => SSyncRaftEntry, from raft log
SSyncRaftEntry* syncEntryDeserialize(const char* buf, uint32_t len) {
  uint32_t        bytes = *((uint32_t*)buf);
  SSyncRaftEntry* pEntry = taosMemoryMalloc(bytes);
  ASSERT(pEntry != NULL);
  memcpy(pEntry, buf, len);
  ASSERT(len == pEntry->bytes);
  return pEntry;
}

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
    cJSON_AddNumberToObject(pRoot, "dataLen", pEntry->dataLen);

    char* s;
    s = syncUtilprintBin((char*)(pEntry->data), pEntry->dataLen);
    cJSON_AddStringToObject(pRoot, "data", s);
    taosMemoryFree(s);

    s = syncUtilprintBin2((char*)(pEntry->data), pEntry->dataLen);
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

// step 7. SSyncRaftEntry => original SRpcMsg, commit to user, delete seqNum, isWeak, term, index
void syncEntry2OriginalRpc(const SSyncRaftEntry* pEntry, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pEntry->originalRpcType;
  pRpcMsg->contLen = pEntry->dataLen;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  memcpy(pRpcMsg->pCont, pEntry->data, pRpcMsg->contLen);
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
SRaftEntryHashCache* raftCacheCreate(SSyncNode* pSyncNode, int32_t maxCount) {
  SRaftEntryHashCache* pCache = taosMemoryMalloc(sizeof(SRaftEntryHashCache));
  if (pCache == NULL) {
    sError("vgId:%d raft cache create error", pSyncNode->vgId);
    return NULL;
  }

  pCache->pEntryHash =
      taosHashInit(sizeof(SyncIndex), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (pCache->pEntryHash == NULL) {
    sError("vgId:%d raft cache create hash error", pSyncNode->vgId);
    return NULL;
  }

  taosThreadMutexInit(&(pCache->mutex), NULL);
  pCache->maxCount = maxCount;
  pCache->currentCount = 0;
  pCache->pSyncNode = pSyncNode;

  return pCache;
}

void raftCacheDestroy(SRaftEntryHashCache* pCache) {
  if (pCache != NULL) {
    taosThreadMutexLock(&(pCache->mutex));
    taosHashCleanup(pCache->pEntryHash);
    taosThreadMutexUnlock(&(pCache->mutex));
    taosThreadMutexDestroy(&(pCache->mutex));
    taosMemoryFree(pCache);
  }
}

// success, return 1
// max count, return 0
// error, return -1
int32_t raftCachePutEntry(struct SRaftEntryHashCache* pCache, SSyncRaftEntry* pEntry) {
  taosThreadMutexLock(&(pCache->mutex));

  if (pCache->currentCount >= pCache->maxCount) {
    taosThreadMutexUnlock(&(pCache->mutex));
    return 0;
  }

  taosHashPut(pCache->pEntryHash, &(pEntry->index), sizeof(pEntry->index), pEntry, pEntry->bytes);
  ++(pCache->currentCount);

  do {
    char eventLog[128];
    snprintf(eventLog, sizeof(eventLog), "raft cache add, type:%s,%d, type2:%s,%d, index:%" PRId64 ", bytes:%d",
             TMSG_INFO(pEntry->msgType), pEntry->msgType, TMSG_INFO(pEntry->originalRpcType), pEntry->originalRpcType,
             pEntry->index, pEntry->bytes);
    syncNodeEventLog(pCache->pSyncNode, eventLog);
  } while (0);

  taosThreadMutexUnlock(&(pCache->mutex));
  return 1;
}

// success, return 0
// error, return -1
// not exist, return -1, terrno = TSDB_CODE_WAL_LOG_NOT_EXIST
int32_t raftCacheGetEntry(struct SRaftEntryHashCache* pCache, SyncIndex index, SSyncRaftEntry** ppEntry) {
  if (ppEntry == NULL) {
    return -1;
  }
  *ppEntry = NULL;

  taosThreadMutexLock(&(pCache->mutex));
  void* pTmp = taosHashGet(pCache->pEntryHash, &index, sizeof(index));
  if (pTmp != NULL) {
    SSyncRaftEntry* pEntry = pTmp;
    *ppEntry = taosMemoryMalloc(pEntry->bytes);
    memcpy(*ppEntry, pTmp, pEntry->bytes);

    do {
      char eventLog[128];
      snprintf(eventLog, sizeof(eventLog), "raft cache get, type:%s,%d, type2:%s,%d, index:%" PRId64,
               TMSG_INFO((*ppEntry)->msgType), (*ppEntry)->msgType, TMSG_INFO((*ppEntry)->originalRpcType),
               (*ppEntry)->originalRpcType, (*ppEntry)->index);
      syncNodeEventLog(pCache->pSyncNode, eventLog);
    } while (0);

    taosThreadMutexUnlock(&(pCache->mutex));
    return 0;
  }

  taosThreadMutexUnlock(&(pCache->mutex));
  terrno = TSDB_CODE_WAL_LOG_NOT_EXIST;
  return -1;
}

// success, return 0
// error, return -1
// not exist, return -1, terrno = TSDB_CODE_WAL_LOG_NOT_EXIST
int32_t raftCacheGetEntryP(struct SRaftEntryHashCache* pCache, SyncIndex index, SSyncRaftEntry** ppEntry) {
  if (ppEntry == NULL) {
    return -1;
  }
  *ppEntry = NULL;

  taosThreadMutexLock(&(pCache->mutex));
  void* pTmp = taosHashGet(pCache->pEntryHash, &index, sizeof(index));
  if (pTmp != NULL) {
    SSyncRaftEntry* pEntry = pTmp;
    *ppEntry = pEntry;

    do {
      char eventLog[128];
      snprintf(eventLog, sizeof(eventLog), "raft cache get, type:%s,%d, type2:%s,%d, index:%" PRId64,
               TMSG_INFO((*ppEntry)->msgType), (*ppEntry)->msgType, TMSG_INFO((*ppEntry)->originalRpcType),
               (*ppEntry)->originalRpcType, (*ppEntry)->index);
      syncNodeEventLog(pCache->pSyncNode, eventLog);
    } while (0);

    taosThreadMutexUnlock(&(pCache->mutex));
    return 0;
  }

  taosThreadMutexUnlock(&(pCache->mutex));
  terrno = TSDB_CODE_WAL_LOG_NOT_EXIST;
  return -1;
}

int32_t raftCacheDelEntry(struct SRaftEntryHashCache* pCache, SyncIndex index) {
  taosThreadMutexLock(&(pCache->mutex));
  taosHashRemove(pCache->pEntryHash, &index, sizeof(index));
  --(pCache->currentCount);
  taosThreadMutexUnlock(&(pCache->mutex));
  return 0;
}

int32_t raftCacheGetAndDel(struct SRaftEntryHashCache* pCache, SyncIndex index, SSyncRaftEntry** ppEntry) {
  if (ppEntry == NULL) {
    return -1;
  }
  *ppEntry = NULL;

  taosThreadMutexLock(&(pCache->mutex));
  void* pTmp = taosHashGet(pCache->pEntryHash, &index, sizeof(index));
  if (pTmp != NULL) {
    SSyncRaftEntry* pEntry = pTmp;
    *ppEntry = taosMemoryMalloc(pEntry->bytes);
    memcpy(*ppEntry, pTmp, pEntry->bytes);

    do {
      char eventLog[128];
      snprintf(eventLog, sizeof(eventLog), "raft cache get-and-del, type:%s,%d, type2:%s,%d, index:%" PRId64,
               TMSG_INFO((*ppEntry)->msgType), (*ppEntry)->msgType, TMSG_INFO((*ppEntry)->originalRpcType),
               (*ppEntry)->originalRpcType, (*ppEntry)->index);
      syncNodeEventLog(pCache->pSyncNode, eventLog);
    } while (0);

    taosHashRemove(pCache->pEntryHash, &index, sizeof(index));
    --(pCache->currentCount);

    taosThreadMutexUnlock(&(pCache->mutex));
    return 0;
  }

  taosThreadMutexUnlock(&(pCache->mutex));
  terrno = TSDB_CODE_WAL_LOG_NOT_EXIST;
  return -1;
}

int32_t raftCacheClear(struct SRaftEntryHashCache* pCache) {
  taosThreadMutexLock(&(pCache->mutex));
  taosHashClear(pCache->pEntryHash);
  pCache->currentCount = 0;
  taosThreadMutexUnlock(&(pCache->mutex));
  return 0;
}

//-----------------------------------
cJSON* raftCache2Json(SRaftEntryHashCache* pCache) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pCache != NULL) {
    taosThreadMutexLock(&(pCache->mutex));

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

    taosThreadMutexUnlock(&(pCache->mutex));
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
  printf("raftCachePrint | len:%" PRIu64 " | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void raftCachePrint2(char* s, SRaftEntryHashCache* pCache) {
  char* serialized = raftCache2Str(pCache);
  printf("raftCachePrint2 | len:%" PRIu64 " | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void raftCacheLog(SRaftEntryHashCache* pCache) {
  char* serialized = raftCache2Str(pCache);
  sTrace("raftCacheLog | len:%" PRIu64 " | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void raftCacheLog2(char* s, SRaftEntryHashCache* pCache) {
  if (gRaftDetailLog) {
    char* serialized = raftCache2Str(pCache);
    sTraceLong("raftCacheLog2 | len:%" PRIu64 " | %s | %s", strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

//-----------------------------------
static char* keyFn(const void* pData) {
  SSyncRaftEntry* pEntry = (SSyncRaftEntry*)pData;
  return (char*)(&(pEntry->index));
}

static int cmpFn(const void* p1, const void* p2) { return memcmp(p1, p2, sizeof(SyncIndex)); }

SRaftEntryCache* raftEntryCacheCreate(SSyncNode* pSyncNode, int32_t maxCount) {
  SRaftEntryCache* pCache = taosMemoryMalloc(sizeof(SRaftEntryCache));
  if (pCache == NULL) {
    sError("vgId:%d raft cache create error", pSyncNode->vgId);
    return NULL;
  }

  pCache->pSkipList =
      tSkipListCreate(MAX_SKIP_LIST_LEVEL, TSDB_DATA_TYPE_BINARY, sizeof(SyncIndex), cmpFn, SL_ALLOW_DUP_KEY, keyFn);
  if (pCache->pSkipList == NULL) {
    sError("vgId:%d raft cache create hash error", pSyncNode->vgId);
    return NULL;
  }

  taosThreadMutexInit(&(pCache->mutex), NULL);
  pCache->maxCount = maxCount;
  pCache->currentCount = 0;
  pCache->pSyncNode = pSyncNode;

  return pCache;
}

void raftEntryCacheDestroy(SRaftEntryCache* pCache) {
  if (pCache != NULL) {
    taosThreadMutexLock(&(pCache->mutex));
    tSkipListDestroy(pCache->pSkipList);
    taosThreadMutexUnlock(&(pCache->mutex));
    taosThreadMutexDestroy(&(pCache->mutex));
    taosMemoryFree(pCache);
  }
}

// success, return 1
// max count, return 0
// error, return -1
int32_t raftEntryCachePutEntry(struct SRaftEntryCache* pCache, SSyncRaftEntry* pEntry) {
  taosThreadMutexLock(&(pCache->mutex));

  if (pCache->currentCount >= pCache->maxCount) {
    taosThreadMutexUnlock(&(pCache->mutex));
    return 0;
  }

  SSkipListNode* pSkipListNode = tSkipListPut(pCache->pSkipList, pEntry);
  ASSERT(pSkipListNode != NULL);
  ++(pCache->currentCount);

  do {
    char eventLog[128];
    snprintf(eventLog, sizeof(eventLog), "raft cache add, type:%s,%d, type2:%s,%d, index:%" PRId64 ", bytes:%d",
             TMSG_INFO(pEntry->msgType), pEntry->msgType, TMSG_INFO(pEntry->originalRpcType), pEntry->originalRpcType,
             pEntry->index, pEntry->bytes);
    syncNodeEventLog(pCache->pSyncNode, eventLog);
  } while (0);

  taosThreadMutexUnlock(&(pCache->mutex));
  return 1;
}

// find one, return 1
// not found, return 0
// error, return -1
int32_t raftEntryCacheGetEntry(struct SRaftEntryCache* pCache, SyncIndex index, SSyncRaftEntry** ppEntry) {
  ASSERT(ppEntry != NULL);
  SSyncRaftEntry* pEntry = NULL;
  int32_t         code = raftEntryCacheGetEntryP(pCache, index, &pEntry);
  if (code == 1) {
    *ppEntry = taosMemoryMalloc(pEntry->bytes);
    memcpy(*ppEntry, pEntry, pEntry->bytes);
  } else {
    *ppEntry = NULL;
  }
  return code;
}

// find one, return 1
// not found, return 0
// error, return -1
int32_t raftEntryCacheGetEntryP(struct SRaftEntryCache* pCache, SyncIndex index, SSyncRaftEntry** ppEntry) {
  taosThreadMutexLock(&(pCache->mutex));

  SyncIndex index2 = index;
  int32_t   code = 0;

  SArray* entryPArray = tSkipListGet(pCache->pSkipList, (char*)(&index2));
  int32_t arraySize = taosArrayGetSize(entryPArray);
  if (arraySize == 1) {
    SSkipListNode** ppNode = (SSkipListNode**)taosArrayGet(entryPArray, 0);
    ASSERT(*ppNode != NULL);
    *ppEntry = (SSyncRaftEntry*)SL_GET_NODE_DATA(*ppNode);
    code = 1;

  } else if (arraySize == 0) {
    code = 0;

  } else {
    ASSERT(0);

    code = -1;
  }
  taosArrayDestroy(entryPArray);

  taosThreadMutexUnlock(&(pCache->mutex));
  return code;
}

// count = -1, clear all
// count >= 0, clear count
// return -1, error
// return delete count
int32_t raftEntryCacheClear(struct SRaftEntryCache* pCache, int32_t count) {
  taosThreadMutexLock(&(pCache->mutex));
  int32_t returnCnt = 0;

  if (count == -1) {
    // clear all
    SSkipListIterator* pIter = tSkipListCreateIter(pCache->pSkipList);
    while (tSkipListIterNext(pIter)) {
      SSkipListNode* pNode = tSkipListIterGet(pIter);
      ASSERT(pNode != NULL);
      SSyncRaftEntry* pEntry = (SSyncRaftEntry*)SL_GET_NODE_DATA(pNode);
      syncEntryDestory(pEntry);
      ++returnCnt;
    }
    tSkipListDestroyIter(pIter);

    tSkipListDestroy(pCache->pSkipList);
    pCache->pSkipList =
        tSkipListCreate(MAX_SKIP_LIST_LEVEL, TSDB_DATA_TYPE_BINARY, sizeof(SyncIndex), cmpFn, SL_ALLOW_DUP_KEY, keyFn);
    ASSERT(pCache->pSkipList != NULL);

  } else {
    // clear count
    int                i = 0;
    SSkipListIterator* pIter = tSkipListCreateIter(pCache->pSkipList);
    SArray*            delNodeArray = taosArrayInit(0, sizeof(SSkipListNode*));

    // free entry
    while (tSkipListIterNext(pIter)) {
      SSkipListNode* pNode = tSkipListIterGet(pIter);
      ASSERT(pNode != NULL);
      if (i++ >= count) {
        break;
      }

      // sDebug("push pNode:%p", pNode);
      taosArrayPush(delNodeArray, &pNode);
      ++returnCnt;
      SSyncRaftEntry* pEntry = (SSyncRaftEntry*)SL_GET_NODE_DATA(pNode);
      syncEntryDestory(pEntry);
    }
    tSkipListDestroyIter(pIter);

    // delete skiplist node
    int32_t arraySize = taosArrayGetSize(delNodeArray);
    for (int32_t i = 0; i < arraySize; ++i) {
      SSkipListNode** ppNode = taosArrayGet(delNodeArray, i);
      // sDebug("get pNode:%p", *ppNode);
      tSkipListRemoveNode(pCache->pSkipList, *ppNode);
    }
    taosArrayDestroy(delNodeArray);
  }

  pCache->currentCount -= returnCnt;
  taosThreadMutexUnlock(&(pCache->mutex));
  return returnCnt;
}

cJSON* raftEntryCache2Json(SRaftEntryCache* pCache) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pCache != NULL) {
    taosThreadMutexLock(&(pCache->mutex));

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

    taosThreadMutexUnlock(&(pCache->mutex));
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
  printf("raftEntryCachePrint | len:%" PRIu64 " | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void raftEntryCachePrint2(char* s, SRaftEntryCache* pObj) {
  char* serialized = raftEntryCache2Str(pObj);
  printf("raftEntryCachePrint2 | len:%" PRIu64 " | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void raftEntryCacheLog(SRaftEntryCache* pObj) {
  char* serialized = raftEntryCache2Str(pObj);
  sTrace("raftEntryCacheLog | len:%" PRIu64 " | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void raftEntryCacheLog2(char* s, SRaftEntryCache* pObj) {
  if (gRaftDetailLog) {
    char* serialized = raftEntryCache2Str(pObj);
    sTraceLong("raftEntryCacheLog2 | len:%" PRIu64 " | %s | %s", strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}