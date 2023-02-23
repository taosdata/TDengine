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
#include "syncRaftEntry.h"
#include "syncUtil.h"
#include "tref.h"

SSyncRaftEntry* syncEntryBuild(int32_t dataLen) {
  int32_t         bytes = sizeof(SSyncRaftEntry) + dataLen;
  SSyncRaftEntry* pEntry = taosMemoryCalloc(1, bytes);
  if (pEntry == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pEntry->bytes = bytes;
  pEntry->dataLen = dataLen;
  pEntry->rid = -1;

  return pEntry;
}

SSyncRaftEntry* syncEntryBuildFromClientRequest(const SyncClientRequest* pMsg, SyncTerm term, SyncIndex index) {
  SSyncRaftEntry* pEntry = syncEntryBuild(pMsg->dataLen);
  if (pEntry == NULL) return NULL;

  pEntry->msgType = pMsg->msgType;
  pEntry->originalRpcType = pMsg->originalRpcType;
  pEntry->seqNum = pMsg->seqNum;
  pEntry->isWeak = pMsg->isWeak;
  pEntry->term = term;
  pEntry->index = index;
  memcpy(pEntry->data, pMsg->data, pMsg->dataLen);

  return pEntry;
}

SSyncRaftEntry* syncEntryBuildFromRpcMsg(const SRpcMsg* pMsg, SyncTerm term, SyncIndex index) {
  SSyncRaftEntry* pEntry = syncEntryBuild(pMsg->contLen);
  if (pEntry == NULL) return NULL;

  pEntry->msgType = TDMT_SYNC_CLIENT_REQUEST;
  pEntry->originalRpcType = pMsg->msgType;
  pEntry->seqNum = 0;
  pEntry->isWeak = 0;
  pEntry->term = term;
  pEntry->index = index;
  memcpy(pEntry->data, pMsg->pCont, pMsg->contLen);

  return pEntry;
}

SSyncRaftEntry* syncEntryBuildFromAppendEntries(const SyncAppendEntries* pMsg) {
  SSyncRaftEntry* pEntry = syncEntryBuild((int32_t)(pMsg->dataLen));
  if (pEntry == NULL) return NULL;

  memcpy(pEntry, pMsg->data, pMsg->dataLen);
  return pEntry;
}

SSyncRaftEntry* syncEntryBuildNoop(SyncTerm term, SyncIndex index, int32_t vgId) {
  SSyncRaftEntry* pEntry = syncEntryBuild(sizeof(SMsgHead));
  if (pEntry == NULL) return NULL;

  pEntry->msgType = TDMT_SYNC_CLIENT_REQUEST;
  pEntry->originalRpcType = TDMT_SYNC_NOOP;
  pEntry->seqNum = 0;
  pEntry->isWeak = 0;
  pEntry->term = term;
  pEntry->index = index;

  SMsgHead* pHead = (SMsgHead*)pEntry->data;
  pHead->vgId = vgId;
  pHead->contLen = sizeof(SMsgHead);

  return pEntry;
}

void syncEntryDestroy(SSyncRaftEntry* pEntry) {
  if (pEntry != NULL) {
    sTrace("free entry:%p", pEntry);
    taosMemoryFree(pEntry);
  }
}

void syncEntry2OriginalRpc(const SSyncRaftEntry* pEntry, SRpcMsg* pRpcMsg) {
  pRpcMsg->msgType = pEntry->originalRpcType;
  pRpcMsg->contLen = (int32_t)(pEntry->dataLen);
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  memcpy(pRpcMsg->pCont, pEntry->data, pRpcMsg->contLen);
}

SRaftEntryHashCache* raftCacheCreate(SSyncNode* pSyncNode, int32_t maxCount) {
  SRaftEntryHashCache* pCache = taosMemoryMalloc(sizeof(SRaftEntryHashCache));
  if (pCache == NULL) {
    sError("vgId:%d, raft cache create error", pSyncNode->vgId);
    return NULL;
  }

  pCache->pEntryHash =
      taosHashInit(sizeof(SyncIndex), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (pCache->pEntryHash == NULL) {
    sError("vgId:%d, raft cache create hash error", pSyncNode->vgId);
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
    taosThreadMutexLock(&pCache->mutex);
    taosHashCleanup(pCache->pEntryHash);
    taosThreadMutexUnlock(&pCache->mutex);
    taosThreadMutexDestroy(&(pCache->mutex));
    taosMemoryFree(pCache);
  }
}

// success, return 1
// max count, return 0
// error, return -1
int32_t raftCachePutEntry(struct SRaftEntryHashCache* pCache, SSyncRaftEntry* pEntry) {
  taosThreadMutexLock(&pCache->mutex);

  if (pCache->currentCount >= pCache->maxCount) {
    taosThreadMutexUnlock(&pCache->mutex);
    return 0;
  }

  taosHashPut(pCache->pEntryHash, &(pEntry->index), sizeof(pEntry->index), pEntry, pEntry->bytes);
  ++(pCache->currentCount);

  sNTrace(pCache->pSyncNode, "raft cache add, type:%s,%d, type2:%s,%d, index:%" PRId64 ", bytes:%d",
          TMSG_INFO(pEntry->msgType), pEntry->msgType, TMSG_INFO(pEntry->originalRpcType), pEntry->originalRpcType,
          pEntry->index, pEntry->bytes);
  taosThreadMutexUnlock(&pCache->mutex);
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

  taosThreadMutexLock(&pCache->mutex);
  void* pTmp = taosHashGet(pCache->pEntryHash, &index, sizeof(index));
  if (pTmp != NULL) {
    SSyncRaftEntry* pEntry = pTmp;
    *ppEntry = taosMemoryMalloc(pEntry->bytes);
    memcpy(*ppEntry, pTmp, pEntry->bytes);

    sNTrace(pCache->pSyncNode, "raft cache get, type:%s,%d, type2:%s,%d, index:%" PRId64,
            TMSG_INFO((*ppEntry)->msgType), (*ppEntry)->msgType, TMSG_INFO((*ppEntry)->originalRpcType),
            (*ppEntry)->originalRpcType, (*ppEntry)->index);
    taosThreadMutexUnlock(&pCache->mutex);
    return 0;
  }

  taosThreadMutexUnlock(&pCache->mutex);
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

  taosThreadMutexLock(&pCache->mutex);
  void* pTmp = taosHashGet(pCache->pEntryHash, &index, sizeof(index));
  if (pTmp != NULL) {
    SSyncRaftEntry* pEntry = pTmp;
    *ppEntry = pEntry;

    sNTrace(pCache->pSyncNode, "raft cache get, type:%s,%d, type2:%s,%d, index:%" PRId64,
            TMSG_INFO((*ppEntry)->msgType), (*ppEntry)->msgType, TMSG_INFO((*ppEntry)->originalRpcType),
            (*ppEntry)->originalRpcType, (*ppEntry)->index);
    taosThreadMutexUnlock(&pCache->mutex);
    return 0;
  }

  taosThreadMutexUnlock(&pCache->mutex);
  terrno = TSDB_CODE_WAL_LOG_NOT_EXIST;
  return -1;
}

int32_t raftCacheDelEntry(struct SRaftEntryHashCache* pCache, SyncIndex index) {
  taosThreadMutexLock(&pCache->mutex);
  taosHashRemove(pCache->pEntryHash, &index, sizeof(index));
  --(pCache->currentCount);
  taosThreadMutexUnlock(&pCache->mutex);
  return 0;
}

int32_t raftCacheGetAndDel(struct SRaftEntryHashCache* pCache, SyncIndex index, SSyncRaftEntry** ppEntry) {
  if (ppEntry == NULL) {
    return -1;
  }
  *ppEntry = NULL;

  taosThreadMutexLock(&pCache->mutex);
  void* pTmp = taosHashGet(pCache->pEntryHash, &index, sizeof(index));
  if (pTmp != NULL) {
    SSyncRaftEntry* pEntry = pTmp;
    *ppEntry = taosMemoryMalloc(pEntry->bytes);
    memcpy(*ppEntry, pTmp, pEntry->bytes);

    sNTrace(pCache->pSyncNode, "raft cache get-and-del, type:%s,%d, type2:%s,%d, index:%" PRId64,
            TMSG_INFO((*ppEntry)->msgType), (*ppEntry)->msgType, TMSG_INFO((*ppEntry)->originalRpcType),
            (*ppEntry)->originalRpcType, (*ppEntry)->index);

    taosHashRemove(pCache->pEntryHash, &index, sizeof(index));
    --(pCache->currentCount);

    taosThreadMutexUnlock(&pCache->mutex);
    return 0;
  }

  taosThreadMutexUnlock(&pCache->mutex);
  terrno = TSDB_CODE_WAL_LOG_NOT_EXIST;
  return -1;
}

int32_t raftCacheClear(struct SRaftEntryHashCache* pCache) {
  taosThreadMutexLock(&pCache->mutex);
  taosHashClear(pCache->pEntryHash);
  pCache->currentCount = 0;
  taosThreadMutexUnlock(&pCache->mutex);
  return 0;
}

static char* keyFn(const void* pData) {
  SSyncRaftEntry* pEntry = (SSyncRaftEntry*)pData;
  return (char*)(&(pEntry->index));
}

static int cmpFn(const void* p1, const void* p2) { return memcmp(p1, p2, sizeof(SyncIndex)); }

static void freeRaftEntry(void* param) {
  SSyncRaftEntry* pEntry = (SSyncRaftEntry*)param;
  syncEntryDestroy(pEntry);
}

SRaftEntryCache* raftEntryCacheCreate(SSyncNode* pSyncNode, int32_t maxCount) {
  SRaftEntryCache* pCache = taosMemoryMalloc(sizeof(SRaftEntryCache));
  if (pCache == NULL) {
    sError("vgId:%d, raft cache create error", pSyncNode->vgId);
    return NULL;
  }

  pCache->pSkipList =
      tSkipListCreate(MAX_SKIP_LIST_LEVEL, TSDB_DATA_TYPE_BINARY, sizeof(SyncIndex), cmpFn, SL_ALLOW_DUP_KEY, keyFn);
  if (pCache->pSkipList == NULL) {
    sError("vgId:%d, raft cache create hash error", pSyncNode->vgId);
    return NULL;
  }

  taosThreadMutexInit(&(pCache->mutex), NULL);
  pCache->refMgr = taosOpenRef(10, freeRaftEntry);
  pCache->maxCount = maxCount;
  pCache->currentCount = 0;
  pCache->pSyncNode = pSyncNode;

  return pCache;
}

void raftEntryCacheDestroy(SRaftEntryCache* pCache) {
  if (pCache != NULL) {
    taosThreadMutexLock(&pCache->mutex);
    tSkipListDestroy(pCache->pSkipList);
    if (pCache->refMgr != -1) {
      taosCloseRef(pCache->refMgr);
      pCache->refMgr = -1;
    }
    taosThreadMutexUnlock(&pCache->mutex);
    taosThreadMutexDestroy(&(pCache->mutex));
    taosMemoryFree(pCache);
  }
}

// success, return 1
// max count, return 0
// error, return -1
int32_t raftEntryCachePutEntry(struct SRaftEntryCache* pCache, SSyncRaftEntry* pEntry) {
  taosThreadMutexLock(&pCache->mutex);

  if (pCache->currentCount >= pCache->maxCount) {
    taosThreadMutexUnlock(&pCache->mutex);
    return 0;
  }

  SSkipListNode* pSkipListNode = tSkipListPut(pCache->pSkipList, pEntry);
  ASSERT(pSkipListNode != NULL);
  ++(pCache->currentCount);

  pEntry->rid = taosAddRef(pCache->refMgr, pEntry);
  ASSERT(pEntry->rid >= 0);

  sNTrace(pCache->pSyncNode, "raft cache add, type:%s,%d, type2:%s,%d, index:%" PRId64 ", bytes:%d",
          TMSG_INFO(pEntry->msgType), pEntry->msgType, TMSG_INFO(pEntry->originalRpcType), pEntry->originalRpcType,
          pEntry->index, pEntry->bytes);
  taosThreadMutexUnlock(&pCache->mutex);
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
    int32_t bytes = (int32_t)pEntry->bytes;
    *ppEntry = taosMemoryMalloc((int64_t)bytes);
    memcpy(*ppEntry, pEntry, pEntry->bytes);
    (*ppEntry)->rid = -1;
  } else {
    *ppEntry = NULL;
  }
  return code;
}

// find one, return 1
// not found, return 0
// error, return -1
int32_t raftEntryCacheGetEntryP(struct SRaftEntryCache* pCache, SyncIndex index, SSyncRaftEntry** ppEntry) {
  taosThreadMutexLock(&pCache->mutex);

  SyncIndex index2 = index;
  int32_t   code = 0;

  SArray* entryPArray = tSkipListGet(pCache->pSkipList, (char*)(&index2));
  int32_t arraySize = taosArrayGetSize(entryPArray);
  if (arraySize == 1) {
    SSkipListNode** ppNode = (SSkipListNode**)taosArrayGet(entryPArray, 0);
    ASSERT(*ppNode != NULL);
    *ppEntry = (SSyncRaftEntry*)SL_GET_NODE_DATA(*ppNode);
    taosAcquireRef(pCache->refMgr, (*ppEntry)->rid);
    code = 1;

  } else if (arraySize == 0) {
    code = 0;

  } else {
    ASSERT(0);

    code = -1;
  }
  taosArrayDestroy(entryPArray);

  taosThreadMutexUnlock(&pCache->mutex);
  return code;
}

// count = -1, clear all
// count >= 0, clear count
// return -1, error
// return delete count
int32_t raftEntryCacheClear(struct SRaftEntryCache* pCache, int32_t count) {
  taosThreadMutexLock(&pCache->mutex);
  int32_t returnCnt = 0;

  if (count == -1) {
    // clear all
    SSkipListIterator* pIter = tSkipListCreateIter(pCache->pSkipList);
    while (tSkipListIterNext(pIter)) {
      SSkipListNode* pNode = tSkipListIterGet(pIter);
      ASSERT(pNode != NULL);
      SSyncRaftEntry* pEntry = (SSyncRaftEntry*)SL_GET_NODE_DATA(pNode);
      syncEntryDestroy(pEntry);
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

      // syncEntryDestroy(pEntry);
      taosRemoveRef(pCache->refMgr, pEntry->rid);
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
  taosThreadMutexUnlock(&pCache->mutex);
  return returnCnt;
}
