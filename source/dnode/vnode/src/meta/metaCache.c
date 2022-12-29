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
#include "meta.h"

#define META_CACHE_BASE_BUCKET  1024
#define META_CACHE_STATS_BUCKET 16

// (uid , suid) : child table
// (uid,     0) : normal table
// (suid, suid) : super table
typedef struct SMetaCacheEntry SMetaCacheEntry;
struct SMetaCacheEntry {
  SMetaCacheEntry* next;
  SMetaInfo        info;
};

typedef struct SMetaStbStatsEntry {
  struct SMetaStbStatsEntry* next;
  SMetaStbStats              info;
} SMetaStbStatsEntry;

typedef struct STagFilterResEntry {
  uint64_t suid;    // uid for super table
  SList    list;    // the linked list of md5 digest, extracted from the serialized tag query condition
  uint32_t qTimes;  // queried times for current super table
} STagFilterResEntry;

struct SMetaCache {
  // child, normal, super, table entry cache
  struct SEntryCache {
    int32_t           nEntry;
    int32_t           nBucket;
    SMetaCacheEntry** aBucket;
  } sEntryCache;

  // stable stats cache
  struct SStbStatsCache {
    int32_t              nEntry;
    int32_t              nBucket;
    SMetaStbStatsEntry** aBucket;
  } sStbStatsCache;

  // query cache
  struct STagFilterResCache {
    TdThreadMutex lock;
    SHashObj*  pTableEntry;
    SLRUCache* pUidResCache;
  } sTagFilterResCache;
};

static void entryCacheClose(SMeta* pMeta) {
  if (pMeta->pCache) {
    // close entry cache
    for (int32_t iBucket = 0; iBucket < pMeta->pCache->sEntryCache.nBucket; iBucket++) {
      SMetaCacheEntry* pEntry = pMeta->pCache->sEntryCache.aBucket[iBucket];
      while (pEntry) {
        SMetaCacheEntry* tEntry = pEntry->next;
        taosMemoryFree(pEntry);
        pEntry = tEntry;
      }
    }
    taosMemoryFree(pMeta->pCache->sEntryCache.aBucket);
  }
}

static void statsCacheClose(SMeta* pMeta) {
  if (pMeta->pCache) {
    // close entry cache
    for (int32_t iBucket = 0; iBucket < pMeta->pCache->sStbStatsCache.nBucket; iBucket++) {
      SMetaStbStatsEntry* pEntry = pMeta->pCache->sStbStatsCache.aBucket[iBucket];
      while (pEntry) {
        SMetaStbStatsEntry* tEntry = pEntry->next;
        taosMemoryFree(pEntry);
        pEntry = tEntry;
      }
    }
    taosMemoryFree(pMeta->pCache->sStbStatsCache.aBucket);
  }
}

static void freeCacheEntryFp(void* param) {
  STagFilterResEntry** p = param;
  tdListEmpty(&(*p)->list);
  taosMemoryFreeClear(*p);
}

int32_t metaCacheOpen(SMeta* pMeta) {
  int32_t     code = 0;
  SMetaCache* pCache = NULL;

  pCache = (SMetaCache*)taosMemoryMalloc(sizeof(SMetaCache));
  if (pCache == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  // open entry cache
  pCache->sEntryCache.nEntry = 0;
  pCache->sEntryCache.nBucket = META_CACHE_BASE_BUCKET;
  pCache->sEntryCache.aBucket =
      (SMetaCacheEntry**)taosMemoryCalloc(pCache->sEntryCache.nBucket, sizeof(SMetaCacheEntry*));
  if (pCache->sEntryCache.aBucket == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  // open stats cache
  pCache->sStbStatsCache.nEntry = 0;
  pCache->sStbStatsCache.nBucket = META_CACHE_STATS_BUCKET;
  pCache->sStbStatsCache.aBucket =
      (SMetaStbStatsEntry**)taosMemoryCalloc(pCache->sStbStatsCache.nBucket, sizeof(SMetaStbStatsEntry*));
  if (pCache->sStbStatsCache.aBucket == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err2;
  }

  pCache->sTagFilterResCache.pUidResCache = taosLRUCacheInit(5 * 1024 * 1024, -1, 0.5);
  if (pCache->sTagFilterResCache.pUidResCache == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err2;
  }

  pCache->sTagFilterResCache.pTableEntry =
      taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), false, HASH_NO_LOCK);
  if (pCache->sTagFilterResCache.pTableEntry == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err2;
  }

  taosHashSetFreeFp(pCache->sTagFilterResCache.pTableEntry, freeCacheEntryFp);
  taosThreadMutexInit(&pCache->sTagFilterResCache.lock, NULL);

  pMeta->pCache = pCache;
  return code;

_err2:
  entryCacheClose(pMeta);

_err:
  taosMemoryFree(pCache);
  metaError("vgId:%d, meta open cache failed since %s", TD_VID(pMeta->pVnode), tstrerror(code));
  return code;
}

void metaCacheClose(SMeta* pMeta) {
  if (pMeta->pCache) {
    entryCacheClose(pMeta);
    statsCacheClose(pMeta);

    taosHashCleanup(pMeta->pCache->sTagFilterResCache.pTableEntry);
    taosLRUCacheCleanup(pMeta->pCache->sTagFilterResCache.pUidResCache);
    taosThreadMutexDestroy(&pMeta->pCache->sTagFilterResCache.lock);

    taosMemoryFree(pMeta->pCache);
    pMeta->pCache = NULL;
  }
}

static int32_t metaRehashCache(SMetaCache* pCache, int8_t expand) {
  int32_t code = 0;
  int32_t nBucket;

  if (expand) {
    nBucket = pCache->sEntryCache.nBucket * 2;
  } else {
    nBucket = pCache->sEntryCache.nBucket / 2;
  }

  SMetaCacheEntry** aBucket = (SMetaCacheEntry**)taosMemoryCalloc(nBucket, sizeof(SMetaCacheEntry*));
  if (aBucket == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  // rehash
  for (int32_t iBucket = 0; iBucket < pCache->sEntryCache.nBucket; iBucket++) {
    SMetaCacheEntry* pEntry = pCache->sEntryCache.aBucket[iBucket];

    while (pEntry) {
      SMetaCacheEntry* pTEntry = pEntry->next;

      pEntry->next = aBucket[TABS(pEntry->info.uid) % nBucket];
      aBucket[TABS(pEntry->info.uid) % nBucket] = pEntry;

      pEntry = pTEntry;
    }
  }

  // final set
  taosMemoryFree(pCache->sEntryCache.aBucket);
  pCache->sEntryCache.nBucket = nBucket;
  pCache->sEntryCache.aBucket = aBucket;

_exit:
  return code;
}

int32_t metaCacheUpsert(SMeta* pMeta, SMetaInfo* pInfo) {
  int32_t code = 0;

  // ASSERT(metaIsWLocked(pMeta));

  // search
  SMetaCache*       pCache = pMeta->pCache;
  int32_t           iBucket = TABS(pInfo->uid) % pCache->sEntryCache.nBucket;
  SMetaCacheEntry** ppEntry = &pCache->sEntryCache.aBucket[iBucket];
  while (*ppEntry && (*ppEntry)->info.uid != pInfo->uid) {
    ppEntry = &(*ppEntry)->next;
  }

  if (*ppEntry) {  // update
    ASSERT(pInfo->suid == (*ppEntry)->info.suid);
    if (pInfo->version > (*ppEntry)->info.version) {
      (*ppEntry)->info.version = pInfo->version;
      (*ppEntry)->info.skmVer = pInfo->skmVer;
    }
  } else {  // insert
    if (pCache->sEntryCache.nEntry >= pCache->sEntryCache.nBucket) {
      code = metaRehashCache(pCache, 1);
      if (code) goto _exit;

      iBucket = TABS(pInfo->uid) % pCache->sEntryCache.nBucket;
    }

    SMetaCacheEntry* pEntryNew = (SMetaCacheEntry*)taosMemoryMalloc(sizeof(*pEntryNew));
    if (pEntryNew == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }

    pEntryNew->info = *pInfo;
    pEntryNew->next = pCache->sEntryCache.aBucket[iBucket];
    pCache->sEntryCache.aBucket[iBucket] = pEntryNew;
    pCache->sEntryCache.nEntry++;
  }

_exit:
  return code;
}

int32_t metaCacheDrop(SMeta* pMeta, int64_t uid) {
  int32_t code = 0;

  SMetaCache*       pCache = pMeta->pCache;
  int32_t           iBucket = TABS(uid) % pCache->sEntryCache.nBucket;
  SMetaCacheEntry** ppEntry = &pCache->sEntryCache.aBucket[iBucket];
  while (*ppEntry && (*ppEntry)->info.uid != uid) {
    ppEntry = &(*ppEntry)->next;
  }

  SMetaCacheEntry* pEntry = *ppEntry;
  if (pEntry) {
    *ppEntry = pEntry->next;
    taosMemoryFree(pEntry);
    pCache->sEntryCache.nEntry--;
    if (pCache->sEntryCache.nEntry < pCache->sEntryCache.nBucket / 4 &&
        pCache->sEntryCache.nBucket > META_CACHE_BASE_BUCKET) {
      code = metaRehashCache(pCache, 0);
      if (code) goto _exit;
    }
  } else {
    code = TSDB_CODE_NOT_FOUND;
  }

_exit:
  return code;
}

int32_t metaCacheGet(SMeta* pMeta, int64_t uid, SMetaInfo* pInfo) {
  int32_t code = 0;

  SMetaCache*      pCache = pMeta->pCache;
  int32_t          iBucket = TABS(uid) % pCache->sEntryCache.nBucket;
  SMetaCacheEntry* pEntry = pCache->sEntryCache.aBucket[iBucket];

  while (pEntry && pEntry->info.uid != uid) {
    pEntry = pEntry->next;
  }

  if (pEntry) {
    *pInfo = pEntry->info;
  } else {
    code = TSDB_CODE_NOT_FOUND;
  }

  return code;
}

static int32_t metaRehashStatsCache(SMetaCache* pCache, int8_t expand) {
  int32_t code = 0;
  int32_t nBucket;

  if (expand) {
    nBucket = pCache->sStbStatsCache.nBucket * 2;
  } else {
    nBucket = pCache->sStbStatsCache.nBucket / 2;
  }

  SMetaStbStatsEntry** aBucket = (SMetaStbStatsEntry**)taosMemoryCalloc(nBucket, sizeof(SMetaStbStatsEntry*));
  if (aBucket == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  // rehash
  for (int32_t iBucket = 0; iBucket < pCache->sStbStatsCache.nBucket; iBucket++) {
    SMetaStbStatsEntry* pEntry = pCache->sStbStatsCache.aBucket[iBucket];

    while (pEntry) {
      SMetaStbStatsEntry* pTEntry = pEntry->next;

      pEntry->next = aBucket[TABS(pEntry->info.uid) % nBucket];
      aBucket[TABS(pEntry->info.uid) % nBucket] = pEntry;

      pEntry = pTEntry;
    }
  }

  // final set
  taosMemoryFree(pCache->sStbStatsCache.aBucket);
  pCache->sStbStatsCache.nBucket = nBucket;
  pCache->sStbStatsCache.aBucket = aBucket;

_exit:
  return code;
}

int32_t metaStatsCacheUpsert(SMeta* pMeta, SMetaStbStats* pInfo) {
  int32_t code = 0;

  // ASSERT(metaIsWLocked(pMeta));

  // search
  SMetaCache*          pCache = pMeta->pCache;
  int32_t              iBucket = TABS(pInfo->uid) % pCache->sStbStatsCache.nBucket;
  SMetaStbStatsEntry** ppEntry = &pCache->sStbStatsCache.aBucket[iBucket];
  while (*ppEntry && (*ppEntry)->info.uid != pInfo->uid) {
    ppEntry = &(*ppEntry)->next;
  }

  if (*ppEntry) {  // update
    (*ppEntry)->info.ctbNum = pInfo->ctbNum;
  } else {  // insert
    if (pCache->sStbStatsCache.nEntry >= pCache->sStbStatsCache.nBucket) {
      code = metaRehashStatsCache(pCache, 1);
      if (code) goto _exit;

      iBucket = TABS(pInfo->uid) % pCache->sStbStatsCache.nBucket;
    }

    SMetaStbStatsEntry* pEntryNew = (SMetaStbStatsEntry*)taosMemoryMalloc(sizeof(*pEntryNew));
    if (pEntryNew == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }

    pEntryNew->info = *pInfo;
    pEntryNew->next = pCache->sStbStatsCache.aBucket[iBucket];
    pCache->sStbStatsCache.aBucket[iBucket] = pEntryNew;
    pCache->sStbStatsCache.nEntry++;
  }

_exit:
  return code;
}

int32_t metaStatsCacheDrop(SMeta* pMeta, int64_t uid) {
  int32_t code = 0;

  SMetaCache*          pCache = pMeta->pCache;
  int32_t              iBucket = TABS(uid) % pCache->sStbStatsCache.nBucket;
  SMetaStbStatsEntry** ppEntry = &pCache->sStbStatsCache.aBucket[iBucket];
  while (*ppEntry && (*ppEntry)->info.uid != uid) {
    ppEntry = &(*ppEntry)->next;
  }

  SMetaStbStatsEntry* pEntry = *ppEntry;
  if (pEntry) {
    *ppEntry = pEntry->next;
    taosMemoryFree(pEntry);
    pCache->sStbStatsCache.nEntry--;
    if (pCache->sStbStatsCache.nEntry < pCache->sStbStatsCache.nBucket / 4 &&
        pCache->sStbStatsCache.nBucket > META_CACHE_STATS_BUCKET) {
      code = metaRehashStatsCache(pCache, 0);
      if (code) goto _exit;
    }
  } else {
    code = TSDB_CODE_NOT_FOUND;
  }

_exit:
  return code;
}

int32_t metaStatsCacheGet(SMeta* pMeta, int64_t uid, SMetaStbStats* pInfo) {
  int32_t code = TSDB_CODE_SUCCESS;

  SMetaCache*         pCache = pMeta->pCache;
  int32_t             iBucket = TABS(uid) % pCache->sStbStatsCache.nBucket;
  SMetaStbStatsEntry* pEntry = pCache->sStbStatsCache.aBucket[iBucket];

  while (pEntry && pEntry->info.uid != uid) {
    pEntry = pEntry->next;
  }

  if (pEntry) {
    *pInfo = pEntry->info;
  } else {
    code = TSDB_CODE_NOT_FOUND;
  }

  return code;
}

int32_t metaGetCachedTableUidList(SMeta* pMeta, tb_uid_t suid, const uint8_t* pKey, int32_t keyLen, SArray* pList1,
                                  bool* acquireRes) {
  // generate the composed key for LRU cache
  SLRUCache*     pCache = pMeta->pCache->sTagFilterResCache.pUidResCache;
  SHashObj*      pTableMap = pMeta->pCache->sTagFilterResCache.pTableEntry;
  TdThreadMutex* pLock = &pMeta->pCache->sTagFilterResCache.lock;

  uint64_t buf[3] = {0};
  uint32_t times = 0;

  *acquireRes = 0;
  buf[0] = suid;
  memcpy(&buf[1], pKey, keyLen);

  taosThreadMutexLock(pLock);

  int32_t    len = keyLen + sizeof(uint64_t);
  LRUHandle* pHandle = taosLRUCacheLookup(pCache, buf, len);
  if (pHandle == NULL) {
    taosThreadMutexUnlock(pLock);
    return TSDB_CODE_SUCCESS;
  }

  // do some book mark work after acquiring the filter result from cache
  STagFilterResEntry** pEntry = taosHashGet(pTableMap, &suid, sizeof(uint64_t));
  ASSERT(pEntry != NULL);
  *acquireRes = 1;

  const char* p = taosLRUCacheValue(pCache, pHandle);
  int32_t     size = *(int32_t*)p;

  // set the result into the buffer
  taosArrayAddBatch(pList1, p + sizeof(int32_t), size);

  times = atomic_add_fetch_32(&(*pEntry)->qTimes, 1);
  taosLRUCacheRelease(pCache, pHandle, false);

  // unlock meta
  taosThreadMutexUnlock(pLock);

  // check if scanning all items are necessary or not
  if (times >= 5000 && TD_DLIST_NELES(&(*pEntry)->list) > 10) {
    taosThreadMutexLock(pLock);

    SArray* pInvalidRes = taosArrayInit(64, POINTER_BYTES);

    SListIter iter = {0};
    tdListInitIter(&(*pEntry)->list, &iter, TD_LIST_FORWARD);

    SListNode* pNode = NULL;
    while ((pNode = tdListNext(&iter)) != NULL) {
      memcpy(&buf[1], pNode->data, keyLen);

      // check whether it is existed in LRU cache, and remove it from linked list if not.
      LRUHandle* pRes = taosLRUCacheLookup(pCache, buf, len);
      if (pRes == NULL) {  // remove the item in the linked list
        taosArrayPush(pInvalidRes, &pNode);
      } else {
        taosLRUCacheRelease(pCache, pRes, false);
      }
    }

    // remove the keys, of which query uid lists have been replaced already.
    size_t s = taosArrayGetSize(pInvalidRes);
    for (int32_t i = 0; i < s; ++i) {
      SListNode** p1 = taosArrayGet(pInvalidRes, i);
      tdListPopNode(&(*pEntry)->list, *p1);
      taosMemoryFree(*p1);
    }

    atomic_store_32(&(*pEntry)->qTimes, 0); // reset the query times
    taosArrayDestroy(pInvalidRes);

    taosThreadMutexUnlock(pLock);
  }

  return TSDB_CODE_SUCCESS;
}

static void freePayload(const void* key, size_t keyLen, void* value) {
  if (value == NULL) {
    return;
  }
  taosMemoryFree(value);
}

// check both the payload size and selectivity ratio
int32_t metaUidFilterCachePut(SMeta* pMeta, uint64_t suid, const void* pKey, int32_t keyLen, void* pPayload,
                              int32_t payloadLen, double selectivityRatio) {
  if (selectivityRatio > tsSelectivityRatio) {
    metaDebug("vgId:%d, suid:%" PRIu64
              " failed to add to uid list cache, due to selectivity ratio %.2f less than threshold %.2f",
              TD_VID(pMeta->pVnode), suid, selectivityRatio, tsSelectivityRatio);
    taosMemoryFree(pPayload);
    return TSDB_CODE_SUCCESS;
  }

  if (payloadLen > tsTagFilterResCacheSize) {
    metaDebug("vgId:%d, suid:%" PRIu64
              " failed to add to uid list cache, due to payload length %d greater than threshold %d",
              TD_VID(pMeta->pVnode), suid, payloadLen, tsTagFilterResCacheSize);
    taosMemoryFree(pPayload);
    return TSDB_CODE_SUCCESS;
  }

  SLRUCache*     pCache = pMeta->pCache->sTagFilterResCache.pUidResCache;
  SHashObj*      pTableEntry = pMeta->pCache->sTagFilterResCache.pTableEntry;
  TdThreadMutex* pLock = &pMeta->pCache->sTagFilterResCache.lock;

  taosThreadMutexLock(pLock);

  STagFilterResEntry** pEntry = taosHashGet(pTableEntry, &suid, sizeof(uint64_t));
  if (pEntry == NULL) {
    STagFilterResEntry* p = taosMemoryMalloc(sizeof(STagFilterResEntry));
    p->qTimes = 0;
    tdListInit(&p->list, keyLen);
    taosHashPut(pTableEntry, &suid, sizeof(uint64_t), &p, POINTER_BYTES);
    tdListAppend(&p->list, pKey);
  } else {
    tdListAppend(&(*pEntry)->list, pKey);
  }

  uint64_t buf[3] = {0};
  buf[0] = suid;

  memcpy(&buf[1], pKey, keyLen);
  ASSERT(sizeof(uint64_t) + keyLen == 24);

  // add to cache.
  taosLRUCacheInsert(pCache, buf, sizeof(uint64_t) + keyLen, pPayload, payloadLen, freePayload, NULL,
                     TAOS_LRU_PRIORITY_LOW);

  taosThreadMutexUnlock(pLock);

  metaDebug("vgId:%d, suid:%" PRIu64 " list cache added into cache, total:%d, tables:%d", TD_VID(pMeta->pVnode), suid,
            (int32_t)taosLRUCacheGetUsage(pCache), taosHashGetSize(pTableEntry));

  return TSDB_CODE_SUCCESS;
}

// remove the lru cache that are expired due to the tags value update, or creating, or dropping, of child tables
int32_t metaUidCacheClear(SMeta* pMeta, uint64_t suid) {
  int32_t  keyLen = sizeof(uint64_t) * 3;
  uint64_t p[3] = {0};
  p[0] = suid;

  TdThreadMutex* pLock = &pMeta->pCache->sTagFilterResCache.lock;

  taosThreadMutexLock(pLock);
  STagFilterResEntry** pEntry = taosHashGet(pMeta->pCache->sTagFilterResCache.pTableEntry, &suid, sizeof(uint64_t));
  if (pEntry == NULL || listNEles(&(*pEntry)->list) == 0) {
    taosThreadMutexUnlock(pLock);
    return TSDB_CODE_SUCCESS;
  }

  SListIter iter = {0};
  tdListInitIter(&(*pEntry)->list, &iter, TD_LIST_FORWARD);

  SListNode* pNode = NULL;
  while ((pNode = tdListNext(&iter)) != NULL) {
    memcpy(&p[1], pNode->data, 16);
    taosLRUCacheErase(pMeta->pCache->sTagFilterResCache.pUidResCache, p, keyLen);
  }

  (*pEntry)->qTimes = 0;
  tdListEmpty(&(*pEntry)->list);

  taosThreadMutexUnlock(pLock);
  return TSDB_CODE_SUCCESS;
}
