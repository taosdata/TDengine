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

#ifdef TD_ENTERPRISE
extern const char* tkLogStb[];
extern const char* tkAuditStb[];
extern const int   tkLogStbNum;
extern const int   tkAuditStbNum;
#endif

#define TAG_FILTER_RES_KEY_LEN  32
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
  SHashObj *set;    // the set of md5 digest, extracted from the serialized tag query condition
  uint32_t hitTimes;  // queried times for current super table
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
    uint32_t      accTimes;
    SHashObj*     pTableEntry;
    SLRUCache*    pUidResCache;
  } sTagFilterResCache;

  struct STbGroupResCache {
    TdThreadMutex lock;
    uint32_t      accTimes;
    SHashObj*     pTableEntry;
    SLRUCache*    pResCache;
  } STbGroupResCache;

  struct STbFilterCache {
    SHashObj* pStb;
    SHashObj* pStbName;
  } STbFilterCache;

  struct STbRefDbCache {
    TdThreadMutex lock;
    SHashObj*     pStbRefs; // key: suid, value: SHashObj<dbName, refTimes>
  } STbRefDbCache;
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
  taosHashCleanup((*p)->set);
  taosMemoryFreeClear(*p);
}

static void freeRefDbFp(void* param) {
  SHashObj** p = param;
  taosHashCleanup(*p);
  *p = NULL;
}

int32_t metaCacheOpen(SMeta* pMeta) {
  int32_t code = 0;
  int32_t lino;

  pMeta->pCache = (SMetaCache*)taosMemoryCalloc(1, sizeof(SMetaCache));
  if (pMeta->pCache == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  // open entry cache
  pMeta->pCache->sEntryCache.nEntry = 0;
  pMeta->pCache->sEntryCache.nBucket = META_CACHE_BASE_BUCKET;
  pMeta->pCache->sEntryCache.aBucket =
      (SMetaCacheEntry**)taosMemoryCalloc(pMeta->pCache->sEntryCache.nBucket, sizeof(SMetaCacheEntry*));
  if (pMeta->pCache->sEntryCache.aBucket == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  // open stats cache
  pMeta->pCache->sStbStatsCache.nEntry = 0;
  pMeta->pCache->sStbStatsCache.nBucket = META_CACHE_STATS_BUCKET;
  pMeta->pCache->sStbStatsCache.aBucket =
      (SMetaStbStatsEntry**)taosMemoryCalloc(pMeta->pCache->sStbStatsCache.nBucket, sizeof(SMetaStbStatsEntry*));
  if (pMeta->pCache->sStbStatsCache.aBucket == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  // open tag filter cache
  pMeta->pCache->sTagFilterResCache.pUidResCache = taosLRUCacheInit(5 * 1024 * 1024, -1, 0.5);
  if (pMeta->pCache->sTagFilterResCache.pUidResCache == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  pMeta->pCache->sTagFilterResCache.accTimes = 0;
  pMeta->pCache->sTagFilterResCache.pTableEntry =
      taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), false, HASH_NO_LOCK);
  if (pMeta->pCache->sTagFilterResCache.pTableEntry == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  taosHashSetFreeFp(pMeta->pCache->sTagFilterResCache.pTableEntry, freeCacheEntryFp);
  (void)taosThreadMutexInit(&pMeta->pCache->sTagFilterResCache.lock, NULL);

  // open group res cache
  pMeta->pCache->STbGroupResCache.pResCache = taosLRUCacheInit(5 * 1024 * 1024, -1, 0.5);
  if (pMeta->pCache->STbGroupResCache.pResCache == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  pMeta->pCache->STbGroupResCache.accTimes = 0;
  pMeta->pCache->STbGroupResCache.pTableEntry =
      taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), false, HASH_NO_LOCK);
  if (pMeta->pCache->STbGroupResCache.pTableEntry == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  taosHashSetFreeFp(pMeta->pCache->STbGroupResCache.pTableEntry, freeCacheEntryFp);
  (void)taosThreadMutexInit(&pMeta->pCache->STbGroupResCache.lock, NULL);

  // open filter cache
  pMeta->pCache->STbFilterCache.pStb =
      taosHashInit(0, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (pMeta->pCache->STbFilterCache.pStb == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  pMeta->pCache->STbFilterCache.pStbName =
      taosHashInit(0, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), false, HASH_NO_LOCK);
  if (pMeta->pCache->STbFilterCache.pStbName == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  // open ref db cache
  pMeta->pCache->STbRefDbCache.pStbRefs =
      taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (pMeta->pCache->STbRefDbCache.pStbRefs == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  taosHashSetFreeFp(pMeta->pCache->STbRefDbCache.pStbRefs, freeRefDbFp);
  (void)taosThreadMutexInit(&pMeta->pCache->STbRefDbCache.lock, NULL);


_exit:
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pMeta->pVnode), __func__, __FILE__, lino, tstrerror(code));
    metaCacheClose(pMeta);
  } else {
    metaDebug("vgId:%d, %s success", TD_VID(pMeta->pVnode), __func__);
  }
  return code;
}

void metaCacheClose(SMeta* pMeta) {
  if (pMeta->pCache) {
    entryCacheClose(pMeta);
    statsCacheClose(pMeta);

    taosHashClear(pMeta->pCache->sTagFilterResCache.pTableEntry);
    taosLRUCacheCleanup(pMeta->pCache->sTagFilterResCache.pUidResCache);
    (void)taosThreadMutexDestroy(&pMeta->pCache->sTagFilterResCache.lock);
    taosHashCleanup(pMeta->pCache->sTagFilterResCache.pTableEntry);

    taosHashClear(pMeta->pCache->STbGroupResCache.pTableEntry);
    taosLRUCacheCleanup(pMeta->pCache->STbGroupResCache.pResCache);
    (void)taosThreadMutexDestroy(&pMeta->pCache->STbGroupResCache.lock);
    taosHashCleanup(pMeta->pCache->STbGroupResCache.pTableEntry);

    taosHashCleanup(pMeta->pCache->STbFilterCache.pStb);
    taosHashCleanup(pMeta->pCache->STbFilterCache.pStbName);

    taosHashClear(pMeta->pCache->STbRefDbCache.pStbRefs);
    (void)taosThreadMutexDestroy(&pMeta->pCache->STbRefDbCache.lock);
    taosHashCleanup(pMeta->pCache->STbRefDbCache.pStbRefs);

    taosMemoryFree(pMeta->pCache);
    pMeta->pCache = NULL;
  }
}

static void metaRehashCache(SMetaCache* pCache, int8_t expand) {
  int32_t code = 0;
  int32_t nBucket;

  if (expand) {
    nBucket = pCache->sEntryCache.nBucket * 2;
  } else {
    nBucket = pCache->sEntryCache.nBucket / 2;
  }

  SMetaCacheEntry** aBucket = (SMetaCacheEntry**)taosMemoryCalloc(nBucket, sizeof(SMetaCacheEntry*));
  if (aBucket == NULL) {
    return;
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
  return;
}

int32_t metaCacheUpsert(SMeta* pMeta, SMetaInfo* pInfo) {
  int32_t code = 0;

  // meta is wlocked for calling this func.

  // search
  SMetaCache*       pCache = pMeta->pCache;
  int32_t           iBucket = TABS(pInfo->uid) % pCache->sEntryCache.nBucket;
  SMetaCacheEntry** ppEntry = &pCache->sEntryCache.aBucket[iBucket];
  while (*ppEntry && (*ppEntry)->info.uid != pInfo->uid) {
    ppEntry = &(*ppEntry)->next;
  }

  if (*ppEntry) {  // update
    if (pInfo->suid != (*ppEntry)->info.suid) {
      metaError("meta/cache: suid should be same as the one in cache.");
      return TSDB_CODE_INVALID_PARA;
    }
    if (pInfo->version > (*ppEntry)->info.version) {
      (*ppEntry)->info.version = pInfo->version;
      (*ppEntry)->info.skmVer = pInfo->skmVer;
    }
  } else {  // insert
    if (pCache->sEntryCache.nEntry >= pCache->sEntryCache.nBucket) {
      metaRehashCache(pCache, 1);

      iBucket = TABS(pInfo->uid) % pCache->sEntryCache.nBucket;
    }

    SMetaCacheEntry* pEntryNew = (SMetaCacheEntry*)taosMemoryMalloc(sizeof(*pEntryNew));
    if (pEntryNew == NULL) {
      code = terrno;
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
      metaRehashCache(pCache, 0);
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
    if (pInfo) {
      *pInfo = pEntry->info;
    }
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
    code = terrno;
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

  // meta is wlocked for calling this func.

  // search
  SMetaCache*          pCache = pMeta->pCache;
  int32_t              iBucket = TABS(pInfo->uid) % pCache->sStbStatsCache.nBucket;
  SMetaStbStatsEntry** ppEntry = &pCache->sStbStatsCache.aBucket[iBucket];
  while (*ppEntry && (*ppEntry)->info.uid != pInfo->uid) {
    ppEntry = &(*ppEntry)->next;
  }

  if (*ppEntry) {  // update
    (*ppEntry)->info.ctbNum = pInfo->ctbNum;
    (*ppEntry)->info.colNum = pInfo->colNum;
    (*ppEntry)->info.flags = pInfo->flags;
    (*ppEntry)->info.keep = pInfo->keep;
  } else {  // insert
    if (pCache->sStbStatsCache.nEntry >= pCache->sStbStatsCache.nBucket) {
      TAOS_UNUSED(metaRehashStatsCache(pCache, 1));
      iBucket = TABS(pInfo->uid) % pCache->sStbStatsCache.nBucket;
    }

    SMetaStbStatsEntry* pEntryNew = (SMetaStbStatsEntry*)taosMemoryMalloc(sizeof(*pEntryNew));
    if (pEntryNew == NULL) {
      code = terrno;
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
      TAOS_UNUSED(metaRehashStatsCache(pCache, 0));
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
    if (pInfo) {
      *pInfo = pEntry->info;
    }
  } else {
    code = TSDB_CODE_NOT_FOUND;
  }

  return code;
}

static FORCE_INLINE void setMD5DigestInKey(uint64_t* pBuf, const char* key, int32_t keyLen) {
  memcpy(&pBuf[2], key, keyLen);
}

// the format of key:
// hash table address(8bytes) + suid(8bytes) + MD5 digest(16bytes)
static void initCacheKey(uint64_t* buf, const SHashObj* pHashMap, uint64_t suid, const char* key, int32_t keyLen) {
  buf[0] = (uint64_t)pHashMap;
  buf[1] = suid;
  setMD5DigestInKey(buf, key, keyLen);
}

int32_t metaGetCachedTableUidList(void* pVnode, tb_uid_t suid, const uint8_t* pKey, int32_t keyLen, SArray* pList1,
                                  bool* acquireRes) {
  SMeta*  pMeta = ((SVnode*)pVnode)->pMeta;
  int32_t vgId = TD_VID(pMeta->pVnode);

  // generate the composed key for LRU cache
  SLRUCache*     pCache = pMeta->pCache->sTagFilterResCache.pUidResCache;
  SHashObj*      pTableMap = pMeta->pCache->sTagFilterResCache.pTableEntry;
  TdThreadMutex* pLock = &pMeta->pCache->sTagFilterResCache.lock;

  *acquireRes = 0;
  uint64_t key[4];
  initCacheKey(key, pTableMap, suid, (const char*)pKey, keyLen);

  (void)taosThreadMutexLock(pLock);
  pMeta->pCache->sTagFilterResCache.accTimes += 1;

  LRUHandle* pHandle = taosLRUCacheLookup(pCache, key, TAG_FILTER_RES_KEY_LEN);
  if (pHandle == NULL) {
    (void)taosThreadMutexUnlock(pLock);
    return TSDB_CODE_SUCCESS;
  }

  // do some book mark work after acquiring the filter result from cache
  STagFilterResEntry** pEntry = taosHashGet(pTableMap, &suid, sizeof(uint64_t));
  if (NULL == pEntry) {
    metaError("meta/cache: pEntry should not be NULL.");
    return TSDB_CODE_NOT_FOUND;
  }

  *acquireRes = 1;

  const char* p = taosLRUCacheValue(pCache, pHandle);
  int32_t     size = *(int32_t*)p;

  // set the result into the buffer
  if (taosArrayAddBatch(pList1, p + sizeof(int32_t), size) == NULL) {
    return terrno;
  }

  (*pEntry)->hitTimes += 1;

  uint32_t acc = pMeta->pCache->sTagFilterResCache.accTimes;
  if ((*pEntry)->hitTimes % 5000 == 0 && (*pEntry)->hitTimes > 0) {
    metaInfo("vgId:%d cache hit:%d, total acc:%d, rate:%.2f", vgId, (*pEntry)->hitTimes, acc,
             ((double)(*pEntry)->hitTimes) / acc);
  }

  bool ret = taosLRUCacheRelease(pCache, pHandle, false);

  // unlock meta
  (void)taosThreadMutexUnlock(pLock);
  return TSDB_CODE_SUCCESS;
}

static void freeUidCachePayload(const void* key, size_t keyLen, void* value, void* ud) {
  (void)ud;
  if (value == NULL) {
    return;
  }

  const uint64_t* p = key;
  if (keyLen != sizeof(int64_t) * 4) {
    metaError("key length is invalid, length:%d, expect:%d", (int32_t)keyLen, (int32_t)sizeof(uint64_t) * 2);
    return;
  }

  SHashObj* pHashObj = (SHashObj*)p[0];

  STagFilterResEntry** pEntry = taosHashGet(pHashObj, &p[1], sizeof(uint64_t));

  if (pEntry != NULL && (*pEntry) != NULL) {
    int64_t st = taosGetTimestampUs();
    int32_t code = taosHashRemove((*pEntry)->set, &p[2], sizeof(uint64_t) * 2);
    if (code == TSDB_CODE_SUCCESS) {
      double el = (taosGetTimestampUs() - st) / 1000.0;
      metaInfo("clear items in meta-cache, remain cached item:%d, elapsed time:%.2fms", taosHashGetSize((*pEntry)->set),
               el);
    }
  }

  taosMemoryFree(value);
}

static int32_t addNewEntry(SHashObj* pTableEntry, const void* pKey, int32_t keyLen, uint64_t suid) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  STagFilterResEntry* p = taosMemoryMalloc(sizeof(STagFilterResEntry));
  TSDB_CHECK_NULL(p, code, lino, _end, terrno);

  p->hitTimes = 0;
  p->set = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  TSDB_CHECK_NULL(p->set, code, lino, _end, terrno);
  code = taosHashPut(p->set, pKey, keyLen, NULL, 0);
  TSDB_CHECK_CODE(code, lino, _end);
  code = taosHashPut(pTableEntry, &suid, sizeof(uint64_t), &p, POINTER_BYTES);
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    metaError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    if (p != NULL) {
      if (p->set != NULL) {
        taosHashCleanup(p->set);
      }
      taosMemoryFree(p);
    }
  }
  return code;
}

// check both the payload size and selectivity ratio
int32_t metaUidFilterCachePut(void* pVnode, uint64_t suid, const void* pKey, int32_t keyLen, void* pPayload,
                              int32_t payloadLen, double selectivityRatio) {
  int32_t code = 0;
  SMeta*  pMeta = ((SVnode*)pVnode)->pMeta;
  int32_t vgId = TD_VID(pMeta->pVnode);

  if (selectivityRatio > tsSelectivityRatio) {
    metaDebug("vgId:%d, suid:%" PRIu64
              " failed to add to uid list cache, due to selectivity ratio %.2f less than threshold %.2f",
              vgId, suid, selectivityRatio, tsSelectivityRatio);
    taosMemoryFree(pPayload);
    return TSDB_CODE_SUCCESS;
  }

  if (payloadLen > tsTagFilterResCacheSize) {
    metaDebug("vgId:%d, suid:%" PRIu64
              " failed to add to uid list cache, due to payload length %d greater than threshold %d",
              vgId, suid, payloadLen, tsTagFilterResCacheSize);
    taosMemoryFree(pPayload);
    return TSDB_CODE_SUCCESS;
  }

  SLRUCache*     pCache = pMeta->pCache->sTagFilterResCache.pUidResCache;
  SHashObj*      pTableEntry = pMeta->pCache->sTagFilterResCache.pTableEntry;
  TdThreadMutex* pLock = &pMeta->pCache->sTagFilterResCache.lock;

  uint64_t key[4] = {0};
  initCacheKey(key, pTableEntry, suid, pKey, keyLen);

  (void)taosThreadMutexLock(pLock);
  STagFilterResEntry** pEntry = taosHashGet(pTableEntry, &suid, sizeof(uint64_t));
  if (pEntry == NULL) {
    code = addNewEntry(pTableEntry, pKey, keyLen, suid);
    if (code != TSDB_CODE_SUCCESS) {
      goto _end;
    }
  } else {  // check if it exists or not
    code = taosHashPut((*pEntry)->set, pKey, keyLen, NULL, 0);
    if (code == TSDB_CODE_DUP_KEY) {
      // we have already found the existed items, no need to added to cache anymore.
      (void)taosThreadMutexUnlock(pLock);
      return TSDB_CODE_SUCCESS;
    }
    if (code != TSDB_CODE_SUCCESS) {
      goto _end;
    }
  }

  // add to cache.
  (void)taosLRUCacheInsert(pCache, key, TAG_FILTER_RES_KEY_LEN, pPayload, payloadLen, freeUidCachePayload, NULL, NULL,
                           TAOS_LRU_PRIORITY_LOW, NULL);
_end:
  (void)taosThreadMutexUnlock(pLock);
  metaDebug("vgId:%d, suid:%" PRIu64 " list cache added into cache, total:%d, tables:%d", vgId, suid,
            (int32_t)taosLRUCacheGetUsage(pCache), taosHashGetSize(pTableEntry));

  return code;
}

void metaCacheClear(SMeta* pMeta) {
  metaWLock(pMeta);
  metaCacheClose(pMeta);
  metaCacheOpen(pMeta);
  metaULock(pMeta);
}

// remove the lru cache that are expired due to the tags value update, or creating, or dropping, of child tables
int32_t metaUidCacheClear(SMeta* pMeta, uint64_t suid) {
  uint64_t  p[4] = {0};
  int32_t   vgId = TD_VID(pMeta->pVnode);
  SHashObj* pEntryHashMap = pMeta->pCache->sTagFilterResCache.pTableEntry;

  uint64_t dummy[2] = {0};
  initCacheKey(p, pEntryHashMap, suid, (char*)&dummy[0], 16);

  TdThreadMutex* pLock = &pMeta->pCache->sTagFilterResCache.lock;
  (void)taosThreadMutexLock(pLock);

  STagFilterResEntry** pEntry = taosHashGet(pEntryHashMap, &suid, sizeof(uint64_t));
  if (pEntry == NULL || taosHashGetSize((*pEntry)->set) == 0) {
    (void)taosThreadMutexUnlock(pLock);
    return TSDB_CODE_SUCCESS;
  }

  (*pEntry)->hitTimes = 0;

  char *iter = taosHashIterate((*pEntry)->set, NULL);
  while (iter != NULL) {
    setMD5DigestInKey(p, iter, 2 * sizeof(uint64_t));
    taosLRUCacheErase(pMeta->pCache->sTagFilterResCache.pUidResCache, p, TAG_FILTER_RES_KEY_LEN);
    iter = taosHashIterate((*pEntry)->set, iter);
  }
  taosHashClear((*pEntry)->set);
  (void)taosThreadMutexUnlock(pLock);

  metaDebug("vgId:%d suid:%" PRId64 " cached related tag filter uid list cleared", vgId, suid);
  return TSDB_CODE_SUCCESS;
}

int32_t metaGetCachedTbGroup(void* pVnode, tb_uid_t suid, const uint8_t* pKey, int32_t keyLen, SArray** pList) {
  SMeta*  pMeta = ((SVnode*)pVnode)->pMeta;
  int32_t vgId = TD_VID(pMeta->pVnode);

  // generate the composed key for LRU cache
  SLRUCache*     pCache = pMeta->pCache->STbGroupResCache.pResCache;
  SHashObj*      pTableMap = pMeta->pCache->STbGroupResCache.pTableEntry;
  TdThreadMutex* pLock = &pMeta->pCache->STbGroupResCache.lock;

  *pList = NULL;
  uint64_t key[4];
  initCacheKey(key, pTableMap, suid, (const char*)pKey, keyLen);

  (void)taosThreadMutexLock(pLock);
  pMeta->pCache->STbGroupResCache.accTimes += 1;

  LRUHandle* pHandle = taosLRUCacheLookup(pCache, key, TAG_FILTER_RES_KEY_LEN);
  if (pHandle == NULL) {
    (void)taosThreadMutexUnlock(pLock);
    return TSDB_CODE_SUCCESS;
  }

  STagFilterResEntry** pEntry = taosHashGet(pTableMap, &suid, sizeof(uint64_t));
  if (NULL == pEntry) {
    metaDebug("suid %" PRIu64 " not in tb group cache", suid);
    return TSDB_CODE_NOT_FOUND;
  }

  *pList = taosArrayDup(taosLRUCacheValue(pCache, pHandle), NULL);

  (*pEntry)->hitTimes += 1;

  uint32_t acc = pMeta->pCache->STbGroupResCache.accTimes;
  if ((*pEntry)->hitTimes % 5000 == 0 && (*pEntry)->hitTimes > 0) {
    metaInfo("vgId:%d tb group cache hit:%d, total acc:%d, rate:%.2f", vgId, (*pEntry)->hitTimes, acc,
             ((double)(*pEntry)->hitTimes) / acc);
  }

  bool ret = taosLRUCacheRelease(pCache, pHandle, false);

  // unlock meta
  (void)taosThreadMutexUnlock(pLock);
  return TSDB_CODE_SUCCESS;
}

static void freeTbGroupCachePayload(const void* key, size_t keyLen, void* value, void* ud) {
  (void)ud;
  if (value == NULL) {
    return;
  }

  const uint64_t* p = key;
  if (keyLen != sizeof(int64_t) * 4) {
    metaError("tb group key length is invalid, length:%d, expect:%d", (int32_t)keyLen, (int32_t)sizeof(uint64_t) * 2);
    return;
  }

  SHashObj* pHashObj = (SHashObj*)p[0];

  STagFilterResEntry** pEntry = taosHashGet(pHashObj, &p[1], sizeof(uint64_t));

  if (pEntry != NULL && (*pEntry) != NULL) {
    int64_t st = taosGetTimestampUs();
    int32_t code = taosHashRemove((*pEntry)->set, &p[2], sizeof(uint64_t) * 2);
    if (code == TSDB_CODE_SUCCESS) {
      double el = (taosGetTimestampUs() - st) / 1000.0;
      metaDebug("clear one item in tb group cache, remain cached item:%d, elapsed time:%.2fms",
                taosHashGetSize((*pEntry)->set), el);
    }
  }

  taosArrayDestroy((SArray*)value);
}

int32_t metaPutTbGroupToCache(void* pVnode, uint64_t suid, const void* pKey, int32_t keyLen, void* pPayload,
                              int32_t payloadLen) {
  int32_t code = 0;
  SMeta*  pMeta = ((SVnode*)pVnode)->pMeta;
  int32_t vgId = TD_VID(pMeta->pVnode);

  if (payloadLen > tsTagFilterResCacheSize) {
    metaDebug("vgId:%d, suid:%" PRIu64
              " ignore to add to tb group cache, due to payload length %d greater than threshold %d",
              vgId, suid, payloadLen, tsTagFilterResCacheSize);
    taosArrayDestroy((SArray*)pPayload);
    return TSDB_CODE_SUCCESS;
  }

  SLRUCache*     pCache = pMeta->pCache->STbGroupResCache.pResCache;
  SHashObj*      pTableEntry = pMeta->pCache->STbGroupResCache.pTableEntry;
  TdThreadMutex* pLock = &pMeta->pCache->STbGroupResCache.lock;

  uint64_t key[4] = {0};
  initCacheKey(key, pTableEntry, suid, pKey, keyLen);

  (void)taosThreadMutexLock(pLock);
  STagFilterResEntry** pEntry = taosHashGet(pTableEntry, &suid, sizeof(uint64_t));
  if (pEntry == NULL) {
    code = addNewEntry(pTableEntry, pKey, keyLen, suid);
    if (code != TSDB_CODE_SUCCESS) {
      goto _end;
    }
  } else {  // check if it exists or not
    code = taosHashPut((*pEntry)->set, pKey, keyLen, NULL, 0);
    if (code == TSDB_CODE_DUP_KEY) {
      // we have already found the existed items, no need to added to cache anymore.
      (void)taosThreadMutexUnlock(pLock);
      return TSDB_CODE_SUCCESS;
    }
    if (code != TSDB_CODE_SUCCESS) {
      goto _end;
    }
  }

  // add to cache.
  (void)taosLRUCacheInsert(pCache, key, TAG_FILTER_RES_KEY_LEN, pPayload, payloadLen, freeTbGroupCachePayload, NULL, NULL,
                           TAOS_LRU_PRIORITY_LOW, NULL);
_end:
  (void)taosThreadMutexUnlock(pLock);
  metaDebug("vgId:%d, suid:%" PRIu64 " tb group added into cache, total:%d, tables:%d", vgId, suid,
            (int32_t)taosLRUCacheGetUsage(pCache), taosHashGetSize(pTableEntry));

  return code;
}

// remove the lru cache that are expired due to the tags value update, or creating, or dropping, of child tables
int32_t metaTbGroupCacheClear(SMeta* pMeta, uint64_t suid) {
  uint64_t  p[4] = {0};
  int32_t   vgId = TD_VID(pMeta->pVnode);
  SHashObj* pEntryHashMap = pMeta->pCache->STbGroupResCache.pTableEntry;

  uint64_t dummy[2] = {0};
  initCacheKey(p, pEntryHashMap, suid, (char*)&dummy[0], 16);

  TdThreadMutex* pLock = &pMeta->pCache->STbGroupResCache.lock;
  (void)taosThreadMutexLock(pLock);

  STagFilterResEntry** pEntry = taosHashGet(pEntryHashMap, &suid, sizeof(uint64_t));
  if (pEntry == NULL || taosHashGetSize((*pEntry)->set) == 0) {
    (void)taosThreadMutexUnlock(pLock);
    return TSDB_CODE_SUCCESS;
  }

  (*pEntry)->hitTimes = 0;

  char *iter = taosHashIterate((*pEntry)->set, NULL);
  while (iter != NULL) {
    setMD5DigestInKey(p, iter, 2 * sizeof(uint64_t));
    taosLRUCacheErase(pMeta->pCache->STbGroupResCache.pResCache, p, TAG_FILTER_RES_KEY_LEN);
    iter = taosHashIterate((*pEntry)->set, iter);
  }
  taosHashClear((*pEntry)->set);
  (void)taosThreadMutexUnlock(pLock);

  metaDebug("vgId:%d suid:%" PRId64 " cached related tb group cleared", vgId, suid);
  return TSDB_CODE_SUCCESS;
}

bool metaTbInFilterCache(SMeta* pMeta, const void* key, int8_t type) {
  if (type == 0 && taosHashGet(pMeta->pCache->STbFilterCache.pStb, key, sizeof(tb_uid_t))) {
    return true;
  }

  if (type == 1 && taosHashGet(pMeta->pCache->STbFilterCache.pStbName, key, strlen(key))) {
    return true;
  }

  return false;
}

int32_t metaPutTbToFilterCache(SMeta* pMeta, const void* key, int8_t type) {
  if (type == 0) {
    return taosHashPut(pMeta->pCache->STbFilterCache.pStb, key, sizeof(tb_uid_t), NULL, 0);
  }

  if (type == 1) {
    return taosHashPut(pMeta->pCache->STbFilterCache.pStbName, key, strlen(key), NULL, 0);
  }

  return 0;
}

int32_t metaSizeOfTbFilterCache(SMeta* pMeta, int8_t type) {
  if (type == 0) {
    return taosHashGetSize(pMeta->pCache->STbFilterCache.pStb);
  }
  return 0;
}

int32_t metaInitTbFilterCache(SMeta* pMeta) {
#ifdef TD_ENTERPRISE
  int32_t      tbNum = 0;
  const char** pTbArr = NULL;
  const char*  dbName = NULL;

  if (!(dbName = strchr(pMeta->pVnode->config.dbname, '.'))) return 0;
  if (0 == strncmp(++dbName, "log", TSDB_DB_NAME_LEN)) {
    tbNum = tkLogStbNum;
    pTbArr = (const char**)&tkLogStb;
  } else if (0 == strncmp(dbName, "audit", TSDB_DB_NAME_LEN)) {
    tbNum = tkAuditStbNum;
    pTbArr = (const char**)&tkAuditStb;
  }
  if (tbNum && pTbArr) {
    for (int32_t i = 0; i < tbNum; ++i) {
      TAOS_CHECK_RETURN(metaPutTbToFilterCache(pMeta, pTbArr[i], 1));
    }
  }
#else
#endif
  return 0;
}

int64_t metaGetStbKeep(SMeta* pMeta, int64_t uid) {
  SMetaStbStats stats = {0};

  if (metaStatsCacheGet(pMeta, uid, &stats) == TSDB_CODE_SUCCESS) {
    return stats.keep;
  }

  SMetaEntry* pEntry = NULL;
  if (metaFetchEntryByUid(pMeta, uid, &pEntry) == TSDB_CODE_SUCCESS) {
    int64_t keep = -1;
    if (pEntry->type == TSDB_SUPER_TABLE) {
      keep = pEntry->stbEntry.keep;
    }
    metaFetchEntryFree(&pEntry);
    return keep;
  }
  
  return -1;
}

int32_t metaRefDbsCacheClear(SMeta* pMeta, uint64_t suid) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        vgId = TD_VID(pMeta->pVnode);
  SHashObj*      pEntryHashMap = pMeta->pCache->STbRefDbCache.pStbRefs;
  TdThreadMutex* pLock = &pMeta->pCache->STbRefDbCache.lock;

  (void)taosThreadMutexLock(pLock);

  SHashObj** pEntry = taosHashGet(pEntryHashMap, &suid, sizeof(uint64_t));
  if (pEntry == NULL) {
    goto _return;
  }

  taosHashRemove(pEntryHashMap, &suid, sizeof(uint64_t));

  metaDebug("vgId:%d suid:%" PRId64 " cached virtual stable ref db cleared", vgId, suid);

_return:
  (void)taosThreadMutexUnlock(pLock);
  return code;
}

int32_t metaGetCachedRefDbs(void* pVnode, tb_uid_t suid, SArray* pList) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        line = 0;
  SMeta*         pMeta = ((SVnode*)pVnode)->pMeta;
  SHashObj*      pTableMap = pMeta->pCache->STbRefDbCache.pStbRefs;
  TdThreadMutex* pLock = &pMeta->pCache->STbRefDbCache.lock;

  (void)taosThreadMutexLock(pLock);

  SHashObj** pEntry = taosHashGet(pTableMap, &suid, sizeof(uint64_t));
  if (pEntry) {
    void *iter = taosHashIterate(*pEntry, NULL);
    while (iter != NULL) {
      size_t   dbNameLen = 0;
      char*    name = NULL;
      char*    dbName = NULL;
      name = taosHashGetKey(iter, &dbNameLen);
      TSDB_CHECK_NULL(name, code, line, _return, terrno);
      dbName = taosMemoryMalloc(dbNameLen + 1);
      TSDB_CHECK_NULL(dbName, code, line, _return, terrno);
      tstrncpy(dbName, name, dbNameLen + 1);
      TSDB_CHECK_NULL(taosArrayPush(pList, &dbName), code, line, _return, terrno);
      iter = taosHashIterate(*pEntry, iter);
    }
  }

_return:
  if (code) {
    metaError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  }
  (void)taosThreadMutexUnlock(pLock);
  return code;
}

static int32_t addRefDbsCacheNewEntry(SHashObj* pRefDbs, uint64_t suid, SHashObj **pEntry) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SHashObj*    p = NULL;

  p = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  TSDB_CHECK_NULL(p, code, lino, _end, terrno);

  code = taosHashPut(pRefDbs, &suid, sizeof(uint64_t), &p, POINTER_BYTES);
  TSDB_CHECK_CODE(code, lino, _end);

  *pEntry = p;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    metaError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t metaPutRefDbsToCache(void* pVnode, tb_uid_t suid, SArray* pList) {
  int32_t        code = 0;
  int32_t        line = 0;
  SMeta*         pMeta = ((SVnode*)pVnode)->pMeta;
  SHashObj*      pStbRefs = pMeta->pCache->STbRefDbCache.pStbRefs;
  TdThreadMutex* pLock = &pMeta->pCache->STbRefDbCache.lock;

  (void)taosThreadMutexLock(pLock);

  SHashObj*  pEntry = NULL;
  SHashObj** find = taosHashGet(pStbRefs, &suid, sizeof(uint64_t));
  if (find == NULL) {
    code = addRefDbsCacheNewEntry(pStbRefs, suid, &pEntry);
    TSDB_CHECK_CODE(code, line, _return);
  } else {  // check if it exists or not
    pEntry = *find;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pList); i++) {
    char* dbName = taosArrayGetP(pList, i);
    void* pItem = taosHashGet(pEntry, dbName, strlen(dbName));
    if (pItem == NULL) {
      code = taosHashPut(pEntry, dbName, strlen(dbName), NULL, 0);
      TSDB_CHECK_CODE(code, line, _return);
    }
  }

_return:
  if (code) {
    metaError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  }
  (void)taosThreadMutexUnlock(pLock);

  return code;
}