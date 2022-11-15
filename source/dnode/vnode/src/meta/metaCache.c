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
  uint64_t suid;  // uid for super table
  SList*   pList; // the linked list of md5 digest, extracted from the serialized tag query condition
  uint32_t qTimes;// queried times for current super table
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

  pMeta->pCache = pCache;

_exit:
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

int32_t metaUidFilterCacheGet(SMeta* pMeta, uint64_t suid, const void* pKey, int32_t keyLen, LRUHandle** pHandle) {
  // generate the composed key for LRU cache
  char* p = taosMemoryMalloc(keyLen + sizeof(uint64_t));
  *(uint64_t*) p = suid;
  memcpy(p + sizeof(suid), pKey, keyLen);

  int32_t len = keyLen + sizeof(uint64_t);
  *pHandle = taosLRUCacheLookup(pMeta->pCache->sTagFilterResCache.pUidResCache, p, len);
  if (*pHandle == NULL) {
    taosMemoryFree(p);
    return TSDB_CODE_SUCCESS;
  } else {  // do some book mark work after acquiring the filter result from cache
    STagFilterResEntry* pEntry = taosHashGet(pMeta->pCache->sTagFilterResCache.pTableEntry, &suid, sizeof(uint64_t));
    ASSERT(pEntry != NULL);

    pEntry->qTimes += 1;

    // check if scanning all items are necessary or not
    if (pEntry->qTimes > 5000 && TD_DLIST_NELES(pEntry->pList) > 10) {
      SArray* pList = taosArrayInit(64, POINTER_BYTES);

      SListIter iter = {0};
      tdListInitIter(pEntry->pList, &iter, TD_LIST_FORWARD);

      SListNode* pNode = NULL;
      while ((pNode = tdListNext(&iter)) != NULL) {
        memcpy(p + sizeof(suid), pNode->data, keyLen);

        // check whether it is existed in LRU cache, and remove it from linked list if not.
        void* pRes = taosLRUCacheLookup(pMeta->pCache->sTagFilterResCache.pUidResCache, p, len);
        if (pRes == NULL) {  // remove the item in the linked list
          taosArrayPush(pList, &pNode);
        }
      }

      // remove the keys, of which query uid lists have been replaced already.
      size_t s = taosArrayGetSize(pList);
      for(int32_t i = 0; i < s; ++i) {
        SListNode** p1 = taosArrayGet(pList, i);
        tdListPopNode(pEntry->pList, *p1);
      }
    }

    taosMemoryFree(p);
  }

  return TSDB_CODE_SUCCESS;
}

// check both the payload size and selectivity ratio
int32_t metaUidFilterCachePut(SMeta* pMeta, uint64_t suid, const void* pKey, int32_t keyLen, void* pPayload) {

  return TSDB_CODE_SUCCESS;
}

// remove the lru cache that are expired due to the tags value update, or creating, or dropping, of child tables
int32_t metaUidCacheClear(SMeta* pMeta, uint64_t suid) {
  STagFilterResEntry* pEntry = taosHashGet(pMeta->pCache->sTagFilterResCache.pTableEntry, &suid, sizeof(uint64_t));
  if (pEntry == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t keyLen = sizeof(uint64_t) + 128;
  char* p = taosMemoryMalloc(keyLen);
  *(uint64_t*)p = pEntry->suid;

  SListIter iter = {0};
  tdListInitIter(pEntry->pList, &iter, TD_LIST_FORWARD);

  SListNode* pNode = NULL;
  while ((pNode = tdListNext(&iter)) != NULL) {
    memcpy(p + sizeof(suid), pNode->data, 128);
    taosLRUCacheErase(pMeta->pCache->sTagFilterResCache.pUidResCache, p, keyLen);
  }

  return TSDB_CODE_SUCCESS;
}

