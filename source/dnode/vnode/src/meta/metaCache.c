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

#define META_CACHE_BASE_BUCKET 1024

// (uid , suid) : child table
// (uid,     0) : normal table
// (suid, suid) : super table
typedef struct SMetaCacheEntry SMetaCacheEntry;
struct SMetaCacheEntry {
  SMetaCacheEntry* next;
  SEntryInfo       info;
};

struct SMetaCache {
  int32_t           nEntry;
  int32_t           nBucket;
  SMetaCacheEntry** aBucket;
};

int32_t metaCacheOpen(SMeta* pMeta) {
  int32_t     code = 0;
  SMetaCache* pCache = NULL;

  pCache = (SMetaCache*)taosMemoryMalloc(sizeof(SMetaCache));
  if (pCache == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

  pCache->nEntry = 0;
  pCache->nBucket = META_CACHE_BASE_BUCKET;
  pCache->aBucket = (SMetaCacheEntry**)taosMemoryCalloc(pCache->nBucket, sizeof(SMetaCacheEntry*));
  if (pCache->aBucket == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(pCache);
    goto _err;
  }

  pMeta->pCache = pCache;

  // load the cache info
  TBC* pUidIdxC = NULL;
  if (tdbTbcOpen(pMeta->pUidIdx, &pUidIdxC, NULL) < 0) {
    goto _err;
  }

  void* pKey = NULL;
  void* pData = NULL;
  int   nKey;
  int   nData;

  if (tdbTbcMoveToFirst(pUidIdxC)) {
    goto _exit;
  }

  while (tdbTbcNext(pUidIdxC, &pKey, &nKey, &pData, &nData) == 0) {
    SUidIdxVal* pUidIdxVal = (SUidIdxVal*)pData;
    SEntryInfo  info = {.uid = *(tb_uid_t*)pKey,
                        .suid = pUidIdxVal->suid,
                        .skmVer = pUidIdxVal->skmVer,
                        .version = pUidIdxVal->version};
    code = metaCacheUpsert(pMeta, &info);
    if (code) goto _err;
  }

  tdbFree(pKey);
  tdbFree(pData);
  tdbTbcClose(pUidIdxC);

_exit:
  return code;

_err:
  metaError("vgId:%d meta open cache failed since %s", TD_VID(pMeta->pVnode), tstrerror(code));
  return code;
}

void metaCacheClose(SMeta* pMeta) {
  if (pMeta->pCache) {
    for (int32_t iBucket = 0; iBucket < pMeta->pCache->nBucket; iBucket++) {
      SMetaCacheEntry* pEntry = pMeta->pCache->aBucket[iBucket];
      while (pEntry) {
        SMetaCacheEntry* tEntry = pEntry->next;
        taosMemoryFree(pEntry);
        pEntry = tEntry;
      }
    }
    taosMemoryFree(pMeta->pCache->aBucket);
    taosMemoryFree(pMeta->pCache);
    pMeta->pCache = NULL;
  }
}

static int32_t metaRehashCache(SMetaCache* pCache, int8_t expand) {
  int32_t code = 0;
  int32_t nBucket;

  if (expand) {
    nBucket = pCache->nBucket * 2;
  } else {
    nBucket = pCache->nBucket / 2;
  }

  SMetaCacheEntry** aBucket = (SMetaCacheEntry**)taosMemoryCalloc(nBucket, sizeof(SMetaCacheEntry*));
  if (aBucket == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  // rehash
  for (int32_t iBucket = 0; iBucket < pCache->nBucket; iBucket++) {
    SMetaCacheEntry* pEntry = pCache->aBucket[iBucket];

    while (pEntry) {
      SMetaCacheEntry* pTEntry = pEntry->next;

      pEntry->next = aBucket[TABS(pEntry->info.uid) % nBucket];
      aBucket[TABS(pEntry->info.uid) % nBucket] = pEntry;

      pEntry = pTEntry;
    }
  }

  // final set
  taosMemoryFree(pCache->aBucket);
  pCache->nBucket = nBucket;
  pCache->aBucket = aBucket;

_exit:
  return code;
}

int32_t metaCacheUpsert(SMeta* pMeta, SEntryInfo* pInfo) {
  int32_t code = 0;

  // ASSERT(metaIsWLocked(pMeta));

  // search
  SMetaCache*       pCache = pMeta->pCache;
  int32_t           iBucket = TABS(pInfo->uid) % pCache->nBucket;
  SMetaCacheEntry** ppEntry = &pCache->aBucket[iBucket];
  while (*ppEntry && (*ppEntry)->info.uid != pInfo->uid) {
    ppEntry = &(*ppEntry)->next;
  }

  if (*ppEntry) {  // update
    ASSERT(pInfo->suid == (*ppEntry)->info.suid);
    (*ppEntry)->info.version = pInfo->version;
    (*ppEntry)->info.skmVer = pInfo->skmVer;
  } else {  // insert
    if (pCache->nEntry >= pCache->nBucket) {
      code = metaRehashCache(pCache, 1);
      if (code) goto _exit;

      iBucket = TABS(pInfo->uid) % pCache->nBucket;
    }

    SMetaCacheEntry* pEntryNew = (SMetaCacheEntry*)taosMemoryMalloc(sizeof(*pEntryNew));
    if (pEntryNew == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }

    pEntryNew->info = *pInfo;
    pEntryNew->next = pCache->aBucket[iBucket];
    pCache->aBucket[iBucket] = pEntryNew;

    pCache->nEntry++;
  }

_exit:
  return code;
}

int32_t metaCacheDrop(SMeta* pMeta, int64_t uid) {
  int32_t code = 0;

  // ASSERT(metaIsWLocked(pMeta));

  SMetaCache*       pCache = pMeta->pCache;
  int32_t           iBucket = TABS(uid) % pCache->nBucket;
  SMetaCacheEntry** ppEntry = &pCache->aBucket[iBucket];
  while (*ppEntry && (*ppEntry)->info.uid != uid) {
    ppEntry = &(*ppEntry)->next;
  }

  SMetaCacheEntry* pEntry = *ppEntry;
  if (pEntry) {
    *ppEntry = pEntry->next;
    taosMemoryFree(pEntry);
    pCache->nEntry--;
    if (pCache->nEntry < pCache->nBucket / 4 && pCache->nBucket > META_CACHE_BASE_BUCKET) {
      code = metaRehashCache(pCache, 0);
      if (code) goto _exit;
    }
  } else {
    code = TSDB_CODE_NOT_FOUND;
  }

_exit:
  return code;
}

int32_t metaCacheGetImpl(SMeta* pMeta, int64_t uid, SEntryInfo* pInfo) {
  int32_t code = 0;

  SMetaCache*       pCache = pMeta->pCache;
  int32_t           iBucket = TABS(uid) % pCache->nBucket;
  SMetaCacheEntry** ppEntry = &pCache->aBucket[iBucket];
  while (*ppEntry && (*ppEntry)->info.uid != uid) {
    ppEntry = &(*ppEntry)->next;
  }

  if (*ppEntry) {
    *pInfo = (*ppEntry)->info;
  } else {
    code = TSDB_CODE_NOT_FOUND;
  }

  return code;
}

int32_t metaCacheGet(SMeta* pMeta, int64_t uid, SEntryInfo* pInfo) {
  int32_t code = 0;

  metaRLock(pMeta);
  code = metaCacheGetImpl(pMeta, uid, pInfo);
  metaULock(pMeta);

  return code;
}
