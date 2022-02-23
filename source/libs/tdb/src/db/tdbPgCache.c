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
#include "tdbInt.h"

struct SPCache {
  int             pageSize;
  int             cacheSize;
  int             extraSize;
  int             nRef;
  pthread_mutex_t mutex;
  int             nPage;
  SPgHdr *        lru;
  int             nRecyclable;
  int             nHash;
  SPgHdr *        pgHash;
  int             nFree;
  SPgHdr *        pFree;
};

struct SPgHdr {
  void *  pData;
  SPgid   pgid;
  SPgHdr *pFreeNext;
};

static void tdbPCacheLock(SPCache *pCache);
static void tdbPCacheUnlock(SPCache *pCache);
static bool tdbPCacheLocked(SPCache *pCache);

int tdbOpenPCache(int pageSize, int cacheSize, int extraSize, SPCache **ppCache) {
  SPCache *pCache;
  void *   pPtr;
  SPgHdr * pPgHdr;

  pCache = (SPCache *)calloc(1, sizeof(*pCache));
  if (pCache == NULL) {
    return -1;
  }

  pCache->pageSize = pageSize;
  pCache->cacheSize = cacheSize;
  pCache->extraSize = extraSize;

  pthread_mutex_init(&pCache->mutex, NULL);

  for (int i = 0; i < cacheSize; i++) {
    pPtr = calloc(1, pageSize + extraSize + sizeof(SPgHdr));
    if (pPtr == NULL) {
      return -1;
    }

    pPgHdr = (SPgHdr *)&((char *)pPtr)[pageSize + extraSize];
    pPgHdr->pFreeNext = pCache->pFree;
    pCache->pFree = pPgHdr;

    pCache->nFree++;
  }

  return 0;
}

int tdbClosePCache(SPCache *pCache) {
  /* TODO */
  return 0;
}

void *tdbPCacheFetch(SPCache *pCache, SPgid *pPgid) {
  tdbPCacheLock(pCache);
  // 1. search the hash table
  tdbPCacheUnlock(pCache);
  return NULL;
}

void tdbPCacheRelease(void *pHdr) {
  // TODO
}

static void tdbPCacheLock(SPCache *pCache) { pthread_mutex_lock(&(pCache->mutex)); }

static void tdbPCacheUnlock(SPCache *pCache) { pthread_mutex_unlock(&(pCache->mutex)); }

static bool tdbPCacheLocked(SPCache *pCache) {
  assert(0);
  // TODO
  return true;
}