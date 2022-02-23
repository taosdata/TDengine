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
  SPgHdr          lru;
  int             nRecyclable;
  int             nHash;
  SPgHdr **       pgHash;
  int             nFree;
  SPgHdr *        pFree;
};

#define PCACHE_PAGE_HASH(pgid) 0  // TODO
#define PAGE_IS_PINNED(pPage)  ((pPage)->pLruNext == NULL)

static void    tdbPCacheInitLock(SPCache *pCache);
static void    tdbPCacheClearLock(SPCache *pCache);
static void    tdbPCacheLock(SPCache *pCache);
static void    tdbPCacheUnlock(SPCache *pCache);
static bool    tdbPCacheLocked(SPCache *pCache);
static SPgHdr *tdbPCacheFetchImpl(SPCache *pCache, const SPgid *pPgid, bool alcNewPage);
static void    tdbPCachePinPage(SPgHdr *pPage);
static void    tdbPCacheRemovePageFromHash(SPgHdr *pPage);
static void    tdbPCacheAddPageToHash(SPgHdr *pPage);

int tdbPCacheOpen(int pageSize, int cacheSize, int extraSize, SPCache **ppCache) {
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

  tdbPCacheInitLock(pCache);

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

int tdbPCacheClose(SPCache *pCache) {
  /* TODO */
  return 0;
}

SPgHdr *tdbPCacheFetch(SPCache *pCache, const SPgid *pPgid, bool alcNewPage) {
  SPgHdr *pPage;

  tdbPCacheLock(pCache);
  pPage = tdbPCacheFetchImpl(pCache, pPgid, alcNewPage);
  tdbPCacheUnlock(pCache);

  return pPage;
}

void tdbPCacheFetchFinish(SPCache *pCache, SPgHdr *pPage) {
  // TODO
}

void tdbPCacheRelease(SPgHdr *pHdr) {
  // TODO
}

static void tdbPCacheInitLock(SPCache *pCache) { pthread_mutex_init(&(pCache->mutex), NULL); }

static void tdbPCacheClearLock(SPCache *pCache) { pthread_mutex_destroy(&(pCache->mutex)); }

static void tdbPCacheLock(SPCache *pCache) { pthread_mutex_lock(&(pCache->mutex)); }

static void tdbPCacheUnlock(SPCache *pCache) { pthread_mutex_unlock(&(pCache->mutex)); }

static bool tdbPCacheLocked(SPCache *pCache) {
  assert(0);
  // TODO
  return true;
}

static SPgHdr *tdbPCacheFetchImpl(SPCache *pCache, const SPgid *pPgid, bool alcNewPage) {
  SPgHdr *pPage;

  // 1. Search the hash table
  pPage = pCache->pgHash[PCACHE_PAGE_HASH(pPgid) % pCache->nHash];
  while (pPage) {
    if (memcmp(pPgid, &(pPage->pgid), sizeof(*pPgid)) == 0) break;
    pPage = pPage->pHashNext;
  }

  if (pPage || !alcNewPage) {
    if (pPage) tdbPCachePinPage(pPage);
    return pPage;
  }

  // 2. Try to allocate a new page from the free list
  if (pCache->pFree) {
    pPage = pCache->pFree;
    pCache->pFree = pPage->pFreeNext;
    pCache->nFree--;
    pPage->pLruNext = NULL;
  }

  // 3. Try to Recycle a page
  if (!pPage && !pCache->lru.pLruPrev->isAnchor) {
    pPage = pCache->lru.pLruPrev;
    tdbPCacheRemovePageFromHash(pPage);
    tdbPCachePinPage(pPage);
  }

  // 4. Try a stress allocation

  // 5. Page here are just created from a free list
  // or by recycling or allocated streesly,
  // need to initialize it
  if (pPage) {
    memcpy(&pPage->pgid, pPgid, sizeof(*pPgid));
    pPage->pCache = pCache;
    pPage->pLruNext = NULL;
    tdbPCacheAddPageToHash(pPage);
  }

  return pPage;
}

static void tdbPCachePinPage(SPgHdr *pPage) {
  SPCache *pCache;

  pCache = pPage->pCache;
  if (!PAGE_IS_PINNED(pPage)) {
    pPage->pLruPrev->pLruNext = pPage->pLruNext;
    pPage->pLruNext->pLruPrev = pPage->pLruPrev;
    pPage->pLruNext = NULL;

    pCache->nRecyclable--;
  }
}

static void tdbPCacheRemovePageFromHash(SPgHdr *pPage) {
  SPCache *pCache;
  SPgHdr **ppPage;
  int      h;

  pCache = pPage->pCache;
  h = PCACHE_PAGE_HASH(&(pPage->pgid));
  for (ppPage = &(pCache->pgHash[h % pCache->nHash]); *ppPage != pPage; ppPage = &((*ppPage)->pHashNext))
    ;
  ASSERT(*ppPage == pPage);
  *ppPage = pPage->pHashNext;

  pCache->nPage--;
}

static void tdbPCacheAddPageToHash(SPgHdr *pPage) {
  SPCache *pCache;
  int      h;

  pCache = pPage->pCache;
  h = PCACHE_PAGE_HASH(&pPage->pgid) % pCache->nHash;

  pPage->pHashNext = pCache->pgHash[h];
  pCache->pgHash[h] = pPage;

  pCache->nPage++;
}