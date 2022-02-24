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
  pthread_mutex_t mutex;
  int             nFree;
  SPgHdr *        pFree;
  int             nPage;
  int             nHash;
  SPgHdr **       pgHash;
  int             nRecyclable;
  SPgHdr          lru;
  int             nDirty;
  SPgHdr *        pDirty;
  SPgHdr *        pDirtyTail;
};

#define PCACHE_PAGE_HASH(pgid) 0  // TODO
#define PAGE_IS_PINNED(pPage)  ((pPage)->pLruNext == NULL)

static int     tdbPCacheOpenImpl(SPCache *pCache);
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

  if (tdbPCacheOpenImpl(pCache) < 0) {
    free(pCache);
    return -1;
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

static int tdbPCacheOpenImpl(SPCache *pCache) {
  SPgHdr *pPage;
  u8 *    pPtr;
  int     tsize;

  tdbPCacheInitLock(pCache);

  // Open the free list
  pCache->nFree = 0;
  pCache->pFree = NULL;
  for (int i = 0; i < pCache->cacheSize; i++) {
    tsize = pCache->pageSize + sizeof(SPgHdr) + pCache->extraSize;
    pPtr = (u8 *)calloc(1, tsize);
    if (pPtr == NULL) {
      // TODO
      return -1;
    }

    pPage = (SPgHdr *)(&(pPtr[pCache->pageSize]));
    pPage->pData = (void *)pPtr;
    pPage->pExtra = (void *)(&(pPage[1]));
    // pPage->pgid = 0;
    pPage->isAnchor = 0;
    pPage->isLocalPage = 1;
    pPage->pCache = pCache;
    pPage->pHashNext = NULL;
    pPage->pLruNext = NULL;
    pPage->pLruPrev = NULL;

    pPage->pFreeNext = pCache->pFree;
    pCache->pFree = pPage;
    pCache->nFree++;
  }

  // Open the hash table
  pCache->nPage = 0;
  pCache->nHash = pCache->cacheSize;
  pCache->pgHash = (SPgHdr **)calloc(pCache->nHash, sizeof(SPgHdr *));
  if (pCache->pgHash == NULL) {
    // TODO
    return -1;
  }

  // Open LRU list
  pCache->nRecyclable = 0;
  pCache->lru.isAnchor = 1;
  pCache->lru.pLruNext = &(pCache->lru);
  pCache->lru.pLruPrev = &(pCache->lru);

  // Open dirty list
  pCache->nDirty = 0;
  pCache->pDirty = pCache->pDirtyTail = NULL;

  return 0;
}