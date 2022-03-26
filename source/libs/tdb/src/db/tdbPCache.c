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
  TdThreadMutex mutex;
  int             nFree;
  SPage          *pFree;
  int             nPage;
  int             nHash;
  SPage         **pgHash;
  int             nRecyclable;
  SPage           lru;
};

#define PCACHE_PAGE_HASH(pPgid)                              \
  ({                                                         \
    u32 *t = (u32 *)((pPgid)->fileid);                       \
    t[0] + t[1] + t[2] + t[3] + t[4] + t[5] + (pPgid)->pgno; \
  })
#define PAGE_IS_PINNED(pPage) ((pPage)->pLruNext == NULL)

// For page ref
#define TDB_INIT_PAGE_REF(pPage) ((pPage)->nRef = 0)
#if 0
#define TDB_REF_PAGE(pPage)     (++(pPage)->nRef)
#define TDB_UNREF_PAGE(pPage)   (--(pPage)->nRef)
#define TDB_GET_PAGE_REF(pPage) ((pPage)->nRef)
#else
#define TDB_REF_PAGE(pPage)     atomic_add_fetch_32(&((pPage)->nRef), 1)
#define TDB_UNREF_PAGE(pPage)   atomic_sub_fetch_32(&((pPage)->nRef), 1)
#define TDB_GET_PAGE_REF(pPage) atomic_load_32(&((pPage)->nRef))
#endif

static int    tdbPCacheOpenImpl(SPCache *pCache);
static void   tdbPCacheInitLock(SPCache *pCache);
static void   tdbPCacheClearLock(SPCache *pCache);
static void   tdbPCacheLock(SPCache *pCache);
static void   tdbPCacheUnlock(SPCache *pCache);
static bool   tdbPCacheLocked(SPCache *pCache);
static SPage *tdbPCacheFetchImpl(SPCache *pCache, const SPgid *pPgid, bool alcNewPage);
static void   tdbPCachePinPage(SPage *pPage);
static void   tdbPCacheRemovePageFromHash(SPage *pPage);
static void   tdbPCacheAddPageToHash(SPage *pPage);
static void   tdbPCacheUnpinPage(SPage *pPage);
static void  *tdbOsMalloc(void *arg, size_t size);
static void   tdbOsFree(void *arg, void *ptr);

int tdbPCacheOpen(int pageSize, int cacheSize, SPCache **ppCache) {
  SPCache *pCache;
  void    *pPtr;
  SPage   *pPgHdr;

  pCache = (SPCache *)taosMemoryCalloc(1, sizeof(*pCache));
  if (pCache == NULL) {
    return -1;
  }

  pCache->pageSize = pageSize;
  pCache->cacheSize = cacheSize;

  if (tdbPCacheOpenImpl(pCache) < 0) {
    taosMemoryFree(pCache);
    return -1;
  }

  *ppCache = pCache;
  return 0;
}

int tdbPCacheClose(SPCache *pCache) {
  /* TODO */
  return 0;
}

SPage *tdbPCacheFetch(SPCache *pCache, const SPgid *pPgid, bool alcNewPage) {
  SPage *pPage;

  tdbPCacheLock(pCache);

  pPage = tdbPCacheFetchImpl(pCache, pPgid, alcNewPage);
  if (pPage) {
    TDB_REF_PAGE(pPage);
  }

  tdbPCacheUnlock(pCache);

  return pPage;
}

void tdbPCacheRelease(SPage *pPage) {
  i32 nRef;

  nRef = TDB_UNREF_PAGE(pPage);
  ASSERT(nRef >= 0);

  if (nRef == 0) {
    if (1 /*TODO: page still clean*/) {
      tdbPCacheUnpinPage(pPage);
    } else {
      // TODO
      ASSERT(0);
    }
  }
}

static void tdbPCacheInitLock(SPCache *pCache) { taosThreadMutexInit(&(pCache->mutex), NULL); }

static void tdbPCacheClearLock(SPCache *pCache) { taosThreadMutexDestroy(&(pCache->mutex)); }

static void tdbPCacheLock(SPCache *pCache) { taosThreadMutexLock(&(pCache->mutex)); }

static void tdbPCacheUnlock(SPCache *pCache) { taosThreadMutexUnlock(&(pCache->mutex)); }

static bool tdbPCacheLocked(SPCache *pCache) {
  assert(0);
  // TODO
  return true;
}

static SPage *tdbPCacheFetchImpl(SPCache *pCache, const SPgid *pPgid, bool alcNewPage) {
  SPage *pPage;

  // 1. Search the hash table
  pPage = pCache->pgHash[PCACHE_PAGE_HASH(pPgid) % pCache->nHash];
  while (pPage) {
    if (TDB_IS_SAME_PAGE(&(pPage->pgid), pPgid)) break;
    pPage = pPage->pHashNext;
  }

  if (pPage || !alcNewPage) {
    if (pPage) {
      tdbPCachePinPage(pPage);
    }
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

  // 4. Try a stress allocation (TODO)

  // 5. Page here are just created from a free list
  // or by recycling or allocated streesly,
  // need to initialize it
  if (pPage) {
    memcpy(&(pPage->pgid), pPgid, sizeof(*pPgid));
    pPage->pLruNext = NULL;
    pPage->pPager = NULL;
    tdbPCacheAddPageToHash(pPage);
  }

  return pPage;
}

static void tdbPCachePinPage(SPage *pPage) {
  SPCache *pCache;

  pCache = pPage->pCache;
  if (!PAGE_IS_PINNED(pPage)) {
    pPage->pLruPrev->pLruNext = pPage->pLruNext;
    pPage->pLruNext->pLruPrev = pPage->pLruPrev;
    pPage->pLruNext = NULL;

    pCache->nRecyclable--;
  }
}

static void tdbPCacheUnpinPage(SPage *pPage) {
  SPCache *pCache;
  i32      nRef;

  pCache = pPage->pCache;

  tdbPCacheLock(pCache);

  nRef = TDB_GET_PAGE_REF(pPage);
  ASSERT(nRef >= 0);
  if (nRef == 0) {
    // Add the page to LRU list
    ASSERT(pPage->pLruNext == NULL);

    pPage->pLruPrev = &(pCache->lru);
    pPage->pLruNext = pCache->lru.pLruNext;
    pCache->lru.pLruNext->pLruPrev = pPage;
    pCache->lru.pLruNext = pPage;
  }

  pCache->nRecyclable++;

  tdbPCacheUnlock(pCache);
}

static void tdbPCacheRemovePageFromHash(SPage *pPage) {
  SPCache *pCache;
  SPage  **ppPage;
  int      h;

  pCache = pPage->pCache;
  h = PCACHE_PAGE_HASH(&(pPage->pgid));
  for (ppPage = &(pCache->pgHash[h % pCache->nHash]); *ppPage != pPage; ppPage = &((*ppPage)->pHashNext))
    ;
  ASSERT(*ppPage == pPage);
  *ppPage = pPage->pHashNext;

  pCache->nPage--;
}

static void tdbPCacheAddPageToHash(SPage *pPage) {
  SPCache *pCache;
  int      h;

  pCache = pPage->pCache;
  h = PCACHE_PAGE_HASH(&(pPage->pgid)) % pCache->nHash;

  pPage->pHashNext = pCache->pgHash[h];
  pCache->pgHash[h] = pPage;

  pCache->nPage++;
}

static int tdbPCacheOpenImpl(SPCache *pCache) {
  SPage *pPage;
  u8    *pPtr;
  int    tsize;
  int    ret;

  tdbPCacheInitLock(pCache);

  // Open the free list
  pCache->nFree = 0;
  pCache->pFree = NULL;
  for (int i = 0; i < pCache->cacheSize; i++) {
    ret = tdbPageCreate(pCache->pageSize, &pPage, tdbOsMalloc, NULL);
    if (ret < 0) {
      // TODO: handle error
      return -1;
    }

    // pPage->pgid = 0;
    pPage->isAnchor = 0;
    pPage->isLocalPage = 1;
    pPage->pCache = pCache;
    TDB_INIT_PAGE_REF(pPage);
    pPage->pHashNext = NULL;
    pPage->pLruNext = NULL;
    pPage->pLruPrev = NULL;
    pPage->pDirtyNext = NULL;

    pPage->pFreeNext = pCache->pFree;
    pCache->pFree = pPage;
    pCache->nFree++;
  }

  // Open the hash table
  pCache->nPage = 0;
  pCache->nHash = pCache->cacheSize;
  pCache->pgHash = (SPage **)taosMemoryCalloc(pCache->nHash, sizeof(SPage *));
  if (pCache->pgHash == NULL) {
    // TODO
    return -1;
  }

  // Open LRU list
  pCache->nRecyclable = 0;
  pCache->lru.isAnchor = 1;
  pCache->lru.pLruNext = &(pCache->lru);
  pCache->lru.pLruPrev = &(pCache->lru);

  return 0;
}

int tdbPCacheGetPageSize(SPCache *pCache) { return pCache->pageSize; }

static void *tdbOsMalloc(void *arg, size_t size) {
  void *ptr;

  ptr = taosMemoryMalloc(size);

  return ptr;
}

static void tdbOsFree(void *arg, void *ptr) { taosMemoryFree(ptr); }