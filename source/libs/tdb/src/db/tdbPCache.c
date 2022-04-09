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
  int         pageSize;
  int         cacheSize;
  tdb_mutex_t mutex;
  SPage      *pList;
  int         nFree;
  SPage      *pFree;
  int         nPage;
  int         nHash;
  SPage     **pgHash;
  int         nRecyclable;
  SPage       lru;
};

#define PCACHE_PAGE_HASH(pPgid)                              \
  ({                                                         \
    u32 *t = (u32 *)((pPgid)->fileid);                       \
    t[0] + t[1] + t[2] + t[3] + t[4] + t[5] + (pPgid)->pgno; \
  })
#define PAGE_IS_PINNED(pPage) ((pPage)->pLruNext == NULL)

static int    tdbPCacheOpenImpl(SPCache *pCache);
static SPage *tdbPCacheFetchImpl(SPCache *pCache, const SPgid *pPgid, TXN *pTxn);
static void   tdbPCachePinPage(SPCache *pCache, SPage *pPage);
static void   tdbPCacheRemovePageFromHash(SPCache *pCache, SPage *pPage);
static void   tdbPCacheAddPageToHash(SPCache *pCache, SPage *pPage);
static void   tdbPCacheUnpinPage(SPCache *pCache, SPage *pPage);
static int    tdbPCacheCloseImpl(SPCache *pCache);

static void tdbPCacheInitLock(SPCache *pCache) { tdbMutexInit(&(pCache->mutex), NULL); }
static void tdbPCacheDestroyLock(SPCache *pCache) { tdbMutexDestroy(&(pCache->mutex)); }
static void tdbPCacheLock(SPCache *pCache) { tdbMutexLock(&(pCache->mutex)); }
static void tdbPCacheUnlock(SPCache *pCache) { tdbMutexUnlock(&(pCache->mutex)); }

int tdbPCacheOpen(int pageSize, int cacheSize, SPCache **ppCache) {
  SPCache *pCache;
  void    *pPtr;
  SPage   *pPgHdr;

  pCache = (SPCache *)tdbOsCalloc(1, sizeof(*pCache));
  if (pCache == NULL) {
    return -1;
  }

  pCache->pageSize = pageSize;
  pCache->cacheSize = cacheSize;

  if (tdbPCacheOpenImpl(pCache) < 0) {
    tdbOsFree(pCache);
    return -1;
  }

  *ppCache = pCache;
  return 0;
}

int tdbPCacheClose(SPCache *pCache) {
  if (pCache) {
    tdbPCacheCloseImpl(pCache);
    tdbOsFree(pCache);
  }
  return 0;
}

SPage *tdbPCacheFetch(SPCache *pCache, const SPgid *pPgid, TXN *pTxn) {
  SPage *pPage;

  tdbPCacheLock(pCache);

  pPage = tdbPCacheFetchImpl(pCache, pPgid, pTxn);
  if (pPage) {
    TDB_REF_PAGE(pPage);
  }

  tdbPCacheUnlock(pCache);

  return pPage;
}

void tdbPCacheRelease(SPCache *pCache, SPage *pPage, TXN *pTxn) {
  i32 nRef;

  nRef = TDB_UNREF_PAGE(pPage);
  ASSERT(nRef >= 0);

  if (nRef == 0) {
    tdbPCacheLock(pCache);

    // test the nRef again to make sure
    // it is safe th handle the page
    nRef = TDB_GET_PAGE_REF(pPage);
    if (nRef == 0) {
      if (pPage->isLocal) {
        tdbPCacheUnpinPage(pCache, pPage);
      } else {
        // remove from hash
        tdbPCacheRemovePageFromHash(pCache, pPage);

        // free the page
        if (pTxn && pTxn->xFree) {
          tdbPageDestroy(pPage, pTxn->xFree, pTxn->xArg);
        }
      }
    }

    tdbPCacheUnlock(pCache);
  }
}

int tdbPCacheGetPageSize(SPCache *pCache) { return pCache->pageSize; }

static SPage *tdbPCacheFetchImpl(SPCache *pCache, const SPgid *pPgid, TXN *pTxn) {
  int    ret;
  SPage *pPage;

  // 1. Search the hash table
  pPage = pCache->pgHash[PCACHE_PAGE_HASH(pPgid) % pCache->nHash];
  while (pPage) {
    if (TDB_IS_SAME_PAGE(&(pPage->pgid), pPgid)) break;
    pPage = pPage->pHashNext;
  }

  if (pPage) {
    // TODO: the page need to be copied and
    // replaced the page in hash table
    tdbPCachePinPage(pCache, pPage);
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
    tdbPCacheRemovePageFromHash(pCache, pPage);
    tdbPCachePinPage(pCache, pPage);
  }

  // 4. Try a create new page
  if (!pPage && pTxn && pTxn->xMalloc) {
    ret = tdbPageCreate(pCache->pageSize, &pPage, pTxn->xMalloc, pTxn->xArg);
    if (ret < 0) {
      // TODO
      ASSERT(0);
      return NULL;
    }

    // init the page fields
    pPage->isAnchor = 0;
    pPage->isLocal = 0;
    TDB_INIT_PAGE_REF(pPage);
  }

  // 5. Page here are just created from a free list
  // or by recycling or allocated streesly,
  // need to initialize it
  if (pPage) {
    memcpy(&(pPage->pgid), pPgid, sizeof(*pPgid));
    pPage->pLruNext = NULL;
    pPage->pPager = NULL;

    // TODO: allocated page may not add to hash
    tdbPCacheAddPageToHash(pCache, pPage);
  }

  return pPage;
}

static void tdbPCachePinPage(SPCache *pCache, SPage *pPage) {
  if (!PAGE_IS_PINNED(pPage)) {
    pPage->pLruPrev->pLruNext = pPage->pLruNext;
    pPage->pLruNext->pLruPrev = pPage->pLruPrev;
    pPage->pLruNext = NULL;

    pCache->nRecyclable--;
  }
}

static void tdbPCacheUnpinPage(SPCache *pCache, SPage *pPage) {
  i32 nRef;

  ASSERT(!pPage->isDirty);
  ASSERT(TDB_GET_PAGE_REF(pPage) == 0);

  ASSERT(pPage->pLruNext == NULL);

  pPage->pLruPrev = &(pCache->lru);
  pPage->pLruNext = pCache->lru.pLruNext;
  pCache->lru.pLruNext->pLruPrev = pPage;
  pCache->lru.pLruNext = pPage;

  pCache->nRecyclable++;
}

static void tdbPCacheRemovePageFromHash(SPCache *pCache, SPage *pPage) {
  SPage **ppPage;
  int     h;

  h = PCACHE_PAGE_HASH(&(pPage->pgid));
  for (ppPage = &(pCache->pgHash[h % pCache->nHash]); *ppPage != pPage; ppPage = &((*ppPage)->pHashNext))
    ;
  ASSERT(*ppPage == pPage);
  *ppPage = pPage->pHashNext;

  pCache->nPage--;
}

static void tdbPCacheAddPageToHash(SPCache *pCache, SPage *pPage) {
  int h;

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
    ret = tdbPageCreate(pCache->pageSize, &pPage, NULL, NULL);
    if (ret < 0) {
      // TODO: handle error
      return -1;
    }

    // pPage->pgid = 0;
    pPage->isAnchor = 0;
    pPage->isLocal = 1;
    TDB_INIT_PAGE_REF(pPage);
    pPage->pHashNext = NULL;
    pPage->pLruNext = NULL;
    pPage->pLruPrev = NULL;
    pPage->pDirtyNext = NULL;

    // add page to free list
    pPage->pFreeNext = pCache->pFree;
    pCache->pFree = pPage;
    pCache->nFree++;
  }

  // Open the hash table
  pCache->nPage = 0;
  pCache->nHash = pCache->cacheSize;
  pCache->pgHash = (SPage **)tdbOsCalloc(pCache->nHash, sizeof(SPage *));
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

static int tdbPCacheCloseImpl(SPCache *pCache) {
  SPage *pPage;

  for (pPage = pCache->pList; pPage; pPage = pCache->pList) {
    pCache->pList = pPage->pCacheNext;
    tdbPageDestroy(pPage, NULL, NULL);
  }

  tdbPCacheDestroyLock(pCache);
  return 0;
}
