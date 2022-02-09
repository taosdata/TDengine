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

static void pgCachePinPage(SPage *pPage);
static void pgCacheUnpinPage(SPage *pPage);

int pgCacheCreate(SPgCache **ppPgCache, pgsize_t pgSize, int32_t npage) {
  SPgCache *pPgCache;
  SPage *   pPage;

  *ppPgCache = NULL;

  if (!TDB_IS_PGSIZE_VLD(pgSize)) {
    return -1;
  }

  pPgCache = (SPgCache *)calloc(1, sizeof(*pPgCache));
  if (pPgCache == NULL) {
    return -1;
  }

  taosInitRWLatch(&(pPgCache->mutex));
  pPgCache->pgsize = pgSize;
  pPgCache->npage = npage;

  pPgCache->pages = (SPage *)calloc(npage, sizeof(SPage));
  if (pPgCache->pages == NULL) {
    pgCacheDestroy(pPgCache);
    return -1;
  }

  TD_DLIST_INIT(&(pPgCache->freeList));

  for (int32_t i = 0; i < npage; i++) {
    pPage = pPgCache->pages + i;

    pPage->pgid = TDB_IVLD_PGID;
    pPage->frameid = i;

    pPage->pData = (uint8_t *)calloc(1, pgSize);
    if (pPage->pData == NULL) {
      pgCacheDestroy(pPgCache);
      return -1;
    }

    pPgCache->pght.nbucket = npage;
    pPgCache->pght.buckets = (SPgList *)calloc(pPgCache->pght.nbucket, sizeof(SPgList));
    if (pPgCache->pght.buckets == NULL) {
      pgCacheDestroy(pPgCache);
      return -1;
    }

    TD_DLIST_APPEND_WITH_FIELD(&(pPgCache->freeList), pPage, freeNode);
  }

  *ppPgCache = pPgCache;
  return 0;
}

int pgCacheDestroy(SPgCache *pPgCache) {
  SPage *pPage;
  if (pPgCache) {
    tfree(pPgCache->pght.buckets);
    if (pPgCache->pages) {
      for (int32_t i = 0; i < pPgCache->npage; i++) {
        pPage = pPgCache->pages + i;
        tfree(pPage->pData);
      }

      free(pPgCache->pages);
    }
    free(pPgCache);
  }

  return 0;
}

int pgCacheOpen(SPgCache **ppPgCache) {
  if (*ppPgCache == NULL) {
    if (pgCacheCreate(ppPgCache, TDB_DEFAULT_PGSIZE, TDB_DEFAULT_CACHE_SIZE / TDB_DEFAULT_PGSIZE) < 0) {
      return -1;
    }
  }
  // TODO
  return 0;
}

int pgCacheClose(SPgCache *pPgCache) {
  // TODO
  return 0;
}

#define PG_CACHE_HASH(fileid, pgno)       \
  ({                                      \
    uint64_t *tmp = (uint64_t *)(fileid); \
    (tmp[0] + tmp[1] + tmp[2] + (pgno));  \
  })

SPage *pgCacheFetch(SPgCache *pPgCache, pgid_t pgid) {
  SPage *  pPage;
  SPgFile *pPgFile;
  SPgList *pBucket;

  // 1. Search the page hash table SPgCache.pght
  pBucket = pPgCache->pght.buckets + (PG_CACHE_HASH(pgid.fileid, pgid.pgno) % pPgCache->pght.nbucket);
  pPage = TD_DLIST_HEAD(pBucket);
  while (pPage && tdbCmprPgId(&(pPage->pgid), &pgid)) {
    pPage = TD_DLIST_NODE_NEXT_WITH_FIELD(pPage, pghtNode);
  }

  if (pPage) {
    // Page is found, pin the page and return the page
    pgCachePinPage(pPage);
    return pPage;
  }

  // 2. Check the free list
  pPage = TD_DLIST_HEAD(&(pPgCache->freeList));
  if (pPage) {
    TD_DLIST_POP_WITH_FIELD(&(pPgCache->freeList), pPage, freeNode);
    pgCachePinPage(pPage);
    return pPage;
  }

  // 3. Try to recycle a page from the LRU list
  pPage = TD_DLIST_HEAD(&(pPgCache->lru));
  if (pPage) {
    TD_DLIST_POP_WITH_FIELD(&(pPgCache->lru), pPage, lruNode);
    // TODO: remove from the hash table
    pgCachePinPage(pPage);
    return pPage;
  }

  // 4. If a memory allocator is set, try to allocate from the allocator (TODO)

  return NULL;
}

int pgCacheRelease(SPage *pPage) {
  // TODO
  return 0;
}

static void pgCachePinPage(SPage *pPage) {
  // TODO
}

static void pgCacheUnpinPage(SPage *pPage) {
  // TODO
}