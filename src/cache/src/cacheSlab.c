/*
 * Copyright (c) 2019 TAOS Data, Inc. <cli@taosdata.com>
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

#include "cachePriv.h"
#include "cacheItem.h"
#include "cacheDefine.h"
#include "cacheLog.h"
#include "cacheSlab.h"
#include "cacheTable.h"
#include "osTime.h"

enum {
  LRU_PULL_EVICT = 1,
};

static cacheItem* doAllocItem(cache_t *cache, size_t size, unsigned int id);
static int  createNewSlab(cache_t *cache, cacheSlabClass *pSlab);
static bool isReachMemoryLimit(cache_t *cache, int len);
static int  slabGrowArray(cache_t *cache, cacheSlabClass *pSlab);
static void splitSlabPageIntoFreelist(cache_t *cache,char *ptr, uint32_t id);
static int moveItemFromLru(cache_t *cache, int id, int curLru, uint64_t totalBytes, uint8_t flags,
                           cacheMutex *pMutex, uint32_t* moveToLru,  cacheItem* search, cacheItem** pItem);
static int pullFromLru(cache_t *cache, cacheMutex* pMutex, int origId, int curLru, uint64_t total_bytes, uint8_t flags);

int cacheSlabInit(cache_t *cache) {
  // init pSlab class
  int i = 0;
  size_t size = sizeof(cacheItem) + CHUNK_SIZE;
  
  for (i = 0; i < MAX_NUMBER_OF_SLAB_CLASSES; i++) {
    cacheSlabClass *pSlab = calloc(1, sizeof(cacheSlabClass));
    if (pSlab == NULL) {
      goto error;
    }

    memset(pSlab, 0, sizeof(cacheSlabClass));

    if (cacheMutexInit(&(pSlab->mutex)) != 0) {
      goto error;
    }

    if (size % CHUNK_ALIGN_BYTES) {
      size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);
    }
    pSlab->size = size;
    pSlab->perSlab = SLAB_PAGE_SIZE / size;
    pSlab->id = i;
    pSlab->nAllocSlabs = 0;
    pSlab->slabArray = NULL;
    pSlab->nArray = 0;

    cache->slabs[i] = pSlab;

    size *= cache->options.factor;
  }
  cache->powerLargest = i;

  return CACHE_OK;

error:
  cacheLruDestroy(cache);
  
  return CACHE_FAIL;
}

void cacheSlabDestroy(cache_t *cache) {
  int i = 0;
  for (i = 0; i < MAX_NUMBER_OF_SLAB_CLASSES; ++i) {
    cacheSlabClass *pSlab = cache->slabs[i];
    if (pSlab == NULL) {
      continue;
    }
    cacheMutexDestroy(&(pSlab->mutex));
    free(pSlab->slabArray);
    free(pSlab);
  }
}

uint32_t cacheSlabId(cache_t *cache, size_t size) {
  int i = 0;
  while (size > cache->slabs[i]->size) {
    if (i++ > cache->powerLargest) {
      return cache->powerLargest;
    }
  }

  return i;
}

cacheItem* cacheSlabAllocItem(cache_t *cache, cacheMutex* pMutex, size_t ntotal, uint32_t slabId) {
  cacheItem *pItem = NULL;
  int i;

  for (i = 0; i < 10; ++i) {
    pItem = doAllocItem(cache, ntotal, slabId);
    if (pItem) {
      break;
    }

    if (pullFromLru(cache, pMutex, slabId, CACHE_LRU_COLD, 0, LRU_PULL_EVICT) <= 0) {  /* try to pull pItem fom cold list */
      /* pull pItem from cold list failed, try to pull pItem from hot list */
      if (pullFromLru(cache, pMutex, slabId, CACHE_LRU_HOT, 0, 0) <= 0) {
        break;
      }
    }
  }

  return pItem;
}

void cacheSlabFreeItem(cache_t *cache, cacheItem* pItem, cacheLockFlag flag) {  
  //size_t ntotal = cacheItemTotalBytes(pItem->nkey, pItem->nbytes);
  uint32_t id = item_slab_id(pItem);

  if (cacheItemIsNeverExpired(pItem)) {
    if (pItem->next) pItem->next->prev = pItem->prev;
    if (pItem->prev) pItem->prev->next = pItem->next;
    taosWLockLatch(&(cache->latch));
    if (pItem == cache->neverExpireItemHead) {
      cache->neverExpireItemHead = pItem->next;
    }
    taosWUnLockLatch(&(cache->latch));
  }

  cacheSlabClass* pSlab = cache->slabs[id];
  if (IS_CACHE_LOCK_SLAB(flag)) cacheMutexLock(&pSlab->mutex);

  if (!item_is_chunked(pItem)) {
    pItem->slabLruId = id;
    pItem->prev = NULL;
    pItem->next = pSlab->freeItem;
    if (pSlab->freeItem) pSlab->freeItem->prev = pItem;
    pSlab->freeItem = pItem;
    pSlab->nFree += 1;
  } else {

  }

  pItem->pTable = NULL;

  item_set_free(pItem);

  if (IS_CACHE_LOCK_SLAB(flag)) cacheMutexUnlock(&pSlab->mutex);
}

static bool isReachMemoryLimit(cache_t *cache, int len) {
  if (cache->alloced + len >= cache->options.limit) {
    return true;
  }

  return false;
}

static int slabGrowArray(cache_t *cache, cacheSlabClass *pSlab) {
  if (pSlab->nAllocSlabs == pSlab->nArray) {
    size_t newSize =  (pSlab->nArray != 0) ? pSlab->nArray * 2 : 16;
    void *pArray = realloc(pSlab->slabArray, newSize * sizeof(void *));
    if (pArray == NULL) return 0;
    pSlab->nAllocSlabs = newSize;
    pSlab->slabArray = pArray;
  }

  return 1;
}

static void splitSlabPageIntoFreelist(cache_t *cache, char *ptr, uint32_t id) {
  cacheSlabClass *p = cache->slabs[id];
  int i = 0;
  for (i = 0; i < p->perSlab; i++) {
    cacheItem* pItem = (cacheItem*)ptr;
    pItem->slabLruId = id;
    cacheSlabFreeItem(cache, pItem, 0);
    ptr += p->size;
  }
}

static int createNewSlab(cache_t *cache, cacheSlabClass *pSlab) {
  char *ptr;
  uint32_t id = pSlab->id;
  int len = pSlab->size * pSlab->perSlab;

  if (isReachMemoryLimit(cache, len)) { 
    cacheDebug("cache has been reached limit");
    return CACHE_REACH_LIMIT;
  }

  if (slabGrowArray(cache, pSlab) == 0 || (ptr = allocMemory(cache, len, false)) == NULL) {
    cacheError("allocMemory fail");
    return CACHE_ALLOC_FAIL;
  }

  memset(ptr, 0, (size_t)len);
  splitSlabPageIntoFreelist(cache, ptr, id);

  pSlab->slabArray[pSlab->nArray++] = ptr;

  return CACHE_OK;
}

static cacheItem* doAllocItem(cache_t *cache, size_t size, unsigned int id) {
  cacheSlabClass *pSlab = cache->slabs[id];
  cacheItem *pItem = NULL;

  cacheMutexLock(&pSlab->mutex);

  /* no free pItem, try to alloc new pSlab page */
  if (pSlab->nFree == 0) {
    createNewSlab(cache, pSlab);
  }

  /* if there is free items, free it from free list */
  if (pSlab->nFree > 0) {
    pItem = pSlab->freeItem;
    pSlab->freeItem = pItem->next;
    if (pItem->next) pItem->next->prev = NULL;
    item_unset_free(pItem);
    pSlab->nFree -= 1;
  }

  cacheMutexUnlock(&pSlab->mutex);

  return pItem;
}

/* If we're CACHE_LRU_HOT or CACHE_LRU_WARM and over size limit, send to CACHE_LRU_COLD.
 * If we're COLD_LRU, send to WARM_LRU unless we need to evict
 */
static int moveItemFromLru(cache_t *cache, int lruId, int curLru, uint64_t totalBytes, uint8_t flags,
                            cacheMutex *pMutex, uint32_t* moveToLru,  cacheItem* search, cacheItem** pItem) {
  int removed = 0;
  uint64_t limit = 0;
  cacheOption* opt = &(cache->options);
  cacheSlabLruClass* lru = &(cache->lruArray[lruId]);

  *moveToLru = 0;
  switch (curLru) {
    case CACHE_LRU_HOT:
      limit = totalBytes * opt->hotPercent / 100;
      /* no break here, go through to next case */
    case CACHE_LRU_WARM:
      if (limit == 0) {
        limit = totalBytes * opt->warmPercent / 100;
      }
      if (item_is_active(search)) {     /* is pItem active? */
        item_unset_active(search);      /* mark as unactive */
        removed++;
        if (curLru == CACHE_LRU_WARM) {   /* is warm lru list? */          
          cacheLruMoveToHead(cache, search, false);  /* move to lru head */
          cacheMutexTryUnlock(pMutex);
          //cacheItemRemove(cache, search);
        } else {                          /* else is hot lru list */
          /* Active CACHE_LRU_HOT items flow to CACHE_LRU_WARM */
          *moveToLru = CACHE_LRU_WARM;
          *pItem = search;
        }
      } else if (lru->bytes > limit) {
        /* over size limit, send to CACHE_LRU_COLD */
        *moveToLru = CACHE_LRU_COLD;
        *pItem = search;
        removed++;
      } else {
        /* not active, don't want to move to CACHE_LRU_COLD, not active */
        *pItem = search;
        if (item_is_active(search)) {
          item_unset_active(search);
          *moveToLru = CACHE_LRU_WARM;         
        }
      }
      break;
    case CACHE_LRU_COLD:
      *pItem = search;
      if (flags & LRU_PULL_EVICT) {
        cacheItemUnlink(search->pTable, search, 0);
        removed++;
      }
      break;
  }

  if (*moveToLru != 0) {
    /* unlink from current lru list */
    cacheLruUnlinkItem(cache, search, false);
  }
  return removed;
}

static int pullFromLru(cache_t *cache, cacheMutex* pLockedMutex, int slabId, int curLru, uint64_t totalBytes, uint8_t flags) {
  cacheItem* pItem = NULL;
  cacheItem* search;
  cacheItem* prev;
  cacheSlabLruClass* lru;
  int lruId = slabId;
  int removed = 0;
  int tries = 5;
  uint32_t moveToLru = 0;
  cacheMutex *pMutex = NULL;
  uint64_t now = taosGetTimestamp(TSDB_TIME_PRECISION_MILLI);

  lruId |= curLru;
  lru = &(cache->lruArray[lruId]);
  assert(lru->id == lruId);

  cacheMutexLock(&(lru->mutex));

  search = lru->tail;
  for (; tries > 0 && search != NULL; tries--, search = prev) {
    assert(item_slablru_id(search) == lruId);
    assert(item_is_used(search));    

    prev = search->prev;
    pMutex = getItemMutexByItem(search);
  
    /* hash bucket has been locked by other thread */
    if (pMutex != pLockedMutex && cacheMutexTryLock(pMutex) != 0) {
      continue;
    }

    /* if item user data has been locked by more then one, continue */
    if (itemUserDataRef(search) > 1) {
      continue;
    }

    /* only one item ref count, can release the item safely */
    if (itemIncrRef(search) == 2) {
      setItemRef(search, 1);
      cacheItemUnlink(search->pTable, search, 0);
      if (pMutex != pLockedMutex) cacheMutexTryUnlock(pMutex);
      removed++;
      continue;
    }

    /* is pItem expired? */
    if (cacheItemIsExpired(search, now)) {
      /* refcnt 2 -> 1 */
      cacheItemUnlink(search->pTable, search, 0);
      /* refcnt 1 -> 0 -> freeCacheItem */
      //cacheItemRemove(cache, search);     
      if (pMutex != pLockedMutex) cacheMutexTryUnlock(pMutex);
      removed++;
      continue;
    }

    removed += moveItemFromLru(cache, lruId, curLru, totalBytes, flags, pMutex, &moveToLru, search, &pItem);

    if (pItem != NULL) {
      break;
    }   
  }

  cacheMutexUnlock(&(lru->mutex));

  if (pItem != NULL) {
    if (moveToLru) {  /* move pItem to new lru list */
      /* set new lru list id */
      pItem->slabLruId = item_slab_id(pItem) | moveToLru;
      /* link to new lru list */
      cacheLruLinkItem(cache, pItem, true);
    }

    cacheItemRemove(cache, pItem);
    if (pMutex != pLockedMutex) cacheMutexTryUnlock(pMutex);
  }

  return removed;
}