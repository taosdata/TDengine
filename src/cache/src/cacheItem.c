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

#include <assert.h>
#include <string.h>
#include "cacheTable.h"
#include "cachePriv.h"
#include "cacheLru.h"
#include "cacheItem.h"
#include "cacheSlab.h"

static void freeCacheItem(cache_t* pCache, cacheItem* pItem);
static cacheItem* allocChunkItem(cache_t* pCache, size_t nTotal);
static void updateItemInColdLruList(cacheItem* pItem, uint64_t now);

cacheItem* cacheAllocItem(cacheTable* pTable, uint8_t nkey, uint32_t nbytes, uint64_t expireTime) {
  cache_t* cache = pTable->pCache;
  size_t ntotal = cacheItemTotalBytes(nkey, nbytes);
  uint32_t id = cacheSlabId(cache, ntotal);
  cacheItem* pItem = NULL;

  if (ntotal > cache->slabs[cache->powerLargest - 1]->size) { /* chunk pItem */
    pItem = allocChunkItem(cache, ntotal);
  } else {
    pItem = cacheSlabAllocItem(cache, ntotal, id);
  }

  if (pItem == NULL) {
    return NULL;
    //pItem = malloc(ntotal);
  }

  memset(pItem, 0, sizeof(cacheItem));

  pItem->pTable = pTable;
  pItem->nkey = nkey;
  pItem->nbytes = nbytes;

  itemIncrRef(pItem);
  item_set_used(pItem);

  pItem->expireTime = expireTime;
  if (expireTime == 0) {
    /* never expire, add to never expire list */
    taosWLockLatch(&(cache->latch));
    pItem->next = cache->neverExpireItemHead;
    if (cache->neverExpireItemHead) cache->neverExpireItemHead->prev = pItem;
    cache->neverExpireItemHead = pItem;
    taosWUnLockLatch(&(cache->latch));
  } else {
    /* add to hot lru slab list */    
    pItem->slabLruId = id | CACHE_LRU_HOT;
    cacheLruLinkItem(cache, pItem, true);
  }

  return pItem;
}

void cacheItemUnlink(cacheTable* pTable, cacheItem* pItem, cacheLockFlag flag) {
  assert(pItem->pTable == pTable);
  if (item_is_used(pItem)) {
    cacheTableRemove(pTable, item_key(pItem), pItem->nkey, false);
    cacheLruUnlinkItem(pTable->pCache, pItem, flag);
    cacheItemRemove(pTable->pCache, pItem);
  }
}

void cacheItemRemove(cache_t* pCache, cacheItem* pItem) {
  assert(item_is_used(pItem));
  assert(itemRef(pItem) > 0);

  if (itemDecrRef(pItem) == 0) {
    freeCacheItem(pCache, pItem);
  }
}

void cacheItemBump(cacheTable* pTable, cacheItem* pItem, uint64_t now) {
  pItem->lastTime = now;

  if (item_is_active(pItem)) {
    /* already is active item, return */
    return;
  }

  if (!item_is_fetched(pItem)) {
    /* access only one time, make it as fetched */
    item_set_fetched(pItem);
    return;
  }

  /* already mark as fetched, mark it as active */
  item_set_actived(pItem);

  if (item_slablru_id(pItem) != CACHE_LRU_COLD) {    
    return;
  }

  updateItemInColdLruList(pItem, now);
}

static void updateItemInColdLruList(cacheItem* pItem, uint64_t now) {
  assert(item_is_used(pItem));
  assert(item_slablru_id(pItem) == CACHE_LRU_COLD && item_is_active(pItem));

  cacheMutex* pMutex = getItemMutexByKey(pItem->pTable, item_key(pItem), pItem->nkey);
  cacheMutexLock(pMutex);

  /* update last access time */
  pItem->lastTime = now;

  /* move pItem to warm lru list */
  cacheLruUnlinkItem(pItem->pTable->pCache, pItem, true);
  pItem->slabLruId = item_slab_id(pItem) | CACHE_LRU_WARM;
  cacheLruLinkItem(pItem->pTable->pCache, pItem, true);

  cacheMutexUnlock(pMutex);
}

static void freeCacheItem(cache_t* pCache, cacheItem* pItem) {
  assert(itemRef(pItem) == 0);
  assert(item_is_used(pItem));
  cacheSlabLruClass* pLru = &(pCache->lruArray[item_slablru_id(pItem)]);
  assert(pLru->head != pItem);
  assert(pLru->tail != pItem);

  if (item_is_chunked(pItem)) {
    taosWLockLatch(&(pCache->latch));
    pCache->alloced -= cacheItemTotalBytes(pItem->nkey, pItem->nbytes);
    if (pItem->prev) pItem->prev->next = pItem->next;
    if (pItem->next) pItem->next->prev = pItem->prev;
    if (pItem == pCache->chunkItemHead) pCache->chunkItemHead = pItem->next;
    taosWLockLatch(&(pCache->latch));
    free(pItem);
  } else {
    cacheSlabFreeItem(pCache, pItem, CACHE_LOCK_SLAB);
  }  
}

static cacheItem* allocChunkItem(cache_t* pCache, size_t nTotal) {
  return allocMemory(pCache, nTotal, true);
}

FORCE_INLINE cacheItem* cacheItemByData(void* data) {
  return (cacheItem*)(data - offsetof(cacheItem, data));
}
