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
#include "cacheHashtable.h"
#include "cacheint.h"
#include "cacheItem.h"
#include "cacheSlab.h"

size_t cacheItemTotalBytes(uint8_t nkey, uint32_t nbytes) {
  return sizeof(cacheItem) + sizeof(unsigned int) + (nkey + 1) + nbytes;
}

cacheItem* cacheAllocItem(cache_t* cache, uint8_t nkey, uint32_t nbytes, uint64_t expireTime) {
  size_t ntotal = cacheItemTotalBytes(nkey, nbytes);
  uint32_t id = slabClsId(cache, ntotal);
  cacheItem* item = NULL;

  if (ntotal > 10240) {

  } else {
    item = cacheSlabAllocItem(cache, ntotal, id);
  }

  if (item == NULL) {
    return NULL;
  }

  item->next = item->prev = NULL;
  item->expireTime = expireTime;
  if (expireTime == 0) {
    item->next = cache->neverExpireItemHead;
    if (cache->neverExpireItemHead) cache->neverExpireItemHead->prev = item;
    cache->neverExpireItemHead = item;
  } else {
    id |= CACHE_LRU_HOT;
  }
  item->slabClsId = id;

  return item;
}

void cacheItemFree(cache_t* cache, cacheItem* item) {
  assert(item->refCount == 0);
  assert(!item_is_linked(item));

  cacheSlabFreeItem(cache, item);
}

void cacheItemMoveToLruHead(cache_t* cache, cacheItem* item) {
  cacheSlabLruClass* lru = &(cache->lruArray[item_lruid(item)]);
  cacheMutexLock(&(lru->mutex));

  cacheItemUnlinkFromLru(cache, item, false);
  cacheItemLinkToLru(cache, item, false);

  cacheMutexUnlock(&(lru->mutex));
}

void cacheItemLinkToLru(cache_t* cache, cacheItem* item, bool lock) {
  cacheSlabLruClass* lru = &(cache->lruArray[item_lruid(item)]);

  if (lock) cacheMutexLock(&(lru->mutex));

  cacheItem* tail = lru->tail;
  if (tail->next) tail->next->prev = item;
  item->next = tail->next;
  item->prev = NULL;
  tail->next = item;

  lru->num += 1;
  lru->bytes += cacheItemTotalBytes(item->nkey, item->nbytes);

  if (lock) cacheMutexUnlock(&(lru->mutex));
}

void cacheItemUnlinkFromLru(cache_t* cache, cacheItem* item, bool lock) {  
  cacheSlabLruClass* lru = &(cache->lruArray[item_lruid(item)]);
  if (lock) cacheMutexLock(&(lru->mutex));

  cacheItem* tail = lru->tail;

  if (tail == item) {
    tail = tail->next;
  }

  if (item->next) item->next->prev = item->prev;
  if (item->prev) item->prev->next = item->next;

  lru->num -= 1;
  lru->bytes -= cacheItemTotalBytes(item->nkey, item->nbytes);

  if (lock) cacheMutexUnlock(&(lru->mutex));
}

void cacheItemUnlinkNolock(cacheTable* pTable, cacheItem* item) {
  if (item_is_linked(item)) {
    item_unlink(item);
    cacheTableRemove(pTable, item_key(item), item->nkey);
    cacheItemUnlinkFromLru(pTable->pCache, item, false);
    cacheItemRemove(pTable->pCache, item);
  }
}

void cacheItemRemove(cache_t* cache, cacheItem* item) {
  assert(!item_is_slabbed(item));
  assert(item->refCount > 0);

  if (itemDecrRef(item) == 0) {
    cacheItemFree(cache, item);
  }
}