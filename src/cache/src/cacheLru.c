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

#include "cacheLru.h"
#include "cacheItem.h"

int cacheLruInit(cache_t* pCache) {
  int i = 0;
  cacheSlabLruClass *pLru = &(pCache->lruArray[0]);
  memset(pLru, 0, sizeof(cacheSlabLruClass) * MAX_NUMBER_OF_SLAB_LRU);

  for (i = 0; i < MAX_NUMBER_OF_SLAB_LRU; i++) {
    cacheSlabLruClass* lru = pLru + i;
    if (cacheMutexInit(&(lru->mutex)) != 0) {
      goto _err;
    }

    lru->id = i;
  }
  return CACHE_OK;

_err:
  cacheLruDestroy(pCache);
  return CACHE_FAIL;
}

void cacheLruDestroy(cache_t* pCache) {
  int i = 0;
  cacheSlabLruClass *pLru = &(pCache->lruArray[0]);

  for (i = 0; i < MAX_NUMBER_OF_SLAB_LRU; i++) {
    cacheSlabLruClass* lru = pLru + i;
    if (lru->id == 0) {
      break;
    }
    cacheMutexDestroy(&(lru->mutex));
  }
}

void cacheLruUnlinkItem(cache_t* pCache, cacheItem* pItem, cacheLockFlag flag) {  
  cacheSlabLruClass* pLru = &(pCache->lruArray[item_slablru_id(pItem)]);
  assert(item_slablru_id(pItem) == pLru->id);

  if (IS_CACHE_LOCK_LRU(flag)) cacheMutexLock(&(pLru->mutex));

  cacheItem* head = pLru->head;
  cacheItem* tail = pLru->tail;
  
  assert((head == NULL && tail == NULL) || (head && tail));
  assert(head == NULL || pLru->num > 0);

  if (head == pItem) {
    assert(pItem->prev == NULL);
    pLru->head = pItem->next;
  }
  if (tail == pItem) {
    assert(pItem->next == NULL);
    pLru->tail = pItem->prev;
  }
  assert(pItem->next != pItem);
  assert(pItem->prev != pItem);

  if (pItem->next) pItem->next->prev = pItem->prev;
  if (pItem->prev) pItem->prev->next = pItem->next;
  pItem->next = pItem->prev = NULL;
  pLru->num -= 1;
  pLru->bytes -= cacheItemTotalBytes(pItem->nkey, pItem->nbytes);

  if (IS_CACHE_LOCK_LRU(flag)) cacheMutexUnlock(&(pLru->mutex));
}

void cacheLruLinkItem(cache_t* pCache, cacheItem* pItem, cacheLockFlag flag) {
  cacheSlabLruClass* pLru = &(pCache->lruArray[item_slablru_id(pItem)]);
  assert(item_slablru_id(pItem) == pLru->id);

  if (IS_CACHE_LOCK_LRU(flag)) cacheMutexLock(&(pLru->mutex));
  
  cacheItem* head = pLru->head;
  cacheItem* tail = pLru->tail;

  assert((head == NULL && tail == NULL) || (head && tail));
  assert(head == NULL || pLru->num > 0);

  pItem->next = head;
  pItem->prev = NULL;

  if (pItem->next) pItem->next->prev = pItem;

  pLru->head = pItem;
  if (tail == NULL) pLru->tail = pItem;

  pLru->num += 1;
  pLru->bytes += cacheItemTotalBytes(pItem->nkey, pItem->nbytes);

  if (IS_CACHE_LOCK_LRU(flag)) cacheMutexUnlock(&(pLru->mutex));
}

void cacheLruMoveToHead(cache_t* cache, cacheItem* pItem, cacheLockFlag flag) {
  cacheSlabLruClass* pLru = &(cache->lruArray[item_slablru_id(pItem)]);
  if (IS_CACHE_LOCK_LRU(flag)) cacheMutexLock(&(pLru->mutex));

  cacheLruUnlinkItem(cache, pItem, 0);
  cacheLruLinkItem(cache, pItem, 0);

  if (IS_CACHE_LOCK_LRU(flag)) cacheMutexUnlock(&(pLru->mutex));
}